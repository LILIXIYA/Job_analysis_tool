from __future__ import annotations

import csv
import json
import re
import time
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
import yaml

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta


# =============================
# Utilities
# =============================
def safe_compact(text: str, max_chars: int) -> str:
    text = (text or "").strip()
    if len(text) <= max_chars:
        return text
    head = text[: int(max_chars * 0.7)]
    tail = text[-int(max_chars * 0.3) :]
    return f"{head}\n...\n{tail}"


def strip_code_fences(s: str) -> str:
    s = (s or "").strip()
    s = re.sub(r"^```(?:json)?\s*", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s*```$", "", s)
    return s.strip()


def parse_json_loose(s: str) -> Tuple[Optional[dict], str]:
    s = strip_code_fences(s)

    try:
        obj = json.loads(s)
        if isinstance(obj, dict):
            return obj, ""
    except Exception:
        pass

    m = re.search(r"\{.*\}", s, flags=re.DOTALL)
    if m:
        try:
            obj = json.loads(m.group(0))
            if isinstance(obj, dict):
                return obj, ""
        except Exception as e:
            return None, f"JSON extract parse failed: {e}"

    return None, "Could not parse JSON from model response."


def clamp_int(x: Any, lo: int, hi: int, default: int = 0) -> int:
    try:
        v = int(x)
    except Exception:
        return default
    if v < lo:
        return lo
    if v > hi:
        return hi
    return v


def bullets_to_text(xs: Any) -> str:
    if not isinstance(xs, list):
        return ""
    return "\n".join(f"- {str(x)}" for x in xs if str(x).strip())


# =============================
# Trace-back date filter
# =============================
DATE_COL_CANDIDATES = [
    "scraped_at",
    "collected_at",
    "crawl_time",
    "crawled_at",
    "search_time",
    "searched_at",
    "fetched_at",
    "retrieved_at",
    "timestamp",
    "time",
    "datetime",
    "posted_at",
    "date_posted",
    "posted_time",
    "listed_at",
    "listing_date",
    "created_at",
    "published_at",
]


def _parse_datetime_loose(s: str, now: datetime) -> Optional[datetime]:
    if not s:
        return None
    s0 = str(s).strip()
    if not s0:
        return None

    sl = s0.lower().strip()

    if sl == "today":
        return now.replace(hour=0, minute=0, second=0, microsecond=0)
    if sl == "yesterday":
        d = now - timedelta(days=1)
        return d.replace(hour=0, minute=0, second=0, microsecond=0)

    m = re.search(r"(\d+)\s*(minute|minutes|hour|hours|day|days|week|weeks|month|months)\s*ago", sl)
    if m:
        n = int(m.group(1))
        unit = m.group(2)
        if "minute" in unit:
            return now - timedelta(minutes=n)
        if "hour" in unit:
            return now - timedelta(hours=n)
        if "day" in unit:
            return now - timedelta(days=n)
        if "week" in unit:
            return now - timedelta(weeks=n)
        if "month" in unit:
            return now - timedelta(days=30 * n)

    m2 = re.fullmatch(r"(\d+)\s*([dhwm])", sl)
    if m2:
        n = int(m2.group(1))
        u = m2.group(2)
        if u == "h":
            return now - timedelta(hours=n)
        if u == "d":
            return now - timedelta(days=n)
        if u == "w":
            return now - timedelta(weeks=n)
        if u == "m":
            return now - timedelta(days=30 * n)

    sl_clean = re.sub(r"(z|[+\-]\d{2}:?\d{2})$", "", sl).strip()

    try:
        iso = sl_clean.replace("t", "T")
        return datetime.fromisoformat(iso)
    except Exception:
        pass

    patterns = [
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
        "%Y/%m/%d %H:%M:%S",
        "%m/%d/%Y",
        "%m/%d/%Y %H:%M",
        "%m/%d/%Y %H:%M:%S",
        "%b %d, %Y",
        "%B %d, %Y",
    ]
    for p in patterns:
        try:
            return datetime.strptime(sl_clean, p)
        except Exception:
            continue

    return None


def pick_row_datetime(row: Dict[str, str], fieldnames: List[str], now: datetime) -> Tuple[Optional[datetime], str]:
    existing = set(fieldnames or [])
    for col in DATE_COL_CANDIDATES:
        if col in existing:
            dt = _parse_datetime_loose(row.get(col, ""), now)
            if dt is not None:
                return dt, col

    for col in (fieldnames or []):
        if not col:
            continue
        cl = col.lower()
        if any(k in cl for k in ["date", "time", "posted", "created", "scrap", "crawl", "collect", "search"]):
            dt = _parse_datetime_loose(row.get(col, ""), now)
            if dt is not None:
                return dt, col

    return None, ""


def filter_rows_by_trace_back(
    rows: List[Dict[str, str]],
    fieldnames: List[str],
    trace_back_days: int,
) -> Tuple[List[Dict[str, str]], Dict[str, Any]]:
    now = datetime.now()
    trace_back_days = int(trace_back_days or 0)

    info = {
        "enabled": trace_back_days > 0,
        "trace_back_days": trace_back_days,
        "used_date_col": "",
        "kept": len(rows),
        "dropped": 0,
        "unparsed": 0,
        "cutoff": "",
    }

    if trace_back_days <= 0:
        return rows, info

    cutoff = now - timedelta(days=trace_back_days)
    info["cutoff"] = cutoff.strftime("%Y-%m-%d %H:%M:%S")

    kept: List[Dict[str, str]] = []
    used_col = ""
    parsable_any = False

    for r in rows:
        dt, col = pick_row_datetime(r, fieldnames, now)
        if dt is None:
            info["unparsed"] += 1
            continue
        parsable_any = True
        if not used_col and col:
            used_col = col
        if dt >= cutoff:
            kept.append(r)

    if not parsable_any:
        print(
            "[WARN] trace_back_days is set but no parsable date/time column found in input CSV; "
            "skipping time filter to avoid dropping everything."
        )
        info["kept"] = len(rows)
        info["dropped"] = 0
        info["used_date_col"] = ""
        return rows, info

    info["used_date_col"] = used_col
    info["kept"] = len(kept)
    info["dropped"] = len(rows) - len(kept)
    return kept, info


# =============================
# Resume loader
# =============================
def load_resume(pp_cfg: dict) -> str:
    if pp_cfg.get("resume_text"):
        return pp_cfg["resume_text"].strip()

    path = pp_cfg.get("resume_path")
    if not path:
        raise ValueError("Provide resume_text or resume_path")

    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(path)

    if p.suffix.lower() == ".txt":
        return p.read_text(encoding="utf-8", errors="ignore")

    raise ValueError("For stability, please provide resume as .txt")


# =============================
# Qwen client
# =============================
@dataclass
class QwenConfig:
    base_url: str
    api_key: str
    model: str
    timeout_seconds: int = 60
    connect_timeout_seconds: int = 10


class QwenClient:
    def __init__(self, cfg: QwenConfig):
        self.cfg = cfg
        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {cfg.api_key}",
                "Content-Type": "application/json",
            }
        )

    def chat_once(self, system_prompt: str, user_prompt: str) -> Tuple[str, Optional[str]]:
        url = self.cfg.base_url.rstrip("/") + "/chat/completions"
        payload = {
            "model": self.cfg.model,
            "temperature": 0.2,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }

        try:
            r = self.session.post(
                url,
                json=payload,
                timeout=(self.cfg.connect_timeout_seconds, self.cfg.timeout_seconds),
            )
        except Exception as e:
            return "", f"request failed: {e}"

        if r.status_code != 200:
            return "", f"HTTP {r.status_code}: {(r.text or '')[:300]}"

        try:
            data = r.json()
            return data["choices"][0]["message"]["content"], None
        except Exception as e:
            return "", f"response parse error: {e}"


# =============================
# Prompt (One-call: qualify + preference)
# =============================
SYSTEM_PROMPT = """You are a senior US-based corporate recruiter and hiring manager.
You must evaluate two independent axes:
(A) Qualification / feasibility fit (0–3) based on the resume vs JD requirements.
(B) Candidate preference alignment (0–2) based on the preference profile below.

You MUST return ONLY valid JSON (no markdown, no code fences, no extra text).

Preference Profile (for preference_score only):
- Strongly prefer roles: Machine Learning Engineer, Applied Scientist, Agent/LLM/AI agent development.
- Prefer roles using modern AI/LLM techniques to solve real problems.
- Can accept: traditional ML roles and Data Scientist roles.
- Do NOT prefer: pure Software Engineer roles (non-ML).
- Must avoid: Intern roles, Manager/People-lead roles, Part-time roles.
- Must avoid: roles clearly requiring 10+ years of experience.

Scoring rules:
Qualification score (0–3):
- 0 = clearly not qualified / major mismatch
- 1 = weak match / significant gaps
- 2 = reasonable match / minor gaps or stretch
- 3 = strong match / low risk

Preference score (0–2):
- 0 = strong mismatch with preference profile OR hits any must-avoid category
- 1 = acceptable / neutral
- 2 = strong alignment with preferences

Hard constraints for preference_score:
If JD indicates Intern OR Manager/Lead/Director/People Manager OR Part-time
OR explicitly requires 10+ years experience, preference_score MUST be 0.

Evidence:
- Reasons should cite JD/resume evidence briefly.
- Keep bullets concise.
"""


def build_user_prompt(resume: str, meta: dict, jd: str) -> str:
    return f"""
JOB:
Title: {meta.get("title","")}
Company: {meta.get("company","")}
Location: {meta.get("location","")}

JOB DESCRIPTION:
\"\"\"{safe_compact(jd, 12000)}\"\"\"

RESUME:
\"\"\"{safe_compact(resume, 12000)}\"\"\"

TASK:
1) Summarize JD in 5-8 bullets.
2) Give qualify_score (0–3) with evidence bullets; list missing/weak areas if qualify_score ≤2.
3) Give preference_score (0–2) with evidence bullets based on the preference profile in system prompt.
   - If any hard constraint triggers (intern/manager/contract/part-time/10+ yrs), set preference_score=0 and explain why.
4) Return ONLY JSON exactly with the following keys.

Return JSON:
{{
  "jd_summary_bullets": [],
  "qualify_score": 0,
  "qualify_reason_bullets": [],
  "missing_or_weak_areas": [],
  "preference_score": 0,
  "preference_reason_bullets": []
}}
"""


# =============================
# CSV helpers
# =============================
def read_csv(path: str) -> Tuple[List[Dict[str, str]], List[str]]:
    rows: List[Dict[str, str]] = []

    with open(path, "r", encoding="utf-8", errors="ignore", newline="") as f:

        def _clean_lines():
            for lineno, line in enumerate(f, start=1):
                if "\x00" in line:
                    print(f"[WARN] Skip line {lineno}: contains NUL byte")
                    continue
                yield line

        reader = csv.DictReader(_clean_lines())
        fieldnames = reader.fieldnames or []

        for lineno, row in enumerate(reader, start=2):
            try:
                rows.append(row)
            except Exception as e:
                print(f"[WARN] Skip malformed row at line {lineno}: {e}")
                continue

    return rows, fieldnames


def ensure_output_csv(path: str, fieldnames: List[str]) -> None:
    p = Path(path)
    if p.exists() and p.stat().st_size > 0:
        return
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()


def load_processed_jobids(path: str) -> set:
    p = Path(path)
    if not p.exists():
        return set()
    rows, _ = read_csv(path)
    return {r.get("jobID") for r in rows if r.get("jobID")}


# =============================
# Worker (thread task) - thread-local client
# =============================
_thread_local = threading.local()
_client_cfg: Optional[QwenConfig] = None


def get_thread_client() -> QwenClient:
    """
    Each worker thread gets its own QwenClient (and its own requests.Session).
    This prevents deadlocks / hangs caused by sharing a Session across threads.
    """
    c = getattr(_thread_local, "client", None)
    if c is None:
        assert _client_cfg is not None, "Client config not initialized"
        c = QwenClient(_client_cfg)
        _thread_local.client = c
    return c


def process_one_job(
    r: Dict[str, str],
    base_cols: List[str],
    resume: str,
) -> Tuple[str, Dict[str, str], Optional[str]]:
    """
    Returns: (job_id, out_row, err_msg)
      - err_msg is for unexpected exceptions in this worker
      - LLM / parse errors are already written into out_row
    """
    job_id = r.get("jobID") or ""
    try:
        jd = r.get("job_description", "")
        out_row = {k: r.get(k, "") for k in base_cols}

        if not jd:
            out_row.update(
                llm_jd_summary="",
                qualify_score="",
                preference_score="",
                final_score="",
                qualify_reason="No job_description",
                preference_reason="",
                missing_or_weak_areas="",
            )
            return job_id, out_row, None

        client = get_thread_client()

        content, err = client.chat_once(
            SYSTEM_PROMPT,
            build_user_prompt(resume, r, jd),
        )

        if err:
            out_row.update(
                llm_jd_summary="",
                qualify_score="",
                preference_score="",
                final_score="",
                qualify_reason=f"LLM error: {err}",
                preference_reason="",
                missing_or_weak_areas="",
            )
            return job_id, out_row, None

        obj, perr = parse_json_loose(content)
        if obj is None:
            out_row.update(
                llm_jd_summary="",
                qualify_score="",
                preference_score="",
                final_score="",
                qualify_reason=f"Parse error: {perr}",
                preference_reason="",
                missing_or_weak_areas="",
            )
            return job_id, out_row, None

        q_score = clamp_int(obj.get("qualify_score", 0), 0, 3, default=0)
        p_score = clamp_int(obj.get("preference_score", 0), 0, 2, default=0)
        f_score = q_score * p_score

        out_row.update(
            llm_jd_summary=bullets_to_text(obj.get("jd_summary_bullets", [])),
            qualify_score=str(q_score),
            preference_score=str(p_score),
            final_score=str(f_score),
            qualify_reason=bullets_to_text(obj.get("qualify_reason_bullets", [])),
            preference_reason=bullets_to_text(obj.get("preference_reason_bullets", [])),
            missing_or_weak_areas=bullets_to_text(obj.get("missing_or_weak_areas", [])),
        )
        return job_id, out_row, None

    except Exception as e:
        out_row = {k: r.get(k, "") for k in base_cols}
        out_row.update(
            llm_jd_summary="",
            qualify_score="",
            preference_score="",
            final_score="",
            qualify_reason=f"Worker exception: {e}",
            preference_reason="",
            missing_or_weak_areas="",
        )
        return job_id, out_row, str(e)


# =============================
# Main
# =============================
def main():
    global _client_cfg

    with open("config.yaml", "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)

    pp = cfg["postprocess"]
    q = cfg["qwen"]

    input_csv = pp["input_csv"]
    output_csv = pp["output_csv"]
    sleep_s = float(pp.get("sleep_seconds", 0.0))

    max_workers = int(pp.get("threads", 10))
    trace_back_days = int(pp.get("trace_back_days", 0))

    resume = load_resume(pp)

    in_rows, in_fields = read_csv(input_csv)
    assert "jobID" in in_fields
    assert "job_description" in in_fields

    # Apply trace-back filter on RAW rows BEFORE processing / dedup
    filtered_rows, tb_info = filter_rows_by_trace_back(in_rows, in_fields, trace_back_days)
    if tb_info["enabled"]:
        print(
            f"[INFO] trace_back_days={tb_info['trace_back_days']} | cutoff={tb_info['cutoff']} | "
            f"kept={tb_info['kept']} dropped={tb_info['dropped']} unparsed={tb_info['unparsed']} "
            f"date_col={tb_info['used_date_col'] or 'AUTO/UNKNOWN'}"
        )

    base_cols = [c for c in in_fields if c != "job_description"]

    out_fields = base_cols + [
        "llm_jd_summary",
        "qualify_score",
        "preference_score",
        "final_score",
        "qualify_reason",
        "preference_reason",
        "missing_or_weak_areas",
    ]

    ensure_output_csv(output_csv, out_fields)
    processed = load_processed_jobids(output_csv)

    to_process = [r for r in filtered_rows if r.get("jobID") and r.get("jobID") not in processed]
    total = len(to_process)

    print(f"[INFO] Total jobs to process: {total}")
    print(f"[INFO] Using threads: {max_workers}")

    # Initialize global client config used by thread-local clients
    _client_cfg = QwenConfig(
        base_url=q["base_url"],
        api_key=q["api_key"],
        model=q["model"],
        timeout_seconds=q.get("timeout_seconds", 60),
        connect_timeout_seconds=q.get("connect_timeout_seconds", 10),
    )

    start_time = time.time()
    done = 0

    with open(output_csv, "a", encoding="utf-8", newline="") as fout:
        writer = csv.DictWriter(fout, fieldnames=out_fields)

        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            future_to_jobid = {}

            # submit tasks
            for r in to_process:
                job_id = r.get("jobID") or ""
                fut = ex.submit(process_one_job, r, base_cols, resume)
                future_to_jobid[fut] = job_id

                # optional throttle between submits
                if sleep_s > 0:
                    time.sleep(sleep_s)

            # consume results as they complete
            for fut in as_completed(future_to_jobid):
                job_id = future_to_jobid[fut]
                try:
                    job_id_ret, out_row, unexpected_err = fut.result()
                except Exception as e:
                    out_row = {k: "" for k in base_cols}
                    out_row.update(
                        llm_jd_summary="",
                        qualify_score="",
                        preference_score="",
                        final_score="",
                        qualify_reason=f"Future exception: {e}",
                        preference_reason="",
                        missing_or_weak_areas="",
                    )
                    job_id_ret = job_id
                    unexpected_err = str(e)

                writer.writerow(out_row)
                fout.flush()
                processed.add(job_id_ret)

                done += 1
                elapsed = time.time() - start_time
                avg = elapsed / done if done else 0.0
                eta = avg * (total - done)

                if unexpected_err:
                    print(f"[WARN] {done}/{total} | jobID={job_id_ret} | err={unexpected_err} | ETA={eta/60:.1f}m")
                else:
                    print(f"[OK] {done}/{total} | jobID={job_id_ret} | ETA={eta/60:.1f}m")

    print(f"[INFO] Finished. Output: {output_csv}")


if __name__ == "__main__":
    main()
