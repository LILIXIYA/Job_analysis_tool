from __future__ import annotations

import csv
import json
import logging
import os
import random
import re
import time
import html as ihtml
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import requests
import yaml
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
import webdriver_manager.chrome as ChromeDriverManager


ChromeDriverManager = ChromeDriverManager.ChromeDriverManager
log = logging.getLogger(__name__)


def setupLogger() -> None:
    dt: str = datetime.strftime(datetime.now(), "%m_%d_%y %H_%M_%S ")

    if not os.path.isdir("./logs"):
        os.mkdir("./logs")

    logging.basicConfig(
        filename=("./logs/" + str(dt) + "collectJobs.log"),
        filemode="w",
        format="%(asctime)s::%(name)s::%(levelname)s::%(message)s",
        datefmt="./logs/%d-%b-%y %H:%M:%S",
    )
    log.setLevel(logging.DEBUG)

    c_handler = logging.StreamHandler()
    c_handler.setLevel(logging.DEBUG)
    c_format = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s", "%H:%M:%S")
    c_handler.setFormatter(c_format)
    log.addHandler(c_handler)


def _normalize_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def _normalize_company_url(url: str) -> str:
    """
    Normalize to:
      https://www.linkedin.com/company/<slug>/
    Fixes /life, /people, ?trk..., etc.
    """
    u = (url or "").strip()
    if not u:
        return ""
    m = re.search(r"(https?://www\.linkedin\.com/company/[^/?#]+)", u, re.IGNORECASE)
    if not m:
        return ""
    return m.group(1).rstrip("/") + "/"


# =========================================================
# Models
# =========================================================
@dataclass
class JobRecord:
    run_at: str
    posted_at: str
    timestamp: str
    jobID: str
    title: str
    company: str
    location: str
    workplace_type: str
    seniority: str
    employment_type: str
    easy_apply: bool
    job_description: str
    job_url: str
    search_position: str
    search_location: str

    # ✅ NEW (schema upgrade compatible)
    company_about_url: str = "cannot fetch"
    company_size: str = "cannot fetch"
    associated_members: str = "cannot fetch"


# =========================================================
# CSV Store (dedupe + write) + schema upgrade
# =========================================================
class JobStore:
    def __init__(self, filename: str):
        self.filename = filename
        self._ensure_header_or_upgrade()
        self.savedJobIDs = set(self._load_saved_jobIDs() or [])
        # Track companies that have already been checked for size/members
        self.checked_companies = self._load_checked_companies()

    def _load_checked_companies(self):
        """Load set of company names that have already had their size/members fetched."""
        if not Path(self.filename).exists():
            return set()
        try:
            df = pd.read_csv(self.filename, encoding="utf-8")
            checked = set()
            for _, row in df.iterrows():
                if row["company_size"] != "cannot fetch" or row["associated_members"] != "cannot fetch":
                    checked.add(row["company"])
            return checked
        except Exception as e:
            log.warning(f"Could not load checked companies: {e}")
            return set()

    @staticmethod
    def _header() -> list[str]:
        # ✅ Always keep a single source of truth for columns
        return [
            "run_at",
            "posted_at",
            "timestamp",
            "jobID",
            "title",
            "company",
            "location",
            "workplace_type",
            "seniority",
            "employment_type",
            "easy_apply",
            "job_description",
            "job_url",
            "search_position",
            "search_location",
            # ✅ NEW columns (will be added to old CSV automatically)
            "company_about_url",
            "company_size",
            "associated_members",
        ]

    def _ensure_header_or_upgrade(self) -> None:
        file_path = Path(self.filename)

        # file doesn't exist or empty -> create new with full header
        if (not file_path.exists()) or file_path.stat().st_size == 0:
            with open(self.filename, "w", newline="", encoding="utf-8") as f:
                csv.writer(f).writerow(self._header())
            log.info(f"Created CSV with header: {self.filename}")
            return

        # file exists -> check if schema upgrade needed
        try:
            df = pd.read_csv(self.filename, encoding="utf-8")
        except Exception as e:
            log.warning(f"Failed to read CSV for schema check: {self.filename} err={e}")
            return

        wanted = self._header()
        missing = [c for c in wanted if c not in df.columns]

        if not missing:
            return

        # ✅ upgrade: add missing columns with default "cannot fetch" and rewrite CSV
        for c in missing:
            df[c] = "cannot fetch"

        # stable schema
        df = df.reindex(columns=wanted)
        df.to_csv(self.filename, index=False, encoding="utf-8")
        log.info(f"Upgraded CSV schema, added columns: {missing}")

    def _load_saved_jobIDs(self) -> list[str] | None:
        try:
            if not Path(self.filename).exists():
                return None

            df = pd.read_csv(self.filename, encoding="utf-8")
            if "jobID" not in df.columns:
                df = pd.read_csv(
                    self.filename,
                    header=None,
                    names=["timestamp", "jobID", "job", "company", "attempted", "result"],
                    lineterminator="\n",
                    encoding="utf-8",
                )
            jobIDs = list(df["jobID"].dropna().astype(str).unique())
            log.info(f"{len(jobIDs)} jobIDs loaded from CSV (for dedupe)")
            return jobIDs
        except Exception as e:
            log.warning(f"jobIDs could not be loaded from CSV {self.filename}: {e}")
            return None

    def has(self, job_id: str) -> bool:
        return str(job_id) in self.savedJobIDs

    def company_already_checked(self, company_name: str) -> bool:
        """Check if we've already fetched company size/members for this company."""
        return company_name in self.checked_companies

    def add(self, rec: JobRecord) -> None:
        row = [
            rec.run_at,
            rec.posted_at,
            rec.timestamp,
            rec.jobID,
            rec.title,
            rec.company,
            rec.location,
            rec.workplace_type,
            rec.seniority,
            rec.employment_type,
            bool(rec.easy_apply),
            rec.job_description,
            rec.job_url,
            rec.search_position,
            rec.search_location,
            rec.company_about_url,
            rec.company_size,
            rec.associated_members,
        ]
        with open(self.filename, "a", newline="", encoding="utf-8") as f:
            csv.writer(f).writerow(row)

        self.savedJobIDs.add(str(rec.jobID))
        if rec.company_size != "cannot fetch" or rec.associated_members != "cannot fetch":
            self.checked_companies.add(rec.company)

        log.info(
            f"✅ Saved job {rec.jobID}: {rec.title} | {rec.company} | "
            f"posted_at={rec.posted_at or 'N/A'} | JD_len={len(rec.job_description or '')} | "
            f"size={rec.company_size or 'N/A'} members={rec.associated_members or 'N/A'}"
        )


# =========================================================
# Selenium Browser Layer
# =========================================================
class LinkedInBrowser:
    def __init__(self):
        self.options = self._browser_options()
        self.browser = webdriver.Chrome(
            service=ChromeService(ChromeDriverManager().install()),
            options=self.options,
        )
        # Hide webdriver flag
        self.browser.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        self.wait = WebDriverWait(self.browser, 30)

        self.locator = {
            "search": (By.CLASS_NAME, "jobs-search-results-list"),
            "links": ("xpath", '//div[@data-job-id]'),
            "easy_apply_button": (By.XPATH, '//button[contains(@class, "jobs-apply-button")]'),
        }

    def _browser_options(self):
        options = webdriver.ChromeOptions()
        # Anti-detection options
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)

        # User agent
        options.add_argument(
            "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )

        # Window size variations
        width = random.randint(1300, 1600)
        height = random.randint(700, 900)
        options.add_argument(f"--window-size={width},{height}")

        # Other options
        options.add_argument("--start-maximized")
        options.add_argument("--ignore-certificate-errors")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-blink-features")
        options.add_argument("--disable-infobars")
        options.add_argument("--disable-plugins-discovery")
        options.add_argument("--disable-web-security")
        options.add_argument("--allow-running-insecure-content")
        options.add_argument("--disable-dev-shm-usage")

        # Language settings
        options.add_argument("--lang=en-US,en;q=0.9")

        return options

    def login(self, username: str, password: str) -> None:
        log.info("Logging in.....Please wait :)")
        self.browser.get("https://www.linkedin.com/login?trk=guest_homepage-basic_nav-header-signin")
        try:
            time.sleep(random.uniform(1, 3))
            #time.sleep(20)

            user_field = self.browser.find_element("id", "username")
            pw_field = self.browser.find_element("id", "password")

            for char in username:
                user_field.send_keys(char)
                time.sleep(random.uniform(0.05, 0.2))

            time.sleep(random.uniform(0.5, 1.5))

            user_field.send_keys(Keys.TAB)
            time.sleep(random.uniform(0.5, 1.5))

            for char in password:
                pw_field.send_keys(char)
                time.sleep(random.uniform(0.05, 0.2))

            time.sleep(random.uniform(1, 2))
            #time.sleep(15)

            login_button = self.browser.find_element("xpath", '//button[@type="submit"]')

            actions = ActionChains(self.browser)
            actions.move_to_element(login_button).perform()
            time.sleep(random.uniform(0.2, 0.5))

            login_button.click()
            #time.sleep(random.uniform(5, 10))
            time.sleep(30)

        except TimeoutException:
            log.info("TimeoutException! Username/password field or login button not found")

    def get_driver(self) -> webdriver.Chrome:
        return self.browser

    def close(self) -> None:
        try:
            self.browser.quit()
        except Exception:
            pass

    def is_present(self, locator) -> bool:
        return len(self.browser.find_elements(locator[0], locator[1])) > 0

    def get_elements(self, key: str) -> list:
        locator = self.locator[key]
        if self.is_present(locator):
            return self.browser.find_elements(locator[0], locator[1])
        return []

    def load_page(self, sleep=1):
        scroll_page = 0
        while scroll_page < 2000:
            self.browser.execute_script("window.scrollTo(0," + str(scroll_page) + " );")
            scroll_page += 500
            time.sleep(sleep)

        if sleep != 1:
            self.browser.execute_script("window.scrollTo(0,0);")
            time.sleep(sleep)

        page = BeautifulSoup(self.browser.page_source, "lxml")
        return page

    def open_search_page(
        self,
        position: str,
        location_param: str,
        start: int,
        date_posted_days: int,
        easy_apply_only: bool,
        experience_level: list[int] | list = [],
    ) -> None:
        experience_level_str = ",".join(map(str, experience_level)) if experience_level else ""
        experience_level_param = f"&f_E={experience_level_str}" if experience_level_str else ""

        date_posted_param = ""
        if date_posted_days and int(date_posted_days) > 0:
            seconds = int(date_posted_days) * 24 * 60 * 60 
            date_posted_param = f"&f_TPR=r{seconds}"

        easy_apply_param = "&f_LF=f_AL" if easy_apply_only else ""
        sort_param = "&sortBy=R"
        url = (
            "https://www.linkedin.com/jobs/search/?"
            + easy_apply_param
            + date_posted_param
            + sort_param
            + "&keywords="
            + position
            + location_param
            + "&start="
            + str(start)
            + experience_level_param
        )
        log.info(
            f"Loading jobs page: start={start}, date_posted_days={date_posted_days or 'ALL'}, easy_apply_only={easy_apply_only}"
        )
        self.browser.get(url)
        self.load_page()

    def collect_job_ids_from_result(self, blacklist: list[str], saved_ids: set[str]) -> list[str]:
        job_ids: list[str] = []
        if not self.is_present(self.locator["links"]):
            return job_ids

        links = self.get_elements("links")
        for link in links:
            try:
                if "Applied" in link.text:
                    continue
                if link.text in blacklist:
                    continue

                jobID = link.get_attribute("data-job-id")
                if not jobID or jobID == "search":
                    continue
                if str(jobID) in saved_ids:
                    continue

                job_ids.append(str(jobID))
            except Exception:
                continue

        job_ids = list(dict.fromkeys(job_ids))
        log.info(f"Found {len(job_ids)} new jobIDs on this page")
        return job_ids

    # =========================================================
    # ✅ NEW: helpers for stability / authwall detection
    # =========================================================
    def _is_authwall_or_checkpoint(self) -> bool:
        """
        Detect LinkedIn authwall/checkpoint/login challenge pages.
        Helps classify 'cannot fetch' due to being blocked.
        """
        try:
            url = (self.browser.current_url or "").lower()
            title = (self.browser.title or "").lower()
            if any(x in url for x in ["checkpoint", "authwall", "login", "challenge"]):
                return True
            if any(x in title for x in ["sign in", "security verification", "authentication"]):
                return True

            body = ""
            try:
                body = (self.browser.find_element(By.TAG_NAME, "body").text or "").lower()
            except Exception:
                pass
            if any(x in body for x in ["security verification", "confirm your identity", "sign in to linkedin"]):
                return True
        except Exception:
            return False
        return False

    def wait_company_link_ready(self, timeout: int = 10) -> bool:
        """
        Wait until a /company/ link appears on job detail page.
        This avoids random 'cannot fetch' due to async rendering.
        """
        css_candidates = [
            "a.topcard__org-name-link",
            "a.jobs-unified-top-card__company-name",
            "a[data-control-name='company_link']",
            "a.topcard__flavor--black-link",
        ]

        end = time.time() + timeout
        last_href = ""
        while time.time() < end:
            if self._is_authwall_or_checkpoint():
                return False

            for css in css_candidates:
                try:
                    els = self.browser.find_elements(By.CSS_SELECTOR, css)
                    for el in els:
                        href = (el.get_attribute("href") or "").strip()
                        if href:
                            last_href = href
                        if "/company/" in href:
                            return True
                except Exception:
                    continue

            try:
                anchors = self.browser.find_elements(By.XPATH, "//a[contains(@href,'/company/')]")
                for a in anchors:
                    href = (a.get_attribute("href") or "").strip()
                    if href:
                        last_href = href
                    if "linkedin.com/company/" in href:
                        return True
            except Exception:
                pass

            time.sleep(0.3)

        if last_href:
            log.debug(f"[wait_company_link_ready] timeout but last_href={last_href}")
        return False

    # -------- job view parsing --------
    def open_job_view(self, job_id: str) -> str:
        job_url = f"https://www.linkedin.com/jobs/view/{job_id}"
        self.browser.get(job_url)

        # Wait body
        try:
            WebDriverWait(self.browser, 12).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        except Exception:
            pass

        # Wait company link (key improvement)
        self.wait_company_link_ready(timeout=10)

        return job_url

    def safe_text(self, by: By, selector: str, many: bool = False) -> str:
        try:
            if many:
                els = self.browser.find_elements(by, selector)
                txt = " ".join([e.text.strip() for e in els if e.text.strip()])
                return txt.strip()
            el = self.browser.find_element(by, selector)
            return el.text.strip()
        except Exception:
            return ""

    def parse_job_page_fields(self) -> tuple[str, str, str, str, str, str]:
        title = self.safe_text(By.CSS_SELECTOR, "h1")
        if not title:
            title = (self.browser.title or "").split(" | ")[0].strip()

        company = self.safe_text(By.CSS_SELECTOR, "a.topcard__org-name-link")
        if not company:
            company = self.safe_text(By.CSS_SELECTOR, "span.topcard__flavor a")
        if not company:
            parts = (self.browser.title or "").split(" | ")
            if len(parts) >= 2:
                company = parts[1].strip()

        location = ""
        try:
            bullets = self.browser.find_elements(By.CSS_SELECTOR, "span.topcard__flavor--bullet")
            bullet_texts = [b.text.strip() for b in bullets if b.text.strip()]
            if bullet_texts:
                location = bullet_texts[0]
        except Exception:
            location = ""

        workplace_type = ""
        seniority = ""
        employment_type = ""
        try:
            criteria_items = self.browser.find_elements(By.CSS_SELECTOR, "li.description__job-criteria-item")
            for item in criteria_items:
                txt = item.text.strip()
                lines = [x.strip() for x in txt.split("\n") if x.strip()]
                if len(lines) >= 2:
                    k, v = lines[0].lower(), lines[1]
                    if "workplace type" in k:
                        workplace_type = v
                    elif "seniority level" in k:
                        seniority = v
                    elif "employment type" in k:
                        employment_type = v
        except Exception:
            pass

        return (
            title or "",
            company or "",
            location or "",
            workplace_type or "",
            seniority or "",
            employment_type or "",
        )

    def has_easy_apply(self) -> bool:
        try:
            buttons = self.get_elements("easy_apply_button")
            for b in buttons:
                if "Easy Apply" in (b.text or ""):
                    return True
            if "Easy Apply" in self.browser.page_source:
                return True
        except Exception:
            pass
        return False

    # =========================================================
    # ✅ NEW: job detail -> company url -> about -> (size, members)
    # =========================================================
    def extract_company_url_from_job_detail(self) -> str:
        """
        Extract raw company URL from current job detail page.
        Might be /company/<slug>/life -> we'll normalize later.
        """
        css_candidates = [
            "a.topcard__org-name-link",
            "a.jobs-unified-top-card__company-name",
            "a[data-control-name='company_link']",
            "a.topcard__flavor--black-link",
        ]
        for css in css_candidates:
            try:
                els = self.browser.find_elements(By.CSS_SELECTOR, css)
                for el in els:
                    href = (el.get_attribute("href") or "").strip()
                    if "/company/" in href:
                        return href
            except Exception:
                continue

        try:
            anchors = self.browser.find_elements(By.XPATH, "//a[contains(@href,'/company/')]")
            for a in anchors:
                href = (a.get_attribute("href") or "").strip()
                if "linkedin.com/company/" in href:
                    return href
        except Exception:
            pass

        return ""

    def _fetch_company_size_from_about_dtdd(self) -> str:
        """
        More robust company size extraction:
        - avoids zip(dt, dd) misalignment by using following-sibling dd
        - includes authwall detection
        """
        if self._is_authwall_or_checkpoint():
            return ""

        # Wait body
        try:
            WebDriverWait(self.browser, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        except Exception:
            pass

        # dt -> following-sibling dd
        try:
            dts = self.browser.find_elements(By.CSS_SELECTOR, "dt")
            for dt in dts:
                key = _normalize_space(dt.text).lower()
                if key != "company size":
                    continue
                try:
                    dd = dt.find_element(By.XPATH, "following-sibling::dd[1]")
                    val = _normalize_space(dd.text)
                    if val:
                        return val
                except Exception:
                    continue
        except Exception:
            pass

        # regex fallback
        try:
            body_txt = self.browser.find_element(By.TAG_NAME, "body").text
            m = re.search(
                r"Company size\s*\n\s*([0-9,]+\s*-\s*[0-9,]+\s+employees|[0-9,]+\+\s+employees)",
                body_txt,
                re.I,
            )
            if m:
                return _normalize_space(m.group(1))
        except Exception:
            pass

        return ""

    def _fetch_associated_members_from_anchor(self) -> str:
        xpaths = [
            "//a[contains(@href,'/search/results/people/') and contains(@href,'currentCompany=') and contains(@href,'origin=COMPANY_PAGE_CANNED_SEARCH')]",
            "//a[contains(@href,'/search/results/people/') and contains(@href,'currentCompany=')]",
        ]

        for xp in xpaths:
            try:
                els = self.browser.find_elements(By.XPATH, xp)
                for el in els:
                    t = _normalize_space(el.text or "")
                    m = re.search(r"([0-9,]+)\s+associated\s+members", t, re.I)
                    if m:
                        return f"{m.group(1)} associated members"
            except Exception:
                continue

        try:
            body_txt = self.browser.find_element(By.TAG_NAME, "body").text
            m = re.search(r"([0-9,]+)\s+associated\s+members", body_txt, re.I)
            if m:
                return f"{m.group(1)} associated members"
        except Exception:
            pass

        return ""

    def fetch_company_about_fields_from_current_job(self) -> tuple[str, str, str]:
        """
        Assumes we are already on job detail page.
        Returns: (about_url, company_size, associated_members)
        On failure: ("cannot fetch", "cannot fetch", "cannot fetch")
        """
        # ✅关键：先等 company link ready，避免 job 页异步渲染导致 raw="" 的偶发失败
        self.wait_company_link_ready(timeout=10)

        raw = self.extract_company_url_from_job_detail()
        company_root = _normalize_company_url(raw)
        if not company_root:
            log.debug(f"[company_about] cannot extract company url. raw={raw!r} cur={self.browser.current_url}")
            return "cannot fetch", "cannot fetch", "cannot fetch"

        about_url = company_root.rstrip("/") + "/about/"
        try:
            self.browser.get(about_url)
            WebDriverWait(self.browser, 12).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            time.sleep(random.uniform(5, 15))

            # ✅如果被 authwall / checkpoint，直接返回（about_url 有意义，size/members 失败）
            if self._is_authwall_or_checkpoint():
                log.warning(f"[company_about] blocked/authwall at {self.browser.current_url}")
                return about_url, "cannot fetch", "cannot fetch"

        except Exception as e:
            log.warning(f"[company_about] about page load failed url={about_url} err={e}")
            return about_url, "cannot fetch", "cannot fetch"

        size = self._fetch_company_size_from_about_dtdd()
        members = self._fetch_associated_members_from_anchor()

        return (
            about_url if about_url else "cannot fetch",
            size if size else "cannot fetch",
            members if members else "cannot fetch",
        )


# =========================================================
# Requests API Layer (cookies from selenium)
# =========================================================
class LinkedInAPIClient:
    def __init__(self, driver: webdriver.Chrome):
        self.driver = driver
        self.session: requests.Session | None = None
        self.cookie_refresh_every = 20
        self._refresh_session()

    def _refresh_session(self) -> None:
        s = requests.Session()
        try:
            ua = self.driver.execute_script("return navigator.userAgent;") or "Mozilla/5.0"
        except Exception:
            ua = "Mozilla/5.0"

        s.headers.update(
            {
                "User-Agent": ua,
                "Accept-Language": "en-US,en;q=0.9",
                "Accept": "*/*",
                "Referer": "https://www.linkedin.com/jobs/",
                "Connection": "keep-alive",
            }
        )

        try:
            for c in self.driver.get_cookies():
                s.cookies.set(c["name"], c["value"], domain=c.get("domain"), path=c.get("path", "/"))
        except Exception as e:
            log.warning(f"Failed to read selenium cookies: {e}")

        jsid = s.cookies.get("JSESSIONID")
        if jsid:
            s.headers["csrf-token"] = jsid.strip('"')

        self.session = s

    @staticmethod
    def _html_to_text(html: str) -> str:
        soup = BeautifulSoup(html or "", "html.parser")
        return soup.get_text("\n", strip=True)

    @staticmethod
    def _find_long_strings(obj: Any, min_len: int = 200) -> list[tuple[str, str]]:
        hits: list[tuple[str, str]] = []

        def walk(x, path=""):
            if isinstance(x, dict):
                for k, v in x.items():
                    walk(v, f"{path}.{k}" if path else k)
            elif isinstance(x, list):
                for i, v in enumerate(x):
                    walk(v, f"{path}[{i}]")
            elif isinstance(x, str):
                s = x.strip()
                if len(s) >= min_len:
                    hits.append((path, s))

        walk(obj)
        hits.sort(key=lambda t: len(t[1]), reverse=True)
        return hits

    @staticmethod
    def _format_ms_epoch(ms: int | float) -> str:
        try:
            return datetime.fromtimestamp(float(ms) / 1000.0).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return ""

    def _find_first_ms_by_keys_recursive(self, obj: Any, keys: tuple[str, ...]) -> int | float | None:
        if isinstance(obj, dict):
            for k in keys:
                v = obj.get(k)
                if isinstance(v, (int, float)) and v > 0:
                    return v
            for v in obj.values():
                hit = self._find_first_ms_by_keys_recursive(v, keys)
                if hit is not None:
                    return hit
        elif isinstance(obj, list):
            for v in obj:
                hit = self._find_first_ms_by_keys_recursive(v, keys)
                if hit is not None:
                    return hit
        return None

    def _extract_posted_at(self, data: dict) -> str:
        ts = self._find_first_ms_by_keys_recursive(data, ("listedAt",))
        if ts is None:
            ts = self._find_first_ms_by_keys_recursive(data, ("originalListedAt",))
        if ts is None:
            return ""
        return self._format_ms_epoch(ts)

    def fetch_guest_jd(self, job_id: str) -> str:
        if self.session is None:
            self._refresh_session()

        url = f"https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}"
        try:
            r = self.session.get(url, timeout=25, allow_redirects=True)
        except Exception as e:
            log.warning(f"[guest] request failed job={job_id}: {e}")
            return ""

        if r.status_code != 200 or not (r.text or "").strip():
            log.warning(f"[guest] status={r.status_code} job={job_id} final_url={getattr(r, 'url', '')}")
            return ""

        soup = BeautifulSoup(r.text, "html.parser")
        candidates = [
            soup.select_one("div.show-more-less-html__markup"),
            soup.select_one("div.description__text"),
            soup.select_one("section.description"),
            soup.select_one("div.jobs-description__content"),
        ]
        for c in candidates:
            if c:
                txt = c.get_text("\n", strip=True)
                if len(txt.strip()) > 80:
                    return txt.strip()

        txt = self._html_to_text(r.text)
        return txt if len(txt.strip()) > 80 else ""

    def fetch_voyager_json(self, job_id: str) -> dict:
        if self.session is None:
            self._refresh_session()

        headers = dict(self.session.headers)
        headers["Accept"] = "application/json"

        url = f"https://www.linkedin.com/voyager/api/jobs/jobPostings/{job_id}"
        try:
            r = self.session.get(url, headers=headers, timeout=25, allow_redirects=True)
        except Exception as e:
            log.warning(f"[voyager] request failed job={job_id}: {e}")
            return {}

        if r.status_code != 200 or not (r.text or "").strip():
            log.warning(f"[voyager] status={r.status_code} job={job_id} final_url={getattr(r, 'url', '')}")
            return {}

        raw = (r.text or "").strip()
        raw = re.sub(r"^for\s*\(\s*;;\s*\);\s*", "", raw)
        try:
            return json.loads(raw)
        except Exception:
            try:
                return r.json()
            except Exception:
                return {}

    def fetch_jd_and_posted_at(self, job_id: str, idx: int) -> tuple[str, str]:
        if idx % self.cookie_refresh_every == 0:
            self._refresh_session()

        jd_guest = self.fetch_guest_jd(job_id)

        data = self.fetch_voyager_json(job_id)
        posted_at = self._extract_posted_at(data) if data else ""

        jd_voyager = ""
        if data:
            hits = self._find_long_strings(data, min_len=200)
            best = ""
            for path, s in hits[:200]:
                if "description" in path.lower():
                    best = s
                    break
            if not best and hits:
                best = hits[0][1]

            if best:
                best = ihtml.unescape(best)
                if "<" in best and ">" in best:
                    best = BeautifulSoup(best, "html.parser").get_text("\n", strip=True)
                best = re.sub(r"\n{3,}", "\n\n", best).strip()
                jd_voyager = best if len(best) > 80 else ""

        jd_final = jd_guest if jd_guest else jd_voyager
        return jd_final or "", posted_at or ""


# =========================================================
# Orchestrator
# =========================================================
class JobCollector:
    MAX_SEARCH_TIME = 60 * 60 * 2

    def __init__(
        self,
        username: str,
        password: str,
        filename: str,
        blacklist: list[str],
        blackListTitles: list[str],
        experience_level: list[int],
        date_posted_days: int,
        stop_old_posted_enabled: bool,
        stop_old_posted_consecutive_limit: int,
        stop_old_posted_days_threshold: int,
        easy_apply_only: bool,
        # ✅ Stop C (combo-level empty pages)
        stop_empty_pages_enabled: bool,
        stop_empty_pages_consecutive_limit: int,
        # ✅ NEW
        fetch_company_about_enabled: bool = True,
    ):
        self.run_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.date_posted_days = int(date_posted_days or 7)

        self.blacklist = blacklist
        self.blackListTitles = blackListTitles
        self.experience_level = experience_level or []

        self.easy_apply_only = bool(easy_apply_only)
        self.fetch_company_about_enabled = bool(fetch_company_about_enabled)

        # ✅ Stop B
        self.stop_old_posted_enabled = bool(stop_old_posted_enabled)
        self.stop_old_posted_consecutive_limit = max(1, int(stop_old_posted_consecutive_limit or 10))
        thr = int(stop_old_posted_days_threshold or 0)
        self.stop_old_posted_days_threshold = thr if thr > 0 else self.date_posted_days

        # ✅ Stop C
        self.stop_empty_pages_enabled = bool(stop_empty_pages_enabled)
        self.stop_empty_pages_consecutive_limit = max(1, int(stop_empty_pages_consecutive_limit or 3))

        # ✅ JobStore will auto-upgrade old CSV schema to include the 3 new columns
        self.store = JobStore(filename)

        self.browser = LinkedInBrowser()
        self.browser.login(username, password)
        self.api = LinkedInAPIClient(self.browser.get_driver())

        log.info(f"Run started at: {self.run_at}")
        log.info(f"Search easy_apply_only={self.easy_apply_only}")
        log.info(f"FetchCompanyAbout enabled={self.fetch_company_about_enabled}")
        log.info(
            f"StopB enabled={self.stop_old_posted_enabled}, "
            f"consecutive_limit={self.stop_old_posted_consecutive_limit}, "
            f"days_threshold={self.stop_old_posted_days_threshold}"
        )
        log.info(
            f"StopC enabled={self.stop_empty_pages_enabled}, "
            f"empty_pages_consecutive_limit={self.stop_empty_pages_consecutive_limit}"
        )

    def close(self):
        self.browser.close()

    def start(self, positions: list[str], locations: list[str]):
        combos = [(pos, loc) for pos in positions for loc in locations]
        random.shuffle(combos)

        log.info(f"Total {len(combos)} combinations to process in random order")

        for position, location in combos:
            log.info(f"Searching jobs for {position} @ {location}")
            self._search_one_combo(position, "&location=" + location)

    def _search_one_combo(self, position: str, location_param: str):
        jobs_per_page = 0
        start_time = time.time()

        empty_page_streak = 0

        self.browser.open_search_page(
            position=position,
            location_param=location_param,
            start=jobs_per_page,
            date_posted_days=self.date_posted_days,
            easy_apply_only=self.easy_apply_only,
            experience_level=self.experience_level,
        )

        while time.time() - start_time < self.MAX_SEARCH_TIME:
            try:
                mins_left = int((self.MAX_SEARCH_TIME - (time.time() - start_time)) // 60)
                log.info(f"{mins_left} minutes left in this search")

                self.browser.load_page(sleep=0.5)

                if self.browser.is_present(self.browser.locator["search"]):
                    scrollresults = self.browser.get_elements("search")
                    for i in range(300, 3000, 100):
                        self.browser.browser.execute_script("arguments[0].scrollTo(0, {})".format(i), scrollresults[0])

                jobIDs = self.browser.collect_job_ids_from_result(self.blacklist, self.store.savedJobIDs)

                # ✅ Stop C
                if self.stop_empty_pages_enabled:
                    if not jobIDs:
                        empty_page_streak += 1
                        log.info(
                            f"[StopC] empty_page_streak={empty_page_streak}/{self.stop_empty_pages_consecutive_limit} "
                            f"combo={position} @ {location_param.replace('&location=', '')} start={jobs_per_page}"
                        )
                    else:
                        if empty_page_streak > 0:
                            log.info("[StopC] streak reset (found new jobIDs)")
                        empty_page_streak = 0

                    if empty_page_streak >= self.stop_empty_pages_consecutive_limit:
                        log.info(
                            f"🛑 [StopC triggered] {empty_page_streak} consecutive empty pages for combo "
                            f"{position} @ {location_param.replace('&location=', '')}"
                        )
                        return

                if jobIDs:
                    should_stop_combo = self._collect_jobs(
                        jobIDs,
                        search_position=position,
                        search_location=location_param.replace("&location=", ""),
                    )
                    if should_stop_combo:
                        log.info(f"🛑 Stop paging current combo: {position} @ {location_param.replace('&location=', '')}")
                        return

                jobs_per_page += 25
                self.browser.open_search_page(
                    position=position,
                    location_param=location_param,
                    start=jobs_per_page,
                    date_posted_days=self.date_posted_days,
                    easy_apply_only=self.easy_apply_only,
                    experience_level=self.experience_level,
                )
            except Exception as e:
                log.warning(f"Loop exception: {e}")

    def _parse_posted_at(self, posted_at: str) -> datetime | None:
        if not posted_at:
            return None
        try:
            return datetime.strptime(posted_at, "%Y-%m-%d %H:%M:%S")
        except Exception:
            return None

    def _is_older_than_threshold(self, posted_at: str) -> bool:
        dt = self._parse_posted_at(posted_at)
        if dt is None:
            return False
        delta_days = (datetime.now() - dt).days
        return delta_days > int(self.stop_old_posted_days_threshold)

    def _collect_jobs(self, jobIDs: list[str], search_position: str, search_location: str) -> bool:
        old_job_streak = 0

        for idx, jobID in enumerate(jobIDs):
            try:
                if self.store.has(jobID):
                    continue

                job_url = self.browser.open_job_view(jobID)

                title, company, location, workplace_type, seniority, employment_type = self.browser.parse_job_page_fields()
                easy_apply = self.browser.has_easy_apply()

                jd, posted_at = self.api.fetch_jd_and_posted_at(jobID, idx)

                # ✅ NEW: company about fields (about_url, company_size, associated_members)
                company_about_url = "cannot fetch"
                company_size = "cannot fetch"
                associated_members = "cannot fetch"

                if self.fetch_company_about_enabled:
                    try:
                        company_about_url, company_size, associated_members = (
                            self.browser.fetch_company_about_fields_from_current_job()
                        )

                        # 跳回 job_url 后：等 company link 再继续（更稳）
                        self.browser.browser.get(job_url)
                        try:
                            WebDriverWait(self.browser.browser, 12).until(
                                EC.presence_of_element_located((By.TAG_NAME, "body"))
                            )
                        except Exception:
                            pass
                        self.browser.wait_company_link_ready(timeout=8)

                        time.sleep(random.uniform(0.3, 1.2))
                    except Exception as e:
                        log.warning(f"[company_about] cannot fetch job={jobID} err={e}")

                # Title blacklist
                if self.blackListTitles and title:
                    if any(word.lower() in title.lower() for word in self.blackListTitles):
                        log.info(f"⛔ Skipped (title has blacklisted keyword): {title}")
                        continue

                rec = JobRecord(
                    run_at=self.run_at,
                    posted_at=posted_at,
                    timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    jobID=str(jobID),
                    title=title,
                    company=company,
                    location=location,
                    workplace_type=workplace_type,
                    seniority=seniority,
                    employment_type=employment_type,
                    easy_apply=easy_apply,
                    job_description=jd,
                    job_url=job_url,
                    search_position=search_position,
                    search_location=search_location,
                    company_about_url=company_about_url or "cannot fetch",
                    company_size=company_size or "cannot fetch",
                    associated_members=associated_members or "cannot fetch",
                )
                self.store.add(rec)

                #random_delay = random.uniform(0.1, 5.0)
                random_delay = random.uniform(10, 40)
                time.sleep(random_delay)

                # ✅ Stop B
                if self.stop_old_posted_enabled:
                    if posted_at and self._is_older_than_threshold(posted_at):
                        old_job_streak += 1
                        log.info(
                            f"[StopB] old_streak={old_job_streak}/{self.stop_old_posted_consecutive_limit} "
                            f"job={jobID} posted_at={posted_at} threshold_days={self.stop_old_posted_days_threshold}"
                        )
                    else:
                        if old_job_streak > 0:
                            log.info(f"[StopB] streak reset (posted_at={posted_at or 'EMPTY'})")
                        old_job_streak = 0

                    if old_job_streak >= self.stop_old_posted_consecutive_limit:
                        log.info(
                            f"🛑 [StopB triggered] {old_job_streak} consecutive jobs older than "
                            f"{self.stop_old_posted_days_threshold} days for combo {search_position} @ {search_location}"
                        )
                        return True

                if not posted_at:
                    log.warning(f"[posted_at empty] job={jobID} title={title}")

            except Exception as e:
                log.warning(f"Failed to collect job {jobID}: {e}")

        return False


# =========================================================
# Main
# =========================================================
if __name__ == "__main__":
    setupLogger()

    with open("config.yaml", "r") as stream:
        try:
            parameters = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            raise exc

    assert len(parameters["positions"]) > 0
    assert len(parameters["locations"]) > 0
    assert parameters["username"] is not None
    assert parameters["password"] is not None
    assert parameters["phone_number"] is not None  # kept (even if not used)

    if "uploads" in parameters.keys() and type(parameters["uploads"]) == list:
        raise Exception(
            "uploads read from the config file appear to be in list format"
            + " while should be dict. Try removing '-' from line containing"
            + " filename & path"
        )

    output_filename: list = [f for f in parameters.get("output_filename", ["output.csv"]) if f is not None]
    output_filename: str = output_filename[0] if len(output_filename) > 0 else "output.csv"

    blacklist = parameters.get("blacklist", [])
    blackListTitles = parameters.get("blackListTitles", [])

    locations: list = [l for l in parameters["locations"] if l is not None]
    positions: list = [p for p in parameters["positions"] if p is not None]

    # ✅ Stop B config from YAML
    stop_cfg = parameters.get("stop_old_posted", {}) or {}
    stop_enabled = bool(stop_cfg.get("enabled", False))
    stop_consecutive = int(stop_cfg.get("consecutive_limit", 10) or 10)
    stop_days_threshold = int(stop_cfg.get("days_threshold", 0) or 0)

    # ✅ easy apply toggle from YAML
    search_cfg = parameters.get("search", {}) or {}
    easy_apply_only = bool(search_cfg.get("easy_apply_only", True))

    # ✅ Stop C config from YAML
    empty_cfg = parameters.get("stop_empty_pages", {}) or {}
    empty_enabled = bool(empty_cfg.get("enabled", True))
    empty_limit = int(empty_cfg.get("consecutive_limit", 3) or 3)

    # ✅ company_about toggle from YAML (optional)
    # company_about:
    #   enabled: true
    company_about_cfg = parameters.get("company_about", {}) or {}
    fetch_company_about_enabled = bool(company_about_cfg.get("enabled", True))

    collector = JobCollector(
        username=parameters["username"],
        password=parameters["password"],
        filename=output_filename,
        blacklist=blacklist,
        blackListTitles=blackListTitles,
        experience_level=parameters.get("experience_level", []),
        date_posted_days=parameters.get("date_posted_days", 0),
        stop_old_posted_enabled=stop_enabled,
        stop_old_posted_consecutive_limit=stop_consecutive,
        stop_old_posted_days_threshold=stop_days_threshold,
        easy_apply_only=easy_apply_only,
        stop_empty_pages_enabled=empty_enabled,
        stop_empty_pages_consecutive_limit=empty_limit,
        fetch_company_about_enabled=fetch_company_about_enabled,
    )

    try:
        collector.start(positions, locations)
    finally:
        collector.close()
