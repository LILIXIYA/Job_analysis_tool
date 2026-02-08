# 🚀 Job_analysis_tool

This tool reduces the time and energy drain of job seekers, helping them focus on roles they qualify for and genuinely want.

The intention of this project is described here:

<img width="1199" height="633" alt="image" src="https://github.com/user-attachments/assets/e9c991d5-f8f1-4bb5-8772-9b5bbea678a5" />

Job seeking is a multi-selection process. Using this tool, job seekers can focus on the positions they qualify for and enjoy.

<img width="1187" height="692" alt="image" src="https://github.com/user-attachments/assets/21627554-36da-44bf-8ae3-698abd0e9765" />

---

## 🤔 Why this tool

There are some AI job seeking tools on the market. Most of these tools are:

1. **Profit-driven**
2. **Non-explainable**
3. **Not designed for job seeking as a multi-selection process**

For example, many tools may give you a score to quantify the match between you and a job, but no explanation is provided behind that score.

This tool provides **clear explanations**, and the selection is done in **two steps**, considering both:

- your **qualification**
- your **preference**

---

## 🏃 How to run

All dependencies are specified in `requirementx.txt`.
`pip install -r requirements.txt`

### Key scripts

- `Job_collection.py` is used for automatically scraping job postings.
- `LLM_postprocess_multithread.py` is used for LLM-based scoring.
- `Data_analysis.ipynb` is used for analyzing the results.

All parameters are configured in `config.yaml`.

---

### Run Job_collection.py

To run `Job_collection.py`, you need to provide a LinkedIn username and password (**use at your own risk**).

---

### Run LLM_postprocess_multithread.py

To run `LLM_postprocess_multithread.py`, you need to provide:

- `resume_path`
- `api_key`

Where:

- `resume_path` is the file path to the `.txt` version of your resume.
- `api_key` is the API key for the Qwen model.

The prompts for qualification and preference scoring can be changed in `LLM_postprocess_multithread.py`.

---

## 💰 Qwen API cost

The Qwen API is usage-based (pay-as-you-go).

Typically, processing around **1,000 job results costs about $5**.  
However, new users may receive free credits for certain models after registration.

---

## 📄 Expected outcome

A PDF and some relevant figures showing the jobs that you qualify for and actually like.

![2e291a1a3ccf0a691569f0cdf52db703](https://github.com/user-attachments/assets/715c7a73-59eb-4560-9ee5-5a22d20f6c6e)


---

## ⚠️ Disclaimer

The data collection part is inspired by:

https://github.com/brightdata/linkedin-job-hunting-assistant

This is a non-profit tool for education and study purposes only.





