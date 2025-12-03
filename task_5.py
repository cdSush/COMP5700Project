import pandas as pd
from datasets import load_dataset
import re 


# Initialize datasets
df_pulls = load_dataset("hao-li/AIDev", "all_pull_request")
df_repo = load_dataset("hao-li/AIDev", "all_repository")
df_task = load_dataset("hao-li/AIDev", "pr_task_type")
df_commit = load_dataset("hao-li/AIDev", "pr_commit_details")

#Convert to Dfs
df_pulls_pd = df_pulls["train"].to_pandas()
df_repo_pd = df_repo["train"].to_pandas()
df_task_pd = df_task["train"].to_pandas()
df_commit_pd = df_commit["train"].to_pandas()


#Temp DataFrame 
df_task1 = df_pulls_pd[["title", "id", "agent", "body"]].copy()
df_task1.columns = ["TITLE", "ID", "AGENT", "BODYSTRING"]

# Pull task 3 data into temp
df_task3 = df_task_pd[["id", "title", "reason", "type", "confidence"]].copy()
df_task3.columns = ["PRID", "PRTITLE", "PRREASON", "PRTYPE", "CONFIDENCE"]

# pull task4 data into temp
df_task4 = df_commit_pd[["pr_id", "message", "filename", "patch"]].copy()
df_task4.columns = ["PRID", "PRCOMMITMESSAGE", "PRFILE", "PRDIFF"]


# Filter unusable from data coming in from task 4
def filter(content: str) -> str:
    if pd.isna(content):
        return None
    content = str(content)
    content = re.sub(r"[^\x00-\x7F]+", " ", content)
    content = content.replace("\r", " ").replace("\n", " ")
    content = re.sub(r"\s+", " ", content).strip()
    return content


df_task4["PRDIFF"] = df_task4["PRDIFF"].apply(filter)

# Combine commits to pull request and metadata
df_task4["TEXT_COMMIT"] = (
    df_task4[["PRCOMMITMESSAGE", "PRFILE", "PRDIFF"]]
    .astype(str)
    .agg(" ".join, axis=1)
)

df_commit_agg = (
    df_task4.groupby("PRID")["TEXT_COMMIT"]
    .apply(lambda x: " ".join(x))
    .reset_index()
)
#Final merge
df_combine = df_task1.merge(
    df_task3,
    left_on="ID",
    right_on="PRID",
    how="left",
)

df_combine = df_combine.merge(
    df_commit_agg,
    left_on="ID",
    right_on="PRID",
    how="left",
    suffixes=("", "_COMMIT"),
)

headers = ["TITLE", "BODYSTRING", "PRTITLE", "PRREASON", "TEXT_COMMIT"]

for col in headers:
    if col not in df_combine.columns:
        df_combine[col] = ""

df_combine["FULLTEXT"] = (
    df_combine[headers]
        .fillna("")
        .astype(str)
        .agg(" ".join, axis=1)
)

# Security Check
#*Keywords gathered from previous assignment plz verify

keywords = [
    "race",
    "racy",
    "buffer",
    "overflow",
    "stack",
    "integer",
    "signedness",
    "underflow",
    "improper",
    "unauthenticated",
    "gain access",
    "permission",
    "cross site",
    "css",
    "xss",
    "denial service",
    "dos",
    "crash",
    "deadlock",
    "injection",
    "request forgery",
    "csrf",
    "xsrf",
    "forged",
    "security",
    "vulnerability",
    "vulnerable",
    "exploit",
    "attack",
    "bypass",
    "backdoor",
    "threat",
    "expose",
    "breach",
    "violate",
    "fatal",
    "blacklist",
    "overrun",
    "insecure",
]

wordlist = re.compile(
    "|".join(re.escape(i) for i in keywords),
    re.IGNORECASE,
)


def security_check(text: str) -> int:
    if not isinstance(text, str):
        text = "" if pd.isna(text) else str(text)
    return 1 if wordlist.search(text) else 0


df_combine["SECURITY"] = df_combine["FULLTEXT"].apply(security_check)

# task5 build
df_task5 = df_combine[["ID", "AGENT", "PRTYPE", "CONFIDENCE", "SECURITY"]].copy()
df_task5.columns = ["ID", "AGENT", "TYPE", "CONFIDENCE", "SECURITY"]

df_task5.to_csv("task5.csv", index=False)
print("'task5.csv' created.")