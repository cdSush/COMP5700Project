import pandas as pd

df_all = pd.read_parquet("hf://datasets/hao-li/AIDev/all_pull_request.parquet")
df = df_all[["title", "id", "agent", "body", "repo_id", "repo_url"]].copy()

df = df.rename(columns={
    "title" : "TITLE",
    "id" : "ID",
    "agent" : "AGENTNAME",
    "body" : "BODYSTRING",
    "repo_id" : "REPOID",
    "repo_url" : "REPOURL"
})

df["BODYSTRING"] = (
    df["BODYSTRING"]
    .fillna("")                                 # no NaN values
    .str.strip()                                # remove leading/trailing whitespace
    .str.replace(r"\s*\n\s*", " ", regex=True)  # flatten multi-line text
    .str.replace("\t", " ")                     # remove tabs
)

df.to_csv("all_pull_request.csv", index=False, encoding="utf-8-sig")