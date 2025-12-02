## Amelia Snyder
## Task 2

import pandas as pd


df_all = pd.read_parquet("hf://datasets/hao-li/AIDev/all_repository.parquet")
df = df_all[["id", "language", "stars", "url"]].copy()

df = df.rename(columns={
    "id" : "ID",
    "language" : "LANG",
    "stars" : "STARS",
    "url" : "REPOURL"
})

df.to_csv("all_repo_request.csv", index=False, encoding="utf-8-sig")