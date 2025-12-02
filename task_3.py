##Amelia Snyder
## Task 3

import pandas as pd

df_all = df = pd.read_parquet("hf://datasets/hao-li/AIDev/pr_task_type.parquet")
df = df_all[["id", "title", "reason", "type", "confidence"]].copy()

df = df.rename(columns={
    "id" : "PRID",
    "title" : "PRTITLE",
    "reason" : "PRREASON",
    "type" : "PRTYPE", 
    "confidence" : "CONFIDENCE"
})

df.to_csv("pr_task_type_request.csv", index=False, encoding="utf-8-sig")