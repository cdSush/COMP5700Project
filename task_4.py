## Task 4: Commit Details Processing
## Andrea McCarroll

import pandas as pd
import re

"""
# --- 1. Define Cleaning Function ---
def clean_diff_patch(text):
    
    Cleans the diff patch text to remove special characters that might cause 
    CSV encoding issues, as required by the prompt.
    It replaces newlines and control characters with a space 
    and then encodes/decodes to strictly keep only printable ASCII characters.
    
    if pd.isna(text):
        return ""
    
    # Ensure it's treated as a string
    text = str(text)

    # Remove non-printable control characters, replacing with a space
    text = re.sub(r'[\x00-\x1F\x7F-\x9F]', ' ', text)
    
    # Convert to ASCII, ignoring any remaining non-standard characters, 
    # then decode back to a standard string format.
    return text.encode('ascii', 'ignore').decode('ascii')
"""

def clean_diff_patch(text: str) -> str:
    if pd.isna(text):
        return ""
    # Ensure we work with a string
    text = str(text)
    # Replace newlines and carriage-returns with spaces
    text = text.replace("\r", " ").replace("\n", " ")
    # Drop non-ASCII characters that can cause encoding errors
    text = re.sub(r"[^\x00-\x7F]+", " ", text)
    # Collapse runs of whitespace into a single space
    text = re.sub(r"\s+", " ", text).strip()
    return text


# --- 2. Load Data ---
print("Loading pr_commit_details.parquet...")
df_all = pd.read_parquet("hf://datasets/hao-li/AIDev/pr_commit_details.parquet")


# --- 3. Select Columns ---
df = df_all[[
    "pr_id", "sha", "message", "filename", "status", 
    "additions", "deletions", "changes", "patch"
]].copy()


# --- 4. Apply Cleaning and Renaming ---
print("Applying cleaning and renaming...")

# Apply the cleaning function to the 'patch' column
df["patch"] = df["patch"].apply(clean_diff_patch)


# Rename columns to match the required output headers
df = df.rename(columns={
    "pr_id": "PRID",
    "sha": "PRSHA",
    "message": "PRCOMMITMESSAGE",
    "filename": "PRFILE",
    "status": "PRSTATUS",
    "additions": "PRADDS",
    "deletions": "PRDELSS",  # Note: The required header is PRDELSS (double S)
    "changes": "PRCHANGECOUNT",
    "patch": "PRDIFF"
})


# --- 5. Save to CSV ---
OUTPUT_FILENAME = "pr_commit_details.csv"
df.to_csv(OUTPUT_FILENAME, index=False, encoding="utf-8-sig")
print(f"Task 4 completed. Output saved to {OUTPUT_FILENAME}")
