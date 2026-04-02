import json
import pandas as pd
import os

# --- Configuration ---
INPUT_FILE = "jsonl_crawl_manual/reddit_manual.jsonl"  # path to your JSONL file
OUTPUT_FILE = "reddit_manual.xlsx"                      # desired Excel file

# --- Read JSONL ---
records = []
with open(INPUT_FILE, "r", encoding="utf-8") as f:
    for line in f:
        try:
            data = json.loads(line)
            record = {
                "id": data.get("id"),
                "text": data.get("text"),
                "timestamp": data.get("timestamp"),
                "subreddit": data.get("metadata", {}).get("subreddit"),
                "post_title": data.get("metadata", {}).get("post_title"),
                "url": data.get("metadata", {}).get("url")
            }
            records.append(record)
        except json.JSONDecodeError:
            continue

# --- Convert to DataFrame ---
df = pd.DataFrame(records)

# --- Save to Excel ---
df.to_excel(OUTPUT_FILE, index=False)
print(f"Saved {len(df)} records to {OUTPUT_FILE}")
