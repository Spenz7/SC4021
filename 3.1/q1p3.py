"""
#REV for counting rec frm jsonl file
import json

JSONL_FILE = "jsonl_crawl_manual/reddit_manual.jsonl"

total_records = 0
total_words = 0
unique_words = set()
seen_ids = set()

with open(JSONL_FILE, "r", encoding="utf-8") as f:
    for line in f:
        try:
            record = json.loads(line)
            record_id = record.get("id")
            if record_id in seen_ids:
                continue  # skip duplicate
            seen_ids.add(record_id)
            
            text = record.get("text", "")
            words = text.split()
            total_records += 1
            total_words += len(words)
            unique_words.update(words)
        except json.JSONDecodeError:
            continue

print(f"Total records (unique): {total_records}")
print(f"Total words: {total_words}")
print(f"Unique word types: {len(unique_words)}")
"""

"""
# REV for counting records containing "climate change" from jsonl file
# Count unique "climate change" records and unique words cleanly
# Count unique "climate change" records exactly like Excel
import json
import re
import string

JSONL_FILE = "jsonl_crawl_manual/reddit_manual.jsonl"

total_records = 0
total_words = 0
unique_words = set()
seen_ids = set()
seen_texts = set()  # for deduplication by normalized text

translator = str.maketrans("", "", string.punctuation)

def normalize_text(text):
    # Strip leading/trailing whitespace
    text = text.strip()
    # Collapse multiple inner spaces to a single space
    text = re.sub(r"\s+", " ", text)
    return text.lower()  # Excel usually ignores case

with open(JSONL_FILE, "r", encoding="utf-8") as f:
    for line in f:
        try:
            record = json.loads(line)
            record_id = record.get("id")
            if record_id in seen_ids:
                continue  # skip duplicate by id
            seen_ids.add(record_id)
            
            text = record.get("text", "")
            if "climate change" not in text.lower():
                continue

            text_norm = normalize_text(text)
            if text_norm in seen_texts:
                continue  # skip duplicate by normalized text
            seen_texts.add(text_norm)

            # Count words (punctuation removed, lowercase)
            words = [w.translate(translator).lower() for w in text_norm.split() if w.strip()]
            total_records += 1
            total_words += len(words)
            unique_words.update(words)

        except json.JSONDecodeError:
            continue

print(f"Total records (unique, relevant): {total_records}")
print(f"Total words: {total_words}")
print(f"Unique word types: {len(unique_words)}")

"""

#REV count stats frm 1k records that u crawled
import pandas as pd
import re
import string

# --- Path to your Excel file ---
EXCEL_FILE = r"C:/Users/spenc/OneDrive - Nanyang Technological University/Y2S2/sc4021/prj/SC4021-clean/3.1/obtain1kEvalCrawl/eval1kFinal.xlsx"

# --- Load Excel without headers ---
df = pd.read_excel(EXCEL_FILE, header=None)

# Column 0 contains the comments
texts = df[0].dropna().astype(str)

# Initialize counters
total_records = len(texts)
total_words = 0
unique_words = set()

translator = str.maketrans("", "", string.punctuation)

for text in texts:
    # normalize: strip, collapse spaces, lowercase
    text_norm = re.sub(r"\s+", " ", text.strip()).lower()
    words = [w.translate(translator) for w in text_norm.split() if w.strip()]
    total_words += len(words)
    unique_words.update(words)

print(f"Total records: {total_records}")
print(f"Total words: {total_words}")
print(f"Unique word types: {len(unique_words)}")
