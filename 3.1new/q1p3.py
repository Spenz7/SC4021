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
