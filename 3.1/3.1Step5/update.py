import json
import os

# Step 1: collect postIDs from technology_all.jsonl
post_ids_to_remove = set()
with open('jsonl_crawl_full/technology_all.jsonl', 'r', encoding='utf-8') as f:
    for line in f:
        record = json.loads(line)
        post_id = list(record.values())[0]  # assuming first field is the postID
        post_ids_to_remove.add(post_id)

# Step 2: load seen_posts.json
with open('seen_posts.json', 'r', encoding='utf-8') as f:
    seen_posts = json.load(f)

# Step 3: filter out postIDs
seen_posts = [pid for pid in seen_posts if pid not in post_ids_to_remove]

# Step 4: overwrite seen_posts.json
with open('seen_posts.json', 'w', encoding='utf-8') as f:
    json.dump(seen_posts, f, ensure_ascii=False, indent=2)

# Step 5: delete technology_all.jsonl
os.remove('jsonl_crawl_full/technology_all.jsonl')
