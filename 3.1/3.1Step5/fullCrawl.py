import requests
import json
import os
import time
from datetime import datetime
from collections import defaultdict

# --- Configuration ---
subreddits = [

    "recruiting",
    "recruitment",
    "humanresources",
    "recruitmentagencies",
    #a lot of unrelated topics, if wan this do use fullCrawlManual.py# "technology",
    #a lot of unrelated topics, if wan this do use fullCrawlManual.py#"futurology",
    
    
    #last resort #"recruitinghell"
    
]
#avoid technology and futurology for now, since u accept any comment w "AI" in it

keywords = [
    "AI recruit",
    "AI recruiting",
    "AI hiring",
    "AI resume screening",
    "AI interview",
    "ATS AI",
    "recruitment automation",
    "automated candidate screening",
    "interview bot",
    "candidate ranking AI",
]

MIN_COMMENTS = 25
OUTPUT_FOLDER = "jsonl_crawl_full"
PROGRESS_FILE = "crawl_progress.json"
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# Delays
REQUEST_DELAY_SEARCH = 2.0
REQUEST_DELAY_COMMENTS = 3.0

# Targets
TARGET_COMMENTS = 70000
TARGET_WORDS = 600000

# Track seen posts to avoid duplicates
SEEN_POSTS_FILE = "seen_posts.json"

# --- Load/Save Seen Posts Only ---
def load_seen_posts():
    if os.path.exists(SEEN_POSTS_FILE):
        with open(SEEN_POSTS_FILE, 'r') as f:
            return set(json.load(f))
    return set()

def save_seen_posts(seen_posts):
    """Append new seen posts to the existing JSON file without overwriting."""
    existing = set()
    if os.path.exists(SEEN_POSTS_FILE):
        with open(SEEN_POSTS_FILE, 'r') as f:
            existing = set(json.load(f))
    all_seen = existing.union(seen_posts)
    with open(SEEN_POSTS_FILE, 'w') as f:
        json.dump(list(all_seen), f)

# --- Main Loop (No crawl_progress) ---
seen_posts = load_seen_posts()
total_comments = 0
total_words = 0

print(f"Resuming crawl. Already seen posts: {len(seen_posts)}")
print(f"Sorting by RELEVANCE then COMMENT COUNT with MIN_COMMENTS = {MIN_COMMENTS}")

for sub in subreddits:
    output_file = os.path.join(OUTPUT_FOLDER, f"{sub}_all.jsonl")
    
    for kw in keywords:
        print(f"\nFetching posts for r/{sub} with keyword '{kw}' (relevance sorted)...")
        posts = fetch_posts(sub, kw, limit=100)
        time.sleep(REQUEST_DELAY_SEARCH)

        for post in posts:
            post_data = post["data"]
            post_id = post_data["id"]
            
            if post_id in seen_posts:
                continue  # Skip already processed posts
            
            if post_data.get("num_comments", 0) < MIN_COMMENTS:
                continue  # Skip low-comment posts
            
            post_url = f"https://www.reddit.com{post_data['permalink']}"
            print(f"  Fetching comments for post: {post_url} ({post_data.get('num_comments')} comments)")
            data = fetch_comments(post_url)
            time.sleep(REQUEST_DELAY_COMMENTS)
            
            if not data:
                continue  # Skip if fetch failed
            
            comments_count, words_count = process_comments_to_jsonl(data, post_url, output_file)
            seen_posts.add(post_id)  # Mark as seen
            save_seen_posts(seen_posts)  # Save after each post
            
            total_comments += comments_count
            total_words += words_count

            print(f"  +{comments_count} comments, +{words_count} words | Total: {total_comments}, {total_words}")

print(f"\nFinal stats: {total_comments} comments, {total_words} words")
print(f"Files saved in {OUTPUT_FOLDER}/ :")
for sub in subreddits:
    filepath = os.path.join(OUTPUT_FOLDER, f"{sub}_all.jsonl")
    if os.path.exists(filepath):
        size = os.path.getsize(filepath) / (1024*1024)
        print(f"  - {sub}_all.jsonl ({size:.2f} MB)")

