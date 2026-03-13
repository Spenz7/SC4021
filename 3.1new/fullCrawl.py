import requests
import json
import os
import time
from datetime import datetime

# --- Configuration ---
LINKS_FILE = "reddit_links.txt"  # manually paste one Reddit post URL per line
OUTPUT_FOLDER = "jsonl_crawl_manual"
PROGRESS_FILE = "crawl_progress.json"
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

MIN_COMMENTS = 25
REQUEST_DELAY_COMMENTS = 3.0
SEEN_POSTS_FILE = "seen_posts.json"

def count_existing_comments_words(filepath):
    comments = 0
    words = 0
    if os.path.exists(filepath):
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                try:
                    data = json.loads(line)
                    comments += 1
                    words += len(data["text"].split())
                except:
                    continue
    return comments, words

# --- Load/Save Seen Posts Only ---
def load_seen_posts():
    if os.path.exists(SEEN_POSTS_FILE):
        with open(SEEN_POSTS_FILE, 'r') as f:
            return set(json.load(f))
    return set()

def save_seen_posts(seen_posts):
    existing = set()
    if os.path.exists(SEEN_POSTS_FILE):
        with open(SEEN_POSTS_FILE, 'r') as f:
            existing = set(json.load(f))
    all_seen = existing.union(seen_posts)
    with open(SEEN_POSTS_FILE, 'w') as f:
        json.dump(list(all_seen), f)

# --- Fetch comments from a Reddit post URL ---
def fetch_comments(post_url, max_retries=5):
    headers = {"User-Agent": "Mozilla/5.0 (compatible; SC4021Crawler/1.0; contact=edu)"}
    delay = 10
    for attempt in range(max_retries):
        try:
            r = requests.get(f"{post_url}.json", headers=headers, timeout=15)
            if r.status_code == 200:
                return r.json()
            elif r.status_code == 429:
                wait_time = delay * (2 ** attempt)
                print(f"429 RATE LIMIT: Backing off {wait_time}s...")
                time.sleep(wait_time)
            elif r.status_code in [403, 404]:
                print(f"{r.status_code} FORBIDDEN/NOT FOUND: Skipping {post_url}")
                return None
            else:
                print(f"HTTP {r.status_code} failed for {post_url}")
                if attempt < max_retries - 1:
                    time.sleep(delay * (attempt + 1))
                else:
                    return None
        except requests.exceptions.RequestException as e:
            wait_time = delay * (2 ** attempt)
            print(f"Request error: {e}. Retrying in {wait_time}s...")
            time.sleep(wait_time)
    print(f"Max retries exceeded for {post_url}. Skipping.")
    return None

def process_comments_to_jsonl(input_json, post_url, output_file):
    if not input_json or len(input_json) < 2:
        return 0, 0

    post_data = input_json[0]["data"]["children"][0]["data"]
    subreddit = post_data["subreddit"]
    post_title = post_data["title"]

    comments_count = 0
    words_count = 0

    with open(output_file, "a", encoding="utf-8") as out_f:
        def process_comment(comment):
            nonlocal comments_count, words_count
            if comment["kind"] != "t1":
                return
            c = comment["data"]
            if c.get("body") in ["[deleted]", "[removed]"]:
                return
            text = c.get("body", "")
            record = {
                "id": c["id"],
                "text": text,
                "timestamp": datetime.utcfromtimestamp(c["created_utc"]).isoformat() + "Z",
                "source": "reddit",
                "metadata": {
                    "subreddit": subreddit,
                    "post_title": post_title,
                    "url": post_url
                }
            }
            out_f.write(json.dumps(record, ensure_ascii=False) + "\n")
            comments_count += 1
            words_count += len(text.split())
            if c.get("replies") and isinstance(c["replies"], dict):
                for reply in c["replies"]["data"]["children"]:
                    process_comment(reply)

        for comment in input_json[1]["data"]["children"]:
            process_comment(comment)

    return comments_count, words_count

# --- Main Loop ---
seen_posts = load_seen_posts()
output_file = os.path.join(OUTPUT_FOLDER, f"reddit_manual.jsonl")
total_comments, total_words = count_existing_comments_words(output_file)
print(f"Resuming crawl. Already have {total_comments} comments and {total_words} words")

# Read manually pasted links
with open(LINKS_FILE, 'r') as f:
    reddit_links = [line.strip() for line in f if line.strip()]

print(f"Total links to process: {len(reddit_links)}")
output_file = os.path.join(OUTPUT_FOLDER, f"reddit_manual.jsonl")

for post_url in reddit_links:
    post_id = post_url.rstrip("/").split("/")[-1]
    if post_id in seen_posts:
        continue
    print(f"\nFetching comments for: {post_url}")
    data = fetch_comments(post_url)
    time.sleep(REQUEST_DELAY_COMMENTS)
    if not data:
        continue
    comments_count, words_count = process_comments_to_jsonl(data, post_url, output_file)
    if comments_count < MIN_COMMENTS:
        print(f"Skipping post (less than {MIN_COMMENTS} comments).")
        continue
    seen_posts.add(post_id)
    save_seen_posts(seen_posts)
    total_comments += comments_count
    total_words += words_count
    print(f" +{comments_count} comments, +{words_count} words | Total: {total_comments}, {total_words}")

print(f"\nCrawl complete: {total_comments} comments, {total_words} words")
print(f"Output file: {output_file}")
