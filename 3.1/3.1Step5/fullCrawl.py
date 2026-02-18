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
    "AI recruit"
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

MIN_COMMENTS = 20
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

# --- Load/Save Progress ---
def load_progress():
    """Load progress from previous runs"""
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r') as f:
            return json.load(f)
    return {"total_comments": 0, "total_words": 0, "completed_keywords": []}

def save_progress(progress):
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f)

def load_seen_posts():
    if os.path.exists(SEEN_POSTS_FILE):
        with open(SEEN_POSTS_FILE, 'r') as f:
            return set(json.load(f))
    return set()

def save_seen_posts(seen_posts):
    with open(SEEN_POSTS_FILE, 'w') as f:
        json.dump(list(seen_posts), f)

# --- Fetch functions ---
def fetch_posts(subreddit, keyword, limit=100):
    """Fetch posts sorted by relevance, then locally by comment count"""
    url = f"https://www.reddit.com/r/{subreddit}/search.json"
    headers = {"User-Agent": "Mozilla/5.0 (RedditCrawler/0.1 by YourUsername)"}
    
    # Fetch more than needed to allow for sorting
    fetch_limit = limit * 2
    
    params = {
        "q": keyword,
        "sort": "relevance",  # First, get most relevant
        "limit": fetch_limit,
        "restrict_sr": 1
    }
    
    try:
        r = requests.get(url, headers=headers, params=params, timeout=10)
        if r.status_code != 200:
            print(f"Failed to fetch posts ({subreddit}, '{keyword}'): {r.status_code}")
            return []
        
        posts = r.json().get("data", {}).get("children", [])
        
        if not posts:
            return []
        
        # Then sort by comment count (highest first)
        posts.sort(key=lambda p: p["data"]["num_comments"], reverse=True)
        
        # Return top 'limit' posts
        print(f"  Found {len(posts)} relevant posts, returning top {min(limit, len(posts))} by comment count")
        return posts[:limit]
        
    except Exception as e:
        print(f"Error fetching posts ({subreddit}, '{keyword}'): {e}")
        return []
    
def fetch_comments(post_url, max_retries=5):
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; SC4021Crawler/1.0; contact=edu)"
    }
    
    delay = 10
    for attempt in range(max_retries):
        try:
            r = requests.get(f"{post_url}.json", headers=headers, timeout=15)
            
            if r.status_code == 200:
                return r.json()
                
            elif r.status_code == 429:
                wait_time = delay * (2 ** attempt)
                print(f"429 RATE LIMIT: Backing off for {wait_time}s (attempt {attempt+1}/{max_retries})...")
                time.sleep(wait_time)
                
            elif r.status_code == 403:
                print(f"403 FORBIDDEN: Post may be private/removed. Skipping.")
                return None
                
            elif r.status_code == 404:
                print(f"404 NOT FOUND: Post deleted. Skipping.")
                return None
                
            else:
                print(f"Failed to fetch comments: HTTP {r.status_code}")
                if attempt < max_retries - 1:
                    time.sleep(delay * (attempt + 1))
                else:
                    return None
                    
        except requests.exceptions.Timeout:
            wait_time = delay * (2 ** attempt)
            print(f"TIMEOUT: Request timed out. Retrying in {wait_time}s (attempt {attempt+1}/{max_retries})...")
            time.sleep(wait_time)
            
        except requests.exceptions.ConnectionError:
            wait_time = delay * (2 ** attempt)
            print(f"CONNECTION ERROR: Network issue. Retrying in {wait_time}s (attempt {attempt+1}/{max_retries})...")
            time.sleep(wait_time)
            
        except Exception as e:
            print(f"Unexpected error: {e}")
            if attempt < max_retries - 1:
                time.sleep(delay * (attempt + 1))
            else:
                return None
    
    print(f"Max retries ({max_retries}) exceeded. Skipping this post.")
    return None

def process_comments_to_jsonl(input_json, post_url, output_file):
    """Extract comments and append to subreddit-level JSONL file"""
    if not input_json or len(input_json) < 2:
        return 0, 0

    post_data = input_json[0]["data"]["children"][0]["data"]
    subreddit = post_data["subreddit"]
    post_title = post_data["title"]

    comments_count = 0
    words_count = 0

    # Open in append mode
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

            # Process replies
            if c.get("replies") and isinstance(c["replies"], dict):
                for reply in c["replies"]["data"]["children"]:
                    process_comment(reply)

        for comment in input_json[1]["data"]["children"]:
            process_comment(comment)

    return comments_count, words_count

# --- Main loop ---
progress = load_progress()
seen_posts = load_seen_posts()
total_comments = progress["total_comments"]
total_words = progress["total_words"]

print(f"Resuming crawl. Current totals: {total_comments} comments, {total_words} words")
print(f"Sorting by RELEVANCE then COMMENT COUNT with MIN_COMMENTS = {MIN_COMMENTS}")

for sub in subreddits:
    # One output file per subreddit
    output_file = os.path.join(OUTPUT_FOLDER, f"{sub}_all.jsonl")
    
    for kw in keywords:
        if total_comments >= TARGET_COMMENTS and total_words >= TARGET_WORDS:
            print("Reached target corpus size!")
            break
            
        # Skip if this keyword was completed in a previous run
        kw_key = f"{sub}_{kw}"
        if kw_key in progress["completed_keywords"]:
            print(f"Skipping already completed: {kw_key}")
            continue

        print(f"Fetching posts for r/{sub} with keyword '{kw}' (relevance sorted)...")
        posts = fetch_posts(sub, kw, limit=100)
        time.sleep(REQUEST_DELAY_SEARCH)

        for post in posts:
            if total_comments >= TARGET_COMMENTS and total_words >= TARGET_WORDS:
                break
                
            post_data = post["data"]
            post_id = post_data["id"]
            
            # Skip if we've already processed this post
            if post_id in seen_posts:
                continue
                
            if post_data.get("num_comments", 0) < MIN_COMMENTS:
                continue

            post_url = f"https://www.reddit.com{post_data['permalink']}"
            print(f"  Fetching comments for post: {post_url} ({post_data.get('num_comments')} comments)")
            data = fetch_comments(post_url)
            time.sleep(REQUEST_DELAY_COMMENTS)
            
            if not data:
                continue

            comments_count, words_count = process_comments_to_jsonl(data, post_url, output_file)

            # Mark as seen
            seen_posts.add(post_id)
            save_seen_posts(seen_posts)

            total_comments += comments_count
            total_words += words_count

            print(f"  +{comments_count} comments, +{words_count} words | Total: {total_comments} comments, {total_words} words")

            # Save progress periodically
            if total_comments % 1000 < comments_count:
                progress["total_comments"] = total_comments
                progress["total_words"] = total_words
                save_progress(progress)

        # Mark keyword as completed
        progress["completed_keywords"].append(kw_key)
        save_progress(progress)

# Final progress save
progress["total_comments"] = total_comments
progress["total_words"] = total_words
save_progress(progress)

print(f"\nFinal stats: {total_comments} comments, {total_words} words")
print(f"Files saved in {OUTPUT_FOLDER}/ :")
for sub in subreddits:
    filepath = os.path.join(OUTPUT_FOLDER, f"{sub}_all.jsonl")
    if os.path.exists(filepath):
        size = os.path.getsize(filepath) / (1024*1024)
        print(f"  - {sub}_all.jsonl ({size:.2f} MB)")
