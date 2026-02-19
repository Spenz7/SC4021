import requests
import json
import os
import time
from datetime import datetime

# --- Configuration ---
subreddits = [
    "technology",
    "futurology",
]

keywords = [
    "AI recruiting",
    "AI recruit",
    "AI hiring",
    "AI resume screening",
    "ATS AI",
    "recruitment automation",
    "automated candidate screening",
    "candidate ranking AI",
    "AI interview",
    "interview bot",
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

# Track seen posts
SEEN_POSTS_FILE = "seen_posts.json"

# Step mapping for early-stage hiring (1-6)
STEP_KEYWORDS = {
    "Job Posting & Sourcing": ["AI recruit", "AI recruiting", "AI hiring"],
    "Resume/CV Screening": ["AI resume screening", "ATS AI", "candidate ranking AI", "automated candidate screening"],
    "Pre-employment Assessments": ["AI interview", "interview bot", "AI coding test", "technical assessment"],
    "Interview Scheduling & Initial Screening": [],
    "Candidate Interview": ["AI interview", "interview bot"],
    "Candidate Evaluation/Ranking": ["candidate ranking AI", "recruitment automation"]
}

# --- Load/Save ---
def load_progress():
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r') as f:
            return json.load(f)
    return {"total_comments": 0, "total_words": 0}

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
    url = f"https://www.reddit.com/r/{subreddit}/search.json"
    headers = {"User-Agent": "Mozilla/5.0 (RedditCrawler/0.1 by YourUsername)"}
    fetch_limit = limit * 2
    params = {"q": keyword, "sort": "relevance", "limit": fetch_limit, "restrict_sr": 1}
    try:
        r = requests.get(url, headers=headers, params=params, timeout=10)
        if r.status_code != 200: return []
        posts = r.json().get("data", {}).get("children", [])
        posts.sort(key=lambda p: p["data"]["num_comments"], reverse=True)
        return posts[:limit]
    except: return []

def fetch_comments(post_url, max_retries=5):
    headers = {"User-Agent": "Mozilla/5.0 (compatible; SC4021Crawler/1.0; contact=edu)"}
    delay = 10
    for attempt in range(max_retries):
        try:
            r = requests.get(f"{post_url}.json", headers=headers, timeout=15)
            if r.status_code == 200: return r.json()
            elif r.status_code == 429:
                time.sleep(delay * (2 ** attempt))
            elif r.status_code in [403, 404]: return None
            else:
                if attempt < max_retries - 1: time.sleep(delay * (attempt+1))
                else: return None
        except: time.sleep(delay * (2 ** attempt))
    return None

def process_comments_to_jsonl(input_json, post_url, output_file):
    if not input_json or len(input_json) < 2: return 0,0
    post_data = input_json[0]["data"]["children"][0]["data"]
    subreddit = post_data["subreddit"]
    post_title = post_data["title"]
    comments_count = 0
    words_count = 0

    with open(output_file, "a", encoding="utf-8") as out_f:
        def process_comment(comment):
            nonlocal comments_count, words_count
            if comment["kind"] != "t1": return
            c = comment["data"]
            if c.get("body") in ["[deleted]", "[removed]"]: return
            text = c.get("body", "")
            record = {
                "id": c["id"],
                "text": text,
                "timestamp": datetime.utcfromtimestamp(c["created_utc"]).isoformat() + "Z",
                "source": "reddit",
                "metadata": {"subreddit": subreddit, "post_title": post_title, "url": post_url}
            }
            out_f.write(json.dumps(record, ensure_ascii=False)+"\n")
            comments_count += 1
            words_count += len(text.split())
            if c.get("replies") and isinstance(c["replies"], dict):
                for reply in c["replies"]["data"]["children"]:
                    process_comment(reply)
        for comment in input_json[1]["data"]["children"]:
            process_comment(comment)
    return comments_count, words_count

# --- Determine likely step for a post ---
def suggest_step(post_title):
    title_lower = post_title.lower()
    for step, kw_list in STEP_KEYWORDS.items():
        for kw in kw_list:
            if kw.lower() in title_lower:
                return step
    return "Unknown Step"

# --- Main loop with batch approval ---
progress = load_progress()
seen_posts = load_seen_posts()
total_comments = progress["total_comments"]
total_words = progress["total_words"]
BATCH_SIZE = 30

for sub in subreddits:
    output_file = os.path.join(OUTPUT_FOLDER, f"{sub}_all.jsonl")
    for kw in keywords:
        if total_comments >= TARGET_COMMENTS and total_words >= TARGET_WORDS:
            break

        posts = fetch_posts(sub, kw, limit=100)
        time.sleep(REQUEST_DELAY_SEARCH)
        posts = [p for p in posts if p["data"]["id"] not in seen_posts and p["data"].get("num_comments",0) >= MIN_COMMENTS]

        for i in range(0, len(posts), BATCH_SIZE):
            batch = posts[i:i+BATCH_SIZE]
            print(f"\n--- Post batch {i//BATCH_SIZE + 1} ---")
            for idx, post in enumerate(batch):
                post_data = post["data"]
                step_hint = suggest_step(post_data["title"])
                print(f"{idx+1}. {post_data['title']} | {post_data['permalink']} ({post_data['num_comments']} comments) [Step: {step_hint}]")

            # --- parse input ---
            approval_input = input("Enter y/n for each post (comma-separated, e.g., y,n,y,...): ").strip().lower()
            approvals = [x.strip() for x in approval_input.split(",")]

            if len(approvals) != len(batch):
                print("Mismatch in number of responses. Skipping this batch entirely. No posts marked as seen.")
                continue  # batch skipped, no posts marked as seen

            for idx, post in enumerate(batch):
                post_data = post["data"]
                post_id = post_data["id"]
                post_url = f"https://www.reddit.com{post_data['permalink']}"

                # mark post as seen regardless of y/n
                seen_posts.add(post_id)

                # only fetch/process if user approved
                if approvals[idx] == "y":
                    data = fetch_comments(post_url)
                    time.sleep(REQUEST_DELAY_COMMENTS)
                    if data:
                        comments_count, words_count = process_comments_to_jsonl(data, post_url, output_file)
                        total_comments += comments_count
                        total_words += words_count
                        print(f"  +{comments_count} comments, +{words_count} words | Total: {total_comments}, {total_words}")

            # --- append to seen_posts.json ---
            if os.path.exists(SEEN_POSTS_FILE):
                with open(SEEN_POSTS_FILE, "r", encoding="utf-8") as f:
                    existing = set(json.load(f))
                existing.update(seen_posts)
                seen_posts_to_save = existing
            else:
                seen_posts_to_save = seen_posts

            with open(SEEN_POSTS_FILE, "w", encoding="utf-8") as f:
                json.dump(list(seen_posts_to_save), f)

            # save progress after each batch
            progress["total_comments"] = total_comments
            progress["total_words"] = total_words
            save_progress(progress)


# Final save
progress["total_comments"] = total_comments
progress["total_words"] = total_words
save_progress(progress)
print(f"\nFinal stats: {total_comments} comments, {total_words} words")
