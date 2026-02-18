import pandas as pd
import requests
import time
import random

#REV goes into eval2

# --- Step 1: Read Reddit links from CSV ---
input_file = 'stage1_posts_for_manual_review(outputfrmStep1).csv'
df_links = pd.read_csv(input_file)
reddit_links = df_links['url'].tolist()

HEADERS = {'User-Agent': 'CommentExtractor/0.1'}
MAX_RETRIES = 5
BASE_DELAY = 1.5  # seconds

# --- Step 2: Fetch comments with exponential backoff ---
def fetch_reddit_comments(url):
    if not url.endswith('.json'):
        url = url.rstrip('/') + '/.json'

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, headers=HEADERS, timeout=10)

            if response.status_code == 429:
                delay = BASE_DELAY * (2 ** attempt) + random.uniform(0, 1)
                print(f"429 received. Backing off for {delay:.2f}s (attempt {attempt + 1})")
                time.sleep(delay)
                continue

            response.raise_for_status()
            data = response.json()
            comments_list = []

            def parse_comments(children):
                for child in children:
                    if child.get('kind') != 't1':
                        continue

                    body = child['data'].get('body', '')
                    if body.lower() in ('[deleted]', '[removed]'):
                        continue

                    comments_list.append(body)

                    replies = child['data'].get('replies')
                    if isinstance(replies, dict):
                        parse_comments(replies['data']['children'])

            parse_comments(data[1]['data']['children'])
            return comments_list

        except requests.exceptions.RequestException as e:
            delay = BASE_DELAY * (2 ** attempt)
            print(f"Request error: {e}. Retrying in {delay:.2f}s")
            time.sleep(delay)

    print(f"Failed after retries: {url}")
    return []

# --- Step 3: Process all links ---
all_comments = []

for i, link in enumerate(reddit_links, 1):
    print(f"[{i}/{len(reddit_links)}] Processing: {link}")
    comments = fetch_reddit_comments(link)

    for c in comments:
        all_comments.append({'url': link, 'comment': c})

    time.sleep(0.5)  # small base delay to reduce burstiness

# --- Step 4: Output to Excel ---
output_file = 'eval2.xlsx'
pd.DataFrame(all_comments).to_excel(output_file, index=False)

print(f"Done. Extracted comments saved to {output_file}")
