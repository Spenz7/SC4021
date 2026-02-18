import json
import pandas as pd
import os

# --- Configuration ---
INPUT_FOLDER = "jsonl_crawl_full"  # Define input folder

subreddits = [
    "recruiting",
    "recruitment",
    "humanresources",
    "recruitmentagencies",
    #"technology",
    #"futurology",
    #"recruitinghell"
]

def create_raw_excel():
    """Create Excel with ALL 50k raw comments (before filtering)"""
    
    all_comments = []
    
    for sub in subreddits:
        filepath = os.path.join(INPUT_FOLDER, f"{sub}_all.jsonl")
        if not os.path.exists(filepath):
            print(f"Warning: {filepath} not found, skipping")
            continue
        
        print(f"Loading {sub}...")
        with open(filepath, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f):
                try:
                    record = json.loads(line)
                    all_comments.append({
                        "subreddit": sub,
                        "post_title": record["metadata"]["post_title"],
                        "post_url": record["metadata"]["url"],
                        "comment_text": record["text"],
                        "comment_id": record.get("id", f"{sub}_{line_num}"),
                        "timestamp": record.get("timestamp", "")
                    })
                    
                    # Optional progress indicator
                    if len(all_comments) % 10000 == 0:
                        print(f"  Loaded {len(all_comments):,} comments so far...")
                        
                except json.JSONDecodeError as e:
                    print(f"  Error parsing line {line_num} in {sub}: {e}")
                    continue
                except KeyError as e:
                    print(f"  Missing key {e} in record from {sub}")
                    continue
    
    if not all_comments:
        print("No comments found! Check your input folder.")
        return
    
    # Create DataFrame and save
    df = pd.DataFrame(all_comments)
    output_file = "all_raw_comments.xlsx"
    df.to_excel(output_file, index=False)
    
    print(f"\n{'='*60}")
    print(f"✅ SUCCESS: Saved {len(df):,} raw comments to {output_file}")
    print(f"{'='*60}")
    
    # Print summary stats
    print(f"\nComments by subreddit:")
    by_sub = df['subreddit'].value_counts()
    for sub in subreddits:
        if sub in by_sub:
            print(f"  {sub}: {by_sub[sub]:,} comments")

if __name__ == "__main__":
    create_raw_excel()
