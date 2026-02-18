#doesn't have sampling yet, u obtain relevant comments first, then later see if need sample amongst
#them assuming u have a ton of relevant comments

#REV maybe just look for keyword "AI"

import json
import os
import pandas as pd
from collections import Counter

# --- Configuration ---
INPUT_FOLDER = "jsonl_crawl_full"
OUTPUT_FILE = "all_relevant_comments.xlsx"  # Changed name

subreddits = [
    "recruiting", "recruitment", "humanresources", 
    "recruitinghell", "technology", "futurology", "recruitmentagencies"
]

def is_relevant_combination(text):
    """Check if comment is about AI in hiring"""
    text_lower = text.lower()
    
    # Word pair combinations
    combinations = [
        ("ai", "recruit"), ("ai", "hire"), ("ai", "interview"),
        ("ai", "candidate"), ("ai", "applicant"), ("ai", "resume"),
        ("ai", "screen"), ("artificial intelligence", "recruit"),
        ("machine learning", "recruit"), ("automated", "recruit"),
        ("automated", "hire"), ("algorithm", "recruit"),
        ("bot", "interview"), ("robot", "interview"),
        ("ai", "hiring process"), ("ai", "recruitment process"),
    ]
    
    for word1, word2 in combinations:
        if word1 in text_lower and word2 in text_lower:
            pos1 = text_lower.find(word1)
            pos2 = text_lower.find(word2)
            if abs(pos1 - pos2) < 300:
                return True, f"{word1}+{word2}"
    
    # Topic word counting
    topic_words = [
        "ai", "artificial intelligence", "machine learning", "ml",
        "recruit", "hire", "interview", "candidate", "applicant",
        "resume", "cv", "screening", "automated", "algorithm",
        "bot", "robot", "chatbot", "ats"
    ]
    
    matches = [word for word in topic_words if word in text_lower]
    if len(matches) >= 3:
        return True, f"topic_count:{len(matches)}"
    
    return False, None

def find_all_relevant():
    """Find ALL relevant comments without sampling"""
    
    all_relevant = []
    match_reasons = Counter()
    total_processed = 0
    total_words = 0
    
    print("=" * 60)
    print("FINDING ALL RELEVANT COMMENTS")
    print("=" * 60)
    
    for sub in subreddits:
        filepath = os.path.join(INPUT_FOLDER, f"{sub}_all.jsonl")
        if not os.path.exists(filepath):
            print(f"Warning: {filepath} not found")
            continue
        
        print(f"\nProcessing {sub}...")
        sub_relevant = 0
        sub_processed = 0
        
        with open(filepath, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    record = json.loads(line)
                    sub_processed += 1
                    total_processed += 1
                    
                    # Check relevance
                    is_rel, reason = is_relevant_combination(record["text"])
                    
                    if is_rel:
                        words = len(record["text"].split())
                        total_words += words
                        
                        all_relevant.append({
                            "subreddit": sub,
                            "text": record["text"],
                            "word_count": words,
                            "relevance_reason": reason,
                            "id": record.get("id", ""),
                            "timestamp": record.get("timestamp", "")
                        })
                        sub_relevant += 1
                        match_reasons[reason] += 1
                    
                    # Progress indicator
                    if sub_processed % 5000 == 0:
                        print(f"  Processed {sub_processed:,} comments, found {sub_relevant:,} relevant so far")
                        
                except json.JSONDecodeError:
                    continue
        
        print(f"  {sub}: {sub_relevant:,} relevant out of {sub_processed:,} comments ({sub_relevant/sub_processed*100:.1f}%)")
    
    # Print summary
    print("\n" + "=" * 60)
    print("FINAL RESULTS")
    print("=" * 60)
    print(f"Total comments processed: {total_processed:,}")
    print(f"Total relevant comments found: {len(all_relevant):,}")
    print(f"Percentage relevant: {len(all_relevant)/total_processed*100:.1f}%")
    print(f"Total words in relevant comments: {total_words:,}")
    print(f"Average words per relevant comment: {total_words/len(all_relevant):.1f}")
    
    print("\nTop match reasons:")
    for reason, count in match_reasons.most_common(10):
        print(f"  {reason}: {count} ({count/len(all_relevant)*100:.1f}%)")
    
    print("\nResults by subreddit:")
    by_sub = Counter(c["subreddit"] for c in all_relevant)
    for sub in subreddits:
        if sub in by_sub:
            print(f"  {sub}: {by_sub[sub]:,} comments")
    
    return all_relevant

def save_results(relevant_comments):
    """Save all relevant comments to Excel"""
    
    df = pd.DataFrame(relevant_comments)
    
    # Sort by subreddit then word count (optional)
    df = df.sort_values(["subreddit", "word_count"], ascending=[True, False])
    
    # # Save to Excel
    # df.to_excel(OUTPUT_FILE, index=False)
    # print(f"\n✅ Saved all {len(df):,} relevant comments to {OUTPUT_FILE}")
    
    # Also save a CSV version for easier processing later
    csv_file = OUTPUT_FILE.replace(".xlsx", ".csv")
    df.to_csv(csv_file, index=False, encoding='utf-8')
    print(f"✅ Saved CSV version to {csv_file}")

def main():
    # Find ALL relevant comments
    relevant = find_all_relevant()
    
    if len(relevant) < 1000:
        print("\n⚠️  WARNING: Less than 1,000 relevant comments found!")
        print("Consider expanding your keywords or crawling more data.")
    else:
        print(f"\n✅ Found {len(relevant):,} relevant comments - well above the 10k requirement!")
    
    # Save all results
    save_results(relevant)
    
    # Give next steps
    print("\n" + "=" * 60)
    print("NEXT STEPS")
    print("=" * 60)
    print("1. Review all_relevant_comments.xlsx to see what you have")
    print("2. If you have >10k, you can sample down later using:")
    print("   df = pd.read_excel('all_relevant_comments.xlsx')")
    print("   sampled = df.sample(n=10000)")
    print("3. Create eval.xlsx by taking 1,000 random rows for manual labeling")

if __name__ == "__main__":
    main()
