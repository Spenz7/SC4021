import pandas as pd

def extract_unique_post_titles_from_excel(input_file="all_raw_comments.xlsx", output_file="unique_post_titles.xlsx"):
    """Extract unique post titles from your 50k comments Excel file"""
    
    print(f"Loading {input_file}...")
    
    # Load the Excel file
    df = pd.read_excel(input_file)
    
    print(f"Loaded {len(df):,} total comments")
    
    # Check what columns you have (uncomment if unsure)
    # print("Columns in your file:", df.columns.tolist())
    
    # Adjust column names based on your actual Excel columns
    # Your columns might be named differently - check and adjust!
    expected_columns = {
        'post_title': ['post_title', 'title', 'Post Title', 'post title'],
        'post_url': ['post_url', 'url', 'Post URL', 'post url'],
        'subreddit': ['subreddit', 'Subreddit', 'source']
    }
    
    # Find actual column names
    actual_columns = {}
    for key, possible_names in expected_columns.items():
        for col in df.columns:
            if col.lower() in [name.lower() for name in possible_names]:
                actual_columns[key] = col
                break
    
    print(f"\nDetected columns: {actual_columns}")
    
    # Get unique posts with comment counts
    unique_posts = df.groupby([actual_columns['post_title'], 
                               actual_columns['post_url'], 
                               actual_columns['subreddit']]).size().reset_index(name='comment_count')
    
    # Rename columns for clarity
    unique_posts.columns = ['post_title', 'post_url', 'subreddit', 'comment_count']
    
    # Add a sample comment from each post (first comment)
    sample_comments = df.groupby([actual_columns['post_title']]).first()['comment_text'].reset_index()
    sample_comments.columns = ['post_title', 'sample_comment']
    
    # Merge with unique posts
    unique_posts = unique_posts.merge(sample_comments, on='post_title', how='left')
    
    # Truncate sample comment for readability
    unique_posts['sample_comment'] = unique_posts['sample_comment'].apply(
        lambda x: str(x)[:200] + '...' if len(str(x)) > 200 else str(x)
    )
    
    # Sort by most discussed
    unique_posts = unique_posts.sort_values('comment_count', ascending=False)
    
    # Add column for manual relevance labeling
    unique_posts['relevant_to_topic'] = ''  # You'll fill this in manually
    unique_posts['notes'] = ''  # Optional notes
    
    # Reorder columns for easier review
    unique_posts = unique_posts[['relevant_to_topic', 'subreddit', 'post_title', 
                                  'comment_count', 'sample_comment', 'post_url', 'notes']]
    
    # Save to new Excel file
    unique_posts.to_excel(output_file, index=False)
    
    # Print summary
    print(f"\n{'='*60}")
    print(f"RESULTS")
    print(f"{'='*60}")
    print(f"Total comments in dataset: {len(df):,}")
    print(f"Total unique posts: {len(unique_posts):,}")
    print(f"Average comments per post: {len(df)/len(unique_posts):.1f}")
    
    print(f"\nTop 10 most discussed posts:")
    print(unique_posts.head(10)[['subreddit', 'post_title', 'comment_count']].to_string(index=False))
    
    print(f"\n✅ Saved {len(unique_posts)} unique post titles to {output_file}")
    print("\nNow you can manually mark 'yes'/'no' in the 'relevant_to_topic' column")
    print("Send me the file when you're done and I'll help filter your 50k comments!")

# Alternative: If you just want a quick look without saving
def preview_unique_posts(input_file="all_raw_comments.xlsx", n=20):
    """Preview first n unique posts without saving"""
    
    df = pd.read_excel(input_file)
    
    # Get unique posts with counts
    unique = df.groupby(['post_title', 'subreddit']).size().reset_index(name='comment_count')
    unique = unique.sort_values('comment_count', ascending=False)
    
    print(f"\nTop {n} most discussed posts:")
    print(unique.head(n).to_string(index=False))
    
    return unique

if __name__ == "__main__":
    # Run the main function
    extract_unique_post_titles_from_excel()
    
    # Or just preview:
    # preview_unique_posts(n=20)
