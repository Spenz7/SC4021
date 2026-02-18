import pandas as pd

def filter_ai_comments_three_sheets(input_file="all_raw_comments.xlsx", output_file="ai_filtered_three_sheets.xlsx"):
    """
    THREE SHEETS with CASE-SENSITIVE matching:
    - Only matches "AI" (uppercase), not "ai", "Ai", "aI"
    
    Sheet 1: Comments from posts with "AI" in title (Cond 1)
    Sheet 2: Comments with "AI" in text (Cond 2)
    Sheet 3: Sheet 1 + Sheet 2 combined, with duplicates removed
    """
    
    print(f"Loading {input_file}...")
    df = pd.read_excel(input_file)
    
    print(f"Loaded {len(df):,} total comments")
    print("🔤 Using CASE-SENSITIVE matching: only matches 'AI' (uppercase), not 'ai', 'Ai', or 'aI'")
    
    # Detect column names
    title_col = next((col for col in df.columns if 'title' in col.lower()), 'post_title')
    text_col = next((col for col in df.columns if 'comment' in col.lower() or 'text' in col.lower()), 'comment_text')
    subreddit_col = next((col for col in df.columns if 'subreddit' in col.lower()), 'subreddit')
    url_col = next((col for col in df.columns if 'url' in col.lower()), 'post_url')
    
    # Create a unique ID if not present
    if 'id' not in df.columns and 'comment_id' not in df.columns:
        df['temp_id'] = df.index.astype(str)
        id_col = 'temp_id'
    else:
        id_col = 'id' if 'id' in df.columns else 'comment_id'
    
    print(f"\nUsing columns: title='{title_col}', text='{text_col}', subreddit='{subreddit_col}', url='{url_col}', id='{id_col}'")
    
    # CASE-SENSITIVE pattern: only matches "AI" as a whole word
    pattern = r'\bAI\b'  # Only matches uppercase "AI"
    
    # Use original text (no .lower() conversion)
    title_series = df[title_col].astype(str)
    text_series = df[text_col].astype(str)
    
    # CONDITION 1: Posts with "AI" in title (case sensitive)
    posts_with_ai_title = set(df[title_series.str.contains(pattern, na=False, regex=True)][url_col].unique())
    df['cond1'] = df[url_col].isin(posts_with_ai_title)
    print(f"\n📌 Found {len(posts_with_ai_title):,} unique posts with 'AI' in title")
    
    # CONDITION 2: Comments with "AI" in text (case sensitive)
    df['cond2'] = text_series.str.contains(pattern, na=False, regex=True)
    print(f"📌 Found {df['cond2'].sum():,} comments with 'AI' in text")
    
    # SHEET 1: All comments from posts with AI in title (Cond1)
    sheet1_df = df[df['cond1']].copy()
    sheet1_df = sheet1_df[[title_col, text_col, subreddit_col, url_col, id_col]]
    sheet1_df['source'] = 'post_title_has_AI'
    
    # SHEET 2: All comments with AI in text (Cond2)
    sheet2_df = df[df['cond2']].copy()
    sheet2_df = sheet2_df[[title_col, text_col, subreddit_col, url_col, id_col]]
    sheet2_df['source'] = 'comment_text_has_AI'
    
    # SHEET 3: Sheet1 + Sheet2 combined, duplicates removed
    sheet3_df = pd.concat([sheet1_df, sheet2_df], ignore_index=True)
    initial_count = len(sheet3_df)
    sheet3_df = sheet3_df.drop_duplicates(subset=[id_col], keep='first')
    duplicates_removed = initial_count - len(sheet3_df)
    
    # Add column showing which conditions were met
    def get_merged_source(row):
        in_sheet1 = row[id_col] in sheet1_df[id_col].values
        in_sheet2 = row[id_col] in sheet2_df[id_col].values
        
        if in_sheet1 and in_sheet2:
            return 'both_conditions'
        elif in_sheet1:
            return 'post_title_only'
        else:
            return 'comment_text_only'
    
    sheet3_df['conditions_met'] = sheet3_df.apply(get_merged_source, axis=1)
    
    # Sort all sheets
    sheet1_df = sheet1_df.sort_values(title_col)
    sheet2_df = sheet2_df.sort_values(title_col)
    sheet3_df = sheet3_df.sort_values(title_col)
    
    # Save to Excel with three sheets
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        sheet1_df.to_excel(writer, sheet_name='Posts_with_AI_in_Title', index=False)
        sheet2_df.to_excel(writer, sheet_name='Comments_with_AI_in_Text', index=False)
        sheet3_df.to_excel(writer, sheet_name='All_Unique_Comments', index=False)
    
    # Print summary
    print(f"\n{'='*60}")
    print(f"RESULTS")
    print(f"{'='*60}")
    print(f"Sheet 1 - Posts with 'AI' in title: {len(sheet1_df):,} comments")
    print(f"Sheet 2 - Comments with 'AI' in text: {len(sheet2_df):,} comments")
    print(f"Sheet 3 - All unique comments: {len(sheet3_df):,} comments")
    print(f"  - Duplicates removed: {duplicates_removed:,}")
    
    # Overlap statistics
    overlap_ids = set(sheet1_df[id_col]) & set(sheet2_df[id_col])
    print(f"\n📊 OVERLAP: {len(overlap_ids):,} comments satisfy BOTH conditions")
    
    # Examples of what was matched
    print(f"\n📝 Examples of matched patterns:")
    print(f"  - Will match: 'AI is changing recruiting' ✓")
    print(f"  - Will match: 'The future of AI' ✓")
    print(f"  - Will NOT match: 'ai is changing recruiting' ✗ (lowercase)")
    print(f"  - Will NOT match: 'training ai models' ✗ (lowercase)")
    print(f"  - Will NOT match: 'pail' ✗ (part of word, wrong case)")
    
    # Breakdown of Sheet 3
    sheet3_source_counts = sheet3_df['conditions_met'].value_counts()
    print(f"\n📊 Sheet 3 breakdown:")
    for source, count in sheet3_source_counts.items():
        pct = count/len(sheet3_df)*100
        print(f"  - {source}: {count:,} comments ({pct:.1f}%)")
    
    print(f"\n✅ Saved to {output_file}")

if __name__ == "__main__":
    filter_ai_comments_three_sheets()
