import pandas as pd
import re

def filter_ai_comments_three_sheets(input_file="all_raw_comments.xlsx", output_file="ai_filtered_three_sheets.xlsx"):
    """
    THREE SHEETS with mixed case sensitivity:
    - "AI" must be uppercase
    - All other keywords are case insensitive
    """
    
    print(f"Loading {input_file}...")
    df = pd.read_excel(input_file)
    print(f"Loaded {len(df):,} total comments")
    
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
    
    # Define keyword categories
    interview_terms = [
        'HireVue', 'one[- ]way interview', 'video interview', 'ai interview', 
        'automated interview', 'pre[- ]recorded interview', 'digital interview',
        'phone screen', 'screening call', 'ai assistant', 'chatbot',
        'Olivia Paradox', 'Paradox', 'ai recruiter'
    ]
    
    ats_terms = [
        'ATS', 'applicant tracking', 'resume screening', 'auto reject', 'auto[- ]reject',
        'automated reject', 'automated rejection', 'keyword filter', 'ai screening',
        'algorithm rejected', 'flagged as high-risk', 'screening system'
    ]
    
    candidate_ai_terms = [
        'fake candidate', 'ai generated resume', 'ai resume', 'chatgpt application',
        'ai slop', 'bot application', 'impersonation', 'deepfake',
        'candidates using ai', 'ai generated application', 'chatgpt cover letter'
    ]
    
    opinion_terms = [
        'ai taking your job', 'ai replacing recruiters', 'automated hiring',
        'double standard', 'hypocrit', 'rules for thee', 'ai for me',
        'boycott ai interviews', 'ai outperforms human', 'ai interview boycott'
    ]
    
    assessment_terms = [
        'personality test', 'cognitive test', 'assessment tool', 'skills test',
        'coding test', 'technical assessment'
    ]
    
    # Combine all non-AI terms
    other_keywords = interview_terms + ats_terms + candidate_ai_terms + opinion_terms + assessment_terms
    other_pattern = r'\b(' + '|'.join(other_keywords) + r')\b'
    
    # AI pattern (case sensitive)
    ai_pattern = r'\bAI\b'
    
    title_series = df[title_col].astype(str)
    text_series = df[text_col].astype(str)
    
    print("\n📌 Filtering with mixed case sensitivity:")
    print("   - 'AI' must be uppercase")
    print(f"   - {len(other_keywords)} other keywords are case insensitive")
    
    # CONDITION 1: Posts with relevant keywords in title
    # Check for AI (case sensitive)
    posts_with_ai_title = set(df[title_series.str.contains(ai_pattern, na=False, regex=True)][url_col].unique())
    # Check for other keywords (case insensitive)
    posts_with_other_title = set(df[title_series.str.contains(other_pattern, na=False, regex=True, case=False)][url_col].unique())
    posts_with_keywords = posts_with_ai_title.union(posts_with_other_title)
    df['cond1'] = df[url_col].isin(posts_with_keywords)
    print(f"\n📌 Found {len(posts_with_keywords):,} unique posts with relevant keywords in title")
    
    # CONDITION 2: Comments with relevant keywords in text
    df['cond2'] = (
        text_series.str.contains(ai_pattern, na=False, regex=True) | 
        text_series.str.contains(other_pattern, na=False, regex=True, case=False)
    )
    print(f"📌 Found {df['cond2'].sum():,} comments with relevant keywords in text")
    
    # SHEET 1: All comments from posts with keywords in title
    sheet1_df = df[df['cond1']].copy()
    sheet1_df = sheet1_df[[title_col, text_col, subreddit_col, url_col, id_col]]
    sheet1_df['source'] = 'post_title_has_keywords'
    
    # SHEET 2: All comments with keywords in text
    sheet2_df = df[df['cond2']].copy()
    sheet2_df = sheet2_df[[title_col, text_col, subreddit_col, url_col, id_col]]
    sheet2_df['source'] = 'comment_text_has_keywords'
    
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
        sheet1_df.to_excel(writer, sheet_name='Posts_with_Keywords_Title', index=False)
        sheet2_df.to_excel(writer, sheet_name='Comments_with_Keywords_Text', index=False)
        sheet3_df.to_excel(writer, sheet_name='All_Unique_Comments', index=False)
    
    # Print summary
    print(f"\n{'='*60}")
    print(f"RESULTS")
    print(f"{'='*60}")
    print(f"Sheet 1 - Posts with keywords in title: {len(sheet1_df):,} comments")
    print(f"Sheet 2 - Comments with keywords in text: {len(sheet2_df):,} comments")
    print(f"Sheet 3 - All unique comments: {len(sheet3_df):,} comments")
    print(f"  - Duplicates removed: {duplicates_removed:,}")
    
    # Overlap statistics
    overlap_ids = set(sheet1_df[id_col]) & set(sheet2_df[id_col])
    print(f"\n📊 OVERLAP: {len(overlap_ids):,} comments satisfy BOTH conditions")
    
    # Breakdown of Sheet 3
    sheet3_source_counts = sheet3_df['conditions_met'].value_counts()
    print(f"\n📊 Sheet 3 breakdown:")
    for source, count in sheet3_source_counts.items():
        pct = count/len(sheet3_df)*100
        print(f"  - {source}: {count:,} comments ({pct:.1f}%)")
    
    print(f"\n✅ Saved to {output_file}")

if __name__ == "__main__":
    filter_ai_comments_three_sheets()
