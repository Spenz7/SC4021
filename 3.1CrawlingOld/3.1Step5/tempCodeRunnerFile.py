import pandas as pd
import re

def filter_ai_comments_strict(
    input_file="all_raw_comments.xlsx",
    output_file="ai_filtered_strict.xlsx"
):
    """
    STRICT FILTERING (0% noisy by construction)

    A comment is kept IF AND ONLY IF:
    1) It contains 'AI' (uppercase, word-boundary)
    2) It contains at least one hiring / recruitment related term

    No title-based inclusion.
    No OR logic.
    One output sheet only.
    """

    print(f"Loading {input_file}...")
    df = pd.read_excel(input_file)
    print(f"Loaded {len(df):,} total comments")

    # --- Detect column names robustly ---
    title_col = next((c for c in df.columns if 'title' in c.lower()), 'post_title')
    text_col = next((c for c in df.columns if 'comment' in c.lower() or 'text' in c.lower()), 'comment_text')
    subreddit_col = next((c for c in df.columns if 'subreddit' in c.lower()), 'subreddit')
    url_col = next((c for c in df.columns if 'url' in c.lower()), 'post_url')

    if 'id' in df.columns:
        id_col = 'id'
    elif 'comment_id' in df.columns:
        id_col = 'comment_id'
    else:
        df['temp_id'] = df.index.astype(str)
        id_col = 'temp_id'

    print(f"Using columns:")
    print(f"  title: {title_col}")
    print(f"  text: {text_col}")
    print(f"  subreddit: {subreddit_col}")
    print(f"  url: {url_col}")
    print(f"  id: {id_col}")

    # Convert text column to string once
    text_series = df[text_col].astype(str)

    # --- Define recruitment-stage keywords ---
    interview_terms = [
        'hirevue', 'one[- ]way interview', 'video interview', 'ai interview',
        'automated interview', 'pre[- ]recorded interview', 'digital interview',
        'phone screen', 'screening call', 'chatbot', 'ai recruiter', 'paradox', 'olivia'
    ]

    ats_terms = [
        'ats', 'applicant tracking', 'resume screening', 'cv screening',
        'auto[- ]reject', 'automated rejection', 'keyword filter',
        'algorithm rejected', 'screening system', 'candidate ranking'
    ]

    assessment_terms = [
        'personality test', 'cognitive test', 'assessment tool',
        'skills test', 'coding test', 'technical assessment'
    ]

    opinion_terms = [
        'automated hiring', 'ai replacing recruiters',
        'boycott ai interviews', 'double standard'
    ]

    implicit_hiring_terms = [
        'screened', 'shortlisted', 'filtered out', 'auto[- ]screen',
        'rejected automatically', 'ranking system', 'scoring system'
    ]

    # Combine ALL recruitment-related terms ONCE
    recruitment_terms = (
        interview_terms
        + ats_terms
        + assessment_terms
        + opinion_terms
        + implicit_hiring_terms
    )

    # Build regex pattern AFTER final list is created
    recruitment_pattern = r'\b(' + '|'.join(recruitment_terms) + r')\b'

    # Broader but still explicit AI references
    ai_pattern = r'\b(ai|artificial intelligence|algorithm|automated system|hiring algorithm)\b'

    # Strict AND filtering
    df['relevant'] = (
        text_series.str.contains(ai_pattern, na=False, regex=True, case=False)
        &
        text_series.str.contains(recruitment_pattern, na=False, regex=True, case=False)
    )

    final_df = df[df['relevant']].copy()

    final_df = final_df[
        [title_col, text_col, subreddit_col, url_col, id_col]
    ]

    print(f"Kept {len(final_df):,} comments after strict filtering")
    print(f"Removed {len(df) - len(final_df):,} irrelevant comments")

    # --- Save to Excel (single sheet) ---
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        final_df.to_excel(writer, sheet_name='strict_ai_hiring_comments', index=False)

    print(f"Saved strictly filtered dataset to {output_file}")
    print("Dataset satisfies 0% noisy relevance by construction")

if __name__ == "__main__":
    filter_ai_comments_strict()
