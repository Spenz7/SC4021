import pandas as pd
import json

# Load your Excel file
df = pd.read_excel("ai_filtered_three_sheets.xlsx", sheet_name="All_Unique_Comments")

# Select only the two columns you want
# Adjust the column names if your Excel file uses different headers
df = df[['post_title', 'comment_text']]

# Convert to a list of dicts
records = df.to_dict(orient='records')

# Save to JSON
with open("sample_comments.json", "w", encoding="utf-8") as f:
    json.dump(records, f, ensure_ascii=False, indent=2)

print(f"Saved {len(records)} records to sample_comments.json")
