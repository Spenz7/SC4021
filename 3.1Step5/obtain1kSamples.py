import pandas as pd

# Load Sheet 3
df = pd.read_excel("ai_filtered_three_sheets.xlsx", sheet_name="All_Unique_Comments")

# Random sample 1000 records
sample_df = df.sample(n=2000, random_state=42)

# Save
sample_df.to_excel("eval_sample_1k.xlsx", index=False)

# Check your sample proportions
print("Sample breakdown:")
print(sample_df['conditions_met'].value_counts())
print(f"\nPercentages:")
for condition, count in sample_df['conditions_met'].value_counts().items():
    pct = count/1000*100
    print(f"  {condition}: {pct:.1f}%")
