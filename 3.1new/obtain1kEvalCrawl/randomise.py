import pandas as pd

# Read the source sheet
df = pd.read_excel("evalCrawl.xlsx", sheet_name="eval1k", header=None)

# Randomly sample 1000 rows
sample_df = df.sample(n=1000, random_state=42)  # random_state ensures reproducibility

# Write to a new sheet or file
with pd.ExcelWriter("eval1kFinal.xlsx") as writer:
    sample_df.to_excel(writer, sheet_name="eval1kFinal", index=False, header=False)
