import pandas as pd
import openai
import time
import os
from dotenv import load_dotenv

# --- Load OpenAI API key ---
load_dotenv("openai_api_key.env")
api_key = os.getenv("OPENAI_API_KEY")
if not api_key:
    raise ValueError("OpenAI API key not found in 'openai_api_key.env'")

# --- Create OpenAI client ---
client = openai.OpenAI(api_key=api_key)

# --- Load combined comments file (Excel) ---
input_file = "eval3.xlsx"
df = pd.read_excel(input_file)

# --- For testing, take only first N comments ---
TEST_LIMIT = 100
if TEST_LIMIT:
    df = df.head(TEST_LIMIT)

comments = df['comment'].tolist()

# --- Configuration ---
batch_size = 15
max_retries = 5
backoff_factor = 2
topic_instruction = """
You are helping classify comments about AI in hiring and recruitment. 
Label each comment as 'yes' if it expresses an **opinion, feeling, or judgment** about the topic, otherwise label as 'no'.

Topic definition:
- Posts about recruiters/applicants thoughts on using AI in hiring
- How AI affects hiring decisions and workforce evaluation
- Experiences with ATS or automated rejections
- Opinions on AI replacing HR tasks
- Allow AI in coding tests counts
Exclude:
- Applicants only using AI without relation to recruitment/hiring

Rules:
- Only label 'yes' if the comment expresses an opinion, perspective, or feeling.
- Label 'no' if the comment is purely factual, informational, or irrelevant.
"""

# --- Helper function to call GPT ---
def classify_batch(batch):
    prompt = topic_instruction + "\n\nClassify each comment as 'yes' or 'no', return comma-separated, same order:\n"
    for idx, c in enumerate(batch, 1):
        prompt += f"{idx}. {c.strip()}\n"

    for attempt in range(max_retries):
        try:
            response = client.chat.completions.create(
                model="gpt-4",
                messages=[{"role": "user", "content": prompt}],
                temperature=0
            )
            text = response.choices[0].message.content
            labels = [x.strip().lower() for x in text.replace("\n", ",").split(",") if x.strip()]
            # Fill missing labels with 'no'
            if len(labels) != len(batch):
                labels += ["no"] * (len(batch) - len(labels))
            return labels
        except openai.error.RateLimitError:
            sleep_time = backoff_factor ** attempt
            print(f"429 RateLimitError, retrying in {sleep_time}s...")
            time.sleep(sleep_time)
        except Exception as e:
            print(f"Other OpenAI error: {e}")
            time.sleep(1)

    # If all retries fail, return 'no'
    return ["no"] * len(batch)

# --- Process in batches ---
all_labels = []
total_batches = (len(comments) + batch_size - 1) // batch_size

for i in range(total_batches):
    batch = comments[i*batch_size:(i+1)*batch_size]
    print(f"Processing batch {i+1}/{total_batches} with {len(batch)} comments")
    labels = classify_batch(batch)
    all_labels.extend(labels)
    time.sleep(1)  # polite pause

# --- Assign labels safely ---
df['related_to_topic'] = all_labels[:len(df)]  # match lengths

# --- Save results ---
output_file = "classified_comments_test.xlsx" if TEST_LIMIT else "classified_comments.xlsx"
df.to_excel(output_file, index=False)
print(f"Done! Saved to {output_file}")
