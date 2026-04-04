### What each file does

### 3.1Final

-   **~$10kTrainingData.xlsx** -- Temporary/auto-generated Excel file created by Excel, can be ignored.
-   **10kTrainingData.xlsx** -- Training dataset containing 10,000 comments, balanced for sentiment, ready for classification tasks.
-   **88kTrainingData.xlsx** -- Larger training dataset with 88,000 comments, used if more extensive model training is desired.
-   **eval.xls** -- Evaluation dataset containing 1,000 randomly sampled, manually verified comments for sentiment labeling.

### jsonl_crawl_manual

-   **reddit_manual.jsonl** -- JSON Lines file storing all manually crawled Reddit comments (~11,000), with fields for comment ID, text, timestamp, source, and metadata.

### obtain1kEvalCrawl

-   **evalCrawl.xlsx** -- Filtered ~2,000 relevant comments containing the keyword "climate change".
-   **eval1kFinal.xlsx** -- Randomly sampled 1,000 comments from `evalCrawl.xlsx`, used to create the evaluation dataset.
-   **randomise.py** -- Script to randomly select 1,000 comments from `evalCrawl.xlsx` and save as `eval1kFinal.xlsx`.

### Scripts

-   **fullCrawl.py** -- Python crawler that fetches comments from Reddit posts listed in `reddit_links.txt`, skips deleted/removed comments, recursively retrieves replies, and avoids duplicates using `seen_posts.json`.
-   **jsonlToExcel.py** -- Converts the JSON Lines file (`reddit_manual.jsonl`) into Excel format (`reddit_manual.xlsx`) for easier evaluation and filtering.
-   **q1p3.py** -- Computes statistics for the crawled corpus, including number of records, total words, and unique word types for both 2k and 1k datasets.

### Supporting files

-   **reddit_links.txt** -- List of manually curated Reddit post URLs used as input for crawling.
-   **reddit_manual.xlsx** -- Excel version of `reddit_manual.jsonl` containing all manually crawled comments.
-   **seen_posts.json** -- Tracks posts already crawled to prevent duplicates in repeated runs.
-   **.DS_Store** -- macOS system file; can be ignored.
-   **README.txt** -- Documentation for the 3.1 folder contents.


Execution Order and Workflow for 3.1 Crawling
---------------------------------------------

1.  **`fullCrawl.py`**
    -   Purpose: Crawls Reddit comments from manually curated URLs in `reddit_links.txt`.
    -   Actions:
        -   Skips deleted/removed comments.
        -   Skips posts with fewer than 25 comments.
        -   Recursively collects nested replies.
        -   Appends data to `jsonl_crawl_manual/reddit_manual.jsonl`.
        -   Updates `seen_posts.json` to avoid duplicate crawling.
        -   Note: Since we’ve already ran fullCrawl.py to crawl all the links from reddit_links.txt with >=25 comments, running fullCrawl.py again should not add any new comments to our reddit_manual.jsonl file mentioned below 

    -   **Folder created automatically:** `jsonl_crawl_manual`.
2.  **`jsonlToExcel.py`**
    -   Purpose: Converts `reddit_manual.jsonl` into Excel format for easier inspection.
    -   Output: `reddit_manual.xlsx` inside `jsonl_crawl_manual/`.
    -   Next step: Manually remove duplicates and filter for comments containing "climate change".
3.  **Manual filtering**
    -   Purpose: Reduce to relevant comments (~2,000).
    -   **Folder manually created:** `obtain1kEvalCrawl/`.
    -   Files saved here:
        -   `evalCrawl.xlsx` -- contains the ~2k relevant comments.
        -   `randomise.py` -- used for random sampling.
4.  **`randomise.py`**
    -   Purpose: Randomly selects 1,000 comments from `evalCrawl.xlsx`.
    -   Output: `eval1kFinal.xlsx` inside `obtain1kEvalCrawl/`.
5.  **Final evaluation dataset**
    -   `eval1kFinal.xlsx` → copied over as `eval.xls` in `3.1Final/` and then labelling was done for it there.
    -   **Folder manually created:** `3.1Final/`.
    -   Files stored here:
        -   `10kTrainingData.xlsx` -- subset of public dataset for training.
        -   `88kTrainingData.xlsx` -- larger training set.
        -   `eval.xls` -- final evaluation set (~1,000 comments).
6.  **`q1p3.py`**
    -   Purpose: Computes statistics (#records, total words, unique word types) for any dataset.
    -   Can be run on: `reddit_manual.jsonl`, `evalCrawl.xlsx`, or `eval.xlsx`.

* * * * *

**Notes:**

-   The only fully automated steps are crawling (`fullCrawl.py`) and conversion to Excel (`jsonlToExcel.py`).
-   Manual steps are required for filtering relevant comments and saving final evaluation files.
-   Random sampling is automated via `randomise.py` but depends on the filtered file `evalCrawl.xlsx`.
