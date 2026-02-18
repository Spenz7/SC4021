Mtd1 (faster but may miss out on posts that r related but dh title approved by combination mtd)
Step 1: Crawl posts only (saves post metadata)
         ↓
Step 2: Filter posts using combination method on title+selftext
         ↓
Step 3: Crawl comments ONLY for relevant posts
         ↓
Step 4: Comments are already highly relevant (80-90%)
         ↓
Step 5: Sample to 10k if needed

Mtd2 V1 
Step 1: Crawl posts AND all comments together (haven't included selftext, if have time crawl it too)
         ↓
Step 2: Save ALL comments to JSONL files (50k+ comments) + create excel containing these 50k+ comments
         ↓
Step 3: Filter ALL comments using combination method 
        (checking each comment's text for relevance)
         ↓
Step 4: Found X relevant comments out of 50k total
         ↓
Step 5: If relevant > 10k, sample down to 10k balanced dataset
         ↓
Step 6: Create eval.xlsx with 1k random comments for manual labeling

Steps 1-4:
1) fullCrawl.py -> generates jsonl files stored in jsonl_crawl_full folder (maybe modify it st each son file includes post's body too) + crawl_progress.json and seen_posts.json so that u won't include posts that you've crawled b4 next time u run the prog -> convertAllJsonlToExcelb4filterComments.py -> generates sheet w all comments, i.e. all_raw_comments -> filterComments.py (directly looks thru comments and accepts those that contain certain keyword combinations) -> generates excel file all_relevant_comments


OR
Mtd2 V2 (what I did)
Step3: Filter all post titles with AI in it and store its comments in an excel sheet
Then skip straight to Step 6

Steps 1-3:
1) fullCrawl.py -> generates jsonl files stored in jsonl_crawl_full folder (maybe modify it st each son file includes post's body too) + crawl_progress.json and seen_posts.json so that u won't include posts that you've crawled b4 next time u run the prog -> convertAllJsonlToExcelb4filterComments.py -> generates sheet w all comments, i.e. all_raw_comments -> obtainAIrelatedPostPlusComments.py -> generates excel file (w all comments frm posts with 'AI' in its title in 1 sheet, another sheet where comments contain "AI", then 3rd sheet that has sheet1+sheet2 wo dup (dup initially exists due to comments fulfilling both cond), named ai_filtered_three_sheets.xlsx

Note: rn u accept post titles w "AI" in it + any comment that has "AI" in it (case-sensitive), if too much then only accept
either (maybe only accept post titles w "AI" in it)
we r basically searching keywords in reddit's search bar and pick the top 100 relevant posts. Then we accept posts that contain "AI" in their title, or any comments that contain "AI" in it


Mtd2 optional: extractPostTitles.py (reads an excel file and extracts all post titles once each st u end up w unique list of post titles no dup)
-> generates excel file unique_post_titles
