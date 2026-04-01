# SC4021 – Information Retrieval

Group 39

## Quick Start: End-to-End Flow

1. **Prerequisites**  
   - Python 3
   - Java 21
   - Apache Solr 10.0.0 (provided in `solr-10.0.0`)

2. **Install Python dependencies** (from project root):

	```bash
	pip install -r requirements.txt
	```

3. **Start Solr and the core**

	```bash
	cd solr-10.0.0
	# Windows
	bin\solr.cmd start
	# macOS / Linux
	bin/solr start
	```

Solr runs on http://localhost:8983/solr and the core used is `climate_change_core`.

4. **Run the search UI** (in a new terminal from project root):

	```bash
	cd indexing
	python search_ui.py
	```

5. **Use the UI**  
   Open http://localhost:5000 in your browser.

## Solr Maintenance (Brief)

- **Access Admin UI**: http://localhost:8983/solr → select `climate_change_core`.
- **Edit schema**: use the *Schema Designer* tab, then click *Publish*.
- **Reindex dataset** (after schema changes, from `solr-10.0.0`, assuming `indexing/cleaned_data.csv` exists):

	```bash
	bin\solr.cmd post -c climate_change_core ..\indexing\cleaned_data.csv
	# macOS / Linux
	bin/solr post -c climate_change_core ../indexing/cleaned_data.csv
	```

- **Delete all documents** (keep core): in *Documents* tab, choose *Solr Command* and submit:

	```json
	{"delete": {"query": "*:*"}}
	```

- **Stop Solr** (from `solr-10.0.0`):

	```bash
	bin\solr.cmd stop -p 8983
	# macOS / Linux
	bin/solr stop -p 8983
	```

