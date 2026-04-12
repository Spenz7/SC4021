# SC4021 – Information Retrieval

Group 39

Note: For Task 3.1 Crawling, since it's a fairly independent README wise, I've included a separate README for it inside the folder titled "3.1"

Below is the README for the tasks after 3.1

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
	
	Open a terminal and navigate to the Solr folder:
	```bash
	cd solr-10.0.0
	```

	Run the appropriate start command:

	- On Windows:
		```bash
		bin\solr.cmd start
		```
	- On macOS / Linux:
		```bash
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

## Solr Maintenance

- **Access Admin UI**: http://localhost:8983/solr → Select `climate_change_core`.
- **Edit Schema**: Use the *Schema Designer* tab to maintain the schema, then click *Publish* when done editing.
- **Delete All Documents**: In *Documents* tab, set **Document Type** to **Solr Command (raw XML or JSON)** and submit:

	```json
	{"delete": {"query": "*:*"}}
	```

- **Reindex Dataset**: Whenever the schema is changed (e.g., fields added/removed or types updated), you should delete existing data and reindex the data so all documents conform to the new schema.

	On Windows:
	```bash
	bin\solr.cmd post -c climate_change_core ..\indexing\cleaned_data.csv
	```
	On macOS / Linux:
	```bash
	bin/solr post -c climate_change_core ../indexing/cleaned_data.csv
	```

- **Stop Solr** (from `solr-10.0.0`):

	On Windows:
	```bash
	bin\solr.cmd stop -p 8983
	```
	On macOS / Linux:
	```bash
	bin/solr stop -p 8983
	```
