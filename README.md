# SC4021 – Information Retrieval

Group 39

## Indexing with Apache Solr

This section describes how to maintain the existing Solr setup for this assignment. The core (`climate_change_core`) and initial schema are already created; use this guide when you need to edit the schema and reindex the data.

### 1. Prerequisites

- Java 21 installed and available on the system path.
- Apache Solr 10.0.0 binary release (already included in the `solr-10.0.0` directory of this repository).
- The cleaned dataset file `cleaned_data.csv` (located in the `indexing` folder).

### 2. Start Solr

Open a terminal and navigate to the Solr folder:

```bash
cd solr-10.0.0
```

Run the appropriate start command:

- **Windows**

	```bash
	bin\solr.cmd start
	```

- **macOS / Linux**

	```bash
	bin/solr start
	```

By default, Solr runs on port `8983`.

### 3. Access the Solr Admin UI

Open a browser and go to:

- http://localhost:8983/solr/

Select the `climate_change_core` core from the top-left dropdown.

### 4. Edit the Schema (Schema Designer)

We use the **Schema Designer** tab in the Solr Admin UI to maintain the schema for `climate_change_core` (e.g., adding new fields, changing field types).

Typical maintenance workflow in Schema Designer:

1. **Review existing field types** – check that current types (string, text with analyzers, numeric types, etc.) still fit the updated data.
2. **Update or add fields** – modify existing fields or create new fields that match any changes to the columns in `cleaned_data.csv`, assigning appropriate field types.
3. **Publish schema** – after making changes, click **Publish** to apply the updated schema to the core.

Always publish the schema before reindexing data so that Solr interprets all fields correctly.

Official guide: https://solr.apache.org/guide/solr/latest/indexing-guide/schema-designer.html

### 5. Reindex the Data After Schema Changes

Whenever the schema is changed (e.g., fields added/removed or types updated), you should reindex the data so all documents conform to the new schema.

Recommended steps:

1. **Delete existing documents** from `climate_change_core` (see Section 6 below).
2. **Post the updated CSV** to Solr.

From within the `solr-10.0.0` folder, assuming `cleaned_data.csv` is in the `indexing` folder at the project root, run:

```bash
bin\solr.cmd post -c climate_change_core ..\indexing\cleaned_data.csv
```

On macOS / Linux:

```bash
bin/solr post -c climate_change_core ../indexing/cleaned_data.csv
```

After posting, documents from `cleaned_data.csv` will be reindexed into `climate_change_core` and can be queried via the Solr UI or Solr APIs.

### 6. Deleting All Documents from the Core

To clear all indexed documents in `climate_change_core` without dropping the core:

1. Go to the Solr Admin UI: http://localhost:8983/solr/
2. Select the `climate_change_core` core.
3. Open the **Documents** tab.
4. Set **Document Type** to **Solr Command (raw XML or JSON)**.
5. In the body, enter the JSON delete command:

	 ```json
	 {"delete": {"query": "*:*"}}
	 ```

6. Click **Submit** to execute the delete command.

This removes all documents but keeps the core and its schema.

### 7. Stop Solr

To stop the Solr server running on port 8983 (Windows example):

```bash
bin\solr.cmd stop -p 8983
```

On macOS / Linux:

```bash
bin/solr stop -p 8983
```

Solr should now be fully shut down.

