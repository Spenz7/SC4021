# SC4021 – Information Retrieval

Group 39

## Indexing with Apache Solr

This section describes how we set up Apache Solr and indexed the climate change dataset for this assignment.

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

### 4. Define the Schema (Schema Designer)

We use the **Schema Designer** tab in the Solr Admin UI to configure the schema for `climate_change_core`.

Overall workflow in Schema Designer:

1. **Add field types** – define appropriate types (e.g., string, text with analyzers, numeric types) for the dataset.
2. **Add fields** – create fields that match the columns in `cleaned_data.csv` and assign them to the previously defined field types.
3. **Publish schema** – after editing, click **Publish** to apply the schema changes to the core.

The schema must be published before indexing data so that Solr can interpret all fields correctly.

Official guide: https://solr.apache.org/guide/solr/latest/indexing-guide/schema-designer.html

### 5. Upload (Index) the Data

Ensure `cleaned_data.csv` is accessible from the Solr `bin` directory (either by running the command from the directory containing the file or by providing the full path).

Example (from within `solr-10.0.0`):

```bash
bin\solr.cmd post -c climate_change_core path\to\cleaned_data.csv
```

After posting, documents from `cleaned_data.csv` will be indexed into `climate_change_core` and can be queried via the Solr UI or Solr APIs.

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

