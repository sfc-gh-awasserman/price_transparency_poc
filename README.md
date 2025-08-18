Snowflake Price Transparency File Ingestion POC
Introduction
This proof-of-concept (POC) demonstrates a pipeline for parsing and ingesting healthcare price transparency files into Snowflake for analysis. The process involves setting up an external stage in Microsoft Azure and using a series of SQL scripts to configure the Snowflake environment and process the data.

🧪 Lab 1: Setup and Data Ingestion
This lab will guide you through setting up the necessary architecture and running the data ingestion process.

1. Architecture Setup
First, we will establish a connection between Snowflake and your Azure Blob Storage account. This involves creating a storage integration and an external stage.

Configure Azure Storage Integration:

Open the storage_setup.sql file.

Fill in your specific Azure account details where indicated.

Follow the official Snowflake documentation to complete the storage integration setup: Configure an Azure container for loading data. This guide will walk you through granting Snowflake the required access permissions to your Azure container.

2. Account Setup
Next, you will run a script to create the necessary database objects (tables, stored procedures, etc.) in your Snowflake account.

Run Setup Script:

Execute the entire poc_setup.sql script in your Snowflake worksheet. This will build all the components required to process the price transparency files.

3. Data Processing
With the architecture and account configured, you can now begin the data ingestion process.

Start the Ingestion:

Open the data_ingest.sql file.

Run the code in the script to kick off the pipeline. This will start the process of pulling files from your Azure stage, processing them, and storing the data as parquet files in the external stage. We will load this data into Snowflake native tables in future labs. 