# Snowflake Price Transparency File Ingestion POC

## Introduction

This proof-of-concept (POC) demonstrates a pipeline for parsing and ingesting healthcare price transparency files into Snowflake for analysis. This uses the codebase from the Snowflake Labs repository that can be found [here](https://github.com/Snowflake-Labs/CMSgov-pricing-transparency) The process involves setting up an external stage in Microsoft Azure and using a series of SQL scripts to configure the Snowflake environment and process the data.

---

## 🧪 Lab 1: Setup and Data Ingestion

This lab will guide you through setting up the necessary architecture and running the data ingestion process.

### 1. Architecture Setup

First, we will establish a connection between Snowflake and your Azure Blob Storage account. This involves creating a storage integration and an external stage.

1.  Open the `storage_setup.sql` file.
2.  Fill in your specific Azure account details where indicated.
3.  Follow the official Snowflake documentation to complete the storage integration setup: [Configure an Azure container for loading data](https://docs.snowflake.com/en/user-guide/data-load-azure-config). This guide will walk you through granting Snowflake the required access permissions to your Azure container.

### 2. Account Setup

Next, you will run a script to create the necessary database objects (tables, stored procedures, etc.) in your Snowflake account.

* Execute the entire `poc_setup.sql` script in your Snowflake worksheet. This will build all the components required to process the price transparency files.

### 3. Data Processing

With the architecture and account configured, you can now begin the data ingestion process.

1. Connect to Snowflake with SnowSQL (CLI client), and put the price transparency file in the data directory of the internal stage: put file:///pathtofile/price_transparency.json @data_stg/data auto_compress = false;
2. Open the `data_ingest.sql` file and execute the stored procedure to kick off the pipeline. This will start the process of parsing the price transparency file and writing it to the Azure stage. 

---