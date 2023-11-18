
# MySQL to BigQuery Data Transfer

This project provides a Go script for transferring data from a MySQL database to a Google BigQuery table. The script fetches data from a specified MySQL table, truncates a corresponding BigQuery staging table, and then inserts the data into this staging table.

## Prerequisites

Before you begin, ensure you have the following:

- Go (Golang) installed on your system.
- Access to a MySQL database with data to transfer.
- Access to a Google Cloud project with BigQuery enabled.
- A BigQuery dataset and table where the data will be inserted.

## Setup

1. Google Cloud Service Account: Create a service account in your Google Cloud project with permissions to access BigQuery. Download the service account key JSON file.

2. Configure MySQL Connection: Update the MySQL connection string in the script with the correct credentials and database details.

3. BigQuery Configuration: Specify your Google Cloud project ID, dataset ID, and table ID in the script.

4. Install Go Dependencies: Run the following commands to install required Go packages:

    ```bash
    go get -u github.com/go-sql-driver/mysql
    go get -u cloud.google.com/go/bigquery
    ```

## Usage

1. Run the Script: Execute the script to transfer data from MySQL to BigQuery.

    ```bash
    go run mysql-to-bq.go
    go run mq-merge.go    #To merge you staging table to main table in BigQuery
    ```

2. Verify Data Transfer: Check your BigQuery table to confirm that the data has been transferred correctly.

## Script Overview

- The script establishes a connection to the MySQL database and fetches data from the specified table.
- It then connects to BigQuery, truncates the specified staging table, and inserts the fetched data.
- The transfer process replaces the existing data in the BigQuery staging table with the new data from MySQL.

## Customization

You can customize the script by modifying the following:

- MySQL Query: Change the SQL query to fetch data according to your requirements.
- Data Mapping: Modify the `MySQLData` and `BigQueryData` structs to match the schema of your MySQL table and BigQuery table.
- Modify the mergeDataInBigQuery function to align with your table schema and business logic for merging data.
- Error Handling: Enhance error handling as per your operational needs.

## Troubleshooting

- Ensure that the service account has the necessary permissions in BigQuery.
- Verify that the MySQL and BigQuery table schemas are compatible.
- Check for network connectivity issues between your system and Google Cloud.

## Support

For support, please open an issue in the repository or contact your system administrator.

---