package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"cloud.google.com/go/bigquery"
	_ "github.com/go-sql-driver/mysql"
)

type MySQLData struct {
	// Define fields according to your MySQL table's schema
	employeeNumber int
	lastName       string
	firstName      string
	extension      string
	email          string
	officeCode     int
	reportsTo      sql.NullInt64
	jobTitle       string

	// Add other fields as necessary
}

type BigQueryData struct {
	// Define fields according to your BigQuery table's schema
	employeeNumber int
	lastName       string
	firstName      string
	extension      string
	email          string
	officeCode     int
	reportsTo      sql.NullInt64
	jobTitle       string
	// Add other fields as necessary
}

func truncateBigQueryTable(ctx context.Context, client *bigquery.Client, datasetID, tableID string) error {
	// Construct and execute a DELETE FROM query to truncate the table
	query := client.Query(fmt.Sprintf("DELETE FROM `%s.%s` WHERE TRUE", datasetID, tableID))
	job, err := query.Run(ctx)
	if err != nil {
		return err
	}
	_, err = job.Wait(ctx)
	return err
}
func insertIntoBigQuery(data []MySQLData) {
	ctx := context.Background()
	projectID := "sa-128-ak"                   // Replace with your Google Cloud project ID
	client, err := bigquery.NewClient(ctx, projectID) //, option.WithCredentialsFile("path/to/credentials.json"))
	if err != nil {
		log.Fatalf("Failed to create BigQuery client: %v", err)
	}

	datasetID := "SCD"
	tableID := "employees-staging"

	u := client.Dataset(datasetID).Table(tableID).Inserter()

	var bqData []bigquery.ValueSaver

	for _, d := range data {
		var reportsToValue bigquery.Value
		if d.reportsTo.Valid {
			reportsToValue = d.reportsTo.Int64
		} else {
			reportsToValue = nil // BigQuery will treat this as NULL
		}

		bqRow := &bigquery.ValuesSaver{
			Schema: bigquery.Schema{
				{Name: "employeeNumber", Type: bigquery.IntegerFieldType},
				{Name: "lastName", Type: bigquery.StringFieldType},
				{Name: "firstName", Type: bigquery.StringFieldType},
				{Name: "extension", Type: bigquery.StringFieldType},
				{Name: "email", Type: bigquery.StringFieldType},
				{Name: "officeCode", Type: bigquery.IntegerFieldType},
				{Name: "reportsTo", Type: bigquery.IntegerFieldType},
				{Name: "jobTitle", Type: bigquery.StringFieldType},
			},
			Row: []bigquery.Value{d.employeeNumber, d.lastName, d.firstName, d.extension, d.email,
				d.officeCode, reportsToValue, d.jobTitle},
		}
		bqData = append(bqData, bqRow)
	}
	// var bqData []BigQueryData
	// for _, d := range data {
	// 	bqRow := BigQueryData{
	// 		employeeNumber: d.employeeNumber,
	// 		email:          d.email,
	// 		// Map other fields
	// 	}
	// 	bqData = append(bqData, bqRow)
	//

	fmt.Println(bqData)

	if err := u.Put(ctx, bqData); err != nil {
		log.Fatalf("Failed to insert data into BigQuery: %v", err)
	}
}

func main() {
	// Connect to MySQL
	db, err := sql.Open("mysql", "<user>:<password>@tcp(localhost:3306)/<dataset name>")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Execute a query
	rows, err := db.Query("SELECT * FROM employees")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	var data []MySQLData

	for rows.Next() {
		var d MySQLData
		if err := rows.Scan(&d.employeeNumber, &d.lastName, &d.firstName,
			&d.extension, &d.email, &d.officeCode, &d.reportsTo, &d.jobTitle); err != nil {
			log.Fatal(err)
		}
		data = append(data, d)

		//println(d.employeeNumber, d.lastName, d.firstName, d.extension, d.email, d.officeCode, d.reportsTo, d.jobTitle)
	}

	if err := rows.Err(); err != nil {
		log.Fatalf("Failed to insert rows: %v", err)
	}

	// Truncate the BigQuery table

	ctx := context.Background()
	datasetID := "SCD"
	tableStagingID := "employees-staging"
	projectID := "sa-128-ak" // Replace with your Google Cloud project ID

	client, err := bigquery.NewClient(ctx, projectID) //, option.WithCredentialsFile("path/to/credentials.json"))
	if err != nil {
		log.Fatalf("Failed to create BigQuery client: %v", err)
	}

	if err := truncateBigQueryTable(context.Background(), client, datasetID, tableStagingID); err != nil {
		log.Fatalf("Failed to truncate table: %v", err)
	}

	insertIntoBigQuery(data)

	log.Println("Rows successfully inserted into BigQuery table")
	// Now `data` contains the data from MySQL
	// Next step: Insert into BigQuery
}
