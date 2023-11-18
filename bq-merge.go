package main

import (
	"context"
	"fmt"
	"log"

	"cloud.google.com/go/bigquery"
	_ "github.com/go-sql-driver/mysql"
)

// Step 1: Fetch data from MySQL
const (
	mysqlConnString   = "<user>:<password>@tcp(localhost:3306)/<dataset name>"
	bigQueryProjectID = "sa-128-ak"
	datasetID         = "SCD"
	stagingTableID    = "employees-staging"
	mainTableID       = "employees"
)

// Step 2: Merge data in BigQuery

func mergeDataInBigQuery(ctx context.Context, client *bigquery.Client) error {
	mergeSQL := fmt.Sprintf(`
        MERGE %s.%s AS main
        USING %s.%s AS staging
        ON main.employeeNumber = staging.employeeNumber
        WHEN MATCHED THEN
            UPDATE SET main.lastName = staging.lastName,
					   main.firstName = staging.firstName,
					   main.extention = staging.extention,
					   main.email = staging.email,
					   main.officeCode = staging.officeCode,
					   main.reportsTo = staging.reportsTo,
					   main.jobTitle = staging.jobTitle
        WHEN NOT MATCHED THEN
            INSERT (ID, lastName, firstName, extention, email, officeCode, reportsTo, jobTitle) VALUES (ID, lastName, firstName, extention, email, officeCode, reportsTo, jobTitle)`,
		datasetID, mainTableID, datasetID, stagingTableID)

	query := client.Query(mergeSQL)
	_, err := query.Run(ctx)
	return err
}

// Step 4: main process
func main() {
	ctx := context.Background()

	client, err := bigquery.NewClient(ctx, bigQueryProjectID)
	if err != nil {
		log.Fatalf("Failed to create BigQuery client: %v", err)
	}

	if err := mergeDataInBigQuery(ctx, client); err != nil {
		log.Fatalf("Error merging data in BigQuery: %v", err)
	}

	log.Println("Data merge complete")
}

