package main

import (
	"fmt"
	"log"

	"github.com/apache/arrow/go/arrow/array"
	"github.com/kris-gaudel/tinylake/internal/arrowengine"
	"github.com/kris-gaudel/tinylake/internal/engine"
	"github.com/kris-gaudel/tinylake/internal/queryparser"
)

func main() {
	filePath := "data/sample.csv"
	record, err := arrowengine.LoadCSVToArrowTable(filePath)
	if err != nil {
		log.Fatalf("Failed to load CSV: %v", err)
	}

	fmt.Println("Loaded Arrow Table successfully!")
	fmt.Println("Schema:", record.Schema())
	fmt.Println("Record count:", record.NumRows())

	// Test query
	// queryStr := "SELECT Date, Close FROM prices WHERE Close > 8000.2 AND Close < 9000.2"
	// queryStr := "SELECT Date FROM prices WHERE (Open + Close) / 2 > 5000.2 AND (Open + Close) / 2 < 6000.2"
	// queryStr := "SELECT (Open + Close) / 2, Volume * 100 FROM prices WHERE Close > 1000.2"
	// queryStr := "SELECT COUNT(*) FROM prices"
	// queryStr := "SELECT SUM(Close) FROM prices"
	// queryStr := "SELECT AVG(Volume) FROM prices"
	// queryStr := "SELECT MAX(Close) FROM prices"
	// queryStr := "SELECT MIN(Open) FROM prices"
	// queryStr := "SELECT COUNT(*) FROM prices WHERE Close > 1000"
	// queryStr := "SELECT Date, COUNT(*) FROM prices GROUP BY Date"
	queryStr := "SELECT Date, AVG((High + Low) / 2) FROM prices GROUP BY Date"
	parser := queryparser.NewParser(queryStr)
	query := parser.Parse()

	fmt.Println("Parsed Query:", query.String())

	// Execute the query
	result, err := engine.ExecuteQuery(query, record)
	if err != nil {
		log.Fatalf("query execution failed: %v", err)
	}
	defer result.Release()

	fmt.Println("Query executed successfully.")

	// Pretty-print Result
	printRecord(result)
	fmt.Println("Number of rows in result:", result.NumRows())
}

// Utility function to pretty-print an Arrow Record
func printRecord(rec array.Record) {
	fmt.Println("Result Table:")
	for colIdx := 0; colIdx < int(rec.NumCols()); colIdx++ {
		fmt.Printf("%-20s", rec.ColumnName(colIdx))
	}
	fmt.Println()

	numRows := int(rec.NumRows())
	for row := 0; row < numRows; row++ {
		for colIdx := 0; colIdx < int(rec.NumCols()); colIdx++ {
			col := rec.Column(colIdx)
			if col.IsValid(row) {
				switch col := col.(type) {
				case *array.String:
					fmt.Printf("%-20s", col.Value(row))
				case *array.Float64:
					fmt.Printf("%-20.2f", col.Value(row))
				default:
					fmt.Printf("%-20v", "unsupported")
				}
			} else {
				fmt.Printf("%-20s", "NULL")
			}
		}
		fmt.Println()
	}
}
