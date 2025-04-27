package arrowengine

import (
	"fmt"
	"os"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	arrowcsv "github.com/apache/arrow/go/arrow/csv"
)

func LoadCSVToArrowTable(filePath string) (array.Record, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer f.Close()

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "Date", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "Open", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "High", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "Low", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "Close", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "Volume", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "Market Cap", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
	}, nil)

	reader := arrowcsv.NewReader(f, schema, arrowcsv.WithHeader(true), arrowcsv.WithChunk(-1))
	defer reader.Release()

	ok := reader.Next()
	if !ok {
		if err := reader.Err(); err != nil {
			return nil, err
		}
		return nil, nil
	}
	rec := reader.Record() // get the Arrow Record containing all CSV rows

	rec.Retain()
	reader.Release()

	return rec, nil
}
