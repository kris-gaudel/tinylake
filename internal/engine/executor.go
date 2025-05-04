package engine

import (
	"fmt"
	"strconv"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"

	"github.com/kris-gaudel/tinylake/internal/queryparser"
)

// ExecuteQuery evaluates the parsed query against the given Arrow record.
func ExecuteQuery(q *queryparser.Query, table array.Record) (array.Record, error) {
	pool := memory.NewGoAllocator()
	totalRows := int(table.NumRows())

	// Step 1: Filter rows based on WHERE condition
	passIndices := make([]int, 0, totalRows)
	for row := 0; row < totalRows; row++ {
		pass := true
		if q.Where != nil {
			result, err := evaluateExpression(q.Where, table, row)
			if err != nil {
				return nil, err
			}
			boolResult, ok := result.(bool)
			if !ok {
				return nil, fmt.Errorf("WHERE clause did not evaluate to boolean at row %d", row)
			}
			pass = boolResult
		}
		if pass {
			passIndices = append(passIndices, row)
		}
	}

	// Step 2: Project columns or expressions
	projectedArrays := []array.Interface{}
	projectedFields := []arrow.Field{}

	for projIdx, expr := range q.Projections {
		switch e := expr.(type) {
		case *queryparser.ColumnRef:
			// Simple column projection
			colIdx := findColumnIndex(table, e.Name)
			if colIdx == -1 {
				return nil, fmt.Errorf("column %s not found", e.Name)
			}
			projectedArrays = append(projectedArrays, table.Column(colIdx))
			projectedFields = append(projectedFields, table.Schema().Field(colIdx))

		default:
			// Expression projection: compute per row
			builder := array.NewFloat64Builder(pool)
			defer builder.Release()

			for _, row := range passIndices {
				val, err := evaluateExpression(expr, table, row)
				if err != nil {
					return nil, fmt.Errorf("eval error in projection %d: %w", projIdx, err)
				}
				if val == nil {
					builder.AppendNull()
				} else {
					builder.Append(toFloat(val))
				}
			}

			arr := builder.NewArray()
			defer arr.Release()
			projectedArrays = append(projectedArrays, arr)

			projectedFields = append(projectedFields, arrow.Field{
				Name:     fmt.Sprintf("expr_%d", projIdx),
				Type:     arrow.PrimitiveTypes.Float64,
				Nullable: true,
			})
		}
	}

	// Step 3: Build and return output record
	schema := arrow.NewSchema(projectedFields, nil)
	outRecord := array.NewRecord(schema, projectedArrays, int64(len(passIndices)))
	return outRecord, nil
}

func evaluateExpression(expr queryparser.Expression, table array.Record, row int) (interface{}, error) {
	switch e := expr.(type) {
	case *queryparser.ColumnRef:
		colIdx := findColumnIndex(table, e.Name)
		if colIdx == -1 {
			return nil, fmt.Errorf("column %s not found", e.Name)
		}
		colArr := table.Column(colIdx)

		switch arr := colArr.(type) {
		case *array.Float64:
			if !arr.IsValid(row) {
				return nil, nil
			}
			return arr.Value(row), nil
		case *array.String:
			if !arr.IsValid(row) {
				return nil, nil
			}
			return arr.Value(row), nil
		default:
			return nil, fmt.Errorf("unsupported column type: %T", arr)
		}

	case *queryparser.Literal:
		if v, err := strconv.ParseFloat(e.Value, 64); err == nil {
			return v, nil
		}
		return e.Value, nil

	case *queryparser.BinaryExpr:
		leftVal, _ := evaluateExpression(e.Left, table, row)
		rightVal, _ := evaluateExpression(e.Right, table, row)

		switch e.Op {
		case "AND":
			return toBool(leftVal) && toBool(rightVal), nil
		case "OR":
			return toBool(leftVal) || toBool(rightVal), nil
		case ">":
			return toFloat(leftVal) > toFloat(rightVal), nil
		case "<":
			return toFloat(leftVal) < toFloat(rightVal), nil
		case "=":
			return leftVal == rightVal, nil
		case "+":
			return toFloat(leftVal) + toFloat(rightVal), nil
		case "-":
			return toFloat(leftVal) - toFloat(rightVal), nil
		case "*":
			return toFloat(leftVal) * toFloat(rightVal), nil
		case "/":
			return toFloat(leftVal) / toFloat(rightVal), nil
		default:
			return nil, fmt.Errorf("unsupported operator: %s", e.Op)
		}

	default:
		return nil, fmt.Errorf("unsupported expression type: %T", expr)
	}
}

func toFloat(v interface{}) float64 {
	switch val := v.(type) {
	case float64:
		return val
	case string:
		f, _ := strconv.ParseFloat(val, 64)
		return f
	default:
		return 0
	}
}

func toBool(v interface{}) bool {
	switch val := v.(type) {
	case bool:
		return val
	case float64:
		return val != 0
	case string:
		return val != ""
	default:
		return false
	}
}

func findColumnIndex(table array.Record, name string) int {
	schema := table.Schema()
	for i, field := range schema.Fields() {
		if field.Name == name {
			return i
		}
	}
	return -1
}
