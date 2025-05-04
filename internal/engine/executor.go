package engine

import (
	"fmt"
	"strconv"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"

	"github.com/kris-gaudel/tinylake/internal/queryparser"
)

// ExecuteQuery executes the parsed query on the given Arrow Record.
func ExecuteQuery(q *queryparser.Query, table array.Record) (array.Record, error) {
	pool := memory.NewGoAllocator()

	totalRows := int(table.NumRows())

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
				return nil, err
			}
			pass = boolResult
		}
		if pass {
			passIndices = append(passIndices, row)
		}
	}

	projectedArrays := []array.Interface{}
	projectedFields := []arrow.Field{}

	for _, expr := range q.Projections {
		colRef, ok := expr.(*queryparser.ColumnRef)
		if !ok {
			return nil, fmt.Errorf("unsupported projection type: %T", expr)
		}

		colIdx := findColumnIndex(table, colRef.Name)
		if colIdx == -1 {
			return nil, fmt.Errorf("column %s not found in table", colRef.Name)
		}
		colArr := table.Column(colIdx)
		colField := table.Schema().Field(colIdx)

		builder := array.NewBuilder(pool, colField.Type)
		defer builder.Release()

		switch b := builder.(type) {
		case *array.Float64Builder:
			src := colArr.(*array.Float64)
			for _, i := range passIndices {
				if src.IsValid(i) {
					b.Append(src.Value(i))
				} else {
					b.AppendNull()
				}
			}
			arr := b.NewArray()
			projectedArrays = append(projectedArrays, arr)
			defer arr.Release()

			projectedFields = append(projectedFields, colField)

		case *array.StringBuilder:
			src := colArr.(*array.String)
			for _, i := range passIndices {
				if src.IsValid(i) {
					b.Append(src.Value(i))
				} else {
					b.AppendNull()
				}
			}
			projectedArrays = append(projectedArrays, b.NewArray())
			projectedFields = append(projectedFields, colField)

		default:
			// Add more types here if needed
			return nil, fmt.Errorf("unsupported column type: %T", b)
		}
	}

	outSchema := arrow.NewSchema(projectedFields, nil)
	outRecord := array.NewRecord(outSchema, projectedArrays, int64(len(passIndices)))

	return outRecord, nil
}

func evaluateExpression(expr queryparser.Expression, table array.Record, row int) (interface{}, error) {
	switch e := expr.(type) {
	case *queryparser.ColumnRef:
		colIdx := findColumnIndex(table, e.Name)
		if colIdx == -1 {
			return nil, fmt.Errorf("column %s not found in table", e.Name)
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
		// Try to parse as float first
		if v, err := strconv.ParseFloat(e.Value, 64); err == nil {
			return v, nil
		}
		// Fallback as string
		return e.Value, nil

	case *queryparser.BinaryExpr:
		leftVal, _ := evaluateExpression(e.Left, table, row)
		rightVal, _ := evaluateExpression(e.Right, table, row)

		switch e.Op {
		// Logical operators
		case "AND":
			return toBool(leftVal) && toBool(rightVal), nil
		case "OR":
			return toBool(leftVal) || toBool(rightVal), nil

		// Comparison operators
		case ">":
			return toFloat(leftVal) > toFloat(rightVal), nil
		case "<":
			return toFloat(leftVal) < toFloat(rightVal), nil
		case "=":
			return leftVal == rightVal, nil

		// Arithmetic operators
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

// findColumnIndex finds the index of a column by name in the record schema.
func findColumnIndex(table array.Record, name string) int {
	schema := table.Schema()
	for i, field := range schema.Fields() {
		if field.Name == name {
			return i
		}
	}
	return -1
}

// toFloat helper tries to convert an interface{} to float64
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
