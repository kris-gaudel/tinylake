package engine

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"

	"github.com/kris-gaudel/tinylake/internal/queryparser"
)

func ExecuteQuery(q *queryparser.Query, table array.Record) (array.Record, error) {
	pool := memory.NewGoAllocator()
	totalRows := int(table.NumRows())

	// Step 1: Filter rows based on WHERE
	passIndices := make([]int, 0, totalRows)
	for row := 0; row < totalRows; row++ {
		pass := true
		if q.Where != nil {
			result, err := evaluateExpression(q.Where, table, row)
			if err != nil {
				return nil, err
			}
			if boolResult, ok := result.(bool); ok {
				pass = boolResult
			} else {
				return nil, fmt.Errorf("WHERE clause must evaluate to boolean")
			}
		}
		if pass {
			passIndices = append(passIndices, row)
		}
	}

	// Step 2: Determine if it's an aggregate query
	allAgg := true
	for _, expr := range q.Projections {
		if _, ok := expr.(*queryparser.FuncCall); !ok {
			allAgg = false
			break
		}
	}

	if allAgg {
		return executeAggregates(q.Projections, table, passIndices, pool)
	}

	// Step 3: Regular projection
	projectedArrays := []array.Interface{}
	projectedFields := []arrow.Field{}

	for i, expr := range q.Projections {
		switch e := expr.(type) {
		case *queryparser.ColumnRef:
			colIdx := findColumnIndex(table, e.Name)
			if colIdx == -1 {
				return nil, fmt.Errorf("column %s not found", e.Name)
			}
			projectedArrays = append(projectedArrays, table.Column(colIdx))
			projectedFields = append(projectedFields, table.Schema().Field(colIdx))
		default:
			builder := array.NewFloat64Builder(pool)
			defer builder.Release()
			for _, row := range passIndices {
				val, err := evaluateExpression(expr, table, row)
				if err != nil {
					return nil, err
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
				Name:     fmt.Sprintf("expr_%d", i),
				Type:     arrow.PrimitiveTypes.Float64,
				Nullable: true,
			})
		}
	}

	schema := arrow.NewSchema(projectedFields, nil)
	return array.NewRecord(schema, projectedArrays, int64(len(passIndices))), nil
}

func executeAggregates(exprs []queryparser.Expression, table array.Record, indices []int, pool memory.Allocator) (array.Record, error) {
	fields := []arrow.Field{}
	values := []float64{}

	for i, e := range exprs {
		fc, ok := e.(*queryparser.FuncCall)
		if !ok {
			return nil, fmt.Errorf("non-aggregate in aggregate-only projection")
		}
		val, err := evalAggregateFunction(fc, table, indices)
		if err != nil {
			return nil, err
		}
		values = append(values, val)
		fields = append(fields, arrow.Field{
			Name:     fmt.Sprintf("expr_%d", i),
			Type:     arrow.PrimitiveTypes.Float64,
			Nullable: false,
		})
	}

	arrays := []array.Interface{}
	for _, val := range values {
		b := array.NewFloat64Builder(pool)
		b.Append(val)
		arr := b.NewArray()
		arrays = append(arrays, arr)
	}

	schema := arrow.NewSchema(fields, nil)
	return array.NewRecord(schema, arrays, 1), nil
}

func evalAggregateFunction(f *queryparser.FuncCall, table array.Record, indices []int) (float64, error) {
	name := strings.ToUpper(f.Name)
	switch name {
	case "COUNT":
		if len(f.Args) == 1 {
			if _, ok := f.Args[0].(*queryparser.StarExpr); ok {
				return float64(len(indices)), nil
			}
			count := 0
			for _, row := range indices {
				val, err := evaluateExpression(f.Args[0], table, row)
				if err != nil {
					return 0, err
				}
				if val != nil {
					count++
				}
			}
			return float64(count), nil
		}
	case "SUM", "AVG", "MAX", "MIN":
		if len(f.Args) != 1 {
			return 0, fmt.Errorf("%s expects one argument", name)
		}
		nums := []float64{}
		for _, row := range indices {
			val, err := evaluateExpression(f.Args[0], table, row)
			if err != nil {
				return 0, err
			}
			if val != nil {
				nums = append(nums, toFloat(val))
			}
		}
		switch name {
		case "SUM":
			sum := 0.0
			for _, v := range nums {
				sum += v
			}
			return sum, nil
		case "AVG":
			if len(nums) == 0 {
				return 0, nil
			}
			sum := 0.0
			for _, v := range nums {
				sum += v
			}
			return sum / float64(len(nums)), nil
		case "MAX":
			if len(nums) == 0 {
				return 0, nil
			}
			max := nums[0]
			for _, v := range nums[1:] {
				if v > max {
					max = v
				}
			}
			return max, nil
		case "MIN":
			if len(nums) == 0 {
				return 0, nil
			}
			min := nums[0]
			for _, v := range nums[1:] {
				if v < min {
					min = v
				}
			}
			return min, nil
		}
	}
	return 0, fmt.Errorf("unsupported aggregate function: %s", f.Name)
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
			if arr.IsValid(row) {
				return arr.Value(row), nil
			}
			return nil, nil
		case *array.String:
			if arr.IsValid(row) {
				return arr.Value(row), nil
			}
			return nil, nil
		default:
			return nil, fmt.Errorf("unsupported column type: %T", arr)
		}
	case *queryparser.Literal:
		if f, err := strconv.ParseFloat(e.Value, 64); err == nil {
			return f, nil
		}
		return e.Value, nil
	case *queryparser.BinaryExpr:
		left, _ := evaluateExpression(e.Left, table, row)
		right, _ := evaluateExpression(e.Right, table, row)
		switch e.Op {
		case "+":
			return toFloat(left) + toFloat(right), nil
		case "-":
			return toFloat(left) - toFloat(right), nil
		case "*":
			return toFloat(left) * toFloat(right), nil
		case "/":
			return toFloat(left) / toFloat(right), nil
		case ">":
			return toFloat(left) > toFloat(right), nil
		case "<":
			return toFloat(left) < toFloat(right), nil
		case "=":
			return left == right, nil
		case "AND":
			return toBool(left) && toBool(right), nil
		case "OR":
			return toBool(left) || toBool(right), nil
		default:
			return nil, fmt.Errorf("unsupported operator: %s", e.Op)
		}
	case *queryparser.StarExpr:
		return "*", nil
	case *queryparser.FuncCall:
		return nil, fmt.Errorf("nested function calls not supported in row-wise projection")
	default:
		return nil, fmt.Errorf("unsupported expression: %T", expr)
	}
}

func findColumnIndex(table array.Record, name string) int {
	for i, f := range table.Schema().Fields() {
		if f.Name == name {
			return i
		}
	}
	return -1
}

func toFloat(v interface{}) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case string:
		f, _ := strconv.ParseFloat(x, 64)
		return f
	default:
		return 0
	}
}

func toBool(v interface{}) bool {
	switch x := v.(type) {
	case bool:
		return x
	case float64:
		return x != 0
	case string:
		return x != ""
	default:
		return false
	}
}
