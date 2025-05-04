package queryparser

import (
	"fmt"
	"testing"
)

func TestParseSimpleSelect(t *testing.T) {
	queryStr := "SELECT Date, Close FROM prices WHERE Close > 1000"
	parser := NewParser(queryStr)
	query := parser.Parse()

	if len(query.Projections) != 2 {
		t.Errorf("expected 2 projections, got %d", len(query.Projections))
	}

	if col, ok := query.Projections[0].(*ColumnRef); !ok || col.Name != "Date" {
		t.Errorf("expected first projection to be column 'Date', got %+v", query.Projections[0])
	}

	if col, ok := query.Projections[1].(*ColumnRef); !ok || col.Name != "Close" {
		t.Errorf("expected second projection to be column 'Close', got %+v", query.Projections[1])
	}

	if query.TableName != "prices" {
		t.Errorf("expected table name 'prices', got %s", query.TableName)
	}

	whereExpr, ok := query.Where.(*BinaryExpr)
	if !ok {
		t.Fatalf("expected WHERE to be a binary expression, got %+v", query.Where)
	}

	leftCol, ok := whereExpr.Left.(*ColumnRef)
	if !ok || leftCol.Name != "Close" {
		t.Errorf("expected WHERE left side to be column 'Close', got %+v", whereExpr.Left)
	}

	if whereExpr.Op != ">" {
		t.Errorf("expected WHERE operator '>', got %s", whereExpr.Op)
	}

	rightLit, ok := whereExpr.Right.(*Literal)
	if !ok || rightLit.Value != "1000" {
		t.Errorf("expected WHERE right side literal '1000', got %+v", whereExpr.Right)
	}
}

func TestParseFloatLiteral(t *testing.T) {
	queryStr := "SELECT Close FROM prices WHERE Close > 123.45"
	parser := NewParser(queryStr)
	query := parser.Parse()

	whereExpr, ok := query.Where.(*BinaryExpr)
	if !ok {
		t.Fatalf("expected binary expr in WHERE")
	}

	rightLit, ok := whereExpr.Right.(*Literal)
	if !ok || rightLit.Value != "123.45" {
		t.Errorf("expected literal '123.45', got %+v", whereExpr.Right)
	}
}

func TestParseComplexWhere(t *testing.T) {
	queryStr := "SELECT Date, Close FROM prices WHERE Close > 1000 AND Volume < 5000"
	parser := NewParser(queryStr)
	query := parser.Parse()

	fmt.Println("Parsed Query:", query.String())

	if query.TableName != "prices" {
		t.Errorf("expected table name 'prices', got %s", query.TableName)
	}

	if len(query.Projections) != 2 {
		t.Errorf("expected 2 projections, got %d", len(query.Projections))
	}

	// You can also print the WHERE clause expression tree nicely
	fmt.Println("Parsed WHERE AST:")
	printAST(query.Where, 0)
}

// Helper to print the AST tree nicely
func printAST(expr Expression, indent int) {
	prefix := ""
	for i := 0; i < indent; i++ {
		prefix += "  "
	}

	switch e := expr.(type) {
	case *ColumnRef:
		fmt.Println(prefix+"Column:", e.Name)
	case *Literal:
		fmt.Println(prefix+"Literal:", e.Value)
	case *BinaryExpr:
		fmt.Println(prefix+"BinaryExpr:", e.Op)
		printAST(e.Left, indent+1)
		printAST(e.Right, indent+1)
	default:
		fmt.Println(prefix + "Unknown expression type")
	}
}

func TestParseFuncCall(t *testing.T) {
	queryStr := "SELECT SUM(Volume), COUNT(*) FROM prices"
	parser := NewParser(queryStr)
	query := parser.Parse()

	if len(query.Projections) != 2 {
		t.Errorf("expected 2 projections, got %d", len(query.Projections))
	}

	if fc, ok := query.Projections[0].(*FuncCall); !ok || fc.Name != "SUM" {
		t.Errorf("expected SUM function call, got %+v", query.Projections[0])
	}
	if fc, ok := query.Projections[1].(*FuncCall); !ok || fc.Name != "COUNT" {
		t.Errorf("expected COUNT function call, got %+v", query.Projections[1])
	}
}

func TestParseGroupBy(t *testing.T) {
	query := NewParser("SELECT Region, COUNT(*) FROM prices GROUP BY Region").Parse()

	if len(query.GroupBy) != 1 {
		t.Errorf("expected 1 GROUP BY expression, got %d", len(query.GroupBy))
	}

	if col, ok := query.GroupBy[0].(*ColumnRef); !ok || col.Name != "Region" {
		t.Errorf("expected GROUP BY Region, got %+v", query.GroupBy[0])
	}
}
