package queryparser

import (
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
