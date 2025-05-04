package queryparser

import (
	"fmt"
	"strings"
	"unicode"
)

type Query struct {
	Projections []Expression // list of projections (columns or simple expressions)
	TableName   string       // FROM table
	Where       Expression   // filter expression (WHERE condition), can be nil
}

// Expression represents a parsed expression
type Expression interface{}

type ColumnRef struct {
	Name string
}

type Literal struct {
	Value string
}

type BinaryExpr struct {
	Left  Expression
	Op    string
	Right Expression
}

type TokenType int

const (
	TOKEN_EOF TokenType = iota
	TOKEN_SELECT
	TOKEN_FROM
	TOKEN_WHERE
	TOKEN_IDENTIFIER
	TOKEN_OPERATOR
	TOKEN_LITERAL
	TOKEN_COMMA
	TOKEN_AND
	TOKEN_OR
	TOKEN_NOT
	TOKEN_PLUS
	TOKEN_MINUS
	TOKEN_ASTERISK
	TOKEN_SLASH
	TOKEN_LPAREN
	TOKEN_RPAREN
)

type Token struct {
	Type    TokenType
	Literal string
}

type Lexer struct {
	input []rune
	pos   int
}

// Helper functions to print tokens
func (q *Query) String() string {
	var sb strings.Builder
	sb.WriteString("SELECT ")

	for i, expr := range q.Projections {
		sb.WriteString(formatExpr(expr))
		if i != len(q.Projections)-1 {
			sb.WriteString(", ")
		}
	}

	sb.WriteString(fmt.Sprintf(" FROM %s", q.TableName))

	if q.Where != nil {
		sb.WriteString(" WHERE ")
		sb.WriteString(formatExpr(q.Where))
	}

	return sb.String()
}

func formatExpr(expr Expression) string {
	switch e := expr.(type) {
	case *ColumnRef:
		return e.Name
	case *Literal:
		return fmt.Sprintf("%v", e.Value)
	case *BinaryExpr:
		return fmt.Sprintf("(%s %s %s)", formatExpr(e.Left), e.Op, formatExpr(e.Right))
	default:
		return "UNKNOWN_EXPR"
	}
}

func NewLexer(input string) *Lexer {
	return &Lexer{input: []rune(input)}
}

func (l *Lexer) NextToken() Token {
	l.skipWhitespace()

	if l.pos >= len(l.input) {
		return Token{Type: TOKEN_EOF}
	}

	ch := l.input[l.pos]

	// Identify keywords or identifiers
	if isLetter(ch) {
		start := l.pos
		for l.pos < len(l.input) && (isLetter(l.input[l.pos]) || isDigit(l.input[l.pos])) {
			l.pos++
		}
		word := string(l.input[start:l.pos])
		switch strings.ToUpper(word) {
		case "SELECT":
			return Token{Type: TOKEN_SELECT, Literal: word}
		case "FROM":
			return Token{Type: TOKEN_FROM, Literal: word}
		case "WHERE":
			return Token{Type: TOKEN_WHERE, Literal: word}
		case "AND":
			return Token{Type: TOKEN_AND, Literal: word}
		case "OR":
			return Token{Type: TOKEN_OR, Literal: word}
		case "NOT":
			return Token{Type: TOKEN_NOT, Literal: word}
		}
		return Token{Type: TOKEN_IDENTIFIER, Literal: word}

	}

	if isDigit(ch) || ch == '.' {
		start := l.pos
		hasDot := false

		if ch == '.' {
			hasDot = true
			l.pos++
		}

		for l.pos < len(l.input) {
			c := l.input[l.pos]
			if c == '.' {
				if hasDot {
					break // second dot = invalid
				}
				hasDot = true
				l.pos++
			} else if isDigit(c) {
				l.pos++
			} else {
				break
			}
		}

		return Token{Type: TOKEN_LITERAL, Literal: string(l.input[start:l.pos])}
	}

	// Operators
	// Single-char operators
	switch ch {
	case '>':
		l.pos++
		return Token{Type: TOKEN_OPERATOR, Literal: ">"}
	case '<':
		l.pos++
		return Token{Type: TOKEN_OPERATOR, Literal: "<"}
	case '=':
		l.pos++
		return Token{Type: TOKEN_OPERATOR, Literal: "="}
	case '+':
		l.pos++
		return Token{Type: TOKEN_PLUS, Literal: "+"}
	case '-':
		l.pos++
		return Token{Type: TOKEN_MINUS, Literal: "-"}
	case '*':
		l.pos++
		return Token{Type: TOKEN_ASTERISK, Literal: "*"}
	case '/':
		l.pos++
		return Token{Type: TOKEN_SLASH, Literal: "/"}
	case '(':
		l.pos++
		return Token{Type: TOKEN_LPAREN, Literal: "("}
	case ')':
		l.pos++
		return Token{Type: TOKEN_RPAREN, Literal: ")"}
	case ',':
		l.pos++
		return Token{Type: TOKEN_COMMA, Literal: ","}
	}

	// Comma
	if ch == ',' {
		l.pos++
		return Token{Type: TOKEN_COMMA, Literal: ","}
	}

	panic("unexpected character: " + string(ch))
}

func (l *Lexer) skipWhitespace() {
	for l.pos < len(l.input) && unicode.IsSpace(l.input[l.pos]) {
		l.pos++
	}
}

func isLetter(ch rune) bool {
	return unicode.IsLetter(ch) || ch == '_'
}

func isDigit(ch rune) bool {
	return unicode.IsDigit(ch)
}

type Parser struct {
	lexer *Lexer
	curr  Token
}

func NewParser(input string) *Parser {
	lexer := NewLexer(input)
	return &Parser{
		lexer: lexer,
		curr:  lexer.NextToken(),
	}
}

func (p *Parser) eat(t TokenType) {
	if p.curr.Type != t {
		panic("unexpected token: " + p.curr.Literal)
	}
	p.curr = p.lexer.NextToken()
}

func (p *Parser) Parse() *Query {
	p.eat(TOKEN_SELECT)

	projections := []Expression{}
	projections = append(projections, p.parseExpression(0))

	for p.curr.Type == TOKEN_COMMA {
		p.eat(TOKEN_COMMA)
		projections = append(projections, p.parseExpression(0))
	}

	p.eat(TOKEN_FROM)

	if p.curr.Type != TOKEN_IDENTIFIER {
		panic("expected table name")
	}
	tableName := p.curr.Literal
	p.eat(TOKEN_IDENTIFIER)

	var where Expression = nil
	if p.curr.Type == TOKEN_WHERE {
		p.eat(TOKEN_WHERE)
		where = p.parseExpression(0)
	}

	return &Query{
		Projections: projections,
		TableName:   tableName,
		Where:       where,
	}
}

func (p *Parser) parseExpression(precedence int) Expression {
	left := p.parsePrimary()

	for precedence < p.currentPrecedence() {
		token := p.curr
		p.eat(token.Type)

		right := p.parseExpression(p.tokenPrecedence(token))
		left = &BinaryExpr{
			Left:  left,
			Op:    token.Literal,
			Right: right,
		}
	}

	return left
}

func (p *Parser) parsePrimary() Expression {
	switch p.curr.Type {
	case TOKEN_IDENTIFIER:
		ident := p.curr.Literal
		p.eat(TOKEN_IDENTIFIER)
		return &ColumnRef{Name: ident}
	case TOKEN_LITERAL:
		val := p.curr.Literal
		p.eat(TOKEN_LITERAL)
		return &Literal{Value: val}
	case TOKEN_LPAREN:
		p.eat(TOKEN_LPAREN)
		expr := p.parseExpression(0) // parse inner expression
		p.eat(TOKEN_RPAREN)
		return expr
	default:
		panic("unexpected token in primary: " + p.curr.Literal)
	}
}

func (p *Parser) currentPrecedence() int {
	return p.tokenPrecedence(p.curr)
}

func (p *Parser) tokenPrecedence(tok Token) int {
	switch tok.Type {
	case TOKEN_ASTERISK, TOKEN_SLASH:
		return 3
	case TOKEN_PLUS, TOKEN_MINUS:
		return 2
	case TOKEN_OPERATOR:
		return 2 // same precedence as + and -
	case TOKEN_AND:
		return 1
	case TOKEN_OR:
		return 0
	default:
		return -1
	}
}
