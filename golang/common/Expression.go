package common

import (
	"strings"
)

type Operator string

const (
	Eq                  Operator = "="
	Neq                 Operator = "!="
	And                 Operator = " and "
	Or                  Operator = " or "
	GT                  Operator = ">"
	LT                  Operator = "<"
	GTEQ                Operator = ">="
	LTEQ                Operator = "<="
	MAX_EXPRESSION_SIZE          = 999999
	PLACE_HOLDER                 = "_Place_Holder_"
)

var operatorOrder = make([]Operator, 0)

func initOperatorOrder() {
	if len(operatorOrder) == 0 {
		operatorOrder = append(operatorOrder, GTEQ)
		operatorOrder = append(operatorOrder, LTEQ)
		operatorOrder = append(operatorOrder, Neq)
		operatorOrder = append(operatorOrder, Eq)
		operatorOrder = append(operatorOrder, GT)
		operatorOrder = append(operatorOrder, LT)
	}
}

type Expression struct {
	value    string
	left     *Expression
	right    *Expression
	operator Operator
}

func (expr *Expression) Operator() Operator {
	return expr.operator
}

func (expr *Expression) Value() string {
	return expr.value
}

func (expr *Expression) Left() *Expression {
	return expr.left
}

func (expr *Expression) Right() *Expression {
	return expr.right
}

func rightPlaceHolder(ws string, bo int) *Expression {
	sws := ws[0:bo] + PLACE_HOLDER
	opr, loc := getLastOperator(sws)
	expr := &Expression{}
	expr.operator = opr
	expr.left = parseOperator(ws[0:loc])
	expr.right = parseExpression(ws[bo:])
	return expr
}

func leftPlaceHolder(ws string, bo int) *Expression {
	be := getBE(ws)
	if be == len(ws)-1 {
		return parseOperator(ws[1 : len(ws)-1])
	}
	sws := PLACE_HOLDER + ws[be+1:]
	expr := parseOperator(sws)
	left := parseOperator(ws[bo:be])
	expr.left = left
	return expr
}

func parseExpression(ws string) *Expression {
	initOperatorOrder()
	ws = strings.TrimSpace(ws)
	bo := strings.Index(ws, "(")
	if bo == -1 {
		return parseOperator(ws)
	} else if bo > 0 {
		return rightPlaceHolder(ws, bo)
	} else {
		return leftPlaceHolder(ws, bo)
	}
}

func getFirstOperator(ws string) (Operator, int) {
	loc := MAX_EXPRESSION_SIZE
	var opr Operator
	and := strings.Index(ws, string(And))
	if and != -1 {
		loc = and
		opr = And
	}
	or := strings.Index(ws, string(Or))
	if or != -1 && (or < and || and == -1) {
		loc = or
		opr = Or
	}
	if loc != MAX_EXPRESSION_SIZE {
		return opr, loc
	}
	for _, operator := range operatorOrder {
		l := strings.Index(ws, string(operator))
		if l != -1 && l < loc {
			loc = l
			opr = operator
		}
	}
	return opr, loc
}

func getLastOperator(ws string) (Operator, int) {
	loc := -1
	var opr Operator
	and := strings.LastIndex(ws, string(And))
	if and != -1 {
		loc = and
		opr = And
	}
	or := strings.LastIndex(ws, string(Or))
	if or != -1 && or > and {
		loc = or
		opr = Or
	}
	if loc != -1 {
		return opr, loc
	}
	for _, operator := range operatorOrder {
		l := strings.Index(ws, string(operator))
		if l != -1 && l > loc {
			loc = l
			opr = operator
		}
	}
	return opr, loc
}

func parseOperator(ws string) *Expression {
	opr, loc := getFirstOperator(ws)
	expr := &Expression{}
	if loc != MAX_EXPRESSION_SIZE {
		expr.left = parseExpression(ws[0:loc])
		expr.operator = opr
		expr.right = parseExpression(ws[loc+len(string(opr)):])
		return expr
	} else {
		expr.value = ws
	}
	return expr
}

func getBE(ws string) int {
	count := 1
	i := 1
	for ; i < len(ws); i++ {
		if byte(ws[i]) == byte('(') {
			count++
		} else if byte(ws[i]) == byte(')') {
			count--
		}
		if count == 0 {
			break
		}
	}
	return i
}
