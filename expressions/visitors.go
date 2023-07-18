// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package expressions

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/zeroshade/icegopher"

	"github.com/google/uuid"
)

type BooleanExpressionVisitor[T any] interface {
	VisitTrue() T
	VisitFalse() T
	VisitNot(child T) T
	VisitAnd(left, right T) T
	VisitOr(left, right T) T
	VisitUnboundPredicate(pred UnboundPredicate) T
	VisitBoundPredicate(pred BoundPredicate) T
}

type BoundBooleanExpressionVisitor[T any] interface {
	BooleanExpressionVisitor[T]

	// VisitIn(BoundTerm, []Literal) T
	// VisitNotIn(BoundTerm, []Literal) T
	// VisitIsNaN(BoundTerm) T
	// VisitNotNaN(BoundTerm) T
	VisitIsNull(BoundTerm) T
	VisitNotNull(BoundTerm) T
	VisitEqual(BoundTerm, Literal) T
	VisitNotEqual(BoundTerm, Literal) T
}

func VisitBooleanExpr[T any](e BooleanExpression, visitor BooleanExpressionVisitor[T]) (result T, err error) {
	defer func() {
		if r := recover(); r != nil {
			switch e := r.(type) {
			case string:
				err = errors.New("error encountered during expr visitor: " + e)
			case error:
				err = fmt.Errorf("error encountered during expr visitor: %w", err)
			}
		}
	}()

	result = visitBooleanExpr[T](e, visitor)
	return
}

func visitBooleanExpr[T any](e BooleanExpression, visitor BooleanExpressionVisitor[T]) T {
	switch e := e.(type) {
	case LiteralFalse:
		return visitor.VisitFalse()
	case LiteralTrue:
		return visitor.VisitTrue()
	case UnboundPredicate:
		return visitor.VisitUnboundPredicate(e)
	case BoundPredicate:
		if boundVisitor, ok := visitor.(BoundBooleanExpressionVisitor[T]); ok {
			return visitBoundBooleanExpr(e, boundVisitor)
		}

		return visitor.VisitBoundPredicate(e)
	case And:
		left, right := visitBooleanExpr(e.Left, visitor), visitBooleanExpr(e.Right, visitor)
		return visitor.VisitAnd(left, right)
	case Not:
		child := visitBooleanExpr(e.Child, visitor)
		return visitor.VisitNot(child)
	case Or:
		left, right := visitBooleanExpr(e.Left, visitor), visitBooleanExpr(e.Right, visitor)
		return visitor.VisitOr(left, right)
	}
	panic(icegopher.ErrNotImplemented)
}

func visitBoundBooleanExpr[T any](e BooleanExpression, visitor BoundBooleanExpressionVisitor[T]) T {
	switch e := e.(type) {
	case boundEqualTo:
		visitor.VisitEqual(e.term, e.literal)
	case boundNotEqualTo:
		visitor.VisitNotEqual(e.term, e.literal)
	case boundIsNull:
		visitor.VisitIsNull(e.term)
	case boundNotNull:
		visitor.VisitNotNull(e.term)
	}
	panic(icegopher.ErrNotImplemented)
}

func Bind(s *icegopher.Schema, expr BooleanExpression, caseInsensitive bool) (BooleanExpression, error) {
	return VisitBooleanExpr[BooleanExpression](expr, BindVisitor{Schema: s, CaseInsensitive: caseInsensitive})
}

type BindVisitor struct {
	Schema          *icegopher.Schema
	CaseInsensitive bool
}

func (BindVisitor) VisitTrue() BooleanExpression  { return LiteralTrue{} }
func (BindVisitor) VisitFalse() BooleanExpression { return LiteralFalse{} }
func (BindVisitor) VisitNot(child BooleanExpression) BooleanExpression {
	return Not{Child: child}
}

func (BindVisitor) VisitAnd(left, right BooleanExpression) BooleanExpression {
	return And{Left: left, Right: right}
}

func (BindVisitor) VisitOr(left, right BooleanExpression) BooleanExpression {
	return Or{Left: left, Right: right}
}

func (BindVisitor) VisitBoundPredicate(pred BoundPredicate) BooleanExpression {
	panic("found already bound predicate " + pred.String())
}

func (b BindVisitor) VisitUnboundPredicate(pred UnboundPredicate) BooleanExpression {
	ex, err := pred.Bind(b.Schema, b.CaseInsensitive)
	if err != nil {
		panic(err)
	}

	return ex
}

func RewriteNot(expr BooleanExpression) (BooleanExpression, error) {
	return VisitBooleanExpr[BooleanExpression](expr, rewriteNotVisitor{})
}

type rewriteNotVisitor struct{}

func (rewriteNotVisitor) VisitTrue() BooleanExpression  { return LiteralTrue{} }
func (rewriteNotVisitor) VisitFalse() BooleanExpression { return LiteralFalse{} }
func (rewriteNotVisitor) VisitNot(child BooleanExpression) BooleanExpression {
	return child.Invert()
}

func (rewriteNotVisitor) VisitAnd(left, right BooleanExpression) BooleanExpression {
	return And{Left: left, Right: right}
}

func (rewriteNotVisitor) VisitOr(left, right BooleanExpression) BooleanExpression {
	return Or{Left: left, Right: right}
}

func (rewriteNotVisitor) VisitUnboundPredicate(pred UnboundPredicate) BooleanExpression {
	return pred
}

func (rewriteNotVisitor) VisitBoundPredicate(pred BoundPredicate) BooleanExpression {
	return pred
}

const (
	RowsCannotMatch = false
	RowsMightMatch  = true
)

type InclusiveMetricsEvaluator interface {
	Eval(icegopher.DataFile) (bool, error)
}

func NewInclusiveMetricsEvaluator(s *icegopher.Schema, expr BooleanExpression, caseInsensitive bool) (InclusiveMetricsEvaluator, error) {
	e, err := RewriteNot(expr)
	if err != nil {
		return nil, err
	}

	if e, err = Bind(s, e, caseInsensitive); err != nil {
		return nil, err
	}

	return &metricsEvaluator{
		visitor: inclusiveMetricsEvaluatorVisitor{
			st:   s.AsStruct(),
			expr: e,
		},
	}, nil
}

type metricsEvaluator struct {
	visitor inclusiveMetricsEvaluatorVisitor
}

func (m *metricsEvaluator) Eval(file icegopher.DataFile) (bool, error) {
	if file.Count() == 0 {
		return RowsCannotMatch, nil
	}

	if file.Count() < 0 {
		// older versions don't correctly implement record count from
		// avro file and set record counts to -1 when importing avro
		// tables to iceberg tables.
		return RowsMightMatch, nil
	}

	m.visitor.valCounts = file.ValueCounts()
	m.visitor.nullCounts = file.NullValueCounts()
	m.visitor.nanCounts = file.NaNValueCounts()
	m.visitor.lowerBounds = file.LowerBoundValues()
	m.visitor.upperBounds = file.UpperBoundValues()

	return VisitBooleanExpr[bool](m.visitor.expr, &m.visitor)
}

type inclusiveMetricsEvaluatorVisitor struct {
	st   icegopher.StructType
	expr BooleanExpression

	valCounts   map[int]int64
	nullCounts  map[int]int64
	nanCounts   map[int]int64
	lowerBounds map[int][]byte
	upperBounds map[int][]byte
}

func (m *inclusiveMetricsEvaluatorVisitor) containsOnlyNulls(fieldID int) bool {
	valCount, nullCount := m.valCounts[fieldID], m.nullCounts[fieldID]
	if valCount != 0 && nullCount != 0 {
		return valCount == nullCount
	}
	return false
}

func (m *inclusiveMetricsEvaluatorVisitor) containsOnlyNaN(fieldID int) bool {
	valCount, nanCount := m.valCounts[fieldID], m.nanCounts[fieldID]
	if valCount != 0 && nanCount != 0 {
		return valCount == nanCount
	}
	return false
}

func (m *inclusiveMetricsEvaluatorVisitor) VisitTrue() bool {
	return RowsMightMatch
}

func (m *inclusiveMetricsEvaluatorVisitor) VisitFalse() bool {
	return RowsCannotMatch
}

func (m *inclusiveMetricsEvaluatorVisitor) VisitNot(child bool) bool {
	panic("NOT should be rewritten: " + strconv.FormatBool(child))
}

func (m *inclusiveMetricsEvaluatorVisitor) VisitAnd(left, right bool) bool {
	return left && right
}

func (m *inclusiveMetricsEvaluatorVisitor) VisitOr(left, right bool) bool {
	return left || right
}

func (m *inclusiveMetricsEvaluatorVisitor) VisitUnboundPredicate(UnboundPredicate) bool {
	panic("found unbound predicate")
}

func (m *inclusiveMetricsEvaluatorVisitor) VisitBoundPredicate(BoundPredicate) bool {
	panic("should implement BoundBooleanExpressionVisitor")
}

func (m *inclusiveMetricsEvaluatorVisitor) VisitIsNull(term BoundTerm) bool {
	fieldID := term.Ref().field.ID
	nc, ok := m.nullCounts[fieldID]
	if !ok {
		panic("no null count stats for field id: " + strconv.Itoa(fieldID))
	}

	if nc == 0 {
		return RowsCannotMatch
	}
	return RowsMightMatch
}

func (m *inclusiveMetricsEvaluatorVisitor) VisitNotNull(term BoundTerm) bool {
	// no need to check whether the field is required because binding
	// evaluates that case. if the column has no non-null values, the
	// the expression cannot match.
	fieldID := term.Ref().field.ID
	if m.containsOnlyNulls(fieldID) {
		return RowsCannotMatch
	}
	return RowsMightMatch
}

func withinBoundsString(literal, lowerBounds, upperBounds Literal) bool {
	value := literal.(StringLiteral).Value()

	if lowerBounds != nil {
		lb := lowerBounds.(StringLiteral).Value()
		if lb > value {
			return RowsCannotMatch
		}
	}

	if upperBounds != nil {
		ub := upperBounds.(StringLiteral).Value()
		if ub < value {
			return RowsCannotMatch
		}
	}

	return RowsMightMatch
}

func withinBoundsBinary(literal, lowerBounds, upperBounds []byte) bool {
	if lowerBounds != nil {
		if bytes.Compare(literal, lowerBounds) == -1 {
			return RowsCannotMatch
		}
	}

	if upperBounds != nil {
		if bytes.Compare(literal, upperBounds) == 1 {
			return RowsCannotMatch
		}
	}

	return RowsMightMatch
}

func withinBoundsImpl[T OrderedNumericLiteralType](literal, lowerBounds, upperBounds Literal) bool {
	value := literal.(typedLiteral[T]).Value()

	if lowerBounds != nil {
		lb := lowerBounds.(typedLiteral[T]).Value()
		if math.IsNaN(float64(lb)) {
			return RowsMightMatch
		}
		if lb > value {
			return RowsCannotMatch
		}
	}

	if upperBounds != nil {
		ub := upperBounds.(typedLiteral[T]).Value()
		if math.IsNaN(float64(ub)) {
			return RowsMightMatch
		}
		if ub < value {
			return RowsCannotMatch
		}
	}

	return RowsMightMatch
}

type typedLiteral[L LiteralType] interface {
	Value() L
}

func withinBounds(typ icegopher.Type, literal Literal, lowerBounds, upperBounds []byte) bool {
	lowerBoundLiteral, err := LiteralFromBytes(typ, lowerBounds)
	if err != nil {
		panic(err)
	}
	upperBoundLiteral, err := LiteralFromBytes(typ, upperBounds)
	if err != nil {
		panic(err)
	}

	switch typ.(type) {
	case icegopher.BooleanType:
		val := literal.(BoolLiteral).Value()
		if lowerBoundLiteral != nil {
			lb := lowerBoundLiteral.(BoolLiteral).Value()
			if lb && !val {
				return RowsCannotMatch
			}
		}

		if upperBoundLiteral != nil {
			ub := upperBoundLiteral.(BoolLiteral).Value()
			if !ub && val {
				return RowsCannotMatch
			}
		}

		return RowsMightMatch
	case icegopher.Int32Type:
		return withinBoundsImpl[int32](literal, lowerBoundLiteral, upperBoundLiteral)
	case icegopher.Int64Type:
		return withinBoundsImpl[int64](literal, lowerBoundLiteral, upperBoundLiteral)
	case icegopher.Float32Type:
		return withinBoundsImpl[float32](literal, lowerBoundLiteral, upperBoundLiteral)
	case icegopher.Float64Type:
		return withinBoundsImpl[float64](literal, lowerBoundLiteral, upperBoundLiteral)
	case icegopher.DecimalType:
		val := literal.(DecimalLiteral).Value()
		if lowerBoundLiteral != nil {
			lb := lowerBoundLiteral.(DecimalLiteral).Value()
			if lb.Greater(val) {
				return RowsCannotMatch
			}
		}

		if upperBoundLiteral != nil {
			ub := upperBoundLiteral.(DecimalLiteral).Value()
			if ub.Less(val) {
				return RowsCannotMatch
			}
		}

		return RowsMightMatch
	case icegopher.BinaryType, icegopher.FixedType:
		value := literal.(typedLiteral[[]byte]).Value()
		var lb, ub []byte
		if lowerBoundLiteral != nil {
			lb = lowerBoundLiteral.(typedLiteral[[]byte]).Value()
		}
		if upperBoundLiteral != nil {
			ub = upperBoundLiteral.(typedLiteral[[]byte]).Value()
		}
		return withinBoundsBinary(value, lb, ub)
	case icegopher.StringType:
		return withinBoundsString(literal, lowerBoundLiteral, upperBoundLiteral)
	case icegopher.DateType:
		return withinBoundsImpl[icegopher.Date](literal, lowerBoundLiteral, upperBoundLiteral)
	case icegopher.TimeType:
		return withinBoundsImpl[icegopher.Time](literal, lowerBoundLiteral, upperBoundLiteral)
	case icegopher.TimestampType:
		return withinBoundsImpl[icegopher.Timestamp](literal, lowerBoundLiteral, upperBoundLiteral)
	case icegopher.TimestampTzType:
		return withinBoundsImpl[icegopher.Timestamp](literal, lowerBoundLiteral, upperBoundLiteral)
	case icegopher.UUIDType:
		value := literal.(UUIDLiteral).Value()
		var tmp uuid.UUID
		var lb, ub []byte
		if lowerBoundLiteral != nil {
			tmp = lowerBoundLiteral.(UUIDLiteral).Value()
			lb = tmp[:]
		}
		if upperBoundLiteral != nil {
			tmp = upperBoundLiteral.(UUIDLiteral).Value()
			ub = tmp[:]
		}
		return withinBoundsBinary(value[:], lb, ub)
	}
	return RowsMightMatch
}

func (m *inclusiveMetricsEvaluatorVisitor) VisitEqual(term BoundTerm, literal Literal) bool {
	field := term.Ref().field
	fieldID := field.ID

	if m.containsOnlyNulls(fieldID) || m.containsOnlyNaN(fieldID) {
		return RowsCannotMatch
	}

	if _, ok := field.Type.(icegopher.PrimitiveType); !ok {
		panic("expected primitive type: got " + field.Type.String())
	}

	return withinBounds(field.Type, literal, m.lowerBounds[fieldID], m.upperBounds[fieldID])
}

func (m *inclusiveMetricsEvaluatorVisitor) VisitNotEqual(BoundTerm, Literal) bool {
	return RowsMightMatch
}
