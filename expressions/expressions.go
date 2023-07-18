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
	"fmt"

	"github.com/zeroshade/icegopher"
)

type BoundReference struct {
	field    icegopher.NestedField
	accessor *icegopher.Accessor
}

func (b BoundReference) Ref() BoundReference { return b }

func (b BoundReference) Eval(row icegopher.Row) (any, error) {
	return b.accessor.Get(row)
}

type Reference string

func (r Reference) String() string { return string(r) }

func (r Reference) Bind(s *icegopher.Schema, caseSensitive bool) (BoundTerm, error) {
	var (
		field icegopher.NestedField
		ok    bool
	)
	if caseSensitive {
		if field, ok = s.FindFieldByName(string(r)); !ok {
			return nil, fmt.Errorf("%w: field not found to bind", icegopher.ErrInvalidSchema)
		}
	} else {
		if field, ok = s.FindFieldByNameCaseInsensitive(string(r)); !ok {
			return nil, fmt.Errorf("%w: field not found to bind", icegopher.ErrInvalidSchema)
		}
	}

	acc, err := s.AccessorForField(field.ID)
	if err != nil {
		return nil, err
	}

	return BoundReference{field: field, accessor: acc}, nil
}

type BooleanExpression interface {
	Invert() BooleanExpression
}

type LiteralTrue struct{}

func (LiteralTrue) Invert() BooleanExpression { return LiteralFalse{} }

type LiteralFalse struct{}

func (LiteralFalse) Invert() BooleanExpression { return LiteralTrue{} }

type And struct {
	Left, Right BooleanExpression
}

func (a And) Invert() BooleanExpression {
	return Or{a.Left.Invert(), a.Right.Invert()}
}

type Or struct {
	Left, Right BooleanExpression
}

func (o Or) Invert() BooleanExpression {
	return And{o.Left.Invert(), o.Right.Invert()}
}

type Not struct {
	Child BooleanExpression
}

func (n Not) Invert() BooleanExpression {
	return n.Child
}

type UnboundTerm interface {
	fmt.Stringer
	Bind(s *icegopher.Schema, caseInsensitive bool) (BoundTerm, error)
}

type UnboundPredicate interface {
	BooleanExpression
	fmt.Stringer
	Bind(s *icegopher.Schema, caseInsensitive bool) (BoundPredicate, error)
}

type UnboundLiteralPredicate interface {
	UnboundPredicate
	Literal() Literal
}

type BoundTerm interface {
	Ref() BoundReference
	Eval(row icegopher.Row) (any, error)
}

type BoundPredicate interface {
	BooleanExpression
	fmt.Stringer
	Term() BoundTerm
}

type BoundLiteralPredicate interface {
	BoundPredicate
	Literal() Literal
}

type unboundUnaryPredicate struct {
	term UnboundTerm
}

func (u *unboundUnaryPredicate) bind(s *icegopher.Schema, caseInsensitive bool) (ret boundUnaryPredicate, err error) {
	ret.term, err = u.term.Bind(s, caseInsensitive)
	return
}

type unboundLiteralPredicate struct {
	term    UnboundTerm
	literal Literal
}

func (u *unboundLiteralPredicate) bind(s *icegopher.Schema, caseInsensitive bool) (ret boundLiteralPredicate, err error) {
	ret.term, err = u.term.Bind(s, caseInsensitive)
	if err != nil {
		return
	}

	ret.literal, err = u.literal.To(ret.term.Ref().field.Type)
	return
}

type boundUnaryPredicate struct {
	term BoundTerm
}

func (b boundUnaryPredicate) Term() BoundTerm { return b.term }

type boundLiteralPredicate struct {
	term    BoundTerm
	literal Literal
}

func (b boundLiteralPredicate) Term() BoundTerm  { return b.term }
func (b boundLiteralPredicate) Literal() Literal { return b.literal }

func IsNull(term string) BooleanExpression {
	return isNull{unboundUnaryPredicate: unboundUnaryPredicate{Reference(term)}}
}

type isNull struct {
	unboundUnaryPredicate
}

func (n isNull) Bind(s *icegopher.Schema, caseInsensitive bool) (BoundPredicate, error) {
	b, err := n.unboundUnaryPredicate.bind(s, caseInsensitive)
	if err != nil {
		return nil, err
	}
	return boundIsNull{b}, nil
}

func (n isNull) Invert() BooleanExpression { return notNull(n) }

type boundIsNull struct {
	boundUnaryPredicate
}

func (n boundIsNull) String() string { return fmt.Sprintf("IsNull(%s)", n.term.Ref().field) }

func (n boundIsNull) Invert() BooleanExpression { return boundNotNull(n) }

type notNull struct {
	unboundUnaryPredicate
}

func (n notNull) Invert() BooleanExpression { return isNull(n) }

type boundNotNull struct {
	boundUnaryPredicate
}

func (n boundNotNull) String() string { return fmt.Sprintf("NotNull(%s)", n.term.Ref().field) }

func (n boundNotNull) Invert() BooleanExpression { return boundIsNull(n) }

func EqualTo[L LiteralType](term string, literal L) BooleanExpression {
	return equalTo{
		unboundLiteralPredicate: unboundLiteralPredicate{
			term:    Reference(term),
			literal: NewLiteral(literal),
		},
	}
}

type equalTo struct {
	unboundLiteralPredicate
}

func (e equalTo) String() string {
	return fmt.Sprintf("%s == %v", e.term, e.literal)
}

func (e equalTo) Invert() BooleanExpression {
	return notEqualTo(e)
}

func (e equalTo) Bind(s *icegopher.Schema, caseInsensitive bool) (BoundPredicate, error) {
	bound, err := e.unboundLiteralPredicate.bind(s, caseInsensitive)
	if err != nil {
		return nil, err
	}

	return boundEqualTo{bound}, nil
}

func NotEqualTo[L LiteralType](term string, literal L) BooleanExpression {
	return notEqualTo{
		unboundLiteralPredicate: unboundLiteralPredicate{
			term:    Reference(term),
			literal: NewLiteral(literal),
		},
	}
}

type notEqualTo struct {
	unboundLiteralPredicate
}

func (n notEqualTo) String() string {
	return fmt.Sprintf("%s != %v", n.term, n.literal)
}

func (n notEqualTo) Invert() BooleanExpression {
	return equalTo(n)
}

func (n notEqualTo) Bind(s *icegopher.Schema, caseInsensitive bool) (BoundPredicate, error) {
	bound, err := n.unboundLiteralPredicate.bind(s, caseInsensitive)
	if err != nil {
		return nil, err
	}

	return boundNotEqualTo{bound}, nil
}

type boundEqualTo struct {
	boundLiteralPredicate
}

func (b boundEqualTo) Invert() BooleanExpression { return boundNotEqualTo(b) }

func (b boundEqualTo) String() string {
	return fmt.Sprintf("%s == %v", b.term.Ref().field, b.literal)
}

type boundNotEqualTo struct {
	boundLiteralPredicate
}

func (b boundNotEqualTo) Invert() BooleanExpression { return boundEqualTo(b) }

func (b boundNotEqualTo) String() string {
	return fmt.Sprintf("%s != %v", b.term.Ref().field, b.literal)
}
