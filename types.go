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

package icegopher

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"golang.org/x/exp/slices"
)

var (
	regexFromBrackets = regexp.MustCompile(`^\w+\[(\d+)\]$`)
	decimalRegex      = regexp.MustCompile(`decimal\((\d+),\s*(\d+)\)`)

	ErrInvalidTypeString = errors.New("invalid type")
	ErrNotImplemented    = errors.New("not implemented")
	ErrInvalidArgument   = errors.New("invalid argument")
)

type Properties map[string]string

type Type interface {
	fmt.Stringer
	Type() string
	Equals(Type) bool
}

type NestedType interface {
	Type
	Children() []NestedField
}

type typeIFace struct {
	Type
}

func (t *typeIFace) MarshalJSON() ([]byte, error) {
	return []byte(`"` + t.Type.Type() + `"`), nil
}

func (t *typeIFace) UnmarshalJSON(b []byte) error {
	var typename string
	err := json.Unmarshal(b, &typename)
	if err == nil {
		switch typename {
		case "boolean":
			t.Type = BooleanType{}
		case "int":
			t.Type = Int32Type{}
		case "long":
			t.Type = Int64Type{}
		case "float":
			t.Type = Float32Type{}
		case "double":
			t.Type = Float64Type{}
		case "date":
			t.Type = DateType{}
		case "time":
			t.Type = TimeType{}
		case "timestamp":
			t.Type = TimestampType{}
		case "timestamptz":
			t.Type = TimestampTzType{}
		case "string":
			t.Type = StringType{}
		case "uuid":
			t.Type = UUIDType{}
		case "binary":
			t.Type = BinaryType{}
		default:
			switch {
			case strings.HasPrefix(typename, "fixed"):
				matches := regexFromBrackets.FindStringSubmatch(typename)
				if len(matches) != 2 {
					return fmt.Errorf("%w: %s", ErrInvalidTypeString, typename)
				}

				n, _ := strconv.Atoi(matches[1])
				t.Type = FixedType{len: n}
			case strings.HasPrefix(typename, "decimal"):
				matches := decimalRegex.FindStringSubmatch(typename)
				if len(matches) != 3 {
					return fmt.Errorf("%w: %s", ErrInvalidTypeString, typename)
				}

				prec, _ := strconv.Atoi(matches[1])
				scale, _ := strconv.Atoi(matches[2])
				t.Type = DecimalType{precision: prec, scale: scale}
			default:
				return fmt.Errorf("%w: unrecognized field type", ErrInvalidSchema)
			}
		}
		return nil
	}

	aux := struct {
		TypeName string `json:"type"`
	}{}
	if err = json.Unmarshal(b, &aux); err != nil {
		return err
	}

	switch aux.TypeName {
	case "list":
		t.Type = &ListType{}
	case "map":
		t.Type = &MapType{}
	case "struct":
		t.Type = &StructType{}
	default:
		return fmt.Errorf("%w: %s", ErrInvalidTypeString, aux.TypeName)
	}

	return json.Unmarshal(b, t.Type)
}

type NestedField struct {
	Type `json:"-"`

	ID             int    `json:"id"`
	Name           string `json:"name"`
	Required       bool   `json:"required"`
	Doc            string `json:"doc,omitempty"`
	InitialDefault any    `json:"initial-default,omitempty"`
	WriteDefault   any    `json:"write-default,omitempty"`
}

func optOrReq(required bool) string {
	if required {
		return "required"
	}
	return "optional"
}

func (n NestedField) String() string {
	doc := n.Doc
	if doc != "" {
		doc = " (" + doc + ")"
	}

	return fmt.Sprintf("%d: %s: %s %s%s",
		n.ID, n.Name, optOrReq(n.Required), n.Type, doc)
}

func (n *NestedField) Equals(other NestedField) bool {
	return n.ID == other.ID &&
		n.Name == other.Name &&
		n.Required == other.Required &&
		n.Doc == other.Doc &&
		n.InitialDefault == other.InitialDefault &&
		n.WriteDefault == other.WriteDefault &&
		n.Type.Equals(other.Type)
}

func (n *NestedField) MarshalJSON() ([]byte, error) {
	type Alias NestedField
	return json.Marshal(struct {
		Type *typeIFace `json:"type"`
		*Alias
	}{Type: &typeIFace{n.Type}, Alias: (*Alias)(n)})
}

func (n *NestedField) UnmarshalJSON(b []byte) error {
	type Alias NestedField
	aux := struct {
		Type typeIFace `json:"type"`
		*Alias
	}{
		Alias: (*Alias)(n),
	}

	if err := json.Unmarshal(b, &aux); err != nil {
		return err
	}

	n.Type = aux.Type.Type

	return nil
}

type StructType struct {
	Fields []NestedField `json:"fields"`
}

func (s *StructType) Equals(other Type) bool {
	st, ok := other.(*StructType)
	if !ok {
		return false
	}

	return slices.EqualFunc(s.Fields, st.Fields, func(a, b NestedField) bool {
		return a.Equals(b)
	})
}

func (s *StructType) Children() []NestedField { return s.Fields }

func (s *StructType) MarshalJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type string `json:"type"`
		*StructType
	}{Type: s.Type(), StructType: s})
}

func (*StructType) Type() string { return "struct" }
func (s *StructType) String() string {
	var b strings.Builder
	b.WriteString("struct<")
	for i, f := range s.Fields {
		if i != 0 {
			b.WriteString(", ")
		}
		b.WriteString(f.Name)
		b.WriteString(": ")
		b.WriteString(f.Type.String())
	}
	b.WriteString(">")

	return b.String()
}

type ListType struct {
	ElementID       int  `json:"element-id"`
	Element         Type `json:"element"`
	ElementRequired bool `json:"element-required"`
}

func (l *ListType) Equals(other Type) bool {
	rhs, ok := other.(*ListType)
	if !ok {
		return false
	}

	return l.ElementID == rhs.ElementID &&
		l.Element.Equals(rhs.Element) &&
		l.ElementRequired == rhs.ElementRequired
}

func (l *ListType) Children() []NestedField {
	return []NestedField{l.ElementField()}
}

func (l *ListType) ElementField() NestedField {
	return NestedField{
		ID:       l.ElementID,
		Name:     "element",
		Type:     l.Element,
		Required: l.ElementRequired,
	}
}

func (*ListType) Type() string     { return "list" }
func (l *ListType) String() string { return fmt.Sprintf("list<%s>", l.Element) }

func (l *ListType) UnmarshalJSON(b []byte) error {
	aux := struct {
		ID   int       `json:"element-id"`
		Elem typeIFace `json:"element"`
		Req  bool      `json:"element-required"`
	}{}
	if err := json.Unmarshal(b, &aux); err != nil {
		return err
	}

	l.ElementID = aux.ID
	l.Element = aux.Elem.Type
	l.ElementRequired = aux.Req
	return nil
}

type MapType struct {
	KeyID         int  `json:"key-id"`
	KeyType       Type `json:"key"`
	ValueID       int  `json:"value-id"`
	ValueType     Type `json:"value"`
	ValueRequired bool `json:"value-required"`
}

func (m *MapType) Equals(other Type) bool {
	rhs, ok := other.(*MapType)
	if !ok {
		return false
	}

	return m.KeyID == rhs.KeyID &&
		m.KeyType.Equals(rhs.KeyType) &&
		m.ValueID == rhs.ValueID &&
		m.ValueType.Equals(rhs.ValueType) &&
		m.ValueRequired == rhs.ValueRequired
}

func (m *MapType) Children() []NestedField {
	return []NestedField{m.KeyField(), m.ValueField()}
}

func (m *MapType) KeyField() NestedField {
	return NestedField{
		Name:     "key",
		ID:       m.KeyID,
		Type:     m.KeyType,
		Required: true,
	}
}

func (m *MapType) ValueField() NestedField {
	return NestedField{
		Name:     "value",
		ID:       m.ValueID,
		Type:     m.ValueType,
		Required: m.ValueRequired,
	}
}

func (*MapType) Type() string { return "map" }
func (m *MapType) String() string {
	return fmt.Sprintf("map<%s, %s>", m.KeyType, m.ValueType)
}

func (m *MapType) UnmarshalJSON(b []byte) error {
	aux := struct {
		KeyID    int       `json:"key-id"`
		Key      typeIFace `json:"key"`
		ValueID  int       `json:"value-id"`
		Value    typeIFace `json:"value"`
		ValueReq *bool     `json:"value-required"`
	}{}
	if err := json.Unmarshal(b, &aux); err != nil {
		return err
	}

	m.KeyID, m.KeyType = aux.KeyID, aux.Key.Type
	m.ValueID, m.ValueType = aux.ValueID, aux.Value.Type
	if aux.ValueReq == nil {
		m.ValueRequired = true
	} else {
		m.ValueRequired = *aux.ValueReq
	}
	return nil
}

type FixedType struct {
	len int
}

func (f FixedType) Equals(other Type) bool {
	rhs, ok := other.(FixedType)
	if !ok {
		return false
	}

	return f.len == rhs.len
}
func (f FixedType) Len() int       { return f.len }
func (f FixedType) Type() string   { return fmt.Sprintf("fixed[%d]", f.len) }
func (f FixedType) String() string { return fmt.Sprintf("fixed[%d]", f.len) }

type DecimalType struct {
	precision, scale int
}

func (d DecimalType) Equals(other Type) bool {
	rhs, ok := other.(DecimalType)
	if !ok {
		return false
	}

	return d.precision == rhs.precision &&
		d.scale == rhs.scale
}

func (d DecimalType) Type() string   { return fmt.Sprintf("decimal(%d, %d)", d.precision, d.scale) }
func (d DecimalType) String() string { return fmt.Sprintf("decimal(%d, %d)", d.precision, d.scale) }

type PrimitiveType interface {
	Type
	primitive()
}

type BooleanType struct{}

func (BooleanType) Equals(other Type) bool {
	_, ok := other.(BooleanType)
	return ok
}

func (BooleanType) primitive()     {}
func (BooleanType) Type() string   { return "boolean" }
func (BooleanType) String() string { return "boolean" }

type Int32Type struct{}

func (Int32Type) Equals(other Type) bool {
	_, ok := other.(Int32Type)
	return ok
}

func (Int32Type) primitive()     {}
func (Int32Type) Type() string   { return "int" }
func (Int32Type) String() string { return "int" }

type Int64Type struct{}

func (Int64Type) Equals(other Type) bool {
	_, ok := other.(Int64Type)
	return ok
}

func (Int64Type) primitive()     {}
func (Int64Type) Type() string   { return "long" }
func (Int64Type) String() string { return "long" }

type Float32Type struct{}

func (Float32Type) Equals(other Type) bool {
	_, ok := other.(Float32Type)
	return ok
}

func (Float32Type) primitive()     {}
func (Float32Type) Type() string   { return "float" }
func (Float32Type) String() string { return "float" }

type Float64Type struct{}

func (Float64Type) Equals(other Type) bool {
	_, ok := other.(Float64Type)
	return ok
}

func (Float64Type) primitive()     {}
func (Float64Type) Type() string   { return "double" }
func (Float64Type) String() string { return "double" }

type Date int32

type DateType struct{}

func (DateType) Equals(other Type) bool {
	_, ok := other.(DateType)
	return ok
}

func (DateType) primitive()     {}
func (DateType) Type() string   { return "date" }
func (DateType) String() string { return "date" }

type Time int64

type TimeType struct{}

func (TimeType) Equals(other Type) bool {
	_, ok := other.(TimeType)
	return ok
}

func (TimeType) primitive()     {}
func (TimeType) Type() string   { return "time" }
func (TimeType) String() string { return "time" }

type Timestamp int64
type TimestampType struct{}

func (TimestampType) Equals(other Type) bool {
	_, ok := other.(TimestampType)
	return ok
}

func (TimestampType) primitive()     {}
func (TimestampType) Type() string   { return "timestamp" }
func (TimestampType) String() string { return "timestamp" }

type TimestampTzType struct{}

func (TimestampTzType) Equals(other Type) bool {
	_, ok := other.(TimestampTzType)
	return ok
}

func (TimestampTzType) primitive()     {}
func (TimestampTzType) Type() string   { return "timestamptz" }
func (TimestampTzType) String() string { return "timestamptz" }

type StringType struct{}

func (StringType) Equals(other Type) bool {
	_, ok := other.(StringType)
	return ok
}

func (StringType) primitive()     {}
func (StringType) Type() string   { return "string" }
func (StringType) String() string { return "string" }

type UUIDType struct{}

func (UUIDType) Equals(other Type) bool {
	_, ok := other.(UUIDType)
	return ok
}

func (UUIDType) primitive()     {}
func (UUIDType) Type() string   { return "uuid" }
func (UUIDType) String() string { return "uuid" }

type BinaryType struct{}

func (BinaryType) Equals(other Type) bool {
	_, ok := other.(BinaryType)
	return ok
}

func (BinaryType) primitive()     {}
func (BinaryType) Type() string   { return "binary" }
func (BinaryType) String() string { return "binary" }

var PrimitiveTypes = struct {
	Bool        Type
	Int32       Type
	Int64       Type
	Float32     Type
	Float64     Type
	Date        Type
	Time        Type
	Timestamp   Type
	TimestampTz Type
	String      Type
	Binary      Type
	UUID        Type
}{
	Bool:        BooleanType{},
	Int32:       Int32Type{},
	Int64:       Int64Type{},
	Float32:     Float32Type{},
	Float64:     Float64Type{},
	Date:        DateType{},
	Time:        TimeType{},
	Timestamp:   TimestampType{},
	TimestampTz: TimestampTzType{},
	String:      StringType{},
	Binary:      BinaryType{},
	UUID:        UUIDType{},
}

type Row interface {
	Get(pos uint) (any, error)
	Set(pos uint, value any) error
}

// type Record struct {
// 	values     []any
// 	fieldNames []string
// 	fieldToIdx map[string]int
// }

// func NewRecord(data []any) *Record {}

// func NewRecordFromStruct(data []any, st StructType) (*Record, error) {

// }

// func (r *Record) Fields() []string { return r.fieldNames }

// func (r *Record) Get(pos uint) (any, error) {
// 	if pos >= uint(len(r.values)) {
// 		return nil, fmt.Errorf("%w: accessing field %d out of %d field record",
// 			ErrInvalidArgument, pos, len(r.values))
// 	}

// 	return r.values[pos], nil
// }

// func (r *Record) Set(pos uint, value any) error {
// 	if pos >= uint(len(r.values)) {
// 		return fmt.Errorf("%w: accessing field %d out of %d field record",
// 			ErrInvalidArgument, pos, len(r.values))
// 	}

// 	r.values[pos] = value
// 	return nil
// }
