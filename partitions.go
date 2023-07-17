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
	"encoding"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"golang.org/x/exp/slices"
)

const (
	partitionDataIDStart   = 1000
	InitialPartitionSpecID = 0
)

var (
	UnpartitionedPartitionSpec = &PartitionSpec{id: 0}
)

type PartitionField struct {
	SourceID  int       `json:"source-id"`
	FieldID   int       `json:"field-id"`
	Name      string    `json:"name"`
	Transform Transform `json:"transform"`
}

func (p *PartitionField) String() string {
	return fmt.Sprintf("%d: %s: %s(%d)", p.FieldID, p.Name, p.Transform, p.SourceID)
}

func (p *PartitionField) UnmarshalJSON(b []byte) error {
	type Alias PartitionField
	aux := struct {
		TransformString string `json:"transform"`
		*Alias
	}{
		Alias: (*Alias)(p),
	}

	err := json.Unmarshal(b, &aux)
	if err != nil {
		return err
	}

	if p.Transform, err = ParseTransform(aux.TransformString); err != nil {
		return err
	}

	return nil
}

type PartitionSpec struct {
	id     int
	fields []PartitionField

	sourceIdToFields map[int][]PartitionField
}

func NewPartitionSpec(fields ...PartitionField) PartitionSpec {
	return NewPartitionSpecID(InitialPartitionSpecID, fields...)
}

func NewPartitionSpecID(id int, fields ...PartitionField) PartitionSpec {
	ret := PartitionSpec{id: id, fields: fields}
	ret.initialize()
	return ret
}

func (ps *PartitionSpec) CompatibleWith(other *PartitionSpec) bool {
	if ps == other {
		return true
	}

	if len(ps.fields) != len(other.fields) {
		return false
	}

	return slices.EqualFunc(ps.fields, other.fields, func(left, right PartitionField) bool {
		return left.SourceID == right.SourceID && left.Name == right.Name &&
			left.Transform == right.Transform
	})
}

func (ps *PartitionSpec) Equals(other PartitionSpec) bool {
	return ps.id == other.id && slices.Equal(ps.fields, other.fields)
}

func (ps PartitionSpec) MarshalJSON() ([]byte, error) {
	if ps.fields == nil {
		ps.fields = []PartitionField{}
	}
	return json.Marshal(struct {
		ID     int              `json:"spec-id"`
		Fields []PartitionField `json:"fields"`
	}{ps.id, ps.fields})
}

func (ps *PartitionSpec) UnmarshalJSON(b []byte) error {
	aux := struct {
		ID     int              `json:"spec-id"`
		Fields []PartitionField `json:"fields"`
	}{ID: ps.id, Fields: ps.fields}

	if err := json.Unmarshal(b, &aux); err != nil {
		return err
	}

	ps.id, ps.fields = aux.ID, aux.Fields
	ps.initialize()
	return nil
}

func (ps *PartitionSpec) initialize() {
	ps.sourceIdToFields = make(map[int][]PartitionField)
	for _, f := range ps.fields {
		ps.sourceIdToFields[f.SourceID] =
			append(ps.sourceIdToFields[f.SourceID], f)
	}
}

func (ps *PartitionSpec) ID() int                    { return ps.id }
func (ps *PartitionSpec) NumFields() int             { return len(ps.fields) }
func (ps *PartitionSpec) Field(i int) PartitionField { return ps.fields[i] }
func (ps *PartitionSpec) IsUnpartitioned() bool      { return len(ps.fields) == 0 }
func (ps *PartitionSpec) FieldsBySourceID(fieldID int) []PartitionField {
	return slices.Clone(ps.sourceIdToFields[fieldID])
}

func (ps PartitionSpec) String() string {
	var b strings.Builder
	b.WriteByte('[')
	for i, f := range ps.fields {
		if i == 0 {
			b.WriteString("\n")
		}
		b.WriteString("\t")
		b.WriteString(f.String())
		b.WriteString("\n")
	}
	b.WriteByte(']')

	return b.String()
}

func (ps *PartitionSpec) LastAssignedFieldID() int {
	if len(ps.fields) == 0 {
		return partitionDataIDStart
	}

	id := ps.fields[0].FieldID
	for _, f := range ps.fields[1:] {
		if f.FieldID > id {
			id = f.FieldID
		}
	}
	return id
}

func (ps *PartitionSpec) PartitionType(schema *Schema) StructType {
	nestedFields := []NestedField{}

	return StructType{Fields: nestedFields}
}

var (
	ErrInvalidTransform = errors.New("invalid transform syntax")
)

func ParseTransform(s string) (Transform, error) {
	switch {
	case strings.HasPrefix(s, "bucket"):
		matches := regexFromBrackets.FindStringSubmatch(s)
		if len(matches) != 2 {
			break
		}

		n, _ := strconv.Atoi(matches[1])
		return BucketTransform{N: n}, nil
	case strings.HasPrefix(s, "truncate"):
		matches := regexFromBrackets.FindStringSubmatch(s)
		if len(matches) != 2 {
			break
		}

		n, _ := strconv.Atoi(matches[1])
		return TruncateTransform{W: n}, nil
	default:
		switch s {
		case "identity":
			return IdentityTransform{}, nil
		case "void":
			return VoidTransform{}, nil
		case "year":
			return YearTransform{}, nil
		case "month":
			return MonthTransform{}, nil
		case "day":
			return DayTransform{}, nil
		case "hour":
			return HourTransform{}, nil
		}
	}

	return nil, fmt.Errorf("%w: %s", ErrInvalidTransform, s)
}

type Transform interface {
	fmt.Stringer
	encoding.TextMarshaler
}

type IdentityTransform struct{}

func (t IdentityTransform) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func (IdentityTransform) String() string { return "identity" }

type VoidTransform struct{}

func (t VoidTransform) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func (VoidTransform) String() string { return "void" }

type BucketTransform struct {
	N int
}

func (t BucketTransform) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func (t BucketTransform) String() string { return fmt.Sprintf("bucket[%d]", t.N) }

type TruncateTransform struct {
	W int
}

func (t TruncateTransform) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func (t TruncateTransform) String() string { return fmt.Sprintf("truncate[%d]", t.W) }

type YearTransform struct{}

func (t YearTransform) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func (YearTransform) String() string { return "year" }

type MonthTransform struct{}

func (t MonthTransform) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func (MonthTransform) String() string { return "month" }

type DayTransform struct{}

func (t DayTransform) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func (DayTransform) String() string { return "day" }

type HourTransform struct{}

func (t HourTransform) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func (HourTransform) String() string { return "hour" }
