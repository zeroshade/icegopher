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

package table

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/zeroshade/icegopher"
)

type SortDirection string

const (
	SortASC  SortDirection = "asc"
	SortDESC SortDirection = "desc"
)

type NullOrder string

const (
	NullsFirst NullOrder = "nulls-first"
	NullsLast  NullOrder = "nulls-last"
)

var (
	ErrInvalidSortDirection = errors.New("invalid sort direction, must be 'asc' or 'desc'")
	ErrInvalidNullOrder     = errors.New("invalid null order, must be 'nulls-first' or 'nulls-last'")
)

type SortField struct {
	SourceID  int                 `json:"source-id"`
	Transform icegopher.Transform `json:"transform"`
	Direction SortDirection       `json:"direction"`
	NullOrder NullOrder           `json:"null-order"`
}

func (s *SortField) String() string {
	if _, ok := s.Transform.(icegopher.IdentityTransform); ok {
		return fmt.Sprintf("%d %s %s", s.SourceID, s.Direction, s.NullOrder)
	}
	return fmt.Sprintf("%s(%d) %s %s", s.Transform, s.SourceID, s.Direction, s.NullOrder)
}

func (s *SortField) MarshalJSON() ([]byte, error) {
	if s.Direction == "" {
		s.Direction = SortASC
	}

	if s.NullOrder == "" {
		if s.Direction == SortASC {
			s.NullOrder = NullsFirst
		} else {
			s.NullOrder = NullsLast
		}
	}

	type Alias SortField
	return json.Marshal((*Alias)(s))
}

func (s *SortField) UnmarshalJSON(b []byte) error {
	type Alias SortField
	var aux = struct {
		TransformString string `json:"transform"`
		*Alias
	}{
		Alias: (*Alias)(s),
	}

	err := json.Unmarshal(b, &aux)
	if err != nil {
		return err
	}

	if s.Transform, err = icegopher.ParseTransform(aux.TransformString); err != nil {
		return err
	}

	switch s.Direction {
	case SortASC, SortDESC:
	default:
		return ErrInvalidSortDirection
	}

	switch s.NullOrder {
	case NullsFirst, NullsLast:
	default:
		return ErrInvalidNullOrder
	}

	return nil
}

const (
	InitialSortOrderID  = 1
	UnsortedSortOrderID = 0
)

var UnsortedSortOrder = SortOrder{OrderID: UnsortedSortOrderID, Fields: []SortField{}}

type SortOrder struct {
	OrderID int         `json:"order-id"`
	Fields  []SortField `json:"fields"`
}

func (s SortOrder) String() string {
	var b strings.Builder
	b.WriteByte('[')
	for i, f := range s.Fields {
		if i == 0 {
			b.WriteByte('\n')
		}
		b.WriteString(f.String())
		b.WriteByte('\n')
	}
	b.WriteByte(']')
	return b.String()
}

func (s *SortOrder) UnmarshalJSON(b []byte) error {
	type Alias SortOrder
	aux := (*Alias)(s)

	if err := json.Unmarshal(b, aux); err != nil {
		return err
	}

	if len(s.Fields) == 0 {
		s.Fields = []SortField{}
		s.OrderID = 0
		return nil
	}

	if s.OrderID == 0 {
		s.OrderID = InitialSortOrderID // initialize default sort order id
	}

	return nil
}
