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
	"strings"
	"sync"
	"sync/atomic"

	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

var (
	ErrInvalidSchema = errors.New("invalid schema")
)

type Schema struct {
	ID                 int   `json:"schema-id"`
	IdentifierFieldIDs []int `json:"identifier-field-ids"`

	fields []NestedField

	idToName      atomic.Pointer[map[int]string]
	idToField     atomic.Pointer[map[int]NestedField]
	nameToID      atomic.Pointer[map[string]int]
	nameToIDLower atomic.Pointer[map[string]int]
	idToAccessor  atomic.Pointer[map[int]*Accessor]

	mx sync.Mutex
}

func NewSchema(id int, fields ...NestedField) *Schema {
	return NewSchemaWithIdentifiers(id, []int{}, fields...)
}

func NewSchemaWithIdentifiers(id int, identifierIDs []int, fields ...NestedField) *Schema {
	return &Schema{ID: id, fields: fields, IdentifierFieldIDs: identifierIDs}
}

func (s *Schema) lazyNameToID() (map[string]int, error) {
	index := s.nameToID.Load()
	if index != nil {
		return *index, nil
	}

	s.mx.Lock()
	defer s.mx.Unlock()

	idx, err := IndexByName(s)
	if err != nil {
		return nil, err
	}

	s.nameToID.Store(&idx)
	return idx, nil
}

func (s *Schema) lazyIDToField() (map[int]NestedField, error) {
	index := s.idToField.Load()
	if index != nil {
		return *index, nil
	}

	s.mx.Lock()
	defer s.mx.Unlock()

	idx, err := IndexByID(s)
	if err != nil {
		return nil, err
	}

	s.idToField.Store(&idx)
	return idx, nil
}

func (s *Schema) lazyIDToName() (map[int]string, error) {
	index := s.idToName.Load()
	if index != nil {
		return *index, nil
	}

	s.mx.Lock()
	defer s.mx.Unlock()

	idx, err := IndexNameByID(s)
	if err != nil {
		return nil, err
	}

	s.idToName.Store(&idx)
	return idx, nil
}

func (s *Schema) lazyNameToIDLower() (map[string]int, error) {
	index := s.nameToIDLower.Load()
	if index != nil {
		return *index, nil
	}

	idx, err := s.lazyNameToID()
	if err != nil {
		return nil, err
	}

	s.mx.Lock()
	defer s.mx.Unlock()

	out := make(map[string]int)
	for k, v := range idx {
		out[strings.ToLower(k)] = v
	}

	s.nameToIDLower.Store(&out)
	return out, nil
}

func (s *Schema) lazyIDToAccessor() (map[int]*Accessor, error) {
	index := s.idToAccessor.Load()
	if index != nil {
		return *index, nil
	}

	s.mx.Lock()
	defer s.mx.Unlock()

	idx, err := BuildPositionAccessors(s)
	if err != nil {
		return nil, err
	}

	s.idToAccessor.Store(&idx)
	return idx, nil
}

func (s *Schema) Type() string            { return "struct" }
func (s *Schema) AsStruct() StructType    { return StructType{Fields: s.fields} }
func (s *Schema) NumFields() int          { return len(s.fields) }
func (s *Schema) Field(i int) NestedField { return s.fields[i] }
func (s *Schema) Children() []NestedField { return slices.Clone(s.fields) }
func (s *Schema) UnmarshalJSON(b []byte) error {
	type Alias Schema
	aux := struct {
		Fields []NestedField `json:"fields"`
		*Alias
	}{Alias: (*Alias)(s)}

	if err := json.Unmarshal(b, &aux); err != nil {
		return err
	}

	s.fields = aux.Fields
	if s.IdentifierFieldIDs == nil {
		s.IdentifierFieldIDs = []int{}
	}
	return nil
}

func (s *Schema) MarshalJSON() ([]byte, error) {
	if s.IdentifierFieldIDs == nil {
		s.IdentifierFieldIDs = []int{}
	}

	type Alias Schema
	return json.Marshal(struct {
		Type   string        `json:"type"`
		Fields []NestedField `json:"fields"`
		*Alias
	}{Type: "struct", Fields: s.fields, Alias: (*Alias)(s)})
}

func (s *Schema) FindColumnName(fieldID int) (string, bool) {
	idx, _ := s.lazyIDToName()
	col, ok := idx[fieldID]
	return col, ok
}

func (s *Schema) FindFieldByName(name string) (NestedField, bool) {
	idx, _ := s.lazyNameToID()

	id, ok := idx[name]
	if !ok {
		return NestedField{}, false
	}

	return s.FindFieldByID(id)
}

func (s *Schema) FindFieldByNameCaseInsensitive(name string) (NestedField, bool) {
	idx, _ := s.lazyNameToIDLower()

	id, ok := idx[strings.ToLower(name)]
	if !ok {
		return NestedField{}, false
	}

	return s.FindFieldByID(id)
}

func (s *Schema) FindFieldByID(id int) (NestedField, bool) {
	idx, _ := s.lazyIDToField()
	f, ok := idx[id]
	return f, ok
}

func (s *Schema) FindTypeByID(id int) (Type, bool) {
	f, ok := s.FindFieldByID(id)
	if !ok {
		return nil, false
	}

	return f.Type, true
}

func (s *Schema) FindTypeByName(name string) (Type, bool) {
	f, ok := s.FindFieldByName(name)
	if !ok {
		return nil, false
	}

	return f.Type, true
}

func (s *Schema) FindTypeByNameCaseInsensitive(name string) (Type, bool) {
	f, ok := s.FindFieldByNameCaseInsensitive(name)
	if !ok {
		return nil, false
	}

	return f.Type, true
}

func (s *Schema) Equals(other *Schema) bool {
	if other == nil {
		return false
	}

	if s == other {
		return true
	}

	if len(s.fields) != len(other.fields) {
		return false
	}

	if !slices.Equal(s.IdentifierFieldIDs, other.IdentifierFieldIDs) {
		return false
	}

	return slices.EqualFunc(s.fields, other.fields, func(a, b NestedField) bool {
		return a.Equals(b)
	})
}

func (s *Schema) HighestFieldID() int {
	id, _ := Visit[int](s, findLastFieldID{})
	return id
}

func (s *Schema) AccessorForField(id int) (*Accessor, error) {
	idx, _ := s.lazyIDToAccessor()
	acc, ok := idx[id]
	if !ok {
		return nil, fmt.Errorf("%w: could not find accessor for field with id %d",
			ErrInvalidSchema, id)
	}

	return acc, nil
}

var void = struct{}{}

func (s *Schema) Select(caseSensitive bool, names ...string) (*Schema, error) {
	ids := make(map[int]struct{})
	if caseSensitive {
		nameMap, _ := s.lazyNameToID()
		for _, n := range names {
			id, ok := nameMap[n]
			if !ok {
				return nil, fmt.Errorf("%w: could not find column %s", ErrInvalidSchema, n)
			}
			ids[id] = void
		}
	} else {
		nameMap, _ := s.lazyNameToIDLower()
		for _, n := range names {
			id, ok := nameMap[strings.ToLower(n)]
			if !ok {
				return nil, fmt.Errorf("%w: could not find column %s", ErrInvalidSchema, n)
			}
			ids[id] = void
		}
	}

	return PruneColumns(s, ids, true)
}

type Accessor struct {
	pos   uint
	inner *Accessor
}

func (a *Accessor) Get(container Row) (any, error) {
	var (
		val any
		err error
	)
	for inner := a; inner != nil; inner = inner.inner {
		val, err = container.Get(inner.pos)
		if err != nil {
			return nil, err
		}
		container, _ = val.(Row)
	}

	return val, nil
}

type SchemaVisitor[T any] interface {
	Schema(schema *Schema, structResult T) T
	Struct(st StructType, fieldResults []T) T
	Field(field NestedField, fieldResult T) T
	List(list ListType, elemResult T) T
	Map(mapType MapType, keyResult, valueResult T) T
	Primitive(p PrimitiveType) T
}

type BeforeFieldVisitor interface {
	BeforeField(field NestedField)
}

type AfterFieldVisitor interface {
	AfterField(field NestedField)
}

type BeforeListElementVisitor interface {
	BeforeListElement(elem NestedField)
}

type AfterListElementVisitor interface {
	AfterListElement(elem NestedField)
}

type BeforeMapKeyVisitor interface {
	BeforeMapKey(key NestedField)
}

type AfterMapKeyVisitor interface {
	AfterMapKey(key NestedField)
}

type BeforeMapValueVisitor interface {
	BeforeMapValue(value NestedField)
}

type AfterMapValueVisitor interface {
	AfterMapValue(value NestedField)
}

func Visit[T any](sc *Schema, visitor SchemaVisitor[T]) (res T, err error) {
	defer func() {
		if r := recover(); r != nil {
			switch e := r.(type) {
			case string:
				err = fmt.Errorf("error encountered during schema visitor: %s", e)
			case error:
				err = fmt.Errorf("error encountered during schema visitor: %w", e)
			}
		}
	}()

	return visitor.Schema(sc, visitStruct(sc.AsStruct(), visitor)), nil
}

func visitStruct[T any](obj StructType, visitor SchemaVisitor[T]) T {
	results := make([]T, len(obj.Fields))

	bf, _ := visitor.(BeforeFieldVisitor)
	af, _ := visitor.(AfterFieldVisitor)

	for i, f := range obj.Fields {
		if bf != nil {
			bf.BeforeField(f)
		}

		res := visitField(f, visitor)

		if af != nil {
			af.AfterField(f)
		}

		results[i] = visitor.Field(f, res)
	}

	return visitor.Struct(obj, results)
}

func visitList[T any](obj ListType, visitor SchemaVisitor[T]) T {
	elemField := obj.ElementField()

	if bl, ok := visitor.(BeforeListElementVisitor); ok {
		bl.BeforeListElement(elemField)
	} else if bf, ok := visitor.(BeforeFieldVisitor); ok {
		bf.BeforeField(elemField)
	}

	res := visitField(elemField, visitor)

	if al, ok := visitor.(AfterListElementVisitor); ok {
		al.AfterListElement(elemField)
	} else if af, ok := visitor.(AfterFieldVisitor); ok {
		af.AfterField(elemField)
	}

	return visitor.List(obj, res)
}

func visitMap[T any](obj MapType, visitor SchemaVisitor[T]) T {
	keyField, valueField := obj.KeyField(), obj.ValueField()

	if bmk, ok := visitor.(BeforeMapKeyVisitor); ok {
		bmk.BeforeMapKey(keyField)
	} else if bf, ok := visitor.(BeforeFieldVisitor); ok {
		bf.BeforeField(keyField)
	}

	keyRes := visitField(keyField, visitor)

	if amk, ok := visitor.(AfterMapKeyVisitor); ok {
		amk.AfterMapKey(keyField)
	} else if af, ok := visitor.(AfterFieldVisitor); ok {
		af.AfterField(keyField)
	}

	if bmk, ok := visitor.(BeforeMapValueVisitor); ok {
		bmk.BeforeMapValue(valueField)
	} else if bf, ok := visitor.(BeforeFieldVisitor); ok {
		bf.BeforeField(valueField)
	}

	valueRes := visitField(valueField, visitor)

	if amk, ok := visitor.(AfterMapValueVisitor); ok {
		amk.AfterMapValue(valueField)
	} else if af, ok := visitor.(AfterFieldVisitor); ok {
		af.AfterField(valueField)
	}

	return visitor.Map(obj, keyRes, valueRes)
}

func visitField[T any](f NestedField, visitor SchemaVisitor[T]) T {
	switch typ := f.Type.(type) {
	case *StructType:
		return visitStruct(*typ, visitor)
	case *ListType:
		return visitList(*typ, visitor)
	case *MapType:
		return visitMap(*typ, visitor)
	default: // primitive
		return visitor.Primitive(typ.(PrimitiveType))
	}
}

func IndexByID(schema *Schema) (map[int]NestedField, error) {
	return Visit[map[int]NestedField](schema, &indexByID{index: make(map[int]NestedField)})
}

type indexByID struct {
	index map[int]NestedField
}

func (i *indexByID) Schema(*Schema, map[int]NestedField) map[int]NestedField {
	return i.index
}

func (i *indexByID) Struct(StructType, []map[int]NestedField) map[int]NestedField {
	return i.index
}

func (i *indexByID) Field(field NestedField, _ map[int]NestedField) map[int]NestedField {
	i.index[field.ID] = field
	return i.index
}

func (i *indexByID) List(list ListType, _ map[int]NestedField) map[int]NestedField {
	i.index[list.ElementID] = list.ElementField()
	return i.index
}

func (i *indexByID) Map(mapType MapType, _, _ map[int]NestedField) map[int]NestedField {
	i.index[mapType.KeyID] = mapType.KeyField()
	i.index[mapType.ValueID] = mapType.ValueField()
	return i.index
}

func (i *indexByID) Primitive(PrimitiveType) map[int]NestedField {
	return i.index
}

func IndexByName(schema *Schema) (map[string]int, error) {
	if len(schema.fields) > 0 {
		indexer := &indexByName{
			index:           make(map[string]int),
			shortNameId:     make(map[string]int),
			fieldNames:      make([]string, 0),
			shortFieldNames: make([]string, 0),
		}
		if _, err := Visit[map[string]int](schema, indexer); err != nil {
			return nil, err
		}

		return indexer.ByName(), nil
	}
	return map[string]int{}, nil
}

func IndexNameByID(schema *Schema) (map[int]string, error) {
	indexer := &indexByName{
		index:           make(map[string]int),
		shortNameId:     make(map[string]int),
		fieldNames:      make([]string, 0),
		shortFieldNames: make([]string, 0),
	}
	if _, err := Visit[map[string]int](schema, indexer); err != nil {
		return nil, err
	}
	return indexer.ByID(), nil
}

type indexByName struct {
	index           map[string]int
	shortNameId     map[string]int
	combinedIndex   map[string]int
	fieldNames      []string
	shortFieldNames []string
}

func (i *indexByName) ByID() map[int]string {
	idToName := make(map[int]string)
	for k, v := range i.index {
		idToName[v] = k
	}
	return idToName
}

func (i *indexByName) ByName() map[string]int {
	i.combinedIndex = maps.Clone(i.shortNameId)
	maps.Copy(i.combinedIndex, i.index)
	return i.combinedIndex
}

func (i *indexByName) Primitive(PrimitiveType) map[string]int { return i.index }
func (i *indexByName) addField(name string, fieldID int) {
	fullName := name
	if len(i.fieldNames) > 0 {
		fullName = strings.Join(i.fieldNames, ".") + "." + name
	}

	if _, ok := i.index[fullName]; ok {
		panic(fmt.Errorf("%w: multiple fields for name %s: %d and %d",
			ErrInvalidSchema, fullName, i.index[fullName], fieldID))
	}

	i.index[fullName] = fieldID
	if len(i.shortFieldNames) > 0 {
		shortName := strings.Join(i.shortFieldNames, ".") + "." + name
		i.shortNameId[shortName] = fieldID
	}
}

func (i *indexByName) Schema(*Schema, map[string]int) map[string]int {
	return i.index
}

func (i *indexByName) Struct(StructType, []map[string]int) map[string]int {
	return i.index
}

func (i *indexByName) Field(field NestedField, _ map[string]int) map[string]int {
	i.addField(field.Name, field.ID)
	return i.index
}

func (i *indexByName) List(list ListType, _ map[string]int) map[string]int {
	i.addField(list.ElementField().Name, list.ElementID)
	return i.index
}

func (i *indexByName) Map(mapType MapType, _, _ map[string]int) map[string]int {
	i.addField(mapType.KeyField().Name, mapType.KeyID)
	i.addField(mapType.ValueField().Name, mapType.ValueID)
	return i.index
}

func (i *indexByName) BeforeListElement(elem NestedField) {
	if _, ok := elem.Type.(*StructType); !ok {
		i.shortFieldNames = append(i.shortFieldNames, elem.Name)
	}
	i.fieldNames = append(i.fieldNames, elem.Name)
}

func (i *indexByName) AfterListElement(elem NestedField) {
	if _, ok := elem.Type.(*StructType); !ok {
		i.shortFieldNames = i.shortFieldNames[:len(i.shortFieldNames)-1]
	}
	i.fieldNames = i.fieldNames[:len(i.fieldNames)-1]
}

func (i *indexByName) BeforeField(field NestedField) {
	i.fieldNames = append(i.fieldNames, field.Name)
	i.shortFieldNames = append(i.shortFieldNames, field.Name)
}

func (i *indexByName) AfterField(field NestedField) {
	i.fieldNames = i.fieldNames[:len(i.fieldNames)-1]
	i.shortFieldNames = i.shortFieldNames[:len(i.shortFieldNames)-1]
}

func PruneColumns(schema *Schema, selected map[int]struct{}, selectFullTypes bool) (*Schema, error) {
	// st := schema.AsStruct()
	result, err := Visit[Type](schema, &pruneColVisitor{selected: selected,
		fullTypes: selectFullTypes})
	if err != nil {
		return nil, err
	}

	n, ok := result.(NestedType)
	if !ok {
		n = &StructType{}
	}

	newIdentifierIDs := make([]int, 0, len(schema.IdentifierFieldIDs))
	for _, id := range schema.IdentifierFieldIDs {
		if _, ok := selected[id]; ok {
			newIdentifierIDs = append(newIdentifierIDs, id)
		}
	}

	return &Schema{
		fields:             n.Children(),
		ID:                 schema.ID,
		IdentifierFieldIDs: newIdentifierIDs,
	}, nil
}

type pruneColVisitor struct {
	selected  map[int]struct{}
	fullTypes bool
}

func (p *pruneColVisitor) Schema(_ *Schema, structResult Type) Type {
	return structResult
}

func (p *pruneColVisitor) Struct(st StructType, fieldResults []Type) Type {
	selected, fields := []NestedField{}, st.Fields
	sameType := true

	for i, t := range fieldResults {
		field := fields[i]
		if field.Type == t {
			selected = append(selected, field)
		} else if t != nil {
			sameType = false
			// type has changed, create a new field with the projected type
			selected = append(selected, NestedField{
				ID:       field.ID,
				Name:     field.Name,
				Type:     t,
				Doc:      field.Doc,
				Required: field.Required,
			})
		}
	}

	if len(selected) > 0 {
		if len(selected) == len(fields) && sameType {
			// nothing changed, return the original
			return &st
		} else {
			return &StructType{Fields: selected}
		}
	}

	return nil
}

func (p *pruneColVisitor) Field(field NestedField, fieldResult Type) Type {
	_, ok := p.selected[field.ID]
	if !ok {
		if fieldResult != nil {
			return fieldResult
		}

		return nil
	}

	if p.fullTypes {
		return field.Type
	}

	if _, ok := field.Type.(*StructType); ok {
		return p.projectSelectedStruct(fieldResult)
	}

	typ, ok := field.Type.(PrimitiveType)
	if !ok {
		panic(fmt.Errorf("%w: cannot explicitly project List or Map types, %d:%s of type %s was selected",
			ErrInvalidSchema, field.ID, field.Name, field.Type))
	}
	return typ
}

func (p *pruneColVisitor) List(list ListType, elemResult Type) Type {
	_, ok := p.selected[list.ElementID]
	if !ok {
		if elemResult != nil {
			return p.projectList(&list, elemResult)
		}

		return nil
	}

	if p.fullTypes {
		return &list
	}

	_, ok = list.Element.(*StructType)
	if list.Element != nil && ok {
		projected := p.projectSelectedStruct(elemResult)
		return p.projectList(&list, projected)
	}

	if _, ok = list.Element.(PrimitiveType); !ok {
		panic(fmt.Errorf("%w: cannot explicitly project List or Map types, %d of type %s was selected",
			ErrInvalidSchema, list.ElementID, list.Element))
	}

	return &list
}

func (p *pruneColVisitor) Map(mapType MapType, keyResult, valueResult Type) Type {
	_, ok := p.selected[mapType.ValueID]
	if !ok {
		if valueResult != nil {
			return p.projectMap(&mapType, valueResult)
		}

		if _, ok = p.selected[mapType.KeyID]; ok {
			return &mapType
		}

		return nil
	}

	if p.fullTypes {
		return &mapType
	}

	_, ok = mapType.ValueType.(*StructType)
	if mapType.ValueType != nil && ok {
		projected := p.projectSelectedStruct(valueResult)
		return p.projectMap(&mapType, projected)
	}

	if _, ok = mapType.ValueType.(PrimitiveType); !ok {
		panic(fmt.Errorf("%w: cannot explicitly project List or Map types, Map value %d of type %s was selected",
			ErrInvalidSchema, mapType.ValueID, mapType.ValueType))
	}

	return &mapType
}

func (p *pruneColVisitor) Primitive(_ PrimitiveType) Type { return nil }

func (*pruneColVisitor) projectSelectedStruct(projected Type) *StructType {
	if projected == nil {
		return &StructType{}
	}

	if ty, ok := projected.(*StructType); ok {
		return ty
	}

	panic("expected a struct")
}

func (*pruneColVisitor) projectList(listType *ListType, elementResult Type) *ListType {
	if listType.Element.Equals(elementResult) {
		return listType
	}

	return &ListType{ElementID: listType.ElementID, Element: elementResult,
		ElementRequired: listType.ElementRequired}
}

func (*pruneColVisitor) projectMap(mapType *MapType, valueResult Type) *MapType {
	if mapType.ValueType.Equals(valueResult) {
		return mapType
	}

	return &MapType{
		KeyID:         mapType.KeyID,
		ValueID:       mapType.ValueID,
		KeyType:       mapType.KeyType,
		ValueType:     valueResult,
		ValueRequired: mapType.ValueRequired,
	}
}

type findLastFieldID struct{}

func (findLastFieldID) Schema(_ *Schema, result int) int {
	return result
}

func (findLastFieldID) Struct(_ StructType, fieldResults []int) int {
	return max(fieldResults...)
}

func (findLastFieldID) Field(field NestedField, fieldResult int) int {
	return max(field.ID, fieldResult)
}

func (findLastFieldID) List(_ ListType, elemResult int) int { return elemResult }

func (findLastFieldID) Map(_ MapType, keyResult, valueResult int) int {
	return max(keyResult, valueResult)
}

func (findLastFieldID) Primitive(PrimitiveType) int { return 0 }

type buildPosAccessors struct{}

func (buildPosAccessors) Schema(_ *Schema, result map[int]*Accessor) map[int]*Accessor {
	return result
}

func (buildPosAccessors) Struct(st StructType, fieldResults []map[int]*Accessor) map[int]*Accessor {
	result := make(map[int]*Accessor)

	for pos, f := range st.Fields {
		if fieldResults[pos] != nil {
			for innerID, acc := range fieldResults[pos] {
				result[innerID] = &Accessor{pos: uint(pos), inner: acc}
			}
		} else {
			result[f.ID] = &Accessor{pos: uint(pos)}
		}
	}

	return result
}

func (buildPosAccessors) Field(_ NestedField, fieldResult map[int]*Accessor) map[int]*Accessor {
	return fieldResult
}

func (buildPosAccessors) List(ListType, map[int]*Accessor) map[int]*Accessor      { return nil }
func (buildPosAccessors) Map(_ MapType, _, _ map[int]*Accessor) map[int]*Accessor { return nil }
func (buildPosAccessors) Primitive(PrimitiveType) map[int]*Accessor               { return nil }

func BuildPositionAccessors(schema *Schema) (map[int]*Accessor, error) {
	return Visit[map[int]*Accessor](schema, buildPosAccessors{})
}