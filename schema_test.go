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

package icegopher_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/zeroshade/icegopher"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	tableSchemaNested = icegopher.NewSchemaWithIdentifiers(1,
		[]int{1},
		icegopher.NestedField{
			ID: 1, Name: "foo", Type: icegopher.PrimitiveTypes.String, Required: false},
		icegopher.NestedField{
			ID: 2, Name: "bar", Type: icegopher.PrimitiveTypes.Int32, Required: true},
		icegopher.NestedField{
			ID: 3, Name: "baz", Type: icegopher.PrimitiveTypes.Bool, Required: false},
		icegopher.NestedField{
			ID: 4, Name: "qux", Required: true, Type: &icegopher.ListType{
				ElementID: 5, Element: icegopher.PrimitiveTypes.String, ElementRequired: true}},
		icegopher.NestedField{
			ID: 6, Name: "quux",
			Type: &icegopher.MapType{
				KeyID:   7,
				KeyType: icegopher.PrimitiveTypes.String,
				ValueID: 8,
				ValueType: &icegopher.MapType{
					KeyID:         9,
					KeyType:       icegopher.PrimitiveTypes.String,
					ValueID:       10,
					ValueType:     icegopher.PrimitiveTypes.Int32,
					ValueRequired: true,
				},
				ValueRequired: true,
			},
			Required: true},
		icegopher.NestedField{
			ID: 11, Name: "location", Type: &icegopher.ListType{
				ElementID: 12, Element: &icegopher.StructType{
					Fields: []icegopher.NestedField{
						{ID: 13, Name: "latitude", Type: icegopher.PrimitiveTypes.Float32, Required: false},
						{ID: 14, Name: "longitude", Type: icegopher.PrimitiveTypes.Float32, Required: false},
					},
				},
				ElementRequired: true},
			Required: true},
		icegopher.NestedField{
			ID:   15,
			Name: "person",
			Type: &icegopher.StructType{
				Fields: []icegopher.NestedField{
					{ID: 16, Name: "name", Type: icegopher.PrimitiveTypes.String, Required: false},
					{ID: 17, Name: "age", Type: icegopher.PrimitiveTypes.Int32, Required: true},
				},
			},
			Required: false,
		},
	)

	tableSchemaSimple = icegopher.NewSchemaWithIdentifiers(1,
		[]int{2},
		icegopher.NestedField{ID: 1, Name: "foo", Type: icegopher.PrimitiveTypes.String},
		icegopher.NestedField{ID: 2, Name: "bar", Type: icegopher.PrimitiveTypes.Int32, Required: true},
		icegopher.NestedField{ID: 3, Name: "baz", Type: icegopher.PrimitiveTypes.Bool},
	)
)

func TestNestedFieldToString(t *testing.T) {
	tests := []struct {
		idx      int
		expected string
	}{
		{0, "1: foo: optional string"},
		{1, "2: bar: required int"},
		{2, "3: baz: optional boolean"},
		{3, "4: qux: required list<string>"},
		{4, "6: quux: required map<string, map<string, int>>"},
		{5, "11: location: required list<struct<latitude: float, longitude: float>>"},
		{6, "15: person: optional struct<name: string, age: int>"},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.expected, tableSchemaNested.Field(tt.idx).String())
	}
}

func TestSchemaIndexByIDVisitor(t *testing.T) {
	index, err := icegopher.IndexByID(tableSchemaNested)
	require.NoError(t, err)

	assert.Equal(t, map[int]icegopher.NestedField{
		1: tableSchemaNested.Field(0),
		2: tableSchemaNested.Field(1),
		3: tableSchemaNested.Field(2),
		4: tableSchemaNested.Field(3),
		5: {ID: 5, Name: "element", Type: icegopher.PrimitiveTypes.String, Required: true},
		6: tableSchemaNested.Field(4),
		7: {ID: 7, Name: "key", Type: icegopher.PrimitiveTypes.String, Required: true},
		8: {ID: 8, Name: "value", Type: &icegopher.MapType{
			KeyID:         9,
			KeyType:       icegopher.PrimitiveTypes.String,
			ValueID:       10,
			ValueType:     icegopher.PrimitiveTypes.Int32,
			ValueRequired: true,
		}, Required: true},
		9:  {ID: 9, Name: "key", Type: icegopher.PrimitiveTypes.String, Required: true},
		10: {ID: 10, Name: "value", Type: icegopher.PrimitiveTypes.Int32, Required: true},
		11: tableSchemaNested.Field(5),
		12: {ID: 12, Name: "element", Type: &icegopher.StructType{
			Fields: []icegopher.NestedField{
				{ID: 13, Name: "latitude", Type: icegopher.PrimitiveTypes.Float32, Required: false},
				{ID: 14, Name: "longitude", Type: icegopher.PrimitiveTypes.Float32, Required: false},
			},
		}, Required: true},
		13: {ID: 13, Name: "latitude", Type: icegopher.PrimitiveTypes.Float32, Required: false},
		14: {ID: 14, Name: "longitude", Type: icegopher.PrimitiveTypes.Float32, Required: false},
		15: tableSchemaNested.Field(6),
		16: {ID: 16, Name: "name", Type: icegopher.PrimitiveTypes.String, Required: false},
		17: {ID: 17, Name: "age", Type: icegopher.PrimitiveTypes.Int32, Required: true},
	}, index)
}

func TestSchemaIndexByName(t *testing.T) {
	index, err := icegopher.IndexByName(tableSchemaNested)
	require.NoError(t, err)

	assert.Equal(t, map[string]int{
		"foo":                        1,
		"bar":                        2,
		"baz":                        3,
		"qux":                        4,
		"qux.element":                5,
		"quux":                       6,
		"quux.key":                   7,
		"quux.value":                 8,
		"quux.value.key":             9,
		"quux.value.value":           10,
		"location":                   11,
		"location.element":           12,
		"location.element.latitude":  13,
		"location.element.longitude": 14,
		"location.latitude":          13,
		"location.longitude":         14,
		"person":                     15,
		"person.name":                16,
		"person.age":                 17,
	}, index)
}

func TestSchemaFindColumnName(t *testing.T) {
	tests := []struct {
		id   int
		name string
	}{
		{1, "foo"},
		{2, "bar"},
		{3, "baz"},
		{4, "qux"},
		{5, "qux.element"},
		{6, "quux"},
		{7, "quux.key"},
		{8, "quux.value"},
		{9, "quux.value.key"},
		{10, "quux.value.value"},
		{11, "location"},
		{12, "location.element"},
		{13, "location.element.latitude"},
		{14, "location.element.longitude"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n, ok := tableSchemaNested.FindColumnName(tt.id)
			assert.True(t, ok)
			assert.Equal(t, tt.name, n)
		})
	}
}

func TestSchemaFindColumnNameIDNotFound(t *testing.T) {
	n, ok := tableSchemaNested.FindColumnName(99)
	assert.False(t, ok)
	assert.Empty(t, n)
}

func TestSchemaFindColumnNameByID(t *testing.T) {
	tests := []struct {
		id   int
		name string
	}{
		{1, "foo"},
		{2, "bar"},
		{3, "baz"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n, ok := tableSchemaSimple.FindColumnName(tt.id)
			assert.True(t, ok)
			assert.Equal(t, tt.name, n)
		})
	}
}

func TestSchemaFindFieldByID(t *testing.T) {
	index, err := icegopher.IndexByID(tableSchemaSimple)
	require.NoError(t, err)

	col1 := index[1]
	assert.Equal(t, 1, col1.ID)
	assert.Equal(t, icegopher.PrimitiveTypes.String, col1.Type)
	assert.False(t, col1.Required)

	col2 := index[2]
	assert.Equal(t, 2, col2.ID)
	assert.Equal(t, icegopher.PrimitiveTypes.Int32, col2.Type)
	assert.True(t, col2.Required)

	col3 := index[3]
	assert.Equal(t, 3, col3.ID)
	assert.Equal(t, icegopher.PrimitiveTypes.Bool, col3.Type)
	assert.False(t, col3.Required)
}

func TestFindFieldByIDUnknownField(t *testing.T) {
	index, err := icegopher.IndexByID(tableSchemaSimple)
	require.NoError(t, err)
	_, ok := index[4]
	assert.False(t, ok)
}

func TestSchemaFindField(t *testing.T) {
	tests := []icegopher.NestedField{
		{ID: 1, Name: "foo", Type: icegopher.PrimitiveTypes.String, Required: false},
		{ID: 2, Name: "bar", Type: icegopher.PrimitiveTypes.Int32, Required: true},
		{ID: 3, Name: "baz", Type: icegopher.PrimitiveTypes.Bool, Required: false},
	}

	for _, tt := range tests {
		t.Run(tt.Name, func(t *testing.T) {
			f, ok := tableSchemaSimple.FindFieldByID(tt.ID)
			assert.True(t, ok)
			assert.Equal(t, tt, f)

			f, ok = tableSchemaSimple.FindFieldByName(tt.Name)
			assert.True(t, ok)
			assert.Equal(t, tt, f)

			f, ok = tableSchemaSimple.FindFieldByNameCaseInsensitive(strings.ToUpper(tt.Name))
			assert.True(t, ok)
			assert.Equal(t, tt, f)
		})
	}
}

func TestSchemaFindType(t *testing.T) {
	_, ok := tableSchemaSimple.FindTypeByID(0)
	assert.False(t, ok)
	_, ok = tableSchemaSimple.FindTypeByName("FOOBAR")
	assert.False(t, ok)
	_, ok = tableSchemaSimple.FindTypeByNameCaseInsensitive("FOOBAR")
	assert.False(t, ok)

	tests := []struct {
		id   int
		name string
		typ  icegopher.Type
	}{
		{1, "foo", icegopher.PrimitiveTypes.String},
		{2, "bar", icegopher.PrimitiveTypes.Int32},
		{3, "baz", icegopher.PrimitiveTypes.Bool},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			typ, ok := tableSchemaSimple.FindTypeByID(tt.id)
			assert.True(t, ok)
			assert.Equal(t, tt.typ, typ)

			typ, ok = tableSchemaSimple.FindTypeByName(tt.name)
			assert.True(t, ok)
			assert.Equal(t, tt.typ, typ)

			typ, ok = tableSchemaSimple.FindTypeByNameCaseInsensitive(strings.ToUpper(tt.name))
			assert.True(t, ok)
			assert.Equal(t, tt.typ, typ)
		})
	}
}

func TestSerializeSchema(t *testing.T) {
	data, err := json.Marshal(tableSchemaSimple)
	require.NoError(t, err)

	assert.JSONEq(t, `{
		"type": "struct",
		"fields": [
			{"id": 1, "name": "foo", "type": "string", "required": false},
			{"id": 2, "name": "bar", "type": "int", "required": true},
			{"id": 3, "name": "baz", "type": "boolean", "required": false}
		],
		"schema-id": 1,
		"identifier-field-ids": [2]
	}`, string(data))
}

func TestUnmarshalSchema(t *testing.T) {
	var schema icegopher.Schema
	require.NoError(t, json.Unmarshal([]byte(`{
		"type": "struct",
		"fields": [
			{"id": 1, "name": "foo", "type": "string", "required": false},
			{"id": 2, "name": "bar", "type": "int", "required": true},
			{"id": 3, "name": "baz", "type": "boolean", "required": false}
		],
		"schema-id": 1,
		"identifier-field-ids": [2]
	}`), &schema))

	assert.True(t, tableSchemaSimple.Equals(&schema))
}

func TestPruneColumnsString(t *testing.T) {
	sc, err := icegopher.PruneColumns(tableSchemaNested, map[int]struct{}{1: {}}, false)
	require.NoError(t, err)

	assert.True(t, sc.Equals(icegopher.NewSchemaWithIdentifiers(1, []int{1},
		icegopher.NestedField{ID: 1, Name: "foo", Type: icegopher.PrimitiveTypes.String, Required: false})))
}

func TestPruneColumnsStringFull(t *testing.T) {
	sc, err := icegopher.PruneColumns(tableSchemaNested, map[int]struct{}{1: {}}, true)
	require.NoError(t, err)

	assert.True(t, sc.Equals(icegopher.NewSchemaWithIdentifiers(1, []int{1},
		icegopher.NestedField{ID: 1, Name: "foo", Type: icegopher.PrimitiveTypes.String, Required: false})))
}

func TestPruneColumnsList(t *testing.T) {
	sc, err := icegopher.PruneColumns(tableSchemaNested, map[int]struct{}{5: {}}, false)
	require.NoError(t, err)

	assert.True(t, sc.Equals(icegopher.NewSchema(1,
		icegopher.NestedField{ID: 4, Name: "qux", Required: true, Type: &icegopher.ListType{
			ElementID: 5, Element: icegopher.PrimitiveTypes.String, ElementRequired: true,
		}})))
}

func TestPruneColumnsListItself(t *testing.T) {
	_, err := icegopher.PruneColumns(tableSchemaNested, map[int]struct{}{4: {}}, false)
	assert.ErrorIs(t, err, icegopher.ErrInvalidSchema)

	assert.ErrorContains(t, err, "cannot explicitly project List or Map types, 4:qux of type list<string> was selected")
}

func TestPruneColumnsListFull(t *testing.T) {
	sc, err := icegopher.PruneColumns(tableSchemaNested, map[int]struct{}{5: {}}, true)
	require.NoError(t, err)

	assert.True(t, sc.Equals(icegopher.NewSchema(1,
		icegopher.NestedField{ID: 4, Name: "qux", Required: true, Type: &icegopher.ListType{
			ElementID: 5, Element: icegopher.PrimitiveTypes.String, ElementRequired: true,
		}})))
}

func TestPruneColumnsMap(t *testing.T) {
	sc, err := icegopher.PruneColumns(tableSchemaNested, map[int]struct{}{9: {}}, false)
	require.NoError(t, err)

	assert.True(t, sc.Equals(icegopher.NewSchema(1,
		icegopher.NestedField{
			ID:       6,
			Name:     "quux",
			Required: true,
			Type: &icegopher.MapType{
				KeyID:   7,
				KeyType: icegopher.PrimitiveTypes.String,
				ValueID: 8,
				ValueType: &icegopher.MapType{
					KeyID:         9,
					KeyType:       icegopher.PrimitiveTypes.String,
					ValueID:       10,
					ValueType:     icegopher.PrimitiveTypes.Int32,
					ValueRequired: true,
				},
				ValueRequired: true,
			},
		})))
}

func TestPruneColumnsMapItself(t *testing.T) {
	_, err := icegopher.PruneColumns(tableSchemaNested, map[int]struct{}{6: {}}, false)
	assert.ErrorIs(t, err, icegopher.ErrInvalidSchema)
	assert.ErrorContains(t, err, "cannot explicitly project List or Map types, 6:quux of type map<string, map<string, int>> was selected")
}

func TestPruneColumnsMapFull(t *testing.T) {
	sc, err := icegopher.PruneColumns(tableSchemaNested, map[int]struct{}{9: {}}, true)
	require.NoError(t, err)

	assert.True(t, sc.Equals(icegopher.NewSchema(1,
		icegopher.NestedField{
			ID:       6,
			Name:     "quux",
			Required: true,
			Type: &icegopher.MapType{
				KeyID:   7,
				KeyType: icegopher.PrimitiveTypes.String,
				ValueID: 8,
				ValueType: &icegopher.MapType{
					KeyID:         9,
					KeyType:       icegopher.PrimitiveTypes.String,
					ValueID:       10,
					ValueType:     icegopher.PrimitiveTypes.Int32,
					ValueRequired: true,
				},
				ValueRequired: true,
			},
		})))
}

func TestPruneColumnsMapKey(t *testing.T) {
	sc, err := icegopher.PruneColumns(tableSchemaNested, map[int]struct{}{10: {}}, false)
	require.NoError(t, err)

	assert.True(t, sc.Equals(icegopher.NewSchema(1,
		icegopher.NestedField{
			ID:       6,
			Name:     "quux",
			Required: true,
			Type: &icegopher.MapType{
				KeyID:   7,
				KeyType: icegopher.PrimitiveTypes.String,
				ValueID: 8,
				ValueType: &icegopher.MapType{
					KeyID:         9,
					KeyType:       icegopher.PrimitiveTypes.String,
					ValueID:       10,
					ValueType:     icegopher.PrimitiveTypes.Int32,
					ValueRequired: true,
				},
				ValueRequired: true,
			},
		})))
}

func TestPruneColumnsStruct(t *testing.T) {
	sc, err := icegopher.PruneColumns(tableSchemaNested, map[int]struct{}{16: {}}, false)
	require.NoError(t, err)

	assert.True(t, sc.Equals(icegopher.NewSchema(1,
		icegopher.NestedField{
			ID:       15,
			Name:     "person",
			Required: false,
			Type: &icegopher.StructType{
				Fields: []icegopher.NestedField{{
					ID: 16, Name: "name", Type: icegopher.PrimitiveTypes.String, Required: false,
				}},
			},
		})))
}

func TestPruneColumnsStructFull(t *testing.T) {
	sc, err := icegopher.PruneColumns(tableSchemaNested, map[int]struct{}{16: {}}, true)
	require.NoError(t, err)

	assert.True(t, sc.Equals(icegopher.NewSchema(1,
		icegopher.NestedField{
			ID:       15,
			Name:     "person",
			Required: false,
			Type: &icegopher.StructType{
				Fields: []icegopher.NestedField{{
					ID: 16, Name: "name", Type: icegopher.PrimitiveTypes.String, Required: false,
				}},
			},
		})))
}

func TestPruneColumnsEmptyStruct(t *testing.T) {
	schemaEmptyStruct := icegopher.NewSchema(0, icegopher.NestedField{
		ID: 15, Name: "person", Type: &icegopher.StructType{}, Required: false,
	})

	sc, err := icegopher.PruneColumns(schemaEmptyStruct, map[int]struct{}{15: {}}, false)
	require.NoError(t, err)

	assert.True(t, sc.Equals(icegopher.NewSchema(0,
		icegopher.NestedField{
			ID: 15, Name: "person", Type: &icegopher.StructType{}, Required: false})))
}

func TestPruneColumnsEmptyStructFull(t *testing.T) {
	schemaEmptyStruct := icegopher.NewSchema(0, icegopher.NestedField{
		ID: 15, Name: "person", Type: &icegopher.StructType{}, Required: false,
	})

	sc, err := icegopher.PruneColumns(schemaEmptyStruct, map[int]struct{}{15: {}}, true)
	require.NoError(t, err)

	assert.True(t, sc.Equals(icegopher.NewSchema(0,
		icegopher.NestedField{
			ID: 15, Name: "person", Type: &icegopher.StructType{}, Required: false})))
}

func TestPruneColumnsStructInMap(t *testing.T) {
	nestedSchema := icegopher.NewSchemaWithIdentifiers(1, []int{1},
		icegopher.NestedField{
			ID:       6,
			Name:     "id_to_person",
			Required: true,
			Type: &icegopher.MapType{
				KeyID:   7,
				KeyType: icegopher.PrimitiveTypes.Int32,
				ValueID: 8,
				ValueType: &icegopher.StructType{
					Fields: []icegopher.NestedField{
						{ID: 10, Name: "name", Type: icegopher.PrimitiveTypes.String},
						{ID: 11, Name: "age", Type: icegopher.PrimitiveTypes.Int32, Required: true},
					},
				},
				ValueRequired: true,
			},
		})

	sc, err := icegopher.PruneColumns(nestedSchema, map[int]struct{}{11: {}}, false)
	require.NoError(t, err)

	expected := icegopher.NewSchema(1,
		icegopher.NestedField{
			ID:       6,
			Name:     "id_to_person",
			Required: true,
			Type: &icegopher.MapType{
				KeyID:   7,
				KeyType: icegopher.PrimitiveTypes.Int32,
				ValueID: 8,
				ValueType: &icegopher.StructType{
					Fields: []icegopher.NestedField{
						{ID: 11, Name: "age", Type: icegopher.PrimitiveTypes.Int32, Required: true},
					},
				},
				ValueRequired: true,
			},
		})

	assert.Truef(t, sc.Equals(expected), "expected: %s\ngot: %s", expected, sc)
}

func TestPruneColumnsStructInMapFull(t *testing.T) {
	nestedSchema := icegopher.NewSchemaWithIdentifiers(1, []int{1},
		icegopher.NestedField{
			ID:       6,
			Name:     "id_to_person",
			Required: true,
			Type: &icegopher.MapType{
				KeyID:   7,
				KeyType: icegopher.PrimitiveTypes.Int32,
				ValueID: 8,
				ValueType: &icegopher.StructType{
					Fields: []icegopher.NestedField{
						{ID: 10, Name: "name", Type: icegopher.PrimitiveTypes.String},
						{ID: 11, Name: "age", Type: icegopher.PrimitiveTypes.Int32, Required: true},
					},
				},
				ValueRequired: true,
			},
		})

	sc, err := icegopher.PruneColumns(nestedSchema, map[int]struct{}{11: {}}, true)
	require.NoError(t, err)

	expected := icegopher.NewSchema(1,
		icegopher.NestedField{
			ID:       6,
			Name:     "id_to_person",
			Required: true,
			Type: &icegopher.MapType{
				KeyID:   7,
				KeyType: icegopher.PrimitiveTypes.Int32,
				ValueID: 8,
				ValueType: &icegopher.StructType{
					Fields: []icegopher.NestedField{
						{ID: 11, Name: "age", Type: icegopher.PrimitiveTypes.Int32, Required: true},
					},
				},
				ValueRequired: true,
			},
		})

	assert.Truef(t, sc.Equals(expected), "expected: %s\ngot: %s", expected, sc)
}

func TestPruneColumnsSelectOriginalSchema(t *testing.T) {
	id := tableSchemaNested.HighestFieldID()
	selected := make(map[int]struct{})
	for i := 0; i < id; i++ {
		selected[i] = struct{}{}
	}

	sc, err := icegopher.PruneColumns(tableSchemaNested, selected, true)
	require.NoError(t, err)

	assert.True(t, sc.Equals(tableSchemaNested))
}
