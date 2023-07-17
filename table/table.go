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
	"reflect"

	"github.com/zeroshade/icegopher"
	"github.com/zeroshade/icegopher/io"

	"golang.org/x/exp/slices"
)

func New(ident Identifier, meta Metadata, location string, fs io.IO, catalog Catalog) *Table {
	return &Table{
		identifier:       ident,
		metadata:         meta,
		metadataLocation: location,
		fs:               fs,
		catalog:          catalog,
	}
}

func NewFromLocation(ident Identifier, metalocation string, fsys io.IO, catalog Catalog) (*Table, error) {
	var meta Metadata

	if rf, ok := fsys.(io.ReadFileIO); ok {
		data, err := rf.ReadFile(metalocation)
		if err != nil {
			return nil, err
		}

		if meta, err = ParseMetadataBytes(data); err != nil {
			return nil, err
		}
	} else {
		f, err := fsys.Open(metalocation)
		if err != nil {
			return nil, err
		}
		defer f.Close()

		if meta, err = ParseMetadata(f); err != nil {
			return nil, err
		}
	}
	return New(ident, meta, metalocation, fsys, catalog), nil
}

type Table struct {
	identifier       Identifier
	metadata         Metadata
	metadataLocation string
	fs               io.IO
	catalog          Catalog
}

func (t Table) Equals(other Table) bool {
	return slices.Equal(t.identifier, other.identifier) &&
		t.metadataLocation == other.metadataLocation &&
		reflect.DeepEqual(t.metadata, other.metadata)
}

func (t Table) Identifier() Identifier { return t.identifier }
func (t Table) Metadata() Metadata     { return t.metadata }
func (t Table) MetadataLoc() string    { return t.metadataLocation }
func (t Table) FS() io.IO              { return t.fs }
func (t Table) Catalog() Catalog       { return t.catalog }

func (t Table) Schema() *icegopher.Schema            { return t.metadata.CurrentSchema() }
func (t Table) Spec() icegopher.PartitionSpec        { return t.metadata.PartitionSpec() }
func (t Table) SortOrder() SortOrder                 { return t.metadata.SortOrder() }
func (t Table) Properties() icegopher.Properties     { return t.metadata.Properties() }
func (t Table) Location() string                     { return t.metadata.Loc() }
func (t Table) CurrentSnapshot() *Snapshot           { return t.metadata.CurrentSnapshot() }
func (t Table) SnapshotByID(id int64) *Snapshot      { return t.metadata.SnapshotByID(id) }
func (t Table) SnapshotByName(name string) *Snapshot { return t.metadata.SnapshotByName(name) }
func (t Table) Schemas() map[int]*icegopher.Schema {
	m := make(map[int]*icegopher.Schema)
	for _, s := range t.metadata.Schemas() {
		m[s.ID] = s
	}
	return m
}

func (t *Table) Refresh() (*Table, error) {
	fresh, err := t.catalog.LoadTable(t.identifier[1:]...)
	if err != nil {
		return nil, err
	}

	*t = *fresh
	return fresh, nil
}

type Identifier = []string
type PropertiesUpdateSummary struct {
	Removed []string `json:"removed"`
	Updated []string `json:"updated"`
	Missing []string `json:"missing"`
}

type Catalog interface {
	LoadTable(...string) (*Table, error)
	DropTable(...string) error
	RenameTable(from, to Identifier) (*Table, error)
	CreateNamespace(props icegopher.Properties, namespace ...string) error
	DropNamespace(...string) error
	ListTables(namespace ...string) ([]Identifier, error)
	ListNamespaces(...string) ([]Identifier, error)
	LoadNamespaceProperties(...string) (icegopher.Properties, error)
	UpdateNamespaceProperties(namespace Identifier, removals []string, updates icegopher.Properties) (PropertiesUpdateSummary, error)
}
