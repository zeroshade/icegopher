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

package scanner

import (
	"fmt"
	"sort"

	"github.com/zeroshade/icegopher"
	"github.com/zeroshade/icegopher/expressions"
	"github.com/zeroshade/icegopher/io"
	"github.com/zeroshade/icegopher/table"

	"golang.org/x/exp/slices"
)

type TableScan interface {
	PlanFiles() ([]ScanTask, error)
}

type tableScan struct {
	table          *table.Table
	selectedFields []string
	caseSensitive  bool
	snapshotID     *int64
	options        icegopher.Properties
	limit          *int

	rowFilter expressions.BooleanExpression
}

func (t tableScan) Snapshot() *table.Snapshot {
	if t.snapshotID != nil {
		return t.table.SnapshotByID(*t.snapshotID)
	}
	return t.table.CurrentSnapshot()
}

func (t tableScan) Projection() (*icegopher.Schema, error) {
	snapshotSchema := t.table.Schema()
	if snap := t.Snapshot(); snap != nil {
		if schemaID := snap.SchemaID; schemaID != nil {
			snapshotSchema = t.table.Schemas()[*schemaID]
		}
	}

	if slices.Contains(t.selectedFields, "*") {
		return snapshotSchema, nil
	}

	return snapshotSchema.Select(t.caseSensitive, t.selectedFields...)
}

func (t tableScan) UseRef(name string) (tableScan, error) {
	if t.snapshotID != nil {
		return t, fmt.Errorf("%w: cannot override ref, already set snapshot id=%d", icegopher.ErrInvalidArgument, *t.snapshotID)
	}

	if sn := t.table.SnapshotByName(name); sn != nil {
		t.snapshotID = &sn.SnapshotID
		return t, nil
	}

	return t, fmt.Errorf("%w: cannot scan unknown ref=%s", icegopher.ErrInvalidArgument, name)
}

func (t tableScan) WithCaseSensitive(val bool) tableScan {
	t.caseSensitive = val
	return t
}

type ScanTask interface {
	SizeBytes() int64
	EstimatedRowsCount() int64
	FilesCount() int
}

type FileScanTask struct {
	File        icegopher.DataFile
	DeleteFiles []icegopher.DataFile

	Start, Length int64
}

func NewFileScanTask(data icegopher.DataFile, deletes []icegopher.DataFile, start, length int64) FileScanTask {
	if length <= 0 {
		length = data.FileSizeBytes()
	}

	if start <= 0 {
		start = 0
	}

	return FileScanTask{File: data, DeleteFiles: deletes, Start: start, Length: length}
}

func (f FileScanTask) SizeBytes() int64 {
	length := f.Length
	for _, df := range f.DeleteFiles {
		length += df.FileSizeBytes()
	}
	return length
}

func (f FileScanTask) FilesCount() int {
	return 1 + len(f.DeleteFiles)
}

func openManifest(iofs io.IO, manifest icegopher.ManifestFile, partitionFilter, metricsEval func(icegopher.DataFile) bool) ([]icegopher.ManifestEntry, error) {
	entries, err := manifest.FetchEntries(iofs, true)
	if err != nil {
		return nil, err
	}

	result := make([]icegopher.ManifestEntry, 0)
	for _, entry := range entries {
		if partitionFilter != nil && !partitionFilter(entry.DataFile()) {
			continue
		}
		if metricsEval != nil && !metricsEval(entry.DataFile()) {
			continue
		}
		result = append(result, entry)
	}

	return result, nil
}

func minDataFileSeqNum(manifests []icegopher.ManifestFile) int64 {
	if len(manifests) == 0 {
		return 0
	}

	out := int64(0)
	for _, m := range manifests {
		if m.ManifestContent() != icegopher.ManifestContentData {
			continue
		}

		if m.MinSequenceNum() < out {
			out = m.MinSequenceNum()
		}
	}
	return out
}

func matchDeletesToDataFile(data icegopher.ManifestEntry, sortedPositionDeleteEntries []icegopher.ManifestEntry) ([]icegopher.DataFile, error) {
	find := data.SequenceNum()

	idx := sort.Search(len(sortedPositionDeleteEntries), func(i int) bool {
		n := sortedPositionDeleteEntries[i].SequenceNum()
		return find <= n+1
	})

	relevant := sortedPositionDeleteEntries[idx:]
	if len(relevant) == 0 {
		return []icegopher.DataFile{}, nil
	}

	eval, err := expressions.NewInclusiveMetricsEvaluator(icegopher.PositionalDeleteSchema,
		expressions.EqualTo("file_path", data.DataFile().FilePath()), false)
	if err != nil {
		return nil, err
	}

	result := make([]icegopher.DataFile, 0)
	for _, entry := range relevant {
		r, err := eval.Eval(entry.DataFile())
		if err != nil {
			return nil, err
		}
		if r {
			result = append(result, entry.DataFile())
		}
	}

	return result, nil
}
