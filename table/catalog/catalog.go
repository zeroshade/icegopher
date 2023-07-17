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

package catalog

import (
	"errors"
	"fmt"
	"strings"

	"github.com/zeroshade/icegopher"
	"github.com/zeroshade/icegopher/table"
)

const (
	defaultCatalogName = "default-catalog"

	KeyToken             = "token"
	KeyType              = "type"
	KeyIceberg           = "iceberg"
	KeyWarehouseLocation = "warehouse"
	KeyMetadataLocation  = "metadata_location"
	KeyCredential        = "credential"
)

type CatalogType string

const (
	REST     CatalogType = "rest"
	HIVE     CatalogType = "hive"
	GLUE     CatalogType = "glue"
	DYNAMODB CatalogType = "dynamodb"
)

func inferCatalogType(name string, uri string, props icegopher.Properties) (CatalogType, error) {
	if uri == "" {
		return "", errors.New("URI missing, please provide using --uri")
	}

	switch {
	case strings.HasPrefix(uri, "http"):
		return REST, nil
	case strings.HasPrefix(uri, "thrift"):
		return HIVE, nil
	default:
		return "", fmt.Errorf("could not infer the catalog type from the uri: %s", uri)
	}
}

var (
	ErrNoSuchNamespace        = errors.New("[Catalog] no such namespace")
	ErrNamespaceAlreadyExists = errors.New("[Catalog] namespace already exists")
	ErrRESTError              = errors.New("[Catalog] REST error")
	ErrNoSuchTable            = errors.New("[Catalog] no such table")
)

func ToIdentifier(ident ...string) table.Identifier {
	if len(ident) == 1 {
		if ident[0] == "" {
			return nil
		}
		return table.Identifier(strings.Split(ident[0], "."))
	}

	return table.Identifier(ident)
}

func TableNameFromIdent(ident table.Identifier) string {
	if len(ident) == 0 {
		return ""
	}

	return ident[len(ident)-1]
}

func NamespaceFromIdent(ident table.Identifier) table.Identifier {
	return ident[:len(ident)-1]
}

type catalog struct {
	name  string
	props icegopher.Properties
}

func LoadCatalog(name string, uri string, props icegopher.Properties) (table.Catalog, error) {
	if name == "" {
		name = defaultCatalogName
	}

	catalogType, ok := props[KeyType]
	if !ok {
		typ, err := inferCatalogType(name, uri, props)
		if err != nil {
			return nil, err
		}
		catalogType = string(typ)
	}

	switch CatalogType(strings.ToLower(catalogType)) {
	case REST:
		return NewRestCatalog(name, uri, props)
	case HIVE:
		fallthrough
	case DYNAMODB:
		fallthrough
	case GLUE:
		fallthrough
	default:
		return nil, fmt.Errorf("%w: unimplemented catalog type %s", icegopher.ErrNotImplemented, catalogType)
	}
}
