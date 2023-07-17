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

package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/zeroshade/icegopher"
	"github.com/zeroshade/icegopher/table"
	"github.com/zeroshade/icegopher/table/catalog"

	"github.com/docopt/docopt-go"
)

const usage = `icegopher.

Usage:
  icegopher list [options] [PARENT]
  icegopher location [options] TABLE_ID
  icegopher describe [options] [namespace | table] IDENTIFIER
  icegopher drop [options] (namespace | table) IDENTIFIER
  icegopher properties [options] get (namespace | table) IDENTIFIER [PROPNAME]
  icegopher properties [options] set (namespace | table) IDENTIFIER PROPNAME VALUE
  icegopher properties [options] remove (namespace | table) IDENTIFIER PROPNAME
  icegopher rename [options]  <from> <to>
  icegopher schema [options] TABLE_ID
  icegopher spec [options] TABLE_ID
  icegopher uuid [options] TABLE_ID
  icegopher files [options] TABLE_ID [--history]
  icegopher -h | --help | --version  

Arguments:
  PARENT      Catalog parent namespace
  IDENTIFIER  fully qualified namespace or table
  TABLE_ID    full path to table
  PROPNAME    name of a property
  VALUE       value to set

Options:
  -h --help          show this help message and exit
  --verbose          
  --catalog TEXT     specify catalog type [default: rest]
  --output TYPE      output type (json/text) [default: text]
  --uri TEXT         specify the catalog URI
  --credential TEXT  specify credentials for the catalog`

func main() {
	args, err := docopt.ParseArgs(usage, os.Args[1:], icegopher.Version)
	if err != nil {
		log.Fatal(err)
	}

	cfg := struct {
		List     bool `docopt:"list"`
		Location bool `docopt:"location"`
		Describe bool `docopt:"describe"`
		Drop     bool `docopt:"drop"`
		Props    bool `docopt:"properties"`
		Rename   bool `docopt:"rename"`
		Schema   bool `docopt:"schema"`
		Spec     bool `docopt:"spec"`
		Uuid     bool `docopt:"uuid"`
		Files    bool `docopt:"files"`

		Get    bool `docopt:"get"`
		Set    bool `docopt:"set"`
		Remove bool `docopt:"remove"`

		Namespace bool `docopt:"namespace"`
		Table     bool `docopt:"table"`

		RenameFrom string `docopt:"<from>"`
		RenameTo   string `docopt:"<to>"`

		TableID    string `docopt:"TABLE_ID"`
		Parent     string `docopt:"PARENT"`
		Identifier string `docopt:"IDENTIFIER"`
		PropName   string `docopt:"PROPNAME"`
		Value      string `docopt:"VALUE"`

		Catalog string `docopt:"--catalog"`
		Verbose bool   `docopt:"--verbose"`
		Output  string `docopt:"--output"`
		URI     string `docopt:"--uri"`
		Cred    string `docopt:"--credential"`
		History bool   `docopt:"--history"`
	}{}

	if err := args.Bind(&cfg); err != nil {
		log.Fatal(err)
	}

	var output Output
	switch strings.ToLower(cfg.Output) {
	case "text":
		output = text{}
	case "json":
	default:
		log.Fatal("output type must be either `text` or `json`")
	}

	catProps := icegopher.Properties{}
	if cfg.Cred != "" {
		catProps[catalog.KeyCredential] = cfg.Cred
	}

	if cfg.Catalog != "" {
		catProps[catalog.KeyType] = cfg.Catalog
	}

	cat, err := catalog.LoadCatalog("cli", cfg.URI, catProps)
	if err != nil {
		output.Error(err)
		os.Exit(1)
	}

	switch {
	case cfg.List:
		list(output, cat, cfg.Parent)
	case cfg.Describe:
		entityType := "any"
		if cfg.Namespace {
			entityType = "ns"
		} else if cfg.Table {
			entityType = "tbl"
		}

		describe(output, cat, cfg.Identifier, entityType)
	case cfg.Schema:
		tbl := loadTable(output, cat, cfg.TableID)
		output.Schema(tbl.Schema())
	case cfg.Spec:
		tbl := loadTable(output, cat, cfg.TableID)
		output.Spec(tbl.Spec())
	case cfg.Location:
		tbl := loadTable(output, cat, cfg.TableID)
		output.Text(tbl.Location())
	case cfg.Uuid:
		tbl := loadTable(output, cat, cfg.TableID)
		output.Uuid(tbl.Metadata().TableUUID())
	case cfg.Props:
		properties(output, cat, propCmd{
			get: cfg.Get, set: cfg.Set, remove: cfg.Remove,
			namespace: cfg.Namespace, table: cfg.Table,
			identifier: cfg.Identifier,
			propname:   cfg.PropName,
			value:      cfg.Value,
		})
	case cfg.Rename:
		_, err := cat.RenameTable(catalog.ToIdentifier(cfg.RenameFrom), catalog.ToIdentifier(cfg.RenameTo))
		if err != nil {
			output.Error(err)
			os.Exit(1)
		}

		output.Text("Renamed table from " + cfg.RenameFrom + " to " + cfg.RenameTo)
	case cfg.Drop:
		switch {
		case cfg.Namespace:
			err := cat.DropNamespace(cfg.Identifier)
			if err != nil {
				output.Error(err)
				os.Exit(1)
			}
		case cfg.Table:
			err := cat.DropTable(cfg.Identifier)
			if err != nil {
				output.Error(err)
				os.Exit(1)
			}
		}
	case cfg.Files:
		tbl := loadTable(output, cat, cfg.TableID)
		output.Files(tbl, cfg.History)
	}
}

func list(output Output, cat table.Catalog, parent string) {
	ids, err := cat.ListNamespaces(parent)
	if err != nil {
		output.Error(err)
		os.Exit(1)
	}

	if len(ids) == 0 && parent != "" {
		ids, err = cat.ListTables(parent)
		if err != nil {
			output.Error(err)
			os.Exit(1)
		}
	}
	output.Identifiers(ids)
}

func describe(output Output, cat table.Catalog, id string, entityType string) {
	ident := catalog.ToIdentifier(id)

	isNS, isTbl := false, false
	if (entityType == "any" || entityType == "ns") && len(ident) > 0 {
		nsprops, err := cat.LoadNamespaceProperties(ident...)
		if err != nil {
			if errors.Is(err, catalog.ErrNoSuchNamespace) {
				if entityType != "any" || len(ident) == 1 {
					output.Error(err)
					os.Exit(1)
				}
			} else {
				output.Error(err)
				os.Exit(1)
			}
		} else {
			isNS = true
			output.DescribeProperties(nsprops)
		}
	}

	if (entityType == "any" || entityType == "tbl") && len(ident) > 1 {
		tbl, err := cat.LoadTable(ident...)
		if err != nil {
			if !errors.Is(err, catalog.ErrNoSuchTable) || entityType != "any" {
				output.Error(err)
				os.Exit(1)
			}
		} else {
			isTbl = true
			output.DescribeTable(tbl)
		}
	}

	if !isNS && !isTbl {
		output.Error(fmt.Errorf("%w: table or namespace does not exist: %s",
			catalog.ErrNoSuchNamespace, ident))
	}
}

func loadTable(output Output, cat table.Catalog, id string) *table.Table {
	tbl, err := cat.LoadTable(id)
	if err != nil {
		output.Error(err)
		os.Exit(1)
	}

	return tbl
}

type propCmd struct {
	get, set, remove bool
	namespace, table bool

	identifier, propname, value string
}

func properties(output Output, cat table.Catalog, args propCmd) {
	switch {
	case args.get:
		var props icegopher.Properties
		switch {
		case args.namespace:
			var err error
			props, err = cat.LoadNamespaceProperties(args.identifier)
			if err != nil {
				output.Error(err)
				os.Exit(1)
			}
		case args.table:
			tbl := loadTable(output, cat, args.identifier)
			props = tbl.Metadata().Properties()
		}

		if args.propname == "" {
			output.DescribeProperties(props)
			return
		}

		if val, ok := props[args.propname]; ok {
			output.Text(val)
		} else {
			output.Error(errors.New("could not find property " + args.propname + " on namespace " + args.identifier))
			os.Exit(1)
		}
	case args.set:
		switch {
		case args.namespace:
			_, err := cat.UpdateNamespaceProperties(catalog.ToIdentifier(args.identifier),
				nil, icegopher.Properties{args.propname: args.value})
			if err != nil {
				output.Error(err)
				os.Exit(1)
			}

			output.Text("updated " + args.propname + " on " + args.identifier)
		case args.table:
			loadTable(output, cat, args.identifier)
			output.Text("Setting " + args.propname + "=" + args.value + " on " + args.identifier)
			output.Error(errors.New("not implemented: Writing is WIP"))
		}
	case args.remove:
		switch {
		case args.namespace:
			_, err := cat.UpdateNamespaceProperties(catalog.ToIdentifier(args.identifier),
				[]string{args.propname}, nil)
			if err != nil {
				output.Error(err)
				os.Exit(1)
			}

			output.Text("removing " + args.propname + " from " + args.identifier)
		case args.table:
			loadTable(output, cat, args.identifier)
			output.Text("Setting " + args.propname + "=" + args.value + " on " + args.identifier)
			output.Error(errors.New("not implemented: Writing is WIP"))
		}
	}
}
