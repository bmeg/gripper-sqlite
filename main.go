package main

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	//"database/sql"
	"flag"
	"fmt"
	"github.com/bmeg/grip/gripper"
	_ "github.com/mattn/go-sqlite3"
	//"database/sql/driver"
	"github.com/jmoiron/sqlx"

	"github.com/hashicorp/go-plugin"
)

type Sqlite3Server struct {
	db *sqlx.DB
}

type PrimaryKeyDriver struct {
	db         *sqlx.DB
	tableName  string
	primaryKey string
	fields     []string
}

func (pk *PrimaryKeyDriver) GetTimeout() int {
	return -1
}

func (pk *PrimaryKeyDriver) GetFields() ([]string, error) {
	return pk.fields, nil
}

func (pk *PrimaryKeyDriver) GetFieldLinks() (map[string]string, error) {
	return map[string]string{}, nil
}

func newBaseRow(primaryKey string, data map[string]interface{}) (*gripper.BaseRow, error) {
	if id, ok := data[primaryKey]; ok {
		if idStr, ok := id.(string); ok {
			return &gripper.BaseRow{idStr, data}, nil
		} else if idArr, ok := id.([]byte); ok {
			return &gripper.BaseRow{string(idArr), data}, nil
		} else if idInt, ok := id.(int64); ok {
			idStr := strconv.FormatInt(idInt, 10)
			return &gripper.BaseRow{idStr, data}, nil
		} else {
			return nil, fmt.Errorf("Primary key %#v (%T) not a string: %s", id, id, id)
		}
	}
	return nil, fmt.Errorf("Primary key not found")
}

func (pk *PrimaryKeyDriver) FetchRow(id string) (*gripper.BaseRow, error) {
	var err error
	var out *gripper.BaseRow
	//idInt, _ := strconv.Atoi(id)
	if rows, rerr := pk.db.Queryx(fmt.Sprintf("select * from %s where %s = ? LIMIT 1", pk.tableName, pk.primaryKey), id); rerr == nil {
		for rows.Next() {
			d := map[string]interface{}{}
			if serr := rows.MapScan(d); serr == nil {
				out, err = newBaseRow(pk.primaryKey, d)
			} else {
				log.Printf("Map scanning error: %s", serr)
				err = serr
			}
		}
	} else {
		log.Printf("Error fetching row: %s", rerr)
		err = rerr
	}
	if out == nil {
		log.Printf("Row not found")
		return nil, fmt.Errorf("Row not found")
	}
	return out, err
}

func (pk *PrimaryKeyDriver) FetchRows(ctx context.Context) (chan *gripper.BaseRow, error) {
	out := make(chan *gripper.BaseRow, 10)
	go func() {
		defer close(out)
		if rows, err := pk.db.QueryxContext(ctx, fmt.Sprintf("select * from %s", pk.tableName)); err == nil {
			for rows.Next() {
				d := map[string]interface{}{}
				if err := rows.MapScan(d); err == nil {
					if r, err := newBaseRow(pk.primaryKey, d); err == nil {
						out <- r
					} else {
						log.Printf("Data error: %s", err)
					}
				}
			}
		} else {
			log.Printf("Scanning error: %s", err)
		}
	}()
	return out, nil
}

func (pk *PrimaryKeyDriver) FetchMatchRows(ctx context.Context, field string, value string) (chan *gripper.BaseRow, error) {
	out := make(chan *gripper.BaseRow, 10)
	go func() {
		defer close(out)
		if rows, err := pk.db.Queryx(fmt.Sprintf("select * from %s where %s = ?", pk.tableName, field), value); err == nil {
			for rows.Next() {
				d := make(map[string]interface{})
				slice, _ := rows.SliceScan()
				log.Printf("slice: %s", slice)
				if err := rows.MapScan(d); err == nil {
					if r, err := newBaseRow(pk.primaryKey, d); err == nil {
						out <- r
					} else {
						log.Printf("Data error: %s", err)
					}
				} else {
					//log.Printf("Scanning error: %s", err)
				}
			}
		}
	}()
	return out, nil
}

type EdgeTableDriver struct {
	db          *sqlx.DB
	tableName   string
	source      string
	dest        string
	delim       string
	sourceField string
	destField   string
}

func (et *EdgeTableDriver) GetTimeout() int {
	return -1
}

func (et *EdgeTableDriver) GetFields() ([]string, error) {
	return []string{et.sourceField, et.destField}, nil
}

func (et *EdgeTableDriver) GetFieldLinks() (map[string]string, error) {
	return map[string]string{et.sourceField: et.source, et.destField: et.dest}, nil
}

func (et *EdgeTableDriver) FetchRow(id string) (*gripper.BaseRow, error) {
	var err error
	var out *gripper.BaseRow
	tmp := strings.Split(id, et.delim)
	if len(tmp) < 2 {
		return nil, fmt.Errorf("Unable to parse key")
	}
	src := tmp[0]
	dst := tmp[1]
	//idInt, _ := strconv.Atoi(id)
	if rows, rerr := et.db.Queryx(
		fmt.Sprintf("select * from %s where %s = ? and %s = ? LIMIT 1", et.tableName, et.sourceField, et.destField), src, dst); rerr == nil {
		for rows.Next() {
			d := map[string]interface{}{}
			if serr := rows.MapScan(d); serr == nil {
				out, err = et.newEdgeRow(d)
			} else {
				log.Printf("Map scanning error: %s", serr)
				err = serr
			}
		}
	} else {
		log.Printf("Error fetching row: %s", rerr)
		err = rerr
	}
	if out == nil {
		log.Printf("Row not found")
		return nil, fmt.Errorf("Row not found")
	}
	return out, err
}

func (et *EdgeTableDriver) newEdgeRow(data map[string]interface{}) (*gripper.BaseRow, error) {
	srcStr := ""
	if id, ok := data[et.sourceField]; ok {
		if idStr, ok := id.(string); ok {
			srcStr = idStr
		} else if idArr, ok := id.([]byte); ok {
			srcStr = string(idArr)
		} else if idInt, ok := id.(int64); ok {
			srcStr = strconv.FormatInt(idInt, 10)
		} else {
			return nil, fmt.Errorf("Source key %s %#v (%T) not a string: %s", et.sourceField, id, id, id)
		}
	}
	dstStr := ""
	if id, ok := data[et.destField]; ok {
		if idStr, ok := id.(string); ok {
			dstStr = idStr
		} else if idArr, ok := id.([]byte); ok {
			dstStr = string(idArr)
		} else if idInt, ok := id.(int64); ok {
			dstStr = strconv.FormatInt(idInt, 10)
		} else {
			return nil, fmt.Errorf("Source key %#v (%T) not a string: %s", id, id, id)
		}
	}
	idStr := srcStr + ":" + dstStr
	return &gripper.BaseRow{idStr, map[string]interface{}{et.sourceField: srcStr, et.destField: dstStr}}, nil
}

func (et *EdgeTableDriver) FetchRows(ctx context.Context) (chan *gripper.BaseRow, error) {
	out := make(chan *gripper.BaseRow, 10)
	go func() {
		defer close(out)
		if rows, err := et.db.QueryxContext(ctx, fmt.Sprintf("select * from %s", et.tableName)); err == nil {
			for rows.Next() {
				d := map[string]interface{}{}
				if err := rows.MapScan(d); err == nil {
					if r, err := et.newEdgeRow(d); err == nil {
						out <- r
					} else {
						log.Printf("Data error: %s", err)
					}
				}
			}
		} else {
			log.Printf("Scanning error: %s", err)
		}
	}()
	return out, nil
}

func (et *EdgeTableDriver) FetchMatchRows(ctx context.Context, field string, value string) (chan *gripper.BaseRow, error) {
	out := make(chan *gripper.BaseRow, 10)
	go func() {
		defer close(out)
		if rows, err := et.db.Queryx(fmt.Sprintf("select * from %s where %s = ?", et.tableName, field), value); err == nil {
			for rows.Next() {
				d := make(map[string]interface{})
				slice, _ := rows.SliceScan()
				log.Printf("slice: %s", slice)
				if err := rows.MapScan(d); err == nil {
					if r, err := et.newEdgeRow(d); err == nil {
						out <- r
					}
				} else {
					//log.Printf("Scanning error: %s", err)
				}
			}
		}
	}()
	return out, nil
}

func OpenSqlite(path string) (*Sqlite3Server, error) {
	db, err := sqlx.Open("sqlite3", path)
	if err != nil {
		return nil, err
	}
	return &Sqlite3Server{db}, nil
}

func (a *Sqlite3Server) TableSetup() (map[string]gripper.Driver, error) {

	tables, err := a.listTables()
	if err != nil {
		return nil, err
	}

	primaryKeys := map[string]string{}
	fields := map[string][]string{}

	for _, t := range tables {
		log.Printf("Scanning Table: %s\n", t)
		rows, err := a.db.Query(fmt.Sprintf("PRAGMA table_info(%s);", t))
		if err != nil {
			log.Printf("Err: %s\n", err)
			return nil, err
		}
		fields[t] = []string{}
		for rows.Next() {
			var cid, name, cType, notNull string
			var pk bool
			var dfltValue interface{}
			err := rows.Scan(&cid, &name, &cType, &notNull, &dfltValue, &pk)
			if err != nil {
				return nil, err
			}
			if pk {
				//log.Printf("Table Key: %s - %s\n", t, name)
				primaryKeys[t] = name
			}
			fields[t] = append(fields[t], name)
		}
	}

	linkKeys := map[string]map[string]string{}
	for _, t := range tables {
		linkKeys[t] = map[string]string{}
		rows, err := a.db.Query(fmt.Sprintf("PRAGMA foreign_key_list(%s);", t))
		if err != nil {
			log.Printf("Err: %s\n", err)
			return nil, err
		}
		for rows.Next() {
			var cid, seq, table, from, to, onUpdate, onDelete, match string
			err := rows.Scan(&cid, &seq, &table, &from, &to, &onUpdate, &onDelete, &match)
			if err != nil {
				log.Printf("foreign key scan error: %s", err)
				return nil, err
			}
			//log.Printf("foreign key: %s:%s -> %s:%s\n", t, from, table, to)
			if dest, ok := primaryKeys[table]; ok {
				if dest == to {
					linkKeys[t][from] = table
				} else {
					log.Printf("Dest field not primary key: %s != %s", dest, to)
				}
			} else {
				log.Printf("Dest table not found: %s", table)
			}
		}
	}

	out := map[string]gripper.Driver{}
	for table, key := range primaryKeys {
		out[table] = &PrimaryKeyDriver{
			db:         a.db,
			tableName:  table,
			primaryKey: key,
			fields:     fields[table],
		}
	}

	for table, links := range linkKeys {
		for field, dst := range links {
			d := &EdgeTableDriver{
				db:          a.db,
				tableName:   table,
				source:      table,
				dest:        dst,
				delim:       ":",
				sourceField: primaryKeys[table],
				destField:   field,
			}
			log.Printf("Adding edge table %#v", *d)
			out[table+":"+field] = d
		}
	}
	return out, nil
}

func (a *Sqlite3Server) listTables() ([]string, error) {
	rows, err := a.db.Query("SELECT name FROM sqlite_master WHERE type IN ('table','view') AND name NOT LIKE 'sqlite_%' ORDER BY 1;")
	if err != nil {
		log.Printf("Error scanning tables: %s", err)
		return []string{}, err
	}
	log.Printf("Searching tables\n")
	out := []string{}
	for rows.Next() {
		var name string
		rows.Scan(&name)
		log.Printf("Found: %s\n", name)
		out = append(out, name)
	}
	rows.Close()
	return out, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func mapZip(names []string, values []string) map[string]string {
	log.Printf("%s - %s\n", names, values)
	o := map[string]string{}
	for i := 0; i < min(len(names), len(values)); i++ {
		o[names[i]] = values[i]
	}
	return o
}

func main() {
	flag.Parse()
	configPath := flag.Args()[0]

	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		return
	}

	config := map[string]string{}
	err = json.Unmarshal(data, &config)
	if err != nil {
		return
	}

	var sqlitePath string
	if t, ok := config["path"]; !ok {
		log.Printf("No path found")
		return
	} else {
		sqlitePath = t
	}

	db, err := OpenSqlite(sqlitePath)
	if err != nil {
		log.Printf("Error opening file: %s", err)
		return
	}

	drivers, err := db.TableSetup()
	if err != nil {
		log.Printf("Error opening file: %s", err)
		return
	}

	srv := gripper.NewSimpleTableServer(drivers)
	plugin.Serve(&plugin.ServeConfig{
		HandshakeConfig: gripper.Handshake,
		Plugins: map[string]plugin.Plugin{
			"gripper": &gripper.GripPlugin{Impl: srv},
		},
		GRPCServer: plugin.DefaultGRPCServer,
	})
}
