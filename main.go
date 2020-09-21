package main

import (
	"fmt"
	"math"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/spf13/viper"
)

const PageLimit = 10000

type Total struct {
	Number int `db:"num"`
}

func main() {
	viper.SetConfigName("migrate")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	viper.AddConfigPath("./config/")
	err := viper.ReadInConfig()
	if err != nil {
		panic(err)
	}

	source := viper.GetStringMapString("source")
	target := viper.GetStringMapString("target")

	sourceDB := sqlx.MustConnect("mysql", source["url"])
	err = sourceDB.Ping()
	if err != nil {
		panic(err)
	}

	targetDB := sqlx.MustConnect("mysql", target["url"])
	err = targetDB.Ping()
	if err != nil {
		panic(err)
	}

	tables := viper.GetStringMap("tables")
	for table, fields := range tables {
		// Total
		pageRow := sourceDB.QueryRowx(fmt.Sprintf("SELECT COUNT(*) as num FROM %s", table))
		total := Total{}
		err = pageRow.StructScan(&total)
		if err != nil {
			fmt.Println(err)
			continue
		}
		if total.Number <= 0 {
			continue
		}

		// Page
		page := int(math.Ceil(float64(total.Number) / float64(PageLimit)))
		for p := 1; p <= page; p++ {
			fmt.Println("🦑", p, "/", page, table)
			offset := (p - 1) * PageLimit
			rows, err := sourceDB.Queryx(fmt.Sprintf("SELECT * FROM %s LIMIT %d, %d", table, offset, PageLimit))
			if err != nil {
				fmt.Println(err)
				continue
			}

			// sql
			fieldsNameFragment := ""
			fieldsValueFragment := ""
			fieldsWhereFragment := ""

			f := fields.([]interface{})
			for _, i := range f {
				if i == nil {
					continue
				}

				fieldsNameFragment += i.(string) + ","
				fieldsValueFragment += ":" + i.(string) + ","
				fieldsWhereFragment += " `" + i.(string) + "` = :" + i.(string) + " AND"
			}

			fieldsNameFragment = strings.TrimRight(fieldsNameFragment, ",")
			fieldsValueFragment = strings.TrimRight(fieldsValueFragment, ",")
			fieldsWhereFragment = strings.TrimRight(fieldsWhereFragment, "AND")

			// Foreach
			for rows.Next() {
				columns, _ := rows.Columns()
				values := make([]interface{}, len(columns))
				for i := range values {
					values[i] = new(interface{})
				}
				//var desc interface{}
				err := rows.Scan(values...)
				if err != nil {
					fmt.Println(err)
				}

				m := make(map[string]interface{})
				for i, col := range columns {
					val := values[i].(*interface{})
					m[col] = val
				}

				insertValue := make(map[string]interface{})
				f := fields.([]interface{})
				for _, i := range f {
					if i == nil {
						continue
					}
					if m[i.(string)] != nil {
						v := *m[i.(string)].(*interface{})
						insertValue[i.(string)] = v
					} else {
						insertValue[i.(string)] = nil
					}
				}

				// exist
				existSQL := "SELECT COUNT(*) as num FROM %s WHERE %s"
				existRows, err := targetDB.NamedQuery(fmt.Sprintf(existSQL, table, fieldsWhereFragment), insertValue)
				if err != nil {
					fmt.Println(err)
					continue
				}
				if existRows.Next() {
					v, err := existRows.SliceScan()
					_ = existRows.Close()
					if err != nil {
						fmt.Println(err)
					}
					if v[0].(int64) > 0 {
						fmt.Println("exist")
						continue
					}
				}

				// insert
				insertSQL := "INSERT INTO %s (%s) VALUES (%s)"
				_, err = targetDB.NamedExec(fmt.Sprintf(insertSQL, table, fieldsNameFragment, fieldsValueFragment), insertValue)
				if err != nil {
					fmt.Println(err)
				}
			}

			_ = rows.Close()
		}
	}
}
