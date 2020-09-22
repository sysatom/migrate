package main

import (
	"bytes"
	"fmt"
	"math"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/spf13/viper"
)

const PageLimit = 10000

type Total struct {
	Number int `db:"num"`
}

func main() {
	migrateStartTime := time.Now()

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
		tableStartTime := time.Now()
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
			pageStartTime := time.Now()

			fmt.Println("🦑", p, "/", page, table)
			offset := (p - 1) * PageLimit
			rows, err := sourceDB.Queryx(fmt.Sprintf("SELECT * FROM %s LIMIT %d, %d", table, offset, PageLimit))
			if err != nil {
				fmt.Println(err)
				continue
			}

			// sql
			fieldsNameFragment := bytes.Buffer{}
			fieldsValueFragment := bytes.Buffer{}
			fieldsWhereFragment := bytes.Buffer{}

			f := fields.([]interface{})
			for _, i := range f {
				if i == nil {
					continue
				}

				// field,
				fieldsNameFragment.WriteString(i.(string))
				fieldsNameFragment.WriteString(",")

				// :field,
				fieldsValueFragment.WriteString(":")
				fieldsValueFragment.WriteString(i.(string))
				fieldsValueFragment.WriteString(",")

				// `field` = :field AND
				fieldsWhereFragment.WriteString(" `")
				fieldsWhereFragment.WriteString(i.(string))
				fieldsWhereFragment.WriteString("` = :")
				fieldsWhereFragment.WriteString(i.(string))
				fieldsWhereFragment.WriteString(" AND")
			}

			fieldsNameFragmentStr := strings.TrimRight(fieldsNameFragment.String(), ",")
			fieldsValueFragmentStr := strings.TrimRight(fieldsValueFragment.String(), ",")
			fieldsWhereFragmentStr := strings.TrimRight(fieldsWhereFragment.String(), "AND")

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

				// exist SELECT COUNT(*) as num FROM %s WHERE %s
				existSQL := bytes.NewBufferString("SELECT COUNT(*) as num FROM ")
				existSQL.WriteString(table)
				existSQL.WriteString(" WHERE ")
				existSQL.WriteString(fieldsWhereFragmentStr)
				existRows, err := targetDB.NamedQuery(existSQL.String(), insertValue)
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
						continue
					}
				}

				// insert INSERT INTO %s (%s) VALUES (%s)
				insertSQL := bytes.NewBufferString("INSERT INTO ")
				insertSQL.WriteString(table)
				insertSQL.WriteString(" (")
				insertSQL.WriteString(fieldsNameFragmentStr)
				insertSQL.WriteString(") VALUES (")
				insertSQL.WriteString(fieldsValueFragmentStr)
				insertSQL.WriteString(")")
				_, err = targetDB.NamedExec(insertSQL.String(), insertValue)
				if err != nil {
					fmt.Println(err)
				}
			}

			_ = rows.Close()

			pageEndTime := time.Now()
			fmt.Println("🕒", pageEndTime.Sub(pageStartTime))
		}

		tableEndTime := time.Now()
		fmt.Println("🕒", table, tableEndTime.Sub(tableStartTime))
	}

	fmt.Println("All Done")
	migrateEndTime := time.Now()
	fmt.Println("🕒", "Total", migrateEndTime.Sub(migrateStartTime))
}
