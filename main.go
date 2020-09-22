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

const PageLimit = 1000
const CheckExist = false

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
			fieldsWhereFragment := bytes.Buffer{}

			f := fields.([]interface{})
			for _, i := range f {
				if i == nil {
					continue
				}

				// `field`,
				fieldsNameFragment.WriteString("`")
				fieldsNameFragment.WriteString(i.(string))
				fieldsNameFragment.WriteString("`,")

				// `field` = :field AND
				fieldsWhereFragment.WriteString(" `")
				fieldsWhereFragment.WriteString(i.(string))
				fieldsWhereFragment.WriteString("` = :")
				fieldsWhereFragment.WriteString(i.(string))
				fieldsWhereFragment.WriteString(" AND")
			}

			fieldsNameFragmentStr := strings.TrimRight(fieldsNameFragment.String(), ",")
			fieldsWhereFragmentStr := strings.TrimRight(fieldsWhereFragment.String(), "AND")

			var insertData [][]interface{}

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

				var insertItems []interface{}
				insertValue := make(map[string]interface{})
				f := fields.([]interface{})
				for _, i := range f {
					if i == nil {
						continue
					}
					if m[i.(string)] != nil {
						v := *m[i.(string)].(*interface{})
						insertValue[i.(string)] = v
						insertItems = append(insertItems, v)
					} else {
						insertValue[i.(string)] = nil
						insertItems = append(insertItems, nil)
					}
				}

				// exist SELECT COUNT(*) as num FROM %s WHERE %s
				if CheckExist {
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
				}

				// Insert Data
				insertData = append(insertData, insertItems)
			}
			_ = rows.Close()

			// Batch Insert
			if len(insertData) > 0 {
				insertSQL := bytes.NewBufferString("INSERT INTO ")
				insertSQL.WriteString(table)
				insertSQL.WriteString("(")
				insertSQL.WriteString(fieldsNameFragmentStr)
				insertSQL.WriteString(") VALUES ")
				var values []interface{}

				temp := ""
				for i := 0; i < len(insertData[0]); i++ {
					temp += "?,"
				}
				temp = strings.TrimRight(temp, ",")

				for _, item := range insertData {
					insertSQL.WriteString("(")
					insertSQL.WriteString(temp)
					insertSQL.WriteString("),")
					values = append(values, item...)
				}
				insertSQLStr := strings.TrimRight(insertSQL.String(), ",")
				stmt, err := targetDB.Prepare(insertSQLStr)
				if err != nil {
					fmt.Println(err)
				}

				_, err = stmt.Exec(values...)
				_ = stmt.Close()
				if err != nil {
					fmt.Println(err)
				}
			}

			pageEndTime := time.Now()
			fmt.Println("🕒", pageEndTime.Sub(pageStartTime), "/", pageEndTime.Sub(migrateStartTime))
		}

		tableEndTime := time.Now()
		fmt.Println("🕒", table, tableEndTime.Sub(tableStartTime), "/", tableEndTime.Sub(migrateStartTime))
	}

	fmt.Println("All Done")
	migrateEndTime := time.Now()
	fmt.Println("🕒", "Total", migrateEndTime.Sub(migrateStartTime))
}
