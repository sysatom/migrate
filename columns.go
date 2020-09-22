package main

import (
	"bytes"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/spf13/viper"
)

func main() {
	viper.SetConfigName("migrate")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	viper.AddConfigPath("./config/")
	err := viper.ReadInConfig()
	if err != nil {
		panic(err)
	}

	target := viper.GetStringMapString("target")

	targetDB := sqlx.MustConnect("mysql", target["url"])
	err = targetDB.Ping()
	if err != nil {
		panic(err)
	}

	yaml := bytes.Buffer{}

	rows, err := targetDB.Queryx("SHOW TABLES")
	if err != nil {
		panic(err)
	}
	for rows.Next() {
		s, err := rows.SliceScan()
		if err != nil {
			fmt.Println(err)
			continue
		}

		table := string(s[0].([]byte))
		yaml.WriteString(table)
		yaml.WriteByte(':')
		yaml.WriteByte('\n')
		tableRows, err := targetDB.Queryx("SHOW COLUMNS FROM " + table)
		if err != nil {
			fmt.Println(err)
			continue
		}

		for tableRows.Next() {
			s, err := tableRows.SliceScan()
			if err != nil {
				fmt.Println(err)
				continue
			}
			yaml.WriteByte(' ')
			yaml.WriteByte('-')
			yaml.WriteByte(' ')
			yaml.Write(s[0].([]byte))
			yaml.WriteByte('\n')
		}
		_ = tableRows.Close()
	}
	fmt.Println(yaml.String())
}
