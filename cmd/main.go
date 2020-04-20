package main

import (
	"context"
	"fmt"

	"github.com/hanchuanchuan/inception-core/session"
)

func main() {
	core := session.NewInception()
	core.LoadOptions(session.SourceOptions{
		Host:     "127.0.0.1",
		Port:     3306,
		User:     "test",
		Password: "test",
	})
	sql := `
	inception set lang='zh-cn';
	use test_inc;
	drop table if exists t1;
	create table t1(id int primary key);
	insert into t1 values(1);`
	result, err := core.Audit(context.Background(), sql)
	if err != nil {
		fmt.Println(err)
	}

	for _, row := range result {
		// fmt.Println(fmt.Sprintf("%#v", row))
		if row.ErrLevel == 2 {
			fmt.Println(fmt.Sprintf("sql: %v, err: %v", row.Sql, row.ErrorMessage))
		} else {
			fmt.Println(fmt.Sprintf("[%v] sql: %v", session.StatusList[row.StageStatus], row.Sql))
		}
	}
}
