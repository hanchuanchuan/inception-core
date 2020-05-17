package main

import (
	"context"
	"fmt"

	"github.com/hanchuanchuan/inception-core/config"
	"github.com/hanchuanchuan/inception-core/session"
)

func main() {
	core := session.NewInception()
	// core.LoadOptions(session.SourceOptions{
	// 	Host:           "127.0.0.1",
	// 	Port:           3306,
	// 	User:           "test",
	// 	Password:       "test",
	// 	Backup:         true,
	// 	IgnoreWarnings: true,
	// })
	inc := &config.GetGlobalConfig().Inc

	inc.BackupHost = "127.0.0.1"
	inc.BackupPort = 3306
	inc.BackupUser = "test"
	inc.BackupPassword = "test"
	opt := session.SourceOptions{Host: "db1", Port: 3306, User: "test", Password: "test",
		Check: false, Execute: true, Backup: true, IgnoreWarnings: true,
		Sleep: 0, SleepRows: 0, MiddlewareExtend: "", MiddlewareDB: "",
		ParseHost: "", ParsePort: 0, Fingerprint: false, Print: false,
		Split: false, RealRowCount: false, DB: "",
		Ssl: "", SslCA: "", SslCert: "", SslKey: "",
		TranBatch: 0}
	core.LoadOptions(opt)

	sql := `
	;
use test_inc;

inception set ghost_on=1;
inception set osc_on=0;

alter table t1 add column c31 int;


# drop table if exists t1,t2;

# create table t1(id int primary key,c1 char(1));

# insert into t1 values(1,1),(2,2);

# alter table t1 add column c2 int;

# insert into t1 values(3,3,3);

# drop table if exists t1,t2;

# create table t2(id int primary key,c1 int);

# insert into t2 values(3,3);

# CREATE TABLE share_data_template (
# id bigint(32) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键id，自增',
# share_template json DEFAULT NULL COMMENT '模板内容',
# PRIMARY KEY (id)
# ) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8mb4 COMMENT='通用分享数据模板表';

inception_magic_commit;
`
	result, err := core.RunExecute(context.Background(), sql)
	if err != nil {
		fmt.Println("error:")
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
