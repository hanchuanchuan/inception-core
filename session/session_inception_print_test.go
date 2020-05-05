// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package session_test

import (
	"strconv"
	"strings"
	"testing"

	"github.com/hanchuanchuan/inception-core/config"
	"github.com/hanchuanchuan/inception-core/session"
	. "github.com/pingcap/check"
	"golang.org/x/net/context"
)

var _ = Suite(&testSessionPrintSuite{})

func TestPrint(t *testing.T) {
	TestingT(t)
}

type testSessionPrintSuite struct {
	testCommon
}

func (s *testSessionPrintSuite) SetUpSuite(c *C) {

	s.initSetUp(c)

	config.GetGlobalConfig().Inc.Lang = "en-US"
	config.GetGlobalConfig().Inc.EnableFingerprint = true
	config.GetGlobalConfig().Inc.SqlSafeUpdates = 0

}

func (s *testSessionPrintSuite) TearDownSuite(c *C) {
	s.tearDownSuite(c)
}

func (s *testSessionPrintSuite) makeSQL(sql string) {
	s.sessionService.LoadOptions(session.SourceOptions{
		Host:           s.defaultInc.BackupHost,
		Port:           int(s.defaultInc.BackupPort),
		User:           s.defaultInc.BackupUser,
		Password:       s.defaultInc.BackupPassword,
		RealRowCount:   s.realRowCount,
		IgnoreWarnings: true,
	})
	result, _ := s.sessionService.Print(context.Background(), s.useDB+sql)
	s.rows = make([][]interface{}, len(result))
	for index, row := range result {
		s.rows[index] = row.List()
	}

	// 	a := `/*--user=test;--password=test;--host=127.0.0.1;--query-print=1;--backup=0;--port=3306;--enable-ignore-warnings;*/
	// inception_magic_start;
	// use test_inc;
	// %s;
	// inception_magic_commit;`
	// 	return s.tk.MustQueryInc(fmt.Sprintf(a, sql))
}

func (s *testSessionPrintSuite) testErrorCode(c *C, sql string, errors ...*session.SQLError) {
	if s.isAPI {
		s.sessionService.LoadOptions(session.SourceOptions{
			Host:         s.defaultInc.BackupHost,
			Port:         int(s.defaultInc.BackupPort),
			User:         s.defaultInc.BackupUser,
			Password:     s.defaultInc.BackupPassword,
			RealRowCount: s.realRowCount,
		})

		result, err := s.sessionService.Print(context.Background(), s.useDB+sql)
		c.Assert(err, IsNil)
		// s.records = result
		row := result[len(result)-1]
		errCode := uint8(0)
		if len(errors) > 0 {
			for _, e := range errors {
				level := session.GetErrorLevel(e.Code)
				if level > errCode {
					errCode = level
				}
			}
		}
		if errCode > 0 {
			errMsgs := []string{}
			for _, e := range errors {
				errMsgs = append(errMsgs, e.Error())
			}
			c.Assert(row.ErrorMessage, Equals, strings.Join(errMsgs, "\n"), Commentf("%v", result))
		}
		c.Assert(row.ErrLevel, Equals, errCode, Commentf("%#v", row))
		return
	}

	s.makeSQL(sql)
	row := s.rows[s.getAffectedRows()-1]

	errCode := 0
	if len(errors) > 0 {
		for _, e := range errors {
			level := session.GetErrorLevel(e.Code)
			if int(level) > errCode {
				errCode = int(level)
			}
		}
	}

	if errCode > 0 {
		errMsgs := []string{}
		for _, e := range errors {
			errMsgs = append(errMsgs, e.Error())
		}
		c.Assert(row[4], Equals, strings.Join(errMsgs, "\n"), Commentf("%v", row))
	}

	c.Assert(row[2], Equals, strconv.Itoa(errCode), Commentf("%v", row))

	s.rows = s.rows
}

func (s *testSessionPrintSuite) TestInsert(c *C) {

	s.makeSQL("insert into t1 values(1);")
	row := s.rows[s.getAffectedRows()-1]
	c.Assert(row[2], Equals, int64(0), Commentf("%v", row))

	s.makeSQL("insert into t1 values;")
	row = s.rows[s.getAffectedRows()-1]
	c.Assert(row[2], Equals, int64(2), Commentf("%v", row))
	// c.Assert(row[4], Equals, "line 1 column 21 near \"\" (total length 21)", Commentf("%v", row))
	c.Assert(row[4], Equals, "You have an error in your SQL syntax, near '' at line 1", Commentf("%v", row))

}

func (s *testSessionPrintSuite) TestUpdate(c *C) {

	s.makeSQL(`update t1 set c1=1 where a=1;`)
	row := s.rows[s.getAffectedRows()-1]
	c.Assert(row[2], Equals, int64(0), Commentf("%v", row))

	s.makeSQL("update t1 inner join t2 on t1.id=t2.id set c2=1 where t1.c1=1;")
	row = s.rows[s.getAffectedRows()-1]
	c.Assert(row[2], Equals, int64(0), Commentf("%v", row))

}

func (s *testSessionPrintSuite) TestDelete(c *C) {

	s.makeSQL(`delete from t1 where id=1;`)
	row := s.rows[s.getAffectedRows()-1]
	c.Assert(row[2], Equals, int64(0), Commentf("%v", row))

	// res = s.makeSQL("update t1 inner join t2 on t1.id=t2.id set c2=1 where t1.c1=1;")
	// row = s.rows[s.getAffectedRows()-1]
	// c.Assert(row[2], Equals, int64(0), Commentf("%v", row))

}

func (s *testSessionPrintSuite) TestAlterTable(c *C) {

	s.makeSQL(`alter table t1 add column c1 int;`)
	row := s.rows[s.getAffectedRows()-1]
	c.Assert(row[2], Equals, int64(0), Commentf("%v", row))

}
