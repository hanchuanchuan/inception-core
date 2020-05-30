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

package testkit

import (
	"bytes"
	"fmt"
	"sort"

	// "github.com/hanchuanchuan/goInception/ast"
	"github.com/hanchuanchuan/goInception/util/testutil"
	"github.com/hanchuanchuan/inception-core/kv"
	"github.com/hanchuanchuan/inception-core/session"
	"github.com/pingcap/check"
)

// TestKit is a utility to run sql test.
type TestKit struct {
	c     *check.C
	store kv.Storage
	Se    session.Session
}

// Result is the result returned by MustQuery.
type Result struct {
	rows    [][]string
	comment check.CommentInterface
	c       *check.C
}

// Check asserts the result equals the expected results.
func (res *Result) Check(expected [][]interface{}) {
	resBuff := bytes.NewBufferString("")
	for _, row := range res.rows {
		fmt.Fprintf(resBuff, "%s\n", row)
	}
	needBuff := bytes.NewBufferString("")
	for _, row := range expected {
		fmt.Fprintf(needBuff, "%s\n", row)
	}
	res.c.Assert(resBuff.String(), check.Equals, needBuff.String(), res.comment)
}

// Rows returns the result data.
func (res *Result) Rows() [][]interface{} {
	ifacesSlice := make([][]interface{}, len(res.rows))
	for i := range res.rows {
		ifaces := make([]interface{}, len(res.rows[i]))
		for j := range res.rows[i] {
			ifaces[j] = res.rows[i][j]
		}
		ifacesSlice[i] = ifaces
	}
	return ifacesSlice
}

// Sort sorts and return the result.
func (res *Result) Sort() *Result {
	sort.Slice(res.rows, func(i, j int) bool {
		a := res.rows[i]
		b := res.rows[j]
		for i := range a {
			if a[i] < b[i] {
				return true
			} else if a[i] > b[i] {
				return false
			}
		}
		return false
	})
	return res
}

var connectionID uint64

// Rows is similar to RowsWithSep, use white space as separator string.
func Rows(args ...string) [][]interface{} {
	return testutil.RowsWithSep(" ", args...)
}
