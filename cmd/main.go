package main

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/hanchuanchuan/goInception/util/chunk"
	"github.com/hanchuanchuan/goInception/util/sqlexec"
	"github.com/hanchuanchuan/inception-core/inception"
	"github.com/pingcap/check"
)

// Result is the result returned by MustQuery.
type Result struct {
	rows    [][]interface{}
	comment check.CommentInterface
	c       *check.C
}

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

// ResultSetToResult converts sqlexec.RecordSet to testkit.Result.
// It is used to check results of execute statement in binary mode.
func ResultSetToResult(rows []chunk.Row, rs sqlexec.RecordSet) *Result {

	// tk.c.Assert(errors.ErrorStack(err), check.Equals, "", comment)
	err := rs.Close()
	if err != nil {
		fmt.Println(err)
	}
	// tk.c.Assert(errors.ErrorStack(err), check.Equals, "", comment)
	sRows := make([][]interface{}, len(rows))
	for i := range rows {
		row := rows[i]
		iRow := make([]interface{}, row.Len())
		for j := 0; j < row.Len(); j++ {
			if row.IsNull(j) {
				iRow[j] = "<nil>"
			} else {
				d := row.GetDatum(j, &rs.Fields()[j].Column.FieldType)
				// iRow[j], err = d.ToString()
				v := d.GetValue()
				switch a := v.(type) {
				case []byte:
					iRow[j] = string(a)
				default:
					iRow[j] = v
				}

				// tk.c.Assert(err, check.IsNil)
			}
		}
		sRows[i] = iRow
	}
	return &Result{rows: sRows}
}

func main() {
	core := inception.NewInception()
	// cfg:=config.GetGlobalConfig()
	core.LoadOptions(inception.SourceOptions{
		Host:     "127.0.0.1",
		Port:     3306,
		User:     "test",
		Password: "test",
	})
	result, err := core.Audit(context.Background(), "create table test.tt1(id int)")
	if err != nil {
		fmt.Println(err)
	} else {
		// rows, _ := core.GetRows4Test(context.Background(), result[0])
		// res := ResultSetToResult(rows, result[0])
		// for index, row := range res.Rows() {
		// 	fmt.Println(index)
		// 	fmt.Println(row)
		// }

		for _, row := range result {
			// fmt.Println(index)
			// fmt.Println(row)
			fmt.Println(fmt.Sprintf("%#v", row))

			b, err := json.Marshal(row)
			if err != nil {
				fmt.Println("Umarshal failed:", err)
				return
			} else {
				fmt.Println(string(b))
			}
		}
	}

	// result, err = core.RunExecute(context.Background(), "create table test.tt1(id int)")
	// if err != nil {
	// 	fmt.Println(err)
	// } else {
	// 	res := ResultSetToResult(core, result[0])
	// 	for index, row := range res.Rows() {
	// 		fmt.Println(index)
	// 		fmt.Println(row)
	// 	}
	// }
}
