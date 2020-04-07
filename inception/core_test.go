package inception

import (
	"fmt"
	"testing"

	. "github.com/pingcap/check"
	"golang.org/x/net/context"
)

var _ = Suite(&testSessionIncExecSuite{})

func TestExec(t *testing.T) {
	TestingT(t)
}

type testSessionIncExecSuite struct {
}

// TestDisplayWidth 测试列指定长度参数
func (s *testSessionIncExecSuite) TestInception(c *C) {
	core := inception.NewInception()
	// cfg:=config.GetGlobalConfig()
	core.LoadOptions(inception.SourceOptions{
		Host:     "127.0.0.1",
		Port:     3306,
		User:     "test",
		Password: "test",
	})
	result, err := core.Audit(context.Background(), "create table test.tt1(id int)")
	c.Assert(err, IsNil)

	for _, row := range result {
		fmt.Println(fmt.Sprintf("%#v", row))
	}

}