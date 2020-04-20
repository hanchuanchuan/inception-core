package session

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io/ioutil"
	"math"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
	"github.com/hanchuanchuan/inception-core/ast"
	"github.com/hanchuanchuan/inception-core/config"
	"github.com/hanchuanchuan/inception-core/format"
	"github.com/hanchuanchuan/inception-core/model"
	"github.com/hanchuanchuan/inception-core/mysql"
	"github.com/hanchuanchuan/inception-core/parser/opcode"
	"github.com/hanchuanchuan/inception-core/sessionctx/variable"
	"github.com/hanchuanchuan/inception-core/types"
	"github.com/hanchuanchuan/inception-core/util"
	"github.com/hanchuanchuan/inception-core/util/auth"
	"github.com/hanchuanchuan/inception-core/util/charset"
	"github.com/hanchuanchuan/inception-core/util/sqlexec"
	"github.com/hanchuanchuan/inception-core/util/stringutil"
	"github.com/jinzhu/gorm"
	"github.com/percona/go-mysql/query"
	"github.com/pingcap/errors"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"golang.org/x/net/context"
)

var (

	// 忽略的sql列表, 这些sql大都是不同的客户端自动发出的,跳过以免报错
	skipSqlList = []string{"select @@version_comment limit 1",
		"select @@max_allowed_packet", "set autocommit=0", "show warnings",
		"set names utf8", "set names utf8mb4", "set autocommit = 0"}

	// 匹配sql的option设置
	regParseOption = regexp.MustCompile(`^\/\*(.*?)\*\/`)

	// 匹配字段长度
	regFieldLength = regexp.MustCompile(`^.*?\((\d)`)

	// 匹配标识符,只能包含字母数字和下划线
	regIdentified = regexp.MustCompile(`^[0-9a-zA-Z\_]*$`)
)

// var Keywords map[string]int = parser.GetKeywords()

const (
	maxKeyLength   = 767
	maxKeyLength57 = 3072

	remoteBackupTable              = "$_$Inception_backup_information$_$"
	TABLE_COMMENT_MAXLEN           = 2048
	COLUMN_COMMENT_MAXLEN          = 1024
	INDEX_COMMENT_MAXLEN           = 1024
	TABLE_PARTITION_COMMENT_MAXLEN = 1024
)

func (s *session) ExecuteInc(ctx context.Context, sql string) (recordSets []sqlexec.RecordSet, err error) {

	s.init()
	defer s.clear()

	// f, err := os.Create("profile_cpu")
	// if err != nil {
	// 	log.Error(err)
	// }
	// pprof.StartCPUProfile(f)
	// defer pprof.StopCPUProfile()

	// 全量日志
	if s.inc.GeneralLog {
		atomic.StoreUint32(&variable.ProcessGeneralLog, 1)
	} else {
		atomic.StoreUint32(&variable.ProcessGeneralLog, 0)
	}

	s.recordSets = NewRecordSets()

	if recordSets, err = s.executeInc(ctx, sql); err != nil {
		err = errors.Trace(err)
		s.sessionVars.StmtCtx.AppendError(err)
	}

	// pprof.StopCPUProfile()
	return
}

func (s *session) executeInc(ctx context.Context, sql string) (recordSets []sqlexec.RecordSet, err error) {
	sqlList := strings.Split(sql, "\n")

	// tidb执行的SQL关闭general日志
	logging := s.inc.GeneralLog

	defer func() {
		if s.sessionVars.StmtCtx.AffectedRows() == 0 {
			if s.opt != nil && s.opt.Print {
				s.sessionVars.StmtCtx.AddAffectedRows(uint64(s.printSets.rc.count))
			} else if s.opt != nil && s.opt.split {
				if s.splitSets != nil {
					s.sessionVars.StmtCtx.AddAffectedRows(uint64(s.splitSets.rc.count))
				} else {
					s.sessionVars.StmtCtx.AddAffectedRows(0)
				}
			} else {
				s.sessionVars.StmtCtx.AddAffectedRows(uint64(s.recordSets.rc.count))
			}
		}

		if logging {
			logQuery(sql, s.sessionVars)
		}
	}()

	// defer logQuery(sql, s.sessionVars)

	// s.PrepareTxnCtx(ctx)
	connID := s.sessionVars.ConnectionID
	// err = s.loadCommonGlobalVariablesIfNeeded()
	// if err != nil {
	// 	return nil, errors.Trace(err)
	// }

	charsetInfo, collation := s.sessionVars.GetCharsetInfo()

	lineCount := len(sqlList) - 1
	// batchSize := 1

	tmp := s.processInfo.Load()
	if tmp != nil {
		pi := tmp.(util.ProcessInfo)
		pi.OperState = "CHECKING"
		pi.Percent = 0
		s.processInfo.Store(pi)
	}

	s.stage = StageCheck

	var buf []string

	quotaIsDouble := true
	for i, sql_line := range sqlList {

		// 100行解析一次
		// 如果以分号结尾,或者是最后一行,就做解析
		// strings.HasSuffix(sql_line, ";")
		// && batchSize >= 100)

		if strings.Count(sql_line, "'")%2 == 1 {
			quotaIsDouble = !quotaIsDouble
		}

		if ((strings.HasSuffix(sql_line, ";") || strings.HasSuffix(sql_line, ";\r")) &&
			quotaIsDouble) || i == lineCount {
			// batchSize = 1
			buf = append(buf, sql_line)
			s1 := strings.Join(buf, "\n")

			s1 = strings.TrimRight(s1, ";")

			stmtNodes, err := s.ParseSQL(ctx, s1, charsetInfo, collation)

			if err == nil && len(stmtNodes) == 0 {
				tmpSQL := strings.TrimSpace(s1)
				// 未成功解析时，添加异常判断
				if tmpSQL != "" &&
					!strings.HasPrefix(tmpSQL, "#") &&
					!strings.HasPrefix(tmpSQL, "--") &&
					!strings.HasPrefix(tmpSQL, "/*") {
					err = errors.New("解析失败! 可能是解析器bug,请联系作者.")
				}
			}

			if err != nil {
				log.Errorf("con:%d 解析失败! %s", connID, err)
				log.Error(s1)
				// 移除config配置信息/*user=...*/
				if !s.haveBegin && strings.Contains(s1, "*/") {
					s1 = s1[strings.Index(s1, "*/")+2:]
				}
				if s.opt != nil && s.opt.Print {
					s.printSets.Append(2, strings.TrimSpace(s1), "", err.Error())
				} else if s.opt != nil && s.opt.split {
					s.addNewSplitNode()
					s.splitSets.Append(strings.TrimSpace(s1), err.Error())
				} else {
					s.recordSets.Append(&Record{
						Sql:          strings.TrimSpace(s1),
						ErrLevel:     2,
						ErrorMessage: err.Error(),
					})
				}
				return s.makeResult()
			}

			for i, stmtNode := range stmtNodes {
				//  是ASCII码160的特殊空格
				currentSql := strings.Trim(stmtNode.Text(), " ;\t\n\v\f\r ")

				s.myRecord = &Record{
					Sql:   currentSql,
					Buf:   new(bytes.Buffer),
					Type:  stmtNode,
					Stage: StageCheck,
				}

				switch stmtNode.(type) {
				case *ast.InceptionStartStmt:
					if s.haveBegin {
						s.appendErrorNo(ER_HAVE_BEGIN)

						if strings.Contains(currentSql, "*/") {
							currentSql = currentSql[strings.Index(currentSql, "*/")+2:]
						}
						s.myRecord.Sql = currentSql

						if s.opt != nil && s.opt.Print {
							s.printSets.Append(2, currentSql, "", s.getErrorMessage(ER_HAVE_BEGIN))
						} else if s.opt != nil && s.opt.split {
							s.addNewSplitNode()
							s.splitSets.Append(currentSql, s.getErrorMessage(ER_HAVE_BEGIN))
						} else {
							s.recordSets.Append(s.myRecord)
						}

						log.Errorf("con:%d %v", s.sessionVars.ConnectionID, sql)
						return s.makeResult()
					}

					// // 操作前重设上下文
					// if err := executor.ResetContextOfStmt(s, stmtNode); err != nil {
					// 	return nil, errors.Trace(err)
					// }

					s.haveBegin = true
					s.parseOptions(currentSql)

					if s.db != nil {
						defer s.db.Close()
					}
					if s.ddlDB != nil {
						defer s.ddlDB.Close()
					}
					if s.backupdb != nil {
						defer s.backupdb.Close()
					}

					if s.opt.Print {
						s.printSets = NewPrintSets()
					} else if s.opt.split {
						s.splitSets = NewSplitSets()
					}

					if s.myRecord.ErrLevel == 2 {
						if strings.Contains(currentSql, "*/") {
							currentSql = currentSql[strings.Index(currentSql, "*/")+2:]
						}
						s.myRecord.Sql = currentSql

						if s.opt != nil && s.opt.Print {
							s.printSets.Append(2, "", "", strings.TrimSpace(s.myRecord.Buf.String()))
						} else if s.opt != nil && s.opt.split {
							s.addNewSplitNode()
							s.splitSets.Append("", strings.TrimSpace(s.myRecord.Buf.String()))
						} else {
							s.recordSets.Append(s.myRecord)
						}
						return s.makeResult()
					}

					// sql指纹设置取并集
					if s.opt.fingerprint {
						s.inc.EnableFingerprint = true
					}

					if s.inc.EnableFingerprint {
						s.sqlFingerprint = make(map[string]*Record, 64)
					}

					continue
				case *ast.InceptionCommitStmt:

					if !s.haveBegin {
						s.appendErrorMessage("Must start as begin statement.")
						if s.opt != nil && s.opt.Print {
							s.printSets.Append(2, "", "", strings.TrimSpace(s.myRecord.Buf.String()))
						} else if s.opt != nil && s.opt.split {
							s.addNewSplitNode()
							s.splitSets.Append("", strings.TrimSpace(s.myRecord.Buf.String()))
						} else {
							s.recordSets.Append(s.myRecord)
						}
						return s.makeResult()
					}

					s.haveCommit = true
					s.executeCommit(ctx)
					return s.makeResult()
				default:
					// // TiDB原生执行器
					// if !s.haveBegin {
					// 	istidb, isFlush := s.isRunToTiDB(stmtNode)
					// 	if istidb {
					// 		r, err := s.execute(ctx, currentSql)
					// 		if isFlush {
					// 			// 权限模块的SQL在执行后自动刷新
					// 			s.execute(ctx, "FLUSH PRIVILEGES")
					// 		}
					// 		logging = false
					// 		return r, err
					// 	}
					// }

					need := s.needDataSource(stmtNode)

					if !s.haveBegin && need {
						log.Warnf("%#v", stmtNode)
						s.appendErrorMessage("Must start as begin statement.")
						if s.opt != nil && s.opt.Print {
							s.printSets.Append(2, "", "", strings.TrimSpace(s.myRecord.Buf.String()))
						} else if s.opt != nil && s.opt.split {
							s.addNewSplitNode()
							s.splitSets.Append("", strings.TrimSpace(s.myRecord.Buf.String()))
						} else {
							s.recordSets.Append(s.myRecord)
						}
						return s.makeResult()
					}

					s.SetMyProcessInfo(currentSql, time.Now(), float64(i)/float64(lineCount+1))

					// 交互式命令行
					if _, ok := stmtNode.(*ast.InceptionSetStmt); !need &&
						(!ok || (ok && !s.haveBegin)) {
						if s.opt != nil {
							return nil, errors.New("无效操作!不支持本地操作和远程操作混用!")
						}

						// // 操作前重设上下文
						// if err := executor.ResetContextOfStmt(s, stmtNode); err != nil {
						// 	return nil, errors.Trace(err)
						// }

						return s.processCommand(ctx, stmtNode, currentSql)
					} else {
						var result []sqlexec.RecordSet
						var err error
						if s.opt != nil && s.opt.Print {
							result, err = s.printCommand(ctx, stmtNode, currentSql)
						} else if s.opt != nil && s.opt.split {
							result, err = s.splitCommand(ctx, stmtNode, currentSql)
						} else {
							result, err = s.processCommand(ctx, stmtNode, currentSql)
						}
						if err != nil {
							return nil, err
						}
						if result != nil {
							return result, nil
						}
					}

					// 进程Killed
					if err := checkClose(ctx); err != nil {
						log.Warn("Killed: ", err)
						s.appendErrorMessage("Operation has been killed!")
						if s.opt != nil && s.opt.Print {
							s.printSets.Append(2, "", "", strings.TrimSpace(s.myRecord.Buf.String()))
						} else if s.opt != nil && s.opt.split {
							s.addNewSplitNode()
							s.splitSets.Append("", strings.TrimSpace(s.myRecord.Buf.String()))
						} else {
							s.recordSets.Append(s.myRecord)
						}
						return s.makeResult()
					}
				}

				if !s.haveBegin && s.needDataSource(stmtNode) {
					log.Warnf("%#v", stmtNode)
					s.appendErrorMessage("Must start as begin statement.")
					if s.opt != nil && s.opt.Print {
						s.printSets.Append(2, "", "", strings.TrimSpace(s.myRecord.Buf.String()))
					} else if s.opt != nil && s.opt.split {
						s.addNewSplitNode()
						s.splitSets.Append("", strings.TrimSpace(s.myRecord.Buf.String()))
					} else {
						s.recordSets.Append(s.myRecord)
					}
					return s.makeResult()
				}

				if s.opt != nil && s.opt.Print {
					// s.printSets.Append(2, "", "", strings.TrimSpace(s.myRecord.Buf.String()))
				} else {
					// 远程操作时隐藏本地的set命令
					if _, ok := stmtNode.(*ast.InceptionSetStmt); ok && s.myRecord.ErrLevel == 0 {
						log.Info(currentSql)
					} else {
						s.recordSets.Append(s.myRecord)
					}
				}
			}

			buf = nil

		} else if i < lineCount {
			buf = append(buf, sql_line)
			// batchSize++
		}
	}

	if s.haveBegin && !s.haveCommit {
		if s.opt != nil && s.opt.Print {
			s.printSets.Append(2, "", "", "Must end with commit.")
		} else if s.opt != nil && s.opt.split {
			s.addNewSplitNode()
			s.splitSets.Append("", "Must end with commit.")
		} else {
			s.recordSets.Append(&Record{
				Sql:          "",
				ErrLevel:     2,
				ErrorMessage: "Must end with commit.",
			})
		}
	}

	return s.makeResult()
}

func (s *session) makeResult() (recordSets []sqlexec.RecordSet, err error) {
	if s.opt != nil && s.opt.Print && s.printSets != nil {
		return s.printSets.Rows(), nil
	} else if s.opt != nil && s.opt.split && s.splitSets != nil {
		s.addNewSplitNode()
		// log.Infof("%#v", s.splitSets)
		return s.splitSets.Rows(), nil
	} else {
		return s.recordSets.Rows(), nil
	}
}

func (s *session) isRunToTiDB(stmtNode ast.StmtNode) (is bool, isFlush bool) {

	switch node := stmtNode.(type) {
	case *ast.UseStmt:
		return true, false

	case *ast.ExplainStmt:
		return true, false

	case *ast.UnionStmt:
		return true, false

	case *ast.SelectStmt:
		return true, false

		if node.From != nil {
			join := node.From.TableRefs
			if join.Right == nil {
				switch x := node.From.TableRefs.Left.(type) {
				case *ast.TableSource:
					if s, ok := x.Source.(*ast.TableName); ok {
						// log.Infof("%#v", s)
						if s.Name.L == "user" {
							return true, false
						}
						return false, false
					}
				default:
					log.Infof("%T", x)
					// log.Infof("%#v", x)
				}
			}
		} else {
			return true, false
		}

	case *ast.CreateUserStmt, *ast.AlterUserStmt, *ast.DropUserStmt,
		*ast.GrantStmt, *ast.RevokeStmt,
		*ast.SetPwdStmt:
		return true, true
	case *ast.FlushStmt:
		return true, false

	case *ast.ShowStmt:
		if !node.IsInception {
			// 添加部分命令支持
			switch node.Tp {
			case ast.ShowDatabases, ast.ShowTables,
				ast.ShowTableStatus, ast.ShowColumns,
				ast.ShowWarnings, ast.ShowGrants:
				return true, false
			}
		}
	}

	return false, false
}

func (s *session) needDataSource(stmtNode ast.StmtNode) bool {
	switch node := stmtNode.(type) {
	case *ast.ShowStmt:
		if node.IsInception {
			return false
		}
	case *ast.InceptionSetStmt, *ast.ShowOscStmt, *ast.KillStmt:
		return false
	}

	return true
}

func (s *session) processCommand(ctx context.Context, stmtNode ast.StmtNode,
	currentSql string) ([]sqlexec.RecordSet, error) {
	log.Debug("processCommand")

	switch node := stmtNode.(type) {
	case *ast.InsertStmt:
		s.checkInsert(node, currentSql)
	case *ast.DeleteStmt:
		s.checkDelete(node, currentSql)
	case *ast.UpdateStmt:
		s.checkUpdate(node, currentSql)

	case *ast.UnionStmt:
		for _, sel := range node.SelectList.Selects {
			if sel.Fields != nil {
				for _, field := range sel.Fields.Fields {
					if field.WildCard != nil {
						s.appendErrorNo(ER_SELECT_ONLY_STAR)
					}
				}
			}
		}
		s.checkSelectItem(node, false)
		if s.opt.Execute {
			s.appendErrorNo(ER_NOT_SUPPORTED_YET)
		}

	case *ast.SelectStmt:
		if node.Fields != nil {
			for _, field := range node.Fields.Fields {
				if field.WildCard != nil {
					s.appendErrorNo(ER_SELECT_ONLY_STAR)
				}
			}
		}
		s.checkSelectItem(node, false)
		if s.opt.Execute {
			s.appendErrorNo(ER_NOT_SUPPORTED_YET)
		}

	case *ast.UseStmt:
		s.checkChangeDB(node, currentSql)

	case *ast.CreateDatabaseStmt:
		s.checkCreateDB(node, currentSql)
	case *ast.DropDatabaseStmt:
		s.checkDropDB(node, currentSql)

	case *ast.CreateTableStmt:
		s.checkCreateTable(node, currentSql)
	case *ast.AlterTableStmt:
		s.checkAlterTable(node, currentSql)
	case *ast.DropTableStmt:
		s.checkDropTable(node, currentSql)
	case *ast.RenameTableStmt:
		s.checkRenameTable(node, currentSql)
	case *ast.TruncateTableStmt:
		s.checkTruncateTable(node, currentSql)

	case *ast.CreateIndexStmt:
		s.checkCreateIndex(node.Table, node.IndexName,
			node.IndexColNames, node.IndexOption, nil, node.Unique, ast.ConstraintIndex)

	case *ast.DropIndexStmt:
		s.checkDropIndex(node, currentSql)

	case *ast.CreateViewStmt:
		s.appendErrorMessage(fmt.Sprintf("命令禁止! 无法创建视图'%s'.", node.ViewName.Name))

	case *ast.ShowStmt:
		if node.IsInception {
			switch node.Tp {
			case ast.ShowVariables:
				return s.executeLocalShowVariables(node)
			case ast.ShowProcessList:
				return s.executeLocalShowProcesslist(node)
			case ast.ShowLevels:
				return s.executeLocalShowLevels(node)
			default:
				log.Infof("%#v", node)
				return nil, errors.New("不支持的语法类型")
			}
		} else {
			s.executeInceptionShow(currentSql)
		}

	case *ast.InceptionSetStmt:
		if s.haveBegin || s.isAPI {
			_, err := s.executeInceptionSet(node, currentSql)
			if err != nil {
				s.appendErrorMessage(err.Error())
			}
		} else {
			return s.executeInceptionSet(node, currentSql)
		}

	case *ast.ExplainStmt:
		s.executeInceptionShow(currentSql)

	case *ast.ShowOscStmt:
		switch node.Tp {
		case ast.OscOptionKill:
			return s.executeLocalOscKill(node)
		case ast.OscOptionPause:
			return s.executeLocalOscPause(node)
		case ast.OscOptionResume:
			return s.executeLocalOscResume(node)
		default:
			return s.executeLocalShowOscProcesslist(node)
		}

	case *ast.KillStmt:
		return s.executeKillStmt(node)

	case *ast.SetStmt:

		s.checkSetStmt(node)

	default:
		log.Infof("无匹配类型:%T\n", stmtNode)
		s.appendErrorNo(ER_NOT_SUPPORTED_YET)
	}

	s.mysqlComputeSqlSha1(s.myRecord)

	return nil, nil
}

func (s *session) executeCommit(ctx context.Context) {

	if s.opt.Check || s.opt.Print || !s.opt.Execute || s.opt.split {
		return
	}

	if s.hasErrorBefore() {
		return
	}

	// 如果有错误时,把错误输出放在第一行
	s.myRecord = s.recordSets.All()[0]

	if s.checkIsReadOnly() {
		s.appendErrorMessage("当前数据库为只读模式,无法执行!")
		return
	}

	s.modifyWaitTimeout()

	if s.opt.Backup {
		if !s.checkBinlogIsOn() {
			s.appendErrorMessage("binlog日志未开启,无法备份!")
			return
		}

		if !s.checkBinlogFormatIsRow() {
			s.modifyBinlogFormatRow()
		}

		if !s.checkBinlogRowImageIsFull() {
			s.modifyBinlogRowImageFull()
		}
	}

	if s.hasErrorBefore() {
		return
	}

	defer func() {
		// 执行结束后清理osc进程信息
		s.cleanup()
	}()

	s.executeAllStatement(ctx)

	// 只要有执行成功的,就添加备份
	// if s.recordSets.MaxLevel == 2 ||
	// 	(s.recordSets.MaxLevel == 1 && !s.opt.ignoreWarnings) {
	// 	return
	// }

	if s.opt.Backup {

		// 保存统计信息
		if s.inc.EnableSqlStatistic {
			s.sqlStatisticsSave()
		}

		// 如果连接已断开
		if err := s.backupdb.DB().Ping(); err != nil {
			log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
			addr := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=%s&parseTime=True&loc=Local&autocommit=1",
				s.inc.BackupUser, s.inc.BackupPassword, s.inc.BackupHost, s.inc.BackupPort,
				s.inc.DefaultCharset)
			db, err := gorm.Open("mysql", addr)
			if err != nil {
				log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
				s.appendErrorMessage(err.Error())
				return
			}
			// 禁用日志记录器，不显示任何日志
			db.LogMode(false)
			s.backupdb = db
		}

		log.Debug("开始备份")

		tmp := s.processInfo.Load()
		if tmp != nil {
			pi := tmp.(util.ProcessInfo)
			pi.OperState = "BACKUP"
			pi.Percent = 0
			s.processInfo.Store(pi)
		}

		s.stage = StageBackup

		s.runBackup(ctx)

		// for _, record := range s.recordSets.All() {

		// 	if s.checkSqlIsDML(record) || s.checkSqlIsDDL(record) {
		// 		s.myRecord = record

		// 		errno := s.mysqlCreateBackupTable(record)
		// 		if errno == 2 {
		// 			break
		// 		}
		// 		if record.TableInfo == nil {
		// 			s.AppendErrorNo(ErrNotFoundTableInfo)
		// 		} else {
		// 			s.mysqlBackupSql(record)
		// 		}

		// 		if s.hasError() {
		// 			break
		// 		}
		// 	}
		// }

		if !s.isMiddleware() {
			// 解析binlog生成回滚语句
			s.parserBinlog(ctx)
		} else if s.opt.parseHost != "" && s.opt.parsePort != 0 {
			s.parserBinlog(ctx)
		}
	}
}

// mysqlBackupSql 写备份记录表
// longDataType 为true表示字段类型已更新,否则为text,需要在写入时自动截断
func (s *session) mysqlBackupSql(record *Record, longDataType bool) {
	if s.checkSqlIsDDL(record) {
		s.mysqlExecuteBackupInfoInsertSql(record, longDataType)

		if s.isMiddleware() {
			s.mysqlExecuteBackupSqlForDDL(record)
		}
	} else if s.checkSqlIsDML(record) {
		s.mysqlExecuteBackupInfoInsertSql(record, longDataType)
	}
}

func makeOPIDByTime(execTime int64, threadId uint32, seqNo int) string {
	return fmt.Sprintf("%d_%d_%08d", execTime, threadId, seqNo)
}

func (s *session) checkSqlIsDML(record *Record) bool {
	switch record.Type.(type) {
	case *ast.InsertStmt, *ast.DeleteStmt, *ast.UpdateStmt:
		if record.ExecComplete {
			return true
		}
		return false
	default:
		return false
	}
}

func (s *session) checkSqlIsDDL(record *Record) bool {

	switch record.Type.(type) {
	case *ast.CreateTableStmt,
		*ast.AlterTableStmt,
		*ast.DropTableStmt,
		*ast.RenameTableStmt,
		*ast.TruncateTableStmt,

		// *ast.CreateDatabaseStmt,
		// *ast.DropDatabaseStmt,

		*ast.CreateIndexStmt,
		*ast.DropIndexStmt:
		if record.ExecComplete {
			return true
		}
		return false

	default:
		return false
	}
}

func (s *session) executeAllStatement(ctx context.Context) {

	tmp := s.processInfo.Load()
	if tmp != nil {
		pi := tmp.(util.ProcessInfo)
		pi.OperState = "EXECUTING"
		pi.Percent = 0
		s.processInfo.Store(pi)
	}

	s.stage = StageExec

	if s.opt.Execute && s.inc.EnableSqlStatistic {
		s.statistics = &statisticsInfo{}
	}

	count := len(s.recordSets.All())
	var trans []*Record
	if s.opt.TranBatch > 1 {
		trans = make([]*Record, 0, s.opt.TranBatch)
	}

	// 用于事务. 判断是否为DML语句
	// lastIsDMLTrans := false
	for i, record := range s.recordSets.All() {

		// 忽略不需要备份的类型
		switch record.Type.(type) {
		case *ast.ShowStmt, *ast.ExplainStmt:
			continue
		}

		s.SetMyProcessInfo(record.Sql, time.Now(), float64(i)/float64(count))

		if s.opt.TranBatch > 1 {
			// 非DML操作时,执行并清空事务集合
			switch record.Type.(type) {
			case *ast.InsertStmt, *ast.DeleteStmt, *ast.UpdateStmt:
				if len(trans) < s.opt.TranBatch {
					trans = append(trans, record)
				} else {
					s.executeTransaction(trans)
					trans = nil
					trans = append(trans, record)

					if s.opt.sleep > 0 && s.opt.sleepRows > 0 {
						if s.opt.sleepRows == 1 {
							mysqlSleep(s.opt.sleep)
						} else if i%s.opt.sleepRows == 0 {
							mysqlSleep(s.opt.sleep)
						}
					}
				}

				// lastIsDMLTrans = true
			case *ast.UseStmt, *ast.SetStmt:
				// 环境命令
				// 事务内部和非事务均需要执行
				// log.Infof("1111: [%s] [%d] %s,RowsAffected: %d", s.DBName, s.fetchThreadID(), record.Sql, record.AffectedRows)
				_, err := s.execDDL(record.Sql, true)
				if err != nil {
					// log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
					if myErr, ok := err.(*mysqlDriver.MySQLError); ok {
						s.appendErrorMessage(myErr.Message)
					} else {
						s.appendErrorMessage(err.Error())
					}
					break
				}

				// s.executeRemoteCommand(record)

				if len(trans) < s.opt.TranBatch {
					trans = append(trans, record)
				} else {
					s.executeTransaction(trans)

					trans = nil
					trans = append(trans, record)
				}

			default:
				if len(trans) > 0 {
					s.executeTransaction(trans)
					trans = nil
				}

				// 如果前端是DML语句,则在执行DDL前切换一次数据库
				// log.Infof("lastIsDMLTrans: %v", lastIsDMLTrans)
				// if lastIsDMLTrans {
				// 	s.SwitchDatabase(s.ddlDB)
				// 	lastIsDMLTrans = false
				// }

				s.executeRemoteCommand(record, true)

				// trans = append(trans, record)
				// s.executeTransaction(trans)
				// trans = nil

				if s.opt.sleep > 0 && s.opt.sleepRows > 0 {
					if s.opt.sleepRows == 1 {
						mysqlSleep(s.opt.sleep)
					} else if i%s.opt.sleepRows == 0 {
						mysqlSleep(s.opt.sleep)
					}
				}
			}
		} else {
			s.executeRemoteCommand(record, false)
		}

		if s.hasErrorBefore() {
			break
		}

		s.sqlStatisticsIncrement(record)

		// 进程Killed
		if err := checkClose(ctx); err != nil {
			s.killExecute = true
			log.Warn("Killed: ", err)
			s.appendErrorMessage("Operation has been killed!")
			break
		}

		if s.opt.TranBatch <= 1 && s.opt.sleep > 0 && s.opt.sleepRows > 0 {
			if s.opt.sleepRows == 1 {
				mysqlSleep(s.opt.sleep)
			} else if i%s.opt.sleepRows == 0 {
				mysqlSleep(s.opt.sleep)
			}
		}
	}

	if !s.hasErrorBefore() && s.opt.TranBatch > 1 && len(trans) > 0 {
		s.executeTransaction(trans)
	}
	trans = nil
}

// mysqlSleep Sleep for a while
func mysqlSleep(ms int) {
	if ms <= 0 {
		return
	}

	if ms > 100000 {
		ms = 100000
	}

	for end := time.Now().Add(time.Duration(ms) * time.Millisecond); time.Now().Before(end); {
	}

	return

	// time.Sleep(time.Duration(ms) * time.Millisecond)
}

func (s *session) executeTransaction(records []*Record) int {
	if records == nil {
		return 2
	}

	// for _, record := range records {
	// 	log.Info("sql: ", record.Sql)
	// }

	// 如果事务最后的命令是use或set命令,则忽略掉
	// 如果是use命令,在操作完成后切换会话的数据库
	newUseDB := ""
	skipIndex := len(records)
	for i := len(records) - 1; i >= 0; i-- {
		record := records[i]
		switch node := record.Type.(type) {
		case *ast.UseStmt:
			if newUseDB == "" {
				newUseDB = node.DBName
			}
			skipIndex = i
			continue
		case *ast.SetStmt:
			skipIndex = i
			continue
		}

		break
	}
	defer func() {
		if newUseDB != "" {
			s.dbName = newUseDB
		}
	}()
	if skipIndex == 0 {
		return 0
	} else if skipIndex < len(records)-1 {
		records = records[0:skipIndex]
	}

	// 开始事务
	tx := s.db.Begin()

	if s.dbName != "" {
		res := tx.Exec(fmt.Sprintf("USE `%s`", s.dbName))
		if errs := res.GetErrors(); len(errs) > 0 {
			tx.Rollback()
			s.myRecord = records[0]
			for _, err := range errs {
				if myErr, ok := err.(*mysqlDriver.MySQLError); ok {
					s.appendErrorMessage(myErr.Message)
				} else {
					s.appendErrorMessage(err.Error())
				}
			}
			return 2
		}
	}

	currentThreadId := s.fetchTranThreadID(tx)

	for i := range records {
		record := records[i]
		s.myRecord = record

		if i == 0 && s.opt.Backup {
			if currentThreadId == 0 {
				s.appendErrorMessage("无法获取线程号")
				tx.Rollback()
				return 2
			}
			masterStatus := s.mysqlFetchMasterBinlogPosition()
			if masterStatus == nil {
				s.appendErrorNo(ErrNotFoundMasterStatus)
				tx.Rollback()
				return 2
			} else {
				record.StartFile = masterStatus.File
				record.StartPosition = masterStatus.Position
			}
		}

		record.Stage = StageExec

		start := time.Now()
		res := tx.Exec(record.Sql)

		record.ExecTime = fmt.Sprintf("%.3f", time.Since(start).Seconds())
		record.ExecTimestamp = time.Now().Unix()

		if errs := res.GetErrors(); len(errs) > 0 {
			tx.Rollback()
			log.Errorf("con:%d %v", s.sessionVars.ConnectionID, errs)

			for j := range records {
				r := records[j]
				s.myRecord = r
				r.StageStatus = StatusExecFail
				r.ExecComplete = false
				for _, err := range errs {
					if myErr, ok := err.(*mysqlDriver.MySQLError); ok {
						s.appendErrorMessage(myErr.Message)
					} else {
						s.appendErrorMessage(err.Error())
					}
				}
				if j >= i {
					break
				}
			}
			return 2
		} else {
			// log.Infof("TRAN!!! [%s] [%d] %s,RowsAffected: %d", s.DBName, currentThreadId, record.Sql, res.RowsAffected)
			record.AffectedRows = int(res.RowsAffected)
			record.ThreadId = currentThreadId

			record.StageStatus = StatusExecOK
			record.ExecComplete = true
			s.totalChangeRows += record.AffectedRows

			switch node := record.Type.(type) {
			case *ast.UseStmt:
				s.dbName = node.DBName
			}
		}
	}
	if !s.hasError() {
		tx.Commit()

		if s.opt.Backup {
			record := records[0]
			masterStatus := s.mysqlFetchMasterBinlogPosition()
			if masterStatus == nil {
				s.appendErrorNo(ErrNotFoundMasterStatus)
				return 2
			} else {
				record.EndFile = masterStatus.File
				record.EndPosition = masterStatus.Position

				// 开始位置和结束位置一样,无变更
				if record.StartFile == record.EndFile &&
					record.StartPosition == record.EndPosition {

					record.StartFile = ""
					record.StartPosition = 0
					record.EndFile = ""
					record.EndPosition = 0
					return 0
				}
			}

			for i, r := range records {
				if i > 0 {
					r.StartFile = record.StartFile
					r.StartPosition = record.StartPosition
					r.EndFile = record.EndFile
					r.EndPosition = record.EndPosition
				}
			}
		}
	}

	return 0
}

func (s *session) executeRemoteCommand(record *Record, isTran bool) int {

	s.myRecord = record
	record.Stage = StageExec

	// log.Infof("%T", record.Type)
	switch node := record.Type.(type) {

	case *ast.InsertStmt, *ast.DeleteStmt, *ast.UpdateStmt:

		s.executeRemoteStatementAndBackup(record)

	case *ast.UseStmt,
		*ast.CreateDatabaseStmt,
		*ast.DropDatabaseStmt,

		*ast.CreateTableStmt,
		*ast.AlterTableStmt,
		*ast.DropTableStmt,
		*ast.RenameTableStmt,
		*ast.TruncateTableStmt,

		*ast.CreateIndexStmt,
		*ast.SetStmt,
		*ast.DropIndexStmt:

		s.executeRemoteStatement(record, isTran)

	default:
		log.Infof("无匹配类型: %T\n", node)
		s.appendErrorNo(ER_NOT_SUPPORTED_YET)
	}

	return int(record.ErrLevel)
}

// sqlStatisticsIncrement save statistics info
func (s *session) sqlStatisticsIncrement(record *Record) {

	if !s.opt.Execute || !s.inc.EnableSqlStatistic || s.statistics == nil {
		return
	}

	switch node := record.Type.(type) {
	case *ast.InsertStmt:
		s.statistics.insert++
	case *ast.DeleteStmt:
		s.statistics.deleting++
	case *ast.UpdateStmt:
		s.statistics.update++

	case *ast.UseStmt:
		s.statistics.usedb++

	case *ast.CreateDatabaseStmt:
		s.statistics.createdb++

	// case *ast.DropDatabaseStmt:
	// 	s.statistics.dropdb++

	case *ast.CreateTableStmt:
		s.statistics.createtable++
	case *ast.AlterTableStmt:
		s.statistics.altertable++

		for _, alter := range node.Specs {
			switch alter.Tp {
			case ast.AlterTableOption:
				s.statistics.alteroption++
			case ast.AlterTableAddColumns:
				s.statistics.addcolumn++
			case ast.AlterTableDropColumn:
				s.statistics.dropcolumn++

			case ast.AlterTableAddConstraint:
				s.statistics.createindex++
			case ast.AlterTableDropPrimaryKey, ast.AlterTableDropIndex:
				s.statistics.dropindex++

			// case ast.AlterTableDropForeignKey:

			case ast.AlterTableModifyColumn, ast.AlterTableChangeColumn:
				s.statistics.changecolumn++

			case ast.AlterTableRenameTable:
				s.statistics.rename++

			case ast.AlterTableAlterColumn:
				for _, nc := range alter.NewColumns {
					// if nc.Options != nil {
					// 	s.statistics.changedefault++
					// }
					if nc.Tp != nil {
						if nc.Tp.Charset != "" || nc.Tp.Collate != "" {
							if nc.Tp.Charset != "binary" {
								s.statistics.alterconvert++
							}
						}
					}
				}

			case ast.AlterTableLock,
				ast.AlterTableAlgorithm,
				ast.AlterTableForce:
				s.statistics.alteroption++
			}

		}

	case *ast.DropTableStmt:
		s.statistics.droptable++
	case *ast.RenameTableStmt:
		s.statistics.rename++
	case *ast.TruncateTableStmt:
		s.statistics.truncate++

	case *ast.CreateIndexStmt:
		s.statistics.createindex++
	case *ast.DropIndexStmt:
		s.statistics.dropindex++

	case *ast.SelectStmt:
		s.statistics.selects++

	}
}

// sqlStatisticsSave 保存统计信息
func (s *session) sqlStatisticsSave() {
	if !s.opt.Execute || !s.inc.EnableSqlStatistic || s.statistics == nil {
		return
	}

	s.createStatisticsTable()

	sql := `
	INSERT INTO inception.statistic ( usedb, deleting, inserting, updating,
		selecting, altertable, renaming, createindex, dropindex, addcolumn,
		dropcolumn, changecolumn, alteroption, alterconvert,
		createtable, droptable, CREATEDB, truncating)
	VALUES(?, ?, ?, ?, ?,
	       ?, ?, ?, ?, ?,
	       ?, ?, ?, ?, ?,
	       ?, ?, ?);`

	values := []interface{}{
		s.statistics.usedb,
		s.statistics.deleting,
		s.statistics.insert,
		s.statistics.update,
		s.statistics.selects,
		s.statistics.altertable,
		s.statistics.rename,
		s.statistics.createindex,
		s.statistics.dropindex,
		s.statistics.addcolumn,
		s.statistics.dropcolumn,
		s.statistics.changecolumn,
		s.statistics.alteroption,
		s.statistics.alterconvert,
		s.statistics.createtable,
		s.statistics.droptable,
		s.statistics.createdb,
		s.statistics.truncate,
		// s.statistics.changedefault,
		// s.statistics.dropdb,
	}

	if err := s.backupdb.Exec(sql, values...).Error; err != nil {
		log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
		if myErr, ok := err.(*mysqlDriver.MySQLError); ok {
			s.appendErrorMessage(myErr.Message)
		}
	}
}

func (s *session) createStatisticsTable() {
	sql := "create database if not exists inception;"
	if err := s.backupdb.Exec(sql).Error; err != nil {
		log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
		if myErr, ok := err.(*mysqlDriver.MySQLError); ok {
			if myErr.Number != 1007 { /*ER_DB_CREATE_EXISTS*/
				s.appendErrorMessage(myErr.Message)
			}
		} else {
			s.appendErrorMessage(err.Error())
		}
	}

	sql = statisticsTableSQL()
	if err := s.backupdb.Exec(sql).Error; err != nil {
		log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
		if myErr, ok := err.(*mysqlDriver.MySQLError); ok {
			if myErr.Number != 1050 { /*ER_TABLE_EXISTS_ERROR*/
				s.appendErrorMessage(myErr.Message)
			}
		} else {
			s.appendErrorMessage(err.Error())
		}
	}
}

func statisticsTableSQL() string {

	buf := bytes.NewBufferString("CREATE TABLE if not exists ")

	buf.WriteString("inception.statistic")
	buf.WriteString("(")

	buf.WriteString("id bigint auto_increment primary key, ")
	buf.WriteString("optime timestamp not null default current_timestamp, ")
	buf.WriteString("usedb int not null default 0, ")
	buf.WriteString("deleting int not null default 0, ")
	buf.WriteString("inserting int not null default 0, ")
	buf.WriteString("updating int not null default 0, ")
	buf.WriteString("selecting int not null default 0, ")
	buf.WriteString("altertable int not null default 0, ")
	buf.WriteString("renaming int not null default 0, ")
	buf.WriteString("createindex int not null default 0, ")
	buf.WriteString("dropindex int not null default 0, ")
	buf.WriteString("addcolumn int not null default 0, ")
	buf.WriteString("dropcolumn int not null default 0, ")
	buf.WriteString("changecolumn int not null default 0, ")
	buf.WriteString("alteroption int not null default 0, ")
	buf.WriteString("alterconvert int not null default 0, ")
	buf.WriteString("createtable int not null default 0, ")
	buf.WriteString("droptable int not null default 0, ")
	buf.WriteString("createdb int not null default 0, ")
	buf.WriteString("truncating int not null default 0 ")

	buf.WriteString(")ENGINE INNODB DEFAULT CHARSET UTF8;")

	return buf.String()
}

func (s *session) executeRemoteStatement(record *Record, isTran bool) {
	log.Debug("executeRemoteStatement")

	sqlStmt := record.Sql

	start := time.Now()

	if record.useOsc {
		if s.ghost.GhostOn {
			log.Infof("con:%d use gh-ost", s.sessionVars.ConnectionID)
			s.mysqlExecuteAlterTableGhost(record)
		} else {
			log.Infof("con:%d use pt-osc", s.sessionVars.ConnectionID)
			s.mysqlExecuteAlterTableOsc(record)
		}
		record.ExecTimestamp = time.Now().Unix()
		record.ThreadId = s.fetchThreadID()
		if record.ThreadId == 0 {
			s.appendErrorMessage("无法获取线程号")
		}
		record.ExecTime = fmt.Sprintf("%.3f", time.Since(start).Seconds())

		return
	} else {
		var res sql.Result
		var err error
		if isTran {
			res, err = s.execDDL(sqlStmt, false)
		} else {
			res, err = s.exec(sqlStmt, false)
		}

		record.ExecTime = fmt.Sprintf("%.3f", time.Since(start).Seconds())
		record.ExecTimestamp = time.Now().Unix()

		if err != nil {
			log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
			if myErr, ok := err.(*mysqlDriver.MySQLError); ok {
				s.appendErrorMessage(myErr.Message)
			} else {
				s.appendErrorMessage(err.Error())
			}
			record.StageStatus = StatusExecFail

			// 无法确认是否执行成功,需要通过备份来确认
			if err == mysqlDriver.ErrInvalidConn {
				// 如果没有开启备份,则直接返回
				if s.opt.Backup {
					// 如果是DML语句,则通过备份来验证是否执行成功
					// 如果是DDL语句,则直接报错,由人工确认执行结果,但仍会备份
					switch record.Type.(type) {
					case *ast.InsertStmt, *ast.DeleteStmt, *ast.UpdateStmt:
						record.AffectedRows = 0
					default:
						s.appendErrorMessage("The execution result is unknown! Please confirm manually.")
					}

					record.ThreadId = s.fetchThreadID()
					record.ExecComplete = true
				} else {
					s.appendErrorMessage("The execution result is unknown! Please confirm manually.")
				}
			}

			// log.Infof("[%s] [%d] %s,RowsAffected: %d", s.DBName, s.fetchThreadID(), record.Sql, record.AffectedRows)

			return
		} else {
			affectedRows, err := res.RowsAffected()
			if err != nil {
				s.appendErrorMessage(err.Error())
			}
			record.AffectedRows = int(affectedRows)
			record.ThreadId = s.fetchThreadID()
			if record.ThreadId == 0 {
				s.appendErrorMessage("无法获取线程号")
			} else {
				record.ExecComplete = true
			}

			record.StageStatus = StatusExecOK

			// log.Infof("[%s] [%d] %s,RowsAffected: %d", s.DBName, s.fetchThreadID(), record.Sql, record.AffectedRows)

			switch node := record.Type.(type) {
			// switch record.Type.(type) {
			case *ast.InsertStmt, *ast.DeleteStmt, *ast.UpdateStmt:
				s.totalChangeRows += record.AffectedRows
			case *ast.UseStmt:
				s.dbName = node.DBName
			}

			if _, ok := record.Type.(*ast.CreateTableStmt); ok &&
				record.TableInfo == nil && record.DBName != "" && record.TableName != "" {
				record.TableInfo = s.getTableFromCache(record.DBName, record.TableName, true)
			}
		}
	}
}

func (s *session) executeRemoteStatementAndBackup(record *Record) {
	log.Debug("executeRemoteStatementAndBackup")

	if s.opt.Backup {
		masterStatus := s.mysqlFetchMasterBinlogPosition()
		if masterStatus == nil {
			s.appendErrorNo(ErrNotFoundMasterStatus)
			return
		} else {
			record.StartFile = masterStatus.File
			record.StartPosition = masterStatus.Position
		}
	}

	if s.hasError() {
		record.StageStatus = StatusExecFail
		record.AffectedRows = 0
		return
	}

	s.executeRemoteStatement(record, false)

	if !s.hasError() || record.ExecComplete {
		if s.opt.Backup {
			masterStatus := s.mysqlFetchMasterBinlogPosition()
			if masterStatus == nil {
				s.appendErrorNo(ErrNotFoundMasterStatus)
				return
			} else {
				record.EndFile = masterStatus.File
				record.EndPosition = masterStatus.Position

				// 开始位置和结束位置一样,无变更
				if record.StartFile == record.EndFile &&
					record.StartPosition == record.EndPosition {

					record.StartFile = ""
					record.StartPosition = 0
					record.EndFile = ""
					record.EndPosition = 0
					return
				}
			}
		}

		record.ExecComplete = true
	}
}

func (s *session) mysqlFetchMasterBinlogPosition() *MasterStatus {
	log.Debug("mysqlFetchMasterBinlogPosition")

	sql := "SHOW MASTER STATUS;"
	if s.isMiddleware() {
		sql = s.opt.middlewareExtend + sql
	}

	var r MasterStatus
	rows, err := s.raw(sql)
	if rows != nil {
		defer rows.Close()
	}

	if err != nil {
		log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
		if myErr, ok := err.(*mysqlDriver.MySQLError); ok {
			s.appendErrorMessage(myErr.Message + " (sql: " + sql + ")")
		} else {
			s.appendErrorMessage(err.Error())
		}
	} else {
		for rows.Next() {
			s.db.ScanRows(rows, &r)
			return &r
		}
	}

	return nil
}

func (s *session) checkBinlogFormatIsRow() bool {
	log.Debug("checkBinlogFormatIsRow")

	sql := "show variables like 'binlog_format';"

	var format string

	rows, err := s.raw(sql)
	if rows != nil {
		defer rows.Close()
	}

	if err != nil {
		log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
		if myErr, ok := err.(*mysqlDriver.MySQLError); ok {
			s.appendErrorMessage(myErr.Message)
		} else {
			s.appendErrorMessage(err.Error())
		}
	} else {
		for rows.Next() {
			rows.Scan(&format, &format)
		}
	}

	// log.Infof("binlog format: %s", format)
	return format == "ROW"
}

func (s *session) checkBinlogRowImageIsFull() bool {
	log.Debug("checkBinlogRowImageIsFull")

	sql := "show variables like 'binlog_row_image';"

	var format string

	rows, err := s.raw(sql)
	if rows != nil {
		defer rows.Close()
	}

	if err != nil {
		log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
		if myErr, ok := err.(*mysqlDriver.MySQLError); ok {
			s.appendErrorMessage(myErr.Message)
		} else {
			s.appendErrorMessage(err.Error())
		}
	} else {
		for rows.Next() {
			rows.Scan(&format, &format)
		}
	}

	// log.Infof("binlog format: %s", format)
	return format != "MINIMAL"
}

func (s *session) mysqlServerVersion() {
	log.Debug("mysqlServerVersion")

	if s.dbVersion > 0 {
		return
	}

	var name, value string
	// sql := "select @@version;"
	sql := `show variables where Variable_name in
	('innodb_large_prefix','version','sql_mode','lower_case_table_names','wsrep_on',
	'explicit_defaults_for_timestamp');`

	rows, err := s.raw(sql)
	if rows != nil {
		defer rows.Close()
	}

	if err != nil {
		log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
		if myErr, ok := err.(*mysqlDriver.MySQLError); ok {
			s.appendErrorMessage(myErr.Message)
		} else {
			s.appendErrorMessage(err.Error())
		}
	} else {
		emptyInnodbLargePrefix := true
		for rows.Next() {
			rows.Scan(&name, &value)

			switch name {
			case "version":
				if strings.Contains(strings.ToLower(value), "mariadb") {
					s.dbType = DBTypeMariaDB
				} else if strings.Contains(strings.ToLower(value), "tidb") {
					s.dbType = DBTypeTiDB
				}

				versionStr := strings.Split(value, "-")[0]
				versionSeg := strings.Split(versionStr, ".")
				if len(versionSeg) == 3 {
					versionStr = fmt.Sprintf("%s%02s%02s", versionSeg[0], versionSeg[1], versionSeg[2])
					version, err := strconv.Atoi(versionStr)
					if err != nil {
						s.appendErrorMessage(err.Error())
					}
					s.dbVersion = version
				} else {
					s.appendErrorMessage(fmt.Sprintf("无法解析版本号:%s", value))
				}
				log.Debug("db version: ", s.dbVersion)
			case "innodb_large_prefix":
				emptyInnodbLargePrefix = false
				s.innodbLargePrefix = (value == "ON" || value == "1")
			case "sql_mode":
				if err := s.sessionVars.SetSystemVar(variable.SQLModeVar, value); err != nil {
					log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
					log.Warning(value)
				} else {
					sc := s.GetSessionVars().StmtCtx
					vars := s.sessionVars
					// 未指定严格模式或者NO_ZERO_IN_DATE时,忽略错误日期
					sc.IgnoreZeroInDate = !vars.StrictSQLMode || !vars.SQLMode.HasNoZeroInDateMode()
				}
			case "lower_case_table_names":
				if v, err := strconv.Atoi(value); err != nil {
					log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
					log.Warning(value)
				} else {
					s.lowerCaseTableNames = v
				}
			case "wsrep_on":
				s.isClusterNode = (value == "ON" || value == "1")
			case "explicit_defaults_for_timestamp":
				s.explicitDefaultsForTimestamp = (value == "ON" || value == "1")
			}
		}

		// 如果没有innodb_large_prefix系统变量
		if emptyInnodbLargePrefix {
			if s.dbVersion > 50700 {
				s.innodbLargePrefix = true
			} else {
				s.innodbLargePrefix = false
			}
		}
	}

}

func (s *session) fetchThreadID() uint32 {

	if s.threadID > 0 {
		return s.threadID
	}

	var threadId uint64
	sql := "select connection_id();"
	if s.isMiddleware() {
		sql = s.opt.middlewareExtend + sql
	}

	rows, err := s.raw(sql)
	if rows != nil {
		for rows.Next() {
			rows.Scan(&threadId)
		}
		rows.Close()
	}
	if err != nil {
		// log.Error(err, s.threadID)
		log.Errorf("con:%d thread id:%d %v", s.sessionVars.ConnectionID, s.threadID, err)
		if myErr, ok := err.(*mysqlDriver.MySQLError); ok {
			s.appendErrorMessage(myErr.Message)
		} else {
			s.appendErrorMessage(err.Error())
		}
	}

	// thread_id溢出处理
	if threadId > math.MaxUint32 {
		s.threadID = uint32(threadId % (1 << 32))
	} else {
		s.threadID = uint32(threadId)
	}

	return s.threadID
}

func (s *session) fetchTranThreadID(tx *gorm.DB) uint32 {

	var threadId uint64
	sql := "select connection_id();"
	rows, err := tx.Raw(sql).Rows()
	if err != nil {
		if myErr, ok := err.(*mysqlDriver.MySQLError); ok {
			s.appendErrorMessage(myErr.Message)
		} else {
			s.appendErrorMessage(err.Error())
		}
		return 0
	} else if rows != nil {
		for rows.Next() {
			rows.Scan(&threadId)
		}
		rows.Close()
	}

	var currentThreadId uint32

	if threadId > math.MaxUint32 {
		currentThreadId = uint32(threadId % (1 << 32))
	} else {
		currentThreadId = uint32(threadId)
	}

	return currentThreadId
}

func (s *session) modifyBinlogFormatRow() {
	log.Debug("modifyBinlogFormatRow")

	sql := "set session binlog_format='row';"

	if _, err := s.exec(sql, true); err != nil {
		log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
		if myErr, ok := err.(*mysqlDriver.MySQLError); ok {
			s.appendErrorMessage(myErr.Message + " (sql: " + sql + ")")
		} else {
			s.appendErrorMessage(err.Error())
		}
	}
}

// 设置超时时间
func (s *session) modifyWaitTimeout() {
	if s.inc.WaitTimeout <= 0 {
		return
	}
	log.Debug("modifyWaitTimeout")

	sql := fmt.Sprintf("set session wait_timeout=%d;", s.inc.WaitTimeout)

	if _, err := s.exec(sql, true); err != nil {
		log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
		if myErr, ok := err.(*mysqlDriver.MySQLError); ok {
			s.appendErrorMessage(myErr.Message)
		} else {
			s.appendErrorMessage(err.Error())
		}
	}
}

func (s *session) modifyBinlogRowImageFull() {
	log.Debug("modifyBinlogRowImageFull")

	sql := "set session binlog_row_image='FULL';"

	if _, err := s.exec(sql, true); err != nil {
		log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
		if myErr, ok := err.(*mysqlDriver.MySQLError); ok {
			s.appendErrorMessage(myErr.Message + " (sql: " + sql + ")")
		} else {
			s.appendErrorMessage(err.Error())
		}
	}
}

func (s *session) setSqlSafeUpdates() {
	log.Debug("setSqlSafeUpdates")

	var sql string
	if s.inc.SqlSafeUpdates == 1 {
		sql = "set session sql_safe_updates=1;"
	} else if s.inc.SqlSafeUpdates == 0 {
		sql = "set session sql_safe_updates=0;"
	} else {
		return
	}

	if _, err := s.exec(sql, true); err != nil {
		log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
		if myErr, ok := err.(*mysqlDriver.MySQLError); ok {
			s.appendErrorMessage(myErr.Message)
		} else {
			s.appendErrorMessage(err.Error())
		}
	}
}

func (s *session) checkBinlogIsOn() bool {
	log.Debug("checkBinlogIsOn")

	sql := "show variables like 'log_bin';"

	var format string

	rows, err := s.raw(sql)
	if rows != nil {
		defer rows.Close()
	}
	if err != nil {
		log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
		if myErr, ok := err.(*mysqlDriver.MySQLError); ok {
			s.appendErrorMessage(myErr.Message)
		} else {
			s.appendErrorMessage(err.Error())
		}
	} else {
		for rows.Next() {
			rows.Scan(&format, &format)
		}
	}

	return format == "ON" || format == "1"
}

func (s *session) checkIsReadOnly() bool {
	log.Debug("checkIsReadOnly")

	sql := "show variables like 'read_only';"

	var value string

	rows, err := s.raw(sql)
	if rows != nil {
		defer rows.Close()
	}
	if err != nil {
		log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
		if myErr, ok := err.(*mysqlDriver.MySQLError); ok {
			s.appendErrorMessage(myErr.Message)
		} else {
			s.appendErrorMessage(err.Error())
		}
	} else {
		for rows.Next() {
			rows.Scan(&value, &value)
		}
	}

	return value == "ON" || value == "1"
}

func (s *session) parseOptions(sql string) {

	firsts := regParseOption.FindStringSubmatch(sql)
	if len(firsts) < 2 {
		log.Warning(sql)
		s.appendErrorNo(ER_SQL_INVALID_SOURCE, "inception语法格式错误")
		return
	}

	// options := strings.Replace(strings.Replace(firsts[1], "-", "", -1), "_", "", -1)
	// options := strings.Replace(firsts[1], "=", ": ", -1)
	options := strings.Replace(firsts[1], "remote", "", -1)

	var buf strings.Builder

	for _, line := range strings.Split(options, ";") {
		if strings.Contains(line, "=") {
			v := strings.SplitN(line, "=", 2)
			param, value := v[0], v[1]
			param = strings.Replace(strings.Replace(param, "-", "", -1), "_", "", -1)

			buf.WriteString(param)
			buf.WriteString(": ")
			buf.WriteString(value)

		} else {
			line = strings.Replace(strings.Replace(line, "-", "", -1), "_", "", -1)
			if strings.HasPrefix(line, "enable") {
				buf.WriteString(line[6:])
				buf.WriteString(": true")
			} else if strings.HasPrefix(line, "disable") {
				buf.WriteString(line[7:])
				buf.WriteString(": false")
			} else {
				buf.WriteString(line)
			}
		}
		buf.WriteString("\n")
	}

	opt := buf.String()

	viper := viper.New()
	viper.SetConfigType("yaml")
	viper.ReadConfig(bytes.NewBuffer([]byte(opt)))

	// 设置默认值
	// viper.SetDefault("db", "mysql")

	s.opt = &SourceOptions{
		Host:           viper.GetString("host"),
		Port:           viper.GetInt("port"),
		User:           viper.GetString("user"),
		Password:       viper.GetString("password"),
		Check:          viper.GetBool("check"),
		Execute:        viper.GetBool("execute"),
		Backup:         viper.GetBool("backup"),
		IgnoreWarnings: viper.GetBool("ignoreWarnings"),
		sleep:          viper.GetInt("sleep"),
		sleepRows:      viper.GetInt("sleepRows"),

		middlewareExtend: viper.GetString("middlewareExtend"),
		middlewareDB:     viper.GetString("middlewareDB"),
		parseHost:        viper.GetString("parseHost"),
		parsePort:        viper.GetInt("parsePort"),

		fingerprint: viper.GetBool("fingerprint"),

		Print: viper.GetBool("queryPrint"),

		split:        viper.GetBool("split"),
		RealRowCount: viper.GetBool("realRowCount"),

		db: viper.GetString("db"),

		// 连接加密
		ssl:     viper.GetString("ssl"),
		sslCA:   viper.GetString("sslCa"),
		sslCert: viper.GetString("sslCert"),
		sslKey:  viper.GetString("sslKey"),

		// 开启事务功能，设置一次提交多少记录
		TranBatch: viper.GetInt("trans"),
	}

	if s.opt.split || s.opt.Check || s.opt.Print {
		s.opt.Execute = false
		s.opt.Backup = false

		// 审核阶段自动忽略警告,以免审核过早中止
		s.opt.IgnoreWarnings = true
	}

	if s.hasError() {
		return
	}

	if err := s.checkOptions(); err != nil {
		log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
		s.appendErrorMessage(err.Error())
	}
}

// getTLSConfig 获取tls设置
// https://dev.mysql.com/doc/refman/5.7/en/connection-options.html#option_general_ssl-mode
func (s *session) getTLSConfig() (string, error) {
	tlsValue := "false"
	s.opt.ssl = strings.ToLower(s.opt.ssl)
	switch s.opt.ssl {
	case "preferred", "true":
		tlsValue = "true"
	case "required":
		tlsValue = "skip-verify"
	case "verify_ca", "verify_identity":
		var errMsg string
		if s.opt.sslCA == "" {
			errMsg = "required CA file in PEM format."
		}
		if s.opt.sslCert == "" {
			errMsg += "required X509 cert in PEM format."
		}
		if s.opt.sslCert == "" {
			errMsg += "required X509 key in PEM format."
		}
		if errMsg != "" {
			return "", fmt.Errorf("con:%d %s", s.sessionVars.ConnectionID, errMsg)
		}

		if !Exist(s.opt.sslCA) {
			errMsg = fmt.Sprintf("file: %s cannot open.", s.opt.sslCA)
		}
		if !Exist(s.opt.sslCert) {
			errMsg += fmt.Sprintf("file: %s cannot open.", s.opt.sslCA)
		}
		if !Exist(s.opt.sslKey) {
			errMsg += fmt.Sprintf("file: %s cannot open.", s.opt.sslCA)
		}

		if errMsg != "" {
			return "", fmt.Errorf("con:%d %s", s.sessionVars.ConnectionID, errMsg)
		}

		tlsValue = fmt.Sprintf("%s_%d", s.opt.Host, s.opt.Port)
		if len(tlsValue) > mysql.MaxDatabaseNameLength {
			tlsValue = tlsValue[len(tlsValue)-mysql.MaxDatabaseNameLength:]
		}
		tlsValue = strings.Replace(tlsValue, "-", "_", -1)
		tlsValue = strings.Replace(tlsValue, ".", "_", -1)

		rootCertPool := x509.NewCertPool()
		pem, err := ioutil.ReadFile(s.opt.sslCA)
		if err != nil {
			return "", fmt.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
		}
		if ok := rootCertPool.AppendCertsFromPEM(pem); !ok {
			return "", fmt.Errorf("con:%d Failed to append PEM.", s.sessionVars.ConnectionID)
		}

		clientCert := make([]tls.Certificate, 0, 1)
		certs, err := tls.LoadX509KeyPair(s.opt.sslCert, s.opt.sslKey)
		if err != nil {
			return "", fmt.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
		}
		clientCert = append(clientCert, certs)

		mysqlDriver.RegisterTLSConfig(tlsValue, &tls.Config{
			// ServerName:         s.opt.host,
			RootCAs:            rootCertPool,
			Certificates:       clientCert,
			InsecureSkipVerify: s.opt.ssl == "verify_ca",
		})

	default:
		tlsValue = "false"
	}

	// log.Info(tlsValue)
	// log.Infof("%#v", s.opt)
	return tlsValue, nil
}

func (s *session) parseIncLevel() {
	obj := config.GetGlobalConfig().IncLevel
	t := reflect.TypeOf(obj)
	v := reflect.ValueOf(obj)
	s.incLevel = make(map[string]uint8, v.NumField())

	for i := 0; i < v.NumField(); i++ {
		if v.Field(i).CanInterface() {
			a := v.Field(i).Int()
			if a < 0 {
				a = 0
			} else if a > 2 {
				a = 2
			}
			if k := t.Field(i).Tag.Get("toml"); k != "" {
				s.incLevel[k] = uint8(a)
			} else {
				s.incLevel[t.Field(i).Name] = uint8(a)
			}
		}
	}

	// log.Infof("%#v", s.incLevel)
}

func (s *session) checkTruncateTable(node *ast.TruncateTableStmt, sql string) {

	log.Debug("checkTruncateTable")

	t := node.Table

	if !s.inc.EnableDropTable {
		s.appendErrorNo(ER_CANT_DROP_TABLE, t.Name)
	} else {

		if t.Schema.O == "" {
			t.Schema = model.NewCIStr(s.dbName)
		}

		table := s.getTableFromCache(t.Schema.O, t.Name.O, false)

		if table == nil {
			s.appendErrorNo(ER_TABLE_NOT_EXISTED_ERROR, fmt.Sprintf("%s.%s", t.Schema, t.Name))
		} else {
			s.mysqlShowTableStatus(table)
		}
	}
}

func (s *session) checkDropTable(node *ast.DropTableStmt, sql string) {

	log.Debug("checkDropTable")

	for _, t := range node.Tables {

		if !s.inc.EnableDropTable {
			s.appendErrorNo(ER_CANT_DROP_TABLE, t.Name)
		} else {

			if t.Schema.O == "" {
				t.Schema = model.NewCIStr(s.dbName)
			}

			table := s.getTableFromCache(t.Schema.O, t.Name.O, false)

			//如果表不存在，但存在if existed，则跳过
			if table == nil {
				if !node.IfExists {
					s.appendErrorNo(ER_TABLE_NOT_EXISTED_ERROR, fmt.Sprintf("%s.%s", t.Schema, t.Name))
				}
			} else {
				if s.opt.Execute {
					// 生成回滚语句
					s.mysqlShowCreateTable(table)
				}

				if s.opt.Check {
					// 获取表估计的受影响行数
					s.mysqlShowTableStatus(table)
				}

				s.myRecord.TableInfo = table

				s.myRecord.TableInfo.IsDeleted = true

				if s.inc.MaxDDLAffectRows > 0 && s.myRecord.AffectedRows > int(s.inc.MaxDDLAffectRows) {
					s.appendErrorNo(ER_CHANGE_TOO_MUCH_ROWS,
						"Drop", s.myRecord.AffectedRows, s.inc.MaxDDLAffectRows)
				}
			}
		}
	}
}

// mysqlShowTableStatus 获取表估计的受影响行数
func (s *session) mysqlShowTableStatus(t *TableInfo) {

	if t.IsNew {
		return
	}

	// sql := fmt.Sprintf("show table status from `%s` where name = '%s';", dbname, tableName)
	sql := fmt.Sprintf(`select TABLE_ROWS,TABLE_COLLATION from information_schema.tables
		where table_schema='%s' and table_name='%s';`, t.Schema, t.Name)

	var (
		res       uint
		collation string
	)

	rows, err := s.raw(sql)
	if rows != nil {
		defer rows.Close()
	}
	if err != nil {
		log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
		if myErr, ok := err.(*mysqlDriver.MySQLError); ok {
			s.appendErrorMessage(myErr.Message)
		} else {
			s.appendErrorMessage(err.Error())
		}
	} else if rows != nil {
		for rows.Next() {
			rows.Scan(&res, &collation)
		}
		s.myRecord.AffectedRows = int(res)
		t.Collation = collation
	}
}

// mysqlForeignKeys 获取表的所有外键
func (s *session) mysqlForeignKeys(t *TableInfo) (keys []string) {

	if t.IsNew {
		return
	}

	// sql := fmt.Sprintf("show table status from `%s` where name = '%s';", dbname, tableName)
	sql := fmt.Sprintf(`SELECT CONSTRAINT_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
		WHERE TABLE_SCHEMA='%s' AND TABLE_NAME='%s' and ORDINAL_POSITION = 1;`, t.Schema, t.Name)

	var name string

	rows, err := s.raw(sql)
	if rows != nil {
		defer rows.Close()
	}
	if err != nil {
		log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
		if myErr, ok := err.(*mysqlDriver.MySQLError); ok {
			s.appendErrorMessage(myErr.Message)
		} else {
			s.appendErrorMessage(err.Error())
		}
	} else if rows != nil {
		for rows.Next() {
			rows.Scan(&name)
			keys = append(keys, name)
		}
	}

	return
}

// mysqlGetTableSize 获取表估计的受影响行数
func (s *session) mysqlGetTableSize(t *TableInfo) {

	if t.IsNew || t.TableSize > 0 {
		return
	}

	sql := fmt.Sprintf(`select (DATA_LENGTH + INDEX_LENGTH)/1024/1024 as v
		from information_schema.tables
		where table_schema='%s' and table_name='%s';`, t.Schema, t.Name)

	var res float64

	rows, err := s.raw(sql)
	if rows != nil {
		defer rows.Close()
	}
	if err != nil {
		log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
		if myErr, ok := err.(*mysqlDriver.MySQLError); ok {
			s.appendErrorMessage(myErr.Message)
		} else {
			s.appendErrorMessage(err.Error())
		}
	} else if rows != nil {
		for rows.Next() {
			rows.Scan(&res)
		}
		t.TableSize = uint(res)
	}
}

// mysqlShowCreateTable 生成回滚语句
func (s *session) mysqlShowCreateTable(t *TableInfo) {

	if t.IsNew {
		return
	}

	sql := fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`;", t.Schema, t.Name)

	var res string

	rows, err := s.raw(sql)
	if rows != nil {
		defer rows.Close()
	}

	if err != nil {
		log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
		if myErr, ok := err.(*mysqlDriver.MySQLError); ok {
			s.appendErrorMessage(myErr.Message)
		} else {
			s.appendErrorMessage(err.Error())
		}
	} else if rows != nil {

		for rows.Next() {
			rows.Scan(&res, &res)
		}
		s.myRecord.DDLRollback = res
		s.myRecord.DDLRollback += ";"
	}
}

// mysqlShowCreateDatabase 生成回滚语句
func (s *session) mysqlShowCreateDatabase(name string) {

	sql := fmt.Sprintf("SHOW CREATE DATABASE `%s`;", name)

	var res string

	rows, err := s.raw(sql)
	if rows != nil {
		defer rows.Close()
	}

	if err != nil {
		log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
		if myErr, ok := err.(*mysqlDriver.MySQLError); ok {
			s.appendErrorMessage(myErr.Message)
		} else {
			s.appendErrorMessage(err.Error())
		}
	} else if rows != nil {
		for rows.Next() {
			rows.Scan(&res, &res)
		}
		s.myRecord.DDLRollback = res
		s.myRecord.DDLRollback += ";"
	}
}

func (s *session) checkRenameTable(node *ast.RenameTableStmt, sql string) {

	log.Debug("checkRenameTable")

	originTable := s.getTableFromCache(node.OldTable.Schema.O, node.OldTable.Name.O, true)
	if originTable == nil {
		s.appendErrorNo(ER_TABLE_NOT_EXISTED_ERROR, node.OldTable.Name.O)
	}

	table := s.getTableFromCache(node.NewTable.Schema.O, node.NewTable.Name.O, false)
	if table != nil {
		s.appendErrorNo(ER_TABLE_EXISTS_ERROR, node.NewTable.Name.O)
	}

	s.checkKeyWords(node.NewTable.Schema.O)

	if s.hasError() {
		return
	}

	// 旧表存在,新建不存在时
	if originTable != nil && table == nil {
		table = originTable.copy()

		table.Name = node.NewTable.Name.O
		if node.NewTable.Schema.O == "" {
			table.Schema = s.dbName
		} else {
			table.Schema = node.NewTable.Schema.O
		}
		s.cacheNewTable(table)
		s.myRecord.TableInfo = table

		if s.opt.Execute {
			s.myRecord.DDLRollback = fmt.Sprintf("RENAME TABLE `%s`.`%s` TO `%s`.`%s`;",
				table.Schema, table.Name, originTable.Schema, originTable.Name)
		}
	}

	if originTable != nil {
		// rename后旧表标记删除
		originTable.IsDeleted = true
	}
}

func (s *session) checkCreateTable(node *ast.CreateTableStmt, sql string) {

	log.Debug("checkCreateTable")

	// tidb暂不支持临时表 create temporary table t1

	if node.Table.Schema.O == "" {
		node.Table.Schema = model.NewCIStr(s.dbName)
	}

	if !s.checkDBExists(node.Table.Schema.O, true) {
		return
	}

	s.checkKeyWords(node.Table.Name.O)
	// 如果列名有错误的话,则直接跳出
	if s.myRecord.ErrLevel == 2 {
		return
	}

	table := s.getTableFromCache(node.Table.Schema.O, node.Table.Name.O, false)

	if table != nil {
		if !node.IfNotExists {
			s.appendErrorNo(ER_TABLE_EXISTS_ERROR, node.Table.Name.O)
		}
		s.myRecord.DBName = node.Table.Schema.O
		s.myRecord.TableName = node.Table.Name.O
	} else {

		s.myRecord.DBName = node.Table.Schema.O
		s.myRecord.TableName = node.Table.Name.O

		s.checkCreateTableGrammar(node)

		s.checkAutoIncrement(node)
		s.checkContainDotColumn(node)

		// 缓存表结构 CREATE TABLE LIKE
		if node.ReferTable != nil {
			originTable := s.getTableFromCache(node.ReferTable.Schema.O, node.ReferTable.Name.O, true)
			if originTable != nil {
				table = originTable.copy()

				table.Name = node.Table.Name.O
				table.Schema = node.Table.Schema.O

				s.cacheNewTable(table)
				s.myRecord.TableInfo = table
			}
		} else {

			if !strings.HasPrefix(node.Table.Name.L, s.inc.TablePrefix) {
				s.appendErrorNo(ER_TABLE_PREFIX, s.inc.TablePrefix)
			}

			// 校验列是否重复指定
			checkDup := map[string]bool{}
			for _, c := range node.Cols {
				if _, ok := checkDup[c.Name.Name.L]; ok {
					s.appendErrorNo(ER_DUP_FIELDNAME, c.Name.Name)
				}
				checkDup[c.Name.Name.L] = true
			}

			hasComment := false
			for _, opt := range node.Options {
				switch opt.Tp {
				case ast.TableOptionEngine:
					if s.inc.EnableSetEngine {
						s.checkEngine(opt.StrValue)
					} else {
						s.appendErrorNo(ER_CANT_SET_ENGINE, node.Table.Name.O)
					}
				case ast.TableOptionCharset:
					if s.inc.EnableSetCharset {
						s.checkCharset(opt.StrValue)
					} else {
						s.appendErrorNo(ER_TABLE_CHARSET_MUST_NULL, node.Table.Name.O)
					}
				case ast.TableOptionCollate:
					if s.inc.EnableSetCollation {
						s.checkCollation(opt.StrValue)
					} else {
						s.appendErrorNo(ErrTableCollationNotSupport, node.Table.Name.O)
					}
				case ast.TableOptionComment:
					if opt.StrValue != "" {
						hasComment = true
					}
					if len(opt.StrValue) > TABLE_COMMENT_MAXLEN {
						s.appendErrorMessage(fmt.Sprintf("Comment for table '%s' is too long (max = %d)",
							node.Table.Name.O, TABLE_COMMENT_MAXLEN))
					}
				case ast.TableOptionAutoIncrement:
					if opt.UintValue > 1 {
						s.appendErrorNo(ER_INC_INIT_ERR)
					}
				}
			}

			hasPrimary := false
			for _, ct := range node.Constraints {
				switch ct.Tp {
				case ast.ConstraintPrimaryKey:
					hasPrimary = len(ct.Keys) > 0
					for _, col := range ct.Keys {
						for _, field := range node.Cols {
							if field.Name.Name.L == col.Column.Name.L {
								// 设置主键标志位
								field.Tp.Flag |= mysql.PriKeyFlag
								break
							}
						}
					}
				case ast.ConstraintUniq, ast.ConstraintUniqIndex, ast.ConstraintUniqKey:
					for _, col := range ct.Keys {
						for _, field := range node.Cols {
							if field.Name.Name.L == col.Column.Name.L {
								// 设置唯一键标志位
								field.Tp.Flag |= mysql.UniqueKeyFlag
								break
							}
						}
					}
				}
			}

			if !hasPrimary {
				for _, field := range node.Cols {
					// hasNullFlag := false
					// defaultNullValue := false
					for _, op := range field.Options {
						switch op.Tp {
						// case ast.ColumnOptionNull:
						// 	hasNullFlag = true
						case ast.ColumnOptionPrimaryKey:
							hasPrimary = true

							// if field.Tp.Tp != mysql.TypeInt24 &&
							// 	field.Tp.Tp != mysql.TypeLong &&
							// 	field.Tp.Tp != mysql.TypeLonglong {
							// 	s.AppendErrorNo(ER_PK_COLS_NOT_INT,
							// 		field.Name.Name.O,
							// 		node.Table.Schema, node.Table.Name)
							// }
							// case ast.ColumnOptionDefaultValue:
							// 	if op.Expr.GetDatum().IsNull() {
							// 		defaultNullValue = true
							// 	}
						}
					}

					// if hasPrimary && (hasNullFlag || defaultNullValue) {
					// 	s.AppendErrorNo(ER_PRIMARY_CANT_HAVE_NULL)
					// }
					if hasPrimary {
						break
					}
				}
			}

			if !hasPrimary {
				s.appendErrorNo(ER_TABLE_MUST_HAVE_PK, node.Table.Name.O)
			}

			if !hasComment {
				s.appendErrorNo(ER_TABLE_MUST_HAVE_COMMENT, node.Table.Name.O)
			}

			if len(node.Cols) > 0 {

				// 处理explicitDefaultsForTimestamp逻辑
				if !s.explicitDefaultsForTimestamp {
					timestampColCount := 0
					for _, field := range node.Cols {
						if field.Tp.Tp == mysql.TypeTimestamp {
							timestampColCount++
							if timestampColCount == 1 {
								hasNotNullFlag := false
								hasDefault := false
								for _, op := range field.Options {
									switch op.Tp {
									case ast.ColumnOptionNotNull:
										hasNotNullFlag = true
									case ast.ColumnOptionDefaultValue, ast.ColumnOptionOnUpdate:
										hasDefault = true
									}
								}
								// NOT NULL 并且 没有默认值时,自动设置DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
								if hasNotNullFlag && !hasDefault {
									nowFunc := &ast.FuncCallExpr{FnName: model.NewCIStr(ast.CurrentTimestamp)}
									field.Options = append(field.Options,
										&ast.ColumnOption{Tp: ast.ColumnOptionDefaultValue, Expr: nowFunc})
									field.Options = append(field.Options,
										&ast.ColumnOption{Tp: ast.ColumnOptionOnUpdate, Expr: nowFunc})
								}
							} else {
								hasNullFlag := false
								hasDefault := false
								for _, op := range field.Options {
									switch op.Tp {
									case ast.ColumnOptionNull:
										hasNullFlag = true
									case ast.ColumnOptionDefaultValue:
										hasDefault = true
									}
								}
								// NOT NULL 并且 没有默认值时,自动设置DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
								if !hasNullFlag && !hasDefault {
									// 指定默认值为0000-00-00 00:00:00
									field.Options = append(field.Options,
										&ast.ColumnOption{Tp: ast.ColumnOptionDefaultValue, Expr: ast.NewValueExpr(types.ZeroDatetimeStr)})
								}
							}
						}
					}
				}

				table = s.buildTableInfo(node)

				if s.inc.MustHaveColumns != "" {
					s.checkMustHaveColumns(table)
				}

				currentTimestampCount := 0
				onUpdateTimestampCount := 0

				currentDatetimeCount := 0
				onUpdateDatetimeCount := 0

				for _, field := range node.Cols {
					s.mysqlCheckField(table, field)

					for _, op := range field.Options {
						switch op.Tp {
						case ast.ColumnOptionPrimaryKey:
							s.checkCreateIndex(nil, "PRIMARY",
								[]*ast.IndexColName{
									{Column: field.Name,
										Length: types.UnspecifiedLength},
								}, nil, table, true, ast.ConstraintPrimaryKey)
						case ast.ColumnOptionUniqKey:
							s.checkCreateIndex(nil, field.Name.String(),
								[]*ast.IndexColName{
									{Column: field.Name, Length: types.UnspecifiedLength},
								}, nil, table, true, ast.ConstraintUniq)

						}
					}

					if field.Tp.Tp == mysql.TypeTimestamp && s.inc.EnableTimeStampType {
						for _, op := range field.Options {
							if op.Tp == ast.ColumnOptionDefaultValue {
								if f, ok := op.Expr.(*ast.FuncCallExpr); ok {
									if f.FnName.L == ast.CurrentTimestamp {
										currentTimestampCount++
									}
								}
							} else if op.Tp == ast.ColumnOptionOnUpdate {
								if f, ok := op.Expr.(*ast.FuncCallExpr); ok {
									if f.FnName.L == ast.CurrentTimestamp {
										onUpdateTimestampCount++
									}
								} else {

								}
							}
						}
					}

					if field.Tp.Tp == mysql.TypeDatetime {
						for _, op := range field.Options {
							if op.Tp == ast.ColumnOptionDefaultValue {
								if f, ok := op.Expr.(*ast.FuncCallExpr); ok {
									if f.FnName.L == ast.CurrentTimestamp {
										currentDatetimeCount++
									}
								}
							} else if op.Tp == ast.ColumnOptionOnUpdate {
								if f, ok := op.Expr.(*ast.FuncCallExpr); ok {
									if f.FnName.L == ast.CurrentTimestamp {
										onUpdateDatetimeCount++
									}
								}
							}
						}
					}
				}

				if currentTimestampCount > 1 || onUpdateTimestampCount > 1 {
					s.appendErrorNo(ER_TOO_MUCH_AUTO_TIMESTAMP_COLS)
				}
				if currentDatetimeCount > 1 || onUpdateDatetimeCount > 1 {
					s.appendErrorNo(ER_TOO_MUCH_AUTO_DATETIME_COLS)
				}

				s.cacheNewTable(table)
				s.myRecord.TableInfo = table
			}
		}

		if node.Partition != nil {
			s.appendErrorNo(ER_PARTITION_NOT_ALLOWED)
		}

		if node.Select != nil {
			log.Error("暂不支持语法: ", sql)
			s.appendErrorNo(ER_NOT_SUPPORTED_YET)
		}

		if node.ReferTable != nil || len(node.Cols) > 0 {
			dupIndexes := map[string]bool{}
			for _, ct := range node.Constraints {
				if ct.Tp == ast.ConstraintForeignKey {
					s.checkCreateForeignKey(table, ct)
					continue
				}

				s.checkCreateIndex(nil, ct.Name,
					ct.Keys, ct.Option, table, false, ct.Tp)

				switch ct.Tp {
				case ast.ConstraintKey, ast.ConstraintUniq,
					ast.ConstraintIndex, ast.ConstraintUniqKey,
					ast.ConstraintUniqIndex:
					if ct.Name == "" {
						ct.Name = ct.Keys[0].Column.Name.O
					}
					if _, ok := dupIndexes[strings.ToLower(ct.Name)]; ok {
						s.appendErrorNo(ER_DUP_KEYNAME, ct.Name)
					}
					dupIndexes[strings.ToLower(ct.Name)] = true
				}
			}

			if len(node.Cols) > 0 && table == nil {
				s.appendErrorNo(ER_TABLE_NOT_EXISTED_ERROR, node.Table.Name.O)
				return
			}
		}
	}

	if s.inc.ColumnsMustHaveIndex != "" {
		s.checkColumnsMustHaveindex(table)
	}

	if !s.hasError() && s.opt.Execute {
		s.myRecord.DDLRollback = fmt.Sprintf("DROP TABLE `%s`.`%s`;", table.Schema, table.Name)
	}
}

// checkTableOptions 审核表选项
func (s *session) checkTableOptions(options []*ast.TableOption, table string, isCreate bool) {
	for _, opt := range options {
		switch opt.Tp {
		case ast.TableOptionEngine:
			if s.inc.EnableSetEngine {
				s.checkEngine(opt.StrValue)
			} else {
				s.appendErrorNo(ER_CANT_SET_ENGINE, table)
			}
		case ast.TableOptionCharset:
			if s.inc.EnableSetCharset {
				s.checkCharset(opt.StrValue)
			} else {
				s.appendErrorNo(ER_TABLE_CHARSET_MUST_NULL, table)
			}
		case ast.TableOptionCollate:
			if s.inc.EnableSetCollation {
				s.checkCollation(opt.StrValue)
			} else {
				s.appendErrorNo(ErrTableCollationNotSupport, table)
			}
		case ast.TableOptionComment:
			if len(opt.StrValue) > TABLE_COMMENT_MAXLEN {
				s.appendErrorMessage(fmt.Sprintf("Comment for table '%s' is too long (max = %d)",
					table, TABLE_COMMENT_MAXLEN))
			}
		case ast.TableOptionAutoIncrement:
			if opt.UintValue > 1 && isCreate {
				s.appendErrorNo(ER_INC_INIT_ERR)
			}
		default:
			s.appendErrorNo(ER_NOT_SUPPORTED_ALTER_OPTION)
		}
	}
}

// checkMustHaveColumns 检查表是否包含有必须的字段
func (s *session) checkMustHaveColumns(table *TableInfo) {
	columns := strings.Split(s.inc.MustHaveColumns, ",")
	if len(columns) == 0 {
		return
	}

	var notFountColumns []string
	for _, must_col := range columns {
		col := strings.TrimSpace(must_col)
		col_name := col
		col_type := ""
		if strings.Contains(col, " ") {
			column_name_type := strings.Fields(col)
			if len(column_name_type) > 1 {
				col_name = column_name_type[0]
				col_type = GetDataTypeBase(column_name_type[1])
			}
		}

		found := false
		for _, field := range table.Fields {
			if strings.EqualFold(field.Field, col_name) {
				found = true
				if col_type != "" && !strings.EqualFold(col_type, GetDataTypeBase(field.Type)) {
					notFountColumns = append(notFountColumns, col)
				}
				break
			}
		}
		if !found {
			notFountColumns = append(notFountColumns, col)
		}
	}

	if len(notFountColumns) > 0 {
		s.appendErrorNo(ER_MUST_HAVE_COLUMNS, strings.Join(notFountColumns, ","))
	}
}

func (s *session) checkColumnsMustHaveindex(table *TableInfo) {
	columns := strings.Split(s.inc.ColumnsMustHaveIndex, ",")
	if len(columns) == 0 {
		return
	}
	if table == nil {
		return
	}
	var mustHaveNotHaveIndexCol []string
	for _, mustIndexCol := range columns {
		mustIndexCol = strings.TrimSpace(mustIndexCol)
		col_name := mustIndexCol
		col_type := ""
		if strings.Contains(mustIndexCol, " ") {
			column_name_type := strings.Fields(mustIndexCol)
			if len(column_name_type) > 1 {
				col_name = column_name_type[0]
				col_type = GetDataTypeBase(column_name_type[1])
			}
		}

		inTable := false
		haveIndex := false
		for _, field := range table.Fields {
			//表内包含必须有索引的列
			if strings.EqualFold(col_name, field.Field) {
				inTable = true
				for _, indexColName := range table.Indexes {
					if strings.EqualFold(col_name, indexColName.ColumnName) && indexColName.Seq == 1 {
						haveIndex = true
					}
				}

				if col_type != "" && !strings.EqualFold(col_type, GetDataTypeBase(field.Type)) {
					s.appendErrorNo(ErrColumnsMustHaveIndexTypeErr, col_name, col_type, GetDataTypeBase(field.Type))
				}
			}
		}

		//col_name 在表中，并且没有索引
		if inTable == true && haveIndex == false {
			mustHaveNotHaveIndexCol = append(mustHaveNotHaveIndexCol, col_name)
		}
	}

	if len(mustHaveNotHaveIndexCol) > 0 {
		s.appendErrorNo(ErrColumnsMustHaveIndex, strings.Join(mustHaveNotHaveIndexCol, ","))
	}

}

func (s *session) buildTableInfo(node *ast.CreateTableStmt) *TableInfo {
	log.Debug("buildTableInfo")

	table := &TableInfo{}

	if node.Table.Schema.O == "" {
		table.Schema = s.dbName
	} else {
		table.Schema = node.Table.Schema.O
	}

	var character, collation string
	for _, opt := range node.Options {
		switch opt.Tp {
		case ast.TableOptionCharset:
			character = opt.StrValue
		case ast.TableOptionCollate:
			collation = opt.StrValue
		}
	}

	if character != "" && collation == "" {
		var err error
		collation, err = charset.GetDefaultCollation(character)
		if err != nil {
			s.appendErrorMessage(err.Error())
		}
	} else if character != "" && collation != "" {
		if !charset.ValidCharsetAndCollation(character, collation) {
			s.appendErrorMessage("字符集和排序规则不匹配!")
		}
	}

	if collation != "" {
		table.Collation = collation
	}

	table.Name = node.Table.Name.O
	table.Fields = make([]FieldInfo, 0, len(node.Cols))

	for _, field := range node.Cols {
		c := s.buildNewColumnToCache(table, field)
		table.Fields = append(table.Fields, *c)
	}
	table.IsNewColumns = true

	return table
}

func (s *session) checkAlterTable(node *ast.AlterTableStmt, sql string) {
	log.Debug("checkAlterTable")

	if node.Table.Schema.O == "" {
		node.Table.Schema = model.NewCIStr(s.dbName)
	}

	if !s.checkDBExists(node.Table.Schema.O, true) {
		return
	}

	table := s.getTableFromCache(node.Table.Schema.O, node.Table.Name.O, true)
	if table == nil {
		return
	}

	table.AlterCount++

	if table.AlterCount > 1 {
		s.appendErrorNo(ER_ALTER_TABLE_ONCE, node.Table.Name.O)
	}

	// for _, sepc := range node.Specs {
	// 	if sepc.Options != nil {
	// 		hasComment := false
	// 		for _, opt := range sepc.Options {
	// 			switch opt.Tp {
	// 			case ast.TableOptionEngine:
	// 				if !strings.EqualFold(opt.StrValue, "innodb") {
	// 					s.AppendErrorNo(ER_TABLE_MUST_INNODB, node.Table.Name.O)
	// 				}
	// 			case ast.TableOptionCharset:
	// 				if s.Inc.EnableSetCharset {
	// 					s.checkCharset(opt.StrValue)
	// 				} else {
	// 					s.AppendErrorNo(ER_TABLE_CHARSET_MUST_NULL, node.Table.Name.O)
	// 				}
	// 			case ast.TableOptionCollate:
	// 				if s.Inc.EnableSetCollation {
	// 					s.checkCollation(opt.StrValue)
	// 				} else {
	// 					s.AppendErrorNo(ErrTableCollationNotSupport, node.Table.Name.O)
	// 				}
	// 			case ast.TableOptionComment:
	// 				if opt.StrValue != "" {
	// 					hasComment = true
	// 				}
	// 			}
	// 		}
	// 		if !hasComment {
	// 			s.AppendErrorNo(ER_TABLE_MUST_HAVE_COMMENT, node.Table.Name.O)
	// 		}
	// 	}
	// }

	s.mysqlShowTableStatus(table)
	s.mysqlGetTableSize(table)

	// 如果修改了表名,则调整回滚语句
	hasRenameTable := false
	for _, alter := range node.Specs {
		if alter.Tp != ast.AlterTableRenameTable {
			s.checkAlterUseOsc(table)
		} else {
			hasRenameTable = true
			s.myRecord.useOsc = false
			break
		}
	}

	s.myRecord.TableInfo = table

	if s.opt.Backup {
		s.myRecord.DDLRollback += fmt.Sprintf("ALTER TABLE `%s`.`%s` ",
			table.Schema, table.Name)
	}
	s.alterRollbackBuffer = nil

	if s.inc.MaxDDLAffectRows > 0 && s.myRecord.AffectedRows > int(s.inc.MaxDDLAffectRows) {
		s.appendErrorNo(ER_CHANGE_TOO_MUCH_ROWS,
			"Alter", s.myRecord.AffectedRows, s.inc.MaxDDLAffectRows)
	}

	for i, alter := range node.Specs {

		switch alter.Tp {
		case ast.AlterTableOption:
			if len(alter.Options) == 0 {
				s.appendErrorNo(ER_NOT_SUPPORTED_YET)
			}
			s.checkTableOptions(alter.Options, node.Table.Name.String(), false)
		case ast.AlterTableAddColumns:
			s.checkAddColumn(table, alter)
		case ast.AlterTableDropColumn:
			// s.AppendErrorNo(ER_CANT_DROP_FIELD_OR_KEY, alter.OldColumnName.Name.O)
			s.checkDropColumn(table, alter)

		case ast.AlterTableAddConstraint:
			s.checkAddConstraint(table, alter)

		case ast.AlterTableDropPrimaryKey:
			// s.AppendErrorNo(ER_CANT_DROP_FIELD_OR_KEY, alter.Name)
			s.checkDropPrimaryKey(table, alter)
		case ast.AlterTableDropIndex:
			s.checkAlterTableDropIndex(table, alter.Name)

		case ast.AlterTableDropForeignKey:
			s.checkDropForeignKey(table, alter)

		case ast.AlterTableModifyColumn:
			s.checkModifyColumn(table, alter)
		case ast.AlterTableChangeColumn:

			s.appendErrorNo(ErCantChangeColumn, alter.OldColumnName.String())

			// 如果使用pt-osc,且非第一条语句使用了change命令,则禁止
			if i > 0 && s.myRecord.useOsc && s.osc.OscOn && !s.ghost.GhostOn {
				s.appendErrorMessage("Can't execute this sql,the renamed columns' data maybe lost(pt-osc have a bug)!")
			}
			s.checkChangeColumn(table, alter)

		case ast.AlterTableRenameTable:
			s.checkAlterTableRenameTable(table, alter)

		case ast.AlterTableAlterColumn:
			s.checkAlterTableAlterColumn(table, alter)

		case ast.AlterTableRenameIndex:
			if s.dbVersion < 50701 {
				s.appendErrorNo(ER_NOT_SUPPORTED_YET)
			} else {
				s.checkAlterTableRenameIndex(table, alter)
			}

		case ast.AlterTableLock,
			ast.AlterTableAlgorithm,
			ast.AlterTableForce:
			// 不做校验,允许这些参数

		default:
			s.appendErrorNo(ER_NOT_SUPPORTED_YET)
			log.Info("con:", s.sessionVars.ConnectionID, " 未定义的解析: ", alter.Tp)
		}

		// 由于表结构快照机制,需要在添加/删除列后重新获取一次表结构
		if i < len(node.Specs)-1 {
			table = s.getTableFromCache(node.Table.Schema.O, node.Table.Name.O, true)
			if table == nil {
				return
			}
		}
	}

	if s.inc.ColumnsMustHaveIndex != "" {
		tableCopy := s.getTableFromCache(node.Table.Schema.O, node.Table.Name.O, true)
		s.checkColumnsMustHaveindex(tableCopy)
	}

	// 生成alter回滚语句,多个时逆向
	if !s.hasError() && s.opt.Execute && s.opt.Backup {
		if hasRenameTable {
			for _, alter := range node.Specs {
				if alter.Tp == ast.AlterTableRenameTable {
					table := &TableInfo{
						Name: alter.NewTable.Name.String(),
					}
					if alter.NewTable.Schema.O == "" {
						table.Schema = s.dbName
					} else {
						table.Schema = alter.NewTable.Schema.O
					}
					s.myRecord.DDLRollback = fmt.Sprintf("ALTER TABLE `%s`.`%s` ",
						table.Schema, table.Name)
					break
				}
			}
		} else {
			s.myRecord.DDLRollback = fmt.Sprintf("ALTER TABLE `%s`.`%s` ",
				table.Schema, table.Name)
		}

		n := len(s.alterRollbackBuffer)
		if n > 1 {
			swap := reflect.Swapper(s.alterRollbackBuffer)
			for i, j := 0, n-1; i < j; i, j = i+1, j-1 {
				swap(i, j)
			}
		}

		s.myRecord.DDLRollback += strings.Join(s.alterRollbackBuffer, "")
		if strings.HasSuffix(s.myRecord.DDLRollback, ",") {
			s.myRecord.DDLRollback = strings.TrimSuffix(s.myRecord.DDLRollback, ",") + ";"
		}
	}
	s.alterRollbackBuffer = nil
}

func (s *session) checkAlterTableAlterColumn(t *TableInfo, c *ast.AlterTableSpec) {
	// log.Info("checkAlterTableAlterColumn")

	for _, nc := range c.NewColumns {
		found := false
		var foundField *FieldInfo
		for i, field := range t.Fields {
			if strings.EqualFold(field.Field, nc.Name.Name.O) {
				found = true
				foundField = &t.Fields[i]
				break
			}
		}

		if !found {
			s.appendErrorNo(ER_COLUMN_NOT_EXISTED, fmt.Sprintf("%s.%s", t.Name, nc.Name.Name))
		} else {
			if s.opt.Execute {
				if foundField.Default == nil {
					// s.myRecord.DDLRollback += "DROP DEFAULT,"
					s.alterRollbackBuffer = append(s.alterRollbackBuffer, "DROP DEFAULT,")
				} else {
					// s.myRecord.DDLRollback += fmt.Sprintf("SET DEFAULT '%s',", *foundField.Default)
					s.alterRollbackBuffer = append(s.alterRollbackBuffer,
						fmt.Sprintf("SET DEFAULT '%s',", *foundField.Default))
				}
			}

			if nc.Options == nil {
				// drop default . 不需要判断,可以删除本身为null的默认值
				foundField.Default = nil
			} else {
				// "SET" "DEFAULT" SignedLiteral
				for _, op := range nc.Options {
					defaultValue := fmt.Sprint(op.Expr.GetValue())
					if len(defaultValue) == 0 {
						switch strings.Split(foundField.Type, "(")[0] {
						case "bit", "smallint", "mediumint", "int",
							"bigint", "decimal", "float", "double", "year":
							s.appendErrorNo(ER_INVALID_DEFAULT, nc.Name.Name)
						}
					}

					foundField.Default = &defaultValue
				}
			}

		}
	}
}

func (s *session) checkAlterTableRenameIndex(t *TableInfo, c *ast.AlterTableSpec) {

	indexName := c.FromKey.String()
	newIndexName := c.ToKey.String()

	if len(t.Indexes) == 0 {
		s.appendErrorNo(ER_CANT_DROP_FIELD_OR_KEY, fmt.Sprintf("%s.%s", t.Name, indexName))
		return
	}

	var foundRows []*IndexInfo
	for _, row := range t.Indexes {
		if row.IndexName == indexName && !row.IsDeleted {
			foundRows = append(foundRows, row)
			row.IsDeleted = true
		}
	}

	if len(foundRows) == 0 {
		s.appendErrorNo(ER_CANT_DROP_FIELD_OR_KEY, fmt.Sprintf("%s.%s", t.Name, indexName))
		return
	}

	found := false
	for _, row := range t.Indexes {
		if row.IndexName == newIndexName && !row.IsDeleted {
			found = true
			break
		}
	}

	if found {
		s.appendErrorNo(ER_DUP_KEYNAME, newIndexName)
	}

	if !s.hasError() {
		// cache new index
		for _, index := range foundRows {
			index := &IndexInfo{
				Table:      t.Name,
				NonUnique:  index.NonUnique,
				IndexName:  newIndexName,
				Seq:        index.Seq,
				ColumnName: index.ColumnName,
				IndexType:  index.IndexType,
			}
			t.Indexes = append(t.Indexes, index)
		}
		if s.opt.Execute {
			rollback := fmt.Sprintf("RENAME INDEX `%s` TO `%s`,",
				newIndexName, c.FromKey.String())
			// s.myRecord.DDLRollback += rollback
			s.alterRollbackBuffer = append(s.alterRollbackBuffer, rollback)
		}
	}
}

func (s *session) checkAlterTableRenameTable(t *TableInfo, c *ast.AlterTableSpec) {
	// log.Info("checkAlterTableRenameTable")

	table := s.getTableFromCache(c.NewTable.Schema.O, c.NewTable.Name.O, false)
	if table != nil {
		s.appendErrorNo(ER_TABLE_EXISTS_ERROR, c.NewTable.Name.O)
	} else {
		// 旧表存在,新建不存在时

		table = t.copy()

		table.Name = c.NewTable.Name.O
		if c.NewTable.Schema.O == "" {
			table.Schema = s.dbName
		} else {
			table.Schema = c.NewTable.Schema.O
		}
		s.cacheNewTable(table)
		s.myRecord.TableInfo = table

		if s.opt.Execute {
			s.myRecord.DDLRollback = fmt.Sprintf("RENAME TABLE `%s`.`%s` TO `%s`.`%s`;",
				table.Schema, table.Name, t.Schema, t.Name)
			s.alterRollbackBuffer = append(s.alterRollbackBuffer, fmt.Sprintf("RENAME TO `%s`.`%s`,",
				t.Schema, t.Name))
		}

		// rename后旧表标记删除
		t.IsDeleted = true
	}
}

func (s *session) checkChangeColumn(t *TableInfo, c *ast.AlterTableSpec) {
	log.Debug("checkChangeColumn")

	s.checkModifyColumn(t, c)
}

func (s *session) checkModifyColumn(t *TableInfo, c *ast.AlterTableSpec) {
	log.Debug("checkModifyColumn")

	for _, nc := range c.NewColumns {

		found := false
		foundIndexOld := -1
		var foundField FieldInfo

		if nc.Name.Schema.L != "" && !strings.EqualFold(nc.Name.Schema.L, t.Schema) {
			s.appendErrorNo(ER_WRONG_DB_NAME, nc.Name.Schema.O)
		} else if nc.Name.Table.L != "" && !strings.EqualFold(nc.Name.Table.L, t.Name) {
			s.appendErrorNo(ER_WRONG_TABLE_NAME, nc.Name.Table.O)
		}

		if s.myRecord.ErrLevel == 2 {
			continue
		}

		// 列名未变
		if c.OldColumnName == nil || c.OldColumnName.Name.L == nc.Name.Name.L {
			for i, field := range t.Fields {
				if strings.EqualFold(field.Field, nc.Name.Name.O) {
					found = true
					foundIndexOld = i
					foundField = field
					break
				}
			}

			if !found {
				s.appendErrorNo(ER_COLUMN_NOT_EXISTED, fmt.Sprintf("%s.%s", t.Name, nc.Name.Name))
			} else {

				if c.Position.Tp != ast.ColumnPositionNone {

					s.appendErrorNo(ErCantChangeColumnPosition,
						fmt.Sprintf("%s.%s", t.Name, nc.Name.Name))

					// 在新的快照上变更表结构
					t := s.cacheTableSnapshot(t)

					t.Fields[foundIndexOld] = *(s.buildNewColumnToCache(t, nc))
					newField := t.Fields[foundIndexOld]

					if c.Position.Tp == ast.ColumnPositionFirst {
						tmp := make([]FieldInfo, 0, len(t.Fields))
						tmp = append(tmp, newField)
						if foundIndexOld > 0 {
							tmp = append(tmp, t.Fields[:foundIndexOld]...)
						}
						tmp = append(tmp, t.Fields[foundIndexOld+1:]...)

						t.Fields = tmp
					} else if c.Position.Tp == ast.ColumnPositionAfter {
						foundIndex := -1
						for i, field := range t.Fields {
							if strings.EqualFold(field.Field, c.Position.RelativeColumn.Name.O) && !field.IsDeleted {
								foundIndex = i
								break
							}
						}
						if foundIndex == -1 {
							s.appendErrorNo(ER_COLUMN_NOT_EXISTED,
								fmt.Sprintf("%s.%s", t.Name, c.Position.RelativeColumn.Name))
						} else if foundIndex == foundIndexOld-1 {
							// 原位置和新位置一样,不做操作
						} else {

							tmp := make([]FieldInfo, 0, len(t.Fields)+3)
							// 先把列移除
							// tmp = append(t.Fields[:foundIndexOld], t.Fields[foundIndexOld+1:]...)

							if foundIndex > foundIndexOld {
								tmp = append(tmp, t.Fields[:foundIndexOld]...)
								tmp = append(tmp, t.Fields[foundIndexOld+1:foundIndex+1]...)
								tmp = append(tmp, newField)
								tmp = append(tmp, t.Fields[foundIndex+1:]...)
							} else {
								tmp = append(tmp, t.Fields[:foundIndex+1]...)
								tmp = append(tmp, newField)
								tmp = append(tmp, t.Fields[foundIndex+1:foundIndexOld]...)
								tmp = append(tmp, t.Fields[foundIndexOld+1:]...)
							}

							t.Fields = tmp
						}
					}
				} else {
					t.Fields[foundIndexOld] = *(s.buildNewColumnToCache(t, nc))
				}

				if s.opt.Execute {
					buf := bytes.NewBufferString("MODIFY COLUMN `")
					buf.WriteString(foundField.Field)
					buf.WriteString("` ")
					buf.WriteString(foundField.Type)
					if foundField.Null == "NO" || foundField.Key == "PRI" {
						buf.WriteString(" NOT NULL")
					}
					// if strings.Contains(foundField.Extra, "auto_increment") {
					// 	buf.WriteString(" AUTO_INCREMENT")
					// }
					if foundField.Default != nil {
						if strings.EqualFold(*foundField.Default, ast.CurrentTimestamp) {
							buf.WriteString(" DEFAULT ")
							buf.WriteString(strings.ToUpper(ast.CurrentTimestamp))
						} else {
							buf.WriteString(" DEFAULT '")
							buf.WriteString(*foundField.Default)
							buf.WriteString("'")
						}
					}
					if foundField.Extra != "" {
						buf.WriteString(" ")
						buf.WriteString(strings.ToUpper(foundField.Extra))
					}
					if foundField.Comment != "" {
						buf.WriteString(" COMMENT '")
						buf.WriteString(foundField.Comment)
						buf.WriteString("'")
					}
					buf.WriteString(",")

					// s.myRecord.DDLRollback += buf.String()
					s.alterRollbackBuffer = append(s.alterRollbackBuffer, buf.String())
				}
			}
		} else { // 列名改变

			oldFound := false
			newFound := false
			foundIndexOld := -1
			for i, field := range t.Fields {
				if strings.EqualFold(field.Field, c.OldColumnName.Name.L) && !field.IsDeleted {
					oldFound = true
					foundIndexOld = i
					foundField = field
				}
				if strings.EqualFold(field.Field, nc.Name.Name.L) && !field.IsDeleted {
					newFound = true
				}
			}

			// 未变更列名时,列需要存在
			// 变更列名后,新列名不能存在
			if newFound {
				s.appendErrorNo(ER_COLUMN_EXISTED, fmt.Sprintf("%s.%s", t.Name, nc.Name.Name.O))
			}
			if !oldFound {
				s.appendErrorNo(ER_COLUMN_NOT_EXISTED, fmt.Sprintf("%s.%s", t.Name, c.OldColumnName.Name.O))
			}

			s.checkKeyWords(nc.Name.Name.O)

			if !s.hasError() {

				// 在新的快照上变更表结构
				t := s.cacheTableSnapshot(t)

				// t.Fields[foundIndexOld].Field = nc.Name.Name.O
				t.Fields[foundIndexOld] = *(s.buildNewColumnToCache(t, nc))
				newField := t.Fields[foundIndexOld]

				// 修改列名后标记有新列
				t.IsNewColumns = true

				if c.Position.Tp != ast.ColumnPositionNone {

					s.appendErrorNo(ErCantChangeColumnPosition,
						fmt.Sprintf("%s.%s", t.Name, nc.Name.Name))

					if c.Position.Tp == ast.ColumnPositionFirst {
						tmp := make([]FieldInfo, 0, len(t.Fields))
						tmp = append(tmp, newField)
						if foundIndexOld > 0 {
							tmp = append(tmp, t.Fields[:foundIndexOld]...)
						}
						tmp = append(tmp, t.Fields[foundIndexOld+1:]...)

						t.Fields = tmp
					} else if c.Position.Tp == ast.ColumnPositionAfter {
						foundIndex := -1
						for i, field := range t.Fields {
							if strings.EqualFold(field.Field, c.Position.RelativeColumn.Name.O) && !field.IsDeleted {
								foundIndex = i
								break
							}
						}

						if foundIndex == -1 {
							s.appendErrorNo(ER_COLUMN_NOT_EXISTED,
								fmt.Sprintf("%s.%s", t.Name, c.Position.RelativeColumn.Name))
						} else if foundIndex == foundIndexOld-1 {
							// 原位置和新位置一样,不做操作
						} else {

							tmp := make([]FieldInfo, 0, len(t.Fields)+3)
							// 先把列移除
							// tmp = append(t.Fields[:foundIndexOld], t.Fields[foundIndexOld+1:]...)

							if foundIndex > foundIndexOld {
								tmp = append(tmp, t.Fields[:foundIndexOld]...)
								tmp = append(tmp, t.Fields[foundIndexOld+1:foundIndex+1]...)
								tmp = append(tmp, newField)
								tmp = append(tmp, t.Fields[foundIndex+1:]...)
							} else {
								tmp = append(tmp, t.Fields[:foundIndex+1]...)
								tmp = append(tmp, newField)
								tmp = append(tmp, t.Fields[foundIndex+1:foundIndexOld]...)
								tmp = append(tmp, t.Fields[foundIndexOld+1:]...)
							}

							t.Fields = tmp
						}
					}
				}

				if s.opt.Execute {
					buf := bytes.NewBufferString("CHANGE COLUMN `")
					buf.WriteString(nc.Name.Name.O)
					buf.WriteString("` `")
					buf.WriteString(foundField.Field)
					buf.WriteString("` ")
					buf.WriteString(foundField.Type)
					if foundField.Null == "NO" || foundField.Key == "PRI" {
						buf.WriteString(" NOT NULL")
					}
					// if strings.Contains(foundField.Extra, "auto_increment") {
					// 	buf.WriteString(" AUTO_INCREMENT")
					// }
					if foundField.Default != nil {
						if *foundField.Default == ast.CurrentTimestamp {
							buf.WriteString(" DEFAULT ")
							buf.WriteString(strings.ToUpper(ast.CurrentTimestamp))
						} else {
							buf.WriteString(" DEFAULT '")
							buf.WriteString(*foundField.Default)
							buf.WriteString("'")
						}
					}
					if foundField.Extra != "" {
						buf.WriteString(" ")
						buf.WriteString(strings.ToUpper(foundField.Extra))
					}
					if foundField.Comment != "" {
						buf.WriteString(" COMMENT '")
						buf.WriteString(foundField.Comment)
						buf.WriteString("'")
					}
					buf.WriteString(",")

					// s.myRecord.DDLRollback += buf.String()
					s.alterRollbackBuffer = append(s.alterRollbackBuffer, buf.String())
				}
			}
		}

		// 未变更列名时,列需要存在
		// 变更列名后,新列名不能存在
		// if c.OldColumnName == nil && !found {
		// 	s.AppendErrorNo(ER_COLUMN_NOT_EXISTED, fmt.Sprintf("%s.%s", t.Name, nc.Name.Name))
		// } else if c.OldColumnName != nil &&
		// 	!strings.EqualFold(c.OldColumnName.Name.L, foundField.Field) && found {
		// 	s.AppendErrorNo(ER_COLUMN_EXISTED, fmt.Sprintf("%s.%s", t.Name, foundField.Field))
		// }

		// if !types.IsTypeBlob(nc.Tp.Tp) && (nc.Tp.Charset != "" || nc.Tp.Collate != "") {
		// 	s.AppendErrorNo(ER_CHARSET_ON_COLUMN, t.Name, nc.Name.Name)
		// }

		s.mysqlCheckField(t, nc)

		// 列(或旧列)未找到时结束
		if s.hasError() {
			return
		}

		// 列类型转换审核
		fieldType := nc.Tp.CompactStr()
		if s.inc.CheckColumnTypeChange && fieldType != foundField.Type {
			switch nc.Tp.Tp {
			case mysql.TypeDecimal, mysql.TypeNewDecimal,
				mysql.TypeVarchar,
				mysql.TypeVarString:
				str := string([]byte(foundField.Type)[:7])
				// 类型不一致
				if !strings.Contains(fieldType, str) {
					s.appendErrorNo(ER_CHANGE_COLUMN_TYPE,
						fmt.Sprintf("%s.%s", t.Name, nc.Name.Name),
						foundField.Type, fieldType)
				} else if GetDataTypeLength(fieldType)[0] < GetDataTypeLength(foundField.Type)[0] {
					s.appendErrorNo(ER_CHANGE_COLUMN_TYPE,
						fmt.Sprintf("%s.%s", t.Name, nc.Name.Name),
						foundField.Type, fieldType)
				}
			case mysql.TypeString:
				str := string([]byte(foundField.Type)[:4])
				// 类型不一致
				if !strings.Contains(fieldType, str) {
					s.appendErrorNo(ER_CHANGE_COLUMN_TYPE,
						fmt.Sprintf("%s.%s", t.Name, nc.Name.Name),
						foundField.Type, fieldType)
				} else if GetDataTypeLength(fieldType)[0] < GetDataTypeLength(foundField.Type)[0] {
					s.appendErrorNo(ER_CHANGE_COLUMN_TYPE,
						fmt.Sprintf("%s.%s", t.Name, nc.Name.Name),
						foundField.Type, fieldType)
				}
			default:
				// log.Info(fieldType, ":", foundField.Type)

				oldType := GetDataTypeBase(foundField.Type)
				newType := GetDataTypeBase(fieldType)

				// 判断如果是int8 >> int16 >> int32等转换,则忽略
				oldTypeIndex, ok1 := integerOrderedMaps[GetDataTypeBase(foundField.Type)]
				newTypeIndex, ok2 := integerOrderedMaps2[nc.Tp.Tp]
				if ok1 && ok2 {
					if newTypeIndex < oldTypeIndex {
						s.appendErrorNo(ER_CHANGE_COLUMN_TYPE,
							fmt.Sprintf("%s.%s", t.Name, nc.Name.Name),
							foundField.Type, fieldType)
					}
				} else if oldType == newType &&
					(oldType == "enum" || oldType == "set") {

				} else {
					s.appendErrorNo(ER_CHANGE_COLUMN_TYPE,
						fmt.Sprintf("%s.%s", t.Name, nc.Name.Name),
						foundField.Type, fieldType)
				}
			}
		}
	}

	// if c.Position.Tp != ast.ColumnPositionNone {
	// 	found := false
	// 	for _, field := range t.Fields {
	// 		if strings.EqualFold(field.Field, c.Position.RelativeColumn.Name.O) {
	// 			found = true
	// 			break
	// 		}
	// 	}
	// 	if !found {
	// 		s.AppendErrorNo(ER_COLUMN_NOT_EXISTED,
	// 			fmt.Sprintf("%s.%s", t.Name, c.Position.RelativeColumn.Name))
	// 	}
	// }
}

// hasError return current sql has errors or warnings
func (s *session) hasError() bool {
	if s.myRecord.ErrLevel == 2 ||
		(s.myRecord.ErrLevel == 1 && !s.opt.IgnoreWarnings) {
		return true
	}

	return false
}

// hasError return all sql has errors or warnings
func (s *session) hasErrorBefore() bool {
	if s.recordSets.MaxLevel == 2 ||
		(s.recordSets.MaxLevel == 1 && !s.opt.IgnoreWarnings) {
		return true
	}

	return false
}

func (s *session) mysqlCheckField(t *TableInfo, field *ast.ColumnDef) {
	log.Debug("mysqlCheckField")

	tableName := t.Name
	if !s.inc.EnableEnumSetBit && (field.Tp.Tp == mysql.TypeEnum ||
		field.Tp.Tp == mysql.TypeSet ||
		field.Tp.Tp == mysql.TypeBit) {
		s.appendErrorNo(ER_INVALID_DATA_TYPE, field.Name.Name)
	}

	if field.Tp.Tp == mysql.TypeTimestamp && !s.inc.EnableTimeStampType {
		s.appendErrorNo(ER_INVALID_DATA_TYPE, field.Name.Name)
	}

	if field.Tp.Tp == mysql.TypeString && (s.inc.MaxCharLength > 0 && field.Tp.Flen > int(s.inc.MaxCharLength)) {
		s.appendErrorNo(ER_CHAR_TO_VARCHAR_LEN, field.Name.Name)
	}

	if (field.Tp.Tp == mysql.TypeFloat || field.Tp.Tp == mysql.TypeDouble) && s.inc.CheckFloatDouble {
		s.appendErrorNo(ErrFloatDoubleToDecimal, field.Name.Name)
	}

	s.checkKeyWords(field.Name.Name.O)

	// notNullFlag := mysql.HasNotNullFlag(field.Tp.Flag)
	// autoIncrement := mysql.HasAutoIncrementFlag(field.Tp.Flag)

	hasComment := false
	notNullFlag := false
	autoIncrement := false
	hasDefaultValue := false
	hasGenerated := false
	var defaultValue *types.Datum
	var defaultExpr ast.ExprNode

	isPrimary := false

	if len(field.Options) > 0 {
		for _, op := range field.Options {

			switch op.Tp {
			case ast.ColumnOptionComment:
				if op.Expr.GetDatum().GetString() != "" {
					hasComment = true
				}
			case ast.ColumnOptionNotNull:
				notNullFlag = true
			case ast.ColumnOptionNull:
				notNullFlag = false
			case ast.ColumnOptionAutoIncrement:
				autoIncrement = true
			case ast.ColumnOptionDefaultValue:
				defaultExpr = op.Expr
				defaultValue = op.Expr.GetDatum()
				hasDefaultValue = true
			case ast.ColumnOptionPrimaryKey:
				isPrimary = true
			case ast.ColumnOptionGenerated:
				hasGenerated = true
			case ast.ColumnOptionCollate:
				if s.inc.EnableColumnCharset {
					s.checkCollation(op.StrValue)
				} else {
					s.appendErrorNo(ER_CHARSET_ON_COLUMN, tableName, field.Name.Name)
				}
			}
		}
	}

	if !isPrimary {
		if field.Tp != nil && mysql.HasPriKeyFlag(field.Tp.Flag) {
			isPrimary = true
		}
	}

	if !hasComment {
		s.appendErrorNo(ER_COLUMN_HAVE_NO_COMMENT, field.Name.Name, tableName)
	}

	//有默认值，且归类无效，如(default CURRENT_TIMESTAMP)
	if hasDefaultValue && s.isInvalidDefaultValue(field) {
		s.appendErrorNo(ER_INVALID_DEFAULT, field.Name.Name.O)
	}

	//有默认值，且为NULL，且有NOT NULL约束，如(not null default null)
	if _, ok := defaultExpr.(*ast.ValueExpr); ok && hasDefaultValue && defaultValue.IsNull() && notNullFlag {
		s.appendErrorNo(ER_INVALID_DEFAULT, field.Name.Name.O)
	}

	//有默认值，且不为NULL
	if _, ok := defaultExpr.(*ast.ValueExpr); ok && hasDefaultValue && !defaultValue.IsNull() {
		switch field.Tp.Tp {
		case mysql.TypeTiny, mysql.TypeShort, mysql.TypeInt24,
			mysql.TypeLong, mysql.TypeLonglong,
			mysql.TypeYear,
			mysql.TypeFloat, mysql.TypeDouble, mysql.TypeNewDecimal:
			//验证string型默认值的合法性
			if v, ok := defaultValue.GetValue().(string); ok {
				if v == "" {
					s.appendErrorNo(ER_INVALID_DEFAULT, field.Name.Name)
				} else {
					_, intErr := strconv.ParseInt(defaultValue.GetString(), 10, 64)
					_, floatErr := strconv.ParseFloat(defaultValue.GetString(), 64)
					if intErr != nil && floatErr != nil {
						s.appendErrorNo(ER_INVALID_DEFAULT, field.Name.Name)
					}
				}

			}

		}
	}

	//不可设置default值的部分字段类型
	if hasDefaultValue && !defaultValue.IsNull() && (field.Tp.Tp == mysql.TypeJSON || types.IsTypeBlob(field.Tp.Tp)) {
		s.appendErrorNo(ER_BLOB_CANT_HAVE_DEFAULT, field.Name.Name.O)
	}
	//是否使用 text\blob\json 字段类型
	//当EnableNullable=false，不强制text\blob\json使用NOT NULL
	if types.IsTypeBlob(field.Tp.Tp) {
		s.appendErrorNo(ER_USE_TEXT_OR_BLOB, field.Name.Name)
	} else if field.Tp.Tp == mysql.TypeJSON {
		s.appendErrorNo(ErrJsonTypeSupport, field.Name.Name)
	} else {
		if !notNullFlag && !hasGenerated {
			s.appendErrorNo(ER_NOT_ALLOWED_NULLABLE, field.Name.Name, tableName)
		}
	}

	// 审核所有指定了charset或collate的字段
	if field.Tp.Charset != "" || field.Tp.Collate != "" {
		if field.Tp.Charset != "" && field.Tp.Charset != "binary" {
			if s.inc.EnableColumnCharset {
				s.checkCharset(field.Tp.Charset)
			} else {
				s.appendErrorNo(ER_CHARSET_ON_COLUMN, tableName, field.Name.Name)
			}
		} else if field.Tp.Collate != "" && field.Tp.Collate != "binary" {
			if s.inc.EnableColumnCharset {
				s.checkCollation(field.Tp.Collate)
			} else {
				s.appendErrorNo(ER_CHARSET_ON_COLUMN, tableName, field.Name.Name)
			}
		}
	}

	// 检查bit类型的默认值
	// 只允许数字0和1,以及二进制写法如 b'1'
	if hasDefaultValue && field.Tp.Tp == mysql.TypeBit {
		switch defaultValue.Kind() {
		case types.KindInt64:
			if defaultValue.GetInt64() != 0 && defaultValue.GetInt64() != 1 {
				s.appendErrorNo(ER_INVALID_DEFAULT, field.Name.Name.O)
			}
		case types.KindUint64:
			if defaultValue.GetUint64() != 0 && defaultValue.GetUint64() != 1 {
				s.appendErrorNo(ER_INVALID_DEFAULT, field.Name.Name.O)
			}
		case types.KindMysqlBit, types.KindBinaryLiteral:
			v := defaultValue.GetBinaryLiteral()
			if len(v) == 0 {
				s.appendErrorNo(ER_INVALID_DEFAULT, field.Name.Name.O)
			}
		case types.KindString:
			if defaultValue.GetString() != "" {
				s.appendErrorNo(ER_INVALID_DEFAULT, field.Name.Name.O)
			}
		default:
			s.appendErrorNo(ER_INVALID_DEFAULT, field.Name.Name.O)
		}
	}

	// if isIncorrectName(field.Name.Name.O) {
	// 	s.AppendErrorNo(ER_WRONG_COLUMN_NAME, field.Name.Name)
	// }

	//text/blob/json 字段禁止设置NOT NULL
	if (types.IsTypeBlob(field.Tp.Tp) || field.Tp.Tp == mysql.TypeJSON) && notNullFlag {
		s.appendErrorNo(ER_TEXT_NOT_NULLABLE_ERROR, field.Name.Name, tableName)
	}

	if autoIncrement {
		if !mysql.HasUnsignedFlag(field.Tp.Flag) {
			s.appendErrorNo(ER_AUTOINC_UNSIGNED, tableName)
		}

		if field.Tp.Tp != mysql.TypeLong &&
			field.Tp.Tp != mysql.TypeLonglong &&
			field.Tp.Tp != mysql.TypeInt24 {
			s.appendErrorNo(ER_SET_DATA_TYPE_INT_BIGINT)
		}
	}

	if field.Tp.Tp == mysql.TypeTimestamp {
		// if !mysql.HasNoDefaultValueFlag(field.Tp.Flag) {
		if !hasDefaultValue {
			s.appendErrorNo(ER_TIMESTAMP_DEFAULT, field.Name.Name.O)
		}
	}

	if !hasDefaultValue && field.Tp.Tp != mysql.TypeTimestamp &&
		!types.IsTypeBlob(field.Tp.Tp) && !autoIncrement && !isPrimary && field.Tp.Tp != mysql.TypeJSON && !hasGenerated {
		s.appendErrorNo(ER_WITH_DEFAULT_ADD_COLUMN, field.Name.Name.O, tableName)
	}

	s.checkColumn(field)
	// if (thd->variables.sql_mode & MODE_NO_ZERO_DATE &&
	//        is_timestamp_type(field->sql_type) && !field->def &&
	//        (field->flags & NOT_NULL_FLAG) &&
	//        (field->unireg_check == Field::NONE ||
	//         field->unireg_check == Field::TIMESTAMP_UN_FIELD))
	//    {
	//        my_error(ER_INVALID_DEFAULT, MYF(0), field->field_name);
	//        mysql_errmsg_append(thd);
	//    }
}

func (s *session) checkIndexAttr(tp ast.ConstraintType, name string,
	keys []*ast.IndexColName, table *TableInfo) {

	if tp == ast.ConstraintPrimaryKey {

		if s.inc.MaxPrimaryKeyParts > 0 && len(keys) > int(s.inc.MaxPrimaryKeyParts) {
			s.appendErrorNo(ER_PK_TOO_MANY_PARTS, table.Schema, table.Name, s.inc.MaxPrimaryKeyParts)
		}

		s.checkDuplicateColumnName(keys)

		return
	}

	if name == "" {
		if !s.inc.EnableNullIndexName {
			//s.AppendErrorNo(ER_NULL_NAME_FOR_INDEX, table.Name)
			s.appendErrorNo(ER_WRONG_NAME_FOR_INDEX, "NULL", table.Name)
		}

	} else {
		// found := false
		// for _, field := range table.Fields {
		// 	if strings.EqualFold(field.Field, name) {
		// 		found = true
		// 		break
		// 	}
		// }

		if name != strings.ToUpper(name) {
			s.appendErrorNo(ErrIdentifierUpper, name)
		}

		if isIncorrectName(name) {
			s.appendErrorNo(ER_WRONG_NAME_FOR_INDEX, name, table.Name)
		} else {
			if len(name) > mysql.MaxIndexIdentifierLen {
				s.appendErrorNo(ER_TOO_LONG_IDENT, name)
			}
		}
	}

	if tp != ast.ConstraintPrimaryKey && strings.ToUpper(name) == "PRIMARY" {
		s.appendErrorNo(ER_WRONG_NAME_FOR_INDEX, name, table.Name)
	}

	s.checkDuplicateColumnName(keys)

	switch tp {
	case ast.ConstraintForeignKey:
		s.appendErrorNo(ER_FOREIGN_KEY, table.Name)

	case ast.ConstraintUniq:
		if !strings.HasPrefix(strings.ToLower(name), s.inc.UniqIndexPrefix) {
			s.appendErrorNo(ER_INDEX_NAME_UNIQ_PREFIX, name, s.inc.UniqIndexPrefix, table.Name)
		}

	case ast.ConstraintSpatial:
		if len(keys) > 1 {
			s.appendErrorNo(ER_TOO_MANY_KEY_PARTS, name, table.Name, 1)
		}

	default:
		if !strings.HasPrefix(strings.ToLower(name), s.inc.IndexPrefix) {
			s.appendErrorNo(ER_INDEX_NAME_IDX_PREFIX, name, s.inc.IndexPrefix, table.Name)
		}
	}

	if s.inc.MaxKeyParts > 0 && len(keys) > int(s.inc.MaxKeyParts) {
		s.appendErrorNo(ER_TOO_MANY_KEY_PARTS, name, table.Name, s.inc.MaxKeyParts)
	}

}

func (s *session) checkCreateForeignKey(t *TableInfo, c *ast.Constraint) {
	// log.Infof("%#v", c)

	if !s.inc.EnableForeignKey {
		s.appendErrorNo(ER_FOREIGN_KEY, t.Name)
		return
	}

	for _, col := range c.Keys {
		found := false
		for _, field := range t.Fields {
			if strings.EqualFold(field.Field, col.Column.Name.O) {
				found = true
				break
			}
		}
		if !found {
			s.appendErrorNo(ER_COLUMN_NOT_EXISTED, fmt.Sprintf("%s.%s", t.Name, col.Column.Name.O))
		}
	}

	refTable := s.getTableFromCache(c.Refer.Table.Schema.O, c.Refer.Table.Name.O, true)
	if refTable != nil {
		for _, col := range c.Refer.IndexColNames {
			found := false
			for _, field := range refTable.Fields {
				if strings.EqualFold(field.Field, col.Column.Name.O) {
					found = true
					break
				}
			}
			if !found {
				s.appendErrorNo(ER_COLUMN_NOT_EXISTED, fmt.Sprintf("%s.%s", refTable.Name, col.Column.Name.O))
			}
		}
	}
	if len(c.Keys) != len(c.Refer.IndexColNames) {
		s.appendErrorNo(ErrWrongFkDefWithMatch, c.Name)
	}

	if !t.IsNew && c.Name != "" {
		keys := s.mysqlForeignKeys(t)
		for _, k := range keys {
			if strings.EqualFold(k, c.Name) {
				s.appendErrorNo(ErrFkDupName, c.Name)
				break
			}
		}
	}
}

func (s *session) checkDropForeignKey(t *TableInfo, c *ast.AlterTableSpec) {
	log.Debug("checkDropForeignKey")

	// log.Infof("%s \n", c)
	if s.inc.EnableForeignKey {
		if !t.IsNew {
			keys := s.mysqlForeignKeys(t)
			found := false
			for _, k := range keys {
				if strings.EqualFold(k, c.Name) {
					found = true
					break
				}
			}
			if !found {
				s.appendErrorNo(ER_CANT_DROP_FIELD_OR_KEY, c.Name)
			}
		}
	} else {
		s.appendErrorNo(ER_NOT_SUPPORTED_YET)
	}
}
func (s *session) checkAlterTableDropIndex(t *TableInfo, indexName string) bool {
	log.Debug("checkAlterTableDropIndex")

	// var rows []*IndexInfo

	// if !t.IsNew {
	// 	// 删除索引时回库查询
	// 	sql := fmt.Sprintf("SHOW INDEX FROM `%s`.`%s` where key_name=?", t.Schema, t.Name)
	// 	if err := s.db.Raw(sql, indexName).Scan(&rows).Error; err != nil {
	// 		if myErr, ok := err.(*mysqlDriver.MySQLError); ok {
	// 			s.AppendErrorMessage(myErr.Message)
	// 		} else {
	// 			s.AppendErrorMessage(err.Error())
	// 		}
	// 		return false
	// 	}
	// } else {
	// 	rows = t.Indexes
	// }

	if len(t.Indexes) == 0 {
		s.appendErrorNo(ER_CANT_DROP_FIELD_OR_KEY, fmt.Sprintf("%s.%s", t.Name, indexName))
		return false
	}

	var foundRows []*IndexInfo
	for _, row := range t.Indexes {
		if row.IndexName == indexName && !row.IsDeleted {
			foundRows = append(foundRows, row)
			row.IsDeleted = true
		}
	}

	if len(foundRows) == 0 {
		s.appendErrorNo(ER_CANT_DROP_FIELD_OR_KEY, fmt.Sprintf("%s.%s", t.Name, indexName))
		return false
	}

	if s.opt.Execute {
		var rollbackSql string
		for i, row := range foundRows {
			if i == 0 {
				if indexName == "PRIMARY" {
					rollbackSql += "ADD PRIMARY KEY("
				} else {
					if row.NonUnique == 0 {
						rollbackSql += fmt.Sprintf("ADD UNIQUE INDEX `%s`(", indexName)
					} else {
						if row.IndexType == "SPATIAL" {
							rollbackSql += fmt.Sprintf("ADD %s INDEX `%s`(", row.IndexType, indexName)
						} else {
							rollbackSql += fmt.Sprintf("ADD INDEX `%s`(", indexName)
						}
					}
				}
				rollbackSql += fmt.Sprintf("`%s`", row.ColumnName)
			} else {
				rollbackSql += fmt.Sprintf(",`%s`", row.ColumnName)
			}
		}
		rollbackSql += "),"

		s.myRecord.DDLRollback = fmt.Sprintf("ALTER TABLE `%s`.`%s` ",
			t.Schema, t.Name)
		s.myRecord.DDLRollback += rollbackSql
		if strings.HasSuffix(s.myRecord.DDLRollback, ",") {
			s.myRecord.DDLRollback = strings.TrimSuffix(s.myRecord.DDLRollback, ",") + ";"
		}
		s.alterRollbackBuffer = append(s.alterRollbackBuffer, rollbackSql)
	}
	return true
}

func (s *session) checkDropPrimaryKey(t *TableInfo, c *ast.AlterTableSpec) {
	log.Debug("checkDropPrimaryKey")

	s.checkAlterTableDropIndex(t, "PRIMARY")
}

func (s *session) checkAddColumn(t *TableInfo, c *ast.AlterTableSpec) {

	for _, nc := range c.NewColumns {
		found := false
		for _, field := range t.Fields {
			if strings.EqualFold(field.Field, nc.Name.Name.O) && !field.IsDeleted {
				found = true
				break
			}
		}
		if found {
			s.appendErrorNo(ER_COLUMN_EXISTED, fmt.Sprintf("%s.%s", t.Name, nc.Name.Name))
		} else {
			s.mysqlCheckField(t, nc)

			if !s.hasError() {
				isPrimary := false
				isUnique := false
				for _, op := range nc.Options {
					switch op.Tp {
					case ast.ColumnOptionPrimaryKey:
						isPrimary = true
					case ast.ColumnOptionUniqKey:
						isUnique = true
					}
				}
				if isPrimary || isUnique {
					rows := t.Indexes
					indexName := ""
					if isPrimary {
						indexName = mysql.PrimaryKeyName
					} else if isUnique {
						indexName = nc.Name.Name.String()
					}
					if len(rows) > 0 {
						for _, row := range rows {
							if !row.IsDeleted {
								if strings.EqualFold(row.IndexName, mysql.PrimaryKeyName) {
									s.appendErrorNo(ER_DUP_INDEX, mysql.PrimaryKeyName, t.Schema, t.Name)
									break
								} else if strings.EqualFold(row.IndexName, indexName) {
									indexName = indexName + "_2"
									break
								}
							}
						}
					}
					key_count := 0
					for _, row := range rows {
						if row.Seq == 1 && !row.IsDeleted {
							key_count++
						}
					}
					if s.inc.MaxKeys > 0 && key_count >= int(s.inc.MaxKeys) {
						s.appendErrorNo(ER_TOO_MANY_KEYS, t.Name, s.inc.MaxKeys)
					}
					if !s.hasError() {
						index := &IndexInfo{
							Table:      t.Name,
							IndexName:  indexName,
							Seq:        1,
							ColumnName: nc.Name.Name.String(),
							IndexType:  "BTREE",
							NonUnique:  0,
						}
						t.Indexes = append(t.Indexes, index)
					}
				}
			}

			newColumn := s.buildNewColumnToCache(t, nc)

			// 在新的快照上变更表结构
			t := s.cacheTableSnapshot(t)
			t.IsNewColumns = true

			if c.Position == nil || c.Position.Tp == ast.ColumnPositionNone {
				t.Fields = append(t.Fields, *newColumn)
			} else if c.Position.Tp == ast.ColumnPositionFirst {
				tmp := make([]FieldInfo, 0, len(t.Fields)+1)
				tmp = append(tmp, *newColumn)
				tmp = append(tmp, t.Fields...)
				t.Fields = tmp

			} else if c.Position.Tp == ast.ColumnPositionAfter {
				foundIndex := -1
				for i, field := range t.Fields {
					if strings.EqualFold(field.Field, c.Position.RelativeColumn.Name.O) {
						foundIndex = i
						break
					}
				}
				if foundIndex == -1 {
					s.appendErrorNo(ER_COLUMN_NOT_EXISTED,
						fmt.Sprintf("%s.%s", t.Name, c.Position.RelativeColumn.Name))
				} else if foundIndex == len(t.Fields)-1 {
					t.Fields = append(t.Fields, *newColumn)
				} else {
					tmp := make([]FieldInfo, 0, len(t.Fields)+1)
					tmp = append(tmp, t.Fields[:foundIndex+1]...)
					tmp = append(tmp, *newColumn)
					tmp = append(tmp, t.Fields[foundIndex+1:]...)
					t.Fields = tmp
				}
			}

			if c.Position != nil && c.Position.Tp != ast.ColumnPositionNone {
				s.appendErrorNo(ErCantChangeColumnPosition,
					fmt.Sprintf("%s.%s", t.Name, nc.Name.Name))
			}

			if s.opt.Execute {
				s.alterRollbackBuffer = append(s.alterRollbackBuffer,
					fmt.Sprintf("DROP COLUMN `%s`,",
						nc.Name.Name.O))
				// s.myRecord.DDLRollback += fmt.Sprintf("DROP COLUMN `%s`,",
				// 	nc.Name.Name.O)
			}
		}
	}
}

// checkExistsColumns 获取总列数,以避免删除最后一列
func checkExistsColumns(t *TableInfo) (count int) {
	for _, field := range t.Fields {
		if !field.IsDeleted {
			count++
		}
	}
	return
}

func (s *session) checkDropColumn(t *TableInfo, c *ast.AlterTableSpec) {

	found := false
	for i, field := range t.Fields {
		if strings.EqualFold(field.Field, c.OldColumnName.Name.O) && !field.IsDeleted {
			found = true
			s.mysqlDropColumnRollback(field)

			if checkExistsColumns(t) > 1 {
				// 在新的快照上删除字段
				newTable := s.cacheTableSnapshot(t)
				(&(newTable.Fields[i])).IsDeleted = true
			} else {
				s.appendErrorNo(ErrCantRemoveAllFields)
			}

			break
		}
	}
	if !found {
		s.appendErrorNo(ER_COLUMN_NOT_EXISTED,
			fmt.Sprintf("%s.%s", t.Name, c.OldColumnName.Name.O))
	}
}

// cacheTableSnapshot 保存表快照,用以解析binlog
// 当删除列,变更列顺序时, 重新保存表结构
func (s *session) cacheTableSnapshot(t *TableInfo) *TableInfo {
	newT := t.copy()

	s.cacheNewTable(newT)

	newT.IsNew = t.IsNew

	return newT
}

func (s *session) mysqlDropColumnRollback(field FieldInfo) {
	if s.opt.Check {
		return
	}

	buf := bytes.NewBufferString("ADD COLUMN `")
	buf.WriteString(field.Field)
	buf.WriteString("` ")
	buf.WriteString(field.Type)
	if field.Null == "NO" {
		buf.WriteString(" NOT NULL")
	}
	if field.Default != nil {
		buf.WriteString(" DEFAULT '")
		buf.WriteString(*field.Default)
		buf.WriteString("'")
	}
	if field.Comment != "" {
		buf.WriteString(" COMMENT '")
		buf.WriteString(field.Comment)
		buf.WriteString("'")
	}
	buf.WriteString(",")

	// s.myRecord.DDLRollback += buf.String()
	s.alterRollbackBuffer = append(s.alterRollbackBuffer, buf.String())
}

func (s *session) checkDropIndex(node *ast.DropIndexStmt, sql string) {
	log.Debug("checkDropIndex")

	t := s.getTableFromCache(node.Table.Schema.O, node.Table.Name.O, true)
	if t == nil {
		return
	}

	s.checkAlterTableDropIndex(t, node.IndexName)

}

func (s *session) checkCreateIndex(table *ast.TableName, IndexName string,
	IndexColNames []*ast.IndexColName, IndexOption *ast.IndexOption,
	t *TableInfo, unique bool, tp ast.ConstraintType) {
	log.Debug("checkCreateIndex")

	if t == nil {
		t = s.getTableFromCache(table.Schema.O, table.Name.O, true)
		if t == nil {
			return
		}
	}

	if s.myRecord.TableInfo == nil {
		s.myRecord.TableInfo = t
	}

	if tp == ast.ConstraintPrimaryKey && IndexName == "" {
		IndexName = "PRIMARY"
	}

	s.checkIndexAttr(tp, IndexName, IndexColNames, t)

	keyMaxLen := 0
	// 禁止使用blob列当索引,所以不再检测blob字段时列是否过长
	isBlobColumn := false
	isOverflowIndexLength := false
	for _, col := range IndexColNames {
		found := false
		var foundField FieldInfo

		for _, field := range t.Fields {
			if strings.EqualFold(field.Field, col.Column.Name.O) {
				found = true
				foundField = field
				break
			}
		}
		if !found {
			s.appendErrorNo(ER_COLUMN_NOT_EXISTED, fmt.Sprintf("%s.%s", t.Name, col.Column.Name.O))
		} else {

			if strings.ToLower(foundField.Type) == "json" {
				s.appendErrorMessage(
					fmt.Sprintf("JSON column '%-.192s' cannot be used in key specification.", foundField.Field))
			}

			if strings.Contains(strings.ToLower(foundField.Type), "blob") {
				isBlobColumn = true
				s.appendErrorNo(ER_BLOB_USED_AS_KEY, foundField.Field)
			}

			columnIndexLength := foundField.getDataBytes(s.dbVersion, s.inc.DefaultCharset)

			// Length must be specified for BLOB and TEXT column indexes.
			// if types.IsTypeBlob(col.FieldType.Tp) && ic.Length == types.UnspecifiedLength {
			// 	return nil, errors.Trace(errBlobKeyWithoutLength)
			// }

			if col.Length != types.UnspecifiedLength {
				if !strings.Contains(strings.ToLower(foundField.Type), "blob") &&
					!strings.Contains(strings.ToLower(foundField.Type), "char") &&
					!strings.Contains(strings.ToLower(foundField.Type), "text") {
					s.appendErrorNo(ER_WRONG_SUB_KEY)
					col.Length = types.UnspecifiedLength
				}

				if (strings.Contains(strings.ToLower(foundField.Type), "blob") ||
					strings.Contains(strings.ToLower(foundField.Type), "char") ||
					strings.Contains(strings.ToLower(foundField.Type), "text")) &&
					col.Length > columnIndexLength {
					s.appendErrorNo(ER_WRONG_SUB_KEY)
					col.Length = columnIndexLength
				}
			}

			if col.Length == types.UnspecifiedLength {
				keyMaxLen += columnIndexLength
			} else {
				tmpField := &FieldInfo{
					Field:     foundField.Field,
					Type:      fmt.Sprintf("%s(%d)", GetDataTypeBase(foundField.Type), col.Length),
					Collation: foundField.Collation,
				}

				columnIndexLength = tmpField.getDataLength(s.dbVersion, s.inc.DefaultCharset)
				keyMaxLen += columnIndexLength

				// bysPerChar := 3
				// charset := s.Inc.DefaultCharset
				// if foundField.Collation != "" {
				// 	charset = strings.SplitN(foundField.Collation, "_", 2)[0]
				// }
				// if _, ok := charSets[strings.ToLower(charset)]; ok {
				// 	bysPerChar = charSets[strings.ToLower(charset)]
				// }
				// keyMaxLen += col.Length * bysPerChar

				// if foundField.Collation == "" || strings.HasPrefix(foundField.Collation, "utf8mb4") {
				// 	keyMaxLen += col.Length * 4
				// } else {
				// 	keyMaxLen += col.Length * 3
				// }
			}

			if !s.innodbLargePrefix && !isOverflowIndexLength &&
				!isBlobColumn &&
				columnIndexLength > maxKeyLength {
				s.appendErrorNo(ER_TOO_LONG_KEY, IndexName, maxKeyLength)
				isOverflowIndexLength = true
			}

			if tp == ast.ConstraintPrimaryKey {
				fieldType := GetDataTypeBase(strings.ToLower(foundField.Type))

				// if !strings.Contains(strings.ToLower(foundField.Type), "int") {
				if fieldType != "mediumint" && fieldType != "int" &&
					fieldType != "bigint" {
					s.appendErrorNo(ER_PK_COLS_NOT_INT, foundField.Field, t.Schema, t.Name)
				}

				if foundField.Null == "YES" {
					s.appendErrorNo(ER_PRIMARY_CANT_HAVE_NULL)
				}
			} else if tp == ast.ConstraintSpatial {
				if foundField.Null == "YES" {
					s.appendErrorMessage("All parts of a SPATIAL index must be NOT NULL")
				}
			}

		}
	}

	if len(IndexName) > mysql.MaxIndexIdentifierLen {
		s.appendErrorMessage(fmt.Sprintf("表'%s'的索引'%s'名称过长", t.Name, IndexName))
	}

	if !isBlobColumn && !isOverflowIndexLength {
		// --删除!-- mysql 5.6版本索引长度限制是767,5.7及之后变为3072
		// 未开启innodbLargePrefix时,单列长度不能超过767
		// 所有情况下,总长度不能超过3072
		if keyMaxLen > maxKeyLength57 {
			s.appendErrorNo(ER_TOO_LONG_KEY, IndexName, maxKeyLength57)
		}

		// if s.innodbLargePrefix && keyMaxLen > maxKeyLength57 {
		// 	s.AppendErrorNo(ER_TOO_LONG_KEY, IndexName, maxKeyLength57)
		// } else if !s.innodbLargePrefix && keyMaxLen > maxKeyLength {
		// 	s.AppendErrorNo(ER_TOO_LONG_KEY, IndexName, maxKeyLength)
		// }
	}

	if IndexOption != nil {
		// 注释长度校验
		if len(IndexOption.Comment) > INDEX_COMMENT_MAXLEN {
			s.appendErrorNo(ER_TOO_LONG_INDEX_COMMENT, IndexName, INDEX_COMMENT_MAXLEN)
		}
	}

	rows := t.Indexes

	if len(rows) > 0 {
		for _, row := range rows {
			if s.inc.EnableNullIndexName && row.IndexName == "" {
				continue
			}
			if strings.EqualFold(row.IndexName, IndexName) && !row.IsDeleted {
				s.appendErrorNo(ER_DUP_INDEX, IndexName, t.Schema, t.Name)
				break
			}
		}
	}

	key_count := 0
	for _, row := range rows {
		if row.Seq == 1 && !row.IsDeleted {
			key_count++
		}
	}

	if s.inc.MaxKeys > 0 && key_count >= int(s.inc.MaxKeys) {
		s.appendErrorNo(ER_TOO_MANY_KEYS, t.Name, s.inc.MaxKeys)
	}
	// }

	if s.hasError() {
		return
	}

	indexType := "BTREE"
	if tp == ast.ConstraintSpatial {
		indexType = "SPATIAL"
	}
	// cache new index
	for i, col := range IndexColNames {
		index := &IndexInfo{
			Table: t.Name,
			// NonUnique:  unique  ,
			IndexName:  IndexName,
			Seq:        i + 1,
			ColumnName: col.Column.Name.O,
			IndexType:  indexType,
		}
		if !unique && (tp == ast.ConstraintPrimaryKey || tp == ast.ConstraintUniq ||
			tp == ast.ConstraintUniqIndex || tp == ast.ConstraintUniqKey) {
			unique = true
		}
		if unique {
			index.NonUnique = 0
		} else {
			index.NonUnique = 1
		}
		t.Indexes = append(t.Indexes, index)
	}

	// !t.IsNew &&
	if s.opt.Execute {
		var rollbackSql string
		if IndexName == "PRIMARY" {
			rollbackSql = fmt.Sprintf("DROP PRIMARY KEY,")
		} else {
			rollbackSql = fmt.Sprintf("DROP INDEX `%s`,", IndexName)
		}
		s.myRecord.DDLRollback = fmt.Sprintf("DROP INDEX `%s` ON `%s`.`%s`;",
			IndexName, t.Schema, t.Name)
		s.alterRollbackBuffer = append(s.alterRollbackBuffer, rollbackSql)
	}
}

func (s *session) checkAddConstraint(t *TableInfo, c *ast.AlterTableSpec) {
	log.Debug("checkAddConstraint")

	switch c.Constraint.Tp {
	case ast.ConstraintKey, ast.ConstraintIndex,
		ast.ConstraintSpatial, ast.ConstraintFulltext:
		s.checkCreateIndex(nil, c.Constraint.Name,
			c.Constraint.Keys, c.Constraint.Option, t, false, c.Constraint.Tp)
	case ast.ConstraintUniq, ast.ConstraintUniqIndex, ast.ConstraintUniqKey:
		s.checkCreateIndex(nil, c.Constraint.Name,
			c.Constraint.Keys, c.Constraint.Option, t, true, c.Constraint.Tp)

	case ast.ConstraintPrimaryKey:
		s.checkCreateIndex(nil, "PRIMARY",
			c.Constraint.Keys, c.Constraint.Option, t, true, c.Constraint.Tp)
	case ast.ConstraintForeignKey:
		s.checkCreateForeignKey(t, c.Constraint)
	default:
		s.appendErrorNo(ER_NOT_SUPPORTED_YET)
		log.Info("con:", s.sessionVars.ConnectionID, " 未定义的解析: ", c.Constraint.Tp)
	}
}

func (s *session) checkDBExists(db string, reportNotExists bool) bool {

	if db == "" {
		db = s.dbName
	}

	if db == "" {
		s.appendErrorNo(ER_WRONG_DB_NAME, "")
		return false
	}

	key := db
	if s.IgnoreCase() {
		key = strings.ToLower(db)
	}
	if v, ok := s.dbCacheList[key]; ok {
		return !v.IsDeleted
	}

	sql := "show databases like '%s';"

	// count:= s.Exec(fmt.Sprintf(sql,db)).AffectedRows
	var name string

	rows, err := s.raw(fmt.Sprintf(sql, db))
	if rows != nil {
		defer rows.Close()
	}
	if err != nil {
		log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
		if myErr, ok := err.(*mysqlDriver.MySQLError); ok {
			s.appendErrorMessage(myErr.Message)
		} else {
			s.appendErrorMessage(err.Error())
		}
	} else {
		for rows.Next() {
			rows.Scan(&name)
		}
	}

	if name == "" {
		if reportNotExists {
			s.appendErrorNo(ER_DB_NOT_EXISTED_ERROR, db)
		}
		return false
	} else {
		s.dbCacheList[key] = &DBInfo{
			Name:      db,
			IsNew:     false,
			IsDeleted: false,
		}

		return true
	}

}

func (s *session) checkInsert(node *ast.InsertStmt, sql string) {

	log.Debug("checkInsert")

	// sqlId, ok := s.checkFingerprint(strings.Replace(strings.ToLower(sql), "values", "values ", 1))
	// if ok {
	// 	return
	// }

	x := node

	fieldCount := len(x.Columns)

	if fieldCount == 0 {
		s.appendErrorNo(ER_WITH_INSERT_FIELD)
	}

	t := getSingleTableName(x.Table)

	for _, c := range x.Columns {
		if c.Schema.O == "" {
			c.Schema = model.NewCIStr(s.dbName)
		}
		if c.Table.O == "" {
			c.Table = model.NewCIStr(t.Name.O)
		}
	}

	table := s.getTableFromCache(t.Schema.O, t.Name.O, true)
	if table == nil {
		return
	}

	// 校验列是否重复指定
	if fieldCount > 0 {
		checkDup := map[string]bool{}
		for _, c := range x.Columns {
			if _, ok := checkDup[c.Name.L]; ok {
				s.appendErrorNo(ER_FIELD_SPECIFIED_TWICE, c.Name, c.Table)
			}
			checkDup[c.Name.L] = true
		}
	}

	s.myRecord.TableInfo = table
	if fieldCount == 0 {
		// fieldCount = len(table.Fields)
		for _, field := range table.Fields {
			if !field.IsDeleted {
				fieldCount++
			}
		}
	}

	columnsCannotNull := map[string]bool{}

	for _, c := range x.Columns {
		found := false
		for _, field := range table.Fields {
			if strings.EqualFold(field.Field, c.Name.O) && !field.IsDeleted {
				found = true
				if field.Null == "NO" && !strings.Contains(field.Extra, "auto_increment") {
					columnsCannotNull[c.Name.L] = true
				}
				break
			}
		}
		if !found {
			s.appendErrorNo(ER_COLUMN_NOT_EXISTED, fmt.Sprintf("%s.%s", c.Table, c.Name))
		}
	}

	if len(x.Lists) > 0 {

		if s.inc.MaxInsertRows > 0 && len(x.Lists) > int(s.inc.MaxInsertRows) {
			s.appendErrorNo(ER_INSERT_TOO_MUCH_ROWS,
				len(x.Lists), s.inc.MaxInsertRows)
		}

		// 审核列数是否匹配,是否为not null字段指定了NULL值
		for i, list := range x.Lists {
			if len(list) == 0 {
				s.appendErrorNo(ER_WITH_INSERT_VALUES)
			} else if len(list) != fieldCount {
				s.appendErrorNo(ER_WRONG_VALUE_COUNT_ON_ROW, i+1)
			} else if len(x.Columns) > 0 {
				for colIndex, vv := range list {

					s.checkItem(vv, []*TableInfo{table})

					if v, ok := vv.(*ast.ValueExpr); ok {
						name := x.Columns[colIndex].Name.L
						if _, ok := columnsCannotNull[name]; ok && v.Type.Tp == mysql.TypeNull {
							s.appendErrorNo(ER_BAD_NULL_ERROR, x.Columns[colIndex], i+1)
						}
					}
				}
			}
		}
		s.myRecord.AffectedRows = len(x.Lists)
	} else if x.Select == nil {
		s.appendErrorNo(ER_WITH_INSERT_VALUES)
	}

	if s.hasError() {
		return
	}

	// insert select 语句
	if x.Select != nil {
		sel, ok := x.Select.(*ast.SelectStmt)
		if !ok {
			if u, ok := x.Select.(*ast.UnionStmt); ok {
				sel = u.SelectList.Selects[0]
			}
		}

		if sel != nil {

			// 是否有星号列
			isWildCard := false
			for _, f := range sel.Fields.Fields {
				if f.WildCard != nil {
					isWildCard = true
					break
				}
			}

			if isWildCard {
				s.appendErrorNo(ER_SELECT_ONLY_STAR)

				selectColumnCount, err := s.subSelectColumns(sel)

				if err == nil && fieldCount != selectColumnCount {
					s.appendErrorNo(ER_WRONG_VALUE_COUNT_ON_ROW, 1)
				}
			} else if fieldCount != len(sel.Fields.Fields) {
				// 判断字段数是否匹配
				s.appendErrorNo(ER_WRONG_VALUE_COUNT_ON_ROW, 1)
			}

			var tableList []*ast.TableSource
			var tableInfoList []*TableInfo
			tableList = extractTableList(x.Select, tableList)

			// 判断select中是否有新表
			haveNewTable := false
			for _, tblSource := range tableList {
				tblName, ok := tblSource.Source.(*ast.TableName)
				if !ok {
					cols := s.getSubSelectColumns(tblSource.Source)
					if cols != nil {
						rows := make([]FieldInfo, len(cols))
						for i, colName := range cols {
							rows[i].Field = colName
						}
						t := &TableInfo{
							Schema: "",
							Name:   tblSource.AsName.String(),
							Fields: rows,
						}
						tableInfoList = append(tableInfoList, t)
					}
					continue
				}

				t := s.getTableFromCache(tblName.Schema.O, tblName.Name.O, true)
				if t != nil {
					// if tblSource.AsName.L != "" {
					// 	t.AsName = tblSource.AsName.O
					// }
					// tableInfoList = append(tableInfoList, t)

					if tblSource.AsName.L != "" {
						t.AsName = tblSource.AsName.O
						tableInfoList = append(tableInfoList, t.copy())
					} else {
						tableInfoList = append(tableInfoList, t)
					}
					if t.IsNew {
						haveNewTable = true
					}
				}
			}

			if !s.hasError() {
				// 如果不是新建表时,则直接explain
				if haveNewTable {
					s.checkSelectItem(x.Select, sel.Where != nil)
				} else {
					var selectSql string
					if table.IsNew || table.IsNewColumns || s.dbVersion < 50600 {
						i := strings.Index(strings.ToLower(sql), "select")
						selectSql = sql[i:]
					} else {
						selectSql = sql
					}

					s.explainOrAnalyzeSql(selectSql)

					if sel.From == nil && s.myRecord.AffectedRows == 0 {
						s.myRecord.AffectedRows = 1
					}
				}
			}

			if sel.Where == nil {
				s.appendErrorNo(ER_NO_WHERE_CONDITION)
			}

			if sel.Limit != nil {
				s.appendErrorNo(ER_WITH_LIMIT_CONDITION)
			}

			if sel.OrderBy != nil {
				for _, item := range sel.OrderBy.Items {
					if f, ok := item.Expr.(*ast.FuncCallExpr); ok {
						if f.FnName.L == ast.Rand {
							s.appendErrorNo(ER_ORDERY_BY_RAND)
						}
					}
				}
			}
		}
	}

	// if len(node.Setlist) > 0 {
	// 	s.AppendErrorNo(ER_NOT_SUPPORTED_YET)
	// 	for _, v := range node.Setlist {
	// 		log.Info(v.Column)
	// 	}
	// }

	// s.saveFingerprint(sqlId)
}

// getTableList 根据表对象获取访问的所有表，并判断是否存在新表以避免explain失败
func (s *session) getTableList(tableList []*ast.TableSource) ([]*TableInfo, bool) {
	var tableInfoList []*TableInfo

	// 判断select中是否有新表
	haveNewTable := false
	for _, tblSource := range tableList {
		tblName, ok := tblSource.Source.(*ast.TableName)
		if ok {
			t := s.getTableFromCache(tblName.Schema.O, tblName.Name.O, true)
			if t != nil {
				if tblSource.AsName.L != "" {
					t.AsName = tblSource.AsName.O
					tableInfoList = append(tableInfoList, t.copy())
				} else {
					tableInfoList = append(tableInfoList, t)
				}
				if t.IsNew {
					haveNewTable = true
				}
			}
		} else {
			cols := s.getSubSelectColumns(tblSource.Source)
			if cols != nil {
				rows := make([]FieldInfo, len(cols))
				for i, colName := range cols {
					rows[i].Field = colName
				}
				t := &TableInfo{
					Schema: "",
					Name:   tblSource.AsName.String(),
					Fields: rows,
				}
				tableInfoList = append(tableInfoList, t)
			}
		}
	}
	return tableInfoList, haveNewTable
}

// subSelectColumns 计算子查询的列数(包含有星号列)
func (s *session) subSelectColumns(node ast.ResultSetNode) (int, error) {
	switch sel := node.(type) {
	case *ast.UnionStmt:
		// 取第一个select的列数
		return s.subSelectColumns(sel.SelectList.Selects[0])

	case *ast.SelectStmt:

		// from为空时,直接走explain,sql可能是错误的
		if sel.From == nil {
			return 0, errors.New("no from clause")
		}

		var tableList []*ast.TableSource
		tableList = extractTableList(sel.From.TableRefs, tableList)

		// 获取总列数,并校验表是否都已存在
		totalFieldCount := 0
		for _, tblSource := range tableList {
			tblName, ok := tblSource.Source.(*ast.TableName)
			if !ok {
				continue
			}
			if tblName.Schema.L == "" {
				tblName.Schema = model.NewCIStr(s.dbName)
			}
			t := s.getTableFromCache(tblName.Schema.O, tblName.Name.O, true)
			if t != nil {
				// totalFieldCount += len(t.Fields)
				for _, field := range t.Fields {
					if !field.IsDeleted {
						totalFieldCount++
					}
				}
			} else {
				// return
			}
		}

		selectColumnCount := 0
		for _, f := range sel.Fields.Fields {
			if f.WildCard == nil {
				selectColumnCount++
			} else {
				db := f.WildCard.Schema.L
				wildTable := f.WildCard.Table.L
				if wildTable == "" {
					selectColumnCount += totalFieldCount
				} else {
					found := false
					for _, tblSource := range tableList {
						var tName string
						tblName, ok := tblSource.Source.(*ast.TableName)

						if tblSource.AsName.L != "" {
							tName = tblSource.AsName.L
						} else if ok {
							tName = tblName.Name.L
						}

						if (ok && (db == "" || db == tblName.Schema.L) &&
							wildTable == tName) ||
							(!ok && wildTable == tName) {
							if ok {
								t := s.getTableFromCache(tblName.Schema.O, tblName.Name.O, false)
								if t != nil {
									// selectColumnCount += len(t.Fields)
									for _, field := range t.Fields {
										if !field.IsDeleted {
											selectColumnCount++
										}
									}
								}
							} else {
								length, err := s.subSelectColumns(tblSource.Source)
								if err != nil {
									return 0, err
								}
								selectColumnCount += length
							}
							found = true
							break
						}
					}
					// 别名未找到,说明sql语句有问题,则直接做explain即可
					if !found {
						return 0, errors.New("not found")
					}
				}
			}
		}
		return selectColumnCount, nil
	default:
		// log.Error("未处理的类型: %#v", sel)
		log.Errorf("con:%d %v", s.sessionVars.ConnectionID, sel)
		// log.Error(sel)
	}
	return 0, errors.New("未处理的类型")
}

func (s *session) getSubSelectColumns(node ast.ResultSetNode) []string {
	columns := []string{}

	switch sel := node.(type) {
	case *ast.UnionStmt:
		// 取第一个select的列数
		return s.getSubSelectColumns(sel.SelectList.Selects[0])

	case *ast.SelectStmt:
		// var tableList []*ast.TableSource
		// var tableInfoList []*TableInfo

		// from为空时,直接走explain,sql可能是错误的
		if sel.From == nil {
			// return nil, errors.New("no from clause")
			for _, f := range sel.Fields.Fields {
				if f.AsName.L != "" {
					columns = append(columns, f.AsName.String())
				} else {
					switch e := f.Expr.(type) {
					case *ast.ColumnNameExpr:
						columns = append(columns, e.Name.Name.String())
					// case *ast.VariableExpr:
					//  todo ...
					// 	log.Infof("con:%d %#v", s.sessionVars.ConnectionID, e)
					default:
						log.Infof("con:%d %T", s.sessionVars.ConnectionID, e)
					}
				}
			}
		} else {

			var tableList []*ast.TableSource
			tableList = extractTableList(sel.From.TableRefs, tableList)
			// tableInfoList = s.getTableInfoByTableSource(tableList)

			// if sel.From.TableRefs.On != nil {
			// 	s.checkItem(sel.From.TableRefs.On.Expr, tableInfoList)
			// }

			for _, f := range sel.Fields.Fields {
				if f.WildCard == nil {
					// log.Infof("%#v", f)
					if f.AsName.L != "" {
						columns = append(columns, f.AsName.String())
					} else {
						switch e := f.Expr.(type) {
						case *ast.ColumnNameExpr:
							columns = append(columns, e.Name.Name.String())
						default:
							log.Infof("con:%d %T", s.sessionVars.ConnectionID, e)
						}
					}
				} else {

					db := f.WildCard.Schema.L
					wildTable := f.WildCard.Table.L

					if wildTable == "" {
						for _, tblSource := range tableList {
							tblName, ok := tblSource.Source.(*ast.TableName)
							if ok {
								if tblName.Schema.L == "" {
									tblName.Schema = model.NewCIStr(s.dbName)
								}
								t := s.getTableFromCache(tblName.Schema.O, tblName.Name.O, true)
								if t != nil {
									for _, field := range t.Fields {
										columns = append(columns, field.Field)
									}
								}
							} else {
								cols := s.getSubSelectColumns(tblSource.Source)
								if cols != nil {
									columns = append(columns, cols...)
								}
							}
						}
					} else {
						for _, tblSource := range tableList {
							var tName string
							tblName, ok := tblSource.Source.(*ast.TableName)

							if tblSource.AsName.L != "" {
								tName = tblSource.AsName.L
							} else if ok {
								tName = tblName.Name.L
							}

							if (ok && (db == "" || db == tblName.Schema.L) &&
								wildTable == tName) ||
								(!ok && wildTable == tName) {
								if ok {
									t := s.getTableFromCache(tblName.Schema.O, tblName.Name.O, false)
									if t != nil {
										for _, field := range t.Fields {
											columns = append(columns, field.Field)
										}
									}
								} else {
									cols := s.getSubSelectColumns(tblSource.Source)
									if cols != nil {
										columns = append(columns, cols...)
									}
								}
							}
						}
					}
				}
			}
		}

		// for _, t := range tableInfoList {
		// 	log.Info(t.Name)
		// }
		// log.Infof("%#v", columns)

		// if sel.Fields != nil {
		// 	for _, field := range sel.Fields.Fields {
		// 		if field.WildCard == nil {
		// 			s.checkItem(field.Expr, tableInfoList)
		// 		}
		// 	}
		// }

		// if sel.GroupBy != nil {
		// 	for _, item := range sel.GroupBy.Items {
		// 		s.checkItem(item.Expr, tableInfoList)
		// 	}
		// }

		// if sel.Having != nil {
		// 	s.checkItem(sel.Having.Expr, tableInfoList)
		// }

		// if sel.OrderBy != nil {
		// 	for _, item := range sel.OrderBy.Items {
		// 		s.checkItem(item.Expr, tableInfoList)
		// 	}
		// }

		return columns

	default:
		// log.Error("未处理的类型: %#v", sel)
		// log.Error(sel)
		log.Errorf("con:%d %v", s.sessionVars.ConnectionID, sel)
	}

	// log.Infof("%#v", columns)
	return columns
}

func (s *session) checkDropDB(node *ast.DropDatabaseStmt, sql string) {
	log.Debug("checkDropDB")

	if !s.inc.EnableDropDatabase {
		s.appendErrorNo(ER_CANT_DROP_DATABASE, node.Name)
		return
	}

	if s.checkDBExists(node.Name, !node.IfExists) {
		// if s.opt.execute {
		// 	// 生成回滚语句
		// 	s.mysqlShowCreateDatabase(node.Name)
		// }
		s.dbCacheList[strings.ToLower(node.Name)].IsDeleted = true
	}
}

func (s *session) executeInceptionSet(node *ast.InceptionSetStmt, sql string) ([]sqlexec.RecordSet, error) {
	log.Debug("executeInceptionSet")

	for _, v := range node.Variables {
		if !v.IsSystem {
			return nil, errors.New("无效参数")
		}

		if v.IsGlobal && (s.haveBegin || s.isAPI) {
			return nil, errors.New("全局变量仅支持单独设置")
		}

		// 非本地模式时,只使用全局设置
		if !s.haveBegin && !s.isAPI {
			v.IsGlobal = true
		}

		var value *ast.ValueExpr

		switch expr := v.Value.(type) {
		case *ast.ValueExpr:
			value = expr
		case *ast.UnaryOperationExpr:
			value, _ = expr.V.(*ast.ValueExpr)
			if expr.Op == opcode.Minus {
				value.Datum = types.NewIntDatum(value.GetInt64() * -1)
			}
		default:
			return nil, errors.New("参数值无效")
		}

		cnf := config.GetGlobalConfig()

		if v.IsLevel {
			if s.haveBegin || s.isAPI {
				return nil, errors.New("暂不支持会话级的自定义审核级别")
			}
			err := s.setVariableValue(reflect.TypeOf(cnf.IncLevel), reflect.ValueOf(&cnf.IncLevel).Elem(), v.Name, value)
			if err != nil {
				return nil, err
			}
			continue
		}

		// t := reflect.TypeOf(cnf.Inc)
		// values := reflect.ValueOf(&cnf.Inc).Elem()
		prefix := strings.ToLower(v.Name)
		if strings.Contains(prefix, "_") {
			prefix = strings.Split(prefix, "_")[0]
		}

		var err error
		switch prefix {
		case "osc":
			var object *config.Osc
			if v.IsGlobal {
				object = &cnf.Osc
			} else {
				object = &s.osc
			}
			err = s.setVariableValue(reflect.TypeOf(*object), reflect.ValueOf(object).Elem(), v.Name, value)
			if err != nil {
				return nil, err
			}

		case "ghost":
			var object *config.Ghost
			if v.IsGlobal {
				object = &cnf.Ghost
			} else {
				object = &s.ghost
			}
			err = s.setVariableValue(reflect.TypeOf(*object), reflect.ValueOf(object).Elem(), v.Name, value)
			if err != nil {
				return nil, err
			}

		default:
			if prefix == "version" {
				return nil, errors.New("只读变量")
			}
			var object *config.Inc
			if v.IsGlobal {
				object = &cnf.Inc
			} else {
				object = &s.inc
			}
			err = s.setVariableValue(reflect.TypeOf(*object), reflect.ValueOf(object).Elem(), v.Name, value)
			if err != nil {
				return nil, err
			}
			if prefix == "lang" {
				s.inc.Lang = strings.Replace(strings.ToLower(s.inc.Lang), "-", "_", 1)
			}
		}
	}

	return nil, nil
}

func (s *session) setVariableValue(t reflect.Type, values reflect.Value,
	name string, value *ast.ValueExpr) error {

	found := false
	for i := 0; i < values.NumField(); i++ {
		if values.Field(i).CanInterface() { //判断是否为可导出字段
			if k := t.Field(i).Tag.Get("toml"); strings.EqualFold(k, name) ||
				strings.EqualFold(t.Field(i).Name, name) {
				err := s.setConfigValue(name, values.Field(i), &(value.Datum))
				if err != nil {
					return err
				}
				found = true
				break
			}
		}
	}
	if !found {
		return errors.New("无效参数")
	}
	return nil
}

func (s *session) setLevelValue(t reflect.Type, values reflect.Value,
	name string, value *ast.ValueExpr) error {

	found := false
	for i := 0; i < values.NumField(); i++ {
		if values.Field(i).CanInterface() { //判断是否为可导出字段
			if k := t.Field(i).Tag.Get("toml"); strings.EqualFold(k, name) ||
				strings.EqualFold(t.Field(i).Name, name) {
				err := s.setConfigValue(name, values.Field(i), &(value.Datum))
				if err != nil {
					return err
				}
				found = true
				break
			}
		}
	}
	if !found {
		return errors.New("无效参数")
	}
	return nil
}

func (s *session) checkUInt64SystemVar(name, value string, min, max uint64) (string, error) {
	if value[0] == '-' {
		_, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return value, ErrWrongTypeForVar.GenWithStackByArgs(name)
		}
		s.sessionVars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.GenWithStackByArgs(name, value))
		return fmt.Sprintf("%d", min), nil
	}
	val, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(name)
	}
	if val < min {
		s.sessionVars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.GenWithStackByArgs(name, value))
		return fmt.Sprintf("%d", min), nil
	}
	if val > max {
		s.sessionVars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.GenWithStackByArgs(name, value))
		return fmt.Sprintf("%d", max), nil
	}
	return value, nil
}

func (s *session) checkInt64SystemVar(name, value string, min, max int64) (string, error) {
	val, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return value, ErrWrongTypeForVar.GenWithStackByArgs(name)
	}
	if val < min {
		s.sessionVars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.GenWithStackByArgs(name, value))
		return fmt.Sprintf("%d", min), nil
	}
	if val > max {
		s.sessionVars.StmtCtx.AppendWarning(ErrTruncatedWrongValue.GenWithStackByArgs(name, value))
		return fmt.Sprintf("%d", max), nil
	}
	return value, nil
}

func (s *session) setConfigValue(name string, field reflect.Value, value *types.Datum) error {

	sVal := ""
	var err error
	if !value.IsNull() {
		sVal, err = value.ToString()
	}
	if err != nil {
		return err
	}

	switch field.Type().String() {
	case reflect.String.String():
		field.SetString(sVal)

	case reflect.Uint.String(), reflect.Uint8.String(), reflect.Uint16.String(),
		reflect.Uint32.String(), reflect.Uint64.String():
		// field.SetUint(value.GetUint64())
		v, err := s.checkUInt64SystemVar(name, sVal, 0, math.MaxUint64)
		if err != nil {
			return err
		}

		v1, _ := strconv.ParseUint(v, 10, 64)
		field.SetUint(v1)

	case reflect.Int.String(), reflect.Int8.String(), reflect.Int16.String(),
		reflect.Int32.String(), reflect.Int64.String():
		v, err := s.checkInt64SystemVar(name, sVal, math.MinInt64, math.MaxInt64)
		if err != nil {
			return err
		}

		v1, _ := strconv.ParseInt(v, 10, 64)
		field.SetInt(v1)

	case reflect.Bool.String():
		if strings.EqualFold(sVal, "ON") || sVal == "1" ||
			strings.EqualFold(sVal, "OFF") || sVal == "0" ||
			strings.EqualFold(sVal, "TRUE") || strings.EqualFold(sVal, "FALSE") {
			if strings.EqualFold(sVal, "ON") || sVal == "1" || strings.EqualFold(sVal, "TRUE") {
				field.SetBool(true)
			} else {
				field.SetBool(false)
			}
		} else {
			// s.sessionVars.StmtCtx.AppendError(ErrWrongValueForVar.GenWithStackByArgs(name, sVal))
			return ErrWrongValueForVar.GenWithStackByArgs(name, sVal)
		}
	default:
		field.SetString(sVal)
	}
	return nil
}

func (s *session) showVariables(node *ast.ShowStmt, obj interface{}, res *VariableSets) {

	t := reflect.TypeOf(obj)
	v := reflect.ValueOf(obj)

	var (
		like     string
		patChars []byte
		patTypes []byte
	)
	if node.Pattern != nil {
		if node.Pattern.Pattern != nil {
			va, _ := node.Pattern.Pattern.(*ast.ValueExpr)
			like = va.GetString()
		}
		patChars, patTypes = stringutil.CompilePattern(like, node.Pattern.Escape)
	}

	for i := 0; i < v.NumField(); i++ {
		if v.Field(i).CanInterface() { //判断是否为可导出字段
			if len(like) == 0 {
				if k := t.Field(i).Tag.Get("toml"); k != "" {
					if k == "backup_password" {
						p := auth.EncodePassword(
							fmt.Sprintf("%v", v.Field(i).Interface()))
						res.Append(k, p)
					} else {
						res.Append(k, fmt.Sprintf("%v", v.Field(i).Interface()))
					}
				}
			} else {
				if k := t.Field(i).Tag.Get("toml"); k != "" {
					match := stringutil.DoMatch(k, patChars, patTypes)
					if match && !node.Pattern.Not {
						if k == "backup_password" {
							p := auth.EncodePassword(
								fmt.Sprintf("%v", v.Field(i).Interface()))
							res.Append(k, p)
						} else {
							res.Append(k, fmt.Sprintf("%v", v.Field(i).Interface()))
						}
					} else if !match && node.Pattern.Not {
						if k == "backup_password" {
							p := auth.EncodePassword(
								fmt.Sprintf("%v", v.Field(i).Interface()))
							res.Append(k, p)
						} else {
							res.Append(k, fmt.Sprintf("%v", v.Field(i).Interface()))
						}
					}
				}
			}
		}
	}
}

func (s *session) executeLocalShowVariables(node *ast.ShowStmt) ([]sqlexec.RecordSet, error) {

	res := NewVariableSets(120)
	s.showVariables(node, s.inc, res)
	s.showVariables(node, s.osc, res)
	s.showVariables(node, s.ghost, res)

	s.sessionVars.StmtCtx.AddAffectedRows(uint64(res.rc.count))

	return res.Rows(), nil
}

func (s *session) executeLocalShowProcesslist(node *ast.ShowStmt) ([]sqlexec.RecordSet, error) {
	return nil, nil
	// pl := s.sessionManager.ShowProcessList()

	// var keys []int
	// for k := range pl {
	// 	keys = append(keys, int(k))
	// }
	// sort.Ints(keys)

	// res := NewProcessListSets(len(pl))

	// for _, k := range keys {
	// 	if pi, ok := pl[uint64(k)]; ok {
	// 		var info string
	// 		if node.Full {
	// 			info = pi.Info
	// 		} else {
	// 			info = fmt.Sprintf("%.100v", pi.Info)
	// 		}

	// 		data := []interface{}{
	// 			pi.ID,
	// 			pi.DestUser,
	// 			pi.DestHost,
	// 			pi.DestPort,
	// 			pi.Host,
	// 			pi.Command,
	// 			pi.OperState,
	// 			int64(time.Since(pi.Time) / time.Second),
	// 			info,
	// 		}
	// 		if pi.Percent > 0 {
	// 			data = append(data, fmt.Sprintf("%.2f%%", pi.Percent*100))
	// 		}
	// 		res.appendRow(data)
	// 	}
	// }

	// s.sessionVars.StmtCtx.AddAffectedRows(uint64(res.rc.count))
	// return res.Rows(), nil
}

// splitWhere: 拆分where表达式
func splitWhere(where ast.ExprNode) []ast.ExprNode {
	var conditions []ast.ExprNode
	switch x := where.(type) {
	case nil:
	case *ast.BinaryOperationExpr:
		if x.Op == opcode.LogicAnd {
			conditions = append(conditions, splitWhere(x.L)...)
			conditions = append(conditions, splitWhere(x.R)...)
		} else {
			conditions = append(conditions, x)
		}
	case *ast.ParenthesesExpr:
		conditions = append(conditions, splitWhere(x.Expr)...)
	default:
		conditions = append(conditions, where)
	}
	return conditions
}

// checkColumnName: 检查列是否存在
func (s *session) checkColumnName(expr ast.ExprNode, colNames []string) (colIndex int, err error) {
	colIndex = -1
	if e, ok := expr.(*ast.ColumnNameExpr); ok {
		found := false
		for i, c := range colNames {
			if e.Name.Name.L == c {
				found = true
				colIndex = i
			}
		}
		if !found {
			return colIndex, errors.New(fmt.Sprintf(s.getErrorMessage(ER_COLUMN_NOT_EXISTED), e.Name.Name.String()))
		}
	}
	return colIndex, nil
}

// filterExprNode: 条件筛选
func (s *session) filterExprNode(expr ast.ExprNode, colNames []string, values []string) (bool, error) {
	switch x := expr.(type) {
	case *ast.BinaryOperationExpr:
		switch x.Op {
		case opcode.EQ:
			colIndex, err := s.checkColumnName(x.L, colNames)
			if err != nil {
				return false, err
			}
			if colIndex > -1 {
				if v, ok := x.R.(*ast.ValueExpr); ok {
					sVal, _ := v.ToString()
					if sVal == values[colIndex] {
						return true, nil
					}
				}
			}
		default:
			log.Info(x)
			return false, errors.New("不支持的操作")
		}
	case *ast.PatternLikeExpr:
		colIndex, err := s.checkColumnName(x.Expr, colNames)
		if err != nil {
			return false, err
		}
		if colIndex > -1 {
			if v, ok := x.Pattern.(*ast.ValueExpr); ok {
				like := strings.ToLower(v.GetString())
				patChars, patTypes := stringutil.CompilePattern(like, x.Escape)
				match := stringutil.DoMatch(strings.ToLower(values[colIndex]), patChars, patTypes)
				if match && !x.Not {
					return true, nil
				} else if !match && x.Not {
					return true, nil
				}
			}
		}

	default:
		log.Info(x)
		return false, errors.New("不支持的操作")
	}
	return false, nil
}

// filter: 条件筛选
func (s *session) filter(expr []ast.ExprNode, colNames []string, value []string) (bool, error) {
	for _, e := range expr {
		ok, err := s.filterExprNode(e, colNames, value)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

func (s *session) executeLocalShowLevels(node *ast.ShowStmt) ([]sqlexec.RecordSet, error) {
	log.Debug("executeLocalShowLevels")

	res := NewLevelSets(len(s.incLevel))

	filters := make([]ast.ExprNode, 0)
	if node.Where != nil {
		filters = splitWhere(node.Where)
	}

	if node.Pattern != nil {
		if node.Pattern.Expr == nil {
			node.Pattern.Expr = &ast.ColumnNameExpr{
				Name: &ast.ColumnName{Name: model.NewCIStr("name")},
			}
		}
		filters = append(filters, node.Pattern)
	}

	names := []string{"name", "value", "desc"}

	for i := 1; i < len(ErrorsDefault); i++ {
		code := ErrorCode(i)
		name := code.String()
		if v, ok := s.incLevel[name]; ok {
			if len(filters) > 0 {
				ok, err := s.filter(filters, names, []string{
					name, strconv.Itoa(int(v)), s.getErrorMessage(code),
				})
				if err != nil {
					return nil, err
				}
				if !ok {
					continue
				}
			}
			res.Append(name, int64(v), s.getErrorMessage(code))
		}
	}

	s.sessionVars.StmtCtx.AddAffectedRows(uint64(res.rc.count))
	return res.Rows(), nil
}

func (s *session) executeLocalShowOscProcesslist(node *ast.ShowOscStmt) ([]sqlexec.RecordSet, error) {
	pl := config.GetGlobalConfig().ShowOscProcessList()

	// 根据是否指定sqlsha1控制显示command列
	res := NewOscProcessListSets(len(pl), node.Sqlsha1 != "")

	if node.Sqlsha1 == "" {

		var keys []int
		all := make(map[uint64]*util.OscProcessInfo, len(pl))
		for _, pi := range pl {
			keys = append(keys, int(pi.ID))
			all[pi.ID] = pi
		}
		sort.Ints(keys)

		for _, k := range keys {
			if pi, ok := all[uint64(k)]; ok {
				data := []interface{}{
					pi.Schema,
					pi.Table,
					pi.Command,
					pi.Sqlsha1,
					pi.Percent,
					pi.RemainTime,
					pi.Info,
				}
				res.appendRow(data)
			}
		}
	} else if pi, ok := pl[node.Sqlsha1]; ok {
		data := []interface{}{
			pi.Schema,
			pi.Table,
			// pi.Command,
			pi.Sqlsha1,
			pi.Percent,
			pi.RemainTime,
			pi.Info,
		}
		res.appendRow(data)
	} else {
		s.sessionVars.StmtCtx.AppendWarning(errors.New("osc process not found"))
	}

	s.sessionVars.StmtCtx.AddAffectedRows(uint64(res.rc.count))
	return res.Rows(), nil
}

func (s *session) executeLocalOscKill(node *ast.ShowOscStmt) ([]sqlexec.RecordSet, error) {
	pl := config.GetGlobalConfig().ShowOscProcessList()

	if pi, ok := pl[node.Sqlsha1]; ok {
		if pi.Killed {
			s.sessionVars.StmtCtx.AppendWarning(errors.New("osc process has been aborted"))
		} else {
			pi.Killed = true
		}
	} else {
		return nil, errors.New("osc process not found")
	}

	return nil, nil
}

func (s *session) executeLocalOscPause(node *ast.ShowOscStmt) ([]sqlexec.RecordSet, error) {
	pl := config.GetGlobalConfig().ShowOscProcessList()

	if pi, ok := pl[node.Sqlsha1]; ok {
		if !pi.IsGhost {
			return nil, errors.New("pt-osc process not support pause")
		}

		if pi.Pause {
			s.sessionVars.StmtCtx.AppendWarning(errors.New("osc process has been paused"))
		} else {
			pi.Pause = true
		}
	} else {
		return nil, errors.New("osc process not found")
	}

	return nil, nil
}

func (s *session) executeLocalOscResume(node *ast.ShowOscStmt) ([]sqlexec.RecordSet, error) {
	pl := config.GetGlobalConfig().ShowOscProcessList()

	if pi, ok := pl[node.Sqlsha1]; ok {
		if !pi.IsGhost {
			return nil, errors.New("pt-osc process not support resume")
		}

		if pi.Pause {
			pi.Pause = false
		} else {
			s.sessionVars.StmtCtx.AppendWarning(errors.New("osc process not paused"))
		}
	} else {
		return nil, errors.New("osc process not found")
	}

	return nil, nil
}

func (s *session) executeInceptionShow(sql string) ([]sqlexec.RecordSet, error) {
	log.Debug("executeInceptionShow")

	rows, err := s.raw(sql)
	if rows != nil {
		defer rows.Close()
	}
	if err != nil {
		log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
		if myErr, ok := err.(*mysqlDriver.MySQLError); ok {
			s.appendErrorMessage(myErr.Message)
		} else {
			s.appendErrorMessage(err.Error())
		}
	} else if rows != nil {

		cols, _ := rows.Columns()
		colLength := len(cols)
		var buf strings.Builder
		buf.WriteString(sql)
		buf.WriteString(":\n")

		paramValues := strings.Repeat("? | ", colLength)
		paramValues = strings.TrimRight(paramValues, " | ")

		for rows.Next() {
			// https://kylewbanks.com/blog/query-result-to-map-in-golang
			// Create a slice of interface{}'s to represent each column,
			// and a second slice to contain pointers to each item in the columns slice.
			columns := make([]interface{}, colLength)
			columnPointers := make([]interface{}, colLength)
			for i := range columns {
				columnPointers[i] = &columns[i]
			}

			// Scan the result into the column pointers...
			if err := rows.Scan(columnPointers...); err != nil {
				s.appendErrorMessage(err.Error())
				return nil, nil
			}

			var vv []driver.Value
			for i := range cols {
				val := columnPointers[i].(*interface{})
				vv = append(vv, *val)
			}

			res, err := interpolateParams(paramValues, vv, s.inc.HexBlob)
			if err != nil {
				s.appendErrorMessage(err.Error())
				return nil, nil
			}

			buf.Write(res)
			buf.WriteString("\n")
		}
		s.myRecord.Sql = strings.TrimSpace(buf.String())
	}

	return nil, nil
}

func (s *session) checkCreateDB(node *ast.CreateDatabaseStmt, sql string) {
	log.Debug("checkCreateDB")

	if s.checkDBExists(node.Name, false) {
		if !node.IfNotExists {
			s.appendErrorMessage(fmt.Sprintf("数据库'%s'已存在.", node.Name))
		}
	} else {
		s.checkKeyWords(node.Name)

		for _, opt := range node.Options {
			switch opt.Tp {
			case ast.DatabaseOptionCharset:
				if s.inc.EnableSetCharset {
					s.checkCharset(opt.Value)
				} else {
					s.appendErrorNo(ER_CANT_SET_CHARSET, opt.Value)
				}
			case ast.DatabaseOptionCollate:
				if s.inc.EnableSetCollation {
					s.checkCollation(opt.Value)
				} else {
					s.appendErrorNo(ER_CANT_SET_COLLATION, opt.Value)
				}
			}
		}

		if s.hasError() {
			return
		}

		s.dbCacheList[strings.ToLower(node.Name)] = &DBInfo{
			Name:      node.Name,
			IsNew:     true,
			IsDeleted: false,
		}

		// if s.opt.execute {
		// 	s.myRecord.DDLRollback = fmt.Sprintf("DROP DATABASE `%s`;", node.Name)
		// }
	}
}

func (s *session) checkCharset(charset string) bool {
	if s.inc.SupportCharset != "" {
		for _, item := range strings.Split(s.inc.SupportCharset, ",") {
			if strings.EqualFold(item, charset) {
				return true
			}
		}
		s.appendErrorNo(ErrCharsetNotSupport, s.inc.SupportCharset)
		return false
	}
	return true
}

func (s *session) checkCollation(collation string) bool {
	if s.inc.SupportCollation != "" {
		for _, item := range strings.Split(s.inc.SupportCollation, ",") {
			if strings.EqualFold(item, collation) {
				return true
			}
		}
		s.appendErrorNo(ErrCollationNotSupport, s.inc.SupportCollation)
		return false
	}
	return true
}

func (s *session) checkEngine(engine string) bool {
	if s.inc.SupportEngine != "" {
		for _, item := range strings.Split(s.inc.SupportEngine, ",") {
			if strings.EqualFold(item, engine) {
				return true
			}
		}
		s.appendErrorNo(ErrEngineNotSupport, s.inc.SupportEngine)
		return false
	}
	return true
}

func (s *session) checkChangeDB(node *ast.UseStmt, sql string) {
	log.Debug("checkChangeDB")

	s.dbName = node.DBName

	// 新建库跳过use 切换
	if s.checkDBExists(node.DBName, true) {
		key := node.DBName
		if s.IgnoreCase() {
			key = strings.ToLower(key)
		}
		if v, ok := s.dbCacheList[key]; ok && !v.IsNew {
			_, err := s.exec(fmt.Sprintf("USE `%s`", node.DBName), true)
			if err != nil {
				log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
				if myErr, ok := err.(*mysqlDriver.MySQLError); ok {
					s.appendErrorMessage(myErr.Message)
				} else {
					s.appendErrorMessage(err.Error())
				}
			}
		}
	}
}

func getSingleTableName(tableRefs *ast.TableRefsClause) *ast.TableName {
	if tableRefs == nil || tableRefs.TableRefs == nil || tableRefs.TableRefs.Right != nil {
		return nil
	}
	tblSrc, ok := tableRefs.TableRefs.Left.(*ast.TableSource)
	if !ok {
		return nil
	}
	if tblSrc.AsName.L != "" {
		return nil
	}
	tblName, ok := tblSrc.Source.(*ast.TableName)
	if !ok {
		return nil
	}
	return tblName
}

func (s *session) getExplainInfo(sql string, sqlId string) {

	if s.hasError() {
		return
	}

	var newRecord *Record
	if s.inc.EnableFingerprint && sqlId != "" {
		newRecord = &Record{
			Buf: new(bytes.Buffer),
		}
	}
	r := s.myRecord

	// rows, err := s.Raw(sql)

	// var rowLength Sql.NullInt64

	// if err != nil {
	// 	log.Error(err)
	// 	if myErr, ok := err.(*mysqlDriver.MySQLError); ok {
	// 		s.AppendErrorMessage(myErr.Message)
	// 		if newRecord != nil {
	// 			newRecord.AppendErrorMessage(myErr.Message)
	// 		}
	// 	}
	// } else {
	// 	for rows.Next() {
	// 		var str Sql.NullString
	// 		// | id | select_type | table | partitions | type  | possible_keys | key     | key_len | ref   | rows | filtered | Extra
	// 		if err := rows.Scan(&str, &str, &str, &str, &str, &str, &str, &str, &str, &rowLength, &str, &str); err != nil {
	// 			log.Error(err)
	// 			if myErr, ok := err.(*mysqlDriver.MySQLError); ok {
	// 				s.AppendErrorMessage(myErr.Message)
	// 				if newRecord != nil {
	// 					newRecord.AppendErrorMessage(myErr.Message)
	// 				}
	// 			}
	// 		}
	// 		break
	// 	}
	// 	rows.Close()
	// }

	// if rowLength.Valid {
	// 	r.AffectedRows = int(rowLength.Int64)
	// 	if newRecord != nil {
	// 		newRecord.AffectedRows = r.AffectedRows
	// 	}
	// }

	var rows []ExplainInfo

	// if err := s.db.Raw(sql).Scan(&rows).Error; err != nil {
	if err := s.rawScan(sql, &rows); err != nil {
		if myErr, ok := err.(*mysqlDriver.MySQLError); ok {
			s.appendErrorMessage(myErr.Message)
			if newRecord != nil {
				newRecord.appendErrorMessage(myErr.Message)
			}
		} else {
			s.appendErrorMessage(err.Error())
			if newRecord != nil {
				newRecord.appendErrorMessage(err.Error())
			}
		}
	}

	if len(rows) > 0 {
		if s.inc.ExplainRule == "max" {
			r.AffectedRows = 0
			for _, row := range rows {
				if row.Count > 0 && row.Rows == 0 {
					row.Rows = int(row.Count)
				}
				r.AffectedRows = Max(r.AffectedRows, row.Rows)
			}
		} else {
			if rows[0].Count > 0 && rows[0].Rows == 0 {
				rows[0].Rows = int(rows[0].Count)
			}
			r.AffectedRows = rows[0].Rows
		}

		if newRecord != nil {
			newRecord.AffectedRows = r.AffectedRows
		}
	}

	if s.inc.MaxUpdateRows > 0 && r.AffectedRows > int(s.inc.MaxUpdateRows) {
		switch r.Type.(type) {
		case *ast.DeleteStmt, *ast.UpdateStmt:
			s.appendErrorNo(ER_UDPATE_TOO_MUCH_ROWS,
				r.AffectedRows, s.inc.MaxUpdateRows)
			if newRecord != nil {
				newRecord.appendErrorNo(s.inc.Lang, ER_UDPATE_TOO_MUCH_ROWS,
					r.AffectedRows, s.inc.MaxUpdateRows)
			}
		}
	}

	if newRecord != nil {
		s.sqlFingerprint[sqlId] = newRecord
	}
}

// getRealRowCount: 获取真正的受影响行数
func (s *session) getRealRowCount(sql string, sqlId string) {

	if s.hasError() {
		return
	}

	// var newRecord *Record
	// if s.Inc.EnableFingerprint && sqlId != "" {
	// 	newRecord = &Record{
	// 		Buf: new(bytes.Buffer),
	// 	}
	// }
	r := s.myRecord

	var value int
	rows, err := s.raw(sql)
	if rows != nil {
		defer rows.Close()
	}

	if err != nil {
		log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
		if myErr, ok := err.(*mysqlDriver.MySQLError); ok {
			s.appendErrorMessage(myErr.Message)
			// if newRecord != nil {
			// 	newRecord.AppendErrorMessage(myErr.Message)
			// }
		} else {
			s.appendErrorMessage(err.Error())
			// if newRecord != nil {
			// 	newRecord.AppendErrorMessage(myErr.Message)
			// }
		}
		return
	} else {
		for rows.Next() {
			rows.Scan(&value)
		}
	}

	r.AffectedRows = value
	// if newRecord != nil {
	// 	newRecord.AffectedRows = r.AffectedRows
	// }

	if s.inc.MaxUpdateRows > 0 && r.AffectedRows > int(s.inc.MaxUpdateRows) {
		switch r.Type.(type) {
		case *ast.DeleteStmt, *ast.UpdateStmt:
			s.appendErrorNo(ER_UDPATE_TOO_MUCH_ROWS,
				r.AffectedRows, s.inc.MaxUpdateRows)
			// if newRecord != nil {
			// 	newRecord.AppendErrorNo(ER_UDPATE_TOO_MUCH_ROWS,
			// 		r.AffectedRows, s.Inc.MaxUpdateRows)
			// }
		}
	}

	// if newRecord != nil {
	// 	s.sqlFingerprint[sqlId] = newRecord
	// }
}

func (s *session) explainOrAnalyzeSql(sql string) {

	// // 如果没有表结构,或者新增表 or 新增列时,不做explain
	// if s.myRecord.TableInfo == nil || s.myRecord.TableInfo.IsNew ||
	// 	s.myRecord.TableInfo.IsNewColumns {
	// 	return
	// }

	sqlId, ok := s.checkFingerprint(sql)
	if ok {
		return
	}

	if s.opt.RealRowCount {
		// dml转换成select
		rw, err := NewRewrite(sql)
		if err != nil {
			log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
			s.appendErrorMessage(err.Error())
		} else {
			err = rw.RewriteDML2Select()
			if err != nil {
				log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
				s.appendErrorMessage(err.Error())
			} else {
				sql = rw.select2Count()
				s.getRealRowCount(sql, sqlId)
			}
		}
		return
	} else {
		if s.dbVersion < 50600 {
			rw, err := NewRewrite(sql)
			if err != nil {
				log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
				s.appendErrorMessage(err.Error())
			} else {
				err = rw.RewriteDML2Select()
				if err != nil {
					log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
					s.appendErrorMessage(err.Error())
				} else {
					sql = rw.SQL
					if sql == "" {
						return
					}
				}
			}
		}

		var explain []string

		if s.isMiddleware() {
			explain = append(explain, s.opt.middlewareExtend)
		}

		explain = append(explain, "EXPLAIN ")
		explain = append(explain, sql)

		// rows := s.getExplainInfo(strings.Join(explain, ""))
		s.getExplainInfo(strings.Join(explain, ""), sqlId)
	}
}

func (s *session) anlyzeExplain(rows []ExplainInfo) {
	r := s.myRecord
	if len(rows) > 0 {
		r.AffectedRows = rows[0].Rows
	}
	if s.inc.MaxUpdateRows > 0 && r.AffectedRows > int(s.inc.MaxUpdateRows) {
		switch r.Type.(type) {
		case *ast.DeleteStmt, *ast.UpdateStmt:
			s.appendErrorNo(ER_UDPATE_TOO_MUCH_ROWS,
				r.AffectedRows, s.inc.MaxUpdateRows)
		}
	}
}

func (s *session) checkUpdate(node *ast.UpdateStmt, sql string) {
	log.Debug("checkUpdate")

	// 从set列表读取要更新的表
	var originTable string
	var firstColumnName string
	if node.List != nil {
		for _, l := range node.List {
			if firstColumnName == "" {
				originTable = l.Column.Table.L
				firstColumnName = l.Column.Name.O
			}

			if l.Expr != nil {
				if expr, ok := l.Expr.(*ast.BinaryOperationExpr); ok {
					if expr.Op == opcode.LogicAnd {
						s.appendErrorNo(ErrWrongAndExpr)
					}
				}
			}

		}
	}

	var tableList []*ast.TableSource
	tableList = extractTableList(node.TableRefs.TableRefs, tableList)

	var tableInfoList []*TableInfo

	haveNewTable := false
	catchError := false
	for _, tblSource := range tableList {
		tblName, ok := tblSource.Source.(*ast.TableName)
		if !ok {
			cols := s.getSubSelectColumns(tblSource.Source)
			// log.Info(cols)
			if cols != nil {
				rows := make([]FieldInfo, len(cols))
				for i, colName := range cols {
					rows[i].Field = colName
				}
				t := &TableInfo{
					Schema: "",
					Name:   tblSource.AsName.String(),
					Fields: rows,
				}
				tableInfoList = append(tableInfoList, t)
			}
			continue
		}

		t := s.getTableFromCache(tblName.Schema.O, tblName.Name.O, true)
		if t == nil {
			catchError = true
		} else if s.myRecord.TableInfo == nil {
			// 如果set没有指定列名,则需要根据列名遍历所有访问表的列,看操作的表是哪一个
			if originTable == "" {
				for _, field := range t.Fields {
					if strings.EqualFold(field.Field, firstColumnName) {
						s.myRecord.TableInfo = t
						break
					}
				}
			} else if originTable == tblSource.AsName.L ||
				(tblSource.AsName.L == "" && originTable == tblName.Name.L) {
				s.myRecord.TableInfo = t
			}
		}

		if t != nil {
			if tblSource.AsName.L != "" {
				t.AsName = tblSource.AsName.O
				tableInfoList = append(tableInfoList, t.copy())
			} else {
				tableInfoList = append(tableInfoList, t)
			}
			if t.IsNew {
				haveNewTable = true
			}
		}

		// if i == len(tableList) - 1 && s.myRecord.TableInfo == nil {
		// 	s.myRecord.TableInfo = t
		// }
	}

	if !catchError {
		if s.myRecord.TableInfo == nil {
			if originTable == "" {
				s.appendErrorNo(ER_COLUMN_NOT_EXISTED, firstColumnName)
			} else {
				s.appendErrorNo(ER_COLUMN_NOT_EXISTED,
					fmt.Sprintf("%s.%s", originTable, firstColumnName))
			}
		} else {
			// 新增表 or 新增列时,不能做explain
			for _, l := range node.List {
				// 未指定表别名时加默认设置
				if l.Column.Table.L == "" && len(tableInfoList) == 1 {
					l.Column.Table = model.NewCIStr(s.myRecord.TableInfo.Name)
				}

				if s.checkFieldItem(l.Column, tableInfoList) {

					// update多表操作
					// set不同的表
					// 存储其他表到MultiTables对象
					if len(tableInfoList) > 1 {
						if t := getFieldWithTableInfo(l.Column, tableInfoList); t != nil {
							if !strings.EqualFold(t.Schema, s.myRecord.TableInfo.Schema) ||
								!strings.EqualFold(t.Name, s.myRecord.TableInfo.Name) {
								key := fmt.Sprintf("%s.%s", t.Schema, t.Name)
								key = strings.ToLower(key)

								if s.myRecord.MultiTables == nil {
									s.myRecord.MultiTables = make(map[string]*TableInfo, 0)
									s.myRecord.MultiTables[key] = t
								} else if _, ok := s.myRecord.MultiTables[key]; !ok {
									s.myRecord.MultiTables[key] = t
								}
							}
						}
					}
				}

				// 多表update情况时，下面的判断会有问题
				// found := false
				// for _, field := range s.myRecord.TableInfo.Fields {
				// 	if strings.EqualFold(field.Field, l.Column.Name.L) && !field.IsDeleted {
				// 		found = true
				// 		break
				// 	}
				// }
				// if !found {
				// 	s.AppendErrorNo(ER_COLUMN_NOT_EXISTED,
				// 		fmt.Sprintf("%s.%s", s.myRecord.TableInfo.Name, l.Column.Name.L))
				// } else {
				// 	if len(tableInfoList) > 1 {
				// 		s.checkFieldItem(l.Column, tableInfoList)
				// 	}
				// }

				s.checkItem(l.Expr, tableInfoList)
			}

			s.checkSelectItem(node.TableRefs.TableRefs, node.Where != nil)
			// if node.TableRefs.TableRefs.On != nil {
			// 	s.checkItem(node.TableRefs.TableRefs.On.Expr, tableInfoList)
			// }
			s.checkItem(node.Where, tableInfoList)

			// 如果没有表结构,或者新增表 or 新增列时,不做explain
			if !s.hasError() && !s.myRecord.TableInfo.IsNew && !s.myRecord.TableInfo.IsNewColumns && !haveNewTable {
				s.explainOrAnalyzeSql(sql)
			}
		}
	}

	if node.Where == nil {
		s.appendErrorNo(ER_NO_WHERE_CONDITION)
	} else {
		// log.Infof("%#v", node.Where)
		if !s.checkVaildWhere(node.Where) {
			s.appendErrorNo(ErrUseValueExpr)
		}
	}

	if node.Limit != nil {
		s.appendErrorNo(ER_WITH_LIMIT_CONDITION)
	}

	if node.Order != nil {
		s.appendErrorNo(ER_WITH_ORDERBY_CONDITION)
	}

	// s.saveFingerprint(sqlId)
}

// checkColumnTypeImplicitConversion 列类型隐式转换检查
func (s *session) checkColumnTypeImplicitConversion(e *ast.BinaryOperationExpr, tables []*TableInfo) {
	if !s.inc.CheckImplicitTypeConversion {
		return
	}
	log.Debug("checkColumnTypeImplicitConversion")

	col, ok1 := e.L.(*ast.ColumnNameExpr)
	val, ok2 := e.R.(*ast.ValueExpr)
	// && val != nil 可以判断非空列的is null逻辑

	if ok1 && ok2 && val != nil {
		field, tableName := getFieldInfo(col.Name, tables)
		if field != nil {
			fieldType := strings.Split(strings.ToLower(field.Type), "(")[0]
			switch fieldType {
			case "bit", "tinyint", "smallint", "mediumint", "int", "integer",
				"bigint", "decimal", "float", "double", "real":
				if !types.IsTypeNumeric(val.Type.Tp) {
					s.appendErrorNo(ErrImplicitTypeConversion, tableName, field.Field, fieldType)
				}
			case "date", "time", "datetime", "timestamp",
				"char", "binary", "varchar", "varbinary", "enum", "set",
				"tibyblob", "tinytext", "blob", "text",
				"mediumblob", "mediumtext", "longblob", "longtext", "json",
				"geometry", "point", "linestring", "polygon":
				// "year",
				// "geometry", "point", "linestring", "polygon",
				if !types.IsString(val.Type.Tp) && !types.IsTypeTemporal(val.Type.Tp) {
					s.appendErrorNo(ErrImplicitTypeConversion, tableName, field.Field, fieldType)
				}
			}
		}
	}
}

func (s *session) checkItem(expr ast.ExprNode, tables []*TableInfo) bool {

	if expr == nil {
		return true
	}

	// log.Infof("%#v", expr)

	switch e := expr.(type) {
	case *ast.ColumnNameExpr:
		s.checkFieldItem(e.Name, tables)
		if e.Refer != nil {
			s.checkItem(e.Refer.Expr, tables)
		}

	case *ast.BinaryOperationExpr:
		if s.inc.CheckImplicitTypeConversion {
			s.checkColumnTypeImplicitConversion(e, tables)
		}

		return s.checkItem(e.L, tables) && s.checkItem(e.R, tables)

	case *ast.UnaryOperationExpr:
		return s.checkItem(e.V, tables)

	case *ast.FuncCallExpr:
		return s.checkFuncItem(e, tables)

	case *ast.FuncCastExpr:
		return s.checkItem(e.Expr, tables)

	case *ast.AggregateFuncExpr:
		return s.checkAggregateFuncItem(e, tables)

	case *ast.PatternInExpr:
		s.checkItem(e.Expr, tables)
		for _, expr := range e.List {
			s.checkItem(expr, tables)
		}
		if e.Sel != nil {
			s.checkItem(e.Sel, tables)
		}
	case *ast.PatternLikeExpr:
		s.checkItem(e.Expr, tables)
	case *ast.PatternRegexpExpr:
		s.checkItem(e.Expr, tables)

	case *ast.SubqueryExpr:
		s.checkSelectItem(e.Query, false)

	case *ast.CompareSubqueryExpr:
		s.checkItem(e.L, tables)
		s.checkItem(e.R, tables)

	case *ast.ExistsSubqueryExpr:
		s.checkSelectItem(e.Sel, false)

	case *ast.IsNullExpr:
		s.checkItem(e.Expr, tables)
	case *ast.IsTruthExpr:
		s.checkItem(e.Expr, tables)

	case *ast.BetweenExpr:
		s.checkItem(e.Expr, tables)
		s.checkItem(e.Left, tables)
		s.checkItem(e.Right, tables)

	case *ast.CaseExpr:
		s.checkItem(e.Value, tables)
		for _, when := range e.WhenClauses {
			s.checkItem(when.Expr, tables)
			s.checkItem(when.Result, tables)
		}
		s.checkItem(e.ElseClause, tables)

	case *ast.DefaultExpr:
		// s.checkFieldItem(e.Name, tables)
		// pass

	case *ast.ParenthesesExpr:
		s.checkItem(e.Expr, tables)

	case *ast.RowExpr:
		for _, expr := range e.Values {
			s.checkItem(expr, tables)
		}

	case *ast.ValuesExpr:
		s.checkFieldItem(e.Column.Name, tables)

	case *ast.VariableExpr:
		s.checkItem(e.Value, tables)

	case *ast.ValueExpr, *ast.ParamMarkerExpr, *ast.PositionExpr:
		// pass

	default:
		log.Infof("checkItem: %#v", e)
	}

	return true
}

// checkFieldItem 检查字段
func (s *session) checkFieldItem(name *ast.ColumnName, tables []*TableInfo) bool {
	found := false
	db := name.Schema.L

	// 未指定列别名时，判断列是否有歧义
	// Error 1052: Column 'refund_amounts' in field list is ambiguous
	isAmbiguous := false
	for _, t := range tables {
		var tName string
		if t.AsName != "" {
			tName = t.AsName
		} else {
			tName = t.Name
		}

		if name.Table.L != "" {
			if name.Table.L != "" && (db == "" || strings.EqualFold(t.Schema, db)) &&
				(strings.EqualFold(tName, name.Table.L)) {
				for _, field := range t.Fields {
					if strings.EqualFold(field.Field, name.Name.L) && !field.IsDeleted {
						found = true
						break
					}
				}
				if found {
					break
				}
			}
		} else {
			for _, field := range t.Fields {
				if strings.EqualFold(field.Field, name.Name.L) && !field.IsDeleted {
					if found {
						isAmbiguous = true
						break
					} else {
						found = true
					}
				}
			}
			if isAmbiguous {
				break
			}
		}
	}

	if isAmbiguous {
		s.appendErrorNo(ER_NON_UNIQ_ERROR, name.Name.O)
	}

	if found {
		return true
	} else {
		if name.Table.L == "" {
			s.appendErrorNo(ER_COLUMN_NOT_EXISTED, name.Name.O)
		} else {
			s.appendErrorNo(ER_COLUMN_NOT_EXISTED,
				fmt.Sprintf("%s.%s", name.Table.O, name.Name.O))
		}
		return false
	}
}

// checkFuncItem 检查函数的字段
func (s *session) checkFuncItem(f *ast.FuncCallExpr, tables []*TableInfo) bool {

	for _, arg := range f.Args {
		s.checkItem(arg, tables)
	}

	// log.Info(f.FnName.L)
	// switch f.FnName.L {
	// case ast.Nullif:
	// 	log.Infof("%#v", f)
	// 	for _, arg := range f.Args {
	// 		log.Infof("%#v", arg)
	// 	}
	// }

	return false
}

// checkFuncItem 检查聚合函数的字段
func (s *session) checkAggregateFuncItem(f *ast.AggregateFuncExpr, tables []*TableInfo) bool {

	for _, arg := range f.Args {
		s.checkItem(arg, tables)
	}

	// log.Info(f.F)
	// switch f.FnName.L {
	// case ast.Nullif:
	// 	log.Infof("%#v", f)
	// 	for _, arg := range f.Args {
	// 		log.Infof("%#v", arg)
	// 	}
	// }

	return false
}

func (s *session) checkDelete(node *ast.DeleteStmt, sql string) {
	log.Debug("checkDelete")

	// sqlId, ok := s.checkFingerprint(sql)
	// if ok {
	// 	return
	// }

	var tableList []*ast.TableSource
	tableList = extractTableList(node.TableRefs.TableRefs, tableList)

	// var tableInfoList []*TableInfo
	// for _, tblSource := range tableList {
	// 	tblName, _ := tblSource.Source.(*ast.TableName)

	// 	t := s.getTableFromCache(tblName.Schema.O, tblName.Name.O, true)
	// 	if t != nil {
	// 		if tblSource.AsName.L != "" {
	// 			t.AsName = tblSource.AsName.O
	// 			tableInfoList = append(tableInfoList, t.copy())
	// 		} else {
	// 			tableInfoList = append(tableInfoList, t)
	// 		}

	// 		if node.Tables == nil && s.myRecord.TableInfo == nil {
	// 			s.myRecord.TableInfo = t
	// 		}
	// 	}
	// }

	tableInfoList, hasNew := s.getTableList(tableList)

	if node.Tables == nil {
		if s.myRecord.TableInfo == nil && len(tableInfoList) > 0 {
			s.myRecord.TableInfo = tableInfoList[0]
		}
	} else {
		for _, name := range node.Tables.Tables {
			found := false
			db := name.Schema.String()
			for i, t := range tableInfoList {
				var tName string
				if t.AsName != "" {
					tName = t.AsName
				} else {
					tName = t.Name
				}
				if name.Name.L != "" && (db == "" || strings.EqualFold(t.Schema, db)) &&
					(strings.EqualFold(tName, name.Name.L)) {
					if s.myRecord.TableInfo == nil {
						s.myRecord.TableInfo = tableInfoList[i]
					}
					found = true
					break
				}
			}
			if !found {
				if db == "" {
					db = s.dbName
				}
				s.appendErrorNo(ER_TABLE_NOT_EXISTED_ERROR,
					fmt.Sprintf("%s.%s", db, name.Name))
			}
		}
	}

	if node.TableRefs.TableRefs.On != nil {
		s.checkItem(node.TableRefs.TableRefs.On.Expr, tableInfoList)
	}
	// if node.BeforeFrom {
	// 	s.checkItem(node.TableRefs.TableRefs.On.Expr, tableInfoList)
	// }
	if s.myRecord.TableInfo != nil && !s.hasError() {
		s.checkItem(node.Where, tableInfoList)
	}

	if !s.hasError() {
		// 如果没有表结构,或者新增表 or 新增列时,不做explain
		if s.myRecord.TableInfo != nil && !s.myRecord.TableInfo.IsNew &&
			!s.myRecord.TableInfo.IsNewColumns && !hasNew {
			s.explainOrAnalyzeSql(sql)
		}
	}

	if node.Where == nil {
		s.appendErrorNo(ER_NO_WHERE_CONDITION)
	} else {
		if !s.checkVaildWhere(node.Where) {
			s.appendErrorNo(ErrUseValueExpr)
		}
	}

	if node.Limit != nil {
		s.appendErrorNo(ER_WITH_LIMIT_CONDITION)
	}

	if node.Order != nil {
		s.appendErrorNo(ER_WITH_ORDERBY_CONDITION)
	}

	// s.saveFingerprint(sqlId)
}

func (s *session) queryTableFromDB(db string, tableName string, reportNotExists bool) []FieldInfo {
	if db == "" {
		db = s.dbName
	}
	var rows []FieldInfo
	sql := fmt.Sprintf("SHOW FULL FIELDS FROM `%s`.`%s`", db, tableName)

	if err := s.rawScan(sql, &rows); err != nil {
		if myErr, ok := err.(*mysqlDriver.MySQLError); ok {
			if myErr.Number != 1146 {
				log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
				s.appendErrorMessage(myErr.Message + ".")
			} else if reportNotExists {
				s.appendErrorNo(ER_TABLE_NOT_EXISTED_ERROR, fmt.Sprintf("%s.%s", db, tableName))
			}
		} else {
			s.appendErrorMessage(err.Error() + ".")
		}
		return nil
	}
	return rows
}

func (s *session) queryIndexFromDB(db string, tableName string, reportNotExists bool) []*IndexInfo {
	if db == "" {
		db = s.dbName
	}
	var rows []*IndexInfo
	sql := fmt.Sprintf("SHOW INDEX FROM `%s`.`%s`", db, tableName)

	if err := s.rawScan(sql, &rows); err != nil {
		if myErr, ok := err.(*mysqlDriver.MySQLError); ok {
			if myErr.Number != 1146 {
				log.Errorf("con:%d %v", s.sessionVars.ConnectionID, err)
				s.appendErrorMessage(myErr.Message + ".")
			} else if reportNotExists {
				s.appendErrorMessage(myErr.Message + ".")
				// s.AppendErrorNo(ER_TABLE_NOT_EXISTED_ERROR, fmt.Sprintf("%s.%s", db, tableName))
			}

		} else {
			s.appendErrorMessage(err.Error() + ".")
		}
		return nil
	}
	return rows
}

func (s *session) appendErrorMessage(msg string) {
	if s.stage != StageCheck && s.recordSets.MaxLevel != 2 {
		if s.stage == StageBackup {
			s.myRecord.Buf.WriteString("Backup: ")
		} else if s.stage == StageExec {
			s.myRecord.Buf.WriteString("Execute: ")
		}
	}
	s.recordSets.MaxLevel = 2
	s.myRecord.appendErrorMessage(msg)
}

func (s *session) appendWarning(number ErrorCode, values ...interface{}) {
	if s.stage == StageBackup {
		s.myRecord.Buf.WriteString("Backup: ")
	} else if s.stage == StageExec {
		s.myRecord.Buf.WriteString("Execute: ")
	}
	s.myRecord.appendWarning(s.inc.Lang, number, values...)
	s.recordSets.MaxLevel = uint8(Max(int(s.recordSets.MaxLevel), int(s.myRecord.ErrLevel)))
}

func (s *session) appendErrorNo(number ErrorCode, values ...interface{}) {
	r := s.myRecord

	// 不检查时退出
	if !s.checkInceptionVariables(number) {
		return
	}

	var level uint8 = 2
	if v, ok := s.incLevel[number.String()]; ok {
		level = v
	} else {
		level = GetErrorLevel(number)
	}

	if level > 0 {
		r.ErrLevel = uint8(Max(int(r.ErrLevel), int(level)))
		s.recordSets.MaxLevel = uint8(Max(int(s.recordSets.MaxLevel), int(s.myRecord.ErrLevel)))
		if s.stage == StageBackup {
			r.Buf.WriteString("Backup: ")
		} else if s.stage == StageExec {
			r.Buf.WriteString("Execute: ")
		}
		if len(values) == 0 {
			r.Buf.WriteString(s.getErrorMessage(number))
		} else {
			r.Buf.WriteString(fmt.Sprintf(s.getErrorMessage(number), values...))
		}
		r.Buf.WriteString("\n")
	}
}

func (s *session) checkKeyWords(name string) {
	if name != strings.ToUpper(name) {
		s.appendErrorNo(ErrIdentifierUpper, name)
	}

	if !regIdentified.MatchString(name) {
		s.appendErrorNo(ER_INVALID_IDENT, name)
	} else if _, ok := Keywords[strings.ToUpper(name)]; ok {
		s.appendErrorNo(ER_IDENT_USE_KEYWORD, name)
	}

	if len(name) > mysql.MaxTableNameLength {
		s.appendErrorNo(ER_TOO_LONG_IDENT, name)
	}
}

func (s *session) checkInceptionVariables(number ErrorCode) bool {
	switch number {
	case ER_WITH_INSERT_FIELD:
		return s.inc.CheckInsertField

	case ER_NO_WHERE_CONDITION, ErrJoinNoOnCondition:
		return s.inc.CheckDMLWhere

	case ER_WITH_LIMIT_CONDITION:
		return s.inc.CheckDMLLimit

	case ER_WITH_ORDERBY_CONDITION:
		return s.inc.CheckDMLOrderBy

	case ER_SELECT_ONLY_STAR:
		if s.inc.EnableSelectStar {
			return false
		}
	case ER_ORDERY_BY_RAND:
		if s.inc.EnableOrderByRand {
			return false
		}
	case ER_NOT_ALLOWED_NULLABLE:
		if s.inc.EnableNullable {
			return false
		}

	case ER_FOREIGN_KEY:
		if s.inc.EnableForeignKey {
			return false
		}
	case ER_USE_TEXT_OR_BLOB:
		if s.inc.EnableBlobType {
			return false
		}
	case ErrJsonTypeSupport:
		if s.inc.EnableJsonType {
			return false
		}
	case ER_PK_COLS_NOT_INT:
		return s.inc.EnablePKColumnsOnlyInt

	case ER_TABLE_MUST_HAVE_COMMENT:
		return s.inc.CheckTableComment

	case ER_COLUMN_HAVE_NO_COMMENT:
		return s.inc.CheckColumnComment

	case ER_TABLE_MUST_HAVE_PK:
		return s.inc.CheckPrimaryKey

	case ER_PARTITION_NOT_ALLOWED:
		if s.inc.EnablePartitionTable {
			return false
		}
	case ER_USE_ENUM:
		if s.inc.EnableEnumSetBit {
			return false
		}
	case ER_INVALID_DATA_TYPE:
		return true

	case ER_INDEX_NAME_IDX_PREFIX, ER_INDEX_NAME_UNIQ_PREFIX:
		return s.inc.CheckIndexPrefix

	case ER_AUTOINC_UNSIGNED:
		return s.inc.EnableAutoIncrementUnsigned

	case ER_INC_INIT_ERR:
		return s.inc.CheckAutoIncrementInitValue

	case ER_INVALID_IDENT:
		return s.inc.CheckIdentifier

	case ER_SET_DATA_TYPE_INT_BIGINT:
		return s.inc.CheckAutoIncrementDataType

	case ER_TIMESTAMP_DEFAULT:
		return s.inc.CheckTimestampDefault

	case ER_TOO_MUCH_AUTO_TIMESTAMP_COLS:
		return s.inc.CheckTimestampCount

	case ER_CHARSET_ON_COLUMN:
		if s.inc.EnableColumnCharset {
			return false
		}
	case ER_IDENT_USE_KEYWORD:
		if s.inc.EnableIdentiferKeyword {
			return false
		}
	case ER_AUTO_INCR_ID_WARNING:
		return s.inc.CheckAutoIncrementName

	case ER_ALTER_TABLE_ONCE:
		return s.inc.MergeAlterTable

	case ER_WITH_DEFAULT_ADD_COLUMN:
		return s.inc.CheckColumnDefaultValue

	case ER_CHANGE_COLUMN_TYPE:
		return s.inc.CheckColumnTypeChange

	case ErCantChangeColumnPosition:
		return s.inc.CheckColumnPositionChange

	case ER_TEXT_NOT_NULLABLE_ERROR:
		return !s.inc.EnableBlobNotNull
		/*case ER_NULL_NAME_FOR_INDEX:
		  return s.Inc.EnableNullIndexName*/
	case ER_DATETIME_DEFAULT:
		return s.inc.CheckDatetimeDefault
	case ER_TOO_MUCH_AUTO_DATETIME_COLS:
		return s.inc.CheckDatetimeCount
	case ErrIdentifierUpper:
		return s.inc.CheckIdentifierUpper
	case ErCantChangeColumn:
		return !s.inc.EnableChangeColumn
	}

	return true
}

// extractTableList 抽取语句from涉及的表
func extractTableList(node ast.ResultSetNode, input []*ast.TableSource) []*ast.TableSource {
	if node == nil {
		return input
	}

	switch x := node.(type) {
	case *ast.Join:
		input = extractTableList(x.Left, input)
		input = extractTableList(x.Right, input)

		// log.Infof("%#v", x.On)
		// if x.On == nil {
		// 	s.AppendErrorNo(ErrJoinNoOnCondition)
		// }
	case *ast.TableSource:
		// if s, ok := x.Source.(*ast.TableName); ok {
		// 	if x.AsName.L != "" {
		// 		newTableName := *s
		// 		newTableName.Name = x.AsName
		// 		s.Name = x.AsName
		// 		input = append(input, &newTableName)
		// 	} else {
		// 		input = append(input, s)
		// 	}
		// }
		input = append(input, x)
	case *ast.SelectStmt:
		if x.From != nil {
			input = extractTableList(x.From.TableRefs, input)
		}
	case *ast.UnionStmt:
		for _, sel := range x.SelectList.Selects {
			input = extractTableList(sel, input)
		}
	default:
		log.Infof("%T", x)
		// log.Infof("%#v", x)
	}
	return input
}

func (s *session) getTableFromCache(db string, tableName string, reportNotExists bool) *TableInfo {
	if db == "" {
		db = s.dbName
	}

	if db == "" {
		s.appendErrorNo(ER_WRONG_DB_NAME, "")
		return nil
	}

	if !s.checkDBExists(db, true) {
		return nil
	}

	key := fmt.Sprintf("%s.%s", db, tableName)
	if s.IgnoreCase() {
		key = strings.ToLower(key)
	}

	if t, ok := s.tableCacheList[key]; ok {
		// 如果表已删除, 之后又使用到,则报错
		if t.IsDeleted {
			if reportNotExists {
				s.appendErrorNo(ER_TABLE_NOT_EXISTED_ERROR, fmt.Sprintf("%s.%s", t.Schema, t.Name))
			}
			return nil
		}
		t.AsName = ""
		return t
	} else {
		rows := s.queryTableFromDB(db, tableName, reportNotExists)
		if rows != nil {
			newT := &TableInfo{
				Schema: db,
				Name:   tableName,
				Fields: rows,
			}
			if rows := s.queryIndexFromDB(db, tableName, reportNotExists); rows != nil {
				newT.Indexes = rows
			}
			s.tableCacheList[key] = newT

			return newT
		}
	}

	return nil
}

func (s *session) cacheNewTable(t *TableInfo) {
	if t.Schema == "" {
		t.Schema = s.dbName
	}
	key := fmt.Sprintf("%s.%s", t.Schema, t.Name)
	if s.IgnoreCase() {
		key = strings.ToLower(key)
	}

	t.IsNew = true
	// 如果表删除后新建,直接覆盖即可
	s.tableCacheList[key] = t
}

func (s *session) buildNewColumnToCache(t *TableInfo, field *ast.ColumnDef) *FieldInfo {

	c := &FieldInfo{}

	c.Field = field.Name.Name.String()
	c.Type = field.Tp.InfoSchemaStr()
	// c.Null = "YES"
	c.Null = ""
	c.Tp = field.Tp

	// if !isExplicitTimeStamp() {
	// 	// Check and set TimestampFlag, OnUpdateNowFlag and NotNullFlag.
	// 	if col.Tp == mysql.TypeTimestamp {
	// 		col.Flag |= mysql.TimestampFlag
	// 		col.Flag |= mysql.OnUpdateNowFlag
	// 		col.Flag |= mysql.NotNullFlag
	// 	}
	// }

	for _, op := range field.Options {
		switch op.Tp {
		case ast.ColumnOptionComment:
			c.Comment = op.Expr.GetDatum().GetString()
		case ast.ColumnOptionNull:
			c.Null = "YES"

			field.Tp.Flag &= ^mysql.NotNullFlag
		case ast.ColumnOptionNotNull:
			c.Null = "NO"

			field.Tp.Flag |= mysql.NotNullFlag
		case ast.ColumnOptionPrimaryKey:
			c.Key = "PRI"

			field.Tp.Flag |= mysql.PriKeyFlag
		case ast.ColumnOptionUniqKey:
			field.Tp.Flag |= mysql.UniqueKeyFlag

		case ast.ColumnOptionDefaultValue:
			switch v := op.Expr.(type) {
			case *ast.FuncCallExpr:
				c.Default = new(string)
				*c.Default = v.FnName.L
			case *ast.ValueExpr:
				if v.GetValue() == nil {
					c.Null = "YES"
					c.Default = nil
				} else {
					c.Default = new(string)
					*c.Default = v.GetString()
				}
			default:
				c.Default = new(string)
				var builder strings.Builder
				op.Expr.Restore(format.NewRestoreCtx(format.DefaultRestoreFlags, &builder))
				*c.Default = builder.String()
			}

		case ast.ColumnOptionAutoIncrement:
			if strings.ToLower(c.Field) != "id" {
				s.appendErrorNo(ER_AUTO_INCR_ID_WARNING, c.Field)
			}
			field.Tp.Flag |= mysql.AutoIncrementFlag
			c.Extra += "auto_increment"
		case ast.ColumnOptionOnUpdate:
			if field.Tp.Tp == mysql.TypeTimestamp || field.Tp.Tp == mysql.TypeDatetime {
				if !IsCurrentTimestampExpr(op.Expr) {
					s.appendErrorNo(ER_INVALID_ON_UPDATE, c.Field)
				} else {
					c.Extra += "on update CURRENT_TIMESTAMP"
				}
			} else {
				s.appendErrorNo(ER_INVALID_ON_UPDATE, c.Field)
			}
			field.Tp.Flag |= mysql.OnUpdateNowFlag
		case ast.ColumnOptionCollate:
			c.Collation = op.StrValue
		}
	}

	if c.Collation == "" && t.Collation != "" {
		// 字符串类型才需要排序规则
		switch strings.ToLower(GetDataTypeBase(c.Type)) {
		case "char", "binary", "varchar", "varbinary", "enum", "set",
			"geometry", "point", "linestring", "polygon",
			"tinytext", "text", "mediumtext", "longtext":
			c.Collation = t.Collation
		}
	}

	if c.Key != "PRI" && mysql.HasPriKeyFlag(field.Tp.Flag) {
		c.Key = "PRI"
	}

	if c.Default == nil {
		field.Tp.Flag |= mysql.NoDefaultValueFlag
	}
	c.IsNew = true
	return c
}

// checkSelectItem 子句递归检查
func (s *session) checkSelectItem(node ast.ResultSetNode, hasWhere bool) []*TableInfo {
	if node == nil {
		return nil
	}

	switch x := node.(type) {
	case *ast.UnionStmt:
		stmt := x.SelectList
		for _, sel := range stmt.Selects[:len(stmt.Selects)-1] {
			if sel.Limit != nil {
				s.appendErrorNo(ErrWrongUsage, "UNION", "LIMIT")
			}
			if sel.OrderBy != nil {
				s.appendErrorNo(ErrWrongUsage, "UNION", "ORDER BY")
			}
		}

		for _, sel := range stmt.Selects {
			s.checkSubSelectItem(sel)
		}

	case *ast.SelectStmt:
		return s.checkSubSelectItem(x)

	case *ast.Join:
		tableInfoList := s.checkSelectItem(x.Left, false)
		tableInfoList = append(tableInfoList, s.checkSelectItem(x.Right, false)...)

		if x.On != nil {
			s.checkItem(x.On.Expr, tableInfoList)
		} else if x.Right != nil {
			// 没有任何where条件时
			if !hasWhere && !x.NaturalJoin && !x.StraightJoin && x.Using == nil {
				s.appendErrorNo(ErrJoinNoOnCondition)
			}
		}
		return tableInfoList
	case *ast.TableSource:
		switch tblSource := x.Source.(type) {
		case *ast.TableName:
			t := s.getTableFromCache(tblSource.Schema.O, tblSource.Name.O, true)
			if t != nil {
				if x.AsName.L != "" {
					t.AsName = x.AsName.O
					return []*TableInfo{t.copy()}
				} else {
					return []*TableInfo{t}
				}
			} else {
				return nil
			}
		case *ast.SelectStmt:
			s.checkSubSelectItem(tblSource)

			cols := s.getSubSelectColumns(tblSource)
			if cols != nil {
				rows := make([]FieldInfo, len(cols))
				for i, colName := range cols {
					rows[i].Field = colName
				}
				t := &TableInfo{
					Schema: "",
					Name:   x.AsName.String(),
					Fields: rows,
				}
				return []*TableInfo{t}
			}

		case *ast.UnionStmt:
			s.checkSelectItem(tblSource, false)

			cols := s.getSubSelectColumns(tblSource)
			if cols != nil {
				rows := make([]FieldInfo, len(cols))
				for i, colName := range cols {
					rows[i].Field = colName
				}
				t := &TableInfo{
					Schema: "",
					Name:   x.AsName.String(),
					Fields: rows,
				}
				return []*TableInfo{t}
			}

		default:
			return s.checkSelectItem(tblSource, false)
		}

	default:
		log.Infof("con:%d %T", s.sessionVars.ConnectionID, x)
	}
	return nil
}

func (s *session) checkSubSelectItem(node *ast.SelectStmt) []*TableInfo {
	log.Debug("checkSubSelectItem")

	var tableList []*ast.TableSource
	if node.From != nil {
		// 递归审核子查询
		// s.checkSelectItem(node.From.TableRefs)

		tableList = extractTableList(node.From.TableRefs, tableList)

		s.checkTableAliasDuplicate(node.From.TableRefs, make(map[string]interface{}))
	}

	var tableInfoList []*TableInfo
	for _, tblSource := range tableList {

		switch x := tblSource.Source.(type) {
		case *ast.TableName:
			tblName := x
			t := s.getTableFromCache(tblName.Schema.O, tblName.Name.O, true)
			if t != nil {
				// if tblSource.AsName.L != "" {
				// 	t.AsName = tblSource.AsName.O
				// }
				// tableInfoList = append(tableInfoList, t)

				if tblSource.AsName.L != "" {
					t.AsName = tblSource.AsName.O
					tableInfoList = append(tableInfoList, t.copy())
				} else {
					tableInfoList = append(tableInfoList, t)
				}
			}
		case *ast.SelectStmt:
			// 递归审核子查询
			s.checkSubSelectItem(x)

			cols := s.getSubSelectColumns(x)
			// log.Info(cols)
			if cols != nil {
				rows := make([]FieldInfo, len(cols))
				for i, colName := range cols {
					rows[i].Field = colName
				}
				t := &TableInfo{
					Schema: "",
					Name:   tblSource.AsName.String(),
					Fields: rows,
				}
				tableInfoList = append(tableInfoList, t)
			}
		default:
			log.Infof("con:%d %T", s.sessionVars.ConnectionID, x)
			tableInfoList = append(tableInfoList, s.checkSelectItem(tblSource, false)...)
		}
	}

	if node.Fields != nil {
		for _, field := range node.Fields.Fields {
			if field.WildCard == nil {
				s.checkItem(field.Expr, tableInfoList)
			}
		}
	}

	if node.From != nil && node.From.TableRefs.On != nil {
		s.checkItem(node.From.TableRefs.On.Expr, tableInfoList)
	}

	s.checkItem(node.Where, tableInfoList)

	// log.Info("group by : ", s.sessionVars.SQLMode.HasOnlyFullGroupBy())
	if s.sessionVars.SQLMode.HasOnlyFullGroupBy() && node.From != nil {
		var err error
		if node.GroupBy != nil {
			err = s.checkOnlyFullGroupByWithGroupClause(node, tableInfoList)
		} else {
			err = s.checkOnlyFullGroupByWithOutGroupClause(node.Fields.Fields)
		}
		if err != nil {
			s.appendErrorMessage(err.Error())
		}
	}

	if node.GroupBy != nil {
		for _, item := range node.GroupBy.Items {
			s.checkItem(item.Expr, tableInfoList)
		}
	}

	if node.Having != nil {
		s.checkItem(node.Having.Expr, tableInfoList)
	}

	if node.OrderBy != nil {
		for _, item := range node.OrderBy.Items {
			s.checkItem(item.Expr, tableInfoList)
		}
	}

	return tableInfoList
	// return !s.hasError()
}

// getTableInfoList 获取语句涉及到的表信息,不包含where子查询中用到的
func (s *session) getTableInfoList(node ast.ResultSetNode) []*TableInfo {
	if node == nil {
		return nil
	}

	var tableList []*ast.TableSource
	tableList = extractTableList(node, tableList)
	s.checkTableAliasDuplicate(node, make(map[string]interface{}))

	return s.getTableInfoByTableSource(tableList)
}

// getTableInfoByTableSource 根据from的对象获取涉及表信息
func (s *session) getTableInfoByTableSource(tableList []*ast.TableSource) (tableInfoList []*TableInfo) {

	for _, tblSource := range tableList {
		switch x := tblSource.Source.(type) {
		case *ast.TableName:
			tblName := x
			t := s.getTableFromCache(tblName.Schema.O, tblName.Name.O, true)
			if t != nil {
				// t.AsName = tblSource.AsName.O
				// tableInfoList = append(tableInfoList, t)

				if tblSource.AsName.L != "" {
					t.AsName = tblSource.AsName.O
					tableInfoList = append(tableInfoList, t.copy())
				} else {
					tableInfoList = append(tableInfoList, t)
				}
			}
		case *ast.SelectStmt:
			cols := s.getSubSelectColumns(x)
			if cols != nil {
				rows := make([]FieldInfo, len(cols))
				for i, colName := range cols {
					rows[i].Field = colName
				}
				t := &TableInfo{
					Schema: "",
					Name:   tblSource.AsName.String(),
					Fields: rows,
				}
				tableInfoList = append(tableInfoList, t)
			}
		default:
			log.Infof("con:%d %T", s.sessionVars.ConnectionID, x)
		}
	}
	return tableInfoList
}
func (s *session) isMiddleware() bool {
	return s.opt.middlewareExtend != ""
}

func (s *session) executeKillStmt(node *ast.KillStmt) ([]sqlexec.RecordSet, error) {
	sm := s.GetSessionManager()
	if sm == nil {
		return nil, nil
	}
	sm.Kill(node.ConnectionID, node.Query)
	// conf := config.GetGlobalConfig()
	// if node.TiDBExtension || conf.CompatibleKillQuery {
	// 	sm := s.GetSessionManager()
	// 	if sm == nil {
	// 		return nil, nil
	// 	}
	// 	sm.Kill(node.ConnectionID, node.Query)
	// } else {
	// 	err := errors.New("Invalid operation. Please use 'KILL TIDB [CONNECTION | QUERY] connectionID' instead")
	// 	s.sessionVars.StmtCtx.AppendWarning(err)
	// }
	return nil, nil
}

// checkFingerprint 检查sql指纹,如果指纹存在,则直接跳过
func (s *session) checkFingerprint(sql string) (string, bool) {
	if s.inc.EnableFingerprint {
		fingerprint := query.Fingerprint(sql)
		id := query.Id(fingerprint)

		if record, ok := s.sqlFingerprint[id]; ok {
			// s.myRecord.TableInfo = record.TableInfo
			s.myRecord.AffectedRows = record.AffectedRows
			if record.ErrLevel > s.myRecord.ErrLevel {
				s.myRecord.ErrLevel = record.ErrLevel
			}
			msg := record.Buf.String()
			if msg != "" {
				s.myRecord.appendErrorMessage(strings.TrimSpace(msg))
				// 可能是警告,也可能是错误
				s.myRecord.ErrLevel = record.ErrLevel
			}
			return id, true
		}
		return id, false
	}

	return "", false
}

// saveFingerprint 保存sql指纹
func (s *session) saveFingerprint(sqlId string) {
	if s.inc.EnableFingerprint && sqlId != "" {
		s.sqlFingerprint[sqlId] = s.myRecord
	}
}

// cleanup 清理变量,缓存,osc进程等
func (s *session) cleanup() {

	// 执行完成或中止后清理osc进程信息
	pl := config.GetGlobalConfig().ShowOscProcessList()
	if len(pl) == 0 {
		return
	}
	oscList := []string{}
	for _, pi := range pl {
		if pi.ConnID == s.sessionVars.ConnectionID {
			oscList = append(oscList, pi.Sqlsha1)
		}
	}

	if len(oscList) > 0 {
		for _, sha1 := range oscList {
			delete(pl, sha1)
		}
	}
}

func (s *session) checkSetStmt(node *ast.SetStmt) {
	for _, variable := range node.Variables {
		if variable.Name == ast.SetNames {
			if value, ok := variable.Value.(*ast.ValueExpr); ok {
				v := value.GetString()
				if strings.EqualFold(v, "utf8") || strings.EqualFold(v, "utf8mb4") {
					continue
				}
				s.appendErrorNo(ErrCharsetNotSupport, "utf8,utf8mb4")
			}
		} else {
			s.appendErrorNo(ER_NOT_SUPPORTED_YET)
			continue
		}
	}
}

// IgnoreCase 判断是否忽略大小写
func (s *session) IgnoreCase() bool {
	return s.lowerCaseTableNames > 0
}

// getErrorMessage 获取审核信息
func (s *session) getErrorMessage(code ErrorCode) string {
	return GetErrorMessage(code, s.inc.Lang)
}

// checkVaildWhere 校验where条件是否有效
// 如果只有单个值或者类似1+2这种表达式，则认为是无效的表达式
func (s *session) checkVaildWhere(expr ast.ExprNode) bool {
	switch x := expr.(type) {
	case nil:
	case *ast.BinaryOperationExpr:
		if x.L != nil && x.R != nil {
			_, ok1 := x.L.(*ast.ValueExpr)
			_, ok2 := x.R.(*ast.ValueExpr)
			if !ok1 || !ok2 {
				return true
			}

			switch x.Op {
			case opcode.LogicAnd, opcode.LogicOr, opcode.And,
				opcode.Or, opcode.Xor,
				opcode.Plus, opcode.Minus, opcode.Mul, opcode.Mod,
				opcode.Div, opcode.IntDiv:
				return false
			}
		}
	case *ast.ParenthesesExpr:
		return s.checkVaildWhere(x.Expr)
	case *ast.ValueExpr:
		return false
	default:
		return true
	}
	return true
}