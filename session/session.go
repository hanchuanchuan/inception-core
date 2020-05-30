// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

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

package session

import (
	"crypto/tls"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hanchuanchuan/goInception/ast"
	"github.com/hanchuanchuan/goInception/mysql"
	"github.com/hanchuanchuan/goInception/sessionctx/stmtctx"
	"github.com/hanchuanchuan/goInception/util"
	"github.com/hanchuanchuan/goInception/util/auth"
	"github.com/hanchuanchuan/goInception/util/charset"
	"github.com/hanchuanchuan/goInception/util/chunk"
	"github.com/hanchuanchuan/goInception/util/sqlexec"
	"github.com/hanchuanchuan/inception-core/config"
	"github.com/hanchuanchuan/inception-core/kv"
	"github.com/hanchuanchuan/inception-core/parser"
	"github.com/hanchuanchuan/inception-core/privilege"
	"github.com/hanchuanchuan/inception-core/sessionctx"
	"github.com/hanchuanchuan/inception-core/sessionctx/variable"
	"github.com/hanchuanchuan/inception-core/terror"
	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"

	"github.com/jinzhu/gorm"
)

// Session context
type Session interface {
	sessionctx.Context
	AffectedRows() uint64 // Affected rows by latest executed stmt.
	// Execute(context.Context, string) ([]sqlexec.RecordSet, error)    // Execute a sql statement.
	Execute(context.Context, string) ([]Record, error)               // Execute a sql statement.
	ExecuteInc(context.Context, string) ([]sqlexec.RecordSet, error) // Execute a sql statement.

	SetConnectionID(uint64)
	SetProcessInfo(string, time.Time)

	SetCollation(coID int) error
	SetSessionManager(util.SessionManager)

	Auth(user *auth.UserIdentity, auth []byte, salt []byte) bool
	ShowProcess() util.ProcessInfo

	// 用以测试
	GetAlterTablePostPart(sql string, isPtOSC bool) string

	// LoadOptions 加载配置
	LoadOptions(opt SourceOptions) error
	// Audit 审核
	Audit(ctx context.Context, sql string) ([]Record, error)
	// RunExecute 执行
	RunExecute(ctx context.Context, sql string) ([]Record, error)

	// 特殊SQL审核
	CheckStmt(ctx context.Context, stmtNode ast.StmtNode,
		currentSQL string) ([]sqlexec.RecordSet, error)

	// 拆分
	Split(ctx context.Context, sql string) ([]SplitRecord, error)
	// 打印语法树
	Print(ctx context.Context, sql string) ([]PrintRecord, error)
	// 打印语法树
	QueryTree(ctx context.Context, sql string) ([]PrintRecord, error)
}

var (
	_ Session = (*session)(nil)
)

type stmtRecord struct {
	stmtID  uint32
	st      ast.Statement
	stmtCtx *stmtctx.StatementContext
	params  []interface{}
}

type session struct {
	// processInfo is used by ShowProcess(), and should be modified atomically.
	processInfo atomic.Value
	txn         TxnState

	mu struct {
		sync.RWMutex
		values map[fmt.Stringer]interface{}
	}

	// store kv.Storage

	parser *parser.Parser

	sessionVars    *variable.SessionVars
	sessionManager util.SessionManager

	// statsCollector *statistics.SessionStatsCollector

	haveBegin  bool
	haveCommit bool
	// 标识API请求
	isAPI bool

	recordSets *MyRecordSets

	opt *SourceOptions

	db       *gorm.DB
	backupdb *gorm.DB

	// 执行DDL操作的数据库连接. 仅用于事务功能
	ddlDB *gorm.DB

	dbName string

	myRecord *Record

	tableCacheList map[string]*TableInfo
	dbCacheList    map[string]*DBInfo

	// 备份库
	backupDBCacheList map[string]bool
	// 备份库中的备份表
	backupTableCacheList map[string]bool

	inc   config.Inc
	osc   config.Osc
	ghost config.Ghost

	// 异步备份的通道
	ch chan *chanData

	// 批量写入表$_$Inception_backup_information$_$
	chBackupRecord chan *chanBackup

	// 批量insert
	insertBuffer []interface{}

	// 记录表结构以重用
	// allTables map[uint64]*Table

	// 记录上次的备份表名,如果表名改变时,刷新insert缓存
	lastBackupTable string

	// 总的操作行数,当备份时用以计算备份进度
	totalChangeRows int
	backupTotalRows int

	// 数据库类型
	dbType int
	// 数据库版本号
	dbVersion int

	// 远程数据库线程ID,在启用备份功能时,用以记录线程ID来解析binlog
	threadID uint32

	sqlFingerprint map[string]*Record
	// // osc进程解析通道
	// chanOsc chan *ChanOscData

	// 当前阶段 [Check,Execute,Backup]
	stage byte

	// 打印语法树
	printSets *PrintSets

	// 时间戳类型是否需要明确指定默认值
	explicitDefaultsForTimestamp bool

	// 判断kill操作在哪个阶段,如果是在执行阶段时,则不停止备份
	killExecute bool

	// 统计信息
	statistics *statisticsInfo

	// DDL和DML分隔结果
	splitSets *SplitSets

	// 自定义审核级别,通过解析config.GetGlobalConfig().IncLevel生成
	incLevel map[string]uint8

	alterRollbackBuffer []string

	// 目标数据库的innodb_large_prefix设置
	innodbLargePrefix bool
	// 目标数据库的lower-case-table-names设置, 默认值为1,即不区分大小写
	lowerCaseTableNames int
	// PXC集群节点
	isClusterNode bool
}

func (s *session) AffectedRows() uint64 {
	return s.sessionVars.StmtCtx.AffectedRows()
}

func (s *session) SetConnectionID(connectionID uint64) {
	s.sessionVars.ConnectionID = connectionID
}

func (s *session) GetTLSState() *tls.ConnectionState {
	return s.sessionVars.TLSConnectionState
}

func (s *session) SetCollation(coID int) error {
	cs, co, err := charset.GetCharsetInfoByID(coID)
	if err != nil {
		return errors.Trace(err)
	}
	for _, v := range variable.SetNamesVariables {
		terror.Log(errors.Trace(s.sessionVars.SetSystemVar(v, cs)))
	}
	terror.Log(errors.Trace(s.sessionVars.SetSystemVar(variable.CollationConnection, co)))
	return nil
}

func (s *session) GetAlterTablePostPart(sql string, isPtOSC bool) string {
	return s.getAlterTablePostPart(sql, isPtOSC)
}

func (s *session) SetSessionManager(sm util.SessionManager) {
	s.sessionManager = sm
}

func (s *session) GetSessionManager() util.SessionManager {
	return s.sessionManager
}

// func (s *session) GetClient() kv.Client {
// 	return s.store.GetClient()
// }

// func (s *session) String() string {
// 	// TODO: how to print binded context in values appropriately?
// 	sessVars := s.sessionVars
// 	data := map[string]interface{}{
// 		"id":         sessVars.ConnectionID,
// 		"user":       sessVars.User,
// 		"currDBName": sessVars.CurrentDB,
// 		"status":     sessVars.Status,
// 		"strictMode": sessVars.StrictSQLMode,
// 	}
// 	if s.txn.Valid() {
// 		// if txn is committed or rolled back, txn is nil.
// 		data["txn"] = s.txn.String()
// 	}
// 	if sessVars.SnapshotTS != 0 {
// 		data["snapshotTS"] = sessVars.SnapshotTS
// 	}
// 	if sessVars.LastInsertID > 0 {
// 		data["lastInsertID"] = sessVars.LastInsertID
// 	}
// 	if len(sessVars.PreparedStmts) > 0 {
// 		data["preparedStmtCount"] = len(sessVars.PreparedStmts)
// 	}
// 	b, err := json.MarshalIndent(data, "", "  ")
// 	terror.Log(errors.Trace(err))
// 	return string(b)
// }

const sqlLogMaxLen = 1024

func (s *session) sysSessionPool() *pools.ResourcePool {
	// return domain.GetDomain(s).SysSessionPool()
	return nil
}

// ExecRestrictedSQL implements RestrictedSQLExecutor interface.
// This is used for executing some restricted sql statements, usually executed during a normal statement execution.
// Unlike normal Exec, it doesn't reset statement status, doesn't commit or rollback the current transaction
// and doesn't write binlog.
func (s *session) ExecRestrictedSQL(sctx sessionctx.Context, sql string) ([]chunk.Row, []*ast.ResultField, error) {
	ctx := context.TODO()

	// Use special session to execute the sql.
	tmp, err := s.sysSessionPool().Get()
	if err != nil {
		return nil, nil, err
	}
	se := tmp.(*session)
	defer s.sysSessionPool().Put(tmp)

	return execRestrictedSQL(ctx, se, sql)
}

func execRestrictedSQL(ctx context.Context, se *session, sql string) ([]chunk.Row, []*ast.ResultField, error) {
	return nil, nil, nil
	// recordSets, err := se.Execute(ctx, sql)
	// if err != nil {
	// 	return nil, nil, err
	// }

	// var (
	// 	rows   []chunk.Row
	// 	fields []*ast.ResultField
	// )
	// // Execute all recordset, take out the first one as result.
	// for i, rs := range recordSets {
	// 	tmp, err := drainRecordSet(ctx, se, rs)
	// 	if err != nil {
	// 		return nil, nil, err
	// 	}
	// 	if err = rs.Close(); err != nil {
	// 		return nil, nil, err
	// 	}

	// 	if i == 0 {
	// 		rows = tmp
	// 		fields = rs.Fields()
	// 	}
	// }
	// return rows, fields, nil
}

// getExecRet executes restricted sql and the result is one column.
// It returns a string value.
func (s *session) getExecRet(ctx sessionctx.Context, sql string) (string, error) {
	rows, fields, err := s.ExecRestrictedSQL(ctx, sql)
	if err != nil {
		return "", errors.Trace(err)
	}
	// if len(rows) == 0 {
	// 	return "", executor.ErrResultIsEmpty
	// }
	d := rows[0].GetDatum(0, &fields[0].Column.FieldType)
	value, err := d.ToString()
	if err != nil {
		return "", errors.Trace(err)
	}
	return value, nil
}

// GetGlobalSysVar implements GlobalVarAccessor.GetGlobalSysVar interface.
func (s *session) GetGlobalSysVar(name string) (string, error) {
	if s.Value(sessionctx.Initing) != nil {
		// When running bootstrap or upgrade, we should not access global storage.
		return "", nil
	}
	sql := fmt.Sprintf(`SELECT VARIABLE_VALUE FROM %s.%s WHERE VARIABLE_NAME="%s";`,
		mysql.SystemDB, mysql.GlobalVariablesTable, name)
	sysVar, err := s.getExecRet(s, sql)
	if err != nil {
		// if executor.ErrResultIsEmpty.Equal(err) {
		// 	if sv, ok := variable.SysVars[name]; ok {
		// 		return sv.Value, nil
		// 	}
		// 	return "", variable.UnknownSystemVar.GenWithStackByArgs(name)
		// }
		return "", errors.Trace(err)
	}
	return sysVar, nil
}

// SetGlobalSysVar implements GlobalVarAccessor.SetGlobalSysVar interface.
func (s *session) SetGlobalSysVar(name, value string) error {
	if name == variable.SQLModeVar {
		value = mysql.FormatSQLModeStr(value)
		if _, err := mysql.GetSQLMode(value); err != nil {
			return errors.Trace(err)
		}
	}
	var sVal string
	var err error
	sVal, err = variable.ValidateSetSystemVar(s.sessionVars, name, value)
	if err != nil {
		return errors.Trace(err)
	}
	sql := fmt.Sprintf(`REPLACE %s.%s VALUES ('%s', '%s');`,
		mysql.SystemDB, mysql.GlobalVariablesTable, strings.ToLower(name), sVal)
	_, _, err = s.ExecRestrictedSQL(s, sql)
	return errors.Trace(err)
}

func (s *session) ParseSQL(ctx context.Context, sql, charset, collation string) ([]ast.StmtNode, error) {
	s.parser.SetSQLMode(s.sessionVars.SQLMode)
	stmts, _, err := s.parser.Parse(sql, charset, collation)
	return stmts, err
}

func (s *session) SetProcessInfo(sql string, t time.Time) {
	pi := util.ProcessInfo{
		ID:        s.sessionVars.ConnectionID,
		DB:        s.sessionVars.CurrentDB,
		Command:   "LOCAL",
		Time:      t,
		State:     s.sessionVars.Status,
		Info:      sql,
		OperState: "INIT",
	}
	if s.sessionVars.User != nil {
		pi.User = s.sessionVars.User.Username
		pi.Host = s.sessionVars.User.Hostname
	}
	s.processInfo.Store(pi)
}

func (s *session) SetMyProcessInfo(sql string, t time.Time, percent float64) {
	tmp := s.processInfo.Load()
	if tmp != nil {
		pi := tmp.(util.ProcessInfo)

		pi.Info = sql
		pi.Time = t
		pi.Percent = percent
		s.processInfo.Store(pi)
	}
}

func (s *session) Execute(ctx context.Context, sql string) ([]Record, error) {
	return s.RunExecute(ctx, sql)
}

func (s *session) Txn() kv.Transaction {
	if !s.txn.Valid() {
		return nil
	}
	return &s.txn
}

func (s *session) SetValue(key fmt.Stringer, value interface{}) {
	s.mu.Lock()
	s.mu.values[key] = value
	s.mu.Unlock()
}

func (s *session) Value(key fmt.Stringer) interface{} {
	s.mu.RLock()
	value := s.mu.values[key]
	s.mu.RUnlock()
	return value
}

// Close function does some clean work when session end.
func (s *session) Close() {
	// if s.statsCollector != nil {
	// 	s.statsCollector.Delete()
	// }
	// ctx := context.TODO()
	// if err := s.RollbackTxn(ctx); err != nil {
	// 	log.Error("session Close error:", errors.ErrorStack(err))
	// }
}

// GetSessionVars implements the context.Context interface.
func (s *session) GetSessionVars() *variable.SessionVars {
	return s.sessionVars
}

func (s *session) Auth(user *auth.UserIdentity, authentication []byte, salt []byte) bool {
	pm := privilege.GetPrivilegeManager(s)

	// Check IP.
	var success bool
	user.AuthUsername, user.AuthHostname, success = pm.ConnectionVerification(user.Username, user.Hostname, authentication, salt)
	if success {
		s.sessionVars.User = user
		return true
	}

	// Check Hostname.
	for _, addr := range getHostByIP(user.Hostname) {
		u, h, success := pm.ConnectionVerification(user.Username, addr, authentication, salt)
		if success {
			s.sessionVars.User = &auth.UserIdentity{
				Username:     user.Username,
				Hostname:     addr,
				AuthUsername: u,
				AuthHostname: h,
			}
			return true
		}
	}

	log.Errorf("User connection verification failed %s", user)
	return false
}

func getHostByIP(ip string) []string {
	if ip == "127.0.0.1" {
		return []string{"localhost"}
	}
	addrs, err := net.LookupAddr(ip)
	terror.Log(errors.Trace(err))
	return addrs
}

// CreateSession4Test creates a new session environment for test.
func CreateSession4Test(store kv.Storage) (Session, error) {
	s, err := CreateSession(store)
	if err == nil {
		// initialize session variables for test.
		s.GetSessionVars().MaxChunkSize = 2
	}
	return s, errors.Trace(err)
}

// CreateSession creates a new session environment.
func CreateSession(store kv.Storage) (Session, error) {
	s, err := createSession(store)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Add auth here.
	// do, err := domap.Get(store)
	// if err != nil {
	// 	return nil, errors.Trace(err)
	// }
	// pm := &privileges.UserPrivileges{
	// 	Handle: do.PrivilegeHandle(),
	// }
	// privilege.BindPrivilegeManager(s, pm)

	// Add stats collector, and it will be freed by background stats worker
	// which periodically updates stats using the collected data.
	// if do.StatsHandle() != nil && do.StatsUpdating() {
	// 	s.statsCollector = do.StatsHandle().NewSessionStatsCollector()
	// }

	return s, nil
}

func createSession(store kv.Storage) (*session, error) {
	s := &session{
		// store:               store,
		parser:              parser.New(),
		sessionVars:         variable.NewSessionVars(),
		lowerCaseTableNames: 1,
	}
	return s, nil
}

const (
	notBootstrapped         = 0
	currentBootstrapVersion = 24
)

// // GetStore gets the store of session.
// func (s *session) GetStore() kv.Storage {
// 	return s.store
// }

func (s *session) ShowProcess() util.ProcessInfo {
	var pi util.ProcessInfo
	tmp := s.processInfo.Load()
	if tmp != nil {
		pi = tmp.(util.ProcessInfo)
		// pi.Mem = s.GetSessionVars().StmtCtx.MemTracker.BytesConsumed()
	}
	return pi
}

// logStmt logs some crucial SQL including: CREATE USER/GRANT PRIVILEGE/CHANGE PASSWORD/DDL etc and normal SQL
// if variable.ProcessGeneralLog is set.
func logStmt(node ast.StmtNode, vars *variable.SessionVars) {
	switch stmt := node.(type) {
	case *ast.CreateUserStmt, *ast.DropUserStmt, *ast.AlterUserStmt, *ast.SetPwdStmt, *ast.GrantStmt,
		*ast.RevokeStmt, *ast.AlterTableStmt, *ast.CreateDatabaseStmt, *ast.CreateIndexStmt, *ast.CreateTableStmt,
		*ast.DropDatabaseStmt, *ast.DropIndexStmt, *ast.DropTableStmt, *ast.RenameTableStmt, *ast.TruncateTableStmt:
		user := vars.User
		schemaVersion := vars.TxnCtx.SchemaVersion
		if ss, ok := node.(ast.SensitiveStmtNode); ok {
			log.Infof("[CRUCIAL OPERATION] con:%d schema_ver:%d %s (by %s).", vars.ConnectionID, schemaVersion, ss.SecureText(), user)
		} else {
			log.Infof("[CRUCIAL OPERATION] con:%d schema_ver:%d %s (by %s).", vars.ConnectionID, schemaVersion, stmt.Text(), user)
		}
	default:
		logQuery(node.Text(), vars)
	}
}

func logQuery(query string, vars *variable.SessionVars) {
	if atomic.LoadUint32(&variable.ProcessGeneralLog) != 0 && !vars.InRestrictedSQL {
		// log.Infof("[GENERAL_LOG] con:%d user:%s schema_ver:%d start_ts:%d sql:%s%s",
		// 	vars.ConnectionID, vars.User, vars.TxnCtx.SchemaVersion, vars.TxnCtx.StartTS, query, vars.GetExecuteArgumentsInfo())
		log.Infof("[GENERAL_LOG] con:%d user:%s sql:%s%s",
			vars.ConnectionID, vars.User, query, vars.GetExecuteArgumentsInfo())
	}
}

// resetContextOfStmt resets the StmtContext and session variables.
// Before every execution, we must clear statement context.
func (s *session) resetContextOfStmt() (err error) {
	vars := s.GetSessionVars()
	sc := new(stmtctx.StatementContext)
	sc.TimeZone = vars.Location()

	vars.PreparedParams = vars.PreparedParams[:0]

	if vars.LastInsertID > 0 {
		vars.PrevLastInsertID = vars.LastInsertID
		vars.LastInsertID = 0
	}
	vars.ResetPrevAffectedRows()
	err = vars.SetSystemVar("warning_count", fmt.Sprintf("%d", vars.StmtCtx.NumWarnings(false)))
	if err != nil {
		return errors.Trace(err)
	}
	err = vars.SetSystemVar("error_count", fmt.Sprintf("%d", vars.StmtCtx.NumWarnings(true)))
	if err != nil {
		return errors.Trace(err)
	}
	vars.InsertID = 0
	vars.StmtCtx = sc
	return
}
