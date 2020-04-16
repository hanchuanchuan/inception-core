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
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hanchuanchuan/inception-core/ast"
	"github.com/hanchuanchuan/inception-core/config"
	"github.com/hanchuanchuan/inception-core/kv"
	"github.com/hanchuanchuan/inception-core/meta"
	"github.com/hanchuanchuan/inception-core/mysql"
	"github.com/hanchuanchuan/inception-core/parser"
	"github.com/hanchuanchuan/inception-core/privilege"
	"github.com/hanchuanchuan/inception-core/sessionctx"
	"github.com/hanchuanchuan/inception-core/sessionctx/stmtctx"
	"github.com/hanchuanchuan/inception-core/sessionctx/variable"
	"github.com/hanchuanchuan/inception-core/terror"
	"github.com/hanchuanchuan/inception-core/types"
	"github.com/hanchuanchuan/inception-core/util"
	"github.com/hanchuanchuan/inception-core/util/auth"
	"github.com/hanchuanchuan/inception-core/util/charset"
	"github.com/hanchuanchuan/inception-core/util/chunk"
	"github.com/hanchuanchuan/inception-core/util/kvcache"
	"github.com/hanchuanchuan/inception-core/util/sqlexec"
	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/context"

	"github.com/jinzhu/gorm"
)

// Session context
type Session interface {
	sessionctx.Context
	Status() uint16                                                  // Flag of current status, such as autocommit.
	LastInsertID() uint64                                            // LastInsertID is the last inserted auto_increment ID.
	AffectedRows() uint64                                            // Affected rows by latest executed stmt.
	Execute(context.Context, string) ([]sqlexec.RecordSet, error)    // Execute a sql statement.
	ExecuteInc(context.Context, string) ([]sqlexec.RecordSet, error) // Execute a sql statement.
	String() string                                                  // String is used to debug.
	// RollbackTxn(context.Context) error
	// PrepareStmt executes prepare statement in binary protocol.
	// PrepareStmt(sql string) (stmtID uint32, paramCount int, fields []*ast.ResultField, err error)
	// ExecutePreparedStmt executes a prepared statement.
	// ExecutePreparedStmt(ctx context.Context, stmtID uint32, param ...interface{}) (sqlexec.RecordSet, error)
	DropPreparedStmt(stmtID uint32) error
	SetClientCapability(uint32) // Set client capability flags.
	SetConnectionID(uint64)
	SetCommandValue(byte)
	SetProcessInfo(string, time.Time, byte)
	SetTLSState(*tls.ConnectionState)
	SetCollation(coID int) error
	SetSessionManager(util.SessionManager)
	Close()
	Auth(user *auth.UserIdentity, auth []byte, salt []byte) bool
	ShowProcess() util.ProcessInfo
	// PrePareTxnCtx is exported for test.
	// PrepareTxnCtx(context.Context)
	// FieldList returns fields list of a table.
	// FieldList(tableName string) (fields []*ast.ResultField, err error)

	// 用以测试
	GetAlterTablePostPart(sql string, isPtOSC bool) string

	LoadOptions(opt SourceOptions) error
	Audit(ctx context.Context, sql string) ([]Record, error)
	RunExecute(ctx context.Context, sql string) ([]Record, error)
	Split(ctx context.Context, sql string) ([]SplitRecord, error)
	Print(ctx context.Context, sql string) ([]PrintRecord, error)
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

// StmtHistory holds all histories of statements in a txn.
type StmtHistory struct {
	history []*stmtRecord
}

// Add appends a stmt to history list.
func (h *StmtHistory) Add(stmtID uint32, st ast.Statement, stmtCtx *stmtctx.StatementContext, params ...interface{}) {
	s := &stmtRecord{
		stmtID:  stmtID,
		st:      st,
		stmtCtx: stmtCtx,
		params:  append(([]interface{})(nil), params...),
	}
	h.history = append(h.history, s)
}

// Count returns the count of the history.
func (h *StmtHistory) Count() int {
	return len(h.history)
}

type session struct {
	// processInfo is used by ShowProcess(), and should be modified atomically.
	processInfo atomic.Value
	txn         TxnState

	mu struct {
		sync.RWMutex
		values map[fmt.Stringer]interface{}
	}

	store kv.Storage

	parser *parser.Parser

	preparedPlanCache *kvcache.SimpleLRUCache

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

func (s *session) getMembufCap() int {
	if s.sessionVars.LightningMode {
		return kv.ImportingTxnMembufCap
	}

	return kv.DefaultTxnMembufCap
}

func (s *session) cleanRetryInfo() {
	if !s.sessionVars.RetryInfo.Retrying {
		retryInfo := s.sessionVars.RetryInfo
		for _, stmtID := range retryInfo.DroppedPreparedStmtIDs {
			delete(s.sessionVars.PreparedStmts, stmtID)
		}
		retryInfo.Clean()
	}
}

func (s *session) Status() uint16 {
	return s.sessionVars.Status
}

func (s *session) LastInsertID() uint64 {
	if s.sessionVars.LastInsertID > 0 {
		return s.sessionVars.LastInsertID
	}
	return s.sessionVars.InsertID
}

func (s *session) AffectedRows() uint64 {
	return s.sessionVars.StmtCtx.AffectedRows()
}

func (s *session) SetClientCapability(capability uint32) {
	s.sessionVars.ClientCapability = capability
}

func (s *session) SetConnectionID(connectionID uint64) {
	s.sessionVars.ConnectionID = connectionID
}

func (s *session) SetTLSState(tlsState *tls.ConnectionState) {
	// If user is not connected via TLS, then tlsState == nil.
	if tlsState != nil {
		s.sessionVars.TLSConnectionState = tlsState
	}
}

func (s *session) SetCommandValue(command byte) {
	atomic.StoreUint32(&s.sessionVars.CommandValue, uint32(command))
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

// func (s *session) PreparedPlanCache() *kvcache.SimpleLRUCache {
// 	return s.preparedPlanCache
// }

func (s *session) SetSessionManager(sm util.SessionManager) {
	s.sessionManager = sm
}

func (s *session) GetSessionManager() util.SessionManager {
	return s.sessionManager
}

func (s *session) GetClient() kv.Client {
	return s.store.GetClient()
}

func (s *session) String() string {
	// TODO: how to print binded context in values appropriately?
	sessVars := s.sessionVars
	data := map[string]interface{}{
		"id":         sessVars.ConnectionID,
		"user":       sessVars.User,
		"currDBName": sessVars.CurrentDB,
		"status":     sessVars.Status,
		"strictMode": sessVars.StrictSQLMode,
	}
	if s.txn.Valid() {
		// if txn is committed or rolled back, txn is nil.
		data["txn"] = s.txn.String()
	}
	if sessVars.SnapshotTS != 0 {
		data["snapshotTS"] = sessVars.SnapshotTS
	}
	if sessVars.LastInsertID > 0 {
		data["lastInsertID"] = sessVars.LastInsertID
	}
	if len(sessVars.PreparedStmts) > 0 {
		data["preparedStmtCount"] = len(sessVars.PreparedStmts)
	}
	b, err := json.MarshalIndent(data, "", "  ")
	terror.Log(errors.Trace(err))
	return string(b)
}

const sqlLogMaxLen = 1024

// SchemaChangedWithoutRetry is used for testing.
var SchemaChangedWithoutRetry bool

func (s *session) isRetryableError(err error) bool {
	if SchemaChangedWithoutRetry {
		return kv.IsRetryableError(err)
	}
	return kv.IsRetryableError(err)
	// return kv.IsRetryableError(err) || domain.ErrInfoSchemaChanged.Equal(err)
}

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

// ExecRestrictedSQLWithSnapshot implements RestrictedSQLExecutor interface.
// This is used for executing some restricted sql statements with snapshot.
// If current session sets the snapshot timestamp, then execute with this snapshot timestamp.
// Otherwise, execute with the current transaction start timestamp if the transaction is valid.
func (s *session) ExecRestrictedSQLWithSnapshot(sctx sessionctx.Context, sql string) ([]chunk.Row, []*ast.ResultField, error) {
	ctx := context.TODO()

	// Use special session to execute the sql.
	tmp, err := s.sysSessionPool().Get()
	if err != nil {
		return nil, nil, err
	}
	se := tmp.(*session)
	defer s.sysSessionPool().Put(tmp)
	var snapshot uint64
	txn := s.Txn()
	// if err != nil {
	// 	return nil, nil, err
	// }
	if txn.Valid() {
		snapshot = s.txn.StartTS()
	}
	if s.sessionVars.SnapshotTS != 0 {
		snapshot = s.sessionVars.SnapshotTS
	}
	// Set snapshot.
	if snapshot != 0 {
		if err := se.sessionVars.SetSystemVar(variable.TiDBSnapshot, strconv.FormatUint(snapshot, 10)); err != nil {
			return nil, nil, err
		}
		defer func() {
			if err := se.sessionVars.SetSystemVar(variable.TiDBSnapshot, ""); err != nil {
				log.Error("set tidbSnapshot error", err)
			}
		}()
	}
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

func drainRecordSet(ctx context.Context, se *session, rs sqlexec.RecordSet) ([]chunk.Row, error) {
	var rows []chunk.Row
	chk := rs.NewChunk()
	for {
		err := rs.Next(ctx, chk)
		if err != nil || chk.NumRows() == 0 {
			return rows, errors.Trace(err)
		}
		iter := chunk.NewIterator4Chunk(chk)
		for r := iter.Begin(); r != iter.End(); r = iter.Next() {
			rows = append(rows, r)
		}
		chk = chunk.Renew(chk, se.sessionVars.MaxChunkSize)
	}
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

// GetAllSysVars implements GlobalVarAccessor.GetAllSysVars interface.
func (s *session) GetAllSysVars() (map[string]string, error) {
	if s.Value(sessionctx.Initing) != nil {
		return nil, nil
	}
	sql := `SELECT VARIABLE_NAME, VARIABLE_VALUE FROM %s.%s;`
	sql = fmt.Sprintf(sql, mysql.SystemDB, mysql.GlobalVariablesTable)
	rows, _, err := s.ExecRestrictedSQL(s, sql)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ret := make(map[string]string)
	for _, r := range rows {
		k, v := r.GetString(0), r.GetString(1)
		ret[k] = v
	}
	return ret, nil
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

func (s *session) SetProcessInfo(sql string, t time.Time, command byte) {
	pi := util.ProcessInfo{
		ID:        s.sessionVars.ConnectionID,
		DB:        s.sessionVars.CurrentDB,
		Command:   "LOCAL",
		Time:      t,
		State:     s.Status(),
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

func (s *session) Execute(ctx context.Context, sql string) (recordSets []sqlexec.RecordSet, err error) {
	// if recordSets, err = s.Audit(ctx, sql); err != nil {
	// 	err = errors.Trace(err)
	// 	s.sessionVars.StmtCtx.AppendError(err)
	// }
	return
}

// rollbackOnError makes sure the next statement starts a new transaction with the latest InfoSchema.
func (s *session) rollbackOnError(ctx context.Context) {
	// if !s.sessionVars.InTxn() {
	// 	terror.Log(s.RollbackTxn(ctx))
	// }
}

// checkArgs makes sure all the arguments' types are known and can be handled.
// integer types are converted to int64 and uint64, time.Time is converted to types.Time.
// time.Duration is converted to types.Duration, other known types are leaved as it is.
func checkArgs(args ...interface{}) error {
	for i, v := range args {
		switch x := v.(type) {
		case bool:
			if x {
				args[i] = int64(1)
			} else {
				args[i] = int64(0)
			}
		case int8:
			args[i] = int64(x)
		case int16:
			args[i] = int64(x)
		case int32:
			args[i] = int64(x)
		case int:
			args[i] = int64(x)
		case uint8:
			args[i] = uint64(x)
		case uint16:
			args[i] = uint64(x)
		case uint32:
			args[i] = uint64(x)
		case uint:
			args[i] = uint64(x)
		case int64:
		case uint64:
		case float32:
		case float64:
		case string:
		case []byte:
		case time.Duration:
			args[i] = types.Duration{Duration: x}
		case time.Time:
			args[i] = types.Time{Time: types.FromGoTime(x), Type: mysql.TypeDatetime}
		case nil:
		default:
			return errors.Errorf("cannot use arg[%d] (type %T):unsupported type", i, v)
		}
	}
	return nil
}

func (s *session) DropPreparedStmt(stmtID uint32) error {
	vars := s.sessionVars
	// if _, ok := vars.PreparedStmts[stmtID]; !ok {
	// 	return plannercore.ErrStmtNotFound
	// }
	vars.RetryInfo.DroppedPreparedStmtIDs = append(vars.RetryInfo.DroppedPreparedStmtIDs, stmtID)
	return nil
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

func (s *session) ClearValue(key fmt.Stringer) {
	s.mu.Lock()
	delete(s.mu.values, key)
	s.mu.Unlock()
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

func chooseMinLease(n1 time.Duration, n2 time.Duration) time.Duration {
	if n1 <= n2 {
		return n1
	}
	return n2
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

// loadSystemTZ loads systemTZ from mysql.tidb
func loadSystemTZ(se *session) (string, error) {
	sql := `select variable_value from mysql.tidb where variable_name = "system_tz"`
	rss, errLoad := se.Execute(context.Background(), sql)
	if errLoad != nil {
		return "", errLoad
	}
	// the record of mysql.tidb under where condition: variable_name = "system_tz" should shall only be one.
	defer rss[0].Close()
	chk := rss[0].NewChunk()
	rss[0].Next(context.Background(), chk)
	return chk.GetRow(0).GetString(0), nil
}

func createSession(store kv.Storage) (*session, error) {
	// dom, err := domap.Get(store)
	// if err != nil {
	// 	return nil, errors.Trace(err)
	// }
	s := &session{
		store:               store,
		parser:              parser.New(),
		sessionVars:         variable.NewSessionVars(),
		lowerCaseTableNames: 1,
	}

	// if plannercore.PreparedPlanCacheEnabled() {
	// 	s.preparedPlanCache = kvcache.NewSimpleLRUCache(plannercore.PreparedPlanCacheCapacity)
	// }
	// s.mu.values = make(map[fmt.Stringer]interface{})
	// // domain.BindDomain(s, dom)
	// // session implements variable.GlobalVarAccessor. Bind it to ctx.
	// s.sessionVars.GlobalVarsAccessor = s
	// s.sessionVars.BinlogClient = binloginfo.GetPumpClient()
	// s.txn.init()
	return s, nil
}

const (
	notBootstrapped         = 0
	currentBootstrapVersion = 24
)

func getStoreBootstrapVersion(store kv.Storage) int64 {
	storeBootstrappedLock.Lock()
	defer storeBootstrappedLock.Unlock()
	// check in memory
	_, ok := storeBootstrapped[store.UUID()]
	if ok {
		return currentBootstrapVersion
	}

	var ver int64
	// check in kv store
	err := kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		var err error
		t := meta.NewMeta(txn)
		ver, err = t.GetBootstrapVersion()
		return errors.Trace(err)
	})

	if err != nil {
		log.Fatalf("check bootstrapped err %v", err)
	}

	if ver > notBootstrapped {
		// here mean memory is not ok, but other server has already finished it
		storeBootstrapped[store.UUID()] = true
	}

	return ver
}

func finishBootstrap(store kv.Storage) {
	storeBootstrappedLock.Lock()
	storeBootstrapped[store.UUID()] = true
	storeBootstrappedLock.Unlock()

	err := kv.RunInNewTxn(store, true, func(txn kv.Transaction) error {
		t := meta.NewMeta(txn)
		err := t.FinishBootstrap(currentBootstrapVersion)
		return errors.Trace(err)
	})
	if err != nil {
		log.Fatalf("finish bootstrap err %v", err)
	}
}

const quoteCommaQuote = "', '"
const loadCommonGlobalVarsSQL = "select HIGH_PRIORITY * from mysql.global_variables where variable_name in ('" +
	variable.AutocommitVar + quoteCommaQuote +
	variable.SQLModeVar + quoteCommaQuote +
	variable.MaxAllowedPacket + quoteCommaQuote +
	variable.TimeZone + quoteCommaQuote +
	variable.BlockEncryptionMode + quoteCommaQuote +
	/* TiDB specific global variables: */
	variable.TiDBSkipUTF8Check + quoteCommaQuote +
	variable.TiDBIndexJoinBatchSize + quoteCommaQuote +
	variable.TiDBIndexLookupSize + quoteCommaQuote +
	variable.TiDBIndexLookupConcurrency + quoteCommaQuote +
	variable.TiDBIndexLookupJoinConcurrency + quoteCommaQuote +
	variable.TiDBIndexSerialScanConcurrency + quoteCommaQuote +
	variable.TiDBHashJoinConcurrency + quoteCommaQuote +
	variable.TiDBProjectionConcurrency + quoteCommaQuote +
	variable.TiDBHashAggPartialConcurrency + quoteCommaQuote +
	variable.TiDBHashAggFinalConcurrency + quoteCommaQuote +
	variable.TiDBBackoffLockFast + quoteCommaQuote +
	variable.TiDBConstraintCheckInPlace + quoteCommaQuote +
	variable.TiDBDDLReorgWorkerCount + quoteCommaQuote +
	variable.TiDBOptInSubqUnFolding + quoteCommaQuote +
	variable.TiDBDistSQLScanConcurrency + quoteCommaQuote +
	variable.TiDBMaxChunkSize + quoteCommaQuote +
	variable.TiDBEnableCascadesPlanner + quoteCommaQuote +
	variable.TiDBRetryLimit + quoteCommaQuote +
	variable.TiDBDisableTxnAutoRetry + "')"

// ActivePendingTxn implements Context.ActivePendingTxn interface.
func (s *session) ActivePendingTxn() error {
	if s.txn.Valid() {
		return nil
	}
	txnCap := s.getMembufCap()
	// The transaction status should be pending.
	if err := s.txn.changePendingToValid(txnCap); err != nil {
		return errors.Trace(err)
	}
	s.sessionVars.TxnCtx.StartTS = s.txn.StartTS()
	return nil
}

// InitTxnWithStartTS create a transaction with startTS.
func (s *session) InitTxnWithStartTS(startTS uint64) error {
	if s.txn.Valid() {
		return nil
	}

	// no need to get txn from txnFutureCh since txn should init with startTs
	txn, err := s.store.BeginWithStartTS(startTS)
	if err != nil {
		return errors.Trace(err)
	}
	s.txn.changeInvalidToValid(txn)
	s.txn.SetCap(s.getMembufCap())
	// err = s.loadCommonGlobalVariablesIfNeeded()
	// if err != nil {
	// 	return errors.Trace(err)
	// }
	return nil
}

// GetStore gets the store of session.
func (s *session) GetStore() kv.Storage {
	return s.store
}

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

// ResetContextOfStmt resets the StmtContext and session variables.
// Before every execution, we must clear statement context.
func (s *session) ResetContextOfStmt() (err error) {
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
