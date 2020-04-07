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

package inception

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hanchuanchuan/goInception/ast"
	"github.com/hanchuanchuan/goInception/config"
	"github.com/hanchuanchuan/goInception/domain"
	"github.com/hanchuanchuan/goInception/executor"
	"github.com/hanchuanchuan/goInception/kv"
	"github.com/hanchuanchuan/goInception/meta"
	"github.com/hanchuanchuan/goInception/model"
	"github.com/hanchuanchuan/goInception/mysql"
	"github.com/hanchuanchuan/goInception/owner"
	"github.com/hanchuanchuan/goInception/parser"
	plannercore "github.com/hanchuanchuan/goInception/planner/core"
	"github.com/hanchuanchuan/goInception/privilege"
	"github.com/hanchuanchuan/goInception/privilege/privileges"
	"github.com/hanchuanchuan/goInception/sessionctx"
	"github.com/hanchuanchuan/goInception/sessionctx/binloginfo"
	"github.com/hanchuanchuan/goInception/sessionctx/variable"
	"github.com/hanchuanchuan/goInception/terror"
	"github.com/hanchuanchuan/goInception/types"
	"github.com/hanchuanchuan/goInception/util"
	"github.com/hanchuanchuan/goInception/util/auth"
	"github.com/hanchuanchuan/goInception/util/charset"
	"github.com/hanchuanchuan/goInception/util/chunk"
	"github.com/hanchuanchuan/goInception/util/kvcache"
	"github.com/hanchuanchuan/goInception/util/sqlexec"
	"github.com/hanchuanchuan/goInception/util/timeutil"
	"github.com/ngaut/pools"
	"github.com/pingcap/errors"
	"github.com/pingcap/tipb/go-binlog"
	log "github.com/sirupsen/logrus"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type domainMap struct {
	domains map[string]*domain.Domain
	mu      sync.Mutex
}

func (dm *domainMap) Get(store kv.Storage) (d *domain.Domain, err error) {
	key := store.UUID()
	dm.mu.Lock()
	defer dm.mu.Unlock()
	d = dm.domains[key]
	if d != nil {
		return
	}
	return

	// ddlLease := time.Duration(0)
	// statisticLease := time.Duration(0)
	// ddlLease = schemaLease
	// statisticLease = statsLease
	// err = util.RunWithRetry(util.DefaultMaxRetries, util.RetryInterval, func() (retry bool, err1 error) {
	// 	log.Infof("store %v new domain, ddl lease %v, stats lease %d", store.UUID(), ddlLease, statisticLease)
	// 	factory := createSessionFunc(store)
	// 	sysFactory := createSessionWithDomainFunc(store)
	// 	d = domain.NewDomain(store, ddlLease, statisticLease, factory)
	// 	err1 = d.Init(ddlLease, sysFactory)
	// 	if err1 != nil {
	// 		// If we don't clean it, there are some dirty data when retrying the function of Init.
	// 		d.Close()
	// 		log.Errorf("[ddl] init domain failed %v", errors.ErrorStack(errors.Trace(err1)))
	// 	}
	// 	return true, errors.Trace(err1)
	// })
	// if err != nil {
	// 	return nil, errors.Trace(err)
	// }
	// dm.domains[key] = d

	// return
}

func (dm *domainMap) Delete(store kv.Storage) {
	dm.mu.Lock()
	delete(dm.domains, store.UUID())
	dm.mu.Unlock()
}

var (
	domap = &domainMap{
		domains: map[string]*domain.Domain{},
	}
	stores = make(map[string]kv.Driver)
	// store.UUID()-> IfBootstrapped
	storeBootstrapped     = make(map[string]bool)
	storeBootstrappedLock sync.Mutex

	// schemaLease is the time for re-updating remote schema.
	// In online DDL, we must wait 2 * SchemaLease time to guarantee
	// all servers get the neweset schema.
	// Default schema lease time is 1 second, you can change it with a proper time,
	// but you must know that too little may cause badly performance degradation.
	// For production, you should set a big schema lease, like 300s+.
	schemaLease = 1 * time.Second

	// statsLease is the time for reload stats table.
	statsLease = 3 * time.Second
)

// SetSchemaLease changes the default schema lease time for DDL.
// This function is very dangerous, don't use it if you really know what you do.
// SetSchemaLease only affects not local storage after bootstrapped.
func SetSchemaLease(lease time.Duration) {
	schemaLease = lease
}

// SetStatsLease changes the default stats lease time for loading stats info.
func SetStatsLease(lease time.Duration) {
	statsLease = lease
}

// Parse parses a query string to raw ast.StmtNode.
func Parse(ctx sessionctx.Context, src string) ([]ast.StmtNode, error) {
	log.Debug("compiling", src)
	charset, collation := ctx.GetSessionVars().GetCharsetInfo()
	p := parser.New()
	p.SetSQLMode(ctx.GetSessionVars().SQLMode)
	stmts, _, err := p.Parse(src, charset, collation)
	if err != nil {
		log.Warnf("compiling %s, error: %v", src, err)
		return nil, errors.Trace(err)
	}
	return stmts, nil
}

// Compile is safe for concurrent use by multiple goroutines.
func Compile(ctx context.Context, sctx sessionctx.Context, stmtNode ast.StmtNode) (ast.Statement, error) {
	compiler := executor.Compiler{Ctx: sctx}
	stmt, err := compiler.Compile(ctx, stmtNode)
	return stmt, errors.Trace(err)
}

// runStmt executes the ast.Statement and commit or rollback the current transaction.
func runStmt(ctx context.Context, sctx sessionctx.Context, s ast.Statement) (ast.RecordSet, error) {
	var err error
	var rs ast.RecordSet
	// se := sctx.(*Inception)
	rs, err = s.Exec(ctx)
	return rs, err

	// // All the history should be added here.
	// se.GetSessionVars().TxnCtx.StatementCount++
	// if !s.IsReadOnly() {
	// 	if err == nil {
	// 		GetHistory(sctx).Add(0, s, se.sessionVars.StmtCtx)
	// 	}
	// 	if sctx.Txn() != nil {
	// 		if err != nil {
	// 			sctx.StmtRollback()
	// 		} else {
	// 			sctx.StmtCommit()
	// 		}
	// 	}
	// }
	// if !se.sessionVars.InTxn() {
	// 	if err != nil {
	// 		log.Info("RollbackTxn for ddl/autocommit error.")
	// 		err1 := se.RollbackTxn(ctx)
	// 		terror.Log(errors.Trace(err1))
	// 	} else {
	// 		err = se.CommitTxn(ctx)
	// 	}
	// } else {
	// 	// If the user insert, insert, insert ... but never commit, TiDB would OOM.
	// 	// So we limit the statement count in a transaction here.
	// 	history := GetHistory(sctx)
	// 	if history.Count() > int(config.GetGlobalConfig().Performance.StmtCountLimit) {
	// 		err1 := se.RollbackTxn(ctx)
	// 		terror.Log(errors.Trace(err1))
	// 		return rs, errors.Errorf("statement count %d exceeds the transaction limitation, autocommit = %t",
	// 			history.Count(), sctx.GetSessionVars().IsAutocommit())
	// 	}
	// }
	// return rs, errors.Trace(err)
}

// GetRows4Test gets all the rows from a RecordSet, only used for test.
func GetRows4Test(ctx context.Context, sctx sessionctx.Context, rs ast.RecordSet) ([]chunk.Row, error) {
	if rs == nil {
		return nil, nil
	}
	var rows []chunk.Row
	chk := rs.NewChunk()
	for {
		// Since we collect all the rows, we can not reuse the chunk.
		iter := chunk.NewIterator4Chunk(chk)

		err := rs.Next(ctx, chk)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if chk.NumRows() == 0 {
			break
		}

		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			rows = append(rows, row)
		}
		chk = chunk.Renew(chk, sctx.GetSessionVars().MaxChunkSize)
	}
	return rows, nil
}

// RegisterStore registers a kv storage with unique name and its associated Driver.
func RegisterStore(name string, driver kv.Driver) error {
	name = strings.ToLower(name)

	if _, ok := stores[name]; ok {
		return errors.Errorf("%s is already registered", name)
	}

	stores[name] = driver
	return nil
}

// NewStore creates a kv Storage with path.
//
// The path must be a URL format 'engine://path?params' like the one for
// session.Open() but with the dbname cut off.
// Examples:
//    goleveldb://relative/path
//    boltdb:///absolute/path
//
// The engine should be registered before creating storage.
func NewStore(path string) (kv.Storage, error) {
	return newStoreWithRetry(path, util.DefaultMaxRetries)
}

func newStoreWithRetry(path string, maxRetries int) (kv.Storage, error) {
	storeURL, err := url.Parse(path)
	if err != nil {
		return nil, errors.Trace(err)
	}

	name := strings.ToLower(storeURL.Scheme)
	d, ok := stores[name]
	if !ok {
		return nil, errors.Errorf("invalid uri format, storage %s is not registered", name)
	}

	var s kv.Storage
	err = util.RunWithRetry(maxRetries, util.RetryInterval, func() (bool, error) {
		log.Infof("new store")
		s, err = d.Open(path)
		return kv.IsRetryableError(err), err
	})
	return s, errors.Trace(err)
}

// DialPumpClientWithRetry tries to dial to binlogSocket,
// if any error happens, it will try to re-dial,
// or return this error when timeout.
func DialPumpClientWithRetry(binlogSocket string, maxRetries int, dialerOpt grpc.DialOption) (*grpc.ClientConn, error) {
	var clientCon *grpc.ClientConn
	err := util.RunWithRetry(maxRetries, util.RetryInterval, func() (bool, error) {
		log.Infof("setup binlog client")
		var err error
		tlsConfig, err := config.GetGlobalConfig().Security.ToTLSConfig()
		if err != nil {
			log.Infof("error happen when setting binlog client: %s", errors.ErrorStack(err))
		}

		if tlsConfig != nil {
			clientCon, err = grpc.Dial(binlogSocket, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)), dialerOpt)
		} else {
			clientCon, err = grpc.Dial(binlogSocket, grpc.WithInsecure(), dialerOpt)
		}

		if err != nil {
			log.Infof("error happen when setting binlog client: %s", errors.ErrorStack(err))
		}
		return true, errors.Trace(err)
	})
	return clientCon, errors.Trace(err)
}

var queryStmtTable = []string{"explain", "select", "show", "execute", "describe", "desc", "admin"}

func trimSQL(sql string) string {
	// Trim space.
	sql = strings.TrimSpace(sql)
	// Trim leading /*comment*/
	// There may be multiple comments
	for strings.HasPrefix(sql, "/*") {
		i := strings.Index(sql, "*/")
		if i != -1 && i < len(sql)+1 {
			sql = sql[i+2:]
			sql = strings.TrimSpace(sql)
			continue
		}
		break
	}
	// Trim leading '('. For `(select 1);` is also a query.
	return strings.TrimLeft(sql, "( ")
}

// IsQuery checks if a sql statement is a query statement.
func IsQuery(sql string) bool {
	sqlText := strings.ToLower(trimSQL(sql))
	for _, key := range queryStmtTable {
		if strings.HasPrefix(sqlText, key) {
			return true
		}
	}

	return false
}

var (
	errForUpdateCantRetry = terror.ClassSession.New(codeForUpdateCantRetry,
		mysql.MySQLErrName[mysql.ErrForUpdateCantRetry])
)

const (
	codeForUpdateCantRetry terror.ErrCode = mysql.ErrForUpdateCantRetry
)

func init() {
	sessionMySQLErrCodes := map[terror.ErrCode]uint16{
		codeForUpdateCantRetry: mysql.ErrForUpdateCantRetry,
	}
	terror.ErrClassToMySQLCodes[terror.ClassSession] = sessionMySQLErrCodes
}

func logQuery(query string, vars *variable.SessionVars) {
	// query = executor.QueryReplacer.Replace(query)
	// log.Infof("[GENERAL_LOG] con:%d user:%s schema_ver:%d start_ts:%d sql:%s%s",
	// 	vars.ConnectionID, vars.User, vars.TxnCtx.SchemaVersion, vars.TxnCtx.StartTS, query, vars.GetExecuteArgumentsInfo())
	log.Infof("[GENERAL_LOG] con:%d user:%s sql:%s%s",
		vars.ConnectionID, vars.User, query, vars.GetExecuteArgumentsInfo())
}

// DDLOwnerChecker returns s.ddlOwnerChecker.
func (s *inceptionCore) DDLOwnerChecker() owner.DDLOwnerChecker {
	return s.ddlOwnerChecker
}

func (s *inceptionCore) getMembufCap() int {
	if s.sessionVars.LightningMode {
		return kv.ImportingTxnMembufCap
	}

	return kv.DefaultTxnMembufCap
}

func (s *inceptionCore) cleanRetryInfo() {
	if !s.sessionVars.RetryInfo.Retrying {
		retryInfo := s.sessionVars.RetryInfo
		for _, stmtID := range retryInfo.DroppedPreparedStmtIDs {
			delete(s.sessionVars.PreparedStmts, stmtID)
		}
		retryInfo.Clean()
	}
}

func (s *inceptionCore) Status() uint16 {
	return s.sessionVars.Status
}

func (s *inceptionCore) LastInsertID() uint64 {
	if s.sessionVars.LastInsertID > 0 {
		return s.sessionVars.LastInsertID
	}
	return s.sessionVars.InsertID
}

func (s *inceptionCore) AffectedRows() uint64 {
	return s.sessionVars.StmtCtx.AffectedRows()
}

func (s *inceptionCore) SetClientCapability(capability uint32) {
	s.sessionVars.ClientCapability = capability
}

func (s *inceptionCore) SetConnectionID(connectionID uint64) {
	s.sessionVars.ConnectionID = connectionID
}

func (s *inceptionCore) SetTLSState(tlsState *tls.ConnectionState) {
	// If user is not connected via TLS, then tlsState == nil.
	if tlsState != nil {
		s.sessionVars.TLSConnectionState = tlsState
	}
}

func (s *inceptionCore) SetCommandValue(command byte) {
	atomic.StoreUint32(&s.sessionVars.CommandValue, uint32(command))
}

func (s *inceptionCore) GetTLSState() *tls.ConnectionState {
	return s.sessionVars.TLSConnectionState
}

func (s *inceptionCore) SetCollation(coID int) error {
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

func (s *inceptionCore) GetAlterTablePostPart(sql string, isPtOSC bool) string {
	return s.getAlterTablePostPart(sql, isPtOSC)
}

func (s *inceptionCore) PreparedPlanCache() *kvcache.SimpleLRUCache {
	// return s.preparedPlanCache
	return nil
}

func (s *inceptionCore) SetSessionManager(sm util.SessionManager) {
	s.sessionManager = sm
}

func (s *inceptionCore) GetSessionManager() util.SessionManager {
	return s.sessionManager
}

func (s *inceptionCore) StoreQueryFeedback(feedback interface{}) {
	// if s.statsCollector != nil {
	// 	do, err := GetDomain(s.store)
	// 	if err != nil {
	// 		log.Debug("domain not found: ", err)
	// 		return
	// 	}
	// 	err = s.statsCollector.StoreQueryFeedback(feedback, do.StatsHandle())
	// 	if err != nil {
	// 		log.Debug("store query feedback error: ", err)
	// 		return
	// 	}
	// }
}

// FieldList returns fields list of a table.
func (s *inceptionCore) FieldList(tableName string) ([]*ast.ResultField, error) {
	is := executor.GetInfoSchema(s)
	dbName := model.NewCIStr(s.GetSessionVars().CurrentDB)
	tName := model.NewCIStr(tableName)
	table, err := is.TableByName(dbName, tName)
	if err != nil {
		return nil, errors.Trace(err)
	}

	cols := table.Cols()
	fields := make([]*ast.ResultField, 0, len(cols))
	for _, col := range table.Cols() {
		rf := &ast.ResultField{
			ColumnAsName: col.Name,
			TableAsName:  tName,
			DBName:       dbName,
			Table:        table.Meta(),
			Column:       col.ColumnInfo,
		}
		fields = append(fields, rf)
	}
	return fields, nil
}

// func (s *Inception) HaveBegin() bool {
// 	return s.haveBegin
// }

// func (s *Inception) HaveCommit() bool {
// 	return s.haveCommit
// }

// func (s *Inception) RecordSets() *MyRecordSets {
// 	return s.recordSets
// }

func (s *inceptionCore) doCommit(ctx context.Context) error {
	return nil

	// if !s.txn.Valid() {
	// 	return nil
	// }
	// defer func() {
	// 	s.txn.changeToInvalid()
	// 	s.sessionVars.SetStatusFlag(mysql.ServerStatusInTrans, false)
	// }()
	// if s.txn.IsReadOnly() {
	// 	return nil
	// }
	// if s.sessionVars.BinlogClient != nil {
	// 	prewriteValue := binloginfo.GetPrewriteValue(s, false)
	// 	if prewriteValue != nil {
	// 		prewriteData, err := prewriteValue.Marshal()
	// 		if err != nil {
	// 			return errors.Trace(err)
	// 		}
	// 		info := &binloginfo.BinlogInfo{
	// 			Data: &binlog.Binlog{
	// 				Tp:            binlog.BinlogType_Prewrite,
	// 				PrewriteValue: prewriteData,
	// 			},
	// 			Client: s.sessionVars.BinlogClient.(binlog.PumpClient),
	// 		}
	// 		s.txn.SetOption(kv.BinlogInfo, info)
	// 	}
	// }

	// // Get the related table IDs.
	// relatedTables := s.GetSessionVars().TxnCtx.TableDeltaMap
	// tableIDs := make([]int64, 0, len(relatedTables))
	// for id := range relatedTables {
	// 	tableIDs = append(tableIDs, id)
	// }
	// // Set this option for 2 phase commit to validate schema lease.
	// s.txn.SetOption(kv.SchemaChecker, domain.NewSchemaChecker(domain.GetDomain(s), s.sessionVars.TxnCtx.SchemaVersion, tableIDs))

	// if err := s.txn.Commit(sessionctx.SetCommitCtx(ctx, s)); err != nil {
	// 	return errors.Trace(err)
	// }
	// return nil
}

func (s *inceptionCore) doCommitWithRetry(ctx context.Context) error {
	return nil

	// var txnSize int
	// if s.txn.Valid() {
	// 	txnSize = s.txn.Size()
	// }
	// err := s.doCommit(ctx)
	// if err != nil {
	// 	commitRetryLimit := s.sessionVars.RetryLimit
	// 	if s.sessionVars.DisableTxnAutoRetry && !s.sessionVars.InRestrictedSQL {
	// 		// Do not retry non-autocommit transactions.
	// 		// For autocommit single statement transactions, the history count is always 1.
	// 		// For explicit transactions, the statement count is more than 1.
	// 		history := GetHistory(s)
	// 		if history.Count() > 1 {
	// 			commitRetryLimit = 0
	// 		}
	// 	}
	// 	// Don't retry in BatchInsert mode. As a counter-example, insert into t1 select * from t2,
	// 	// BatchInsert already commit the first batch 1000 rows, then it commit 1000-2000 and retry the statement,
	// 	// Finally t1 will have more data than t2, with no errors return to user!
	// 	if s.isRetryableError(err) && !s.sessionVars.BatchInsert && commitRetryLimit > 0 {
	// 		log.Warnf("con:%d retryable error: %v, txn: %v", s.sessionVars.ConnectionID, err, s.txn)
	// 		// Transactions will retry 2 ~ commitRetryLimit times.
	// 		// We make larger transactions retry less times to prevent cluster resource outage.
	// 		txnSizeRate := float64(txnSize) / float64(kv.TxnTotalSizeLimit)
	// 		maxRetryCount := commitRetryLimit - int64(float64(commitRetryLimit-1)*txnSizeRate)
	// 		err = s.retry(ctx, uint(maxRetryCount))
	// 	}
	// }
	// s.cleanRetryInfo()

	// if isoLevelOneShot := &s.sessionVars.TxnIsolationLevelOneShot; isoLevelOneShot.State != 0 {
	// 	switch isoLevelOneShot.State {
	// 	case 1:
	// 		isoLevelOneShot.State = 2
	// 	case 2:
	// 		isoLevelOneShot.State = 0
	// 		isoLevelOneShot.Value = ""
	// 	}
	// }

	// if err != nil {
	// 	log.Warnf("con:%d finished txn:%v, %v", s.sessionVars.ConnectionID, s.txn, err)
	// 	return errors.Trace(err)
	// }
	// mapper := s.GetSessionVars().TxnCtx.TableDeltaMap
	// if s.statsCollector != nil && mapper != nil {
	// 	for id, item := range mapper {
	// 		s.statsCollector.Update(id, item.Delta, item.Count, &item.ColSize)
	// 	}
	// }
	// return nil
}

func (s *inceptionCore) CommitTxn(ctx context.Context) error {
	err := s.doCommitWithRetry(ctx)
	return errors.Trace(err)
}

func (s *inceptionCore) RollbackTxn(ctx context.Context) error {
	return nil
	// var err error
	// if s.txn.Valid() {
	// 	terror.Log(s.txn.Rollback())
	// }
	// s.cleanRetryInfo()
	// s.txn.changeToInvalid()
	// s.sessionVars.SetStatusFlag(mysql.ServerStatusInTrans, false)
	// return errors.Trace(err)
}

func (s *inceptionCore) GetClient() kv.Client {
	return s.store.GetClient()
}

func (s *inceptionCore) String() string {
	// TODO: how to print binded context in values appropriately?
	sessVars := s.sessionVars
	data := map[string]interface{}{
		"id":         sessVars.ConnectionID,
		"user":       sessVars.User,
		"currDBName": sessVars.CurrentDB,
		"status":     sessVars.Status,
		"strictMode": sessVars.StrictSQLMode,
	}
	// if s.txn.Valid() {
	// 	// if txn is committed or rolled back, txn is nil.
	// 	data["txn"] = s.txn.String()
	// }
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

func (s *inceptionCore) isRetryableError(err error) bool {
	if SchemaChangedWithoutRetry {
		return kv.IsRetryableError(err)
	}
	return kv.IsRetryableError(err) || domain.ErrInfoSchemaChanged.Equal(err)
}

func (s *inceptionCore) retry(ctx context.Context, maxCnt uint) error {
	return nil

	// connID := s.sessionVars.ConnectionID
	// if s.sessionVars.TxnCtx.ForUpdate {
	// 	return errForUpdateCantRetry.GenWithStackByArgs(connID)
	// }
	// s.sessionVars.RetryInfo.Retrying = true
	// var retryCnt uint
	// defer func() {
	// 	s.sessionVars.RetryInfo.Retrying = false
	// 	// s.txn.changeToInvalid()
	// 	// retryCnt only increments on retryable error, so +1 here.
	// 	s.sessionVars.SetStatusFlag(mysql.ServerStatusInTrans, false)
	// }()

	// nh := GetHistory(s)
	// var err error
	// var schemaVersion int64
	// for {
	// 	s.PrepareTxnCtx(ctx)
	// 	s.sessionVars.RetryInfo.ResetOffset()
	// 	for i, sr := range nh.history {
	// 		st := sr.st
	// 		if st.IsReadOnly() {
	// 			continue
	// 		}
	// 		schemaVersion, err = st.RebuildPlan()
	// 		if err != nil {
	// 			return errors.Trace(err)
	// 		}

	// 		if retryCnt == 0 {
	// 			// We do not have to log the query every time.
	// 			// We print the queries at the first try only.
	// 			log.Warnf("con:%d schema_ver:%d retry_cnt:%d query_num:%d sql:%s", connID, schemaVersion, retryCnt, i, sqlForLog(st.OriginText()))
	// 		} else {
	// 			log.Warnf("con:%d schema_ver:%d retry_cnt:%d query_num:%d", connID, schemaVersion, retryCnt, i)
	// 		}
	// 		s.sessionVars.StmtCtx = sr.stmtCtx
	// 		s.sessionVars.StmtCtx.ResetForRetry()
	// 		_, err = st.Exec(ctx)
	// 		if err != nil {
	// 			s.StmtRollback()
	// 			break
	// 		}
	// 		s.StmtCommit()
	// 	}
	// 	if hook := ctx.Value("preCommitHook"); hook != nil {
	// 		// For testing purpose.
	// 		hook.(func())()
	// 	}
	// 	if err == nil {
	// 		err = s.doCommit(ctx)
	// 		if err == nil {
	// 			break
	// 		}
	// 	}
	// 	if !s.isRetryableError(err) {
	// 		log.Warnf("con:%d session:%v, err:%v in retry", connID, s, err)
	// 		return errors.Trace(err)
	// 	}
	// 	retryCnt++
	// 	if retryCnt >= maxCnt {
	// 		log.Warnf("con:%d Retry reached max count %d", connID, retryCnt)
	// 		return errors.Trace(err)
	// 	}
	// 	log.Warnf("con:%d retryable error: %v, txn: %v", connID, err, s.txn)
	// 	kv.BackOff(retryCnt)
	// 	s.txn.changeToInvalid()
	// 	s.sessionVars.SetStatusFlag(mysql.ServerStatusInTrans, false)
	// }
	// return err
}

func sqlForLog(sql string) string {
	if len(sql) > sqlLogMaxLen {
		sql = sql[:sqlLogMaxLen] + fmt.Sprintf("(len:%d)", len(sql))
	}
	return executor.QueryReplacer.Replace(sql)
}

func (s *inceptionCore) sysSessionPool() *pools.ResourcePool {
	return domain.GetDomain(s).SysSessionPool()
}

// ExecRestrictedSQL implements RestrictedSQLExecutor interface.
// This is used for executing some restricted sql statements, usually executed during a normal statement execution.
// Unlike normal Exec, it doesn't reset statement status, doesn't commit or rollback the current transaction
// and doesn't write binlog.
func (s *inceptionCore) ExecRestrictedSQL(sctx sessionctx.Context, sql string) ([]chunk.Row, []*ast.ResultField, error) {
	ctx := context.TODO()

	// Use special session to execute the sql.
	tmp, err := s.sysSessionPool().Get()
	if err != nil {
		return nil, nil, err
	}
	se := tmp.(*inceptionCore)
	defer s.sysSessionPool().Put(tmp)

	return execRestrictedSQL(ctx, se, sql)
}

// ExecRestrictedSQLWithSnapshot implements RestrictedSQLExecutor interface.
// This is used for executing some restricted sql statements with snapshot.
// If current session sets the snapshot timestamp, then execute with this snapshot timestamp.
// Otherwise, execute with the current transaction start timestamp if the transaction is valid.
func (s *inceptionCore) ExecRestrictedSQLWithSnapshot(sctx sessionctx.Context, sql string) ([]chunk.Row, []*ast.ResultField, error) {
	ctx := context.TODO()

	// Use special session to execute the sql.
	tmp, err := s.sysSessionPool().Get()
	if err != nil {
		return nil, nil, err
	}
	se := tmp.(*inceptionCore)
	defer s.sysSessionPool().Put(tmp)
	var snapshot uint64
	// txn := s.Txn()
	// // if err != nil {
	// // 	return nil, nil, err
	// // }
	// if txn.Valid() {
	// 	snapshot = s.txn.StartTS()
	// }
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
				log.Error("set tidbSnapshot error", zap.Error(err))
			}
		}()
	}
	return execRestrictedSQL(ctx, se, sql)
}

func execRestrictedSQL(ctx context.Context, se *inceptionCore, sql string) ([]chunk.Row, []*ast.ResultField, error) {
	recordSets, err := se.Execute(ctx, sql)
	if err != nil {
		return nil, nil, err
	}

	var (
		rows   []chunk.Row
		fields []*ast.ResultField
	)
	// Execute all recordset, take out the first one as result.
	for i, rs := range recordSets {
		tmp, err := drainRecordSet(ctx, se, rs)
		if err != nil {
			return nil, nil, err
		}
		if err = rs.Close(); err != nil {
			return nil, nil, err
		}

		if i == 0 {
			rows = tmp
			fields = rs.Fields()
		}
	}
	return rows, fields, nil
}

func drainRecordSet(ctx context.Context, se *inceptionCore, rs sqlexec.RecordSet) ([]chunk.Row, error) {
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
func (s *inceptionCore) getExecRet(ctx sessionctx.Context, sql string) (string, error) {
	rows, fields, err := s.ExecRestrictedSQL(ctx, sql)
	if err != nil {
		return "", errors.Trace(err)
	}
	if len(rows) == 0 {
		return "", executor.ErrResultIsEmpty
	}
	d := rows[0].GetDatum(0, &fields[0].Column.FieldType)
	value, err := d.ToString()
	if err != nil {
		return "", errors.Trace(err)
	}
	return value, nil
}

// GetAllSysVars implements GlobalVarAccessor.GetAllSysVars interface.
func (s *inceptionCore) GetAllSysVars() (map[string]string, error) {
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
func (s *inceptionCore) GetGlobalSysVar(name string) (string, error) {
	if s.Value(sessionctx.Initing) != nil {
		// When running bootstrap or upgrade, we should not access global storage.
		return "", nil
	}
	sql := fmt.Sprintf(`SELECT VARIABLE_VALUE FROM %s.%s WHERE VARIABLE_NAME="%s";`,
		mysql.SystemDB, mysql.GlobalVariablesTable, name)
	sysVar, err := s.getExecRet(s, sql)
	if err != nil {
		if executor.ErrResultIsEmpty.Equal(err) {
			if sv, ok := variable.SysVars[name]; ok {
				return sv.Value, nil
			}
			return "", variable.UnknownSystemVar.GenWithStackByArgs(name)
		}
		return "", errors.Trace(err)
	}
	return sysVar, nil
}

// SetGlobalSysVar implements GlobalVarAccessor.SetGlobalSysVar interface.
func (s *inceptionCore) SetGlobalSysVar(name, value string) error {
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

func (s *inceptionCore) ParseSQL(ctx context.Context, sql, charset, collation string) ([]ast.StmtNode, error) {
	s.parser.SetSQLMode(s.sessionVars.SQLMode)
	stmts, _, err := s.parser.Parse(sql, charset, collation)
	return stmts, err
}

func (s *inceptionCore) SetProcessInfo(sql string, t time.Time, command byte) {
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

func (s *inceptionCore) SetMyProcessInfo(sql string, t time.Time, percent float64) {

	tmp := s.processInfo.Load()
	if tmp != nil {
		pi := tmp.(util.ProcessInfo)

		pi.Info = sql
		pi.Time = t
		pi.Percent = percent
		s.processInfo.Store(pi)
	}
}

func (s *inceptionCore) executeStatement(ctx context.Context, connID uint64, stmtNode ast.StmtNode, stmt ast.Statement, recordSets []sqlexec.RecordSet) ([]sqlexec.RecordSet, error) {
	s.SetValue(sessionctx.QueryString, stmt.OriginText())
	if _, ok := stmtNode.(ast.DDLNode); ok {
		s.SetValue(sessionctx.LastExecuteDDL, true)
	} else {
		s.ClearValue(sessionctx.LastExecuteDDL)
	}

	if s.Value(sessionctx.Initing) == nil {
		logStmt(stmtNode, s.sessionVars)
	}

	recordSet, err := runStmt(ctx, s, stmt)
	if err != nil {
		if !kv.ErrKeyExists.Equal(err) && s.Value(sessionctx.Initing) == nil {
			log.Warnf("con:%d schema_ver:%d session error:\n%v\n%s",
				connID, s.sessionVars.TxnCtx.SchemaVersion, errors.ErrorStack(err), s)
		}
		return nil, errors.Trace(err)
	}

	if recordSet != nil {
		recordSets = append(recordSets, recordSet)
	}
	return recordSets, nil
}

func (s *inceptionCore) Execute(ctx context.Context, sql string) (recordSets []sqlexec.RecordSet, err error) {
	if recordSets, err = s.execute(ctx, sql); err != nil {
		err = errors.Trace(err)
		s.sessionVars.StmtCtx.AppendError(err)
	}
	return
}

func (s *inceptionCore) execute(ctx context.Context, sql string) (recordSets []sqlexec.RecordSet, err error) {
	s.PrepareTxnCtx(ctx)
	connID := s.sessionVars.ConnectionID
	err = s.loadCommonGlobalVariablesIfNeeded()
	if err != nil {
		return nil, errors.Trace(err)
	}

	charsetInfo, collation := s.sessionVars.GetCharsetInfo()

	// Step1: Compile query string to abstract syntax trees(ASTs).
	stmtNodes, err := s.ParseSQL(ctx, sql, charsetInfo, collation)
	if err != nil {
		s.rollbackOnError(ctx)
		log.Warnf("con:%d parse error:\n%v\n%s", connID, err, sql)
		return nil, errors.Trace(err)
	}

	compiler := executor.Compiler{Ctx: s}
	for _, stmtNode := range stmtNodes {
		s.PrepareTxnCtx(ctx)

		// Step2: Transform abstract syntax tree to a physical plan(stored in executor.ExecStmt).
		// Some executions are done in compile stage, so we reset them before compile.
		if err := executor.ResetContextOfStmt(s, stmtNode); err != nil {
			return nil, errors.Trace(err)
		}
		stmt, err := compiler.Compile(ctx, stmtNode)
		if err != nil {
			s.rollbackOnError(ctx)
			if s.Value(sessionctx.Initing) == nil {
				log.Warnf("con:%d compile error:\n%v\n%s", connID, err, sql)
			}
			return nil, errors.Trace(err)
		}

		// Step3: Execute the physical plan.
		if recordSets, err = s.executeStatement(ctx, connID, stmtNode, stmt, recordSets); err != nil {
			return nil, errors.Trace(err)
		}
	}

	if s.sessionVars.ClientCapability&mysql.ClientMultiResults == 0 && len(recordSets) > 1 {
		// return the first recordset if client doesn't support ClientMultiResults.
		recordSets = recordSets[:1]
	}
	return recordSets, nil
}

// rollbackOnError makes sure the next statement starts a new transaction with the latest InfoSchema.
func (s *inceptionCore) rollbackOnError(ctx context.Context) {
	if !s.sessionVars.InTxn() {
		terror.Log(s.RollbackTxn(ctx))
	}
}

// PrepareStmt is used for executing prepare statement in binary protocol
func (s *inceptionCore) PrepareStmt(sql string) (stmtID uint32, paramCount int, fields []*ast.ResultField, err error) {
	if s.sessionVars.TxnCtx.InfoSchema == nil {
		// We don't need to create a transaction for prepare statement, just get information schema will do.
		s.sessionVars.TxnCtx.InfoSchema = domain.GetDomain(s).InfoSchema()
	}
	err = s.loadCommonGlobalVariablesIfNeeded()
	if err != nil {
		err = errors.Trace(err)
		return
	}

	ctx := context.Background()
	inTxn := s.GetSessionVars().InTxn()
	// NewPrepareExec may need startTS to build the executor, for example prepare statement has subquery in int.
	// So we have to call PrepareTxnCtx here.
	s.PrepareTxnCtx(ctx)
	prepareExec := executor.NewPrepareExec(s, executor.GetInfoSchema(s), sql)
	err = prepareExec.Next(ctx, nil)
	if err != nil {
		err = errors.Trace(err)
		return
	}
	if !inTxn {
		// We could start a transaction to build the prepare executor before, we should rollback it here.
		err = s.RollbackTxn(ctx)
		if err != nil {
			err = errors.Trace(err)
			return
		}
	}
	return prepareExec.ID, prepareExec.ParamCount, prepareExec.Fields, nil
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

// ExecutePreparedStmt executes a prepared statement.
func (s *inceptionCore) ExecutePreparedStmt(ctx context.Context, stmtID uint32, args ...interface{}) (sqlexec.RecordSet, error) {
	err := checkArgs(args...)
	if err != nil {
		return nil, errors.Trace(err)
	}

	s.PrepareTxnCtx(ctx)
	st, err := executor.CompileExecutePreparedStmt(s, stmtID, args...)
	if err != nil {
		return nil, errors.Trace(err)
	}
	logQuery(st.OriginText(), s.sessionVars)
	r, err := runStmt(ctx, s, st)
	return r, errors.Trace(err)
}

func (s *inceptionCore) DropPreparedStmt(stmtID uint32) error {
	vars := s.sessionVars
	if _, ok := vars.PreparedStmts[stmtID]; !ok {
		return plannercore.ErrStmtNotFound
	}
	vars.RetryInfo.DroppedPreparedStmtIDs = append(vars.RetryInfo.DroppedPreparedStmtIDs, stmtID)
	return nil
}

func (s *inceptionCore) Txn() kv.Transaction {
	if !s.txn.Valid() {
		return nil
	}
	return &s.txn
}

func (s *inceptionCore) NewTxn() error {
	// if s.txn.Valid() {
	// 	txnID := s.txn.StartTS()
	// 	ctx := context.TODO()
	// 	err := s.CommitTxn(ctx)
	// 	if err != nil {
	// 		return errors.Trace(err)
	// 	}
	// 	vars := s.GetSessionVars()
	// 	log.Infof("con:%d schema_ver:%d NewTxn() inside a transaction auto commit: %d", vars.ConnectionID, vars.TxnCtx.SchemaVersion, txnID)
	// }

	txn, err := s.store.Begin()
	if err != nil {
		return errors.Trace(err)
	}
	txn.SetCap(s.getMembufCap())
	txn.SetVars(s.sessionVars.KVVars)
	// s.txn.changeInvalidToValid(txn)
	is := domain.GetDomain(s).InfoSchema()
	s.sessionVars.TxnCtx = &variable.TransactionContext{
		InfoSchema:    is,
		SchemaVersion: is.SchemaMetaVersion(),
		CreateTime:    time.Now(),
		StartTS:       txn.StartTS(),
	}
	return nil
}

func (s *inceptionCore) SetValue(key fmt.Stringer, value interface{}) {
	s.mu.Lock()
	s.mu.values[key] = value
	s.mu.Unlock()
}

func (s *inceptionCore) Value(key fmt.Stringer) interface{} {
	s.mu.RLock()
	value := s.mu.values[key]
	s.mu.RUnlock()
	return value
}

func (s *inceptionCore) ClearValue(key fmt.Stringer) {
	s.mu.Lock()
	delete(s.mu.values, key)
	s.mu.Unlock()
}

// Close function does some clean work when session end.
func (s *inceptionCore) Close() {
	// if s.statsCollector != nil {
	// 	s.statsCollector.Delete()
	// }
	ctx := context.TODO()
	if err := s.RollbackTxn(ctx); err != nil {
		log.Error("session Close error:", errors.ErrorStack(err))
	}
}

// GetSessionVars implements the context.Context interface.
func (s *inceptionCore) GetSessionVars() *variable.SessionVars {
	return s.sessionVars
}

func (s *inceptionCore) Auth(user *auth.UserIdentity, authentication []byte, salt []byte) bool {
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
func CreateSession4Test(store kv.Storage) (*inceptionCore, error) {
	s, err := CreateSession(store)
	if err == nil {
		// initialize session variables for test.
		s.GetSessionVars().MaxChunkSize = 2
	}
	return s, errors.Trace(err)
}

// CreateSession creates a new session environment.
func CreateSession(store kv.Storage) (*inceptionCore, error) {
	s, err := createSession(store)
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Add auth here.
	do, err := domap.Get(store)
	if err != nil {
		return nil, errors.Trace(err)
	}
	pm := &privileges.UserPrivileges{
		Handle: do.PrivilegeHandle(),
	}
	privilege.BindPrivilegeManager(s, pm)

	// Add stats collector, and it will be freed by background stats worker
	// which periodically updates stats using the collected data.
	// if do.StatsHandle() != nil && do.StatsUpdating() {
	// 	s.statsCollector = do.StatsHandle().NewSessionStatsCollector()
	// }

	return s, nil
}

// loadSystemTZ loads systemTZ from mysql.tidb
func loadSystemTZ(se *inceptionCore) (string, error) {
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

// BootstrapSession runs the first time when the TiDB server start.
func BootstrapSession(store kv.Storage) (*domain.Domain, error) {
	// ver := getStoreBootstrapVersion(store)
	// if ver == notBootstrapped {
	// 	runInBootstrapSession(store, bootstrap)
	// } else if ver < currentBootstrapVersion {
	// 	runInBootstrapSession(store, upgrade)
	// }

	se, err := createSession(store)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// get system tz from mysql.tidb
	tz, err := loadSystemTZ(se)
	if err != nil {
		return nil, errors.Trace(err)
	}

	timeutil.SetSystemTZ(tz)

	dom := domain.GetDomain(se)
	err = dom.LoadPrivilegeLoop(se)
	if err != nil {
		return nil, errors.Trace(err)
	}

	se1, err := createSession(store)
	if err != nil {
		return nil, errors.Trace(err)
	}
	err = dom.UpdateTableStatsLoop(se1)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if raw, ok := store.(domain.EtcdBackend); ok {
		err = raw.StartGCWorker()
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	return dom, errors.Trace(err)
}

// GetDomain gets the associated domain for store.
func GetDomain(store kv.Storage) (*domain.Domain, error) {
	return domap.Get(store)
}

// runInBootstrapSession create a special session for boostrap to run.
// If no bootstrap and storage is remote, we must use a little lease time to
// bootstrap quickly, after bootstrapped, we will reset the lease time.
// TODO: Using a bootstap tool for doing this may be better later.
func runInBootstrapSession(store kv.Storage, bootstrap func(*inceptionCore)) {
	saveLease := schemaLease
	schemaLease = chooseMinLease(schemaLease, 100*time.Millisecond)
	s, err := createSession(store)
	if err != nil {
		// Bootstrap fail will cause program exit.
		log.Fatal(errors.ErrorStack(err))
	}
	schemaLease = saveLease

	s.SetValue(sessionctx.Initing, true)

	// 调整日志级别,忽略初始化时的info日志
	level := log.GetLevel()
	log.SetLevel(log.WarnLevel)
	bootstrap(s)
	finishBootstrap(store)

	s.ClearValue(sessionctx.Initing)

	dom := domain.GetDomain(s)
	dom.Close()
	domap.Delete(store)
	log.SetLevel(level)
}

func createSession(store kv.Storage) (*inceptionCore, error) {
	dom, err := domap.Get(store)
	if err != nil {
		return nil, errors.Trace(err)
	}
	s := &inceptionCore{
		store:           store,
		parser:          parser.New(),
		sessionVars:     variable.NewSessionVars(),
		ddlOwnerChecker: dom.DDL().OwnerManager(),

		LowerCaseTableNames: 1,
		// haveBegin:  false,
		// haveCommit: false,

		// tableCacheList: make(map[string]*TableInfo),
		// dbCacheList:    make(map[string]bool),

		// backupDBCacheList:    make(map[string]bool),
		// backupTableCacheList: make(map[string]bool),

		// Inc: config.GetGlobalConfig().Inc,
	}

	// if plannercore.PreparedPlanCacheEnabled() {
	// 	s.preparedPlanCache = kvcache.NewSimpleLRUCache(plannercore.PreparedPlanCacheCapacity)
	// }
	s.mu.values = make(map[fmt.Stringer]interface{})
	domain.BindDomain(s, dom)
	// session implements variable.GlobalVarAccessor. Bind it to ctx.
	s.sessionVars.GlobalVarsAccessor = s
	s.sessionVars.BinlogClient = binloginfo.GetPumpClient()
	// s.txn.init()
	return s, nil
}

// createSessionWithDomain creates a new Session and binds it with a Domain.
// We need this because when we start DDL in Domain, the DDL need a session
// to change some system tables. But at that time, we have been already in
// a lock context, which cause we can't call createSesion directly.
func createSessionWithDomain(store kv.Storage, dom *domain.Domain) (*inceptionCore, error) {
	s := &inceptionCore{
		store:       store,
		parser:      parser.New(),
		sessionVars: variable.NewSessionVars(),
	}
	// if plannercore.PreparedPlanCacheEnabled() {
	// 	s.preparedPlanCache = kvcache.NewSimpleLRUCache(plannercore.PreparedPlanCacheCapacity)
	// }
	s.mu.values = make(map[fmt.Stringer]interface{})
	domain.BindDomain(s, dom)
	// session implements variable.GlobalVarAccessor. Bind it to ctx.
	s.sessionVars.GlobalVarsAccessor = s
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

// loadCommonGlobalVariablesIfNeeded loads and applies commonly used global variables for the session.
func (s *inceptionCore) loadCommonGlobalVariablesIfNeeded() error {
	vars := s.sessionVars
	if vars.CommonGlobalLoaded {
		return nil
	}
	if s.Value(sessionctx.Initing) != nil {
		// When running bootstrap or upgrade, we should not access global storage.
		return nil
	}

	var err error
	// Use GlobalVariableCache if TiDB just loaded global variables within 2 second ago.
	// When a lot of connections connect to TiDB simultaneously, it can protect TiKV meta region from overload.
	gvc := domain.GetDomain(s).GetGlobalVarsCache()
	succ, rows, fields := gvc.Get()
	if !succ {
		// Set the variable to true to prevent cyclic recursive call.
		vars.CommonGlobalLoaded = true
		rows, fields, err = s.ExecRestrictedSQL(s, loadCommonGlobalVarsSQL)
		if err != nil {
			vars.CommonGlobalLoaded = false
			log.Errorf("Failed to load common global variables.")
			return errors.Trace(err)
		}
		gvc.Update(rows, fields)
	}

	for _, row := range rows {
		varName := row.GetString(0)
		varVal := row.GetDatum(1, &fields[1].Column.FieldType)
		if _, ok := vars.GetSystemVar(varName); !ok {
			err = variable.SetSessionSystemVar(s.sessionVars, varName, varVal)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	vars.CommonGlobalLoaded = true
	return nil
}

// PrepareTxnCtx starts a goroutine to begin a transaction if needed, and creates a new transaction context.
// It is called before we execute a sql query.
func (s *inceptionCore) PrepareTxnCtx(ctx context.Context) {
	return
	// if s.txn.validOrPending() {
	// 	return
	// }

	// txnFuture := s.getTxnFuture(ctx)
	// s.txn.changeInvalidToPending(txnFuture)
	// is := domain.GetDomain(s).InfoSchema()
	// s.sessionVars.TxnCtx = &variable.TransactionContext{
	// 	InfoSchema:    is,
	// 	SchemaVersion: is.SchemaMetaVersion(),
	// 	CreateTime:    time.Now(),
	// }
	// if !s.sessionVars.IsAutocommit() {
	// 	s.sessionVars.SetStatusFlag(mysql.ServerStatusInTrans, true)
	// }
}

// RefreshTxnCtx implements context.RefreshTxnCtx interface.
func (s *inceptionCore) RefreshTxnCtx(ctx context.Context) error {
	if err := s.doCommit(ctx); err != nil {
		return errors.Trace(err)
	}

	return errors.Trace(s.NewTxn())
}

// ActivePendingTxn implements Context.ActivePendingTxn interface.
func (s *inceptionCore) ActivePendingTxn() error {
	if s.txn.Valid() {
		return nil
	}
	// txnCap := s.getMembufCap()
	// // The transaction status should be pending.
	// if err := s.txn.changePendingToValid(txnCap); err != nil {
	// 	return errors.Trace(err)
	// }
	// s.sessionVars.TxnCtx.StartTS = s.txn.StartTS()
	return nil
}

// InitTxnWithStartTS create a transaction with startTS.
func (s *inceptionCore) InitTxnWithStartTS(startTS uint64) error {
	if s.txn.Valid() {
		return nil
	}

	// no need to get txn from txnFutureCh since txn should init with startTs
	// txn, err := s.store.BeginWithStartTS(startTS)
	// if err != nil {
	// 	return errors.Trace(err)
	// }
	// s.txn.changeInvalidToValid(txn)
	// s.txn.SetCap(s.getMembufCap())
	// err = s.loadCommonGlobalVariablesIfNeeded()
	// if err != nil {
	// 	return errors.Trace(err)
	// }
	return nil
}

// GetStore gets the store of session.
func (s *inceptionCore) GetStore() kv.Storage {
	return s.store
}

func (s *inceptionCore) ShowProcess() util.ProcessInfo {
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

// func (s *Inception) getTxnFuture(ctx context.Context) *txnFuture {
// 	oracleStore := s.store.GetOracle()
// 	tsFuture := oracleStore.GetTimestampAsync(ctx)
// 	return &txnFuture{tsFuture, s.store}
// }

// StmtCommit implements the sessionctx.Context interface.
func (s *inceptionCore) StmtCommit() {
	// if s.txn.fail != nil {
	// 	return
	// }

	// defer s.txn.cleanup()
	// st := &s.txn
	// err := kv.WalkMemBuffer(st.buf, func(k kv.Key, v []byte) error {
	// 	// gofail: var mockStmtCommitError bool
	// 	// if mockStmtCommitError {
	// 	// 	return errors.New("mock stmt commit error")
	// 	// }
	// 	if len(v) == 0 {
	// 		return errors.Trace(st.Transaction.Delete(k))
	// 	}
	// 	return errors.Trace(st.Transaction.Set(k, v))
	// })
	// if err != nil {
	// 	s.txn.fail = errors.Trace(err)
	// 	return
	// }

	// // Need to flush binlog.
	// for tableID, delta := range st.mutations {
	// 	mutation := getBinlogMutation(s, tableID)
	// 	mergeToMutation(mutation, delta)
	// }

	// if len(st.dirtyTableOP) > 0 {
	// 	dirtyDB := executor.GetDirtyDB(s)
	// 	for _, op := range st.dirtyTableOP {
	// 		mergeToDirtyDB(dirtyDB, op)
	// 	}
	// }
}

// StmtRollback implements the sessionctx.Context interface.
func (s *inceptionCore) StmtRollback() {
	// s.txn.cleanup()
	return
}

// StmtGetMutation implements the sessionctx.Context interface.
func (s *inceptionCore) StmtGetMutation(tableID int64) *binlog.TableMutation {
	return nil
	// st := &s.txn
	// if _, ok := st.mutations[tableID]; !ok {
	// 	st.mutations[tableID] = &binlog.TableMutation{TableId: tableID}
	// }
	// return st.mutations[tableID]
}

func (s *inceptionCore) StmtAddDirtyTableOP(op int, tid int64, handle int64, row []types.Datum) {
	// s.txn.dirtyTableOP = append(s.txn.dirtyTableOP, dirtyTableOperation{op, tid, handle, row})
}

// GetRows4Test gets all the rows from a RecordSet, only used for test.
func (s *inceptionCore) GetRows4Test(ctx context.Context, rs ast.RecordSet) ([]chunk.Row, error) {
	if rs == nil {
		return nil, nil
	}
	var rows []chunk.Row
	chk := rs.NewChunk()
	for {
		// Since we collect all the rows, we can not reuse the chunk.
		iter := chunk.NewIterator4Chunk(chk)

		err := rs.Next(ctx, chk)
		if err != nil {
			return nil, errors.Trace(err)
		}
		if chk.NumRows() == 0 {
			break
		}

		for row := iter.Begin(); row != iter.End(); row = iter.Next() {
			rows = append(rows, row)
		}
		chk = chunk.Renew(chk, s.GetSessionVars().MaxChunkSize)
	}
	return rows, nil
}
