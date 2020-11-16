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
	"sync"

	"github.com/hanchuanchuan/goInception/util"
	log "github.com/sirupsen/logrus"
)

// Server is the MySQL protocol server
type Server struct {
	rwlock  *sync.RWMutex
	clients map[uint64]*session
	// osc进程列表
	oscProcessList map[string]*util.OscProcessInfo
}

// ConnectionCount gets current connection count.
func (s *Server) ConnectionCount() int {
	var cnt int
	s.rwlock.RLock()
	cnt = len(s.clients)
	s.rwlock.RUnlock()
	return cnt
}

func (s *Server) InitOscProcessList() {
	if s.oscProcessList == nil {
		s.oscProcessList = make(map[string]*util.OscProcessInfo, 0)
	}
}

// AddOscProcess 添加osc进程
func (s *Server) AddOscProcess(p *util.OscProcessInfo) {
	s.oscProcessList[p.Sqlsha1] = p
}

// ShowOscProcessList 返回osc进程列表
func (s *Server) ShowOscProcessList() map[string]*util.OscProcessInfo {
	return s.oscProcessList
}

// ShowProcessList implements the SessionManager interface.
func (s *Server) ShowProcessList() map[uint64]util.ProcessInfo {
	s.rwlock.RLock()

	rs := make(map[uint64]util.ProcessInfo, len(s.clients))
	for _, client := range s.clients {
		pi := client.ShowProcess()
		rs[pi.ID] = pi
	}
	s.rwlock.RUnlock()
	return rs
}

// Kill implements the SessionManager interface.
func (s *Server) Kill(connectionID uint64, query bool) {
	s.rwlock.Lock()
	defer s.rwlock.Unlock()
	log.Infof("[server] Kill connectionID %d, query %t]", connectionID, query)

	conn, ok := s.clients[connectionID]
	if !ok {
		return
	}

	conn.mu.RLock()
	cancelFunc := conn.mu.cancelFunc
	conn.mu.RUnlock()
	if cancelFunc != nil {
		cancelFunc()
	}
}

const (
	connStatusDispatching int32 = iota
	connStatusReading
	connStatusShutdown     // Closed by server.
	connStatusWaitShutdown // Notified by server to close.
)
