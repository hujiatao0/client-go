// Copyright 2026 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package client

import (
	"fmt"
	"math/rand/v2"
	"os"
	"sync/atomic"
	"time"

	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/tikv/client-go/v2/tikvrpc"
	clienttrace "github.com/tikv/client-go/v2/trace"
	"go.uber.org/zap"
)

const diagnosticStreamMetadataKey = "x-cq-trace-stream"

var diagnosticEntrySequence atomic.Uint64
var diagnosticEnabled atomic.Bool

func init() {
	diagnosticEnabled.Store(os.Getenv("TIDB_CQ_TRACE_FILE") != "")
}

func diagnosticRequestMethod(req *tikvpb.BatchCommandsRequest_Request) string {
	if !diagnosticEnabled.Load() || !clienttrace.IsCategoryEnabled(clienttrace.CategoryKVRequest) {
		return ""
	}
	// Capture before encodeRequestCmd replaces the command with encoded bytes.
	return fmt.Sprintf("%T", req.Cmd)
}

// A stream nonce is shared with TiKV through initial metadata. Request IDs are
// scoped to a stream, so a bare request ID is not a cross-process identity.
func newDiagnosticStreamID() uint64 {
	// Configure identity even while runtime capture is off; otherwise enabling
	// capture would require reconnecting and could move the stream to another CQ.
	if !diagnosticEnabled.Load() {
		return 0
	}
	return rand.Uint64() | 1
}

func (e *batchCommandsEntry) traceStage(name string, at time.Time, streamID, requestID uint64) {
	if e.ctx == nil || !diagnosticEnabled.Load() || !clienttrace.IsCategoryEnabled(clienttrace.CategoryKVRequest) {
		return
	}
	if e.diagnosticEntryID.Load() == 0 {
		e.diagnosticEntryID.CompareAndSwap(0, diagnosticEntrySequence.Add(1))
	}
	if state := e.batchState.Load(); state != nil {
		streamID = state.diagnosticStreamID
	}
	if requestID == 0 {
		requestID = e.requestID.Load()
	}
	clienttrace.TraceEvent(e.ctx, clienttrace.CategoryKVRequest, name,
		zap.String("rpc", e.diagnosticMethod),
		zap.Uint64("entry_id", e.diagnosticEntryID.Load()),
		zap.Uint64("stream_id", streamID), zap.Uint64("request_id", requestID),
		zap.Int64("elapsed_ns", at.Sub(e.reqArriveAt).Nanoseconds()),
		zap.Int64("stage_unix_ns", at.UnixNano()),
	)
}

func (e *batchCommandsEntry) traceResponseDetails(response *tikvpb.BatchCommandsResponse_Response) {
	if e.ctx == nil || !diagnosticEnabled.Load() || !clienttrace.IsCategoryEnabled(clienttrace.CategoryKVRequest) {
		return
	}
	rpc, err := tikvrpc.FromBatchCommandsResponse(response)
	if err != nil || rpc == nil {
		return
	}
	details := rpc.GetExecDetailsV2()
	if details == nil {
		return
	}
	state := e.batchState.Load()
	var streamID uint64
	if state != nil {
		streamID = state.diagnosticStreamID
	}
	td, wd := details.GetTimeDetailV2(), details.GetWriteDetail()
	fields := []zap.Field{
		zap.Uint64("entry_id", e.diagnosticEntryID.Load()),
		zap.Uint64("stream_id", streamID), zap.Uint64("request_id", e.requestID.Load()),
		zap.Uint64("server_rpc_ns", td.GetTotalRpcWallTimeNs()),
		zap.Uint64("response_batch_wait_ns", td.GetKvGrpcWaitTimeNs()),
		zap.Uint64("kv_wait_ns", td.GetWaitWallTimeNs()),
		zap.Uint64("kv_process_ns", td.GetProcessWallTimeNs()),
		zap.Uint64("latch_ns", wd.GetLatchWaitNanos()),
		zap.Uint64("scheduler_process_ns", wd.GetProcessNanos()),
		zap.Uint64("throttle_ns", wd.GetThrottleNanos()),
		zap.Uint64("store_batch_wait_ns", wd.GetStoreBatchWaitNanos()),
		zap.Uint64("propose_send_wait_ns", wd.GetProposeSendWaitNanos()),
		zap.Uint64("persist_ns", wd.GetPersistLogNanos()),
		zap.Uint64("wal_ns", wd.GetRaftDbSyncLogNanos()),
		zap.Uint64("commit_log_ns", wd.GetCommitLogNanos()),
		zap.Uint64("apply_wait_ns", wd.GetApplyBatchWaitNanos()),
		zap.Uint64("apply_ns", wd.GetApplyLogNanos()),
	}
	if pool := details.GetReadPoolTaskDetails(); pool != nil {
		fields = append(fields,
			zap.Uint64("read_queue_wait_ns", pool.GetTotalQueueWaitNanos()),
			zap.Uint64("read_wake_wait_ns", pool.GetTotalWakeWaitNanos()),
			zap.Uint64("read_poll_wall_ns", pool.GetPollWallNanos()),
		)
	}
	clienttrace.TraceEvent(e.ctx, clienttrace.CategoryKVRequest, "batch.response.details", fields...)
}
