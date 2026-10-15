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
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clienttrace "github.com/tikv/client-go/v2/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestDiagnosticBatchKeepsIdentityAfterStreamRecreation(t *testing.T) {
	previous := diagnosticEnabled.Swap(true)
	clienttrace.SetIsCategoryEnabledFunc(func(category clienttrace.Category) bool { return category == clienttrace.CategoryKVRequest })
	t.Cleanup(func() {
		diagnosticEnabled.Store(previous)
		clienttrace.SetIsCategoryEnabledFunc(nil)
		clienttrace.SetTraceEventFunc(nil)
	})
	var mu sync.Mutex
	var events []map[string]any
	clienttrace.SetTraceEventFunc(func(ctx context.Context, _ clienttrace.Category, _ string, fields ...zap.Field) {
		require.Equal(t, []byte{1, 2}, clienttrace.TraceIDFromContext(ctx))
		encoder := zapcore.NewMapObjectEncoder()
		for _, field := range fields {
			field.AddTo(encoder)
		}
		mu.Lock()
		events = append(events, encoder.Fields)
		mu.Unlock()
	})
	stream := &batchCommandsStream{diagnosticID: 7}
	entry := &batchCommandsEntry{ctx: clienttrace.ContextWithTraceID(context.Background(), []byte{1, 2}), reqArriveAt: time.Now()}
	entry.requestID.Store(42)
	entry.batchState.Store(&batchCommandsRequestState{stream: stream, diagnosticStreamID: 7})
	// A recreated stream must not relabel the old request's immutable state.
	stream.diagnosticID = 9
	var workers sync.WaitGroup
	for range 8 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			entry.traceStage("batch.recv", time.Now(), 0, 0)
		}()
	}
	workers.Wait()
	require.Len(t, events, 8)
	for _, event := range events {
		require.Equal(t, uint64(7), event["stream_id"])
		require.Equal(t, uint64(42), event["request_id"])
		require.Equal(t, events[0]["entry_id"], event["entry_id"])
	}
}

func TestDiagnosticStreamIdentityExistsWhileCaptureIsOff(t *testing.T) {
	previous := diagnosticEnabled.Swap(true)
	clienttrace.SetIsCategoryEnabledFunc(func(clienttrace.Category) bool { return false })
	t.Cleanup(func() {
		diagnosticEnabled.Store(previous)
		clienttrace.SetIsCategoryEnabledFunc(nil)
	})
	require.NotZero(t, newDiagnosticStreamID())
	diagnosticEnabled.Store(false)
	require.Zero(t, newDiagnosticStreamID())
}
