// Copyright 2021-2022 Buf Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package connect_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/bufbuild/connect-go"
	"github.com/bufbuild/connect-go/internal/assert"
	pingv1 "github.com/bufbuild/connect-go/internal/gen/connect/ping/v1"
	"github.com/bufbuild/connect-go/internal/gen/connect/ping/v1/pingv1connect"
)

func TestNewClient_InitFailure(t *testing.T) {
	t.Parallel()
	client := pingv1connect.NewPingServiceClient(
		http.DefaultClient,
		"http://127.0.0.1:8080",
		// This triggers an error during initialization, so each call will short circuit returning an error.
		connect.WithSendCompression("invalid"),
	)
	validateExpectedError := func(t *testing.T, err error) {
		t.Helper()
		assert.NotNil(t, err)
		var connectErr *connect.Error
		assert.True(t, errors.As(err, &connectErr))
		assert.Equal(t, connectErr.Message(), `unknown compression "invalid"`)
	}

	t.Run("unary", func(t *testing.T) {
		t.Parallel()
		_, err := client.Ping(context.Background(), connect.NewRequest(&pingv1.PingRequest{}))
		validateExpectedError(t, err)
	})

	t.Run("bidi", func(t *testing.T) {
		t.Parallel()
		bidiStream := client.CumSum(context.Background())
		err := bidiStream.Send(&pingv1.CumSumRequest{})
		validateExpectedError(t, err)
	})

	t.Run("client_stream", func(t *testing.T) {
		t.Parallel()
		clientStream := client.Sum(context.Background())
		err := clientStream.Send(&pingv1.SumRequest{})
		validateExpectedError(t, err)
	})

	t.Run("server_stream", func(t *testing.T) {
		t.Parallel()
		_, err := client.CountUp(context.Background(), connect.NewRequest(&pingv1.CountUpRequest{Number: 3}))
		validateExpectedError(t, err)
	})
}

func TestClientEndToEnd(t *testing.T) {
	t.Parallel()
	mux := http.NewServeMux()
	mux.Handle(pingv1connect.NewPingServiceHandler(pingServer{}))
	server := httptest.NewUnstartedServer(mux)
	server.EnableHTTP2 = true
	server.StartTLS()
	t.Cleanup(server.Close)

	run := func(t *testing.T, opts ...connect.ClientOption) {
		httpClient := &drainTestClient{t: t, client: server.Client()}
		client := pingv1connect.NewPingServiceClient(
			httpClient,
			server.URL,
			connect.WithClientOptions(opts...),
			connect.WithInterceptors(&assertPeerInterceptor{t}),
		)
		ctx := context.Background()
		// unary
		_, err := client.Ping(ctx, connect.NewRequest(&pingv1.PingRequest{}))
		assert.Nil(t, err)
		assert.True(t, httpClient.CheckDrained())
		// client streaming
		ctx, cancel := context.WithCancel(ctx)
		t.Cleanup(cancel)
		clientStream := client.Sum(ctx)
		assert.NotZero(t, clientStream.Peer().Addr)
		assert.NotZero(t, clientStream.Peer().Protocol)
		err = clientStream.Send(&pingv1.SumRequest{})
		assert.Nil(t, err)
		_, closeErr := clientStream.CloseAndReceive()
		assert.Nil(t, closeErr)
		assert.True(t, httpClient.CheckDrained())
		// server streaming
		ctx, cancel = context.WithCancel(ctx)
		t.Cleanup(cancel)
		serverStream, err := client.CountUp(ctx, connect.NewRequest(&pingv1.CountUpRequest{Number: 1}))
		assert.Nil(t, err)
		for {
			if !serverStream.Receive() {
				assert.Nil(t, serverStream.Err())
				break
			}
		}
		assert.True(t, httpClient.CheckDrained())
		assert.Nil(t, serverStream.Close())
		// bidi streaming
		ctx, cancel = context.WithCancel(ctx)
		t.Cleanup(cancel)
		bidiStream := client.CumSum(ctx)
		assert.NotZero(t, bidiStream.Peer().Addr)
		assert.NotZero(t, bidiStream.Peer().Protocol)
		err = bidiStream.Send(&pingv1.CumSumRequest{})
		assert.Nil(t, err)
		assert.Nil(t, bidiStream.CloseRequest())
		for {
			_, err := bidiStream.Receive()
			if err != nil {
				assert.True(t, errors.Is(err, io.EOF))
				break
			}
		}
		assert.True(t, httpClient.CheckDrained())
		assert.Nil(t, bidiStream.CloseResponse())
	}

	t.Run("connect", func(t *testing.T) {
		t.Parallel()
		run(t)
	})
	t.Run("grpc", func(t *testing.T) {
		t.Parallel()
		run(t, connect.WithGRPC())
	})
	t.Run("grpcweb", func(t *testing.T) {
		t.Parallel()
		run(t, connect.WithGRPCWeb())
	})
}

type assertPeerInterceptor struct {
	tb testing.TB
}

func (a *assertPeerInterceptor) WrapUnary(next connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		assert.NotZero(a.tb, req.Peer().Addr)
		assert.NotZero(a.tb, req.Peer().Protocol)
		return next(ctx, req)
	}
}

func (a *assertPeerInterceptor) WrapStreamingClient(next connect.StreamingClientFunc) connect.StreamingClientFunc {
	return func(ctx context.Context, spec connect.Spec) connect.StreamingClientConn {
		conn := next(ctx, spec)
		assert.NotZero(a.tb, conn.Peer().Addr)
		assert.NotZero(a.tb, conn.Peer().Protocol)
		assert.NotZero(a.tb, conn.Spec())
		return conn
	}
}

func (a *assertPeerInterceptor) WrapStreamingHandler(next connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return func(ctx context.Context, conn connect.StreamingHandlerConn) error {
		assert.NotZero(a.tb, conn.Peer().Addr)
		assert.NotZero(a.tb, conn.Peer().Protocol)
		assert.NotZero(a.tb, conn.Spec())
		return next(ctx, conn)
	}
}

type drainTestClient struct {
	t       *testing.T
	client  *http.Client
	drained atomic.Bool
}

func (c *drainTestClient) Do(req *http.Request) (*http.Response, error) {
	resp, err := c.client.Do(req)
	if err != nil || resp.Body == nil {
		return resp, err
	}
	resp.Body = &drainTestReader{t: c.t, ReadCloser: resp.Body, drained: &c.drained}
	return resp, nil
}

func (c *drainTestClient) CheckDrained() bool {
	return c.drained.Swap(false)
}

type drainTestReader struct {
	t *testing.T
	io.ReadCloser
	drained *atomic.Bool
}

func (r *drainTestReader) Read(p []byte) (n int, err error) {
	n, err = r.ReadCloser.Read(p)
	if err != nil {
		r.t.Logf("drained response body: %v", err)
		r.drained.Store(true)
	}
	return n, err
}
