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

package connect_grpcadapter

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/fullstorydev/grpchan/grpchantesting"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/bufbuild/connect-go"
)

func TestAdapter(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	l2, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	var ti testInterceptor
	h := NewConnectHandler(connect.WithInterceptors(&ti))
	grpchantesting.RegisterTestServiceServer(h, &grpchantesting.TestServer{})

	// HTTP 1.1
	svr := http.Server{Handler: h}
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		err := svr.Serve(l)
		require.Equal(t, http.ErrServerClosed, err)
	}()
	defer func() {
		err := svr.Close()
		require.NoError(t, err)
		<-serverDone
	}()

	// and HTTP/2
	svr2 := http.Server{
		Handler: h2c.NewHandler(h, &http2.Server{}),
	}
	server2Done := make(chan struct{})
	go func() {
		defer close(server2Done)
		err := svr2.Serve(l2)
		require.Equal(t, http.ErrServerClosed, err)
	}()
	defer func() {
		err := svr2.Close()
		require.NoError(t, err)
		<-server2Done
	}()

	makeHttp1Client := func() (*contentTypeCapturingClient, string) {
		return &contentTypeCapturingClient{transport: http.DefaultTransport}, "http://" + l.Addr().String()
	}
	makeHttp2Client := func() (*contentTypeCapturingClient, string) {
		return &contentTypeCapturingClient{transport: &http2.Transport{
				AllowHTTP: true,
				DialTLS: func(network, addr string, _ *tls.Config) (net.Conn, error) {
					return net.Dial(network, addr)
				},
			}},
			"http://" + l2.Addr().String()
	}
	testCases := []struct {
		name                string
		factory             func() (*contentTypeCapturingClient, string)
		supportsBidiAndGrpc bool
	}{
		{
			name:                "http-1.1",
			factory:             makeHttp1Client,
			supportsBidiAndGrpc: false,
		},
		{
			name:                "http-2",
			factory:             makeHttp2Client,
			supportsBidiAndGrpc: true,
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			t.Run("connect", func(t *testing.T) {
				ti.reset()
				cl, baseUrl := testCase.factory()
				ch := NewChannel(cl, baseUrl, connect.WithInterceptors(&ti))
				grpchantesting.RunChannelTestCases(t, ch, testCase.supportsBidiAndGrpc)
				assert.Equal(t, []string{"application/connect+proto", "application/proto"}, cl.getContentTypes())
				ti.checkCounts(t)
			})
			t.Run("grpcweb", func(t *testing.T) {
				ti.reset()
				cl, baseUrl := testCase.factory()
				ch := NewChannel(cl, baseUrl, connect.WithInterceptors(&ti), connect.WithGRPCWeb())
				grpchantesting.RunChannelTestCases(t, ch, testCase.supportsBidiAndGrpc)
				assert.Equal(t, []string{"application/grpc-web+proto"}, cl.getContentTypes())
				ti.checkCounts(t)
			})
			if testCase.supportsBidiAndGrpc {
				t.Run("grpc", func(t *testing.T) {
					ti.reset()
					cl, baseUrl := testCase.factory()
					ch := NewChannel(cl, baseUrl, connect.WithInterceptors(&ti), connect.WithGRPC())
					grpchantesting.RunChannelTestCases(t, ch, true)
					assert.Equal(t, []string{"application/grpc+proto"}, cl.getContentTypes())
					ti.checkCounts(t)
				})
			}
		})
	}
}

type testInterceptor struct {
	unaryCount        atomic.Int32
	streamClientCount atomic.Int32
	streamServerCount atomic.Int32
}

func (ti *testInterceptor) reset() {
	ti.unaryCount.Store(0)
	ti.streamClientCount.Store(0)
	ti.streamServerCount.Store(0)
}

func (ti *testInterceptor) checkCounts(t *testing.T) {
	count := ti.unaryCount.Load()
	assert.True(t, count > 0)
	assert.True(t, count%2 == 0)
	count = ti.streamClientCount.Load()
	assert.True(t, count > 0)
	assert.Equal(t, count, ti.streamServerCount.Load())
}

func (ti *testInterceptor) WrapUnary(unaryFunc connect.UnaryFunc) connect.UnaryFunc {
	return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
		ti.unaryCount.Add(1)
		return unaryFunc(ctx, req)
	}
}

func (ti *testInterceptor) WrapStreamingClient(clientFunc connect.StreamingClientFunc) connect.StreamingClientFunc {
	return func(ctx context.Context, spec connect.Spec) connect.StreamingClientConn {
		ti.streamClientCount.Add(1)
		return clientFunc(ctx, spec)
	}
}

func (ti *testInterceptor) WrapStreamingHandler(handlerFunc connect.StreamingHandlerFunc) connect.StreamingHandlerFunc {
	return func(ctx context.Context, stream connect.StreamingHandlerConn) error {
		ti.streamServerCount.Add(1)
		return handlerFunc(ctx, stream)
	}
}

type contentTypeCapturingClient struct {
	transport    http.RoundTripper
	mu           sync.Mutex
	contentTypes map[string]struct{}
}

func (c *contentTypeCapturingClient) Do(request *http.Request) (*http.Response, error) {
	c.addContentType(request.Header.Get("content-type"))
	return c.transport.RoundTrip(request)
}

func (c *contentTypeCapturingClient) addContentType(contentType string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.contentTypes == nil {
		c.contentTypes = map[string]struct{}{}
	}
	c.contentTypes[contentType] = struct{}{}
}

func (c *contentTypeCapturingClient) getContentTypes() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	results := make([]string, 0, len(c.contentTypes))
	for k := range c.contentTypes {
		results = append(results, k)
	}
	sort.Strings(results)
	return results
}
