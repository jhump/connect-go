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
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"

	"github.com/bufbuild/connect-go"
)

// ConnectHandler represents the server-side of Connect services. Service
// implementations are registered with it, and then the handler is
// registered with an HTTP server to actually expose the services.
type ConnectHandler struct {
	mux  http.ServeMux
	opts []connect.HandlerOption
}

var _ grpc.ServiceRegistrar = (*ConnectHandler)(nil)
var _ http.Handler = (*ConnectHandler)(nil)

// NewConnectHandler returns a new handler that can be registered with an HTTP
// server. Callers should first register gRPC service implementations using
// the returned handler. The handler will support gRPC, gRPC-web, and Connect
// protocols.
//
// The given options allow customizing the behavior of the server. To use a
// gRPC interceptor (instead of Connect interceptor), wrap the returned handler
// using grpchan.WithInterceptor and register gRPC service implementations via
// that wrapper.
//
// If using any custom codecs, they MUST first be wrapped in a CodecAdapter
// or else there will be runtime errors when trying to use them.
func NewConnectHandler(opts ...connect.HandlerOption) *ConnectHandler {
	return &ConnectHandler{opts: addDefaultCodecsHandler(opts)}
}

// RegisterService implements grpc.ServiceRegistrar, allowing gRPC service
// implementations to be registered with the handler.
func (c *ConnectHandler) RegisterService(desc *grpc.ServiceDesc, impl interface{}) {
	for _, method := range desc.Methods {
		path := fmt.Sprintf("/%s/%s", desc.ServiceName, method.MethodName)
		c.mux.Handle(path, connect.NewUnaryHandler[deferredAny, indirectAny](
			path,
			unaryHandleFunc(impl, method.Handler),
			c.opts...,
		))
	}
	for _, method := range desc.Streams {
		path := fmt.Sprintf("/%s/%s", desc.ServiceName, method.StreamName)
		switch {
		case method.ClientStreams && method.ServerStreams:
			c.mux.Handle(path, connect.NewBidiStreamHandler[deferredAny, indirectAny](
				path,
				streamHandleFuncBidi(impl, method.Handler),
				c.opts...,
			))
		case method.ClientStreams:
			c.mux.Handle(path, connect.NewClientStreamHandler[deferredAny, indirectAny](
				path,
				streamHandleFuncClientStreams(impl, method.Handler),
				c.opts...,
			))
		case method.ServerStreams:
			c.mux.Handle(path, connect.NewServerStreamHandler[deferredAny, indirectAny](
				path,
				streamHandleFuncServerStreams(impl, method.Handler),
				c.opts...,
			))
		}
	}
	c.mux.Handle("/"+desc.ServiceName, http.NotFoundHandler())
}

// ServeHTTP implements http.Handler, allowing registered services to be
// exposed via an HTTP server.
func (c *ConnectHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	c.mux.ServeHTTP(writer, request)
}

func unaryHandleFunc(
	impl any,
	handler func(srv any, ctx context.Context, dec func(any) error, interceptor grpc.UnaryServerInterceptor) (any, error),
) func(context.Context, *connect.Request[deferredAny]) (*connect.Response[indirectAny], error) {
	return func(ctx context.Context, req *connect.Request[deferredAny]) (*connect.Response[indirectAny], error) {
		ctx = metadata.NewIncomingContext(ctx, toMetadata(req.Header()))
		sts := serverTransportStream{method: req.Spec().Procedure}
		defer sts.finish()
		ctx = grpc.NewContextWithServerTransportStream(ctx, &sts)
		resp, err := handler(impl, ctx, req.Msg.unmarshal, nil)
		headers, trailers := sts.getHeaders(), sts.getTrailers()
		if err != nil {
			return nil, toConnectError(err, headers, trailers)
		}
		connResp := connect.NewResponse(&indirectAny{message: resp})
		toHeaders(headers, connResp.Header())
		toHeaders(trailers, connResp.Trailer())
		return connResp, nil
	}
}

type serverTransportStream struct {
	method string

	mu                sync.Mutex
	headers, trailers metadata.MD
	sent              bool
	done              bool
}

func (s *serverTransportStream) Method() string {
	return s.method
}

func (s *serverTransportStream) SetHeader(md metadata.MD) error {
	return s.setHeader(md, false)
}

func (s *serverTransportStream) SendHeader(md metadata.MD) error {
	return s.setHeader(md, true)
}

func (s *serverTransportStream) setHeader(md metadata.MD, send bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sent {
		// We can't actually send them eagerly with Connect. But we pretend
		// they were sent to at least simulate the gRPC behavior that you
		// cannot call SetHeader or SendHeader after they've already been sent.
		return errors.New("headers already sent")
	}
	s.headers = metadata.Join(s.headers, md)
	if send {
		s.sent = true
	}
	return nil
}

func (s *serverTransportStream) SetTrailer(md metadata.MD) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.done {
		return errors.New("trailers already sent")
	}
	s.trailers = metadata.Join(s.trailers, md)
	return nil
}

func (s *serverTransportStream) getHeaders() metadata.MD {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.headers
}

func (s *serverTransportStream) getTrailers() metadata.MD {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.trailers
}

func (s *serverTransportStream) finish() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent = true
	s.done = true
}

func streamHandleFuncBidi(
	impl any,
	handler func(srv any, stream grpc.ServerStream) error,
) func(context.Context, *connect.BidiStream[deferredAny, indirectAny]) error {
	return func(ctx context.Context, stream *connect.BidiStream[deferredAny, indirectAny]) error {
		ctx = metadata.NewIncomingContext(ctx, toMetadata(stream.RequestHeader()))
		ctx = peer.NewContext(ctx, &peer.Peer{Addr: tcpAddr(stream.Peer().Addr)})
		streamAdapter := &serverStreamAdapter{stream: stream}
		defer streamAdapter.finish()
		streamAdapter.ctx = grpc.NewContextWithServerTransportStream(ctx, (*serverStreamTransportAdapter)(streamAdapter))
		return toConnectError(handler(impl, streamAdapter), nil, nil)
	}
}

func streamHandleFuncClientStreams(
	impl any,
	handler func(srv any, stream grpc.ServerStream) error,
) func(context.Context, *connect.ClientStream[deferredAny]) (*connect.Response[indirectAny], error) {
	return func(ctx context.Context, stream *connect.ClientStream[deferredAny]) (*connect.Response[indirectAny], error) {
		ctx = metadata.NewIncomingContext(ctx, toMetadata(stream.RequestHeader()))
		ctx = peer.NewContext(ctx, &peer.Peer{Addr: tcpAddr(stream.Peer().Addr)})
		bidiStream := &connectBidiStreamFromClientStream{stream: stream, headers: http.Header{}, trailers: http.Header{}}
		streamAdapter := &serverStreamAdapter{stream: bidiStream}
		defer streamAdapter.finish()
		streamAdapter.ctx = grpc.NewContextWithServerTransportStream(ctx, (*serverStreamTransportAdapter)(streamAdapter))
		err := handler(impl, streamAdapter)
		if err != nil {
			err = toConnectError(err, nil, nil)
			var connError *connect.Error
			if errors.As(err, &connError) {
				mergeHeaders(connError.Meta(), bidiStream.headers)
				mergeHeaders(connError.Meta(), bidiStream.trailers)
			}
			return nil, err
		}
		connResp := connect.NewResponse(bidiStream.response.Load())
		mergeHeaders(connResp.Header(), bidiStream.headers)
		mergeHeaders(connResp.Trailer(), bidiStream.trailers)
		return connResp, nil
	}
}

func streamHandleFuncServerStreams(
	impl any,
	handler func(srv any, stream grpc.ServerStream) error,
) func(context.Context, *connect.Request[deferredAny], *connect.ServerStream[indirectAny]) error {
	return func(ctx context.Context, req *connect.Request[deferredAny], stream *connect.ServerStream[indirectAny]) error {
		ctx = metadata.NewIncomingContext(ctx, toMetadata(req.Header()))
		ctx = peer.NewContext(ctx, &peer.Peer{Addr: tcpAddr(req.Peer().Addr)})
		bidiStream := &connectBidiStreamFromServerStream{stream: stream, reqHeaders: req.Header(), spec: req.Spec()}
		bidiStream.req.Store(req.Msg)
		streamAdapter := &serverStreamAdapter{stream: bidiStream}
		defer streamAdapter.finish()
		streamAdapter.ctx = grpc.NewContextWithServerTransportStream(ctx, (*serverStreamTransportAdapter)(streamAdapter))
		return toConnectError(handler(impl, streamAdapter), nil, nil)
	}
}

type connectBidiStream interface {
	Spec() connect.Spec
	Receive() (*deferredAny, error)
	RequestHeader() http.Header
	Send(*indirectAny) error
	ResponseHeader() http.Header
	ResponseTrailer() http.Header
}

type connectBidiStreamFromClientStream struct {
	stream *connect.ClientStream[deferredAny]

	headers, trailers http.Header
	response          atomic.Pointer[indirectAny]
}

func (c *connectBidiStreamFromClientStream) Spec() connect.Spec {
	return c.stream.Spec()
}

func (c *connectBidiStreamFromClientStream) Receive() (*deferredAny, error) {
	if !c.stream.Receive() {
		err := c.stream.Err()
		if err == nil {
			err = io.EOF
		}
		return nil, err
	}
	return c.stream.Msg(), nil
}

func (c *connectBidiStreamFromClientStream) RequestHeader() http.Header {
	return c.stream.RequestHeader()
}

func (c *connectBidiStreamFromClientStream) Send(msg *indirectAny) error {
	for {
		if c.response.Load() != nil {
			return errors.New("cannot send >1 response for client-streaming method")
		}
		if c.response.CompareAndSwap(nil, msg) {
			return nil
		}
	}
}

func (c *connectBidiStreamFromClientStream) ResponseHeader() http.Header {
	return c.headers
}

func (c *connectBidiStreamFromClientStream) ResponseTrailer() http.Header {
	return c.trailers
}

func mergeHeaders(dest, src http.Header) {
	for k, vals := range src {
		for _, v := range vals {
			dest.Add(k, v)
		}
	}
}

type connectBidiStreamFromServerStream struct {
	stream     *connect.ServerStream[indirectAny]
	req        atomic.Pointer[deferredAny]
	reqHeaders http.Header
	spec       connect.Spec
}

func (c *connectBidiStreamFromServerStream) Spec() connect.Spec {
	return c.spec
}

func (c *connectBidiStreamFromServerStream) Receive() (*deferredAny, error) {
	req := c.req.Swap(nil)
	if req == nil {
		return nil, io.EOF
	}
	return req, nil
}

func (c *connectBidiStreamFromServerStream) RequestHeader() http.Header {
	return c.reqHeaders
}

func (c *connectBidiStreamFromServerStream) Send(msg *indirectAny) error {
	return c.stream.Send(msg)
}

func (c *connectBidiStreamFromServerStream) ResponseHeader() http.Header {
	return c.stream.ResponseHeader()
}

func (c *connectBidiStreamFromServerStream) ResponseTrailer() http.Header {
	return c.stream.ResponseTrailer()
}

type serverStreamAdapter struct {
	ctx    context.Context
	stream connectBidiStream

	mu   sync.Mutex
	sent bool
	done bool
}

func (s *serverStreamAdapter) SetHeader(md metadata.MD) error {
	return s.setHeader(md, false)
}

func (s *serverStreamAdapter) SendHeader(md metadata.MD) error {
	return s.setHeader(md, true)
}

func (s *serverStreamAdapter) setHeader(md metadata.MD, send bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sent {
		// We can't actually send them eagerly with Connect. But we pretend
		// they were sent to at least simulate the gRPC behavior that you
		// cannot call SetHeader or SendHeader after they've already been sent.
		return errors.New("headers already sent")
	}
	toHeaders(md, s.stream.ResponseHeader())
	if send {
		s.sent = true
	}
	return nil
}

func (s *serverStreamAdapter) SetTrailer(md metadata.MD) {
	_ = s.setTrailer(md)
}

func (s *serverStreamAdapter) setTrailer(md metadata.MD) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.done {
		return errors.New("trailers already sent")
	}
	toHeaders(md, s.stream.ResponseTrailer())
	return nil
}

func (s *serverStreamAdapter) Context() context.Context {
	return s.ctx
}

func (s *serverStreamAdapter) SendMsg(m interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent = true
	return toStatusError(s.ctx, s.stream.Send(&indirectAny{message: m}), true)
}

func (s *serverStreamAdapter) RecvMsg(m interface{}) error {
	req, err := s.stream.Receive()
	if err != nil {
		return toStatusError(s.ctx, err, true)
	}
	return req.unmarshal(m)
}

func (s *serverStreamAdapter) finish() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sent = true
	s.done = true
}

type serverStreamTransportAdapter serverStreamAdapter

func (s *serverStreamTransportAdapter) Method() string {
	return s.stream.Spec().Procedure
}

func (s *serverStreamTransportAdapter) SetHeader(md metadata.MD) error {
	return (*serverStreamAdapter)(s).SetHeader(md)
}

func (s *serverStreamTransportAdapter) SendHeader(md metadata.MD) error {
	return (*serverStreamAdapter)(s).SendHeader(md)
}

func (s *serverStreamTransportAdapter) SetTrailer(md metadata.MD) error {
	return (*serverStreamAdapter)(s).setTrailer(md)
}
