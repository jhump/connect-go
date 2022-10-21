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
	"io"
	"net/http"
	"runtime"
	"strings"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"

	"github.com/bufbuild/connect-go"
)

var errTransportSecurityRequired = errors.New("transport security is required")

type connectChannel struct {
	httpClient     connect.HTTPClient
	baseURL        string
	connectOptions []connect.ClientOption
}

var _ grpc.ClientConnInterface = (*connectChannel)(nil)

// NewChannel creates a new gRPC channel backed by a Connect client.
//
// The given options allow customizing the behavior of the client. To use a
// gRPC interceptor (instead of Connect interceptor), wrap the returned value
// using grpchan.InterceptChannel and create gRPC stubs via that wrapper.
//
// If using any custom codecs, they MUST first be wrapped in a CodecAdapter
// or else there will be runtime errors when trying to use them.
func NewChannel(httpClient connect.HTTPClient, baseURL string, opts ...connect.ClientOption) grpc.ClientConnInterface {
	return &connectChannel{
		httpClient:     httpClient,
		baseURL:        baseURL,
		connectOptions: addDefaultCodecsClient(opts),
	}
}

func (c *connectChannel) Invoke(ctx context.Context, method string, args interface{}, reply interface{}, opts ...grpc.CallOption) error {
	url := c.baseURL + method

	var callOpts callOptions
	if err := callOpts.processOptions(ctx, url, c.connectOptions, opts); err != nil {
		return err
	}

	cl := connect.NewClient[indirectAny, deferredAny](c.httpClient, url, callOpts.connOpts...)
	req := connect.NewRequest(&indirectAny{args})
	if len(callOpts.reqHeaders) > 0 {
		toHeaders(callOpts.reqHeaders, req.Header())
	}
	resp, err := cl.CallUnary(ctx, req)
	if err != nil {
		var connError *connect.Error
		if errors.As(err, &connError) {
			callOpts.onFirstResponse(req.Peer(), connError.Meta())
			callOpts.onFinish(connError.Meta())
		}
		return toStatusError(ctx, err, false)
	}
	callOpts.onFirstResponse(req.Peer(), resp.Header())
	callOpts.onFinish(resp.Trailer())
	return resp.Msg.unmarshal(reply)
}

func (c *connectChannel) NewStream(ctx context.Context, sd *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	url := c.baseURL + method

	var callOpts callOptions
	if err := callOpts.processOptions(ctx, url, c.connectOptions, opts); err != nil {
		return nil, err
	}

	cl := connect.NewClient[indirectAny, deferredAny](c.httpClient, url, callOpts.connOpts...)
	ctx, cancel := context.WithCancel(ctx)

	var stream connectBidiStreamForClient
	switch {
	case sd.ClientStreams && sd.ServerStreams:
		stream = cl.CallBidiStream(ctx)
	case sd.ClientStreams:
		clientStream := &connectBidiStreamForClientFromClientStream{stream: cl.CallClientStream(ctx)}
		clientStream.cond.L = &clientStream.mu
		stream = clientStream
	case sd.ServerStreams:
		serverStream := &connectBidiStreamForClientFromServerStream{ctx: ctx, cl: cl}
		serverStream.cond.L = &serverStream.mu
		stream = serverStream
	}

	if len(callOpts.reqHeaders) > 0 {
		toHeaders(callOpts.reqHeaders, stream.RequestHeader())
	}
	go func() {
		<-ctx.Done()
		_ = stream.CloseResponse()
	}()
	result := &clientStreamAdapter{ctx: ctx, stream: stream, callOpts: &callOpts}
	runtime.SetFinalizer(result, func(adapter *clientStreamAdapter) {
		cancel()
	})
	return result, nil
}

type callOptions struct {
	headers, trailers []*metadata.MD
	peers             []*peer.Peer
	connOpts          []connect.ClientOption
	reqHeaders        metadata.MD
}

func (callOpts *callOptions) processOptions(ctx context.Context, url string, baseConnectOptions []connect.ClientOption, opts []grpc.CallOption) error {
	var codec connect.Codec
	var contentSubType string

	callOpts.reqHeaders, _ = metadata.FromOutgoingContext(ctx)
	callOpts.connOpts = baseConnectOptions
	for _, opt := range opts {
		switch opt := opt.(type) {
		case grpc.CustomCodecCallOption:
			codec = deprecatedCodecAdapter{Codec: opt.Codec}
			contentSubType = opt.Codec.String()
		case grpc.ForceCodecCallOption:
			codec = opt.Codec
			contentSubType = opt.Codec.Name()
		case grpc.ContentSubtypeCallOption:
			// TODO: error if we see content sub-type *without* a codec?
			contentSubType = opt.ContentSubtype
		case grpc.CompressorCallOption:
			compressor := encoding.GetCompressor(opt.CompressorType)
			callOpts.connOpts = append(callOpts.connOpts, connect.WithAcceptCompression(opt.CompressorType,
				adaptDecompressor(compressor.Decompress), adaptCompressor(compressor.Compress)))
		case grpc.MaxSendMsgSizeCallOption:
			callOpts.connOpts = append(callOpts.connOpts, connect.WithSendMaxBytes(opt.MaxSendMsgSize))
		case grpc.MaxRecvMsgSizeCallOption:
			callOpts.connOpts = append(callOpts.connOpts, connect.WithReadMaxBytes(opt.MaxRecvMsgSize))
		case grpc.PeerCallOption:
			callOpts.peers = append(callOpts.peers, opt.PeerAddr)
		case grpc.PerRPCCredsCallOption:
			if opt.Creds.RequireTransportSecurity() && !strings.HasPrefix(url, "https://") {
				return errTransportSecurityRequired
			}
			credsMetadata, err := opt.Creds.GetRequestMetadata(ctx, url)
			if err != nil {
				return err
			}
			if len(credsMetadata) > 0 {
				callOpts.reqHeaders = metadata.Join(callOpts.reqHeaders, metadata.New(credsMetadata))
			}
		case grpc.HeaderCallOption:
			callOpts.headers = append(callOpts.headers, opt.HeaderAddr)
		case grpc.TrailerCallOption:
			callOpts.trailers = append(callOpts.trailers, opt.TrailerAddr)
		default:
			// we don't recognize this one
		}
	}

	if codec != nil {
		codec = &codecAdapter{codec: codec, name: contentSubType}
		callOpts.connOpts = append(callOpts.connOpts, connect.WithCodec(codec))
	}
	return nil
}

func (callOpts *callOptions) onFirstResponse(p connect.Peer, headers http.Header) {
	actualPeer := peer.Peer{
		Addr: tcpAddr(p.Addr),
	}
	for _, p := range callOpts.peers {
		*p = actualPeer
	}
	if len(callOpts.headers) > 0 {
		actualHeaders := toMetadata(headers)
		for _, h := range callOpts.headers {
			*h = actualHeaders
		}
	}
}

func (callOpts *callOptions) onFinish(trailers http.Header) {
	if len(callOpts.trailers) > 0 {
		actualTrailers := toMetadata(trailers)
		for _, t := range callOpts.trailers {
			*t = actualTrailers
		}
	}
}

type connectBidiStreamForClient interface {
	Peer() connect.Peer
	RequestHeader() http.Header
	Send(*indirectAny) error
	CloseRequest() error
	Receive() (*deferredAny, error)
	ResponseHeader() http.Header
	ResponseTrailer() http.Header
	CloseResponse() error
}

type connectBidiStreamForClientFromClientStream struct {
	stream *connect.ClientStreamForClient[indirectAny, deferredAny]

	mu      sync.Mutex
	cond    sync.Cond
	resp    *connect.Response[deferredAny]
	recvErr error
}

func (c *connectBidiStreamForClientFromClientStream) Peer() connect.Peer {
	return c.stream.Peer()
}

func (c *connectBidiStreamForClientFromClientStream) RequestHeader() http.Header {
	return c.stream.RequestHeader()
}

func (c *connectBidiStreamForClientFromClientStream) Send(msg *indirectAny) error {
	return c.stream.Send(msg)
}

func (c *connectBidiStreamForClientFromClientStream) CloseRequest() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.resp != nil || c.recvErr != nil {
		// already set response and receive error
		return errors.New("stream already closed; response already received")
	}
	c.resp, c.recvErr = c.stream.CloseAndReceive()
	c.cond.Broadcast()
	return nil
}

func (c *connectBidiStreamForClientFromClientStream) Receive() (*deferredAny, error) {
	resp, err := c.receive()
	if err != nil {
		return nil, err
	}
	return resp.Msg, err
}

func (c *connectBidiStreamForClientFromClientStream) receive() (*connect.Response[deferredAny], error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for {
		if c.resp != nil || c.recvErr != nil {
			return c.resp, c.recvErr
		}
		// response not yet received
		c.cond.Wait()
	}
}

func (c *connectBidiStreamForClientFromClientStream) ResponseHeader() http.Header {
	resp, err := c.receive()
	if err != nil {
		var connError *connect.Error
		if errors.As(err, &connError) {
			return connError.Meta()
		}
	}
	return resp.Header()
}

func (c *connectBidiStreamForClientFromClientStream) ResponseTrailer() http.Header {
	resp, err := c.receive()
	if err != nil {
		var connError *connect.Error
		if errors.As(err, &connError) {
			return connError.Meta()
		}
	}
	return resp.Trailer()
}

func (c *connectBidiStreamForClientFromClientStream) CloseResponse() error {
	return nil
}

type connectBidiStreamForClientFromServerStream struct {
	ctx context.Context
	cl  *connect.Client[indirectAny, deferredAny]

	mu         sync.Mutex
	cond       sync.Cond
	reqHeaders http.Header
	req        *connect.Request[indirectAny]
	stream     *connect.ServerStreamForClient[deferredAny]
	sendErr    error
}

func (c *connectBidiStreamForClientFromServerStream) Peer() connect.Peer {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.req == nil {
		return connect.Peer{}
	}
	return c.req.Peer()
}

func (c *connectBidiStreamForClientFromServerStream) RequestHeader() http.Header {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.req != nil {
		return c.req.Header()
	}
	if c.reqHeaders == nil {
		c.reqHeaders = http.Header{}
	}
	return c.reqHeaders
}

func (c *connectBidiStreamForClientFromServerStream) Send(msg *indirectAny) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.req != nil {
		return errors.New("request already sent")
	}
	c.req = connect.NewRequest(msg)
	mergeHeaders(c.req.Header(), c.reqHeaders)
	c.stream, c.sendErr = c.cl.CallServerStream(c.ctx, c.req)
	c.cond.Broadcast()
	return c.sendErr
}

func (c *connectBidiStreamForClientFromServerStream) CloseRequest() error {
	return nil
}

func (c *connectBidiStreamForClientFromServerStream) getStream() (*connect.ServerStreamForClient[deferredAny], error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for {
		if c.stream != nil || c.sendErr != nil {
			return c.stream, c.sendErr
		}
		// haven't yet sent request and created stream
		c.cond.Wait()
	}
}

func (c *connectBidiStreamForClientFromServerStream) Receive() (*deferredAny, error) {
	stream, err := c.getStream()
	if err != nil {
		return nil, err
	}
	if !stream.Receive() {
		err := stream.Err()
		if err == nil {
			err = io.EOF
		}
		return nil, err
	}
	return stream.Msg(), nil
}

func (c *connectBidiStreamForClientFromServerStream) ResponseHeader() http.Header {
	stream, err := c.getStream()
	if err != nil {
		var connError *connect.Error
		if errors.As(err, &connError) {
			return connError.Meta()
		}
	}
	return stream.ResponseHeader()
}

func (c *connectBidiStreamForClientFromServerStream) ResponseTrailer() http.Header {
	stream, err := c.getStream()
	if err != nil {
		var connError *connect.Error
		if errors.As(err, &connError) {
			return connError.Meta()
		}
	}
	return stream.ResponseTrailer()
}

func (c *connectBidiStreamForClientFromServerStream) CloseResponse() error {
	stream, err := c.getStream()
	if err != nil {
		return err
	}
	return stream.Close()
}

type clientStreamAdapter struct {
	ctx      context.Context
	stream   connectBidiStreamForClient
	callOpts *callOptions
}

func (c *clientStreamAdapter) Header() (metadata.MD, error) {
	hdrs := c.stream.ResponseHeader()
	return toMetadata(hdrs), nil
}

func (c *clientStreamAdapter) Trailer() metadata.MD {
	tlrs := c.stream.ResponseTrailer()
	return toMetadata(tlrs)
}

func (c *clientStreamAdapter) CloseSend() error {
	return c.stream.CloseRequest()
}

func (c *clientStreamAdapter) Context() context.Context {
	return c.ctx
}

func (c *clientStreamAdapter) SendMsg(msg interface{}) error {
	return toStatusError(c.ctx, c.stream.Send(&indirectAny{message: msg}), true)
}

func (c *clientStreamAdapter) RecvMsg(msg interface{}) error {
	resp, err := c.stream.Receive()
	if err != nil {
		return toStatusError(c.ctx, err, true)
	}
	return resp.unmarshal(msg)
}
