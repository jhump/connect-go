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
	"strings"

	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding"
	"google.golang.org/grpc/encoding/proto"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/bufbuild/connect-go"
)

type tcpAddr string

func (t tcpAddr) Network() string {
	return "tcp"
}

func (t tcpAddr) String() string {
	return string(t)
}

func toHeaders(md metadata.MD, hdrs http.Header) {
	for k, vals := range md {
		for _, v := range vals {
			if strings.HasSuffix(strings.ToLower(k), "-bin") {
				v = connect.EncodeBinaryHeader(([]byte)(v))
			}
			hdrs.Add(k, v)
		}
	}
}

func toMetadata(hdrs http.Header) metadata.MD {
	md := make(metadata.MD, len(hdrs))
	for k, vals := range hdrs {
		for _, v := range vals {
			if strings.HasSuffix(strings.ToLower(k), "-bin") {
				if decoded, err := connect.DecodeBinaryHeader(v); err == nil {
					v = string(decoded)
				}
			}
			md.Append(k, v)
		}
	}
	return md
}

func adaptDecompressor(decomp func(r io.Reader) (io.Reader, error)) func() connect.Decompressor {
	return func() connect.Decompressor {
		return &decompressor{create: decomp}
	}
}

func adaptCompressor(comp func(w io.Writer) (io.WriteCloser, error)) func() connect.Compressor {
	return func() connect.Compressor {
		return &compressor{create: comp}
	}
}

type decompressor struct {
	io.Reader
	create func(r io.Reader) (io.Reader, error)
}

func (d *decompressor) Close() error {
	if closer, ok := d.Reader.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

func (d *decompressor) Reset(reader io.Reader) error {
	newReader, err := d.create(reader)
	if err != nil {
		return err
	}
	d.Reader = newReader
	return nil
}

type compressor struct {
	io.WriteCloser
	err    error
	create func(r io.Writer) (io.WriteCloser, error)
}

func (c *compressor) Close() error {
	return c.WriteCloser.Close()
}

func (c *compressor) Reset(writer io.Writer) {
	c.WriteCloser, c.err = c.create(writer)
}

func (c *compressor) Write(p []byte) (n int, err error) {
	if c.err != nil {
		return 0, err
	}
	return c.WriteCloser.Write(p)
}

type indirectAny struct {
	message any
}

type deferredAny struct {
	codec connect.Codec
	data  []byte
}

func (h *deferredAny) unmarshal(msg any) error {
	return h.codec.Unmarshal(h.data, msg)
}

// CodecAdapter adapts the given codec to work with options provided to
// NewChannel and to NewConnectHandler. Although the signature indicates the
// argument should be a connect.Codec, you may also use a gRPC encoding.Codec,
// too (since the interface is identical to that of Connect).
func CodecAdapter(codec connect.Codec) connect.Codec {
	return &codecAdapter{codec: codec, name: codec.Name()}
}

var _ encoding.Codec = connect.Codec(nil)

type codecAdapter struct {
	codec connect.Codec
	name  string
}

func (c *codecAdapter) Name() string {
	return c.name
}

func (c *codecAdapter) Marshal(a any) ([]byte, error) {
	if ph, ok := a.(*indirectAny); ok {
		a = ph.message
	}
	return c.codec.Marshal(a)
}

func (c *codecAdapter) Unmarshal(bytes []byte, a any) error {
	if ph, ok := a.(*deferredAny); ok {
		ph.codec = c.codec
		// must make defensive copy
		ph.data = make([]byte, len(bytes))
		copy(ph.data, bytes)
		return nil
	}
	return c.codec.Unmarshal(bytes, a)
}

type deprecatedCodecAdapter struct {
	grpc.Codec
}

func (d deprecatedCodecAdapter) Name() string {
	return d.Codec.String()
}

var defaultCodecs = []connect.Option{
	connect.WithCodec(CodecAdapter(encoding.GetCodec(proto.Name))),
}

func addDefaultCodecsClient(opts []connect.ClientOption) []connect.ClientOption {
	combined := make([]connect.ClientOption, 0, len(opts)+len(defaultCodecs))
	for _, opt := range defaultCodecs {
		combined = append(combined, opt)
	}
	combined = append(combined, opts...)
	return combined
}

func addDefaultCodecsHandler(opts []connect.HandlerOption) []connect.HandlerOption {
	combined := make([]connect.HandlerOption, 0, len(opts)+len(defaultCodecs))
	for _, opt := range defaultCodecs {
		combined = append(combined, opt)
	}
	combined = append(combined, opts...)
	return combined
}

func toConnectError(err error, headers, trailers metadata.MD) error {
	if err == nil {
		return nil
	}
	if err == io.EOF {
		return err
	}
	stat, _ := status.FromError(err)
	code := stat.Code()
	if code == codes.OK {
		code = codes.Unknown
	}
	connError := connect.NewError(connect.Code(code), err)
	for _, detail := range stat.Proto().GetDetails() {
		connError.AddDetail(connect.NewErrorDetailFromAny(detail))
	}
	toHeaders(headers, connError.Meta())
	toHeaders(trailers, connError.Meta())
	return connError
}

func toStatusError(ctx context.Context, err error, allowEOF bool) error {
	if err == nil {
		return nil
	}
	if allowEOF && errors.Is(err, io.EOF) {
		return io.EOF
	}
	var connError *connect.Error
	if !errors.As(err, &connError) {
		if errors.Is(err, context.Canceled) {
			if ctx.Err() == context.DeadlineExceeded {
				// It is possible for deadline exceeded to result in cancellation
				// such that "cancelled" is what gets returned. If we see "cancelled"
				// but also know that the deadline was exceeded, "fix" the error.
				err = context.DeadlineExceeded
				// fall-through below to report this error
			} else {
				return status.Error(codes.Canceled, err.Error())
			}
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return status.Error(codes.DeadlineExceeded, err.Error())
		}
		stat := status.New(codes.Unknown, err.Error())
		return statusError{error: err, stat: stat}
	}
	stat := &spb.Status{
		Code:    int32(connError.Code()),
		Message: connError.Message(),
	}
	for _, detail := range connError.Details() {
		stat.Details = append(stat.Details, detail.Any())
	}
	return statusError{error: connError.Unwrap(), stat: status.FromProto(stat)}
}

type statusError struct {
	error
	stat *status.Status
}

func (s statusError) GRPCStatus() *status.Status {
	return s.stat
}

func (s statusError) Unwrap() error {
	return s.error
}
