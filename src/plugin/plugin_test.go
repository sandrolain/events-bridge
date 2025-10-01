package plugin

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/sandrolain/events-bridge/src/message"
	"github.com/sandrolain/events-bridge/src/plugin/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
	gproto "google.golang.org/protobuf/proto"
)

type fakePluginClient struct {
	statusRes      *proto.StatusRes
	statusErr      error
	shutdownCalls  int
	shutdownErr    error
	sourceStream   grpc.ServerStreamingClient[proto.PluginMessage]
	sourceErr      error
	sourceCalls    int
	runnerResp     *proto.PluginMessage
	runnerErr      error
	runnerRequests []*proto.PluginMessage
	targetErr      error
	targetRequests []*proto.PluginMessage
}

func (f *fakePluginClient) Status(context.Context, *proto.StatusReq, ...grpc.CallOption) (*proto.StatusRes, error) {
	if f.statusErr != nil {
		return nil, f.statusErr
	}
	if f.statusRes != nil {
		return f.statusRes, nil
	}
	return &proto.StatusRes{Status: proto.Status_STATUS_READY}, nil
}

func (f *fakePluginClient) Shutdown(context.Context, *proto.ShutdownReq, ...grpc.CallOption) (*proto.ShutdownRes, error) {
	f.shutdownCalls++
	if f.shutdownErr != nil {
		return nil, f.shutdownErr
	}
	return &proto.ShutdownRes{}, nil
}

func (f *fakePluginClient) Source(context.Context, *proto.SourceReq, ...grpc.CallOption) (grpc.ServerStreamingClient[proto.PluginMessage], error) {
	f.sourceCalls++
	if f.sourceErr != nil {
		return nil, f.sourceErr
	}
	return f.sourceStream, nil
}

func (f *fakePluginClient) Runner(_ context.Context, msg *proto.PluginMessage, _ ...grpc.CallOption) (*proto.PluginMessage, error) {
	f.runnerRequests = append(f.runnerRequests, clonePluginMessage(msg))
	if f.runnerErr != nil {
		return nil, f.runnerErr
	}
	if f.runnerResp != nil {
		return clonePluginMessage(f.runnerResp), nil
	}
	return &proto.PluginMessage{}, nil
}

func (f *fakePluginClient) Target(_ context.Context, msg *proto.PluginMessage, _ ...grpc.CallOption) (*proto.TargetRes, error) {
	f.targetRequests = append(f.targetRequests, clonePluginMessage(msg))
	if f.targetErr != nil {
		return nil, f.targetErr
	}
	return &proto.TargetRes{}, nil
}

func clonePluginMessage(msg *proto.PluginMessage) *proto.PluginMessage {
	if msg == nil {
		return nil
	}
	cloned := gproto.Clone(msg)
	return cloned.(*proto.PluginMessage)
}

type fakeSourceStream struct {
	messages []*proto.PluginMessage
	errors   []error
	msgIdx   int
	errIdx   int
}

func (f *fakeSourceStream) Recv() (*proto.PluginMessage, error) {
	if f.msgIdx < len(f.messages) {
		msg := f.messages[f.msgIdx]
		f.msgIdx++
		return msg, nil
	}
	if f.errIdx < len(f.errors) {
		err := f.errors[f.errIdx]
		f.errIdx++
		return nil, err
	}
	return nil, io.EOF
}

func (f *fakeSourceStream) Header() (metadata.MD, error) {
	return nil, nil
}

func (f *fakeSourceStream) Trailer() metadata.MD {
	return nil
}

func (f *fakeSourceStream) CloseSend() error {
	return nil
}

func (f *fakeSourceStream) Context() context.Context {
	return context.Background()
}

func (f *fakeSourceStream) SendMsg(any) error {
	return nil
}

func (f *fakeSourceStream) RecvMsg(any) error {
	return nil
}

type runnerSourceStub struct {
	data        []byte
	dataErr     error
	metadata    message.MessageMetadata
	metadataErr error
}

func (s *runnerSourceStub) GetID() []byte { return nil }

func (s *runnerSourceStub) GetMetadata() (message.MessageMetadata, error) {
	if s.metadataErr != nil {
		return nil, s.metadataErr
	}
	return s.metadata, nil
}

func (s *runnerSourceStub) GetData() ([]byte, error) {
	if s.dataErr != nil {
		return nil, s.dataErr
	}
	return s.data, nil
}

func (s *runnerSourceStub) Ack() error { return nil }

func (s *runnerSourceStub) Nak() error { return nil }

func (s *runnerSourceStub) Reply(*message.ReplyData) error { return nil }

func newTestLogger(w io.Writer) *slog.Logger {
	return slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{AddSource: false}))
}

func TestPluginManagerCreateAndRetrieve(t *testing.T) {
	pm := &PluginManager{
		slog:    newTestLogger(io.Discard),
		plugins: make(map[string]*Plugin),
		server:  grpc.NewServer(),
	}
	t.Cleanup(pm.server.Stop)

	cfg := PluginConfig{Name: "demo", Exec: "noop", Protocol: "unix"}
	plugin, err := pm.CreatePlugin(cfg)
	if err != nil {
		t.Fatalf("CreatePlugin returned error: %v", err)
	}
	if plugin.Config.Name != cfg.Name {
		t.Fatalf("unexpected plugin name: %s", plugin.Config.Name)
	}
	if plugin.timeout != DefaultTimeout {
		t.Fatalf("expected default timeout, got %s", plugin.timeout)
	}

	if _, err := pm.CreatePlugin(cfg); err == nil {
		t.Fatalf("expected error creating plugin with duplicate name")
	}

	fetched, err := pm.GetPlugin(cfg.Name)
	if err != nil {
		t.Fatalf("GetPlugin error: %v", err)
	}
	if fetched != plugin {
		t.Fatalf("GetPlugin returned different instance")
	}

	if _, err := pm.GetPlugin("missing"); err == nil {
		t.Fatalf("expected error for missing plugin")
	}
}

func TestPluginManagerGetOrCreate(t *testing.T) {
	pm := &PluginManager{
		slog:    newTestLogger(io.Discard),
		plugins: make(map[string]*Plugin),
	}

	cfg := PluginConfig{Name: "sample", Exec: "noop", Protocol: "unix"}
	plugin, err := pm.GetOrCreatePlugin(cfg, false)
	if err != nil {
		t.Fatalf("GetOrCreatePlugin error: %v", err)
	}
	if plugin.Config.Name != cfg.Name {
		t.Fatalf("unexpected plugin name: %s", plugin.Config.Name)
	}

	again, err := pm.GetOrCreatePlugin(cfg, true)
	if err != nil {
		t.Fatalf("unexpected error retrieving existing plugin: %v", err)
	}
	if again != plugin {
		t.Fatalf("expected existing plugin instance to be reused")
	}
}

func TestGetPluginManagerSingleton(t *testing.T) {
	globManager = nil
	first, err := GetPluginManager()
	if err != nil {
		t.Fatalf("GetPluginManager error: %v", err)
	}
	second, err := GetPluginManager()
	if err != nil {
		t.Fatalf("GetPluginManager second call error: %v", err)
	}
	if first != second {
		t.Fatalf("expected same instance across calls")
	}
	globManager = nil
}

func TestPluginTargetSendsMessage(t *testing.T) {
	fakeClient := &fakePluginClient{}
	p := &Plugin{
		Config: PluginConfig{Name: "target"},
		client: fakeClient,
		slog:   newTestLogger(io.Discard),
	}

	original := &runnerSourceStub{metadata: message.MessageMetadata{"trace": "source"}, data: []byte("origin")}
	msg := message.NewRunnerMessage(original)
	msg.SetData([]byte("payload"))
	msg.AddMetadata("foo", "bar")

	if err := p.Target(context.Background(), msg); err != nil {
		t.Fatalf("Target returned error: %v", err)
	}
	if len(fakeClient.targetRequests) != 1 {
		t.Fatalf("expected one target request, got %d", len(fakeClient.targetRequests))
	}
	req := fakeClient.targetRequests[0]
	if string(req.Data) != "payload" {
		t.Fatalf("unexpected target data sent: %q", req.Data)
	}
	md := make(map[string]string)
	for _, kv := range req.Metadata {
		md[kv.Name] = kv.Value
	}
	if md["foo"] != "bar" {
		t.Fatalf("unexpected metadata sent: %#v", md)
	}
}

func TestPluginTargetHandlesMetadataError(t *testing.T) {
	fakeClient := &fakePluginClient{}
	p := &Plugin{
		Config: PluginConfig{Name: "target"},
		client: fakeClient,
		slog:   newTestLogger(io.Discard),
	}
	errMeta := errors.New("metadata failure")
	msg := message.NewRunnerMessage(&runnerSourceStub{metadataErr: errMeta})

	if err := p.Target(context.Background(), msg); !errors.Is(err, errMeta) {
		t.Fatalf("expected metadata error, got %v", err)
	}
}

func TestPluginRunnerReturnsResponse(t *testing.T) {
	fakeClient := &fakePluginClient{
		runnerResp: &proto.PluginMessage{
			Uuid: "resp",
			Data: []byte("response"),
			Metadata: []*proto.Metadata{
				{Name: "status", Value: "ok"},
			},
		},
	}
	p := &Plugin{
		Config: PluginConfig{Name: "runner"},
		client: fakeClient,
		slog:   newTestLogger(io.Discard),
	}

	msg := message.NewRunnerMessage(&runnerSourceStub{})
	msg.SetData([]byte("payload"))
	msg.AddMetadata("foo", "bar")

	res, err := p.Runner(context.Background(), msg)
	if err != nil {
		t.Fatalf("Runner returned error: %v", err)
	}
	if len(fakeClient.runnerRequests) != 1 {
		t.Fatalf("expected runner request to be sent")
	}
	respData, err := res.GetTargetData()
	if err != nil {
		t.Fatalf("unexpected error getting response data: %v", err)
	}
	if string(respData) != "response" {
		t.Fatalf("unexpected response data: %q", respData)
	}
	respMeta, err := res.GetTargetMetadata()
	if err != nil {
		t.Fatalf("unexpected error getting response metadata: %v", err)
	}
	if respMeta["status"] != "ok" {
		t.Fatalf("unexpected response metadata: %#v", respMeta)
	}
}

func TestPluginRunnerHandlesMetadataError(t *testing.T) {
	p := &Plugin{
		Config: PluginConfig{Name: "runner"},
		client: &fakePluginClient{},
		slog:   newTestLogger(io.Discard),
	}
	errMeta := errors.New("metadata failure")
	msg := message.NewRunnerMessage(&runnerSourceStub{metadataErr: errMeta})

	if _, err := p.Runner(context.Background(), msg); !errors.Is(err, errMeta) {
		t.Fatalf("expected metadata error, got %v", err)
	}
}

func TestPluginRunnerHandlesDataError(t *testing.T) {
	p := &Plugin{
		Config: PluginConfig{Name: "runner"},
		client: &fakePluginClient{},
		slog:   newTestLogger(io.Discard),
	}
	errData := errors.New("data failure")
	msg := message.NewRunnerMessage(&runnerSourceStub{dataErr: errData})

	if _, err := p.Runner(context.Background(), msg); !errors.Is(err, errData) {
		t.Fatalf("expected data error, got %v", err)
	}
}

func TestPluginSourceEmitsMessages(t *testing.T) {
	fakeStream := &fakeSourceStream{
		messages: []*proto.PluginMessage{
			{Uuid: "1", Data: []byte("a"), Metadata: []*proto.Metadata{{Name: "k", Value: "v"}}},
			{Uuid: "2", Data: []byte("b")},
		},
		errors: []error{io.EOF},
	}

	fakeClient := &fakePluginClient{sourceStream: fakeStream}
	p := &Plugin{
		Config: PluginConfig{Name: "source"},
		client: fakeClient,
		slog:   newTestLogger(io.Discard),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	output, closeFn, err := p.Source(ctx, 2, map[string]string{"foo": "bar"})
	if err != nil {
		t.Fatalf("Source returned error: %v", err)
	}

	for i := 0; i < 2; i++ {
		select {
		case msg := <-output:
			if msg == nil {
				t.Fatalf("received nil message")
			}
		case <-time.After(time.Second):
			t.Fatalf("timeout waiting for message %d", i)
		}
	}

	closeFn()

	select {
	case _, ok := <-output:
		if ok {
			t.Fatalf("expected channel to be closed")
		}
	case <-time.After(time.Second):
		t.Fatalf("timeout waiting for channel close")
	}
}

func TestPluginSourceLogsErrors(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	fakeStream := &fakeSourceStream{
		messages: nil,
		errors:   []error{errors.New("boom"), io.EOF},
	}

	fakeClient := &fakePluginClient{sourceStream: fakeStream}
	p := &Plugin{
		Config: PluginConfig{Name: "source"},
		client: fakeClient,
		slog:   newTestLogger(buf),
	}

	output, closeFn, err := p.Source(context.Background(), 1, nil)
	if err != nil {
		t.Fatalf("Source returned error: %v", err)
	}

	time.Sleep(150 * time.Millisecond)
	closeFn()

	select {
	case _, ok := <-output:
		if ok {
			t.Fatalf("expected channel without values")
		}
	case <-time.After(time.Second):
		t.Fatalf("timeout waiting for channel close")
	}

	if !strings.Contains(buf.String(), "failed to receive input") {
		t.Fatalf("expected error log, got %q", buf.String())
	}
}

func TestPluginSourceReturnsErrorWhenClientFails(t *testing.T) {
	fakeClient := &fakePluginClient{sourceErr: errors.New("dial failed")}
	p := &Plugin{
		Config: PluginConfig{Name: "source"},
		client: fakeClient,
		slog:   newTestLogger(io.Discard),
	}

	if _, _, err := p.Source(context.Background(), 1, nil); err == nil {
		t.Fatalf("expected error when source client fails")
	}
}

func TestPluginStopInvokesShutdownAndClose(t *testing.T) {
	listener := bufconn.Listen(1024)
	srv := grpc.NewServer()
	go func() {
		_ = srv.Serve(listener)
	}()
	t.Cleanup(func() {
		srv.Stop()
		listener.Close() //nolint:errcheck
	})

	conn, err := grpc.NewClient("bufconn",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to dial bufconn: %v", err)
	}

	fakeClient := &fakePluginClient{}
	p := &Plugin{
		Config: PluginConfig{Name: "stop"},
		client: fakeClient,
		conn:   conn,
		slog:   newTestLogger(io.Discard),
	}

	p.Stop()

	if fakeClient.shutdownCalls != 1 {
		t.Fatalf("expected shutdown to be invoked, got %d", fakeClient.shutdownCalls)
	}
}

func TestPluginManagerStop(t *testing.T) {
	fakeClient := &fakePluginClient{}
	pm := &PluginManager{
		slog: newTestLogger(io.Discard),
		plugins: map[string]*Plugin{
			"one": {Config: PluginConfig{Name: "one"}, client: fakeClient, slog: newTestLogger(io.Discard)},
		},
		server: grpc.NewServer(),
	}
	t.Cleanup(pm.server.Stop)

	if err := pm.Stop(); err != nil {
		t.Fatalf("unexpected error stopping plugin manager: %v", err)
	}

	if fakeClient.shutdownCalls != 1 {
		t.Fatalf("expected plugin shutdown to be called")
	}
}

func TestPluginStartUnsupportedProtocol(t *testing.T) {
	p := &Plugin{
		Config: PluginConfig{Name: "weird", Protocol: "invalid"},
		slog:   newTestLogger(io.Discard),
	}

	if err := p.Start(); err == nil || !strings.Contains(err.Error(), "unsupported protocol") {
		t.Fatalf("expected unsupported protocol error, got %v", err)
	}
}

func TestGetFreePort(t *testing.T) {
	port, err := GetFreePort()
	if err != nil {
		t.Fatalf("GetFreePort error: %v", err)
	}
	if port <= 0 {
		t.Fatalf("expected positive port, got %d", port)
	}
}
