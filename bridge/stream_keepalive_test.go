package cursorbridgeruntime

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	sdkv1 "github.com/ba0f3/cursor-sdk-go/sdk/v1"
	"github.com/ba0f3/cursor-sdk-go/sdk/v1/sdkv1connect"
	"github.com/ba0f3/cursor-sdk-go/bridge/agentwire"
	"github.com/ba0f3/cursor-sdk-go/bridge/cursorapi"
	"github.com/ba0f3/cursor-sdk-go/bridge/localrun"
)

/**
 * @brief Idle frames are exactly empty and a transport failure cancels and joins its producer.
 * @param t Short private timer and cancellation-aware source.
 */
func TestRunStreamKeepaliveEmptyAndFailure(t *testing.T) {
	if runStreamKeepaliveIdle != 15*time.Second {
		t.Fatal("SDK idle default changed")
	}
	stopped := make(chan struct{})
	want := errors.New("private transport disconnected")
	err := withRunStreamKeepalive(context.Background(), time.Millisecond, func(ctx context.Context, _ func(*sdkv1.RunStreamMessage) error) error {
		<-ctx.Done()
		close(stopped)
		return context.Cause(ctx)
	}, func(message *sdkv1.RunStreamMessage) error {
		if message.GetEnvelope() != nil || message.Offset != nil || proto.Size(message) != 0 {
			t.Error("keepalive invented an envelope, offset or payload")
		}
		return want
	})
	if !errors.Is(err, want) {
		t.Fatalf("transport error=%v", err)
	}
	select {
	case <-stopped:
	default:
		t.Fatal("keepalive returned before producer cleanup")
	}
}

/**
 * @brief Real message delivery restarts the idle interval rather than leaving the old timer active.
 * @param t Private timer and acknowledged message fixture.
 */
func TestRunStreamKeepaliveResetsAfterRealMessage(t *testing.T) {
	interval := 60 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	realAt := make(chan time.Time, 1)
	idleAt := make(chan time.Time, 1)
	finish := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- withRunStreamKeepalive(ctx, interval, func(ctx context.Context, emit func(*sdkv1.RunStreamMessage) error) error {
			timer := time.NewTimer(interval / 2)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-ctx.Done():
				return context.Cause(ctx)
			}
			offset := "1"
			if err := emit(&sdkv1.RunStreamMessage{Offset: &offset}); err != nil {
				return err
			}
			select {
			case <-finish:
				return nil
			case <-ctx.Done():
				return context.Cause(ctx)
			}
		}, func(message *sdkv1.RunStreamMessage) error {
			if message.Offset != nil {
				realAt <- time.Now()
			} else {
				select {
				case idleAt <- time.Now():
				default:
				}
			}
			return nil
		})
	}()
	var sent time.Time
	select {
	case sent = <-realAt:
	case <-ctx.Done():
		t.Fatal("real message not delivered")
	}
	for {
		select {
		case idle := <-idleAt:
			if idle.Before(sent) {
				continue
			}
			if idle.Sub(sent) < interval-time.Millisecond {
				t.Fatal("real traffic did not restart idle timeout")
			}
			close(finish)
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			return
		case <-ctx.Done():
			t.Fatal("idle timer did not restart")
		}
	}
}

/**
 * @brief One writer handles backpressure and caller cancellation joins a producer blocked on delivery acknowledgement.
 * @param t Cancellation-aware blocked transport.
 */
func TestRunStreamKeepaliveSerializesAndCancels(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered, sourceStopped := make(chan struct{}), make(chan struct{})
	var writes, active atomic.Int32
	done := make(chan error, 1)
	go func() {
		done <- withRunStreamKeepalive(ctx, time.Millisecond, func(ctx context.Context, emit func(*sdkv1.RunStreamMessage) error) error {
			defer close(sourceStopped)
			offset := "1"
			return emit(&sdkv1.RunStreamMessage{Offset: &offset})
		}, func(*sdkv1.RunStreamMessage) error {
			if active.Add(1) != 1 {
				t.Error("concurrent transport writes")
			}
			defer active.Add(-1)
			if writes.Add(1) == 1 {
				close(entered)
			}
			<-ctx.Done()
			return ctx.Err()
		})
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("transport did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked writer or producer leaked after cancellation")
	}
	select {
	case <-sourceStopped:
	default:
		t.Fatal("producer was not joined")
	}
	if writes.Load() != 1 || active.Load() != 0 {
		t.Fatal("keepalive bypassed serialized backpressure")
	}
}

/**
 * @brief Engine error codes survive the stream adapter without invented idle or status frames.
 * @param t Immediate typed failure fixture.
 */
func TestRunStreamKeepalivePreservesTypedError(t *testing.T) {
	want := connect.NewError(connect.CodeNotFound, errors.New("private missing run"))
	err := withRunStreamKeepalive(context.Background(), time.Second, func(context.Context, func(*sdkv1.RunStreamMessage) error) error { return want }, func(*sdkv1.RunStreamMessage) error { t.Error("failed source produced a transport frame"); return nil })
	if err != want {
		t.Fatalf("typed source error changed: %v", err)
	}
}

/**
 * @brief Idle Send and ObserveRun frames do not enter replay history; observer delivery failures leave generation alive.
 * @param t Private native HTTP/2 stream and independent observer transport.
 */
func TestRunStreamKeepaliveDoesNotJournalAndIsolatesObserver(t *testing.T) {
	field := func(number protowire.Number, value []byte) []byte {
		return protowire.AppendBytes(protowire.AppendTag(nil, number, protowire.BytesType), value)
	}
	release := make(chan struct{})
	var upstream *httptest.Server
	upstream = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/auth/exchange_user_api_key":
			_ = json.NewEncoder(w).Encode(map[string]string{"accessToken": upstream.URL})
		case "/aiserver.v1.ServerConfigService/GetServerConfig":
			_, _ = w.Write(field(27, append(field(1, []byte(upstream.URL)), field(2, []byte(upstream.URL))...)))
		case "/agent.v1.AgentService/Run":
			var header [5]byte
			if _, err := io.ReadFull(r.Body, header[:]); err != nil {
				t.Error(err)
				return
			}
			if header[0] != 0 || binary.BigEndian.Uint32(header[1:]) > 8<<20 {
				t.Error("invalid private native request frame")
				return
			}
			if _, err := io.CopyN(io.Discard, r.Body, int64(binary.BigEndian.Uint32(header[1:]))); err != nil {
				t.Error(err)
				return
			}
			w.Header().Set("Content-Type", "application/connect+proto")
			w.(http.Flusher).Flush()
			select {
			case <-release:
			case <-r.Context().Done():
				return
			}
			_, _ = w.Write(agentwire.EncodeConnectDataFrame(field(1, field(1, field(1, []byte("done"))))))
			_, _ = w.Write(agentwire.EncodeConnectDataFrame(field(1, field(14, nil))))
			_, _ = w.Write([]byte{2, 0, 0, 0, 2, '{', '}'})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	upstream.EnableHTTP2 = true
	upstream.StartTLS()
	defer upstream.Close()
	api := cursorapi.NewClient(upstream.Client())
	api.APIBaseURL = upstream.URL
	engine := localrun.NewEngine(api)
	defer engine.Close()
	id, _, err := engine.CreateAgent(&sdkv1.AgentOptions{ApiKey: "fake-keepalive-key"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	frames := make(chan *sdkv1.RunStreamMessage, 128)
	done := make(chan error, 1)
	go func() {
		defer close(frames)
		done <- withRunStreamKeepalive(ctx, 10*time.Millisecond, func(ctx context.Context, emit func(*sdkv1.RunStreamMessage) error) error {
			return engine.Send(ctx, id, "question", nil, nil, emit)
		}, func(message *sdkv1.RunStreamMessage) error {
			select {
			case frames <- proto.Clone(message).(*sdkv1.RunStreamMessage):
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	var received []*sdkv1.RunStreamMessage
	var runID string
	for runID == "" {
		select {
		case message, ok := <-frames:
			if !ok {
				t.Fatalf("native send ended before its user event: %v", <-done)
			}
			received = append(received, message)
			if event := message.GetSdkMessage(); event != nil && event.GetType() == "user" {
				runID, _ = event.GetMessage().AsMap()["run_id"].(string)
			}
		case <-ctx.Done():
			t.Fatal("native send did not emit its user event")
		}
	}
	idleSeen := false
	for !idleSeen {
		select {
		case message, ok := <-frames:
			if !ok {
				t.Fatalf("native send ended before an idle frame: %v", <-done)
			}
			received = append(received, message)
			if proto.Size(message) == 0 {
				idleSeen = true
			}
		case <-ctx.Done():
			t.Fatal("idle send did not emit a keepalive")
		}
	}
	observerFailure := errors.New("observer disconnected")
	err = withRunStreamKeepalive(ctx, 5*time.Millisecond, func(ctx context.Context, emit func(*sdkv1.RunStreamMessage) error) error {
		return engine.ObserveRun(ctx, runID, "", emit)
	}, func(message *sdkv1.RunStreamMessage) error {
		if proto.Size(message) == 0 {
			return observerFailure
		}
		return nil
	})
	if !errors.Is(err, observerFailure) {
		t.Fatalf("observer failure=%v", err)
	}
	if engine.GetRun(runID).GetStatus() != sdkv1.RunLifecycleStatus_RUN_LIFECYCLE_STATUS_RUNNING {
		t.Fatal("observer transport failure canceled generation")
	}
	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("generation did not survive observer failure")
	}
	for message := range frames {
		received = append(received, message)
	}
	var real []*sdkv1.RunStreamMessage
	for _, message := range received {
		if proto.Size(message) == 0 {
			if message.Offset != nil || message.GetEnvelope() != nil {
				t.Fatal("idle transport frame has history identity")
			}
			continue
		}
		real = append(real, message)
	}
	var replay []*sdkv1.RunStreamMessage
	if err := engine.ObserveRun(ctx, runID, "", func(message *sdkv1.RunStreamMessage) error { replay = append(replay, message); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(real) != len(replay) {
		t.Fatalf("replay contains keepalive journal entries: real=%d replay=%d", len(real), len(replay))
	}
	for i := range real {
		if !proto.Equal(real[i], replay[i]) {
			t.Fatal("idle transport frames changed history or offsets")
		}
	}
}

/**
 * @brief Local idempotency and unsupported cloud requests are rejected before agent/run creation or provider traffic.
 * @param t Real authenticated Connect RPCs with a private provider traffic counter.
 */
func TestNativeBridgeRejectsLocalIdempotencyBeforeExecution(t *testing.T) {
	var traffic atomic.Int32
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		traffic.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer provider.Close()
	server, err := Start(context.Background(), Options{Workspace: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close(context.Background()) }()
	server.engine.API.APIBaseURL, server.engine.API.CloudBaseURL = provider.URL, provider.URL
	client := sdkv1connect.NewSdkAgentServiceClient(&http.Client{Transport: &testBridgeAuthTransport{token: server.AuthToken()}}, server.BaseURL())
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	key := "private-idempotency-key"
	for _, request := range []*sdkv1.CreateAgentRequest{
		{Options: &sdkv1.AgentOptions{ApiKey: "fake"}, IdempotencyKey: &key},
		{Options: &sdkv1.AgentOptions{ApiKey: "fake", Cloud: &sdkv1.CloudAgentOptions{}}},
	} {
		if _, err := client.CreateAgent(ctx, connect.NewRequest(request)); connect.CodeOf(err) != connect.CodeUnimplemented {
			t.Fatalf("create rejection=%v", err)
		}
	}
	agents, _, err := server.engine.ListAgents(nil)
	if err != nil || len(agents) != 0 {
		t.Fatal("rejected create persisted an agent")
	}
	empty := ""
	created, err := client.CreateAgent(ctx, connect.NewRequest(&sdkv1.CreateAgentRequest{Options: &sdkv1.AgentOptions{ApiKey: "fake"}, IdempotencyKey: &empty}))
	if err != nil {
		t.Fatal(err)
	}
	id := created.Msg.GetAgentId()
	before, err := server.engine.AgentInfo(id)
	if err != nil {
		t.Fatal(err)
	}
	for _, request := range []*sdkv1.SendRequest{
		{AgentId: id, Message: &sdkv1.UserMessage{Text: "no execution"}, IdempotencyKey: &key},
		{AgentId: id, Message: &sdkv1.UserMessage{Text: "no execution"}, Options: &sdkv1.SendOptions{Cloud: &sdkv1.CloudSendOptions{}}},
	} {
		stream, err := client.Send(ctx, connect.NewRequest(request))
		if err == nil {
			for stream.Receive() {
				t.Error("rejected send streamed a local event")
			}
			err = stream.Err()
		}
		if connect.CodeOf(err) != connect.CodeUnimplemented {
			t.Fatalf("send rejection=%v", err)
		}
	}
	after, err := server.engine.AgentInfo(id)
	if err != nil || !proto.Equal(before, after) {
		t.Fatal("rejected send changed agent state")
	}
	messages, err := server.engine.AgentMessages(id, nil)
	if err != nil || len(messages) != 0 {
		t.Fatal("rejected send created local run history")
	}
	if traffic.Load() != 0 {
		t.Fatal("rejected request contacted provider")
	}
}
