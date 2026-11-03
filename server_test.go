package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func testConfig() config {
	return config{
		roomTTL:    time.Minute,
		maxRooms:   10,
		iceServers: []iceServer{{URLs: []string{"stun:stun.l.google.com:19302"}}},
	}
}

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	return newTestServerWith(t, testConfig())
}

func newTestServerWith(t *testing.T, cfg config) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(newServer(cfg).routes())
	t.Cleanup(ts.Close)
	return ts
}

func createRoom(t *testing.T, ts *httptest.Server) createRoomResponse {
	t.Helper()
	resp, err := http.Post(ts.URL+"/v1/rooms", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create room status = %d", resp.StatusCode)
	}
	var room createRoomResponse
	if err := json.NewDecoder(resp.Body).Decode(&room); err != nil {
		t.Fatal(err)
	}
	return room
}

func dialRoom(t *testing.T, ts *httptest.Server, room, token string) (*websocket.Conn, *http.Response, error) {
	t.Helper()
	url := strings.Replace(ts.URL, "http", "ws", 1) + "/v1/rooms/" + room + "/ws?token=" + token
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return websocket.Dial(ctx, url, nil)
}

func readUntil(t *testing.T, conn *websocket.Conn, want string) map[string]any {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			t.Fatalf("read %q: %v", want, err)
		}
		var msg map[string]any
		if err := json.Unmarshal(data, &msg); err != nil {
			t.Fatalf("decode %q: %v (%s)", want, err, data)
		}
		if msg["type"] == want {
			return msg
		}
	}
}

func writeJSONMessage(t *testing.T, conn *websocket.Conn, value any) {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := conn.Write(ctx, websocket.MessageText, data); err != nil {
		t.Fatal(err)
	}
}

func TestHealthAndRoomCreation(t *testing.T) {
	ts := newTestServer(t)

	resp, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("health status = %d", resp.StatusCode)
	}

	room := createRoom(t, ts)
	if room.Room == "" || room.Token == "" {
		t.Fatalf("room = %+v", room)
	}
	if room.WSPath != "/v1/rooms/"+room.Room+"/ws" {
		t.Fatalf("ws_path = %q", room.WSPath)
	}
	if len(room.ICEServers) != 1 || room.ICEServers[0].URLs[0] != "stun:stun.l.google.com:19302" {
		t.Fatalf("ice servers = %+v", room.ICEServers)
	}
}

func TestRelaysOfferAnswerAndICE(t *testing.T) {
	ts := newTestServer(t)
	room := createRoom(t, ts)

	a, _, err := dialRoom(t, ts, room.Room, room.Token)
	if err != nil {
		t.Fatal(err)
	}
	defer a.CloseNow()
	readUntil(t, a, "waiting")

	b, _, err := dialRoom(t, ts, room.Room, room.Token)
	if err != nil {
		t.Fatal(err)
	}
	defer b.CloseNow()
	readUntil(t, a, "ready")
	readUntil(t, b, "ready")

	writeJSONMessage(t, a, map[string]any{"type": "offer", "sdp": "offer-sdp"})
	offer := readUntil(t, b, "offer")
	if offer["sdp"] != "offer-sdp" {
		t.Fatalf("offer = %+v", offer)
	}

	writeJSONMessage(t, b, map[string]any{"type": "answer", "sdp": "answer-sdp"})
	answer := readUntil(t, a, "answer")
	if answer["sdp"] != "answer-sdp" {
		t.Fatalf("answer = %+v", answer)
	}

	writeJSONMessage(t, b, map[string]any{"type": "ice", "candidate": "candidate"})
	ice := readUntil(t, a, "ice")
	if ice["candidate"] != "candidate" {
		t.Fatalf("ice = %+v", ice)
	}
}

func TestRejectsWrongToken(t *testing.T) {
	ts := newTestServer(t)
	room := createRoom(t, ts)

	_, resp, err := dialRoom(t, ts, room.Room, "wrong-token")
	if err == nil {
		t.Fatal("expected a dial error")
	}
	if resp == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %v, err = %v", resp, err)
	}
}

func TestRoomIsLimitedToTwoPeers(t *testing.T) {
	ts := newTestServer(t)
	room := createRoom(t, ts)

	a, _, err := dialRoom(t, ts, room.Room, room.Token)
	if err != nil {
		t.Fatal(err)
	}
	defer a.CloseNow()
	readUntil(t, a, "waiting")

	b, _, err := dialRoom(t, ts, room.Room, room.Token)
	if err != nil {
		t.Fatal(err)
	}
	defer b.CloseNow()
	readUntil(t, a, "ready")
	readUntil(t, b, "ready")

	c, _, err := dialRoom(t, ts, room.Room, room.Token)
	if err != nil {
		t.Fatal(err)
	}
	defer c.CloseNow()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, _, err := c.Read(ctx); err == nil {
		t.Fatal("third peer should have been closed")
	}
}

func TestCORSAndPreflight(t *testing.T) {
	ts := newTestServer(t)
	req, err := http.NewRequest(http.MethodOptions, ts.URL+"/v1/rooms", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Origin", "https://app.example")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("preflight status = %d", resp.StatusCode)
	}
	// Origin checks are deny-by-default, so an unconfigured server must not
	// echo the caller's origin.
	if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "" {
		t.Fatalf("allow origin = %q, want empty", got)
	}
	if got := resp.Header.Get("Vary"); !strings.Contains(got, "Origin") {
		t.Fatalf("vary = %q, want Origin", got)
	}
}

func TestCORSAllowsConfiguredOrigin(t *testing.T) {
	cfg := testConfig()
	cfg.allowedOrigins = []string{"https://app.example"}
	ts := newTestServerWith(t, cfg)

	req, err := http.NewRequest(http.MethodOptions, ts.URL+"/v1/rooms", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Origin", "https://app.example")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "https://app.example" {
		t.Fatalf("allow origin = %q", got)
	}
}

func TestCORSAllowsEveryOriginWhenEnabled(t *testing.T) {
	cfg := testConfig()
	cfg.allowAllOrigins = true
	ts := newTestServerWith(t, cfg)

	req, err := http.NewRequest(http.MethodOptions, ts.URL+"/v1/rooms", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Origin", "https://app.example")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("allow origin = %q", got)
	}
}

func TestWebSocketRejectsUnknownOrigin(t *testing.T) {
	ts := newTestServer(t)
	room := createRoom(t, ts)

	url := strings.Replace(ts.URL, "http", "ws", 1) + "/v1/rooms/" + room.Room + "/ws?token=" + room.Token
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, resp, err := websocket.Dial(ctx, url, &websocket.DialOptions{
		HTTPHeader: http.Header{"Origin": []string{"https://evil.example"}},
	})
	if err == nil {
		conn.CloseNow()
		t.Fatal("dial from a foreign origin should fail")
	}
	if resp == nil || resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %v, err = %v", resp, err)
	}
}

func TestWebSocketAcceptsBearerToken(t *testing.T) {
	ts := newTestServer(t)
	room := createRoom(t, ts)

	url := strings.Replace(ts.URL, "http", "ws", 1) + "/v1/rooms/" + room.Room + "/ws"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{
		HTTPHeader: http.Header{"Authorization": []string{"Bearer " + room.Token}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	readUntil(t, conn, "waiting")
}

func TestExpiredRoomIsRemoved(t *testing.T) {
	cfg := testConfig()
	cfg.roomTTL = 50 * time.Millisecond
	ts := newTestServerWith(t, cfg)
	room := createRoom(t, ts)

	time.Sleep(80 * time.Millisecond)

	_, resp, err := dialRoom(t, ts, room.Room, room.Token)
	if err == nil {
		t.Fatal("expected a dial error for an expired room")
	}
	if resp == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %v, err = %v", resp, err)
	}
}

func TestRoomStaysAliveWhileActive(t *testing.T) {
	cfg := testConfig()
	cfg.roomTTL = 200 * time.Millisecond
	ts := newTestServerWith(t, cfg)
	room := createRoom(t, ts)

	a, _, err := dialRoom(t, ts, room.Room, room.Token)
	if err != nil {
		t.Fatal(err)
	}
	defer a.CloseNow()
	readUntil(t, a, "waiting")

	// Keep relaying traffic for longer than the TTL; the room must survive.
	for i := 0; i < 4; i++ {
		time.Sleep(60 * time.Millisecond)
		writeJSONMessage(t, a, map[string]any{"type": "ice", "candidate": i})
	}

	b, _, err := dialRoom(t, ts, room.Room, room.Token)
	if err != nil {
		t.Fatal(err)
	}
	defer b.CloseNow()
	readUntil(t, b, "ready")
}

func TestRateLimitsRoomCreation(t *testing.T) {
	cfg := testConfig()
	cfg.rateLimit = 2
	ts := newTestServerWith(t, cfg)

	for i := 0; i < 2; i++ {
		createRoom(t, ts)
	}
	resp, err := http.Post(ts.URL+"/v1/rooms", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", resp.StatusCode)
	}
	if resp.Header.Get("Retry-After") == "" {
		t.Fatal("missing Retry-After header")
	}
}

func TestCreateRoomResponseIsNotCacheable(t *testing.T) {
	ts := newTestServer(t)
	resp, err := http.Post(ts.URL+"/v1/rooms", "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if got := resp.Header.Get("Cache-Control"); got != "no-store" {
		t.Fatalf("cache-control = %q", got)
	}
}

func TestLastActiveDoesNotRefreshOnRead(t *testing.T) {
	r := &room{lastActivity: time.Now().Add(-time.Hour)}
	first := r.lastActive()
	time.Sleep(5 * time.Millisecond)
	if second := r.lastActive(); !second.Equal(first) {
		t.Fatalf("lastActive moved from %v to %v; expiry checks must not renew a room", first, second)
	}
}

func TestLoadConfig(t *testing.T) {
	t.Setenv("ALLOWED_ORIGINS", "https://app.example, https://other.example")
	t.Setenv("RATE_LIMIT", "5")
	t.Setenv("TRUST_PROXY", "true")
	t.Setenv("STUN_URLS", "stun.example:3478")

	cfg, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.allowAllOrigins {
		t.Fatal("allowAllOrigins should be false with an explicit list")
	}
	if len(cfg.allowedOrigins) != 2 || cfg.allowedOrigins[0] != "https://app.example" {
		t.Fatalf("allowed origins = %q", cfg.allowedOrigins)
	}
	if cfg.rateLimit != 5 {
		t.Fatalf("rate limit = %d", cfg.rateLimit)
	}
	if !cfg.trustProxy {
		t.Fatal("trustProxy should be true")
	}
	if got := cfg.iceServers[0].URLs[0]; got != "stun:stun.example:3478" {
		t.Fatalf("stun url = %q", got)
	}
}

func TestLoadConfigAllowsStar(t *testing.T) {
	t.Setenv("ALLOWED_ORIGINS", "*")

	cfg, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.allowAllOrigins {
		t.Fatal("allowAllOrigins should be true")
	}
	if cfg.allowedOrigins != nil {
		t.Fatalf("allowed origins = %q, want nil", cfg.allowedOrigins)
	}
}

func TestLoadConfigDefaultsToNoExplicitOrigins(t *testing.T) {
	t.Setenv("ALLOWED_ORIGINS", "")

	cfg, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.allowAllOrigins || len(cfg.allowedOrigins) != 0 {
		t.Fatalf("allowAll = %v, origins = %q", cfg.allowAllOrigins, cfg.allowedOrigins)
	}
}

func TestLoadConfigRejectsBadRateLimit(t *testing.T) {
	t.Setenv("RATE_LIMIT", "-1")

	if _, err := loadConfig(); err == nil {
		t.Fatal("negative RATE_LIMIT should be rejected")
	}
}
