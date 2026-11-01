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
		roomTTL:         time.Minute,
		maxRooms:        10,
		allowAllOrigins: true,
		iceServers:      []iceServer{{URLs: []string{"stun:stun.l.google.com:19302"}}},
	}
}

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	ts := httptest.NewServer(newServer(testConfig()).routes())
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
	if got := resp.Header.Get("Access-Control-Allow-Origin"); got != "*" {
		t.Fatalf("allow origin = %q", got)
	}
}
