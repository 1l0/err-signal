// Command err-signal is a tiny WebRTC signaling service. It exchanges SDP and
// ICE messages between two peers through a short-lived room and never sees
// vault data. The actual sync payload travels over a WebRTC data channel.
package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
)

const (
	maxPeers        = 2
	maxMessageBytes = 64 << 10
	writeTimeout    = 10 * time.Second
	pingInterval    = 30 * time.Second
	cleanupInterval = 30 * time.Second
)

// iceServer is the WebRTC configuration returned with a new room. The default
// deployment uses the free Google STUN servers; TURN is optional.
type iceServer struct {
	URLs       []string `json:"urls"`
	Username   string   `json:"username,omitempty"`
	Credential string   `json:"credential,omitempty"`
}

type config struct {
	addr            string
	roomTTL         time.Duration
	maxRooms        int
	allowedOrigins  []string
	allowAllOrigins bool
	iceServers      []iceServer
	log             *slog.Logger
}

type server struct {
	cfg config

	mu    sync.Mutex
	rooms map[string]*room
}

type room struct {
	id        string
	tokenHash [32]byte

	mu           sync.Mutex
	peers        []*peer
	lastActivity time.Time
}

type peer struct {
	conn      *websocket.Conn
	send      chan []byte
	done      chan struct{}
	closeOnce sync.Once
}

type controlMessage struct {
	Type  string `json:"type"`
	Peers int    `json:"peers,omitempty"`
}

type createRoomResponse struct {
	Room       string      `json:"room"`
	Token      string      `json:"token"`
	WSPath     string      `json:"ws_path"`
	ExpiresAt  time.Time   `json:"expires_at"`
	ICEServers []iceServer `json:"ice_servers"`
}

func newServer(cfg config) *server {
	if cfg.log == nil {
		cfg.log = slog.Default()
	}
	return &server{cfg: cfg, rooms: map[string]*room{}}
}

func (s *server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/rooms", s.handleCreateRoom)
	mux.HandleFunc("GET /v1/rooms/{room}/ws", s.handleWS)
	mux.HandleFunc("GET /healthz", s.handleHealth)
	return mux
}

func (s *server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	rooms := len(s.rooms)
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "rooms": rooms})
}

func (s *server) handleCreateRoom(w http.ResponseWriter, r *http.Request) {
	if s.cors(w, r) {
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.mu.Lock()
	if len(s.rooms) >= s.cfg.maxRooms {
		s.mu.Unlock()
		http.Error(w, "too many rooms", http.StatusServiceUnavailable)
		return
	}
	id := randomToken(12)
	token := randomToken(32)
	now := time.Now()
	room := &room{
		id:           id,
		tokenHash:    sha256.Sum256([]byte(token)),
		lastActivity: now,
	}
	s.rooms[id] = room
	s.mu.Unlock()

	s.cfg.log.Info("room created", "room", id)
	writeJSON(w, http.StatusCreated, createRoomResponse{
		Room:       id,
		Token:      token,
		WSPath:     "/v1/rooms/" + id + "/ws",
		ExpiresAt:  now.Add(s.cfg.roomTTL),
		ICEServers: s.cfg.iceServers,
	})
}

func (s *server) handleWS(w http.ResponseWriter, r *http.Request) {
	roomID := r.PathValue("room")
	token := r.URL.Query().Get("token")
	room := s.room(roomID)
	if room == nil || !room.validToken(token) {
		http.Error(w, "room not found or token invalid", http.StatusUnauthorized)
		return
	}

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		InsecureSkipVerify: s.cfg.allowAllOrigins,
		OriginPatterns:     s.cfg.allowedOrigins,
		CompressionMode:    websocket.CompressionDisabled,
	})
	if err != nil {
		return
	}
	conn.SetReadLimit(maxMessageBytes)

	p := newPeer(conn)
	count := room.join(p)
	if count == 0 {
		_ = conn.Close(websocket.StatusPolicyViolation, "room is full")
		return
	}
	go p.writeLoop()

	defer func() {
		room.leave(p)
		p.close(websocket.StatusNormalClosure, "")
	}()

	if count == 1 {
		p.enqueueControl(controlMessage{Type: "waiting", Peers: count})
	} else {
		room.broadcastControl(controlMessage{Type: "ready", Peers: count})
	}

	for {
		typ, data, err := conn.Read(r.Context())
		if err != nil {
			return
		}
		if typ != websocket.MessageText && typ != websocket.MessageBinary {
			continue
		}
		if !room.relay(p, data) {
			return
		}
	}
}

func (s *server) room(id string) *room {
	s.mu.Lock()
	defer s.mu.Unlock()
	room := s.rooms[id]
	if room == nil {
		return nil
	}
	if time.Since(room.touch()) > s.cfg.roomTTL {
		delete(s.rooms, id)
		return nil
	}
	return room
}

func (s *server) cors(w http.ResponseWriter, r *http.Request) bool {
	origin := r.Header.Get("Origin")
	switch {
	case s.cfg.allowAllOrigins:
		w.Header().Set("Access-Control-Allow-Origin", "*")
	case origin != "" && s.originAllowed(origin):
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Add("Vary", "Origin")
	}
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusNoContent)
		return true
	}
	return false
}

func (s *server) originAllowed(origin string) bool {
	for _, allowed := range s.cfg.allowedOrigins {
		if strings.EqualFold(allowed, origin) {
			return true
		}
	}
	return false
}

func (s *server) closeAll(reason string) {
	s.mu.Lock()
	rooms := make([]*room, 0, len(s.rooms))
	for id, room := range s.rooms {
		delete(s.rooms, id)
		rooms = append(rooms, room)
	}
	s.mu.Unlock()
	for _, room := range rooms {
		room.closeAll(reason)
	}
}

func (s *server) cleanupLoop(ctx context.Context) {
	ticker := time.NewTicker(cleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			s.mu.Lock()
			var expired []*room
			for id, room := range s.rooms {
				if now.Sub(room.touch()) > s.cfg.roomTTL {
					delete(s.rooms, id)
					expired = append(expired, room)
				}
			}
			s.mu.Unlock()
			for _, room := range expired {
				room.closeAll("room expired")
			}
		}
	}
}

func (r *room) validToken(token string) bool {
	sum := sha256.Sum256([]byte(token))
	return subtle.ConstantTimeCompare(sum[:], r.tokenHash[:]) == 1
}

func (r *room) touch() time.Time {
	r.mu.Lock()
	r.lastActivity = time.Now()
	last := r.lastActivity
	r.mu.Unlock()
	return last
}

func (r *room) join(p *peer) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.peers) >= maxPeers {
		return 0
	}
	r.peers = append(r.peers, p)
	r.lastActivity = time.Now()
	return len(r.peers)
}

func (r *room) leave(p *peer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, peer := range r.peers {
		if peer == p {
			r.peers = append(r.peers[:i], r.peers[i+1:]...)
			break
		}
	}
	r.lastActivity = time.Now()
}

func (r *room) relay(from *peer, data []byte) bool {
	r.mu.Lock()
	var target *peer
	for _, peer := range r.peers {
		if peer != from {
			target = peer
			break
		}
	}
	r.lastActivity = time.Now()
	r.mu.Unlock()
	if target == nil {
		return true
	}
	if !target.enqueue(data) {
		target.close(websocket.StatusPolicyViolation, "peer is too slow")
		return false
	}
	return true
}

func (r *room) broadcastControl(msg controlMessage) {
	data, err := json.Marshal(msg)
	if err != nil {
		return
	}
	r.mu.Lock()
	peers := append([]*peer(nil), r.peers...)
	r.mu.Unlock()
	for _, p := range peers {
		p.enqueue(data)
	}
}

func (r *room) closeAll(reason string) {
	r.mu.Lock()
	peers := append([]*peer(nil), r.peers...)
	r.peers = nil
	r.mu.Unlock()
	for _, p := range peers {
		p.close(websocket.StatusGoingAway, reason)
	}
}

func newPeer(conn *websocket.Conn) *peer {
	return &peer{
		conn: conn,
		send: make(chan []byte, 32),
		done: make(chan struct{}),
	}
}

func (p *peer) enqueue(data []byte) bool {
	select {
	case p.send <- data:
		return true
	case <-p.done:
		return false
	default:
		return false
	}
}

func (p *peer) enqueueControl(msg controlMessage) {
	data, err := json.Marshal(msg)
	if err != nil {
		return
	}
	p.enqueue(data)
}

func (p *peer) writeLoop() {
	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-p.done:
			return
		case data := <-p.send:
			ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
			err := p.conn.Write(ctx, websocket.MessageText, data)
			cancel()
			if err != nil {
				p.close(websocket.StatusInternalError, "write failed")
				return
			}
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), writeTimeout)
			err := p.conn.Ping(ctx)
			cancel()
			if err != nil {
				p.close(websocket.StatusGoingAway, "ping failed")
				return
			}
		}
	}
}

func (p *peer) close(code websocket.StatusCode, reason string) {
	p.closeOnce.Do(func() {
		close(p.done)
		_ = p.conn.Close(code, reason)
	})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func randomToken(bytes int) string {
	buf := make([]byte, bytes)
	if _, err := rand.Read(buf); err != nil {
		panic(fmt.Sprintf("crypto/rand: %v", err))
	}
	return base64.RawURLEncoding.EncodeToString(buf)
}

func loadConfig() (config, error) {
	cfg := config{
		addr:            envAddr(),
		roomTTL:         10 * time.Minute,
		maxRooms:        1000,
		allowedOrigins:  splitList(os.Getenv("ALLOWED_ORIGINS")),
		allowAllOrigins: true,
		log:             slog.New(slog.NewTextHandler(os.Stdout, nil)),
	}
	if raw := os.Getenv("ROOM_TTL"); raw != "" {
		ttl, err := time.ParseDuration(raw)
		if err != nil {
			return config{}, fmt.Errorf("ROOM_TTL: %w", err)
		}
		cfg.roomTTL = ttl
	}
	if raw := os.Getenv("MAX_ROOMS"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n <= 0 {
			return config{}, fmt.Errorf("MAX_ROOMS must be a positive integer")
		}
		cfg.maxRooms = n
	}
	if len(cfg.allowedOrigins) > 0 {
		for _, origin := range cfg.allowedOrigins {
			if origin == "*" {
				cfg.allowAllOrigins = true
				cfg.allowedOrigins = nil
				break
			}
			cfg.allowAllOrigins = false
		}
	} else {
		cfg.allowAllOrigins = true
	}
	cfg.iceServers = iceServersFromEnv()
	if len(cfg.iceServers) == 0 {
		return config{}, errors.New("no ICE servers configured")
	}
	return cfg, nil
}

func envAddr() string {
	if addr := os.Getenv("ADDR"); addr != "" {
		return addr
	}
	if port := os.Getenv("PORT"); port != "" {
		return ":" + port
	}
	return ":8080"
}

func splitList(raw string) []string {
	var out []string
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func iceServersFromEnv() []iceServer {
	stun := splitList(os.Getenv("STUN_URLS"))
	if len(stun) == 0 {
		stun = []string{
			"stun.l.google.com:19302",
			"stun1.l.google.com:19302",
			"stun2.l.google.com:19302",
			"stun3.l.google.com:19302",
			"stun4.l.google.com:19302",
		}
	}
	for i, url := range stun {
		if !strings.HasPrefix(url, "stun:") && !strings.HasPrefix(url, "stuns:") {
			stun[i] = "stun:" + url
		}
	}
	out := []iceServer{{URLs: stun}}
	turn := splitList(os.Getenv("TURN_URLS"))
	if len(turn) > 0 {
		for i, url := range turn {
			if !strings.HasPrefix(url, "turn:") && !strings.HasPrefix(url, "turns:") {
				turn[i] = "turn:" + url
			}
		}
		out = append(out, iceServer{
			URLs:       turn,
			Username:   os.Getenv("TURN_USERNAME"),
			Credential: os.Getenv("TURN_CREDENTIAL"),
		})
	}
	return out
}
