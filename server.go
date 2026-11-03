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
	"math"
	"net"
	"net/http"
	"os"
	"slices"
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
	// limiterIdleTTL is how long an idle per-client rate limit bucket is kept.
	limiterIdleTTL = 10 * time.Minute
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
	rateLimit       int
	trustProxy      bool
	allowedOrigins  []string
	allowAllOrigins bool
	iceServers      []iceServer
	log             *slog.Logger
}

type server struct {
	cfg     config
	limiter *rateLimiter

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
	return &server{
		cfg:     cfg,
		limiter: newRateLimiter(cfg.rateLimit),
		rooms:   map[string]*room{},
	}
}

func (s *server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/rooms", s.handleCreateRoom)
	mux.HandleFunc("GET /v1/rooms/{room}/ws", s.handleWS)
	mux.HandleFunc("GET /healthz", s.handleHealth)
	return mux
}

func (s *server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *server) handleCreateRoom(w http.ResponseWriter, r *http.Request) {
	if s.cors(w, r) {
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !s.limiter.allow(clientIP(r, s.cfg.trustProxy)) {
		w.Header().Set("Retry-After", "60")
		http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
		return
	}

	// The response carries a bearer token, so it must never be cached.
	w.Header().Set("Cache-Control", "no-store")

	s.mu.Lock()
	if len(s.rooms) >= s.cfg.maxRooms {
		s.mu.Unlock()
		w.Header().Set("Retry-After", "30")
		http.Error(w, "too many rooms", http.StatusServiceUnavailable)
		return
	}
	id, err := uniqueRoomID(s.rooms)
	if err != nil {
		s.mu.Unlock()
		s.cfg.log.Error("generate room id", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	token, err := randomToken(32)
	if err != nil {
		s.mu.Unlock()
		s.cfg.log.Error("generate room token", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
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
	token := tokenFromRequest(r)
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
		s.cfg.log.Debug("websocket accept failed", "room", roomID, "err", err)
		return
	}
	conn.SetReadLimit(maxMessageBytes)

	p := newPeer(conn)
	count := room.join(p)
	if count == 0 || !s.isRegistered(roomID, room) {
		// The room filled up, or the cleanup loop expired it between lookup and
		// join. Either way this connection must not linger in a detached room.
		reason := "room is gone"
		if count == 0 {
			reason = "room is full"
		}
		room.leave(p)
		_ = conn.Close(websocket.StatusPolicyViolation, reason)
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
	room := s.rooms[id]
	if room == nil {
		s.mu.Unlock()
		return nil
	}
	if time.Since(room.lastActive()) > s.cfg.roomTTL {
		delete(s.rooms, id)
		s.mu.Unlock()
		room.closeAll("room expired")
		return nil
	}
	s.mu.Unlock()
	return room
}

// isRegistered reports whether room is still the live room stored under id.
func (s *server) isRegistered(id string, room *room) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rooms[id] == room
}

func (s *server) cors(w http.ResponseWriter, r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin != "" {
		w.Header().Add("Vary", "Origin")
	}
	switch {
	case s.cfg.allowAllOrigins:
		w.Header().Set("Access-Control-Allow-Origin", "*")
	case origin != "" && s.originAllowed(origin):
		w.Header().Set("Access-Control-Allow-Origin", origin)
	}
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
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
				if now.Sub(room.lastActive()) > s.cfg.roomTTL {
					delete(s.rooms, id)
					expired = append(expired, room)
				}
			}
			s.mu.Unlock()
			for _, room := range expired {
				room.closeAll("room expired")
			}
			s.limiter.sweep(limiterIdleTTL)
		}
	}
}

func (r *room) validToken(token string) bool {
	sum := sha256.Sum256([]byte(token))
	return subtle.ConstantTimeCompare(sum[:], r.tokenHash[:]) == 1
}

// lastActive reports the last time the room saw any activity. It never
// mutates the timestamp: expiry checks must not keep a room alive.
func (r *room) lastActive() time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.lastActivity
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
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func randomToken(n int) (string, error) {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("crypto/rand: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(buf), nil
}

// uniqueRoomID picks an unused room id. The caller must hold s.mu.
func uniqueRoomID(rooms map[string]*room) (string, error) {
	for {
		id, err := randomToken(12)
		if err != nil {
			return "", err
		}
		if _, exists := rooms[id]; !exists {
			return id, nil
		}
	}
}

// tokenFromRequest reads the room token from the query string (browsers cannot
// set headers on a WebSocket handshake) or from an Authorization: Bearer
// header (native clients and tooling).
func tokenFromRequest(r *http.Request) string {
	if token := r.URL.Query().Get("token"); token != "" {
		return token
	}
	const prefix = "Bearer "
	if header := r.Header.Get("Authorization"); len(header) > len(prefix) && strings.EqualFold(header[:len(prefix)], prefix) {
		return strings.TrimSpace(header[len(prefix):])
	}
	return ""
}

// clientIP returns the rate limiting key for a request. X-Forwarded-For is only
// honoured when the deployment sits behind a trusted proxy, because clients can
// spoof the header otherwise.
func clientIP(r *http.Request, trustProxy bool) string {
	if trustProxy {
		if forwarded := r.Header.Get("X-Forwarded-For"); forwarded != "" {
			if first := strings.TrimSpace(strings.Split(forwarded, ",")[0]); first != "" {
				return first
			}
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// rateLimiter is a per-key token bucket. A nil limiter allows everything, which
// is what RATE_LIMIT=0 asks for.
type rateLimiter struct {
	mu      sync.Mutex
	buckets map[string]*bucket
	perSec  float64
	burst   float64
	now     func() time.Time
}

type bucket struct {
	tokens float64
	last   time.Time
}

func newRateLimiter(perMinute int) *rateLimiter {
	if perMinute <= 0 {
		return nil
	}
	return &rateLimiter{
		buckets: map[string]*bucket{},
		perSec:  float64(perMinute) / 60,
		burst:   float64(perMinute),
		now:     time.Now,
	}
}

func (l *rateLimiter) allow(key string) bool {
	if l == nil {
		return true
	}
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	b := l.buckets[key]
	if b == nil {
		b = &bucket{tokens: l.burst, last: now}
		l.buckets[key] = b
	}
	b.tokens = math.Min(l.burst, b.tokens+now.Sub(b.last).Seconds()*l.perSec)
	b.last = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

func (l *rateLimiter) sweep(maxIdle time.Duration) {
	if l == nil {
		return
	}
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	for key, b := range l.buckets {
		if now.Sub(b.last) > maxIdle {
			delete(l.buckets, key)
		}
	}
}

func loadConfig() (config, error) {
	cfg := config{
		addr:           envAddr(),
		roomTTL:        10 * time.Minute,
		maxRooms:       1000,
		rateLimit:      60,
		allowedOrigins: splitList(os.Getenv("ALLOWED_ORIGINS")),
		log:            slog.New(slog.NewTextHandler(os.Stdout, nil)),
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
	if raw := os.Getenv("RATE_LIMIT"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 0 {
			return config{}, fmt.Errorf("RATE_LIMIT must be a non-negative integer")
		}
		cfg.rateLimit = n
	}
	if raw := os.Getenv("TRUST_PROXY"); raw != "" {
		trust, err := strconv.ParseBool(raw)
		if err != nil {
			return config{}, fmt.Errorf("TRUST_PROXY: %w", err)
		}
		cfg.trustProxy = trust
	}
	// Origin checks are deny-by-default: without ALLOWED_ORIGINS only requests
	// from the signaling server's own host are accepted. "*" opts into the old
	// allow-everything behaviour and is meant for local development only.
	if slices.Contains(cfg.allowedOrigins, "*") {
		cfg.allowedOrigins = nil
		cfg.allowAllOrigins = true
	}
	if cfg.allowAllOrigins {
		cfg.log.Warn("ALLOWED_ORIGINS=*: every origin may create rooms and join; do not use this in production")
	} else if len(cfg.allowedOrigins) == 0 {
		cfg.log.Warn("ALLOWED_ORIGINS is unset: only same-origin requests are accepted; set it to the client origins, or to * for development")
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
