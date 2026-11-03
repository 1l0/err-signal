# err-signal

A tiny WebRTC signaling service for err's QR-triggered P2P sync. It exchanges
SDP and ICE messages between two peers through a short-lived room. It never sees
vault data: the encrypted segments travel over the WebRTC data channel, and the
signaling room only lasts for the connection handshake.

## API

### `POST /v1/rooms`

Creates a room for one initiator and one responder.

```sh
curl -sS -X POST http://localhost:8080/v1/rooms
```

```json
{
  "room": "hQ3...",
  "token": "6dJ...",
  "ws_path": "/v1/rooms/hQ3.../ws",
  "expires_at": "2026-11-01T10:12:00Z",
  "ice_servers": [
    {
      "urls": [
        "stun:stun.l.google.com:19302",
        "stun:stun1.l.google.com:19302",
        "stun:stun2.l.google.com:19302",
        "stun:stun3.l.google.com:19302",
        "stun:stun4.l.google.com:19302"
      ]
    }
  ]
}
```

The QR code for the other device should include the room id, token, the
signaling URL and the vault material. The key stays in the QR; the signaling
server never receives it.

### `GET /v1/rooms/{room}/ws?token=...`

Upgrades to a WebSocket. The server relays opaque text/binary messages to the
other peer and emits two control messages:

```json
{"type":"waiting","peers":1}
{"type":"ready","peers":2}
```

Client messages are opaque to the server; the expected payloads are
`offer`, `answer` and `ice` (JSON). The data channel itself carries the
encrypted sync segments.

### `GET /healthz`

```json
{"ok":true}
```

The health probe intentionally exposes no room counts or other runtime state.

## Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `ADDR` | `:8080` (or `PORT`) | Listen address. |
| `PORT` | — | Used when `ADDR` is empty. |
| `ROOM_TTL` | `10m` | Idle lifetime of a room; peers are disconnected when it expires. |
| `MAX_ROOMS` | `1000` | Hard cap on concurrent rooms. |
| `RATE_LIMIT` | `60` | Room creations per minute per client IP. `0` disables the limit. |
| `TRUST_PROXY` | `false` | Take the client IP from `X-Forwarded-For`. Enable only behind a trusted reverse proxy. |
| `ALLOWED_ORIGINS` | — (same origin only) | Comma-separated origins (for example `https://app.example`) allowed for CORS and WebSocket origin checks. Use `*` to allow every origin, for local development only. |
| `STUN_URLS` | the five Google STUN servers | Comma-separated STUN URLs or `host:port` values. |
| `TURN_URLS` | — | Optional comma-separated TURN URLs. |
| `TURN_USERNAME` | — | Optional TURN username. |
| `TURN_CREDENTIAL` | — | Optional TURN credential. |

Origin checks are deny-by-default: with `ALLOWED_ORIGINS` unset, only requests
whose `Origin` matches the signaling host itself are accepted, so a browser app
served from another origin must list that origin in `ALLOWED_ORIGINS`. Behind a
reverse proxy, also set `TRUST_PROXY=true` so that `RATE_LIMIT` applies per
client instead of per proxy address.

The defaults are **STUN only**. STUN helps peers discover their public address;
it does not relay traffic. Symmetric NAT or some cellular networks may require a
TURN server. Add one (for example a small `coturn` instance) through the
`TURN_*` variables when direct connections fail in the field.

The server has no database. Rooms live in memory and disappear on restart; a
client simply scans again. Do not log SDP, ICE candidates or tokens.

## Run

```sh
go run .
```

or build the container:

```sh
docker build -t err-signal .
docker run --rm -p 8080:8080 \
  -e ALLOWED_ORIGINS='https://app.example' \
  -e TRUST_PROXY=true \
  err-signal
```

For local development where the client runs on another localhost port, either
list its origin explicitly or allow everything:

```sh
ALLOWED_ORIGINS='*' go run .
```

The server logs a warning when `ALLOWED_ORIGINS=*` is set.

## Test

```sh
go test -race ./...
```

The tests cover room creation, ICE configuration, the waiting/ready handshake,
offer/answer/ICE relay, token rejection, the two-peer limit, CORS and WebSocket
origin checks, room expiry, rate limiting and configuration parsing.

## Security notes

- The signaling service only sees room metadata, SDP and ICE candidates. The
  vault key and the encrypted segments never reach it; they travel in the QR
  code and over the WebRTC data channel.
- A room token is a bearer credential, and browsers cannot set headers on a
  WebSocket handshake, so the token is normally passed as `?token=`. Scrub that
  parameter from reverse proxy and APM access logs. Native clients may send
  `Authorization: Bearer <token>` instead.
- `POST /v1/rooms` is unauthenticated. `RATE_LIMIT` and `MAX_ROOMS` bound abuse,
  but a public deployment should additionally sit behind a proxy with TLS and,
  if needed, edge rate limiting.
- Report vulnerabilities as described in [SECURITY.md](SECURITY.md).

## License

[MIT](LICENSE)

