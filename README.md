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
{"ok":true,"rooms":3}
```

## Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `ADDR` | `:8080` (or `PORT`) | Listen address. |
| `PORT` | — | Used when `ADDR` is empty. |
| `ROOM_TTL` | `10m` | Idle lifetime of a room. |
| `MAX_ROOMS` | `1000` | Hard cap on concurrent rooms. |
| `ALLOWED_ORIGINS` | `*` | Comma-separated exact origins for CORS/WS origin checks. Set this in production. |
| `STUN_URLS` | the five Google STUN servers | Comma-separated STUN URLs or `host:port` values. |
| `TURN_URLS` | — | Optional comma-separated TURN URLs. |
| `TURN_USERNAME` | — | Optional TURN username. |
| `TURN_CREDENTIAL` | — | Optional TURN credential. |

The defaults are **STUN only**. STUN helps peers discover their public address;
it does not relay traffic. Symmetric NAT or some cellular networks may require a
TURN server. Add one (for example a small `coturn` instance) through the
`TURN_*` variables when direct connections fail in the field.

The server has no database. Rooms live in memory and disappear on restart; a
client simply scans again. Do not log SDP, ICE candidates or tokens.

