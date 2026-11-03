# Security policy

## Reporting a vulnerability

Please report security issues privately through GitHub's **Report a
vulnerability** flow (repository → Security → Advisories). Do not open a public
issue for an unfixed vulnerability.

Include the affected endpoint or commit, reproduction steps, and the impact you
expect. You will get an acknowledgement as soon as possible, and coordinated
disclosure once a fix is available.

## What this service is

`err-signal` is a rendezvous service. It creates short-lived rooms and relays
opaque WebRTC handshake messages (SDP offers/answers and ICE candidates) between
at most two peers. It never receives vault keys or vault data: the key travels
in the QR code shown by the client, and the encrypted sync segments travel over
the WebRTC data channel.

## Threat model

In scope:

- Unauthorized access to a room or its handshake messages.
- Bypassing the room expiry, the room cap or the per-client rate limit.
- Leaking tokens, SDP or ICE candidates into logs.
- Denial of service against the signaling endpoint.

Out of scope:

- Compromise of a peer device, or of the vault key embedded in the QR code.
- Weakness in the WebRTC stack of the clients.
- Traffic analysis of the peer-to-peer connection after the handshake.

## Hardening checklist for deployments

- Serve the service over TLS (terminate at a reverse proxy).
- Set `ALLOWED_ORIGINS` to the exact client origins instead of `*`.
- Set `TRUST_PROXY=true` only when a trusted proxy sets `X-Forwarded-For`, so
  `RATE_LIMIT` applies per client.
- Scrub `token` query parameters from proxy and APM access logs.
- Keep the defaults for `ROOM_TTL` and `MAX_ROOMS` unless you have a reason to
  change them.
