# Eth2-crawl

A server to collect Rumor data, and serve it for other applications to use.

## Usage

`go run . --serve-addr=0.0.0.0:5000 --producer-key=foobar --consumer-key=example`

## Routes

- `/user/ws`: websocket for consuming applications to get peerstore changes as flattened events
- `/peerstore/input/ws`: websocket for rumor peerstore tee: `peer track tee --dest=wsjson --path=ws://0.0.0.0:5000/peerstore/input/ws --key=foobar`
- `/peerstore/latest`: GET latest aggregate peerstore json
- `/peerstore/history`: GET latest aggregate peerstore history json (flattened events)

flattened events format: `[source_address, 'put'/'del', time_ms, peer_id, key, value]`

API keys are simple for now: 
- One for producers, one for consumers. This may be expanded with api-users later, but is sufficient for now.
- Keys are put in the `X-Api-Key` header.

## License

MIT, see [`LICENSE`](./LICENSE) file.
