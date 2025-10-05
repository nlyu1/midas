## Agora Refactor Plan: Cross-machine comms with few ports and no NDP pain

### Why this doc
- Capture what we learned about IPv6/NDP on Linux and why random-per-publisher IPs fail without extra work.
- Lay out two workable architectures that keep external port count small and enable cross-machine communication.
- Choose a minimal-change path and a forward path, with step-by-step migration and test plans.

### Problem statement (brief)
- Goal: Processes across machines must communicate reliably. We want "unlimited" publishers without burning many ports. Today each publisher gets a random IPv6 in a shared /64 and uses fixed service ports (8081/8082/8083).
- Pain: Linux IPv6 Neighbor Discovery (NDP) only responds for addresses the kernel explicitly owns. Random binds succeed with ip_nonlocal_bind, but remote peers can't learn the MAC → "No route to host". Attempts to mark a subnet local break external NDP.

### What we tried (condensed)
- Local /64 route on physical or dummy iface → same-machine seemed plausible, but broke external NDP (Mac couldn't ping ::3). Kernel routing and NDP behaviors conflict.
- Add /64 as an address → kernel still doesn't answer NDP for arbitrary inner addresses; only for explicit /128s.
- Proxy NDP (sysctl) → requires per-address entries; not scalable.
- ndppd → with a subnet-wide local route it still failed; without local routes it doesn't fix same-host ND avoidance.

### Key conclusions
- You can't make the kernel answer NDP for arbitrary addresses in a subnet without either:
  - Assigning each exact address (/128) to the interface, or
  - Running an NDP proxy that truly answers for the range and doesn't get short-circuited by local routes.
- Marking an entire subnet local on a physical interface conflicts with external NDP.

### Two viable architectures

#### Option 1 — Per-publisher /128 assignment (keeps fixed ports)
- On publisher startup, briefly add its allocated IPv6 as `/128` on the NIC, then bind on 8081/8082/8083.
- Add with `nodad` to avoid ~1s DAD delay; delete on shutdown.
- Pros: Simple, preserves "many IPs, few ports", works cross-machine and same-host, no new components.
- Cons: Needs CAP_NET_ADMIN (or a tiny helper with that capability).

Commands (publisher pre/post):
```bash
sudo ip -6 addr add fde5:402f:ab0a:1:....:....:....:..../128 dev enp6s0 nodad
# ... run publisher ...
sudo ip -6 addr del fde5:402f:ab0a:1:....:....:....:..../128 dev enp6s0
```

#### Option 2 — Per-host gateway (fixed ports), internal UDS/loopback (no caps)
- A small per-host gateway listens on `[host]:8081/8082/8083` and routes by `pub_id` to a local endpoint per publisher:
  - Preferred: Unix Domain Sockets (UDS) at `/run/agora/<pub_id>/{service,string,ping}.sock`.
  - Minimal-change: TCP loopback `::1:<ephemeral>` per service.
- External (remote) clients still connect over TCP/WS. Only the destination host’s gateway is in the data path.
- Pros: No kernel privileges, avoids NDP complexities, unlimited publishers with UDS, fixed external ports.
- Cons: Adds a small component (gateway). Internal hop adds one local copy—typically negligible.

Cross-platform note:
- Linux/macOS: UDS; Windows: Named Pipes or TCP loopback internally. External remains TCP/WS everywhere.

### Recommended path
- If CAP_NET_ADMIN is acceptable: adopt Option 1 now. Cleanest and minimal change.
- If not: adopt Option 2 in two small steps:
  1) Gateway + loopback TCP (no publisher changes).
  2) Move internal hop to UDS later for perf/security.

### Detailed architecture (Option 2)

Components per host:
- Gateway process
  - Listeners: 8081 (service WS), 8082 (omni WS), 8083 (ping/RPC).
  - Registry: map `(service_kind, pub_id) -> internal target`.
  - Proxy: accept TCP, parse `pub_id` (URL path or first-frame prelude), connect to internal `{UDS|loopback}`, pipe bidirectionally with backpressure.
- Publishers
  - Serve on UDS (or `::1:ephemeral`) only. No public TCP ports.
- MetaServer
  - Registry returns `{host_ipv6, fixed_port (8081/2/3), pub_id}` for each publisher.
  - Confirmation connects via gateway too (uniform code path local/remote).

Connection workflows
- Publisher on Mac, MetaServer on Ubuntu
  1) Mac gateway listens on 8081/8082/8083.
  2) Mac publisher starts UDS (or loopback) servers.
  3) Publisher registers with MetaServer (Ubuntu) and provides `host=[mac_v6]`, `pub_id`.
  4) Subscriber gets `{host=[mac_v6], port=8081, pub_id}` and connects to `ws://[mac_v6]:8081/pub/<pub_id>`.
  5) Gateway proxies to `/run/agora/<pub_id>/service.sock` (or `::1:ephemeral`).
- Publisher on Ubuntu, MetaServer on Ubuntu
  1) Ubuntu gateway listens on 8081/8082/8083.
  2) Ubuntu publisher serves on UDS/loopback.
  3) MetaServer registers `host=[ubuntu_v6], pub_id`.
  4) Subscriber connects to `ws://[ubuntu_v6]:8081/pub/<pub_id>`; gateway proxies to local publisher.

### Minimal-change migration plan (Option 2)
1) Implement gateway (no publisher changes yet)
   - External TCP listeners on 8081/2/3.
   - Parse `pub_id` from WS URL `/pub/<pub_id>` or small handshake.
   - Internal dial: `::1:<ephemeral>` (publishers bind loopback instead of random IPv6).
   - Update MetaServer to return `{host, port, pub_id}`.
   - Update clients to connect using the gateway URL form.
2) Switch ping to gateway path
   - MetaServer confirmation goes through `[host]:8083/pub/<pub_id>` and gateway proxies to publisher ping (loopback TCP).
3) (Optional) Move internal hop to UDS
   - Add `RawStreamServer::new_unix(path)` variant and tarpc `serde_transport::unix` for ping.
   - Replace loopback endpoints with UDS paths under `/run/agora/<pub_id>/`.

### API/UI changes (external)
- Before: `ws://[pub_ipv6]:8081` (per-publisher IP)
- After:  `ws://[host_ipv6]:8081/pub/<pub_id>` (fixed ports per host)
- Ping/tarpc: analogous `tcp://[host_ipv6]:8083/pub/<pub_id>` (transport stays the same externally)

### Gateway responsibilities (sketch)
- start_listener(port, kind)
- extract_pub_id(stream, kind) → (pub_id, head_bytes)
- resolve_target(kind, pub_id) → {uds_path | (::1, port)}
- connect_internal(target) → {UnixStream|TcpStream}
- forward_bidirectional(ext, int)
- registry CRUD and health checks

### Performance considerations
- Gateway is per-host, async, multi-core. Use SO_REUSEPORT for N listeners to spread accepts across cores. The NIC is typically the bottleneck, not the proxy.
- UDS is usually faster than loopback TCP for local hops; both are acceptable.

### Security considerations
- UDS with `0700` directories under `/run/agora/<pub_id>/` restricts access by user/group.
- TLS/mTLS termination can be added at gateway if needed. External surface is limited to 3 ports.

### Testing plan
- Unit: gateway pub_id parsing; registry lookups; error paths (unknown pub_id, missing UDS).
- Integration:
  - Same-host: client → gateway → local publisher (service, string, ping).
  - Cross-host: Mac ↔ Ubuntu via IPv6; confirm ping and streams.
  - Scale: many simultaneous publishers and subscribers.
- Diagnostics: `ss -6 -tlnp | grep 808*`, `ip -6 neigh`, UDS existence and perms.

### Rollout strategy
- Phase 0 (current): revert routing tweaks; keep only owned addresses (e.g., ::3) on NIC.
- Phase 1: introduce gateway + loopback TCP; metaserver returns `{host, port, pub_id}`; clients switch URLs.
- Phase 2 (optional): migrate internal hop to UDS.
- Phase 3 (optional): add TLS at gateway, metrics, autoscale workers.

### Alternative (Option 1) implementation notes
- Provide a tiny helper binary (rtnetlink) with `CAP_NET_ADMIN` to add/del `/128` with `nodad` quickly (1–5ms per call). Clean up on exit.
- This keeps the current Agora code paths and external API intact.

### Open questions / decisions
- Do we grant publishers `CAP_NET_ADMIN`? If yes, Option 1 is simplest.
- Do we want UDS immediately or stage through loopback TCP?
- How should metaserver detect same-host publishers? (If all confirmation goes via gateway, this is moot.)
- Windows support: if required soon, prefer loopback TCP internally first.

### Appendix: tarpc over UDS (Unix)
```rust
use tarpc::{client, tokio_serde::formats::Json};
use tarpc::serde_transport::unix;

let path = "/run/agora/<pub_id>/ping.sock";
let transport = unix::connect(path, Json::default);
let client = PingRpcClient::new(client::Config::default(), transport.await?).spawn();
```

### Appendix: RawStream over UDS
- Tungstenite works over any AsyncRead/Write. Implement `RawStreamServer::new_unix(path)` that binds a `UnixListener`, accepts, wraps with tungstenite, and reuses the same broadcast pipeline.

---

In one line: Either add `/128` per publisher (fast, needs a tiny capability) or introduce a per-host gateway that multiplexes fixed ports to local UDS/loopback endpoints. Both solve cross-machine comms with few ports and no fragile NDP hacks.


# Progress

1. Refactored metaserver to keep track of new `PublisherInfo`
    - Need to come back to fix PingClient after that is implemented
