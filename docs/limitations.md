# Limitations and future work

## What is implemented

FlashPort now has both a deterministic simulation/benchmark path and a working local HTTP
file-transfer path. The transfer path supports one byte range per request, concurrent
clients/ranges, per-logical-transfer rate limiting, resume manifests, final SHA-256 checking,
live scheduler/transfer monitoring, and a local dashboard. The implementation is intended
for an academic demonstration, not production public file hosting.

The browser UI registers receivers only while their page is open. Sender uploads are capped
at 1 GB and stored in `shared/.flashport-uploads/`; upload cleanup/retention is manual. Browser
receivers assemble the file before saving it, so available client memory limits practical
download size.

## Security and deployment scope

- The API has no user-account authentication or TLS configuration. Transfer tokens protect
  progress/range/status routes for one logical transfer, but file listing, uploads, receiver
  registration, delivery creation, and scheduler selection are open to anyone who can reach
  the server.
- Bind to `127.0.0.1` for the demo. Do not expose it on an untrusted network without adding
  authentication, TLS, authorization, quotas, and audit controls.
- The server reads direct files only from the configured shared folder, rejects path
  traversal, resolves symlinks before access, and detects source changes during streaming.
- The client stores a bearer token in its local resume sidecar. Protect download folders
  from other local users.
- Transfer state, scheduler state, completed metrics, and allocation history are in-memory.
  Run one Uvicorn worker. A process restart clears server state; the client can validate
  saved chunk hashes against the current source and resume using a new server-side transfer.
- Only a single HTTP byte range is accepted per response; multipart ranges are rejected.

## Network and algorithm limitations

- Application-level token buckets limit bytes yielded by the server, not bytes delivered
  through TCP or across a physical link. Configured capacity is not guaranteed link capacity.
- HTTP request latency includes application/server delay and is not raw RTT. Packet loss and
  TCP retransmissions are not collected. They are reported as unavailable, not estimated.
- Client progress/goodput is client-reported. Server stream-byte counts may include retried
  bytes and do not prove packet delivery.
- A `tc/netem` helper only constructs manual Linux command lines. It is not executed and was
  not validated in this Windows environment. Applying it requires explicit user approval
  and suitable privileges.
- The simulator remains a discrete-time, single-bottleneck abstraction and is separate from
  live measurements. Independent is represented as Equal Share under a similar-RTT
  approximation. Neither is a general TCP model.
- SRPT uses known remaining bytes and can defer large transfers. FlashPort is a heuristic;
  benchmark results are workload-specific and do not establish that it is optimal, novel,
  or faster on real networks.
- No ML model/component was present in the inspected repository.

## Future work

- Add authenticated users, transfer ownership/quotas, TLS deployment guidance, and durable
  server-side state before network exposure.
- Add OS-specific, permission-aware collection for real RTT and transport counters, if
  available, keeping these separate from application latency.
- Validate application scheduling under controlled, authorized Linux `tc/netem` conditions
  and compare client goodput against independent OS-level measurements.
- Evaluate policy objectives beyond mean completion time, including starvation bounds,
  per-class service, and user-selected fairness constraints.
