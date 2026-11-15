# Network model and measurement guide

## Live data path

```text
client asks for file
   -> server registers logical transfer and snapshots source size/hash
   -> scheduler assigns bytes/second to each active logical transfer
   -> all parallel Range responses for that transfer consume the same token bucket
   -> server yields bounded file blocks
   -> client writes each range at its byte offset and records its chunk hash
   -> client reports progress; server verifies range coverage and final SHA-256
```

Server file reads use 16 KiB blocks by default. Client chunk size controls the range size
(default 1 MiB); client `max_connections` controls concurrent range requests across all
downloads made by that client object. Server memory use is bounded by the streaming block
size and the number of active response tasks, not by total file size.

## Rate allocation versus actual goodput

Rates and sizes use **bytes/second** and **bytes**. Let `C` be the configured application
capacity and `r_i` the policy allocation for active logical transfer `i`. The scheduler
must maintain:

```text
0 <= r_i
sum_i(r_i) <= C
```

The configured capacity is an application-level sending budget, not a measurement of the
physical NIC or an instruction to an Internet router. Each transfer has a token bucket with
rate `r_i`; every concurrent range request for that transfer calls the same bucket:

```text
tokens(t) = min(burst, tokens(previous) + r_i * elapsed_seconds)
```

A stream waits until enough tokens exist for the next block. Rate allocations are refreshed
every 250 ms by default. The bucket has a bounded short burst; this is normal token-bucket
behavior, so a short instantaneous sample can differ from `r_i`. Applications, disk I/O,
TCP congestion control, competing network traffic, and host scheduling can further change
the delivered rate.

FlashPort gets remaining file size, recent client-reported service, and active transfer
count. It is not given router queue state, congestion-window state, or unobserved traffic.
`IndependentScheduler` and `EqualShareScheduler` produce equal allocations in this model.
SRPT assigns all current application capacity to the active flow with least remaining
bytes. These live allocations are distinct from the simulator's discrete-time byte delivery.

## Metrics and their meaning

- **Client-reported downloaded bytes:** completed chunks written by the client, reported to
  the server. It is not a packet capture.
- **Server stream bytes:** bytes offered by the application stream, including retransmitted
  ranges. It may exceed unique file bytes after retries and is not proof of TCP delivery.
- **Range overhead:** server stream bytes minus unique range coverage. This reports repeated
  or not-confirmed application-range bytes; it is not TCP packet retransmission overhead.
- **Current client goodput:** latest client-reported rate while a transfer is active.
- **Completed aggregate goodput:** total bytes in verified completed non-empty transfers
  divided by their actual makespan (first registered start through last verified finish).
- **Flow completion time:** verified completion time minus transfer registration time.
- **Jain fairness:** computed over completed non-empty flows' lifetime-average goodput
  `x_i = size_i / FCT_i`, using
  `J = (sum(x_i) ** 2) / (n * sum(x_i ** 2))`. It is not instantaneous rate fairness.
- **HTTP request latency:** client-measured time until response headers, including server
  scheduling/queue delay and transport time. This is only an application-level proxy, not a
  pure RTT measurement.
- **Network utilization:** active client-reported goodput divided by configured application
  capacity; it may differ from physical-link utilization.

The live monitor does not claim packet-loss, TCP retransmission, or transport RTT visibility.
Those are unavailable without operating-system/network instrumentation.

## Optional controlled Linux experiments

`flashport.network.traffic_control.build_tc_commands` returns a reviewed command pair; it
does not launch a process. Example Python usage on Linux:

```python
from flashport.network.traffic_control import TcProfile, build_tc_commands

apply, remove = build_tc_commands(
    TcProfile(bandwidth_mbit=20, delay_ms=25, packet_loss_percent=0.1),
    device="veth-flash0",
)
print("Review before running:", " ".join(apply))
print("Cleanup command:", " ".join(remove))
```

To try it, use a dedicated test interface or isolated VM. After reviewing the effect and
obtaining explicit administrator approval, run the printed command manually; run the
corresponding cleanup command afterward. These commands generally require `sudo` and can
change host networking. FlashPort never executes them. The helper is Linux-only; this
Windows development environment cannot validate `tc/netem` behavior. Use `iperf3` or OS
tools only in an explicitly authorized test environment and label their results separately
from FlashPort's simulated and application-level metrics.
