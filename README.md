# Total-Order Multicast for Replication

CECS 327 — Spring 2026 | Assignment 3

## Overview

A simulated replicated key-value store that uses **Total-Order Multicast** to ensure all replicas apply updates in the same order, even under concurrent writes and random network delays.

```
Clients
|      \       (clients can send to any replica)
v       v
+----+ +----+ +----+ +----+
| R1 | | R2 | | R3 | | R4 |
+----+ +----+ +----+ +----+
  \      |     /      |
   \-----|----/--------|
      total-order multicast
   (TOBCAST + ACK, holdback queues, deliver only when safe)
```

## Files

| File | Description |
|------|-------------|
| `messages.py` | `ToBcast` and `Ack` message types with Lamport timestamps |
| `replica.py` | Replica logic: Lamport clock, holdback queue, delivery rule, KV store |
| `network.py` | Simulated network with configurable random delays |
| `store.py` | Key-value store supporting `put`, `append`, and `incr` operations |

## How It Works

1. A client sends an update to any replica.
2. That replica increments its Lamport clock, stamps a `ToBcast`, and broadcasts it to all replicas.
3. Every replica that receives a `ToBcast` pushes it onto a min-heap holdback queue (ordered by `(ts, sender_id)`) and broadcasts an `Ack`.
4. A replica delivers the head of the holdback queue only when `max_seen[k] > head.ts` for **every** replica `k` — guaranteeing no replica can later produce a message with a smaller timestamp.
5. Delivered operations are applied to the local `KVStore`.

## How to Run

Requires Python 3.10+. No external dependencies.

```bash
python simulation.py
```

The driver (`simulation.py`) launches N replicas, sends concurrent updates, introduces random network delays, then verifies that all replicas end with identical state and identical delivered-update sequences.

## Correctness Check

At the end of each run the driver asserts:
- All replicas have **identical final KV state**
- All replicas have **identical delivered-update sequences** (same order by `update_id`)
