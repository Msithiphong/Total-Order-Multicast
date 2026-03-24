1. Why does replication need total ordering for conflicting operations? Use a concrete example.

Without total ordering, replicas can apply conflicting updates in different sequences and
permanently diverge. Consider a shared bank account starting at $1,000 with two concurrent
operations:
Op A (sent to R1): put("balance", 1111) — e.g. a deposit Op B (sent to R2):
put("balance", 1110) — e.g. a withdrawal
If R1 delivers A then B, it ends with balance = 1110. If R2 delivers B then A, it ends with
balance = 1111. The replicas have diverged — a classic split-brain scenario.
Total-order multicast forces every replica to agree on a single delivery sequence before
applying any update. All replicas apply A then B (or all apply B then A), so they always land on
the same final value. 

2. What do Lamport clocks guarantee and what do they not guarantee? (ordering vs real time; partial order vs total order with tie-breaks)
What they guarantee:
Lamport clocks give a causal order: if event A causally precedes event B (A "happened
before" B, written A → B), then clock(A) < clock(B). This is useful for knowing that if message
M was sent before message N, M gets a lower timestamp.
What they do not guarantee:
Real time: A higher timestamp does not mean an event happened later on a wall clock. Two
events on different machines with no causal link can have any relative Lamport timestamps
regardless of when they actually occurred.
Total order alone: Because two concurrent events (neither causally precedes the other) can
have the same timestamp, Lamport clocks only give a partial order by themselves. This
algorithm extends that to a total order by breaking ties deterministically with replica_id — so
(ts=5, replica_id=2) < (ts=5, replica_id=3). The tie-break is arbitrary but consistent across all
replicas.


3. Your algorithm assumes reliable FIFO communication. What breaks if messages can be lost or delivered out of FIFO order?
If messages are lost:
The delivery rule waits for max_seen[R] > msg.ts for every replica R. If a TOBCAST or ACK
from replica R is dropped, max_seen[R] never advances past the lost message's timestamp
— causing every replica's holdback queue to stall forever (livelock/deadlock). The system
stops making progress.
If FIFO order is violated:
Suppose R2 sends TOBCAST(ts=3) and TOBCAST(ts=5), but ts=5 arrives before ts=3. When
ts=5 arrives, max_seen[R2] is updated to 5. The delivery rule may then incorrectly conclude it
is safe to deliver a message with ts=4 — even though ts=3 from R2 is still in-flight and could
arrive later with a smaller timestamp. This violates the total order guarantee.
In short: message loss breaks liveness (progress halts), and FIFO violations break safety
(replicas may apply updates in different orders). Both require additional mechanisms —
retransmission/ACKs for loss, sequence numbers for FIFO — which this simplified model
assumes away.

4. Where is the “coordination” happening in your implementation (middleware vs application logic)? 
The coordination is split across two layers:
Middleware layer (network.py):
The network handles message delivery — broadcasting TOBCASTs and ACKs to all replicas
and simulating channel behavior (delays, reordering in Part B). It is coordination
infrastructure: it ensures every replica sees every message, but it has no knowledge of
timestamps or ordering logic.
Application logic layer (replica.py):
All the meaningful coordination happens here — incrementing Lamport clocks, maintaining
the holdback queue, tracking max_seen, and evaluating the delivery rule. This is where
replicas "agree" on an order: not through a central coordinator or leader, but through each
replica independently running the same deterministic rule on the same set of messages. The
consensus emerges from the protocol, not from any single node making decisions.
This design is intentional: keeping coordination logic in replica.py makes it easy to test in
isolation (unit-test a single Replica object) and swap out the network layer for a real
socket-based implementation without changing the core algorithm.