# replica.py — A2/A3/A4: Replica with Lamport clock, holdback queue, delivery rule

import heapq
from messages import ToBcast, Ack
from store import KVStore


class Replica:
    def __init__(self, replica_id: int, num_replicas: int, network):
        self.replica_id = replica_id
        self.num_replicas = num_replicas
        self.network = network

        # A2: local data structures
        self.clock = 0                          # Lamport logical clock
        self.holdback_queue: list[ToBcast] = [] # min-heap ordered by (ts, sender_id)
        self.max_seen: dict[int, int] = {}      # max_seen[k] = highest ts from replica k
        self.update_counter = 0                 # for generating unique update IDs

        # A4: replicated store
        self.store = KVStore()

        # log of delivered updates (for correctness checks)
        self.delivered_log: list[ToBcast] = []

        # register with the network
        self.network.register(self)

    # ------------------------------------------------------------------
    # Client interface
    # ------------------------------------------------------------------

    def handle_client_update(self, op: tuple):
        """Called when a client sends an update to this replica."""
        self.clock += 1                         # increment BEFORE stamping
        self.update_counter += 1
        update_id = f"upd-{self.replica_id}-{self.update_counter}"

        msg = ToBcast(
            ts=self.clock,
            sender_id=self.replica_id,
            update_id=update_id,
            op=op,
        )
        # broadcast to ALL replicas (including self)
        self.network.broadcast(self.replica_id, msg)

    # ------------------------------------------------------------------
    # Message receive dispatch
    # ------------------------------------------------------------------

    def receive(self, msg):
        """Called by the network when a message arrives."""
        if isinstance(msg, ToBcast):
            self._handle_tobcast(msg)
        elif isinstance(msg, Ack):
            self._handle_ack(msg)

    # ------------------------------------------------------------------
    # A1/A2: Lamport clock update + holdback queue management
    # ------------------------------------------------------------------

    def _handle_tobcast(self, msg: ToBcast):
        # Lamport receive rule
        self.clock = max(self.clock, msg.ts) + 1

        # push onto holdback queue (heap ordered by (ts, sender_id))
        heapq.heappush(self.holdback_queue, msg)

        # track the highest timestamp seen from this sender
        self.max_seen[msg.sender_id] = max(
            self.max_seen.get(msg.sender_id, 0), msg.ts
        )

        # send ACK to all replicas (including self)
        # Use self.clock (already incremented past msg.ts) so that
        # receivers' max_seen[self] advances beyond the TOBCAST's ts.
        ack = Ack(ts=self.clock, sender_id=self.replica_id, update_id=msg.update_id)
        self.network.broadcast(self.replica_id, ack)

        # attempt delivery
        self._try_deliver()

    def _handle_ack(self, msg: Ack):
        # Lamport receive rule
        self.clock = max(self.clock, msg.ts) + 1

        # track the highest timestamp seen from this sender
        self.max_seen[msg.sender_id] = max(
            self.max_seen.get(msg.sender_id, 0), msg.ts
        )

        # attempt delivery
        self._try_deliver()

    # ------------------------------------------------------------------
    # A3: Delivery rule (the key correctness condition)
    # ------------------------------------------------------------------

    def _try_deliver(self):
        """
        Deliver the head of the holdback queue if it is safe.
        Safe iff for EVERY replica k, max_seen[k] > head.ts,
        meaning no replica can later send a message with ts <= head.ts.
        """
        while self.holdback_queue:
            head = self.holdback_queue[0]  # peek

            safe = all(
                self.max_seen.get(k, 0) > head.ts
                for k in range(self.num_replicas)
            )
            if not safe:
                break

            heapq.heappop(self.holdback_queue)
            self._deliver(head)

    # ------------------------------------------------------------------
    # A4: Apply the update to the replicated store
    # ------------------------------------------------------------------

    def _deliver(self, msg: ToBcast):
        self.store.apply(msg.op)
        self.delivered_log.append(msg)
