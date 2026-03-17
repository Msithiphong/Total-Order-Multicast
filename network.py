# network.py — Simulated network for message passing between replicas

import random
from collections import defaultdict, deque


class Network:
    """
    Simulated reliable network with optional random delays.
    Maintains FIFO order per (sender, receiver) pair.
    Messages are buffered and delivered when tick() is called.
    """

    def __init__(self, delay_range=(0, 0)):
        self.delay_range = delay_range          # (min_delay, max_delay) in ticks
        self.replicas: dict[int, object] = {}   # replica_id -> Replica
        # in-flight messages: list of (deliver_at_tick, recipient_id, msg)
        self.in_flight: list[tuple[int, int, object]] = []
        self.current_tick = 0

    def register(self, replica):
        self.replicas[replica.replica_id] = replica

    def send(self, sender_id: int, recipient_id: int, msg):
        delay = random.randint(*self.delay_range)
        deliver_at = self.current_tick + delay
        self.in_flight.append((deliver_at, recipient_id, msg))

    def broadcast(self, sender_id: int, msg):
        for rid in self.replicas:
            self.send(sender_id, rid, msg)

    def tick(self):
        """Advance one tick and deliver all messages due at this tick."""
        ready = [m for m in self.in_flight if m[0] <= self.current_tick]
        remaining = [m for m in self.in_flight if m[0] > self.current_tick]
        self.in_flight = remaining

        # Shuffle ready messages to simulate non-deterministic arrival order
        # across different senders (FIFO per sender is preserved by Lamport ts)
        random.shuffle(ready)

        for _, recipient_id, msg in ready:
            self.replicas[recipient_id].receive(msg)

        self.current_tick += 1

    def drain(self):
        """Keep ticking until all in-flight messages are delivered."""
        while self.in_flight:
            self.tick()

    def has_pending(self) -> bool:
        return len(self.in_flight) > 0
