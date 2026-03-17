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
        # Per (sender, receiver) FIFO channel: deque of (deliver_at_tick, msg)
        self.channels: dict[tuple[int, int], deque] = defaultdict(deque)
        self.current_tick = 0

    def register(self, replica):
        self.replicas[replica.replica_id] = replica

    def send(self, sender_id: int, recipient_id: int, msg):
        delay = random.randint(*self.delay_range)
        deliver_at = self.current_tick + delay

        channel = self.channels[(sender_id, recipient_id)]
        # Enforce FIFO: new message cannot arrive before any already-queued message
        if channel:
            deliver_at = max(deliver_at, channel[-1][0])
        channel.append((deliver_at, msg))

    def broadcast(self, sender_id: int, msg):
        for rid in self.replicas:
            self.send(sender_id, rid, msg)

    def tick(self):
        """Advance one tick and deliver all ready messages, preserving per-channel FIFO."""
        made_progress = True
        while made_progress:
            made_progress = False
            # Collect channels whose front message is ready
            ready_keys = [
                key for key, ch in self.channels.items()
                if ch and ch[0][0] <= self.current_tick
            ]
            # Shuffle to simulate non-deterministic arrival across different senders
            random.shuffle(ready_keys)

            for key in ready_keys:
                ch = self.channels[key]
                if ch and ch[0][0] <= self.current_tick:
                    _, msg = ch.popleft()
                    self.replicas[key[1]].receive(msg)
                    made_progress = True

        self.current_tick += 1

    def drain(self):
        """Keep ticking until all in-flight messages are delivered."""
        while self.has_pending():
            self.tick()

    def has_pending(self) -> bool:
        return any(ch for ch in self.channels.values())
