# messages.py — A1: Message types for Total-Order Multicast

from dataclasses import dataclass, field
from typing import Any, Tuple


@dataclass(order=True)
class ToBcast:
    """
    Broadcast message sent by a replica when it receives a client update.
    Stamped with the sender's Lamport clock at time of sending.
    All replicas (including sender) receive this and push it to their holdback queue.
    """
    ts: int                  # Lamport timestamp — used for ordering
    sender_id: int           # Tie-breaker when ts values are equal
    update_id: str = field(compare=False)  # Unique ID, e.g. "upd-2-7"
    op: Tuple[Any, ...] = field(compare=False)  # e.g. ("put", "balance", 100)

    def __str__(self):
        return f"TOBCAST(id={self.update_id}, op={self.op}, ts={self.ts}, from={self.sender_id})"


@dataclass(order=True)
class Ack:
    """
    Acknowledgment sent by every replica upon receiving a ToBcast.
    Lets other replicas know the highest timestamp this replica has seen,
    which is what makes the delivery rule safe to evaluate.
    """
    ts: int                  # Timestamp of the ToBcast being acknowledged
    sender_id: int           # Replica sending this ACK
    update_id: str = field(compare=False)  # Matches the ToBcast being acknowledged

    def __str__(self):
        return f"ACK(id={self.update_id}, ts={self.ts}, from={self.sender_id})"


# Timestamp rule (A1):
# When a replica sends a ToBcast, it first increments its Lamport clock,
# then stamps the message as (clock, replica_id).
# On receiving any message with timestamp t, a replica updates:
#   clock = max(clock, t) + 1