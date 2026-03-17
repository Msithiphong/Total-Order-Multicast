# test_driver.py — Part B: Test harness for Total-Order Multicast

import random
from network import Network
from replica import Replica


def run_experiment(name, num_replicas, ops, seed=42):
    """
    Run one experiment and verify correctness.

    Args:
        name:         Human-readable experiment label
        num_replicas: Number of replicas (N)
        ops:          List of (target_replica_index, operation_tuple)
        seed:         Random seed for reproducibility
    """
    random.seed(seed)
    net = Network(delay_range=(0, 5))
    replicas = [Replica(i, num_replicas, net) for i in range(num_replicas)]

    for target, op in ops:
        replicas[target].handle_client_update(op)

    net.drain()

    # --- Correctness checks ---
    states = [r.store.snapshot() for r in replicas]
    logs = [[m.update_id for m in r.delivered_log] for r in replicas]
    total_ops = len(ops)

    all_equal_state = all(s == states[0] for s in states)
    all_equal_log = all(l == logs[0] for l in logs)
    all_delivered = all(len(r.delivered_log) == total_ops for r in replicas)

    print(f"\n{'='*60}")
    print(f"Experiment: {name}  (N={num_replicas}, ops={total_ops}, seed={seed})")
    print(f"{'='*60}")
    for i, r in enumerate(replicas):
        print(f"  R{i} state: {r.store.snapshot()}")
        print(f"  R{i} log:   {[m.update_id for m in r.delivered_log]}")
    print(f"  All states equal:    {all_equal_state}")
    print(f"  All logs equal:      {all_equal_log}")
    print(f"  All msgs delivered:  {all_delivered}")

    assert all_equal_state, f"FAIL [{name}]: states diverged!"
    assert all_equal_log, f"FAIL [{name}]: delivery order diverged!"
    assert all_delivered, f"FAIL [{name}]: not all messages delivered!"
    print(f"  PASSED")


# ---------------------------------------------------------------
# Experiment 1: Concurrent conflicting updates
# ---------------------------------------------------------------
def test_concurrent_conflicting():
    """put + append on the same key, sent to different replicas."""
    ops = [
        (0, ("put", "x", "100")),
        (1, ("append", "x", "_extra")),
        (2, ("put", "x", "200")),
        (3, ("append", "x", "_final")),
    ]
    for seed in range(10):
        run_experiment("Concurrent conflicting", 4, ops, seed=seed)


# ---------------------------------------------------------------
# Experiment 2: High contention (20-50 updates to the same key)
# ---------------------------------------------------------------
def test_high_contention():
    """30 increments to the same key spread across replicas."""
    num_replicas = 4
    ops = [(i % num_replicas, ("incr", "counter")) for i in range(30)]
    for seed in range(10):
        run_experiment("High contention", num_replicas, ops, seed=seed)


# ---------------------------------------------------------------
# Experiment 3: Non-conflicting updates (different keys)
# ---------------------------------------------------------------
def test_non_conflicting():
    """Updates to distinct keys — total order still preserved."""
    ops = [
        (0, ("put", "a", "1")),
        (1, ("put", "b", "2")),
        (2, ("put", "c", "3")),
        (3, ("put", "d", "4")),
        (4, ("put", "e", "5")),
    ]
    for seed in range(10):
        run_experiment("Non-conflicting", 5, ops, seed=seed)


# ---------------------------------------------------------------
# Main
# ---------------------------------------------------------------
if __name__ == "__main__":
    print("=" * 60)
    print("  Total-Order Multicast — Test Harness")
    print("=" * 60)

    test_concurrent_conflicting()
    test_high_contention()
    test_non_conflicting()

    print(f"\n{'='*60}")
    print("  ALL EXPERIMENTS PASSED")
    print(f"{'='*60}")
