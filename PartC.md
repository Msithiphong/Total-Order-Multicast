1. Why does replication need total ordering for conflicting operations? Use a concrete example.

2. What do Lamport clocks guarantee and what do they not guarantee? (ordering vs real time; partial order vs total order with tie-breaks)

3. Your algorithm assumes reliable FIFO communication. What breaks if messages can be lost or delivered out of FIFO order?

4. Where is the “coordination” happening in your implementation (middleware vs application logic)? 