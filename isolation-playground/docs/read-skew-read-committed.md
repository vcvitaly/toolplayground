# Read Skew under READ COMMITTED isolation

## Initial state
| id | owner | amount |
|----|-------|--------|
| 1  | alice | 1000   |
| 2  | bob   | 100    |
| 3  | bob   | 900    |

Bob's total across accounts 2 & 3 = **1000**

## Sequence Diagram

```mermaid
sequenceDiagram
    participant Main as Main Thread
    participant TX1 as TX1 Thread
    participant TX2 as TX2 Thread
    participant PG as Postgres

    Main->>Main: Setup TransactionTemplates<br/>(READ_COMMITTED)

    Main->>TX1: CompletableFuture.runAsync
    Main->>TX2: CompletableFuture.runAsync
    Main->>Main: allOf(cf1, cf2).join()

    Note over TX1: BEGIN (READ COMMITTED)
    TX1->>PG: withdraw(id=2, 100)<br/>UPDATE accounts SET amount = 0 WHERE id = 2
    PG-->>TX1: OK (row locked)
    TX1->>TX1: phase = _1_TX1_WITHDRAWN_FROM_BOB1
    TX1->>TX1: await(_2_TX2_CHECKED_BOB1_AMOUNT)

    Note over TX2: await(_1_TX1_WITHDRAWN_FROM_BOB1) ✓
    Note over TX2: BEGIN (READ COMMITTED)
    TX2->>PG: findById(2)<br/>SELECT * FROM accounts WHERE id = 2
    PG-->>TX2: bob1 = {id=2, amount=100}<br/>(TX1 not committed → sees old value)
    TX2->>TX2: assert bob1.amount == 100 ✓
    TX2->>TX2: phase = _2_TX2_CHECKED_BOB1_AMOUNT
    TX2->>TX2: await(_3_TX1_DEPOSITED_TO_BOB2)

    Note over TX1: await(_2_TX2_CHECKED_BOB1_AMOUNT) ✓
    TX1->>PG: deposit(id=3, 100)<br/>UPDATE accounts SET amount = 1000 WHERE id = 3
    PG-->>TX1: OK
    TX1->>TX1: phase = _3_TX1_DEPOSITED_TO_BOB2
    Note over TX1: COMMIT

    Note over TX2: await(_3_TX1_DEPOSITED_TO_BOB2) ✓
    TX2->>PG: findById(3)<br/>SELECT * FROM accounts WHERE id = 3
    PG-->>TX2: bob2 = {id=3, amount=1000}<br/>(TX1 committed → sees new value)
    TX2->>TX2: assert bob2.amount == 1000 ✓

    Note over TX2: ⚠️ READ SKEW DETECTED<br/>bob1(100) + bob2(1000) = 1100 ≠ 1000<br/>Read inconsistent snapshot across two reads
    TX2->>TX2: assert sum == 1100 ✓
    Note over TX2: COMMIT

    TX1-->>Main: cf1 complete
    TX2-->>Main: cf2 complete
    Main->>Main: join() returns
```

## Explanation

**Read skew** occurs when a transaction reads two related pieces of data at different points in time
and sees an inconsistent state because another transaction modified and committed changes between the two reads.

Under **READ COMMITTED**, each `SELECT` sees the latest *committed* data at the moment it executes.
This means two reads within the same transaction can see data from different committed snapshots:

1. **TX2 reads account 2** → TX1 has not committed yet → sees old value **100**
2. **TX1 commits** (withdrew 100 from account 2, deposited 100 to account 3)
3. **TX2 reads account 3** → TX1 has committed → sees new value **1000**
4. **TX2 sees total = 1100** instead of the correct **1000** — a read skew anomaly

This anomaly is prevented under **REPEATABLE READ** or **SERIALIZABLE** isolation levels,
where TX2 would see a consistent snapshot throughout its entire transaction.

