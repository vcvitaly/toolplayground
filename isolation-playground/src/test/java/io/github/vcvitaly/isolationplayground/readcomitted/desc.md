### No dirty reads
```mermaid
sequenceDiagram
    participant Main as Main Thread
    participant T1 as Thread 1 (TX1)
    participant T2 as Thread 2 (TX2)
    participant DB as PostgreSQL

    Main->>T1: CompletableFuture.runAsync
    Main->>T2: CompletableFuture.runAsync

    T1->>DB: BEGIN (READ_COMMITTED)
    T1->>DB: UPDATE accounts SET amount = amount - 200 WHERE id=1
    T1->>DB: SELECT * FROM accounts WHERE id=1
    Note over T1: alice.amount = 800 ✓
    T1-->>T2: signal: _1_TX1_WITHDRAWN_FROM_ALICE

    T2->>DB: BEGIN (READ_COMMITTED)
    T2->>DB: SELECT * FROM accounts WHERE id=1
    Note over T2: alice.amount = 1000 ✓<br/>(no dirty read — TX1 not committed)
    T2-->>T1: signal: _2_TX2_CHECKED_BALANCE

    T1->>DB: COMMIT
    T1-->>T2: signal: _3_TX1_COMMITTED

    T2->>DB: SELECT * FROM accounts WHERE id=1
    Note over T2: alice.amount = 800 ✓<br/>(sees committed data)
    T2->>DB: COMMIT

    T1-->>Main: cf1 complete
    T2-->>Main: cf2 complete
    Main->>Main: allOf(cf1, cf2).join()
```

### Read skew
Both reads are correct separately, but we get an incorrect result in total.<br/>
```mermaid
sequenceDiagram
    participant T1 as Thread 1 (T1)
    participant DB as PostgreSQL
    participant T2 as Thread 2 (T2)

    Note over DB: Initial state: acc2=100, acc3=900

    T1->>T1: BEGIN (READ COMMITTED)
    T2->>T2: waiting...

    T1->>DB: withdraw(acc2, 100) → acc2 = 0
    Note over T1: row locked, not yet committed
    T1-->>T2: signal: _1_WITHDRAWN_FROM_BOB1

    T2->>T2: BEGIN (READ COMMITTED)
    T2->>DB: findById(2) → bob2 = 100
    Note over T2: Reads pre-withdraw value (T1 uncommitted)
    T2-->>T1: signal: _2_CHECKED_BOB1_AMOUNT

    T1->>DB: deposit(acc3, 100) → acc3 = 1000
    T1->>T1: COMMIT
    T1-->>T2: signal: _3_DEPOSITED_TO_BOB2

    T2->>DB: findById(3) → bob3 = 1000
    Note over T2: READ COMMITTED sees T1's commit for acc3 and actually reads 1000

    Note over T2: bob2(100) + bob3(1000) = 1100 ≠ 1000
    Note over T2: ⚠️ Read Skew: T2 saw a stale bob2<br/>but updated bob3,<br/>getting an inconsistent snapshot
    T2->>T2: COMMIT
```
What's happening:<br/>
1. T1 begins and withdraws 100 from account 2 (but hasn't committed yet).<br/> 
2. T2 reads account 2 — since T1 hasn't committed, READ COMMITTED returns the old value (100).<br/> 
3. T1 then deposits 100 into account 3 and commits.<br/>
4. T2 now reads account 3. Under READ COMMITTED, each statement sees the latest committed data, so T2 could see 1000, and the sum is 1100 instead of the correct total of 1000.<br/> 
This is the read skew anomaly: T2's two reads happen at different points in time relative to T1's commit, yielding an inconsistent view of the data. A higher isolation level (e.g., REPEATABLE READ) would prevent this.<br/>

### Lost update
```mermaid
sequenceDiagram
    participant Main as Main Thread
    participant T1 as Transaction 1
    participant T2 as Transaction 2
    participant DB as PostgreSQL

    Main->>Main: INSERT alice (balance=800)
    Main->>T1: CompletableFuture.runAsync
    Main->>T2: CompletableFuture.runAsync

    T1->>DB: BEGIN (READ COMMITTED)
    T2->>DB: BEGIN (READ COMMITTED)

    T1->>DB: SELECT alice → balance=800
    T2->>DB: SELECT alice → balance=800

    Note over T1,T2: Both threads sleep 100ms

    T1->>DB: UPDATE balance = 800+100 = 900
    T1->>DB: COMMIT

    T2->>DB: UPDATE balance = 800+100 = 900
    Note right of T2: Lost update! Overwrites T1's write
    T2->>DB: COMMIT

    Main->>Main: join(cf1, cf2)
    Main->>DB: SELECT alice → balance=900
    Note over Main: Assert balance == 900 (not 1000)
    Note over Main: 💥 One deposit is lost
```