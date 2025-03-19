# **TRANSACTION MANAGER IMPLEMENTATION**

## **CSE 5331-001**

---

## **Overall Status**

In `zgt_tx.C`, we implemented the following functions:

1. **Readtx**: Handles read operations for transactions. It acquires necessary locks and performs the actual read operation.
2. **Writetx**: Handles write operations for transactions. It acquires necessary locks and performs the actual write operation.

We ensured a clean operation flow: starting an operation, processing read/write operations.

3. **process_read_write_operation**: Executes common logic for both read and write operations, such as lock acquisition.
4. **aborttx**: Manages transaction abortion, releasing resources and locks.
5. **committx**: Manages transaction commitment, releasing resources and locks.
6. **do_commit_abort_operation**: Handles common logic for commit and abort operations, releasing locks and updating status.
7. **set_lock**: Manages lock acquisition, handling shared and exclusive modes with necessary waits.
8. **free_locks**: Releases all transaction locks and logs released objects.

In `zgt_tm.C`, we implemented the following functions:

1. **TxWrite**: Creates threads to perform transaction write operations.
2. **CommitTx**: Creates threads to handle transaction commits.
3. **AbortTx**: Creates threads to handle transaction aborts.

---

## **Difficulties Encountered**

The primary challenges encountered during implementation:

1. Ensuring sequential execution of transaction operations.
2. Coordinating locking mechanisms across transactions.
3. Handling transaction waiting conditions and avoiding deadlocks.
4. Balancing retries and preventing infinite loops.
5. Synchronizing concurrent transactions effectively.
6. Preventing and managing deadlock scenarios.
7. Correctly managing semaphore mechanisms.

---

## **File Descriptions**

No additional files were created. All utilized files were provided in the initial bundle.

---

## **Division of Labor**

- **Inshaad Merchant** – Implemented `ReadTx()`, `WriteTx()`, `CommitTx()`, and `AbortTx()`.
- **Araohat Kokate** – Debugged logical errors and ensured accuracy of log outputs.
- **Aindrila Bhattacharya** – Compiled difficulties encountered, logical errors, and results into a PDF report.

---

## **Logical Errors**

### **Transaction Abortion Handling**

In our implementation, transaction abortion due to system failures or exceptional conditions involves a robust cleanup process ensuring data integrity. When an abort occurs—explicitly via `AbortTx` or implicitly through deadlock detection—the `do_commit_abort_operation` function updates the transaction status to `TR_ABORT`, preventing further operations. It then systematically frees locks using `free_locks`, logging the final object states before removing locks. Crucially, waiting transactions are appropriately signaled through semaphore mechanisms to resume operations. This process maintains semaphore integrity and consistency throughout the cleanup.

### **Locking and Deadlock Prevention**

We adopted a two-phase locking approach, acquiring locks at transaction start and holding them until completion. The `set_lock` function carefully evaluates lock compatibility, permitting concurrent shared locks but ensuring exclusivity for exclusive locks. Transactions failing immediate lock acquisition enter a `TR_WAIT` state, waiting on semaphores linked to current lock holders. We designed an effective retry mechanism in `process_read_write_operation` to prevent indefinite blocking or infinite retries. The comprehensive deadlock prevention measures effectively balance transaction concurrency and system integrity.
