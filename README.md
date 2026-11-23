# Append4

Embedded Haskell transactional KV backed by vendored LMDB 0.9.36. No daemon,
external LMDB installation, old Haskell LMDB package, or application flush clock
is required. Upstream source and license provenance is in cbits/lmdb/README.md.

## Transactions

submit queues an ordered transaction for an immediately active, bound writer
thread. Each accepted transaction is one LMDB commit and one logical version.
Default LMDB synchronous durability is retained: no NOSYNC, NOMETASYNC or
WRITEMAP flags are enabled. Success and afterCommit callbacks follow commit.
flush is an ordering barrier, not a trigger for persistence. closeStore stops
admission, drains queued writes, waits for active readers and releases ownership.
A failed commit fails the store closed; its outcome may be uncertain after an IO
error, so callers must reconcile on restart rather than blindly retry effects.

transaction stages nested writes against an LMDB read snapshot, checks its
version at commit, and rolls back on exceptions. Only touched keys and pending
mutations are held in the Haskell heap. Ordinary get and prefix/range reads use
LMDB's B-tree; they do not materialize the whole database. Read transactions use
MDB_NOTLS, with copied return values, and write transactions stay on one OS thread.
Keep transaction actions short and keep external work outside them.

Cancellation before submission discards staged writes. Cancellation while waiting
for an enqueued commit settles it before releasing the author and invokes accepted
post-commit callbacks before rethrowing that cancellation. Callbacks are ordinary
IO, not durable jobs: persist action intent before dispatching external effects.

NoChange rejects an exact logical round trip within one transaction. Separate
reversing transactions are retained. Explicit submit IDs use an indexed duplicate
ledger on disk; generated host transaction IDs need no ledger entry.

## Layout and retention

One environment contains named databases state, receipts, expiry, audit and
metadata. Application keys starting with receipts/ and expiry/ use their own
databases; all namespaces participate in the same atomic transaction.

openStore retains ordered mutation history on disk. historyPage reads bounded
pages; history explicitly requests all records. readSnapshot explicitly creates
an immutable in-memory map. snapshotAt reconstructs older versions in bounded
history pages. None of these full reads runs on the ordinary write path or open.
History remains unbounded on disk until the application chooses a retention policy.

openStoreWith defaultOptions { historyPolicy = CurrentOnly } disables mutation
history for temporary stores such as relay mailboxes. Deleted pages become
reusable once readers release them; logical deletion is not secure byte erasure.
Historical reads then support only the current version and the empty version 0.
The chosen policy is persisted and a mismatched reopen is rejected. Explicit
submit deduplication IDs remain until the store is replaced, even in CurrentOnly.

The map starts at 16 MiB and grows up to a configurable 64 GiB default ceiling.
Resizing waits for active readers and retries only prepared mutations, never
application IO. The map reserves address space, not a Haskell heap of that size.
Long-lived readers delay page reuse and resizing. A capacity failure rolls back
the entire transaction and fails closed; reopen with a larger configured ceiling.

Files are the data path, LMDB's -lock file and Append4's .lock ownership file.
Use local filesystems. Existing Append4 journals are not LMDB files and cannot be
opened with this version. This is a fresh-data change, with no format migration.

## Validation

From a Sapiens checkout:

    cabal --project-dir=build --project-file=cabal.project build exe:append4-fixture
    lean --run submodules/append4/spec/Main.lean PATH_TO_APPEND4_FIXTURE

The Lean checks cover commits without a clock, isolation, indexed ranges,
deduplication across reopen, rollback, cancellation, concurrent authors, history,
close/drain, failed commits, map growth without replaying application IO, page
reuse for transient data, exclusive ownership and killed-writer recovery.
The fixture's load mode reports elapsed time for 200 concurrent durable writes.

Target: at most 2,000 first-party Haskell lines and 250 Lean lines. Vendored LMDB
remains upstream C code, not a second custom storage implementation.
