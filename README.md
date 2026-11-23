# Append4

Embedded Haskell append-only KV storage. The application owns its clock and calls
`flush`; Append4 has one writer worker and no timer, daemon or network service.

`submit` queues an ordered transaction. `flush` captures a batch, validates each
transaction against the preceding accepted state, writes one checksummed frame,
syncs it, then publishes the snapshot and acknowledges tickets. An empty batch
or a batch of rejections writes nothing. Outcomes include `NoChange`, `Conflict`,
`Duplicate`, and `Empty`. Separate transactions that undo each other remain in
history. There is no cross-transaction netting.

`transaction store action` stages nested `putValue`/`deleteValue` calls, rolls them
back on an exception, checks the original snapshot before commit, and waits for
the host's next flush. Call `flush` from a separate host worker. `afterCommit`
callbacks run only after successful completion; callbacks are not durable jobs.
The host must persist action intents/state before dispatching external effects.
Cancellation after enqueue drains that transaction before releasing the author
and runs accepted post-commit callbacks before rethrowing cancellation.

`readSnapshot` returns an immutable map. `snapshotAt` returns the final state of
an append; `history` preserves transaction and mutation order. `closeStore`
rejects admission, drains pending writes and releases ownership, including when
sync fails. Failed writes fail closed; they may be present after recovery even
though no acknowledgment reached the caller.

The journal uses an 8-byte big-endian length, JSON append payload and a 32-byte
SHA-256 checksum chained to the previous frame. Frames are limited to 64 MiB.
Recovery discards only an incomplete final frame and rejects complete corrupt
frames. A hash chain does not authenticate history or detect valid tail removal
without a separately trusted head. Only one process can open a store.

All history stays in memory and on disk. Prefix reads and historical reads scan
maps/history. Compaction, bounded retention, indexes and replication are not
implemented. Expiring a logical key does not erase its historical bytes.

Target: at most **2,000 Haskell lines**, with **250 Lean4 lines** available for
behavior coverage. Count production and fixture Haskell separately; do not meet
the target by compressing code or moving implementation into another language.

## Validation

From a Sapiens checkout with this repository at `submodules/append4`:

```sh
cabal --project-dir=build --project-file=cabal.project build exe:append4-fixture
lean --run submodules/append4/spec/Main.lean "$(cabal --project-dir=build --project-file=cabal.project list-bin exe:append4-fixture)"
```

The Lean assertions exercise the compiled Haskell implementation with temporary
journals: rejection, ordered commits, immutable/history reads, rollback, close,
sync failure, ownership, corrupt/incomplete frames and killed-writer recovery.
