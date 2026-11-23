import Lean
open Lean System

-- Assertions exercise the compiled Haskell implementation through disposable
-- files and processes. No second storage implementation is hidden in this model.
def main (args : List String) : IO UInt32 := do
  let [executable] := args | throw <| IO.userError "Pass the append4-fixture executable"
  IO.FS.withTempDir fun root => do
    let observed ← IO.Process.output {cmd := executable, args := #["checks",root.toString]}
    unless observed.exitCode == 0 do throw <| IO.userError observed.stderr
    let facts ← IO.ofExcept <| Json.parse observed.stdout
    for name in ["emptyFlush","commitWithoutClock","noChange","noCrossTransactionNetting",
      "snapshotIsolation","historicalRead","atomicConflict","orderedValidation","duplicate","concurrentValidation","rollback",
      "afterCommit","prefixAndRange","cancelledAuthor","concurrentAuthors","closeDrains","recovery",
      "failedCommit","mapGrowth","boundedTransientStorage","historyPolicy","legacyRejected","exclusiveWriter"] do
      unless (← IO.ofExcept <| facts.getObjValAs? Bool name) do throw <| IO.userError s!"FAIL {name}"
      IO.println s!"PASS {name}"
  IO.FS.withTempDir fun root => do
    let child ← IO.Process.spawn {cmd := executable, args := #["write",root.toString], stdout := .piped}
    try
      let line ← child.stdout.getLine
      unless line.trimAscii.toString == "committed" do throw <| IO.userError "Missing durable acknowledgement"
    finally
      child.kill
      discard child.wait
    for _ in [0:2] do
      let result ← IO.Process.output {cmd := executable, args := #["recover",root.toString]}
      unless result.exitCode == 0 do throw <| IO.userError result.stderr
      let saved ← IO.ofExcept <| Json.parse result.stdout
      unless (← IO.ofExcept <| saved.getObjValAs? Nat "version") == 1 && (← IO.ofExcept <| saved.getObjValAs? String "role") == "assistant" do
        throw <| IO.userError "Acknowledged commit lost after killed writer"
    let pending ← IO.Process.spawn {cmd := executable, args := #["uncommitted",root.toString], stdout := .piped}
    try
      unless (← pending.stdout.getLine).trimAscii.toString == "staged" do throw <| IO.userError "Missing staged transaction"
    finally
      pending.kill
      discard pending.wait
    let recovered ← IO.Process.output {cmd := executable, args := #["recover",root.toString]}
    let saved ← IO.ofExcept <| Json.parse recovered.stdout
    unless (← IO.ofExcept <| saved.getObjValAs? Nat "version") == 1 && (← IO.ofExcept <| saved.getObjValAs? String "role") == "assistant" do throw <| IO.userError "Uncommitted transaction became durable"
    IO.println "PASS killed writer / repeated recovery / pending transaction"
  return 0
