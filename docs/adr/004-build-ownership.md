# ADR 004: Coordinator executes shared build gates

Status: Accepted, 2026-10-01. Applies to this foundation execution.

Implementation remains partitioned into the plan's maximum three GPT-6-Luna coding lanes. The coordinator runs compiler, test, vet, lint and acceptance-observer gates. Workers edit and format their owned files, then repair reported failures.

The claim CLI returns exit zero for both WON and LOST. A worker misread LOST and released another lane's SHA. All APRL build actions were halted; the affected lane reacquired a fresh lease. Its newer ref was preserved and the cause was disclosed through the established project channel. No continuous ownership is asserted for the interrupted window; affected APRL checks are rerun under correct ownership.

The coordinator requires a literal WON, records that exact SHA, verifies the shared ref before and after each command, and releases only that SHA in the same foreground process. Load above 10 holds the next stage. Completed stages with unchanged source fingerprints are retained; no stage with interrupted ownership is certified. Never overwrite another holder or restore an obsolete claim after a newer acquisition.
