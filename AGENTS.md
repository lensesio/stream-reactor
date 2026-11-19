# Notes for AI coding agents

## Error policy: NOOP is not a production policy

The connectors' NOOP error policy (`connect.<connector>.error.policy=NOOP`) logs the exception, drops it and carries on. It is not a production policy.

- Reason about delivery guarantees (exactly-once, no data loss, no duplicates, zombie-task fencing) under THROW and RETRY only. A guarantee must never depend on an exception being swallowed.
- Fencing and integrity failures must fail the task or retry it.
- Do not add NOOP-specific code, tests or doc sentences to make a guarantee hold under NOOP. Examples: error kinds NOOP cannot swallow, a rewind via `SinkTaskContext.offset` when NOOP swallows an error.
- When a review finding is a gap that only exists under NOOP, label it NOOP-only and leave it out of the change.
- When you brief a reviewer or a subagent, tell it to ignore NOOP-only scenarios.

Why: one cloud sink exactly-once change grew several review rounds of NOOP-only hardening, all of it later removed. Part of it was unsafe. Kafka's `WorkerSinkTask.rewind()` updates `lastCommittedOffsets`, so a NOOP-driven rewind can lose buffered records when exactly-once is off and a rebalance happens.
