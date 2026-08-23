# bulwark

**Raft consensus in TypeScript, held up by deterministic simulation.** Seeded partitions and crashes, the paper's five safety properties checked at every state transition, and every client history verified linearizable.

[![CI](https://github.com/BOTIROFF-D/bulwark/actions/workflows/ci.yml/badge.svg)](https://github.com/BOTIROFF-D/bulwark/actions/workflows/ci.yml)
[![npm](https://img.shields.io/npm/v/%40botiroff%2Fbulwark)](https://www.npmjs.com/package/@botiroff/bulwark)
![node](https://img.shields.io/badge/node-%E2%89%A518-blue)
![license](https://img.shields.io/badge/license-MIT-blue)

---

Anyone can write something that elects a leader on a good day. The hard part of consensus is the day the network splits, two nodes think they are in charge, a message from four seconds ago arrives twice, and a machine comes back from the dead with a stale log — and the system still cannot be caught telling a client something impossible.

That is not a property you can test by running it and watching. This repository is mostly the apparatus for proving it.

## The evidence

A test suite that only ever passes proves very little. It might be finding bugs; nothing might be looking. So the **bug museum** takes the working implementation, switches off exactly one of Raft's rules, and requires the harness to catch it — with a seed, and with a named property.

```
no up-to-date check      → Leader Completeness: n2 became leader in term 4 without
                           committed entry r6 at index 8 (term 1)              (run 2)
no prevLog check         → Log Matching: index 1 term 1 holds noop:n1:1 elsewhere
                           but r2 on n4                                        (run 1)
keeps conflicting entry  → State Machine Safety: index 8: n5 applied noop:n5:6,
                           n2 applied r7                                       (run 4)
no leader no-op          → replicas disagree: n1={"a":"p1.2","b":"p0.2"}
                           but n4={"a":"p1.2","b":"p1.3"}                      (run 25)
ignores a newer term     → client 2 gave up on cas                             (run 1)

Figure 8                 → Leader Completeness: n5 became leader in term 4 without
                           committed entry entry-A at index 1 (term 1)
unpersisted vote         → Election Safety: n2 and n3 are both leader in term 1
```

None of these are strawmen. Each has a subsection of the paper explaining why the obvious version is unsafe, and the paper would not need those subsections if nobody had shipped the obvious version.

The last two are *directed* rather than random. Figure 8 needs four leadership changes in a prescribed order with prescribed partitions; the persistence window is one tick wide. Hoping to stumble into either is not a plan, so [`museum/directed.test.ts`](./museum/directed.test.ts) builds them.

**What is caught differs by rule, and the difference is the interesting part.** Three break a safety property outright. One leaves every safety property intact and quietly stops the cluster converging. One costs no safety at all — only progress. They are labelled accordingly rather than flattened into "the test goes red".

## The bug it found in its own implementation

Worth telling, because it is the case the harness exists for.

Early on, every safety property held under every schedule — and the chaos suite still failed, on run 25, with two replicas holding different values. The logs agreed. Nothing had crashed. No property in Figure 3 of the paper had been violated.

The cause is §5.4.2 working exactly as specified. A leader may not commit an entry inherited from an earlier term by counting replicas; it has to commit an entry of its own term, which carries the earlier ones with it. But if the clients happen to fall silent right after a leadership change, that entry never arrives — so the inherited entries stay uncommitted forever, and replicas that applied them under the previous leader sit permanently ahead of replicas that never got the commit signal.

The paper's remedy is one sentence in §8: a new leader appends a blank no-op entry for its own term. That is now in [`node.ts`](./src/raft/node.ts), and its absence is exhibit four.

A test that only checked "did the writes end up right" would have passed. A test that only checked Raft's own rules would have passed. Comparing replicas after the dust settles is what caught it, which is why that comparison is part of the harness rather than an afterthought.

## What is checked

**The five safety properties** from Figure 3, evaluated after every state transition — after a tick, after a delivered message — rather than polled. A violation is attributed to the transition that caused it.

| Property | Meaning |
| --- | --- |
| Election Safety | at most one leader per term |
| Leader Append-Only | a leader only appends to its log, never rewrites it |
| Log Matching | same index and term implies identical logs up to there |
| Leader Completeness | a committed entry appears in every later leader's log |
| State Machine Safety | no two replicas apply different commands at an index |

**Linearizability** of the client history, by [Wing and Gong's algorithm](./src/check/linearizability.ts): every operation must appear to take effect instantaneously at some point between its call and its return, in a single order consistent with real time. A system can satisfy all five Raft properties and still fail this — apply a retried command twice and consensus is intact while the client saw something impossible. Deciding linearizability is NP-complete, so the search memoises on (remaining operations, model state) and reports `inconclusive` rather than guessing when its budget runs out.

**Convergence**, once the faults stop. Safety must hold at all times; agreement is only owed after the network stops fighting the algorithm, so replicas are compared at the end rather than mid-partition.

## How it is put together

**Raft is a state machine with no I/O in it.** The node never touches a clock, a socket or a disk. `tick` advances its timers, `receive` feeds it a message, `propose` gives it a command, and everything it wants to do comes back out through `takeMessages` and `takeApplied`. That is not a stylistic preference: an implementation that calls `setTimeout` and `socket.write` internally can only be tested by running it and hoping the interesting interleavings turn up. etcd's raft package is shaped the same way for the same reason.

**The cluster exists only inside a simulation.** Every tick, message delay, dropped packet and crash is a decision drawn from a seed, using [unflake](https://github.com/BOTIROFF-D/unflake). Raft's proofs assume messages may be lost, delayed, reordered and duplicated and that nodes may crash and restart; anything less than injecting all five is testing a network the algorithm was never designed for.

**Faults are bounded to keep a quorum reachable.** Not politeness — Raft owes nothing without a quorum, so a scenario that isolates every group is testing a system that is *allowed* to stop, and a client giving up there would be correct behaviour reported as a bug. Each act is checked before it happens and skipped if it would leave no group with a quorum of live nodes.

**Safety rules are individually switchable**, which is what makes the museum possible and doubles as documentation of which line of the paper is doing which job.

## Usage

```bash
npm install @botiroff/bulwark
```

Or to work on it:

```bash
npm install
npm test        # the implementation, under fault injection
npm run museum  # every exhibit, ~6 seconds
```

```ts
import { check } from "unflake";
import { runScenario, SafetyMonitor, checkLinearizable, operationsFrom, HOSTILE } from "@botiroff/bulwark";

await check("raft stays safe", async (sim) => {
  const { cluster } = await runScenario(sim, {
    size: 5,
    clients: 3,
    faults: HOSTILE,
    chaos: true, // crashes, restarts, partitions
  });

  const disagreement = SafetyMonitor.replicasAgree(cluster);
  if (disagreement) sim.fail(disagreement);

  const report = checkLinearizable(operationsFrom(cluster.history));
  if (report.status === "not-linearizable") sim.fail(report.reason);
}, { runs: 200 });
```

## Limits

Stated plainly, because a consensus implementation that oversells itself is worse than one that does not exist.

**This is not production Raft.** No membership changes, no log compaction, no snapshots. A restarted node replays its whole log. Those are the three features that turn a correct algorithm into a deployable system, and every one of them has its own safety argument.

**Reads go through the log.** Correct and linearizable, and slower than it needs to be. Real implementations use lease-based or read-index reads, which are an optimisation with a fresh set of ways to be wrong.

**The model is the model.** Faults are message loss, delay, reordering, duplication, partitions, and crash-restart with durable state preserved. Not modelled: disk corruption, partial writes, clock skew between nodes, Byzantine behaviour, or a network that delivers to some recipients and not others within one broadcast.

**Passing is not proof.** Hundreds of seeds is hundreds of schedules from a space that is astronomically larger. It is a very good fuzz run, not a theorem.

Where a theorem is available, it is worth having. [pnueli](https://github.com/BOTIROFF-D/pnueli) model-checks Election Safety on a bounded specification of the same algorithm and visits every reachable state: 2,428 for three nodes over three terms, 6,801,084 for five nodes. Remove one-vote-per-term from that specification and two leaders in one term is simply reachable — the same failure this repository has to construct by hand, by killing a node in the one-tick window between granting a vote and persisting it.

The two answer different questions and neither replaces the other. A proof about a specification says the algorithm is sound; it says nothing about whether this code implements that algorithm, which is what the fault injection here is for.

**The linearizability checker can give up.** NP-complete in general; the budget is finite; `inconclusive` is a real outcome and is reported as one rather than rounded to "fine".

## Prior art

The algorithm is [Ongaro and Ousterhout's](https://raft.github.io/raft.pdf), and section references throughout the code point at the extended paper.

The testing approach comes from [FoundationDB](https://apple.github.io/foundationdb/testing.html), which built its own language to get deterministic simulation, and [TigerBeetle](https://github.com/tigerbeetle/tigerbeetle), whose VOPR does the same for a financial ledger. [Jepsen](https://jepsen.io/) attacks the problem from outside and is where linearizability checking of real systems became standard practice; [Knossos](https://github.com/jepsen-io/knossos) and [Porcupine](https://github.com/anishathalye/porcupine) are the checkers this one is a small relative of. The no-I/O node design is [etcd's](https://github.com/etcd-io/raft).

Part of a set: [unflake](https://github.com/BOTIROFF-D/unflake) samples the schedules, this tests consensus with them, [adya](https://github.com/BOTIROFF-D/adya) tests transaction isolation, and [pnueli](https://github.com/BOTIROFF-D/pnueli) proves small instances outright instead of sampling.

## Who wrote this

[Doniyor Botirov](https://dbit.one/en/founder), founder of [dbit.one](https://dbit.one/en). The reasoning behind this repository at length — the four ways an acknowledged write disappears, and why the museum exists: [The master dies: what actually happens to your data](https://dbit.one/en/blog/raft-consensus-deterministic-simulation).

## License

MIT
