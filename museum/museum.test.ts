/**
 * The bug museum.
 *
 * A test suite that only ever passes proves very little — it might be finding
 * bugs, or nothing might be looking. So each exhibit takes the working
 * implementation, switches off exactly one of Raft's rules, and requires the
 * harness to catch it: with a seed, and with a named property.
 *
 * None of these are strawmen. Each has a subsection of the paper devoted to
 * explaining why the obvious version is unsafe, and the paper would not need
 * those subsections if nobody had shipped the obvious version.
 *
 * What is caught differs by rule, and the difference is the interesting part:
 * some break a safety property immediately, one leaves every safety property
 * intact and quietly stops the cluster converging, and one costs no safety at
 * all — only progress. They are labelled accordingly rather than flattened
 * into "the test goes red".
 *
 * A failure here means the harness stopped catching something it used to.
 * That is worse news than a failure in the main suite.
 */

import { describe, expect, it } from "vitest";
import { check, UnflakeFailure } from "unflake";
import { CORRECT_RULES, type SafetyRules } from "../src/raft/types.js";
import { HOSTILE } from "../src/sim/cluster.js";
import { runScenario, type ScenarioOptions } from "../src/sim/scenario.js";
import { SafetyMonitor } from "../src/check/invariants.js";
import { checkLinearizable, operationsFrom } from "../src/check/linearizability.js";
import type { Sim } from "unflake";

interface Caught {
  readonly what: string;
  readonly onRun: number;
}

async function exhibit(
  name: string,
  broken: Partial<SafetyRules>,
  runs: number,
): Promise<Caught | null> {
  const rules: SafetyRules = { ...CORRECT_RULES, ...broken };
  // A safety violation aborts the run through the invariant, so the scenario
  // never returns — the monitor has to be captured on the way in if the
  // specific property is to be reported rather than the invariant's name.
  // Every run gets its own monitor, and shrinking runs the scenario again
  // with variants that may not reproduce — so reading "the last monitor"
  // would often find a clean one. Keep them all and report the first that
  // actually caught something.
  const monitors: SafetyMonitor[] = [];
  const options: ScenarioOptions = {
    size: 5,
    clients: 3,
    operationsPerClient: 4,
    faults: HOSTILE,
    chaos: true,
    rules,
    onStart: (m) => {
      monitors.push(m);
    },
  };

  const body = async (sim: Sim): Promise<void> => {
    const { cluster } = await runScenario(sim, options);
    const disagreement = SafetyMonitor.replicasAgree(cluster);
    if (disagreement) sim.fail(disagreement);
    const report = checkLinearizable(operationsFrom(cluster.history));
    if (report.status === "not-linearizable") sim.fail(report.reason);
  };

  const error = await check(name, body, {
    runs,
    verbose: false,
    // The museum only needs to know a bug was caught, not to read a minimal
    // trace of it, and a full shrink costs more than finding the bug did.
    shrinkAttempts: 20,
    maxWallClockMs: 20_000,
  }).then(
    () => null,
    (e: unknown) => (e instanceof UnflakeFailure ? e : null),
  );
  const failure = error?.report.failure?.failure;
  if (!failure) return null;

  const specific = monitors.find((m) => m.violation !== null)?.violation;
  return { what: specific ?? failure.message, onRun: error?.report.failedOnRun ?? 0 };
}

function report(label: string, caught: Caught | null): void {
  console.log(`  ${label.padEnd(26)} → ${caught?.what} (run ${caught?.onRun})`);
}

describe("bug museum", () => {
  it("catches a candidate with a stale log winning an election (§5.4.1)", async () => {
    // Without the up-to-date check a node missing committed entries can win,
    // and everything it is missing is overwritten. This is the rule the whole
    // election restriction exists for.
    const caught = await exhibit("no up-to-date check", { upToDateCheck: false }, 80);
    expect(caught, "a stale candidate winning is no longer caught").not.toBeNull();
    expect(caught?.what).toMatch(/Leader Completeness|Commit divergence|State Machine Safety/);
    report("no up-to-date check", caught);
  });

  it("catches appending entries without checking the previous one (§5.3)", async () => {
    const caught = await exhibit("no prevLog check", { prevLogCheck: false }, 80);
    expect(caught, "a missing prevLog check is no longer caught").not.toBeNull();
    report("no prevLog check", caught);
  });

  it("catches a follower keeping a conflicting entry (§5.3)", async () => {
    const caught = await exhibit("no truncation", { truncateConflicting: false }, 80);
    expect(caught, "a follower keeping a conflict is no longer caught").not.toBeNull();
    report("keeps conflicting entry", caught);
  });

  it("catches a cluster that stops converging without the leader's blank entry (§8)", async () => {
    // The one this project found in its own implementation. Every safety
    // property holds throughout: §5.4.2 simply forbids committing inherited
    // entries by counting replicas, so with no entry of the leader's own term
    // they stay uncommitted — and if the clients fall silent there, replicas
    // that applied them under the previous leader are permanently ahead.
    //
    // Nothing crashes and nothing is logged. Only a comparison of replicas
    // after the dust settles shows it, which is why that comparison is part
    // of the harness rather than an afterthought.
    const caught = await exhibit("no leader no-op", { leaderNoop: false }, 120);
    expect(caught, "the missing no-op is no longer caught").not.toBeNull();
    expect(caught?.what).toMatch(/replicas disagree/);
    report("no leader no-op", caught);
  });

  it("catches a node that ignores a newer term — as a stall, not a safety bug (§5.1)", async () => {
    // Worth being precise about. A node that will not adopt a higher term
    // does not corrupt anything: its stale-term messages are rejected, and
    // its own vote requests are refused by everyone who has moved on. It just
    // stops being useful. With enough of the cluster inert, clients run out
    // of retries.
    //
    // So the harness catches this one, but not through a safety property —
    // through a client that never gets an answer. Filing it as a safety
    // violation would be more dramatic and less true.
    const caught = await exhibit("no step down", { stepDownOnHigherTerm: false }, 40);
    expect(caught, "a node ignoring a newer term is no longer caught").not.toBeNull();
    expect(caught?.what).toMatch(/gave up/);
    report("ignores a newer term", caught);
  });
});
