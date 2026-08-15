/**
 * The main event: Raft under partitions, crashes, dropped, delayed,
 * reordered and duplicated messages — with the paper's five safety
 * properties checked at every state transition and every client history
 * verified linearizable.
 *
 * Each run is a different schedule drawn from a seed, so a failure here comes
 * back as a number rather than a story about a build server.
 */

import { describe, expect, it } from "vitest";
import { check } from "unflake";
import { HOSTILE, NO_FAULTS } from "../src/sim/cluster.js";
import { runScenario } from "../src/sim/scenario.js";
import { SafetyMonitor } from "../src/check/invariants.js";
import { checkLinearizable, operationsFrom } from "../src/check/linearizability.js";
import type { Sim } from "unflake";

const scenario =
  (options: Parameters<typeof runScenario>[1]) =>
  async (sim: Sim): Promise<void> => {
    const { cluster } = await runScenario(sim, options);

    // Convergence, once the faults have stopped.
    const disagreement = SafetyMonitor.replicasAgree(cluster);
    if (disagreement) sim.fail(disagreement);

    // And the property a client can actually feel.
    const report = checkLinearizable(operationsFrom(cluster.history));
    if (report.status === "not-linearizable") sim.fail(report.reason);
  };

describe("raft under fault injection", () => {
  it("holds safety and linearizability with a lossy network", async () => {
    const report = await check(
      "safe and linearizable under a lossy network",
      scenario({ size: 5, clients: 3, operationsPerClient: 4, faults: HOSTILE }),
      { runs: 60, verbose: false },
    );
    expect(report.ok).toBe(true);
  });

  it("holds safety and linearizability through crashes and partitions", async () => {
    const report = await check(
      "safe and linearizable under chaos",
      scenario({ size: 5, clients: 3, operationsPerClient: 4, faults: HOSTILE, chaos: true }),
      { runs: 60, verbose: false },
    );
    expect(report.ok).toBe(true);
  });

  it("survives a three-node cluster, where a single failure is half the quorum", async () => {
    const report = await check(
      "three nodes stay safe under chaos",
      scenario({ size: 3, clients: 2, operationsPerClient: 4, faults: HOSTILE, chaos: true }),
      { runs: 60, verbose: false },
    );
    expect(report.ok).toBe(true);
  });

  it("still agrees when nothing goes wrong", async () => {
    const report = await check(
      "safe and linearizable on a quiet network",
      scenario({ size: 5, clients: 3, operationsPerClient: 5, faults: NO_FAULTS }),
      { runs: 40, verbose: false },
    );
    expect(report.ok).toBe(true);
  });
});
