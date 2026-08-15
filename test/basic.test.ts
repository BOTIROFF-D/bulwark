/**
 * The floor: does it work at all when nothing is going wrong?
 *
 * Worth having separately from the fault-injection suite, because a bug that
 * breaks leader election in a quiet cluster should be reported as "election
 * is broken", not as a linearizability violation twenty seconds into a
 * partition.
 */

import { describe, expect, it } from "vitest";
import { simulate } from "unflake";
import { Cluster, NO_FAULTS } from "../src/sim/cluster.js";
import { SafetyMonitor } from "../src/check/invariants.js";

describe("a quiet cluster", () => {
  it("elects exactly one leader", async () => {
    let leaders: string[] = [];
    const result = await simulate(
      async (sim) => {
        const cluster = new Cluster(sim, { size: 3, faults: NO_FAULTS });
        cluster.start();
        await sim.sleep(200);
        leaders = cluster.ids.filter((id) => cluster.node(id)?.currentRole === "leader");
        cluster.stop();
      },
      { seed: 1 },
    );

    expect(result.ok).toBe(true);
    expect(leaders).toHaveLength(1);
  });

  it("replicates a write to every node and reads it back", async () => {
    let readBack: string | null = null;
    let logs: number[] = [];

    const result = await simulate(
      async (sim) => {
        const cluster = new Cluster(sim, { size: 3, faults: NO_FAULTS });
        const monitor = new SafetyMonitor();
        cluster.watch(monitor);
        sim.invariant("raft safety properties", () => monitor.violation === null);

        cluster.start();
        await sim.sleep(200); // let an election settle

        await cluster.invoke(0, { kind: "put", key: "a", value: "1" });
        const got = await cluster.invoke(0, { kind: "get", key: "a" });
        readBack = got.value;

        await sim.sleep(200); // let the commit index reach the followers
        logs = cluster.ids.map((id) => cluster.node(id)?.entries.length ?? -1);
        cluster.stop();
      },
      { seed: 1 },
    );

    expect(result.failure?.message).toBeUndefined();
    expect(result.ok).toBe(true);
    expect(readBack).toBe("1");
    // Three entries in every log: the leader's blank no-op for its term, the
    // put, and the get — reads go through the log too, so that they are
    // linearizable rather than merely fast.
    expect(logs).toEqual([3, 3, 3]);
  });

  it("keeps a compare-and-swap honest", async () => {
    const outcomes: { ok: boolean; value: string | null }[] = [];
    const result = await simulate(
      async (sim) => {
        const cluster = new Cluster(sim, { size: 3, faults: NO_FAULTS });
        cluster.start();
        await sim.sleep(200);

        outcomes.push(await cluster.invoke(0, { kind: "cas", key: "k", expected: "nope", value: "x" }));
        outcomes.push(await cluster.invoke(0, { kind: "cas", key: "k", expected: null, value: "x" }));
        outcomes.push(await cluster.invoke(0, { kind: "get", key: "k" }));
        cluster.stop();
      },
      { seed: 1 },
    );

    expect(result.ok).toBe(true);
    expect(outcomes[0]).toEqual({ ok: false, value: null }); // wrong expectation
    expect(outcomes[1]).toEqual({ ok: true, value: "x" }); // matched empty
    expect(outcomes[2]).toEqual({ ok: true, value: "x" });
  });
});
