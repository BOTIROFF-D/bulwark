/**
 * A whole scenario: a cluster, some clients, and something going wrong.
 *
 * The faults are bounded so that a majority always survives. That is not
 * politeness — Raft guarantees nothing without a quorum, so a scenario that
 * kills three of five nodes is testing a system that is allowed to stop, and
 * a client giving up there would be a correct outcome reported as a bug. The
 * interesting region is exactly the one where progress is still required.
 */

import type { Sim } from "unflake";
import { Cluster, TICK_MS, type ClusterOptions, type FaultProfile } from "./cluster.js";
import { SafetyMonitor } from "../check/invariants.js";
import type { Command, SafetyRules } from "../raft/types.js";

export interface ScenarioOptions {
  readonly size?: number;
  readonly clients?: number;
  readonly operationsPerClient?: number;
  readonly faults?: FaultProfile;
  readonly rules?: SafetyRules;
  /** Crash, restart and partition nodes while the clients work. */
  readonly chaos?: boolean;
  /** Keys the clients contend over. Fewer keys means more contention. */
  readonly keys?: readonly string[];
  /**
   * Called as soon as the cluster and monitor exist.
   *
   * A safety violation aborts the run through unflake's invariant, so the
   * scenario never returns and its caller never sees which property broke —
   * the failure message only carries the invariant's name. This hands the
   * monitor over up front so the specific violation can be read afterwards,
   * which is what the museum needs in order to assert that the *right* thing
   * was caught rather than merely something.
   */
  readonly onStart?: (monitor: SafetyMonitor, cluster: Cluster) => void;
}

export interface ScenarioResult {
  readonly cluster: Cluster;
  readonly monitor: SafetyMonitor;
}

export async function runScenario(sim: Sim, options: ScenarioOptions = {}): Promise<ScenarioResult> {
  const size = options.size ?? 5;
  const clients = options.clients ?? 3;
  const operations = options.operationsPerClient ?? 6;
  const keys = options.keys ?? ["a", "b"];

  const clusterOptions: ClusterOptions = {
    size,
    ...(options.rules ? { rules: options.rules } : {}),
    ...(options.faults ? { faults: options.faults } : {}),
  };
  const cluster = new Cluster(sim, clusterOptions);
  const monitor = new SafetyMonitor();
  cluster.watch(monitor);
  options.onStart?.(monitor, cluster);

  // The invariant is a plain field read: the monitor already ran at the
  // transition, so unflake attributes the violation to the step that caused
  // it without paying for a scan on every scheduling decision.
  sim.invariant("raft safety properties", () => monitor.violation === null);

  cluster.start();

  let chaosRunning = options.chaos === true;
  if (chaosRunning) {
    void sim.spawn("chaos", async () => {
      while (chaosRunning) {
        await sim.sleep(sim.pick("chaos gap", [15, 30, 50]) * TICK_MS);
        if (!chaosRunning) return;
        applyChaos(sim, cluster, size);
      }
    });
  }

  await Promise.all(
    Array.from({ length: clients }, (_, process) =>
      sim.spawn(`client-${process}`, async () => {
        for (let i = 0; i < operations; i++) {
          await cluster.invoke(process, randomCommand(sim, keys, process, i));
          await sim.sleep(sim.pick("client gap", [1, 3, 8]) * TICK_MS);
        }
      }),
    ),
  );

  chaosRunning = false;
  await cluster.settle();
  cluster.stop();

  return { cluster, monitor };
}

function randomCommand(
  sim: Sim,
  keys: readonly string[],
  process: number,
  sequence: number,
): Command {
  const key = sim.pick("key", keys) as string;
  const kind = sim.pick("op", ["put", "get", "cas"] as const);
  const value = `p${process}.${sequence}`;
  switch (kind) {
    case "put":
      return { kind: "put", key, value };
    case "get":
      return { kind: "get", key };
    case "cas":
      // Usually a miss, which is fine — a rejected cas is as much of a
      // linearizability obligation as an accepted one.
      return { kind: "cas", key, expected: sim.pick("cas expects", [null, value]), value };
  }
}

/**
 * One disruptive act, chosen from the seed.
 *
 * Crashes and partitions compose, and composing them carelessly is how a
 * fault-injection suite starts reporting false findings: two nodes down plus
 * a three-way split can leave every group short of a quorum, at which point
 * Raft is *supposed* to stop and a client giving up is correct behaviour.
 * So each act is checked before it happens — if it would leave no group with
 * a quorum of live nodes, it is skipped.
 *
 * The result is a fault model that stays inside the region where the
 * algorithm still owes us progress, which is the only region where a stall
 * means something.
 */
function applyChaos(sim: Sim, cluster: Cluster, size: number): void {
  const quorum = Math.floor(size / 2) + 1;
  const down = cluster.ids.filter((id) => cluster.isDown(id));
  const action = sim.pick("chaos", ["crash", "restart", "partition", "heal"] as const);

  switch (action) {
    case "crash": {
      const alive = cluster.ids.filter((id) => !cluster.isDown(id));
      const victim = sim.pick("crash victim", alive);
      if (!victim) return;
      if (!keepsQuorum(cluster, quorum, new Set([...down, victim]))) return;
      cluster.crash(victim);
      return;
    }

    case "restart": {
      const victim = sim.pick("restart victim", down.length > 0 ? down : cluster.ids);
      if (victim) cluster.restart(victim);
      return;
    }

    case "partition": {
      // Isolate as many live nodes as can be spared while still leaving one
      // side with a quorum. The isolated side cannot elect a leader, which is
      // the point: whichever leader survives must be on the majority side,
      // and any leader stranded in the minority has to discover it is no
      // longer in charge.
      const live = cluster.ids.filter((id) => !cluster.isDown(id));
      const spare = live.length - quorum;
      if (spare <= 0) return;
      const isolated = new Set([...live.slice(0, spare), ...down]);
      cluster.setPartition([
        cluster.ids.filter((id) => isolated.has(id)),
        cluster.ids.filter((id) => !isolated.has(id)),
      ]);
      return;
    }

    case "heal":
      cluster.healPartition();
      return;
  }
}

/** Would some partition group still hold a quorum of live nodes? */
function keepsQuorum(cluster: Cluster, quorum: number, down: ReadonlySet<string>): boolean {
  const perGroup = new Map<number, number>();
  for (const id of cluster.ids) {
    if (down.has(id)) continue;
    const group = cluster.groupOf(id);
    perGroup.set(group, (perGroup.get(group) ?? 0) + 1);
  }
  for (const count of perGroup.values()) if (count >= quorum) return true;
  return false;
}
