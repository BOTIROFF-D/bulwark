/**
 * Exhibits that a random search will not reach.
 *
 * Two of Raft's rules guard against situations so specific that hoping to
 * stumble into them is not a plan. Figure 8 needs four leadership changes in
 * a prescribed order with prescribed partitions; the persistence rule opens a
 * window one tick wide. Both are constructed here instead, which is not a
 * weaker test — it is the difference between "we ran it a lot" and "here is
 * the scenario, and here is what it does".
 *
 * The randomised museum next door still covers the rules whose violations are
 * common enough to fall out of chaos on their own.
 */

import { describe, expect, it } from "vitest";
import { simulate, type Sim } from "unflake";
import { Cluster, TICK_MS } from "../src/sim/cluster.js";
import { CORRECT_RULES } from "../src/raft/types.js";
import { SafetyMonitor } from "../src/check/invariants.js";

const QUIET = { dropRate: 0, duplicateRate: 0, latencyTicks: [2, 2] as const };
/** Long enough that nothing campaigns on its own: elections are driven here. */
const MANUAL_ELECTIONS = [100_000, 100_000] as const;

async function ticks(sim: Sim, n: number): Promise<void> {
  await sim.sleep(n * TICK_MS);
}

/**
 * Campaign until this node wins, or give up.
 *
 * One campaign is rarely enough. Every node that hears a vote request adopts
 * its term, so a node returning from a crash starts behind and its first
 * attempt is refused by peers that already voted in that term. Retrying is
 * what a real candidate does when its election times out, and it is the only
 * way to say "this specific node leads next" without rigging the timers.
 */
async function electLeader(sim: Sim, cluster: Cluster, id: string): Promise<boolean> {
  for (let attempt = 0; attempt < 10; attempt++) {
    cluster.node(id)?.campaign();
    for (let i = 0; i < 40; i++) {
      await ticks(sim, 1);
      if (cluster.node(id)?.currentRole === "leader") return true;
    }
  }
  return false;
}

describe("directed exhibits", () => {
  it("loses a committed entry when a leader commits an inherited one (Figure 8)", async () => {
    let violation: string | null = null;

    const result = await simulate(
      async (sim) => {
        const cluster = new Cluster(sim, {
          size: 5,
          faults: QUIET,
          electionTimeoutTicks: MANUAL_ELECTIONS,
          heartbeatTicks: 4,
          // Both switches off. The blank entry a leader normally appends
          // would create a current-term entry that commits legitimately and
          // carries the inherited ones with it — which is exactly why the
          // paper prescribes it, and why the commit restriction alone is not
          // what makes this scenario unreachable.
          rules: { ...CORRECT_RULES, currentTermCommitOnly: false, leaderNoop: false },
        });
        const monitor = new SafetyMonitor();
        cluster.watch(monitor);
        cluster.start();
        const [s1, s2, s3, s4, s5] = cluster.ids as [string, string, string, string, string];

        // (a) S1 leads term 1 and gets one entry onto itself and S2 only.
        expect(await electLeader(sim, cluster, s1), "S1 never led term 1").toBe(true);
        cluster.setPartition([
          [s1, s2],
          [s3, s4, s5],
        ]);
        cluster.node(s1)?.propose({ kind: "put", key: "x", value: "A" }, "entry-A");
        await ticks(sim, 30);

        // (b) S1 goes away. S5 wins term 2 among the other three and appends
        //     an entry of its own that reaches nobody — isolated before the
        //     outbox is ever drained.
        cluster.crash(s1);
        cluster.setPartition([[s2], [s3, s4, s5]]);
        expect(await electLeader(sim, cluster, s5), "S5 never led").toBe(true);
        cluster.node(s5)?.propose({ kind: "put", key: "x", value: "B" }, "entry-B");
        cluster.setPartition([[s5], [s2], [s3, s4]]);
        await ticks(sim, 30);

        // (c) S1 comes back and wins term 3: S2 has the same log, S3 has an
        //     empty one, so both vote for it. It replicates its term-1 entry
        //     to S3 — now a majority holds it, and with the commit
        //     restriction gone S1 declares it committed.
        cluster.restart(s1);
        cluster.setPartition([
          [s1, s2, s3],
          [s4],
          [s5],
        ]);
        expect(await electLeader(sim, cluster, s1), "S1 never led again").toBe(true);
        await ticks(sim, 40);

        // (d) S1 goes away again and S5 returns. Its entry is from a later
        //     term, so everyone votes for it — and it overwrites the entry
        //     S1 committed in (c).
        cluster.crash(s1);
        cluster.healPartition();
        await electLeader(sim, cluster, s5);
        await ticks(sim, 60);

        violation = monitor.violation;
        cluster.stop();
      },
      { seed: 1, maxWallClockMs: 60_000 },
    );

    expect(result.ok).toBe(true);
    expect(violation, "the harness stopped catching the Figure 8 scenario").not.toBeNull();
    console.log(`  Figure 8            → ${violation}`);
  });

  it("elects two leaders in one term when a vote does not survive a crash", async () => {
    let violation: string | null = null;

    const result = await simulate(
      async (sim) => {
        const cluster = new Cluster(sim, {
          size: 3,
          faults: QUIET,
          electionTimeoutTicks: MANUAL_ELECTIONS,
          rules: { ...CORRECT_RULES, persistBeforeReply: false },
        });
        const monitor = new SafetyMonitor();
        const [n1, n2, n3] = cluster.ids as [string, string, string];

        // Crash n1 the instant it has granted a vote it has not yet written
        // down. The reply is already in flight by the time this runs, so the
        // candidate counts a vote the voter is about to forget — a window a
        // random search would have to land in exactly, and never does.
        let crashed = false;
        cluster.watch({
          check(c) {
            monitor.check(c);
            if (crashed) return;
            if (c.node(n1)?.hasUnpersistedState) {
              c.crash(n1);
              crashed = true;
            }
          },
        });

        cluster.start();

        // n3 sits out the first election. Hearing a vote request would raise
        // its term, and it would then campaign in the *next* one — two
        // leaders in consecutive terms is ordinary Raft. The violation is two
        // leaders in the *same* term, so n3 has to stay at term 0.
        cluster.setPartition([[n3], [n1, n2]]);

        cluster.node(n2)?.campaign();
        await ticks(sim, 20);
        expect(crashed, "n1 never reached the unpersisted window").toBe(true);
        expect(cluster.node(n2)?.currentRole, "n2 did not win term 1").toBe("leader");

        // n1 wakes with no memory of the vote, or even of the term it was
        // cast in. Keep n2 away so nothing reminds it.
        cluster.restart(n1);
        cluster.setPartition([[n2], [n1, n3]]);
        await ticks(sim, 5);
        cluster.node(n3)?.campaign();
        await ticks(sim, 40);

        violation = monitor.violation;
        cluster.stop();
      },
      { seed: 1, maxWallClockMs: 60_000 },
    );

    expect(result.ok).toBe(true);
    expect(violation, "the harness stopped catching an unpersisted vote").not.toBeNull();
    console.log(`  unpersisted vote    → ${violation}`);
  });
});
