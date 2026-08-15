/**
 * The five safety properties from Figure 3 of the Raft paper, checked
 * continuously.
 *
 *   Election Safety      at most one leader per term
 *   Leader Append-Only   a leader only appends to its log, never rewrites it
 *   Log Matching         same index and term implies identical logs up to there
 *   Leader Completeness  a committed entry appears in every later leader's log
 *   State Machine Safety no two replicas apply different commands at an index
 *
 * These are not assertions about the end state. Raft can pass every "is the
 * data right afterwards" test while violating safety in the middle of a
 * partition and getting away with it because the offending replica was
 * overwritten before anyone looked. So the monitor runs at every point where
 * a node's state actually changes — after a tick, after a delivered
 * message — rather than polling, which makes it both exact and cheap.
 *
 * Cheapness matters more than it sounds. The checker runs tens of thousands
 * of times per scenario, so the scan is written to cost integer comparisons
 * on the unchanged part of each log and to touch a hash map only where an
 * entry has actually changed.
 */

import type { RaftNode } from "../raft/node.js";
import type { NodeId } from "../raft/types.js";
import type { Cluster } from "../sim/cluster.js";

interface CommittedEntry {
  readonly term: number;
  readonly requestId: string;
}

export class SafetyMonitor {
  /** The first violation seen. Null while everything holds. */
  violation: string | null = null;

  /** "index:term" → requestId. Log Matching, in its smallest useful form. */
  private readonly entryIdentity = new Map<string, string>();
  /** Per node, the term we last validated at each log index. */
  private readonly validated = new Map<NodeId, number[]>();
  /** Entries known committed anywhere, by index. */
  private readonly committed = new Map<number, CommittedEntry>();
  private highestCommitted = 0;
  /** Leaders already checked for completeness, as "id:term". */
  private readonly completeLeaders = new Set<string>();
  /** Longest log each leader had while holding a given term. */
  private readonly leaderLogLength = new Map<NodeId, { term: number; length: number }>();

  private fail(message: string): void {
    if (!this.violation) this.violation = message;
  }

  check(cluster: Cluster): void {
    if (this.violation) return;

    const live: RaftNode[] = [];
    for (const id of cluster.ids) {
      const node = cluster.node(id);
      if (node) live.push(node);
    }

    this.checkElectionSafety(live);
    this.checkLeaderAppendOnly(live);
    this.checkLogMatching(live);
    this.recordCommitted(live);
    this.checkLeaderCompleteness(live);
    this.checkStateMachineSafety(cluster);
  }

  /** At most one leader per term (§5.2), guaranteed by one-vote-per-term. */
  private checkElectionSafety(live: readonly RaftNode[]): void {
    const leaderOfTerm = new Map<number, NodeId>();
    for (const node of live) {
      if (node.currentRole !== "leader") continue;
      const existing = leaderOfTerm.get(node.term);
      if (existing && existing !== node.id) {
        this.fail(`Election Safety: ${existing} and ${node.id} are both leader in term ${node.term}`);
        return;
      }
      leaderOfTerm.set(node.term, node.id);
    }
  }

  /** A leader never removes or overwrites its own entries (§5.3). */
  private checkLeaderAppendOnly(live: readonly RaftNode[]): void {
    for (const node of live) {
      if (node.currentRole !== "leader") continue;
      const previous = this.leaderLogLength.get(node.id);
      if (previous && previous.term === node.term && node.entries.length < previous.length) {
        this.fail(
          `Leader Append-Only: ${node.id} shrank its log from ${previous.length} to ` +
            `${node.entries.length} while leader in term ${node.term}`,
        );
        return;
      }
      this.leaderLogLength.set(node.id, { term: node.term, length: node.entries.length });
    }
  }

  /**
   * Two entries with the same index and term are the same entry (§5.3).
   *
   * Walking each log from the start would be correct and far too slow, so the
   * walk skips any prefix whose terms are unchanged since the last check —
   * integer comparisons, no allocation. Only entries that actually moved get
   * looked up. A truncation changes a term at the point it happened, so the
   * scan naturally resumes from there.
   */
  private checkLogMatching(live: readonly RaftNode[]): void {
    for (const node of live) {
      let seen = this.validated.get(node.id);
      if (!seen) {
        seen = [];
        this.validated.set(node.id, seen);
      }
      const log = node.entries;

      for (let i = 0; i < log.length; i++) {
        const entry = log[i];
        if (!entry) continue;
        if (seen[i] === entry.term) continue; // unchanged since last time

        const key = `${i + 1}:${entry.term}`;
        const known = this.entryIdentity.get(key);
        if (known === undefined) {
          this.entryIdentity.set(key, entry.requestId);
        } else if (known !== entry.requestId) {
          this.fail(
            `Log Matching: index ${i + 1} term ${entry.term} holds ${known} elsewhere ` +
              `but ${entry.requestId} on ${node.id}`,
          );
          return;
        }
        seen[i] = entry.term;
      }
      if (seen.length > log.length) seen.length = log.length;
    }
  }

  /** Remember what got committed, so later leaders can be held to it. */
  private recordCommitted(live: readonly RaftNode[]): void {
    for (const node of live) {
      const upto = Math.min(node.committed, node.entries.length);
      for (let index = this.highestCommitted + 1; index <= upto; index++) {
        const entry = node.entries[index - 1];
        if (!entry) break;
        const known = this.committed.get(index);
        if (known && (known.term !== entry.term || known.requestId !== entry.requestId)) {
          this.fail(
            `Commit divergence: index ${index} was committed as ${known.requestId} ` +
              `(term ${known.term}) but ${node.id} has ${entry.requestId} (term ${entry.term})`,
          );
          return;
        }
        this.committed.set(index, { term: entry.term, requestId: entry.requestId });
        this.highestCommitted = Math.max(this.highestCommitted, index);
      }
    }
  }

  /**
   * A committed entry is in the log of every leader of a later term (§5.4).
   *
   * Checked when a node first becomes leader in a term — which is precisely
   * when the property has something to say — and again for each newly
   * committed entry against the leaders already standing. Both directions are
   * needed and both are cheap; re-verifying every leader against every
   * committed entry on every state change would be neither.
   */
  private checkLeaderCompleteness(live: readonly RaftNode[]): void {
    for (const node of live) {
      if (node.currentRole !== "leader") continue;
      const key = `${node.id}:${node.term}`;
      if (this.completeLeaders.has(key)) continue;
      this.completeLeaders.add(key);

      for (const [index, entry] of this.committed) {
        if (entry.term >= node.term) continue;
        const present = node.entries[index - 1];
        if (!present || present.term !== entry.term || present.requestId !== entry.requestId) {
          this.fail(
            `Leader Completeness: ${node.id} became leader in term ${node.term} without ` +
              `committed entry ${entry.requestId} at index ${index} (term ${entry.term})`,
          );
          return;
        }
      }
    }
  }

  /** No two replicas apply different commands at the same index (§5.4.3). */
  private checkStateMachineSafety(cluster: Cluster): void {
    if (cluster.divergence) this.fail(`State Machine Safety: ${cluster.divergence}`);
  }

  /**
   * Once a scenario has quiesced, every replica should hold the same data.
   * This is convergence, not safety — it is only guaranteed after the faults
   * stop — so it is checked at the end rather than continuously.
   */
  static replicasAgree(cluster: Cluster): string | null {
    const snapshots = cluster.ids.map((id) => ({
      id,
      state: JSON.stringify(cluster.machine(id).snapshot()),
    }));
    const first = snapshots[0];
    if (!first) return null;
    for (const other of snapshots) {
      if (other.state !== first.state) {
        return `replicas disagree: ${first.id}=${first.state} but ${other.id}=${other.state}`;
      }
    }
    return null;
  }
}
