/**
 * A cluster that only exists inside a simulation.
 *
 * Nothing here touches a real socket or a real clock. Every tick, every
 * message delay, every dropped packet and every crash is a decision drawn
 * from unflake's seed — so a run that breaks a safety property is not an
 * anecdote about a flaky integration test, it is a number you can hand to
 * someone else.
 *
 * The faults are the interesting part. Raft's proofs assume messages may be
 * "lost, delayed, reordered and duplicated", and nodes may "crash and later
 * restart"; anything less than injecting all five is testing a network that
 * the algorithm was never designed for.
 */

import type { Sim } from "unflake";
import { RaftNode } from "../raft/node.js";
import {
  CORRECT_RULES,
  type Command,
  type Message,
  type NodeId,
  type PersistentState,
  type SafetyRules,
} from "../raft/types.js";
import { KeyValue, type KvResult } from "./state-machine.js";

/** One virtual millisecond per Raft tick keeps the arithmetic readable. */
export const TICK_MS = 1;

export interface FaultProfile {
  /** Probability a message is dropped outright. */
  readonly dropRate: number;
  /** Probability a delivered message is also delivered a second time. */
  readonly duplicateRate: number;
  /** Delivery delay in ticks, drawn per message — this is what reorders them. */
  readonly latencyTicks: readonly [number, number];
}

export const NO_FAULTS: FaultProfile = {
  dropRate: 0,
  duplicateRate: 0,
  latencyTicks: [1, 2],
};

export const HOSTILE: FaultProfile = {
  dropRate: 0.1,
  duplicateRate: 0.05,
  latencyTicks: [1, 6],
};

export interface ClusterOptions {
  readonly size: number;
  readonly rules?: SafetyRules;
  readonly faults?: FaultProfile;
  readonly electionTimeoutTicks?: readonly [number, number];
  readonly heartbeatTicks?: number;
}

export interface HistoryEvent {
  readonly process: number;
  readonly type: "invoke" | "ok" | "fail";
  readonly command: Command;
  readonly result?: KvResult;
  readonly time: number;
}

interface Pending {
  readonly node: NodeId;
  readonly resolve: (result: KvResult) => void;
}

/**
 * Something that inspects the cluster whenever its state changes.
 *
 * Declared here as a bare interface rather than importing the safety monitor,
 * which would make the two modules import each other. It also means the
 * cluster does not care what is watching it.
 */
export interface Observer {
  check(cluster: Cluster): void;
}

export class Cluster {
  readonly ids: NodeId[];
  readonly history: HistoryEvent[] = [];

  /**
   * Which command ended up applied at each log index, by whichever replica
   * got there first. State Machine Safety says every replica must agree, so
   * a second replica writing something different here is the violation
   * itself, caught at the moment it happens rather than by comparing dumps
   * afterwards.
   */
  readonly appliedAt = new Map<number, { requestId: string; term: number; by: NodeId }>();
  /** Set when a replica applies a different command at an index than another did. */
  divergence: string | null = null;

  private readonly nodes = new Map<NodeId, RaftNode>();
  private readonly machines = new Map<NodeId, KeyValue>();
  private readonly durable = new Map<NodeId, PersistentState>();
  private readonly down = new Set<NodeId>();
  private readonly group = new Map<NodeId, number>();
  private readonly pending = new Map<string, Pending>();

  private readonly rules: SafetyRules;
  private faults: FaultProfile;
  private readonly electionTimeoutTicks: readonly [number, number];
  private readonly heartbeatTicks: number;
  private running = false;
  private requestCounter = 0;
  private observer: Observer | null = null;

  constructor(
    private readonly sim: Sim,
    options: ClusterOptions,
  ) {
    this.ids = Array.from({ length: options.size }, (_, i) => `n${i + 1}`);
    this.rules = options.rules ?? CORRECT_RULES;
    this.faults = options.faults ?? NO_FAULTS;
    this.electionTimeoutTicks = options.electionTimeoutTicks ?? [12, 24];
    this.heartbeatTicks = options.heartbeatTicks ?? 3;

    for (const id of this.ids) {
      this.machines.set(id, new KeyValue());
      this.group.set(id, 0);
      this.nodes.set(id, this.spawnNode(id));
      this.durable.set(id, this.nodes.get(id)!.durableState);
    }
  }

  private spawnNode(id: NodeId, restoreFrom?: PersistentState): RaftNode {
    return new RaftNode({
      id,
      peers: this.ids,
      electionTimeoutTicks: this.electionTimeoutTicks,
      heartbeatTicks: this.heartbeatTicks,
      random: () => this.sim.random(),
      rules: this.rules,
      ...(restoreFrom ? { restoreFrom } : {}),
    });
  }

  /**
   * Watch every state transition. Called after a tick and after each
   * delivered message — the only two places a node's state can move — so a
   * violation is caught at the transition that caused it rather than at
   * whatever moment a poller happened to look.
   */
  watch(observer: Observer): void {
    this.observer = observer;
  }

  private observe(): void {
    this.observer?.check(this);
  }

  node(id: NodeId): RaftNode | undefined {
    return this.down.has(id) ? undefined : this.nodes.get(id);
  }

  /** Every node, including crashed ones — the invariant checker needs them all. */
  allNodes(): RaftNode[] {
    return [...this.nodes.values()];
  }

  isDown(id: NodeId): boolean {
    return this.down.has(id);
  }

  machine(id: NodeId): KeyValue {
    return this.machines.get(id) as KeyValue;
  }

  // ── lifecycle ────────────────────────────────────────────────────────────

  start(): void {
    this.running = true;
    for (const id of this.ids) {
      void this.sim.spawn(`node-${id}`, async () => {
        while (this.running) {
          await this.sim.sleep(TICK_MS);
          if (!this.running) return;
          if (this.down.has(id)) continue;
          this.nodes.get(id)?.tick();
          this.drain(id);
          this.observe();
        }
      });
    }
  }

  stop(): void {
    this.running = false;
  }

  /**
   * Remove every fault, revive everyone, and let the cluster run until it
   * agrees. Safety must hold at all times, but *convergence* is only
   * guaranteed once the network stops fighting the algorithm — so the final
   * comparison of replicas is made here, not in the middle of a partition.
   */
  async settle(ticks = 400): Promise<void> {
    this.faults = NO_FAULTS;
    this.healPartition();
    for (const id of this.ids) if (this.down.has(id)) this.restart(id);
    await this.sim.sleep(ticks * TICK_MS);
  }

  // ── faults ───────────────────────────────────────────────────────────────

  crash(id: NodeId): void {
    if (this.down.has(id)) return;
    // Durable state survives; everything else — role, commitIndex, the state
    // machine — does not. A restarted node replays its log from the start.
    this.durable.set(id, this.nodes.get(id)!.durableState);
    this.machines.set(id, new KeyValue());
    this.down.add(id);
  }

  restart(id: NodeId): void {
    if (!this.down.has(id)) return;
    this.nodes.set(id, this.spawnNode(id, this.durable.get(id)));
    this.down.delete(id);
  }

  /** Split the cluster; messages between groups are silently dropped. */
  setPartition(groups: readonly (readonly NodeId[])[]): void {
    groups.forEach((members, index) => {
      for (const id of members) this.group.set(id, index);
    });
  }

  healPartition(): void {
    for (const id of this.ids) this.group.set(id, 0);
  }

  /** Which side of the split a node is on. Used to keep faults survivable. */
  groupOf(id: NodeId): number {
    return this.group.get(id) ?? 0;
  }

  private partitioned(from: NodeId, to: NodeId): boolean {
    return this.group.get(from) !== this.group.get(to);
  }

  // ── message plumbing ─────────────────────────────────────────────────────

  private drain(id: NodeId): void {
    const node = this.nodes.get(id);
    if (!node) return;

    for (const message of node.takeMessages()) this.deliver(message);

    for (const { index, entry } of node.takeApplied()) {
      const seen = this.appliedAt.get(index);
      if (!seen) {
        this.appliedAt.set(index, { requestId: entry.requestId, term: entry.term, by: id });
      } else if (seen.requestId !== entry.requestId && !this.divergence) {
        this.divergence =
          `index ${index}: ${seen.by} applied ${seen.requestId}, ` +
          `${id} applied ${entry.requestId}`;
      }

      const result = this.machines.get(id)?.apply(entry);
      const waiter = this.pending.get(entry.requestId);
      if (waiter && waiter.node === id && result) {
        this.pending.delete(entry.requestId);
        waiter.resolve(result);
      }
    }
  }

  private deliver(message: Message): void {
    void this.sim.spawn(`net-${message.from}->${message.to}`, async () => {
      if (this.faults.dropRate > 0 && this.sim.chance(`drop ${message.type}`, this.faults.dropRate)) {
        return;
      }
      await this.sim.io(`${message.from}->${message.to} ${message.type}`, {
        latency: [this.faults.latencyTicks[0] * TICK_MS, this.faults.latencyTicks[1] * TICK_MS],
      });
      this.handoff(message);

      if (
        this.faults.duplicateRate > 0 &&
        this.sim.chance(`duplicate ${message.type}`, this.faults.duplicateRate)
      ) {
        await this.sim.io(`duplicate ${message.type}`, {
          latency: [1 * TICK_MS, this.faults.latencyTicks[1] * TICK_MS],
        });
        this.handoff(message);
      }
    });
  }

  /** Deliver if the recipient is alive and reachable at *this* moment. */
  private handoff(message: Message): void {
    if (this.down.has(message.to)) return;
    if (this.partitioned(message.from, message.to)) return;
    const target = this.nodes.get(message.to);
    if (!target) return;
    target.receive(message);
    this.drain(message.to);
    this.observe();
  }

  // ── clients ──────────────────────────────────────────────────────────────

  /**
   * One client operation, retried until it gets a definite answer.
   *
   * Retrying keeps the history free of "maybe it happened" outcomes, which
   * would otherwise force the linearizability checker to consider both
   * possibilities for every timed-out request. It is safe only because the
   * state machine remembers request ids: the same operation reaching two
   * different leaders is applied once.
   */
  async invoke(process: number, command: Command, timeoutTicks = 60): Promise<KvResult> {
    const requestId = `r${++this.requestCounter}`;
    this.history.push({ process, type: "invoke", command, time: this.sim.now });

    let target = this.ids[process % this.ids.length] as NodeId;
    // Sixty attempts, five ticks apart, is three hundred ticks of retrying —
    // an order of magnitude more than an election needs. Past that the
    // cluster is not slow, it is stuck, and saying so quickly matters: a
    // client that keeps trying forever turns a liveness bug into a hang.
    for (let attempt = 0; attempt < 60; attempt++) {
      const outcome = await this.attempt(target, requestId, command, timeoutTicks);
      if (outcome) {
        this.history.push({
          process,
          type: "ok",
          command,
          result: outcome,
          time: this.sim.now,
        });
        return outcome;
      }
      // Follow the hint if there is one, otherwise walk the cluster.
      const hint = this.nodes.get(target)?.leaderId;
      target =
        hint && !this.down.has(hint)
          ? hint
          : (this.ids[(this.ids.indexOf(target) + 1) % this.ids.length] as NodeId);
      await this.sim.sleep(5 * TICK_MS);
    }

    this.history.push({ process, type: "fail", command, time: this.sim.now });
    throw new Error(`client ${process} gave up on ${command.kind}`);
  }

  private async attempt(
    target: NodeId,
    requestId: string,
    command: Command,
    timeoutTicks: number,
  ): Promise<KvResult | null> {
    const node = this.node(target);
    if (!node) return null;

    const proposal = node.propose(command, requestId);
    if (!proposal) return null; // not the leader
    this.drain(target);
    this.observe();

    let settled: KvResult | null = null;
    const answered = new Promise<KvResult>((resolve) => {
      this.pending.set(requestId, { node: target, resolve });
    }).then((result) => {
      settled = result;
    });

    await Promise.race([answered, this.sim.sleep(timeoutTicks * TICK_MS)]);
    if (settled) return settled;

    // Timed out. The entry may still commit later; the session table makes
    // the retry idempotent, so abandoning this attempt is safe.
    this.pending.delete(requestId);
    return null;
  }
}
