/**
 * Raft, as a state machine with no I/O in it.
 *
 * The node never touches a clock, a socket or a disk. It is driven: `tick`
 * advances its timers, `receive` feeds it a message, `propose` gives it a
 * client command — and everything it wants to do comes back out through
 * `takeMessages` and `takeApplied`. The driver decides when those happen.
 *
 * That is not a stylistic preference. A consensus implementation that calls
 * `setTimeout` and `socket.write` internally can only be tested by running it
 * for real and hoping the interesting interleavings show up; one shaped like
 * this can be replayed exactly, which is the difference between "we tested it"
 * and "here is the seed". etcd's raft package is built the same way, for the
 * same reason.
 *
 * Section references are to the extended Raft paper (Ongaro & Ousterhout,
 * 2014), and the safety rules in §5 are individually switchable — see
 * SafetyRules in ./types.ts and ../../museum for why.
 */

import {
  CORRECT_RULES,
  type Applied,
  type AppendEntries,
  type AppendEntriesReply,
  type Command,
  type LogEntry,
  type Message,
  type NodeId,
  type PersistentState,
  type RaftOptions,
  type RequestVote,
  type RequestVoteReply,
  type Role,
  type SafetyRules,
} from "./types.js";

export class RaftNode {
  readonly id: NodeId;
  private readonly peers: readonly NodeId[];
  private readonly rules: SafetyRules;
  private readonly electionTimeoutTicks: readonly [number, number];
  private readonly heartbeatTicks: number;
  private readonly random: () => number;

  // ── persistent state (§5, must be durable before replying) ───────────────
  private currentTerm = 0;
  private votedFor: NodeId | null = null;
  private log: LogEntry[] = [];

  // ── volatile state, lost on crash ────────────────────────────────────────
  private role: Role = "follower";
  private commitIndex = 0;
  private lastApplied = 0;
  private leader: NodeId | null = null;
  private votesGranted = new Set<NodeId>();
  private readonly nextIndex = new Map<NodeId, number>();
  private readonly matchIndex = new Map<NodeId, number>();

  private electionElapsed = 0;
  private electionTimeout = 0;
  private heartbeatElapsed = 0;

  private outbox: Message[] = [];
  private appliedQueue: Applied[] = [];

  /**
   * What a crash would leave behind. The driver reads this when it kills a
   * node and feeds it back on restart, which is the only way the
   * persist-before-reply rule can be observed to matter.
   */
  private durable: PersistentState;
  private dirty = false;

  constructor(options: RaftOptions) {
    this.id = options.id;
    this.peers = options.peers.filter((p) => p !== options.id);
    this.rules = options.rules ?? CORRECT_RULES;
    this.electionTimeoutTicks = options.electionTimeoutTicks;
    this.heartbeatTicks = options.heartbeatTicks;
    this.random = options.random;

    if (options.restoreFrom) {
      this.currentTerm = options.restoreFrom.currentTerm;
      this.votedFor = options.restoreFrom.votedFor;
      this.log = [...options.restoreFrom.log];
    }
    this.durable = this.snapshot();
    this.resetElectionTimer();
  }

  // ── log helpers, 1-based like the paper ──────────────────────────────────

  private lastIndex(): number {
    return this.log.length;
  }

  /** Term at a log index; index 0 is the position before the log, term 0. */
  private termAt(index: number): number {
    if (index <= 0) return 0;
    return this.log[index - 1]?.term ?? 0;
  }

  private entryAt(index: number): LogEntry | undefined {
    return this.log[index - 1];
  }

  private majority(): number {
    return Math.floor((this.peers.length + 1) / 2) + 1;
  }

  // ── durability ───────────────────────────────────────────────────────────

  private snapshot(): PersistentState {
    return { currentTerm: this.currentTerm, votedFor: this.votedFor, log: [...this.log] };
  }

  /**
   * Persistent state changed. With the rule on, it reaches stable storage
   * now — before any reply that depended on it goes out. With the rule off,
   * the write is deferred to the next tick, which is exactly the window a
   * real implementation opens when it forgets to await its fsync: reply, then
   * crash, and the vote it promised never happened.
   */
  private markPersistent(): void {
    if (this.rules.persistBeforeReply) this.durable = this.snapshot();
    else this.dirty = true;
  }

  /** The state that would survive a crash right now. */
  get durableState(): PersistentState {
    return { ...this.durable, log: [...this.durable.log] };
  }

  // ── role transitions ─────────────────────────────────────────────────────

  private resetElectionTimer(): void {
    const [min, max] = this.electionTimeoutTicks;
    this.electionElapsed = 0;
    // Randomised per attempt (§5.2): identical timeouts across a cluster
    // produce split votes forever, and the paper's fix is jitter, not cleverness.
    this.electionTimeout = min + Math.floor(this.random() * (max - min + 1));
  }

  private becomeFollower(term: number): void {
    if (term > this.currentTerm) {
      this.currentTerm = term;
      this.votedFor = null;
      this.markPersistent();
    }
    this.role = "follower";
    this.leader = null;
    this.votesGranted.clear();
    this.resetElectionTimer();
  }

  private becomeCandidate(): void {
    this.currentTerm += 1;
    this.votedFor = this.id;
    // The vote for self must be durable before soliciting others: a crash
    // here that forgot it could let this node vote again in the same term.
    this.markPersistent();
    this.role = "candidate";
    this.leader = null;
    this.votesGranted = new Set([this.id]);
    this.resetElectionTimer();

    const lastLogIndex = this.lastIndex();
    const lastLogTerm = this.termAt(lastLogIndex);
    for (const peer of this.peers) {
      this.send({
        type: "RequestVote",
        from: this.id,
        to: peer,
        term: this.currentTerm,
        lastLogIndex,
        lastLogTerm,
      });
    }

    // A single-node cluster wins its own election immediately.
    if (this.votesGranted.size >= this.majority()) this.becomeLeader();
  }

  private becomeLeader(): void {
    this.role = "leader";
    this.leader = this.id;
    this.votesGranted.clear();

    // §8 — append a blank entry for the new term before anything else.
    //
    // §5.4.2 forbids committing an inherited entry by counting replicas, so
    // without something of this leader's own term those entries can never be
    // declared committed. If clients go quiet at that moment they never are:
    // replicas that applied them under the previous leader stay permanently
    // ahead of the ones that never received the commit signal. The logs agree
    // and every safety property holds — the cluster simply stops converging.
    if (this.rules.leaderNoop) {
      this.log.push({
        term: this.currentTerm,
        command: { kind: "noop" },
        requestId: `noop:${this.id}:${this.currentTerm}`,
      });
      this.markPersistent();
    }

    for (const peer of this.peers) {
      this.nextIndex.set(peer, this.lastIndex() + 1);
      this.matchIndex.set(peer, 0);
    }
    this.heartbeatElapsed = 0;
    this.broadcastAppend();
  }

  // ── driving ──────────────────────────────────────────────────────────────

  /** Advance one tick of this node's local time. */
  tick(): void {
    if (this.dirty) {
      this.durable = this.snapshot();
      this.dirty = false;
    }

    if (this.role === "leader") {
      this.heartbeatElapsed += 1;
      if (this.heartbeatElapsed >= this.heartbeatTicks) {
        this.heartbeatElapsed = 0;
        this.broadcastAppend();
      }
    } else {
      this.electionElapsed += 1;
      if (this.electionElapsed >= this.electionTimeout) this.becomeCandidate();
    }

    this.applyCommitted();
  }

  receive(message: Message): void {
    // §5.1 — any message carrying a newer term makes this node a follower,
    // whatever it was doing. Terms are Raft's logical clock; ignoring one is
    // how two leaders end up alive at once.
    if (this.rules.stepDownOnHigherTerm && message.term > this.currentTerm) {
      this.becomeFollower(message.term);
    }

    switch (message.type) {
      case "RequestVote":
        this.handleRequestVote(message);
        break;
      case "RequestVoteReply":
        this.handleRequestVoteReply(message);
        break;
      case "AppendEntries":
        this.handleAppendEntries(message);
        break;
      case "AppendEntriesReply":
        this.handleAppendEntriesReply(message);
        break;
    }

    this.applyCommitted();
  }

  /** Append a client command. Returns null when this node is not the leader. */
  propose(command: Command, requestId: string): { index: number; term: number } | null {
    if (this.role !== "leader") return null;
    this.log.push({ term: this.currentTerm, command, requestId });
    this.markPersistent();
    this.broadcastAppend();
    return { index: this.lastIndex(), term: this.currentTerm };
  }

  // ── RPC handlers ─────────────────────────────────────────────────────────

  private handleRequestVote(request: RequestVote): void {
    let granted = false;

    if (request.term === this.currentTerm) {
      const free = this.votedFor === null || this.votedFor === request.from;
      // §5.4.1 — the election restriction. A candidate missing committed
      // entries must never win, or those entries can be overwritten later.
      const upToDate =
        !this.rules.upToDateCheck || this.isUpToDate(request.lastLogTerm, request.lastLogIndex);
      if (free && upToDate) {
        granted = true;
        this.votedFor = request.from;
        this.markPersistent();
        this.resetElectionTimer();
      }
    }

    this.send({
      type: "RequestVoteReply",
      from: this.id,
      to: request.from,
      term: this.currentTerm,
      voteGranted: granted,
    });
  }

  /** §5.4.1 — "at least as up to date as": compare last term, then length. */
  private isUpToDate(lastLogTerm: number, lastLogIndex: number): boolean {
    const myLastTerm = this.termAt(this.lastIndex());
    if (lastLogTerm !== myLastTerm) return lastLogTerm > myLastTerm;
    return lastLogIndex >= this.lastIndex();
  }

  private handleRequestVoteReply(reply: RequestVoteReply): void {
    // A reply from an election this node has already left tells it nothing.
    if (this.role !== "candidate" || reply.term !== this.currentTerm) return;
    if (!reply.voteGranted) return;
    this.votesGranted.add(reply.from);
    if (this.votesGranted.size >= this.majority()) this.becomeLeader();
  }

  private handleAppendEntries(request: AppendEntries): void {
    if (request.term < this.currentTerm) {
      this.replyAppend(request, false, request.prevLogIndex);
      return;
    }

    // A leader exists for this term, so a candidate gives up and everyone
    // restarts their election clock.
    this.role = "follower";
    this.leader = request.from;
    this.resetElectionTimer();

    // §5.3 — the log matching property is maintained by this one check.
    const prevMatches =
      request.prevLogIndex <= this.lastIndex() &&
      this.termAt(request.prevLogIndex) === request.prevLogTerm;
    if (this.rules.prevLogCheck && !prevMatches) {
      this.replyAppend(request, false, request.prevLogIndex);
      return;
    }

    for (let i = 0; i < request.entries.length; i++) {
      const index = request.prevLogIndex + 1 + i;
      const incoming = request.entries[i];
      if (!incoming) continue;

      if (index > this.lastIndex()) {
        this.log.push(incoming);
      } else if (this.termAt(index) !== incoming.term) {
        if (this.rules.truncateConflicting) {
          // §5.3 — delete the conflicting entry and everything after it.
          this.log.length = index - 1;
          this.log.push(incoming);
        }
        // Without the rule the stale entry is left in place, and this
        // follower's log silently diverges from the leader's.
      }
      // Same index, same term: already have it, by the log matching property.
    }
    this.markPersistent();

    const lastNewIndex = request.prevLogIndex + request.entries.length;
    if (request.leaderCommit > this.commitIndex) {
      this.commitIndex = Math.min(request.leaderCommit, lastNewIndex);
    }

    this.replyAppend(request, true, lastNewIndex);
  }

  private replyAppend(request: AppendEntries, success: boolean, matchIndex: number): void {
    this.send({
      type: "AppendEntriesReply",
      from: this.id,
      to: request.from,
      term: this.currentTerm,
      success,
      prevLogIndex: request.prevLogIndex,
      matchIndex,
    });
  }

  private handleAppendEntriesReply(reply: AppendEntriesReply): void {
    if (this.role !== "leader" || reply.term !== this.currentTerm) return;

    if (reply.success) {
      // Never move backwards: replies arrive out of order in this simulator,
      // and an old one must not undo a newer confirmation.
      if (reply.matchIndex > (this.matchIndex.get(reply.from) ?? 0)) {
        this.matchIndex.set(reply.from, reply.matchIndex);
        this.nextIndex.set(reply.from, reply.matchIndex + 1);
      }
      this.advanceCommit();
      return;
    }

    // Back off one index and retry — but only for the attempt that is still
    // outstanding. A duplicated or delayed rejection would otherwise walk
    // nextIndex down past where the follower actually diverges.
    const next = this.nextIndex.get(reply.from) ?? 1;
    if (reply.prevLogIndex + 1 === next) {
      this.nextIndex.set(reply.from, Math.max(1, next - 1));
      this.sendAppend(reply.from);
    }
  }

  // ── leader duties ────────────────────────────────────────────────────────

  private broadcastAppend(): void {
    for (const peer of this.peers) this.sendAppend(peer);
  }

  private sendAppend(peer: NodeId): void {
    const next = this.nextIndex.get(peer) ?? this.lastIndex() + 1;
    const prevLogIndex = next - 1;
    this.send({
      type: "AppendEntries",
      from: this.id,
      to: peer,
      term: this.currentTerm,
      prevLogIndex,
      prevLogTerm: this.termAt(prevLogIndex),
      entries: this.log.slice(prevLogIndex),
      leaderCommit: this.commitIndex,
    });
  }

  /**
   * §5.3/§5.4.2 — advance the commit index to the highest N replicated on a
   * majority.
   *
   * The term check is the entire content of Figure 8: counting replicas of an
   * *old* term's entry is not enough to call it committed, because a later
   * leader with a different log can still overwrite it. A leader may only
   * commit by replicating an entry from its own term — which carries the
   * earlier ones with it.
   */
  private advanceCommit(): void {
    for (let n = this.lastIndex(); n > this.commitIndex; n--) {
      if (this.rules.currentTermCommitOnly && this.termAt(n) !== this.currentTerm) continue;
      let replicas = 1; // the leader itself
      for (const peer of this.peers) {
        if ((this.matchIndex.get(peer) ?? 0) >= n) replicas += 1;
      }
      if (replicas >= this.majority()) {
        this.commitIndex = n;
        return;
      }
    }
  }

  private applyCommitted(): void {
    while (this.lastApplied < this.commitIndex) {
      this.lastApplied += 1;
      const entry = this.entryAt(this.lastApplied);
      if (entry) this.appliedQueue.push({ index: this.lastApplied, entry });
    }
  }

  private send(message: Message): void {
    this.outbox.push(message);
  }

  // ── outputs ──────────────────────────────────────────────────────────────

  takeMessages(): Message[] {
    const messages = this.outbox;
    this.outbox = [];
    return messages;
  }

  takeApplied(): Applied[] {
    const applied = this.appliedQueue;
    this.appliedQueue = [];
    return applied;
  }

  // ── introspection, for the invariant checker ─────────────────────────────

  get currentRole(): Role {
    return this.role;
  }

  get term(): number {
    return this.currentTerm;
  }

  get leaderId(): NodeId | null {
    return this.leader;
  }

  get entries(): readonly LogEntry[] {
    return this.log;
  }

  get committed(): number {
    return this.commitIndex;
  }

  get applied(): number {
    return this.lastApplied;
  }

  /**
   * True when persistent state has changed but has not reached stable
   * storage yet — which only ever happens with the persist-before-reply rule
   * switched off.
   *
   * Exposed for the harness. The window this opens is one tick wide, and a
   * random search almost never crashes a node inside it; the museum uses this
   * to crash at exactly the wrong moment and show what the rule is worth.
   */
  get hasUnpersistedState(): boolean {
    return this.dirty;
  }

  /**
   * Start an election now, ignoring the timer.
   *
   * A test affordance, and a standard one — etcd's raft exposes the same
   * thing. Scenarios like Figure 8 depend on a specific node winning a
   * specific term, and waiting for randomised timeouts to produce that by
   * chance is not a test, it is a lottery.
   */
  campaign(): void {
    this.becomeCandidate();
  }
}
