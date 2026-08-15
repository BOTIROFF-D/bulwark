/**
 * Raft's vocabulary, following the extended paper (Ongaro & Ousterhout, 2014).
 *
 * Log indices are 1-based throughout, as in the paper, because every rule in
 * §5 is stated in those terms and translating them to 0-based in your head is
 * how off-by-one bugs get written. Index 0 is the position before the first
 * entry and has term 0 by definition.
 */

export type NodeId = string;

/** What the replicated state machine understands. Opaque to Raft itself. */
export type Command =
  | { readonly kind: "put"; readonly key: string; readonly value: string }
  | { readonly kind: "get"; readonly key: string }
  | {
      readonly kind: "cas";
      readonly key: string;
      readonly expected: string | null;
      readonly value: string;
    }
  /**
   * The blank entry a new leader appends to its own term (§8).
   *
   * Not an optimisation. §5.4.2 forbids committing an entry from an earlier
   * term by counting replicas, so entries inherited from a previous leader
   * stay uncommitted until this leader commits something of its own. If
   * clients happen to go quiet at that moment, they stay uncommitted forever
   * — replicas that already applied them under the old leader sit permanently
   * ahead of replicas that never got the commit signal.
   *
   * The state machine ignores it; its only job is to carry the prefix.
   */
  | { readonly kind: "noop" };

export interface LogEntry {
  readonly term: number;
  readonly command: Command;
  /**
   * Identifies the client request. Clients retry after a timeout, and without
   * this a retry that reaches a new leader would be applied a second time —
   * which is a linearizability violation even though every Raft rule was
   * followed.
   */
  readonly requestId: string;
}

export type Role = "follower" | "candidate" | "leader";

/**
 * The state Raft requires on stable storage. §5: these must survive a crash,
 * and must be durable *before* the RPC that depended on them is answered.
 */
export interface PersistentState {
  currentTerm: number;
  votedFor: NodeId | null;
  log: LogEntry[];
}

export interface RequestVote {
  readonly type: "RequestVote";
  readonly from: NodeId;
  readonly to: NodeId;
  readonly term: number;
  readonly lastLogIndex: number;
  readonly lastLogTerm: number;
}

export interface RequestVoteReply {
  readonly type: "RequestVoteReply";
  readonly from: NodeId;
  readonly to: NodeId;
  readonly term: number;
  readonly voteGranted: boolean;
}

export interface AppendEntries {
  readonly type: "AppendEntries";
  readonly from: NodeId;
  readonly to: NodeId;
  readonly term: number;
  readonly prevLogIndex: number;
  readonly prevLogTerm: number;
  readonly entries: readonly LogEntry[];
  readonly leaderCommit: number;
}

export interface AppendEntriesReply {
  readonly type: "AppendEntriesReply";
  readonly from: NodeId;
  readonly to: NodeId;
  readonly term: number;
  readonly success: boolean;
  /**
   * Echoed from the request. The paper's reply carries neither this nor
   * matchIndex, which is fine over an ordered transport — but this simulator
   * reorders and duplicates messages on purpose, and without them a stale
   * reply moves nextIndex to a position the follower never confirmed. Real
   * implementations carry the same information for the same reason.
   */
  readonly prevLogIndex: number;
  /** Highest index the follower now has from this leader. Only when success. */
  readonly matchIndex: number;
}

export type Message = RequestVote | RequestVoteReply | AppendEntries | AppendEntriesReply;

/** An entry the state machine should apply, handed to the driver in order. */
export interface Applied {
  readonly index: number;
  readonly entry: LogEntry;
}

/**
 * Raft's safety rules, each one switchable.
 *
 * Every flag here guards a specific property in §5, and turning one off is
 * how the bug museum works: disable exactly one rule, run the same search,
 * and watch the property it protects break. Naming them individually also
 * documents which line of the paper is doing which job — something a correct
 * implementation usually hides.
 */
export interface SafetyRules {
  /** §5.4.1 — a candidate whose log is behind must not win an election. */
  readonly upToDateCheck: boolean;
  /** §5.4.2 — a leader only commits by counting replicas of a current-term entry. */
  readonly currentTermCommitOnly: boolean;
  /** §5.3 — AppendEntries is refused unless prevLogIndex/prevLogTerm match. */
  readonly prevLogCheck: boolean;
  /** §5.3 — a conflicting entry and everything after it is deleted. */
  readonly truncateConflicting: boolean;
  /** §5 — durable before replying, so a crash cannot un-cast a vote. */
  readonly persistBeforeReply: boolean;
  /** §5.1 — any message with a higher term makes the node a follower. */
  readonly stepDownOnHigherTerm: boolean;
  /**
   * §8 — a new leader appends a blank entry for its own term.
   *
   * Listed among the safety rules because leaving it out has a consequence
   * the paper does not spell out: §5.4.2 forbids committing inherited entries
   * by counting replicas, so without an entry of the leader's own term they
   * stay uncommitted. If clients fall silent there, replicas that applied
   * them under the previous leader stay permanently ahead of the ones that
   * did not — every safety property intact, the cluster simply never agrees
   * again.
   */
  readonly leaderNoop: boolean;
}

export const CORRECT_RULES: SafetyRules = {
  upToDateCheck: true,
  currentTermCommitOnly: true,
  prevLogCheck: true,
  truncateConflicting: true,
  persistBeforeReply: true,
  stepDownOnHigherTerm: true,
  leaderNoop: true,
};

export interface RaftOptions {
  readonly id: NodeId;
  readonly peers: readonly NodeId[];
  /** Election timeout is drawn from this inclusive tick range, per attempt. */
  readonly electionTimeoutTicks: readonly [number, number];
  readonly heartbeatTicks: number;
  /** Seeded, so a whole cluster's timing is reproducible. */
  readonly random: () => number;
  readonly rules?: SafetyRules;
  /** Durable state recovered from a previous life, if this is a restart. */
  readonly restoreFrom?: PersistentState;
}
