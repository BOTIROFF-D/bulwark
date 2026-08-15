/**
 * The replicated state machine: a key-value store with client sessions.
 *
 * The session table is not decoration. Clients retry after a timeout, and a
 * retry that reaches a different leader gets proposed a second time — so the
 * same command can legitimately appear twice in a committed log. Raft's own
 * rules permit that; what stops it from being observable is applying each
 * request id exactly once and returning the remembered answer (§6.3).
 *
 * Without it, `cas` and any non-idempotent command would double-apply and the
 * history would fail linearizability while every consensus rule held — a
 * failure of the system, not of Raft.
 */

import type { Command, LogEntry } from "../raft/types.js";

export interface KvResult {
  /** For get and put, the value; for a failed cas, the value that was there. */
  readonly value: string | null;
  /** False only for a cas whose expected value did not match. */
  readonly ok: boolean;
}

export class KeyValue {
  private readonly data = new Map<string, string>();
  private readonly sessions = new Map<string, KvResult>();

  apply(entry: LogEntry): KvResult {
    const remembered = this.sessions.get(entry.requestId);
    if (remembered) return remembered;

    const result = this.execute(entry.command);
    this.sessions.set(entry.requestId, result);
    return result;
  }

  private execute(command: Command): KvResult {
    switch (command.kind) {
      case "noop":
        return { value: null, ok: true };
      case "put":
        this.data.set(command.key, command.value);
        return { value: command.value, ok: true };
      case "get":
        return { value: this.data.get(command.key) ?? null, ok: true };
      case "cas": {
        const current = this.data.get(command.key) ?? null;
        if (current !== command.expected) return { value: current, ok: false };
        this.data.set(command.key, command.value);
        return { value: command.value, ok: true };
      }
    }
  }

  /** Used to compare replicas once a scenario has quiesced. */
  snapshot(): Record<string, string> {
    return Object.fromEntries([...this.data.entries()].sort(([a], [b]) => (a < b ? -1 : 1)));
  }
}

/** The same execution, without any Raft — the oracle the history is judged against. */
export function sequentialApply(state: Map<string, string>, command: Command): KvResult {
  switch (command.kind) {
    case "noop":
      return { value: null, ok: true };
    case "put":
      state.set(command.key, command.value);
      return { value: command.value, ok: true };
    case "get":
      return { value: state.get(command.key) ?? null, ok: true };
    case "cas": {
      const current = state.get(command.key) ?? null;
      if (current !== command.expected) return { value: current, ok: false };
      state.set(command.key, command.value);
      return { value: command.value, ok: true };
    }
  }
}
