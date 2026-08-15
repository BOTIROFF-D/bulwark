/**
 * Is this history linearizable?
 *
 * The safety properties in ./invariants.ts say Raft followed its own rules.
 * This says something a client can actually feel: that every operation
 * appears to take effect instantaneously at some point between when it was
 * issued and when it returned, and that those points can be arranged into a
 * single sequential order consistent with the real-time order of
 * non-overlapping operations.
 *
 * A system can satisfy all five Raft properties and still fail this — apply a
 * retried command twice and consensus is intact while the client saw
 * something impossible. That is why both checks are here.
 *
 * The algorithm is Wing and Gong's: repeatedly pick an operation that is
 * allowed to go first, apply it to a sequential model, and recurse; on a dead
 * end, undo and try the next. Deciding linearizability is NP-complete in
 * general, so two things keep it usable — memoising on (remaining operations,
 * model state), because many different orders reach the same place, and a
 * hard budget on explored states, after which the answer is honestly
 * "inconclusive" rather than a guess.
 */

import { sequentialApply, type KvResult } from "../sim/state-machine.js";
import type { Command } from "../raft/types.js";
import type { HistoryEvent } from "../sim/cluster.js";

export interface Operation {
  readonly id: number;
  readonly process: number;
  readonly command: Command;
  readonly result: KvResult;
  /** When the client issued it. */
  readonly invokedAt: number;
  /** When the client learned the answer. */
  readonly returnedAt: number;
}

export type LinearizabilityReport =
  | { readonly status: "linearizable"; readonly order: readonly number[]; readonly explored: number }
  | { readonly status: "not-linearizable"; readonly reason: string; readonly explored: number }
  | { readonly status: "inconclusive"; readonly reason: string; readonly explored: number };

/**
 * Pair up invocations and responses.
 *
 * Every operation in these scenarios completes: clients retry until they get
 * a definite answer, and the state machine's session table makes that safe.
 * An unmatched invocation therefore means a client gave up, which is a
 * liveness failure worth reporting on its own rather than quietly modelling
 * as "might have happened".
 */
export function operationsFrom(history: readonly HistoryEvent[]): Operation[] {
  const open = new Map<number, HistoryEvent>();
  const operations: Operation[] = [];
  let id = 0;

  for (const event of history) {
    if (event.type === "invoke") {
      open.set(event.process, event);
      continue;
    }
    const invoke = open.get(event.process);
    if (!invoke) throw new Error(`response from process ${event.process} with no invocation`);
    open.delete(event.process);
    if (event.type === "fail" || !event.result) {
      throw new Error(`process ${event.process} never got an answer for ${event.command.kind}`);
    }
    operations.push({
      id: id++,
      process: event.process,
      command: invoke.command,
      result: event.result,
      invokedAt: invoke.time,
      returnedAt: event.time,
    });
  }

  if (open.size > 0) {
    const stuck = [...open.keys()].join(", ");
    throw new Error(`process(es) ${stuck} left an operation outstanding`);
  }
  return operations;
}

function sameResult(a: KvResult, b: KvResult): boolean {
  return a.ok === b.ok && a.value === b.value;
}

function stateKey(state: Map<string, string>): string {
  return [...state.entries()]
    .sort(([a], [b]) => (a < b ? -1 : 1))
    .map(([k, v]) => `${k}=${v}`)
    .join(",");
}

export interface CheckOptions {
  /** States to explore before giving up. Default 2,000,000. */
  readonly budget?: number;
}

export function checkLinearizable(
  operations: readonly Operation[],
  options: CheckOptions = {},
): LinearizabilityReport {
  const budget = options.budget ?? 2_000_000;
  let explored = 0;

  /**
   * Orders that led nowhere, keyed by what is left to do and where the model
   * got to. Two different prefixes that leave the same work and the same
   * state are interchangeable, and without collapsing them the search
   * re-derives the same dead ends factorially many times.
   */
  const dead = new Set<string>();
  const order: number[] = [];

  const search = (remaining: readonly Operation[], state: Map<string, string>): boolean => {
    if (remaining.length === 0) return true;
    if (explored++ > budget) return false;

    const key = `${remaining.map((o) => o.id).join(".")}|${stateKey(state)}`;
    if (dead.has(key)) return false;

    // An operation may go first only if nothing still pending was already
    // known to have finished before it started. That is where real-time
    // order enters the algorithm — everything else is free to permute.
    let earliestReturn = Infinity;
    for (const op of remaining) earliestReturn = Math.min(earliestReturn, op.returnedAt);

    for (const candidate of remaining) {
      if (candidate.invokedAt > earliestReturn) continue;

      const next = new Map(state);
      const produced = sequentialApply(next, candidate.command);
      if (!sameResult(produced, candidate.result)) continue;

      order.push(candidate.id);
      if (search(remaining.filter((op) => op !== candidate), next)) return true;
      order.pop();
    }

    dead.add(key);
    return false;
  };

  const linearizable = search(operations, new Map());

  if (linearizable) return { status: "linearizable", order: [...order], explored };
  if (explored > budget) {
    return {
      status: "inconclusive",
      reason: `gave up after exploring ${explored} states; the history may still be linearizable`,
      explored,
    };
  }
  return {
    status: "not-linearizable",
    reason: describeFailure(operations),
    explored,
  };
}

/**
 * Say something useful about *why*, since "not linearizable" on its own sends
 * people back to a wall of history. The most common shape by far is a read
 * that returned a value nothing ever wrote, so name that outright; otherwise
 * hand back a compact rendering of the history to read.
 */
function describeFailure(operations: readonly Operation[]): string {
  const written = new Set<string>([...operations.flatMap(valuesWritten)]);
  for (const op of operations) {
    if (op.command.kind !== "get") continue;
    const observed = op.result.value;
    if (observed !== null && !written.has(`${op.command.key}=${observed}`)) {
      return (
        `process ${op.process} read ${op.command.key}=${observed}, ` +
        `which no operation ever wrote`
      );
    }
  }
  return `no sequential order explains this history:\n${render(operations)}`;
}

function describeCommand(command: Command): string {
  switch (command.kind) {
    case "get":
      return `get(${command.key})`;
    case "put":
      return `put(${command.key}, ${command.value})`;
    case "cas":
      return `cas(${command.key}, ${command.expected} → ${command.value})`;
    case "noop":
      // Never reaches a client history — leaders append it, clients do not.
      return "noop";
  }
}

function valuesWritten(op: Operation): string[] {
  if (op.command.kind === "put") return [`${op.command.key}=${op.command.value}`];
  if (op.command.kind === "cas" && op.result.ok) return [`${op.command.key}=${op.command.value}`];
  return [];
}

function render(operations: readonly Operation[]): string {
  return [...operations]
    .sort((a, b) => a.invokedAt - b.invokedAt)
    .map((op) => {
      const window = `[${op.invokedAt}..${op.returnedAt}]`;
      const call = describeCommand(op.command);
      const answer = op.result.ok ? String(op.result.value) : `rejected (saw ${op.result.value})`;
      return `  p${op.process} ${window.padEnd(14)} ${call.padEnd(26)} → ${answer}`;
    })
    .join("\n");
}
