import type { Plugin } from '@opencode-ai/plugin';

declare const process: {
  env: Record<string, string | undefined>;
};

/**
 * Beads integration plugin for OpenCode.
 *
 * Replicates the Claude Code hooks:
 *   - SessionStart  → `bd prime`  (run at session start)
 *   - PreCompact    → `bd prime`  (run before/during compaction)
 *
 * `bd prime` outputs Beads workflow context (open issues, ready work, etc.)
 * that gets injected into the LLM's system context so it can track tasks
 * across compactions and new sessions.
 */
export const BeadsPlugin: Plugin = async ({ $, client }) => {
  const debug = process.env.OPENCODE_BEADS_DEBUG === '1';

  const logDebug = async (message: string, extra?: Record<string, unknown>) => {
    if (!debug) return;
    try {
      await client.app.log({
        body: {
          service: 'beads-plugin',
          level: 'debug',
          message,
          extra,
        },
      });
    } catch {
      // If structured logging fails (older opencode, etc.), don't break chat.
    }
  };

  // Cache `bd prime` so we don't shell out on every hook.
  let cachedPrime = '';
  let lastPrimeAtMs = 0;

  const runBdPrime = async (): Promise<string> => {
    try {
      const result = await $`bd prime`.text();
      return result.trim();
    } catch {
      // bd not installed or not a beads project — silently skip
      return '';
    }
  };

  const refreshPrimeIfStale = async (): Promise<void> => {
    const now = Date.now();
    // Keep this low to stay reasonably up to date, but high enough to avoid
    // spamming shell executions during chat streaming.
    const maxAgeMs = 30_000;
    if (cachedPrime && now - lastPrimeAtMs < maxAgeMs) return;

    const next = await runBdPrime();
    if (next) {
      cachedPrime = next;
      lastPrimeAtMs = now;
      await logDebug('bd prime refreshed', { chars: cachedPrime.length });
    }
  };

  return {
    // Equivalent to Claude Code's SessionStart hook
    'session.created': async () => {
      cachedPrime = await runBdPrime();
      lastPrimeAtMs = Date.now();
      await logDebug('session.created: bd prime captured', {
        chars: cachedPrime.length,
      });
    },

    // Inject bd context into the system prompt so it is present in the
    // model context during normal chat (not only during compaction).
    'experimental.chat.system.transform': async (_input, output) => {
      await refreshPrimeIfStale();
      if (cachedPrime) {
        output.system.push(cachedPrime);
        await logDebug('system.transform: injected bd prime', {
          chars: cachedPrime.length,
        });
      } else {
        await logDebug('system.transform: no bd prime to inject');
      }
    },

    // Equivalent to Claude Code's PreCompact hook —
    // injects bd prime output into the compaction context so
    // Beads state survives context compaction
    'experimental.session.compacting': async (_input, output) => {
      await refreshPrimeIfStale();
      if (cachedPrime) {
        output.context.push(cachedPrime);
        await logDebug('session.compacting: injected bd prime', {
          chars: cachedPrime.length,
        });
      } else {
        await logDebug('session.compacting: no bd prime to inject');
      }
    },

    // Also run after compaction completes (belt-and-suspenders)
    'session.compacted': async () => {
      cachedPrime = await runBdPrime();
      lastPrimeAtMs = Date.now();
      await logDebug('session.compacted: bd prime captured', {
        chars: cachedPrime.length,
      });
    },
  };
};
