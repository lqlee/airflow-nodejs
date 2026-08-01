/**
 * Notify Provider — reusable notification operators.
 *
 * Ships LogNotifyOperator (always works, no external deps) and
 * stub operators for Slack/email (shell-based, show the pattern).
 */

/** @param {any} def */
function provider(def) { return def }

export default provider({
  name: 'notify-provider',
  version: '1.0.0',
  description: 'Notification operators — log, Slack (stub), email (stub)',
  connectionTypes: ['slack', 'smtp'],

  operators: {
    /**
     * LogNotifyOperator — write a notification to task logs (always works).
     * @param {{ message: string, level?: 'info'|'warn'|'error' }} opts
     */
    LogNotifyOperator: (opts = {}) => ({
      shell: {
        interpreter: 'sh',
        command: [
          `LEVEL="${opts.level ?? 'info'}"`,
          `MSG="${opts.message ?? 'Notification'}"`,
          `TS=$(date -u +"%Y-%m-%dT%H:%M:%SZ")`,
          `echo "[$TS] [$LEVEL] $MSG"`,
          `echo "  dag_id  : $DAG_ID"`,
          `echo "  run_id  : $RUN_ID"`,
          `echo "  task_id : $TASK_ID"`,
        ].join('\n'),
      },
    }),

    /**
     * SlackNotifyOperator — post a message to Slack via webhook (stub).
     * Replace SLACK_WEBHOOK_URL env var with your actual webhook.
     * @param {{ message: string, channel?: string }} opts
     */
    SlackNotifyOperator: (opts = {}) => ({
      shell: {
        interpreter: 'sh',
        command: [
          `WEBHOOK="${'${SLACK_WEBHOOK_URL}'}"`,
          `if [ -z "$WEBHOOK" ]; then`,
          `  echo "[slack-stub] SLACK_WEBHOOK_URL not set — logging only"`,
          `  echo "  channel : ${opts.channel ?? '#general'}"`,
          `  echo "  message : ${opts.message ?? 'Pipeline notification'}"`,
          `  echo "  dag_id  : $DAG_ID run_id: $RUN_ID"`,
          `else`,
          `  curl -s -X POST -H 'Content-type: application/json' \\`,
          `    --data '{"channel":"${opts.channel ?? '#general'}","text":"${opts.message ?? 'Pipeline notification'} (dag: '"'"'$DAG_ID'"'"', run: '"'"'$RUN_ID'"'"')"}' \\`,
          `    "$WEBHOOK"`,
          `  echo "Slack notification sent"`,
          `fi`,
        ].join('\n'),
      },
    }),
  },
})
