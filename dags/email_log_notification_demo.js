/**
 * Email Notification with Log Attachment Demo.
 *
 * Sends an email when the pipeline completes, attaching the task log
 * as a gzipped file. Logs are compressed with gzip and split at 100 MB
 * per attachment if the compressed log is very large.
 *
 * Requirements:
 *   - Python 3 runtime (use `airflow-nodejs:python` image variant)
 *   - LOG_BACKEND=file (default) — log file must exist on disk
 *
 * Setup env vars (in docker-compose.yml or SECRETS_BACKEND=env):
 *   SMTP_HOST=smtp.gmail.com
 *   SMTP_PORT=587
 *   SMTP_USER=your@gmail.com
 *   SMTP_PASS=your-app-password        ← Gmail: use App Password, not account password
 *   NOTIFY_EMAIL=recipient@example.com
 *
 * For Gmail App Password:
 *   1. Enable 2-Factor Authentication on your Google account
 *   2. Go to: Google Account → Security → App passwords
 *   3. Generate a password for "Mail"
 *
 * Trigger:
 *   POST /dags/email_log_notification_demo/trigger
 *   body: { "conf": { "rows": 100 } }
 */

import { getOperator } from 'airflow-nodejs/providers'

const EmailNotifyOperator = getOperator('notify-provider', 'EmailNotifyOperator')
const LogNotifyOperator   = getOperator('notify-provider', 'LogNotifyOperator')
const noop = () => ({ shell: { interpreter: 'sh', command: 'echo "operator not loaded"' } })

export default {
  id: 'email_log_notification_demo',
  schedule: null,

  tasks: {

    // ── Main pipeline work ────────────────────────────────────────────────────
    process: {
      run: async (ctx) => {
        ctx.log.info('Starting data processing pipeline')
        ctx.log.info({ rows: ctx.conf.rows ?? 0, dag: ctx.dagId })

        // Simulate processing
        for (let i = 0; i < 5; i++) {
          ctx.log.info(`Processing batch ${i + 1}/5`)
        }

        ctx.log.info('Pipeline complete')
        return { processed: ctx.conf.rows ?? 0, status: 'ok' }
      }
    },

    // ── Send email with log attached (runs after process, success or failure) ─
    // triggerRule: 'all_done' ensures email fires whether pipeline succeeded or failed
    email_report: {
      dependsOn: ['process'],
      triggerRule: 'all_done',
      ...(EmailNotifyOperator ?? noop)({
        subject: 'Pipeline $dag_id report — run $run_id',
        body: [
          'Pipeline: $dag_id',
          'Run ID:   $run_id',
          'Task:     $task_id',
          '',
          'The task log is attached as a gzip-compressed file.',
          'Decompress: gunzip <filename>.log.gz',
          '',
          'Log size is split at 100 MB per attachment if needed.',
        ].join('\n'),
      }),
    },

    // ── Fallback: log-only notify if Python is not available ─────────────────
    log_fallback: {
      dependsOn: ['process'],
      triggerRule: 'all_done',
      ...(LogNotifyOperator ?? noop)({
        message: 'Pipeline $dag_id done — check email for log attachment',
        level: 'info',
      }),
    },

  }
}
