/**
 * Notify Provider — reusable notification operators.
 *
 * Operators:
 *   LogNotifyOperator    — always works, no external deps
 *   SlackNotifyOperator  — Slack incoming webhook
 *   SmsNotifyOperator    — SMS via Twilio REST API
 *   AwsSnsOperator       — SMS via AWS SNS (uses aws-cli)
 *
 * Set credentials via environment variables or the secrets backend —
 * never hardcode in DAG files.
 */

/** @param {any} def */
function provider(def) { return def }

export default provider({
  name: 'notify-provider',
  version: '3.0.0',
  description: 'Notification operators — log, Slack, SMS (Twilio), AWS SNS, Email with log attachment',
  connectionTypes: ['slack', 'smtp', 'twilio', 'aws-sns'],

  operators: {
    /**
     * LogNotifyOperator — write a notification to task logs (always works, no deps).
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
     * SlackNotifyOperator — post a message to a Slack incoming webhook.
     *
     * Required env var: SLACK_WEBHOOK_URL
     *   Set in docker-compose.yml or SECRETS_BACKEND=env:
     *     AIRFLOW_VAR_SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
     *
     * @param {{ message: string, channel?: string }} opts
     */
    SlackNotifyOperator: (opts = {}) => ({
      shell: {
        interpreter: 'sh',
        command: [
          `WEBHOOK="${'${SLACK_WEBHOOK_URL}'}"`,
          `if [ -z "$WEBHOOK" ]; then`,
          `  echo "[slack] SLACK_WEBHOOK_URL not set — logging only"`,
          `  echo "  channel : ${opts.channel ?? '#general'}"`,
          `  echo "  message : ${opts.message ?? 'Pipeline notification'}"`,
          `else`,
          `  curl -s -X POST -H 'Content-type: application/json' \\`,
          `    --data '{"channel":"${opts.channel ?? '#general'}","text":"${opts.message ?? 'Pipeline notification'} (dag: '"'"'$DAG_ID'"'"', run: '"'"'$RUN_ID'"'"')"}' \\`,
          `    "$WEBHOOK" && echo "[slack] notification sent"`,
          `fi`,
        ].join('\n'),
      },
    }),

    /**
     * SmsNotifyOperator — send an SMS via Twilio REST API.
     *
     * Required env vars (set via secrets backend or docker-compose):
     *   TWILIO_ACCOUNT_SID   — your Twilio Account SID (ACxxxxxxxx)
     *   TWILIO_AUTH_TOKEN    — your Twilio Auth Token
     *   TWILIO_FROM          — your Twilio phone number (+12025551234)
     *   TWILIO_TO            — recipient phone number (+12025555678)
     *                          or pass `to` in opts to override per-task
     *
     * Get credentials: https://console.twilio.com
     * Free trial: $15 credit, verified numbers only.
     *
     * @param {{ message: string, to?: string }} opts
     */
    SmsNotifyOperator: (opts = {}) => ({
      shell: {
        interpreter: 'sh',
        command: [
          `SID="${'${TWILIO_ACCOUNT_SID}'}"`,
          `TOKEN="${'${TWILIO_AUTH_TOKEN}'}"`,
          `FROM="${'${TWILIO_FROM}'}"`,
          `TO="${opts.to ?? '${TWILIO_TO}'}"`,
          `MSG="${opts.message ?? 'Pipeline $DAG_ID completed (run: $RUN_ID)'}"`,
          ``,
          `if [ -z "$SID" ] || [ -z "$TOKEN" ] || [ -z "$FROM" ] || [ -z "$TO" ]; then`,
          `  echo "[sms] Twilio credentials not set — logging only"`,
          `  echo "  to      : $TO"`,
          `  echo "  message : $MSG"`,
          `  exit 0`,
          `fi`,
          ``,
          `echo "[sms] Sending SMS to $TO via Twilio..."`,
          `RESPONSE=$(curl -s -X POST \\`,
          `  "https://api.twilio.com/2010-04-01/Accounts/$SID/Messages.json" \\`,
          `  --user "$SID:$TOKEN" \\`,
          `  --data-urlencode "To=$TO" \\`,
          `  --data-urlencode "From=$FROM" \\`,
          `  --data-urlencode "Body=$MSG")`,
          ``,
          `STATUS=$(echo "$RESPONSE" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('status','unknown'))" 2>/dev/null || echo "sent")`,
          `echo "[sms] Twilio status: $STATUS"`,
          ``,
          `# Non-2xx means error — check for 'error_code' in response`,
          `echo "$RESPONSE" | python3 -c "import json,sys; d=json.load(sys.stdin); exit(1 if d.get('error_code') else 0)" 2>/dev/null || (echo "[sms] Error: $RESPONSE" && exit 1)`,
        ].join('\n'),
      },
    }),

    /**
     * AwsSnsOperator — send an SMS or push notification via AWS SNS.
     *
     * Requires: aws-cli installed in the runtime image (or use Python variant).
     * IAM permission needed: sns:Publish
     *
     * Required env vars:
     *   AWS_DEFAULT_REGION   — e.g. us-east-1
     *   AWS_ACCESS_KEY_ID    — or use IAM role / IRSA in Kubernetes
     *   AWS_SECRET_ACCESS_KEY
     *
     * @param {{ message: string, topicArn?: string, phoneNumber?: string }} opts
     *   Provide either topicArn (SNS topic) or phoneNumber (direct SMS, E.164 format).
     */
    AwsSnsOperator: (opts = {}) => {
      const target = opts.topicArn
        ? `--topic-arn "${opts.topicArn}"`
        : `--phone-number "${opts.phoneNumber ?? '${SNS_PHONE_NUMBER}'}"`;
      return {
        shell: {
          interpreter: 'sh',
          command: [
            `MSG="${opts.message ?? 'Pipeline $DAG_ID completed (run: $RUN_ID)'}"`,
            `echo "[sns] Publishing to AWS SNS..."`,
            `aws sns publish \\`,
            `  ${target} \\`,
            `  --message "$MSG" \\`,
            `  --region "\${AWS_DEFAULT_REGION:-us-east-1}"`,
            `echo "[sns] Published"`,
          ].join('\n'),
        },
      };
    },

    /**
     * EmailNotifyOperator — send an email with task log attached as a gzipped file.
     *
     * - Finds the task log file (LOG_BACKEND=file, default path under LOG_DIR)
     * - Gzip-compresses it in memory
     * - Splits into ≤100 MB parts if the compressed file is larger
     * - Sends each part as a separate email with the attachment
     * - Falls back to plain-text email if no log file found
     *
     * Requires Python 3 (use `airflow-nodejs:python` image variant).
     *
     * Required env vars:
     *   SMTP_HOST      — SMTP server hostname (e.g. smtp.gmail.com)
     *   SMTP_PORT      — SMTP port (587 for STARTTLS, 465 for SSL)
     *   SMTP_USER      — sender email address
     *   SMTP_PASS      — sender password (use App Password for Gmail)
     *   NOTIFY_EMAIL   — recipient email address
     *
     * Optional env vars:
     *   SMTP_USE_SSL   — set to 'true' for port 465 (SSL); default STARTTLS
     *   LOG_DIR        — task log directory (default: ./logs)
     *   LOG_PART_MB    — max attachment size in MB (default: 100)
     *
     * @param {{ subject?: string, body?: string }} opts
     */
    EmailNotifyOperator: (opts = {}) => ({
      python: {
        interpreter: 'python3',
        code: [
          'import os, gzip, smtplib, math',
          'from email.mime.multipart import MIMEMultipart',
          'from email.mime.text import MIMEText',
          'from email.mime.application import MIMEApplication',
          'from email.mime.base import MIMEBase',
          'from email import encoders',
          'from pathlib import Path',
          '',
          '# ── Config ──────────────────────────────────────────────────────────',
          'dag_id    = os.environ["DAG_ID"]',
          'run_id    = os.environ["RUN_ID"]',
          'task_id   = os.environ["TASK_ID"]',
          'smtp_host = os.environ.get("SMTP_HOST", "smtp.gmail.com")',
          'smtp_port = int(os.environ.get("SMTP_PORT", "587"))',
          'smtp_user = os.environ["SMTP_USER"]',
          'smtp_pass = os.environ["SMTP_PASS"]',
          'to_addr   = os.environ["NOTIFY_EMAIL"]',
          'use_ssl   = os.environ.get("SMTP_USE_SSL", "").lower() == "true"',
          'log_dir   = os.environ.get("LOG_DIR", "logs")',
          'part_mb   = int(os.environ.get("LOG_PART_MB", "100"))',
          'part_bytes = part_mb * 1024 * 1024',
          '',
          `subject = "${opts.subject ?? 'Pipeline $dag_id completed — run $run_id'}".replace("$dag_id", dag_id).replace("$run_id", run_id[-8:])`,
          `body_text = "${opts.body ?? 'DAG: $dag_id\\nRun: $run_id\\nTask: $task_id\\n\\nSee attached log file.'}".replace("$dag_id", dag_id).replace("$run_id", run_id[-8:]).replace("$task_id", task_id)`,
          '',
          '# ── Find log file ───────────────────────────────────────────────────',
          'def safe_name(s):',
          '    import re',
          '    return re.sub(r"[^a-zA-Z0-9_-]", "_", s)[:64]',
          '',
          'log_path = Path(log_dir) / safe_name(dag_id) / safe_name(run_id) / f"{safe_name(task_id)}.log"',
          'print(f"[email] Looking for log file: {log_path}")',
          '',
          'log_exists = log_path.exists()',
          'if log_exists:',
          '    log_bytes = log_path.read_bytes()',
          '    gz_bytes  = gzip.compress(log_bytes, compresslevel=9)',
          '    print(f"[email] Log: {len(log_bytes):,} bytes → compressed: {len(gz_bytes):,} bytes")',
          'else:',
          '    print(f"[email] No log file found — sending plain text email")',
          '    gz_bytes = None',
          '',
          '# ── Split into parts if needed ──────────────────────────────────────',
          'if gz_bytes and len(gz_bytes) > part_bytes:',
          '    n_parts = math.ceil(len(gz_bytes) / part_bytes)',
          '    parts   = [gz_bytes[i*part_bytes:(i+1)*part_bytes] for i in range(n_parts)]',
          '    print(f"[email] Splitting into {n_parts} parts of ≤{part_mb} MB each")',
          'else:',
          '    parts = [gz_bytes] if gz_bytes else [None]',
          '    n_parts = 1',
          '',
          '# ── Send email(s) ───────────────────────────────────────────────────',
          'def make_smtp():',
          '    if use_ssl:',
          '        s = smtplib.SMTP_SSL(smtp_host, smtp_port)',
          '    else:',
          '        s = smtplib.SMTP(smtp_host, smtp_port)',
          '        s.starttls()',
          '    s.login(smtp_user, smtp_pass)',
          '    return s',
          '',
          'for idx, part_data in enumerate(parts):',
          '    msg = MIMEMultipart()',
          '    msg["From"]    = smtp_user',
          '    msg["To"]      = to_addr',
          '    part_label     = f" (part {idx+1}/{n_parts})" if n_parts > 1 else ""',
          '    msg["Subject"] = f"{subject}{part_label}"',
          '',
          '    body_part_note = f"\\n\\n[Attachment: part {idx+1} of {n_parts}]" if n_parts > 1 else ""',
          '    msg.attach(MIMEText(body_text + body_part_note, "plain"))',
          '',
          '    if part_data:',
          '        fname = f"{safe_name(dag_id)}_{safe_name(run_id)[-8:]}_{safe_name(task_id)}"',
          '        fname += f"_part{idx+1}.log.gz" if n_parts > 1 else ".log.gz"',
          '        attachment = MIMEApplication(part_data, _subtype="gzip")',
          '        attachment.add_header("Content-Disposition", "attachment", filename=fname)',
          '        msg.attach(attachment)',
          '        print(f"[email] Attaching {fname} ({len(part_data):,} bytes compressed)")',
          '',
          '    with make_smtp() as smtp:',
          '        smtp.send_message(msg)',
          '    print(f"[email] Sent part {idx+1}/{n_parts} to {to_addr}")',
          '',
          'print(f"[email] Done — {n_parts} email(s) sent to {to_addr}")',
        ].join('\n'),
        env: {
          // Credentials — set these via docker-compose or SECRETS_BACKEND=env
          // SMTP_HOST: 'smtp.gmail.com',
          // SMTP_PORT: '587',
          // SMTP_USER: 'your@gmail.com',
          // SMTP_PASS: 'your-app-password',
          // NOTIFY_EMAIL: 'team@example.com',
        },
      },
    }),
  },
})
