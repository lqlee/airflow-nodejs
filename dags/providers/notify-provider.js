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
  version: '2.0.0',
  description: 'Notification operators — log, Slack, SMS (Twilio), AWS SNS',
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
  },
})
