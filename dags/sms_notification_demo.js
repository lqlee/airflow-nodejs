/**
 * SMS Notification Demo — send a text message when a pipeline completes.
 *
 * Two approaches shown:
 *   A. SmsNotifyOperator (Twilio) — via notify-provider
 *   B. AwsSnsOperator (AWS SNS)   — via notify-provider
 *
 * Setup (Twilio):
 *   1. Sign up at https://console.twilio.com (free $15 trial)
 *   2. Get Account SID, Auth Token, and a Twilio phone number
 *   3. Set env vars in docker-compose.yml or SECRETS_BACKEND=env:
 *        TWILIO_ACCOUNT_SID=ACxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
 *        TWILIO_AUTH_TOKEN=your_auth_token
 *        TWILIO_FROM=+12025551234   (your Twilio number)
 *        TWILIO_TO=+12025555678     (recipient)
 *
 * Trigger:
 *   POST /dags/sms_notification_demo/trigger   body: {}
 */

import { getOperator } from 'airflow-nodejs/providers'

const SmsNotifyOperator = getOperator('notify-provider', 'SmsNotifyOperator')
const AwsSnsOperator    = getOperator('notify-provider', 'AwsSnsOperator')
const LogNotifyOperator = getOperator('notify-provider', 'LogNotifyOperator')

const noop = () => ({ shell: { interpreter: 'sh', command: 'echo "provider not loaded"' } })

export default {
  id: 'sms_notification_demo',
  schedule: null,

  tasks: {

    // ── Main work ────────────────────────────────────────────────────────────
    process_data: {
      run: async (ctx) => {
        console.log('[sms_demo] processing data...')
        return { records: 42, status: 'ok' }
      }
    },

    // ── Option A: SMS via Twilio (runs after process_data) ───────────────────
    // triggerRule: 'all_done' ensures SMS fires whether pipeline succeeded or failed
    sms_twilio: {
      dependsOn: ['process_data'],
      triggerRule: 'all_done',
      ...(SmsNotifyOperator ?? noop)({
        message: 'Pipeline $DAG_ID completed — run $RUN_ID',
        // to: '+12025559999',  // override recipient per-task (otherwise uses TWILIO_TO env)
      }),
    },

    // ── Option B: SMS/push via AWS SNS ────────────────────────────────────────
    // Uncomment and fill in your SNS topic ARN or phone number:
    //
    // sms_aws: {
    //   dependsOn: ['process_data'],
    //   triggerRule: 'all_done',
    //   ...(AwsSnsOperator ?? noop)({
    //     message: 'Pipeline $DAG_ID done — run $RUN_ID',
    //     phoneNumber: '+12025555678',   // direct SMS (E.164 format)
    //     // OR: topicArn: 'arn:aws:sns:us-east-1:123456789:my-alerts-topic',
    //   }),
    // },

    // ── Fallback: just log if no SMS credentials ─────────────────────────────
    log_notify: {
      dependsOn: ['process_data'],
      triggerRule: 'all_done',
      ...(LogNotifyOperator ?? noop)({
        message: 'Pipeline completed (SMS operators above send the real alert)',
        level: 'info',
      }),
    },

  }
}
