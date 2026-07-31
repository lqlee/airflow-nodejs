import { dag } from 'airflow-nodejs/dag/types';
// §3 §16 — scheduled DAG for cron + backfill tests
export default dag({
  id: 'verify_scheduled',
  schedule: '*/2 * * * *',
  tasks: {
    tick: { run: async (ctx) => ({ ts: new Date().toISOString(), conf: ctx.conf }) },
  }
})
