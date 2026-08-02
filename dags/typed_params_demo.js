import { dag } from 'airflow-nodejs/dag/types';

/**
 * Typed Params Demo — DAG parameters with type/default/validation.
 *
 * Params are validated at trigger time. Missing required params
 * or type/range/enum violations return a 400 error before the run starts.
 * Default values are merged into conf automatically.
 *
 * Trigger examples:
 *   # All defaults — only required 'pipeline_name' supplied:
 *   POST /dags/typed_params_demo/trigger
 *   body: { "conf": { "pipeline_name": "my-etl" } }
 *
 *   # Override defaults:
 *   body: { "conf": { "pipeline_name": "prod-etl", "env": "prod", "batch_size": 500 } }
 *
 *   # Missing required → 400:
 *   body: { "conf": {} }
 *   → { "error": "Param validation failed", "param_errors": [{ "param": "pipeline_name", ... }] }
 *
 *   # Wrong type → 400:
 *   body: { "conf": { "pipeline_name": "x", "batch_size": "big" } }
 *   → param_errors: [{ "param": "batch_size", "message": "must be an integer" }]
 */
export default dag({
  id: 'typed_params_demo',
  schedule: null,

  params: {
    // Required — no default
    pipeline_name: {
      type: 'string',
      description: 'Name of the pipeline to run (required)',
    },

    // Optional with enum constraint
    env: {
      type: 'string',
      enum: ['dev', 'staging', 'prod'],
      default: 'dev',
      description: 'Target environment',
    },

    // Optional with range constraint
    batch_size: {
      type: 'integer',
      minimum: 1,
      maximum: 10000,
      default: 100,
      description: 'Number of records per batch (1–10000)',
    },

    // Optional boolean
    dry_run: {
      type: 'boolean',
      default: false,
      description: 'If true, process but do not write output',
    },

    // Optional with pattern
    output_prefix: {
      type: 'string',
      pattern: '^[a-zA-Z0-9_-]+$',
      default: 'output',
      description: 'S3/storage prefix (alphanumeric, underscores, hyphens only)',
    },
  },

  tasks: {
    run_pipeline: {
      run: async (ctx) => {
        // ctx.conf contains caller-supplied values + defaults merged in
        const { pipeline_name, env, batch_size, dry_run, output_prefix } = ctx.conf

        console.log(`[typed_params_demo] pipeline: ${pipeline_name}`)
        console.log(`[typed_params_demo] env:      ${env}`)
        console.log(`[typed_params_demo] batch:    ${batch_size}`)
        console.log(`[typed_params_demo] dry_run:  ${dry_run}`)
        console.log(`[typed_params_demo] prefix:   ${output_prefix}`)

        if (dry_run) {
          console.log('[typed_params_demo] DRY RUN — no output written')
        }

        return {
          pipeline: pipeline_name,
          env,
          batch_size,
          dry_run,
          records_processed: dry_run ? 0 : Number(batch_size),
        }
      }
    }
  }
})
