/**
 * OpenTelemetry traces: one span per Dag run, with one child span per task try.
 *
 * Runs outlive processes (a run can last hours, move between scheduler replicas, survive restarts), so
 * spans are not held open: each is emitted when it finishes, with its real start time. The run's trace
 * and span ids are derived from the run id and when it was (last) queued, so a task span recorded on
 * any replica — before the run itself finishes — lands under the right run span, and a cleared and
 * re-run run gets a fresh run span in the same trace.
 *
 * Enabled when an OTLP endpoint is configured (OTEL_EXPORTER_OTLP_ENDPOINT or
 * OTEL_EXPORTER_OTLP_TRACES_ENDPOINT, plus the usual OTEL_EXPORTER_OTLP_HEADERS etc.), or with
 * OTEL_TRACES_EXPORTER=console. OTEL_SDK_DISABLED=true turns it off. Service name: OTEL_SERVICE_NAME
 * (default airflow-nodejs). When disabled nothing below loads the SDK and every emit is a no-op.
 */
import { createHash } from 'node:crypto'
import { ROOT_CONTEXT, SpanKind, SpanStatusCode, TraceFlags, trace, type Attributes, type Tracer } from '@opentelemetry/api'
import type { SpanExporter, IdGenerator } from '@opentelemetry/sdk-trace'

interface Provider { getTracer(name: string): Tracer; forceFlush(): Promise<void>; shutdown(): Promise<void> }

let provider: Provider | null = null
let tracer: Tracer | null = null
/** Ids for the next root span (a run span); read synchronously by the id generator inside startSpan. */
let nextRootIds: { traceId: string; spanId: string } | null = null

export function isTracingEnabled(): boolean {
  return tracer !== null
}

function configuredExporter(): 'otlp' | 'console' | null {
  if (process.env.OTEL_SDK_DISABLED === 'true') return null
  const kind = process.env.OTEL_TRACES_EXPORTER?.trim().toLowerCase()
  if (kind === 'none') return null
  if (kind === 'console') return 'console'
  return process.env.OTEL_EXPORTER_OTLP_ENDPOINT || process.env.OTEL_EXPORTER_OTLP_TRACES_ENDPOINT ? 'otlp' : null
}

/**
 * Start the tracer provider if tracing is configured. `exporter` (tests) replaces the configured one
 * and exports synchronously. Returns whether tracing is on.
 */
export async function initTracing(opts: { exporter?: SpanExporter } = {}): Promise<boolean> {
  if (provider) return true
  const kind = opts.exporter ? 'injected' : configuredExporter()
  if (!kind) return false
  const sdk = await import('@opentelemetry/sdk-trace')
  const { resourceFromAttributes } = await import('@opentelemetry/resources')
  const random = new sdk.RandomIdGenerator()
  const idGenerator: IdGenerator = {
    generateTraceId: () => nextRootIds?.traceId ?? random.generateTraceId(),
    generateSpanId: () => nextRootIds?.spanId ?? random.generateSpanId(),
  }
  let processor
  if (opts.exporter) processor = new sdk.SimpleSpanProcessor({ exporter: opts.exporter })
  else if (kind === 'console') processor = new sdk.SimpleSpanProcessor({ exporter: new sdk.ConsoleSpanExporter() })
  else {
    const { OTLPTraceExporter } = await import('@opentelemetry/exporter-trace-otlp-http')
    processor = new sdk.BatchSpanProcessor({ exporter: new OTLPTraceExporter() })
  }
  provider = new sdk.TracerProvider({
    resource: resourceFromAttributes({ 'service.name': process.env.OTEL_SERVICE_NAME || 'airflow-nodejs' }),
    idGenerator,
    spanProcessors: [processor],
  })
  tracer = provider.getTracer('airflow-nodejs')
  return true
}

/** Flush pending spans and stop (graceful shutdown; tests). */
export async function shutdownTracing(): Promise<void> {
  const p = provider
  provider = null
  tracer = null
  await p?.shutdown().catch((err: unknown) => console.error(`[tracing] shutdown failed: ${(err as Error).message}`))
}

/** Trace id of a run (its whole history), and the span id of the run attempt queued at `queuedAt`. */
export function runSpanIds(runId: string, queuedAt: Date | null | undefined): { traceId: string; spanId: string } {
  const hex = (s: string) => createHash('sha256').update(s).digest('hex')
  return { traceId: hex(`run:${runId}`).slice(0, 32), spanId: hex(`run:${runId}:${queuedAt?.toISOString() ?? ''}`).slice(0, 16) }
}

const toDate = (d: unknown): Date | null => (d instanceof Date ? d : d ? new Date(d as string) : null)

/** The run fields a span needs (a dag_runs document). */
export interface RunForSpan {
  _id: { toString(): string }
  dag_id: string
  state: string
  created_at: Date
  queued_at?: Date | null
  started_at?: Date | null
  ended_at?: Date | null
  trigger_type?: string
  logical_date?: Date | null
}

/** The attempt's queue time: the latest clear, else creation (legacy runs have no queued_at). */
const attemptQueuedAt = (run: Pick<RunForSpan, 'queued_at' | 'created_at'>) => toDate(run.queued_at) ?? toDate(run.created_at)

/** Emit the span of a finished run attempt (success, failed, cancelled or marked). */
export function emitRunSpan(run: RunForSpan): void {
  if (!tracer) return
  const runId = run._id.toString()
  const queuedAt = attemptQueuedAt(run)
  const attributes: Attributes = {
    'airflow.dag_id': run.dag_id,
    'airflow.run_id': runId,
    'airflow.run.state': run.state,
  }
  if (run.trigger_type) attributes['airflow.run.trigger_type'] = run.trigger_type
  if (run.logical_date) attributes['airflow.run.logical_date'] = toDate(run.logical_date)!.toISOString()
  if (run.started_at) attributes['airflow.run.started_at'] = toDate(run.started_at)!.toISOString()
  nextRootIds = runSpanIds(runId, queuedAt)
  try {
    const span = tracer.startSpan(`dag_run ${run.dag_id}`, { kind: SpanKind.INTERNAL, root: true, startTime: queuedAt ?? new Date(), attributes })
    span.setStatus(run.state === 'failed' ? { code: SpanStatusCode.ERROR, message: 'run failed' } : { code: SpanStatusCode.OK })
    span.end(toDate(run.ended_at) ?? new Date())
  } finally {
    nextRootIds = null
  }
}

/** The task-instance fields a span needs. */
export interface TryForSpan {
  dag_run_id: string
  dag_id: string
  task_id: string
  map_index?: number | null
  try_number: number
  started_at?: Date | null
}

/** Emit the span of one finished task try, as a child of its run attempt's span. */
export function emitTaskSpan(
  ti: TryForSpan,
  run: Pick<RunForSpan, 'queued_at' | 'created_at'>,
  outcome: 'success' | 'failed',
  endedAt: Date,
  error: string | null,
): void {
  if (!tracer) return
  const { traceId, spanId } = runSpanIds(ti.dag_run_id, attemptQueuedAt(run))
  const parent = trace.setSpanContext(ROOT_CONTEXT, { traceId, spanId, traceFlags: TraceFlags.SAMPLED, isRemote: true })
  const attributes: Attributes = {
    'airflow.dag_id': ti.dag_id,
    'airflow.run_id': ti.dag_run_id,
    'airflow.task_id': ti.task_id,
    'airflow.task.try_number': ti.try_number + 1,   // 1-based, as shown in the UI and Airflow
    'airflow.task.state': outcome,
  }
  if (ti.map_index != null && ti.map_index >= 0) attributes['airflow.task.map_index'] = ti.map_index
  const name = `task ${ti.dag_id}.${ti.task_id}${attributes['airflow.task.map_index'] !== undefined ? `[${ti.map_index}]` : ''}`
  const span = tracer.startSpan(name, { kind: SpanKind.INTERNAL, startTime: toDate(ti.started_at) ?? endedAt, attributes }, parent)
  span.setStatus(outcome === 'failed' ? { code: SpanStatusCode.ERROR, message: (error ?? 'failed').slice(0, 1000) } : { code: SpanStatusCode.OK })
  span.end(endedAt)
}
