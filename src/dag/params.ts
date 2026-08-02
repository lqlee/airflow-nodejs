/**
 * DAG params — typed parameter definitions with validation.
 *
 * Equivalent to Airflow's Param class. Each param has a type, optional default,
 * optional description, and optional constraints (enum, min/max, pattern).
 *
 * At trigger time, the caller's conf is validated and merged with defaults.
 * Missing required params (no default, not supplied) → 400 error.
 * Type mismatches → 400 error with a descriptive message.
 */

import type { DagDefinition, ParamDefinition } from './types.js'

export interface ParamValidationError {
  param: string
  message: string
}

export interface ParamValidationResult {
  ok: boolean
  errors: ParamValidationError[]
  /** Merged conf: defaults + caller-supplied values (caller wins on collision) */
  mergedConf: Record<string, unknown>
}

/**
 * Validate caller-supplied conf against the DAG's params schema.
 * Returns merged conf (defaults filled in) on success.
 */
export function validateParams(
  dag: DagDefinition,
  callerConf: Record<string, unknown>,
): ParamValidationResult {
  if (!dag.params || Object.keys(dag.params).length === 0) {
    // No params defined — pass through unchanged
    return { ok: true, errors: [], mergedConf: callerConf }
  }

  const errors: ParamValidationError[] = []
  const mergedConf: Record<string, unknown> = { ...callerConf }

  for (const [key, param] of Object.entries(dag.params)) {
    const supplied = key in callerConf
    const value = supplied ? callerConf[key] : param.default

    // Required check (no default and not supplied)
    if (!supplied && param.default === undefined) {
      errors.push({ param: key, message: `Required param '${key}' not provided` })
      continue
    }

    // Fill in default
    if (!supplied && param.default !== undefined) {
      mergedConf[key] = param.default
    }

    const v = mergedConf[key]

    // Type check
    if (v !== undefined && v !== null) {
      const typeErr = checkType(key, v, param)
      if (typeErr) { errors.push(typeErr); continue }
    }

    // Enum check
    if (param.enum !== undefined && v !== undefined && v !== null) {
      if (!param.enum.includes(v)) {
        errors.push({
          param: key,
          message: `Param '${key}' must be one of [${param.enum.map(e => JSON.stringify(e)).join(', ')}], got ${JSON.stringify(v)}`,
        })
        continue
      }
    }

    // Numeric range
    if (param.type === 'number' || param.type === 'integer') {
      if (typeof v === 'number') {
        if (param.minimum !== undefined && v < param.minimum) {
          errors.push({ param: key, message: `Param '${key}' must be >= ${param.minimum}, got ${v}` })
        }
        if (param.maximum !== undefined && v > param.maximum) {
          errors.push({ param: key, message: `Param '${key}' must be <= ${param.maximum}, got ${v}` })
        }
        if (param.type === 'integer' && !Number.isInteger(v)) {
          errors.push({ param: key, message: `Param '${key}' must be an integer, got ${v}` })
        }
      }
    }

    // String pattern
    if (param.type === 'string' && param.pattern && typeof v === 'string') {
      if (!new RegExp(param.pattern).test(v)) {
        errors.push({ param: key, message: `Param '${key}' must match pattern /${param.pattern}/, got '${v}'` })
      }
    }
  }

  return {
    ok: errors.length === 0,
    errors,
    mergedConf,
  }
}

function checkType(key: string, value: unknown, param: ParamDefinition): ParamValidationError | null {
  if (!param.type) return null

  const actual = Array.isArray(value) ? 'array' : typeof value

  switch (param.type) {
    case 'string':
      if (actual !== 'string') return { param: key, message: `Param '${key}' must be a string, got ${actual}` }
      break
    case 'number':
      if (actual !== 'number') return { param: key, message: `Param '${key}' must be a number, got ${actual}` }
      break
    case 'integer':
      if (actual !== 'number') return { param: key, message: `Param '${key}' must be an integer, got ${actual}` }
      break
    case 'boolean':
      if (actual !== 'boolean') return { param: key, message: `Param '${key}' must be a boolean, got ${actual}` }
      break
    case 'array':
      if (actual !== 'array') return { param: key, message: `Param '${key}' must be an array, got ${actual}` }
      break
    case 'object':
      if (actual !== 'object' || Array.isArray(value)) return { param: key, message: `Param '${key}' must be an object, got ${actual}` }
      break
  }
  return null
}
