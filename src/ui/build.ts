/**
 * Compile the UI (src/ui/app.jsx) to plain JavaScript — no Babel in the browser, so the page can run
 * under a strict Content-Security-Policy (script-src 'self', no 'unsafe-eval').
 *
 *   npm run build  → tsc, then `node dist/ui/build.js` writes dist/public/app.js (served in production)
 *   npm run dev    → the server compiles app.jsx on request instead (see api/ui-assets.ts)
 *
 * Uses the TypeScript compiler already in devDependencies; it is never loaded by the running server
 * in production.
 */
import { readFile, writeFile, mkdir } from 'node:fs/promises'
import { dirname, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'

const __dirname = dirname(fileURLToPath(import.meta.url))
/** Package root: one level above src/ (dev) or dist/ (compiled). */
const ROOT = resolve(__dirname, '../..')
export const UI_SOURCE = resolve(ROOT, 'src/ui/app.jsx')
export const UI_OUTPUT = resolve(ROOT, 'dist/public/app.js')

export interface CompiledUi { code: string; errors: string[] }

/** JSX → ES2020 using the page's global React (loaded from /vendor/react.js). */
export async function compileUi(source: string): Promise<CompiledUi> {
  const ts = (await import('typescript')).default
  const out = ts.transpileModule(source, {
    fileName: 'app.jsx',
    reportDiagnostics: true,
    compilerOptions: { jsx: ts.JsxEmit.React, target: ts.ScriptTarget.ES2020, module: ts.ModuleKind.None, allowJs: true, removeComments: false },
  })
  const errors = (out.diagnostics ?? []).map(d => {
    const pos = d.file && d.start !== undefined ? d.file.getLineAndCharacterOfPosition(d.start) : null
    return `${pos ? `app.jsx:${pos.line + 1}:${pos.character + 1} ` : ''}${ts.flattenDiagnosticMessageText(d.messageText, '\n')}`
  })
  // A classic script: TypeScript adds `export {}`/"use strict" markers only for modules — strip any
  const code = out.outputText.replace(/^export \{\};?\s*$/m, '')
  return { code: `/* Generated from src/ui/app.jsx by src/ui/build.ts — do not edit */\n${code}`, errors }
}

async function main(): Promise<void> {
  const { code, errors } = await compileUi(await readFile(UI_SOURCE, 'utf8'))
  if (errors.length) {
    console.error(`[build-ui] ${errors.length} error(s):\n${errors.join('\n')}`)
    process.exit(1)
  }
  await mkdir(dirname(UI_OUTPUT), { recursive: true })
  await writeFile(UI_OUTPUT, code)
  console.log(`[build-ui] wrote ${UI_OUTPUT} (${Math.round(code.length / 1024)} KiB)`)
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  main().catch(err => { console.error(`[build-ui] ${(err as Error).message}`); process.exit(1) })
}
