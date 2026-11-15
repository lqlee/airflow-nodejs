#!/usr/bin/env node
/** `anode` entry point (package.json "bin"). See cli/run.ts for commands. */
import { installStreamMasking } from '../secrets/masker.js'
import { runCli } from './run.js'

// `tasks test` runs task code here — mask secrets it reads in everything printed
installStreamMasking()

const code = await runCli(process.argv.slice(2), {
  io: { out: s => process.stdout.write(`${s}\n`), err: s => process.stderr.write(`${s}\n`) },
})
// Exit explicitly: a tested task may leave handles open (sockets, timers)
process.exitCode = code
process.stdout.write('', () => process.exit(code))
