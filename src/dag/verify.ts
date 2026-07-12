import { loadDags } from './loader.js'
import { listDags, getDag } from './registry.js'

await loadDags()

const dags = listDags()
console.log(`\n✅ Loaded ${dags.length} dag(s):`)
for (const d of dags) {
  const tasks = Object.entries(d.tasks)
  console.log(`   ${d.id} — schedule: ${d.schedule ?? 'manual'}`)
  for (const [taskId, task] of tasks) {
    const deps = task.dependsOn?.length ? ` (depends on: ${task.dependsOn.join(', ')})` : ''
    console.log(`     • ${taskId}${deps}`)
  }
}

// Verify getDag lookup works
const found = getDag('hello_world')
console.log(`\n✅ getDag('hello_world'):`, found ? 'found' : 'NOT FOUND ❌')

console.log('\n✅ Phase 2 complete')
