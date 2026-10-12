// Prints the GoogleSQL DDL for the Spanner backend (for a DBA / Terraform / gcloud):
//   npm run spanner:ddl > schema.sql
import { buildDdl } from './schema.js'

console.log(buildDdl().map(s => `${s};`).join('\n\n'))
