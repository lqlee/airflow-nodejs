/** Vault and Akeyless backends against a local fake of each API, and the connection URI format. */
import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { createServer, type IncomingMessage, type Server } from 'node:http'
import { mkdtemp, writeFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { VaultSecretsBackend, AkeylessSecretsBackend, type VaultOptions } from '../remote.js'
import { parseConnectionUri, connectionFromSecretString } from '../conn-format.js'
import { getSecretsBackend, resetSecretsBackend } from '../index.js'

type Handler = (req: IncomingMessage, body: Record<string, unknown>) => { status: number; body: unknown }
let server: Server
let base: string
let handler: Handler
const requests: Array<{ method: string; url: string; headers: IncomingMessage['headers']; body: Record<string, unknown> }> = []

beforeAll(async () => {
  server = createServer((req, res) => {
    let raw = ''
    req.on('data', c => { raw += c })
    req.on('end', () => {
      const body = raw ? JSON.parse(raw) as Record<string, unknown> : {}
      requests.push({ method: req.method!, url: req.url!, headers: req.headers, body })
      const out = handler(req, body)
      res.writeHead(out.status, { 'content-type': 'application/json' })
      res.end(JSON.stringify(out.body))
    })
  })
  await new Promise<void>(r => server.listen(0, '127.0.0.1', r))
  const addr = server.address() as { port: number }
  base = `http://127.0.0.1:${addr.port}`
})
afterAll(async () => { await new Promise(r => server.close(r)) })
afterEach(() => {
  requests.length = 0
  resetSecretsBackend()
  for (const k of ['SECRETS_BACKEND', 'VAULT_ADDR', 'VAULT_TOKEN', 'SECRETS_CACHE_TTL_MS']) delete process.env[k]
})

const vaultOpts = (o: Partial<VaultOptions> = {}): VaultOptions => ({
  addr: base, mount: 'secret', kvVersion: 2, connectionsPath: 'connections', variablesPath: 'variables', ...o,
})

describe('connection formats', () => {
  it('parses an Airflow connection URI', () => {
    expect(parseConnectionUri('db', 'google-cloud-platform://us%40r:p%40ss+w@db.example.com:5432/my%2Fschema?sslmode=require&timeout=10'))
      .toEqual({
        conn_id: 'db', conn_type: 'google_cloud_platform', host: 'db.example.com', port: 5432, schema: 'my/schema',
        login: 'us@r', password: 'p@ss+w', extra: { sslmode: 'require', timeout: '10' },
      })
    expect(parseConnectionUri('x', 'http://h?__extra__=%7B%22a%22%3A%7B%22b%22%3A1%7D%7D').extra).toEqual({ a: { b: 1 } })
  })

  it('reads JSON, json:-prefixed JSON, a conn_uri field, or a URI', () => {
    expect(connectionFromSecretString('a', '{"conn_type":"http","host":"h","port":"8080","extra":"{\\"k\\":1}"}'))
      .toMatchObject({ conn_type: 'http', host: 'h', port: 8080, extra: { k: 1 } })
    expect(connectionFromSecretString('a', 'json:{"conn_uri":"mysql://u:p@h/s"}')).toMatchObject({ conn_type: 'mysql', login: 'u', schema: 's' })
    expect(() => connectionFromSecretString('a', 'not a uri')).toThrow(/not a URI/)
  })
})

describe('VaultSecretsBackend', () => {
  it('reads KV v2 connections and variables with the token and namespace headers', async () => {
    handler = req => {
      if (req.url === '/v1/secret/data/connections/pg') return { status: 200, body: { data: { data: { conn_uri: 'postgres://u:pw@db:5432/app' } } } }
      if (req.url === '/v1/secret/data/variables/api_key') return { status: 200, body: { data: { data: { value: 'v-123' } } } }
      return { status: 404, body: { errors: [] } }
    }
    const vault = new VaultSecretsBackend(vaultOpts({ token: 's.tok', namespace: 'team-a' }))
    expect(await vault.getConnection('pg')).toMatchObject({ conn_type: 'postgres', host: 'db', password: 'pw', schema: 'app' })
    expect(await vault.getVariable('api_key')).toBe('v-123')
    expect(await vault.getVariable('missing')).toBeNull()
    expect(requests[0].headers).toMatchObject({ 'x-vault-token': 's.tok', 'x-vault-namespace': 'team-a' })
  })

  it('KV v1 layout and field-style connections', async () => {
    handler = req => req.url === '/v1/kv/conns/http_api'
      ? { status: 200, body: { data: { conn_type: 'http', host: 'api.example.com', password: 'p' } } }
      : { status: 404, body: {} }
    const vault = new VaultSecretsBackend(vaultOpts({ token: 't', kvVersion: 1, mount: 'kv', connectionsPath: 'conns' }))
    expect(await vault.getConnection('http_api')).toMatchObject({ conn_type: 'http', host: 'api.example.com', password: 'p' })
  })

  it('Kubernetes login: posts the service-account JWT, reuses the token, logs in again when it is rejected', async () => {
    const dir = await mkdtemp(join(tmpdir(), 'vault-jwt-'))
    const jwtPath = join(dir, 'token')
    await writeFile(jwtPath, 'jwt-abc\n')
    let issued = 0
    let revoked = ''
    handler = (req, body) => {
      if (req.url === '/v1/auth/k8s-cluster/login') {
        expect(body).toEqual({ role: 'airflow', jwt: 'jwt-abc' })
        return { status: 200, body: { auth: { client_token: `tok-${++issued}`, lease_duration: 3600 } } }
      }
      if (req.headers['x-vault-token'] === revoked) return { status: 403, body: { errors: ['permission denied'] } }
      return { status: 200, body: { data: { data: { value: String(req.headers['x-vault-token']) } } } }
    }
    try {
      const vault = new VaultSecretsBackend(vaultOpts({ role: 'airflow', authPath: 'k8s-cluster', jwtPath }))
      expect(await vault.getVariable('a')).toBe('tok-1')
      expect(await vault.getVariable('b')).toBe('tok-1')   // token reused
      revoked = 'tok-1'
      expect(await vault.getVariable('c')).toBe('tok-2')   // 403 → one re-login
    } finally {
      await rm(dir, { recursive: true })
    }
  })

  it('a server error fails the lookup with the cause instead of returning "not found"', async () => {
    handler = () => ({ status: 500, body: { errors: ['sealed'] } })
    const vault = new VaultSecretsBackend(vaultOpts({ token: 't' }))
    await expect(vault.getVariable('x')).rejects.toThrow(/vault read variables\/x: HTTP 500/)
  })

  it('SECRETS_CACHE_TTL_MS caches found secrets', async () => {
    process.env.SECRETS_CACHE_TTL_MS = '60000'
    handler = () => ({ status: 200, body: { data: { data: { value: 'cached' } } } })
    const vault = new VaultSecretsBackend(vaultOpts({ token: 't' }))
    await vault.getVariable('x')
    await vault.getVariable('x')
    expect(requests).toHaveLength(1)
  })

  it('is selected by SECRETS_BACKEND=vault and refuses to start without credentials', () => {
    process.env.SECRETS_BACKEND = 'vault'
    process.env.VAULT_ADDR = base
    expect(() => getSecretsBackend()).toThrow(/VAULT_TOKEN/)
    process.env.VAULT_TOKEN = 't'
    resetSecretsBackend()
    expect(getSecretsBackend().name).toBe('vault')
  })
})

describe('AkeylessSecretsBackend', () => {
  it('API-key login, then get-secret-value for connections (URI or JSON) and variables', async () => {
    const secrets: Record<string, string> = {
      '/airflow/connections/pg': 'postgres://u:pw@db/app',
      '/airflow/connections/api': '{"conn_type":"http","host":"api"}',
      '/airflow/variables/region': 'us-east1',
    }
    handler = (req, body) => {
      if (req.url === '/auth') {
        expect(body).toEqual({ 'access-id': 'p-123', 'access-type': 'access_key', 'access-key': 'k-xyz' })
        return { status: 200, body: { token: 't-abc' } }
      }
      expect(body.token).toBe('t-abc')
      const name = (body.names as string[])[0]
      return name in secrets ? { status: 200, body: { [name]: secrets[name] } } : { status: 404, body: { error: 'item not found' } }
    }
    const ak = new AkeylessSecretsBackend({ apiUrl: base, accessId: 'p-123', accessKey: 'k-xyz', connectionsPath: '/airflow/connections', variablesPath: '/airflow/variables/' })
    expect(await ak.getConnection('pg')).toMatchObject({ conn_type: 'postgres', password: 'pw' })
    expect(await ak.getConnection('api')).toMatchObject({ conn_type: 'http', host: 'api' })
    expect(await ak.getVariable('region')).toBe('us-east1')
    expect(await ak.getVariable('nope')).toBeNull()
    expect(requests.filter(r => r.url === '/auth')).toHaveLength(1)
  })

  it('Kubernetes auth sends the base64 service-account token and the gateway URL', async () => {
    const dir = await mkdtemp(join(tmpdir(), 'ak-jwt-'))
    const jwtPath = join(dir, 'token')
    await writeFile(jwtPath, 'sa-jwt')
    handler = (req, body) => req.url === '/auth'
      ? { status: 200, body: { token: 't-k8s', echo: body } }
      : { status: 200, body: { '/airflow/variables/v': 'ok' } }
    try {
      const ak = new AkeylessSecretsBackend({ apiUrl: base, accessId: 'p-k8s', k8sAuthConfigName: 'wcnp', gatewayUrl: 'https://gw:8000', jwtPath, connectionsPath: '/airflow/connections', variablesPath: '/airflow/variables' })
      expect(await ak.getVariable('v')).toBe('ok')
      expect(requests[0].body).toEqual({
        'access-id': 'p-k8s', 'access-type': 'k8s', 'k8s-auth-config-name': 'wcnp',
        'k8s-service-account-token': Buffer.from('sa-jwt').toString('base64'), 'gateway-url': 'https://gw:8000',
      })
    } finally {
      await rm(dir, { recursive: true })
    }
  })

  it('a rejected login is an error, not a missing secret', async () => {
    handler = () => ({ status: 401, body: { error: 'access denied' } })
    const ak = new AkeylessSecretsBackend({ apiUrl: base, accessId: 'p', accessKey: 'bad', connectionsPath: '/c', variablesPath: '/v' })
    await expect(ak.getVariable('x')).rejects.toThrow(/akeyless auth failed: HTTP 401/)
  })
})
