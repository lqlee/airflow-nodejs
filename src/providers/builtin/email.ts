/**
 * smtp provider (nodemailer): EmailOperator.
 *
 *   connection (type smtp, default conn id smtp_default): host, port, login, password;
 *   extra (as Airflow's smtp provider): from_email, disable_ssl (default false → implicit TLS, port 465),
 *   disable_tls (with disable_ssl: no STARTTLS either), timeout (seconds, default 30)
 */
import type { ConnectionRuntime } from '../../connections/index.js'
import { loadDriver } from './drivers.js'
import type { OperatorImpl } from './runtime.js'
import { optStr, str, strList } from './args.js'

const SMTP = ['smtp', 'email']

const flag = (v: unknown) => v === true || v === 'true'

export function smtpTransport(conn: ConnectionRuntime) {
  const extra = conn.extra ?? {}
  const ssl = !flag(extra.disable_ssl)
  const tls = !ssl && !flag(extra.disable_tls)
  const timeout = Number(extra.timeout ?? 30) * 1000
  return {
    host: conn.host ?? 'localhost',
    port: conn.port ?? (ssl ? 465 : 587),
    secure: ssl,
    requireTLS: tls,
    ignoreTLS: !ssl && !tls,
    auth: conn.login ? { user: conn.login, pass: conn.password ?? '' } : undefined,
    connectionTimeout: timeout, greetingTimeout: timeout, socketTimeout: timeout,
  }
}

export const IMPLS: Record<string, OperatorImpl> = {
  'email.send': async (args, rt) => {
    const conn = await rt.connection(optStr(args.connId, 'connId') ?? 'smtp_default', SMTP)
    const from = optStr(args.from, 'from') ?? (conn.extra?.from_email as string | undefined) ?? conn.login
    if (!from) throw new Error('no sender — set from, or from_email in the connection extra')
    const html = optStr(args.html, 'html')
    const text = optStr(args.text, 'text')
    const list = (k: string) => (args[k] === undefined ? undefined : strList(args[k], k))
    const nodemailer = await loadDriver('nodemailer')
    const transport = nodemailer.createTransport(smtpTransport(conn))
    try {
      const info = await transport.sendMail({
        from, to: strList(args.to, 'to'), cc: list('cc'), bcc: list('bcc'), replyTo: optStr(args.replyTo, 'replyTo'),
        subject: str(args.subject, 'subject'),
        ...(html ? { html } : {}), ...(text || !html ? { text: text ?? '' } : {}),
        attachments: list('files')?.map(path => ({ path })),
      })
      rt.log(`[email] sent "${String(args.subject)}" to ${info.accepted.length} recipient(s)`)
      return { messageId: info.messageId, accepted: info.accepted.map(String), rejected: info.rejected.map(String) }
    } finally {
      transport.close()
    }
  },
}
