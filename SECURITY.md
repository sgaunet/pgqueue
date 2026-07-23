# Security Considerations

## Authentication & Authorization

pgqueue relies on PostgreSQL's authentication and authorization model:

- **Database-level access control**: Use PostgreSQL roles and permissions to restrict access.
- **No built-in ACLs**: All clients with database access can access all queues.
- **Network security**: Use SSL/TLS for database connections (`sslmode=require` or `sslmode=verify-full`).

### Recommended Setup

```sql
-- Create a restricted role for queue operations
CREATE ROLE pgqueue_app LOGIN PASSWORD 'strong-password';

-- Grant access to metadata tables
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE pgqueue_metadata TO pgqueue_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE pgqueue_subscribers TO pgqueue_app;
GRANT SELECT, INSERT ON TABLE pgqueue_replay_log TO pgqueue_app;

-- Grant access to per-queue tables as they are created
-- (must be repeated after each CreateChannel/CreateTopic call)
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO pgqueue_app;

-- Optionally restrict to specific queues
REVOKE ALL ON TABLE pgqueue_msg_sensitive_orders FROM pgqueue_app;
GRANT SELECT, UPDATE ON TABLE pgqueue_msg_sensitive_orders TO pgqueue_consumer;
GRANT INSERT ON TABLE pgqueue_msg_sensitive_orders TO pgqueue_publisher;
```

## Required PostgreSQL Privileges

pgqueue runs its own DDL — it creates and drops tables and indexes on the fly —
so the connection it is given needs more than plain DML rights. The privileges
break down by operation:

| Operation | SQL the library issues | Required privilege |
|-----------|------------------------|--------------------|
| `InitSchema` | `CREATE TABLE` / `CREATE INDEX` for the four global tables; `CREATE SCHEMA IF NOT EXISTS` when a non-`public` schema is configured via `WithSchema` | `CREATE` on the target schema (plus `CREATE` on the database if pgqueue must create the schema) |
| `CreateChannel` / `CreateTopic` | `CREATE TABLE` / `CREATE INDEX` for the per-queue tables (`pgqueue_msg_*`, `pgqueue_dlq_*`, and `pgqueue_sub_*` for topics) | `CREATE` on the schema |
| `DeleteChannel` / `DeleteTopic` | `DROP TABLE IF EXISTS` on the per-queue tables; `DELETE` on the global metadata/subscriber tables | Ownership of the per-queue tables (PostgreSQL only lets the owner `DROP`), plus `DELETE` on the global tables |
| `PurgeQueue` | `DELETE` / `TRUNCATE` on the per-queue tables | `DELETE` (and `TRUNCATE` if used) on those tables |
| Publish | `INSERT` (and `SELECT` on metadata) | `INSERT` / `SELECT` on the per-queue and metadata tables |
| Consume, ack/nack, GC, replay, stats | `SELECT` / `UPDATE` / `INSERT` / `DELETE`; `SELECT … FOR UPDATE SKIP LOCKED`; transaction-scoped advisory locks (`pg_advisory_xact_lock`) | `SELECT` / `INSERT` / `UPDATE` / `DELETE` on the per-queue and global tables; **advisory locks need no privilege** |
| Push delivery (`pglisten`) | `LISTEN` on the consumer connection; `NOTIFY` (via `pg_notify`) inside the publishing transaction | **none** — `LISTEN`/`NOTIFY` require no special privilege |
| Schema migrations (inside `InitSchema`) | session-level advisory lock (`pg_advisory_lock` / `pg_advisory_unlock`) to serialize concurrent upgrades | **none** |

The simplest deployment gives one role both DDL and DML rights (it then owns
everything it creates, so `DROP` "just works"):

```sql
CREATE ROLE pgqueue_app LOGIN PASSWORD 'strong-password';
GRANT CREATE ON SCHEMA public TO pgqueue_app;        -- DDL: InitSchema + Create*/Delete*
-- pgqueue_app owns the tables it creates, so DELETE/DROP need no extra grant.
```

To **separate** DDL from runtime — let an operator/migration role create and
drop queues while applications only publish and consume — split the roles. Note
that `DeleteChannel`/`DeleteTopic` issue `DROP TABLE`, which in PostgreSQL only
the table owner (or a superuser) may run, so deletion stays with the role that
created the queues:

```sql
-- Admin/migration role: creates and drops queues.
CREATE ROLE pgqueue_admin LOGIN PASSWORD '...';
GRANT CREATE ON SCHEMA public TO pgqueue_admin;

-- Runtime role: publishes and consumes, but cannot create or drop queues.
CREATE ROLE pgqueue_app LOGIN PASSWORD '...';
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO pgqueue_app;
-- Re-grant after each CreateChannel/CreateTopic (or use DEFAULT PRIVILEGES):
ALTER DEFAULT PRIVILEGES FOR ROLE pgqueue_admin IN SCHEMA public
    GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO pgqueue_app;
```

`ALTER DEFAULT PRIVILEGES` makes future tables created by `pgqueue_admin`
automatically grantable to `pgqueue_app`, sparing you a manual `GRANT` after
every `CreateChannel`/`CreateTopic`.

## Payload Encryption

pgqueue does **not** encrypt message payloads. Data is stored as `BYTEA` in PostgreSQL. If your messages contain sensitive data, encrypt before publishing:

```go
// Encrypt before publishing
ciphertext, err := encrypt(sensitiveData, encryptionKey)
if err != nil {
    log.Fatal(err)
}
pq.Publish(ctx, "orders", ciphertext)

// Decrypt after consuming
msg, err := pq.ReceiveChannel(ctx, "orders", pgqueue.WithVisibilityTimeout(30*time.Second))
if err != nil {
    log.Fatal(err)
}
plaintext, err := decrypt(msg.Payload, encryptionKey)
```

Consider also enabling [PostgreSQL TDE](https://www.postgresql.org/docs/current/encryption-options.html) or filesystem-level encryption for data at rest.

## Rate Limiting

pgqueue has no built-in rate limiting. To protect against resource exhaustion:

- **Application-level**: Use a token bucket or leaky bucket limiter before publishing.
- **PostgreSQL connection limits**: Configure `max_connections` and use connection pooling (PgBouncer, pgpool).
- **Statement timeout**: Set `statement_timeout` to prevent long-running queries from holding locks.
- **Network-level**: Use firewall rules to restrict database access to known hosts.

## Clock Skew and UUIDv7

UUIDv7 embeds a millisecond timestamp for message ordering. This depends on accurate system time:

- **Clock skew**: If the system clock jumps backwards, newly published messages may sort before older ones, breaking expected ordering.
- **Mitigation**: Use NTP (`chrony` or `systemd-timesyncd`) or PTP for time synchronization on all database servers and application hosts.
- **Multi-node publishing**: When multiple application instances publish to the same queue, ensure clocks are synchronized across all nodes to maintain global ordering.
- **Detection**: Monitor for clock drift using your infrastructure monitoring tools.

## SQL Injection Protection

All SQL queries use parameterized placeholders (`$1`, `$2`, etc.) via `database/sql`. Queue names are validated against:

```go
queueNameRegex = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)
```

Table names are derived from validated queue names via `sanitizeTableName()` (dashes converted to underscores). **Do not bypass this validation** — always use `CreateChannel()`/`CreateTopic()` to create queues.

## Reporting Vulnerabilities

If you discover a security vulnerability, please report it responsibly:

- **Do not** open a public GitHub issue for security vulnerabilities.
- Open a [private security advisory](https://github.com/sgaunet/pgqueue/security/advisories/new) on this repository.
- Include steps to reproduce, impact assessment, and any suggested fixes.

We will acknowledge receipt within 48 hours and work with you on a fix before public disclosure.
