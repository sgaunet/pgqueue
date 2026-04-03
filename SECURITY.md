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
msg, err := pq.ConsumeFromChannel(ctx, "orders", 30*time.Second)
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
