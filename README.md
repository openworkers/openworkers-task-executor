# OpenWorkers Task Executor

A minimal, standalone task executor that runs JavaScript tasks using V8. Unlike the full OpenWorkers runner, this executor:

- Has minimal dependencies
- Only implements `fetch()` (no KV, Storage, Database bindings)
- Supports multiple task sources: CLI, NATS, PostgreSQL

Perfect for background jobs, scheduled tasks, or lightweight serverless workloads.

## Features

| Feature    | Description                     | Default |
| ---------- | ------------------------------- | ------- |
| `nats`     | NATS message queue listener     | ✅      |
| `database` | PostgreSQL queue with pg_notify | ❌      |

## Installation

```bash
# Build with default features (nats)
cargo build --release

# Build with database support
cargo build --release --features database

# Build with all features
cargo build --release --features nats,database
```

### V8 Snapshot (Important)

For optimal performance and stability, generate a V8 snapshot before running:

```bash
cargo run --bin snapshot
```

This creates a snapshot at `/tmp/openworkers-runtime-snapshot.bin` containing pre-compiled JavaScript APIs (URL, Headers, Request, Response, etc.).

**Without a snapshot:**
- Slower cold starts (~2-3ms vs ~100µs)
- APIs are compiled on every request

**Docker:** The Docker image includes the snapshot automatically.

## Usage

### One-shot execution (`run`)

Execute a single JavaScript file:

```bash
# Simple execution
task-executor run script.js

# With JSON payload
task-executor run script.js --payload '{"name": "world"}'

# With timeout (ms)
task-executor run script.js --timeout 5000

# Quiet mode (suppress console.log)
task-executor run script.js --quiet
```

Example script:

```javascript
export default {
  async task(payload) {
    const response = await fetch("https://api.example.com/data");
    const data = await response.json();

    return {
      input: payload,
      result: data,
    };
  },
};
```

### NATS listener (`listen`)

Listen for tasks on a NATS subject:

```bash
task-executor listen \
  --nats nats://localhost:4222 \
  --subject tasks \
  --root ./workers \
  --timeout 30000
```

#### NATS Message Format

```json
{
  "script": "hello.js",
  "payload": { "name": "world" },
  "timeout": 5000
}
```

> **Note**: Scripts must exist in the `--root` directory. Nested paths like `"script": "workers/task.js"` are allowed.

### Database listener (`db-listen`)

Listen for tasks from a PostgreSQL table using `pg_notify`:

```bash
task-executor db-listen \
  --database-url postgres://user:pass@localhost/mydb \
  --table ow_tasks \
  --root ./workers \
  --timeout 30000
```

#### Environment Variables

| Variable       | Description               | Default    |
| -------------- | ------------------------- | ---------- |
| `DATABASE_URL` | PostgreSQL connection URL | (required) |
| `TASK_TABLE`   | Name of the tasks table   | `ow_tasks` |

#### SQL Schema

Apply this schema to your database (adjust table name as needed):

```sql
CREATE TABLE ow_tasks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    script TEXT NOT NULL,
    payload JSONB,
    status TEXT NOT NULL DEFAULT 'pending',
    result JSONB,
    error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ
);

-- Index for efficient pending task lookup
CREATE INDEX idx_ow_tasks_pending ON ow_tasks(created_at) WHERE status = 'pending';

-- Notification trigger
CREATE OR REPLACE FUNCTION notify_ow_task_created() RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify('ow_tasks_created', NEW.id::text);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ow_task_notify_insert
    AFTER INSERT ON ow_tasks
    FOR EACH ROW EXECUTE FUNCTION notify_ow_task_created();
```

> **Note**: If you use a custom table name, update the trigger to notify on `{table_name}_created`.

#### Inserting Tasks

```sql
-- Script must exist in the --root directory
INSERT INTO ow_tasks (script, payload)
VALUES ('hello.js', '{"name": "world"}');

-- Nested paths are allowed
INSERT INTO ow_tasks (script, payload)
VALUES ('workers/process.js', '{"data": [1, 2, 3]}');
```

#### Task Lifecycle

1. **pending** → Task created, waiting for pickup
2. **running** → Task claimed by executor, `started_at` set
3. **completed** → Success, `result` contains return value
4. **failed** → Error, `error` contains message

## Script API

Scripts must export a default object with a `task` method:

```javascript
export default {
  async task(payload) {
    // payload is the JSON payload passed to the task

    // Use fetch() for HTTP requests
    const response = await fetch("https://api.example.com");

    // Return value is stored as the task result
    return {
      status: "done",
      data: await response.json(),
    };
  },
};
```

### Available APIs

| API                      | Description                           |
| ------------------------ | ------------------------------------- |
| `fetch()`                | Standard Fetch API for HTTP requests  |
| `Request` / `Response`   | Fetch API request/response classes    |
| `Headers`                | HTTP headers manipulation             |
| `URL` / `URLSearchParams`| URL parsing and manipulation          |
| `TextEncoder/Decoder`    | UTF-8 encoding/decoding               |
| `atob()` / `btoa()`      | Base64 encoding/decoding              |
| `crypto.randomUUID()`    | Generate random UUIDs                 |
| `crypto.getRandomValues` | Cryptographic random values           |
| `AbortController`        | Request cancellation                  |
| `Blob` / `FormData`      | Binary data and form handling         |
| `console.log/warn/error` | Logging (printed to stderr)           |
| `setTimeout/setInterval` | Timers (within task execution)        |

## Logging

Enable debug logging with:

```bash
RUST_LOG=debug task-executor run script.js
```

## Multiple Workers

The database listener supports running multiple instances concurrently. Tasks are claimed using `SELECT FOR UPDATE SKIP LOCKED`, ensuring each task is processed exactly once.

```bash
# Terminal 1
task-executor db-listen --database-url $DATABASE_URL

# Terminal 2
task-executor db-listen --database-url $DATABASE_URL

# Both will process tasks without conflicts
```

## Testing

```bash
# Run all tests (requires PostgreSQL for db tests)
cargo test --features database

# Run only database tests
cargo test --features database db_
```

Database tests use `.env.test` for configuration:

```bash
# .env.test
DATABASE_URL=postgres://postgres:postgres@localhost/postgres
```

Tests create isolated tables (`test_tasks_{uuid}`) and clean up automatically.

## License

MIT
