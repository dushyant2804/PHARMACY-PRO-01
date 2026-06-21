# PHARMACY-PRO-01

Pharmacy management API with backend-enforced tenant isolation.

## Tenant and demo isolation

At startup, records without a `tenant_id` are assigned to `REAL_TENANT_ID` (default: `real_shop`) without deleting existing records. The seeded demo account always uses the fixed `demo_shop` tenant/shop and receives its own settings, distributors, medicines, customers, purchase order, invoice, ledger transactions, expense, daily sale, and daily summary. All business collection access is scoped by the authenticated user's tenant in the backend, and demo writes are rejected with HTTP 403.

Demo credentials can be configured with `DEMO_EMAIL` and `DEMO_PASSWORD`. The demo identity is always isolated in `demo_shop` and is never returned by real-pharmacy User Management.

## Optional initial admin seeding

No real-pharmacy admin is created automatically. To explicitly seed an initial admin into `REAL_TENANT_ID`, set all three variables:

- `SEED_ADMIN=true`
- `ADMIN_EMAIL`
- `ADMIN_PASSWORD` (at least 10 characters with uppercase, lowercase, and a number)

There are no fallback admin credentials. Missing, weak, or known unsafe default credentials cause explicitly enabled admin seeding to fail.

## Password reset email configuration

The forgot-password flow stores only an HMAC hash of the six-digit OTP, expires it after ten minutes, limits verification attempts, and rate-limits requests. To deliver OTPs, configure:

- `SMTP_HOST`
- `SMTP_PORT` (default: `587`)
- `SMTP_FROM`
- `SMTP_USERNAME` and `SMTP_PASSWORD` when authentication is required
- `SMTP_STARTTLS` (default: `true`)

Without `SMTP_HOST` and `SMTP_FROM`, `/api/auth/forgot-password` returns a generic response with `delivery_configured: false` and an SMTP setup TODO. It never returns the OTP.

## Local-first runtime mode

PharmacyOS supports two runtime modes without changing existing API contracts:

- `CLOUD_MODE` (default): existing Render + MongoDB Atlas behavior. `MONGO_URL` and `DB_NAME` are required exactly as before.
- `LOCAL_MODE`: local backend with a SQLite database for a single shop PC. Set `PHARMACYOS_MODE=LOCAL_MODE`; optionally set `LOCAL_DB_PATH` and `BACKUP_DIR`.

Local mode stores JSON documents in SQLite through a Mongo-like adapter so the existing routes, fields, calculations, invoices, ledgers, reports, auth, and settings continue to call the same collection methods. Cloud mode still uses Motor/MongoDB unchanged.

### Backup and sync safety

Local mode writes timestamped JSON backups and queues cloud-sync work instead of overwriting cloud data blindly:

- `POST /api/backup/manual` creates a manual backup.
- Creating a Daily Closing triggers a background backup.
- App shutdown creates an exit backup when local mode is active.
- A scheduled backup runs every 30 minutes while the backend is open.
- `GET /api/backup/health` reports local backend/database status, last backup, pending backup count, and cloud reachability.
- `POST /api/backup/sync/retry` performs a safe dry-run queue check; destructive cloud restore remains intentionally manual.

### Migration/export flow

- `GET /api/backup/export` exports the current tenant data from Atlas/cloud or local mode.
- `POST /api/backup/import?dry_run=true` verifies import counts without writing data.
- `POST /api/backup/import?dry_run=false` imports supported collections into the active runtime database and returns count verification for medicines, invoices, purchase orders, distributors, customers, ledgers, returns, and settings.

Always perform the dry-run verification first and keep the generated timestamped backup file before any import.

### Restore and integrity guarantees

Every local backup is checked immediately after creation: the backend verifies that the file exists, that it is non-empty, and stores its SHA-256 checksum, byte size, timestamp, collection counts, and upload-file count in backup metadata. Backup payloads include files under `UPLOAD_DIR` (including branding/logo/signature uploads) with relative paths, sizes, checksums, and encoded file content.

`POST /api/backup/restore` is dry-run by default. It validates the backup path, JSON structure, optional `expected_sha256`, collection counts, and upload checksums before any data is changed. A confirmed restore (`dry_run=false&confirm=true`) first creates a `pre_restore` backup of the active local database, then restores supported collections and upload files.

Cloud sync retry remains dry-run unless explicitly confirmed and is intentionally non-destructive: the endpoint reports pending queue state and refuses blind overwrite semantics so newer cloud data cannot be replaced without a future conflict-reviewed sync implementation.
