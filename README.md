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

When the local backend is running on the default port, the frontend health check should target `http://localhost:8000/api/health`. The root health alias `GET /health` returns the same local server status for compatibility, and startup logs include `Local PharmacyOS server running at http://localhost:8000`.

### Windows local desktop launcher

Windows desktop users do not need to type terminal commands to start the local backend. Use the quiet desktop launcher for day-to-day use and keep the visible BAT launcher for troubleshooting:

1. Double click `PharmacyOS-Launch.vbs` for the desktop-software experience. It runs `PharmacyOS-Desktop-Start.bat` hidden, starts the local backend in the background, waits until `http://127.0.0.1:8000/api/health` reports `LOCAL_MODE`, then opens `http://127.0.0.1:8000` in a Chrome app window. If Chrome is not installed in a standard Windows location, it falls back to the default browser.
2. If startup fails, `PharmacyOS-Launch.vbs` shows a short troubleshooting message. Review `logs\pharmacyos-local.log` and `logs\pharmacyos-backend-output.log`, or double click `PharmacyOS-Start.bat` to run the original visible launcher and see detailed console output.
3. To stop safely, double click `PharmacyOS-Stop.bat`; it asks the local backend to create an app-exit backup before stopping the backend process.

To create a desktop shortcut with an icon:

1. Right click `PharmacyOS-Launch.vbs` and choose **Send to > Desktop (create shortcut)**.
2. On the desktop, right click the new shortcut, choose **Properties**, and keep **Target** pointed at `PharmacyOS-Launch.vbs`.
3. Choose **Change Icon...** and select a PharmacyOS `.ico` file if one is included with the desktop package, or select an icon from a Windows file such as `%SystemRoot%\System32\shell32.dll`.
4. Rename the shortcut to **PharmacyOS**.

Existing shortcuts remain compatible: `start-pharmacyos-local.bat` calls `PharmacyOS-Start.bat`, and `stop-pharmacyos-local.bat` calls `PharmacyOS-Stop.bat`. `PharmacyOS-Start.bat` is intentionally still visible so support staff can inspect startup messages and backend errors.

The launcher uses the existing SQLite local database path `local_data\pharmacyos.sqlite3` so existing local data is untouched. It also keeps the existing backup and upload paths beside the application: `backups\` and `uploads\`. Startup, stop, and quiet-launcher status messages are appended to `logs\pharmacyos-local.log`; backend output from the quiet launcher is appended to `logs\pharmacyos-backend-output.log`.

### Local desktop folder structure

The Windows desktop package keeps local runtime folders beside the application:

- `local_data\` stores the active SQLite database (`pharmacyos.sqlite3`) and local auth/sync tokens. This is the existing local database location and must not be deleted during launch, stop, or future updates.
- `data\` is reserved for future desktop-package metadata and updater staging. Auto-update is not implemented yet.
- `backups\` stores timestamped local JSON and package backups.
- `uploads\` stores local upload files such as branding, logo, and signature assets.
- `logs\` stores launcher/backend logs, including `logs\pharmacyos-local.log`.

### Backup and sync safety

Local mode writes timestamped JSON backups locally first, then attempts non-destructive cloud backups to MongoDB Atlas and Google Drive. Cloud mode remains unchanged.

- `POST /api/backup/manual` creates a manual backup and attempts Atlas plus Google Drive upload.
- `POST /api/backup/exit` creates an app-exit backup and attempts Atlas plus Google Drive upload.
- Creating a Daily Closing triggers a background local backup and cloud upload attempt.
- App shutdown creates an exit backup when local mode is active.
- A scheduled backup runs every 30 minutes while the backend is open.
- `GET /api/backup/health` reports local, Atlas, and Google Drive status, last successful timestamps, and pending queue counts.
- `GET /api/backup/status` is a compatibility alias for `GET /api/backup/health`.
- `POST /api/backup/sync/retry` retries pending Atlas and Google Drive queue entries.
- `POST /api/backup/google-drive/device-login` starts Google OAuth device-code login for a local Windows desktop, and `POST /api/backup/google-drive/device-token` stores the resulting token locally.

Atlas backup uses `ATLAS_BACKUP_MONGO_URL` and stores timestamped records in `local_backup_snapshots` without replacing existing snapshots. Google Drive backup uses `GOOGLE_DRIVE_CLIENT_ID`, optional `GOOGLE_DRIVE_CLIENT_SECRET`, and `GOOGLE_DRIVE_TOKEN_PATH`; uploaded backup packages go into a `PharmacyOS` Drive folder. Set `BACKUP_ENCRYPTION_KEY` to encrypt the packaged JSON backup before Drive upload. Failed Atlas or Google Drive uploads remain queued locally and are retried by scheduled/manual/exit backup flows or `/api/backup/sync/retry`.

### Migration/export flow

- `GET /api/backup/export` exports the current tenant data from Atlas/cloud or local mode.
- `POST /api/backup/import?dry_run=true` verifies import counts without writing data.
- `POST /api/backup/import?dry_run=false` imports supported collections into the active runtime database and returns count verification for medicines, invoices, purchase orders, distributors, customers, ledgers, returns, and settings.

Always perform the dry-run verification first and keep the generated timestamped backup file before any import.

### Restore and integrity guarantees

Every local backup is checked immediately after creation: the backend verifies that the file exists, that it is non-empty, and stores its SHA-256 checksum, byte size, timestamp, collection counts, and upload-file count in backup metadata. Backup payloads include files under `UPLOAD_DIR` (including branding/logo/signature uploads) with relative paths, sizes, checksums, and encoded file content.

`POST /api/backup/restore` is dry-run by default. It validates the backup path, JSON structure, optional `expected_sha256`, collection counts, and upload checksums before any data is changed. A confirmed restore (`dry_run=false&confirm=true`) first creates a `pre_restore` backup of the active local database, then restores supported collections and upload files.

Cloud sync retry is intentionally non-destructive: retry inserts timestamped backup records/files only, marks successful queue items complete, and keeps failed Atlas or Google Drive uploads pending for the next retry.
