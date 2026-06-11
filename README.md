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
