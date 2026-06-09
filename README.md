# PHARMACY-PRO-01

Pharmacy management API with backend-enforced tenant isolation.

## Tenant and demo isolation

At startup, records without a `tenant_id` are assigned to `REAL_TENANT_ID` (default: `real_shop`) without deleting existing records. The seeded demo account uses `DEMO_TENANT_ID` (default: `demo_shop`) and receives its own distributors, medicines, customers, purchase order, invoice, ledger transactions, expense, and daily summary. All business collection access is scoped by the authenticated user's tenant in the backend, and demo writes are rejected with HTTP 403.

Demo credentials can be configured with `DEMO_EMAIL` and `DEMO_PASSWORD`. Defaults are `demo@pharmacy.com` and `DemoAccess123`; production deployments should explicitly set them.

## Password reset email configuration

The forgot-password flow stores only an HMAC hash of the six-digit OTP, expires it after ten minutes, limits verification attempts, and rate-limits requests. To deliver OTPs, configure:

- `SMTP_HOST`
- `SMTP_PORT` (default: `587`)
- `SMTP_FROM`
- `SMTP_USERNAME` and `SMTP_PASSWORD` when authentication is required
- `SMTP_STARTTLS` (default: `true`)

Without `SMTP_HOST` and `SMTP_FROM`, `/api/auth/forgot-password` returns a generic response with `delivery_configured: false` and an SMTP setup TODO. It never returns the OTP.
