# Security Policy

dtex is open source and self-hosted. There is no managed backend; the engine,
the baked connectors, and the contract enforcement all run on your machines.
This document covers **how to report a vulnerability** and **what is in
scope**. For the full threat model — secrets handling, redaction, the
third-party-connector trust model — see
[`docs/08-security.md`](./docs/08-security.md).

## Supported versions

We support the most recent minor release line with security updates.

| Version | Supported          |
|---------|--------------------|
| 0.1.x   | Yes                |
| < 0.1   | No                 |

Once a newer minor (`0.2.x`, `1.0.x`) is released, the previous line will be
supported for at least 90 days for critical security fixes.

## Reporting a vulnerability

**Please do not file public GitHub issues for security problems.**

Two ways to report, in order of preference:

1. **GitHub private Security Advisory** (preferred) — open a draft advisory
   at
   <https://github.com/vej-ai/dtex/security/advisories/new>.
   This keeps the report private until a fix is ready and lets us
   coordinate a CVE if appropriate.

2. **Email** — `hello@vej.ai`. Include a description of the issue, the
   dtex version (`dtex --version`), reproduction steps, and any proof-of-
   concept code. PGP is not currently required.

### What to expect

- **Acknowledgement** within **7 days** of your report.
- A first assessment (confirmed / not reproducible / out of scope) within
  **14 days**.
- A fix or coordinated disclosure within **90 days** of acknowledgement for
  confirmed in-scope vulnerabilities. Complex issues may take longer; if so
  we will tell you and agree on a revised timeline.

We credit reporters in release notes by default. If you prefer to remain
anonymous, please say so in your report.

## Scope

**In scope** — the dtex engine and its baked artifacts:

- The engine and CLI (`dtex run`, `dtex validate`, `dtex state`, `dtex runs`,
  `dtex secrets test`, etc.).
- The connector contract: discovery, config resolution, manifest validation,
  the `@stream` / `@resource` / `@destination` decorators.
- The baked source connectors: `filesystem`, `rest`, `postgres`, `shiphero`,
  `stripe`.
- The baked destination connectors: `duckdb`, `bigquery`.
- The baked secret-manager resolvers: `gcp-secret-manager`,
  `aws-secrets-manager`, `vault`.
- Log redaction, the `_dtex_state` / `_dtex_runs` tables, the run-record
  surface.

Examples of in-scope issues: a secret leaking into a log line or run
record, a config-resolution bug that lets one target read another's
state, a path-traversal in `dtex init` / `dtex new`, a resolver that
returns a secret value to a calling-side error message, a destination
write path that can corrupt or partially commit data without rolling back.

**Out of scope** — issues whose root cause is in a third-party package:

- Bugs in `google-cloud-bigquery`, `google-cloud-storage`, `psycopg`,
  `requests`, `boto3`, `hvac`, `pyarrow`, `duckdb`, `pyyaml`, or any other
  dependency listed in `pyproject.toml`. Please report those upstream; if
  the bug has a dtex-side mitigation, we will accept a separate report on
  that mitigation.
- The security posture of a **third-party connector** you installed from
  outside the baked set. dtex does not sandbox connector code
  (see [`docs/08-security.md` §7](./docs/08-security.md)). A malicious
  connector running with your credentials is a known limitation of the v1
  trust model, not a vulnerability in dtex itself.
- Operational misconfiguration: a committed `profiles.yml`, a world-readable
  `.env`, an over-broad warehouse role. dtex documents the safe defaults
  ([`docs/08-security.md` §4–5](./docs/08-security.md)) and warns when it
  can detect drift; correct operation is on the operator.

If you are unsure whether something is in scope, **report it anyway** — we
would rather triage and decline than miss a real issue.

## See also

- [`docs/08-security.md`](./docs/08-security.md) — full threat model,
  redaction contract, secret-manager resolver protocol, connector trust
  model.
- [`CODE_OF_CONDUCT.md`](./CODE_OF_CONDUCT.md).
- [`CONTRIBUTING.md`](./CONTRIBUTING.md).
