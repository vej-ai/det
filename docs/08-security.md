# 08 — Security

> Part of the det design handbook. See [README.md](./README.md) for the full table of contents.

det is open source and self-hosted. There is no vendor backend, no managed secret vault, no sandbox you inherit for free. That makes security a **design responsibility of the tool and the operator**, not a deployment afterthought. This section is concrete: where secrets live, how they are resolved, and the honest risks of running other people's connectors.

---

## 1. Where secrets live

det enforces one rule above all: **secrets never enter version control.**

| File | Committed to git? | Holds secrets? |
|---|---|---|
| `register.yaml` | **Yes** | **Never.** Declares config *keys*, not values. |
| `det_project.yml` | **Yes** | Never. Project metadata only. |
| `profiles.yml` | **No** — gitignored | Yes — connection config, may reference env vars. |
| `.env` (optional) | **No** — gitignored | Yes — local env var values. |
| `.det/` | **No** — gitignored | State and logs (logs are redacted, see §6). |

`register.yaml` is part of a connector and is meant to be shared, even published to a registry. It must therefore be **value-free** for anything sensitive. It declares that a connector *needs* an `api_key`; it never contains one.

```yaml
# connectors/stripe/register.yaml — committed, no secrets
name: stripe
kind: source
config:
  api_key:   { required: true, secret: true }   # 'secret: true' → redacted everywhere
  page_size: { required: false, default: 100 }
```

The `secret: true` marker is load-bearing: it tells det to redact this value in logs, in `--dry-run` output, and in run records (§6).

---

## 2. `profiles.yml` and `${ENV_VAR}` interpolation

`profiles.yml` is the operator's file. It holds connection config per environment ("target") and is **never committed**. Secrets in it should themselves be indirections — environment variables or secret-manager references — so the file can be reviewed without exposing live credentials.

```yaml
# profiles.yml — gitignored
stripe:
  default: dev
  targets:
    dev:
      api_key: ${STRIPE_API_KEY_DEV}            # env var interpolation
      page_size: 100
    prod:
      api_key: ${STRIPE_API_KEY_PROD}
      page_size: 500

bigquery:                                       # a destination profile
  default: dev
  targets:
    dev:
      type: duckdb
      path: .det/dev.duckdb                # zero-config local dev
    prod:
      type: bigquery
      project: my-gcp-project
      dataset: raw
      credentials: ${GOOGLE_APPLICATION_CREDENTIALS}
```

### Interpolation rules

- `${VAR}` is replaced with the value of environment variable `VAR` at load time.
- `${VAR:-default}` supplies a fallback if `VAR` is unset.
- A `${VAR}` that resolves to nothing for a **required** secret is a hard configuration error (exit code `2` — see [07 §3](./07-cli-and-library-api.md)). det fails *before* running, naming the missing variable. It never silently runs with an empty credential.
- Interpolation is **string-substitution only** — no shell, no command execution. `${...}` cannot run code.

For local development, det auto-loads a gitignored `.env` file from the project root into the environment before interpolation (dotenv convention). In CI and production, the orchestrator/container supplies the variables directly; no `.env` is shipped.

---

## 3. Secret references and pluggable secret managers

Environment variables are the v1 baseline. They are simple and universal, but they put plaintext secrets in the process environment and in CI settings. Teams with stricter requirements want secrets fetched **at run time** from a manager. det supports this with a typed reference syntax and a pluggable resolver.

A `secrets[].ref` value in a connector's `register.yaml` may be a **secret reference** in one of three forms (the original two `${...}` forms PLUS the pluggable URL form, locked at stage 9a):

```yaml
# Pluggable secret-manager URL (stage 9a)
prod:
  api_key:     secret://gcp-secret-manager/projects/my-proj/secrets/stripe-key/versions/latest
  credentials: secret://vault/secret/data/warehouse#service_account
```

A `secret://` URL has the shape `secret://<scheme>/<path>[#<field>]`. At config-resolution time, det parses the URL and hands the `(path, field)` pair to the matching **resolver**:

```python
class SecretResolver(Protocol):
    scheme: ClassVar[str]   # e.g. "gcp", "aws-secrets-manager", "vault"
    def resolve(self, path: str, field: str | None) -> str: ...
```

**Stage 9a ships the core (the `SecretResolver` Protocol, URL parsing, the plugin registry); no live cloud resolver is built in.** A live GCP / AWS / Vault resolver is delivered either by a third-party package (entry-point registration) or by a project-local `det_plugins.py` (see below).

| Resolver | Scheme | Status |
|---|---|---|
| Environment variables | `${env.X}` (built-in syntax) | **v1** |
| Profiles.yml lookup | `${profile.X.Y}` (built-in syntax) | **v1** |
| `secret://` plugin surface | `secret://<scheme>/...` (protocol + parser) | **v1 (stage 9a)** |
| GCP Secret Manager | `secret://gcp-secret-manager/projects/<p>/secrets/<n>/versions/<v>` (plugin) | **v1 (stage 9b)** |
| AWS Secrets Manager | `secret://aws-secrets-manager/...` (plugin) | v2 (9c) |
| HashiCorp Vault | `secret://vault/...` (plugin) | v2 |

### GCP Secret Manager — setup

The `gcp-secret-manager` resolver auto-registers via entry-point when the optional extra is installed. One-time setup on a GCP project:

1. **Create the secret + a version** (using `gcloud`):

   ```sh
   gcloud secrets create my-stripe-key --replication-policy=automatic
   echo -n 'sk_live_xxx' | gcloud secrets versions add my-stripe-key --data-file=-
   ```

2. **Grant access** to the principal det runs as (service account in CI, your user ADC locally):

   ```sh
   gcloud secrets add-iam-policy-binding my-stripe-key \
     --member='serviceAccount:det-runner@<proj>.iam.gserviceaccount.com' \
     --role='roles/secretmanager.secretAccessor'
   ```

3. **Install the extra**: `pip install 'det[gcp-secrets]'`.

4. **Reference it in `profiles.yml`**:

   ```yaml
   stripe:
     prod:
       api_key: secret://gcp-secret-manager/projects/my-proj/secrets/my-stripe-key/versions/latest
   ```

Authentication uses Application Default Credentials — set `GOOGLE_APPLICATION_CREDENTIALS` or run `gcloud auth application-default login`. The `#field` URL fragment is ignored (GCP returns a single opaque blob per version); a one-time warning is logged per unique `(path, field)` pair.

The `SecretResolver` protocol and `secret://` parsing exist so a manager can be added as a small package or a project-local plugin **without an engine change** — the same extensibility philosophy as the `StateBackend` in [05](./05-destinations-and-state.md). Resolved secret values are held only in memory for the duration of the run and are subject to the redaction rules in §6.

> Resolved-secret caching (Q11) is deferred: stage 9a commits to **fresh-every-run** (no on-disk cache). The per-process resolver INSTANCE is cached after first use (a GCP SDK init only runs once), but the per-reference value is re-fetched on every run.

### Writing a custom resolver

A resolver is any object whose class declares a `scheme: ClassVar[str]` and implements `resolve(path, field) -> str`. Register a factory (a zero-arg callable returning a resolver instance) under a scheme:

```python
# my_pkg/my_resolver.py
from typing import ClassVar
import det

class MyVaultResolver:
    scheme: ClassVar[str] = "my-vault"
    def resolve(self, path: str, field: str | None) -> str:
        # Talk to your manager here; never embed the value in any exception text.
        return _fetch_from_vault(path, field)

def factory() -> MyVaultResolver:
    return MyVaultResolver()
```

There are **two ways** to surface a resolver to det:

1. **Entry-point** (for distributable packages). In `pyproject.toml`:

   ```toml
   [project.entry-points."det.secret_resolvers"]
   my-vault = "my_pkg.my_resolver:factory"
   ```

   The entry-point NAME is the scheme; the value is a `module:factory` pointing at the zero-arg factory. Loaded lazily — only on the first `secret://my-vault/...` reference.

2. **Project-local `det_plugins.py`** (no packaging required). Drop a file at the project root (next to `det_project.yml`):

   ```python
   # det_plugins.py
   from typing import ClassVar
   import det

   class MyResolver:
       scheme: ClassVar[str] = "my-scheme"
       def resolve(self, path, field):
           return _fetch(path, field)

   det.register_secret_resolver("my-scheme", MyResolver)
   ```

   The file runs once per project per process at engine startup. It is arbitrary Python with the engine's privileges — same trust model as `sources/<name>/source.py`.

**Resolution precedence**: project-local registration always wins over an entry-point of the same scheme. Explicit beats implicit (same rule as project-local connectors shadowing baked ones, [03 §5](./03-connector-contract.md)).

### Verifying resolution with `det secrets test`

The `det secrets test` command resolves every declared reference and reports `✓` / `✗` per reference WITHOUT printing the resolved value — the operator can verify "my creds are wired up right" without leaking what they are. See [07 — CLI and Library API](./07-cli-and-library-api.md) for the full surface.

---

## 4. `.gitignore` defaults

`det init` writes a `.gitignore` that pre-empts the most common credential leaks. A fresh project is safe by default:

```gitignore
# det — generated by `det init`
profiles.yml          # connection config & secrets — NEVER commit
.env                  # local environment variables
*.env
.det/            # run state, logs, local DuckDB files
*.duckdb
__pycache__/
*.pyc

# allow a committable template so teammates know the shape
!profiles.example.yml
```

`det init` also drops a `profiles.example.yml` with the structure but placeholder values — this *is* committed, so a new teammate sees the expected shape without seeing a real key. det prints a one-line reminder after `init`: *"profiles.yml is gitignored — never commit real credentials."*

A lightweight `det test` (and CI) scans `profiles.yml`'s tracked status: if `profiles.yml` is somehow tracked by git, it emits a loud warning. det cannot prevent a determined mistake, but it makes the safe path the default and the unsafe path noisy.

---

## 5. File permissions on `profiles.yml`

A secrets file readable by every user on the host is a leak. On creation (`det init`) and whenever det writes `profiles.yml` or `.env`, it sets mode `0600` (owner read/write only). On every run, det **checks** the mode of `profiles.yml`:

- World- or group-readable (`o+r` / `g+r`) → a `[warn]` log line: *"profiles.yml is readable by other users — run `chmod 600 profiles.yml`."*
- This is a warning, not a hard failure: in some container setups the file is mounted read-only with broader bits and the operator has accepted that. det informs; the operator decides.

The same `0600` expectation applies to `.env` and to any on-disk state that could contain a resolved secret. det never writes resolved secret *values* to disk (see §3 open question).

---

## 6. Redaction in logs and run records

[09 — Logging and Observability](./09-logging-and-observability.md) specifies the log format; here is the security contract it must honor.

- Every config key marked `secret: true` in `register.yaml`, and every value that arrived via `${ENV_VAR}` or `secret://`, is **redacted** to `***` in: stdout logs, `.det/logs/` files, `--dry-run` config dumps, run records, and exception messages.
- Redaction is by **value**, not just by key: det builds a set of known secret values at run start and scrubs any occurrence of them from log strings before they are written. This catches a secret that leaks into, say, an HTTP error body echoed by a connector.
- Connector code receives the *real* secret (it must, to authenticate) but the engine's logging layer sits between connector output and the log sink. A connector that deliberately `print()`s a secret bypasses redaction — see the trust model below.
- URLs are redacted of userinfo and query strings that match secret values.

Redaction is best-effort and value-based; it is a strong safety net, not a guarantee against a hostile connector.

---

## 7. Trust model — running third-party connectors

This is the most important and most under-appreciated security fact about det, and the handbook will not soft-pedal it:

> **A connector is arbitrary Python. Running a connector runs its code on your machine, with your privileges, with access to your credentials.**

det imports and executes `@stream` / `@destination` functions in-process. A community connector you `pip install` or copy into `connectors/` can read your `profiles.yml`, exfiltrate credentials, read any file your user can read, and make any network call. This is the same trust model as installing any PyPI package or a dbt package with macros — but it must be stated plainly because EL connectors are *expected* to handle credentials.

**det does not sandbox connector code in v1.** Claiming otherwise would be dishonest — true sandboxing (subprocess isolation, seccomp, containers, capability dropping) is hard, leaky, and out of scope for the v1 simplicity bar. What det does instead:

1. **Provenance is explicit.** Pre-baked connectors ship inside the `det` package and are reviewed as part of the project. Project-local connectors in `connectors/` are *your* code. A connector from anywhere else is third-party — treat it like any untrusted dependency.
2. **Read the code.** A connector is a small folder of plain Python. Unlike an Airbyte connector image, it is *meant* to be read before you run it. The folder-of-Python design is itself a security feature: auditability.
3. **`--allow-unsafe-connectors` gate (v2).** A planned flag so that running a connector whose source is outside the project or the baked set requires explicit opt-in. Without the flag, det refuses to execute an unrecognized-provenance connector. This does not *sandbox* — it prevents *accidental* execution of untrusted code.
4. **Least-privilege credentials.** The strongest practical mitigation, and operator-side: give each connector a credential scoped to exactly what it needs (a read-only API token, a warehouse role that can write only its own dataset). If a connector is malicious, the blast radius is that credential. det's per-connector config makes this natural — every connector has its own credential block.
5. **Signed connector registry (v3).** If a public connector registry/marketplace materializes (see [10 — Roadmap](./10-roadmap-and-scope.md)), connectors would be signed and checksum-pinned, so what you audited is what you run. This is a future feature, not a v1 promise.

> [Open question: is opt-in subprocess isolation worth building for v2 — run each connector in a child process with a restricted environment (scrubbed env vars, no `profiles.yml` access, only its own resolved config passed in)? It would not stop a determined attacker but would contain accidents and reduce blast radius. It costs IPC complexity and breaks the in-process simplicity. Proposal: prototype after v1; decide based on whether a real community-connector ecosystem emerges.]

**Bottom line for the operator:** treat a det connector exactly as you treat any third-party Python dependency. Pin versions, read the code of anything you did not write, scope every credential to least privilege, and prefer the pre-baked connectors when they exist.

### Reference

- `profiles.yml` precedence in config resolution → [07 — CLI and Library API](./07-cli-and-library-api.md)
- Log redaction implementation → [09 — Logging and Observability](./09-logging-and-observability.md)
- Connector folder layout (`register.yaml`) → [03 — The Connector Contract](./03-connector-contract.md)
- Registry / marketplace direction → [10 — Roadmap and Scope](./10-roadmap-and-scope.md)
