# Security Policy

## Reporting a vulnerability

Please report security issues **privately** via a
[GitHub security advisory](https://github.com/corruptmane/natsio/security/advisories/new)
— never a public issue or discussion.

You'll get an acknowledgement, and once a fix is available it ships in a patch
release with credit (unless you'd prefer to stay anonymous).

## Supported versions

The core's 1.x line is supported; security fixes ship in its latest patch
release. Upgrade to the latest 1.x release for fixes. Pre-1.0 core releases
are unsupported. Extensions version independently and currently remain
pre-1.0; only their latest releases receive fixes.

Python 3.13–3.15 on Linux and macOS is covered by CI. NATS Server 2.14.0
is the minimum tested baseline; current stable NATS is also a merge gate.
Upstream development is a separate scheduled compatibility check, not a
support guarantee. Windows is not currently covered by the test matrix.

## Scope notes

natsio ships **no cryptography of its own** — NKey/JWT signing is delegated to
PyNaCl or `cryptography` via the `natsio[nkeys]` / `natsio[cryptography]`
extras. Vulnerabilities in those backends should be reported upstream; report
here only if natsio *uses* them unsafely.

TLS discovery is committed only after a successful authenticated handshake.
Discovery received before an in-band TLS upgrade is discarded; the encrypted
INFO refresh supplies topology. Completing TLS does not authenticate earlier
plaintext bytes.
Use TLS for every configured endpoint when credentials must always travel
encrypted; an explicitly configured plaintext endpoint remains an opt-in to
plaintext. Diagnostics omit URL credentials, but configuration accessors such
as `connected_url` and `ConnectOptions.servers` retain their original values.
Do not log those raw values or credential-provider exceptions that include
secrets. Never include real credentials in a vulnerability report.
