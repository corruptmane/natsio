# Security Policy

## Reporting a vulnerability

Please report security issues **privately** via a
[GitHub security advisory](https://github.com/corruptmane/natsio/security/advisories/new)
— never a public issue or discussion.

You'll get an acknowledgement, and once a fix is available it ships in a patch
release with credit (unless you'd prefer to stay anonymous).

## Supported versions

natsio is pre-1.0; only the latest released version receives fixes. Once 1.0
ships, this section will name the supported release lines.

## Scope notes

natsio ships **no cryptography of its own** — NKey/JWT signing is delegated to
PyNaCl or `cryptography` via the `natsio[nkeys]` / `natsio[cryptography]`
extras. Vulnerabilities in those backends should be reported upstream; report
here only if natsio *uses* them unsafely.
