# Security policy

EmitLane has published pre-1.0 releases; v0.7.0 is the current released baseline.
When reporting a vulnerability, identify the affected tag and whether it also
affects the latest release. A long-term support window and backport policy have
not yet been established; do not assume older minor versions receive fixes.
Development branches are not released or qualified versions.

## Reporting a vulnerability

Please report suspected security vulnerabilities privately through
[GitHub Security Advisories](https://github.com/EmitLane/emitlane/security/advisories/new).
Do not open a public issue or discussion for a vulnerability.

Include the affected version or commit, impact, reproduction steps, and any
suggested mitigation you can safely share. Do not include live credentials,
production event payloads, or other sensitive data. The maintainers will
acknowledge the report and coordinate disclosure and remediation through the
private advisory.

## Security-sensitive areas

- database credentials;
- broker credentials;
- TLS configuration;
- Admin API authorization;
- event payload/PII exposure;
- logs;
- replay/retry control operations;
- SQL migration privileges.

## Default safety direction

- Admin API off by default;
- raw payload not logged;
- no superuser requirement;
- non-root container where practical;
- secrets provided via environment/secret manager, not committed config;
- Admin API operator mutations are audit logged.
