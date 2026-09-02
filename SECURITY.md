# Security policy

EmitLane is currently unreleased. No production version is supported yet. This
policy will be updated with supported-version details before the first widely
announced release.

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
- operator mutations audit logged when Admin API ships.
