# Security Policy

## Reporting a Vulnerability

Apache Kvrocks follows the [Apache Software Foundation security process](https://www.apache.org/security/).
Please report suspected vulnerabilities **privately** to `security@apache.org` (the Kvrocks PMC is reachable
at `private@kvrocks.apache.org`). Do **not** open public GitHub issues or pull requests for security reports.

When reporting, include the affected version, a description, and — if you can — which security property you
believe is violated (see the Threat Model below) and a reproduction.

## Threat Model

What Kvrocks considers in scope and out of scope, the security properties it claims and the ones it explicitly
disclaims (namespace isolation, admin/namespace token separation, the Lua sandbox, the no-auth/no-TLS defaults),
the adversary model, and how inbound reports and tool/AI findings are triaged are documented in
[THREAT_MODEL.md](./THREAT_MODEL.md). Reporters and triagers should consult it alongside this policy.
