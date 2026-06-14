# Security

## Threat Model

Apache Nutch's full security threat model — scope and intended use, the
adversary model, the security properties the project provides and disclaims,
known non-findings (recurring false positives), and triage dispositions — is in
[THREAT_MODEL.md](./THREAT_MODEL.md). It is written as a superset of the
security model published on the project website.

The project website remains the operator-facing reference:

- **Security model (operational guidance)**:
  <https://nutch.apache.org/documentation/security/#security-model>
- **Security policy, vulnerability reporting, advisories, and CVEs**:
  <https://nutch.apache.org/documentation/security/>

The project website is the authoritative source for the disclosure process and
operational hardening; `THREAT_MODEL.md` is the in-repo model that automated
tooling and triagers can follow mechanically.

Please report any security issues to security@apache.org.
