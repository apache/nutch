# Agent guidance

This file is read by automated agents (security scanners, code
analyzers, AI assistants) operating on this repository. It
points them at the human-authored references they should
consult before producing output.

## Security

Security model: [SECURITY.md](./SECURITY.md) -> [THREAT_MODEL.md](./THREAT_MODEL.md)
(the project's full threat model, a superset of the website security model at
<https://nutch.apache.org/documentation/security/#security-model>).

Agents that scan this repository should consult `THREAT_MODEL.md` for the
project's in-scope / out-of-scope declarations, the security properties it
provides and disclaims, and the known non-findings (recurring false positives)
before reporting issues. Note: Apache Nutch fetches and parses untrusted web
content by design — that is the crawler's function, not a vulnerability; see
`THREAT_MODEL.md` §3, §9, and §11a.
