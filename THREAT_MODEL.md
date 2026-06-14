<!--
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Apache Nutch — Security Threat Model

## §1 Header

- **Project:** Apache Nutch (core crawler — `apache/nutch`)
- **Modeled against:** `master` HEAD as of 2026-06-06.
- **Authors:** ASF Security team (v0 draft, generated via the
  `threat-model-producer` rubric at the Nutch PMC's request), for the PMC to review.
- **Status:** **DRAFT v0 — draft-first, not yet maintainer-ratified.** Built as a
  superset of the project's existing website security model; the sections that
  were not covered there are *(inferred)* and must be confirmed (see §14).
- **Version binding:** versioned with the project; a report against release *N*
  is triaged against the model as it stood at *N*.
- **Reporting cross-reference:** findings that violate a §8 property should be
  reported privately per the project's disclosure channel (`security@apache.org`);
  findings under §3 or §9 are closed citing this document.
- **Provenance legend:** *(documented)* = stated in Nutch's docs/website security
  model; *(maintainer)* = confirmed by a Nutch PMC member; *(inferred)* = reasoned
  from docs/code/domain knowledge, not yet confirmed (each has a §14 question).
- **Draft confidence:** ~10 documented / 0 maintainer / ~22 inferred.
- **Relationship to the website model:** this document is a strict superset of
  the security model published at
  <https://nutch.apache.org/documentation/security/#security-model>; nothing that
  page asserts is dropped or weakened here. Where this draft adds a section the
  page did not cover (adversary model, enumerated properties, known non-findings,
  triage dispositions), it is tagged *(inferred)* pending PMC confirmation.

**What Nutch is.** Apache Nutch is an extensible, Hadoop-based open-source web
crawler: an operator runs it (in local or distributed/batch mode, or via the
`nutch-server` REST control API) to fetch, parse, and index web content at
scale through a plugin architecture (protocol / parse / index / scoring
plugins; parsing largely via Apache Tika). The defining security fact is that
**Nutch fetches and parses content from the open, untrusted web by design** —
the crawled bytes are attacker-controllable input, and the threat model is about
robustly handling that input and about which controls are the operator's job,
not about preventing Nutch from reaching or parsing hostile content.

## §2 Scope and intended use

- **Primary intended use** *(documented)*: an operator-deployed crawler run
  **in a trusted environment**, fetching web content into a crawl store and
  handing parsed content to an indexer. The website model states Nutch is
  "designed to operate in trusted environments, either locally or on a Hadoop
  cluster."
- **Deployment shape** *(documented/inferred)*: batch crawl jobs (local or on
  Hadoop), plus an optional `nutch-server` REST API for orchestration.
- **Caller roles:**
  - **operator / admin** — trusted; owns seeds, URL filters, plugin config, the
    Hadoop/host environment, and the `nutch-server` endpoint.
  - **crawled-content supplier** — the untrusted web: every fetched page,
    redirect, `robots.txt`, sitemap, feed, and embedded resource is
    attacker-controllable input. *(inferred — the core in-model adversary.)*
  - **REST API client** — in the trusted environment per the website model; the
    legacy REST API provided no authentication. *(documented)*

**Component-family table** *(inferred — confirm in §14)*:

| Family | Entry point | Touches outside process? | In model? |
| --- | --- | --- | --- |
| Fetcher / protocol plugins | `protocol-http(client)`, etc. | network (arbitrary URLs) | **yes** — consumes untrusted content |
| Parser plugins (Tika, HTML, feed, etc.) | `parse-*` | CPU/memory on untrusted bytes | **yes** |
| URL filtering / normalization | `urlfilter-*`, `urlnormalizer-*` (regex) | — | **yes** (scoping boundary) |
| Crawl store / DB | local FS / HDFS | disk | **yes** |
| Indexer plugins | `indexer-solr`, `-elastic`, … | network (backend) | **boundary** — backend is the operator's |
| `nutch-server` REST API | HTTP control endpoint | network | **yes** |
| Bundled/contrib plugins, examples | various | varies | **per-plugin** — confirm supported set (§14) |

## §3 Out of scope (explicit non-goals)

- **Defending a Nutch deployment exposed outside a trusted environment** (e.g.
  an internet-reachable `nutch-server` with no fronting auth). The website model
  scopes Nutch to trusted environments. *(documented)*
- **Preventing Nutch from fetching arbitrary or internal URLs.** Reaching URLs
  is the crawler's function; restricting *which* URLs is the operator's job via
  URL filters / seed scoping (so crawler "SSRF" is operator-config, see §9/§11a).
  *(inferred)*
- **The security of indexer/storage backends** (Solr/Elasticsearch/HDFS) and the
  Hadoop cluster — Nutch writes to them; it does not own their security.
  *(inferred)*
- **Contrib / unsupported plugins / examples** — threat-modeled separately;
  confirm the supported plugin set in §14. *(inferred)*

## §4 Trust boundaries and data flow

Two boundaries matter, and they are different from a typical service:
1. **The fetch boundary (primary).** Everything Nutch fetches from the web is
   untrusted and crosses into the parser/store. The trust transition is
   "bytes-from-the-internet → parsed structures." *(inferred)*
2. **The operator/config boundary.** Seeds, URL filters, plugin selection, and
   the `nutch-server` endpoint are operator-controlled and trusted. *(inferred)*

Data flow: seed URLs (operator) → fetch (untrusted content) → parse (Tika/plugins
on untrusted bytes) → URL extraction + filtering → crawl DB → index to backend.

**Reachability preconditions per family:**
- A fetcher/parser finding is in-model when it is reachable **from
  attacker-controlled fetched content** (a hostile page/feed/redirect) — that is
  the core in-model surface.
- A `nutch-server` REST finding is in-model only if it is reachable by a network
  client the deployment was not supposed to expose (and the website model assumes
  a trusted environment).
- A finding that requires control of seeds / URL filters / plugin config is
  **out of model** (operator-trusted input).

## §5 Assumptions about the environment

- **Trusted operator environment** *(documented)*: Nutch runs where only trusted
  operators reach its control surfaces; HTTPS / network isolation for any exposed
  endpoint is the operator's responsibility.
- **Operator-controlled config** *(inferred)*: seeds, `regex-urlfilter`,
  normalizers, plugin set, and backend credentials are trusted inputs.
- **Backends provisioned by the operator** *(inferred)*: Solr/Elastic/HDFS.
- **What Nutch does to its host** *(inferred — §14)*: opens outbound network
  connections to arbitrary fetched hosts; reads/writes the crawl store; runs
  parser libraries over untrusted bytes; with `nutch-server`, opens a listening
  HTTP port. Not expected to run as root.

## §5a Build-time and configuration variants

Config knobs that move the security envelope *(inferred — confirm in §14)*:

| Knob | Default | Effect | Maintainer stance |
| --- | --- | --- | --- |
| `nutch-server` REST API | not started unless launched; **no auth** | An exposed control endpoint with no auth on an untrusted network = unauthenticated control | **?** trusted-env-only posture — §14.1 |
| URL filters / scope (`regex-urlfilter.txt`, domain/host filters) | permissive sample; operator-tuned | Without scoping, the crawler will follow links anywhere, including internal hosts | **?** operator responsibility (§10) — §14.2 |
| `protocol-*` plugin (e.g. `protocol-httpclient`) + SSL/redirect handling | per-plugin | TLS verification / redirect-following behavior on untrusted hosts | **?** §14.3 |
| `db.max.outlinks`, fetcher size/time limits, parse limits | defaults exist | Bound resource use on hostile/large content | **?** §14.4 |
| `plugin.includes` (which plugins load) | a default set | Determines the actually-exposed parser/protocol attack surface | **?** supported set — §14.5 |

## §6 Assumptions about inputs

| Surface | Input | Attacker-controllable? | Operator must enforce |
| --- | --- | --- | --- |
| Fetcher | fetched page bytes, headers, redirects, `robots.txt`, sitemaps, feeds | **yes — by design** | resource caps; URL scope *(inferred)* |
| Parser plugins (Tika etc.) | content bytes + declared content-type | **yes** | parser limits; disable risky parsers *(inferred — §14)* |
| URL filters/normalizers | extracted URLs (from untrusted pages) fed into regex | **yes** (URLs come from hostile pages) | bounded/safe regex (avoid ReDoS) *(inferred)* |
| Seeds / `regex-urlfilter` / plugin config | config files | **no — operator-trusted** | filesystem perms *(inferred)* |
| `nutch-server` REST | HTTP control requests | **yes** if exposed | keep in trusted env / front with auth *(documented)* |
| Indexer write | parsed docs → backend | trusted path | backend access control *(inferred)* |

Size/shape/rate: hostile content can be arbitrarily large/deep/compressed;
whether super-linear parse cost / OOM / hang on crafted content is in-model
needs a §8 line *(inferred — §14)*.

## §7 Adversary model

**In scope** *(inferred — §14)*:
- **Malicious web-content supplier** — anyone who controls a page Nutch fetches:
  serves hostile HTML/XML/feeds/redirects/headers/`robots.txt` to crash the
  parser, exhaust resources, exfiltrate via the parser (XXE), or poison the crawl
  DB / extracted links. This is the primary, always-present adversary.
- **Network client of an exposed `nutch-server`** — if the REST control API is
  reachable beyond the trusted environment.

**Capabilities:** full control of the bytes/headers/redirects Nutch fetches;
cannot (assumed) reach the operator's config, host, or backends.

**Explicitly out of scope:**
- The **operator** and anyone who controls seeds/filters/plugins/host — already
  trusted.
- Anyone reaching a `nutch-server` or crawl store that the operator exposed
  outside the trusted environment — out of model per §2/§3.

## §8 Security properties the project provides

*(All *(inferred)* pending §14 — the website model describes posture but does not
enumerate committed properties.)*

1. **Robust handling of well-formed fetched content** within configured limits —
   parsing/normalization completes or fails safely rather than corrupting the
   crawl store or the host. Violation symptom: crash/OOB/OOM/hang driven by
   fetched content. Severity: **security-relevant** (untrusted-input-driven).
   *(inferred)*
2. **Crawl scope honoured** — Nutch fetches within the operator's URL
   filters/seeds; it does not silently ignore the configured scope. Violation
   symptom: fetching URLs the filters exclude. Severity: **medium**. *(inferred)*
3. **Resource bounding on hostile content** — *needs a line.* Propose:
   super-linear CPU/memory in fetched-content size, or a hang on crafted content,
   is in-model; merely "slow on a huge legitimate crawl" is not. Confirm the line
   in §14. *(inferred)*
4. **No execution of fetched content** — fetched pages are data, not code; Nutch
   does not execute scripts from crawled pages server-side. Violation symptom:
   crawled-content-driven code execution. Severity: **critical**. *(inferred)*

## §9 Security properties the project does *not* provide

- **No sandbox/curation of what it crawls.** Nutch will fetch whatever the
  operator's scope permits, including internal hosts — "SSRF" via the crawler is
  inherent and is controlled by URL filters, not by Nutch refusing to fetch.
  *(inferred)*
- **No authentication on the legacy `nutch-server` REST API.** It assumes a
  trusted environment. *(documented)*
- **No protection for deployments outside a trusted environment.** *(documented)*

**False-friend properties:**
- **URL filters are a crawl-scoping tool, not a security boundary against a
  hostile operator or a guarantee against SSRF to internal services** — they
  reduce reachable URLs but are operator-configured and regex-based. *(inferred)*
- **"It fetched an internal URL" is not by itself a vulnerability** — it means
  the operator's scope allowed it. *(inferred)*

**Well-known attack classes left to the operator / inherent to crawling:**
- **XXE** in XML/feed parsing of hostile content (Tika/parsers) — a real
  in-model concern (parser must be hardened).
- **ReDoS** in URL-filter / normalizer regex fed URLs from hostile pages.
- **Decompression/zip bombs** and **billion-laughs** in fetched compressed/markup
  content.
- **SSRF** to internal services — inherent to a crawler; scoped by the operator.
- **Crawler traps / infinite link spaces** — bounded by fetch limits, an
  operator tuning task.

## §10 Downstream / operator responsibilities

- **Run Nutch only in a trusted environment**; do not expose `nutch-server` (or
  the crawl store) to untrusted networks without fronting auth. *(documented)*
- **Scope the crawl** with URL filters / seeds to avoid fetching internal or
  unintended hosts. *(documented/inferred)*
- **Set fetcher/parser resource limits** (`db.max.outlinks`, fetch size/time,
  parse limits) appropriate to crawling hostile content. *(inferred)*
- **Choose `plugin.includes` deliberately** — only load the protocol/parse
  plugins you need. *(inferred)*
- **Secure indexer/storage backends** independently. *(inferred)*
- **Protect config files and crawl data** with host filesystem permissions.
  *(inferred)*

## §11 Known misuse patterns

- Exposing `nutch-server` (no auth) to an untrusted network.
- Running with an over-permissive URL filter so the crawler reaches internal
  services.
- Treating URL filters as a security guarantee rather than a scoping aid.
- Crawling hostile content without resource limits (parser DoS / crawler traps).
- Loading unnecessary/unsupported parser plugins, widening the attack surface.

## §11a Known non-findings (recurring false positives)

The highest-leverage section for keeping scan output signal-heavy:

- **"Nutch fetches arbitrary / internal URLs (SSRF)."** Reaching URLs is the
  crawler's function; scope is the operator's URL-filter job. Out of model unless
  it bypasses the configured scope. (§3, §9) *(inferred)*
- **"Nutch parses attacker-controlled HTML/XML."** That is the job; a *crash/OOB/
  XXE* on hostile content is `VALID`, but "it parses untrusted input" alone is
  not a finding. (§6, §8) *(inferred)*
- **"`nutch-server` REST API has no authentication."** Documented trusted-env
  posture; operator fronts/isolates it. (§5a, §9) *(documented)*
- **"Crawler followed a redirect / hit a trap / used lots of CPU on a huge
  crawl."** Operator tuning (limits/scope), not a defect, unless super-linear on
  crafted content per §8. (§8, §10) *(inferred)*
- **Static-analysis "SSRF/ederef of untrusted input" on the fetch/parse path** —
  in-model only when it crashes the parser/host or escapes the configured scope.
  (§4 reachability test) *(inferred)*

## §12 Conditions that would change this model

- A new protocol/parse plugin in the supported set; adding authentication to
  `nutch-server`; a new deployment mode; a change to default URL-filter/limit
  behavior; promoting a contrib plugin into core.
- **A report that cannot be routed to one §13 disposition** is evidence the model
  is incomplete — revise §8/§9 rather than make an ad-hoc call.

## §13 Triage dispositions

| Disposition | Meaning | Licensed by |
| --- | --- | --- |
| `VALID` | Hostile fetched content crashes/OOBs/hangs the parser/host, achieves XXE/code-exec, escapes the configured crawl scope, or (on an in-scope exposed surface) bypasses an §8 property. | §8, §6, §7 |
| `VALID-HARDENING` | No §8 property broken, but a §11 misuse is too easy (e.g. an unsafe default regex); hardened at maintainer discretion. | §11 |
| `OUT-OF-MODEL: trusted-input` | Requires control of seeds / URL filters / plugin config / host. | §6 |
| `OUT-OF-MODEL: adversary-not-in-scope` | Requires operator privilege, or access to an endpoint exposed outside the trusted environment. | §7 |
| `OUT-OF-MODEL: non-default-build` | Only manifests under a discouraged config (e.g. exposed `nutch-server`, over-broad filters) the PMC rules operator-owned. | §5a |
| `BY-DESIGN: property-disclaimed` | Crawler reaches/parses untrusted content, "SSRF" by scope, etc. | §9 |
| `KNOWN-NON-FINDING` | Matches a §11a pattern. | §11a |
| `MODEL-GAP` | Cannot be routed above → revise the model. | §12 |

## §14 Open questions for the maintainers

Grouped in waves; each states a **proposed answer** to confirm/correct/strike.
Every *(inferred)* tag above maps to one of these.

**Wave 1 — scope & adversary (these shape everything):**
1. **Trusted-environment posture / `nutch-server`.** Proposed: the supported
   posture is "trusted environment only"; an exposed no-auth `nutch-server` is
   `OUT-OF-MODEL: non-default-build`. Correct? (→ §5a, §3, §11a)
2. **Crawler SSRF / scope.** Proposed: fetching internal/arbitrary URLs is
   by-design and controlled by operator URL filters, not a Nutch vulnerability;
   only *escaping the configured scope* is in-model. Correct? (→ §9, §11a)
3. **Primary adversary = the crawled-content supplier.** Proposed: the main
   in-model attacker is whoever controls fetched content (hostile HTML/XML/feeds/
   redirects), and parser robustness against it is the core property. Agree? Any
   other in-model adversary? (→ §7)

**Wave 2 — properties & parsers:**
4. **Parser hardening (XXE / bombs).** Are XML/feed parsers configured against
   XXE and decompression/entity bombs by default, or is that operator config?
   (→ §8, §9)
5. **Resource line.** Where is the line between an in-model parser-DoS on crafted
   content and an out-of-model "expensive legitimate crawl"? (→ §8)
6. **Supported plugin set.** Which `protocol-*` / `parse-*` / `indexer-*` plugins
   are first-class for security vs. contrib/unsupported? (→ §2/§3/§5a)

**Wave 3 — meta:**
7. **Canonicalization.** This in-repo `THREAT_MODEL.md` is drafted as a superset
   of the website security-model page. Proposed: `SECURITY.md` points here for the
   full model, and the website page stays the operator-facing how-to. Agree, or
   should the website page remain canonical with this as a supplement? (→ meta)

## §15 Machine-readable companion

Deferred for v0; a `threat-model.yaml` sidecar can be generated once the prose is
ratified.
