<!--
SPDX-License-Identifier: Apache-2.0

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# Threat Model — Apache Kvrocks

## §1 Header

- **Project:** Apache Kvrocks — a distributed key-value NoSQL database that uses RocksDB as its
  storage engine and is compatible with the Redis (RESP) protocol, adding namespace-token
  multi-tenancy, binlog-based async replication, Redis-Sentinel failover, and a proxyless
  centralized cluster mode *(documented — README)*.
- **Modelled against:** `apache/kvrocks` `unstable`/HEAD (2026-05-31).
- **Status:** **DRAFT — v0, reviewed by the Kvrocks PMC (PragmaTwice).** Produced by the ASF Security team
  via the `threat-model-producer` rubric
  (<https://gist.github.com/potiuk/da14a826283038ddfe38cc9fe6310573>) for the PMC to react to. PragmaTwice
  (Kvrocks PMC) has reviewed and answered the open questions; all §14 items are resolved and folded in as
  *(maintainer)*.
- **Version binding:** versioned with the project; a report against release *N* is triaged against the
  model as it stood at *N*.
- **Reporting cross-reference:** §8-property violations → report privately per `SECURITY.md` /
  <https://www.apache.org/security/>; §3 / §9 findings are closed citing this document.
- **Provenance legend:** *(documented)* = Kvrocks docs/README/`kvrocks.conf`/source; *(maintainer)* =
  confirmed by a Kvrocks PMC member; *(inferred)* = reasoned from code/config/Redis-family domain
  norms, **not yet confirmed** — each routes to a §14 question.
- **Draft confidence:** with PragmaTwice's review folded in, the core model (§2–§13) is PMC-confirmed; the residual *(inferred)* tags are limited to low-stakes environmental details (host-inventory, platform specifics).

Kvrocks is a network server: clients speak the Redis wire protocol to it over TCP (default port
`6666`, default `bind 127.0.0.1`) *(documented — `kvrocks.conf`)*. Data is partitioned into
**namespaces**, each gated by its own token; the `requirepass` value is the **admin token** and is the
only one permitted to run namespace-management and sensitive admin commands such as `config`,
`slaveof`, and `bgsave` *(documented — README, Namespace section)*. Persistence is RocksDB on local
disk; replication ships a binlog from master to replica.

## §2 Scope and intended use

Primary intended use *(documented)*: a self-hosted, Redis-compatible KV store optimized for larger-than-
memory datasets on RocksDB, accessed by Redis clients, optionally clustered.

Caller roles:

- **Unauthenticated client** — a TCP peer that has not presented a valid token; untrusted.
- **Namespace client** — authenticated with a namespace token; trusted only within its namespace.
- **Admin client** — authenticated with the `requirepass` admin token; trusted for the instance.
- **Replica / cluster peer** — another Kvrocks node in the same replication/cluster topology; assumed
  operator-provisioned and trusted *(maintainer — peers are trusted)*.
- **Operator / deployer** — controls `kvrocks.conf`, the data directory, TLS material, and the network
  exposure. Fully trusted; **out of model** as adversary (§3).

**Component-family table:**

| Family | Entry point | Touches outside process | In model? |
| --- | --- | --- | --- |
| RESP command + connection layer | TCP `:6666`, `src/server`, command dispatch | network | **Yes** |
| AuthN + namespace isolation | `AUTH`, namespace tokens, `requirepass` | — | **Yes** |
| Admin command surface | `config`, `slaveof`, `bgsave`, `namespace`, `cluster`, `debug` | disk / net | **Yes** |
| Lua scripting | `EVAL`/`FUNCTION`, `src/commands/cmd_script.cc`, `src/storage/scripting.cc` (LuaJIT) | sandboxed Lua | **Yes** |
| Storage engine | RocksDB, `src/storage` | filesystem | **Yes** |
| Replication | binlog master→replica (async) | network | **Yes** |
| Cluster mode | proxyless centralized cluster, slot migration | network | **Yes** |
| TLS transport (optional) | `tls-*` config | network | **Yes** (when enabled) |
| Build / dev tooling | `dev/`, `utils/`, `x.py`, `cmake/` | — | No → §3 |
| Tests | `tests/` | — | No → §3 |

## §3 Out of scope (explicit non-goals)

- **The operator / deployer as adversary.** Anyone who can edit `kvrocks.conf`, read the RocksDB data
  directory, hold the admin token, or change the bind/TLS posture has already won (§9) *(inferred)*.
- **Network/transport hardening beyond what Kvrocks offers.** When TLS is disabled (default), confidentiality
  and integrity of the wire — including the auth token — are the deployment's responsibility (firewalling,
  private network, or enabling TLS) *(documented — TLS is opt-in in `kvrocks.conf`)*.
- **A downstream proxy/app that builds RESP commands from untrusted user input.** Command-injection at that
  layer is the integrator's problem, not Kvrocks's (§9).
- **Custom builds** with non-default compile options, and operator-supplied Lua scripts run under the admin
  token.
- **Shipped-but-unsupported code:** `dev/`, `utils/`, `x.py`, `tests/` *(inferred)*.

## §4 Trust boundaries and data flow

The trust boundary is the **TCP connection + the token presented on it**. A connection's identity
(unauthenticated → namespace → admin) determines which commands and which keyspace it may touch
*(documented — namespace/admin-token rules)*.

Trust transitions:

1. **Connect → AUTH:** a new connection is unauthenticated. If `requirepass` (or namespace tokens) are
   configured, commands are refused until a valid token is presented via `AUTH` *(maintainer —
   PragmaTwice: commands are refused pre-`AUTH` when a token is configured; default `requirepass` is unset, see §5a)*.
2. **Namespace token → keyspace:** a namespace-authenticated connection is confined to that namespace's
   keys and is denied admin/namespace/cluster commands *(maintainer — strict per-namespace keyspace
   confinement; admin-only metadata)*.
3. **Admin token → full control:** the admin token may run all commands, including `config`, `slaveof`,
   `bgsave`, namespace management, and cluster operations *(documented)*.
4. **EVAL → Lua sandbox:** scripts execute in the embedded LuaJIT sandbox, which is intended to deny
   arbitrary host access *(maintainer — scripting confined to the namespace; no host access)*.
5. **Master → replica:** the master streams a binlog to replicas; the replica applies it. Peers are
   mutually trusted within the topology *(maintainer — peers are trusted)*.

**Reachability preconditions:**

- A finding in the **command/RESP** path is in-model if reachable from an unauthenticated or
  namespace-token connection (admin-token-only reach → lower severity, see §7).
- A finding in **namespace isolation** is in-model if it lets a namespace token read/write outside its
  namespace or run an admin-only command.
- A finding in **Lua scripting** is in-model if a script permitted to a non-admin caller can break the
  sandbox or access another namespace / host resources.
- A finding reachable only from `kvrocks.conf`, the data dir, or the admin token is out of model (§3).
- A finding requiring a malicious replica/cluster peer is **out of model** — peers are trusted
  *(maintainer)*.

## §5 Assumptions about the environment

- **OS/runtime:** a POSIX host with a filesystem for the RocksDB data directory; a conformant C++ runtime
  *(inferred)*.
- **Network:** the operator controls who can reach the listening port; default `bind 127.0.0.1` limits this
  to localhost until changed *(documented — `kvrocks.conf`)*.
- **Storage:** local disk is trusted; RocksDB data at rest is **not** encrypted by Kvrocks, and no
  per-namespace encryption is claimed *(maintainer — no per-namespace encryption claimed)*.
- **Replication/cluster peers:** provisioned by the operator on a trusted network; peers are trusted
  *(maintainer)*.
- **What Kvrocks does to its host (inventory, *(inferred)* — wave-2 target):** binds a TCP port; reads/writes
  the configured data directory; reads `kvrocks.conf`; opens outbound connections to replication masters /
  cluster peers; may write RDB/backup files on `bgsave`. Not assumed to spawn shells or read arbitrary files
  outside its data dir (Lua sandbox permitting).

## §5a Build-time and configuration variants

These `kvrocks.conf` knobs change which §8 properties hold. **Defaults below are documented; the
maintainer has clarified the supported posture for the auth/TLS defaults (see rulings).**

| Knob | Default *(documented — `kvrocks.conf`)* | Effect | Insecure-default ruling |
| --- | --- | --- | --- |
| `requirepass` | **unset (commented)** | No admin token ⇒ **no authentication**; all clients are effectively admin | *(maintainer)* If `requirepass` is unset, operators are responsible for restricting access to trusted personnel only; an unauth-access report against a routable, no-`requirepass` deployment is operator responsibility |
| `bind` | `127.0.0.1` | Localhost-only by default; limits exposure | Safe default; reports requiring a routable bind + no auth are operator misconfig |
| `tls-port` / `tls-*` | **off** | No transport encryption ⇒ token + data in plaintext on the wire | *(maintainer)* Same as `requirepass`: when TLS is off, operators are responsible for restricting access to trusted personnel only; plaintext-on-untrusted-network is operator responsibility |
| namespace tokens | none until configured | Multi-tenant isolation only exists once namespaces+tokens are set | Strict per-namespace keyspace confinement *(maintainer)*; pub/sub is a known exception (§9) |
| Lua scripting (`EVAL`) | enabled *(inferred)* | Adds a sandboxed code-execution surface | Sandbox confined to the namespace; no host access *(maintainer)* |
| `maxclients` / value-size / proto limits | defaults *(inferred)* | DoS envelope | No intrinsic guarantee beyond configured limits *(maintainer)* |

## §6 Assumptions about inputs

| Entry point | Parameter | Attacker-controllable? | Caller/operator must enforce |
| --- | --- | --- | --- |
| any RESP command | command name + arguments | **yes** (any connected client) | auth gate; namespace confinement; arg bounds |
| `AUTH` | token | **yes** | constant-time compare *(maintainer — desirable hardening)*; throttling |
| key/value ops | key, value bytes | **yes** (within namespace) | value-size / memory limits |
| `EVAL`/`FUNCTION` | Lua script body + keys/args | **yes** (if non-admin may script) | sandbox; no cross-namespace/host access |
| `namespace`/`config`/`slaveof`/`bgsave`/`cluster` | args | **yes**, but **admin-token-gated** | admin-token-only enforcement |
| replication stream | binlog bytes from master | from a **trusted** peer *(maintainer — peers are trusted)* | peer authenticity (network/TLS) |
| `kvrocks.conf` | all keys | **no — operator-trusted** | never sourced from a client |

Inputs are bounded only where the operator configures limits; Kvrocks provides no intrinsic guarantee
beyond configured limits — it is not assumed to bound pipeline depth, value size, `KEYS`/scan cost, or
Lua run-time intrinsically beyond configured maxima *(maintainer — no intrinsic DoS guarantee; §8.6/§9)*.

## §7 Adversary model

- **Primary adversary:** an untrusted TCP client — unauthenticated, or holding only a namespace token and
  trying to exceed it. Capabilities: send arbitrary RESP commands, attempt `AUTH`, run permitted Lua, push
  large/expensive workloads.
- **Goals:** access data in another namespace or without a token; run admin commands without the admin token;
  break the Lua sandbox; exhaust CPU/memory/disk; read tokens/data off an unencrypted wire.
- **Out of model:** the operator, the admin-token holder, anyone with filesystem/`kvrocks.conf` access, and
  a malicious replication/cluster peer (peers are trusted *(maintainer)*). A command reachable **only** with
  the admin token is `OUT-OF-MODEL: adversary-not-in-scope` unless it crosses into the host beyond the
  documented admin surface.

## §8 Security properties the project provides

*(All *(inferred)* working hypotheses for v0 unless tagged; symptom + severity per the rubric.)*

1. **Namespace data isolation.** A connection authenticated with a namespace token can access only that
   namespace's keyspace; it cannot read/write other namespaces *(maintainer — strict per-namespace keyspace
   confinement; admin-only metadata)*. **Known limitation:** pub/sub does **not** currently respect
   namespaces *(maintainer)* — see §9. *Symptom:* cross-namespace read/write (excluding the documented
   pub/sub limitation). *Severity:* critical.
2. **Admin/namespace privilege separation.** Only the admin (`requirepass`) token may run namespace
   management and sensitive admin commands (`config`, `slaveof`, `bgsave`, cluster ops) *(documented)*.
   *Symptom:* a namespace/unauth client runs an admin command. *Severity:* critical.
3. **Authentication gate (when configured).** With `requirepass`/namespace tokens set, commands beyond
   `AUTH` are refused on an unauthenticated connection *(maintainer — PragmaTwice)*. *Symptom:* pre-auth data access.
   *Severity:* critical.
4. **Lua sandboxing.** Scripts run in a constrained LuaJIT environment without arbitrary host/file access and
   confined to the caller's namespace *(maintainer — scripting confined to the namespace; no host access)*.
   *Symptom:* sandbox escape / host access / cross-namespace access from a script. *Severity:* critical.
5. **Memory safety on protocol parsing.** Well-formed and malformed RESP input does not cause memory-corruption
   on supported platforms *(inferred)*. *Symptom:* OOB read/write, crash from crafted input. *Severity:*
   critical.
6. **Resource bounds.** There is **no intrinsic DoS guarantee beyond configured limits** *(maintainer)*;
   the operator's contract is to configure `maxclients` and size limits. (The `luajit_bytecode_dos` test
   shows Lua DoS is taken seriously, but no broader intrinsic guarantee is claimed.) *Symptom:* hang/OOM/disk-fill
   beyond configured limits. *Severity:* medium (treated as operator-tuning, not a property breach, unless a
   configured limit is bypassed).

## §9 Security properties the project does NOT provide

- **No authentication by default.** `requirepass` ships unset; an operator who binds to a routable interface
  without setting it exposes an **unauthenticated admin-level** store *(documented — `kvrocks.conf`)*. The
  `bind 127.0.0.1` default mitigates this until changed. **When `requirepass` is unset, operators are
  responsible for restricting access to trusted personnel only** *(maintainer)*.
- **No transport encryption by default.** TLS is opt-in; on an untrusted network the auth token and all data
  are observable/modifiable *(documented)*. **As with `requirepass`, when TLS is off operators are responsible
  for restricting access to trusted personnel only** *(maintainer)*.
- **No defence against the operator / admin-token holder** (§3).
- **Namespace isolation is logical, not cryptographic.** All namespaces share one RocksDB instance on disk;
  isolation is an access-control property at the command layer, not at-rest encryption or per-tenant key
  separation. **No per-namespace encryption is claimed** *(maintainer)*.
- **Pub/sub does not respect namespaces (known limitation).** Namespace keyspace confinement is strict for
  keys, but pub/sub channels are **not** currently namespaced — a namespace client can publish/subscribe
  across the instance *(maintainer — known limitation)*. This is a documented current limitation rather than
  a property breach; a report matching exactly this behaviour is a `KNOWN-NON-FINDING` (§11a), while
  namespacing pub/sub is desirable hardening.
- **No strong anti-DoS guarantee** against expensive commands (`KEYS`, large `MGET`, huge values, deep
  pipelines) or adversarial Lua beyond configured limits — **no intrinsic guarantee beyond configured limits**
  *(maintainer)*.

**False-friend properties:**

- *The namespace token looks like a per-user credential but is a shared per-namespace secret* — anyone with
  the token is every user of that namespace; rotation is manual (`namespace set`).
- *`requirepass` looks like "a password" but is the **admin** token* — it is not a low-privilege credential;
  giving it out grants full control.
- *Namespace confinement looks total but pub/sub is an exception* — keyspace access is strictly confined, but
  pub/sub channels currently cross namespaces *(maintainer)*.
- *Replication/cluster membership is authenticated and peers are trusted* — it is **not** a defence against a
  malicious peer; the model assumes trusted peers *(maintainer)*.

**Well-known attack classes left to the operator/integrator:**

- **Unauthenticated exposure** (the Redis-family classic) — set `requirepass` and/or restrict `bind`; when
  unset, restrict access to trusted personnel *(maintainer)*.
- **Plaintext token/data sniffing & MITM** without TLS — when TLS is off, restrict access to trusted personnel
  *(maintainer)*.
- **RESP command injection** from a downstream app that interpolates untrusted input into commands.
- **Lua-based DoS / sandbox probing.**
- **Cross-namespace pub/sub** on a multi-tenant instance (known limitation, §9).

## §10 Downstream (operator) responsibilities

- **Set `requirepass` (and namespace tokens) before binding to any non-localhost interface; if `requirepass`
  is left unset, restrict access to trusted personnel only** *(maintainer)*.
- Enable TLS (`tls-port`, certs, `tls-auth-clients`) on untrusted networks; otherwise keep traffic on a
  trusted/segmented network. **When TLS is off, restrict access to trusted personnel only** *(maintainer)*.
- Treat the admin token as root-equivalent; distribute only namespace tokens to tenants; rotate on exposure.
- Do not rely on namespace isolation for **pub/sub** — channels are not currently namespaced *(maintainer)*.
- Run replication/cluster peers on a trusted network (and/or with TLS); peers are trusted, so provision them
  yourself *(maintainer)*.
- Configure resource limits (`maxclients`, value/proto size) for the deployment's risk profile — there is no
  intrinsic DoS guarantee beyond these *(maintainer)*.
- Protect the RocksDB data directory and backup/RDB files at the filesystem layer.

## §11 Known misuse patterns

- Exposing Kvrocks to a routable network with `requirepass` unset (unauthenticated admin store) without
  restricting access to trusted personnel.
- Handing the admin token to applications that only need a single namespace.
- Building RESP commands by string-concatenating untrusted user input in a downstream app.
- Running over plaintext on an untrusted network and treating namespace tokens as if confidential.
- Assuming RocksDB-at-rest is per-namespace isolated/encrypted.
- Relying on namespace isolation for **pub/sub** channels (not currently namespaced).

## §11a Known non-findings (recurring false positives)

*(v0 seed — the PMC's real list is the highest-leverage §14 input.)*

- **"No password set"/"unauthenticated access"** flagged against a default config — by design, mitigated by
  `bind 127.0.0.1`; operator responsibility (when `requirepass` is unset, operators restrict access to
  trusted personnel) *(maintainer)* → `BY-DESIGN: property-disclaimed` / operator responsibility.
- **"Plaintext protocol / no TLS"** against default config — TLS is opt-in (§9/§10); operator responsibility
  (restrict access to trusted personnel when TLS is off) *(maintainer)*.
- **Cross-namespace pub/sub** — pub/sub does not currently respect namespaces; this is a known limitation, not
  a property breach *(maintainer)* → `KNOWN-NON-FINDING` (namespacing pub/sub is desirable hardening).
- **Admin command "danger" (`FLUSHALL`, `CONFIG`, `DEBUG`, `bgsave`)** reachable with the admin token — by
  design; admin token is root-equivalent (§7).
- **Findings in `tests/`, `dev/`, `utils/`, `x.py`** — out of scope (§3).
- **RESP command injection** attributable to a downstream caller concatenating input — not a Kvrocks bug (§9).
- **RocksDB-internal warnings** from the bundled storage engine that are not reachable from client input.
- **Non-constant-time token compare** in `AUTH` — constant-time compare is desirable hardening, not a property
  breach *(maintainer)* → `VALID-HARDENING`.

## §12 Conditions that would change this model

- A new command that crosses the namespace boundary or relaxes admin-token gating.
- A change to the default `requirepass`/`bind`/TLS posture.
- A new client-reachable surface (new protocol, HTTP admin API, new cluster control plane).
- A change to the Lua sandbox scope or who may script.
- Treating replication/cluster peers as untrusted (would pull them into §7).
- Namespacing pub/sub (would close the known limitation in §9).
- Any report that can't be routed to a single §13 disposition (→ revise the model).

## §13 Triage dispositions

| Disposition | Meaning | Licensed by |
| --- | --- | --- |
| `VALID` | Violates a claimed property via an in-scope adversary/input. | §8, §6, §7 |
| `VALID-HARDENING` | No §8 property broken, but a §11 misuse is easy enough to warrant hardening (e.g. constant-time `AUTH` compare, namespacing pub/sub). | §11 |
| `OUT-OF-MODEL: trusted-input` | Requires control of a trusted input (config / admin token / replica stream). | §6 |
| `OUT-OF-MODEL: adversary-not-in-scope` | Requires operator / admin-token / filesystem / trusted-peer capability. | §7, §3 |
| `OUT-OF-MODEL: unsupported-component` | Lands in `tests/`, `dev/`, `utils/`, tooling. | §3 |
| `OUT-OF-MODEL: non-default-build` | Only under a discouraged/non-default `kvrocks.conf` setting. | §5a |
| `BY-DESIGN: property-disclaimed` | Concerns a §9-disclaimed property (no-auth default, no-TLS default, logical-only namespace isolation, non-namespaced pub/sub). | §9 |
| `KNOWN-NON-FINDING` | Matches a §11a entry (incl. cross-namespace pub/sub). | §11a |
| `MODEL-GAP` | Routes to none of the above → revise the model. | §12 |

## §14 Open questions for the maintainers

All questions below have been answered by PragmaTwice (Kvrocks PMC) and folded into the body as
*(maintainer)*; they are retained as a record of the resolution. Three waves.

**Wave 1 — scope & insecure-default rulings (§2/§3/§5a/§8/§9):**
1. *(Answered — maintainer.)* Running **without `requirepass`** / with **TLS off**: operators are responsible
   for restricting access to trusted personnel only. An unauth/plaintext report against such a deployment is
   operator responsibility, not a property breach. *Remaining:* none.
2. *(Answered — maintainer.)* **Replication / cluster peers are trusted** — a malicious peer holding valid
   topology credentials is out of the §7 adversary model. *Remaining:* none.
3. *(Answered — maintainer.)* The **authentication gate** (§8.3) behaves as expected: commands are refused
   pre-`AUTH` on an unauthenticated connection once a token is configured. *Remaining:* none.

**Wave 2 — isolation & scripting (§4/§8):**
4. *(Answered — maintainer.)* **Namespace isolation:** strict per-namespace keyspace confinement; metadata is
   admin-only. **Known limitation:** pub/sub does **not** currently respect namespaces (documented in §9 as a
   known limitation / `KNOWN-NON-FINDING`; namespacing it is desirable hardening). *Remaining:* none — strict
   per-namespace keyspace confinement holds; pub/sub is the only documented cross-namespace exception.
5. *(Answered — maintainer.)* **Lua scripting** is confined to the caller's namespace with no host access;
   namespace clients may run it, confined to their namespace. *Remaining:* none.
6. *(Answered — maintainer.)* **RocksDB data-at-rest** is operator-trusted disk; **no per-namespace encryption
   is claimed**. *Remaining:* none.

**Wave 3 — resource line, auth hardening, §11a (§8/§9/§11a):**
7. *(Answered — maintainer.)* **Resource line:** no intrinsic guarantee beyond configured limits; the contract
   is to configure `maxclients` and size limits. *Remaining:* none.
8. *(Answered — maintainer.)* **`AUTH` hardening:** network controls are expected; a constant-time token
   compare is **desirable hardening** (`VALID-HARDENING`), not a property breach; brute-force throttling is
   operator/network responsibility. *Remaining:* none.
9. *(Answered — maintainer.)* The §11a list above is the PMC's recurring-non-finding set; no additions at
   this time.

**Meta:**
10. *(Answered — maintainer.)* This model lives in root `THREAT_MODEL.md` referenced from `SECURITY.md` (this
    PR) and covers `apache/kvrocks`; `apache/kvrocks-controller` has its own model (its cluster-control-plane
    trust surface differs).

## §15 Machine-readable companion

Deferred for v0. A `threat-model.yaml` can later encode the §6 trust table, §2/§3 component scoping, §8
property/severity/symptom rows, §9 false friends, §11a non-findings, and §13 dispositions for automated triage.
