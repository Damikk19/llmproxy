# Rate Limiting — Implementation Plan

**Status:** Split into two phases — see **§0**. Phase 0 (performance
groundwork, §16.1–16.2) is implemented. **§17 is an open gate on phase 2 that
also constrains phase 1's schema** — resolve it before writing `schema.sql`.
§15 holds the smaller open questions.
**Scope:** Per-user, per-model and global rate limiting for `llmproxy`
**Hard constraint:** Must not break existing config files or databases.

---

## 0. Phasing (read this first)

> ### ⛔ OPEN GATE — §17
> The limit model needs redesigning before phase 2: **§5 cannot express "this
> user, this model"** — it has a per-user level and a per-model level but no
> `(user, model)` cell. See **§17**.
>
> It is filed as a phase-2 gate, but **§17.5 shows it partly constrains phase
> 1's schema**, which has not been written yet. Resolving it now costs nothing;
> resolving it after phase 1 ships means migrating live columns.

The work ships in **two phases**. Phase 1 is the safety switch: stop one API
key from swarming the proxy or a backend. Phase 2 adds daily token budgets.

The cut is not arbitrary — it falls exactly where the storage tier changes:

| | **Phase 1 — rate** | **Phase 2 — quota** |
|---|---|---|
| Dimensions | `rpm`, `concurrency` | `prompt_tpd`, `completion_tpd` |
| **Counter** storage | in-memory only | DB (+ in-memory cache) |
| **Limit** storage | `api_key` row / key document — read on the existing auth lookup | same, 2 more fields |
| SQLite schema | 2 nullable `api_key` columns (v1 → **v2**) | 2 more columns + `api_key_daily_usage` (v2 → **v3**) |
| Mongo | `user_list` reads 2 optional fields off the key doc. **No new collection, no index, no migration** (schemaless) | new collection, unique index, identity + date contract |
| Per-key limits settable by | SQLite: `ctl`, immediately. **Mongo: only once CGC persists the fields** (§15.6) | same |
| Added per-request DB cost | **zero** — the limits ride the auth row, which is cached (§16.2) | one cached read (§16.3) |
| New failure modes | none — no counter is persisted | read/write failure policy (§15.1) |
| Multi-replica | degrades to `N ×` (documented since day one) | same, once §16.3 is on |

**Both backends are touched in phase 1** — the phases split on where the
*counters* live, not on whether the database is involved at all. The *limits*
are per-key configuration and must be stored and read on both backends in phase
1. What phase 1 avoids is a persisted **counter**: no daily table, no
`usage_today`/`usage_add`, no accounting write, no new Mongo collection or
index. Reading two extra fields from a row the proxy already fetches costs
nothing; maintaining a durable counter is what carries the risk.

**Why this is the right cut.** Everything that makes this feature *risky* —
schema migration for a durable counter, a new Mongo collection and index, an
accounting write on the billing path, overshoot semantics, retention — lives
entirely in phase 2. Phase 1 touches no persisted counter at all: if its
in-memory state is wrong, a restart fixes it. That makes phase 1 safe to ship
fast, which is the whole point of a safety switch.

**Phase 1 delivers both stated goals in their cheap form:**
- *Protect the proxy and backends from one key* — `rpm` + `concurrency`. Works
  on **both** backends from config alone (global + per-model), so the safety
  switch is available everywhere on day one.
- *Restricted / trial keys* — per-key `rpm`/`concurrency` plus the existing
  `expires`. **On SQLite this works immediately** via `ctl`. **On Mongo it does
  not**, until CGC persists the fields on the key document (§15.6) — `ctl` does
  not manage Mongo keys by design. A token budget waits for phase 2 on both.

> **Consequence to accept before starting:** if Mongo is the production
> database, phase 1 ships the safety switch but *not* per-key trial limits. Two
> ways forward, neither blocking phase 1: agree the field shape with the CGC
> team during phase 1 (§15.6), or derive limits from `subscription_level`,
> which `user_list` already reads into `_tier` (`db.py:150`) and which needs
> nothing from CGC — see §15.8.

**Shared machinery is built once, in phase 1**, and phase 2 only extends it:
the precedence resolver (§5), the bucket rule, the `slot` context manager
(§6.2), the 429 builder (§8), the migration ladder and `_ensure_column` (§4.3),
and the config validation style (§3). Phase 2 adds two dimensions to a resolver
that already loops over dimensions, one step to `slot`, and one ALTER-plus-table
migration to a ladder that already exists.

**Phase 1 does NOT ship the phase-2 columns.** `prompt_tpd` / `completion_tpd`
are added in phase 2, not pre-created as dead columns in phase 1 — a column
that `user_list` returns and `ctl` can set but nothing enforces is a trap: an
admin would set a daily cap and believe it was in force.

Section tags below: **[P1]**, **[P2]**, or untagged where shared.

---

## 1. Goal

Add four rate-limit dimensions, each resolvable at three precedence levels
(per-user → per-model → global):

| Dimension | Meaning | Window | Check timing | Storage | Bucket key |
|---|---|---|---|---|---|
| `rpm` | requests per minute | sliding 60 s | pre-forward | **in-memory** | per-user/global → `(user)`; per-model → `(user, model)` |
| `concurrency` | in-flight requests | in-flight | acquire / release | **in-memory** | same rule as `rpm` |
| `prompt_tpd` | input tokens per day | UTC calendar day | pre-forward read + post-response increment | **DB** | per-user/global → `SUM` over models; per-model → one model row |
| `completion_tpd` | output tokens per day | UTC calendar day | same | **DB** | same |

**Why two storage tiers:**
- `prompt_tpd` / `completion_tpd` are **cumulative across restarts and replicas**
  → must live in the DB (next to billing). In-memory would reset on restart
  (billing leak) and split across replicas (N× the quota).
- `rpm` / `concurrency` are **ephemeral** (recent activity / live connections)
  → in-memory, zero new dependencies. Concurrency *must* be in-process.

> **Deployment note:** This design assumes a **single replica** (confirmed).
> RPM and concurrency are enforced per-process. If the proxy is ever scaled to
> N replicas, a user effectively gets `N ×` those two limits. Strict
> multi-replica RPM/concurrency would require Redis or DB-heartbeat counters
> (out of scope).
>
> **After phase 1 this is the whole story:** `rpm` and `concurrency` are the
> only dimensions, both in-memory, so "single replica" is the only deployment
> caveat there is. Nothing is persisted, so nothing can be corrupted — a
> restart simply resets the windows.
>
> **[P2] TPD is no longer the exception.** With the in-process usage cache
> (§16.3) enabled — the default — each replica reads its own cached daily total
> and misses the others' writes, so TPD also degrades to roughly `N ×` under
> multi-replica. Set `usage_cache = false` to restore strict DB-backed TPD at
> the cost of one DB read per request. **After phase 2, every dimension is
> single-replica-scoped; this note is the single place that records it.**

---

## 2. Locked decisions

1. **Single replica** → RPM & concurrency stay in-memory (no Redis/DB counters).
2. **Daily reset = UTC midnight** (`date('now')` in SQLite).
3. **All four dimensions at all three levels** — `rpm`, `concurrency`,
   `prompt_tpd`, `completion_tpd` each resolve per-user → per-model → global.
   Delivered in two phases (§0): `rpm`/`concurrency` first, `*_tpd` second. The
   resolver loops over dimensions, so phase 2 adds entries to a list rather
   than changing the mechanism.
4. **Precedence:** per-user overrides everything; per-model overrides global;
   global applies only if nothing prior is set.
5. **`0 = no limit` (unlimited)** everywhere — config and DB. `0` exempts a key
   or model from a dimension entirely. DB `NULL` / config-absent = **fall
   through** to the next precedence level (not the same as `0`).
6. **Audio is exempt** from `prompt_tpd` / `completion_tpd` checks (audio bills
   by seconds, not tokens). Audio **is** subject to `rpm` and `concurrency`.
7. **TPD overshoot is bounded and intentional.** Tokens are known only after the
   response, so the pre-check reads "today so far" and the increment runs after.
   Matches OpenAI behaviour. Documented, not a bug. Worst case is
   `Σ_models(concurrency_m) × max_request_size` — **not** `concurrency ×
   max_request_size`: a user-level TPD limit is one bucket summed across all
   models, while per-model concurrency is one bucket *per* model, so the
   in-flight count that races the pre-check is the sum over every model the key
   can reach. A model whose concurrency resolves to `None`/`0` makes that sum
   unbounded, so §3 requires a global `concurrency` floor whenever any `*_tpd`
   is configured.
8. **The Mongo client is process-global** (§16.1). Rebuilding a Motor client per
   request defeats connection pooling entirely; the current per-request
   create/close is a defect this change fixes, not a design to preserve.
9. **Auth lookups are cached in-process** (§16.2), with `expires` re-evaluated
   in Python on every hit so key expiry stays exact. The cache TTL delays
   **revocation** only. This is the one deliberate behavioural change in the
   plan — see §16.2 for the trade and the SIGHUP escape hatch.
10. **Daily token totals are cached in-process, write-through** (§16.3). The DB
    stays the source of truth and every increment still hits it, so a restart
    reseeds exactly. Costs strict multi-replica TPD (see §1).

---

## 3. Config (additive, all optional)

New top-level block and an optional sub-table per backend:

```toml
# Seconds to cache a successful API-key lookup in memory (§16.2). 0 disables.
# Bounds how long a REVOKED key keeps working; key EXPIRY is unaffected (it is
# re-checked in Python on every request). SIGHUP flushes the cache immediately.
auth_cache_ttl = 5

# Global defaults — applied when no per-user override and no per-model limit.
[rate_limit]
rpm = 60
concurrency = 5
prompt_tpd = 10000000        # 10M input tokens/day per user
completion_tpd = 1000000     # 1M output tokens/day per user

# Keep today's token totals in memory (write-through to the DB). Removes one
# DB read per request. Set false ONLY if running more than one replica (§1).
usage_cache = true

# Existing backend block, unchanged except for the new optional sub-table.
[backends.llama3-8b]
url = "https://..."
token = "..."
device = "a5000"
# ...existing fields unchanged...

# Optional: overrides the global defaults for THIS model (all 4 dimensions).
[backends.llama3-8b.rate_limit]
rpm = 30
concurrency = 3
prompt_tpd = 0               # 0 = this model is exempt from the daily cap
completion_tpd = 0
```

**`config.validate()` additions** — for the top-level `[rate_limit]` block and
**every** `[backends.*.rate_limit]` sub-table: each of `rpm`, `concurrency`,
`prompt_tpd`, `completion_tpd`, where present, must be a non-negative **int**
(`>= 0`). Reject negatives. (Mirrors the existing
`client_max_size` / `max_model_len` validation style — note `type(v) is not int`
already excludes `bool`, which is the behaviour we want.)

Phase 1 validates `rpm` and `concurrency` only; phase 2 adds the `*_tpd` keys to
the same loop.

Plus:
- `auth_cache_ttl`, where present, must be a non-negative int. *(shipped)*
- **[P2]** `usage_cache`, where present, must be a bool.
- **[P2] Concurrency floor:** if any `*_tpd` is set anywhere (top-level, any
  backend sub-table, and — for SQLite — reachable via a per-user column), then
  `[rate_limit].concurrency` must be present and `> 0`. Without a floor at
  precedence level 3, a backend with no concurrency limit makes TPD overshoot
  unbounded (§2.7). One check; no per-backend auditing needed.

**`reload_config`** (SIGHUP) — already re-reads `backends` (so per-model limits
reload for free). Extend it to also reload the top-level `[rate_limit]` block,
`auth_cache_ttl`, and to **flush the auth cache** (§16.2) — that flush is the
operator's instant-revocation lever, so it must be part of the same handler.

---

## 4. DB schema & migration

### 4.1 New `api_key` columns (all nullable)

**[P1]** — schema v2:
```sql
ALTER TABLE api_key ADD COLUMN rpm INTEGER;
ALTER TABLE api_key ADD COLUMN concurrency INTEGER;
```

**[P2]** — schema v3:
```sql
ALTER TABLE api_key ADD COLUMN prompt_tpd INTEGER;
ALTER TABLE api_key ADD COLUMN completion_tpd INTEGER;
```

- `NULL` → fall through to per-model / global.
- `0` → unlimited (exempt).
- `>0` → cap.

### 4.2 New daily-counter table (durable TPD store) — [P2]

Rows are **per `(api_key, date, model)`** so a per-user read can `SUM` across
models while a per-model read filters to one. Historical data stays correct if a
key's bucketing level changes later.

```sql
CREATE TABLE IF NOT EXISTS api_key_daily_usage (
    api_key           TEXT NOT NULL,
    date              TEXT NOT NULL,   -- 'YYYY-MM-DD' UTC (date('now'))
    model             TEXT NOT NULL,
    prompt_tokens     INTEGER NOT NULL DEFAULT 0,
    completion_tokens INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (api_key, date, model)
);
```

One row per (key, day, model). Tiny growth (~one row/key/model/day). Pruning of
old rows is a later concern (a daily cron or a startup sweep) — **out of scope**
for this change; just leave rows in place.

### 4.3 Migration ladder in `SqliteDatabase.create`

Replace the current single-version init (`ver == 0 → schema + stamp 1`) with a
real ladder. The code comment in `db.py` already says *"a future schema v2 needs
real migrations here"* — this is that v2. **The ladder is built in phase 1 and
extended by one rung in phase 2**, which is most of why the phases split
cleanly.

Each rung runs in order from the stored version, so a v1 database upgraded
after phase 2 ships walks v1 → v2 → v3 in a single boot.

**Phase 1 (target `user_version = 2`):**
- `ver == 0` → run base `schema.sql`, **then** `_ensure_column` `rpm` and
  `concurrency`. This heals the documented v0 footgun: a DB created with the
  **old** `schema.sql` (`sqlite3 db.sqlite < schema.sql`) has the `api_key`
  table but **not** the new columns, and `CREATE TABLE IF NOT EXISTS` will
  **not** add them. Stamp `2`.
- `ver == 1` → `_ensure_column` `rpm`, `concurrency`. Stamp `2`.
- `ver >= 2` → no-op.

**Phase 2 (target `user_version = 3`):** add one rung, leave the others alone:
- `ver == 2` → `_ensure_column` `prompt_tpd`, `completion_tpd` +
  `CREATE TABLE IF NOT EXISTS api_key_daily_usage`. Stamp `3`.
- The `ver == 0` rung's post-`schema.sql` `_ensure_column` list grows to all
  four, and it stamps `3`.

**`_ensure_column(table, col, ddl)`** helper: query `PRAGMA table_info(<table>)`;
`ALTER TABLE … ADD COLUMN …` only if the column is absent. Idempotent. This
directly extends the philosophy the existing `test_db.py` suite already encodes
("preexisting data preserved", "partial schema healed").

> The base `schema.sql` is also updated in each phase so **fresh** databases get
> the columns directly. The `_ensure_column` path exists only to heal
> **pre-existing** databases that predate the change.
>
> **Do not skip the intermediate stamp in phase 2.** It is tempting to collapse
> v1 → v3 once both phases exist, but a proxy that ran phase 1 has live v2
> databases in production; the `ver == 2` rung is the only thing that upgrades
> them.

### 4.4 DB method changes

Each phase adds only its own two dimensions. Below, **[P1]** = `rpm`,
`concurrency`; **[P2]** = `prompt_tpd`, `completion_tpd`.

- **`user_list`** — `SELECT` adds the phase's columns (NULL when unset).
  - **Mongo `user_list`** — read these from the `api_keys` doc **if present**
    (optional platform-set fields); fall through to config when absent. No
    schema change on Mongo (it's schemaless). **Phase 1 note:** until CGC
    persists `rpm`/`concurrency` on the key document, Mongo deployments get
    config-only limits (global + per-model). That is enough for the safety
    switch; per-key trial limits on Mongo need the CGC contract (§15.6).
- **`user_update`** — widen the assertion by the phase's columns
  (`{"expires","comment","rpm","concurrency"}` after phase 1);
  `UPDATE` each provided field.
  - **Mongo** — stays `NotImplementedError` (platform manages keys).
- **`user_create`** — accept the phase's columns as optional arguments.
  - **Mongo** — stays `NotImplementedError`.
- **[P2] NEW `usage_today(user, model, bucket)`** → `(prompt, completion)`:
  - `bucket == "user"` (per-user/global limit):
    `SELECT COALESCE(SUM(prompt_tokens),0), COALESCE(SUM(completion_tokens),0)
     FROM api_key_daily_usage WHERE api_key=? AND date=?`
  - `bucket == "user_model"` (per-model limit): add `AND model=?`.
  - **Mongo:** `$group`/`$sum` match on `{api_key, date}` (± `model`).
- **[P2] NEW `usage_add(user, model, prompt_delta, completion_delta)`** → atomic
  upsert:
  ```sql
  INSERT INTO api_key_daily_usage
      (api_key, date, model, prompt_tokens, completion_tokens)
  VALUES (?, ?, ?, ?, ?)
  ON CONFLICT(api_key, date, model) DO UPDATE SET
      prompt_tokens     = prompt_tokens     + excluded.prompt_tokens,
      completion_tokens = completion_tokens + excluded.completion_tokens;
  ```
  - **Mongo:** `find_one_and_update` with `$inc` upsert on
    `{api_key, date, model}` — implemented to mirror `billing_record_add`.

---

## 5. Limit resolution (per dimension, independent)

> **⛔ SUPERSEDED IN PART BY §17.** The three-level precedence below cannot
> express a limit scoped to `(user, model)`, which is now a requirement. Treat
> this section as the *phase 1* resolver only, and as the starting point §17
> revises — not as settled design.

For **each** of the four dimensions, resolved independently:

```
# 1. Per-user override (DB column) — 0 = unlimited, NULL = fall through
if user[dim] is not None:
    limit  = user[dim]            # 0 => unlimited
    bucket = "user"               # single bucket across all models
# 2. Per-model config
elif b_cfg.get("rate_limit", {}).get(dim) is not None:
    limit  = b_cfg["rate_limit"][dim]   # 0 => unlimited
    bucket = "user_model"                # bucket per (user, model)
# 3. Global config
elif cfg.get("rate_limit", {}).get(dim) is not None:
    limit  = cfg["rate_limit"][dim]      # 0 => unlimited
    bucket = "user"
# 4. Nothing configured
else:
    limit  = None                        # unlimited, no enforcement
    bucket = None

if limit in (None, 0):
    skip enforcement for this dimension   # 0 => explicitly unlimited
```

**Counter-key / bucket rule (the one subtle bit):**
- Limit from a **per-user override or global** → a **single bucket per user**
  (a per-user "5 concurrent" = 5 *total* across all models; a global "60 rpm"
  = 60 per user across all models).
- Limit from a **per-model** config → a bucket **per `(user, model)`** (that
  model's limit is independent of other models).

So a per-user override is a true total cap; per-model limits are scoped to that
model. One `if`-branch in the resolver; identical rule for all four dimensions.

---

## 6. Enforcement placement & flow

### 6.1 Where the checks live

All three checks need `user` + `b_name` + `b_cfg`, which are only available
**inside** `proxy.request` (after the body is parsed and the model resolved),
**before** `app["client"].post(...)`. Thread `user` through as a new optional
parameter (handlers already obtain it from `require_auth`):

```python
# chat.py, embeddings.py, messages.py, responses.py, audio.py — all five:
user = await auth.require_auth(f_req)
async with proxy.request(f_req, body_transform, user=user) as (b_res, b_name, b_cfg):
    ...
```

`proxy.request` gains an optional `user=None` kwarg (default preserves current
behaviour for any caller that doesn't pass it). After model resolution, before
posting, it enters a new async context manager:

```python
async with ratelimit.slot(f_req, user, b_name, b_cfg) as ok:
    if not ok:                 # 429 already raised inside; here for clarity
        ...
    async with app["client"].post(...) as b_res:
        ...
        yield b_res, b_name, b_cfg
```

(Implementation detail: `slot` is an `@contextlib.asynccontextmanager` that does
the checks, yields, and releases the concurrency slot in `finally`. A 429 is
raised by raising `HTTPTooManyRequests` *before* `yield`, so the backend `post`
is never opened for a rejected request.)

### 6.2 `slot` ordering (matters)

1. Resolve all four limits + bucket keys (cheap, pure).
2. **`rpm`** — in-memory deque per bucket key: evict entries older than 60 s; if
   `sum + 1 > limit` → raise 429 (do **not** append); else append `(now, 1)`.
3. **`concurrency`** — `in_flight[key] += 1`; if `> limit` → decrement back and
   raise 429 (never hold a slot for a rejection).
4. **[P2] `prompt_tpd` / `completion_tpd`** — **skipped entirely when both limits
   resolve to `None`/`0`** (the default deployment), and skipped for the audio
   path (no tokens). Otherwise `await ratelimit.usage_today(...)`, which is
   served from the in-process cache (§16.3) and only touches the DB on a cold
   `(subject, date)`. If `prompt >= prompt_tpd` or `completion >=
   completion_tpd` → **release the concurrency slot acquired in step 3**, then
   raise 429.
5. `yield` → the backend request runs.
6. **`finally`** → release the concurrency slot (`in_flight[key] -= 1`). The
   `finally` covers streaming **and** the post-disconnect drain (the existing
   `streaming.drain` keeps reading after the client vanishes; the slot is held
   until the handler returns).

**Phase 1 ends at step 3** — steps 1–3, 5 and 6 are the whole `slot`. Step 4 is
added in phase 2, which is why the ordering is worth getting right now: phase 1
should not have to be re-sequenced later.

Ordering rationale: **both** free in-memory checks (`rpm`, then `concurrency`)
come before the potentially-DB-backed one (`tpd`), so a request rejected for
concurrency never pays for a usage lookup. Concurrency is acquired before the
TPD check purely because it is free; step 4 releases it on rejection, so a
rejected request still never holds a slot.

> Earlier drafts ordered this `rpm` → `tpd` → `concurrency` on the rationale
> that concurrency should be acquired last. That put the *most* expensive check
> ahead of a free one: a key already over its concurrency limit would do a usage
> lookup on every rejected request — the exact load-shedding path that must stay
> cheap.

### 6.3 TPD increment (post-response) — [P2]

Each handler already computes `prompt_tokens` / `completion_tokens` in its
billing tail (chat, embeddings, messages, responses). Add **one** call next to
the existing `metrics.observe_text_tokens(...)`:

```python
await ratelimit.record_usage(f_req, user, b_name,
    prompt_tokens, completion_tokens)
```

Same per-handler pattern as `observe_text_tokens`, **deliberately separate**:
metrics = best-effort, process-global; quota = DB-persisted, durable. Audio's
handler does **not** call `record_usage` (it bills seconds, not tokens).

**Failure policy:** on `billing.record`'s `GracefulExit` (DB failure kills the
worker), the increment never runs — consistent with the existing "billing is the
moat" policy: a failed bill does not penalise the user's quota. Conversely, a
`record_usage` DB failure should **log + not kill** the worker (quota is
best-effort enforcement, not revenue) — but should be surfaced in logs/metrics so
it doesn't silently let a user exceed. (Resolved in §15.1.)

`embeddings` counts only prompt tokens → `record_usage(user, model,
prompt_tokens, 0)`.

---

## 7. Audio handling

- Audio **is** subject to `rpm` and `concurrency` (it goes through
  `ratelimit.slot` like every handler).
- Audio is **exempt** from `prompt_tpd` / `completion_tpd`: the resolver still
  runs, but the audio handler never calls `record_usage` (no tokens to account),
  and the pre-forward TPD *check* is skipped for the audio path. A user whose
  token TPD is exhausted can still transcribe.
- A future `audio_seconds_tpd` dimension would reuse the exact same machinery;
  **out of scope** for this change.

---

## 8. The 429 response

Status `429`, plus a `Retry-After` header where deterministic, plus a body
shaped per API flavour so client SDKs parse it natively. Flavour chosen by
`f_req.rel_url.path`:

| Path | Body flavour |
|---|---|
| `/v1/chat/completions`, `/v1/completions`, `/v1/embeddings`, `/v1/responses`, `/v1/audio/transcriptions` | OpenAI |
| `/v1/messages` | Anthropic |

**`Retry-After`:**
- `rpm` → seconds until the oldest in-window entry expires (ceil to ≥1).
- `concurrency` → omitted (no deterministic time; depends on in-flight finishing).
- `prompt_tpd` / `completion_tpd` → seconds to UTC midnight.

**Body examples:**

OpenAI flavour:
```json
{
  "error": {
    "message": "Rate limit exceeded: 60 rpm (model 'llama3-8b').",
    "type": "rate_limit_exceeded",
    "code": "rate_limit_exceeded"
  }
}
```

Anthropic flavour (`/v1/messages`):
```json
{
  "type": "error",
  "error": { "type": "rate_limit_error", "message": "Rate limit exceeded: 60 rpm (model 'llama3-8b')." }
}
```

The `message` names the dimension, the limit, and the scope:
- `rpm`: `"Rate limit exceeded: 60 rpm (model 'llama3-8b')."`
- `concurrency`: `"Concurrency limit exceeded: 5."`
- `prompt_tpd`: `"Daily token limit exceeded: 10000000 prompt_tpd."`
- `completion_tpd`: `"Daily token limit exceeded: 1000000 completion_tpd."`

---

## 9. Metrics

Add one counter to `metrics.py`:

```
llmproxy_rate_limit_rejections_total{model, dimension}
```

`dimension ∈ {rpm, concurrency, prompt_tpd, completion_tpd}`. Increment on each
429 raised by `ratelimit.slot`. The existing `examples/prometheus-alerts.yml`
can alert on a non-zero rate of this counter. (Keep the existing
`llmproxy_active_requests` gauge untouched — it already counts all in-flight
requests, independent of the new concurrency *limit*.)

Plus, for §16:

```
llmproxy_auth_cache_hits_total
llmproxy_auth_cache_misses_total
llmproxy_rate_limit_accounting_errors_total
```

The accounting-errors counter is the one that must be alerted on: it is the
signal that quota enforcement has silently degraded (§15.1). A hit-rate near
zero on the auth cache means the TTL is too short to be doing anything.

---

## 10. `ctl` (llmproxyctl) changes

### `user create`
Add four optional flags, each taking a non-negative int:
`--rpm`, `--concurrency`, `--prompt-tpd`, `--completion-tpd`.
- Absent → column `NULL` (fall through).
- `0` → unlimited.
- `>0` → cap.

### `user list`
Print the four new columns (in addition to the existing hash/expires/status/comment).

### `user update`
Add the same four optional flags.

**Semantics (per the owner's decision):** the rate-limit flags use **replace
semantics** — *omitted means clear to NULL* (fall through), not "leave unchanged".

- `--rpm 30` → set to 30
- `--rpm 0` → set to 0 (unlimited)
- flag omitted → that column is set to `NULL`

**Footgun guard:** to prevent an admin who is *only* editing `expires`/`comment`
from accidentally wiping a key's rate-limit overrides, apply this rule:

- If **≥1** rate-limit flag is provided → write **all four** rate-limit columns
  (provided flags get their value; omitted flags → `NULL`). This is the
  "declare the full rate-limit profile" mode.
- If **0** rate-limit flags are provided → rate-limit columns are **left
  untouched** (only `expires`/`comment` are affected, exactly as today).

The existing guard ("at least one of expires/comment/rpm/concurrency/
prompt_tpd/completion_tpd must be provided, else error 'No fields to update'")
is kept, extended to include the four new flags.

> **Admin workflow note:** because omitted flags clear to `NULL` when editing
> rate limits, run `user list` first and re-pass any values you want to keep.
> This differs from the existing `expires`/`comment` (merge semantics) and is
> intentional, per owner decision. If the team prefers uniform merge semantics
> (omitted = unchanged, `--flag ""` = clear), it is a one-line change in
> `command_user_update` — flagged here for awareness.

### Docs
Update `README.md` (rate-limiting section) and the comments in
`llmproxy/config.toml` (new `[rate_limit]` block + per-backend sub-table, with
the `0 = unlimited` / `NULL = fall through` semantics called out).

---

## 11. Backward compatibility (the hard requirement)

- **No `[rate_limit]`, no per-model sub-table, all DB columns `NULL`** →
  **zero behavioural change**. Every dimension falls through to "unlimited".
- **Existing v1 SQLite DB** auto-migrates to v2 at startup: `_ensure_column`
  adds the 4 nullable columns, `CREATE TABLE IF NOT EXISTS` adds the daily
  table, all existing rows preserved (the `test_db.py` suite already encodes
  "preexisting data is preserved"; we extend it).
- **v0 / manually-created DB** (`sqlite3 db.sqlite < schema.sql` before this
  change) healed via `_ensure_column` (the documented v0 footgun).
- **No new dependencies.** `pyproject.toml` unchanged (in-memory deques use
  stdlib `collections.deque`; the daily table uses the existing `aiosqlite`).
- **Mongo read path** keeps working: override fields optional (missing → config
  fallback); `user_create`/`user_update` stay `NotImplementedError` (platform
  manages them); `usage_today`/`usage_add` implemented via `$sum`/`$inc` to
  mirror `billing_record_add`.
- **`/v1/models`** left unrate-limited (cheap read, no model in body, no
  forwarding).
- **`proxy.request`** keeps its current signature default (`user=None`) so any
  caller that doesn't pass `user` behaves exactly as before.

**Two honest exceptions to "zero behavioural change":**

1. **`auth_cache_ttl` defaults to `5`** (§16.2), so a *revoked* key can keep
   working for up to 5 s. Key expiry is unaffected. Set `auth_cache_ttl = 0` for
   byte-identical current semantics. Called out because it is security-visible.
2. **`test_db.py` does not pass verbatim.** It asserts `user_version == 1` in
   five places (lines 51, 67, 77, 104, 122); the v2 stamp breaks all five. The
   *proxy* behaviour is unchanged — the test constants are not. §12.3's "the
   existing test suite still passes verbatim" was overstated and is corrected
   here.

---

## 12. Test plan

Split by phase: everything mentioning `rpm`, `concurrency`, precedence, the
bucket rule, the migration ladder or the 429 body is **phase 1**; everything
mentioning TPD, `usage_today`/`usage_add`, the daily table or the usage cache is
**phase 2**.

### 12.1 `tests/test_ratelimit.py` (new)
- **Resolution precedence:** per-user wins over per-model wins over global;
  `0 = unlimited` (no enforcement); `NULL`/absent = fall through.
- **Counter-key / bucket rule:** per-user/global → single per-user bucket
  (e.g. one user hitting two models shares the rpm bucket); per-model →
  per-`(user,model)` bucket (independent counts).
- **RPM sliding window:** eviction of >60 s entries; rejection + `Retry-After`
  math (seconds to oldest entry expiry); a 4th request after the window
  succeeds.
- **Concurrency:** acquire/release; release on exception; release on
  client-disconnect-during-stream (simulate via `streaming.drain`); 2nd
  concurrent over the limit → 429 (no `Retry-After`).
- **TPD:** pre-check rejects when over; `record_usage` accumulates same-day;
  midnight rollover (mock `date('now')`); per-user `SUM` across models vs
  per-model single-row read.
- **Audio exemption:** audio path skips TPD checks (unit-level on the
  resolver / slot).

### 12.2 `tests/test_db.py` (extend)
- v1 → v2: columns + daily table added; `user_version` → 2; data preserved.
- v0-with-preexisting-old-`api_key`-table → `_ensure_column` heals; data
  preserved; version stamped 2.
- `user_list` returns the 4 new columns (NULL for legacy rows).
- `user_create` / `user_update` round-trip the 4 fields (incl. `0` = unlimited,
  clear to NULL).
- `usage_today`: empty → `(0,0)`; per-user `SUM` across two model rows;
  per-model read filters; midnight boundary (different `date`).
- `usage_add`: atomic upsert; same-day accumulation; cross-model independence;
  idempotent under conflict.

### 12.3 `tests/test_proxy.py` (extend, integration via `mockbackend`)
- `rpm=2` → 3rd request 429 (correct body flavour + `Retry-After`), 4th after
  window succeeds.
- `concurrency=1` → 2nd concurrent (use a slow/mock backend response) blocked
  with 429.
- `prompt_tpd=N` → requests succeed until over, then 429; reset next "day"
  (mock date); `completion_tpd` analogous.
- Per-model limit scoped to that model (a second model unaffected).
- Per-user `0` override exempts a key despite a global limit.
- **No limits configured → behaviour unchanged** (golden test: existing
  `test_proxy.py` / `test_messages.py` / `test_responses.py` pass verbatim.
  `test_db.py` does **not** — its `user_version == 1` assertions move to `2`;
  see §11.)
- 429 body shape assertions for OpenAI-flavour paths **and** `/v1/messages`
  (Anthropic flavour).

### 12.4 `tests/test_cache.py` (new — §16)

**Written and passing (13 tests) for §16.1 and §16.2:**
- **Mongo client reuse:** two `create` calls for the same URI return the same
  instance, a different URI does not; the per-request `close()` does **not**
  tear the client down; `shutdown_all()` does.
- **Auth cache:** hit avoids a second `user_list` (counted via a fake db); TTL
  expiry forces a re-read; a row whose `expires` has passed while still cached
  is rejected **without** a DB round trip and the dead entry is dropped;
  `expires` normalizes identically from a naive ISO string, an aware ISO string
  and a datetime; an unparseable `expires` is never cached (fail safe); a
  negative result is cached; the map stays at or under `CACHE_MAX_ENTRIES`
  after `CACHE_MAX_ENTRIES + 50` distinct bad keys; `flush_cache()` clears; a
  caller mutating the returned dict cannot poison the cache; a bad auth scheme
  never touches the database.

**Still to write, with §16.3:**
- **Usage cache:** cold `(subject, date)` issues exactly one DB read covering
  all model rows; subsequent per-user *and* per-model reads are served from
  memory; `usage_add` is write-through (DB row updated **and** memory updated);
  a simulated DB write failure keeps the in-memory increment and increments the
  error metric; date rollover evicts stale entries; `usage_cache = false` falls
  back to a direct read on every call.
- **Restart fidelity:** seed the DB, drop the cache, re-read — totals match.

---

## 13. File-by-file change list

### Phase 0 — performance groundwork (§16) — DONE except pragmas

| File | Change |
|---|---|
| `llmproxy/db.py` | ✅ Process-global Mongo client + `shutdown_all()` (§16.1). **TODO:** SQLite pragmas (WAL / `synchronous=NORMAL` / `busy_timeout`) — defer to phase 2, which is what adds write transactions. |
| `llmproxy/auth.py` | ✅ Auth cache (§16.2). |
| `llmproxy/app.py`, `ctl.py`, `config.py`, `config.toml`, `metrics.py`, `README.md` | ✅ Wiring, `auth_cache_ttl`, cache counters, docs. |
| `tests/test_cache.py` | ✅ New, 13 tests. |

### Phase 1 — `rpm` + `concurrency`

| File | Change |
|---|---|
| `llmproxy/schema.sql` | Add `rpm`, `concurrency` to `api_key`. |
| `llmproxy/db.py` | **SQLite:** migration ladder (v0/v1 → **v2**) + `_ensure_column`; `user_list`/`user_update`/`user_create` carry the 2 fields. **Mongo:** `user_list` returns the 2 fields from the key doc when present, `None` otherwise (no collection, index or migration); `user_create`/`user_update` stay `NotImplementedError`. |
| `llmproxy/config.py` | `validate()` `[rate_limit]` + per-backend sub-tables (non-neg ints); `reload_config` reloads `[rate_limit]`. |
| `llmproxy/config.toml` | `[rate_limit]` defaults + `[backends.*.rate_limit]` example. |
| `llmproxy/ratelimit.py` | **NEW** — resolver, in-memory rpm deque + concurrency counter, `slot` async CM, 429 builder (per-flavour body + `Retry-After`), bucket sweeper. |
| `llmproxy/proxy.py` | Accept optional `user=`; enter `ratelimit.slot(...)` around the backend `post`. |
| `llmproxy/chat.py`, `embeddings.py`, `messages.py`, `responses.py`, `audio.py` | Pass `user=user` into `proxy.request(...)`. **No billing-tail change.** |
| `llmproxy/metrics.py` | `llmproxy_rate_limit_rejections_total{model, dimension}`. |
| `llmproxy/ctl.py` | `user create`/`list`/`update` flags for `--rpm`, `--concurrency`. |
| `README.md` | Rate-limiting section. |
| `tests/test_ratelimit.py` | **NEW** (§12.1, rpm/concurrency parts). |
| `tests/test_db.py`, `tests/test_proxy.py` | Extend (§12.2 / §12.3, rpm/concurrency parts). |

### Phase 2 — `prompt_tpd` + `completion_tpd`

| File | Change |
|---|---|
| `llmproxy/schema.sql` | Add `prompt_tpd`, `completion_tpd`; add `api_key_daily_usage`. |
| `llmproxy/db.py` | One new ladder rung (v2 → **v3**); `usage_today` + `usage_add` (SQLite + Mongo); Mongo collection + **unique index** `{subject,date,model}`; SQLite pragmas. |
| `llmproxy/ratelimit.py` | Two more dimensions in the resolver; TPD step in `slot`; `record_usage`; **usage cache** (§16.3); accounting-error metric. |
| `llmproxy/chat.py`, `embeddings.py`, `messages.py`, `responses.py` | `ratelimit.record_usage(...)` in the billing tail. **`audio.py` does not** — it bills seconds. |
| `llmproxy/config.py` | Validate `*_tpd`, `usage_cache`, and the **global concurrency floor** (§3). |
| `llmproxy/ctl.py` | `--prompt-tpd`, `--completion-tpd`. |
| `tests/test_ratelimit.py`, `test_db.py`, `test_proxy.py`, `test_cache.py` | Extend for TPD + the usage cache. |

No changes to `pyproject.toml`, `Dockerfile`, `compose.yaml`, or the `k8s/`
manifests in either phase.

---

## 14. Implementation order

### Phase 0 — performance groundwork (§16)

1. ✅ Process-global Mongo client (§16.1).
2. ✅ Auth cache (§16.2).
3. SQLite pragmas (WAL / `synchronous=NORMAL` / `busy_timeout`) — **deferred to
   phase 2**, which is what adds a second write transaction per request. Phase 1
   writes nothing, so it does not need them.

### Phase 1 — `rpm` + `concurrency`

0. **⛔ Answer §17.6 question 3 before writing `schema.sql`:** is per-`(user,
   model)` scoping needed for `rpm`/`concurrency`, or only for TPD? If only TPD,
   phase 1's scalar columns are correct and phase 1 proceeds unchanged. If it is
   needed here too, phase 1 must ship the final shape (profile name or limit
   table) rather than scalar columns that phase 2 would have to migrate away
   from (§17.5).
1. **Decide §15.5 (two-phase enforcement)** — it determines where `slot`
   is called from, and retrofitting it later means touching all five handlers a
   second time. It is also cheapest to do now: phase 1 has no DB read to
   sequence around.
2. `schema.sql` + `db.py` migration ladder (v0/v1 → v2) + `_ensure_column` +
   `user_list`/`user_create`/`user_update` — extend `test_db.py` (its five
   `user_version == 1` assertions become `2`). **Both backends:** SQLite gets
   the columns and the ladder; Mongo gets two optional field reads in
   `user_list` and nothing else. Decide §15.8 (tier limits) here or not at all —
   it adds a precedence level and is far cheaper before step 4 than after.
3. `config.py` validation + `config.toml` docs.
4. `ratelimit.py`: resolver, rpm deque (`len()`, **not** `sum()`), concurrency
   counter, `slot`, 429 builder, bucket sweeper — plus `test_ratelimit.py`.
5. Wire `proxy.py` + the five handlers; `metrics.py` rejection counter.
6. `ctl.py` `--rpm` / `--concurrency`.
7. `tests/test_proxy.py` integration tests.
8. `README.md`.

Steps 2–4 add no behavioural change until step 5 wires enforcement in.

**Phase 1 exit criteria** — all must hold before phase 2 starts:
- A key over `rpm` gets a 429 with a correct `Retry-After`, in both API
  flavours, and the next request after the window succeeds.
- A key over `concurrency` gets a 429 with no `Retry-After`, and the counter
  returns to zero after: normal completion, handler exception, backend timeout,
  and mid-stream client disconnect.
- With no `[rate_limit]` and all columns NULL, the existing suite passes and no
  429 is ever raised.
- Bucket maps do not grow without bound across a soak run.

### Phase 2 — `prompt_tpd` + `completion_tpd`

0. **⛔ GATE: resolve §17 — the limit model.** Phase 2 cannot be specified, let
   alone built, until it is settled how a limit is scoped to `(user, model)`,
   whether limits compose by precedence or conjunction, and where they are
   stored. Answering §17.6's seven questions is the deliverable. **Do this
   before phase 1's `schema.sql` is written** (§17.5), not before phase 2.
1. **Answer the open Mongo questions** (§15.1 read policy, collection
   name, who creates the unique index, identity type, date encoding). Phase 2 is
   mostly Mongo work; starting to code before these are settled is how the
   silent-undercount bugs get in. §17's answer to question 6 determines how big
   the CGC ask is.
2. SQLite pragmas (deferred from phase 0).
3. `schema.sql` + the v2 → v3 ladder rung + `usage_today` / `usage_add` on both
   backends + the Mongo unique index — extend `test_db.py`.
4. Usage cache (§16.3) + `test_cache.py`.
5. Two more dimensions in the resolver + the TPD step in `slot` + the global
   concurrency floor in `config.validate()`.
6. `record_usage` in the four token-billing handlers (not `audio.py`).
7. `ctl.py` `--prompt-tpd` / `--completion-tpd`; integration tests; README.

---

## 15. Open questions for the team (non-blocking)

### 15.1 `record_usage` DB failure policy — RESOLVED
**Lenient: log + `llmproxy_rate_limit_accounting_errors_total` + continue.**
The usage cache (§16.3) makes this materially safer than it was when first
raised: on a failed DB write the **in-memory** counter still increments, so the
limit keeps being enforced correctly for the life of the process and only a
restart during an outage loses accounting. Killing the worker (billing's
`GracefulExit` policy) is not warranted — quota is not revenue.

Still open, and *not* covered by the cache: **the read path.** `usage_today` on
a cold `(subject, date)` hits the DB, and on Mongo that is a network call that
can time out. Fail **open** (serve, log, metric — TPD becomes a soft limit) or
fail **closed** (503)? Recommend fail-open to match the write policy, but it
must be an explicit decision, not a default that falls out of the code.

### 15.2 Daily row pruning — [P2]
`api_key_daily_usage` grows ~one row/key/model/day. A startup sweep or scheduled
job deleting rows older than N days is **not** included. Decide retention window
(or accept unbounded growth for now).

### 15.3 Clock / `date('now')` mocking in tests — [P2]
SQLite `date('now')` and Mongo's UTC date both need deterministic values in
tests. Use a small injectable "today" helper (module-level `_today()` returning
`datetime.now(UTC).date().isoformat()`) that tests monkeypatch, rather than
freezing the real clock. Confirm this approach.

### 15.4 `/v1/models` rate limiting — [P1]
Left unrate-limited. If brute-force model enumeration is a concern, apply the
global `rpm`/`concurrency` defaults to it (user-only, no model). Belongs to
phase 1 if wanted at all — it is a rate concern, not a quota one, and it falls
out for free if §15.5's pre-body check lands (that check needs no model either).

### 15.5 Two-phase enforcement — [P1] DECIDE FIRST
The auth cache removes the DB round trip from a rejected request, but the
request still reads and JSON-parses **up to 32 MiB** of body before
`ratelimit.slot` is reached, because §6.1 places the checks inside
`proxy.request`. That defeats the "protect llmproxy from one api-key" goal:
the swarm is rejected only after the proxy has done the expensive work.

`user` is available in every handler *before* `proxy.request` is entered, and a
limit resolved from the **per-key column or the global default** needs no model.
So the resolver can run in two phases — key-level limits enforced pre-body,
per-model limits enforced where they are today. Same resolver, called twice;
it already knows which precedence level it matched.

Not folded into this revision because it changes §6.1's structure. **Decide at
the start of phase 1**: it determines where `slot` is called from, retrofitting
it later means touching all five handlers twice, and it is cheapest to build
now because phase 1 has no DB read to sequence around. The auth cache (§16.2,
already shipped) is what makes the pre-body check nearly free.

Note this is *phase-1 shaped*: `rpm` and `concurrency` are exactly the
dimensions that want to reject before the body is read. Phase 2's TPD check
needs the model anyway, so it stays where §6.1 puts it.

### 15.6 Mongo per-key overrides — the CGC contract — [P1 scoping, P2 blocking]
`ctl` does not manage Mongo keys by design: CGC mints them. So a per-key `rpm`
or `concurrency` on Mongo can only come from the `cgc.api_keys` document, which
means CGC has to persist the fields.

**This does not block phase 1.** The safety switch works from config alone
(global + per-model), which is enough to stop one key swarming a backend. What
it blocks is per-key trial limits on Mongo (goal B).

Proposal for the contract — nest under one key to avoid collisions in a shared
collection, absent/`null` = fall through, `0` = unlimited:

```js
{ "llm_rate_limit": { "rpm": 60, "concurrency": 5,
                      "prompt_tpd": 0, "completion_tpd": 0 } }
```

Agree the shape with the platform team during phase 1 even if they implement it
later, so phase 2 does not stall waiting on a schema conversation.

### 15.7 Lifetime (non-daily) quota for trial keys — [P2]
`*_tpd` resets at UTC midnight, so a trial key with `prompt_tpd = 100000` grants
100k tokens *every day, forever*. Bounded only by `expires`. If "this key is
good for 1M tokens total" is a requirement, `api_key_daily_usage` already
supports it — the same `SUM` without the `date` filter — so it is two more
nullable columns and one branch in `usage_today`.

Two consequences if adopted: (a) it is **incompatible with row pruning**
(§15.2) unless a rolled-up total is kept on `api_key`, and that decision must
be made *before* rows are ever deleted; (b) `Retry-After` has no meaningful
value for an exhausted lifetime cap — omit it and word the 429 as terminal.

### 15.8 Tier-derived limits — the Mongo answer that needs no CGC work — [P1]
`MongoDatabase.user_list` already joins `cgc.rest_users` and reads
`subscription_level` into `_tier` (`db.py:150`), where it is currently used only
for billing. Adding a fourth precedence level keyed on it gives differentiated
per-customer limits on Mongo with **no schema change and no CGC dependency**:

```toml
[rate_limit]                    # fallback when tier is absent/unknown
rpm = 60
concurrency = 5

[rate_limit.tiers.free]
rpm = 10
concurrency = 1

[rate_limit.tiers.pro]
rpm = 600
concurrency = 20
```

Precedence becomes **per-key → tier → per-model → global**. Reloads on SIGHUP
like the rest of the config, and maps onto how the product is already priced.

Two things to decide if adopted: whether a tier limit buckets per **user** or
per **org** (a 20-seat org sharing one `concurrency = 20` behaves very
differently from 20 users each getting 20), and whether tier sits above or below
per-model in the ladder.

**Not currently in phase 1's scope** — it is listed here because it is the only
way to get non-uniform per-key-ish limits on Mongo without waiting on §15.6, and
that decision is cheapest to make before the resolver is written, not after.

---

## 16. Performance: connection reuse and caching

Three changes, agreed. §16.1 and §16.2 fix costs the proxy pays **today**;
§16.3 belongs to this feature. All three are per-process and assume the
single-replica deployment of §1.

> **§16.1 and §16.2 are IMPLEMENTED** (`db.py`, `auth.py`, `app.py`, `ctl.py`,
> `config.py`, `config.toml`, `metrics.py`, `tests/test_cache.py`). Full suite:
> 104 passed. **§16.3 is not built** — it depends on `usage_today`/`usage_add`,
> which land with the rate limiter itself.

The root problem they address: `get_db` builds a database connection **per
request** (`db.py:55-69`) and the `close_db` middleware tears it down when the
handler returns (`app.py:70-78`), with an uncached auth query inside it. This
plan adds two more DB operations on top of that. Fixing the substrate first is
worth more than the feature.

### 16.1 Process-global Mongo client — IMPLEMENTED

**Problem.** `MongoDatabase.create` runs `AsyncIOMotorClient(..., connect=True)`
plus `await db.admin.command("ping")` (`db.py:96-103`) on **every request**.
Motor is designed for one long-lived pooled client per process; as written,
pooling is defeated and each request pays TCP + TLS + auth + a ping before doing
any work. A request that will be rejected with 429 pays it too.

**Change.**
- Module-level `_clients: dict[str, MongoDatabase]` keyed by URI. `create()`
  returns the cached instance when present; the `ping` happens once, at startup.
- `MongoDatabase.close()` becomes a **no-op**, so the existing `close_db`
  middleware needs no change and keeps working for SQLite.
- New `MongoDatabase.shutdown()` classmethod closes every cached client; wire it
  to `app.on_cleanup` next to the existing `client_close`, and call it at the
  end of `ctl` commands so `asyncio.run` doesn't exit with a live client.
- Motor does its own topology monitoring and reconnection, so a long-lived
  client is *more* resilient to a Mongo blip than per-request creation, not
  less.

**Scope note.** SQLite is deliberately left on per-request connections. An
aiosqlite connection is a thread and serializes all queries through it, so
sharing one would serialize the whole proxy. Connection reuse there needs a
pool, which is a separate change with a different risk profile — not folded in
here. The SQLite pragmas (WAL, `synchronous=NORMAL`, `busy_timeout`) still
belong in step 0 and matter more once this feature doubles write transactions.

**Trade-off: none.** This is a defect fix.

### 16.2 Auth result cache — IMPLEMENTED

**Problem.** `require_auth` issues an uncached `user_list(digest)` on every
request (`auth.py:18`). Once §15.5 lands, this is the *only* remaining
per-request DB cost on the reject path — and until it lands it is the cost a
swarming key inflicts most cheaply.

**Change.** TTL'd map `sha256 digest → (row, monotonic_deadline)`. The digest is
already computed with no DB access (`auth.py:15`), so it is a free cache key.

- **`expires` is re-evaluated in Python on every hit.** Today expiry is filtered
  in SQL (`db.py:230-235`); a naive cache would keep an expired key alive for
  the whole TTL. Caching the row and comparing `row["expires"]` to the clock on
  each request keeps expiry **exact**. This matters directly for trial keys.
  - Normalize on the way in: SQLite returns `expires` as an ISO **string**,
    Mongo as a **tz-aware datetime** (`tz_aware=True`, `db.py:97`). Convert once
    at insertion so the hot path does a single comparison.
- **Negative caching is bounded.** Not caching misses leaves an invalid-key
  flood hitting the DB every request; caching them unbounded hands an attacker
  control of the map's cardinality. Use a small fixed-capacity LRU (≈4096) with
  a short TTL (≈5 s).
- **Invalidation is TTL + SIGHUP.** `reload_config` flushes the cache, giving
  operators an instant-revocation lever without a restart.
- The cached row carries the four rate-limit columns, so a cache hit also makes
  limit resolution free.
- Metrics: `llmproxy_auth_cache_hits_total`, `llmproxy_auth_cache_misses_total`.

**Trade-off — the one behavioural change in this plan.** A **revoked** key keeps
working for up to `auth_cache_ttl` seconds. Key *expiry* is unaffected (checked
in Python), and SIGHUP flushes on demand. Default `5`; set `0` to disable
entirely and restore today's exact semantics. **This needs explicit sign-off —
it is a security-visible change, not a pure optimization.**

**Second-order effect found while implementing.** On a cache hit `require_auth`
no longer calls `get_db`, so `req["db"]` is unset and the *first* code to need
the database is `billing.record`, in the handler's tail. For a **streaming**
request that tail runs after `write_eof()` — so the connection is now opened
after the client already holds the complete response.

Net cost is unchanged (one connection per billed request, opened later) and
rejected requests now open **none**, which is the point. But it exposed a
latent race in the test suite: five streaming tests read `event_oneoff`
immediately after the response completed and used to win only because the
connection already existed. They now poll via `get_events(expect=N)` until the
rows land or a timeout fires, so a genuine billing failure still fails rather
than hangs. `LLMProxyAppTestCase.asyncSetUp` also flushes the cache, since it
is process-global and each test builds a fresh database.

Worth knowing for §16.3 and for the rate limiter generally: **anything moved
into a handler's tail runs after the client has been served**, and tests that
assert on it must wait.

### 16.3 Daily usage cache (write-through) — [P2]

**Problem.** Without it, every request with a TPD limit configured does a usage
read before forwarding. On SQLite that is a local query; on Mongo it is a
network **aggregation** (`$match` + `$group`) per request.

**Change.**
- **Seed unit is `(subject, date)`, not `(subject, date, model)`.** One query
  loads *all* model rows for that key-day, so both bucket modes — per-user
  `SUM` across models and per-model single-row — are then served from memory. A
  per-model seed would leave the per-user `SUM` unable to know which models
  exist.
- **Write-through, never write-back.** `usage_add` updates memory **and** the DB
  on every call. A crash therefore loses nothing and a restart reseeds exactly;
  the DB stays the source of truth. On a DB write failure, keep the in-memory
  increment (so the limit still enforces) and emit
  `llmproxy_rate_limit_accounting_errors_total` — see §15.1.
- **Eviction.** Entries for past dates are dead weight: drop lazily when a
  looked-up entry's `date` is not today, plus a periodic sweep so an idle key's
  stale entry cannot linger indefinitely. Steady-state memory is
  `keys × models × 2 ints`.
- **Toggle.** `[rate_limit].usage_cache`, default `true`. `false` restores a
  direct DB read per call — required for multi-replica (§1).
- Interacts with §6.2 step 4: when no TPD limit applies, neither the cache nor
  the DB is touched at all.

**Trade-off.** Strict multi-replica TPD, which §1 previously singled out as the
one dimension that survived horizontal scaling. Acceptable under the locked
single-replica decision, and reversible via the toggle — but it is a real
property being given up, and §1 now records it.

### 16.4 What must NOT be cached

- **`billing.record`** — durable write, every time, no exceptions. Revenue moat.
- **`usage_add`** — write-through only; deferring the write loses quota
  accounting on crash and reseeds the counter low.
- **rpm / concurrency** — already in-memory by design (§1); that is the storage
  tier, not a cache, and it has no DB behind it to fall back to.

---

## 17. GATE — the limit model must be redesigned before phase 2

**Status: open. Blocks phase 2. Partly blocks phase 1's schema (see §17.5).**

### 17.1 The gap

Requirement: *limit particular users on particular models.* The design in §5
**cannot express that.** Its three levels are:

| Level | Scope | Expresses |
|---|---|---|
| per-user (`api_key` column) | one scalar per key | this user, **all** models |
| per-model (`[backends.X.rate_limit]`) | one scalar per backend | **all** users, this model |
| global (`[rate_limit]`) | one scalar | all users, all models |

There is no `(user, model)` cell anywhere. "Give Acme 1M prompt_tpd on
`llama3-8b` but 100k on `llama3-70b`" is unrepresentable — not a config
limitation but a data-model one. A per-`(user, model)` limit needs a **table**
(or a nested document), not a scalar column plus a scalar config key.

Note this is **not** specific to TPD. The same need almost certainly applies to
`concurrency` — "this customer may run 20 concurrent on the small model, 2 on
the 70B" is a more natural ask than a single number across a heterogeneous fleet
of GPUs. Which is why this partly reaches back into phase 1.

### 17.2 The crux: precedence or conjunction?

§5 resolves each dimension by **precedence** — exactly one level wins, and its
bucket rule follows from which level that was. That structurally cannot express
two caps at once.

But the natural business rule is a **conjunction**:

> Acme gets 5M prompt tokens/day **in total**, **and** at most 500k of that on
> the 70B model.

That is two limits with two different buckets — one per-user aggregate, one
per-`(user, model)` — both enforced on the same request. Precedence gives you
one or the other. Conjunction is a different evaluation model: collect every
applicable limit, check them all, and report whichever is hit first.

**This is the decision to make.** Everything else in §17 follows from it:

- If **precedence**: the resolver keeps its current shape; only the *lookup* has
  to gain a `(user, model)` level. Cheapest, but you can never say "1M total AND
  200k on the expensive one."
- If **conjunction**: the resolver returns a *list* of `(limit, bucket)` pairs
  per dimension; `slot` loops over them. The rpm deque and concurrency counter
  must then be keyed by bucket, which they already are — so the change is
  smaller than it sounds, and it is *far* cheaper before `ratelimit.py` exists
  than after.

Recommendation: **conjunction**, with a per-user aggregate and an optional
per-`(user, model)` cap. It is what "limit particular users on particular
models" actually means once a customer has more than one model, and retrofitting
it means rewriting the resolver and every test that asserts on precedence.

### 17.3 Where the limits live — three shapes

**A. Per-`(key, model)` rows.** A `api_key_limit(api_key, model, rpm,
concurrency, prompt_tpd, completion_tpd)` table, `model = '*'` meaning
all-models. Maximum flexibility; subsumes the phase-1 `api_key` columns (the
`'*'` row *is* the column). Cost: `ctl` grows a per-model editing surface,
`user list` output becomes a nested thing, and on Mongo it is a nested document
CGC has to write.

**B. Named profiles / plans.** Define limit sets in config, store only a
**name** on the key:

```toml
[rate_limit.profiles.trial]
rpm = 10
concurrency = 1
prompt_tpd = 100000
[rate_limit.profiles.trial.models.llama3-70b]
prompt_tpd = 0            # or "denied" — see §17.4
```

The key carries `profile = "trial"`. Cost: no one-off per-key numbers without
inventing a profile. Benefits are large: **one string column** on `api_key`;
**CGC only has to store a name**, not four numbers per model — a much smaller
ask than §15.6; SIGHUP reload for free; admins pick from a reviewed set instead
of typing numbers; and it composes with §15.8 (`subscription_level` *is* a
profile name).

**C. Hybrid.** Profiles as the default, per-key rows (A) as an override for the
rare bespoke customer. Most commercial APIs land here.

Recommendation: **B now, with the resolver written so A can be added later** —
i.e. resolve through a function that returns the applicable limit set, not
through direct column reads scattered in the code. For a GPU cloud with
subscription tiers already in the database, profiles match the business model,
and they make the Mongo story dramatically easier.

### 17.4 Denial vs unlimited — `0` is overloaded

Locked decision §2.5 says `0 = unlimited`. Per-`(user, model)` limits introduce a
requirement `0` cannot express: **this user may not use this model at all**
(trial keys excluded from the 70B; a customer who has not paid for a GPU class).

Today that reads as `prompt_tpd = 0` → *unlimited* → the exact opposite of the
intent. A third state is needed — a `denied` sentinel, a negative value, or an
explicit `allow`/`deny` flag — and it should return **403**, not 429, because
retrying will never help. Decide alongside §17.3; it changes the config grammar.

### 17.5 What this means for phase 1 — decide now, not later

Phase 1 is specified to add `api_key.rpm` and `api_key.concurrency` as scalar
columns. **If §17.3 lands on profiles (B), those columns are the wrong shape** —
the key should carry a profile name instead, and the scalar columns become
either dead or a second source of truth for the same concept.

Concretely, phase 1's schema has three possible futures:

1. **Scalar columns stay** and become the `'*'`/no-profile fallback. Workable,
   but phase 2 then has two mechanisms for one concept.
2. **Columns are replaced** by `profile TEXT` — a v2 → v3 migration that has to
   *move* data, not just add it. Doable, but it is a real migration against live
   production rows rather than an additive one.
3. **Phase 1 ships the final shape from day one** — a decision made *now*, which
   costs nothing because no schema has landed yet.

Option 3 is obviously cheapest and is the reason this section is a gate rather
than a note. **§17 should be resolved before phase 1's `schema.sql` is
written**, not merely before phase 2 begins.

### 17.6 Questions to answer

1. Precedence or conjunction (§17.2)? — *the one that constrains everything else*
2. Profiles, per-`(key, model)` rows, or hybrid (§17.3)?
3. Is per-`(user, model)` needed for `rpm`/`concurrency` too, or only TPD?
   (If yes → phase 1 is affected. If only TPD → phase 1's columns survive.)
4. How is "no access to this model" expressed, and does it return 403 (§17.4)?
5. Does a per-user aggregate cap coexist with per-model caps, or replace it?
6. For Mongo: does CGC store a profile **name** (easy) or a nested limit
   **document** (harder)? This determines whether §15.6 is a small ask or a
   large one.
7. Does the bucket for an aggregate cap key on the API key, the user, or the org?
