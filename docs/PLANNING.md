# Planning Notes

Planning artifact captured before and during implementation, recording the
phasing decision and the key engineering decisions. The full rationale and
diagrams live in [DESIGN.md](DESIGN.md); this file is the condensed plan that
drove the build.

## Goal

Turn raw customer email uploads (date-partitioned files, possibly nested
containers) into a clean, deduplicated set of individual emails with full
lineage and durable, crash-safe state. Time-boxed to 1-3 hours, so scope is
phased.

## Phasing

- **P0 (build):** correct end-to-end pipeline over the provided fixtures and all
  ten edge cases — discovery + CDC, recursive unpacking, content-hash dedup,
  staging, lineage, skip tracking, crash safety. Emails staged intact.
- **P1 (design only):** attachment extraction, parallel workers + durable queue,
  object store + Postgres, streaming extraction for large archives, dead-letter
  quarantine. Documented in DESIGN.md "Production notes", not implemented.

Each phase is a vertical, shippable slice rather than a horizontal layer.

## Key engineering decisions

| Decision | Options considered | Choice | Why |
|----------|--------------------|--------|-----|
| Email unique id | path-based; `Message-ID`; SHA-256 of raw leaf bytes | **SHA-256 of leaf bytes** | Content-addressed: correct for "two containers both emit `001.eml`" (different bytes → different id) and collapses true duplicates; deterministic; enables idempotent writes. |
| Source identity (CDC) | path + size + mtime; path + content hash | **path + `source_hash` (SHA-256)** | mtime/size are fragile (re-uploads, copies, clock skew); hashing source bytes gives a true identity. Pre-filter by cheap signals at scale. |
| Container unpacking | recursion; explicit worklist | **explicit worklist (deque) + depth/expansion guards** | Avoids stack overflow on deep nesting; uniform handling of arbitrary nesting; zip-bomb protection. |
| State store | JSON files; SQLite | **SQLite (single source of truth)** | Transactional commits per source give crash safety with no extra infra; one place for sources, emails, lineage, skipped. |
| Dedup timing | on source; on leaf after unpacking | **on leaf bytes after unpacking** | The only point where identical-content and identical-inner-name cases resolve correctly. |
| Lineage | structured rows only; readable string | **ordered string (`!` hops, `#` indices) + rows** | Human-readable provenance plus queryable rows pointing at the staged id. |
| Output layout | flat; partitioned by date | **`staged/<partition>/<content_hash>.<ext>` + `manifest.jsonl`** | Deterministic, idempotent paths; downstream reads by partition; manifest for consumers. |

## Edge-case plan (behavior decided up front)

1. Same filename, different partitions → keep both (id is content, not path).
2. ZIP-in-ZIP → unroll via worklist.
3. Identical inner names, different bytes (ZIP and MBOX) → keep distinct.
4. Password-protected ZIP → skip, `encrypted_zip`.
5. Corrupted ZIP → skip, `corrupt_zip`; partition continues.
6. Non-email files → skip, `non_email`.
7. Empty container → mark source done, `empty_container`.
8. Crash mid-run → transactional state + idempotent writes → resume cleanly.
9. Re-upload to same partition across runs → deduped by source + content hash.
10. Deep chain ZIP→ZIP→MBOX→emails → unroll to `MAX_DEPTH`, else `depth_exceeded`.

## Testing plan

- Unit: classifier, lineage building, unpacker guards.
- Integration: full `run()` over a built bucket asserting staged count, lineages,
  and skip reasons.
- Crash-restart: inject a failure before commit, restart, assert no dupes/loss.

## Risks / open questions

- PST parsing depends on an optional library (`pypff`); absent → skip with reason.
- Raw-byte dedup treats any byte difference as distinct; production may prefer
  normalized content hashing.
- Hashing every source per scan is costly at scale → cheap pre-filter + stored
  hashes / object version IDs in production.
