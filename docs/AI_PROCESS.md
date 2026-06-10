# AI Process Documentation

This document describes how AI tools were used to produce the Email File
Ingestion Pipeline, as requested by the assignment.

## Tools used

- **Cursor IDE** as the agentic coding environment.
- **Anthropic Claude Opus 4.8** as the underlying model.

## Approach: design-first, then implement, then harden

I worked the way I would on a real feature rather than letting the model
free-code. The process had three clear phases:

1. **Design first.** I iterated with the model on a technical design document —
   over several passes, against a fixed structure derived from the assignment —
   until the design genuinely looked right to me. Nothing was implemented until
   the design was settled.
2. **Implement from the approved design.** Once the design doc was final, I asked
   the model to implement it, so the code followed an agreed plan instead of being
   discovered ad hoc.
3. **Iterate and harden.** We then ran several tightening loops: fixing code,
   removing redundant files, and making sure every one of the ten edge cases was
   covered and backed by a test.

Throughout, the model was the fast collaborator; the decisions, the review, and
the "is this actually correct?" judgment stayed with me.

## How AI was used, by stage

### 1. Requirements analysis
- Fed the raw assignment brief to the model and asked for a plain-language
  restatement to confirm a shared understanding of the P0 responsibilities
  (discover, unpack, dedup, output) and the P1 scaling section.
- Used it to surface the implicit constraints (unknown upload completion,
  multiple uploads per partition, crash safety) so they were not missed.

### 2. Design-doc iterations
- Drove the design doc through several iterations against a fixed section
  skeleton (Context, Flow Diagram, Phases, Unique ID, CDC, Unpacking/Dedup,
  Edge Cases, Production Scale, Scope), matching what the assignment asks for.
- The key engineering decisions were made and pressure-tested here:
  - **Unique id = SHA-256 of the leaf email bytes** (content-addressed) — the only
    option that correctly handles "two containers both emit `001.eml`" while
    collapsing byte-identical copies. Compared against path-based and `Message-ID`.
  - **Transactional SQLite ledger** keyed by source identity, with content-hash
    idempotency as the second line of defense for crash safety.
  - **Explicit worklist unpacker with depth/expansion guards** over naive
    recursion, to stay safe against zip bombs and arbitrarily deep nesting.
- Stopped iterating only when the design read as something I'd be comfortable
  defending in review.

### 3. Implementation from the design
- With the design doc final, asked the model to implement it module by module
  (classification, unpacker, staging/dedup, SQLite store, discovery/CDC, CLI),
  so the code traced back to deliberate design choices.

### 4. Iteration and hardening
- Several review loops on the generated code: corrected behavior, simplified, and
  **removed redundant files** so the repo stayed lean.
- Walked the ten assignment edge cases one by one and added tests until each had
  explicit, justified coverage (including the MBOX variant of identical inner
  names and the deep nested-container chain).
- Verified end-to-end against the provided fixtures and the generated edge-case
  bucket, checking the staged output and the SQLite state.

## Artifacts generated

- [`docs/PLANNING.md`](PLANNING.md) — planning file capturing the phasing
  decision and the key engineering decisions before drafting.
- `docs/DESIGN.md` — the technical design document the implementation was built
  from.
- This file, `docs/AI_PROCESS.md`.

## Human review and validation

- Every AI-proposed decision was reviewed for correctness against the
  assignment's explicit edge cases; the edge-case table was checked one-by-one so
  each of the ten cases maps to a deliberate, justified behavior.
- The phasing was adjusted so each phase is an independently shippable vertical
  slice rather than a horizontal layer.
- Redundant files produced along the way were removed, and design-level snippets
  were kept illustrative rather than treated as final implementation.
