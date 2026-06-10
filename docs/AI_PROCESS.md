# AI Process Documentation

This document describes how AI tools were used while producing the Email File Ingestion Pipeline design, as requested by the assignment.

## Tools used

- **AI coding assistant (LLM-based)** inside the IDE, used for requirements analysis, design exploration, and authoring this design doc.

## How AI was used, by stage

### 1. Requirements analysis
- Fed the raw assignment brief to the assistant and asked for a plain-language restatement to confirm a shared understanding of the four P0 responsibilities (discover, unpack, dedup, output) plus attachments and the P1 scaling section.
- Used it to enumerate the implicit constraints (unknown upload completion, multiple uploads per partition, crash safety) so they were not missed in the design.

### 2. Design exploration
- Asked the assistant to compare unique-identifier strategies (content hash vs. path-based vs. composite). The decision to use **SHA-256 of the leaf email bytes** came from this discussion, specifically because it is the only option that correctly handles the "two PSTs both emit `001.eml`" case while also deduplicating true byte-identical copies.
- Explored CDC options and converged on a **transactional SQLite ledger** keyed by source identity, with content-hash idempotency as the second line of defense for crash safety.
- Explored container unpacking and chose an **explicit worklist with depth/expansion guards** over naive recursion to stay safe against zip bombs and arbitrarily deep nesting.

### 3. Doc authoring
- The assistant drafted the design doc against a fixed section skeleton (Context, Flow Diagram, Phases, API Design, Integration, Effort, Special Considerations, Edge Cases, Deployment, QA, Planning), including the mermaid flow diagram, illustrative Python/SQL snippets, and the edge-case decision table.

## Artifacts generated

- [`docs/PLANNING.md`](PLANNING.md) — a planning file capturing the phasing decision and the key engineering decisions (unique id, CDC, lineage, unpacking, output layout) before drafting.
- `docs/DESIGN.md` — the technical design document.
- This file, `docs/AI_PROCESS.md`.

## Human review and validation

- Every AI-proposed decision was reviewed for correctness against the assignment's explicit edge cases; the edge-case table was checked one-by-one to ensure each of the ten listed cases maps to a deliberate, justified behavior.
- The phasing was adjusted so each phase is an independently shippable vertical slice rather than a horizontal layer, which better matches how the work would actually be delivered and demoed.
- Snippets were kept intentionally illustrative (design-level) rather than treated as final implementation, consistent with this being a design doc.
