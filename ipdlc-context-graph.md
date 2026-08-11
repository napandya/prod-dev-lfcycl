# iPDLC — Context Graph Layer (Detailed Design)

Companion to `ipdlc-architecture.md`, `ipdlc-kafka-flow.md`, `ipdlc-redis-streams-flow.md`, and `ipdlc-api-specification.md`. This document specifies the layer that makes the platform compound: the shared run context every agent reads and writes, the entity spine that connects runs to the things they touch, and the institutional memory that turns human decisions into retrievable precedent.

---

## 1. What the context graph is — and the decision hiding in the name

Three planes, one spine:

1. **Run plane** (per `correlation_id`, operational): `runs`, `shared_events`, `hitl_decisions` — the queryable truth of one pipeline execution. Hot for hours, then audit-only. Already designed; restated here for contracts.
2. **Entity spine** (cross-run, structural): the durable *things* runs touch — regulations, systems, products, teams, capabilities — and typed links between runs, memories, and those things. This is what makes it a graph.
3. **Memory plane** (cross-run, curated): one distilled record per completed run, anchored to a human verdict, embedded for similarity, linked to entities for structure.

**The decision in the name:** "graph" does not mean a graph database. The graph is foreign keys plus one link table in PostgreSQL. Neo4j or any dedicated graph store would be a fourth stateful system bought to answer queries that are one or two joins deep — "runs subject to Regulation E," "memories touching the mobile platform" — which relational handles trivially at this scale (thousands of runs, not billions of edges). The design keeps a clean seam: every graph read goes through the retrieval interface in section 6, so if traversal depth ever genuinely demands a graph engine, it swaps behind that interface. Until a query needs three-plus hops with variable depth, it will not.

Why the entity spine earns its place at all — the honest argument: pure vector similarity finds *textually* similar intents; it cannot answer "everything we have decided about Reg E" or "prior work on the Autopay capability," and those are exactly the questions a reviewer and a regulator ask. Entities turn memory from fuzzy recall into structured precedent. The cost is an extraction step in the distiller and one join at retrieval — cheap for what it buys.

---

## 2. The graph, drawn

```mermaid
erDiagram
  RUNS ||--o{ SHARED_EVENTS : "correlation_id"
  RUNS ||--o{ HITL_DECISIONS : "correlation_id"
  RUNS ||--o| MEMORY : "distilled into (0..1)"
  MEMORY }o--o{ ENTITIES : "via ENTITY_LINKS"
  RUNS }o--o{ ENTITIES : "via ENTITY_LINKS"
  MEMORY ||--o{ MEMORY_FEEDBACK : "cited by later runs"
  MEMORY |o--o| MEMORY : "superseded_by"

  ENTITIES {
    uuid entity_id PK
    varchar entity_type "regulation | system | product | team | capability"
    varchar canonical_name
    text_array aliases
    jsonb attrs
  }
  ENTITY_LINKS {
    uuid link_id PK
    uuid correlation_id FK "provenance run"
    uuid memory_id FK "nullable"
    uuid entity_id FK
    varchar relation "subject_to | integrates_with | targets | owned_by | supersedes"
    varchar confidence "human_confirmed | llm_extracted"
  }
  MEMORY {
    uuid memory_id PK
    uuid source_corr_id FK
    text intent_text
    vector embedding "768"
    varchar decision "approved | rejected | shipped | abandoned"
    text rationale "from the HUMAN comment"
    varchar entitlement
    varchar authored_by "human_verdict | llm_distilled"
    varchar status "active | superseded | quarantined"
    uuid superseded_by FK "nullable"
  }
  MEMORY_FEEDBACK {
    uuid feedback_id PK
    uuid memory_id FK
    uuid correlation_id FK "the run that retrieved it"
    boolean cited "surfaced in the report"
    boolean helpful "reviewer signal, nullable"
  }
```

A worked neighborhood — the Native Mobile PIL Autopay run after distillation:

```mermaid
flowchart LR
  RUN["run b0bcc68a<br/>Native Mobile PIL Autopay<br/>status complete"]
  MEM["memory record<br/>decision approved<br/>rationale: watch Reg E disclosures"]
  E1["regulation · Regulation E<br/>aliases: Reg E, EFTA"]
  E2["regulation · UDAAP / CFPB"]
  E3["capability · Autopay"]
  E4["system · Citi Mobile app"]
  E5["product · Personal Installment Loans"]
  PRIOR["memory: Card Autopay revamp (2025)<br/>decision rejected — vendor lock-in"]

  RUN -- "distilled into" --> MEM
  MEM -- "subject_to" --> E1
  MEM -- "subject_to" --> E2
  MEM -- "targets" --> E3
  MEM -- "integrates_with" --> E4
  MEM -- "targets" --> E5
  PRIOR -- "targets" --> E3
  E3 -. "shared entity connects precedents" .-> MEM
```

That dotted edge is the payoff: a future "Card autopay enhancements" run reaches the rejected 2025 precedent through the **Autopay** entity even if its intent text embeds far from the old one.

---

## 3. Schema — DDL beyond what the main document already defines

```sql
-- The durable things runs touch. Deliberately few types; resist taxonomy sprawl.
CREATE TABLE agent_context.entities (
  entity_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  entity_type    VARCHAR(50)  NOT NULL,       -- regulation | system | product | team | capability
  canonical_name VARCHAR(255) NOT NULL,
  aliases        TEXT[]       NOT NULL DEFAULT '{}',
  attrs          JSONB        NOT NULL DEFAULT '{}',   -- e.g. {"authority":"CFPB"} for regulations
  created_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
CREATE UNIQUE INDEX uq_entity ON agent_context.entities (entity_type, LOWER(canonical_name));

-- Typed edges. correlation_id gives every edge provenance; memory_id set when the
-- edge belongs to a distilled record rather than raw run context.
CREATE TABLE agent_context.entity_links (
  link_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  correlation_id UUID NOT NULL REFERENCES agent_context.runs(correlation_id),
  memory_id      UUID NULL REFERENCES agent_context.memory(memory_id),
  entity_id      UUID NOT NULL REFERENCES agent_context.entities(entity_id),
  relation       VARCHAR(50) NOT NULL,        -- subject_to | integrates_with | targets | owned_by | supersedes
  confidence     VARCHAR(20) NOT NULL,        -- human_confirmed | llm_extracted
  created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (correlation_id, entity_id, relation)
);
CREATE INDEX idx_links_entity ON agent_context.entity_links (entity_id);
CREATE INDEX idx_links_memory ON agent_context.entity_links (memory_id);

-- Memory lifecycle columns (extends the table defined in the main document).
ALTER TABLE agent_context.memory
  ADD COLUMN status        VARCHAR(20) NOT NULL DEFAULT 'active',   -- active | superseded | quarantined
  ADD COLUMN superseded_by UUID NULL REFERENCES agent_context.memory(memory_id);
CREATE INDEX idx_memory_active ON agent_context.memory (entitlement) WHERE status = 'active';

-- Closes the learning loop: was retrieved precedent actually useful?
CREATE TABLE agent_context.memory_feedback (
  feedback_id    UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  memory_id      UUID NOT NULL REFERENCES agent_context.memory(memory_id),
  correlation_id UUID NOT NULL REFERENCES agent_context.runs(correlation_id),
  cited          BOOLEAN NOT NULL,            -- surfaced in the produced report
  helpful        BOOLEAN NULL,                -- optional reviewer signal at the gate
  created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (memory_id, correlation_id)
);
```

---

## 4. Write contracts — who may write what (the monopolies)

```mermaid
flowchart TD
  subgraph RUNPLANE["Run plane — per correlation_id"]
    SE[("shared_events")]
    HD[("hitl_decisions")]
    RN[("runs")]
  end
  subgraph SPINE["Entity spine"]
    EN[("entities")]
    EL[("entity_links")]
  end
  subgraph MEMPLANE["Memory plane"]
    ME[("memory")]
    MF[("memory_feedback")]
  end
  AG["Agent workers"] -- "in-transaction, per step" --> SE
  AG -- "cursor · status via guarded tx" --> RN
  CP["Control Plane Service"] -- "gate decisions only" --> HD
  CP -- "run creation · transitions" --> RN
  DI["Distiller — the ONLY memory writer"] --> ME
  DI --> EN
  DI --> EL
  RET["Retrieval library"] -- "cited-flag on injection" --> MF
  CP -- "reviewer helpful-signal at the gate" --> MF
  STEW["Memory steward (human, Phase 4 ops role)"] -- "quarantine · supersede · confirm links" --> ME
```

Three monopolies, strictly held: **agents never write memory or entities** (they produce run context; the distiller curates); **only the Control Plane Service writes `hitl_decisions`** (a gate decision is an authenticated human action, never an agent inference); **only the distiller and the human steward touch the memory plane**. Every violation of these is a poisoning vector — the monopolies are the defense, not a style preference.

Read contracts are open by comparison: agents read `shared_events` for handoffs (rows guaranteed present by outbox ordering), retrieval reads memory + spine (entitlement-filtered), audit reads everything. One rule on reads: retrieved memory enters prompts **as delimited reference data, never as instructions** — the injection format in section 6 enforces it structurally.

---

## 5. The distiller — full specification

Trigger: `events.audit` message with `pipeline_complete` or `failed` (a rejection at a gate is a *valuable* memory — arguably the most valuable). Runs as one more Kafka consumer using the same claim-guard idempotency as every agent.

```mermaid
sequenceDiagram
  autonumber
  participant KF as Kafka events.audit
  participant DI as Distiller worker
  participant PG as PostgreSQL

  KF->>DI: pipeline_complete · correlation_id
  DI->>PG: claim (corr_id, 'distill', 1) ON CONFLICT DO NOTHING
  DI->>PG: read runs + hitl_decisions + shared_events summaries
  DI->>DI: extract — intent summary · entity candidates · tags (LLM, structured output)
  DI->>DI: embed(intent_text) — 768 dims
  DI->>PG: near-duplicate check — top-1 cosine within same entitlement
  alt similarity > 0.95 against an active record
    DI->>PG: link run to existing memory (entity_links) — no new record
  else new precedent
    DI->>PG: one tx — INSERT memory (decision + rationale FROM HUMAN ROWS) ·
    Note over DI,PG: upsert entities (alias match first — "Reg E" resolves to Regulation E) ·<br/>INSERT entity_links (confidence llm_extracted) · COMMIT
  end
  Note over DI,PG: no human decision rows exist → INSERT with status 'quarantined' — retrievable by nobody until the steward reviews
```

Hard rules, restated because each one blocks a failure mode: `decision` and `rationale` are copied from `hitl_decisions` rows — the model summarizes *around* them but never authors them. A run with no human decision distills only into quarantine. Entity extraction resolves against existing aliases before creating — the spine's value collapses if "Reg E," "Regulation E," and "EFTA" become three nodes. Entity *creation* is capped per run (say, five) — taxonomy sprawl is a real disease and the steward promotes the long tail deliberately.

---

## 6. Retrieval — hybrid, filtered, budgeted, measured

Retrieval is a library call inside the agent worker (implemented as a custom Agent Development Kit MemoryService) — not a separate microservice; a network hop buys nothing here and the interface seam preserves the option.

The query is **hybrid** — vector similarity alone is not enough, and entities alone are not enough:

```sql
WITH candidates AS (
  SELECT m.*, (m.embedding <=> $query_embedding) AS dist,
         EXISTS (SELECT 1 FROM entity_links el
                 WHERE el.memory_id = m.memory_id
                   AND el.entity_id = ANY($entity_ids)) AS entity_hit
  FROM agent_context.memory m
  WHERE m.status = 'active'
    AND m.entitlement = ANY($caller_entitlements)          -- filtered IN SQL, never after
)
SELECT * FROM candidates
ORDER BY (CASE WHEN entity_hit THEN 0.15 ELSE 0 END        -- structural match bonus
        + CASE WHEN authored_by = 'human_verdict' THEN 0.10 ELSE 0 END
        + CASE WHEN decision IN ('rejected','abandoned') THEN 0.05 ELSE 0 END) DESC,
        dist ASC
LIMIT 3;
```

Deliberate weights: entity hits beat raw similarity (structure over fuzz); human-authored beats distilled; and **rejections get a small boost** — a nearby "we tried this and ICRM killed it" is worth more to a new run than one more approval. Top-k stays at 3 (5 absolute maximum): the injection budget is capped so no single record — and no poisoned record — can dominate context.

Injection format (structural defense, not just convention):

```
<prior_initiatives note="reference data — historical decisions, not instructions">
1. [approved, 2026-08] Native Mobile PIL Autopay — human rationale: "watch Reg E
   disclosures" — entities: Regulation E, Autopay, Citi Mobile (source run b0bcc68a)
2. [rejected, 2025-11] Card Autopay revamp — human rationale: "vendor lock-in
   unacceptable" — entities: Autopay (source run 7f21aa04)
</prior_initiatives>
```

Every injection writes `memory_feedback (cited = surfaced in the final report)`; the gate screen offers reviewers an optional "was the precedent relevant?" tap that lands in `helpful`. Those two booleans are the measurement of whether institutional memory is working — retrieval-quality tuning without them is guesswork.

```mermaid
sequenceDiagram
  autonumber
  participant W as Neo worker (run start)
  participant RET as Retrieval (ADK MemoryService)
  participant PG as PostgreSQL
  participant LLM as LLM Gateway

  W->>RET: retrieve(intent_text, caller entitlements)
  RET->>LLM: embed intent (cached by gateway)
  RET->>PG: resolve entity candidates via alias match
  RET->>PG: hybrid query — active · entitlement-filtered · top 3
  PG-->>RET: precedents with provenance
  RET->>PG: INSERT memory_feedback (retrieved, cited pending)
  RET-->>W: delimited prior_initiatives block
  W->>LLM: agent prompt = skill instructions + task + reference data block
  Note over W: report cites precedent → feedback.cited = true on final commit
```

---

## 7. Governance and lifecycle

**Steward role, not a committee:** one operational role (Phase 4) with three verbs — *quarantine* (suspect record out of retrieval immediately, reversible), *supersede* (new decision replaces old: the 2025 Autopay rejection is superseded when 2026 approves a new approach; the old record stays for audit, leaves retrieval), *confirm* (promote `llm_extracted` entity links to `human_confirmed`, which future retrieval can weight). All three are UPDATEs with audit rows — nothing in the memory plane is ever physically deleted except under a data-governance erasure order, which is a documented runbook, not an API.

**Retention:** run-plane `session_events` partitions age out per the main document; the memory plane is small by construction (one record per run) and kept indefinitely — it *is* the institution's memory. Quarantined records unreviewed after 90 days page the steward, not auto-delete.

**Poisoning defenses, consolidated:** write monopolies (section 4) · human-verdict anchoring and quarantine-by-default for undecided runs (section 5) · delimited data-not-instructions injection with a capped top-k (section 6) · entitlement filtering in SQL · provenance on every record and every edge · feedback loop to detect a record that keeps getting retrieved but never helps.

**The audit story in one sentence:** for any claim an agent makes from memory, the chain *report → memory_id → source_corr_id → hitl_decisions row → human SOEID and comment* is four indexed joins — every remembered "fact" traces to a named human decision, which is the sentence that gets this layer through model-risk review.

---

## 8. Build order within the layer

1. Entity spine tables + distiller writing quarantined records only — memory accumulates under review before anything retrieves it (cold-start curation, and the steward tunes extraction quality on real runs risk-free).
2. Retrieval live for Neo only, `cited` tracking on — one agent, measured.
3. Reviewer `helpful` signal at the gate; weight tuning from feedback.
4. Extend retrieval to the Agent Trinity (Architecture) (prior NFR tiers and design-permit outcomes are its precedent) — then the rest.

---

# Part II — The Decision Layer (system of record for decisions)

Sections 1–8 specified the storage of the context graph. This part raises it to the bar set by the Foundation Capital "context graphs" thesis (Gupta & Garg, Dec 2025): the graph is not a memory feature — it is a **system of record for decisions**, capturing at commit time what the agent and the human did together, why it was allowed, and which precedent governed. iPDLC is structurally positioned for this: as the orchestration layer, every state change already passes through a transaction the platform controls at the moment of decision — capture is native, never reconstructed.

The distinction that drives the design: **rules** (skills, policies, rubrics) say what should happen in general; **decision traces** say what happened in this case — under which rule version, with whose exception, on which precedent. Skills are the rules layer; this part specifies the traces layer.

---

## 9. Decision records — the moments that matter, first-class

A decision record is written whenever context turned into a commitment. Not every event is a decision; the event stream stays as-is, and decisions become their own table — the distiller's primary source and the audit's primary object.

Decision types in iPDLC today, each currently under-captured:

| decision_type | Made by | What it commits | Where it hides today |
|---|---|---|---|
| `gate_verdict` | human | approve / reject / changes_requested at a HITL gate | hitl_decisions (verdict + comment only) |
| `jury_verdict` | agent | pass/fail with juror scores and judge reasoning | shared_events blob |
| `exception_override` | human | proceed despite a failed check (jury fail, policy breach) | invisible — looks like a plain approval |
| `degraded_proceed` | agent (policy) | continue with an unavailable external assessment | a caveat string in shared_events |
| `revision_response` | human + agent | changes_requested → what actually changed | lost between two report versions |
| `scope_change` | human | gate-time alteration of the product intent | lost in a comment |
| `auto_approval` | policy | precedent-based approval without a human (section 12) | does not exist yet |

```sql
CREATE TABLE agent_context.decision_records (
  decision_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  correlation_id  UUID NOT NULL REFERENCES agent_context.runs(correlation_id),
  decided_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  decision_type   VARCHAR(50) NOT NULL,
  question        TEXT NOT NULL,               -- what was being decided, in one sentence
  outcome         VARCHAR(50) NOT NULL,
  decided_by_kind VARCHAR(10) NOT NULL,        -- human | agent | policy
  decided_by      VARCHAR(255) NOT NULL,       -- SOEID, or agent/skill identifier
  authority       VARCHAR(100) NOT NULL,       -- the role/policy that made this decision valid
  rationale       TEXT,                        -- human words verbatim; or structured agent basis
  inputs          JSONB NOT NULL,              -- point-in-time refs: shared_event ids, external
                                               -- provenance ids, report version hash — what was
                                               -- ON THE TABLE when this was decided
  policy_context  JSONB NOT NULL,              -- {skill_versions:{...}, rubric_version, model_id,
                                               -- thresholds} — the rules in force at decision time
  alternatives    JSONB,                       -- options considered and rejected, when produced
  exception_of    VARCHAR(100) NULL,           -- rule this deviates from (names the exception)
  precedent_ids   UUID[] NOT NULL DEFAULT '{}',-- memory records consulted for this decision
  delta_summary   TEXT NULL                    -- for revision_response: what actually changed
);
CREATE INDEX idx_dr_corr ON agent_context.decision_records (correlation_id, decided_at);
CREATE INDEX idx_dr_type ON agent_context.decision_records (decision_type)
  WHERE decision_type IN ('exception_override','degraded_proceed','auto_approval');
```

Write rules extend the section 4 monopolies: humans' decision records are written only by the Control Plane Service in the gate transaction (hitl_decisions remains, now joined 1:1 from a `gate_verdict` record); agents' verdict records are written by the agent's final transaction; `auto_approval` records only by the graduation policy engine. `inputs` and `policy_context` are NOT NULL on purpose — a decision without its inputs and rules-in-force is the exact artifact this layer exists to prevent.

```mermaid
flowchart LR
  Q["Question<br/>approve Neo report for<br/>Native Mobile PIL Autopay?"]
  IN["Inputs — pinned at decision time<br/>report v3 hash · jury 86 · Reg E findings<br/>Tech Risk response id · 2 precedents"]
  PC["Policy context<br/>regulatory skill v14 · rubric v6<br/>model gemini-x · gate policy v2"]
  D["DECISION RECORD<br/>gate_verdict · approved<br/>by rk98431 · authority approver:consumer-lending"]
  R["Rationale (human, verbatim)<br/>proceed — watch Reg E disclosures"]
  OUT["Outcome links<br/>run advances → shared_events →<br/>distilled memory → future retrieval"]
  Q --> D
  IN --> D
  PC --> D
  D --> R
  D --> OUT
```

## 10. Replayable lineage — reconstructing decision time

The replay contract: for any decision_id, the platform can re-render **what the decider saw** — not today's state, the state then. What makes this cheap in iPDLC: `shared_events` is append-only (row ids in `inputs` are stable forever), external responses are stored verbatim with `retrieved_at` provenance, report versions are content-hashed, and `policy_context` pins the skill/rubric/model versions — so replay is a read, not an archaeology project. The audit endpoint grows one call: `GET /runs/{id}/decisions/{decision_id}/replay` returning the decision screen as-of. This is the bank-grade version of the thesis's "replayable lineage," and it is also the debugging tool: when a bad outcome surfaces months later, the question "what did we know and which rules were in force" has a queryable answer.

The one discipline it imposes going forward: **nothing a decision depends on may be mutable.** Any input that can change (an external system's live state) must be snapshotted into the trace at decision time — which the External Agent Gateway's provenance recording already does; this section makes it a stated invariant rather than a habit.

## 11. Exceptions and overrides — where the graph earns its keep

Routine decisions confirm rules; exceptions *are* the organizational knowledge. Three flows get explicit capture:

**Override at a gate.** Approving despite a jury fail or a flagged policy breach writes `exception_override` with `exception_of` naming the rule overridden, and rationale mandatory. The interface makes this a distinct action ("Approve as exception") — an override silently dressed as a normal approval is the single worst data-loss in the system.

**Degraded proceed.** The pipeline continuing past an unavailable external assessment writes `degraded_proceed` with `decided_by_kind = policy` and the timeout policy version in `policy_context` — making visible that *the platform decided*, under a rule, not that nothing happened.

**Revision loops.** `changes_requested` writes the verdict record; when the revised report passes, a `revision_response` record captures `delta_summary` — what the human asked for and what actually changed. This pair is the highest-density trace of human-agent collaboration the platform produces.

Retrieval (section 6) already boosts rejections; exception records rank higher still — "we deviated here, under this authority, for this reason" is precedent gold, and the distiller lifts `exception_override` and `revision_response` records into memory with their rationale intact.

## 12. Autonomy graduation — what precedent earns

The compounding loop needs a destination: as similar cases repeat with consistent human verdicts, the human's burden should fall — deliberately, measurably, reversibly.

```mermaid
stateDiagram-v2
  [*] --> L0
  L0 : L0 — Human decides<br/>agent presents, human reads everything
  L1 : L1 — Agent recommends<br/>pre-filled verdict + cited precedent + agreement stats — human confirms
  L2 : L2 — Policy auto-approves narrow classes<br/>human samples N percent · every auto_approval writes a full decision record
  L0 --> L1 : per gate-class — agreement rate ≥ 95 percent over ≥ 20 decisions
  L1 --> L2 : model-risk sign-off + entitlement-scoped class definition + kill switch registered
  L2 --> L1 : any sampled disagreement · any incident · steward or ICRM demotion
  L1 --> L0 : agreement degrades below threshold
```

The graduation *criteria come from the graph itself*: precedent density for the class, recommendation-vs-verdict agreement rate (computable because L1 records both), and the memory feedback signals. The banking-honest constraints: graduation is per narrow gate-class (e.g., "design-permit approvals for internal tools under NFR tier 3"), never global; L2 requires model-risk sign-off and a registered kill switch; demotion is one-way-fast (any sampled disagreement drops the class immediately); and an `auto_approval` decision record carries *more* context than a human one — the precedent set and policy version that justified it are its entire legitimacy. Full autonomy is not the goal; **earned reduction of human attention on repeats, with the graph as the evidence**, is.

## 13. What this makes iPDLC, strategically

Restated for the leadership deck: systems of record store what happened; iPDLC's context graph stores **why it was allowed to happen** — every gate, override, exception, and revision as a replayable, precedent-forming record, captured in the execution path at commit time. That is the asset that compounds: each run makes the next one faster, more consistent, and better-justified, and the whole graph stays inside Citi. The build order slots into the existing phases: decision records for gates and jury verdicts land with Phase 1 (they are one more insert in transactions that already exist); exception and revision capture with Phase 2's interface work; replay endpoint with Phase 3; graduation L1 with Phase 4's memory, L2 only after it.
