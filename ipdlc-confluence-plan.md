# iPDLC — Confluence Space Build Plan

How to stand the space up, then every page: its **Idea** (the question it answers), its **Overview** (the summary paragraph at the top — written here so it can be pasted), and its **Details** (sections, source artifact, owner, cadence). Build the space in the order this document is written.

---

# Part 1 — Creating the space

## 1.1 Space settings

| Setting | Value | Why |
|---|---|---|
| Space name | **iPDLC Platform** | Matches the product name everywhere else |
| Space key | **IPDLC** | Short, stable — it appears in every URL; never rename it |
| Space type | Documentation space (not team space) | Documentation template gives page-tree navigation by default |
| Homepage | Custom (Part 2, page 0) | The default homepage is a dead end; replace it immediately |

## 1.2 Permission model — set before writing anything

Three Confluence groups (request from your admin if they don't exist):

| Group | Space permission | Who |
|---|---|---|
| `ipdlc-core` | Add/edit pages, manage attachments | The build team |
| `ipdlc-risk` | Add/edit within *Governance & Risk* only (page restrictions) | ICRM / model-risk partners |
| Org default (all staff) | View only | Everyone else — the space is readable by default, restricted by exception |

Page-level restrictions applied in exactly one place: the *Governance & Risk* branch (view: core + risk + named leadership). Restricting broadly kills adoption; restrict the one branch that needs it.

## 1.3 Conventions — the rules that keep the space alive

**The source-of-truth rule (state it on the Home page, verbatim):**
> Sections 2–4 (*Architecture, Platform Engineering, Experience & Interfaces*) are **mirrored from git** — the repo is truth; edit via pull request, CI republishes on merge. Sections 1, 5–7 (*Strategy, Operations, Governance, Demos*) are **Confluence-native** — edit here directly. If a git-mirrored page looks wrong, the fix is a PR, not an edit.

**Page header block** — every page starts with the same 4-field table (make it a Confluence template named `ipdlc-page-header`):
`Owner (a name, not a team)` · `Status (draft / in-review / approved)` · `Last reviewed (date)` · `Source (repo path, or "confluence-native")`

**Labels** (applied at creation, used by every index macro): `ipdlc` on everything, plus one of `strategy` / `architecture` / `adr` / `engineering` / `interface` / `runbook` / `governance` / `demo`.

**Naming:** display name **Agent Trinity (Architecture)** for humans; `trinity` in all technical identifiers. Acronyms expanded on first use per page. The Go service is the **Control Plane Service**.

## 1.4 Plugins and mechanics to verify with the Confluence admin (before build day)

1. **Mermaid rendering** — a marketplace plugin (e.g. "Mermaid Diagrams for Confluence"). If not approvable in your instance: fallback is CI rendering each Mermaid block to SVG and attaching images beside the source block. Decide which before mirroring the engineering docs.
2. **HTML macro is almost certainly disabled** (correctly). The two interactive artifacts (`ipdlc-architecture-explorer.html`, `ipdlc-context-graph-briefing.html`) are **attachments with a "download and open in a browser" note**, or better, hosted on an internal static route and linked. Do not fight the sanitizer.
3. **Confluence REST API token** for the CI mirror job (a service account in `ipdlc-core`).

## 1.5 Build order

Day 1: space + groups + header template + Home page + section parent pages (empty shells with their Overview paragraphs). Day 2: Strategy pages + attach both HTML artifacts + the 8 ADRs. Day 3: run the CI mirror for sections 2–4. Week 2: Operations and Governance pages as their content is confirmed (`(confirm)` items resolved).

---

# Part 2 — The pages

Tree (page titles as they will appear):

```
IPDLC Home
├─ 1. Strategy & Leadership
│   ├─ Executive briefing — the context graph
│   ├─ Phase roadmap and exit criteria
│   └─ The decision-trace thesis
├─ 2. Architecture
│   ├─ System context and containers
│   ├─ Deployment — GTD and SWD
│   ├─ Security and trust boundaries
│   └─ Architecture Decision Records  (parent + ADR-001 … ADR-010)
├─ 3. Platform Engineering
│   ├─ Kafka — topics, configs, and flows
│   ├─ The outbox relay
│   ├─ Redis Streams and realtime delivery
│   ├─ Database schemas
│   └─ Context graph and decision layer
├─ 4. Experience & Interfaces
│   ├─ UI architecture (pdlc-ui)
│   └─ API specification
├─ 5. Operations
│   ├─ Runbooks  (parent + one child per runbook)
│   ├─ Dashboards and alerts
│   └─ Failure drills
├─ 6. Governance & Risk   [restricted]
│   ├─ Model-risk narrative
│   ├─ Memory stewardship
│   ├─ Entitlements and data classification
│   └─ Autonomy graduation policy
└─ 7. Demos
    ├─ Architecture explorer
    └─ Demo scripts
```

---

## Page 0 — IPDLC Home

**Idea.** The one page everyone lands on; routes each of the three audiences to their entrance in one click and answers "what is this and where does it stand" in thirty seconds.

**Overview (paste as the page intro).**
> iPDLC is Citi's multi-agent product development platform: from a raw product idea to a reviewable pull request, with every decision along the way captured as a permanent, replayable record. This space holds the strategy, the architecture, and the operating documentation. **Leadership:** start with the [Executive briefing]. **Engineers:** start with [System context and containers]. **Risk partners:** start with the [Model-risk narrative].

**Details.** Current phase + status line (kept current by hand — this is the one Home-page maintenance duty); the three audience links; the source-of-truth rule verbatim (§1.3); the artifact table from `ipdlc-README.md`; a "recently updated" macro filtered on label `ipdlc`. *Owner: platform lead. Confluence-native. Reviewed monthly.*

---

## Section 1 — Strategy & Leadership

### 1a. Executive briefing — the context graph

**Idea.** The Director/MD pitch, self-serve: why the platform's decision capture is an asset, not a feature.

**Overview.**
> Our systems record what happened; iPDLC records why it was allowed to happen. This page carries the interactive briefing — the anatomy of one real decision record, how precedent compounds, and what it earns — plus the five takeaways in text for anyone who won't open the attachment.

**Details.** The `ipdlc-context-graph-briefing.html` attachment with open instructions; the five takeaways written out (thesis · what gets lost today · the five-layer decision record · compounding · the three-stage ladder with its guardrail sentence); the "why this is ours to build" argument; link to the thesis page below. *Owner: platform lead. Confluence-native. Reviewed before each leadership presentation.*

### 1b. Phase roadmap and exit criteria

**Idea.** The single current answer to "where are we and what does done mean" — the page leadership checks between briefings.

**Overview.**
> The platform ships in five phases, each with exit criteria that are failure drills and measurable outcomes, not feature lists. Phase status is maintained here and only here.

**Details.** The phase table from `ipdlc-architecture.md` §1 with a live **Status** column added (not-started / in-progress / exited, with exit-drill dates); the Phase 1 headline ("kill a pod mid-run; replay a message; refresh mid-pipeline — all three pass"); the open `(confirm)` items as a task list (KaaS stretch-vs-mirror, PostgreSQL replica mode, COIN JWT claims and SameSite, HTTP/2 on the route, mandated frontend toolchain). *Owner: delivery lead. Confluence-native. Reviewed per sprint.*

### 1c. The decision-trace thesis

**Idea.** The intellectual grounding — why decision records rather than just logs — for anyone who asks "is this our idea or an industry direction."

**Overview.**
> Rules say what should happen in general; decision traces record what happened in this case — under which policy version, with whose exception, on which precedent. iPDLC applies this thesis to the product development lifecycle: the platform sits in the execution path, so capture happens at commit time, never reconstructed after the fact.

**Details.** Condensed from `ipdlc-context-graph.md` Part II intro + §13; the rules-vs-traces distinction with one PDLC example of each; reference to Foundation Capital "AI's trillion-dollar opportunity: context graphs" (Gupta & Garg, Dec 2025) with link; the strategic sentence for decks ("the correctness architecture is the capture architecture"). *Owner: platform lead. Confluence-native.*

---

## Section 2 — Architecture (git-mirrored)

### 2a. System context and containers

**Idea.** The first technical page anyone reads: who and what surrounds the platform, and what the deployable pieces are.

**Overview.**
> One diagram of the platform in its world (users, COIN, Agent Trinity's external Tech Risk dependency, JIRA, GitHub, model providers), then the container view: the Single Page Application, the Go Control Plane Service, the outbox relay, five agent workers on the Google Agent Development Kit, and the three stores — with the five design principles that govern them, starting with "one write path."

**Details.** Mirrored from `ipdlc-architecture.md` §§2–3 + §11.1 (context diagram, container diagram, runtime sequence, principles). *Owner: architect. Source: repo. Republished on merge.*

### 2b. Deployment — GTD and SWD

**Idea.** What runs where across Georgetown and South West, and what survives losing either.

**Overview.**
> Both data centers are active: stateless services and agent workers run in each; job consumer groups span both; the events topic is deliberately consumed twice so each local Redis is complete; the outbox relay runs single-active with its standby in the other center. Assumptions still marked (confirm) are tracked on the roadmap page.

**Details.** Mirrored from §11.4 + §8 sizing; placement rules written out; failover behavior per component. *Owner: platform engineer. Source: repo.*

### 2c. Security and trust boundaries

**Idea.** The page security review reads first — every boundary data crosses, and the three flows needing explicit sign-off.

**Overview.**
> Data classifications on every cross-boundary flow. Three flows need named sign-off: prompts leaving to model endpoints, the current-state architecture crossing to the Tech Risk team, and GitHub write access for agent-generated code. Identity: COIN authenticates once; the Control Plane Service verifies locally per request; downstream acts under service identity with the human recorded as data.

**Details.** Mirrored from §11.3 + the auth model + CSRF; the agent-output-sanitization chain (UI's half of injection defense) cross-linked to the UI page. *Owner: architect + security partner. Source: repo.*

### 2d. Architecture Decision Records (parent page + children)

**Idea.** The platform practices its own thesis: decisions as first-class records. "Why did we choose X" is a link, not archaeology.

**Overview (parent page).**
> One page per significant decision: context, options considered, decision, consequences, status. The review board approves ADRs, not documents. New ADRs are proposed as pull requests.

**Details.** ADR template (Context / Options considered / Decision / Consequences / Status / Date / Deciders) plus the eight founding records, each ~one screen, content lifted from the existing docs:
- **ADR-001** Transactional outbox over fire-and-forget writes — kills the handoff race
- **ADR-002** Server-Sent Events over WebSockets — direction, not duration, decides
- **ADR-003** PostgreSQL as the sole state authority — guarded transitions, no split-brain
- **ADR-004** Agents as Kafka consumers running the ADK Runner in-process — no uvicorn, no Celery
- **ADR-005** Per-data-center Redis with dual event consumer groups — duplicate the cheap thing
- **ADR-006** Relational context graph in PostgreSQL — no graph database until traversal demands it
- **ADR-007** Skills as versioned capability units over monolithic prompts — eval-gated promotion
- **ADR-008** "Control Plane Service" naming and scope — gateway duties + stateless run interpretation
- **ADR-009** Adapter-mediated federation over direct topic access — external agents park-and-resume through one bridge; neither side subscribes to the other's bus
- **ADR-010** A2A for agents, MCP for tools — external deciders integrate via the gateway; capability servers (e.g. RISE design) attach as allowlisted MCP toolsets with provenance
*Owner: architect. Source: repo (`/adr` folder).*
---

## Section 3 — Platform Engineering (git-mirrored)

### 3a. Kafka — topics, configs, and flows

**Idea.** Everything that touches the message bus, including the worked run an engineer can trace end to end.

**Overview.**
> Two rules govern everything: only the outbox relay produces, and every consumer is idempotent. This page holds the topic inventory with partitioning rationale, producer and consumer configurations line by line, dead-letter handling, the cross-data-center consumer-group design, and one fully worked pipeline run — every message for Native Mobile PIL Autopay, including the redelivery drill.

**Details.** Mirrored from `ipdlc-kafka-flow.md` §§1–9. *Owner: platform engineer. Source: repo.*

### 3b. The outbox relay

**Idea.** The least familiar, most load-bearing component gets its own page so nobody has to rediscover the dual-write problem.

**Overview.**
> An agent must commit to PostgreSQL and announce on Kafka — two systems, no shared transaction, and both orderings fail under a crash. The outbox pattern makes the announcement a row committed with the state it announces; the relay is the deliberately boring process that publishes rows after commit. Single active leader, at-least-once by design, absorbed by idempotent consumers.

**Details.** Mirrored from `ipdlc-kafka-flow.md` §10 (component view, the crash-window sequence, the two numbers to watch, the Debezium alternative). *Owner: platform engineer. Source: repo.*

### 3c. Redis Streams and realtime delivery

**Idea.** How an event consumed by any pod in either data center reaches the exact browser tab watching that run.

**Overview.**
> Redis is the last-mile buffer, never a source of truth. One stream per run; the Server-Sent Events frame identifier is the Redis entry identifier, so browser reconnect resumes exactly; snapshot-first recovery turns every Redis failure into a re-render instead of an incident. Per-data-center Redis with both centers consuming every event keeps failover boring.

**Details.** Mirrored from `ipdlc-redis-streams-flow.md` (lifecycle, write/read paths, multi-DC correctness, reconnect sequence, worked example, sizing). *Owner: platform engineer. Source: repo.*

### 3d. Database schemas

**Idea.** The single reference for every table — shared context, private agent schemas, and the decision layer — with change control through pull requests.

**Overview.**
> PostgreSQL is the platform's only state authority. This page carries the full DDL: runs (plan-driven), shared_events, the outbox, HITL decisions, the agent-schema template (with the idempotency claim constraint), the agent registry, and the context-graph tables — entities, links, memory, feedback, and decision records. Schema changes ship as migrations reviewed like code.

**Details.** Mirrored from `ipdlc-architecture.md` §6 + `ipdlc-context-graph.md` §3 and §9 DDL; the entity-relationship diagrams; the token-cost increment rule; JSONB versioning discipline. *Owner: platform engineer. Source: repo.*

### 3e. Context graph and decision layer

**Idea.** The full engineering specification of the platform's compounding asset — the page model-risk reviewers and engineers share.

**Overview.**
> Part I: the three planes (run context, entity spine, curated memory), the write monopolies, the distiller, hybrid entitlement-filtered retrieval, and stewardship. Part II: decision records as first-class rows, replayable lineage, exceptions and revision loops as the richest precedent, and the autonomy-graduation ladder whose promotion evidence comes from the graph itself.

**Details.** Mirrored from `ipdlc-context-graph.md` in full. The audit sentence highlighted in a panel: every remembered claim traces report → memory → source run → decision → named human in four joins. *Owner: architect. Source: repo.*

---

## Section 4 — Experience & Interfaces (git-mirrored)

### 4a. UI architecture (pdlc-ui)

**Idea.** The frontend's complete design, organized around one principle a reviewer can test any component against.

**Overview.**
> The interface is a projection of server state: one snapshot seeds a reducer, ordered deltas mutate it, and refresh, reconnect, and data-center failover are the same code path. RunProvider is the only component allowed side effects. Approvals are never optimistic; all agent output is sanitized through one shared renderer; the composer draft is the only legitimate client-only state.

**Details.** Mirrored from `ipdlc-ui-architecture.md` (component trees, typed state contracts, the connection state machine, sequences, the error-state matrix, testing including the hostile-markdown fixture, performance budgets, build order). *Owner: frontend lead. Source: repo.*

### 4b. API specification

**Idea.** The contract for every endpoint the platform exposes — design intent here, enforced truth in the repo's OpenAPI.

**Overview.**
> Fifteen endpoints across identity, the run lifecycle, gates, Studio, streaming, health, and operations — with conventions (cookie auth, CSRF on writes, idempotency keys, a stable error model), the Server-Sent Events contract, and the sequences behind the state-changing paths, including the approval race that returns 409 as a normal outcome.

**Details.** Mirrored from `ipdlc-api-specification.md`; banner linking the OpenAPI file and noting CI diffs the two. *Owner: backend lead. Source: repo.*

---

## Section 5 — Operations (Confluence-native)

### 5a. Runbooks (parent + one child per runbook)

**Idea.** At 03:00, the on-call engineer needs steps, not architecture. One page per failure, findable by its symptom.

**Overview (parent).**
> Each runbook: symptom → check → action → verify → escalate, one screen, no theory. Titles are symptoms ("Run stuck in running with stale last_update_time"), because that is what gets searched during an incident.

**Details (children to create).** Stuck run (the lost-job signature and outbox republish with attempt+1) · DLQ arrival triage and replay · Outbox unpublished depth growing (relay/leader/Kafka checks) · PostgreSQL failover posture (what stops: new runs; what continues: streams, in-flight consumption) · Redis restart (why nobody should page: snapshot-first covers it) · Agent consumer group empty (registry, KEDA, rebalance) · SSE mass-reconnect after a network event. *Owner: on-call rotation. Reviewed after every incident.*

### 5b. Dashboards and alerts

**Idea.** The few numbers that matter per system, each linked to its live board, each with its page rationale.

**Overview.**
> Consumer lag per group (scaling input and stall alarm) · outbox unpublished depth (relay health) · publish lag · DLQ arrivals (every one is a failed run — page) · SSE connections per pod · guardrail blocks · token cost per run and agent. If a number is on this page, someone acts on it; if nobody would act, it does not belong here.

**Details.** Table: metric → meaning → threshold → alert → dashboard link; OpenTelemetry note (correlation identifier as the trace attribute end to end). *Owner: platform engineer.*

### 5c. Failure drills

**Idea.** Phase exit criteria stay honest only if drills are scheduled, executed, and dated.

**Overview.**
> The three Phase 1 drills — pod kill mid-run, message replay without duplicate side effects, browser refresh with zero missed events — plus the Phase 3 drill (external agent killed mid-run → visibly degraded step, pipeline completes). Last-run dates and results recorded here; a drill without a date is a claim, not evidence.

**Details.** Per drill: procedure, expected observations, evidence links, history table. *Owner: delivery lead. Cadence: per release for Phase 1 drills.*

---

## Section 6 — Governance & Risk (Confluence-native, restricted)

### 6a. Model-risk narrative

**Idea.** The pre-written answer to the model-risk review, so the review starts from your framing.

**Overview.**
> Every agent output that matters passes a human gate; every gate decision is a record with its inputs and rules-in-force pinned; every remembered claim traces to a named human in four joins; skills are versioned and eval-gated; autonomy is graduated per narrow class, evidence-based, and one-disagreement reversible.

**Details.** The audit chain walked with a real example; skill versioning as policy versioning; juror-vs-human calibration; the graduation guardrails; open questions log for the risk partners. *Owner: platform lead + risk partner.*

### 6b. Memory stewardship

**Idea.** The steward's operating manual for the three verbs that govern institutional memory.

**Overview.**
> Quarantine (out of retrieval now, reversible), supersede (new decision replaces old; the old stays for audit), confirm (promote extracted entity links to human-confirmed). Nothing is physically deleted outside a documented data-governance erasure runbook. Quarantined records unreviewed after 90 days page the steward.

**Details.** Procedures per verb; the cold-start rule (distiller writes quarantine-only until curation quality is proven); entity-taxonomy hygiene (alias merging, the five-per-run creation cap). *Owner: memory steward.*

### 6c. Entitlements and data classification

**Idea.** Who can see what, in one authoritative page — the one that gets audited.

**Overview.**
> Runs, memory, and retrieval are entitlement-scoped in SQL, never filtered after the fact; existence outside your entitlement returns not-found by design. Classifications ride every cross-boundary flow; the three sign-off flows are tracked here with their approval status.

**Details.** Entitlement model mapped to roles and gates; classification table per flow; the outbound-review record for the Tech Risk data flow. *Owner: platform lead + security partner. Restricted.*

### 6d. Autonomy graduation policy

**Idea.** The formal policy for what precedent earns — the page that makes Stage III defensible before anyone asks for it.

**Overview.**
> Graduation is per narrow gate-class, never global. Stage II requires sustained recommendation-versus-verdict agreement over a minimum decision count; Stage III additionally requires model-risk sign-off, a registered kill switch, and sampling — and demotes on a single sampled disagreement. Current class statuses live in the table below.

**Details.** Criteria with numbers; the class-status table (empty at launch — its emptiness is the point); the demotion procedure; links to the evidence queries. *Owner: platform lead + risk partner.*

---

## Section 7 — Demos

### 7a. Architecture explorer

**Idea.** The live topology simulator, packaged so anyone can run a credible demo without the build team present.

**Overview.**
> Animated traces across the real topology — the interface, COIN, the Control Plane Service, Kafka topics, PostgreSQL tables, Redis, and the five agents — every packet keyed by one correlation identifier, with the human gate that waits for a real click. Attachment below; download and open in a browser (it is fully self-contained).

**Details.** The `ipdlc-architecture-explorer.html` attachment; what each workflow demonstrates; the three honest simplifications (relay poll collapsed, one control-plane block for both data centers, theatrical packet timing) so presenters aren't caught flat-footed. *Owner: platform lead.*

### 7b. Demo scripts

**Idea.** What to click, in what order, for which audience — the difference between a demo and a wander.

**Overview.**
> Three scripts: leadership (briefing first, decision-record layer iv opened early, one quarter advanced while making the compounding point), engineering (full pipeline in the explorer, then the redelivery drill and the reject path), risk (the decision record, the audit chain, the graduation guardrails).

**Details.** Per script: duration, click path, the one sentence to land per stop, likely questions with answers. *Owner: whoever presents next — this page improves after every demo.*

---

# Part 3 — Maintenance covenant

Five rules, on the Home page, enforced socially: (1) every page carries the header block, and a page unreviewed for two quarters gets flagged by its owner or archived; (2) git-mirrored pages are never edited in Confluence; (3) new significant decisions become ADRs before they become implementations; (4) runbooks are updated in the incident retro, not later; (5) the Home page status line is the delivery lead's weekly duty. A space with these five rules stays alive; without them, it becomes the attic.
