# iPDLC Architecture Set — Index

| Artifact | Audience | Contents |
|---|---|---|
| `ipdlc-context-graph-briefing.html` | Director / MD | Interactive executive briefing: decision traces, institutional memory, the compounding graph, the autonomy ladder |
| `ipdlc-architecture.md` | Engineering / review board | Master architecture: phase plan, system + sequence + component diagrams, DDL, Kafka topology, GTD/SWD deployment, state machine, trust-boundary DFD |
| `ipdlc-kafka-flow.md` | Engineering | Topics, producer/consumer configs, DLQs, cross-DC, outbox relay deep dive, full worked run |
| `ipdlc-redis-streams-flow.md` | Engineering | SSE fan-out via Redis Streams, resume/reconnect, snapshot-first recovery, multi-DC correctness |
| `ipdlc-ui-architecture.md` | Frontend | Single Page Application design: component trees, state contracts, SSE client state machine, security, testing |
| `ipdlc-api-specification.md` | Engineering | 15 endpoints, contracts, error model, endpoint sequences, component-to-endpoint map |
| `ipdlc-context-graph.md` | Engineering / model risk | Part I: run context, entity spine, memory, distiller, retrieval, governance. Part II: decision records, replayable lineage, exceptions, autonomy graduation |
| `ipdlc-architecture-explorer.html` | Engineering demo | Live topology simulator: animated traces across UI, COIN, control plane, Kafka topics, PostgreSQL tables, Redis, agents |

Naming: the architecture/NFR agent is **Agent Trinity (Architecture)** throughout; the Go service is the **Control Plane Service**; acronyms are expanded on first use. Every Mermaid diagram in the set validates against Mermaid 11.
