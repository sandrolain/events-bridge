---
description: Crea documentazione d'uso in inglese del servizio nella directory /docs.
mode: agent
---

Crea documentazione d'uso, in lingua inglese, del servizio nella directory /docs:

- `/docs/index.md`: Descrizione generale breve del progetto
- `/docs/getting-started.md`: Getting Started e uso del servizio
- `/docs/architecture.md`: Descrizione dell'architettura, sources, runners, targets
- `/docs/connectors/{name}.md` Per ogni connector una pagina apposita dove {name} è l'identificativo/filename del connector. Descrizione del connector e parametri di configurazione (options) per ssource, runner, target (se presenti).
- `/docs/security.md`: Considerazioni di sicurezza