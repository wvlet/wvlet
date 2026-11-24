# Writing design docs

Source code is authoritative. Use these pages to guide readers to the code and summarize what is hard to infer from any one file.

- Lead with what the area owns. Keep the important boundaries, gates, and exceptions a maintainer could miss. Link to the source and tests for exact behavior, not inventories of functions or flags.
- Keep each fact in one place. Link to the [system map](../ARCHITECTURE.md) for shared boundaries and the relevant [ADR](../adr/README.md) for why. Use short paragraphs or bullets, not a running log of changes.
- A gate ("when changing X, run Y") belongs on the page of the area that owns X. Give it a row in the "Where the details live" table in [`AGENTS.md`](../AGENTS.md) when an agent would not otherwise know to open the page.
- Use Mermaid when it replaces prose. Skip the diagram if a sentence is clearer.
- When a documented contract changes, revise or remove the existing sentence or bullet in the same PR. Add text only for a new contract readers need to know.
