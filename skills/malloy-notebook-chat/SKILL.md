---
name: malloy-notebook-chat
description: Steps to follow when the chat is bound to a notebook or saved report. The notebook's cells are the agent's primary context, answer from it, run its queries, and only reach for get_context when the user asks about something outside it.
---
# Notebook/Report Chat Workflow

Steps to follow when the user asks a question:

> **Tool names** are written bare here — `get_context`, `execute_query`, `search_malloy_docs`. The exact prefixed name depends on the host surface; match each against the tools you actually have.

1. Interpret the user's question as being about the bound notebook/report unless they explicitly ask about something else. Pronouns and shorthand ("this", "it", "the notebook", "the report", "the data", "what's here", "summarize", "key insights", "findings", "anything interesting") all refer to the notebook above. Never respond with a clarifying question about what the user means when the referent is clearly this notebook.
2. Start with a brief, natural acknowledgment that references the specific question: one sentence, varied wording.
3. The notebook above IS your context. Its code cells define the queries the user cares about. For any question:
  - For broad requests like "summarize", "what are the key insights", or "tell me about this notebook", run the notebook's queries via `execute_query` and synthesize the findings across them. Do NOT ask the user to be more specific.
  - If the question can be answered by a query already in the notebook, run that cell's query via `execute_query` (exact code, or a minor variation like adding a filter or changing a group_by).
  - If the question asks for an analysis that is clearly NOT in the notebook (new source, different package, different domain), then, and only then, call `get_context` to explore.
  - Do NOT call `get_context` as a default first step. The notebook already tells you what's available.
4. Before writing or modifying a query, read the `malloy-queries` skill for syntax patterns. When you tweak a query (add a `where:` clause, change a `group_by`, etc.), do NOT add `#(filter)` annotations: filters live on the source's model file and are inherited by this notebook automatically. Query-level `where:` filtering inside a cell is fine; declaring new filter UI is a model change, not a chat-time change.
5. Summarize insights from query results. Do not echo raw rows: the user sees them rendered.
