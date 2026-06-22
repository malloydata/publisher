# MCP tools and skills: design principles

This is the directional framing for how tools, skills, and agents relate in the Malloy
agent ecosystem. It guides the design of individual tools and skills and is the foundation
for the contributor guidelines and for design reviews. Read it alongside the
[design exceptions](./design-exceptions.md), which records deliberate divergences from what
follows.

Companion docs in this guide:

- [Tool description template](./tool-description-template.md): the 5-section structure every
  tool description follows, with an annotated example.
- [Authoring skills](./authoring-skills.md): the repository conventions for writing a skill,
  manifests, cross-skill references, and packaging across hosts.

## Background

As the ecosystem grows, the number of MCP tools and skills grows with it, written by many
contributors and consumed across several hosts (the Publisher server, IDE extensions, and
command-line agents). Without shared patterns, tools drift apart in shape and quality and
skills duplicate or contradict each other. This document sets the patterns so the surface
can scale: a common model of who does what, and a few principles that keep tools composable
and skills lean.

## Scope

This framework focuses on tools that read, for example a context-retrieval tool, a
docs-search tool, or a query tool. These are illustrative categories; which of them ship in the
open-source surface is recorded in the [design exceptions](./design-exceptions.md) (the
retrieval tool is prospective, and docs-search is currently a static-link redirect). Tools that
write or actuate a change are more
straightforward: they perform a specific operation on a known domain object (a publish tool
takes a package and publishes it). For this write-up we assume write tools are out of scope.
Guidance for sub-agents (when to reach for a sub-agent instead of a tool or skill) is a known
gap to fill later.

## Simplifying assumption: tools are designed to be accompanied by skills

Early MCP tools were designed to work standalone: plug a tool into a model with no
accompanying context. That meant tools had to carry their own documentation, workflow
guidance, and interpretation hints in every response. The assumption here is the opposite:
tools are accompanied by skills. That changes tool design meaningfully. Tools can be leaner
and focused on returning domain data, because skills carry the reasoning context.

The assumption may need to relax for environments you do not control. Some hosts surface MCP
tools but do not load skills the same way a skill-aware host does, so always-on guidance
degrades to on-demand or disappears. Packaging for those hosts is covered in
[authoring skills](./authoring-skills.md); the principle still holds where skills are present.

This makes context budget a first-class design constraint. Skills load when needed: a host
reads skill descriptions on initialization, then reads the full skill when it becomes
relevant (or the user invokes it directly). Tool responses add to that budget on every call.
The two compound. A verbose skill plus a verbose response on every call eats the context the
agent needs for reasoning. So both skills and tool responses should stay lean. A response
should return domain objects at the right granularity: enough to reason well, not so much
that it crowds out everything else. When in doubt, return less with a signal that more is
available.

### Prevention content versus reference content

Not all skill content loads the same way. The "if it is only relevant sometimes, it is a
tool call" rule (the under-20% rule) applies to reference content but not to prevention
content.

Prevention content is read before acting and should load proactively, either always-on or
triggered before the agent writes code. Syntax gotchas, common error patterns, and critical
constraints are prevention content. The under-20% rule does not apply, because the goal is
first-pass correctness. Iterative correction is expensive: repeated query calls with
doc-search lookups in between burn context and wall-clock time, so guidance here should
target the specific errors the agent makes often and steer it away up front.

Reference content is looked up when needed and should be on-demand via tool calls. The
under-20% rule applies. Full syntax docs, renderer property lists, dialect-specific functions,
and pattern examples are reference content.

The test: if removing the content would cause an error the agent cannot self-correct, it is
prevention. If removing it would cause suboptimal code that a doc-search tool could fix after
the fact, it is reference.

## Principle 1: agents reason, skills guide, tools retrieve

Agents handle reasoning: inferring intent, forming a plan, deciding what to do with results.
Skills provide domain knowledge that shapes how the agent reasons. Tools retrieve from a
single domain, which preserves the agent's context window and keeps it focused on reasoning.

Tools do not infer intent. That is the agent's job, guided by skills. A tool does what it is
told, it does not interpret why it is being asked. A retrieval tool returns the entities
matching the parameters it was given. A query tool executes the Malloy it receives. The
agent, informed by skills, decides what to ask for and what to do with the results.

Skills encode three kinds of agent behavior: rules, decisions, and options. Rules always
fire: the skill says "do X" and the agent does it. Decisions require evidence: the agent
proposes with data and the user confirms. Options are user preferences: the agent offers a
yes or no. The distinction is who holds the information: the workflow contract (rule), the
data (decision), or the user (option).

Skills guide, they do not prescribe transitions. The agent decides what to do next, not the
skill. A skill provides knowledge for the current phase and declares its output. It does not
dictate which skill loads after it. A higher-level orchestrating skill may propose a sequence
of steps and point each step at a focused skill, but focused skills themselves must not
reference follow-up skills.

## Principle 2: skills operate across domains

A tool is scoped to a single domain. Its response reflects the structure of that domain:
sources and entities for a semantic model, result sets for queries, documents and sections
for docs. Natural-language phrases, conversation context, and workflow state are metadata,
not the organizing principle.

Skills, by contrast, use multiple tools and operate across domains. An analysis skill calls a
retrieval tool (semantic-model domain) and then a query tool (query domain), orchestrating a
workflow that spans both. The skill ties the domains together; the tools do not need to know
about each other.

Because skills chain tools, tools in the same ecosystem should be composable: they should
share a common vocabulary for cross-cutting concepts like source scope, entity references,
and trace IDs. If a retrieval tool returns a source scope (environment, package, model,
source), that scope should pass directly into a query tool with no reformatting. When tool
outputs map cleanly to tool inputs, agents make fewer errors and skills stay simpler. A good
design-review check: can this tool's output be consumed by another tool without the agent
doing translation work?

Tools have explicit contracts: input schema, response shape, error cases, expressed in the
language of their single domain. They emit domain-native observability: confidence scores,
trace IDs, provenance. Skills have explicit scopes: what workflow they cover and what tools
they expect to call.

Tool descriptions are the UX layer for agents. The description is the interface the model
reads to decide when and how to call the tool. A clear description with a mediocre
implementation outperforms a mediocre description with a great implementation, because the
agent will not know how to use the latter well. Write descriptions for models, not humans:
unambiguous about what the tool does, when to call it, when not to call it, and what the
response looks like. All tool descriptions follow the same template (see
[tool description template](./tool-description-template.md)).

On tool boundaries, prefer composability over consolidation. Prefer fewer tools, but do not
force consolidation when tools serve genuinely different purposes (raw schema discovery
versus inferred suggestions, for instance). The test is composability, whether outputs flow
into inputs without translation, not tool count.

## Principle 3: skills assume tools, tools assume skills are present

Skills are designed knowing which tools are available. An analysis skill knows it can call the
retrieval tool and the query tool, and it teaches the agent when and how to call them as part
of a workflow.

Given the simplifying assumption, tools can assume skills are present. Responses stay lean:
domain objects, scores, metadata, with no workflow instructions or interpretation guides.
Content like next-step suggestions and embedded documentation belongs in skills, not in tool
responses. The tool description provides basic parameter and response documentation; skills
provide the expertise.

Skills say which tool to call, not how to call it. The default is that a skill tells the agent
when to use a tool; the how (input schema, response shape, field semantics) lives in the tool
description and metadata, not duplicated in the skill. If the agent needs a workflow to
determine inputs (validating a parameter before a call, say), the skill can describe that
workflow, but it should avoid restating input or output structure that already lives on the
tool.
