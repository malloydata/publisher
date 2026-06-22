# Contributor guide

How to design and contribute MCP tools and agent skills for the Malloy ecosystem. Read these
in order the first time; after that, the principles and the exceptions are the two you return
to in reviews.

- [Design principles](./design-principles.md). The model of who does what (agents reason,
  skills guide, tools retrieve), the prevention-versus-reference distinction, and the three
  principles that keep tools composable and skills lean. Start here.
- [Tool description template](./tool-description-template.md). The five-section structure every
  tool description follows, with an annotated example.
- [Authoring skills](./authoring-skills.md). Repository conventions: skill structure and
  frontmatter, cross-skill references, manifests, and how skills reach different hosts.
- [Design exceptions](./design-exceptions.md). The living record of deliberate divergences from
  the principles. In a review, anything that diverges and is not listed here is a bug, not a
  decision.

The principles, the exceptions, and the template are a living contract, reviewed as new tools
and skills land. They are the highest-leverage artifact in this repository: they are what keep
a surface built by many contributors, across several hosts, coherent enough to scale.
