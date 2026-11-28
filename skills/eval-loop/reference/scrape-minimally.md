### Scrape, minimally

Importing an existing corpus IS the scrape step: copy the set from its home
(for example a benchmarks checkout) into `evals/<set>/` and convert to the
ledger shapes. **`skill:eval-import` is that job in full** -- how to classify
what arrived with each question, why nothing imports as a verified value, and
the seal that makes a later edit to a question detectable. Read it whenever the
questions came from outside, which is most of the time. While importing:

- Freeze each case's `split`: `dev` or `holdout`. Diagnose and improve read
  dev cases only; the acceptance check runs both. A set that is all dev cannot defend an
  accept.
- Later, each diagnosed-and-fixed failure becomes a new frozen dev case, so a
  fixed bug cannot silently return.

Scraping from production logs (chat transcripts, retrieval traces) is the
other supported source, and usually the better one: real traffic asks what
people actually ask. Where your logs physically live is a host concern; look
for a host-specific log-fetching skill. `skill:eval-import` takes over once
you have the text, and its `reference/case-format.md` covers what a log pull
needs that a question list does not.

Prefer variety over volume when you sample, from either source. Cases that
differ in grain, source, filter shape, and phrasing are what move a
measurement; a second sample of the same case is nearly free of new
information.
