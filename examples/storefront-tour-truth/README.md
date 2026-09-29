# storefront-tour-truth

Truth package for the `storefront-tour` eval set in
[`../storefront/evals/storefront-tour`](../storefront/evals/storefront-tour).
`truth.malloy` holds one source per raw table `storefront.malloy` reads, and no
modelling: no measures, no joins, no labels. Every golden's `canonicalQuery`
runs here.

Two rules:

1. **Never serve this on the answerer's Publisher.** It goes on a second
   server the answerer has no route to. This package lives outside
   `examples/storefront/` for the same reason: Publisher serves every `.malloy`
   under a package directory, so a truth package kept inside the model package
   is served as one of that model's own models, and the answerer can query the
   raw tables.
2. **Never import the model under test.** A truth query that reuses a model
   measure lets a bug in the model certify its own golden.

## Why `data/` is a copy

It holds its own copy of the four files rather than a symlink into
`../storefront/data`. A symlink does not survive the way Publisher installs a
package: it copies the directory into `publisher_data/<env>/<pkg>/` and
resolves the link to an ABSOLUTE path in whatever checkout it was copied from,
so the served package either breaks with `ENOENT` or silently depends on one
machine's directory layout. Both were observed.

The copy can drift from `examples/storefront/data` if the example data is
regenerated, and that is safe rather than silent: `verify_goldens.py`
re-derives every golden against this package before every arm and refuses the
run when a value has moved. Regenerate both together and it will tell you if
you forget.

## Serving it

The set's [README](../storefront/evals/storefront-tour/README.md) has the
commands in context. In short, on a server of its own, then re-derive:

```bash
python3 skills/eval-answer/scripts/verify_goldens.py \
    --set examples/storefront/evals/storefront-tour \
    --publisher http://localhost:4881 --environment truth
```

`--environment truth` is not optional: the default is `samples`, and every
case answers 404 without it.
