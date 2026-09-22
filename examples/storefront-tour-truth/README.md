# storefront-tour-truth

Truth package for the eval set beside it. `truth.malloy` holds one source per
raw table `storefront.malloy` reads, and no modelling: no measures, no joins,
no labels. Every golden's `canonicalQuery` runs here.

Two rules:

1. **Never serve this on the answerer's Publisher.** It is served by a second
   server the answerer has no route to. Serving both from one config puts gold
   within the answerer's reach.
2. **Never import the model under test.** A truth query that reuses a model
   measure lets a bug in the model certify its own golden.

`data/` is a symlink to `examples/storefront/data/`. That is deliberate: a copy
would go stale the next time the example data is regenerated, and every golden
would quietly start disagreeing with the model's own numbers.

Serve it on its own server and re-derive before a run:

```bash
python3 skills/eval-loop/scripts/serve.py --server-root /tmp/storefront-truth \
    --port 4881 --mcp-port 4882
python3 skills/eval-answer/scripts/verify_goldens.py \
    --set examples/storefront/evals/storefront-tour --publisher http://localhost:4881
```
