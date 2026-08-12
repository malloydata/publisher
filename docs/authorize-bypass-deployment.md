# Deploying with the authorize bypass

Publisher accepts a request header that **skips `#(authorize)` gate evaluation**:

```
x-publisher-bypass-authorize: true
```

It exists so a data-management caller can scan a gated source — an indexer is a machine identity
with no givens, so a gated source returns 403 and is never indexed, which turns an author's
`#(index)` tag into an empty index. [docs/authorize.md § Authorize
bypass](authorize.md#authorize-bypass-for-trusted-data-management-callers) covers the semantics
(only gates are skipped; `where:`, caps, and restricted mode are untouched).

This page is for whoever operates the deployment. There is exactly one thing you must do.

## Strip it at the edge

**Publisher does not authenticate anything and does not bound who may send this header.** The name
and value are documented — here and in every copy of these docs — so treat them as known to
everyone. Whether a gate holds is therefore a property of your edge, not of Publisher.

If untrusted callers can reach Publisher through a proxy that forwards unrecognized request
headers — which many do by default — then an end user can set this header on their own request, it
rides through, and the gate is off. No guessing involved.

So: **clear it on every inbound request at your gateway**, and let it be set only by the internal
caller that needs it, on the internal hop.

### Prefer an allowlist over a blocklist

Clearing this one header is the minimum. It is also one forgotten header away from a hole the next
time Publisher (or anything else behind the same gateway) grows a control of this shape.

The durable posture is to **forward only the headers you recognize** and drop the rest. If your
gateway already does that, this is close to a non-issue for you — but confirm it rather than assume
it, because nothing in Publisher will tell you when it is wrong.

If you can only blocklist, clear **both** spellings. Gateways that accept underscores in header
names treat `x_publisher_bypass_authorize` as a distinct header, and directives generally match
case-insensitively but not across `-`/`_`:

```nginx
# ingress-nginx / nginx with the headers-more module, in the server context
more_clear_input_headers "X-Publisher-Bypass-Authorize" "X_Publisher_Bypass_Authorize";
```

`proxy_set_header ... ""` is **not** a substitute in nginx: directives set inside a `location`
block override inherited ones, so a server-level `proxy_set_header` silently does nothing once any
lower level defines its own. Verify whatever you write actually takes effect — a snippet that
no-ops is worse than not claiming the control, because it reads as protection.

### What this does and does not protect

Publisher has no tenant boundary of its own, so your application's authorization still decides
which packages a caller reaches. What the header removes is the **in-model** gating: role- or
row-level policy *within* data the caller is otherwise entitled to reach. That is the residual
case to reason about if the strip is missing — not cross-tenant access.

## Tell whether a bypass happened

Two signals, emitted together on every skipped gate.

**Counter** — `publisher_authorize_bypass_total`, labelled `entry_point` (`source` | `runnable`).
This is the alertable one. Any nonzero rate from a path that should not be using the bypass is a
finding. Two cautions:

- Alert on the **sum**, not on a ratio. `runnable` fires on every bypassed query; `source` fires
  only when the run target was resolvable from surface syntax before compilation, so an ad-hoc
  query emits `runnable` alone.
- The counter has no org / package / source labels, deliberately — they are unbounded cardinality.
  They are on the log line.

**Log line** — `authorize bypass`, at info, with `entryPoint`, `sourceName`, `modelPath`, and
`packageName`. This is what an investigation reads once the counter moves. `sourceName` is
`"(query)"` when the target could not be resolved.

Neither signal records *who* sent the header — Publisher does not know. If you need caller
attribution, log it at the hop that sets the header, and join on package + model.

## If you do not need it

There is no flag to disable it, and adding one would be a false comfort: a flag lives in the same
process a compromised caller is already talking to, whereas the edge strip is a different trust
domain. If no caller in your deployment needs a bypass, strip the header inbound and nothing will
ever set it.

## Where this is going

The bypass is an interim answer. The shape that keeps the decision with the model author is
identity-bound givens ([docs/authorize.md § Security
model](authorize.md#security-model)) — a reserved system given the caller cannot set, so an author
writes `#(authorize) "$ROLE = 'analyst' or $SYSTEM_CALLER = 'indexer'"` and a source they never
opted in stays gated. This header instead removes gating globally for callers you trust wholesale.
When identity-bound givens land, expect this to narrow or be withdrawn.
