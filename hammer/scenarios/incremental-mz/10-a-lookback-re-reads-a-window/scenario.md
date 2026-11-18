---
id: incremental-delta-lookback
tags: build-control, incremental
package: ivk
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# `lookback=` re-reads a window below the stored boundary

A delta of a `timestamp` watermark starts at the newest watermark its table
holds (`incremental-delta-reads-late-rows`), so a row stamped below that point
and loaded later is out of its reach. `lookback="3 hours"` starts each delta
three hours further back, and `merge_key=` matches every row it reads again to
its stored copy rather than writing it twice.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.ivk_events

| id:int | amount:num | loaded_at:timestamp |
| ------ | ---------- | ------------------- |
| 1      | 10         | null                |
| 2      | 20         | null                |

## Mutate orders_pg.ivk_events

```sql
UPDATE ivk_events SET loaded_at = (now() AT TIME ZONE 'UTC') - INTERVAL '3 hours' WHERE id = 1;
UPDATE ivk_events SET loaded_at = (now() AT TIME ZONE 'UTC') - INTERVAL '2 hours' WHERE id = 2;
```

## Model ivk.malloy

```malloy
##! experimental.persistence

source: ev is orders_pg.table('public.ivk_events')

#@ persist name="ivk_rows" refresh="incremental" watermark="loaded_at" merge_key="id" lookback="3 hours"
source: events is ev -> { select: id, amount, loaded_at }
```

## Publish

Build 1 seeds both rows.

## Mutate orders_pg.ivk_events

Row 3 is stamped two and a half hours ago, below the newest stored row (two
hours ago), and lands only now: without `lookback=` no delta would read it.
Row 9 is stamped tomorrow, which a delta leaves for later and a rebuild would
store, so its absence shows build 2 was a delta.

```sql
INSERT INTO ivk_events (id, amount, loaded_at) VALUES (3, 300, (now() AT TIME ZONE 'UTC') - INTERVAL '150 minutes');
INSERT INTO ivk_events (id, amount, loaded_at) VALUES (9, 900, (now() AT TIME ZONE 'UTC') + INTERVAL '1 day');
```

## Publish

Build 2 is a delta from five hours ago: the newest stored row less the
lookback. It reads row 3, and rows 1 and 2 again.

## SQL the lookback wrote it, and each row once

```sql
SELECT id, amount FROM ivk_rows ORDER BY id;
```

Expect:

| id | amount |
| -- | ------ |
| 1  | 10     |
| 2  | 20     |
| 3  | 300    |

## Query served equals live

```malloy
run: events -> { where: loaded_at < now; aggregate: n is count(), total is amount.sum() }
```

Expect:

| n | total |
| - | ----- |
| 3 | 330   |
