---
id: incremental-delta-reads-late-rows
tags: build-control, incremental
package: ivl
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# A delta writes a row that lands late, and counts one it cannot reach

A `timestamp` watermark's range ends at the run's own clock. Its START used to
be the previous run's clock too, so a row stamped before that run started but
loaded after it sat below every later range: never written, while the source
kept refreshing by delta and serving an undercount. A delta now starts at the
newest watermark the TABLE holds, so such a row is read however late it lands.

A row stamped below even that point is out of every delta's reach. The run
counts it (`lateRows` on its manifest entry, a `persist-incremental-late-rows`
warning on the package), and a `reseed` recovers it. (`lookback=`, which starts
deltas far enough back to read such rows themselves, is
`incremental-delta-lookback`.)

The timestamps are relative to the clock (`now()` in UTC), since what matters
is where each row sits against the runs' own start times.

## Publisher

- PERSIST_STORAGE_MODE: on

## Data orders_pg.ivl_events

| id:int | amount:num | loaded_at:timestamp |
| ------ | ---------- | ------------------- |
| 1      | 10         | null                |
| 2      | 20         | null                |

## Mutate orders_pg.ivl_events

Stamped three and two hours ago, well before the first run.

```sql
UPDATE ivl_events SET loaded_at = (now() AT TIME ZONE 'UTC') - INTERVAL '3 hours' WHERE id = 1;
UPDATE ivl_events SET loaded_at = (now() AT TIME ZONE 'UTC') - INTERVAL '2 hours' WHERE id = 2;
```

## Model ivl.malloy

```malloy
##! experimental.persistence

source: ev is orders_pg.table('public.ivl_events')

#@ persist name="ivl_rows" refresh="incremental" watermark="loaded_at" merge_key="id"
source: events is ev -> { select: id, amount, loaded_at }
```

## Publish

Build 1 seeds both rows. Its boundary is its own start time.

## Mutate orders_pg.ivl_events

**The late row.** Stamped an hour ago: above the newest stored row (two hours
ago), below build 1's start. It lands only now. Row 9 is stamped tomorrow: a
delta's range ends at its run's clock, so it leaves row 9 for later, where a
rebuild would store it. Its absence below is what shows build 2 was a delta.

```sql
INSERT INTO ivl_events (id, amount, loaded_at) VALUES (3, 300, (now() AT TIME ZONE 'UTC') - INTERVAL '1 hour');
INSERT INTO ivl_events (id, amount, loaded_at) VALUES (9, 900, (now() AT TIME ZONE 'UTC') + INTERVAL '1 day');
```

## Publish

Build 2 is a delta from the newest stored row, so it reads the late row. From
build 1's start time it would have read nothing.

## SQL the delta wrote the late row

```sql
SELECT id, amount FROM ivl_rows ORDER BY id;
```

Expect:

| id | amount |
| -- | ------ |
| 1  | 10     |
| 2  | 20     |
| 3  | 300    |

## Query served equals live

Up to the clock, which is all a delta has been asked to cover.

```malloy
run: events -> { where: loaded_at < now; aggregate: n is count(), total is amount.sum() }
```

Expect:

| n | total |
| - | ----- |
| 3 | 330   |

## Mutate orders_pg.ivl_events

**A row below the newest stored one.** Stamped ninety minutes ago, under the
hour-old row the table now holds, so the next delta starts above it.

```sql
INSERT INTO ivl_events (id, amount, loaded_at) VALUES (4, 400, (now() AT TIME ZONE 'UTC') - INTERVAL '90 minutes');
```

## Publish

Build 3's delta cannot reach row 4, and its late-row check finds it in the
window just below the delta's start.

## Warns ivl

cites: persist-incremental-late-rows

## SQL the stored table still lacks it

```sql
SELECT count(*) AS n FROM ivl_rows;
```

Expect:

| n |
| - |
| 3 |

## Publish (reseed)

Build 4 rebuilds the table in full, which recovers row 4 (and stores row 9,
since a rebuild reads every row).

## SQL a reseed recovers it

```sql
SELECT count(*) AS n, sum(amount) AS total FROM ivl_rows;
```

Expect:

| n | total |
| - | ----- |
| 5 | 1630  |

## Mutate orders_pg.ivl_events

A row stamped after the newest row stored up to the clock. Row 9, stored
tomorrow's, is past the boundary build 4 recorded, so the next delta starts at
row 3 rather than at row 9 and reads this one.

```sql
INSERT INTO ivl_events (id, amount, loaded_at) VALUES (5, 500, (now() AT TIME ZONE 'UTC') - INTERVAL '30 minutes');
```

## Publish

Build 5 is a delta from row 3.

## SQL a row stamped ahead of the clock does not move the start

```sql
SELECT id, amount FROM ivl_rows ORDER BY id;
```

Expect:

| id | amount |
| -- | ------ |
| 1  | 10     |
| 2  | 20     |
| 3  | 300    |
| 4  | 400    |
| 5  | 500    |
| 9  | 900    |
