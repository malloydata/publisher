---
id: a-pair-table-refresh-replaces-whole-parents
tags: build-control, incremental, materialize
package: pfx
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# An incremental pair table's refresh replaces whole parents, keeping one parent row each

`flights` inlines its join_many to `segments`, so its table is a pair table: one
row per flight and segment, with `__parent_row` marking one row per flight. It
refreshes incrementally because its watermark names a field on each side of the
join, `watermark=["batch", "segments.batch"]`: the table's watermark is, per
flight, the greatest batch of the flight and its segments, so a segment that
arrives for an old flight moves that flight. Each refresh replaces every row of
each flight whose watermark moved, by the flight's key, in the lake.

What it proves, end to end with a Postgres origin and a DuckLake destination:

- **A late-arriving child reaches the table**: flight 1, built in batch 1, gets a
  segment in batch 3.
- **A childless parent gains a child, and a parent loses its last** (restamped,
  the incremental contract), and a new flight arrives with its segment.
- **Each flight is counted once**: `flight_count` aggregates the `__parent_row`
  rows, so a refresh that left a replaced flight's old rows beside its new ones
  would count it twice.
- **The range is open above**: flight 6, whose segment is in the frontier batch
  4, is read by the same refresh. A pair table's range has no upper bound, so a
  parent held above the frontier by one child is read again rather than left
  behind.
- **It is a delta, not a rebuild**: flight 7's segment is deleted with no
  watermark moved, so the refreshed table keeps it until the `reseed`, the
  incremental contract.

`flights_full` names only the flight's own batch, so a segment arriving for an
old flight could never be seen: its refresh is not honoured, with a
`materialize-pair-full-refresh` warning, and it is rebuilt in full instead, still
built and served.

## Publisher

- PERSIST_STORAGE_MODE: on
- PUBLISHER_MATERIALIZE: true

## Data orders_pg.pfx_flights

| id:int | carrier:text | batch:int |
| ------ | ------------ | --------- |
| 1      | AA           | 1         |
| 2      | UA           | 1         |
| 3      | AA           | 2         |
| 4      | UA           | 2         |
| 7      | AA           | 1         |

## Data orders_pg.pfx_segments

Flight 3 has no segment.

| seg_id:int | flight_id:int | miles:num | batch:int |
| ---------- | ------------- | --------- | --------- |
| 10         | 1             | 100       | 1         |
| 11         | 1             | 50        | 1         |
| 20         | 2             | 200       | 1         |
| 40         | 4             | 300       | 2         |
| 70         | 7             | 5         | 1         |

## Model pfx.malloy

```malloy
source: segments is orders_pg.table('public.pfx_segments') extend {
  primary_key: seg_id
}

#(materialize) storage="lake" inline=["segments"] refresh="incremental" watermark=["batch", "segments.batch"]
source: flights is orders_pg.table('public.pfx_flights') extend {
  primary_key: id
  join_many: segments on segments.flight_id = id
  measure:
    flight_count is count()
    seg_ids is count(segments.seg_id)
    seg_total is segments.miles.sum()
}

#(materialize) name="pfx_flights_full" storage="lake" inline=["segments"] refresh="incremental" watermark="batch"
source: flights_full is orders_pg.table('public.pfx_flights') extend {
  primary_key: id
  join_many: segments on segments.flight_id = id
  measure:
    flight_count is count()
    seg_ids is count(segments.seg_id)
    seg_total is segments.miles.sum()
}
```

## Warns pfx

`flights_full`'s watermark names only the flight's own field, so its refresh is
rebuilt in full rather than refused: the source is still built and served.

cites: rebuilt in full on every refresh

## Publish

Build 1 seeds both: a full build, which records the frontier it stored (batch 2).

expect binding: flights -> lake
expect binding: flights_full -> lake

## Query by carrier

Mixed parent and child measures read the pair table.

```malloy
run: flights -> { group_by: carrier; aggregate: flight_count, seg_ids, seg_total; order_by: carrier }
```

servedFrom: storage

Expect:

| carrier | flight_count | seg_ids | seg_total |
| ------- | ------------ | ------- | --------- |
| AA      | 3            | 3       | 155       |
| UA      | 2            | 2       | 500       |

## Mutate orders_pg.pfx_segments

A segment arrives for flight 1, built in batch 1 (a late-arriving child); flight
3 gets its first; flight 2 loses its only one; new flight 5 has one; new flight
6's segment is in batch 4, the frontier; and flight 7's segment is deleted with
no watermark moved, which only a full build sees.

```sql
INSERT INTO pfx_segments VALUES (12, 1, 25, 3), (30, 3, 70, 3), (50, 5, 10, 3), (60, 6, 1000, 4);
DELETE FROM pfx_segments WHERE seg_id IN (20, 70);
```

## Mutate orders_pg.pfx_flights

Flight 2, which lost its segment, is restamped; flights 5 and 6 arrive.

```sql
UPDATE pfx_flights SET batch = 3 WHERE id = 2;
INSERT INTO pfx_flights VALUES (5, 'AA', 3), (6, 'UA', 4);
```

## Publish (forceRefresh)

Build 2. `flights` advances by a delta over every flight whose watermark is 2 or
more, open above: every row of flights 1, 2, 3, 4, 5 and 6 is replaced, by key.
Flight 7's watermark is 1, so its rows are left as they are. `flights_full` is rebuilt in full
(`forceRefresh` defeats skip-if-unchanged, as a scheduled refresh does).

## Query by carrier (again)

AA: flights 1, 3, 5 and 7, each once, with six segments (three of flight 1's,
and flight 7's deleted one, still stored ⇒ a delta ran). UA: flight 2, now
childless, flight 4, and flight 6 from the frontier batch.

servedFrom: storage

Expect:

| carrier | flight_count | seg_ids | seg_total |
| ------- | ------------ | ------- | --------- |
| AA      | 4            | 6       | 260       |
| UA      | 3            | 2       | 1300      |

## Query full rebuild by carrier

The table rebuilt in full has no segment for flight 7.

```malloy
run: flights_full -> { group_by: carrier; aggregate: flight_count, seg_ids, seg_total; order_by: carrier }
```

servedFrom: storage

Expect:

| carrier | flight_count | seg_ids | seg_total |
| ------- | ------------ | ------- | --------- |
| AA      | 4            | 5       | 255       |
| UA      | 3            | 2       | 1300      |

## Publish (reseed)

Build 3 sends `flights` back to a full build.

## Query by carrier (again)

Flight 7's deleted segment is gone, and the table agrees with the full rebuild.

servedFrom: storage

Expect:

| carrier | flight_count | seg_ids | seg_total |
| ------- | ------------ | ------- | --------- |
| AA      | 4            | 5       | 255       |
| UA      | 3            | 2       | 1300      |
