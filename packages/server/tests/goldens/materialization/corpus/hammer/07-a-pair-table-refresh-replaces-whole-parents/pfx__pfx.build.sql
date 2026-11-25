-- hammer/07-a-pair-table-refresh-replaces-whole-parents/pfx/pfx.malloy: build plan
-- Addresses are computed with a fixed connection digest (golden-digest:<connection>),
-- so they move exactly when the SQL or the folded layout moves.

-- ==== source flights
-- sourceID: flights@<hammer>/pfx/pfx.malloy
-- connection: orders_pg
-- sourceEntityId: 6d7c8ec876bb43d20d0fe128a9b6496061993c33daf789899eb25136c28ca4b0
-- compiler key: 6d7c8ec876bb43d20d0fe128a9b6496061993c33daf789899eb25136c28ca4b0 (the sourceEntityId)
-- build SQL:
SELECT 
   base."id" as "id",
   base."carrier" as "carrier",
   base."batch" as "batch",
   segments_0."seg_id" as "segments__seg_id",
   segments_0."flight_id" as "segments__flight_id",
   segments_0."miles" as "segments__miles",
   segments_0."batch" as "segments__batch",
   (ROW_NUMBER() OVER(PARTITION BY (base."id")  ORDER BY  base."id" ASC NULLS LAST ))=1 as "__parent_row",
   MAX(CASE WHEN LEN(LIST_FILTER([COALESCE((base."batch"),(segments_0."batch")),COALESCE((segments_0."batch"),(base."batch"))], x -> x is null)) > 0 THEN NULL ELSE GREATEST(COALESCE((base."batch"),(segments_0."batch")),COALESCE((segments_0."batch"),(base."batch"))) END) OVER(PARTITION BY (base."id")  ) as "__pair_watermark"
FROM public.pfx_flights as base
 LEFT JOIN public.pfx_segments AS segments_0
  ON segments_0."flight_id"=base."id"
GROUP BY 1,2,3,4,5,6,7
ORDER BY 1 ASC NULLS LAST

-- ==== source flights_full
-- sourceID: flights_full@<hammer>/pfx/pfx.malloy
-- connection: orders_pg
-- sourceEntityId: e9a0423e9691ec2a22099a295c06be1e716754975b61b5cbd847b1dbfb02da77
-- compiler key: e9a0423e9691ec2a22099a295c06be1e716754975b61b5cbd847b1dbfb02da77 (the sourceEntityId)
-- build SQL:
SELECT 
   base."id" as "id",
   base."carrier" as "carrier",
   base."batch" as "batch",
   segments_0."seg_id" as "segments__seg_id",
   segments_0."flight_id" as "segments__flight_id",
   segments_0."miles" as "segments__miles",
   segments_0."batch" as "segments__batch",
   (ROW_NUMBER() OVER(PARTITION BY (base."id")  ORDER BY  base."id" ASC NULLS LAST ))=1 as "__parent_row"
FROM public.pfx_flights as base
 LEFT JOIN public.pfx_segments AS segments_0
  ON segments_0."flight_id"=base."id"
GROUP BY 1,2,3,4,5,6,7
ORDER BY 1 ASC NULLS LAST

-- generated files

-- ==== <hammer>/pfx/pfx.malloy.materialize.malloy
##! experimental.persistence
import { flights__mat__base is flights } from 'pfx.malloy'
import { flights__mat__pair_key__source is flights } from 'pfx.malloy'
import { flights__mat__pair_leaf_key__segments__source is segments } from 'pfx.malloy'
import { flights_full__mat__base is flights_full } from 'pfx.malloy'
import { flights_full__mat__pair_key__source is flights_full } from 'pfx.malloy'
import { flights_full__mat__pair_leaf_key__segments__source is segments } from 'pfx.malloy'

#@ persist name="flights" storage="lake" refresh="incremental" watermark="__pair_watermark" merge_key="id"
source: flights__mat is flights__mat__base -> {
  group_by:
    id,
    carrier,
    batch,
    segments__seg_id is segments.seg_id,
    segments__flight_id is segments.flight_id,
    segments__miles is segments.miles,
    segments__batch is segments.batch
  calculate:
    __parent_row is row_number() { partition_by: id } = 1
    __pair_watermark is max_window(greatest(coalesce(batch, segments__batch), coalesce(segments__batch, batch))) { partition_by: id }
  order_by: id
}

query: flights__mat__pair_key is flights__mat__pair_key__source -> {
  group_by: id
  aggregate: matches is count()
} -> {
  where: matches > 1 or id is null
  select: id
  limit: 1
}

query: flights__mat__pair_leaf_key__segments is flights__mat__pair_leaf_key__segments__source -> {
  group_by: seg_id
  aggregate: matches is count()
} -> {
  where: matches > 1 or seg_id is null
  select: seg_id
  limit: 1
}

#@ persist name="pfx_flights_full" storage="lake"
source: flights_full__mat is flights_full__mat__base -> {
  group_by:
    id,
    carrier,
    batch,
    segments__seg_id is segments.seg_id,
    segments__flight_id is segments.flight_id,
    segments__miles is segments.miles,
    segments__batch is segments.batch
  calculate:
    __parent_row is row_number() { partition_by: id } = 1
  order_by: id
}

query: flights_full__mat__pair_key is flights_full__mat__pair_key__source -> {
  group_by: id
  aggregate: matches is count()
} -> {
  where: matches > 1 or id is null
  select: id
  limit: 1
}

query: flights_full__mat__pair_leaf_key__segments is flights_full__mat__pair_leaf_key__segments__source -> {
  group_by: seg_id
  aggregate: matches is count()
} -> {
  where: matches > 1 or seg_id is null
  select: seg_id
  limit: 1
}
