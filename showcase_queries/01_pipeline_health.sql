--raw -> silver --> gold
-- did data make it thorugh the whole system?
select
    (select count(*) from public.raw_lab_json)           as raw_rx_batches,
    (select count(*) from public.raw_lab_csv)            as raw_labtrak_rows,
    (select count(*) from analytics.deduped_lab_samples) as silver_samples,
    (select count(*) from analytics.batch_summary)       as gold_batches,
    (select count(*) from analytics.catalyst_performance) as catalyst_summaries,
    (select count(*) from analytics.temperature_performance) as temperature_summaries;