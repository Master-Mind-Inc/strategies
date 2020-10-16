SELECT toDateTime(ts,'UTC') as date, price_del
FROM {database}.tbl_{model_name}_logs
WHERE ts BETWEEN {ts_start} AND {ts_finish}
ORDER BY ts