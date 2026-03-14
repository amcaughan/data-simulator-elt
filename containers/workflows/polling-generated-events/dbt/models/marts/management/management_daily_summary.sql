{{ config(
  partitioned_by=["event_date"],
  external_location=marts_table_location("management", "management_daily_summary")
) }}

select
  sum(transaction_count) as transaction_count,
  cast(sum(gross_amount) as double) as gross_amount,
  cast(avg(average_amount) as double) as average_transaction_amount,
  sum(declined_transaction_count) as declined_transaction_count,
  cast(sum(declined_transaction_count) as double) / nullif(sum(transaction_count), 0) as decline_rate,
  sum(coalesce(anomaly_transaction_count, 0)) as anomaly_transaction_count,
  cast(sum(coalesce(anomaly_transaction_count, 0)) as double) / nullif(sum(transaction_count), 0) as anomaly_rate,
  event_date
from {{ ref('model_evaluator_daily_metrics') }}
group by event_date
