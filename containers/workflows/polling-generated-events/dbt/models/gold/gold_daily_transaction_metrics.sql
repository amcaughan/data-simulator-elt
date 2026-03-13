{{ config(partitioned_by=["event_date"]) }}

select
  event_date,
  channel,
  card_region,
  merchant_category,
  count(*) as transaction_count,
  cast(sum(amount) as double) as gross_amount,
  cast(avg(amount) as double) as average_amount,
  sum(case when is_declined then 1 else 0 end) as declined_transaction_count,
  cast(sum(case when is_declined then amount else 0 end) as double) as declined_amount,
  cast(sum(case when is_declined then 1 else 0 end) as double) / nullif(count(*), 0) as decline_rate
from {{ ref('gold_transactions') }}
group by 1, 2, 3, 4
