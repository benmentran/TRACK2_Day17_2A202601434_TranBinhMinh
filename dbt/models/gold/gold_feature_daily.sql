-- ---------------------------------------------------------------------------
-- gold_feature_daily — đặc trưng theo ngày cho agent định tuyến.
-- Grain: 1 hàng / 1 cặp (event_date, customer_id).
--
-- SỰ CỐ #1043 — KHẮC PHỤC NHIỆM VỤ 2
--
--   Đo trên bronze_events (14 ngày): P99(_ingested_at - event_time) = 2,73
--   ngày, max = 2,94 ngày, ~5% bản ghi tới kho muộn hơn 1 ngày.
--
--   Lỗi cũ: `where event_date > (select max(event_date) from this)` — chỉ
--   nhận event_date MỚI hơn ngày lớn nhất đã có. Một event xảy ra 08-12 tới
--   kho 08-15: lượt 08-15 max(event_date) đã là 08-14 → bị loại; lượt 08-16
--   max là 08-15 → vẫn bị loại. Không bao giờ được xử lý.
--
--   Sửa hai chỗ, phải đi cùng nhau:
--     1. Lookback 3 ngày (theo P99 = 2,73 ngày, không theo max vì max nhạy
--        với outlier; mỗi ngày lùi thêm phải trả phí quét lại ngày đó ở MỌI
--        lượt chạy sau này).
--     2. unique_key 2 cột (event_date, customer_id) + merge — window rộng
--        làm cùng một cặp bị tính lại nhiều lần, merge THAY THẾ thay vì
--        cộng dồn như insert.
-- ---------------------------------------------------------------------------

{{ config(
    materialized          = 'incremental',
    unique_key            = ['event_date', 'customer_id'],
    incremental_strategy  = 'merge',
    on_schema_change      = 'fail'
) }}

select
    event_date,
    customer_id,
    customer_name,
    segment,
    count(*)                                                  as n_events,
    count(distinct ticket_id)                                 as n_tickets,
    sum(case when is_escalated then 1 else 0 end)             as n_escalated,
    round(avg(latency_ms), 2)                                 as avg_latency_ms,
    quantile_cont(latency_ms, 0.95)::int                      as p95_latency_ms,
    sum(tokens_in)                                            as tokens_in,
    sum(tokens_out)                                           as tokens_out
from {{ ref('silver_events') }}

{% if is_incremental() %}
-- Lookback 3 ngày (P99 = 2,73 ngày): xử lý cả những event tới kho muộn,
-- và merge theo (event_date, customer_id) để lần tính sau thay thế lần trước.
where event_date > (select max(event_date) from {{ this }}) - interval '3 days'
{% endif %}

group by 1, 2, 3, 4
