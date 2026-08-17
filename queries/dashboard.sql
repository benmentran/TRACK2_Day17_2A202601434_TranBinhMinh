-- Dashboard "Sức khoẻ hội thoại theo khách hàng" của đội CSKH.
-- Người dùng chọn MỘT khách hàng và MỘT ngày, rồi bấm Load.
--
-- Ba tháng trước truy vấn này chạy 2 giây. Bây giờ 38 giây.
-- Không ai sửa dòng nào trong file này.
--
-- Bạn ĐƯỢC PHÉP viết lại truy vấn, miễn là kết quả trả về không đổi
-- (tools/explain.py kiểm tra điều đó bằng hash của kết quả).
--
-- ĐÃ TÁI CẤU TRÚC (Bài mở rộng A):
--   * dataset mới `data/gold_events_v2/` — tools/compact.py partition theo
--     event_date (14 thư mục, mỗi thư mục một ngày), hàng sắp theo
--     (customer_name, event_time). DuckDB nhận diện hive partition tự động
--     nên điều kiện `event_date = ...` loại được 13/14 thư mục NGAY TỪ ĐƯỜNG
--     DẪN, không cần mở file.
--   * điều kiện ngày viết lại thành `event_date = date '...'`: cột đứng một
--     mình một vế, engine so được với tên thư mục partition. Bản cũ bọc
--     event_time trong strftime() — một function call — không so sánh được
--     với bất kỳ thống kê min/max nào, buộc phải mở toàn bộ file.

select
    customer_name,
    count(*)                                        as n_events,
    count(distinct ticket_id)                       as n_tickets,
    round(avg(latency_ms), 1)                       as avg_latency_ms,
    quantile_cont(latency_ms, 0.95)::int            as p95_latency_ms,
    sum(case when is_escalated then 1 else 0 end)   as n_escalated,
    sum(tokens_in + tokens_out)                     as tokens_total
from read_parquet('data/gold_events_v2/*/*.parquet')
where event_date = date '2026-08-09'
  and customer_name = 'ACME'
group by 1