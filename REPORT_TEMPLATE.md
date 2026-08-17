# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Trần Bình Minh  **Lớp:** AICB-P2T2  **Ngày:** 17/08/2026

---

## 0 · Kết quả `make verify`

<details>
<summary>Output ba lượt chạy (python tools/verify.py — tương đương make verify)</summary>

```
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  LAB 17 · make verify
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  run 1/3 … 133.5s
  run 2/3 … 131.1s
  run 3/3 … 134.1s

  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
  gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 11/11 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
  dashboard rows scanned                      ✓ 5,000,000 → 9,324 (536.3×, cần ≥ 10×)
    số file parquet                           ✓ 5,000 → 14
    kết quả truy vấn không đổi                ✓
  DAG: catchup / max_active_runs              ✓ False / 1

  TỔNG KẾT
  ──────────────────────────────────────────────────────────────────────────
  ✓  1 · gold_training_set idempotent & đúng số hàng
  ✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
  ✓  3 · contract + quarantine + dbt test
  ✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
  ──────────────────────────────────────────────────────────────────────────
  4/4 tiêu chí đạt
```

</details>

Tổng kết: **4 / 4 tiêu chí đạt** — ba bảng Gold ổn định qua 3 lượt và đúng số
hàng so với `expected/`, `dbt test` 11/11 pass, quarantine 312/312, dashboard
giảm 536 lần rows scanned (bài mở rộng A), crash-test đạt (bài mở rộng B).

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Chạy `make pipeline` lần thứ hai, số hàng `gold_training_set` tăng từ 12.480 lên gấp đôi; mỗi lượt chạy lại là bảng phình thêm và cùng một `ticket_id` xuất hiện nhiều lần. Sau thao tác **Clear Task** trên Airflow (phiếu #1041), số hàng nhảy loạn vì nhiều run ghi chồng lên nhau. |
| **Nguyên nhân** | Model `incremental` chỉ khai báo `on_schema_change='fail'`, **thiếu `unique_key`**. Khi không có key, dbt không biết dòng nào là "cùng một dòng" nên generate ra câu `INSERT` thuần: chạy lại cùng một partition ngày là **ghi thêm** row mới, không phải ghi đè. Nguồn CDC có dòng `op = 'u'` (update) — một ticket tạo ngày D1 rồi update ngày D2 lọt qua bộ lọc `run_date` ở hai partition ngày khác nhau, nên cách "xoá partition ngày rồi ghi lại" cũng không chạm được nó. DAG để `catchup=True` và `max_active_runs` không giới hạn chỉ làm lỗi nổ thường xuyên hơn; gốc rễ nằm ở cách model được materialize. |
| **Cách khắc phục** | `dbt/models/gold/gold_training_set.sql`: thêm `unique_key = 'ticket_id'` và `incremental_strategy = 'merge'` — merge THAY THẾ dòng cũ theo key nên phép ghi idempotent. `dags/ai_training_pipeline.py`: `catchup=False`, `max_active_runs=1`. |
| **Bằng chứng** | trước: lượt 2 = 24.960 hàng (12.480 × 2) · sau: 12.480 hàng ở cả 3 lượt, checksum cùng `8dd7c98653` · `gold_training_set: 1 hàng / 1 ticket` ✓ |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | `gold_feature_daily` chỉ có 8.645 hàng thay vì 9.100 (14 ngày × 650 khách). Các cặp `(event_date, customer_id)` bị thiếu tập trung ở 11 ngày đầu; những ngày gần hiện tại không thiếu hàng nào. |
| **P99 độ trễ đo được** | **2,73 ngày** *(bắt buộc)* — đo trên `bronze_events`: P50 = 0,13 ngày, P95 = 1,81 ngày, max = 2,94 ngày, 5,05% bản ghi tới kho muộn hơn 1 ngày so với lúc sự kiện xảy ra. |
| **Lookback đã chọn** | 3 ngày — vì P99 = 2,73 ngày < 3 ngày: lùi 3 ngày bao phủ 99% bản ghi, số outlier còn lại nằm trong sai số thiết kế chấp nhận được. |
| **Nguyên nhân** | Điều kiện lọc incremental `where event_date > (select max(event_date) from {{ this }})` chỉ nhận những `event_date` **lớn hơn ngày lớn nhất đã có trong bảng đích**. Một event xảy ra ngày 08-12 nhưng tới kho ngày 08-15: lúc lượt 08-15 chạy, `max(event_date)` đã là 08-14 → 08-12 không lọt; lượt 08-16, `max` là 08-15 → vẫn không lọt. Mỗi lượt chạy, cánh cửa lọc lại dịch về trước theo chính dữ liệu đã có, nên event đến muộn **không bao giờ** được xử lý — bị loại vĩnh viễn, không phải "xử lý trễ". |
| **Cách khắc phục** | `dbt/models/gold/gold_feature_daily.sql`: (1) lookback 3 ngày: `event_date > (select max(event_date) from {{ this }}) - interval '3 days'`; (2) `unique_key = ['event_date', 'customer_id']` + `incremental_strategy = 'merge'` — window rộng làm cùng một cặp được tính lại ở nhiều lượt, merge **thay thế** lần tính trước thay vì cộng dồn như insert. |
| **Bằng chứng** | trước: 8.645 hàng · sau: 9.100 hàng, ổn định 3 lượt (checksum `3db448685c`) · `gold_training_set` giữ nguyên 12.480 ✓ |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> `max` là đại lượng của **outlier**: một lần mạng lỗi hiếm gặp làm max nhảy
> vọt rồi không bao giờ quay lại — theo max nghĩa là lookback vô hạn tăng theo
> từng sự cố một lần. P99 cho biết 99% dữ liệu thật cần bao lâu (2,73 ngày),
> nên lookback 3 ngày chứa 99% bản ghi với chi phí cố định. Chi phí của mỗi
> ngày lùi thêm là phải quét và tính lại ngày đó **ở mọi lượt chạy sau này**
> (không phải một lần): window 3 ngày nghĩa là mỗi ngày vận hành phải tính lại
> 3 ngày thay vì 1. Chọn max = 2,94 ngày thì window vẫn phải là 3 ngày nên chi
> phí gần như tương đương — nhưng nếu một dịp bất thường đẩy max lên 5 ngày,
> chi phí đó bị trả mãi mãi dù không có dữ liệu nào tới muộn hơn 3 ngày nữa.
> P99 cũng cho lời hứa kiểm chứng được: 1% còn lại có thể thiếu ở lượt chạy,
> và con số đó nằm trong sai số thiết kế đã chấp nhận từ đầu.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | `silver_tickets.priority` có tỷ lệ NULL rất lớn và xuất hiện các giá trị `0`, `5`, `-1` trong khi contract quy định 1..4. Theo ngày: từ 08-10 trở đi, `priority_raw` trong CDC đổi từ dạng số sang dạng chuỗi `urgent` / `high` / `medium` / `low`. |
| **Nguyên nhân** | Team backend **đổi cách biểu diễn** priority từ số sang nhãn chuỗi (schema evolution) giữa chu kỳ vận hành, nhưng macro nhận dữ liệu thì không đổi: `normalize_priority` dùng `try_cast(priority_raw as integer)` — phép cast **số** lên chuỗi chữ trả về NULL, nên toàn bộ nhãn chuỗi hợp lệ bị coi là lỗi. Silver nhận NULL hàng loạt, và nếu quarantine bật thì mọi bản ghi từ 08-10 bị vứt đi dù chúng mang đúng ý nghĩa contract cũ, chỉ khác vỏ bọc biểu diễn. |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | **1)** Số hợp lệ `'1' '2' '3' '4'` — đúng contract ban đầu → giữ nguyên. **2)** Nhãn chuỗi `urgent → 1`, `high → 2`, `medium → 3`, `low → 4` (theo tài liệu API của backend) — cùng ý nghĩa contract, khác biểu diễn → **map về số**, không phải lỗi. **3)** `'P1' 'unknown' '0' '5' '-1' '' NULL` — dữ liệu hỏng thật → macro trả NULL, bản ghi vào `quarantine_tickets`. |
| **Cách khắc phục** | **(a)** `dbt/macros/normalize_priority.sql`: khối `CASE` ba nhóm — so sánh **chuỗi** chứ không so số, nên `'0' '5' '-1'` không lọt nhóm 1 dù chúng đúng là số. Macro dùng chung cho `silver_tickets` và `quarantine_tickets` nên hai model không thể lệch nhau. **(b)** `silver_tickets.sql`: **lọc bản ghi hỏng TRƯỚC, xếp hạng SAU** — chỉ loại bản ghi CDC hỏng, ticket vẫn còn trạng thái hợp lệ từ lần update trước (nếu lọc sau khi chọn bản ghi mới nhất, ticket có bản ghi cuối hỏng sẽ biến mất, tụt từ 12.480 xuống 12.168). **(c)** `quarantine_tickets.sql`: `where {{ normalize_priority('priority_raw') }} is null`. **(d)** `dbt/models/silver/schema.yml`: `contract.enforced: true` + bật test `not_null` và `accepted_values: [1, 2, 3, 4]` cho `priority` — contract ràng buộc **kiểu dữ liệu** (integer), miền giá trị 1..4 là việc của test, cần cả hai. |
| **Bằng chứng** | `quarantine_tickets` = 312 hàng (đúng grain: 1 hàng / 1 bản ghi CDC) · `dbt test` 11/11 pass (bản gốc 9 test, thêm 2) · `silver_tickets` vẫn đủ 12.480 ticket · `priority ∈ 1..4`, không NULL ✓ |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để
pipeline dừng khi gặp bản ghi lỗi?

> **Chặn ở Silver, không chặn ở Bronze.** Bronze là nhật ký trung thực của
> nguồn — nếu Bronze từ chối row lỗi, sau này điều tra sự cố (hỏng từ khi nào,
> hỏng theo kiểu nào, chiếm tỷ lệ bao nhiêu) không còn vật chứng để truy
> ngược. Quarantine giữ bản ghi lỗi kèm `priority_raw` gốc để team sửa và nạp
> lại sau.
>
> **Không để `dbt test` fail dừng cả DAG** vì tỷ lệ: 312 bản ghi lỗi trên hơn
> 63.000 bản ghi CDC (dưới 0,5%). Dừng pipeline vì 0,5% dữ liệu hỏng là chặn
> 99,5% dữ liệu tốt đang chờ được phục vụ — mọi model hạ nguồn (Silver → Gold
> → model AI) đứng yên. Cách đúng: cách ly số ít hỏng, tiếp tục phục vụ phần
> lớn, test là **cảnh báo** ghi vào nhật ký chứ không phải công tắc dừng khẩn
> cấp. Chỉ nên chặn DAG khi lỗi đe doạ dữ liệu đã nhập (sai nguồn, sai schema).

---

## 4 · *(mở rộng)* Bài A — Dashboard chậm (EXTRA.md)

| | |
|---|---|
| **Bài đã làm** | **A** (và B ở mục 5) |
| **Triệu chứng** | Dashboard CSKH mất 38 giây thay vì 2 giây. `python tools/explain.py` đo `rows scanned = 5.000.000` cho một tập thật chỉ có 130.683 hàng — 5.000 file Parquet tí hon (~26 hàng/file), mỗi file vẫn tốn công quét tương đương ~1.000 hàng. |
| **Nguyên nhân** | Hai yếu tố cộng hưởng. **(1) Layout:** `data/gold_events/` là 5.000 file nhỏ, không partition, thứ tự hàng ngẫu nhiên; tên file không mang thông tin nào của hai điều kiện lọc (`customer_name`, ngày), nên engine không thể biết file nào vô ích TRƯỚC khi mở — buộc phải mở cả 5.000 file. **(2) Predicate không sargable:** `where strftime(event_time, '%Y-%m-%d') = '2026-08-09'` bọc cột trong một function call — engine không so được biểu thức đó với tên thư mục partition, cũng không so được với thống kê min/max của row group. Parquet không có index; thứ duy nhất điều khiển được là **file nằm ở đâu** (partition) và **hàng theo thứ tự nào trong file** (row group). |
| **Cách khắc phục** | `tools/compact.py`: `COPY ... PARTITION_BY (event_date)` — 14 thư mục, mỗi thư mục một ngày. Không chọn partition theo `customer_name` (650 giá trị → 650 thư mục, mỗi thư mục vài chục KB, và vì MỌI file đều chứa đủ 14 ngày nên điều kiện ngày không bỏ được file nào). `ORDER BY customer_name, event_time` — hàng cùng khách liền nhau nên min/max row group theo `customer_name` sát nhau. `ROW_GROUP_SIZE 2048` — mặc định 122.880 gói cả ngày (~9.300 hàng) vào một row group, min/max phủ toàn bộ, bộ lọc khách không bỏ được phần nào. `queries/dashboard.sql`: trỏ sang `data/gold_events_v2/*/*.parquet`, viết lại filter thành `event_date = date '2026-08-09'` — cột đứng một mình một vế, DuckDB nhận diện hive partition tự động và loại 13/14 thư mục ngay từ đường dẫn. |
| **Bằng chứng** | trước: `rows scanned 5.000.000` · `files 5.000` · `rows on disk 130.683` · hash `4379e4c5d9f3` — sau: `rows scanned 9.324` (giảm **536×**, yêu cầu ≥ 10×) · `files 14` · `rows on disk 130.683` (không đổi, không mất hàng) · `result hash 4379e4c5d9f3` (**không đổi**) |

---

## 5 · *(mở rộng)* Bài B — Consumer bị giết giữa batch (EXTRA.md)

| | |
|---|---|
| **Bài đã làm** | **B** |
| **Triệu chứng** | `make crash-test` giết consumer ở lô 7 rồi khởi động lại: với code gốc, số hàng cuối ít hơn số hàng chuẩn — bản ghi bị **mất**. |
| **Nguyên nhân** | `consume()` commit offset TRƯỚC khi ghi dữ liệu. `consumer.commit()` ghi offset xuống file đĩa, rồi mới tới `write_batch()`. Nếu tiến trình chết tại `maybe_crash()` giữa hai thao tác đó, offset đã dịch nhưng lô chưa được ghi — lần khởi động lại đọc từ sau lô đó và không bao giờ đọc lại nó: **mất dữ liệu vĩnh viễn** (at-most-once). Việc "mất" không phải do mạng hay đĩa, mà do thứ tự hai thao tác rời rạc không thể atomic với nhau. |
| **Cách khắc phục** | **(a)** Đảo thứ tự thành ghi trước, commit sau (at-least-once): nếu chết tại `maybe_crash()`, offset chưa dịch, restart đọc lại lô đó. **(b)** `write_batch()` dùng `INSERT ... ON CONFLICT (event_id) DO UPDATE` với cột `event_id` khai báo `PRIMARY KEY` trong DDL — lô phát lại ghi đè lên chính hàng cũ thay vì tạo hàng mới (phép ghi idempotent). |
| **Bằng chứng** | A (không sự cố): 20.000 / 20.000 event_id · B (giết lô 7): offset đã commit = 3.000 (6 lô × 500), lô 7 chưa ghi nhận → restart đọc lại từ 3.000 · C (khởi động lại): 20.000 / 20.000 — **không mất, không trùng, C == A** → BÀI MỞ RỘNG B: ĐẠT. `make verify` sau đó vẫn 4/4 tiêu chí. |

`DO UPDATE` so với `DO NOTHING` khi một message được replay với nội dung **đã
đổi**: `DO NOTHING` giữ bản ghi cũ (lần ghi đầu thắng), `DO UPDATE` lấy nội
dung mới nhất (last-write-wins). Với dữ liệu stream mà payload có thể được
hiệu chỉnh (ví dụ latency đo lại chính xác hơn), bản mới nhất là sự thật cần
giữ — nên tôi chọn `DO UPDATE`.

---

## 6 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Cách model được materialize: `unique_key` + `incremental_strategy` có khai báo không, dbt sinh ra `INSERT` hay `MERGE`. Kiểm tra `dbt/target/run/.../<model>.sql` xem câu lệnh thật. |
| 2 | Điều kiện lọc trong khối `is_incremental()`: window có bao phủ độ trễ dữ liệu thật (đo percentile của `_ingested_at - event_time`) không, và grain có cột khoá để merge không. |
| 3 | Macro/transform chuẩn hoá dữ liệu nguồn có xử lý **schema evolution** (cùng ý nghĩa, khác biểu diễn) hay chỉ cast cứng; contract và test có tách riêng kiểu dữ liệu với miền giá trị không. |

