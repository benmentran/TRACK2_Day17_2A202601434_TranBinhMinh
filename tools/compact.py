#!/usr/bin/env python3
"""Tái cấu trúc dataset Parquet của dashboard — BÀI MỞ RỘNG A (EXTRA.md).

Hiện trạng: `data/gold_events/` gồm 5.000 file nhỏ, không partition, thứ tự
hàng ngẫu nhiên. Query dashboard lọc theo (customer_name, event_time) phải mở
cả 5.000 file: rows scanned = 5.000.000, dù dữ liệu thật chỉ 130.683 hàng.

    python tools/compact.py       # ghi dataset mới
    python tools/explain.py       # đo lại và so với baseline

BA QUYẾT ĐỊNH THIẾT KẾ (và lý do):

  1. <cột partition> = event_date
     Dashboard lọc theo HAI cột: customer_name (= 650 giá trị phân biệt)
     và ngày (strftime(event_time, ...)). Partition theo customer_name tạo
     650 thư mục — mỗi thư mục vài chục KB, và vì MỌI file đều chứa đủ 14
     ngày nên điều kiện ngày không còn chỗ nào để bỏ qua file.
     Partition theo event_date chỉ tạo 14 thư mục, mỗi thư mục đúng một
     ngày (~9.300 hàng): engine biết file nào cần mở NGAY TỪ ĐƯỜNG DẪN,
     không cần mở file rồi mới lọc. Điều kiện customer_name được lọc trong
     file — file một ngày chỉ ~9.300 hàng, vừa tầm.

  2. <cột A>, <cột B> = customer_name, event_time
     Thứ tự hàng quyết định thống kê min/max của mỗi row group có ích hay
     vô dụng. Sắp theo customer_name để các hàng cùng một khách hàng nằm
     liền nhau → min/max của customer_name trong mỗi row group sát nhau,
     bộ lọc khách hàng có dữ liệu để bỏ qua row group không chứa khách đó.
     Trong cùng khách, sắp theo event_time cho min/max thời gian sát.

  3. ROW_GROUP_SIZE = 2.048
     Một ngày có ~9.300 hàng. Mặc định 122.880 hàng/row group gói cả ngày
     vào MỘT row group — min/max phủ toàn bộ, bộ lọc customer_name không
     bỏ được phần nào. Row group 2.048 hàng giữ min/max đủ nhỏ để lọc theo
     khách hàng có tác dụng ở mức row group.

Sau khi chạy xong, kiểm tra lại bằng `python tools/explain.py`: `rows scanned`
phải giảm, `files` phải giảm, và `result hash` phải GIỮ NGUYÊN.
"""

from __future__ import annotations

import pathlib
import sys

import duckdb

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from tools.common import DATA  # noqa: E402

SRC = DATA / "gold_events"
DST = DATA / "gold_events_v2"


def main() -> int:
    con = duckdb.connect()
    # 1 luồng → mỗi partition đúng 1 file, output ổn định giữa các lần chạy.
    con.execute("set threads = 1")

    src_glob = str(SRC).replace("\\", "/") + "/*.parquet"
    dst_path = str(DST).replace("\\", "/")

    n_src = len(list(SRC.glob("*.parquet")))
    print(f"  nguồn : {SRC}  ({n_src:,} file)")

    n_before = con.execute(
        f"select count(*) from read_parquet('{src_glob}')"
    ).fetchone()[0]

    con.execute(f"""
        copy (
            select
                event_id, ticket_id, customer_id, customer_name, segment,
                event_type, model, latency_ms, tokens_in, tokens_out,
                is_escalated, event_time, event_date
            from read_parquet('{src_glob}')
            order by customer_name, event_time
        ) to '{dst_path}' (
            format parquet,
            partition_by (event_date),
            overwrite_or_ignore,
            row_group_size 2048
        )
    """)

    n_after = con.execute(
        f"select count(*) from read_parquet('{dst_path}/**/*.parquet')"
    ).fetchone()[0]

    # Không được mất hàng nào khi tái cấu trúc.
    assert n_before == n_after, f"mất hàng: {n_before:,} → {n_after:,}"
    n_files = len(list(DST.glob("*/**/*.parquet")))
    print(f"  đích  : {DST}  ({n_files:,} file · {n_after:,} hàng)")
    print("  xong. Chạy `python tools/explain.py` để đo lại.")
    return 0


if __name__ == "__main__":
    sys.exit(main())