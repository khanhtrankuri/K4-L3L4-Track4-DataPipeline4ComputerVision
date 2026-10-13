# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Trần Long Khánh |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | 2A202602538 |
| GitHub | khanhtrankuri |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | trip-02689 |

## Kết quả cuối

- `dataset_version` của bạn: `0d3cdac9ee8c`  (khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: **732 / 86 / 201** (tổng 1.019 khung).
- `report/verify.txt`: tạo bằng `python3 scripts/check_submission.py --save-verify` (phải có `27/27 checks passed`).
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, và ảnh bài tập sập/chạy lại (log task đỏ thấy trip của bạn).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

Nguồn A có `ts_utc` dạng ISO UTC, `speed_mps` theo m/s, `heading_deg`, `lat/lon`,
và bàn đạp `gas/brake` dạng tỷ lệ. Nguồn B dùng epoch-ms `timestamp_ms`, tốc độ
`spd_kmh` theo km/h, `hdg`, `latitude/longitude`, và `throttle_pct/brake_pct` theo %;
đồng hồ B còn nhanh 1.500 ms. Giữ hai bảng Bronze và mọi giá trị dạng chữ giúp bảo
toàn dữ liệu gốc để kiểm toán, tiếp nhận schema drift và sửa luật Silver rồi xử lý lại,
thay vì mất thông tin hoặc làm hỏng cả batch khi ép kiểu ngay lúc ingest.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

Staging đổi B từ km/h sang m/s, đổi epoch-ms sang timestamp UTC, trừ offset 1.500 ms,
đổi tên cột và bàn đạp % về cùng schema với A. Bằng chứng là mẫu đầu B ở
`2025-10-08 14:30:00.009 UTC`, lệch video chỉ 9 ms; tốc độ cực đại sau chuẩn hóa là
100 km/h. Mỗi drive nhận 20 dòng `duplicate`, 30 `speed_missing`, 3
`speed_out_of_range` (53 dòng/drive, tổng 106); dòng sạch lần lượt là 2.968 và 3.181.
`rejects` giữ nguyên dòng và lý do để truy vết, sửa và nạp lại, không xóa lỗi âm thầm.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

`in_range` là luật đúng/sai cứng: tốc độ ngoài 0..70 m/s làm dữ liệu không an toàn nên
severity `error` phải dừng downstream. `max_gap_ms` phản ánh đoạn cảm biến mất tín hiệu
có thể tồn tại ngoài thực tế, nên `warn` để quan sát mà không chặn toàn batch. Khi nới
luật làm sạch lên 1.000 m/s, test `in_range_silver_telemetry_speed_mps__70__0` bắt đúng
6 dòng: `PASS=5 WARN=1 ERROR=1`. Khôi phục luật cho `PASS=6 WARN=1 ERROR=0`, chứng
minh cổng chất lượng chặn lỗi lọt qua dù bước biến đổi vẫn chạy xong.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

Trước thí nghiệm có 1.246 dòng, fingerprint `6c7480dfde7e`; sau hai lần chạy lại
`trip-00003` vẫn đúng 1.246 dòng và cùng fingerprint. Trip có 60 khung, còn cách ghi
`append` hai lần tạo 120 dòng (2x). Delta `overwrite` với predicate chỉ thay partition
`trip_id` và commit nguyên tử: lỗi trước commit vẫn giữ snapshot cũ, lỗi sau commit có
snapshot mới hoàn chỉnh. Cách “xóa rồi ghi” có khoảng trống mà sự cố có thể làm mất
partition; append thì không phân biệt retry và làm nhân đôi.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

Khung đen gần như không chứa tín hiệu nên bị loại; khung tối vẫn còn cảnh hữu ích nên
chỉ gắn cờ và giữ (A: 6 đen, 4 tối giữ lại; B: 4 đen, 61 tối giữ lại). Tôi đồng ý vì
loại toàn bộ ảnh tối sẽ làm dataset thiếu đường hầm/bóng râm. Tôi chọn ngưỡng dHash 2:
nó loại 149 khung A và 53 khung B, cân bằng hơn ngưỡng 0 quá chặt và ngưỡng 6 loại
363/244 khung. Xe chạy chậm tạo nhiều khung liền kề giống nhau, nên cùng ngưỡng sẽ
đánh dấu nhiều near-duplicate hơn; dữ liệu A còn có đoạn camera đứng hình 6 giây.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

Trong 1.019 khung dùng được thuộc 26 trip, chia ngẫu nhiên theo frame làm cả **26/26**
trip xuất hiện ở nhiều split và cả 26 có mặt đồng thời ở train lẫn test. Chia theo trip
của Gold có **0** trip bị cắt. Các frame kề nhau có cùng cảnh, góc nhìn, ánh sáng và
vật thể gần như giống hệt; đưa chúng sang hai split khiến mô hình nhớ cảnh đã học thay
vì tổng quát hóa. Vì vậy metric test sẽ đẹp giả tạo và không phản ánh chuyến xe mới.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

Version là băm của danh sách `frame_id|split` đã sắp xếp nên cùng nội dung luôn ra
`0d3cdac9ee8c`, không phụ thuộc thời điểm chạy hay số lần retry; chạy pipeline và export
lại vẫn giữ đúng chuỗi này. Ngày giờ hoặc counter có thể đổi dù dữ liệu giống hệt, nên
không chứng minh được reproducibility. MSSV `2A202602538` được dùng làm salt khi xếp
trip vào split (732/86/201), do đó bạn khác có mapping trip khác, nội dung manifest
khác và version khác dù đầu vào video/telemetry giống nhau.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

Manifest ghi đầu vào gồm Bronze `raw_telemetry_a/b` version 0, `trips_raw` version 0,
`video_files` version 1; Silver `frames` version 25, `telemetry`, `frames_flagged`,
`frame_telemetry` đều version 0. Hai video gốc có SHA-256 lần lượt
`636ba1a98dfc...e7e9aa` và `bfa62703f645...ee9fdc`. Để tái tạo sau ba tháng cần các
version Delta này, đủ SHA-256 video, MSSV/salt, luật loại khung, luật split theo trip,
manifest SHA-256 `04de7ca36c6d...373ff2` và mã pipeline/dbt tương ứng.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

Với phần chữ số của MSSV, `2202602538 mod 26 = 6`, nên `silver_frames[6]` ứng với
`trip-02689`. Đúng 1 task chuyển đỏ với log `simulated crash while extracting
trip-02689`; 7 task downstream thành `upstream_failed`. Tôi clear task đỏ để nó chạy
lại đúng 1 lần; sau khi nó xanh, tôi reset 7 trạng thái downstream để chúng chạy lần
đầu (không chạy lại các partition frame đã thành công). DAG cuối xanh, verify vẫn
27/27, có 1.246 frame, không nhân đôi và version vẫn `0d3cdac9ee8c`; ảnh Graph và log
nằm trong `report/screenshots/`.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Ba điểm nghẽn đầu tiên là: (1) một Airflow LocalExecutor với parallelism 2 và scheduler
đơn; thay bằng Airflow/Kubernetes executor có autoscaling và queue. (2) FFmpeg +
DuckDB/dbt chạy trên một node và một file, đặc biệt decode/join 500 luồng video; chuyển
frame extraction sang job phân tán (Kubernetes/Spark/Ray), telemetry ELT sang
Spark/Flink và table format lakehouse. (3) MinIO và Postgres đơn node sẽ nghẽn I/O,
dung lượng và truy vấn dashboard; dùng object storage phân tán/cloud cùng warehouse
MPP có partition, autoscaling và retention. Ingest cũng cần hàng đợi như Kafka để
hấp thụ burst và backpressure thay vì ghi thẳng đồng bộ.

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

```sql
select trip_id, drive_id, frames, frames_kept,
       round(100.0 * (frames - frames_kept) / nullif(frames, 0), 1) as drop_rate_pct
from marts.mart_trip_summary
order by drop_rate_pct desc, trip_id
limit 10;
```

Câu hỏi tìm trip có tỷ lệ khung bị loại cao nhất để ưu tiên điều tra chất lượng. Kết quả
đứng đầu là `trip-04622` của drive A: giữ 26/60, tức loại **56,7%**; tiếp theo là
`trip-03904` với 50,0% và `trip-02086` với 46,7%.

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

Điều bất ngờ nhất là retry một partition vẫn giữ nguyên cả 1.246 dòng lẫn fingerprint,
trong khi append sai lập tức nhân đôi. Tôi đã hiểu rõ hơn ranh giới giữa cảnh tối hữu
ích và khung đen, nhưng vẫn muốn tìm hiểu cách chọn ngưỡng dHash thích nghi theo tốc
độ thay vì cố định 2 bit. Một lỗi hiện chưa được bắt tốt là GPS “nhảy” tới tọa độ vẫn
hợp lệ về miền giá trị nhưng phi vật lý theo vận tốc; nên bổ sung kiểm tra quãng đường,
gia tốc và tính liên tục không gian-thời gian giữa các mẫu.
