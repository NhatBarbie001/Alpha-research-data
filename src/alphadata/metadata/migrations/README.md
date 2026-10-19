# Migration của DB metadata

Thay đổi schema bảng metadata:

1. Sửa [docs/04-data-model.md](../../../../docs/04-data-model.md) mục 7.
2. Sửa `tables.py` để DB mới được tạo đúng schema mới nhất.
3. Thêm file `NNNN_mo_ta.sql` ở thư mục này (số tiếp theo, 4 chữ số) đưa DB cũ lên cùng schema. Mỗi câu lệnh kết thúc bằng `;`, viết được trên cả Postgres và SQLite nếu có thể; DB SQLite ở máy dev xoá đi tạo lại cũng được.

`MetaStore` tự chạy file chưa áp dụng khi khởi tạo. DB mới bỏ qua các file (vì `tables.py` đã là bản mới nhất) và chỉ ghi lại là đã áp dụng. Không sửa bảng bằng tay trên server.
