# گزارش تشخیص و حدود شواهد

این گزارش، تشخیص اولیه و نتیجهٔ تعمیر را از نتیجهٔ آزمایش‌های جدید جدا می‌کند. مرجع تمرین، PDF چهارصفحه‌ای در `docs/assignment3/ASSIGNMENT_ORIGINAL.pdf` است.

| مورد | مشاهدهٔ واقعی | اقدام و شاهد نهایی |
|---|---|---|
| دسترسی | اجرای ابزارها روی Windows؛ kubeconfig اولیه دارای CA/هویت نامرتبط | kubeconfig معتبر از VM گرفته شد؛ دسترسی API و SSH بدون انتشار محتوای محرمانه برقرار شد |
| Git | پوشهٔ Windows فاقد Repository بود | مبنای واردشده پیش از تغییر با Commit `04232cc` ثبت شد |
| Kubernetes | یک کنترل‌پلین و دو Worker؛ kubeadm 1.36.6 و containerd | نصب کلاستر و CNI تکرار نشد؛ وضعیت واقعی دوباره جمع‌آوری شد |
| bank-a-3 | Timeline درخواستی شامل minimum recovery point نبود | Fencing، ساخت نسخهٔ تازه، حفظ Archive/PVC قدیمی و سپس سه Replica سالم |
| bank-meta-3 | Join کامل، ولی PVC بدون ownerReference و Detached | کنترل Backup Manifest و System Identifier؛ تعمیر مالکیت؛ همان PVC وارد Streaming شد |
| Synchronous Commit | ANY 2 / required و فقط یک Standby سالم؛ انتظار SyncRep | کاهش به ۱ انجام نشد؛ Replication کامل بازسازی شد |
| 2PC | ۱۷ Pending در Snapshot اول؛ ۱۶ INITIATED و یک ABORT_DECIDED | Recovery برنامه به صفر Pending/Prepared رسید؛ تصمیم یا موجودی دستی نوشته نشد |
| تراکنش تاریخی ce… | تصمیم زنده ABORT بود، برخلاف لاگ قدیمی PREPARED/NULL | فقط هماهنگ‌کننده تصمیم موجود را تثبیت و پایان داد |
| قفل هماهنگ‌کننده | Session قفل و Session عملیات Metadata یکسان نبودند | تمام عملیات هر تراکنش روی Session قفل‌دار یکسان؛ قطع Session مانع اتصال مجدد و تصمیم متناقض می‌شود |
| Client نامشخص | لغو SyncRep می‌توانست WARNING بدهد و Commit محلی دیده شود | تایمر محدود، تشخیص WARNING و Barrier دوام پیش از ACK/Retry/Status |
| PREPARE دیررس | رقابت با ABORT می‌توانست Prepared دیررس ایجاد کند | قفل مشترک Participant، Tombstone و جدول مستقل Fence |
| Idempotency | بررسی جداگانهٔ هر Shard/نوع عملیات کافی نبود | Registry سراسری و رد Payload متفاوت با همان ID |
| Application/Operator | توزیع واقعی بعضی Replicaها ناقص؛ Recovery و Operator تک‌Replica | چهار Deployment بانکی و Operator و Barman روی دو Worker؛ Leader Election اپراتور بررسی شد |
| HPA | Topology Spread تضمین حذف مناسب در Downscale نیست | بازتوزیع محدود Pod بدون PVC و الزام بررسی محل واقعی در Preflight |
| Backup | completed به‌تنهایی اثبات نبود | Restore مستقل هر سه دیتابیس، کنترل WAL و مقایسهٔ تمام جداول عمومی |
| Restore روی WAL آرام | pg_switch_wal روی Segment بدون رکورد می‌تواند کاری نکند | Restore Point یکتا، Cutoff واقعی و تست رگرسیون PostgreSQL؛ تلاش ناموفق نگهداری شد |
| Worker دائماً گمشده | PVهای local-path روی Node ازبین‌رفته قابل زمان‌بندی نبودند | Retain قبل از آزادسازی Binding، Worker با UID جدید و ساخت Replica از دادهٔ زنده |
| مدیریت ازبین‌رفته | یک etcd محلی و PKI روی کنترل‌پلین | Archive رمز‌شدهٔ خارج VM، Restore واقعی روی جایگزین و حفظ UID منابع و داده‌های بانکی |

شواهد اولیه در `diagnostic-authenticated.json`، `stalled-meta-detail.json` و `audit-before-repair.json` هستند. نتیجهٔ تعمیر در `audit-baseline-healthy.json` و نتایج خرابی در پوشه‌های `bonus1-20261115-04`، `bonus2a-20261115-01` و `bonus2b-20261115-01` قرار دارند. گزارش دانشگاهی و جدول نقش‌ها در `BONUS1_BONUS2_FINAL_REPORT_FA.md` آمده است.

علت دقیق همهٔ ۷۳ پاسخ HTTP 500 تاریخی از روی شمارندهٔ ارسالی قابل استخراج نیست؛ برای تک‌تک آن‌ها Traceback متناظر موجود نیست. همچنین برای ۱۱۸ پاسخ 200 آزمایش قدیمی، Log کامل Request ID/Body ارائه نشده بود، بنابراین اثبات ACK به ACK آن اجرای تاریخی ادعا نمی‌شود. تاریخچهٔ پایدار موجود در Baseline حفظ شده و آزمایش‌های جدید دارای Log کامل و قابل تطبیق با دیتابیس‌اند.

برای منشأ اولیهٔ ناسازگاری Timeline، پیام قطعی PostgreSQL ثبت شده است؛ نسبت‌دادن آن به یک عیب مشخص VMware، دیسک یا یک باگ نسخه‌ای بدون شواهد قبل از خرابی انجام نشده است. نسخهٔ نامعتبر به عنوان منبع Recovery استفاده نشد و برای بررسی بیشتر محفوظ ماند.

در Ubuntu نیز ۶۵ فایل کد/پیکربندی/تست به نسخهٔ آزموده‌شده منتقل و Hash آن‌ها با Windows تطبیق داده شد. نسخهٔ پیش از تغییر روی VM و Windows محفوظ است. تنها اختلاف مستقل، Load Generator قدیمی Ubuntu بود که پس از بررسی با نسخهٔ دارای ثبت پایدار ID/Result جایگزین شد. Git در Ubuntu نصب نبود؛ تاریخچهٔ Commit روی Windows نگهداری می‌شود.
