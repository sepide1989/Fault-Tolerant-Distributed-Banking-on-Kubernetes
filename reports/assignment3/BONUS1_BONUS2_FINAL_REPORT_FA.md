# گزارش عملی تعمیر و ارزیابی بونس‌های ۱ و ۲

پروژه: Assignment3_FaultTolerant_Banking_BootstrapFix_v3

تاریخ اجرای ثبت‌شده: ۲۰۲۶/۱۱/۱۵
وضعیت این نسخه: تعمیر و هر سه سناریوی بونس I، II-A و II-B عملاً اجرا و با شواهد داده‌ای اعتبارسنجی شده‌اند.

## مرجع و دامنه

صورت اصلی چهارصفحه‌ای از Downloads پیدا و کامل خوانده شد؛ نسخهٔ آن در `docs/assignment3/ASSIGNMENT_ORIGINAL.pdf` نگهداری می‌شود. فایل `kubernetes.pdf` اولیهٔ Repository جزوهٔ دیگری بود. الزامات Correctness، Deliverables و هر دو Bonus مبنای بررسی قرار گرفتند. نصب اولیهٔ Kubernetes، Namespace و Bootstrap حساب‌ها تکرار نشدند.

کار از میزبان Windows با kubectl، Docker و vmrun انجام شد. kubeconfig اولیه به CA و هویت این کلاستر تعلق نداشت. پس از انتقال kubeconfig واقعی از کنترل‌پلین، دسترسی معتبر برقرار شد. Secrets، کلیدهای SSH، kubeconfig و بسته‌های حاوی PKI در مسیر نادیده‌گرفته‌شدهٔ `private/` نگهداری می‌شوند و جزو تحویل عمومی نیستند.

## نتیجهٔ قابل استناد

| موضوع | نتیجهٔ واقعی | شاهد |
|---|---|---|
| بررسی اولیه | FAIL: مجموع درست، ولی ۱۷ تراکنش معلق و Replication ناقص | `audit-before-repair.json` |
| تعمیر داده و Recovery | PASS: دقیقاً ۲۰۰۰ حساب، مجموع ۱۹۹۹۵۰۰، Pending و Prepared صفر | `audit-baseline-healthy.json` |
| Backup و Restore هر سه دیتابیس | PASS: Restore مستقل و مقایسهٔ جداول با منبع و WAL ثبت‌شده | `restore-proof.json` |
| بونس I، قطع واقعی Worker C | PASS با وقفهٔ مجاز در نوشتن و بدون تصمیم‌سازی دستی | `bonus1-20261115-04/result.json` |
| بونس II-A، نابودی دائمی Worker و دیسک | PASS: حذف واقعی کپی Worker C، جایگزینی با هویت جدید، بازسازی Redundancy و حفظ ۹۶ درخواست تأییدشده | `bonus2a-20261115-01/result.json` |
| بونس II-B، نابودی دائمی کنترل‌پلین | PASS: حذف واقعی کپی، Restore همان مدیریت از etcd/PKI، حفظ UIDها و ۴۰۰ درخواست تأییدشده و اجرای Job تازه | `bonus2b-20261115-01/result.json` |
| تست‌های اصلاحات | PASS: ۲۵ تست، بدون Fail/Skip، شامل ۹ تست PostgreSQL واقعی | `local-validation.xml` |

PASSهای این جدول به اجرای واقعی و شواهد مشخص‌شده مربوط‌اند. حذف Worker کپی و پنج PVC دقیقاً مشخص‌شده و حذف جداگانهٔ کنترل‌پلین کپی، هر کدام با تأیید صریح کاربر انجام شدند. دسترس‌پذیری پیوسته ادعا نمی‌شود؛ توقف موقت و پاسخ نامشخص ثبت شده و موفقیت محسوب نشده است.

## علت‌های ریشه‌ای و تعمیر

### Replicaهای PostgreSQL

در bank-a-3 خطای واقعی «Timeline درخواستی شامل minimum recovery point نیست» مشاهده شد؛ این مسیر PGDATA با تاریخچهٔ Primary جدید سازگار نبود. Instance معیوب Fenced شد و با ساخت Instance چهارم از منبع سالم، ظرفیت Replication بازگردانده شد. پیش از بازنشسته‌کردن Instance معیوب، Backup خارجی و Restore واقعی هر سه دیتابیس بررسی شدند. دادهٔ قدیمی به صورت Archive دارای Hash روی Windows و PVC/PV نگهداری شد؛ PVC یا دیسک آن حذف نشد.

در bank-meta-3 عملیات pg_basebackup قبلاً تمام شده بود، اما PVC پس از destroy با keep-pvc فاقد ownerReference مربوط به Cluster بود. بررسی کد همان نسخهٔ CloudNativePG نشان داد PVC خارج از شاخص PVCهای تحت مدیریت اپراتور مانده است؛ بنابراین تکمیل Job به ساخت Pod منجر نمی‌شد. Manifest، UID، System Identifier، Timeline و محتوای Basebackup بررسی و pg_verifybackup اجرا شد. فقط اتصال مالکیت PVC و وضعیت initialization اصلاح شد؛ همان داده با موفقیت وارد Streaming شد. destroy مجدداً اجرا نشد.

### Commit همگام، Timeout و نتیجهٔ نامشخص

با سه Instance روی دو Worker، `ANY 2` به تأیید هر دو Standby نیاز دارد. خرابی یک Worker این شرط را موقتاً ناممکن می‌کند. این توقف، مطابق اولویت حفظ داده پذیرفته شد. کاهش کورکورانه به ANY 1 اعمال نشد: در توزیع ۲+۱ ممکن است Primary و Standby تأییدکننده هر دو روی Worker نابودشده باشند.

یک نکتهٔ مهم با PostgreSQL واقعی آزموده شد: لغو انتظار SyncRep می‌تواند WARNING برگرداند، در حالی که تراکنش قبلاً فقط به‌صورت محلی Commit شده است. Timeout معمول SQL نیز به تنهایی سقف قابل اتکایی برای این انتظار نبود. مسیر جدید، انتظار DB را محدود می‌کند، WARNING لغو SyncRep را موفقیت تلقی نمی‌کند و نتیجه را UNKNOWN/503 گزارش می‌کند. Retry و خواندن نتیجهٔ نهایی، پیش از اعلام موفقیت، دوباره WAL مربوط به همان نتیجه را با تأیید همگام تثبیت می‌کنند.

### Recovery پروتکل 2PC

قفل Advisory هر تراکنش و عملیات Metadata اکنون روی یک Session یکسان اجرا می‌شوند. از دست رفتن Session باعث اتصال مجدد و تصمیم‌گیری با قفل ازدست‌رفته نمی‌شود. تصمیم پایدار برگشت‌پذیر نیست، تراکنش COMPLETED باز نمی‌شود و قبل از ارسال تصمیم به Participantها، دوام WAL تصمیم دوباره بررسی می‌شود.

در Participant نیز PREPARE، COMMIT و ABORT همان شناسه با قفل Session سریال می‌شوند. Tombstone مربوط به ABORT مانع PREPARE دیررس می‌شود. جدول مستقل participant_fences برای Barrier لازم است تا Retry روی ردیف قفل‌شده در Prepared Transaction متوقف نماند. Registry سراسری، استفادهٔ مجدد از یک Request ID با عملیات یا Payload متفاوت را در تمام Shardها رد می‌کند.

در بررسی زنده، تراکنش تاریخی `cea8460a4481459aaa7ae6d5cf99083f` برخلاف آخرین لاگ ارسالی کاربر، تصمیم پایدار ABORT داشت و در ABORT_DECIDED مانده بود. هیچ COMMIT PREPARED/ROLLBACK PREPARED دستی یا ویرایش مستقیم تصمیم/موجودی انجام نشد. هماهنگ‌کننده از مسیر عادی Recovery آن را پایان داد. شمار ABORT پس از تخلیهٔ درخواست‌های از قبل در حال اجرا از ۶۵ به ۸۶ رسید؛ افزایش ۲۱ به معنی وجود ۲۱ Pending در اولین Snapshot نیست، زیرا آن Snapshot دقیقاً ۱۷ Pending داشت.

در یک اجرای میانی، Session قدیمی Gateway پیش از اعمال سیاست جدید، پس از خاموشی VM قفل Metadata را نگه داشته بود. فقط پس از اثبات Pod UID/IP قدیمی، زمان پیش از قطع، حالت idle و قفل همان تراکنش، Backend یتیم بسته شد؛ تصمیمی نوشته نشد. آن اجرای میانی شاهد Recovery کاملاً خودکار محسوب نمی‌شود. idle_session_timeout و TCP liveness برای Sessionهای بعدی اضافه شدند. اجرای موفق نهایی بونس I به این دخالت نیاز نداشت.

### استقرار و ابزار اثبات

Applicationها، دو Replica اپراتور با Leader Election و دو Replica پلاگین Barman بین Workerها توزیع شدند. Preflight محل واقعی Podها، PVCهای مستقل، Node Affinity، نقش‌های PostgreSQL، Streaming و Sync، Backup و شاهد Restore را بررسی می‌کند. Topology Spread به‌تنهایی تضمین نمی‌کند Downscale، Pod مناسب را نگه دارد؛ بررسی و بازتوزیع محدود Podهای Stateless نیز اضافه شد.

Load Generator برای هر Intent و Result، Request ID، Payload، زمان و نتیجه را در JSONL ثبت و Flush می‌کند. Retry همان شناسه و Payload را دارد. 202، 503، Transport Error و HTTP 200 با Body نامعتبر موفقیت محسوب نمی‌شوند.

Audit جدید علاوه بر مجموع، شناسهٔ دقیق حساب‌ها و مالکیت Shard، Journalهای محلی، Metadata، Vote/ACK، Participantها، Preparedها، دوام تاریخچهٔ قبلی و ACKهای Client را بررسی می‌کند. برای تک‌تک حساب‌ها، اثر تمام عملیات جدید از Ledger بازپخش و با موجودی مقایسه می‌شود. دو Snapshot پایدار یکسان لازم است. Replica مشاهده‌نشده باعث FAIL می‌شود.

## اجرای واقعی بونس I

در Run `bonus1-20261115-04` بار چهار Worker به مدت ۱۸۰ ثانیه با Seed برابر ۷ اجرا شد. Worker C در ساعت UTC برابر `08:34:05.574206` واقعاً با VMware خاموش و در `08:35:30.891312` روشن شد. Hypervisor خاموش‌بودن آن را تأیید کرده است. Image آزموده‌شده `banking-app:v3-recovery-20261115-2` بود.

۱۵۰ پاسخ HTTP 200 با Body نهایی معتبر، ۵۴ پاسخ 503 و ۴ خطای Transport ثبت شدند. ۵۸ نتیجهٔ اخیر Success شمرده نشدند. پس از بازگشت Worker، هر ۹ Instance مشاهده شدند و Replication برقرار شد. تمام درخواست‌های تأییدشده و تاریخچهٔ پیش از خرابی حفظ شدند؛ بازپخش Ledger برای تک‌تک حساب‌ها قبول شد. Pending و Prepared صفر و مجموع ۱۹۹۹۵۰۰ بود.

| Cluster | Primary پیش از قطع | Primary پس از Recovery | وضع نهایی |
|---|---|---|---|
| bank-a | bank-a-2 روی C | bank-a-1 روی B | دو Standby در Streaming/Quorum |
| bank-b | bank-b-1 روی C | bank-b-2 روی B | دو Standby در Streaming/Quorum |
| bank-meta | bank-meta-2 روی B | bank-meta-2 روی B | دو Standby در Streaming/Quorum |

در Capture اولیهٔ انتهای بار، خواندن ترتیبی دو Shard با آخرین عملیات هم‌زمان شد و مجموع موقت ۱۹۹۹۵۰۱ دیده شد. همان Capture به دلیل ناپایداری Snapshot و عدم تطبیق بازپخش Ledger **FAIL** شد و حفظ شده است. پس از تخلیهٔ RPCهای باقی‌مانده، دو Snapshot یکسان و Audit کامل `after-stable.json` قبول شد. هیچ موجودی اصلاح نشد. نتیجهٔ نهایی به شاهد پایدار ارجاع می‌دهد و خطای مشاهدهٔ اولیه را پنهان نمی‌کند.

شمار Metadata پیش از این Run برابر ۴۰۰ COMMIT و ۸۸ ABORT و پس از آن ۴۷۳ COMMIT و ۸۹ ABORT بود. تعداد Successهای Client الزاماً با افزایش COMMITهای بین Shardها برابر نیست، زیرا بار شامل انتقال محلی نیز هست و بعضی نتیجه‌های نامشخص ممکن است نهایتاً Commit شوند.

Fencing فیزیکی در این آزمایش، خاموشی تأییدشدهٔ VM بود. خروجی‌های همهٔ Replicaهای قابل دسترس حداکثر یک Primary را نشان دادند. این شاهد، اثبات تحمل هر نوع Network Partition بدون Fencing نیست؛ برای آن سناریوی اضافی نباید ادعای آزمایش‌نشده کرد.

## بونس II و محیط ایزوله

سه Full Clone مستقل با دیسک‌های مستقل در پوشهٔ `Banking_DR_Isolated_20261115-lab1` ساخته شدند. شبکهٔ آن‌ها VMnet1 و Bucket آن‌ها `banking-dr-20261115-lab1` است. ۱۳۹ Object با حجم ۲۰۸۶۳۱۸۸۶ بایت از Backup خارجی کپی شد؛ اختلاف ETag فایل‌های Multipart با مقایسهٔ محتوای واقعی بررسی شد. مسیر منبع حذف یا بازنویسی نشد. Guard هویت سخت‌افزاری جلوی ورود زودهنگام کپی به کلاستر اصلی را می‌گیرد و دسترسی کپی‌ها به endpoint اصلی S3 مسدود شده است.

### نتیجهٔ واقعی II-A

پس از Baseline، Preflight و Restore مستقل هر سه دیتابیس، کاربر حذف فقط پوشهٔ کپی Worker C و پنج PVC مشخص‌شده را تأیید کرد. در Run `bonus2a-20261115-01`، پوشهٔ VM و دیسک آن در `2026-11-15T12:22:27.5891867Z` واقعاً حذف شد. Worker جدید `banking-worker-replacement-1` از OS آماده با Kubernetes identity و PGDATA قرنطینه‌شده وارد شد؛ دادهٔ قدیمی Worker به عنوان منبع Recovery استفاده نشد.

بار اول ۳۶۰ ثانیه، چهار Worker و Seed برابر ۱۷ داشت: ۷۲ پاسخ معتبر موفق، ۱۲۰ پاسخ 503، دو پاسخ 202 و دو Transport Error. پس از Recovery، ۲۴ درخواست دیگر با Seed برابر ۷۱ همگی موفق بودند. Audit ترکیبی، هر ۹۶ درخواست تأییدشده و تمام تاریخچهٔ پیشین را حفظ‌شده نشان می‌دهد؛ مجموع ۱۹۹۹۵۰۰، دقیقاً ۲۰۰۰ حساب، Pending و Prepared صفر و بازپخش Ledger برای تک‌تک حساب‌ها قبول است. Metadata نهایی ۵۱۸ COMMIT و ۹۲ ABORT دارد.

| Cluster | Primary پیش از حذف | Primary پس از Recovery | Standby محلی / Standby روی جایگزین |
|---|---|---|---|
| bank-a | bank-a-2 روی C | bank-a-1 روی B | bank-a-3 روی B / bank-a-2 روی جایگزین |
| bank-b | bank-b-1 روی C | bank-b-2 روی B | bank-b-1 روی B / bank-b-3 روی جایگزین |
| bank-meta | bank-meta-2 روی B | bank-meta-2 روی B | bank-meta-3 روی B / bank-meta-4 روی جایگزین |

پنج PV قدیمی ابتدا Retain شدند و با UID دقیق حفاظت شدند. PVCهای Active مربوط به دیسک گمشده UID تازه دارند؛ Node Affinity و System Identifierها بررسی شدند. دادهٔ local-path خودکار به Node دیگری منتقل نشده است. Replicaها از نسخهٔ زندهٔ باقی‌مانده روی B ساخته شدند. پس از Recovery، Backup تازه و Restore تازهٔ هر سه دیتابیس، شامل تمام جداول عمومی و Registry و Fenceها، نیز قبول شد. شواهد در `final-audit.json`، `final-preflight.json` و `post-recovery-restore-proof.json` همین Run قرار دارند.

این اجرا به اصلاح ابزار Provisioning نیاز داشت: شروع زودهنگام kubelet روی OS بدون config، ترتیب آزادسازی Claimها پیش از Readyشدن جایگزین، و رقابت اپراتور در ساخت Pod میان حذف Pod قدیمی و PVC قدیمی آشکار شدند. ترتیب ابزار اصلاح شد: ابتدا جایگزین Ready، سپس توقف موقت Reconciliation هنگام تغییر bindingها و در پایان فعال‌سازی مجدد آن. Pod بدون PVC و بدون Node که در رقابت ایجاد شده بود با بررسی UID حذف شد؛ هیچ PGDATA متصل به آن وجود نداشت.

در کپی ایزوله، CloudNativePG شمارهٔ ۳ را برای bank-a دوباره انتخاب کرد و Basebackup تازه روی Claim قدیمیِ Detached ساخت. تکمیل Job، pg_verifybackup و System Identifier بررسی و ownerReference همان Claim تعمیر شد. نسخهٔ اصلی دادهٔ بازنشسته‌شده روی VM مبدأ و Archive خارج VM حفظ شد. برای bank-meta که ابتدا سه کپی روی B ساخته شده بود، یک کپی چهارم روی Worker جایگزین ساخته و Standby اضافی B پس از Fencing و Archive سرد، با حفظ PVC/PV بازنشسته شد. هیچ تصمیم 2PC یا موجودی دستی تغییر نکرد. تعیین‌تکلیف تراکنش‌ها از مسیر Recovery برنامه انجام شد.

چهار کلاستر تک‌نمونه‌ایِ Restore آزمایشی قدیمی که PVC آن‌ها به Worker نابودشده وابسته بود، با Claimهای نگهداری‌شده به عنوان Artifact ازدست‌رفته ثبت و Reconciliation آن‌ها متوقف شد. این منابع، Replica بانکی Active یا منبع معتبر Recovery معرفی نشده‌اند. Restoreهای تازه پس از بازیابی روی Storage جدید موفق‌اند. بنابراین PASS این سناریو به سیستم بانکی و Redundancy بازسازی‌شده مربوط است، نه ادعای سالم‌بودن Artifactهای دیسک نابودشده.

### نتیجهٔ واقعی II-B

پس از پایان II-A، Snapshot تازهٔ etcd و تنظیمات kubeadm، PKI و kubelet خارج از VM ذخیره و رمزگشایی، Hash و Restore آزمایشی آن روی Worker بررسی شد. etcdutl نسخهٔ 3.6.8 از همان Image در حال اجرا گرفته شد؛ kubeadm نسخهٔ 1.36.6 بود. پس از تأیید جداگانهٔ کاربر، کنترل‌پلین کپی و دیسک آن در `2026-11-15T12:54:17.3362411Z` واقعاً حذف شدند.

روی VM جایگزین، etcd و PKI و Manifestهای موروثی قبلاً قرنطینه شده و خالی‌بودن مسیرهای فعال دوباره بررسی شد. فقط Archive معتبر خارج VM برای Restore استفاده شد. Snapshot با افزایش Revision به اندازهٔ یک میلیارد و Mark Compacted بازیابی شد؛ hostname، IP اصلی API، CA و گواهی‌های لازم حفظ شدند. kubeadm init یا ساخت Namespace و Cluster بانکی خالی انجام نشد. ثبت اعتبارسنجی مدیریت در `12:57:17 UTC` کامل شد؛ این زمان، زمان پایان اعتبارسنجی است و به‌عنوان اندازه‌گیری دقیق کمترین RTO معرفی نمی‌شود.

هر سه UID مربوط به Clusterهای بانکی، UID مربوط به Namespace، هر ۹ PVC فعال و UID منابع Nodeها حفظ شدند. UUID سخت‌افزاری کنترل‌پلین جدید متفاوت است. Workerها با همان endpoint و اعتماد قبلی دوباره به API متصل شدند و به join مجدد نیاز نداشتند. یک Job کاملاً تازه و بدون PVC از طریق مدیریت بازیابی‌شده ایجاد، روی `banking-worker-replacement-1` زمان‌بندی و با موفقیت اجرا شد. این شواهد در `management-restore.json` و `management-verification.json` همین Run قرار دارند.

بار ۳۰۰ ثانیه‌ای با چهار Worker و Seed برابر ۲۹، از NodePort مربوط به Worker B اجرا شد: ۴۰۰ پاسخ معتبر موفق، ۲۸ پاسخ 503، یک پاسخ 202 و ۵۰ Transport Error. هیچ‌کدام از ۷۹ نتیجهٔ اخیر Success محسوب نشدند. Audit نهایی همهٔ ۴۰۰ درخواست تأییدشده و تاریخچهٔ قبل را حفظ‌شده نشان می‌دهد؛ Pending و Prepared صفر، مجموع ۱۹۹۹۵۰۰ و شناسه‌های دقیق ۲۰۰۰ حساب صحیح‌اند. Metadata از ۵۱۸ COMMIT و ۹۲ ABORT به ۷۱۷ COMMIT و ۹۳ ABORT رسیده است. Primaryهای بانکی همان bank-a-1، bank-b-2 و bank-meta-2 روی B باقی ماندند؛ در این سناریو PostgreSQL روی کنترل‌پلین قرار نداشت.

Backup تازهٔ بانکی و Restore دوبارهٔ هر سه دیتابیس با مقایسهٔ تمام جداول عمومی، شامل تاریخچهٔ پس از این خرابی، قبول شد. Backup تازهٔ مدیریت پس از بازیابی نیز روی Windows ذخیره و Restore آزمایشی آن روی Worker اعتبارسنجی شد. خروجی نهایی در `final-preflight-stable.json` و `post-recovery-restore-proof.json` قرار دارد.

یک خطای ابزار تست نیز بدون پنهان‌کردن نتیجه اصلاح شد: گرفتن insert LSN روی Segment خالی و سپس pg_switch_wal الزاماً WAL تازه‌ای برای Archive نمی‌سازد. تلاش اول به‌درستی FAIL شد و در `post-recovery-restore-proof-attempt1.json` حفظ شده است. ابزار اکنون یک Restore Point واقعی و یکتا در WAL می‌سازد، Segment حاوی آن را می‌بندد و Archive همان Segment را بررسی می‌کند. این کار هیچ جدول بانکی یا تصمیم تراکنشی را تغییر نمی‌دهد. تست PostgreSQL واقعی برای آن اضافه شد و Restore کامل بعدی قبول شد.

## تحلیل دوام و دامنهٔ خرابی

| وضعیت | نسخه‌های باقی‌مانده در چیدمان اولیه | امکان ACK جدید با ANY 2 / required |
|---|---|---|
| هر دو Worker سالم | Primary و هر دو Standby، در دو دامنهٔ خرابی | فقط پس از Flush روی هر سه Instance |
| از دست رفتن Worker C | برای A/B یک نسخه روی B؛ برای Meta دو نسخه روی B | تا بازسازی دو Standby برای Primary جدید، Commit جدید تأیید نمی‌شود |
| از دست رفتن Worker B | برای A/B دو نسخه روی C؛ برای Meta یک نسخه روی C | همان توقف ایمن؛ تصمیم‌های قبلاً تأییدشده روی نسخهٔ باقی‌مانده وجود دارند |
| از دست رفتن فقط کنترل‌پلین | دیسک‌های هر سه Instance روی Workerها باقی‌اند | خود PostgreSQL در صورت دسترسی شبکه می‌تواند ACK بدهد؛ API/DNS ممکن است دسترس‌پذیری Client را مختل کند |

استدلال ایمنی برای هر دو Worker از الزام Flush روی Primary و دو Standby و چیدمان ۲+۱ به دست می‌آید. Failover Quorum و Fencing فیزیکی نیز فعال/ثبت شده‌اند. خرابی واقعی Worker در اجراهای ثبت‌شده روی C اعمال شد؛ جدول B تحلیل معماری است و آزمون فیزیکی دیگری برای B در این نوبت ادعا نمی‌شود. نابودی هم‌زمان هر دو Worker یا Windows Host خارج از دامنهٔ سه سناریوی مستقل اجراشده است؛ RPO صفر برای آن‌ها از Backup غیرهمگام نتیجه گرفته نمی‌شود.

## وضعیت کلاستر اصلی پس از آزمایش‌ها

کپی بازیابی‌شده با داده‌های آزمایش و Backupهای خودش محفوظ و خاموش شد. سه VM اصلی دوباره روشن و هر چهار Deployment بانکی روی Image `banking-app:v3-recovery-20261115-3` مستقر شدند. Primaryهای اصلی برای داشتن Standby محلی و دوردست به bank-a-2 روی C، bank-b-1 روی C و bank-meta-2 روی B تنظیم شدند. Audit اصلی در `original-final-audit.json` تمام ۱۵۰ ACK آزمایش I و تاریخچهٔ اصلی را حفظ‌شده نشان می‌دهد؛ مجموع ۱۹۹۹۵۰۰ و Pending/Prepared صفر است.

| Cluster اصلی | Primary | Standby محلی | Standby دوردست |
|---|---|---|---|
| bank-a | bank-a-2 روی C | bank-a-4 روی C | bank-a-1 روی B |
| bank-b | bank-b-1 روی C | bank-b-3 روی C | bank-b-2 روی B |
| bank-meta | bank-meta-2 روی B | bank-meta-3 روی B | bank-meta-1 روی C |

Preflight و Restore تازهٔ هر سه دیتابیس اصلی در `original-final-preflight.json` و `original-final-restore-proof.json` قبول شدند. Snapshot تازهٔ مدیریت اصلی نیز در خارج VM ذخیره و Restore Probe آن بررسی شد. Gateway اصلی در `http://192.168.43.128:30080` با پاسخ واقعی Health برابر 200 دوباره در سرویس است. Podهای موقت تعمیر، پس از پایان کار بدون حذف PVC یا دایرکتوری میزبان حذف شدند.

دادهٔ تست‌های II-A و II-B متعلق به کپی ایزوله است و به‌صورت دستی با کلاستر اصلی ادغام نشده است. آن ACKها در VMهای جایگزین نگهداری‌شده و Backup مستقل Bucket آزمایش موجودند. برای مشاهدهٔ همان مجموعه‌داده باید محیط ایزوله را طبق راهنما فعال کرد. برابر بودن مجموع دو محیط به معنی یکسان‌بودن تاریخچهٔ آن‌ها نیست.

## بازتولید و تحویل

راهنمای مرحله‌به‌مرحله و فرمان‌های بازتولید در `docs/assignment3/BONUS_RECOVERY_RUNBOOK_FA.md` قرار دارد. ابزارهای مهم شامل `preflight_assignment3.py`، `audit.py`، `load_transactions.py`، `run_bonus1.py`، `recover_lost_worker.py`، `join_replacement_worker.sh`، `backup_control_plane_kubectl.py` و `restore_control_plane.sh` هستند. ابزار حذف VM فقط پوشهٔ کپی ایزوله، UUID دقیق، دیسک‌های داخل همان پوشه و Token مخصوص همان هدف را قبول می‌کند؛ پیش‌فرض Dry Run است.

اصلاح تازه‌تر خواندن وضعیت Local/Participant در Image با پسوند `-3` ساخته و در محیط ایزوله و سپس کلاستر اصلی مستقر شد. بونس‌های II-A و II-B با همین نسخه اجرا شدند؛ Run موفق بونس I با `-2` بوده است. مجموعهٔ نهایی ۲۵ تست، از جمله ۹ تست PostgreSQL واقعی، قبول است. Guard اضافی Recovery، UUID سخت‌افزاری Nodeهای باقی‌مانده را نیز با طرح بررسی می‌کند، زیرا Full Clone می‌تواند UIDهای Kubernetes را حفظ کند و UID به‌تنهایی محیط اصلی و کپی را از هم تشخیص نمی‌دهد.

مخزن Git پیش از تغییر با Commit مبنا ثبت شد. Commitهای اصلاحات و وضعیت نهایی استقرار در فایل وضعیت تحویل ثبت می‌شوند. انتشار GitHub/DockerHub و ویدئوی ارائه هنوز انجام نشده‌اند؛ حساب و مقصد انتشار برای آن‌ها ارائه نشده است.

وضعیت عملی بونس‌ها PASS است؛ وضعیت انتشار/ویدئوی تحویل کامل صورت تمرین PARTIAL باقی می‌ماند. فهرست تغییر فایل‌ها در `FILE_CHANGES.txt`، جدول خام ۵۴ ردیفی نقش‌ها و Streaming قبل/بعد در `REPLICATION_BEFORE_AFTER.csv` و جمع‌بندی ماشینی در `RESULTS_SUMMARY.json` قرار دارند.

## منابع فنی

- [Replication در CloudNativePG](https://cloudnative-pg.io/docs/1.30/replication/)
- [Failover Quorum در CloudNativePG 1.30](https://cloudnative-pg.io/docs/1.30/failover/)
- [Synchronous Replication در PostgreSQL 17](https://www.postgresql.org/docs/17/warm-standby.html)
- [کد PostgreSQL 17: رفتار لغو انتظار SyncRep](https://github.com/postgres/postgres/blob/REL_17_STABLE/src/backend/replication/syncrep.c)
- [توابع Backup، Restore Point و pg_switch_wal در PostgreSQL 17](https://www.postgresql.org/docs/17/functions-admin.html)
- [بازیابی etcd 3.6 و Revision Bump](https://etcd.io/docs/v3.6/op-guide/recovery/)
