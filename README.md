# Hybrid Big Data Order Processing Pipeline

## 1. فكرة المشروع

هذا المشروع عبارة عن نظام متكامل لمعالجة وتحليل بيانات الطلبات باستخدام Hybrid Big Data Pipeline.

المشروع يجمع بين:

- Python Batch Processing للملفات الصغيرة.
- Apache PySpark للملفات الكبيرة.
- MongoDB لتخزين البيانات.
- ELT Architecture.
- Data Cleaning & Validation.
- Quarantine للسجلات غير القابلة للتصحيح بأمان.
- Upsert وIdempotency لمنع التكرار.
- MongoDB Queries & Indexes.
- Explain Execution Statistics.
- Aggregation Reports.
- Incremental Materialized Views.
- Scheduled Jobs.
- FastAPI.
- Swagger UI.
- Flask Dashboard كواجهة إضافية للمشروع.

---

## 2. معمارية المشروع

```text
CSV File
   |
   v
Automatic File Router
   |
   +-----------------------+
   |                       |
Small File             Large File
<= 200 MB              > 200 MB
   |                       |
Python Batch             PySpark
   |                       |
   +------ MongoDB Raw ----+
              |
              v
      Cleaning & Validation
              |
       +------+------+
       |             |
Valid/Corrected   Quarantine
       |             |
       v             v
orders_validated  quarantine_orders
       |
       +-------------------------------+
       |               |               |
     Queries        Aggregations     Indexes
       |               |               |
       +---------------+---------------+
                       |
                       v
              Materialized Views
                       |
                       v
                 Scheduled Jobs
                       |
                       v
                    FastAPI
                       |
                       v
                 Swagger /docs
```

---

## 3. التقنيات المستخدمة

```text
Python
PyMongo
MongoDB
Apache Spark / PySpark
Flask
FastAPI
Uvicorn
Pytest
```

الإصدارات المستخدمة موجودة في:

```text
requirements.txt
```

---

## 4. متطلبات التشغيل

تم اختبار المشروع باستخدام:

```text
Python 3.11
Java 11
Apache Spark 3.5.9
MongoDB Local Server
Windows 11
```

يجب أن تكون MongoDB تعمل قبل تشغيل المشروع.

---

## 5. إنشاء البيئة الافتراضية

من داخل مجلد المشروع:

```powershell
python -m venv .venv
```

تفعيل البيئة:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv\Scripts\Activate.ps1
```

تثبيت المكتبات:

```powershell
python -m pip install -r requirements.txt
```

---

## 6. إعداد PySpark

في PowerShell:

```powershell
$env:PYSPARK_PYTHON = "$PWD\.venv\Scripts\python.exe"
$env:PYSPARK_DRIVER_PYTHON = "$PWD\.venv\Scripts\python.exe"
```

يجب أن يكون Spark وJava معرفين بشكل صحيح على الجهاز.

---

## 7. إعداد MongoDB

الإعدادات الافتراضية:

```text
MONGO_URI=mongodb://localhost:27017
MONGO_DB_NAME=midterm_data_pipeline
```

يوجد ملف مثال:

```text
.env.example
```

يمكن أيضًا تحديد قاعدة بيانات مختلفة من PowerShell:

```powershell
$env:MONGO_DB_NAME = "bigdata_final_test"
```

وهذا مفيد للاختبارات حتى لا تختلط بيانات الاختبار مع قاعدة البيانات الأصلية.

---

## 8. تهيئة MongoDB

لتجهيز Collections وIndexes الأساسية:

```powershell
python -m src.mongo_setup
```

---

## 9. تشغيل Pipeline الرئيسي

الصيغة العامة:

```powershell
python -m src.main --input .\data\your_file.csv
```

النظام يحدد المحرك تلقائيًا حسب حجم الملف.

---

## 10. Automatic File Router

قيمة الحد الفاصل موجودة في:

```text
config/settings.py
```

القيمة الحالية:

```python
SMALL_FILE_THRESHOLD_MB = 200
```

القرار:

```text
File <= 200 MB
    -> Python Batch

File > 200 MB
    -> PySpark
```

لا يوجد برنامج منفصل لكل محرك؛ نقطة التشغيل الرئيسية تستخدم Router تلقائي.

---

## 11. Python Batch Processing

الملفات الصغيرة تتم قراءتها باستخدام Streaming CSV Processing.

لا يتم تحميل الملف كاملًا إلى الذاكرة.

الإدخال يتم على دفعات:

```text
Batch Size = 1000
```

ويتم تسجيل:

```text
Batch Number
Records Count
Elapsed Time
Throughput
```

---

## 12. PySpark Processing

الملفات الكبيرة تتم معالجتها باستخدام:

```text
SparkSession
Spark DataFrame API
Fixed Schema
Partitions
MongoDB
```

يتم تسجيل:

```text
Input Partitions
Elapsed Time
Throughput
```

---

## 13. ELT Architecture

المشروع يستخدم ELT وليس ETL التقليدي.

كل البيانات تدخل أولًا إلى:

```text
orders_raw
```

ثم يتم تنفيذ قواعد:

```text
Cleaning
Normalization
Validation
Classification
```

ولا يتم حذف السجل السيئ قبل حفظ نسخته الخام.

---

## 14. MongoDB Collections

أهم Collections:

```text
orders_raw
orders_validated
quarantine_orders
daily_sales_summary
city_sales_summary
mv_refresh_state
job_logs
```

---

## 15. Data Quality Classification

كل سجل ينتهي في واحدة من الحالات التالية:

```text
valid
corrected
quarantined
```

ويجب أن يتحقق دائمًا:

```text
Raw Count =
Valid +
Corrected +
Quarantine
```

---

## 16. Data Cleaning Rules

المشروع يحتوي على مجموعة من قواعد التصحيح الآلي، منها:

```text
Arabic digits normalization
Price words normalization
Thousands separator removal
Currency normalization
Phone normalization
Email normalization
Date normalization
Whitespace / text normalization
Quantity string normalization inside items_json
Total amount recalculation
```

مثال:

```text
14-06-2026 18:01:00
```

يتم تحويله إلى:

```text
2026-06-14T18:01:00
```

ومثال:

```json
"qty": "2"
```

يتم تحويله إلى:

```json
"qty": 2
```

---

## 17. Audit Trail

كل سجل تم تصحيحه يحتفظ بتفاصيل التصحيح.

مثال:

```json
{
  "field": "order_date",
  "original_value": "14-06-2026 18:01:00",
  "corrected_value": "2026-06-14T18:01:00",
  "rule_code": "NORMALIZE_DATE_ISO"
}
```

---

## 18. Quarantine

السجل الذي لا يمكن تصحيحه بأمان ينتقل إلى:

```text
quarantine_orders
```

من أمثلة أسباب العزل:

```text
ID_ORDER_MISSING
ID_CUSTOMER_MISSING
DATE_IMPOSSIBLE_INVALID
JSON_ITEMS_CORRUPTED
ITEMS_EMPTY
VALUE_NEGATIVE_AMBIGUOUS
PRICE_UNKNOWN
INVALID_PHONE_TOO_SHORT
EMAIL_MISSING_DOMAIN
UNKNOWN_ORDER_STATUS
UNKNOWN_CURRENCY
MISSING_ITEM_SKU
ERRORS_CONFLICTING_MULTIPLE
```

---

## 19. نتيجة Dataset الاختبار

تم اختبار المشروع على Dataset مكون من:

```text
20000 Records
```

والنتيجة:

```text
Raw:         20000
Valid:       12000
Corrected:    5000
Quarantine:   3000
Validated:   17000
```

Consistency Check:

```text
20000 = 12000 + 5000 + 3000
```

النتيجة:

```text
Consistency check PASSED
```

---

## 20. Upsert & Idempotency

المشروع يستخدم MongoDB Upsert على:

```text
order_id
```

عند تشغيل نفس Dataset مرة ثانية كانت النتيجة:

```text
Inserted:       0
Updated:        0
Unchanged:  17000
```

وهذا يثبت أن إعادة نفس البيانات لا تنتج Duplicate Records.

---

# Phase 2 - Final Project

## 21. MongoDB Queries

يوجد الملف:

```text
src/queries.py
```

ويحتوي على 5 استعلامات عملية:

```text
orders_by_city
orders_by_customer
orders_by_date_range
orders_by_status_and_date
orders_by_payment_status
```

لعرض الاستعلامات المتاحة:

```powershell
python -m src.queries
```

---

## 22. MongoDB Indexes

يوجد الملف:

```text
src/indexes.py
```

Indexes المستخدمة:

```text
idx_city
idx_customer_id
idx_order_date
idx_status_order_date
```

الـIndex الأخير Compound Index:

```text
status + order_date
```

لإنشاء الـIndexes:

```powershell
python -m src.indexes
```

### سبب اختيار كل Index

#### 1. idx_city

يستخدم الحقل:

```text
city
```

السبب: الاستعلام `orders_by_city` يبحث عن الطلبات حسب المدينة، لذلك يساعد هذا الفهرس MongoDB على الوصول مباشرة إلى سجلات المدينة المطلوبة بدل فحص جميع السجلات.

الاستعلام المستفيد:

```text
orders_by_city
```

#### 2. idx_customer_id

يستخدم الحقل:

```text
customer_id
```

السبب: الاستعلام `orders_by_customer` يبحث عن طلبات عميل محدد باستخدام `customer_id`، لذلك يقلل عدد Documents التي يحتاج MongoDB إلى فحصها.

الاستعلام المستفيد:

```text
orders_by_customer
```

#### 3. idx_order_date

يستخدم الحقل:

```text
order_date
```

السبب: الاستعلام `orders_by_date_range` يستخدم نطاقًا زمنيًا على `order_date`، لذلك يسمح الفهرس لـMongoDB بتنفيذ Range Scan بدل تنفيذ Collection Scan كامل.

الاستعلام المستفيد:

```text
orders_by_date_range
```

#### 4. idx_status_order_date

هذا Compound Index مكوّن من:

```text
status
order_date
```

السبب: الاستعلام `orders_by_status_and_date` يبحث حسب `status` ويطبق نطاقًا زمنيًا على `order_date`، لذلك تم وضع الحقلين معًا في Compound Index لخدمة هذا الاستعلام مباشرة.

الاستعلام المستفيد:

```text
orders_by_status_and_date
```

---

## 23. Explain Execution Statistics

يوجد الملف:

```text
src/explain_analysis.py
```

يتم مقارنة 3 Queries قبل وبعد إنشاء الـIndexes باستخدام:

```text
executionStats
```

تشغيل التحليل:

```powershell
python -m src.explain_analysis
```

التقرير يُحفظ في:

```text
reports/explain_results.json
```

### أثر الـIndexes

قبل إنشاء الـIndexes كانت الاستعلامات تعتمد على:

```text
COLLSCAN
```

أي فحص Collection بشكل واسع.

بعد إنشاء الـIndexes أصبحت تعتمد على:

```text
IXSCAN
```

أي استخدام الفهرس للوصول إلى عدد أقل بكثير من السجلات.

مثال من آخر اختبار فعلي:

```text
orders_by_city

BEFORE
Docs Examined: 17000
Plan: COLLSCAN

AFTER
Docs Examined: 649
Plan: IXSCAN
```

```text
orders_by_date_range

BEFORE
Docs Examined: 17000
Plan: COLLSCAN

AFTER
Docs Examined: 61
Plan: IXSCAN
```

```text
orders_by_status_and_date

BEFORE
Docs Examined: 17000
Plan: COLLSCAN

AFTER
Docs Examined: 11
Plan: IXSCAN
```

هذا يوضح أن اختيار الفهارس مرتبط مباشرة بالحقول المستخدمة داخل الاستعلامات، وأن عدد Documents التي يفحصها MongoDB انخفض بشكل كبير بعد إنشاء الفهارس.

---

## 24. Aggregation Reports

يوجد الملف:

```text
src/aggregations.py
```

ويحتوي على 5 تقارير مستقلة:

```text
sales_by_city
top_customers
daily_sales
orders_by_status
payment_status_summary
```

لعرض التقارير المتاحة:

```powershell
python -m src.aggregations
```

التقارير تعتمد على بيانات MongoDB الفعلية ولا تحتوي على نتائج Hardcoded.

---

## 25. Materialized Views

يوجد الملف:

```text
src/materialized_views.py
```

يوجد Materialized Views اثنان:

```text
daily_sales_summary
city_sales_summary
```

تشغيل Refresh:

```powershell
python -m src.materialized_views
```

أول تشغيل على 17000 سجل:

```text
daily_sales_summary
Status: refreshed
Processed documents: 17000
Affected keys: 181

city_sales_summary
Status: refreshed
Processed documents: 17000
Affected keys: 7
```

عند التشغيل مرة ثانية بدون بيانات جديدة:

```text
daily_sales_summary
Status: up_to_date
Processed documents: 0
Affected keys: 0

city_sales_summary
Status: up_to_date
Processed documents: 0
Affected keys: 0
```

وهذا يثبت أن عملية التحديث Incremental ولا تقوم بعمل Full Rebuild في كل مرة.

---

## 26. Incremental Materialized View Strategy

يتم الاحتفاظ بحالة آخر Refresh داخل:

```text
mv_refresh_state
```

ويتم حفظ:

```text
last_processed_id
last_refresh_at
processed_documents
```

عند وجود بيانات جديدة يتم اكتشاف الـDates أو Cities المتأثرة فقط، ثم يتم تحديث Keys المتأثرة فقط.

---

## 27. Scheduled Jobs

يوجد الملف:

```text
src/jobs.py
```

يوجد Jobان:

```text
refresh_materialized_views
ensure_indexes
```

الجدول الحالي:

```text
refresh_materialized_views
Daily at 01:00

ensure_indexes
Daily at 01:30
```

تشغيل Job يدويًا:

```powershell
python -m src.jobs refresh_materialized_views
```

أو:

```powershell
python -m src.jobs ensure_indexes
```

تشغيل Scheduler:

```powershell
python -m src.jobs scheduler
```

يجب إبقاء نافذة Scheduler مفتوحة أثناء التشغيل المجدول.

---

## 28. Job Logging

كل تشغيل Job يتم تسجيله في:

```text
job_logs
```

ويحتوي على:

```text
job_name
started_at
ended_at
status
message
```

تم اختبار المهمتين يدويًا وكانت النتيجة:

```text
refresh_materialized_views -> success
ensure_indexes -> success
```

---

# FastAPI

## 29. تشغيل FastAPI

لتشغيل API:

```powershell
python -m uvicorn api.main:app --host 127.0.0.1 --port 8000
```

الرابط:

```text
http://127.0.0.1:8000
```

Swagger UI:

```text
http://127.0.0.1:8000/docs
```

OpenAPI JSON:

```text
http://127.0.0.1:8000/openapi.json
```

---

## 30. FastAPI Endpoints

| Method | Endpoint | الوظيفة |
|---|---|---|
| GET | `/health` | التحقق من حالة API |
| POST | `/ingest` | تشغيل Phase 1 Pipeline |
| POST | `/indexes` | إنشاء MongoDB Indexes |
| GET | `/queries` | عرض Queries |
| GET | `/queries/{name}` | تشغيل Query |
| GET | `/aggregations` | عرض Aggregation Reports |
| GET | `/aggregations/{name}` | تشغيل Aggregation |
| POST | `/refresh-mv` | تحديث Materialized Views |
| GET | `/materialized-views` | عرض Materialized Views |
| GET | `/jobs` | عرض Jobs وLogs |
| POST | `/jobs/{name}/run` | تشغيل Job يدويًا |

---

## 31. Health Endpoint

مثال:

```text
GET /health
```

Response:

```json
{
  "status": "ok",
  "database": "midterm_data_pipeline"
}
```

---

## 32. Ingest Endpoint

`POST /ingest` لا يحتوي على Pipeline منفصل.

هو يعيد استخدام نفس Phase 1 Pipeline:

```text
Automatic Router
Python Batch
PySpark
Raw Load
Quality Rules
Validation
Upsert
Quarantine
Metrics
```

مثال Request:

```json
{
  "input_path": "data/sample.csv"
}
```

النظام يقرر المحرك تلقائيًا حسب حجم الملف.

---

## 33. Ingest Idempotency Through API

تم تشغيل نفس Dataset عبر:

```text
POST /ingest
```

والنتيجة:

```text
Raw Count:    20000
Valid:        12000
Corrected:     5000
Quarantine:    3000

Inserted:         0
Updated:          0
Unchanged:    17000
```

وهذا يثبت أن FastAPI يعيد استخدام نفس Upsert وIdempotency Logic الخاص بـPhase 1.

---

## 34. Query API Example

مثال:

```text
GET /queries/orders_by_city?city=عدن&limit=5
```

Response يحتوي على:

```text
name: orders_by_city
count: 5
results: [...]
```

---

## 35. Aggregation API Example

مثال:

```text
GET /aggregations/sales_by_city?limit=5
```

يرجع أفضل المدن حسب:

```text
order_count
total_sales
```

---

## 36. Materialized View API

تشغيل:

```text
POST /refresh-mv
```

إذا لم توجد بيانات جديدة:

```json
{
  "status": "success",
  "views": [
    {
      "view": "daily_sales_summary",
      "status": "up_to_date",
      "processed_documents": 0,
      "affected_keys": 0
    },
    {
      "view": "city_sales_summary",
      "status": "up_to_date",
      "processed_documents": 0,
      "affected_keys": 0
    }
  ]
}
```

---

## 37. Jobs API

عرض Jobs:

```text
GET /jobs
```

تشغيل Job يدويًا:

```text
POST /jobs/ensure_indexes/run
```

أو:

```text
POST /jobs/refresh_materialized_views/run
```

---

# Dashboard

## 38. Flask Web Dashboard

يوجد Dashboard إضافي مبني باستخدام Flask.

تشغيله:

```powershell
.\run_dashboard.bat
```

ثم فتح:

```text
http://127.0.0.1:5000
```

الـDashboard يستخدم لعرض نتائج المشروع واختبار ملفات CSV أثناء العرض.

FastAPI مستقل عنه وموجود لتحقيق متطلبات الـFinal API.

---

# Testing

## 39. فحص Syntax

يمكن فحص ملفات Phase 2 باستخدام:

```powershell
python -m py_compile .\src\indexes.py
python -m py_compile .\src\queries.py
python -m py_compile .\src\explain_analysis.py
python -m py_compile .\src\aggregations.py
python -m py_compile .\src\materialized_views.py
python -m py_compile .\src\jobs.py
python -m py_compile .\api\main.py
```

---

## 40. Pytest

لتشغيل الاختبارات:

```powershell
python -m pytest
```

آخر نتيجة اختبار:

```text
16 passed
```

---

# Reports

## 41. ملفات النتائج

نتائج Pipeline:

```text
reports/results.json
```

نتائج Explain:

```text
reports/explain_results.json
```

---

# Project Structure

## 42. بنية المشروع

```text
midterm-data-pipeline/
|
|-- README.md
|-- requirements.txt
|-- .env.example
|-- run_dashboard.bat
|
|-- api/
|   `-- main.py
|
|-- config/
|   `-- settings.py
|
|-- data/
|   `-- .gitkeep
|
|-- src/
|   |-- main.py
|   |-- file_router.py
|   |-- batch_loader.py
|   |-- spark_loader.py
|   |-- spark_quality_pipeline_stringdates.py
|   |-- quality_rules.py
|   |-- elt_pipeline.py
|   |-- mongo_setup.py
|   |-- metrics.py
|   |-- indexes.py
|   |-- queries.py
|   |-- explain_analysis.py
|   |-- aggregations.py
|   |-- materialized_views.py
|   `-- jobs.py
|
|-- reports/
|   |-- results.json
|   `-- explain_results.json
|
|-- tests/
|
`-- web/
    |-- app.py
    `-- templates/
        `-- presentation.html
```

---

# Quick Start

## 43. تشغيل سريع للمشروع

تفعيل البيئة:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv\Scripts\Activate.ps1
```

إعداد PySpark:

```powershell
$env:PYSPARK_PYTHON = "$PWD\.venv\Scripts\python.exe"
$env:PYSPARK_DRIVER_PYTHON = "$PWD\.venv\Scripts\python.exe"
```

تشغيل MongoDB Setup:

```powershell
python -m src.mongo_setup
```

تشغيل Pipeline:

```powershell
python -m src.main --input .\data\your_file.csv
```

إنشاء Indexes:

```powershell
python -m src.indexes
```

تشغيل Explain Comparison:

```powershell
python -m src.explain_analysis
```

تشغيل Materialized Views:

```powershell
python -m src.materialized_views
```

تشغيل Job:

```powershell
python -m src.jobs refresh_materialized_views
```

تشغيل FastAPI:

```powershell
python -m uvicorn api.main:app --host 127.0.0.1 --port 8000
```

فتح Swagger:

```text
http://127.0.0.1:8000/docs
```

---

## 44. ملاحظات مهمة للتقييم

المشروع لا يعتمد داخل منطق التنفيذ على:

```text
Hardcoded File Names
Hardcoded Row Counts
Hardcoded Query Results
Hardcoded Aggregation Results
```

الـRouter يعتمد على حجم الملف الفعلي.

الـQueries تعمل على البيانات الموجودة في MongoDB.

الـAggregations تنتج النتائج من البيانات الفعلية.

Explain Analysis يختار Sample Values من قاعدة البيانات.

Materialized Views تستخدم Incremental Refresh.

FastAPI يعيد استخدام Phase 1 Pipeline بدل إنشاء Ingest Pipeline جديد.

---

## 45. GitHub Repository

المشروع موجود في نفس مستودع GitHub المستخدم في Phase 1:

```text
https://github.com/mm7090mm7090-glitch/midterm-data-pipeline
```

---

## 46. الخلاصة

المشروع يحقق Pipeline متكامل يبدأ من:

```text
CSV
-> Automatic Router
-> Python Batch / PySpark
-> MongoDB Raw
-> Cleaning & Validation
-> Valid / Corrected / Quarantine
-> Upsert & Idempotency
-> Queries
-> Indexes
-> Explain
-> Aggregation Reports
-> Incremental Materialized Views
-> Scheduled Jobs
-> FastAPI
-> Swagger
```

وبذلك تبقى مرحلة Phase 1 موجودة وتعمل كما هي، وتمت إضافة متطلبات Final Project عليها داخل نفس المشروع ونفس GitHub Repository.
