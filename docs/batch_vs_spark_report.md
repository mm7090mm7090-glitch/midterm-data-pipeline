\# تقرير مقارنة Python Batch و PySpark



\## 1. الهدف من المقارنة



يستخدم المشروع Hybrid Data Pipeline بحيث يتم اختيار محرك المعالجة تلقائيًا حسب حجم ملف CSV.



القواعد المستخدمة:



```text

File size <= 200 MB  -> Python Batch

File size > 200 MB   -> PySpark

