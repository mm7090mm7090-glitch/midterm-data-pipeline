\# Hybrid Big Data Order Processing Pipeline



\## 1. فكرة المشروع



هذا المشروع عبارة عن نظام معالجة بيانات طلبات باستخدام أسلوب Hybrid Data Pipeline.



يعتمد المشروع على:



\- Python Batch Processing للملفات الصغيرة.

\- Apache PySpark للملفات الكبيرة.

\- MongoDB لتخزين البيانات.

\- ELT Architecture بحيث يتم تحميل البيانات إلى Raw أولًا قبل تنظيفها.

\- Data Cleaning \& Validation.

\- Quarantine للبيانات التي لا يمكن تصحيحها بأمان.

\- Upsert وIdempotency لمنع تكرار البيانات.

\- Metrics لقياس أداء كل تشغيل.



\---



\## 2. مسار العمل



```text

CSV File

&#x20;  |

&#x20;  v

Automatic Router

&#x20;  |

&#x20;  +-----------------------+

&#x20;  |                       |

Small File             Large File

<= 200 MB              > 200 MB

&#x20;  |                       |

Python Batch             PySpark

&#x20;  |                       |

&#x20;  +------- MongoDB Raw ---+

&#x20;              |

&#x20;              v

&#x20;      Cleaning \& Validation

&#x20;              |

&#x20;      +-------+---------+

&#x20;      |                 |

Valid / Corrected     Quarantine

&#x20;      |                 |

orders\_validated   quarantine\_orders

