import uuid
from pathlib import Path

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from config.settings import (
    DATABASE_NAME,
)

from src.file_router import (
    choose_engine,
)

from src.main import (
    run_python_batch,
    run_pyspark,
)

from src.mongo_setup import (
    setup_mongodb,
)

from src.indexes import (
    create_indexes,
)

from src.queries import (
    list_queries,
    run_query,
)

from src.aggregations import (
    list_aggregations,
    run_aggregation,
)

from src.materialized_views import (
    refresh_all_materialized_views,
    list_materialized_views,
)

from src.jobs import (
    list_jobs,
    run_job,
    get_recent_job_logs,
)


# =========================================================
# FastAPI Application
# =========================================================

app = FastAPI(
    title="Big Data Final Project API",
    description=(
        "Unified API for ingestion, queries, indexes, "
        "aggregations, materialized views, and scheduled jobs."
    ),
    version="1.0.0",
)


# =========================================================
# Request Models
# =========================================================

class IngestRequest(BaseModel):
    input_path: str
    run_id: str | None = None


# =========================================================
# Health
# =========================================================

@app.get("/health")
def health():
    return {
        "status": "ok",
        "database": DATABASE_NAME,
    }


# =========================================================
# Ingest
#
# Reuses the SAME Phase 1 hybrid pipeline:
#
# - File Router
# - Python Batch
# - PySpark
# - Raw loading
# - Quality pipeline
# - MongoDB
# =========================================================

@app.post("/ingest")
def ingest(
    request: IngestRequest,
):
    try:
        input_path = Path(
            request.input_path
        ).resolve()

        if not input_path.exists():
            raise HTTPException(
                status_code=404,
                detail=(
                    f"Input file not found: "
                    f"{input_path}"
                ),
            )

        if not input_path.is_file():
            raise HTTPException(
                status_code=400,
                detail=(
                    "input_path must point "
                    "to a file."
                ),
            )

        run_id = (
            request.run_id
            or str(
                uuid.uuid4()
            )
        )

        # Same MongoDB setup used by Phase 1.
        setup_mongodb()

        # Same automatic router used by Phase 1.
        route = choose_engine(
            input_path
        )

        engine = route[
            "engine"
        ]

        if engine == "python_batch":
            result = run_python_batch(
                file_path=input_path,
                run_id=run_id,
            )

            return {
                "status": "success",
                "run_id": run_id,
                "engine": engine,
                "reason":
                    route.get(
                        "reason"
                    ),
                "result": result,
            }

        if engine == "pyspark":
            run_pyspark(
                file_path=input_path,
                run_id=run_id,
            )

            return {
                "status": "success",
                "run_id": run_id,
                "engine": engine,
                "reason":
                    route.get(
                        "reason"
                    ),
                "message":
                    "PySpark pipeline completed.",
            }

        raise HTTPException(
            status_code=400,
            detail=(
                f"Unsupported engine: "
                f"{engine}"
            ),
        )

    except HTTPException:
        raise

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=str(exc),
        )


# =========================================================
# Indexes
# =========================================================

@app.post("/indexes")
def indexes():
    try:
        return create_indexes()

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=str(exc),
        )


# =========================================================
# Queries - List
# =========================================================

@app.get("/queries")
def queries():
    return {
        "count": len(
            list_queries()
        ),
        "queries":
            list_queries(),
    }


# =========================================================
# Queries - Execute by name
#
# Examples:
#
# /queries/orders_by_city?city=عدن
#
# /queries/orders_by_customer?
# customer_id=عميل-8007114
#
# /queries/orders_by_date_range?
# start_date=2026-06-01T00:00:00&
# end_date=2026-06-30T23:59:59
# =========================================================

@app.get("/queries/{name}")
def query_by_name(
    name: str,
    city: str | None = None,
    customer_id: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    status: str | None = None,
    payment_status: str | None = None,
    limit: int = 50,
):
    try:
        if limit < 1:
            raise HTTPException(
                status_code=400,
                detail=(
                    "limit must be "
                    "greater than 0."
                ),
            )

        if limit > 500:
            limit = 500

        # ---------------------------------------------
        # Query 1
        # ---------------------------------------------

        if name == "orders_by_city":
            if not city:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Parameter 'city' "
                        "is required."
                    ),
                )

            results = run_query(
                name,
                city=city,
                limit=limit,
            )

        # ---------------------------------------------
        # Query 2
        # ---------------------------------------------

        elif name == "orders_by_customer":
            if not customer_id:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Parameter "
                        "'customer_id' "
                        "is required."
                    ),
                )

            results = run_query(
                name,
                customer_id=
                    customer_id,
                limit=limit,
            )

        # ---------------------------------------------
        # Query 3
        # ---------------------------------------------

        elif name == "orders_by_date_range":
            if (
                not start_date
                or not end_date
            ):
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Parameters "
                        "'start_date' and "
                        "'end_date' "
                        "are required."
                    ),
                )

            results = run_query(
                name,
                start_date=
                    start_date,
                end_date=
                    end_date,
                limit=limit,
            )

        # ---------------------------------------------
        # Query 4
        # ---------------------------------------------

        elif (
            name
            == "orders_by_status_and_date"
        ):
            if (
                not status
                or not start_date
                or not end_date
            ):
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Parameters 'status', "
                        "'start_date', and "
                        "'end_date' are required."
                    ),
                )

            results = run_query(
                name,
                status=status,
                start_date=
                    start_date,
                end_date=
                    end_date,
                limit=limit,
            )

        # ---------------------------------------------
        # Query 5
        # ---------------------------------------------

        elif (
            name
            == "orders_by_payment_status"
        ):
            if not payment_status:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Parameter "
                        "'payment_status' "
                        "is required."
                    ),
                )

            results = run_query(
                name,
                payment_status=
                    payment_status,
                limit=limit,
            )

        else:
            raise HTTPException(
                status_code=404,
                detail=(
                    f"Unknown query: "
                    f"{name}"
                ),
            )

        return {
            "name": name,
            "count": len(
                results
            ),
            "results": results,
        }

    except HTTPException:
        raise

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=str(exc),
        )


# =========================================================
# Aggregations - List
# =========================================================

@app.get("/aggregations")
def aggregations():
    reports = list_aggregations()

    return {
        "count": len(
            reports
        ),
        "aggregations":
            reports,
    }


# =========================================================
# Aggregations - Execute by name
# =========================================================

@app.get("/aggregations/{name}")
def aggregation_by_name(
    name: str,
    limit: int = 20,
):
    try:
        if limit < 1:
            raise HTTPException(
                status_code=400,
                detail=(
                    "limit must be "
                    "greater than 0."
                ),
            )

        if limit > 500:
            limit = 500

        if name in {
            "sales_by_city",
            "top_customers",
        }:
            results = run_aggregation(
                name,
                limit=limit,
            )

        elif name == "daily_sales":
            results = run_aggregation(
                name,
                limit=limit,
            )

        elif name in {
            "orders_by_status",
            "payment_status_summary",
        }:
            results = run_aggregation(
                name
            )

        else:
            raise HTTPException(
                status_code=404,
                detail=(
                    f"Unknown aggregation: "
                    f"{name}"
                ),
            )

        return {
            "name": name,
            "count": len(
                results
            ),
            "results": results,
        }

    except HTTPException:
        raise

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=str(exc),
        )


# =========================================================
# Materialized Views
# =========================================================

@app.post("/refresh-mv")
def refresh_mv():
    try:
        return (
            refresh_all_materialized_views()
        )

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=str(exc),
        )


# Optional information endpoint.
@app.get("/materialized-views")
def materialized_views():
    return {
        "views":
            list_materialized_views()
    }


# =========================================================
# Jobs - List
# =========================================================

@app.get("/jobs")
def jobs():
    try:
        return {
            "jobs":
                list_jobs(),

            "recent_logs":
                get_recent_job_logs(
                    20
                ),
        }

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=str(exc),
        )


# =========================================================
# Jobs - Manual Run
# =========================================================

@app.post("/jobs/{name}/run")
def run_named_job(
    name: str,
):
    try:
        valid_jobs = {
            job["name"]
            for job in list_jobs()
        }

        if name not in valid_jobs:
            raise HTTPException(
                status_code=404,
                detail=(
                    f"Unknown job: "
                    f"{name}"
                ),
            )

        result = run_job(
            name
        )

        if (
            result.get("status")
            == "failed"
        ):
            raise HTTPException(
                status_code=500,
                detail=result,
            )

        return result

    except HTTPException:
        raise

    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=str(exc),
        )