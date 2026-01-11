import http
from typing import TYPE_CHECKING

from celery import current_app
from django.db import connection
from django.http import JsonResponse

if TYPE_CHECKING:
    from django.http import HttpRequest


def health_check(request: "HttpRequest") -> JsonResponse:
    """
    Health check endpoint returning status of Database and Celery.
    """
    # 1. Database Check
    try:
        connection.ensure_connection()
        db_status = "ok"
    except Exception as e:
        db_status = f"error: {str(e)}"

    # 2. Celery Check
    try:
        # Check connection to the broker (SQS)
        with current_app.connection_or_acquire() as conn:
            conn.ensure_connection(max_retries=1)
        celery_status = "ok"
    except Exception as e:
        celery_status = f"error: {str(e)}"

    is_healthy = db_status == "ok" and celery_status == "ok"
    status_code = http.HTTPStatus.OK if is_healthy else http.HTTPStatus.SERVICE_UNAVAILABLE

    return JsonResponse(
        {
            "status": "healthy" if is_healthy else "unhealthy",
            "components": {"database": db_status, "celery": celery_status},
        },
        status=status_code,
    )
