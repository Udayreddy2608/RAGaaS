import httpx
from api.core.config import settings

async def get_airflow_token() -> str:
    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{settings.airflow_base_url}/auth/token",
            json={
                "username": settings.airflow_username,
                "password": settings.airflow_password,
            },
        )
        response.raise_for_status()
        return response.json()["access_token"]


from datetime import datetime, timezone

async def trigger_dag(dag_conf: dict, dag_id: str = "ingest_documents") -> str:
    token = await get_airflow_token()

    async with httpx.AsyncClient() as client:
        response = await client.post(
            f"{settings.airflow_base_url}/api/v2/dags/{dag_id}/dagRuns",
            json={
                "conf": dag_conf,
                "logical_date": datetime.now(timezone.utc).isoformat(),
            },
            headers={
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json",
            },
        )
        if not response.is_success:
            print("Airflow error:", response.text, flush=True)
        response.raise_for_status()
        return response.json()["dag_run_id"]