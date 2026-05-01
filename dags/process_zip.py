from __future__ import annotations

import zipfile
import tempfile
import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from common.minio_hook import MinioHook
# from api.core.utils import compute_hash

default_args = {
    'owner': 'rag-brain',
    'retries': 3,
    'retry_delay': timedelta(minutes=2),
    'email_on_failure': False,
}

@dag(
    dag_id="process_zip",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs= 5,
    tags = ['rag-brain', 'ingestion']
)
def process_zip():
    @task
    def read_trigger_conf(**context):
        conf = context["dag_run"].conf
        if not conf:
            raise ValueError("DAG triggered without conf. Expected bucket, key, force.")
        
        bucket = conf['bucket']
        key = conf['key']
        force = conf.get('force', False)
        print(f"Processing zip: MinIO://{bucket}/{key} force={force}")
        return {
            'bucket': bucket,
            'key':    key,
            'force':  force,
        }
    
    @task 
    def download_zip(zip_info: dict) -> str:
        minio = MinioHook()
        client = minio.get_client()

        tmp = tempfile.NamedTemporaryFile(delete = False, suffix = ".zip")
        tmp_path = tmp.name
        tmp.close()

        client.fget_object(
            bucket_name= zip_info["bucket"],
            object_name= zip_info['key'],
            file_path= tmp_path
        )
        return tmp_path
    
    @task
    def iterate_zip_entries(tmp_path: str):
        entries = []
        with zipfile.ZipFile(tmp_path) as zf:
            for entry in zf.infolist():
                if entry.is_dir():
                    continue

                entry_filename = entry.filename
                entry_type = entry_filename.split(".")[-1].lower()

                if entry_type == "zip":
                    print(f"skipping nested zip: {entry_filename}")
                    continue

                if entry_type not in {"pdf", "docx", "txt"}:
                    print(f"skipping unsupported type: {entry_filename}")
                    continue

                entries.append({
                    "filename": entry_filename,
                    "file_type": entry_type,
                    "file_size": entry.file_size
                })

        print(f"Found {len(entries)} valid entries to process")
        return entries

    @task
    def process_entry(entry: dict, tmp_path: str, force: bool) -> dict:
        import hashlib
        import httpx
        from datetime import datetime, timezone
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = MinioHook()
        client = hook.get_client()

        tmp_entry = tempfile.NamedTemporaryFile(delete=False)
        tmp_entry_path = tmp_entry.name
        tmp_entry.close()

        try:
            with zipfile.ZipFile(tmp_path) as zf:
                with zf.open(entry['filename']) as src, open(tmp_entry_path, 'wb') as dst:
                    dst.write(src.read())

            sha256 = hashlib.sha256()
            with open(tmp_entry_path, 'rb') as f:
                while chunk := f.read(1024 * 1024):
                    sha256.update(chunk)
            file_hash = sha256.hexdigest()

            pg = PostgresHook(postgres_conn_id="postgres_default")
            row = pg.get_first(
                "SELECT file_hash FROM rag.ingested_files WHERE file_hash = %s",
                parameters=(file_hash,)
            )

            if row and not force:
                print(f"skipping duplicate: {entry['filename']}")
                return {"status": "skipped", "filename": entry["filename"]}

            client.fput_object(
                bucket_name="uploads",
                object_name=entry['filename'],
                file_path=tmp_entry_path
            )

            airflow_base_url = os.getenv("AIRFLOW_BASE_URL", "http://airflow-apiserver:8080")
            airflow_api_url = f"{airflow_base_url}/api/v2"
            username = os.getenv("AIRFLOW_USERNAME", "airflow")
            password = os.getenv("AIRFLOW_PASSWORD", "airflow")

            token_resp = httpx.post(
                f"{airflow_base_url}/auth/token",
                json={"username": username, "password": password},
            )
            token_resp.raise_for_status()
            token = token_resp.json()["access_token"]

            dag_resp = httpx.post(
                f"{airflow_api_url}/dags/ingest_documents/dagRuns",
                json={
                    "logical_date": datetime.now(timezone.utc).isoformat(),
                    "conf": {
                        "bucket":    "uploads",
                        "key":       entry['filename'],
                        "file_type": entry['file_type'],
                        "force":     force,
                    },
                },
                headers={"Authorization": f"Bearer {token}"},
            )
            dag_resp.raise_for_status()
            dag_run_id = dag_resp.json()["dag_run_id"]

            print(f"Triggered ingest_documents for {entry['filename']} → {dag_run_id}")
            return {"status": "triggered", "filename": entry['filename'], "dag_run_id": dag_run_id}

        finally:
            os.remove(tmp_entry_path)

    @task
    def get_force(zip_info: dict) -> bool:
        return zip_info['force']
    
    zip_info = read_trigger_conf()
    tmp_path = download_zip(zip_info)
    entries  = iterate_zip_entries(tmp_path)
    force = get_force(zip_info)

    process_entry.partial(
        tmp_path=tmp_path,
        force=force
    ).expand(entry=entries)

process_zip()