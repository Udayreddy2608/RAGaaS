from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.sdk import Asset
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from common.config import MINIO_BUCKETS

REQUIRED_BUCKETS = list(MINIO_BUCKETS.values())
raw_docs_ready = Asset("s3://raw-docs/ready")


default_args = {
    'owner': 'rag-brain',
    'retries': 3,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False
}

@dag(
    dag_id='ingest_documents',
    default_args=default_args,
    schedule=None,
    start_date=datetime(2026, 4, 26),
    catchup=False,
    max_active_runs=5,
    tags=['rag-brain','ingestion']
)
def ingest_documents():    
    @task
    def read_trigger_conf(**context):
        conf = context['dag_run'].conf
        if not conf:
            raise ValueError("DAG triggered without conf. Expected bucket, key, file_type.")
        
        bucket = conf['bucket']
        key = conf['key']
        file_type = conf['file_type']

        print(f"Processing: MinIO://{bucket}/{key} with file type: {file_type}")
        return {
            'bucket': bucket,
            'key': key,
            'file_type': file_type
        }
    
    @task.branch
    def route_by_type(file_info: dict):
        file_type = file_info['file_type']
        branch_map  = {
            'pdf': 'process_pdf',
            'docx': 'process_docx',
            'txt': 'process_txt',
            'url': 'process_url'
        }
        if file_type not in branch_map:
            raise ValueError(f"Unsupported file type: {file_type}")
        return branch_map[file_type]

    @task(task_id='process_pdf')
    def process_pdf(file_info: dict):
        import pypdf
        import io
        from common.minio_hook import MinioHook
        import hashlib

        hook = MinioHook()
        bucket = file_info['bucket']
        key = file_info['key']
        
        raw_bytes = hook.get_object(bucket=bucket, key=key)

        reader = pypdf.PdfReader(io.BytesIO(raw_bytes))
        pages = [page.extract_text() or "" for page in reader.pages]
        text = "\n\n".join(pages)

        print(f"Extracted {len(pages)} pages, {len(text)} chars from {key}")

        stem = key.split("/")[-1].replace(".pdf", "")
        output_key = f"raw-text/{stem}.txt"
        output_bucket = MINIO_BUCKETS['raw_docs']
        hook.put_object(output_bucket, output_key, text.encode('utf-8'))
        print(f"Uploaded raw text -> MinIO://{output_bucket}/{output_key}")

        return {
            'bucket': output_bucket,
            'key': output_key,
            'source_filename': key,
            'file_hash': hashlib.sha256(raw_bytes).hexdigest(),
        }

    @task(task_id='process_docx')
    def process_docx(file_info: dict):
        import io
        from docx import Document
        from common.minio_hook import MinioHook
        import hashlib

        hook = MinioHook()
        bucket = file_info['bucket']
        key = file_info['key']

        raw_bytes = hook.get_object(bucket=bucket, key=key)
        doc = Document(io.BytesIO(raw_bytes))
        text = "\n\n".join([paragraph.text for paragraph in doc.paragraphs])

        print(f"Extracted {len(text)} chars from {key}")

        stem = key.split("/")[-1].replace(".docx", "")
        output_key = f"raw-text/{stem}.txt"
        output_bucket = MINIO_BUCKETS['raw_docs']
        hook.put_object(output_bucket, output_key, text.encode('utf-8'))
        print(f"Uploaded raw text -> MinIO://{output_bucket}/{output_key}")

        return {
            'bucket': output_bucket,
            'key': output_key,
            'source_filename': key,
            'file_hash': hashlib.sha256(raw_bytes).hexdigest(),
        }

    @task(task_id='process_txt')
    def process_txt(file_info: dict):
        print(f"Processing TXT: MinIO://{file_info['bucket']}/{file_info['key']}")
        from common.minio_hook import MinioHook
        import hashlib

        hook = MinioHook()
        bucket = file_info['bucket']
        key = file_info['key']

        raw_bytes = hook.get_object(bucket=bucket, key=key)
        text = raw_bytes.decode('utf-8', errors = 'replace')

        print(f"Extracted {len(text)} chars from {key}")

        stem = key.split("/")[-1].replace(".txt", "")
        output_key = f"raw-text/{stem}.txt"
        output_bucket = MINIO_BUCKETS['raw_docs']
        hook.put_object(output_bucket, output_key, text.encode('utf-8'))
        print(f"Uploaded raw text -> MinIO://{output_bucket}/{output_key}")

        return {
            'bucket': output_bucket,
            'key': output_key,
            'source_filename': key,
            'file_hash': hashlib.sha256(raw_bytes).hexdigest(),
        }

    @task(task_id='process_url')
    def process_url(file_info: dict):
        import urllib.request
        from common.minio_hook import MinioHook
        import hashlib

        print(f"Processing URL: {file_info['key']}")
        hook = MinioHook()
        bucket = file_info['bucket']
        key = file_info['key']

        raw_bytes = hook.get_object(bucket=bucket, key=key)
        urls = raw_bytes.decode('utf-8').strip().splitlines()

        parts = []
        for url in urls:
            url = url.strip()
            if not url:
                continue
            try:
                with urllib.request.urlopen(url, timeout= 15) as response:
                    content = response.read().decode('utf-8', errors='replace')
                parts.append(f"URL: {url}\nContent:\n{content}\n\n{'-'*80}\n\n")
                print(f"Fetched content from {url} ({len(content)} chars)")
            except Exception as e:
                print(f"Failed to fetch {url}: {e}")
                parts.append(f"URL: {url}\nFailed to fetch content: {e}\n\n{'-'*80}\n\n")
        
        text = "\n\n".join(parts)
        stem = key.split("/")[-1].replace(".txt", "")
        output_key = f"raw-text/{stem}.txt"
        output_bucket = MINIO_BUCKETS['raw_docs']
        hook.put_object(output_bucket, output_key, text.encode('utf-8'))
        print(f"Uploaded URL content -> MinIO://{output_bucket}/{output_key}")
        return {
            'bucket': output_bucket,
            'key': output_key,
            'source_filename': key,
            'file_hash': hashlib.sha256(raw_bytes).hexdigest(),
        }

    @task(
    task_id="done",
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    outlets=[raw_docs_ready],
    )
    def done(**context):
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        ti = context["ti"]
        conf = context['dag_run'].conf
        force = conf.get("force", False)

        file_info = (
            ti.xcom_pull(task_ids="process_pdf")
            or ti.xcom_pull(task_ids="process_docx")
            or ti.xcom_pull(task_ids="process_txt")
            or ti.xcom_pull(task_ids="process_url")
        )

        if not file_info:
            raise ValueError("No file_info found from any parser task.")

        pg = PostgresHook(postgres_conn_id = "postgres_default")
        if force:
            pg.run(
                "DELETE FROM rag.ingested_files WHERE file_hash = %s",
                parameters=(file_info["file_hash"],)
            )

        pg.run(
            """
            INSERT INTO rag.ingested_files
                (file_hash, source_filename, minio_key, dag_run_id)
            VALUES
                (%s, %s, %s, %s)
            ON CONFLICT (file_hash) DO NOTHING
            """,
            parameters=(
                file_info["file_hash"],
                file_info["source_filename"],
                file_info["key"],
                context["dag_run"].run_id,
            )
        )
        context["outlet_events"][raw_docs_ready].extra = file_info
        print(f"Emitting asset with extra: {file_info}")
    
    # buckets = check_buckets()
    file_info = read_trigger_conf()
    branch = route_by_type(file_info)
    pdf = process_pdf(file_info)
    docx = process_docx(file_info)
    txt = process_txt(file_info)
    url = process_url(file_info)
    done_task = done()
    file_info >> branch >> [pdf, docx, txt, url] >> done_task

ingest_documents()