from fastapi import APIRouter, File, Query, UploadFile

from api.core.airflow import trigger_dag
from api.core.db import get_by_hash
from api.core.minio import upload_to_minio
from api.core.utils import compute_hash

import zipfile
import tempfile
import os

router = APIRouter()

SUPPORTED_TYPES = {"pdf", "docx", "txt"}

@router.post("/upload")
async def upload(
    file: UploadFile = File(...),
    force: bool = Query(default= False)
):
    triggered = []
    skipped = []
    rejected = []

    filename = file.filename
    file_type = filename.split(".")[-1].lower()

    if file_type == "zip":
        if file_type == "zip":
            dag_conf = await upload_to_minio(file, file.filename, file.size, "zip", force)
            dag_run_id = await trigger_dag(dag_conf, dag_id= "process_zip")
            triggered.append({"filename": filename, "dag_run_id": dag_run_id})

    elif file_type in SUPPORTED_TYPES:
        file_hash = await compute_hash(file)
        existing = await get_by_hash(file_hash)

        if existing and not force:
            skipped.append({"filename" : filename,
                            "file_hash": file_hash})
        
        else:
            dag_conf = await upload_to_minio(file, filename, file.size, file_type, force)
            dag_run_id = await trigger_dag(dag_conf)

            triggered.append({
                "filename": filename,
                "dag_run_id": dag_run_id
            })
    else:
        rejected.append({"filename": filename,
                         "reason": f"unsupported file type"
                         f"{file_type}. supported: {SUPPORTED_TYPES}"})
    
    return {
        "triggered": triggered,
        "skipped": skipped,
        "rejected": rejected
    }