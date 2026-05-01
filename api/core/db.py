import databases
import sqlalchemy

from api.core.config import settings


database = databases.Database(settings.postgres_dsn)

metadata = sqlalchemy.MetaData(schema= "rag")

ingested_files = sqlalchemy.Table(
    "ingested_files",
    metadata,
    sqlalchemy.Column("file_hash",       sqlalchemy.Text, primary_key=True),
    sqlalchemy.Column("source_filename", sqlalchemy.Text, nullable=False),
    sqlalchemy.Column("minio_key",       sqlalchemy.Text, nullable=False),
    sqlalchemy.Column("dag_run_id",      sqlalchemy.Text, nullable=False),
    sqlalchemy.Column(
        "ingested_at",
        sqlalchemy.DateTime(timezone=True),
        nullable=False,
        server_default=sqlalchemy.text("now()"),
    ),
)

def create_schema_if_missing() -> None:

    sync_dsn = settings.postgres_dsn.replace("+asyncpg", "+psycopg2")
    engine = sqlalchemy.create_engine(sync_dsn)
    try:
        with engine.begin() as conn:
            conn.execute(sqlalchemy.text("CREATE SCHEMA IF NOT EXISTS rag"))
        metadata.create_all(engine, checkfirst=True)
    finally:
        engine.dispose()

async def get_by_hash(file_hash: str):
    row = await database.fetch_one(
        "SELECT * FROM rag.ingested_files WHERE file_hash = :hash",
        values= {"hash": file_hash}
    )
    return row

async def insert(
    file_hash:       str,
    source_filename: str,
    minio_key:       str,
    dag_run_id:      str,
) -> None:
    await database.execute(
        """
        INSERT INTO rag.ingested_files
            (file_hash, source_filename, minio_key, dag_run_id)
        VALUES
            (:hash, :filename, :minio_key, :dag_run_id)
        """,
        values={
            "hash":       file_hash,
            "filename":   source_filename,
            "minio_key":  minio_key,
            "dag_run_id": dag_run_id,
        },
    )

async def delete_by_hash(file_hash: str) -> None:
    await database.execute(
        "DELETE FROM rag.ingested_files WHERE file_hash = :hash",
        values={"hash": file_hash},
    )