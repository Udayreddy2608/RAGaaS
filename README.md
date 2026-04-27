# RagBrain — RAG-as-a-Service Airflow POC

> **RAG-as-a-Service** pipeline powered by **Apache Airflow 3.0**, **MinIO**, **Qdrant**, and **OpenAI** embeddings. Automatically ingests documents (PDF, DOCX, TXT, URL), chunks them, generates vector embeddings, and upserts them into a vector store — all orchestrated as event-driven, data-aware DAGs.

---

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
  - [High-Level System Architecture](#high-level-system-architecture)
  - [DAG Pipeline Flow](#dag-pipeline-flow)
  - [Document Ingestion DAG](#document-ingestion-dag-ingest_documents)
  - [Chunk & Enrich DAG](#chunk--enrich-dag-chunk_and_enrich)
  - [Infrastructure Setup DAG](#infrastructure-setup-dag-setup_infrastructure)
  - [Data Flow Diagram](#data-flow-diagram)
- [Services](#services)
- [Project Structure](#project-structure)
- [DAGs](#dags)
- [Custom Hooks](#custom-hooks)
- [Configuration](#configuration)
- [Getting Started](#getting-started)
- [Environment Variables](#environment-variables)
- [Current Progress](#current-progress)
- [Tech Stack](#tech-stack)

---

## Overview

**RagBrain** is a proof-of-concept implementation of a fully automated **Retrieval-Augmented Generation (RAG)** ingestion pipeline. It leverages Apache Airflow 3.0's **Asset-driven scheduling** (data-aware DAGs) to create a reactive, event-driven document processing system.

### What It Does

1. **Ingests** documents from MinIO object storage (PDF, DOCX, TXT, or URLs)
2. **Routes** each document by type to the appropriate parser
3. **Chunks** raw text using LangChain's `RecursiveCharacterTextSplitter`
4. **Embeds** chunks using OpenAI's `text-embedding-3-small` model (1536 dimensions)
5. **Stores** vector embeddings with rich metadata into a Qdrant collection

---

## Architecture

### High-Level System Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          RAGBRAIN SYSTEM                                │
│                                                                         │
│  ┌──────────────┐    ┌──────────────────────────────────────────────┐  │
│  │   MinIO       │    │              Apache Airflow 3.0              │  │
│  │  (S3-compat.) │    │                                              │  │
│  │               │    │  ┌─────────────────┐  ┌──────────────────┐  │  │
│  │  [uploads]    │───▶│  │ ingest_documents│  │ setup_infra DAG  │  │  │
│  │  [raw-docs]   │◀──│  │      DAG        │  │  (runs @once)    │  │  │
│  │  [chunked-docs│    │  └────────┬────────┘  └──────────────────┘  │  │
│  │  [manifests]  │    │           │ Asset Event                      │  │
│  └──────────────┘    │           ▼                                  │  │
│                       │  ┌─────────────────┐                        │  │
│  ┌──────────────┐    │  │ chunk_and_enrich │                        │  │
│  │   Qdrant      │◀──│  │      DAG        │                        │  │
│  │ Vector Store  │    │  └─────────────────┘                        │  │
│  │               │    └──────────────────────────────────────────────┘  │
│  │  [rag-brain]  │                                                       │
│  │  collection   │    ┌──────────────────────────────────────────────┐  │
│  └──────────────┘    │              OpenAI API                       │  │
│                       │  text-embedding-3-small (1536-dim)           │  │
│  ┌──────────────┐    └──────────────────────────────────────────────┘  │
│  │  PostgreSQL   │                                                       │
│  │  (Airflow DB) │                                                       │
│  └──────────────┘                                                       │
└─────────────────────────────────────────────────────────────────────────┘
```

### High-Level System Architecture (Mermaid)

```mermaid
graph TB
    subgraph External["External Inputs"]
        PDF["PDF Files"]
        DOCX["DOCX Files"]
        TXT["TXT Files"]
        URL["URL Lists"]
    end

    subgraph MinIO["MinIO Object Storage"]
        UB["uploads bucket"]
        RDB["raw-docs bucket"]
        CDB["chunked-docs bucket"]
        MB["manifests bucket"]
    end

    subgraph Airflow["Apache Airflow 3.0"]
        INGEST["ingest_documents DAG"]
        CAE["chunk_and_enrich DAG"]
        INFRA["setup_infrastructure DAG"]
        ASSET["Asset: s3://raw-docs/ready"]
    end

    subgraph VectorDB["Qdrant Vector Store"]
        COL["rag-brain collection\n(cosine, 1536-dim)"]
    end

    subgraph AI["OpenAI API"]
        EMB["text-embedding-3-small"]
    end

    PG["PostgreSQL\n(Airflow metadata DB)"]

    PDF & DOCX & TXT & URL -->|uploaded to| UB
    UB -->|DAG triggered with conf| INGEST
    INGEST -->|extracted text| RDB
    INGEST -->|emits| ASSET
    ASSET -->|triggers| CAE
    RDB -->|pull raw text| CAE
    CAE -->|batch JSON| CDB
    CAE -->|embed texts| EMB
    EMB -->|1536-dim vectors| CAE
    CAE -->|upsert points| COL
    INFRA -->|creates buckets & collection| MinIO & VectorDB
    Airflow <-->|metadata| PG
```

---

### DAG Pipeline Flow

```mermaid
sequenceDiagram
    participant User
    participant Airflow
    participant MinIO
    participant Qdrant
    participant OpenAI

    User->>Airflow: Trigger ingest_documents DAG<br/>{bucket, key, file_type}
    Airflow->>Airflow: read_trigger_conf()
    Airflow->>Airflow: route_by_type() [branch]
    Airflow->>MinIO: get_object(bucket, key)
    MinIO-->>Airflow: raw bytes
    Airflow->>Airflow: parse document (PDF/DOCX/TXT/URL)
    Airflow->>MinIO: put_object(raw-docs, raw-text/*.txt)
    Airflow->>Airflow: emit Asset event [s3://raw-docs/ready]

    Note over Airflow: Asset-triggered DAG auto-starts

    Airflow->>Airflow: chunk_and_enrich DAG triggered
    Airflow->>MinIO: pull_raw_doc() — get text
    Airflow->>Airflow: chunk_document() — split into batches
    Airflow->>MinIO: store batch JSON files
    loop For each batch (parallel via dynamic task mapping)
        Airflow->>OpenAI: embeddings.create(texts)
        OpenAI-->>Airflow: 1536-dim vectors
        Airflow->>Qdrant: upsert(points with metadata)
    end
```

---

### Document Ingestion DAG (`ingest_documents`)

```mermaid
flowchart TD
    A([DAG Triggered\nwith conf]) --> B[read_trigger_conf]
    B --> C{route_by_type}
    C -->|pdf| D[process_pdf\nPyPDF extraction]
    C -->|docx| E[process_docx\npython-docx extraction]
    C -->|txt| F[process_txt\nUTF-8 decode]
    C -->|url| G[process_url\nurllib fetch]
    D --> H([done\nEmit Asset Event])
    E --> H
    F --> H
    G --> H

    style A fill:#6366f1,color:#fff
    style H fill:#10b981,color:#fff
    style C fill:#f59e0b,color:#fff
```

**Key Behaviors:**
- Triggered manually with a JSON `conf` payload: `{ "bucket": "...", "key": "...", "file_type": "..." }`
- Supports up to **5 concurrent runs** (`max_active_runs=5`)
- Automatic **3 retries** with a 2-minute delay on failure
- Emits an **Airflow Asset** (`s3://raw-docs/ready`) with file metadata upon success, automatically triggering the downstream DAG

---

### Chunk & Enrich DAG (`chunk_and_enrich`)

```mermaid
flowchart TD
    ASSET([Asset Event\ns3://raw-docs/ready]) --> A[pull_raw_doc\nread asset event metadata]
    A --> B[chunk_document\nLangChain splitter\nchunk_size=512, overlap=64]
    B --> C{Dynamic Task Mapping\nbatch_size=50}
    C -->|batch_0| D1[tag_metadata\nbatch_0]
    C -->|batch_1| D2[tag_metadata\nbatch_1]
    C -->|batch_N| D3[tag_metadata\nbatch_N]
    D1 & D2 & D3 --> E[(Qdrant\nrag-brain collection)]

    style ASSET fill:#6366f1,color:#fff
    style E fill:#10b981,color:#fff
    style C fill:#f59e0b,color:#fff
```

**Key Behaviors:**
- **Asset-scheduled**: automatically runs whenever `ingest_documents` completes successfully
- Uses **Airflow's dynamic task mapping** (`expand()`) to process all chunk batches in parallel
- Each batch = up to 50 chunks, embedded in a single OpenAI API call
- Points stored in Qdrant with full provenance metadata: `source_filename`, `file_hash`, `chunk_index`, `ingested_at`
- Deterministic **UUID5** point IDs (based on `file_hash + chunk_index`) enable idempotent upserts

---

### Infrastructure Setup DAG (`setup_infrastructure`)

```mermaid
flowchart LR
    A([Runs @once]) --> B[setup\ntask]
    B --> C{Check & Create\nMinIO Buckets}
    B --> D{Check & Create\nQdrant Collection}
    C --> C1[uploads]
    C --> C2[raw-docs]
    C --> C3[chunked-docs]
    C --> C4[manifests]
    D --> D1["rag-brain\ncosine · 1536-dim"]

    style A fill:#6366f1,color:#fff
```

---

### Data Flow Diagram

```mermaid
flowchart LR
    subgraph Input
        DOC["Source Document\n(PDF/DOCX/TXT/URL)"]
    end

    subgraph MinIO Storage
        UP["uploads/\noriginal file"]
        RT["raw-docs/\nraw-text/*.txt"]
        CD["chunked-docs/\n<stem>/batch_N.json"]
    end

    subgraph Processing
        PARSE["Parser\n(file-type specific)"]
        SPLIT["LangChain\nRecursiveCharacterTextSplitter\nchunk=512, overlap=64"]
        EMBED["OpenAI\ntext-embedding-3-small\n1536-dim vector"]
    end

    subgraph Qdrant
        VEC["PointStruct\n• id (UUID5)\n• vector (1536-dim)\n• payload:\n  - text\n  - source_filename\n  - file_hash\n  - chunk_index\n  - ingested_at"]
    end

    DOC --> UP --> PARSE --> RT --> SPLIT --> CD --> EMBED --> VEC
```

---

## Services

The full stack is defined in `docker-compose.yaml` using **Apache Airflow 3.0 with LocalExecutor**:

| Service | Image | Port | Role |
|---|---|---|---|
| `postgres` | `postgres:13` | — | Airflow metadata database |
| `airflow-apiserver` | Custom (Dockerfile) | `8080` | Airflow API + Web UI |
| `airflow-scheduler` | Custom (Dockerfile) | — | DAG scheduling engine |
| `airflow-dag-processor` | Custom (Dockerfile) | — | DAG file parsing |
| `airflow-triggerer` | Custom (Dockerfile) | — | Async deferrable operators |
| `airflow-init` | Custom (Dockerfile) | — | DB migration + admin user creation |
| `qdrant` | `qdrant/qdrant:latest` | `6333` (HTTP), `6334` (gRPC) | Vector database |
| `minio` | `minio/minio:latest` | `9000` (API), `9001` (Console) | S3-compatible object storage |

> **Executor**: `LocalExecutor` (Redis/Celery commented out for simplicity)

---

## Project Structure

```
airflow_adv/
├── dags/
│   ├── infra.py                  # setup_infrastructure DAG (@once)
│   ├── ingest_documents.py       # Document ingestion DAG (manual trigger)
│   ├── chunk_and_enrich.py       # Chunk, embed & upsert DAG (asset-triggered)
│   └── common/
│       ├── __init__.py
│       ├── config.py             # Central config (buckets, chunking, model)
│       ├── minio_hook.py         # Custom Airflow Hook for MinIO
│       └── qdrant_hook.py        # Custom Airflow Hook for Qdrant
├── plugins/
│   └── hooks/                    # (reserved for plugin-based hooks)
├── config/
│   └── airflow.cfg               # Airflow configuration file
├── logs/                         # Airflow task logs (volume-mounted)
├── Dockerfile                    # Custom Airflow image with uv + dependencies
├── docker-compose.yaml           # Full stack: Airflow + MinIO + Qdrant + PG
├── pyproject.toml                # Python dependencies (managed by uv)
├── .env.example                  # Environment variable template
├── .gitignore
└── README.md
```

---

## DAGs

### 1. `setup_infrastructure` — `dags/infra.py`

| Property | Value |
|---|---|
| Schedule | `@once` |
| Tags | `rag-brain`, `setup` |
| Purpose | Creates MinIO buckets and Qdrant collection on first run |

Run this **once** before any ingestion to provision all required infrastructure.

---

### 2. `ingest_documents` — `dags/ingest_documents.py`

| Property | Value |
|---|---|
| Schedule | Manual (`None`) |
| Max Active Runs | 5 |
| Retries | 3 (2-min delay) |
| Tags | `rag-brain`, `ingestion` |
| Output Asset | `s3://raw-docs/ready` |

**Trigger payload:**
```json
{
  "bucket": "uploads",
  "key": "path/to/your/document.pdf",
  "file_type": "pdf"
}
```

Supported `file_type` values: `pdf`, `docx`, `txt`, `url`

---

### 3. `chunk_and_enrich` — `dags/chunk_and_enrich.py`

| Property | Value |
|---|---|
| Schedule | Asset-triggered by `s3://raw-docs/ready` |
| Max Active Runs | 5 |
| Retries | 3 (5-min delay) |
| Tags | `rag-brain`, `chunking` |

Uses **dynamic task mapping** to parallelize embedding across all chunk batches.

---

## Custom Hooks

### `MinioHook` — `dags/common/minio_hook.py`

Extends `airflow.hooks.base.BaseHook` to provide S3-compatible operations via the `minio` Python SDK.

| Method | Description |
|---|---|
| `get_client()` | Lazy-initializes `Minio` client from Airflow connection `minio_default` |
| `create_bucket(name)` | Idempotently creates a bucket |
| `bucket_exists(name)` | Checks if a bucket exists |
| `get_object(bucket, key)` | Downloads an object and returns raw `bytes` |
| `put_object(bucket, key, data)` | Uploads `bytes` or `str` to MinIO |

**Connection ID:** `minio_default`

---

### `QdrantHook` — `dags/common/qdrant_hook.py`

Extends `airflow.hooks.base.BaseHook` to wrap the `qdrant-client` SDK.

| Method | Description |
|---|---|
| `get_client()` | Lazy-initializes `QdrantClient` from Airflow connection `qdrant_default` |
| `collection_exists(name)` | Checks if a collection exists |
| `create_collection(name, size)` | Creates a cosine-distance collection with specified vector size |
| `upsert(collection, points)` | Upserts a list of `PointStruct` objects |

**Connection ID:** `qdrant_default`

---

## Configuration

All tuneable parameters live in `dags/common/config.py`:

```python
MINIO_BUCKETS = {
    "uploads":      "uploads",       # Raw document uploads
    "raw_docs":     "raw-docs",      # Extracted plain text
    "chunked_docs": "chunked-docs",  # Chunk batch JSON files
    "manifests":    "manifests",     # (reserved for future use)
}

CHUNK_CONFIG = {
    "chunk_size": 512,               # Characters per chunk
    "chunk_overlap": 64,             # Overlap between adjacent chunks
    "batch_size": 50,                # Chunks per OpenAI embedding API call
}

QDRANT_COLLECTION = "rag-brain"
EMBEDDING_MODEL    = "text-embedding-3-small"
EMBEDDING_DIMENSION = 1536
```

---

## Getting Started

### Prerequisites

- Docker & Docker Compose
- OpenAI API key

### 1. Clone & Configure

```bash
git clone <repo-url>
cd airflow_adv

cp .env.example .env
# Edit .env with your credentials
```

### 2. Set Up Environment Variables

See [Environment Variables](#environment-variables) below.

### 3. Build & Start All Services

```bash
docker compose up --build -d
```

This will:
- Build the custom Airflow image with all dependencies
- Start PostgreSQL, MinIO, Qdrant, and all Airflow components
- Run DB migrations and create the default admin user (`airflow`/`airflow`)

### 4. Access the UIs

| Service | URL | Credentials |
|---|---|---|
| Airflow Web UI | http://localhost:8080 | `airflow` / `airflow` |
| MinIO Console | http://localhost:9001 | From `.env` |
| Qdrant Dashboard | http://localhost:6333/dashboard | — |

### 5. Add Airflow Connections

In the Airflow UI under **Admin > Connections**, add:

**MinIO Connection:**
| Field | Value |
|---|---|
| Connection ID | `minio_default` |
| Connection Type | `Generic` |
| Host | `minio` |
| Port | `9000` |
| Login | `<MINIO_ROOT_USER>` |
| Password | `<MINIO_ROOT_PASSWORD>` |

**Qdrant Connection:**
| Field | Value |
|---|---|
| Connection ID | `qdrant_default` |
| Connection Type | `Generic` |
| Host | `qdrant` |
| Port | `6333` |

Alternatively, set via environment variables in `.env`:
```
AIRFLOW_CONN_MINIO_DEFAULT=generic://access_key:secret_key@minio:9000
AIRFLOW_CONN_QDRANT_DEFAULT=generic://qdrant:6333
```

### 6. Run the Infrastructure Setup DAG

In the Airflow UI, trigger the `setup_infrastructure` DAG. This creates all MinIO buckets and the Qdrant collection.

### 7. Ingest a Document

Upload your document to the `uploads` MinIO bucket, then trigger `ingest_documents` with:

```json
{
  "bucket": "uploads",
  "key": "my-document.pdf",
  "file_type": "pdf"
}
```

The `chunk_and_enrich` DAG will automatically start once ingestion completes.

---

## Environment Variables

Copy `.env.example` to `.env` and populate:

```env
# MinIO
MINIO_ROOT_USER=your_access_key
MINIO_ROOT_PASSWORD=your_secret_key

# Airflow Connections (inline URI format)
AIRFLOW_CONN_MINIO_DEFAULT=generic://your_access_key:your_secret_key@minio:9000
AIRFLOW_CONN_QDRANT_DEFAULT=generic://qdrant:6333

# OpenAI (set in Airflow Variables or as env var)
OPENAI_API_KEY=sk-...

# Optional port overrides
QDRANT_PORT=6333
QDRANT_GRPC_PORT=6334
MINIO_API_PORT=9000
MINIO_CONSOLE_PORT=9001
```

---

## Current Progress

| Feature | Status |
|---|---|
| Docker Compose full stack | Complete |
| Custom Airflow image (uv-based build) | Complete |
| `setup_infrastructure` DAG | Complete |
| MinIO custom Hook (`MinioHook`) | Complete |
| Qdrant custom Hook (`QdrantHook`) | Complete |
| `ingest_documents` DAG — PDF parser | Complete |
| `ingest_documents` DAG — DOCX parser | Complete |
| `ingest_documents` DAG — TXT parser | Complete |
| `ingest_documents` DAG — URL fetcher | Complete |
| Asset-driven DAG chaining | Complete |
| `chunk_and_enrich` DAG — LangChain chunking | Complete |
| `chunk_and_enrich` DAG — OpenAI embeddings | Complete |
| `chunk_and_enrich` DAG — Qdrant upsert | Complete |
| Dynamic task mapping (parallel batch embedding) | Complete |
| Idempotent UUID5-based point IDs | Complete |
| Retrieval / Query API | Not started |
| Monitoring / alerting hooks | Not started |
| CI/CD pipeline | Not started |
| Kubernetes / production deployment | Not started |

---

## Tech Stack

| Component | Technology | Version |
|---|---|---|
| Workflow Orchestration | Apache Airflow | 3.0.0 |
| Object Storage | MinIO | Latest |
| Vector Database | Qdrant | Latest |
| Metadata Database | PostgreSQL | 13 |
| Embeddings | OpenAI `text-embedding-3-small` | — |
| Text Splitting | LangChain `RecursiveCharacterTextSplitter` | >=0.3.8 |
| PDF Parsing | PyPDF | >=6.10.2 |
| DOCX Parsing | python-docx | >=1.2.0 |
| Package Manager | uv | Latest |
| Python | Python | >=3.12 |
| Container Runtime | Docker + Docker Compose | — |

---

## Notes

- **LocalExecutor** is used for simplicity; the Celery/Redis stack is commented out in `docker-compose.yaml` but can be re-enabled for horizontal scaling.
- The `manifests` bucket is provisioned but not yet used — reserved for future document tracking and deduplication.
- `OPENAI_API_KEY` must be available as an environment variable inside Airflow task containers. The recommended approach is to set it in `.env` so Docker Compose passes it to all services.
- Point IDs are generated as **UUID5** (namespace=`NAMESPACE_DNS`, name=`file_hash_chunkindex`), ensuring the same document ingested twice produces the same IDs, enabling safe, idempotent upserts in Qdrant.

---

> 💡 **This project was architected and designed by the author.** All technical decisions — system design, DAG structure, data flow, and infrastructure — were made deliberately. AI was used as a development assistant, not as a replacement for engineering judgment.
