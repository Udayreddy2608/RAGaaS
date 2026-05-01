import hashlib

async def compute_hash(file) -> str:
    sha256 = hashlib.sha256()
    while chunk := await file.read(1024 * 1024):
        sha256.update(chunk)
    await file.seek(0)
    return sha256.hexdigest()