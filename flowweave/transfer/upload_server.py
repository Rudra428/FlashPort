"""Chunked, resumable, integrity-verified HTTP upload receiver with atomic finalization."""

from __future__ import annotations

import hashlib
import math
import shutil
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from flowweave.transfer.security import safe_resolve_under, sanitize_filename


@dataclass
class UploadSession:
    session_id: str
    transfer_id: str
    filename: str
    total_bytes: int
    chunk_size: int
    expected_chunks: int
    expected_sha256: str | None
    staging_dir: Path
    created_at: float
    updated_at: float
    received_chunks: set[int] = field(default_factory=set)
    completed: bool = False
    final_path: Path | None = None


class ChunkedUploadStore:
    """Manages staged chunk uploads, ownership checks, SHA-256 verification, and atomic commits."""

    def __init__(self, upload_dir: str | Path = "uploads", max_file_bytes: int = 2 * 1024**3) -> None:
        self.upload_dir = Path(upload_dir)
        self.staging_root = self.upload_dir / ".staging"
        self.max_file_bytes = max_file_bytes
        self.upload_dir.mkdir(parents=True, exist_ok=True)
        self.staging_root.mkdir(parents=True, exist_ok=True)
        self._sessions: dict[str, UploadSession] = {}
        self._by_transfer_id: dict[str, str] = {}
        self._lock = threading.Lock()

    def init_session(
        self,
        *,
        filename: str,
        total_bytes: int,
        chunk_size: int,
        transfer_id: str | None = None,
        expected_sha256: str | None = None,
    ) -> dict[str, Any]:
        """Create a new upload session or resume an existing one owned by `transfer_id`."""
        safe_name = sanitize_filename(filename)
        if total_bytes < 0 or total_bytes > self.max_file_bytes:
            raise ValueError(f"Invalid total_bytes: {total_bytes}")
        if chunk_size <= 0 or chunk_size > 64 * 1024 * 1024:
            raise ValueError(f"Invalid chunk_size: {chunk_size}")

        tid = transfer_id or f"up-{uuid.uuid4().hex[:8]}"
        now = time.time()

        with self._lock:
            # Support resumable upload when the same transfer_id reconnects.
            existing_sid = self._by_transfer_id.get(tid)
            if existing_sid and existing_sid in self._sessions:
                sess = self._sessions[existing_sid]
                if (
                    not sess.completed
                    and sess.filename == safe_name
                    and sess.total_bytes == total_bytes
                    and sess.chunk_size == chunk_size
                ):
                    sess.updated_at = now
                    return {
                        "session_id": sess.session_id,
                        "transfer_id": sess.transfer_id,
                        "filename": sess.filename,
                        "expected_chunks": sess.expected_chunks,
                        "existing_chunks": sorted(sess.received_chunks),
                        "resumed": True,
                    }

            sid = uuid.uuid4().hex[:12]
            expected_chunks = max(1, math.ceil(total_bytes / chunk_size)) if total_bytes > 0 else 1
            staging_dir = self.staging_root / sid
            staging_dir.mkdir(parents=True, exist_ok=True)

            sess = UploadSession(
                session_id=sid,
                transfer_id=tid,
                filename=safe_name,
                total_bytes=total_bytes,
                chunk_size=chunk_size,
                expected_chunks=expected_chunks,
                expected_sha256=expected_sha256.lower() if expected_sha256 else None,
                staging_dir=staging_dir,
                created_at=now,
                updated_at=now,
            )
            self._sessions[sid] = sess
            self._by_transfer_id[tid] = sid
            return {
                "session_id": sid,
                "transfer_id": tid,
                "filename": safe_name,
                "expected_chunks": expected_chunks,
                "existing_chunks": [],
                "resumed": False,
            }

    def store_chunk(
        self,
        *,
        session_id: str,
        transfer_id: str,
        index: int,
        payload: bytes,
        start_byte: int | None = None,
        end_byte: int | None = None,
        chunk_sha256: str | None = None,
    ) -> dict[str, Any]:
        """Validate boundaries & ownership, verify optional chunk hash, and atomically stage chunk."""
        with self._lock:
            sess = self._sessions.get(session_id)
            if sess is None:
                raise KeyError(f"Unknown upload session: {session_id}")
            if sess.completed:
                raise ValueError(f"Upload session {session_id} is already completed")
            if sess.transfer_id != transfer_id:
                raise PermissionError("Transfer ownership mismatch for upload session")
            if index < 0 or index >= sess.expected_chunks:
                raise ValueError(f"Chunk index {index} out of bounds [0, {sess.expected_chunks - 1}]")

            expected_start = index * sess.chunk_size
            expected_end = (
                min(sess.total_bytes - 1, expected_start + sess.chunk_size - 1)
                if sess.total_bytes > 0
                else -1
            )
            expected_len = (expected_end - expected_start + 1) if sess.total_bytes > 0 else 0

            if start_byte is not None and start_byte != expected_start:
                raise ValueError(f"Chunk start mismatch: got {start_byte}, expected {expected_start}")
            if end_byte is not None and end_byte != expected_end:
                raise ValueError(f"Chunk end mismatch: got {end_byte}, expected {expected_end}")
            if len(payload) != expected_len:
                raise ValueError(f"Chunk length mismatch: got {len(payload)}, expected {expected_len}")

            if chunk_sha256 is not None:
                actual_hash = hashlib.sha256(payload).hexdigest()
                if actual_hash.lower() != chunk_sha256.lower():
                    raise ValueError(f"Chunk {index} SHA-256 mismatch")

            staging_dir = sess.staging_dir

        part_path = staging_dir / f"chunk_{index:06d}.part"
        tmp_path = staging_dir / f"chunk_{index:06d}.tmp"
        tmp_path.write_bytes(payload)
        tmp_path.replace(part_path)

        with self._lock:
            sess.received_chunks.add(index)
            sess.updated_at = time.time()
            received_count = len(sess.received_chunks)
            expected_chunks = sess.expected_chunks

        return {
            "session_id": session_id,
            "chunk_index": index,
            "bytes_written": len(payload),
            "received_chunks": received_count,
            "expected_chunks": expected_chunks,
        }

    def get_status(self, session_id: str) -> dict[str, Any]:
        with self._lock:
            sess = self._sessions.get(session_id)
            if sess is None:
                raise KeyError(f"Unknown upload session: {session_id}")
            return {
                "session_id": sess.session_id,
                "transfer_id": sess.transfer_id,
                "filename": sess.filename,
                "total_bytes": sess.total_bytes,
                "expected_chunks": sess.expected_chunks,
                "received_chunks": sorted(sess.received_chunks),
                "completed": sess.completed,
            }

    def finalize_session(self, *, session_id: str, transfer_id: str) -> dict[str, Any]:
        """Reassemble staged chunks, verify SHA-256, atomically finalize file, and clean staging."""
        with self._lock:
            sess = self._sessions.get(session_id)
            if sess is None:
                raise KeyError(f"Unknown upload session: {session_id}")
            if sess.transfer_id != transfer_id:
                raise PermissionError("Transfer ownership mismatch for upload session")
            missing = [i for i in range(sess.expected_chunks) if i not in sess.received_chunks]
            if missing:
                raise ValueError(f"Cannot finalize upload: missing chunks {missing[:10]}")
            staging_dir = sess.staging_dir
            expected_chunks = sess.expected_chunks
            total_bytes = sess.total_bytes
            expected_sha = sess.expected_sha256
            final_name = f"{sess.transfer_id}_{sess.filename}"

        target_path = safe_resolve_under(self.upload_dir, final_name)
        assembled_tmp = staging_dir / "assembled.tmp"
        hasher = hashlib.sha256()
        written = 0

        with assembled_tmp.open("wb") as out_fh:
            for idx in range(expected_chunks):
                part_path = staging_dir / f"chunk_{idx:06d}.part"
                with part_path.open("rb") as in_fh:
                    while True:
                        buf = in_fh.read(65_536)
                        if not buf:
                            break
                        out_fh.write(buf)
                        hasher.update(buf)
                        written += len(buf)

        actual_sha = hasher.hexdigest()
        if written != total_bytes:
            assembled_tmp.unlink(missing_ok=True)
            raise IOError(f"Assembled upload size {written} != expected {total_bytes}")
        if expected_sha and actual_sha.lower() != expected_sha.lower():
            assembled_tmp.unlink(missing_ok=True)
            raise ValueError(f"Upload SHA-256 mismatch: expected {expected_sha}, got {actual_sha}")

        # Atomic finalization into destination directory, then remove staging dir.
        assembled_tmp.replace(target_path)
        shutil.rmtree(staging_dir, ignore_errors=True)

        with self._lock:
            sess.completed = True
            sess.final_path = target_path
            sess.updated_at = time.time()

        return {
            "session_id": session_id,
            "transfer_id": transfer_id,
            "filename": target_path.name,
            "dest_path": str(target_path),
            "total_bytes": written,
            "sha256": actual_sha,
        }

    def abort_session(self, session_id: str) -> bool:
        with self._lock:
            sess = self._sessions.pop(session_id, None)
            if sess is None:
                return False
            self._by_transfer_id.pop(sess.transfer_id, None)
            staging_dir = sess.staging_dir
        shutil.rmtree(staging_dir, ignore_errors=True)
        return True

    def cleanup_abandoned(self, max_age_s: float = 3600.0) -> int:
        """Remove incomplete staged uploads older than `max_age_s`."""
        now = time.time()
        removed = 0
        with self._lock:
            stale_ids = [
                sid
                for sid, sess in self._sessions.items()
                if not sess.completed and (now - sess.updated_at) >= max_age_s
            ]
        for sid in stale_ids:
            if self.abort_session(sid):
                removed += 1
        return removed
