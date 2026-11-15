import asyncio
import hashlib
import json
import socket

import aiohttp
import pytest
import pytest_asyncio
import uvicorn

from flashport.api.main import create_app
from flashport.transfer.client import IntegrityError, TransferClient


@pytest_asyncio.fixture
async def running_server(tmp_path):
    shared = tmp_path / "shared"
    shared.mkdir()
    app = create_app(
        shared_directory=shared,
        capacity_bps=80_000_000,
        allocation_interval=0.01,
    )
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(128)
    listener.setblocking(False)
    port = listener.getsockname()[1]
    server = uvicorn.Server(uvicorn.Config(app, log_level="critical", access_log=False))
    task = asyncio.create_task(server.serve(sockets=[listener]))
    for _ in range(500):
        if server.started:
            break
        if task.done():
            await task
        await asyncio.sleep(0.01)
    else:
        server.should_exit = True
        await task
        raise RuntimeError("test server did not start")
    try:
        yield f"http://127.0.0.1:{port}", shared
    finally:
        server.should_exit = True
        await asyncio.wait_for(task, timeout=10)
        listener.close()


@pytest.mark.asyncio
async def test_file_listing_metadata_ranges_and_secure_paths(running_server) -> None:
    base_url, shared = running_server
    payload = bytes(range(256)) * 300
    (shared / "payload.bin").write_bytes(payload)

    async with aiohttp.ClientSession() as session:
        listing = await session.get(f"{base_url}/api/files")
        assert listing.status == 200
        assert [item["filename"] for item in await listing.json()] == ["payload.bin"]

        metadata_response = await session.get(f"{base_url}/api/files/payload.bin/metadata")
        metadata = await metadata_response.json()
        assert metadata_response.status == 200
        assert metadata["sha256"] == hashlib.sha256(payload).hexdigest()

        registration = await session.post(
            f"{base_url}/api/transfers",
            json={"filename": "payload.bin", "client_id": "range-test"},
        )
        assert registration.status == 200
        transfer = await registration.json()
        transfer_url = f"{base_url}/api/transfers/{transfer['transfer_id']}/content"
        headers = {"X-FlashPort-Token": transfer["token"], "Range": "bytes=17-42"}
        response = await session.get(transfer_url, headers=headers)
        assert response.status == 206
        assert response.headers["Content-Range"] == f"bytes 17-42/{len(payload)}"
        assert await response.read() == payload[17:43]

        invalid_range = await session.get(
            transfer_url,
            headers={**headers, "Range": "bytes=2-5,10-12"},
        )
        assert invalid_range.status == 416
        unauthorized = await session.get(
            transfer_url,
            headers={"Range": "bytes=0-1"},
        )
        assert unauthorized.status == 401
        bad_progress = await session.put(
            f"{base_url}/api/transfers/{transfer['transfer_id']}/progress",
            headers={"X-FlashPort-Token": transfer["token"]},
            json={"downloaded_bytes": len(payload) + 1},
        )
        assert bad_progress.status == 409
        claimed_complete = await session.put(
            f"{base_url}/api/transfers/{transfer['transfer_id']}/progress",
            headers={"X-FlashPort-Token": transfer["token"]},
            json={"downloaded_bytes": len(payload)},
        )
        assert claimed_complete.status == 200
        false_completion = await session.post(
            f"{base_url}/api/transfers/{transfer['transfer_id']}/complete",
            headers={"X-FlashPort-Token": transfer["token"]},
            json={"sha256": transfer["sha256"]},
        )
        assert false_completion.status == 409
        cancelled = await session.post(
            f"{base_url}/api/transfers/{transfer['transfer_id']}/cancel",
            headers={"X-FlashPort-Token": transfer["token"]},
        )
        assert (await cancelled.json())["state"] == "cancelled"
        after_cancel = await session.get(
            transfer_url,
            headers={**headers, "Range": "bytes=0-1"},
        )
        assert after_cancel.status == 409

        traversal = await session.post(
            f"{base_url}/api/transfers",
            json={"filename": "..\\outside.txt", "client_id": "range-test"},
        )
        assert traversal.status == 400
        missing = await session.post(
            f"{base_url}/api/transfers",
            json={"filename": "not-present.txt", "client_id": "range-test"},
        )
        assert missing.status == 404
        (shared / "mutable.bin").write_bytes(b"before")
        mutation_registration = await session.post(
            f"{base_url}/api/transfers",
            json={"filename": "mutable.bin", "client_id": "range-test"},
        )
        mutation = await mutation_registration.json()
        (shared / "mutable.bin").write_bytes(b"changed")
        changed_status = await session.get(
            f"{base_url}/api/transfers/{mutation['transfer_id']}/status",
            headers={"X-FlashPort-Token": mutation["token"]},
        )
        assert changed_status.status == 409

        scheduler = await session.post(
            f"{base_url}/api/scheduler",
            json={"policy": "srpt", "configured_capacity_bps": 50_000_000},
        )
        assert scheduler.status == 200
        assert (await scheduler.json())["selected_policy"] == "srpt"
        simulation = await session.get(f"{base_url}/api/simulation/comparison?seed=9")
        assert (await simulation.json())["measurement_type"] == "simulation"
        dashboard = await session.get(base_url)
        html = await dashboard.text()
        assert "Send a file" in html
        assert "Receive on this device" in html
        benchmarks = await session.get(f"{base_url}/api/benchmarks")
        assert (await benchmarks.json())["measurement_type"] == "simulation"
        graph = await session.get(f"{base_url}/api/benchmarks/graphs/fairness.png")
        assert graph.status == 200


@pytest.mark.asyncio
async def test_browser_upload_delivers_verified_file_to_registered_receivers(running_server) -> None:
    base_url, shared = running_server
    content = bytes((index * 17) % 256 for index in range(90_123))
    async with aiohttp.ClientSession() as session:
        uploaded_response = await session.post(
            f"{base_url}/api/uploads",
            params={"filename": "shared payload.bin"},
            data=content,
        )
        assert uploaded_response.status == 200
        uploaded = await uploaded_response.json()
        assert uploaded["size"] == len(content)
        assert uploaded["sha256"] == hashlib.sha256(content).hexdigest()
        assert not (shared / "shared payload.bin").exists()

        receiver_responses = await asyncio.gather(
            session.post(f"{base_url}/api/receivers", json={"name": "Laptop"}),
            session.post(f"{base_url}/api/receivers", json={"name": "Tablet"}),
        )
        receivers = [await response.json() for response in receiver_responses]
        assert all(response.status == 200 for response in receiver_responses)
        listed = await (await session.get(f"{base_url}/api/receivers")).json()
        assert {item["receiver_id"] for item in listed} == {
            receiver["receiver_id"] for receiver in receivers
        }

        delivery_response = await session.post(
            f"{base_url}/api/deliveries",
            json={
                "upload_id": uploaded["upload_id"],
                "receiver_ids": [receiver["receiver_id"] for receiver in receivers],
            },
        )
        assert delivery_response.status == 200
        delivery = await delivery_response.json()

        for receiver in receivers:
            receiver_headers = {
                "X-FlashPort-Receiver-Token": receiver["token"],
            }
            jobs_response = await session.get(
                f"{base_url}/api/receivers/{receiver['receiver_id']}/deliveries",
                headers=receiver_headers,
            )
            assert jobs_response.status == 200
            [job] = await jobs_response.json()
            transfer_headers = {"X-FlashPort-Token": job["token"]}
            content_response = await session.get(
                f"{base_url}/api/transfers/{job['transfer_id']}/content",
                headers=transfer_headers,
            )
            assert content_response.status == 200
            assert await content_response.read() == content
            progress_response = await session.put(
                f"{base_url}/api/transfers/{job['transfer_id']}/progress",
                headers=transfer_headers,
                json={"downloaded_bytes": len(content)},
            )
            assert progress_response.status == 200
            complete_response = await session.post(
                f"{base_url}/api/transfers/{job['transfer_id']}/complete",
                headers=transfer_headers,
                json={"sha256": hashlib.sha256(content).hexdigest()},
            )
            assert complete_response.status == 200

        status_response = await session.get(
            f"{base_url}/api/deliveries/{delivery['delivery_id']}"
        )
        status = await status_response.json()
        assert all(receiver["state"] == "completed" for receiver in status["receivers"])

        unauthenticated_jobs = await session.get(
            f"{base_url}/api/receivers/{receivers[0]['receiver_id']}/deliveries"
        )
        assert unauthenticated_jobs.status == 401
        removed = await session.delete(
            f"{base_url}/api/receivers/{receivers[0]['receiver_id']}",
            headers={"X-FlashPort-Receiver-Token": receivers[0]["token"]},
        )
        assert removed.status == 200
        remaining = await (await session.get(f"{base_url}/api/receivers")).json()
        assert [item["receiver_id"] for item in remaining] == [receivers[1]["receiver_id"]]


@pytest.mark.asyncio
async def test_parallel_ranges_share_one_transfer_rate_budget(running_server) -> None:
    base_url, shared = running_server
    content = b"x" * 32_768
    (shared / "limited.bin").write_bytes(content)
    async with aiohttp.ClientSession() as session:
        registration = await session.post(
            f"{base_url}/api/transfers",
            json={"filename": "limited.bin", "client_id": "rate-test"},
        )
        transfer = await registration.json()
        url = f"{base_url}/api/transfers/{transfer['transfer_id']}/content"
        token = {"X-FlashPort-Token": transfer["token"]}
        await session.post(
            f"{base_url}/api/scheduler",
            json={"policy": "equal_share", "configured_capacity_bps": 80_000},
        )

        async def fetch(start: int, end: int) -> bytes:
            async with session.get(
                url,
                headers={**token, "Range": f"bytes={start}-{end}"},
            ) as response:
                assert response.status == 206
                return await response.read()

        started = asyncio.get_running_loop().time()
        first_task = asyncio.create_task(fetch(0, 16_383))
        second_task = asyncio.create_task(fetch(16_384, 32_767))
        await asyncio.sleep(0.05)
        scheduler = await (await session.get(f"{base_url}/api/scheduler")).json()
        assert sum(scheduler["allocations"].values()) <= scheduler["configured_capacity_bps"]
        first, second = await asyncio.gather(first_task, second_task)
        elapsed = asyncio.get_running_loop().time() - started

    assert first + second == content
    assert elapsed >= 0.30


@pytest.mark.asyncio
async def test_clients_download_concurrent_chunks_and_empty_files(running_server, tmp_path) -> None:
    base_url, shared = running_server
    first = bytes((index * 37) % 256 for index in range(1_400_123))
    second = bytes((index * 13) % 256 for index in range(330_101))
    (shared / "first.bin").write_bytes(first)
    (shared / "second.bin").write_bytes(second)
    (shared / "empty.bin").write_bytes(b"")

    async with TransferClient(base_url, chunk_size=128_000, max_connections=5) as client_a:
        async with TransferClient(base_url, chunk_size=128_000, max_connections=3) as client_b:
            paths_a, path_b = await asyncio.gather(
                client_a.download_many(
                    ["first.bin", "second.bin", "empty.bin"],
                    tmp_path / "client-a",
                ),
                client_b.download_file("first.bin", tmp_path / "client-b"),
            )

    assert [path.read_bytes() for path in paths_a] == [first, second, b""]
    assert path_b.read_bytes() == first
    assert hashlib.sha256(path_b.read_bytes()).hexdigest() == hashlib.sha256(first).hexdigest()
    with pytest.raises(FileExistsError):
        async with TransferClient(base_url) as client:
            await client.download_file("first.bin", tmp_path / "client-b")

    async with aiohttp.ClientSession() as session:
        status = await session.get(f"{base_url}/api/transfers")
        completed = await status.json()
        assert len(completed) == 4
        assert all(item["state"] == "completed" for item in completed)
        assert all(item["downloaded"] == item["size"] for item in completed)
        metrics = await (await session.get(f"{base_url}/api/network/metrics")).json()
        assert metrics["completed_transfer_count"] == 3
        assert metrics["client_reported_downloaded_bytes"] == 2 * len(first) + len(second)


@pytest.mark.asyncio
async def test_interrupted_download_resumes_from_completed_chunks(running_server, tmp_path) -> None:
    base_url, shared = running_server
    content = bytes((index * 29) % 256 for index in range(500_000))
    (shared / "resume.bin").write_bytes(content)
    destination = tmp_path / "resume-destination"
    callback_count = 0

    def interrupt_after_one_chunk(progress):
        nonlocal callback_count
        if progress["downloaded_bytes"]:
            callback_count += 1
            raise RuntimeError("simulated client interruption")

    async with TransferClient(base_url, chunk_size=64_000, max_connections=1) as client:
        with pytest.raises(RuntimeError, match="interruption"):
            await client.download_file(
                "resume.bin",
                destination,
                progress_callback=interrupt_after_one_chunk,
            )

    part = destination / ".resume.bin.flashport.part"
    manifest = destination / ".resume.bin.flashport.json"
    assert callback_count == 1
    assert part.is_file() and manifest.is_file()
    saved_manifest = json.loads(manifest.read_text(encoding="utf-8"))
    assert len(saved_manifest["completed_chunks"]) == 1
    assert saved_manifest["chunk_hashes"]["0"]

    async with TransferClient(base_url, chunk_size=64_000, max_connections=2) as resumed:
        final_path = await resumed.download_file("resume.bin", destination)
    assert final_path.read_bytes() == content
    assert not part.exists() and not manifest.exists()
    assert hashlib.sha256(final_path.read_bytes()).hexdigest() == hashlib.sha256(content).hexdigest()
    async with aiohttp.ClientSession() as session:
        transfers = await (await session.get(f"{base_url}/api/transfers")).json()
    assert len(transfers) == 1
    assert transfers[0]["state"] == "completed"
    assert transfers[0]["downloaded"] == len(content)
