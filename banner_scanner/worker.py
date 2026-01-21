"""Defines the asynchronous workers"""

# Standard libraries
import asyncio
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Literal

import aiofiles
from aiocsv import AsyncDictReader, AsyncDictWriter

# Project libraries
from banner import grab_banner
from constants import SENTINEL
from utils import Result, Target


@dataclass
class Stats:
    enqueued: int = 0
    started: int = 0
    completed: int = 0
    failed: int = 0
    start_time: float = time.monotonic()


async def scan_worker(
    in_queue: asyncio.Queue[Target],
    out_queue: asyncio.Queue[Result],
    semaphore: asyncio.Semaphore,
    banner_type: Literal["ssh"],
    stats: Stats,
    connect_timeout: float = 1.0,
    read_timeout: float = 1.0,
):
    while True:
        target = await in_queue.get()
        try:
            # Exit if a SENTINEL is received
            if target is SENTINEL:
                return

            stats.started += 1

            # Grab banner
            async with semaphore:
                result = await grab_banner(
                    target=target, banner_type=banner_type, connect_timeout=connect_timeout, read_timeout=read_timeout
                )
                if result.banner is None:
                    stats.failed += 1
                else:
                    stats.completed += 1

            # Write result to output queue
            await out_queue.put(result)
        finally:
            in_queue.task_done()


async def status_worker(
    in_queue: asyncio.Queue[Target | object],
    out_queue: asyncio.Queue[Result | object],
    stats: Stats,
    interval: float = 1.0,
):
    try:
        while True:
            await asyncio.sleep(interval)

            elapsed = time.monotonic() - stats.start_time
            rate = (stats.completed + stats.failed) / elapsed if elapsed > 0 else 0.0

            line = (
                f"[STATUS] "
                f"queued={in_queue.qsize():>7} | "
                f"out={out_queue.qsize():>7} | "
                f"started={stats.started:>7} | "
                f"completed={stats.completed:>7} | "
                f"failed={stats.failed:>7} | "
                f"rate={rate:7.1f}/s"
            )
            sys.stdout.write("\r" + line.ljust(110))
            sys.stdout.flush()
    except asyncio.CancelledError:
        sys.stdout.write("\n")
        sys.stdout.flush()
        return


async def result_queue_to_csv_worker(queue: asyncio.Queue, csv_path: Path):
    async with aiofiles.open(file=csv_path, mode="w", encoding="utf-8", newline="") as csv_file:
        writer = None

        while True:
            result = await queue.get()
            try:
                if result is SENTINEL:
                    break

                row = asdict(result)

                # Write headers
                if writer is None:
                    writer = AsyncDictWriter(csv_file, fieldnames=list(row.keys()))
                    await writer.writeheader()

                await writer.writerow(row)
            finally:
                queue.task_done()


async def csv_to_target_queue_worker(csv_path: Path, queue: asyncio.Queue, worker_count: int):
    async with aiofiles.open(file=csv_path, encoding="utf-8", newline="") as csv_file:
        reader = AsyncDictReader(csv_file)

        async for row in reader:
            try:
                host = row["ip"].strip()
                port = int(row["port"].strip())
            except (KeyError, TypeError, ValueError):
                continue
            await queue.put(Target(ip=host, port=port))

    # Mark the end of new targets for the scan workers
    for _ in range(worker_count):
        await queue.put(SENTINEL)
