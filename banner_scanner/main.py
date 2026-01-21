"""Main logic"""

# Standard libraries
import argparse
import asyncio
from pathlib import Path

from constants import (
    BANNER_TYPES,
    CONNECTION_TIMEOUT,
    MAX_CONCURRENCY,
    MAX_IN_QUEUE,
    MAX_OUT_QUEUE,
    READ_TIMEOUT,
    SENTINEL,
    WORKER_COUNT,
    VERSION,
)

# Project libraries
from worker import Stats, csv_to_target_queue_worker, result_queue_to_csv_worker, scan_worker, status_worker


async def main():
    parser = argparse.ArgumentParser(
        prog="Banner Scanner", description="A utility for performing asynchronous banner grabbing"
    )
    parser.add_argument("--version", action="version", version=f"Banner Scanner v{VERSION}")
    parser.add_argument("-i", "--input", required=True, type=Path, help="The input csv file with a ip and port column")
    parser.add_argument("-o", "--output", required=True, type=Path, help="The path to write the output csv file to")
    parser.add_argument("-b", "--banner", required=True, choices=BANNER_TYPES, type=str, help="The banner type")
    parser.add_argument(
        "--concurrency", default=MAX_CONCURRENCY, type=int, help=f"Max concurrency, default: {MAX_CONCURRENCY}"
    )
    parser.add_argument("--in-queue", default=MAX_IN_QUEUE, type=int, help=f"Max input queue, default: {MAX_IN_QUEUE}")
    parser.add_argument(
        "--out-queue", default=MAX_OUT_QUEUE, type=int, help=f"Max output queue, default: {MAX_OUT_QUEUE}"
    )
    parser.add_argument(
        "-w", "--workers", default=WORKER_COUNT, type=int, help=f"Banner grabbing worker count, default: {WORKER_COUNT}"
    )
    parser.add_argument(
        "--connection-timeout",
        default=CONNECTION_TIMEOUT,
        type=float,
        help=f"Timeout for socket connections, default: {CONNECTION_TIMEOUT}",
    )
    parser.add_argument(
        "--read-timeout",
        default=READ_TIMEOUT,
        type=float,
        help=f"Timeout for receiving banners, default: {READ_TIMEOUT}",
    )
    args = parser.parse_args()

    stats = Stats()

    # Create queues and set concurrency
    target_queue = asyncio.Queue(maxsize=args.in_queue)
    result_queue = asyncio.Queue(maxsize=args.out_queue)
    semaphore = asyncio.Semaphore(args.concurrency)

    # Start workers
    worker_tasks = [
        asyncio.create_task(
            scan_worker(
                in_queue=target_queue,
                out_queue=result_queue,
                semaphore=semaphore,
                banner_type=args.banner,
                stats=stats,
                connect_timeout=args.connection_timeout,
                read_timeout=args.read_timeout,
            )
        )
        for i in range(args.workers)
    ]
    read_task = asyncio.create_task(
        csv_to_target_queue_worker(csv_path=args.input, queue=target_queue, worker_count=args.workers)
    )
    write_task = asyncio.create_task(result_queue_to_csv_worker(queue=result_queue, csv_path=args.output))
    status_task = asyncio.create_task(
        status_worker(
            in_queue=target_queue,
            out_queue=result_queue,
            stats=stats,
        )
    )

    # Wait for ingest to finish
    await read_task
    await target_queue.join()

    # Wait for workers to exit after eating sentinel
    await asyncio.gather(*worker_tasks)

    # Tell the egress worker to stop
    await result_queue.put(SENTINEL)
    await result_queue.join()
    await write_task

    # Cancel the status task
    status_task.cancel()
    await status_task


asyncio.run(main())
