"""Defines the banner grabbing/parsing functions"""

# Standard libraries
import asyncio
import contextlib
from typing import Literal

from constants import BANNER_TYPES

# Project libraries
from utils import Result, Target


async def grab_banner(
    target: Target, banner_type: Literal[BANNER_TYPES], connect_timeout: float, read_timeout: float
) -> Result:
    """Connects to a remote host and attempts to grab an ssh banner

    Args:
        target: The targe IP and port
        connect_timeout: The connection timeout
        read_timeout: The read timeout

    Returns:
        The result of the scanner
    """
    reader = None
    writer = None
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(target.ip, target.port), timeout=connect_timeout
        )
        if banner_type == "ssh":
            banner = await asyncio.wait_for(reader.readline(), timeout=read_timeout)
        else:
            raise KeyError(f"Invalid banner type {banner_type}")
        writer.close()
        await writer.wait_closed()
        return Result(ip=target.ip, port=target.port, banner=banner.decode(errors="replace").strip())
    except Exception as ex:
        return Result(ip=target.ip, port=target.port, error=str(ex))
    finally:
        if writer is not None:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()
