"""Utility functions"""

# Standard libraries
from dataclasses import dataclass


@dataclass
class Target:
    ip: str
    port: int


@dataclass
class Result:
    ip: str
    port: int
    banner: str | None = None
    error: str | None = None
