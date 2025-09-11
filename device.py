from dataclasses import dataclass
from typing import List, Tuple
import logging
import os
import pyodbc
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


@dataclass
class Device:
    ip: str
    name: str

    @staticmethod
    def sqlToModel(rows: List[Tuple]) -> List["Device"]:
        """Convert DB rows (iterable of tuples) to list of Device objects.

        Expected row shape: (IP, DeviceName)
        If DeviceName is NULL, falls back to IP.
        """
        devices: List[Device] = []
        for row in rows:
            if not row:
                continue
            ip = row[0]
            name = None
            if len(row) > 1:
                name = row[1]
            if name is None:
                name = ip
            devices.append(Device(ip=str(ip), name=str(name)))
        return devices


class DeviceRepository:
    """Repository for loading devices from the database.

    Usage:
        repo = DeviceRepository()
        devices = repo.initial_devices()
    """

    def __init__(self) -> None:
        self.device_info: List[Device] = []

    def initial_devices(self) -> List[Device]:
        """Load devices from [EmpBook_db].[dbo].[Device] and populate self.device_info.

        Returns list of Device objects (may be empty on error).
        """
        # Custom static list of device IPs provided by the user.
        ips = [
            "192.168.3.246",
            "192.168.3.227",
            "192.168.3.231",
            "192.168.3.229",
            "192.168.3.243",
            "192.168.3.232",
            "192.168.3.238",
            "192.168.3.244",
            "192.168.3.218",
            "192.168.3.225",
            "192.168.3.241",
            "192.168.3.249",
            "192.168.3.251",
            "192.168.3.213",
            "192.168.3.222",
            "192.168.3.239",
            # "192.168.3.220",
            "192.168.3.233",
            # "192.168.3.225",  # duplicate in source list
            "192.168.3.221",
            "192.168.3.226",
            # "192.168.3.219",
            "192.168.3.247",
            "192.168.3.245",
            "192.168.3.248",
            # "192.168.3.214",
        ]

        # Deduplicate while preserving order
        seen = set()
        devices: List[Device] = []
        for ip in ips:
            if ip in seen:
                continue
            seen.add(ip)
            devices.append(Device(ip=ip, name=ip))

        self.device_info = devices
        return devices


def fetch_initial_devices() -> List[Device]:
    """Convenience function: instantiate repository and return devices."""
    repo = DeviceRepository()
    return repo.initial_devices()
