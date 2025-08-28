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
        # build connection string from environment (same style as test_connection.py)
        server = os.getenv("MSSQL_SERVER")
        database = os.getenv("MSSQL_DATABASE")
        username = os.getenv("MSSQL_USER")
        password = os.getenv("MSSQL_PASSWORD")
        driver = os.getenv("MSSQL_ODBC_DRIVER")

        # If the configured driver isn't available on the system, attempt to auto-select
        # a known available MS ODBC driver. This helps when Docker image has msodbcsql18
        # but .env was left as "ODBC Driver 17 for SQL Server".
        try:
            available = [d for d in pyodbc.drivers()]
            if driver and driver not in available:
                # prefer 18 then 17 if present
                if "ODBC Driver 18 for SQL Server" in available:
                    driver = "ODBC Driver 18 for SQL Server"
                elif "ODBC Driver 17 for SQL Server" in available:
                    driver = "ODBC Driver 17 for SQL Server"
                else:
                    # pick first available driver as last resort s
                    driver = available[0] if available else driver
        except Exception:
            # if pyodbc.drivers() fails for any reason, continue with configured driver
            pass

        def make_conn_str(server, database, user, pw, driver):
            return f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};UID={user};PWD={pw};TrustServerCertificate=yes;"

        conn = None
        cursor = None
        try:
            conn_str = make_conn_str(server, os.getenv("MSSQL_DATABASE"), username, password, driver)
            conn = pyodbc.connect(conn_str, timeout=5)
            cursor = conn.cursor()
            select_query = "SELECT [IP],[DeviceName] FROM [EmpBook_db].[dbo].[Device] WITH (NOLOCK) WHERE [Flag] = 1"
            cursor.execute(select_query)
            rows = cursor.fetchall()
            self.device_info = Device.sqlToModel(rows)
            return self.device_info
        except Exception as e:
            logger.exception("equipment Error: %s", e)
            return []
        finally:
            try:
                if cursor:
                    cursor.close()
            except Exception:
                pass
            try:
                if conn:
                    conn.close()
            except Exception:
                pass


def fetch_initial_devices() -> List[Device]:
    """Convenience function: instantiate repository and return devices."""
    repo = DeviceRepository()
    return repo.initial_devices()
