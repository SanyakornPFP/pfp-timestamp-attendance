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
        env_driver = os.getenv("MSSQL_ODBC_DRIVER")

        def make_conn_str(server, database, user, pw, driver):
            return f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};UID={user};PWD={pw};TrustServerCertificate=yes;"

        # helper: mask password for logging
        def masked_conn_str(driver):
            masked_pw = "***" if password else ""
            return f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};UID={username};PWD={masked_pw};"

        conn = None
        cursor = None
        try:
            # Validate required env vars
            missing = [name for name, val in (('MSSQL_SERVER', server), ('MSSQL_DATABASE', database), ('MSSQL_USER', username), ('MSSQL_PASSWORD', password)) if not val]
            if missing:
                logger.error("Missing required MSSQL environment variables: %s", ",".join(missing))
                return []

            # Detect installed ODBC drivers and pick a suitable SQL Server driver
            available_drivers = [d for d in pyodbc.drivers() if d and d.strip()]
            logger.debug("Available ODBC drivers: %s", available_drivers)

            preferred_drivers = []
            if env_driver:
                preferred_drivers.append(env_driver)
            # common Microsoft drivers in descending preference
            preferred_drivers.extend([
                'ODBC Driver 18 for SQL Server',
                'ODBC Driver 17 for SQL Server',
                'ODBC Driver 13 for SQL Server',
                'SQL Server Native Client 11.0',
                'SQL Server'
            ])

            chosen_driver = None
            for d in preferred_drivers:
                if d in available_drivers:
                    chosen_driver = d
                    break

            if not chosen_driver:
                logger.error("No suitable ODBC driver found. Available drivers: %s. Please install Microsoft ODBC Driver for SQL Server and/or set MSSQL_ODBC_DRIVER in your environment.", available_drivers)
                return []

            conn_str = make_conn_str(server, database, username, password, chosen_driver)
            logger.info("Connecting to DB using driver '%s' — %s", chosen_driver, masked_conn_str(chosen_driver))

            # small retry loop for transient issues
            last_exc = None
            for attempt in range(1, 3):
                try:
                    conn = pyodbc.connect(conn_str, timeout=5)
                    break
                except Exception as e:
                    logger.warning("DB connect attempt %d failed: %s", attempt, e)
                    last_exc = e
                    if attempt < 2:
                        import time
                        time.sleep(1)
            else:
                # all attempts failed
                raise last_exc
            cursor = conn.cursor()
            select_query = "SELECT [IP],[DeviceName] FROM [EmpBook_db].[dbo].[Device] WITH (NOLOCK) WHERE [Flag] = 1"
            cursor.execute(select_query)
            rows = cursor.fetchall()
            self.device_info = Device.sqlToModel(rows)
            return self.device_info
        except Exception as e:
            # log the exception with guidance for IM002 driver errors
            if isinstance(e, pyodbc.InterfaceError) or (hasattr(e, 'args') and e.args and 'IM002' in str(e.args[0])):
                logger.exception("equipment Error (ODBC driver issue): %s — Available drivers: %s. Recommend installing Microsoft ODBC Driver for SQL Server or setting MSSQL_ODBC_DRIVER environment variable.", e, pyodbc.drivers())
            else:
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