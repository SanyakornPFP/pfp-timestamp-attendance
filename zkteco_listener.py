import logging
import os
import threading
import signal
import sys
from typing import List, Optional, Union
from datetime import datetime

import pyodbc
from dotenv import load_dotenv

from device import fetch_initial_devices, Device

try:
	from zk import ZK
except ImportError:
	raise SystemExit("Missing dependency 'pyzk'. Install via pip: pip install pyzk")

logger = logging.getLogger("zkteco_listener")
logging.basicConfig(
	level=os.getenv("LOG_LEVEL", "INFO"),
	format="%(asctime)s %(levelname)s [%(threadName)s] %(message)s",
)


STOP_EVENT = threading.Event()

# Load environment variables from .env if present
load_dotenv()


def _choose_sql_driver(env_driver: Optional[str] = None) -> Optional[str]:
	available_drivers = [d for d in pyodbc.drivers() if d and d.strip()]
	preferred = []
	if env_driver:
		preferred.append(env_driver)
	preferred.extend([
		'ODBC Driver 18 for SQL Server',
		'ODBC Driver 17 for SQL Server',
		'ODBC Driver 13 for SQL Server',
		'SQL Server Native Client 11.0',
		'SQL Server'
	])
	for d in preferred:
		if d in available_drivers:
			return d
	return None


def _open_sql_connection() -> Optional[pyodbc.Connection]:
	server = os.getenv("MSSQL_SERVER")
	database = os.getenv("MSSQL_DATABASE")
	username = os.getenv("MSSQL_USER")
	password = os.getenv("MSSQL_PASSWORD")
	env_driver = os.getenv("MSSQL_ODBC_DRIVER")

	missing = [name for name, val in (("MSSQL_SERVER", server), ("MSSQL_DATABASE", database), ("MSSQL_USER", username), ("MSSQL_PASSWORD", password)) if not val]
	if missing:
		logger.error("Missing MSSQL env vars for WorkTimeAlert: %s", ",".join(missing))
		return None

	driver = _choose_sql_driver(env_driver)
	if not driver:
		logger.error("No suitable ODBC driver found for SQL Server. Set MSSQL_ODBC_DRIVER or install Microsoft ODBC driver.")
		return None

	conn_str = f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};UID={username};PWD={password};TrustServerCertificate=yes;"
	try:
		return pyodbc.connect(conn_str, timeout=5)
	except Exception as e:
		logger.error("Open SQL connection failed: %s", e)
		return None


def _parse_attendance_timestamp(value) -> Optional[datetime]:
	if value is None:
		return None


def _normalize_user_id(user_id: Optional[Union[str, int]]) -> Optional[str]:
	"""Return user_id as a zero-left-padded 5-char string (e.g., 5233 -> 05233).
	If user_id is None or empty after str/strip, returns None.
	"""
	if user_id is None:
		return None
	s = str(user_id).strip()
	if not s:
		return None
	if len(s) < 5:
		s = s.zfill(5)
	return s
	if isinstance(value, datetime):
		return value
	try:
		# Common format: 'YYYY-MM-DD HH:MM:SS'
		return datetime.fromisoformat(str(value))
	except Exception:
		return None


def upsert_worktime_alert_if_needed(db: pyodbc.Connection, emp_id, ip: str, ts: datetime) -> bool:
	"""Insert a row into WorkTimeAlert if last scan for today is >= 10 minutes ago.

	Returns True if inserted, False if skipped.
	"""
	try:
		cur = db.cursor()
		# Get latest record for this employee and device today
		cur.execute(
			"""
			SELECT TOP 1 [DateTimeStamp]
			FROM [EmpBook_db].[dbo].[WorkTimeAlert] WITH (NOLOCK)
			WHERE [EmpId] = ? AND [IPStamp] = ? AND CAST([DateTimeStamp] AS date) = CAST(? AS date)
			ORDER BY [DateTimeStamp] DESC
			""",
			emp_id, ip, ts
		)
		row = cur.fetchone()
		if row and row[0]:
			last_ts = row[0]
			# pyodbc returns datetime already; ensure type
			if not isinstance(last_ts, datetime):
				try:
					last_ts = datetime.fromisoformat(str(last_ts))
				except Exception:
					last_ts = None
			if last_ts is not None:
				diff_sec = (ts - last_ts).total_seconds()
				if diff_sec < 600:
					# Less than 10 minutes, skip insert
					return False

		# Insert new alert
		cur.execute(
			"""
			INSERT INTO [EmpBook_db].[dbo].[WorkTimeAlert] ([DateTimeStamp],[EmpId],[IPStamp],[IsSend])
			VALUES (?,?,?,0)
			""",
			ts, emp_id, ip
		)
		db.commit()
		return True
	except Exception as e:
		logger.error("WorkTimeAlert insert check failed for emp=%s ip=%s: %s", emp_id, ip, e)
		try:
			db.rollback()
		except Exception:
			pass
		return False


def run_live_capture(device: Device, port: int) -> None:
	"""Connect to a ZKTeco device and stream live attendance events.

	Only uses the live_capture() generator. Runs until STOP_EVENT is set.
	"""
	zk = ZK(
		device.ip,
		port=port,
		timeout=10,
		password=0,
		force_udp=False,
		ommit_ping=False,
	)
	conn = None
	db_conn = None
	try:
		logger.info("Connecting to device '%s' (%s:%d)", device.name, device.ip, port)
		conn = zk.connect()
		logger.info("Connected: %s (%s)", device.name, device.ip)
		# Open a DB connection for this thread
		db_conn = _open_sql_connection()
		if db_conn is None:
			logger.error("DB connection unavailable; live capture will still run but inserts disabled.")

		# live_capture is a generator producing attendance events continuously
		for attendance in conn.live_capture():
			if STOP_EVENT.is_set():
				break
			# Skip empty / heartbeat messages (None)
			if attendance is None:
				continue
			# Safely extract attributes (pyzk Attendance object can vary by device)
			user_id_raw = getattr(attendance, 'user_id', getattr(attendance, 'uid', None))
			user_id = _normalize_user_id(user_id_raw)
			timestamp = getattr(attendance, 'timestamp', getattr(attendance, 'time', None))
			status = getattr(attendance, 'status', None)
			punch = getattr(attendance, 'punch', getattr(attendance, 'type', None))
			# Format similar to requested example
			# Only log when it's a real attendance
			logger.info("LiveCapture %s: <Attendance>: %s : %s (%s, %s)", device.ip, user_id, timestamp, status, punch)

			# Insert into WorkTimeAlert with 10-minute spacing
			if db_conn is not None and user_id is not None:
				ts_dt = _parse_attendance_timestamp(timestamp) or datetime.now()
				inserted = upsert_worktime_alert_if_needed(db_conn, user_id, device.ip, ts_dt)
				if inserted:
					logger.debug("Inserted WorkTimeAlert for emp=%s ip=%s at %s", user_id, device.ip, ts_dt)
				else:
					logger.debug("Skipped insert (within 10 minutes) for emp=%s ip=%s", user_id, device.ip)
	except KeyboardInterrupt:
		pass
	except Exception as e:
		logger.error("Device %s (%s) error: %s", device.name, device.ip, e)
	finally:
		try:
			if conn:
				conn.disconnect()
		except Exception:
			pass
		try:
			if db_conn:
				db_conn.close()
		except Exception:
			pass
		logger.info("Disconnected from %s (%s)", device.name, device.ip)


def setup_signal_handlers(threads: List[threading.Thread]):
	def _handler(signum, frame):
		logger.info("Signal %s received – stopping live capture...", signum)
		STOP_EVENT.set()
		for t in threads:
			t.join(timeout=5)
		logger.info("All threads stopped. Exiting.")
		sys.exit(0)

	for sig in (signal.SIGINT, signal.SIGTERM):
		try:
			signal.signal(sig, _handler)
		except Exception:
			# On Windows, SIGTERM may not be available; ignore silently
			pass


def main():
	port_env = os.getenv("ZK_PORT")
	try:
		port = int(port_env) if port_env else 4370
	except ValueError:
		logger.warning("Invalid ZK_PORT '%s', falling back to 4370", port_env)
		port = 4370

	devices = fetch_initial_devices()
	if not devices:
		logger.error("No devices loaded from database. Ensure DB connectivity and records.")
		return 1

	logger.info("Loaded %d devices: %s", len(devices), 
				", ".join(f"{d.name}({d.ip})" for d in devices))

	threads: List[threading.Thread] = []
	for device in devices:
		t = threading.Thread(
			target=run_live_capture, args=(device, port), name=f"LiveCapture-{device.ip}", daemon=True
		)
		threads.append(t)
		t.start()

	setup_signal_handlers(threads)

	logger.info("Live capture running. Press Ctrl+C to stop.")
	try:
		while any(t.is_alive() for t in threads):
			for t in threads:
				t.join(timeout=0.5)
			if STOP_EVENT.is_set():
				break
	except KeyboardInterrupt:
		STOP_EVENT.set()
		for t in threads:
			t.join(timeout=5)

	logger.info("Shutdown complete.")
	return 0


if __name__ == "__main__":
	sys.exit(main())

