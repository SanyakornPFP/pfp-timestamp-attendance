import logging
import os
import threading
import signal
import sys
from typing import List, Optional, Union
from datetime import datetime, timedelta

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


try:
	ATTENDANCE_TZ_OFFSET = int(os.getenv("ATTENDANCE_TZ_OFFSET", "0"))
except ValueError:
	ATTENDANCE_TZ_OFFSET = 0
	logger.warning("Invalid ATTENDANCE_TZ_OFFSET; falling back to 0.")


def _parse_attendance_timestamp(value) -> Optional[datetime]:
	"""Parse an attendance timestamp value to a datetime.

	Supports already-datetime objects and ISO/basic string formats. Applies an
	optional hour offset via ATTENDANCE_TZ_OFFSET env (e.g. set to 7 for UTC+7).
	Returns None if parsing fails.
	"""
	if value is None:
		return None
	if isinstance(value, datetime):
		dt = value
	else:
		# Try ISO first
		try:
			dt = datetime.fromisoformat(str(value))
		except Exception:
			# Fallback common format
			try:
				dt = datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S")
			except Exception:
				return None
	# Apply timezone offset if configured
	if ATTENDANCE_TZ_OFFSET:
		dt = dt + timedelta(hours=ATTENDANCE_TZ_OFFSET)
	return dt


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


def get_employee_shift(db: pyodbc.Connection, emp_id: str, ts: datetime):
	"""ดึงข้อมูลกะงานจาก [db_pfpdashboard].[dbo].[VListPeriodEmployee]
	ตรวจสอบทั้งวันนี้และเมื่อวานเพื่อรองรับกะงานข้ามคืน
	"""
	try:
		cur = db.cursor()
		date_today = ts.date()
		date_yesterday = date_today - timedelta(days=1)

		cur.execute(
			"""
			SELECT [DatePeriod], [InTmp], [OutTmp]
			FROM [db_pfpdashboard].[dbo].[VListPeriodEmployee] WITH (NOLOCK)
			WHERE [EmpId] = ? AND [DatePeriod] IN (?, ?)
			ORDER BY [DatePeriod] DESC
			""",
			emp_id, date_today, date_yesterday
		)
		rows = cur.fetchall()

		for row in rows:
			d_period, in_val, out_val = row
			if not in_val or not out_val:
				continue
			
			# แปลงเวลา (InTmp/OutTmp) เป็น datetime
			def combine_time(d, t):
				if isinstance(t, str):
					try: t = datetime.strptime(t[:5], "%H:%M").time()
					except: return None
				return datetime.combine(d, t)

			shift_start = combine_time(d_period.date() if hasattr(d_period, "date") else d_period, in_val)
			shift_end = combine_time(d_period.date() if hasattr(d_period, "date") else d_period, out_val)

			if not shift_start or not shift_end:
				continue

			# กรณีดึก (เช่น 22:00 - 06:00)
			if shift_end <= shift_start:
				shift_end += timedelta(days=1)

			# กำหนดช่วงเวลา (Window) ที่อนุญาตให้บันทึกในกะนี้ (เช่น ก่อนสแกนเข้า 4 ชม. ถึง หลังสแกนออก 8 ชม.)
			window_start = shift_start - timedelta(hours=4)
			window_end = shift_end + timedelta(hours=8)

			if window_start <= ts <= window_end:
				return d_period, shift_start, shift_end

		return None, None, None
	except Exception as e:
		logger.error("Fetch shift failed for %s: %s", emp_id, e)
		return None, None, None


def upsert_attendance_log(db: pyodbc.Connection, emp_id: str, ip: str, ts: datetime) -> bool:
	"""บันทึกเวลาเข้า-ออก โดยอ้างอิงจากกะงาน (Shift-based Logic)
	
	เงื่อนไข:
	1. ค้นหากะงานที่ครอบคลุมเวลาที่แสกน (ts)
	2. ตรวจสอบว่าในกะนั้นมีการบันทึก TimeIn ไปแล้วหรือยัง
	3. ถ้ายังไม่มี -> INSERT (TimeIn)
	4. ถ้ามีแล้ว -> UPDATE (TimeOut)
	"""
	try:
		cur = db.cursor()
		
		# 1. ค้นหากะงาน
		shift_date, shift_start, shift_end = get_employee_shift(db, emp_id, ts)

		# 2. ค้นหา Record เดิมที่อยู่ในช่วงเวลาของกะงานนี้
		if shift_date:
			# ใช้ช่วงเวลาของกะงานเป็นเกณฑ์ในการหา Record (TimeIn ควรอยู่ใกล้ช่วงเริ่มกะ)
			cur.execute(
				"""
				SELECT TOP 1 [Id], [TimeIn], [TimeOut]
				FROM [EmpBook_db].[dbo].[TimeAttandanceLog] WITH (NOLOCK)
				WHERE [EmpId] = ? 
				  AND [TimeIn] >= ? 
				  AND [TimeIn] <= ?
				ORDER BY [TimeIn] DESC
				""",
				emp_id, 
				shift_start - timedelta(hours=4),
				shift_start + timedelta(hours=14)
			)
		else:
			# Fallback: กรณีไม่พบกะงาน ให้ใช้ตรรกะเดิม (หา Record ล่าสุด)
			cur.execute(
				"""
				SELECT TOP 1 [Id], [TimeIn], [TimeOut]
				FROM [EmpBook_db].[dbo].[TimeAttandanceLog] WITH (NOLOCK)
				WHERE [EmpId] = ?
				ORDER BY [DateTimeStamp] DESC
				""",
				emp_id
			)
		
		row = cur.fetchone()

		if row:
			row_id, first_in, last_out = row
			diff_sec = (ts - first_in).total_seconds()

			# ป้องกันการแสกนซ้ำซ้อนในเวลาอันสั้น ( < 10 นาที)
			if diff_sec < 600:
				return False

			# ถ้าเข้าเงื่อนไขว่าเคยสแกนเข้าแล้ว -> ให้บันทึกเป็นสแกนออก (Update TimeOut)
			# รองรับทั้งการออกก่อนเวลา และการอัปเดตเวลาออกล่าสุด (ภายใน 1 ชม. หลังสแกนออกเดิม)
			can_update = False
			if last_out is None:
				can_update = True
			else:
				diff_from_last_out = (ts - last_out).total_seconds()
				if 0 < diff_from_last_out < 3600: # สแกนซ้ำภายใน 1 ชม. ให้ทับเวลาเดิม
					can_update = True

			if can_update:
				cur.execute(
					"""
					UPDATE [EmpBook_db].[dbo].[TimeAttandanceLog]
					SET [TimeOut] = ?, [IPStampOut] = ?
					WHERE [Id] = ?
					""",
					ts, ip, row_id
				)
				db.commit()
				return True

		# 3. กรณีเข่างานครั้งแรกของกะ (TimeIn)
		cur.execute(
			"""
			INSERT INTO [EmpBook_db].[dbo].[TimeAttandanceLog] 
			([DateTimeStamp], [EmpId], [IPStampIn], [TimeIn], [TimeOut])
			VALUES (?, ?, ?, ?, NULL)
			""",
			ts, emp_id, ip, ts
		)
		db.commit()
		return True
	except Exception as e:
		logger.error("Attendance log failed for emp=%s: %s", emp_id, e)
		try: db.rollback()
		except: pass
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

			# Insert into TimeAttandanceLog
			if db_conn is not None and user_id is not None:
				ts_dt = _parse_attendance_timestamp(timestamp) or datetime.now()
				inserted = upsert_attendance_log(db_conn, user_id, device.ip, ts_dt)
				if inserted:
					logger.info("Processed attendance for emp=%s at %s", user_id, ts_dt)
				else:
					logger.debug("Skipped or updated (duplicate/threshold) for emp=%s", user_id)
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