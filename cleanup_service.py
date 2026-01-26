"""Standalone Cleanup Service for Incomplete Attendance Records

บริการนี้ทำหน้าที่ตรวจสอบและปิด Record ที่ค้างอยู่ (TimeOut IS NULL)
โดยเฉพาะกรณีที่ไม่มีกะงาน (No Shift) และ Record เก่าเกิน 16 ชั่วโมง

สามารถรันแยกจาก zkteco_listener.py ได้
"""

import logging
import os
import signal
import sys
import time
from datetime import datetime, timedelta
from typing import Optional

import pyodbc
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("cleanup_service")
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)

# ค่าเริ่มต้น: ตรวจสอบทุก 4 ชั่วโมง
CLEANUP_INTERVAL_SECONDS = int(os.getenv("CLEANUP_INTERVAL_SECONDS", "14400"))
# เกณฑ์เวลาที่ถือว่า Record เก่าเกินไป (ชั่วโมง)
CLEANUP_THRESHOLD_HOURS = int(os.getenv("CLEANUP_THRESHOLD_HOURS", "16"))

STOP_FLAG = False


def _choose_sql_driver(env_driver: Optional[str] = None) -> Optional[str]:
    """เลือก ODBC Driver ที่เหมาะสมสำหรับ SQL Server"""
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
    """เปิด Connection ไปยัง SQL Server"""
    server = os.getenv("MSSQL_SERVER")
    database = os.getenv("MSSQL_DATABASE")
    username = os.getenv("MSSQL_USER")
    password = os.getenv("MSSQL_PASSWORD")
    env_driver = os.getenv("MSSQL_ODBC_DRIVER")

    missing = [
        name for name, val in (
            ("MSSQL_SERVER", server),
            ("MSSQL_DATABASE", database),
            ("MSSQL_USER", username),
            ("MSSQL_PASSWORD", password)
        ) if not val
    ]
    if missing:
        logger.error("Missing MSSQL env vars: %s", ", ".join(missing))
        return None

    driver = _choose_sql_driver(env_driver)
    if not driver:
        logger.error("No suitable ODBC driver found for SQL Server.")
        return None

    conn_str = f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};UID={username};PWD={password};TrustServerCertificate=yes;"
    try:
        return pyodbc.connect(conn_str, timeout=5)
    except Exception as e:
        logger.error("Open SQL connection failed: %s", e)
        return None


def cleanup_incomplete_records(db: pyodbc.Connection) -> int:
    """ตรวจสอบและปิด Record ที่ค้างอยู่ (TimeOut IS NULL) เกินกว่า threshold

    Returns:
        จำนวน Record ที่ถูก cleanup
    """
    cleaned_count = 0
    try:
        cur = db.cursor()
        threshold_time = datetime.now() - timedelta(hours=CLEANUP_THRESHOLD_HOURS)

        # ค้นหา Record ที่ค้าง (TimeOut เป็น NULL) และเก่ากว่า threshold
        cur.execute(
            """
            SELECT [Id], [EmpId], [DateTimeStamp], [TimeIn]
            FROM [EmpBook_db].[dbo].[TimeAttandanceLog] WITH (NOLOCK)
            WHERE [TimeOut] IS NULL
              AND (
                ([TimeIn] IS NOT NULL AND [TimeIn] < ?)
                OR
                ([TimeIn] IS NULL AND [DateTimeStamp] < ?)
              )
            """,
            threshold_time, threshold_time
        )
        records = cur.fetchall()

        if not records:
            logger.debug("No incomplete records found older than %d hours.", CLEANUP_THRESHOLD_HOURS)
            return 0

        logger.info("Found %d incomplete records older than %d hours.", len(records), CLEANUP_THRESHOLD_HOURS)

        for row in records:
            r_id, emp_id, dt_stamp, t_in = row
            ref_time = t_in if t_in else dt_stamp

            # ค่า Default สำหรับ TimeOut คือเวลาเดียวกับ TimeIn
            auto_timeout = ref_time

            # พยายามหาแผนงาน (Shift) เพื่อดึงเวลาเลิกงานจริงมาใส่
            try:
                dt_period = dt_stamp.date() if hasattr(dt_stamp, "date") else dt_stamp
                cur.execute(
                    """
                    SELECT TOP 1 [OutTmp]
                    FROM [db_pfpdashboard].[dbo].[VListPeriodEmployee] WITH (NOLOCK)
                    WHERE [EmpId] = ? AND [DatePeriod] = ?
                    """,
                    emp_id, dt_period
                )
                shift_row = cur.fetchone()
                if shift_row and shift_row[0]:
                    t_str = str(shift_row[0])[:5]  # "HH:mm"
                    out_time = datetime.strptime(t_str, "%H:%M").time()
                    base_date = dt_stamp.date() if hasattr(dt_stamp, "date") else dt_stamp
                    auto_timeout = datetime.combine(base_date, out_time)

                    # กรณีข้ามวัน
                    if t_in and auto_timeout <= t_in:
                        auto_timeout += timedelta(days=1)
            except Exception as e:
                logger.debug("Shift lookup failed for record %s: %s", r_id, e)

            # อัปเดตข้อมูล
            cur.execute(
                """
                UPDATE [EmpBook_db].[dbo].[TimeAttandanceLog]
                SET [TimeOut] = ?, [IPStampOut] = 'AUTO_CLEANUP'
                WHERE [Id] = ?
                """,
                auto_timeout, r_id
            )
            db.commit()
            cleaned_count += 1
            logger.info(
                "Cleaned record ID %s (Emp: %s, TimeIn: %s) -> TimeOut: %s",
                r_id, emp_id, t_in, auto_timeout
            )

    except Exception as e:
        logger.error("Cleanup failed: %s", e)
        try:
            db.rollback()
        except:
            pass

    return cleaned_count


def run_cleanup_loop():
    """Main loop สำหรับรัน cleanup เป็นระยะๆ"""
    global STOP_FLAG

    logger.info(
        "Cleanup service started. Interval: %d seconds, Threshold: %d hours",
        CLEANUP_INTERVAL_SECONDS, CLEANUP_THRESHOLD_HOURS
    )

    while not STOP_FLAG:
        db_conn = _open_sql_connection()
        if db_conn:
            try:
                cleaned = cleanup_incomplete_records(db_conn)
                logger.info("Cleanup cycle completed. Records cleaned: %d", cleaned)
            finally:
                db_conn.close()
        else:
            logger.warning("Skipped cleanup cycle due to DB connection failure.")

        # รอจนถึงรอบถัดไป (เช็ค STOP_FLAG ทุก 10 วินาที)
        wait_cycles = CLEANUP_INTERVAL_SECONDS // 10
        for _ in range(wait_cycles):
            if STOP_FLAG:
                break
            time.sleep(10)

    logger.info("Cleanup service stopped.")


def signal_handler(signum, frame):
    """จัดการ Signal สำหรับ graceful shutdown"""
    global STOP_FLAG
    logger.info("Signal %s received. Shutting down...", signum)
    STOP_FLAG = True


def main():
    """Entry point สำหรับ Cleanup Service"""
    # ตั้งค่า signal handlers
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, signal_handler)
        except Exception:
            pass

    # รัน cleanup loop
    run_cleanup_loop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
