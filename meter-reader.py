"""
Reads utility meter transmissions from rtlamr and stores RAW cumulative readings
in a DuckDB database at a configured interval.

Key points:
- Writes to DuckDB (ts, meter_id, commodity, consumption).
- Stores raw cumulative readings (no rate-of-change).
- Graceful shutdown: terminate only processes started by this script (process groups).
- Defaults DB + logs into ./data/ next to this script.
- Log rotation via TimedRotatingFileHandler (customizable in settings.py).

All overridable settings live in settings.py
"""

import subprocess
import signal
import sys
import time
import logging
import os
import duckdb
from datetime import datetime, timezone
from logging.handlers import TimedRotatingFileHandler

# The settings file is installed in the FAT boot partition of the Pi SD card
sys.path.insert(0, '/boot/pi_logger')
import settings  # noqa: E402

# ---------------- Paths / Defaults next to this script ----------------
BASE_DIR = os.path.dirname(os.path.realpath(__file__))
DEFAULT_DATA_DIR = os.path.join(BASE_DIR, 'data')

DATA_DIR = getattr(settings, 'DATA_DIR', DEFAULT_DATA_DIR)
DEFAULT_LOG_PATH = os.path.join(DATA_DIR, 'meter_reader.log')
DEFAULT_DB_PATH = os.path.join(DATA_DIR, 'meter_reader.duckdb')

LOG_PATH = getattr(settings, 'LOG_PATH', DEFAULT_LOG_PATH)
DB_PATH = getattr(settings, 'DUCKDB_PATH', DEFAULT_DB_PATH)
TABLE_NAME = getattr(settings, 'TABLE_NAME', 'meter_readings')

# If True, this script will start/stop rtl_tcp itself using safe process-group handling.
START_RTL_TCP = getattr(settings, 'START_RTL_TCP', False)

# rtl_tcp command and args (used only if START_RTL_TCP == True)
RTL_TCP_CMD = getattr(settings, 'RTL_TCP_CMD', '/usr/bin/rtl_tcp')

# rtlamr command and args
RTLAMR_CMD = getattr(settings, 'RTLAMR_CMD', '/home/pi/gocode/bin/rtlamr')
RTLAMR_ARGS = getattr(settings, 'RTLAMR_ARGS', [
    '-gainbyindex=24',  # index 24 was found to be the most sensitive
    '-format=csv'
])

# Commodity mapping (AMR commodity type -> label)
commod_map = {
    2: 'Gas',
    4: 'Elec',
    5: 'Elec',
    7: 'Elec',
    8: 'Elec',
    9: 'Gas',
    11: 'Water',
    12: 'Gas',
    13: 'Water',
}

# Filter by IDs (optional) and interval
METER_IDS = getattr(settings, 'METER_IDS', [])
POST_INTERVAL_SEC = float(getattr(settings, 'METER_POST_INTERVAL', 5)) * 60.0  # minutes -> seconds

# ---------------- Logging (with rotation) ----------------
def setup_logging(log_path: str):
    # Ensure the data/log directory exists
    try:
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
    except Exception:
        pass  # fallback to STDERR if we can't create the directory

    logger = logging.getLogger()
    logger.setLevel(getattr(settings, 'LOG_LEVEL', logging.INFO))

    fmt = logging.Formatter(
        fmt='%(asctime)s %(levelname)s [%(process)d] %(name)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Rotation controls (customizable in settings.py)
    rotate_when = getattr(settings, 'LOG_ROTATE_WHEN', 'midnight')  # 'S','M','H','D','W0'-'W6','midnight'
    rotate_interval = int(getattr(settings, 'LOG_ROTATE_INTERVAL', 1))  # how many units of 'when'
    backup_count = int(getattr(settings, 'LOG_BACKUP_COUNT', 14))  # how many rotated files to keep
    use_utc = bool(getattr(settings, 'LOG_UTC', True))

    try:
        fh = TimedRotatingFileHandler(
            log_path,
            when=rotate_when,
            interval=rotate_interval,
            backupCount=backup_count,
            utc=use_utc,
            encoding='utf-8',
        )
        fh.setFormatter(fmt)
        logger.addHandler(fh)
        logger.info('Logging initialized: %s (when=%s, interval=%s, backups=%s, utc=%s)',
                    log_path, rotate_when, rotate_interval, backup_count, use_utc)
    except Exception as e:
        sh = logging.StreamHandler(sys.stderr)
        sh.setFormatter(fmt)
        logger.addHandler(sh)
        logger.warning('Could not open %s (%s). Falling back to STDERR.', log_path, e)

setup_logging(LOG_PATH)
logging.warning('meter_reader has restarted')

# ---------------- State & Helpers ----------------
procs = {
    'rtl_tcp': None,   # subprocess.Popen or None
    'rtlamr': None
}

# Dictionary keyed on Meter ID that holds the last reading timestamp/value used for interval gating
last_reads = {}  # meter_id -> (ts_last, read_last)

def get_last(meter_id):
    return last_reads.get(meter_id, (None, None))

def set_last(meter_id, ts, val):
    last_reads[meter_id] = (ts, val)

def _start_process(cmd_with_args):
    """
    Start a child process in its own process group so we can send signals to the group.
    """
    return subprocess.Popen(
        cmd_with_args,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE if 'rtlamr' in cmd_with_args[0] else None,
        text=True,
        preexec_fn=os.setsid  # start new session -> new process group
    )

def _terminate_process_group(p, name, timeout=5.0):
    """
    Gracefully terminate a subprocess started with preexec_fn=os.setsid.
    Sends SIGTERM to the process group, waits, then SIGKILL if needed.
    """
    if p is None:
        return
    try:
        pgid = os.getpgid(p.pid)
    except Exception:
        pgid = None

    try:
        if pgid is not None:
            logging.info('Terminating %s (pgid %s) with SIGTERM...', name, pgid)
            os.killpg(pgid, signal.SIGTERM)
        else:
            logging.info('Terminating %s (pid %s) with SIGTERM...', name, p.pid)
            p.terminate()
    except ProcessLookupError:
        return

    try:
        p.wait(timeout=timeout)
        logging.info('%s exited cleanly.', name)
    except subprocess.TimeoutExpired:
        logging.warning('%s did not exit in %ss; sending SIGKILL...', name, timeout)
        try:
            if pgid is not None:
                os.killpg(pgid, signal.SIGKILL)
            else:
                p.kill()
        except ProcessLookupError:
            pass
        p.wait(timeout=2)

def graceful_shutdown(signum=None, frame=None):
    logging.info('Shutting down meter_reader...')
    _terminate_process_group(procs.get('rtlamr'), 'rtlamr')
    if START_RTL_TCP:
        _terminate_process_group(procs.get('rtl_tcp'), 'rtl_tcp')
    logging.info('Shutdown complete.')
    if signum is not None:
        sys.exit(0)

# If process is being killed, go through shutdown process
signal.signal(signal.SIGTERM, graceful_shutdown)
signal.signal(signal.SIGINT, graceful_shutdown)

# ---------------- DuckDB Setup ----------------
# Ensure DB directory exists
db_dir = os.path.dirname(DB_PATH)
if db_dir and not os.path.isdir(db_dir):
    os.makedirs(db_dir, exist_ok=True)

con = duckdb.connect(DB_PATH)
con.execute(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        ts TIMESTAMP,          -- UTC timestamp when reading recorded
        meter_id INT,
        commodity TEXT,
        consumption INT
    )
""")
#try:
#    con.execute(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_meter_ts ON {TABLE_NAME}(meter_id, ts)")
#except Exception:
#    pass

# ---------------- Optional rtl_tcp Startup ----------------
if START_RTL_TCP:
    try:
        procs['rtl_tcp'] = _start_process([RTL_TCP_CMD])
        logging.info('Started rtl_tcp under script management.')
        time.sleep(5.0)  # allow TCP server to come up
    except Exception:
        logging.exception('Failed to start rtl_tcp.')
        graceful_shutdown()

# ---------------- Start rtlamr ----------------
try:
    procs['rtlamr'] = _start_process([RTLAMR_CMD] + RTLAMR_ARGS)
    logging.info('Started rtlamr.')
except Exception:
    logging.exception('Failed to start rtlamr.')
    graceful_shutdown()

# ---------------- Main Loop ----------------
ts_last_checkpoint = 0      # tracks when last flush of the .wal occurred in duckdb
while True:
    line = ''
    try:
        line = procs['rtlamr'].stdout.readline()
        if not line:
            time.sleep(0.5)
            raise RuntimeError('rtlamr output ended unexpectedly.')

        line = line.strip()
        flds = line.split(',')

        if len(flds) != 9:
            # valid readings have nine fields
            continue

        meter_id = int(flds[3])
        if METER_IDS and meter_id not in METER_IDS:
            continue

        ts_cur = time.time()
        read_cur = int(flds[7])

        commod_type_num = int(flds[4])
        commodity = commod_map.get(commod_type_num, 'Elec')

        logging.debug('%s %s %s %s', ts_cur, meter_id, read_cur, commod_type_num)

        ts_last, _read_last = get_last(meter_id)
        print(meter_id, commodity, read_cur)
        if ts_last is None:
            set_last(meter_id, ts_cur, read_cur)
            con.execute(
                f"INSERT INTO {TABLE_NAME} (ts, meter_id, commodity, consumption) VALUES (?, ?, ?, ?)",
                (datetime.fromtimestamp(ts_cur, tz=timezone.utc), meter_id, commodity, read_cur)
            )
            con.commit()
            logging.info('First stored reading for Meter #%s: %s (%s)', meter_id, read_cur, commodity)
            continue

        if ts_cur > ts_last + POST_INTERVAL_SEC:
            ts_store = datetime.fromtimestamp(ts_cur, tz=timezone.utc)
            con.execute(
                f"INSERT INTO {TABLE_NAME} (ts, meter_id, commodity, consumption) VALUES (?, ?, ?, ?)",
                (ts_store, meter_id, commodity, read_cur)
            )
            con.commit()
            logging.debug('DuckDB insert: %s %s %s %s', ts_store.isoformat(), meter_id, commodity, read_cur)
            set_last(meter_id, ts_cur, read_cur)

        if ts_cur > ts_last_checkpoint + 60:
            # flush .wal every minute
            con.execute("CHECKPOINT")
            ts_last_checkpoint = ts_cur

    except KeyboardInterrupt:
        graceful_shutdown()
    except Exception:
        logging.exception('Error processing reading %s', line)
        time.sleep(2)
