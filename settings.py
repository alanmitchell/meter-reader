# ---- Paths (all optional) ----
# Directory for DB + logs (defaults to a "data" folder next to the script)
# DATA_DIR = "/home/pi/pi_logger/data"

# Explicit log and DB paths (optional; otherwise derived from DATA_DIR)
# LOG_PATH = "/home/pi/pi_logger/data/meter_reader.log"
# DUCKDB_PATH = "/home/pi/pi_logger/data/meter_reader.duckdb"

# DuckDB table name (optional)
# TABLE_NAME = "meter_readings"

# ---- rtlamr / rtl_tcp control ----
# If True, script will start/stop rtl_tcp itself.
START_RTL_TCP = True

# RTL_TCP_CMD = "/usr/bin/rtl_tcp"

# Path to rtlamr and arguments
RTLAMR_CMD = "/home/alan/go/bin/rtlamr"
RTLAMR_ARGS = ["-gainbyindex=24", "-format=csv"]

# ---- Filtering and interval ----
# Only record these meter IDs; leave empty to accept all
# METER_IDS = [12345678, 23456789]
# Minutes between stored readings per meter
METER_POST_INTERVAL = 7

# ---- Logging options ----
# LOG_LEVEL can be logging.DEBUG, logging.INFO, etc. (use a string or import logging)
# If left unset, defaults to logging.INFO from the script.
# Example using string:
# LOG_LEVEL = 20  # INFO

# Timed rotation parameters:
# LOG_ROTATE_WHEN: 'S','M','H','D','W0'-'W6','midnight'  (default: 'midnight')
# LOG_ROTATE_INTERVAL: how many of 'when' units between rotations (default: 5)
# LOG_BACKUP_COUNT: how many rotated files to keep (default: 14)
# LOG_UTC: rotate based on UTC (default: True)

# LOG_ROTATE_WHEN = "midnight"
LOG_ROTATE_INTERVAL = 5
# LOG_BACKUP_COUNT = 14
LOG_UTC = False
