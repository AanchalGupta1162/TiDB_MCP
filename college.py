import mysql.connector
import logging
from mcp.server.fastmcp import FastMCP
from dotenv import load_dotenv
import os 

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


load_dotenv() 


TIDB_CONFIG = {
    'user': os.environ.get('TIDB_USER'),
    'password': os.environ.get('TIDB_PASSWORD'),
    'host': os.environ.get('TIDB_HOST'),
    'port': int(os.environ.get('TIDB_PORT', 4000)), 
    'database': os.environ.get('TIDB_DATABASE')
}

if not all([TIDB_CONFIG['user'], TIDB_CONFIG['password'], TIDB_CONFIG['host'], TIDB_CONFIG['database']]):
    raise ValueError("Missing one or more required database environment variables (TIDB_USER, TIDB_PASSWORD, TIDB_HOST, TIDB_DATABASE)")

mcp = FastMCP("Academic Calendar Service")


def get_db_connection():
    """Establishes a connection to the TiDB database."""
    try:
        conn = mysql.connector.connect(**TIDB_CONFIG)
        log.info("Database connection successful.")
        return conn
    except mysql.connector.Error as err:
        log.error(f"Error connecting to TiDB: {err}")
        raise ConnectionError(f"Failed to connect to the database: {err}") from err


@mcp.tool()
def get_events_in_duration(start_date: str, end_date: str) -> str:
    """
    Retrieves all events within a specific date range from the TiDB database.
    Args:
        start_date: The start date of the period, in 'YYYY-MM-DD' format.
        end_date: The end date of the period, in 'YYYY-MM-DD' format.
    """
    log.info(f"Executing get_events_in_duration for dates {start_date} to {end_date}")
    try:
        with get_db_connection() as conn:
            with conn.cursor(dictionary=True) as cursor:
                query = "SELECT * FROM events WHERE event_date BETWEEN %s AND %s ORDER BY event_date, `start_time`"
                cursor.execute(query, (start_date, end_date))
                events = cursor.fetchall()
        
                for event in events:
                    for key, value in event.items():
                        event[key] = str(value)
                
                if not events:
                    return f"No events found between {start_date} and {end_date}."
                return str(events)
    except Exception as e:
        log.error(f"An error occurred in get_events_in_duration: {e}")
        return f"An internal error occurred: {e}"

@mcp.tool()
def get_events_by_type(event_type: str) -> str:
    """
    Retrieves all events of a specific type from the TiDB database.
    Args:
        event_type: The type of the event to retrieve (e.g., 'exam', 'lecture').
    """
    log.info(f"Executing get_events_by_type for type '{event_type}'")
    try:
        with get_db_connection() as conn:
            with conn.cursor(dictionary=True) as cursor:
                query = "SELECT * FROM events WHERE LOWER(`event_type`) = LOWER(%s)"
                cursor.execute(query, (event_type,))
                events = cursor.fetchall()

                for event in events:
                    for key, value in event.items():
                        event[key] = str(value)

                if not events:
                    return f"No events found of type '{event_type}'."
                return str(events)
    except Exception as e:
        log.error(f"An error occurred in get_events_by_type: {e}")
        return f"An internal error occurred: {e}"

if __name__ == "__main__":
    log.info("Starting FastMCP server...")
    mcp.run()