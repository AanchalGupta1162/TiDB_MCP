import mysql.connector
import logging
from mcp.server.fastmcp import FastMCP

# --- Configure Logging ---
# This helps you see server activity in the terminal.
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# --- 1. Create the FastMCP Server Instance ---
mcp = FastMCP("Academic Calendar Service")

# --- TiDB Connection Details ---
# IMPORTANT: Replace these with your actual TiDB connection details.
TIDB_CONFIG = {
    'user': 'kJXFUfsnGgrD5x7.root',
    'password': 'vZ0OV7hJ86Zi9gXz',
    'host': 'gateway01.ap-southeast-1.prod.aws.tidbcloud.com',
    'port': 4000,
    'database': 'MCP'
}

def get_db_connection():
    """Establishes a connection to the TiDB database."""
    try:
        conn = mysql.connector.connect(**TIDB_CONFIG)
        log.info("Database connection successful.")
        return conn
    except mysql.connector.Error as err:
        log.error(f"Error connecting to TiDB: {err}")
        raise ConnectionError(f"Failed to connect to the database: {err}") from err

# --- 2. Define Tools with the @mcp.tool() Decorator ---

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
                query = "SELECT * FROM events WHERE event_date BETWEEN %s AND %s ORDER BY event_date, start_time"
                cursor.execute(query, (start_date, end_date))
                events = cursor.fetchall()
        
                # Convert date/time objects to strings for clean output
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
                query = "SELECT * FROM events WHERE LOWER(event_type) = LOWER(%s)"
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

# --- 3. Run the Server ---
async def main():
    """An async main function to run the server."""
    log.info("Starting FastMCP server asynchronously...")
    await mcp.run()

if __name__ == "__main__":
    # This command starts the Python asyncio event loop and runs our
    # main() function. This is a blocking call that will keep the
    # application alive on the server.
    asyncio.run(main())