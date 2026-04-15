"""Redshift database connection module."""
import os
from contextlib import contextmanager
from typing import Generator

import redshift_connector
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


def get_connection_config() -> dict:
    """Get Redshift connection configuration from environment variables.

    Returns:
        dict: Connection parameters for redshift_connector.connect()

    Raises:
        ValueError: If required environment variables are missing
    """
    required_vars = ["REDSHIFT_HOST", "REDSHIFT_USER"]
    missing = [var for var in required_vars if not os.getenv(var)]

    # Suporta REDSHIFT_DATABASE ou REDSHIFT_NAME
    database = os.getenv("REDSHIFT_DATABASE") or os.getenv("REDSHIFT_NAME")
    if not database:
        missing.append("REDSHIFT_DATABASE (or REDSHIFT_NAME)")

    # Suporta REDSHIFT_PASSWORD ou REDSHIFT_PASS
    password = os.getenv("REDSHIFT_PASSWORD") or os.getenv("REDSHIFT_PASS")
    if not password:
        missing.append("REDSHIFT_PASSWORD (or REDSHIFT_PASS)")

    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")

    return {
        "host": os.getenv("REDSHIFT_HOST"),
        "port": int(os.getenv("REDSHIFT_PORT", "5439")),
        "database": database,
        "user": os.getenv("REDSHIFT_USER"),
        "password": password,
    }


@contextmanager
def get_connection() -> Generator[redshift_connector.Connection, None, None]:
    """Get a Redshift database connection as a context manager.

    Yields:
        redshift_connector.Connection: Active database connection

    Example:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
    """
    config = get_connection_config()
    conn = redshift_connector.connect(**config)
    try:
        yield conn
    finally:
        conn.close()


def test_connection() -> dict:
    """Test the Redshift connection and return status.

    Returns:
        dict: Connection status with keys:
            - connected (bool): True if connection successful
            - message (str): Status message
            - host (str): Redshift host (masked for security)
    """
    try:
        config = get_connection_config()
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()

            if result and result[0] == 1:
                host = config["host"]
                masked_host = host[:10] + "..." if len(host) > 10 else host

                return {
                    "connected": True,
                    "message": "Redshift connection successful",
                    "host": masked_host,
                }
    except ValueError as e:
        return {
            "connected": False,
            "message": f"Configuration error: {str(e)}",
            "host": None,
        }
    except Exception as e:
        return {
            "connected": False,
            "message": f"Connection failed: {str(e)}",
            "host": None,
        }

    return {
        "connected": False,
        "message": "Unknown error",
        "host": None,
    }
