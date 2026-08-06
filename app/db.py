"""Database configuration and cached connection lifecycle for the dashboard."""

from __future__ import annotations

import os

import psycopg2
import streamlit as st
from dotenv import load_dotenv


load_dotenv()


def _database_config(prefix: str) -> dict[str, str | None]:
    """Return a psycopg2-compatible configuration from the project environment."""
    return {
        "host": os.getenv(f"{prefix}_DB_HOST"),
        "port": os.getenv(f"{prefix}_DB_PORT"),
        "dbname": os.getenv(f"{prefix}_DB_NAME"),
        "user": os.getenv(f"{prefix}_DB_USER"),
        "password": os.getenv(f"{prefix}_DB_PASSWORD"),
        "connect_timeout": 5,
    }


@st.cache_resource(show_spinner=False)
def get_warehouse_connection():
    """Create and retain the Gold warehouse connection for this Streamlit process."""
    return psycopg2.connect(**_database_config("DEST"))


@st.cache_resource(show_spinner=False)
def get_oltp_connection():
    """Create and retain the Silver OLTP connection for patient-detail lookups."""
    return psycopg2.connect(**_database_config("SRC"))


def clear_connection_cache() -> None:
    """Forget cached connections after a connection error or configuration change."""
    get_warehouse_connection.clear()
    get_oltp_connection.clear()
