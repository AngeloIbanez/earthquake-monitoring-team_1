"""
Configuration manager for Natural Hazard Monitoring System.
Loads YAML configurations and system environment variables.
"""

import os
from pathlib import Path
import yaml
from typing import Dict, Any

DEFAULT_CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "sources.yaml"

def load_sources_config(config_path: Path = DEFAULT_CONFIG_PATH) -> Dict[str, Any]:
    """Loads and validates the sources configuration YAML file."""
    if not config_path.exists():
        raise FileNotFoundError(f"Sources config file not found at: {config_path}")
    
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    return config

def get_db_connection_params() -> Dict[str, Any]:
    """Retrieves PostgreSQL connection parameters from environment variables."""
    return {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "database": os.getenv("POSTGRES_DB", "earthquake_monitoring"),
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", "postgres")
    }
