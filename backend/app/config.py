"""Runtime configuration and demo seed data."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = ROOT_DIR / "data"


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.environ.get(name, default))
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, default))
    except ValueError:
        return default


def _env_bool(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() not in {"0", "false", "no", "off"}


@dataclass
class Settings:
    db_path: str = str(DATA_DIR / "inbox.db")
    signature_tolerance_seconds: int = 300
    dispatcher_enabled: bool = True
    worker_interval_seconds: float = 0.15
    claim_batch_size: int = 100
    max_concurrency: int = 16
    default_timeout_seconds: float = 2.0
    default_max_attempts: int = 5
    default_retry_base_seconds: float = 0.4
    default_retry_max_seconds: float = 8.0
    # Local demo secrets. Real deployments would read these from a secret store.
    source_secrets: dict[str, str] = field(
        default_factory=lambda: {
            "shop": os.environ.get("SOURCE_SHOP_SECRET", "shop-demo-secret-2026"),
            "crm": os.environ.get("SOURCE_CRM_SECRET", "crm-demo-secret-2026"),
        }
    )

    @classmethod
    def from_env(cls) -> "Settings":
        secrets = {
            "shop": os.environ.get("SOURCE_SHOP_SECRET", "shop-demo-secret-2026"),
            "crm": os.environ.get("SOURCE_CRM_SECRET", "crm-demo-secret-2026"),
        }
        return cls(
            db_path=os.environ.get("WEBHOOK_DB_PATH", str(DATA_DIR / "inbox.db")),
            signature_tolerance_seconds=_env_int("WEBHOOK_SIGNATURE_TOLERANCE", 300),
            dispatcher_enabled=_env_bool("WEBHOOK_DISPATCHER_ENABLED", True),
            worker_interval_seconds=_env_float("WEBHOOK_WORKER_INTERVAL", 0.15),
            claim_batch_size=_env_int("WEBHOOK_CLAIM_BATCH", 100),
            max_concurrency=_env_int("WEBHOOK_MAX_CONCURRENCY", 16),
            default_timeout_seconds=_env_float("WEBHOOK_HTTP_TIMEOUT", 2.0),
            default_max_attempts=_env_int("WEBHOOK_MAX_ATTEMPTS", 5),
            default_retry_base_seconds=_env_float("WEBHOOK_RETRY_BASE", 0.4),
            default_retry_max_seconds=_env_float("WEBHOOK_RETRY_MAX", 8.0),
            source_secrets=secrets,
        )


# Demo sources and subscriptions. Created only when the database is empty so that
# operator changes in the UI / SQL survive normal restarts.
DEFAULT_SOURCES = [
    ("shop", "Shop 商城", "shop-demo-secret-2026"),
    ("crm", "CRM 客户系统", "crm-demo-secret-2026"),
]

DEFAULT_SUBSCRIPTIONS = [
    {
        "id": "shop-fast",
        "name": "receiver-fast",
        "source": "shop",
        "event_type": "*",
        "callback_url": "http://127.0.0.1:9101/hooks/orders",
        "partition_key_field": "object_id",
        "timeout_seconds": 2.0,
        "max_attempts": 5,
    },
    {
        "id": "shop-chaos",
        "name": "receiver-chaos",
        "source": "shop",
        "event_type": "*",
        "callback_url": "http://127.0.0.1:9102/hooks/orders",
        "partition_key_field": "object_id",
        "timeout_seconds": 2.0,
        "max_attempts": 5,
    },
    {
        "id": "crm-fast",
        "name": "receiver-fast",
        "source": "crm",
        "event_type": "*",
        "callback_url": "http://127.0.0.1:9101/hooks/orders",
        "partition_key_field": "object_id",
        "timeout_seconds": 2.0,
        "max_attempts": 5,
    },
]
