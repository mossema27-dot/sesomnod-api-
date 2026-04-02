"""
Model storage in PostgreSQL (bytea). Survives Railway redeploys.
"""
import io
import logging
from typing import Optional

import asyncpg
import joblib

logger = logging.getLogger("sesomnod.model_storage")


async def save_model_to_db(
    conn: asyncpg.Connection,
    model_name: str,
    model_obj: object,
    accuracy: float,
    training_samples: int,
) -> None:
    """Serialize model with joblib and save to PostgreSQL."""
    buffer = io.BytesIO()
    joblib.dump(model_obj, buffer)
    model_bytes = buffer.getvalue()

    await conn.execute("""
        INSERT INTO ml_models
            (model_name, model_data, accuracy, training_samples)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (model_name) DO UPDATE SET
            model_data = EXCLUDED.model_data,
            accuracy = EXCLUDED.accuracy,
            training_samples = EXCLUDED.training_samples,
            created_at = NOW()
    """, model_name, model_bytes, accuracy, training_samples)
    logger.info(
        "Model '%s' saved to DB (%d bytes, acc=%.3f)",
        model_name, len(model_bytes), accuracy,
    )


async def load_model_from_db(
    conn: asyncpg.Connection,
    model_name: str,
) -> Optional[object]:
    """Load and deserialize model from PostgreSQL."""
    row = await conn.fetchrow(
        "SELECT model_data FROM ml_models WHERE model_name = $1",
        model_name,
    )
    if row is None:
        return None
    buffer = io.BytesIO(bytes(row["model_data"]))
    model = joblib.load(buffer)
    logger.info("Model '%s' loaded from DB.", model_name)
    return model


async def model_exists_in_db(
    conn: asyncpg.Connection,
    model_name: str,
) -> bool:
    """Check if model exists in DB."""
    row = await conn.fetchrow(
        "SELECT 1 FROM ml_models WHERE model_name = $1",
        model_name,
    )
    return row is not None
