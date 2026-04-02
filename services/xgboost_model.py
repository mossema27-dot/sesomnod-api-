"""
XGBoost match outcome model with SHAP explainability.
Model is stored in PostgreSQL (survives Railway redeploys).
"""
import asyncio
import logging
from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import shap

from services.xgb_training import build_training_dataset, FEATURE_COLS, FEATURE_LABELS
from services.football_data_fetcher import get_historical_data
from services.model_storage import save_model_to_db, load_model_from_db, model_exists_in_db

logger = logging.getLogger("sesomnod.xgboost")

MODEL_NAME = "sesomnod_xgb_v1"


@dataclass
class XGBPrediction:
    """Result from XGBoost model prediction."""
    home_win_prob: float
    draw_prob: float
    away_win_prob: float
    predicted_outcome: str
    confidence: float
    shap_top3: list = field(default_factory=list)
    model_available: bool = False
    features_used: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        """Convert to JSON-serializable dict."""
        return {
            "home_win_prob": self.home_win_prob,
            "draw_prob": self.draw_prob,
            "away_win_prob": self.away_win_prob,
            "predicted_outcome": self.predicted_outcome,
            "confidence": self.confidence,
            "model_available": self.model_available,
            "shap_top3": self.shap_top3,
        }


_model: Optional[xgb.XGBClassifier] = None
_explainer = None
_model_loaded: bool = False
_model_lock = asyncio.Lock()


async def ensure_model_loaded(db_pool) -> bool:
    """
    Ensure XGBoost model is loaded from DB or trained fresh.
    Thread-safe via asyncio.Lock. Runs CPU work in thread pool.
    """
    global _model, _explainer, _model_loaded

    if _model_loaded and _model is not None:
        return True

    async with _model_lock:
        if _model_loaded and _model is not None:
            return True

        try:
            async with db_pool.acquire() as conn:
                # Try loading from DB first
                if await model_exists_in_db(conn, MODEL_NAME):
                    loaded = await load_model_from_db(conn, MODEL_NAME)
                    if loaded is not None:
                        _model = loaded
                        _explainer = shap.TreeExplainer(_model)
                        _model_loaded = True
                        logger.info("XGBoost model loaded from DB.")
                        return True

                # Train new model in thread pool
                logger.info("Training XGBoost on football-data.co.uk data...")
                model, acc, n_samples = await asyncio.to_thread(_train_sync)

                await save_model_to_db(conn, MODEL_NAME, model, acc, n_samples)

                _model = model
                _explainer = shap.TreeExplainer(_model)
                _model_loaded = True
                logger.info("XGBoost model trained and saved (acc=%.3f).", acc)
                return True

        except Exception as e:
            logger.error("XGBoost load/train failed: %s", e, exc_info=True)
            return False


def _train_sync() -> tuple:
    """Synchronous training. Returns (model, accuracy, n_samples)."""
    df = get_historical_data()
    X, y = build_training_dataset(df)

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y,
    )

    model = xgb.XGBClassifier(
        n_estimators=300,
        max_depth=4,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        eval_metric="mlogloss",
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)

    acc = accuracy_score(y_test, model.predict(X_test))
    logger.info("XGBoost accuracy: %.3f on %d test samples", acc, len(X_test))
    return model, acc, len(X)


def predict_match(
    home_attack_str: float = 1.2,
    home_defense_weakness: float = 1.0,
    away_attack_str: float = 1.0,
    away_defense_weakness: float = 1.2,
    home_form: float = 0.5,
    away_form: float = 0.5,
    h2h_home_winrate: float = 0.5,
) -> XGBPrediction:
    """
    Predict match outcome with XGBoost.
    All parameters have safe defaults.
    Never raises — always returns XGBPrediction.
    """
    features_dict = {
        "home_attack_str": home_attack_str,
        "home_defense_weakness": home_defense_weakness,
        "away_attack_str": away_attack_str,
        "away_defense_weakness": away_defense_weakness,
        "home_form": home_form,
        "away_form": away_form,
        "h2h_home_winrate": h2h_home_winrate,
    }

    if not _model_loaded or _model is None:
        # Fallback heuristic when model not yet loaded
        hw = min(0.70, max(0.15,
            0.35 + (home_attack_str - away_defense_weakness) * 0.1
            + (home_form - away_form) * 0.15
        ))
        aw = min(0.65, max(0.10,
            0.28 + (away_attack_str - home_defense_weakness) * 0.1
            + (away_form - home_form) * 0.10
        ))
        d = max(0.05, 1.0 - hw - aw)
        total = hw + d + aw
        return XGBPrediction(
            home_win_prob=round(hw / total, 4),
            draw_prob=round(d / total, 4),
            away_win_prob=round(aw / total, 4),
            predicted_outcome="HOME_WIN" if hw > aw else "AWAY_WIN",
            confidence=round(max(hw, d, aw) / total, 4),
            shap_top3=[],
            model_available=False,
            features_used=features_dict,
        )

    try:
        X = pd.DataFrame([features_dict])[FEATURE_COLS]
        proba = _model.predict_proba(X)[0]
        # Classes: 0=away win, 1=draw, 2=home win
        away_prob = float(proba[0])
        draw_prob = float(proba[1])
        home_prob = float(proba[2])

        predicted_idx = int(np.argmax(proba))
        outcome_map = {0: "AWAY_WIN", 1: "DRAW", 2: "HOME_WIN"}

        # SHAP explainability
        shap_top3: list[dict] = []
        try:
            shap_values = _explainer.shap_values(X)
            # Handle different SHAP output formats:
            # List of arrays (one per class) → shap_values[class_idx][sample_idx]
            # 3D array (n_samples, n_features, n_classes) → shap_values[0, :, class_idx]
            if isinstance(shap_values, list):
                sv = np.array(shap_values[predicted_idx]).flatten()[:len(FEATURE_COLS)]
            elif hasattr(shap_values, 'ndim') and shap_values.ndim == 3:
                sv = shap_values[0, :, predicted_idx]
            else:
                sv = np.array(shap_values).flatten()[:len(FEATURE_COLS)]
            feature_shap = list(zip(FEATURE_COLS, sv))
            feature_shap.sort(key=lambda x: abs(x[1]), reverse=True)
            shap_top3 = [
                {
                    "feature": f,
                    "label": FEATURE_LABELS.get(f, f),
                    "shap_value": round(float(v), 4),
                    "direction": "positiv" if v > 0 else "negativ",
                    "value": round(float(features_dict.get(f, 0)), 3),
                }
                for f, v in feature_shap[:3]
            ]
            logger.info("SHAP top3: %s", [(s['label'], s['shap_value']) for s in shap_top3])
        except Exception as shap_err:
            logger.warning("SHAP failed: %s", shap_err, exc_info=True)

        return XGBPrediction(
            home_win_prob=round(home_prob, 4),
            draw_prob=round(draw_prob, 4),
            away_win_prob=round(away_prob, 4),
            predicted_outcome=outcome_map[predicted_idx],
            confidence=round(float(proba[predicted_idx]), 4),
            shap_top3=shap_top3,
            model_available=True,
            features_used=features_dict,
        )

    except Exception as e:
        logger.error("XGBoost predict failed: %s", e, exc_info=True)
        return XGBPrediction(
            home_win_prob=0.45,
            draw_prob=0.27,
            away_win_prob=0.28,
            predicted_outcome="HOME_WIN",
            confidence=0.45,
            shap_top3=[],
            model_available=False,
            features_used=features_dict,
        )
