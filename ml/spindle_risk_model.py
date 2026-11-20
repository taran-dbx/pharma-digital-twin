# MLflow "models from code" definition of the Spindle 3 thermal-risk model.
# Logged verbatim by ml/train_spindle_risk.py; the served endpoint runs it.
#
# Input: one row per scoring instant with the FEATURE_NAMES columns built by
# iot_twin.ml.spindle_features (the app sends them as dataframe_records).
# Output per row: risk_score (probability of an excursion beyond the validated
# limit within the label horizon), top_features (JSON list of the largest
# TreeSHAP contributions {feature, contribution (log-odds), value}), and the
# decision_threshold / clear_threshold chosen at training time (the "decision"
# artifact), so the cut-off travels with the model version.
from __future__ import annotations

import json

import mlflow
import pandas as pd

TOP_K = 3
OUTPUT_COLUMNS = ["risk_score", "top_features", "decision_threshold", "clear_threshold"]


class SpindleRiskModel(mlflow.pyfunc.PythonModel):
    def load_context(self, context):
        import lightgbm as lgb

        self._booster = lgb.Booster(model_file=context.artifacts["booster"])
        self._features = list(self._booster.feature_name())
        with open(context.artifacts["decision"], encoding="utf-8") as handle:
            decision = json.load(handle)
        self._decision = float(decision["decision_threshold"])
        self._clear = float(decision["clear_threshold"])

    def predict(self, context, model_input, params=None):
        frame = pd.DataFrame(model_input)[self._features].astype(float)
        scores = self._booster.predict(frame)
        contributions = self._booster.predict(frame, pred_contrib=True)
        rows = []
        for index in range(len(frame)):
            values = frame.iloc[index]
            ranked = sorted(
                zip(self._features, contributions[index][:-1], values, strict=True),
                key=lambda item: abs(item[1]),
                reverse=True,
            )[:TOP_K]
            top = [
                {
                    "feature": name,
                    "contribution": round(float(contribution), 4),
                    "value": round(float(value), 4),
                }
                for name, contribution, value in ranked
            ]
            rows.append(
                {
                    "risk_score": float(scores[index]),
                    "top_features": json.dumps(top),
                    "decision_threshold": self._decision,
                    "clear_threshold": self._clear,
                }
            )
        return pd.DataFrame(rows, columns=OUTPUT_COLUMNS)


mlflow.models.set_model(SpindleRiskModel())
