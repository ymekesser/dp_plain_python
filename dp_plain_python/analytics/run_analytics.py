import logging
import pandas as pd
import numpy as np
from pathlib import Path
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
import sklearn.metrics as metrics

log = logging.getLogger(__name__)

transformed_analytics_path = "local_data\\transformed_analytics"
feature_set_filename = "feature_set.csv"
analytics_path = "local_data\\analytics"


def run_analytics() -> None:
    df_features = _read_feature_set()

    X = df_features[
        [
            "storey_median",
            "floor_area_sqm",
            "lease_commence_date",
            "remaining_lease_in_months",
            "distance_to_closest_mrt",
            "distance_to_closest_mall",
            "distance_to_cbd",
        ]
    ].to_numpy()
    y = df_features[["resale_price"]].to_numpy()

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, random_state=1337, test_size=0.25
    )

    pipe = Pipeline([("scaler", StandardScaler()), ("linreg", LinearRegression())])

    pipe.fit(X_train, y_train)

    y_train_pred = pipe.predict(X_train)
    y_test_pred = pipe.predict(X_test)
    print(f"R^2 Score on Training:{metrics.r2_score(y_train, y_train_pred)}")
    print(f"R^2 Score on Test:{metrics.r2_score(y_test, y_test_pred)}")

    print()

    print(
        "Mean Absolute Error (MAE):", metrics.mean_absolute_error(y_test, y_test_pred)
    )
    print("Mean Squared Error (MSE):", metrics.mean_squared_error(y_test, y_test_pred))
    print(
        "Root Mean Squared Error (RMSE):",
        np.sqrt(metrics.mean_squared_error(y_test, y_test_pred)),
    )
    mape = np.mean(np.abs((y_test - y_test_pred) / np.abs(y_test)))
    print("Mean Absolute Percentage Error (MAPE):", round(mape * 100, 2))
    print("Accuracy:", round(100 * (1 - mape), 2))

    print()

    importance = pipe.named_steps["linreg"].coef_
    for i, v in enumerate(importance):
        print(f"Feature: {i}, Score: {v}")


def _read_feature_set() -> pd.DataFrame:
    log.info(f"Loading {feature_set_filename} from transformed_analytics for analytics")
    path = Path(transformed_analytics_path) / feature_set_filename

    return pd.read_csv(path)
