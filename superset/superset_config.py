"""Small, local-only Superset configuration for the DATA 226 lab."""

import os


SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"

# Dashboard-native city and time filters are used by the lab dashboard.
FEATURE_FLAGS = {
    "DASHBOARD_NATIVE_FILTERS": True,
}

ROW_LIMIT = 5000
