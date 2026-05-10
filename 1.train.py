import pandas as pd
import numpy as np
from sklearn.model_selection import KFold
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import r2_score, mean_squared_error
import joblib
import os

# ===== Load data =====
df = pd.read_csv("data/facebook_cleaned.csv")

FEATURES = ["num_likes", "num_comments", "num_shares"]
X = df[FEATURES]
y = df["engagement_score"]

# ===== 10-fold split =====
kf = KFold(n_splits=10, shuffle=True, random_state=42)

r2_scores = []
rmse_scores = []

for train_idx, test_idx in kf.split(X):
    X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
    y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]

    model = RandomForestRegressor(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)

    r2_scores.append(r2_score(y_test, y_pred))
    rmse_scores.append(mean_squared_error(y_test, y_pred) ** 0.5)

# ===== Final result =====
print("Average R2 (Test):", np.mean(r2_scores))
print("Average RMSE:", np.mean(rmse_scores))

# ===== Train final model on ALL data =====
final_model = RandomForestRegressor(n_estimators=100, random_state=42)
final_model.fit(X, y)

os.makedirs("model", exist_ok=True)
joblib.dump(final_model, "model/model.pkl")
joblib.dump(FEATURES, "model/features.pkl")
