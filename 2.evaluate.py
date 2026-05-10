import pandas as pd
import joblib
from sklearn.metrics import r2_score, mean_squared_error

# Load data & model
df = pd.read_csv("data/facebook_cleaned.csv")
model = joblib.load("model/model.pkl")
features = joblib.load("model/features.pkl")

X = df[features]
y = df["engagement_score"]

# Predict
y_pred = model.predict(X)

# Metrics
r2 = r2_score(y, y_pred)
rmse = mean_squared_error(y, y_pred) ** 0.5

print("=== MODEL EVALUATION ===")
print(f"R2 score : {r2:.4f}")
print(f"RMSE     : {rmse:.2f}")
