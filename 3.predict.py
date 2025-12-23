import pandas as pd
import joblib

model = joblib.load("model/model.pkl")
features = joblib.load("model/features.pkl")

# Ví dụ 1 bài đăng tương lai
future_post = {
    "num_likes": 200,
    "num_comments": 30,
    "num_shares": 15
}

X_future = pd.DataFrame([future_post])[features]

prediction = model.predict(X_future)[0]

print("Predicted future engagement:", prediction)
