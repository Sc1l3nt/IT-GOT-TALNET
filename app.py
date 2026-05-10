import streamlit as st
import pandas as pd
import joblib

# ===== Load model =====
model = joblib.load("model/model.pkl")
features = joblib.load("model/features.pkl")

st.title("📊 Engagement Prediction App")

# ===== User input =====
status_type = st.selectbox(
    "Status type",
    ["status", "photo", "video", "link"]
)

time_frame = st.selectbox(
    "Time frame",
    ["morning", "afternoon", "evening", "night"]
)

num_likes = st.number_input("Expected likes", min_value=0)
num_comments = st.number_input("Expected comments", min_value=0)
num_shares = st.number_input("Expected shares", min_value=0)

# ===== Mapping =====
STATUS_TYPE_MAP = {
    "status": 0,
    "photo": 1,
    "video": 2,
    "link": 3
}

TIME_FRAME_MAP = {
    "morning": 0,
    "afternoon": 1,
    "evening": 2,
    "night": 3
}

if st.button("Predict engagement"):
    input_data = {
        "num_likes": num_likes,
        "num_comments": num_comments,
        "num_shares": num_shares
    }

    X_input = pd.DataFrame([input_data])[features]

    prediction = model.predict(X_input)[0]

    st.success(f"📈 Predicted engagement score: {prediction:.2f}")
