import pandas as pd

# =========================
# 1. LOAD DATA
# =========================
df = pd.read_csv(
    "/mnt/data/Social Media Engagement Dataset.csv",
    parse_dates=["date"]
)

# =========================
# 2. SORT DATA (BẮT BUỘC)
# Đảm bảo dữ liệu theo đúng thứ tự thời gian
# =========================
df = df.sort_values(["page_id", "date"]).reset_index(drop=True)

# =========================
# 3. DAILY AGGREGATION (nếu dữ liệu chưa theo ngày)
# =========================
daily = df.groupby(["page_id", "date"]).agg({
    "post_count": "sum",
    "reactions": "sum",
    "comments": "sum",
    "shares": "sum",
    "avg_negative_ratio": "mean",
    "positive_sum": "sum",
    "negative_sum": "sum"
}).reset_index()

# =========================
# 4. SLIDING WINDOW 7 DAYS
# =========================
feature_rows = []

for page_id, group in daily.groupby("page_id"):
    group = group.sort_values("date").reset_index(drop=True)

    # window 7 ngày liên tiếp
    for i in range(len(group) - 6):
        window = group.iloc[i:i+7]

        feature_rows.append({
            "page_id": page_id,
            "window_start": window["date"].iloc[0],
            "window_end": window["date"].iloc[-1],

            # Activity
            "total_posts_7d": window["post_count"].sum(),
            "posts_per_day": window["post_count"].mean(),

            # Engagement
            "total_reactions_7d": window["reactions"].sum(),
            "total_comments_7d": window["comments"].sum(),
            "total_shares_7d": window["shares"].sum(),
            "engagement_per_post": (
                window["reactions"].sum() / window["post_count"].sum()
                if window["post_count"].sum() > 0 else 0
            ),

            # Sentiment
            "avg_negative_ratio_7d": window["avg_negative_ratio"].mean(),
            "positive_reactions_7d": window["positive_sum"].sum(),
            "negative_reactions_7d": window["negative_sum"].sum(),

            # Trend (so sánh đầu tuần – cuối tuần)
            "reaction_trend_7d": (
                window["reactions"].iloc[-1] - window["reactions"].iloc[0]
            )
        })

# =========================
# 5. FEATURE TABLE OUTPUT
# =========================
feature_df = pd.DataFrame(feature_rows)

# Xem thử kết quả
print(feature_df.head())
