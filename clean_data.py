import pandas as pd

# =========================
# 1. LOAD & CLEAN DATA
# =========================
df = pd.read_csv("Facebook_Marketplace_data.csv")

# Drop useless columns
df = df.drop(columns=["Column1", "Column2", "Column3", "Column4"], errors="ignore")

# Parse time
df["status_published"] = pd.to_datetime(df["status_published"])
df = df.sort_values("status_published")
df["date"] = df["status_published"].dt.date

# Define entity (1 page giả lập)
df["page_id"] = "facebook_page"

# =========================
# 2. NLP PROXY – SENTIMENT FROM REACTIONS
# =========================
# Tổng reaction
df["total_reactions"] = (
    df["num_likes"]
    + df["num_loves"]
    + df["num_wows"]
    + df["num_hahas"]
    + df["num_sads"]
    + df["num_angrys"]
)

# Positive & Negative signals
df["positive_reactions"] = (
    df["num_likes"]
    + df["num_loves"]
    + df["num_wows"]
    + df["num_hahas"]
)

df["negative_reactions"] = df["num_sads"] + df["num_angrys"]

# Sentiment proxy (NLP-style feature)
df["negative_ratio"] = df["negative_reactions"] / df["total_reactions"].replace(0, 1)

# =========================
# 3. DAILY AGGREGATION (PAGE LEVEL)
# =========================
daily = (
    df.groupby(["page_id", "date"])
      .agg(
          post_count=("status_id", "count"),
          reactions=("total_reactions", "sum"),
          comments=("num_comments", "sum"),
          shares=("num_shares", "sum"),
          avg_negative_ratio=("negative_ratio", "mean"),
          positive_sum=("positive_reactions", "sum"),
          negative_sum=("negative_reactions", "sum")
      )
      .reset_index()
)

print("Daily data sample:")
print(daily.head())

# =========================
# 4. SLIDING WINDOW 3 DAYS (FEATURE TABLE)
# =========================
feature_rows = []

for page, group in daily.groupby("page_id"):
    group = group.sort_values("date").reset_index(drop=True)

    for i in range(len(group) - 2):
        window = group.iloc[i:i+3]

        feature_rows.append({
            "page_id": page,
            "window_start": window["date"].iloc[0],
            "window_end": window["date"].iloc[-1],

            # Activity
            "total_posts_3d": window["post_count"].sum(),
            "posts_per_day": window["post_count"].mean(),

            # Engagement
            "total_reactions_3d": window["reactions"].sum(),
            "total_comments_3d": window["comments"].sum(),
            "total_shares_3d": window["shares"].sum(),
            "engagement_per_post": window["reactions"].sum() / window["post_count"].sum(),

            # NLP / Sentiment features
            "avg_negative_ratio_3d": window["avg_negative_ratio"].mean(),
            "positive_reactions_3d": window["positive_sum"].sum(),
            "negative_reactions_3d": window["negative_sum"].sum(),

            # Trend (time-aware)
            "reaction_trend": window["reactions"].iloc[-1] - window["reactions"].iloc[0]
        })

# =========================
# 5. EXPORT FEATURE TABLE
# =========================
feature_table = pd.DataFrame(feature_rows)

print("\nFeature table sample:")
print(feature_table.head())

feature_table.to_csv("feature_table_3day.csv", index=False)
print("\nSaved feature_table_3day.csv")
