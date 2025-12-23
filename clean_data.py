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

# =========================
# 2. NLP PROXY – SENTIMENT FROM REACTIONS
# =========================
# Time frame 
df["time_frame"] = df["status_published"].dt.to_period("H")

# Positive & Negative signals
df["positive_reactions"] = (
    df["num_likes"]
    + df["num_loves"]
    + df["num_wows"]
    + df["num_hahas"]
)

df["negative_reactions"] = df["num_sads"] + df["num_angrys"]

# Sentiment proxy (NLP-style feature)
df["negative_ratio"] = df["negative_reactions"] / df["num_reactions"].replace(0, 1)

# =========================
# 3. DAILY AGGREGATION (PAGE LEVEL)
# =========================
daily = (
    df.groupby(["status_type", "time_frame"])
      .agg(
          post_count=("status_id", "count"),
          reactions=("num_reactions", "median"),
          comments=("num_comments", "median"),
          shares=("num_shares", "median"),
          avg_negative_ratio=("negative_ratio", "median"),
          positive_sum=("positive_reactions", "median"),
          negative_sum=("negative_reactions", "median")
      )
      .reset_index()
)


# =========================
# 4. SLIDING WINDOW 1 WEEK (FEATURE TABLE)
# =========================
'''
feature_rows = []
   feature_rows.append({
            "page_id": page,
            "window_start": window["date"].iloc[0],
            "window_end": window["date"].iloc[-1],

for status_type, group in daily.groupby("status_type"):
    group = group.sort_values("time_frame").reset_index(drop=True)

    for i in range(len(group) - 6):
        window = group.iloc[i:i+7]

        feature_rows.append({
            "status_type": status_type,
            "window_start": window["time_frame"].iloc[0].start_time,
            "window_end": window["time_frame"].iloc[-1].end_time,

            # Activity
            "total_posts_1w": window["post_count"].sum(),
            "posts_per_day": window["post_count"].mean(),

            # Engagement
            "total_reactions_1w": window["reactions"].sum(),
            "total_comments_1w": window["comments"].sum(),
            "total_shares_1w": window["shares"].sum(),
            "engagement_per_post": window["reactions"].sum() / window["post_count"].sum(),
            "engagement_per_post": (
                window["reactions"].sum() / window["post_count"].sum()
                if window["post_count"].sum() > 0 else 0
            ),

            # NLP / Sentiment features
            "avg_negative_ratio_1w": window["avg_negative_ratio"].mean(),
            "positive_reactions_1w": window["positive_sum"].sum(),
            "negative_reactions_1w": window["negative_sum"].sum(),

            # Trend (time-aware)
            "reaction_trend": window["reactions"].iloc[-1] - window["reactions"].iloc[0]
        })

# =========================
# 5. EXPORT FEATURE TABLE
# =========================
feature_table = pd.DataFrame(feature_rows)

print("\nFeature table sample:")
print(feature_table.head())

'''
df['day_name']=df['status_published'].dt.day_name()


week = (
    df.groupby(["status_type","day_name"])
      .agg(
          post_count=("status_id", "count"),
          reactions=("num_reactions", "median"),
          comments=("num_comments", "median"),
          shares=("num_shares", "median"),
          avg_negative_ratio=("negative_ratio", "median"),
          positive_sum=("positive_reactions", "median"),
          negative_sum=("negative_reactions", "median")
      )
      .reset_index()
)

print("Daily data sample:")
print(daily)
print("Week data sample:")
print(week)

