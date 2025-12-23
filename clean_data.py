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
df["positive_ratio"] = df["positive_reactions"] / df["num_reactions"]

# Viral
p_viral=0.12 # tỉ lệ chuyển đổi trung bình của những lời mời đó
df['viral']=df['num_shares']*p_viral

# Factored Engagement (Tổng hợp tương tác có trọng số)
k_comment=2
k_reaction=1
k_share=3
df['factored']=k_comment*df['num_comments']+k_reaction*df['num_reactions']+k_share*df['num_shares']

# =========================
# 3. DAILY AGGREGATION (PAGE LEVEL)
# =========================
daily = (
    df.groupby(["status_type", "time_frame"])
      .agg(
          post_count=("status_id", "count"),
          reactions=("num_reactions", "median"),
          positive_reactions=("positive_reactions", "median"),
          negative_reactions=("negative_reactions", "median"),
          avg_positive_ratio=("positive_ratio", "median"),
          shares=("num_shares", "median"),
          viral=("viral","median"),
          engagement=('factored','median')
      )
      .reset_index()
)


# =========================
# 4. SLIDING WINDOW 1 WEEK (FEATURE TABLE)
# =========================
df['day_name']=df['status_published'].dt.day_name()


week = (
    df.groupby(["status_type","day_name"])
      .agg(
          post_count=("status_id", "count"),
          reactions=("num_reactions", "median"),
          positive_reactions=("positive_reactions", "median"),
          negative_reactions=("negative_reactions", "median"),
          avg_positive_ratio=("positive_ratio", "median"),
          shares=("num_shares", "median"),
          viral=("viral","median"),
          engagement=('factored','median')
      )
      .reset_index()
)

# print("Daily data sample:")
# print(daily)
# print("Week data sample:")
# print(week)

print(df)