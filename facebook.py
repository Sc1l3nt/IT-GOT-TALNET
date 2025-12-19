import pandas as pd
import matplotlib.pyplot as plt

# T-Transform

# 1 làm sạch 
df = pd.read_csv('Facebook_Marketplace_data.csv')
df.head()
df.describe()
df.isna().sum()
# bỏ trùng lặp
df = df.drop_duplicates()

mdata = ['num_reactions', 'num_comments', 'num_shares']
df[mdata] = df[mdata].fillna(0)

df['engagement_score'] =( df['num_reactions'] + df['num_comments'] * 2 + df['num_shares'] * 3)

# comment và share được gắn trọng số cao hơn nhằm thể hiện mức độ tương tác sâu hơn -> phân loại được low medium high
df['engagement_level'] = pd.qcut(
    df['engagement_score'],
    q=3,
    labels=['low','medium', 'high']
)
#

# đổi kiểu dữ liệu text -> ngày
df['status_published'] = pd.to_datetime(df['status_published'])     
# phân tích thời gian xem giờ nào sẽ có độ tương tác cao
df['hour'] = df['status_published'].dt.hour
df['dayofweek'] = df['status_published'].dt.dayofweek



# encode category
if 'devide' in df.columns:
    df = pd.get_dummies(df, columns=['device'], drop_first=True)

# scaling: dùng ở bước train model
# StandardScaler() 



df.to_csv('facebook_cleaned.csv', index=False)
