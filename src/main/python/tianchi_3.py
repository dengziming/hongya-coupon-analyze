# coding=utf-8
import pandas as pd

path_to_offline = "/Users/dengziming/Desktop/hongya/tmp/day10/ccf_offline_stage1_train.csv"
path_to_online = "/Users/dengziming/Desktop/hongya/tmp/day10/ccf_online_stage1_train.csv"

OffTrain = pd.read_csv(path_to_offline, low_memory=False)

# 头文件信息，输出：user_id,merchant_id,coupon_id,discount_rate,distance,date_received,date
print OffTrain.head()

# 把其中出现的所有的用户ID都统计出来
FUser = OffTrain[['user_id']]
print("FUser.shape=", FUser.shape)

# 去重，总共有539438个独立用户
FUser.drop_duplicates(inplace=True)
OffTrainUser = FUser.shape[0]
print("OffTrainUser=", OffTrainUser)
FUser = FUser.reset_index(drop=True)

# 读取正样本，总共75382个正样本
OffTrainP = pd.read_csv('ccf_offline_stage1_train_P.csv')
OffTrainPNumber = OffTrainP.shape[0]
print("OffTrainPNumber=", OffTrainPNumber)
OffTrainPperUser = OffTrainPNumber * 1.0 / OffTrainUser
# 每个独立用户可能购买的几率是13.974173%
print("OffTrainPperUser=", OffTrainPperUser)

# 寻找同样的ID在P样本中出现的次数


# 得到userid列
t = OffTrainP[['user_id']]
# 添加 Feature1，线下消费总次数
t['FUser1'] = 1
# 对数据进行求和，得到每个userid的购买次数
t = t.groupby('user_id').agg('sum').reset_index()
# join操作
FUser = pd.merge(FUser, t, on=['user_id'], how='left')
print(FUser.head(5))

# 把所有NaN填充为0
FUser = FUser.fillna(0)
print(FUser.head(5))

t = OffTrain[OffTrain['date'] != "null"]
t = t[['user_id']]
# 添加特征2，领取优惠券后消费的次数
t['FUser2'] = 1
# 求和
t = t.groupby('user_id').agg('sum').reset_index()

# join
FUser = pd.merge(FUser, t, on=['user_id'], how='left')
FUser = FUser.fillna(0)
print(FUser.head(5))

print(FUser.FUser2.describe())
FUser.to_csv('FUser.csv', index=False, header=True)
