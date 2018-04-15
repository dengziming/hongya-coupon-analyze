# coding=utf-8
import pandas as pd

path_to_offline = "/Users/dengziming/Desktop/hongya/tmp/day10/ccf_offline_stage1_train.csv"
path_to_online = "/Users/dengziming/Desktop/hongya/tmp/day10/ccf_online_stage1_train.csv"

OffTrain = pd.read_csv(path_to_offline, low_memory=False)

# 头文件信息，输出：user_id,merchant_id,coupon_id,discount_rate,distance,date_received,date
print OffTrain.head()

# 把线下商户ID都提取出来
FMer = OffTrain[['merchant_id']]
print("FMer.shape=%s", FMer.shape)
# 去掉重复的
FMer.drop_duplicates(inplace=True)
print("FMer.shape=", FMer.shape)
# 重新建立索引
FMer = FMer.reset_index(drop=True)
t = OffTrain[OffTrain['coupon_id'] != "null"]  # 取出所有有领取优惠券的部分
# print(t.shape)
t = t[['merchant_id']]
t['FMer1'] = 1  # 特征1
t = t.groupby('merchant_id').agg('sum').reset_index()  # 求和
# print(t.head())
FMer = pd.merge(FMer, t, on=['merchant_id'], how='left')
FMer = FMer.fillna(0)
print(FMer.head())
# FMer2 线下总领取优惠券后消费次数
t = OffTrain[OffTrain['coupon_id'] != "null"]  # 取出所有有领取优惠券的部分
print(t.shape)
t = t[t['date'] != 'null']
print(t.shape)
t = t[['merchant_id']]
t['FMer2'] = 1  # 特征2
t = t.groupby('merchant_id').agg('sum').reset_index()  # 求和
# print(t.head())
FMer = pd.merge(FMer, t, on=['merchant_id'], how='left')
FMer = FMer.fillna(0)
print(FMer.head())
# FMer3 线下总消费次数
t = OffTrain[OffTrain['date'] != "null"]  # 取出所有有消费的部分
print(t.shape)
t = t[['merchant_id']]
t['FMer3'] = 1  # 特征3
t = t.groupby('merchant_id').agg('sum').reset_index()  # 求和
# print(t.head())
FMer = pd.merge(FMer, t, on=['merchant_id'], how='left')
FMer = FMer.fillna(0)
print(FMer.head())
FMer.to_csv('FMer.csv', index=False, header=True)
