# coding=utf-8
import pandas as pd

path_to_offline = "/Users/dengziming/Desktop/hongya/tmp/day10/ccf_offline_stage1_train.csv"
path_to_online = "/Users/dengziming/Desktop/hongya/tmp/day10/ccf_online_stage1_train.csv"

data = pd.read_csv(path_to_offline)

# 输出排名前列和后列的商户ID ，发现了两级分化严重啊，小店铺真的可怜，看来不一定做生意就能赚钱啊。
d1 = data['merchant_id']
print(d1.value_counts())

# 输出排名前列和后列的用户ID ，发现用户层两级分化更严重啊，土豪的日志就是买买买，但是穷人们。。。
d1 = data['user_id']
print(d1.value_counts())

# 直观感觉土豪估计以后还是会买买买，穷人基本上不会买了。中间层才有数据分析和挖掘的价值。

# 同理我们统计一下线上的店铺信息
print "统计线下信息"
data = pd.read_csv(path_to_online)

# 线上线下就是不一样，线上交易明显量更多
d1 = data['merchant_id']
print(d1.value_counts())

# 输出排名前列和后列的用户ID ，发现线上用户消费更多，线上土豪的更土豪了
d1 = data['user_id']
print(d1.value_counts())
