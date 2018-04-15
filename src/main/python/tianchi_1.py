# coding=utf-8
import pandas as pd

path_to_offline = "/Users/dengziming/Desktop/hongya/tmp/day10/ccf_offline_stage1_train.csv"

data = pd.read_csv(path_to_offline)

print(data.head())
print(data.shape[0])
print(data.shape[1])

# 最终发现数据有：1754884行

# 我们发现 其中有很多Coupon_id为null的数据。我们做的是优惠券使用预测，可是这些数据都没有用优惠劵，所以，首先将这些数据挑选出来。

# 不为空的数据
data1 = data[data['coupon_id'] != "null"]
print(data1.head())
print(data1.shape)

# 为空的数据
data2 = data[data['coupon_id'] == "null"]
print(data2.head())
print(data2.shape)

# 最终发现null数据有：701602行，不为空的：1053282行，保存为.csv备用


# 这两段代码保存数据
# data1.to_csv('ccf_offline_stage1_train_NoNull.csv',index=False,header=True)
# data2.to_csv('ccf_offline_stage1_train_Null.csv',index=False,header=True)
print "检查数据"

# 首先检查没有优惠券的数据
data = data2

test = data[data['date'] == "null"]
print(test.head())
print("没有优惠券的数据中，没有消费的条数为：")
print(test.shape[0])

# 这里打印出来发现没有消费的条数为0？
# 阿里提供数据的时候提供的都是消费数据，因为没有领取优惠劵，也没有实际消费的，在阿里不会能留下数据给我们！

# 所以，我们在预测的时候，如果没有领取优惠券，可以直接预测为消费！ 当然数据里面没有这种情况。


# 再看优惠券不为空的数据集合
data = data1

test1 = data[data['date'] == "null"]
print(test1.head())
print("有优惠券的数据中，没有消费的条数为：")
print(test1.shape[0])
test1.to_csv('ccf_offline_stage1_train_N.csv', index=False, header=True)


test2 = data[data['date'] != "null"]
print(test2.head())
print("有优惠券的数据中，并且消费的条数为：")
print(test2.shape[0])
test2.to_csv('ccf_offline_stage1_train_P.csv', index=False, header=True)



# 正样本：75382个 负样本 977900个
# 那么平均的使用率为75382/1053282=0.071569
print (test2.shape[0] * 1.0 / (test1.shape[0] + test2.shape[0]))