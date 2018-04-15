# coding=utf-8
import pandas as pd
import numpy as np
import time
from sklearn.ensemble import RandomForestRegressor

path_to_offline = "/Users/dengziming/Desktop/hongya/tmp/day10/ccf_offline_stage1_train.csv"
path_to_online = "/Users/dengziming/Desktop/hongya/tmp/day10/ccf_online_stage1_train.csv"
path_to_test = "/Users/dengziming/Desktop/hongya/tmp/day10/ccf_offline_stage1_test_revised.csv"
path_to_offline_train_N = 'ccf_offline_stage1_train_N.csv'
path_to_offline_train_P = 'ccf_offline_stage1_train_P.csv'
path_to_FMer = "FMer.csv"
path_to_FUser = "FUser.csv"
path_to_Result = "sample_submission20180401.csv"

OffTrain = pd.read_csv(path_to_offline, low_memory=False)

# 头文件信息，输出：user_id,merchant_id,coupon_id,discount_rate,distance,date_received,date
print OffTrain.head()

# 读取特征文件
FMer = pd.read_csv(path_to_FMer)  # 商户特征
FUser = pd.read_csv(path_to_FUser)  # 用户特征

# 读取样本数据
OffTrainN = pd.read_csv(path_to_offline_train_N)
OffTrainP = pd.read_csv(path_to_offline_train_P)

# 加入FLag区分P和N
OffTrainN['Flag'] = 0
OffTrainP['Flag'] = 1

# 负样本建立特征列
print("OffTrainN", OffTrainN.shape)
# 和特征join，添加特征
OffTrainN = pd.merge(OffTrainN, FUser, on=['user_id'], how='left')
print OffTrainN.head()
OffTrainN = pd.merge(OffTrainN, FMer, on=['merchant_id'], how='left')

print(OffTrainN.shape)
print (OffTrainN.head())

# 正样本建立特征列
print("OffTrainP", OffTrainP.shape)
# 和特征join，添加特征

OffTrainP = pd.merge(OffTrainP, FUser, on=['user_id'], how='left')
OffTrainP = pd.merge(OffTrainP, FMer, on=['merchant_id'], how='left')

print(OffTrainP.shape)
OffTrainP.head()

# 生成Flag数组
OffTrainFlagP = OffTrainP['Flag'].values
print("OffTrainFlagP", OffTrainFlagP)
print(OffTrainFlagP.shape)
OffTrainFlagN = OffTrainN['Flag'].values
print("OffTrainFlagN", OffTrainFlagN)
print(OffTrainFlagN.shape)

# 合并Flag
OffTrainFlag = np.append(OffTrainFlagP, OffTrainFlagN)
print("OffTrainFlag", OffTrainFlag)
print(OffTrainFlag.shape[0])

# 生成特征数组
OffTrainFeatureP = OffTrainP[['FUser1', 'FUser2', 'FMer1', 'FMer2', 'FMer3']].values
print("OffTrainFeatureP", OffTrainFeatureP)
print(OffTrainFeatureP.shape)
OffTrainFeatureN = OffTrainN[['FUser1', 'FUser2', 'FMer1', 'FMer2', 'FMer3']].values
print("OffTrainFeatureN", OffTrainFeatureN)
print(OffTrainFeatureN.shape)

# 合并特征
OffTrainFeature = np.append(OffTrainFeatureP, OffTrainFeatureN, axis=0)
print("OffTrainFeature", OffTrainFeature)
print(OffTrainFeature.shape)

'''训练模型'''
print "开始计算模型"
# 使用模型
rf = RandomForestRegressor()  # 这里使用了默认的参数设置
rf.fit(OffTrainFeature, OffTrainFlag)  # 进行模型的训练

# 使用模型预估
temp = rf.predict(OffTrainFeature)
start = time.time()
err = 0
for i in range(OffTrainFeature.shape[0]):
    t = temp[i] - OffTrainFlag[i]
    if (t > 0.5) | (t < -0.5):
        err += 1
err = err * 1.0 / OffTrainFeature.shape[0]
end = time.time()
print ("建模时间：")
print(end - start)
print ("模型在测试数据上的精度：")
print(1 - err)

# 读取测试集
Test = pd.read_csv(path_to_test)

Test = pd.merge(Test, FUser, on=['user_id'], how='left')
Test = pd.merge(Test, FMer, on=['merchant_id'], how='left')
Test['Flag'] = 0.0
print(Test.shape)
print(Test.head())
Test = Test.fillna(0)
TestFeature = Test[['FUser1', 'FUser2', 'FMer1', 'FMer2', 'FMer3']].values
print(TestFeature.shape)
print(TestFeature)
start = time.time()
temp = rf.predict(TestFeature)
end = time.time()
print(end - start)
Test['Flag'] = temp
Test.head()
Test.to_csv(path_to_Result, columns=['user_id', 'coupon_id', 'date_received', 'Flag'],
            index=False, header=False)
