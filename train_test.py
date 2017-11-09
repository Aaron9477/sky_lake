
# 计算测试数据

import numpy as np
import src.utils as utils
import gc
import time

import os
import pymysql
from sklearn.externals import joblib    #储存模型
import time
from sklearn.model_selection import train_test_split
from xgboost.sklearn import XGBClassifier   #Boosting Tree方法
from src.utils import get_data
from src.utils import get_model
from src.utils import root_path


conn = utils.get_db_conn()
cur = conn.cursor()
sql = 'SELECT mall_id FROM shop_info GROUP BY mall_id ORDER BY COUNT(*)'  # 按照商铺数量进行排序，找出所有商场？？？？？？
cur.execute(sql)
malls = [r[0] for r in cur.fetchall()]  # 得到商场的数组
random_state = 10

def get_time():
    return time.asctime(time.localtime(time.time()))

def examinate(algorithm_name):
    table_name = 'score_' + algorithm_name  #结果存储表名
    for mall_id in malls:
        print(mall_id, ' with ', algorithm_name, ' starts...')
        sql = "SELECT train_time FROM {s} WHERE mall_id='{m}'".format(m=mall_id, s=table_name)  # 检测这个商场有没有建过模型，建过模型会有记录
        try:    # 测试有没有建过表，如果没建过就会建表
            cur.execute(sql)
        except pymysql.err.ProgrammingError:
            sql2 = '''CREATE TABLE `{n}` (
                                `mall_id`  varchar(255) NOT NULL ,
                                `result`  varchar(255) NULL ,
                                `param`  varchar(255) NULL ,
                                `train_time`  int NULL ,
                                PRIMARY KEY (`mall_id`)
                                );'''.format(n=table_name)
            cur.execute(sql2)
            cur.execute(sql)
        if cur.rowcount != 0:  # 已经建过模型
            print(mall_id, ' has already been fittedwith ', algorithm_name)
            continue
        metrix, tar = get_data(mall_id)
        x_train, x_test, y_train, y_test = train_test_split(metrix, tar, test_size=0.1,
                                                            random_state=random_state)  # 分割测试集和训练集
        save_dir = root_path + "model/" + algorithm_name + "_" + mall_id + "_model.m"  # 存储模型位置
        clf = get_model(algorithm_name) # 根据名称获取新模型
        train_time = time.time()
        clf.fit(x_train, y_train)
        train_time = time.time() - train_time
        print('time : ', train_time)
        score = clf.score(x_test, y_test)  # 检验训练效果，得到准确度
        train_time = int(train_time)
        sql = "INSERT INTO {tn} SET result='{s}', train_time={tt},mall_id='{m}' " \
              "ON DUPLICATE KEY UPDATE result='{s}', train_time={tt}".format(
            s=score, m=mall_id, tt=train_time, tn=table_name)
        cur.execute(sql)
        joblib.dump(clf, save_dir)
        print(get_time(), ' saved a model for ', mall_id, ' with ', algorithm_name, ' .  score ', score)
        conn.commit()

if __name__ == '__main__':
    malls = utils.get_malls()
    conn = utils.get_db_conn()
    cur = conn.cursor()
    i = 1
    for mall_id in malls:
        print(utils.get_time(), ' ','start handle mall ', mall_id)

        # xgb获取模型
        # model = utils.get_model_xgb(mall_id)

        # 比较xgb和RF，选择模型
        sql = "select result from score_xgb where mall_id='{m}'".format(m=mall_id)
        cur.execute(sql)
        xgb_res= float(cur.fetchall()[0][0])
        sql = "select result from score_rf_1000 where mall_id='{m}'".format(m=mall_id)
        cur.execute(sql)
        rf_res= float(cur.fetchall()[0][0])
        if (xgb_res-rf_res)>0.005:
            model = utils.get_model_xgb(mall_id)
            print("{m} choose xgb".format(m=mall_id))
        else:
            model = utils.get_model_rf(mall_id)
            print("{m} choose RF".format(m=mall_id))

        # 选定模型实施测试
        if model == 0:
            print('no model for mall ', mall_id)
            continue
        # 查出所有wifi，排序
        sql = 'SELECT DISTINCT wifi_ssid FROM {m} ORDER BY wifi_ssid'.format(m=mall_id)
        cur.execute(sql)
        wifis = [r[0] for r in cur.fetchall()]
        # 初始化数据矩阵和初始向量
        matrix = []
        weight_conn = 1.5   # 连接为true时的权重
        matrix_day = []
        weight_day = 3  # [0, 0, 3, 0, 0, 0, 0]
        matrix_hour = []
        # 以上三个矩阵分别存储wifi信息，消费时间是周几的信息，消费时间是几点的信息，最后合并三个矩阵，作为全部数据
        weight_hour = 3 # [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        vec = [0 for wifi in range(0, len(wifis))]
        vec_mod_day = [0 for x in range(0,7)]
        vec_mod_hour = [0 for x in range(0,24)]
        rows = []
        # 查询所有数据
        sql = "SELECT row_id,wifi_ssid,wifi_db,time_stamp,wifi_conn,DAYOFWEEK(time_stamp),HOUR(time_stamp),MINUTE(time_stamp) FROM data_test_final WHERE mall_id='%s' ORDER BY row_id,wifi_ssid " % mall_id
        cur.execute(sql)
        row = cur.fetchone()
        v = vec[:]
        vec_day = vec_mod_day[:]
        vec_day[ row[5] - 1 ] = weight_day
        vec_hour = vec_mod_hour[:]
        hour = (row[6]+1) if row[7]>=30  else row[6]
        vec_hour[0 if hour > 23 else hour] = weight_hour
        row_id = row[0]
        if wifis.__contains__(row[1]):
            v[wifis.index(row[1])] = utils.normal(row[2])
        for r in cur.fetchall():
            # 根据是否与前一条row_id相同进行不同操作
            if r[0] != row_id:
                matrix.append(v)
                matrix_day.append(vec_day)
                matrix_hour.append(vec_hour)
                # tar.append(shop_id)
                rows.append(row_id)
                v = vec[:]
                vec_day = vec_mod_day[:]
                vec_day[r[5] - 1] = weight_day
                vec_hour = vec_mod_hour[:]
                hour = (r[6] + 1) if r[7] >= 30  else r[6]
                vec_hour[0 if hour > 23 else hour] = weight_hour
                row_id = r[0]
            if wifis.__contains__(r[1]):
                v[wifis.index(r[1])] = utils.normal(r[2])
        matrix.append(v)
        matrix_day.append(vec_day)
        matrix_hour.append(vec_hour)
        rows.append(row_id)
        matrix = np.hstack([matrix_day,matrix_hour,matrix])
        result = model.predict(matrix)
        # print(result)
        # print(cur.rowcount)
        # print(len(result))
        # print(len(rows))
        for r in range(0, len(rows)):
            sql = "INSERT INTO data_test_result VALUES ('{r}','{s}')".format(r = rows[r], s=result[r])
            cur.execute(sql)
        sql = "INSERT INTO data_test_handled SET mall_id='%s',handled=1 ON DUPLICATE KEY UPDATE handled=1" % mall_id
        cur.execute(sql)
        conn.commit()
        print(utils.get_time(), ' ',mall_id, ' handled done')
        print(i, ' handled.')
        i += 1
        # 清空内存
        gc.collect()

# 之前的，只有wifi作为对象
# if __name__ == '__main__':
#     malls = utils.get_malls()
#     conn = utils.get_db_conn()
#     cur = conn.cursor()
#     i = 1
#     for mall_id in malls:
#         print(utils.get_time(), ' ','start handle mall ', mall_id)
#         # 获取模型
#         model = utils.get_model_xgb(mall_id)
#         if model == 0:
#             print('no model for mall ', mall_id)
#             continue
#         # 查出所有wifi，排序
#         sql = 'SELECT DISTINCT wifi_ssid FROM {m} ORDER BY wifi_ssid'.format(m=mall_id)
#         cur.execute(sql)
#         wifis = [r[0] for r in cur.fetchall()]
#         # 初始化数据矩阵和初始向量
#         metrix = []
#         vec = [0 for wifi in range(0, len(wifis))]
#         rows = []
#         # 查询所有数据
#         sql = "SELECT row_id,wifi_ssid,wifi_db FROM data_test_final WHERE mall_id='%s' ORDER BY row_id,wifi_ssid " % mall_id
#         cur.execute(sql)
#         row = cur.fetchone()
#         v = vec[:]
#         row_id = row[0]
#         if wifis.__contains__(row[1]):
#             v[wifis.index(row[1])] = utils.normal(row[2])
#         for r in cur.fetchall():
#             # 根据是否与前一条row_id相同进行不同操作
#             if r[0] != row_id:
#                 metrix.append(v)
#                 rows.append(row_id)
#                 v = vec[:]
#                 row_id = r[0]
#             if wifis.__contains__(r[1]):
#                 v[wifis.index(r[1])] = utils.normal(r[2])
#         metrix.append(v)
#         rows.append(row_id)
#         metrix = np.array(metrix)
#         result = model.predict(metrix)
#         # print(result)
#         # print(cur.rowcount)
#         # print(len(result))
#         # print(len(rows))
#         for r in range(0, len(rows)):
#             sql = "INSERT INTO data_test_result VALUES ('{r}','{s}')".format(r = rows[r], s=result[r])
#             cur.execute(sql)
#         sql = "INSERT INTO data_test_handled SET mall_id='%s',handled=1" % mall_id
#         cur.execute(sql)
#         conn.commit()
#         print(utils.get_time(), ' ',mall_id, ' handled done')
#         print(i, ' malls handled.')
#         i += 1
