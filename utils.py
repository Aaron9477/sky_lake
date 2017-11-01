import pymysql as db
import numpy as np
import os.path as op
import time
from sklearn.externals import joblib

from sklearn.neighbors import KNeighborsClassifier
from sklearn.naive_bayes import GaussianNB, MultinomialNB, BernoulliNB
from sklearn.tree import DecisionTreeClassifier
from sklearn.linear_model import SGDClassifier
import lightgbm.sklearn as gbm

# 这是工具类文件！！！！！！！！！！！！！！


root_path = '../'

# 获取时间
def get_time():
    return time.asctime(time.localtime(time.time()))


# 归一化信号强度，1-10
def normal(val):
    ret = 0
    if val>=-10:
        ret = 0
    elif val <= -90:
        ret = 9
    else:
        ret = int(-val / 10)
    return 10 - ret


# 获取一个数据库连接
def get_db_conn():
    return db.connect(host='localhost', port=3306, user='root', passwd='199477', db='tianchi2')


# 获取所有商场
def get_malls():
    conn = get_db_conn()
    cur = conn.cursor()
    sql = 'SELECT DISTINCT mall_id FROM shop_info'
    cur.execute(sql)
    malls = [r[0] for r in cur.fetchall()]
    cur.close()
    conn.close()
    return malls


# 获取存储文件名，固定格式
def get_file(type, mall_id):
    switcher = {
        'data' : root_path + 'data/' + mall_id + '_data',
        'tar' : root_path + 'data/'  + mall_id + '_tar'
    }
    return switcher.get(type, 'no_data')


# 获取数据，如果数据已经存储在文件中，直接读取文件，否则进行数据库查询并构建数据
def get_data(mall_id):
    if op.exists(get_file('data', mall_id) + '.npy') and op.exists(get_file('tar', mall_id) + '.npy'):
        metrix = np.load(get_file('data', mall_id) + '.npy')
        tar = np.load(get_file('tar', mall_id) + '.npy')
    else:
        print('start to storage data of ',mall_id)
        conn = get_db_conn()
        cur = conn.cursor()
        # 查出所有wifi，排序
        sql = 'SELECT DISTINCT wifi_ssid FROM {m} ORDER BY wifi_ssid'.format(m = mall_id)
        cur.execute(sql)
        wifi_ssids = [r[0] for r in cur.fetchall()]
        vec_mod = [0 for x in range(0,len(wifi_ssids))]
        # print(wifi_ssids)
        # 建立最终矩阵
        metrix = []
        # 创建答案数组
        tar = []
        # 查出所有数据，按照 data_id, wifi_ssid 排序
        sql = 'SELECT row_id,wifi_ssid,wifi_db,shop_id FROM {m} ORDER BY row_id,wifi_ssid'.format(m = mall_id)
        cur.execute(sql)
        r = cur.fetchone()
        data_id = r[0]
        vec = vec_mod[:]
        shop_id = r[3]
        vec[wifi_ssids.index(r[1])] = normal(r[2])
        for r in cur.fetchall():
            if r[0] != data_id:
                metrix.append(vec)
                tar.append(shop_id)
                data_id = r[0]
                vec = vec_mod[:]
                shop_id = r[3]
            vec[wifi_ssids.index(r[1])] = normal(r[2])
        metrix.append(vec)
        tar.append(shop_id)
        metrix = np.array(metrix)
        tar = np.array(tar)
        np.save(get_file('data', mall_id), metrix)
        np.save(get_file('tar', mall_id), tar)
        sql = "INSERT INTO storaged_data SET mall_id='{m}'".format(m=mall_id)
        cur.execute(sql)
        conn.commit()
        cur.close()
        conn.close()
        print(mall_id, ' is finished')
    return metrix,tar


# 获取一个商场的xgb模型的路径，用于存储和读取
def get_model_path_xgb(mall_id):
    return root_path + 'model/xgboost_%s_model.m' % mall_id


# 获取一个商场的xgb模型
def get_model_xgb(mall_id):
    path = get_model_path_xgb(mall_id)
    if op.exists(path):
        return joblib.load(path)
    else:
        return 0


def get_model(algorithm_name):
    if algorithm_name == 'knn':  # 根据算法名称使用不同算法
        clf = KNeighborsClassifier(n_neighbors=5)
    elif algorithm_name == 'DT':
        clf = DecisionTreeClassifier()
    elif algorithm_name == 'SGD':
        clf = SGDClassifier()
    elif algorithm_name == 'MNB':
        clf = MultinomialNB()
    elif algorithm_name == 'GBM':
        clf = gbm.LGBMClassifier()
    elif algorithm_name == 'GNB':
        clf = GaussianNB()
    elif algorithm_name == 'BNB':
        clf = BernoulliNB()
    else:
        print('wrong input!')  # 输入错误直接退出
        exit()
    return  clf