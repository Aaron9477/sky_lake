
import src.utils as u
import numpy as np
import gc
import os
from numba import jit


# 训练并获取模型
# @jit
def get_model_rf(mall_id):
    data, tar = u.get_data(mall_id)
    clf = u.get_model('RF_1000')
    clf.fit(data, tar)
    return clf

def get_model_knn(mall_id):
    data, tar = u.get_data(mall_id)
    clf = u.get_model('knn_5')
    clf.fit(data, tar)
    return clf


# 获取测试集数据
# @jit
def get_data(mall_id, cur):
    # 查出所有wifi，排序
    sql = 'SELECT DISTINCT wifi_ssid FROM {m} ORDER BY wifi_ssid'.format(m=mall_id)
    cur.execute(sql)
    wifis = [r[0] for r in cur.fetchall()]
    # 初始化数据矩阵和初始向量
    matrix = []
    vec = [0 for wifi in range(0, len(wifis))]
    rows = []
    # 查询所有数据
    sql = "SELECT row_id,wifi_ssid,wifi_db FROM data_test_final WHERE mall_id='%s' ORDER BY row_id,wifi_ssid " % mall_id
    cur.execute(sql)
    row = cur.fetchone()
    v = vec[:]
    row_id = row[0]
    if wifis.__contains__(row[1]):
        v[wifis.index(row[1])] = u.normal(row[2])
    for r in cur.fetchall():
        # 根据是否与前一条row_id相同进行不同操作
        if r[0] != row_id:
            matrix.append(v)
            rows.append(row_id)
            v = vec[:]
            row_id = r[0]
        if wifis.__contains__(r[1]):
            v[wifis.index(r[1])] = u.normal(r[2])
    matrix.append(v)
    rows.append(row_id)
    matrix = np.array(matrix)
    return rows, matrix


# 数据存储
# @jit
def result_store_rf(rows, result, cur):
    for r in range(0, len(rows)):
        sql = "INSERT INTO data_test_result VALUES ('{r}','{s}') ON duplicate KEY UPDATE result='{s}'".format(r=rows[r], s=result[r])
        cur.execute(sql)

def result_store_knn(rows, result, cur):
    for r in range(0, len(rows)):
        sql = "INSERT INTO data_test_result_knn VALUES ('{r}','{s}') ON duplicate KEY UPDATE result='{s}'".format(r=rows[r], s=result[r])
        cur.execute(sql)


# 判断是否已处理
# @jit
def handled(mall, cur):
    sql = "select handled from data_test_handled WHERE mall_id='%s'" % mall
    cur.execute(sql)
    if cur.rowcount < 1:
        return 0
    return cur.fetchone()[0]


if __name__ == '__main__':
    malls = u.get_malls()
    conn = u.get_db_conn()
    cur = conn.cursor()
    for mall in malls :
        if handled(mall, cur) == 1:
            print(u.get_time(),' ', mall, ' of rf already handled...')
            continue
        print(u.get_time(),' ', mall, ' of rf start handling...')
        # 建模的同时提取数据进行训练
        clf = get_model_rf(mall)
        print(u.get_time(),' ', mall, ' of rf train done...')
        rows, data = get_data(mall, cur)
        result = clf.predict(data)
        print(u.get_time(),' ', mall, ' of rf predict done...')
        result_store_rf(rows, result, cur)
        print(u.get_time(),' ', mall, ' of rf result stored...')
        sql = "INSERT INTO data_test_handled SET mall_id='%s',handled=1 ON DUPLICATE KEY UPDATE handled=1" % mall
        cur.execute(sql)
        conn.commit()
        # 清空内存
        gc.collect()

    for mall in malls :
        if handled(mall, cur) == 2:
            print(u.get_time(),' ', mall, ' of knn already handled...')
            continue
        print(u.get_time(),' ', mall, ' of knn start handling...')
        # 建模的同时提取数据进行训练
        clf = get_model_knn(mall)
        print(u.get_time(),' ', mall, ' of knn train done...')
        rows, data = get_data(mall, cur)
        result = clf.predict(data)
        print(u.get_time(),' ', mall, ' of knn predict done...')
        result_store_knn(rows, result, cur)
        print(u.get_time(),' ', mall, ' of knn result stored...')
        sql = "INSERT INTO data_test_handled SET mall_id='%s',handled=2 ON DUPLICATE KEY UPDATE handled=2" % mall
        cur.execute(sql)
        conn.commit()
        # 清空内存
        gc.collect()

    cur.close()
    conn.close()
    print('all malls done')

    os.system('shutdown -s -t 5')
    exit()
