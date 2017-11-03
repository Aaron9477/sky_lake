
# 计算测试数据

import numpy as np
import src.utils as utils

if __name__ == '__main__':
    malls = utils.get_malls()
    conn = utils.get_db_conn()
    cur = conn.cursor()
    i = 1
    for mall_id in malls:
        print(utils.get_time(), ' ','start handle mall ', mall_id)
        # 获取模型
        model = utils.get_model_xgb(mall_id)
        if model == 0:
            print('no model for mall ', mall_id)
            continue
        # 查出所有wifi，排序
        sql = 'SELECT DISTINCT wifi_ssid FROM {m} ORDER BY wifi_ssid'.format(m=mall_id)
        cur.execute(sql)
        wifis = [r[0] for r in cur.fetchall()]
        # 初始化数据矩阵和初始向量
        metrix = []
        vec = [0 for wifi in range(0, len(wifis))]
        rows = []
        # 查询所有数据
        sql = "SELECT row_id,wifi_ssid,wifi_db FROM data_test_final WHERE mall_id='%s' ORDER BY row_id,wifi_ssid " % mall_id
        cur.execute(sql)
        row = cur.fetchone()
        v = vec[:]
        row_id = row[0]
        if wifis.__contains__(row[1]):
            v[wifis.index(row[1])] = utils.normal(row[2])
        for r in cur.fetchall():
            # 根据是否与前一条row_id相同进行不同操作
            if r[0] != row_id:
                metrix.append(v)
                rows.append(row_id)
                v = vec[:]
                row_id = r[0]
            if wifis.__contains__(r[1]):
                v[wifis.index(r[1])] = utils.normal(r[2])
        metrix.append(v)
        rows.append(row_id)
        metrix = np.array(metrix)
        result = model.predict(metrix)
        # print(result)
        # print(cur.rowcount)
        # print(len(result))
        # print(len(rows))
        for r in range(0, len(rows)):
            sql = "INSERT INTO data_test_result VALUES ('{r}','{s}')".format(r = rows[r], s=result[r])
            cur.execute(sql)
        sql = "INSERT INTO data_test_handled SET mall_id='%s',handled=1" % mall_id
        cur.execute(sql)
        conn.commit()
        print(utils.get_time(), ' ',mall_id, ' handled done')
        print(i, ' malls handled.')
        i += 1
