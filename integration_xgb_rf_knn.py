import pymysql
import src.utils as u
import numpy as np
from numba import jit
import threading


conn = u.get_db_conn()
cur = conn.cursor()

@jit
def run(datas):
    for i in datas:
        knn_result = query('result_17_11_7_knn_5_only_wifi', i)
        xgb_result = query('result_17_11_4_xgb_with_date', i)
        if knn_result == xgb_result:
            write(knn_result, i)
        else:
            rf_result = query('result_17_11_8_rf_1000_only_wifi', i)
            write(rf_result, i)
        if i%1000 == 0:
            print("{id-1000}~{id} have been handled".format(id=i))


@jit
def query(table, row_id):
    row_id = row_id + 1 # row_id从1开始而不是0
    sql = "select result from {t} where row_id={r}".format(t=table, r=row_id)
    cur.execute(sql)
    conn.commit()
    return cur.fetchall()[0][0]

@jit
def write(result, row_id):
    row_id = row_id + 1 # row_id从1开始而不是0
    sql = "INSERT INTO data_test_result VALUES ('{r}','{s}') ON duplicate KEY UPDATE result='{s}'".format(r=row_id,s=result)
    cur.execute(sql)
    conn.commit()


if __name__ == '__main__':
    # 使用多线程
    # thread_count = 50000
    # threads = []
    #
    # for m in [range(i,i+thread_count) for i in range(0, 483931, thread_count)]:
    #     t = threading.Thread(target=run, args=m)
    #     t.start()
    #     threads.append(t)
    # for t in threads:
    #     t.join()
    # print("all done")

    # 未使用多线程
    for i in range(483931):
        knn_result = query('result_17_11_7_knn_5_only_wifi', i)
        xgb_result = query('result_17_11_4_xgb_with_date', i)
        if knn_result == xgb_result:
            write(knn_result, i)
        else:
            rf_result = query('result_17_11_8_rf_1000_only_wifi', i)
            write(rf_result, i)
        if i%1000 == 0:
            print("{id} have been handled".format(id=i))

