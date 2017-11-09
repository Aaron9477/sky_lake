# 从数据库输出结果文件
import src.utils as utils

if __name__ == '__main__':
    path = 'F:/WYZ/tianchi/result_integration_xgbrfknn_11_8.csv'
    conn = utils.get_db_conn()
    cur = conn.cursor()
    sql = """
    SELECT * FROM result_17_11_8_integration_xgbrfknn_only_wifi INTO OUTFILE '{p}'
    FIELDS TERMINATED by ','
    OPTIONALLY ENCLOSED by ''
    LINES TERMINATED by '\n'
    """.format(p=path)
    cur.execute(sql)
    conn.commit()
    cur.close()
    conn.close()