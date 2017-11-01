import pymysql as db

if __name__ == '__main__':
    conn = db.connect(host='localhost', port=3306, user='root', passwd='199477', db='tianchi2')
    cur = conn.cursor()
    sql = '''SELECT mall_id FROM score_xgb'''
    cur.execute(sql)
    malls = [r[0] for r in cur.fetchall()]

    for mall in malls:
        sql = "UPDATE score_xgb SET num_behavior=(SELECT COUNT(DISTINCT row_id) FROM {a}) WHERE mall_id='{a}'".format(a=mall)
        cur.execute(sql)
        conn.commit()