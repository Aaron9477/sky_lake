import pymysql as db
import src.utils as u

if __name__ == '__main__':

    mall = 'm_690'
    model = u.get_model_xgb(mall)
    metrix, tar = u.get_data(mall)
    result = model.predict(metrix)
    print(len(result))
    print(len(tar))
    wrong = 0
    for r in range(0,len(result)):
        if result[r]!=tar[r]:
            wrong += 1
    print(wrong)
    print(float(1-wrong/len(result)))


    exit()

    conn = db.connect(host='localhost', port=3306, user='root', passwd='199477', db='tianchi2')
    cur = conn.cursor()
    sql = '''SELECT mall_id FROM score_xgb'''
    cur.execute(sql)
    malls = [r[0] for r in cur.fetchall()]

    for mall in malls:
        sql = "UPDATE score_xgb SET num_behavior=(SELECT COUNT(DISTINCT row_id) FROM {a}) WHERE mall_id='{a}'".format(a=mall)
        cur.execute(sql)
        conn.commit()