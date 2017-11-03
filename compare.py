import src.utils as u


if __name__ == '__main__':
    conn = u.get_db_conn()
    cur = conn.cursor()
    malls = u.get_malls()
    # algs = ['xgb','knn','DT','SGD','MNB','GNB','BNB','GBM']
    algs = ['knn','DT','SGD','MNB','GNB','BNB','GBM']
    score_max = []
    algs_max = []
    score_tmp = 0
    alg_tmp = ''
    for mall in malls:
        for alg in algs:
            sql = "select result from score_{a} WHERE mall_id='{m}'".format(a=alg, m=mall)
            cur.execute(sql)
            score = float(cur.fetchall()[0][0])
            if score > score_tmp:
                score_tmp = score
                alg_tmp = alg
        # if alg_tmp != 'xgb':
        #     print(alg_tmp)
        score_max.append(score)
        algs_max.append(alg_tmp)
        score = 0
    print(score_max)
    print(algs_max)
