# 使用xgboost训练，并且储存模型

import os
import pymysql
from sklearn.externals import joblib    #储存模型
import time
import src.utils as u
from sklearn.model_selection import train_test_split
from xgboost.sklearn import XGBClassifier   #Boosting Tree方法
from src.utils import get_data
from src.utils import get_model
from src.utils import root_path

conn = u.get_db_conn()
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
        try:
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
                                                            random_state=random_state)  # 分个测试集和训练集
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


    sql = 'SELECT mall_id FROM shop_info GROUP BY mall_id ORDER BY COUNT(*)'    #按照商铺数量进行排序，找出所有商场？？？？？？
    cur.execute(sql)
    malls = [r[0] for r in cur.fetchall()]  #得到商场的数组
    print ('xgb starts')
    for mall_id in malls:
        print(mall_id, ' starts...')
        metrix, tar = get_data(mall_id)
        sql = "SELECT param FROM score_xgb WHERE mall_id='{m}'".format(m=mall_id)  #检测这个商场有没有建过模型，建过模型会有记录
        try:
            cur.execute(sql)
        except pymysql.err.ProgrammingError:
            sql2 = '''CREATE TABLE `score_xgb` (
                    `mall_id`  varchar(255) NOT NULL ,
                    `result`  varchar(255) NULL ,
                    `param`  varchar(255) NULL ,
                    `train_time`  int NULL ,
                    PRIMARY KEY (`mall_id`)
                    );'''
            cur.execute(sql2)
            conn.commit()
            cur.execute(sql)
        if cur.rowcount !=0 :  #已经建过模型
            print(mall_id, ' has already been fitted.')
            continue
        # random_state是随机数的种子，如果该数不是0，每次随机数组是确定的
        # 此处应该保持random_state是固定的，否则测试训练好的的模型，它会测试训练时用过的数据
        x_train, x_test, y_train, y_test = train_test_split(metrix, tar, test_size=0.1, random_state=random_state)  #分个测试集和训练集

        # xgboost方法，基于boosting tree(提升树方法)
        # 设参数 训练慢
        i = 1   #训练前初始化，从60次迭代开始
        score = 0
        train_time = 0

        # xgboost训练
        clf_name = "xgboost"
        save_dir = root_path + "model/" + clf_name + "_" +mall_id + "_model.m"    #存储模型位置

        while True:
            i += 1
            step = 30   #每次增加的步长
            n_est = i * step
            print(get_time(), 'iterate times : ', n_est)
            clf = XGBClassifier(learning_rate =0.1, #学习率 典型值为0.01-0.2
                                n_estimators=n_est,
                                max_depth=5,   #树的最大深度 一般3-10
                                min_child_weight=1,    #决定最小叶子节点样本权重和 值较大，避免过拟合 值过高，会导致欠拟合
                                gamma=0,   #指定了节点分裂所需的最小损失函数下降值。 这个参数的值越大，算法越保守
                                subsample=0.8, #对于每棵树，随机采样的比例 减小，算法保守，避免过拟合。值设置得过小，它会导致欠拟合 典型值：0.5-1
                                colsample_bytree=0.8,  #每棵随机采样的列数的占比
                                objective= 'binary:logistic',    #使用二分类
                                nthread=7, #线程数
                                scale_pos_weight=1,    #在各类别样本十分不平衡时，参数设定为一个正值，可以使算法更快收敛
                                seed=0)   #随机数的种子 设置它可以复现随机数据的结果
            train_time = time.time()
            clf.fit(x_train, y_train)   #实施训练
            train_time = time.time() - train_time   #得到训练时间
            print('time : ', train_time)
            score_tmp = clf.score(x_test,y_test)    #检验训练效果，得到准确度
            if (score_tmp-score)<0.001 or score_tmp>0.965 or i>4:    #只有得分开始下降或者得分大于0.98才会退出循环
                if score_tmp > score:   #当前得分大于上一次的时候，需要存储新模型。其它情况不需要存储新模型
                    joblib.dump(clf, save_dir)
                    print(get_time(), ' saved a model for ', mall_id, ' . previoes score ', score, ' . new score',
                          score_tmp)
                    n_est = i * step
                else:
                    n_est = (i - 1) * step
                score = score_tmp if score < score_tmp else score
                train_time = int(train_time)
                # 存储模型分数等信息
                sql = "INSERT INTO score_xgb SET result='{s}',param={t}, train_time={tt},mall_id='{m}' " \
                      "ON DUPLICATE KEY UPDATE result='{s}',param={t}, train_time={tt}".format(s=score, t=n_est, m=mall_id,tt=train_time)
                # print(sql)
                cur.execute(sql)
                conn.commit()
                print('mall : {m:6} | score : {s:10} | iterate times : {t} '.format(m=mall_id, s=score, t=(i-1)*step))
                # xgb_malls.append(mall_id)
                # xgb_score.append(score)
                break   #只有这里能退出循环

            joblib.dump(clf, save_dir)
            print(get_time(), ' saved a model for ', mall_id, ' . previoes score ', score, ' . new score', score_tmp)
            score = score_tmp
            # print('test done... spent time = {s}'.format(s=get_time() - time))

    print ('knn training starts')    #每个方法都要单独建个表格

    algs = ['knn','DT','SGD','MNB','GNB','BNB','GBM']
    for a in algs:
        examinate(a)
        print (a, ' training starts')


    os.system('shutdown -s -t 5')
    exit()




