
# 数据处理，删除某些wifi
# 删除连接数小于9并且所有链接强度小于-30的wifi
# 采用了多线程运算，提高效率

import src.utils as u
import threading

strength = 35   #强度阈值
connects = 6    #连接数阈值

def run(mall_ids,i):
    print(mall_ids)
    for mall_id in mall_ids:
        print(mall_id,' starts')
        conn = u.get_db_conn()
        cur = conn.cursor()
        sql = 'SELECT wifi_ssid FROM {m} GROUP BY wifi_ssid HAVING MAX(wifi_db)<-{s} AND COUNT(*)<{c}'.format(m=mall_id,s=strength,c=connects)
        cur.execute(sql)
        if cur.rowcount > 0:
            wifis = ["'"+r[0]+"'" for r in  cur.fetchall()] # 给所有的wifi ssid 加上引号，组成都好连接的字符串
            sql = "DELETE FROM {m} WHERE wifi_ssid in ({s})".format(m=mall_id,s=','.join(wifis))
            # print(sql)
            cur.execute(sql)
            conn.commit()
            print(mall_id,' done')
        cur.close()
        conn.close()

if __name__ == '__main__':
    malls = u.get_malls()
    thread_count = 15
    threads = []
    for m in [malls[i:i+thread_count] for i in range(0,len(malls),thread_count)]:
        t = threading.Thread(target=run,args=(m,1))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    print("all done")