import src.utils as u
import pymysql


if __name__ == '__main__':
    conn = u.get_db_conn()
    cur = conn.cursor()
    sql = 'SELECT DISTINCT mall_id FROM shop_info'  # 提取所有商场
    cur.execute(sql)

    for r in cur.fetchall():  # 对于每一个商场建一个表
    # for r in [['m_2333']]:
        try:
            print('mall %s begins...' % r[0] )
            sql = '''
          CREATE TABLE `{s}` 
        (`row_id` int(255) DEFAULT  NULL,
        `user_id` varchar(255) DEFAULT NULL,  
        `shop_id` varchar(255) DEFAULT NULL,  
        `time_stamp` datetime DEFAULT NULL,
        `longitude` varchar(255) DEFAULT NULL,
        `latitude` varchar(255) DEFAULT NULL,
        `wifi_ssid` varchar(255) DEFAULT NULL,
        `wifi_db` int(255) DEFAULT NULL,
        `wifi_conn` varchar(255) DEFAULT NULL)
        ENGINE=InnoDB DEFAULT CHARSET=utf8;'''.format(s=r[0])  # 提取的值存在一个数组里，第一个值是提取的值
            cur.execute(sql)
            sql = '''INSERT INTO {s} 
        SELECT * FROM user_behavior_final WHERE shop_id IN (SELECT shop_id FROM shop_info WHERE mall_id='{s}')'''.format(
                s=r[0])  # 将购买行为对应商场进行存储
            cur.execute(sql)
            print('mall %s done...' % r[0] )
        except pymysql.err.InternalError:
            print('mall %s already done...' % r[0] )
