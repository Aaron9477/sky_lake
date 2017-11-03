
import src.utils as u

if __name__ == '__main__':
    conn = u.get_db_conn()
    cur = conn.cursor()
    test_file_path = 'F:/WYZ/tianchi/evaluation_public.csv' # 测试集文件路径
    # 创建测试集初始表
    print("开始创建测试集初始表")
    sql = """
        CREATE TABLE IF NOT EXISTS  `data_test` (
      `row_id` int(11) DEFAULT NULL,
      `user_id` varchar(255) DEFAULT NULL,
      `mall_id` varchar(255) DEFAULT NULL,
      `timestamp` varchar(255) DEFAULT NULL,
      `longitude` varchar(255) DEFAULT NULL,
      `latitude` varchar(255) DEFAULT NULL,
      `wifi_infos` varchar(1024) DEFAULT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
    """
    cur.execute(sql)
    # 从文件加载数据
    print("开始从文件加载数据")
    sql = """
    LOAD DATA INFILE '%s' INTO TABLE data_test
    FIELDS TERMINATED by ','
    LINES TERMINATED by '\n'
    IGNORE 1 LINES
    """ % test_file_path
    cur.execute(sql)
    # 创建临时表
    print("开始创建临时表")
    sql = """
    CREATE TABLE  IF NOT EXISTS `tmp` (
      `id` int(11) NOT NULL,
      PRIMARY KEY (`id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
    """
    conn.commit()
    for i in range(1,21):
        sql = """
        insert into `tmp` SET id={id} ON Duplicate KEY UPDATE id={id}
        """.format(id=i)
        cur.execute(sql)
    conn.commit()
    # 创建分割后的表
    print("开始创建分割后的表")
    sql = """
    CREATE TABLE  IF NOT EXISTS `data_test_final` (
      `row_id` int(11) DEFAULT NULL,
      `user_id` varchar(255) DEFAULT NULL,
      `mall_id` varchar(255) DEFAULT NULL,
      `time_stamp` datetime DEFAULT NULL,
      `longitude` varchar(255) DEFAULT NULL,
      `latitude` varchar(255) DEFAULT NULL,
      `wifi_ssid` varchar(255) DEFAULT NULL,
      `wifi_db` int(255) DEFAULT NULL,
      `wifi_conn` varchar(255) DEFAULT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
    """
    cur.execute(sql)
    conn.commit()
    # 分割表
    print("开始分割表")
    sql = """
    INSERT INTO data_test_final
    SELECT
    b.row_id,b.user_id,b.mall_id,b.timestamp,b.longitude,b.latitude,
    SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(b.wifi_infos,';',t.id) ,';',-1),'|',1),
    SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(b.wifi_infos,';',t.id) ,';',-1),'|',2),'|',-1),
    SUBSTRING_INDEX(SUBSTRING_INDEX(SUBSTRING_INDEX(b.wifi_infos,';',t.id) ,';',-1),'|',-1)
    FROM data_test b
    JOIN tmp t
    ON t.id<=( LENGTH(b.wifi_infos) - LENGTH(REPLACE(b.wifi_infos,';','')) + 1 )
    """
    cur.execute(sql)
    conn.commit()
    # 创建结果表
    print("开始创建结果表")
    sql = """
    CREATE TABLE IF NOT EXISTS  `data_test_result` (
      `row_id` int(11) NOT NULL,
      `result` varchar(255) DEFAULT NULL,
      PRIMARY KEY (`row_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
    """
    cur.execute(sql)
    conn.commit()
    # 添加测试表的联合索引，加快查询速度
    print("开始添加测试表的联合索引")
    sql = """
    ALTER TABLE `data_test_final`
    ADD INDEX (`row_id`, `wifi_ssid`) ;
    ALTER TABLE `data_test_final`
    ADD INDEX (`mall_id`) ;
    """
    cur.execute(sql)
    conn.commit()
    # 创建处理记录表，记录已经处理的商场
    print("开始创建处理记录表")
    sql = """
    CREATE TABLE `data_test_handled` (
      `mall_id` varchar(255) NOT NULL,
      `handled` int(11) DEFAULT '0',
      PRIMARY KEY (`mall_id`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
    """
    cur.execute(sql)
    conn.commit()

    print("test db init done")
    cur.close()
    conn.close()
