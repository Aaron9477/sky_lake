# 数据增加经纬度，并保存
import src.utils as u


if __name__ == '__main__':
    for mall in u.get_malls():
        u.get_data_loc(mall)