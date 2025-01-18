import geopandas as gpd
from sqlalchemy import create_engine
from geoalchemy2 import Geometry, WKTElement
import os
from pathlib import Path
from collections import Counter
import traceback

def get_info():
    print("请输入用户名(User):")
    User = input()
    print("请输入密码(Passwords):")
    Passwords = input()
    print("请输入数据库名称(Database):")
    Database = input()
    print("请输入端口(默认5432):")
    n = input()
    print("请输入服务器(默认为127.0.0.1):")
    m = input()
    return User,Passwords,Database,n,m

def shp2pgsql(file, engine, success_files, failed_files):
    """单个shp文件入库"""
    file_name = os.path.split(file)[1]
    try:
        print(f"正在写入: {file_name}")
        tbl_name = file_name.split('.')[0]  # 表名
        map_data = gpd.read_file(file, encoding='utf-8')
        if map_data.empty:
            print(f"警告: 文件 {file_name} 数据为空，跳过此文件。")
            return
        spatial_ref = map_data.crs.to_epsg() or 4326  # 确保获取有效 SRID
        print(f"空间参考系 (SRID): {spatial_ref}")

        # 检查是否有空的几何数据
        if map_data['geometry'].isnull().any():
            print(f"警告: 文件 {file_name} 包含空的几何数据，跳过此文件。")
            return
        
        map_data['geometry'] = map_data['geometry'].apply(lambda x: WKTElement(x.wkt, spatial_ref))
        map_data.to_sql(
            name=tbl_name,
            con=engine,
            if_exists='replace',
            chunksize=1000,
            dtype={'geometry': Geometry(geometry_type='GEOMETRY', srid=spatial_ref)},
            method='multi'
        )
        print(f"表 {tbl_name} 写入成功")
        success_files.append(file_name)  # 记录成功文件
    except Exception as e:
        error_message = str(e)
        print(f"写入文件 {file_name} 失败: {error_message}")
        traceback.print_exc()  # 打印详细错误堆栈
        failed_files.append((file_name, error_message))  # 记录失败文件及原因

def shp2pgsql_batch(dir_name, username, password, host, port, dbname):
    """批量任务"""
    dir_path = Path(dir_name)
    shp_files = list(dir_path.rglob("*.shp"))  # 递归查找 .shp 文件
    file_names = [file.name for file in shp_files]  # 提取文件名
    file_counts = Counter(file_names)  # 统计文件名出现次数
    unique_files = list({file.name: file for file in shp_files}.values())  # 去重，保留第一个出现的路径

    # 打印找到的文件数量和重复文件
    print(f"找到的shp文件数量: {len(file_names)}")
    print(f"去重后的shp文件数量: {len(unique_files)}")

    # 打印找到的文件名称
    if unique_files:
        print("\n找到的shp文件名称列表：")
        for idx, file in enumerate(unique_files, start=1):
            print(f"{idx}. {file.name}")

    # 打印重复文件
    duplicate_files = [name for name, count in file_counts.items() if count > 1]
    if duplicate_files:
        print("\n以下shp文件名称重复：")
        for dup_file in duplicate_files:
            print(f"- {dup_file}")

    # 创建数据库连接
    connection_string = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{dbname}?client_encoding=UTF8"
    engine = create_engine(connection_string)

    try:
        with engine.connect():
            print("\n数据库连接成功！")
    except Exception as e:
        print(f"数据库连接失败: {e}")
        return

    # 记录成功和失败的文件
    success_files = []
    failed_files = []

    # 处理每个 .shp 文件
    for file in unique_files:
        print(f"\n正在处理文件: {file.name}")
        shp2pgsql(str(file), engine, success_files, failed_files)

    # 输出汇总信息
    print("\n任务完成！文件处理情况如下：")
    print(f"成功写入的文件数量: {len(success_files)}")
    print(f"未成功写入的文件数量: {len(failed_files)}")

    if success_files:
        print("\n成功写入的文件列表：")
        for success_file in success_files:
            print(f"- {success_file}")

    if failed_files:
        print("\n未成功写入的文件列表及原因：")
        for failed_file, reason in failed_files:
            print(f"- {failed_file}: {reason}")
    else:
        print("\n所有文件已成功写入数据库！")

# 执行任务计划
if __name__ == '__main__':
    file_path = r'/Users/yangsai/Downloads/QGIS/数据'
    username, password, dbname, port, host = get_info()
    shp2pgsql_batch(file_path, username, password, host, port, dbname)