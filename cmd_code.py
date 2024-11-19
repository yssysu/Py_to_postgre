import geopandas as gpd
from sqlalchemy import create_engine
from geoalchemy2 import Geometry, WKTElement
import os
from pathlib import Path

def shp2pgsql(file, engine):
    """单个shp文件入库"""
    try:
        file_name = os.path.split(file)[1]
        print('正在写入: ' + str(file))  # 转换为字符串
        tbl_name = file_name.split('.')[0]  # 表名
        map_data = gpd.GeoDataFrame.from_file(file)
        spatial_ref = map_data.crs.to_epsg() or 4326  # 确保获取有效 SRID
        print(f"空间参考系 (SRID): {spatial_ref}")
        map_data['geometry'] = map_data['geometry'].apply(
            lambda x: WKTElement(x.wkt, spatial_ref))
        map_data.to_sql(
            name=tbl_name,
            con=engine,
            if_exists='replace',
            chunksize=1000,
            dtype={'geometry': Geometry(geometry_type='GEOMETRY', srid=spatial_ref)},
            method='multi'
        )
        print(f"表 {tbl_name} 写入成功")
    except Exception as e:
        print(f"写入文件 {file} 失败: {e}")
    return None


def shp2pgsql_batch(dir_name, username, password, host, port, dbname):
    """创建批量任务"""
    # 使用 pathlib 查找所有 .shp 文件
    dir_path = Path(dir_name)
    shp_files = list(dir_path.rglob("*.shp"))  # 递归查找 .shp 文件
    print(f"找到的shp文件数量: {len(shp_files)}")  # 打印文件数量
    if shp_files:
        print("找到的shp文件名称列表:")
        for idx, file in enumerate(shp_files, start=1):
            relative_path = file.relative_to(dir_path)  # 获取相对路径
            print(f"{idx}. {relative_path}")  # 打印每个文件的相对路径
    
    # 创建数据库连接
    connection_string = f"postgresql+psycopg2://{username}:{password}@{host}:{port}/{dbname}"
    engine = create_engine(connection_string)

    with engine.connect() as connection:
        print("数据库连接成功！")

    # 处理每个 .shp 文件
    for file in shp_files:
        relative_path = file.relative_to(dir_path)
        print(f"正在处理文件: {relative_path}")
        shp2pgsql(str(file), engine)  # 确保传入的 file 是字符串类型
    return None

# 执行任务计划
if __name__ == '__main__':
    # 推荐使用绝对路径，以免在不同操作系统下出现问题
    file_path = r'path/to/your/directory'
    username = 'yourname'
    password = 'yourpassword'
    host = 'yourhost'
    port = 'yourport'
    dbname = 'yourdatabase'
    shp2pgsql_batch(file_path, username, password, host, port, dbname)