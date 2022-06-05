# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

# 数据库的异步存储
# https://www.jb51.net/article/180687.htm
# useful for handling different item types with a single interface


from itemadapter import ItemAdapter
import pymysql
from twisted.enterprise import adbapi 

# 异步更新操作


class MySQLPipeline(object, ):
    def __init__(self, dbpool):
        self.dbpool = dbpool

    @classmethod
    def from_settings(cls, settings):
        adbparams = dict(
            host=settings['MYSQL_HOST'],
            db=settings['MYSQL_DBNAME'],
            user=settings['MYSQL_USER'],
            password=settings['MYSQL_PASSWORD'],
            cursorclass=pymysql.cursors.DictCursor  # 指定cursor类型
        )
        # 连接数据池ConnectionPool，使用pymysql连接
        dbpool = adbapi.ConnectionPool('pymysql', **adbparams)
        # 返回实例化参数
        return cls(dbpool)

    def process_item(self, item, spider):
        query = self.dbpool.runInteraction(self.do_insert, item)  # 指定操作方法和操作数据
        query.addCallback(self.handle_error)  # 处理异常
        return item

    def do_insert(self, cursor, item):
        # 在数据入库之前先查找一下，看看数据库里是否有该数据
        select_playsql = """
            select movie_title from plays where title='%s' 
        """%(object.movie_title)
        # 如果有该数据，则对数据进行更新
        if select_playsql is not None:
            update_sql = """
                update plays set movie_time=%s,movie_year=%s,playurl=%s where movie_title=%s
            """%(object.movie_time,object.movie_year,object.playurl,object.movie_title)
            cursor.execute(update_sql,(item['movie_time'],item['movie_year'],item['playurl'],item['movie_title'],))
        # 如果没有改数据，则插入数据
        else:
            insert_sql = """
                insert into plays(movie_title, movie_aroct, movie_type, movie_place, movie_beiz, movie_director, movie_time, movie_year, movie_detail,movie_url) VAULES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,)
            """
            cursor.execute(insert_sql,(item['movie_title'],item['movie_aroct'],item['movie_type'],item['movie_place'],item['movie_beiz'],item['movie_director'],item['movie_time'],item['movie_year'],item['movie_detail'],item['movie_url']))

        # 下载表存储数据，代码结构基本如播放表，只是里面的数据不一样
        select_installsql = """
                    select insta_title from install where title='%s' 
                """ % (object.insta_title)
        if select_installsql is not None:
            update_sql = """
                        update install set insta_time=%s,insta_year=%s,install_address=%s where insta_title=%s
                    """ % (object.insta_time, object.insta_year, object.install_address, object.insta_title)
            cursor.execute(update_sql, (item['insta_time'], item['insta_year'], item['install_address'], item['insta_title'],))
        else:
            insert_sql = """
                        insert into install(insta_title, insta_type, insta_time, insta_year, insta_place, insta_director, insta_actors, insta_state, insta_intro,install_address) VAULES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,)
                    """
            cursor.execute(insert_sql, (
            item['insta_title'], item['insta_type'], item['insta_time'], item['insta_year'], item['insta_place'],
            item['insta_director'], item['insta_actors'], item['insta_state'], item['insta_intro'], item['install_address']))
    #     insert_sql = """
    # insert into djye(music_title, cate_title, download_url, path) VALUES (%s,%s,%s,%s)
    # """
    #     cursor.execute(insert_sql, (item['music_title'], item['cate_title'], item['download_url'][0], item['path']))
    def handle_error(self, failure):
        if failure:
            # 打印错误信息
            print(failure)