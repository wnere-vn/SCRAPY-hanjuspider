#coding=utf-8

from unicodedata import category
import scrapy
import re
from urllib.request import urljoin
from urllib.parse import urljoin
import time
import pymysql

class HanJuSpider(scrapy.Spider):
    name = 'juji'
    allowed_domains = ['juji.tv']
    con = None
    cursor = None
    start_urls = ['https://www.juji.tv/']
    custom_settings = {  # 自定义每个爬虫的配置文件
        # 不遵守网站规定的爬虫协议
        'ROBOTSTXT_OBEY': False,
        # 并发请求数量
        'CONCURRENT_REQUESTS': 3,
        # 请求间的间隔
        'DOWNLOAD_DELAY': .5,
        # 是否允许Cookie记录相关信息
        'COOKIES_ENABLED': False,
        # 自定义请求头
        'DEFAULT_REQUEST_HEADERS': {
            'User-Agent': ' Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36',
            # 'Cookie': 'xxxx'
        },
        # 缓存爬取的网页（说明：项目完成后取消缓存）
        'HTTPCACHE_ENABLED': True,
        'HTTPCACHE_EXPIRATION_SECS': 0,
        'HTTPCACHE_DIR':'httpcache',
        'HTTPCACHE_IGNORE_HTTP_CODES':[],
        'HTTPCACHE_STORAGE':'scrapy.extensions.httpcache.FilesystemCacheStorage',
    }
    obj = {}
    # 传值函数,把url传值给下一个函数
    def parse(self,response):

        yield scrapy.Request(
            url=response.url,
            callback=self.parseBigNav,
            dont_filter=True,
            meta={},
        )
   
    # 大分类函数
    def parseBigNav(self,response):
        # 先获取到电影电视剧的小分类地址
        cates =response.xpath("//ul[@class='drop-list']/li/a")
        # print(cates,'++++++++++++++++++')
        # 写两个for循环是因为需要把数据分为视频和下载两部分，写了两个yield传值一个给视频一个给下载
        # 下载部分
        for cate in cates[6:]:
            # xpath获取标题和网址
            cate_instahref = cate.xpath('@href').extract_first("")
            cate_instatitle = cate.xpath('text()').extract_first("")
            # 使用字符串拼接将url补充完整
            cate_instahref = "https://www.jujitv.net"+ cate_instahref
            yield scrapy.Request(
                url=cate_instahref,
                callback=self.parseAllPage_insta,
                dont_filter=True,
                meta={
                    'cate_instatitle':cate_instatitle,
                },
            )
        # 视频部分
        for cate in cates[0:6]:
            # xpath获取标题和网址
            cate_moviehref = cate.xpath('@href').extract_first("")
            cate_movietitle = cate.xpath('text()').extract_first("")
            # 使用字符串拼接将url补充完整
            cate_moviehref = "https://www.jujitv.net"+ cate_moviehref
            yield scrapy.Request(
                url=cate_moviehref,
                callback=self.parseAllPage_movie,
                dont_filter=True,
                # 将处理好的数据传给下一个函数。
                meta={
                    'cate_movietitle':cate_movietitle,
                },
            )

    # 采集所有页视频函数
    def parseAllPage_movie(self,response):
        # 将处理后的分类标题拿到
        cate_movietitle = response.meta['cate_movietitle']
        allmovie_pages = response.xpath("//div[@class='qire-box']/div/div/a")
        for allmovie_page in allmovie_pages:
            allmovie_page = allmovie_page.xpath("@data").extract()
        allmovie_pag = allmovie_page[0]
        allmovie_pag = allmovie_pag.split("-")[-1]
        allmovie_pag = int(allmovie_pag)
        # print(allmovie_pag)
        for moviepage in range(1, 2):
            allmovie_url = urljoin(response.url,f"index-{moviepage}.html")
            yield scrapy.Request(
                url=allmovie_url,
                callback=self.parseOnePage_Movie,
                dont_filter=True,
                meta={
                    'cate_movietitle': cate_movietitle,
                },
            )

    #采集所有页下载函数
    def parseAllPage_insta(self, response):
        # 将处理后的分类标题拿到
        cate_instatitle = response.meta['cate_instatitle']
        # 获取到页数部分
        allinsta_pages = response.xpath("//div[@class='list_left']/div/div/a")
        # 处理获取总页数
        for allinsta_page in allinsta_pages:
            allinsta_page = allinsta_page.xpath("@data").extract()
        # 总页数部分
        allinsta_pag = allinsta_page[0]
        allinsta_pag = allinsta_pag.split("-")[-1]
        allinsta_pag = int(allinsta_pag)
        # 循环拼接获取所有页
        for instapage in range(1, 2):
            if range(1) :
                allinsta_url = urljoin(response.url,"index.html")
            else:
                allinsta_url = urljoin(response.url,f"index-{instapage}.html")
        yield scrapy.Request(
            url=allinsta_url,
            callback=self.parseOnePage_insta,
            dont_filter=True,
            meta={
                'cate_instatitle': cate_instatitle,
            },
        )

    #采集一页视频函数
    def parseOnePage_Movie(self,response):
        cate_movietitle = response.meta['cate_movietitle']
        onemovies = response.xpath("//div/ul/li/a[@class='play-img']")
        for onemovie in onemovies[:2]:
            onemovie_href = onemovie.xpath("@href").extract_first("")
            onemovie_href = urljoin(response.url,onemovie_href)
            yield scrapy.Request(
                url=onemovie_href,
                callback=self.parseimovieDetail,
                dont_filter=True,
                meta={
                     'cate_movietitle': cate_movietitle,
                }
            )

    # 采集一页下载函数
    def parseOnePage_insta(self, response):
        cate_instatitle = response.meta['cate_instatitle']
        oneinstas = response.xpath("//div[@class='tit']/a")
        for oneinsta in oneinstas[:2]:
            oneinsta_href = oneinsta.xpath("@href").extract_first("")
            oneinsta_href = f"https://www.jujitv.net{oneinsta_href}"
            yield scrapy.Request(
                url=oneinsta_href,
                callback=self.parseinstaDetail,
                dont_filter=True,
                meta={
                     'cate_instatitle': cate_instatitle,
                }
            )
   
    # 采集详情下载页函数
    def parseinstaDetail(self,response):
        cate_instatitle = response.meta['cate_instatitle']
        # 采集标题
        insta_all = response.xpath("//div/div['art_info']")
        #  [剧 名]: 恋爱相对论
        insta_title = insta_all.xpath('text()').extract()[7]
        # 恋爱相对论
        insta_title = insta_title.split(":")[1]
        #采集类型
        insta_types = response.xpath("//div/div['art_info']/a")
        insta_type = insta_types.xpath('text()').extract_first()
        # print(insta_type)
         #采集年份
        insta_year = insta_all.xpath('text()').extract()[10]
        insta_year = insta_year.split(":")[1]
        # print(insta_year)
        #采集时间
        insta_time = insta_all.xpath('text()').extract()[11]
        insta_time = insta_time.split(":")[1]
        # print(insta_time)
        #采集地区
        insta_place = insta_all.xpath('text()').extract()[12]
        insta_place = insta_place.split(":")[1]
        # print(insta_place)
        # 采集导演
        insta_director = insta_all.xpath('text()').extract()[13]
        insta_director = insta_director.split(":")[1]
        # print(insta_director)
        # 采集演员
        insta_actors = insta_all.xpath('text()').extract()[14]
        insta_actors = insta_actors.split(":")[1]
        insta_actors =insta_actors.split(" ")
        #采集状态
        insta_state = insta_all.xpath('text()').extract()[15]
        insta_state = insta_state.split(":")[1]
        # print(insta_state)
         # 采集简介
        insta_intro = insta_all.xpath('text()').extract()[16]
        insta_intro = insta_intro.split(":")[1]
        # print(insta_intro)
        # 采集地址
        insta_address =response.xpath("//div[@class='down_list']/ul")
        for insta_addres in insta_address[0:1]:
            insta_addres = insta_address.xpath("//li/span/a")
            insta_addre = insta_addres.xpath('@href').extract_first()
            insta_texts = insta_address.xpath("//li/p")
            insta_text = insta_texts.xpath("text()")
            install_address = insta_addre+'-'+insta_text
        # 将数据存入obj
        obj = {
            'cate_instatitle': cate_instatitle,
            'insta_title': insta_title,
            'insta_type': insta_type,
            'insta_time': insta_time,
            'insta_year': insta_year,
            'insta_place': insta_place,
            'insta_director': insta_director,
            'insta_actors': insta_actors,
            'insta_state': insta_state,
            'insta_intro': insta_intro,
            'install_address': [install_address],
        }




    # 采集详情播放页函数
    def parseimovieDetail(self,response):
        cate_movietitle = response.meta['cate_movietitle']
        new_time = time.strftime('%y%m%d',time.localtime(time.time()))
        # movie_uid = response.url
        # movie_uid = movie_uid.split("/")[-1]
        # movie_uid = movie_uid.split(".")[0]
        # movie_uidf = "play-"
        # movie_uid = movie_uid - movie_uidf
        # movie_urlm3u8 =f"https://new.iskcd.com/{new_time}/{movie_uid}/1100kb/hls/playlist_up.m3u8"

        # 标题
        movie_titles = response.xpath("//div[@id='vod']/h2/span")
        movie_title = movie_titles.xpath("text()").extract_first()

        # 主演
        movie_arocts = response.xpath("//div[@class='infos']/dd")[2]
        movie_aroct = movie_arocts.xpath("text()").extract_first()

        #类型
        movie_types = response.xpath("//div[@class='infos']/dd/a")
        movie_type = movie_types.xpath("tetx()").extract_first()

        movie_all = response.xpath("//div[@class='infos']/dd/span")
        for movie_one in movie_all[0:7]:
            movie_one = movie_all.xpath("text()").extract_first()

        # 地区
        movie_place = movie_one[0]

        #备注
        movie_beiz = movie_one[1]

        #导演
        movie_director = movie_one[2]

        #时间
        movie_time = movie_one[3]

        # 年份
        movie_year = movie_one[4]

        #详情
        movie_details = response.xpath("//div[@class='detail-desc-cnt']")
        movie_detail = movie_details.xpath("text()").extract_first()

        #地址
        movie_urls = response.xpath("//div/ul[@class='play-list']/li")[0]
        for movie_url in movie_urls:
            movie_url = movie_urls.xpath("@href").extract_first()
            movie_title = movie_urls.xpath("text()").extract_first()
            movie_url = movie_url.split("/")[-1]
            movie_url = movie_url.split(".")[0]
            movie_urlf = "play-"
            movie_url = movie_url - movie_urlf
        movie_ins = {
            'movie_title':movie_title,
            'movie_aroct': movie_aroct,
            'movie_type': movie_type,
            'movie_place': movie_place,
            'movie_beiz': movie_beiz,
            'movie_director': movie_director,
            'movie_time': movie_time,
            'movie_year': movie_year,
            'movie_detail': movie_detail,
            'movie_url':[movie_url]
        }
        # 更新obj
        obj = object.update(movie_ins)
        yield obj

    def parsedb(self,response):
        if not self.con:
            # 使用connect连接数据库
            self.con=pymysql.connect(
                user="root", password="123456",
                host="127.0.0.1", port=3306, charset="utf8"
            )
        if not self.cursor:
            self.cursor = self.con.cursor()
        # 创建juji数据库
        self.cursor.execute("""
            create database if not exists  juji;
        """)
        self.cursor.execute("""use juji""")
        # 创建大分类表
        self.cursor.execute("""
            create table if not exists bigcate(
                id integer not null primary key auto_increment,
                title varchar(50) not null
            )
        """)
        # 创建播放表
        self.cursor.execute("""
            create table if not exists plays(
                id integer not null primary key auto_increment,
                big_id integer ,
                movie_title varchar(50) not null,
                movie_aroct varchar(100) not null,
                movie_type varchar(10) not null,
                movie_place varchar(20) not null,
                movie_beiz varchar (20) not null,
                movie_director varchar(50) not null,
                movie_time date not null,
                movie_year year not null,
                movie_detail varchar(255) not null,
                playurl varchar(255),
                foreign key(big_id) references  bigcate(id),
            )
        """)
        self.cursor.execute("""
            create table if not exists install(
                id integer not null primary key auto_increment,
                big_id integer ,
                insta_title varchar(50)) not null,
                insta_type char(20),
                insta_time time,
                insta_year year,
                insta_place char(10),
                insta_director varchar(100) not null,
                insta_actors varchar(100) not null,
                insta_state char(20),
                insta_intro text,
                install_address text not null,
                foreign key(big_id) references  bigcate(id),
        """)
        self.connect.commit()
        self.cursor.close()
        self.connect.close()




