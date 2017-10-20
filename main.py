# coding:utf-8

from bson import ObjectId
from datetime import datetime, timedelta
import requests
from spiders import JokeNetEase, JokeNeiHan, JokeQiuShi, JokeXiHa, JokePengFu, JokeWaDuanZi
from comments import CommentNetEase, CommentNeihan, CommentXiHa, CommentPengfu
from pymongo import MongoClient
import logging

DEBUG = False
UPLOAD_URL = "http://xxxx:8081/api/store/joke"
UPLOAD_COMMENT_URL = "http://xxxx:8081/api/store/comment"

SPIDER_MAP = [
    # {"key": "neihan","url": "http://neihanshequ.com/joke/?is_json=1","class": JokeNeiHan,},
    # {"key": "netease","url": "http://3g.163.com/touch/jsonp/joke/chanListNews/T1419316284722/2/0-40.html","class": JokeNetEase},
    # {"key": "xixihaha","url": "http://www.xxhh.com/duanzi/","class": JokeXiHa},
    # {"key": "qiushi","url": "http://m2.qiushibaike.com/article/list/text?page=1&count=30","class": JokeQiuShi},
    {"key": "pengfu","url": "http://www.pengfu.com/xiaohua_1.html","class": JokePengFu},
    {"key": "waduanzi","url": "http://www.waduanzi.com/joke/page/1","class": JokeWaDuanZi}
]

COMMENT_MAP = {
    "xixihaha": CommentXiHa,
    "netease": CommentNetEase,
    "neihan": CommentNeihan,
    "pengfu": CommentPengfu
}

if DEBUG:
    client = MongoClient(
        host="mongodb://user:password@公网IP:27017/thirdparty",
        maxPoolSize=1, minPoolSize=1
    )
else:
    client = MongoClient(
        host="mongodb:///user:password@内网IP:27017/thirdparty",
        maxPoolSize=1, minPoolSize=1
    )
db = client.get_default_database()
joke_collection = db.jokes
comment_collection = db.joke_comments


def main():
    for num,config in enumerate(SPIDER_MAP):
        logging.info("task : %s" % num)
        key = config.get("key")
        logging.info("start crawl: %s" % key)
        try:
            jokes = config["class"].run(config["url"])
        except Exception as e:
            logging.error(e.message, exc_info=True)
        else:
            for joke in jokes:
                joke_id = joke.store(joke_collection)
                if not joke_id:
                    continue
                else:
                    upload_to_pg(str(joke_id))
                if not COMMENT_MAP.get(key):
                    continue
                comments = COMMENT_MAP[key].run(joke_id, joke.comment_need)
                for comment in comments:
                    comment_id = comment.store(comment_collection)
                    if comment_id:
                        upload_comment_pg(str(comment_id))
        logging.info("end crawl: %s" % key)
    client.close()


def upload_to_pg(_id):
    try:
        joke = joke_collection.find_one({"_id": ObjectId(_id)})
    except Exception:
        return
    if not joke:
        return
    if joke["pb_site"] == u"捧腹网":
        online_source_id = 5266
    elif joke["pb_site"] == u"挖段子":
        online_source_id = 5267
    else:
        return
    assert isinstance(joke["pb_time"], datetime)
    assert isinstance(joke["insert"], datetime)
    insert = joke["insert"] + timedelta(hours=8)
    data = {
        "title": joke["content"],
        "unique_id": _id,
        "publish_site": joke["author"],
        "publish_time": joke["pb_time"].isoformat()[:-7]+"Z",
        "insert_time": insert.isoformat()[:-7]+"Z",
        "author": joke["author"],
        "author_icon": joke["avatar"],
        "site_icon": joke["avatar"],
        "source_id": online_source_id,
        "online": True,
        "content": [{"txt": joke["content"]}],
        "like": joke["n_like"],
        "dislike": joke["n_dislike"],
        "comment": joke["n_comment"],
    }
    try:
        r = requests.post(UPLOAD_URL, json=data, timeout=(3, 5))
    except Exception as e:
        logging.error(e.message)
    else:
        logging.info(r.content)


def upload_comment_pg(_id):
    try:
        comment = comment_collection.find_one({"_id": ObjectId(_id)})
    except Exception:
        return
    if not comment:
        return
    assert isinstance(comment["insert"], datetime)
    insert = comment["insert"] + timedelta(hours=8)
    data = {
        "content": comment["content"],
        "commend": comment["n_like"],
        "insert_time": insert.isoformat()[:-7]+"Z",
        "user_name": comment["author"],
        "avatar": comment["avatar"],
        "foreign_id": comment["joke"],
        "unique_id": comment["unique"],
    }
    try:
        r = requests.post(UPLOAD_COMMENT_URL, json=data, timeout=(3, 5))
    except Exception as e:
        logging.error(e.message)
    else:
        logging.info(r.content)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S",
                        filename="joke.log",
                        filemode="a+")

    main()
    client.close()
