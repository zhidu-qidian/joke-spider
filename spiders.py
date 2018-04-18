# coding: utf-8

from datetime import datetime
import hashlib
import json
import logging
from types import UnicodeType
import requests
from w3lib.encoding import html_to_unicode
from bs4 import Tag, BeautifulSoup
from pymongo.errors import DuplicateKeyError



def find_tag(root, param):
    if not isinstance(root, (Tag, BeautifulSoup)):
        return None
    method = param.get("method", "find")
    params = param["params"]
    nth = param.get("nth", 0)
    if method == "find":
        tag = root.find(**params)
        return tag
    elif method == "find_all":
        tags = root.find_all(**params)
    elif method == "select":
        tags = root.select(**params)
    else:
        raise ValueError("param['method'] only support find, find_all and select")
    return tags[nth] if len(tags) > nth else None


def find_tags(root, param):
    if not isinstance(root, (Tag, BeautifulSoup)):
        return []
    method = param.get("method", "find_all")
    params = param["params"]
    if method == "find":
        tag = root.find(**params)
        if tag is None:
            return []
        else:
            return [tag]
    elif method == "find_all":
        tags = root.find_all(**params)
    elif method == "select":
        tags = root.select(**params)
    else:
        raise ValueError("param['method'] only support find, find_all and select")
    return tags


def extract_tag_attribute(root, name="text"):
    if root is None:
        return ""
    assert isinstance(root, (Tag, BeautifulSoup))
    if name == "text":
        return root.get_text().strip()
    else:
        value = root.get(name, "")
        if isinstance(value, (list, tuple)):
            return ",".join(value)
        else:
            return value.strip()


class JokeBase(object):
    headers = {"user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.86 Safari/537.36"}
    timeout = 30
    r_json = False

    @classmethod
    def download(cls, url, c_json=False, skip=None, headers=None):
        if headers is None:
            headers = cls.headers
        response = requests.get(url, headers=headers, timeout=(10, cls.timeout))
        content = response.content
        if skip:
            content = content[skip[0]:skip[1]]
        if c_json:
            return json.loads(content)
        else:
            _, content = html_to_unicode(
                content_type_header=response.headers.get("content-type"),
                html_body_str=content
            )
            return content.encode("utf-8")

    @classmethod
    def prepare(cls, document):
        return document

    @classmethod
    def parse(cls, document):
        raise NotImplementedError

    @classmethod
    def run(cls, url):
        document = cls.download(url, c_json=cls.r_json)
        doc = cls.prepare(document)
        jokes = cls.parse(doc)
        logging.info("%s: %s" % (cls.__name__, len(jokes)))
        return jokes


class Joke(object):

    def __init__(self):
        self.title = None
        self.author = None
        self.avatar = None
        self.pb_time = datetime.utcnow()
        self.pb_site = None
        self.content = None
        self.n_comment = 0
        self.n_like = 0
        self.n_dislike = 0
        self.comment_need = {}

    def show(self):
        print("title: %s" % self.title)
        print("author: %s" % self.author)
        print("avatar: %s" % self.avatar)
        print("pb_time: %s" % self.pb_time)
        print("pb_site: %s" % self.pb_site)
        print("content: %s" % self.content)
        print("n_comment: %s" % self.n_comment)
        print("n_like: %s" % self.n_like)
        print("n_dislike: %s" % self.n_dislike)
        print("*" * 120)

    @staticmethod
    def unique(string):
        if isinstance(string, UnicodeType):
            string = string.encode("utf-8")
        return hashlib.md5(string).hexdigest()

    def store(self, collection):
        if not (self.author or self.avatar or self.content):
            logging.warn("joke miss fields author: %s, avatar: %s content: %s"
                         % (self.author, self.avatar, self.content))
        document = dict()
        if self.title:
            document["title"] = self.title
        document["author"] = self.author
        document["avatar"] = self.avatar
        document["pb_time"] = self.pb_time
        document["pb_site"] = self.pb_site
        document["content"] = self.content
        document["n_comment"] = int(self.n_comment)
        document["n_like"] = int(self.n_like)
        document["n_dislike"] = int(self.n_dislike)
        document["insert"] = datetime.utcnow()
        document["unique"] = self.unique(document["content"])
        try:
            result = collection.insert_one(document)
        except DuplicateKeyError:
            pass
        except Exception as e:
            logging.error(e.message, exc_info=True)
        else:
            logging.info("store joke id: %s" % result.inserted_id)
            return result.inserted_id





class JokeNeiHan(JokeBase):

    r_json = True

    @classmethod
    def parse(cls, document):
        data = document.get("data", {})
        groups = data.get("data", [])
        jokes = list()
        for g in groups:
            g = g["group"]
            joke = Joke()
            joke.author = g["user"]["name"]
            joke.avatar = g["user"]["avatar_url"]
            joke.pb_time = datetime.fromtimestamp(g["create_time"])
            joke.pb_site = u"内涵段子"
            joke.content = g["text"]
            joke.n_comment = g["comment_count"]
            joke.n_like = g["digg_count"]
            joke.n_dislike = g["bury_count"]
            joke.comment_need["code"] = g["code"]
            jokes.append(joke)
        return jokes


class JokeNetEase(JokeBase):

    r_json = True

    @classmethod
    def parse(cls, document):
        data = document.get(u"段子", [])
        jokes = list()
        for g in data:
            if g.get("imgsum", 0) == 0:
                joke = Joke()
                joke.title = g["title"]
                joke.pb_site = g["source"]
                joke.content = g["digest"]
                joke.n_comment = g["replyCount"]
                joke.n_like = g["upTimes"]
                joke.n_dislike = g["downTimes"]
                joke.comment_need["code"] = g["docid"]
                jokes.append(joke)
        return jokes


class JokeQiuShi(JokeBase):

    headers = {
        "User-Agent": "qiushibalke_10.8.1_WIFI_auto_19",
        "Source": "android_10.8.1",
        "Model": "Xiaomi/hydrogen/hydrogen:6.0.1/MMB29M/V7.5.6.0.MBCCNDE:user/release-keys",
        "Uuid": "IMEI_8728c26518fa3ae795a7f787073d375f",
        "Deviceidinfo": '{"DEVICEID": "862535037295724","SIMNO": "89860112817005617959","IMSI": "460012225499106","ANDROID_ID": "27dafccd6e32bfb2","SDK_INT": 23,"SERIAL"a882d7f9","MAC": "02:00:00:00:00:00","RANDOM": ""}'
    }
    r_json = True

    @classmethod
    def parse(cls, document):
        jokes = list()
        data = document.get("items", [])
        for g in data:
            if not g.get("user"):
                continue
            joke = Joke()
            joke.author = g["user"]["login"]
            avatar = g["user"].get("thumb")
            if not avatar:
                continue
            if avatar.startswith("//"):
                avatar = "http:" + avatar
            joke.avatar = avatar
            joke.pb_site = u"糗事百科"
            joke.pb_time = datetime.fromtimestamp(g["created_at"])
            joke.content = g["content"]
            joke.n_comment = g.get("comments_count", 0)
            if g.get("votes"):
                joke.n_like = g["votes"]["up"]
                joke.n_dislike = abs(g["votes"]["down"])
            jokes.append(joke)
        return jokes


class JokeXiHa(JokeBase):

    config = {
        "id": {"params": {"selector": "div.comment"}, "method": "select", "attribute": "id"},
        "content": {"params": {"selector": "div.article > pre"}, "method": "select"},
        "author": {"params": {"selector": "div.user-info-username > a"}, "method": "select"},
        "avatar": {"params": {"selector": "div.user-avatar40 > a > img"}, "attribute": "src", "method": "select"},
    }

    @staticmethod
    def find_extract_tag_attribute(tag, params):
        if params.get("params"):
            tag = find_tag(tag, params)
        attribute = params.get("attribute", "text")
        return extract_tag_attribute(tag, attribute)

    @classmethod
    def fetch_metadata(cls, ids):
        url = "http://dg.xxhh.com/getcnums/?__jsonp__=fn&ids={ids}".format(ids=",".join(ids))
        document = cls.download(url, c_json=True, skip=(3, -1))
        metadata = dict()
        for i, meta in enumerate(document.get("d", [])):
            metadata[ids[i]] = {
                "n_comment": int(meta[0]),
                "n_like": int(meta[1]),
                "n_dislike": int(meta[2]),
            }
        return metadata

    @classmethod
    def parse(cls, document):
        soup = BeautifulSoup(document, "lxml", from_encoding="utf-8")
        tags = soup.select(selector="div.min > div.section")
        jokes = list()
        for tag in tags:
            joke = Joke()
            joke.author = cls.find_extract_tag_attribute(tag, cls.config["author"])
            joke.avatar = cls.find_extract_tag_attribute(tag, cls.config["avatar"])
            joke.pb_site = u"嘻嘻哈哈"
            joke.content = cls.find_extract_tag_attribute(tag, cls.config["content"])
            _id = cls.find_extract_tag_attribute(tag, cls.config["id"])
            joke.id = _id.replace("comment-", "")
            jokes.append(joke)
        metadata = cls.fetch_metadata([joke.id for joke in jokes])
        for joke in jokes:
            meta = metadata[joke.id]
            joke.n_comment = meta["n_comment"]
            joke.n_like = meta["n_like"]
            joke.n_dislike = meta["n_dislike"]
            joke.comment_need["code"] = joke.id
            del joke.id
        return jokes

class JokePengFu(JokeBase):

    config = {
        "id": { "method": "select", "attribute": "id"},
        "title": {"params": {"selector": "h1.dp-b > a"}, "method": "select"},
        "content": {"params": {"selector": "div.content-img"}, "method": "select"},
        "author": {"params": {"selector": "p.user_name_list > a"}, "method": "select"},
        "avatar": {"params": {"selector": "a.mem-header > img"}, "attribute": "src", "method": "select"},
        "n_like": {"params": {"selector": "span.ding em"}, "method": "select"},
        "n_dislike": {"params": {"selector": "span.cai em"}, "method": "select"},
        "n_comment": {"params": {"selector": "span.commentClick em"}, "method": "select"},

    }

    @staticmethod
    def find_extract_tag_attribute(tag, params):
        if params.get("params"):
            tag = find_tag(tag, params)
        attribute = params.get("attribute", "text")
        return extract_tag_attribute(tag, attribute)


    @classmethod
    def parse(cls, document):
        soup = BeautifulSoup(document, "lxml", from_encoding="utf-8")
        tags = soup.select(selector="div.list-item")
        jokes = list()
        for tag in tags:
            joke = Joke()
            joke.title = cls.find_extract_tag_attribute(tag,cls.config["title"])
            joke.author = cls.find_extract_tag_attribute(tag, cls.config["author"])
            joke.avatar = cls.find_extract_tag_attribute(tag, cls.config["avatar"])
            joke.pb_site = u"捧腹网"
            joke.content = cls.find_extract_tag_attribute(tag, cls.config["content"])
            joke.n_comment = cls.find_extract_tag_attribute(tag,cls.config["n_comment"])
            joke.n_like = cls.find_extract_tag_attribute(tag,cls.config["n_like"])
            joke.n_dislike = cls.find_extract_tag_attribute(tag,cls.config["n_dislike"])
            joke.comment_need["code"] = cls.find_extract_tag_attribute(tag,cls.config["id"])
            jokes.append(joke)
        return jokes

class JokeWaDuanZi(JokeBase):

    config = {
        "title": {"params": {"selector": "h2.item-title > a"}, "method": "select"},
        "content": {"params": {"selector": "div.item-content"}, "method": "select"},
        "author": {"params": {"selector": "div.post-author > a"}, "method": "select"},
        "avatar": {"params": {"selector": "div.post-author > img"}, "attribute": "src", "method": "select"},
        "n_like": {"params": {"selector": "div.item-toolbar > ul > li:nth-of-type(1) > a"}, "method": "select"},
        "n_dislike": {"params": {"selector": "div.item-toolbar > ul > li:nth-of-type(2) > a"}, "method": "select"},

    }

    @staticmethod
    def find_extract_tag_attribute(tag, params):
        if params.get("params"):
            tag = find_tag(tag, params)
        attribute = params.get("attribute", "text")
        return extract_tag_attribute(tag, attribute)


    @classmethod
    def parse(cls, document):
        soup = BeautifulSoup(document, "lxml", from_encoding="utf-8")
        tags = soup.select(selector="div.post-item")
        jokes = list()
        for tag in tags:
            joke = Joke()
            joke.title = cls.find_extract_tag_attribute(tag,cls.config["title"])
            joke.author = cls.find_extract_tag_attribute(tag, cls.config["author"])
            joke.avatar = cls.find_extract_tag_attribute(tag, cls.config["avatar"])
            joke.pb_site = u"挖段子"
            joke.content = cls.find_extract_tag_attribute(tag, cls.config["content"])
            joke.n_comment = 0
            joke.n_like = cls.find_extract_tag_attribute(tag,cls.config["n_like"])
            n_dislike = cls.find_extract_tag_attribute(tag,cls.config["n_dislike"])
            joke.n_dislike = abs(int(n_dislike))
            jokes.append(joke)
        return jokes
