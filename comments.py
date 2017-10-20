# coding:utf-8

import logging
import json
import hashlib
from types import UnicodeType
from datetime import datetime
import requests
from w3lib.encoding import html_to_unicode
from pymongo.errors import DuplicateKeyError


class JokeComment(object):
    def __init__(self):
        self.joke = None
        self.author = None
        self.avatar = None
        self.content = None
        self.n_like = 0

    def show(self):
        print("joke: %s" % self.joke)
        print("author: %s" % self.author)
        print("avatar: %s" % self.avatar)
        print("content: %s" % self.content)
        print("n_like: %s" % self.n_like)
        print("*" * 120)

    @staticmethod
    def unique(string):
        if isinstance(string, UnicodeType):
            string = string.encode("utf-8")
        return hashlib.md5(string).hexdigest()

    def store(self, collection):
        if not (self.author or self.avatar or self.content):
            logging.warn("joke-comment miss fields author: %s, avatar: %s content: %s"
                         % (self.author, self.avatar, self.content))
        document = dict()
        document["joke"] = self.joke
        document["author"] = self.author
        document["avatar"] = self.avatar
        document["content"] = self.content
        document["n_like"] = int(self.n_like)
        document["insert"] = datetime.utcnow()
        unique_text = "%s%s" % (document["author"], document["content"])
        document["unique"] = self.unique(unique_text)
        try:
            result = collection.insert_one(document)
        except DuplicateKeyError:
            pass
        except Exception as e:
            logging.error(e.message, exc_info=True)
        else:
            logging.info("store joke-comment id: %s" % result.inserted_id)
            return result.inserted_id


class CommentBase(object):
    COMMENT_URL = None
    headers = {
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.86 Safari/537.36"}
    timeout = 30
    r_json = True
    skip = None
    joke = None

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
    def get_urls(cls, comment_need):
        return list()

    @classmethod
    def parse(cls, doc):
        comments = list()
        for item in doc:
            comment = JokeComment()
            comment.author = None
            comment.avatar = None
            comment.content = None
            comment.n_like = None
            comment.joke = cls.joke
            comments.append(comment)
        return comments

    @classmethod
    def run(cls, joke_id, comment_need):
        cls.joke = str(joke_id)
        urls = cls.get_urls(comment_need)
        comments = list()
        for url in urls:
            document = cls.download(url, c_json=cls.r_json, skip=cls.skip, headers=cls.headers)
            comments_ = cls.parse(document)
            comments.extend(comments_)
        logging.info("%s: %s" % (cls.__name__, len(comments)))
        return comments


class CommentXiHa(CommentBase):
    COMMENT_URL = "http://dg.xxhh.com/api/v2/getComment.php?id={id}&sid=joke&p=1&limit=100&__jsonp__=fn"
    skip = (3, -1)

    @classmethod
    def get_urls(cls, comment_need):
        urls = list()
        comment_url = cls.COMMENT_URL.format(id=comment_need["code"])
        urls.append(comment_url)
        return urls

    @classmethod
    def parse(cls, doc):
        data = doc.get("c", [])
        comments = list()
        for item in data:
            comment = JokeComment()
            comment.author = item.get("mn", "匿名")
            comment.avatar = item.get("ml")
            comment.content = item.get("c")
            comment.n_like = item.get("fl",0)
            comment.joke = cls.joke
            comments.append(comment)
        return comments


class CommentNeihan(CommentBase):
    COMMENT_URL = "http://neihanshequ.com/m/api/get_essay_comments/?group_id={g_id}&offset={offset}"
    LIMIT = 20

    @classmethod
    def get_urls(cls, comment_need):
        pages = (int(comment_need["comment_count"]) + cls.LIMIT - 1) / cls.LIMIT
        urls = [cls.COMMENT_URL.format(g_id=comment_need["code"], offset=page * cls.LIMIT) for page in range(pages)]
        return urls

    @classmethod
    def parse(cls, doc):
        data = doc["data"].get("recent_comments")
        comments = list()
        for item in data:
            comment = JokeComment()
            comment.joke = cls.joke
            comment.author = item.get("user_name", "匿名")
            comment.avatar = item.get("avatar_url")
            comment.content = item.get("text")
            comment.n_like = item.get("digg_count", 0)
            comments.append(comment)
        return comments


class CommentNetEase(CommentBase):
    COMMENT_URL = "http://comment.api.163.com/" \
                  "api/v1/products/a2869674571f77b5a0867c3d71db5856/" \
                  "threads/{_id}/app/comments/newList?" \
                  "offset={offset}&limit={limit}"
    LIMIT = 40

    @classmethod
    def get_urls(cls, comment_need):
        _id = comment_need["code"]
        url = cls.COMMENT_URL.format(_id=_id,
                                     offset=0,
                                     limit=cls.LIMIT)
        result = cls.download(url, c_json=True)
        if not result: return list()
        count = result["newListSize"]
        pages = (count + cls.LIMIT - 1) / cls.LIMIT
        urls = [cls.COMMENT_URL.format(_id=comment_need["code"],
                                       offset=page * cls.LIMIT,
                                       limit=cls.LIMIT)
                for page in range(pages)]
        return urls

    @classmethod
    def parse(cls, doc):
        ids = set()
        for _id in doc.get("commentIds", []):
            ids.add(_id.split(",")[0])
        _comments = doc.get("comments", {})
        comments = list()
        for _id in ids:
            comment = JokeComment()
            comment.joke = cls.joke
            comment_ = _comments.get(_id)
            nickname = comment_["user"].get("nickname")
            comment.author = nickname if nickname else None
            logo = comment_["user"].get("avatar")
            if logo and "netease.com" in logo and "noface" in logo:
                logo = None
            comment.avatar = logo if logo else None
            vote = comment_.get("vote")
            comment.n_like = int(vote) if vote else 0
            comment.content = comment_["content"]
            comments.append(comment)
        return comments


class CommentPengfu(CommentBase):
    COMMENT_URL = "http://api1.pengfu.com/humor/getComments?id={id}"
    @classmethod
    def get_urls(cls, comment_need):
        urls = list()
        comment_url = cls.COMMENT_URL.format(id=comment_need["code"])
        urls.append(comment_url)
        return urls

    @classmethod
    def parse(cls, doc):
        data = doc.get("data", [])
        comments = list()
        for item in data:
            comment = JokeComment()
            comment.author = item.get("name", "匿名")
            comment.avatar = item.get("avatar")
            content = item.get("content_json")
            if not content:continue
            comment.content = content[0].get("comment_content")
            comment.n_like = item.get("like",0)
            comment.joke = cls.joke
            comments.append(comment)
        return comments


if __name__ == "__main__":
    comments = CommentNetEase.run("neihan", {"code": "CE2OUGTG9001UGTH"})
    for comment in comments:
        comment.show()
