joke_spider 脚本说明

## 功能

抓取以下四个数据源的信息，存入Mongo数据库，并上传到pg

+ 网易-段子
+ 内涵段子-文字
+ 嘻嘻哈哈
+ 糗事百科-文字


## 依赖

+ bs4
+ requests

## 代码 intro

+ `spiders.py` 段子抓取代码
+ `comments.py` 段子评论抓取代码
+ `main.py`  主代码，逻辑控制，启动代码

## 测试运行

`$python main.py`

**注意**：目前单线操作，如果需要可以将段子抓取和评论抓取分开。