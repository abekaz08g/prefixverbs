#!-*- coding: utf-8 -*-

from pymongo import MongoClient
from bson.objectid import ObjectId
import json
import datetime
import re

G = {
    u'prefixverbs': u'prefixverbs',
    u'sentenceCollection': u'dzeit',
    u'documentCollection': u'docs'
}


def getObjectIdfromTime(gen_time):
    """
    用途：
        西暦の日付からObjectIdを生成
    引数：
        gen_time: 日付を表す独自アノテーション。u'2010-1-1'

    戻り値：
        oid: ObjectId
    """
    tl = [int(item) for item in gen_time.split(u'-')]
    time = datetime.datetime(tl[0], tl[1], tl[2])
    oid = ObjectId.from_datetime(time)
    return oid


def connect(dbname):
    """
    用途：
        データベースに接続する。
    引数:
        dbname: 接続するデータベース名
    戻り値:
        db: 接続するデータベース。
    """
    c = MongoClient()
    db = c[dbname]
    return db


def getMetaData(DB, start, end):
    """
    用途：
        コーパス検索時のヘッダ情報取得
    引数：
        DB: 新聞コーパスのデータベース
        start: データ取得開始日
        end: データ取得終了日
    戻り値：
        corpusStat: ドキュメント数、文の数、延べ語数、開始日、終了日
    """
    corpusStat = {
        u'start': start,
        u'end': end,
    }
    startId = getObjectIdfromTime(start)
    endId = getObjectIdfromTime(end)
    q = {
        u'_id': {
            u'$gt': startId,
            u'$lt': endId
        }
    }
    documents = DB[G[u'sentenceCollection']]['docs'].find(q).count()
    res = DB[G[u'sentenceCollection']].find(q)
    sentences = res.count()
    words = 0
    for item in res:
        words += len(item[u'surface'])
    corpusStat[u'documents'] = documents
    corpusStat[u'sentences'] = sentences
    corpusStat[u'words'] = words
    return corpusStat


class prefixverben:
    """
    用途：
        接頭辞動詞を取得するためのクラス。
    """
    def __init__(self, conf):
        """
        用途：
            設定ファイルconfを読み込みクラス変数をセットする。
            prefixverbenクラスのコンストラクタメソッド
        引数：
            conf: 設定ファイル。json形式
        戻り値：
            なし
        """
        f = open(conf)
        confd = json.load(f)
        self.sourceDB = confd['sourceDB']
        self.distDB = confd[u'distDB']
        self.classification = confd[u'classification']
        self.start = confd[u'start']
        self.end = confd[u'end']
        self.query = confd[u'query']
        self.cond = confd[u'cond']

    def setPVcol(self):
        """
        用途：
            接頭辞動詞コレクション(prefixverbs)の準備
        使用するクラス変数：
            distDB, classification
        戻り値：なし
        """
        db = connect(self.distDB)
        col = db[G[u'prefixverbs']]
        f = open(self.classification)
        pvdata = json.load(f)
        classificationType = pvdata[u'type']
        col.insert({u'type': classificationType})
        for key in pvdata[u'body']:
            bulk = []
            for v in pvdata[u'body'][key]:
                bulk.append({u'verb': v, u'class': key})
            col.insert(bulk)
        f.close()

    def getDataFromCorpus(self):
        """
        用途：
            コーパスから接頭辞動詞データを取得する
        使用するクラス変数：
            sourceDB, start, end, query, cond
        戻り値：
            res: mongoDBカーソルオブジェクト
        """
        soid = getObjectIdfromTime(self.start)
        eoid = getObjectIdfromTime(self.end)
        term = {u'$gt': soid, u'$lt': eoid}
        self.query[u'_id'] = term
        self.query[u'lemma'] = re.compile(self.query[u'lemma'])
        db = connect(self.sourceDB)
        col = db[G[u'sentenceCollection']]
        res = col.find(self.query, self.cond)
        return res

    def saveDataToSnapshot(self, res):
        """
        用途：
            データの保存。
            ヘッダ情報を取得し、保存。
            接頭辞動詞データそのものの保存。
        使用するクラス変数：
            start, end, query, cond, distDB, sourceDB
        戻り値：
            なし
        """
        distDB = connect(self.distDB)
        sourceDB = connect(self.sourceDB)
        metaData = getMetaData(sourceDB, self.start, self.end)
        distDB[G[u'prefixverbs']][u'head'].insert(metaData)
        pvs = []
        for item in res:
            for l in item[u'lemma']:
                sobj = self.query[u'lemma'].searc(l)
                if sobj:
                    pvs.append(sobj.group(0), item[u'_id'])
        print len(pvs)
