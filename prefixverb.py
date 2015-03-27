#!-*- coding: utf-8 -*-

import pymongo
from bson.objectid import ObjectId
import json
import datetime
import re


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
    try:
        c = pymongo.MongoClient()
    except:
        c = pymongo.Connection('localhost', 27017)

    db = c[dbname]
    return db


def getMetaData(SentCol, DocCol, start, end):
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
    documents = DocCol.find(q).count()
    res = SentCol.find(q)
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
        self.SrcCol = conf['SrcCol']
        self.DistCol = conf[u'DistCol']
        self.DocCol = conf[u'DocCol']
        self.HeadCol = conf[u'HeadCol']
        self.classification = conf[u'classification']
        self.start = conf[u'start']
        self.end = conf[u'end']
        self.query = conf[u'query']
        self.cond = conf[u'cond']

    def setPVcol(self):
        """
        用途：
            接頭辞動詞コレクション(prefixverbs)の準備
        使用するクラス変数：
            distDB, classification
        戻り値：なし
        """
        f = open(self.classification).read().decode('UTF-8')
        pvdata = json.loads(f)
        classificationType = pvdata[u'type']
        self.DistCol.insert({u'type': classificationType})
        for key in pvdata[u'body']:
            bulk = []
            for v in pvdata[u'body'][key]:
                bulk.append({u'verb': v, u'class': key})
            self.DistCol.insert(bulk)

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
        res = self.SrcCol.find(self.query, self.cond)
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
        metaData = getMetaData(self.SrcCol, self.DocCol, self.start, self.end)
        self.HeadCol.insert(metaData)
        i = 0
        for item in res:
            for l in item[u'lemma']:
                sobj = self.query[u'lemma'].search(l)
                if sobj:
                    i += 1
                    verb = sobj.group(0)
                    q = {u'verb': verb}
                    opr = {u'$addToSet': {u'sids': item[u'_id']}}
                    self.DistCol.update(q, opr, True)
        self.HeadCol.update(
            {}, {u'$set': {u'pvcount': i}}, False, True
        )
