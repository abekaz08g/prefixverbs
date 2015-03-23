#!-*- coding: utf-8 -*-

from pymongo import MongoClient
from bson.objectid import ObjectId


def getObjectIdfromTime(gen_time):
    """
    用途：
        西暦の日付からObjectIdを生成
    引数：
        gen_time: datetimeオブジェクト。例：datetime.datetime(2010, 1, 1)

    戻り値：
        oid: ObjectId
    """
    oid = ObjectId.from_datetime(gen_time)
    return oid


def connect(conf):
    """
    用途：
        データベースに接続し、必要とするコレクションを返す。
    引数:
        conf: データベース接続用の設定。データベース名、コレクション名を指定。
            形式
            {'dbname': database name, 'colname': collection name}
    戻り値:
        col: 接続するコレクションを返す。
    """
    c = MongoClient()
    col = c[conf['dbname']][conf['colname']]
    return col


def setIdfield(srccol, dstcol, startid=None):
    """
    用途：
        データベースコレクションのスナップショットを撮る範囲を規定する。
    引数：
        srccol: スナップショットを撮るデータベースコレクション
        dstcol: スナップショットを保存するデータベースコレクション
        startid: 指定されている場合，このドキュメントidを持つドキュメンとから
            開始
    戻り値：
        なし
    """
    endid = srccol.find({}, {u'_id': 1})[srccol.count()-1]
    idfield = {
        u'field': u'idfield',
        u'startid': startid,
        u'endid': endid
    }
    dstcol['head'].insert(idfield)


class snapshot:
    """
    用途：
        データベースコレクションのスナップショットを撮るためのクラス。
    """
    def __init__(self, srccol, dstcol, start=None):
        """
        用途：
            snapshotクラスのコンストラクタメソッド
        引数：
            srccol: スナップショットを撮るデータベースコレクション
            dstcol: スナップショットを保存するデータベースコレクション
            start: 指定されている場合，このドキュメントidを持つドキュメンとから
                開始
        戻り値：
            なし
        """
        self.srccol = srccol
        self.dstcol = dstcol
        self.startid = start
        self.endid = srccol.find({}, {u'_id': 1})[srccol.count()-1]
        setIdfield(srccol, dstcol, start)

    def countItems(self, q):
        """
        用途：
            スナップショット対象のコレクションにおける、
            クエリqで記述された要素の数を数え、
            スナップショット保存コレクションのヘッダに保存する。
            （field: items）
        引数：
            q: クエリ。[{u'label': 要素ラベル, u'pat': 要素のmongodbクエリ}]
        戻り値：
            なし
        """
        for item in q:
            if self.startid is not None:
                q[u'_id'] = {u'$gt': self.startid, u'$lt': self.endid}
            else:
                q[u'_id'] = {u'$gt': self.startid, u'$lt': self.endid}
            itemsdoc = {u'field': u'items'}
            freq = self.srccol.find(q).count()
            itemsdoc[item[u'label']] = freq
            self.dstcol['head'].insert(itemsdoc)

    def extractItem(self, q, regpat, labelledItems):
        """
        用途：
            スナップショットを撮るコレクションから，特定の要素の例文idを抽出し
            スナップショット保存コレクションのボディに保存する。
        引数：
            q: クエリ。[{u'tags':[タグ集合], u'pat': 要素のmongodbクエリ}]
            regpat: 検索要素の正規表現パターン
        """
        clf = classifier(labelledItems)
        if self.startid is not None:
            q[u'_id'] = {u'$gt': self.startid, u'$lt': self.endid}
        else:
            q[u'_id'] = {u'$gt': self.startid, u'$lt': self.endid}
        sentences = self.srccol.find(q[u'pat'])
        classifySentences(clf, regpat, sentences, self.dstcol)


def classifySentences(classifier, regpat, sentences, dstcol):
    """
    用途：
        文集合を与えて，要素ごとに分類
    引数：
        classifier: 分類器
        regpat: 要素の正規表現パターン
        sentences: 文集合
        dstcol: 保存先コレクション
    """
    for sentence in sentences:
        for w in sentence[u'lemma']:
            r = regpat.search(w)
            if r:
                item = r.group(0)
                labels = classifier.classify(item)
                dstcol[u'body'].update(
                    {'item': item},
                    {
                        labels: labels,
                        u'$addToSet': {u'ids': sentence[u'_id']}
                    },
                    True
                )


class classifier:
    """
    用途：
        er-動詞を、形成規則別に分類
    コンストラクタ引数：
        labelledVerbs: {ラベル（A~Z）: [該当要素一覧], ...}
    """
    def __init__(self, labelledItems):
        """
        用途：
            コンストラクタ
        引数：
            labelledItems:  {ラベル（A~Z）: [該当要素一覧], ...}
        戻り値：
            なし
        """
        self.classTable = labelledItems

    def classify(self, item):
        """
        用途：
            当該要素に対応する形成規則を決定
        引数：
            item: 要素
        戻り値：
            labels: 形成規則のラベルのリスト。該当ラベルがない場合は空のリスト。
        """
        labels = []
        for label in self.classTable:
            if item in self.classTable(label):
                labels.append(label)
            else:
                pass
        return labels
