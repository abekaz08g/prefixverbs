#!-*- coding: utf-8 -*-

from pymongo import MongoClient
import re


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


class classifier:
    """
    用途：
        er-動詞を、形成規則別に分類
    コンストラクタ引数：
        labelledErverben: {ラベル（A~Z）: [該当er-動詞一覧], ...}
    """
    def __init__(self, labelledErverben):
        """
        用途：
            コンストラクタ
        引数：
            labelledErverben:  {ラベル（A~Z）: [該当er-動詞一覧], ...}
        戻り値：
            なし
        """
        self.classTable = labelledErverben

    def classify(self, erverb):
        """
        用途：
            当該er-動詞に対応する形成規則を決定
        引数：
            erverb: er-動詞
        戻り値：
            labels: 形成規則のラベルのリスト。該当ラベルがない場合は空のリスト。
        """
        labels = []
        for label in self.classTable:
            if erverb in self.classTable(label):
                labels.append(label)
            else:
                pass
        return labels


def classify(sentcol, erverbcol, q, classifier):
    """
    用途：
        コーパスの文コレクションを検索して、er-動詞が含まれる文を取得。
        得られたデータをer-動詞コレクションに保存
    引数：
        sentcol: コーパスの文コレクション
        erverbcol: er-動詞コレクション。er-動詞、ラベル、文idの集合
        q: mongodb検索メソッドfindの引数となるクエリ。検索文と出力指定からなる。
        classifier: classifierオブジェクト
    戻り値：
        なし
    """
    pat = re.compile(u'^er.+n$')
    res = sentcol.find(q)
    for item in res:
        for w in item[u'lemma']:
            r = pat.search(w)
            if r:
                erverb = r.group(0)
                labels = classifier.classify(erverb)
                erverbcol.update(
                    {'erverb': erverb},
                    {
                        labels: labels,
                        u'$addToSet': {u'ids': item[u'_id']}
                    },
                    True
                )


def getSentence(col, sid):
    """
    用途： 文idを指定して，文データを取得
    引数：
        col: 文コレクション
        sid: 文id
    戻り値：
        sentData: 文データ
    """
    sentData = col.find_one({u'_id': sid})
    return sentData


def getErverbenClass(sentCol, erverbLabels):


    

def run(conf, erverbLabels):
    """
    用途：
        コーパスからer-動詞が含まれる事例を取得して、
        ラベルごとに分類し、
        事例集をプリントアウト
    引数：
        conf: 出力ファイルや、データベースの設定など
    戻り値：

    """
    #er-動詞を検索→動詞、ラベル、文idのタプルからなるリスト
    erverbenClass = getErverbenClass(sentCol, erverbLabels)



    ラベルごとに、ヒット数でソート
    統計情報作成
    動詞ごとに文データ取得
    書き出し