#!-*- coding: utf-8 -*-

from prefixverb import connect
from prefixverb import PrefixVerbs
import os.path


"""
新聞コーパスの文データ（dzeit），文書データ（dzeit.docs）を利用する
"""
sourceDB = connect(u'web')
SrcCol = sourceDB[u'dzeit']
DocCol = sourceDB[u'dzeit'][u'docs']

"""
er-動詞使用データのスナップショットを保存するためのデータベースを
新規に用意する。
"""
distDB = connect(u"erverben_141101_150228")
DistCol = distDB[u'prefixverbs']
HeadCol = distDB[u'head']

"""
接頭辞動詞の分類テーブルは外出し。絶対パスで指定。
"""
classTableName = u'labelledErverben.json'
classTableDir = u'test'
script_dir = os.path.abspath(os.path.dirname(__file__))
classTablePath = os.path.join(script_dir, classTableDir, classTableName)

"""
処理用の設定
"""
conf = {
    u'SrcCol': SrcCol,
    u'DocCol': DocCol,
    u'DistCol': DistCol,
    u'HeadCol': HeadCol,
    u'classification': classTablePath,
    u'start': u'2014-11-01',
    u'end': u'2015-02-28',
    u'query': {
        u'lemma': u'^er.+n'
    },
    u'cond': {u'_id': 1, u'lemma': 1}
}

pv = PrefixVerbs(conf)
pv.setPVcol()
res = pv.getDataFromCorpus()
pv.saveDataToSnapshot(res)
