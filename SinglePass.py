# encoding=utf-8
import jieba
from mssqlconn import *
from numpy import *
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import TfidfVectorizer
import sys
reload(sys)
sys.setdefaultencoding('utf8')

ms=MSSQL(host='xxx.xxx.xxx.xxx', user='xxx', pwd='xxx', db='TextMining')
stopwords = [line.strip().decode('utf-8') for line in open('stopwords.txt').readlines()]
theta=0.5
xClusterID=1

def fenci(inTxtLst):
    retTxtLst=[]
    for line in inTxtLst:
        line=[ln.decode('utf-8') for ln in line.splitlines() if ln.strip()]
        strline=''.join(line)
        seglist = jieba.cut(strline, cut_all=False)  # 精确模式
        lst = list(seglist)
        for seg in lst[:]:#注意这里要使用切片，不然删除了元素之后，index改变
            if seg in stopwords:
                lst.remove(seg)
        output = ' '.join(list(lst))  # 空格拼接
        retTxtLst.append(output)
    return retTxtLst




def getTfidfMat(lst):#测试函数
    # 将文本中的词语转换为词频矩阵 矩阵元素a[i][j] 表示j词在i类文本下的词频
    vectorizer = CountVectorizer()
    # 该类会统计每个词语的tf-idf权值
    transformer = TfidfTransformer()
    # 第一个fit_transform是计算tf-idf 第二个fit_transform是将文本转为词频矩阵
    tfidf = transformer.fit_transform(vectorizer.fit_transform(lst))
    # 获取词袋模型中的所有词语
    word = vectorizer.get_feature_names()
    # 将tf-idf矩阵抽取出来，元素w[i][j]表示j词在i类文本中的tf-idf权重
    weight = tfidf.toarray()
    #词频cp=vectorizer.fit_transform(lst)
    #词频cp=cp.toarray()
    # for i in range(len(weight)):
    #     for j in range(len(word)):
    #         print word[j],cp[i][j],'#',
    #     print '\n'
    return weight


if __name__ == "__main__":

    #开始single-pass部分
    try:
        resList = ms.ExecQuery("SELECT ID,content FROM corpora WHERE isProcessed=0 AND SourceType='News'")
        for (ID0, content0) in resList:#读出未分类的新闻
            ####更新weightMat
            corpus = []
            trClusterID = []
            try:
                resList = ms.ExecQuery("SELECT ID,content,ClusterID,isProcessed FROM corpora WHERE isProcessed=1")
                for (ID1, content1, ClusterID, isProcessed) in resList:
                    corpus.append(content1)
                    trClusterID.append(ClusterID)
            except:
                print '\nSome error/exception occurred.x'

            segedTxtlst = fenci(corpus)
            vectorizer = TfidfVectorizer()
            trainTfidf = vectorizer.fit_transform(segedTxtlst)
            weightMat = trainTfidf.toarray()  # 得到语料库的VSM
            ####更新weightMat结束

            temContent=[]
            temContent.append(content0)
            segedInLst = fenci(temContent)#对该新闻分词
            testTfidf = vectorizer.transform(segedInLst)
            testVec = testTfidf.toarray()#得到基于tf-idf的文档向量
            # 计算testVec和weightMat每一行的余弦相似度
            xx = cosine_similarity(testVec, weightMat)
            ndxx=array(xx)
            max=ndxx.max()
            if(max>theta):
                indxx=argmax(ndxx)#最大值在weightMat的index已经找到
                ms.ExecNonQuery("UPDATE corpora set ClusterID='%s',isProcessed=1 WHERE ID=%s"%(trClusterID[indxx],ID0))
            else:#
                # 不大于某阈值就新建一个分类
                ms.ExecNonQuery("UPDATE corpora set ClusterID='%s',isProcessed=1 WHERE ID=%s"%(xClusterID,ID0))
                xClusterID+=1
            #已经把一条新闻聚到某个簇了，下面要更新一下weightMat

    except:
        print '\nSome error/exception occurred.y'
    #single-pass部分结束


