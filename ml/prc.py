#coding=utf-8

# 文件 9.1 来自订购资费数据， 脚本如下：
# cat 90|awk '{print $2}'|sort|uniq -c|awk '{if($1>500)print $0}' >9.1
# 返回以套餐key， 序号为value 的字典
def makeArr():
    arr = {}
    arr1 = []
    i = 0
    for line in file('9.1'):
        x = line.strip().split()
        if len(x) != 2:
            continue
        arr[x[1]] = i
        i += 1
        arr1.append(x[1])
    return arr, arr1

# 按 id_no 生成套餐为特征列的数据, 以给定时间为分隔， 某个套餐为类别
# 这是一个非平衡的特征数据
# 寻找一个数量比较多的prc+eff_date:
#     cat 90|sort|awk '{if($4=209912)print $0}'|awk '{print $3,$2}'|sort|uniq -c|sort -n
#    8445 ACAS04aetjn 201404
#    8563 ACAS04aetjn 201405
def makeDat(month, prc0, outfile):
    id_no0 = '0'
    arr0, arr1 = makeArr()
    fout = file(outfile, 'w')
    arr1.append('Name')
    fout.write(','.join(arr1)); fout.write('\n')
    for line in file('90.2'):
        x = line.strip().split()
        if len(x) != 4:
            continue
        id_no = x[0]; prc = x[1]; begin_month=int(x[2])
        if id_no != id_no0:
            if id_no0 != '0':
                fout.write(','.join(arr))
                fout.write('\n')
            id_no0 = id_no
            arr = ['0' for i in range(len(arr0)+1)]
            arr[-1] = '-1'
        if prc == prc0 and begin_month == month:
            arr[-1] = '1'
        elif begin_month < month:
            i = arr0.get(prc, -1)
            if i != -1:
                arr[i] = '1'
    fout.close()



from numpy import *
from svm import smoP, kernelTrans
import random, time

def loadDataSet(fileName):
    dataMat = []; labelMat = []
    fr = open(fileName)
    for line in fr.readlines():
        lineArr = line.strip().split()
        l = len(lineArr)
        if l < 50: continue
        dataMat.append([float(lineArr[i]) for i in range(l-1)])
        labelMat.append(float(lineArr[-1]))
    #return dataMat, labelMat

    # 剔除数据， 全部数据太多， 内存不够用： 正例全保留， 反例随机保留正例数量2倍
    outDat = []; outLab = []
    ct = len(labelMat)
    for i in range(ct):
        if labelMat[i] > 0:
            outDat.append(dataMat[i])
            outLab.append(labelMat[i])
    random.seed(time.time())
    for n in range(len(outLab)*2):
        i = random.randint(0, ct-1)
        outDat.append(dataMat[i])
        outLab.append(labelMat[i])
    return outDat, outLab

def saveSVM(svInd, sVs, labelSV, alphasSV):
    fout = file("svm.dat", "w")
    fout.write(repr(svInd))
    fout.write("\n\n")
    fout.write(repr(sVs))
    fout.write("\n\n")
    fout.write(repr(labelSV))
    fout.write("\n\n")
    fout.write(repr(alphasSV))
    fout.write("\n\n")
    fout.close()

def testPrc(kTup=('rbf', 10)):
    dataArr, labelArr = loadDataSet('201404')
    print 'load data done'
    b, alphas = smoP(dataArr, labelArr, 200, 0.0001, 10000, kTup)
    datMat = mat(dataArr);
    labelMat = mat(labelArr).transpose()
    svInd = nonzero(alphas.A > 0)[0]
    sVs = datMat[svInd]
    labelSV = labelMat[svInd];
    print "there are %d Support Vectors" % shape(sVs)[0]
    saveSVM(svInd, sVs, labelSV, alphas[svInd])  #保存支持向量
    m, n = shape(datMat)
    errorCount = 0
    for i in range(m):
        kernelEval = kernelTrans(sVs, datMat[i, :], kTup)
        predict = kernelEval.T * multiply(labelSV, alphas[svInd]) + b
        if sign(predict) != sign(labelArr[i]): errorCount += 1
    print "the training error rate is: %f" % (float(errorCount) / m)
# 全部数据太大， 难以绘图和分析， 取局部数据， 正例全部， 反例随机保留正例数量2倍
def random_seekData(fileName, outfile):
    dataMat = []
    fr = open(fileName)
    title = fr.readline()
    fo = file(outfile, "w")
    fo.write(title)
    vct = 0
    for line in fr.readlines():
        if line.endswith(',1\n'):
            fo.write(line)
            vct += 1
        else:
            dataMat.append(line)

    random.seed(time.time())
    ct = len(dataMat)
    for n in range(vct*2):
        i = random.randint(0, ct-1)
        fo.write(dataMat[i])
    fr.close()
    fo.close()

if __name__ == '__main__':
    # makeDat(201404, 'ACAS04aetjn', '201404')  #这个作为训练数据
    # makeDat(201405, 'ACAS04aetjn', '201405')  # 这个作为测试数据
    random_seekData('201404', '201404.out')
    # testPrc()

