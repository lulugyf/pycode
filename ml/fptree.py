#coding=utf-8

# 使用 FP-growth 算法做关联分析
# 套餐数据获取环境: 10.113.183.41 /conherence/siteqi/g

import os, sys

#cat 00.txt|awk '{print $2}'|sort|uniq -c|awk '{if($1 > 999)print $0}' >idx.2
# 产品生效日期集中度查看： cat 00.txt|awk '{print $3}'|sort|uniq -c|sort -n -k 2
#       看到的结果是比较集中在 201308 ~ 201405 之间， 所以处理关联时只取两个时间点之间的数据

def merge_records(fname, codefile='prod.list', outfile='00.out'):
    # 过滤出现最少1000的代码, 其它的扔掉
    cc = {}
    for l in file(codefile):
        f = l.strip().split()
        cc[f[1]] = int(f[0])
    d = {}
    for l in file(fname):
        f = l.strip().split()
        if len(f) != 4: continue
        id, code, edate = f[0], f[1], int(f[2])
        if edate < 201308 or edate > 201405: # 只要在这两个日期间订购的
            continue
        if not cc.has_key(code):
            continue
        if d.has_key(id):
            d[id].append(code)
        else:
            d[id] = [code,]
    fo = file(outfile, "w")
    for k, v in d.items():
        #print >>fo, k, " ".join(v)
        print >> fo, " ".join(set(v))  #需要对记录剃重, 所以使用set
    fo.close()


class treeNode:
    def __init__(self, nameValue, numOccur, parentNode):
        self.name = nameValue
        self.count = numOccur
        self.nodeLink = None
        self.parent = parentNode  # needs to be updated
        self.children = {}

    def inc(self, numOccur):
        self.count += numOccur

    def disp(self, ind=1):
        print '  ' * ind, self.name, ' ', self.count
        for child in self.children.values():
            child.disp(ind + 1)

def createTree(dataSet, minSup=1):  # create FP-tree from dataset but don't mine
    headerTable = {}
    # go over dataSet twice
    for trans in dataSet:  # first pass counts frequency of occurance
        for item in trans:
            headerTable[item] = headerTable.get(item, 0) + dataSet[trans]
    for k in headerTable.keys():  # remove items not meeting minSup
        if headerTable[k] < minSup:
            del (headerTable[k])
    freqItemSet = set(headerTable.keys())
    # print 'freqItemSet: ',freqItemSet
    if len(freqItemSet) == 0: return None, None  # if no items meet min support -->get out
    for k, v in headerTable.items():
        headerTable[k] = [v, None]  # reformat headerTable to use Node link
    print 'headerTable count: ', len(headerTable)
    print 'dataSet count: ', len(dataSet)
    retTree = treeNode('Null Set', 1, None)  # create tree
    i = 0
    for tranSet, count in dataSet.items():  # go through dataset 2nd time
        i += 1
        if i % 1000 == 0:
            print 'ddd ', i, time.ctime(time.time())
        localD = {}
        for item in tranSet:  # put transaction items in order
            if item in freqItemSet:
                localD[item] = headerTable[item][0]
        if len(localD) > 0:
            orderedItems = [v[0] for v in sorted(localD.items(), key=lambda p: p[1], reverse=True)]
            updateTree(orderedItems, retTree, headerTable, count)  # populate tree with ordered freq itemset
    return retTree, headerTable  # return tree and header table

def updateTree(items, inTree, headerTable, count):
    i0 = items[0]
    if i0 in inTree.children:  # check if orderedItems[0] in retTree.children
        inTree.children[i0].inc(count)  # incrament count
    else:  # add items[0] to inTree.children
        inTree.children[i0] = treeNode(i0, count, inTree)
        if headerTable[i0][1] == None:  # update header table
            headerTable[i0][1] = inTree.children[i0]
        else:
            updateHeader(headerTable[i0][1], inTree.children[i0])
    if len(items) > 1:  # call updateTree() with remaining ordered items
        updateTree(items[1::], inTree.children[i0], headerTable, count)


def updateHeader(nodeToTest, targetNode):  # this version does not use recursion
    while (nodeToTest.nodeLink != None):  # Do not use recursion to traverse a linked list!
        nodeToTest = nodeToTest.nodeLink
    nodeToTest.nodeLink = targetNode


def ascendTree(leafNode, prefixPath):  # ascends from leaf node to root
    if leafNode.parent != None:
        prefixPath.append(leafNode.name)
        ascendTree(leafNode.parent, prefixPath)


def findPrefixPath(basePat, treeNode):  # treeNode comes from header table
    condPats = {}
    while treeNode != None:
        prefixPath = []
        ascendTree(treeNode, prefixPath)
        if len(prefixPath) > 1:
            condPats[frozenset(prefixPath[1:])] = treeNode.count
        treeNode = treeNode.nodeLink
    return condPats


def mineTree(inTree, headerTable, minSup, preFix, freqItemList):
    bigL = [v[0] for v in sorted(headerTable.items(), key=lambda p: p[1])]  # (sort header table)
    for basePat in bigL:  # start from bottom of header table
        newFreqSet = preFix.copy()
        newFreqSet.add(basePat)
        # print 'finalFrequent Item: ',newFreqSet    #append to set
        freqItemList.append(newFreqSet)
        condPattBases = findPrefixPath(basePat, headerTable[basePat][1])
        # print 'condPattBases :',basePat, condPattBases
        # 2. construct cond FP-tree from cond. pattern base
        myCondTree, myHead = createTree(condPattBases, minSup)
        # print 'head from conditional tree: ', myHead
        if myHead != None:  # 3. mine cond. FP-tree
            # print 'conditional tree for: ',newFreqSet
            # myCondTree.disp(1)
            mineTree(myCondTree, myHead, minSup, newFreqSet, freqItemList)


def loadSimpDat():
    simpDat = [['r', 'z', 'h', 'j', 'p'],
               ['z', 'y', 'x', 'w', 'v', 'u', 't', 's'],
               ['z'],
               ['r', 'x', 'n', 'o', 's'],
               ['y', 'r', 'x', 'z', 'q', 't', 'p'],
               ['y', 'z', 'x', 'e', 'q', 's', 't', 'm']]
    return simpDat

def loadFileDataSet(fname):
    dat = []
    for l in file(fname):
        f = l.strip().split()
        dat.append(f[1:])
    print 'file loaded.'
    return dat


def createInitSet(dataSet):
    retDict = {}
    for trans in dataSet:
        f = frozenset(trans)
        retDict[f] = retDict.get(f, 0) + 1
    print 'init set finished.'
    return retDict

import time
if __name__ == '__main__':
    #os.chdir("e:/tmp/00")
    #merge_records('00.txt', 'idx.2', '00.out')
    #print 'done1'

    dataSet = loadSimpDat()
    #dataSet = loadFileDataSet("001.out")
    dataSet = createInitSet(dataSet)
    file("x", "w").write(repr(dataSet))
    print 'begin createTree', time.ctime(time.time())
    tree, headerTable = createTree(dataSet, 3)   # 20000 records, 3=52s  30=50s
    print 'createTree finished.', time.ctime(time.time())
    tree.disp()

    #print findPrefixPath('r', headerTable['r'][1])

    freqItems = []
    mineTree(tree, headerTable, 3, set([]), freqItems)
    print freqItems