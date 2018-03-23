import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from pandas.plotting import andrews_curves, radviz

def test_curves():
    # data = pd.read_csv('e:/tmp/22/iris.csv')
    data = pd.read_csv('e:/tmp/22/201404.out')
    data.head()

    plt.figure()
    andrews_curves(data, 'Name')
    plt.show()

def test_radviz():
    # data = pd.read_csv('e:/tmp/22/iris.csv')
    data = pd.read_csv('e:/tmp/22/201404.out')
    print data.head()

    plt.figure()
    radviz(data, 'Name')
    plt.show()

if __name__ == '__main__':
    test_curves()
    # test_radviz()