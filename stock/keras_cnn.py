#encoding=utf8

'''Trains a simple convnet on the MNIST dataset.
Gets to 99.25% test accuracy after 12 epochs
(there is still a lot of margin for parameter tuning).
16 seconds per epoch on a GRID K520 GPU.
'''

from __future__ import print_function
import keras
from keras.datasets import mnist
from keras.models import Sequential
from keras.layers import Dense, Dropout, Flatten
from keras.layers import Conv2D, MaxPooling2D
from keras import backend as K
from keras.models import model_from_json
import numpy as np

import tensorflow as tf


def conf_session():
    from keras.backend.tensorflow_backend import set_session
    config = tf.ConfigProto()
    #config.gpu_options.per_process_gpu_memory_fraction = 0.5
    config.gpu_options.allow_growth = True
    set_session(tf.Session(config=config))

def save_model(model, model_file="model.json", weights_file="model.h5"):
    # serialize model to JSON
    model_json = model.to_json()
    with open(model_file, "w") as json_file:
        json_file.write(model_json)
    # serialize weights to HDF5
    model.save_weights(weights_file)
    print("Saved model to disk")

def load_model(model_file="model.json", weights_file="model.h5"):
    # load json and create model
    with open(model_file, 'r') as json_file:
        loaded_model_json = json_file.read()
    loaded_model = model_from_json(loaded_model_json)
    # load weights into new model
    loaded_model.load_weights(weights_file)
    print("Loaded model from disk")

    # evaluate loaded model on test data
    loaded_model.compile(loss='binary_crossentropy', optimizer='rmsprop', metrics=['accuracy'])
    #score = loaded_model.evaluate(X, Y, verbose=0)
    #print("%s: %.2f%%" % (loaded_model.metrics_names[1], score[1] * 100))
    return loaded_model

def create_model(input_shape, num_classes):
    model = Sequential()
    model.add(Conv2D(32, kernel_size=(3, 3), activation='relu', input_shape=input_shape))
    model.add(Conv2D(64, (3, 3), activation='relu'))
    model.add(MaxPooling2D(pool_size=(2, 2), dim_ordering="th"))
    model.add(Dropout(0.25))
    model.add(Flatten())
    model.add(Dense(128, activation='relu'))
    model.add(Dropout(0.5))
    model.add(Dense(num_classes, activation='softmax'))
    return model

def load_datasets(pickle_file):
    from six.moves import cPickle as pickle
    # import os
    # os.chdir("e:/stock/out11")
    # pickle_file = 'datasets.pickle'
    with open(pickle_file, 'rb') as f:
        save = pickle.load(f)
    return save['train_x'], save['train_y'], save['test_x'], save['test_y']


def main():
    conf_session()
    batch_size = 1024
    num_classes = 12
    epochs = 12

    # input image dimensions
    img_rows, img_cols = 5, 60
    # the data, shuffled and split between train and test sets
    #(x_train, y_train), (x_test, y_test) = mnist.load_data("e:/00/mnist.npz")
    x_train,y_train,x_test,y_test = load_datasets('e:/stock/out11/datasets1.pickle')

    x_train = x_train.reshape(x_train.shape[0], img_rows, img_cols, 1)
    x_test = x_test.reshape(x_test.shape[0], img_rows, img_cols, 1)
    input_shape = (img_rows, img_cols, 1)

    #x_train = x_train.astype('float32')
    #x_test = x_test.astype('float32')

    print('x_train shape:', x_train.shape)
    print(x_train.shape[0], 'train samples')
    print(x_test.shape[0], 'test samples')
    # convert class vectors to binary class matrices
    y_train = keras.utils.to_categorical(y_train, num_classes)
    y_test = keras.utils.to_categorical(y_test, num_classes)

    #model = create_model(input_shape, num_classes)
    model = load_model('e:/stock/out11/model0.json', 'e:/stock/out11/weights0.h5')
    model.compile(loss=keras.losses.categorical_crossentropy, optimizer=keras.optimizers.Adadelta(), metrics=['accuracy'])
    #model.fit(x_train, y_train, batch_size=batch_size, epochs=epochs, verbose=1, validation_data=(x_test, y_test))
    score = model.evaluate(x_test, y_test, verbose=0)
    print('Test loss:', score[0])
    print('Test accuracy:', score[1])

    #save_model(model, 'e:/stock/out11/model0.json', 'e:/stock/out11/weights0.h5')


def mytest():
    conf_session()
    batch_size = 1024
    num_classes = 12
    epochs = 12

    # input image dimensions
    img_rows, img_cols = 5, 60
    # the data, shuffled and split between train and test sets
    # (x_train, y_train), (x_test, y_test) = mnist.load_data("e:/00/mnist.npz")
    x_train, y_train, x_test, y_test = load_datasets('e:/stock/out11/datasets1.pickle')

    x_train = x_train.reshape(x_train.shape[0], img_rows, img_cols, 1)
    x_test = x_test.reshape(x_test.shape[0], img_rows, img_cols, 1)
    y_train = keras.utils.to_categorical(y_train, num_classes)
    y_test = keras.utils.to_categorical(y_test, num_classes)

    model = load_model('e:/stock/out11/model0.json', 'e:/stock/out11/weights0.h5')
    model.compile(loss=keras.losses.categorical_crossentropy, optimizer=keras.optimizers.Adadelta(),
                  metrics=['accuracy'])
    y_predict = model.predict(x_train)
    y = y_predict * 100.0
    np.savetxt("e:/stock/y_predict.txt", y, fmt='%.2f', delimiter=' ', newline='\n')
    scores = model.evaluate(x_train, y_train, verbose=1)
    return model, x_train, y_train, x_test, y_test

# 还原类别
def from_categorical(y, mx=0.45):
    o = np.zeros(y.shape[0])
    for i in range(y.shape[0]):
        for j in range(y.shape[1]):
            if y[i, j] >= mx:
                o[i] = j
                break
    return o

def xx(y, x_train, y_train):
    nt = open("e:/stock/out11/train1.txt").readlines()
    fo = open("e:/stock/out11/train1.1.txt", "w")
    fo1 = open("e:/stock/out11/x.1.txt", "w")
    for i in range(y.shape[0]):
        if y[i] > 0:
            fo.write("%d %d %s"%(y_train[i], y[i], nt[i]))
            np.savetxt(fo1, x_train[i, 1, :, 0], fmt="%.4f", delimiter=' ', newline=' ')
            fo1.write(' %d %d\n'%(y_train[i], y[i]))
    fo.close()
    fo1.close()
def xx1(tpi=-1):
    import matplotlib.pyplot as plt
    fpath = 'e:/stock/out11/x.1.txt'
    dc = np.loadtxt(fpath)
    import random, time
    random.seed(time.time())
    rng = [i for i in range(dc.shape[0])]
    random.shuffle(rng)
    plt.ylim(-0.5, 0.5)
    colors = [0, 0, 0, 0, 0, 0, 'yellow', 'green', 'red', 'blue', 'black', 'orange']
    #                            6          7       8        9        10         11
    '''    if hr < -.3: return 1
    elif hr < -.2: return 2
    elif hr < -.1: return 3
    elif hr < -0.05: return 4
    elif hr < -0.02: return 5
    elif hr < 0.02: return 6
    elif hr < 0.05: return 7
    elif hr < 0.1: return 8
    elif hr < 0.2: return 9
    elif hr < 0.3: return 10
    else: return 11'''
    legend = {}
    for i in range(200):
        tp = int(dc[rng[i], tpi])
        #if tp != 11: continue
        if legend.get(tp, 0) == 0:
            plt.plot(dc[rng[i], :-2], color=colors[tp], label="%d"%tp)
            legend[tp] = 1
        else:
            plt.plot(dc[rng[i], :-2], color=colors[tp])
    plt.legend()
    plt.show()

'''
import sys
from importlib import reload
sys.path.insert(0, 'd:/worksrc/pycode/stock')
import keras_cnn as kc

'''
if __name__ == '__main__':
    main()