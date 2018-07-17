import numpy as np
import matplotlib.pyplot as plt
from ctypes.wintypes import PINT

np.random.seed(0)
N = 100  # number of points per class
D = 2  # dimensionality
K = 3  # number of classes
X = np.zeros((N * K, D))  # 初始化0矩阵
# print('xiehy:',X,'--------')
y = np.zeros(N * K, dtype='uint8')
for j in range(K):
    ix = range(N * j, N * (j + 1))
    r = np.linspace(0.0, 1, N)  # radius
    t = np.linspace(j * 4, (j + 1) * 4, N) + np.random.randn(N) * 0.2  # theta
    X[ix] = np.c_[r * np.sin(t), r * np.cos(t)]  ##改变X的每一个值
    y[ix] = j

h = 100  # size of hidden layer
W = 0.01 * np.random.randn(D, h)  # x:300*2  2*100
b = np.zeros((1, h))
W2 = 0.01 * np.random.randn(h, K)
b2 = np.zeros((1, K))

# some hyperparameters
step_size = 1e-0  # 学习率
reg = 1e-3  # regularization strength正则化惩罚项目

# gradient descent loop
num_examples = X.shape[0]
# print(X.shape[0])
for i in range(5000):
    # evaluate class scores, [N x K]
    #  print(X,'--------',W)
    hidden_layer = np.maximum(0, np.dot(X, W) + b)  # note, ReLU activation hidden_layer:300*100 relu wx+
    # print(hidden_layer)
    # print('------\n',np.dot(X, W) ,b)
    ###########scores=relu(wx+b)*w2+b2
    scores = np.dot(hidden_layer, W2) + b2  # scores:300*3的分值
    # print(scores)
    # print(scores.shape)

    # compute the class probabilities
    exp_scores = np.exp(scores)  # e^x
    # print(exp_scores)
    probs = exp_scores / np.sum(exp_scores, axis=1, keepdims=True)  # [N x K] 每行每个值所占比例 sigmod
    # print probs.shape
    # print(probs)
    # compute the loss: average cross-entropy loss and regularization
    corect_logprobs = -np.log(probs[range(num_examples), y])  ##求log 上面指定了y对应的正确区域值
    #  print(y)
    #  print(probs[range(num_examples), y])
    # print(corect_logprobs)
    data_loss = np.sum(corect_logprobs) / num_examples  ## loss 均值
    # print(data_loss)
    reg_loss = 0.5 * reg * np.sum(W * W) + 0.5 * reg * np.sum(W2 * W2)
    loss = data_loss + reg_loss  ##损失值
    if i % 100 == 0:
        print("iteration %d: loss %f" % (i, loss))

    # compute the gradient on scores
    dscores = probs  ##simod后的概率值
    # print('----old')
    # print(dscores)
    dscores[range(num_examples), y] -= 1  ##正确的概率值-1
    # print('----1---')
    # print(dscores)
    dscores /= num_examples
    # print('------tidu--')
    # print(num_examples)
    # print(dscores)

    # backpropate the gradient to the parameters反向传播
    # first backprop into parameters W2 and b2
    # print(hidden_layer)
    # print('------qiudao--') ##矩阵自身求导就是行列转置
    # print(hidden_layer.T) relu(wx+b)*w2+b2
    dW2 = np.dot(hidden_layer.T, dscores)
    db2 = np.sum(dscores, axis=0, keepdims=True)  ## 对常数求导就是1 因此直接是梯度*1=梯度
    # print(dscores)
    # print(db2)
    # next backprop into hidden layer
    dhidden = np.dot(dscores, W2.T)
    # backprop the ReLU non-linearity
    dhidden[hidden_layer <= 0] = 0
    # finally into W,b
    dW = np.dot(X.T, dhidden)
    db = np.sum(dhidden, axis=0, keepdims=True)

    # add regularization gradient contribution
    dW2 += reg * W2
    dW += reg * W

    # perform a parameter update
    W += -step_size * dW
    b += -step_size * db
    W2 += -step_size * dW2
    b2 += -step_size * db2

###最后一次正向传播
hidden_layer = np.maximum(0, np.dot(X, W) + b)
scores = np.dot(hidden_layer, W2) + b2
# print('over')
# print(scores)
predicted_class = np.argmax(scores, axis=1)  # 返回沿轴axis最大值的索引。 0列 1 行
# print(predicted_class)
# print(y)
print('training accuracy: %.2f' % (np.mean(predicted_class == y)))  ##对正确的求均值

h = 0.02
x_min, x_max = X[:, 0].min() - 1, X[:, 0].max() + 1
y_min, y_max = X[:, 1].min() - 1, X[:, 1].max() + 1
xx, yy = np.meshgrid(np.arange(x_min, x_max, h),
                     np.arange(y_min, y_max, h))
Z = np.dot(np.maximum(0, np.dot(np.c_[xx.ravel(), yy.ravel()], W) + b), W2) + b2
Z = np.argmax(Z, axis=1)
Z = Z.reshape(xx.shape)
fig = plt.figure()
plt.contourf(xx, yy, Z, cmap=plt.cm.Spectral, alpha=0.8)
# c是颜色，s是形状大小 X[:,0]是numpy中数组的一种写法，表示对一个二维数组，取该二维数组第一维中的所有数据，第二维中取第0个数据，直观来说，X[:,0]就是取所有行的第0个数据, X[:,1] 就是取所有行的第1个数据。
plt.scatter(X[:, 0], X[:, 1], c=y, s=40, cmap=plt.cm.Spectral)
plt.xlim(xx.min(), xx.max())
plt.ylim(yy.min(), yy.max())
plt.show()
