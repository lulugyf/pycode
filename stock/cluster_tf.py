#encoding=utf-8

import tensorflow as tf
from random import choice, shuffle
from numpy import array


def TFKMeansCluster(vectors, noofclusters):
    """
    K-Means Clustering using TensorFlow.
    `vertors`应该是一个n*k的二维的NumPy的数组，其中n代表着K维向量的数目
    'noofclusters' 代表了待分的集群的数目，是一个整型值

    !!! 这个计算速度特别慢, 但还是能出结果
    """

    noofclusters = int(noofclusters)
    assert noofclusters < len(vectors)

    # 找出每个向量的维度
    dim = len(vectors[0])

    # 辅助随机地从可得的向量中选取中心点
    vector_indices = list(range(len(vectors)))
    shuffle(vector_indices)

    # 计算图
    # 我们创建了一个默认的计算流的图用于整个算法中，这样就保证了当函数被多次调用      #时，默认的图并不会被从上一次调用时留下的未使用的OPS或者Variables挤满

    graph = tf.Graph()

    with graph.as_default():
        # 计算的会话
        sess = tf.Session()

        ##构建基本的计算的元素

        ##首先我们需要保证每个中心点都会存在一个Variable矩阵
        ##从现有的点集合中抽取出一部分作为默认的中心点
        centroids = [tf.Variable((vectors[vector_indices[i]]))
                     for i in range(noofclusters)]

        ##创建一个placeholder用于存放各个中心点可能的分类的情况
        centroid_value = tf.placeholder("float64", [dim])
        cent_assigns = []
        for centroid in centroids:
            cent_assigns.append(tf.assign(centroid, centroid_value))

        ##对于每个独立向量的分属的类别设置为默认值0
        assignments = [tf.Variable(0) for i in range(len(vectors))]

        ##这些节点在后续的操作中会被分配到合适的值
        assignment_value = tf.placeholder("int32")
        cluster_assigns = []
        for assignment in assignments:
            cluster_assigns.append(tf.assign(assignment,
                                             assignment_value))

        ##下面创建用于计算平均值的操作节点
        # 输入的placeholder
        mean_input = tf.placeholder("float", [None, dim])

        # 节点/OP接受输入，并且计算0维度的平均值，譬如输入的向量列表
        mean_op = tf.reduce_mean(mean_input, 0)

        ##用于计算欧几里得距离的节点
        v1 = tf.placeholder("float", [dim])
        v2 = tf.placeholder("float", [dim])
        euclid_dist = tf.sqrt(tf.reduce_sum(tf.pow(tf.subtract(v1, v2), 2)))

        ##这个OP会决定应该将向量归属到哪个节点
        ##基于向量到中心点的欧几里得距离
        # Placeholder for input
        centroid_distances = tf.placeholder("float", [noofclusters])
        cluster_assignment = tf.argmin(centroid_distances, 0)

        ##初始化所有的状态值
        ##这会帮助初始化图中定义的所有Variables。Variable-initializer应该定         ##义在所有的Variables被构造之后，这样所有的Variables才会被纳入初始化
        init_op = tf.initialize_all_variables()

        # 初始化所有的变量
        sess.run(init_op)

        ##集群遍历

        # 接下来在K-Means聚类迭代中使用最大期望算法。为了简单起见，只让它执行固定的次数，而不设置一个终止条件
        noofiterations = 100
        for iteration_n in range(noofiterations):
            ##期望步骤
            ##基于上次迭代后算出的中心点的未知
            ##the _expected_ centroid assignments.
            # 首先遍历所有的向量
            for vector_n in range(len(vectors)):
                vect = vectors[vector_n]

                # 计算给定向量与分配的中心节点之间的欧几里得距离
                distances = [sess.run(euclid_dist, feed_dict={
                    v1: vect, v2: sess.run(centroid)})
                             for centroid in centroids]
                # 下面可以使用集群分配操作，将上述的距离当做输入
                assignment = sess.run(cluster_assignment, feed_dict={
                    centroid_distances: distances})
                # 接下来为每个向量分配合适的值
                sess.run(cluster_assigns[vector_n], feed_dict={
                    assignment_value: assignment})

            ##最大化的步骤
            # 基于上述的期望步骤，计算每个新的中心点的距离从而使集群内的平方和最小
            for cluster_n in range(noofclusters):
                # 收集所有分配给该集群的向量
                assigned_vects = [vectors[i] for i in range(len(vectors))
                                  if sess.run(assignments[i]) == cluster_n]
                # 计算新的集群中心点
                new_location = sess.run(mean_op, feed_dict={
                    mean_input: array(assigned_vects)})
                # 为每个向量分配合适的中心点
                sess.run(cent_assigns[cluster_n], feed_dict={
                    centroid_value: new_location})

        # 返回中心节点和分组
        centroids = sess.run(centroids)
        assignments = sess.run(assignments)
        return centroids, assignments

#from https://blog.altoros.com/using-k-means-clustering-in-tensorflow.html
def cluster_1():
    import matplotlib.pyplot as plt
    import numpy as np
    import tensorflow as tf

    points_n = 200
    clusters_n = 3
    iteration_n = 100

    points = tf.constant(np.random.uniform(0, 10, (points_n, 2)))
    centroids = tf.Variable(tf.slice(tf.random_shuffle(points), [0, 0], [clusters_n, -1]))

    points_expanded = tf.expand_dims(points, 0)
    centroids_expanded = tf.expand_dims(centroids, 1)

    distances = tf.reduce_sum(tf.square(tf.subtract(points_expanded, centroids_expanded)), 2)
    assignments = tf.argmin(distances, 0)

    means = []
    for c in range(clusters_n):
        means.append(tf.reduce_mean(
            tf.gather(points,
                      tf.reshape(
                          tf.where(
                              tf.equal(assignments, c)
                          ), [1, -1])
                      ), reduction_indices=[1]))

    new_centroids = tf.concat(0, means)

    update_centroids = tf.assign(centroids, new_centroids)
    init = tf.initialize_all_variables()

    with tf.Session() as sess:
        sess.run(init)
        for step in range(iteration_n):
            [_, centroid_values, points_values, assignment_values] = sess.run(
                [update_centroids, centroids, points, assignments])

        print
        "centroids" + "\n", centroid_values

    plt.scatter(points_values[:, 0], points_values[:, 1], c=assignment_values, s=50, alpha=0.5)
    plt.plot(centroid_values[:, 0], centroid_values[:, 1], 'kx', markersize=15)
    plt.show()


# from https://gist.github.com/dave-andersen/265e68a5e879b5540ebc
def kmeans2():
    import tensorflow as tf
    import numpy as np
    import time

    N = 10000
    K = 4
    MAX_ITERS = 1000

    start = time.time()

    points = tf.Variable(tf.random_uniform([N, 2]))
    cluster_assignments = tf.Variable(tf.zeros([N], dtype=tf.int64))

    # Silly initialization:  Use the first K points as the starting
    # centroids.  In the real world, do this better.
    centroids = tf.Variable(tf.slice(points.initialized_value(), [0, 0], [K, 2]))

    # Replicate to N copies of each centroid and K copies of each
    # point, then subtract and compute the sum of squared distances.
    rep_centroids = tf.reshape(tf.tile(centroids, [N, 1]), [N, K, 2])
    rep_points = tf.reshape(tf.tile(points, [1, K]), [N, K, 2])
    sum_squares = tf.reduce_sum(tf.square(rep_points - rep_centroids),
                                reduction_indices=2)

    # Use argmin to select the lowest-distance point
    best_centroids = tf.argmin(sum_squares, 1)
    did_assignments_change = tf.reduce_any(tf.not_equal(best_centroids,
                                                        cluster_assignments))

    def bucket_mean(data, bucket_ids, num_buckets):
        total = tf.unsorted_segment_sum(data, bucket_ids, num_buckets)
        count = tf.unsorted_segment_sum(tf.ones_like(data), bucket_ids, num_buckets)
        return total / count

    means = bucket_mean(points, best_centroids, K)

    # Do not write to the assigned clusters variable until after
    # computing whether the assignments have changed - hence with_dependencies
    with tf.control_dependencies([did_assignments_change]):
        do_updates = tf.group(
            centroids.assign(means),
            cluster_assignments.assign(best_centroids))

    init = tf.initialize_all_variables()

    sess = tf.Session()
    sess.run(init)

    changed = True
    iters = 0

    while changed and iters < MAX_ITERS:
        iters += 1
        [changed, _] = sess.run([did_assignments_change, do_updates])

    [centers, assignments] = sess.run([centroids, cluster_assignments])
    end = time.time()
    print("Found in %.2f seconds" % (end - start)), iters, "iterations"
    print
    "Centroids:"
    print
    centers
    print
    "Cluster assignments:", assignments

if __name__ == '__main__':
    import sys, numpy as np
    sys.path.insert(0, 'e:/worksrc/pycode/stock')
    import cluster_tf as ctf
    dd = np.loadtxt('e:/stock/xx', dtype=float)
    centroids, assignments = ctf.TFKMeansCluster(dd, 20)