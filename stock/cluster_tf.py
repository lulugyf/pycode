#encoding=utf-8

import tensorflow as tf
from random import choice, shuffle
from numpy import array
from six.moves import cPickle as pickle


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
    K = 6  # num clusters
    F = 2  # num features
    MAX_ITERS = 1000

    start = time.time()

    mypoints = np.random.randn(N, F) # tf.random_uniform([N, F])
    points = tf.Variable(mypoints)
    cluster_assignments = tf.Variable(tf.zeros([N], dtype=tf.int64))

    # Silly initialization:  Use the first K points as the starting
    # centroids.  In the real world, do this better.
    centroids = tf.Variable(tf.slice(points.initialized_value(), [0, 0], [K, F]))

    # Replicate to N copies of each centroid and K copies of each
    # point, then subtract and compute the sum of squared distances.
    rep_centroids = tf.reshape(tf.tile(centroids, [N, 1]), [N, K, F])
    rep_points = tf.reshape(tf.tile(points, [1, K]), [N, K, F])
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
    print("Centroids:")
    print(centers)
    print("Cluster assignments:", assignments)
    return centers, mypoints, assignments

def kmeans3(mypoints, num_clusters, max_iters=1000):
    import tensorflow as tf
    import time

    N = mypoints.shape[0]
    K = num_clusters
    F = mypoints.shape[1]  # num features
    MAX_ITERS = max_iters

    start = time.time()
    points = tf.Variable(mypoints)
    cluster_assignments = tf.Variable(tf.zeros([N], dtype=tf.int64))

    centroids = tf.Variable(tf.slice(points.initialized_value(), [0, 0], [K, F]))

    rep_centroids = tf.reshape(tf.tile(centroids, [N, 1]), [N, K, F])
    rep_points = tf.reshape(tf.tile(points, [1, K]), [N, K, F])
    sum_squares = tf.reduce_sum(tf.square(rep_points - rep_centroids),
                                reduction_indices=2)

    best_centroids = tf.argmin(sum_squares, 1)
    did_assignments_change = tf.reduce_any(tf.not_equal(best_centroids, cluster_assignments))

    def bucket_mean(data, bucket_ids, num_buckets):
        total = tf.unsorted_segment_sum(data, bucket_ids, num_buckets)
        count = tf.unsorted_segment_sum(tf.ones_like(data), bucket_ids, num_buckets)
        return total / count

    means = bucket_mean(points, best_centroids, K)

    with tf.control_dependencies([did_assignments_change]):
        do_updates = tf.group(
            centroids.assign(means),
            cluster_assignments.assign(best_centroids))

    init = tf.initialize_all_variables()

    config = tf.ConfigProto()
    config.gpu_options.allow_growth = True
    sess = tf.Session(config=config)
    sess.run(init)

    changed = True
    iters = 0

    while changed and iters < MAX_ITERS:
        iters += 1
        [changed, _] = sess.run([did_assignments_change, do_updates])

    [centers, assignments] = sess.run([centroids, cluster_assignments])
    end = time.time()
    print("Found in %.2f seconds" % (end - start)), iters, "iterations"
    print("Centroids:")
    print(centers)
    print("Cluster assignments:", assignments)

# from https://github.com/tensorflow/tensorflow/blob/master/tensorflow/contrib/factorization/python/ops/kmeans.py
def kmeans4(points, num_clusters, num_iterations = 10):
    #import numpy as np
    #import tensorflow as tf
    #num_points = 100
    # dimensions = 2
    # points = np.random.uniform(0, 1000, [num_points, dimensions])

    def input_fn():
        return tf.train.limit_epochs(
            tf.convert_to_tensor(points, dtype=tf.float32), num_epochs=1)

    #num_clusters = 5
    kmeans = tf.contrib.factorization.KMeansClustering(
        num_clusters=num_clusters, use_mini_batch=False)
    # train
    #num_iterations = 10
    previous_centers = None
    for _ in range(num_iterations):
        kmeans.train(input_fn)
        cluster_centers = kmeans.cluster_centers()
        if previous_centers is not None:
            print('delta:', cluster_centers - previous_centers)
        previous_centers = cluster_centers
        print('score:', kmeans.score(input_fn) )
    #print('cluster centers:', cluster_centers)
    # map the input points to their clusters
    cluster_indices = list(kmeans.predict_cluster_index(input_fn))
    # for i, point in enumerate(points):
    #     cluster_index = cluster_indices[i]
    #     center = cluster_centers[cluster_index]
    #     print('point:', point, 'is in cluster', cluster_index, 'centered at', center)
    return kmeans, cluster_indices, cluster_centers

def draw2(centers, points, assignments):
    import matplotlib.pyplot as plt
    import numpy as np
    for i in range(centers.shape[0]):
        p = points[np.nonzero(assignments[:] == i), :][0, :, :]
        plt.scatter(p[:, 0], p[:, 1])  # color='red'
        c = centers[i, :]
        plt.scatter(c[0], c[1], color='blue', s=20, edgecolor='none')
    plt.show()

if __name__ == '__main__':
    # import sys, numpy as np
    # sys.path.insert(0, 'e:/worksrc/pycode/stock')
    # import cluster_tf as ctf
    # dd = np.loadtxt('e:/stock/xx', dtype=float)
    # centroids, assignments = ctf.TFKMeansCluster(dd, 20)
    from six.moves import cPickle as pickle
    pickle_file = "e:/stock/clusters0329.pickle"
    with open(pickle_file, "rb") as f: lines = pickle.load(f)
    kmeans4(lines, 60, 10)
'''
import sys
sys.path.insert(0, 'e:/worksrc/pycode/stock')
from importlib import reload
import cluster_tf as ctf

from six.moves import cPickle as pickle
with open("e:/stock/clusters0329.pickle", "rb") as f: lines = pickle.load(f)
kmeans, cluster_indices, cluster_centers = ctf.kmeans4(lines, 60, 10)
with open("e:/stock/clusters0329_result.pickle", "wb") as f: pickle.dump(np.array(cluster_indices), f, pickle.HIGHEST_PROTOCOL)

'''