#!/usr/bin/env python2
# coding UTF-8

from __future__ import division, print_function

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import math
import random
from sys import float_info
from pyspark import SparkContext, SparkConf


# Point Class
#class Point(object):
#    def __init__(self, x, y, density = None, distance = None):
#        self.x = x              
#        self.y = y
#        self.density = density
#        self.distance_to_higher_density_point = distance
#
#    def __str__(self):
#        return "(%d, %d)" % (self.x, self.y)
#
#    # Dinstance between two points
#    def distance_to(self, other):
#        return math.sqrt((self.x - other.x)**2 + (self.y - other.y)**2)


# Dinstance between two points
def distance_to(point_i, point_j):
    return math.sqrt((point_i["x"] - point_j["x"])**2 + (point_i["y"] - point_j["y"])**2)

# Function generating list of random points
def generate_list_of_random_points(n, limit):
    list_of_random_points = []
    limit += 1
    for counter in xrange(n):
        x = round(random.uniform(0, limit), 2)
        y = round(random.uniform(0, limit), 2)
        point = {"id": counter+1, "x": x, "y": y,
                 "density": None,
                 "distance_to_higher_density_point": None,
                 "id_of_point_with_higher_density": None,
                 "cluster": None}
        list_of_random_points.append(point)
    return list_of_random_points


# Function computing density for points
# Density is computed as a number of points which is closed to current than cutoff_distance
def set_density(point_i, points, cutoff_distance):
    point_i["density"] = 0

    for point_j in points:
        if point_i == point_j: continue

        if distance_to(point_i, point_j) < cutoff_distance: point_i["density"] += 1

    return point_i


# Function computing distance_to_higher_density_point for points
# distance_to_higher_density_point is computed as minimum distance to point with higher density
# If point has the highest density this is computed as max distance to any other point
def set_distance_to_higher_density_point(point_i, points):
    point_i["distance_to_higher_density_point"] = float("inf")
        
    for point_j in points:
        if point_i == point_j: continue

        if point_i["density"] >= point_j["density"]: continue

        if distance_to(point_i, point_j) < point_i["distance_to_higher_density_point"]:
            point_i["distance_to_higher_density_point"] = distance_to(point_i, point_j)
            point_i["id_of_point_with_higher_density"] = point_j["id"]

    if point_i["distance_to_higher_density_point"] == float("inf"):
       point_i["distance_to_higher_density_point"] = 0

    for point_j in points:
        if point_i == point_j: continue
            
        if distance_to(point_i, point_j) > point_i["distance_to_higher_density_point"]:
            point_i["distance_to_higher_density_point"] = distance_to(point_i, point_j)

    return point_i
    

def generate_and_calculate(sc, n, limit, cutoff_distance):
    list_of_random_points = generate_list_of_random_points(n, limit)
    plot_of_x_and_y(list_of_random_points, "x_and_y.png")
    pointsRDD = sc.parallelize(list_of_random_points)

    points_with_local_density = pointsRDD.map(
        lambda point: set_density(point, list_of_random_points, cutoff_distance))
    list_of_random_points = [point for point in points_with_local_density.toLocalIterator()]

    points_with_distance_to_higher_density_point = points_with_local_density.map(
        lambda point: set_distance_to_higher_density_point(point, list_of_random_points))
    # print("\n\npoints_with_distance_to_higher_density_point:")
    # print(points_with_distance_to_higher_density_point.collect())

    return points_with_distance_to_higher_density_point


def plot_of_x_and_y(points, file_name):
    x = [point["x"] for point in points]
    y = [point["y"] for point in points]
    fig, ax = matplotlib.pyplot.subplots()
    ax.scatter(x, y, color='b')
    ax.set_xlabel('x')
    ax.set_ylabel('y')
    fig.savefig(file_name)


def plot_of_density_and_distance_to_higher_density_point(points, file_name):
    points =  points.collect()
    x = [point["density"] for point in points]
    y = [point["distance_to_higher_density_point"] for point in points]
    c = ['green' if point["cluster"] is not None else 'red' for point in points]
    fig, ax = matplotlib.pyplot.subplots()
    ax.scatter(x, y, c=c)
    ax.set_xlabel('density')
    ax.set_ylabel('distance_to_higher_density_point')
    fig.savefig(file_name)

def plot_clusters(points, clusters_color, file_name):
    def get_color(point):
        if point["cluster"] == 0: return 'blue'
        if point["cluster"] == 1: return 'green'
        if point["cluster"] == 2: return 'red'
        if point["cluster"] == 3: return 'yellow'
        if point["cluster"] == 4: return 'black'
        if point["cluster"] == 5: return 'purple'
        if point["cluster"] == 6: return 'cyan'
        if point["cluster"] == 7: return 'lime'

    x = [point["x"] for point in points.collect()]
    y = [point["y"] for point in points.collect()]
    c = [get_color(point) for point in points.collect()]
    fig, ax = matplotlib.pyplot.subplots()
    ax.scatter(x, y, color=c)
    ax.set_xlabel('x')
    ax.set_ylabel('y')
    fig.savefig(file_name)

def choose_centers_of_clusters(points, n):
    # TODO: try to write function, which automatically chooses number of centers
    def set_center(point, centers):
        for i, center in enumerate(centers):
            if point["id"] == center["id"]:
                    point["cluster"] = i

        return point

    def set_cluster(point, all_points):
        if point["cluster"] != None or point["id_of_point_with_higher_density"] == None: return point

        ref_point = [ p for p in all_points 
                            if p["id"] == point["id_of_point_with_higher_density"] ][0]
        point["cluster"] = ref_point["cluster"]

        return point

    
    sorted_points = points.sortBy(
        lambda p: -(p["density"] * p["distance_to_higher_density_point"]))
    centers = sorted_points.take(n)
    points = points.map(
        lambda point: set_center(point, centers))

    all_points = points.collect()
    while None in [ point["cluster"] for point in points.collect()]:
        print("Assigning clusters")
        points = points.map(lambda point: set_cluster(point, all_points))
        all_points = points.collect()

    return points

if __name__ == "__main__":
    conf = SparkConf().setAppName('DataMining_Project')
    sc = SparkContext(conf=conf)

    clusters = 7

    points = generate_and_calculate(sc, n=200, limit=10, cutoff_distance=1)
    points = choose_centers_of_clusters(points, clusters)

    plot_clusters(points, ['green', 'blue', 'red', 'black', 'yellow'], 'clusters.png')
    
    plot_of_density_and_distance_to_higher_density_point(points, 'density.png')
    for point in points.collect():
        print(point)

    print("\n\nDataMining_Project!")

