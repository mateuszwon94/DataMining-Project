#!/usr/bin/env python3.6
# coding UTF-8


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
        x = random.randrange(limit)
        y = random.randrange(limit)
        point = {"id": counter+1, "x": x, "y": y, "density": None, "distance_to_higher_density_point" : None}
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

    if point_i["distance_to_higher_density_point"] == float("inf"):
       point_i["distance_to_higher_density_point"] = 0

    for point_j in points:
        if point_i == point_j: continue
            
        if distance_to(point_i, point_j) > point_i["distance_to_higher_density_point"]:
            point_i["distance_to_higher_density_point"] = distance_to(point_i, point_j)

    return point_i
    

def generate_and_calculate(sc, n, limit):
    list_of_random_points = generate_list_of_random_points(n=10, limit=20)
    pointsRDD = sc.parallelize(list_of_random_points)

    cutoff_distance = 10
    points_with_local_density = pointsRDD.map(lambda point: set_density(point, list_of_random_points, cutoff_distance))
    list_of_random_points = [point for point in points_with_local_density.toLocalIterator()]

    points_with_distance_to_higher_density_point = points_with_local_density.map(lambda point: set_distance_to_higher_density_point(point, list_of_random_points))
    print("\n\npoints_with_distance_to_higher_density_point:")
    print(points_with_distance_to_higher_density_point.collect())

    return [point for point in points_with_distance_to_higher_density_point.toLocalIterator()]


if __name__ == "__main__":
    conf = SparkConf().setAppName('DataMining_Project')
    sc = SparkContext(conf=conf)

    list_of_random_points = generate_and_calculate(sc, n=15, limit=20)
    

    print("\n\nDataMining_Project!")

