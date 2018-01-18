#!/usr/bin/env python3.6
# coding UTF-8


import math
import random
from sys import float_info
from pyspark import SparkContext, SparkConf

# Point Class
class Point(object):
    def __init__(self, x, y, density = None, distance = None):
        self.x = x              
        self.y = y
        self.density = density
        self.distance_to_higher_density_point = distance

    def __str__(self):
        return "(%d, %d)" % (self.x, self.y)

    # Dinstance between two points
    def distance_to(self, other):
        return sqrt((self.x - other.x)**2 + (self.y - other.y)**2)

# Function generating list of random points
def generate_list_of_random_points(n):
    list_of_random_points = []
    limit_of_range = 101
    for i in xrange(n):
        x = random.randrange(-limit_of_range, limit_of_range)
        y = random.randrange(-limit_of_range, limit_of_range)
        point = Point(x, y)
        print(point)
        list_of_random_points.append(point)
    return list_of_random_points


# Function computing density for points
# Density is computed as a number of points which is closed to current than cutoff_distance
def set_density(points, cutoff_distance):
    for point_i in points:
        point_i.density = 0

        for point_j in points:
            if point_i == point_j: continue

            if point_i.distance_to(point_j) < cutoff_distance: point_i.density += 1

# Function computing distance_to_higher_density_point for points
# distance_to_higher_density_point is computed as minimum distance to point with higher density
# If point has the highest density this is computed as max distance to any other point
def set_distance_to_higher_density_point(points):
    for point_i in points:
        point_i.distance_to_higher_density_point = float_info.max
        
        for point_j in points:
            if point_i == point_j: continue

            if point_i.density >= point_j.density: continue

            if point_i.distance_to(point_j) < point_i.distance_to_higher_density_point:
                point_i.distance_to_higher_density_point = point_i.distance_to(point_j)

        if point_i.distance_to_higher_density_point == float_info.max:
            point_i.distance_to_higher_density_point = 0

            for point_j in points:
                if point_i == point_j: continue

                if point_i.distance_to(point_j) > point_i.distance_to_higher_density_point:
                    point_i.distance_to_higher_density_point = point_i.distance_to(point_j)




if __name__ == "__main__":
    conf = SparkConf().setAppName('DataMining_Project')
    sc = SparkContext(conf=conf)

    list_of_random_points = generate_list_of_random_points(10)
    pointsRDD = sc.parallelize(list_of_random_points)

    print("DataMining_Project!")

