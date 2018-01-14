#!/usr/bin/env python3.6
# coding UTF-8

import math
from sys import float_info
import pyspark

# Point Class
class Point:
    def __init__(self, x, y, density = None, distance = None):
        self.x = x              
        self.y = y
        self.density = density
        self.distance_to_higher_density_point = distance

    # Dinstance between two points
    def distance_to(self, other):
        return sqrt((self.x - other.x)**2 + (self.y - other.y)**2)

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
# If point has the hifhest density this is computed as max distance to any other point
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
