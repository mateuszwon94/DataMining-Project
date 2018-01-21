#!/usr/bin/env python2
# coding UTF-8

from __future__ import division, print_function

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib import colors
import math
import random
import time
from sys import float_info
from pyspark import SparkContext, SparkConf

clusters_color = [name for name, c in dict(colors.BASE_COLORS, **colors.CSS4_COLORS).items()
                  if "white" not in name]
random.shuffle(clusters_color)

# Nice output :)
def print_point(point):
    print("(%.2f, %.2f)  -> id=%d,  \tid_of_closest_neighbor=%s,\tcluster=%s,\tdensity=%d,\tdistance_to_higher_density_point=%.4f" % \
        (point["x"], point["y"], point["id"], str(point["id_of_point_with_higher_density"]), 
         str(point["cluster"]), point["density"], point["distance_to_higher_density_point"]))

# Dinstance between two points
def distance_to(point_i, point_j):
    return math.sqrt((point_i["x"] - point_j["x"])**2 + (point_i["y"] - point_j["y"])**2)

# Function generating list of random points
def generate_list_of_random_points(n, limit):
    list_of_random_points = []
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
    
# Function generates n random points 
# and calculates density and distance to higher density point
def generate_and_calculate(sc, n, limit, cutoff_distance):
    
    # Simple plot of generated points
    def plot_of_x_and_y(points, file_name):
        x = [point["x"] for point in points]
        y = [point["y"] for point in points]
        fig, ax = matplotlib.pyplot.subplots()
        ax.scatter(x, y, color='b')
        ax.set_xlabel('x')
        ax.set_ylabel('y')
        fig.savefig(file_name)

    list_of_random_points = generate_list_of_random_points(n, limit)
    plot_of_x_and_y(list_of_random_points, "x_and_y.png")
    pointsRDD = sc.parallelize(list_of_random_points)

    points_with_local_density = pointsRDD.map(
        lambda point: set_density(point, list_of_random_points, cutoff_distance))
    list_of_random_points = [point for point in points_with_local_density.toLocalIterator()]

    points_with_distance_to_higher_density_point = points_with_local_density.map(
        lambda point: set_distance_to_higher_density_point(point, list_of_random_points))

    return points_with_distance_to_higher_density_point

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

def plot_clusters(points, file_name):

    def get_color(point, clusters_color):
        if point["cluster"] is None:
            return clusters_color[0]
        return clusters_color[point["cluster"]]

    x = [point["x"] for point in points.collect()]
    y = [point["y"] for point in points.collect()]
    c = [get_color(point, clusters_color) for point in points.collect()]
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
                    point["cluster"] = i+1

        return point
    
    sorted_points = points.sortBy(
        lambda p: -(p["density"] * p["distance_to_higher_density_point"]))
    centers = sorted_points.take(n)

    points = points.map(
        lambda point: set_center(point, centers))

    print("\n\nCenters of clusters:")
    for point in points.collect():
        if point["cluster"]:
            print_point(point)

    return points

def assign_points_to_clusters(points):
        
    def set_cluster(point, all_points):
        if point["cluster"] is not None or point["id_of_point_with_higher_density"] is None:
            return point

        ref_point = [ p for p in all_points 
                            if p["id"] == point["id_of_point_with_higher_density"] ][0]
        point["cluster"] = ref_point["cluster"]

        return point
    
    while None in [point["cluster"] for point in points.collect()]:
        all_points = points.collect()
        points = points.map(lambda point: set_cluster(point, all_points))

    return points

def main(sc, clusters, n, limit, cutoff_distance):
    clusters_color = ['black', 'green', 'blue', 'red', 'yellow', 'purple', 'cyan', 'lime']

    points = generate_and_calculate(sc, n, limit, cutoff_distance)
    points = choose_centers_of_clusters(points, clusters)
    plot_of_density_and_distance_to_higher_density_point(points, 'density.png')

    points = assign_points_to_clusters(points)
    plot_clusters(points, 'clusters.png')  

    print("\n\nPoints:")
    for point in points.collect():
        print_point(point)

def complexity_in_time(sc, clusters, limit, cutoff_distance, p_min, p_max, k):
    complexity_data = {"points": [], "time": []}
    
    for i in xrange(p_min, p_max + 1, k):
        try:
            start_time = time.time()
            points = generate_and_calculate(sc, i, limit, cutoff_distance)
            points = choose_centers_of_clusters(points, clusters)
            points = assign_points_to_clusters(points)
        
            elapsed_time = time.time() - start_time
            complexity_data["points"].append(i)
            complexity_data["time"].append(elapsed_time)

            plot_clusters(points, 'clusters_T-%d.png' % i)
            print("Points: %d  --->  time = %f"% (i, elapsed_time))
            matplotlib.pyplot.close('all')
           
        except Exception:
            print("Exception!!!")
            continue

    avg = sum(complexity_data["time"])/len(complexity_data["time"])
    print("Average time is = %f"% (i, avg))

    fig, ax = matplotlib.pyplot.subplots()
    ax.scatter(complexity_data["points"], complexity_data["time"])
    ax.set_xlabel('points')
    ax.set_ylabel('time')
    fig.savefig('complexity_time-%d.png' % clusters)

def complexity_in_clusters_number(sc, clusters_min, clusters_max, limit, cutoff_distance, p, k=1):
    complexity_data = {"points": [], "time": []}
        
    for i in xrange(clusters_min, clusters_max + 1, k):
        try:
            start_time = time.time()
            points = generate_and_calculate(sc, p, limit, cutoff_distance)
            points = choose_centers_of_clusters(points, i)
            points = assign_points_to_clusters(points)
        
            elapsed_time = time.time() - start_time
            complexity_data["points"].append(i)
            complexity_data["time"].append(elapsed_time)

            plot_clusters(points, 'clusters_C-%d.png' % i)
            print("Clusters: %d  --->  time = %f"% (i, elapsed_time))
            matplotlib.pyplot.close('all')
           
        except Exception:
            print("Exception!!!")
            continue

    fig, ax = matplotlib.pyplot.subplots()
    ax.scatter(complexity_data["points"], complexity_data["time"])
    ax.set_xlabel('clusters')
    ax.set_ylabel('time')
    fig.savefig('complexity_clusters-%d.png' % p)

if __name__ == "__main__":
    conf = SparkConf().setAppName('DataMining_Project')
    sc = SparkContext(conf=conf)

    #main(sc, clusters=5, n=100, limit=10, cutoff_distance=1)

    #complexity_in_time(sc, clusters=5, limit=10, cutoff_distance=1, p_min=20, p_max=1000, k=20)
    complexity_in_clusters_number(sc, clusters_min=3, clusters_max=50, limit=10, cutoff_distance=1, p=300)

    matplotlib.pyplot.close('all')
    print("\n\nDataMining_Project!")
