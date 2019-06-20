# -*- coding: utf-8 -*-
"""
Created on Fri Oct 19 17:57:03 2018

@author: gabriel
"""
import os
import requests
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon
import numpy as np
import dask.dataframe as dd
import glob 
import gmplot

#matplotlib inline
import matplotlib.pyplot as plt
import seaborn as sns

#from IPython.display import IFrame

os.chdir("C:\\Users\\gabriel\\Dropbox (Brown)\\research\\Geodata_politics\\data\\cuebiq\\sample\\2016110100\\")
shapedir = "C:\\Users\\gabriel\\Dropbox (Brown)\\research\\geodata\\data\\boston\\"
outputdir = "C:\\Users\\gabriel\\Dropbox (Brown)\\research\\geodata\\build\\output\\"
#%% Load GPS file

df = pd.concat([pd.read_csv(f, sep = "\t", header = None) for f in glob.glob('part-r-*.csv')], ignore_index = True)
df = df.drop([5,6,7], axis = 1)
df.columns = ['timestamp', 'uniqueid', 'lat', 'long', 'dwell']

#%% Import Boston Shapefile

boston_neigh = gpd.read_file(shapedir + "Boston_Neighborhoods.shp" )
boston_seg = gpd.read_file(shapedir + "Boston_Segments.shp" )

#extract crs from map 
coord_sys = boston_neigh.crs
#basemap = boston_neigh.plot(figsize = (30,30), color = "lightgray")
#boston_seg.plot(ax = basemap, color = "gray")

#%%Plot one individual

#filter 1 individual
indiv_1 = df[(df.uniqueid=="cff273ecf7e13d49bab3a73106cdc21d289de92b1f63d766e461920d9169e975")]
##sort by timestamp
#extract time from timestamp
indiv_1[["date", "time_tzone"]] = indiv_1['timestamp'].str.split("T", expand = True)
indiv_1[["time", "tzone"]] = indiv_1["time_tzone"].str.split("-", expand = True)
indiv_1['tzone'] = ('-' + indiv_1['tzone'].astype(str)) #to finish: cannot convert to number
#indiv_1['timestamp'] = pd.to_datetime(indiv_1['timestamp'],  utc = True) #TO FINISH: CONVERT TIME STAMP TO DATE TIME KEEPING TZONE
#indiv_1.timestamp = indiv_1.timestamp.tz_convert('US/Eastern')
indiv_1 = indiv_1.sort_values(['time'], ascending = [True])

#convert info to geodataframe
geometry = [Point(xy) for xy in zip(indiv_1.long, indiv_1.lat)]
indiv_1 = gpd.GeoDataFrame(indiv_1, crs=coord_sys, geometry=geometry)


#take min and max lat and long to have a properly zoomed map
min_lat, max_lat, min_lon, max_lon = \
min(indiv_1['lat']), max(indiv_1['lat']), \
min(indiv_1['long']), max(indiv_1['long'])


#empty map using gmplot 
## Create empty map with zoom level 16
mymap = gmplot.GoogleMapPlotter(
    min_lat + (max_lat - min_lat) / 2, 
    min_lon + (max_lon - min_lon) / 2, 
    16)
mymap.plot(indiv_1['lat'], indiv_1['long'], 'blue', edge_width=1)
mymap.scatter([indiv_1.iloc[9]['lat']], [indiv_1.iloc[9]['long']], 
              'red', size=10, marker=False)
mymap.draw(outputdir + 'my_gm_plot.html')
os.system(outputdir + 'my_gm_plot.html')


basemap = boston_neigh.plot(figsize = (80,80), color = "lightgray")
streetmap = boston_seg.plot(ax = basemap, zorder = 1, color = "gray")
streetmap = indiv_1.plot(ax = streetmap, zorder = 2, cmap=plt.cm.Blues, edgecolors = 'black')
plt.savefig(outputdir + 'indiv1_map.png')

#%%sort by frequencies
freq = df.groupby('uniqueid').size()
freq = freq.sort_values(0, ascending = True)
