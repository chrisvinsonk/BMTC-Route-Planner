import pyspark
import pandas as pd
import itertools
from pyspark.sql import SparkSession

#spark session for big data manipulation
spark = SparkSession.builder.appName('Practice').getOrCreate()

#spark dataset manipulation
route_schedule_pyspark= spark.read.csv('bmtc_schedule.csv',header=True)
route_timings_pyspark= spark.read.csv('bmtc-bus-timings-2014.csv',header=True)
bmtc = route_schedule_pyspark.join(route_timings_pyspark, on='route_no')
bmtc = bmtc.drop("departure_from_origin","arrival_at_origin", "departure_from_destination" ,"arrival_at_destination", "map_json_content")


#origin bus stops dataset
bus_stop_origin = bmtc.select(bmtc["origin"]).dropDuplicates()
origin_bus_pd= bus_stop_origin.toPandas()
origin = origin_bus_pd.values.tolist()
origin1 = [item for sublist in origin for item in sublist]

#destination bus stop
bus_stop_dest = bmtc.select(bmtc["destination"]).dropDuplicates()
dest_bus_pd= bus_stop_dest.toPandas()
dest = dest_bus_pd.values.tolist()
dest1 = [item for sublist in dest for item in sublist]

#streamlit front end
import streamlit as st
st.title("Bus Route Recommender")
st.write("Recommends Bus Route Based on Origin Bus Stop and Destination Bus Stop")
default_value = ''
#get origin bus stop
origin_bus_pd+=['']
origin_stop = st.selectbox( 'Origin Bus Stop: ', origin_bus_pd)

#create list of destination stops from given origin bus
filtered_data = bmtc.filter(bmtc["origin"] == origin_stop)
stop_dest = filtered_data.select(bmtc["destination"]).dropDuplicates()
dest_pd= stop_dest.toPandas()
dest_ = dest_pd.values.tolist()
dest = [item for sublist in dest_ for item in sublist]
dest+=['']
#get destination bus stop
dest_stop = st.selectbox( 'Destination Bus Stop: ', dest)

#get routes between entered origin and destination bus stops
routes=((bmtc.filter((bmtc["origin"]==origin_stop) & (bmtc["destination"]==dest_stop)).dropDuplicates(["route_no"])).select(["route_no"])).toPandas().values.tolist()
routes1 = [item for sublist in routes for item in sublist]
if(len(routes1)==0):
    st.write('No direct buses available')
else:
    st.write('Buses available:')
    st.dataframe(data=routes1)
bus_num = str(routes1[0])

from googlesearch.googlesearch import GoogleSearch
import webbrowser
query = "bmtc " + bus_num + " moovit"
from googlesearch import search

def google_search(query):
    for url in search(query, num_results=1):
        return url
result = google_search(query)
st.title("Route Details")
st.write(result)

#webbrowser.open(result)

import requests
from bs4 import BeautifulSoup

# Term to search for
term = "map"

# Make a request to the website
response = requests.get(result)

# Parse the HTML of the website
soup = BeautifulSoup(response.text, 'html.parser')

# Find all links on the website
links = soup.find_all('a')

# Iterate through the links
for link in links:
    # Check if the term is in the link text or the link URL
    if term in link.text or term in link.get('href'):
        # Open the link that contains the term
        url1 = link.get('href')
        #webbrowser.open(url1)
        break
map_query = "bmtc " + bus_num + " moovit"
map_result = google_search(map_query)
st.title("Map:")
st.write(map_result)
st.components.v1.iframe(map_result, width=800, height=800, scrolling=True)

print("end")