from distutils.log import debug
from enum import Flag
import re
from flask import Flask, jsonify, render_template, request, send_from_directory, url_for, redirect
from cassandra.cluster import Cluster
import math
import os
import warnings
warnings.filterwarnings('ignore')


app = Flask(__name__)

cluster = Cluster()
session = cluster.connect()


@app.route('/first_query')
def first_query():
    return render_template('input_form.html')

@app.route('/second_query')
def second_query():
    return render_template('second_form.html')

@app.route('/second', methods=['POST', 'GET'])
def data_second():
    if request.method == 'POST':
        print("OOOOOOOOOOOOOOOOOOOOOoooooooooooooooo")
        lat = request.form.get("lat")
        lon = request.form.get("lon")

        point_lat = round(float(lat), 4)
        point_lon = round(float(lon), 4)

        start = request.form.get("start")
        end = request.form.get("end")
        week_date = (start, end)

        session.encoder.mapping[tuple] = session.encoder.cql_encode_tuple
        query = "select * from TaxiServiceKeyspace.week_table WHERE week=%s AND lat CONTAINS %s AND lon CONTAINS %s ALLOW FILTERING"
        rows = session.execute(query, (week_date, point_lat, point_lon))

        
        lat_list = rows[0].lat
        lon_list = rows[0].lon
        index_list = []
        flag = False
        for i in range (0, len(lat_list)):
            if round(lat_list[i], 4)==point_lat and round(lon_list[i], 4)==point_lon:
                print(i)
                # index = i
                flag = True
                index_list.append(i)

        print(flag)
        if len(index_list) == 0:
            return "no data found"

        else:
            form_data = {}
            form_data['time_data'] = []
            form_data['base'] = []
            for i in index_list:
                form_data['time_data'].append(rows[0].date_time[i])
                form_data['base'].append(rows[0].base[i])

            # print(rows)
            print(form_data)
            return render_template('data2.html',form_data = form_data)
            # return redirect(url_for('data_second'))

@app.route('/data', methods=['POST'])
def handle_data():
    if request.method == 'POST':
        form_data = request.form

        lat = request.form.get("lat")
        lon = request.form.get("lon")

        point_lat = round(float(lat), 4)
        point_lon = round(float(lon), 4)
        start_point = (point_lat, point_lon)

        session.encoder.mapping[tuple] = session.encoder.cql_encode_tuple
        query = "select * from TaxiServiceKeyspace.start_coordinates_table WHERE start=%s"

        rows = session.execute(query, (start_point, ))

        
        form_data = {}
        form_data['date_times'] = rows[0].date_time
        form_data['bases'] = rows[0].base
        
        return render_template('data.html',form_data = form_data)


@app.route('/third_query')
def third_query():

    
    query = "select * from TaxiServiceKeyspace.midday_table limit 2"
    rows = session.execute(query)

    
    form_data = {}
    # form_data['date_times'] = []
    form_data['latitudes'] = []
    form_data['longitudes'] = []
    form_data['bases'] = []
    for i in range(0,2):
        # form_data['date_times'].append(rows[i].date_time)
        form_data['bases'].append(rows[i].base)
        form_data['latitudes'].append(rows[i].lat)
        form_data['longitudes'].append(rows[i].lon)
    
    return render_template('data.html',form_data = form_data)

app.run(debug = True)



