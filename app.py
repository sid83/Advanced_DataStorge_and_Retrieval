import datetime as dt
import pandas as pd
import numpy as np

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, inspect, func

from flask import Flask, jsonify

# create engine for connecting to hawaii.sqlite db
engine = create_engine("sqlite:///Resources_cleaned/hawaii.sqlite", connect_args={'check_same_thread': False})

# Reflect Database into ORM class
Base = automap_base()
Base.prepare(engine, reflect=True)
Station = Base.classes.station
Measurement = Base.classes.measurement

# Start a session to query the database
session = Session(engine)

# FLASK set up
app = Flask(__name__)

# Defining Flask Routes
@app.route('/')
def home():
    """ List of all available routes """
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/<start> (start date as '%Y-%m-%d')<br/>"
        f"/api/v1.0/<start>/<end>(start/end date as '%Y-%m-%d')"
        )
@app.route('/api/v1.0/precipitation')
def pcpt():
    """ Query the dates and precipitation observations from last year"""
    # Latest date of measurement data
    latest_date1, = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
    # date@ one year before latest measurement
    year_range = latest_date1 - dt.timedelta(days=365)

    # Querying date and pcpt from db
    results_prcp = session.query(Measurement.date, func.avg(Measurement.prcp)).\
                                filter(Measurement.date>year_range).\
                                group_by(Measurement.date).all()

    # Creating dictionary from query results and append to a list
    results=[]
    for result in results_prcp:
        x=list(np.ravel(result))
        date_str = dt.datetime.strftime(x[0],  "%Y-%m-%d")
        prcp_dict={}
        prcp_dict[date_str] = '{:.4f}'.format(x[1])
        results.append(prcp_dict)
    # jsonify the data and return 
    return jsonify(results)

@app.route('/api/v1.0/stations')
def stations():
    """ Query the Weather Stations Details"""
    results_stations = session.query(Station).all()
    # Creating dict from query results
    stations=[]
    for station in results_stations:
        st_dict={}
        st_dict['station_id'] = station.station_id
        st_dict['name'] = station.name
        st_dict['latitude'] = station.latitude
        st_dict['longitude'] = station.longitude
        st_dict['elevation'] = station.elevation
        stations.append(st_dict)
    return jsonify(stations)

@app.route('/api/v1.0/tobs')
def tobs():
    """ Returns the dates and temperature observations from last year"""
    # Latest date of measurement data
    latest_date2, = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
    # date@ one year before latest measurement
    year_range = latest_date2 - dt.timedelta(days=365)
    # Querying date and pcpt from db
    results_tobs = session.query(Measurement.date, func.avg(Measurement.tobs)).\
                                filter(Measurement.date>year_range).\
                                group_by(Measurement.date).all()

    # Creating dictionary from query results and append to a list
    results=[]
    for result in results_tobs:
        x=list(np.ravel(result))
        date_str = dt.datetime.strftime(x[0],  "%Y-%m-%d")
        tobs_dict={}
        tobs_dict['Date'] = date_str
        tobs_dict['Temperatures'] = '{:.2f}'.format(x[1])
        results.append(tobs_dict)
    # jsonify the data and return 
    return jsonify(results)

@app.route('/api/v1.0/<start>', defaults={'end': None})
@app.route('/api/v1.0/<start>/<end>')
def temps(start, end=None):
    """Returns the min, max and avg temperature from start date or 
        between start/end date range
        ARGS: start, end: start date / end date ('%Y-%m-%d')
        RETURNS: JSONIFIED dictionary containing min, max and avg temps. 
        """
    # Latest date of measurement data
    latest_date3, = session.query(Measurement.date).order_by(Measurement.date.desc()).first()

    start_date = dt.datetime.strptime(start, "%Y-%m-%d")
    if end is None:
        end_date = latest_date3
    else:
        end_date = dt.datetime.strptime(end, "%Y-%m-%d")
    
    results_temps = session.query(Measurement.date, func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)).\
                    filter(Measurement.date>start_date).filter(Measurement.date<=end_date).\
                    group_by(Measurement.date).all()
    results=[]
    for result in results_temps:
        date_str = dt.datetime.strftime(result[0],  "%Y-%m-%d")
        datetob_dict={}
        datetob_dict['Date'] = date_str
        datetob_dict['Min. Temp'] = result[1]
        datetob_dict['Avg. Temp'] = result[2]
        datetob_dict['Max. Temp'] = result[3]
        results.append(datetob_dict)
    # jsonify and return
    return jsonify(results)

if __name__ == '__main__':
    app.run(debug=True)