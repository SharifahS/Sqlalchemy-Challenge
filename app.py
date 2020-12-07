
from scipy import stats
import datetime as dt
import pandas as pd
import numpy as np

import sqlalchemy
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from sqlalchemy.ext.automap import automap_base

from flask import Flask, jsonify


# create engine 
engine = create_engine("sqlite:///Resources/hawaii.sqlite") 

# reflect an existing database into a new model
# reflect the tables
Base = automap_base()
Base.prepare(engine, reflect=True)

# Save reference to the table
measurement = Base.classes.measurement 
station = Base.classes.station

app = Flask(__name__)

@app.route("/")
def Home_Page():
    return (
        f"Welcome!<br/>"
        f"<br/>"
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/start_date<br/>"
        f"/api/v1.0/start_date/end_date<br/>"
    )

@app.route("/api/v1.0/precipitation")
def precipitation():

    #Create our session (link) from Python to the DB
    session = Session(engine)

    # Retrieve the last record date date in dataset
    last = session.query(measurement.date).\
               order_by(measurement.date.desc()).first()

    # Retrieve the first record date from last record date in dataset
    last12 = (dt.datetime.strptime(last[0],'%Y-%m-%d') - dt.timedelta(days = 365)).strftime('%Y-%m-%d')

    # Perform a query to retrieve the date and precipitation for last year's data
    precipitation = session.query(measurement.date, measurement.prcp).\
                    filter(measurement.date >= last12).all()
    
    # Convert the query results to a dictionary using `date` as the key and `prcp` as the value
    prcpData = []
    for date, prcp in precipitation:
            prcpDict = {}
            prcpDict['date'] = date
            prcpDict['prcp'] = prcp
            prcpData.append(prcpDict)
    
    session.close()

    # Return the JSON representation of your dictionary
    return jsonify(prcpData)


"""Return a JSON list of stations from the dataset."""

@app.route("/api/v1.0/stations")
def stations():
    
    #Create our session (link) from Python to the DB
    session = Session(engine)

    # Perform a query to retrieve stations and names data
    station_data = session.query(station.station, station.name).all()
    
    # Convert the query results to a dictionary using `station` and `name`
    statnData = []

    for statn, names in station_data:
        statnDict = {}
        statnDict['station'] = statn
        statnDict['name'] = names
        statnData.append(statnDict)

    session.close()

    # Return the JSON representation
    return jsonify(statnData)


@app.route("/api/v1.0/tobs")
def tobs():
    
    #Create our session (link) from Python to the DB
    session = Session(engine)

    # Identify the most active station
    active = session.query(measurement.station, func.count(measurement.station)).\
                    group_by(measurement.station).\
                    order_by(func.count(measurement.station).desc()).all()
    
    # Retrieve the last record date
    lastactive = session.query(measurement.date).\
                    filter(measurement.station == active[0][0]).\
                    order_by(measurement.date.desc()).first()

    # Retrieve the first record date
    last12active = (dt.datetime.strptime(lastactive[0],'%Y-%m-%d') - \
                            dt.timedelta(days = 365)).strftime('%Y-%m-%d')

    # Retrieve data from previous year for the most active station
    queryActiveStatnData = session.query(measurement.station, measurement.date, measurement.tobs).\
                           filter(measurement.station == active[0][0]).\
                           filter(measurement.date >= last12active).all() 

    # Convert the query results to a dictionary
    tobsData = []

    for statn, dates, tob in queryActiveStatnData:
        tobsDict = {}
        tobsDict['station'] = statn
        tobsDict['date'] = dates
        tobsDict['tobs'] = tob
        tobsData.append(tobsDict)
        
    session.close()

    # Return the JSON representation
    return jsonify(tobsData)


"""
Return a JSON list of the minimum temperature, the average temperature, 
and the max temperature for a given start or start-end range. 
When given the start only, calculate `TMIN`, `TAVG`, and `TMAX` for all dates greater than and equal to the start date."""

@app.route("/api/v1.0/<start_date>")
def StartDates(start_date):
    
    session = Session(engine)
        
    start = session.query(func.min(measurement.tobs),\
                     func.avg(measurement.tobs), func.max(measurement.tobs)).\
                     filter(measurement.date >= start_date).all()

    startData = []

    for tmin, tavg, tmax in start:
        startDict = {}
        startDict['min'] = tmin
        startDict['avg'] = tavg
        startDict['max'] = tmax
        startData.append(startDict)

    session.close()

    # Return the JSON representation of your dictionary
    return jsonify(startData)

""" 
When given the start and the end date, calculate the `TMIN`, `TAVG`, and `TMAX` for dates between the start and end date inclusive."""

@app.route("/api/v1.0/<start_date>/<end_date>")
def StartEndDates(start_date, end_date):
    
    #Create our session (link) from Python to the DB
    session = Session(engine)
    
    startend = session.query(func.min(measurement.tobs),\
                        func.avg(measurement.tobs), func.max(measurement.tobs)).\
                        filter(measurement.date >= start_date).\
                        filter(measurement.date <= end_date).all()
    
    startendData = []

    for tmin, tavg, tmax in startend:
        startendDict = {}
        startendDict['min'] = tmin
        startendDict['avg'] = tavg
        startendDict['max'] = tmax
        startendData.append(startendDict)
 
    session.close()

    # Return the JSON representation of your dictionary
    return jsonify(startendData)


if __name__ == '__main__':
    app.run(debug=True)