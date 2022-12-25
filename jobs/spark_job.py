from dependencies.spark import startSpark
import pandas as pd
import numpy as np


  
def main():  
    sparkSessin, configDict, sparkLog = startSpark(app_name='airport_processing_job',
        files=['configs/etl_config.json'])

    # log that main ETL job is starting
    sparkLog.warn('etl_job is up-and-running')
    pandTemp = pd.DataFrame(np.random.random(10))
    aparkTemp = sparkSessin.createDataFrame(pandTemp)

    #print the tables in spark catalog
    print(sparkSessin.catalog.listTables())

    sparkSessin.createOrReplaceTempView('temp_tabl')
    #print the tables in spark catalog
    print(sparkSessin.catalog.listTables())
    airportPath = './data/airports.csv'
    sparkLog.warn('airport extraction job is up-and-running')

    airports = extractData(sparkSessin, airportPath)
    print(sparkSessin.catalog.listDatabases())
    #print the tables in spark catalog
    print(sparkSessin.catalog.listTables())
    flightPath = './data/flight_small.csv'
    sparkLog.warn('flights extraction job is up-and-running')

    flights = extractData(sparkSessin, flightPath)
    flights.show()

    sparkLog.warn('flights transformation job is up-and-running')
    sparkLog.warn('convert datatype of fights column to float')
    transData = filterData(transData, configDict['column_to_convert'])

    sparkLog.warn('add new calculated column based on air_time column (convert it to hours)')
    transData = transformedData(flights)

    sparkLog.warn('filter fights based on distance ')
    transData = filterData(transData, configDict['distance'])

    sparkLog.warn('calculate average of fights time ')
    transData = avgSpeedOfFlight(transData)

    sparkLog.warn('load data job is runing up ')
    transData = loadData(transData)

    
def extractData(sparkSessin, filePath):
    airports = sparkSessin.read.option(header = True).csv(filePath)
    #airports.show()
    return airports
def transformedData(flights):
    #flightsName = flights.createOrReplaceTempView('small_flights')
    #print the tables in spark catalog
    #print(sparkSessin.catalog.listTables())

    #flightDF = sparkSessin.table('small_flights')
    #flightDF.show()

    #add new calculated column based on air_time column 
    flights = flights.withColumn('duration_hrs', flights.air_time / 60)
    flights.show()
    flights.describe().show()
    return flights
def filterData(flights, distance):
    # filter flights by distance more than 1000
    #flightsFilterDist = flights.filter('distance > 1000')
    flightsFilterDist = flights.filter(flights.distance > distance)
    flightsFilterDist.show()

    #select only three columns from flight datasets
    selectedFlight = flightsFilterDist.select('tailnum','origin','dest')

    #filter flight from SEA to PDX
    flightsOriginDest = selectedFlight.filter(selectedFlight.origin == 'SEA').filter(selectedFlight.dest == 'PDX')
    return flightsOriginDest
def avgSpeedOfFlight(flights):

    #Create a table of the average speed of each flight both ways.
    # you can use selectExpr()
    avg_speed = (flights.distance/(flights.air_time/60)).alias("avg_speed")
    speedAvg = flights.select('origin', 'dest', 'tailnum', avg_speed)
    #speedAvgTbl.show()
    return speedAvg
def convertTypeToFloat(flights, col = []):
    if len(col) == 0:
        return flights

    flights.describe()
    #arr_time: string and distance: string, so to find min() and max() we need to convert this float 
    flights =flights.withColumn(col[0], flights.col[0].cast('float'))
    flights = flights.withColumn(col[1], flights.col[1].cast('float'))
    flights.describe(col).show()
    return flights

def shortDistanceOfFlight(flights, originPlace):
    #Find the length of the shortest (in terms of distance) flight that left PDX 
    flights = flights.filter('origin' == originPlace).groupBy().min('distance')
    return flights
def longTimeOfFlight(flights, originPlace):
    #Find the length of the longest (in terms of time) flight that left SEA
    flights = flights.filter('origin' == originPlace).groupBy().min('air_time')
    return flights

    #get the average air time of Delta Airlines flights  that left SEA.
    #flights.filter('carrier' == 'DL').filter('origin' == 'SEA').groupBy().avg('air_time').show()
def loadData(transformedDF):
    transformedDF.coalesc(1).write.csv('./data/output/data_after_transform', header= True, mode = 'overwrite')


if __name__=='__main__':
    main()






