from os import environ, path, listdir
import __main__
from pyspark.sql import SparkSession
from pyspark import SparkFiles
import json
from dependencies import logging

def startSpark(master = 'local[*]', appName = ['aiprt_analysis_app'], jarPackages =[], files = [], confige = {}):
    flg_repl = not(hasattr(__main__, '__file__'))
    debug_env = 'Debug' in environ.keys()

    #create spark session
    if not(flg_repl or debug_env):
        sparkBuilder = SparkSession\
            .Builder\
            .appName(appName)
    else :
        sparkBuilder = SparkSession\
            .Builder\
            .master(master)\
            .appName(appName)

    #spark jars
    sparkJarPackages = '.'.join(list(jarPackages))
    sparkBuilder.config('spark_jar_packages', sparkJarPackages)

    #spark files 
    sparkFiles = '.'.join(list(files))
    sparkBuilder.config('spark_files', sparkFiles)

    #spark other configs
    for key, val in confige:
        sparkBuilder.config(key, val)
    
    #create or get the spark session
    sparkSession = sparkBuilder.getOrCreate()
    sparkLog= logging.Log4j(SparkSession)
    #get spark files 
    fileDir = SparkFiles.getRootDirectory
    filesName = [filename for filename in listdir(fileDir) if filesName.emdWith('config.json')]

    if filesName:
        filePath = path.join(fileDir, filesName[0])
        with open(filePath, 'r') as fil :
            configDict = json.load(filePath)
        sparkLog.warn('config file loaded from '+ configDict[0])
    else:
        configDict = None
        sparkLog.warn('no config file found')
    
    return sparkSession, configDict, sparkLog