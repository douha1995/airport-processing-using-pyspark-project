from spark import startSpark

class Log4j:
    def __init__(self, sparkSession):
        sparkconfig= sparkSession.SparkContext.getConf
        appName= sparkconfig.get('spark.app.name')
        appId = sparkconfig.get('spark.app.id')
        log4j = sparkSession._jar.org.apche.log4j
        prefixMsg = '<' + appName +', ' + appId + '> '
        self.log= log4j.logManager.getLogger(prefixMsg)

    def warn(self, msg):
        self.log.warn(msg)

    def error(self, msg):
        self.log.error(msg)
    
    def info(self, msg):
        self.log.info(msg)

