events = spark.read.csv("Data/event-type.csv", inferSchema=True, header=True)
events = events.withColumn('triming', 
events.event_type[0:11])
events = events.drop(events.triming)
events = events.withColumn('stype', 
events.event_type[12:20])
events = events.drop(events.event_type)
events = events.withColumn('event_type', 
events.stype)
events = events.drop(events.stype)
events.createOrReplaceTempView("Events")

severities = spark.read.csv("Data/severity-type.csv", inferSchema=True, header=True)
severities = severities.withColumn('triming', 
severities.severity_type[14:20])
severities = severities.drop(severities.triming)
severities = severities.withColumn('stype', 
severities.severity_type[14:20])
severities = severities.drop(severities.severity_type)
severities = severities.withColumn('severity_type', 
severities.stype)
severities = severities.drop(severities.stype)
severities.createOrReplaceTempView("Severes")

logs = spark.read.csv("Data/log-feature.csv", inferSchema=True, header=True)
logs = logs.withColumn('triming', 
logs.log_feature[0:8])
logs = logs.withColumn('stype', logs.log_feature[9:20])
logs = logs.drop(logs.log_feature)
logs = logs.withColumn('log_feature', logs.stype)
logs = logs.drop(logs.stype)
logs = logs.drop(logs.triming)
logs.createOrReplaceTempView("Logs")

resources = spark.read.csv("Data/resource-type.csv", inferSchema=True, header=True)
resources = resources.withColumn('triming', 
resources.resource_type[0:13])
resources = resources.withColumn('stype', resources.resource_type[14:20])
resources = resources.drop(resources.resource_type)
resources = resources.withColumn('resource_type', resources.stype)
resources = resources.drop(resources.stype)
resources = resources.drop(resources.triming)
resources.createOrReplaceTempView("Resources")

trains = spark.read.csv("Data/train.csv", inferSchema=True, header=True)
trains = trains.withColumn('triming', 
trains.location[0:9])
trains = trains.withColumn('stype', trains.location[9:20])
trains = trains.drop(trains.location)
trains = trains.withColumn('location', trains.stype)
trains = trains.drop(trains.stype)
trains = trains.drop(trains.triming)
trains.createOrReplaceTempView("Train")

tests = spark.read.csv("Data/test.csv", inferSchema=True, header=True)
tests = tests.withColumn('triming', 
tests.location[0:9])
tests = tests.withColumn('stype', tests.location[9:20])
tests = tests.drop(tests.location)
tests = tests.withColumn('location', tests.stype)
tests = tests.drop(tests.stype)
tests = tests.drop(tests.triming)
tests.createOrReplaceTempView("Test")
