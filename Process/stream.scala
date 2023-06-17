import pyspark.sql.functions as F
import pyspark.sql.types as T

trainSchema = trains.schema
str_train = (spark.readStream.schema(trainSchema).option("maxFilesPerTrigger", 1).csv("data/cleans/"))
train_count = str_train.groupby('fault_severity')
