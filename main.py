import findspark
from pyspark import SparkContext
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
import pyspark.sql.types as T


def main():
    
    # findspark.init()    
    sc = SparkContext()
    print(sc)
    


if __name__ == '__main__':
    main()
