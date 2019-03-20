from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions


# Create a Glue context
glueContext = GlueContext(SparkContext.getOrCreate())

# Create a DynamicFrame using the uber' table
uber_dyf = glueContext.create_dynamic_frame.from_catalog(database="aafc-okorol-nyc-uber-db2", table_name="uber_nyc_data_csv")
uber_df=uber_dyf.toDF()
print uber_df.printSchema()
uber_df.show(100)

uber_df1=uber_df.drop(uber_df.id)
uber_df1=uber_df.dropna(how='any')
uber_df1=uber_df1.filter((uber_df1.destination_taz != 'NULL')  & 
    (uber_df1.origin_taz != 'NULL')  & 
    (uber_df1.trip_duration != 'NULL') )
uber_df1.show(100)

from pyspark.sql.functions import udf, to_timestamp
from pyspark.sql.types import FloatType, LongType, IntegerType
from datetime import datetime
 
distance=udf(lambda x: x[0], FloatType())

def day(x):
    pickup=datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
    return int(pickup.date().day)
    
pickup_day = udf(day, IntegerType())

def month(x):
    pickup=datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
    return int(pickup.date().month)
    
pickup_month = udf(month, IntegerType())

def hour(x):
    pickup=datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
    return int(pickup.time().hour)
    
pickup_hour = udf(hour, IntegerType())

def duration_seconds(x):
    time=x.split(':')
    duration = long(int(time[0]) * 3600 + int(time[1]) * 60 + int(time[2]))
    return duration
duration=udf(duration_seconds, LongType())

origin=udf(lambda x: int(x,16), IntegerType())
destination=udf(lambda x: int(x,16), IntegerType())

uber_df2 = uber_df1.select(origin(uber_df1.origin_taz).alias('origin'), 
    destination(uber_df1.destination_taz).alias('destination'), 
    pickup_month(uber_df1.pickup_datetime).alias('month'), 
    pickup_day(uber_df1.pickup_datetime).alias('day'), 
    pickup_hour(uber_df1.pickup_datetime).alias('hour'), 
    distance(uber_df1.trip_distance).alias('distance'),
    duration(uber_df1.trip_duration).alias('duration'))

uber_df2.limit(100).show(100)
uber_df2.count()

# after s3:// is te bucket name that I have created in S3  (/uber is not part of it)
uber_df2.write.save("s3://aafc-okorol-nyc-uber-bucket2/uber", format='csv', header=True)

