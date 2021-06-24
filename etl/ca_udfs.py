from time import struct_time
from datetime import datetime as dt
from pyspark.sql import functions as F, types as T

# convert time.struct_time instances to dictionaries, with keys consistent with attributes 
def struct_time_as_dict(time: struct_time):
    st_dict = {
               'tm_year':   time.tm_year, 
               'tm_yday':   time.tm_yday, 
               'tm_hour':   time.tm_hour, 
               'tm_mon':    time.tm_mon,  
               'tm_mday':   time.tm_mday, 
               'tm_wday':   time.tm_wday, 
               'tm_is_dst': time.tm_isdst 
              } 
    return st_dict

@F.udf(T.MapType(T.StringType(), T.IntegerType()))
def datestring_to_dict(date: str):
    time_format = '%Y-%m-%dT%H:%M:%S.%f'
    date_as_dt  = dt.strptime(date, time_format)
    date_dict   = struct_time_as_dict(date_as_dt.timetuple())
    return date_dict

# NOTE timestamp() returns system's local time. For now, only relative order matters.
@F.udf(T.IntegerType())
def hours_since_uepoch(date: str):
    time_format   = '%Y-%m-%dT%H:%M:%S.%f'
    date_as_dt    = dt.strptime(date, time_format)
    secs_per_hour = 3600
    return int(dt.timestamp(date_as_dt)/secs_per_hour)


