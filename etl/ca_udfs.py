from time import struct_time
from datetime import datetime as dt
from pytz import timezone
from pyspark.sql import DataFrame, SQLContext, functions as F, types as T
from sklearn.cluster import KMeans

@F.udf(T.MapType(T.StringType(), T.IntegerType()))
def datestring_to_dict(date: str):
    time_format = '%Y-%m-%dT%H:%M:%S.%f'
    date_as_dt  = dt.strptime(date, time_format)
    date_dict   = struct_time_as_dict(date_as_dt.timetuple())
    return date_dict

@F.udf(T.IntegerType())
def hours_since_uepoch(date: str):
    time_format = '%Y-%m-%dT%H:%M:%S.%f'
    time_zone   = 'America/Chicago'

    # create datetime instance, and set tz to Chicago
    naive_dt  = dt.strptime(date, time_format)
    target_tz = timezone(time_zone)
    chi_dt    = target_tz.localize(naive_dt)

    # NOTE posix timestamp; will be wrt UTC 
    utc_stamp = dt.timestamp(chi_dt) 
    return int(utc_stamp)

# convert time.struct_time instances to dictionaries, with keys consistent with attributes 
def struct_time_as_dict(time: struct_time):
    st_dict = {
               'tm_year':   time.tm_year, 
               'tm_yday':   time.tm_yday, 
               'tm_hour':   time.tm_hour, 
               'tm_min' :   time.tm_min,
               'tm_mon':    time.tm_mon,  
               'tm_mday':   time.tm_mday, 
               'tm_wday':   time.tm_wday, 
               'tm_is_dst': time.tm_isdst 
              } 
    return st_dict

# clusters rows in target_df, based on feature_keys, then create new column in target with cluster IDs
def cluster_join(context: SQLContext, target_df: DataFrame, \
                 feature_keys: list, target_key: str, join_key: str) -> DataFrame: 

    features = target_df.select(feature_keys).collect()
    labels   = KMeans().fit(features).labels_.tolist()

    join_vals = target_df.select(join_key).collect()
    to_join   = [{target_key: labels[row_idx], join_key: join_vals[row_idx][join_key]} \
                  for row_idx in range(len(labels))]     

    to_join         = context.createDataFrame(data=to_join)
    target_w_labels = target_df.join(to_join, join_key)
    return target_w_labels 


