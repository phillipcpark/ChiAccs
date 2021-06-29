from time import struct_time
from datetime import datetime as dt
from pytz import timezone
from pyspark.sql import DataFrame, SQLContext, functions as F, types as T
from sklearn.cluster import KMeans


@F.udf(T.StructType([T.StructField("tm_year", T.IntegerType(), False),\
                     T.StructField("tm_mon", T.IntegerType(), False),\
                     T.StructField("tm_mday", T.IntegerType(), False),\
                     T.StructField("tm_hour", T.IntegerType(), False),\
                     T.StructField("tm_yday", T.IntegerType(), False),\
                     T.StructField("tm_wday", T.IntegerType(), False)]))
def datestring_to_dict(date: str):
    time_format = '%Y-%m-%dT%H:%M:%S.%f'
    date_comps  = dt.strptime(date, time_format).timetuple()
    return (date_comps.tm_year, date_comps.tm_mon, date_comps.tm_mday,\
            date_comps.tm_hour, date_comps.tm_yday, date_comps.tm_wday)

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

#
# non UDFs, but are used in conjunction, or have a similar transform role 
#

# slice datestring into components and distribute into columns named by time.struct_time attributes 
def distr_datestring(df: DataFrame, datestring_key: str) -> DataFrame:
    placeholder = 'date_comps' 
    df          = df.withColumn(placeholder, datestring_to_dict(F.col(datestring_key)))\
                    .select('*', placeholder+'.*')
    df = df.drop(placeholder) 
    return df

# clusters rows in target_df, based on feature_keys, then create new column in target with cluster IDs
def cluster_join(context: SQLContext, target_df: DataFrame, feature_keys: list, \
                 target_key: str, join_key: str, num_clusts: int) -> DataFrame: 

    features = target_df.select(feature_keys).collect()
    labels   = KMeans(n_clusters = num_clusts).fit(features).labels_.tolist()

    join_vals = target_df.select(join_key).collect()
    to_join   = [{target_key: labels[row_idx], join_key: join_vals[row_idx][join_key]} \
                  for row_idx in range(len(labels))]     

    to_join         = context.createDataFrame(data=to_join)
    target_w_labels = target_df.join(to_join, join_key)
    return target_w_labels 


