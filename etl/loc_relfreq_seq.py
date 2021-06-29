import etl_utils as utils
import ca_udfs as udfs
from ca_extract import load_dframe
from pyspark.sql import functions as F

#
# pipeline loads multivariate time series of relative accident frequencies in each city subdivision,
# over multiple timescales relative to several lags
#
if __name__=='__main__':
    args     = utils.parse_args()
    config   = utils.load_config(args['config_path'])
    contexts = utils.init_contexts()
    ca_df    = load_dframe(config, contexts)

    # extract date components from datestring and distribute to columns   
    ca_df = udfs.distr_datestring(ca_df, 'crash_date')
 
    # cluster accidents based on coordinates, to subdivide city
    clust_attrs = ['latitude', 'longitude']
    ca_df       = udfs.cluster_join(context      = contexts['sql'],\
                                    target_df    = ca_df,\
                                    feature_keys = clust_attrs, \
                                    target_key   = 'cluster_id', \
                                    join_key     = 'crash_record_id', \
                                    num_clusts   = config['transform']['clustering']['cluster_count']) 

    # scalar representation of date, for sorting and windowing 
    ca_df = ca_df.withColumn('utc_timestamp', udfs.hours_since_uepoch(F.col('crash_date')))
    ca_df = ca_df.sort('utc_timestamp', ascending=False)

    ca_df.show(100)
   
