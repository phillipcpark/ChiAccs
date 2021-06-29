import etl_utils as utils
import ca_udfs as udfs
from ca_extract import load_dframe
from pyspark.sql import functions as F

from copy import deepcopy

import sys #NOTE debug

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

    #
    # NOTE proto windowing
    #
    from datetime import datetime as dt
    from datetime import timedelta
    from pytz import timezone
    from dateutil.relativedelta import relativedelta

    # for now, number of steps is predetermined, but will need logic later on to automatically infer
    steps   = 20
    stride  = timedelta(hours=-2)
    lags    = [relativedelta(years=-1) + timedelta(days=-1), relativedelta(years=-1) + relativedelta(weeks=-1)]
    win_szs = [timedelta(days=14), relativedelta(weeks=4)] 

    latest_acc = ca_df.select('crash_date').head()['crash_date']

    # create datetime instance, and set tz to Chicago
    naive_dt  = dt.strptime(latest_acc,'%Y-%m-%dT%H:%M:%S.%f')
    target_tz = timezone('America/Chicago')
    chi_dt    = target_tz.localize(naive_dt)
    curr_pos  = chi_dt #assume stride-aligned, for now     

    for step in range(steps):
        for lag in lags:
            for win_sz in win_szs:
                win_end   = curr_pos + lag 
                win_start = win_end - win_sz

                win_end   = win_end.timestamp()
                win_start = win_start.timestamp()
                
                win = ca_df.filter(ca_df['utc_timestamp'].between(win_start, win_end))
        curr_pos = curr_pos + stride









