import etl_utils as utils
import ca_udfs as udfs
from ca_extract import load_dframe
from pyspark.sql import functions as F

import pandas as pd

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

    steps   = 10 # steps predetermined, need logic to automatically infer
    stride  = timedelta(hours=-24)
    lags    = [ relativedelta(years=-1) + timedelta(days=-1),
                relativedelta(years=-1) + relativedelta(weeks=-1) ]
    win_szs = [ timedelta(days=7),
                relativedelta(weeks=4) ] 

    # need to enumerate all cluster ids, since some may not be observed in window
    clust_ids = ca_df.select('cluster_id').distinct().sort('cluster_id')
    clust_ids = clust_ids.withColumn('rel_freq', F.lit(0.0))
 
    # get starting position; last acc for now, needs to be stride-aligned
    latest_acc = ca_df.select('crash_date').head()['crash_date']

    naive_dt   = dt.strptime(latest_acc,'%Y-%m-%dT%H:%M:%S.%f')
    curr_pos   = timezone('America/Chicago').localize(naive_dt)

    # each (lag, win_sz) pair will have different frame, and will be merged at end
    headers = [row['cluster_id'] for row in clust_ids.select('cluster_id').collect()]

    ts_dfs = { str(lag)+'_'+str(win_sz): [] for lag in lags \
                                            for win_sz in win_szs }

    # pandas faster for sliding windos
    clust_ids = clust_ids.toPandas()
    ca_df = ca_df.toPandas()

    for step in range(steps):
        for lag in lags:
            for win_sz in win_szs:

                # extract window
                win_end   = curr_pos + lag 
                win_start = win_end - win_sz
                win_end   = win_end.timestamp()
                win_start = win_start.timestamp()                
                win = ca_df[ca_df['utc_timestamp'].between(win_start, win_end)]
                
                # compute relative freqs over clusters
                total_accs = win['cluster_id'].count()        
                rel_freqs  = clust_ids[['cluster_id', 'rel_freq']]
            
                if (total_accs == 0):
                    rel_freqs['rel_freq'] = pd.Series([0.125 for i in range(8)])
                else:                
                    rel_freqs['counts']   = win.groupby(['cluster_id']).size() 
                    rel_freqs['counts']   = rel_freqs['counts'].fillna(0.0) 
                    rel_freqs['rel_freq'] = rel_freqs['counts']/float(total_accs)    
                    rel_freqs             = rel_freqs[['cluster_id', 'rel_freq']]

                #good up to here
                print(rel_freqs)
                sys.exit(0)

                curr_key = str(lag) + '_' + str(win_sz)
                ts_dfs[curr_key].append(rel_freqs)        
        curr_pos = curr_pos + stride
  
    # combine dfs
    #df = ts_dfs[str(lags[0]) + '_' + str(win_szs[0])][0]
    #for _df in ts_dfs[str(lags[0]) + '_' + str(win_szs[0])][1:]:
    #    df = df.union(_df)
    #df.show()







