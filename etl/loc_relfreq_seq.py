import etl_utils as utils
import ca_udfs as udfs
from ca_extract import load_dframe
from pyspark.sql import functions as F

#
# pipeline creates multivariate time series of relative accident frequencies in each city subdivision,
# over multiple timescales relative to several lags
#
if __name__=='__main__':
    args     = utils.parse_args()
    config   = utils.load_config(args['config_path'])
    contexts = utils.init_contexts()
    ca_df    = load_dframe(config, contexts)

    #extract date components, consistent with time.struct_time attributes 
    ca_df = ca_df.withColumn('date_attributes', udfs.datestring_to_dict( F.col('crash_date') ))

    #integer representation of accident date, for sorting and windowing
    ca_df = ca_df.withColumn('hours_since_uepoch', udfs.hours_since_uepoch(F.col('crash_date')))
    ca_df = ca_df.sort('hours_since_uepoch', ascending=False)
    
    ca_df.show(100)    
