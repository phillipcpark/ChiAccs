import etl_utils as utils
from ca_extract import load_dframe

#
# pipeline creates multivariate time series of relative accident frequencies in each city subdivision,
# over multiple timescales relative to several lags
#
if __name__=='__main__':
    args     = utils.parse_args()
    config   = utils.load_config(args['config_path'])
    contexts = utils.init_contexts()

    ca_df = load_dframe(config, contexts)
    
