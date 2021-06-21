import argparse
from json import load
from pyspark import SparkContext
from pyspark.sql import SQLContext

# read CL arguments
def parse_args() -> dict:
    parser = argparse.ArgumentParser()
    parser.add_argument("-cfg", help="path to JSON config for job", required=True, \
                        dest="config_path", metavar="") 
    args = vars(parser.parse_args())
    return args

# read JSON from path
def load_config(path)->dict:
    with open(path, "r") as config_fo:
        config = load(config_fo)
    return config 

# instantiate Spark and SQLContexts for session
def init_contexts():
    sctx     = SparkContext()
    sql_ctx  = SQLContext(sctx)
    contexts = {"spark": sctx, "sql": sql_ctx}   
    return contexts

