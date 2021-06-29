import requests
import pyspark

# construct URL for SODA request 
def url_from_fields(url, fields:list=None, limit=None):
    if not(fields is None):
        url += "?$select="
        for field in fields:
            url += field
            if not(field is fields[-1]):
                url += ","
    if not(limit is None):
        url += "&$limit=" + str(limit)
    return url

# make get request, returning list of per-accident JSONs
def request_get(url)->list:
    response = requests.get(url)
    response.raise_for_status() 

    r_json_list = response.json() 
    return r_json_list

# load dataframe based on config read from file 
def load_dframe(config: dict, contexts: dict) -> pyspark.sql.DataFrame:
    request_url = url_from_fields(url    = config["extract"]["url"], \
                                  fields = config["extract"]["fields"], \
                                  limit  = config["extract"]["limit"])
    df = request_get(request_url)
    df = contexts['spark'].parallelize(df, numSlices=config["transform"] \
                                                           ["compute_dimensions"] \
                                                           ["parallel_slices"])           
    df = contexts['sql'].read.json(df)
    df = df.dropna()

    # FIXME need more general approach for casting
    df = df.withColumn('latitude', df['latitude'].cast("double"))
    df = df.withColumn('longitude', df['longitude'].cast("double"))
    return df
        
