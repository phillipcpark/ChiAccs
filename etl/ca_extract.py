import requests

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

# make get request
def request_get(url)->list:
    response = requests.get(url)
    response.raise_for_status() 

    r_json_list = response.json() 
    return r_json_list
    
