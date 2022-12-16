from datetime import datetime, timedelta
import re

def get_todays_first_dt_path() :
    timenow = datetime.now()
    str_time = str(timenow)
    clean_time = re.sub("[^0-9]","" , str_time)
    short_time = clean_time [0:8]
    todays_dt = str('dt=' + short_time + '00/')
    todays_dt_path = ('s3://sovrn-prd-ue2-general-data/weblog-superset/datasource=requests/' + todays_dt)
    print ('todays dt path is ' + todays_dt_path)
    return todays_dt_path
    
def get_todays_first_dt() :
    timenow = datetime.now()
    str_time = str(timenow)
    clean_time = re.sub("[^0-9]","" , str_time)
    short_time = clean_time [0:8]
    todays_dt = str('dt_' + short_time + '00')
    return todays_dt