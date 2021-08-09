import requests
import json
import mariadb
import time

# Retrieve username and password from a json file to keep hidden

f = open('db_credentials.json')
db_credentials = json.load(f)

# Connect to the database
conn = mariadb.connect(
        user=db_credentials['username'],
        password=db_credentials['password'],
        host="localhost",
        port=3306,
        database="garden")

cur = conn.cursor()
# Reference guide here: https://www.metoffice.gov.uk/binaries/content/assets/metofficegovuk/pdf/data/datapoint_api_reference.pdf

location = '310048' # For Chester, Uk. For other locations, need to pull full sitelist to get code.

resource = 'val/wxfcs/all/json/'+location

res = '?res=3hourly&'

APIkey = 'ff1620a2-a58b-48d1-9380-d6cb6b30f0af' # Import this from a text file later on

url = 'http://datapoint.metoffice.gov.uk/public/data/'+resource+res+'key='+APIkey

response = requests.get(url)
json_data = json.loads(response.text)

forecasts = json_data['SiteRep']['DV']

# Extract variables from the JSON data
t = time.localtime()
retrieval_date = time.strftime('%Y-%m-%d %H:%M:%S',t)
data_type = forecasts['type']
area = forecasts['Location']['name']
country = forecasts['Location']['country']
for day in forecasts['Location']['Period']:
    forecast_date = day['value'][:-1]
    for obs in day['Rep']:
        forecast_time = time.strftime('%H:%M:%S',time.gmtime(int(obs['$'])*60))
        wind_direction = obs['D']
        feels_like = obs['F']
        gust = obs['G']
        humidity = obs['H']
        precip = obs['Pp']
        wind_speed = obs['S']
        temp = obs['T']
        vis = obs['V']
        weather_type = obs['W']
        uv = obs['U']
        
        # Once variables extracted, insert into the Database
        
        cur.execute("INSERT INTO WeatherForecast VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (retrieval_date, data_type, area, country, forecast_date, str(forecast_time), wind_direction, feels_like,
                     gust, humidity, precip, wind_speed, temp, vis, weather_type, uv))

# Delete duplicate records where we ahve the same ForecastDate and ForecastTime by taking the record with the more recent RetrieveDate

cur.execute("DELETE t1 FROM WeatherForecast t1 INNER JOIN WeatherForecast t2 WHERE t1.RetrievedDate < t2.RetrievedDate AND t1.ForecastDate = t2.ForecastDate AND t1.ForecastTime =t2.ForecastTime;") 

# Commit the changes to save all updates

cur.execute("COMMIT;")

# Close the connection

cur.close()


