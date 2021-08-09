
# This was to pull all forecast locations and write it to a text file to find an individual city (UK only)

import requests
import json


response = requests.get('http://datapoint.metoffice.gov.uk/public/data/val/wxfcs/all/json/sitelist?key='+APIkey)
json_data = json.loads(response.text)
with open('locations.txt','w') as f:
    for i in json_data['Locations']['Location']:
        f.write(str(i['name'])+str(i['id']))
        f.write('\n')
    f.close()