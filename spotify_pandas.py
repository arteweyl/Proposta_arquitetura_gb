import requests
import os
import base64
import json
import pandas as pd
from dotenv import load_dotenv

"""Este aquivo existe para mostrar uma forma de resolução diferente utilizando pandas para esse caso específico
"""


load_dotenv('ingestion/.env')

client_id = os.getenv('CLIENT_ID')
client_secret = os.getenv('CLIENT_SECRET')


def get_token():
    auth_string = client_id + ":" + client_secret
    auth_bytes = auth_string.encode("ascii")
    auth_base64 = str(base64.b64encode(auth_bytes),'ascii')
    url = "https://accounts.spotify.com/api/token"
    headers = {
        "Authorization": f"Basic {auth_base64}",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    data = {"grant_type": "client_credentials"}
    result = requests.request('POST', url=url, headers=headers, data=data)
    json_result = json.loads(result.content)
    token = json_result["access_token"]
    return token

def get_auth_header(token):
    return {'Authorization': 'Bearer ' + token }

def search_for_term(token,id):
    url = "https://api.spotify.com/v1/search"
    headers = get_auth_header(token)
    query = f"?q={id}&type=show&market=BR&limit=1"
    query_url = url + query
    result = requests.get(query_url,headers=headers)
    json_result = json.loads(result.content)['shows']['items']
    return json_result

def get_episodes_by_show(token,show_id):
    url = f'https://api.spotify.com/v1/shows/{show_id}/episodes?&limit=50'
    headers = get_auth_header(token)
    result = requests.get(url,headers=headers)
    json_result = json.loads(result.content)['items']
    return json_result

token = get_token()
buscas=search_for_term(token,'Data%Hackers')
show_id=[busca['id'] for busca in buscas if busca is not None ].pop()
print(show_id)
episodes = get_episodes_by_show(token,show_id)
episodes


data_table_1 = {
    'id': [episode['id'] for episode in episodes],
    'name': [episode['name'] for episode in episodes],
    'description': [episode['description'] for episode in episodes],

}
data_table_2 = {
    'id': [episode['id'] for episode in episodes],
    'name': [episode['name'] for episode in episodes],
    'description': [episode['description'] for episode in episodes],
    'release_date': [episode['release_date'] for episode in episodes],
    'duration_ms': [episode['duration_ms'] for episode in episodes],
    'language': [episode['language'] for episode in episodes],
    'explicit': [episode['explicit'] for episode in episodes],
    'type': [episode['type'] for episode in episodes],
}

df1 = pd.DataFrame(data_table_1)
df2 = pd.DataFrame(data_table_2)

filtered_df = df2[df2['description'].str.contains('boticario', case=False)]
print(filtered_df)
print(df2)