import sys
import socket
import time
import requests
import os
import re
import json

TCP_IP = "0.0.0.0"
TCP_PORT = 9999
conn = None
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")
# if the connection is accepted, proceed
conn, addr = s.accept()
print("Connected... Starting sending data.")
data = ['language', 'full_name', 'pushed_at', 'stargazers_count', 'description']
dic = []
my_lang = ['Python', 'Java', 'JavaScript']
token = os.getenv('TOKEN')
while True:
    try:
        token = os.getenv('TOKEN')
        url = 'https://api.github.com/search/repositories?q=+language:{$Pogramming Language}&sort=updated&order=desc&per_page=50'
        language_res = requests.get(url, headers={"Authorization": "token "})
        languages_json = language_res.json()

        for old_dict in languages_json['items']:
            if old_dict['language'] in my_lang:
                dict_you_want = {your_key: old_dict[your_key] for your_key in data}

                dic.append(dict_you_want)
                sending = f"{json.dumps(dict_you_want)}\n".encode()
                conn.send(sending)
                print(sending)
                time.sleep(3)
    except KeyboardInterrupt:
        s.shutdown(socket.SHUT_RD)
