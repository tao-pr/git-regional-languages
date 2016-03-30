"""
Github API executer

@author StarColon Projects
"""

from requests.auth import HTTPBasicAuth
from termcolor import colored
from pprint import pprint
import argparse
import requests
import json
import sys
import os

api_url = "https://api.github.com/"

def relpath(path):
	return os.path.dirname(os.path.realpath(__file__)) + '/' + path

def api_token():
	with open(relpath('../API-TOKEN'),'r') as token:
		return token.read().split('\n')

def call(route):
	token = api_token()
	resp  = requests.get(
		api_url + route,
		auth=HTTPBasicAuth(token[0], token[1])
		)

	return resp.json()


if __name__ == '__main__':
	arguments = argparse.ArgumentParser()
	arguments.add_argument('--route', type=str, default='')
	args = vars(arguments.parse_args(sys.argv[1:]))

	print(colored('*************************','cyan'))
	print(colored('  ' + api_url + args['route'],'cyan'))
	print(colored('*************************','cyan'))

	resp = call(args['route'])

	pprint(resp)
