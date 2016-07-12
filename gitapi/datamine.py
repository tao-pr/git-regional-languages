"""
Github Information Retrieval

@author StarColon Projects
"""

from . import get
from pprint import pprint
from termcolor import colored

def get_repo_owner_and_langs(since=None):
	repos = get.repos(since)

	def _take_owner_and_langs(repo):
		repo_id       = repo['id']
		repo_name     = repo['name']
		repo_owner    = get_user_info(repo['owner']['login'])
		repo_langs    = get_repo_langs(repo['name'],repo['owner']['login'])

		info = {
			'id':    repo_id,
			'name':  repo_name,
			'owner': repo_owner,
			'langs': repo_langs
		}
		# TAODEBUG:
		print(colored('********','cyan'))
		pprint(info)

		return info

 
	# Take languages / owner of each repo
	return [_take_owner_and_langs(repo) for repo in repos]


def get_user_info(usr):
	user = get.user(usr)

	if 'login' not in user:
		return {
			'login':     None,
			'type':      None,
			'name':      None,
			'location':  "",
			'repos':     0,
			'followers': 0
		}

	# Take the brief info
	return {
		'login':	      user['login'],
		'type':         user['type'], # User
		'name':         user['name'],
		'location':     user['location'],
		'repos':        user['public_repos'], # number
		'followers':    user['followers'] # number
	}

def get_repo_langs(name,owner):
	langs = get.repo_langs(name,owner)
	return langs

