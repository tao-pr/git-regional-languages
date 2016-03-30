"""
Github API consumer - by service

@author StarColon Projects
"""

import call as GithubAPI

def repos(since=None):
	if not since:
		return GithubAPI.call('repositories')
	else:
		return GithubAPI.call('repositories?since={0}'.format(since))

def repo_langs(repo,owner):
	return GithubAPI.call('repos/{0}/{1}/languages'.format(owner,repo))

def user(name):
	return GithubAPI.call('users/{0}'.format(name))

