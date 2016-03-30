"""
Github data streamer
Download raw data to DB

@auther StarColon Projects
"""

from gitapi import datamine
from termcolor import colored

def init_mongo():
	pass


if __name__ == '__main__':
	
	# TAOTODO: Sequentially download the data to MongoDB
	repo_start_id = None
	num_batch = 300

	while num_batch>0:
		print(colored("******* #{0} BATCHES TO GO *********",'cyan'))
		print("")

		repos = datamine.get_repo_owner_and_langs(repo_start_id)

		# Port all repos to MongoDB
		##

		# Next segment
		repo_start_id = repos[-1]['id']
		num_batch -= 1