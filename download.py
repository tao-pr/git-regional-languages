"""
Github data streamer
Download raw data to DB

@auther StarColon Projects
"""

from gitapi import datamine
from termcolor import colored
from pymongo import MongoClient

def init_mongo(host,dbname,collection):
	cl = MongoClient('{0}:27017/'.format(host))
	db = cl[dbname] 
	return db[collection]


if __name__ == '__main__':
	
	# Sequentially download the data to MongoDB
	repo_start_id = 61043 #NOTE: Set to `None` to download from very beginning
	num_batch = 16
	mongo = init_mongo('mongodb://localhost','gitlang','repos')

	while num_batch>0:
		print(colored("******* #{0} BATCHES TO GO *********".format(num_batch),'cyan'))
		print("")

		repos = datamine.get_repo_owner_and_langs(repo_start_id)

		# Port all repos to MongoDB
		mongo.insert_many(repos)

		# Next segment
		repo_start_id = repos[-1]['id']
		num_batch -= 1

	print(colored("FINISHED!",'green'))

