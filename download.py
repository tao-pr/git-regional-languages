"""
Github data streamer
Download raw data to DB

@auther StarColon Projects
"""

from gitapi import datamine
from termcolor import colored
from pymongo import MongoClient
import pymongo

def init_mongo(host,dbname,collection):
	cl = MongoClient('{0}:27017/'.format(host))
	db = cl[dbname] 
	return db[collection]


def get_recent_repo_id(collection):
	if collection.count()==0:
		return 0
	return collection.find().sort('_id',pymongo.DESCENDING)[0]['id']


if __name__ == '__main__':
	
	# Sequentially download the data to MongoDB
	mongo = init_mongo('mongodb://localhost','gitlang','repos')
	repo_start_id = get_recent_repo_id(mongo)
	num_batch = 3000 # TAODEBUG: limit

	print(colored("Start downloading from #{0}".format(repo_start_id),"cyan"))

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

