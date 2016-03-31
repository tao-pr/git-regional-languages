/**
 * MongoDB record MapReduce task runner
 *
 * @author StarColon Projects
 */

var colors = require('colors');
var mongo = require('mongoskin');
var _ = require('underscore');


var MapReduce = {}

MapReduce.db = function(serverAddr,dbName){
	return mongo.db(serverAddr + '/' + dbName)
}

MapReduce.doEach = function(db,collection){
	return function(f){
		db.collection[collection].mapReduce // TAOTODO: How?
	}
}





module.exports = MapReduce;