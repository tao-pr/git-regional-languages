/**
 * MongoDB record MapReduce task runner
 *
 * @author StarColon Projects
 */

var colors = require('colors');
var mongo = require('mongoskin');
var _ = require('underscore');


var MapReduce = {}

MapReduce.db = function(serverAddr,dbName,collection){
	return mongo.db(serverAddr + '/' + dbName).collection[collection]
}

MapReduce.langDistributionByLocation = function(dbsrc){
	var map = function(){
		Object.keys(this.langs).forEach(function(lang){
			// {key = language, value = [location,code amount]}
			emit(lang,[this.owner.location,this.langs[lang]])
		})
	}
	var reduce = function(key,values){
		var lang = key;
		var agg = {}
		values.forEach(function(value){
			var location = value[0];
			var amount = value[1];
			if (agg.hasOwnProperty(location))
				agg[location] += amount;
			else
				agg[location] = amount;
		})
		return agg;
	}
	dbsrc.mapReduce(
		map.toString(),
		reduce.toString(),
		{
			out: "distributionLangs"
		}
	)
}





module.exports = MapReduce;