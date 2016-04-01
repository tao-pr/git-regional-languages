/**
 * MongoDB record MapReduce task runner
 *
 * @author StarColon Projects
 */

var colors  = require('colors');
var mongo   = require('mongoskin');
var _       = require('underscore');
var Promise = require('bluebird');


var MapReduce = {}

MapReduce.db = function(serverAddr,dbName,collection){
	return mongo.db(serverAddr + '/' + dbName).collection(collection);
}

MapReduce.langDistributionByLocation = function(dbsrc){
	var map = function(){
		var record = this;
		Object.keys(this.langs).forEach(function(lang){
			// {key = language, value = [location,code amount]}
			emit(lang,[record.owner.location,record.langs[lang]])
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

	return new Promise(function(done,reject){
		dbsrc.mapReduce(
			map,reduce,
			{out: "distLangByRegion"},
			function(err,destCollection){
				if (err){
					console.error('ERROR MapReduce:'.red);
					console.error(err);
					return reject(err)
				}

				// Generate the output
				return done(destCollection.find().toArray())
			}
		)
	})
}





module.exports = MapReduce;