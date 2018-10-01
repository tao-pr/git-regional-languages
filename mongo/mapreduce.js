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

/**
 * Generate the correlation of pairs of languages
 */
MapReduce.langCorrelation = function(dbsrc){
	var map = function(){
		var record = this;

		if (Object.keys(this.langs).length > 1)
			Object.keys(this.langs).forEach(function(baselang){
				Object.keys(record.langs).forEach(function(lang){
					var dict = {};
					dict[lang] = [record.langs[lang]/record.langs[baselang]];
					emit(baselang, dict)
				})
			})
	}
	var reduce = function(key,values){
		var kv = {};
		values.forEach(function(dict){
			Object.keys(dict).forEach(function(k){
				if (!(k in kv)){
					kv[k] = [];
				}
				kv[k] = kv[k].concat(dict[k]);
			})
		})
		return kv;
	}

	return new Promise(function(done,reject){
		dbsrc.mapReduce(
			map.toString(),
			reduce.toString(),
			{out: "langCorrelation"},
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

/**
 * Get the distribution of language by regions
 */
MapReduce.langDistributionByLocation = function(dbsrc){
	var map = function(){
		var record = this;

		// Skip the repo if the total LoC is too small
		var sumloc = 0;
		Object.keys(this.langs).forEach(function(lang){
			var loc = record.langs[lang];
			sumloc += loc;
		})

		if (sumloc >= 200){
			Object.keys(this.langs).forEach(function(lang){
				// {key = language, value = [location,code amount]}
				if (record.owner.repos > 1 && record.owner.followers >= 10)
					emit(lang,[record.owner.location,record.langs[lang]])
			})
		}
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
			map.toString(),
			reduce.toString(),
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

/**
 * Get all regions
 */
MapReduce.allRegions = function(dbsrc){
	var map = function(){
		emit(this.owner.location,1)
	}
	var reduce = function(key,values){
		return Array.sum(values)
	}

	return new Promise(function(done,reject){
		dbsrc.mapReduce(
			map.toString(), 
			reduce.toString(),
			{out: "regions"},
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