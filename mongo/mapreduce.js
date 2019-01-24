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
 *
 */
MapReduce.asGraph = function(dbsrc){

	var map = function(){
		var record = this;
		let totLoc = 0;
		let langs = [];
		if (Object.keys(this.langs).length > 1)
			Object.keys(this.langs).forEach((baselang) => {
				let w = record.langs[baselang]
				langs.push({lang: baselang, density: w})
				totLoc += w
			})

		if (totLoc > 0){
			langs.map((l) => l.density /= totLoc*1.0)	
		}

		// Take only one main language
		// and all links to the neighbour languages
		let primaryLang = {lang: null, density: 0}
		langs.forEach((a) => {
			if (a.density > primaryLang.density){
				primaryLang = {lang: a.lang, density: a.density}
			}
		})

		langs.forEach((a) => {
			if (a.lang != primaryLang.lang){
				emit({
					lang: 			primaryLang.lang,
					density: 		primaryLang.density,
					neighbours: {lang: a.lang, ratios: [primaryLang.density / a.density]}
				})
			}
		})
	}
	var reduce = function(key,values){
		// TAOTODO:
	}

	return Promise.resolve((done,reject) => {
		dbsrc.mapReduce(
			map.toString(),
			reduce.toString(),
			{out: "graph"},
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
 * Generate the correlation of pairs of languages
 */
MapReduce.langCorrelation = function(dbsrc){
	var map = function(){
		var record = this;

		if (Object.keys(this.langs).length > 1)
			Object.keys(this.langs).forEach(function(baselang){
				Object.keys(record.langs).forEach(function(lang){
					if (lang != baselang){
						var dict = {};
						dict[lang] = 1;
						dict[baselang] = 1;
						emit(baselang, dict)
					}
				})
			})
	}
	var reduce = function(key,values){
		var kv = {};
		values.forEach(function(dict){
			Object.keys(dict).forEach(function(k){
				if (!(k in kv)){
					kv[k] = 0;
				}
				kv[k] += dict[k];
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