"use strict";
/**
 * Github regional language distribution processor
 *
 * @author StarColon Projects
 */

// For reference of MapReduce usage on MongoSkin:
// look at: https://github.com/TBEDP/tjob/tree/master/visualResume/node_modules/mongoskin

var colors    = require('colors');
var _         = require('underscore');
var Promise   = require('bluebird');
var mongo     = require('mongoskin');
var fs = require('fs');
var MapReduce = require('./mongo/mapreduce.js');
var GeoDB     = require('./mongo/geodb.js');
var Gmap      = require('./mapapi/gmap.js');

const MONGO_SVR = 'mongodb://localhost';
const MONGO_DB  = 'gitlang';

const invalid_locations = [
		'undefined','null','0','1','Earth',
		'the internet','the interweb','worldwide'
	]

/**
 * Given language raw record from MongoDB,
 * filter valid ones, and craft a density object
 */
function analyseEachLang(language){
	var _v = Object.keys(language.value)
		.filter((location) => 
			!~invalid_locations.indexOf(location.toLowerCase()) &&
			location.length <= 48
		)
		.map((location) => [location,language.value[location]])
		.filter((n) => !isNaN(n[1]))

	// Sort by density of language written
	_v = _.sortBy(_v,(n) => -n[1])

	return {language: language._id, dist:_v}
}

/**
 * Map from location list to associated Geolocations
 * and store them in the DB
 */
function updateGeoRegions(locations){
	
	var geoDb = GeoDB.db(MONGO_SVR,MONGO_DB);
	var geoUpdate = GeoDB.update(geoDb);

	function validLocation(location){
		if (
			!location ||
			location.length < 3 ||
			location.length > 10 ||
			!!~invalid_locations.indexOf(location) ||
			!!~location.indexOf('@') ||
			!!location.toLowerCase().indexOf('where')){

				console.log(`Skip - ${location}`.yellow);
				return false;
		}
		else return true;
	}

	function saveLocation(geolocation){
		var location = geolocation[0];
		var geoinfo  = geolocation[1];

		if (geoinfo==null || geoinfo.status != 'OK'){
			console.error(`Unable to locate: ${location}`.red);
			geoUpdate(
					location,
					null,
					null,
					null
				)
		}
		else{
			geoUpdate(
					location,
					geoinfo['city'],
					geoinfo['country'],
					geoinfo['pos']
				)
		}
	}

	// Individual location updater
	function createGeoUpdater(location,i){
		return Promise.delay(1200+1400*i)
			.then(()        => Gmap.locationToInfo(location))
			.then((geoinfo) => saveLocation(geoinfo))
	}

	locations = locations.filter(validLocation);

	return GeoDB.listLocations(geoDb)
		.then((existLocations) => // Skip existing locations
			locations.filter((r) => !~existLocations.indexOf(r))
		)
		.then((locations) => Promise.map(locations,createGeoUpdater))
}

/**
 * Collects language distribution over regions
 * from the MongoDB and produces a density map
 */
function generateGeoLanguageDensity(){
	var geoDb  = GeoDB.db(MONGO_SVR,MONGO_DB);
	var distDb = mongo.db(MONGO_SVR + '/' + MONGO_DB).collection('distLangByRegion');

	console.log('Generating language density by geolocations...'.green)

	// {lang} is basically a record of "distLangByRegion"
	function createGeoDensityMap(locationMapping){
		return function(lang){
			var takeGeolocation = function(location){
				if (locationMapping.hasOwnProperty(location)){
					var pos     = locationMapping[location];
					var density = lang.value[location];
					if (pos != null)
						return [pos.lat,pos.lng,density]
					else
						return null;
				}
				else
					return null;
			}
			var geolocations = Object.keys(lang.value).map(takeGeolocation);

			geolocations = _.reject(geolocations,_.isNull);
			return {lang: lang, coords: geolocations};
		}
	}

	return GeoDB.listLocationMapping(geoDb)
		.then(function(locationMapping){
			return new Promise(function(done,reject){
				distDb.find({}).toArray(function(err,langs){
					if (err){
						console.error('ERROR iterating distLangByRegion:'.red);
						console.error(err);
						return reject(err);
					}

					var map = langs.map(createGeoDensityMap(locationMapping))
					return done(map)
				})
			})
		})
}

/**
 * Generate an output geospatial JSON file
 */
function generateJson(outputJson){
	return function (densityMapping){
		console.log('Generating JSON data...'.green);

		return new Promise(function(done,reject){
			var jsonData = JSON.stringify(densityMapping);
			fs.writeFile(outputJson,jsonData,function(err){
				if (err){
					console.error('ERROR writing output JSON :'.red,outputJson);
					console.error(err);
					return reject(err)
				}
				return done(densityMapping)
			})
		})
	}
}

/**
 * Generate an output Javascript file
 * which embeds the language density mapping data
 */
function generateJs(outputJs){
	return function(densityMapping){
		console.log('Generating embeded JS data...'.green);

		return new Promise(function(done,reject){
			var jsonData = JSON.stringify(densityMapping);
			var content = `function getDist(){ return ${jsonData}}`;
			fs.writeFile(outputJs,content,function(err){
				if (err){
					console.error('ERROR writing output JS :'.red,outputJs);
					console.error(err);
					return reject(err)
				}
				return done(densityMapping)
			})
		})
	}
}

/**
 * Port the Google API key to the specified path
 */
function portGoogleAPIKey(outputPath){
	return new Promise(function(done,reject){
		var apiKey = fs.readFileSync('./GOOGLE-API-KEY','utf-8').trim();
		var content = `function getAPIKey(){ return '${apiKey}'}`
		fs.writeFile(outputPath,content,function(err){
			if (err){
				console.error('ERROR writing API key JS : '.red,outputPath);
				console.error(err);
				return reject(err)
			}
			return done()
		})
	})
}

/**
 * Main entry
 */
function prep(){
	console.log('******************'.green);
	console.log('  Preparing data'.green)
	console.log('******************'.green);
	var datasource = MapReduce.db(MONGO_SVR,MONGO_DB,'repos');
	MapReduce.langDistributionByLocation(datasource)
		.then(function(dist){

			// Sort density and remove null location
			//-----------------------------------------------------
			console.log('Analysing repo spatial density...'.green)
			
			// Remove invalid language object
			return dist
				.filter((language) => language._id != 'message')
				.map(analyseEachLang)
		})
		.then(function (dist){
			
			// Pump all regions, fetch their geolocations
			// and save to a collection
			//---------------------------------------
			console.log('Analysing geolocations...'.green)
			return MapReduce.allRegions(datasource)
				.then(function(regions){

					// TAOTODO: Skip the locations which are already resolved

					// Strip only unique and contentful locations
					var locations = _.uniq(_.pluck(regions,'_id'));
					    locations = _.reject(locations,_.isNull);

					return locations;			
				})
				.then(updateGeoRegions) // Update geolocation mapping to DB
		})
		.then(generateGeoLanguageDensity)
		.then(generateJson('spark/src/main/resources/dist.json'))
		.then(generateJs('html/js/dist.js'))
		.then(() => portGoogleAPIKey('html/js/gapi.js'))
		.then(() => process.exit(0))
}



// Start
prep()
