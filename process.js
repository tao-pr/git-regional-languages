"use stricts";
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
		if (!!~invalid_locations.indexOf(location)){
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
					done(map)
				})
			})
		})
}

function generateHtml(densityMapping){
	// TAOTODO:
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

					// Strip only unique and contentful locations
					var locations = _.uniq(_.pluck(regions,'_id'));
					    locations = _.reject(locations,_.isNull);

					return locations;					
				})
				.then(updateGeoRegions) // Update geolocation mapping to DB
		})
		.then(generateGeoLanguageDensity)
		.then(generateHtml)
		.then(() => process.exit(0))
}



// Start
prep()
