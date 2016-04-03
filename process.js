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

		// TAODEBUG:
		console.log('saveLocation : '.green, location)

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

	// TAODEBUG: Take only first 3
	locations = locations.filter(validLocation).slice(0,3)

	return Promise.map(locations,createGeoUpdater)
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
			
			// Remove invalid language object
			var _dist = dist.filter((language) => language._id != 'message')

			_dist = _dist.map(function(language){
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
			})

			return _dist
		})
		.then(function (dist){
			
			// Pump all regions, fetch their geolocations
			// and save to a collection
			//---------------------------------------

			return MapReduce.allRegions(datasource)
				.then(function(regions){

					// Strip only unique and contentful locations
					var locations = _.uniq(_.pluck(regions,'_id'));
					    locations = _.reject(locations,_.isNull);

					return locations;					
				})
				.then(updateGeoRegions) // Update geolocation mapping to DB

		})
		.then(() => process.exit(0))
}



// Start
prep()
