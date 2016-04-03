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

					// Strip only unique locations
					var locations = _.uniq(_.pluck(regions,'_id'));
					locations = _.reject(locations,_.isNull);
					console.log(locations); // TAODEBUG:

					return locations;					
				})
				.then(function(locations){
					
					function createGeoUpdater(location,i){
						return Promise.delay(1200+1000*i)
							.then(() => [location,updateGeolocation(location)])
					}

					var dbgeo = dbGeo();

					// Delayed update the locations
					return Promise.map(locations,createGeoUpdater)
						.then((results) => {

							var geoDb = GeoDB.db(MONGO_SVR,MONGO_DB);
							var geoUpdate = GeoDB.update(geoDb);

							results.forEach((geolocation) => {

								console.log(geolocation); // TAODEBUG:

								if (geolocation[1].status != 'OK'){
									console.error(`Could not find location of: ${geolocation[0]}`.red);
									return null;
								}

								var location = geolocation[0];
								var geoinfo  = geolocation[1];

								geoUpdate(
									location,
									geoinfo['city'],geoinfo['country'],
									geoinfo['pos']
								);

							})
						})
				})

		})
		.then(() => process.exit(0))
}

/**
 * From the given physical location,
 * update the geolocation to the collection "geo"
 * @param {String} location/address
 */
function updateGeolocation(location){
	if (!!~invalid_locations.indexOf(location)){
		console.log(`Skip - ${location}`.yellow);
		return null;
	}

	console.log('Updating geolocation: '.cyan, location)
	return Gmap.locationToInfo(location)
}


// Start
prep()
