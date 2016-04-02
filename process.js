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
var MapReduce = require('./mongo/mapreduce.js');
var Gmap      = require('./mapapi/gmap.js');

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
	var datasource = MapReduce.db('mongodb://localhost','gitlang','repos');
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


			// TAODEBUG:
			console.log(JSON.stringify(_dist,null,2))

			return _dist
		})
		.then(function (dist){
			// Pump all regions to another collection
			return MapReduce.allRegions(datasource)
				.then(function(regions){

					// Strip only unique locations
					var locations = _.uniq(_.pluck(regions,'_id'));
					console.log(locations); // TAODEBUG:

					return locations;					
				})
				.then(function(locations){
					
					function createGeoUpdater(location){
						return Promise.delay(1200)
							.then(() => updateGeolocation(location))
					}

					// Delayed update the locations
					return Promise.each(locations,createGeoUpdater)
						.then((results) => {
							// TAOTODO: Do something with the results



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
