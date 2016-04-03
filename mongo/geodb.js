/**
 * Geolocation storage DB
 *
 * @author StarColon Projects
 */

var mongo   = require('mongoskin');
var Promise = require('bluebird');

var GeoDB = {}

GeoDB.db = function(svr,dbname){
	return mongo.db(svr + '/' + dbname).collection('geo');
}


/**
 * Update the information of a location
 * @param {String} location name (used as index)
 * @param {String} city name 
 * @param {String} country name
 * @param {Object} geo coordinate
 */
GeoDB.update = function(db){
	return function(location,city,country,latlng){
		return new Promise(done,reject){
			var query = {location: location}
			var record = {
				location: location,
				city: city,
				country: country,
				pos: latlng
			}
			var options = {upsert: true}
			db.update(query,record,options,function(err,n){
				if (err){
					console.error('ERROR updating Geolocation'.red);
					console.error(err);
					return reject(err)
				}
				done(n)
			})
		}
	}
}


module.exports = GeoDB;