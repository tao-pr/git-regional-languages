/**
 * Geolocation storage DB
 *
 * @author StarColon Projects
 */

var _       = require('underscore');
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
		return new Promise(function(done,reject){
			var query = {location: location}
			var record = {
				location: location,
				city: city,
				country: country,
				pos: latlng
			}
			var options = {upsert: true}

			console.log('Updating...'.cyan,location);
			
			db.update(query,record,options,function(err,n){
				if (err){
					console.error('ERROR updating Geolocation'.red);
					console.error(err);
					return reject(err)
				}
				done(n)
			})
		})
	}
}

/**
 * @return {Array} of location string
 */
GeoDB.listLocations = function(db){
	return new Promise(function(done,reject){
		db.find({}).toArray(function(err,n){
			if (err) reject(err);
			else done(_.pluck(n,'location'));
		})
	})
}

/**
 * @return {Object} hash-table-like which maps location -> geolocation
 */ 
GeoDB.listLocationMapping = function(db){
	return new Promise(function(done,reject){
		db.find({}).toArray(function(err,n){
			if (err) reject(err);
			else {
				var mapping = {}
				n.forEach((m) => {
					mapping[m.location] = m.pos
				})
				return done(mapping)
			}
		})
	})
}

/**
 * Check whether the location exists in the database
 */
GeoDB.exists = function(db,location){
	return new Promise(function(done,reject){
		db.count({location: location},function(err,n){
			if (err) reject(err);
			else done(n>0)
		})
	})
}


module.exports = GeoDB;