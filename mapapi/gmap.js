/**
 * Google Map API middleman
 *
 * @author StarColon Projects
 */

var gmap     = {}
var fs       = require('fs');
var requests = require('requests');
var colors   = require('colors');
var Promise  = require('bluebird');

const API_ENDPOINT = "https://maps.googleapis.com/maps/api/geocode/json?";
const PATH_API_KEY = "../GOOGLE-API-KEY";

function readGoogleApiKey(){
	return fs.readFileSync(PATH_API_KEY, 'utf8');
}

function apiEndPoint(location){
	return [
		API_ENDPOINT,
		'api=',readGoogleApiKey(),
		'&address=',location
	].join('')
}

function interpretResults(rjson){
	if (rjson['status'] != "OK"){
		console.error(rjson['status'].toString().red);
		return null;
	}
	return {
		status:   rjson['status'],
		country:  rjson['results'].address_components
		          .filter((c) => c.types.join('-')=='country-political')
		          [0].long_name,
		city:     rjson['results'].address_components
		          .filter((c) => c.types.join('-')=='administrative_area_level_1-political')
		          [0].long_name,
		pos:      rjson['results'].geometry.location // {lat:00, lng:00}		          
	}
}

gmap.locationToInfo = function(location){
	return new Promise(function(done,reject){
		request.get(apiEndPoint(location), function(err,resp,body){
			if (err){
				console.error('Error computing @: '.red + location);
				console.error(err.toString().red);
				return reject(err)
			}
			info = interpretResults(body);
			return done(info)
		})
	})
}

module.exports = gmap;