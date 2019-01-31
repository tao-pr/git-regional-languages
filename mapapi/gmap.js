/**
 * Google Map API middleman
 *
 * @author StarColon Projects
 */

var gmap     = {}
var fs       = require('fs');
var request  = require('request');
var colors   = require('colors');
var Promise  = require('bluebird');

const API_ENDPOINT = "https://maps.googleapis.com/maps/api/geocode/json?";

const dirCurrent   = require('path').dirname(__filename) + '/';
const PATH_API_KEY = dirCurrent + "../GOOGLE-API-KEY";

function readGoogleApiKey(){
	return fs.readFileSync(PATH_API_KEY, 'utf8');
}

function apiEndPoint(location){
	var url = [
		API_ENDPOINT,
		'api=',readGoogleApiKey().trim(),
		'&address=',encodeURI(location)
	].join('')

	console.log(url.yellow);

	return url;
}

function interpretResults(rjson){

	if (rjson['status'] != "OK"){
		console.error(rjson['status'].toString().red);
		return null;
	}

	function takeComponent(component){
		var m = rjson['results'][0].address_components
			.filter((c) => c.types.join('-')==component);
		if (m.length==0) 
			return null;
		else
			return m[0].long_name;
	}

	return {
		status:   rjson['status'],
		country:  takeComponent('country-political'),
		city:     takeComponent('administrative_area_level_1-political'),
		pos:      rjson['results'][0].geometry.location // {lat:00, lng:00}		          
	}
}

/**
 * Get info of an address
 * @param {String} physical location/address
 */
gmap.locationToInfo = function(location){
	return new Promise(function(done,reject){
		request.get(apiEndPoint(location), function(err,resp,body){
			if (err){
				console.error('Error computing @: '.red + location);
				console.error(err.toString().red);
				return done([location,null])
			}
			info = interpretResults(JSON.parse(body));
			return done([location,info])
		})
	})
}

module.exports = gmap;