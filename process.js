"use stricts";
/**
 * Github regional language distribution processor
 *
 * @author StarColon Projects
 */

var colors    = require('colors');
var MapReduce = require('./mongo/mapreduce.js');

function prep(){
	console.log('******************'.green);
	console.log('  Preparing data'.green)
	console.log('******************'.green);
	var datasource = MapReduce.db('localhost','gitlangs','repos');
	MapReduce.langDistributionByLocation(datasource)
}
