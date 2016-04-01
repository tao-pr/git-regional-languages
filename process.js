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
	var datasource = MapReduce.db('mongodb://localhost','gitlang','repos');
	MapReduce.langDistributionByLocation(datasource)
		.then(function(dist){
			console.log(dist); // TAODEBUG:
		})
		.then(() => process.exit(0))
}



// Start
prep()
