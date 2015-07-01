'use strict';

var xlsx = require('node-xlsx');
var mongodb = require('mongodb');
var MongoClient = mongodb.MongoClient;
var ObjectID = mongodb.ObjectID;
var obj = xlsx.parse(__dirname + '/all_systems-strings.xlsx');
var data = obj[0].data;
var header = data.shift();
var url = 'mongodb://localhost:27017/manway_local';

console.log('header:', header);
console.log(data[0]);
var sourceIndex = -1;
var pathsIndex = -1;

for( var i in header ){
	if( header[i] === 'SOURCE' ){
		sourceIndex = i;
	}

	if( header[i] === 'PATHS' ){
		pathsIndex = i;
	}
}

var importRow = function( row ){
	if( !row[pathsIndex] ){
		console.log('%s, Error:empty path in row: %s', new Date(), row);
		return;
	}

	var paths = row[pathsIndex].split(',');
	console.log('paths length:', paths.length);
	var pathPart;
	var collectionName;
	var _id;
	var key;
	var value;
	paths.forEach(function( path ){
		pathPart = path.split('.');
		collectionName = pathPart.shift();
		_id = pathPart.shift();
		key = pathPart.join('.');
		value = row[sourceIndex];
		updateTranslation(collectionName, _id, key, value);
		// console.log('collection(%s), _id(%s), key(%s), value(%s).',
		//			pathPart.shift(),
		//			pathPart.shift(),
		//			pathPart.join('.'),
		//			row[sourceIndex]);
	});
};
var count = 0;
var mapper = {};

var getIdObject = function( id ){
	if( mapper[id] ){
		return mapper[id];
	}
	mapper[id] = new ObjectID(id);
	return mapper[id];
}
var updateTranslation = function( collectionName, _id, key, value, withoutEn ){
	getCollection(collectionName, function(err, collection){
		var set = {};
//		value = '所有都变成了中文';
		if( withoutEn ){
			set[key] = value;
		} else {
			set[key + '.en'] = value;
		}
		collection.findAndModify({_id:getIdObject(_id)}, [], {$set: set}, function( err, row ){
			if( err ){
				return updateTranslation( collectionName, _id, key, value, true);
//				console.log('find and modify error:', err);
			}
			var keyParts = key.split('.');
			console.log('%s: collection(%s) id(%s) updater(%s).',count++, collectionName, _id, JSON.stringify(set));
		});

	});
}

var importData = function( d ){
	for( var rowIndex in d ){
		importRow(d[rowIndex]);
	}
};

var collections = {};
var db = null;

var getCollection = function( collectionName, cb ){
	if( collections[collectionName] ){
		return cb(null, collections[collectionName]);
	}

	collections[collectionName] = db.collection(collectionName);
	return cb(null, collections[collectionName]);
};

MongoClient.connect(url, function( err, database ){
	if( err ){
		console.log('mongodb connect error:', err);
		process.exit(0);
	}

	db = database;
	console.log('db serilaizer:', db.bson_serializer);
	console.log('Mongodb is connected.')
	importData(data);
});

console.log('sourceIndex:%s, pathsIndex:%s', sourceIndex, pathsIndex);
