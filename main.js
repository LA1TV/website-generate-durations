var probe = require('node-ffprobe');
var mysql = require("mysql");
var config = require("./config.json");

var mysqlCon = null;

connectMysql().then(function(a) {
	mysqlCon = a;

	getFileIds().then(function(ids) {
		ids.forEach(function(id) {
			probe(config.filesLocation+"/"+id, function(err, probeData) {
				var duration = probeData.format.duration;
				insertDuration(id, duration);
			});
		});
		console.log("Finished!");
	});
});

function getFileIds() {
	return new Promise(function(resolve) {
		mysqlCon.query('SELECT id FROM files WHERE file_type_id=3 AND in_use=1 AND process_state=1 ORDER BY id', function(err, results) {
			if (err) throw(err);
			resolve(results.map(function(a) {
				return a.id;
			}));
		});
	});
}

function insertDuration(id, duration) {
	return new Promise(function(resolve) {
		console.log(id, duration);
		var now = new Date();
		mysqlCon.query('INSERT INTO vod_data (file_id,duration,created_at,updated_at) VALUES (?,?,?,?)', [id, duration, now, now], function(err, result) {
			if (err) throw(err);
			resolve(result);
		});
	});
}

function connectMysql() {
	return new Promise(function(resolve) {
		var connection = mysql.createConnection({
			host: config.mysql.host,
			port: config.mysql.port,
			user: config.mysql.user,
			password: config.mysql.password,
			database: config.mysql.database
		});
		connection.connect(function(err) {
			if (err) throw(err);
			resolve(connection);
		});
	});
}