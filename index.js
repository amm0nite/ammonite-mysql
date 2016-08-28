var mysql = require('mysql');

var pool = null;

var configure = function(config) {
    if (!config) {
        throw new Error('Invalid Config');
    }

    var requiredProperties = ['user', 'host', 'password', 'database'];
    for (let property in requiredProperties) {
        if (config.hasOwnProperty(property)) {
            throw new Error('Missing ' + property + ' config property');
        }
    }

    pool = mysql.createPool({
        connectionLimit: 10,
        host: config.host,
        user: config.user,
        password: config.password,
        database: config.base
    });
};

var find = function(table, options, next) {
    if (!pool) return next('Not Configured');

    var whereElements = []
    var values = [];
    for (let name in options) {
        whereElements.push(name + ' = ?');
        values.push(options[name]);
    }
    
    var sql = 'SELECT * FROM ' + mysql.escapeId(table) + ' WHERE ' + whereElements.join(' AND ');
    var req = pool.query(sql, values, next);
};

var findOne = function(table, options, next) {
    if (!pool) return next('Not Configured');

    find(table, options, function(err, res) {
        if (err) {
            return next(err);
        }
        if (res.length > 1) {
            return next('Too many results');
        }
        var found = null;
        if (res.length == 1) {
            found = res[0];
        }
        return next(null, found);
    });
};

var insert = function(table, options, next) {
    if (!pool) return next('Not Configured');

    var sql = 'INSERT INTO ' + mysql.escapeId(table) + ' SET ?';
    console.log(sql);
    var req = pool.query(sql, options, function(err, res) {
        if (err) {
            return next(err);
        }
        return findOne(table, { 'id': res.insertId }, next);
    });
}

module.exports = {
    'configure': configure,
    'find':      find,
    'findOne':   findOne,
    'insert':    insert
};