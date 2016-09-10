var mysql = require('mysql');

var pool = null;

var configure = function(config) {
    if (!config) {
        throw new Error('Invalid Config');
    }

    var requiredProperties = ['user', 'host', 'password', 'database'];
    for (let index in requiredProperties) {
        let property = requiredProperties[index];
        if (!config.hasOwnProperty(property)) {
            throw new Error('Missing ' + property + ' config property');
        }
    }

    pool = mysql.createPool({
        connectionLimit: 10,
        host: config.host,
        user: config.user,
        password: config.password,
        database: config.database
    });
};

var find = function(table, where, tail, next) {
    if (!pool) return next('Not Configured');

    var whereElements = []
    var values = [];
    for (let name in where) {
        whereElements.push(mysql.escapeId(name) + ' = ?');
        values.push(where[name]);
    }
    
    var sql = 'SELECT * FROM ' + mysql.escapeId(table) + ' WHERE ' + whereElements.join(' AND ');
    if (tail) {
        sql = sql + ' ' + tail;
    }

    var req = pool.query(sql, values, next);
};

var findOne = function(table, where, next) {
    if (!pool) return next('Not Configured');

    find(table, where, 'LIMIT 1', function(err, res) {
        if (err) {
            return next(err);
        }
        var found = null;
        if (res.length == 1) {
            found = res[0];
        }
        return next(null, found);
    });
};

var findLast = function(table, where, next) {
    if (!pool) return next('Not Configured');

    find(table, where, 'ORDER BY id DESC LIMIT 1', function(err, res) {
        if (err) {
            return next(err);
        }
        var found = null;
        if (res.length == 1) {
            found = res[0];
        }
        return next(null, found);
    });
};

var insert = function(table, values, next) {
    if (!pool) return next('Not Configured');

    var sql = 'INSERT INTO ' + mysql.escapeId(table) + ' SET ?';
    var req = pool.query(sql, values, function(err, res) {
        if (err) {
            return next(err);
        }
        return findOne(table, { 'id': res.insertId }, next);
    });
};

var findAll = function(table, where, next) {
    if (!pool) return next('Not Configured');
    
    var offset = 0;
    var limit = 0;

    if (where.hasOwnProperty('_offset')) {
        offset = parseInt(where._offset);
        delete where._offset;
    }
    if (where.hasOwnProperty('_limit')) {
        limit = parseInt(where._limit);
        delete where._limit;
    }

    var tail = 'ORDER BY id DESC';
    if (limit) {
        tail += ' LIMIT ';
        if (offset) {
            tail += offset + ',';
        }
        tail += limit;
    }

    return find(table, where, tail, next);
};

module.exports = {
    'configure': configure,
    'findAll':   findAll,
    'findOne':   findOne,
    'insert':    insert,
};