var mysql = require('mysql');
var moment = require('moment');
var datetime = "YYYY-MM-DD HH:mm:ss";

var lib = { 'pool': null, 'tables': {} };

lib.configure = function(config) {
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

    lib.pool = mysql.createPool({
        connectionLimit: 10,
        host: config.host,
        user: config.user,
        password: config.password,
        database: config.database
    });
};

lib.readStructure = function(table, next) {
    if (lib.tables.hasOwnProperty(table)) {
        return next(null);
    }

    var sql = 'SHOW COLUMNS FROM ' + mysql.escapeId(table);
    var req = lib.pool.query(sql, function(err, res) {
        if (err) {
            return next(err);
        }

        var tableStructure = {};
        for (let i in res) {
            let row = res[i];
            tableStructure[row.Field] = row;
        }
        lib.tables[table] = tableStructure;

        return next(null);
    });
}

lib.find = function(table, where, tail, next) {
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

    var req = lib.pool.query(sql, values, next);
};

lib.findOne = function(table, where, next) {
    lib.find(table, where, 'LIMIT 1', function(err, res) {
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

lib.findLast = function(table, where, next) {
   lib.find(table, where, 'ORDER BY id DESC LIMIT 1', function(err, res) {
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

lib.insert = function(table, values, next) {
    if (lib.tables[table].hasOwnProperty('createdAt') && !values.hasOwnProperty('createdAt')) {
        values.createdAt = moment().format(lib.datetime);
    }

    var sql = 'INSERT INTO ' + mysql.escapeId(table) + ' SET ?';
    var req = lib.pool.query(sql, values, function(err, res) {
        if (err) {
            return next(err);
        }
        return lib.findOne(table, { 'id': res.insertId }, next);
    });
};

lib.findAll = function(table, where, next) {
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

    return lib.find(table, where, tail, next);
};

module.exports = {
    'configure': lib.configure 
};
var functions = [
    'findAll', 
    'findOne', 
    'insert'
];

for (let i in functions) {
    var func = functions[i];
    
    module.exports[func] = function(table, options, next) {
        if (!lib.pool) {
            return next({ 'message': 'Not Configured' });
        }
        
        lib.readStructure(table, function(err) {
            if (err) {
                return next(err);
            }

            lib[func](table, options, next);
        });
    };
}