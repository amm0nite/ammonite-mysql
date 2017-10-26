const mysql = require('mysql');
const moment = require('moment');
const uuidV4 = require('uuid/v4');

var lib = { 'pool': null, 'tables': {} };

lib.datetimeFormat = "YYYY-MM-DD HH:mm:ss";

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

lib.query = function(sql, next) {
    if (!lib.pool) {
        return next({ 'message': 'Not Configured' });
    }
    return lib.pool.query(sql, next);
};

lib.readStructure = function(table, next) {
    if (lib.tables.hasOwnProperty(table)) {
        return next(null);
    }

    var sql = 'SHOW COLUMNS FROM ' + mysql.escapeId(table);
    var req = lib.query(sql, function(err, res) {
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

lib.whereClause = function(data) {
    var whereElements = []
    for (let name in data) {
        let value = data[name];
        whereElements.push(mysql.escapeId(name) + ' = ' + mysql.escape(value));
    }
    return whereElements.join(' AND ');
};

lib.castValues = function(data) {
    for (let name in data) {
        let value = data[name];
        if (moment.isDate(value) || moment.isMoment(value)) {
            data[name] = moment(value).format(lib.datetimeFormat);
        }
    }
};

lib.find = function(table, where, tail, next) {
    lib.castValues(where);

    var sql = 'SELECT * FROM ' + mysql.escapeId(table) + ' WHERE ' + lib.whereClause(where);
    if (tail) {
        sql = sql + ' ' + tail;
    }

    return lib.query(sql, next);
};

lib.findOne = function(table, where, next) {
    lib.castValues(where);

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
    lib.castValues(where);

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
    lib.castValues(values);

    if (lib.tables[table].hasOwnProperty('createdAt') && !values.hasOwnProperty('createdAt')) {
        values.createdAt = moment().format(lib.datetimeFormat);
    }
    if (lib.tables[table].hasOwnProperty('uid') && !values.hasOwnProperty('uid')) {
        values.uid = uuidV4();
    }

    var sql = 'INSERT INTO ' + mysql.escapeId(table) + ' SET ?';
    lib.query(sql, values, function(err, res) {
        if (err) {
            return next(err);
        }
        return lib.findOne(table, { 'id': res.insertId }, next);
    });
};

lib.findAll = function(table, where, next) {
    lib.castValues(where);

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

lib.update = function(table, values, next) {
    lib.castValues(values);

    let reference = null;
    if (values.hasOwnProperty('id')) {
        reference = 'id';
    }
    else if (values.hasOwnProperty('uid')) {
        reference = 'uid';
    }
    if (!reference) {
        return next({ 'message': 'Missing id or uid' });
    }

    if (lib.tables[table].hasOwnProperty('updatedAt') && !values.hasOwnProperty('updatedAt')) {
        values.updatedAt = moment().format(lib.datetimeFormat);
    }

    var updateElements = [];
    var updateValues = [];
    for (let col in values) {
        if (col != 'id' && col != 'uid') {
            updateElements.push(mysql.escapeId(col) + ' = ?');
            updateValues.push(values[col]);
        }
    }
    updateValues.push(values[reference]);

    var sql = 'UPDATE ' + mysql.escapeId(table) + ' SET ' + updateElements.join(', ') + ' WHERE ' + reference + ' = ?';
    return lib.query(sql, updateValues, next);
};

lib.delete = function(table, where, next) {
    lib.castValues(where);

    var sql = 'DELETE FROM ' + mysql.escapeId(table) + ' WHERE ' + lib.whereClause(where);
    return lib.query(sql, next);
};

module.exports = {
    'datetimeFormat': lib.datetimeFormat,
    'configure': lib.configure
};
var functions = [
    'findAll',
    'findOne',
    'findLast',
    'insert',
    'update',
    'delete',
];

for (let i in functions) {
    let functionName = functions[i];
    
    module.exports[functionName] = function(table, options, next) {
        lib.readStructure(table, function(err) {
            if (err) { return next(err); }
            return lib[functionName](table, options, next);
        });
    };
}

module.exports.query = lib.query;
