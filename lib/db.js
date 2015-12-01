/**
 * @file db.js
 */

var cluster = require('cluster-db')

/**
 * @class DB
 * @param {DAO} dao
 */

function DB (dao) {
  /**
   * master db selector
   * @memberof DB.prototype
   * @type {string}
   * @protocted
   */
  this._master = 'master'
  /**
   * slave db selector
   * @memberof DB.prototype
   * @type {string}
   * @protocted
   */
  this._slave = 'slave*'

  /**
   * dao object
   * @type {DAO}
   * @member _dao
   * @memberof DB.prototype
   * @protected
   */
  this._dao = dao
}

/**
 * set master db selector.
 * @method setMaster
 * @memberof DB.prototype
 * @param {string} id
 */
DB.prototype.setMaster = function (id) {
  this._master = id
}

/**
 * set slave db selector.
 * @method setSlave
 * @memberof DB.prototype
 * @param {string} id
 */
DB.prototype.setSlave = function (id) {
  this._slave = id
}

/**
 * request to master
 * @method master
 * @memberof DB.prototype
 * @param {function} cb
 * @param {string} sql
 * @param {array} value
 */
DB.prototype.master = function (cb, sql, value) {
  return this.query(this._master, cb, sql, value)
}

/**
 * request to slave
 * @method slave
 * @memberof DB.prototype
 * @param {function} cb
 * @param {string} sql
 * @param {array} value
 */
DB.prototype.slave = function (cb, sql, value) {
  return this.query(this._slave, cb, sql, value)
}

/**
 * request query
 * @todo default id is master.
 * @method query
 * @memberof DB.prototype
 * @param {string} id
 * @param {function} cb cb(err, rows, fields, query)
 * @param {string} sql
 * @param {array} value
 * @example
 * db.query('slave*', cb, sql, value)
 * db.query(cb, sql, value)
 */
DB.prototype.query = function (id, cb, sql, value) {
  if (typeof id !== 'string') {
    value = sql
    sql = cb
    cb = id
    id = this._master
  }

  if (typeof value === 'undefined') {
    value = []
  }

  cluster.get(id, function (err, conn) {
    if (err) {
      throw err
    }

    var query = conn.query(sql, value, function (err, rows, fields) {
      conn.release()

      if (err) {
        console.error(sql, value)
        throw err
      }

      cb(null, rows, fields, query)
    })

    console.log(query.sql)
  })
}

/**
 * return selected field item.
 * @todo default id is slave*
 * @method fetchOne
 * @memberof DB.prototype
 * @param {string} id
 * @param {function} cb
 * @param {string} sql
 * @param {array} value
 * @param {string} field
 * @example
 * this.db.fetchOne('slave*', cb, sql, value, 'cnt')
 * this.db.fetchOne(cb, sql, value, 'cnt')
 */
DB.prototype.fetchOne = function (id, cb, sql, value, field) {
  if (typeof id !== 'string') {
    field = value
    value = sql
    sql = cb
    cb = id
    id = this._slave
  }

  return this.query(id, function (err, rows, fields, query) {
    cb(err, rows.length === 0 ? null : rows[0][field], fields, query)
  }, sql, value)
}

/**
 * return items of zero index
 * @todo default id is slave*
 * @method fetch
 * @memberof DB.prototype
 * @param {string} id
 * @param {Function} cb
 * @param {string} sql
 * @param {array} value
 * @example
 * this.db.fetch('slave*', cb, sql, value)
 * this.db.fetch(cb, sql, value)
 */
DB.prototype.fetch = function (id, cb, sql, value) {
  var self = this

  if (typeof id !== 'string') {
    value = sql
    sql = cb
    cb = id
    id = this._slave
  }

  return this.query(id, function (err, rows, fields, query) {
    cb(err, rows.length === 0 ? null : self._dao.migrateItem(rows[0]), fields, query)
  }, sql, value)
}

/**
 * return all items.
 * @todo default id is slave*
 * @method fetchAll
 * @memberof DB.prototype
 * @param {string} id
 * @param {function} cb
 * @param {string} sql
 * @param {array} value
 * @example
 * this.db.fetchAll('slave*', cb, sql, value)
 * this.db.fetchAll(cb, sql, value)
 */
DB.prototype.fetchAll = function (id, cb, sql, value) {
  var self = this

  if (typeof id !== 'string') {
    value = sql
    sql = cb
    cb = id
    id = this._slave
  }

  return this.query(id, function (err, rows, fields, query) {
    cb(err, self._dao.migrate(rows), fields, query)
  }, sql, value)
}

/**
 * request insert query.
 * @todo default id is master.
 * @method insert
 * @memberof DB.prototype
 * @param {string} id
 * @param {function} cb
 * @param {string} table
 * @param {object} item
 * @return {DB~RESULT}
 * @example
 * this.db.insert('master', cb, table, item)
 * this.db.insert(cb, table, item)
 */
DB.prototype.insert = function (id, cb, table, item) {
  if (typeof id !== 'string') {
    item = table
    table = cb
    cb = id
    id = this._master
  }

  return this.query(id, function (err, res) {
    cb(err, err ? { insertId: 0, affectedRows: 0, changedRows: 0 } : res)
  }, 'INSERT INTO ?? SET ?', [table, item])
}

/**
 * request update query.
 * @todo default id is master
 * @param {string} id
 * @param {function} cb
 * @param {string} table
 * @param {object} item
 * @param {array} where
 * @param {array} value
 * @return {DB~RESULT}
 * @example
 * this.db.update('master', cb, table, item, where, value)
 * this.db.update(cb, table, item, where, value)
 */
DB.prototype.update = function (id, cb, table, item, where, value) {
  if (typeof id !== 'string') {
    value = where
    where = item
    item = table
    table = cb
    cb = id
    id = this._master
  }

  var sql = ['UPDATE ?? SET ?']

  if (typeof where === 'string') {
    where = [where]
  }

  if (typeof value === 'undefined') {
    value = []
  } else if (typeof value !== 'object') {
    value = [value]
  }

  if (where && where.length > 0) {
    sql.push('WHERE', where.join(' '))
  }

  value.unshift(table, item)

  return this.query(id, function (err, res) {
    cb(err, err ? { insertId: 0, affectedRows: 0, changedRows: 0 } : res)
  }, sql.join(' '), value)
}

/**
 * request delete query.
 * @todo default id is master.
 * @param {string} id
 * @param {function} cb
 * @param {string} table
 * @param {array} where
 * @param {array} value
 * @return {DB~RESULT}
 * @example
 * this.db.delete('master', cb, table, where, value)
 * this.db.delete(cb, table, where, value)
 */
DB.prototype.delete = function (id, cb, table, where, value) {
  if (typeof id !== 'string') {
    value = where
    where = table
    table = cb
    cb = id
    id = this._master
  }

  var sql = ['DELETE FROM ??']
  if (typeof value === 'undefined') {
    value = []
  } else if (typeof value !== 'object') {
    value = [value]
  }

  if (typeof where === 'string') {
    where = [where]
  }

  if (where && where.length > 0) {
    sql.push('WHERE', where.join(' '))
  }

  value.unshift(table)

  return this.query(id, function (err, res) {
    cb(err, err ? { insertId: 0, affectedRows: 0, changedRows: 0 } : res)
  }, sql.join(' '), value)
}

/**
 * return query string for offset.
 * @method offsetSQL
 * @memberof DB.prototype
 * @param {number} page
 * @param {number} limit
 * @return {string}
 */
DB.prototype.offsetSQL = function (page, limit) {
  return ['LIMIT', limit * (page - 1), ',', limit].join(' ')
}

/**
 * return limit query string.
 * @method limitSQL
 * @memberof DB.prototype
 * @param {number} limit
 * @param {number} offset
 * @return {string}
 */
DB.prototype.limitSQL = function (limit, offset) {
  return ['LIMIT', offset, ',', limit].join(' ')
}

/**
 * return select query string.
 * @method selectSQL
 * @memberof DB.prototype
 * @param {array} fields
 * @param {array} table
 * @param {array} [where]
 * @param {array} [order]
 * @param {number} [limit]
 * @param {number} [offset]
 * @return {string}
 */
DB.prototype.selectSQL = function (fields, table, where, order, limit, offset, group) {
  // not exist order
  if (typeof order !== 'object') {
    offset = limit
    limit = order
    order = []
  }
  var sql = ['SELECT', fields.join(', '), 'FROM', table.join(' ')]

  if (where && where.length > 0) {
    sql.push('WHERE', where.join(' '))
  }

  if (group && group.length > 0) {
    sql.push('GROUP BY', group.join(' '))
  }

  if (order && order.length > 0) {
    sql.push('ORDER BY', order.join(', '))
  }

  if (typeof limit !== 'undefined' && typeof offset !== 'undefined') {
    sql.push(this.limitSQL(limit, offset))
  }

  return sql.join(' ')
}

/**
 * add condition to where array
 * @method where
 * @memberof DB.prototype
 * @param {string} where
 * @param {array} pool
 * @param {string} [operand] default: AND
 */
DB.prototype.where = function (where, pool, operand) {
  operand = operand || 'AND'
  return pool.push((pool.length > 0 ? operand + ' ' : '') + where)
}

/**
 * make array for ordering.
 * @method orderBy
 * @memberof DB.prototype
 * @param {array<DB~SORT>} sort
 * @return {array}
 */
DB.prototype.orderBy = function (sort, filter) {
  sort = sort || []
  filter = filter || []

  var order = []
  for (var i = 0; i < sort.length; i++) {
    if (filter.indexOf(sort[i].field) === -1) {
      continue
    }

    order.push(cluster.escapeId(sort[i].field) + ' ' + ((sort[i].dir || 'DESC').toUpperCase() === 'DESC' ? 'DESC' : 'ASC'))
  }

  return order
}

/**
 * bind query string by value.
 * @param {string} sql
 * @param {array} value
 * @return {string}
 * @example
 * var sql = "SELECT * FROM ?? WHERE ?? = ?"
var inserts = ['users', 'id', userId]
sql = db.format(sql, inserts)
 */
DB.prototype.format = function (sql, value) {
  return cluster.format(sql, value)
}

/**
 * @typedef {object} DB~RESULT
 * @property {number} insertId
 * @property {number} affectedRows
 * @property {number} changedRows
 */

/**
 * @typedef {object} DB~SORT
 * @property {string} field field name
 * @property {string} dir DESC or ASC
 */

module.exports = DB
