/**
 * @file dao.js
 */

var moment = require('moment')
var DB = require('./db')

/**
 * @class DAO
 */
function DAO () {
  this._fields = []

  /**
   * @memberof DAO.prototype
   * @member db
   * @type {DB}
   */
  this.db = new DB(this)
}

DAO.prototype.DAO = DAO

/**
 * return current unix timestamp
 * @memberof DAO.prototype
 * @method unix
 * @return {number} timestamp
 */
DAO.prototype.unix = function () {
  return moment().unix()
}

/**
 * return select field string.
 * @example
 * var fields = dao.fields('U.', 'us_', ['us_no', 'name'])
 * this.db.query("SELECT ? FROM users", [fields], function(err, result) {})
 * @name fields
 * @memberof DAO.prototype
 * @param {string} [prefix] prefix of fields
 * @param {string} [rename] table name
 * @param {array} [fields] field list
 * @return {string}
 */
DAO.prototype.fields = function (prefix, rename, fields) {
  prefix = prefix || ''
  rename = rename || ''
  fields = fields || this._fields

  var result = []

  for (var i = 0, cnt = fields.length, field = null, name = null; i < cnt; i++) {
    field = fields[i]
    name = field.replace(/\w+\./g, '').replace(/`/g, '')

    result.push(prefix + (prefix ? name : field) + (rename ? (' AS ' + rename + name) : ''))
  }

  return result.join(', ')
}

/**
 * normalize data
 * @name parseItem
 * @memberof DAO.prototype
 * @param {object} item
 * @param {string} [rename] field 명이 변경된 경우 prefix (ch_)
 * @param {array} [fields] field list
 * @param {boolean} [copy] where copy (default: false)
 * @return {object}
 */
DAO.prototype.parseItem = function (item, rename, fields, copy) {
  rename = rename || ''
  fields = fields || this._fields
  copy = !!copy

  var result = copy ? JSON.parse(JSON.stringify(item)) : {}

  for (var i = 0, cnt = fields.length, field = null, name = null; i < cnt; i++) {
    field = fields[i]
    field = field.replace(/^([a-z]*\.)/i, '').replace(/`/g, '')

    name = rename + field
    if (typeof item[name] !== 'undefined') {
      result[field] = item[name]
    }

    delete item[name]
  }

  return this.normalize(result)
}

/**
 * migrate return data
 * @method migrate
 * @memberof DAO.prototype
 * @return {array}
 */
DAO.prototype.migrate = function (items) {
  for (var i = 0; i < items.length; i++) {
    items[i] = this.migrateItem(items[i])
  }

  return items
}

/**
 * migrate return item
 * @method migrateItem
 * @memberof DAO.prototype
 * @param {object} item
 * @return {object}
 */
DAO.prototype.migrateItem = function (item) {
  return this.parseItem(item, '', this._fields, true)
}

/**
 * normalize return data
 * @method normalize
 * @memberof DAO.prototype
 * @param {object} item
 * @return {object}
 */
DAO.prototype.normalize = function (item) {
  if (typeof item.utime !== 'undefined') {
    item.utime = moment.unix(item.utime).format()
  }
  if (typeof item.ctime !== 'undefined') {
    item.ctime = moment.unix(item.ctime).format()
  }

  return item
}

DAO.prototype.extract = function (item, target, fields) {
  if (!item[target]) {
    return item
  }
  DAO.each(fields, function (name) {
    item[target + '_' + name] = item[target][name]
    delete item[target][name]
  })
  delete item[target]
  return item
}

DAO.prototype.merge = function (item, target, fields) {
  if (!item[target]) {
    item[target] = {}
  }
  DAO.each(fields, function (name) {
    item[target][name] = item[target + '_' + name]
    delete item[target + '_' + name]
  })
  return item
}

DAO.each = function (item, cb) {
  for (var key in item) {
    if (!item.hasOwnProperty(key)) {
      continue
    }
    cb(item[key], key)
  }
}

module.exports = new DAO()
