'use strict';
var _ = {
  omitBy: require('lodash.omitby')
}
var async = require('async')
var debug = require('./debugging')
var mongodb = require('mongodb')
var Joi = require('joi')
var semver = require('semver')
const JSONAPI = require('jsonapi-server')
var MIN_SERVER_VERSION = '1.10.0';

var MongoStore = module.exports = function MongoStore(config) {
  MongoStore._checkMinServerVersion()
  this._config = config
};

/**
  Handlers readiness status. This should be set to `true` once all handlers are ready to process requests.
 */
MongoStore.prototype.ready = false


MongoStore._checkMinServerVersion = function() {
  var serverVersion = require('jsonapi-server')._version
  if (!serverVersion) return
  if (semver.lt(serverVersion, MIN_SERVER_VERSION)) {
    throw new Error('This version of jsonapi-store-mongodb requires jsonapi-server>=' + MIN_SERVER_VERSION + '.')
  }
}


MongoStore._mongoUuid = function(uuid) {
  return uuid
  // new mongodb.Binary(uuid, mongodb.Binary.SUBTYPE_UUID);
}


MongoStore._isRelationshipAttribute = function(attribute) {
  return attribute._settings && (attribute._settings.__one || attribute._settings.__many)
}


MongoStore._toMongoDocument = function(resource) {
  var document = _.omitBy(resource, function(value) { return value === undefined })
  document._id = MongoStore._mongoUuid(document.id)
  return document
}

MongoStore._getRelationshipAttributeNames = function(attributes) {
  var attributeNames = Object.getOwnPropertyNames(attributes)
  var relationshipAttributeNames = attributeNames.reduce(function(partialAttributeNames, name) {
    var attribute = attributes[name]
    if (MongoStore._isRelationshipAttribute(attribute)) {
      return partialAttributeNames.concat(name)
    }
    return partialAttributeNames
  }, [])
  return relationshipAttributeNames
};


MongoStore._filterElementToMongoExpr = function(filterElement) {
  var value = filterElement.value
  if (!filterElement.operator) return value
  var mongoExpr = {
    '>': { $gt: value },
    '<': { $lt: value },
    '~': new RegExp('^' + value + '$', 'i'),
    ':': new RegExp(value)
  }[filterElement.operator]
  return mongoExpr
};


MongoStore.prototype._getSearchCriteria = function(request) {
  var self = this
  var filter = request.processedFilter
  if (!filter) return { }
  var criteria = Object.keys(filter).map(function(attribute) {
    var values = filter[attribute].map(MongoStore._filterElementToMongoExpr)
    var attributeConfig = self.resourceConfig.attributes[attribute]
    // Relationships need to be queried via .id
    if (attributeConfig && attributeConfig._settings) {
      attribute += '.id';
    }
    values = values.reduce(function(mongoExpressions, mongoExpr) {
      if (mongoExpr !== null) {
        var mongoExprForAttr = { }
        mongoExprForAttr[attribute] = mongoExpr
        mongoExpressions.push(mongoExprForAttr)
      }
      return mongoExpressions
    }, [])
    if (values.length === 0) {
      return null
    }
    if (values.length === 1) {
      return values[0]
    }
    return { $or: values }
  }).filter(function(value) {
    return value !== null
  })
  if (criteria.length === 0) {
    return { }
  }
  if (criteria.length === 1) {
    return criteria[0]
  }
  return { $and: criteria }
}


MongoStore._notFoundError = function(type, id) {
  return {
    status: '404',
    code: 'ENOTFOUND',
    title: 'Requested resource does not exist',
    detail: 'There is no ' + type + ' with id ' + id
  }
};

MongoStore._unknownError = function(err) {
  return {
    status: '500',
    code: 'EUNKNOWN',
    title: 'An unknown error has occured',
    detail: err
  }
};

MongoStore.prototype._createIndexForRelationships = function() {
  const relationshipAttributeNames = this.resourceConfig.relationshipAttributeNames

  if (!Array.isArray(relationshipAttributeNames) || !relationshipAttributeNames.length) {
    return
  }

  const resourceName                = this.resourceConfig.resource
  const collection                  = this._db.collection(resourceName)

  relationshipAttributeNames.forEach(function(name) {
    collection.createIndex({
      [`${name}.id`]:1
    })
  })
};

MongoStore._getLookUps = function(resourceConfig) {
  const lookUps    = []
  const attributes = resourceConfig.attributes
  for (const attributeName in attributes) {
    const attribute = attributes[attributeName]
    if (attribute._settings && attribute._settings.__as && resourceConfig.foreignIncludes.includes(attributeName)) {
      // let isValid = attribute._settings.__one !== undefined
      // if (attribute._settings.__many) {
      //   const foreignResourceConfig = JSONAPI._resources[attribute._settings.__many[0]]
      //   const targetAttribute = foreignResourceConfig.attributes && foreignResourceConfig.attributes[attribute._settings.__as]
      //   isValid = (targetAttribute !== undefined) && (targetAttribute._settings.__one !== undefined)
      // }
      if (attribute._settings.__one) {
        lookUps.push({
          $lookup:{
            from:(attribute._settings.__one || attribute._settings.__many)[0],
            as:attributeName,
            let:{
              id:'$_id'
            },
            pipeline: [{
              $project:{
                _id:0,
                id: 1,
                type: 1,
                [attribute._settings.__as]:1
              }
            },
            {
              $match:{
                $expr:{
                  $eq:[`$${attribute._settings.__as}.id`,'$$id']
                }
              }
            },{
              $project:{
                [attribute._settings.__as]:0
              }
            }]
          }
        })
      }
    }
  }
  return lookUps
}

MongoStore.prototype._createViewForRelationships = async function() {

  const lookUps      = MongoStore._getLookUps(this.resourceConfig)
  const resourceName = this.resourceConfig.resource
  if (lookUps.length) {
    // create view
    this._viewName = `view-${resourceName}`
    try {
      const view = this._db.collection(this._viewName)
      if (view) {
        await view.drop()
      }
    } catch (e) {
    }
    this._db.createCollection(this._viewName, {
      viewOn: resourceName,
      pipeline:lookUps
    })
  } else {
    this._viewName = resourceName
  }
  console.log(`for ${resourceName}, viewName : ${this._viewName}`)
}

MongoStore.prototype._applySort = function(request, cursor) {
  if (!request.params.sort) return cursor

  var attribute = request.params.sort
  var order = 1
  attribute = String(attribute)
  if (attribute[0] === '-') {
    order = -1
    attribute = attribute.substring(1, attribute.length)
  }
  var sortParam = { }
  sortParam[attribute] = order

  return cursor.sort(sortParam)
}


MongoStore.prototype._applyPagination = function(request, cursor) {
  if (!request.params.page) return cursor
  var offset = request.params.page.offset
  var limit = request.params.page.limit
  if (offset) {
    cursor = cursor.skip(offset)
  }
  if (limit) {
    cursor = cursor.limit(limit)
  }
  return cursor
};

/**
  Initialise gets invoked once for each resource that uses this handler.
 */
MongoStore.prototype.initialise = async function(resourceConfig) {
  if (!this._config.url) {
    return console.error('MongoDB url missing from configuration')
  }
  this.resourceConfig                           = resourceConfig

  if (!Array.isArray(resourceConfig.foreignIncludes)) {
    resourceConfig.foreignIncludes = []
  }

  resourceConfig.relationshipAttributeNames     = MongoStore._getRelationshipAttributeNames(resourceConfig.attributes)

  try {
    this._client = await mongodb.MongoClient.connect(this._config.url, {
      reconnectTries: 999999999,
      reconnectInterval: 5000,
      useNewUrlParser:true
    })
    const dbName = this._config.url.substring(this._config.url.lastIndexOf('/') + 1)
    this._db     = this._client.db(dbName)
    this._client.on('close', (err) => {
      console.error('mongodb connection closed:', err.message)
      this.ready = false
      this._db.collection('Nope').findOne(
        { _id: 0 },
        { id: 1 },
        () => {
          this.ready = true
        }
      )
    })
  } catch(err) {
    console.error('mongodb connection failed:', err.message)
    setTimeout(
      () => this.initialise(resourceConfig),
      5000
    )
  }


  this._createIndexForRelationships()
  await this._createViewForRelationships()
  this.ready = true
}


/**
  Drops the database if it already exists and populates it with example documents.
 */
MongoStore.prototype.populate = function(callback) {
  var self = this
  if (!self._db) return

  self._db.dropDatabase(function(err) {
    if (err) return console.error('error dropping database', err.message)
    async.each(self.resourceConfig.examples, function(document, cb) {
      var validationResult = Joi.validate(document, self.resourceConfig.attributes)
      if (validationResult.error) {
        return cb(validationResult.error)
      }
      self.create({ params: {} }, validationResult.value, cb)
    }, function(error) {
      if (error) console.error('error creating example document:', error)
      return callback()
    })
  })
};


MongoStore.prototype.fetchForeignRelation = function(document, attributes, callback) {
  async.eachLimit(
    attributes,
    10,
    (attributeName, callbackEach) => {
      const attribute = this.resourceConfig.attributes[attributeName]
      const settings  = attribute && attribute._settings
      if (settings && settings.__many && settings.__as) {
        const targetCollection = attribute._settings.__many[0]
        const collection       = this._db.collection(targetCollection)
        const cursor           = collection.find(
          { [`${settings.__as}.id`]:document.id },
          { projection: { _id:0, id: 1, type: 1 } }
        )
        cursor.toArray( (err, array) => {
          if (err) {
            return callbackEach(err)
          }
          document[attributeName] = array
          callbackEach()
        })
      } else {
        callbackEach()
      }
    },
    callback
  )
}

/**
  Search for a list of resources, give a resource type.
 */
MongoStore.prototype.search = function(request, callback) {  
  const collection = this._db.collection(this._viewName)
  // const collection = self._db.collection(request.params.type)
  const criteria   = this._getSearchCriteria(request)
  // debug('search', JSON.stringify(criteria))

  const cursor = collection.find(criteria, { _id: 0 });
  this._applySort(request, cursor)
  this._applyPagination(request, cursor)

  cursor.toArray(
    (toArrayError, documents) => {
      if (toArrayError) {
        return callback(MongoStore._unknownError(toArrayError))
      }
      let index = 0
      if (this.resourceConfig.foreignIncludes.length) {
        async.eachLimit(
          documents,
          10,
          (document,callbackEach) => {
            this.fetchForeignRelation(
              document,
              this.resourceConfig.foreignIncludes,
              (error) => {
                ++index
                if (index % 1000 == 0) {
                  console.log(`processed document ${++index} of ${documents.length}`)
                }
                callbackEach(error)
              }
            )
          },
          (error) => {
            if (error) {
              return callback(MongoStore._unknownError(error))
            } else {
              callback(null, documents, documents.length)
            }
          }
        )
      } else {
        callback(null, documents, documents.length)
      }
    }
  )
};





/**
  Find a specific resource, given a resource type and and id.
 */
MongoStore.prototype.find = function(request, callback) {
  const fastSearch = request.headers.fastsearch === "true" || request.headers.nofetchforeignkey === "true"
  const collection = this._db.collection(this._viewName)
  const documentId = MongoStore._mongoUuid(request.params.id)

  collection.findOne(
    { _id: documentId },
    { _id: 0 },
    (err, document) => {
      if (err || !document) {
        return callback(MongoStore._notFoundError(request.params.type, request.params.id))
      }
      if (!fastSearch) {
        this.fetchForeignRelation(
          document,
          this.resourceConfig.relationshipAttributeNames,
          (error) => {
            if (error) {
              return callback(MongoStore._unknownError(error))
            } else {
              callback(null, document)
            }
          }
        )
      } else {
        callback(null, document)
      }
    }
  )
}


/**
  Create (store) a new resource give a resource type and an object.
 */
MongoStore.prototype.create = function(request, newResource, callback) {
  const collection  = this._db.collection(newResource.type)
  const document    = MongoStore._toMongoDocument(newResource)
  // debug('insert', JSON.stringify(document))
  collection.insertOne(
    document,
    function(err) {
      if (err) {
        return callback(MongoStore._unknownError(err))
      }
      collection.findOne(
        document,
        { _id: 0 },
        function(findErr, result) {
          if (findErr) {
            return callback(findErr)
          }
          if (!result) {
            return callback('Could not find document after insert')
          }
          return callback(null, result)
        }
      )
    }
  )
};


/**
  Delete a resource, given a resource type and an id.
 */
MongoStore.prototype.delete = function(request, callback) {
  var collection = this._db.collection(request.params.type)
  var documentId = MongoStore._mongoUuid(request.params.id)
  collection.deleteOne(
    { _id: documentId },
    function(err, result) {
      if (err) {
        return callback(MongoStore._unknownError(err))
      }
      if (result.deletedCount === 0) {
        return callback(MongoStore._notFoundError(request.params.type, request.params.id))
      }
      return callback(null, result)
    }
  )
};


/**
  Update a resource, given a resource type and id, along with a partialResource.
  partialResource contains a subset of changes that need to be merged over the original.
 */
MongoStore.prototype.update = function(request, partialResource, callback) {
  const collection      = this._db.collection(request.params.type)
  const documentId      = MongoStore._mongoUuid(request.params.id)
  const partialDocument = _.omitBy(partialResource, function(value) { return value === undefined })
  // debug('findOneAndUpdate', JSON.stringify(partialDocument))
  collection.findOneAndUpdate(
    { _id: documentId },
    { $set: partialDocument },
    {
      returnOriginal: false,
      upsert:true,
      projection: { _id: 0 }
    },
    function(err, result) {
      if (err) {
        debug('err', JSON.stringify(err))
        return callback(MongoStore._unknownError(err))
      }
      if (!result || !result.value) {
        return callback(MongoStore._notFoundError(request.params.type, request.params.id))
      }
      return callback(null, result.value)
    }
  )
};
