// Copyright 2016 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This library helps build the Cloud Code you need to migrate from
// Parse to another backend. It is modeled as a newer version of Cloud Code;
// all functions are Promise based--they take an old object and return a
// Promise. In the case of a before* trigger, that Promise can resolves to a
// Parse Object to replace what object should be written.
//
// This library runs before* triggers, migrate code, and then after* triggers.
// New objects will need an additional save to be migrated because they are
// missing their objectID in the first pass.

var _ = require('underscore');

function classString(klass) {
  if (typeof klass === "string") {
    return klass;
  } else {
    return (new klass()).className;
  }
}

function def(symbol) {
  return typeof(symbol) != "undefined";
}

// There is no fixed way to know a JS class, but this is a pretty good
// test for Parse.Object since it's an internal method for resolving
// save results.
function isParseObject(obj) {
  return typeof(obj) === "object" && def(obj._applyOpSet);
}

// Tests run under Node, which has a different import path than Cloud Code
var IS_NODE = (typeof process !== 'undefined' &&
               !!process.versions &&
               !!process.versions.node &&
               !process.version.electron)
var consts = IS_NODE ? require('./consts') : require('cloud/consts.js');
var MAXIMUM_DURATION = 14.5 * 60 * 1000,
    ThisIsNotAResponseObject =  {
      success: function() {
        throw "The migration tool expects you to return promises, not use callbacks";
      },
      error: function() {
        throw "The migration tool expecgts you to return promises, not use callbacks";
      }
    };

var Migrator = function(Parse) {
  this._triggers = {};

  // These four functions are like the built-in Parse Cloud Code
  // triggers, but for simplicity's sake they return Promises instead
  // of using a callback object. This allows us to have one uniform
  // response
  this.beforeSave = this._registerFn("beforeSave");
  this.afterSave = this._registerFn("afterSave");
  this.beforeDelete = this._registerFn("beforeDelete");
  this.afterDelete = this._registerFn("afterDelete");

  // Migrate functions are a new trigger type. These methods are called between
  // before/afterSave triggers. The migrateObject function is also used to
  // create jobs/functions.
  this.migrateObject = this._registerFn("migrateObject");
  this.migrateDelete = this._registerFn("migrateDelete");

  this._parse = Parse;
}

Migrator.prototype._registerFn = function(triggerType) {
  var self = this;
  return function(klass, callback) {
    var key = classString(klass);
    var obj = self._triggers[key] || {};
    if (def(obj[triggerType])) {
      throw "Already registered a " + triggerType + " trigger for " + klass;
    }
    obj[triggerType] = callback;
    self._triggers[key] = obj;
  }
}

Migrator.prototype.handlers = function(klass) {
  return this._triggers[classString(klass)] || {};
}

// exportTriggers exports all the necessary Parse Cloud Code.
// The actual functions being registered are generated with getXYZ so we
// can more easily test invocation of these functions.
Migrator.prototype.exportTriggers = function() {
  var self = this;
  _.each(this._triggers, function(handlers, klass) {
    if (def(handlers.beforeSave) || def(handlers.migrateObject)) {
      self._parse.Cloud.beforeSave(klass, self.getBeforeSave(klass));
    }

    if (def(handlers.afterSave) || def(handlers.migrateObject)) {
      self._parse.Cloud.afterSave(klass, self.getAfterSave(klass));
    }

    if (def(handlers.beforeDelete) || def(handlers.migrateDelete)) {
      self._parse.Cloud.beforeDelete(klass, self.getBeforeDelete(klass));
    }

    if (def(handlers.afterDelete)) {
      self._parse.Cloud.afterDelete(klass, self.getAfterDelete(klass));
    }
  });

  self._parse.Cloud.job("import", self.getImportJob());
};

Migrator.prototype.getBeforeSave = function(klass) {
  var handlers = this.handlers(klass),
    beforeSave = handlers.beforeSave,
    migrate = handlers.migrateObject,
    _Promise = this._parse.Promise;

  return function(request, response) {
    var obj;
    // Skip beforeSave if it doesn't exist (duh) but also if this is just
    // a quick second pass to keep people from worrying about not having
    // an objectId in their migration.
    var changed = request.object.dirtyKeys();
    var shouldBeforeSave = def(beforeSave) &&
      !(changed.length === 1 && changed[0] == consts.MIGRATION_KEY);

    return _Promise.as().then(function() {
      if (shouldBeforeSave) {
        return beforeSave(request, ThisIsNotAResponseObject);
      }
    }).then(function(maybeNew) {
      obj = isParseObject(maybeNew) ? maybeNew : request.object;

      // Hybrid apps can explicitly opt-out.
      if (obj.get(consts.MIGRATION_KEY) == consts.IS_MIGRATED || !migrate) {
        return obj;
      }

      if (obj.isNew()) {
        // Will re-dirty the object in afterSave so another beforeSave
        // catches this case with the objectId available.
        return obj;
      }

       // won't save unless the migation succeeds.
      if (obj.get(consts.MIGRATION_KEY) == consts.NEEDS_SECOND_PASS) {
        obj.set(consts.MIGRATION_KEY, consts.FINISHED_SECOND_PASS);
      } else {
        obj.set(consts.MIGRATION_KEY, consts.IS_MIGRATED);
      }
      return _Promise.as().then(function() {
        return migrate && migrate(obj);
      }).then(function() {
        // migration functions shouldn't change the object or it will
        // behave weirdly between beforeSave and the migration job.
        return obj;
      });

    }).then(function() {
      return response.success(obj);
    }, function(err) {
      return response.error(err);
    });
  }
};

Migrator.prototype.getAfterSave = function(klass) {
  var handlers = this.handlers(klass),
    afterSave = handlers.afterSave,
    migrate = handlers.migrateObject,
    _Promise = this._parse.Promise,
    maybeTouch = function(obj) {
      if (obj.existed() || !def(migrate)) {
        return _Promise.as();
      }
      obj.set(consts.MIGRATION_KEY, consts.NEEDS_SECOND_PASS);
      return obj.save();
    }

  return function(request) {
    // We don't have long and there is no failure mode. Let's do this in parallel:
    return def(afterSave) ?
      _Promise.all([afterSave(request), maybeTouch(request.object)]) :
      maybeTouch(request.object);
    // Parse doesn't have a response object; returning an outer promise for testability.
  }
};

Migrator.prototype.getBeforeDelete = function(klass) {
  var handlers = this.handlers(klass),
    beforeDelete = handlers.beforeDelete,
    migrateDelete = handlers.migrateDelete,
    _Promise = this._parse.Promise;

  return function(request, response) {
    return _Promise.as().then(function() {
      return beforeDelete && beforeDelete(request, ThisIsNotAResponseObject);
    }).then(function() {
      return migrateDelete && migrateDelete(request.object);
    }).then(function() {
      return response.success();
    }, function(err) {
      return response.error(err);
    });
  };
};

Migrator.prototype.getAfterDelete = function(klass) {
  var afterDelete = this.handlers(klass).afterDelete,
    _Promise = this._parse.Promise;
  return function(request) {
    return _Promise.as().then(afterDelete);
  }
}

Migrator.prototype.getImportJob = function() {
  var self = this,
    _Promise = this._parse.Promise;
  return function(request, status) {
    var deadline = new Date() + MAXIMUM_DURATION;

    var lastMigration = _Promise.as(0);
    console.log("Starting import pass");
    var totalMigrated = 0;
    _.each(self._triggers, function(handlers, klass) {
      if (!def(handlers.migrateObject)) {
        console.log(klass + " has no migration function; nothing to import");
        return;
      } else {
        console.log("Will import class " +  klass);
      }

      lastMigration = lastMigration.then(function(migrated) {
        totalMigrated += migrated;
        console.log("Starting import of class " + klass);
        return self._migrateClass(klass, handlers.migrateObject);
      });
    });
    return lastMigration.then(function(migrated) {
      totalMigrated += migrated;
    }).then(function() {
      var message = "Done with an import pass";
      if (totalMigrated == 0) {
        message = "Completed initial import!";
      }
      console.log(message);
      status.success(message);
      return totalMigrated;
    }, function(error) {
      return status.error(error)
    });
  }
};

Migrator.prototype._migrateClass = function(klass, migration, deadline) {
  var self = this,
    _Promise = this._parse.Promise;
  if (Date.now() > deadline) {
    console.log("Shutting down to avoid unclean exit from Parse Cloud Jobs");
    return _Promise.as(0);
  }

  // if we use any kind of sort (even objectId) then notEqualTo will eventually
  // be inefficient and we can time out without even migrating a single object.
  // We could alternatively keep a separate migration table and track of the most
  // recent timestamp migrated (using ObjectIDs to break ties). This could work
  // but tends to make cases like new object migrations much harder. It also
  // violates the assumption that your hybrid clients can opt out of migrations
  // by setting "migrationStatus" to 1 on the client.
  var query = new self._parse.Query(klass)
    .notContainedIn(
      consts.MIGRATION_KEY,
      [consts.IS_MIGRATED, consts.NEEDS_SECOND_PASS, consts.FINISHED_SECOND_PASS]
    ).limit(consts.BATCH_SIZE);

  // For each batch, map that batch to a migration of a single record and
  // then setting that record's migation status to done. Then wait for
  // that batch to complete before resolving the outer promise that lets
  // us fetch a new batch.
  return query.find().then(function(objects) {
    var migrations = _.map(objects, function(object) {
      return _Promise.as().then(function() {
        return migration(object);
      }).then(function() {
        object.set(consts.MIGRATION_KEY, consts.IS_MIGRATED);
        return object.save();
      });
    });
    return _Promise.when(migrations).then(function() {
      return migrations.length;
    });

  }).then(function(migrated) {
    // Recursion is the for loop of async.
    if (migrated == consts.BATCH_SIZE) {
      return self._migrateClass(klass, migration, deadline).then(function(accum) {
        return accum + migrated;
      });
    }
    console.log("Done migrating " + klass + " class");
    return migrated;
  });
};

module.exports = function(parse) { return new Migrator(parse); };
