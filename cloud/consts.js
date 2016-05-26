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

module.exports = {
  // Key used to track the state machine for the migrator
  MIGRATION_KEY: "migrationStatus",

  // The object is already migrated/is in a stable state
  IS_MIGRATED: 1,

  // Unfortuantely we cannot detect the changed fields in an afterSave
  // trigger. Since we need to do a second pass to do migrations of new
  // objects, this helps us avoid user-defined afterSave side effects
  // happening twice. This is also a stable state but will get set to
  // IS_MIGRATED if there are any other writes.
  FINISHED_SECOND_PASS: 2,

  // Set in beforeSave after a new object is created. The exported afterSave
  // sets this so we can do a new pass that only runs the migration script.
  NEEDS_SECOND_PASS: 3,

  // The number of records that are queried at once in the migration job.
  // This is exported so it can be lowered in tests.
  BATCH_SIZE: 1000
};
