const Test = require('thunk-test')

const test = Test.all([
  require('./DiskHashTable.test'),
  require('./DiskSortedHashTable.test'),
])

if (process.argv[1] == __filename) {
  test()
}

module.exports = test
