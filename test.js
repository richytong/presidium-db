const Test = require('thunk-test')

require('.')
require('./index')

const test = Test.all([
  require('./test/assertBalanced.test')
  require('./DiskHashTable.test'),
  require('./DiskSortedHashTable.test'),
])

if (process.argv[1] == __filename) {
  test()
}

module.exports = test
