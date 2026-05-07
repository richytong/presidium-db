const Test = require('thunk-test')

require('.')
require('./index')

const test = Test.all([
  require('./_internal/getLeftParentItem.test')
  require('./_internal/getRightParentItem.test')
  require('./_internal/getLeftmostNodeItemExcluding.test')
  require('./_internal/getRightmostNodeItemExcluding.test')
  require('./DiskHashTable.test'),
  require('./DiskSortedHashTable.test'),
])

if (process.argv[1] == __filename) {
  test()
}

module.exports = test
