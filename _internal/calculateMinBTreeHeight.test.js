const Test = require('thunk-test')
const calculateMinBTreeHeight = require('./calculateMinBTreeHeight')

const test = new Test('calculateMinBTreeHeight', calculateMinBTreeHeight)

test.case(0, 2, 0)
test.case(1, 2, 0)
test.case(127, 2, 3)
test.case(1023, 2, 4)
test.case(53, 3, 2)
test.case(485, 3, 3)
test.case(127, 3, 2)
test.case(1023, 3, 3)
test.case(511, 4, 2)
test.case(2047, 4, 3)
test.case(127, 4, 2)
test.case(1023, 4, 3)
test.case(6249, 5, 3)
test.case(127, 5, 2)
test.case(1023, 5, 3)

if (process.argv[1] == __filename) {
  test()
}

module.exports = test
