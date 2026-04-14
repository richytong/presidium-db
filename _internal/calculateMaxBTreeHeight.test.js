const Test = require('thunk-test')
const calculateMaxBTreeHeight = require('./calculateMaxBTreeHeight')

const test = new Test('calculateMaxBTreeHeight', calculateMaxBTreeHeight)

test.case(0, 2, 0)
test.case(1, 2, 0)
test.case(127, 2, 6)
test.case(1023, 2, 9)
test.case(53, 3, 3)
test.case(485, 3, 5)
test.case(127, 3, 3)
test.case(1023, 3, 5)
test.case(511, 4, 4)
test.case(2047, 4, 5)
test.case(127, 4, 3)
test.case(1023, 4, 4)
test.case(6249, 5, 5)
test.case(127, 5, 2)
test.case(1023, 5, 3)

if (process.argv[1] == __filename) {
  test()
}

module.exports = test
