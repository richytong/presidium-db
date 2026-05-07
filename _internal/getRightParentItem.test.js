const Test = require('thunk-test')
const getRightParentItem = require('./getRightParentItem')

const test = new Test('getRightParentItem', getRightParentItem)

const parent1 = { isLeftChildPointer: true }
test.case(parent1, parent1)

const parent2 = { isRightChildPointer: true, rightItem: {} }
test.case(parent2, parent2.rightItem)

const parent3 = { isRightChildPointer: true }
test.case(parent3, undefined)

const parent4 = {}
test.throws(parent4, new Error('parent node item isLeftChildPointer or isRightChildPointer unset'))

test.case(undefined, undefined)

if (process.argv[1] == __filename) {
  test()
}

module.exports = test
