const Test = require('thunk-test')
const getLeftParentItem = require('./getLeftParentItem')

const test = new Test('getLeftParentItem', getLeftParentItem)

const parent1 = { isRightChildPointer: true }
test.case(parent1, parent1)

const parent2 = { isLeftChildPointer: true, leftItem: {} }
test.case(parent2, parent2.leftItem)

const parent3 = { isLeftChildPointer: true }
test.case(parent3, undefined)

const parent4 = {}
test.throws(parent4, new Error('parent node item isLeftChildPointer or isRightChildPointer unset'))

test.case(undefined, undefined)

if (process.argv[1] == __filename) {
  test()
}

module.exports = test
