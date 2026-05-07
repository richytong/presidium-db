const Test = require('thunk-test')
const getRightmostNodeItemExcluding = require('./getRightmostNodeItemExcluding')

const test = new Test('getRightmostNodeItemExcluding', getRightmostNodeItemExcluding)

test.case([{ index: 1 }, { index: 2 }, { index: 3 }], { index: 3 }, { index: 2 })
test.case([{ index: 1 }, { index: 2 }, { index: 3 }], { index: 2 }, { index: 3 })
test.case([{ index: 1 }, { index: 2 }], { index: 2 }, { index: 1 })
test.case([{ index: 1 }, { index: 2 }], { index: 1 }, { index: 2 })
test.case([{ index: 1 }], { index: 1 }, undefined)
test.case([], { index: 1 }, undefined)

if (process.argv[1] == __filename) {
  test()
}

module.exports = test
