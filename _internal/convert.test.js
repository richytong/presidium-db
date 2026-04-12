const Test = require('thunk-test')
const convert = require('./convert')

const test = new Test('convert', convert)

test.case(1, 'string', '1')
test.case('1', 'string', '1')
test.case('1', 'number', 1)
test.case(1, 'number', 1)
test.throws('1', 'unknown', new Error('Unrecognized type unknown'))

if (process.argv[1] == __filename) {
  test()
}

module.exports = test
