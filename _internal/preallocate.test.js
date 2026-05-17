const Test = require('thunk-test')
const assert = require('assert')
const fs = require('fs')
const os = require('os')
const preallocate = require('./preallocate')

const test = new Test('preallocate', async function integration() {
  const filepath = `${__dirname}/preallocate_test`
  await fs.promises.rm(filepath).catch(() => {})

  await preallocate(filepath, 100)

  let stats = await fs.promises.stat(filepath)
  assert.strictEqual(stats.size, 100)

  await preallocate(filepath, 200)

  stats = await fs.promises.stat(filepath)
  assert.strictEqual(stats.size, 200)

  await fs.promises.rm(filepath)
}).case()

if (process.argv[1] == __filename) {
  test()
}

module.exports = test
