const Test = require('thunk-test')
const fs = require('fs')
const assert = require('assert')
const DiskHashTable = require('./DiskHashTable')
const DiskSortedHashTable = require('./DiskSortedHashTable')
const randomUniqueNumbersGenerator = require('./_internal/randomUniqueNumbersGenerator')

const loadTest1 = new Test('DiskHashTable', async function integration3() {
  const size = 2_000_000

  await fs.promises.rm(`${__dirname}/DiskHashTable_test_data`, { recursive: true }).catch(() => {})
  await fs.promises.rm(`${__dirname}/DiskSortedHashTable_test_data`, { recursive: true }).catch(() => {})

  const ht = new DiskHashTable({
    storagePath: `${__dirname}/DiskHashTable_test_data/${size}`,
    headerPath: `${__dirname}/DiskHashTable_test_data/${size}_header`,
    initialLength: size * 2,
    itemSize: 100,
    sortValueType: 'number',
    resizeRatio: 0,
  })
  await ht.destroy()
  await ht.init()

  const insertedNumbers = []
  for (const n of randomUniqueNumbersGenerator(size, size * 2)) {
    const start = performance.now()
    await ht.set(`key${n}`, `value${n}`, n)
    console.log('set', `key${n}`, `value${n}`, n, 'in', `${performance.now() - start}ms, current count ${ht.count()}`)
    assert.strictEqual(await ht.get(`key${n}`), `value${n}`)
    insertedNumbers.push(n)
  }

  for (const n of insertedNumbers) {
    const start = performance.now()
    await ht.delete(`key${n}`)
    console.log(`deleted key${n} in ${performance.now() - start}ms; current count ${ht.count()}`)
    assert.strictEqual(await ht.get(`key${n}`), undefined)
  }

  await fs.promises.rm(`${__dirname}/DiskHashTable_test_data`, { recursive: true })

}).case()

const loadTest2 = new Test('DiskSortedHashTable', async function integration37() {
  const size = 2_000_000
  const degree = 2

  await fs.promises.rm(`${__dirname}/DiskHashTable_test_data`, { recursive: true }).catch(() => {})
  await fs.promises.rm(`${__dirname}/DiskSortedHashTable_test_data`, { recursive: true }).catch(() => {})

  const ht = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/${size}`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/${size}_header`,
    initialLength: size * 2,
    itemSize: 100,
    sortValueType: 'number',
    resizeRatio: 0,
    degree,
  })
  await ht.destroy()
  await ht.init()

  const insertedNumbers = []
  for (const n of randomUniqueNumbersGenerator(size, size * 2)) {
    const start = performance.now()
    await ht.set(`key${n}`, `value${n}`, n)
    console.log('set', `key${n}`, `value${n}`, n, 'in', `${performance.now() - start}ms, degree ${degree}, current count ${ht.count()}`)
    assert.strictEqual(await ht.get(`key${n}`), `value${n}`)
    insertedNumbers.push(n)
  }

  for (const n of insertedNumbers) {
    const start = performance.now()
    await ht.delete(`key${n}`)
    console.log(`deleted key${n} in ${performance.now() - start}ms; degree ${degree}, current count ${ht.count()}`)
    assert.strictEqual(await ht.get(`key${n}`), undefined)
  }

  await fs.promises.rm(`${__dirname}/DiskSortedHashTable_test_data`, { recursive: true })

}).case()

const loadTests = Test.all([
  loadTest1,
  loadTest2,
])

if (process.argv[1] == __filename) {
  loadTests()
}

module.exports = loadTests
