const DiskSortedHashTable = require('./DiskSortedHashTable')
const randomUniqueNumbersGenerator = require('./_internal/randomUniqueNumbersGenerator')

async function runBenchmark(degree, size) {
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
    insertedNumbers.push(n)
  }

  for (const n of insertedNumbers) {
    const start = performance.now()
    await ht.delete(`key${n}`)
    console.log(`deleted key${n} in ${performance.now() - start}ms; degree ${degree}, current count ${ht.count()}`)
  }
}

runBenchmark(2, 2_000_000_000)
