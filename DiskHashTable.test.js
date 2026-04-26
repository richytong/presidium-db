const Test = require('thunk-test')
const assert = require('assert')
const DiskHashTable = require('./DiskHashTable')

const test1 = new Test('DiskHashTable', async function integration1() {
  const ht1024 = new DiskHashTable({
    storagePath: `${__dirname}/DiskHashTable_test_data/1024`,
    headerPath: `${__dirname}/DiskHashTable_test_data/1024_header`,
    initialLength: 1024,
  })
  await ht1024.destroy()
  await ht1024.init()

  assert.strictEqual(ht1024.count(), 0)
  assert.equal(ht1024._deletedCount, 0)

  assert.strictEqual(await ht1024.get('notfound'), undefined)

  await ht1024.set('maroon', '#800000')
  await ht1024.set('yellow', '#FFFF00')

  assert.equal(await ht1024.get('maroon'), '#800000')
  assert.equal(await ht1024.get('yellow'), '#FFFF00')

  assert.equal(ht1024.count(), 2)
  assert.equal(ht1024._deletedCount, 0)

  await ht1024.set('maroon', '#800___')

  assert.equal(ht1024.count(), 2)
  assert.equal(ht1024._deletedCount, 0)

  assert.equal(await ht1024.get('maroon'), '#800___')
  await ht1024.delete('maroon').then(didDelete => assert(didDelete))

  assert.equal(ht1024.count(), 1)
  assert.equal(ht1024._deletedCount, 1)

  assert.strictEqual(await ht1024.get('maroon'), undefined)

  await ht1024.set('maroon', '#800000')
  assert.equal(await ht1024.get('maroon'), '#800000')

  assert.equal(ht1024.count(), 2)
  assert.equal(ht1024._deletedCount, 0)

  assert.strictEqual(await ht1024.get('notfound'), undefined)
  await ht1024.delete('notfound').then(didDelete => assert(!didDelete))

  assert.equal(ht1024.count(), 2)
  assert.equal(ht1024._deletedCount, 0)

  const ht1 = new DiskHashTable({
    storagePath: `${__dirname}/DiskHashTable_test_data/1`,
    headerPath: `${__dirname}/DiskHashTable_test_data/1_header`,
    initialLength: 1,
  })
  await ht1.destroy()
  await ht1.init()

  await ht1.set('maroon', '#800000')
  assert.strictEqual(await ht1.get('x'), undefined)

  await assert.rejects(
    ht1.set('yellow', '#FFFF00'),
    new Error('Disk hash table is full')
  )

  assert.strictEqual(await ht1024.get('notfound'), undefined)

  ht1024.close()
  ht1.close()
}).case()

const test1_1 = new Test('DiskHashTable', async function integration1_1() {
  const ht2 = new DiskHashTable({
    storagePath: `${__dirname}/DiskHashTable_test_data/2`,
    headerPath: `${__dirname}/DiskHashTable_test_data/2_header`,
    initialLength: 2,
  })
  await ht2.destroy()
  await ht2.init()

  await ht2.set('maroon', '#800000')
  assert.equal(await ht2.get('maroon'), '#800000')
  const collisionKey = 'maroon1'
  await ht2.set(collisionKey, '#800000(1)')
  assert.equal(await ht2.get('maroon'), '#800000')
  assert.equal(await ht2.get(collisionKey), '#800000(1)')
  await ht2.delete('maroon').then(didDelete => assert(didDelete))
  await ht2.delete('maroon').then(didDelete => assert(!didDelete))
  assert.equal(await ht2.get('maroon'), undefined)
  await ht2.delete(collisionKey).then(didDelete => assert(didDelete))
  assert.equal(await ht2.get(collisionKey), undefined)
  await ht2.delete('maroon3').then(didDelete => assert(!didDelete))

  ht2.close()
}).case()

const test1_2 = new Test('DiskHashTable', async function integration1_2() {
  const ht3 = new DiskHashTable({
    storagePath: `${__dirname}/DiskHashTable_test_data/3`,
    headerPath: `${__dirname}/DiskHashTable_test_data/3_header`,
    initialLength: 3,
  })
  await ht3.destroy()
  await ht3.init()

  await ht3.set('maroon', '#800000')
  assert.equal(await ht3.get('maroon'), '#800000')
  const collisionKey = 'maroon3'
  await ht3.set(collisionKey, '#800000(1)')
  assert.equal(await ht3.get('maroon'), '#800000')
  assert.equal(await ht3.get(collisionKey), '#800000(1)')

  ht3.close()
}).case()

const test1_3 = new Test('DiskHashTable', async function integration1_3() {
  const ht1024 = new DiskHashTable({
    storagePath: `${__dirname}/DiskHashTable_test_data/1024`,
    headerPath: `${__dirname}/DiskHashTable_test_data/1024_header`,
    initialLength: 1024,
  })
  await ht1024.destroy()
  await ht1024.init()

  assert.equal(ht1024._length, 1024)

  assert.strictEqual(ht1024.count(), 0)

  await ht1024.set('maroon', '#800000')
  await ht1024.set('yellow', '#FFFF00')
  await ht1024.set('black', '#000000')

  assert.equal(await ht1024.get('black'), '#000000')
  assert.equal(await ht1024.get('yellow'), '#FFFF00')
  assert.equal(await ht1024.get('black'), '#000000')

  assert.strictEqual(ht1024.count(), 3)

  await ht1024.close()
  await ht1024.init()

  assert.equal(ht1024._length, 1024)

  assert.equal(await ht1024.get('black'), '#000000')
  assert.equal(await ht1024.get('yellow'), '#FFFF00')
  assert.equal(await ht1024.get('black'), '#000000')

  assert.strictEqual(ht1024.count(), 3)

  await ht1024.clear()

  assert.strictEqual(await ht1024.get('black'), undefined)
  assert.strictEqual(await ht1024.get('yellow'), undefined)
  assert.strictEqual(await ht1024.get('black'), undefined)

  assert.strictEqual(ht1024.count(), 0)

  ht1024.close()
}).case()

const test1_4 = new Test('DiskHashTable', async function integration1_4() {
  const ht1024 = new DiskHashTable({
    storagePath: `${__dirname}/DiskHashTable_test_data/1024`,
    headerPath: `${__dirname}/DiskHashTable_test_data/1024_header`,
    initialLength: 1024,
  })
  await ht1024.destroy()
  await ht1024.init()

  await ht1024.set('maroon', '#800000')
  await ht1024.set('yellow', '#FFFF00')
  await ht1024.set('black', '#000000')

  {
    const values = []
    for await (const value of ht1024.iterator()) {
      values.push(value)
    }
    assert.equal(values.length, 3)
    assert.equal(values[0], '#000000')
    assert.equal(values[1], '#FFFF00')
    assert.equal(values[2], '#800000')
  }

  {
    const items = []
    for await (const item of ht1024._itemsIterator()) {
      items.push(item)
    }
    assert.equal(items.length, 3)
    assert.equal(items[0].value, '#000000')
    assert.equal(items[0].key, 'black')
    assert.equal(items[1].value, '#FFFF00')
    assert.equal(items[1].key, 'yellow')
    assert.equal(items[2].value, '#800000')
    assert.equal(items[2].key, 'maroon')
  }

  ht1024.close()
}).case()

const test1_5 = new Test('DiskHashTable', async function integration1_5() {
  const ht10 = new DiskHashTable({
    storagePath: `${__dirname}/DiskHashTable_test_data/10`,
    headerPath: `${__dirname}/DiskHashTable_test_data/10_header`,
    initialLength: 10,
    resizeRatio: 0.8,
    resizeFactor: 2,
  })
  await ht10.destroy()
  await ht10.init()

  assert.equal(ht10._count, 0)
  assert.equal(ht10._length, 10)

  await ht10.set('1', 'value1')
  await ht10.set('2', 'value2')
  await ht10.set('3', 'value3')

  assert.equal(ht10._count, 3)
  assert.equal(ht10._length, 10)

  await ht10.set('4', 'value4')
  await ht10.set('5', 'value5')
  await ht10.set('6', 'value6')

  assert.equal(ht10._count, 6)
  assert.equal(ht10._length, 10)

  await ht10.set('7', 'value7')

  assert.equal(ht10._count, 7)
  assert.equal(ht10._length, 10)

  await ht10.set('8', 'value8')

  assert.equal(ht10._count, 8)
  assert.equal(ht10._length, 10)

  await ht10.set('9', 'value9')

  assert.equal(ht10._count, 9)
  assert.equal(ht10._length, 20)

  await ht10.set('10', 'value10')

  assert.equal(ht10._count, 10)
  assert.equal(ht10._length, 20)

  await ht10.set('11', 'value11')
  await ht10.set('12', 'value12')
  await ht10.set('13', 'value13')
  await ht10.set('14', 'value14')
  await ht10.set('15', 'value15')
  await ht10.set('16', 'value16')

  assert.equal(ht10._count, 16)
  assert.equal(ht10._length, 20)

  await ht10.set('17', 'value17')

  assert.equal(ht10._count, 17)
  assert.equal(ht10._length, 40)

  await ht10.set('18', 'value18')
  await ht10.set('19', 'value19')
  await ht10.set('20', 'value20')
  await ht10.set('21', 'value21')
  await ht10.set('22', 'value22')
  await ht10.set('23', 'value23')
  await ht10.set('24', 'value24')
  await ht10.set('25', 'value25')
  await ht10.set('26', 'value26')
  await ht10.set('27', 'value27')
  await ht10.set('28', 'value28')
  await ht10.set('29', 'value29')
  await ht10.set('30', 'value30')
  await ht10.set('31', 'value31')
  await ht10.set('32', 'value32')

  assert.equal(ht10._count, 32)
  assert.equal(ht10._length, 40)

  await ht10.set('33', 'value33')

  assert.equal(ht10._count, 33)
  assert.equal(ht10._length, 80)

  ht10.close()
}).case()

const test = Test.all([
  test1,
  test1_1,
  test1_2,
  test1_3,
  test1_4,
  test1_5,
])

if (process.argv[1] == __filename) {
  test()
}

module.exports = test
