const Test = require('thunk-test')
const assert = require('assert')
const DiskSortedHashTable = require('./DiskSortedHashTable')

const test1 = new Test('DiskSortedHashTable', async function integration1() {
  const ht1024 = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/1024`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/1024_header`,
    initialLength: 1024,
    sortValueType: 'number',
  })
  await ht1024.destroy()
  await ht1024.init()

  assert.strictEqual(ht1024.count(), 0)

  {
    const forwardValues = []
    for await (const value of ht1024.forwardIterator()) {
      forwardValues.push(value)
    }
    assert.equal(forwardValues.length, 0)
  }

  {
    const reverseValues = []
    for await (const value of ht1024.reverseIterator()) {
      reverseValues.push(value)
    }
    assert.equal(reverseValues.length, 0)
  }

  await ht1024.set('maroon', '#800000', 1)
  await ht1024.set('yellow', '#FFFF00', 2)

  assert.equal(ht1024.count(), 2)

  assert.equal(await ht1024.get('maroon'), '#800000')
  assert.equal(await ht1024.get('yellow'), '#FFFF00')

  await ht1024.set('black', '#000', 4)

  assert.equal(ht1024.count(), 3)

  {
    const forwardValues = []
    for await (const value of ht1024.forwardIterator()) {
      forwardValues.push(value)
    }
    assert.equal(forwardValues.length, 3)
    assert.equal(forwardValues[0], '#800000')
    assert.equal(forwardValues[1], '#FFFF00')
    assert.equal(forwardValues[2], '#000')
  }

  {
    const reverseValues = []
    for await (const value of ht1024.reverseIterator()) {
      reverseValues.push(value)
    }
    assert.equal(reverseValues.length, 3)
    assert.equal(reverseValues[0], '#000')
    assert.equal(reverseValues[1], '#FFFF00')
    assert.equal(reverseValues[2], '#800000')
  }

  assert.strictEqual(await ht1024.get('notfound'), undefined)
  await ht1024.delete('notfound').then(didDelete => assert(!didDelete))

  assert.equal(ht1024.count(), 3)

  await ht1024.delete('maroon').then(didDelete => assert(didDelete))
  assert.strictEqual(await ht1024.get('maroon'), undefined)

  assert.equal(ht1024.count(), 2)

  {
    const forwardValues = []
    for await (const value of ht1024.forwardIterator()) {
      forwardValues.push(value)
    }
    assert.equal(forwardValues.length, 2)
    assert.equal(forwardValues[0], '#FFFF00')
    assert.equal(forwardValues[1], '#000')
  }

  {
    const reverseValues = []
    for await (const value of ht1024.reverseIterator()) {
      reverseValues.push(value)
    }
    assert.equal(reverseValues.length, 2)
    assert.equal(reverseValues[0], '#000')
    assert.equal(reverseValues[1], '#FFFF00')
  }

  await ht1024.clear()

  assert.equal(ht1024.count(), 0)

  {
    const forwardValues = []
    for await (const value of ht1024.forwardIterator()) {
      forwardValues.push(value)
    }
    assert.equal(forwardValues.length, 0)
  }

  {
    const reverseValues = []
    for await (const value of ht1024.reverseIterator()) {
      reverseValues.push(value)
    }
    assert.equal(reverseValues.length, 0)
  }

  const ht1 = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/1`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/1_header`,
    initialLength: 1,
    sortValueType: 'number',
  })
  await ht1.destroy()
  await ht1.init()

  await ht1.set('maroon', '#800000', 1)
  assert.strictEqual(await ht1.get('x'), undefined)

  await assert.rejects(
    ht1.set('yellow', '#FFFF00', 2),
    new Error('Hash table is full')
  )

  await assert.rejects(
    ht1._getKey(-1),
    new Error('Negative index')
  )

  ht1024.close()
  ht1.close()
}).case()

const test1_1 = new Test('DiskSortedHashTable', async function integration1_1() {
  const ht2 = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/2`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/2_header`,
    initialLength: 2,
    sortValueType: 'number',
  })
  await ht2.destroy()
  await ht2.init()

  await ht2.set('maroon', '#800000', 1)
  assert.equal(await ht2.get('maroon'), '#800000')
  const collisionKey = 'maroon1'
  await ht2.set(collisionKey, '#800000(1)', 1)
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

const test1_2 = new Test('DiskSortedHashTable', async function integration1_2() {
  const ht3 = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/3`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/3_header`,
    initialLength: 3,
    sortValueType: 'number',
  })
  await ht3.destroy()
  await ht3.init()

  await ht3.set('maroon', '#800000', 1)
  assert.equal(await ht3.get('maroon'), '#800000')
  const collisionKey = 'maroon3'
  await ht3.set(collisionKey, '#800000(1)', 1)
  assert.equal(await ht3.get('maroon'), '#800000')
  assert.equal(await ht3.get(collisionKey), '#800000(1)')

  ht3.close()
}).case()

const test1_3 = new Test('DiskSortedHashTable', async function integration1_3() {
  const ht1024 = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/1024`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/1024_header`,
    initialLength: 1024,
    sortValueType: 'string',
  })
  await ht1024.destroy()
  await ht1024.init()

  await ht1024.set('maroon', '#800000', 'a')
  await ht1024.set('yellow', '#FFFF00', 'b')

  assert.equal(await ht1024.get('maroon'), '#800000')
  assert.equal(await ht1024.get('yellow'), '#FFFF00')

  await ht1024.set('black', '#000', 'd')

  {
    const forwardValues = []
    for await (const value of ht1024.forwardIterator()) {
      forwardValues.push(value)
    }
    assert.equal(forwardValues.length, 3)
    assert.equal(forwardValues[0], '#800000')
    assert.equal(forwardValues[1], '#FFFF00')
    assert.equal(forwardValues[2], '#000')
  }

  {
    const reverseValues = []
    for await (const value of ht1024.reverseIterator()) {
      reverseValues.push(value)
    }
    assert.equal(reverseValues.length, 3)
    assert.equal(reverseValues[0], '#000')
    assert.equal(reverseValues[1], '#FFFF00')
    assert.equal(reverseValues[2], '#800000')
  }

  await ht1024.set('maroon', '#800000', 'e')

  {
    const forwardValues = []
    for await (const value of ht1024.forwardIterator()) {
      forwardValues.push(value)
    }
    assert.equal(forwardValues.length, 3)
    assert.equal(forwardValues[0], '#FFFF00')
    assert.equal(forwardValues[1], '#000')
    assert.equal(forwardValues[2], '#800000')
  }

  ht1024.close()
}).case()

const test1_4 = new Test('DiskSortedHashTable', async function integration1_4() {
  const ht1024 = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/1024`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/1024_header`,
    initialLength: 1024,
    sortValueType: 'number',
  })
  await ht1024.destroy()
  await ht1024.init()

  assert.equal(ht1024._length, 1024)

  assert.strictEqual(ht1024.count(), 0)

  await ht1024.set('maroon', '#800000', 1)
  await ht1024.set('yellow', '#FFFF00', 2)
  await ht1024.set('black', '#000000', 3)

  assert.equal(ht1024.count(), 3)

  assert.equal(await ht1024.get('black'), '#000000')
  assert.equal(await ht1024.get('yellow'), '#FFFF00')
  assert.equal(await ht1024.get('black'), '#000000')

  assert.equal(ht1024.count(), 3)

  await ht1024.close()
  await ht1024.init()

  assert.equal(ht1024.count(), 3)
  assert.equal(ht1024._length, 1024)

  assert.equal(await ht1024.get('black'), '#000000')
  assert.equal(await ht1024.get('yellow'), '#FFFF00')
  assert.equal(await ht1024.get('black'), '#000000')

  await ht1024.clear()

  assert.strictEqual(ht1024.count(), 0)

  assert.strictEqual(await ht1024.get('black'), undefined)
  assert.strictEqual(await ht1024.get('yellow'), undefined)
  assert.strictEqual(await ht1024.get('black'), undefined)

  assert.equal(ht1024.count(), 0)

  ht1024.close()
}).case()

const test2 = new Test('DiskSortedHashTable', async function integration2() {
  const ht1024 = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/1024`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/1024_header`,
    initialLength: 1024,
    sortValueType: 'number',
  })
  await ht1024.destroy()
  await ht1024.init()

  await ht1024.set('black', '#000', 4)
  await ht1024.set('yellow', '#FFFF00', 2)
  await ht1024.set('maroon', '#800000', 1)

  {
    const forwardValues = []
    for await (const value of ht1024.forwardIterator()) {
      forwardValues.push(value)
    }
    assert.equal(forwardValues.length, 3)
    assert.equal(forwardValues[0], '#800000')
    assert.equal(forwardValues[1], '#FFFF00')
    assert.equal(forwardValues[2], '#000')
  }

  {
    const reverseValues = []
    for await (const value of ht1024.reverseIterator()) {
      reverseValues.push(value)
    }
    assert.equal(reverseValues.length, 3)
    assert.equal(reverseValues[0], '#000')
    assert.equal(reverseValues[1], '#FFFF00')
    assert.equal(reverseValues[2], '#800000')
  }

  ht1024.close()
}).case()

const test3 = new Test('DiskSortedHashTable', async function integration3() {
  const ht1024 = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/1024`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/1024_header`,
    initialLength: 1024,
    sortValueType: 'number',
  })
  await ht1024.destroy()
  await ht1024.init()

  await ht1024.set('yellow', '#FFFF00', 2)
  await ht1024.set('black', '#000', 4)
  await ht1024.set('maroon', '#800000', 1)

  {
    const forwardValues = []
    for await (const value of ht1024.forwardIterator()) {
      forwardValues.push(value)
    }
    assert.equal(forwardValues.length, 3)
    assert.equal(forwardValues[0], '#800000')
    assert.equal(forwardValues[1], '#FFFF00')
    assert.equal(forwardValues[2], '#000')
  }

  {
    const reverseValues = []
    for await (const value of ht1024.reverseIterator()) {
      reverseValues.push(value)
    }
    assert.equal(reverseValues.length, 3)
    assert.equal(reverseValues[0], '#000')
    assert.equal(reverseValues[1], '#FFFF00')
    assert.equal(reverseValues[2], '#800000')
  }

  ht1024.close()
}).case()

const test4 = new Test('DiskSortedHashTable', async function integration4() {
  const ht1024 = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/1024`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/1024_header`,
    initialLength: 1024,
    sortValueType: 'number',
  })
  await ht1024.destroy()
  await ht1024.init()

  await ht1024.set('maroon', '#800000', 1)
  await ht1024.set('yellow', '#FFFF00', 2)
  await ht1024.set('black', '#000', 4)
  await ht1024.set('white', '#FFF', 3)

  {
    const forwardValues = []
    for await (const value of ht1024.forwardIterator()) {
      forwardValues.push(value)
    }
    assert.equal(forwardValues.length, 4)
    assert.equal(forwardValues[0], '#800000')
    assert.equal(forwardValues[1], '#FFFF00')
    assert.equal(forwardValues[2], '#FFF')
    assert.equal(forwardValues[3], '#000')
  }

  {
    const reverseValues = []
    for await (const value of ht1024.reverseIterator()) {
      reverseValues.push(value)
    }
    assert.equal(reverseValues.length, 4)
    assert.equal(reverseValues[0], '#000')
    assert.equal(reverseValues[1], '#FFF')
    assert.equal(reverseValues[2], '#FFFF00')
    assert.equal(reverseValues[3], '#800000')
  }

  ht1024.close()
}).case()

const test5 = new Test('DiskSortedHashTable', async function integration5() {
  const ht1024 = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/1024`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/1024_header`,
    initialLength: 1024,
    sortValueType: 'number',
  })
  await ht1024.destroy()
  await ht1024.init()

  await ht1024.set('maroon', '#800000', 1)
  await ht1024.set('yellow', '#FFFF00', 2)
  await ht1024.set('black', '#000', 4)
  await ht1024.set('white', '#FFF', 3)
  await ht1024.set('white2', '#FFF(2)', 3)

  {
    const forwardValues = []
    for await (const value of ht1024.forwardIterator()) {
      forwardValues.push(value)
    }
    assert.equal(forwardValues.length, 5)
    assert.equal(forwardValues[0], '#800000')
    assert.equal(forwardValues[1], '#FFFF00')
    assert.equal(forwardValues[2], '#FFF')
    assert.equal(forwardValues[3], '#FFF(2)')
    assert.equal(forwardValues[4], '#000')
  }

  {
    const reverseValues = []
    for await (const value of ht1024.reverseIterator()) {
      reverseValues.push(value)
    }
    assert.equal(reverseValues.length, 5)
    assert.equal(reverseValues[0], '#000')
    assert.equal(reverseValues[1], '#FFF(2)')
    assert.equal(reverseValues[2], '#FFF')
    assert.equal(reverseValues[3], '#FFFF00')
    assert.equal(reverseValues[4], '#800000')
  }

  await ht1024.set('white3', '#FFF(3)', 3)

  {
    const forwardValues = []
    for await (const value of ht1024.forwardIterator()) {
      forwardValues.push(value)
    }
    assert.equal(forwardValues.length, 6)
    assert.equal(forwardValues[0], '#800000')
    assert.equal(forwardValues[1], '#FFFF00')
    assert.equal(forwardValues[2], '#FFF')
    assert.equal(forwardValues[3], '#FFF(3)')
    assert.equal(forwardValues[4], '#FFF(2)')
    assert.equal(forwardValues[5], '#000')
  }

  {
    const reverseValues = []
    for await (const value of ht1024.reverseIterator()) {
      reverseValues.push(value)
    }
    assert.equal(reverseValues.length, 6)
    assert.equal(reverseValues[0], '#000')
    assert.equal(reverseValues[1], '#FFF(2)')
    assert.equal(reverseValues[2], '#FFF(3)')
    assert.equal(reverseValues[3], '#FFF')
    assert.equal(reverseValues[4], '#FFFF00')
    assert.equal(reverseValues[5], '#800000')
  }

  ht1024.close()
}).case()

const test6 = new Test('DiskSortedHashTable', async function integration6() {
  const ht1024 = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/1024`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/1024_header`,
    initialLength: 1024,
    sortValueType: 'number',
  })
  await ht1024.destroy()
  await ht1024.init()

  await ht1024.set('maroon', '#800000', 1)
  await ht1024.set('yellow', '#FFFF00', 2)
  await ht1024.set('black', '#000', 4)

  await ht1024.close()
  await ht1024.init()

  {
    const forwardValues = []
    for await (const value of ht1024.forwardIterator()) {
      forwardValues.push(value)
    }
    assert.equal(forwardValues.length, 3)
    assert.equal(forwardValues[0], '#800000')
    assert.equal(forwardValues[1], '#FFFF00')
    assert.equal(forwardValues[2], '#000')
  }

  {
    const reverseValues = []
    for await (const value of ht1024.reverseIterator()) {
      reverseValues.push(value)
    }
    assert.equal(reverseValues.length, 3)
    assert.equal(reverseValues[0], '#000')
    assert.equal(reverseValues[1], '#FFFF00')
    assert.equal(reverseValues[2], '#800000')
  }

  await ht1024.set('maroon', '#800000', 1)
  await ht1024.set('yellow', '#FFFF00', 2)

  assert.equal(await ht1024.get('maroon'), '#800000')
  assert.equal(await ht1024.get('yellow'), '#FFFF00')

  await ht1024.set('black', '#000', 4)

  {
    const forwardValues = []
    for await (const value of ht1024.forwardIterator()) {
      forwardValues.push(value)
    }
    assert.equal(forwardValues.length, 3)
    assert.equal(forwardValues[0], '#800000')
    assert.equal(forwardValues[1], '#FFFF00')
    assert.equal(forwardValues[2], '#000')
  }

  {
    const reverseValues = []
    for await (const value of ht1024.reverseIterator()) {
      reverseValues.push(value)
    }
    assert.equal(reverseValues.length, 3)
    assert.equal(reverseValues[0], '#000')
    assert.equal(reverseValues[1], '#FFFF00')
    assert.equal(reverseValues[2], '#800000')
  }

  ht1024.close()
}).case()

const test7 = new Test('DiskSortedHashTable', async function integration7() {
  const ht1024 = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/1024`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/1024_header`,
    initialLength: 1024,
    sortValueType: 'number',
  })
  await ht1024.destroy()
  await ht1024.init()

  await ht1024.set('maroon', '#800000', 1)
  await ht1024.set('yellow', '#FFFF00', 2)
  await ht1024.set('black', '#000', 4)

  {
    const forwardValues = []
    for await (const value of ht1024.forwardIterator()) {
      forwardValues.push(value)
    }
    assert.equal(forwardValues.length, 3)
    assert.equal(forwardValues[0], '#800000')
    assert.equal(forwardValues[1], '#FFFF00')
    assert.equal(forwardValues[2], '#000')
  }

  await ht1024.set('maroon', '#800000', 5)

  {
    const forwardValues = []
    for await (const value of ht1024.forwardIterator()) {
      forwardValues.push(value)
    }
    assert.equal(forwardValues.length, 3)
    assert.equal(forwardValues[0], '#FFFF00')
    assert.equal(forwardValues[1], '#000')
    assert.equal(forwardValues[2], '#800000')
  }

  await ht1024.set('yellow', '#FFFF00', 6)

  {
    const forwardValues = []
    for await (const value of ht1024.forwardIterator()) {
      forwardValues.push(value)
    }
    assert.equal(forwardValues.length, 3)
    assert.equal(forwardValues[0], '#000')
    assert.equal(forwardValues[1], '#800000')
    assert.equal(forwardValues[2], '#FFFF00')
  }

  await ht1024.set('black', '#000000', 7)

  {
    const forwardValues = []
    for await (const value of ht1024.forwardIterator()) {
      forwardValues.push(value)
    }
    assert.equal(forwardValues.length, 3)
    assert.equal(forwardValues[0], '#800000')
    assert.equal(forwardValues[1], '#FFFF00')
    assert.equal(forwardValues[2], '#000000')
  }

  ht1024.close()
}).case()

const test8 = new Test('DiskSortedHashTable', async function integration8() {
  const ht1024 = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/1024`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/1024_header`,
    initialLength: 1024,
    sortValueType: 'number',
  })
  await ht1024.destroy()
  await ht1024.init()

  await ht1024.set('maroon', '#800000', 1)
  await ht1024.set('yellow', '#FFFF00', 2)
  await ht1024.set('black', '#000', 4)

  {
    const forwardValues = []
    for await (const value of ht1024.forwardIterator()) {
      forwardValues.push(value)
    }
    assert.equal(forwardValues.length, 3)
    assert.equal(forwardValues[0], '#800000')
    assert.equal(forwardValues[1], '#FFFF00')
    assert.equal(forwardValues[2], '#000')
  }

  await ht1024.set('black', '#000000', 0)

  {
    const forwardValues = []
    for await (const value of ht1024.forwardIterator()) {
      forwardValues.push(value)
    }
    assert.equal(forwardValues.length, 3)
    assert.equal(forwardValues[0], '#000000')
    assert.equal(forwardValues[1], '#800000')
    assert.equal(forwardValues[2], '#FFFF00')
  }

  await ht1024.set('yellow', '#FFFF00', -1)

  {
    const forwardValues = []
    for await (const value of ht1024.forwardIterator()) {
      forwardValues.push(value)
    }
    assert.equal(forwardValues.length, 3)
    assert.equal(forwardValues[0], '#FFFF00')
    assert.equal(forwardValues[1], '#000000')
    assert.equal(forwardValues[2], '#800000')
  }

  await ht1024.set('maroon', '#800000', -2)

  {
    const forwardValues = []
    for await (const value of ht1024.forwardIterator()) {
      forwardValues.push(value)
    }
    assert.equal(forwardValues.length, 3)
    assert.equal(forwardValues[0], '#800000')
    assert.equal(forwardValues[1], '#FFFF00')
    assert.equal(forwardValues[2], '#000000')
  }

  ht1024.close()
}).case()

const test9 = new Test('DiskSortedHashTable', async function integration9() {
  const ht1024 = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/1024`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/1024_header`,
    initialLength: 1024,
    sortValueType: 'number',
  })
  await ht1024.destroy()
  await ht1024.init()

  await ht1024.set('maroon', '#800000', 1)
  await ht1024.set('yellow', '#FFFF00', 50)
  await ht1024.set('black', '#000', 100)

  {
    const forwardValues = []
    for await (const value of ht1024.forwardIterator()) {
      forwardValues.push(value)
    }
    assert.equal(forwardValues.length, 3)
    assert.equal(forwardValues[0], '#800000')
    assert.equal(forwardValues[1], '#FFFF00')
    assert.equal(forwardValues[2], '#000')
  }

  await ht1024.set('yellow', '#FFFF00', 51)

  {
    const forwardValues = []
    for await (const value of ht1024.forwardIterator()) {
      forwardValues.push(value)
    }
    assert.equal(forwardValues.length, 3)
    assert.equal(forwardValues[0], '#800000')
    assert.equal(forwardValues[1], '#FFFF00')
    assert.equal(forwardValues[2], '#000')
  }

  await ht1024.set('black', '#000000', 24)

  {
    const forwardValues = []
    for await (const value of ht1024.forwardIterator()) {
      forwardValues.push(value)
    }
    assert.equal(forwardValues.length, 3)
    assert.equal(forwardValues[0], '#800000')
    assert.equal(forwardValues[1], '#000000')
    assert.equal(forwardValues[2], '#FFFF00')
  }

  await ht1024.set('maroon', '#800000', 26)

  {
    const forwardValues = []
    for await (const value of ht1024.forwardIterator()) {
      forwardValues.push(value)
    }
    assert.equal(forwardValues.length, 3)
    assert.equal(forwardValues[0], '#000000')
    assert.equal(forwardValues[1], '#800000')
    assert.equal(forwardValues[2], '#FFFF00')
  }

  await ht1024.set('yellow', '#FFFF00', 25)

  {
    const forwardValues = []
    for await (const value of ht1024.forwardIterator()) {
      forwardValues.push(value)
    }
    assert.equal(forwardValues.length, 3)
    assert.equal(forwardValues[0], '#000000')
    assert.equal(forwardValues[1], '#FFFF00')
    assert.equal(forwardValues[2], '#800000')
  }

  ht1024.close()
}).case()

const test10 = new Test('DiskSortedHashTable', async function integration10() {
  const ht1024 = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/1024`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/1024_header`,
    initialLength: 1024,
    sortValueType: 'number',
  })
  await ht1024.destroy()
  await ht1024.init()

  await ht1024.set('maroon', '#800000', 1)
  await ht1024.set('maroon', '#800000', 3)
  await ht1024.set('yellow', '#FFFF00', 1)
  await ht1024.set('black', '#000000', 0)

  {
    const forwardValues = []
    for await (const value of ht1024.forwardIterator()) {
      forwardValues.push(value)
    }
    assert.equal(forwardValues.length, 3)
    assert.equal(forwardValues[0], '#000000')
    assert.equal(forwardValues[1], '#FFFF00')
    assert.equal(forwardValues[2], '#800000')
  }

  ht1024.close()
}).case()

const test11 = new Test('DiskSortedHashTable', async function integration11() {
  const ht1024 = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/1024`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/1024_header`,
    initialLength: 1024,
    sortValueType: 'number',
  })
  await ht1024.destroy()
  await ht1024.init()

  await ht1024.set('maroon', '#800000', 1)
  await ht1024.set('yellow', '#FFFF00', 2)
  await ht1024.set('black', '#000', 4)

  {
    const forwardValues = []
    for await (const value of ht1024.forwardIterator()) {
      forwardValues.push(value)
    }
    assert.equal(forwardValues.length, 3)
    assert.equal(forwardValues[0], '#800000')
    assert.equal(forwardValues[1], '#FFFF00')
    assert.equal(forwardValues[2], '#000')
  }

  await ht1024.delete('maroon')

  {
    const forwardValues = []
    for await (const value of ht1024.forwardIterator()) {
      forwardValues.push(value)
    }
    assert.equal(forwardValues.length, 2)
    assert.equal(forwardValues[0], '#FFFF00')
    assert.equal(forwardValues[1], '#000')
  }

  await ht1024.delete('yellow')

  {
    const forwardValues = []
    for await (const value of ht1024.forwardIterator()) {
      forwardValues.push(value)
    }
    assert.equal(forwardValues.length, 1)
    assert.equal(forwardValues[0], '#000')
  }

  await ht1024.delete('black')

  {
    const forwardValues = []
    for await (const value of ht1024.forwardIterator()) {
      forwardValues.push(value)
    }
    assert.equal(forwardValues.length, 0)
  }

  ht1024.close()
}).case()

const test12 = new Test('DiskSortedHashTable', async function integration12() {
  const ht1024 = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/1024`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/1024_header`,
    initialLength: 1024,
    sortValueType: 'number',
  })
  await ht1024.destroy()
  await ht1024.init()

  await ht1024.set('maroon', '#800000', 1)
  await ht1024.set('yellow', '#FFFF00', 2)
  await ht1024.set('black', '#000', 4)

  {
    const forwardValues = []
    for await (const value of ht1024.forwardIterator()) {
      forwardValues.push(value)
    }
    assert.equal(forwardValues.length, 3)
    assert.equal(forwardValues[0], '#800000')
    assert.equal(forwardValues[1], '#FFFF00')
    assert.equal(forwardValues[2], '#000')
  }

  await ht1024.delete('black')

  {
    const forwardValues = []
    for await (const value of ht1024.forwardIterator()) {
      forwardValues.push(value)
    }
    assert.equal(forwardValues.length, 2)
    assert.equal(forwardValues[0], '#800000')
    assert.equal(forwardValues[1], '#FFFF00')
  }

  await ht1024.delete('yellow')

  {
    const forwardValues = []
    for await (const value of ht1024.forwardIterator()) {
      forwardValues.push(value)
    }
    assert.equal(forwardValues.length, 1)
    assert.equal(forwardValues[0], '#800000')
  }

  await ht1024.delete('maroon')

  {
    const forwardValues = []
    for await (const value of ht1024.forwardIterator()) {
      forwardValues.push(value)
    }
    assert.equal(forwardValues.length, 0)
  }

  ht1024.close()
}).case()

const test13 = new Test('DiskSortedHashTable', async function integration13() {
  const ht1024 = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/1024`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/1024_header`,
    initialLength: 1024,
    sortValueType: 'number',
  })
  await ht1024.destroy()
  await ht1024.init()

  await ht1024.set('maroon', '#800000', 1)
  await ht1024.set('yellow', '#FFFF00', 2)
  await ht1024.set('black', '#000', 4)

  {
    const forwardValues = []
    for await (const value of ht1024.forwardIterator()) {
      forwardValues.push(value)
    }
    assert.equal(forwardValues.length, 3)
    assert.equal(forwardValues[0], '#800000')
    assert.equal(forwardValues[1], '#FFFF00')
    assert.equal(forwardValues[2], '#000')
  }

  await ht1024.delete('yellow')

  {
    const forwardValues = []
    for await (const value of ht1024.forwardIterator()) {
      forwardValues.push(value)
    }
    assert.equal(forwardValues.length, 2)
    assert.equal(forwardValues[0], '#800000')
    assert.equal(forwardValues[1], '#000')
  }

  ht1024.close()
}).case()

const test14_0 = new Test('DiskSortedHashTable', async function integration14_0() {
  const ht100 = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/10`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/10_header`,
    initialLength: 100,
    sortValueType: 'number',
    resizeRatio: 0,
  })
  await ht100.destroy()
  await ht100.init()

  async function logForwardValues() {
    const values = []
    for await (const value of ht100.forwardIterator()) {
      values.push(value)
    }
    console.log(values)
  }

  await ht100.set('1', 'value1', 1)

  {
    const values = []
    for await (const value of ht100.forwardIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value1',
    ])
  }

  {
    const values = []
    for await (const value of ht100.reverseIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value1',
    ])
  }

  await ht100.set('2', 'value2', 2)
  await ht100.set('3', 'value3', 3)

  {
    const values = []
    for await (const value of ht100.forwardIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value1',
      'value2',
      'value3',
    ])
  }

  {
    const values = []
    for await (const value of ht100.reverseIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value3',
      'value2',
      'value1',
    ])
  }

  await ht100.set('4', 'value4', 4)
  await ht100.set('5', 'value5', 5)
  await ht100.set('6', 'value6', 6)

  {
    const values = []
    for await (const value of ht100.forwardIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value1',
      'value2',
      'value3',
      'value4',
      'value5',
      'value6',
    ])
  }

  {
    const values = []
    for await (const value of ht100.reverseIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value6',
      'value5',
      'value4',
      'value3',
      'value2',
      'value1',
    ])
  }

  await ht100.set('7', 'value7', 7)
  await ht100.set('8', 'value8', 8)

  {
    const values = []
    for await (const value of ht100.forwardIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value1',
      'value2',
      'value3',
      'value4',
      'value5',
      'value6',
      'value7',
      'value8',
    ])
  }

  {
    const values = []
    for await (const value of ht100.reverseIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value8',
      'value7',
      'value6',
      'value5',
      'value4',
      'value3',
      'value2',
      'value1',
    ])
  }

  await ht100.set('9', 'value9', 9)

  {
    const values = []
    for await (const value of ht100.forwardIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value1',
      'value2',
      'value3',
      'value4',
      'value5',
      'value6',
      'value7',
      'value8',
      'value9',
    ])
  }

  {
    const values = []
    for await (const value of ht100.reverseIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value9',
      'value8',
      'value7',
      'value6',
      'value5',
      'value4',
      'value3',
      'value2',
      'value1',
    ])
  }

  await ht100.set('10', 'value10', 10)
  await ht100.set('11', 'value11', 11)
  await ht100.set('12', 'value12', 12)
  await ht100.set('13', 'value13', 13)
  await ht100.set('14', 'value14', 14)
  await ht100.set('15', 'value15', 15)
  await ht100.set('16', 'value16', 16)

  {
    const values = []
    for await (const value of ht100.forwardIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value1',
      'value2',
      'value3',
      'value4',
      'value5',
      'value6',
      'value7',
      'value8',
      'value9',
      'value10',
      'value11',
      'value12',
      'value13',
      'value14',
      'value15',
      'value16',
    ])
  }

  {
    const values = []
    for await (const value of ht100.reverseIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value16',
      'value15',
      'value14',
      'value13',
      'value12',
      'value11',
      'value10',
      'value9',
      'value8',
      'value7',
      'value6',
      'value5',
      'value4',
      'value3',
      'value2',
      'value1',
    ])
  }

  await ht100.set('17', 'value17', 17)

  {
    const values = []
    for await (const value of ht100.forwardIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value1',
      'value2',
      'value3',
      'value4',
      'value5',
      'value6',
      'value7',
      'value8',
      'value9',
      'value10',
      'value11',
      'value12',
      'value13',
      'value14',
      'value15',
      'value16',
      'value17',
    ])
  }

  {
    const values = []
    for await (const value of ht100.reverseIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value17',
      'value16',
      'value15',
      'value14',
      'value13',
      'value12',
      'value11',
      'value10',
      'value9',
      'value8',
      'value7',
      'value6',
      'value5',
      'value4',
      'value3',
      'value2',
      'value1',
    ])
  }

  await ht100.set('18', 'value18', 18)
  await ht100.set('19', 'value19', 19)
  await ht100.set('20', 'value20', 20)
  await ht100.set('21', 'value21', 21)
  await ht100.set('22', 'value22', 22)
  await ht100.set('23', 'value23', 23)
  await ht100.set('24', 'value24', 24)
  await ht100.set('25', 'value25', 25)
  await ht100.set('26', 'value26', 26)
  await ht100.set('27', 'value27', 27)
  await ht100.set('28', 'value28', 28)
  await ht100.set('29', 'value29', 29)
  await ht100.set('30', 'value30', 30)
  await ht100.set('31', 'value31', 31)
  await ht100.set('32', 'value32', 32)

  {
    const values = []
    for await (const value of ht100.forwardIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value1',
      'value2',
      'value3',
      'value4',
      'value5',
      'value6',
      'value7',
      'value8',
      'value9',
      'value10',
      'value11',
      'value12',
      'value13',
      'value14',
      'value15',
      'value16',
      'value17',
      'value18',
      'value19',
      'value20',
      'value21',
      'value22',
      'value23',
      'value24',
      'value25',
      'value26',
      'value27',
      'value28',
      'value29',
      'value30',
      'value31',
      'value32',
    ])
  }

  {
    const values = []
    for await (const value of ht100.reverseIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value32',
      'value31',
      'value30',
      'value29',
      'value28',
      'value27',
      'value26',
      'value25',
      'value24',
      'value23',
      'value22',
      'value21',
      'value20',
      'value19',
      'value18',
      'value17',
      'value16',
      'value15',
      'value14',
      'value13',
      'value12',
      'value11',
      'value10',
      'value9',
      'value8',
      'value7',
      'value6',
      'value5',
      'value4',
      'value3',
      'value2',
      'value1',
    ])
  }

  await ht100.set('33', 'value33', 33)

  {
    const values = []
    for await (const value of ht100.forwardIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value1',
      'value2',
      'value3',
      'value4',
      'value5',
      'value6',
      'value7',
      'value8',
      'value9',
      'value10',
      'value11',
      'value12',
      'value13',
      'value14',
      'value15',
      'value16',
      'value17',
      'value18',
      'value19',
      'value20',
      'value21',
      'value22',
      'value23',
      'value24',
      'value25',
      'value26',
      'value27',
      'value28',
      'value29',
      'value30',
      'value31',
      'value32',
      'value33',
    ])
  }

  {
    const values = []
    for await (const value of ht100.reverseIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value33',
      'value32',
      'value31',
      'value30',
      'value29',
      'value28',
      'value27',
      'value26',
      'value25',
      'value24',
      'value23',
      'value22',
      'value21',
      'value20',
      'value19',
      'value18',
      'value17',
      'value16',
      'value15',
      'value14',
      'value13',
      'value12',
      'value11',
      'value10',
      'value9',
      'value8',
      'value7',
      'value6',
      'value5',
      'value4',
      'value3',
      'value2',
      'value1',
    ])
  }

  ht100.close()
}).case()

const test14 = new Test('DiskSortedHashTable', async function integration14() {
  const ht10 = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/10`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/10_header`,
    initialLength: 10,
    sortValueType: 'number',
    resizeRatio: 0.8,
    resizeFactor: 2,
  })
  await ht10.destroy()
  await ht10.init()

  async function logForwardValues() {
    const values = []
    for await (const item of ht10._forwardItemsIterator()) {
      values.push(item.value)
    }
    console.log(values)
  }

  assert.equal(ht10._count, 0)
  assert.equal(ht10._length, 10)

  await ht10.set('1', 'value1', 1)

  {
    const values = []
    for await (const value of ht10.forwardIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value1',
    ])
  }

  {
    const values = []
    for await (const value of ht10.reverseIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value1',
    ])
  }

  await ht10.set('2', 'value2', 2)
  await ht10.set('3', 'value3', 3)

  assert.equal(ht10._count, 3)
  assert.equal(ht10._length, 10)

  {
    const values = []
    for await (const value of ht10.forwardIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value1',
      'value2',
      'value3',
    ])
  }

  {
    const values = []
    for await (const value of ht10.reverseIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value3',
      'value2',
      'value1',
    ])
  }

  await ht10.set('4', 'value4', 4)
  await ht10.set('5', 'value5', 5)
  await ht10.set('6', 'value6', 6)

  assert.equal(ht10._count, 6)
  assert.equal(ht10._length, 10)

  {
    const values = []
    for await (const value of ht10.forwardIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value1',
      'value2',
      'value3',
      'value4',
      'value5',
      'value6',
    ])
  }

  {
    const values = []
    for await (const value of ht10.reverseIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value6',
      'value5',
      'value4',
      'value3',
      'value2',
      'value1',
    ])
  }

  await ht10.set('7', 'value7', 7)

  assert.equal(ht10._count, 7)
  assert.equal(ht10._length, 10)

  await ht10.set('8', 'value8', 8)

  assert.equal(ht10._count, 8)
  assert.equal(ht10._length, 10)

  {
    const values = []
    for await (const value of ht10.forwardIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value1',
      'value2',
      'value3',
      'value4',
      'value5',
      'value6',
      'value7',
      'value8',
    ])
  }

  {
    const values = []
    for await (const value of ht10.reverseIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value8',
      'value7',
      'value6',
      'value5',
      'value4',
      'value3',
      'value2',
      'value1',
    ])
  }

  await ht10.set('9', 'value9', 9)

  assert.equal(ht10._count, 9)
  assert.equal(ht10._length, 20)

  {
    const values = []
    for await (const value of ht10.forwardIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value1',
      'value2',
      'value3',
      'value4',
      'value5',
      'value6',
      'value7',
      'value8',
      'value9',
    ])
  }

  {
    const values = []
    for await (const value of ht10.reverseIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value9',
      'value8',
      'value7',
      'value6',
      'value5',
      'value4',
      'value3',
      'value2',
      'value1',
    ])
  }

  await ht10.set('10', 'value10', 10)

  assert.equal(ht10._count, 10)
  assert.equal(ht10._length, 20)

  await ht10.set('11', 'value11', 11)
  await ht10.set('12', 'value12', 12)
  await ht10.set('13', 'value13', 13)
  await ht10.set('14', 'value14', 14)
  await ht10.set('15', 'value15', 15)
  await ht10.set('16', 'value16', 16)

  assert.equal(ht10._count, 16)
  assert.equal(ht10._length, 20)

  {
    const values = []
    for await (const value of ht10.forwardIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value1',
      'value2',
      'value3',
      'value4',
      'value5',
      'value6',
      'value7',
      'value8',
      'value9',
      'value10',
      'value11',
      'value12',
      'value13',
      'value14',
      'value15',
      'value16',
    ])
  }

  {
    const values = []
    for await (const value of ht10.reverseIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value16',
      'value15',
      'value14',
      'value13',
      'value12',
      'value11',
      'value10',
      'value9',
      'value8',
      'value7',
      'value6',
      'value5',
      'value4',
      'value3',
      'value2',
      'value1',
    ])
  }

  await ht10.set('17', 'value17', 17)

  assert.equal(ht10._count, 17)
  assert.equal(ht10._length, 40)

  {
    const values = []
    for await (const value of ht10.forwardIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value1',
      'value2',
      'value3',
      'value4',
      'value5',
      'value6',
      'value7',
      'value8',
      'value9',
      'value10',
      'value11',
      'value12',
      'value13',
      'value14',
      'value15',
      'value16',
      'value17',
    ])
  }

  {
    const values = []
    for await (const value of ht10.reverseIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value17',
      'value16',
      'value15',
      'value14',
      'value13',
      'value12',
      'value11',
      'value10',
      'value9',
      'value8',
      'value7',
      'value6',
      'value5',
      'value4',
      'value3',
      'value2',
      'value1',
    ])
  }

  await ht10.set('18', 'value18', 18)
  await ht10.set('19', 'value19', 19)
  await ht10.set('20', 'value20', 20)
  await ht10.set('21', 'value21', 21)
  await ht10.set('22', 'value22', 22)
  await ht10.set('23', 'value23', 23)
  await ht10.set('24', 'value24', 24)
  await ht10.set('25', 'value25', 25)
  await ht10.set('26', 'value26', 26)
  await ht10.set('27', 'value27', 27)
  await ht10.set('28', 'value28', 28)
  await ht10.set('29', 'value29', 29)
  await ht10.set('30', 'value30', 30)
  await ht10.set('31', 'value31', 31)
  await ht10.set('32', 'value32', 32)

  assert.equal(ht10._count, 32)
  assert.equal(ht10._length, 40)

  {
    const values = []
    for await (const value of ht10.forwardIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value1',
      'value2',
      'value3',
      'value4',
      'value5',
      'value6',
      'value7',
      'value8',
      'value9',
      'value10',
      'value11',
      'value12',
      'value13',
      'value14',
      'value15',
      'value16',
      'value17',
      'value18',
      'value19',
      'value20',
      'value21',
      'value22',
      'value23',
      'value24',
      'value25',
      'value26',
      'value27',
      'value28',
      'value29',
      'value30',
      'value31',
      'value32',
    ])
  }

  {
    const values = []
    for await (const value of ht10.reverseIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value32',
      'value31',
      'value30',
      'value29',
      'value28',
      'value27',
      'value26',
      'value25',
      'value24',
      'value23',
      'value22',
      'value21',
      'value20',
      'value19',
      'value18',
      'value17',
      'value16',
      'value15',
      'value14',
      'value13',
      'value12',
      'value11',
      'value10',
      'value9',
      'value8',
      'value7',
      'value6',
      'value5',
      'value4',
      'value3',
      'value2',
      'value1',
    ])
  }

  await ht10.set('33', 'value33', 33)

  assert.equal(ht10._count, 33)
  assert.equal(ht10._length, 80)

  {
    const values = []
    for await (const value of ht10.forwardIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value1',
      'value2',
      'value3',
      'value4',
      'value5',
      'value6',
      'value7',
      'value8',
      'value9',
      'value10',
      'value11',
      'value12',
      'value13',
      'value14',
      'value15',
      'value16',
      'value17',
      'value18',
      'value19',
      'value20',
      'value21',
      'value22',
      'value23',
      'value24',
      'value25',
      'value26',
      'value27',
      'value28',
      'value29',
      'value30',
      'value31',
      'value32',
      'value33',
    ])
  }

  {
    const values = []
    for await (const value of ht10.reverseIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value33',
      'value32',
      'value31',
      'value30',
      'value29',
      'value28',
      'value27',
      'value26',
      'value25',
      'value24',
      'value23',
      'value22',
      'value21',
      'value20',
      'value19',
      'value18',
      'value17',
      'value16',
      'value15',
      'value14',
      'value13',
      'value12',
      'value11',
      'value10',
      'value9',
      'value8',
      'value7',
      'value6',
      'value5',
      'value4',
      'value3',
      'value2',
      'value1',
    ])
  }

  ht10.close()
}).case()

const test15 = new Test('DiskSortedHashTable', async function integration15() {
  const ht10 = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/10`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/10_header`,
    initialLength: 10,
    sortValueType: 'number',
    resizeRatio: 0.8,
    resizeFactor: 2,
  })
  await ht10.destroy()
  await ht10.init()

  assert.equal(ht10._count, 0)
  assert.equal(ht10._length, 10)

  await ht10.set('33', 'value33', 33)

  {
    const values = []
    for await (const value of ht10.forwardIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value33',
    ])
  }

  {
    const values = []
    for await (const value of ht10.reverseIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value33',
    ])
  }

  await ht10.set('32', 'value32', 32)
  await ht10.set('31', 'value31', 31)

  assert.equal(ht10._count, 3)
  assert.equal(ht10._length, 10)

  {
    const values = []
    for await (const value of ht10.forwardIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value31',
      'value32',
      'value33',
    ])
  }

  {
    const values = []
    for await (const value of ht10.reverseIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value33',
      'value32',
      'value31',
    ])
  }

  await ht10.set('30', 'value30', 30)
  await ht10.set('29', 'value29', 29)
  await ht10.set('28', 'value28', 28)

  assert.equal(ht10._count, 6)
  assert.equal(ht10._length, 10)

  {
    const values = []
    for await (const value of ht10.forwardIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value28',
      'value29',
      'value30',
      'value31',
      'value32',
      'value33',
    ])
  }

  {
    const values = []
    for await (const value of ht10.reverseIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value33',
      'value32',
      'value31',
      'value30',
      'value29',
      'value28',
    ])
  }

  await ht10.set('27', 'value27', 27)

  assert.equal(ht10._count, 7)
  assert.equal(ht10._length, 10)

  await ht10.set('26', 'value26', 26)

  assert.equal(ht10._count, 8)
  assert.equal(ht10._length, 10)

  await ht10.set('25', 'value25', 25)

  assert.equal(ht10._count, 9)
  assert.equal(ht10._length, 20)

  await ht10.set('24', 'value24', 24)

  assert.equal(ht10._count, 10)
  assert.equal(ht10._length, 20)

  {
    const values = []
    for await (const value of ht10.forwardIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value24',
      'value25',
      'value26',
      'value27',
      'value28',
      'value29',
      'value30',
      'value31',
      'value32',
      'value33',
    ])
  }

  {
    const values = []
    for await (const value of ht10.reverseIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value33',
      'value32',
      'value31',
      'value30',
      'value29',
      'value28',
      'value27',
      'value26',
      'value25',
      'value24',
    ])
  }

  await ht10.set('23', 'value23', 23)
  await ht10.set('22', 'value22', 22)
  await ht10.set('21', 'value21', 21)
  await ht10.set('20', 'value20', 20)
  await ht10.set('19', 'value19', 19)
  await ht10.set('18', 'value18', 18)

  assert.equal(ht10._count, 16)
  assert.equal(ht10._length, 20)

  {
    const values = []
    for await (const value of ht10.forwardIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value18',
      'value19',
      'value20',
      'value21',
      'value22',
      'value23',
      'value24',
      'value25',
      'value26',
      'value27',
      'value28',
      'value29',
      'value30',
      'value31',
      'value32',
      'value33',
    ])
  }

  {
    const values = []
    for await (const value of ht10.reverseIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value33',
      'value32',
      'value31',
      'value30',
      'value29',
      'value28',
      'value27',
      'value26',
      'value25',
      'value24',
      'value23',
      'value22',
      'value21',
      'value20',
      'value19',
      'value18',
    ])
  }

  await ht10.set('17', 'value17', 17)

  assert.equal(ht10._count, 17)
  assert.equal(ht10._length, 40)

  await ht10.set('16', 'value16', 16)
  await ht10.set('15', 'value15', 15)
  await ht10.set('14', 'value14', 14)
  await ht10.set('13', 'value13', 13)
  await ht10.set('12', 'value12', 12)
  await ht10.set('11', 'value11', 11)
  await ht10.set('10', 'value10', 10)
  await ht10.set('9', 'value9', 9)
  await ht10.set('8', 'value8', 8)
  await ht10.set('7', 'value7', 7)
  await ht10.set('6', 'value6', 6)
  await ht10.set('5', 'value5', 5)
  await ht10.set('4', 'value4', 4)
  await ht10.set('3', 'value3', 3)
  await ht10.set('2', 'value2', 2)

  assert.equal(ht10._count, 32)
  assert.equal(ht10._length, 40)

  {
    const values = []
    for await (const value of ht10.forwardIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value2',
      'value3',
      'value4',
      'value5',
      'value6',
      'value7',
      'value8',
      'value9',
      'value10',
      'value11',
      'value12',
      'value13',
      'value14',
      'value15',
      'value16',
      'value17',
      'value18',
      'value19',
      'value20',
      'value21',
      'value22',
      'value23',
      'value24',
      'value25',
      'value26',
      'value27',
      'value28',
      'value29',
      'value30',
      'value31',
      'value32',
      'value33',
    ])
  }

  {
    const values = []
    for await (const value of ht10.reverseIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value33',
      'value32',
      'value31',
      'value30',
      'value29',
      'value28',
      'value27',
      'value26',
      'value25',
      'value24',
      'value23',
      'value22',
      'value21',
      'value20',
      'value19',
      'value18',
      'value17',
      'value16',
      'value15',
      'value14',
      'value13',
      'value12',
      'value11',
      'value10',
      'value9',
      'value8',
      'value7',
      'value6',
      'value5',
      'value4',
      'value3',
      'value2',
    ])
  }

  await ht10.set('1', 'value1', 1)

  assert.equal(ht10._count, 33)
  assert.equal(ht10._length, 80)

  {
    const values = []
    for await (const value of ht10.forwardIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value1',
      'value2',
      'value3',
      'value4',
      'value5',
      'value6',
      'value7',
      'value8',
      'value9',
      'value10',
      'value11',
      'value12',
      'value13',
      'value14',
      'value15',
      'value16',
      'value17',
      'value18',
      'value19',
      'value20',
      'value21',
      'value22',
      'value23',
      'value24',
      'value25',
      'value26',
      'value27',
      'value28',
      'value29',
      'value30',
      'value31',
      'value32',
      'value33',
    ])
  }

  {
    const values = []
    for await (const value of ht10.forwardIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value1',
      'value2',
      'value3',
      'value4',
      'value5',
      'value6',
      'value7',
      'value8',
      'value9',
      'value10',
      'value11',
      'value12',
      'value13',
      'value14',
      'value15',
      'value16',
      'value17',
      'value18',
      'value19',
      'value20',
      'value21',
      'value22',
      'value23',
      'value24',
      'value25',
      'value26',
      'value27',
      'value28',
      'value29',
      'value30',
      'value31',
      'value32',
      'value33',
    ])
  }

  ht10.close()
}).case()

const test16 = new Test('DiskSortedHashTable', async function integration16() {
  const ht100 = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/100`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/100_header`,
    initialLength: 100,
    sortValueType: 'number',
    resizeRatio: 0,
  })
  await ht100.destroy()
  await ht100.init()

  await ht100.set('33', 'value33', 33)
  await ht100.set('1', 'value1', 1)
  await ht100.set('32', 'value32', 32)

  {
    const values = []
    for await (const value of ht100.forwardIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value1',
      'value32',
      'value33',
    ])
  }

  {
    const values = []
    for await (const value of ht100.reverseIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value33',
      'value32',
      'value1',
    ])
  }

  await ht100.set('2', 'value2', 2)
  await ht100.set('31', 'value31', 31)
  await ht100.set('3', 'value3', 3)
  await ht100.set('30', 'value30', 30)
  await ht100.set('4', 'value4', 4)
  await ht100.set('29', 'value29', 29)
  await ht100.set('5', 'value5', 5)

  {
    const values = []
    for await (const value of ht100.forwardIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value1',
      'value2',
      'value3',
      'value4',
      'value5',
      'value29',
      'value30',
      'value31',
      'value32',
      'value33',
    ])
  }

  {
    const values = []
    for await (const value of ht100.reverseIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value33',
      'value32',
      'value31',
      'value30',
      'value29',
      'value5',
      'value4',
      'value3',
      'value2',
      'value1',
    ])
  }

  await ht100.set('28', 'value28', 28)
  await ht100.set('6', 'value6', 6)
  await ht100.set('27', 'value27', 27)

  {
    const values = []
    for await (const value of ht100.forwardIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value1',
      'value2',
      'value3',
      'value4',
      'value5',
      'value6',
      'value27',
      'value28',
      'value29',
      'value30',
      'value31',
      'value32',
      'value33',
    ])
  }

  await ht100.set('7', 'value7', 7)
  await ht100.set('26', 'value26', 26)
  await ht100.set('8', 'value8', 8)

  {
    const values = []
    for await (const value of ht100.forwardIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value1',
      'value2',
      'value3',
      'value4',
      'value5',
      'value6',
      'value7',
      'value8',
      'value26',
      'value27',
      'value28',
      'value29',
      'value30',
      'value31',
      'value32',
      'value33',
    ])
  }

  {
    const values = []
    for await (const value of ht100.reverseIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value33',
      'value32',
      'value31',
      'value30',
      'value29',
      'value28',
      'value27',
      'value26',
      'value8',
      'value7',
      'value6',
      'value5',
      'value4',
      'value3',
      'value2',
      'value1',
    ])
  }

  await ht100.set('25', 'value25', 25)
  await ht100.set('9', 'value9', 9)
  await ht100.set('24', 'value24', 24)
  await ht100.set('10', 'value10', 10)
  await ht100.set('23', 'value23', 23)
  await ht100.set('11', 'value11', 11)
  await ht100.set('22', 'value22', 22)
  await ht100.set('12', 'value12', 12)
  await ht100.set('21', 'value21', 21)
  await ht100.set('13', 'value13', 13)
  await ht100.set('20', 'value20', 20)
  await ht100.set('14', 'value14', 14)
  await ht100.set('19', 'value19', 19)
  await ht100.set('15', 'value15', 15)
  await ht100.set('18', 'value18', 18)
  await ht100.set('16', 'value16', 16)
  await ht100.set('17', 'value17', 17)

  assert.equal(ht100._count, 33)

  {
    const values = []
    for await (const value of ht100.forwardIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value1',
      'value2',
      'value3',
      'value4',
      'value5',
      'value6',
      'value7',
      'value8',
      'value9',
      'value10',
      'value11',
      'value12',
      'value13',
      'value14',
      'value15',
      'value16',
      'value17',
      'value18',
      'value19',
      'value20',
      'value21',
      'value22',
      'value23',
      'value24',
      'value25',
      'value26',
      'value27',
      'value28',
      'value29',
      'value30',
      'value31',
      'value32',
      'value33',
    ])
  }

  {
    const values = []
    for await (const value of ht100.reverseIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value33',
      'value32',
      'value31',
      'value30',
      'value29',
      'value28',
      'value27',
      'value26',
      'value25',
      'value24',
      'value23',
      'value22',
      'value21',
      'value20',
      'value19',
      'value18',
      'value17',
      'value16',
      'value15',
      'value14',
      'value13',
      'value12',
      'value11',
      'value10',
      'value9',
      'value8',
      'value7',
      'value6',
      'value5',
      'value4',
      'value3',
      'value2',
      'value1',
    ])
  }

  ht100.close()
}).case()

const test17 = new Test('DiskSortedHashTable', async function integration17() {
  const ht = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/1024`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/1024_header`,
    initialLength: 1024,
    sortValueType: 'number',
    resizeRatio: 0,
  })
  await ht.destroy()
  await ht.init()

  const values = []
  let n = 1
  while (n < 1024) {
    await ht.set(`${n}`, `value${n}`, n)
    values.push(`value${n}`)
    n += 1
  }

  {
    const forwardValues = []
    for await (const value of ht.forwardIterator()) {
      forwardValues.push(value)
    }
    assert.deepEqual(forwardValues, values)
  }

  {
    const reverseValues = []
    for await (const value of ht.reverseIterator()) {
      reverseValues.push(value)
    }
    assert.deepEqual(reverseValues, values.reverse())
  }

  await ht.close()
}).case()

const test17_1 = new Test('DiskSortedHashTable', async function integration17_1() {
  const ht = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/1024`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/1024_header`,
    initialLength: 1024,
    sortValueType: 'number',
    resizeRatio: 0,
  })
  await ht.destroy()
  await ht.init()

  const values = []
  let n = 1023
  while (n >= 0) {
    await ht.set(`${n}`, `value${n}`, n)
    values.push(`value${n}`)
    n -= 1
  }

  {
    const forwardValues = []
    for await (const value of ht.forwardIterator()) {
      forwardValues.push(value)
    }
    assert.deepEqual(forwardValues, values.reverse())
  }

  {
    const reverseValues = []
    for await (const value of ht.reverseIterator()) {
      reverseValues.push(value)
    }
    assert.deepEqual(reverseValues, values.reverse())
  }

  await ht.close()
}).case()

const test17_2 = new Test('DiskSortedHashTable', async function integration17_2() {
  const ht = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/1024`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/1024_header`,
    initialLength: 1024,
    sortValueType: 'number',
    resizeRatio: 0,
  })
  await ht.destroy()
  await ht.init()

  const values = []
  let n1 = 512
  let n2 = 511
  while (n1 < 1024) {
    await ht.set(`${n1}`, `value${n1}`, n1)
    await ht.set(`${n2}`, `value${n2}`, n2)
    values.push(`value${n1}`)
    values.push(`value${n2}`)
    n1 += 1
    n2 -= 1
  }

  values.sort((a, b) => Number(a.replace('value', '') - Number(b.replace('value', ''))))

  {
    const forwardValues = []
    for await (const value of ht.forwardIterator()) {
      forwardValues.push(value)
    }
    assert.deepEqual(forwardValues, values)
  }

  {
    const reverseValues = []
    for await (const value of ht.reverseIterator()) {
      reverseValues.push(value)
    }
    assert.deepEqual(reverseValues, values.reverse())
  }

  await ht.close()
}).case()

const test17_3 = new Test('DiskSortedHashTable', async function integration17_3() {
  const ht = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/1024`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/1024_header`,
    initialLength: 1024,
    sortValueType: 'number',
    resizeRatio: 0,
  })
  await ht.destroy()
  await ht.init()

  const values = []
  let n1 = 1023
  let n2 = 0
  while (n1 >= 512) {
    await ht.set(`${n1}`, `value${n1}`, n1)
    await ht.set(`${n2}`, `value${n2}`, n2)
    values.push(`value${n1}`)
    values.push(`value${n2}`)
    n1 -= 1
    n2 += 1
  }

  values.sort((a, b) => Number(a.replace('value', '') - Number(b.replace('value', ''))))

  {
    const forwardValues = []
    for await (const value of ht.forwardIterator()) {
      forwardValues.push(value)
    }
    assert.deepEqual(forwardValues, values)
  }

  {
    const reverseValues = []
    for await (const value of ht.reverseIterator()) {
      reverseValues.push(value)
    }
    assert.deepEqual(reverseValues, values.reverse())
  }

  await ht.close()
}).case()

const test = Test.all([
  test1,
  test1_1,
  test1_2,
  test1_3,
  test1_4,
  test2,
  test3,
  test4,
  test5,
  test6,
  test7,
  test8,
  test9,
  test10,
  test11,
  test12,
  test13,
  test14_0,
  test14,
  test15,
  test16,
  test17,
  test17_1,
  test17_2,
  test17_3,
])

if (process.argv[1] == __filename) {
  test()
}

module.exports = test
