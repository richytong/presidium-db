const Test = require('thunk-test')
const assert = require('assert')
const DiskSortedHashTable = require('./DiskSortedHashTable')
const generateKLengthPermutations = require('./_internal/generateKLengthPermutations')
const AsyncPool = require('./_internal/AsyncPool')
const assertBalanced = require('./test/assertBalanced')
const assertMinHeight = require('./test/assertMinHeight')
const assertMaxHeight = require('./test/assertMaxHeight')
const assertMinKeysPerNode = require('./test/assertMinKeysPerNode')
const assertMaxKeysPerNode = require('./test/assertMaxKeysPerNode')
const calculateMaxBTreeHeight = require('./_internal/calculateMaxBTreeHeight')
const calculateMinBTreeHeight = require('./_internal/calculateMinBTreeHeight')

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
    const nodes = []
    const leafNodes = []
    await ht1024._constructBTree({
      unique: true,
      onNode({ node }) {
        nodes.push(node)
      },
      onLeaf({ node }) {
        leafNodes.push(node)
      },
    })

    assert.equal(leafNodes.length, 0)
    assert.equal(nodes.length, 0)
  }

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

  await assert.rejects(
    ht1024._constructBTree({
      unique: false,
      onLeaf() {},
    }),
    new Error('onLeaf option requires unique to be true')
  )

  {
    const nodes = []
    const leafNodes = []
    await ht1024._constructBTree({
      unique: true,
      onNode({ node }) {
        nodes.push(node)
      },
      onLeaf({ node }) {
        leafNodes.push(node)
      },
    })

    assert.equal(nodes.length, 1)
    assert.equal(leafNodes.length, 1)
    assert.deepEqual(nodes[0].items.map(item => item.sortValue), [1, 2, 4])
    assert.deepEqual(leafNodes[0].items.map(item => item.sortValue), [1, 2, 4])
  }

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
    const items = await ht1024._traverseInOrder()
    assert.deepEqual(items.map(item => item.sortValue), [1, 2, 4])
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
    const items = await ht1024._traverseInOrder()
    assert.deepEqual(items.map(item => item.sortValue), [2, 4])
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

  {
    const items = await ht1024._traverseInOrder()
    assert.deepEqual(items.map(item => item.sortValue), [1, 2, 4])
  }

  await ht1024.delete('maroon').then(didDelete => assert(didDelete))

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
    const items = await ht1024._traverseInOrder()
    assert.deepEqual(items.map(item => item.sortValue), [2, 4])
  }

  await ht1024.delete('yellow').then(didDelete => assert(didDelete))

  {
    const forwardValues = []
    for await (const value of ht1024.forwardIterator()) {
      forwardValues.push(value)
    }
    assert.equal(forwardValues.length, 1)
    assert.equal(forwardValues[0], '#000')
  }

  {
    const items = await ht1024._traverseInOrder()
    assert.deepEqual(items.map(item => item.sortValue), [4])
  }

  await ht1024.delete('black').then(didDelete => assert(didDelete))

  {
    const forwardValues = []
    for await (const value of ht1024.forwardIterator()) {
      forwardValues.push(value)
    }
    assert.equal(forwardValues.length, 0)
  }

  {
    const items = await ht1024._traverseInOrder()
    assert.deepEqual(items.map(item => item.sortValue), [])
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

const test14_00 = new Test('DiskSortedHashTable', async function integration14_00() {
  const ht100 = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/10`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/10_header`,
    initialLength: 100,
    sortValueType: 'number',
    resizeRatio: 0,
  })
  await ht100.destroy()
  await ht100.init()

  await ht100.set('1', 'value1', 1)
  await ht100.set('2', 'value2', 2)
  await ht100.set('3', 'value3', 3)
  await ht100.set('4', 'value4', 4)
  await ht100.set('5', 'value5', 5)
  await ht100.set('6', 'value6', 6)
  await ht100.set('7', 'value7', 7)
  await ht100.set('8', 'value8', 8)
  await ht100.set('9', 'value9', 9)
  await ht100.set('10', 'value10', 10)
  await ht100.set('11', 'value11', 11)
  await ht100.set('12', 'value12', 12)
  await ht100.set('13', 'value13', 13)
  await ht100.set('14', 'value14', 14)
  await ht100.set('15', 'value15', 15)

  await ht100._logBTree()

  ht100.close()
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

  await ht100.delete('33')

  {
    const items = await ht100._traverseInOrder()
    assert.deepEqual(items.map(item => item.sortValue), [
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
      11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
      21, 22, 23, 24, 25, 26, 27, 28, 29, 30,
      31, 32,
    ])
  }

  await ht100.delete('31')

  {
    const items = await ht100._traverseInOrder()
    assert.deepEqual(items.map(item => item.sortValue), [
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
      11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
      21, 22, 23, 24, 25, 26, 27, 28, 29, 30,
      32,
    ])
  }

  await ht100.delete('32')

  {
    const items = await ht100._traverseInOrder()
    assert.deepEqual(items.map(item => item.sortValue), [
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
      11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
      21, 22, 23, 24, 25, 26, 27, 28, 29, 30,
    ])
  }


  await ht100.delete('1')

  {
    const items = await ht100._traverseInOrder()
    assert.deepEqual(items.map(item => item.sortValue), [
      2, 3, 4, 5, 6, 7, 8, 9, 10,
      11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
      21, 22, 23, 24, 25, 26, 27, 28, 29, 30,
    ])
  }

  await ht100.delete('29')

  {
    const items = await ht100._traverseInOrder()
    assert.deepEqual(items.map(item => item.sortValue), [
      2, 3, 4, 5, 6, 7, 8, 9, 10,
      11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
      21, 22, 23, 24, 25, 26, 27, 28, 30,
    ])
  }

  await ht100.delete('30')

  {
    const items = await ht100._traverseInOrder()
    assert.deepEqual(items.map(item => item.sortValue), [
      2, 3, 4, 5, 6, 7, 8, 9, 10,
      11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
      21, 22, 23, 24, 25, 26, 27, 28,
    ])
  }

  await ht100.delete('27')

  {
    const items = await ht100._traverseInOrder()
    assert.deepEqual(items.map(item => item.sortValue), [
      2, 3, 4, 5, 6, 7, 8, 9, 10,
      11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
      21, 22, 23, 24, 25, 26, 28,
    ])
  }

  await ht100.delete('28')

  {
    const items = await ht100._traverseInOrder()
    assert.deepEqual(items.map(item => item.sortValue), [
      2, 3, 4, 5, 6, 7, 8, 9, 10,
      11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
      21, 22, 23, 24, 25, 26,
    ])
  }

  await ht100.delete('5')

  {
    const items = await ht100._traverseInOrder()
    assert.deepEqual(items.map(item => item.sortValue), [
      2, 3, 4, 6, 7, 8, 9, 10,
      11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
      21, 22, 23, 24, 25, 26,
    ])
  }

  await ht100.delete('4')

  {
    const items = await ht100._traverseInOrder()
    assert.deepEqual(items.map(item => item.sortValue), [
      2, 3, 6, 7, 8, 9, 10,
      11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
      21, 22, 23, 24, 25, 26,
    ])
  }

  await ht100.delete('23')

  {
    const items = await ht100._traverseInOrder()
    assert.deepEqual(items.map(item => item.sortValue), [
      2, 3, 6, 7, 8, 9, 10,
      11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
      21, 22, 24, 25, 26,
    ])
  }

  await ht100.delete('23').then(didDelete => assert(!didDelete))

  await ht100.delete('13')

  {
    const items = await ht100._traverseInOrder()
    assert.deepEqual(items.map(item => item.sortValue), [
      2, 3, 6, 7, 8, 9, 10,
      11, 12, 14, 15, 16, 17, 18, 19, 20,
      21, 22, 24, 25, 26,
    ])
  }

  await ht100.delete('3')

  {
    const items = await ht100._traverseInOrder()
    assert.deepEqual(items.map(item => item.sortValue), [
      2, 6, 7, 8, 9, 10,
      11, 12, 14, 15, 16, 17, 18, 19, 20,
      21, 22, 24, 25, 26,
    ])
  }

  await ht100.delete('2')

  {
    const items = await ht100._traverseInOrder()
    assert.deepEqual(items.map(item => item.sortValue), [
      6, 7, 8, 9, 10,
      11, 12, 14, 15, 16, 17, 18, 19, 20,
      21, 22, 24, 25, 26,
    ])
  }

  await ht100.delete('6')

  {
    const items = await ht100._traverseInOrder()
    assert.deepEqual(items.map(item => item.sortValue), [
      7, 8, 9, 10,
      11, 12, 14, 15, 16, 17, 18, 19, 20,
      21, 22, 24, 25, 26,
    ])
  }

  await ht100.delete('7')

  {
    const items = await ht100._traverseInOrder()
    assert.deepEqual(items.map(item => item.sortValue), [
      8, 9, 10,
      11, 12, 14, 15, 16, 17, 18, 19, 20,
      21, 22, 24, 25, 26,
    ])
  }

  await ht100.delete('17')

  {
    const items = await ht100._traverseInOrder()
    assert.deepEqual(items.map(item => item.sortValue), [
      8, 9, 10,
      11, 12, 14, 15, 16, 18, 19, 20,
      21, 22, 24, 25, 26,
    ])
  }

  await ht100.delete('12')

  {
    const items = await ht100._traverseInOrder()
    assert.deepEqual(items.map(item => item.sortValue), [
      8, 9, 10,
      11, 14, 15, 16, 18, 19, 20,
      21, 22, 24, 25, 26,
    ])
  }

  await ht100._logBTree()
  await ht100.delete('20')
  await ht100._logBTree()
  process.exit()

  {
    const items = await ht100._traverseInOrder()
    assert.deepEqual(items.map(item => item.sortValue), [
      8, 9, 10,
      11, 14, 15, 16, 18, 19,
      21, 22, 24, 25, 26,
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

const test17_0 = new Test('DiskSortedHashTable', async function integration17_0() {
  const ht = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/10`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/10_header`,
    initialLength: 100,
    sortValueType: 'number',
    resizeRatio: 0,
  })
  await ht.destroy()
  await ht.init()

  async function logForwardValues() {
    const values = []
    for await (const value of ht.forwardIterator()) {
      values.push(value)
    }
    console.log(values)
  }

  await ht.set('1', 'value1', 1)

  {
    const values = []
    for await (const value of ht.forwardIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value1',
    ])
  }

  {
    const values = []
    for await (const value of ht.reverseIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value1',
    ])
  }

  await ht.set('2', 'value2', 2)
  await ht.set('3', 'value3', 3)

  {
    const values = []
    for await (const value of ht.forwardIterator()) {
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
    for await (const value of ht.reverseIterator()) {
      values.push(value)
    }
    assert.deepEqual(values, [
      'value3',
      'value2',
      'value1',
    ])
  }

  {
    const nodes = []
    const leafNodes = []

    await ht._constructBTree({
      unique: true,
      onNode({ node }) {
        nodes.push(node)
      },
      onLeaf({ node }) {
        leafNodes.push(node)
      },
    })

    assert.equal(nodes.length, 1)
    assert.equal(leafNodes.length, 1)
    assert.deepEqual(nodes[0].items.map(item => item.sortValue), [1, 2, 3])
    assert.deepEqual(leafNodes[0].items.map(item => item.sortValue), [1, 2, 3])
  }

  await ht.set('4', 'value4', 4)

  {
    const nodes = []
    const leafNodes = []

    await ht._constructBTree({
      unique: true,
      onNode({ node }) {
        nodes.push(node)
      },
      onLeaf({ node }) {
        leafNodes.push(node)
      },
    })

    assert.equal(nodes.length, 3)
    assert.equal(leafNodes.length, 2)
    assert.deepEqual(nodes[0].items.map(item => item.sortValue), [2])
    assert.deepEqual(nodes[1].items.map(item => item.sortValue), [1])
    assert.deepEqual(nodes[2].items.map(item => item.sortValue), [3, 4])
    assert.deepEqual(leafNodes[0].items.map(item => item.sortValue), [1])
    assert.deepEqual(leafNodes[1].items.map(item => item.sortValue), [3, 4])

    await assertBalanced(ht)
    await assertMinHeight(ht, 0)
    await assertMaxHeight(ht, 1)
    await assertMinKeysPerNode(ht, 1)
    await assertMaxKeysPerNode(ht, 2)

    await assertMinHeight(ht, 1)
    await assert.rejects(
      assertMinHeight(ht, 2),
      new Error('b-tree under min height (1 / 2)')
    )

    await assert.rejects(
      assertMaxHeight(ht, 0),
      new Error('b-tree over max height (1 / 0)')
    )

    await assert.rejects(
      assertMaxKeysPerNode(ht, 1),
      new Error('b-tree node over maximum number of keys per node (2 / 1)')
    )

    await assert.rejects(
      assertMinKeysPerNode(ht, 2),
      new Error('b-tree node under minimum number of keys per node (1 / 2)')
    )

    await assert.rejects(
      assertMaxKeysPerNode(ht, 1),
      new Error('b-tree node over maximum number of keys per node (2 / 1)')
    )
  }

  await ht.set('4', 'value4', 4)
  await ht.set('5', 'value5', 5)
  await ht.set('6', 'value6', 6)
  await ht.set('7', 'value7', 7)
  await ht.set('8', 'value8', 8)
  await ht.set('9', 'value9', 9)
  await ht.set('10', 'value10', 10)
  await ht.set('11', 'value11', 11)
  await ht.set('12', 'value12', 12)

  
  assert.deepEqual(
    JSON.stringify(await ht._constructBTree({ unique: false }), (key, value) => {
      if (key == 'items' || key == 'keys') {
        return undefined
      }
      return value
    }),
    JSON.stringify({
      "5": {
        "leftChild": {
          "2": {
            "leftChild": {
              "1": {}
            },
            "rightChild": {
              "4": {}
            }
          }
        },
        "rightChild": {
          "7": {
            "leftChild": {
              "6": {}
            },
            "rightChild": {
              "8": {}
            }
          },
          "9": {
            "leftChild": {
              "8": {}
            },
            "rightChild": {
              "10": {},
              "11": {},
              "12": {}
            }
          }
        }
      },
      "root": true,
      "totalItemsCount": 4,
      "totalKeys": [
        "1",
        "2",
        "4",
        "5",
        "6",
        "7",
        "8",
        "8",
        "9",
        "10",
        "11",
        "12"
      ]
    })
  )

  await assertBalanced(ht)
  await assertMinHeight(ht, 2)

  const indexOf2 = 50

  await ht._writeBTreeLeftChildNodeRightmostItemIndex(indexOf2, -1)
  await ht._writeBTreeRightChildNodeRightmostItemIndex(indexOf2, -1)

  assert.deepEqual(
    JSON.stringify(await ht._constructBTree({ unique: false }), (key, value) => {
      if (key == 'items' || key == 'keys') {
        return undefined
      }
      return value
    }),
    JSON.stringify({
      "5": {
        "leftChild": {
          "2": {}
        },
        "rightChild": {
          "7": {
            "leftChild": {
              "6": {}
            },
            "rightChild": {
              "8": {}
            }
          },
          "9": {
            "leftChild": {
              "8": {}
            },
            "rightChild": {
              "10": {},
              "11": {},
              "12": {}
            }
          }
        }
      },
      "root": true,
      "totalItemsCount": 4,
      "totalKeys": [
        "2",
        "5",
        "6",
        "7",
        "8",
        "8",
        "9",
        "10",
        "11",
        "12"
      ]
    })
  )

  await assert.rejects(
    assertBalanced(ht),
    new Error('b-tree not balanced')
  )

  await assert.rejects(
    assertMinHeight(ht, 2),
    new Error('b-tree under min height (1 / 2)')
  )

  ht.close()
}).case()

const test17 = new Test('DiskSortedHashTable', async function integration17() {
  const sortedNumbers = [...require('./test/randomNumbers1023_1.json')].sort((a, b) => a - b)
  assert.equal(sortedNumbers.length, 1023)

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
    console.log('set', `${n}`, `value${n}`, n)
    await ht.set(`${n}`, `value${n}`, n)
    values.push(`value${n}`)
    n += 1
  }

  await ht._logBTree()
  process.exit()

  await assertBalanced(ht)
  await assertMinHeight(ht, calculateMinBTreeHeight(1023, 2))
  await assertMaxHeight(ht, calculateMaxBTreeHeight(1023, 2))
  await assertMinKeysPerNode(ht, 1)
  await assertMaxKeysPerNode(ht, 3)

  assert.deepEqual(values, sortedNumbers.map(n => `value${n}`))

  {
    const forwardValues = []
    for await (const value of ht.forwardIterator()) {
      forwardValues.push(value)
    }
    assert.deepEqual(forwardValues, values)
  }

  {
    const items = await ht._traverseInOrder()
    assert.deepEqual(items.map(item => Number(item.sortValue)), sortedNumbers)
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
  const sortedNumbers = [...require('./test/randomNumbers1023_1.json')].sort((a, b) => a - b)
  assert.equal(sortedNumbers.length, 1023)

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
  while (n > 0) {
    console.log('set', `${n}`, `value${n}`, n)
    await ht.set(`${n}`, `value${n}`, n)
    values.push(`value${n}`)
    n -= 1
  }

  await assertBalanced(ht)
  await assertMinHeight(ht, calculateMinBTreeHeight(1023, 2))
  await assertMaxHeight(ht, calculateMaxBTreeHeight(1023, 2))
  await assertMinKeysPerNode(ht, 1)
  await assertMaxKeysPerNode(ht, 3)

  values.reverse()
  assert.deepEqual(values, sortedNumbers.map(n => `value${n}`))

  {
    const forwardValues = []
    for await (const value of ht.forwardIterator()) {
      forwardValues.push(value)
    }
    assert.deepEqual(forwardValues, values)
  }

  {
    const items = await ht._traverseInOrder()
    assert.deepEqual(items.map(item => Number(item.sortValue)), sortedNumbers)
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
  const sortedNumbers = [...require('./test/randomNumbers1023_1.json')].sort((a, b) => a - b)
  assert.equal(sortedNumbers.length, 1023)

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
    if (n1 == 1023) {
      console.log('set', `${n1}`, `value${n1}`, n1)
      await ht.set(`${n1}`, `value${n1}`, n1)
      values.push(`value${n1}`)
    } else {
      console.log('set', `${n1}`, `value${n1}`, n1)
      await ht.set(`${n1}`, `value${n1}`, n1)
      console.log('set', `${n2}`, `value${n2}`, n2)
      await ht.set(`${n2}`, `value${n2}`, n2)
      values.push(`value${n1}`)
      values.push(`value${n2}`)
    }
    n1 += 1
    n2 -= 1
  }

  await assertBalanced(ht)
  await assertMinHeight(ht, calculateMinBTreeHeight(1023, 2))
  await assertMaxHeight(ht, calculateMaxBTreeHeight(1023, 2))
  await assertMinKeysPerNode(ht, 1)
  await assertMaxKeysPerNode(ht, 3)

  values.sort((a, b) => Number(a.replace('value', '') - Number(b.replace('value', ''))))
  assert.deepEqual(values, sortedNumbers.map(n => `value${n}`))

  {
    const forwardValues = []
    for await (const value of ht.forwardIterator()) {
      forwardValues.push(value)
    }
    assert.deepEqual(forwardValues, values)
  }

  {
    const items = await ht._traverseInOrder()
    assert.deepEqual(items.map(item => Number(item.sortValue)), sortedNumbers)
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
  const sortedNumbers = [...require('./test/randomNumbers1023_1.json')].sort((a, b) => a - b)
  assert.equal(sortedNumbers.length, 1023)

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
  let n2 = 1
  while (n1 >= 512) {
    if (n1 == 512) {
      console.log('set', `${n1}`, `value${n1}`, n1)
      await ht.set(`${n1}`, `value${n1}`, n1)
      values.push(`value${n1}`)
    } else {
      console.log('set', `${n1}`, `value${n1}`, n1)
      await ht.set(`${n1}`, `value${n1}`, n1)
      console.log('set', `${n2}`, `value${n2}`, n2)
      await ht.set(`${n2}`, `value${n2}`, n2)
      values.push(`value${n1}`)
      values.push(`value${n2}`)
    }
    n1 -= 1
    n2 += 1
  }

  await assertBalanced(ht)
  await assertMinHeight(ht, calculateMinBTreeHeight(1023, 2))
  await assertMaxHeight(ht, calculateMaxBTreeHeight(1023, 2))
  await assertMinKeysPerNode(ht, 1)
  await assertMaxKeysPerNode(ht, 3)

  values.sort((a, b) => Number(a.replace('value', '') - Number(b.replace('value', ''))))
  assert.deepEqual(values, sortedNumbers.map(n => `value${n}`))

  {
    const forwardValues = []
    for await (const value of ht.forwardIterator()) {
      forwardValues.push(value)
    }
    assert.deepEqual(forwardValues, values)
  }

  {
    const items = await ht._traverseInOrder()
    assert.deepEqual(items.map(item => Number(item.sortValue)), sortedNumbers)
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

const test17_4 = new Test('DiskSortedHashTable', async function integration17_4() {
  const sortedNumbers = [...require('./test/randomNumbers1023_1.json')].sort((a, b) => a - b)
  assert.equal(sortedNumbers.length, 1023)

  const ht = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/1024`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/1024_header`,
    initialLength: 1024,
    sortValueType: 'number',
    resizeRatio: 0,
    degree: 3,
  })
  await ht.destroy()
  await ht.init()

  const values = []
  let n = 1
  while (n < 1024) {
    console.log('set', `${n}`, `value${n}`, n)
    await ht.set(`${n}`, `value${n}`, n)
    values.push(`value${n}`)
    n += 1
  }

  await assertBalanced(ht)
  await assertMinHeight(ht, calculateMinBTreeHeight(1023, 3))
  await assertMaxHeight(ht, calculateMaxBTreeHeight(1023, 3))
  await assertMinKeysPerNode(ht, 2)
  await assertMaxKeysPerNode(ht, 5)

  assert.deepEqual(values, sortedNumbers.map(n => `value${n}`))

  {
    const forwardValues = []
    for await (const value of ht.forwardIterator()) {
      forwardValues.push(value)
    }
    assert.deepEqual(forwardValues, values)
  }

  {
    const items = await ht._traverseInOrder()
    assert.deepEqual(items.map(item => Number(item.sortValue)), sortedNumbers)
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

const test17_5 = new Test('DiskSortedHashTable', async function integration17_5() {
  const sortedNumbers = [...require('./test/randomNumbers1023_1.json')].sort((a, b) => a - b)
  assert.equal(sortedNumbers.length, 1023)

  const ht = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/1024`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/1024_header`,
    initialLength: 1024,
    sortValueType: 'number',
    resizeRatio: 0,
    degree: 3,
  })
  await ht.destroy()
  await ht.init()

  const values = []
  let n = 1023
  while (n > 0) {
    console.log('set', `${n}`, `value${n}`, n)
    await ht.set(`${n}`, `value${n}`, n)
    values.push(`value${n}`)
    n -= 1
  }

  await assertBalanced(ht)
  await assertMinHeight(ht, calculateMinBTreeHeight(1023, 3))
  await assertMaxHeight(ht, calculateMaxBTreeHeight(1023, 3))
  await assertMinKeysPerNode(ht, 2)
  await assertMaxKeysPerNode(ht, 5)

  values.reverse()
  assert.deepEqual(values, sortedNumbers.map(n => `value${n}`))

  {
    const forwardValues = []
    for await (const value of ht.forwardIterator()) {
      forwardValues.push(value)
    }
    assert.deepEqual(forwardValues, values)
  }

  {
    const items = await ht._traverseInOrder()
    assert.deepEqual(items.map(item => Number(item.sortValue)), sortedNumbers)
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

const test17_6 = new Test('DiskSortedHashTable', async function integration17_6() {
  const sortedNumbers = [...require('./test/randomNumbers1023_1.json')].sort((a, b) => a - b)
  assert.equal(sortedNumbers.length, 1023)

  const ht = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/1024`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/1024_header`,
    initialLength: 1024,
    sortValueType: 'number',
    resizeRatio: 0,
    degree: 3,
  })
  await ht.destroy()
  await ht.init()

  const values = []
  let n1 = 512
  let n2 = 511
  while (n1 < 1024) {
    if (n1 == 1023) {
      console.log('set', `${n1}`, `value${n1}`, n1)
      await ht.set(`${n1}`, `value${n1}`, n1)
      values.push(`value${n1}`)
    } else {
      console.log('set', `${n1}`, `value${n1}`, n1)
      await ht.set(`${n1}`, `value${n1}`, n1)
      console.log('set', `${n2}`, `value${n2}`, n2)
      await ht.set(`${n2}`, `value${n2}`, n2)
      values.push(`value${n1}`)
      values.push(`value${n2}`)
    }
    n1 += 1
    n2 -= 1
  }

  await assertBalanced(ht)
  await assertMinHeight(ht, calculateMinBTreeHeight(1023, 3))
  await assertMaxHeight(ht, calculateMaxBTreeHeight(1023, 3))
  await assertMinKeysPerNode(ht, 2)
  await assertMaxKeysPerNode(ht, 5)

  values.sort((a, b) => Number(a.replace('value', '') - Number(b.replace('value', ''))))
  assert.deepEqual(values, sortedNumbers.map(n => `value${n}`))

  {
    const forwardValues = []
    for await (const value of ht.forwardIterator()) {
      forwardValues.push(value)
    }
    assert.deepEqual(forwardValues, values)
  }

  {
    const items = await ht._traverseInOrder()
    assert.deepEqual(items.map(item => Number(item.sortValue)), sortedNumbers)
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

const test17_7_0 = new Test('DiskSortedHashTable', async function integration17_7_0() {
  const sortedNumbers = [...require('./test/randomNumbers127_1.json')].sort((a, b) => a - b)
  assert.equal(sortedNumbers.length, 127)

  const ht = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/256`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/256_header`,
    initialLength: 256,
    sortValueType: 'number',
    resizeRatio: 0,
    degree: 3,
  })
  await ht.destroy()
  await ht.init()

  const values = []
  let n1 = 127
  let n2 = 1
  while (n1 >= 64) {

    if (n1 == 64) {
      console.log('set', `${n1}`, `value${n1}`, n1)
      await ht.set(`${n1}`, `value${n1}`, n1)
      values.push(`value${n1}`)
    } else {
      console.log('set', `${n1}`, `value${n1}`, n1)
      await ht.set(`${n1}`, `value${n1}`, n1)
      console.log('set', `${n2}`, `value${n2}`, n2)
      await ht.set(`${n2}`, `value${n2}`, n2)
      values.push(`value${n1}`)
      values.push(`value${n2}`)
    }
    n1 -= 1
    n2 += 1
  }

  await assertBalanced(ht)
  await assertMinHeight(ht, calculateMinBTreeHeight(127, 3))
  await assertMaxHeight(ht, calculateMaxBTreeHeight(127, 3))
  await assertMinKeysPerNode(ht, 2)
  await assertMaxKeysPerNode(ht, 5)

  values.sort((a, b) => Number(a.replace('value', '') - Number(b.replace('value', ''))))
  assert.deepEqual(values, sortedNumbers.map(n => `value${n}`))

  {
    const forwardValues = []
    for await (const value of ht.forwardIterator()) {
      forwardValues.push(value)
    }
    assert.deepEqual(forwardValues, values)
  }

  {
    const items = await ht._traverseInOrder()
    assert.deepEqual(items.map(item => Number(item.sortValue)), sortedNumbers)
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

const test17_7 = new Test('DiskSortedHashTable', async function integration17_7() {
  const sortedNumbers = [...require('./test/randomNumbers1023_1.json')].sort((a, b) => a - b)
  assert.equal(sortedNumbers.length, 1023)

  const ht = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/1024`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/1024_header`,
    initialLength: 1024,
    sortValueType: 'number',
    resizeRatio: 0,
    degree: 3,
  })
  await ht.destroy()
  await ht.init()

  const values = []
  let n1 = 1023
  let n2 = 1

  while (n1 >= 512) {
    if (n1 == 512) {
      console.log('set', `${n1}`, `value${n1}`, n1)
      await ht.set(`${n1}`, `value${n1}`, n1)
      values.push(`value${n1}`)
    } else {

      console.log('set', `${n1}`, `value${n1}`, n1)
      await ht.set(`${n1}`, `value${n1}`, n1)
      console.log('set', `${n2}`, `value${n2}`, n2)
      await ht.set(`${n2}`, `value${n2}`, n2)
      values.push(`value${n1}`)
      values.push(`value${n2}`)
    }

    n1 -= 1
    n2 += 1
  }

  await assertBalanced(ht)
  await assertMinHeight(ht, calculateMinBTreeHeight(1023, 3))
  await assertMaxHeight(ht, calculateMaxBTreeHeight(1023, 3))
  await assertMinKeysPerNode(ht, 2)
  await assertMaxKeysPerNode(ht, 5)

  values.sort((a, b) => Number(a.replace('value', '') - Number(b.replace('value', ''))))
  assert.deepEqual(values, sortedNumbers.map(n => `value${n}`))

  {
    const forwardValues = []
    for await (const value of ht.forwardIterator()) {
      forwardValues.push(value)
    }
    assert.deepEqual(forwardValues, values)
  }

  {
    const items = await ht._traverseInOrder()
    assert.deepEqual(items.map(item => Number(item.sortValue)), sortedNumbers)
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

const test18 = new Test('DiskSortedHashTable', async function integration18() {
  const numbers = [...require('./test/randomNumbers127_1.json')]
  assert.equal(numbers.length, 127)
  const sortedNumbers = [...numbers].sort((a, b) => a - b)
  const sortedNumbersReverse = [...sortedNumbers].reverse()
  const sortedValues = sortedNumbers.map(n => `value${n}`)
  const sortedValuesReverse = sortedNumbersReverse.map(n => `value${n}`)

  const ht = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/256`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/256_header`,
    initialLength: 256,
    sortValueType: 'number',
    resizeRatio: 0,
    degree: 2,
  })
  await ht.destroy()
  await ht.init()

  for (const numbers of [
    require('./test/randomNumbers127_1.json'),
    require('./test/randomNumbers127_2.json'),
    require('./test/randomNumbers127_3.json'),
    require('./test/randomNumbers127_4.json'),
    require('./test/randomNumbers127_5.json'),
    require('./test/randomNumbers127_6.json'),
    require('./test/randomNumbers127_7.json'),
    require('./test/randomNumbers127_8.json'),
    require('./test/randomNumbers127_9.json'),
    require('./test/randomNumbers127_10.json'),
    require('./test/randomNumbers127_11.json'),
    require('./test/randomNumbers127_12.json'),
    require('./test/randomNumbers127_13.json'),
    require('./test/randomNumbers127_14.json'),
    require('./test/randomNumbers127_15.json'),
    require('./test/randomNumbers127_16.json'),
    require('./test/randomNumbers127_17.json'),
    require('./test/randomNumbers127_18.json'),
    require('./test/randomNumbers127_19.json'),
    require('./test/randomNumbers127_20.json'),
  ]) {
    console.log(JSON.stringify(numbers))

    for (const n of numbers) {
      await ht.set(`key${n}`, `value${n}`, n)
    }

    await assertBalanced(ht)
    await assertMinHeight(ht, calculateMinBTreeHeight(127, 2))
    await assertMaxHeight(ht, calculateMaxBTreeHeight(127, 2))
    await assertMinKeysPerNode(ht, 1)
    await assertMaxKeysPerNode(ht, 3)

    {
      const forwardValues = []
      for await (const value of ht.forwardIterator()) {
        forwardValues.push(value)
      }
      assert.deepEqual(forwardValues, sortedValues)
    }

    {
      const items = await ht._traverseInOrder()
      assert.deepEqual(items.map(item => Number(item.sortValue)), sortedNumbers)
    }

    {
      const reverseValues = []
      for await (const value of ht.reverseIterator()) {
        reverseValues.push(value)
      }
      assert.deepEqual(reverseValues, sortedValuesReverse)
    }

    await ht.clear()
  }


  await ht.close()
  await ht.destroy()

}).case()

const test19 = new Test('DiskSortedHashTable', async function integration19() {
  const numbers = require('./test/randomNumbers127_1.json')
  assert.equal(numbers.length, 127)
  const sortedNumbers = [...numbers].sort((a, b) => a - b)
  const sortedNumbersReverse = [...sortedNumbers].reverse()
  const sortedValues = sortedNumbers.map(n => `value${n}`)
  const sortedValuesReverse = sortedNumbersReverse.map(n => `value${n}`)

  const ht = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/256`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/256_header`,
    initialLength: 256,
    sortValueType: 'number',
    resizeRatio: 0,
    degree: 3,
  })
  await ht.destroy()
  await ht.init()

  for (const numbers of [
    require('./test/randomNumbers127_1.json'),
    require('./test/randomNumbers127_2.json'),
    require('./test/randomNumbers127_3.json'),
    require('./test/randomNumbers127_4.json'),
    require('./test/randomNumbers127_5.json'),
    require('./test/randomNumbers127_6.json'),
    require('./test/randomNumbers127_7.json'),
    require('./test/randomNumbers127_8.json'),
    require('./test/randomNumbers127_9.json'),
    require('./test/randomNumbers127_10.json'),
    require('./test/randomNumbers127_11.json'),
    require('./test/randomNumbers127_12.json'),
    require('./test/randomNumbers127_13.json'),
    require('./test/randomNumbers127_14.json'),
    require('./test/randomNumbers127_15.json'),
    require('./test/randomNumbers127_16.json'),
    require('./test/randomNumbers127_17.json'),
    require('./test/randomNumbers127_18.json'),
    require('./test/randomNumbers127_19.json'),
    require('./test/randomNumbers127_20.json'),
  ]) {
    console.log(JSON.stringify(numbers))

    for (const n of numbers) {
      await ht.set(`key${n}`, `value${n}`, n)
    }

    await assertBalanced(ht)
    await assertMinHeight(ht, calculateMinBTreeHeight(127, 3))
    await assertMaxHeight(ht, calculateMaxBTreeHeight(127, 3))
    await assertMinKeysPerNode(ht, 2)
    await assertMaxKeysPerNode(ht, 5)

    {
      const forwardValues = []
      for await (const value of ht.forwardIterator()) {
        forwardValues.push(value)
      }
      assert.deepEqual(forwardValues, sortedValues)
    }

    {
      const items = await ht._traverseInOrder()
      assert.deepEqual(items.map(item => Number(item.sortValue)), sortedNumbers)
    }

    {
      const reverseValues = []
      for await (const value of ht.reverseIterator()) {
        reverseValues.push(value)
      }
      assert.deepEqual(reverseValues, sortedValuesReverse)
    }

    await ht.clear()
  }


  await ht.close()
  await ht.destroy()

}).case()

const test20 = new Test('DiskSortedHashTable', async function integration20() {
  const numbers = [...require('./test/randomNumbers1023_1.json')]
  assert.equal(numbers.length, 1023)
  const sortedNumbers = [...numbers].sort((a, b) => a - b)
  const sortedNumbersReverse = [...sortedNumbers].reverse()
  const sortedValues = sortedNumbers.map(n => `value${n}`)
  const sortedValuesReverse = sortedNumbersReverse.map(n => `value${n}`)

  const ht = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/2048`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/2048_header`,
    initialLength: 2048,
    sortValueType: 'number',
    resizeRatio: 0,
    degree: 2,
  })
  await ht.destroy()
  await ht.init()

  const numbersArray = []
  let i = 1
  while (i <= 100) {
    const numbers = require(`./test/randomNumbers1023_${i}.json`)
    numbersArray.push(numbers)
    i += 1
  }

  for (const numbers of numbersArray) {
    console.log(JSON.stringify(numbers))

    for (const n of numbers) {
      await ht.set(`key${n}`, `value${n}`, n)
    }

    await assertBalanced(ht)
    await assertMinHeight(ht, calculateMinBTreeHeight(1023, 2))
    await assertMaxHeight(ht, calculateMaxBTreeHeight(1023, 2))
    await assertMinKeysPerNode(ht, 1)
    await assertMaxKeysPerNode(ht, 3)

    {
      const forwardValues = []
      for await (const value of ht.forwardIterator()) {
        forwardValues.push(value)
      }
      assert.deepEqual(forwardValues, sortedValues)
    }

    {
      const items = await ht._traverseInOrder()
      assert.deepEqual(items.map(item => Number(item.sortValue)), sortedNumbers)
    }

    {
      const reverseValues = []
      for await (const value of ht.reverseIterator()) {
        reverseValues.push(value)
      }
      assert.deepEqual(reverseValues, sortedValuesReverse)
    }

    await ht.clear()
  }


  await ht.close()
  await ht.destroy()

}).case()

const test21 = new Test('DiskSortedHashTable', async function integration21() {
  const numbers = [...require('./test/randomNumbers1023_1.json')]
  assert.equal(numbers.length, 1023)
  const sortedNumbers = [...numbers].sort((a, b) => a - b)
  const sortedNumbersReverse = [...sortedNumbers].reverse()
  const sortedValues = sortedNumbers.map(n => `value${n}`)
  const sortedValuesReverse = sortedNumbersReverse.map(n => `value${n}`)

  const ht = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/2048`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/2048_header`,
    initialLength: 2048,
    sortValueType: 'number',
    resizeRatio: 0,
    degree: 3,
  })
  await ht.destroy()
  await ht.init()

  const numbersArray = []
  let i = 1
  while (i <= 100) {
    const numbers = require(`./test/randomNumbers1023_${i}.json`)
    numbersArray.push(numbers)
    i += 1
  }

  for (const numbers of numbersArray) {
    console.log(JSON.stringify(numbers))

    for (const n of numbers) {
      await ht.set(`key${n}`, `value${n}`, n)
    }

    await assertBalanced(ht)
    await assertMinHeight(ht, calculateMinBTreeHeight(1023, 3))
    await assertMaxHeight(ht, calculateMaxBTreeHeight(1023, 3))
    await assertMinKeysPerNode(ht, 2)
    await assertMaxKeysPerNode(ht, 5)

    {
      const forwardValues = []
      for await (const value of ht.forwardIterator()) {
        forwardValues.push(value)
      }
      assert.deepEqual(forwardValues, sortedValues)
    }

    {
      const items = await ht._traverseInOrder()
      assert.deepEqual(items.map(item => Number(item.sortValue)), sortedNumbers)
    }

    {
      const reverseValues = []
      for await (const value of ht.reverseIterator()) {
        reverseValues.push(value)
      }
      assert.deepEqual(reverseValues, sortedValuesReverse)
    }

    await ht.clear()
  }


  await ht.close()
  await ht.destroy()

}).case()

const test22 = new Test('DiskSortedHashTable', async function integration22() {
  const numbers = [...require('./test/randomNumbers1023_1.json')]
  assert.equal(numbers.length, 1023)
  const sortedNumbers = [...numbers].sort((a, b) => a - b)
  const sortedNumbersReverse = [...sortedNumbers].reverse()
  const sortedValues = sortedNumbers.map(n => `value${n}`)
  const sortedValuesReverse = sortedNumbersReverse.map(n => `value${n}`)

  const ht = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/2048`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/2048_header`,
    initialLength: 2048,
    sortValueType: 'number',
    resizeRatio: 0,
    degree: 4,
  })
  await ht.destroy()
  await ht.init()

  const numbersArray = []
  let i = 1
  while (i <= 100) {
    const numbers = require(`./test/randomNumbers1023_${i}.json`)
    numbersArray.push(numbers)
    i += 1
  }

  for (const numbers of numbersArray) {
    console.log(JSON.stringify(numbers))

    for (const n of numbers) {
      await ht.set(`key${n}`, `value${n}`, n)
    }

    await assertBalanced(ht)
    await assertMinHeight(ht, calculateMinBTreeHeight(1023, 4))
    await assertMaxHeight(ht, calculateMaxBTreeHeight(1023, 4))
    await assertMinKeysPerNode(ht, 3)
    await assertMaxKeysPerNode(ht, 7)

    {
      const forwardValues = []
      for await (const value of ht.forwardIterator()) {
        forwardValues.push(value)
      }
      assert.deepEqual(forwardValues, sortedValues)
    }

    {
      const items = await ht._traverseInOrder()
      assert.deepEqual(items.map(item => Number(item.sortValue)), sortedNumbers)
    }

    {
      const reverseValues = []
      for await (const value of ht.reverseIterator()) {
        reverseValues.push(value)
      }
      assert.deepEqual(reverseValues, sortedValuesReverse)
    }

    await ht.clear()
  }


  await ht.close()
  await ht.destroy()

}).case()

const test23_0 = new Test('DiskSortedHashTable', async function integration23_0() {

  const ht = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/2048`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/2048_header`,
    initialLength: 2048,
    sortValueType: 'number',
    resizeRatio: 0,
    degree: 5,
  })
  await ht.destroy()
  await ht.init()

  // ./test/randomNumbers1023_1.json has a root node with only 3 children,
  // which falls below the minimum child requirement per node of 4
  const numbers = [...require('./test/randomNumbers1023_1.json')]
  assert.equal(numbers.length, 1023)

  console.log(JSON.stringify(numbers))

  for (const n of numbers) {
    await ht.set(`key${n}`, `value${n}`, n)
  }

  let rootNode = null

  await ht._constructBTree({
    unique: true,
    onNode({ node }) {
      if (node.root) {
        rootNode = node
      }
    }
  })

  assert.equal(rootNode.items.length, 3)

  await assertMinKeysPerNode(ht, 4) // ok because the standard minimum keys-per-node rule for b-trees does not apply to the root node

  await ht.close()
  await ht.destroy()
}).case()

const test23 = new Test('DiskSortedHashTable', async function integration23() {
  const numbers = [...require('./test/randomNumbers1023_1.json')]
  assert.equal(numbers.length, 1023)
  const sortedNumbers = [...numbers].sort((a, b) => a - b)
  const sortedNumbersReverse = [...sortedNumbers].reverse()
  const sortedValues = sortedNumbers.map(n => `value${n}`)
  const sortedValuesReverse = sortedNumbersReverse.map(n => `value${n}`)

  const ht = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/2048`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/2048_header`,
    initialLength: 2048,
    sortValueType: 'number',
    resizeRatio: 0,
    degree: 5,
  })
  await ht.destroy()
  await ht.init()

  const numbersArray = []
  let i = 1
  while (i <= 100) {
    const numbers = require(`./test/randomNumbers1023_${i}.json`)
    numbersArray.push(numbers)
    i += 1
  }

  for (const numbers of numbersArray) {
    console.log(JSON.stringify(numbers))

    for (const n of numbers) {
      await ht.set(`key${n}`, `value${n}`, n)
    }

    await assertBalanced(ht)
    await assertMinHeight(ht, calculateMinBTreeHeight(1023, 5))
    await assertMaxHeight(ht, calculateMaxBTreeHeight(1023, 5))
    await assertMinKeysPerNode(ht, 4)
    await assertMaxKeysPerNode(ht, 9)

    {
      const forwardValues = []
      for await (const value of ht.forwardIterator()) {
        forwardValues.push(value)
      }
      assert.deepEqual(forwardValues, sortedValues)
    }

    {
      const items = await ht._traverseInOrder()
      assert.deepEqual(items.map(item => Number(item.sortValue)), sortedNumbers)
    }

    {
      const reverseValues = []
      for await (const value of ht.reverseIterator()) {
        reverseValues.push(value)
      }
      assert.deepEqual(reverseValues, sortedValuesReverse)
    }

    await ht.clear()
  }


  await ht.close()
  await ht.destroy()

}).case()

const test24 = new Test('DiskSortedHashTable', async function integration24() {
  const numbers = [...require('./test/randomNumbers32767_1.json')]
  assert.equal(numbers.length, 32767)
  const sortedNumbers = [...numbers].sort((a, b) => a - b)
  const sortedNumbersReverse = [...sortedNumbers].reverse()
  const sortedValues = sortedNumbers.map(n => `value${n}`)
  const sortedValuesReverse = sortedNumbersReverse.map(n => `value${n}`)

  const ht = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/65536`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/65536_header`,
    initialLength: 65536,
    sortValueType: 'number',
    resizeRatio: 0,
    degree: 3,
  })
  await ht.destroy()
  await ht.init()

  const numbersArray = []
  let i = 1
  while (i <= 1) {
    const numbers = require(`./test/randomNumbers32767_${i}.json`)
    numbersArray.push(numbers)
    i += 1
  }

  for (const numbers of numbersArray) {
    console.log(JSON.stringify(numbers))

    for (const n of numbers) {
      console.log('set', `${n}`, `value${n}`, n)
      await ht.set(`key${n}`, `value${n}`, n)
    }

    await assertBalanced(ht)
    await assertMinHeight(ht, calculateMinBTreeHeight(32767, 3))
    await assertMaxHeight(ht, calculateMaxBTreeHeight(32767, 3))
    await assertMinKeysPerNode(ht, 2)
    await assertMaxKeysPerNode(ht, 5)

    {
      const forwardValues = []
      for await (const value of ht.forwardIterator()) {
        forwardValues.push(value)
      }
      assert.deepEqual(forwardValues, sortedValues)
    }

    {
      const items = await ht._traverseInOrder()
      assert.deepEqual(items.map(item => Number(item.sortValue)), sortedNumbers)
    }

    {
      const reverseValues = []
      for await (const value of ht.reverseIterator()) {
        reverseValues.push(value)
      }
      assert.deepEqual(reverseValues, sortedValuesReverse)
    }

    await ht.clear()
  }


  await ht.close()
  await ht.destroy()

}).case()

const test25 = new Test('DiskSortedHashTable', async function integration25() {
  const numbers = [...require('./test/randomNumbers32767_1.json')]
  assert.equal(numbers.length, 32767)
  const sortedNumbers = [...numbers].sort((a, b) => a - b)
  const sortedNumbersReverse = [...sortedNumbers].reverse()
  const sortedValues = sortedNumbers.map(n => `value${n}`)
  const sortedValuesReverse = sortedNumbersReverse.map(n => `value${n}`)

  const ht = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/65536`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/65536_header`,
    initialLength: 65536,
    sortValueType: 'number',
    resizeRatio: 0,
    degree: 2,
  })
  await ht.destroy()
  await ht.init()

  const numbersArray = []
  let i = 1
  while (i <= 1) {
    const numbers = require(`./test/randomNumbers32767_${i}.json`)
    numbersArray.push(numbers)
    i += 1
  }

  for (const numbers of numbersArray) {
    console.log(JSON.stringify(numbers))

    for (const n of numbers) {
      console.log('set', `${n}`, `value${n}`, n)
      await ht.set(`key${n}`, `value${n}`, n)
    }

    await assertBalanced(ht)
    await assertMinHeight(ht, calculateMinBTreeHeight(32767, 2))
    await assertMaxHeight(ht, calculateMaxBTreeHeight(32767, 2))
    await assertMinKeysPerNode(ht, 1)
    await assertMaxKeysPerNode(ht, 3)

    {
      const forwardValues = []
      for await (const value of ht.forwardIterator()) {
        forwardValues.push(value)
      }
      assert.deepEqual(forwardValues, sortedValues)
    }

    {
      const items = await ht._traverseInOrder()
      assert.deepEqual(items.map(item => Number(item.sortValue)), sortedNumbers)
    }

    {
      const reverseValues = []
      for await (const value of ht.reverseIterator()) {
        reverseValues.push(value)
      }
      assert.deepEqual(reverseValues, sortedValuesReverse)
    }

    await ht.clear()
  }


  await ht.close()
  await ht.destroy()

}).case()

const test26 = new Test('DiskSortedHashTable', async function integration26() {
  const numbers = [...require('./test/randomNumbers32767_1.json')]
  assert.equal(numbers.length, 32767)
  const sortedNumbers = [...numbers].sort((a, b) => a - b)
  const sortedNumbersReverse = [...sortedNumbers].reverse()
  const sortedValues = sortedNumbers.map(n => `value${n}`)
  const sortedValuesReverse = sortedNumbersReverse.map(n => `value${n}`)

  const ht = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/65536`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/65536_header`,
    initialLength: 65536,
    sortValueType: 'number',
    resizeRatio: 0,
    degree: 4,
  })
  await ht.destroy()
  await ht.init()

  const numbersArray = []
  let i = 1
  while (i <= 1) {
    const numbers = require(`./test/randomNumbers32767_${i}.json`)
    numbersArray.push(numbers)
    i += 1
  }

  for (const numbers of numbersArray) {
    console.log(JSON.stringify(numbers))

    for (const n of numbers) {
      console.log('set', `${n}`, `value${n}`, n)
      await ht.set(`key${n}`, `value${n}`, n)
    }

    await assertBalanced(ht)
    await assertMinHeight(ht, calculateMinBTreeHeight(32767, 4))
    await assertMaxHeight(ht, calculateMaxBTreeHeight(32767, 4))
    await assertMinKeysPerNode(ht, 3)
    await assertMaxKeysPerNode(ht, 7)

    {
      const forwardValues = []
      for await (const value of ht.forwardIterator()) {
        forwardValues.push(value)
      }
      assert.deepEqual(forwardValues, sortedValues)
    }

    {
      const items = await ht._traverseInOrder()
      assert.deepEqual(items.map(item => Number(item.sortValue)), sortedNumbers)
    }

    {
      const reverseValues = []
      for await (const value of ht.reverseIterator()) {
        reverseValues.push(value)
      }
      assert.deepEqual(reverseValues, sortedValuesReverse)
    }

    await ht.clear()
  }


  await ht.close()
  await ht.destroy()

}).case()

const test27 = new Test('DiskSortedHashTable', async function integration27() {
  const numbers = [...require('./test/randomNumbers32767_1.json')]
  assert.equal(numbers.length, 32767)
  const sortedNumbers = [...numbers].sort((a, b) => a - b)
  const sortedNumbersReverse = [...sortedNumbers].reverse()
  const sortedValues = sortedNumbers.map(n => `value${n}`)
  const sortedValuesReverse = sortedNumbersReverse.map(n => `value${n}`)

  const ht = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/65536`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/65536_header`,
    initialLength: 65536,
    sortValueType: 'number',
    resizeRatio: 0,
    degree: 5,
  })
  await ht.destroy()
  await ht.init()

  const numbersArray = []
  let i = 1
  while (i <= 1) {
    const numbers = require(`./test/randomNumbers32767_${i}.json`)
    numbersArray.push(numbers)
    i += 1
  }

  for (const numbers of numbersArray) {
    console.log(JSON.stringify(numbers))

    for (const n of numbers) {
      console.log('set', `${n}`, `value${n}`, n)
      await ht.set(`key${n}`, `value${n}`, n)
    }

    await assertBalanced(ht)
    await assertMinHeight(ht, calculateMinBTreeHeight(32767, 5))
    await assertMaxHeight(ht, calculateMaxBTreeHeight(32767, 5))
    await assertMinKeysPerNode(ht, 4)
    await assertMaxKeysPerNode(ht, 9)

    {
      const forwardValues = []
      for await (const value of ht.forwardIterator()) {
        forwardValues.push(value)
      }
      assert.deepEqual(forwardValues, sortedValues)
    }

    {
      const items = await ht._traverseInOrder()
      assert.deepEqual(items.map(item => Number(item.sortValue)), sortedNumbers)
    }

    {
      const reverseValues = []
      for await (const value of ht.reverseIterator()) {
        reverseValues.push(value)
      }
      assert.deepEqual(reverseValues, sortedValuesReverse)
    }

    await ht.clear()
  }


  await ht.close()
  await ht.destroy()

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
  test17_0,
  test17_1,
  test17_2,
  test17_3,
  test17_4,
  test17_5,
  test17_6,
  test17_7_0,
  test17_7,
  test18,
  test19,
  test20,
  test21,
  test22,
  test23_0,
  test23,
  test24,
  test25,
  test26,
  test27,
])

if (process.argv[1] == __filename) {
  // test()
  test14_0()
  // test14_00()
}

module.exports = test
