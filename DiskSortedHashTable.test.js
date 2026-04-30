const Test = require('thunk-test')
const assert = require('assert')
const DiskSortedHashTable = require('./DiskSortedHashTable')
const generateKLengthPermutations = require('./_internal/generateKLengthPermutations')
const AsyncPool = require('./_internal/AsyncPool')
const traverseInOrder = require('./_internal/traverseInOrder')
const assertBalanced = require('./test/assertBalanced')
const assertMinHeight = require('./test/assertMinHeight')
const assertMaxHeight = require('./test/assertMaxHeight')
const assertMinKeysPerNode = require('./test/assertMinKeysPerNode')
const assertMaxKeysPerNode = require('./test/assertMaxKeysPerNode')
const assertInternalNodesIntegrity = require('./test/assertInternalNodesIntegrity')
const calculateMaxBTreeHeight = require('./_internal/calculateMaxBTreeHeight')
const calculateMinBTreeHeight = require('./_internal/calculateMinBTreeHeight')
const constructBTree = require('./_internal/constructBTree')

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
    await constructBTree(ht1024, {
      unique: true,
      onNode({ node }) {
        nodes.push(node)
      },
      onLeaf({ node }) {
        leafNodes.push(node)
      },
    })

    assert.equal(leafNodes.length, 1)
    assert.equal(nodes.length, 1)
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

  {
    const nodes = []
    const leafNodes = []
    await constructBTree(ht1024, {
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
    const btreeRootNode = await constructBTree(ht1024, { unique: false })
    const items = traverseInOrder(btreeRootNode)
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
  assert.equal(ht1024._deletedCount, 0)

  await ht1024.delete('maroon').then(didDelete => assert(didDelete))
  assert.strictEqual(await ht1024.get('maroon'), undefined)

  assert.equal(ht1024.count(), 2)
  assert.equal(ht1024._deletedCount, 1)

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
    const btreeRootNode = await constructBTree(ht1024, { unique: false })
    const items = traverseInOrder(btreeRootNode)
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
  assert.equal(ht1024._deletedCount, 0)

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
  assert.equal(ht2.count(), 1)
  assert.equal(ht2._deletedCount, 0)

  const collisionKey = 'maroon1'
  await ht2.set(collisionKey, '#800000(1)', 1)
  assert.equal(ht2.count(), 2)
  assert.equal(ht2._deletedCount, 0)

  assert.equal(await ht2.get('maroon'), '#800000')
  assert.equal(await ht2.get(collisionKey), '#800000(1)')

  await ht2.delete('maroon').then(didDelete => assert(didDelete))
  assert.equal(ht2.count(), 1)
  assert.equal(ht2._deletedCount, 1)

  await ht2.delete('maroon').then(didDelete => assert(!didDelete))
  assert.equal(ht2.count(), 1)
  assert.equal(ht2._deletedCount, 1)

  assert.equal(await ht2.get('maroon'), undefined)
  await ht2.delete(collisionKey).then(didDelete => assert(didDelete))
  assert.equal(ht2.count(), 0)
  assert.equal(ht2._deletedCount, 2)

  assert.equal(await ht2.get(collisionKey), undefined)
  await ht2.delete('maroon3').then(didDelete => assert(!didDelete))
  assert.equal(ht2.count(), 0)
  assert.equal(ht2._deletedCount, 2)

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

const test1_5 = new Test('DiskSortedHashTable', async function integration1_5() {
  const ht1024 = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/1024`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/1024_header`,
    initialLength: 1024,
    itemSize: 2048,
    sortValueType: 'number',
  })
  await ht1024.destroy()
  await ht1024.init()

  assert.equal(ht1024._length, 1024)

  assert.strictEqual(ht1024.count(), 0)

  await ht1024.set('maroon', Buffer.from('#800000'), 1)
  await ht1024.set('yellow', Buffer.from('#FFFF00'), 2)
  await ht1024.set('black', new Uint8Array(Buffer.from('#000000')), 4)

  assert(Buffer.from('#800000').equals(await ht1024.getBinary('maroon')))
  assert(Buffer.from('#FFFF00').equals(await ht1024.getBinary('yellow')))
  assert(Buffer.from('#000000').equals(await ht1024.getBinary('black')))

  assert.strictEqual(ht1024.count(), 3)

  await ht1024.close()
  await ht1024.init()

  assert.equal(ht1024._length, 1024)

  assert(Buffer.from('#800000').equals(await ht1024.getBinary('maroon')))
  assert(Buffer.from('#FFFF00').equals(await ht1024.getBinary('yellow')))
  assert(Buffer.from('#000000').equals(await ht1024.getBinary('black')))

  assert.strictEqual(ht1024.count(), 3)

  await ht1024.set('maroon', Buffer.from('#800000_'), 1)
  await ht1024.set('yellow', Buffer.from('#FFFF00_'), 2)
  await ht1024.set('black', new Uint8Array(Buffer.from('#000000_')), 4)

  assert(Buffer.from('#800000_').equals(await ht1024.getBinary('maroon')))
  assert(Buffer.from('#FFFF00_').equals(await ht1024.getBinary('yellow')))
  assert(Buffer.from('#000000_').equals(await ht1024.getBinary('black')))

  {
    const forwardValues = []
    for await (const value of ht1024.forwardIterator({ valueType: 'binary' })) {
      forwardValues.push(value)
    }
    assert.equal(forwardValues.length, 3)
    assert(Buffer.from('#800000_').equals(forwardValues[0]))
    assert(Buffer.from('#FFFF00_').equals(forwardValues[1]))
    assert(Buffer.from('#000000_').equals(forwardValues[2]))
  }

  {
    const reverseValues = []
    for await (const value of ht1024.reverseIterator({ valueType: 'binary' })) {
      reverseValues.push(value)
    }
    assert.equal(reverseValues.length, 3)
    assert(Buffer.from('#000000_').equals(reverseValues[0]))
    assert(Buffer.from('#FFFF00_').equals(reverseValues[1]))
    assert(Buffer.from('#800000_').equals(reverseValues[2]))
  }

  assert.strictEqual(ht1024.count(), 3)

  await ht1024.clear()

  assert.strictEqual(await ht1024.getBinary('maroon'), undefined)
  assert.strictEqual(await ht1024.getBinary('yellow'), undefined)
  assert.strictEqual(await ht1024.getBinary('black'), undefined)

  assert.strictEqual(ht1024.count(), 0)
  assert.strictEqual(ht1024._deletedCount, 0)

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

  assert.equal(ht1024.count(), 0)
  assert.equal(ht1024._deletedCount, 0)

  await ht1024.set('maroon', '#800000', 1)
  await ht1024.set('yellow', '#FFFF00', 2)
  await ht1024.set('black', '#000', 4)

  assert.equal(ht1024.count(), 3)
  assert.equal(ht1024._deletedCount, 0)

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

  assert.equal(ht1024.count(), 3)
  assert.equal(ht1024._deletedCount, 0)

  await ht1024.delete('maroon').then(didDelete => assert(didDelete))

  assert.equal(ht1024.count(), 2)
  assert.equal(ht1024._deletedCount, 1)

  await ht1024.set('maroon', '#800000', 5)

  assert.equal(ht1024.count(), 3)
  assert.equal(ht1024._deletedCount, 0)

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

  assert.equal(ht1024.count(), 3)
  assert.equal(ht1024._deletedCount, 0)

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

  assert.equal(ht1024.count(), 3)
  assert.equal(ht1024._deletedCount, 0)

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
    const btreeRootNode = await constructBTree(ht1024, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [1, 2, 4])
  }

  await ht1024.delete('maroon').then(didDelete => assert(didDelete))

  assert.equal(ht1024.count(), 2)
  assert.equal(ht1024._deletedCount, 1)

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
    const btreeRootNode = await constructBTree(ht1024, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [2, 4])
  }

  await ht1024.delete('yellow').then(didDelete => assert(didDelete))

  assert.equal(ht1024.count(), 1)
  assert.equal(ht1024._deletedCount, 2)

  {
    const forwardValues = []
    for await (const value of ht1024.forwardIterator()) {
      forwardValues.push(value)
    }
    assert.equal(forwardValues.length, 1)
    assert.equal(forwardValues[0], '#000')
  }

  {
    const btreeRootNode = await constructBTree(ht1024, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [4])
  }

  await ht1024.delete('black').then(didDelete => assert(didDelete))

  assert.equal(ht1024.count(), 0)
  assert.equal(ht1024._deletedCount, 3)

  {
    const forwardValues = []
    for await (const value of ht1024.forwardIterator()) {
      forwardValues.push(value)
    }
    assert.equal(forwardValues.length, 0)
  }

  {
    const btreeRootNode = await constructBTree(ht1024, { unique: false })
    const items = traverseInOrder(btreeRootNode)
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

  await ht100.set('key1', 'value1', 1)
  await ht100.set('key2', 'value2', 2)
  await ht100.set('key3', 'value3', 3)
  await ht100.set('key4', 'value4', 4)
  await ht100.set('key5', 'value5', 5)
  await ht100.set('key6', 'value6', 6)
  await ht100.set('key7', 'value7', 7)
  await ht100.set('key8', 'value8', 8)
  await ht100.set('key9', 'value9', 9)
  await ht100.set('key10', 'value10', 10)
  await ht100.set('key11', 'value11', 11)
  await ht100.set('key12', 'value12', 12)
  await ht100.set('key13', 'value13', 13)
  await ht100.set('key14', 'value14', 14)
  await ht100.set('key15', 'value15', 15)

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
    ])
  }

  await ht100.delete('key1')
  await ht100.delete('key2')
  await ht100.delete('key3')
  await ht100.delete('key4')
  await ht100.delete('key5')
  await ht100.delete('key6')
  await ht100.delete('key7')
  await ht100.delete('key8')
  await ht100.delete('key9')
  await ht100.delete('key10')

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      11, 12, 13, 14, 15,
    ])
  }

  await ht100.set('key1', 'value1', 1)
  await ht100.set('key2', 'value2', 2)
  await ht100.set('key3', 'value3', 3)
  await ht100.set('key4', 'value4', 4)
  await ht100.set('key5', 'value5', 5)
  await ht100.set('key6', 'value6', 6)
  await ht100.set('key7', 'value7', 7)
  await ht100.set('key8', 'value8', 8)
  await ht100.set('key9', 'value9', 9)
  await ht100.set('key10', 'value10', 10)

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
    ])
  }

  ht100.close()
  ht100.destroy()
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

  assert.equal(ht10._count, 0)
  assert.equal(ht10._length, 10)

  await ht10.set('key1', 'value1', 1)

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

  await ht10.set('key2', 'value2', 2)
  await ht10.set('key3', 'value3', 3)

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

  await ht10.set('key4', 'value4', 4)
  await ht10.set('key5', 'value5', 5)
  await ht10.set('key6', 'value6', 6)

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

  await ht10.set('key7', 'value7', 7)

  assert.equal(ht10._count, 7)
  assert.equal(ht10._length, 10)

  await ht10.set('key8', 'value8', 8)

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

  await ht10.set('key9', 'value9', 9)

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

  await ht10.set('key10', 'value10', 10)

  assert.equal(ht10._count, 10)
  assert.equal(ht10._length, 20)

  await ht10.set('key11', 'value11', 11)
  await ht10.set('key12', 'value12', 12)
  await ht10.set('key13', 'value13', 13)
  await ht10.set('key14', 'value14', 14)
  await ht10.set('key15', 'value15', 15)
  await ht10.set('key16', 'value16', 16)

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

  await ht10.set('key17', 'value17', 17)

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

  await ht10.set('key18', 'value18', 18)
  await ht10.set('key19', 'value19', 19)
  await ht10.set('key20', 'value20', 20)
  await ht10.set('key21', 'value21', 21)
  await ht10.set('key22', 'value22', 22)
  await ht10.set('key23', 'value23', 23)
  await ht10.set('key24', 'value24', 24)
  await ht10.set('key25', 'value25', 25)
  await ht10.set('key26', 'value26', 26)
  await ht10.set('key27', 'value27', 27)
  await ht10.set('key28', 'value28', 28)
  await ht10.set('key29', 'value29', 29)
  await ht10.set('key30', 'value30', 30)
  await ht10.set('key31', 'value31', 31)
  await ht10.set('key32', 'value32', 32)

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

  await ht10.set('key33', 'value33', 33)

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

const test_assert1 = new Test('DiskSortedHashTable', async function integration_assert1() {
  const ht = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/10`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/10_header`,
    initialLength: 100,
    sortValueType: 'number',
    resizeRatio: 0,
  })
  await ht.destroy()
  await ht.init()

  await ht.set('key1', 'value1', 1)

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

  await ht.set('key2', 'value2', 2)
  await ht.set('key3', 'value3', 3)

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

    await constructBTree(ht, {
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

  await ht.set('key4', 'value4', 4)

  {
    const nodes = []
    const leafNodes = []

    await constructBTree(ht, {
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

    const btreeRootNode = await constructBTree(ht, { unique: false })
    assertBalanced(btreeRootNode)
    assertMinHeight(btreeRootNode, 0)
    assertMaxHeight(btreeRootNode, 1)
    assertMinKeysPerNode(btreeRootNode, 1)
    assertMaxKeysPerNode(btreeRootNode, 2)
    assertInternalNodesIntegrity(btreeRootNode)

    assertMinHeight(btreeRootNode, 1)
    await assert.throws(
      () => assertMinHeight(btreeRootNode, 2),
      new Error('b-tree under min height (1 / 2)')
    )

    await assert.throws(
      () => assertMaxHeight(btreeRootNode, 0),
      new Error('b-tree over max height (1 / 0)')
    )

    await assert.throws(
      () => assertMaxKeysPerNode(btreeRootNode, 1),
      new Error('b-tree node over maximum number of keys per node (2 / 1)')
    )

    await assert.throws(
      () => assertMinKeysPerNode(btreeRootNode, 2),
      new Error('b-tree node under minimum number of keys per node (1 / 2)')
    )

    await assert.throws(
      () => assertMaxKeysPerNode(btreeRootNode, 1),
      new Error('b-tree node over maximum number of keys per node (2 / 1)')
    )
  }

  await ht.set('key4', 'value4', 4)
  await ht.set('key5', 'value5', 5)
  await ht.set('key6', 'value6', 6)
  await ht.set('key7', 'value7', 7)
  await ht.set('key8', 'value8', 8)
  await ht.set('key9', 'value9', 9)
  await ht.set('key10', 'value10', 10)
  await ht.set('key11', 'value11', 11)
  await ht.set('key12', 'value12', 12)

  {
    const btreeRootNode = await constructBTree(ht, { unique: false })

    assert.equal(
      JSON.stringify(btreeRootNode, (key, value) => {
        if (key == 'items' || key == 'keys') {
          return undefined
        }
        return value
      }),
      JSON.stringify({
        "root": true,
        "key4": {
          "leftChild": {
            "key2": {
              "leftChild": {
                "key1": {}
              },
              "rightChild": {
                "key3": {}
              }
            }
          },
          "rightChild": {
            "key6": {
              "leftChild": {
                "key5": {}
              },
              "rightChild": {
                "key7": {}
              }
            },
            "key8": {
              "leftChild": {
                "key7": {}
              },
              "rightChild": {
                "key9": {}
              }
            },
            "key10": {
              "leftChild": {
                "key9": {}
              },
              "rightChild": {
                "key11": {},
                "key12": {}
              }
            }
          }
        }
      })
    )

    assertBalanced(btreeRootNode)
    assertMinHeight(btreeRootNode, 2)
  }

  const indexOf2 = ht._hash1('key2')

  await ht._writeBTreeLeftChildNodeRightmostItemIndex(indexOf2, -1)
  await ht._writeBTreeRightChildNodeRightmostItemIndex(indexOf2, -1)

  {
    const btreeRootNode = await constructBTree(ht, { unique: false })

    assert.equal(
      JSON.stringify(btreeRootNode, (key, value) => {
        if (key == 'items' || key == 'keys') {
          return undefined
        }
        return value
      }),
      JSON.stringify({
        "root": true,
        "key4": {
          "leftChild": {
            "key2": {}
          },
          "rightChild": {
            "key6": {
              "leftChild": {
                "key5": {}
              },
              "rightChild": {
                "key7": {}
              }
            },
            "key8": {
              "leftChild": {
                "key7": {}
              },
              "rightChild": {
                "key9": {}
              }
            },
            "key10": {
              "leftChild": {
                "key9": {}
              },
              "rightChild": {
                "key11": {},
                "key12": {}
              }
            }
          }
        }
      })
    )

    assert.throws(
      () => assertBalanced(btreeRootNode),
      new Error('b-tree not balanced')
    )

    assert.throws(
      () => assertMinHeight(btreeRootNode, 2),
      new Error('b-tree under min height (1 / 2)')
    )
  }

  ht.close()
}).case()


const test_assert2 = new Test('DiskSortedHashTable', async function integration_assert2() {
  const ht = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/255`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/255_header`,
    initialLength: 255,
    sortValueType: 'number',
    resizeRatio: 0,
  })
  await ht.destroy()
  await ht.init()

  await ht.set('key1', 'value1', 1)
  await ht.set('key2', 'value2', 2)
  await ht.set('key3', 'value3', 3)
  await ht.set('key4', 'value4', 4)
  await ht.set('key5', 'value5', 5)
  await ht.set('key6', 'value6', 6)
  await ht.set('key7', 'value7', 7)
  await ht.set('key8', 'value8', 8)
  await ht.set('key9', 'value9', 9)
  await ht.set('key10', 'value10', 10)
  await ht.set('key11', 'value11', 11)
  await ht.set('key12', 'value12', 12)
  await ht.set('key13', 'value13', 13)
  await ht.set('key14', 'value14', 14)
  await ht.set('key15', 'value15', 15)
  await ht.set('key16', 'value16', 16)
  await ht.set('key17', 'value17', 17)
  await ht.set('key18', 'value18', 18)
  await ht.set('key19', 'value19', 19)
  await ht.set('key20', 'value20', 20)
  await ht.set('key21', 'value21', 21)

  assert.equal(
    JSON.stringify(await constructBTree(ht, { unique: false }), (key, value) => {
      if (key == 'items' || key == 'keys') {
        return undefined
      }
      return value
    }),
    JSON.stringify({
      "root": true,
      "key8": {
        "leftChild": {
          "key4": {
            "leftChild": {
              "key2": {
                "leftChild": {
                  "key1": {}
                },
                "rightChild": {
                  "key3": {}
                }
              }
            },
            "rightChild": {
              "key6": {
                "leftChild": {
                  "key5": {}
                },
                "rightChild": {
                  "key7": {}
                }
              }
            }
          }
        },
        "rightChild": {
          "key12": {
            "leftChild": {
              "key10": {
                "leftChild": {
                  "key9": {}
                },
                "rightChild": {
                  "key11": {}
                }
              }
            },
            "rightChild": {
              "key14": {
                "leftChild": {
                  "key13": {}
                },
                "rightChild": {
                  "key15": {}
                }
              }
            }
          },
          "key16": {
            "leftChild": {
              "key14": {
                "leftChild": {
                  "key13": {}
                },
                "rightChild": {
                  "key15": {}
                }
              }
            },
            "rightChild": {
              "key18": {
                "leftChild": {
                  "key17": {}
                },
                "rightChild": {
                  "key19": {},
                  "key20": {},
                  "key21": {}
                }
              }
            }
          }
        }
      }
    })
  )

  {
    const btreeRootNode = await constructBTree(ht, { unique: false })
    assertBalanced(btreeRootNode)
  }

  const indexOf16 = ht._hash1('key16')
  const indexOf14 = ht._hash1('key14')

  const btreeItem14 = await ht._getBTreeItem(indexOf14)
  await ht._writeBTreeLeftChildNodeRightmostItemIndex(indexOf16, btreeItem14.btreeLeftChildNodeRightmostItemIndex)

  assert.equal(
    JSON.stringify(await constructBTree(ht, { unique: false }), (key, value) => {
      if (key == 'items' || key == 'keys') {
        return undefined
      }
      return value
    }),
    JSON.stringify({
      "root": true,
      "key8": {
        "leftChild": {
          "key4": {
            "leftChild": {
              "key2": {
                "leftChild": {
                  "key1": {}
                },
                "rightChild": {
                  "key3": {}
                }
              }
            },
            "rightChild": {
              "key6": {
                "leftChild": {
                  "key5": {}
                },
                "rightChild": {
                  "key7": {}
                }
              }
            }
          }
        },
        "rightChild": {
          "key12": {
            "leftChild": {
              "key10": {
                "leftChild": {
                  "key9": {}
                },
                "rightChild": {
                  "key11": {}
                }
              }
            },
            "rightChild": {
              "key14": {
                "leftChild": {
                  "key13": {}
                },
                "rightChild": {
                  "key15": {}
                }
              }
            }
          },
          "key16": {
            "leftChild": {
              "key13": {}
            },
            "rightChild": {
              "key18": {
                "leftChild": {
                  "key17": {}
                },
                "rightChild": {
                  "key19": {},
                  "key20": {},
                  "key21": {}
                }
              }
            }
          }
        }
      }
    })
  )

  {
    const btreeRootNode = await constructBTree(ht, { unique: false })

    assert.throws(
      () => assertBalanced(btreeRootNode),
      new Error('b-tree not balanced')
    )
  }

  ht.close()
}).case()

const test_assert3 = new Test('DiskSortedHashTable', async function integration_assert3() {
  const ht = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/255`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/255_header`,
    initialLength: 255,
    sortValueType: 'number',
    resizeRatio: 0,
  })
  await ht.destroy()
  await ht.init()

  await ht.set('key1', 'value1', 1)
  await ht.set('key2', 'value2', 2)
  await ht.set('key3', 'value3', 3)
  await ht.set('key4', 'value4', 4)
  await ht.set('key5', 'value5', 5)
  await ht.set('key6', 'value6', 6)
  await ht.set('key7', 'value7', 7)
  await ht.set('key8', 'value8', 8)
  // await ht.set('9', 'value9', 9)
  // await ht.set('10', 'value10', 10)
  // await ht.set('11', 'value11', 11)
  // await ht.set('12', 'value12', 12)
  // await ht.set('13', 'value13', 13)
  // await ht.set('14', 'value14', 14)
  // await ht.set('15', 'value15', 15)
  // await ht.set('16', 'value16', 16)
  // await ht.set('17', 'value17', 17)
  // await ht.set('18', 'value18', 18)
  // await ht.set('19', 'value19', 19)
  // await ht.set('20', 'value20', 20)
  // await ht.set('21', 'value21', 21)

  assert.equal(
    JSON.stringify(await constructBTree(ht, { unique: false }), (key, value) => {
      if (key == 'items' || key == 'keys') {
        return undefined
      }
      return value
    }),
    JSON.stringify({
      "root": true,
      "key2": {
        "leftChild": {
          "key1": {}
        },
        "rightChild": {
          "key3": {}
        }
      },
      "key4": {
        "leftChild": {
          "key3": {}
        },
        "rightChild": {
          "key5": {}
        }
      },
      "key6": {
        "leftChild": {
          "key5": {}
        },
        "rightChild": {
          "key7": {},
          "key8": {}
        }
      }
    })
  )

  {
    const btreeRootNode = await constructBTree(ht, { unique: false })
    assertBalanced(btreeRootNode)
    assertInternalNodesIntegrity(btreeRootNode)
  }

  const indexOf1 = ht._hash1('key1')
  const indexOf4 = ht._hash1('key4')

  await ht._writeBTreeLeftChildNodeRightmostItemIndex(indexOf4, indexOf1)

  assert.equal(
    JSON.stringify(await constructBTree(ht, { unique: false }), (key, value) => {
      if (key == 'items' || key == 'keys') {
        return undefined
      }
      return value
    }),
    JSON.stringify({
      "root": true,
      "key2": {
        "leftChild": {
          "key1": {}
        },
        "rightChild": {
          "key3": {}
        }
      },
      "key4": {
        "leftChild": {
          "key1": {}
        },
        "rightChild": {
          "key5": {}
        }
      },
      "key6": {
        "leftChild": {
          "key5": {}
        },
        "rightChild": {
          "key7": {},
          "key8": {}
        }
      }
    })
  )

  {
    const btreeRootNode = await constructBTree(ht, { unique: false })

    assertBalanced(btreeRootNode)

    assert.throws(
      () => assertInternalNodesIntegrity(btreeRootNode),
      error => {
        assert.equal(error.name, 'AssertionError')
        return true
      }
    )
  }

  ht.close()
}).case()

const test_assert4 = new Test('DiskSortedHashTable', async function integration_assert4() {
  const ht = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/255`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/255_header`,
    initialLength: 255,
    sortValueType: 'number',
    resizeRatio: 0,
  })
  await ht.destroy()
  await ht.init()

  await ht.set('key1', 'value1', 1)
  await ht.set('key2', 'value2', 2)
  await ht.set('key3', 'value3', 3)
  await ht.set('key4', 'value4', 4)
  await ht.set('key5', 'value5', 5)
  await ht.set('key6', 'value6', 6)
  await ht.set('key7', 'value7', 7)
  await ht.set('key8', 'value8', 8)
  await ht.set('9', 'value9', 9)
  await ht.set('10', 'value10', 10)
  await ht.set('11', 'value11', 11)
  await ht.set('12', 'value12', 12)
  await ht.set('13', 'value13', 13)
  // await ht.set('14', 'value14', 14)
  // await ht.set('15', 'value15', 15)
  // await ht.set('16', 'value16', 16)
  // await ht.set('17', 'value17', 17)
  // await ht.set('18', 'value18', 18)
  // await ht.set('19', 'value19', 19)
  // await ht.set('20', 'value20', 20)
  // await ht.set('21', 'value21', 21)

  assert.equal(
    JSON.stringify(await constructBTree(ht, { unique: false }), (key, value) => {
      if (key == 'items' || key == 'keys') {
        return undefined
      }
      return value
    }),
    JSON.stringify({
      "root": true,
      "key4": {
        "leftChild": {
          "key2": {
            "leftChild": {
              "key1": {}
            },
            "rightChild": {
              "key3": {}
            }
          }
        },
        "rightChild": {
          "key6": {
            "leftChild": {
              "key5": {}
            },
            "rightChild": {
              "key7": {}
            }
          }
        }
      },
      "key8": {
        "leftChild": {
          "key6": {
            "leftChild": {
              "key5": {}
            },
            "rightChild": {
              "key7": {}
            }
          }
        },
        "rightChild": {
          "10": {
            "leftChild": {
              "9": {}
            },
            "rightChild": {
              "11": {},
              "12": {},
              "13": {}
            }
          }
        }
      }
    })
  )

  {
    const btreeRootNode = await constructBTree(ht, { unique: false })
    assertBalanced(btreeRootNode)
    assertInternalNodesIntegrity(btreeRootNode)
  }

  const indexOf2 = ht._hash1('key2')
  const indexOf8 = ht._hash1('key8')

  await ht._writeBTreeLeftChildNodeRightmostItemIndex(indexOf8, indexOf2)

  assert.equal(
    JSON.stringify(await constructBTree(ht, { unique: false }), (key, value) => {
      if (key == 'items' || key == 'keys') {
        return undefined
      }
      return value
    }),
    JSON.stringify({
      "root": true,
      "key4": {
        "leftChild": {
          "key2": {
            "leftChild": {
              "key1": {}
            },
            "rightChild": {
              "key3": {}
            }
          }
        },
        "rightChild": {
          "key6": {
            "leftChild": {
              "key5": {}
            },
            "rightChild": {
              "key7": {}
            }
          }
        }
      },
      "key8": {
        "leftChild": {
          "key2": {
            "leftChild": {
              "key1": {}
            },
            "rightChild": {
              "key3": {}
            }
          }
        },
        "rightChild": {
          "10": {
            "leftChild": {
              "9": {}
            },
            "rightChild": {
              "11": {},
              "12": {},
              "13": {}
            }
          }
        }
      }
    })
  )

  {
    const btreeRootNode = await constructBTree(ht, { unique: false })

    assertBalanced(btreeRootNode)

    assert.throws(
      () => assertInternalNodesIntegrity(btreeRootNode),
      error => {
        assert.equal(error.name, 'AssertionError')
        return true
      }
    )
  }

  ht.close()
}).case()

const test_assert5 = new Test('DiskSortedHashTable', async function integration_assert5() {
  const ht = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/255`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/255_header`,
    initialLength: 255,
    sortValueType: 'number',
    resizeRatio: 0,
  })
  await ht.destroy()
  await ht.init()

  assert.equal(
    JSON.stringify(await constructBTree(ht, { unique: false }), (key, value) => {
      if (key == 'items' || key == 'keys') {
        return undefined
      }
      return value
    }),
    JSON.stringify({
      "root": true,
    })
  )

  {
    const btreeRootNode = await constructBTree(ht, { unique: false })
    assertBalanced(btreeRootNode)
    assertInternalNodesIntegrity(btreeRootNode)
  }

  ht.close()
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

  await ht100.set('key1', 'value1', 1)

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

  await ht100.set('key2', 'value2', 2)
  await ht100.set('key3', 'value3', 3)

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

  await ht100.set('key4', 'value4', 4)
  await ht100.set('key5', 'value5', 5)
  await ht100.set('key6', 'value6', 6)

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

  await ht100.set('key7', 'value7', 7)
  await ht100.set('key8', 'value8', 8)

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

  await ht100.set('key9', 'value9', 9)

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

  await ht100.set('key10', 'value10', 10)
  await ht100.set('key11', 'value11', 11)
  await ht100.set('key12', 'value12', 12)
  await ht100.set('key13', 'value13', 13)
  await ht100.set('key14', 'value14', 14)
  await ht100.set('key15', 'value15', 15)
  await ht100.set('key16', 'value16', 16)

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

  await ht100.set('key17', 'value17', 17)

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

  await ht100.set('key18', 'value18', 18)
  await ht100.set('key19', 'value19', 19)
  await ht100.set('key20', 'value20', 20)
  await ht100.set('key21', 'value21', 21)
  await ht100.set('key22', 'value22', 22)
  await ht100.set('key23', 'value23', 23)
  await ht100.set('key24', 'value24', 24)
  await ht100.set('key25', 'value25', 25)
  await ht100.set('key26', 'value26', 26)
  await ht100.set('key27', 'value27', 27)
  await ht100.set('key28', 'value28', 28)
  await ht100.set('key29', 'value29', 29)
  await ht100.set('key30', 'value30', 30)
  await ht100.set('key31', 'value31', 31)
  await ht100.set('key32', 'value32', 32)

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

  await ht100.set('key33', 'value33', 33)

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

  assert.strictEqual(ht100.count(), 33)

  await ht100.delete('key33')

  assert.strictEqual(ht100.count(), 32)

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
      11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
      21, 22, 23, 24, 25, 26, 27, 28, 29, 30,
      31, 32,
    ])
  }

  await ht100.delete('key31')

  assert.strictEqual(ht100.count(), 31)

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
      11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
      21, 22, 23, 24, 25, 26, 27, 28, 29, 30,
      32,
    ])
  }

  await ht100.delete('key32')

  assert.strictEqual(ht100.count(), 30)

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
      11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
      21, 22, 23, 24, 25, 26, 27, 28, 29, 30,
    ])
  }

  await ht100.delete('key1')

  assert.strictEqual(ht100.count(), 29)

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      2, 3, 4, 5, 6, 7, 8, 9, 10,
      11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
      21, 22, 23, 24, 25, 26, 27, 28, 29, 30,
    ])
  }

  await ht100.delete('key29')

  assert.strictEqual(ht100.count(), 28)

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      2, 3, 4, 5, 6, 7, 8, 9, 10,
      11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
      21, 22, 23, 24, 25, 26, 27, 28, 30,
    ])
  }

  await ht100.delete('key30')

  assert.strictEqual(ht100.count(), 27)

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      2, 3, 4, 5, 6, 7, 8, 9, 10,
      11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
      21, 22, 23, 24, 25, 26, 27, 28,
    ])
  }

  await ht100.delete('key27')

  assert.strictEqual(ht100.count(), 26)

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      2, 3, 4, 5, 6, 7, 8, 9, 10,
      11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
      21, 22, 23, 24, 25, 26, 28,
    ])
  }

  await ht100.delete('key28')

  assert.strictEqual(ht100.count(), 25)

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      2, 3, 4, 5, 6, 7, 8, 9, 10,
      11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
      21, 22, 23, 24, 25, 26,
    ])
  }

  await ht100.delete('key5')

  assert.strictEqual(ht100.count(), 24)

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      2, 3, 4, 6, 7, 8, 9, 10,
      11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
      21, 22, 23, 24, 25, 26,
    ])
  }

  await ht100.delete('key4')

  assert.strictEqual(ht100.count(), 23)

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      2, 3, 6, 7, 8, 9, 10,
      11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
      21, 22, 23, 24, 25, 26,
    ])
  }

  await ht100.delete('key23')

  assert.strictEqual(ht100.count(), 22)

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      2, 3, 6, 7, 8, 9, 10,
      11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
      21, 22, 24, 25, 26,
    ])
  }

  await ht100.delete('key23').then(didDelete => assert(!didDelete))

  assert.strictEqual(ht100.count(), 22)

  await ht100.delete('key13')

  assert.strictEqual(ht100.count(), 21)

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      2, 3, 6, 7, 8, 9, 10,
      11, 12, 14, 15, 16, 17, 18, 19, 20,
      21, 22, 24, 25, 26,
    ])
  }

  await ht100.delete('key3')

  assert.strictEqual(ht100.count(), 20)

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      2, 6, 7, 8, 9, 10,
      11, 12, 14, 15, 16, 17, 18, 19, 20,
      21, 22, 24, 25, 26,
    ])
  }

  await ht100.delete('key2')

  assert.strictEqual(ht100.count(), 19)

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      6, 7, 8, 9, 10,
      11, 12, 14, 15, 16, 17, 18, 19, 20,
      21, 22, 24, 25, 26,
    ])
  }

  await ht100.delete('key6')

  assert.strictEqual(ht100.count(), 18)

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      7, 8, 9, 10,
      11, 12, 14, 15, 16, 17, 18, 19, 20,
      21, 22, 24, 25, 26,
    ])
  }

  await ht100.delete('key7')

  assert.strictEqual(ht100.count(), 17)

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      8, 9, 10,
      11, 12, 14, 15, 16, 17, 18, 19, 20,
      21, 22, 24, 25, 26,
    ])
  }

  await ht100.delete('key17')

  assert.strictEqual(ht100.count(), 16)

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      8, 9, 10,
      11, 12, 14, 15, 16, 18, 19, 20,
      21, 22, 24, 25, 26,
    ])
  }

  await ht100.delete('key12')

  assert.strictEqual(ht100.count(), 15)

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      8, 9, 10,
      11, 14, 15, 16, 18, 19, 20,
      21, 22, 24, 25, 26,
    ])
  }

  await ht100.delete('key20')

  assert.strictEqual(ht100.count(), 14)

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      8, 9, 10,
      11, 14, 15, 16, 18, 19,
      21, 22, 24, 25, 26,
    ])
  }

  assert.equal(
    JSON.stringify(await constructBTree(ht100, { unique: false }), (key, value) => {
      if (key == 'items' || key == 'keys') {
        return undefined
      }
      return value
    }),
    JSON.stringify({
      "root": true,
      "key11": {
        "leftChild": {
          "key9": {
            "leftChild": {
              "key8": {}
            },
            "rightChild": {
              "key10": {}
            }
          }
        },
        "rightChild": {
          "key16": {
            "leftChild": {
              "key14": {},
              "key15": {}
            },
            "rightChild": {
              "key18": {}
            }
          }
        }
      },
      "key19": {
        "leftChild": {
          "key16": {
            "leftChild": {
              "key14": {},
              "key15": {}
            },
            "rightChild": {
              "key18": {}
            }
          }
        },
        "rightChild": {
          "key22": {
            "leftChild": {
              "key21": {}
            },
            "rightChild": {
              "key24": {}
            }
          },
          "key25": {
            "leftChild": {
              "key24": {}
            },
            "rightChild": {
              "key26": {}
            }
          }
        }
      }
    })
  )

  await ht100.delete('key25')

  assert.strictEqual(ht100.count(), 13)

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      8, 9, 10,
      11, 14, 15, 16, 18, 19,
      21, 22, 24, 26,
    ])
  }

  assert.equal(
    JSON.stringify(await constructBTree(ht100, { unique: false }), (key, value) => {
      if (key == 'items' || key == 'keys') {
        return undefined
      }
      return value
    }),
    JSON.stringify({
      "root": true,
      "key11": {
        "leftChild": {
          "key9": {
            "leftChild": {
              "key8": {}
            },
            "rightChild": {
              "key10": {}
            }
          }
        },
        "rightChild": {
          "key16": {
            "leftChild": {
              "key14": {},
              "key15": {}
            },
            "rightChild": {
              "key18": {}
            }
          }
        }
      },
      "key19": {
        "leftChild": {
          "key16": {
            "leftChild": {
              "key14": {},
              "key15": {}
            },
            "rightChild": {
              "key18": {}
            }
          }
        },
        "rightChild": {
          "key24": {
            "leftChild": {
              "key21": {},
              "key22": {}
            },
            "rightChild": {
              "key26": {}
            }
          }
        }
      }
    })
  )

  await ht100.delete('key15')

  assert.strictEqual(ht100.count(), 12)

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      8, 9, 10,
      11, 14, 16, 18, 19,
      21, 22, 24, 26,
    ])
  }

  await ht100.delete('key21')

  assert.strictEqual(ht100.count(), 11)

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      8, 9, 10,
      11, 14, 16, 18, 19,
      22, 24, 26,
    ])
  }

  await ht100.delete('key19')

  assert.strictEqual(ht100.count(), 10)

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      8, 9, 10,
      11, 14, 16, 18,
      22, 24, 26,
    ])
  }

  assert.equal(
    JSON.stringify(await constructBTree(ht100, { unique: false }), (key, value) => {
      if (key == 'items' || key == 'keys') {
        return undefined
      }
      return value
    }),
    JSON.stringify({
      "root": true,
      "key18": {
        "leftChild": {
          "key9": {
            "leftChild": {
              "key8": {}
            },
            "rightChild": {
              "key10": {}
            }
          },
          "key11": {
            "leftChild": {
              "key10": {}
            },
            "rightChild": {
              "key14": {},
              "key16": {}
            }
          }
        },
        "rightChild": {
          "key24": {
            "leftChild": {
              "key22": {}
            },
            "rightChild": {
              "key26": {}
            }
          }
        }
      }
    })
  )

  await ht100.delete('key19') // not found

  assert.strictEqual(ht100.count(), 10)

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      8, 9, 10,
      11, 14, 16, 18,
      22, 24, 26,
    ])
  }

  await ht100.delete('key11')

  assert.strictEqual(ht100.count(), 9)

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      8, 9, 10,
      14, 16, 18,
      22, 24, 26,
    ])
  }

  assert.equal(
    JSON.stringify(await constructBTree(ht100, { unique: false }), (key, value) => {
      if (key == 'items' || key == 'keys') {
        return undefined
      }
      return value
    }),
    JSON.stringify({
      "root": true,
      "key18": {
        "leftChild": {
          "key9": {
            "leftChild": {
              "key8": {}
            },
            "rightChild": {
              "key10": {}
            }
          },
          "key14": {
            "leftChild": {
              "key10": {}
            },
            "rightChild": {
              "key16": {}
            }
          }
        },
        "rightChild": {
          "key24": {
            "leftChild": {
              "key22": {}
            },
            "rightChild": {
              "key26": {}
            }
          }
        }
      }
    })
  )

  await ht100.delete('key14')

  assert.strictEqual(ht100.count(), 8)

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      8, 9, 10,
      16, 18,
      22, 24, 26,
    ])
  }

  await ht100.delete('key18')

  assert.strictEqual(ht100.count(), 7)

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      8, 9, 10,
      16,
      22, 24, 26,
    ])
  }

  assert.equal(
    JSON.stringify(await constructBTree(ht100, { unique: false }), (key, value) => {
      if (key == 'items' || key == 'keys') {
        return undefined
      }
      return value
    }),
    JSON.stringify({
      "root": true,
      "key16": {
        "leftChild": {
          "key9": {
            "leftChild": {
              "key8": {}
            },
            "rightChild": {
              "key10": {}
            }
          }
        },
        "rightChild": {
          "key24": {
            "leftChild": {
              "key22": {}
            },
            "rightChild": {
              "key26": {}
            }
          }
        }
      }
    })
  )

  await ht100.delete('key24')

  assert.strictEqual(ht100.count(), 6)

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      8, 9, 10,
      16,
      22, 26,
    ])
  }

  assert.equal(
    JSON.stringify(await constructBTree(ht100, { unique: false }), (key, value) => {
      if (key == 'items' || key == 'keys') {
        return undefined
      }
      return value
    }),
    JSON.stringify({
      "root": true,
      "key9": {
        "leftChild": {
          "key8": {}
        },
        "rightChild": {
          "key10": {}
        }
      },
      "key16": {
        "leftChild": {
          "key10": {}
        },
        "rightChild": {
          "key22": {},
          "key26": {}
        }
      }
    })
  )

  await ht100.delete('key16')

  assert.strictEqual(ht100.count(), 5)

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      8, 9, 10,
      22, 26,
    ])
  }

  assert.equal(
    JSON.stringify(await constructBTree(ht100, { unique: false }), (key, value) => {
      if (key == 'items' || key == 'keys') {
        return undefined
      }
      return value
    }),
    JSON.stringify({
      "root": true,
      "key9": {
        "leftChild": {
          "key8": {}
        },
        "rightChild": {
          "key10": {}
        }
      },
      "key22": {
        "leftChild": {
          "key10": {}
        },
        "rightChild": {
          "key26": {}
        }
      }
    })
  )

  await ht100.delete('key9')

  assert.strictEqual(ht100.count(), 4)

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      8, 10,
      22, 26,
    ])
  }

  await ht100.set('key9', 'value9', 9)

  assert.strictEqual(ht100.count(), 5)

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      8, 9, 10,
      22, 26,
    ])
  }

  await ht100.set('key11', 'value11', 11)

  assert.strictEqual(ht100.count(), 6)

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      8, 9, 10, 11,
      22, 26,
    ])
  }

  await ht100.delete('key11')

  assert.strictEqual(ht100.count(), 5)

  assert.equal(
    JSON.stringify(await constructBTree(ht100, { unique: false }), (key, value) => {
      if (key == 'items' || key == 'keys') {
        return undefined
      }
      return value
    }),
    JSON.stringify({
      "root": true,
      "key9": {
        "leftChild": {
          "key8": {}
        },
        "rightChild": {
          "key10": {}
        }
      },
      "key22": {
        "leftChild": {
          "key10": {}
        },
        "rightChild": {
          "key26": {}
        }
      }
    })
  )

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      8, 9, 10,
      22, 26,
    ])
  }

  await ht100.delete('key26')

  assert.strictEqual(ht100.count(), 4)

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      8, 9, 10,
      22,
    ])
  }

  assert.equal(
    JSON.stringify(await constructBTree(ht100, { unique: false }), (key, value) => {
      if (key == 'items' || key == 'keys') {
        return undefined
      }
      return value
    }),
    JSON.stringify({
      "root": true,
      "key9": {
        "leftChild": {
          "key8": {}
        },
        "rightChild": {
          "key10": {},
          "key22": {}
        }
      }
    })
  )

  await ht100.delete('key10')

  assert.strictEqual(ht100.count(), 3)

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      8, 9,
      22,
    ])
  }

  assert.equal(
    JSON.stringify(await constructBTree(ht100, { unique: false }), (key, value) => {
      if (key == 'items' || key == 'keys') {
        return undefined
      }
      return value
    }),
    JSON.stringify({
      "root": true,
      "key9": {
        "leftChild": {
          "key8": {}
        },
        "rightChild": {
          "key22": {}
        }
      }
    })
  )

  await ht100.delete('key9')

  assert.strictEqual(ht100.count(), 2)

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      8, 22,
    ])
  }

  assert.equal(
    JSON.stringify(await constructBTree(ht100, { unique: false }), (key, value) => {
      if (key == 'items' || key == 'keys') {
        return undefined
      }
      return value
    }),
    JSON.stringify({
      "root": true,
      "key8": {},
      "key22": {}
    })
  )

  await ht100.delete('key22')

  assert.strictEqual(ht100.count(), 1)

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      8,
    ])
  }

  assert.equal(
    JSON.stringify(await constructBTree(ht100, { unique: false }), (key, value) => {
      if (key == 'items' || key == 'keys') {
        return undefined
      }
      return value
    }),
    JSON.stringify({
      "root": true,
      "key8": {}
    })
  )

  await ht100.delete('key8')

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [])
  }

  assert.equal(
    JSON.stringify(await constructBTree(ht100, { unique: false }), (key, value) => {
      if (key == 'items' || key == 'keys') {
        return undefined
      }
      return value
    }),
    JSON.stringify({
      "root": true
    })
  )

  ht100.close()
}).case()

const test14_1 = new Test('DiskSortedHashTable', async function integration14_1() {
  const ht100 = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/10`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/10_header`,
    initialLength: 100,
    sortValueType: 'number',
    resizeRatio: 0,
  })
  await ht100.destroy()
  await ht100.init()

  await ht100.set('key4', 'value4', 4)
  await ht100.set('key3', 'value3', 3)
  await ht100.set('key2', 'value2', 2)
  await ht100.set('key1', 'value1', 1)

  await ht100.delete('key4')

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [1, 2, 3])
  }

  assert.equal(
    JSON.stringify(await constructBTree(ht100, { unique: false }), (key, value) => {
      if (key == 'items' || key == 'keys') {
        return undefined
      }
      return value
    }),
    JSON.stringify({
      "root": true,
      "key2": {
        "leftChild": {
          "key1": {},
        },
        "rightChild": {
          "key3": {}
        }
      }
    })
  )

  ht100.close()
}).case()

const test14_2 = new Test('DiskSortedHashTable', async function integration14_2() {
  const ht100 = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/10`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/10_header`,
    initialLength: 100,
    sortValueType: 'number',
    resizeRatio: 0,
  })
  await ht100.destroy()
  await ht100.init()

  await ht100.set('key33', 'value33', 33)
  await ht100.set('key32', 'value32', 32)
  await ht100.set('key31', 'value31', 31)
  await ht100.set('key30', 'value30', 30)
  await ht100.set('key29', 'value29', 29)
  await ht100.set('key28', 'value28', 28)
  await ht100.set('key27', 'value27', 27)
  await ht100.set('key26', 'value26', 26)
  await ht100.set('key25', 'value25', 25)
  await ht100.set('key24', 'value24', 24)
  await ht100.set('key23', 'value23', 23)
  await ht100.set('key22', 'value22', 22)
  await ht100.set('key21', 'value21', 21)
  await ht100.set('key20', 'value20', 20)
  await ht100.set('key19', 'value19', 19)
  await ht100.set('key18', 'value18', 18)
  await ht100.set('key17', 'value17', 17)
  await ht100.set('key16', 'value16', 16)
  await ht100.set('key15', 'value15', 15)
  await ht100.set('key14', 'value14', 14)
  await ht100.set('key13', 'value13', 13)
  await ht100.set('key12', 'value12', 12)
  await ht100.set('key11', 'value11', 11)
  await ht100.set('key10', 'value10', 10)
  await ht100.set('key9', 'value9', 9)
  await ht100.set('key8', 'value8', 8)
  await ht100.set('key7', 'value7', 7)
  await ht100.set('key6', 'value6', 6)
  await ht100.set('key5', 'value5', 5)
  await ht100.set('key4', 'value4', 4)
  await ht100.set('key3', 'value3', 3)
  await ht100.set('key2', 'value2', 2)
  await ht100.set('key1', 'value1', 1)

  await ht100.delete('key23')

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
      11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
      21, 22, 24, 25, 26, 27, 28, 29, 30,
      31, 32, 33,
    ])
  }

  await ht100.delete('key19')

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
      11, 12, 13, 14, 15, 16, 17, 18, 20,
      21, 22, 24, 25, 26, 27, 28, 29, 30,
      31, 32, 33,
    ])
  }

  await ht100.delete('key25')

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
      11, 12, 13, 14, 15, 16, 17, 18, 20,
      21, 22, 24, 26, 27, 28, 29, 30,
      31, 32, 33,
    ])
  }

  await ht100.delete('key24')

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
      11, 12, 13, 14, 15, 16, 17, 18, 20,
      21, 22, 26, 27, 28, 29, 30,
      31, 32, 33,
    ])
  }

  ht100.close()
}).case()

const test14_3 = new Test('DiskSortedHashTable', async function integration14_3() {
  const ht100 = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/10`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/10_header`,
    initialLength: 100,
    sortValueType: 'number',
    resizeRatio: 0,
  })
  await ht100.destroy()
  await ht100.init()

  await ht100.set('key33', 'value33', 33)
  await ht100.set('key32', 'value32', 32)
  await ht100.set('key31', 'value31', 31)
  await ht100.set('key30', 'value30', 30)
  await ht100.set('key29', 'value29', 29)
  await ht100.set('key28', 'value28', 28)
  await ht100.set('key27', 'value27', 27)
  await ht100.set('key26', 'value26', 26)
  await ht100.set('key25', 'value25', 25)
  await ht100.set('key24', 'value24', 24)
  await ht100.set('key23', 'value23', 23)
  await ht100.set('key22', 'value22', 22)
  await ht100.set('key21', 'value21', 21)
  await ht100.set('key20', 'value20', 20)
  await ht100.set('key19', 'value19', 19)
  await ht100.set('key18', 'value18', 18)
  await ht100.set('key17', 'value17', 17)
  await ht100.set('key16', 'value16', 16)
  await ht100.set('key15', 'value15', 15)
  await ht100.set('key14', 'value14', 14)
  await ht100.set('key13', 'value13', 13)
  await ht100.set('key12', 'value12', 12)
  await ht100.set('key11', 'value11', 11)
  await ht100.set('key10', 'value10', 10)
  await ht100.set('key9', 'value9', 9)
  await ht100.set('key8', 'value8', 8)
  await ht100.set('key7', 'value7', 7)
  await ht100.set('key6', 'value6', 6)
  await ht100.set('key5', 'value5', 5)
  await ht100.set('key4', 'value4', 4)
  await ht100.set('key3', 'value3', 3)
  await ht100.set('key2', 'value2', 2)
  await ht100.set('key1', 'value1', 1)

  await ht100.delete('key19')

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
      11, 12, 13, 14, 15, 16, 17, 18, 20,
      21, 22, 23, 24, 25, 26, 27, 28, 29, 30,
      31, 32, 33,
    ])
  }

  ht100.close()
}).case()

const test14_4 = new Test('DiskSortedHashTable', async function integration14_4() {
  const ht100 = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/10`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/10_header`,
    initialLength: 100,
    sortValueType: 'number',
    resizeRatio: 0,
  })
  await ht100.destroy()
  await ht100.init()

  await ht100.set('key330', 'value330', 330)
  await ht100.set('key320', 'value320', 320)
  await ht100.set('key310', 'value310', 310)
  await ht100.set('key300', 'value300', 300)
  await ht100.set('key290', 'value290', 290)
  await ht100.set('key280', 'value280', 280)
  await ht100.set('key270', 'value270', 270)
  await ht100.set('key260', 'value26', 260)
  await ht100.set('key250', 'value250', 250)
  await ht100.set('key240', 'value240', 240)
  await ht100.set('key230', 'value230', 230)
  await ht100.set('key220', 'value220', 220)
  await ht100.set('key210', 'value210', 210)
  await ht100.set('key200', 'value200', 200)
  await ht100.set('key190', 'value190', 190)
  await ht100.set('key18', 'value18', 18)
  await ht100.set('key17', 'value17', 17)
  await ht100.set('key16', 'value16', 16)
  await ht100.set('key15', 'value15', 15)
  await ht100.set('key14', 'value14', 14)
  await ht100.set('key13', 'value13', 13)
  await ht100.set('key12', 'value12', 12)
  await ht100.set('key11', 'value11', 11)
  await ht100.set('key10', 'value10', 10)
  await ht100.set('key9', 'value9', 9)
  await ht100.set('key8', 'value8', 8)
  await ht100.set('key7', 'value7', 7)
  await ht100.set('key6', 'value6', 6)
  await ht100.set('key5', 'value5', 5)
  await ht100.set('key4', 'value4', 4)
  await ht100.set('key3', 'value3', 3)
  await ht100.set('key2', 'value2', 2)
  await ht100.set('key1', 'value1', 1)

  assert.strictEqual(ht100.count(), 33)

  await ht100.set('key251', 'value251', 251)
  await ht100.set('key252', 'value252', 252)
  await ht100.set('key253', 'value253', 253)
  await ht100.set('key254', 'value254', 254)
  await ht100.set('key255', 'value255', 255)
  await ht100.set('key256', 'value256', 256)
  await ht100.set('key257', 'value257', 257)

  assert.strictEqual(ht100.count(), 40)

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
      11, 12, 13, 14, 15, 16, 17, 18, 190, 200,
      210, 220, 230, 240, 250, 251, 252, 253, 254, 255, 256, 257, 260, 270, 280, 290, 300,
      310, 320, 330,
    ])
  }

  await ht100.delete('key190')

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
      11, 12, 13, 14, 15, 16, 17, 18, 200,
      210, 220, 230, 240, 250, 251, 252, 253, 254, 255, 256, 257, 260, 270, 280, 290, 300,
      310, 320, 330,
    ])
  }

  await ht100.delete('key210')

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
      11, 12, 13, 14, 15, 16, 17, 18, 200,
      220, 230, 240, 250, 251, 252, 253, 254, 255, 256, 257, 260, 270, 280, 290, 300,
      310, 320, 330,
    ])
  }

  await ht100.delete('key255')

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
      11, 12, 13, 14, 15, 16, 17, 18, 200,
      220, 230, 240, 250, 251, 252, 253, 254, 256, 257, 260, 270, 280, 290, 300,
      310, 320, 330,
    ])
  }

  await ht100.delete('key256')

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
      11, 12, 13, 14, 15, 16, 17, 18, 200,
      220, 230, 240, 250, 251, 252, 253, 254, 257, 260, 270, 280, 290, 300,
      310, 320, 330,
    ])
  }

  await ht100.delete('key254')

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
      11, 12, 13, 14, 15, 16, 17, 18, 200,
      220, 230, 240, 250, 251, 252, 253, 257, 260, 270, 280, 290, 300,
      310, 320, 330,
    ])
  }

  await ht100.delete('key252')

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
      11, 12, 13, 14, 15, 16, 17, 18, 200,
      220, 230, 240, 250, 251, 253, 257, 260, 270, 280, 290, 300,
      310, 320, 330,
    ])

    assertBalanced(btreeRootNode)
  }

  ht100.close()
}).case()

const test14_5 = new Test('DiskSortedHashTable', async function integration14_5() {
  const ht100 = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/10`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/10_header`,
    initialLength: 100,
    sortValueType: 'number',
    resizeRatio: 0,
  })
  await ht100.destroy()
  await ht100.init()

  // await ht100.set('key1', 'value1', 1)
  // await ht100.set('key2', 'value2', 2)
  // await ht100.set('key3', 'value3', 3)
  // await ht100.set('key4', 'value4', 4)
  // await ht100.set('key5', 'value5', 5)
  // await ht100.set('key6', 'value6', 6)
  // await ht100.set('key7', 'value7', 7)
  // await ht100.set('key8', 'value8', 8)
  await ht100.set('key9', 'value9', 9)
  await ht100.set('key10', 'value10', 10)
  await ht100.set('key11', 'value11', 11)
  await ht100.set('key12', 'value12', 12)
  await ht100.set('key13', 'value13', 13)
  await ht100.set('key14', 'value14', 14)
  await ht100.set('key15', 'value15', 15)
  await ht100.set('key16', 'value16', 16)
  await ht100.set('key17', 'value17', 17)
  await ht100.set('key18', 'value18', 18)
  await ht100.set('key190', 'value190', 190)
  await ht100.set('key200', 'value200', 200)
  await ht100.set('key210', 'value210', 210)
  await ht100.set('key220', 'value220', 220)
  await ht100.set('key230', 'value230', 230)
  await ht100.set('key240', 'value240', 240)
  await ht100.set('key250', 'value250', 250)
  await ht100.set('key260', 'value260', 260)
  await ht100.set('key270', 'value270', 270)
  await ht100.set('key280', 'value280', 280)
  await ht100.set('key290', 'value290', 290)
  await ht100.set('key300', 'value300', 300)
  await ht100.set('key310', 'value310', 310)
  await ht100.set('key320', 'value320', 320)
  await ht100.set('key330', 'value330', 330)

  assert.strictEqual(ht100.count(), 25)

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      9, 10,
      11, 12, 13, 14, 15, 16, 17, 18, 190, 200,
      210, 220, 230, 240, 250, 260, 270, 280, 290, 300,
      310, 320, 330,
    ])
  }

  await ht100.delete('key11')

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      9, 10,
      12, 13, 14, 15, 16, 17, 18, 190, 200,
      210, 220, 230, 240, 250, 260, 270, 280, 290, 300,
      310, 320, 330,
    ])
  }

  await ht100.delete('key250')

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      9, 10,
      12, 13, 14, 15, 16, 17, 18, 190, 200,
      210, 220, 230, 240, 260, 270, 280, 290, 300,
      310, 320, 330,
    ])
  }

  await ht100.delete('key190')

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      9, 10,
      12, 13, 14, 15, 16, 17, 18, 200,
      210, 220, 230, 240, 260, 270, 280, 290, 300,
      310, 320, 330,
    ])
  }

  await ht100.delete('key18')

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      9, 10,
      12, 13, 14, 15, 16, 17, 200,
      210, 220, 230, 240, 260, 270, 280, 290, 300,
      310, 320, 330,
    ])
  }

  await ht100.delete('key17')

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      9, 10,
      12, 13, 14, 15, 16, 200,
      210, 220, 230, 240, 260, 270, 280, 290, 300,
      310, 320, 330,
    ])
  }


  await ht100.delete('key230')

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      9, 10,
      12, 13, 14, 15, 16, 200,
      210, 220, 240, 260, 270, 280, 290, 300,
      310, 320, 330,
    ])
  }

  await ht100.delete('key15')

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      9, 10,
      12, 13, 14, 16, 200,
      210, 220, 240, 260, 270, 280, 290, 300,
      310, 320, 330,
    ])
  }

  await ht100.delete('key16')

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      9, 10,
      12, 13, 14, 200,
      210, 220, 240, 260, 270, 280, 290, 300,
      310, 320, 330,
    ])
  }


  await ht100.delete('key310')

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      9, 10,
      12, 13, 14, 200,
      210, 220, 240, 260, 270, 280, 290, 300,
      320, 330,
    ])
  }

  await ht100.delete('key320')

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      9, 10,
      12, 13, 14, 200,
      210, 220, 240, 260, 270, 280, 290, 300,
      330,
    ])
  }

  await ht100.delete('key330')

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      9, 10,
      12, 13, 14, 200,
      210, 220, 240, 260, 270, 280, 290, 300,
    ])
  }

  await ht100.delete('key290')

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      9, 10,
      12, 13, 14, 200,
      210, 220, 240, 260, 270, 280, 300,
    ])
  }

  await ht100.delete('key300')

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      9, 10,
      12, 13, 14, 200,
      210, 220, 240, 260, 270, 280,
    ])
  }

  await ht100.delete('key9')

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      10,
      12, 13, 14, 200,
      210, 220, 240, 260, 270, 280,
    ])
  }

  await ht100.delete('key10')

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      12, 13, 14, 200,
      210, 220, 240, 260, 270, 280,
    ])
  }

  await ht100.delete('key12')

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      13, 14, 200,
      210, 220, 240, 260, 270, 280,
    ])
  }

  await ht100.delete('key13')

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      14, 200,
      210, 220, 240, 260, 270, 280,
    ])
  }

  await ht100.delete('key14')

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      200, 210, 220, 240, 260, 270, 280,
    ])
  }

  await ht100.delete('key260')

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      200, 210, 220, 240, 270, 280,
    ])
  }

  await ht100.delete('key240')

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      200, 210, 220, 270, 280,
    ])
  }

  await ht100.delete('key210')

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      200, 220, 270, 280,
    ])
  }

  await ht100.delete('key270')

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      200, 220, 280,
    ])
  }

  await ht100.delete('key220')

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      200, 280,
    ])
  }

  await ht100.delete('key200')

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [
      280,
    ])
  }

  await ht100.delete('key280')

  {
    const btreeRootNode = await constructBTree(ht100, { unique: false })
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => item.sortValue), [])
  }

  ht100.close()
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

  await ht10.set('key33', 'value33', 33)

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

  await ht10.set('key32', 'value32', 32)
  await ht10.set('key31', 'value31', 31)

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

  await ht10.set('key30', 'value30', 30)
  await ht10.set('key29', 'value29', 29)
  await ht10.set('key28', 'value28', 28)

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

  await ht10.set('key27', 'value27', 27)

  assert.equal(ht10._count, 7)
  assert.equal(ht10._length, 10)

  await ht10.set('key26', 'value26', 26)

  assert.equal(ht10._count, 8)
  assert.equal(ht10._length, 10)

  await ht10.set('key25', 'value25', 25)

  assert.equal(ht10._count, 9)
  assert.equal(ht10._length, 20)

  await ht10.set('key24', 'value24', 24)

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

  await ht10.set('key23', 'value23', 23)
  await ht10.set('key22', 'value22', 22)
  await ht10.set('key21', 'value21', 21)
  await ht10.set('key20', 'value20', 20)
  await ht10.set('key19', 'value19', 19)
  await ht10.set('key18', 'value18', 18)

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

  await ht10.set('key17', 'value17', 17)

  assert.equal(ht10._count, 17)
  assert.equal(ht10._length, 40)

  await ht10.set('key16', 'value16', 16)
  await ht10.set('key15', 'value15', 15)
  await ht10.set('key14', 'value14', 14)
  await ht10.set('key13', 'value13', 13)
  await ht10.set('key12', 'value12', 12)
  await ht10.set('key11', 'value11', 11)
  await ht10.set('key10', 'value10', 10)
  await ht10.set('key9', 'value9', 9)
  await ht10.set('key8', 'value8', 8)
  await ht10.set('key7', 'value7', 7)
  await ht10.set('key6', 'value6', 6)
  await ht10.set('key5', 'value5', 5)
  await ht10.set('key4', 'value4', 4)
  await ht10.set('key3', 'value3', 3)
  await ht10.set('key2', 'value2', 2)

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

  await ht10.set('key1', 'value1', 1)

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

  await ht100.set('key33', 'value33', 33)
  await ht100.set('key1', 'value1', 1)
  await ht100.set('key32', 'value32', 32)

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

  await ht100.set('key2', 'value2', 2)
  await ht100.set('key31', 'value31', 31)
  await ht100.set('key3', 'value3', 3)
  await ht100.set('key30', 'value30', 30)
  await ht100.set('key4', 'value4', 4)
  await ht100.set('key29', 'value29', 29)
  await ht100.set('key5', 'value5', 5)

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

  await ht100.set('key28', 'value28', 28)
  await ht100.set('key6', 'value6', 6)
  await ht100.set('key27', 'value27', 27)

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

  await ht100.set('key7', 'value7', 7)
  await ht100.set('key26', 'value26', 26)
  await ht100.set('key8', 'value8', 8)

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

  await ht100.set('key25', 'value25', 25)
  await ht100.set('key9', 'value9', 9)
  await ht100.set('key24', 'value24', 24)
  await ht100.set('key10', 'value10', 10)
  await ht100.set('key23', 'value23', 23)
  await ht100.set('key11', 'value11', 11)
  await ht100.set('key22', 'value22', 22)
  await ht100.set('key12', 'value12', 12)
  await ht100.set('key21', 'value21', 21)
  await ht100.set('key13', 'value13', 13)
  await ht100.set('key20', 'value20', 20)
  await ht100.set('key14', 'value14', 14)
  await ht100.set('key19', 'value19', 19)
  await ht100.set('key15', 'value15', 15)
  await ht100.set('key18', 'value18', 18)
  await ht100.set('key16', 'value16', 16)
  await ht100.set('key17', 'value17', 17)

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

const test17_2 = new Test('DiskSortedHashTable', async function integration17_2() {
  const randomNumbers = require('./test/randomNumbers1023_1.json')
  const sortedNumbers = [...randomNumbers].sort((a, b) => a - b)
  assert.equal(sortedNumbers.length, 1023)

  const ht = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/1024`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/1024_header`,
    initialLength: 1023 * 8,
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
      const start1 = performance.now()
      await ht.set(`key${n1}`, `value${n1}`, n1)
      console.log('set', `key${n1}`, `value${n1}`, n1, 'in', `${performance.now() - start1}ms`)
      values.push(`value${n1}`)
    } else {
      const start1 = performance.now()
      await ht.set(`key${n1}`, `value${n1}`, n1)
      console.log('set', `key${n1}`, `value${n1}`, n1, 'in', `${performance.now() - start1}ms`)
      const start2 = performance.now()
      await ht.set(`key${n2}`, `value${n2}`, n2)
      console.log('set', `key${n2}`, `value${n2}`, n2, 'in', `${performance.now() - start2}ms`)
      values.push(`value${n1}`)
      values.push(`value${n2}`)
    }
    n1 += 1
    n2 -= 1
  }

  const btreeRootNode = await constructBTree(ht, { unique: false })
  assertBalanced(btreeRootNode)
  assertMinHeight(btreeRootNode, calculateMinBTreeHeight(1023, 2))
  assertMaxHeight(btreeRootNode, calculateMaxBTreeHeight(1023, 2))
  assertMinKeysPerNode(btreeRootNode, 1)
  assertMaxKeysPerNode(btreeRootNode, 3)
  assertInternalNodesIntegrity(btreeRootNode)
  assert.equal(ht.count(), 1023)

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
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => Number(item.sortValue)), sortedNumbers)
  }

  {
    const reverseValues = []
    for await (const value of ht.reverseIterator()) {
      reverseValues.push(value)
    }
    assert.deepEqual(reverseValues, values.reverse())
  }

  ht.close()
}).case()

const test17_3 = new Test('DiskSortedHashTable', async function integration17_3() {
  const randomNumbers = require('./test/randomNumbers1023_1.json')
  const sortedNumbers = [...randomNumbers].sort((a, b) => a - b)
  assert.equal(sortedNumbers.length, 1023)

  const ht = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/1024`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/1024_header`,
    initialLength: 1023 * 8,
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
      const start1 = performance.now()
      await ht.set(`key${n1}`, `value${n1}`, n1)
      console.log('set', `key${n1}`, `value${n1}`, n1, 'in', `${performance.now() - start1}ms`)
      values.push(`value${n1}`)
    } else {
      const start1 = performance.now()
      await ht.set(`key${n1}`, `value${n1}`, n1)
      console.log('set', `key${n1}`, `value${n1}`, n1, 'in', `${performance.now() - start1}ms`)
      const start2 = performance.now()
      await ht.set(`key${n2}`, `value${n2}`, n2)
      console.log('set', `key${n2}`, `value${n2}`, n2, 'in', `${performance.now() - start2}ms`)
      values.push(`value${n1}`)
      values.push(`value${n2}`)
    }
    n1 -= 1
    n2 += 1
  }

  const btreeRootNode = await constructBTree(ht, { unique: false })
  assertBalanced(btreeRootNode)
  assertMinHeight(btreeRootNode, calculateMinBTreeHeight(1023, 2))
  assertMaxHeight(btreeRootNode, calculateMaxBTreeHeight(1023, 2))
  assertMinKeysPerNode(btreeRootNode, 1)
  assertMaxKeysPerNode(btreeRootNode, 3)
  assertInternalNodesIntegrity(btreeRootNode)
  assert.equal(ht.count(), 1023)

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
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => Number(item.sortValue)), sortedNumbers)
  }

  {
    const reverseValues = []
    for await (const value of ht.reverseIterator()) {
      reverseValues.push(value)
    }
    assert.deepEqual(reverseValues, values.reverse())
  }

  ht.close()
}).case()

const test17_6 = new Test('DiskSortedHashTable', async function integration17_6() {
  const randomNumbers = require('./test/randomNumbers1023_1.json')
  const sortedNumbers = [...randomNumbers].sort((a, b) => a - b)
  assert.equal(sortedNumbers.length, 1023)

  const ht = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/1024`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/1024_header`,
    initialLength: 1023 * 8,
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
      const start1 = performance.now()
      await ht.set(`key${n1}`, `value${n1}`, n1)
      console.log('set', `key${n1}`, `value${n1}`, n1, 'in', `${performance.now() - start1}ms`)
      values.push(`value${n1}`)
    } else {
      const start1 = performance.now()
      await ht.set(`key${n1}`, `value${n1}`, n1)
      console.log('set', `key${n1}`, `value${n1}`, n1, 'in', `${performance.now() - start1}ms`)
      const start2 = performance.now()
      await ht.set(`key${n2}`, `value${n2}`, n2)
      console.log('set', `key${n2}`, `value${n2}`, n2, 'in', `${performance.now() - start2}ms`)
      values.push(`value${n1}`)
      values.push(`value${n2}`)
    }
    n1 += 1
    n2 -= 1
  }

  const btreeRootNode = await constructBTree(ht, { unique: false })
  assertBalanced(btreeRootNode)
  assertMinHeight(btreeRootNode, calculateMinBTreeHeight(1023, 3))
  assertMaxHeight(btreeRootNode, calculateMaxBTreeHeight(1023, 3))
  assertMinKeysPerNode(btreeRootNode, 2)
  assertMaxKeysPerNode(btreeRootNode, 5)
  assertInternalNodesIntegrity(btreeRootNode)
  assert.equal(ht.count(), 1023)

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
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => Number(item.sortValue)), sortedNumbers)
  }

  {
    const reverseValues = []
    for await (const value of ht.reverseIterator()) {
      reverseValues.push(value)
    }
    assert.deepEqual(reverseValues, values.reverse())
  }

  ht.close()
}).case()

const test17_7_0 = new Test('DiskSortedHashTable', async function integration17_7_0() {
  const randomNumbers = require('./test/randomNumbers127_1.json')
  const sortedNumbers = [...randomNumbers].sort((a, b) => a - b)
  assert.equal(sortedNumbers.length, 127)

  const ht = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/127`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/127_header`,
    initialLength: 127 * 8,
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
      const start1 = performance.now()
      await ht.set(`key${n1}`, `value${n1}`, n1)
      console.log('set', `key${n1}`, `value${n1}`, n1, 'in', `${performance.now() - start1}ms`)
      values.push(`value${n1}`)
    } else {
      const start1 = performance.now()
      await ht.set(`key${n1}`, `value${n1}`, n1)
      console.log('set', `key${n1}`, `value${n1}`, n1, 'in', `${performance.now() - start1}ms`)
      const start2 = performance.now()
      await ht.set(`key${n2}`, `value${n2}`, n2)
      console.log('set', `key${n2}`, `value${n2}`, n2, 'in', `${performance.now() - start2}ms`)
      values.push(`value${n1}`)
      values.push(`value${n2}`)
    }
    n1 -= 1
    n2 += 1
  }

  const btreeRootNode = await constructBTree(ht, { unique: false })
  assertBalanced(btreeRootNode)
  assertMinHeight(btreeRootNode, calculateMinBTreeHeight(127, 3))
  assertMaxHeight(btreeRootNode, calculateMaxBTreeHeight(127, 3))
  assertMinKeysPerNode(btreeRootNode, 2)
  assertMaxKeysPerNode(btreeRootNode, 5)
  assertInternalNodesIntegrity(btreeRootNode)
  assert.equal(ht.count(), 127)

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
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => Number(item.sortValue)), sortedNumbers)
  }

  {
    const reverseValues = []
    for await (const value of ht.reverseIterator()) {
      reverseValues.push(value)
    }
    assert.deepEqual(reverseValues, values.reverse())
  }

  ht.close()
}).case()

const test17_7_1 = new Test('DiskSortedHashTable', async function integration17_7_1() {
  const randomNumbers = require('./test/randomNumbers127_1.json')
  const sortedNumbers = [...randomNumbers].sort((a, b) => a - b)
  assert.equal(sortedNumbers.length, 127)

  const ht = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/127`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/127_header`,
    initialLength: 127 * 8,
    sortValueType: 'number',
    resizeRatio: 0,
    degree: 2,
  })
  await ht.destroy()
  await ht.init()

  const values = []
  let n1 = 127
  let n2 = 1
  while (n1 >= 64) {
    if (n1 == 64) {
      const start1 = performance.now()
      await ht.set(`key${n1}`, `value${n1}`, n1)
      console.log('set', `key${n1}`, `value${n1}`, n1, 'in', `${performance.now() - start1}ms`)
      values.push(`value${n1}`)
    } else {
      const start1 = performance.now()
      await ht.set(`key${n1}`, `value${n1}`, n1)
      console.log('set', `key${n1}`, `value${n1}`, n1, 'in', `${performance.now() - start1}ms`)
      const start2 = performance.now()
      console.log('set', `key${n2}`, `value${n2}`, n2, 'in', `${performance.now() - start2}ms`)
      await ht.set(`key${n2}`, `value${n2}`, n2)
      values.push(`value${n1}`)
      values.push(`value${n2}`)
    }
    n1 -= 1
    n2 += 1
  }

  const btreeRootNode = await constructBTree(ht, { unique: false })
  assertBalanced(btreeRootNode)
  assertMinHeight(btreeRootNode, calculateMinBTreeHeight(127, 2))
  assertMaxHeight(btreeRootNode, calculateMaxBTreeHeight(127, 2))
  assertMinKeysPerNode(btreeRootNode, 1)
  assertMaxKeysPerNode(btreeRootNode, 3)
  assertInternalNodesIntegrity(btreeRootNode)
  assert.equal(ht.count(), 127)

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
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => Number(item.sortValue)), sortedNumbers)
  }

  {
    const reverseValues = []
    for await (const value of ht.reverseIterator()) {
      reverseValues.push(value)
    }
    assert.deepEqual(reverseValues, values.reverse())
  }

  ht.close()
}).case()

const test17_7_2 = new Test('DiskSortedHashTable', async function integration17_7_2() {
  const randomNumbers = require('./test/randomNumbers127_1.json')
  const sortedNumbers = [...randomNumbers].sort((a, b) => a - b)
  assert.equal(sortedNumbers.length, 127)

  const ht = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/127`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/127_header`,
    initialLength: 127 * 8,
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
      const start1 = performance.now()
      await ht.set(`key${n1}`, `value${n1}`, n1)
      console.log('set', `key${n1}`, `value${n1}`, n1, 'in', `${performance.now() - start1}ms`)
      values.push(`value${n1}`)
    } else {
      const start1 = performance.now()
      await ht.set(`key${n1}`, `value${n1}`, n1)
      console.log('set', `key${n1}`, `value${n1}`, n1, 'in', `${performance.now() - start1}ms`)
      const start2 = performance.now()
      await ht.set(`key${n2}`, `value${n2}`, n2)
      console.log('set', `key${n2}`, `value${n2}`, n2, 'in', `${performance.now() - start2}ms`)
      values.push(`value${n1}`)
      values.push(`value${n2}`)
    }
    n1 -= 1
    n2 += 1
  }

  const btreeRootNode = await constructBTree(ht, { unique: false })
  assertBalanced(btreeRootNode)
  assertMinHeight(btreeRootNode, calculateMinBTreeHeight(127, 3))
  assertMaxHeight(btreeRootNode, calculateMaxBTreeHeight(127, 3))
  assertMinKeysPerNode(btreeRootNode, 2)
  assertMaxKeysPerNode(btreeRootNode, 5)
  assertInternalNodesIntegrity(btreeRootNode)
  assert.equal(ht.count(), 127)

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
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => Number(item.sortValue)), sortedNumbers)
  }

  {
    const reverseValues = []
    for await (const value of ht.reverseIterator()) {
      reverseValues.push(value)
    }
    assert.deepEqual(reverseValues, values.reverse())
  }

  ht.close()
}).case()

const test18 = new Test('DiskSortedHashTable', async function integration18() {
  const randomNumbers = require('./test/randomNumbers127_1.json')
  const sortedNumbers = [...randomNumbers].sort((a, b) => a - b)
  assert.equal(sortedNumbers.length, 127)

  const ht = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/127`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/127_header`,
    initialLength: 127 * 8,
    sortValueType: 'number',
    resizeRatio: 0,
    degree: 2,
  })
  await ht.destroy()
  await ht.init()

  const values = []
  let n = 1
  while (n < 128) {
    const start = performance.now()
    await ht.set(`key${n}`, `value${n}`, n)
    console.log('set', `key${n}`, `value${n}`, n, 'in', `${performance.now() - start}ms`)
    values.push(`value${n}`)
    n += 1
  }

  const btreeRootNode = await constructBTree(ht, { unique: false })
  assertBalanced(btreeRootNode)
  assertMinHeight(btreeRootNode, calculateMinBTreeHeight(127, 2))
  assertMaxHeight(btreeRootNode, calculateMaxBTreeHeight(127, 2))
  assertMinKeysPerNode(btreeRootNode, 1)
  assertMaxKeysPerNode(btreeRootNode, 3)
  assertInternalNodesIntegrity(btreeRootNode)
  assert.equal(ht.count(), 127)

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
    const forwardValues = []
    for await (const value of ht.forwardIterator({ startingSortValue: 11, endingSortValue: 117 })) {
      forwardValues.push(value)
    }
    assert.deepEqual(forwardValues, values.slice(10, -10))
  }

  {
    const forwardValues = []
    for await (const value of ht.forwardIterator({ exclusiveStartKey: 'key10', endingSortValue: 117 })) {
      forwardValues.push(value)
    }
    assert.deepEqual(forwardValues, values.slice(10, -10))
  }

  {
    const forwardValues = []
    for await (const value of ht.forwardIterator({ startingSortValue: 117 })) {
      forwardValues.push(value)
    }
    assert.deepEqual(forwardValues, values.slice(-11))
  }

  {
    const forwardValues = []
    for await (const value of ht.forwardIterator({ endingSortValue: 11 })) {
      forwardValues.push(value)
    }
    assert.deepEqual(forwardValues, values.slice(0, 11))
  }

  {
    const items = traverseInOrder(btreeRootNode)
    assert.deepEqual(items.map(item => Number(item.sortValue)), sortedNumbers)
  }

  const valuesReverse = [...values].reverse()

  {
    const reverseValues = []
    for await (const value of ht.reverseIterator()) {
      reverseValues.push(value)
    }
    assert.deepEqual(reverseValues, valuesReverse)
  }

  {
    const reverseValues = []
    for await (const value of ht.reverseIterator({ startingSortValue: 117, endingSortValue: 11 })) {
      reverseValues.push(value)
    }
    assert.deepEqual(reverseValues, valuesReverse.slice(10, -10))
  }

  {
    const reverseValues = []
    for await (const value of ht.reverseIterator({ exclusiveStartKey: 'key118', endingSortValue: 11 })) {
      reverseValues.push(value)
    }
    assert.deepEqual(reverseValues, valuesReverse.slice(10, -10))
  }

  {
    const reverseValues = []
    for await (const value of ht.reverseIterator({ startingSortValue: 11 })) {
      reverseValues.push(value)
    }
    assert.deepEqual(reverseValues, valuesReverse.slice(-11))
  }

  {
    const reverseValues = []
    for await (const value of ht.reverseIterator({ endingSortValue: 117 })) {
      reverseValues.push(value)
    }
    assert.deepEqual(reverseValues, valuesReverse.slice(0, 11))
  }

  await ht.delete('key17')

  {
    const forwardValues = []
    for await (const value of ht.forwardIterator({ exclusiveStartKey: 'key17' })) {
      forwardValues.push(value)
    }
    assert.deepEqual(forwardValues, [])
  }

  {
    const reverseValues = []
    for await (const value of ht.reverseIterator({ exclusiveStartKey: 'key17' })) {
      reverseValues.push(value)
    }
    assert.deepEqual(reverseValues, [])
  }

  {
    const forwardValues = []
    for await (const value of ht.forwardIterator({ exclusiveStartKey: 'notfound' })) {
      forwardValues.push(value)
    }
    assert.deepEqual(forwardValues, [])
  }

  {
    const reverseValues = []
    for await (const value of ht.reverseIterator({ exclusiveStartKey: 'notfound' })) {
      reverseValues.push(value)
    }
    assert.deepEqual(reverseValues, [])
  }

  ht.close()
}).case()

const test19 = new Test('DiskSortedHashTable', async function integration19() {
  const ht = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/127`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/127_header`,
    initialLength: 127 * 8,
    sortValueType: 'number',
    resizeRatio: 0,
    degree: 3,
  })
  await ht.destroy()
  await ht.init()

  const sortedNumbers = []
  const values = []

  for (let i = 0; i < 13; i++) {
    await ht.set(`key1${i}`, 'value1', 1)
    sortedNumbers.push(1)
    values.push('value1')
  }
  for (let i = 0; i < 13; i++) {
    await ht.set(`key2${i}`, 'value2', 2)
    sortedNumbers.push(2)
    values.push('value2')
  }
  for (let i = 0; i < 13; i++) {
    await ht.set(`key3${i}`, 'value3', 3)
    sortedNumbers.push(3)
    values.push('value3')
  }

  assert.equal(sortedNumbers.length, 39)

  const btreeRootNode = await constructBTree(ht, { unique: false })
  assertBalanced(btreeRootNode)
  assertMinHeight(btreeRootNode, calculateMinBTreeHeight(39, 3))
  assertMaxHeight(btreeRootNode, calculateMaxBTreeHeight(39, 3))
  assertMinKeysPerNode(btreeRootNode, 2)
  assertMaxKeysPerNode(btreeRootNode, 5)
  assertInternalNodesIntegrity(btreeRootNode)
  assert.equal(ht.count(), 39)

  assert.deepEqual(values, sortedNumbers.map(n => `value${n}`))

  {
    const forwardValues = []
    for await (const value of ht.forwardIterator()) {
      forwardValues.push(value)
    }
    assert.deepEqual(forwardValues, values)
  }

  {
    const forwardValues = []
    for await (const value of ht.forwardIterator({ startingSortValue: 1, endingSortValue: 3 })) {
      forwardValues.push(value)
    }
    assert.deepEqual(forwardValues, values)
  }

  {
    const forwardValues = []
    for await (const value of ht.forwardIterator({ startingSortValue: 1, endingSortValue: 1 })) {
      forwardValues.push(value)
    }
    assert.deepEqual(forwardValues, values.slice(0, 13))
  }

  {
    const forwardValues = []
    for await (const value of ht.forwardIterator({ startingSortValue: 1, endingSortValue: 2 })) {
      forwardValues.push(value)
    }
    assert.deepEqual(forwardValues, values.slice(0, 26))
  }

  {
    const forwardValues = []
    for await (const value of ht.forwardIterator({ startingSortValue: 2, endingSortValue: 2 })) {
      forwardValues.push(value)
    }
    assert.deepEqual(forwardValues, values.slice(13, 26))
  }

  {
    const forwardValues = []
    for await (const value of ht.forwardIterator({ startingSortValue: 2, endingSortValue: 3 })) {
      forwardValues.push(value)
    }
    assert.deepEqual(forwardValues, values.slice(13, 39))
  }

  {
    const forwardValues = []
    for await (const value of ht.forwardIterator({ startingSortValue: 3, endingSortValue: 3 })) {
      forwardValues.push(value)
    }
    assert.deepEqual(forwardValues, values.slice(26, 39))
  }

  const valuesReverse = [...values].reverse()

  {
    const reverseValues = []
    for await (const value of ht.reverseIterator()) {
      reverseValues.push(value)
    }
    assert.deepEqual(reverseValues, valuesReverse)
  }

  {
    const reverseValues = []
    for await (const value of ht.reverseIterator({ startingSortValue: 3, endingSortValue: 1 })) {
      reverseValues.push(value)
    }
    assert.deepEqual(reverseValues, valuesReverse)
  }

  {
    const reverseValues = []
    for await (const value of ht.reverseIterator({ startingSortValue: 1, endingSortValue: 1 })) {
      reverseValues.push(value)
    }
    assert.deepEqual(reverseValues, valuesReverse.slice(26, 39))
  }

  {
    const reverseValues = []
    for await (const value of ht.reverseIterator({ startingSortValue: 2, endingSortValue: 1 })) {
      reverseValues.push(value)
    }
    assert.deepEqual(reverseValues, valuesReverse.slice(13, 39))
  }

  {
    const reverseValues = []
    for await (const value of ht.reverseIterator({ startingSortValue: 2, endingSortValue: 2 })) {
      reverseValues.push(value)
    }
    assert.deepEqual(reverseValues, valuesReverse.slice(13, 26))
  }

  {
    const reverseValues = []
    for await (const value of ht.reverseIterator({ startingSortValue: 3, endingSortValue: 2 })) {
      reverseValues.push(value)
    }
    assert.deepEqual(reverseValues, valuesReverse.slice(0, 26))
  }

  {
    const reverseValues = []
    for await (const value of ht.reverseIterator({ startingSortValue: 3, endingSortValue: 3 })) {
      reverseValues.push(value)
    }
    assert.deepEqual(reverseValues, valuesReverse.slice(0, 13))
  }

  ht.close()
}).case()

const test_root_min_keys = new Test('DiskSortedHashTable', async function integration_root_min_keys() {
  const ht = new DiskSortedHashTable({
    storagePath: `${__dirname}/DiskSortedHashTable_test_data/1023`,
    headerPath: `${__dirname}/DiskSortedHashTable_test_data/1023_header`,
    initialLength: 1023 * 8,
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

  await constructBTree(ht, {
    unique: true,
    onNode({ node }) {
      if (node.root) {
        rootNode = node
      }
    }
  })

  assert.equal(rootNode.items.length, 3)

  assertMinKeysPerNode(rootNode, 4) // ok because the standard minimum keys-per-node rule for b-trees does not apply to the root node

  ht.close()
  await ht.destroy()
}).case()

const test28 = new Test('DiskSortedHashTable', async function integration28() {
  const randomNumbers = require('./test/randomNumbers127_1.json')
  assert.equal(randomNumbers.length, 127)
  const sortedNumbers = [...randomNumbers].sort((a, b) => a - b)
  const sortedNumbersReverse = [...sortedNumbers].reverse()
  const sortedValues = sortedNumbers.map(n => `value${n}`)
  const sortedValuesReverse = sortedNumbersReverse.map(n => `value${n}`)

  const randomNumbersArray = []
  let i = 1
  while (i <= 10) {
    const numbers = require(`./test/randomNumbers127_${i}.json`)
    randomNumbersArray.push(numbers)
    i += 1
  }

  const insertNumbersArray = [sortedNumbers, sortedNumbersReverse, ...randomNumbersArray]
  const deleteNumbersArray = [...insertNumbersArray]

  for (const degree of [2]) {
    for (const deleteNumbers of deleteNumbersArray) {
      const deleteNumbersIndex = deleteNumbersArray.indexOf(deleteNumbers)

      const ht = new DiskSortedHashTable({
        storagePath: `${__dirname}/DiskSortedHashTable_test_data/127`,
        headerPath: `${__dirname}/DiskSortedHashTable_test_data/127_header`,
        initialLength: 127 * 8,
        sortValueType: 'number',
        resizeRatio: 0,
        degree,
      })
      await ht.destroy()
      await ht.init()


      for (const insertNumbers of insertNumbersArray) {
        const insertNumbersIndex = insertNumbersArray.indexOf(insertNumbers)

        for (const n of insertNumbers) {
          const start = performance.now()
          await ht.set(`key${n}`, `value${n}`, n)
          console.log('set', `key${n}`, `value${n}`, n, 'in', `${performance.now() - start}ms`)
        }

        const btreeRootNode = await constructBTree(ht, { unique: false })
        assertBalanced(btreeRootNode)
        assertMinHeight(btreeRootNode, calculateMinBTreeHeight(127, degree))
        assertMaxHeight(btreeRootNode, calculateMaxBTreeHeight(127, degree))
        assertMinKeysPerNode(btreeRootNode, degree - 1)
        assertMaxKeysPerNode(btreeRootNode, (degree * 2) - 1)
        assertInternalNodesIntegrity(btreeRootNode)
        assert.equal(ht.count(), 127)

        {
          const forwardValues = []
          for await (const value of ht.forwardIterator()) {
            forwardValues.push(value)
          }
          assert.deepEqual(forwardValues, sortedValues)
        }

        {
          const items = traverseInOrder(btreeRootNode)
          assert.deepEqual(items.map(item => Number(item.sortValue)), sortedNumbers)
        }

        {
          const reverseValues = []
          for await (const value of ht.reverseIterator()) {
            reverseValues.push(value)
          }
          assert.deepEqual(reverseValues, sortedValuesReverse)
        }

        const sortedNumbers2 = [...sortedNumbers]

        let ndeleted = 0
        for (const n of deleteNumbers) {
          const start = performance.now()
          await ht.delete(`key${n}`)
          console.log(`deleted key${n} in ${performance.now() - start}ms; degree ${degree}; delete numbers index ${deleteNumbersIndex}; insert numbers index ${insertNumbersIndex}`)
          ndeleted += 1

          const btreeRootNode = await constructBTree(ht, { unique: false })
          assertBalanced(btreeRootNode)
          assertMinHeight(btreeRootNode, calculateMinBTreeHeight(127 - ndeleted, degree))
          assertMaxHeight(btreeRootNode, calculateMaxBTreeHeight(127 - ndeleted, degree))
          assertMinKeysPerNode(btreeRootNode, degree - 1)
          assertMaxKeysPerNode(btreeRootNode, (degree * 2) - 1)
          assertInternalNodesIntegrity(btreeRootNode)
          assert.equal(ht.count(), 127 - ndeleted)
          assert.equal(ht._deletedCount, ndeleted)

          sortedNumbers2.splice(sortedNumbers2.indexOf(n), 1)

          {
            const items = traverseInOrder(btreeRootNode)
            assert.deepEqual(items.map(item => Number(item.sortValue)), sortedNumbers2)
          }
        }

        assert.equal(ht.count(), 0)

        await ht.clear()
      }

      ht.close()
      await ht.destroy()
    }
  }

}).case()

const test29 = new Test('DiskSortedHashTable', async function integration29() {
  const randomNumbers = require('./test/randomNumbers511_1.json')
  assert.equal(randomNumbers.length, 511)
  const sortedNumbers = [...randomNumbers].sort((a, b) => a - b)
  const sortedNumbersReverse = [...sortedNumbers].reverse()
  const sortedValues = sortedNumbers.map(n => `value${n}`)
  const sortedValuesReverse = sortedNumbersReverse.map(n => `value${n}`)

  const randomNumbersArray = []
  let i = 1
  while (i <= 10) {
    const numbers = require(`./test/randomNumbers511_${i}.json`)
    randomNumbersArray.push(numbers)
    i += 1
  }

  const insertNumbersArray = [sortedNumbers, sortedNumbersReverse, ...randomNumbersArray]
  const deleteNumbersArray = [...insertNumbersArray]

  for (const degree of [3]) {
    for (const deleteNumbers of deleteNumbersArray) {
      const deleteNumbersIndex = deleteNumbersArray.indexOf(deleteNumbers)

      const ht = new DiskSortedHashTable({
        storagePath: `${__dirname}/DiskSortedHashTable_test_data/511`,
        headerPath: `${__dirname}/DiskSortedHashTable_test_data/511_header`,
        initialLength: 511 * 8,
        sortValueType: 'number',
        resizeRatio: 0,
        degree,
      })
      await ht.destroy()
      await ht.init()

      for (const insertNumbers of insertNumbersArray) {
        const insertNumbersIndex = insertNumbersArray.indexOf(insertNumbers)

        for (const n of insertNumbers) {
          const start = performance.now()
          await ht.set(`key${n}`, `value${n}`, n)
          console.log('set', `key${n}`, `value${n}`, n, 'in', `${performance.now() - start}ms`)
        }

        const btreeRootNode = await constructBTree(ht, { unique: false })
        assertBalanced(btreeRootNode)
        assertMinHeight(btreeRootNode, calculateMinBTreeHeight(511, degree))
        assertMaxHeight(btreeRootNode, calculateMaxBTreeHeight(511, degree))
        assertMinKeysPerNode(btreeRootNode, degree - 1)
        assertMaxKeysPerNode(btreeRootNode, (degree * 2) - 1)
        assertInternalNodesIntegrity(btreeRootNode)
        assert.equal(ht.count(), 511)

        {
          const forwardValues = []
          for await (const value of ht.forwardIterator()) {
            forwardValues.push(value)
          }
          assert.deepEqual(forwardValues, sortedValues)
        }

        {
          const items = traverseInOrder(btreeRootNode)
          assert.deepEqual(items.map(item => Number(item.sortValue)), sortedNumbers)
        }

        {
          const reverseValues = []
          for await (const value of ht.reverseIterator()) {
            reverseValues.push(value)
          }
          assert.deepEqual(reverseValues, sortedValuesReverse)
        }

        const sortedNumbers2 = [...sortedNumbers]

        let ndeleted = 0
        for (const n of deleteNumbers) {
          const start = performance.now()
          await ht.delete(`key${n}`)
          console.log(`deleted key${n} in ${performance.now() - start}ms; degree ${degree}; delete numbers index ${deleteNumbersIndex}; insert numbers index ${insertNumbersIndex}`)
          ndeleted += 1

          const btreeRootNode = await constructBTree(ht, { unique: false })
          assertBalanced(btreeRootNode)
          assertMinHeight(btreeRootNode, calculateMinBTreeHeight(511 - ndeleted, degree))
          assertMaxHeight(btreeRootNode, calculateMaxBTreeHeight(511 - ndeleted, degree))
          assertMinKeysPerNode(btreeRootNode, degree - 1)
          assertMaxKeysPerNode(btreeRootNode, (degree * 2) - 1)
          assertInternalNodesIntegrity(btreeRootNode)
          assert.equal(ht.count(), 511 - ndeleted)
          assert.equal(ht._deletedCount, ndeleted)

          sortedNumbers2.splice(sortedNumbers2.indexOf(n), 1)

          {
            const items = traverseInOrder(btreeRootNode)
            assert.deepEqual(items.map(item => Number(item.sortValue)), sortedNumbers2)
          }
        }

        assert.equal(ht.count(), 0)

        await ht.clear()
      }

      ht.close()
      await ht.destroy()
    }
  }

}).case()

const test30 = new Test('DiskSortedHashTable', async function integration30() {
  const randomNumbers = require('./test/randomNumbers1023_1.json')
  assert.equal(randomNumbers.length, 1023)
  const sortedNumbers = [...randomNumbers].sort((a, b) => a - b)
  const sortedNumbersReverse = [...sortedNumbers].reverse()
  const sortedValues = sortedNumbers.map(n => `value${n}`)
  const sortedValuesReverse = sortedNumbersReverse.map(n => `value${n}`)

  const randomNumbersArray = []
  let i = 1
  while (i <= 10) {
    const numbers = require(`./test/randomNumbers1023_${i}.json`)
    randomNumbersArray.push(numbers)
    i += 1
  }

  const insertNumbersArray = [sortedNumbers, sortedNumbersReverse, ...randomNumbersArray]
  const deleteNumbersArray = [...insertNumbersArray]

  for (const degree of [4]) {
    for (const deleteNumbers of deleteNumbersArray) {
      const deleteNumbersIndex = deleteNumbersArray.indexOf(deleteNumbers)

      const ht = new DiskSortedHashTable({
        storagePath: `${__dirname}/DiskSortedHashTable_test_data/1023`,
        headerPath: `${__dirname}/DiskSortedHashTable_test_data/1023_header`,
        initialLength: 1023 * 8,
        sortValueType: 'number',
        resizeRatio: 0,
        degree,
      })
      await ht.destroy()
      await ht.init()

      for (const insertNumbers of insertNumbersArray) {
        const insertNumbersIndex = insertNumbersArray.indexOf(insertNumbers)

        for (const n of insertNumbers) {
          const start = performance.now()
          await ht.set(`key${n}`, `value${n}`, n)
          console.log('set', `key${n}`, `value${n}`, n, 'in', `${performance.now() - start}ms`)
        }

        const btreeRootNode = await constructBTree(ht, { unique: false })
        assertBalanced(btreeRootNode)
        assertMinHeight(btreeRootNode, calculateMinBTreeHeight(1023, degree))
        assertMaxHeight(btreeRootNode, calculateMaxBTreeHeight(1023, degree))
        assertMinKeysPerNode(btreeRootNode, degree - 1)
        assertMaxKeysPerNode(btreeRootNode, (degree * 2) - 1)
        assertInternalNodesIntegrity(btreeRootNode)
        assert.equal(ht.count(), 1023)

        {
          const forwardValues = []
          for await (const value of ht.forwardIterator()) {
            forwardValues.push(value)
          }
          assert.deepEqual(forwardValues, sortedValues)
        }

        {
          const items = traverseInOrder(btreeRootNode)
          assert.deepEqual(items.map(item => Number(item.sortValue)), sortedNumbers)
        }

        {
          const reverseValues = []
          for await (const value of ht.reverseIterator()) {
            reverseValues.push(value)
          }
          assert.deepEqual(reverseValues, sortedValuesReverse)
        }

        let ndeleted = 0
        for (const n of deleteNumbers) {
          const start = performance.now()
          await ht.delete(`key${n}`)
          console.log(`deleted key${n} in ${performance.now() - start}ms; degree ${degree}; delete numbers index ${deleteNumbersIndex}; insert numbers index ${insertNumbersIndex}`)
          ndeleted += 1
          assert.equal(ht.count(), 1023 - ndeleted)
          assert.equal(ht._deletedCount, ndeleted)
        }

        assert.equal(ht.count(), 0)

        await ht.clear()
      }

      ht.close()
      await ht.destroy()
    }
  }

}).case()

const test31 = new Test('DiskSortedHashTable', async function integration31() {
  const randomNumbers = require('./test/randomNumbers2047_1.json')
  assert.equal(randomNumbers.length, 2047)
  const sortedNumbers = [...randomNumbers].sort((a, b) => a - b)
  const sortedNumbersReverse = [...sortedNumbers].reverse()
  const sortedValues = sortedNumbers.map(n => `value${n}`)
  const sortedValuesReverse = sortedNumbersReverse.map(n => `value${n}`)

  const randomNumbersArray = []
  let i = 1
  while (i <= 10) {
    const numbers = require(`./test/randomNumbers2047_${i}.json`)
    randomNumbersArray.push(numbers)
    i += 1
  }

  const insertNumbersArray = [sortedNumbers, sortedNumbersReverse, ...randomNumbersArray]
  const deleteNumbersArray = [...insertNumbersArray]

  for (const degree of [5]) {
    for (const deleteNumbers of deleteNumbersArray) {
      const deleteNumbersIndex = deleteNumbersArray.indexOf(deleteNumbers)

      const ht = new DiskSortedHashTable({
        storagePath: `${__dirname}/DiskSortedHashTable_test_data/2047`,
        headerPath: `${__dirname}/DiskSortedHashTable_test_data/2047_header`,
        initialLength: 2047 * 8,
        sortValueType: 'number',
        resizeRatio: 0,
        degree,
      })
      await ht.destroy()
      await ht.init()

      for (const insertNumbers of insertNumbersArray) {
        const insertNumbersIndex = insertNumbersArray.indexOf(insertNumbers)

        for (const n of insertNumbers) {
          const start = performance.now()
          await ht.set(`key${n}`, `value${n}`, n)
          console.log('set', `key${n}`, `value${n}`, n, 'in', `${performance.now() - start}ms; degree ${degree}; delete numbers index ${deleteNumbersIndex}; insert numbers index ${insertNumbersIndex}`)
        }

        const btreeRootNode = await constructBTree(ht, { unique: false })
        assertBalanced(btreeRootNode)
        assertMinHeight(btreeRootNode, calculateMinBTreeHeight(2047, degree))
        assertMaxHeight(btreeRootNode, calculateMaxBTreeHeight(2047, degree))
        assertMinKeysPerNode(btreeRootNode, degree - 1)
        assertMaxKeysPerNode(btreeRootNode, (degree * 2) - 1)
        assertInternalNodesIntegrity(btreeRootNode)
        assert.equal(ht.count(), 2047)

        {
          const forwardValues = []
          for await (const value of ht.forwardIterator()) {
            forwardValues.push(value)
          }
          assert.deepEqual(forwardValues, sortedValues)
        }

        {
          const items = traverseInOrder(btreeRootNode)
          assert.deepEqual(items.map(item => Number(item.sortValue)), sortedNumbers)
        }

        {
          const reverseValues = []
          for await (const value of ht.reverseIterator()) {
            reverseValues.push(value)
          }
          assert.deepEqual(reverseValues, sortedValuesReverse)
        }

        const sortedNumbers2 = [...sortedNumbers]

        let ndeleted = 0
        for (const n of deleteNumbers) {
          const start = performance.now()
          await ht.delete(`key${n}`)
          console.log(`deleted key${n} in ${performance.now() - start}ms; degree ${degree}; delete numbers index ${deleteNumbersIndex}; insert numbers index ${insertNumbersIndex}`)
          ndeleted += 1
          assert.equal(ht.count(), 2047 - ndeleted)
          assert.equal(ht._deletedCount, ndeleted)
        }

        assert.equal(ht.count(), 0)

        await ht.clear()
      }

      ht.close()
      await ht.destroy()
    }
  }

}).case()

const test32 = new Test('DiskSortedHashTable', async function integration32() {
  const randomNumbers = require('./test/randomNumbers4095_1.json')
  assert.equal(randomNumbers.length, 4095)
  const sortedNumbers = [...randomNumbers].sort((a, b) => a - b)
  const sortedNumbersReverse = [...sortedNumbers].reverse()
  const sortedValues = sortedNumbers.map(n => `value${n}`)
  const sortedValuesReverse = sortedNumbersReverse.map(n => `value${n}`)

  const randomNumbersArray = []
  let i = 1
  while (i <= 8) {
    const numbers = require(`./test/randomNumbers4095_${i}.json`)
    randomNumbersArray.push(numbers)
    i += 1
  }

  const insertNumbersArray = [sortedNumbers, sortedNumbersReverse, ...randomNumbersArray]
  const deleteNumbersArray = [...insertNumbersArray]

  for (const degree of [6]) {
    for (const deleteNumbers of deleteNumbersArray) {
      const deleteNumbersIndex = deleteNumbersArray.indexOf(deleteNumbers)

      const ht = new DiskSortedHashTable({
        storagePath: `${__dirname}/DiskSortedHashTable_test_data/4095`,
        headerPath: `${__dirname}/DiskSortedHashTable_test_data/4095_header`,
        initialLength: 4095 * 8,
        sortValueType: 'number',
        resizeRatio: 0,
        degree,
      })
      await ht.destroy()
      await ht.init()

      for (const insertNumbers of insertNumbersArray) {
        const insertNumbersIndex = insertNumbersArray.indexOf(insertNumbers)

        for (const n of insertNumbers) {
          const start = performance.now()
          await ht.set(`key${n}`, `value${n}`, n)
          console.log('set', `key${n}`, `value${n}`, n, 'in', `${performance.now() - start}ms; degree ${degree}; delete numbers index ${deleteNumbersIndex}; insert numbers index ${insertNumbersIndex}`)
        }

        const btreeRootNode = await constructBTree(ht, { unique: false })
        assertBalanced(btreeRootNode)
        assertMinHeight(btreeRootNode, calculateMinBTreeHeight(4095, degree))
        assertMaxHeight(btreeRootNode, calculateMaxBTreeHeight(4095, degree))
        assertMinKeysPerNode(btreeRootNode, degree - 1)
        assertMaxKeysPerNode(btreeRootNode, (degree * 2) - 1)
        assertInternalNodesIntegrity(btreeRootNode)
        assert.equal(ht.count(), 4095)

        {
          const forwardValues = []
          for await (const value of ht.forwardIterator()) {
            forwardValues.push(value)
          }
          assert.deepEqual(forwardValues, sortedValues)
        }

        {
          const items = traverseInOrder(btreeRootNode)
          assert.deepEqual(items.map(item => Number(item.sortValue)), sortedNumbers)
        }

        {
          const reverseValues = []
          for await (const value of ht.reverseIterator()) {
            reverseValues.push(value)
          }
          assert.deepEqual(reverseValues, sortedValuesReverse)
        }

        const sortedNumbers2 = [...sortedNumbers]

        let ndeleted = 0
        for (const n of deleteNumbers) {
          const start = performance.now()
          await ht.delete(`key${n}`)
          console.log(`deleted key${n} in ${performance.now() - start}ms; degree ${degree}; delete numbers index ${deleteNumbersIndex}; insert numbers index ${insertNumbersIndex}`)
          ndeleted += 1
          assert.equal(ht.count(), 4095 - ndeleted)
          assert.equal(ht._deletedCount, ndeleted)
        }

        assert.equal(ht.count(), 0)

        await ht.clear()
      }

      ht.close()
      await ht.destroy()
    }
  }

}).case()

const test33 = new Test('DiskSortedHashTable', async function integration33() {
  const randomNumbers = require('./test/randomNumbers8191_1.json')
  assert.equal(randomNumbers.length, 8191)
  const sortedNumbers = [...randomNumbers].sort((a, b) => a - b)
  const sortedNumbersReverse = [...sortedNumbers].reverse()
  const sortedValues = sortedNumbers.map(n => `value${n}`)
  const sortedValuesReverse = sortedNumbersReverse.map(n => `value${n}`)

  const randomNumbersArray = []
  let i = 1
  while (i <= 5) {
    const numbers = require(`./test/randomNumbers8191_${i}.json`)
    randomNumbersArray.push(numbers)
    i += 1
  }

  const insertNumbersArray = [sortedNumbers, sortedNumbersReverse, ...randomNumbersArray]
  const deleteNumbersArray = [...insertNumbersArray]

  for (const degree of [7]) {
    for (const deleteNumbers of deleteNumbersArray) {
      const deleteNumbersIndex = deleteNumbersArray.indexOf(deleteNumbers)

      const ht = new DiskSortedHashTable({
        storagePath: `${__dirname}/DiskSortedHashTable_test_data/8191`,
        headerPath: `${__dirname}/DiskSortedHashTable_test_data/8191_header`,
        initialLength: 8191 * 8,
        sortValueType: 'number',
        resizeRatio: 0,
        degree,
      })
      await ht.destroy()
      await ht.init()

      for (const insertNumbers of insertNumbersArray) {
        const insertNumbersIndex = insertNumbersArray.indexOf(insertNumbers)

        for (const n of insertNumbers) {
          const start = performance.now()
          await ht.set(`key${n}`, `value${n}`, n)
          console.log('set', `key${n}`, `value${n}`, n, 'in', `${performance.now() - start}ms; degree ${degree}; delete numbers index ${deleteNumbersIndex}; insert numbers index ${insertNumbersIndex}`)
        }

        const btreeRootNode = await constructBTree(ht, { unique: false })
        assertBalanced(btreeRootNode)
        assertMinHeight(btreeRootNode, calculateMinBTreeHeight(8191, degree))
        assertMaxHeight(btreeRootNode, calculateMaxBTreeHeight(8191, degree))
        assertMinKeysPerNode(btreeRootNode, degree - 1)
        assertMaxKeysPerNode(btreeRootNode, (degree * 2) - 1)
        assertInternalNodesIntegrity(btreeRootNode)
        assert.equal(ht.count(), 8191)

        {
          const forwardValues = []
          for await (const value of ht.forwardIterator()) {
            forwardValues.push(value)
          }
          assert.deepEqual(forwardValues, sortedValues)
        }

        {
          const items = traverseInOrder(btreeRootNode)
          assert.deepEqual(items.map(item => Number(item.sortValue)), sortedNumbers)
        }

        {
          const reverseValues = []
          for await (const value of ht.reverseIterator()) {
            reverseValues.push(value)
          }
          assert.deepEqual(reverseValues, sortedValuesReverse)
        }

        const sortedNumbers2 = [...sortedNumbers]

        let ndeleted = 0
        for (const n of deleteNumbers) {
          const start = performance.now()
          await ht.delete(`key${n}`)
          console.log(`deleted key${n} in ${performance.now() - start}ms; degree ${degree}; delete numbers index ${deleteNumbersIndex}; insert numbers index ${insertNumbersIndex}`)
          ndeleted += 1
          assert.equal(ht.count(), 8191 - ndeleted)
          assert.equal(ht._deletedCount, ndeleted)
        }

        assert.equal(ht.count(), 0)

        await ht.clear()
      }

      ht.close()
      await ht.destroy()
    }
  }

}).case()

const test34 = new Test('DiskSortedHashTable', async function integration34() {
  const randomNumbers = require('./test/randomNumbers16383_1.json')
  assert.equal(randomNumbers.length, 16383)
  const sortedNumbers = [...randomNumbers].sort((a, b) => a - b)
  const sortedNumbersReverse = [...sortedNumbers].reverse()
  const sortedValues = sortedNumbers.map(n => `value${n}`)
  const sortedValuesReverse = sortedNumbersReverse.map(n => `value${n}`)

  const randomNumbersArray = []
  let i = 1
  while (i <= 3) {
    const numbers = require(`./test/randomNumbers16383_${i}.json`)
    randomNumbersArray.push(numbers)
    i += 1
  }

  const insertNumbersArray = [sortedNumbers, sortedNumbersReverse, ...randomNumbersArray]
  const deleteNumbersArray = [...insertNumbersArray]

  for (const degree of [8]) {
    for (const deleteNumbers of deleteNumbersArray) {
      const deleteNumbersIndex = deleteNumbersArray.indexOf(deleteNumbers)

      const ht = new DiskSortedHashTable({
        storagePath: `${__dirname}/DiskSortedHashTable_test_data/16383`,
        headerPath: `${__dirname}/DiskSortedHashTable_test_data/16383_header`,
        initialLength: 16383 * 8,
        sortValueType: 'number',
        resizeRatio: 0,
        degree,
      })
      await ht.destroy()
      await ht.init()

      for (const insertNumbers of insertNumbersArray) {
        const insertNumbersIndex = insertNumbersArray.indexOf(insertNumbers)

        for (const n of insertNumbers) {
          const start = performance.now()
          await ht.set(`key${n}`, `value${n}`, n)
          console.log('set', `key${n}`, `value${n}`, n, 'in', `${performance.now() - start}ms; degree ${degree}; delete numbers index ${deleteNumbersIndex}; insert numbers index ${insertNumbersIndex}`)
        }

        const btreeRootNode = await constructBTree(ht, { unique: false })
        assertBalanced(btreeRootNode)
        assertMinHeight(btreeRootNode, calculateMinBTreeHeight(16383, degree))
        assertMaxHeight(btreeRootNode, calculateMaxBTreeHeight(16383, degree))
        assertMinKeysPerNode(btreeRootNode, degree - 1)
        assertMaxKeysPerNode(btreeRootNode, (degree * 2) - 1)
        assertInternalNodesIntegrity(btreeRootNode)
        assert.equal(ht.count(), 16383)

        {
          const forwardValues = []
          for await (const value of ht.forwardIterator()) {
            forwardValues.push(value)
          }
          assert.deepEqual(forwardValues, sortedValues)
        }

        {
          const items = traverseInOrder(btreeRootNode)
          assert.deepEqual(items.map(item => Number(item.sortValue)), sortedNumbers)
        }

        {
          const reverseValues = []
          for await (const value of ht.reverseIterator()) {
            reverseValues.push(value)
          }
          assert.deepEqual(reverseValues, sortedValuesReverse)
        }

        const sortedNumbers2 = [...sortedNumbers]

        let ndeleted = 0
        for (const n of deleteNumbers) {
          const start = performance.now()
          await ht.delete(`key${n}`)
          console.log(`deleted key${n} in ${performance.now() - start}ms; degree ${degree}; delete numbers index ${deleteNumbersIndex}; insert numbers index ${insertNumbersIndex}`)
          ndeleted += 1
          assert.equal(ht.count(), 16383 - ndeleted)
          assert.equal(ht._deletedCount, ndeleted)
        }

        assert.equal(ht.count(), 0)

        await ht.clear()
      }

      ht.close()
      await ht.destroy()
    }
  }

}).case()

const test35 = new Test('DiskSortedHashTable', async function integration35() {
  const randomNumbers = require('./test/randomNumbers32767_1.json')
  assert.equal(randomNumbers.length, 32767)
  const sortedNumbers = [...randomNumbers].sort((a, b) => a - b)
  const sortedNumbersReverse = [...sortedNumbers].reverse()
  const sortedValues = sortedNumbers.map(n => `value${n}`)
  const sortedValuesReverse = sortedNumbersReverse.map(n => `value${n}`)

  const randomNumbersArray = []
  let i = 1
  while (i <= 1) {
    const numbers = require(`./test/randomNumbers32767_${i}.json`)
    randomNumbersArray.push(numbers)
    i += 1
  }

  const insertNumbersArray = [sortedNumbers, sortedNumbersReverse, ...randomNumbersArray]
  const deleteNumbersArray = [...insertNumbersArray]

  for (const degree of [9]) {
    for (const deleteNumbers of deleteNumbersArray) {
      const deleteNumbersIndex = deleteNumbersArray.indexOf(deleteNumbers)

      const ht = new DiskSortedHashTable({
        storagePath: `${__dirname}/DiskSortedHashTable_test_data/32767`,
        headerPath: `${__dirname}/DiskSortedHashTable_test_data/32767_header`,
        initialLength: 32767 * 8,
        sortValueType: 'number',
        resizeRatio: 0,
        degree,
      })
      await ht.destroy()
      await ht.init()

      for (const insertNumbers of insertNumbersArray) {
        const insertNumbersIndex = insertNumbersArray.indexOf(insertNumbers)

        for (const n of insertNumbers) {
          const start = performance.now()
          await ht.set(`key${n}`, `value${n}`, n)
          console.log('set', `key${n}`, `value${n}`, n, 'in', `${performance.now() - start}ms; degree ${degree}; delete numbers index ${deleteNumbersIndex}; insert numbers index ${insertNumbersIndex}`)
        }

        const btreeRootNode = await constructBTree(ht, { unique: false })
        assertBalanced(btreeRootNode)
        assertMinHeight(btreeRootNode, calculateMinBTreeHeight(32767, degree))
        assertMaxHeight(btreeRootNode, calculateMaxBTreeHeight(32767, degree))
        assertMinKeysPerNode(btreeRootNode, degree - 1)
        assertMaxKeysPerNode(btreeRootNode, (degree * 2) - 1)
        assertInternalNodesIntegrity(btreeRootNode)
        assert.equal(ht.count(), 32767)

        {
          const forwardValues = []
          for await (const value of ht.forwardIterator()) {
            forwardValues.push(value)
          }
          assert.deepEqual(forwardValues, sortedValues)
        }

        {
          const items = traverseInOrder(btreeRootNode)
          assert.deepEqual(items.map(item => Number(item.sortValue)), sortedNumbers)
        }

        {
          const reverseValues = []
          for await (const value of ht.reverseIterator()) {
            reverseValues.push(value)
          }
          assert.deepEqual(reverseValues, sortedValuesReverse)
        }

        const sortedNumbers2 = [...sortedNumbers]

        let ndeleted = 0
        for (const n of deleteNumbers) {
          const start = performance.now()
          await ht.delete(`key${n}`)
          console.log(`deleted key${n} in ${performance.now() - start}ms; degree ${degree}; delete numbers index ${deleteNumbersIndex}; insert numbers index ${insertNumbersIndex}`)
          ndeleted += 1
          assert.equal(ht.count(), 32767 - ndeleted)
          assert.equal(ht._deletedCount, ndeleted)
        }

        assert.equal(ht.count(), 0)

        await ht.clear()
      }

      ht.close()
      await ht.destroy()
    }
  }

}).case()

const test36 = new Test('DiskSortedHashTable', async function integration36() {
  const randomNumbers = require('./test/randomNumbers1023_1.json')
  assert.equal(randomNumbers.length, 1023)
  const sortedNumbers = [...randomNumbers].sort((a, b) => a - b)
  const sortedNumbersReverse = [...sortedNumbers].reverse()
  const sortedValues = sortedNumbers.map(n => `value${n}`)
  const sortedValuesReverse = sortedNumbersReverse.map(n => `value${n}`)

  const randomNumbersArray = []
  let i = 1
  while (i <= 10) {
    const numbers = require(`./test/randomNumbers1023_${i}.json`)
    randomNumbersArray.push(numbers)
    i += 1
  }

  const insertNumbersArray = [sortedNumbers, sortedNumbersReverse, ...randomNumbersArray]

  async function runHt(insertNumbers, degree, insertNumbersIndex) {
    const ht = new DiskSortedHashTable({
      storagePath: `${__dirname}/DiskSortedHashTable_test_data/1023_${insertNumbersIndex}`,
      headerPath: `${__dirname}/DiskSortedHashTable_test_data/1023_${insertNumbersIndex}_header`,
      initialLength: 1023 * 8,
      itemSize: 2048,
      sortValueType: 'number',
      resizeRatio: 0,
      degree,
    })
    await ht.destroy()
    await ht.init()

    for (const n of insertNumbers) {
      const start = performance.now()
      await ht.set(`key${n}`, `value${n}`, n)
      console.log('set', `key${n}`, `value${n}`, n, 'in', `${performance.now() - start}ms, degree ${degree}, insert numbers index ${insertNumbersIndex}`)
    }

    const btreeRootNode = await constructBTree(ht, { unique: false })
    assertBalanced(btreeRootNode)
    assertMinHeight(btreeRootNode, calculateMinBTreeHeight(1023, degree))
    assertMaxHeight(btreeRootNode, calculateMaxBTreeHeight(1023, degree))
    assertMinKeysPerNode(btreeRootNode, degree - 1)
    assertMaxKeysPerNode(btreeRootNode, (degree * 2) - 1)
    assertInternalNodesIntegrity(btreeRootNode)
    assert.equal(ht.count(), 1023)

    {
      const forwardValues = []
      for await (const value of ht.forwardIterator()) {
        forwardValues.push(value)
      }
      assert.deepEqual(forwardValues, sortedValues)
    }

    {
      const items = traverseInOrder(btreeRootNode)
      assert.deepEqual(items.map(item => Number(item.sortValue)), sortedNumbers)
    }

    {
      const reverseValues = []
      for await (const value of ht.reverseIterator()) {
        reverseValues.push(value)
      }
      assert.deepEqual(reverseValues, sortedValuesReverse)
    }

    ht.close()
    await ht.destroy()
  }

  for (const degree of [2]) {
    const promises = []
    for (let i = 0; i < insertNumbersArray.length; i++) {
      promises.push(runHt(insertNumbersArray[i], degree, i))
    }
    await Promise.all(promises)
  }

}).case()

const test = Test.all([
  test1,
  test1_1,
  test1_2,
  test1_3,
  test1_4,
  test1_5,
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
  test_assert1,
  test_assert2,
  test_assert3,
  test_assert4,
  test_assert5,
  test14,
  test14_00,
  test14_0,
  test14_1,
  test14_2,
  test14_3,
  test14_4,
  test14_5,
  test15,
  test16,
  test17_2,
  test17_3,
  test17_6,
  test17_7_0,
  test17_7_1,
  test17_7_2,
  test18,
  test19,
  test_root_min_keys,
  test28,
  test29,
  test30,
  test31,
  test32,
  test33,
  test34,
  test35,
  test36,
])

if (process.argv[1] == __filename) {
  test()
}

module.exports = test
