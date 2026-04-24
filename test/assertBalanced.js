const assert = require('assert')

// assertBalanced(ht DiskSortedHashTable) -> Promise<>
async function assertBalanced(ht) {
  const leafHeights = []
  await ht._constructBTree({
    unique: false,
    onLeaf({ height }) {
      leafHeights.push(height)
    }
  })
  try {
    assert.deepEqual(leafHeights, Array(leafHeights.length).fill(leafHeights[0]))
  } catch (_error) {
    throw new Error('b-tree not balanced')
  }
}

module.exports = assertBalanced
