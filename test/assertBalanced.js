const assert = require('assert')
const traverse = require('../_internal/traverse')

// assertBalanced(btreeRootNode object) -> Promise<>
function assertBalanced(btreeRootNode) {
  const leafHeights = []

  traverse(btreeRootNode, {
    onLeaf({ height }) {
      leafHeights.push(height)
    },
  })

  try {
    assert.deepEqual(leafHeights, Array(leafHeights.length).fill(leafHeights[0]))
  } catch (_error) {
    throw new Error('b-tree not balanced')
  }
}

module.exports = assertBalanced
