const traverse = require('../_internal/traverse')

// assertMinKeysPerNode(btreeRootNode object, minKeysPerNode number) -> Promise<>
function assertMinKeysPerNode(btreeRootNode, minKeysPerNode) {
  traverse(btreeRootNode, {
    onNode({ node }) {
      if (node.items.length < minKeysPerNode) {
        if (node.root) {
          // ok
        } else {
          const error = new Error(`b-tree node under minimum number of keys per node (${node.items.length} / ${minKeysPerNode})`)
          error.node = node
          throw error
        }
      }
    }
  })
}

module.exports = assertMinKeysPerNode
