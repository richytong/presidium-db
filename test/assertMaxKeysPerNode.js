const traverse = require('../_internal/traverse')

// assertMaxKeysPerNode(btreeRootNode object, maxKeysPerNode number) -> Promise<>
function assertMaxKeysPerNode(btreeRootNode, maxKeysPerNode) {
  traverse(btreeRootNode, {
    onNode({ node }) {
      if (node.items.length > maxKeysPerNode) {
        throw new Error(`b-tree node over maximum number of keys per node (${node.items.length} / ${maxKeysPerNode})`)
      }
    }
  })
}

module.exports = assertMaxKeysPerNode
