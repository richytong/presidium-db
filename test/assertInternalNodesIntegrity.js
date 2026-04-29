const assert = require('assert')

// assertInternalNodesIntegrity(btreeNode object) -> undefined
function assertInternalNodesIntegrity(btreeNode) {
  const isLeaf =
    (btreeNode.root && btreeNode.items.length === 0)
    || (btreeNode.items[0].btreeLeftChildNodeRightmostItemIndex === -1 && btreeNode.items[0].btreeRightChildNodeRightmostItemIndex === -1)

  if (isLeaf) {
    return
  }

  for (const item of btreeNode.items) {
    const key = btreeNode[item.key]

    if (isLeaf && item.btreeLeftChildNodeRightmostItemIndex > -1 && item.btreeRightChildNodeRightmostItemIndex > -1) {
      const error = new Error('Imbalanced node')
      error.node = btreeNode
      throw error
    }

    if (key.leftChild) {
      assertInternalNodesIntegrity(key.leftChild)
    }
    if (key.rightChild) {
      assertInternalNodesIntegrity(key.rightChild)
    }
  }

  for (let i = 1; i < (btreeNode.items.length - 1); i++) {
    const item = btreeNode.items[i]
    const leftItem = btreeNode.items[i - 1]
    const rightItem = btreeNode.items[i + 1]

    const key = btreeNode[item.key]
    const leftKey = btreeNode[leftItem.key]
    const rightKey = btreeNode[rightItem.key]

    assert.deepEqual(key.leftChild.keys, leftKey.rightChild.keys)
    assert.deepEqual(key.rightChild.keys, rightKey.leftChild.keys)
  }

  if (btreeNode.items.length > 1) {
    const lastItem = btreeNode.items[btreeNode.items.length - 1]
    const leftItem = btreeNode.items[btreeNode.items.length - 2]

    const lastKey = btreeNode[lastItem.key]
    const leftKey = btreeNode[leftItem.key]

    assert.deepEqual(lastKey.leftChild.keys, leftKey.rightChild.keys)
  }

}

module.exports = assertInternalNodesIntegrity
