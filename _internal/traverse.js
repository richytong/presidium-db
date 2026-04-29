// traverse(btreeNode object, options { onLeaf: function, onNode: function }, height number) -> undefined
function traverse(btreeNode, options = {}, height = 0) {
  const { onNode, onLeaf } = options

  if (onNode) {
    onNode({ height, node: btreeNode })
  }

  const isLeaf =
    (btreeNode.root && btreeNode.items.length === 0)
    || (btreeNode.items[0].btreeLeftChildNodeRightmostItemIndex === -1 && btreeNode.items[0].btreeRightChildNodeRightmostItemIndex === -1)

  if (isLeaf && onLeaf) {
    onLeaf({ height, node: btreeNode })
  }

  for (const item of btreeNode.items) {
    if (isLeaf && item.btreeLeftChildNodeRightmostItemIndex > -1 && item.btreeRightChildNodeRightmostItemIndex > -1) {
      const error = new Error('Imbalanced node')
      error.node = btreeNode
      throw error
    }

    const key = btreeNode[item.key]

    if (key.leftChild) {
      key.leftChild.parentNode = btreeNode
      traverse(key.leftChild, options, height + 1)
    }
    if (key.rightChild) {
      key.rightChild.parentNode = btreeNode
      traverse(key.rightChild, options, height + 1)
    }
  }

}

module.exports = traverse
