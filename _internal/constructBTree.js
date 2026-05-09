const getNodeItems = require('./getNodeItems')

// constructBTree(ht DiskSortedHashTable, options { unique: boolean, onNode: function, onLeaf: function })
// constructBTree(ht DiskSortedHashTable, options { unique: boolean, onNode: function, onLeaf: function }, btreeNode object, memo object) -> Promise<>
async function constructBTree(ht, options, btreeNode = {}, memo = {}) {
  const { unique = false, onNode, onLeaf } = options

  memo.keys ??= []
  memo.height ??= 0

  let { height, isLeaf } = memo

  if (btreeNode.items == null) {
    const btreeRootNodeRightmostItem = await ht._getBTreeRootNodeRightmostItem()
    if (btreeRootNodeRightmostItem) {
      btreeRootNodeRightmostItem.key = await ht._readKey(
        btreeRootNodeRightmostItem.index,
        btreeRootNodeRightmostItem.keyByteLength
      )
    }
    const btreeRootNodeItems = btreeRootNodeRightmostItem == null
      ? []
      : await getNodeItems(ht, btreeRootNodeRightmostItem)

    isLeaf = btreeRootNodeItems.length === 0 || (
      btreeRootNodeItems.every(item => item.btreeLeftChildNodeRightmostItemIndex == -1)
      && btreeRootNodeItems.every(item => item.btreeRightChildNodeRightmostItemIndex == -1)
    )

    btreeNode.items = btreeRootNodeItems
    btreeNode.root = true
    memo.keys.push(...btreeRootNodeItems.map(item => item.key))
  }

  if (onNode) {
    onNode({ height, node: btreeNode })
  }

  if (isLeaf && onLeaf) {
    onLeaf({ height, node: btreeNode })
  }

  let i = 0
  for (const item of btreeNode.items) {
    btreeNode[item.key] = {}

    let leftConditional
    if (unique) {
      leftConditional = i === 0 && item.btreeLeftChildNodeRightmostItemIndex > -1
    } else {
      leftConditional = item.btreeLeftChildNodeRightmostItemIndex > -1
    }

    const rightConditional = item.btreeRightChildNodeRightmostItemIndex > -1

    if (leftConditional) {
      const btreeLeftChildNodeRightmostItem = await ht._getItem(item.btreeLeftChildNodeRightmostItemIndex)
      const btreeLeftChildNodeItems = await getNodeItems(ht, btreeLeftChildNodeRightmostItem)

      btreeNode[item.key].leftChild = { items: btreeLeftChildNodeItems }
      btreeNode[item.key].leftChild.keys = btreeLeftChildNodeItems.map(item => item.key)
      memo.keys.push(...btreeLeftChildNodeItems.map(item => item.key))

      await constructBTree(ht, options, btreeNode[item.key].leftChild, {
        ...memo,
        height: height + 1,
        isLeaf: (
          btreeNode[item.key].leftChild.items.every(item => item.btreeLeftChildNodeRightmostItemIndex == -1)
          && btreeNode[item.key].leftChild.items.every(item => item.btreeRightChildNodeRightmostItemIndex == -1)
        ),
      })
    }

    if (rightConditional) {
      const btreeRightChildNodeRightmostItem = await ht._getItem(item.btreeRightChildNodeRightmostItemIndex)
      const btreeRightChildNodeItems = await getNodeItems(ht, btreeRightChildNodeRightmostItem)

      btreeNode[item.key].rightChild = { items: btreeRightChildNodeItems }
      btreeNode[item.key].rightChild.keys = btreeRightChildNodeItems.map(item => item.key)
      memo.keys.push(...btreeRightChildNodeItems.map(item => item.key))

      await constructBTree(ht, options, btreeNode[item.key].rightChild, {
        ...memo,
        height: height + 1,
        isLeaf: (
          btreeNode[item.key].rightChild.items.every(item => item.btreeLeftChildNodeRightmostItemIndex == -1)
          && btreeNode[item.key].rightChild.items.every(item => item.btreeRightChildNodeRightmostItemIndex == -1)
        ),
      })
    }

    i += 1
  }

  return btreeNode
}

module.exports = constructBTree
