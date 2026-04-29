// traverseInOrder(btreeRootNode object) -> items Array<object>
function traverseInOrder(btreeRootNode) {
  const result = []
  const stack = [{ node: btreeRootNode, itemIndex: 0, stage: 'left' }]

  while (stack.length > 0) {
    const current = stack[stack.length - 1]
    const { node, itemIndex, stage } = current

    if (itemIndex >= node.items.length) {
      stack.pop()
      continue
    }

    const currentItem = node.items[itemIndex]
    const childContainer = node[currentItem.key]

    if (stage === 'left') {
      current.stage = 'item'
      if (childContainer && childContainer.leftChild && itemIndex === 0) {
        stack.push({ node: childContainer.leftChild, itemIndex: 0, stage: 'left' })
      }
    } 
    else if (stage === 'item') {
      result.push(currentItem)
      current.stage = 'right'
    } 
    else if (stage === 'right') {
      current.itemIndex++
      current.stage = 'left'
      if (childContainer && childContainer.rightChild) {
        stack.push({ node: childContainer.rightChild, itemIndex: 0, stage: 'left' })
      }
    }
  }

  return result
}

module.exports = traverseInOrder
