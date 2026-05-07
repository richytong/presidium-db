// getRightmostNodeItemExcluding(
//   nodeItems Array<{ index: number },
//   nodeItem { index: number }>
// ) -> rightmostNodeItem { index: number }
function getRightmostNodeItemExcluding(nodeItems, nodeItem) {
  let j = nodeItems.length - 1
  while (j > -1) {
    const _nodeItem = nodeItems[j]
    if (_nodeItem.index === nodeItem.index) {
      j -= 1
      continue
    }
    return _nodeItem
  }
  return undefined
}

module.exports = getRightmostNodeItemExcluding
