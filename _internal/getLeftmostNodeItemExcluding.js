// getLeftmostNodeItemExcluding(
//   nodeItems Array<{ index: number },
//   nodeItem { index: number }>
// ) -> leftmostNodeItem { index: number }
function getLeftmostNodeItemExcluding(nodeItems, nodeItem) {
  let leftmostGrandparentNodeItem
  for (const _nodeItem of nodeItems) {
    if (_nodeItem.index === nodeItem.index) {
      continue
    }
    return _nodeItem
  }
  return undefined
}

module.exports = getLeftmostNodeItemExcluding
