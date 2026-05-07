// getLeftParentItem(parentItem {
//   isRightChildPointer: boolean,
//   isLeftChildPointer: boolean,
//   leftItem: object,
// })
function getLeftParentItem(parentItem) {
  if (parentItem == null) {
    return undefined
  }

  if (parentItem.isRightChildPointer) {
    return parentItem
  }

  if (parentItem.isLeftChildPointer) {
    if (parentItem.leftItem) {
      return parentItem.leftItem
    }
    return undefined
  }

  throw new Error('parent node item isLeftChildPointer or isRightChildPointer unset')
}

module.exports = getLeftParentItem
