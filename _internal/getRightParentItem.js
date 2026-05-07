// getRightParentItem(parentItem {
//   isRightChildPointer: boolean,
//   isLeftChildPointer: boolean,
//   leftItem: object,
// })
function getRightParentItem(parentItem) {
  if (parentItem.isLeftChildPointer) {
    return parentItem
  }

  if (parentItem.isRightChildPointer) {
    if (parentItem.rightItem) {
      return parentItem.rightItem
    }
    return undefined
  }

  throw new Error('parent node item isLeftChildPointer or isRightChildPointer unset')
}

module.exports = getRightParentItem
