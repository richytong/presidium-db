// getNodeItems(ht DiskSortedHashTable, rightmostItem { index: number, btreeLeftItemIndex: number }) -> Promise<number>
async function getNodeItems(ht, rightmostItem) {
  const btreeNodeItems = [rightmostItem]
  let currentItem = rightmostItem

  while (currentItem.btreeLeftItemIndex > -1) {
    currentItem = await ht._getItem(currentItem.btreeLeftItemIndex)
    btreeNodeItems.unshift(currentItem)
  }

  return btreeNodeItems
}

module.exports = getNodeItems
