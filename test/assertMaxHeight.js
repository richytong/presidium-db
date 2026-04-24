// assertMaxHeight(ht DiskSortedHashTable, maxHeight number) -> Promise<>
async function assertMaxHeight(ht, maxHeight) {
  await ht._constructBTree({
    unique: false,
    onLeaf({ height }) {
      if (height > maxHeight) {
        throw new Error(`b-tree over max height (${height} / ${maxHeight})`)
      }
    }
  })
}

module.exports = assertMaxHeight
