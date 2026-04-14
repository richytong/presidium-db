// assertMinHeight(ht DiskSortedHashTable, minHeight number) -> Promise<>
async function assertMinHeight(ht, minHeight) {
  await ht._constructBTree({
    unique: true,
    onLeaf({ height }) {
      if (height < minHeight) {
        throw new Error(`b-tree under min height (${height} / ${minHeight})`)
      }
    }
  })
}

module.exports = assertMinHeight
