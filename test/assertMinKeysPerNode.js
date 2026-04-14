// assertMinKeysPerNode(ht DiskSortedHashTable, minKeysPerNode number) -> Promise<>
async function assertMinKeysPerNode(ht, minKeysPerNode) {
  await ht._constructBTree({
    unique: true,
    onNode({ node }) {
      if (node.items.length < minKeysPerNode) {
        if (node.root) {
          // ok
        } else {
          console.error(node)
          throw new Error(`b-tree node under minimum number of keys per node (${node.items.length} / ${minKeysPerNode})`)
        }
      }
    }
  })
}

module.exports = assertMinKeysPerNode
