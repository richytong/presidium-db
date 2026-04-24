// assertMaxKeysPerNode(ht DiskSortedHashTable, maxKeysPerNode number) -> Promise<>
async function assertMaxKeysPerNode(ht, maxKeysPerNode) {
  await ht._constructBTree({
    unique: false,
    onNode({ node }) {
      if (node.items.length > maxKeysPerNode) {
        throw new Error(`b-tree node over maximum number of keys per node (${node.items.length} / ${maxKeysPerNode})`)
      }
    }
  })
}

module.exports = assertMaxKeysPerNode
