// logBTree(ht DiskSortedHashTable, unique boolean) -> Promise<>
async function logBTree(ht, unique = false) {
  const btreeRootNode = await ht._constructBTree({ unique })

  console.log(JSON.stringify(btreeRootNode, (key, value) => {
    if (key == 'items' || key == 'keys') {
      return undefined
    }
    return value
  }, 2))
}

module.exports = logBTree
