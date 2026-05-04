const constructBTree2 = require('./constructBTree2')

// logBTree(ht DiskSortedHashTable, unique boolean) -> Promise<>
async function logBTree(ht, unique = false) {
  const btreeRootNode = await constructBTree2(ht, { unique })

  console.log(JSON.stringify(btreeRootNode, (key, value) => {
    if (key == 'items' || key == 'keys') {
      return undefined
    }
    return value
  }, 2))
}

module.exports = logBTree
