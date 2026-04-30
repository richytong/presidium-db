const fs = require('fs')

// saveBTree(ht DiskSortedHashTable, filepath string, unique boolean, btreeRootNode object) -> Promise<>
async function saveBTree(ht, filepath, unique = false, btreeRootNode) {
  btreeRootNode ??= await ht._constructBTree({ unique })

  await fs.promises.writeFile(filepath, JSON.stringify(btreeRootNode, (key, value) => {
    if (key == 'items' || key == 'keys') {
      return undefined
    }
    return value
  }, 2))
}

module.exports = saveBTree
