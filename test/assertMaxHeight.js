const traverse = require('../_internal/traverse')

// assertMaxHeight(btreeRootNode object, maxHeight number) -> Promise<>
function assertMaxHeight(btreeRootNode, maxHeight) {
  traverse(btreeRootNode, {
    onLeaf({ height }) {
      if (height > maxHeight) {
        throw new Error(`b-tree over max height (${height} / ${maxHeight})`)
      }
    }
  })
}

module.exports = assertMaxHeight
