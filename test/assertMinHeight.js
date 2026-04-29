const traverse = require('../_internal/traverse')

// assertMinHeight(btreeRootNode object, minHeight number) -> Promise<>
function assertMinHeight(btreeRootNode, minHeight) {
  traverse(btreeRootNode, {
    onLeaf({ height }) {
      if (height < minHeight) {
        throw new Error(`b-tree under min height (${height} / ${minHeight})`)
      }
    },
  })
}

module.exports = assertMinHeight
