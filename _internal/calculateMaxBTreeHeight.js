/**
 * @name calculateMaxBTreeHeight
 *
 * @docs
 * ```coffeescript [specscript]
 * calculateMaxBTreeHeight(n number, degree number) -> maxHeight number
 * ```
 *
 * Calculates the maximum height of a b-tree.
 *
 * Arguments:
 *   * `n` - `number` - number of keys/items.
 *   * `degree` - `number` - the degree of the b-tree.
 *
 * Return:
 *   * `maxHeight` - `number` - the maximum height of the b-tree.
 */
function calculateMaxBTreeHeight(n, degree) {
  if (n === 0) return 0

  const maxHeight = Math.log((n + 1) / 2) / Math.log(degree)

  return Math.floor(maxHeight)
}

module.exports = calculateMaxBTreeHeight
