/**
 * @name calculateMinBTreeHeight
 *
 * @docs
 * ```coffeescript [specscript]
 * calculateMinBTreeHeight(n number, degree number) -> minHeight number
 * ```
 *
 * Calculates the minimum height of a b-tree.
 *
 * Arguments:
 *   * `n` - `number` - number of keys/items.
 *   * `degree` - `number` - the degree of the b-tree.
 *
 * Return:
 *   * `minHeight` - `number` - the minimum height of the b-tree.
 */
function calculateMinBTreeHeight(n, degree) {
  if (n === 0) return 0

  const order = 2 * degree

  const minHeight = Math.log(n + 1) / Math.log(order)

  return Math.ceil(minHeight) - 1
}

module.exports = calculateMinBTreeHeight
