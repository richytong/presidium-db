/**
 * Calculates the minimum height of a B-tree.
 * @param {number} n - Total number of keys in the tree.
 * @param {number} t - The minimum degree of the B-tree.
 * @returns {number} The minimum height (starting from 1).
 */
function calculateMinBTreeHeight(n, t) {
  if (n === 0) return 0
  
  // The maximum number of children for any node is m = 2t
  const m = 2 * t
  
  // Formula: h = ceil(log_m(n + 1))
  // In JS, log_base(x) is Math.log(x) / Math.log(base)
  return Math.ceil(Math.log(n + 1) / Math.log(m))
}

module.exports = calculateMinBTreeHeight
