/**
 * Calculates the maximum height of a B-tree.
 * @param {number} n - Total number of keys in the tree.
 * @param {number} t - Minimum degree (t >= 2).
 * @returns {number} The maximum possible height (starting from 1).
 */
function calculateMaxBTreeHeight(n, t) {
    if (n === 0) return 0;
    if (n === 1) return 1;
    
    // Formula: floor(log_t((n + 1) / 2)) + 1
    // In JS, log_b(x) is Math.log(x) / Math.log(b)
    const height = Math.floor(Math.log((n + 1) / 2) / Math.log(t)) + 1;
    
    return height;
}

module.exports = calculateMaxBTreeHeight
