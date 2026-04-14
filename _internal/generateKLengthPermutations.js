// generateKLengthPermutations(n number, k number) -> Array<Array<number>>
function* generateKLengthPermutations(n, k) {
  // Basic validation
  if (k > n || k <= 0) return;

  function* backtrack(currentPath, usedSet) {
    // Base case: yield the permutation when it reaches length k
    if (currentPath.length === k) {
      yield [...currentPath];
      return;
    }

    for (let i = 1; i <= n; i++) {
      if (usedSet.has(i)) continue;

      currentPath.push(i);
      usedSet.add(i);

      // Delegate to the recursive generator call
      yield* backtrack(currentPath, usedSet);

      // Backtrack
      usedSet.delete(i);
      currentPath.pop();
    }
  }

  yield* backtrack([], new Set());
}

module.exports = generateKLengthPermutations
