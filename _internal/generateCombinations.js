// generateCombinations(n number) -> Array<Array<number>>
function generateKLengthCombinations(n, k) {
  const result = [];
  
  // Basic validation: cannot pick more items than available
  if (k > n || k <= 0) return [];

  function backtrack(start, current) {
    // Only push to result if the current combination matches the target length
    if (current.length === k) {
      result.push([...current]);
      return; // Stop exploring this branch once length is reached
    }

    for (let i = start; i <= n; i++) {
      current.push(i);
      backtrack(i + 1, current);
      current.pop();
    }
  }

  backtrack(1, []);
  return result;
}

// Example: Generate all combinations of length 2 from numbers up to 3
console.log(generateKLengthCombinations(3, 3));
// Output: [[1, 2], [1, 3], [2, 3]]


module.exports = generateCombinations
