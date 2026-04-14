// generateRandomUniqueNumbers(length number) -> Array
function generateRandomUniqueNumbers(length) {
  const result = []
  while (result.length < length) {
    const n = Math.trunc(Math.random() * length) + 1
    if (result.includes(n)) {
      continue
    }
    result.push(n)
  }
  return result
}

module.exports = generateRandomUniqueNumbers
