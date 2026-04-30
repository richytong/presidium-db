// randomUniqueNumbersGenerator(length number, maximum number) -> Array
function * randomUniqueNumbersGenerator(length, maximum) {
  const yielded = []
  while (yielded.length < length) {
    const n = Math.trunc(Math.random() * maximum) + 1
    if (yielded.includes(n)) {
      continue
    }
    yielded.push(n)
    yield n
  }
}

module.exports = randomUniqueNumbersGenerator
