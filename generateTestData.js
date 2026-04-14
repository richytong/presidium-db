const fs = require('fs')
const { isDeepEqual } = require('rubico/x')
const generateRandomUniqueNumbers = require('./_internal/generateRandomUniqueNumbers')

setImmediate(async () => {

  const randomNumbersArr = []

  let i = 1
  while (i <= 100) {
    let randomNumbers = generateRandomUniqueNumbers(127)
    while (randomNumbersArr.some(existingRandomNumbers => isDeepEqual(existingRandomNumbers, randomNumbers))) {
      console.log('regenerating random numbers...')
      randomNumbers = generateRandomUniqueNumbers(127)
    }

    const content = JSON.stringify(randomNumbers)
    console.log(content)
    await fs.promises.writeFile(`${__dirname}/test/randomNumbers127_${i}.json`, content)

    i += 1
  }
})
