const fs = require('fs')
const { isDeepEqual } = require('rubico/x')
const generateRandomUniqueNumbers = require('./_internal/generateRandomUniqueNumbers')

setImmediate(async () => {

  const randomNumbersArr = []

  let i = 1
  while (i <= 10) {
    let randomNumbers = generateRandomUniqueNumbers(32767)
    while (randomNumbersArr.some(existingRandomNumbers => isDeepEqual(existingRandomNumbers, randomNumbers))) {
      console.log('regenerating random numbers...')
      randomNumbers = generateRandomUniqueNumbers(32767)
    }

    const content = JSON.stringify(randomNumbers)
    console.log(content)
    await fs.promises.writeFile(`${__dirname}/test/randomNumbers32767_${i}.json`, content)

    i += 1
  }
})
