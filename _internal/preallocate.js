const { execSync } = require('child_process')

// preallocate(filepath string, sizeBytes number) -> undefined
function preallocate(filepath, sizeBytes) {
  execSync(`truncate -s ${sizeBytes} ${filepath}`)
}

module.exports = preallocate
