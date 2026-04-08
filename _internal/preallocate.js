const { execSync } = require('child_process')
const fs = require('fs')

/**
 * Pre-allocates a file using the 'fallocate' system utility.
 * This is extremely fast and ensures contiguous disk blocks.
 */
function preallocate(filePath, sizeInBytes) {
  execSync(`fallocate -l ${sizeInBytes} ${filePath}`)
}

module.exports = preallocate
