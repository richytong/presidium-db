const { execSync } = require('child_process')
const fs = require('fs')
const os = require('os')

/**
 * Pre-allocates a file using the 'fallocate' system utility.
 * This is extremely fast and ensures contiguous disk blocks.
 */
function preallocate(filePath, sizeInBytes) {
  const platform = os.platform()
  if (platform == 'linux') {
    execSync(`fallocate -l ${sizeInBytes} ${filePath}`)
  } else if (platform == 'darwin') {
    execSync(`mkfile ${sizeInBytes} ${filePath}`)
  } else {
    throw new Error(`unsupported platform ${platform}`)
  }
}

module.exports = preallocate
