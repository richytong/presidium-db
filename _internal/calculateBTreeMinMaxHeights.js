const calculateMinBTreeHeight = require('./calculateMinBTreeHeight')
const calculateMaxBTreeHeight = require('./calculateMaxBTreeHeight')

const degree = 2
const numberOfItems = 127

console.log('Min height:', calculateMinBTreeHeight(numberOfItems, degree * 2))
console.log('Max height:', calculateMaxBTreeHeight(numberOfItems, degree))
