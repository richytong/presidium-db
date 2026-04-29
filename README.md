# Presidium DB
![presidium](https://rubico.land/assets/presidium-logo-3-w200.jpg)

Source code: [GitHub](https://github.com/richytong/presidium-db) |
License: [CFOSS](https://cloutsworld.com/en-us/legal/license/cfoss)

![Node.js CI](https://github.com/richytong/presidium-db/workflows/Node.js%20CI/badge.svg)
[![codecov](https://codecov.io/gh/richytong/presidium-db/branch/master/graph/badge.svg)](https://codecov.io/gh/richytong/presidium-db)
[![npm version](https://img.shields.io/npm/v/presidium-db.svg?style=flat)](https://www.npmjs.com/package/presidium-db)

Presidium DB library.

## Installation
with [npm](https://docs.npmjs.com/downloading-and-installing-node-js-and-npm):
```bash
npm i presidium-db
```

require Presidium DB in [CommonJS](https://nodejs.org/docs/latest/api/modules.html#modules-commonjs-modules):
```javascript
const DiskHashTable = require('presidium-db/DiskHashTable')
const DiskSortedHashTable = require('presidium-db/DiskSortedHashTable')
```

## [Store data on disk as a hash table](https://presidium.services/docs/DiskHashTable)
```javascript
const DiskHashTable = require('presidium-db/DiskHashTable')

const ht = new DiskHashTable({
  storageFilepath: '/path/to/storage-file',
  headerFilepath: '/path/to/header-file',
  initialLength: 1024,
})
await ht.init()

await ht.set('my-key', 'my-value')

const myValue = await ht.get('my-key')
console.log(myValue) // 'my-value'

await ht.delete('my-key')
```

## [Store data on disk as a sorted hash table](https://presidium.services/docs/DiskSortedHashTable)
```javascript
const DiskSortedHashTable = require('presidium-db/DiskSortedHashTable')

const sortedHt = new DiskSortedHashTable({
  storageFilepath: '/path/to/storage-file',
  headerFilepath: '/path/to/header-file',
  initialLength: 1024,
})
await sortedHt.init()

await sortedHt.set('first-key', 'first-value', 1)
await sortedHt.set('second-key', 'second-value', 2)
await sortedHt.set('third-key', 'third-value', 3)

for await (const value of sortedHt.forwardIterator()) {
  console.log(value) // first-value
                     // second-value
                     // third-value
}

for await (const value of sortedHt.reverseIterator()) {
  console.log(value) // third-value
                     // second-value
                     // first-value
}

for await (const value of sortedHt.forwardIterator({ startingSortValue: 2, endingSortValue: 3 })) {
  console.log(value) // second-value
                     // third-value
}

for await (const value of sortedHt.reverseIterator({ startingSortValue: 2, endingSortValue: 1 })) {
  console.log(value) // second-value
                     // first-value
}
```

## License
Presidium DB is distributed under the [CFOSS License](https://cloutsworld.com/en-us/legal/license/cfoss).

# Support
  * minimum Node.js version: 16

## Supported Platforms
  * Linux
