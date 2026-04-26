/**
 * Presidium DB
 * https://github.com/richytong/presidium-db
 * (c) Richard Tong
 * Presidium DB may be freely distributed under the CFOSS license.
 */

const fs = require('fs')
const preallocate = require('./_internal/preallocate')
const crypto = require('crypto')

const DATA_SLICE_SIZE = 512 * 1024

const ENCODING = 'utf8'

const EMPTY = 0

const OCCUPIED = 1

const REMOVED = 2

/**
 * @name DiskHashTable
 *
 * @docs
 * ```coffeescript [specscript]
 * new DiskHashTable(options {
 *   initialLength: number,
 *   storagePath: string,
 *   headerPath: string,
 *   resizeRatio: number,
 *   resizeFactor: number,
 * }) -> ht DiskHashTable
 * ```
 *
 * Presidium DiskHashTable class. Creates a hash table that stores all data on disk.
 *
 * Arguments:
 *   * `options`
 *     * `initialLength` - `number` - the initial length of the disk hash table. Defaults to 1024.
 *     * `storagePath` - `string` - the path to the file used to store the disk hash table data.
 *     * `headerPath` - `string` - the path to the file used to store header information about the disk hash table.
 *     * `resizeRatio` - `number` - the ratio of number of items to table length at which to resize the disk hash table. Minimum value 0 (no resize), maximum value 1. Defaults to 0.
 *     * `resizeFactor` - `number` - the factor that is multiplied with the disk hash table's current length to determine the new table length on a resize.
 *
 * Return:
 *   * `ht` - [`DiskHashTable`](/docs/DiskHashTable) - a `DiskHashTable` instance.
 *
 * ```javascript
 * const ht = new DiskHashTable({
 *   initialLength: 1024,
 *   storagePath: '/path/to/storage-file',
 *   headerPath: '/path/to/header-file',
 *   resizeRatio: 0.7,
 *   resizeFactor: 4,
 * })
 * ```
 *
 * Limits:
 *   * 511 KiB for key, and value.
 *
 * Supported platforms:
 *   * `linux64`
 *
 * ## Resizing the disk hash table
 * When an item is inserted into the disk hash table via [set](/docs/DiskHashTable#set), the current capacity ratio of the table is calculated as the sum of the table's count and deleted count divided by the table's length. If the current capacity ratio exceeds the `resizeRatio` (and the `resizeRatio` is not 0), a resize of the table occurs.
 *
 * During a table resize, each item of the table is added into a temporary storage file using the new table length calculated from the equation below:
 *
 * ```
 * newTableLength = oldTableLength * resizeFactor
 * ```
 *
 * Once all of the items have been added into the temporary storage file, the temporary storage file is moved to the location of the old storage file to be used as the new storage file.
 *
 * ## Allocation of disk space
 * The disk hash table initially preallocates a block of memory on disk of `(512 * initialLength)` KiB for database operations. When the disk hash table is resized, the block of memory on disk is reallocated to a new size of `(512 * initialLength * numberOfResizes * resizeFactor)` KiB.
 */
class DiskHashTable {
  constructor(options) {
    this.initialLength = options.initialLength ?? 1024
    this._length = null
    this._count = null
    this._deletedCount = null
    this.storagePath = options.storagePath
    this.headerPath = options.headerPath
    this.storageFd = null
    this.headerFd = null
    this.resizeRatio = options.resizeRatio ?? 0
    this.resizeFactor = options.resizeFactor ?? 4
  }

  // _initializeHeader() -> headerReadBuffer Promise<Buffer>
  async _initializeHeader() {
    const headerReadBuffer = Buffer.alloc(16)
    headerReadBuffer.writeUInt32BE(this.initialLength, 0)
    headerReadBuffer.writeUInt32BE(0, 4)
    headerReadBuffer.writeInt32BE(0, 8)
    headerReadBuffer.writeInt32BE(-1, 12)

    await this.headerFd.write(headerReadBuffer, {
      offset: 0,
      position: 0,
      length: headerReadBuffer.length,
    })

    return headerReadBuffer
  }

  /**
   * @name init
   *
   * @docs
   * ```coffeescript [specscript]
   * ht.init() -> Promise<>
   * ```
   *
   * Initializes the disk hash table.
   *
   * Arguments:
   *   * (none)
   *
   * Return:
   *   * Empty promise.
   *
   * ```javascript
   * await ht.init()
   * ```
   */
  async init() {
    for (const filepath of [this.storagePath, this.headerPath]) {
      const dir = filepath.split('/').slice(0, -1).join('/')
      await fs.promises.mkdir(dir, { recursive: true })

      const now = new Date()
      try {
        fs.utimesSync(filepath, now, now)
      } catch (error) {
        fs.closeSync(fs.openSync(filepath, 'a'))
      }
    }

    this.storageFd = await fs.promises.open(this.storagePath, 'r+')
    this.headerFd = await fs.promises.open(this.headerPath, 'r+')

    let headerReadBuffer = await this._readHeader()
    if (headerReadBuffer.every(byte => byte === 0)) {
      headerReadBuffer = await this._initializeHeader()
    }

    const length = headerReadBuffer.readUInt32BE(0)
    this._length = length

    const count = headerReadBuffer.readUInt32BE(4)
    this._count = count

    const deletedCount = headerReadBuffer.readUInt32BE(8)
    this._deletedCount = deletedCount

    const headIndex = headerReadBuffer.readInt32BE(12)
    this._headIndex = headIndex

    await preallocate(this.headerPath, 16)
    await preallocate(this.storagePath, DATA_SLICE_SIZE * length)
  }

  /**
   * @name clear
   *
   * @docs
   * ```coffeescript [specscript]
   * clear() -> Promise<>
   * ```
   *
   * Clears all data from the disk hash table.
   *
   * Arguments:
   *   * (none)
   *
   * Return:
   *   * Empty promise.
   *
   * ```javascript
   * await ht.clear()
   * ```
   */
  async clear() {
    this.close()

    await fs.promises.rm(this.storagePath).catch(() => {})
    await fs.promises.rm(this.headerPath).catch(() => {})

    for (const filepath of [this.storagePath, this.headerPath]) {
      const dir = filepath.split('/').slice(0, -1).join('/')
      await fs.promises.mkdir(dir, { recursive: true })

      const now = new Date()
      try {
        fs.utimesSync(filepath, now, now)
      } catch (error) {
        fs.closeSync(fs.openSync(filepath, 'a'))
      }
    }

    this.storageFd = await fs.promises.open(this.storagePath, 'r+')
    this.headerFd = await fs.promises.open(this.headerPath, 'r+')

    const headerReadBuffer = await this._initializeHeader()

    const length = headerReadBuffer.readUInt32BE(0)
    this._length = length

    const count = headerReadBuffer.readUInt32BE(4)
    this._count = count

    const deletedCount = headerReadBuffer.readUInt32BE(8)
    this._deletedCount = deletedCount

    const headIndex = headerReadBuffer.readInt32BE(12)
    this._headIndex = headIndex
  }

  /**
   * @name destroy
   *
   * @docs
   * ```coffeescript [specscript]
   * destroy() -> Promise<>
   * ```
   *
   * Removes all system resources used by the disk hash table.
   *
   * Arguments:
   *   * (none)
   *
   * Return:
   *   * Empty promise.
   *
   * ```javascript
   * await ht.destroy()
   * ```
   */
  async destroy() {
    await fs.promises.rm(this.storagePath).catch(() => {})
    await fs.promises.rm(this.headerPath).catch(() => {})
  }

  /**
   * @name close
   *
   * @docs
   * ```coffeescript [specscript]
   * close() -> undefined
   * ```
   *
   * Closes the underlying file handles used by the disk hash table.
   *
   * Arguments:
   *   * (none)
   *
   * Return:
   *   * `undefined`
   *
   * ```javascript
   * ht.close()
   * ```
   */
  close() {
    this.storageFd.close()
    this.headerFd.close()
    this.storageFd = null
    this.headerFd = null
  }

  // _hash1(key string) -> number
  _hash1(key) {
    let hashCode = 0
    const prime = 31
    for (let i = 0; i < key.length; i++) {
      hashCode = (prime * hashCode + key.charCodeAt(i)) % this._length
    }
    return hashCode
  }

  // _hash2(key string) -> number
  _hash2(key) {
    let hash = 0
    for (let i = 0; i < key.length; i++) {
      hash = (hash << 3) - hash + key.charCodeAt(i)
    }
    const prime = 7 
    return prime - (Math.abs(hash) % prime)
  }

  // header file
  // 32 bits / 4 bytes table length
  // 32 bits / 4 bytes item count
  // 32 bits / 4 bytes deleted item count
  // 32 bits / 4 bytes head index

  // _readHeader() -> headerReadBuffer Promise<Buffer>
  async _readHeader() {
    const headerReadBuffer = Buffer.alloc(16)

    await this.headerFd.read({
      buffer: headerReadBuffer,
      offset: 0,
      position: 0,
      length: 16,
    })

    return headerReadBuffer
  }

  // _read(index number) -> readBuffer Promise<Buffer>
  async _read(index) {
    const position = index * DATA_SLICE_SIZE
    const readBuffer = Buffer.alloc(DATA_SLICE_SIZE)

    await this.storageFd.read({
      buffer: readBuffer,
      offset: 0,
      position,
      length: DATA_SLICE_SIZE,
    })

    return readBuffer
  }

  // _getItem(index number) -> item { index: number, nextIndex: number, value: string }
  async _getItem(index) {
    if (index == -1) {
      return undefined
    }

    const readBuffer = await this._read(index)
    const statusMarker = readBuffer.readUInt8(0)
    if (statusMarker == OCCUPIED || statusMarker == REMOVED) {
      const keyByteLength = readBuffer.readUInt32BE(1)
      const valueByteLength = readBuffer.readUInt32BE(5)
      const nextIndex = readBuffer.readInt32BE(9)
      const keyBuffer = readBuffer.subarray(13, keyByteLength + 13)
      const key = keyBuffer.toString(ENCODING)
      const valueBuffer = readBuffer.subarray(
        13 + keyByteLength,
        13 + keyByteLength + valueByteLength
      )
      const value = valueBuffer.toString(ENCODING)
      return { statusMarker, index, nextIndex, key, value }
    }

    return undefined
  }

  // _setStatusMarker(index number, marker number) -> Promise<>
  async _setStatusMarker(index, marker) {
    const position = index * DATA_SLICE_SIZE
    const buffer = Buffer.alloc(1)
    buffer.writeUInt8(marker, 0)

    await this.storageFd.write(buffer, {
      offset: 0,
      position,
      length: buffer.length,
    })
  }

  // _getHeadIndex() -> headIndex Promise<number>
  async _getHeadIndex() {
    const headerReadBuffer = await this._readHeader()
    const headIndex = headerReadBuffer.readInt32BE(12)
    return headIndex
  }

  // _setHeadIndex(index number) -> Promise<>
  async _setHeadIndex(index) {
    const position = 12
    const buffer = Buffer.alloc(4)
    buffer.writeInt32BE(index, 0)

    await this.headerFd.write(buffer, {
      offset: 0,
      position,
      length: 4,
    })
  }

  // _getNextIndex(index number) -> nextIndex Promise<number>
  async _getNextIndex(index) {
    if (index == -1) {
      throw new Error('Negative index')
    }

    const readBuffer = await this._read(index)
    const nextIndex = readBuffer.readInt32BE(9)
    return nextIndex
  }

  // _set(key string, value string, fd fs.FileHandle) -> Promise<>
  async _set(key, value, fd) {
    let index = this._hash1(key)

    const startIndex = index
    const stepSize = this._hash2(key)

    let currentItem = await this._getItem(index)
    while (currentItem) {
      if (key == currentItem.key) {
        break
      }

      index = (index + stepSize) % this._length
      if (index == startIndex) {
        throw new Error('Disk hash table is full')
      }

      currentItem = await this._getItem(index)
    }

    let nextIndex
    if (currentItem == null) { // insert
      await this._incrementCount()
      nextIndex = await this._getHeadIndex()
      await this._setHeadIndex(index)
    } else { // update
      nextIndex = await this._getNextIndex(index)
      if (currentItem.statusMarker == REMOVED) {
        await this._incrementCount()
        await this._decrementDeletedCount()
      }
    }

    const position = index * DATA_SLICE_SIZE
    const buffer = Buffer.alloc(DATA_SLICE_SIZE)

    // 8 bits / 1 byte for status marker: 0 empty / 1 occupied / 2 deleted
    // 32 bits / 4 bytes for key size
    // 32 bits / 4 bytes for value size
    // 32 bits / 4 bytes for next index
    // chunk for key
    // remainder for value
    const statusMarker = 1
    const keyByteLength = Buffer.byteLength(key, ENCODING)
    const valueByteLength = Buffer.byteLength(value, ENCODING)
    buffer.writeUInt8(statusMarker, 0)
    buffer.writeUint32BE(keyByteLength, 1)
    buffer.writeUint32BE(valueByteLength, 5)
    buffer.writeInt32BE(nextIndex, 9)
    buffer.write(key, 13, keyByteLength, ENCODING)
    buffer.write(value, keyByteLength + 13, valueByteLength, ENCODING)

    await this.storageFd.write(buffer, {
      offset: 0,
      position,
      length: buffer.length,
    })
  }

  /**
   * @name set
   *
   * @docs
   * ```coffeescript [specscript]
   * set(key string, value string) -> Promise<>
   * ```
   *
   * Sets and stores a value by key in the disk hash table.
   *
   * Arguments:
   *   * `key` - `string` - the key to set.
   *   * `value` - `string` - the value to set corresponding to the key.
   *
   * Return:
   *   * Empty promise.
   *
   * ```javascript
   * await ht.set('my-key', 'my-value')
   * ```
   */
  async set(key, value) {
    if (this.resizeRatio > 0 && ((this._count + this._deletedCount) / this._length) >= this.resizeRatio) {
      await this._resize()
    }
    await this._set(key, value)
  }

  /**
   * @name get
   *
   * @docs
   * ```coffeescript [specscript]
   * get(key string) -> value Promise<string>
   * ```
   *
   * Gets a value by key from the disk hash table.
   *
   * Arguments:
   *   * `key` - `string` - the key corresponding to the value.
   *
   * Return:
   *   * `value` - `string` - the value corresponding to the key.
   *
   * ```javascript
   * const value = await ht.get('my-key')
   * ```
   */
  async get(key) {
    let index = this._hash1(key)
    const startIndex = index
    const stepSize = this._hash2(key)

    let currentItem = await this._getItem(index)
    while (currentItem) {
      if (key == currentItem.key) {
        break
      }

      index = (index + stepSize) % this._length
      if (index == startIndex) {
        return undefined // entire table searched
      }

      currentItem = await this._getItem(index)
    }

    if (currentItem == null) {
      return undefined
    }

    if (currentItem.statusMarker == OCCUPIED) {
      return currentItem.value
    }

    return undefined
  }

  /**
   * @name delete
   *
   * @docs
   * ```coffeescript [specscript]
   * delete(key string) -> didDelete Promise<boolean>
   * ```
   *
   * Deletes an item by key from the disk hash table.
   *
   * Arguments:
   *   * `key` - `string` - the key to delete.
   *
   * Return:
   *   * `didDelete` - `boolean` - a promise of whether the key and corresponding value was deleted.
   *
   * ```javascript
   * const didDelete = await ht.delete('my-key')
   * ```
   */
  async delete(key) {
    let index = this._hash1(key)
    const startIndex = index
    const stepSize = this._hash2(key)

    let currentItem = await this._getItem(index)
    while (currentItem) {
      if (key == currentItem.key) {
        break
      }

      index = (index + stepSize) % this._length
      if (index == startIndex) {
        return false // entire table searched
      }

      currentItem = await this._getItem(index)
    }

    if (currentItem == null) {
      return false
    }

    const readBuffer = await this._read(index)
    const statusMarker = readBuffer.readUInt8(0)

    if (statusMarker == OCCUPIED) {
      await this._setStatusMarker(index, REMOVED)
      await this._decrementCount()
      await this._incrementDeletedCount()
      return true
    }

    return false
  }

  // _updateCount() -> Promise<>
  async _updateCount() {
    const position = 4
    const buffer = Buffer.alloc(4)
    buffer.writeUInt32BE(this._count, 0)

    await this.headerFd.write(buffer, {
      offset: 0,
      position,
      length: 4,
    })
  }

  // _updateDeletedCount() -> Promise<>
  async _updateDeletedCount() {
    const position = 8
    const buffer = Buffer.alloc(4)
    buffer.writeUInt32BE(this._deletedCount, 0)

    await this.headerFd.write(buffer, {
      offset: 0,
      position,
      length: 4,
    })
  }

  // _incrementCount() -> Promise<>
  async _incrementCount() {
    this._count += 1
    await this._updateCount()
  }

  // _decrementCount() -> Promise<>
  async _decrementCount() {
    this._count -= 1
    await this._updateCount()
  }

  // _incrementDeletedCount() -> Promise<>
  async _incrementDeletedCount() {
    this._deletedCount += 1
    await this._updateDeletedCount()
  }

  // _decrementDeletedCount() -> Promise<>
  async _decrementDeletedCount() {
    this._deletedCount -= 1
    await this._updateDeletedCount()
  }

  // _resize() -> Promise<>
  async _resize() {
    const currentHeaderFd = this.headerFd
    const currentStorageFd = this.storageFd

    const temporaryStoragePath = `${this.storagePath}-tmp-${crypto.randomUUID()}-${Date.now()}`
    const temporaryHeaderPath = `${this.headerPath}-tmp-${crypto.randomUUID()}-${Date.now()}`

    const temporaryHt = new DiskHashTable({
      initialLength: this._length * this.resizeFactor,
      storagePath: temporaryStoragePath,
      headerPath: temporaryHeaderPath,
    })
    await temporaryHt.init()

    for await (const item of this._itemsIterator()) {
      await temporaryHt.set(item.key, item.value)
    }

    temporaryHt.close()
    this.close()

    await fs.promises.rename(temporaryStoragePath, this.storagePath)
    await fs.promises.rename(temporaryHeaderPath, this.headerPath)

    await this.init()
  }

  /**
   * @name count
   *
   * @docs
   * ```coffeescript [specscript]
   * count() -> number
   * ```
   *
   * Returns the number of items in the disk hash table.
   *
   * Arguments:
   *   * (none)
   *
   * Return:
   *   * `number` - the number of items in the disk hash table.
   *
   * ```javascript
   * const count = ht.count()
   * ```
   */
  count() {
    return this._count
  }

  // _itemsIterator() -> items AsyncGenerator<{ index: number, nextIndex: number, key: string, value: string }>
  async * _itemsIterator() {
    const headIndex = await this._getHeadIndex()
    let item = await this._getItem(headIndex)
    while (item) {
      yield item
      item = await this._getItem(item.nextIndex)
    }
  }

  /**
   * @name iterator
   *
   * @docs
   * ```coffeescript [specscript]
   * iterator() -> values AsyncGenerator<string>
   * ```
   *
   * Returns an iterator of all items in the disk hash table. Items are yielded by reverse insertion order.
   *
   * ```javascript
   * await ht.set('key1', 'value1')
   * await ht.set('key2', 'value2')
   * await ht.set('key3', 'value3')
   *
   * for await (const value of ht.iterator()) {
   *   console.log(value) // value3
   *                      // value2
   *                      // value1
   * }
   * ```
   */
  async * iterator() {
    const headIndex = await this._getHeadIndex()
    let item = await this._getItem(headIndex)
    while (item) {
      yield item.value
      item = await this._getItem(item.nextIndex)
    }
  }

}

module.exports = DiskHashTable
