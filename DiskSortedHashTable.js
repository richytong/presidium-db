/**
 * Presidium DB
 * https://github.com/richytong/presidium-db
 * (c) Richard Tong
 * Presidium DB may be freely distributed under the CFOSS license.
 */

const fs = require('fs')

const DATA_SLICE_SIZE = 512 * 1024

const ENCODING = 'utf8'

const EMPTY = 0

const OCCUPIED = 1

const REMOVED = 2

/**
 * @name DiskSortedHashTable
 *
 * @docs
 * ```coffeescript [specscript]
 * new DiskSortedHashTable(options {
 *   initialLength: number,
 *   storagePath: string,
 *   headerPath: string,
 *   resizeRatio: number,
 *   resizeFactor: number,
 * }) -> sortedHt DiskSortedHashTable
 * ```
 *
 * Presidium DiskSortedHashTable class. Creates a sorted hash table that stores all data on disk.
 *
 * Arguments:
 *   * `options`
 *     * `initialLength` - `number` - the initial length of the disk sorted hash table. Defaults to 1024.
 *     * `storagePath` - `string` - the path to the file used to store the disk sorted hash table data.
 *     * `headerPath` - `string` - the path to the file used to store header information about the disk sorted hash table.
 *     * `resizeRatio` - `number` - the ratio of number of items to table length at which to resize the disk sorted hash table. Minimum value 0 (no resize), maximum value 1. Defaults to 0.
 *     * `resizeFactor` - `number` - the factor that is multiplied with the disk sorted hash table's current length to determine the new table length on a resize.
 *
 * Return:
 *   * `sortedHt` - [`DiskSortedHashTable`](/docs/DiskSortedHashTable) - a `DiskSortedHashTable` instance.
 *
 * ```javascript
 * const sortedHt = new DiskSortedHashTable({
 *   initialLength: 1024,
 *   storagePath: '/path/to/storage-file',
 *   headerPath: '/path/to/header-file',
 *   resizeRatio: 0.5,
 *   resizeFactor: 1000,
 * })
 * ```
 *
 * ## Resizing the disk sorted hash table
 * When an item is inserted into the disk sorted hash table via [set](/docs/DiskHashTable#set), the current capacity ratio of the table is calculated as the table's count divided by the table's length. If the current capacity ratio exceeds the `resizeRatio` (and the `resizeRatio` is not 0), a resize of the table occurs.
 *
 * During a table resize, each item of the table is added into a temporary storage file using the new table length calculated from the equation below:
 *
 * ```
 * newTableLength = oldTableLength * resizeFactor
 * ```
 *
 * Once all of the items have been added into the temporary storage file, the temporary storage file is moved to the location of the old storage file to be used as the new storage file.
 */
class DiskSortedHashTable {
  constructor(options) {
    this.initialLength = options.initialLength ?? 1024
    this._length = null
    this._count = null
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
    headerReadBuffer.writeInt32BE(-1, 8)
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
   * sortedHt.init() -> Promise<>
   * ```
   *
   * Initializes the disk sorted hash table.
   *
   * Arguments:
   *   * (none)
   *
   * Return:
   *   * Empty promise.
   *
   * ```javascript
   * await sortedHt.init()
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
  }

  /**
   * @name clear
   *
   * @docs
   * ```coffeescript [specscript]
   * clear() -> Promise<>
   * ```
   *
   * Clears all data from the disk sorted hash table.
   *
   * Arguments:
   *   * (none)
   *
   * Return:
   *   * Empty promise.
   *
   * ```javascript
   * await sortedHt.clear()
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
  }

  /**
   * @name destroy
   *
   * @docs
   * ```coffeescript [specscript]
   * destroy() -> Promise<>
   * ```
   *
   * Removes all system resources used by the disk sorted hash table.
   *
   * Arguments:
   *   * (none)
   *
   * Return:
   *   * Empty promise.
   *
   * ```javascript
   * await sortedHt.destroy()
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
   * Closes the underlying file handles used by the disk sorted hash table.
   *
   * Arguments:
   *   * (none)
   *
   * Return:
   *   * `undefined`
   *
   * ```javascript
   * sortedHt.close()
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
  // 32 bits / 4 bytes first item index
  // 32 bits / 4 bytes last item index

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

  // _writeFirstIndex(index number) -> Promise<>
  async _writeFirstIndex(index) {
    const position = 8
    const buffer = Buffer.alloc(4)
    buffer.writeInt32BE(index, 0)

    await this.headerFd.write(buffer, {
      offset: 0,
      position,
      length: buffer.length,
    })
  }

  // _writeLastIndex(index number) -> Promise<>
  async _writeLastIndex(index) {
    const position = 12
    const buffer = Buffer.alloc(4)
    buffer.writeInt32BE(index, 0)

    await this.headerFd.write(buffer, {
      offset: 0,
      position,
      length: buffer.length,
    })
  }

  // _getKey(index number) -> key Promise<string>
  async _getKey(index) {
    if (index == -1) {
      throw new Error('Negative index')
    }

    const readBuffer = await this._read(index)

    const statusMarker = readBuffer.readUInt8(0)
    if (statusMarker === EMPTY) {
      return undefined
    }

    const keyByteLength = readBuffer.readUInt32BE(1)
    const keyBuffer = readBuffer.subarray(21, keyByteLength + 21)
    return keyBuffer.toString(ENCODING)
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

  // _parseItem(readBuffer Buffer, index number) -> { index: number, readBuffer: Buffer, sortValue: string|number, key: string, value: string }
  _parseItem(readBuffer, index) {
    const item = {}
    item.index = index
    item.readBuffer = readBuffer

    const statusMarker = readBuffer.readUInt8(0)
    item.statusMarker = statusMarker

    const forwardIndex = readBuffer.readInt32BE(13)
    const reverseIndex = readBuffer.readInt32BE(17)
    item.forwardIndex = forwardIndex
    item.reverseIndex = reverseIndex

    const keyByteLength = readBuffer.readUInt32BE(1)
    const keyBuffer = readBuffer.subarray(21, keyByteLength + 21)
    const key = keyBuffer.toString(ENCODING)
    item.key = key

    const sortValueByteLength = readBuffer.readUInt32BE(5)
    const sortValueBuffer = readBuffer.subarray(
      21 + keyByteLength,
      21 + keyByteLength + sortValueByteLength
    )
    const sortValue = sortValueBuffer.toString(ENCODING)
    item.sortValue = sortValue

    const valueByteLength = readBuffer.readUInt32BE(9)
    const valueBuffer = readBuffer.subarray(
      21 + keyByteLength + sortValueByteLength,
      21 + keyByteLength + sortValueByteLength + valueByteLength
    )
    const value = valueBuffer.toString(ENCODING)
    item.value = value

    return item
  }

  // _getForwardStartItem() -> item { index: number, readBuffer: Buffer, sortValue: string|number, value: string }
  async _getForwardStartItem() {
    const headerReadBuffer = await this._readHeader()
    const index = headerReadBuffer.readInt32BE(8)
    if (index == -1) {
      return undefined
    }
    const readBuffer = await this._read(index)
    return this._parseItem(readBuffer, index)
  }

  // _getReverseStartItem() -> item { index: number, readBuffer: Buffer, sortValue: string|number, value: string }
  async _getReverseStartItem() {
    const headerReadBuffer = await this._readHeader()
    const index = headerReadBuffer.readInt32BE(12)
    if (index == -1) {
      return undefined
    }
    const readBuffer = await this._read(index)
    return this._parseItem(readBuffer, index)
  }

  // _getItem(index number) -> item { index: number, readBuffer: Buffer, sortValue: string|number, value: string }
  async _getItem(index) {
    if (index == -1) {
      return undefined
    }
    const readBuffer = await this._read(index)
    return this._parseItem(readBuffer, index)
  }

  // _updateForwardIndex(index number, forwardIndex number) -> Promise<>
  async _updateForwardIndex(index, forwardIndex) {
    const position = (index * DATA_SLICE_SIZE) + 13
    const buffer = Buffer.alloc(4)
    buffer.writeInt32BE(forwardIndex, 0)

    await this.storageFd.write(buffer, {
      offset: 0,
      position,
      length: buffer.length,
    })
  }

  // _updateReverseIndex(index number, reverseIndex number) -> Promise<>
  async _updateReverseIndex(index, reverseIndex) {
    const position = (index * DATA_SLICE_SIZE) + 17
    const buffer = Buffer.alloc(4)
    buffer.writeInt32BE(reverseIndex, 0)

    await this.storageFd.write(buffer, {
      offset: 0,
      position,
      length: buffer.length,
    })
  }

  // _updateLength() -> Promise<>
  async _updateLength() {
    const position = 0
    const buffer = Buffer.alloc(4)
    buffer.writeUInt32BE(this._length, 0)

    await this.headerFd.write(buffer, {
      offset: 0,
      position,
      length: 4,
    })
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

  // _insert(key string, value string, sortValue number|string, index number) -> Promise<>
  async _insert(key, value, sortValue, index) {
    const forwardStartItem = await this._getForwardStartItem()
    let previousForwardItem = null
    let currentForwardItem = forwardStartItem
    while (currentForwardItem) {
      const left = typeof sortValue == 'string' ? currentForwardItem.sortValue : Number(currentForwardItem.sortValue)
      if (sortValue > left) {
        previousForwardItem = currentForwardItem
        currentForwardItem = await this._getItem(previousForwardItem.forwardIndex)
        continue
      }
      break
    }

    let reverseIndex = -1
    let forwardIndex = -1
    if (previousForwardItem == null) { // item to insert is first in the list
      await this._writeFirstIndex(index)
      if (forwardStartItem == null) { // item to insert is also last in the list
        await this._writeLastIndex(index)
      } else {
        forwardIndex = forwardStartItem.index
        await this._updateReverseIndex(forwardStartItem.index, index)
      }
    } else if (previousForwardItem.forwardIndex == -1) { // item to insert is the last in the list
      await this._writeLastIndex(index)
      await this._updateForwardIndex(previousForwardItem.index, index)
      reverseIndex = previousForwardItem.index
    } else { // item to insert is ahead of previousForwardItem and there was an item ahead of previousForwardItem
      await this._updateForwardIndex(previousForwardItem.index, index)
      await this._updateReverseIndex(currentForwardItem.index, index)
      forwardIndex = previousForwardItem.forwardIndex
      reverseIndex = previousForwardItem.index
    }

    const position = index * DATA_SLICE_SIZE
    const buffer = Buffer.alloc(DATA_SLICE_SIZE)
    const sortValueString = typeof sortValue == 'string' ? sortValue : sortValue.toString()

    // 8 bits / 1 byte for status marker: 0 empty / 1 occupied / 2 deleted
    // 32 bits / 4 bytes for key size
    // 32 bits / 4 bytes for sort value size
    // 32 bits / 4 bytes for value size
    // 32 bits / 4 bytes for forward index
    // 32 bits / 4 bytes for reverse index
    // chunk for key
    // chunk for sort value
    // remainder for value
    const statusMarker = 1
    const keyByteLength = Buffer.byteLength(key, ENCODING)
    const sortValueByteLength = Buffer.byteLength(sortValueString, ENCODING)
    const valueByteLength = Buffer.byteLength(value, ENCODING)
    buffer.writeUInt8(statusMarker, 0)
    buffer.writeUInt32BE(keyByteLength, 1)
    buffer.writeUInt32BE(sortValueByteLength, 5)
    buffer.writeUInt32BE(valueByteLength, 9)
    buffer.writeInt32BE(forwardIndex, 13)
    buffer.writeInt32BE(reverseIndex, 17)
    buffer.write(key, 21, keyByteLength, ENCODING)
    buffer.write(sortValueString, 21 + keyByteLength, sortValueByteLength, ENCODING)
    buffer.write(value, 21 + keyByteLength + sortValueByteLength, valueByteLength, ENCODING)

    await this.storageFd.write(buffer, {
      offset: 0,
      position,
      length: buffer.length,
    })
  }

  // _update(key string, value string, sortValue number|string, index number) -> Promise<>
  async _update(key, value, sortValue, index) {
    const item = await this._getItem(index)

    let forwardIndex = item.forwardIndex
    let reverseIndex = item.reverseIndex

    if (sortValue != item.sortValue) {
      if (item.reverseIndex == -1) { // item to update is first in the list
        if (item.forwardIndex > -1) { // there is an item behind item to update
          await this._updateReverseIndex(item.forwardIndex, -1)
          await this._writeFirstIndex(item.forwardIndex)
        } else { // item to update is first and last in the list
          await this._writeFirstIndex(-1)
          await this._writeLastIndex(-1)
        }
      } else if (item.forwardIndex == -1) { // item to update is last in the list
        if (item.reverseIndex > -1) { // there is an item ahead of item to update
          await this._updateForwardIndex(item.reverseIndex, -1)
          await this._writeLastIndex(item.forwardIndex)
        } else { // item to update is first and last in the list
        }
      } else { // item to update is in the middle of the list
        await this._updateReverseIndex(item.forwardIndex, item.reverseIndex)
        await this._updateForwardIndex(item.reverseIndex, item.forwardIndex)
      }

      const forwardStartItem = await this._getForwardStartItem()
      let previousForwardItem = null
      let currentForwardItem = forwardStartItem
      while (currentForwardItem) {
        const left = typeof sortValue == 'string' ? currentForwardItem.sortValue : Number(currentForwardItem.sortValue)
        if (sortValue > left) {
          previousForwardItem = currentForwardItem
          currentForwardItem = await this._getItem(previousForwardItem.forwardIndex)
          continue
        }
        break
      }

      if (previousForwardItem == null) { // item to update is first in the list
        reverseIndex = -1
        await this._writeFirstIndex(index)
        if (forwardStartItem == null) { // item to update is also last in the list
          forwardIndex = -1
          await this._writeLastIndex(index)
        } else {
          forwardIndex = forwardStartItem.index
          await this._updateReverseIndex(forwardStartItem.index, index)
        }
      } else if (previousForwardItem.forwardIndex == -1) { // item to insert is the last in the list
        forwardIndex = -1
        await this._writeLastIndex(index)
        await this._updateForwardIndex(previousForwardItem.index, index)
        reverseIndex = previousForwardItem.index
      } else { // item to insert is ahead of previousForwardItem and there was an item ahead of previousForwardItem
        await this._updateForwardIndex(previousForwardItem.index, index)
        await this._updateReverseIndex(currentForwardItem.index, index)
        forwardIndex = previousForwardItem.forwardIndex
        reverseIndex = previousForwardItem.index
      }

    }

    const position = index * DATA_SLICE_SIZE
    const buffer = Buffer.alloc(DATA_SLICE_SIZE)
    const sortValueString = typeof sortValue == 'string' ? sortValue : sortValue.toString()

    // 8 bits / 1 byte for status marker: 0 empty / 1 occupied / 2 deleted
    // 32 bits / 4 bytes for key size
    // 32 bits / 4 bytes for sort value size
    // 32 bits / 4 bytes for value size
    // 32 bits / 4 bytes for forward index
    // 32 bits / 4 bytes for reverse index
    // chunk for key
    // chunk for sort value
    // remainder for value
    const statusMarker = 1
    const keyByteLength = Buffer.byteLength(key, ENCODING)
    const sortValueByteLength = Buffer.byteLength(sortValueString, ENCODING)
    const valueByteLength = Buffer.byteLength(value, ENCODING)
    buffer.writeUInt8(statusMarker, 0)
    buffer.writeUInt32BE(keyByteLength, 1)
    buffer.writeUInt32BE(sortValueByteLength, 5)
    buffer.writeUInt32BE(valueByteLength, 9)
    buffer.writeInt32BE(forwardIndex, 13)
    buffer.writeInt32BE(reverseIndex, 17)
    buffer.write(key, 21, keyByteLength, ENCODING)
    buffer.write(sortValueString, 21 + keyByteLength, sortValueByteLength, ENCODING)
    buffer.write(value, 21 + keyByteLength + sortValueByteLength, valueByteLength, ENCODING)

    await this.storageFd.write(buffer, {
      offset: 0,
      position,
      length: buffer.length,
    })
  }

  // _resize() -> Promise<>
  async _resize() {
    const currentHeaderFd = this.headerFd
    const currentStorageFd = this.storageFd

    const temporaryStoragePath = `${this.storagePath}-tmp-${crypto.randomUUID()}-${Date.now()}`
    const temporaryHeaderPath = `${this.headerPath}-tmp-${crypto.randomUUID()}-${Date.now()}`

    const temporaryHt = new DiskSortedHashTable({
      initialLength: this._length * this.resizeFactor,
      storagePath: temporaryStoragePath,
      headerPath: temporaryHeaderPath,
    })
    await temporaryHt.init()

    for await (const item of this._forwardItemsIterator()) {
      await temporaryHt.set(item.key, item.value, item.sortValue)
    }

    temporaryHt.close()
    this.close()

    await fs.promises.rename(temporaryStoragePath, this.storagePath)
    await fs.promises.rename(temporaryHeaderPath, this.headerPath)

    await this.init()
  }

  // _set(key string, value string, sortValue number|string) -> Promise<>
  async _set(key, value, sortValue) {
    let index = this._hash1(key)

    const startIndex = index
    const stepSize = this._hash2(key)

    let currentKey = await this._getKey(index)
    while (currentKey) {
      if (key == currentKey) {
        break
      }
      index = (index + stepSize) % this._length
      if (index == startIndex) {
        throw new Error('Hash table is full')
      }
      currentKey = await this._getKey(index)
    }

    if (currentKey == null) {
      await this._insert(key, value, sortValue, index)
      await this._incrementCount()
    } else {
      await this._update(key, value, sortValue, index)
    }
  }

  /**
   * @name set
   *
   * @docs
   * ```coffeescript [specscript]
   * set(
   *   key string,
   *   value string,
   *   sortValue string|number
   * ) -> Promise<>
   * ```
   */
  /**
   * @name set
   *
   * @docs
   * ```coffeescript [specscript]
   * set(key string, value string, sortValue string|number) -> Promise<>
   * ```
   *
   * Sets and stores a value by key and sort-value in the disk sorted hash table.
   *
   * Arguments:
   *   * `key` - `string` - the key to set.
   *   * `value` - `string` - the value to set corresponding to the key.
   *   * `sortValue` - `string|number` - the value by which the item is sorted in the disk sorted hash table.
   *
   * Return:
   *   * Empty promise.
   *
   * ```javascript
   * await sortedHt.set('key1', 'value1', 1)
   * await sortedHt.set('key2', 'value2', 2)
   * await sortedHt.set('key3', 'value3', 3)
   * ```
   */
  async set(key, value, sortValue) {
    if (this.resizeRatio > 0 && (this._count / this._length) >= this.resizeRatio) {
      await this._resize()
    }
    await this._set(key, value, sortValue)
  }

  /**
   * @name get
   *
   * @docs
   * ```coffeescript [specscript]
   * get(key string) -> value Promise<string>
   * ```
   *
   * Gets a value by key from the disk sorted hash table.
   *
   * Arguments:
   *   * `key` - `string` - the key corresponding to the value.
   *
   * Return:
   *   * `value` - `string` - the value corresponding to the key.
   *
   * ```javascript
   * const value = await sortedHt.get('my-key')
   * ```
   */
  async get(key) {
    let index = this._hash1(key)
    const startIndex = index
    const stepSize = this._hash2(key)

    let currentKey = await this._getKey(index)
    while (currentKey) {
      if (key == currentKey) {
        break
      }

      index = (index + stepSize) % this._length
      if (index == startIndex) {
        return undefined // entire table searched
      }

      currentKey = await this._getKey(index)
    }

    if (currentKey == null) {
      return undefined
    }

    const readBuffer = await this._read(index)

    const statusMarker = readBuffer.readUInt8(0)
    if (statusMarker === OCCUPIED) {
      const keyByteLength = readBuffer.readUInt32BE(1)
      const sortValueByteLength = readBuffer.readUInt32BE(5)
      const valueByteLength = readBuffer.readUInt32BE(9)
      const valueBuffer = readBuffer.subarray(
        21 + keyByteLength + sortValueByteLength,
        21 + keyByteLength + sortValueByteLength + valueByteLength
      )
      return valueBuffer.toString(ENCODING)
    }

    return undefined
  }

  // _forwardItemsIterator() -> values AsyncGenerator<string>
  async * _forwardItemsIterator() {
    let currentForwardItem = await this._getForwardStartItem()
    while (currentForwardItem) {
      yield currentForwardItem
      currentForwardItem = await this._getItem(currentForwardItem.forwardIndex)
    }
  }

  /**
   * @name forwardIterator
   *
   * @docs
   * ```coffeescript [specscript]
   * forwardIterator() -> values AsyncGenerator<string>
   * ```
   *
   * Returns a iterator of all items in the disk hash table sorted by sort-value. Items are yielded in ascending order.
   *
   * ```javascript
   * await ht.set('key1', 'value1', 1)
   * await ht.set('key2', 'value2', 2)
   * await ht.set('key3', 'value3', 3)
   *
   * for await (const value of ht.forwardIterator()) {
   *   console.log(value) // value1
   *                      // value2
   *                      // value3
   * }
   * ```
   */
  async * forwardIterator() {
    let currentForwardItem = await this._getForwardStartItem()
    while (currentForwardItem) {
      yield currentForwardItem.value
      currentForwardItem = await this._getItem(currentForwardItem.forwardIndex)
    }
  }

  /**
   * @name reverseIterator
   *
   * @docs
   * ```coffeescript [specscript]
   * reverseIterator() -> values AsyncGenerator<string>
   * ```
   *
   * Returns a iterator of all items in the disk hash table sorted by sort-value. Items are yielded in descending order.
   *
   * ```javascript
   * await ht.set('key1', 'value1', 1)
   * await ht.set('key2', 'value2', 2)
   * await ht.set('key3', 'value3', 3)
   *
   * for await (const value of ht.reverseIterator()) {
   *   console.log(value) // value3
   *                      // value2
   *                      // value1
   * }
   * ```
   */
  async * reverseIterator() {
    let currentReverseItem = await this._getReverseStartItem()
    while (currentReverseItem) {
      yield currentReverseItem.value
      currentReverseItem = await this._getItem(currentReverseItem.reverseIndex)
    }
  }

  /**
   * @name delete
   *
   * @docs
   * ```coffeescript [specscript]
   * delete(key string) -> didDelete Promise<boolean>
   * ```
   *
   * Deletes an item by key from the disk sorted hash table.
   *
   * Arguments:
   *   * `key` - `string` - the key to delete.
   *
   * Return:
   *   * `didDelete` - `boolean` - a promise of whether the key and corresponding value was deleted.
   *
   * ```javascript
   * const didDelete = await sortedHt.delete('my-key')
   * ```
   */
  async delete(key) {
    let index = this._hash1(key)
    const startIndex = index
    const stepSize = this._hash2(key)

    let currentKey = await this._getKey(index)
    while (currentKey) {
      if (key == currentKey) {
        break
      }

      index = (index + stepSize) % this._length
      if (index == startIndex) {
        return false // entire table searched
      }

      currentKey = await this._getKey(index)
    }

    if (currentKey == null) {
      return false
    }

    const item = await this._getItem(index)

    if (item.reverseIndex == -1) { // item to delete is first in the list
      if (item.forwardIndex > -1) { // there is an item behind item to delete
        await this._updateReverseIndex(item.forwardIndex, -1)
        await this._writeFirstIndex(item.forwardIndex)
      } else { // item to remove is first and last in the list
        await this._writeFirstIndex(-1)
        await this._writeLastIndex(-1)
      }
    } else if (item.forwardIndex == -1) { // item to delete is last in the list
      if (item.reverseIndex > -1) { // there is an item ahead of item to delete
        await this._updateForwardIndex(item.reverseIndex, -1)
        await this._writeLastIndex(item.forwardIndex)
      } else { // item is first and last in the list (handled above)
      }
    } else { // item to delete is in the middle of the list
      await this._updateReverseIndex(item.forwardIndex, item.reverseIndex)
      await this._updateForwardIndex(item.reverseIndex, item.forwardIndex)
    }

    if (item.statusMarker === OCCUPIED) {
      await this._setStatusMarker(index, REMOVED)
      await this._decrementCount()
      return true
    }

    return false
  }

  /**
   * @name count
   *
   * @docs
   * ```coffeescript [specscript]
   * count() -> number
   * ```
   *
   * Returns the number of items in the disk sorted hash table.
   *
   * Arguments:
   *   * (none)
   *
   * Return:
   *   * `number` - the number of items in the disk sorted hash table.
   *
   * ```javascript
   * const count = sortedHt.count()
   * ```
   */
  count() {
    return this._count
  }

}

module.exports = DiskSortedHashTable
