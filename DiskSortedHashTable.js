/**
 * Presidium DB
 * https://github.com/richytong/presidium-db
 * (c) Richard Tong
 * Presidium DB may be freely distributed under the CFOSS license.
 */

const fs = require('fs')
const preallocate = require('./_internal/preallocate')
const convert = require('./_internal/convert')
const getLeftParentItem = require('./_internal/getLeftParentItem')
const getRightParentItem = require('./_internal/getRightParentItem')
const getLeftmostNodeItemExcluding = require('./_internal/getLeftmostNodeItemExcluding')
const getRightmostNodeItemExcluding = require('./_internal/getRightmostNodeItemExcluding')

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
 *   storagePath: string,
 *   headerPath: string,
 *   initialLength: number,
 *   itemSize: number,
 *   sortValueType: 'string'|'number',
 *   resizeRatio: number,
 *   resizeFactor: number,
 *   degree: number,
 * }) -> sortedHt DiskSortedHashTable
 * ```
 *
 * Presidium DiskSortedHashTable class. Creates a sorted hash table that stores all data on disk.
 *
 * Arguments:
 *   * `options`
 *     * `storagePath` - `string` - the path to the file used to store the disk sorted hash table data.
 *     * `headerPath` - `string` - the path to the file used to store header information about the disk sorted hash table.
 *     * `initialLength` - `number` - the initial length of the disk sorted hash table. Minimum value 1024, maximum value `Math.floor(9007199254740991 / itemSize)`. Defaults to 1024.
 *     * `itemSize` - `number` - the size in bytes of each item (including internal item info, key, value, and sortValue) stored on disk. Minimum value 1024. Defaults to 524288.
 *     * `sortValueType` - `'string'|'number'` - the type of the disk sorted hash table sort-values.
 *     * `resizeRatio` - `number` - the ratio of number of items to table length at which to resize the disk sorted hash table. Minimum value 0 (no resize), maximum value 1. Defaults to 0.
 *     * `resizeFactor` - `number` - the factor that is multiplied with the disk sorted hash table's current length to determine the new table length on a resize.
 *     * `degree` - `number` - minimum value 2, defaults to 2 - defines the following parameters for the internal b-tree that organizes all of the items in the disk sorted hash table:
 *       * Minimum number of items per b-tree node: `degree - 1`
 *       * Maximum number of items per b-tree node: `(2 * degree) - 1`
 *       * Minimum number of children per b-tree node: `degree`
 *       * Maximum number of children per b-tree node: `2 * degree`
 *
 * Return:
 *   * `sortedHt` - [`DiskSortedHashTable`](/docs/DiskSortedHashTable) - a `DiskSortedHashTable` instance.
 *
 * ```javascript
 * const sortedHt = new DiskSortedHashTable({
 *   storagePath: '/path/to/storage-file',
 *   headerPath: '/path/to/header-file',
 *   initialLength: 1024,
 *   itemSize: 512 * 1024,
 *   sortValueType: 'number',
 *   resizeRatio: 0.5,
 *   resizeFactor: 1000,
 *   degree: 2,
 * })
 * ```
 *
 * Supported platforms:
 *   * `linux64`
 *
 * ## Maximum length of the disk sorted hash table
 * The maximum length of the disk sorted hash table is represented by the following equation:
 * ```
 * Math.floor(9_007_199_254_740_991 / itemSize)
 * ```
 *
 * ## Allocation of disk space
 * The disk sorted hash table initially preallocates a block of memory on disk of `(itemSize * initialLength)` bytes as the storage file and a 48-byte block of memory as the header file for database operations. When the disk sorted hash table is resized, the block of memory on disk is reallocated to a new size of `(itemSize * initialLength * numberOfResizes * resizeFactor)` bytes.
 *
 * ## Resizing the disk sorted hash table
 * When an item is inserted into the disk sorted hash table via [set](/docs/DiskSortedHashTable#set), the current capacity ratio of the table is calculated as the sum of the table's count and deleted count divided by the table's length. If the current capacity ratio exceeds the `resizeRatio` (and the `resizeRatio` is not 0), a resize of the table occurs.
 *
 * During a table resize, each item of the table is added into a temporary storage file using the new table length calculated from the equation below:
 *
 * ```
 * newTableLength = oldTableLength * resizeFactor
 * ```
 *
 * Once all of the items have been added into the temporary storage file, the temporary storage file is moved to the location of the old storage file to be used as the new storage file.
 *
 * ## Optimizing the disk sorted hash table b-tree
 * The value of `degree` determines the structure of the internal b-tree used by the disk sorted hash table. A higher value for `degree` results in a shorter b-tree and more items per b-tree node, while a lower value results in a taller b-tree and fewer items per b-tree node. The default value of 2 is a safe choice for most use cases.
 *
 */
class DiskSortedHashTable {
  constructor(options) {
    this.storagePath = options.storagePath
    this.headerPath = options.headerPath
    this.initialLength = options.initialLength ?? 1024
    this.itemSize = options.itemSize ?? 512 * 1024
    this.sortValueType = options.sortValueType
    this._length = null
    this._count = null
    this._deletedCount = null
    this.storageFd = null
    this.headerFd = null
    this.resizeRatio = options.resizeRatio ?? 0
    this.resizeFactor = options.resizeFactor ?? 4
    this.degree = options.degree ?? 2
  }

  // _initializeHeader() -> headerReadBuffer Promise<Buffer>
  async _initializeHeader() {
    const headerReadBuffer = Buffer.alloc(48)
    headerReadBuffer.writeBigUInt64BE(BigInt(this.initialLength), 0)
    headerReadBuffer.writeBigUInt64BE(BigInt(0), 8)
    headerReadBuffer.writeBigUInt64BE(BigInt(0), 16)
    headerReadBuffer.writeBigInt64BE(BigInt(-1), 24)
    headerReadBuffer.writeBigInt64BE(BigInt(-1), 32)
    headerReadBuffer.writeBigInt64BE(BigInt(-1), 40)

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

    const length = Number(headerReadBuffer.readBigUInt64BE(0))
    this._length = length

    const count = Number(headerReadBuffer.readBigUInt64BE(8))
    this._count = count

    const deletedCount = Number(headerReadBuffer.readBigUInt64BE(16))
    this._deletedCount = deletedCount

    await preallocate(this.headerPath, 48)
    await preallocate(this.storagePath, BigInt(this.itemSize) * BigInt(length))
  }

  /**
   * @name clear
   *
   * @docs
   * ```coffeescript [specscript]
   * clear() -> Promise<>
   * ```
   *
   * Clears all data from the disk sorted hash table. Reallocates the header and storage files.
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

    const length = Number(headerReadBuffer.readBigUInt64BE(0))
    this._length = length

    const count = Number(headerReadBuffer.readBigUInt64BE(8))
    this._count = count

    const deletedCount = Number(headerReadBuffer.readBigUInt64BE(16))
    this._deletedCount = deletedCount

    await preallocate(this.headerPath, 48)
    await preallocate(this.storagePath, BigInt(this.itemSize) * BigInt(length))
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
    let hash = 0
    const prime = 31
    for (let i = 0; i < key.length; i++) {
      hash = (prime * hash + key.charCodeAt(i)) % this._length
    }
    return hash
  }

  // _hash2(key string) -> number
  _hash2(key) {
    let hash = 0
    for (let i = 0; i < key.length; i++) {
      hash = (hash << 3) - hash + key.charCodeAt(i)
    }
    const prime = 3
    return prime - (hash % prime)
  }

  // header file
  // 8 bytes for table length
  // 8 bytes for item count
  // 8 bytes for deleted item count
  // 8 bytes for first item index
  // 8 bytes for last item index
  // 8 bytes for btree root rightmost item index

  // _readHeader() -> headerReadBuffer Promise<Buffer>
  async _readHeader() {
    const headerReadBuffer = Buffer.alloc(48)

    await this.headerFd.read({
      buffer: headerReadBuffer,
      offset: 0,
      position: 0,
      length: 48,
    })

    return headerReadBuffer
  }

  // _readKey(index number, keyByteLength number) -> key string
  async _readKey(index, keyByteLength) {
    const position = (index * this.itemSize) + 53
    const keyBuffer = Buffer.alloc(keyByteLength)

    await this.storageFd.read({
      buffer: keyBuffer,
      offset: 0,
      position,
      length: keyByteLength,
    })

    return keyBuffer.toString(ENCODING)
  }

  // _readSortValue(index number, keyByteLength number, sortValueByteLength number) -> sortValue number|string
  async _readSortValue(index, keyByteLength, sortValueByteLength) {
    const position = (index * this.itemSize) + 53 + keyByteLength
    const sortValueBuffer = Buffer.alloc(sortValueByteLength)

    await this.storageFd.read({
      buffer: sortValueBuffer,
      offset: 0,
      position,
      length: sortValueByteLength,
    })

    const sortValue = convert(sortValueBuffer.toString(ENCODING), this.sortValueType)

    return sortValue
  }

  // _readBinaryValue(index number, keyByteLength number, sortValueByteLength number, valueByteLength number) -> key string
  async _readBinaryValue(index, keyByteLength, sortValueByteLength, valueByteLength) {
    const position = (index * this.itemSize) + 53 + keyByteLength + sortValueByteLength
    const valueBuffer = Buffer.alloc(valueByteLength)

    await this.storageFd.read({
      buffer: valueBuffer,
      offset: 0,
      position,
      length: valueByteLength,
    })

    return valueBuffer
  }

  // _readHead(index number) -> readBuffer Promise<Buffer>
  async _readHead(index) {
    const position = index * this.itemSize
    const readBuffer = Buffer.alloc(53)

    await this.storageFd.read({
      buffer: readBuffer,
      offset: 0,
      position,
      length: 53,
    })

    return readBuffer
  }

  // _read(index number) -> readBuffer Promise<Buffer>
  async _read(index) {
    const position = index * this.itemSize
    const readBuffer = Buffer.alloc(this.itemSize)

    await this.storageFd.read({
      buffer: readBuffer,
      offset: 0,
      position,
      length: this.itemSize,
    })

    return readBuffer
  }

  // _writeFirstIndex(index number) -> Promise<>
  async _writeFirstIndex(index) {
    const position = 24
    const buffer = Buffer.alloc(8)
    buffer.writeBigInt64BE(BigInt(index), 0)

    await this.headerFd.write(buffer, {
      offset: 0,
      position,
      length: buffer.length,
    })
  }

  // _writeLastIndex(index number) -> Promise<>
  async _writeLastIndex(index) {
    const position = 32
    const buffer = Buffer.alloc(8)
    buffer.writeBigInt64BE(BigInt(index), 0)

    await this.headerFd.write(buffer, {
      offset: 0,
      position,
      length: buffer.length,
    })
  }

  // _writeBTreeRootRightmostItemIndex(index number) -> Promise<>
  async _writeBTreeRootRightmostItemIndex(index) {
    const position = 40
    const buffer = Buffer.alloc(8)
    buffer.writeBigInt64BE(BigInt(index), 0)

    await this.headerFd.write(buffer, {
      offset: 0,
      position,
      length: buffer.length,
    })
  }

  // _writeBTreeLeftChildNodeRightmostItemIndex(
  //   btreeNodeItemIndex number,
  //   btreeLeftChildNodeRightmostItemIndex number
  // ) -> Promise<>
  async _writeBTreeLeftChildNodeRightmostItemIndex(
    btreeNodeItemIndex, btreeLeftChildNodeRightmostItemIndex
  ) {
    const position = (btreeNodeItemIndex * this.itemSize) + 29
    const buffer = Buffer.alloc(8)
    buffer.writeBigInt64BE(BigInt(btreeLeftChildNodeRightmostItemIndex), 0)

    await this.storageFd.write(buffer, {
      offset: 0,
      position,
      length: buffer.length,
    })
  }

  // _writeBTreeRightChildNodeRightmostItemIndex(
  //   btreeNodeItemIndex number,
  //   btreeRightChildNodeRightmostItemIndex number
  // ) -> Promise<>
  async _writeBTreeRightChildNodeRightmostItemIndex(
    btreeNodeItemIndex, btreeRightChildNodeRightmostItemIndex
  ) {
    const position = (btreeNodeItemIndex * this.itemSize) + 37
    const buffer = Buffer.alloc(8)
    buffer.writeBigInt64BE(BigInt(btreeRightChildNodeRightmostItemIndex), 0)

    await this.storageFd.write(buffer, {
      offset: 0,
      position,
      length: buffer.length,
    })
  }

  // _writeBTreeLeftItemIndex(
  //   btreeNodeItemIndex number,
  //   btreeLeftItemIndex number
  // ) -> Promise<>
  async _writeBTreeLeftItemIndex(
    btreeNodeItemIndex, btreeLeftItemIndex
  ) {
    const position = (btreeNodeItemIndex * this.itemSize) + 45
    const buffer = Buffer.alloc(8)
    buffer.writeBigInt64BE(BigInt(btreeLeftItemIndex), 0)

    await this.storageFd.write(buffer, {
      offset: 0,
      position,
      length: buffer.length,
    })
  }

  // _getBTreeRootNodeRightmostItem() -> btreeRootNodeRightmostItem Promise<{
  //   index: number,
  //   sortValue: string|number,
  //   btreeLeftChildNodeRightmostItemIndex: number,
  //   btreeRightChildNodeRightmostItemIndex: number,
  //   btreeLeftItemIndex: number,
  // }>
  async _getBTreeRootNodeRightmostItem() {
    const headerReadBuffer = await this._readHeader()
    const index = Number(headerReadBuffer.readBigInt64BE(40))

    if (index == -1) {
      return undefined
    }

    const headReadBuffer = await this._readHead(index)
    const item = this._parseItemHead(headReadBuffer, index)
    const sortValue = await this._readSortValue(index, item.keyByteLength, item.sortValueByteLength)

    item.sortValue = sortValue

    return item
  }

  // _getHeadItem(index number) -> headItem Promise<{ index: number, forwardIndex: number, reverseIndex: number }>
  async _getHeadItem(index) {
    const headReadBuffer = await this._readHead(index)
    const item = this._parseItemHead(headReadBuffer, index)
    return item
  }

  // _getKeyItem(index number) -> keyItem Promise<{ index: number, key: string }>
  async _getKeyItem(index) {
    const headReadBuffer = await this._readHead(index)
    const item = this._parseItemHead(headReadBuffer, index)

    if (item.statusMarker == EMPTY) {
      return undefined
    }

    const key = await this._readKey(index, item.keyByteLength)

    item.key = key

    return item
  }

  // _getBTreeItem(index number) -> btreeItem Promise<{
  //   index: number,
  //   sortValue: string|number,
  //   btreeLeftChildNodeRightmostItemIndex: number,
  //   btreeRightChildNodeRightmostItemIndex: number,
  //   btreeLeftItemIndex: number,
  // }>
  async _getBTreeItem(index) {
    const headReadBuffer = await this._readHead(index)
    const item = this._parseItemHead(headReadBuffer, index)
    const sortValue = await this._readSortValue(index, item.keyByteLength, item.sortValueByteLength)

    item.sortValue = sortValue

    return item
  }

  // _getBTreeNodeItems(btreeRightmostItem { index: number, btreeLeftItemIndex: number }) -> Promise<number>
  async _getBTreeNodeItems(btreeRightmostItem) {
    if (btreeRightmostItem == null) {
      return []
    }
    const btreeNodeItems = [btreeRightmostItem]
    let currentBTreeItem = btreeRightmostItem

    while (currentBTreeItem.btreeLeftItemIndex > -1) {
      currentBTreeItem = await this._getBTreeItem(currentBTreeItem.btreeLeftItemIndex)
      btreeNodeItems.unshift(currentBTreeItem)
    }

    return btreeNodeItems
  }

  // _splitBTreeChildNodeRight(
  //   btreeChildNodeItems Array<{
  //     index: number,
  //     sortValue: string|number,
  //     btreeLeftChildNodeRightmostItemIndex: number,
  //     btreeRightChildNodeRightmostItemIndex: number,
  //     btreeLeftItemIndex: number,
  //   }>,
  //   btreeLeftParentNodeItem {
  //     index: number,
  //     sortValue: string|number,
  //     btreeLeftChildNodeRightmostItemIndex: number,
  //     btreeRightChildNodeRightmostItemIndex: number,
  //     btreeLeftItemIndex: number,
  //   },
  //   btreeRightParentNodeItem {
  //     index: number,
  //     sortValue: string|number,
  //     btreeLeftChildNodeRightmostItemIndex: number,
  //     btreeRightChildNodeRightmostItemIndex: number,
  //     btreeLeftItemIndex: number,
  //   },
  //   btreeRootNodeRightmostItem {
  //     index: number,
  //     sortValue: string|number,
  //     btreeLeftChildNodeRightmostItemIndex: number,
  //     btreeRightChildNodeRightmostItemIndex: number,
  //     btreeLeftItemIndex: number,
  //   }
  // ) -> Promise<>
  async _splitBTreeChildNodeRight(
    btreeChildNodeItems,
    btreeLeftParentNodeItem,
    btreeRightParentNodeItem,
    btreeRootNodeRightmostItem
  ) {

    // Maximum number of items per b-tree node: `(2 * degree) - 1`
    // Maximum number of children per b-tree node: `2 * degree`

    const btreeLeftChildNodeItems = btreeChildNodeItems.slice(0, this.degree - 1)
    const btreeMiddleItem = btreeChildNodeItems[this.degree - 1]
    const btreeRightChildNodeItems = btreeChildNodeItems.slice(this.degree)

    // middle item left item -> left parent node item
    await this._writeBTreeLeftItemIndex(
      btreeMiddleItem.index,
      btreeLeftParentNodeItem.index
    )
    btreeMiddleItem.btreeLeftItemIndex = btreeLeftParentNodeItem.index

    if (btreeRightParentNodeItem) {
      // right parent node item left item -> middle item
      await this._writeBTreeLeftItemIndex(
        btreeRightParentNodeItem.index,
        btreeMiddleItem.index
      )
      btreeRightParentNodeItem.btreeLeftItemIndex = btreeMiddleItem.index
    }

    if (btreeLeftParentNodeItem.index === btreeRootNodeRightmostItem.index) {
      // root rightmost item -> middle item
      await this._writeBTreeRootRightmostItemIndex(btreeMiddleItem.index)
    }

    // middle item left child node rightmost item -> rightmost item of left child node items
    await this._writeBTreeLeftChildNodeRightmostItemIndex(
      btreeMiddleItem.index,
      btreeLeftChildNodeItems[btreeLeftChildNodeItems.length - 1].index
    )
    btreeMiddleItem.btreeLeftChildNodeRightmostItemIndex =
      btreeLeftChildNodeItems[btreeLeftChildNodeItems.length - 1].index

    // middle item right child node rightmost item -> rightmost item of right child node items
    await this._writeBTreeRightChildNodeRightmostItemIndex(
      btreeMiddleItem.index,
      btreeRightChildNodeItems[btreeRightChildNodeItems.length - 1].index
    )
    btreeMiddleItem.btreeRightChildNodeRightmostItemIndex =
      btreeRightChildNodeItems[btreeRightChildNodeItems.length - 1].index

    // left parent node item right child node rightmost item -> rightmost item of left child node items
    await this._writeBTreeRightChildNodeRightmostItemIndex(
      btreeLeftParentNodeItem.index,
      btreeLeftChildNodeItems[btreeLeftChildNodeItems.length - 1].index
    )
    btreeLeftParentNodeItem.btreeRightChildNodeRightmostItemIndex =
      btreeLeftChildNodeItems[btreeLeftChildNodeItems.length - 1].index

    // right parent node item left child node rightmost item does not need to be updated

    // first item of right child node items left item -> -1
    await this._writeBTreeLeftItemIndex(btreeRightChildNodeItems[0].index, -1)
    btreeRightChildNodeItems[0].btreeLeftItemIndex = -1

    return [btreeLeftChildNodeItems, btreeMiddleItem, btreeRightChildNodeItems]
  }

  // _splitBTreeChildNodeLeft(btreeChildNodeItems Array<{
  //   index: number,
  //   sortValue: string|number,
  //   btreeLeftChildNodeRightmostItemIndex: number,
  //   btreeRightChildNodeRightmostItemIndex: number,
  //   btreeLeftItemIndex: number,
  // }>, btreeRightParentNodeItem {
  //   index: number,
  //   sortValue: string|number,
  //   btreeLeftChildNodeRightmostItemIndex: number,
  //   btreeRightChildNodeRightmostItemIndex: number,
  //   btreeLeftItemIndex: number,
  // }) -> Promise<>
  async _splitBTreeChildNodeLeft(
    btreeChildNodeItems, btreeRightParentNodeItem
  ) {

    const btreeLeftChildNodeItems = btreeChildNodeItems.slice(0, this.degree - 1)
    const btreeMiddleItem = btreeChildNodeItems[this.degree - 1]
    const btreeRightChildNodeItems = btreeChildNodeItems.slice(this.degree)

    // node = [item item item]

    await this._writeBTreeLeftItemIndex(
      btreeRightParentNodeItem.index,
      btreeMiddleItem.index
    )
    btreeRightParentNodeItem.btreeLeftItemIndex = btreeMiddleItem.index

    await this._writeBTreeLeftItemIndex(btreeMiddleItem.index, -1)
    btreeMiddleItem.btreeLeftItemIndex = -1

    await this._writeBTreeLeftChildNodeRightmostItemIndex(
      btreeMiddleItem.index,
      btreeLeftChildNodeItems[btreeLeftChildNodeItems.length - 1].index
    )
    btreeMiddleItem.btreeLeftChildNodeRightmostItemIndex =
      btreeLeftChildNodeItems[btreeLeftChildNodeItems.length - 1].index

    await this._writeBTreeRightChildNodeRightmostItemIndex(
      btreeMiddleItem.index,
      btreeRightChildNodeItems[btreeRightChildNodeItems.length - 1].index
    )
    btreeMiddleItem.btreeRightChildNodeRightmostItemIndex =
      btreeRightChildNodeItems[btreeRightChildNodeItems.length - 1].index

    await this._writeBTreeLeftItemIndex(btreeRightChildNodeItems[0].index, -1)
    btreeRightChildNodeItems[0].btreeLeftItemIndex = -1

    return [btreeLeftChildNodeItems, btreeMiddleItem, btreeRightChildNodeItems]
  }

  // _splitBTreeRootNode(btreeRootNodeItems Array<{
  //   index: number,
  //   sortValue: string|number,
  //   btreeLeftChildNodeRightmostItemIndex: number,
  //   btreeRightChildNodeRightmostItemIndex: number,
  //   btreeLeftItemIndex: number,
  // }>) -> Promise<>
  async _splitBTreeRootNode(btreeRootNodeItems) {
    const btreeLeftChildNodeItems = btreeRootNodeItems.slice(0, this.degree - 1)
    const btreeMiddleItem = btreeRootNodeItems[this.degree - 1]
    const btreeRightChildNodeItems = btreeRootNodeItems.slice(this.degree)

    // update root rightmost item
    await this._writeBTreeRootRightmostItemIndex(btreeMiddleItem.index) // new root

    // new root has no left item
    await this._writeBTreeLeftItemIndex(btreeMiddleItem.index, -1)
    btreeMiddleItem.btreeLeftItemIndex = -1

    // update root left child node rightmost item index
    await this._writeBTreeLeftChildNodeRightmostItemIndex(
      btreeMiddleItem.index,
      btreeLeftChildNodeItems[btreeLeftChildNodeItems.length - 1].index
    )
    btreeMiddleItem.btreeLeftChildNodeRightmostItemIndex =
      btreeLeftChildNodeItems[btreeLeftChildNodeItems.length - 1].index

    // update root right child node rightmost item index
    await this._writeBTreeRightChildNodeRightmostItemIndex(
      btreeMiddleItem.index,
      btreeRightChildNodeItems[btreeRightChildNodeItems.length - 1].index
    )
    btreeMiddleItem.btreeRightChildNodeRightmostItemIndex =
      btreeRightChildNodeItems[btreeRightChildNodeItems.length - 1].index

    // remove left item of first right child node item
    await this._writeBTreeLeftItemIndex(btreeRightChildNodeItems[0].index, -1)
    btreeRightChildNodeItems[0].btreeLeftItemIndex = -1

    return btreeMiddleItem
  }

  // _insertBTreeNodeItem(
  //   index number,
  //   sortValue string|number,
  //   btreeNodeItems Array<{
  //     index: number,
  //     sortValue: string|number,
  //     btreeLeftChildNodeRightmostItemIndex: number,
  //     btreeRightChildNodeRightmostItemIndex: number,
  //     btreeLeftItemIndex: number,
  //   }>,
  //   btreeRootNodeRightmostItem: {
  //     index: number,
  //     sortValue: string|number,
  //     btreeLeftChildNodeRightmostItemIndex: number,
  //     btreeRightChildNodeRightmostItemIndex: number,
  //     btreeLeftItemIndex: number,
  //   },
  //   btreeParentNodeItem {
  //     index: number,
  //     sortValue: string|number,
  //     btreeLeftChildNodeRightmostItemIndex: number,
  //     btreeRightChildNodeRightmostItemIndex: number,
  //     btreeLeftItemIndex: number,
  //   },
  // )
  async _insertBTreeNodeItem(
    index,
    sortValue,
    btreeNodeItems,
    btreeRootNodeRightmostItem,
    btreeParentNodeItem,
  ) {

    let i = btreeNodeItems.length - 1

    while (i >= 0 && btreeNodeItems[i].sortValue > sortValue) {
      i -= 1
    }
    i += 1 // if i === 0, sortValue is less than all items' sortValues in btreeNodeItems, insert item at very left
           // if i === btreeNodeItems.length, sortValue is greater than all items' sortValues in btreeNodeItems, insert item at very right
           // if i > 0, sortValue is less than btreeNodeItems[i].sortValue, insert item in middle

    if (
      btreeNodeItems[0].btreeRightChildNodeRightmostItemIndex == -1
      || btreeNodeItems[0].btreeLeftChildNodeRightmostItemIndex == -1
    ) { // leaf node

      let btreeRightItemIndex
      let btreeLeftItemIndex
      if (i === 0) {
        btreeLeftItemIndex = -1
        btreeRightItemIndex = btreeNodeItems[i].index
      } else if (i === btreeNodeItems.length) {
        btreeLeftItemIndex = btreeNodeItems[i - 1].index
        btreeRightItemIndex = -1
      } else {
        btreeLeftItemIndex = btreeNodeItems[i - 1].index
        btreeRightItemIndex = btreeNodeItems[i].index
      }

      // insert item into leaf node

      if (btreeRightItemIndex > -1) {
        // point right item to item
        await this._writeBTreeLeftItemIndex(btreeRightItemIndex, index)
      }

      if (btreeParentNodeItem == null) {
        // skip
      } else if (btreeParentNodeItem.isLeftChildPointer) { // parent item sortValue was greater than item's sortValue

        // left item was the rightmost item of the parent node item's left child node
        if (
          btreeLeftItemIndex > -1
          && btreeLeftItemIndex === btreeParentNodeItem.btreeLeftChildNodeRightmostItemIndex
        ) {
          await this._writeBTreeLeftChildNodeRightmostItemIndex(
            btreeParentNodeItem.index, index
          )
        }

        // parent item had a left item pointing right to the current rightmost item of the leaf node
        if (
          btreeLeftItemIndex > -1
          && btreeLeftItemIndex === btreeParentNodeItem.leftItem?.btreeRightChildNodeRightmostItemIndex
        ) {
          await this._writeBTreeRightChildNodeRightmostItemIndex(
            btreeParentNodeItem.leftItem.index, index
          )
        }

      } else if (btreeParentNodeItem.isRightChildPointer) {

        // left item was the rightmost item of the parent node item's right child node
        if (
          btreeLeftItemIndex > -1
          && btreeLeftItemIndex === btreeParentNodeItem.btreeRightChildNodeRightmostItemIndex
        ) {
          await this._writeBTreeRightChildNodeRightmostItemIndex(
            btreeParentNodeItem.index, index
          )
        }

        // parent item had a right item pointing left to the current rightmost item of the leaf node
        if (
          btreeLeftItemIndex > -1
          && btreeLeftItemIndex === btreeParentNodeItem.rightItem?.btreeLeftChildNodeRightmostItemIndex
        ) {
          await this._writeBTreeLeftChildNodeRightmostItemIndex(
            btreeParentNodeItem.rightItem.index, index
          )
        }

      }


      let predecessor = null
      let successor = null

      if (i === 0) { // sortValue is less than all items' sortValues in btreeNodeItems
        let parentItem = btreeParentNodeItem
        while (parentItem) {
          if (parentItem.isRightChildPointer) {
            predecessor = parentItem
            break
          }
          parentItem = parentItem.leftItem
            ?? parentItem.btreeParentNodeItem
        }
      } else {
        predecessor = btreeNodeItems[i - 1]
      }

      if (i === btreeNodeItems.length) { // sortValue is greater than all items' sortValues in btreeNodeItems
        let parentItem = btreeParentNodeItem
        while (parentItem) {
          if (parentItem.isLeftChildPointer) {
            successor = parentItem
            break
          }
          parentItem = parentItem.rightItem
            ?? parentItem.btreeParentNodeItem
        }
      } else {
        successor = btreeNodeItems[i]
      }

      return { predecessor, successor, btreeLeftItemIndex }
    }


    if (i === 0) { // move to left child

      let btreeChildNodeItemsParentNodeItem
      let btreeChildNodeItemsLeftParentNodeItem
      let btreeChildNodeItemsRightParentNodeItem = btreeNodeItems[i]

      btreeChildNodeItemsParentNodeItem = btreeChildNodeItemsRightParentNodeItem
      btreeChildNodeItemsParentNodeItem.isLeftChildPointer = true

      const btreeChildNodeRightmostItem = await this._getBTreeItem(
        btreeChildNodeItemsParentNodeItem.btreeLeftChildNodeRightmostItemIndex
      )
      let btreeChildNodeItems = await this._getBTreeNodeItems(btreeChildNodeRightmostItem)

      if (btreeChildNodeItems.length == ((2 * this.degree) - 1)) { // current b-tree node at maximum number of items
        const [btreeLeftNodeItems, btreeNewParentNodeItem, btreeRightNodeItems] =
          await this._splitBTreeChildNodeLeft(btreeChildNodeItems, btreeChildNodeItemsRightParentNodeItem)

        await this._writeBTreeLeftChildNodeRightmostItemIndex(
          btreeChildNodeItemsRightParentNodeItem.index,
          btreeRightNodeItems[btreeRightNodeItems.length - 1].index
        )

        delete btreeChildNodeItemsParentNodeItem.isLeftChildPointer
        delete btreeChildNodeItemsParentNodeItem.btreeParentNodeItem

        btreeChildNodeItemsParentNodeItem = btreeNewParentNodeItem
        btreeChildNodeItemsParentNodeItem.btreeParentNodeItem = btreeParentNodeItem

        if (sortValue > btreeNewParentNodeItem.sortValue) { // move to right child
          btreeChildNodeItems = btreeRightNodeItems
          btreeChildNodeItemsParentNodeItem.isRightChildPointer = true
          btreeChildNodeItemsLeftParentNodeItem = btreeChildNodeItemsParentNodeItem
        } else { // move to left child
          btreeChildNodeItems = btreeLeftNodeItems
          btreeChildNodeItemsParentNodeItem.isLeftChildPointer = true
          btreeChildNodeItemsRightParentNodeItem = btreeChildNodeItemsParentNodeItem
        }

      }


      btreeChildNodeItemsParentNodeItem.btreeParentNodeItem = btreeParentNodeItem

      if (btreeChildNodeItemsRightParentNodeItem?.btreeLeftItemIndex == btreeChildNodeItemsParentNodeItem.index) {
        btreeChildNodeItemsParentNodeItem.rightItem = btreeChildNodeItemsRightParentNodeItem
      }
      if (btreeChildNodeItemsParentNodeItem.rightItem) {
        btreeChildNodeItemsParentNodeItem.rightItem.isLeftChildPointer = true
      }

      return this._insertBTreeNodeItem(
        index,
        sortValue,
        btreeChildNodeItems,
        btreeRootNodeRightmostItem,
        btreeChildNodeItemsParentNodeItem,
      )
    }


    // move to right child

    let btreeChildNodeItemsParentNodeItem
    let btreeChildNodeItemsLeftParentNodeItem
    let btreeChildNodeItemsRightParentNodeItem

    if (i === btreeNodeItems.length) { // sortValue is greater than all items' sortValues in btreeNodeItems
      btreeChildNodeItemsLeftParentNodeItem = btreeNodeItems[i - 1]
      btreeChildNodeItemsRightParentNodeItem = undefined
    }
    else { // (i > 0) sortValue is less than btreeNodeItems[i].sortValue, insert item in middle
      btreeChildNodeItemsLeftParentNodeItem = btreeNodeItems[i - 1]
      btreeChildNodeItemsRightParentNodeItem = btreeNodeItems[i]
      // i === (btreeNodeItems.length - 1) ? undefined : btreeNodeItems[i + 1]
    }
    btreeChildNodeItemsParentNodeItem = btreeChildNodeItemsLeftParentNodeItem
    btreeChildNodeItemsParentNodeItem.isRightChildPointer = true

    const btreeChildNodeRightmostItem = await this._getBTreeItem(
      btreeChildNodeItemsParentNodeItem.btreeRightChildNodeRightmostItemIndex
    )
    let btreeChildNodeItems = await this._getBTreeNodeItems(btreeChildNodeRightmostItem)

    if (btreeChildNodeItems.length == ((2 * this.degree) - 1)) { // current b-tree node at maximum number of items
      const [btreeLeftNodeItems, btreeNewParentNodeItem, btreeRightNodeItems] = await this._splitBTreeChildNodeRight(
        btreeChildNodeItems,
        btreeChildNodeItemsLeftParentNodeItem,
        btreeChildNodeItemsRightParentNodeItem,
        btreeRootNodeRightmostItem
      )

      if (i === btreeNodeItems.length) { // update parent nodes with new child rightmost item
        if (btreeParentNodeItem == null) {
          // skip
        }
        else if (btreeParentNodeItem.isLeftChildPointer) {
          // update parent left child node rightmost item
          await this._writeBTreeLeftChildNodeRightmostItemIndex(
            btreeParentNodeItem.index, btreeNewParentNodeItem.index
          )

          if (btreeParentNodeItem.leftItem) {
            // update parent left item right child node rightmost item
            await this._writeBTreeRightChildNodeRightmostItemIndex(
              btreeParentNodeItem.leftItem.index,
              btreeNewParentNodeItem.index
            )
          }
        }
        else if (btreeParentNodeItem.isRightChildPointer) {
          // update parent right child node rightmost item
          await this._writeBTreeRightChildNodeRightmostItemIndex(
            btreeParentNodeItem.index, btreeNewParentNodeItem.index
          )

          if (btreeParentNodeItem.rightItem) {
            // update parent right item right child node rightmost item
            await this._writeBTreeLeftChildNodeRightmostItemIndex(
              btreeParentNodeItem.rightItem.index,
              btreeNewParentNodeItem.index
            )
          }
        }
      }

      await this._writeBTreeRightChildNodeRightmostItemIndex(
        btreeChildNodeItemsLeftParentNodeItem.index,
        btreeLeftNodeItems[btreeLeftNodeItems.length - 1].index
      )

      if (btreeNewParentNodeItem.btreeLeftItemIndex == btreeParentNodeItem?.btreeRightChildNodeRightmostItemIndex) {
        await this._writeBTreeRightChildNodeRightmostItemIndex(
          btreeParentNodeItem.index, btreeNewParentNodeItem.index
        )
        btreeParentNodeItem.btreeRightChildNodeRightmostItemIndex = btreeNewParentNodeItem.index
      }
      else if (btreeNewParentNodeItem.btreeLeftItemIndex == btreeParentNodeItem?.btreeLeftChildNodeRightmostItemIndex) {
        await this._writeBTreeLeftChildNodeRightmostItemIndex(
          btreeParentNodeItem.index, btreeNewParentNodeItem.index
        )
        btreeParentNodeItem.btreeLeftChildNodeRightmostItemIndex = btreeNewParentNodeItem.index
      }

      delete btreeChildNodeItemsParentNodeItem.isRightChildPointer
      delete btreeChildNodeItemsParentNodeItem.btreeParentNodeItem

      btreeChildNodeItemsParentNodeItem = btreeNewParentNodeItem
      btreeChildNodeItemsParentNodeItem.btreeParentNodeItem = btreeParentNodeItem

      // (old right) (new parent) (old left)                                   (new parent) (old left)
      // (old right) (new left)  (old left)                                    (new left)  (old left)
      // (old right) (new right) (old left)                                    (new right) (old left)

      if (sortValue > btreeNewParentNodeItem.sortValue) { // move to right child
        btreeChildNodeItems = btreeRightNodeItems
        btreeChildNodeItemsParentNodeItem.isRightChildPointer = true
        btreeChildNodeItemsLeftParentNodeItem = btreeChildNodeItemsParentNodeItem
      } else { // move to left child
        btreeChildNodeItems = btreeLeftNodeItems
        btreeChildNodeItemsParentNodeItem.isLeftChildPointer = true
        btreeChildNodeItemsRightParentNodeItem = btreeChildNodeItemsParentNodeItem
      }

    }

    btreeChildNodeItemsParentNodeItem.btreeParentNodeItem = btreeParentNodeItem

    if (btreeChildNodeItemsRightParentNodeItem?.btreeLeftItemIndex == btreeChildNodeItemsParentNodeItem.index) {
      btreeChildNodeItemsParentNodeItem.rightItem = btreeChildNodeItemsRightParentNodeItem
    }
    if (btreeChildNodeItemsParentNodeItem.rightItem) {
      btreeChildNodeItemsParentNodeItem.rightItem.isLeftChildPointer = true
    }
    if (btreeChildNodeItemsParentNodeItem.btreeLeftItemIndex == btreeChildNodeItemsLeftParentNodeItem?.index) {
      btreeChildNodeItemsParentNodeItem.leftItem = btreeChildNodeItemsLeftParentNodeItem
    }
    if (btreeChildNodeItemsParentNodeItem.leftItem) {
      btreeChildNodeItemsParentNodeItem.leftItem.isRightChildPointer = true
    }

    return this._insertBTreeNodeItem(
      index,
      sortValue,
      btreeChildNodeItems,
      btreeRootNodeRightmostItem,
      btreeChildNodeItemsParentNodeItem
    )
  }

  // _balanceBTreeGrandparentNodes(
  //   grandparentNodeItem {
  //     index: number,
  //     sortValue: string|number,
  //     btreeLeftChildNodeRightmostItemIndex: number,
  //     btreeRightChildNodeRightmostItemIndex: number,
  //     btreeLeftItemIndex: number,
  //   },
  //   grandparentNodeItems Array<{
  //     index: number,
  //     sortValue: string|number,
  //     btreeLeftChildNodeRightmostItemIndex: number,
  //     btreeRightChildNodeRightmostItemIndex: number,
  //     btreeLeftItemIndex: number,
  //   }>,
  //   rotatedChildNodeItem {
  //     index: number,
  //     sortValue: string|number,
  //     btreeLeftChildNodeRightmostItemIndex: number,
  //     btreeRightChildNodeRightmostItemIndex: number,
  //     btreeLeftItemIndex: number,
  //   },
  //   grandparentNodeItemIsChildRightmostItem boolean,
  //   rotatedParentNodeItem {
  //     index: number,
  //     sortValue: string|number,
  //     btreeLeftChildNodeRightmostItemIndex: number,
  //     btreeRightChildNodeRightmostItemIndex: number,
  //     btreeLeftItemIndex: number,
  //   },
  //   parentNodeItems Array<{
  //     index: number,
  //     sortValue: string|number,
  //     btreeLeftChildNodeRightmostItemIndex: number,
  //     btreeRightChildNodeRightmostItemIndex: number,
  //     btreeLeftItemIndex: number,
  //   }>
  // ) -> Promise<>
  async _balanceBTreeGrandparentNodes(
    grandparentNodeItem,
    grandparentNodeItems,
    rotatedChildNodeItem,
    grandparentNodeItemIsChildRightmostItem,
    rotatedParentNodeItem,
    parentNodeItems
  ) {

    const greatGrandparentNodeItem = grandparentNodeItem.btreeParentNodeItem
    const leftGreatGrandparentNodeItem = getLeftParentItem(greatGrandparentNodeItem)
    const rightGreatGrandparentNodeItem = getRightParentItem(greatGrandparentNodeItem)

    if (grandparentNodeItems.length === (this.degree - 1) && rotatedChildNodeItem == null) { // grandparent node under minimum after grandparent rotation

      let grandparentLeftSiblingNodeItems
      if (leftGreatGrandparentNodeItem) {
        const grandparentLeftSiblingRightmostItem =
          await this._getBTreeItem(leftGreatGrandparentNodeItem.btreeLeftChildNodeRightmostItemIndex)
        grandparentLeftSiblingNodeItems =
          await this._getBTreeNodeItems(grandparentLeftSiblingRightmostItem)
      }

      let grandparentRightSiblingNodeItems
      if (rightGreatGrandparentNodeItem) {
        const grandparentRightSiblingRightmostItem =
          await this._getBTreeItem(rightGreatGrandparentNodeItem.btreeRightChildNodeRightmostItemIndex)
        grandparentRightSiblingNodeItems =
          await this._getBTreeNodeItems(grandparentRightSiblingRightmostItem)
      }

      if (grandparentLeftSiblingNodeItems?.length > (this.degree - 1)) { // grandparent left sibling node over minimum

        // left great grandparent becomes grandparent
        // grandparent left sibling node rightmost item becomes left great grandparent

        if (leftGreatGrandparentNodeItem.rightItem) {
          // point left great grandparent right item left item
          await this._writeBTreeLeftItemIndex(
            leftGreatGrandparentNodeItem.rightItem.index,
            grandparentLeftSiblingNodeItems[grandparentLeftSiblingNodeItems.length - 1].index
          )

          if (grandparentNodeItem.rightItem) {
            // left great grandparent right item left child rightmost item stays the same
          } else if (grandparentNodeItem.leftItem) {
            // point left great grandparent right item left child
            await this._writeBTreeLeftChildNodeRightmostItemIndex(
              leftGreatGrandparentNodeItem.rightItem.index,
              grandparentNodeItem.leftItem.index
            )
          } else {
            // point left great grandparent right item left child
            await this._writeBTreeLeftChildNodeRightmostItemIndex(
              leftGreatGrandparentNodeItem.rightItem.index,
              leftGreatGrandparentNodeItem.index
            )
          }

        } else {

          if (leftGreatGrandparentNodeItem.btreeParentNodeItem == null) { // root
            // point root
            await this._writeBTreeRootRightmostItemIndex(
              grandparentLeftSiblingNodeItems[grandparentLeftSiblingNodeItems.length - 1].index
            )
          }
        }

        if (leftGreatGrandparentNodeItem.leftItem) {
          // point left great grandparent left item right child
          await this._writeBTreeRightChildNodeRightmostItemIndex(
            leftGreatGrandparentNodeItem.leftItem.index,
            grandparentLeftSiblingNodeItems[grandparentLeftSiblingNodeItems.length - 2].index,
          )
        }

        // point left great grandparent left item
        await this._writeBTreeLeftItemIndex(leftGreatGrandparentNodeItem.index, -1)

        if (grandparentNodeItemIsChildRightmostItem) {
          // point left great grandparent right child
          await this._writeBTreeRightChildNodeRightmostItemIndex(
            leftGreatGrandparentNodeItem.index, grandparentNodeItem.index
          )
        } else if (grandparentNodeItem.leftItem) {
          // point left great grandparent right child
          await this._writeBTreeRightChildNodeRightmostItemIndex(
            leftGreatGrandparentNodeItem.index,
            grandparentNodeItems[0].btreeLeftChildNodeRightmostItemIndex
          )
        } else if (grandparentNodeItem.btreeRightChildNodeRightmostItemIndex === rotatedParentNodeItem.index) {
          // point left great grandparent right child
          await this._writeBTreeRightChildNodeRightmostItemIndex(
            leftGreatGrandparentNodeItem.index,
            parentNodeItems[parentNodeItems.length - 2].index
          )
        } else {
          // point left great grandparent right child
          await this._writeBTreeRightChildNodeRightmostItemIndex(
            leftGreatGrandparentNodeItem.index,
            grandparentNodeItem.btreeRightChildNodeRightmostItemIndex
          )
        }

        // point left great grandparent left child
        await this._writeBTreeLeftChildNodeRightmostItemIndex(
          leftGreatGrandparentNodeItem.index,
          grandparentLeftSiblingNodeItems[grandparentLeftSiblingNodeItems.length - 1].btreeRightChildNodeRightmostItemIndex
        )

        // point grandparent left sibling node rightmost item left item
        await this._writeBTreeLeftItemIndex(
          grandparentLeftSiblingNodeItems[grandparentLeftSiblingNodeItems.length - 1].index,
          leftGreatGrandparentNodeItem.btreeLeftItemIndex
        )

        if (grandparentNodeItem.rightItem) {
          // point grandparent left sibling node rightmost item right child
          await this._writeBTreeRightChildNodeRightmostItemIndex(
            grandparentLeftSiblingNodeItems[grandparentLeftSiblingNodeItems.length - 1].index,
            grandparentNodeItems[grandparentNodeItems.length - 1].index
          )

          const leftmostGrandparentNodeItem =
            getLeftmostNodeItemExcluding(grandparentNodeItems, grandparentNodeItem)

          // merge left great grandparent into grandparent node
          await this._writeBTreeLeftItemIndex(
            leftmostGrandparentNodeItem.index, leftGreatGrandparentNodeItem.index
          )

        } else if (grandparentNodeItem.leftItem) {
          // point grandparent left sibling node rightmost item right child
          await this._writeBTreeRightChildNodeRightmostItemIndex(
            grandparentLeftSiblingNodeItems[grandparentLeftSiblingNodeItems.length - 1].index,
            grandparentNodeItem.leftItem.index
          )

          const leftmostGrandparentNodeItem =
            getLeftmostNodeItemExcluding(grandparentNodeItems, grandparentNodeItem)

          // merge left great grandparent into grandparent node
          await this._writeBTreeLeftItemIndex(
            leftmostGrandparentNodeItem.index, leftGreatGrandparentNodeItem.index
          )

        } else {
          // point grandparent left sibling node rightmost item right child
          await this._writeBTreeRightChildNodeRightmostItemIndex(
            grandparentLeftSiblingNodeItems[grandparentLeftSiblingNodeItems.length - 1].index,
            leftGreatGrandparentNodeItem.index
          )
        }

        // point grandparent left sibling node rightmost item left child
        await this._writeBTreeLeftChildNodeRightmostItemIndex(
          grandparentLeftSiblingNodeItems[grandparentLeftSiblingNodeItems.length - 1].index,
          grandparentLeftSiblingNodeItems[grandparentLeftSiblingNodeItems.length - 2].index,
        )

        if (leftGreatGrandparentNodeItem.btreeParentNodeItem && leftGreatGrandparentNodeItem.rightItem == null) {

          const leftGreatGreatGrandparentNodeItem =
            getLeftParentItem(leftGreatGrandparentNodeItem.btreeParentNodeItem)
          if (leftGreatGreatGrandparentNodeItem) {
            // point left great great grandparent right child
            await this._writeBTreeRightChildNodeRightmostItemIndex(
              leftGreatGreatGrandparentNodeItem.index,
              grandparentLeftSiblingNodeItems[grandparentLeftSiblingNodeItems.length - 1].index
            )
          }

          const rightGreatGreatGrandparentNodeItem =
            getRightParentItem(leftGreatGrandparentNodeItem.btreeParentNodeItem)
          if (rightGreatGreatGrandparentNodeItem) {
            // point right great great grandparent left child
            await this._writeBTreeLeftChildNodeRightmostItemIndex(
              rightGreatGreatGrandparentNodeItem.index,
              grandparentLeftSiblingNodeItems[grandparentLeftSiblingNodeItems.length - 1].index
            )
          }

        }

      } else if (grandparentRightSiblingNodeItems?.length > (this.degree - 1)) { // grandparent right sibling node over minimum

        // right great grandparent becomes grandparent
        // grandparent right sibling leftmost item becomes right great grandparent

        if (rightGreatGrandparentNodeItem.rightItem) {
          // point right great grandparent right item left item
          await this._writeBTreeLeftItemIndex(
            rightGreatGrandparentNodeItem.rightItem.index,
            grandparentRightSiblingNodeItems[0].index
          )
        }

        if (rightGreatGrandparentNodeItem.leftItem) {
          // point right great grandparent left item right child
          await this._writeBTreeRightChildNodeRightmostItemIndex(
            rightGreatGrandparentNodeItem.leftItem.index,
            rightGreatGrandparentNodeItem.index
          )
        }

        if (grandparentNodeItems.length === 1) {
          // point right great grandparent left item
          await this._writeBTreeLeftItemIndex(rightGreatGrandparentNodeItem.index, -1)
        } else {
          const rightmostGrandparentNodeItem =
            getRightmostNodeItemExcluding(grandparentNodeItems, grandparentNodeItem)

          // point right great grandparent left item
          await this._writeBTreeLeftItemIndex(
            rightGreatGrandparentNodeItem.index, rightmostGrandparentNodeItem.index
          )
        }

        if (grandparentNodeItemIsChildRightmostItem) {
          // point right great grandparent left child
          await this._writeBTreeLeftChildNodeRightmostItemIndex(
            rightGreatGrandparentNodeItem.index, grandparentNodeItem.index
          )
        } else if (grandparentNodeItems[grandparentNodeItems.length - 1].btreeRightChildNodeRightmostItemIndex === rotatedParentNodeItem.index) {
          // point right great grandparent left child
          await this._writeBTreeLeftChildNodeRightmostItemIndex(
            rightGreatGrandparentNodeItem.index,
            parentNodeItems[parentNodeItems.length - 2].index
          )
        } else {
          // point right great grandparent left child
          await this._writeBTreeLeftChildNodeRightmostItemIndex(
            rightGreatGrandparentNodeItem.index,
            grandparentNodeItems[grandparentNodeItems.length - 1].btreeRightChildNodeRightmostItemIndex
          )
        }

        // point right great grandparent right child
        await this._writeBTreeRightChildNodeRightmostItemIndex(
          rightGreatGrandparentNodeItem.index,
          grandparentRightSiblingNodeItems[0].btreeLeftChildNodeRightmostItemIndex
        )

        // point grandparent right sibling leftmost item left item
        await this._writeBTreeLeftItemIndex(
          grandparentRightSiblingNodeItems[0].index,
          rightGreatGrandparentNodeItem.btreeLeftItemIndex
        )

        // point grandparent right sibling leftmost item left child
        await this._writeBTreeLeftChildNodeRightmostItemIndex(
          grandparentRightSiblingNodeItems[0].index,
          rightGreatGrandparentNodeItem.index
        )

        // point grandparent right sibling leftmost item right child
        await this._writeBTreeRightChildNodeRightmostItemIndex(
          grandparentRightSiblingNodeItems[0].index,
          rightGreatGrandparentNodeItem.btreeRightChildNodeRightmostItemIndex
        )

        // point grandparent right sibling leftmost item right item left item
        await this._writeBTreeLeftItemIndex(
          grandparentRightSiblingNodeItems[1].index, -1
        )

        if (rightGreatGrandparentNodeItem.btreeParentNodeItem == null && rightGreatGrandparentNodeItem.rightItem == null) {
          await this._writeBTreeRootRightmostItemIndex(grandparentRightSiblingNodeItems[0].index)
        }

        if (rightGreatGrandparentNodeItem.btreeParentNodeItem && rightGreatGrandparentNodeItem.rightItem == null) {

          const leftGreatGreatGrandparentNodeItem =
            getLeftParentItem(rightGreatGrandparentNodeItem.btreeParentNodeItem)
          if (leftGreatGreatGrandparentNodeItem) {
            // point left great great grandparent right child
            await this._writeBTreeRightChildNodeRightmostItemIndex(
              leftGreatGreatGrandparentNodeItem.index,
              grandparentRightSiblingNodeItems[0].index
            )
          }

          const rightGreatGreatGrandparentNodeItem =
            getRightParentItem(rightGreatGrandparentNodeItem.btreeParentNodeItem)
          if (rightGreatGreatGrandparentNodeItem) {
            // point right great great grandparent left child
            await this._writeBTreeLeftChildNodeRightmostItemIndex(
              rightGreatGreatGrandparentNodeItem.index,
              grandparentRightSiblingNodeItems[0].index
            )
          }

        }

      } else if (grandparentLeftSiblingNodeItems?.length === (this.degree - 1)) { // grandparent left sibling node at minimum

        // left great grandparent becomes grandparent

        if (leftGreatGrandparentNodeItem.rightItem) {
          // point left great grandparent right item left item
          await this._writeBTreeLeftItemIndex(
            leftGreatGrandparentNodeItem.rightItem.index,
            leftGreatGrandparentNodeItem.btreeLeftItemIndex
          )

          if (grandparentNodeItems.length > 1) {
            if (grandparentNodeItem.index === grandparentNodeItems[grandparentNodeItems.length - 1].index) {
              // point left great grandparent right item left child
              await this._writeBTreeLeftChildNodeRightmostItemIndex(
                leftGreatGrandparentNodeItem.rightItem.index,
                grandparentNodeItem.leftItem.index
              )
            }
          } else {
            // point left great grandparent right item left child
            await this._writeBTreeLeftChildNodeRightmostItemIndex(
              leftGreatGrandparentNodeItem.rightItem.index,
              leftGreatGrandparentNodeItem.index
            )
          }
        }

        if (leftGreatGrandparentNodeItem.leftItem) {
          if (grandparentNodeItems.length > 1) {
            if (grandparentNodeItem.index === grandparentNodeItems[grandparentNodeItems.length - 1].index) {
              // point left great grandparent left item right child
              await this._writeBTreeRightChildNodeRightmostItemIndex(
                leftGreatGrandparentNodeItem.leftItem.index,
                grandparentNodeItem.leftItem.index
              )
            } else {
              // point left great grandparent left item right child
              await this._writeBTreeRightChildNodeRightmostItemIndex(
                leftGreatGrandparentNodeItem.leftItem.index,
                grandparentNodeItems[grandparentNodeItems.length - 1].index
              )
            }
          } else {
            // point left great grandparent left item right child
            await this._writeBTreeRightChildNodeRightmostItemIndex(
              leftGreatGrandparentNodeItem.leftItem.index,
              leftGreatGrandparentNodeItem.index
            )
          }
        }

        const leftmostGrandparentNodeItem =
          getLeftmostNodeItemExcluding(grandparentNodeItems, grandparentNodeItem)

        if (grandparentNodeItems.length > 1) {
          // merge grandparent node into grandparent left sibling node
          await this._writeBTreeLeftItemIndex(
            leftmostGrandparentNodeItem.index, leftGreatGrandparentNodeItem.index
          )
        }

        // merge left great grandparent into left child node
        // point left great grandparent left item
        await this._writeBTreeLeftItemIndex(
          leftGreatGrandparentNodeItem.index,
          leftGreatGrandparentNodeItem.btreeLeftChildNodeRightmostItemIndex
        )

        // point left great grandparent left child
        await this._writeBTreeLeftChildNodeRightmostItemIndex(
          leftGreatGrandparentNodeItem.index,
          grandparentLeftSiblingNodeItems[grandparentLeftSiblingNodeItems.length - 1].btreeRightChildNodeRightmostItemIndex
        )

        if (grandparentNodeItemIsChildRightmostItem) {
          // point left great grandparent right child
          await this._writeBTreeRightChildNodeRightmostItemIndex(
            leftGreatGrandparentNodeItem.index, grandparentNodeItem.index
          )
        } else if (grandparentNodeItems.length === 1) {
          // point left great grandparent right child
          await this._writeBTreeRightChildNodeRightmostItemIndex(
            leftGreatGrandparentNodeItem.index,
            grandparentNodeItem.btreeRightChildNodeRightmostItemIndex
          )
        } else if (leftmostGrandparentNodeItem.btreeLeftChildNodeRightmostItemIndex === rotatedParentNodeItem.index) {
          // point left great grandparent right child
          await this._writeBTreeRightChildNodeRightmostItemIndex(
            leftGreatGrandparentNodeItem.index,
            parentNodeItems[parentNodeItems.length - 2].index
          )
        } else {
          // point left great grandparent right child
          await this._writeBTreeRightChildNodeRightmostItemIndex(
            leftGreatGrandparentNodeItem.index,
            leftmostGrandparentNodeItem.btreeLeftChildNodeRightmostItemIndex
          )
        }

        if (leftGreatGrandparentNodeItem.btreeParentNodeItem) {
          // root rightmost item stays the same
        } else if (leftGreatGrandparentNodeItem.rightItem) {
          // root rightmost item stays the same
        } else if (leftGreatGrandparentNodeItem.leftItem) {
          // point root
          await this._writeBTreeRootRightmostItemIndex(leftGreatGrandparentNodeItem.leftItem.index)
        } else if (grandparentNodeItems.length > 1) {
          const rightmostGrandparentNodeItem =
            getRightmostNodeItemExcluding(grandparentNodeItems, grandparentNodeItem)

          // point root
          await this._writeBTreeRootRightmostItemIndex(rightmostGrandparentNodeItem.index)
        } else {
          // point root
          await this._writeBTreeRootRightmostItemIndex(leftGreatGrandparentNodeItem.index)
        }

        // grandparentNodeItemIsChildRightmostItem
        // rotatedChildNodeItem ? false
        // parent right sibling at minimum (no left sibling) ? false
        // parent left sibling at minimum ? btreeParentNodeItems.length === 1

        if (leftGreatGrandparentNodeItem.btreeParentNodeItem) {
          await this._balanceBTreeGrandparentNodes(
            leftGreatGrandparentNodeItem,
            grandparentNodeItem.btreeParentNodeItems,
            null,
            grandparentNodeItems.length === 1,
            grandparentNodeItem,
            grandparentNodeItems
          )
        }

      } else if (grandparentRightSiblingNodeItems?.length === (this.degree - 1)) { // grandparent right sibling node at minimum

        // right great grandparent becomes grandparent

        if (greatGrandparentNodeItem.rightItem) {
          // point right great grandparent right item left item
          await this._writeBTreeLeftItemIndex(
            greatGrandparentNodeItem.rightItem.index,
            greatGrandparentNodeItem.btreeLeftItemIndex
          )

          // right great grandparent right item left child stays the same
        }

        const rightmostGrandparentNodeItem =
          getRightmostNodeItemExcluding(grandparentNodeItems, grandparentNodeItem)

        if (grandparentNodeItemIsChildRightmostItem) {
          // point right great grandparent left child
          await this._writeBTreeLeftChildNodeRightmostItemIndex(
            greatGrandparentNodeItem.index, grandparentNodeItem.index
          )
        } else if (grandparentNodeItem.rightItem) {
          // point right great grandparent left child
          await this._writeBTreeLeftChildNodeRightmostItemIndex(
            greatGrandparentNodeItem.index,
            rightmostGrandparentNodeItem.btreeRightChildNodeRightmostItemIndex
          )
        } else if (grandparentNodeItem.btreeRightChildNodeRightmostItemIndex === rotatedParentNodeItem.index) {
          // point right great grandparent left child
          await this._writeBTreeLeftChildNodeRightmostItemIndex(
            greatGrandparentNodeItem.index,
            parentNodeItems[parentNodeItems.length - 2].index
          )
        } else {
          // point right great grandparent left child
          await this._writeBTreeLeftChildNodeRightmostItemIndex(
            greatGrandparentNodeItem.index, grandparentNodeItem.btreeRightChildNodeRightmostItemIndex
          )
        }

        if (grandparentNodeItem.rightItem) {
          // point right great grandparent left item
          await this._writeBTreeLeftItemIndex(
            greatGrandparentNodeItem.index, rightmostGrandparentNodeItem.index
          )
        } else {
          // point right great grandparent left item
          await this._writeBTreeLeftItemIndex(
            greatGrandparentNodeItem.index,
            grandparentNodeItem.btreeLeftItemIndex
          )
        }

        // merge right great grandparent into right child node
        await this._writeBTreeLeftItemIndex(
          grandparentRightSiblingNodeItems[0].index,
          greatGrandparentNodeItem.index
        )

        // point right great grandparent right child
        await this._writeBTreeRightChildNodeRightmostItemIndex(
          greatGrandparentNodeItem.index,
          grandparentRightSiblingNodeItems[0].btreeLeftChildNodeRightmostItemIndex
        )

        if (greatGrandparentNodeItem.btreeParentNodeItem) {
          // root rightmost item stays the same
        } else if (greatGrandparentNodeItem.rightItem) {
          // root rightmost item stays the same
        } else {
          // point root
          await this._writeBTreeRootRightmostItemIndex(
            greatGrandparentNodeItem.btreeRightChildNodeRightmostItemIndex
          )
        }

        // grandparentNodeItemIsChildRightmostItem
        // rotatedChildNodeItem ? false
        // parent right sibling at minimum (no left sibling) ? false
        // parent left sibling at minimum ? btreeParentNodeItems.length === 1

        if (greatGrandparentNodeItem.btreeParentNodeItem) {
          await this._balanceBTreeGrandparentNodes(
            greatGrandparentNodeItem,
            grandparentNodeItem.btreeParentNodeItems,
            null,
            false,
            grandparentNodeItem,
            grandparentNodeItems
          )
        }

      }

    } else { // grandparent node over minimum after grandparent rotation

      if (rotatedChildNodeItem && grandparentNodeItem.rightItem == null) {

        if (leftGreatGrandparentNodeItem) {
          // point left great grandparent right child
          await this._writeBTreeRightChildNodeRightmostItemIndex(
            leftGreatGrandparentNodeItem.index,
            rotatedChildNodeItem.index
          )
        }

        if (rightGreatGrandparentNodeItem) {
          // point right great grandparent left child
          await this._writeBTreeLeftChildNodeRightmostItemIndex(
            rightGreatGrandparentNodeItem.index,
            rotatedChildNodeItem.index
          )
        }

      } else if (grandparentNodeItem.leftItem && grandparentNodeItem.rightItem == null) {

        if (leftGreatGrandparentNodeItem) {
          // point left great grandparent right child
          await this._writeBTreeRightChildNodeRightmostItemIndex(
            leftGreatGrandparentNodeItem.index,
            grandparentNodeItem.leftItem.index
          )
        }

        if (rightGreatGrandparentNodeItem) {
          // point right great grandparent left child
          await this._writeBTreeLeftChildNodeRightmostItemIndex(
            rightGreatGrandparentNodeItem.index,
            grandparentNodeItem.leftItem.index
          )
        }

      }

    }

  }

  // _balanceBTreeAfterDelete(
  //   btreeItem {
  //     index: number,
  //     sortValue: string|number,
  //     btreeLeftChildNodeRightmostItemIndex: number,
  //     btreeRightChildNodeRightmostItemIndex: number,
  //     btreeLeftItemIndex: number,
  //   },
  //   btreeNodeItems Array<{
  //     index: number,
  //     sortValue: string|number,
  //     btreeLeftChildNodeRightmostItemIndex: number,
  //     btreeRightChildNodeRightmostItemIndex: number,
  //     btreeLeftItemIndex: number,
  //   },
  //   btreeRootNodeItems Array<{
  //     index: number,
  //     sortValue: string|number,
  //     btreeLeftChildNodeRightmostItemIndex: number,
  //     btreeRightChildNodeRightmostItemIndex: number,
  //     btreeLeftItemIndex: number,
  //   }>,
  //   i number,
  //   btreeParentNodeItem {
  //     index: number,
  //     sortValue: string|number,
  //     btreeLeftChildNodeRightmostItemIndex: number,
  //     btreeRightChildNodeRightmostItemIndex: number,
  //     btreeLeftItemIndex: number,
  //   },
  //   btreeParentNodeItems Array<{
  //     index: number,
  //     sortValue: string|number,
  //     btreeLeftChildNodeRightmostItemIndex: number,
  //     btreeRightChildNodeRightmostItemIndex: number,
  //     btreeLeftItemIndex: number,
  //   }>,
  //   btreeParentLeftSiblingNodeItems Array<{
  //     index: number,
  //     sortValue: string|number,
  //     btreeLeftChildNodeRightmostItemIndex: number,
  //     btreeRightChildNodeRightmostItemIndex: number,
  //     btreeLeftItemIndex: number,
  //   }>,
  //   btreeParentRightSiblingNodeItems Array<{
  //     index: number,
  //     sortValue: string|number,
  //     btreeLeftChildNodeRightmostItemIndex: number,
  //     btreeRightChildNodeRightmostItemIndex: number,
  //     btreeLeftItemIndex: number,
  //   }>
  // ) -> Promise<>
  async _balanceBTreeAfterDelete(
    btreeItem,
    btreeNodeItems,
    btreeRootNodeItems,
    i,
    btreeParentNodeItem,
    btreeParentNodeItems,
    btreeParentLeftSiblingNodeItems,
    btreeParentRightSiblingNodeItems,
  ) {

    const leftParentNodeItem = getLeftParentItem(btreeParentNodeItem)
    const leftSiblingNodeItems = leftParentNodeItem == null
      ? []
      : await this._getBTreeNodeItems(await this._getBTreeItem(leftParentNodeItem.btreeLeftChildNodeRightmostItemIndex))

    const rightParentNodeItem = getRightParentItem(btreeParentNodeItem)
    const rightSiblingNodeItems = rightParentNodeItem == null
      ? []
      : await this._getBTreeNodeItems(await this._getBTreeItem(rightParentNodeItem.btreeRightChildNodeRightmostItemIndex))

    if (leftSiblingNodeItems.length > (this.degree - 1)) { // left sibling node has more than the minimum number of items

      // left parent becomes new leaf node item
      await this._writeBTreeLeftChildNodeRightmostItemIndex(
        leftParentNodeItem.index, -1
      )
      await this._writeBTreeRightChildNodeRightmostItemIndex(
        leftParentNodeItem.index, -1
      )

      if (btreeNodeItems.length === 1) { // no items left in node after deletion
        // skip
      } else {
        const leftmostBTreeNodeItem = getLeftmostNodeItemExcluding(btreeNodeItems, btreeItem)

        // merge node with left parent
        await this._writeBTreeLeftItemIndex(
          leftmostBTreeNodeItem.index, leftParentNodeItem.index
        )
      }

      // point left parent left item
      await this._writeBTreeLeftItemIndex(leftParentNodeItem.index, -1)

      // left sibling node rightmost item becomes new internal node item

      const rightmostBTreeNodeItem = getRightmostNodeItemExcluding(btreeNodeItems, btreeItem)

      if (btreeNodeItems.length === 1) { // no items left in node after deletion
        // point left sibling node rightmost item right child
        await this._writeBTreeRightChildNodeRightmostItemIndex(
          leftSiblingNodeItems[leftSiblingNodeItems.length - 1].index,
          leftParentNodeItem.index
        )
      } else {
        // point left sibling node rightmost item right child
        await this._writeBTreeRightChildNodeRightmostItemIndex(
          leftSiblingNodeItems[leftSiblingNodeItems.length - 1].index,
          rightmostBTreeNodeItem.index
        )
      }

      // point left sibling node rightmost item left child
      await this._writeBTreeLeftChildNodeRightmostItemIndex(
        leftSiblingNodeItems[leftSiblingNodeItems.length - 1].index,
        leftSiblingNodeItems[leftSiblingNodeItems.length - 2].index
      )

      // point left sibling node rightmost item left item
      await this._writeBTreeLeftItemIndex(
        leftSiblingNodeItems[leftSiblingNodeItems.length - 1].index,
        leftParentNodeItem.btreeLeftItemIndex
      )

      if (leftParentNodeItem.index === btreeRootNodeItems[btreeRootNodeItems.length - 1].index) { // left parent was root node
        // point root
        await this._writeBTreeRootRightmostItemIndex(
          leftSiblingNodeItems[leftSiblingNodeItems.length - 1].index
        )
      }

      if (leftParentNodeItem.leftItem) {
        // point left parent left item right child
        await this._writeBTreeRightChildNodeRightmostItemIndex(
          leftParentNodeItem.leftItem.index,
          leftSiblingNodeItems[leftSiblingNodeItems.length - 2].index
        )
      }

      if (rightParentNodeItem) {
        if (btreeNodeItems.length === 1) { // no items left in node after deletion
          // point right parent left child
          await this._writeBTreeLeftChildNodeRightmostItemIndex(
            rightParentNodeItem.index, leftParentNodeItem.index
          )
        } else {
          // point right parent left child
          await this._writeBTreeLeftChildNodeRightmostItemIndex(
            rightParentNodeItem.index, rightmostBTreeNodeItem.index
          )
        }

        // point right parent left item
        await this._writeBTreeLeftItemIndex(
          rightParentNodeItem.index,
          leftSiblingNodeItems[leftSiblingNodeItems.length - 1].index
        )
      }

      // grandparent

      if (leftParentNodeItem.btreeParentNodeItem && leftParentNodeItem.rightItem == null) {

        if (leftParentNodeItem.btreeParentNodeItem.isLeftChildPointer) {
          // point left parent right grandparent left child
          await this._writeBTreeLeftChildNodeRightmostItemIndex(
            leftParentNodeItem.btreeParentNodeItem.index,
            leftSiblingNodeItems[leftSiblingNodeItems.length - 1].index
          )

          if (leftParentNodeItem.btreeParentNodeItem.leftItem) {
            // point left parent left grandparent right child
            await this._writeBTreeRightChildNodeRightmostItemIndex(
              leftParentNodeItem.btreeParentNodeItem.leftItem.index,
              leftSiblingNodeItems[leftSiblingNodeItems.length - 1].index
            )
          }

        } else if (leftParentNodeItem.btreeParentNodeItem.isRightChildPointer) {
          // point left parent left grandparent right child
          await this._writeBTreeRightChildNodeRightmostItemIndex(
            leftParentNodeItem.btreeParentNodeItem.index,
            leftSiblingNodeItems[leftSiblingNodeItems.length - 1].index
          )

          if (leftParentNodeItem.btreeParentNodeItem.rightItem) {
            // point left parent right grandparent left child
            await this._writeBTreeLeftChildNodeRightmostItemIndex(
              leftParentNodeItem.btreeParentNodeItem.rightItem.index,
              leftSiblingNodeItems[leftSiblingNodeItems.length - 1].index
            )
          }

        } else {
          throw new Error('grandparent node item isLeftChildPointer or isRightChildPointer unset')
        }

      }

    } else if (rightSiblingNodeItems.length > (this.degree - 1)) { // right sibling node has more than the minimum number of items

      // right parent becomes new leaf node item
      await this._writeBTreeLeftChildNodeRightmostItemIndex(
        rightParentNodeItem.index, -1
      )
      await this._writeBTreeRightChildNodeRightmostItemIndex(
        rightParentNodeItem.index, -1
      )

      if (btreeNodeItems.length === 1) { // no items left in node after deletion
        // point right parent node item left item
        await this._writeBTreeLeftItemIndex(rightParentNodeItem.index, -1)
      } else {
        const rightmostBTreeNodeItem = getRightmostNodeItemExcluding(btreeNodeItems, btreeItem)

        // point right parent node item left item
        await this._writeBTreeLeftItemIndex(
          rightParentNodeItem.index, rightmostBTreeNodeItem.index
        )
      }

      // right sibling node leftmost item becomes parent node
      // point right sibling node left child
      await this._writeBTreeLeftChildNodeRightmostItemIndex(
        rightSiblingNodeItems[0].index, rightParentNodeItem.index
      )
      // point right sibling node right child
      await this._writeBTreeRightChildNodeRightmostItemIndex(
        rightSiblingNodeItems[0].index,
        rightSiblingNodeItems[rightSiblingNodeItems.length - 1].index
      )

      // new internal node points to previous right parent node's left item
      await this._writeBTreeLeftItemIndex(
        rightSiblingNodeItems[0].index, rightParentNodeItem.btreeLeftItemIndex
      )
      // right sibling node leftmost item's right item becomes leftmost item
      await this._writeBTreeLeftItemIndex(rightSiblingNodeItems[1].index, -1)

      if (rightParentNodeItem.rightItem) {
        // point right item of right parent left item
        await this._writeBTreeLeftItemIndex(
          rightParentNodeItem.rightItem.index, rightSiblingNodeItems[0].index
        )
      }

      if (rightParentNodeItem.index === btreeRootNodeItems[btreeRootNodeItems.length - 1].index) { // right parent was root node
        // point root
        await this._writeBTreeRootRightmostItemIndex(rightSiblingNodeItems[0].index)
      }

      if (leftParentNodeItem) {
        // point left parent right child
        await this._writeBTreeRightChildNodeRightmostItemIndex(
          leftParentNodeItem.index, rightParentNodeItem.index
        )
      }

      // point grandparents to new internal node
      if (btreeParentNodeItem.btreeParentNodeItem == null) {
        // skip
      } else if (btreeParentNodeItem.btreeParentNodeItem.isLeftChildPointer) { // leftmost grandparent
        if (btreeParentNodeItem.btreeParentNodeItem.btreeLeftChildNodeRightmostItemIndex === rightParentNodeItem.index) { // right parent node item was rightmost item
          if (rightParentNodeItem.rightItem == null) {
            // point right grandparent left child
            await this._writeBTreeLeftChildNodeRightmostItemIndex(
              btreeParentNodeItem.btreeParentNodeItem.index,
              rightSiblingNodeItems[0].index
            )
          }
        }
      } else if (btreeParentNodeItem.btreeParentNodeItem.isRightChildPointer) {
        if (btreeParentNodeItem.btreeParentNodeItem.btreeRightChildNodeRightmostItemIndex === rightParentNodeItem.index) { // right parent node item was rightmost item
          if (rightParentNodeItem.rightItem == null) {
            // point left grandparent right child
            await this._writeBTreeRightChildNodeRightmostItemIndex(
              btreeParentNodeItem.btreeParentNodeItem.index,
              rightSiblingNodeItems[0].index
            )

            if (btreeParentNodeItem.btreeParentNodeItem.rightItem) {
              // point right grandparent left child
              await this._writeBTreeLeftChildNodeRightmostItemIndex(
                btreeParentNodeItem.btreeParentNodeItem.rightItem.index,
                rightSiblingNodeItems[0].index
              )
            }
          }
        }
      } else {
        throw new Error('grandparent node item isLeftChildPointer or isRightChildPointer unset')
      }

    } else if (leftSiblingNodeItems.length === (this.degree - 1) && rightSiblingNodeItems.length === (this.degree - 1)) { // left and right sibling nodes at minimum

      // left parent becomes new leaf node item
      await this._writeBTreeLeftChildNodeRightmostItemIndex(
        leftParentNodeItem.index, -1
      )
      await this._writeBTreeRightChildNodeRightmostItemIndex(
        leftParentNodeItem.index, -1
      )

      // left parent left child node rightmost item becomes left item
      await this._writeBTreeLeftItemIndex(
        leftParentNodeItem.index,
        leftParentNodeItem.btreeLeftChildNodeRightmostItemIndex
      )

      // point right parent left item
      await this._writeBTreeLeftItemIndex(
        rightParentNodeItem.index, leftParentNodeItem.btreeLeftItemIndex
      )

      if (btreeNodeItems.length === 1) { // no items left in node after deletion
        // point right parent to left child
        await this._writeBTreeLeftChildNodeRightmostItemIndex(
          rightParentNodeItem.index, leftParentNodeItem.index
        )
      } else if (i === (btreeNodeItems.length - 1)) { // rightmost item was deleted
        // point right parent to left child
        await this._writeBTreeLeftChildNodeRightmostItemIndex(
          rightParentNodeItem.index, btreeItem.btreeLeftItemIndex
        )
      } else { // non-rightmost item was deleted
        // right parent left child node rightmost item stays the same
      }

      // update node items with left parent
      if (btreeNodeItems.length === 1) { // no items left in node after deletion
        // skip
      } else {
        const leftmostBTreeNodeItem = getLeftmostNodeItemExcluding(btreeNodeItems, btreeItem)

        // merge left parent with node
        await this._writeBTreeLeftItemIndex(
          leftmostBTreeNodeItem.index, leftParentNodeItem.index
        )
      }

      if (leftParentNodeItem.leftItem) {
        if (btreeNodeItems.length === 1) { // no items left in node after deletion
          // point left parent left item right child
          await this._writeBTreeRightChildNodeRightmostItemIndex(
            leftParentNodeItem.leftItem.index, leftParentNodeItem.index
          )
        } else {
          const rightmostBTreeNodeItem = getRightmostNodeItemExcluding(btreeNodeItems, btreeItem)

          // point left parent left item right child
          await this._writeBTreeRightChildNodeRightmostItemIndex(
            leftParentNodeItem.leftItem.index,
            rightmostBTreeNodeItem.index
          )
        }
      }

      if (btreeParentNodeItems.length <= (this.degree - 1)) { // parent node at minimum / root node at or under minimum

        if (btreeParentNodeItem.btreeParentNodeItem == null) { // no grandparent

          // root stays the same

        } else if (btreeParentLeftSiblingNodeItems?.length > (this.degree - 1)) { // parent left sibling node over minimum

          const leftGrandparentNodeItem = getLeftParentItem(btreeParentNodeItem.btreeParentNodeItem)

          // left grandparent item becomes parent item

          // point left grandparent left item
          await this._writeBTreeLeftItemIndex(leftGrandparentNodeItem.index, -1)

          if (leftParentNodeItem.leftItem) {
            // point left item of left parent left item
            await this._writeBTreeLeftItemIndex(
              rightParentNodeItem.index,
              leftParentNodeItem.btreeLeftItemIndex
            )

            // point leftmost parent
            await this._writeBTreeLeftItemIndex(
              btreeParentNodeItems[0].index, leftGrandparentNodeItem.index
            )
          } else {
            // point right parent left item
            await this._writeBTreeLeftItemIndex(
              rightParentNodeItem.index, leftGrandparentNodeItem.index
            )
          }

          if (leftParentNodeItem.leftItem) {
            // point left grandparent right child
            await this._writeBTreeRightChildNodeRightmostItemIndex(
              leftGrandparentNodeItem.index,
              btreeParentNodeItems[0].btreeLeftChildNodeRightmostItemIndex
            )
          } else {
            const rightmostBTreeNodeItem = getRightmostNodeItemExcluding(btreeNodeItems, btreeItem)

            // point left grandparent right child
            await this._writeBTreeRightChildNodeRightmostItemIndex(
              leftGrandparentNodeItem.index, rightmostBTreeNodeItem.index
            )
          }

          // left grandparent takes rightmost left grandchild node as left child

          // point left grandparent left child
          await this._writeBTreeLeftChildNodeRightmostItemIndex(
            leftGrandparentNodeItem.index,
            btreeParentLeftSiblingNodeItems[btreeParentLeftSiblingNodeItems.length - 1].btreeRightChildNodeRightmostItemIndex
          )

          // parent left sibling node rightmost item becomes left grandparent

          if (leftGrandparentNodeItem.index === btreeRootNodeItems[btreeRootNodeItems.length - 1].index) {
            // point root
            await this._writeBTreeRootRightmostItemIndex(
              btreeParentLeftSiblingNodeItems[btreeParentLeftSiblingNodeItems.length - 1].index
            )
          }

          if (leftGrandparentNodeItem.rightItem) {
            // point left grandparent right item left item
            await this._writeBTreeLeftItemIndex(
              leftGrandparentNodeItem.rightItem.index,
              btreeParentLeftSiblingNodeItems[btreeParentLeftSiblingNodeItems.length - 1].index
            )
          }

          if (leftGrandparentNodeItem.leftItem) {
            // point left grandparent left item right child
            await this._writeBTreeRightChildNodeRightmostItemIndex(
              leftGrandparentNodeItem.leftItem.index,
              btreeParentLeftSiblingNodeItems[btreeParentLeftSiblingNodeItems.length - 2].index
            )
          }

          // point parent left sibling node rightmost item right child left item
          await this._writeBTreeLeftItemIndex(
            btreeParentLeftSiblingNodeItems[btreeParentLeftSiblingNodeItems.length - 1].index,
            leftGrandparentNodeItem.btreeLeftItemIndex
          )

          // point parent left sibling node rightmost item right child
          await this._writeBTreeRightChildNodeRightmostItemIndex(
            btreeParentLeftSiblingNodeItems[btreeParentLeftSiblingNodeItems.length - 1].index,
            btreeParentNodeItems[btreeParentNodeItems.length - 1].index
          )

          // point parent left sibling node rightmost item left child
          await this._writeBTreeLeftChildNodeRightmostItemIndex(
            btreeParentLeftSiblingNodeItems[btreeParentLeftSiblingNodeItems.length - 1].index,
            btreeParentLeftSiblingNodeItems[btreeParentLeftSiblingNodeItems.length - 2].index
          )

          // great grandparent

          if (leftGrandparentNodeItem.btreeParentNodeItem) {
            await this._balanceBTreeGrandparentNodes(
              leftGrandparentNodeItem,
              btreeParentNodeItem.btreeParentNodeItems,
              btreeParentLeftSiblingNodeItems[btreeParentLeftSiblingNodeItems.length - 1]
            )
          }

        } else if (btreeParentRightSiblingNodeItems?.length > (this.degree - 1)) { // parent right sibling node over minimum

          const rightGrandparentNodeItem = getRightParentItem(btreeParentNodeItem.btreeParentNodeItem)

          // right grandparent item becomes parent item

          // point right grandparent left item
          await this._writeBTreeLeftItemIndex(
            rightGrandparentNodeItem.index,
            btreeParentNodeItems[btreeParentNodeItems.length - 1].index
          )

          // point right grandparent left child
          await this._writeBTreeLeftChildNodeRightmostItemIndex(
            rightGrandparentNodeItem.index,
            btreeParentNodeItems[btreeParentNodeItems.length - 1].btreeRightChildNodeRightmostItemIndex
          )

          // right grandparent takes leftmost right grandchild node as right child
          // point right grandparent right child
          await this._writeBTreeRightChildNodeRightmostItemIndex(
            rightGrandparentNodeItem.index,
            btreeParentRightSiblingNodeItems[0].btreeLeftChildNodeRightmostItemIndex
          )

          // parent right sibling node leftmost item becomes right grandparent

          if (rightGrandparentNodeItem.index === btreeRootNodeItems[btreeRootNodeItems.length - 1].index) {
            // point root
            await this._writeBTreeRootRightmostItemIndex(
              btreeParentRightSiblingNodeItems[0].index
            )
          }

          if (rightGrandparentNodeItem.rightItem) {
            // point right grandparent right item left item
            await this._writeBTreeLeftItemIndex(
              rightGrandparentNodeItem.rightItem.index,
              btreeParentRightSiblingNodeItems[0].index
            )
          }

          if (rightGrandparentNodeItem.leftItem) {
            // point right grandparent left item right child
            await this._writeBTreeRightChildNodeRightmostItemIndex(
              rightGrandparentNodeItem.leftItem.index,
              rightGrandparentNodeItem.index
            )
          }

          // point parent right sibling node leftmost item left item
          await this._writeBTreeLeftItemIndex(
            btreeParentRightSiblingNodeItems[0].index,
            rightGrandparentNodeItem.btreeLeftItemIndex
          )

          // point parent right sibling node leftmost item left child
          await this._writeBTreeLeftChildNodeRightmostItemIndex(
            btreeParentRightSiblingNodeItems[0].index,
            rightGrandparentNodeItem.index
          )

          // point parent right sibling node leftmost item right child
          await this._writeBTreeRightChildNodeRightmostItemIndex(
            btreeParentRightSiblingNodeItems[0].index,
            btreeParentRightSiblingNodeItems[btreeParentRightSiblingNodeItems.length - 1].index
          )

          // parent right sibling node leftmost item right item becomes leftmost item
          await this._writeBTreeLeftItemIndex(
            btreeParentRightSiblingNodeItems[1].index, -1
          )

          // great grandparent

          if (rightGrandparentNodeItem.btreeParentNodeItem) {
            await this._balanceBTreeGrandparentNodes(
              rightGrandparentNodeItem,
              btreeParentNodeItem.btreeParentNodeItems,
              btreeParentRightSiblingNodeItems[0]
            )
          }

        } else if (btreeParentLeftSiblingNodeItems?.length === (this.degree - 1)) { // parent left sibling at minimum

          const leftGrandparentNodeItem = getLeftParentItem(btreeParentNodeItem.btreeParentNodeItem)

          // left grandparent item becomes parent item

          // point left grandparent left item
          await this._writeBTreeLeftItemIndex(
            leftGrandparentNodeItem.index,
            leftGrandparentNodeItem.btreeLeftChildNodeRightmostItemIndex
          )

          if (leftGrandparentNodeItem.rightItem) {
            // point left grandparent right item left item
            await this._writeBTreeLeftItemIndex(
              leftGrandparentNodeItem.rightItem.index,
              leftGrandparentNodeItem.btreeLeftItemIndex
            )

            // point left grandparent right item left child
            await this._writeBTreeLeftChildNodeRightmostItemIndex(
              leftGrandparentNodeItem.rightItem.index,
              btreeParentNodeItems[btreeParentNodeItems.length - 1].index
            )
          }

          if (leftGrandparentNodeItem.leftItem) {
            // point left grandparent left item right child
            await this._writeBTreeRightChildNodeRightmostItemIndex(
              leftGrandparentNodeItem.leftItem.index,
              btreeParentNodeItems[btreeParentNodeItems.length - 1].index
            )
          }

          if (leftParentNodeItem.leftItem) {
            // merge parent node with parent left sibling node
            await this._writeBTreeLeftItemIndex(
              btreeParentNodeItems[0].index, leftGrandparentNodeItem.index
            )
            // point left grandparent right child
            await this._writeBTreeRightChildNodeRightmostItemIndex(
              leftGrandparentNodeItem.index,
              btreeParentNodeItems[0].btreeLeftChildNodeRightmostItemIndex
            )
          } else {

            // merge parent node with parent left sibling node
            await this._writeBTreeLeftItemIndex(
              rightParentNodeItem.index, leftGrandparentNodeItem.index
            )

            const rightmostBTreeNodeItem = getRightmostNodeItemExcluding(btreeNodeItems, btreeItem)

            // point left grandparent right child
            await this._writeBTreeRightChildNodeRightmostItemIndex(
              leftGrandparentNodeItem.index, rightmostBTreeNodeItem.index
            )
          }

          // point left grandparent left child
          await this._writeBTreeLeftChildNodeRightmostItemIndex(
            leftGrandparentNodeItem.index,
            btreeParentLeftSiblingNodeItems[btreeParentLeftSiblingNodeItems.length - 1].btreeRightChildNodeRightmostItemIndex
          )

          if (leftGrandparentNodeItem.btreeParentNodeItem) {
            // root rightmost item stays the same
          } else if (leftGrandparentNodeItem.rightItem) {
            // root rightmost item stays the same
          } else if (leftGrandparentNodeItem.leftItem) {
            // point root rightmost item
            await this._writeBTreeRootRightmostItemIndex(leftGrandparentNodeItem.leftItem.index)
          } else {
            // point root rightmost item
            await this._writeBTreeRootRightmostItemIndex(
              btreeParentNodeItems[btreeParentNodeItems.length - 1].index
            )
          }

          // great grandparent

          if (leftGrandparentNodeItem.btreeParentNodeItem) {
            await this._balanceBTreeGrandparentNodes(
              leftGrandparentNodeItem,
              btreeParentNodeItem.btreeParentNodeItems,
              null,
              btreeParentNodeItems.length === 1,
              leftParentNodeItem,
              btreeParentNodeItems
            )
          }

        } else if (btreeParentRightSiblingNodeItems?.length === (this.degree - 1)) { // parent right sibling at minimum (no left sibling)

          const rightGrandparentNodeItem = getRightParentItem(btreeParentNodeItem.btreeParentNodeItem)

          // right grandparent item becomes parent item

          if (rightGrandparentNodeItem.rightItem) {
            // point right grandparent right item left item
            await this._writeBTreeLeftItemIndex(
              rightGrandparentNodeItem.rightItem.index,
              rightGrandparentNodeItem.btreeLeftItemIndex
            )
          }

          // point right grandparent left item
          await this._writeBTreeLeftItemIndex(
            rightGrandparentNodeItem.index,
            btreeParentNodeItems[btreeParentNodeItems.length - 1].index
          )

          // point right grandparent left child
          await this._writeBTreeLeftChildNodeRightmostItemIndex(
            rightGrandparentNodeItem.index,
            btreeParentNodeItems[btreeParentNodeItems.length - 1].btreeRightChildNodeRightmostItemIndex
          )

          // point right grandparent right child
          await this._writeBTreeRightChildNodeRightmostItemIndex(
            rightGrandparentNodeItem.index,
            btreeParentRightSiblingNodeItems[0].btreeLeftChildNodeRightmostItemIndex
          )

          // merge parent node with parent right sibling node
          await this._writeBTreeLeftItemIndex(
            btreeParentRightSiblingNodeItems[0].index,
            rightGrandparentNodeItem.index
          )

          if (rightGrandparentNodeItem.btreeParentNodeItem) {
            // root rightmost item stays the same
          } else if (rightGrandparentNodeItem.rightItem) {
            // root rightmost item stays the same
          } else {
            // point root
            await this._writeBTreeRootRightmostItemIndex(
              btreeParentRightSiblingNodeItems[btreeParentRightSiblingNodeItems.length - 1].index
            )
          }

          // great grandparent

          if (rightGrandparentNodeItem.btreeParentNodeItem) {
            await this._balanceBTreeGrandparentNodes(
              rightGrandparentNodeItem,
              btreeParentNodeItem.btreeParentNodeItems,
              null,
              false,
              leftParentNodeItem,
              btreeParentNodeItems
            )
          }

        }

      }

    } else if (leftSiblingNodeItems.length === 0 && rightSiblingNodeItems.length === (this.degree - 1)) { // no left sibling node, right sibling node at minimum

      // right parent becomes leaf
      await this._writeBTreeLeftChildNodeRightmostItemIndex(rightParentNodeItem.index, -1)
      await this._writeBTreeRightChildNodeRightmostItemIndex(rightParentNodeItem.index, -1)

      if (rightParentNodeItem.rightItem) {
        // point right item of right parent left item
        await this._writeBTreeLeftItemIndex(
          rightParentNodeItem.rightItem.index,
          rightParentNodeItem.btreeLeftItemIndex
        )
      }

      if (btreeNodeItems.length === 1) { // no items left in node after deletion
        // skip
      } else {
        const rightmostBTreeNodeItem = getRightmostNodeItemExcluding(btreeNodeItems, btreeItem)

        // point right parent left item
        await this._writeBTreeLeftItemIndex(
          rightParentNodeItem.index, rightmostBTreeNodeItem.index
        )
      }

      // merge node with right sibling node
      await this._writeBTreeLeftItemIndex(
        rightSiblingNodeItems[0].index, rightParentNodeItem.index
      )

      if (btreeParentNodeItems.length <= (this.degree - 1)) { // parent node at minimum / root node at or under minimum

        if (btreeParentNodeItem.btreeParentNodeItem == null) { // no grandparent

          if (btreeParentNodeItems.length === 1) { // no parents after right parent becomes leaf
            await this._writeBTreeRootRightmostItemIndex(
              rightSiblingNodeItems[rightSiblingNodeItems.length - 1].index
            )
          }

        } else if (btreeParentLeftSiblingNodeItems?.length > (this.degree - 1)) { // parent left sibling node over minimum

          const leftGrandparentNodeItem = getLeftParentItem(btreeParentNodeItem.btreeParentNodeItem)

          // left grandparent item becomes parent item

          // point left grandparent left item
          await this._writeBTreeLeftItemIndex(leftGrandparentNodeItem.index, -1)

          if (rightParentNodeItem.rightItem) {
            // point right item of right parent left item
            await this._writeBTreeLeftItemIndex(
              rightParentNodeItem.rightItem.index, leftGrandparentNodeItem.index
            )

            // point left grandparent right child
            await this._writeBTreeRightChildNodeRightmostItemIndex(
              leftGrandparentNodeItem.index,
              rightParentNodeItem.rightItem.btreeLeftChildNodeRightmostItemIndex
            )
          } else {
            // point left grandparent right child
            await this._writeBTreeRightChildNodeRightmostItemIndex(
              leftGrandparentNodeItem.index,
              rightSiblingNodeItems[rightSiblingNodeItems.length - 1].index
            )
          }

          // left grandparent takes rightmost left grandchild node as left child

          // point left grandparent left child
          await this._writeBTreeLeftChildNodeRightmostItemIndex(
            leftGrandparentNodeItem.index,
            btreeParentLeftSiblingNodeItems[btreeParentLeftSiblingNodeItems.length - 1].btreeRightChildNodeRightmostItemIndex
          )

          // parent left sibling node rightmost item becomes left grandparent

          if (leftGrandparentNodeItem.index === btreeRootNodeItems[btreeRootNodeItems.length - 1].index) {
            // point root
            await this._writeBTreeRootRightmostItemIndex(
              btreeParentLeftSiblingNodeItems[btreeParentLeftSiblingNodeItems.length - 1].index
            )
          }

          if (leftGrandparentNodeItem.rightItem) {
            // point left grandparent right item left item
            await this._writeBTreeLeftItemIndex(
              leftGrandparentNodeItem.rightItem.index,
              btreeParentLeftSiblingNodeItems[btreeParentLeftSiblingNodeItems.length - 1].index
            )

            if (btreeParentNodeItems.length === 1) {
              // point left grandparent right item left child
              await this._writeBTreeLeftChildNodeRightmostItemIndex(
                leftGrandparentNodeItem.rightItem.index,
                leftGrandparentNodeItem.index
              )
            } else {
              // left grandparent right item left child stays the same
            }
          }

          if (leftGrandparentNodeItem.leftItem) {
            // point left grandparent left item right child
            await this._writeBTreeRightChildNodeRightmostItemIndex(
              leftGrandparentNodeItem.leftItem.index,
              btreeParentLeftSiblingNodeItems[btreeParentLeftSiblingNodeItems.length - 2].index
            )
          }

          // point parent left sibling node rightmost item right child left item
          await this._writeBTreeLeftItemIndex(
            btreeParentLeftSiblingNodeItems[btreeParentLeftSiblingNodeItems.length - 1].index,
            leftGrandparentNodeItem.btreeLeftItemIndex
          )

          if (rightParentNodeItem.rightItem) {
            // point parent left sibling node rightmost item right child
            await this._writeBTreeRightChildNodeRightmostItemIndex(
              btreeParentLeftSiblingNodeItems[btreeParentLeftSiblingNodeItems.length - 1].index,
              btreeParentNodeItems[btreeParentNodeItems.length - 1].index
            )
          } else {
            // point parent left sibling node rightmost item right child
            await this._writeBTreeRightChildNodeRightmostItemIndex(
              btreeParentLeftSiblingNodeItems[btreeParentLeftSiblingNodeItems.length - 1].index,
              leftGrandparentNodeItem.index
            )
          }

          // point parent left sibling node rightmost item left child
          await this._writeBTreeLeftChildNodeRightmostItemIndex(
            btreeParentLeftSiblingNodeItems[btreeParentLeftSiblingNodeItems.length - 1].index,
            btreeParentLeftSiblingNodeItems[btreeParentLeftSiblingNodeItems.length - 2].index
          )

          // great grandparent

          if (leftGrandparentNodeItem.btreeParentNodeItem) {
            await this._balanceBTreeGrandparentNodes(
              leftGrandparentNodeItem,
              btreeParentNodeItem.btreeParentNodeItems,
              btreeParentLeftSiblingNodeItems[btreeParentLeftSiblingNodeItems.length - 1]
            )
          }

        } else if (btreeParentRightSiblingNodeItems?.length > (this.degree - 1)) { // parent right sibling node over minimum

          const rightGrandparentNodeItem = getRightParentItem(btreeParentNodeItem.btreeParentNodeItem)

          // right grandparent item becomes parent item

          if (btreeParentNodeItems.length === 1) { // no parents after right parent becomes leaf
            // point right grandparent left item
            await this._writeBTreeLeftItemIndex(rightGrandparentNodeItem.index, -1)

            // point right grandparent left child
            await this._writeBTreeLeftChildNodeRightmostItemIndex(
              rightGrandparentNodeItem.index,
              rightSiblingNodeItems[rightSiblingNodeItems.length - 1].index
            )

          } else {
            // point right grandparent left item
            const rightmostParentNodeItem = btreeParentNodeItems[btreeParentNodeItems.length - 1]
            await this._writeBTreeLeftItemIndex(
              rightGrandparentNodeItem.index, rightmostParentNodeItem.index
            )

            // point right grandparent left child
            await this._writeBTreeLeftChildNodeRightmostItemIndex(
              rightGrandparentNodeItem.index,
              rightmostParentNodeItem.btreeRightChildNodeRightmostItemIndex
            )
          }

          // right grandparent takes leftmost right grandchild node as right child
          // point right grandparent right child
          await this._writeBTreeRightChildNodeRightmostItemIndex(
            rightGrandparentNodeItem.index,
            btreeParentRightSiblingNodeItems[0].btreeLeftChildNodeRightmostItemIndex
          )

          // parent right sibling node leftmost item becomes right grandparent

          if (rightGrandparentNodeItem.index === btreeRootNodeItems[btreeRootNodeItems.length - 1].index) {
            // point root
            await this._writeBTreeRootRightmostItemIndex(btreeParentRightSiblingNodeItems[0].index)
          }

          if (rightGrandparentNodeItem.rightItem) {
            // point right grandparent right item left item
            await this._writeBTreeLeftItemIndex(
              rightGrandparentNodeItem.rightItem.index,
              btreeParentRightSiblingNodeItems[0].index
            )
          }

          if (rightGrandparentNodeItem.leftItem) {
            // point right grandparent left item right child
            await this._writeBTreeRightChildNodeRightmostItemIndex(
              rightGrandparentNodeItem.leftItem.index, rightGrandparentNodeItem.index
            )
          }

          // point parent right sibling node leftmost item left item
          await this._writeBTreeLeftItemIndex(
            btreeParentRightSiblingNodeItems[0].index,
            rightGrandparentNodeItem.btreeLeftItemIndex
          )

          // point parent right sibling node leftmost item left child
          await this._writeBTreeLeftChildNodeRightmostItemIndex(
            btreeParentRightSiblingNodeItems[0].index, rightGrandparentNodeItem.index
          )

          // point parent right sibling node leftmost item right child
          await this._writeBTreeRightChildNodeRightmostItemIndex(
            btreeParentRightSiblingNodeItems[0].index,
            btreeParentRightSiblingNodeItems[btreeParentRightSiblingNodeItems.length - 1].index
          )

          // parent right sibling node leftmost item right item becomes leftmost item
          await this._writeBTreeLeftItemIndex(
            btreeParentRightSiblingNodeItems[1].index, -1
          )

          // great grandparent

          if (rightGrandparentNodeItem.btreeParentNodeItem) {
            await this._balanceBTreeGrandparentNodes(
              rightGrandparentNodeItem,
              btreeParentNodeItem.btreeParentNodeItems,
              btreeParentRightSiblingNodeItems[0]
            )
          }

        } else if (btreeParentLeftSiblingNodeItems?.length === (this.degree - 1)) { // parent left sibling at minimum

          const leftGrandparentNodeItem = getLeftParentItem(btreeParentNodeItem.btreeParentNodeItem)

          // left grandparent item becomes parent item

          // point left grandparent left item
          await this._writeBTreeLeftItemIndex(
            leftGrandparentNodeItem.index,
            leftGrandparentNodeItem.btreeLeftChildNodeRightmostItemIndex
          )

          if (leftGrandparentNodeItem.rightItem) {
            // point left grandparent right item left item
            await this._writeBTreeLeftItemIndex(
              leftGrandparentNodeItem.rightItem.index,
              leftGrandparentNodeItem.btreeLeftItemIndex
            )

            if (btreeParentNodeItem.rightItem) {
              // parent node rightmost item stays the same
            } else {
              // point left grandparent right item left child
              await this._writeBTreeLeftChildNodeRightmostItemIndex(
                leftGrandparentNodeItem.rightItem.index, leftGrandparentNodeItem.index
              )
            }
          }

          if (leftGrandparentNodeItem.leftItem) {
            if (btreeParentNodeItems.length > 1) {
              // point left grandparent left item right child
              await this._writeBTreeRightChildNodeRightmostItemIndex(
                leftGrandparentNodeItem.leftItem.index,
                btreeParentNodeItems[btreeParentNodeItems.length - 1].index
              )
            } else {
              // point left grandparent left item right child
              await this._writeBTreeRightChildNodeRightmostItemIndex(
                leftGrandparentNodeItem.leftItem.index, leftGrandparentNodeItem.index
              )
            }
          }

          if (rightParentNodeItem.rightItem) {
            // merge parent node into left sibling parent node
            await this._writeBTreeLeftItemIndex(
              rightParentNodeItem.rightItem.index, leftGrandparentNodeItem.index
            )
          }

          // point left grandparent right child
          await this._writeBTreeRightChildNodeRightmostItemIndex(
            leftGrandparentNodeItem.index,
            rightSiblingNodeItems[rightSiblingNodeItems.length - 1].index
          )

          // point left grandparent left child
          await this._writeBTreeLeftChildNodeRightmostItemIndex(
            leftGrandparentNodeItem.index,
            btreeParentLeftSiblingNodeItems[btreeParentLeftSiblingNodeItems.length - 1].btreeRightChildNodeRightmostItemIndex
          )

          if (leftGrandparentNodeItem.btreeParentNodeItem) {
            // root rightmost item stays the same
          } else if (leftGrandparentNodeItem.rightItem) {
            // root rightmost item stays the same
          } else if (leftGrandparentNodeItem.leftItem) {
            // point root rightmost item
            await this._writeBTreeRootRightmostItemIndex(leftGrandparentNodeItem.leftItem.index)
          } else if (btreeParentNodeItem.rightItem) {
            // point root rightmost item
            await this._writeBTreeRootRightmostItemIndex(btreeParentNodeItems[btreeParentNodeItems.length - 1].index)
          } else {
            // point root rightmost item
            await this._writeBTreeRootRightmostItemIndex(leftGrandparentNodeItem.index)
          }

          // great grandparent

          if (leftGrandparentNodeItem.btreeParentNodeItem) {
            await this._balanceBTreeGrandparentNodes(
              leftGrandparentNodeItem,
              btreeParentNodeItem.btreeParentNodeItems,
              null,
              btreeParentNodeItems.length === 1,
              rightParentNodeItem,
              btreeParentNodeItems
            )
          }

        } else if (btreeParentRightSiblingNodeItems?.length === (this.degree - 1)) { // parent right sibling at minimum (no left sibling)

          const rightGrandparentNodeItem = getRightParentItem(btreeParentNodeItem.btreeParentNodeItem)

          // right grandparent item becomes parent item

          if (rightGrandparentNodeItem.rightItem) {
            // point right grandparent right item
            await this._writeBTreeLeftItemIndex(
              rightGrandparentNodeItem.rightItem.index,
              rightGrandparentNodeItem.btreeLeftItemIndex
            )
          }

          if (btreeParentNodeItem.rightItem) {
            // point right grandparent left item
            await this._writeBTreeLeftItemIndex(
              rightGrandparentNodeItem.index,
              btreeParentNodeItems[btreeParentNodeItems.length - 1].index
            )
          }

          // point right grandparent left child
          await this._writeBTreeLeftChildNodeRightmostItemIndex(
            rightGrandparentNodeItem.index,
            btreeParentNodeItems[btreeParentNodeItems.length - 1].btreeRightChildNodeRightmostItemIndex
          )

          // point right grandparent right child
          await this._writeBTreeRightChildNodeRightmostItemIndex(
            rightGrandparentNodeItem.index,
            btreeParentRightSiblingNodeItems[0].btreeLeftChildNodeRightmostItemIndex
          )

          // merge parent node with parent right sibling node
          await this._writeBTreeLeftItemIndex(
            btreeParentRightSiblingNodeItems[0].index,
            rightGrandparentNodeItem.index
          )

          if (rightGrandparentNodeItem.btreeParentNodeItem) {
            // root rightmost item stays the same
          } else if (rightGrandparentNodeItem.rightItem) {
            // root rightmost item stays the same
          } else {
            // point root
            await this._writeBTreeRootRightmostItemIndex(
              btreeParentRightSiblingNodeItems[btreeParentRightSiblingNodeItems.length - 1].index
            )
          }

          // great grandparent

          if (rightGrandparentNodeItem.btreeParentNodeItem) {
            await this._balanceBTreeGrandparentNodes(
              rightGrandparentNodeItem,
              btreeParentNodeItem.btreeParentNodeItems,
              null,
              false,
              rightParentNodeItem,
              btreeParentNodeItems
            )
          }

        }

      }

    } else if (rightSiblingNodeItems.length === 0 && leftSiblingNodeItems.length === (this.degree - 1)) { // no right sibling node, left sibling node at minimum

      // left parent becomes leaf
      await this._writeBTreeLeftChildNodeRightmostItemIndex(leftParentNodeItem.index, -1)
      await this._writeBTreeRightChildNodeRightmostItemIndex(leftParentNodeItem.index, -1)

      if (btreeNodeItems.length === 1) { // no items left in node after deletion
        // skip
      } else {
        const leftmostBTreeNodeItem = getLeftmostNodeItemExcluding(btreeNodeItems, btreeItem)

        // merge left parent into node
        await this._writeBTreeLeftItemIndex(
          leftmostBTreeNodeItem.index, leftParentNodeItem.index
        )
      }

      // merge node with left sibling node
      await this._writeBTreeLeftItemIndex(
        leftParentNodeItem.index,
        leftSiblingNodeItems[leftSiblingNodeItems.length - 1].index
      )

      const rightmostBTreeNodeItem = getRightmostNodeItemExcluding(btreeNodeItems, btreeItem)

      if (leftParentNodeItem.leftItem) {

        if (btreeNodeItems.length === 1) { // no items left in node after deletion
          // point left parent left item right child
          await this._writeBTreeRightChildNodeRightmostItemIndex(
            leftParentNodeItem.leftItem.index, leftParentNodeItem.index
          )
        } else {
          // point left parent left item right child
          await this._writeBTreeRightChildNodeRightmostItemIndex(
            leftParentNodeItem.leftItem.index, rightmostBTreeNodeItem.index
          )
        }

        if (leftParentNodeItem.btreeParentNodeItem == null) { // no grandparent
          // point root
          await this._writeBTreeRootRightmostItemIndex(leftParentNodeItem.leftItem.index)
        } else {
          if (leftParentNodeItem.btreeParentNodeItem.isRightChildPointer) {
            // point left parent left grandparent right child
            await this._writeBTreeRightChildNodeRightmostItemIndex(
              leftParentNodeItem.btreeParentNodeItem.index,
              leftParentNodeItem.leftItem.index
            )

            if (leftParentNodeItem.btreeParentNodeItem.rightItem) {
              // point left parent right grandparent left child
              await this._writeBTreeLeftChildNodeRightmostItemIndex(
                leftParentNodeItem.btreeParentNodeItem.rightItem.index,
                leftParentNodeItem.leftItem.index
              )
            }

          } else if (leftParentNodeItem.btreeParentNodeItem.isLeftChildPointer) {
            // point left parent right grandparent left child
            await this._writeBTreeLeftChildNodeRightmostItemIndex(
              leftParentNodeItem.btreeParentNodeItem.index,
              leftParentNodeItem.leftItem.index
            )

            if (leftParentNodeItem.btreeParentNodeItem.leftItem) {
              // point left parent left grandparent right child
              await this._writeBTreeRightChildNodeRightmostItemIndex(
                leftParentNodeItem.btreeParentNodeItem.leftItem.index,
                leftParentNodeItem.leftItem.index
              )
            }

          } else {
            throw new Error('grandparent parent node item isLeftChildPointer or isRightChildPointer unset')
          }

        }

      }

      if (btreeParentNodeItems.length <= (this.degree - 1)) { // parent node at minimum / root node at or under minimum

        if (btreeParentNodeItem.btreeParentNodeItem == null) { // no grandparent

          if (leftParentNodeItem.leftItem) {
            // point root
            await this._writeBTreeRootRightmostItemIndex(leftParentNodeItem.leftItem.index)
          } else if (btreeNodeItems.length > 1) {
            // point root
            await this._writeBTreeRootRightmostItemIndex(rightmostBTreeNodeItem.index)
          } else { // no parents left after deletion
            // point root
            await this._writeBTreeRootRightmostItemIndex(leftParentNodeItem.index)
          }

        } else if (btreeParentLeftSiblingNodeItems?.length > (this.degree - 1)) { // parent left sibling node over minimum

          const leftGrandparentNodeItem = getLeftParentItem(btreeParentNodeItem.btreeParentNodeItem)

          // left grandparent item becomes parent item

          // point left grandparent left item
          await this._writeBTreeLeftItemIndex(leftGrandparentNodeItem.index, -1)

          if (leftParentNodeItem.leftItem) {
            // point first item of parent node left item
            await this._writeBTreeLeftItemIndex(
              btreeParentNodeItems[0].index, leftGrandparentNodeItem.index
            )
          }

          if (btreeNodeItems.length === 1) {
            // point left grandparent right child
            await this._writeBTreeRightChildNodeRightmostItemIndex(
              leftGrandparentNodeItem.index, leftParentNodeItem.index
            )
          } else {
            // point left grandparent right child
            await this._writeBTreeRightChildNodeRightmostItemIndex(
              leftGrandparentNodeItem.index,
              btreeParentNodeItems[0].btreeLeftChildNodeRightmostItemIndex
            )
          }

          // point left grandparent left child
          await this._writeBTreeLeftChildNodeRightmostItemIndex(
            leftGrandparentNodeItem.index,
            btreeParentLeftSiblingNodeItems[btreeParentLeftSiblingNodeItems.length - 1].btreeRightChildNodeRightmostItemIndex
          )

          // parent left sibling node rightmost item becomes left grandparent

          if (leftGrandparentNodeItem.index === btreeRootNodeItems[btreeRootNodeItems.length - 1].index) {
            // point root
            await this._writeBTreeRootRightmostItemIndex(
              btreeParentLeftSiblingNodeItems[btreeParentLeftSiblingNodeItems.length - 1].index
            )
          }

          if (leftGrandparentNodeItem.rightItem) {
            // point left grandparent right item left item
            await this._writeBTreeLeftItemIndex(
              leftGrandparentNodeItem.rightItem.index,
              btreeParentLeftSiblingNodeItems[btreeParentLeftSiblingNodeItems.length - 1].index
            )

            if (btreeParentNodeItems.length === 1) { // no parents after right parent becomes leaf
              // point left grandparent right item left child
              await this._writeBTreeLeftChildNodeRightmostItemIndex(
                leftGrandparentNodeItem.rightItem.index, leftGrandparentNodeItem.index
              )
            }
          }

          if (leftGrandparentNodeItem.leftItem) {
            // point left grandparent left item right child
            await this._writeBTreeRightChildNodeRightmostItemIndex(
              leftGrandparentNodeItem.leftItem.index,
              btreeParentLeftSiblingNodeItems[btreeParentLeftSiblingNodeItems.length - 2].index
            )
          }

          // point parent left sibling node rightmost item right child left item
          await this._writeBTreeLeftItemIndex(
            btreeParentLeftSiblingNodeItems[btreeParentLeftSiblingNodeItems.length - 1].index,
            leftGrandparentNodeItem.btreeLeftItemIndex
          )

          if (leftParentNodeItem.leftItem) {
            // point parent left sibling node rightmost item right child
            await this._writeBTreeRightChildNodeRightmostItemIndex(
              btreeParentLeftSiblingNodeItems[btreeParentLeftSiblingNodeItems.length - 1].index,
              leftParentNodeItem.leftItem.index
            )
          } else {
            // point parent left sibling node rightmost item right child
            await this._writeBTreeRightChildNodeRightmostItemIndex(
              btreeParentLeftSiblingNodeItems[btreeParentLeftSiblingNodeItems.length - 1].index,
              leftGrandparentNodeItem.index
            )
          }

          // point parent left sibling node rightmost item left child
          await this._writeBTreeLeftChildNodeRightmostItemIndex(
            btreeParentLeftSiblingNodeItems[btreeParentLeftSiblingNodeItems.length - 1].index,
            btreeParentLeftSiblingNodeItems[btreeParentLeftSiblingNodeItems.length - 2].index
          )

          // great grandparent

          if (leftGrandparentNodeItem.btreeParentNodeItem) {
            await this._balanceBTreeGrandparentNodes(
              leftGrandparentNodeItem,
              btreeParentNodeItem.btreeParentNodeItems,
              btreeParentLeftSiblingNodeItems[btreeParentLeftSiblingNodeItems.length - 1]
            )
          }

        } else if (btreeParentRightSiblingNodeItems?.length > (this.degree - 1)) { // parent right sibling node over minimum

          const rightGrandparentNodeItem = getRightParentItem(btreeParentNodeItem.btreeParentNodeItem)

          // right grandparent item becomes parent item

          if (leftParentNodeItem.leftItem) {
            // point right grandparent left item
            await this._writeBTreeLeftItemIndex(
              rightGrandparentNodeItem.index, leftParentNodeItem.leftItem.index
            )
          } else { // no parents after left parent becomes leaf
            // point right grandparent left item
            await this._writeBTreeLeftItemIndex(
              rightGrandparentNodeItem.index, -1
            )
          }

          if (btreeNodeItems.length === 1) { // no items left in node after deletion
            // point right grandparent left child
            await this._writeBTreeLeftChildNodeRightmostItemIndex(
              rightGrandparentNodeItem.index, leftParentNodeItem.index
            )
          } else {
            const rightmostBTreeNodeItem = getRightmostNodeItemExcluding(btreeNodeItems, btreeItem)

            // point right grandparent left child
            await this._writeBTreeLeftChildNodeRightmostItemIndex(
              rightGrandparentNodeItem.index, rightmostBTreeNodeItem.index
            )
          }

          // right grandparent takes leftmost right grandchild node as right child
          // point right grandparent right child
          await this._writeBTreeRightChildNodeRightmostItemIndex(
            rightGrandparentNodeItem.index,
            btreeParentRightSiblingNodeItems[0].btreeLeftChildNodeRightmostItemIndex
          )

          // parent right sibling node leftmost item becomes right grandparent

          if (rightGrandparentNodeItem.index === btreeRootNodeItems[btreeRootNodeItems.length - 1].index) {
            // point root
            await this._writeBTreeRootRightmostItemIndex(btreeParentRightSiblingNodeItems[0].index)
          }

          if (rightGrandparentNodeItem.rightItem) {
            // point right grandparent right item left item
            await this._writeBTreeLeftItemIndex(
              rightGrandparentNodeItem.rightItem.index,
              btreeParentRightSiblingNodeItems[0].index
            )
          }

          if (rightGrandparentNodeItem.leftItem) {
            // point right grandparent left item right child
            await this._writeBTreeRightChildNodeRightmostItemIndex(
              rightGrandparentNodeItem.leftItem.index, rightGrandparentNodeItem.index
            )
          }

          // point parent right sibling node leftmost item left item
          await this._writeBTreeLeftItemIndex(
            btreeParentRightSiblingNodeItems[0].index,
            rightGrandparentNodeItem.btreeLeftItemIndex
          )

          // point parent right sibling node leftmost item left child
          await this._writeBTreeLeftChildNodeRightmostItemIndex(
            btreeParentRightSiblingNodeItems[0].index, rightGrandparentNodeItem.index
          )

          // point parent right sibling node leftmost item right child
          await this._writeBTreeRightChildNodeRightmostItemIndex(
            btreeParentRightSiblingNodeItems[0].index,
            btreeParentRightSiblingNodeItems[btreeParentRightSiblingNodeItems.length - 1].index
          )

          // parent right sibling node leftmost item right item becomes leftmost item
          await this._writeBTreeLeftItemIndex(
            btreeParentRightSiblingNodeItems[1].index, -1
          )

          // great grandparent

          if (rightGrandparentNodeItem.btreeParentNodeItem) {
            await this._balanceBTreeGrandparentNodes(
              rightGrandparentNodeItem,
              btreeParentNodeItem.btreeParentNodeItems,
              btreeParentRightSiblingNodeItems[0]
            )
          }

        } else if (btreeParentLeftSiblingNodeItems?.length === (this.degree - 1)) { // parent left sibling at minimum

          const leftGrandparentNodeItem = getLeftParentItem(btreeParentNodeItem.btreeParentNodeItem)

          // left grandparent item becomes parent item

          // point left grandparent left item
          await this._writeBTreeLeftItemIndex(
            leftGrandparentNodeItem.index,
            leftGrandparentNodeItem.btreeLeftChildNodeRightmostItemIndex
          )

          if (leftGrandparentNodeItem.rightItem) {
            // point left grandparent right item left item
            await this._writeBTreeLeftItemIndex(
              leftGrandparentNodeItem.rightItem.index,
              leftGrandparentNodeItem.btreeLeftItemIndex
            )

            if (leftParentNodeItem.leftItem) {
              // point left grandparent right item left child
              await this._writeBTreeLeftChildNodeRightmostItemIndex(
                leftGrandparentNodeItem.rightItem.index,
                leftParentNodeItem.leftItem.index
              )
            } else {
              // point left grandparent right item left child
              await this._writeBTreeLeftChildNodeRightmostItemIndex(
                leftGrandparentNodeItem.rightItem.index, leftGrandparentNodeItem.index
              )
            }
          }

          if (leftGrandparentNodeItem.leftItem) {
            if (leftParentNodeItem.leftItem) {
              // point left grandparent left item right child
              await this._writeBTreeRightChildNodeRightmostItemIndex(
                leftGrandparentNodeItem.leftItem.index,
                leftParentNodeItem.leftItem.index
              )
            } else {
              // point left grandparent left item right child
              await this._writeBTreeRightChildNodeRightmostItemIndex(
                leftGrandparentNodeItem.leftItem.index,
                leftGrandparentNodeItem.index
              )
            }
          }

          if (leftParentNodeItem.leftItem) {
            // point left grandparent right child
            await this._writeBTreeRightChildNodeRightmostItemIndex(
              leftGrandparentNodeItem.index,
              btreeParentNodeItems[0].btreeLeftChildNodeRightmostItemIndex
            )

            // merge parent node with left grandparent
            await this._writeBTreeLeftItemIndex(
              btreeParentNodeItems[0].index, leftGrandparentNodeItem.index
            )
          } else { // no parents after left parent becomes leaf
            // point left grandparent right child
            await this._writeBTreeRightChildNodeRightmostItemIndex(
              leftGrandparentNodeItem.index, leftParentNodeItem.index
            )
          }

          // point left grandparent left child
          await this._writeBTreeLeftChildNodeRightmostItemIndex(
            leftGrandparentNodeItem.index,
            btreeParentLeftSiblingNodeItems[btreeParentLeftSiblingNodeItems.length - 1].btreeRightChildNodeRightmostItemIndex
          )

          if (leftGrandparentNodeItem.btreeParentNodeItem) {
            // root rightmost item stays the same
          } else if (leftGrandparentNodeItem.rightItem) {
            // root rightmost item stays the same
          } else if (leftGrandparentNodeItem.leftItem) {
            // point root rightmost item
            await this._writeBTreeRootRightmostItemIndex(leftGrandparentNodeItem.leftItem.index)
          } else if (btreeParentNodeItem.leftItem) {
            // point root rightmost item
            await this._writeBTreeRootRightmostItemIndex(btreeParentNodeItem.leftItem.index)
          } else {
            // point root rightmost item
            await this._writeBTreeRootRightmostItemIndex(leftGrandparentNodeItem.index)
          }

          // great grandparent

          if (leftGrandparentNodeItem.btreeParentNodeItem) {
            await this._balanceBTreeGrandparentNodes(
              leftGrandparentNodeItem,
              btreeParentNodeItem.btreeParentNodeItems,
              null,
              btreeParentNodeItems.length === 1,
              leftParentNodeItem,
              btreeParentNodeItems
            )
          }

        } else if (btreeParentRightSiblingNodeItems?.length === (this.degree - 1)) { // parent right sibling at minimum (no left sibling)

          const rightGrandparentNodeItem = getRightParentItem(btreeParentNodeItem.btreeParentNodeItem)

          // right grandparent item becomes parent item

          if (rightGrandparentNodeItem.rightItem) {
            // point right grandparent right item
            await this._writeBTreeLeftItemIndex(
              rightGrandparentNodeItem.rightItem.index,
              rightGrandparentNodeItem.btreeLeftItemIndex
            )
          }

          if (leftParentNodeItem.leftItem) {
            // point right grandparent left item
            await this._writeBTreeLeftItemIndex(
              rightGrandparentNodeItem.index, leftParentNodeItem.leftItem.index
            )

            const rightmostBTreeNodeItem = getRightmostNodeItemExcluding(btreeNodeItems, btreeItem)

            // point right grandparent left child
            await this._writeBTreeLeftChildNodeRightmostItemIndex(
              rightGrandparentNodeItem.index, rightmostBTreeNodeItem.index
            )

          } else {
            // point right grandparent left child
            await this._writeBTreeLeftChildNodeRightmostItemIndex(
              rightGrandparentNodeItem.index, leftParentNodeItem.index
            )
          }

          // point right grandparent right child
          await this._writeBTreeRightChildNodeRightmostItemIndex(
            rightGrandparentNodeItem.index,
            btreeParentRightSiblingNodeItems[0].btreeLeftChildNodeRightmostItemIndex
          )

          // merge parent node with parent right sibling node
          await this._writeBTreeLeftItemIndex(
            btreeParentRightSiblingNodeItems[0].index,
            rightGrandparentNodeItem.index
          )

          if (rightGrandparentNodeItem.btreeParentNodeItem) {
            // root rightmost item stays the same
          } else if (rightGrandparentNodeItem.rightItem) {
            // root rightmost item stays the same
          } else {
            // point root
            await this._writeBTreeRootRightmostItemIndex(
              btreeParentRightSiblingNodeItems[btreeParentRightSiblingNodeItems.length - 1].index
            )
          }

          // great grandparent

          if (rightGrandparentNodeItem.btreeParentNodeItem) {
            await this._balanceBTreeGrandparentNodes(
              rightGrandparentNodeItem,
              btreeParentNodeItem.btreeParentNodeItems,
              null,
              false,
              leftParentNodeItem,
              btreeParentNodeItems
            )
          }

        }

      }

    }

  }

  // _deleteBTreeNodeItem(
  //   btreeItem {
  //     index: number,
  //     sortValue: string|number,
  //     btreeLeftChildNodeRightmostItemIndex: number,
  //     btreeRightChildNodeRightmostItemIndex: number,
  //     btreeLeftItemIndex: number,
  //   },
  //   btreeNodeItems Array<{
  //     index: number,
  //     sortValue: string|number,
  //     btreeLeftChildNodeRightmostItemIndex: number,
  //     btreeRightChildNodeRightmostItemIndex: number,
  //     btreeLeftItemIndex: number,
  //   },
  //   btreeRootNodeItems Array<{
  //     index: number,
  //     sortValue: string|number,
  //     btreeLeftChildNodeRightmostItemIndex: number,
  //     btreeRightChildNodeRightmostItemIndex: number,
  //     btreeLeftItemIndex: number,
  //   }>,
  //   btreeParentNodeItem {
  //     index: number,
  //     sortValue: string|number,
  //     btreeLeftChildNodeRightmostItemIndex: number,
  //     btreeRightChildNodeRightmostItemIndex: number,
  //     btreeLeftItemIndex: number,
  //   },
  //   btreeParentNodeItems Array<{
  //     index: number,
  //     sortValue: string|number,
  //     btreeLeftChildNodeRightmostItemIndex: number,
  //     btreeRightChildNodeRightmostItemIndex: number,
  //     btreeLeftItemIndex: number,
  //   }>,
  //   btreeParentLeftSiblingNodeItems Array<{
  //     index: number,
  //     sortValue: string|number,
  //     btreeLeftChildNodeRightmostItemIndex: number,
  //     btreeRightChildNodeRightmostItemIndex: number,
  //     btreeLeftItemIndex: number,
  //   }>,
  //   btreeParentRightSiblingNodeItems Array<{
  //     index: number,
  //     sortValue: string|number,
  //     btreeLeftChildNodeRightmostItemIndex: number,
  //     btreeRightChildNodeRightmostItemIndex: number,
  //     btreeLeftItemIndex: number,
  //   }>
  // ) -> Promise<>
  async _deleteBTreeNodeItem(
    btreeItem,
    btreeNodeItems,
    btreeRootNodeItems,
    btreeParentNodeItem,
    btreeParentNodeItems,
    btreeParentLeftSiblingNodeItems,
    btreeParentRightSiblingNodeItems,
  ) {

    let foundBTreeItem = false
    let i = 0
    while (i < btreeNodeItems.length) {
      if (btreeNodeItems[i].index === btreeItem.index) {
        foundBTreeItem = true
        break
      }
      i += 1
    }

    const isLeaf = btreeNodeItems[0].btreeRightChildNodeRightmostItemIndex === -1
      || btreeNodeItems[0].btreeLeftChildNodeRightmostItemIndex === -1

    if (foundBTreeItem) { // i < btreeNodeItems.length

      if (isLeaf) {

        if (btreeNodeItems.length > (this.degree - 1)) { // leaf node has more than the minimum number of items per node

          // remove btreeItem
          await this._writeBTreeLeftItemIndex(btreeItem.index, -1)
          if (i < (btreeNodeItems.length - 1)) {
            await this._writeBTreeLeftItemIndex(
              btreeNodeItems[i + 1].index,
              btreeItem.btreeLeftItemIndex
            )
          }

          if (i === (btreeNodeItems.length - 1)) { // rightmost item
            // point parent nodes' left/right child node rightmost item to left item

            const leftParentNodeItem = getLeftParentItem(btreeParentNodeItem)
            if (leftParentNodeItem) {
              // point left parent right child
              await this._writeBTreeRightChildNodeRightmostItemIndex(
                leftParentNodeItem.index, btreeItem.btreeLeftItemIndex
              )
            }

            const rightParentNodeItem = getRightParentItem(btreeParentNodeItem)
            if (rightParentNodeItem) {
              // point right parent left child
              await this._writeBTreeLeftChildNodeRightmostItemIndex(
                rightParentNodeItem.index, btreeItem.btreeLeftItemIndex
              )
            }

            if (btreeParentNodeItem == null) { // root node
              // left item becomes root node rightmost item
              await this._writeBTreeRootRightmostItemIndex(btreeItem.btreeLeftItemIndex)
            }
          }

        } else if (btreeParentNodeItem == null) { // root leaf node has the minimum number of items per node

          // remove btreeItem
          await this._writeBTreeLeftItemIndex(btreeItem.index, -1)
          if (i < (btreeNodeItems.length - 1)) {
            await this._writeBTreeLeftItemIndex(
              btreeNodeItems[i + 1].index,
              btreeItem.btreeLeftItemIndex
            )
          }

          if (i === (btreeNodeItems.length - 1)) { // rightmost item
            // left item becomes root node rightmost item
            await this._writeBTreeRootRightmostItemIndex(btreeItem.btreeLeftItemIndex)
          }

        } else { // leaf node has the minimum number of items per node

          // remove btreeItem
          await this._writeBTreeLeftItemIndex(btreeItem.index, -1)
          if (i < (btreeNodeItems.length - 1)) {
            await this._writeBTreeLeftItemIndex(
              btreeNodeItems[i + 1].index,
              btreeItem.btreeLeftItemIndex
            )
          }

          // balance b-tree
          await this._balanceBTreeAfterDelete(
            btreeItem,
            btreeNodeItems,
            btreeRootNodeItems,
            i,
            btreeParentNodeItem,
            btreeParentNodeItems,
            btreeParentLeftSiblingNodeItems,
            btreeParentRightSiblingNodeItems,
          )
        }

      } else { // internal

        // find in-order predecessor

        let predecessorParentNodeItem = btreeNodeItems[i]
        let predecessorParentNodeItems = btreeNodeItems

        for (let j = 0; j < predecessorParentNodeItems.length; j++) {
          predecessorParentNodeItems[j].btreeParentNodeItem = btreeParentNodeItem
          predecessorParentNodeItems[j].btreeParentNodeItems = btreeParentNodeItems
        }

        for (let j = 0; j < (predecessorParentNodeItems.length - 1); j++) {
          predecessorParentNodeItems[j].rightItem = predecessorParentNodeItems[j + 1]
          if (predecessorParentNodeItems[j].index === predecessorParentNodeItem.index) {
            predecessorParentNodeItem.rightItem = predecessorParentNodeItems[j + 1]
          }
        }
        for (let j = 1; j < predecessorParentNodeItems.length; j++) {
          predecessorParentNodeItems[j].leftItem = predecessorParentNodeItems[j - 1]
          if (predecessorParentNodeItems[j].index === predecessorParentNodeItem.index) {
            predecessorParentNodeItem.leftItem = predecessorParentNodeItems[j - 1]
          }
        }

        let predecessor = await this._getBTreeItem(
          predecessorParentNodeItem.btreeLeftChildNodeRightmostItemIndex
        )
        predecessor.btreeParentNodeItems = predecessorParentNodeItems
        predecessor.btreeParentNodeItem = predecessorParentNodeItem
        predecessor.btreeParentNodeItem.isLeftChildPointer = true

        while (predecessor.btreeRightChildNodeRightmostItemIndex > -1) {
          predecessorParentNodeItem = predecessor
          predecessorParentNodeItems =
            await this._getBTreeNodeItems(predecessorParentNodeItem)

          for (let j = 0; j < (predecessorParentNodeItems.length - 1); j++) {
            predecessorParentNodeItems[j].rightItem = predecessorParentNodeItems[j + 1]
            if (predecessorParentNodeItems[j].index === predecessorParentNodeItem.index) {
              predecessorParentNodeItem.rightItem = predecessorParentNodeItems[j + 1]
            }
          }
          for (let j = 1; j < predecessorParentNodeItems.length; j++) {
            predecessorParentNodeItems[j].leftItem = predecessorParentNodeItems[j - 1]
            if (predecessorParentNodeItems[j].index === predecessorParentNodeItem.index) {
              predecessorParentNodeItem.leftItem = predecessorParentNodeItems[j - 1]
            }
          }

          predecessor = await this._getBTreeItem(
            predecessorParentNodeItem.btreeRightChildNodeRightmostItemIndex
          )
          predecessor.btreeParentNodeItems = predecessorParentNodeItems
          predecessor.btreeParentNodeItem = predecessorParentNodeItem
          predecessor.btreeParentNodeItem.isRightChildPointer = true
        }

        const predecessorNodeItems = await this._getBTreeNodeItems(predecessor)
        const k = predecessorNodeItems.length - 1

        let predecessorParentLeftSiblingNodeRightmostItem
        if (predecessorParentNodeItem.btreeParentNodeItem == null) {
          // skip
        } else if (predecessorParentNodeItem.btreeParentNodeItem.isRightChildPointer) {
          predecessorParentLeftSiblingNodeRightmostItem = await this._getBTreeItem(
            predecessorParentNodeItem.btreeParentNodeItem.btreeLeftChildNodeRightmostItemIndex
          )
        }
        else if (predecessorParentNodeItem.btreeParentNodeItem.isLeftChildPointer) {
          if (predecessorParentNodeItem.btreeParentNodeItem.leftItem) {
            predecessorParentLeftSiblingNodeRightmostItem = await this._getBTreeItem(
              predecessorParentNodeItem.btreeParentNodeItem.leftItem.btreeLeftChildNodeRightmostItemIndex
            )
          }
        }

        let predecessorParentRightSiblingNodeRightmostItem
        if (predecessorParentNodeItem.btreeParentNodeItem == null) {
          // skip
        } else if (predecessorParentNodeItem.btreeParentNodeItem.isLeftChildPointer) {
          predecessorParentRightSiblingNodeRightmostItem = await this._getBTreeItem(
            predecessorParentNodeItem.btreeParentNodeItem.btreeRightChildNodeRightmostItemIndex
          )
        }
        else if (predecessorParentNodeItem.btreeParentNodeItem.isRightChildPointer) {
          if (predecessorParentNodeItem.btreeParentNodeItem.rightItem) {
            predecessorParentRightSiblingNodeRightmostItem = await this._getBTreeItem(
              predecessorParentNodeItem.btreeParentNodeItem.rightItem.btreeRightChildNodeRightmostItemIndex
            )
          }
        }

        const predecessorParentLeftSiblingNodeItems =
          predecessorParentLeftSiblingNodeRightmostItem == null
          ? undefined
          : await this._getBTreeNodeItems(predecessorParentLeftSiblingNodeRightmostItem)

        const predecessorParentRightSiblingNodeItems =
          predecessorParentRightSiblingNodeRightmostItem == null
          ? undefined
          : await this._getBTreeNodeItems(predecessorParentRightSiblingNodeRightmostItem)


        // move predecessor to btreeItem position in b-tree

        if (i === (btreeNodeItems.length - 1)) {
          const leftParentNodeItem = getLeftParentItem(btreeParentNodeItem)
          if (leftParentNodeItem) {
            // point left parent right child
            await this._writeBTreeRightChildNodeRightmostItemIndex(
              leftParentNodeItem.index, predecessor.index
            )
          }

          const rightParentNodeItem = getRightParentItem(btreeParentNodeItem)
          if (rightParentNodeItem) {
            // point right parent left child
            await this._writeBTreeLeftChildNodeRightmostItemIndex(
              rightParentNodeItem.index, predecessor.index
            )
          }
        }

        if (btreeItem.index === btreeRootNodeItems[btreeRootNodeItems.length - 1].index) {
          // point root
          await this._writeBTreeRootRightmostItemIndex(predecessor.index)
        }

        if (i < (btreeNodeItems.length - 1)) {
          // point right item of predecessor left item
          await this._writeBTreeLeftItemIndex(
            btreeNodeItems[i + 1].index, predecessor.index
          )
        }

        if (i > 0) {
          // point predecessor left item
          await this._writeBTreeLeftItemIndex(
            predecessor.index, btreeNodeItems[i - 1].index
          )
        } else if (predecessor.btreeLeftItemIndex > -1) {
          // point predecessor left item
          await this._writeBTreeLeftItemIndex(predecessor.index, -1)
        }

        if (btreeItem.btreeLeftChildNodeRightmostItemIndex === predecessor.index) {
          if (predecessorNodeItems.length > 1) {
            // point predecessor left child
            await this._writeBTreeLeftChildNodeRightmostItemIndex(
              predecessor.index, predecessor.btreeLeftItemIndex
            )
          } else {
            // predecessor left child will be set during _balanceBTreeAfterDelete
          }
        } else {
          // point predecessor left child
          await this._writeBTreeLeftChildNodeRightmostItemIndex(
            predecessor.index, btreeItem.btreeLeftChildNodeRightmostItemIndex
          )
        }

        // point predecessor right child
        await this._writeBTreeRightChildNodeRightmostItemIndex(
          predecessor.index, btreeItem.btreeRightChildNodeRightmostItemIndex
        )

        if (predecessorNodeItems.length > (this.degree - 1)) { // leaf node has more than the minimum number of items per node

          const predecessorLeftParentNodeItem = getLeftParentItem(predecessorParentNodeItem)
          if (predecessorLeftParentNodeItem) {
            // point predecessor left parent right child
            await this._writeBTreeRightChildNodeRightmostItemIndex(
              predecessorLeftParentNodeItem.index,
              predecessorNodeItems[predecessorNodeItems.length - 2].index
            )
          }

          const predecessorRightParentNodeItem = getRightParentItem(predecessorParentNodeItem)
          if (predecessorRightParentNodeItem) {
            // point predecessor right parent left child
            await this._writeBTreeLeftChildNodeRightmostItemIndex(
              predecessorRightParentNodeItem.index,
              predecessorNodeItems[predecessorNodeItems.length - 2].index
            )
          }

        } else {

          { // replace btreeItem with predecessor
            let curNodeItem = predecessor
            while (curNodeItem.btreeParentNodeItem) {

              if (curNodeItem.btreeParentNodeItem.index === btreeItem.index) {

                curNodeItem.btreeParentNodeItem.index = predecessor.index
                curNodeItem.btreeParentNodeItem.statusMarker = predecessor.statusMarker
                curNodeItem.btreeParentNodeItem.forwardIndex = predecessor.forwardIndex
                curNodeItem.btreeParentNodeItem.reverseIndex = predecessor.reverseIndex
                curNodeItem.btreeParentNodeItem.sortValue = predecessor.sortValue
                curNodeItem.btreeParentNodeItem.value = predecessor.value

                if (curNodeItem.btreeParentNodeItem.rightItem) {
                  curNodeItem.btreeParentNodeItem.rightItem.btreeLeftItemIndex = curNodeItem.index
                }

                const grandparentNodeItem = curNodeItem.btreeParentNodeItem.btreeParentNodeItem
                if (grandparentNodeItem && curNodeItem.btreeParentNodeItem.rightItem == null) {
                  const leftGrandparentNodeItem = getLeftParentItem(grandparentNodeItem)
                  if (leftGrandparentNodeItem) {
                    leftGrandparentNodeItem.btreeRightChildNodeRightmostItemIndex = curNodeItem.btreeParentNodeItem.index
                  }
                  const rightGrandparentNodeItem = getRightParentItem(grandparentNodeItem)
                  if (rightGrandparentNodeItem) {
                    rightGrandparentNodeItem.btreeLeftChildNodeRightmostItemIndex = curNodeItem.btreeParentNodeItem.index
                  }
                }

                break
              }

              curNodeItem = curNodeItem.btreeParentNodeItem

            }

          }

          // balance b-tree
          await this._balanceBTreeAfterDelete(
            predecessor,
            predecessorNodeItems,
            btreeRootNodeItems,
            k,
            predecessorParentNodeItem,
            predecessorParentNodeItems,
            predecessorParentLeftSiblingNodeItems,
            predecessorParentRightSiblingNodeItems
          )
        }

      }

      return
    }

    if (isLeaf) {
      throw new Error('Did not find btreeItem')
    }

    i = btreeNodeItems.length - 1
    while (i >= 0 && btreeNodeItems[i].sortValue >= btreeItem.sortValue) {
      i -= 1
    }

    i += 1 // if i === 0, sortValue is less than all items' sortValues in btreeNodeItems, move to btreeNodeItems[0].leftChild
           // else if i === btreeNodeItems.length, sortValue is greater than all items' sortValues in btreeNodeItems, move to btreeNodeItems[btreeNodeItems.length - 1].rightChild
           // else (i > 0 and i < btreeNodeItems.length), sortValue is less than btreeNodeItems[i].sortValue, move to btreeNodeItems[i].rightChild

    const leftParentNodeItem = getLeftParentItem(btreeParentNodeItem)
    const leftSiblingNodeItems = leftParentNodeItem == null
      ? []
      : await this._getBTreeNodeItems(await this._getBTreeItem(leftParentNodeItem.btreeLeftChildNodeRightmostItemIndex))

    const rightParentNodeItem = getRightParentItem(btreeParentNodeItem)
    const rightSiblingNodeItems = rightParentNodeItem == null
      ? []
      : await this._getBTreeNodeItems(await this._getBTreeItem(rightParentNodeItem.btreeRightChildNodeRightmostItemIndex))


    if (i === 0) { // move to left child

      const btreeChildNodeItemsParentNodeItem = btreeNodeItems[0]
      btreeChildNodeItemsParentNodeItem.isLeftChildPointer = true

      const btreeChildNodeRightmostItem = await this._getBTreeItem(
        btreeChildNodeItemsParentNodeItem.btreeLeftChildNodeRightmostItemIndex
      )
      const btreeChildNodeItems = await this._getBTreeNodeItems(btreeChildNodeRightmostItem)

      btreeChildNodeItemsParentNodeItem.btreeParentNodeItems = btreeParentNodeItems
      btreeChildNodeItemsParentNodeItem.btreeParentNodeItem = btreeParentNodeItem

      for (const item of btreeChildNodeItems) {
        item.btreeParentNodeItems = btreeNodeItems
        item.btreeParentNodeItem = btreeChildNodeItemsParentNodeItem
      }
      for (let j = 0; j < (btreeNodeItems.length - 1); j++) {
        btreeNodeItems[j].rightItem = btreeNodeItems[j + 1]
      }
      for (let j = 1; j < btreeNodeItems.length; j++) {
        btreeNodeItems[j].leftItem = btreeNodeItems[j - 1]
      }

      if (btreeChildNodeItemsParentNodeItem.rightItem) {
        btreeChildNodeItemsParentNodeItem.rightItem.isLeftChildPointer = true
      }

      return this._deleteBTreeNodeItem(
        btreeItem,
        btreeChildNodeItems,
        btreeRootNodeItems,
        btreeChildNodeItemsParentNodeItem,
        btreeNodeItems,
        leftSiblingNodeItems,
        rightSiblingNodeItems
      )
    }


    // move to right child

    const btreeChildNodeItemsParentNodeItem = btreeNodeItems[i - 1]
    btreeChildNodeItemsParentNodeItem.isRightChildPointer = true

    const btreeChildNodeRightmostItem = await this._getBTreeItem(
      btreeChildNodeItemsParentNodeItem.btreeRightChildNodeRightmostItemIndex
    )
    const btreeChildNodeItems = await this._getBTreeNodeItems(btreeChildNodeRightmostItem)

    btreeChildNodeItemsParentNodeItem.btreeParentNodeItems = btreeParentNodeItems
    btreeChildNodeItemsParentNodeItem.btreeParentNodeItem = btreeParentNodeItem

    for (const item of btreeChildNodeItems) {
      item.btreeParentNodeItems = btreeNodeItems
      item.btreeParentNodeItem = btreeChildNodeItemsParentNodeItem
    }
    for (let j = 0; j < (btreeNodeItems.length - 1); j++) {
      btreeNodeItems[j].rightItem = btreeNodeItems[j + 1]
    }
    for (let j = 1; j < btreeNodeItems.length; j++) {
      btreeNodeItems[j].leftItem = btreeNodeItems[j - 1]
    }

    if (btreeChildNodeItemsParentNodeItem.leftItem) {
      btreeChildNodeItemsParentNodeItem.leftItem.isRightChildPointer = true
    }
    if (btreeChildNodeItemsParentNodeItem.rightItem) {
      btreeChildNodeItemsParentNodeItem.rightItem.isLeftChildPointer = true
    }

    return this._deleteBTreeNodeItem(
      btreeItem,
      btreeChildNodeItems,
      btreeRootNodeItems,
      btreeChildNodeItemsParentNodeItem,
      btreeNodeItems,
      leftSiblingNodeItems,
      rightSiblingNodeItems
    )
  }

  // _setStatusMarker(index number, marker number) -> Promise<>
  async _setStatusMarker(index, marker) {
    const position = index * this.itemSize
    const buffer = Buffer.alloc(1)
    buffer.writeUInt8(marker, 0)

    await this.storageFd.write(buffer, {
      offset: 0,
      position,
      length: buffer.length,
    })
  }

  // _parseItemHead(headReadBuffer Buffer, index number) -> {
  //   statusMarker: 0|1|2,
  //   index: number,
  //   sortValue: string|number,
  //   btreeLeftChildNodeRightmostItemIndex: number,
  //   btreeRightChildNodeRightmostItemIndex: number,
  //   btreeLeftItemIndex: number,
  // }
  _parseItemHead(headReadBuffer, index) {
    const item = {}
    item.index = index

    const statusMarker = headReadBuffer.readUInt8(0)
    item.statusMarker = statusMarker

    const forwardIndex = Number(headReadBuffer.readBigInt64BE(13))
    const reverseIndex = Number(headReadBuffer.readBigInt64BE(21))
    item.forwardIndex = forwardIndex
    item.reverseIndex = reverseIndex

    const btreeLeftChildNodeRightmostItemIndex = Number(headReadBuffer.readBigInt64BE(29))
    const btreeRightChildNodeRightmostItemIndex = Number(headReadBuffer.readBigInt64BE(37))
    const btreeLeftItemIndex = Number(headReadBuffer.readBigInt64BE(45))

    item.btreeLeftChildNodeRightmostItemIndex = btreeLeftChildNodeRightmostItemIndex
    item.btreeRightChildNodeRightmostItemIndex = btreeRightChildNodeRightmostItemIndex
    item.btreeLeftItemIndex = btreeLeftItemIndex

    const keyByteLength = headReadBuffer.readUInt32BE(1)
    item.keyByteLength = keyByteLength

    const sortValueByteLength = headReadBuffer.readUInt32BE(5)
    item.sortValueByteLength = sortValueByteLength

    const valueByteLength = headReadBuffer.readUInt32BE(9)
    item.valueByteLength = valueByteLength

    return item
  }

  // _parseItem(readBuffer Buffer, index number, valueType 'string'|'binary') -> {
  //   index: number,
  //   readBuffer: Buffer,
  //   sortValue: string|number,
  //   key: string,
  //   value: string,
  // }
  _parseItem(readBuffer, index, valueType = 'binary') {
    const item = {}
    item.index = index
    item.readBuffer = readBuffer

    const statusMarker = readBuffer.readUInt8(0)
    item.statusMarker = statusMarker

    const forwardIndex = Number(readBuffer.readBigInt64BE(13))
    const reverseIndex = Number(readBuffer.readBigInt64BE(21))
    item.forwardIndex = forwardIndex
    item.reverseIndex = reverseIndex

    const btreeLeftChildNodeRightmostItemIndex = Number(readBuffer.readBigInt64BE(29))
    const btreeRightChildNodeRightmostItemIndex = Number(readBuffer.readBigInt64BE(37))
    const btreeLeftItemIndex = Number(readBuffer.readBigInt64BE(45))

    item.btreeLeftChildNodeRightmostItemIndex = btreeLeftChildNodeRightmostItemIndex
    item.btreeRightChildNodeRightmostItemIndex = btreeRightChildNodeRightmostItemIndex
    item.btreeLeftItemIndex = btreeLeftItemIndex

    const keyByteLength = readBuffer.readUInt32BE(1)
    const keyBuffer = readBuffer.subarray(53, keyByteLength + 53)
    const key = keyBuffer.toString(ENCODING)
    item.key = key

    const sortValueByteLength = readBuffer.readUInt32BE(5)
    const sortValueBuffer = readBuffer.subarray(
      53 + keyByteLength,
      53 + keyByteLength + sortValueByteLength
    )
    const sortValue = convert(sortValueBuffer.toString(ENCODING), this.sortValueType)
    item.sortValue = sortValue

    const valueByteLength = readBuffer.readUInt32BE(9)
    const valueBuffer = readBuffer.subarray(
      53 + keyByteLength + sortValueByteLength,
      53 + keyByteLength + sortValueByteLength + valueByteLength
    )
    const value = valueType == 'binary' ? valueBuffer : valueBuffer.toString(ENCODING)
    item.value = value

    return item
  }

  // _getForwardStartItem(valueType 'string'|'binary') -> item { index: number, readBuffer: Buffer, sortValue: string|number, value: string }
  async _getForwardStartItem(valueType = 'binary') {
    const headerReadBuffer = await this._readHeader()
    const index = Number(headerReadBuffer.readBigInt64BE(24))
    if (index == -1) {
      return undefined
    }
    const readBuffer = await this._read(index)
    return this._parseItem(readBuffer, index, valueType)
  }

  // _getReverseStartItem(valueType 'string'|'number') -> item { index: number, readBuffer: Buffer, sortValue: string|number, value: string }
  async _getReverseStartItem(valueType = 'binary') {
    const headerReadBuffer = await this._readHeader()
    const index = Number(headerReadBuffer.readBigInt64BE(32))
    if (index == -1) {
      return undefined
    }
    const readBuffer = await this._read(index)
    return this._parseItem(readBuffer, index, valueType)
  }

  // _getItem(index number, valueType 'string'|'binary') -> item { index: number, readBuffer: Buffer, sortValue: string|number, value: string }
  async _getItem(index, valueType = 'binary') {
    if (index == -1) {
      return undefined
    }
    const readBuffer = await this._read(index)
    const item = this._parseItem(readBuffer, index, valueType)

    if (item.statusMarker == EMPTY) {
      return undefined
    }

    return item
  }

  // _updateForwardIndex(index number, forwardIndex number) -> Promise<>
  async _updateForwardIndex(index, forwardIndex) {
    const position = (index * this.itemSize) + 13
    const buffer = Buffer.alloc(8)
    buffer.writeBigInt64BE(BigInt(forwardIndex), 0)

    await this.storageFd.write(buffer, {
      offset: 0,
      position,
      length: buffer.length,
    })
  }

  // _updateReverseIndex(index number, reverseIndex number) -> Promise<>
  async _updateReverseIndex(index, reverseIndex) {
    const position = (index * this.itemSize) + 21
    const buffer = Buffer.alloc(8)
    buffer.writeBigInt64BE(BigInt(reverseIndex), 0)

    await this.storageFd.write(buffer, {
      offset: 0,
      position,
      length: buffer.length,
    })
  }

  // _updateCount() -> Promise<>
  async _updateCount() {
    const position = 8
    const buffer = Buffer.alloc(8)
    buffer.writeBigUInt64BE(BigInt(this._count), 0)

    await this.headerFd.write(buffer, {
      offset: 0,
      position,
      length: 8,
    })
  }

  // _updateDeletedCount() -> Promise<>
  async _updateDeletedCount() {
    const position = 16
    const buffer = Buffer.alloc(8)
    buffer.writeBigUInt64BE(BigInt(this._deletedCount), 0)

    await this.headerFd.write(buffer, {
      offset: 0,
      position,
      length: 8,
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

  // _insert(key string, value string|Buffer|Uint8Array, sortValue number|string, index number) -> Promise<>
  async _insert(key, value, sortValue, index) {

    const btreeLeftChildNodeRightmostItemIndex = -1
    const btreeRightChildNodeRightmostItemIndex = -1
    let btreeLeftItemIndex = -1

    let leftItem = null
    let rightItem = null
    const btreeRootNodeRightmostItem = await this._getBTreeRootNodeRightmostItem()
    const btreeRootNodeItems = await this._getBTreeNodeItems(btreeRootNodeRightmostItem)

    if (btreeRootNodeRightmostItem == null) { // first item in btree
      await this._writeBTreeRootRightmostItemIndex(index)
    }
    else if (btreeRootNodeItems.length == ((2 * this.degree) - 1)) { // current b-tree node at maximum number of items
      const btreeRootNodeItem = await this._splitBTreeRootNode(btreeRootNodeItems)
      const insertResult = await this._insertBTreeNodeItem(
        index,
        sortValue,
        [btreeRootNodeItem],
        btreeRootNodeItem
      )

      leftItem = insertResult.predecessor
      rightItem = insertResult.successor
      btreeLeftItemIndex = insertResult.btreeLeftItemIndex

      // new b-tree item is always inserted at leaf

    } else {
      const insertResult = await this._insertBTreeNodeItem(
        index,
        sortValue,
        btreeRootNodeItems,
        btreeRootNodeRightmostItem
      )

      leftItem = insertResult.predecessor
      rightItem = insertResult.successor
      btreeLeftItemIndex = insertResult.btreeLeftItemIndex
      if (btreeLeftItemIndex === btreeRootNodeRightmostItem.index) {
        await this._writeBTreeRootRightmostItemIndex(index)
      }
    }

    const forwardStartItem = await this._getForwardStartItem()

    let reverseIndex = -1
    let forwardIndex = -1
    if (leftItem == null) { // item to insert is first in the list
      await this._writeFirstIndex(index)
      if (forwardStartItem == null) { // item to insert is also last in the list
        await this._writeLastIndex(index)
      } else {
        forwardIndex = forwardStartItem.index
        await this._updateReverseIndex(forwardStartItem.index, index)
      }
    } else if (rightItem == null) { // item to insert is the last in the list
      await this._writeLastIndex(index)
      await this._updateForwardIndex(leftItem.index, index)
      reverseIndex = leftItem.index
    } else { // item to insert is ahead of previousForwardItem and there was an item ahead of previousForwardItem
      await this._updateForwardIndex(leftItem.index, index)
      await this._updateReverseIndex(rightItem.index, index)
      forwardIndex = rightItem.index
      reverseIndex = leftItem.index
    }

    const position = index * this.itemSize
    const buffer = Buffer.alloc(this.itemSize)
    const sortValueString = typeof sortValue == 'string' ? sortValue : sortValue.toString()


    // storage file slice
    // 1 byte for status marker: 0 empty / 1 occupied / 2 deleted
    // 4 bytes for key size
    // 4 bytes for sort value size
    // 4 bytes for value size
    // 8 bytes for forward index
    // 8 bytes for reverse index
    // 8 bytes for btree left child node rightmost item index
    // 8 bytes for btree right child node rightmost item index
    // 8 bytes for btree left item index
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
    buffer.writeBigInt64BE(BigInt(forwardIndex), 13)
    buffer.writeBigInt64BE(BigInt(reverseIndex), 21)
    buffer.writeBigInt64BE(BigInt(btreeLeftChildNodeRightmostItemIndex), 29)
    buffer.writeBigInt64BE(BigInt(btreeRightChildNodeRightmostItemIndex), 37)
    buffer.writeBigInt64BE(BigInt(btreeLeftItemIndex), 45)
    buffer.write(key, 53, keyByteLength, ENCODING)
    buffer.write(sortValueString, 53 + keyByteLength, sortValueByteLength, ENCODING)

    if (value.constructor == Uint8Array) {
      Buffer.from(value).copy(buffer, 53 + keyByteLength + sortValueByteLength)
    } else if (value.constructor == Buffer) {
      value.copy(buffer, 53 + keyByteLength + sortValueByteLength)
    } else {
      buffer.write(value, 53 + keyByteLength + sortValueByteLength, valueByteLength, ENCODING)
    }

    await this.storageFd.write(buffer, {
      offset: 0,
      position,
      length: buffer.length,
    })
  }

// _update(key string, value string|Buffer|Uint8Array, sortValue number|string, index number, item {
//   index: number,
//   sortValue: string|number,
//   btreeLeftChildNodeRightmostItemIndex: number,
//   btreeRightChildNodeRightmostItemIndex: number,
//   btreeLeftItemIndex: number,
// }) -> Promise<>
  async _update(key, value, sortValue, index, item) {

    let btreeLeftChildNodeRightmostItemIndex = item.btreeLeftChildNodeRightmostItemIndex
    let btreeRightChildNodeRightmostItemIndex = item.btreeRightChildNodeRightmostItemIndex
    let btreeLeftItemIndex = item.btreeLeftItemIndex

    let forwardIndex = item.forwardIndex
    let reverseIndex = item.reverseIndex

    if (sortValue == item.sortValue) {

      if (item.statusMarker == REMOVED) {

        let leftItem = null
        let rightItem = null
        const btreeRootNodeRightmostItem = await this._getBTreeRootNodeRightmostItem()
        const btreeRootNodeItems = await this._getBTreeNodeItems(btreeRootNodeRightmostItem)

        btreeLeftChildNodeRightmostItemIndex = -1
        btreeRightChildNodeRightmostItemIndex = -1

        if (btreeRootNodeRightmostItem == null) { // first item in btree
          await this._writeBTreeRootRightmostItemIndex(index)
          btreeLeftItemIndex = -1
        } else if (btreeRootNodeItems.length == ((2 * this.degree) - 1)) { // current b-tree node at maximum number of items
          const btreeRootNodeItem = await this._splitBTreeRootNode(btreeRootNodeItems, { sortValue })
          const insertResult = await this._insertBTreeNodeItem(
            index,
            sortValue,
            [btreeRootNodeItem],
            btreeRootNodeItem
          )

          leftItem = insertResult.predecessor
          rightItem = insertResult.successor
          btreeLeftItemIndex = insertResult.btreeLeftItemIndex

          // new b-tree item is always inserted at leaf

        } else {
          const insertResult = await this._insertBTreeNodeItem(
            index,
            sortValue,
            btreeRootNodeItems,
            btreeRootNodeRightmostItem
          )

          leftItem = insertResult.predecessor
          rightItem = insertResult.successor
          btreeLeftItemIndex = insertResult.btreeLeftItemIndex
          if (btreeLeftItemIndex === btreeRootNodeRightmostItem.index) {
            await this._writeBTreeRootRightmostItemIndex(index)
          }
        }

        const forwardStartItem = await this._getForwardStartItem()

        if (leftItem == null) { // item to update is first in the list
          reverseIndex = -1
          await this._writeFirstIndex(index)
          if (forwardStartItem == null) { // item to update is also last in the list
            forwardIndex = -1
            await this._writeLastIndex(index)
          } else {
            forwardIndex = forwardStartItem.index
            await this._updateReverseIndex(forwardStartItem.index, index)
          }
        } else if (rightItem == null) { // item to update is the last in the list
          forwardIndex = -1
          await this._writeLastIndex(index)
          await this._updateForwardIndex(leftItem.index, index)
          reverseIndex = leftItem.index
        } else { // item to update is ahead of leftItem and there was an item ahead of leftItem
          await this._updateForwardIndex(leftItem.index, index)
          await this._updateReverseIndex(rightItem.index, index)
          forwardIndex = leftItem.forwardIndex
          reverseIndex = leftItem.index
        }

      }

    } else {

      if (item.statusMarker == OCCUPIED) {
        const btreeRootNodeRightmostItem = await this._getBTreeRootNodeRightmostItem()
        const btreeRootNodeItems = await this._getBTreeNodeItems(btreeRootNodeRightmostItem)
        const btreeItem = await this._getBTreeItem(index)
        await this._deleteBTreeNodeItem(btreeItem, btreeRootNodeItems, btreeRootNodeItems)
      }

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


      let leftItem = null
      let rightItem = null

      {
        const btreeRootNodeRightmostItem = await this._getBTreeRootNodeRightmostItem()
        const btreeRootNodeItems = await this._getBTreeNodeItems(btreeRootNodeRightmostItem)

        btreeLeftChildNodeRightmostItemIndex = -1
        btreeRightChildNodeRightmostItemIndex = -1

        if (btreeRootNodeRightmostItem == null) { // first item in btree
          await this._writeBTreeRootRightmostItemIndex(index)
          btreeLeftItemIndex = -1
        }
        else if (btreeRootNodeItems.length == ((2 * this.degree) - 1)) { // current b-tree node at maximum number of items
          const btreeRootNodeItem = await this._splitBTreeRootNode(btreeRootNodeItems)
          const insertResult = await this._insertBTreeNodeItem(
            index,
            sortValue,
            [btreeRootNodeItem],
            btreeRootNodeItem
          )

          leftItem = insertResult.predecessor
          rightItem = insertResult.successor
          btreeLeftItemIndex = insertResult.btreeLeftItemIndex
          if (btreeLeftItemIndex === btreeRootNodeItem.index) {
            await this._writeBTreeRootRightmostItemIndex(index)
          }

        } else {
          const insertResult = await this._insertBTreeNodeItem(
            index,
            sortValue,
            btreeRootNodeItems,
            btreeRootNodeRightmostItem
          )

          leftItem = insertResult.predecessor
          rightItem = insertResult.successor
          btreeLeftItemIndex = insertResult.btreeLeftItemIndex
          if (btreeLeftItemIndex === btreeRootNodeRightmostItem.index) {
            await this._writeBTreeRootRightmostItemIndex(index)
          }
        }
      }

      const forwardStartItem = await this._getForwardStartItem()

      if (leftItem == null) { // item to update is first in the list
        reverseIndex = -1
        await this._writeFirstIndex(index)
        if (forwardStartItem == null) { // item to update is also last in the list
          forwardIndex = -1
          await this._writeLastIndex(index)
        } else {
          forwardIndex = forwardStartItem.index
          await this._updateReverseIndex(forwardStartItem.index, index)
        }
      } else if (rightItem == null) { // item to update is the last in the list
        forwardIndex = -1
        await this._writeLastIndex(index)
        await this._updateForwardIndex(leftItem.index, index)
        reverseIndex = leftItem.index
      } else { // item to update is ahead of leftItem and there was an item ahead of leftItem
        await this._updateForwardIndex(leftItem.index, index)
        await this._updateReverseIndex(rightItem.index, index)
        forwardIndex = leftItem.forwardIndex
        reverseIndex = leftItem.index
      }

    }

    const position = index * this.itemSize
    const buffer = Buffer.alloc(this.itemSize)
    const sortValueString = typeof sortValue == 'string' ? sortValue : sortValue.toString()

    const statusMarker = 1
    const keyByteLength = Buffer.byteLength(key, ENCODING)
    const sortValueByteLength = Buffer.byteLength(sortValueString, ENCODING)
    const valueByteLength = Buffer.byteLength(value, ENCODING)
    buffer.writeUInt8(statusMarker, 0)
    buffer.writeUInt32BE(keyByteLength, 1)
    buffer.writeUInt32BE(sortValueByteLength, 5)
    buffer.writeUInt32BE(valueByteLength, 9)
    buffer.writeBigInt64BE(BigInt(forwardIndex), 13)
    buffer.writeBigInt64BE(BigInt(reverseIndex), 21)
    buffer.writeBigInt64BE(BigInt(btreeLeftChildNodeRightmostItemIndex), 29)
    buffer.writeBigInt64BE(BigInt(btreeRightChildNodeRightmostItemIndex), 37)
    buffer.writeBigInt64BE(BigInt(btreeLeftItemIndex), 45)
    buffer.write(key, 53, keyByteLength, ENCODING)
    buffer.write(sortValueString, 53 + keyByteLength, sortValueByteLength, ENCODING)

    if (value.constructor == Uint8Array) {
      Buffer.from(value).copy(buffer, 53 + keyByteLength + sortValueByteLength)
    } else if (value.constructor == Buffer) {
      value.copy(buffer, 53 + keyByteLength + sortValueByteLength)
    } else {
      buffer.write(value, 53 + keyByteLength + sortValueByteLength, valueByteLength, ENCODING)
    }

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
      storagePath: temporaryStoragePath,
      headerPath: temporaryHeaderPath,
      initialLength: this._length * this.resizeFactor,
      sortValueType: this.sortValueType,
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
    sortValue = convert(sortValue, this.sortValueType)

    let index = this._hash1(key)

    const startIndex = index
    const stepSize = this._hash2(key)

    let currentItem = await this._getKeyItem(index)
    while (currentItem) {
      if (key == currentItem.key) {
        break
      }
      index = (index + stepSize) % this._length
      if (index == startIndex) {
        if (this._count < this._length) {
          throw new Error('Unreachable index')
        }
        throw new Error('Hash table is full')
      }
      currentItem = await this._getKeyItem(index)
    }

    if (currentItem == null) { // insert
      await this._insert(key, value, sortValue, index)
      await this._incrementCount()
    } else { // update
      currentItem.sortValue = await this._readSortValue(
        index,
        currentItem.keyByteLength,
        currentItem.sortValueByteLength
      )
      await this._update(key, value, sortValue, index, currentItem)
      if (currentItem.statusMarker == REMOVED) {
        await this._incrementCount()
        await this._decrementDeletedCount()
      }
    }
  }

  /**
   * @name set
   *
   * @docs
   * ```coffeescript [specscript]
   * set(key string, value string|Buffer|Uint8Array, sortValue string|number) -> Promise<>
   * ```
   *
   * Sets and stores a value by key and sort-value in the disk sorted hash table.
   *
   * Arguments:
   *   * `key` - `string` - the key to set.
   *   * `value` - `string|Buffer|Uint8Array` - the value to set corresponding to the key.
   *   * `sortValue` - `string|number` - the value by which the item is sorted in the disk sorted hash table.
   *
   * Return:
   *   * Empty promise.
   *
   * ```javascript
   * await sortedHt.set('key1', 'value1', 1)
   * await sortedHt.set('key2', 'value2', 2)
   * await sortedHt.set('key3', 'value3', 3)
   *
   * await sortedHt.set('my-buffer', Buffer.from('binary'), 4)
   * ```
   */
  async set(key, value, sortValue) {
    if (this.resizeRatio > 0 && ((this._count + this._deletedCount) / this._length) >= this.resizeRatio) {
      await this._resize()
    }

    await this._set(key, value, sortValue)
  }

  // _get(key string, valueType 'string'|'binary')
  async _get(key, valueType) {
    let index = this._hash1(key)
    const startIndex = index
    const stepSize = this._hash2(key)

    let currentItem = await this._getKeyItem(index, valueType)
    while (currentItem) {
      if (key == currentItem.key) {
        break
      }

      index = (index + stepSize) % this._length
      if (index == startIndex) {
        return undefined // entire table searched
      }

      currentItem = await this._getKeyItem(index, valueType)
    }

    if (currentItem == null) {
      return undefined
    }

    if (currentItem.statusMarker == OCCUPIED) {
      const valueBuffer = await this._readBinaryValue(
        index,
        currentItem.keyByteLength,
        currentItem.sortValueByteLength,
        currentItem.valueByteLength
      )
      return valueType == 'binary' ? valueBuffer : valueBuffer.toString(ENCODING)
    }

    return undefined
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
    return this._get(key, 'string')
  }

  /**
   * @name getBinary
   *
   * @docs
   * ```coffeescript [specscript]
   * getBinary(key string) -> binaryValue Promise<Buffer>
   * ```
   *
   * Gets a binary value by key from the disk sorted hash table.
   *
   * Arguments:
   *   * `key` - `string` - the key corresponding to the binary value.
   *
   * Return:
   *   * `binaryValue` - `Buffer` - the binary value corresponding to the key.
   *
   * ```javascript
   * const buffer = await ht.getBinary('my-buffer')
   * ```
   */
  async getBinary(key) {
    return this._get(key, 'binary')
  }

  // _findBTreeNodeItemGTE(
  //   sortValue string|number,
  //   btreeNodeItems Array<{
  //     index: number,
  //     sortValue: string|number,
  //     btreeLeftChildNodeRightmostItemIndex: number,
  //     btreeRightChildNodeRightmostItemIndex: number,
  //     btreeLeftItemIndex: number,
  //   }>,
  //   btreeRootNodeRightmostItem: {
  //     index: number,
  //     sortValue: string|number,
  //     btreeLeftChildNodeRightmostItemIndex: number,
  //     btreeRightChildNodeRightmostItemIndex: number,
  //     btreeLeftItemIndex: number,
  //   },
  //   btreeParentNodeItem {
  //     index: number,
  //     sortValue: string|number,
  //     btreeLeftChildNodeRightmostItemIndex: number,
  //     btreeRightChildNodeRightmostItemIndex: number,
  //     btreeLeftItemIndex: number,
  //   },
  //   foundItem {
  //     index: number,
  //     sortValue: string|number,
  //     btreeLeftChildNodeRightmostItemIndex: number,
  //     btreeRightChildNodeRightmostItemIndex: number,
  //     btreeLeftItemIndex: number,
  //   }
  // )
  async _findBTreeNodeItemGTE(
    sortValue,
    btreeNodeItems,
    btreeRootNodeRightmostItem,
    btreeParentNodeItem,
    foundItem
  ) {

    // first item GTE sortValue
    // leftmost item equal to sortValue

    let i = 0

    while (i < btreeNodeItems.length) {
      const item = btreeNodeItems[i]
      if (item.sortValue >= sortValue) {
        foundItem = item
        break
      }
      i += 1
    }

    if (
      btreeNodeItems[0].btreeRightChildNodeRightmostItemIndex == -1
      || btreeNodeItems[0].btreeLeftChildNodeRightmostItemIndex == -1
    ) { // leaf node
      return foundItem
    }


    i = btreeNodeItems.length - 1

    while (i >= 0 && btreeNodeItems[i].sortValue >= sortValue) {
      i -= 1
    }
    i += 1 // if i === 0, sortValue is less than all items' sortValues in btreeNodeItems, insert item at very left
           // if i === btreeNodeItems.length, sortValue is greater than all items' sortValues in btreeNodeItems, insert item at very right
           // if i > 0, sortValue is less than btreeNodeItems[i].sortValue, insert item in middle

    if (i === 0) { // move to left child

      let btreeChildNodeItemsParentNodeItem
      let btreeChildNodeItemsLeftParentNodeItem
      let btreeChildNodeItemsRightParentNodeItem = btreeNodeItems[i]

      btreeChildNodeItemsParentNodeItem = btreeChildNodeItemsRightParentNodeItem

      const btreeChildNodeRightmostItem = await this._getBTreeItem(
        btreeChildNodeItemsParentNodeItem.btreeLeftChildNodeRightmostItemIndex
      )
      let btreeChildNodeItems = await this._getBTreeNodeItems(btreeChildNodeRightmostItem)

      return this._findBTreeNodeItemGTE(
        sortValue,
        btreeChildNodeItems,
        btreeRootNodeRightmostItem,
        btreeChildNodeItemsParentNodeItem,
        foundItem
      )
    }

    // move to right child

    let btreeChildNodeItemsParentNodeItem
    let btreeChildNodeItemsLeftParentNodeItem
    let btreeChildNodeItemsRightParentNodeItem

    if (i === btreeNodeItems.length) { // sortValue is greater than all items' sortValues in btreeNodeItems
      btreeChildNodeItemsLeftParentNodeItem = btreeNodeItems[i - 1]
      btreeChildNodeItemsRightParentNodeItem = undefined
    }
    else { // (i > 0) sortValue is less than btreeNodeItems[i].sortValue, insert item in middle
      btreeChildNodeItemsLeftParentNodeItem = btreeNodeItems[i - 1]
      btreeChildNodeItemsRightParentNodeItem = btreeNodeItems[i]
      // i === (btreeNodeItems.length - 1) ? undefined : btreeNodeItems[i + 1]
    }
    btreeChildNodeItemsParentNodeItem = btreeChildNodeItemsLeftParentNodeItem

    const btreeChildNodeRightmostItem = await this._getBTreeItem(
      btreeChildNodeItemsParentNodeItem.btreeRightChildNodeRightmostItemIndex
    )
    let btreeChildNodeItems = await this._getBTreeNodeItems(btreeChildNodeRightmostItem)

    return this._findBTreeNodeItemGTE(
      sortValue,
      btreeChildNodeItems,
      btreeRootNodeRightmostItem,
      btreeChildNodeItemsParentNodeItem,
      foundItem
    )
  }

  // _findBTreeNodeItemLTE(
  //   sortValue string|number,
  //   btreeNodeItems Array<{
  //     index: number,
  //     sortValue: string|number,
  //     btreeLeftChildNodeRightmostItemIndex: number,
  //     btreeRightChildNodeRightmostItemIndex: number,
  //     btreeLeftItemIndex: number,
  //   }>,
  //   btreeRootNodeRightmostItem: {
  //     index: number,
  //     sortValue: string|number,
  //     btreeLeftChildNodeRightmostItemIndex: number,
  //     btreeRightChildNodeRightmostItemIndex: number,
  //     btreeLeftItemIndex: number,
  //   },
  //   btreeParentNodeItem {
  //     index: number,
  //     sortValue: string|number,
  //     btreeLeftChildNodeRightmostItemIndex: number,
  //     btreeRightChildNodeRightmostItemIndex: number,
  //     btreeLeftItemIndex: number,
  //   },
  //   foundItem {
  //     index: number,
  //     sortValue: string|number,
  //     btreeLeftChildNodeRightmostItemIndex: number,
  //     btreeRightChildNodeRightmostItemIndex: number,
  //     btreeLeftItemIndex: number,
  //   }
  // )
  async _findBTreeNodeItemLTE(
    sortValue,
    btreeNodeItems,
    btreeRootNodeRightmostItem,
    btreeParentNodeItem,
    foundItem
  ) {

    // first item LTE sortValue
    // rightmost item equal to sortValue

    let i = btreeNodeItems.length - 1

    while (i >= 0) {
      const item = btreeNodeItems[i]
      if (item.sortValue <= sortValue) {
        foundItem = item
        break
      }
      i -= 1
    }

    if (
      btreeNodeItems[0].btreeRightChildNodeRightmostItemIndex == -1
      || btreeNodeItems[0].btreeLeftChildNodeRightmostItemIndex == -1
    ) { // leaf node
      return foundItem
    }


    i = btreeNodeItems.length - 1

    while (i >= 0 && btreeNodeItems[i].sortValue > sortValue) {
      i -= 1
    }
    i += 1 // if i === 0, sortValue is less than all items' sortValues in btreeNodeItems, insert item at very left
           // if i === btreeNodeItems.length, sortValue is greater than all items' sortValues in btreeNodeItems, insert item at very right
           // if i > 0, sortValue is less than btreeNodeItems[i].sortValue, insert item in middle

    if (i === 0) { // move to left child

      let btreeChildNodeItemsParentNodeItem
      let btreeChildNodeItemsLeftParentNodeItem
      let btreeChildNodeItemsRightParentNodeItem = btreeNodeItems[i]

      btreeChildNodeItemsParentNodeItem = btreeChildNodeItemsRightParentNodeItem

      const btreeChildNodeRightmostItem = await this._getBTreeItem(
        btreeChildNodeItemsParentNodeItem.btreeLeftChildNodeRightmostItemIndex
      )
      let btreeChildNodeItems = await this._getBTreeNodeItems(btreeChildNodeRightmostItem)

      return this._findBTreeNodeItemLTE(
        sortValue,
        btreeChildNodeItems,
        btreeRootNodeRightmostItem,
        btreeChildNodeItemsParentNodeItem,
        foundItem
      )
    }

    // move to right child

    let btreeChildNodeItemsParentNodeItem
    let btreeChildNodeItemsLeftParentNodeItem
    let btreeChildNodeItemsRightParentNodeItem

    if (i === btreeNodeItems.length) { // sortValue is greater than all items' sortValues in btreeNodeItems
      btreeChildNodeItemsLeftParentNodeItem = btreeNodeItems[i - 1]
      btreeChildNodeItemsRightParentNodeItem = undefined
    }
    else { // (i > 0) sortValue is less than btreeNodeItems[i].sortValue, insert item in middle
      btreeChildNodeItemsLeftParentNodeItem = btreeNodeItems[i - 1]
      btreeChildNodeItemsRightParentNodeItem = btreeNodeItems[i]
      // i === (btreeNodeItems.length - 1) ? undefined : btreeNodeItems[i + 1]
    }
    btreeChildNodeItemsParentNodeItem = btreeChildNodeItemsLeftParentNodeItem

    const btreeChildNodeRightmostItem = await this._getBTreeItem(
      btreeChildNodeItemsParentNodeItem.btreeRightChildNodeRightmostItemIndex
    )
    let btreeChildNodeItems = await this._getBTreeNodeItems(btreeChildNodeRightmostItem)

    return this._findBTreeNodeItemLTE(
      sortValue,
      btreeChildNodeItems,
      btreeRootNodeRightmostItem,
      btreeChildNodeItemsParentNodeItem,
      foundItem
    )
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
   *
   * forwardIterator(options {
   *   exclusiveStartKey: string,
   *   startingSortValue: string|number,
   *   endingSortValue: string|number,
   *   valueType: 'string'|'binary',
   * }) -> values AsyncGenerator<string|Buffer>
   * ```
   *
   * Returns an iterator of all items in the disk sorted hash table sorted by sort-value. Items are yielded in ascending order.
   *
   * If a starting sort-value and ending sort-value are provided, the iterator returns only items with sort-values between the starting and ending sort-values, including items with sort-values equal to the starting and ending sort-values. If only a starting sort-value is provided, the iterator returns all items with sort values greater than or equal to the starting sort-value. If only an ending sort-value is provided, the iterator returns all items with sort values less than or equal to the ending sort-value.
   *
   * If an exclusive start key is provided, the iterator returns items with sort-values greater than or equal to the sort value of the item at the exclusive start key, not including the item at the exclusive start key. The exclusive start key takes precedence over the starting sort-value.
   *
   * Arguments:
   *   * (none) - retrieves all items in the disk sorted hash table.
   *   * `options`
   *     * `exclusiveStartKey` - `string` - the key after which to start iterating.
   *     * `startingSortValue` - `string|number` - the sort value from which to start iterating.
   *     * `endingSortValue` - `string|number` - the sort value at which to stop iterating.
   *     * `valueType` - `'string'|'binary'` - the type of value that the iterator yields. Defaults to `'string'`.
   *       * `'string'` - iterator yields `string` values.
   *       * `'binary'` - iterator yields `Buffer` values.
   *
   * Return:
   *   * `values` - `AsyncGenerator<string|Buffer>` - an async iterator of the values of all items in the disk sorted hash table sorted by sort-value in ascending order.
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
   *
   * for await (const value of ht.forwardIterator({ startingSortValue: 2, endingSortValue: 3 })) {
   *   console.log(value) // value2
   *                      // value3
   * }
   *
   * for await (const value of ht.forwardIterator({ exclusiveStartKey: 'key1' })) {
   *   console.log(value) // value2
   *                      // value3
   * }
   *
   * for await (const value of ht.forwardIterator({ exclusiveStartKey: 'key1', endingSortValue: 2 })) {
   *   console.log(value) // value2
   * }
   * ```
   */
  async * forwardIterator(options = {}) {
    let {
      exclusiveStartKey,
      startingSortValue,
      endingSortValue,
      valueType = 'string',
    } = options

    let currentForwardItem
    if (exclusiveStartKey) {
      const exclusiveStartIndex = this._hash1(exclusiveStartKey)
      const exclusiveStartItem = await this._getHeadItem(exclusiveStartIndex)
      if (exclusiveStartItem == null || exclusiveStartItem.statusMarker == REMOVED) {
        currentForwardItem = undefined
      } else {
        currentForwardItem = await this._getItem(exclusiveStartItem.forwardIndex, valueType)
      }
    } else if (startingSortValue != null) {
      startingSortValue = convert(startingSortValue, this.sortValueType)

      const btreeRootNodeRightmostItem = await this._getBTreeRootNodeRightmostItem()
      const btreeRootNodeItems = await this._getBTreeNodeItems(btreeRootNodeRightmostItem)
      const foundBTreeItem = await this._findBTreeNodeItemGTE(
        startingSortValue, btreeRootNodeItems, btreeRootNodeRightmostItem
      )
      currentForwardItem = await this._getItem(foundBTreeItem.index, valueType)
    } else {
      currentForwardItem = await this._getForwardStartItem(valueType)
    }

    if (endingSortValue != null) {
      endingSortValue = convert(endingSortValue, this.sortValueType)

      while (currentForwardItem) {
        if (currentForwardItem.sortValue > endingSortValue) {
          break
        }
        yield currentForwardItem.value
        currentForwardItem = await this._getItem(currentForwardItem.forwardIndex, valueType)
      }
    } else {
      while (currentForwardItem) {
        yield currentForwardItem.value
        currentForwardItem = await this._getItem(currentForwardItem.forwardIndex, valueType)
      }
    }
  }

  /**
   * @name reverseIterator
   *
   * @docs
   * ```coffeescript [specscript]
   * reverseIterator() -> values AsyncGenerator<string|Buffer>
   *
   * reverseIterator(options {
   *   exclusiveStartKey: string,
   *   startingSortValue: string|number,
   *   endingSortValue: string|number,
   *   valueType: 'string'|'binary',
   * }) -> values AsyncGenerator<string|Buffer>
   * ```
   *
   * Returns an iterator of all items in the disk sorted hash table sorted by sort-value. Items are yielded in descending order.
   *
   * If a starting sort-value and ending sort-value are provided, the iterator returns only items with sort-values between the starting and ending sort-values, including items with sort-values equal to the starting and ending sort-values. If only a starting sort-value is provided, the iterator returns items with sort values less than or equal to the starting sort-value.
   *
   * If an exclusive start key is provided, the iterator returns items with sort-values less than or equal to the sort value of the item at the exclusive start key, not including the item at the exclusive start key. The exclusive start key takes precedence over the starting sort-value.
   *
   * Arguments:
   *   * (none) - retrieves all items in the disk sorted hash table.
   *   * `options`
   *     * `exclusiveStartKey` - `string` - the key after which to start iterating.
   *     * `startingSortValue` - `string|number` - the sort value from which to start iterating.
   *     * `endingSortValue` - `string|number` - the sort value at which to stop iterating.
   *     * `valueType` - `'string'|'binary'` - the type of value that the iterator yields. Defaults to `'string'`.
   *       * `string` - iterator yields `string` values.
   *       * `binary` - iterator yields `Buffer` values.
   *
   * Return:
   *   * `values` - `AsyncGenerator<string|Buffer>` - an async iterator of the values of all items in the disk sorted hash table sorted by sort-value in descending order.
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
   *
   * for await (const value of ht.reverseIterator({ startingSortValue: 2, endingSortValue: 1 })) {
   *   console.log(value) // value2
   *                      // value1
   * }
   *
   * for await (const value of ht.reverseIterator({ exclusiveStartKey: 'key3' })) {
   *   console.log(value) // value2
   *                      // value1
   * }
   *
   * for await (const value of ht.reverseIterator({ exclusiveStartKey: 'key3', endingSortValue: 2 })) {
   *   console.log(value) // value2
   * }
   * ```
   */
  async * reverseIterator(options = {}) {
    let {
      exclusiveStartKey,
      startingSortValue,
      endingSortValue,
      valueType = 'string',
    } = options

    let currentForwardItem
    if (exclusiveStartKey) {
      const exclusiveStartIndex = this._hash1(exclusiveStartKey)
      const exclusiveStartItem = await this._getHeadItem(exclusiveStartIndex)
      if (exclusiveStartItem == null || exclusiveStartItem.statusMarker == REMOVED) {
        currentForwardItem = undefined
      } else {
        currentForwardItem = await this._getItem(exclusiveStartItem.reverseIndex, valueType)
      }
    } else if (startingSortValue != null) {
      startingSortValue = convert(startingSortValue, this.sortValueType)

      const btreeRootNodeRightmostItem = await this._getBTreeRootNodeRightmostItem()
      const btreeRootNodeItems = await this._getBTreeNodeItems(btreeRootNodeRightmostItem)
      const foundBTreeItem = await this._findBTreeNodeItemLTE(
        startingSortValue, btreeRootNodeItems, btreeRootNodeRightmostItem
      )
      currentForwardItem = await this._getItem(foundBTreeItem.index, valueType)
    } else {
      currentForwardItem = await this._getReverseStartItem(valueType)
    }

    if (endingSortValue != null) {
      endingSortValue = convert(endingSortValue, this.sortValueType)

      while (currentForwardItem) {
        if (currentForwardItem.sortValue < endingSortValue) {
          break
        }
        yield currentForwardItem.value
        currentForwardItem = await this._getItem(currentForwardItem.reverseIndex, valueType)
      }
    } else {
      while (currentForwardItem) {
        yield currentForwardItem.value
        currentForwardItem = await this._getItem(currentForwardItem.reverseIndex, valueType)
      }
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

    let currentItem = await this._getKeyItem(index)
    while (currentItem) {
      if (key == currentItem.key) {
        break
      }

      index = (index + stepSize) % this._length
      if (index == startIndex) {
        return false // entire table searched
      }

      currentItem = await this._getKeyItem(index)
    }

    if (currentItem == null) {
      return false
    }

    if (currentItem.statusMarker == OCCUPIED) {
      const btreeRootNodeRightmostItem = await this._getBTreeRootNodeRightmostItem()
      const btreeRootNodeItems = await this._getBTreeNodeItems(btreeRootNodeRightmostItem)
      const btreeItem = await this._getBTreeItem(index)
      await this._deleteBTreeNodeItem(btreeItem, btreeRootNodeItems, btreeRootNodeItems)
    }

    if (currentItem.reverseIndex == -1) { // item to delete is first in the list
      if (currentItem.forwardIndex > -1) { // there is an item behind item to delete
        await this._updateReverseIndex(currentItem.forwardIndex, -1)
        await this._writeFirstIndex(currentItem.forwardIndex)
      } else { // item to remove is first and last in the list
        await this._writeFirstIndex(-1)
        await this._writeLastIndex(-1)
      }
    } else if (currentItem.forwardIndex == -1) { // item to delete is last in the list
      if (currentItem.reverseIndex > -1) { // there is an item ahead of item to delete
        await this._updateForwardIndex(currentItem.reverseIndex, -1)
        await this._writeLastIndex(currentItem.forwardIndex)
      } else { // item is first and last in the list (handled above)
      }
    } else { // item to delete is in the middle of the list
      await this._updateReverseIndex(currentItem.forwardIndex, currentItem.reverseIndex)
      await this._updateForwardIndex(currentItem.reverseIndex, currentItem.forwardIndex)
    }

    if (currentItem.statusMarker == OCCUPIED) {
      await this._setStatusMarker(index, REMOVED)
      await this._decrementCount()
      await this._incrementDeletedCount()
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
