/**
 * Presidium DB
 * https://github.com/richytong/presidium-db
 * (c) Richard Tong
 * Presidium DB may be freely distributed under the CFOSS license.
 */

const fs = require('fs')
const convert = require('./_internal/convert')

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
 *   storagePath: string,
 *   headerPath: string,
 *   initialLength: number,
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
 *     * `initialLength` - `number` - the initial length of the disk sorted hash table. Defaults to 1024.
 *     * `sortValueType` - `'string'|'number'` - the type of the disk sorted hash table sort-values.
 *     * `resizeRatio` - `number` - the ratio of number of items to table length at which to resize the disk sorted hash table. Minimum value 0 (no resize), maximum value 1. Defaults to 0.
 *     * `resizeFactor` - `number` - the factor that is multiplied with the disk sorted hash table's current length to determine the new table length on a resize.
 *     * `degree` - `number` - minimum value `2`, defaults to `2` - defines the following parameters for the internal b-tree that organizes all of the items in the disk sorted hash table:
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
 *   initialLength: 1024,
 *   storagePath: '/path/to/storage-file',
 *   headerPath: '/path/to/header-file',
 *   resizeRatio: 0.5,
 *   resizeFactor: 1000,
 *   degree: 2,
 * })
 * ```
 *
 * Limits:
 *   * 511 KiB for key, value, and sortValue.
 *
 * ## Resizing the disk sorted hash table
 * When an item is inserted into the disk sorted hash table via [set](/docs/DiskSortedHashTable#set), the current capacity ratio of the table is calculated as the table's count divided by the table's length. If the current capacity ratio exceeds the `resizeRatio` (and the `resizeRatio` is not 0), a resize of the table occurs.
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
 * The value for `degree` ultimately affects the height of the internal b-tree, which determines the speed of insert and update operations via [set](/docs/DiskSortedHashTable#set) on the disk sorted hash table. A higher value for `degree` results in a shorter b-tree and more items per b-tree node, while a lower value results in a taller b-tree and fewer items per b-tree node. The default value of `2` is a safe choice for most use cases.
 */
class DiskSortedHashTable {
  constructor(options) {
    this.storagePath = options.storagePath
    this.headerPath = options.headerPath
    this.initialLength = options.initialLength ?? 1024
    this.sortValueType = options.sortValueType
    this._length = null
    this._count = null
    this.storageFd = null
    this.headerFd = null
    this.resizeRatio = options.resizeRatio ?? 0
    this.resizeFactor = options.resizeFactor ?? 4
    this.degree = options.degree ?? 2
  }

  // _initializeHeader() -> headerReadBuffer Promise<Buffer>
  async _initializeHeader() {
    const headerReadBuffer = Buffer.alloc(20)
    headerReadBuffer.writeUInt32BE(this.initialLength, 0)
    headerReadBuffer.writeUInt32BE(0, 4)
    headerReadBuffer.writeInt32BE(-1, 8)
    headerReadBuffer.writeInt32BE(-1, 12)
    headerReadBuffer.writeInt32BE(-1, 16)

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
  // 4 bytes for table length
  // 4 bytes for item count
  // 4 bytes for first item index
  // 4 bytes for last item index
  // 4 bytes for btree root rightmost item index

  // _readHeader() -> headerReadBuffer Promise<Buffer>
  async _readHeader() {
    const headerReadBuffer = Buffer.alloc(20)

    await this.headerFd.read({
      buffer: headerReadBuffer,
      offset: 0,
      position: 0,
      length: 20,
    })

    return headerReadBuffer
  }

  // _getHeader() -> Promise<>
  async _getHeader() {
    const headerReadBuffer = await this._readHeader()

    const length = headerReadBuffer.readUInt32BE(0)
    const count = headerReadBuffer.readUInt32BE(4)
    const firstIndex = headerReadBuffer.readInt32BE(8)
    const lastIndex = headerReadBuffer.readInt32BE(12)
    const btreeRootNodeRightmostItemIndex = headerReadBuffer.readInt32BE(16)

    return {
      length,
      count,
      firstIndex,
      lastIndex,
      btreeRootNodeRightmostItemIndex,
    }
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

    const btreeLeftSiblingItemIndex = readBuffer.readInt32BE(29)

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

  // _writeBTreeRootRightmostItemIndex(index number) -> Promise<>
  async _writeBTreeRootRightmostItemIndex(index) {
    const position = 16
    const buffer = Buffer.alloc(4)
    buffer.writeInt32BE(index, 0)

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
    const position = (btreeNodeItemIndex * DATA_SLICE_SIZE) + 21
    const buffer = Buffer.alloc(4)
    buffer.writeInt32BE(btreeLeftChildNodeRightmostItemIndex, 0)

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
    const position = (btreeNodeItemIndex * DATA_SLICE_SIZE) + 25
    const buffer = Buffer.alloc(4)
    buffer.writeInt32BE(btreeRightChildNodeRightmostItemIndex, 0)

    await this.storageFd.write(buffer, {
      offset: 0,
      position,
      length: buffer.length,
    })
  }

  // _writeBTreeLeftSiblingItemIndex(
  //   btreeNodeItemIndex number,
  //   btreeLeftSiblingItemIndex number
  // ) -> Promise<>
  async _writeBTreeLeftSiblingItemIndex(
    btreeNodeItemIndex, btreeLeftSiblingItemIndex
  ) {
    const position = (btreeNodeItemIndex * DATA_SLICE_SIZE) + 29
    const buffer = Buffer.alloc(4)
    buffer.writeInt32BE(btreeLeftSiblingItemIndex, 0)

    await this.storageFd.write(buffer, {
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
    const keyBuffer = readBuffer.subarray(33, keyByteLength + 33)
    return keyBuffer.toString(ENCODING)
  }

  // _getBTreeRootNodeRightmostItem() -> btreeRootNodeRightmostItem Promise<{
  //   index: number,
  //   sortValue: string|number,
  //   btreeLeftChildNodeRightmostItemIndex: number,
  //   btreeRightChildNodeRightmostItemIndex: number,
  //   btreeLeftSiblingItemIndex: number,
  // }>
  async _getBTreeRootNodeRightmostItem() {
    const headerReadBuffer = await this._readHeader()
    const index = headerReadBuffer.readInt32BE(16)
    if (index == -1) {
      return undefined
    }
    const readBuffer = await this._read(index)
    return this._parseBTreeItem(readBuffer, index)
  }

  // _getBTreeItem(index number) -> btreeItem Promise<{
  //   index: number,
  //   sortValue: string|number,
  //   btreeLeftChildNodeRightmostItemIndex: number,
  //   btreeRightChildNodeRightmostItemIndex: number,
  //   btreeLeftSiblingItemIndex: number,
  // }>
  async _getBTreeItem(index) {
    if (index == -1) {
      return undefined
    }
    const readBuffer = await this._read(index)
    return this._parseBTreeItem(readBuffer, index)
  }

  // _getBTreeNodeItems(btreeRightmostItem { index: number, btreeLeftSiblingItemIndex: number }) -> Promise<number>
  async _getBTreeNodeItems(btreeRightmostItem) {
    if (btreeRightmostItem == null) {
      return []
    }
    const btreeNodeItems = [btreeRightmostItem]
    let currentBTreeItem = btreeRightmostItem

    while (currentBTreeItem.btreeLeftSiblingItemIndex > -1) {
      currentBTreeItem = await this._getBTreeItem(currentBTreeItem.btreeLeftSiblingItemIndex)
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
  //     btreeLeftSiblingItemIndex: number,
  //   }>,
  //   btreeLeftParentNodeItem {
  //     index: number,
  //     sortValue: string|number,
  //     btreeLeftChildNodeRightmostItemIndex: number,
  //     btreeRightChildNodeRightmostItemIndex: number,
  //     btreeLeftSiblingItemIndex: number,
  //   },
  //   btreeRightParentNodeItem {
  //     index: number,
  //     sortValue: string|number,
  //     btreeLeftChildNodeRightmostItemIndex: number,
  //     btreeRightChildNodeRightmostItemIndex: number,
  //     btreeLeftSiblingItemIndex: number,
  //   },
  //   btreeRootNodeRightmostItem {
  //     index: number,
  //     sortValue: string|number,
  //     btreeLeftChildNodeRightmostItemIndex: number,
  //     btreeRightChildNodeRightmostItemIndex: number,
  //     btreeLeftSiblingItemIndex: number,
  //   }
  // ) -> Promise<>
  async _splitBTreeChildNodeRight(
    btreeChildNodeItems,
    btreeLeftParentNodeItem,
    btreeRightParentNodeItem,
    btreeRootNodeRightmostItem,
    { sortValue }
  ) {

    const _sv = async index => {
      if (index == null) {
        return undefined
      }
      if (index == -1) {
        return undefined
      }
      const item = await this._getBTreeItem(index)
      return item.sortValue
    }

    const _hy = async item => {
      if (item == null) {
        return
      }
      if (item.btreeLeftChildNodeRightmostItemIndex > -1) {
        item.btreeLeftChildNodeRightmostItemSortValue = await _sv(item.btreeLeftChildNodeRightmostItemIndex)
      }
      if (item.btreeRightChildNodeRightmostItemIndex > -1) {
        item.btreeRightChildNodeRightmostItemSortValue = await _sv(item.btreeRightChildNodeRightmostItemIndex)
      }
      return item
    }

    // Maximum number of items per b-tree node: `(2 * degree) - 1`
    // Maximum number of children per b-tree node: `2 * degree`

    const btreeLeftChildNodeItems = btreeChildNodeItems.slice(0, this.degree - 1)
    const btreeMiddleItem = btreeChildNodeItems[this.degree - 1]
    const btreeRightChildNodeItems = btreeChildNodeItems.slice(this.degree)

    // middle item left sibling -> left parent node item
    await this._writeBTreeLeftSiblingItemIndex(
      btreeMiddleItem.index,
      btreeLeftParentNodeItem.index
    )
    btreeMiddleItem.btreeLeftSiblingItemIndex = btreeLeftParentNodeItem.index

    if (btreeRightParentNodeItem) {
      // right parent node item left sibling -> middle item
      await this._writeBTreeLeftSiblingItemIndex(
        btreeRightParentNodeItem.index,
        btreeMiddleItem.index
      )
      btreeRightParentNodeItem.btreeLeftSiblingItemIndex = btreeMiddleItem.index
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

    // first item of right child node items left sibling -> -1
    await this._writeBTreeLeftSiblingItemIndex(btreeRightChildNodeItems[0].index, -1)
    btreeRightChildNodeItems[0].btreeLeftSiblingItemIndex = -1

    return [btreeLeftChildNodeItems, btreeMiddleItem, btreeRightChildNodeItems]
  }

  // _splitBTreeChildNodeLeft(btreeChildNodeItems Array<{
  //   index: number,
  //   sortValue: string|number,
  //   btreeLeftChildNodeRightmostItemIndex: number,
  //   btreeRightChildNodeRightmostItemIndex: number,
  //   btreeLeftSiblingItemIndex: number,
  // }>, btreeRightParentNodeItem {
  //   index: number,
  //   sortValue: string|number,
  //   btreeLeftChildNodeRightmostItemIndex: number,
  //   btreeRightChildNodeRightmostItemIndex: number,
  //   btreeLeftSiblingItemIndex: number,
  // }) -> Promise<>
  async _splitBTreeChildNodeLeft(
    btreeChildNodeItems, btreeRightParentNodeItem, { sortValue }
  ) {

    const _sv = async index => {
      if (index == null) {
        return undefined
      }
      if (index == -1) {
        return undefined
      }
      const item = await this._getBTreeItem(index)
      return item.sortValue
    }

    const _hy = async item => {
      if (item == null) {
        return
      }
      if (item.btreeLeftChildNodeRightmostItemIndex > -1) {
        item.btreeLeftChildNodeRightmostItemSortValue = await _sv(item.btreeLeftChildNodeRightmostItemIndex)
      }
      if (item.btreeRightChildNodeRightmostItemIndex > -1) {
        item.btreeRightChildNodeRightmostItemSortValue = await _sv(item.btreeRightChildNodeRightmostItemIndex)
      }
      return item
    }

    const btreeLeftChildNodeItems = btreeChildNodeItems.slice(0, this.degree - 1)
    const btreeMiddleItem = btreeChildNodeItems[this.degree - 1]
    const btreeRightChildNodeItems = btreeChildNodeItems.slice(this.degree)

    // node = [item item item]

    await this._writeBTreeLeftSiblingItemIndex(
      btreeRightParentNodeItem.index,
      btreeMiddleItem.index
    )
    btreeRightParentNodeItem.btreeLeftSiblingItemIndex = btreeMiddleItem.index

    await this._writeBTreeLeftSiblingItemIndex(btreeMiddleItem.index, -1)
    btreeMiddleItem.btreeLeftSiblingItemIndex = -1

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

    await this._writeBTreeLeftSiblingItemIndex(btreeRightChildNodeItems[0].index, -1)
    btreeRightChildNodeItems[0].btreeLeftSiblingItemIndex = -1

    return [btreeLeftChildNodeItems, btreeMiddleItem, btreeRightChildNodeItems]
  }

  // _splitBTreeRootNode(btreeRootNodeItems Array<{
  //   index: number,
  //   sortValue: string|number,
  //   btreeLeftChildNodeRightmostItemIndex: number,
  //   btreeRightChildNodeRightmostItemIndex: number,
  //   btreeLeftSiblingItemIndex: number,
  // }>) -> Promise<>
  async _splitBTreeRootNode(btreeRootNodeItems, { sortValue }) {
    const btreeLeftChildNodeItems = btreeRootNodeItems.slice(0, this.degree - 1)
    const btreeMiddleItem = btreeRootNodeItems[this.degree - 1]
    const btreeRightChildNodeItems = btreeRootNodeItems.slice(this.degree)

    // update root rightmost item
    await this._writeBTreeRootRightmostItemIndex(btreeMiddleItem.index) // new root

    // new root has no left sibling
    await this._writeBTreeLeftSiblingItemIndex(btreeMiddleItem.index, -1)
    btreeMiddleItem.btreeLeftSiblingItemIndex = -1

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

    // remove left sibling of first right child node item
    await this._writeBTreeLeftSiblingItemIndex(btreeRightChildNodeItems[0].index, -1)
    btreeRightChildNodeItems[0].btreeLeftSiblingItemIndex = -1

    return btreeMiddleItem
  }

  // _insertBTreeLeafNodeItem(index: number, btreeRightSiblingItemIndex: number) -> Promise<>
  async _insertBTreeLeafNodeItem(index, btreeRightSiblingItemIndex) {
    if (btreeRightSiblingItemIndex > -1) {
      await this._writeBTreeLeftSiblingItemIndex(btreeRightSiblingItemIndex, index)
    }
  }

  // _insertBTreeNodeItem(
  //   index number,
  //   sortValue string|number,
  //   btreeNodeItems Array<{
  //     index: number,
  //     sortValue: string|number,
  //     btreeLeftChildNodeRightmostItemIndex: number,
  //     btreeRightChildNodeRightmostItemIndex: number,
  //     btreeLeftSiblingItemIndex: number,
  //   }>,
  //   btreeRootNodeRightmostItem: {
  //     index: number,
  //     sortValue: string|number,
  //     btreeLeftChildNodeRightmostItemIndex: number,
  //     btreeRightChildNodeRightmostItemIndex: number,
  //     btreeLeftSiblingItemIndex: number,
  //   },
  //   btreeNodeItemsParentNodeItem {
  //     index: number,
  //     sortValue: string|number,
  //     btreeLeftChildNodeRightmostItemIndex: number,
  //     btreeRightChildNodeRightmostItemIndex: number,
  //     btreeLeftSiblingItemIndex: number,
  //   },
  //   btreeNodeItemsParentLeftNodeItem {
  //     index: number,
  //     sortValue: string|number,
  //     btreeLeftChildNodeRightmostItemIndex: number,
  //     btreeRightChildNodeRightmostItemIndex: number,
  //     btreeLeftSiblingItemIndex: number,
  //   }
  //   btreeNodeItemsParentRightNodeItem {
  //     index: number,
  //     sortValue: string|number,
  //     btreeLeftChildNodeRightmostItemIndex: number,
  //     btreeRightChildNodeRightmostItemIndex: number,
  //     btreeLeftSiblingItemIndex: number,
  //   }
  // )
  async _insertBTreeNodeItem(
    index,
    sortValue,
    btreeNodeItems,
    btreeRootNodeRightmostItem,
    btreeNodeItemsParentNodeItem,
    btreeNodeItemsParentLeftNodeItem,
    btreeNodeItemsParentRightNodeItem
  ) {

    const _sv = async index => {
      if (index == null) {
        return undefined
      }
      if (index == -1) {
        return undefined
      }
      const item = await this._getBTreeItem(index)
      return item.sortValue
    }

    const _hy = async item => {
      if (item == null) {
        return
      }
      if (item.btreeLeftChildNodeRightmostItemIndex > -1) {
        item.btreeLeftChildNodeRightmostItemSortValue = await _sv(item.btreeLeftChildNodeRightmostItemIndex)
      }
      if (item.btreeRightChildNodeRightmostItemIndex > -1) {
        item.btreeRightChildNodeRightmostItemSortValue = await _sv(item.btreeRightChildNodeRightmostItemIndex)
      }
      return item
    }


    if (btreeNodeItemsParentNodeItem && btreeNodeItemsParentNodeItem.btreeParentNodeItem?.btreeParentNodeItem?.index == btreeNodeItemsParentNodeItem.index) {
      throw new Error('circular parent')
    }

    let i = btreeNodeItems.length - 1

    while (i >= 0 && convert(btreeNodeItems[i].sortValue, this.sortValueType) > sortValue) {
      i -= 1
    }
    i += 1 // if i === 0, sortValue is less than all items' sortValues in btreeNodeItems, insert item at very left
           // if i === btreeNodeItems.length, sortValue is greater than all items' sortValues in btreeNodeItems, insert item at very right
           // if i > 0, sortValue is less than btreeNodeItems[i].sortValue, insert item in middle

    if (
      btreeNodeItems[0].btreeRightChildNodeRightmostItemIndex == -1
      || btreeNodeItems[0].btreeLeftChildNodeRightmostItemIndex == -1
    ) { // leaf node

      let btreeRightSiblingItemIndex
      let btreeLeftSiblingItemIndex
      if (i === 0) {
        btreeLeftSiblingItemIndex = -1
        btreeRightSiblingItemIndex = btreeNodeItems[i].index
      } else if (i === btreeNodeItems.length) {
        btreeLeftSiblingItemIndex = btreeNodeItems[i - 1].index
        btreeRightSiblingItemIndex = -1
      } else {
        btreeLeftSiblingItemIndex = btreeNodeItems[i - 1].index
        btreeRightSiblingItemIndex = btreeNodeItems[i].index
      }

      // insert item into leaf node

      if (btreeRightSiblingItemIndex > -1) {
        // point right sibling to item
        await this._writeBTreeLeftSiblingItemIndex(btreeRightSiblingItemIndex, index)
      }

      if (btreeNodeItemsParentNodeItem == null) {
        // skip
      }
      else if (btreeNodeItemsParentNodeItem.isLeftChildPointer) { // parent item sortValue was greater than item's sortValue

        // left sibling was the rightmost item of the parent node item's left child node
        if (
          btreeLeftSiblingItemIndex > -1
          && btreeLeftSiblingItemIndex === btreeNodeItemsParentNodeItem.btreeLeftChildNodeRightmostItemIndex
        ) {
          await this._writeBTreeLeftChildNodeRightmostItemIndex(
            btreeNodeItemsParentNodeItem.index, index
          )
        }

        // parent item had a left sibling pointing right to the current rightmost item of the leaf node
        if (
          btreeLeftSiblingItemIndex > -1
          && btreeLeftSiblingItemIndex === btreeNodeItemsParentNodeItem.btreeLeftSiblingNodeItem?.btreeRightChildNodeRightmostItemIndex
        ) {
          await this._writeBTreeRightChildNodeRightmostItemIndex(
            btreeNodeItemsParentNodeItem.btreeLeftSiblingNodeItem.index, index
          )
        }

      }
      else if (btreeNodeItemsParentNodeItem.isRightChildPointer) {

        // left sibling was the rightmost item of the parent node item's right child node
        if (
          btreeLeftSiblingItemIndex > -1
          && btreeLeftSiblingItemIndex === btreeNodeItemsParentNodeItem.btreeRightChildNodeRightmostItemIndex
        ) {
          await this._writeBTreeRightChildNodeRightmostItemIndex(
            btreeNodeItemsParentNodeItem.index, index
          )
        }

        // parent item had a right sibling pointing left to the current rightmost item of the leaf node
        if (
          btreeLeftSiblingItemIndex > -1
          && btreeLeftSiblingItemIndex === btreeNodeItemsParentNodeItem.btreeRightSiblingNodeItem?.btreeLeftChildNodeRightmostItemIndex
        ) {
          await this._writeBTreeLeftChildNodeRightmostItemIndex(
            btreeNodeItemsParentNodeItem.btreeRightSiblingNodeItem.index, index
          )
        }

      }
      else {
        throw new Error('parent node item isLeftChildPointer or isRightChildPointer unset')
      }


      let predecessor = null
      let successor = null

      if (i === 0) { // sortValue is less than all items' sortValues in btreeNodeItems
        let btreeParentNodeItem = btreeNodeItemsParentNodeItem
        while (btreeParentNodeItem) {
          if (btreeParentNodeItem.isRightChildPointer) {
            predecessor = btreeParentNodeItem
            break
          }
          // btreeParentNodeItem = btreeParentNodeItem.btreeParentNodeItem
          btreeParentNodeItem = btreeParentNodeItem.btreeLeftSiblingNodeItem
            ?? btreeParentNodeItem.btreeParentNodeItem
        }
      } else {
        predecessor = btreeNodeItems[i - 1]
      }

      if (i === btreeNodeItems.length) { // sortValue is greater than all items' sortValues in btreeNodeItems
        let btreeParentNodeItem = btreeNodeItemsParentNodeItem
        while (btreeParentNodeItem) {
          if (btreeParentNodeItem.isLeftChildPointer) {
            successor = btreeParentNodeItem
            break
          }
          btreeParentNodeItem = btreeParentNodeItem.btreeRightSiblingNodeItem
            ?? btreeParentNodeItem.btreeParentNodeItem
        }
      } else {
        successor = btreeNodeItems[i]
      }

      return { predecessor, successor, btreeLeftSiblingItemIndex }
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
        const [btreeLeftNodeItems, btreeNewParentNodeItem, btreeRightNodeItems] = await this._splitBTreeChildNodeLeft(
          btreeChildNodeItems,
          btreeChildNodeItemsRightParentNodeItem,
          { sortValue }
        )

        await this._writeBTreeLeftChildNodeRightmostItemIndex(
          btreeChildNodeItemsRightParentNodeItem.index,
          btreeRightNodeItems[btreeRightNodeItems.length - 1].index
        )

        delete btreeChildNodeItemsParentNodeItem.isLeftChildPointer
        delete btreeChildNodeItemsParentNodeItem.btreeParentNodeItem

        btreeChildNodeItemsParentNodeItem = btreeNewParentNodeItem
        btreeChildNodeItemsParentNodeItem.btreeParentNodeItem = btreeNodeItemsParentNodeItem

        if (convert(sortValue, this.sortValueType) > btreeNewParentNodeItem.sortValue) { // move to right child
          btreeChildNodeItems = btreeRightNodeItems
          btreeChildNodeItemsParentNodeItem.isRightChildPointer = true
          btreeChildNodeItemsLeftParentNodeItem = btreeChildNodeItemsParentNodeItem
        } else { // move to left child
          btreeChildNodeItems = btreeLeftNodeItems
          btreeChildNodeItemsParentNodeItem.isLeftChildPointer = true
          btreeChildNodeItemsRightParentNodeItem = btreeChildNodeItemsParentNodeItem
        }

      }


      btreeChildNodeItemsParentNodeItem.btreeParentNodeItem = btreeNodeItemsParentNodeItem

      if (btreeChildNodeItemsRightParentNodeItem?.btreeLeftSiblingItemIndex == btreeChildNodeItemsParentNodeItem.index) {
        btreeChildNodeItemsParentNodeItem.btreeRightSiblingNodeItem = btreeChildNodeItemsRightParentNodeItem
      }
      if (btreeChildNodeItemsParentNodeItem.btreeRightSiblingNodeItem) {
        btreeChildNodeItemsParentNodeItem.btreeRightSiblingNodeItem.isLeftChildPointer = true
      }
      if (btreeChildNodeItemsParentNodeItem.btreeLeftSiblingItemIndex == btreeChildNodeItemsLeftParentNodeItem?.index) {
        btreeChildNodeItemsParentNodeItem.btreeLeftSiblingNodeItem = btreeChildNodeItemsLeftParentNodeItem
      }
      if (btreeChildNodeItemsParentNodeItem.btreeLeftSiblingNodeItem) {
        btreeChildNodeItemsParentNodeItem.btreeLeftSiblingNodeItem.isRightChildPointer = true
      }

      return this._insertBTreeNodeItem(
        index,
        sortValue,
        btreeChildNodeItems,
        btreeRootNodeRightmostItem,
        btreeChildNodeItemsParentNodeItem,
        btreeChildNodeItemsLeftParentNodeItem,
        btreeChildNodeItemsRightParentNodeItem
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
        btreeRootNodeRightmostItem,
        { sortValue }
      )

      if (i === btreeNodeItems.length) { // update parent nodes with new child rightmost item
        if (btreeNodeItemsParentNodeItem == null) {
          // skip
        }
        else if (btreeNodeItemsParentNodeItem.isLeftChildPointer) {
          // update parent left child node rightmost item
          await this._writeBTreeLeftChildNodeRightmostItemIndex(
            btreeNodeItemsParentNodeItem.index, btreeNewParentNodeItem.index
          )

          if (btreeNodeItemsParentNodeItem.btreeLeftSiblingNodeItem) {
            // update parent left sibling right child node rightmost item
            await this._writeBTreeRightChildNodeRightmostItemIndex(
              btreeNodeItemsParentNodeItem.btreeLeftSiblingNodeItem.index,
              btreeNewParentNodeItem.index
            )
          }
        }
        else if (btreeNodeItemsParentNodeItem.isRightChildPointer) {
          // update parent right child node rightmost item
          await this._writeBTreeRightChildNodeRightmostItemIndex(
            btreeNodeItemsParentNodeItem.index, btreeNewParentNodeItem.index
          )

          if (btreeNodeItemsParentNodeItem.btreeRightSiblingNodeItem) {
            // update parent right sibling right child node rightmost item
            await this._writeBTreeLeftChildNodeRightmostItemIndex(
              btreeNodeItemsParentNodeItem.btreeRightSiblingNodeItem.index,
              btreeNewParentNodeItem.index
            )
          }
        }
        else {
          throw new Error('parent node item isLeftChildPointer or isRightChildPointer unset')
        }
      }

      await this._writeBTreeRightChildNodeRightmostItemIndex(
        btreeChildNodeItemsLeftParentNodeItem.index,
        btreeLeftNodeItems[btreeLeftNodeItems.length - 1].index
      )

      if (btreeNewParentNodeItem.btreeLeftSiblingItemIndex == btreeNodeItemsParentNodeItem?.btreeRightChildNodeRightmostItemIndex) {
        await this._writeBTreeRightChildNodeRightmostItemIndex(
          btreeNodeItemsParentNodeItem.index, btreeNewParentNodeItem.index
        )
        btreeNodeItemsParentNodeItem.btreeRightChildNodeRightmostItemIndex = btreeNewParentNodeItem.index
      }
      else if (btreeNewParentNodeItem.btreeLeftSiblingItemIndex == btreeNodeItemsParentNodeItem?.btreeLeftChildNodeRightmostItemIndex) {
        await this._writeBTreeLeftChildNodeRightmostItemIndex(
          btreeNodeItemsParentNodeItem.index, btreeNewParentNodeItem.index
        )
        btreeNodeItemsParentNodeItem.btreeLeftChildNodeRightmostItemIndex = btreeNewParentNodeItem.index
      }

      delete btreeChildNodeItemsParentNodeItem.isRightChildPointer
      delete btreeChildNodeItemsParentNodeItem.btreeParentNodeItem

      btreeChildNodeItemsParentNodeItem = btreeNewParentNodeItem
      btreeChildNodeItemsParentNodeItem.btreeParentNodeItem = btreeNodeItemsParentNodeItem

      // (old right) (new parent) (old left)                                   (new parent) (old left)
      // (old right) (new left)  (old left)                                    (new left)  (old left)
      // (old right) (new right) (old left)                                    (new right) (old left)

      if (convert(sortValue, this.sortValueType) > btreeNewParentNodeItem.sortValue) { // move to right child
        btreeChildNodeItems = btreeRightNodeItems
        btreeChildNodeItemsParentNodeItem.isRightChildPointer = true
        btreeChildNodeItemsLeftParentNodeItem = btreeChildNodeItemsParentNodeItem
      } else { // move to left child
        btreeChildNodeItems = btreeLeftNodeItems
        btreeChildNodeItemsParentNodeItem.isLeftChildPointer = true
        btreeChildNodeItemsRightParentNodeItem = btreeChildNodeItemsParentNodeItem
      }

    }

    btreeChildNodeItemsParentNodeItem.btreeParentNodeItem = btreeNodeItemsParentNodeItem

    if (btreeChildNodeItemsRightParentNodeItem?.btreeLeftSiblingItemIndex == btreeChildNodeItemsParentNodeItem.index) {
      btreeChildNodeItemsParentNodeItem.btreeRightSiblingNodeItem = btreeChildNodeItemsRightParentNodeItem
    }
    if (btreeChildNodeItemsParentNodeItem.btreeRightSiblingNodeItem) {
      btreeChildNodeItemsParentNodeItem.btreeRightSiblingNodeItem.isLeftChildPointer = true
    }
    if (btreeChildNodeItemsParentNodeItem.btreeLeftSiblingItemIndex == btreeChildNodeItemsLeftParentNodeItem?.index) {
      btreeChildNodeItemsParentNodeItem.btreeLeftSiblingNodeItem = btreeChildNodeItemsLeftParentNodeItem
    }
    if (btreeChildNodeItemsParentNodeItem.btreeLeftSiblingNodeItem) {
      btreeChildNodeItemsParentNodeItem.btreeLeftSiblingNodeItem.isRightChildPointer = true
    }

    return this._insertBTreeNodeItem(
      index,
      sortValue,
      btreeChildNodeItems,
      btreeRootNodeRightmostItem,
      btreeChildNodeItemsParentNodeItem,
      btreeChildNodeItemsLeftParentNodeItem,
      btreeChildNodeItemsRightParentNodeItem
    )
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

  // _parseBTreeItem(readBuffer Buffer, index number) -> {
  //   statusMarker: 0|1|2,
  //   index: number,
  //   sortValue: string|number,
  //   btreeLeftChildNodeRightmostItemIndex: number,
  //   btreeRightChildNodeRightmostItemIndex: number,
  //   btreeLeftSiblingItemIndex: number,
  // }
  _parseBTreeItem(readBuffer, index) {
    const item = {}
    item.index = index

    const statusMarker = readBuffer.readUInt8(0)
    item.statusMarker = statusMarker

    const forwardIndex = readBuffer.readInt32BE(13)
    const reverseIndex = readBuffer.readInt32BE(17)
    item.forwardIndex = forwardIndex
    item.reverseIndex = reverseIndex

    const btreeLeftChildNodeRightmostItemIndex = readBuffer.readInt32BE(21)
    const btreeRightChildNodeRightmostItemIndex = readBuffer.readInt32BE(25)
    const btreeLeftSiblingItemIndex = readBuffer.readInt32BE(29)

    item.btreeLeftChildNodeRightmostItemIndex = btreeLeftChildNodeRightmostItemIndex
    item.btreeRightChildNodeRightmostItemIndex = btreeRightChildNodeRightmostItemIndex
    item.btreeLeftSiblingItemIndex = btreeLeftSiblingItemIndex

    const keyByteLength = readBuffer.readUInt32BE(1)
    const keyBuffer = readBuffer.subarray(33, keyByteLength + 33)
    const key = keyBuffer.toString(ENCODING)
    item.key = key

    const sortValueByteLength = readBuffer.readUInt32BE(5)
    const sortValueBuffer = readBuffer.subarray(
      33 + keyByteLength,
      33 + keyByteLength + sortValueByteLength
    )
    const sortValue = sortValueBuffer.toString(ENCODING)
    item.sortValue = sortValue

    const valueByteLength = readBuffer.readUInt32BE(9)
    const valueBuffer = readBuffer.subarray(
      33 + keyByteLength + sortValueByteLength,
      33 + keyByteLength + sortValueByteLength + valueByteLength
    )
    const value = valueBuffer.toString(ENCODING)
    item.value = value

    return item
  }

  // _parseItem(readBuffer Buffer, index number) -> {
  //   index: number,
  //   readBuffer: Buffer,
  //   sortValue: string|number,
  //   key: string,
  //   value: string,
  // }
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
    const keyBuffer = readBuffer.subarray(33, keyByteLength + 33)
    const key = keyBuffer.toString(ENCODING)
    item.key = key

    const sortValueByteLength = readBuffer.readUInt32BE(5)
    const sortValueBuffer = readBuffer.subarray(
      33 + keyByteLength,
      33 + keyByteLength + sortValueByteLength
    )
    const sortValue = sortValueBuffer.toString(ENCODING)
    item.sortValue = sortValue

    const valueByteLength = readBuffer.readUInt32BE(9)
    const valueBuffer = readBuffer.subarray(
      33 + keyByteLength + sortValueByteLength,
      33 + keyByteLength + sortValueByteLength + valueByteLength
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

  // _constructBTree(options { unique: boolean, onNode: function, onLeaf: function })
  // _constructBTree(options { unique: boolean, onNode: function, onLeaf: function }, btreeNode object, memo object) -> Promise<>
  async _constructBTree(options, btreeNode = {}, memo = {}) {
    const { unique = false, onNode, onLeaf } = options

    if (onLeaf && !unique) {
      throw new Error('onLeaf option requires unique to be true')
    }

    memo.totalItemsCount ??= 0
    memo.keys ??= []
    memo.height ??= 1

    let { height, isLeaf } = memo

    if (btreeNode.items == null) {
      const btreeRootNodeRightmostItem = await this._getBTreeRootNodeRightmostItem()
      const btreeRootNodeItems = btreeRootNodeRightmostItem == null
        ? []
        : await this._getBTreeNodeItems(btreeRootNodeRightmostItem)

      isLeaf = btreeRootNodeItems.length === 0 ? false : (
        btreeRootNodeItems.every(item => item.btreeLeftChildNodeRightmostItemIndex == -1)
        && btreeRootNodeItems.every(item => item.btreeRightChildNodeRightmostItemIndex == -1)
      )

      btreeNode.items = btreeRootNodeItems
      btreeNode.root = btreeRootNodeRightmostItem != null
      memo.totalItemsCount += btreeRootNodeItems.length
      memo.keys.push(...btreeRootNodeItems.map(item => item.sortValue))
    }

    if ((btreeNode.root || height > 1) && onNode) {
      onNode({ height, node: btreeNode })
    }

    if (isLeaf && onLeaf) {
      onLeaf({ height, node: btreeNode })
    }

    let i = 0
    for (const item of btreeNode.items) {
      btreeNode[item.sortValue] = {}

      let leftConditional
      if (unique) {
        leftConditional = i === 0 && item.btreeLeftChildNodeRightmostItemIndex > -1
      } else {
        leftConditional = item.btreeLeftChildNodeRightmostItemIndex > -1
      }

      const rightConditional = item.btreeRightChildNodeRightmostItemIndex > -1

      if (leftConditional) {
        const btreeLeftChildNodeRightmostItem = await this._getBTreeItem(item.btreeLeftChildNodeRightmostItemIndex)
        const btreeLeftChildNodeItems = await this._getBTreeNodeItems(btreeLeftChildNodeRightmostItem)

        btreeNode[item.sortValue].leftChild = { items: btreeLeftChildNodeItems }
        btreeNode[item.sortValue].leftChild.keys = btreeLeftChildNodeItems.map(item => item.sortValue)
        memo.totalItemsCount += btreeLeftChildNodeItems.length
        memo.keys.push(...btreeLeftChildNodeItems.map(item => item.sortValue))

        if (btreeNode[item.sortValue].leftChild.items.length === 0) {
          throw new Error('leftChild has no items')
        }

        await this._constructBTree(options, btreeNode[item.sortValue].leftChild, {
          ...memo,
          height: height + 1,
          isLeaf: (
            btreeNode[item.sortValue].leftChild.items.every(item => item.btreeLeftChildNodeRightmostItemIndex == -1)
            && btreeNode[item.sortValue].leftChild.items.every(item => item.btreeRightChildNodeRightmostItemIndex == -1)
          ),
        })
      }

      if (rightConditional) {
        const btreeRightChildNodeRightmostItem = await this._getBTreeItem(item.btreeRightChildNodeRightmostItemIndex)
        const btreeRightChildNodeItems = await this._getBTreeNodeItems(btreeRightChildNodeRightmostItem)

        btreeNode[item.sortValue].rightChild = { items: btreeRightChildNodeItems }
        btreeNode[item.sortValue].rightChild.keys = btreeRightChildNodeItems.map(item => item.sortValue)
        memo.totalItemsCount += btreeRightChildNodeItems.length
        memo.keys.push(...btreeRightChildNodeItems.map(item => item.sortValue))

        if (btreeNode[item.sortValue].rightChild.items.length === 0) {
          throw new Error('rightChild has no items')
        }

        await this._constructBTree(options, btreeNode[item.sortValue].rightChild, {
          ...memo,
          height: height + 1,
          isLeaf: (
            btreeNode[item.sortValue].rightChild.items.every(item => item.btreeLeftChildNodeRightmostItemIndex == -1)
            && btreeNode[item.sortValue].rightChild.items.every(item => item.btreeRightChildNodeRightmostItemIndex == -1)
          ),
        })
      }

      i += 1
    }

    if (btreeNode.root) {
      btreeNode.totalItemsCount = memo.totalItemsCount
      btreeNode.totalKeys = memo.keys.sort((a, b) => Number(a) - Number(b))
    }

    return btreeNode
  }

  // _logBTree(unique boolean) -> Promise<>
  async _logBTree(unique = false) {
    const btreeRootNode = await this._constructBTree({ unique })

    console.log(JSON.stringify(btreeRootNode, (key, value) => {
      if (key == 'items' || key == 'keys') {
        return undefined
      }
      return value
    }, 2))

  }

  // _traverseInOrder(options {
  //   onLeaf: function,
  // }) -> items Promise<Array<{
  //   index: number,
  //   sortValue: string|number,
  //   btreeLeftChildNodeRightmostItemIndex: number,
  //   btreeRightChildNodeRightmostItemIndex: number,
  //   btreeLeftSiblingItemIndex: number,
  // }>>
  async _traverseInOrder() {
    const btreeRootNode = await this._constructBTree({ unique: true })

    const result = [];
    const stack = [{ node: btreeRootNode, itemIndex: 0, stage: 'left' }];

    while (stack.length > 0) {
      const current = stack[stack.length - 1];
      const { node, itemIndex, stage } = current;

      // Finish node if we've gone through all items
      if (itemIndex >= node.items.length) {
        stack.pop();
        continue;
      }

      const currentItem = node.items[itemIndex];
      // FIX: Access child using 'sortValue' because that matches the object keys '46', '93'
      const childContainer = node[currentItem.sortValue];

      if (stage === 'left') {
        current.stage = 'item';
        if (childContainer && childContainer.leftChild) {
          stack.push({ node: childContainer.leftChild, itemIndex: 0, stage: 'left' });
        }
      } 
      else if (stage === 'item') {
        result.push(currentItem);
        current.stage = 'right';
      } 
      else if (stage === 'right') {
        current.itemIndex++; // Move to next item in this node
        current.stage = 'left'; 
        if (childContainer && childContainer.rightChild) {
          stack.push({ node: childContainer.rightChild, itemIndex: 0, stage: 'left' });
        }
      }
    }

    return result
  }

  // _insert(key string, value string, sortValue number|string, index number) -> Promise<>
  async _insert(key, value, sortValue, index) {

    const btreeLeftChildNodeRightmostItemIndex = -1
    const btreeRightChildNodeRightmostItemIndex = -1
    let btreeLeftSiblingItemIndex = -1

    let leftItem = null
    let rightItem = null
    const btreeRootNodeRightmostItem = await this._getBTreeRootNodeRightmostItem()
    const btreeRootNodeItems = await this._getBTreeNodeItems(btreeRootNodeRightmostItem)

    if (btreeRootNodeRightmostItem == null) { // first item in btree
      await this._writeBTreeRootRightmostItemIndex(index)
    }
    else if (btreeRootNodeItems.length == ((2 * this.degree) - 1)) { // current b-tree node at maximum number of items
      const btreeRootNodeItem = await this._splitBTreeRootNode(btreeRootNodeItems, { sortValue })

      const insertResult = await this._insertBTreeNodeItem(
        index,
        sortValue,
        [btreeRootNodeItem],
        btreeRootNodeItem
      )

      /*
      console.log(sortValue, 'insertResult', {
        predecessor: insertResult.predecessor?.sortValue,
        successor: insertResult.successor?.sortValue,
      })
      */

      leftItem = insertResult.predecessor
      rightItem = insertResult.successor
      btreeLeftSiblingItemIndex = insertResult.btreeLeftSiblingItemIndex
      if (btreeLeftSiblingItemIndex === btreeRootNodeItem.index) {
        await this._writeBTreeRootRightmostItemIndex(index)
      }
    } else {

      const insertResult = await this._insertBTreeNodeItem(
        index,
        sortValue,
        btreeRootNodeItems,
        btreeRootNodeRightmostItem
      )

      /*
      console.log(sortValue, 'insertResult', {
        predecessor: insertResult.predecessor?.sortValue,
        successor: insertResult.successor?.sortValue,
      })
      */

      leftItem = insertResult.predecessor
      rightItem = insertResult.successor
      btreeLeftSiblingItemIndex = insertResult.btreeLeftSiblingItemIndex
      if (btreeLeftSiblingItemIndex === btreeRootNodeRightmostItem.index) {
        await this._writeBTreeRootRightmostItemIndex(index)
      }
    }

    const forwardStartItem = await this._getForwardStartItem()

    /*
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
    */

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

    const position = index * DATA_SLICE_SIZE
    const buffer = Buffer.alloc(DATA_SLICE_SIZE)
    const sortValueString = typeof sortValue == 'string' ? sortValue : sortValue.toString()


    // storage file slice
    // 1 byte for status marker: 0 empty / 1 occupied / 2 deleted
    // 4 bytes for key size
    // 4 bytes for sort value size
    // 4 bytes for value size
    // 4 bytes for forward index
    // 4 bytes for reverse index
    // 4 bytes for left child btree node rightmost key index
    // 4 bytes for right child btree node rightmost key index
    // 4 bytes for previous sibling key index
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
    buffer.writeInt32BE(btreeLeftChildNodeRightmostItemIndex, 21)
    buffer.writeInt32BE(btreeRightChildNodeRightmostItemIndex, 25)
    buffer.writeInt32BE(btreeLeftSiblingItemIndex, 29)
    buffer.write(key, 33, keyByteLength, ENCODING)
    buffer.write(sortValueString, 33 + keyByteLength, sortValueByteLength, ENCODING)
    buffer.write(value, 33 + keyByteLength + sortValueByteLength, valueByteLength, ENCODING)

    await this.storageFd.write(buffer, {
      offset: 0,
      position,
      length: buffer.length,
    })

  }

  // _update(key string, value string, sortValue number|string, index number) -> Promise<>
  async _update(key, value, sortValue, index) {
    const item = await this._getItem(index)

    const btreeLeftChildNodeRightmostItemIndex = -1
    const btreeRightChildNodeRightmostItemIndex = -1
    const btreeLeftSiblingItemIndex = -1

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
    buffer.writeInt32BE(btreeLeftChildNodeRightmostItemIndex, 21)
    buffer.writeInt32BE(btreeRightChildNodeRightmostItemIndex, 25)
    buffer.writeInt32BE(btreeLeftSiblingItemIndex, 29)
    buffer.write(key, 33, keyByteLength, ENCODING)
    buffer.write(sortValueString, 33 + keyByteLength, sortValueByteLength, ENCODING)
    buffer.write(value, 33 + keyByteLength + sortValueByteLength, valueByteLength, ENCODING)

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
        if (this._count < this._length) {
          throw new Error('Unreachable index')
        }
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
        33 + keyByteLength + sortValueByteLength,
        33 + keyByteLength + sortValueByteLength + valueByteLength
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
   * Arguments:
   *   * (none)
   *
   * Return:
   *   * `values` - `AsyncGenerator<string>` - an async iterator of the values of all items in the disk hash table sorted by sort-value in ascending order.
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
   * Arguments:
   *   * (none)
   *
   * Return:
   *   * `values` - `AsyncGenerator<string>` - an async iterator of the values of all items in the disk hash table sorted by sort-value in descending order.
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
