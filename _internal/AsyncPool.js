/**
 * @name AsyncPool
 *
 * @docs
 * ```coffeescript [specscript]
 * new AsyncPool(size number) -> pool AsyncPool
 * ```
 */
class AsyncPool {
  constructor(size) {
    this.size = size
    this.promises = new Set()
  }

  // then(f function) -> Promise<>
  then(f) {
    return Promise.race(this.promises).then(f)
  }

  // add(promise Promise) -> Promise<>
  async add(promise) {
    const pooledPromise = promise.then(() => {
      this.promises.delete(pooledPromise)
    })
    this.promises.add(pooledPromise)
    if (this.promises.size == this.size) {
      await this
    }
    return undefined
  }

  // all() -> Promise<>
  async all() {
    return Promise.all(this.promises)
  }
}

module.exports = AsyncPool
