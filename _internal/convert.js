// convertPrimitive(value any, t "number"|"string"|"index") -> number|string
function convertPrimitive(value, t) {
  if (t == "string") {
    return value.toString()
  }
  if (t == "number") {
    return Number(value)
  }
  if (t == "index") {
    if (typeof value == "string" || typeof value == "number") {
      return value
    }
    const error = new Error("Bad index value")
    error.value = value
    throw error
  }
  throw new Error(`Unknown type ${t}`)
}

/**
 * @name convert
 *
 * @docs
 * ```coffeescript [specscript]
 * convert(value any, t "number"|"string"|"index"|Array<"number"|"string"|"index">) -> number|string
 * ```
 */
function convert(value, t) {
  if (Array.isArray(t)) {
    const result = []
    const array = Array.isArray(value) ? value : value.split("")
    for (let i = 0; i < array.length; i++) {
      result.push(convertPrimitive(array[i], t[i]))
    }
    return result
  }
  return convertPrimitive(value, t)
}

module.exports = convert
