/**
 * @name convert
 *
 * @docs
 * ```coffeescript [specscript]
 * convert(value any, t 'number'|'string') -> number|string
 * ```
 */
function convert(value, t) {
  if (t == 'string') {
    return value.toString()
  }
  if (t == 'number') {
    return Number(value)
  }
  throw new Error(`Unrecognized type ${t}`)
}

module.exports = convert
