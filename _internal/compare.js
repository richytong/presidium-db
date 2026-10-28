// eq(a any, b any) -> boolean
const eq = (a, b) => a == b

// gt(a any, b any) -> boolean
const gt = (a, b) => a > b

// gte(a any, b any) -> boolean
const gte = (a, b) => a >= b

// lte(a any, b any) -> boolean
const lte = (a, b) => a <= b

/**
 * @name compare
 *
 * @docs
 * ```specscript
 * type SortValue number|string|Array<number|string>
 *
 * compare(operator "eq"|"gt"|"gte"|"lte", sortValueA SortValue, sortValueB SortValue) -> boolean
 * ```
 */
function compare(operator, sortValueA, sortValueB) {
  let comparator
  if (operator == "eq") {
    comparator = eq
  } else if (operator == "gt") {
    comparator = gt
  } else if (operator == "gte") {
    comparator = gte
  } else if (operator == "lte") {
    comparator = lte
  } else {
    throw new Error(`Unknown operator ${operator}`)
  }

  if (Array.isArray(sortValueA)) {
    if (sortValueA.length != sortValueB.length) {
      throw new Error("Different sort value lengths")
    }
    for (let i = 0; i < sortValueA.length; i++) {
      if (eq(sortValueA[i], sortValueB[i])) {
      } else {
        return comparator(sortValueA[i], sortValueB[i])
      }
    }
    return comparator != gt
  }

  return comparator(sortValueA, sortValueB)

}

module.exports = compare
