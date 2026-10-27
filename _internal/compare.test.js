const Test = require("thunk-test")
const assert = require("assert")
const compare = require("./compare")

const test1 = new Test("compare", compare)

test1.case("eq", 1, 1, true)
test1.case("eq", 1, "1", true)
test1.case("gt", 1, 2, false)
test1.case("gt", 2, 1, true)
test1.case("gte", 1, 1, true)
test1.case("gte", 2, 1, true)
test1.case("gte", 1, 2, false)
test1.throws("unknown", 0, 0, new Error("Unknown operator unknown"))

function test2() {
  const table = {
    sortValueType: ["number", "string", "string"]
  }
  assert.equal(compare.call(table, "eq", [1, "a", "a"], [1, "a", "a"]), true)
  assert.equal(compare.call(table, "eq", ["2", "a", "a"], [2, "a", "a"]), true)
  assert.equal(compare.call(table, "eq", [1, "a", "a"], [2, "a", "a"]), false)
  assert.equal(compare.call(table, "eq", [2, "a", "a"], [2, "a", "a"]), true)
  assert.equal(compare.call(table, "eq", [2, "a", "a"], [2, "b", "a"]), false)
  assert.equal(compare.call(table, "gt", [2, "a", "a"], [1, "a", "a"]), true)
  assert.equal(compare.call(table, "gt", [1, "b", "a"], [1, "a", "a"]), true)
  assert.equal(compare.call(table, "gt", [1, "a", "a"], [1, "a", "a"]), false)
  assert.equal(compare.call(table, "gte", [1, "a", "a"], [1, "a", "a"]), true)
  assert.equal(compare.call(table, "gte", [1, "a", "a"], [1, "a", "b"]), false)
  assert.equal(compare.call(table, "gt", [1, "a", "a"], [1, "a", "b"]), false)
  assert.equal(compare.call(table, "lte", [1, "a", "a"], [1, "a", "a"]), true)
  assert.equal(compare.call(table, "lte", [1, "a", "a"], [2, "a", "a"]), true)
  assert.equal(compare.call(table, "lte", [1, "b", "a"], [2, "a", "a"]), true)
  assert.equal(compare.call(table, "lte", [1, "b", "a"], [1, "a", "a"]), false)
  assert.equal(compare.call(table, "lte", [1, "a", "b"], [1, "a", "a"]), false)

  assert.throws(
    () => compare.call(table, "eq", [], [2, "b", "a"]),
    new Error("Different sort value lengths")
  )
}

async function test() {
  await test1()
  await test2()
  console.log("Success")
}

if (process.argv[1] == __filename) {
  test()
}

module.exports = test
