const Test = require("thunk-test")
const convert = require("./convert")

const test = new Test("convert", convert)

test.case(1, "string", "1")
test.case("1", "string", "1")
test.case("1", "number", 1)
test.case(1, "number", 1)
test.case([1, "1", "a"], ["number", "string", "index"], [1, "1", "a"])
test.case("11a", ["number", "string", "index"], [1, "1", "a"])
test.throws({}, "index", new Error("Bad index value"))
test.throws("1", "unknown", new Error("Unknown type unknown"))

if (process.argv[1] == __filename) {
  test()
}

module.exports = test
