// Generated file. Do not edit.
//
// @malloydata/malloy-filter@0.0.427 (MIT), bundled as a browser ES
// module by scripts/vendor-malloy-filter.mjs. Regenerate with:
//
//   bun run vendor:malloy-filter
//
// Exports StringFilterExpression and NumberFilterExpression. The page uses
// their unparse/parse pair so a picked or typed value is escaped by the
// grammar's own printer rather than by a rule this repository maintains.

var __create = Object.create;
var __defProp = Object.defineProperty;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __getProtoOf = Object.getPrototypeOf;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __commonJS = (cb, mod) => function __require() {
  return mod || (0, cb[__getOwnPropNames(cb)[0]])((mod = { exports: {} }).exports, mod), mod.exports;
};
var __copyProps = (to, from, except, desc) => {
  if (from && typeof from === "object" || typeof from === "function") {
    for (let key of __getOwnPropNames(from))
      if (!__hasOwnProp.call(to, key) && key !== except)
        __defProp(to, key, { get: () => from[key], enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable });
  }
  return to;
};
var __toESM = (mod, isNodeMode, target) => (target = mod != null ? __create(__getProtoOf(mod)) : {}, __copyProps(
  // If the importer is in node compatibility mode or this is not an ESM
  // file that has been converted to a CommonJS file using a Babel-
  // compatible transform (i.e. "__esModule" has not been set), then set
  // "default" to the CommonJS "module.exports" for node compatibility.
  isNodeMode || !mod || !mod.__esModule ? __defProp(target, "default", { value: mod, enumerable: true }) : target,
  mod
));

// node_modules/@malloydata/malloy-filter/dist/filter_interface.js
var require_filter_interface = __commonJS({
  "node_modules/@malloydata/malloy-filter/dist/filter_interface.js"(exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    exports.isChainOp = isChainOp;
    exports.isStringCondition = isStringCondition;
    exports.isStringFilter = isStringFilter;
    exports.isBooleanFilter = isBooleanFilter;
    exports.isNumberFilter = isNumberFilter;
    exports.isTemporalFilter = isTemporalFilter;
    exports.isFilterExpression = isFilterExpression;
    exports.isFilterable = isFilterable;
    function isChainOp(s) {
      return ["and", "or", ","].includes(s);
    }
    function isStringCondition(sc) {
      return ["starts", "ends", "contains", "="].includes(sc.operator);
    }
    function isStringFilter(sc) {
      return typeof sc === "object" && sc !== null && "operator" in sc && typeof sc.operator === "string" && [
        "starts",
        "ends",
        "contains",
        "=",
        "~",
        "null",
        "none",
        "empty",
        "and",
        "or",
        ",",
        "()"
      ].includes(sc.operator);
    }
    function isBooleanFilter(bc) {
      return typeof bc === "object" && bc !== null && "operator" in bc && typeof bc.operator === "string" && ["null", "none", "true", "false", "=false", "=true"].includes(bc.operator);
    }
    function isNumberFilter(sc) {
      return typeof sc === "object" && sc !== null && "operator" in sc && typeof sc.operator === "string" && [
        "range",
        "<=",
        ">=",
        "!=",
        "=",
        ">",
        "<",
        "and",
        "or",
        "()",
        "null",
        "none"
      ].includes(sc.operator);
    }
    function isTemporalFilter(sc) {
      return typeof sc === "object" && sc !== null && "operator" in sc && typeof sc.operator === "string" && [
        "literal",
        "before",
        "after",
        "to",
        "for",
        "in",
        "and",
        "or",
        "in_last",
        "this",
        "last",
        "next",
        "()",
        "null",
        "none"
      ].includes(sc.operator);
    }
    function isFilterExpression(obj) {
      return typeof obj === "object" && obj !== null && "operator" in obj;
    }
    function isFilterable(s) {
      return [
        "string",
        "number",
        "boolean",
        "timestamp",
        "timestamptz",
        "date"
      ].includes(s);
    }
  }
});

// node_modules/@malloydata/malloy-filter/dist/boolean_filter_expression.js
var require_boolean_filter_expression = __commonJS({
  "node_modules/@malloydata/malloy-filter/dist/boolean_filter_expression.js"(exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    exports.BooleanFilterExpression = void 0;
    exports.BooleanFilterExpression = {
      parse(srcText) {
        var _a;
        if (srcText.match(/^\s*$/)) {
          return { parsed: null, log: [] };
        }
        const ret = { parsed: null, log: [] };
        let src = srcText.toLowerCase().trim().replace(/\s\s+/, " ");
        let negate = false;
        if (src.startsWith("not ")) {
          negate = true;
          src = src.slice(4);
        }
        if (src === "true") {
          ret.parsed = { operator: "true" };
        } else if (src === "=true") {
          ret.parsed = { operator: "=true" };
        } else if (src === "false") {
          ret.parsed = { operator: "false" };
        } else if (src === "=false") {
          ret.parsed = { operator: "=false" };
        } else if (src === "null") {
          ret.parsed = { operator: "null" };
        } else if (src === "none") {
          ret.parsed = { operator: "none" };
        } else {
          const nonSpace = srcText.match(/[^\s]/);
          const startIndex = nonSpace ? (_a = nonSpace.index) !== null && _a !== void 0 ? _a : 0 : 0;
          ret.log = [
            {
              message: `Illegal boolean filter '${src}'. Must be one of true,=true,false,=false,null,none`,
              severity: "error",
              startIndex,
              endIndex: startIndex + srcText.length - 1
            }
          ];
        }
        if (negate && ret.parsed) {
          ret.parsed.not = true;
        }
        return ret;
      },
      unparse(bc) {
        if (bc === null) {
          return "";
        }
        const n = bc.not ? "not " : "";
        return n + bc.operator;
      }
    };
  }
});

// node_modules/@malloydata/malloy-filter/dist/clause_utils.js
var require_clause_utils = __commonJS({
  "node_modules/@malloydata/malloy-filter/dist/clause_utils.js"(exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    exports.maybeNot = maybeNot;
    exports.unescape = unescape;
    exports.escape = escape;
    exports.matchOp = matchOp;
    exports.conjoin = conjoin;
    exports.joinNumbers = joinNumbers;
    exports.mkRange = mkRange;
    exports.mkValues = mkValues;
    exports.numNot = numNot;
    exports.temporalNot = temporalNot;
    exports.joinTemporal = joinTemporal;
    exports.timeLiteral = timeLiteral;
    exports.mkUnits = mkUnits;
    var filter_interface_1 = require_filter_interface();
    function maybeNot(data) {
      const [isMinus, op] = data;
      if (isMinus && op && (0, filter_interface_1.isStringFilter)(op)) {
        return { ...op, not: true };
      }
      return op;
    }
    function unescape(str) {
      return str.replace(/\\(.)/g, "$1");
    }
    function escape(str) {
      const lstr = str.toLowerCase();
      if (lstr === "null" || lstr === "empty" || lstr === "none") {
        return "\\" + str;
      }
      return str.replace(/([,; |()\\%_-])/g, "\\$1");
    }
    function describeString(s) {
      let percentStart = false;
      let percentEnd = false;
      let endSpaceCnt = 0;
      let hasLike = false;
      const iLen = s.length;
      for (let i = 0; i < iLen; i += 1) {
        const c = s[i];
        if (c === " " || c === "	") {
          endSpaceCnt += 1;
          continue;
        }
        endSpaceCnt = 0;
        if (c === "%") {
          hasLike = true;
          if (i === 0) {
            percentStart = true;
          }
          percentEnd = true;
        } else {
          percentEnd = false;
          if (c === "\\") {
            i += 1;
          } else if (c === "_") {
            hasLike = true;
          }
        }
      }
      return {
        hasLike,
        percentEnd: percentEnd && iLen > 1,
        percentStart: percentStart && iLen > 1,
        endSpace: endSpaceCnt
      };
    }
    function matchOp(matchSrc) {
      let matchTxt = matchSrc.trimStart();
      const { hasLike, percentEnd, percentStart, endSpace } = describeString(matchTxt);
      if (endSpace > 0) {
        matchTxt = matchTxt.slice(0, -endSpace);
      }
      if (hasLike) {
        if (percentStart && percentEnd) {
          const mid = matchTxt.slice(1, -1);
          if (!describeString(mid).hasLike && mid.length > 0) {
            return { operator: "contains", values: [unescape(mid)] };
          }
        } else if (percentEnd) {
          const tail = matchTxt.slice(0, -1);
          if (!describeString(tail).hasLike) {
            return { operator: "starts", values: [unescape(tail)] };
          }
        } else if (percentStart) {
          const head = matchTxt.slice(1);
          if (!describeString(head).hasLike) {
            return { operator: "ends", values: [unescape(head)] };
          }
        }
        return { operator: "~", escaped_values: [matchTxt] };
      }
      if (matchTxt.toLowerCase() === "null") {
        return { operator: "null" };
      }
      if (matchTxt.toLowerCase() === "empty") {
        return { operator: "empty" };
      }
      if (matchTxt.toLowerCase() === "none") {
        return { operator: "none" };
      }
      return { operator: "=", values: [unescape(matchTxt)] };
    }
    function sameAs(a, b) {
      var _a, _b;
      return a.operator === b.operator && ((_a = a["not"]) !== null && _a !== void 0 ? _a : false) === ((_b = b["not"]) !== null && _b !== void 0 ? _b : false);
    }
    function conjoin(left, op, right) {
      op = op.toLowerCase();
      if ((0, filter_interface_1.isStringFilter)(left) && (0, filter_interface_1.isStringFilter)(right)) {
        if (op === ",") {
          if (left.operator === "~" && sameAs(left, right)) {
            return {
              ...left,
              escaped_values: [...left.escaped_values, ...right.escaped_values]
            };
          }
          if ((0, filter_interface_1.isStringCondition)(left) && sameAs(left, right)) {
            return { ...left, values: [...left.values, ...right.values] };
          }
        }
        const operator = op === "," ? "," : op === "|" ? "or" : op === ";" ? "and" : void 0;
        if (operator) {
          if (left.operator === operator) {
            return { ...left, members: [...left.members, right] };
          }
          return { operator, members: [left, right] };
        }
      }
      return null;
    }
    function joinNumbers(left, op, right) {
      op = op.toLowerCase();
      if ((0, filter_interface_1.isNumberFilter)(left) && (0, filter_interface_1.isNumberFilter)(right)) {
        if (op === "or" && left.operator === "=" && sameAs(left, right)) {
          const ret = {
            operator: "=",
            values: [...left.values, ...right.values]
          };
          if (left.not) {
            ret.not = true;
          }
          return ret;
        }
        if (op === "and" || op === "or") {
          if (left.operator === op) {
            return { ...left, members: [...left.members, right] };
          }
          return { operator: op, members: [left, right] };
        }
      }
      return null;
    }
    function mkRange(left, rFrom, rTo, right) {
      return {
        operator: "range",
        startValue: rFrom,
        startOperator: left === "(" ? ">" : ">=",
        endValue: rTo,
        endOperator: right === ")" ? "<" : "<="
      };
    }
    function mkValues(n, nList) {
      return { values: [n, ...nList] };
    }
    function numNot(op, notToken) {
      if ((0, filter_interface_1.isNumberFilter)(op) && notToken) {
        if (op.operator === "=")
          return { operator: "!=", values: op.values };
        if (op.operator === "!=")
          return { operator: "=", values: op.values };
        return { ...op, not: true };
      }
      return op;
    }
    function temporalNot(op, notToken) {
      if ((0, filter_interface_1.isTemporalFilter)(op) && notToken) {
        if ("not" in op) {
          const ret = { ...op };
          if (op.not) {
            delete ret.not;
          } else {
            ret.not = true;
          }
          return ret;
        }
        return { ...op, not: true };
      }
      return op;
    }
    function joinTemporal(left, op, right) {
      op = op.toLowerCase();
      if ((0, filter_interface_1.isTemporalFilter)(left) && (0, filter_interface_1.isTemporalFilter)(right)) {
        if (op === "and" || op === "or") {
          if (left.operator === op) {
            return { ...left, members: [...left.members, right] };
          }
          return { operator: op, members: [left, right] };
        }
      }
      return null;
    }
    function timeLiteral(literal, units) {
      const ret = { moment: "literal", literal };
      if (units) {
        ret.units = units;
      }
      return ret;
    }
    function mkUnits(unit_s) {
      switch (unit_s.toLowerCase()) {
        case "second":
        case "seconds":
          return "second";
        case "minute":
        case "minutes":
          return "minute";
        case "hour":
        case "hours":
          return "hour";
        case "day":
        case "days":
          return "day";
        case "week":
        case "weeks":
          return "week";
        case "month":
        case "months":
          return "month";
        case "quarter":
        case "quarters":
          return "quarter";
        case "year":
        case "years":
          return "year";
      }
      return void 0;
    }
  }
});

// node_modules/@malloydata/malloy-filter/dist/lib/fexpr_number_parser.js
var require_fexpr_number_parser = __commonJS({
  "node_modules/@malloydata/malloy-filter/dist/lib/fexpr_number_parser.js"(exports, module) {
    "use strict";
    var { numNot, mkRange, joinNumbers, mkValues } = require_clause_utils();
    function peg$subclass(child, parent) {
      function C() {
        this.constructor = child;
      }
      C.prototype = parent.prototype;
      child.prototype = new C();
    }
    function peg$SyntaxError(message, expected, found, location) {
      var self = Error.call(this, message);
      if (Object.setPrototypeOf) {
        Object.setPrototypeOf(self, peg$SyntaxError.prototype);
      }
      self.expected = expected;
      self.found = found;
      self.location = location;
      self.name = "SyntaxError";
      return self;
    }
    peg$subclass(peg$SyntaxError, Error);
    function peg$padEnd(str, targetLength, padString) {
      padString = padString || " ";
      if (str.length > targetLength) {
        return str;
      }
      targetLength -= str.length;
      padString += padString.repeat(targetLength);
      return str + padString.slice(0, targetLength);
    }
    peg$SyntaxError.prototype.format = function(sources) {
      var str = "Error: " + this.message;
      if (this.location) {
        var src = null;
        var k;
        for (k = 0; k < sources.length; k++) {
          if (sources[k].source === this.location.source) {
            src = sources[k].text.split(/\r\n|\n|\r/g);
            break;
          }
        }
        var s = this.location.start;
        var offset_s = this.location.source && typeof this.location.source.offset === "function" ? this.location.source.offset(s) : s;
        var loc = this.location.source + ":" + offset_s.line + ":" + offset_s.column;
        if (src) {
          var e = this.location.end;
          var filler = peg$padEnd("", offset_s.line.toString().length, " ");
          var line = src[s.line - 1];
          var last = s.line === e.line ? e.column : line.length + 1;
          var hatLen = last - s.column || 1;
          str += "\n --> " + loc + "\n" + filler + " |\n" + offset_s.line + " | " + line + "\n" + filler + " | " + peg$padEnd("", s.column - 1, " ") + peg$padEnd("", hatLen, "^");
        } else {
          str += "\n at " + loc;
        }
      }
      return str;
    };
    peg$SyntaxError.buildMessage = function(expected, found) {
      var DESCRIBE_EXPECTATION_FNS = {
        literal: function(expectation) {
          return '"' + literalEscape(expectation.text) + '"';
        },
        class: function(expectation) {
          var escapedParts = expectation.parts.map(function(part) {
            return Array.isArray(part) ? classEscape(part[0]) + "-" + classEscape(part[1]) : classEscape(part);
          });
          return "[" + (expectation.inverted ? "^" : "") + escapedParts.join("") + "]";
        },
        any: function() {
          return "any character";
        },
        end: function() {
          return "end of input";
        },
        other: function(expectation) {
          return expectation.description;
        }
      };
      function hex(ch) {
        return ch.charCodeAt(0).toString(16).toUpperCase();
      }
      function literalEscape(s) {
        return s.replace(/\\/g, "\\\\").replace(/"/g, '\\"').replace(/\0/g, "\\0").replace(/\t/g, "\\t").replace(/\n/g, "\\n").replace(/\r/g, "\\r").replace(/[\x00-\x0F]/g, function(ch) {
          return "\\x0" + hex(ch);
        }).replace(/[\x10-\x1F\x7F-\x9F]/g, function(ch) {
          return "\\x" + hex(ch);
        });
      }
      function classEscape(s) {
        return s.replace(/\\/g, "\\\\").replace(/\]/g, "\\]").replace(/\^/g, "\\^").replace(/-/g, "\\-").replace(/\0/g, "\\0").replace(/\t/g, "\\t").replace(/\n/g, "\\n").replace(/\r/g, "\\r").replace(/[\x00-\x0F]/g, function(ch) {
          return "\\x0" + hex(ch);
        }).replace(/[\x10-\x1F\x7F-\x9F]/g, function(ch) {
          return "\\x" + hex(ch);
        });
      }
      function describeExpectation(expectation) {
        return DESCRIBE_EXPECTATION_FNS[expectation.type](expectation);
      }
      function describeExpected(expected2) {
        var descriptions = expected2.map(describeExpectation);
        var i, j;
        descriptions.sort();
        if (descriptions.length > 0) {
          for (i = 1, j = 1; i < descriptions.length; i++) {
            if (descriptions[i - 1] !== descriptions[i]) {
              descriptions[j] = descriptions[i];
              j++;
            }
          }
          descriptions.length = j;
        }
        switch (descriptions.length) {
          case 1:
            return descriptions[0];
          case 2:
            return descriptions[0] + " or " + descriptions[1];
          default:
            return descriptions.slice(0, -1).join(", ") + ", or " + descriptions[descriptions.length - 1];
        }
      }
      function describeFound(found2) {
        return found2 ? '"' + literalEscape(found2) + '"' : "end of input";
      }
      return "Expected " + describeExpected(expected) + " but " + describeFound(found) + " found.";
    };
    function peg$parse(input, options) {
      options = options !== void 0 ? options : {};
      var peg$FAILED = {};
      var peg$source = options.grammarSource;
      var peg$startRuleFunctions = { numberFilter: peg$parsenumberFilter };
      var peg$startRuleFunction = peg$parsenumberFilter;
      var peg$c0 = "(";
      var peg$c1 = ")";
      var peg$c2 = "!=";
      var peg$c3 = ",";
      var peg$c4 = "=";
      var peg$c5 = "<=";
      var peg$c6 = ">=";
      var peg$c7 = "[";
      var peg$c8 = "]";
      var peg$c9 = "-";
      var peg$c10 = ".";
      var peg$c11 = "not";
      var peg$c12 = "null";
      var peg$c13 = "none";
      var peg$c14 = "and";
      var peg$c15 = "or";
      var peg$c16 = "to";
      var peg$r0 = /^[<>]/;
      var peg$r1 = /^[0-9]/;
      var peg$r2 = /^[Ee]/;
      var peg$r3 = /^[+\-]/;
      var peg$r4 = /^[a-zA-Z]/;
      var peg$r5 = /^[ \t]/;
      var peg$e0 = peg$literalExpectation("(", false);
      var peg$e1 = peg$literalExpectation(")", false);
      var peg$e2 = peg$literalExpectation("!=", false);
      var peg$e3 = peg$literalExpectation(",", false);
      var peg$e4 = peg$literalExpectation("=", false);
      var peg$e5 = peg$literalExpectation("<=", false);
      var peg$e6 = peg$literalExpectation(">=", false);
      var peg$e7 = peg$classExpectation(["<", ">"], false, false);
      var peg$e8 = peg$literalExpectation("[", false);
      var peg$e9 = peg$literalExpectation("]", false);
      var peg$e10 = peg$literalExpectation("-", false);
      var peg$e11 = peg$classExpectation([["0", "9"]], false, false);
      var peg$e12 = peg$literalExpectation(".", false);
      var peg$e13 = peg$classExpectation(["E", "e"], false, false);
      var peg$e14 = peg$classExpectation(["+", "-"], false, false);
      var peg$e15 = peg$literalExpectation("not", true);
      var peg$e16 = peg$literalExpectation("null", true);
      var peg$e17 = peg$literalExpectation("none", true);
      var peg$e18 = peg$literalExpectation("and", true);
      var peg$e19 = peg$literalExpectation("or", true);
      var peg$e20 = peg$literalExpectation("to", true);
      var peg$e21 = peg$classExpectation([["a", "z"], ["A", "Z"]], false, false);
      var peg$e22 = peg$otherExpectation("whitespace");
      var peg$e23 = peg$classExpectation([" ", "	"], false, false);
      var peg$f0 = function(head, tail) {
        return tail.reduce((left, [, cop, , right]) => joinNumbers(left, cop, right), head);
      };
      var peg$f1 = function(not, clause) {
        return numNot(clause, not);
      };
      var peg$f2 = function(clause) {
        return clause;
      };
      var peg$f3 = function() {
        return { operator: "null" };
      };
      var peg$f4 = function() {
        return { operator: "none" };
      };
      var peg$f5 = function(expr) {
        return { operator: "()", expr };
      };
      var peg$f6 = function(open, b, e, close) {
        return mkRange(open, b, e, close);
      };
      var peg$f7 = function(n, nList) {
        return { operator: "!=", ...mkValues(n, nList.map((x) => x[3])) };
      };
      var peg$f8 = function(n, nList) {
        return { operator: "=", ...mkValues(n, nList.map((x) => x[3])) };
      };
      var peg$f9 = function(op, n) {
        return { operator: op, values: [n] };
      };
      var peg$f10 = function(n, nList) {
        return { operator: "=", ...mkValues(n, nList.map((x) => x[3])) };
      };
      var peg$f11 = function() {
        return "[";
      };
      var peg$f12 = function() {
        return "(";
      };
      var peg$f13 = function() {
        return "]";
      };
      var peg$f14 = function() {
        return ")";
      };
      var peg$f15 = function(n) {
        return n;
      };
      var peg$f16 = function() {
        return "or";
      };
      var peg$f17 = function() {
        return "and";
      };
      var peg$f18 = function() {
        return "not";
      };
      var peg$currPos = options.peg$currPos | 0;
      var peg$savedPos = peg$currPos;
      var peg$posDetailsCache = [{ line: 1, column: 1 }];
      var peg$maxFailPos = peg$currPos;
      var peg$maxFailExpected = options.peg$maxFailExpected || [];
      var peg$silentFails = options.peg$silentFails | 0;
      var peg$result;
      if (options.startRule) {
        if (!(options.startRule in peg$startRuleFunctions)) {
          throw new Error(`Can't start parsing from rule "` + options.startRule + '".');
        }
        peg$startRuleFunction = peg$startRuleFunctions[options.startRule];
      }
      function text() {
        return input.substring(peg$savedPos, peg$currPos);
      }
      function offset() {
        return peg$savedPos;
      }
      function range() {
        return {
          source: peg$source,
          start: peg$savedPos,
          end: peg$currPos
        };
      }
      function location() {
        return peg$computeLocation(peg$savedPos, peg$currPos);
      }
      function expected(description, location2) {
        location2 = location2 !== void 0 ? location2 : peg$computeLocation(peg$savedPos, peg$currPos);
        throw peg$buildStructuredError([peg$otherExpectation(description)], input.substring(peg$savedPos, peg$currPos), location2);
      }
      function error(message, location2) {
        location2 = location2 !== void 0 ? location2 : peg$computeLocation(peg$savedPos, peg$currPos);
        throw peg$buildSimpleError(message, location2);
      }
      function peg$literalExpectation(text2, ignoreCase) {
        return { type: "literal", text: text2, ignoreCase };
      }
      function peg$classExpectation(parts, inverted, ignoreCase) {
        return { type: "class", parts, inverted, ignoreCase };
      }
      function peg$anyExpectation() {
        return { type: "any" };
      }
      function peg$endExpectation() {
        return { type: "end" };
      }
      function peg$otherExpectation(description) {
        return { type: "other", description };
      }
      function peg$computePosDetails(pos) {
        var details = peg$posDetailsCache[pos];
        var p;
        if (details) {
          return details;
        } else {
          if (pos >= peg$posDetailsCache.length) {
            p = peg$posDetailsCache.length - 1;
          } else {
            p = pos;
            while (!peg$posDetailsCache[--p]) {
            }
          }
          details = peg$posDetailsCache[p];
          details = {
            line: details.line,
            column: details.column
          };
          while (p < pos) {
            if (input.charCodeAt(p) === 10) {
              details.line++;
              details.column = 1;
            } else {
              details.column++;
            }
            p++;
          }
          peg$posDetailsCache[pos] = details;
          return details;
        }
      }
      function peg$computeLocation(startPos, endPos, offset2) {
        var startPosDetails = peg$computePosDetails(startPos);
        var endPosDetails = peg$computePosDetails(endPos);
        var res = {
          source: peg$source,
          start: {
            offset: startPos,
            line: startPosDetails.line,
            column: startPosDetails.column
          },
          end: {
            offset: endPos,
            line: endPosDetails.line,
            column: endPosDetails.column
          }
        };
        if (offset2 && peg$source && typeof peg$source.offset === "function") {
          res.start = peg$source.offset(res.start);
          res.end = peg$source.offset(res.end);
        }
        return res;
      }
      function peg$fail(expected2) {
        if (peg$currPos < peg$maxFailPos) {
          return;
        }
        if (peg$currPos > peg$maxFailPos) {
          peg$maxFailPos = peg$currPos;
          peg$maxFailExpected = [];
        }
        peg$maxFailExpected.push(expected2);
      }
      function peg$buildSimpleError(message, location2) {
        return new peg$SyntaxError(message, null, null, location2);
      }
      function peg$buildStructuredError(expected2, found, location2) {
        return new peg$SyntaxError(peg$SyntaxError.buildMessage(expected2, found), expected2, found, location2);
      }
      function peg$parsenumberFilter() {
        var s0, s1, s2, s3, s4, s5, s6, s7;
        s0 = peg$currPos;
        s1 = peg$parsenumberUnary();
        if (s1 !== peg$FAILED) {
          s2 = [];
          s3 = peg$currPos;
          s4 = peg$parse_();
          s5 = peg$parseconjunction();
          if (s5 !== peg$FAILED) {
            s6 = peg$parse_();
            s7 = peg$parsenumberUnary();
            if (s7 !== peg$FAILED) {
              s4 = [s4, s5, s6, s7];
              s3 = s4;
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
          while (s3 !== peg$FAILED) {
            s2.push(s3);
            s3 = peg$currPos;
            s4 = peg$parse_();
            s5 = peg$parseconjunction();
            if (s5 !== peg$FAILED) {
              s6 = peg$parse_();
              s7 = peg$parsenumberUnary();
              if (s7 !== peg$FAILED) {
                s4 = [s4, s5, s6, s7];
                s3 = s4;
              } else {
                peg$currPos = s3;
                s3 = peg$FAILED;
              }
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
          }
          peg$savedPos = s0;
          s0 = peg$f0(s1, s2);
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
        return s0;
      }
      function peg$parsenumberUnary() {
        var s0, s1, s2, s3;
        s0 = peg$currPos;
        s1 = peg$parseNOT();
        if (s1 !== peg$FAILED) {
          s2 = peg$parse_();
          s3 = peg$parseclause();
          if (s3 !== peg$FAILED) {
            peg$savedPos = s0;
            s0 = peg$f1(s1, s3);
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
        if (s0 === peg$FAILED) {
          s0 = peg$currPos;
          s1 = peg$parseclause();
          if (s1 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$f2(s1);
          }
          s0 = s1;
        }
        return s0;
      }
      function peg$parseclause() {
        var s0, s1, s2, s3, s4, s5, s6, s7, s8, s9;
        s0 = peg$currPos;
        s1 = peg$parseNULL();
        if (s1 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$f3();
        }
        s0 = s1;
        if (s0 === peg$FAILED) {
          s0 = peg$currPos;
          s1 = peg$parseNONE();
          if (s1 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$f4();
          }
          s0 = s1;
          if (s0 === peg$FAILED) {
            s0 = peg$currPos;
            if (input.charCodeAt(peg$currPos) === 40) {
              s1 = peg$c0;
              peg$currPos++;
            } else {
              s1 = peg$FAILED;
              if (peg$silentFails === 0) {
                peg$fail(peg$e0);
              }
            }
            if (s1 !== peg$FAILED) {
              s2 = peg$parse_();
              s3 = peg$parsenumberFilter();
              if (s3 !== peg$FAILED) {
                s4 = peg$parse_();
                if (input.charCodeAt(peg$currPos) === 41) {
                  s5 = peg$c1;
                  peg$currPos++;
                } else {
                  s5 = peg$FAILED;
                  if (peg$silentFails === 0) {
                    peg$fail(peg$e1);
                  }
                }
                if (s5 !== peg$FAILED) {
                  peg$savedPos = s0;
                  s0 = peg$f5(s3);
                } else {
                  peg$currPos = s0;
                  s0 = peg$FAILED;
                }
              } else {
                peg$currPos = s0;
                s0 = peg$FAILED;
              }
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
            if (s0 === peg$FAILED) {
              s0 = peg$currPos;
              s1 = peg$parseopenInterval();
              if (s1 !== peg$FAILED) {
                s2 = peg$parse_();
                s3 = peg$parseN();
                if (s3 !== peg$FAILED) {
                  s4 = peg$parse_();
                  s5 = peg$parseTO();
                  if (s5 !== peg$FAILED) {
                    s6 = peg$parse_();
                    s7 = peg$parseN();
                    if (s7 !== peg$FAILED) {
                      s8 = peg$parse_();
                      s9 = peg$parsecloseInterval();
                      if (s9 !== peg$FAILED) {
                        peg$savedPos = s0;
                        s0 = peg$f6(s1, s3, s7, s9);
                      } else {
                        peg$currPos = s0;
                        s0 = peg$FAILED;
                      }
                    } else {
                      peg$currPos = s0;
                      s0 = peg$FAILED;
                    }
                  } else {
                    peg$currPos = s0;
                    s0 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s0;
                  s0 = peg$FAILED;
                }
              } else {
                peg$currPos = s0;
                s0 = peg$FAILED;
              }
              if (s0 === peg$FAILED) {
                s0 = peg$currPos;
                if (input.substr(peg$currPos, 2) === peg$c2) {
                  s1 = peg$c2;
                  peg$currPos += 2;
                } else {
                  s1 = peg$FAILED;
                  if (peg$silentFails === 0) {
                    peg$fail(peg$e2);
                  }
                }
                if (s1 !== peg$FAILED) {
                  s2 = peg$parse_();
                  s3 = peg$parseN();
                  if (s3 !== peg$FAILED) {
                    s4 = [];
                    s5 = peg$currPos;
                    s6 = peg$parse_();
                    if (input.charCodeAt(peg$currPos) === 44) {
                      s7 = peg$c3;
                      peg$currPos++;
                    } else {
                      s7 = peg$FAILED;
                      if (peg$silentFails === 0) {
                        peg$fail(peg$e3);
                      }
                    }
                    if (s7 !== peg$FAILED) {
                      s8 = peg$parse_();
                      s9 = peg$parseN();
                      if (s9 !== peg$FAILED) {
                        s6 = [s6, s7, s8, s9];
                        s5 = s6;
                      } else {
                        peg$currPos = s5;
                        s5 = peg$FAILED;
                      }
                    } else {
                      peg$currPos = s5;
                      s5 = peg$FAILED;
                    }
                    while (s5 !== peg$FAILED) {
                      s4.push(s5);
                      s5 = peg$currPos;
                      s6 = peg$parse_();
                      if (input.charCodeAt(peg$currPos) === 44) {
                        s7 = peg$c3;
                        peg$currPos++;
                      } else {
                        s7 = peg$FAILED;
                        if (peg$silentFails === 0) {
                          peg$fail(peg$e3);
                        }
                      }
                      if (s7 !== peg$FAILED) {
                        s8 = peg$parse_();
                        s9 = peg$parseN();
                        if (s9 !== peg$FAILED) {
                          s6 = [s6, s7, s8, s9];
                          s5 = s6;
                        } else {
                          peg$currPos = s5;
                          s5 = peg$FAILED;
                        }
                      } else {
                        peg$currPos = s5;
                        s5 = peg$FAILED;
                      }
                    }
                    peg$savedPos = s0;
                    s0 = peg$f7(s3, s4);
                  } else {
                    peg$currPos = s0;
                    s0 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s0;
                  s0 = peg$FAILED;
                }
                if (s0 === peg$FAILED) {
                  s0 = peg$currPos;
                  if (input.charCodeAt(peg$currPos) === 61) {
                    s1 = peg$c4;
                    peg$currPos++;
                  } else {
                    s1 = peg$FAILED;
                    if (peg$silentFails === 0) {
                      peg$fail(peg$e4);
                    }
                  }
                  if (s1 !== peg$FAILED) {
                    s2 = peg$parse_();
                    s3 = peg$parseN();
                    if (s3 !== peg$FAILED) {
                      s4 = [];
                      s5 = peg$currPos;
                      s6 = peg$parse_();
                      if (input.charCodeAt(peg$currPos) === 44) {
                        s7 = peg$c3;
                        peg$currPos++;
                      } else {
                        s7 = peg$FAILED;
                        if (peg$silentFails === 0) {
                          peg$fail(peg$e3);
                        }
                      }
                      if (s7 !== peg$FAILED) {
                        s8 = peg$parse_();
                        s9 = peg$parseN();
                        if (s9 !== peg$FAILED) {
                          s6 = [s6, s7, s8, s9];
                          s5 = s6;
                        } else {
                          peg$currPos = s5;
                          s5 = peg$FAILED;
                        }
                      } else {
                        peg$currPos = s5;
                        s5 = peg$FAILED;
                      }
                      while (s5 !== peg$FAILED) {
                        s4.push(s5);
                        s5 = peg$currPos;
                        s6 = peg$parse_();
                        if (input.charCodeAt(peg$currPos) === 44) {
                          s7 = peg$c3;
                          peg$currPos++;
                        } else {
                          s7 = peg$FAILED;
                          if (peg$silentFails === 0) {
                            peg$fail(peg$e3);
                          }
                        }
                        if (s7 !== peg$FAILED) {
                          s8 = peg$parse_();
                          s9 = peg$parseN();
                          if (s9 !== peg$FAILED) {
                            s6 = [s6, s7, s8, s9];
                            s5 = s6;
                          } else {
                            peg$currPos = s5;
                            s5 = peg$FAILED;
                          }
                        } else {
                          peg$currPos = s5;
                          s5 = peg$FAILED;
                        }
                      }
                      peg$savedPos = s0;
                      s0 = peg$f8(s3, s4);
                    } else {
                      peg$currPos = s0;
                      s0 = peg$FAILED;
                    }
                  } else {
                    peg$currPos = s0;
                    s0 = peg$FAILED;
                  }
                  if (s0 === peg$FAILED) {
                    s0 = peg$currPos;
                    s1 = peg$currPos;
                    if (input.substr(peg$currPos, 2) === peg$c5) {
                      s2 = peg$c5;
                      peg$currPos += 2;
                    } else {
                      s2 = peg$FAILED;
                      if (peg$silentFails === 0) {
                        peg$fail(peg$e5);
                      }
                    }
                    if (s2 === peg$FAILED) {
                      if (input.substr(peg$currPos, 2) === peg$c6) {
                        s2 = peg$c6;
                        peg$currPos += 2;
                      } else {
                        s2 = peg$FAILED;
                        if (peg$silentFails === 0) {
                          peg$fail(peg$e6);
                        }
                      }
                      if (s2 === peg$FAILED) {
                        s2 = input.charAt(peg$currPos);
                        if (peg$r0.test(s2)) {
                          peg$currPos++;
                        } else {
                          s2 = peg$FAILED;
                          if (peg$silentFails === 0) {
                            peg$fail(peg$e7);
                          }
                        }
                      }
                    }
                    if (s2 !== peg$FAILED) {
                      s1 = input.substring(s1, peg$currPos);
                    } else {
                      s1 = s2;
                    }
                    if (s1 !== peg$FAILED) {
                      s2 = peg$parse_();
                      s3 = peg$parseN();
                      if (s3 !== peg$FAILED) {
                        peg$savedPos = s0;
                        s0 = peg$f9(s1, s3);
                      } else {
                        peg$currPos = s0;
                        s0 = peg$FAILED;
                      }
                    } else {
                      peg$currPos = s0;
                      s0 = peg$FAILED;
                    }
                    if (s0 === peg$FAILED) {
                      s0 = peg$currPos;
                      s1 = peg$parseN();
                      if (s1 !== peg$FAILED) {
                        s2 = [];
                        s3 = peg$currPos;
                        s4 = peg$parse_();
                        if (input.charCodeAt(peg$currPos) === 44) {
                          s5 = peg$c3;
                          peg$currPos++;
                        } else {
                          s5 = peg$FAILED;
                          if (peg$silentFails === 0) {
                            peg$fail(peg$e3);
                          }
                        }
                        if (s5 !== peg$FAILED) {
                          s6 = peg$parse_();
                          s7 = peg$parseN();
                          if (s7 !== peg$FAILED) {
                            s4 = [s4, s5, s6, s7];
                            s3 = s4;
                          } else {
                            peg$currPos = s3;
                            s3 = peg$FAILED;
                          }
                        } else {
                          peg$currPos = s3;
                          s3 = peg$FAILED;
                        }
                        while (s3 !== peg$FAILED) {
                          s2.push(s3);
                          s3 = peg$currPos;
                          s4 = peg$parse_();
                          if (input.charCodeAt(peg$currPos) === 44) {
                            s5 = peg$c3;
                            peg$currPos++;
                          } else {
                            s5 = peg$FAILED;
                            if (peg$silentFails === 0) {
                              peg$fail(peg$e3);
                            }
                          }
                          if (s5 !== peg$FAILED) {
                            s6 = peg$parse_();
                            s7 = peg$parseN();
                            if (s7 !== peg$FAILED) {
                              s4 = [s4, s5, s6, s7];
                              s3 = s4;
                            } else {
                              peg$currPos = s3;
                              s3 = peg$FAILED;
                            }
                          } else {
                            peg$currPos = s3;
                            s3 = peg$FAILED;
                          }
                        }
                        peg$savedPos = s0;
                        s0 = peg$f10(s1, s2);
                      } else {
                        peg$currPos = s0;
                        s0 = peg$FAILED;
                      }
                    }
                  }
                }
              }
            }
          }
        }
        return s0;
      }
      function peg$parseopenInterval() {
        var s0, s1;
        s0 = peg$currPos;
        if (input.charCodeAt(peg$currPos) === 91) {
          s1 = peg$c7;
          peg$currPos++;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) {
            peg$fail(peg$e8);
          }
        }
        if (s1 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$f11();
        }
        s0 = s1;
        if (s0 === peg$FAILED) {
          s0 = peg$currPos;
          if (input.charCodeAt(peg$currPos) === 40) {
            s1 = peg$c0;
            peg$currPos++;
          } else {
            s1 = peg$FAILED;
            if (peg$silentFails === 0) {
              peg$fail(peg$e0);
            }
          }
          if (s1 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$f12();
          }
          s0 = s1;
        }
        return s0;
      }
      function peg$parsecloseInterval() {
        var s0, s1;
        s0 = peg$currPos;
        if (input.charCodeAt(peg$currPos) === 93) {
          s1 = peg$c8;
          peg$currPos++;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) {
            peg$fail(peg$e9);
          }
        }
        if (s1 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$f13();
        }
        s0 = s1;
        if (s0 === peg$FAILED) {
          s0 = peg$currPos;
          if (input.charCodeAt(peg$currPos) === 41) {
            s1 = peg$c1;
            peg$currPos++;
          } else {
            s1 = peg$FAILED;
            if (peg$silentFails === 0) {
              peg$fail(peg$e1);
            }
          }
          if (s1 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$f14();
          }
          s0 = s1;
        }
        return s0;
      }
      function peg$parseN() {
        var s0, s1, s2, s3, s4, s5, s6, s7, s8, s9;
        s0 = peg$currPos;
        s1 = peg$currPos;
        s2 = peg$currPos;
        if (input.charCodeAt(peg$currPos) === 45) {
          s3 = peg$c9;
          peg$currPos++;
        } else {
          s3 = peg$FAILED;
          if (peg$silentFails === 0) {
            peg$fail(peg$e10);
          }
        }
        if (s3 === peg$FAILED) {
          s3 = null;
        }
        s4 = peg$currPos;
        s5 = [];
        s6 = input.charAt(peg$currPos);
        if (peg$r1.test(s6)) {
          peg$currPos++;
        } else {
          s6 = peg$FAILED;
          if (peg$silentFails === 0) {
            peg$fail(peg$e11);
          }
        }
        if (s6 !== peg$FAILED) {
          while (s6 !== peg$FAILED) {
            s5.push(s6);
            s6 = input.charAt(peg$currPos);
            if (peg$r1.test(s6)) {
              peg$currPos++;
            } else {
              s6 = peg$FAILED;
              if (peg$silentFails === 0) {
                peg$fail(peg$e11);
              }
            }
          }
        } else {
          s5 = peg$FAILED;
        }
        if (s5 !== peg$FAILED) {
          if (input.charCodeAt(peg$currPos) === 46) {
            s6 = peg$c10;
            peg$currPos++;
          } else {
            s6 = peg$FAILED;
            if (peg$silentFails === 0) {
              peg$fail(peg$e12);
            }
          }
          if (s6 === peg$FAILED) {
            s6 = null;
          }
          s7 = [];
          s8 = input.charAt(peg$currPos);
          if (peg$r1.test(s8)) {
            peg$currPos++;
          } else {
            s8 = peg$FAILED;
            if (peg$silentFails === 0) {
              peg$fail(peg$e11);
            }
          }
          while (s8 !== peg$FAILED) {
            s7.push(s8);
            s8 = input.charAt(peg$currPos);
            if (peg$r1.test(s8)) {
              peg$currPos++;
            } else {
              s8 = peg$FAILED;
              if (peg$silentFails === 0) {
                peg$fail(peg$e11);
              }
            }
          }
          s5 = [s5, s6, s7];
          s4 = s5;
        } else {
          peg$currPos = s4;
          s4 = peg$FAILED;
        }
        if (s4 === peg$FAILED) {
          s4 = peg$currPos;
          s5 = [];
          s6 = input.charAt(peg$currPos);
          if (peg$r1.test(s6)) {
            peg$currPos++;
          } else {
            s6 = peg$FAILED;
            if (peg$silentFails === 0) {
              peg$fail(peg$e11);
            }
          }
          while (s6 !== peg$FAILED) {
            s5.push(s6);
            s6 = input.charAt(peg$currPos);
            if (peg$r1.test(s6)) {
              peg$currPos++;
            } else {
              s6 = peg$FAILED;
              if (peg$silentFails === 0) {
                peg$fail(peg$e11);
              }
            }
          }
          if (input.charCodeAt(peg$currPos) === 46) {
            s6 = peg$c10;
            peg$currPos++;
          } else {
            s6 = peg$FAILED;
            if (peg$silentFails === 0) {
              peg$fail(peg$e12);
            }
          }
          if (s6 !== peg$FAILED) {
            s7 = [];
            s8 = input.charAt(peg$currPos);
            if (peg$r1.test(s8)) {
              peg$currPos++;
            } else {
              s8 = peg$FAILED;
              if (peg$silentFails === 0) {
                peg$fail(peg$e11);
              }
            }
            if (s8 !== peg$FAILED) {
              while (s8 !== peg$FAILED) {
                s7.push(s8);
                s8 = input.charAt(peg$currPos);
                if (peg$r1.test(s8)) {
                  peg$currPos++;
                } else {
                  s8 = peg$FAILED;
                  if (peg$silentFails === 0) {
                    peg$fail(peg$e11);
                  }
                }
              }
            } else {
              s7 = peg$FAILED;
            }
            if (s7 !== peg$FAILED) {
              s5 = [s5, s6, s7];
              s4 = s5;
            } else {
              peg$currPos = s4;
              s4 = peg$FAILED;
            }
          } else {
            peg$currPos = s4;
            s4 = peg$FAILED;
          }
        }
        if (s4 !== peg$FAILED) {
          s5 = peg$currPos;
          s6 = input.charAt(peg$currPos);
          if (peg$r2.test(s6)) {
            peg$currPos++;
          } else {
            s6 = peg$FAILED;
            if (peg$silentFails === 0) {
              peg$fail(peg$e13);
            }
          }
          if (s6 !== peg$FAILED) {
            s7 = input.charAt(peg$currPos);
            if (peg$r3.test(s7)) {
              peg$currPos++;
            } else {
              s7 = peg$FAILED;
              if (peg$silentFails === 0) {
                peg$fail(peg$e14);
              }
            }
            if (s7 === peg$FAILED) {
              s7 = null;
            }
            s8 = [];
            s9 = input.charAt(peg$currPos);
            if (peg$r1.test(s9)) {
              peg$currPos++;
            } else {
              s9 = peg$FAILED;
              if (peg$silentFails === 0) {
                peg$fail(peg$e11);
              }
            }
            if (s9 !== peg$FAILED) {
              while (s9 !== peg$FAILED) {
                s8.push(s9);
                s9 = input.charAt(peg$currPos);
                if (peg$r1.test(s9)) {
                  peg$currPos++;
                } else {
                  s9 = peg$FAILED;
                  if (peg$silentFails === 0) {
                    peg$fail(peg$e11);
                  }
                }
              }
            } else {
              s8 = peg$FAILED;
            }
            if (s8 !== peg$FAILED) {
              s6 = [s6, s7, s8];
              s5 = s6;
            } else {
              peg$currPos = s5;
              s5 = peg$FAILED;
            }
          } else {
            peg$currPos = s5;
            s5 = peg$FAILED;
          }
          if (s5 === peg$FAILED) {
            s5 = null;
          }
          s3 = [s3, s4, s5];
          s2 = s3;
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          s1 = input.substring(s1, peg$currPos);
        } else {
          s1 = s2;
        }
        if (s1 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$f15(s1);
        }
        s0 = s1;
        return s0;
      }
      function peg$parseconjunction() {
        var s0, s1;
        s0 = peg$currPos;
        s1 = peg$parseOR();
        if (s1 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$f16();
        }
        s0 = s1;
        if (s0 === peg$FAILED) {
          s0 = peg$currPos;
          s1 = peg$parseAND();
          if (s1 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$f17();
          }
          s0 = s1;
        }
        return s0;
      }
      function peg$parseNOT() {
        var s0, s1, s2, s3;
        s0 = peg$currPos;
        s1 = input.substr(peg$currPos, 3);
        if (s1.toLowerCase() === peg$c11) {
          peg$currPos += 3;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) {
            peg$fail(peg$e15);
          }
        }
        if (s1 !== peg$FAILED) {
          s2 = peg$currPos;
          peg$silentFails++;
          s3 = peg$parseidChar();
          peg$silentFails--;
          if (s3 === peg$FAILED) {
            s2 = void 0;
          } else {
            peg$currPos = s2;
            s2 = peg$FAILED;
          }
          if (s2 !== peg$FAILED) {
            peg$savedPos = s0;
            s0 = peg$f18();
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
        return s0;
      }
      function peg$parseNULL() {
        var s0, s1, s2, s3;
        s0 = peg$currPos;
        s1 = input.substr(peg$currPos, 4);
        if (s1.toLowerCase() === peg$c12) {
          peg$currPos += 4;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) {
            peg$fail(peg$e16);
          }
        }
        if (s1 !== peg$FAILED) {
          s2 = peg$currPos;
          peg$silentFails++;
          s3 = peg$parseidChar();
          peg$silentFails--;
          if (s3 === peg$FAILED) {
            s2 = void 0;
          } else {
            peg$currPos = s2;
            s2 = peg$FAILED;
          }
          if (s2 !== peg$FAILED) {
            s1 = [s1, s2];
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
        return s0;
      }
      function peg$parseNONE() {
        var s0, s1, s2, s3;
        s0 = peg$currPos;
        s1 = input.substr(peg$currPos, 4);
        if (s1.toLowerCase() === peg$c13) {
          peg$currPos += 4;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) {
            peg$fail(peg$e17);
          }
        }
        if (s1 !== peg$FAILED) {
          s2 = peg$currPos;
          peg$silentFails++;
          s3 = peg$parseidChar();
          peg$silentFails--;
          if (s3 === peg$FAILED) {
            s2 = void 0;
          } else {
            peg$currPos = s2;
            s2 = peg$FAILED;
          }
          if (s2 !== peg$FAILED) {
            s1 = [s1, s2];
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
        return s0;
      }
      function peg$parseAND() {
        var s0, s1, s2, s3;
        s0 = peg$currPos;
        s1 = input.substr(peg$currPos, 3);
        if (s1.toLowerCase() === peg$c14) {
          peg$currPos += 3;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) {
            peg$fail(peg$e18);
          }
        }
        if (s1 !== peg$FAILED) {
          s2 = peg$currPos;
          peg$silentFails++;
          s3 = peg$parseidChar();
          peg$silentFails--;
          if (s3 === peg$FAILED) {
            s2 = void 0;
          } else {
            peg$currPos = s2;
            s2 = peg$FAILED;
          }
          if (s2 !== peg$FAILED) {
            s1 = [s1, s2];
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
        return s0;
      }
      function peg$parseOR() {
        var s0, s1, s2, s3;
        s0 = peg$currPos;
        s1 = input.substr(peg$currPos, 2);
        if (s1.toLowerCase() === peg$c15) {
          peg$currPos += 2;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) {
            peg$fail(peg$e19);
          }
        }
        if (s1 !== peg$FAILED) {
          s2 = peg$currPos;
          peg$silentFails++;
          s3 = peg$parseidChar();
          peg$silentFails--;
          if (s3 === peg$FAILED) {
            s2 = void 0;
          } else {
            peg$currPos = s2;
            s2 = peg$FAILED;
          }
          if (s2 !== peg$FAILED) {
            s1 = [s1, s2];
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
        return s0;
      }
      function peg$parseTO() {
        var s0, s1, s2, s3;
        s0 = peg$currPos;
        s1 = input.substr(peg$currPos, 2);
        if (s1.toLowerCase() === peg$c16) {
          peg$currPos += 2;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) {
            peg$fail(peg$e20);
          }
        }
        if (s1 !== peg$FAILED) {
          s2 = peg$currPos;
          peg$silentFails++;
          s3 = peg$parseidChar();
          peg$silentFails--;
          if (s3 === peg$FAILED) {
            s2 = void 0;
          } else {
            peg$currPos = s2;
            s2 = peg$FAILED;
          }
          if (s2 !== peg$FAILED) {
            s1 = [s1, s2];
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
        return s0;
      }
      function peg$parseidChar() {
        var s0;
        s0 = input.charAt(peg$currPos);
        if (peg$r4.test(s0)) {
          peg$currPos++;
        } else {
          s0 = peg$FAILED;
          if (peg$silentFails === 0) {
            peg$fail(peg$e21);
          }
        }
        return s0;
      }
      function peg$parse_() {
        var s0, s1;
        peg$silentFails++;
        s0 = [];
        s1 = input.charAt(peg$currPos);
        if (peg$r5.test(s1)) {
          peg$currPos++;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) {
            peg$fail(peg$e23);
          }
        }
        while (s1 !== peg$FAILED) {
          s0.push(s1);
          s1 = input.charAt(peg$currPos);
          if (peg$r5.test(s1)) {
            peg$currPos++;
          } else {
            s1 = peg$FAILED;
            if (peg$silentFails === 0) {
              peg$fail(peg$e23);
            }
          }
        }
        peg$silentFails--;
        s1 = peg$FAILED;
        if (peg$silentFails === 0) {
          peg$fail(peg$e22);
        }
        return s0;
      }
      peg$result = peg$startRuleFunction();
      if (options.peg$library) {
        return (
          /** @type {any} */
          {
            peg$result,
            peg$currPos,
            peg$FAILED,
            peg$maxFailExpected,
            peg$maxFailPos
          }
        );
      }
      if (peg$result !== peg$FAILED && peg$currPos === input.length) {
        return peg$result;
      } else {
        if (peg$result !== peg$FAILED && peg$currPos < input.length) {
          peg$fail(peg$endExpectation());
        }
        throw peg$buildStructuredError(peg$maxFailExpected, peg$maxFailPos < input.length ? input.charAt(peg$maxFailPos) : null, peg$maxFailPos < input.length ? peg$computeLocation(peg$maxFailPos, peg$maxFailPos + 1) : peg$computeLocation(peg$maxFailPos, peg$maxFailPos));
      }
    }
    module.exports = {
      StartRules: ["numberFilter"],
      SyntaxError: peg$SyntaxError,
      parse: peg$parse
    };
  }
});

// node_modules/@malloydata/malloy-filter/dist/peggy_parse.js
var require_peggy_parse = __commonJS({
  "node_modules/@malloydata/malloy-filter/dist/peggy_parse.js"(exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    exports.run_parser = run_parser;
    var filter_interface_1 = require_filter_interface();
    function isPeggySyntaxError(e) {
      return e instanceof Error && "location" in e;
    }
    function run_parser(src, parse) {
      try {
        const expr = parse(src);
        if ((0, filter_interface_1.isFilterExpression)(expr)) {
          return { parsed: expr, log: [] };
        }
        return { parsed: null, log: [] };
      } catch (e) {
        if (isPeggySyntaxError(e)) {
          const loc = e.location;
          const startIndex = loc ? loc.start.offset : 0;
          const endIndex = loc ? loc.end.offset - 1 : src.length - 1;
          return {
            parsed: null,
            log: [
              {
                message: e.message,
                startIndex,
                endIndex: Math.max(startIndex, endIndex),
                severity: "error"
              }
            ]
          };
        }
        throw e;
      }
    }
  }
});

// node_modules/@malloydata/malloy-filter/dist/number_filter_expression.js
var require_number_filter_expression = __commonJS({
  "node_modules/@malloydata/malloy-filter/dist/number_filter_expression.js"(exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    exports.NumberFilterExpression = void 0;
    var filter_interface_1 = require_filter_interface();
    var fexpr_number_parser_1 = require_fexpr_number_parser();
    var peggy_parse_1 = require_peggy_parse();
    exports.NumberFilterExpression = {
      parse(src) {
        if (src.match(/^\s*$/)) {
          return { parsed: null, log: [] };
        }
        const parse_result = (0, peggy_parse_1.run_parser)(src, fexpr_number_parser_1.parse);
        if (parse_result.parsed && (0, filter_interface_1.isNumberFilter)(parse_result.parsed)) {
          return { parsed: parse_result.parsed, log: [] };
        }
        return { parsed: null, log: parse_result.log };
      },
      unparse(nc) {
        if (nc === null) {
          return "";
        }
        switch (nc.operator) {
          case "=":
            return nc.values.join(", ");
          case "!=":
            return "!= " + nc.values.join(", ");
          case ">":
          case "<":
          case "<=":
          case ">=":
            if (nc.not) {
              return nc.values.map((v) => nc.operator === "=" ? `not ${v}` : `not ${nc.operator} ${v}`).join(", ");
            }
            return nc.values.map((v) => `${nc.operator} ${v}`).join(", ");
          case "range": {
            const left = nc.startOperator === ">" ? "(" : "[";
            const right = nc.endOperator === "<" ? ")" : "]";
            const rExpr = `${left}${nc.startValue} to ${nc.endValue}${right}`;
            return nc.not ? `not ${rExpr}` : rExpr;
          }
          case "null": {
            return nc.not ? "not null" : "null";
          }
          case "none": {
            return nc.not ? "not none" : "none";
          }
          case "and":
          case "or":
            return nc.members.map((m) => exports.NumberFilterExpression.unparse(m)).join(` ${nc.operator} `);
          case "()": {
            const expr = "(" + exports.NumberFilterExpression.unparse(nc.expr) + ")";
            return nc.not ? "not " + expr : expr;
          }
        }
        return `no unparse for ${JSON.stringify(nc)}`;
      }
    };
  }
});

// node_modules/@malloydata/malloy-filter/dist/lib/fexpr_string_parser.js
var require_fexpr_string_parser = __commonJS({
  "node_modules/@malloydata/malloy-filter/dist/lib/fexpr_string_parser.js"(exports, module) {
    "use strict";
    var { conjoin, maybeNot, matchOp } = require_clause_utils();
    function peg$subclass(child, parent) {
      function C() {
        this.constructor = child;
      }
      C.prototype = parent.prototype;
      child.prototype = new C();
    }
    function peg$SyntaxError(message, expected, found, location) {
      var self = Error.call(this, message);
      if (Object.setPrototypeOf) {
        Object.setPrototypeOf(self, peg$SyntaxError.prototype);
      }
      self.expected = expected;
      self.found = found;
      self.location = location;
      self.name = "SyntaxError";
      return self;
    }
    peg$subclass(peg$SyntaxError, Error);
    function peg$padEnd(str, targetLength, padString) {
      padString = padString || " ";
      if (str.length > targetLength) {
        return str;
      }
      targetLength -= str.length;
      padString += padString.repeat(targetLength);
      return str + padString.slice(0, targetLength);
    }
    peg$SyntaxError.prototype.format = function(sources) {
      var str = "Error: " + this.message;
      if (this.location) {
        var src = null;
        var k;
        for (k = 0; k < sources.length; k++) {
          if (sources[k].source === this.location.source) {
            src = sources[k].text.split(/\r\n|\n|\r/g);
            break;
          }
        }
        var s = this.location.start;
        var offset_s = this.location.source && typeof this.location.source.offset === "function" ? this.location.source.offset(s) : s;
        var loc = this.location.source + ":" + offset_s.line + ":" + offset_s.column;
        if (src) {
          var e = this.location.end;
          var filler = peg$padEnd("", offset_s.line.toString().length, " ");
          var line = src[s.line - 1];
          var last = s.line === e.line ? e.column : line.length + 1;
          var hatLen = last - s.column || 1;
          str += "\n --> " + loc + "\n" + filler + " |\n" + offset_s.line + " | " + line + "\n" + filler + " | " + peg$padEnd("", s.column - 1, " ") + peg$padEnd("", hatLen, "^");
        } else {
          str += "\n at " + loc;
        }
      }
      return str;
    };
    peg$SyntaxError.buildMessage = function(expected, found) {
      var DESCRIBE_EXPECTATION_FNS = {
        literal: function(expectation) {
          return '"' + literalEscape(expectation.text) + '"';
        },
        class: function(expectation) {
          var escapedParts = expectation.parts.map(function(part) {
            return Array.isArray(part) ? classEscape(part[0]) + "-" + classEscape(part[1]) : classEscape(part);
          });
          return "[" + (expectation.inverted ? "^" : "") + escapedParts.join("") + "]";
        },
        any: function() {
          return "any character";
        },
        end: function() {
          return "end of input";
        },
        other: function(expectation) {
          return expectation.description;
        }
      };
      function hex(ch) {
        return ch.charCodeAt(0).toString(16).toUpperCase();
      }
      function literalEscape(s) {
        return s.replace(/\\/g, "\\\\").replace(/"/g, '\\"').replace(/\0/g, "\\0").replace(/\t/g, "\\t").replace(/\n/g, "\\n").replace(/\r/g, "\\r").replace(/[\x00-\x0F]/g, function(ch) {
          return "\\x0" + hex(ch);
        }).replace(/[\x10-\x1F\x7F-\x9F]/g, function(ch) {
          return "\\x" + hex(ch);
        });
      }
      function classEscape(s) {
        return s.replace(/\\/g, "\\\\").replace(/\]/g, "\\]").replace(/\^/g, "\\^").replace(/-/g, "\\-").replace(/\0/g, "\\0").replace(/\t/g, "\\t").replace(/\n/g, "\\n").replace(/\r/g, "\\r").replace(/[\x00-\x0F]/g, function(ch) {
          return "\\x0" + hex(ch);
        }).replace(/[\x10-\x1F\x7F-\x9F]/g, function(ch) {
          return "\\x" + hex(ch);
        });
      }
      function describeExpectation(expectation) {
        return DESCRIBE_EXPECTATION_FNS[expectation.type](expectation);
      }
      function describeExpected(expected2) {
        var descriptions = expected2.map(describeExpectation);
        var i, j;
        descriptions.sort();
        if (descriptions.length > 0) {
          for (i = 1, j = 1; i < descriptions.length; i++) {
            if (descriptions[i - 1] !== descriptions[i]) {
              descriptions[j] = descriptions[i];
              j++;
            }
          }
          descriptions.length = j;
        }
        switch (descriptions.length) {
          case 1:
            return descriptions[0];
          case 2:
            return descriptions[0] + " or " + descriptions[1];
          default:
            return descriptions.slice(0, -1).join(", ") + ", or " + descriptions[descriptions.length - 1];
        }
      }
      function describeFound(found2) {
        return found2 ? '"' + literalEscape(found2) + '"' : "end of input";
      }
      return "Expected " + describeExpected(expected) + " but " + describeFound(found) + " found.";
    };
    function peg$parse(input, options) {
      options = options !== void 0 ? options : {};
      var peg$FAILED = {};
      var peg$source = options.grammarSource;
      var peg$startRuleFunctions = { stringFilter: peg$parsestringFilter };
      var peg$startRuleFunction = peg$parsestringFilter;
      var peg$c0 = "-";
      var peg$c1 = "(";
      var peg$c2 = ")";
      var peg$c3 = "\\";
      var peg$c4 = ",";
      var peg$c5 = ";";
      var peg$c6 = "|";
      var peg$r0 = /^[^\n,;()|]/;
      var peg$r1 = /^[ \t]/;
      var peg$e0 = peg$literalExpectation("-", false);
      var peg$e1 = peg$literalExpectation("(", false);
      var peg$e2 = peg$literalExpectation(")", false);
      var peg$e3 = peg$otherExpectation("match string");
      var peg$e4 = peg$literalExpectation("\\", false);
      var peg$e5 = peg$anyExpectation();
      var peg$e6 = peg$classExpectation(["\n", ",", ";", "(", ")", "|"], true, false);
      var peg$e7 = peg$literalExpectation(",", false);
      var peg$e8 = peg$literalExpectation(";", false);
      var peg$e9 = peg$literalExpectation("|", false);
      var peg$e10 = peg$otherExpectation("whitespace");
      var peg$e11 = peg$classExpectation([" ", "	"], false, false);
      var peg$f0 = function(head, tail) {
        return tail.reduce((left, [, cop, , right]) => conjoin(left, cop, right), head);
      };
      var peg$f1 = function(clause) {
        return maybeNot([true, clause]);
      };
      var peg$f2 = function(clause) {
        return clause;
      };
      var peg$f3 = function(expr) {
        return { operator: "()", expr };
      };
      var peg$f4 = function(str) {
        return matchOp(str);
      };
      var peg$f5 = function(s) {
        return s;
      };
      var peg$f6 = function() {
        return ",";
      };
      var peg$f7 = function() {
        return ";";
      };
      var peg$f8 = function() {
        return "|";
      };
      var peg$currPos = options.peg$currPos | 0;
      var peg$savedPos = peg$currPos;
      var peg$posDetailsCache = [{ line: 1, column: 1 }];
      var peg$maxFailPos = peg$currPos;
      var peg$maxFailExpected = options.peg$maxFailExpected || [];
      var peg$silentFails = options.peg$silentFails | 0;
      var peg$result;
      if (options.startRule) {
        if (!(options.startRule in peg$startRuleFunctions)) {
          throw new Error(`Can't start parsing from rule "` + options.startRule + '".');
        }
        peg$startRuleFunction = peg$startRuleFunctions[options.startRule];
      }
      function text() {
        return input.substring(peg$savedPos, peg$currPos);
      }
      function offset() {
        return peg$savedPos;
      }
      function range() {
        return {
          source: peg$source,
          start: peg$savedPos,
          end: peg$currPos
        };
      }
      function location() {
        return peg$computeLocation(peg$savedPos, peg$currPos);
      }
      function expected(description, location2) {
        location2 = location2 !== void 0 ? location2 : peg$computeLocation(peg$savedPos, peg$currPos);
        throw peg$buildStructuredError([peg$otherExpectation(description)], input.substring(peg$savedPos, peg$currPos), location2);
      }
      function error(message, location2) {
        location2 = location2 !== void 0 ? location2 : peg$computeLocation(peg$savedPos, peg$currPos);
        throw peg$buildSimpleError(message, location2);
      }
      function peg$literalExpectation(text2, ignoreCase) {
        return { type: "literal", text: text2, ignoreCase };
      }
      function peg$classExpectation(parts, inverted, ignoreCase) {
        return { type: "class", parts, inverted, ignoreCase };
      }
      function peg$anyExpectation() {
        return { type: "any" };
      }
      function peg$endExpectation() {
        return { type: "end" };
      }
      function peg$otherExpectation(description) {
        return { type: "other", description };
      }
      function peg$computePosDetails(pos) {
        var details = peg$posDetailsCache[pos];
        var p;
        if (details) {
          return details;
        } else {
          if (pos >= peg$posDetailsCache.length) {
            p = peg$posDetailsCache.length - 1;
          } else {
            p = pos;
            while (!peg$posDetailsCache[--p]) {
            }
          }
          details = peg$posDetailsCache[p];
          details = {
            line: details.line,
            column: details.column
          };
          while (p < pos) {
            if (input.charCodeAt(p) === 10) {
              details.line++;
              details.column = 1;
            } else {
              details.column++;
            }
            p++;
          }
          peg$posDetailsCache[pos] = details;
          return details;
        }
      }
      function peg$computeLocation(startPos, endPos, offset2) {
        var startPosDetails = peg$computePosDetails(startPos);
        var endPosDetails = peg$computePosDetails(endPos);
        var res = {
          source: peg$source,
          start: {
            offset: startPos,
            line: startPosDetails.line,
            column: startPosDetails.column
          },
          end: {
            offset: endPos,
            line: endPosDetails.line,
            column: endPosDetails.column
          }
        };
        if (offset2 && peg$source && typeof peg$source.offset === "function") {
          res.start = peg$source.offset(res.start);
          res.end = peg$source.offset(res.end);
        }
        return res;
      }
      function peg$fail(expected2) {
        if (peg$currPos < peg$maxFailPos) {
          return;
        }
        if (peg$currPos > peg$maxFailPos) {
          peg$maxFailPos = peg$currPos;
          peg$maxFailExpected = [];
        }
        peg$maxFailExpected.push(expected2);
      }
      function peg$buildSimpleError(message, location2) {
        return new peg$SyntaxError(message, null, null, location2);
      }
      function peg$buildStructuredError(expected2, found, location2) {
        return new peg$SyntaxError(peg$SyntaxError.buildMessage(expected2, found), expected2, found, location2);
      }
      function peg$parsestringFilter() {
        var s0, s1, s2, s3, s4, s5, s6, s7, s8;
        s0 = peg$currPos;
        s1 = peg$parse_();
        s2 = peg$parsesfUnary();
        if (s2 !== peg$FAILED) {
          s3 = [];
          s4 = peg$currPos;
          s5 = peg$parse_();
          s6 = peg$parseconjunction();
          if (s6 !== peg$FAILED) {
            s7 = peg$parse_();
            s8 = peg$parsesfUnary();
            if (s8 !== peg$FAILED) {
              s5 = [s5, s6, s7, s8];
              s4 = s5;
            } else {
              peg$currPos = s4;
              s4 = peg$FAILED;
            }
          } else {
            peg$currPos = s4;
            s4 = peg$FAILED;
          }
          while (s4 !== peg$FAILED) {
            s3.push(s4);
            s4 = peg$currPos;
            s5 = peg$parse_();
            s6 = peg$parseconjunction();
            if (s6 !== peg$FAILED) {
              s7 = peg$parse_();
              s8 = peg$parsesfUnary();
              if (s8 !== peg$FAILED) {
                s5 = [s5, s6, s7, s8];
                s4 = s5;
              } else {
                peg$currPos = s4;
                s4 = peg$FAILED;
              }
            } else {
              peg$currPos = s4;
              s4 = peg$FAILED;
            }
          }
          peg$savedPos = s0;
          s0 = peg$f0(s2, s3);
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
        return s0;
      }
      function peg$parsesfUnary() {
        var s0, s1, s2, s3;
        s0 = peg$currPos;
        if (input.charCodeAt(peg$currPos) === 45) {
          s1 = peg$c0;
          peg$currPos++;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) {
            peg$fail(peg$e0);
          }
        }
        if (s1 !== peg$FAILED) {
          s2 = peg$parse_();
          s3 = peg$parseclause();
          if (s3 !== peg$FAILED) {
            peg$savedPos = s0;
            s0 = peg$f1(s3);
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
        if (s0 === peg$FAILED) {
          s0 = peg$currPos;
          s1 = peg$parseclause();
          if (s1 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$f2(s1);
          }
          s0 = s1;
        }
        return s0;
      }
      function peg$parseclause() {
        var s0, s1, s2, s3, s4, s5;
        s0 = peg$currPos;
        if (input.charCodeAt(peg$currPos) === 40) {
          s1 = peg$c1;
          peg$currPos++;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) {
            peg$fail(peg$e1);
          }
        }
        if (s1 !== peg$FAILED) {
          s2 = peg$parse_();
          s3 = peg$parsestringFilter();
          if (s3 !== peg$FAILED) {
            s4 = peg$parse_();
            if (input.charCodeAt(peg$currPos) === 41) {
              s5 = peg$c2;
              peg$currPos++;
            } else {
              s5 = peg$FAILED;
              if (peg$silentFails === 0) {
                peg$fail(peg$e2);
              }
            }
            if (s5 !== peg$FAILED) {
              peg$savedPos = s0;
              s0 = peg$f3(s3);
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
        if (s0 === peg$FAILED) {
          s0 = peg$currPos;
          s1 = peg$parsematchStr();
          if (s1 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$f4(s1);
          }
          s0 = s1;
        }
        return s0;
      }
      function peg$parsematchStr() {
        var s0, s1, s2, s3, s4, s5;
        peg$silentFails++;
        s0 = peg$currPos;
        s1 = peg$currPos;
        s2 = [];
        s3 = peg$currPos;
        if (input.charCodeAt(peg$currPos) === 92) {
          s4 = peg$c3;
          peg$currPos++;
        } else {
          s4 = peg$FAILED;
          if (peg$silentFails === 0) {
            peg$fail(peg$e4);
          }
        }
        if (s4 !== peg$FAILED) {
          if (input.length > peg$currPos) {
            s5 = input.charAt(peg$currPos);
            peg$currPos++;
          } else {
            s5 = peg$FAILED;
            if (peg$silentFails === 0) {
              peg$fail(peg$e5);
            }
          }
          if (s5 !== peg$FAILED) {
            s4 = [s4, s5];
            s3 = s4;
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
        } else {
          peg$currPos = s3;
          s3 = peg$FAILED;
        }
        if (s3 === peg$FAILED) {
          s3 = input.charAt(peg$currPos);
          if (peg$r0.test(s3)) {
            peg$currPos++;
          } else {
            s3 = peg$FAILED;
            if (peg$silentFails === 0) {
              peg$fail(peg$e6);
            }
          }
        }
        if (s3 !== peg$FAILED) {
          while (s3 !== peg$FAILED) {
            s2.push(s3);
            s3 = peg$currPos;
            if (input.charCodeAt(peg$currPos) === 92) {
              s4 = peg$c3;
              peg$currPos++;
            } else {
              s4 = peg$FAILED;
              if (peg$silentFails === 0) {
                peg$fail(peg$e4);
              }
            }
            if (s4 !== peg$FAILED) {
              if (input.length > peg$currPos) {
                s5 = input.charAt(peg$currPos);
                peg$currPos++;
              } else {
                s5 = peg$FAILED;
                if (peg$silentFails === 0) {
                  peg$fail(peg$e5);
                }
              }
              if (s5 !== peg$FAILED) {
                s4 = [s4, s5];
                s3 = s4;
              } else {
                peg$currPos = s3;
                s3 = peg$FAILED;
              }
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
            if (s3 === peg$FAILED) {
              s3 = input.charAt(peg$currPos);
              if (peg$r0.test(s3)) {
                peg$currPos++;
              } else {
                s3 = peg$FAILED;
                if (peg$silentFails === 0) {
                  peg$fail(peg$e6);
                }
              }
            }
          }
        } else {
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          s1 = input.substring(s1, peg$currPos);
        } else {
          s1 = s2;
        }
        if (s1 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$f5(s1);
        }
        s0 = s1;
        peg$silentFails--;
        if (s0 === peg$FAILED) {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) {
            peg$fail(peg$e3);
          }
        }
        return s0;
      }
      function peg$parseconjunction() {
        var s0, s1;
        s0 = peg$currPos;
        if (input.charCodeAt(peg$currPos) === 44) {
          s1 = peg$c4;
          peg$currPos++;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) {
            peg$fail(peg$e7);
          }
        }
        if (s1 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$f6();
        }
        s0 = s1;
        if (s0 === peg$FAILED) {
          s0 = peg$currPos;
          if (input.charCodeAt(peg$currPos) === 59) {
            s1 = peg$c5;
            peg$currPos++;
          } else {
            s1 = peg$FAILED;
            if (peg$silentFails === 0) {
              peg$fail(peg$e8);
            }
          }
          if (s1 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$f7();
          }
          s0 = s1;
          if (s0 === peg$FAILED) {
            s0 = peg$currPos;
            if (input.charCodeAt(peg$currPos) === 124) {
              s1 = peg$c6;
              peg$currPos++;
            } else {
              s1 = peg$FAILED;
              if (peg$silentFails === 0) {
                peg$fail(peg$e9);
              }
            }
            if (s1 !== peg$FAILED) {
              peg$savedPos = s0;
              s1 = peg$f8();
            }
            s0 = s1;
          }
        }
        return s0;
      }
      function peg$parse_() {
        var s0, s1;
        peg$silentFails++;
        s0 = [];
        s1 = input.charAt(peg$currPos);
        if (peg$r1.test(s1)) {
          peg$currPos++;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) {
            peg$fail(peg$e11);
          }
        }
        while (s1 !== peg$FAILED) {
          s0.push(s1);
          s1 = input.charAt(peg$currPos);
          if (peg$r1.test(s1)) {
            peg$currPos++;
          } else {
            s1 = peg$FAILED;
            if (peg$silentFails === 0) {
              peg$fail(peg$e11);
            }
          }
        }
        peg$silentFails--;
        s1 = peg$FAILED;
        if (peg$silentFails === 0) {
          peg$fail(peg$e10);
        }
        return s0;
      }
      peg$result = peg$startRuleFunction();
      if (options.peg$library) {
        return (
          /** @type {any} */
          {
            peg$result,
            peg$currPos,
            peg$FAILED,
            peg$maxFailExpected,
            peg$maxFailPos
          }
        );
      }
      if (peg$result !== peg$FAILED && peg$currPos === input.length) {
        return peg$result;
      } else {
        if (peg$result !== peg$FAILED && peg$currPos < input.length) {
          peg$fail(peg$endExpectation());
        }
        throw peg$buildStructuredError(peg$maxFailExpected, peg$maxFailPos < input.length ? input.charAt(peg$maxFailPos) : null, peg$maxFailPos < input.length ? peg$computeLocation(peg$maxFailPos, peg$maxFailPos + 1) : peg$computeLocation(peg$maxFailPos, peg$maxFailPos));
      }
    }
    module.exports = {
      StartRules: ["stringFilter"],
      SyntaxError: peg$SyntaxError,
      parse: peg$parse
    };
  }
});

// node_modules/@malloydata/malloy-filter/dist/string_filter_expression.js
var require_string_filter_expression = __commonJS({
  "node_modules/@malloydata/malloy-filter/dist/string_filter_expression.js"(exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    exports.StringFilterExpression = void 0;
    var filter_interface_1 = require_filter_interface();
    var fexpr_string_parser_1 = require_fexpr_string_parser();
    var clause_utils_1 = require_clause_utils();
    var peggy_parse_1 = require_peggy_parse();
    exports.StringFilterExpression = {
      parse(src) {
        if (src.match(/^\s*$/)) {
          return { parsed: null, log: [] };
        }
        const parse_result = (0, peggy_parse_1.run_parser)(src, fexpr_string_parser_1.parse);
        if (parse_result.parsed && (0, filter_interface_1.isStringFilter)(parse_result.parsed)) {
          return { parsed: parse_result.parsed, log: [] };
        }
        return { parsed: null, log: parse_result.log };
      },
      unparse(sc) {
        if (sc === null) {
          return "";
        }
        switch (sc.operator) {
          case "=":
            if (sc.not) {
              return sc.values.map((s) => "-" + (0, clause_utils_1.escape)(s)).join(", ");
            }
            return sc.values.map((s) => (0, clause_utils_1.escape)(s)).join(", ");
          case "~":
            if (sc.not) {
              return sc.escaped_values.map((s) => "-" + s).join(", ");
            }
            return sc.escaped_values.join(", ");
          case "starts":
            if (sc.not) {
              return sc.values.map((s) => "-" + (0, clause_utils_1.escape)(s) + "%").join(", ");
            }
            return sc.values.map((s) => (0, clause_utils_1.escape)(s) + "%").join(", ");
          case "ends":
            if (sc.not) {
              return sc.values.map((s) => "-%" + (0, clause_utils_1.escape)(s)).join(", ");
            }
            return sc.values.map((s) => "%" + (0, clause_utils_1.escape)(s)).join(", ");
          case "contains":
            if (sc.not) {
              return sc.values.map((s) => "-%" + (0, clause_utils_1.escape)(s) + "%").join(", ");
            }
            return sc.values.map((s) => "%" + (0, clause_utils_1.escape)(s) + "%").join(", ");
          case "or":
            return sc.members.map((or) => exports.StringFilterExpression.unparse(or)).join(" | ");
          case "and":
            return sc.members.map((or) => exports.StringFilterExpression.unparse(or)).join("; ");
          case ",":
            return sc.members.map((or) => exports.StringFilterExpression.unparse(or)).join(", ");
          case "()": {
            const expr = "(" + exports.StringFilterExpression.unparse(sc.expr) + ")";
            return sc.not ? "-" + expr : expr;
          }
          case "null":
            return sc.not ? "-null" : "null";
          case "empty":
            return sc.not ? "-empty" : "empty";
          case "none":
            return sc.not ? "-none" : "none";
        }
      }
    };
  }
});

// node_modules/@malloydata/malloy-filter/dist/lib/ftemporal_parser.js
var require_ftemporal_parser = __commonJS({
  "node_modules/@malloydata/malloy-filter/dist/lib/ftemporal_parser.js"(exports, module) {
    "use strict";
    var { temporalNot, joinTemporal, timeLiteral, mkUnits } = require_clause_utils();
    function peg$subclass(child, parent) {
      function C() {
        this.constructor = child;
      }
      C.prototype = parent.prototype;
      child.prototype = new C();
    }
    function peg$SyntaxError(message, expected, found, location) {
      var self = Error.call(this, message);
      if (Object.setPrototypeOf) {
        Object.setPrototypeOf(self, peg$SyntaxError.prototype);
      }
      self.expected = expected;
      self.found = found;
      self.location = location;
      self.name = "SyntaxError";
      return self;
    }
    peg$subclass(peg$SyntaxError, Error);
    function peg$padEnd(str, targetLength, padString) {
      padString = padString || " ";
      if (str.length > targetLength) {
        return str;
      }
      targetLength -= str.length;
      padString += padString.repeat(targetLength);
      return str + padString.slice(0, targetLength);
    }
    peg$SyntaxError.prototype.format = function(sources) {
      var str = "Error: " + this.message;
      if (this.location) {
        var src = null;
        var k;
        for (k = 0; k < sources.length; k++) {
          if (sources[k].source === this.location.source) {
            src = sources[k].text.split(/\r\n|\n|\r/g);
            break;
          }
        }
        var s = this.location.start;
        var offset_s = this.location.source && typeof this.location.source.offset === "function" ? this.location.source.offset(s) : s;
        var loc = this.location.source + ":" + offset_s.line + ":" + offset_s.column;
        if (src) {
          var e = this.location.end;
          var filler = peg$padEnd("", offset_s.line.toString().length, " ");
          var line = src[s.line - 1];
          var last = s.line === e.line ? e.column : line.length + 1;
          var hatLen = last - s.column || 1;
          str += "\n --> " + loc + "\n" + filler + " |\n" + offset_s.line + " | " + line + "\n" + filler + " | " + peg$padEnd("", s.column - 1, " ") + peg$padEnd("", hatLen, "^");
        } else {
          str += "\n at " + loc;
        }
      }
      return str;
    };
    peg$SyntaxError.buildMessage = function(expected, found) {
      var DESCRIBE_EXPECTATION_FNS = {
        literal: function(expectation) {
          return '"' + literalEscape(expectation.text) + '"';
        },
        class: function(expectation) {
          var escapedParts = expectation.parts.map(function(part) {
            return Array.isArray(part) ? classEscape(part[0]) + "-" + classEscape(part[1]) : classEscape(part);
          });
          return "[" + (expectation.inverted ? "^" : "") + escapedParts.join("") + "]";
        },
        any: function() {
          return "any character";
        },
        end: function() {
          return "end of input";
        },
        other: function(expectation) {
          return expectation.description;
        }
      };
      function hex(ch) {
        return ch.charCodeAt(0).toString(16).toUpperCase();
      }
      function literalEscape(s) {
        return s.replace(/\\/g, "\\\\").replace(/"/g, '\\"').replace(/\0/g, "\\0").replace(/\t/g, "\\t").replace(/\n/g, "\\n").replace(/\r/g, "\\r").replace(/[\x00-\x0F]/g, function(ch) {
          return "\\x0" + hex(ch);
        }).replace(/[\x10-\x1F\x7F-\x9F]/g, function(ch) {
          return "\\x" + hex(ch);
        });
      }
      function classEscape(s) {
        return s.replace(/\\/g, "\\\\").replace(/\]/g, "\\]").replace(/\^/g, "\\^").replace(/-/g, "\\-").replace(/\0/g, "\\0").replace(/\t/g, "\\t").replace(/\n/g, "\\n").replace(/\r/g, "\\r").replace(/[\x00-\x0F]/g, function(ch) {
          return "\\x0" + hex(ch);
        }).replace(/[\x10-\x1F\x7F-\x9F]/g, function(ch) {
          return "\\x" + hex(ch);
        });
      }
      function describeExpectation(expectation) {
        return DESCRIBE_EXPECTATION_FNS[expectation.type](expectation);
      }
      function describeExpected(expected2) {
        var descriptions = expected2.map(describeExpectation);
        var i, j;
        descriptions.sort();
        if (descriptions.length > 0) {
          for (i = 1, j = 1; i < descriptions.length; i++) {
            if (descriptions[i - 1] !== descriptions[i]) {
              descriptions[j] = descriptions[i];
              j++;
            }
          }
          descriptions.length = j;
        }
        switch (descriptions.length) {
          case 1:
            return descriptions[0];
          case 2:
            return descriptions[0] + " or " + descriptions[1];
          default:
            return descriptions.slice(0, -1).join(", ") + ", or " + descriptions[descriptions.length - 1];
        }
      }
      function describeFound(found2) {
        return found2 ? '"' + literalEscape(found2) + '"' : "end of input";
      }
      return "Expected " + describeExpected(expected) + " but " + describeFound(found) + " found.";
    };
    function peg$parse(input, options) {
      options = options !== void 0 ? options : {};
      var peg$FAILED = {};
      var peg$source = options.grammarSource;
      var peg$startRuleFunctions = { temporalFilter: peg$parsetemporalFilter };
      var peg$startRuleFunction = peg$parsetemporalFilter;
      var peg$c0 = "(";
      var peg$c1 = ")";
      var peg$c2 = "second";
      var peg$c3 = "minute";
      var peg$c4 = "hour";
      var peg$c5 = "day";
      var peg$c6 = "week";
      var peg$c7 = "month";
      var peg$c8 = "quarter";
      var peg$c9 = "year";
      var peg$c10 = "s";
      var peg$c11 = "-";
      var peg$c12 = ":";
      var peg$c13 = "not";
      var peg$c14 = "null";
      var peg$c15 = "none";
      var peg$c16 = "to";
      var peg$c17 = "now";
      var peg$c18 = "last";
      var peg$c19 = "this";
      var peg$c20 = "next";
      var peg$c21 = "ago";
      var peg$c22 = "from";
      var peg$c23 = "before";
      var peg$c24 = "after";
      var peg$c25 = "through";
      var peg$c26 = "starting";
      var peg$c27 = "for";
      var peg$c28 = "today";
      var peg$c29 = "yesterday";
      var peg$c30 = "tomorrow";
      var peg$c31 = "and";
      var peg$c32 = "or";
      var peg$c33 = "monday";
      var peg$c34 = "tuesday";
      var peg$c35 = "wednesday";
      var peg$c36 = "thursday";
      var peg$c37 = "friday";
      var peg$c38 = "saturday";
      var peg$c39 = "sunday";
      var peg$r0 = /^[0-9]/;
      var peg$r1 = /^[ Tt]/;
      var peg$r2 = /^[.,]/;
      var peg$r3 = /^[Ww]/;
      var peg$r4 = /^[Kk]/;
      var peg$r5 = /^[Qq]/;
      var peg$r6 = /^[1234]/;
      var peg$r7 = /^[a-zA-Z]/;
      var peg$r8 = /^[ \t]/;
      var peg$e0 = peg$literalExpectation("(", false);
      var peg$e1 = peg$literalExpectation(")", false);
      var peg$e2 = peg$classExpectation([["0", "9"]], false, false);
      var peg$e3 = peg$literalExpectation("second", true);
      var peg$e4 = peg$literalExpectation("minute", true);
      var peg$e5 = peg$literalExpectation("hour", true);
      var peg$e6 = peg$literalExpectation("day", true);
      var peg$e7 = peg$literalExpectation("week", true);
      var peg$e8 = peg$literalExpectation("month", true);
      var peg$e9 = peg$literalExpectation("quarter", true);
      var peg$e10 = peg$literalExpectation("year", true);
      var peg$e11 = peg$literalExpectation("s", true);
      var peg$e12 = peg$literalExpectation("-", false);
      var peg$e13 = peg$classExpectation([" ", "T", "t"], false, false);
      var peg$e14 = peg$literalExpectation(":", false);
      var peg$e15 = peg$classExpectation([".", ","], false, false);
      var peg$e16 = peg$classExpectation(["W", "w"], false, false);
      var peg$e17 = peg$classExpectation(["K", "k"], false, false);
      var peg$e18 = peg$classExpectation(["Q", "q"], false, false);
      var peg$e19 = peg$classExpectation(["1", "2", "3", "4"], false, false);
      var peg$e20 = peg$literalExpectation("not", true);
      var peg$e21 = peg$literalExpectation("null", true);
      var peg$e22 = peg$literalExpectation("none", true);
      var peg$e23 = peg$literalExpectation("to", true);
      var peg$e24 = peg$literalExpectation("now", true);
      var peg$e25 = peg$literalExpectation("last", true);
      var peg$e26 = peg$literalExpectation("this", true);
      var peg$e27 = peg$literalExpectation("next", true);
      var peg$e28 = peg$literalExpectation("ago", true);
      var peg$e29 = peg$literalExpectation("from", true);
      var peg$e30 = peg$literalExpectation("before", true);
      var peg$e31 = peg$literalExpectation("after", true);
      var peg$e32 = peg$literalExpectation("through", true);
      var peg$e33 = peg$literalExpectation("starting", true);
      var peg$e34 = peg$literalExpectation("for", true);
      var peg$e35 = peg$literalExpectation("today", true);
      var peg$e36 = peg$literalExpectation("yesterday", true);
      var peg$e37 = peg$literalExpectation("tomorrow", true);
      var peg$e38 = peg$literalExpectation("and", true);
      var peg$e39 = peg$literalExpectation("or", true);
      var peg$e40 = peg$literalExpectation("monday", true);
      var peg$e41 = peg$literalExpectation("tuesday", true);
      var peg$e42 = peg$literalExpectation("wednesday", true);
      var peg$e43 = peg$literalExpectation("thursday", true);
      var peg$e44 = peg$literalExpectation("friday", true);
      var peg$e45 = peg$literalExpectation("saturday", true);
      var peg$e46 = peg$literalExpectation("sunday", true);
      var peg$e47 = peg$classExpectation([["a", "z"], ["A", "Z"]], false, false);
      var peg$e48 = peg$otherExpectation("optional whitespace");
      var peg$e49 = peg$classExpectation([" ", "	"], false, false);
      var peg$e50 = peg$otherExpectation("whitespace");
      var peg$f0 = function(head, tail) {
        return tail.reduce((left, [, cop, , right]) => joinTemporal(left, cop, right), head);
      };
      var peg$f1 = function(clause) {
        return temporalNot(clause, true);
      };
      var peg$f2 = function(clause) {
        return clause;
      };
      var peg$f3 = function() {
        return { operator: "null" };
      };
      var peg$f4 = function() {
        return { operator: "none" };
      };
      var peg$f5 = function(expr) {
        return { operator: "()", expr };
      };
      var peg$f6 = function(m) {
        return { operator: "before", before: m };
      };
      var peg$f7 = function(m) {
        return { operator: "before", before: m, not: true };
      };
      var peg$f8 = function(m) {
        return { operator: "after", after: m };
      };
      var peg$f9 = function(m) {
        return { operator: "after", after: m, not: true };
      };
      var peg$f10 = function(ln, d) {
        return { operator: ln, ...d };
      };
      var peg$f11 = function(m, m2) {
        return { operator: "to", fromMoment: m, toMoment: m2 };
      };
      var peg$f12 = function(m, d) {
        return { ...d, operator: "for", begin: m };
      };
      var peg$f13 = function(m) {
        return { operator: "in", in: m };
      };
      var peg$f14 = function(d) {
        return { operator: "in_last", ...d };
      };
      var peg$f15 = function() {
        return "last";
      };
      var peg$f16 = function() {
        return "next";
      };
      var peg$f17 = function(n, u) {
        return { units: u, n };
      };
      var peg$f18 = function(n) {
        return n;
      };
      var peg$f19 = function(u) {
        return mkUnits(u);
      };
      var peg$f20 = function() {
        return { moment: "now" };
      };
      var peg$f21 = function() {
        return { moment: "today" };
      };
      var peg$f22 = function() {
        return { moment: "yesterday" };
      };
      var peg$f23 = function() {
        return { moment: "tomorrow" };
      };
      var peg$f24 = function(d) {
        return { moment: "ago", ...d };
      };
      var peg$f25 = function(d) {
        return { moment: "from_now", ...d };
      };
      var peg$f26 = function(dn) {
        return { moment: dn.toLowerCase(), which: "next" };
      };
      var peg$f27 = function(dn) {
        return { moment: dn.toLowerCase(), which: "last" };
      };
      var peg$f28 = function(lnt, u) {
        return { moment: lnt, units: u };
      };
      var peg$f29 = function(dn) {
        return { moment: dn.toLowerCase(), which: "last" };
      };
      var peg$f30 = function(tl) {
        return tl;
      };
      var peg$f31 = function() {
        return "this";
      };
      var peg$f32 = function() {
        return "next";
      };
      var peg$f33 = function() {
        return "last";
      };
      var peg$f34 = function(l) {
        return timeLiteral(l);
      };
      var peg$f35 = function(l) {
        return timeLiteral(l, "week");
      };
      var peg$f36 = function(l) {
        return timeLiteral(l, "quarter");
      };
      var peg$f37 = function(l) {
        return timeLiteral(l, "minute");
      };
      var peg$f38 = function(l) {
        return timeLiteral(l, "hour");
      };
      var peg$f39 = function(l) {
        return timeLiteral(l, "day");
      };
      var peg$f40 = function(l) {
        return timeLiteral(l, "month");
      };
      var peg$f41 = function(l) {
        return timeLiteral(l, "year");
      };
      var peg$f42 = function(w) {
        return w;
      };
      var peg$f43 = function() {
        return "or";
      };
      var peg$f44 = function() {
        return "and";
      };
      var peg$currPos = options.peg$currPos | 0;
      var peg$savedPos = peg$currPos;
      var peg$posDetailsCache = [{ line: 1, column: 1 }];
      var peg$maxFailPos = peg$currPos;
      var peg$maxFailExpected = options.peg$maxFailExpected || [];
      var peg$silentFails = options.peg$silentFails | 0;
      var peg$result;
      if (options.startRule) {
        if (!(options.startRule in peg$startRuleFunctions)) {
          throw new Error(`Can't start parsing from rule "` + options.startRule + '".');
        }
        peg$startRuleFunction = peg$startRuleFunctions[options.startRule];
      }
      function text() {
        return input.substring(peg$savedPos, peg$currPos);
      }
      function offset() {
        return peg$savedPos;
      }
      function range() {
        return {
          source: peg$source,
          start: peg$savedPos,
          end: peg$currPos
        };
      }
      function location() {
        return peg$computeLocation(peg$savedPos, peg$currPos);
      }
      function expected(description, location2) {
        location2 = location2 !== void 0 ? location2 : peg$computeLocation(peg$savedPos, peg$currPos);
        throw peg$buildStructuredError([peg$otherExpectation(description)], input.substring(peg$savedPos, peg$currPos), location2);
      }
      function error(message, location2) {
        location2 = location2 !== void 0 ? location2 : peg$computeLocation(peg$savedPos, peg$currPos);
        throw peg$buildSimpleError(message, location2);
      }
      function peg$literalExpectation(text2, ignoreCase) {
        return { type: "literal", text: text2, ignoreCase };
      }
      function peg$classExpectation(parts, inverted, ignoreCase) {
        return { type: "class", parts, inverted, ignoreCase };
      }
      function peg$anyExpectation() {
        return { type: "any" };
      }
      function peg$endExpectation() {
        return { type: "end" };
      }
      function peg$otherExpectation(description) {
        return { type: "other", description };
      }
      function peg$computePosDetails(pos) {
        var details = peg$posDetailsCache[pos];
        var p;
        if (details) {
          return details;
        } else {
          if (pos >= peg$posDetailsCache.length) {
            p = peg$posDetailsCache.length - 1;
          } else {
            p = pos;
            while (!peg$posDetailsCache[--p]) {
            }
          }
          details = peg$posDetailsCache[p];
          details = {
            line: details.line,
            column: details.column
          };
          while (p < pos) {
            if (input.charCodeAt(p) === 10) {
              details.line++;
              details.column = 1;
            } else {
              details.column++;
            }
            p++;
          }
          peg$posDetailsCache[pos] = details;
          return details;
        }
      }
      function peg$computeLocation(startPos, endPos, offset2) {
        var startPosDetails = peg$computePosDetails(startPos);
        var endPosDetails = peg$computePosDetails(endPos);
        var res = {
          source: peg$source,
          start: {
            offset: startPos,
            line: startPosDetails.line,
            column: startPosDetails.column
          },
          end: {
            offset: endPos,
            line: endPosDetails.line,
            column: endPosDetails.column
          }
        };
        if (offset2 && peg$source && typeof peg$source.offset === "function") {
          res.start = peg$source.offset(res.start);
          res.end = peg$source.offset(res.end);
        }
        return res;
      }
      function peg$fail(expected2) {
        if (peg$currPos < peg$maxFailPos) {
          return;
        }
        if (peg$currPos > peg$maxFailPos) {
          peg$maxFailPos = peg$currPos;
          peg$maxFailExpected = [];
        }
        peg$maxFailExpected.push(expected2);
      }
      function peg$buildSimpleError(message, location2) {
        return new peg$SyntaxError(message, null, null, location2);
      }
      function peg$buildStructuredError(expected2, found, location2) {
        return new peg$SyntaxError(peg$SyntaxError.buildMessage(expected2, found), expected2, found, location2);
      }
      function peg$parsetemporalFilter() {
        var s0, s1, s2, s3, s4, s5, s6, s7;
        s0 = peg$currPos;
        s1 = peg$parsetemporalUnary();
        if (s1 !== peg$FAILED) {
          s2 = [];
          s3 = peg$currPos;
          s4 = peg$parse_();
          s5 = peg$parseconjunction();
          if (s5 !== peg$FAILED) {
            s6 = peg$parse_();
            s7 = peg$parsetemporalUnary();
            if (s7 !== peg$FAILED) {
              s4 = [s4, s5, s6, s7];
              s3 = s4;
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
          while (s3 !== peg$FAILED) {
            s2.push(s3);
            s3 = peg$currPos;
            s4 = peg$parse_();
            s5 = peg$parseconjunction();
            if (s5 !== peg$FAILED) {
              s6 = peg$parse_();
              s7 = peg$parsetemporalUnary();
              if (s7 !== peg$FAILED) {
                s4 = [s4, s5, s6, s7];
                s3 = s4;
              } else {
                peg$currPos = s3;
                s3 = peg$FAILED;
              }
            } else {
              peg$currPos = s3;
              s3 = peg$FAILED;
            }
          }
          peg$savedPos = s0;
          s0 = peg$f0(s1, s2);
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
        return s0;
      }
      function peg$parsetemporalUnary() {
        var s0, s1, s2, s3;
        s0 = peg$currPos;
        s1 = peg$parseNOT();
        if (s1 !== peg$FAILED) {
          s2 = peg$parse__();
          if (s2 !== peg$FAILED) {
            s3 = peg$parseclause();
            if (s3 !== peg$FAILED) {
              peg$savedPos = s0;
              s0 = peg$f1(s3);
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
        if (s0 === peg$FAILED) {
          s0 = peg$currPos;
          s1 = peg$parseclause();
          if (s1 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$f2(s1);
          }
          s0 = s1;
        }
        return s0;
      }
      function peg$parseclause() {
        var s0, s1, s2, s3, s4, s5;
        s0 = peg$currPos;
        s1 = peg$parseNULL();
        if (s1 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$f3();
        }
        s0 = s1;
        if (s0 === peg$FAILED) {
          s0 = peg$currPos;
          s1 = peg$parseNONE();
          if (s1 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$f4();
          }
          s0 = s1;
          if (s0 === peg$FAILED) {
            s0 = peg$currPos;
            if (input.charCodeAt(peg$currPos) === 40) {
              s1 = peg$c0;
              peg$currPos++;
            } else {
              s1 = peg$FAILED;
              if (peg$silentFails === 0) {
                peg$fail(peg$e0);
              }
            }
            if (s1 !== peg$FAILED) {
              s2 = peg$parse_();
              s3 = peg$parsetemporalFilter();
              if (s3 !== peg$FAILED) {
                s4 = peg$parse_();
                if (input.charCodeAt(peg$currPos) === 41) {
                  s5 = peg$c1;
                  peg$currPos++;
                } else {
                  s5 = peg$FAILED;
                  if (peg$silentFails === 0) {
                    peg$fail(peg$e1);
                  }
                }
                if (s5 !== peg$FAILED) {
                  peg$savedPos = s0;
                  s0 = peg$f5(s3);
                } else {
                  peg$currPos = s0;
                  s0 = peg$FAILED;
                }
              } else {
                peg$currPos = s0;
                s0 = peg$FAILED;
              }
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
            if (s0 === peg$FAILED) {
              s0 = peg$currPos;
              s1 = peg$parseBEFORE();
              if (s1 !== peg$FAILED) {
                s2 = peg$parse__();
                if (s2 !== peg$FAILED) {
                  s3 = peg$parsemoment();
                  if (s3 !== peg$FAILED) {
                    peg$savedPos = s0;
                    s0 = peg$f6(s3);
                  } else {
                    peg$currPos = s0;
                    s0 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s0;
                  s0 = peg$FAILED;
                }
              } else {
                peg$currPos = s0;
                s0 = peg$FAILED;
              }
              if (s0 === peg$FAILED) {
                s0 = peg$currPos;
                s1 = peg$parseSTARTING();
                if (s1 !== peg$FAILED) {
                  s2 = peg$parse__();
                  if (s2 !== peg$FAILED) {
                    s3 = peg$parsemoment();
                    if (s3 !== peg$FAILED) {
                      peg$savedPos = s0;
                      s0 = peg$f7(s3);
                    } else {
                      peg$currPos = s0;
                      s0 = peg$FAILED;
                    }
                  } else {
                    peg$currPos = s0;
                    s0 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s0;
                  s0 = peg$FAILED;
                }
                if (s0 === peg$FAILED) {
                  s0 = peg$currPos;
                  s1 = peg$parseAFTER();
                  if (s1 !== peg$FAILED) {
                    s2 = peg$parse__();
                    if (s2 !== peg$FAILED) {
                      s3 = peg$parsemoment();
                      if (s3 !== peg$FAILED) {
                        peg$savedPos = s0;
                        s0 = peg$f8(s3);
                      } else {
                        peg$currPos = s0;
                        s0 = peg$FAILED;
                      }
                    } else {
                      peg$currPos = s0;
                      s0 = peg$FAILED;
                    }
                  } else {
                    peg$currPos = s0;
                    s0 = peg$FAILED;
                  }
                  if (s0 === peg$FAILED) {
                    s0 = peg$currPos;
                    s1 = peg$parseTHROUGH();
                    if (s1 !== peg$FAILED) {
                      s2 = peg$parse__();
                      if (s2 !== peg$FAILED) {
                        s3 = peg$parsemoment();
                        if (s3 !== peg$FAILED) {
                          peg$savedPos = s0;
                          s0 = peg$f9(s3);
                        } else {
                          peg$currPos = s0;
                          s0 = peg$FAILED;
                        }
                      } else {
                        peg$currPos = s0;
                        s0 = peg$FAILED;
                      }
                    } else {
                      peg$currPos = s0;
                      s0 = peg$FAILED;
                    }
                    if (s0 === peg$FAILED) {
                      s0 = peg$currPos;
                      s1 = peg$parselastOrNext();
                      if (s1 !== peg$FAILED) {
                        s2 = peg$parse__();
                        if (s2 !== peg$FAILED) {
                          s3 = peg$parseduration();
                          if (s3 !== peg$FAILED) {
                            peg$savedPos = s0;
                            s0 = peg$f10(s1, s3);
                          } else {
                            peg$currPos = s0;
                            s0 = peg$FAILED;
                          }
                        } else {
                          peg$currPos = s0;
                          s0 = peg$FAILED;
                        }
                      } else {
                        peg$currPos = s0;
                        s0 = peg$FAILED;
                      }
                      if (s0 === peg$FAILED) {
                        s0 = peg$currPos;
                        s1 = peg$parsemoment();
                        if (s1 !== peg$FAILED) {
                          s2 = peg$parse__();
                          if (s2 !== peg$FAILED) {
                            s3 = peg$parseTO();
                            if (s3 !== peg$FAILED) {
                              s4 = peg$parse__();
                              if (s4 !== peg$FAILED) {
                                s5 = peg$parsemoment();
                                if (s5 !== peg$FAILED) {
                                  peg$savedPos = s0;
                                  s0 = peg$f11(s1, s5);
                                } else {
                                  peg$currPos = s0;
                                  s0 = peg$FAILED;
                                }
                              } else {
                                peg$currPos = s0;
                                s0 = peg$FAILED;
                              }
                            } else {
                              peg$currPos = s0;
                              s0 = peg$FAILED;
                            }
                          } else {
                            peg$currPos = s0;
                            s0 = peg$FAILED;
                          }
                        } else {
                          peg$currPos = s0;
                          s0 = peg$FAILED;
                        }
                        if (s0 === peg$FAILED) {
                          s0 = peg$currPos;
                          s1 = peg$parsemoment();
                          if (s1 !== peg$FAILED) {
                            s2 = peg$parse__();
                            if (s2 !== peg$FAILED) {
                              s3 = peg$parseFOR();
                              if (s3 !== peg$FAILED) {
                                s4 = peg$parse__();
                                if (s4 !== peg$FAILED) {
                                  s5 = peg$parseduration();
                                  if (s5 !== peg$FAILED) {
                                    peg$savedPos = s0;
                                    s0 = peg$f12(s1, s5);
                                  } else {
                                    peg$currPos = s0;
                                    s0 = peg$FAILED;
                                  }
                                } else {
                                  peg$currPos = s0;
                                  s0 = peg$FAILED;
                                }
                              } else {
                                peg$currPos = s0;
                                s0 = peg$FAILED;
                              }
                            } else {
                              peg$currPos = s0;
                              s0 = peg$FAILED;
                            }
                          } else {
                            peg$currPos = s0;
                            s0 = peg$FAILED;
                          }
                          if (s0 === peg$FAILED) {
                            s0 = peg$currPos;
                            s1 = peg$parsemoment();
                            if (s1 !== peg$FAILED) {
                              peg$savedPos = s0;
                              s1 = peg$f13(s1);
                            }
                            s0 = s1;
                            if (s0 === peg$FAILED) {
                              s0 = peg$currPos;
                              s1 = peg$parseduration();
                              if (s1 !== peg$FAILED) {
                                peg$savedPos = s0;
                                s1 = peg$f14(s1);
                              }
                              s0 = s1;
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
        return s0;
      }
      function peg$parselastOrNext() {
        var s0, s1;
        s0 = peg$currPos;
        s1 = peg$parseLAST();
        if (s1 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$f15();
        }
        s0 = s1;
        if (s0 === peg$FAILED) {
          s0 = peg$currPos;
          s1 = peg$parseNEXT();
          if (s1 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$f16();
          }
          s0 = s1;
        }
        return s0;
      }
      function peg$parseduration() {
        var s0, s1, s2, s3;
        s0 = peg$currPos;
        s1 = peg$parseinteger();
        if (s1 !== peg$FAILED) {
          s2 = peg$parse__();
          if (s2 !== peg$FAILED) {
            s3 = peg$parseunit();
            if (s3 !== peg$FAILED) {
              peg$savedPos = s0;
              s0 = peg$f17(s1, s3);
            } else {
              peg$currPos = s0;
              s0 = peg$FAILED;
            }
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
        return s0;
      }
      function peg$parseinteger() {
        var s0, s1, s2, s3;
        s0 = peg$currPos;
        s1 = peg$currPos;
        s2 = [];
        s3 = input.charAt(peg$currPos);
        if (peg$r0.test(s3)) {
          peg$currPos++;
        } else {
          s3 = peg$FAILED;
          if (peg$silentFails === 0) {
            peg$fail(peg$e2);
          }
        }
        if (s3 !== peg$FAILED) {
          while (s3 !== peg$FAILED) {
            s2.push(s3);
            s3 = input.charAt(peg$currPos);
            if (peg$r0.test(s3)) {
              peg$currPos++;
            } else {
              s3 = peg$FAILED;
              if (peg$silentFails === 0) {
                peg$fail(peg$e2);
              }
            }
          }
        } else {
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          s1 = input.substring(s1, peg$currPos);
        } else {
          s1 = s2;
        }
        if (s1 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$f18(s1);
        }
        s0 = s1;
        return s0;
      }
      function peg$parseunit() {
        var s0, s1, s2, s3, s4;
        s0 = peg$currPos;
        s1 = peg$currPos;
        s2 = input.substr(peg$currPos, 6);
        if (s2.toLowerCase() === peg$c2) {
          peg$currPos += 6;
        } else {
          s2 = peg$FAILED;
          if (peg$silentFails === 0) {
            peg$fail(peg$e3);
          }
        }
        if (s2 === peg$FAILED) {
          s2 = input.substr(peg$currPos, 6);
          if (s2.toLowerCase() === peg$c3) {
            peg$currPos += 6;
          } else {
            s2 = peg$FAILED;
            if (peg$silentFails === 0) {
              peg$fail(peg$e4);
            }
          }
          if (s2 === peg$FAILED) {
            s2 = input.substr(peg$currPos, 4);
            if (s2.toLowerCase() === peg$c4) {
              peg$currPos += 4;
            } else {
              s2 = peg$FAILED;
              if (peg$silentFails === 0) {
                peg$fail(peg$e5);
              }
            }
            if (s2 === peg$FAILED) {
              s2 = input.substr(peg$currPos, 3);
              if (s2.toLowerCase() === peg$c5) {
                peg$currPos += 3;
              } else {
                s2 = peg$FAILED;
                if (peg$silentFails === 0) {
                  peg$fail(peg$e6);
                }
              }
              if (s2 === peg$FAILED) {
                s2 = input.substr(peg$currPos, 4);
                if (s2.toLowerCase() === peg$c6) {
                  peg$currPos += 4;
                } else {
                  s2 = peg$FAILED;
                  if (peg$silentFails === 0) {
                    peg$fail(peg$e7);
                  }
                }
                if (s2 === peg$FAILED) {
                  s2 = input.substr(peg$currPos, 5);
                  if (s2.toLowerCase() === peg$c7) {
                    peg$currPos += 5;
                  } else {
                    s2 = peg$FAILED;
                    if (peg$silentFails === 0) {
                      peg$fail(peg$e8);
                    }
                  }
                  if (s2 === peg$FAILED) {
                    s2 = input.substr(peg$currPos, 7);
                    if (s2.toLowerCase() === peg$c8) {
                      peg$currPos += 7;
                    } else {
                      s2 = peg$FAILED;
                      if (peg$silentFails === 0) {
                        peg$fail(peg$e9);
                      }
                    }
                    if (s2 === peg$FAILED) {
                      s2 = input.substr(peg$currPos, 4);
                      if (s2.toLowerCase() === peg$c9) {
                        peg$currPos += 4;
                      } else {
                        s2 = peg$FAILED;
                        if (peg$silentFails === 0) {
                          peg$fail(peg$e10);
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
        if (s2 !== peg$FAILED) {
          s1 = input.substring(s1, peg$currPos);
        } else {
          s1 = s2;
        }
        if (s1 !== peg$FAILED) {
          s2 = input.charAt(peg$currPos);
          if (s2.toLowerCase() === peg$c10) {
            peg$currPos++;
          } else {
            s2 = peg$FAILED;
            if (peg$silentFails === 0) {
              peg$fail(peg$e11);
            }
          }
          if (s2 === peg$FAILED) {
            s2 = null;
          }
          s3 = peg$currPos;
          peg$silentFails++;
          s4 = peg$parseidChar();
          peg$silentFails--;
          if (s4 === peg$FAILED) {
            s3 = void 0;
          } else {
            peg$currPos = s3;
            s3 = peg$FAILED;
          }
          if (s3 !== peg$FAILED) {
            peg$savedPos = s0;
            s0 = peg$f19(s1);
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
        return s0;
      }
      function peg$parsemoment() {
        var s0, s1, s2, s3, s4, s5;
        s0 = peg$currPos;
        s1 = peg$parseNOW();
        if (s1 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$f20();
        }
        s0 = s1;
        if (s0 === peg$FAILED) {
          s0 = peg$currPos;
          s1 = peg$parseTODAY();
          if (s1 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$f21();
          }
          s0 = s1;
          if (s0 === peg$FAILED) {
            s0 = peg$currPos;
            s1 = peg$parseYESTERDAY();
            if (s1 !== peg$FAILED) {
              peg$savedPos = s0;
              s1 = peg$f22();
            }
            s0 = s1;
            if (s0 === peg$FAILED) {
              s0 = peg$currPos;
              s1 = peg$parseTOMORROW();
              if (s1 !== peg$FAILED) {
                peg$savedPos = s0;
                s1 = peg$f23();
              }
              s0 = s1;
              if (s0 === peg$FAILED) {
                s0 = peg$currPos;
                s1 = peg$parseduration();
                if (s1 !== peg$FAILED) {
                  s2 = peg$parse__();
                  if (s2 !== peg$FAILED) {
                    s3 = peg$parseAGO();
                    if (s3 !== peg$FAILED) {
                      peg$savedPos = s0;
                      s0 = peg$f24(s1);
                    } else {
                      peg$currPos = s0;
                      s0 = peg$FAILED;
                    }
                  } else {
                    peg$currPos = s0;
                    s0 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s0;
                  s0 = peg$FAILED;
                }
                if (s0 === peg$FAILED) {
                  s0 = peg$currPos;
                  s1 = peg$parseduration();
                  if (s1 !== peg$FAILED) {
                    s2 = peg$parse__();
                    if (s2 !== peg$FAILED) {
                      s3 = peg$parseFROM();
                      if (s3 !== peg$FAILED) {
                        s4 = peg$parse__();
                        if (s4 !== peg$FAILED) {
                          s5 = peg$parseNOW();
                          if (s5 !== peg$FAILED) {
                            peg$savedPos = s0;
                            s0 = peg$f25(s1);
                          } else {
                            peg$currPos = s0;
                            s0 = peg$FAILED;
                          }
                        } else {
                          peg$currPos = s0;
                          s0 = peg$FAILED;
                        }
                      } else {
                        peg$currPos = s0;
                        s0 = peg$FAILED;
                      }
                    } else {
                      peg$currPos = s0;
                      s0 = peg$FAILED;
                    }
                  } else {
                    peg$currPos = s0;
                    s0 = peg$FAILED;
                  }
                  if (s0 === peg$FAILED) {
                    s0 = peg$currPos;
                    s1 = peg$parseNEXT();
                    if (s1 !== peg$FAILED) {
                      s2 = peg$parse__();
                      if (s2 !== peg$FAILED) {
                        s3 = peg$parseweekday();
                        if (s3 !== peg$FAILED) {
                          peg$savedPos = s0;
                          s0 = peg$f26(s3);
                        } else {
                          peg$currPos = s0;
                          s0 = peg$FAILED;
                        }
                      } else {
                        peg$currPos = s0;
                        s0 = peg$FAILED;
                      }
                    } else {
                      peg$currPos = s0;
                      s0 = peg$FAILED;
                    }
                    if (s0 === peg$FAILED) {
                      s0 = peg$currPos;
                      s1 = peg$parseLAST();
                      if (s1 !== peg$FAILED) {
                        s2 = peg$parse__();
                        if (s2 !== peg$FAILED) {
                          s3 = peg$parseweekday();
                          if (s3 !== peg$FAILED) {
                            peg$savedPos = s0;
                            s0 = peg$f27(s3);
                          } else {
                            peg$currPos = s0;
                            s0 = peg$FAILED;
                          }
                        } else {
                          peg$currPos = s0;
                          s0 = peg$FAILED;
                        }
                      } else {
                        peg$currPos = s0;
                        s0 = peg$FAILED;
                      }
                      if (s0 === peg$FAILED) {
                        s0 = peg$currPos;
                        s1 = peg$parselastNextThis();
                        if (s1 !== peg$FAILED) {
                          s2 = peg$parse__();
                          if (s2 !== peg$FAILED) {
                            s3 = peg$parseunit();
                            if (s3 !== peg$FAILED) {
                              peg$savedPos = s0;
                              s0 = peg$f28(s1, s3);
                            } else {
                              peg$currPos = s0;
                              s0 = peg$FAILED;
                            }
                          } else {
                            peg$currPos = s0;
                            s0 = peg$FAILED;
                          }
                        } else {
                          peg$currPos = s0;
                          s0 = peg$FAILED;
                        }
                        if (s0 === peg$FAILED) {
                          s0 = peg$currPos;
                          s1 = peg$parseweekday();
                          if (s1 !== peg$FAILED) {
                            peg$savedPos = s0;
                            s1 = peg$f29(s1);
                          }
                          s0 = s1;
                          if (s0 === peg$FAILED) {
                            s0 = peg$currPos;
                            s1 = peg$parsetimeLiteral();
                            if (s1 !== peg$FAILED) {
                              peg$savedPos = s0;
                              s1 = peg$f30(s1);
                            }
                            s0 = s1;
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
        return s0;
      }
      function peg$parselastNextThis() {
        var s0, s1;
        s0 = peg$currPos;
        s1 = peg$parseTHIS();
        if (s1 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$f31();
        }
        s0 = s1;
        if (s0 === peg$FAILED) {
          s0 = peg$currPos;
          s1 = peg$parseNEXT();
          if (s1 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$f32();
          }
          s0 = s1;
          if (s0 === peg$FAILED) {
            s0 = peg$currPos;
            s1 = peg$parseLAST();
            if (s1 !== peg$FAILED) {
              peg$savedPos = s0;
              s1 = peg$f33();
            }
            s0 = s1;
          }
        }
        return s0;
      }
      function peg$parsetimeLiteral() {
        var s0, s1, s2, s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15, s16, s17, s18, s19, s20, s21, s22, s23, s24, s25;
        s0 = peg$currPos;
        s1 = peg$currPos;
        s2 = peg$currPos;
        s3 = input.charAt(peg$currPos);
        if (peg$r0.test(s3)) {
          peg$currPos++;
        } else {
          s3 = peg$FAILED;
          if (peg$silentFails === 0) {
            peg$fail(peg$e2);
          }
        }
        if (s3 !== peg$FAILED) {
          s4 = input.charAt(peg$currPos);
          if (peg$r0.test(s4)) {
            peg$currPos++;
          } else {
            s4 = peg$FAILED;
            if (peg$silentFails === 0) {
              peg$fail(peg$e2);
            }
          }
          if (s4 !== peg$FAILED) {
            s5 = input.charAt(peg$currPos);
            if (peg$r0.test(s5)) {
              peg$currPos++;
            } else {
              s5 = peg$FAILED;
              if (peg$silentFails === 0) {
                peg$fail(peg$e2);
              }
            }
            if (s5 !== peg$FAILED) {
              s6 = input.charAt(peg$currPos);
              if (peg$r0.test(s6)) {
                peg$currPos++;
              } else {
                s6 = peg$FAILED;
                if (peg$silentFails === 0) {
                  peg$fail(peg$e2);
                }
              }
              if (s6 !== peg$FAILED) {
                if (input.charCodeAt(peg$currPos) === 45) {
                  s7 = peg$c11;
                  peg$currPos++;
                } else {
                  s7 = peg$FAILED;
                  if (peg$silentFails === 0) {
                    peg$fail(peg$e12);
                  }
                }
                if (s7 !== peg$FAILED) {
                  s8 = input.charAt(peg$currPos);
                  if (peg$r0.test(s8)) {
                    peg$currPos++;
                  } else {
                    s8 = peg$FAILED;
                    if (peg$silentFails === 0) {
                      peg$fail(peg$e2);
                    }
                  }
                  if (s8 !== peg$FAILED) {
                    s9 = input.charAt(peg$currPos);
                    if (peg$r0.test(s9)) {
                      peg$currPos++;
                    } else {
                      s9 = peg$FAILED;
                      if (peg$silentFails === 0) {
                        peg$fail(peg$e2);
                      }
                    }
                    if (s9 !== peg$FAILED) {
                      if (input.charCodeAt(peg$currPos) === 45) {
                        s10 = peg$c11;
                        peg$currPos++;
                      } else {
                        s10 = peg$FAILED;
                        if (peg$silentFails === 0) {
                          peg$fail(peg$e12);
                        }
                      }
                      if (s10 !== peg$FAILED) {
                        s11 = input.charAt(peg$currPos);
                        if (peg$r0.test(s11)) {
                          peg$currPos++;
                        } else {
                          s11 = peg$FAILED;
                          if (peg$silentFails === 0) {
                            peg$fail(peg$e2);
                          }
                        }
                        if (s11 !== peg$FAILED) {
                          s12 = input.charAt(peg$currPos);
                          if (peg$r0.test(s12)) {
                            peg$currPos++;
                          } else {
                            s12 = peg$FAILED;
                            if (peg$silentFails === 0) {
                              peg$fail(peg$e2);
                            }
                          }
                          if (s12 !== peg$FAILED) {
                            s13 = input.charAt(peg$currPos);
                            if (peg$r1.test(s13)) {
                              peg$currPos++;
                            } else {
                              s13 = peg$FAILED;
                              if (peg$silentFails === 0) {
                                peg$fail(peg$e13);
                              }
                            }
                            if (s13 !== peg$FAILED) {
                              s14 = input.charAt(peg$currPos);
                              if (peg$r0.test(s14)) {
                                peg$currPos++;
                              } else {
                                s14 = peg$FAILED;
                                if (peg$silentFails === 0) {
                                  peg$fail(peg$e2);
                                }
                              }
                              if (s14 !== peg$FAILED) {
                                s15 = input.charAt(peg$currPos);
                                if (peg$r0.test(s15)) {
                                  peg$currPos++;
                                } else {
                                  s15 = peg$FAILED;
                                  if (peg$silentFails === 0) {
                                    peg$fail(peg$e2);
                                  }
                                }
                                if (s15 !== peg$FAILED) {
                                  if (input.charCodeAt(peg$currPos) === 58) {
                                    s16 = peg$c12;
                                    peg$currPos++;
                                  } else {
                                    s16 = peg$FAILED;
                                    if (peg$silentFails === 0) {
                                      peg$fail(peg$e14);
                                    }
                                  }
                                  if (s16 !== peg$FAILED) {
                                    s17 = input.charAt(peg$currPos);
                                    if (peg$r0.test(s17)) {
                                      peg$currPos++;
                                    } else {
                                      s17 = peg$FAILED;
                                      if (peg$silentFails === 0) {
                                        peg$fail(peg$e2);
                                      }
                                    }
                                    if (s17 !== peg$FAILED) {
                                      s18 = input.charAt(peg$currPos);
                                      if (peg$r0.test(s18)) {
                                        peg$currPos++;
                                      } else {
                                        s18 = peg$FAILED;
                                        if (peg$silentFails === 0) {
                                          peg$fail(peg$e2);
                                        }
                                      }
                                      if (s18 !== peg$FAILED) {
                                        if (input.charCodeAt(peg$currPos) === 58) {
                                          s19 = peg$c12;
                                          peg$currPos++;
                                        } else {
                                          s19 = peg$FAILED;
                                          if (peg$silentFails === 0) {
                                            peg$fail(peg$e14);
                                          }
                                        }
                                        if (s19 !== peg$FAILED) {
                                          s20 = input.charAt(peg$currPos);
                                          if (peg$r0.test(s20)) {
                                            peg$currPos++;
                                          } else {
                                            s20 = peg$FAILED;
                                            if (peg$silentFails === 0) {
                                              peg$fail(peg$e2);
                                            }
                                          }
                                          if (s20 !== peg$FAILED) {
                                            s21 = input.charAt(peg$currPos);
                                            if (peg$r0.test(s21)) {
                                              peg$currPos++;
                                            } else {
                                              s21 = peg$FAILED;
                                              if (peg$silentFails === 0) {
                                                peg$fail(peg$e2);
                                              }
                                            }
                                            if (s21 !== peg$FAILED) {
                                              s22 = peg$currPos;
                                              s23 = input.charAt(peg$currPos);
                                              if (peg$r2.test(s23)) {
                                                peg$currPos++;
                                              } else {
                                                s23 = peg$FAILED;
                                                if (peg$silentFails === 0) {
                                                  peg$fail(peg$e15);
                                                }
                                              }
                                              if (s23 !== peg$FAILED) {
                                                s24 = [];
                                                s25 = input.charAt(peg$currPos);
                                                if (peg$r0.test(s25)) {
                                                  peg$currPos++;
                                                } else {
                                                  s25 = peg$FAILED;
                                                  if (peg$silentFails === 0) {
                                                    peg$fail(peg$e2);
                                                  }
                                                }
                                                while (s25 !== peg$FAILED) {
                                                  s24.push(s25);
                                                  s25 = input.charAt(peg$currPos);
                                                  if (peg$r0.test(s25)) {
                                                    peg$currPos++;
                                                  } else {
                                                    s25 = peg$FAILED;
                                                    if (peg$silentFails === 0) {
                                                      peg$fail(peg$e2);
                                                    }
                                                  }
                                                }
                                                s23 = [s23, s24];
                                                s22 = s23;
                                              } else {
                                                peg$currPos = s22;
                                                s22 = peg$FAILED;
                                              }
                                              if (s22 === peg$FAILED) {
                                                s22 = null;
                                              }
                                              s3 = [s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15, s16, s17, s18, s19, s20, s21, s22];
                                              s2 = s3;
                                            } else {
                                              peg$currPos = s2;
                                              s2 = peg$FAILED;
                                            }
                                          } else {
                                            peg$currPos = s2;
                                            s2 = peg$FAILED;
                                          }
                                        } else {
                                          peg$currPos = s2;
                                          s2 = peg$FAILED;
                                        }
                                      } else {
                                        peg$currPos = s2;
                                        s2 = peg$FAILED;
                                      }
                                    } else {
                                      peg$currPos = s2;
                                      s2 = peg$FAILED;
                                    }
                                  } else {
                                    peg$currPos = s2;
                                    s2 = peg$FAILED;
                                  }
                                } else {
                                  peg$currPos = s2;
                                  s2 = peg$FAILED;
                                }
                              } else {
                                peg$currPos = s2;
                                s2 = peg$FAILED;
                              }
                            } else {
                              peg$currPos = s2;
                              s2 = peg$FAILED;
                            }
                          } else {
                            peg$currPos = s2;
                            s2 = peg$FAILED;
                          }
                        } else {
                          peg$currPos = s2;
                          s2 = peg$FAILED;
                        }
                      } else {
                        peg$currPos = s2;
                        s2 = peg$FAILED;
                      }
                    } else {
                      peg$currPos = s2;
                      s2 = peg$FAILED;
                    }
                  } else {
                    peg$currPos = s2;
                    s2 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s2;
                  s2 = peg$FAILED;
                }
              } else {
                peg$currPos = s2;
                s2 = peg$FAILED;
              }
            } else {
              peg$currPos = s2;
              s2 = peg$FAILED;
            }
          } else {
            peg$currPos = s2;
            s2 = peg$FAILED;
          }
        } else {
          peg$currPos = s2;
          s2 = peg$FAILED;
        }
        if (s2 !== peg$FAILED) {
          s1 = input.substring(s1, peg$currPos);
        } else {
          s1 = s2;
        }
        if (s1 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$f34(s1);
        }
        s0 = s1;
        if (s0 === peg$FAILED) {
          s0 = peg$currPos;
          s1 = peg$currPos;
          s2 = peg$currPos;
          s3 = input.charAt(peg$currPos);
          if (peg$r0.test(s3)) {
            peg$currPos++;
          } else {
            s3 = peg$FAILED;
            if (peg$silentFails === 0) {
              peg$fail(peg$e2);
            }
          }
          if (s3 !== peg$FAILED) {
            s4 = input.charAt(peg$currPos);
            if (peg$r0.test(s4)) {
              peg$currPos++;
            } else {
              s4 = peg$FAILED;
              if (peg$silentFails === 0) {
                peg$fail(peg$e2);
              }
            }
            if (s4 !== peg$FAILED) {
              s5 = input.charAt(peg$currPos);
              if (peg$r0.test(s5)) {
                peg$currPos++;
              } else {
                s5 = peg$FAILED;
                if (peg$silentFails === 0) {
                  peg$fail(peg$e2);
                }
              }
              if (s5 !== peg$FAILED) {
                s6 = input.charAt(peg$currPos);
                if (peg$r0.test(s6)) {
                  peg$currPos++;
                } else {
                  s6 = peg$FAILED;
                  if (peg$silentFails === 0) {
                    peg$fail(peg$e2);
                  }
                }
                if (s6 !== peg$FAILED) {
                  if (input.charCodeAt(peg$currPos) === 45) {
                    s7 = peg$c11;
                    peg$currPos++;
                  } else {
                    s7 = peg$FAILED;
                    if (peg$silentFails === 0) {
                      peg$fail(peg$e12);
                    }
                  }
                  if (s7 !== peg$FAILED) {
                    s8 = input.charAt(peg$currPos);
                    if (peg$r0.test(s8)) {
                      peg$currPos++;
                    } else {
                      s8 = peg$FAILED;
                      if (peg$silentFails === 0) {
                        peg$fail(peg$e2);
                      }
                    }
                    if (s8 !== peg$FAILED) {
                      s9 = input.charAt(peg$currPos);
                      if (peg$r0.test(s9)) {
                        peg$currPos++;
                      } else {
                        s9 = peg$FAILED;
                        if (peg$silentFails === 0) {
                          peg$fail(peg$e2);
                        }
                      }
                      if (s9 !== peg$FAILED) {
                        if (input.charCodeAt(peg$currPos) === 45) {
                          s10 = peg$c11;
                          peg$currPos++;
                        } else {
                          s10 = peg$FAILED;
                          if (peg$silentFails === 0) {
                            peg$fail(peg$e12);
                          }
                        }
                        if (s10 !== peg$FAILED) {
                          s11 = input.charAt(peg$currPos);
                          if (peg$r0.test(s11)) {
                            peg$currPos++;
                          } else {
                            s11 = peg$FAILED;
                            if (peg$silentFails === 0) {
                              peg$fail(peg$e2);
                            }
                          }
                          if (s11 !== peg$FAILED) {
                            s12 = input.charAt(peg$currPos);
                            if (peg$r0.test(s12)) {
                              peg$currPos++;
                            } else {
                              s12 = peg$FAILED;
                              if (peg$silentFails === 0) {
                                peg$fail(peg$e2);
                              }
                            }
                            if (s12 !== peg$FAILED) {
                              if (input.charCodeAt(peg$currPos) === 45) {
                                s13 = peg$c11;
                                peg$currPos++;
                              } else {
                                s13 = peg$FAILED;
                                if (peg$silentFails === 0) {
                                  peg$fail(peg$e12);
                                }
                              }
                              if (s13 !== peg$FAILED) {
                                s14 = input.charAt(peg$currPos);
                                if (peg$r3.test(s14)) {
                                  peg$currPos++;
                                } else {
                                  s14 = peg$FAILED;
                                  if (peg$silentFails === 0) {
                                    peg$fail(peg$e16);
                                  }
                                }
                                if (s14 !== peg$FAILED) {
                                  s15 = input.charAt(peg$currPos);
                                  if (peg$r4.test(s15)) {
                                    peg$currPos++;
                                  } else {
                                    s15 = peg$FAILED;
                                    if (peg$silentFails === 0) {
                                      peg$fail(peg$e17);
                                    }
                                  }
                                  if (s15 !== peg$FAILED) {
                                    s3 = [s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15];
                                    s2 = s3;
                                  } else {
                                    peg$currPos = s2;
                                    s2 = peg$FAILED;
                                  }
                                } else {
                                  peg$currPos = s2;
                                  s2 = peg$FAILED;
                                }
                              } else {
                                peg$currPos = s2;
                                s2 = peg$FAILED;
                              }
                            } else {
                              peg$currPos = s2;
                              s2 = peg$FAILED;
                            }
                          } else {
                            peg$currPos = s2;
                            s2 = peg$FAILED;
                          }
                        } else {
                          peg$currPos = s2;
                          s2 = peg$FAILED;
                        }
                      } else {
                        peg$currPos = s2;
                        s2 = peg$FAILED;
                      }
                    } else {
                      peg$currPos = s2;
                      s2 = peg$FAILED;
                    }
                  } else {
                    peg$currPos = s2;
                    s2 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s2;
                  s2 = peg$FAILED;
                }
              } else {
                peg$currPos = s2;
                s2 = peg$FAILED;
              }
            } else {
              peg$currPos = s2;
              s2 = peg$FAILED;
            }
          } else {
            peg$currPos = s2;
            s2 = peg$FAILED;
          }
          if (s2 !== peg$FAILED) {
            s1 = input.substring(s1, peg$currPos);
          } else {
            s1 = s2;
          }
          if (s1 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$f35(s1);
          }
          s0 = s1;
          if (s0 === peg$FAILED) {
            s0 = peg$currPos;
            s1 = peg$currPos;
            s2 = peg$currPos;
            s3 = input.charAt(peg$currPos);
            if (peg$r0.test(s3)) {
              peg$currPos++;
            } else {
              s3 = peg$FAILED;
              if (peg$silentFails === 0) {
                peg$fail(peg$e2);
              }
            }
            if (s3 !== peg$FAILED) {
              s4 = input.charAt(peg$currPos);
              if (peg$r0.test(s4)) {
                peg$currPos++;
              } else {
                s4 = peg$FAILED;
                if (peg$silentFails === 0) {
                  peg$fail(peg$e2);
                }
              }
              if (s4 !== peg$FAILED) {
                s5 = input.charAt(peg$currPos);
                if (peg$r0.test(s5)) {
                  peg$currPos++;
                } else {
                  s5 = peg$FAILED;
                  if (peg$silentFails === 0) {
                    peg$fail(peg$e2);
                  }
                }
                if (s5 !== peg$FAILED) {
                  s6 = input.charAt(peg$currPos);
                  if (peg$r0.test(s6)) {
                    peg$currPos++;
                  } else {
                    s6 = peg$FAILED;
                    if (peg$silentFails === 0) {
                      peg$fail(peg$e2);
                    }
                  }
                  if (s6 !== peg$FAILED) {
                    if (input.charCodeAt(peg$currPos) === 45) {
                      s7 = peg$c11;
                      peg$currPos++;
                    } else {
                      s7 = peg$FAILED;
                      if (peg$silentFails === 0) {
                        peg$fail(peg$e12);
                      }
                    }
                    if (s7 !== peg$FAILED) {
                      s8 = input.charAt(peg$currPos);
                      if (peg$r5.test(s8)) {
                        peg$currPos++;
                      } else {
                        s8 = peg$FAILED;
                        if (peg$silentFails === 0) {
                          peg$fail(peg$e18);
                        }
                      }
                      if (s8 !== peg$FAILED) {
                        s9 = input.charAt(peg$currPos);
                        if (peg$r6.test(s9)) {
                          peg$currPos++;
                        } else {
                          s9 = peg$FAILED;
                          if (peg$silentFails === 0) {
                            peg$fail(peg$e19);
                          }
                        }
                        if (s9 !== peg$FAILED) {
                          s3 = [s3, s4, s5, s6, s7, s8, s9];
                          s2 = s3;
                        } else {
                          peg$currPos = s2;
                          s2 = peg$FAILED;
                        }
                      } else {
                        peg$currPos = s2;
                        s2 = peg$FAILED;
                      }
                    } else {
                      peg$currPos = s2;
                      s2 = peg$FAILED;
                    }
                  } else {
                    peg$currPos = s2;
                    s2 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s2;
                  s2 = peg$FAILED;
                }
              } else {
                peg$currPos = s2;
                s2 = peg$FAILED;
              }
            } else {
              peg$currPos = s2;
              s2 = peg$FAILED;
            }
            if (s2 !== peg$FAILED) {
              s1 = input.substring(s1, peg$currPos);
            } else {
              s1 = s2;
            }
            if (s1 !== peg$FAILED) {
              peg$savedPos = s0;
              s1 = peg$f36(s1);
            }
            s0 = s1;
            if (s0 === peg$FAILED) {
              s0 = peg$currPos;
              s1 = peg$currPos;
              s2 = peg$currPos;
              s3 = input.charAt(peg$currPos);
              if (peg$r0.test(s3)) {
                peg$currPos++;
              } else {
                s3 = peg$FAILED;
                if (peg$silentFails === 0) {
                  peg$fail(peg$e2);
                }
              }
              if (s3 !== peg$FAILED) {
                s4 = input.charAt(peg$currPos);
                if (peg$r0.test(s4)) {
                  peg$currPos++;
                } else {
                  s4 = peg$FAILED;
                  if (peg$silentFails === 0) {
                    peg$fail(peg$e2);
                  }
                }
                if (s4 !== peg$FAILED) {
                  s5 = input.charAt(peg$currPos);
                  if (peg$r0.test(s5)) {
                    peg$currPos++;
                  } else {
                    s5 = peg$FAILED;
                    if (peg$silentFails === 0) {
                      peg$fail(peg$e2);
                    }
                  }
                  if (s5 !== peg$FAILED) {
                    s6 = input.charAt(peg$currPos);
                    if (peg$r0.test(s6)) {
                      peg$currPos++;
                    } else {
                      s6 = peg$FAILED;
                      if (peg$silentFails === 0) {
                        peg$fail(peg$e2);
                      }
                    }
                    if (s6 !== peg$FAILED) {
                      if (input.charCodeAt(peg$currPos) === 45) {
                        s7 = peg$c11;
                        peg$currPos++;
                      } else {
                        s7 = peg$FAILED;
                        if (peg$silentFails === 0) {
                          peg$fail(peg$e12);
                        }
                      }
                      if (s7 !== peg$FAILED) {
                        s8 = input.charAt(peg$currPos);
                        if (peg$r0.test(s8)) {
                          peg$currPos++;
                        } else {
                          s8 = peg$FAILED;
                          if (peg$silentFails === 0) {
                            peg$fail(peg$e2);
                          }
                        }
                        if (s8 !== peg$FAILED) {
                          s9 = input.charAt(peg$currPos);
                          if (peg$r0.test(s9)) {
                            peg$currPos++;
                          } else {
                            s9 = peg$FAILED;
                            if (peg$silentFails === 0) {
                              peg$fail(peg$e2);
                            }
                          }
                          if (s9 !== peg$FAILED) {
                            if (input.charCodeAt(peg$currPos) === 45) {
                              s10 = peg$c11;
                              peg$currPos++;
                            } else {
                              s10 = peg$FAILED;
                              if (peg$silentFails === 0) {
                                peg$fail(peg$e12);
                              }
                            }
                            if (s10 !== peg$FAILED) {
                              s11 = input.charAt(peg$currPos);
                              if (peg$r0.test(s11)) {
                                peg$currPos++;
                              } else {
                                s11 = peg$FAILED;
                                if (peg$silentFails === 0) {
                                  peg$fail(peg$e2);
                                }
                              }
                              if (s11 !== peg$FAILED) {
                                s12 = input.charAt(peg$currPos);
                                if (peg$r0.test(s12)) {
                                  peg$currPos++;
                                } else {
                                  s12 = peg$FAILED;
                                  if (peg$silentFails === 0) {
                                    peg$fail(peg$e2);
                                  }
                                }
                                if (s12 !== peg$FAILED) {
                                  s13 = input.charAt(peg$currPos);
                                  if (peg$r1.test(s13)) {
                                    peg$currPos++;
                                  } else {
                                    s13 = peg$FAILED;
                                    if (peg$silentFails === 0) {
                                      peg$fail(peg$e13);
                                    }
                                  }
                                  if (s13 !== peg$FAILED) {
                                    s14 = input.charAt(peg$currPos);
                                    if (peg$r0.test(s14)) {
                                      peg$currPos++;
                                    } else {
                                      s14 = peg$FAILED;
                                      if (peg$silentFails === 0) {
                                        peg$fail(peg$e2);
                                      }
                                    }
                                    if (s14 !== peg$FAILED) {
                                      s15 = input.charAt(peg$currPos);
                                      if (peg$r0.test(s15)) {
                                        peg$currPos++;
                                      } else {
                                        s15 = peg$FAILED;
                                        if (peg$silentFails === 0) {
                                          peg$fail(peg$e2);
                                        }
                                      }
                                      if (s15 !== peg$FAILED) {
                                        if (input.charCodeAt(peg$currPos) === 58) {
                                          s16 = peg$c12;
                                          peg$currPos++;
                                        } else {
                                          s16 = peg$FAILED;
                                          if (peg$silentFails === 0) {
                                            peg$fail(peg$e14);
                                          }
                                        }
                                        if (s16 !== peg$FAILED) {
                                          s17 = input.charAt(peg$currPos);
                                          if (peg$r0.test(s17)) {
                                            peg$currPos++;
                                          } else {
                                            s17 = peg$FAILED;
                                            if (peg$silentFails === 0) {
                                              peg$fail(peg$e2);
                                            }
                                          }
                                          if (s17 !== peg$FAILED) {
                                            s18 = input.charAt(peg$currPos);
                                            if (peg$r0.test(s18)) {
                                              peg$currPos++;
                                            } else {
                                              s18 = peg$FAILED;
                                              if (peg$silentFails === 0) {
                                                peg$fail(peg$e2);
                                              }
                                            }
                                            if (s18 !== peg$FAILED) {
                                              s3 = [s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15, s16, s17, s18];
                                              s2 = s3;
                                            } else {
                                              peg$currPos = s2;
                                              s2 = peg$FAILED;
                                            }
                                          } else {
                                            peg$currPos = s2;
                                            s2 = peg$FAILED;
                                          }
                                        } else {
                                          peg$currPos = s2;
                                          s2 = peg$FAILED;
                                        }
                                      } else {
                                        peg$currPos = s2;
                                        s2 = peg$FAILED;
                                      }
                                    } else {
                                      peg$currPos = s2;
                                      s2 = peg$FAILED;
                                    }
                                  } else {
                                    peg$currPos = s2;
                                    s2 = peg$FAILED;
                                  }
                                } else {
                                  peg$currPos = s2;
                                  s2 = peg$FAILED;
                                }
                              } else {
                                peg$currPos = s2;
                                s2 = peg$FAILED;
                              }
                            } else {
                              peg$currPos = s2;
                              s2 = peg$FAILED;
                            }
                          } else {
                            peg$currPos = s2;
                            s2 = peg$FAILED;
                          }
                        } else {
                          peg$currPos = s2;
                          s2 = peg$FAILED;
                        }
                      } else {
                        peg$currPos = s2;
                        s2 = peg$FAILED;
                      }
                    } else {
                      peg$currPos = s2;
                      s2 = peg$FAILED;
                    }
                  } else {
                    peg$currPos = s2;
                    s2 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s2;
                  s2 = peg$FAILED;
                }
              } else {
                peg$currPos = s2;
                s2 = peg$FAILED;
              }
              if (s2 !== peg$FAILED) {
                s1 = input.substring(s1, peg$currPos);
              } else {
                s1 = s2;
              }
              if (s1 !== peg$FAILED) {
                peg$savedPos = s0;
                s1 = peg$f37(s1);
              }
              s0 = s1;
              if (s0 === peg$FAILED) {
                s0 = peg$currPos;
                s1 = peg$currPos;
                s2 = peg$currPos;
                s3 = input.charAt(peg$currPos);
                if (peg$r0.test(s3)) {
                  peg$currPos++;
                } else {
                  s3 = peg$FAILED;
                  if (peg$silentFails === 0) {
                    peg$fail(peg$e2);
                  }
                }
                if (s3 !== peg$FAILED) {
                  s4 = input.charAt(peg$currPos);
                  if (peg$r0.test(s4)) {
                    peg$currPos++;
                  } else {
                    s4 = peg$FAILED;
                    if (peg$silentFails === 0) {
                      peg$fail(peg$e2);
                    }
                  }
                  if (s4 !== peg$FAILED) {
                    s5 = input.charAt(peg$currPos);
                    if (peg$r0.test(s5)) {
                      peg$currPos++;
                    } else {
                      s5 = peg$FAILED;
                      if (peg$silentFails === 0) {
                        peg$fail(peg$e2);
                      }
                    }
                    if (s5 !== peg$FAILED) {
                      s6 = input.charAt(peg$currPos);
                      if (peg$r0.test(s6)) {
                        peg$currPos++;
                      } else {
                        s6 = peg$FAILED;
                        if (peg$silentFails === 0) {
                          peg$fail(peg$e2);
                        }
                      }
                      if (s6 !== peg$FAILED) {
                        if (input.charCodeAt(peg$currPos) === 45) {
                          s7 = peg$c11;
                          peg$currPos++;
                        } else {
                          s7 = peg$FAILED;
                          if (peg$silentFails === 0) {
                            peg$fail(peg$e12);
                          }
                        }
                        if (s7 !== peg$FAILED) {
                          s8 = input.charAt(peg$currPos);
                          if (peg$r0.test(s8)) {
                            peg$currPos++;
                          } else {
                            s8 = peg$FAILED;
                            if (peg$silentFails === 0) {
                              peg$fail(peg$e2);
                            }
                          }
                          if (s8 !== peg$FAILED) {
                            s9 = input.charAt(peg$currPos);
                            if (peg$r0.test(s9)) {
                              peg$currPos++;
                            } else {
                              s9 = peg$FAILED;
                              if (peg$silentFails === 0) {
                                peg$fail(peg$e2);
                              }
                            }
                            if (s9 !== peg$FAILED) {
                              if (input.charCodeAt(peg$currPos) === 45) {
                                s10 = peg$c11;
                                peg$currPos++;
                              } else {
                                s10 = peg$FAILED;
                                if (peg$silentFails === 0) {
                                  peg$fail(peg$e12);
                                }
                              }
                              if (s10 !== peg$FAILED) {
                                s11 = input.charAt(peg$currPos);
                                if (peg$r0.test(s11)) {
                                  peg$currPos++;
                                } else {
                                  s11 = peg$FAILED;
                                  if (peg$silentFails === 0) {
                                    peg$fail(peg$e2);
                                  }
                                }
                                if (s11 !== peg$FAILED) {
                                  s12 = input.charAt(peg$currPos);
                                  if (peg$r0.test(s12)) {
                                    peg$currPos++;
                                  } else {
                                    s12 = peg$FAILED;
                                    if (peg$silentFails === 0) {
                                      peg$fail(peg$e2);
                                    }
                                  }
                                  if (s12 !== peg$FAILED) {
                                    s13 = input.charAt(peg$currPos);
                                    if (peg$r1.test(s13)) {
                                      peg$currPos++;
                                    } else {
                                      s13 = peg$FAILED;
                                      if (peg$silentFails === 0) {
                                        peg$fail(peg$e13);
                                      }
                                    }
                                    if (s13 !== peg$FAILED) {
                                      s14 = input.charAt(peg$currPos);
                                      if (peg$r0.test(s14)) {
                                        peg$currPos++;
                                      } else {
                                        s14 = peg$FAILED;
                                        if (peg$silentFails === 0) {
                                          peg$fail(peg$e2);
                                        }
                                      }
                                      if (s14 !== peg$FAILED) {
                                        s15 = input.charAt(peg$currPos);
                                        if (peg$r0.test(s15)) {
                                          peg$currPos++;
                                        } else {
                                          s15 = peg$FAILED;
                                          if (peg$silentFails === 0) {
                                            peg$fail(peg$e2);
                                          }
                                        }
                                        if (s15 !== peg$FAILED) {
                                          s3 = [s3, s4, s5, s6, s7, s8, s9, s10, s11, s12, s13, s14, s15];
                                          s2 = s3;
                                        } else {
                                          peg$currPos = s2;
                                          s2 = peg$FAILED;
                                        }
                                      } else {
                                        peg$currPos = s2;
                                        s2 = peg$FAILED;
                                      }
                                    } else {
                                      peg$currPos = s2;
                                      s2 = peg$FAILED;
                                    }
                                  } else {
                                    peg$currPos = s2;
                                    s2 = peg$FAILED;
                                  }
                                } else {
                                  peg$currPos = s2;
                                  s2 = peg$FAILED;
                                }
                              } else {
                                peg$currPos = s2;
                                s2 = peg$FAILED;
                              }
                            } else {
                              peg$currPos = s2;
                              s2 = peg$FAILED;
                            }
                          } else {
                            peg$currPos = s2;
                            s2 = peg$FAILED;
                          }
                        } else {
                          peg$currPos = s2;
                          s2 = peg$FAILED;
                        }
                      } else {
                        peg$currPos = s2;
                        s2 = peg$FAILED;
                      }
                    } else {
                      peg$currPos = s2;
                      s2 = peg$FAILED;
                    }
                  } else {
                    peg$currPos = s2;
                    s2 = peg$FAILED;
                  }
                } else {
                  peg$currPos = s2;
                  s2 = peg$FAILED;
                }
                if (s2 !== peg$FAILED) {
                  s1 = input.substring(s1, peg$currPos);
                } else {
                  s1 = s2;
                }
                if (s1 !== peg$FAILED) {
                  peg$savedPos = s0;
                  s1 = peg$f38(s1);
                }
                s0 = s1;
                if (s0 === peg$FAILED) {
                  s0 = peg$currPos;
                  s1 = peg$currPos;
                  s2 = peg$currPos;
                  s3 = input.charAt(peg$currPos);
                  if (peg$r0.test(s3)) {
                    peg$currPos++;
                  } else {
                    s3 = peg$FAILED;
                    if (peg$silentFails === 0) {
                      peg$fail(peg$e2);
                    }
                  }
                  if (s3 !== peg$FAILED) {
                    s4 = input.charAt(peg$currPos);
                    if (peg$r0.test(s4)) {
                      peg$currPos++;
                    } else {
                      s4 = peg$FAILED;
                      if (peg$silentFails === 0) {
                        peg$fail(peg$e2);
                      }
                    }
                    if (s4 !== peg$FAILED) {
                      s5 = input.charAt(peg$currPos);
                      if (peg$r0.test(s5)) {
                        peg$currPos++;
                      } else {
                        s5 = peg$FAILED;
                        if (peg$silentFails === 0) {
                          peg$fail(peg$e2);
                        }
                      }
                      if (s5 !== peg$FAILED) {
                        s6 = input.charAt(peg$currPos);
                        if (peg$r0.test(s6)) {
                          peg$currPos++;
                        } else {
                          s6 = peg$FAILED;
                          if (peg$silentFails === 0) {
                            peg$fail(peg$e2);
                          }
                        }
                        if (s6 !== peg$FAILED) {
                          if (input.charCodeAt(peg$currPos) === 45) {
                            s7 = peg$c11;
                            peg$currPos++;
                          } else {
                            s7 = peg$FAILED;
                            if (peg$silentFails === 0) {
                              peg$fail(peg$e12);
                            }
                          }
                          if (s7 !== peg$FAILED) {
                            s8 = input.charAt(peg$currPos);
                            if (peg$r0.test(s8)) {
                              peg$currPos++;
                            } else {
                              s8 = peg$FAILED;
                              if (peg$silentFails === 0) {
                                peg$fail(peg$e2);
                              }
                            }
                            if (s8 !== peg$FAILED) {
                              s9 = input.charAt(peg$currPos);
                              if (peg$r0.test(s9)) {
                                peg$currPos++;
                              } else {
                                s9 = peg$FAILED;
                                if (peg$silentFails === 0) {
                                  peg$fail(peg$e2);
                                }
                              }
                              if (s9 !== peg$FAILED) {
                                if (input.charCodeAt(peg$currPos) === 45) {
                                  s10 = peg$c11;
                                  peg$currPos++;
                                } else {
                                  s10 = peg$FAILED;
                                  if (peg$silentFails === 0) {
                                    peg$fail(peg$e12);
                                  }
                                }
                                if (s10 !== peg$FAILED) {
                                  s11 = input.charAt(peg$currPos);
                                  if (peg$r0.test(s11)) {
                                    peg$currPos++;
                                  } else {
                                    s11 = peg$FAILED;
                                    if (peg$silentFails === 0) {
                                      peg$fail(peg$e2);
                                    }
                                  }
                                  if (s11 !== peg$FAILED) {
                                    s12 = input.charAt(peg$currPos);
                                    if (peg$r0.test(s12)) {
                                      peg$currPos++;
                                    } else {
                                      s12 = peg$FAILED;
                                      if (peg$silentFails === 0) {
                                        peg$fail(peg$e2);
                                      }
                                    }
                                    if (s12 !== peg$FAILED) {
                                      s3 = [s3, s4, s5, s6, s7, s8, s9, s10, s11, s12];
                                      s2 = s3;
                                    } else {
                                      peg$currPos = s2;
                                      s2 = peg$FAILED;
                                    }
                                  } else {
                                    peg$currPos = s2;
                                    s2 = peg$FAILED;
                                  }
                                } else {
                                  peg$currPos = s2;
                                  s2 = peg$FAILED;
                                }
                              } else {
                                peg$currPos = s2;
                                s2 = peg$FAILED;
                              }
                            } else {
                              peg$currPos = s2;
                              s2 = peg$FAILED;
                            }
                          } else {
                            peg$currPos = s2;
                            s2 = peg$FAILED;
                          }
                        } else {
                          peg$currPos = s2;
                          s2 = peg$FAILED;
                        }
                      } else {
                        peg$currPos = s2;
                        s2 = peg$FAILED;
                      }
                    } else {
                      peg$currPos = s2;
                      s2 = peg$FAILED;
                    }
                  } else {
                    peg$currPos = s2;
                    s2 = peg$FAILED;
                  }
                  if (s2 !== peg$FAILED) {
                    s1 = input.substring(s1, peg$currPos);
                  } else {
                    s1 = s2;
                  }
                  if (s1 !== peg$FAILED) {
                    peg$savedPos = s0;
                    s1 = peg$f39(s1);
                  }
                  s0 = s1;
                  if (s0 === peg$FAILED) {
                    s0 = peg$currPos;
                    s1 = peg$currPos;
                    s2 = peg$currPos;
                    s3 = input.charAt(peg$currPos);
                    if (peg$r0.test(s3)) {
                      peg$currPos++;
                    } else {
                      s3 = peg$FAILED;
                      if (peg$silentFails === 0) {
                        peg$fail(peg$e2);
                      }
                    }
                    if (s3 !== peg$FAILED) {
                      s4 = input.charAt(peg$currPos);
                      if (peg$r0.test(s4)) {
                        peg$currPos++;
                      } else {
                        s4 = peg$FAILED;
                        if (peg$silentFails === 0) {
                          peg$fail(peg$e2);
                        }
                      }
                      if (s4 !== peg$FAILED) {
                        s5 = input.charAt(peg$currPos);
                        if (peg$r0.test(s5)) {
                          peg$currPos++;
                        } else {
                          s5 = peg$FAILED;
                          if (peg$silentFails === 0) {
                            peg$fail(peg$e2);
                          }
                        }
                        if (s5 !== peg$FAILED) {
                          s6 = input.charAt(peg$currPos);
                          if (peg$r0.test(s6)) {
                            peg$currPos++;
                          } else {
                            s6 = peg$FAILED;
                            if (peg$silentFails === 0) {
                              peg$fail(peg$e2);
                            }
                          }
                          if (s6 !== peg$FAILED) {
                            if (input.charCodeAt(peg$currPos) === 45) {
                              s7 = peg$c11;
                              peg$currPos++;
                            } else {
                              s7 = peg$FAILED;
                              if (peg$silentFails === 0) {
                                peg$fail(peg$e12);
                              }
                            }
                            if (s7 !== peg$FAILED) {
                              s8 = input.charAt(peg$currPos);
                              if (peg$r0.test(s8)) {
                                peg$currPos++;
                              } else {
                                s8 = peg$FAILED;
                                if (peg$silentFails === 0) {
                                  peg$fail(peg$e2);
                                }
                              }
                              if (s8 !== peg$FAILED) {
                                s9 = input.charAt(peg$currPos);
                                if (peg$r0.test(s9)) {
                                  peg$currPos++;
                                } else {
                                  s9 = peg$FAILED;
                                  if (peg$silentFails === 0) {
                                    peg$fail(peg$e2);
                                  }
                                }
                                if (s9 !== peg$FAILED) {
                                  s3 = [s3, s4, s5, s6, s7, s8, s9];
                                  s2 = s3;
                                } else {
                                  peg$currPos = s2;
                                  s2 = peg$FAILED;
                                }
                              } else {
                                peg$currPos = s2;
                                s2 = peg$FAILED;
                              }
                            } else {
                              peg$currPos = s2;
                              s2 = peg$FAILED;
                            }
                          } else {
                            peg$currPos = s2;
                            s2 = peg$FAILED;
                          }
                        } else {
                          peg$currPos = s2;
                          s2 = peg$FAILED;
                        }
                      } else {
                        peg$currPos = s2;
                        s2 = peg$FAILED;
                      }
                    } else {
                      peg$currPos = s2;
                      s2 = peg$FAILED;
                    }
                    if (s2 !== peg$FAILED) {
                      s1 = input.substring(s1, peg$currPos);
                    } else {
                      s1 = s2;
                    }
                    if (s1 !== peg$FAILED) {
                      peg$savedPos = s0;
                      s1 = peg$f40(s1);
                    }
                    s0 = s1;
                    if (s0 === peg$FAILED) {
                      s0 = peg$currPos;
                      s1 = peg$currPos;
                      s2 = peg$currPos;
                      s3 = input.charAt(peg$currPos);
                      if (peg$r0.test(s3)) {
                        peg$currPos++;
                      } else {
                        s3 = peg$FAILED;
                        if (peg$silentFails === 0) {
                          peg$fail(peg$e2);
                        }
                      }
                      if (s3 !== peg$FAILED) {
                        s4 = input.charAt(peg$currPos);
                        if (peg$r0.test(s4)) {
                          peg$currPos++;
                        } else {
                          s4 = peg$FAILED;
                          if (peg$silentFails === 0) {
                            peg$fail(peg$e2);
                          }
                        }
                        if (s4 !== peg$FAILED) {
                          s5 = input.charAt(peg$currPos);
                          if (peg$r0.test(s5)) {
                            peg$currPos++;
                          } else {
                            s5 = peg$FAILED;
                            if (peg$silentFails === 0) {
                              peg$fail(peg$e2);
                            }
                          }
                          if (s5 !== peg$FAILED) {
                            s6 = input.charAt(peg$currPos);
                            if (peg$r0.test(s6)) {
                              peg$currPos++;
                            } else {
                              s6 = peg$FAILED;
                              if (peg$silentFails === 0) {
                                peg$fail(peg$e2);
                              }
                            }
                            if (s6 !== peg$FAILED) {
                              s3 = [s3, s4, s5, s6];
                              s2 = s3;
                            } else {
                              peg$currPos = s2;
                              s2 = peg$FAILED;
                            }
                          } else {
                            peg$currPos = s2;
                            s2 = peg$FAILED;
                          }
                        } else {
                          peg$currPos = s2;
                          s2 = peg$FAILED;
                        }
                      } else {
                        peg$currPos = s2;
                        s2 = peg$FAILED;
                      }
                      if (s2 !== peg$FAILED) {
                        s1 = input.substring(s1, peg$currPos);
                      } else {
                        s1 = s2;
                      }
                      if (s1 !== peg$FAILED) {
                        peg$savedPos = s0;
                        s1 = peg$f41(s1);
                      }
                      s0 = s1;
                    }
                  }
                }
              }
            }
          }
        }
        return s0;
      }
      function peg$parseweekday() {
        var s0, s1, s2;
        s0 = peg$currPos;
        s1 = peg$currPos;
        s2 = peg$parseMONDAY();
        if (s2 === peg$FAILED) {
          s2 = peg$parseTUESDAY();
          if (s2 === peg$FAILED) {
            s2 = peg$parseWEDNESDAY();
            if (s2 === peg$FAILED) {
              s2 = peg$parseTHURSDAY();
              if (s2 === peg$FAILED) {
                s2 = peg$parseFRIDAY();
                if (s2 === peg$FAILED) {
                  s2 = peg$parseSATURDAY();
                  if (s2 === peg$FAILED) {
                    s2 = peg$parseSUNDAY();
                  }
                }
              }
            }
          }
        }
        if (s2 !== peg$FAILED) {
          s1 = input.substring(s1, peg$currPos);
        } else {
          s1 = s2;
        }
        if (s1 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$f42(s1);
        }
        s0 = s1;
        return s0;
      }
      function peg$parseconjunction() {
        var s0, s1;
        s0 = peg$currPos;
        s1 = peg$parseOR();
        if (s1 !== peg$FAILED) {
          peg$savedPos = s0;
          s1 = peg$f43();
        }
        s0 = s1;
        if (s0 === peg$FAILED) {
          s0 = peg$currPos;
          s1 = peg$parseAND();
          if (s1 !== peg$FAILED) {
            peg$savedPos = s0;
            s1 = peg$f44();
          }
          s0 = s1;
        }
        return s0;
      }
      function peg$parseNOT() {
        var s0, s1, s2, s3;
        s0 = peg$currPos;
        s1 = input.substr(peg$currPos, 3);
        if (s1.toLowerCase() === peg$c13) {
          peg$currPos += 3;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) {
            peg$fail(peg$e20);
          }
        }
        if (s1 !== peg$FAILED) {
          s2 = peg$currPos;
          peg$silentFails++;
          s3 = peg$parseidChar();
          peg$silentFails--;
          if (s3 === peg$FAILED) {
            s2 = void 0;
          } else {
            peg$currPos = s2;
            s2 = peg$FAILED;
          }
          if (s2 !== peg$FAILED) {
            s1 = [s1, s2];
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
        return s0;
      }
      function peg$parseNULL() {
        var s0, s1, s2, s3;
        s0 = peg$currPos;
        s1 = input.substr(peg$currPos, 4);
        if (s1.toLowerCase() === peg$c14) {
          peg$currPos += 4;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) {
            peg$fail(peg$e21);
          }
        }
        if (s1 !== peg$FAILED) {
          s2 = peg$currPos;
          peg$silentFails++;
          s3 = peg$parseidChar();
          peg$silentFails--;
          if (s3 === peg$FAILED) {
            s2 = void 0;
          } else {
            peg$currPos = s2;
            s2 = peg$FAILED;
          }
          if (s2 !== peg$FAILED) {
            s1 = [s1, s2];
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
        return s0;
      }
      function peg$parseNONE() {
        var s0, s1, s2, s3;
        s0 = peg$currPos;
        s1 = input.substr(peg$currPos, 4);
        if (s1.toLowerCase() === peg$c15) {
          peg$currPos += 4;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) {
            peg$fail(peg$e22);
          }
        }
        if (s1 !== peg$FAILED) {
          s2 = peg$currPos;
          peg$silentFails++;
          s3 = peg$parseidChar();
          peg$silentFails--;
          if (s3 === peg$FAILED) {
            s2 = void 0;
          } else {
            peg$currPos = s2;
            s2 = peg$FAILED;
          }
          if (s2 !== peg$FAILED) {
            s1 = [s1, s2];
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
        return s0;
      }
      function peg$parseTO() {
        var s0, s1, s2, s3;
        s0 = peg$currPos;
        s1 = input.substr(peg$currPos, 2);
        if (s1.toLowerCase() === peg$c16) {
          peg$currPos += 2;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) {
            peg$fail(peg$e23);
          }
        }
        if (s1 !== peg$FAILED) {
          s2 = peg$currPos;
          peg$silentFails++;
          s3 = peg$parseidChar();
          peg$silentFails--;
          if (s3 === peg$FAILED) {
            s2 = void 0;
          } else {
            peg$currPos = s2;
            s2 = peg$FAILED;
          }
          if (s2 !== peg$FAILED) {
            s1 = [s1, s2];
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
        return s0;
      }
      function peg$parseNOW() {
        var s0, s1, s2, s3;
        s0 = peg$currPos;
        s1 = input.substr(peg$currPos, 3);
        if (s1.toLowerCase() === peg$c17) {
          peg$currPos += 3;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) {
            peg$fail(peg$e24);
          }
        }
        if (s1 !== peg$FAILED) {
          s2 = peg$currPos;
          peg$silentFails++;
          s3 = peg$parseidChar();
          peg$silentFails--;
          if (s3 === peg$FAILED) {
            s2 = void 0;
          } else {
            peg$currPos = s2;
            s2 = peg$FAILED;
          }
          if (s2 !== peg$FAILED) {
            s1 = [s1, s2];
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
        return s0;
      }
      function peg$parseLAST() {
        var s0, s1, s2, s3;
        s0 = peg$currPos;
        s1 = input.substr(peg$currPos, 4);
        if (s1.toLowerCase() === peg$c18) {
          peg$currPos += 4;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) {
            peg$fail(peg$e25);
          }
        }
        if (s1 !== peg$FAILED) {
          s2 = peg$currPos;
          peg$silentFails++;
          s3 = peg$parseidChar();
          peg$silentFails--;
          if (s3 === peg$FAILED) {
            s2 = void 0;
          } else {
            peg$currPos = s2;
            s2 = peg$FAILED;
          }
          if (s2 !== peg$FAILED) {
            s1 = [s1, s2];
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
        return s0;
      }
      function peg$parseTHIS() {
        var s0, s1, s2, s3;
        s0 = peg$currPos;
        s1 = input.substr(peg$currPos, 4);
        if (s1.toLowerCase() === peg$c19) {
          peg$currPos += 4;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) {
            peg$fail(peg$e26);
          }
        }
        if (s1 !== peg$FAILED) {
          s2 = peg$currPos;
          peg$silentFails++;
          s3 = peg$parseidChar();
          peg$silentFails--;
          if (s3 === peg$FAILED) {
            s2 = void 0;
          } else {
            peg$currPos = s2;
            s2 = peg$FAILED;
          }
          if (s2 !== peg$FAILED) {
            s1 = [s1, s2];
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
        return s0;
      }
      function peg$parseNEXT() {
        var s0, s1, s2, s3;
        s0 = peg$currPos;
        s1 = input.substr(peg$currPos, 4);
        if (s1.toLowerCase() === peg$c20) {
          peg$currPos += 4;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) {
            peg$fail(peg$e27);
          }
        }
        if (s1 !== peg$FAILED) {
          s2 = peg$currPos;
          peg$silentFails++;
          s3 = peg$parseidChar();
          peg$silentFails--;
          if (s3 === peg$FAILED) {
            s2 = void 0;
          } else {
            peg$currPos = s2;
            s2 = peg$FAILED;
          }
          if (s2 !== peg$FAILED) {
            s1 = [s1, s2];
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
        return s0;
      }
      function peg$parseAGO() {
        var s0, s1, s2, s3;
        s0 = peg$currPos;
        s1 = input.substr(peg$currPos, 3);
        if (s1.toLowerCase() === peg$c21) {
          peg$currPos += 3;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) {
            peg$fail(peg$e28);
          }
        }
        if (s1 !== peg$FAILED) {
          s2 = peg$currPos;
          peg$silentFails++;
          s3 = peg$parseidChar();
          peg$silentFails--;
          if (s3 === peg$FAILED) {
            s2 = void 0;
          } else {
            peg$currPos = s2;
            s2 = peg$FAILED;
          }
          if (s2 !== peg$FAILED) {
            s1 = [s1, s2];
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
        return s0;
      }
      function peg$parseFROM() {
        var s0, s1, s2, s3;
        s0 = peg$currPos;
        s1 = input.substr(peg$currPos, 4);
        if (s1.toLowerCase() === peg$c22) {
          peg$currPos += 4;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) {
            peg$fail(peg$e29);
          }
        }
        if (s1 !== peg$FAILED) {
          s2 = peg$currPos;
          peg$silentFails++;
          s3 = peg$parseidChar();
          peg$silentFails--;
          if (s3 === peg$FAILED) {
            s2 = void 0;
          } else {
            peg$currPos = s2;
            s2 = peg$FAILED;
          }
          if (s2 !== peg$FAILED) {
            s1 = [s1, s2];
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
        return s0;
      }
      function peg$parseBEFORE() {
        var s0, s1, s2, s3;
        s0 = peg$currPos;
        s1 = input.substr(peg$currPos, 6);
        if (s1.toLowerCase() === peg$c23) {
          peg$currPos += 6;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) {
            peg$fail(peg$e30);
          }
        }
        if (s1 !== peg$FAILED) {
          s2 = peg$currPos;
          peg$silentFails++;
          s3 = peg$parseidChar();
          peg$silentFails--;
          if (s3 === peg$FAILED) {
            s2 = void 0;
          } else {
            peg$currPos = s2;
            s2 = peg$FAILED;
          }
          if (s2 !== peg$FAILED) {
            s1 = [s1, s2];
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
        return s0;
      }
      function peg$parseAFTER() {
        var s0, s1, s2, s3;
        s0 = peg$currPos;
        s1 = input.substr(peg$currPos, 5);
        if (s1.toLowerCase() === peg$c24) {
          peg$currPos += 5;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) {
            peg$fail(peg$e31);
          }
        }
        if (s1 !== peg$FAILED) {
          s2 = peg$currPos;
          peg$silentFails++;
          s3 = peg$parseidChar();
          peg$silentFails--;
          if (s3 === peg$FAILED) {
            s2 = void 0;
          } else {
            peg$currPos = s2;
            s2 = peg$FAILED;
          }
          if (s2 !== peg$FAILED) {
            s1 = [s1, s2];
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
        return s0;
      }
      function peg$parseTHROUGH() {
        var s0, s1, s2, s3;
        s0 = peg$currPos;
        s1 = input.substr(peg$currPos, 7);
        if (s1.toLowerCase() === peg$c25) {
          peg$currPos += 7;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) {
            peg$fail(peg$e32);
          }
        }
        if (s1 !== peg$FAILED) {
          s2 = peg$currPos;
          peg$silentFails++;
          s3 = peg$parseidChar();
          peg$silentFails--;
          if (s3 === peg$FAILED) {
            s2 = void 0;
          } else {
            peg$currPos = s2;
            s2 = peg$FAILED;
          }
          if (s2 !== peg$FAILED) {
            s1 = [s1, s2];
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
        return s0;
      }
      function peg$parseSTARTING() {
        var s0, s1, s2, s3;
        s0 = peg$currPos;
        s1 = input.substr(peg$currPos, 8);
        if (s1.toLowerCase() === peg$c26) {
          peg$currPos += 8;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) {
            peg$fail(peg$e33);
          }
        }
        if (s1 !== peg$FAILED) {
          s2 = peg$currPos;
          peg$silentFails++;
          s3 = peg$parseidChar();
          peg$silentFails--;
          if (s3 === peg$FAILED) {
            s2 = void 0;
          } else {
            peg$currPos = s2;
            s2 = peg$FAILED;
          }
          if (s2 !== peg$FAILED) {
            s1 = [s1, s2];
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
        return s0;
      }
      function peg$parseFOR() {
        var s0, s1, s2, s3;
        s0 = peg$currPos;
        s1 = input.substr(peg$currPos, 3);
        if (s1.toLowerCase() === peg$c27) {
          peg$currPos += 3;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) {
            peg$fail(peg$e34);
          }
        }
        if (s1 !== peg$FAILED) {
          s2 = peg$currPos;
          peg$silentFails++;
          s3 = peg$parseidChar();
          peg$silentFails--;
          if (s3 === peg$FAILED) {
            s2 = void 0;
          } else {
            peg$currPos = s2;
            s2 = peg$FAILED;
          }
          if (s2 !== peg$FAILED) {
            s1 = [s1, s2];
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
        return s0;
      }
      function peg$parseTODAY() {
        var s0, s1, s2, s3;
        s0 = peg$currPos;
        s1 = input.substr(peg$currPos, 5);
        if (s1.toLowerCase() === peg$c28) {
          peg$currPos += 5;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) {
            peg$fail(peg$e35);
          }
        }
        if (s1 !== peg$FAILED) {
          s2 = peg$currPos;
          peg$silentFails++;
          s3 = peg$parseidChar();
          peg$silentFails--;
          if (s3 === peg$FAILED) {
            s2 = void 0;
          } else {
            peg$currPos = s2;
            s2 = peg$FAILED;
          }
          if (s2 !== peg$FAILED) {
            s1 = [s1, s2];
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
        return s0;
      }
      function peg$parseYESTERDAY() {
        var s0, s1, s2, s3;
        s0 = peg$currPos;
        s1 = input.substr(peg$currPos, 9);
        if (s1.toLowerCase() === peg$c29) {
          peg$currPos += 9;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) {
            peg$fail(peg$e36);
          }
        }
        if (s1 !== peg$FAILED) {
          s2 = peg$currPos;
          peg$silentFails++;
          s3 = peg$parseidChar();
          peg$silentFails--;
          if (s3 === peg$FAILED) {
            s2 = void 0;
          } else {
            peg$currPos = s2;
            s2 = peg$FAILED;
          }
          if (s2 !== peg$FAILED) {
            s1 = [s1, s2];
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
        return s0;
      }
      function peg$parseTOMORROW() {
        var s0, s1, s2, s3;
        s0 = peg$currPos;
        s1 = input.substr(peg$currPos, 8);
        if (s1.toLowerCase() === peg$c30) {
          peg$currPos += 8;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) {
            peg$fail(peg$e37);
          }
        }
        if (s1 !== peg$FAILED) {
          s2 = peg$currPos;
          peg$silentFails++;
          s3 = peg$parseidChar();
          peg$silentFails--;
          if (s3 === peg$FAILED) {
            s2 = void 0;
          } else {
            peg$currPos = s2;
            s2 = peg$FAILED;
          }
          if (s2 !== peg$FAILED) {
            s1 = [s1, s2];
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
        return s0;
      }
      function peg$parseAND() {
        var s0, s1, s2, s3;
        s0 = peg$currPos;
        s1 = input.substr(peg$currPos, 3);
        if (s1.toLowerCase() === peg$c31) {
          peg$currPos += 3;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) {
            peg$fail(peg$e38);
          }
        }
        if (s1 !== peg$FAILED) {
          s2 = peg$currPos;
          peg$silentFails++;
          s3 = peg$parseidChar();
          peg$silentFails--;
          if (s3 === peg$FAILED) {
            s2 = void 0;
          } else {
            peg$currPos = s2;
            s2 = peg$FAILED;
          }
          if (s2 !== peg$FAILED) {
            s1 = [s1, s2];
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
        return s0;
      }
      function peg$parseOR() {
        var s0, s1, s2, s3;
        s0 = peg$currPos;
        s1 = input.substr(peg$currPos, 2);
        if (s1.toLowerCase() === peg$c32) {
          peg$currPos += 2;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) {
            peg$fail(peg$e39);
          }
        }
        if (s1 !== peg$FAILED) {
          s2 = peg$currPos;
          peg$silentFails++;
          s3 = peg$parseidChar();
          peg$silentFails--;
          if (s3 === peg$FAILED) {
            s2 = void 0;
          } else {
            peg$currPos = s2;
            s2 = peg$FAILED;
          }
          if (s2 !== peg$FAILED) {
            s1 = [s1, s2];
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
        return s0;
      }
      function peg$parseMONDAY() {
        var s0, s1, s2, s3;
        s0 = peg$currPos;
        s1 = input.substr(peg$currPos, 6);
        if (s1.toLowerCase() === peg$c33) {
          peg$currPos += 6;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) {
            peg$fail(peg$e40);
          }
        }
        if (s1 !== peg$FAILED) {
          s2 = peg$currPos;
          peg$silentFails++;
          s3 = peg$parseidChar();
          peg$silentFails--;
          if (s3 === peg$FAILED) {
            s2 = void 0;
          } else {
            peg$currPos = s2;
            s2 = peg$FAILED;
          }
          if (s2 !== peg$FAILED) {
            s1 = [s1, s2];
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
        return s0;
      }
      function peg$parseTUESDAY() {
        var s0, s1, s2, s3;
        s0 = peg$currPos;
        s1 = input.substr(peg$currPos, 7);
        if (s1.toLowerCase() === peg$c34) {
          peg$currPos += 7;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) {
            peg$fail(peg$e41);
          }
        }
        if (s1 !== peg$FAILED) {
          s2 = peg$currPos;
          peg$silentFails++;
          s3 = peg$parseidChar();
          peg$silentFails--;
          if (s3 === peg$FAILED) {
            s2 = void 0;
          } else {
            peg$currPos = s2;
            s2 = peg$FAILED;
          }
          if (s2 !== peg$FAILED) {
            s1 = [s1, s2];
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
        return s0;
      }
      function peg$parseWEDNESDAY() {
        var s0, s1, s2, s3;
        s0 = peg$currPos;
        s1 = input.substr(peg$currPos, 9);
        if (s1.toLowerCase() === peg$c35) {
          peg$currPos += 9;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) {
            peg$fail(peg$e42);
          }
        }
        if (s1 !== peg$FAILED) {
          s2 = peg$currPos;
          peg$silentFails++;
          s3 = peg$parseidChar();
          peg$silentFails--;
          if (s3 === peg$FAILED) {
            s2 = void 0;
          } else {
            peg$currPos = s2;
            s2 = peg$FAILED;
          }
          if (s2 !== peg$FAILED) {
            s1 = [s1, s2];
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
        return s0;
      }
      function peg$parseTHURSDAY() {
        var s0, s1, s2, s3;
        s0 = peg$currPos;
        s1 = input.substr(peg$currPos, 8);
        if (s1.toLowerCase() === peg$c36) {
          peg$currPos += 8;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) {
            peg$fail(peg$e43);
          }
        }
        if (s1 !== peg$FAILED) {
          s2 = peg$currPos;
          peg$silentFails++;
          s3 = peg$parseidChar();
          peg$silentFails--;
          if (s3 === peg$FAILED) {
            s2 = void 0;
          } else {
            peg$currPos = s2;
            s2 = peg$FAILED;
          }
          if (s2 !== peg$FAILED) {
            s1 = [s1, s2];
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
        return s0;
      }
      function peg$parseFRIDAY() {
        var s0, s1, s2, s3;
        s0 = peg$currPos;
        s1 = input.substr(peg$currPos, 6);
        if (s1.toLowerCase() === peg$c37) {
          peg$currPos += 6;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) {
            peg$fail(peg$e44);
          }
        }
        if (s1 !== peg$FAILED) {
          s2 = peg$currPos;
          peg$silentFails++;
          s3 = peg$parseidChar();
          peg$silentFails--;
          if (s3 === peg$FAILED) {
            s2 = void 0;
          } else {
            peg$currPos = s2;
            s2 = peg$FAILED;
          }
          if (s2 !== peg$FAILED) {
            s1 = [s1, s2];
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
        return s0;
      }
      function peg$parseSATURDAY() {
        var s0, s1, s2, s3;
        s0 = peg$currPos;
        s1 = input.substr(peg$currPos, 8);
        if (s1.toLowerCase() === peg$c38) {
          peg$currPos += 8;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) {
            peg$fail(peg$e45);
          }
        }
        if (s1 !== peg$FAILED) {
          s2 = peg$currPos;
          peg$silentFails++;
          s3 = peg$parseidChar();
          peg$silentFails--;
          if (s3 === peg$FAILED) {
            s2 = void 0;
          } else {
            peg$currPos = s2;
            s2 = peg$FAILED;
          }
          if (s2 !== peg$FAILED) {
            s1 = [s1, s2];
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
        return s0;
      }
      function peg$parseSUNDAY() {
        var s0, s1, s2, s3;
        s0 = peg$currPos;
        s1 = input.substr(peg$currPos, 6);
        if (s1.toLowerCase() === peg$c39) {
          peg$currPos += 6;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) {
            peg$fail(peg$e46);
          }
        }
        if (s1 !== peg$FAILED) {
          s2 = peg$currPos;
          peg$silentFails++;
          s3 = peg$parseidChar();
          peg$silentFails--;
          if (s3 === peg$FAILED) {
            s2 = void 0;
          } else {
            peg$currPos = s2;
            s2 = peg$FAILED;
          }
          if (s2 !== peg$FAILED) {
            s1 = [s1, s2];
            s0 = s1;
          } else {
            peg$currPos = s0;
            s0 = peg$FAILED;
          }
        } else {
          peg$currPos = s0;
          s0 = peg$FAILED;
        }
        return s0;
      }
      function peg$parseidChar() {
        var s0;
        s0 = input.charAt(peg$currPos);
        if (peg$r7.test(s0)) {
          peg$currPos++;
        } else {
          s0 = peg$FAILED;
          if (peg$silentFails === 0) {
            peg$fail(peg$e47);
          }
        }
        return s0;
      }
      function peg$parse_() {
        var s0, s1;
        peg$silentFails++;
        s0 = [];
        s1 = input.charAt(peg$currPos);
        if (peg$r8.test(s1)) {
          peg$currPos++;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) {
            peg$fail(peg$e49);
          }
        }
        while (s1 !== peg$FAILED) {
          s0.push(s1);
          s1 = input.charAt(peg$currPos);
          if (peg$r8.test(s1)) {
            peg$currPos++;
          } else {
            s1 = peg$FAILED;
            if (peg$silentFails === 0) {
              peg$fail(peg$e49);
            }
          }
        }
        peg$silentFails--;
        s1 = peg$FAILED;
        if (peg$silentFails === 0) {
          peg$fail(peg$e48);
        }
        return s0;
      }
      function peg$parse__() {
        var s0, s1;
        peg$silentFails++;
        s0 = [];
        s1 = input.charAt(peg$currPos);
        if (peg$r8.test(s1)) {
          peg$currPos++;
        } else {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) {
            peg$fail(peg$e49);
          }
        }
        if (s1 !== peg$FAILED) {
          while (s1 !== peg$FAILED) {
            s0.push(s1);
            s1 = input.charAt(peg$currPos);
            if (peg$r8.test(s1)) {
              peg$currPos++;
            } else {
              s1 = peg$FAILED;
              if (peg$silentFails === 0) {
                peg$fail(peg$e49);
              }
            }
          }
        } else {
          s0 = peg$FAILED;
        }
        peg$silentFails--;
        if (s0 === peg$FAILED) {
          s1 = peg$FAILED;
          if (peg$silentFails === 0) {
            peg$fail(peg$e50);
          }
        }
        return s0;
      }
      peg$result = peg$startRuleFunction();
      if (options.peg$library) {
        return (
          /** @type {any} */
          {
            peg$result,
            peg$currPos,
            peg$FAILED,
            peg$maxFailExpected,
            peg$maxFailPos
          }
        );
      }
      if (peg$result !== peg$FAILED && peg$currPos === input.length) {
        return peg$result;
      } else {
        if (peg$result !== peg$FAILED && peg$currPos < input.length) {
          peg$fail(peg$endExpectation());
        }
        throw peg$buildStructuredError(peg$maxFailExpected, peg$maxFailPos < input.length ? input.charAt(peg$maxFailPos) : null, peg$maxFailPos < input.length ? peg$computeLocation(peg$maxFailPos, peg$maxFailPos + 1) : peg$computeLocation(peg$maxFailPos, peg$maxFailPos));
      }
    }
    module.exports = {
      StartRules: ["temporalFilter"],
      SyntaxError: peg$SyntaxError,
      parse: peg$parse
    };
  }
});

// node_modules/@malloydata/malloy-filter/dist/temporal_filter_expression.js
var require_temporal_filter_expression = __commonJS({
  "node_modules/@malloydata/malloy-filter/dist/temporal_filter_expression.js"(exports) {
    "use strict";
    Object.defineProperty(exports, "__esModule", { value: true });
    exports.TemporalFilterExpression = void 0;
    var filter_interface_1 = require_filter_interface();
    var ftemporal_parser_1 = require_ftemporal_parser();
    var peggy_parse_1 = require_peggy_parse();
    exports.TemporalFilterExpression = {
      parse(src) {
        if (src.match(/^\s*$/)) {
          return { parsed: null, log: [] };
        }
        const parse_result = (0, peggy_parse_1.run_parser)(src, ftemporal_parser_1.parse);
        if (parse_result.parsed && (0, filter_interface_1.isTemporalFilter)(parse_result.parsed)) {
          return { parsed: parse_result.parsed, log: [] };
        }
        return { parsed: null, log: parse_result.log };
      },
      unparse(tc) {
        if (tc === null) {
          return "";
        }
        switch (tc.operator) {
          case "null":
            return notStr(tc, "null");
          case "none":
            return notStr(tc, "none");
          case "in": {
            return notStr(tc, momentToStr(tc.in));
          }
          case "()":
            return notStr(tc, "(" + exports.TemporalFilterExpression.unparse(tc.expr) + ")");
          case "in_last":
            return notStr(tc, durStr(tc));
          case "last":
          case "next":
            return notStr(tc, `${tc.operator} ${durStr(tc)}`);
          case "before":
            return `${tc.not ? "starting" : "before"} ${momentToStr(tc.before)}`;
          case "after":
            return `${tc.not ? "through" : "after"} ${momentToStr(tc.after)}`;
          case "to":
            return notStr(tc, `${momentToStr(tc.fromMoment)} to ${momentToStr(tc.toMoment)}`);
          case "for":
            return notStr(tc, `${momentToStr(tc.begin)} for ${durStr(tc)}`);
          case "or":
            return tc.members.map((or) => exports.TemporalFilterExpression.unparse(or)).join(" or ");
          case "and":
            return tc.members.map((and) => exports.TemporalFilterExpression.unparse(and)).join(" and ");
        }
      }
    };
    function notStr(tc, s) {
      if ("not" in tc && tc.not) {
        return "not " + s;
      }
      return s;
    }
    function durStr(d) {
      return d.n === "1" ? `1 ${d.units}` : `${d.n} ${d.units}s`;
    }
    function momentToStr(m) {
      switch (m.moment) {
        case "literal":
          return m.literal;
        case "now":
        case "today":
        case "yesterday":
        case "tomorrow":
          return m.moment;
        case "monday":
        case "tuesday":
        case "wednesday":
        case "thursday":
        case "friday":
        case "saturday":
        case "sunday":
          return m.which === "next" ? "next " + m.moment : m.moment;
        case "this":
        case "next":
        case "last":
          return `${m.moment} ${m.units}`;
        case "ago":
          return `${durStr(m)} ago`;
        case "from_now":
          return `${durStr(m)} from now`;
      }
    }
  }
});

// node_modules/@malloydata/malloy-filter/dist/index.js
var require_dist = __commonJS({
  "node_modules/@malloydata/malloy-filter/dist/index.js"(exports) {
    "use strict";
    var __createBinding = exports && exports.__createBinding || (Object.create ? (function(o, m, k, k2) {
      if (k2 === void 0) k2 = k;
      var desc = Object.getOwnPropertyDescriptor(m, k);
      if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
        desc = { enumerable: true, get: function() {
          return m[k];
        } };
      }
      Object.defineProperty(o, k2, desc);
    }) : (function(o, m, k, k2) {
      if (k2 === void 0) k2 = k;
      o[k2] = m[k];
    }));
    var __exportStar = exports && exports.__exportStar || function(m, exports2) {
      for (var p in m) if (p !== "default" && !Object.prototype.hasOwnProperty.call(exports2, p)) __createBinding(exports2, m, p);
    };
    Object.defineProperty(exports, "__esModule", { value: true });
    exports.TemporalFilterExpression = exports.StringFilterExpression = exports.NumberFilterExpression = exports.BooleanFilterExpression = void 0;
    __exportStar(require_filter_interface(), exports);
    var boolean_filter_expression_1 = require_boolean_filter_expression();
    Object.defineProperty(exports, "BooleanFilterExpression", { enumerable: true, get: function() {
      return boolean_filter_expression_1.BooleanFilterExpression;
    } });
    var number_filter_expression_1 = require_number_filter_expression();
    Object.defineProperty(exports, "NumberFilterExpression", { enumerable: true, get: function() {
      return number_filter_expression_1.NumberFilterExpression;
    } });
    var string_filter_expression_1 = require_string_filter_expression();
    Object.defineProperty(exports, "StringFilterExpression", { enumerable: true, get: function() {
      return string_filter_expression_1.StringFilterExpression;
    } });
    var temporal_filter_expression_1 = require_temporal_filter_expression();
    Object.defineProperty(exports, "TemporalFilterExpression", { enumerable: true, get: function() {
      return temporal_filter_expression_1.TemporalFilterExpression;
    } });
  }
});

// vendor-entry.mjs
var import_malloy_filter = __toESM(require_dist(), 1);
var export_NumberFilterExpression = import_malloy_filter.NumberFilterExpression;
var export_StringFilterExpression = import_malloy_filter.StringFilterExpression;
export {
  export_NumberFilterExpression as NumberFilterExpression,
  export_StringFilterExpression as StringFilterExpression
};
