// ../typescript/src/value.ts
class ReplyError extends Error {
  constructor(message) {
    super(message);
    this.name = "ReplyError";
  }
}
var dec = new TextDecoder;
function checkError(v) {
  if (v.kind.case === "error")
    throw new ReplyError(v.kind.value);
  return v;
}
function isNull(v) {
  return v.kind.case === "null";
}
function asString(v) {
  checkError(v);
  switch (v.kind.case) {
    case "null":
      return null;
    case "blobString":
      return dec.decode(v.kind.value);
    case "simpleString":
    case "bigNumber":
      return v.kind.value;
    case "verbatim":
      return dec.decode(v.kind.value.payload);
    case "int":
      return v.kind.value.toString();
    case "double":
      return String(v.kind.value);
    case "bool":
      return v.kind.value ? "1" : "0";
    default:
      throw new ReplyError(`unexpected aggregate reply type "${v.kind.case ?? "unset"}"`);
  }
}
function asBytes(v) {
  checkError(v);
  switch (v.kind.case) {
    case "null":
      return null;
    case "blobString":
      return v.kind.value;
    case "verbatim":
      return v.kind.value.payload;
    default: {
      const s = asString(v);
      return s === null ? null : new TextEncoder().encode(s);
    }
  }
}
function asBigInt(v) {
  checkError(v);
  if (v.kind.case === "int")
    return v.kind.value;
  const s = asString(v);
  if (s === null)
    throw new ReplyError("expected integer reply, got null");
  try {
    return BigInt(s);
  } catch {
    throw new ReplyError(`expected integer reply, got "${s}"`);
  }
}
function asInt(v) {
  return Number(asBigInt(v));
}
function asDouble(v) {
  checkError(v);
  if (v.kind.case === "null")
    return null;
  if (v.kind.case === "double")
    return v.kind.value;
  if (v.kind.case === "int")
    return Number(v.kind.value);
  const s = asString(v);
  if (s === null)
    return null;
  const n = Number(s);
  if (Number.isNaN(n) && s !== "nan" && s !== "-nan") {
    throw new ReplyError(`expected double reply, got "${s}"`);
  }
  return n;
}
function asBool(v) {
  checkError(v);
  if (v.kind.case === "bool")
    return v.kind.value;
  return asBigInt(v) !== 0n;
}
function elements(v) {
  checkError(v);
  switch (v.kind.case) {
    case "array":
    case "set":
    case "push":
      return v.kind.value.elems;
    default:
      throw new ReplyError(`expected array reply, got "${v.kind.case ?? "unset"}"`);
  }
}
function asStringArray(v) {
  return elements(v).map(asString);
}
function asStringMap(v) {
  checkError(v);
  const out = {};
  if (v.kind.case === "map") {
    for (const p of v.kind.value.pairs) {
      const k = p.key ? asString(p.key) : null;
      const val = p.value ? asString(p.value) : null;
      if (k !== null && val !== null)
        out[k] = val;
    }
    return out;
  }
  const flat = elements(v);
  for (let i = 0;i + 1 < flat.length; i += 2) {
    const k = asString(flat[i]);
    const val = asString(flat[i + 1]);
    if (k !== null && val !== null)
      out[k] = val;
  }
  return out;
}
function asScoredMembers(v) {
  const flat = elements(v);
  const out = [];
  if (flat.every((e) => e.kind.case === "array" || e.kind.case === "set")) {
    for (const pair of flat) {
      const elems = elements(pair);
      const member = elems[0] ? asString(elems[0]) : null;
      const score = elems[1] ? asDouble(elems[1]) : null;
      if (member !== null && score !== null)
        out.push({ member, score });
    }
    return out;
  }
  for (let i = 0;i + 1 < flat.length; i += 2) {
    const member = asString(flat[i]);
    const score = asDouble(flat[i + 1]);
    if (member !== null && score !== null)
      out.push({ member, score });
  }
  return out;
}
// ../typescript/src/storage.ts
class MemoryStorage {
  m = new Map;
  getItem(key) {
    return this.m.get(key) ?? null;
  }
  setItem(key, value) {
    this.m.set(key, value);
  }
  removeItem(key) {
    this.m.delete(key);
  }
}
function defaultStorage() {
  try {
    const ls = globalThis.localStorage;
    if (ls) {
      const probe = "__ultima_probe__";
      ls.setItem(probe, "1");
      ls.removeItem(probe);
      return ls;
    }
  } catch {}
  return new MemoryStorage;
}
// node_modules/@bufbuild/protobuf/dist/esm/is-message.js
function isMessage(arg, schema) {
  const isMessage2 = arg !== null && typeof arg == "object" && "$typeName" in arg && typeof arg.$typeName == "string";
  if (!isMessage2) {
    return false;
  }
  if (schema === undefined) {
    return true;
  }
  return schema.typeName === arg.$typeName;
}
// node_modules/@bufbuild/protobuf/dist/esm/descriptors.js
var ScalarType;
(function(ScalarType2) {
  ScalarType2[ScalarType2["DOUBLE"] = 1] = "DOUBLE";
  ScalarType2[ScalarType2["FLOAT"] = 2] = "FLOAT";
  ScalarType2[ScalarType2["INT64"] = 3] = "INT64";
  ScalarType2[ScalarType2["UINT64"] = 4] = "UINT64";
  ScalarType2[ScalarType2["INT32"] = 5] = "INT32";
  ScalarType2[ScalarType2["FIXED64"] = 6] = "FIXED64";
  ScalarType2[ScalarType2["FIXED32"] = 7] = "FIXED32";
  ScalarType2[ScalarType2["BOOL"] = 8] = "BOOL";
  ScalarType2[ScalarType2["STRING"] = 9] = "STRING";
  ScalarType2[ScalarType2["BYTES"] = 12] = "BYTES";
  ScalarType2[ScalarType2["UINT32"] = 13] = "UINT32";
  ScalarType2[ScalarType2["SFIXED32"] = 15] = "SFIXED32";
  ScalarType2[ScalarType2["SFIXED64"] = 16] = "SFIXED64";
  ScalarType2[ScalarType2["SINT32"] = 17] = "SINT32";
  ScalarType2[ScalarType2["SINT64"] = 18] = "SINT64";
})(ScalarType || (ScalarType = {}));

// node_modules/@bufbuild/protobuf/dist/esm/wire/varint.js
function varint64read() {
  const buf = this.buf;
  let pos = this.pos;
  let lo = 0;
  let hi = 0;
  for (let shift = 0;shift < 28; shift += 7) {
    const b = buf[pos++];
    lo |= (b & 127) << shift;
    if ((b & 128) == 0) {
      this.pos = pos;
      this.assertBounds();
      this.varint64Lo = lo;
      this.varint64Hi = hi;
      return;
    }
  }
  const middleByte = buf[pos++];
  lo |= (middleByte & 15) << 28;
  hi = (middleByte & 112) >> 4;
  if ((middleByte & 128) == 0) {
    this.pos = pos;
    this.assertBounds();
    this.varint64Lo = lo;
    this.varint64Hi = hi;
    return;
  }
  for (let shift = 3;shift <= 31; shift += 7) {
    const b = buf[pos++];
    hi |= (b & 127) << shift;
    if ((b & 128) == 0) {
      this.pos = pos;
      this.assertBounds();
      this.varint64Lo = lo;
      this.varint64Hi = hi;
      return;
    }
  }
  throw new Error("invalid varint");
}
var TWO_PWR_32_DBL = 4294967296;
function int64FromString(dec2) {
  const minus = dec2[0] === "-";
  if (minus) {
    dec2 = dec2.slice(1);
  }
  const base = 1e6;
  let lowBits = 0;
  let highBits = 0;
  function add1e6digit(begin, end) {
    const digit1e6 = Number(dec2.slice(begin, end));
    highBits *= base;
    lowBits = lowBits * base + digit1e6;
    if (lowBits >= TWO_PWR_32_DBL) {
      highBits = highBits + (lowBits / TWO_PWR_32_DBL | 0);
      lowBits = lowBits % TWO_PWR_32_DBL;
    }
  }
  add1e6digit(-24, -18);
  add1e6digit(-18, -12);
  add1e6digit(-12, -6);
  add1e6digit(-6);
  return minus ? negate(lowBits, highBits) : newBits(lowBits, highBits);
}
function int64ToString(lo, hi) {
  let bits = newBits(lo, hi);
  const negative = bits.hi & 2147483648;
  if (negative) {
    bits = negate(bits.lo, bits.hi);
  }
  const result = uInt64ToString(bits.lo, bits.hi);
  return negative ? "-" + result : result;
}
function uInt64ToString(lo, hi) {
  ({ lo, hi } = toUnsigned(lo, hi));
  if (hi <= 2097151) {
    return String(TWO_PWR_32_DBL * hi + lo);
  }
  const low = lo & 16777215;
  const mid = (lo >>> 24 | hi << 8) & 16777215;
  const high = hi >> 16 & 65535;
  let digitA = low + mid * 6777216 + high * 6710656;
  let digitB = mid + high * 8147497;
  let digitC = high * 2;
  const base = 1e7;
  if (digitA >= base) {
    digitB += Math.floor(digitA / base);
    digitA %= base;
  }
  if (digitB >= base) {
    digitC += Math.floor(digitB / base);
    digitB %= base;
  }
  return digitC.toString() + decimalFrom1e7WithLeadingZeros(digitB) + decimalFrom1e7WithLeadingZeros(digitA);
}
function toUnsigned(lo, hi) {
  return { lo: lo >>> 0, hi: hi >>> 0 };
}
function newBits(lo, hi) {
  return { lo: lo | 0, hi: hi | 0 };
}
function negate(lowBits, highBits) {
  highBits = ~highBits;
  if (lowBits) {
    lowBits = ~lowBits + 1;
  } else {
    highBits += 1;
  }
  return newBits(lowBits, highBits);
}
var decimalFrom1e7WithLeadingZeros = (digit1e7) => {
  const partial = String(digit1e7);
  return "0000000".slice(partial.length) + partial;
};
function varint32write(value, bytes) {
  if (value >>> 0 < 128) {
    bytes.push(value);
    return;
  }
  if (value >= 0) {
    while (value > 127) {
      bytes.push(value & 127 | 128);
      value = value >>> 7;
    }
    bytes.push(value);
  } else {
    for (let i = 0;i < 9; i++) {
      bytes.push(value & 127 | 128);
      value = value >> 7;
    }
    bytes.push(1);
  }
}
function varint32read() {
  let b = this.buf[this.pos++];
  if ((b & 128) === 0) {
    this.assertBounds();
    return b;
  }
  let result = b & 127;
  b = this.buf[this.pos++];
  result |= (b & 127) << 7;
  if ((b & 128) === 0) {
    this.assertBounds();
    return result;
  }
  b = this.buf[this.pos++];
  result |= (b & 127) << 14;
  if ((b & 128) === 0) {
    this.assertBounds();
    return result;
  }
  b = this.buf[this.pos++];
  result |= (b & 127) << 21;
  if ((b & 128) === 0) {
    this.assertBounds();
    return result;
  }
  b = this.buf[this.pos++];
  result |= (b & 15) << 28;
  for (let readBytes = 5;(b & 128) !== 0 && readBytes < 10; readBytes++)
    b = this.buf[this.pos++];
  if ((b & 128) !== 0)
    throw new Error("invalid varint");
  this.assertBounds();
  return result >>> 0;
}

// node_modules/@bufbuild/protobuf/dist/esm/proto-int64.js
var protoInt64 = /* @__PURE__ */ makeInt64Support();
function makeInt64Support() {
  const dv = new DataView(new ArrayBuffer(8));
  const ok = typeof BigInt === "function" && typeof dv.getBigInt64 === "function" && typeof dv.getBigUint64 === "function" && typeof dv.setBigInt64 === "function" && typeof dv.setBigUint64 === "function" && (!!globalThis.Deno || !!globalThis.Bun || typeof process != "object" || typeof process.env != "object" || process.env.BUF_BIGINT_DISABLE !== "1");
  if (ok) {
    const MIN = BigInt("-9223372036854775808");
    const MAX = BigInt("9223372036854775807");
    const UMIN = BigInt("0");
    const UMAX = BigInt("18446744073709551615");
    return {
      zero: BigInt(0),
      supported: true,
      parse(value) {
        const bi = typeof value == "bigint" ? value : BigInt(value);
        if (bi > MAX || bi < MIN) {
          throw new Error(`invalid int64: ${value}`);
        }
        return bi;
      },
      uParse(value) {
        const bi = typeof value == "bigint" ? value : BigInt(value);
        if (bi > UMAX || bi < UMIN) {
          throw new Error(`invalid uint64: ${value}`);
        }
        return bi;
      },
      enc(value) {
        dv.setBigInt64(0, this.parse(value), true);
        return {
          lo: dv.getInt32(0, true),
          hi: dv.getInt32(4, true)
        };
      },
      uEnc(value) {
        dv.setBigInt64(0, this.uParse(value), true);
        return {
          lo: dv.getInt32(0, true),
          hi: dv.getInt32(4, true)
        };
      },
      dec(lo, hi) {
        dv.setInt32(0, lo, true);
        dv.setInt32(4, hi, true);
        return dv.getBigInt64(0, true);
      },
      uDec(lo, hi) {
        dv.setInt32(0, lo, true);
        dv.setInt32(4, hi, true);
        return dv.getBigUint64(0, true);
      }
    };
  }
  return {
    zero: "0",
    supported: false,
    parse(value) {
      if (typeof value != "string") {
        value = value.toString();
      }
      assertInt64String(value);
      return value;
    },
    uParse(value) {
      if (typeof value != "string") {
        value = value.toString();
      }
      assertUInt64String(value);
      return value;
    },
    enc(value) {
      if (typeof value != "string") {
        value = value.toString();
      }
      assertInt64String(value);
      return int64FromString(value);
    },
    uEnc(value) {
      if (typeof value != "string") {
        value = value.toString();
      }
      assertUInt64String(value);
      return int64FromString(value);
    },
    dec(lo, hi) {
      return int64ToString(lo, hi);
    },
    uDec(lo, hi) {
      return uInt64ToString(lo, hi);
    }
  };
}
function assertInt64String(value) {
  if (!/^-?[0-9]+$/.test(value)) {
    throw new Error("invalid int64: " + value);
  }
}
function assertUInt64String(value) {
  if (!/^[0-9]+$/.test(value)) {
    throw new Error("invalid uint64: " + value);
  }
}

// node_modules/@bufbuild/protobuf/dist/esm/reflect/scalar.js
function scalarZeroValue(type, longAsString) {
  switch (type) {
    case ScalarType.STRING:
      return "";
    case ScalarType.BOOL:
      return false;
    case ScalarType.DOUBLE:
    case ScalarType.FLOAT:
      return 0;
    case ScalarType.INT64:
    case ScalarType.UINT64:
    case ScalarType.SFIXED64:
    case ScalarType.FIXED64:
    case ScalarType.SINT64:
      return longAsString ? "0" : protoInt64.zero;
    case ScalarType.BYTES:
      return new Uint8Array(0);
    default:
      return 0;
  }
}

// node_modules/@bufbuild/protobuf/dist/esm/reflect/unsafe.js
var unsafeLocal = Symbol.for("reflect unsafe local");
function unsafeIsSetExplicit(target, localName) {
  return Object.prototype.hasOwnProperty.call(target, localName) && target[localName] !== undefined;
}

// node_modules/@bufbuild/protobuf/dist/esm/reflect/guard.js
function isObject(arg) {
  return arg !== null && typeof arg == "object" && !Array.isArray(arg);
}

// node_modules/@bufbuild/protobuf/dist/esm/wkt/wrappers.js
function isWrapperDesc(messageDesc) {
  const f = messageDesc.fields[0];
  return isWrapperTypeName(messageDesc.typeName) && f !== undefined && f.fieldKind == "scalar" && f.name == "value" && f.number == 1;
}
var wrapperTypeNames = /* @__PURE__ */ new Set([
  "google.protobuf.DoubleValue",
  "google.protobuf.FloatValue",
  "google.protobuf.Int64Value",
  "google.protobuf.UInt64Value",
  "google.protobuf.Int32Value",
  "google.protobuf.UInt32Value",
  "google.protobuf.BoolValue",
  "google.protobuf.StringValue",
  "google.protobuf.BytesValue"
]);
function isWrapperTypeName(name) {
  return wrapperTypeNames.has(name);
}

// node_modules/@bufbuild/protobuf/dist/esm/create.js
var EDITION_PROTO3 = 999;
var EDITION_PROTO2 = 998;
var IMPLICIT = 2;
function create(schema, init) {
  if (isMessage(init, schema)) {
    return init;
  }
  return compiledCreate(schema)(init);
}
var compiledCreates = new WeakMap;
function compiledCreate(desc) {
  let compiled = compiledCreates.get(desc);
  if (compiled === undefined) {
    compiled = compileCreate(desc);
    compiledCreates.set(desc, compiled);
  }
  return compiled;
}
var INIT_SINGULAR = 0;
var INIT_LIST = 1;
var INIT_MAP = 2;
var INIT_ONEOF = 3;
function compileCreate(desc) {
  const typeName = desc.typeName;
  const { properties, prototype } = compileInitMessage(desc);
  return (init) => {
    let message;
    if (prototype !== undefined) {
      message = Object.create(prototype);
      message.$typeName = typeName;
    } else {
      message = { $typeName: typeName };
    }
    for (let i = 0;i < properties.length; i++) {
      const property = properties[i];
      const name = property.name;
      const initValue = init === null || init === undefined ? undefined : init[name];
      switch (property.kind) {
        case INIT_SINGULAR:
          if (initValue != null) {
            message[name] = property.convert !== undefined ? property.convert(initValue) : initValue;
          } else if (property.constant !== undefined) {
            message[name] = property.constant;
          }
          break;
        case INIT_LIST:
          message[name] = property.convert !== undefined && Array.isArray(initValue) ? initValue.map(property.convert) : initValue !== null && initValue !== undefined ? initValue : [];
          break;
        case INIT_MAP:
          if (property.convert === undefined || !isObject(initValue)) {
            message[name] = initValue !== null && initValue !== undefined ? initValue : {};
          } else {
            const converted = {};
            const keys = Object.keys(initValue);
            for (let k = 0;k < keys.length; k++) {
              converted[keys[k]] = property.convert(initValue[keys[k]]);
            }
            message[name] = converted;
          }
          break;
        case INIT_ONEOF: {
          const oneofValue = initValue;
          if ((oneofValue === null || oneofValue === undefined ? undefined : oneofValue.case) != null) {
            const convert = property.convert.get(oneofValue.case);
            if (convert !== undefined) {
              message[name] = {
                case: oneofValue.case,
                value: convert(oneofValue.value)
              };
              break;
            }
          }
          message[name] = { case: undefined };
          break;
        }
      }
    }
    return message;
  };
}
function compileInitMessage(desc) {
  var _a, _b;
  const properties = [];
  const prototype = {};
  const usePrototype = needsPrototypeChain(desc);
  for (const member of desc.members) {
    const name = member.localName;
    if (member.kind == "oneof") {
      properties.push({
        name,
        kind: INIT_ONEOF,
        constant: undefined,
        convert: compileConvertOneof(member)
      });
      continue;
    }
    switch (member.fieldKind) {
      case "message": {
        properties.push({
          name,
          kind: INIT_SINGULAR,
          constant: undefined,
          convert: compileConvertMessage(member)
        });
        break;
      }
      case "list": {
        properties.push({
          name,
          kind: INIT_LIST,
          constant: undefined,
          convert: member.listKind == "message" ? (_a = compileConvertMessage(member)) !== null && _a !== undefined ? _a : (value) => value : member.scalar == ScalarType.BYTES ? toU8Arr : undefined
        });
        break;
      }
      case "map": {
        properties.push({
          name,
          kind: INIT_MAP,
          constant: undefined,
          convert: member.mapKind == "message" ? (_b = compileConvertMessage(member)) !== null && _b !== undefined ? _b : (value) => value : member.scalar == ScalarType.BYTES ? toU8Arr : undefined
        });
        break;
      }
      default: {
        const zeroValue = createZeroValue(member);
        properties.push({
          name,
          kind: INIT_SINGULAR,
          constant: member.presence == IMPLICIT ? zeroValue : undefined,
          convert: member.fieldKind == "scalar" && member.scalar == ScalarType.BYTES ? toU8Arr : undefined
        });
        if (usePrototype) {
          prototype[name] = zeroValue;
        }
        break;
      }
    }
  }
  return {
    properties,
    prototype: usePrototype ? prototype : undefined
  };
}
function compileConvertOneof(oneof) {
  const converters = new Map;
  for (const field of oneof.fields) {
    let convert;
    if (field.fieldKind == "message") {
      convert = compileConvertMessage(field);
    } else if (field.fieldKind == "scalar" && field.scalar == ScalarType.BYTES) {
      convert = toU8Arr;
    }
    converters.set(field.localName, convert !== null && convert !== undefined ? convert : (value) => value);
  }
  return converters;
}
function compileConvertMessage(field) {
  if (field.fieldKind == "message" && !field.oneof && isWrapperDesc(field.message)) {
    return field.message.fields[0].scalar == ScalarType.BYTES ? toU8Arr : undefined;
  }
  if (field.message.typeName == "google.protobuf.Struct" && field.parent.typeName !== "google.protobuf.Value") {
    return;
  }
  const messageDesc = field.message;
  let compiled;
  return (value) => {
    if (!isObject(value) || isMessage(value, messageDesc)) {
      return value;
    }
    compiled !== null && compiled !== undefined || (compiled = compiledCreate(messageDesc));
    return compiled(value);
  };
}
function toU8Arr(value) {
  return Array.isArray(value) ? new Uint8Array(value) : value;
}
function needsPrototypeChain(desc) {
  switch (desc.file.edition) {
    case EDITION_PROTO3:
      return false;
    case EDITION_PROTO2:
      return true;
    default:
      return desc.fields.some((f) => f.presence != IMPLICIT && f.fieldKind != "message" && !f.oneof);
  }
}
function createZeroValue(field) {
  const defaultValue = field.getDefaultValue();
  if (defaultValue !== undefined) {
    return field.fieldKind == "scalar" && field.longAsString ? defaultValue.toString() : defaultValue;
  }
  return field.fieldKind == "scalar" ? scalarZeroValue(field.scalar, field.longAsString) : field.enum.values[0].number;
}
// node_modules/@bufbuild/protobuf/dist/esm/reflect/error.js
class FieldError extends Error {
  constructor(fieldOrOneof, message, name = "FieldValueInvalidError") {
    super(message);
    this.name = name;
    this.field = () => fieldOrOneof;
  }
}

// node_modules/@bufbuild/protobuf/dist/esm/wire/text-encoding.js
var te;
function configureTextEncoding(textEncoding) {
  var _a;
  te = Object.assign(Object.assign({}, textEncoding), { encodeUtf8Into: (_a = textEncoding.encodeUtf8Into) !== null && _a !== undefined ? _a : emulateEncodeInto(textEncoding.encodeUtf8.bind(textEncoding)) });
}
function getTextEncoding() {
  if (!te) {
    const globals = globalThis;
    if (!globals.TextEncoder || !globals.TextDecoder) {
      throw new Error("encoding API missing: install TextEncoder and TextDecoder on globalThis");
    }
    const textEncoder = new globals.TextEncoder;
    const textDecoder = new globals.TextDecoder;
    let textDecoderStrict;
    const config = {
      encodeUtf8(text) {
        return textEncoder.encode(text);
      },
      decodeUtf8(bytes, strict) {
        if (strict) {
          if (!textDecoderStrict) {
            textDecoderStrict = new globals.TextDecoder("utf-8", {
              fatal: true
            });
          }
          return textDecoderStrict.decode(bytes);
        }
        return textDecoder.decode(bytes);
      },
      checkUtf8(text) {
        try {
          encodeURIComponent(text);
          return true;
        } catch (_) {
          return false;
        }
      }
    };
    if (textEncoder.encodeInto) {
      config.encodeUtf8Into = textEncoder.encodeInto.bind(textEncoder);
    }
    const nativeStringIsWellFormed = String.prototype.isWellFormed;
    if (nativeStringIsWellFormed) {
      config.checkUtf8 = (text) => {
        return nativeStringIsWellFormed.call(text);
      };
    }
    configureTextEncoding(config);
  }
  return te;
}
function emulateEncodeInto(encodeUtf8) {
  return (text, dest) => {
    const bytes = encodeUtf8(text);
    dest.set(bytes);
    return { written: bytes.byteLength };
  };
}

// node_modules/@bufbuild/protobuf/dist/esm/wire/binary-encoding.js
var WireType;
(function(WireType2) {
  WireType2[WireType2["Varint"] = 0] = "Varint";
  WireType2[WireType2["Bit64"] = 1] = "Bit64";
  WireType2[WireType2["LengthDelimited"] = 2] = "LengthDelimited";
  WireType2[WireType2["StartGroup"] = 3] = "StartGroup";
  WireType2[WireType2["EndGroup"] = 4] = "EndGroup";
  WireType2[WireType2["Bit32"] = 5] = "Bit32";
})(WireType || (WireType = {}));
var FLOAT32_MAX = 340282346638528860000000000000000000000;
var FLOAT32_MIN = -340282346638528860000000000000000000000;
var UINT32_MAX = 4294967295;
var INT32_MAX = 2147483647;
var INT32_MIN = -2147483648;

class BinaryWriter {
  constructor(encodeUtf8) {
    this.stackPos = [];
    this.encodeUtf8Into = encodeUtf8 ? emulateEncodeInto(encodeUtf8) : getTextEncoding().encodeUtf8Into;
    this.buffer = EMPTY_BUFFER;
    this.viewCache = EMPTY_VIEW;
    this.pos = 0;
  }
  ensureCapacity(size) {
    const required = this.pos + size;
    if (required > this.buffer.length) {
      let newLen = this.buffer.length || INITIAL_SIZE;
      while (newLen < required)
        newLen *= 2;
      const newBuf = new Uint8Array(newLen);
      if (this.pos > 0)
        newBuf.set(this.buffer);
      this.buffer = newBuf;
    }
  }
  view() {
    const bytes = this.buffer;
    const view = this.viewCache;
    if (view.byteLength === bytes.byteLength)
      return view;
    const newView = new DataView(bytes.buffer);
    this.viewCache = newView;
    return newView;
  }
  finish() {
    const result = this.buffer.slice(0, this.pos);
    this.pos = 0;
    this.stackPos = [];
    return result;
  }
  fork() {
    this.stackPos.push(this.pos);
    this.ensureCapacity(DEFAULT_LEN_PREFIX_SIZE);
    this.buffer[this.pos++] = 0;
    return this;
  }
  join() {
    const forkPos = this.stackPos.pop();
    if (forkPos === undefined)
      throw new Error("invalid state, fork stack empty");
    const len = this.pos - forkPos - DEFAULT_LEN_PREFIX_SIZE;
    const lenPrefixSize = varint32Size(len);
    if (lenPrefixSize > DEFAULT_LEN_PREFIX_SIZE) {
      this.ensureCapacity(lenPrefixSize - DEFAULT_LEN_PREFIX_SIZE);
      this.buffer.copyWithin(forkPos + lenPrefixSize, forkPos + DEFAULT_LEN_PREFIX_SIZE, this.pos);
    }
    this.pos = forkPos;
    this.uint32(len);
    this.pos += len;
    return this;
  }
  tag(fieldNo, type) {
    return this.uint32((fieldNo << 3 | type) >>> 0);
  }
  raw(chunk) {
    this.ensureCapacity(chunk.length);
    this.buffer.set(chunk, this.pos);
    this.pos += chunk.length;
    return this;
  }
  uint32(value) {
    assertUInt32(value);
    this.ensureCapacity(5);
    if (value < 128) {
      this.buffer[this.pos++] = value;
      return this;
    }
    while (value > 127) {
      this.buffer[this.pos++] = value & 127 | 128;
      value >>>= 7;
    }
    this.buffer[this.pos++] = value;
    return this;
  }
  int32(value) {
    assertInt32(value);
    if (value >= 0) {
      return this.uint32(value);
    }
    this.ensureCapacity(10);
    for (let i = 0;i < 9; i++) {
      this.buffer[this.pos++] = value & 127 | 128;
      value >>= 7;
    }
    this.buffer[this.pos++] = 1;
    return this;
  }
  bool(value) {
    this.ensureCapacity(1);
    this.buffer[this.pos++] = value ? 1 : 0;
    return this;
  }
  bytes(value) {
    this.uint32(value.byteLength);
    return this.raw(value);
  }
  string(value) {
    if (typeof value !== "string") {
      value = String(value);
    }
    const len = value.length;
    if (len <= ASCII_MAX_LENGTH) {
      this.ensureCapacity(len + 1);
      const ascii = this.buffer;
      let pos = this.pos;
      ascii[pos++] = len;
      let i = 0;
      for (;i < len; i++) {
        const code = value.charCodeAt(i);
        if (code > 127)
          break;
        ascii[pos++] = code;
      }
      if (i == len) {
        this.pos = pos;
        return this;
      }
    }
    this.ensureCapacity(len * 3 + 5);
    const lenPrefixSizeGuess = varint32Size(len);
    const buf = this.buffer;
    const start = this.pos;
    const { written } = this.encodeUtf8Into(value, buf.subarray(start + lenPrefixSizeGuess));
    const lenPrefixSize = varint32Size(written);
    if (lenPrefixSize != lenPrefixSizeGuess) {
      buf.copyWithin(start + lenPrefixSize, start + lenPrefixSizeGuess, start + lenPrefixSizeGuess + written);
    }
    this.uint32(written);
    this.pos += written;
    return this;
  }
  float(value) {
    assertFloat32(value);
    this.ensureCapacity(4);
    this.view().setFloat32(this.pos, value, true);
    this.pos += 4;
    return this;
  }
  double(value) {
    this.ensureCapacity(8);
    this.view().setFloat64(this.pos, value, true);
    this.pos += 8;
    return this;
  }
  fixed32(value) {
    assertUInt32(value);
    this.ensureCapacity(4);
    this.view().setUint32(this.pos, value, true);
    this.pos += 4;
    return this;
  }
  sfixed32(value) {
    assertInt32(value);
    this.ensureCapacity(4);
    this.view().setInt32(this.pos, value, true);
    this.pos += 4;
    return this;
  }
  sint32(value) {
    assertInt32(value);
    return this.uint32((value << 1 ^ value >> 31) >>> 0);
  }
  sfixed64(value) {
    const tc = protoInt64.enc(value);
    this.ensureCapacity(8);
    const view = this.view();
    view.setInt32(this.pos, tc.lo, true);
    view.setInt32(this.pos + 4, tc.hi, true);
    this.pos += 8;
    return this;
  }
  fixed64(value) {
    const tc = protoInt64.uEnc(value);
    this.ensureCapacity(8);
    const view = this.view();
    view.setInt32(this.pos, tc.lo, true);
    view.setInt32(this.pos + 4, tc.hi, true);
    this.pos += 8;
    return this;
  }
  int64(value) {
    const tc = protoInt64.enc(value);
    return this.writeVarint64(tc.lo, tc.hi);
  }
  sint64(value) {
    const tc = protoInt64.enc(value), sign = tc.hi >> 31, lo = tc.lo << 1 ^ sign, hi = (tc.hi << 1 | tc.lo >>> 31) ^ sign;
    return this.writeVarint64(lo, hi);
  }
  uint64(value) {
    const tc = protoInt64.uEnc(value);
    return this.writeVarint64(tc.lo, tc.hi);
  }
  writeVarint64(lo, hi) {
    this.ensureCapacity(10);
    const buf = this.buffer;
    let pos = this.pos;
    for (let i = 0;i < 28; i = i + 7) {
      const shift = lo >>> i;
      const hasNext = !(shift >>> 7 == 0 && hi == 0);
      buf[pos++] = (hasNext ? shift | 128 : shift) & 255;
      if (!hasNext) {
        this.pos = pos;
        return this;
      }
    }
    const splitBits = lo >>> 28 & 15 | (hi & 7) << 4;
    const hasMoreBits = !(hi >> 3 == 0);
    buf[pos++] = (hasMoreBits ? splitBits | 128 : splitBits) & 255;
    if (!hasMoreBits) {
      this.pos = pos;
      return this;
    }
    for (let i = 3;i < 31; i = i + 7) {
      const shift = hi >>> i;
      const hasNext = !(shift >>> 7 == 0);
      buf[pos++] = (hasNext ? shift | 128 : shift) & 255;
      if (!hasNext) {
        this.pos = pos;
        return this;
      }
    }
    buf[pos++] = hi >>> 31 & 1;
    this.pos = pos;
    return this;
  }
}
var INITIAL_SIZE = 128;
var DEFAULT_LEN_PREFIX_SIZE = 1;
var EMPTY_BUFFER = new Uint8Array(0);
var EMPTY_VIEW = new DataView(EMPTY_BUFFER.buffer);
var ASCII_MAX_LENGTH = 32;
function varint32Size(value) {
  if (value < 128)
    return 1;
  if (value < 16384)
    return 2;
  if (value < 2097152)
    return 3;
  if (value < 268435456)
    return 4;
  return 5;
}

class BinaryReader {
  constructor(buf, decodeUtf8 = getTextEncoding().decodeUtf8) {
    this.decodeUtf8 = decodeUtf8;
    this.varint64Lo = 0;
    this.varint64Hi = 0;
    this.varint64 = varint64read;
    this.uint32 = varint32read;
    this.buf = buf;
    this.len = buf.length;
    this.pos = 0;
    this.view = new DataView(buf.buffer, buf.byteOffset, buf.byteLength);
  }
  tag() {
    const start = this.pos;
    const tag = this.uint32();
    const bytesRead = this.pos - start;
    if (bytesRead > 5 || bytesRead == 5 && this.buf[this.pos - 1] > 15) {
      throw new Error("illegal tag: varint overflows uint32");
    }
    const fieldNo = tag >>> 3;
    const wireType = tag & 7;
    if (fieldNo <= 0 || wireType > 5) {
      throw new Error("illegal tag: field no " + fieldNo + " wire type " + wireType);
    }
    return [fieldNo, wireType];
  }
  skip(wireType, fieldNo, recursionLimit = 100) {
    let start = this.pos;
    switch (wireType) {
      case WireType.Varint:
        while (this.buf[this.pos++] & 128) {}
        break;
      case WireType.Bit64:
        this.pos += 4;
      case WireType.Bit32:
        this.pos += 4;
        break;
      case WireType.LengthDelimited:
        let len = this.uint32();
        this.pos += len;
        break;
      case WireType.StartGroup:
        if (recursionLimit <= 0) {
          throw new Error("maximum recursion depth reached");
        }
        for (;; ) {
          const [fn, wt] = this.tag();
          if (wt === WireType.EndGroup) {
            if (fieldNo !== undefined && fn !== fieldNo) {
              throw new Error("invalid end group tag");
            }
            break;
          }
          this.skip(wt, fn, recursionLimit - 1);
        }
        break;
      default:
        throw new Error("cant skip wire type " + wireType);
    }
    this.assertBounds();
    return this.buf.subarray(start, this.pos);
  }
  assertBounds() {
    if (this.pos > this.len)
      throw new RangeError("premature EOF");
  }
  int32() {
    return this.uint32() | 0;
  }
  sint32() {
    let zze = this.uint32();
    return zze >>> 1 ^ -(zze & 1);
  }
  int64() {
    this.varint64();
    return protoInt64.dec(this.varint64Lo, this.varint64Hi);
  }
  uint64() {
    this.varint64();
    return protoInt64.uDec(this.varint64Lo, this.varint64Hi);
  }
  sint64() {
    this.varint64();
    let lo = this.varint64Lo;
    let hi = this.varint64Hi;
    let s = -(lo & 1);
    lo = (lo >>> 1 | (hi & 1) << 31) ^ s;
    hi = hi >>> 1 ^ s;
    return protoInt64.dec(lo, hi);
  }
  bool() {
    const b = this.buf[this.pos];
    if (b < 128) {
      this.pos++;
      return b !== 0;
    }
    this.varint64();
    return this.varint64Lo !== 0 || this.varint64Hi !== 0;
  }
  fixed32() {
    return this.view.getUint32((this.pos += 4) - 4, true);
  }
  sfixed32() {
    return this.view.getInt32((this.pos += 4) - 4, true);
  }
  fixed64() {
    return protoInt64.uDec(this.sfixed32(), this.sfixed32());
  }
  sfixed64() {
    return protoInt64.dec(this.sfixed32(), this.sfixed32());
  }
  float() {
    return this.view.getFloat32((this.pos += 4) - 4, true);
  }
  double() {
    return this.view.getFloat64((this.pos += 8) - 8, true);
  }
  bytes() {
    let len = this.uint32(), start = this.pos;
    this.pos += len;
    this.assertBounds();
    return this.buf.subarray(start, start + len);
  }
  string(strict) {
    const bytes = this.bytes();
    const len = bytes.length;
    if (len <= ASCII_MAX_LENGTH) {
      const codes = new Array(len);
      for (let i = 0;i < len; i++) {
        const byte = bytes[i];
        if (byte > 127) {
          return this.decodeUtf8(bytes, strict);
        }
        codes[i] = byte;
      }
      return String.fromCharCode.apply(String, codes);
    }
    return this.decodeUtf8(bytes, strict);
  }
}
function assertInt32(arg) {
  if (typeof arg == "string") {
    arg = Number(arg);
  } else if (typeof arg != "number") {
    throw new Error("invalid int32: " + typeof arg);
  }
  if (!Number.isInteger(arg) || arg > INT32_MAX || arg < INT32_MIN)
    throw new Error("invalid int32: " + arg);
}
function assertUInt32(arg) {
  if (typeof arg == "string") {
    arg = Number(arg);
  } else if (typeof arg != "number") {
    throw new Error("invalid uint32: " + typeof arg);
  }
  if (!Number.isInteger(arg) || arg > UINT32_MAX || arg < 0)
    throw new Error("invalid uint32: " + arg);
}
function assertFloat32(arg) {
  if (typeof arg == "string") {
    const o = arg;
    arg = Number(arg);
    if (Number.isNaN(arg) && o !== "NaN") {
      throw new Error("invalid float32: " + o);
    }
  } else if (typeof arg != "number") {
    throw new Error("invalid float32: " + typeof arg);
  }
  if (Number.isFinite(arg) && (arg > FLOAT32_MAX || arg < FLOAT32_MIN))
    throw new Error("invalid float32: " + arg);
}

// node_modules/@bufbuild/protobuf/dist/esm/reflect/message.js
var NULL_VALUE = 0;
function localMessageMapper(field) {
  if (usesJsonRepresentation(field)) {
    return {
      toMessage: (local) => wktStructToReflect(local),
      toLocal: (message) => wktStructToLocal(message)
    };
  }
  if (field.fieldKind == "message" && !field.oneof && isWrapperDesc(field.message)) {
    const wrapperDesc = field.message;
    const valueLocalName = wrapperDesc.fields[0].localName;
    return {
      toMessage: (local) => {
        const message = create(wrapperDesc);
        if (local !== undefined) {
          message[valueLocalName] = local;
        }
        return message;
      },
      toLocal: (message) => message[valueLocalName]
    };
  }
  const childDesc = field.message;
  return {
    toMessage: (local) => local === undefined ? create(childDesc) : local,
    toLocal: (message) => message
  };
}
function usesJsonRepresentation(field) {
  return field.message.typeName == "google.protobuf.Struct" && field.parent.typeName != "google.protobuf.Value";
}
function wktStructToReflect(json) {
  const struct = {
    $typeName: "google.protobuf.Struct",
    fields: {}
  };
  if (isObject(json)) {
    for (const k of Object.keys(json)) {
      struct.fields[k] = wktValueToReflect(json[k]);
    }
  }
  return struct;
}
function wktStructToLocal(val) {
  const json = {};
  for (const k of Object.keys(val.fields)) {
    json[k] = wktValueToLocal(val.fields[k]);
  }
  return json;
}
function wktValueToLocal(val) {
  switch (val.kind.case) {
    case "structValue":
      return wktStructToLocal(val.kind.value);
    case "listValue":
      return val.kind.value.values.map(wktValueToLocal);
    case "nullValue":
    case undefined:
      return null;
    default:
      return val.kind.value;
  }
}
function wktValueToReflect(json) {
  const value = {
    $typeName: "google.protobuf.Value",
    kind: { case: undefined }
  };
  switch (typeof json) {
    case "number":
      value.kind = { case: "numberValue", value: json };
      break;
    case "string":
      value.kind = { case: "stringValue", value: json };
      break;
    case "boolean":
      value.kind = { case: "boolValue", value: json };
      break;
    case "object":
      if (json === null) {
        value.kind = { case: "nullValue", value: NULL_VALUE };
      } else if (Array.isArray(json)) {
        const listValue = {
          $typeName: "google.protobuf.ListValue",
          values: []
        };
        if (Array.isArray(json)) {
          for (const e of json) {
            listValue.values.push(wktValueToReflect(e));
          }
        }
        value.kind = {
          case: "listValue",
          value: listValue
        };
      } else {
        value.kind = {
          case: "structValue",
          value: wktStructToReflect(json)
        };
      }
      break;
  }
  return value;
}
// node_modules/@bufbuild/protobuf/dist/esm/wire/base64-encoding.js
var nativeSetFromBase64 = Uint8Array.prototype.setFromBase64;
function base64Decode(base64Str) {
  const len = base64Str.length;
  let size = len - (len + 3 >> 2);
  if ((len & 3) == 0 && base64Str[len - 1] == "=") {
    size -= base64Str[len - 2] == "=" ? 2 : 1;
  }
  const bytes = new Uint8Array(size);
  let written = -1;
  if (nativeSetFromBase64) {
    try {
      const result = nativeSetFromBase64.call(bytes, base64Str);
      if (result.read == len) {
        written = result.written;
      }
    } catch (_a) {}
  }
  if (written < 0) {
    written = setFromBase64(bytes, base64Str);
  }
  return written == size ? bytes : bytes.subarray(0, written);
}
function setFromBase64(bytes, base64Str) {
  const table = getDecodeTable();
  let bytePos = 0, groupPos = 0, b, p = 0;
  for (let i = 0;i < base64Str.length; i++) {
    b = table[base64Str.charCodeAt(i)];
    if (b === undefined) {
      switch (base64Str[i]) {
        case "=":
          groupPos = 0;
        case `
`:
        case "\r":
        case "\t":
        case " ":
          continue;
        default:
          throw Error("invalid base64 string");
      }
    }
    switch (groupPos) {
      case 0:
        p = b;
        groupPos = 1;
        break;
      case 1:
        bytes[bytePos++] = p << 2 | (b & 48) >> 4;
        p = b;
        groupPos = 2;
        break;
      case 2:
        bytes[bytePos++] = (p & 15) << 4 | (b & 60) >> 2;
        p = b;
        groupPos = 3;
        break;
      case 3:
        bytes[bytePos++] = (p & 3) << 6 | b;
        groupPos = 0;
        break;
    }
  }
  if (groupPos == 1)
    throw Error("invalid base64 string");
  return bytePos;
}
var nativeToBase64 = Uint8Array.prototype.toBase64;
var encodeTableStd;
var encodeTableUrl;
var decodeTable;
function getEncodeTable(encoding) {
  if (!encodeTableStd) {
    encodeTableStd = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/".split("");
    encodeTableUrl = encodeTableStd.slice(0, -2).concat("-", "_");
  }
  return encoding == "url" ? encodeTableUrl : encodeTableStd;
}
function getDecodeTable() {
  if (!decodeTable) {
    decodeTable = [];
    const encodeTable = getEncodeTable("std");
    for (let i = 0;i < encodeTable.length; i++)
      decodeTable[encodeTable[i].charCodeAt(0)] = i;
    decodeTable[45] = encodeTable.indexOf("+");
    decodeTable[95] = encodeTable.indexOf("/");
  }
  return decodeTable;
}

// node_modules/@bufbuild/protobuf/dist/esm/reflect/names.js
function protoCamelCase(snakeCase) {
  let capNext = false;
  const b = [];
  for (let i = 0;i < snakeCase.length; i++) {
    let c = snakeCase.charAt(i);
    switch (c) {
      case "_":
        capNext = true;
        break;
      case "0":
      case "1":
      case "2":
      case "3":
      case "4":
      case "5":
      case "6":
      case "7":
      case "8":
      case "9":
        b.push(c);
        capNext = false;
        break;
      default:
        if (capNext) {
          capNext = false;
          c = c.toUpperCase();
        }
        b.push(c);
        break;
    }
  }
  return b.join("");
}
var reservedObjectProperties = new Set([
  "constructor",
  "toString",
  "toJSON",
  "valueOf"
]);
function safeObjectProperty(name) {
  return reservedObjectProperties.has(name) ? name + "$" : name;
}

// node_modules/@bufbuild/protobuf/dist/esm/codegenv2/restore-json-names.js
function restoreJsonNames(message) {
  for (const f of message.field) {
    if (!unsafeIsSetExplicit(f, "jsonName")) {
      f.jsonName = protoCamelCase(f.name);
    }
  }
  message.nestedType.forEach(restoreJsonNames);
}

// node_modules/@bufbuild/protobuf/dist/esm/wire/text-format.js
function parseTextFormatEnumValue(descEnum, value) {
  const enumValue = descEnum.values.find((v) => v.name === value);
  if (!enumValue) {
    throw new Error(`cannot parse ${descEnum} default value: ${value}`);
  }
  return enumValue.number;
}
function parseTextFormatScalarValue(type, value) {
  switch (type) {
    case ScalarType.STRING:
      return value;
    case ScalarType.BYTES: {
      const u = unescapeBytesDefaultValue(value);
      if (u === false) {
        throw new Error(`cannot parse ${ScalarType[type]} default value: ${value}`);
      }
      return u;
    }
    case ScalarType.INT64:
    case ScalarType.SFIXED64:
    case ScalarType.SINT64:
      return protoInt64.parse(value);
    case ScalarType.UINT64:
    case ScalarType.FIXED64:
      return protoInt64.uParse(value);
    case ScalarType.DOUBLE:
    case ScalarType.FLOAT:
      switch (value) {
        case "inf":
          return Number.POSITIVE_INFINITY;
        case "-inf":
          return Number.NEGATIVE_INFINITY;
        case "nan":
          return Number.NaN;
        default:
          return parseFloat(value);
      }
    case ScalarType.BOOL:
      return value === "true";
    case ScalarType.INT32:
    case ScalarType.UINT32:
    case ScalarType.SINT32:
    case ScalarType.FIXED32:
    case ScalarType.SFIXED32:
      return parseInt(value, 10);
  }
}
function unescapeBytesDefaultValue(str) {
  const b = [];
  const input = {
    tail: str,
    c: "",
    next() {
      if (this.tail.length == 0) {
        return false;
      }
      this.c = this.tail[0];
      this.tail = this.tail.substring(1);
      return true;
    },
    take(n) {
      if (this.tail.length >= n) {
        const r = this.tail.substring(0, n);
        this.tail = this.tail.substring(n);
        return r;
      }
      return false;
    }
  };
  while (input.next()) {
    switch (input.c) {
      case "\\":
        if (input.next()) {
          switch (input.c) {
            case "\\":
              b.push(input.c.charCodeAt(0));
              break;
            case "b":
              b.push(8);
              break;
            case "f":
              b.push(12);
              break;
            case "n":
              b.push(10);
              break;
            case "r":
              b.push(13);
              break;
            case "t":
              b.push(9);
              break;
            case "v":
              b.push(11);
              break;
            case "0":
            case "1":
            case "2":
            case "3":
            case "4":
            case "5":
            case "6":
            case "7": {
              const s = input.c;
              const t = input.take(2);
              if (t === false) {
                return false;
              }
              const n = parseInt(s + t, 8);
              if (Number.isNaN(n)) {
                return false;
              }
              b.push(n);
              break;
            }
            case "x": {
              const s = input.c;
              const t = input.take(2);
              if (t === false) {
                return false;
              }
              const n = parseInt(s + t, 16);
              if (Number.isNaN(n)) {
                return false;
              }
              b.push(n);
              break;
            }
            case "u": {
              const s = input.c;
              const t = input.take(4);
              if (t === false) {
                return false;
              }
              const n = parseInt(s + t, 16);
              if (Number.isNaN(n)) {
                return false;
              }
              const chunk = new Uint8Array(4);
              const view = new DataView(chunk.buffer);
              view.setInt32(0, n, true);
              b.push(chunk[0], chunk[1], chunk[2], chunk[3]);
              break;
            }
            case "U": {
              const s = input.c;
              const t = input.take(8);
              if (t === false) {
                return false;
              }
              const tc = protoInt64.uEnc(s + t);
              const chunk = new Uint8Array(8);
              const view = new DataView(chunk.buffer);
              view.setInt32(0, tc.lo, true);
              view.setInt32(4, tc.hi, true);
              b.push(chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6], chunk[7]);
              break;
            }
          }
        }
        break;
      default:
        b.push(input.c.charCodeAt(0));
    }
  }
  return new Uint8Array(b);
}

// node_modules/@bufbuild/protobuf/dist/esm/reflect/nested-types.js
function* nestedTypes(desc) {
  switch (desc.kind) {
    case "file":
      for (const message of desc.messages) {
        yield message;
        yield* nestedTypes(message);
      }
      yield* desc.enums;
      yield* desc.services;
      yield* desc.extensions;
      break;
    case "message":
      for (const message of desc.nestedMessages) {
        yield message;
        yield* nestedTypes(message);
      }
      yield* desc.nestedEnums;
      yield* desc.nestedExtensions;
      break;
  }
}

// node_modules/@bufbuild/protobuf/dist/esm/registry.js
function createFileRegistry(...args) {
  const registry = createBaseRegistry();
  if (!args.length) {
    return registry;
  }
  if ("$typeName" in args[0] && args[0].$typeName == "google.protobuf.FileDescriptorSet") {
    for (const file of args[0].file) {
      addFile(file, registry);
    }
    return registry;
  }
  if ("$typeName" in args[0]) {
    let recurseDeps = function(file) {
      const deps = [];
      for (const protoFileName of file.dependency) {
        if (registry.getFile(protoFileName) != null) {
          continue;
        }
        if (seen.has(protoFileName)) {
          continue;
        }
        const dep = resolve(protoFileName);
        if (!dep) {
          throw new Error(`Unable to resolve ${protoFileName}, imported by ${file.name}`);
        }
        if ("kind" in dep) {
          registry.addFile(dep, false, true);
        } else {
          seen.add(dep.name);
          deps.push(dep);
        }
      }
      return deps.concat(...deps.map(recurseDeps));
    };
    const input = args[0];
    const resolve = args[1];
    const seen = new Set;
    for (const file of [input, ...recurseDeps(input)].reverse()) {
      addFile(file, registry);
    }
  } else {
    for (const fileReg of args) {
      for (const file of fileReg.files) {
        registry.addFile(file);
      }
    }
  }
  return registry;
}
function createBaseRegistry() {
  const types = new Map;
  const extendees = new Map;
  const files = new Map;
  return {
    kind: "registry",
    types,
    extendees,
    [Symbol.iterator]() {
      return types.values();
    },
    get files() {
      return files.values();
    },
    addFile(file, skipTypes, withDeps) {
      files.set(file.proto.name, file);
      if (!skipTypes) {
        for (const type of nestedTypes(file)) {
          this.add(type);
        }
      }
      if (withDeps) {
        for (const f of file.dependencies) {
          this.addFile(f, skipTypes, withDeps);
        }
      }
    },
    add(desc) {
      if (desc.kind == "extension") {
        let numberToExt = extendees.get(desc.extendee.typeName);
        if (!numberToExt) {
          extendees.set(desc.extendee.typeName, numberToExt = new Map);
        }
        numberToExt.set(desc.number, desc);
      }
      types.set(desc.typeName, desc);
    },
    get(typeName) {
      return types.get(typeName);
    },
    getFile(fileName) {
      return files.get(fileName);
    },
    getMessage(typeName) {
      const t = types.get(typeName);
      return (t === null || t === undefined ? undefined : t.kind) == "message" ? t : undefined;
    },
    getEnum(typeName) {
      const t = types.get(typeName);
      return (t === null || t === undefined ? undefined : t.kind) == "enum" ? t : undefined;
    },
    getExtension(typeName) {
      const t = types.get(typeName);
      return (t === null || t === undefined ? undefined : t.kind) == "extension" ? t : undefined;
    },
    getExtensionFor(extendee, no) {
      var _a;
      return (_a = extendees.get(extendee.typeName)) === null || _a === undefined ? undefined : _a.get(no);
    },
    getService(typeName) {
      const t = types.get(typeName);
      return (t === null || t === undefined ? undefined : t.kind) == "service" ? t : undefined;
    }
  };
}
var EDITION_PROTO22 = 998;
var EDITION_PROTO32 = 999;
var EDITION_UNSTABLE = 9999;
var TYPE_STRING = 9;
var TYPE_GROUP = 10;
var TYPE_MESSAGE = 11;
var TYPE_BYTES = 12;
var TYPE_ENUM = 14;
var LABEL_REPEATED = 3;
var LABEL_REQUIRED = 2;
var JS_STRING = 1;
var IDEMPOTENCY_UNKNOWN = 0;
var EXPLICIT = 1;
var IMPLICIT2 = 2;
var LEGACY_REQUIRED = 3;
var PACKED = 1;
var DELIMITED = 2;
var OPEN = 1;
var VERIFY = 2;
var maximumEdition = 1001;
var featureDefaults = {
  998: {
    fieldPresence: 1,
    enumType: 2,
    repeatedFieldEncoding: 2,
    utf8Validation: 3,
    messageEncoding: 1,
    jsonFormat: 2,
    enforceNamingStyle: 2,
    defaultSymbolVisibility: 1
  },
  999: {
    fieldPresence: 2,
    enumType: 1,
    repeatedFieldEncoding: 1,
    utf8Validation: 2,
    messageEncoding: 1,
    jsonFormat: 1,
    enforceNamingStyle: 2,
    defaultSymbolVisibility: 1
  },
  1000: {
    fieldPresence: 1,
    enumType: 1,
    repeatedFieldEncoding: 1,
    utf8Validation: 2,
    messageEncoding: 1,
    jsonFormat: 1,
    enforceNamingStyle: 2,
    defaultSymbolVisibility: 1
  },
  1001: {
    fieldPresence: 1,
    enumType: 1,
    repeatedFieldEncoding: 1,
    utf8Validation: 2,
    messageEncoding: 1,
    jsonFormat: 1,
    enforceNamingStyle: 1,
    defaultSymbolVisibility: 2
  }
};
function addFile(proto, reg) {
  var _a, _b;
  const file = {
    kind: "file",
    proto,
    deprecated: (_b = (_a = proto.options) === null || _a === undefined ? undefined : _a.deprecated) !== null && _b !== undefined ? _b : false,
    edition: getFileEdition(proto),
    name: proto.name.replace(/\.proto$/, ""),
    dependencies: findFileDependencies(proto, reg),
    enums: [],
    messages: [],
    extensions: [],
    services: [],
    toString() {
      return `file ${proto.name}`;
    }
  };
  const mapEntriesStore = new Map;
  const mapEntries = {
    get(typeName) {
      return mapEntriesStore.get(typeName);
    },
    add(desc) {
      var _a2;
      assert(((_a2 = desc.proto.options) === null || _a2 === undefined ? undefined : _a2.mapEntry) === true);
      mapEntriesStore.set(desc.typeName, desc);
    }
  };
  for (const enumProto of proto.enumType) {
    addEnum(enumProto, file, undefined, reg);
  }
  for (const messageProto of proto.messageType) {
    addMessage(messageProto, file, undefined, reg, mapEntries);
  }
  for (const serviceProto of proto.service) {
    addService(serviceProto, file, reg);
  }
  addExtensions(file, reg);
  for (const mapEntry of mapEntriesStore.values()) {
    addFields(mapEntry, reg, mapEntries);
  }
  for (const message of file.messages) {
    addFields(message, reg, mapEntries);
    addExtensions(message, reg);
  }
  reg.addFile(file, true);
}
function addExtensions(desc, reg) {
  switch (desc.kind) {
    case "file":
      for (const proto of desc.proto.extension) {
        const ext = newField(proto, desc, reg);
        desc.extensions.push(ext);
        reg.add(ext);
      }
      break;
    case "message":
      for (const proto of desc.proto.extension) {
        const ext = newField(proto, desc, reg);
        desc.nestedExtensions.push(ext);
        reg.add(ext);
      }
      for (const message of desc.nestedMessages) {
        addExtensions(message, reg);
      }
      break;
  }
}
function addFields(message, reg, mapEntries) {
  const allOneofs = message.proto.oneofDecl.map((proto) => newOneof(proto, message));
  const oneofsSeen = new Set;
  for (const proto of message.proto.field) {
    const oneof = findOneof(proto, allOneofs);
    const field = newField(proto, message, reg, oneof, mapEntries);
    message.fields.push(field);
    message.field[field.localName] = field;
    if (oneof === undefined) {
      message.members.push(field);
    } else {
      oneof.fields.push(field);
      if (!oneofsSeen.has(oneof)) {
        oneofsSeen.add(oneof);
        message.members.push(oneof);
      }
    }
  }
  for (const oneof of allOneofs.filter((o) => oneofsSeen.has(o))) {
    message.oneofs.push(oneof);
  }
  for (const child of message.nestedMessages) {
    addFields(child, reg, mapEntries);
  }
}
function addEnum(proto, file, parent, reg) {
  var _a, _b, _c, _d, _e;
  const sharedPrefix = findEnumSharedPrefix(proto.name, proto.value);
  const desc = {
    kind: "enum",
    proto,
    deprecated: (_b = (_a = proto.options) === null || _a === undefined ? undefined : _a.deprecated) !== null && _b !== undefined ? _b : false,
    file,
    parent,
    open: true,
    name: proto.name,
    typeName: makeTypeName(proto, parent, file),
    value: {},
    values: [],
    sharedPrefix,
    toString() {
      return `enum ${this.typeName}`;
    }
  };
  desc.open = isEnumOpen(desc);
  reg.add(desc);
  for (const p of proto.value) {
    const name = p.name;
    desc.values.push(desc.value[p.number] = {
      kind: "enum_value",
      proto: p,
      deprecated: (_d = (_c = p.options) === null || _c === undefined ? undefined : _c.deprecated) !== null && _d !== undefined ? _d : false,
      parent: desc,
      name,
      localName: safeObjectProperty(sharedPrefix == undefined ? name : name.substring(sharedPrefix.length)),
      number: p.number,
      toString() {
        return `enum value ${desc.typeName}.${name}`;
      }
    });
  }
  ((_e = parent === null || parent === undefined ? undefined : parent.nestedEnums) !== null && _e !== undefined ? _e : file.enums).push(desc);
}
function addMessage(proto, file, parent, reg, mapEntries) {
  var _a, _b, _c, _d;
  const desc = {
    kind: "message",
    proto,
    deprecated: (_b = (_a = proto.options) === null || _a === undefined ? undefined : _a.deprecated) !== null && _b !== undefined ? _b : false,
    file,
    parent,
    name: proto.name,
    typeName: makeTypeName(proto, parent, file),
    fields: [],
    field: {},
    oneofs: [],
    members: [],
    nestedEnums: [],
    nestedMessages: [],
    nestedExtensions: [],
    toString() {
      return `message ${this.typeName}`;
    }
  };
  if (((_c = proto.options) === null || _c === undefined ? undefined : _c.mapEntry) === true) {
    mapEntries.add(desc);
  } else {
    ((_d = parent === null || parent === undefined ? undefined : parent.nestedMessages) !== null && _d !== undefined ? _d : file.messages).push(desc);
    reg.add(desc);
  }
  for (const enumProto of proto.enumType) {
    addEnum(enumProto, file, desc, reg);
  }
  for (const messageProto of proto.nestedType) {
    addMessage(messageProto, file, desc, reg, mapEntries);
  }
}
function addService(proto, file, reg) {
  var _a, _b;
  const desc = {
    kind: "service",
    proto,
    deprecated: (_b = (_a = proto.options) === null || _a === undefined ? undefined : _a.deprecated) !== null && _b !== undefined ? _b : false,
    file,
    name: proto.name,
    typeName: makeTypeName(proto, undefined, file),
    methods: [],
    method: {},
    toString() {
      return `service ${this.typeName}`;
    }
  };
  file.services.push(desc);
  reg.add(desc);
  for (const methodProto of proto.method) {
    const method = newMethod(methodProto, desc, reg);
    desc.methods.push(method);
    desc.method[method.localName] = method;
  }
}
function newMethod(proto, parent, reg) {
  var _a, _b, _c, _d;
  let methodKind;
  if (proto.clientStreaming && proto.serverStreaming) {
    methodKind = "bidi_streaming";
  } else if (proto.clientStreaming) {
    methodKind = "client_streaming";
  } else if (proto.serverStreaming) {
    methodKind = "server_streaming";
  } else {
    methodKind = "unary";
  }
  const input = reg.getMessage(trimLeadingDot(proto.inputType));
  const output = reg.getMessage(trimLeadingDot(proto.outputType));
  assert(input, `invalid MethodDescriptorProto: input_type ${proto.inputType} not found`);
  assert(output, `invalid MethodDescriptorProto: output_type ${proto.inputType} not found`);
  const name = proto.name;
  return {
    kind: "rpc",
    proto,
    deprecated: (_b = (_a = proto.options) === null || _a === undefined ? undefined : _a.deprecated) !== null && _b !== undefined ? _b : false,
    parent,
    name,
    localName: safeObjectProperty(name.length ? safeObjectProperty(name[0].toLowerCase() + name.substring(1)) : name),
    methodKind,
    input,
    output,
    idempotency: (_d = (_c = proto.options) === null || _c === undefined ? undefined : _c.idempotencyLevel) !== null && _d !== undefined ? _d : IDEMPOTENCY_UNKNOWN,
    toString() {
      return `rpc ${parent.typeName}.${name}`;
    }
  };
}
function newOneof(proto, parent) {
  return {
    kind: "oneof",
    proto,
    deprecated: false,
    parent,
    fields: [],
    name: proto.name,
    localName: safeObjectProperty(protoCamelCase(proto.name)),
    toString() {
      return `oneof ${parent.typeName}.${this.name}`;
    }
  };
}
function newField(proto, parentOrFile, reg, oneof, mapEntries) {
  var _a, _b, _c;
  const isExtension = mapEntries === undefined;
  const field = {
    kind: "field",
    proto,
    deprecated: (_b = (_a = proto.options) === null || _a === undefined ? undefined : _a.deprecated) !== null && _b !== undefined ? _b : false,
    name: proto.name,
    number: proto.number,
    scalar: undefined,
    message: undefined,
    enum: undefined,
    presence: getFieldPresence(proto, oneof, isExtension, parentOrFile),
    utf8Validation: isUtf8Validated(proto, parentOrFile),
    listKind: undefined,
    mapKind: undefined,
    mapKey: undefined,
    delimitedEncoding: undefined,
    packed: undefined,
    longAsString: false,
    getDefaultValue: undefined
  };
  let toStr;
  if (isExtension) {
    const file = parentOrFile.kind == "file" ? parentOrFile : parentOrFile.file;
    const parent = parentOrFile.kind == "file" ? undefined : parentOrFile;
    const typeName = makeTypeName(proto, parent, file);
    field.kind = "extension";
    field.file = file;
    field.parent = parent;
    field.oneof = undefined;
    field.typeName = typeName;
    field.jsonName = `[${typeName}]`;
    toStr = () => `extension ${typeName}`;
    const extendee = reg.getMessage(trimLeadingDot(proto.extendee));
    assert(extendee, `invalid FieldDescriptorProto: extendee ${proto.extendee} not found`);
    field.extendee = extendee;
  } else {
    const parent = parentOrFile;
    assert(parent.kind == "message");
    field.parent = parent;
    field.oneof = oneof;
    field.localName = oneof ? protoCamelCase(proto.name) : safeObjectProperty(protoCamelCase(proto.name));
    field.jsonName = proto.jsonName;
    toStr = () => `field ${parent.typeName}.${proto.name}`;
  }
  Object.defineProperty(field, "toString", {
    value: toStr,
    writable: true,
    enumerable: true,
    configurable: true
  });
  const label = proto.label;
  const type = proto.type;
  const jstype = (_c = proto.options) === null || _c === undefined ? undefined : _c.jstype;
  if (label === LABEL_REPEATED) {
    const mapEntry = type == TYPE_MESSAGE ? mapEntries === null || mapEntries === undefined ? undefined : mapEntries.get(trimLeadingDot(proto.typeName)) : undefined;
    if (mapEntry) {
      field.fieldKind = "map";
      const { key, value } = findMapEntryFields(mapEntry);
      field.mapKey = key.scalar;
      field.mapKind = value.fieldKind;
      field.message = value.message;
      field.delimitedEncoding = false;
      field.enum = value.enum;
      field.scalar = value.scalar;
      return field;
    }
    field.fieldKind = "list";
    switch (type) {
      case TYPE_MESSAGE:
      case TYPE_GROUP:
        field.listKind = "message";
        field.message = reg.getMessage(trimLeadingDot(proto.typeName));
        assert(field.message);
        field.delimitedEncoding = isDelimitedEncoding(proto, parentOrFile);
        break;
      case TYPE_ENUM:
        field.listKind = "enum";
        field.enum = reg.getEnum(trimLeadingDot(proto.typeName));
        assert(field.enum);
        break;
      default:
        field.listKind = "scalar";
        field.scalar = type;
        field.longAsString = jstype == JS_STRING;
        break;
    }
    field.packed = isPackedField(proto, parentOrFile);
    return field;
  }
  switch (type) {
    case TYPE_MESSAGE:
    case TYPE_GROUP:
      field.fieldKind = "message";
      field.message = reg.getMessage(trimLeadingDot(proto.typeName));
      assert(field.message, `invalid FieldDescriptorProto: type_name ${proto.typeName} not found`);
      field.delimitedEncoding = isDelimitedEncoding(proto, parentOrFile);
      field.getDefaultValue = () => {
        return;
      };
      break;
    case TYPE_ENUM: {
      const enumeration = reg.getEnum(trimLeadingDot(proto.typeName));
      assert(enumeration !== undefined, `invalid FieldDescriptorProto: type_name ${proto.typeName} not found`);
      field.fieldKind = "enum";
      field.enum = reg.getEnum(trimLeadingDot(proto.typeName));
      field.getDefaultValue = () => {
        return unsafeIsSetExplicit(proto, "defaultValue") ? parseTextFormatEnumValue(enumeration, proto.defaultValue) : undefined;
      };
      break;
    }
    default: {
      field.fieldKind = "scalar";
      field.scalar = type;
      field.longAsString = jstype == JS_STRING;
      field.getDefaultValue = () => {
        return unsafeIsSetExplicit(proto, "defaultValue") ? parseTextFormatScalarValue(type, proto.defaultValue) : undefined;
      };
      break;
    }
  }
  return field;
}
function getFileEdition(proto) {
  switch (proto.syntax) {
    case "":
    case "proto2":
      return EDITION_PROTO22;
    case "proto3":
      return EDITION_PROTO32;
    case "editions":
      if (proto.edition === EDITION_UNSTABLE) {
        return maximumEdition;
      }
      if (proto.edition in featureDefaults) {
        return proto.edition;
      }
      throw new Error(`${proto.name}: unsupported edition`);
    default:
      throw new Error(`${proto.name}: unsupported syntax "${proto.syntax}"`);
  }
}
function findFileDependencies(proto, reg) {
  return proto.dependency.map((wantName) => {
    const dep = reg.getFile(wantName);
    if (!dep) {
      throw new Error(`Cannot find ${wantName}, imported by ${proto.name}`);
    }
    return dep;
  });
}
function findEnumSharedPrefix(enumName, values) {
  const prefix = camelToSnakeCase(enumName) + "_";
  for (const value of values) {
    if (!value.name.toLowerCase().startsWith(prefix)) {
      return;
    }
    const shortName = value.name.substring(prefix.length);
    if (shortName.length == 0) {
      return;
    }
    if (/^\d/.test(shortName)) {
      return;
    }
  }
  return prefix;
}
function camelToSnakeCase(camel) {
  return (camel.substring(0, 1) + camel.substring(1).replace(/[A-Z]/g, (c) => "_" + c)).toLowerCase();
}
function makeTypeName(proto, parent, file) {
  let typeName;
  if (parent) {
    typeName = `${parent.typeName}.${proto.name}`;
  } else if (file.proto.package.length > 0) {
    typeName = `${file.proto.package}.${proto.name}`;
  } else {
    typeName = `${proto.name}`;
  }
  return typeName;
}
function trimLeadingDot(typeName) {
  return typeName.startsWith(".") ? typeName.substring(1) : typeName;
}
function findOneof(proto, allOneofs) {
  if (!unsafeIsSetExplicit(proto, "oneofIndex")) {
    return;
  }
  if (proto.proto3Optional) {
    return;
  }
  const oneof = allOneofs[proto.oneofIndex];
  assert(oneof, `invalid FieldDescriptorProto: oneof #${proto.oneofIndex} for field #${proto.number} not found`);
  return oneof;
}
function getFieldPresence(proto, oneof, isExtension, parent) {
  if (proto.label == LABEL_REQUIRED) {
    return LEGACY_REQUIRED;
  }
  if (proto.label == LABEL_REPEATED) {
    return IMPLICIT2;
  }
  if (!!oneof || proto.proto3Optional) {
    return EXPLICIT;
  }
  if (isExtension) {
    return EXPLICIT;
  }
  const resolved = resolveFeature("fieldPresence", { proto, parent });
  if (resolved == IMPLICIT2 && (proto.type == TYPE_MESSAGE || proto.type == TYPE_GROUP)) {
    return EXPLICIT;
  }
  return resolved;
}
function isPackedField(proto, parent) {
  if (proto.label != LABEL_REPEATED) {
    return false;
  }
  switch (proto.type) {
    case TYPE_STRING:
    case TYPE_BYTES:
    case TYPE_GROUP:
    case TYPE_MESSAGE:
      return false;
  }
  const o = proto.options;
  if (o && unsafeIsSetExplicit(o, "packed")) {
    return o.packed;
  }
  return PACKED == resolveFeature("repeatedFieldEncoding", {
    proto,
    parent
  });
}
function findMapEntryFields(mapEntry) {
  const key = mapEntry.fields.find((f) => f.number === 1);
  const value = mapEntry.fields.find((f) => f.number === 2);
  assert(key && key.fieldKind == "scalar" && key.scalar != ScalarType.BYTES && key.scalar != ScalarType.FLOAT && key.scalar != ScalarType.DOUBLE && value && value.fieldKind != "list" && value.fieldKind != "map");
  return { key, value };
}
function isEnumOpen(desc) {
  var _a;
  return OPEN == resolveFeature("enumType", {
    proto: desc.proto,
    parent: (_a = desc.parent) !== null && _a !== undefined ? _a : desc.file
  });
}
function isDelimitedEncoding(proto, parent) {
  if (proto.type == TYPE_GROUP) {
    return true;
  }
  return DELIMITED == resolveFeature("messageEncoding", {
    proto,
    parent
  });
}
function isUtf8Validated(proto, parent) {
  return VERIFY == resolveFeature("utf8Validation", {
    proto,
    parent
  });
}
function resolveFeature(name, ref) {
  var _a, _b;
  const featureSet = (_a = ref.proto.options) === null || _a === undefined ? undefined : _a.features;
  if (featureSet) {
    const val = featureSet[name];
    if (val != 0) {
      return val;
    }
  }
  if ("kind" in ref) {
    if (ref.kind == "message") {
      return resolveFeature(name, (_b = ref.parent) !== null && _b !== undefined ? _b : ref.file);
    }
    const editionDefaults = featureDefaults[ref.edition];
    if (!editionDefaults) {
      throw new Error(`feature default for edition ${ref.edition} not found`);
    }
    return editionDefaults[name];
  }
  return resolveFeature(name, ref.parent);
}
function assert(condition, msg) {
  if (!condition) {
    throw new Error(msg);
  }
}

// node_modules/@bufbuild/protobuf/dist/esm/codegenv2/boot.js
function boot(boot2) {
  const root = bootFileDescriptorProto(boot2);
  root.messageType.forEach(restoreJsonNames);
  const reg = createFileRegistry(root, () => {
    return;
  });
  return reg.getFile(root.name);
}
function bootFileDescriptorProto(init) {
  const proto = Object.create({
    syntax: "",
    edition: 0
  });
  return Object.assign(proto, Object.assign(Object.assign({ $typeName: "google.protobuf.FileDescriptorProto", dependency: [], publicDependency: [], weakDependency: [], optionDependency: [], service: [], extension: [] }, init), { messageType: init.messageType.map(bootDescriptorProto), enumType: init.enumType.map(bootEnumDescriptorProto) }));
}
function bootDescriptorProto(init) {
  var _a, _b, _c, _d, _e, _f, _g, _h;
  const proto = Object.create({
    visibility: 0
  });
  return Object.assign(proto, {
    $typeName: "google.protobuf.DescriptorProto",
    name: init.name,
    field: (_b = (_a = init.field) === null || _a === undefined ? undefined : _a.map(bootFieldDescriptorProto)) !== null && _b !== undefined ? _b : [],
    extension: [],
    nestedType: (_d = (_c = init.nestedType) === null || _c === undefined ? undefined : _c.map(bootDescriptorProto)) !== null && _d !== undefined ? _d : [],
    enumType: (_f = (_e = init.enumType) === null || _e === undefined ? undefined : _e.map(bootEnumDescriptorProto)) !== null && _f !== undefined ? _f : [],
    extensionRange: (_h = (_g = init.extensionRange) === null || _g === undefined ? undefined : _g.map((e) => Object.assign({ $typeName: "google.protobuf.DescriptorProto.ExtensionRange" }, e))) !== null && _h !== undefined ? _h : [],
    oneofDecl: [],
    reservedRange: [],
    reservedName: []
  });
}
function bootFieldDescriptorProto(init) {
  const proto = Object.create({
    label: 1,
    typeName: "",
    extendee: "",
    defaultValue: "",
    oneofIndex: 0,
    jsonName: "",
    proto3Optional: false
  });
  return Object.assign(proto, Object.assign(Object.assign({ $typeName: "google.protobuf.FieldDescriptorProto" }, init), { options: init.options ? bootFieldOptions(init.options) : undefined }));
}
function bootFieldOptions(init) {
  var _a, _b, _c;
  const proto = Object.create({
    ctype: 0,
    packed: false,
    jstype: 0,
    lazy: false,
    unverifiedLazy: false,
    deprecated: false,
    weak: false,
    debugRedact: false,
    retention: 0
  });
  return Object.assign(proto, Object.assign(Object.assign({ $typeName: "google.protobuf.FieldOptions" }, init), { targets: (_a = init.targets) !== null && _a !== undefined ? _a : [], editionDefaults: (_c = (_b = init.editionDefaults) === null || _b === undefined ? undefined : _b.map((e) => Object.assign({ $typeName: "google.protobuf.FieldOptions.EditionDefault" }, e))) !== null && _c !== undefined ? _c : [], uninterpretedOption: [] }));
}
function bootEnumDescriptorProto(init) {
  const proto = Object.create({
    visibility: 0
  });
  return Object.assign(proto, {
    $typeName: "google.protobuf.EnumDescriptorProto",
    name: init.name,
    reservedName: [],
    reservedRange: [],
    value: init.value.map((e) => Object.assign({ $typeName: "google.protobuf.EnumValueDescriptorProto" }, e))
  });
}

// node_modules/@bufbuild/protobuf/dist/esm/codegenv2/message.js
function messageDesc(file, path, ...paths) {
  return paths.reduce((acc, cur) => acc.nestedMessages[cur], file.messages[path]);
}

// node_modules/@bufbuild/protobuf/dist/esm/wkt/gen/google/protobuf/descriptor_pb.js
var file_google_protobuf_descriptor = /* @__PURE__ */ boot({ name: "google/protobuf/descriptor.proto", package: "google.protobuf", messageType: [{ name: "FileDescriptorSet", field: [{ name: "file", number: 1, type: 11, label: 3, typeName: ".google.protobuf.FileDescriptorProto" }], extensionRange: [{ start: 536000000, end: 536000001 }] }, { name: "FileDescriptorProto", field: [{ name: "name", number: 1, type: 9, label: 1 }, { name: "package", number: 2, type: 9, label: 1 }, { name: "dependency", number: 3, type: 9, label: 3 }, { name: "public_dependency", number: 10, type: 5, label: 3 }, { name: "weak_dependency", number: 11, type: 5, label: 3 }, { name: "option_dependency", number: 15, type: 9, label: 3 }, { name: "message_type", number: 4, type: 11, label: 3, typeName: ".google.protobuf.DescriptorProto" }, { name: "enum_type", number: 5, type: 11, label: 3, typeName: ".google.protobuf.EnumDescriptorProto" }, { name: "service", number: 6, type: 11, label: 3, typeName: ".google.protobuf.ServiceDescriptorProto" }, { name: "extension", number: 7, type: 11, label: 3, typeName: ".google.protobuf.FieldDescriptorProto" }, { name: "options", number: 8, type: 11, label: 1, typeName: ".google.protobuf.FileOptions" }, { name: "source_code_info", number: 9, type: 11, label: 1, typeName: ".google.protobuf.SourceCodeInfo" }, { name: "syntax", number: 12, type: 9, label: 1 }, { name: "edition", number: 14, type: 14, label: 1, typeName: ".google.protobuf.Edition" }] }, { name: "DescriptorProto", field: [{ name: "name", number: 1, type: 9, label: 1 }, { name: "field", number: 2, type: 11, label: 3, typeName: ".google.protobuf.FieldDescriptorProto" }, { name: "extension", number: 6, type: 11, label: 3, typeName: ".google.protobuf.FieldDescriptorProto" }, { name: "nested_type", number: 3, type: 11, label: 3, typeName: ".google.protobuf.DescriptorProto" }, { name: "enum_type", number: 4, type: 11, label: 3, typeName: ".google.protobuf.EnumDescriptorProto" }, { name: "extension_range", number: 5, type: 11, label: 3, typeName: ".google.protobuf.DescriptorProto.ExtensionRange" }, { name: "oneof_decl", number: 8, type: 11, label: 3, typeName: ".google.protobuf.OneofDescriptorProto" }, { name: "options", number: 7, type: 11, label: 1, typeName: ".google.protobuf.MessageOptions" }, { name: "reserved_range", number: 9, type: 11, label: 3, typeName: ".google.protobuf.DescriptorProto.ReservedRange" }, { name: "reserved_name", number: 10, type: 9, label: 3 }, { name: "visibility", number: 11, type: 14, label: 1, typeName: ".google.protobuf.SymbolVisibility" }], nestedType: [{ name: "ExtensionRange", field: [{ name: "start", number: 1, type: 5, label: 1 }, { name: "end", number: 2, type: 5, label: 1 }, { name: "options", number: 3, type: 11, label: 1, typeName: ".google.protobuf.ExtensionRangeOptions" }] }, { name: "ReservedRange", field: [{ name: "start", number: 1, type: 5, label: 1 }, { name: "end", number: 2, type: 5, label: 1 }] }] }, { name: "ExtensionRangeOptions", field: [{ name: "uninterpreted_option", number: 999, type: 11, label: 3, typeName: ".google.protobuf.UninterpretedOption" }, { name: "declaration", number: 2, type: 11, label: 3, typeName: ".google.protobuf.ExtensionRangeOptions.Declaration", options: { retention: 2 } }, { name: "features", number: 50, type: 11, label: 1, typeName: ".google.protobuf.FeatureSet" }, { name: "verification", number: 3, type: 14, label: 1, typeName: ".google.protobuf.ExtensionRangeOptions.VerificationState", defaultValue: "UNVERIFIED", options: { retention: 2 } }], nestedType: [{ name: "Declaration", field: [{ name: "number", number: 1, type: 5, label: 1 }, { name: "full_name", number: 2, type: 9, label: 1 }, { name: "type", number: 3, type: 9, label: 1 }, { name: "reserved", number: 5, type: 8, label: 1 }, { name: "repeated", number: 6, type: 8, label: 1 }] }], enumType: [{ name: "VerificationState", value: [{ name: "DECLARATION", number: 0 }, { name: "UNVERIFIED", number: 1 }] }], extensionRange: [{ start: 1000, end: 536870912 }] }, { name: "FieldDescriptorProto", field: [{ name: "name", number: 1, type: 9, label: 1 }, { name: "number", number: 3, type: 5, label: 1 }, { name: "label", number: 4, type: 14, label: 1, typeName: ".google.protobuf.FieldDescriptorProto.Label" }, { name: "type", number: 5, type: 14, label: 1, typeName: ".google.protobuf.FieldDescriptorProto.Type" }, { name: "type_name", number: 6, type: 9, label: 1 }, { name: "extendee", number: 2, type: 9, label: 1 }, { name: "default_value", number: 7, type: 9, label: 1 }, { name: "oneof_index", number: 9, type: 5, label: 1 }, { name: "json_name", number: 10, type: 9, label: 1 }, { name: "options", number: 8, type: 11, label: 1, typeName: ".google.protobuf.FieldOptions" }, { name: "proto3_optional", number: 17, type: 8, label: 1 }], enumType: [{ name: "Type", value: [{ name: "TYPE_DOUBLE", number: 1 }, { name: "TYPE_FLOAT", number: 2 }, { name: "TYPE_INT64", number: 3 }, { name: "TYPE_UINT64", number: 4 }, { name: "TYPE_INT32", number: 5 }, { name: "TYPE_FIXED64", number: 6 }, { name: "TYPE_FIXED32", number: 7 }, { name: "TYPE_BOOL", number: 8 }, { name: "TYPE_STRING", number: 9 }, { name: "TYPE_GROUP", number: 10 }, { name: "TYPE_MESSAGE", number: 11 }, { name: "TYPE_BYTES", number: 12 }, { name: "TYPE_UINT32", number: 13 }, { name: "TYPE_ENUM", number: 14 }, { name: "TYPE_SFIXED32", number: 15 }, { name: "TYPE_SFIXED64", number: 16 }, { name: "TYPE_SINT32", number: 17 }, { name: "TYPE_SINT64", number: 18 }] }, { name: "Label", value: [{ name: "LABEL_OPTIONAL", number: 1 }, { name: "LABEL_REPEATED", number: 3 }, { name: "LABEL_REQUIRED", number: 2 }] }] }, { name: "OneofDescriptorProto", field: [{ name: "name", number: 1, type: 9, label: 1 }, { name: "options", number: 2, type: 11, label: 1, typeName: ".google.protobuf.OneofOptions" }] }, { name: "EnumDescriptorProto", field: [{ name: "name", number: 1, type: 9, label: 1 }, { name: "value", number: 2, type: 11, label: 3, typeName: ".google.protobuf.EnumValueDescriptorProto" }, { name: "options", number: 3, type: 11, label: 1, typeName: ".google.protobuf.EnumOptions" }, { name: "reserved_range", number: 4, type: 11, label: 3, typeName: ".google.protobuf.EnumDescriptorProto.EnumReservedRange" }, { name: "reserved_name", number: 5, type: 9, label: 3 }, { name: "visibility", number: 6, type: 14, label: 1, typeName: ".google.protobuf.SymbolVisibility" }], nestedType: [{ name: "EnumReservedRange", field: [{ name: "start", number: 1, type: 5, label: 1 }, { name: "end", number: 2, type: 5, label: 1 }] }] }, { name: "EnumValueDescriptorProto", field: [{ name: "name", number: 1, type: 9, label: 1 }, { name: "number", number: 2, type: 5, label: 1 }, { name: "options", number: 3, type: 11, label: 1, typeName: ".google.protobuf.EnumValueOptions" }] }, { name: "ServiceDescriptorProto", field: [{ name: "name", number: 1, type: 9, label: 1 }, { name: "method", number: 2, type: 11, label: 3, typeName: ".google.protobuf.MethodDescriptorProto" }, { name: "options", number: 3, type: 11, label: 1, typeName: ".google.protobuf.ServiceOptions" }] }, { name: "MethodDescriptorProto", field: [{ name: "name", number: 1, type: 9, label: 1 }, { name: "input_type", number: 2, type: 9, label: 1 }, { name: "output_type", number: 3, type: 9, label: 1 }, { name: "options", number: 4, type: 11, label: 1, typeName: ".google.protobuf.MethodOptions" }, { name: "client_streaming", number: 5, type: 8, label: 1, defaultValue: "false" }, { name: "server_streaming", number: 6, type: 8, label: 1, defaultValue: "false" }] }, { name: "FileOptions", field: [{ name: "java_package", number: 1, type: 9, label: 1 }, { name: "java_outer_classname", number: 8, type: 9, label: 1 }, { name: "java_multiple_files", number: 10, type: 8, label: 1, defaultValue: "false", options: {} }, { name: "java_generate_equals_and_hash", number: 20, type: 8, label: 1, options: { deprecated: true } }, { name: "java_string_check_utf8", number: 27, type: 8, label: 1, defaultValue: "false" }, { name: "optimize_for", number: 9, type: 14, label: 1, typeName: ".google.protobuf.FileOptions.OptimizeMode", defaultValue: "SPEED" }, { name: "go_package", number: 11, type: 9, label: 1 }, { name: "cc_generic_services", number: 16, type: 8, label: 1, defaultValue: "false" }, { name: "java_generic_services", number: 17, type: 8, label: 1, defaultValue: "false" }, { name: "py_generic_services", number: 18, type: 8, label: 1, defaultValue: "false" }, { name: "deprecated", number: 23, type: 8, label: 1, defaultValue: "false" }, { name: "cc_enable_arenas", number: 31, type: 8, label: 1, defaultValue: "true" }, { name: "objc_class_prefix", number: 36, type: 9, label: 1 }, { name: "csharp_namespace", number: 37, type: 9, label: 1 }, { name: "swift_prefix", number: 39, type: 9, label: 1 }, { name: "php_class_prefix", number: 40, type: 9, label: 1 }, { name: "php_namespace", number: 41, type: 9, label: 1 }, { name: "php_metadata_namespace", number: 44, type: 9, label: 1 }, { name: "ruby_package", number: 45, type: 9, label: 1 }, { name: "features", number: 50, type: 11, label: 1, typeName: ".google.protobuf.FeatureSet" }, { name: "uninterpreted_option", number: 999, type: 11, label: 3, typeName: ".google.protobuf.UninterpretedOption" }], enumType: [{ name: "OptimizeMode", value: [{ name: "SPEED", number: 1 }, { name: "CODE_SIZE", number: 2 }, { name: "LITE_RUNTIME", number: 3 }] }], extensionRange: [{ start: 1000, end: 536870912 }] }, { name: "MessageOptions", field: [{ name: "message_set_wire_format", number: 1, type: 8, label: 1, defaultValue: "false" }, { name: "no_standard_descriptor_accessor", number: 2, type: 8, label: 1, defaultValue: "false" }, { name: "deprecated", number: 3, type: 8, label: 1, defaultValue: "false" }, { name: "map_entry", number: 7, type: 8, label: 1 }, { name: "deprecated_legacy_json_field_conflicts", number: 11, type: 8, label: 1, options: { deprecated: true } }, { name: "features", number: 12, type: 11, label: 1, typeName: ".google.protobuf.FeatureSet" }, { name: "uninterpreted_option", number: 999, type: 11, label: 3, typeName: ".google.protobuf.UninterpretedOption" }], extensionRange: [{ start: 1000, end: 536870912 }] }, { name: "FieldOptions", field: [{ name: "ctype", number: 1, type: 14, label: 1, typeName: ".google.protobuf.FieldOptions.CType", defaultValue: "STRING" }, { name: "packed", number: 2, type: 8, label: 1 }, { name: "jstype", number: 6, type: 14, label: 1, typeName: ".google.protobuf.FieldOptions.JSType", defaultValue: "JS_NORMAL" }, { name: "lazy", number: 5, type: 8, label: 1, defaultValue: "false" }, { name: "unverified_lazy", number: 15, type: 8, label: 1, defaultValue: "false" }, { name: "deprecated", number: 3, type: 8, label: 1, defaultValue: "false" }, { name: "weak", number: 10, type: 8, label: 1, defaultValue: "false", options: { deprecated: true } }, { name: "debug_redact", number: 16, type: 8, label: 1, defaultValue: "false" }, { name: "retention", number: 17, type: 14, label: 1, typeName: ".google.protobuf.FieldOptions.OptionRetention" }, { name: "targets", number: 19, type: 14, label: 3, typeName: ".google.protobuf.FieldOptions.OptionTargetType" }, { name: "edition_defaults", number: 20, type: 11, label: 3, typeName: ".google.protobuf.FieldOptions.EditionDefault" }, { name: "features", number: 21, type: 11, label: 1, typeName: ".google.protobuf.FeatureSet" }, { name: "feature_support", number: 22, type: 11, label: 1, typeName: ".google.protobuf.FieldOptions.FeatureSupport" }, { name: "uninterpreted_option", number: 999, type: 11, label: 3, typeName: ".google.protobuf.UninterpretedOption" }], nestedType: [{ name: "EditionDefault", field: [{ name: "edition", number: 3, type: 14, label: 1, typeName: ".google.protobuf.Edition" }, { name: "value", number: 2, type: 9, label: 1 }] }, { name: "FeatureSupport", field: [{ name: "edition_introduced", number: 1, type: 14, label: 1, typeName: ".google.protobuf.Edition" }, { name: "edition_deprecated", number: 2, type: 14, label: 1, typeName: ".google.protobuf.Edition" }, { name: "deprecation_warning", number: 3, type: 9, label: 1 }, { name: "edition_removed", number: 4, type: 14, label: 1, typeName: ".google.protobuf.Edition" }, { name: "removal_error", number: 5, type: 9, label: 1 }] }], enumType: [{ name: "CType", value: [{ name: "STRING", number: 0 }, { name: "CORD", number: 1 }, { name: "STRING_PIECE", number: 2 }] }, { name: "JSType", value: [{ name: "JS_NORMAL", number: 0 }, { name: "JS_STRING", number: 1 }, { name: "JS_NUMBER", number: 2 }] }, { name: "OptionRetention", value: [{ name: "RETENTION_UNKNOWN", number: 0 }, { name: "RETENTION_RUNTIME", number: 1 }, { name: "RETENTION_SOURCE", number: 2 }] }, { name: "OptionTargetType", value: [{ name: "TARGET_TYPE_UNKNOWN", number: 0 }, { name: "TARGET_TYPE_FILE", number: 1 }, { name: "TARGET_TYPE_EXTENSION_RANGE", number: 2 }, { name: "TARGET_TYPE_MESSAGE", number: 3 }, { name: "TARGET_TYPE_FIELD", number: 4 }, { name: "TARGET_TYPE_ONEOF", number: 5 }, { name: "TARGET_TYPE_ENUM", number: 6 }, { name: "TARGET_TYPE_ENUM_ENTRY", number: 7 }, { name: "TARGET_TYPE_SERVICE", number: 8 }, { name: "TARGET_TYPE_METHOD", number: 9 }] }], extensionRange: [{ start: 1000, end: 536870912 }] }, { name: "OneofOptions", field: [{ name: "features", number: 1, type: 11, label: 1, typeName: ".google.protobuf.FeatureSet" }, { name: "uninterpreted_option", number: 999, type: 11, label: 3, typeName: ".google.protobuf.UninterpretedOption" }], extensionRange: [{ start: 1000, end: 536870912 }] }, { name: "EnumOptions", field: [{ name: "allow_alias", number: 2, type: 8, label: 1 }, { name: "deprecated", number: 3, type: 8, label: 1, defaultValue: "false" }, { name: "deprecated_legacy_json_field_conflicts", number: 6, type: 8, label: 1, options: { deprecated: true } }, { name: "features", number: 7, type: 11, label: 1, typeName: ".google.protobuf.FeatureSet" }, { name: "uninterpreted_option", number: 999, type: 11, label: 3, typeName: ".google.protobuf.UninterpretedOption" }], extensionRange: [{ start: 1000, end: 536870912 }] }, { name: "EnumValueOptions", field: [{ name: "deprecated", number: 1, type: 8, label: 1, defaultValue: "false" }, { name: "features", number: 2, type: 11, label: 1, typeName: ".google.protobuf.FeatureSet" }, { name: "debug_redact", number: 3, type: 8, label: 1, defaultValue: "false" }, { name: "feature_support", number: 4, type: 11, label: 1, typeName: ".google.protobuf.FieldOptions.FeatureSupport" }, { name: "uninterpreted_option", number: 999, type: 11, label: 3, typeName: ".google.protobuf.UninterpretedOption" }], extensionRange: [{ start: 1000, end: 536870912 }] }, { name: "ServiceOptions", field: [{ name: "features", number: 34, type: 11, label: 1, typeName: ".google.protobuf.FeatureSet" }, { name: "deprecated", number: 33, type: 8, label: 1, defaultValue: "false" }, { name: "uninterpreted_option", number: 999, type: 11, label: 3, typeName: ".google.protobuf.UninterpretedOption" }], extensionRange: [{ start: 1000, end: 536870912 }] }, { name: "MethodOptions", field: [{ name: "deprecated", number: 33, type: 8, label: 1, defaultValue: "false" }, { name: "idempotency_level", number: 34, type: 14, label: 1, typeName: ".google.protobuf.MethodOptions.IdempotencyLevel", defaultValue: "IDEMPOTENCY_UNKNOWN" }, { name: "features", number: 35, type: 11, label: 1, typeName: ".google.protobuf.FeatureSet" }, { name: "uninterpreted_option", number: 999, type: 11, label: 3, typeName: ".google.protobuf.UninterpretedOption" }], enumType: [{ name: "IdempotencyLevel", value: [{ name: "IDEMPOTENCY_UNKNOWN", number: 0 }, { name: "NO_SIDE_EFFECTS", number: 1 }, { name: "IDEMPOTENT", number: 2 }] }], extensionRange: [{ start: 1000, end: 536870912 }] }, { name: "UninterpretedOption", field: [{ name: "name", number: 2, type: 11, label: 3, typeName: ".google.protobuf.UninterpretedOption.NamePart" }, { name: "identifier_value", number: 3, type: 9, label: 1 }, { name: "positive_int_value", number: 4, type: 4, label: 1 }, { name: "negative_int_value", number: 5, type: 3, label: 1 }, { name: "double_value", number: 6, type: 1, label: 1 }, { name: "string_value", number: 7, type: 12, label: 1 }, { name: "aggregate_value", number: 8, type: 9, label: 1 }], nestedType: [{ name: "NamePart", field: [{ name: "name_part", number: 1, type: 9, label: 2 }, { name: "is_extension", number: 2, type: 8, label: 2 }] }] }, { name: "FeatureSet", field: [{ name: "field_presence", number: 1, type: 14, label: 1, typeName: ".google.protobuf.FeatureSet.FieldPresence", options: { retention: 1, targets: [4, 1], editionDefaults: [{ value: "EXPLICIT", edition: 900 }, { value: "IMPLICIT", edition: 999 }, { value: "EXPLICIT", edition: 1000 }] } }, { name: "enum_type", number: 2, type: 14, label: 1, typeName: ".google.protobuf.FeatureSet.EnumType", options: { retention: 1, targets: [6, 1], editionDefaults: [{ value: "CLOSED", edition: 900 }, { value: "OPEN", edition: 999 }] } }, { name: "repeated_field_encoding", number: 3, type: 14, label: 1, typeName: ".google.protobuf.FeatureSet.RepeatedFieldEncoding", options: { retention: 1, targets: [4, 1], editionDefaults: [{ value: "EXPANDED", edition: 900 }, { value: "PACKED", edition: 999 }] } }, { name: "utf8_validation", number: 4, type: 14, label: 1, typeName: ".google.protobuf.FeatureSet.Utf8Validation", options: { retention: 1, targets: [4, 1], editionDefaults: [{ value: "NONE", edition: 900 }, { value: "VERIFY", edition: 999 }] } }, { name: "message_encoding", number: 5, type: 14, label: 1, typeName: ".google.protobuf.FeatureSet.MessageEncoding", options: { retention: 1, targets: [4, 1], editionDefaults: [{ value: "LENGTH_PREFIXED", edition: 900 }] } }, { name: "json_format", number: 6, type: 14, label: 1, typeName: ".google.protobuf.FeatureSet.JsonFormat", options: { retention: 1, targets: [3, 6, 1], editionDefaults: [{ value: "LEGACY_BEST_EFFORT", edition: 900 }, { value: "ALLOW", edition: 999 }] } }, { name: "enforce_naming_style", number: 7, type: 14, label: 1, typeName: ".google.protobuf.FeatureSet.EnforceNamingStyle", options: { retention: 2, targets: [1, 2, 3, 4, 5, 6, 7, 8, 9], editionDefaults: [{ value: "STYLE_LEGACY", edition: 900 }, { value: "STYLE2024", edition: 1001 }] } }, { name: "default_symbol_visibility", number: 8, type: 14, label: 1, typeName: ".google.protobuf.FeatureSet.VisibilityFeature.DefaultSymbolVisibility", options: { retention: 2, targets: [1], editionDefaults: [{ value: "EXPORT_ALL", edition: 900 }, { value: "EXPORT_TOP_LEVEL", edition: 1001 }] } }], nestedType: [{ name: "VisibilityFeature", enumType: [{ name: "DefaultSymbolVisibility", value: [{ name: "DEFAULT_SYMBOL_VISIBILITY_UNKNOWN", number: 0 }, { name: "EXPORT_ALL", number: 1 }, { name: "EXPORT_TOP_LEVEL", number: 2 }, { name: "LOCAL_ALL", number: 3 }, { name: "STRICT", number: 4 }] }] }], enumType: [{ name: "FieldPresence", value: [{ name: "FIELD_PRESENCE_UNKNOWN", number: 0 }, { name: "EXPLICIT", number: 1 }, { name: "IMPLICIT", number: 2 }, { name: "LEGACY_REQUIRED", number: 3 }] }, { name: "EnumType", value: [{ name: "ENUM_TYPE_UNKNOWN", number: 0 }, { name: "OPEN", number: 1 }, { name: "CLOSED", number: 2 }] }, { name: "RepeatedFieldEncoding", value: [{ name: "REPEATED_FIELD_ENCODING_UNKNOWN", number: 0 }, { name: "PACKED", number: 1 }, { name: "EXPANDED", number: 2 }] }, { name: "Utf8Validation", value: [{ name: "UTF8_VALIDATION_UNKNOWN", number: 0 }, { name: "VERIFY", number: 2 }, { name: "NONE", number: 3 }] }, { name: "MessageEncoding", value: [{ name: "MESSAGE_ENCODING_UNKNOWN", number: 0 }, { name: "LENGTH_PREFIXED", number: 1 }, { name: "DELIMITED", number: 2 }] }, { name: "JsonFormat", value: [{ name: "JSON_FORMAT_UNKNOWN", number: 0 }, { name: "ALLOW", number: 1 }, { name: "LEGACY_BEST_EFFORT", number: 2 }] }, { name: "EnforceNamingStyle", value: [{ name: "ENFORCE_NAMING_STYLE_UNKNOWN", number: 0 }, { name: "STYLE2024", number: 1 }, { name: "STYLE_LEGACY", number: 2 }] }], extensionRange: [{ start: 1000, end: 9995 }, { start: 9995, end: 1e4 }, { start: 1e4, end: 10001 }] }, { name: "FeatureSetDefaults", field: [{ name: "defaults", number: 1, type: 11, label: 3, typeName: ".google.protobuf.FeatureSetDefaults.FeatureSetEditionDefault" }, { name: "minimum_edition", number: 4, type: 14, label: 1, typeName: ".google.protobuf.Edition" }, { name: "maximum_edition", number: 5, type: 14, label: 1, typeName: ".google.protobuf.Edition" }], nestedType: [{ name: "FeatureSetEditionDefault", field: [{ name: "edition", number: 3, type: 14, label: 1, typeName: ".google.protobuf.Edition" }, { name: "overridable_features", number: 4, type: 11, label: 1, typeName: ".google.protobuf.FeatureSet" }, { name: "fixed_features", number: 5, type: 11, label: 1, typeName: ".google.protobuf.FeatureSet" }] }] }, { name: "SourceCodeInfo", field: [{ name: "location", number: 1, type: 11, label: 3, typeName: ".google.protobuf.SourceCodeInfo.Location" }], nestedType: [{ name: "Location", field: [{ name: "path", number: 1, type: 5, label: 3, options: { packed: true } }, { name: "span", number: 2, type: 5, label: 3, options: { packed: true } }, { name: "leading_comments", number: 3, type: 9, label: 1 }, { name: "trailing_comments", number: 4, type: 9, label: 1 }, { name: "leading_detached_comments", number: 6, type: 9, label: 3 }] }], extensionRange: [{ start: 536000000, end: 536000001 }] }, { name: "GeneratedCodeInfo", field: [{ name: "annotation", number: 1, type: 11, label: 3, typeName: ".google.protobuf.GeneratedCodeInfo.Annotation" }], nestedType: [{ name: "Annotation", field: [{ name: "path", number: 1, type: 5, label: 3, options: { packed: true } }, { name: "source_file", number: 2, type: 9, label: 1 }, { name: "begin", number: 3, type: 5, label: 1 }, { name: "end", number: 4, type: 5, label: 1 }, { name: "semantic", number: 5, type: 14, label: 1, typeName: ".google.protobuf.GeneratedCodeInfo.Annotation.Semantic" }], enumType: [{ name: "Semantic", value: [{ name: "NONE", number: 0 }, { name: "SET", number: 1 }, { name: "ALIAS", number: 2 }] }] }] }], enumType: [{ name: "Edition", value: [{ name: "EDITION_UNKNOWN", number: 0 }, { name: "EDITION_LEGACY", number: 900 }, { name: "EDITION_PROTO2", number: 998 }, { name: "EDITION_PROTO3", number: 999 }, { name: "EDITION_2023", number: 1000 }, { name: "EDITION_2024", number: 1001 }, { name: "EDITION_UNSTABLE", number: 9999 }, { name: "EDITION_1_TEST_ONLY", number: 1 }, { name: "EDITION_2_TEST_ONLY", number: 2 }, { name: "EDITION_99997_TEST_ONLY", number: 99997 }, { name: "EDITION_99998_TEST_ONLY", number: 99998 }, { name: "EDITION_99999_TEST_ONLY", number: 99999 }, { name: "EDITION_MAX", number: 2147483647 }] }, { name: "SymbolVisibility", value: [{ name: "VISIBILITY_UNSET", number: 0 }, { name: "VISIBILITY_LOCAL", number: 1 }, { name: "VISIBILITY_EXPORT", number: 2 }] }] });
var FileDescriptorProtoSchema = /* @__PURE__ */ messageDesc(file_google_protobuf_descriptor, 1);
var ExtensionRangeOptions_VerificationState;
(function(ExtensionRangeOptions_VerificationState2) {
  ExtensionRangeOptions_VerificationState2[ExtensionRangeOptions_VerificationState2["DECLARATION"] = 0] = "DECLARATION";
  ExtensionRangeOptions_VerificationState2[ExtensionRangeOptions_VerificationState2["UNVERIFIED"] = 1] = "UNVERIFIED";
})(ExtensionRangeOptions_VerificationState || (ExtensionRangeOptions_VerificationState = {}));
var FieldDescriptorProto_Type;
(function(FieldDescriptorProto_Type2) {
  FieldDescriptorProto_Type2[FieldDescriptorProto_Type2["DOUBLE"] = 1] = "DOUBLE";
  FieldDescriptorProto_Type2[FieldDescriptorProto_Type2["FLOAT"] = 2] = "FLOAT";
  FieldDescriptorProto_Type2[FieldDescriptorProto_Type2["INT64"] = 3] = "INT64";
  FieldDescriptorProto_Type2[FieldDescriptorProto_Type2["UINT64"] = 4] = "UINT64";
  FieldDescriptorProto_Type2[FieldDescriptorProto_Type2["INT32"] = 5] = "INT32";
  FieldDescriptorProto_Type2[FieldDescriptorProto_Type2["FIXED64"] = 6] = "FIXED64";
  FieldDescriptorProto_Type2[FieldDescriptorProto_Type2["FIXED32"] = 7] = "FIXED32";
  FieldDescriptorProto_Type2[FieldDescriptorProto_Type2["BOOL"] = 8] = "BOOL";
  FieldDescriptorProto_Type2[FieldDescriptorProto_Type2["STRING"] = 9] = "STRING";
  FieldDescriptorProto_Type2[FieldDescriptorProto_Type2["GROUP"] = 10] = "GROUP";
  FieldDescriptorProto_Type2[FieldDescriptorProto_Type2["MESSAGE"] = 11] = "MESSAGE";
  FieldDescriptorProto_Type2[FieldDescriptorProto_Type2["BYTES"] = 12] = "BYTES";
  FieldDescriptorProto_Type2[FieldDescriptorProto_Type2["UINT32"] = 13] = "UINT32";
  FieldDescriptorProto_Type2[FieldDescriptorProto_Type2["ENUM"] = 14] = "ENUM";
  FieldDescriptorProto_Type2[FieldDescriptorProto_Type2["SFIXED32"] = 15] = "SFIXED32";
  FieldDescriptorProto_Type2[FieldDescriptorProto_Type2["SFIXED64"] = 16] = "SFIXED64";
  FieldDescriptorProto_Type2[FieldDescriptorProto_Type2["SINT32"] = 17] = "SINT32";
  FieldDescriptorProto_Type2[FieldDescriptorProto_Type2["SINT64"] = 18] = "SINT64";
})(FieldDescriptorProto_Type || (FieldDescriptorProto_Type = {}));
var FieldDescriptorProto_Label;
(function(FieldDescriptorProto_Label2) {
  FieldDescriptorProto_Label2[FieldDescriptorProto_Label2["OPTIONAL"] = 1] = "OPTIONAL";
  FieldDescriptorProto_Label2[FieldDescriptorProto_Label2["REPEATED"] = 3] = "REPEATED";
  FieldDescriptorProto_Label2[FieldDescriptorProto_Label2["REQUIRED"] = 2] = "REQUIRED";
})(FieldDescriptorProto_Label || (FieldDescriptorProto_Label = {}));
var FileOptions_OptimizeMode;
(function(FileOptions_OptimizeMode2) {
  FileOptions_OptimizeMode2[FileOptions_OptimizeMode2["SPEED"] = 1] = "SPEED";
  FileOptions_OptimizeMode2[FileOptions_OptimizeMode2["CODE_SIZE"] = 2] = "CODE_SIZE";
  FileOptions_OptimizeMode2[FileOptions_OptimizeMode2["LITE_RUNTIME"] = 3] = "LITE_RUNTIME";
})(FileOptions_OptimizeMode || (FileOptions_OptimizeMode = {}));
var FieldOptions_CType;
(function(FieldOptions_CType2) {
  FieldOptions_CType2[FieldOptions_CType2["STRING"] = 0] = "STRING";
  FieldOptions_CType2[FieldOptions_CType2["CORD"] = 1] = "CORD";
  FieldOptions_CType2[FieldOptions_CType2["STRING_PIECE"] = 2] = "STRING_PIECE";
})(FieldOptions_CType || (FieldOptions_CType = {}));
var FieldOptions_JSType;
(function(FieldOptions_JSType2) {
  FieldOptions_JSType2[FieldOptions_JSType2["JS_NORMAL"] = 0] = "JS_NORMAL";
  FieldOptions_JSType2[FieldOptions_JSType2["JS_STRING"] = 1] = "JS_STRING";
  FieldOptions_JSType2[FieldOptions_JSType2["JS_NUMBER"] = 2] = "JS_NUMBER";
})(FieldOptions_JSType || (FieldOptions_JSType = {}));
var FieldOptions_OptionRetention;
(function(FieldOptions_OptionRetention2) {
  FieldOptions_OptionRetention2[FieldOptions_OptionRetention2["RETENTION_UNKNOWN"] = 0] = "RETENTION_UNKNOWN";
  FieldOptions_OptionRetention2[FieldOptions_OptionRetention2["RETENTION_RUNTIME"] = 1] = "RETENTION_RUNTIME";
  FieldOptions_OptionRetention2[FieldOptions_OptionRetention2["RETENTION_SOURCE"] = 2] = "RETENTION_SOURCE";
})(FieldOptions_OptionRetention || (FieldOptions_OptionRetention = {}));
var FieldOptions_OptionTargetType;
(function(FieldOptions_OptionTargetType2) {
  FieldOptions_OptionTargetType2[FieldOptions_OptionTargetType2["TARGET_TYPE_UNKNOWN"] = 0] = "TARGET_TYPE_UNKNOWN";
  FieldOptions_OptionTargetType2[FieldOptions_OptionTargetType2["TARGET_TYPE_FILE"] = 1] = "TARGET_TYPE_FILE";
  FieldOptions_OptionTargetType2[FieldOptions_OptionTargetType2["TARGET_TYPE_EXTENSION_RANGE"] = 2] = "TARGET_TYPE_EXTENSION_RANGE";
  FieldOptions_OptionTargetType2[FieldOptions_OptionTargetType2["TARGET_TYPE_MESSAGE"] = 3] = "TARGET_TYPE_MESSAGE";
  FieldOptions_OptionTargetType2[FieldOptions_OptionTargetType2["TARGET_TYPE_FIELD"] = 4] = "TARGET_TYPE_FIELD";
  FieldOptions_OptionTargetType2[FieldOptions_OptionTargetType2["TARGET_TYPE_ONEOF"] = 5] = "TARGET_TYPE_ONEOF";
  FieldOptions_OptionTargetType2[FieldOptions_OptionTargetType2["TARGET_TYPE_ENUM"] = 6] = "TARGET_TYPE_ENUM";
  FieldOptions_OptionTargetType2[FieldOptions_OptionTargetType2["TARGET_TYPE_ENUM_ENTRY"] = 7] = "TARGET_TYPE_ENUM_ENTRY";
  FieldOptions_OptionTargetType2[FieldOptions_OptionTargetType2["TARGET_TYPE_SERVICE"] = 8] = "TARGET_TYPE_SERVICE";
  FieldOptions_OptionTargetType2[FieldOptions_OptionTargetType2["TARGET_TYPE_METHOD"] = 9] = "TARGET_TYPE_METHOD";
})(FieldOptions_OptionTargetType || (FieldOptions_OptionTargetType = {}));
var MethodOptions_IdempotencyLevel;
(function(MethodOptions_IdempotencyLevel2) {
  MethodOptions_IdempotencyLevel2[MethodOptions_IdempotencyLevel2["IDEMPOTENCY_UNKNOWN"] = 0] = "IDEMPOTENCY_UNKNOWN";
  MethodOptions_IdempotencyLevel2[MethodOptions_IdempotencyLevel2["NO_SIDE_EFFECTS"] = 1] = "NO_SIDE_EFFECTS";
  MethodOptions_IdempotencyLevel2[MethodOptions_IdempotencyLevel2["IDEMPOTENT"] = 2] = "IDEMPOTENT";
})(MethodOptions_IdempotencyLevel || (MethodOptions_IdempotencyLevel = {}));
var FeatureSet_VisibilityFeature_DefaultSymbolVisibility;
(function(FeatureSet_VisibilityFeature_DefaultSymbolVisibility2) {
  FeatureSet_VisibilityFeature_DefaultSymbolVisibility2[FeatureSet_VisibilityFeature_DefaultSymbolVisibility2["DEFAULT_SYMBOL_VISIBILITY_UNKNOWN"] = 0] = "DEFAULT_SYMBOL_VISIBILITY_UNKNOWN";
  FeatureSet_VisibilityFeature_DefaultSymbolVisibility2[FeatureSet_VisibilityFeature_DefaultSymbolVisibility2["EXPORT_ALL"] = 1] = "EXPORT_ALL";
  FeatureSet_VisibilityFeature_DefaultSymbolVisibility2[FeatureSet_VisibilityFeature_DefaultSymbolVisibility2["EXPORT_TOP_LEVEL"] = 2] = "EXPORT_TOP_LEVEL";
  FeatureSet_VisibilityFeature_DefaultSymbolVisibility2[FeatureSet_VisibilityFeature_DefaultSymbolVisibility2["LOCAL_ALL"] = 3] = "LOCAL_ALL";
  FeatureSet_VisibilityFeature_DefaultSymbolVisibility2[FeatureSet_VisibilityFeature_DefaultSymbolVisibility2["STRICT"] = 4] = "STRICT";
})(FeatureSet_VisibilityFeature_DefaultSymbolVisibility || (FeatureSet_VisibilityFeature_DefaultSymbolVisibility = {}));
var FeatureSet_FieldPresence;
(function(FeatureSet_FieldPresence2) {
  FeatureSet_FieldPresence2[FeatureSet_FieldPresence2["FIELD_PRESENCE_UNKNOWN"] = 0] = "FIELD_PRESENCE_UNKNOWN";
  FeatureSet_FieldPresence2[FeatureSet_FieldPresence2["EXPLICIT"] = 1] = "EXPLICIT";
  FeatureSet_FieldPresence2[FeatureSet_FieldPresence2["IMPLICIT"] = 2] = "IMPLICIT";
  FeatureSet_FieldPresence2[FeatureSet_FieldPresence2["LEGACY_REQUIRED"] = 3] = "LEGACY_REQUIRED";
})(FeatureSet_FieldPresence || (FeatureSet_FieldPresence = {}));
var FeatureSet_EnumType;
(function(FeatureSet_EnumType2) {
  FeatureSet_EnumType2[FeatureSet_EnumType2["ENUM_TYPE_UNKNOWN"] = 0] = "ENUM_TYPE_UNKNOWN";
  FeatureSet_EnumType2[FeatureSet_EnumType2["OPEN"] = 1] = "OPEN";
  FeatureSet_EnumType2[FeatureSet_EnumType2["CLOSED"] = 2] = "CLOSED";
})(FeatureSet_EnumType || (FeatureSet_EnumType = {}));
var FeatureSet_RepeatedFieldEncoding;
(function(FeatureSet_RepeatedFieldEncoding2) {
  FeatureSet_RepeatedFieldEncoding2[FeatureSet_RepeatedFieldEncoding2["REPEATED_FIELD_ENCODING_UNKNOWN"] = 0] = "REPEATED_FIELD_ENCODING_UNKNOWN";
  FeatureSet_RepeatedFieldEncoding2[FeatureSet_RepeatedFieldEncoding2["PACKED"] = 1] = "PACKED";
  FeatureSet_RepeatedFieldEncoding2[FeatureSet_RepeatedFieldEncoding2["EXPANDED"] = 2] = "EXPANDED";
})(FeatureSet_RepeatedFieldEncoding || (FeatureSet_RepeatedFieldEncoding = {}));
var FeatureSet_Utf8Validation;
(function(FeatureSet_Utf8Validation2) {
  FeatureSet_Utf8Validation2[FeatureSet_Utf8Validation2["UTF8_VALIDATION_UNKNOWN"] = 0] = "UTF8_VALIDATION_UNKNOWN";
  FeatureSet_Utf8Validation2[FeatureSet_Utf8Validation2["VERIFY"] = 2] = "VERIFY";
  FeatureSet_Utf8Validation2[FeatureSet_Utf8Validation2["NONE"] = 3] = "NONE";
})(FeatureSet_Utf8Validation || (FeatureSet_Utf8Validation = {}));
var FeatureSet_MessageEncoding;
(function(FeatureSet_MessageEncoding2) {
  FeatureSet_MessageEncoding2[FeatureSet_MessageEncoding2["MESSAGE_ENCODING_UNKNOWN"] = 0] = "MESSAGE_ENCODING_UNKNOWN";
  FeatureSet_MessageEncoding2[FeatureSet_MessageEncoding2["LENGTH_PREFIXED"] = 1] = "LENGTH_PREFIXED";
  FeatureSet_MessageEncoding2[FeatureSet_MessageEncoding2["DELIMITED"] = 2] = "DELIMITED";
})(FeatureSet_MessageEncoding || (FeatureSet_MessageEncoding = {}));
var FeatureSet_JsonFormat;
(function(FeatureSet_JsonFormat2) {
  FeatureSet_JsonFormat2[FeatureSet_JsonFormat2["JSON_FORMAT_UNKNOWN"] = 0] = "JSON_FORMAT_UNKNOWN";
  FeatureSet_JsonFormat2[FeatureSet_JsonFormat2["ALLOW"] = 1] = "ALLOW";
  FeatureSet_JsonFormat2[FeatureSet_JsonFormat2["LEGACY_BEST_EFFORT"] = 2] = "LEGACY_BEST_EFFORT";
})(FeatureSet_JsonFormat || (FeatureSet_JsonFormat = {}));
var FeatureSet_EnforceNamingStyle;
(function(FeatureSet_EnforceNamingStyle2) {
  FeatureSet_EnforceNamingStyle2[FeatureSet_EnforceNamingStyle2["ENFORCE_NAMING_STYLE_UNKNOWN"] = 0] = "ENFORCE_NAMING_STYLE_UNKNOWN";
  FeatureSet_EnforceNamingStyle2[FeatureSet_EnforceNamingStyle2["STYLE2024"] = 1] = "STYLE2024";
  FeatureSet_EnforceNamingStyle2[FeatureSet_EnforceNamingStyle2["STYLE_LEGACY"] = 2] = "STYLE_LEGACY";
})(FeatureSet_EnforceNamingStyle || (FeatureSet_EnforceNamingStyle = {}));
var GeneratedCodeInfo_Annotation_Semantic;
(function(GeneratedCodeInfo_Annotation_Semantic2) {
  GeneratedCodeInfo_Annotation_Semantic2[GeneratedCodeInfo_Annotation_Semantic2["NONE"] = 0] = "NONE";
  GeneratedCodeInfo_Annotation_Semantic2[GeneratedCodeInfo_Annotation_Semantic2["SET"] = 1] = "SET";
  GeneratedCodeInfo_Annotation_Semantic2[GeneratedCodeInfo_Annotation_Semantic2["ALIAS"] = 2] = "ALIAS";
})(GeneratedCodeInfo_Annotation_Semantic || (GeneratedCodeInfo_Annotation_Semantic = {}));
var Edition;
(function(Edition2) {
  Edition2[Edition2["EDITION_UNKNOWN"] = 0] = "EDITION_UNKNOWN";
  Edition2[Edition2["EDITION_LEGACY"] = 900] = "EDITION_LEGACY";
  Edition2[Edition2["EDITION_PROTO2"] = 998] = "EDITION_PROTO2";
  Edition2[Edition2["EDITION_PROTO3"] = 999] = "EDITION_PROTO3";
  Edition2[Edition2["EDITION_2023"] = 1000] = "EDITION_2023";
  Edition2[Edition2["EDITION_2024"] = 1001] = "EDITION_2024";
  Edition2[Edition2["EDITION_UNSTABLE"] = 9999] = "EDITION_UNSTABLE";
  Edition2[Edition2["EDITION_1_TEST_ONLY"] = 1] = "EDITION_1_TEST_ONLY";
  Edition2[Edition2["EDITION_2_TEST_ONLY"] = 2] = "EDITION_2_TEST_ONLY";
  Edition2[Edition2["EDITION_99997_TEST_ONLY"] = 99997] = "EDITION_99997_TEST_ONLY";
  Edition2[Edition2["EDITION_99998_TEST_ONLY"] = 99998] = "EDITION_99998_TEST_ONLY";
  Edition2[Edition2["EDITION_99999_TEST_ONLY"] = 99999] = "EDITION_99999_TEST_ONLY";
  Edition2[Edition2["EDITION_MAX"] = 2147483647] = "EDITION_MAX";
})(Edition || (Edition = {}));
var SymbolVisibility;
(function(SymbolVisibility2) {
  SymbolVisibility2[SymbolVisibility2["VISIBILITY_UNSET"] = 0] = "VISIBILITY_UNSET";
  SymbolVisibility2[SymbolVisibility2["VISIBILITY_LOCAL"] = 1] = "VISIBILITY_LOCAL";
  SymbolVisibility2[SymbolVisibility2["VISIBILITY_EXPORT"] = 2] = "VISIBILITY_EXPORT";
})(SymbolVisibility || (SymbolVisibility = {}));

// node_modules/@bufbuild/protobuf/dist/esm/from-binary.js
function makeReadContext(options) {
  return Object.assign(Object.assign({ readUnknownFields: true, recursionLimit: 100 }, options), { depth: 0 });
}
function fromBinary(schema, bytes, options) {
  const message = create(schema);
  compiledReader(schema).read(message, new BinaryReader(bytes), makeReadContext(options), bytes.byteLength);
  return message;
}
var compiledReaders = new WeakMap;
function compiledReader(desc) {
  let compiled = compiledReaders.get(desc);
  if (compiled === undefined) {
    compiled = compileMessage(desc);
  }
  return compiled;
}
function compileMessage(desc) {
  const descString = String(desc);
  const fieldReaders = new Map;
  const compiled = {
    read: compileMessageReader(descString, fieldReaders),
    readGroup: compileGroupReader(descString, fieldReaders)
  };
  compiledReaders.set(desc, compiled);
  for (const field of desc.fields) {
    fieldReaders.set(field.number, compileFieldReader(field));
  }
  return compiled;
}
function compileMessageReader(descString, fieldReaders) {
  return (message, reader, ctx, length) => {
    var _a;
    if (++ctx.depth > ctx.recursionLimit) {
      throw new Error(`cannot decode ${descString} from binary: maximum recursion depth of ${ctx.recursionLimit} reached`);
    }
    const end = reader.pos + length;
    const unknownFields = (_a = message.$unknown) !== null && _a !== undefined ? _a : [];
    while (reader.pos < end) {
      const [fieldNo, wireType] = reader.tag();
      const fieldReader = fieldReaders.get(fieldNo);
      if (fieldReader === undefined) {
        const data = reader.skip(wireType, fieldNo, ctx.recursionLimit - ctx.depth);
        if (ctx.readUnknownFields) {
          unknownFields.push({ no: fieldNo, wireType, data });
        }
        continue;
      }
      fieldReader(message, reader, ctx, wireType);
    }
    if (unknownFields.length > 0) {
      message.$unknown = unknownFields;
    }
    ctx.depth--;
  };
}
function compileGroupReader(descString, fieldReaders) {
  return (message, reader, ctx, fieldNo) => {
    var _a;
    if (++ctx.depth > ctx.recursionLimit) {
      throw new Error(`cannot decode ${descString} from binary: maximum recursion depth of ${ctx.recursionLimit} reached`);
    }
    let recordFieldNo;
    let wireType;
    const unknownFields = (_a = message.$unknown) !== null && _a !== undefined ? _a : [];
    while (reader.pos < reader.len) {
      [recordFieldNo, wireType] = reader.tag();
      if (wireType == WireType.EndGroup) {
        break;
      }
      const fieldReader = fieldReaders.get(recordFieldNo);
      if (fieldReader === undefined) {
        const data = reader.skip(wireType, recordFieldNo, ctx.recursionLimit - ctx.depth);
        if (ctx.readUnknownFields) {
          unknownFields.push({ no: recordFieldNo, wireType, data });
        }
        continue;
      }
      fieldReader(message, reader, ctx, wireType);
    }
    if (wireType != WireType.EndGroup || recordFieldNo !== fieldNo) {
      throw new Error("invalid end group tag");
    }
    if (unknownFields.length > 0) {
      message.$unknown = unknownFields;
    }
    ctx.depth--;
  };
}
function compileFieldReader(field) {
  switch (field.fieldKind) {
    case "scalar":
      return compileScalarFieldReader(field);
    case "enum":
      return compileEnumFieldReader(field);
    case "message":
      return compileMessageFieldReader(field);
    case "list":
      return compileListFieldReader(field);
    case "map":
      return compileMapFieldReader(field);
  }
}
function compileScalarFieldReader(field) {
  const readScalar = compileScalarReader(field.scalar, field.utf8Validation, field.longAsString);
  const localName = field.localName;
  if (field.oneof) {
    const oneofLocalName = field.oneof.localName;
    return (message, reader) => {
      message[oneofLocalName] = {
        case: localName,
        value: readScalar(reader)
      };
    };
  }
  return (message, reader) => {
    message[localName] = readScalar(reader);
  };
}
function compileEnumFieldReader(field) {
  var _a;
  const localName = field.localName;
  const oneofLocalName = (_a = field.oneof) === null || _a === undefined ? undefined : _a.localName;
  if (field.enum.open) {
    if (oneofLocalName !== undefined) {
      return (message, reader) => {
        message[oneofLocalName] = { case: localName, value: reader.int32() };
      };
    }
    return (message, reader) => {
      message[localName] = reader.int32();
    };
  }
  const values = field.enum.values;
  const fieldNo = field.number;
  return (message, reader, ctx, wireType) => {
    var _a2;
    const val = reader.int32();
    if (values.some((v) => v.number === val)) {
      if (oneofLocalName !== undefined) {
        message[oneofLocalName] = { case: localName, value: val };
      } else {
        message[localName] = val;
      }
    } else if (ctx.readUnknownFields) {
      const bytes = [];
      varint32write(val, bytes);
      const unknownFields = (_a2 = message.$unknown) !== null && _a2 !== undefined ? _a2 : [];
      unknownFields.push({
        no: fieldNo,
        wireType,
        data: new Uint8Array(bytes)
      });
      message.$unknown = unknownFields;
    }
  };
}
function compileMessageFieldReader(field) {
  const localName = field.localName;
  const { toMessage, toLocal } = localMessageMapper(field);
  const readChild = compileChildReader(field);
  if (field.oneof) {
    const oneofLocalName = field.oneof.localName;
    return (message, reader, ctx) => {
      const oneof = message[oneofLocalName];
      const child = toMessage(oneof.case === localName ? oneof.value : undefined);
      readChild(child, reader, ctx);
      message[oneofLocalName] = { case: localName, value: toLocal(child) };
    };
  }
  return (message, reader, ctx) => {
    const child = toMessage(message[localName]);
    readChild(child, reader, ctx);
    message[localName] = toLocal(child);
  };
}
function compileChildReader(field) {
  const compiledChild = compiledReader(field.message);
  if (field.delimitedEncoding) {
    const fieldNo = field.number;
    return (child, reader, ctx) => compiledChild.readGroup(child, reader, ctx, fieldNo);
  }
  return (child, reader, ctx) => compiledChild.read(child, reader, ctx, reader.uint32());
}
function compileListFieldReader(field) {
  const localName = field.localName;
  if (field.listKind == "message") {
    const { toMessage, toLocal } = localMessageMapper(field);
    const readChild = compileChildReader(field);
    return (message, reader, ctx) => {
      const child = toMessage(undefined);
      readChild(child, reader, ctx);
      message[localName].push(toLocal(child));
    };
  }
  const scalarType = field.listKind == "enum" ? ScalarType.INT32 : field.scalar;
  const longAsString = field.listKind == "scalar" ? field.longAsString : false;
  const readScalar = compileScalarReader(scalarType, field.utf8Validation, longAsString);
  const packedPossible = scalarType != ScalarType.STRING && scalarType != ScalarType.BYTES;
  return (message, reader, ctx, wireType) => {
    const items = message[localName];
    if (wireType == WireType.LengthDelimited && packedPossible) {
      const end = reader.uint32() + reader.pos;
      while (reader.pos < end) {
        items.push(readScalar(reader));
      }
    } else {
      items.push(readScalar(reader));
    }
  };
}
function compileMapFieldReader(field) {
  const localName = field.localName;
  const readKey = compileScalarReader(field.mapKey, field.utf8Validation, false);
  const keyZero = scalarZeroValue(field.mapKey, false);
  let readValue;
  let valueDefault;
  switch (field.mapKind) {
    case "scalar": {
      const scalar = field.scalar;
      const readScalar = compileScalarReader(scalar, field.utf8Validation, false);
      readValue = (reader) => readScalar(reader);
      if (scalar == ScalarType.BYTES) {
        valueDefault = () => new Uint8Array(0);
      } else {
        const zero = scalarZeroValue(scalar, false);
        valueDefault = () => zero;
      }
      break;
    }
    case "enum": {
      const zero = field.enum.values[0].number;
      readValue = (reader) => reader.int32();
      valueDefault = () => zero;
      break;
    }
    case "message": {
      const { toMessage, toLocal } = localMessageMapper(field);
      const readChild = compiledReader(field.message).read;
      readValue = (reader, ctx) => {
        const child = toMessage(undefined);
        readChild(child, reader, ctx, reader.uint32());
        return toLocal(child);
      };
      valueDefault = () => toLocal(toMessage(undefined));
      break;
    }
  }
  return (message, reader, ctx) => {
    const record = message[localName];
    let key;
    let val;
    const len = reader.uint32();
    const end = reader.pos + len;
    while (reader.pos < end) {
      const [fieldNo] = reader.tag();
      switch (fieldNo) {
        case 1:
          key = readKey(reader);
          break;
        case 2:
          val = readValue(reader, ctx);
          break;
      }
    }
    if (key === undefined) {
      key = keyZero;
    }
    if (val === undefined) {
      val = valueDefault();
    }
    record[key] = val;
  };
}
function compileScalarReader(type, utf8Validation, longAsString) {
  switch (type) {
    case ScalarType.STRING:
      return (reader) => reader.string(utf8Validation);
    case ScalarType.BOOL:
      return (reader) => reader.bool();
    case ScalarType.DOUBLE:
      return (reader) => reader.double();
    case ScalarType.FLOAT:
      return (reader) => reader.float();
    case ScalarType.INT32:
      return (reader) => reader.int32();
    case ScalarType.INT64:
      if (longAsString) {
        return (reader) => String(reader.int64());
      }
      return (reader) => reader.int64();
    case ScalarType.UINT64:
      if (longAsString) {
        return (reader) => String(reader.uint64());
      }
      return (reader) => reader.uint64();
    case ScalarType.FIXED64:
      if (longAsString) {
        return (reader) => String(reader.fixed64());
      }
      return (reader) => reader.fixed64();
    case ScalarType.BYTES:
      return (reader) => reader.bytes();
    case ScalarType.FIXED32:
      return (reader) => reader.fixed32();
    case ScalarType.SFIXED32:
      return (reader) => reader.sfixed32();
    case ScalarType.SFIXED64:
      if (longAsString) {
        return (reader) => String(reader.sfixed64());
      }
      return (reader) => reader.sfixed64();
    case ScalarType.SINT64:
      if (longAsString) {
        return (reader) => String(reader.sint64());
      }
      return (reader) => reader.sint64();
    case ScalarType.UINT32:
      return (reader) => reader.uint32();
    case ScalarType.SINT32:
      return (reader) => reader.sint32();
  }
}

// node_modules/@bufbuild/protobuf/dist/esm/codegenv2/file.js
function fileDesc(b64, imports) {
  var _a;
  const root = fromBinary(FileDescriptorProtoSchema, base64Decode(b64));
  root.messageType.forEach(restoreJsonNames);
  root.dependency = (_a = imports === null || imports === undefined ? undefined : imports.map((f) => f.proto.name)) !== null && _a !== undefined ? _a : [];
  const reg = createFileRegistry(root, (protoFileName) => imports === null || imports === undefined ? undefined : imports.find((f) => f.proto.name === protoFileName));
  return reg.getFile(root.name);
}

// node_modules/@bufbuild/protobuf/dist/esm/to-binary.js
var IMPLICIT3 = 2;
var LEGACY_REQUIRED2 = 3;
var writeDefaults = {
  writeUnknownFields: true
};
function makeWriteOptions(options) {
  return options ? Object.assign(Object.assign({}, writeDefaults), options) : writeDefaults;
}
function toBinary(schema, message, options) {
  const writer = new BinaryWriter;
  compiledWriter(schema)(writer, makeWriteOptions(options), message);
  return writer.finish();
}
var compiledWriters = new WeakMap;
function compiledWriter(desc) {
  let compiled = compiledWriters.get(desc);
  if (compiled === undefined) {
    compiled = compileMessage2(desc);
  }
  return compiled;
}
function compileMessage2(desc) {
  const typeName = desc.typeName;
  const sortedFields = desc.fields.concat().sort((a, b) => a.number - b.number);
  const foreignField = sortedFields[0];
  const fieldWriters = [];
  const compiled = (writer, opts, message) => {
    if (message.$typeName !== typeName && foreignField !== undefined) {
      throw new FieldError(foreignField, `cannot use ${foreignField} with message ${message.$typeName}`, "ForeignFieldError");
    }
    for (let i = 0;i < fieldWriters.length; i++) {
      fieldWriters[i](writer, opts, message);
    }
    const unknown = message.$unknown;
    if (unknown !== undefined && opts.writeUnknownFields) {
      for (let i = 0;i < unknown.length; i++) {
        const { no, wireType, data } = unknown[i];
        writer.tag(no, wireType).raw(data);
      }
    }
  };
  compiledWriters.set(desc, compiled);
  for (const field of sortedFields) {
    fieldWriters.push(compileField(field));
  }
  return compiled;
}
function compileField(field) {
  switch (field.fieldKind) {
    case "message":
    case "scalar":
    case "enum":
      return compileSingularField(field);
    case "list":
      return compileListField(field);
    case "map":
      return compileMapField(field);
  }
}
function compileSingularField(field) {
  const writeValue = compileSingularValue(field);
  const localName = field.localName;
  if (field.oneof) {
    const oneofLocalName = field.oneof.localName;
    return (writer, opts, message) => {
      const oneof = message[oneofLocalName];
      if (oneof.case === localName) {
        writeValue(writer, opts, oneof.value);
      }
    };
  }
  if (field.presence != IMPLICIT3) {
    const requiredError = field.presence == LEGACY_REQUIRED2 ? `cannot encode ${field} to binary: required field not set` : undefined;
    return (writer, opts, message) => {
      const value = message[localName];
      if (value !== undefined && Object.prototype.hasOwnProperty.call(message, localName)) {
        writeValue(writer, opts, value);
      } else if (requiredError !== undefined) {
        throw new Error(requiredError);
      }
    };
  }
  if (field.fieldKind == "enum") {
    const zero = field.enum.values[0].number;
    return (writer, opts, message) => {
      const value = message[localName];
      if (value !== zero) {
        writeValue(writer, opts, value);
      }
    };
  }
  switch (field.scalar) {
    case ScalarType.BOOL:
      return (writer, opts, message) => {
        const value = message[localName];
        if (value !== false) {
          writeValue(writer, opts, value);
        }
      };
    case ScalarType.STRING:
      return (writer, opts, message) => {
        const value = message[localName];
        if (value !== "") {
          writeValue(writer, opts, value);
        }
      };
    case ScalarType.BYTES:
      return (writer, opts, message) => {
        const value = message[localName];
        if (!(value instanceof Uint8Array) || value.byteLength > 0) {
          writeValue(writer, opts, value);
        }
      };
    case ScalarType.DOUBLE:
    case ScalarType.FLOAT:
      return (writer, opts, message) => {
        const value = message[localName];
        if (!Object.is(value, 0)) {
          writeValue(writer, opts, value);
        }
      };
    default:
      return (writer, opts, message) => {
        const value = message[localName];
        if (value != 0) {
          writeValue(writer, opts, value);
        }
      };
  }
}
function compileSingularValue(field) {
  switch (field.fieldKind) {
    case "message": {
      const { toMessage } = localMessageMapper(field);
      const writeChild = compileChildWriter(field);
      return (writer, opts, value) => {
        writeChild(writer, opts, toMessage(value));
      };
    }
    case "scalar":
    case "enum": {
      const scalarType = field.fieldKind == "enum" ? ScalarType.INT32 : field.scalar;
      const fieldNo = field.number;
      const wireType = writeTypeOfScalar(scalarType);
      const writeScalar = compileScalarValue(scalarType, field.parent.typeName, field.name);
      return (writer, opts, value) => {
        writer.tag(fieldNo, wireType);
        writeScalar(writer, value);
      };
    }
  }
}
function compileListField(field) {
  const localName = field.localName;
  const fieldNo = field.number;
  switch (field.listKind) {
    case "message": {
      const { toMessage } = localMessageMapper(field);
      const writeChild = compileChildWriter(field);
      return (writer, opts, message) => {
        const items = message[localName];
        for (let i = 0;i < items.length; i++) {
          writeChild(writer, opts, toMessage(items[i]));
        }
      };
    }
    case "scalar":
    case "enum": {
      const scalarType = field.listKind == "enum" ? ScalarType.INT32 : field.scalar;
      const writeScalar = compileScalarValue(scalarType, field.parent.typeName, field.name);
      if (field.packed) {
        return (writer, opts, message) => {
          const items = message[localName];
          if (items.length == 0) {
            return;
          }
          writer.tag(fieldNo, WireType.LengthDelimited).fork();
          for (let i = 0;i < items.length; i++) {
            writeScalar(writer, items[i]);
          }
          writer.join();
        };
      }
      const wireType = writeTypeOfScalar(scalarType);
      return (writer, opts, message) => {
        const items = message[localName];
        for (let i = 0;i < items.length; i++) {
          writer.tag(fieldNo, wireType);
          writeScalar(writer, items[i]);
        }
      };
    }
  }
}
function compileMapField(field) {
  const localName = field.localName;
  const fieldNo = field.number;
  const writeKey = compileMapKey(field);
  if (field.mapKind == "message") {
    const { toMessage } = localMessageMapper(field);
    const writeMessage = compiledWriter(field.message);
    return (writer, opts, message) => {
      const record = message[localName];
      const keys = Object.keys(record);
      for (let i = 0;i < keys.length; i++) {
        const key = keys[i];
        writer.tag(fieldNo, WireType.LengthDelimited).fork();
        writeKey(writer, key);
        writer.tag(2, WireType.LengthDelimited).fork();
        writeMessage(writer, opts, toMessage(record[key]));
        writer.join();
        writer.join();
      }
    };
  }
  const scalarType = field.mapKind == "enum" ? ScalarType.INT32 : field.scalar;
  const valueWireType = writeTypeOfScalar(scalarType);
  const writeScalar = compileScalarValue(scalarType, field.parent.typeName, field.name);
  return (writer, opts, message) => {
    const record = message[localName];
    const keys = Object.keys(record);
    for (let i = 0;i < keys.length; i++) {
      const key = keys[i];
      writer.tag(fieldNo, WireType.LengthDelimited).fork();
      writeKey(writer, key);
      writer.tag(2, valueWireType);
      writeScalar(writer, record[key]);
      writer.join();
    }
  };
}
function compileMapKey(field) {
  const wireType = writeTypeOfScalar(field.mapKey);
  const writeScalar = compileScalarValue(field.mapKey, field.parent.typeName, field.name);
  const convertKey = compileMapKeyConverter(field.mapKey);
  return (writer, key) => {
    writer.tag(1, wireType);
    writeScalar(writer, convertKey(key));
  };
}
function compileMapKeyConverter(type) {
  switch (type) {
    case ScalarType.STRING:
      return (key) => key;
    case ScalarType.BOOL:
      return (key) => key === "true" ? true : key === "false" ? false : key;
    case ScalarType.UINT64:
    case ScalarType.FIXED64:
      return (key) => {
        try {
          return protoInt64.uParse(key);
        } catch (_a) {
          return key;
        }
      };
    case ScalarType.INT64:
    case ScalarType.SFIXED64:
    case ScalarType.SINT64:
      return (key) => {
        try {
          return protoInt64.parse(key);
        } catch (_a) {
          return key;
        }
      };
    default:
      return (key) => {
        const n = Number.parseInt(key);
        return Number.isFinite(n) ? n : key;
      };
  }
}
function compileScalarValue(type, messageName, fieldName) {
  const writeScalar = compileScalarWrite(type);
  return (writer, value) => {
    try {
      writeScalar(writer, value);
    } catch (e) {
      if (e instanceof Error) {
        throw new Error(`cannot encode field ${messageName}.${fieldName} to binary: ${e.message}`);
      }
      throw e;
    }
  };
}
function compileScalarWrite(type) {
  switch (type) {
    case ScalarType.STRING:
      return (writer, value) => writer.string(value);
    case ScalarType.BOOL:
      return (writer, value) => writer.bool(value);
    case ScalarType.DOUBLE:
      return (writer, value) => writer.double(value);
    case ScalarType.FLOAT:
      return (writer, value) => writer.float(value);
    case ScalarType.INT32:
      return (writer, value) => writer.int32(value);
    case ScalarType.INT64:
      return (writer, value) => writer.int64(value);
    case ScalarType.UINT64:
      return (writer, value) => writer.uint64(value);
    case ScalarType.FIXED64:
      return (writer, value) => writer.fixed64(value);
    case ScalarType.BYTES:
      return (writer, value) => writer.bytes(value);
    case ScalarType.FIXED32:
      return (writer, value) => writer.fixed32(value);
    case ScalarType.SFIXED32:
      return (writer, value) => writer.sfixed32(value);
    case ScalarType.SFIXED64:
      return (writer, value) => writer.sfixed64(value);
    case ScalarType.SINT64:
      return (writer, value) => writer.sint64(value);
    case ScalarType.UINT32:
      return (writer, value) => writer.uint32(value);
    case ScalarType.SINT32:
      return (writer, value) => writer.sint32(value);
  }
}
function compileChildWriter(field) {
  const fieldNo = field.number;
  const writeMessage = compiledWriter(field.message);
  if (field.delimitedEncoding) {
    return (writer, opts, child) => {
      writer.tag(fieldNo, WireType.StartGroup);
      writeMessage(writer, opts, child);
      writer.tag(fieldNo, WireType.EndGroup);
    };
  }
  return (writer, opts, child) => {
    writer.tag(fieldNo, WireType.LengthDelimited).fork();
    writeMessage(writer, opts, child);
    writer.join();
  };
}
function writeTypeOfScalar(type) {
  switch (type) {
    case ScalarType.BYTES:
    case ScalarType.STRING:
      return WireType.LengthDelimited;
    case ScalarType.DOUBLE:
    case ScalarType.FIXED64:
    case ScalarType.SFIXED64:
      return WireType.Bit64;
    case ScalarType.FIXED32:
    case ScalarType.SFIXED32:
    case ScalarType.FLOAT:
      return WireType.Bit32;
    default:
      return WireType.Varint;
  }
}
// ../../gen/ts/ultima/v1/command_pb.ts
var file_proto_ultima_v1_command = /* @__PURE__ */ fileDesc("Ch1wcm90by91bHRpbWEvdjEvY29tbWFuZC5wcm90bxIJdWx0aW1hLnYxItICCgVWYWx1ZRIOCgRudWxsGAEgASgISAASFwoNc2ltcGxlX3N0cmluZxgCIAEoCUgAEg8KBWVycm9yGAMgASgJSAASDQoDaW50GAQgASgDSAASEAoGZG91YmxlGAUgASgBSAASDgoEYm9vbBgGIAEoCEgAEhUKC2Jsb2Jfc3RyaW5nGAcgASgMSAASFAoKYmlnX251bWJlchgIIAEoCUgAEicKCHZlcmJhdGltGAkgASgLMhMudWx0aW1hLnYxLlZlcmJhdGltSAASIQoFYXJyYXkYCiABKAsyEC51bHRpbWEudjEuQXJyYXlIABIdCgNtYXAYCyABKAsyDi51bHRpbWEudjEuTWFwSAASHQoDc2V0GAwgASgLMg4udWx0aW1hLnYxLlNldEgAEh8KBHB1c2gYDSABKAsyDy51bHRpbWEudjEuUHVzaEgAQgYKBGtpbmQiKwoIVmVyYmF0aW0SDgoGZm9ybWF0GAEgASgJEg8KB3BheWxvYWQYAiABKAwiKAoFQXJyYXkSHwoFZWxlbXMYASADKAsyEC51bHRpbWEudjEuVmFsdWUiRgoEUGFpchIdCgNrZXkYASABKAsyEC51bHRpbWEudjEuVmFsdWUSHwoFdmFsdWUYAiABKAsyEC51bHRpbWEudjEuVmFsdWUiJQoDTWFwEh4KBXBhaXJzGAEgAygLMg8udWx0aW1hLnYxLlBhaXIiJgoDU2V0Eh8KBWVsZW1zGAEgAygLMhAudWx0aW1hLnYxLlZhbHVlIicKBFB1c2gSHwoFZWxlbXMYASADKAsyEC51bHRpbWEudjEuVmFsdWUixAsKB0NvbW1hbmQSJAoDZ2V0GAEgASgLMhUudWx0aW1hLnYxLkdldENvbW1hbmRIABIkCgNzZXQYAiABKAsyFS51bHRpbWEudjEuU2V0Q29tbWFuZEgAEiQKA2RlbBgDIAEoCzIVLnVsdGltYS52MS5EZWxDb21tYW5kSAASJgoEaW5jchgEIAEoCzIWLnVsdGltYS52MS5JbmNyQ29tbWFuZEgAEjEKCmluY3JfZmxvYXQYBSABKAsyGy51bHRpbWEudjEuSW5jckZsb2F0Q29tbWFuZEgAEiYKBG1nZXQYBiABKAsyFi51bHRpbWEudjEuTUdldENvbW1hbmRIABImCgRtc2V0GAcgASgLMhYudWx0aW1hLnYxLk1TZXRDb21tYW5kSAASKgoGYXBwZW5kGAggASgLMhgudWx0aW1hLnYxLkFwcGVuZENvbW1hbmRIABIqCgZleGlzdHMYCSABKAsyGC51bHRpbWEudjEuRXhpc3RzQ29tbWFuZEgAEioKBmV4cGlyZRgKIAEoCzIYLnVsdGltYS52MS5FeHBpcmVDb21tYW5kSAASJAoDdHRsGAsgASgLMhUudWx0aW1hLnYxLlR0bENvbW1hbmRIABIsCgdwZXJzaXN0GAwgASgLMhkudWx0aW1hLnYxLlBlcnNpc3RDb21tYW5kSAASJgoEaGdldBgNIAEoCzIWLnVsdGltYS52MS5IR2V0Q29tbWFuZEgAEiYKBGhzZXQYDiABKAsyFi51bHRpbWEudjEuSFNldENvbW1hbmRIABIsCgdoZ2V0YWxsGA8gASgLMhkudWx0aW1hLnYxLkhHZXRBbGxDb21tYW5kSAASJgoEaGRlbBgQIAEoCzIWLnVsdGltYS52MS5IRGVsQ29tbWFuZEgAEiwKB2hpbmNyYnkYESABKAsyGS51bHRpbWEudjEuSEluY3JCeUNvbW1hbmRIABIoCgVscHVzaBgSIAEoCzIXLnVsdGltYS52MS5MUHVzaENvbW1hbmRIABIoCgVycHVzaBgTIAEoCzIXLnVsdGltYS52MS5SUHVzaENvbW1hbmRIABImCgRscG9wGBQgASgLMhYudWx0aW1hLnYxLkxQb3BDb21tYW5kSAASJgoEcnBvcBgVIAEoCzIWLnVsdGltYS52MS5SUG9wQ29tbWFuZEgAEioKBmxyYW5nZRgWIAEoCzIYLnVsdGltYS52MS5MUmFuZ2VDb21tYW5kSAASJgoEbGxlbhgXIAEoCzIWLnVsdGltYS52MS5MTGVuQ29tbWFuZEgAEiYKBHNhZGQYGCABKAsyFi51bHRpbWEudjEuU0FkZENvbW1hbmRIABImCgRzcmVtGBkgASgLMhYudWx0aW1hLnYxLlNSZW1Db21tYW5kSAASLgoIc21lbWJlcnMYGiABKAsyGi51bHRpbWEudjEuU01lbWJlcnNDb21tYW5kSAASMAoJc2lzbWVtYmVyGBsgASgLMhsudWx0aW1hLnYxLlNJc01lbWJlckNvbW1hbmRIABImCgR6YWRkGBwgASgLMhYudWx0aW1hLnYxLlpBZGRDb21tYW5kSAASKgoGenNjb3JlGB0gASgLMhgudWx0aW1hLnYxLlpTY29yZUNvbW1hbmRIABIqCgZ6cmFuZ2UYHiABKAsyGC51bHRpbWEudjEuWlJhbmdlQ29tbWFuZEgAEiYKBHpyZW0YHyABKAsyFi51bHRpbWEudjEuWlJlbUNvbW1hbmRIABIoCgV6Y2FyZBggIAEoCzIXLnVsdGltYS52MS5aQ2FyZENvbW1hbmRIABIsCgdnZW5lcmljGGQgASgLMhkudWx0aW1hLnYxLkNvbW1hbmRSZXF1ZXN0SAASCgoCZGIYZSABKAQSCwoDc2VxGGYgASgEEg8KB3Nlc3Npb24YZyABKAkSFQoNbGFzdF9wdXNoX3NlcRhoIAEoBEIFCgNjbWQiLwoOQ29tbWFuZFJlcXVlc3QSDwoHY29tbWFuZBgBIAEoCRIMCgRhcmdzGAIgAygMImIKD0NvbW1hbmRSZXNwb25zZRILCgNzZXEYASABKAQSHwoFcmVwbHkYAiABKAsyEC51bHRpbWEudjEuVmFsdWUSDwoHc2Vzc2lvbhgDIAEoCRIQCghwdXNoX3NlcRgEIAEoBCI0CgxCYXRjaFJlcXVlc3QSJAoIY29tbWFuZHMYASADKAsyEi51bHRpbWEudjEuQ29tbWFuZCI+Cg1CYXRjaFJlc3BvbnNlEi0KCXJlc3BvbnNlcxgBIAMoCzIaLnVsdGltYS52MS5Db21tYW5kUmVzcG9uc2UiGQoKR2V0Q29tbWFuZBILCgNrZXkYASABKAwiXQoKU2V0Q29tbWFuZBILCgNrZXkYASABKAwSDQoFdmFsdWUYAiABKAwSDgoGdHRsX21zGAMgASgDEgoKAm54GAQgASgIEgoKAnh4GAUgASgIEgsKA2dldBgGIAEoCCIaCgpEZWxDb21tYW5kEgwKBGtleXMYASADKAwiKQoLSW5jckNvbW1hbmQSCwoDa2V5GAEgASgMEg0KBWRlbHRhGAIgASgDIi4KEEluY3JGbG9hdENvbW1hbmQSCwoDa2V5GAEgASgMEg0KBWRlbHRhGAIgASgBIhsKC01HZXRDb21tYW5kEgwKBGtleXMYASADKAwiJgoITVNldFBhaXISCwoDa2V5GAEgASgMEg0KBXZhbHVlGAIgASgMIjEKC01TZXRDb21tYW5kEiIKBXBhaXJzGAEgAygLMhMudWx0aW1hLnYxLk1TZXRQYWlyIisKDUFwcGVuZENvbW1hbmQSCwoDa2V5GAEgASgMEg0KBXZhbHVlGAIgASgMIh0KDUV4aXN0c0NvbW1hbmQSDAoEa2V5cxgBIAMoDCIsCg1FeHBpcmVDb21tYW5kEgsKA2tleRgBIAEoDBIOCgZ0dGxfbXMYAiABKAMiGQoKVHRsQ29tbWFuZBILCgNrZXkYASABKAwiHQoOUGVyc2lzdENvbW1hbmQSCwoDa2V5GAEgASgMIikKC0hHZXRDb21tYW5kEgsKA2tleRgBIAEoDBINCgVmaWVsZBgCIAEoDCIqCgpGaWVsZFZhbHVlEg0KBWZpZWxkGAEgASgMEg0KBXZhbHVlGAIgASgMIkAKC0hTZXRDb21tYW5kEgsKA2tleRgBIAEoDBIkCgVwYWlycxgCIAMoCzIVLnVsdGltYS52MS5GaWVsZFZhbHVlIh0KDkhHZXRBbGxDb21tYW5kEgsKA2tleRgBIAEoDCIqCgtIRGVsQ29tbWFuZBILCgNrZXkYASABKAwSDgoGZmllbGRzGAIgAygMIjsKDkhJbmNyQnlDb21tYW5kEgsKA2tleRgBIAEoDBINCgVmaWVsZBgCIAEoDBINCgVkZWx0YRgDIAEoAyIqCgxMUHVzaENvbW1hbmQSCwoDa2V5GAEgASgMEg0KBWVsZW1zGAIgAygMIioKDFJQdXNoQ29tbWFuZBILCgNrZXkYASABKAwSDQoFZWxlbXMYAiADKAwiGgoLTFBvcENvbW1hbmQSCwoDa2V5GAEgASgMIhoKC1JQb3BDb21tYW5kEgsKA2tleRgBIAEoDCI5Cg1MUmFuZ2VDb21tYW5kEgsKA2tleRgBIAEoDBINCgVzdGFydBgCIAEoAxIMCgRzdG9wGAMgASgDIhoKC0xMZW5Db21tYW5kEgsKA2tleRgBIAEoDCIrCgtTQWRkQ29tbWFuZBILCgNrZXkYASABKAwSDwoHbWVtYmVycxgCIAMoDCIrCgtTUmVtQ29tbWFuZBILCgNrZXkYASABKAwSDwoHbWVtYmVycxgCIAMoDCIeCg9TTWVtYmVyc0NvbW1hbmQSCwoDa2V5GAEgASgMIi8KEFNJc01lbWJlckNvbW1hbmQSCwoDa2V5GAEgASgMEg4KBm1lbWJlchgCIAEoDCItCgxTY29yZWRNZW1iZXISDQoFc2NvcmUYASABKAESDgoGbWVtYmVyGAIgASgMIo4BCgtaQWRkQ29tbWFuZBILCgNrZXkYASABKAwSKAoHbWVtYmVycxgCIAMoCzIXLnVsdGltYS52MS5TY29yZWRNZW1iZXISCgoCbngYAyABKAgSCgoCeHgYBCABKAgSCgoCZ3QYBSABKAgSCgoCbHQYBiABKAgSCgoCY2gYByABKAgSDAoEaW5jchgIIAEoCCIsCg1aU2NvcmVDb21tYW5kEgsKA2tleRgBIAEoDBIOCgZtZW1iZXIYAiABKAwiTQoNWlJhbmdlQ29tbWFuZBILCgNrZXkYASABKAwSDQoFc3RhcnQYAiABKAMSDAoEc3RvcBgDIAEoAxISCgp3aXRoc2NvcmVzGAQgASgIIisKC1pSZW1Db21tYW5kEgsKA2tleRgBIAEoDBIPCgdtZW1iZXJzGAIgAygMIhsKDFpDYXJkQ29tbWFuZBILCgNrZXkYASABKAwiNgoQU3Vic2NyaWJlUmVxdWVzdBIQCghjaGFubmVscxgBIAMoCRIQCghwYXR0ZXJucxgCIAMoCSJdCglQdXNoRXZlbnQSDwoHY2hhbm5lbBgBIAEoCRIPCgdwYXR0ZXJuGAIgASgJEg8KB3BheWxvYWQYAyABKAwSDQoFY291bnQYBCABKAMSDgoGaXNfYWNrGAUgASgIIhAKDk1vbml0b3JSZXF1ZXN0Il8KDENvbW1hbmRFdmVudBIPCgd1bml4X21zGAEgASgDEgoKAmRiGAIgASgEEhMKC2NsaWVudF9hZGRyGAMgASgJEg8KB2NvbW1hbmQYBCABKAkSDAoEYXJncxgFIAMoDEI2WjRnaXRodWIuY29tL3BzY2hsdW1wL3VsdGltYS9nZW4vZ28vdWx0aW1hL3YxO3VsdGltYXYxYgZwcm90bzM");
var CommandSchema = /* @__PURE__ */ messageDesc(file_proto_ultima_v1_command, 7);
var CommandResponseSchema = /* @__PURE__ */ messageDesc(file_proto_ultima_v1_command, 9);

// ../typescript/src/ws.ts
var enc = new TextEncoder;
var dec2 = new TextDecoder;
function b(v) {
  return typeof v === "string" ? enc.encode(v) : v;
}
function pairsOf(p) {
  if (p instanceof Map)
    return [...p.entries()].map(([k, v]) => [k, v]);
  if (Array.isArray(p))
    return p.map(([k, v]) => [k, v]);
  if (typeof p === "object" && p !== null && Symbol.iterator in p) {
    return [...p].map(([k, v]) => [k, v]);
  }
  return Object.entries(p);
}
function subKey(pattern, name) {
  return (pattern ? "p:" : "c:") + name;
}

class UltimaWS {
  options;
  ws = null;
  seq = 0n;
  pending = new Map;
  outbox = [];
  sessionId = null;
  lastPushSeq = 0n;
  wantClose = false;
  reconnectAttempt = 0;
  reconnectTimer = null;
  handshaken = false;
  handshakePending = 0;
  connectedOnce = false;
  dialing = false;
  subs = new Map;
  stateListeners = new Set;
  storage;
  storageKey;
  useSession;
  WSImpl;
  constructor(options) {
    this.options = options;
    this.useSession = options.session !== false;
    this.storage = options.storage ?? defaultStorage();
    this.storageKey = options.storageKey ?? "ultima.wsSession";
    this.WSImpl = options.WebSocketImpl ?? WebSocket;
    const seed = this.useSession ? options.resume ?? this.loadStoredSession() : null;
    if (seed) {
      this.sessionId = seed.id;
      this.lastPushSeq = seed.lastPushSeq ?? 0n;
    }
  }
  get state() {
    if (this.wantClose)
      return "closed";
    if (this.ws && this.ws.readyState === WebSocket.OPEN && this.handshaken)
      return "open";
    if (this.reconnectAttempt > 0 || this.connectedOnce)
      return "reconnecting";
    return "connecting";
  }
  get session() {
    return this.sessionId;
  }
  get pushSeq() {
    return this.lastPushSeq;
  }
  addStateListener(fn) {
    this.stateListeners.add(fn);
    return () => this.stateListeners.delete(fn);
  }
  ready(timeoutMs = 1e4) {
    if (this.state === "open")
      return Promise.resolve();
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        off();
        reject(new Error("UltimaWS: timed out waiting for the connection"));
      }, timeoutMs);
      const off = this.addStateListener((s) => {
        if (s === "open") {
          clearTimeout(timer);
          off();
          resolve();
        } else if (s === "closed") {
          clearTimeout(timer);
          off();
          reject(new Error("UltimaWS: connection closed"));
        }
      });
    });
  }
  connect() {
    if (this.ws || this.dialing || this.wantClose)
      return;
    this.dialing = true;
    Promise.resolve(this.options.beforeConnect?.()).catch((err) => {
      this.options.onError?.(err instanceof Error ? err : new Error(String(err)));
    }).finally(() => {
      this.dialing = false;
      if (this.ws || this.wantClose)
        return;
      this.dial();
    });
  }
  dial() {
    const token = this.options.getToken?.() ?? null;
    const url = this.options.url + (token ? `?access_token=${encodeURIComponent(token)}` : "");
    const ws = new this.WSImpl(url);
    ws.binaryType = "arraybuffer";
    this.ws = ws;
    this.handshaken = false;
    ws.onopen = () => {
      this.reconnectAttempt = 0;
      this.sendHandshake();
      this.setState();
    };
    ws.onmessage = (ev) => {
      if (typeof ev.data === "string")
        return;
      let resp;
      try {
        resp = fromBinary(CommandResponseSchema, new Uint8Array(ev.data));
      } catch {
        return;
      }
      this.onFrame(resp);
    };
    ws.onclose = () => {
      this.ws = null;
      if (this.wantClose) {
        this.setState();
        return;
      }
      const base = this.options.reconnect?.baseDelayMs ?? 500;
      const max = this.options.reconnect?.maxDelayMs ?? 1e4;
      const delay = Math.min(max, base * 2 ** this.reconnectAttempt);
      this.reconnectAttempt++;
      this.setState();
      this.reconnectTimer = setTimeout(() => this.connect(), delay);
    };
    ws.onerror = () => {};
    this.setState();
  }
  close() {
    this.wantClose = true;
    if (this.reconnectTimer)
      clearTimeout(this.reconnectTimer);
    this.reconnectTimer = null;
    this.ws?.close();
    for (const [, p] of this.pending)
      p.reject(new Error("connection closed"));
    this.pending.clear();
    this.outbox = [];
    this.setState();
  }
  forceReconnect() {
    this.ws?.close();
  }
  setState() {
    const s = this.state;
    this.options.onStateChange?.(s);
    for (const fn of this.stateListeners)
      fn(s);
  }
  nextSeq() {
    this.seq += 1n;
    return this.seq;
  }
  sendRaw(frame) {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(frame);
    } else {
      this.outbox.push(frame);
    }
  }
  sendHandshake() {
    if (!this.useSession) {
      this.handshaken = true;
      this.onHandshakeComplete(false);
      return;
    }
    this.handshakePending++;
    const cmd = this.sessionId ? create(CommandSchema, {
      seq: this.nextSeq(),
      session: this.sessionId,
      lastPushSeq: this.lastPushSeq
    }) : create(CommandSchema, { seq: this.nextSeq() });
    this.ws?.send(toBinary(CommandSchema, cmd));
  }
  flush() {
    const queued = this.outbox;
    this.outbox = [];
    for (const frame of queued)
      this.ws?.send(frame);
  }
  onHandshakeComplete(resumed) {
    const reconnect = this.connectedOnce;
    this.connectedOnce = true;
    this.flush();
    this.setState();
    if (reconnect && !resumed) {
      for (const e of this.subs.values()) {
        this.exec(e.pattern ? "psubscribe" : "subscribe", e.name).catch((err) => {
          this.options.onError?.(err instanceof Error ? err : new Error(String(err)));
        });
      }
    }
  }
  onFrame(resp) {
    if (resp.seq === 0n) {
      if (resp.pushSeq > this.lastPushSeq) {
        this.lastPushSeq = resp.pushSeq;
        if (this.sessionId)
          this.persistSession();
      }
      if (resp.reply)
        this.dispatchPush(resp.reply, resp.pushSeq);
      return;
    }
    const value = resp.reply;
    if (this.handshakePending > 0 && !this.handshaken) {
      this.handshakePending--;
      this.handshaken = true;
      if (value?.kind.case === "error") {
        const text = value.kind.value;
        if (text.startsWith("SESSION_EXPIRED")) {
          const lost = this.sessionId ?? "";
          this.sessionId = null;
          this.lastPushSeq = 0n;
          this.storage.removeItem(this.storageKey);
          this.handshaken = false;
          this.sendHandshake();
          this.options.onGap?.(lost);
          return;
        }
        this.sessionId = null;
        this.onHandshakeComplete(false);
        return;
      }
      let resumed = false;
      if (resp.session) {
        resumed = this.sessionId === resp.session && this.sessionId !== null;
        this.sessionId = resp.session;
        this.persistSession();
        if (resumed)
          this.options.onResumed?.(resp.session);
      }
      this.onHandshakeComplete(resumed);
      return;
    }
    if (value?.kind.case === "error" && value.kind.value.startsWith("ABORTED")) {
      const p2 = this.pending.get(resp.seq);
      if (p2) {
        this.pending.delete(resp.seq);
        p2.reject(new Error(value.kind.value));
      }
      return;
    }
    const p = this.pending.get(resp.seq);
    if (p) {
      this.pending.delete(resp.seq);
      if (value)
        p.resolve(value);
      else
        p.reject(new Error("empty reply"));
    }
  }
  dispatchPush(v, pushSeq) {
    this.options.onPush?.(v, pushSeq);
    if (v.kind.case !== "push")
      return;
    const elems = v.kind.value.elems;
    const kind = elems[0] ? asString(elems[0]) : null;
    if (kind === "message" && elems.length >= 3) {
      const channel = elems[1] ? asString(elems[1]) : null;
      const payload = elems[2];
      if (channel === null || !payload)
        return;
      this.deliver(subKey(false, channel), channel, payload, pushSeq, undefined);
    } else if (kind === "pmessage" && elems.length >= 4) {
      const pattern = elems[1] ? asString(elems[1]) : null;
      const channel = elems[2] ? asString(elems[2]) : null;
      const payload = elems[3];
      if (pattern === null || channel === null || !payload)
        return;
      this.deliver(subKey(true, pattern), channel, payload, pushSeq, pattern);
    }
  }
  deliver(key, channel, payload, pushSeq, pattern) {
    const entry = this.subs.get(key);
    if (!entry)
      return;
    const bytes = payload.kind.case === "blobString" ? payload.kind.value : enc.encode(asString(payload) ?? "");
    const msg = {
      channel,
      payload: bytes,
      text: dec2.decode(bytes),
      pushSeq,
      ...pattern !== undefined ? { pattern } : {}
    };
    for (const h of entry.handlers) {
      try {
        h(msg);
      } catch (err) {
        this.options.onError?.(err instanceof Error ? err : new Error(String(err)));
      }
    }
  }
  loadStoredSession() {
    try {
      const raw = this.storage.getItem(this.storageKey);
      if (!raw)
        return null;
      const parsed = JSON.parse(raw);
      if (typeof parsed.id !== "string" || parsed.id === "")
        return null;
      const seq = typeof parsed.lastPushSeq === "string" ? BigInt(parsed.lastPushSeq) : 0n;
      return { id: parsed.id, lastPushSeq: seq };
    } catch {
      return null;
    }
  }
  persistSession() {
    if (!this.sessionId)
      return;
    try {
      this.storage.setItem(this.storageKey, JSON.stringify({ id: this.sessionId, lastPushSeq: this.lastPushSeq.toString() }));
    } catch {}
  }
  run(cmd) {
    if (this.wantClose)
      return Promise.reject(new Error("connection closed"));
    const seq = this.nextSeq();
    const frame = toBinary(CommandSchema, create(CommandSchema, { seq, cmd }));
    return new Promise((resolve, reject) => {
      this.pending.set(seq, { resolve, reject });
      this.sendRaw(frame);
    });
  }
  exec(command, ...args) {
    return this.run({ case: "generic", value: { command, args: args.map(b) } });
  }
  ping(message2) {
    const v = message2 === undefined ? this.exec("ping") : this.exec("ping", message2);
    return v.then((r) => asString(r) ?? "");
  }
  async set(key, value, options) {
    const v = await this.run({
      case: "set",
      value: {
        key: b(key),
        value: b(value),
        ttlMs: BigInt(options?.px ?? 0),
        nx: options?.nx ?? false,
        xx: options?.xx ?? false,
        get: options?.get ?? false
      }
    });
    return asString(v);
  }
  async get(key) {
    return asString(await this.run({ case: "get", value: { key: b(key) } }));
  }
  async getBytes(key) {
    const v = checkError(await this.run({ case: "get", value: { key: b(key) } }));
    if (v.kind.case === "null")
      return null;
    if (v.kind.case === "blobString")
      return v.kind.value;
    const s = asString(v);
    return s === null ? null : enc.encode(s);
  }
  async del(...keys) {
    return asInt(await this.run({ case: "del", value: { keys: keys.map(b) } }));
  }
  async exists(...keys) {
    return asInt(await this.run({ case: "exists", value: { keys: keys.map(b) } }));
  }
  async incrBy(key, delta) {
    return asBigInt(await this.run({ case: "incr", value: { key: b(key), delta: BigInt(delta) } }));
  }
  incr(key) {
    return this.incrBy(key, 1n);
  }
  decr(key) {
    return this.incrBy(key, -1n);
  }
  async incrByFloat(key, delta) {
    const v = await this.run({ case: "incrFloat", value: { key: b(key), delta } });
    const d = asDouble(v);
    if (d === null)
      throw new Error("INCRBYFLOAT: unexpected null reply");
    return d;
  }
  async mget(...keys) {
    return asStringArray(await this.run({ case: "mget", value: { keys: keys.map(b) } }));
  }
  async mset(pairs) {
    const v = await this.run({
      case: "mset",
      value: { pairs: pairsOf(pairs).map(([key, value]) => ({ key: enc.encode(key), value: enc.encode(value) })) }
    });
    checkError(v);
  }
  async append(key, value) {
    return asInt(await this.run({ case: "append", value: { key: b(key), value: b(value) } }));
  }
  async pexpire(key, ttlMs) {
    return asBool(await this.run({ case: "expire", value: { key: b(key), ttlMs: BigInt(ttlMs) } }));
  }
  async pttl(key) {
    return asInt(await this.run({ case: "ttl", value: { key: b(key) } }));
  }
  async persist(key) {
    return asBool(await this.run({ case: "persist", value: { key: b(key) } }));
  }
  async hget(key, field) {
    return asString(await this.run({ case: "hget", value: { key: b(key), field: b(field) } }));
  }
  async hset(key, pairs) {
    return asInt(await this.run({
      case: "hset",
      value: {
        key: b(key),
        pairs: pairsOf(pairs).map(([field, value]) => ({ field: enc.encode(field), value: enc.encode(value) }))
      }
    }));
  }
  async hgetall(key) {
    return asStringMap(await this.run({ case: "hgetall", value: { key: b(key) } }));
  }
  async hdel(key, ...fields2) {
    return asInt(await this.run({ case: "hdel", value: { key: b(key), fields: fields2.map(b) } }));
  }
  async hincrBy(key, field, delta) {
    return asBigInt(await this.run({ case: "hincrby", value: { key: b(key), field: b(field), delta: BigInt(delta) } }));
  }
  async lpush(key, ...elems) {
    return asInt(await this.run({ case: "lpush", value: { key: b(key), elems: elems.map(b) } }));
  }
  async rpush(key, ...elems) {
    return asInt(await this.run({ case: "rpush", value: { key: b(key), elems: elems.map(b) } }));
  }
  async lpop(key) {
    return asString(await this.run({ case: "lpop", value: { key: b(key) } }));
  }
  async rpop(key) {
    return asString(await this.run({ case: "rpop", value: { key: b(key) } }));
  }
  async lrange(key, start, stop) {
    const v = await this.run({ case: "lrange", value: { key: b(key), start: BigInt(start), stop: BigInt(stop) } });
    return asStringArray(v).map((s) => s ?? "");
  }
  async llen(key) {
    return asInt(await this.run({ case: "llen", value: { key: b(key) } }));
  }
  async sadd(key, ...members) {
    return asInt(await this.run({ case: "sadd", value: { key: b(key), members: members.map(b) } }));
  }
  async srem(key, ...members) {
    return asInt(await this.run({ case: "srem", value: { key: b(key), members: members.map(b) } }));
  }
  async smembers(key) {
    const v = await this.run({ case: "smembers", value: { key: b(key) } });
    return asStringArray(v).map((s) => s ?? "");
  }
  async sismember(key, member) {
    return asBool(await this.run({ case: "sismember", value: { key: b(key), member: b(member) } }));
  }
  async zadd(key, members, options) {
    const list = typeof members === "object" && members !== null && Symbol.iterator in members ? [...members] : Object.entries(members);
    return asInt(await this.run({
      case: "zadd",
      value: {
        key: b(key),
        members: list.map(([member, score]) => ({ score, member: enc.encode(member) })),
        nx: options?.nx ?? false,
        xx: options?.xx ?? false,
        gt: options?.gt ?? false,
        lt: options?.lt ?? false,
        ch: options?.ch ?? false,
        incr: false
      }
    }));
  }
  async zscore(key, member) {
    return asDouble(await this.run({ case: "zscore", value: { key: b(key), member: b(member) } }));
  }
  async zrange(key, start, stop) {
    const v = await this.run({
      case: "zrange",
      value: { key: b(key), start: BigInt(start), stop: BigInt(stop), withscores: false }
    });
    return asStringArray(v).map((s) => s ?? "");
  }
  async zrangeWithScores(key, start, stop) {
    const v = await this.run({
      case: "zrange",
      value: { key: b(key), start: BigInt(start), stop: BigInt(stop), withscores: true }
    });
    return asScoredMembers(v);
  }
  async zrem(key, ...members) {
    return asInt(await this.run({ case: "zrem", value: { key: b(key), members: members.map(b) } }));
  }
  async zcard(key) {
    return asInt(await this.run({ case: "zcard", value: { key: b(key) } }));
  }
  async subscribe(channel, handler) {
    await this.addSub(false, channel, handler);
  }
  async psubscribe(pattern, handler) {
    await this.addSub(true, pattern, handler);
  }
  async addSub(pattern, name, handler) {
    const key = subKey(pattern, name);
    let entry = this.subs.get(key);
    const first = !entry;
    if (!entry) {
      entry = { pattern, name, handlers: new Set };
      this.subs.set(key, entry);
    }
    entry.handlers.add(handler);
    if (first) {
      await this.exec(pattern ? "psubscribe" : "subscribe", name);
    }
  }
  async unsubscribe(channel, handler) {
    await this.removeSub(false, channel, handler);
  }
  async punsubscribe(pattern, handler) {
    await this.removeSub(true, pattern, handler);
  }
  async removeSub(pattern, name, handler) {
    const key = subKey(pattern, name);
    const entry = this.subs.get(key);
    if (!entry)
      return;
    if (handler)
      entry.handlers.delete(handler);
    if (handler && entry.handlers.size > 0)
      return;
    this.subs.delete(key);
    await this.exec(pattern ? "punsubscribe" : "unsubscribe", name);
  }
}
// ../typescript/src/rest.ts
class ApiError extends Error {
  status;
  constructor(status, message2) {
    super(message2);
    this.name = "ApiError";
    this.status = status;
  }
}

class RestClient {
  baseUrl;
  auth;
  fetchImpl;
  constructor(options) {
    this.baseUrl = options.baseUrl.replace(/\/+$/, "");
    this.auth = options.auth;
    this.fetchImpl = options.fetchImpl ?? fetch;
  }
  async rawRequest(path, init) {
    const headers = new Headers(init.headers);
    if (init.body !== undefined && !headers.has("Content-Type")) {
      headers.set("Content-Type", "application/json");
    }
    const token = this.auth ? await this.auth.getAccessToken() : null;
    if (token)
      headers.set("Authorization", `Bearer ${token}`);
    return this.fetchImpl(this.baseUrl + path, { ...init, headers });
  }
  async request(method, path, body) {
    const init = { method };
    if (body !== undefined)
      init.body = JSON.stringify(body);
    let res = await this.rawRequest(path, init);
    if (res.status === 401 && this.auth) {
      if (await this.auth.refresh()) {
        res = await this.rawRequest(path, init);
      }
    }
    if (!res.ok)
      throw await errorFrom(res);
    if (res.status === 204)
      return;
    return await res.json();
  }
  ping() {
    return this.request("GET", "/api/v1/ping");
  }
  login(body) {
    return this.request("POST", "/api/v1/auth/login", body);
  }
  refreshToken(refreshToken) {
    return this.request("POST", "/api/v1/auth/refresh", { refresh_token: refreshToken });
  }
  logout(refreshToken) {
    return this.request("POST", "/api/v1/auth/logout", { refresh_token: refreshToken });
  }
  changePassword(currentPassword, newPassword, totp) {
    return this.request("POST", "/api/v1/auth/password", {
      current_password: currentPassword,
      new_password: newPassword,
      ...totp ? { totp } : {}
    });
  }
  totpEnable() {
    return this.request("POST", "/api/v1/auth/totp/enable");
  }
  totpRegenerate() {
    return this.request("POST", "/api/v1/auth/totp/regenerate");
  }
  totpConfirm(code) {
    return this.request("POST", "/api/v1/auth/totp/confirm", { totp: code });
  }
  totpDisable(password, totp) {
    return this.request("POST", "/api/v1/auth/totp/disable", { password, ...totp ? { totp } : {} });
  }
  usersList() {
    return this.request("GET", "/api/v1/admin/users");
  }
  usersCreate(username, password, cls) {
    return this.request("POST", "/api/v1/admin/users", { username, password, class: cls });
  }
  usersUpdate(name, update) {
    return this.request("PUT", `/api/v1/admin/users/${encodeURIComponent(name)}`, update);
  }
  usersDelete(name) {
    return this.request("DELETE", `/api/v1/admin/users/${encodeURIComponent(name)}`);
  }
  usersRevokeSessions(name) {
    return this.request("POST", `/api/v1/admin/users/${encodeURIComponent(name)}/revoke-sessions`);
  }
  info() {
    return this.request("GET", "/api/v1/info");
  }
  shards() {
    return this.request("GET", "/api/v1/shards");
  }
  clients() {
    return this.request("GET", "/api/v1/clients");
  }
  killClient(id) {
    return this.request("POST", `/api/v1/clients/${id}/kill`);
  }
  slowlog(count) {
    return this.request("GET", `/api/v1/slowlog${count ? `?count=${count}` : ""}`);
  }
  resetSlowlog() {
    return this.request("DELETE", "/api/v1/slowlog");
  }
  latency() {
    return this.request("GET", "/api/v1/latency");
  }
  getConfig() {
    return this.request("GET", "/api/v1/config");
  }
  putConfig(entries) {
    return this.request("PUT", "/api/v1/config", { entries });
  }
  flushdb(db, all) {
    return this.request("POST", "/api/v1/flushdb", { db, all });
  }
  scanKeys(cursor, match = "", count = 0, db = 0) {
    const q = new URLSearchParams({ cursor, db: String(db) });
    if (match)
      q.set("match", match);
    if (count > 0)
      q.set("count", String(count));
    return this.request("GET", `/api/v1/keys/scan?${q.toString()}`);
  }
  getKey(key, db = 0) {
    return this.request("GET", `/api/v1/key/${encodeURIComponent(key)}?db=${db}`);
  }
  deleteKey(key, db = 0) {
    return this.request("DELETE", `/api/v1/key/${encodeURIComponent(key)}?db=${db}`);
  }
  save() {
    return this.request("POST", "/api/v1/save");
  }
  bgsave() {
    return this.request("POST", "/api/v1/bgsave");
  }
  bgrewriteaof() {
    return this.request("POST", "/api/v1/bgrewriteaof");
  }
}
async function errorFrom(res) {
  let msg = `${res.status} ${res.statusText}`;
  try {
    const body = await res.json();
    if (typeof body.error === "string" && body.error !== "")
      msg = body.error;
  } catch {}
  return new ApiError(res.status, msg);
}
// ../typescript/src/auth.ts
function decodeJwtClaims(token) {
  const payload = token.split(".")[1];
  if (!payload)
    return null;
  try {
    const b64 = payload.replace(/-/g, "+").replace(/_/g, "/");
    return JSON.parse(atob(b64));
  } catch {
    return null;
  }
}

class AuthManager {
  options;
  rest;
  storage;
  storageKey;
  marginMs;
  tokensValue;
  refreshInFlight = null;
  constructor(options) {
    this.options = options;
    this.rest = new RestClient({ baseUrl: options.baseUrl, ...options.fetchImpl ? { fetchImpl: options.fetchImpl } : {} });
    this.storage = options.storage ?? defaultStorage();
    this.storageKey = options.storageKey ?? "ultima.tokens";
    this.marginMs = options.refreshMarginMs ?? 30000;
    this.tokensValue = this.loadStored();
  }
  get tokens() {
    return this.tokensValue;
  }
  async probe() {
    try {
      await this.rest.info();
      return { authRequired: false };
    } catch (err) {
      if (err instanceof ApiError && err.status === 401)
        return { authRequired: true };
      throw err;
    }
  }
  session() {
    const t = this.tokensValue;
    if (!t)
      return null;
    const claims = decodeJwtClaims(t.accessToken);
    if (!claims || typeof claims.sub !== "string" || claims.sub === "")
      return null;
    return {
      username: claims.sub,
      accountClass: claims.class === "admin" ? "admin" : "data",
      authDisabled: false
    };
  }
  async login(credentials) {
    const creds = credentials ?? this.options.credentials;
    if (!creds)
      throw new Error("AuthManager.login: no credentials");
    const pair = await this.rest.login(creds.totp ? { username: creds.username, password: creds.password, totp: creds.totp } : { username: creds.username, password: creds.password });
    this.setTokens(pair.access_token, pair.refresh_token, pair.expires_in);
    const sess = this.session();
    if (!sess)
      throw new Error("AuthManager.login: server returned an unreadable access token");
    return sess;
  }
  async logout() {
    const t = this.tokensValue;
    if (t) {
      try {
        await this.rest.logout(t.refreshToken);
      } catch {}
    }
    this.setTokens(null);
  }
  async getAccessToken() {
    const t = this.tokensValue;
    if (!t) {
      if (this.options.credentials) {
        await this.login();
        return this.tokensValue?.accessToken ?? null;
      }
      return null;
    }
    if (t.expiresAt - Date.now() > this.marginMs)
      return t.accessToken;
    if (await this.refresh())
      return this.tokensValue?.accessToken ?? null;
    if (this.options.credentials) {
      await this.login();
      return this.tokensValue?.accessToken ?? null;
    }
    return null;
  }
  refresh() {
    if (!this.refreshInFlight) {
      const had = this.tokensValue !== null;
      this.refreshInFlight = this.doRefresh().then((ok) => {
        if (!ok && had) {
          this.setTokens(null);
          this.options.onSessionExpired?.();
        }
        return ok;
      }).finally(() => {
        this.refreshInFlight = null;
      });
    }
    return this.refreshInFlight;
  }
  async doRefresh() {
    const t = this.tokensValue;
    if (!t)
      return false;
    try {
      const pair = await this.rest.refreshToken(t.refreshToken);
      this.setTokens(pair.access_token, pair.refresh_token, pair.expires_in);
      return true;
    } catch {
      return false;
    }
  }
  setTokens(accessToken, refreshToken, expiresIn) {
    if (accessToken === null) {
      this.tokensValue = null;
      this.storage.removeItem(this.storageKey);
      return;
    }
    let expiresAt = Date.now() + (expiresIn ?? 0) * 1000;
    const claims = decodeJwtClaims(accessToken);
    if (claims && typeof claims.exp === "number" && claims.exp > 0) {
      expiresAt = claims.exp * 1000;
    }
    this.tokensValue = { accessToken, refreshToken: refreshToken ?? "", expiresAt };
    try {
      this.storage.setItem(this.storageKey, JSON.stringify(this.tokensValue));
    } catch {}
  }
  loadStored() {
    try {
      const raw = this.storage.getItem(this.storageKey);
      if (!raw)
        return null;
      const parsed = JSON.parse(raw);
      if (typeof parsed.accessToken !== "string" || typeof parsed.refreshToken !== "string")
        return null;
      return {
        accessToken: parsed.accessToken,
        refreshToken: parsed.refreshToken,
        expiresAt: typeof parsed.expiresAt === "number" ? parsed.expiresAt : 0
      };
    } catch {
      return null;
    }
  }
}
// ../typescript/src/client.ts
class UltimaClient {
  auth;
  rest;
  ws;
  baseUrl;
  constructor(options = {}) {
    this.baseUrl = options.baseUrl ?? `${options.secure ? "https" : "http"}://${options.host ?? "127.0.0.1"}:${options.httpPort ?? 6381}`;
    const storage = options.storage ?? defaultStorage();
    this.auth = new AuthManager({
      baseUrl: this.baseUrl,
      storage,
      ...options.credentials ? { credentials: options.credentials } : {},
      ...options.onSessionExpired ? { onSessionExpired: options.onSessionExpired } : {}
    });
    this.rest = new RestClient({ baseUrl: this.baseUrl, auth: this.auth });
    const wsOpts = options.ws ?? {};
    this.ws = new UltimaWS({
      url: wsOpts.url ?? this.baseUrl.replace(/^http/, "ws") + "/ws/v1",
      getToken: () => this.auth.tokens?.accessToken ?? null,
      beforeConnect: () => this.auth.getAccessToken(),
      storage,
      session: wsOpts.session,
      resume: wsOpts.resume,
      ...wsOpts.reconnect ? { reconnect: wsOpts.reconnect } : {},
      ...wsOpts.onPush ? { onPush: wsOpts.onPush } : {},
      ...wsOpts.onGap ? { onGap: wsOpts.onGap } : {},
      ...wsOpts.onResumed ? { onResumed: wsOpts.onResumed } : {},
      ...wsOpts.onStateChange ? { onStateChange: wsOpts.onStateChange } : {},
      ...wsOpts.onError ? { onError: wsOpts.onError } : {},
      ...wsOpts.WebSocketImpl ? { WebSocketImpl: wsOpts.WebSocketImpl } : {}
    });
  }
  login(username, password, totp) {
    return this.auth.login(totp ? { username, password, totp } : { username, password });
  }
  logout() {
    return this.auth.logout();
  }
  connect() {
    this.ws.connect();
  }
  close() {
    this.ws.close();
  }
  ping() {
    return this.ws.ping();
  }
  subscribe(channel, handler) {
    return this.ws.subscribe(channel, handler);
  }
  psubscribe(pattern, handler) {
    return this.ws.psubscribe(pattern, handler);
  }
}
export {
  isNull,
  defaultStorage,
  checkError,
  asStringMap,
  asStringArray,
  asString,
  asScoredMembers,
  asInt,
  asDouble,
  asBytes,
  asBool,
  asBigInt,
  UltimaWS,
  UltimaClient,
  RestClient,
  ReplyError,
  MemoryStorage,
  AuthManager,
  ApiError
};
