"use strict";
(() => {
  var __defProp = Object.defineProperty;
  var __export = (target, all) => {
    for (var name in all)
      __defProp(target, name, { get: all[name], enumerable: true });
  };

  // node_modules/zod/v4/core/core.js
  var NEVER = Object.freeze({
    status: "aborted"
  });
  // @__NO_SIDE_EFFECTS__
  function $constructor(name, initializer3, params) {
    function init(inst, def) {
      var _a;
      Object.defineProperty(inst, "_zod", {
        value: inst._zod ?? {},
        enumerable: false
      });
      (_a = inst._zod).traits ?? (_a.traits = /* @__PURE__ */ new Set());
      inst._zod.traits.add(name);
      initializer3(inst, def);
      for (const k2 in _.prototype) {
        if (!(k2 in inst))
          Object.defineProperty(inst, k2, { value: _.prototype[k2].bind(inst) });
      }
      inst._zod.constr = _;
      inst._zod.def = def;
    }
    const Parent = params?.Parent ?? Object;
    class Definition extends Parent {
    }
    Object.defineProperty(Definition, "name", { value: name });
    function _(def) {
      var _a;
      const inst = params?.Parent ? new Definition() : this;
      init(inst, def);
      (_a = inst._zod).deferred ?? (_a.deferred = []);
      for (const fn2 of inst._zod.deferred) {
        fn2();
      }
      return inst;
    }
    Object.defineProperty(_, "init", { value: init });
    Object.defineProperty(_, Symbol.hasInstance, {
      value: (inst) => {
        if (params?.Parent && inst instanceof params.Parent)
          return true;
        return inst?._zod?.traits?.has(name);
      }
    });
    Object.defineProperty(_, "name", { value: name });
    return _;
  }
  var $brand = Symbol("zod_brand");
  var $ZodAsyncError = class extends Error {
    constructor() {
      super(`Encountered Promise during synchronous parse. Use .parseAsync() instead.`);
    }
  };
  var globalConfig = {};
  function config(newConfig) {
    if (newConfig)
      Object.assign(globalConfig, newConfig);
    return globalConfig;
  }

  // node_modules/zod/v4/core/util.js
  var util_exports = {};
  __export(util_exports, {
    BIGINT_FORMAT_RANGES: () => BIGINT_FORMAT_RANGES,
    Class: () => Class,
    NUMBER_FORMAT_RANGES: () => NUMBER_FORMAT_RANGES,
    aborted: () => aborted,
    allowsEval: () => allowsEval,
    assert: () => assert,
    assertEqual: () => assertEqual,
    assertIs: () => assertIs,
    assertNever: () => assertNever,
    assertNotEqual: () => assertNotEqual,
    assignProp: () => assignProp,
    cached: () => cached,
    captureStackTrace: () => captureStackTrace,
    cleanEnum: () => cleanEnum,
    cleanRegex: () => cleanRegex,
    clone: () => clone,
    createTransparentProxy: () => createTransparentProxy,
    defineLazy: () => defineLazy,
    esc: () => esc,
    escapeRegex: () => escapeRegex,
    extend: () => extend,
    finalizeIssue: () => finalizeIssue,
    floatSafeRemainder: () => floatSafeRemainder,
    getElementAtPath: () => getElementAtPath,
    getEnumValues: () => getEnumValues,
    getLengthableOrigin: () => getLengthableOrigin,
    getParsedType: () => getParsedType,
    getSizableOrigin: () => getSizableOrigin,
    isObject: () => isObject,
    isPlainObject: () => isPlainObject,
    issue: () => issue,
    joinValues: () => joinValues,
    jsonStringifyReplacer: () => jsonStringifyReplacer,
    merge: () => merge,
    normalizeParams: () => normalizeParams,
    nullish: () => nullish,
    numKeys: () => numKeys,
    omit: () => omit,
    optionalKeys: () => optionalKeys,
    partial: () => partial,
    pick: () => pick,
    prefixIssues: () => prefixIssues,
    primitiveTypes: () => primitiveTypes,
    promiseAllObject: () => promiseAllObject,
    propertyKeyTypes: () => propertyKeyTypes,
    randomString: () => randomString,
    required: () => required,
    stringifyPrimitive: () => stringifyPrimitive,
    unwrapMessage: () => unwrapMessage
  });
  function assertEqual(val) {
    return val;
  }
  function assertNotEqual(val) {
    return val;
  }
  function assertIs(_arg) {
  }
  function assertNever(_x) {
    throw new Error();
  }
  function assert(_) {
  }
  function getEnumValues(entries) {
    const numericValues = Object.values(entries).filter((v) => typeof v === "number");
    const values = Object.entries(entries).filter(([k2, _]) => numericValues.indexOf(+k2) === -1).map(([_, v]) => v);
    return values;
  }
  function joinValues(array2, separator = "|") {
    return array2.map((val) => stringifyPrimitive(val)).join(separator);
  }
  function jsonStringifyReplacer(_, value) {
    if (typeof value === "bigint")
      return value.toString();
    return value;
  }
  function cached(getter) {
    const set = false;
    return {
      get value() {
        if (!set) {
          const value = getter();
          Object.defineProperty(this, "value", { value });
          return value;
        }
        throw new Error("cached value already set");
      }
    };
  }
  function nullish(input) {
    return input === null || input === void 0;
  }
  function cleanRegex(source) {
    const start = source.startsWith("^") ? 1 : 0;
    const end = source.endsWith("$") ? source.length - 1 : source.length;
    return source.slice(start, end);
  }
  function floatSafeRemainder(val, step) {
    const valDecCount = (val.toString().split(".")[1] || "").length;
    const stepDecCount = (step.toString().split(".")[1] || "").length;
    const decCount = valDecCount > stepDecCount ? valDecCount : stepDecCount;
    const valInt = Number.parseInt(val.toFixed(decCount).replace(".", ""));
    const stepInt = Number.parseInt(step.toFixed(decCount).replace(".", ""));
    return valInt % stepInt / 10 ** decCount;
  }
  function defineLazy(object3, key, getter) {
    const set = false;
    Object.defineProperty(object3, key, {
      get() {
        if (!set) {
          const value = getter();
          object3[key] = value;
          return value;
        }
        throw new Error("cached value already set");
      },
      set(v) {
        Object.defineProperty(object3, key, {
          value: v
          // configurable: true,
        });
      },
      configurable: true
    });
  }
  function assignProp(target, prop, value) {
    Object.defineProperty(target, prop, {
      value,
      writable: true,
      enumerable: true,
      configurable: true
    });
  }
  function getElementAtPath(obj, path) {
    if (!path)
      return obj;
    return path.reduce((acc, key) => acc?.[key], obj);
  }
  function promiseAllObject(promisesObj) {
    const keys = Object.keys(promisesObj);
    const promises = keys.map((key) => promisesObj[key]);
    return Promise.all(promises).then((results) => {
      const resolvedObj = {};
      for (let i = 0; i < keys.length; i++) {
        resolvedObj[keys[i]] = results[i];
      }
      return resolvedObj;
    });
  }
  function randomString(length = 10) {
    const chars = "abcdefghijklmnopqrstuvwxyz";
    let str = "";
    for (let i = 0; i < length; i++) {
      str += chars[Math.floor(Math.random() * chars.length)];
    }
    return str;
  }
  function esc(str) {
    return JSON.stringify(str);
  }
  var captureStackTrace = Error.captureStackTrace ? Error.captureStackTrace : (..._args) => {
  };
  function isObject(data) {
    return typeof data === "object" && data !== null && !Array.isArray(data);
  }
  var allowsEval = cached(() => {
    if (typeof navigator !== "undefined" && navigator?.userAgent?.includes("Cloudflare")) {
      return false;
    }
    try {
      const F2 = Function;
      new F2("");
      return true;
    } catch (_) {
      return false;
    }
  });
  function isPlainObject(o) {
    if (isObject(o) === false)
      return false;
    const ctor = o.constructor;
    if (ctor === void 0)
      return true;
    const prot = ctor.prototype;
    if (isObject(prot) === false)
      return false;
    if (Object.prototype.hasOwnProperty.call(prot, "isPrototypeOf") === false) {
      return false;
    }
    return true;
  }
  function numKeys(data) {
    let keyCount = 0;
    for (const key in data) {
      if (Object.prototype.hasOwnProperty.call(data, key)) {
        keyCount++;
      }
    }
    return keyCount;
  }
  var getParsedType = (data) => {
    const t = typeof data;
    switch (t) {
      case "undefined":
        return "undefined";
      case "string":
        return "string";
      case "number":
        return Number.isNaN(data) ? "nan" : "number";
      case "boolean":
        return "boolean";
      case "function":
        return "function";
      case "bigint":
        return "bigint";
      case "symbol":
        return "symbol";
      case "object":
        if (Array.isArray(data)) {
          return "array";
        }
        if (data === null) {
          return "null";
        }
        if (data.then && typeof data.then === "function" && data.catch && typeof data.catch === "function") {
          return "promise";
        }
        if (typeof Map !== "undefined" && data instanceof Map) {
          return "map";
        }
        if (typeof Set !== "undefined" && data instanceof Set) {
          return "set";
        }
        if (typeof Date !== "undefined" && data instanceof Date) {
          return "date";
        }
        if (typeof File !== "undefined" && data instanceof File) {
          return "file";
        }
        return "object";
      default:
        throw new Error(`Unknown data type: ${t}`);
    }
  };
  var propertyKeyTypes = /* @__PURE__ */ new Set(["string", "number", "symbol"]);
  var primitiveTypes = /* @__PURE__ */ new Set(["string", "number", "bigint", "boolean", "symbol", "undefined"]);
  function escapeRegex(str) {
    return str.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  }
  function clone(inst, def, params) {
    const cl2 = new inst._zod.constr(def ?? inst._zod.def);
    if (!def || params?.parent)
      cl2._zod.parent = inst;
    return cl2;
  }
  function normalizeParams(_params) {
    const params = _params;
    if (!params)
      return {};
    if (typeof params === "string")
      return { error: () => params };
    if (params?.message !== void 0) {
      if (params?.error !== void 0)
        throw new Error("Cannot specify both `message` and `error` params");
      params.error = params.message;
    }
    delete params.message;
    if (typeof params.error === "string")
      return { ...params, error: () => params.error };
    return params;
  }
  function createTransparentProxy(getter) {
    let target;
    return new Proxy({}, {
      get(_, prop, receiver) {
        target ?? (target = getter());
        return Reflect.get(target, prop, receiver);
      },
      set(_, prop, value, receiver) {
        target ?? (target = getter());
        return Reflect.set(target, prop, value, receiver);
      },
      has(_, prop) {
        target ?? (target = getter());
        return Reflect.has(target, prop);
      },
      deleteProperty(_, prop) {
        target ?? (target = getter());
        return Reflect.deleteProperty(target, prop);
      },
      ownKeys(_) {
        target ?? (target = getter());
        return Reflect.ownKeys(target);
      },
      getOwnPropertyDescriptor(_, prop) {
        target ?? (target = getter());
        return Reflect.getOwnPropertyDescriptor(target, prop);
      },
      defineProperty(_, prop, descriptor) {
        target ?? (target = getter());
        return Reflect.defineProperty(target, prop, descriptor);
      }
    });
  }
  function stringifyPrimitive(value) {
    if (typeof value === "bigint")
      return value.toString() + "n";
    if (typeof value === "string")
      return `"${value}"`;
    return `${value}`;
  }
  function optionalKeys(shape) {
    return Object.keys(shape).filter((k2) => {
      return shape[k2]._zod.optin === "optional" && shape[k2]._zod.optout === "optional";
    });
  }
  var NUMBER_FORMAT_RANGES = {
    safeint: [Number.MIN_SAFE_INTEGER, Number.MAX_SAFE_INTEGER],
    int32: [-2147483648, 2147483647],
    uint32: [0, 4294967295],
    float32: [-34028234663852886e22, 34028234663852886e22],
    float64: [-Number.MAX_VALUE, Number.MAX_VALUE]
  };
  var BIGINT_FORMAT_RANGES = {
    int64: [/* @__PURE__ */ BigInt("-9223372036854775808"), /* @__PURE__ */ BigInt("9223372036854775807")],
    uint64: [/* @__PURE__ */ BigInt(0), /* @__PURE__ */ BigInt("18446744073709551615")]
  };
  function pick(schema, mask) {
    const newShape = {};
    const currDef = schema._zod.def;
    for (const key in mask) {
      if (!(key in currDef.shape)) {
        throw new Error(`Unrecognized key: "${key}"`);
      }
      if (!mask[key])
        continue;
      newShape[key] = currDef.shape[key];
    }
    return clone(schema, {
      ...schema._zod.def,
      shape: newShape,
      checks: []
    });
  }
  function omit(schema, mask) {
    const newShape = { ...schema._zod.def.shape };
    const currDef = schema._zod.def;
    for (const key in mask) {
      if (!(key in currDef.shape)) {
        throw new Error(`Unrecognized key: "${key}"`);
      }
      if (!mask[key])
        continue;
      delete newShape[key];
    }
    return clone(schema, {
      ...schema._zod.def,
      shape: newShape,
      checks: []
    });
  }
  function extend(schema, shape) {
    if (!isPlainObject(shape)) {
      throw new Error("Invalid input to extend: expected a plain object");
    }
    const def = {
      ...schema._zod.def,
      get shape() {
        const _shape = { ...schema._zod.def.shape, ...shape };
        assignProp(this, "shape", _shape);
        return _shape;
      },
      checks: []
      // delete existing checks
    };
    return clone(schema, def);
  }
  function merge(a2, b2) {
    return clone(a2, {
      ...a2._zod.def,
      get shape() {
        const _shape = { ...a2._zod.def.shape, ...b2._zod.def.shape };
        assignProp(this, "shape", _shape);
        return _shape;
      },
      catchall: b2._zod.def.catchall,
      checks: []
      // delete existing checks
    });
  }
  function partial(Class2, schema, mask) {
    const oldShape = schema._zod.def.shape;
    const shape = { ...oldShape };
    if (mask) {
      for (const key in mask) {
        if (!(key in oldShape)) {
          throw new Error(`Unrecognized key: "${key}"`);
        }
        if (!mask[key])
          continue;
        shape[key] = Class2 ? new Class2({
          type: "optional",
          innerType: oldShape[key]
        }) : oldShape[key];
      }
    } else {
      for (const key in oldShape) {
        shape[key] = Class2 ? new Class2({
          type: "optional",
          innerType: oldShape[key]
        }) : oldShape[key];
      }
    }
    return clone(schema, {
      ...schema._zod.def,
      shape,
      checks: []
    });
  }
  function required(Class2, schema, mask) {
    const oldShape = schema._zod.def.shape;
    const shape = { ...oldShape };
    if (mask) {
      for (const key in mask) {
        if (!(key in shape)) {
          throw new Error(`Unrecognized key: "${key}"`);
        }
        if (!mask[key])
          continue;
        shape[key] = new Class2({
          type: "nonoptional",
          innerType: oldShape[key]
        });
      }
    } else {
      for (const key in oldShape) {
        shape[key] = new Class2({
          type: "nonoptional",
          innerType: oldShape[key]
        });
      }
    }
    return clone(schema, {
      ...schema._zod.def,
      shape,
      // optional: [],
      checks: []
    });
  }
  function aborted(x2, startIndex = 0) {
    for (let i = startIndex; i < x2.issues.length; i++) {
      if (x2.issues[i]?.continue !== true)
        return true;
    }
    return false;
  }
  function prefixIssues(path, issues) {
    return issues.map((iss) => {
      var _a;
      (_a = iss).path ?? (_a.path = []);
      iss.path.unshift(path);
      return iss;
    });
  }
  function unwrapMessage(message) {
    return typeof message === "string" ? message : message?.message;
  }
  function finalizeIssue(iss, ctx, config2) {
    const full = { ...iss, path: iss.path ?? [] };
    if (!iss.message) {
      const message = unwrapMessage(iss.inst?._zod.def?.error?.(iss)) ?? unwrapMessage(ctx?.error?.(iss)) ?? unwrapMessage(config2.customError?.(iss)) ?? unwrapMessage(config2.localeError?.(iss)) ?? "Invalid input";
      full.message = message;
    }
    delete full.inst;
    delete full.continue;
    if (!ctx?.reportInput) {
      delete full.input;
    }
    return full;
  }
  function getSizableOrigin(input) {
    if (input instanceof Set)
      return "set";
    if (input instanceof Map)
      return "map";
    if (input instanceof File)
      return "file";
    return "unknown";
  }
  function getLengthableOrigin(input) {
    if (Array.isArray(input))
      return "array";
    if (typeof input === "string")
      return "string";
    return "unknown";
  }
  function issue(...args) {
    const [iss, input, inst] = args;
    if (typeof iss === "string") {
      return {
        message: iss,
        code: "custom",
        input,
        inst
      };
    }
    return { ...iss };
  }
  function cleanEnum(obj) {
    return Object.entries(obj).filter(([k2, _]) => {
      return Number.isNaN(Number.parseInt(k2, 10));
    }).map((el2) => el2[1]);
  }
  var Class = class {
    constructor(..._args) {
    }
  };

  // node_modules/zod/v4/core/errors.js
  var initializer = (inst, def) => {
    inst.name = "$ZodError";
    Object.defineProperty(inst, "_zod", {
      value: inst._zod,
      enumerable: false
    });
    Object.defineProperty(inst, "issues", {
      value: def,
      enumerable: false
    });
    Object.defineProperty(inst, "message", {
      get() {
        return JSON.stringify(def, jsonStringifyReplacer, 2);
      },
      enumerable: true
      // configurable: false,
    });
    Object.defineProperty(inst, "toString", {
      value: () => inst.message,
      enumerable: false
    });
  };
  var $ZodError = $constructor("$ZodError", initializer);
  var $ZodRealError = $constructor("$ZodError", initializer, { Parent: Error });
  function flattenError(error2, mapper = (issue2) => issue2.message) {
    const fieldErrors = {};
    const formErrors = [];
    for (const sub of error2.issues) {
      if (sub.path.length > 0) {
        fieldErrors[sub.path[0]] = fieldErrors[sub.path[0]] || [];
        fieldErrors[sub.path[0]].push(mapper(sub));
      } else {
        formErrors.push(mapper(sub));
      }
    }
    return { formErrors, fieldErrors };
  }
  function formatError(error2, _mapper) {
    const mapper = _mapper || function(issue2) {
      return issue2.message;
    };
    const fieldErrors = { _errors: [] };
    const processError = (error3) => {
      for (const issue2 of error3.issues) {
        if (issue2.code === "invalid_union" && issue2.errors.length) {
          issue2.errors.map((issues) => processError({ issues }));
        } else if (issue2.code === "invalid_key") {
          processError({ issues: issue2.issues });
        } else if (issue2.code === "invalid_element") {
          processError({ issues: issue2.issues });
        } else if (issue2.path.length === 0) {
          fieldErrors._errors.push(mapper(issue2));
        } else {
          let curr = fieldErrors;
          let i = 0;
          while (i < issue2.path.length) {
            const el2 = issue2.path[i];
            const terminal = i === issue2.path.length - 1;
            if (!terminal) {
              curr[el2] = curr[el2] || { _errors: [] };
            } else {
              curr[el2] = curr[el2] || { _errors: [] };
              curr[el2]._errors.push(mapper(issue2));
            }
            curr = curr[el2];
            i++;
          }
        }
      }
    };
    processError(error2);
    return fieldErrors;
  }

  // node_modules/zod/v4/core/parse.js
  var _parse = (_Err) => (schema, value, _ctx, _params) => {
    const ctx = _ctx ? Object.assign(_ctx, { async: false }) : { async: false };
    const result = schema._zod.run({ value, issues: [] }, ctx);
    if (result instanceof Promise) {
      throw new $ZodAsyncError();
    }
    if (result.issues.length) {
      const e = new (_params?.Err ?? _Err)(result.issues.map((iss) => finalizeIssue(iss, ctx, config())));
      captureStackTrace(e, _params?.callee);
      throw e;
    }
    return result.value;
  };
  var _parseAsync = (_Err) => async (schema, value, _ctx, params) => {
    const ctx = _ctx ? Object.assign(_ctx, { async: true }) : { async: true };
    let result = schema._zod.run({ value, issues: [] }, ctx);
    if (result instanceof Promise)
      result = await result;
    if (result.issues.length) {
      const e = new (params?.Err ?? _Err)(result.issues.map((iss) => finalizeIssue(iss, ctx, config())));
      captureStackTrace(e, params?.callee);
      throw e;
    }
    return result.value;
  };
  var _safeParse = (_Err) => (schema, value, _ctx) => {
    const ctx = _ctx ? { ..._ctx, async: false } : { async: false };
    const result = schema._zod.run({ value, issues: [] }, ctx);
    if (result instanceof Promise) {
      throw new $ZodAsyncError();
    }
    return result.issues.length ? {
      success: false,
      error: new (_Err ?? $ZodError)(result.issues.map((iss) => finalizeIssue(iss, ctx, config())))
    } : { success: true, data: result.value };
  };
  var safeParse = /* @__PURE__ */ _safeParse($ZodRealError);
  var _safeParseAsync = (_Err) => async (schema, value, _ctx) => {
    const ctx = _ctx ? Object.assign(_ctx, { async: true }) : { async: true };
    let result = schema._zod.run({ value, issues: [] }, ctx);
    if (result instanceof Promise)
      result = await result;
    return result.issues.length ? {
      success: false,
      error: new _Err(result.issues.map((iss) => finalizeIssue(iss, ctx, config())))
    } : { success: true, data: result.value };
  };
  var safeParseAsync = /* @__PURE__ */ _safeParseAsync($ZodRealError);

  // node_modules/zod/v4/core/regexes.js
  var cuid = /^[cC][^\s-]{8,}$/;
  var cuid2 = /^[0-9a-z]+$/;
  var ulid = /^[0-9A-HJKMNP-TV-Za-hjkmnp-tv-z]{26}$/;
  var xid = /^[0-9a-vA-V]{20}$/;
  var ksuid = /^[A-Za-z0-9]{27}$/;
  var nanoid = /^[a-zA-Z0-9_-]{21}$/;
  var duration = /^P(?:(\d+W)|(?!.*W)(?=\d|T\d)(\d+Y)?(\d+M)?(\d+D)?(T(?=\d)(\d+H)?(\d+M)?(\d+([.,]\d+)?S)?)?)$/;
  var guid = /^([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})$/;
  var uuid = (version2) => {
    if (!version2)
      return /^([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-8][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}|00000000-0000-0000-0000-000000000000)$/;
    return new RegExp(`^([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-${version2}[0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12})$`);
  };
  var email = /^(?!\.)(?!.*\.\.)([A-Za-z0-9_'+\-\.]*)[A-Za-z0-9_+-]@([A-Za-z0-9][A-Za-z0-9\-]*\.)+[A-Za-z]{2,}$/;
  var _emoji = `^(\\p{Extended_Pictographic}|\\p{Emoji_Component})+$`;
  function emoji() {
    return new RegExp(_emoji, "u");
  }
  var ipv4 = /^(?:(?:25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9])\.){3}(?:25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9])$/;
  var ipv6 = /^(([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}|::|([0-9a-fA-F]{1,4})?::([0-9a-fA-F]{1,4}:?){0,6})$/;
  var cidrv4 = /^((25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9])\.){3}(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9])\/([0-9]|[1-2][0-9]|3[0-2])$/;
  var cidrv6 = /^(([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}|::|([0-9a-fA-F]{1,4})?::([0-9a-fA-F]{1,4}:?){0,6})\/(12[0-8]|1[01][0-9]|[1-9]?[0-9])$/;
  var base64 = /^$|^(?:[0-9a-zA-Z+/]{4})*(?:(?:[0-9a-zA-Z+/]{2}==)|(?:[0-9a-zA-Z+/]{3}=))?$/;
  var base64url = /^[A-Za-z0-9_-]*$/;
  var hostname = /^([a-zA-Z0-9-]+\.)*[a-zA-Z0-9-]+$/;
  var e164 = /^\+(?:[0-9]){6,14}[0-9]$/;
  var dateSource = `(?:(?:\\d\\d[2468][048]|\\d\\d[13579][26]|\\d\\d0[48]|[02468][048]00|[13579][26]00)-02-29|\\d{4}-(?:(?:0[13578]|1[02])-(?:0[1-9]|[12]\\d|3[01])|(?:0[469]|11)-(?:0[1-9]|[12]\\d|30)|(?:02)-(?:0[1-9]|1\\d|2[0-8])))`;
  var date = /* @__PURE__ */ new RegExp(`^${dateSource}$`);
  function timeSource(args) {
    const hhmm = `(?:[01]\\d|2[0-3]):[0-5]\\d`;
    const regex = typeof args.precision === "number" ? args.precision === -1 ? `${hhmm}` : args.precision === 0 ? `${hhmm}:[0-5]\\d` : `${hhmm}:[0-5]\\d\\.\\d{${args.precision}}` : `${hhmm}(?::[0-5]\\d(?:\\.\\d+)?)?`;
    return regex;
  }
  function time(args) {
    return new RegExp(`^${timeSource(args)}$`);
  }
  function datetime(args) {
    const time3 = timeSource({ precision: args.precision });
    const opts = ["Z"];
    if (args.local)
      opts.push("");
    if (args.offset)
      opts.push(`([+-]\\d{2}:\\d{2})`);
    const timeRegex = `${time3}(?:${opts.join("|")})`;
    return new RegExp(`^${dateSource}T(?:${timeRegex})$`);
  }
  var string = (params) => {
    const regex = params ? `[\\s\\S]{${params?.minimum ?? 0},${params?.maximum ?? ""}}` : `[\\s\\S]*`;
    return new RegExp(`^${regex}$`);
  };
  var integer = /^\d+$/;
  var number = /^-?\d+(?:\.\d+)?/i;
  var boolean = /true|false/i;
  var _null = /null/i;
  var lowercase = /^[^A-Z]*$/;
  var uppercase = /^[^a-z]*$/;

  // node_modules/zod/v4/core/checks.js
  var $ZodCheck = /* @__PURE__ */ $constructor("$ZodCheck", (inst, def) => {
    var _a;
    inst._zod ?? (inst._zod = {});
    inst._zod.def = def;
    (_a = inst._zod).onattach ?? (_a.onattach = []);
  });
  var numericOriginMap = {
    number: "number",
    bigint: "bigint",
    object: "date"
  };
  var $ZodCheckLessThan = /* @__PURE__ */ $constructor("$ZodCheckLessThan", (inst, def) => {
    $ZodCheck.init(inst, def);
    const origin = numericOriginMap[typeof def.value];
    inst._zod.onattach.push((inst2) => {
      const bag = inst2._zod.bag;
      const curr = (def.inclusive ? bag.maximum : bag.exclusiveMaximum) ?? Number.POSITIVE_INFINITY;
      if (def.value < curr) {
        if (def.inclusive)
          bag.maximum = def.value;
        else
          bag.exclusiveMaximum = def.value;
      }
    });
    inst._zod.check = (payload) => {
      if (def.inclusive ? payload.value <= def.value : payload.value < def.value) {
        return;
      }
      payload.issues.push({
        origin,
        code: "too_big",
        maximum: def.value,
        input: payload.value,
        inclusive: def.inclusive,
        inst,
        continue: !def.abort
      });
    };
  });
  var $ZodCheckGreaterThan = /* @__PURE__ */ $constructor("$ZodCheckGreaterThan", (inst, def) => {
    $ZodCheck.init(inst, def);
    const origin = numericOriginMap[typeof def.value];
    inst._zod.onattach.push((inst2) => {
      const bag = inst2._zod.bag;
      const curr = (def.inclusive ? bag.minimum : bag.exclusiveMinimum) ?? Number.NEGATIVE_INFINITY;
      if (def.value > curr) {
        if (def.inclusive)
          bag.minimum = def.value;
        else
          bag.exclusiveMinimum = def.value;
      }
    });
    inst._zod.check = (payload) => {
      if (def.inclusive ? payload.value >= def.value : payload.value > def.value) {
        return;
      }
      payload.issues.push({
        origin,
        code: "too_small",
        minimum: def.value,
        input: payload.value,
        inclusive: def.inclusive,
        inst,
        continue: !def.abort
      });
    };
  });
  var $ZodCheckMultipleOf = /* @__PURE__ */ $constructor("$ZodCheckMultipleOf", (inst, def) => {
    $ZodCheck.init(inst, def);
    inst._zod.onattach.push((inst2) => {
      var _a;
      (_a = inst2._zod.bag).multipleOf ?? (_a.multipleOf = def.value);
    });
    inst._zod.check = (payload) => {
      if (typeof payload.value !== typeof def.value)
        throw new Error("Cannot mix number and bigint in multiple_of check.");
      const isMultiple = typeof payload.value === "bigint" ? payload.value % def.value === BigInt(0) : floatSafeRemainder(payload.value, def.value) === 0;
      if (isMultiple)
        return;
      payload.issues.push({
        origin: typeof payload.value,
        code: "not_multiple_of",
        divisor: def.value,
        input: payload.value,
        inst,
        continue: !def.abort
      });
    };
  });
  var $ZodCheckNumberFormat = /* @__PURE__ */ $constructor("$ZodCheckNumberFormat", (inst, def) => {
    $ZodCheck.init(inst, def);
    def.format = def.format || "float64";
    const isInt = def.format?.includes("int");
    const origin = isInt ? "int" : "number";
    const [minimum, maximum] = NUMBER_FORMAT_RANGES[def.format];
    inst._zod.onattach.push((inst2) => {
      const bag = inst2._zod.bag;
      bag.format = def.format;
      bag.minimum = minimum;
      bag.maximum = maximum;
      if (isInt)
        bag.pattern = integer;
    });
    inst._zod.check = (payload) => {
      const input = payload.value;
      if (isInt) {
        if (!Number.isInteger(input)) {
          payload.issues.push({
            expected: origin,
            format: def.format,
            code: "invalid_type",
            input,
            inst
          });
          return;
        }
        if (!Number.isSafeInteger(input)) {
          if (input > 0) {
            payload.issues.push({
              input,
              code: "too_big",
              maximum: Number.MAX_SAFE_INTEGER,
              note: "Integers must be within the safe integer range.",
              inst,
              origin,
              continue: !def.abort
            });
          } else {
            payload.issues.push({
              input,
              code: "too_small",
              minimum: Number.MIN_SAFE_INTEGER,
              note: "Integers must be within the safe integer range.",
              inst,
              origin,
              continue: !def.abort
            });
          }
          return;
        }
      }
      if (input < minimum) {
        payload.issues.push({
          origin: "number",
          input,
          code: "too_small",
          minimum,
          inclusive: true,
          inst,
          continue: !def.abort
        });
      }
      if (input > maximum) {
        payload.issues.push({
          origin: "number",
          input,
          code: "too_big",
          maximum,
          inst
        });
      }
    };
  });
  var $ZodCheckMaxLength = /* @__PURE__ */ $constructor("$ZodCheckMaxLength", (inst, def) => {
    var _a;
    $ZodCheck.init(inst, def);
    (_a = inst._zod.def).when ?? (_a.when = (payload) => {
      const val = payload.value;
      return !nullish(val) && val.length !== void 0;
    });
    inst._zod.onattach.push((inst2) => {
      const curr = inst2._zod.bag.maximum ?? Number.POSITIVE_INFINITY;
      if (def.maximum < curr)
        inst2._zod.bag.maximum = def.maximum;
    });
    inst._zod.check = (payload) => {
      const input = payload.value;
      const length = input.length;
      if (length <= def.maximum)
        return;
      const origin = getLengthableOrigin(input);
      payload.issues.push({
        origin,
        code: "too_big",
        maximum: def.maximum,
        inclusive: true,
        input,
        inst,
        continue: !def.abort
      });
    };
  });
  var $ZodCheckMinLength = /* @__PURE__ */ $constructor("$ZodCheckMinLength", (inst, def) => {
    var _a;
    $ZodCheck.init(inst, def);
    (_a = inst._zod.def).when ?? (_a.when = (payload) => {
      const val = payload.value;
      return !nullish(val) && val.length !== void 0;
    });
    inst._zod.onattach.push((inst2) => {
      const curr = inst2._zod.bag.minimum ?? Number.NEGATIVE_INFINITY;
      if (def.minimum > curr)
        inst2._zod.bag.minimum = def.minimum;
    });
    inst._zod.check = (payload) => {
      const input = payload.value;
      const length = input.length;
      if (length >= def.minimum)
        return;
      const origin = getLengthableOrigin(input);
      payload.issues.push({
        origin,
        code: "too_small",
        minimum: def.minimum,
        inclusive: true,
        input,
        inst,
        continue: !def.abort
      });
    };
  });
  var $ZodCheckLengthEquals = /* @__PURE__ */ $constructor("$ZodCheckLengthEquals", (inst, def) => {
    var _a;
    $ZodCheck.init(inst, def);
    (_a = inst._zod.def).when ?? (_a.when = (payload) => {
      const val = payload.value;
      return !nullish(val) && val.length !== void 0;
    });
    inst._zod.onattach.push((inst2) => {
      const bag = inst2._zod.bag;
      bag.minimum = def.length;
      bag.maximum = def.length;
      bag.length = def.length;
    });
    inst._zod.check = (payload) => {
      const input = payload.value;
      const length = input.length;
      if (length === def.length)
        return;
      const origin = getLengthableOrigin(input);
      const tooBig = length > def.length;
      payload.issues.push({
        origin,
        ...tooBig ? { code: "too_big", maximum: def.length } : { code: "too_small", minimum: def.length },
        inclusive: true,
        exact: true,
        input: payload.value,
        inst,
        continue: !def.abort
      });
    };
  });
  var $ZodCheckStringFormat = /* @__PURE__ */ $constructor("$ZodCheckStringFormat", (inst, def) => {
    var _a, _b;
    $ZodCheck.init(inst, def);
    inst._zod.onattach.push((inst2) => {
      const bag = inst2._zod.bag;
      bag.format = def.format;
      if (def.pattern) {
        bag.patterns ?? (bag.patterns = /* @__PURE__ */ new Set());
        bag.patterns.add(def.pattern);
      }
    });
    if (def.pattern)
      (_a = inst._zod).check ?? (_a.check = (payload) => {
        def.pattern.lastIndex = 0;
        if (def.pattern.test(payload.value))
          return;
        payload.issues.push({
          origin: "string",
          code: "invalid_format",
          format: def.format,
          input: payload.value,
          ...def.pattern ? { pattern: def.pattern.toString() } : {},
          inst,
          continue: !def.abort
        });
      });
    else
      (_b = inst._zod).check ?? (_b.check = () => {
      });
  });
  var $ZodCheckRegex = /* @__PURE__ */ $constructor("$ZodCheckRegex", (inst, def) => {
    $ZodCheckStringFormat.init(inst, def);
    inst._zod.check = (payload) => {
      def.pattern.lastIndex = 0;
      if (def.pattern.test(payload.value))
        return;
      payload.issues.push({
        origin: "string",
        code: "invalid_format",
        format: "regex",
        input: payload.value,
        pattern: def.pattern.toString(),
        inst,
        continue: !def.abort
      });
    };
  });
  var $ZodCheckLowerCase = /* @__PURE__ */ $constructor("$ZodCheckLowerCase", (inst, def) => {
    def.pattern ?? (def.pattern = lowercase);
    $ZodCheckStringFormat.init(inst, def);
  });
  var $ZodCheckUpperCase = /* @__PURE__ */ $constructor("$ZodCheckUpperCase", (inst, def) => {
    def.pattern ?? (def.pattern = uppercase);
    $ZodCheckStringFormat.init(inst, def);
  });
  var $ZodCheckIncludes = /* @__PURE__ */ $constructor("$ZodCheckIncludes", (inst, def) => {
    $ZodCheck.init(inst, def);
    const escapedRegex = escapeRegex(def.includes);
    const pattern = new RegExp(typeof def.position === "number" ? `^.{${def.position}}${escapedRegex}` : escapedRegex);
    def.pattern = pattern;
    inst._zod.onattach.push((inst2) => {
      const bag = inst2._zod.bag;
      bag.patterns ?? (bag.patterns = /* @__PURE__ */ new Set());
      bag.patterns.add(pattern);
    });
    inst._zod.check = (payload) => {
      if (payload.value.includes(def.includes, def.position))
        return;
      payload.issues.push({
        origin: "string",
        code: "invalid_format",
        format: "includes",
        includes: def.includes,
        input: payload.value,
        inst,
        continue: !def.abort
      });
    };
  });
  var $ZodCheckStartsWith = /* @__PURE__ */ $constructor("$ZodCheckStartsWith", (inst, def) => {
    $ZodCheck.init(inst, def);
    const pattern = new RegExp(`^${escapeRegex(def.prefix)}.*`);
    def.pattern ?? (def.pattern = pattern);
    inst._zod.onattach.push((inst2) => {
      const bag = inst2._zod.bag;
      bag.patterns ?? (bag.patterns = /* @__PURE__ */ new Set());
      bag.patterns.add(pattern);
    });
    inst._zod.check = (payload) => {
      if (payload.value.startsWith(def.prefix))
        return;
      payload.issues.push({
        origin: "string",
        code: "invalid_format",
        format: "starts_with",
        prefix: def.prefix,
        input: payload.value,
        inst,
        continue: !def.abort
      });
    };
  });
  var $ZodCheckEndsWith = /* @__PURE__ */ $constructor("$ZodCheckEndsWith", (inst, def) => {
    $ZodCheck.init(inst, def);
    const pattern = new RegExp(`.*${escapeRegex(def.suffix)}$`);
    def.pattern ?? (def.pattern = pattern);
    inst._zod.onattach.push((inst2) => {
      const bag = inst2._zod.bag;
      bag.patterns ?? (bag.patterns = /* @__PURE__ */ new Set());
      bag.patterns.add(pattern);
    });
    inst._zod.check = (payload) => {
      if (payload.value.endsWith(def.suffix))
        return;
      payload.issues.push({
        origin: "string",
        code: "invalid_format",
        format: "ends_with",
        suffix: def.suffix,
        input: payload.value,
        inst,
        continue: !def.abort
      });
    };
  });
  var $ZodCheckOverwrite = /* @__PURE__ */ $constructor("$ZodCheckOverwrite", (inst, def) => {
    $ZodCheck.init(inst, def);
    inst._zod.check = (payload) => {
      payload.value = def.tx(payload.value);
    };
  });

  // node_modules/zod/v4/core/doc.js
  var Doc = class {
    constructor(args = []) {
      this.content = [];
      this.indent = 0;
      if (this)
        this.args = args;
    }
    indented(fn2) {
      this.indent += 1;
      fn2(this);
      this.indent -= 1;
    }
    write(arg) {
      if (typeof arg === "function") {
        arg(this, { execution: "sync" });
        arg(this, { execution: "async" });
        return;
      }
      const content = arg;
      const lines = content.split("\n").filter((x2) => x2);
      const minIndent = Math.min(...lines.map((x2) => x2.length - x2.trimStart().length));
      const dedented = lines.map((x2) => x2.slice(minIndent)).map((x2) => " ".repeat(this.indent * 2) + x2);
      for (const line of dedented) {
        this.content.push(line);
      }
    }
    compile() {
      const F2 = Function;
      const args = this?.args;
      const content = this?.content ?? [``];
      const lines = [...content.map((x2) => `  ${x2}`)];
      return new F2(...args, lines.join("\n"));
    }
  };

  // node_modules/zod/v4/core/versions.js
  var version = {
    major: 4,
    minor: 0,
    patch: 0
  };

  // node_modules/zod/v4/core/schemas.js
  var $ZodType = /* @__PURE__ */ $constructor("$ZodType", (inst, def) => {
    var _a;
    inst ?? (inst = {});
    inst._zod.def = def;
    inst._zod.bag = inst._zod.bag || {};
    inst._zod.version = version;
    const checks = [...inst._zod.def.checks ?? []];
    if (inst._zod.traits.has("$ZodCheck")) {
      checks.unshift(inst);
    }
    for (const ch of checks) {
      for (const fn2 of ch._zod.onattach) {
        fn2(inst);
      }
    }
    if (checks.length === 0) {
      (_a = inst._zod).deferred ?? (_a.deferred = []);
      inst._zod.deferred?.push(() => {
        inst._zod.run = inst._zod.parse;
      });
    } else {
      const runChecks = (payload, checks2, ctx) => {
        let isAborted = aborted(payload);
        let asyncResult;
        for (const ch of checks2) {
          if (ch._zod.def.when) {
            const shouldRun = ch._zod.def.when(payload);
            if (!shouldRun)
              continue;
          } else if (isAborted) {
            continue;
          }
          const currLen = payload.issues.length;
          const _ = ch._zod.check(payload);
          if (_ instanceof Promise && ctx?.async === false) {
            throw new $ZodAsyncError();
          }
          if (asyncResult || _ instanceof Promise) {
            asyncResult = (asyncResult ?? Promise.resolve()).then(async () => {
              await _;
              const nextLen = payload.issues.length;
              if (nextLen === currLen)
                return;
              if (!isAborted)
                isAborted = aborted(payload, currLen);
            });
          } else {
            const nextLen = payload.issues.length;
            if (nextLen === currLen)
              continue;
            if (!isAborted)
              isAborted = aborted(payload, currLen);
          }
        }
        if (asyncResult) {
          return asyncResult.then(() => {
            return payload;
          });
        }
        return payload;
      };
      inst._zod.run = (payload, ctx) => {
        const result = inst._zod.parse(payload, ctx);
        if (result instanceof Promise) {
          if (ctx.async === false)
            throw new $ZodAsyncError();
          return result.then((result2) => runChecks(result2, checks, ctx));
        }
        return runChecks(result, checks, ctx);
      };
    }
    inst["~standard"] = {
      validate: (value) => {
        try {
          const r = safeParse(inst, value);
          return r.success ? { value: r.data } : { issues: r.error?.issues };
        } catch (_) {
          return safeParseAsync(inst, value).then((r) => r.success ? { value: r.data } : { issues: r.error?.issues });
        }
      },
      vendor: "zod",
      version: 1
    };
  });
  var $ZodString = /* @__PURE__ */ $constructor("$ZodString", (inst, def) => {
    $ZodType.init(inst, def);
    inst._zod.pattern = [...inst?._zod.bag?.patterns ?? []].pop() ?? string(inst._zod.bag);
    inst._zod.parse = (payload, _) => {
      if (def.coerce)
        try {
          payload.value = String(payload.value);
        } catch (_2) {
        }
      if (typeof payload.value === "string")
        return payload;
      payload.issues.push({
        expected: "string",
        code: "invalid_type",
        input: payload.value,
        inst
      });
      return payload;
    };
  });
  var $ZodStringFormat = /* @__PURE__ */ $constructor("$ZodStringFormat", (inst, def) => {
    $ZodCheckStringFormat.init(inst, def);
    $ZodString.init(inst, def);
  });
  var $ZodGUID = /* @__PURE__ */ $constructor("$ZodGUID", (inst, def) => {
    def.pattern ?? (def.pattern = guid);
    $ZodStringFormat.init(inst, def);
  });
  var $ZodUUID = /* @__PURE__ */ $constructor("$ZodUUID", (inst, def) => {
    if (def.version) {
      const versionMap = {
        v1: 1,
        v2: 2,
        v3: 3,
        v4: 4,
        v5: 5,
        v6: 6,
        v7: 7,
        v8: 8
      };
      const v = versionMap[def.version];
      if (v === void 0)
        throw new Error(`Invalid UUID version: "${def.version}"`);
      def.pattern ?? (def.pattern = uuid(v));
    } else
      def.pattern ?? (def.pattern = uuid());
    $ZodStringFormat.init(inst, def);
  });
  var $ZodEmail = /* @__PURE__ */ $constructor("$ZodEmail", (inst, def) => {
    def.pattern ?? (def.pattern = email);
    $ZodStringFormat.init(inst, def);
  });
  var $ZodURL = /* @__PURE__ */ $constructor("$ZodURL", (inst, def) => {
    $ZodStringFormat.init(inst, def);
    inst._zod.check = (payload) => {
      try {
        const orig = payload.value;
        const url = new URL(orig);
        const href = url.href;
        if (def.hostname) {
          def.hostname.lastIndex = 0;
          if (!def.hostname.test(url.hostname)) {
            payload.issues.push({
              code: "invalid_format",
              format: "url",
              note: "Invalid hostname",
              pattern: hostname.source,
              input: payload.value,
              inst,
              continue: !def.abort
            });
          }
        }
        if (def.protocol) {
          def.protocol.lastIndex = 0;
          if (!def.protocol.test(url.protocol.endsWith(":") ? url.protocol.slice(0, -1) : url.protocol)) {
            payload.issues.push({
              code: "invalid_format",
              format: "url",
              note: "Invalid protocol",
              pattern: def.protocol.source,
              input: payload.value,
              inst,
              continue: !def.abort
            });
          }
        }
        if (!orig.endsWith("/") && href.endsWith("/")) {
          payload.value = href.slice(0, -1);
        } else {
          payload.value = href;
        }
        return;
      } catch (_) {
        payload.issues.push({
          code: "invalid_format",
          format: "url",
          input: payload.value,
          inst,
          continue: !def.abort
        });
      }
    };
  });
  var $ZodEmoji = /* @__PURE__ */ $constructor("$ZodEmoji", (inst, def) => {
    def.pattern ?? (def.pattern = emoji());
    $ZodStringFormat.init(inst, def);
  });
  var $ZodNanoID = /* @__PURE__ */ $constructor("$ZodNanoID", (inst, def) => {
    def.pattern ?? (def.pattern = nanoid);
    $ZodStringFormat.init(inst, def);
  });
  var $ZodCUID = /* @__PURE__ */ $constructor("$ZodCUID", (inst, def) => {
    def.pattern ?? (def.pattern = cuid);
    $ZodStringFormat.init(inst, def);
  });
  var $ZodCUID2 = /* @__PURE__ */ $constructor("$ZodCUID2", (inst, def) => {
    def.pattern ?? (def.pattern = cuid2);
    $ZodStringFormat.init(inst, def);
  });
  var $ZodULID = /* @__PURE__ */ $constructor("$ZodULID", (inst, def) => {
    def.pattern ?? (def.pattern = ulid);
    $ZodStringFormat.init(inst, def);
  });
  var $ZodXID = /* @__PURE__ */ $constructor("$ZodXID", (inst, def) => {
    def.pattern ?? (def.pattern = xid);
    $ZodStringFormat.init(inst, def);
  });
  var $ZodKSUID = /* @__PURE__ */ $constructor("$ZodKSUID", (inst, def) => {
    def.pattern ?? (def.pattern = ksuid);
    $ZodStringFormat.init(inst, def);
  });
  var $ZodISODateTime = /* @__PURE__ */ $constructor("$ZodISODateTime", (inst, def) => {
    def.pattern ?? (def.pattern = datetime(def));
    $ZodStringFormat.init(inst, def);
  });
  var $ZodISODate = /* @__PURE__ */ $constructor("$ZodISODate", (inst, def) => {
    def.pattern ?? (def.pattern = date);
    $ZodStringFormat.init(inst, def);
  });
  var $ZodISOTime = /* @__PURE__ */ $constructor("$ZodISOTime", (inst, def) => {
    def.pattern ?? (def.pattern = time(def));
    $ZodStringFormat.init(inst, def);
  });
  var $ZodISODuration = /* @__PURE__ */ $constructor("$ZodISODuration", (inst, def) => {
    def.pattern ?? (def.pattern = duration);
    $ZodStringFormat.init(inst, def);
  });
  var $ZodIPv4 = /* @__PURE__ */ $constructor("$ZodIPv4", (inst, def) => {
    def.pattern ?? (def.pattern = ipv4);
    $ZodStringFormat.init(inst, def);
    inst._zod.onattach.push((inst2) => {
      const bag = inst2._zod.bag;
      bag.format = `ipv4`;
    });
  });
  var $ZodIPv6 = /* @__PURE__ */ $constructor("$ZodIPv6", (inst, def) => {
    def.pattern ?? (def.pattern = ipv6);
    $ZodStringFormat.init(inst, def);
    inst._zod.onattach.push((inst2) => {
      const bag = inst2._zod.bag;
      bag.format = `ipv6`;
    });
    inst._zod.check = (payload) => {
      try {
        new URL(`http://[${payload.value}]`);
      } catch {
        payload.issues.push({
          code: "invalid_format",
          format: "ipv6",
          input: payload.value,
          inst,
          continue: !def.abort
        });
      }
    };
  });
  var $ZodCIDRv4 = /* @__PURE__ */ $constructor("$ZodCIDRv4", (inst, def) => {
    def.pattern ?? (def.pattern = cidrv4);
    $ZodStringFormat.init(inst, def);
  });
  var $ZodCIDRv6 = /* @__PURE__ */ $constructor("$ZodCIDRv6", (inst, def) => {
    def.pattern ?? (def.pattern = cidrv6);
    $ZodStringFormat.init(inst, def);
    inst._zod.check = (payload) => {
      const [address, prefix] = payload.value.split("/");
      try {
        if (!prefix)
          throw new Error();
        const prefixNum = Number(prefix);
        if (`${prefixNum}` !== prefix)
          throw new Error();
        if (prefixNum < 0 || prefixNum > 128)
          throw new Error();
        new URL(`http://[${address}]`);
      } catch {
        payload.issues.push({
          code: "invalid_format",
          format: "cidrv6",
          input: payload.value,
          inst,
          continue: !def.abort
        });
      }
    };
  });
  function isValidBase64(data) {
    if (data === "")
      return true;
    if (data.length % 4 !== 0)
      return false;
    try {
      atob(data);
      return true;
    } catch {
      return false;
    }
  }
  var $ZodBase64 = /* @__PURE__ */ $constructor("$ZodBase64", (inst, def) => {
    def.pattern ?? (def.pattern = base64);
    $ZodStringFormat.init(inst, def);
    inst._zod.onattach.push((inst2) => {
      inst2._zod.bag.contentEncoding = "base64";
    });
    inst._zod.check = (payload) => {
      if (isValidBase64(payload.value))
        return;
      payload.issues.push({
        code: "invalid_format",
        format: "base64",
        input: payload.value,
        inst,
        continue: !def.abort
      });
    };
  });
  function isValidBase64URL(data) {
    if (!base64url.test(data))
      return false;
    const base642 = data.replace(/[-_]/g, (c2) => c2 === "-" ? "+" : "/");
    const padded = base642.padEnd(Math.ceil(base642.length / 4) * 4, "=");
    return isValidBase64(padded);
  }
  var $ZodBase64URL = /* @__PURE__ */ $constructor("$ZodBase64URL", (inst, def) => {
    def.pattern ?? (def.pattern = base64url);
    $ZodStringFormat.init(inst, def);
    inst._zod.onattach.push((inst2) => {
      inst2._zod.bag.contentEncoding = "base64url";
    });
    inst._zod.check = (payload) => {
      if (isValidBase64URL(payload.value))
        return;
      payload.issues.push({
        code: "invalid_format",
        format: "base64url",
        input: payload.value,
        inst,
        continue: !def.abort
      });
    };
  });
  var $ZodE164 = /* @__PURE__ */ $constructor("$ZodE164", (inst, def) => {
    def.pattern ?? (def.pattern = e164);
    $ZodStringFormat.init(inst, def);
  });
  function isValidJWT(token, algorithm = null) {
    try {
      const tokensParts = token.split(".");
      if (tokensParts.length !== 3)
        return false;
      const [header] = tokensParts;
      if (!header)
        return false;
      const parsedHeader = JSON.parse(atob(header));
      if ("typ" in parsedHeader && parsedHeader?.typ !== "JWT")
        return false;
      if (!parsedHeader.alg)
        return false;
      if (algorithm && (!("alg" in parsedHeader) || parsedHeader.alg !== algorithm))
        return false;
      return true;
    } catch {
      return false;
    }
  }
  var $ZodJWT = /* @__PURE__ */ $constructor("$ZodJWT", (inst, def) => {
    $ZodStringFormat.init(inst, def);
    inst._zod.check = (payload) => {
      if (isValidJWT(payload.value, def.alg))
        return;
      payload.issues.push({
        code: "invalid_format",
        format: "jwt",
        input: payload.value,
        inst,
        continue: !def.abort
      });
    };
  });
  var $ZodNumber = /* @__PURE__ */ $constructor("$ZodNumber", (inst, def) => {
    $ZodType.init(inst, def);
    inst._zod.pattern = inst._zod.bag.pattern ?? number;
    inst._zod.parse = (payload, _ctx) => {
      if (def.coerce)
        try {
          payload.value = Number(payload.value);
        } catch (_) {
        }
      const input = payload.value;
      if (typeof input === "number" && !Number.isNaN(input) && Number.isFinite(input)) {
        return payload;
      }
      const received = typeof input === "number" ? Number.isNaN(input) ? "NaN" : !Number.isFinite(input) ? "Infinity" : void 0 : void 0;
      payload.issues.push({
        expected: "number",
        code: "invalid_type",
        input,
        inst,
        ...received ? { received } : {}
      });
      return payload;
    };
  });
  var $ZodNumberFormat = /* @__PURE__ */ $constructor("$ZodNumber", (inst, def) => {
    $ZodCheckNumberFormat.init(inst, def);
    $ZodNumber.init(inst, def);
  });
  var $ZodBoolean = /* @__PURE__ */ $constructor("$ZodBoolean", (inst, def) => {
    $ZodType.init(inst, def);
    inst._zod.pattern = boolean;
    inst._zod.parse = (payload, _ctx) => {
      if (def.coerce)
        try {
          payload.value = Boolean(payload.value);
        } catch (_) {
        }
      const input = payload.value;
      if (typeof input === "boolean")
        return payload;
      payload.issues.push({
        expected: "boolean",
        code: "invalid_type",
        input,
        inst
      });
      return payload;
    };
  });
  var $ZodNull = /* @__PURE__ */ $constructor("$ZodNull", (inst, def) => {
    $ZodType.init(inst, def);
    inst._zod.pattern = _null;
    inst._zod.values = /* @__PURE__ */ new Set([null]);
    inst._zod.parse = (payload, _ctx) => {
      const input = payload.value;
      if (input === null)
        return payload;
      payload.issues.push({
        expected: "null",
        code: "invalid_type",
        input,
        inst
      });
      return payload;
    };
  });
  var $ZodUnknown = /* @__PURE__ */ $constructor("$ZodUnknown", (inst, def) => {
    $ZodType.init(inst, def);
    inst._zod.parse = (payload) => payload;
  });
  var $ZodNever = /* @__PURE__ */ $constructor("$ZodNever", (inst, def) => {
    $ZodType.init(inst, def);
    inst._zod.parse = (payload, _ctx) => {
      payload.issues.push({
        expected: "never",
        code: "invalid_type",
        input: payload.value,
        inst
      });
      return payload;
    };
  });
  function handleArrayResult(result, final, index) {
    if (result.issues.length) {
      final.issues.push(...prefixIssues(index, result.issues));
    }
    final.value[index] = result.value;
  }
  var $ZodArray = /* @__PURE__ */ $constructor("$ZodArray", (inst, def) => {
    $ZodType.init(inst, def);
    inst._zod.parse = (payload, ctx) => {
      const input = payload.value;
      if (!Array.isArray(input)) {
        payload.issues.push({
          expected: "array",
          code: "invalid_type",
          input,
          inst
        });
        return payload;
      }
      payload.value = Array(input.length);
      const proms = [];
      for (let i = 0; i < input.length; i++) {
        const item = input[i];
        const result = def.element._zod.run({
          value: item,
          issues: []
        }, ctx);
        if (result instanceof Promise) {
          proms.push(result.then((result2) => handleArrayResult(result2, payload, i)));
        } else {
          handleArrayResult(result, payload, i);
        }
      }
      if (proms.length) {
        return Promise.all(proms).then(() => payload);
      }
      return payload;
    };
  });
  function handleObjectResult(result, final, key) {
    if (result.issues.length) {
      final.issues.push(...prefixIssues(key, result.issues));
    }
    final.value[key] = result.value;
  }
  function handleOptionalObjectResult(result, final, key, input) {
    if (result.issues.length) {
      if (input[key] === void 0) {
        if (key in input) {
          final.value[key] = void 0;
        } else {
          final.value[key] = result.value;
        }
      } else {
        final.issues.push(...prefixIssues(key, result.issues));
      }
    } else if (result.value === void 0) {
      if (key in input)
        final.value[key] = void 0;
    } else {
      final.value[key] = result.value;
    }
  }
  var $ZodObject = /* @__PURE__ */ $constructor("$ZodObject", (inst, def) => {
    $ZodType.init(inst, def);
    const _normalized = cached(() => {
      const keys = Object.keys(def.shape);
      for (const k2 of keys) {
        if (!(def.shape[k2] instanceof $ZodType)) {
          throw new Error(`Invalid element at key "${k2}": expected a Zod schema`);
        }
      }
      const okeys = optionalKeys(def.shape);
      return {
        shape: def.shape,
        keys,
        keySet: new Set(keys),
        numKeys: keys.length,
        optionalKeys: new Set(okeys)
      };
    });
    defineLazy(inst._zod, "propValues", () => {
      const shape = def.shape;
      const propValues = {};
      for (const key in shape) {
        const field = shape[key]._zod;
        if (field.values) {
          propValues[key] ?? (propValues[key] = /* @__PURE__ */ new Set());
          for (const v of field.values)
            propValues[key].add(v);
        }
      }
      return propValues;
    });
    const generateFastpass = (shape) => {
      const doc = new Doc(["shape", "payload", "ctx"]);
      const normalized = _normalized.value;
      const parseStr = (key) => {
        const k2 = esc(key);
        return `shape[${k2}]._zod.run({ value: input[${k2}], issues: [] }, ctx)`;
      };
      doc.write(`const input = payload.value;`);
      const ids = /* @__PURE__ */ Object.create(null);
      let counter = 0;
      for (const key of normalized.keys) {
        ids[key] = `key_${counter++}`;
      }
      doc.write(`const newResult = {}`);
      for (const key of normalized.keys) {
        if (normalized.optionalKeys.has(key)) {
          const id = ids[key];
          doc.write(`const ${id} = ${parseStr(key)};`);
          const k2 = esc(key);
          doc.write(`
        if (${id}.issues.length) {
          if (input[${k2}] === undefined) {
            if (${k2} in input) {
              newResult[${k2}] = undefined;
            }
          } else {
            payload.issues = payload.issues.concat(
              ${id}.issues.map((iss) => ({
                ...iss,
                path: iss.path ? [${k2}, ...iss.path] : [${k2}],
              }))
            );
          }
        } else if (${id}.value === undefined) {
          if (${k2} in input) newResult[${k2}] = undefined;
        } else {
          newResult[${k2}] = ${id}.value;
        }
        `);
        } else {
          const id = ids[key];
          doc.write(`const ${id} = ${parseStr(key)};`);
          doc.write(`
          if (${id}.issues.length) payload.issues = payload.issues.concat(${id}.issues.map(iss => ({
            ...iss,
            path: iss.path ? [${esc(key)}, ...iss.path] : [${esc(key)}]
          })));`);
          doc.write(`newResult[${esc(key)}] = ${id}.value`);
        }
      }
      doc.write(`payload.value = newResult;`);
      doc.write(`return payload;`);
      const fn2 = doc.compile();
      return (payload, ctx) => fn2(shape, payload, ctx);
    };
    let fastpass;
    const isObject2 = isObject;
    const jit = !globalConfig.jitless;
    const allowsEval2 = allowsEval;
    const fastEnabled = jit && allowsEval2.value;
    const catchall = def.catchall;
    let value;
    inst._zod.parse = (payload, ctx) => {
      value ?? (value = _normalized.value);
      const input = payload.value;
      if (!isObject2(input)) {
        payload.issues.push({
          expected: "object",
          code: "invalid_type",
          input,
          inst
        });
        return payload;
      }
      const proms = [];
      if (jit && fastEnabled && ctx?.async === false && ctx.jitless !== true) {
        if (!fastpass)
          fastpass = generateFastpass(def.shape);
        payload = fastpass(payload, ctx);
      } else {
        payload.value = {};
        const shape = value.shape;
        for (const key of value.keys) {
          const el2 = shape[key];
          const r = el2._zod.run({ value: input[key], issues: [] }, ctx);
          const isOptional = el2._zod.optin === "optional" && el2._zod.optout === "optional";
          if (r instanceof Promise) {
            proms.push(r.then((r2) => isOptional ? handleOptionalObjectResult(r2, payload, key, input) : handleObjectResult(r2, payload, key)));
          } else if (isOptional) {
            handleOptionalObjectResult(r, payload, key, input);
          } else {
            handleObjectResult(r, payload, key);
          }
        }
      }
      if (!catchall) {
        return proms.length ? Promise.all(proms).then(() => payload) : payload;
      }
      const unrecognized = [];
      const keySet = value.keySet;
      const _catchall = catchall._zod;
      const t = _catchall.def.type;
      for (const key of Object.keys(input)) {
        if (keySet.has(key))
          continue;
        if (t === "never") {
          unrecognized.push(key);
          continue;
        }
        const r = _catchall.run({ value: input[key], issues: [] }, ctx);
        if (r instanceof Promise) {
          proms.push(r.then((r2) => handleObjectResult(r2, payload, key)));
        } else {
          handleObjectResult(r, payload, key);
        }
      }
      if (unrecognized.length) {
        payload.issues.push({
          code: "unrecognized_keys",
          keys: unrecognized,
          input,
          inst
        });
      }
      if (!proms.length)
        return payload;
      return Promise.all(proms).then(() => {
        return payload;
      });
    };
  });
  function handleUnionResults(results, final, inst, ctx) {
    for (const result of results) {
      if (result.issues.length === 0) {
        final.value = result.value;
        return final;
      }
    }
    final.issues.push({
      code: "invalid_union",
      input: final.value,
      inst,
      errors: results.map((result) => result.issues.map((iss) => finalizeIssue(iss, ctx, config())))
    });
    return final;
  }
  var $ZodUnion = /* @__PURE__ */ $constructor("$ZodUnion", (inst, def) => {
    $ZodType.init(inst, def);
    defineLazy(inst._zod, "optin", () => def.options.some((o) => o._zod.optin === "optional") ? "optional" : void 0);
    defineLazy(inst._zod, "optout", () => def.options.some((o) => o._zod.optout === "optional") ? "optional" : void 0);
    defineLazy(inst._zod, "values", () => {
      if (def.options.every((o) => o._zod.values)) {
        return new Set(def.options.flatMap((option) => Array.from(option._zod.values)));
      }
      return void 0;
    });
    defineLazy(inst._zod, "pattern", () => {
      if (def.options.every((o) => o._zod.pattern)) {
        const patterns = def.options.map((o) => o._zod.pattern);
        return new RegExp(`^(${patterns.map((p2) => cleanRegex(p2.source)).join("|")})$`);
      }
      return void 0;
    });
    inst._zod.parse = (payload, ctx) => {
      let async = false;
      const results = [];
      for (const option of def.options) {
        const result = option._zod.run({
          value: payload.value,
          issues: []
        }, ctx);
        if (result instanceof Promise) {
          results.push(result);
          async = true;
        } else {
          if (result.issues.length === 0)
            return result;
          results.push(result);
        }
      }
      if (!async)
        return handleUnionResults(results, payload, inst, ctx);
      return Promise.all(results).then((results2) => {
        return handleUnionResults(results2, payload, inst, ctx);
      });
    };
  });
  var $ZodDiscriminatedUnion = /* @__PURE__ */ $constructor("$ZodDiscriminatedUnion", (inst, def) => {
    $ZodUnion.init(inst, def);
    const _super = inst._zod.parse;
    defineLazy(inst._zod, "propValues", () => {
      const propValues = {};
      for (const option of def.options) {
        const pv2 = option._zod.propValues;
        if (!pv2 || Object.keys(pv2).length === 0)
          throw new Error(`Invalid discriminated union option at index "${def.options.indexOf(option)}"`);
        for (const [k2, v] of Object.entries(pv2)) {
          if (!propValues[k2])
            propValues[k2] = /* @__PURE__ */ new Set();
          for (const val of v) {
            propValues[k2].add(val);
          }
        }
      }
      return propValues;
    });
    const disc = cached(() => {
      const opts = def.options;
      const map = /* @__PURE__ */ new Map();
      for (const o of opts) {
        const values = o._zod.propValues[def.discriminator];
        if (!values || values.size === 0)
          throw new Error(`Invalid discriminated union option at index "${def.options.indexOf(o)}"`);
        for (const v of values) {
          if (map.has(v)) {
            throw new Error(`Duplicate discriminator value "${String(v)}"`);
          }
          map.set(v, o);
        }
      }
      return map;
    });
    inst._zod.parse = (payload, ctx) => {
      const input = payload.value;
      if (!isObject(input)) {
        payload.issues.push({
          code: "invalid_type",
          expected: "object",
          input,
          inst
        });
        return payload;
      }
      const opt = disc.value.get(input?.[def.discriminator]);
      if (opt) {
        return opt._zod.run(payload, ctx);
      }
      if (def.unionFallback) {
        return _super(payload, ctx);
      }
      payload.issues.push({
        code: "invalid_union",
        errors: [],
        note: "No matching discriminator",
        input,
        path: [def.discriminator],
        inst
      });
      return payload;
    };
  });
  var $ZodIntersection = /* @__PURE__ */ $constructor("$ZodIntersection", (inst, def) => {
    $ZodType.init(inst, def);
    inst._zod.parse = (payload, ctx) => {
      const input = payload.value;
      const left = def.left._zod.run({ value: input, issues: [] }, ctx);
      const right = def.right._zod.run({ value: input, issues: [] }, ctx);
      const async = left instanceof Promise || right instanceof Promise;
      if (async) {
        return Promise.all([left, right]).then(([left2, right2]) => {
          return handleIntersectionResults(payload, left2, right2);
        });
      }
      return handleIntersectionResults(payload, left, right);
    };
  });
  function mergeValues(a2, b2) {
    if (a2 === b2) {
      return { valid: true, data: a2 };
    }
    if (a2 instanceof Date && b2 instanceof Date && +a2 === +b2) {
      return { valid: true, data: a2 };
    }
    if (isPlainObject(a2) && isPlainObject(b2)) {
      const bKeys = Object.keys(b2);
      const sharedKeys = Object.keys(a2).filter((key) => bKeys.indexOf(key) !== -1);
      const newObj = { ...a2, ...b2 };
      for (const key of sharedKeys) {
        const sharedValue = mergeValues(a2[key], b2[key]);
        if (!sharedValue.valid) {
          return {
            valid: false,
            mergeErrorPath: [key, ...sharedValue.mergeErrorPath]
          };
        }
        newObj[key] = sharedValue.data;
      }
      return { valid: true, data: newObj };
    }
    if (Array.isArray(a2) && Array.isArray(b2)) {
      if (a2.length !== b2.length) {
        return { valid: false, mergeErrorPath: [] };
      }
      const newArray = [];
      for (let index = 0; index < a2.length; index++) {
        const itemA = a2[index];
        const itemB = b2[index];
        const sharedValue = mergeValues(itemA, itemB);
        if (!sharedValue.valid) {
          return {
            valid: false,
            mergeErrorPath: [index, ...sharedValue.mergeErrorPath]
          };
        }
        newArray.push(sharedValue.data);
      }
      return { valid: true, data: newArray };
    }
    return { valid: false, mergeErrorPath: [] };
  }
  function handleIntersectionResults(result, left, right) {
    if (left.issues.length) {
      result.issues.push(...left.issues);
    }
    if (right.issues.length) {
      result.issues.push(...right.issues);
    }
    if (aborted(result))
      return result;
    const merged = mergeValues(left.value, right.value);
    if (!merged.valid) {
      throw new Error(`Unmergable intersection. Error path: ${JSON.stringify(merged.mergeErrorPath)}`);
    }
    result.value = merged.data;
    return result;
  }
  var $ZodRecord = /* @__PURE__ */ $constructor("$ZodRecord", (inst, def) => {
    $ZodType.init(inst, def);
    inst._zod.parse = (payload, ctx) => {
      const input = payload.value;
      if (!isPlainObject(input)) {
        payload.issues.push({
          expected: "record",
          code: "invalid_type",
          input,
          inst
        });
        return payload;
      }
      const proms = [];
      if (def.keyType._zod.values) {
        const values = def.keyType._zod.values;
        payload.value = {};
        for (const key of values) {
          if (typeof key === "string" || typeof key === "number" || typeof key === "symbol") {
            const result = def.valueType._zod.run({ value: input[key], issues: [] }, ctx);
            if (result instanceof Promise) {
              proms.push(result.then((result2) => {
                if (result2.issues.length) {
                  payload.issues.push(...prefixIssues(key, result2.issues));
                }
                payload.value[key] = result2.value;
              }));
            } else {
              if (result.issues.length) {
                payload.issues.push(...prefixIssues(key, result.issues));
              }
              payload.value[key] = result.value;
            }
          }
        }
        let unrecognized;
        for (const key in input) {
          if (!values.has(key)) {
            unrecognized = unrecognized ?? [];
            unrecognized.push(key);
          }
        }
        if (unrecognized && unrecognized.length > 0) {
          payload.issues.push({
            code: "unrecognized_keys",
            input,
            inst,
            keys: unrecognized
          });
        }
      } else {
        payload.value = {};
        for (const key of Reflect.ownKeys(input)) {
          if (key === "__proto__")
            continue;
          const keyResult = def.keyType._zod.run({ value: key, issues: [] }, ctx);
          if (keyResult instanceof Promise) {
            throw new Error("Async schemas not supported in object keys currently");
          }
          if (keyResult.issues.length) {
            payload.issues.push({
              origin: "record",
              code: "invalid_key",
              issues: keyResult.issues.map((iss) => finalizeIssue(iss, ctx, config())),
              input: key,
              path: [key],
              inst
            });
            payload.value[keyResult.value] = keyResult.value;
            continue;
          }
          const result = def.valueType._zod.run({ value: input[key], issues: [] }, ctx);
          if (result instanceof Promise) {
            proms.push(result.then((result2) => {
              if (result2.issues.length) {
                payload.issues.push(...prefixIssues(key, result2.issues));
              }
              payload.value[keyResult.value] = result2.value;
            }));
          } else {
            if (result.issues.length) {
              payload.issues.push(...prefixIssues(key, result.issues));
            }
            payload.value[keyResult.value] = result.value;
          }
        }
      }
      if (proms.length) {
        return Promise.all(proms).then(() => payload);
      }
      return payload;
    };
  });
  var $ZodEnum = /* @__PURE__ */ $constructor("$ZodEnum", (inst, def) => {
    $ZodType.init(inst, def);
    const values = getEnumValues(def.entries);
    inst._zod.values = new Set(values);
    inst._zod.pattern = new RegExp(`^(${values.filter((k2) => propertyKeyTypes.has(typeof k2)).map((o) => typeof o === "string" ? escapeRegex(o) : o.toString()).join("|")})$`);
    inst._zod.parse = (payload, _ctx) => {
      const input = payload.value;
      if (inst._zod.values.has(input)) {
        return payload;
      }
      payload.issues.push({
        code: "invalid_value",
        values,
        input,
        inst
      });
      return payload;
    };
  });
  var $ZodLiteral = /* @__PURE__ */ $constructor("$ZodLiteral", (inst, def) => {
    $ZodType.init(inst, def);
    inst._zod.values = new Set(def.values);
    inst._zod.pattern = new RegExp(`^(${def.values.map((o) => typeof o === "string" ? escapeRegex(o) : o ? o.toString() : String(o)).join("|")})$`);
    inst._zod.parse = (payload, _ctx) => {
      const input = payload.value;
      if (inst._zod.values.has(input)) {
        return payload;
      }
      payload.issues.push({
        code: "invalid_value",
        values: def.values,
        input,
        inst
      });
      return payload;
    };
  });
  var $ZodTransform = /* @__PURE__ */ $constructor("$ZodTransform", (inst, def) => {
    $ZodType.init(inst, def);
    inst._zod.parse = (payload, _ctx) => {
      const _out = def.transform(payload.value, payload);
      if (_ctx.async) {
        const output = _out instanceof Promise ? _out : Promise.resolve(_out);
        return output.then((output2) => {
          payload.value = output2;
          return payload;
        });
      }
      if (_out instanceof Promise) {
        throw new $ZodAsyncError();
      }
      payload.value = _out;
      return payload;
    };
  });
  var $ZodOptional = /* @__PURE__ */ $constructor("$ZodOptional", (inst, def) => {
    $ZodType.init(inst, def);
    inst._zod.optin = "optional";
    inst._zod.optout = "optional";
    defineLazy(inst._zod, "values", () => {
      return def.innerType._zod.values ? /* @__PURE__ */ new Set([...def.innerType._zod.values, void 0]) : void 0;
    });
    defineLazy(inst._zod, "pattern", () => {
      const pattern = def.innerType._zod.pattern;
      return pattern ? new RegExp(`^(${cleanRegex(pattern.source)})?$`) : void 0;
    });
    inst._zod.parse = (payload, ctx) => {
      if (def.innerType._zod.optin === "optional") {
        return def.innerType._zod.run(payload, ctx);
      }
      if (payload.value === void 0) {
        return payload;
      }
      return def.innerType._zod.run(payload, ctx);
    };
  });
  var $ZodNullable = /* @__PURE__ */ $constructor("$ZodNullable", (inst, def) => {
    $ZodType.init(inst, def);
    defineLazy(inst._zod, "optin", () => def.innerType._zod.optin);
    defineLazy(inst._zod, "optout", () => def.innerType._zod.optout);
    defineLazy(inst._zod, "pattern", () => {
      const pattern = def.innerType._zod.pattern;
      return pattern ? new RegExp(`^(${cleanRegex(pattern.source)}|null)$`) : void 0;
    });
    defineLazy(inst._zod, "values", () => {
      return def.innerType._zod.values ? /* @__PURE__ */ new Set([...def.innerType._zod.values, null]) : void 0;
    });
    inst._zod.parse = (payload, ctx) => {
      if (payload.value === null)
        return payload;
      return def.innerType._zod.run(payload, ctx);
    };
  });
  var $ZodDefault = /* @__PURE__ */ $constructor("$ZodDefault", (inst, def) => {
    $ZodType.init(inst, def);
    inst._zod.optin = "optional";
    defineLazy(inst._zod, "values", () => def.innerType._zod.values);
    inst._zod.parse = (payload, ctx) => {
      if (payload.value === void 0) {
        payload.value = def.defaultValue;
        return payload;
      }
      const result = def.innerType._zod.run(payload, ctx);
      if (result instanceof Promise) {
        return result.then((result2) => handleDefaultResult(result2, def));
      }
      return handleDefaultResult(result, def);
    };
  });
  function handleDefaultResult(payload, def) {
    if (payload.value === void 0) {
      payload.value = def.defaultValue;
    }
    return payload;
  }
  var $ZodPrefault = /* @__PURE__ */ $constructor("$ZodPrefault", (inst, def) => {
    $ZodType.init(inst, def);
    inst._zod.optin = "optional";
    defineLazy(inst._zod, "values", () => def.innerType._zod.values);
    inst._zod.parse = (payload, ctx) => {
      if (payload.value === void 0) {
        payload.value = def.defaultValue;
      }
      return def.innerType._zod.run(payload, ctx);
    };
  });
  var $ZodNonOptional = /* @__PURE__ */ $constructor("$ZodNonOptional", (inst, def) => {
    $ZodType.init(inst, def);
    defineLazy(inst._zod, "values", () => {
      const v = def.innerType._zod.values;
      return v ? new Set([...v].filter((x2) => x2 !== void 0)) : void 0;
    });
    inst._zod.parse = (payload, ctx) => {
      const result = def.innerType._zod.run(payload, ctx);
      if (result instanceof Promise) {
        return result.then((result2) => handleNonOptionalResult(result2, inst));
      }
      return handleNonOptionalResult(result, inst);
    };
  });
  function handleNonOptionalResult(payload, inst) {
    if (!payload.issues.length && payload.value === void 0) {
      payload.issues.push({
        code: "invalid_type",
        expected: "nonoptional",
        input: payload.value,
        inst
      });
    }
    return payload;
  }
  var $ZodCatch = /* @__PURE__ */ $constructor("$ZodCatch", (inst, def) => {
    $ZodType.init(inst, def);
    inst._zod.optin = "optional";
    defineLazy(inst._zod, "optout", () => def.innerType._zod.optout);
    defineLazy(inst._zod, "values", () => def.innerType._zod.values);
    inst._zod.parse = (payload, ctx) => {
      const result = def.innerType._zod.run(payload, ctx);
      if (result instanceof Promise) {
        return result.then((result2) => {
          payload.value = result2.value;
          if (result2.issues.length) {
            payload.value = def.catchValue({
              ...payload,
              error: {
                issues: result2.issues.map((iss) => finalizeIssue(iss, ctx, config()))
              },
              input: payload.value
            });
            payload.issues = [];
          }
          return payload;
        });
      }
      payload.value = result.value;
      if (result.issues.length) {
        payload.value = def.catchValue({
          ...payload,
          error: {
            issues: result.issues.map((iss) => finalizeIssue(iss, ctx, config()))
          },
          input: payload.value
        });
        payload.issues = [];
      }
      return payload;
    };
  });
  var $ZodPipe = /* @__PURE__ */ $constructor("$ZodPipe", (inst, def) => {
    $ZodType.init(inst, def);
    defineLazy(inst._zod, "values", () => def.in._zod.values);
    defineLazy(inst._zod, "optin", () => def.in._zod.optin);
    defineLazy(inst._zod, "optout", () => def.out._zod.optout);
    inst._zod.parse = (payload, ctx) => {
      const left = def.in._zod.run(payload, ctx);
      if (left instanceof Promise) {
        return left.then((left2) => handlePipeResult(left2, def, ctx));
      }
      return handlePipeResult(left, def, ctx);
    };
  });
  function handlePipeResult(left, def, ctx) {
    if (aborted(left)) {
      return left;
    }
    return def.out._zod.run({ value: left.value, issues: left.issues }, ctx);
  }
  var $ZodReadonly = /* @__PURE__ */ $constructor("$ZodReadonly", (inst, def) => {
    $ZodType.init(inst, def);
    defineLazy(inst._zod, "propValues", () => def.innerType._zod.propValues);
    defineLazy(inst._zod, "values", () => def.innerType._zod.values);
    defineLazy(inst._zod, "optin", () => def.innerType._zod.optin);
    defineLazy(inst._zod, "optout", () => def.innerType._zod.optout);
    inst._zod.parse = (payload, ctx) => {
      const result = def.innerType._zod.run(payload, ctx);
      if (result instanceof Promise) {
        return result.then(handleReadonlyResult);
      }
      return handleReadonlyResult(result);
    };
  });
  function handleReadonlyResult(payload) {
    payload.value = Object.freeze(payload.value);
    return payload;
  }
  var $ZodCustom = /* @__PURE__ */ $constructor("$ZodCustom", (inst, def) => {
    $ZodCheck.init(inst, def);
    $ZodType.init(inst, def);
    inst._zod.parse = (payload, _) => {
      return payload;
    };
    inst._zod.check = (payload) => {
      const input = payload.value;
      const r = def.fn(input);
      if (r instanceof Promise) {
        return r.then((r2) => handleRefineResult(r2, payload, input, inst));
      }
      handleRefineResult(r, payload, input, inst);
      return;
    };
  });
  function handleRefineResult(result, payload, input, inst) {
    if (!result) {
      const _iss = {
        code: "custom",
        input,
        inst,
        // incorporates params.error into issue reporting
        path: [...inst._zod.def.path ?? []],
        // incorporates params.error into issue reporting
        continue: !inst._zod.def.abort
        // params: inst._zod.def.params,
      };
      if (inst._zod.def.params)
        _iss.params = inst._zod.def.params;
      payload.issues.push(issue(_iss));
    }
  }

  // node_modules/zod/v4/locales/en.js
  var parsedType = (data) => {
    const t = typeof data;
    switch (t) {
      case "number": {
        return Number.isNaN(data) ? "NaN" : "number";
      }
      case "object": {
        if (Array.isArray(data)) {
          return "array";
        }
        if (data === null) {
          return "null";
        }
        if (Object.getPrototypeOf(data) !== Object.prototype && data.constructor) {
          return data.constructor.name;
        }
      }
    }
    return t;
  };
  var error = () => {
    const Sizable = {
      string: { unit: "characters", verb: "to have" },
      file: { unit: "bytes", verb: "to have" },
      array: { unit: "items", verb: "to have" },
      set: { unit: "items", verb: "to have" }
    };
    function getSizing(origin) {
      return Sizable[origin] ?? null;
    }
    const Nouns = {
      regex: "input",
      email: "email address",
      url: "URL",
      emoji: "emoji",
      uuid: "UUID",
      uuidv4: "UUIDv4",
      uuidv6: "UUIDv6",
      nanoid: "nanoid",
      guid: "GUID",
      cuid: "cuid",
      cuid2: "cuid2",
      ulid: "ULID",
      xid: "XID",
      ksuid: "KSUID",
      datetime: "ISO datetime",
      date: "ISO date",
      time: "ISO time",
      duration: "ISO duration",
      ipv4: "IPv4 address",
      ipv6: "IPv6 address",
      cidrv4: "IPv4 range",
      cidrv6: "IPv6 range",
      base64: "base64-encoded string",
      base64url: "base64url-encoded string",
      json_string: "JSON string",
      e164: "E.164 number",
      jwt: "JWT",
      template_literal: "input"
    };
    return (issue2) => {
      switch (issue2.code) {
        case "invalid_type":
          return `Invalid input: expected ${issue2.expected}, received ${parsedType(issue2.input)}`;
        case "invalid_value":
          if (issue2.values.length === 1)
            return `Invalid input: expected ${stringifyPrimitive(issue2.values[0])}`;
          return `Invalid option: expected one of ${joinValues(issue2.values, "|")}`;
        case "too_big": {
          const adj = issue2.inclusive ? "<=" : "<";
          const sizing = getSizing(issue2.origin);
          if (sizing)
            return `Too big: expected ${issue2.origin ?? "value"} to have ${adj}${issue2.maximum.toString()} ${sizing.unit ?? "elements"}`;
          return `Too big: expected ${issue2.origin ?? "value"} to be ${adj}${issue2.maximum.toString()}`;
        }
        case "too_small": {
          const adj = issue2.inclusive ? ">=" : ">";
          const sizing = getSizing(issue2.origin);
          if (sizing) {
            return `Too small: expected ${issue2.origin} to have ${adj}${issue2.minimum.toString()} ${sizing.unit}`;
          }
          return `Too small: expected ${issue2.origin} to be ${adj}${issue2.minimum.toString()}`;
        }
        case "invalid_format": {
          const _issue = issue2;
          if (_issue.format === "starts_with") {
            return `Invalid string: must start with "${_issue.prefix}"`;
          }
          if (_issue.format === "ends_with")
            return `Invalid string: must end with "${_issue.suffix}"`;
          if (_issue.format === "includes")
            return `Invalid string: must include "${_issue.includes}"`;
          if (_issue.format === "regex")
            return `Invalid string: must match pattern ${_issue.pattern}`;
          return `Invalid ${Nouns[_issue.format] ?? issue2.format}`;
        }
        case "not_multiple_of":
          return `Invalid number: must be a multiple of ${issue2.divisor}`;
        case "unrecognized_keys":
          return `Unrecognized key${issue2.keys.length > 1 ? "s" : ""}: ${joinValues(issue2.keys, ", ")}`;
        case "invalid_key":
          return `Invalid key in ${issue2.origin}`;
        case "invalid_union":
          return "Invalid input";
        case "invalid_element":
          return `Invalid value in ${issue2.origin}`;
        default:
          return `Invalid input`;
      }
    };
  };
  function en_default() {
    return {
      localeError: error()
    };
  }

  // node_modules/zod/v4/core/registries.js
  var $output = Symbol("ZodOutput");
  var $input = Symbol("ZodInput");
  var $ZodRegistry = class {
    constructor() {
      this._map = /* @__PURE__ */ new Map();
      this._idmap = /* @__PURE__ */ new Map();
    }
    add(schema, ..._meta) {
      const meta = _meta[0];
      this._map.set(schema, meta);
      if (meta && typeof meta === "object" && "id" in meta) {
        if (this._idmap.has(meta.id)) {
          throw new Error(`ID ${meta.id} already exists in the registry`);
        }
        this._idmap.set(meta.id, schema);
      }
      return this;
    }
    clear() {
      this._map = /* @__PURE__ */ new Map();
      this._idmap = /* @__PURE__ */ new Map();
      return this;
    }
    remove(schema) {
      const meta = this._map.get(schema);
      if (meta && typeof meta === "object" && "id" in meta) {
        this._idmap.delete(meta.id);
      }
      this._map.delete(schema);
      return this;
    }
    get(schema) {
      const p2 = schema._zod.parent;
      if (p2) {
        const pm = { ...this.get(p2) ?? {} };
        delete pm.id;
        return { ...pm, ...this._map.get(schema) };
      }
      return this._map.get(schema);
    }
    has(schema) {
      return this._map.has(schema);
    }
  };
  function registry() {
    return new $ZodRegistry();
  }
  var globalRegistry = /* @__PURE__ */ registry();

  // node_modules/zod/v4/core/api.js
  function _string(Class2, params) {
    return new Class2({
      type: "string",
      ...normalizeParams(params)
    });
  }
  function _email(Class2, params) {
    return new Class2({
      type: "string",
      format: "email",
      check: "string_format",
      abort: false,
      ...normalizeParams(params)
    });
  }
  function _guid(Class2, params) {
    return new Class2({
      type: "string",
      format: "guid",
      check: "string_format",
      abort: false,
      ...normalizeParams(params)
    });
  }
  function _uuid(Class2, params) {
    return new Class2({
      type: "string",
      format: "uuid",
      check: "string_format",
      abort: false,
      ...normalizeParams(params)
    });
  }
  function _uuidv4(Class2, params) {
    return new Class2({
      type: "string",
      format: "uuid",
      check: "string_format",
      abort: false,
      version: "v4",
      ...normalizeParams(params)
    });
  }
  function _uuidv6(Class2, params) {
    return new Class2({
      type: "string",
      format: "uuid",
      check: "string_format",
      abort: false,
      version: "v6",
      ...normalizeParams(params)
    });
  }
  function _uuidv7(Class2, params) {
    return new Class2({
      type: "string",
      format: "uuid",
      check: "string_format",
      abort: false,
      version: "v7",
      ...normalizeParams(params)
    });
  }
  function _url(Class2, params) {
    return new Class2({
      type: "string",
      format: "url",
      check: "string_format",
      abort: false,
      ...normalizeParams(params)
    });
  }
  function _emoji2(Class2, params) {
    return new Class2({
      type: "string",
      format: "emoji",
      check: "string_format",
      abort: false,
      ...normalizeParams(params)
    });
  }
  function _nanoid(Class2, params) {
    return new Class2({
      type: "string",
      format: "nanoid",
      check: "string_format",
      abort: false,
      ...normalizeParams(params)
    });
  }
  function _cuid(Class2, params) {
    return new Class2({
      type: "string",
      format: "cuid",
      check: "string_format",
      abort: false,
      ...normalizeParams(params)
    });
  }
  function _cuid2(Class2, params) {
    return new Class2({
      type: "string",
      format: "cuid2",
      check: "string_format",
      abort: false,
      ...normalizeParams(params)
    });
  }
  function _ulid(Class2, params) {
    return new Class2({
      type: "string",
      format: "ulid",
      check: "string_format",
      abort: false,
      ...normalizeParams(params)
    });
  }
  function _xid(Class2, params) {
    return new Class2({
      type: "string",
      format: "xid",
      check: "string_format",
      abort: false,
      ...normalizeParams(params)
    });
  }
  function _ksuid(Class2, params) {
    return new Class2({
      type: "string",
      format: "ksuid",
      check: "string_format",
      abort: false,
      ...normalizeParams(params)
    });
  }
  function _ipv4(Class2, params) {
    return new Class2({
      type: "string",
      format: "ipv4",
      check: "string_format",
      abort: false,
      ...normalizeParams(params)
    });
  }
  function _ipv6(Class2, params) {
    return new Class2({
      type: "string",
      format: "ipv6",
      check: "string_format",
      abort: false,
      ...normalizeParams(params)
    });
  }
  function _cidrv4(Class2, params) {
    return new Class2({
      type: "string",
      format: "cidrv4",
      check: "string_format",
      abort: false,
      ...normalizeParams(params)
    });
  }
  function _cidrv6(Class2, params) {
    return new Class2({
      type: "string",
      format: "cidrv6",
      check: "string_format",
      abort: false,
      ...normalizeParams(params)
    });
  }
  function _base64(Class2, params) {
    return new Class2({
      type: "string",
      format: "base64",
      check: "string_format",
      abort: false,
      ...normalizeParams(params)
    });
  }
  function _base64url(Class2, params) {
    return new Class2({
      type: "string",
      format: "base64url",
      check: "string_format",
      abort: false,
      ...normalizeParams(params)
    });
  }
  function _e164(Class2, params) {
    return new Class2({
      type: "string",
      format: "e164",
      check: "string_format",
      abort: false,
      ...normalizeParams(params)
    });
  }
  function _jwt(Class2, params) {
    return new Class2({
      type: "string",
      format: "jwt",
      check: "string_format",
      abort: false,
      ...normalizeParams(params)
    });
  }
  function _isoDateTime(Class2, params) {
    return new Class2({
      type: "string",
      format: "datetime",
      check: "string_format",
      offset: false,
      local: false,
      precision: null,
      ...normalizeParams(params)
    });
  }
  function _isoDate(Class2, params) {
    return new Class2({
      type: "string",
      format: "date",
      check: "string_format",
      ...normalizeParams(params)
    });
  }
  function _isoTime(Class2, params) {
    return new Class2({
      type: "string",
      format: "time",
      check: "string_format",
      precision: null,
      ...normalizeParams(params)
    });
  }
  function _isoDuration(Class2, params) {
    return new Class2({
      type: "string",
      format: "duration",
      check: "string_format",
      ...normalizeParams(params)
    });
  }
  function _number(Class2, params) {
    return new Class2({
      type: "number",
      checks: [],
      ...normalizeParams(params)
    });
  }
  function _int(Class2, params) {
    return new Class2({
      type: "number",
      check: "number_format",
      abort: false,
      format: "safeint",
      ...normalizeParams(params)
    });
  }
  function _boolean(Class2, params) {
    return new Class2({
      type: "boolean",
      ...normalizeParams(params)
    });
  }
  function _null2(Class2, params) {
    return new Class2({
      type: "null",
      ...normalizeParams(params)
    });
  }
  function _unknown(Class2) {
    return new Class2({
      type: "unknown"
    });
  }
  function _never(Class2, params) {
    return new Class2({
      type: "never",
      ...normalizeParams(params)
    });
  }
  function _lt(value, params) {
    return new $ZodCheckLessThan({
      check: "less_than",
      ...normalizeParams(params),
      value,
      inclusive: false
    });
  }
  function _lte(value, params) {
    return new $ZodCheckLessThan({
      check: "less_than",
      ...normalizeParams(params),
      value,
      inclusive: true
    });
  }
  function _gt(value, params) {
    return new $ZodCheckGreaterThan({
      check: "greater_than",
      ...normalizeParams(params),
      value,
      inclusive: false
    });
  }
  function _gte(value, params) {
    return new $ZodCheckGreaterThan({
      check: "greater_than",
      ...normalizeParams(params),
      value,
      inclusive: true
    });
  }
  function _multipleOf(value, params) {
    return new $ZodCheckMultipleOf({
      check: "multiple_of",
      ...normalizeParams(params),
      value
    });
  }
  function _maxLength(maximum, params) {
    const ch = new $ZodCheckMaxLength({
      check: "max_length",
      ...normalizeParams(params),
      maximum
    });
    return ch;
  }
  function _minLength(minimum, params) {
    return new $ZodCheckMinLength({
      check: "min_length",
      ...normalizeParams(params),
      minimum
    });
  }
  function _length(length, params) {
    return new $ZodCheckLengthEquals({
      check: "length_equals",
      ...normalizeParams(params),
      length
    });
  }
  function _regex(pattern, params) {
    return new $ZodCheckRegex({
      check: "string_format",
      format: "regex",
      ...normalizeParams(params),
      pattern
    });
  }
  function _lowercase(params) {
    return new $ZodCheckLowerCase({
      check: "string_format",
      format: "lowercase",
      ...normalizeParams(params)
    });
  }
  function _uppercase(params) {
    return new $ZodCheckUpperCase({
      check: "string_format",
      format: "uppercase",
      ...normalizeParams(params)
    });
  }
  function _includes(includes, params) {
    return new $ZodCheckIncludes({
      check: "string_format",
      format: "includes",
      ...normalizeParams(params),
      includes
    });
  }
  function _startsWith(prefix, params) {
    return new $ZodCheckStartsWith({
      check: "string_format",
      format: "starts_with",
      ...normalizeParams(params),
      prefix
    });
  }
  function _endsWith(suffix, params) {
    return new $ZodCheckEndsWith({
      check: "string_format",
      format: "ends_with",
      ...normalizeParams(params),
      suffix
    });
  }
  function _overwrite(tx) {
    return new $ZodCheckOverwrite({
      check: "overwrite",
      tx
    });
  }
  function _normalize(form) {
    return _overwrite((input) => input.normalize(form));
  }
  function _trim() {
    return _overwrite((input) => input.trim());
  }
  function _toLowerCase() {
    return _overwrite((input) => input.toLowerCase());
  }
  function _toUpperCase() {
    return _overwrite((input) => input.toUpperCase());
  }
  function _array(Class2, element, params) {
    return new Class2({
      type: "array",
      element,
      // get element() {
      //   return element;
      // },
      ...normalizeParams(params)
    });
  }
  function _custom(Class2, fn2, _params) {
    const norm = normalizeParams(_params);
    norm.abort ?? (norm.abort = true);
    const schema = new Class2({
      type: "custom",
      check: "custom",
      fn: fn2,
      ...norm
    });
    return schema;
  }
  function _refine(Class2, fn2, _params) {
    const schema = new Class2({
      type: "custom",
      check: "custom",
      fn: fn2,
      ...normalizeParams(_params)
    });
    return schema;
  }

  // node_modules/@modelcontextprotocol/sdk/dist/esm/server/zod-compat.js
  function isZ4Schema(s2) {
    const schema = s2;
    return !!schema._zod;
  }
  function safeParse2(schema, data) {
    if (isZ4Schema(schema)) {
      const result2 = safeParse(schema, data);
      return result2;
    }
    const v3Schema = schema;
    const result = v3Schema.safeParse(data);
    return result;
  }
  function getObjectShape(schema) {
    if (!schema)
      return void 0;
    let rawShape;
    if (isZ4Schema(schema)) {
      const v4Schema = schema;
      rawShape = v4Schema._zod?.def?.shape;
    } else {
      const v3Schema = schema;
      rawShape = v3Schema.shape;
    }
    if (!rawShape)
      return void 0;
    if (typeof rawShape === "function") {
      try {
        return rawShape();
      } catch {
        return void 0;
      }
    }
    return rawShape;
  }
  function getLiteralValue(schema) {
    if (isZ4Schema(schema)) {
      const v4Schema = schema;
      const def2 = v4Schema._zod?.def;
      if (def2) {
        if (def2.value !== void 0)
          return def2.value;
        if (Array.isArray(def2.values) && def2.values.length > 0) {
          return def2.values[0];
        }
      }
    }
    const v3Schema = schema;
    const def = v3Schema._def;
    if (def) {
      if (def.value !== void 0)
        return def.value;
      if (Array.isArray(def.values) && def.values.length > 0) {
        return def.values[0];
      }
    }
    const directValue = schema.value;
    if (directValue !== void 0)
      return directValue;
    return void 0;
  }

  // node_modules/zod/v4/classic/iso.js
  var iso_exports = {};
  __export(iso_exports, {
    ZodISODate: () => ZodISODate,
    ZodISODateTime: () => ZodISODateTime,
    ZodISODuration: () => ZodISODuration,
    ZodISOTime: () => ZodISOTime,
    date: () => date2,
    datetime: () => datetime2,
    duration: () => duration2,
    time: () => time2
  });
  var ZodISODateTime = /* @__PURE__ */ $constructor("ZodISODateTime", (inst, def) => {
    $ZodISODateTime.init(inst, def);
    ZodStringFormat.init(inst, def);
  });
  function datetime2(params) {
    return _isoDateTime(ZodISODateTime, params);
  }
  var ZodISODate = /* @__PURE__ */ $constructor("ZodISODate", (inst, def) => {
    $ZodISODate.init(inst, def);
    ZodStringFormat.init(inst, def);
  });
  function date2(params) {
    return _isoDate(ZodISODate, params);
  }
  var ZodISOTime = /* @__PURE__ */ $constructor("ZodISOTime", (inst, def) => {
    $ZodISOTime.init(inst, def);
    ZodStringFormat.init(inst, def);
  });
  function time2(params) {
    return _isoTime(ZodISOTime, params);
  }
  var ZodISODuration = /* @__PURE__ */ $constructor("ZodISODuration", (inst, def) => {
    $ZodISODuration.init(inst, def);
    ZodStringFormat.init(inst, def);
  });
  function duration2(params) {
    return _isoDuration(ZodISODuration, params);
  }

  // node_modules/zod/v4/classic/errors.js
  var initializer2 = (inst, issues) => {
    $ZodError.init(inst, issues);
    inst.name = "ZodError";
    Object.defineProperties(inst, {
      format: {
        value: (mapper) => formatError(inst, mapper)
        // enumerable: false,
      },
      flatten: {
        value: (mapper) => flattenError(inst, mapper)
        // enumerable: false,
      },
      addIssue: {
        value: (issue2) => inst.issues.push(issue2)
        // enumerable: false,
      },
      addIssues: {
        value: (issues2) => inst.issues.push(...issues2)
        // enumerable: false,
      },
      isEmpty: {
        get() {
          return inst.issues.length === 0;
        }
        // enumerable: false,
      }
    });
  };
  var ZodError = $constructor("ZodError", initializer2);
  var ZodRealError = $constructor("ZodError", initializer2, {
    Parent: Error
  });

  // node_modules/zod/v4/classic/parse.js
  var parse2 = /* @__PURE__ */ _parse(ZodRealError);
  var parseAsync2 = /* @__PURE__ */ _parseAsync(ZodRealError);
  var safeParse3 = /* @__PURE__ */ _safeParse(ZodRealError);
  var safeParseAsync2 = /* @__PURE__ */ _safeParseAsync(ZodRealError);

  // node_modules/zod/v4/classic/schemas.js
  var ZodType = /* @__PURE__ */ $constructor("ZodType", (inst, def) => {
    $ZodType.init(inst, def);
    inst.def = def;
    Object.defineProperty(inst, "_def", { value: def });
    inst.check = (...checks) => {
      return inst.clone(
        {
          ...def,
          checks: [
            ...def.checks ?? [],
            ...checks.map((ch) => typeof ch === "function" ? { _zod: { check: ch, def: { check: "custom" }, onattach: [] } } : ch)
          ]
        }
        // { parent: true }
      );
    };
    inst.clone = (def2, params) => clone(inst, def2, params);
    inst.brand = () => inst;
    inst.register = (reg, meta) => {
      reg.add(inst, meta);
      return inst;
    };
    inst.parse = (data, params) => parse2(inst, data, params, { callee: inst.parse });
    inst.safeParse = (data, params) => safeParse3(inst, data, params);
    inst.parseAsync = async (data, params) => parseAsync2(inst, data, params, { callee: inst.parseAsync });
    inst.safeParseAsync = async (data, params) => safeParseAsync2(inst, data, params);
    inst.spa = inst.safeParseAsync;
    inst.refine = (check2, params) => inst.check(refine(check2, params));
    inst.superRefine = (refinement) => inst.check(superRefine(refinement));
    inst.overwrite = (fn2) => inst.check(_overwrite(fn2));
    inst.optional = () => optional(inst);
    inst.nullable = () => nullable(inst);
    inst.nullish = () => optional(nullable(inst));
    inst.nonoptional = (params) => nonoptional(inst, params);
    inst.array = () => array(inst);
    inst.or = (arg) => union([inst, arg]);
    inst.and = (arg) => intersection(inst, arg);
    inst.transform = (tx) => pipe(inst, transform(tx));
    inst.default = (def2) => _default(inst, def2);
    inst.prefault = (def2) => prefault(inst, def2);
    inst.catch = (params) => _catch(inst, params);
    inst.pipe = (target) => pipe(inst, target);
    inst.readonly = () => readonly(inst);
    inst.describe = (description) => {
      const cl2 = inst.clone();
      globalRegistry.add(cl2, { description });
      return cl2;
    };
    Object.defineProperty(inst, "description", {
      get() {
        return globalRegistry.get(inst)?.description;
      },
      configurable: true
    });
    inst.meta = (...args) => {
      if (args.length === 0) {
        return globalRegistry.get(inst);
      }
      const cl2 = inst.clone();
      globalRegistry.add(cl2, args[0]);
      return cl2;
    };
    inst.isOptional = () => inst.safeParse(void 0).success;
    inst.isNullable = () => inst.safeParse(null).success;
    return inst;
  });
  var _ZodString = /* @__PURE__ */ $constructor("_ZodString", (inst, def) => {
    $ZodString.init(inst, def);
    ZodType.init(inst, def);
    const bag = inst._zod.bag;
    inst.format = bag.format ?? null;
    inst.minLength = bag.minimum ?? null;
    inst.maxLength = bag.maximum ?? null;
    inst.regex = (...args) => inst.check(_regex(...args));
    inst.includes = (...args) => inst.check(_includes(...args));
    inst.startsWith = (...args) => inst.check(_startsWith(...args));
    inst.endsWith = (...args) => inst.check(_endsWith(...args));
    inst.min = (...args) => inst.check(_minLength(...args));
    inst.max = (...args) => inst.check(_maxLength(...args));
    inst.length = (...args) => inst.check(_length(...args));
    inst.nonempty = (...args) => inst.check(_minLength(1, ...args));
    inst.lowercase = (params) => inst.check(_lowercase(params));
    inst.uppercase = (params) => inst.check(_uppercase(params));
    inst.trim = () => inst.check(_trim());
    inst.normalize = (...args) => inst.check(_normalize(...args));
    inst.toLowerCase = () => inst.check(_toLowerCase());
    inst.toUpperCase = () => inst.check(_toUpperCase());
  });
  var ZodString = /* @__PURE__ */ $constructor("ZodString", (inst, def) => {
    $ZodString.init(inst, def);
    _ZodString.init(inst, def);
    inst.email = (params) => inst.check(_email(ZodEmail, params));
    inst.url = (params) => inst.check(_url(ZodURL, params));
    inst.jwt = (params) => inst.check(_jwt(ZodJWT, params));
    inst.emoji = (params) => inst.check(_emoji2(ZodEmoji, params));
    inst.guid = (params) => inst.check(_guid(ZodGUID, params));
    inst.uuid = (params) => inst.check(_uuid(ZodUUID, params));
    inst.uuidv4 = (params) => inst.check(_uuidv4(ZodUUID, params));
    inst.uuidv6 = (params) => inst.check(_uuidv6(ZodUUID, params));
    inst.uuidv7 = (params) => inst.check(_uuidv7(ZodUUID, params));
    inst.nanoid = (params) => inst.check(_nanoid(ZodNanoID, params));
    inst.guid = (params) => inst.check(_guid(ZodGUID, params));
    inst.cuid = (params) => inst.check(_cuid(ZodCUID, params));
    inst.cuid2 = (params) => inst.check(_cuid2(ZodCUID2, params));
    inst.ulid = (params) => inst.check(_ulid(ZodULID, params));
    inst.base64 = (params) => inst.check(_base64(ZodBase64, params));
    inst.base64url = (params) => inst.check(_base64url(ZodBase64URL, params));
    inst.xid = (params) => inst.check(_xid(ZodXID, params));
    inst.ksuid = (params) => inst.check(_ksuid(ZodKSUID, params));
    inst.ipv4 = (params) => inst.check(_ipv4(ZodIPv4, params));
    inst.ipv6 = (params) => inst.check(_ipv6(ZodIPv6, params));
    inst.cidrv4 = (params) => inst.check(_cidrv4(ZodCIDRv4, params));
    inst.cidrv6 = (params) => inst.check(_cidrv6(ZodCIDRv6, params));
    inst.e164 = (params) => inst.check(_e164(ZodE164, params));
    inst.datetime = (params) => inst.check(datetime2(params));
    inst.date = (params) => inst.check(date2(params));
    inst.time = (params) => inst.check(time2(params));
    inst.duration = (params) => inst.check(duration2(params));
  });
  function string2(params) {
    return _string(ZodString, params);
  }
  var ZodStringFormat = /* @__PURE__ */ $constructor("ZodStringFormat", (inst, def) => {
    $ZodStringFormat.init(inst, def);
    _ZodString.init(inst, def);
  });
  var ZodEmail = /* @__PURE__ */ $constructor("ZodEmail", (inst, def) => {
    $ZodEmail.init(inst, def);
    ZodStringFormat.init(inst, def);
  });
  var ZodGUID = /* @__PURE__ */ $constructor("ZodGUID", (inst, def) => {
    $ZodGUID.init(inst, def);
    ZodStringFormat.init(inst, def);
  });
  var ZodUUID = /* @__PURE__ */ $constructor("ZodUUID", (inst, def) => {
    $ZodUUID.init(inst, def);
    ZodStringFormat.init(inst, def);
  });
  var ZodURL = /* @__PURE__ */ $constructor("ZodURL", (inst, def) => {
    $ZodURL.init(inst, def);
    ZodStringFormat.init(inst, def);
  });
  var ZodEmoji = /* @__PURE__ */ $constructor("ZodEmoji", (inst, def) => {
    $ZodEmoji.init(inst, def);
    ZodStringFormat.init(inst, def);
  });
  var ZodNanoID = /* @__PURE__ */ $constructor("ZodNanoID", (inst, def) => {
    $ZodNanoID.init(inst, def);
    ZodStringFormat.init(inst, def);
  });
  var ZodCUID = /* @__PURE__ */ $constructor("ZodCUID", (inst, def) => {
    $ZodCUID.init(inst, def);
    ZodStringFormat.init(inst, def);
  });
  var ZodCUID2 = /* @__PURE__ */ $constructor("ZodCUID2", (inst, def) => {
    $ZodCUID2.init(inst, def);
    ZodStringFormat.init(inst, def);
  });
  var ZodULID = /* @__PURE__ */ $constructor("ZodULID", (inst, def) => {
    $ZodULID.init(inst, def);
    ZodStringFormat.init(inst, def);
  });
  var ZodXID = /* @__PURE__ */ $constructor("ZodXID", (inst, def) => {
    $ZodXID.init(inst, def);
    ZodStringFormat.init(inst, def);
  });
  var ZodKSUID = /* @__PURE__ */ $constructor("ZodKSUID", (inst, def) => {
    $ZodKSUID.init(inst, def);
    ZodStringFormat.init(inst, def);
  });
  var ZodIPv4 = /* @__PURE__ */ $constructor("ZodIPv4", (inst, def) => {
    $ZodIPv4.init(inst, def);
    ZodStringFormat.init(inst, def);
  });
  var ZodIPv6 = /* @__PURE__ */ $constructor("ZodIPv6", (inst, def) => {
    $ZodIPv6.init(inst, def);
    ZodStringFormat.init(inst, def);
  });
  var ZodCIDRv4 = /* @__PURE__ */ $constructor("ZodCIDRv4", (inst, def) => {
    $ZodCIDRv4.init(inst, def);
    ZodStringFormat.init(inst, def);
  });
  var ZodCIDRv6 = /* @__PURE__ */ $constructor("ZodCIDRv6", (inst, def) => {
    $ZodCIDRv6.init(inst, def);
    ZodStringFormat.init(inst, def);
  });
  var ZodBase64 = /* @__PURE__ */ $constructor("ZodBase64", (inst, def) => {
    $ZodBase64.init(inst, def);
    ZodStringFormat.init(inst, def);
  });
  var ZodBase64URL = /* @__PURE__ */ $constructor("ZodBase64URL", (inst, def) => {
    $ZodBase64URL.init(inst, def);
    ZodStringFormat.init(inst, def);
  });
  var ZodE164 = /* @__PURE__ */ $constructor("ZodE164", (inst, def) => {
    $ZodE164.init(inst, def);
    ZodStringFormat.init(inst, def);
  });
  var ZodJWT = /* @__PURE__ */ $constructor("ZodJWT", (inst, def) => {
    $ZodJWT.init(inst, def);
    ZodStringFormat.init(inst, def);
  });
  var ZodNumber = /* @__PURE__ */ $constructor("ZodNumber", (inst, def) => {
    $ZodNumber.init(inst, def);
    ZodType.init(inst, def);
    inst.gt = (value, params) => inst.check(_gt(value, params));
    inst.gte = (value, params) => inst.check(_gte(value, params));
    inst.min = (value, params) => inst.check(_gte(value, params));
    inst.lt = (value, params) => inst.check(_lt(value, params));
    inst.lte = (value, params) => inst.check(_lte(value, params));
    inst.max = (value, params) => inst.check(_lte(value, params));
    inst.int = (params) => inst.check(int(params));
    inst.safe = (params) => inst.check(int(params));
    inst.positive = (params) => inst.check(_gt(0, params));
    inst.nonnegative = (params) => inst.check(_gte(0, params));
    inst.negative = (params) => inst.check(_lt(0, params));
    inst.nonpositive = (params) => inst.check(_lte(0, params));
    inst.multipleOf = (value, params) => inst.check(_multipleOf(value, params));
    inst.step = (value, params) => inst.check(_multipleOf(value, params));
    inst.finite = () => inst;
    const bag = inst._zod.bag;
    inst.minValue = Math.max(bag.minimum ?? Number.NEGATIVE_INFINITY, bag.exclusiveMinimum ?? Number.NEGATIVE_INFINITY) ?? null;
    inst.maxValue = Math.min(bag.maximum ?? Number.POSITIVE_INFINITY, bag.exclusiveMaximum ?? Number.POSITIVE_INFINITY) ?? null;
    inst.isInt = (bag.format ?? "").includes("int") || Number.isSafeInteger(bag.multipleOf ?? 0.5);
    inst.isFinite = true;
    inst.format = bag.format ?? null;
  });
  function number2(params) {
    return _number(ZodNumber, params);
  }
  var ZodNumberFormat = /* @__PURE__ */ $constructor("ZodNumberFormat", (inst, def) => {
    $ZodNumberFormat.init(inst, def);
    ZodNumber.init(inst, def);
  });
  function int(params) {
    return _int(ZodNumberFormat, params);
  }
  var ZodBoolean = /* @__PURE__ */ $constructor("ZodBoolean", (inst, def) => {
    $ZodBoolean.init(inst, def);
    ZodType.init(inst, def);
  });
  function boolean2(params) {
    return _boolean(ZodBoolean, params);
  }
  var ZodNull = /* @__PURE__ */ $constructor("ZodNull", (inst, def) => {
    $ZodNull.init(inst, def);
    ZodType.init(inst, def);
  });
  function _null3(params) {
    return _null2(ZodNull, params);
  }
  var ZodUnknown = /* @__PURE__ */ $constructor("ZodUnknown", (inst, def) => {
    $ZodUnknown.init(inst, def);
    ZodType.init(inst, def);
  });
  function unknown() {
    return _unknown(ZodUnknown);
  }
  var ZodNever = /* @__PURE__ */ $constructor("ZodNever", (inst, def) => {
    $ZodNever.init(inst, def);
    ZodType.init(inst, def);
  });
  function never(params) {
    return _never(ZodNever, params);
  }
  var ZodArray = /* @__PURE__ */ $constructor("ZodArray", (inst, def) => {
    $ZodArray.init(inst, def);
    ZodType.init(inst, def);
    inst.element = def.element;
    inst.min = (minLength, params) => inst.check(_minLength(minLength, params));
    inst.nonempty = (params) => inst.check(_minLength(1, params));
    inst.max = (maxLength, params) => inst.check(_maxLength(maxLength, params));
    inst.length = (len, params) => inst.check(_length(len, params));
    inst.unwrap = () => inst.element;
  });
  function array(element, params) {
    return _array(ZodArray, element, params);
  }
  var ZodObject = /* @__PURE__ */ $constructor("ZodObject", (inst, def) => {
    $ZodObject.init(inst, def);
    ZodType.init(inst, def);
    util_exports.defineLazy(inst, "shape", () => def.shape);
    inst.keyof = () => _enum(Object.keys(inst._zod.def.shape));
    inst.catchall = (catchall) => inst.clone({ ...inst._zod.def, catchall });
    inst.passthrough = () => inst.clone({ ...inst._zod.def, catchall: unknown() });
    inst.loose = () => inst.clone({ ...inst._zod.def, catchall: unknown() });
    inst.strict = () => inst.clone({ ...inst._zod.def, catchall: never() });
    inst.strip = () => inst.clone({ ...inst._zod.def, catchall: void 0 });
    inst.extend = (incoming) => {
      return util_exports.extend(inst, incoming);
    };
    inst.merge = (other) => util_exports.merge(inst, other);
    inst.pick = (mask) => util_exports.pick(inst, mask);
    inst.omit = (mask) => util_exports.omit(inst, mask);
    inst.partial = (...args) => util_exports.partial(ZodOptional, inst, args[0]);
    inst.required = (...args) => util_exports.required(ZodNonOptional, inst, args[0]);
  });
  function object2(shape, params) {
    const def = {
      type: "object",
      get shape() {
        util_exports.assignProp(this, "shape", { ...shape });
        return this.shape;
      },
      ...util_exports.normalizeParams(params)
    };
    return new ZodObject(def);
  }
  function looseObject(shape, params) {
    return new ZodObject({
      type: "object",
      get shape() {
        util_exports.assignProp(this, "shape", { ...shape });
        return this.shape;
      },
      catchall: unknown(),
      ...util_exports.normalizeParams(params)
    });
  }
  var ZodUnion = /* @__PURE__ */ $constructor("ZodUnion", (inst, def) => {
    $ZodUnion.init(inst, def);
    ZodType.init(inst, def);
    inst.options = def.options;
  });
  function union(options, params) {
    return new ZodUnion({
      type: "union",
      options,
      ...util_exports.normalizeParams(params)
    });
  }
  var ZodDiscriminatedUnion = /* @__PURE__ */ $constructor("ZodDiscriminatedUnion", (inst, def) => {
    ZodUnion.init(inst, def);
    $ZodDiscriminatedUnion.init(inst, def);
  });
  function discriminatedUnion(discriminator, options, params) {
    return new ZodDiscriminatedUnion({
      type: "union",
      options,
      discriminator,
      ...util_exports.normalizeParams(params)
    });
  }
  var ZodIntersection = /* @__PURE__ */ $constructor("ZodIntersection", (inst, def) => {
    $ZodIntersection.init(inst, def);
    ZodType.init(inst, def);
  });
  function intersection(left, right) {
    return new ZodIntersection({
      type: "intersection",
      left,
      right
    });
  }
  var ZodRecord = /* @__PURE__ */ $constructor("ZodRecord", (inst, def) => {
    $ZodRecord.init(inst, def);
    ZodType.init(inst, def);
    inst.keyType = def.keyType;
    inst.valueType = def.valueType;
  });
  function record(keyType, valueType, params) {
    return new ZodRecord({
      type: "record",
      keyType,
      valueType,
      ...util_exports.normalizeParams(params)
    });
  }
  var ZodEnum = /* @__PURE__ */ $constructor("ZodEnum", (inst, def) => {
    $ZodEnum.init(inst, def);
    ZodType.init(inst, def);
    inst.enum = def.entries;
    inst.options = Object.values(def.entries);
    const keys = new Set(Object.keys(def.entries));
    inst.extract = (values, params) => {
      const newEntries = {};
      for (const value of values) {
        if (keys.has(value)) {
          newEntries[value] = def.entries[value];
        } else
          throw new Error(`Key ${value} not found in enum`);
      }
      return new ZodEnum({
        ...def,
        checks: [],
        ...util_exports.normalizeParams(params),
        entries: newEntries
      });
    };
    inst.exclude = (values, params) => {
      const newEntries = { ...def.entries };
      for (const value of values) {
        if (keys.has(value)) {
          delete newEntries[value];
        } else
          throw new Error(`Key ${value} not found in enum`);
      }
      return new ZodEnum({
        ...def,
        checks: [],
        ...util_exports.normalizeParams(params),
        entries: newEntries
      });
    };
  });
  function _enum(values, params) {
    const entries = Array.isArray(values) ? Object.fromEntries(values.map((v) => [v, v])) : values;
    return new ZodEnum({
      type: "enum",
      entries,
      ...util_exports.normalizeParams(params)
    });
  }
  var ZodLiteral = /* @__PURE__ */ $constructor("ZodLiteral", (inst, def) => {
    $ZodLiteral.init(inst, def);
    ZodType.init(inst, def);
    inst.values = new Set(def.values);
    Object.defineProperty(inst, "value", {
      get() {
        if (def.values.length > 1) {
          throw new Error("This schema contains multiple valid literal values. Use `.values` instead.");
        }
        return def.values[0];
      }
    });
  });
  function literal(value, params) {
    return new ZodLiteral({
      type: "literal",
      values: Array.isArray(value) ? value : [value],
      ...util_exports.normalizeParams(params)
    });
  }
  var ZodTransform = /* @__PURE__ */ $constructor("ZodTransform", (inst, def) => {
    $ZodTransform.init(inst, def);
    ZodType.init(inst, def);
    inst._zod.parse = (payload, _ctx) => {
      payload.addIssue = (issue2) => {
        if (typeof issue2 === "string") {
          payload.issues.push(util_exports.issue(issue2, payload.value, def));
        } else {
          const _issue = issue2;
          if (_issue.fatal)
            _issue.continue = false;
          _issue.code ?? (_issue.code = "custom");
          _issue.input ?? (_issue.input = payload.value);
          _issue.inst ?? (_issue.inst = inst);
          _issue.continue ?? (_issue.continue = true);
          payload.issues.push(util_exports.issue(_issue));
        }
      };
      const output = def.transform(payload.value, payload);
      if (output instanceof Promise) {
        return output.then((output2) => {
          payload.value = output2;
          return payload;
        });
      }
      payload.value = output;
      return payload;
    };
  });
  function transform(fn2) {
    return new ZodTransform({
      type: "transform",
      transform: fn2
    });
  }
  var ZodOptional = /* @__PURE__ */ $constructor("ZodOptional", (inst, def) => {
    $ZodOptional.init(inst, def);
    ZodType.init(inst, def);
    inst.unwrap = () => inst._zod.def.innerType;
  });
  function optional(innerType) {
    return new ZodOptional({
      type: "optional",
      innerType
    });
  }
  var ZodNullable = /* @__PURE__ */ $constructor("ZodNullable", (inst, def) => {
    $ZodNullable.init(inst, def);
    ZodType.init(inst, def);
    inst.unwrap = () => inst._zod.def.innerType;
  });
  function nullable(innerType) {
    return new ZodNullable({
      type: "nullable",
      innerType
    });
  }
  var ZodDefault = /* @__PURE__ */ $constructor("ZodDefault", (inst, def) => {
    $ZodDefault.init(inst, def);
    ZodType.init(inst, def);
    inst.unwrap = () => inst._zod.def.innerType;
    inst.removeDefault = inst.unwrap;
  });
  function _default(innerType, defaultValue) {
    return new ZodDefault({
      type: "default",
      innerType,
      get defaultValue() {
        return typeof defaultValue === "function" ? defaultValue() : defaultValue;
      }
    });
  }
  var ZodPrefault = /* @__PURE__ */ $constructor("ZodPrefault", (inst, def) => {
    $ZodPrefault.init(inst, def);
    ZodType.init(inst, def);
    inst.unwrap = () => inst._zod.def.innerType;
  });
  function prefault(innerType, defaultValue) {
    return new ZodPrefault({
      type: "prefault",
      innerType,
      get defaultValue() {
        return typeof defaultValue === "function" ? defaultValue() : defaultValue;
      }
    });
  }
  var ZodNonOptional = /* @__PURE__ */ $constructor("ZodNonOptional", (inst, def) => {
    $ZodNonOptional.init(inst, def);
    ZodType.init(inst, def);
    inst.unwrap = () => inst._zod.def.innerType;
  });
  function nonoptional(innerType, params) {
    return new ZodNonOptional({
      type: "nonoptional",
      innerType,
      ...util_exports.normalizeParams(params)
    });
  }
  var ZodCatch = /* @__PURE__ */ $constructor("ZodCatch", (inst, def) => {
    $ZodCatch.init(inst, def);
    ZodType.init(inst, def);
    inst.unwrap = () => inst._zod.def.innerType;
    inst.removeCatch = inst.unwrap;
  });
  function _catch(innerType, catchValue) {
    return new ZodCatch({
      type: "catch",
      innerType,
      catchValue: typeof catchValue === "function" ? catchValue : () => catchValue
    });
  }
  var ZodPipe = /* @__PURE__ */ $constructor("ZodPipe", (inst, def) => {
    $ZodPipe.init(inst, def);
    ZodType.init(inst, def);
    inst.in = def.in;
    inst.out = def.out;
  });
  function pipe(in_, out) {
    return new ZodPipe({
      type: "pipe",
      in: in_,
      out
      // ...util.normalizeParams(params),
    });
  }
  var ZodReadonly = /* @__PURE__ */ $constructor("ZodReadonly", (inst, def) => {
    $ZodReadonly.init(inst, def);
    ZodType.init(inst, def);
  });
  function readonly(innerType) {
    return new ZodReadonly({
      type: "readonly",
      innerType
    });
  }
  var ZodCustom = /* @__PURE__ */ $constructor("ZodCustom", (inst, def) => {
    $ZodCustom.init(inst, def);
    ZodType.init(inst, def);
  });
  function check(fn2) {
    const ch = new $ZodCheck({
      check: "custom"
      // ...util.normalizeParams(params),
    });
    ch._zod.check = fn2;
    return ch;
  }
  function custom(fn2, _params) {
    return _custom(ZodCustom, fn2 ?? (() => true), _params);
  }
  function refine(fn2, _params = {}) {
    return _refine(ZodCustom, fn2, _params);
  }
  function superRefine(fn2) {
    const ch = check((payload) => {
      payload.addIssue = (issue2) => {
        if (typeof issue2 === "string") {
          payload.issues.push(util_exports.issue(issue2, payload.value, ch._zod.def));
        } else {
          const _issue = issue2;
          if (_issue.fatal)
            _issue.continue = false;
          _issue.code ?? (_issue.code = "custom");
          _issue.input ?? (_issue.input = payload.value);
          _issue.inst ?? (_issue.inst = ch);
          _issue.continue ?? (_issue.continue = !ch._zod.def.abort);
          payload.issues.push(util_exports.issue(_issue));
        }
      };
      return fn2(payload.value, payload);
    });
    return ch;
  }
  function preprocess(fn2, schema) {
    return pipe(transform(fn2), schema);
  }

  // node_modules/zod/v4/classic/external.js
  config(en_default());

  // node_modules/@modelcontextprotocol/sdk/dist/esm/types.js
  var RELATED_TASK_META_KEY = "io.modelcontextprotocol/related-task";
  var JSONRPC_VERSION = "2.0";
  var AssertObjectSchema = custom((v) => v !== null && (typeof v === "object" || typeof v === "function"));
  var ProgressTokenSchema = union([string2(), number2().int()]);
  var CursorSchema = string2();
  var TaskCreationParamsSchema = looseObject({
    /**
     * Time in milliseconds to keep task results available after completion.
     * If null, the task has unlimited lifetime until manually cleaned up.
     */
    ttl: union([number2(), _null3()]).optional(),
    /**
     * Time in milliseconds to wait between task status requests.
     */
    pollInterval: number2().optional()
  });
  var TaskMetadataSchema = object2({
    ttl: number2().optional()
  });
  var RelatedTaskMetadataSchema = object2({
    taskId: string2()
  });
  var RequestMetaSchema = looseObject({
    /**
     * If specified, the caller is requesting out-of-band progress notifications for this request (as represented by notifications/progress). The value of this parameter is an opaque token that will be attached to any subsequent notifications. The receiver is not obligated to provide these notifications.
     */
    progressToken: ProgressTokenSchema.optional(),
    /**
     * If specified, this request is related to the provided task.
     */
    [RELATED_TASK_META_KEY]: RelatedTaskMetadataSchema.optional()
  });
  var BaseRequestParamsSchema = object2({
    /**
     * See [General fields: `_meta`](/specification/draft/basic/index#meta) for notes on `_meta` usage.
     */
    _meta: RequestMetaSchema.optional()
  });
  var TaskAugmentedRequestParamsSchema = BaseRequestParamsSchema.extend({
    /**
     * If specified, the caller is requesting task-augmented execution for this request.
     * The request will return a CreateTaskResult immediately, and the actual result can be
     * retrieved later via tasks/result.
     *
     * Task augmentation is subject to capability negotiation - receivers MUST declare support
     * for task augmentation of specific request types in their capabilities.
     */
    task: TaskMetadataSchema.optional()
  });
  var isTaskAugmentedRequestParams = (value) => TaskAugmentedRequestParamsSchema.safeParse(value).success;
  var RequestSchema = object2({
    method: string2(),
    params: BaseRequestParamsSchema.loose().optional()
  });
  var NotificationsParamsSchema = object2({
    /**
     * See [MCP specification](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/47339c03c143bb4ec01a26e721a1b8fe66634ebe/docs/specification/draft/basic/index.mdx#general-fields)
     * for notes on _meta usage.
     */
    _meta: RequestMetaSchema.optional()
  });
  var NotificationSchema = object2({
    method: string2(),
    params: NotificationsParamsSchema.loose().optional()
  });
  var ResultSchema = looseObject({
    /**
     * See [MCP specification](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/47339c03c143bb4ec01a26e721a1b8fe66634ebe/docs/specification/draft/basic/index.mdx#general-fields)
     * for notes on _meta usage.
     */
    _meta: RequestMetaSchema.optional()
  });
  var RequestIdSchema = union([string2(), number2().int()]);
  var JSONRPCRequestSchema = object2({
    jsonrpc: literal(JSONRPC_VERSION),
    id: RequestIdSchema,
    ...RequestSchema.shape
  }).strict();
  var isJSONRPCRequest = (value) => JSONRPCRequestSchema.safeParse(value).success;
  var JSONRPCNotificationSchema = object2({
    jsonrpc: literal(JSONRPC_VERSION),
    ...NotificationSchema.shape
  }).strict();
  var isJSONRPCNotification = (value) => JSONRPCNotificationSchema.safeParse(value).success;
  var JSONRPCResultResponseSchema = object2({
    jsonrpc: literal(JSONRPC_VERSION),
    id: RequestIdSchema,
    result: ResultSchema
  }).strict();
  var isJSONRPCResultResponse = (value) => JSONRPCResultResponseSchema.safeParse(value).success;
  var ErrorCode;
  (function(ErrorCode2) {
    ErrorCode2[ErrorCode2["ConnectionClosed"] = -32e3] = "ConnectionClosed";
    ErrorCode2[ErrorCode2["RequestTimeout"] = -32001] = "RequestTimeout";
    ErrorCode2[ErrorCode2["ParseError"] = -32700] = "ParseError";
    ErrorCode2[ErrorCode2["InvalidRequest"] = -32600] = "InvalidRequest";
    ErrorCode2[ErrorCode2["MethodNotFound"] = -32601] = "MethodNotFound";
    ErrorCode2[ErrorCode2["InvalidParams"] = -32602] = "InvalidParams";
    ErrorCode2[ErrorCode2["InternalError"] = -32603] = "InternalError";
    ErrorCode2[ErrorCode2["UrlElicitationRequired"] = -32042] = "UrlElicitationRequired";
  })(ErrorCode || (ErrorCode = {}));
  var JSONRPCErrorResponseSchema = object2({
    jsonrpc: literal(JSONRPC_VERSION),
    id: RequestIdSchema.optional(),
    error: object2({
      /**
       * The error type that occurred.
       */
      code: number2().int(),
      /**
       * A short description of the error. The message SHOULD be limited to a concise single sentence.
       */
      message: string2(),
      /**
       * Additional information about the error. The value of this member is defined by the sender (e.g. detailed error information, nested errors etc.).
       */
      data: unknown().optional()
    })
  }).strict();
  var isJSONRPCErrorResponse = (value) => JSONRPCErrorResponseSchema.safeParse(value).success;
  var JSONRPCMessageSchema = union([
    JSONRPCRequestSchema,
    JSONRPCNotificationSchema,
    JSONRPCResultResponseSchema,
    JSONRPCErrorResponseSchema
  ]);
  var JSONRPCResponseSchema = union([JSONRPCResultResponseSchema, JSONRPCErrorResponseSchema]);
  var EmptyResultSchema = ResultSchema.strict();
  var CancelledNotificationParamsSchema = NotificationsParamsSchema.extend({
    /**
     * The ID of the request to cancel.
     *
     * This MUST correspond to the ID of a request previously issued in the same direction.
     */
    requestId: RequestIdSchema.optional(),
    /**
     * An optional string describing the reason for the cancellation. This MAY be logged or presented to the user.
     */
    reason: string2().optional()
  });
  var CancelledNotificationSchema = NotificationSchema.extend({
    method: literal("notifications/cancelled"),
    params: CancelledNotificationParamsSchema
  });
  var IconSchema = object2({
    /**
     * URL or data URI for the icon.
     */
    src: string2(),
    /**
     * Optional MIME type for the icon.
     */
    mimeType: string2().optional(),
    /**
     * Optional array of strings that specify sizes at which the icon can be used.
     * Each string should be in WxH format (e.g., `"48x48"`, `"96x96"`) or `"any"` for scalable formats like SVG.
     *
     * If not provided, the client should assume that the icon can be used at any size.
     */
    sizes: array(string2()).optional(),
    /**
     * Optional specifier for the theme this icon is designed for. `light` indicates
     * the icon is designed to be used with a light background, and `dark` indicates
     * the icon is designed to be used with a dark background.
     *
     * If not provided, the client should assume the icon can be used with any theme.
     */
    theme: _enum(["light", "dark"]).optional()
  });
  var IconsSchema = object2({
    /**
     * Optional set of sized icons that the client can display in a user interface.
     *
     * Clients that support rendering icons MUST support at least the following MIME types:
     * - `image/png` - PNG images (safe, universal compatibility)
     * - `image/jpeg` (and `image/jpg`) - JPEG images (safe, universal compatibility)
     *
     * Clients that support rendering icons SHOULD also support:
     * - `image/svg+xml` - SVG images (scalable but requires security precautions)
     * - `image/webp` - WebP images (modern, efficient format)
     */
    icons: array(IconSchema).optional()
  });
  var BaseMetadataSchema = object2({
    /** Intended for programmatic or logical use, but used as a display name in past specs or fallback */
    name: string2(),
    /**
     * Intended for UI and end-user contexts — optimized to be human-readable and easily understood,
     * even by those unfamiliar with domain-specific terminology.
     *
     * If not provided, the name should be used for display (except for Tool,
     * where `annotations.title` should be given precedence over using `name`,
     * if present).
     */
    title: string2().optional()
  });
  var ImplementationSchema = BaseMetadataSchema.extend({
    ...BaseMetadataSchema.shape,
    ...IconsSchema.shape,
    version: string2(),
    /**
     * An optional URL of the website for this implementation.
     */
    websiteUrl: string2().optional(),
    /**
     * An optional human-readable description of what this implementation does.
     *
     * This can be used by clients or servers to provide context about their purpose
     * and capabilities. For example, a server might describe the types of resources
     * or tools it provides, while a client might describe its intended use case.
     */
    description: string2().optional()
  });
  var FormElicitationCapabilitySchema = intersection(object2({
    applyDefaults: boolean2().optional()
  }), record(string2(), unknown()));
  var ElicitationCapabilitySchema = preprocess((value) => {
    if (value && typeof value === "object" && !Array.isArray(value)) {
      if (Object.keys(value).length === 0) {
        return { form: {} };
      }
    }
    return value;
  }, intersection(object2({
    form: FormElicitationCapabilitySchema.optional(),
    url: AssertObjectSchema.optional()
  }), record(string2(), unknown()).optional()));
  var ClientTasksCapabilitySchema = looseObject({
    /**
     * Present if the client supports listing tasks.
     */
    list: AssertObjectSchema.optional(),
    /**
     * Present if the client supports cancelling tasks.
     */
    cancel: AssertObjectSchema.optional(),
    /**
     * Capabilities for task creation on specific request types.
     */
    requests: looseObject({
      /**
       * Task support for sampling requests.
       */
      sampling: looseObject({
        createMessage: AssertObjectSchema.optional()
      }).optional(),
      /**
       * Task support for elicitation requests.
       */
      elicitation: looseObject({
        create: AssertObjectSchema.optional()
      }).optional()
    }).optional()
  });
  var ServerTasksCapabilitySchema = looseObject({
    /**
     * Present if the server supports listing tasks.
     */
    list: AssertObjectSchema.optional(),
    /**
     * Present if the server supports cancelling tasks.
     */
    cancel: AssertObjectSchema.optional(),
    /**
     * Capabilities for task creation on specific request types.
     */
    requests: looseObject({
      /**
       * Task support for tool requests.
       */
      tools: looseObject({
        call: AssertObjectSchema.optional()
      }).optional()
    }).optional()
  });
  var ClientCapabilitiesSchema = object2({
    /**
     * Experimental, non-standard capabilities that the client supports.
     */
    experimental: record(string2(), AssertObjectSchema).optional(),
    /**
     * Present if the client supports sampling from an LLM.
     */
    sampling: object2({
      /**
       * Present if the client supports context inclusion via includeContext parameter.
       * If not declared, servers SHOULD only use `includeContext: "none"` (or omit it).
       */
      context: AssertObjectSchema.optional(),
      /**
       * Present if the client supports tool use via tools and toolChoice parameters.
       */
      tools: AssertObjectSchema.optional()
    }).optional(),
    /**
     * Present if the client supports eliciting user input.
     */
    elicitation: ElicitationCapabilitySchema.optional(),
    /**
     * Present if the client supports listing roots.
     */
    roots: object2({
      /**
       * Whether the client supports issuing notifications for changes to the roots list.
       */
      listChanged: boolean2().optional()
    }).optional(),
    /**
     * Present if the client supports task creation.
     */
    tasks: ClientTasksCapabilitySchema.optional()
  });
  var InitializeRequestParamsSchema = BaseRequestParamsSchema.extend({
    /**
     * The latest version of the Model Context Protocol that the client supports. The client MAY decide to support older versions as well.
     */
    protocolVersion: string2(),
    capabilities: ClientCapabilitiesSchema,
    clientInfo: ImplementationSchema
  });
  var InitializeRequestSchema = RequestSchema.extend({
    method: literal("initialize"),
    params: InitializeRequestParamsSchema
  });
  var ServerCapabilitiesSchema = object2({
    /**
     * Experimental, non-standard capabilities that the server supports.
     */
    experimental: record(string2(), AssertObjectSchema).optional(),
    /**
     * Present if the server supports sending log messages to the client.
     */
    logging: AssertObjectSchema.optional(),
    /**
     * Present if the server supports sending completions to the client.
     */
    completions: AssertObjectSchema.optional(),
    /**
     * Present if the server offers any prompt templates.
     */
    prompts: object2({
      /**
       * Whether this server supports issuing notifications for changes to the prompt list.
       */
      listChanged: boolean2().optional()
    }).optional(),
    /**
     * Present if the server offers any resources to read.
     */
    resources: object2({
      /**
       * Whether this server supports clients subscribing to resource updates.
       */
      subscribe: boolean2().optional(),
      /**
       * Whether this server supports issuing notifications for changes to the resource list.
       */
      listChanged: boolean2().optional()
    }).optional(),
    /**
     * Present if the server offers any tools to call.
     */
    tools: object2({
      /**
       * Whether this server supports issuing notifications for changes to the tool list.
       */
      listChanged: boolean2().optional()
    }).optional(),
    /**
     * Present if the server supports task creation.
     */
    tasks: ServerTasksCapabilitySchema.optional()
  });
  var InitializeResultSchema = ResultSchema.extend({
    /**
     * The version of the Model Context Protocol that the server wants to use. This may not match the version that the client requested. If the client cannot support this version, it MUST disconnect.
     */
    protocolVersion: string2(),
    capabilities: ServerCapabilitiesSchema,
    serverInfo: ImplementationSchema,
    /**
     * Instructions describing how to use the server and its features.
     *
     * This can be used by clients to improve the LLM's understanding of available tools, resources, etc. It can be thought of like a "hint" to the model. For example, this information MAY be added to the system prompt.
     */
    instructions: string2().optional()
  });
  var InitializedNotificationSchema = NotificationSchema.extend({
    method: literal("notifications/initialized"),
    params: NotificationsParamsSchema.optional()
  });
  var PingRequestSchema = RequestSchema.extend({
    method: literal("ping"),
    params: BaseRequestParamsSchema.optional()
  });
  var ProgressSchema = object2({
    /**
     * The progress thus far. This should increase every time progress is made, even if the total is unknown.
     */
    progress: number2(),
    /**
     * Total number of items to process (or total progress required), if known.
     */
    total: optional(number2()),
    /**
     * An optional message describing the current progress.
     */
    message: optional(string2())
  });
  var ProgressNotificationParamsSchema = object2({
    ...NotificationsParamsSchema.shape,
    ...ProgressSchema.shape,
    /**
     * The progress token which was given in the initial request, used to associate this notification with the request that is proceeding.
     */
    progressToken: ProgressTokenSchema
  });
  var ProgressNotificationSchema = NotificationSchema.extend({
    method: literal("notifications/progress"),
    params: ProgressNotificationParamsSchema
  });
  var PaginatedRequestParamsSchema = BaseRequestParamsSchema.extend({
    /**
     * An opaque token representing the current pagination position.
     * If provided, the server should return results starting after this cursor.
     */
    cursor: CursorSchema.optional()
  });
  var PaginatedRequestSchema = RequestSchema.extend({
    params: PaginatedRequestParamsSchema.optional()
  });
  var PaginatedResultSchema = ResultSchema.extend({
    /**
     * An opaque token representing the pagination position after the last returned result.
     * If present, there may be more results available.
     */
    nextCursor: CursorSchema.optional()
  });
  var TaskStatusSchema = _enum(["working", "input_required", "completed", "failed", "cancelled"]);
  var TaskSchema = object2({
    taskId: string2(),
    status: TaskStatusSchema,
    /**
     * Time in milliseconds to keep task results available after completion.
     * If null, the task has unlimited lifetime until manually cleaned up.
     */
    ttl: union([number2(), _null3()]),
    /**
     * ISO 8601 timestamp when the task was created.
     */
    createdAt: string2(),
    /**
     * ISO 8601 timestamp when the task was last updated.
     */
    lastUpdatedAt: string2(),
    pollInterval: optional(number2()),
    /**
     * Optional diagnostic message for failed tasks or other status information.
     */
    statusMessage: optional(string2())
  });
  var CreateTaskResultSchema = ResultSchema.extend({
    task: TaskSchema
  });
  var TaskStatusNotificationParamsSchema = NotificationsParamsSchema.merge(TaskSchema);
  var TaskStatusNotificationSchema = NotificationSchema.extend({
    method: literal("notifications/tasks/status"),
    params: TaskStatusNotificationParamsSchema
  });
  var GetTaskRequestSchema = RequestSchema.extend({
    method: literal("tasks/get"),
    params: BaseRequestParamsSchema.extend({
      taskId: string2()
    })
  });
  var GetTaskResultSchema = ResultSchema.merge(TaskSchema);
  var GetTaskPayloadRequestSchema = RequestSchema.extend({
    method: literal("tasks/result"),
    params: BaseRequestParamsSchema.extend({
      taskId: string2()
    })
  });
  var GetTaskPayloadResultSchema = ResultSchema.loose();
  var ListTasksRequestSchema = PaginatedRequestSchema.extend({
    method: literal("tasks/list")
  });
  var ListTasksResultSchema = PaginatedResultSchema.extend({
    tasks: array(TaskSchema)
  });
  var CancelTaskRequestSchema = RequestSchema.extend({
    method: literal("tasks/cancel"),
    params: BaseRequestParamsSchema.extend({
      taskId: string2()
    })
  });
  var CancelTaskResultSchema = ResultSchema.merge(TaskSchema);
  var ResourceContentsSchema = object2({
    /**
     * The URI of this resource.
     */
    uri: string2(),
    /**
     * The MIME type of this resource, if known.
     */
    mimeType: optional(string2()),
    /**
     * See [MCP specification](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/47339c03c143bb4ec01a26e721a1b8fe66634ebe/docs/specification/draft/basic/index.mdx#general-fields)
     * for notes on _meta usage.
     */
    _meta: record(string2(), unknown()).optional()
  });
  var TextResourceContentsSchema = ResourceContentsSchema.extend({
    /**
     * The text of the item. This must only be set if the item can actually be represented as text (not binary data).
     */
    text: string2()
  });
  var Base64Schema = string2().refine((val) => {
    try {
      atob(val);
      return true;
    } catch {
      return false;
    }
  }, { message: "Invalid Base64 string" });
  var BlobResourceContentsSchema = ResourceContentsSchema.extend({
    /**
     * A base64-encoded string representing the binary data of the item.
     */
    blob: Base64Schema
  });
  var RoleSchema = _enum(["user", "assistant"]);
  var AnnotationsSchema = object2({
    /**
     * Intended audience(s) for the resource.
     */
    audience: array(RoleSchema).optional(),
    /**
     * Importance hint for the resource, from 0 (least) to 1 (most).
     */
    priority: number2().min(0).max(1).optional(),
    /**
     * ISO 8601 timestamp for the most recent modification.
     */
    lastModified: iso_exports.datetime({ offset: true }).optional()
  });
  var ResourceSchema = object2({
    ...BaseMetadataSchema.shape,
    ...IconsSchema.shape,
    /**
     * The URI of this resource.
     */
    uri: string2(),
    /**
     * A description of what this resource represents.
     *
     * This can be used by clients to improve the LLM's understanding of available resources. It can be thought of like a "hint" to the model.
     */
    description: optional(string2()),
    /**
     * The MIME type of this resource, if known.
     */
    mimeType: optional(string2()),
    /**
     * Optional annotations for the client.
     */
    annotations: AnnotationsSchema.optional(),
    /**
     * See [MCP specification](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/47339c03c143bb4ec01a26e721a1b8fe66634ebe/docs/specification/draft/basic/index.mdx#general-fields)
     * for notes on _meta usage.
     */
    _meta: optional(looseObject({}))
  });
  var ResourceTemplateSchema = object2({
    ...BaseMetadataSchema.shape,
    ...IconsSchema.shape,
    /**
     * A URI template (according to RFC 6570) that can be used to construct resource URIs.
     */
    uriTemplate: string2(),
    /**
     * A description of what this template is for.
     *
     * This can be used by clients to improve the LLM's understanding of available resources. It can be thought of like a "hint" to the model.
     */
    description: optional(string2()),
    /**
     * The MIME type for all resources that match this template. This should only be included if all resources matching this template have the same type.
     */
    mimeType: optional(string2()),
    /**
     * Optional annotations for the client.
     */
    annotations: AnnotationsSchema.optional(),
    /**
     * See [MCP specification](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/47339c03c143bb4ec01a26e721a1b8fe66634ebe/docs/specification/draft/basic/index.mdx#general-fields)
     * for notes on _meta usage.
     */
    _meta: optional(looseObject({}))
  });
  var ListResourcesRequestSchema = PaginatedRequestSchema.extend({
    method: literal("resources/list")
  });
  var ListResourcesResultSchema = PaginatedResultSchema.extend({
    resources: array(ResourceSchema)
  });
  var ListResourceTemplatesRequestSchema = PaginatedRequestSchema.extend({
    method: literal("resources/templates/list")
  });
  var ListResourceTemplatesResultSchema = PaginatedResultSchema.extend({
    resourceTemplates: array(ResourceTemplateSchema)
  });
  var ResourceRequestParamsSchema = BaseRequestParamsSchema.extend({
    /**
     * The URI of the resource to read. The URI can use any protocol; it is up to the server how to interpret it.
     *
     * @format uri
     */
    uri: string2()
  });
  var ReadResourceRequestParamsSchema = ResourceRequestParamsSchema;
  var ReadResourceRequestSchema = RequestSchema.extend({
    method: literal("resources/read"),
    params: ReadResourceRequestParamsSchema
  });
  var ReadResourceResultSchema = ResultSchema.extend({
    contents: array(union([TextResourceContentsSchema, BlobResourceContentsSchema]))
  });
  var ResourceListChangedNotificationSchema = NotificationSchema.extend({
    method: literal("notifications/resources/list_changed"),
    params: NotificationsParamsSchema.optional()
  });
  var SubscribeRequestParamsSchema = ResourceRequestParamsSchema;
  var SubscribeRequestSchema = RequestSchema.extend({
    method: literal("resources/subscribe"),
    params: SubscribeRequestParamsSchema
  });
  var UnsubscribeRequestParamsSchema = ResourceRequestParamsSchema;
  var UnsubscribeRequestSchema = RequestSchema.extend({
    method: literal("resources/unsubscribe"),
    params: UnsubscribeRequestParamsSchema
  });
  var ResourceUpdatedNotificationParamsSchema = NotificationsParamsSchema.extend({
    /**
     * The URI of the resource that has been updated. This might be a sub-resource of the one that the client actually subscribed to.
     */
    uri: string2()
  });
  var ResourceUpdatedNotificationSchema = NotificationSchema.extend({
    method: literal("notifications/resources/updated"),
    params: ResourceUpdatedNotificationParamsSchema
  });
  var PromptArgumentSchema = object2({
    /**
     * The name of the argument.
     */
    name: string2(),
    /**
     * A human-readable description of the argument.
     */
    description: optional(string2()),
    /**
     * Whether this argument must be provided.
     */
    required: optional(boolean2())
  });
  var PromptSchema = object2({
    ...BaseMetadataSchema.shape,
    ...IconsSchema.shape,
    /**
     * An optional description of what this prompt provides
     */
    description: optional(string2()),
    /**
     * A list of arguments to use for templating the prompt.
     */
    arguments: optional(array(PromptArgumentSchema)),
    /**
     * See [MCP specification](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/47339c03c143bb4ec01a26e721a1b8fe66634ebe/docs/specification/draft/basic/index.mdx#general-fields)
     * for notes on _meta usage.
     */
    _meta: optional(looseObject({}))
  });
  var ListPromptsRequestSchema = PaginatedRequestSchema.extend({
    method: literal("prompts/list")
  });
  var ListPromptsResultSchema = PaginatedResultSchema.extend({
    prompts: array(PromptSchema)
  });
  var GetPromptRequestParamsSchema = BaseRequestParamsSchema.extend({
    /**
     * The name of the prompt or prompt template.
     */
    name: string2(),
    /**
     * Arguments to use for templating the prompt.
     */
    arguments: record(string2(), string2()).optional()
  });
  var GetPromptRequestSchema = RequestSchema.extend({
    method: literal("prompts/get"),
    params: GetPromptRequestParamsSchema
  });
  var TextContentSchema = object2({
    type: literal("text"),
    /**
     * The text content of the message.
     */
    text: string2(),
    /**
     * Optional annotations for the client.
     */
    annotations: AnnotationsSchema.optional(),
    /**
     * See [MCP specification](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/47339c03c143bb4ec01a26e721a1b8fe66634ebe/docs/specification/draft/basic/index.mdx#general-fields)
     * for notes on _meta usage.
     */
    _meta: record(string2(), unknown()).optional()
  });
  var ImageContentSchema = object2({
    type: literal("image"),
    /**
     * The base64-encoded image data.
     */
    data: Base64Schema,
    /**
     * The MIME type of the image. Different providers may support different image types.
     */
    mimeType: string2(),
    /**
     * Optional annotations for the client.
     */
    annotations: AnnotationsSchema.optional(),
    /**
     * See [MCP specification](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/47339c03c143bb4ec01a26e721a1b8fe66634ebe/docs/specification/draft/basic/index.mdx#general-fields)
     * for notes on _meta usage.
     */
    _meta: record(string2(), unknown()).optional()
  });
  var AudioContentSchema = object2({
    type: literal("audio"),
    /**
     * The base64-encoded audio data.
     */
    data: Base64Schema,
    /**
     * The MIME type of the audio. Different providers may support different audio types.
     */
    mimeType: string2(),
    /**
     * Optional annotations for the client.
     */
    annotations: AnnotationsSchema.optional(),
    /**
     * See [MCP specification](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/47339c03c143bb4ec01a26e721a1b8fe66634ebe/docs/specification/draft/basic/index.mdx#general-fields)
     * for notes on _meta usage.
     */
    _meta: record(string2(), unknown()).optional()
  });
  var ToolUseContentSchema = object2({
    type: literal("tool_use"),
    /**
     * The name of the tool to invoke.
     * Must match a tool name from the request's tools array.
     */
    name: string2(),
    /**
     * Unique identifier for this tool call.
     * Used to correlate with ToolResultContent in subsequent messages.
     */
    id: string2(),
    /**
     * Arguments to pass to the tool.
     * Must conform to the tool's inputSchema.
     */
    input: record(string2(), unknown()),
    /**
     * See [MCP specification](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/47339c03c143bb4ec01a26e721a1b8fe66634ebe/docs/specification/draft/basic/index.mdx#general-fields)
     * for notes on _meta usage.
     */
    _meta: record(string2(), unknown()).optional()
  });
  var EmbeddedResourceSchema = object2({
    type: literal("resource"),
    resource: union([TextResourceContentsSchema, BlobResourceContentsSchema]),
    /**
     * Optional annotations for the client.
     */
    annotations: AnnotationsSchema.optional(),
    /**
     * See [MCP specification](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/47339c03c143bb4ec01a26e721a1b8fe66634ebe/docs/specification/draft/basic/index.mdx#general-fields)
     * for notes on _meta usage.
     */
    _meta: record(string2(), unknown()).optional()
  });
  var ResourceLinkSchema = ResourceSchema.extend({
    type: literal("resource_link")
  });
  var ContentBlockSchema = union([
    TextContentSchema,
    ImageContentSchema,
    AudioContentSchema,
    ResourceLinkSchema,
    EmbeddedResourceSchema
  ]);
  var PromptMessageSchema = object2({
    role: RoleSchema,
    content: ContentBlockSchema
  });
  var GetPromptResultSchema = ResultSchema.extend({
    /**
     * An optional description for the prompt.
     */
    description: string2().optional(),
    messages: array(PromptMessageSchema)
  });
  var PromptListChangedNotificationSchema = NotificationSchema.extend({
    method: literal("notifications/prompts/list_changed"),
    params: NotificationsParamsSchema.optional()
  });
  var ToolAnnotationsSchema = object2({
    /**
     * A human-readable title for the tool.
     */
    title: string2().optional(),
    /**
     * If true, the tool does not modify its environment.
     *
     * Default: false
     */
    readOnlyHint: boolean2().optional(),
    /**
     * If true, the tool may perform destructive updates to its environment.
     * If false, the tool performs only additive updates.
     *
     * (This property is meaningful only when `readOnlyHint == false`)
     *
     * Default: true
     */
    destructiveHint: boolean2().optional(),
    /**
     * If true, calling the tool repeatedly with the same arguments
     * will have no additional effect on the its environment.
     *
     * (This property is meaningful only when `readOnlyHint == false`)
     *
     * Default: false
     */
    idempotentHint: boolean2().optional(),
    /**
     * If true, this tool may interact with an "open world" of external
     * entities. If false, the tool's domain of interaction is closed.
     * For example, the world of a web search tool is open, whereas that
     * of a memory tool is not.
     *
     * Default: true
     */
    openWorldHint: boolean2().optional()
  });
  var ToolExecutionSchema = object2({
    /**
     * Indicates the tool's preference for task-augmented execution.
     * - "required": Clients MUST invoke the tool as a task
     * - "optional": Clients MAY invoke the tool as a task or normal request
     * - "forbidden": Clients MUST NOT attempt to invoke the tool as a task
     *
     * If not present, defaults to "forbidden".
     */
    taskSupport: _enum(["required", "optional", "forbidden"]).optional()
  });
  var ToolSchema = object2({
    ...BaseMetadataSchema.shape,
    ...IconsSchema.shape,
    /**
     * A human-readable description of the tool.
     */
    description: string2().optional(),
    /**
     * A JSON Schema 2020-12 object defining the expected parameters for the tool.
     * Must have type: 'object' at the root level per MCP spec.
     */
    inputSchema: object2({
      type: literal("object"),
      properties: record(string2(), AssertObjectSchema).optional(),
      required: array(string2()).optional()
    }).catchall(unknown()),
    /**
     * An optional JSON Schema 2020-12 object defining the structure of the tool's output
     * returned in the structuredContent field of a CallToolResult.
     * Must have type: 'object' at the root level per MCP spec.
     */
    outputSchema: object2({
      type: literal("object"),
      properties: record(string2(), AssertObjectSchema).optional(),
      required: array(string2()).optional()
    }).catchall(unknown()).optional(),
    /**
     * Optional additional tool information.
     */
    annotations: ToolAnnotationsSchema.optional(),
    /**
     * Execution-related properties for this tool.
     */
    execution: ToolExecutionSchema.optional(),
    /**
     * See [MCP specification](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/47339c03c143bb4ec01a26e721a1b8fe66634ebe/docs/specification/draft/basic/index.mdx#general-fields)
     * for notes on _meta usage.
     */
    _meta: record(string2(), unknown()).optional()
  });
  var ListToolsRequestSchema = PaginatedRequestSchema.extend({
    method: literal("tools/list")
  });
  var ListToolsResultSchema = PaginatedResultSchema.extend({
    tools: array(ToolSchema)
  });
  var CallToolResultSchema = ResultSchema.extend({
    /**
     * A list of content objects that represent the result of the tool call.
     *
     * If the Tool does not define an outputSchema, this field MUST be present in the result.
     * For backwards compatibility, this field is always present, but it may be empty.
     */
    content: array(ContentBlockSchema).default([]),
    /**
     * An object containing structured tool output.
     *
     * If the Tool defines an outputSchema, this field MUST be present in the result, and contain a JSON object that matches the schema.
     */
    structuredContent: record(string2(), unknown()).optional(),
    /**
     * Whether the tool call ended in an error.
     *
     * If not set, this is assumed to be false (the call was successful).
     *
     * Any errors that originate from the tool SHOULD be reported inside the result
     * object, with `isError` set to true, _not_ as an MCP protocol-level error
     * response. Otherwise, the LLM would not be able to see that an error occurred
     * and self-correct.
     *
     * However, any errors in _finding_ the tool, an error indicating that the
     * server does not support tool calls, or any other exceptional conditions,
     * should be reported as an MCP error response.
     */
    isError: boolean2().optional()
  });
  var CompatibilityCallToolResultSchema = CallToolResultSchema.or(ResultSchema.extend({
    toolResult: unknown()
  }));
  var CallToolRequestParamsSchema = TaskAugmentedRequestParamsSchema.extend({
    /**
     * The name of the tool to call.
     */
    name: string2(),
    /**
     * Arguments to pass to the tool.
     */
    arguments: record(string2(), unknown()).optional()
  });
  var CallToolRequestSchema = RequestSchema.extend({
    method: literal("tools/call"),
    params: CallToolRequestParamsSchema
  });
  var ToolListChangedNotificationSchema = NotificationSchema.extend({
    method: literal("notifications/tools/list_changed"),
    params: NotificationsParamsSchema.optional()
  });
  var ListChangedOptionsBaseSchema = object2({
    /**
     * If true, the list will be refreshed automatically when a list changed notification is received.
     * The callback will be called with the updated list.
     *
     * If false, the callback will be called with null items, allowing manual refresh.
     *
     * @default true
     */
    autoRefresh: boolean2().default(true),
    /**
     * Debounce time in milliseconds for list changed notification processing.
     *
     * Multiple notifications received within this timeframe will only trigger one refresh.
     * Set to 0 to disable debouncing.
     *
     * @default 300
     */
    debounceMs: number2().int().nonnegative().default(300)
  });
  var LoggingLevelSchema = _enum(["debug", "info", "notice", "warning", "error", "critical", "alert", "emergency"]);
  var SetLevelRequestParamsSchema = BaseRequestParamsSchema.extend({
    /**
     * The level of logging that the client wants to receive from the server. The server should send all logs at this level and higher (i.e., more severe) to the client as notifications/logging/message.
     */
    level: LoggingLevelSchema
  });
  var SetLevelRequestSchema = RequestSchema.extend({
    method: literal("logging/setLevel"),
    params: SetLevelRequestParamsSchema
  });
  var LoggingMessageNotificationParamsSchema = NotificationsParamsSchema.extend({
    /**
     * The severity of this log message.
     */
    level: LoggingLevelSchema,
    /**
     * An optional name of the logger issuing this message.
     */
    logger: string2().optional(),
    /**
     * The data to be logged, such as a string message or an object. Any JSON serializable type is allowed here.
     */
    data: unknown()
  });
  var LoggingMessageNotificationSchema = NotificationSchema.extend({
    method: literal("notifications/message"),
    params: LoggingMessageNotificationParamsSchema
  });
  var ModelHintSchema = object2({
    /**
     * A hint for a model name.
     */
    name: string2().optional()
  });
  var ModelPreferencesSchema = object2({
    /**
     * Optional hints to use for model selection.
     */
    hints: array(ModelHintSchema).optional(),
    /**
     * How much to prioritize cost when selecting a model.
     */
    costPriority: number2().min(0).max(1).optional(),
    /**
     * How much to prioritize sampling speed (latency) when selecting a model.
     */
    speedPriority: number2().min(0).max(1).optional(),
    /**
     * How much to prioritize intelligence and capabilities when selecting a model.
     */
    intelligencePriority: number2().min(0).max(1).optional()
  });
  var ToolChoiceSchema = object2({
    /**
     * Controls when tools are used:
     * - "auto": Model decides whether to use tools (default)
     * - "required": Model MUST use at least one tool before completing
     * - "none": Model MUST NOT use any tools
     */
    mode: _enum(["auto", "required", "none"]).optional()
  });
  var ToolResultContentSchema = object2({
    type: literal("tool_result"),
    toolUseId: string2().describe("The unique identifier for the corresponding tool call."),
    content: array(ContentBlockSchema).default([]),
    structuredContent: object2({}).loose().optional(),
    isError: boolean2().optional(),
    /**
     * See [MCP specification](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/47339c03c143bb4ec01a26e721a1b8fe66634ebe/docs/specification/draft/basic/index.mdx#general-fields)
     * for notes on _meta usage.
     */
    _meta: record(string2(), unknown()).optional()
  });
  var SamplingContentSchema = discriminatedUnion("type", [TextContentSchema, ImageContentSchema, AudioContentSchema]);
  var SamplingMessageContentBlockSchema = discriminatedUnion("type", [
    TextContentSchema,
    ImageContentSchema,
    AudioContentSchema,
    ToolUseContentSchema,
    ToolResultContentSchema
  ]);
  var SamplingMessageSchema = object2({
    role: RoleSchema,
    content: union([SamplingMessageContentBlockSchema, array(SamplingMessageContentBlockSchema)]),
    /**
     * See [MCP specification](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/47339c03c143bb4ec01a26e721a1b8fe66634ebe/docs/specification/draft/basic/index.mdx#general-fields)
     * for notes on _meta usage.
     */
    _meta: record(string2(), unknown()).optional()
  });
  var CreateMessageRequestParamsSchema = TaskAugmentedRequestParamsSchema.extend({
    messages: array(SamplingMessageSchema),
    /**
     * The server's preferences for which model to select. The client MAY modify or omit this request.
     */
    modelPreferences: ModelPreferencesSchema.optional(),
    /**
     * An optional system prompt the server wants to use for sampling. The client MAY modify or omit this prompt.
     */
    systemPrompt: string2().optional(),
    /**
     * A request to include context from one or more MCP servers (including the caller), to be attached to the prompt.
     * The client MAY ignore this request.
     *
     * Default is "none". Values "thisServer" and "allServers" are soft-deprecated. Servers SHOULD only use these values if the client
     * declares ClientCapabilities.sampling.context. These values may be removed in future spec releases.
     */
    includeContext: _enum(["none", "thisServer", "allServers"]).optional(),
    temperature: number2().optional(),
    /**
     * The requested maximum number of tokens to sample (to prevent runaway completions).
     *
     * The client MAY choose to sample fewer tokens than the requested maximum.
     */
    maxTokens: number2().int(),
    stopSequences: array(string2()).optional(),
    /**
     * Optional metadata to pass through to the LLM provider. The format of this metadata is provider-specific.
     */
    metadata: AssertObjectSchema.optional(),
    /**
     * Tools that the model may use during generation.
     * The client MUST return an error if this field is provided but ClientCapabilities.sampling.tools is not declared.
     */
    tools: array(ToolSchema).optional(),
    /**
     * Controls how the model uses tools.
     * The client MUST return an error if this field is provided but ClientCapabilities.sampling.tools is not declared.
     * Default is `{ mode: "auto" }`.
     */
    toolChoice: ToolChoiceSchema.optional()
  });
  var CreateMessageRequestSchema = RequestSchema.extend({
    method: literal("sampling/createMessage"),
    params: CreateMessageRequestParamsSchema
  });
  var CreateMessageResultSchema = ResultSchema.extend({
    /**
     * The name of the model that generated the message.
     */
    model: string2(),
    /**
     * The reason why sampling stopped, if known.
     *
     * Standard values:
     * - "endTurn": Natural end of the assistant's turn
     * - "stopSequence": A stop sequence was encountered
     * - "maxTokens": Maximum token limit was reached
     *
     * This field is an open string to allow for provider-specific stop reasons.
     */
    stopReason: optional(_enum(["endTurn", "stopSequence", "maxTokens"]).or(string2())),
    role: RoleSchema,
    /**
     * Response content. Single content block (text, image, or audio).
     */
    content: SamplingContentSchema
  });
  var CreateMessageResultWithToolsSchema = ResultSchema.extend({
    /**
     * The name of the model that generated the message.
     */
    model: string2(),
    /**
     * The reason why sampling stopped, if known.
     *
     * Standard values:
     * - "endTurn": Natural end of the assistant's turn
     * - "stopSequence": A stop sequence was encountered
     * - "maxTokens": Maximum token limit was reached
     * - "toolUse": The model wants to use one or more tools
     *
     * This field is an open string to allow for provider-specific stop reasons.
     */
    stopReason: optional(_enum(["endTurn", "stopSequence", "maxTokens", "toolUse"]).or(string2())),
    role: RoleSchema,
    /**
     * Response content. May be a single block or array. May include ToolUseContent if stopReason is "toolUse".
     */
    content: union([SamplingMessageContentBlockSchema, array(SamplingMessageContentBlockSchema)])
  });
  var BooleanSchemaSchema = object2({
    type: literal("boolean"),
    title: string2().optional(),
    description: string2().optional(),
    default: boolean2().optional()
  });
  var StringSchemaSchema = object2({
    type: literal("string"),
    title: string2().optional(),
    description: string2().optional(),
    minLength: number2().optional(),
    maxLength: number2().optional(),
    format: _enum(["email", "uri", "date", "date-time"]).optional(),
    default: string2().optional()
  });
  var NumberSchemaSchema = object2({
    type: _enum(["number", "integer"]),
    title: string2().optional(),
    description: string2().optional(),
    minimum: number2().optional(),
    maximum: number2().optional(),
    default: number2().optional()
  });
  var UntitledSingleSelectEnumSchemaSchema = object2({
    type: literal("string"),
    title: string2().optional(),
    description: string2().optional(),
    enum: array(string2()),
    default: string2().optional()
  });
  var TitledSingleSelectEnumSchemaSchema = object2({
    type: literal("string"),
    title: string2().optional(),
    description: string2().optional(),
    oneOf: array(object2({
      const: string2(),
      title: string2()
    })),
    default: string2().optional()
  });
  var LegacyTitledEnumSchemaSchema = object2({
    type: literal("string"),
    title: string2().optional(),
    description: string2().optional(),
    enum: array(string2()),
    enumNames: array(string2()).optional(),
    default: string2().optional()
  });
  var SingleSelectEnumSchemaSchema = union([UntitledSingleSelectEnumSchemaSchema, TitledSingleSelectEnumSchemaSchema]);
  var UntitledMultiSelectEnumSchemaSchema = object2({
    type: literal("array"),
    title: string2().optional(),
    description: string2().optional(),
    minItems: number2().optional(),
    maxItems: number2().optional(),
    items: object2({
      type: literal("string"),
      enum: array(string2())
    }),
    default: array(string2()).optional()
  });
  var TitledMultiSelectEnumSchemaSchema = object2({
    type: literal("array"),
    title: string2().optional(),
    description: string2().optional(),
    minItems: number2().optional(),
    maxItems: number2().optional(),
    items: object2({
      anyOf: array(object2({
        const: string2(),
        title: string2()
      }))
    }),
    default: array(string2()).optional()
  });
  var MultiSelectEnumSchemaSchema = union([UntitledMultiSelectEnumSchemaSchema, TitledMultiSelectEnumSchemaSchema]);
  var EnumSchemaSchema = union([LegacyTitledEnumSchemaSchema, SingleSelectEnumSchemaSchema, MultiSelectEnumSchemaSchema]);
  var PrimitiveSchemaDefinitionSchema = union([EnumSchemaSchema, BooleanSchemaSchema, StringSchemaSchema, NumberSchemaSchema]);
  var ElicitRequestFormParamsSchema = TaskAugmentedRequestParamsSchema.extend({
    /**
     * The elicitation mode.
     *
     * Optional for backward compatibility. Clients MUST treat missing mode as "form".
     */
    mode: literal("form").optional(),
    /**
     * The message to present to the user describing what information is being requested.
     */
    message: string2(),
    /**
     * A restricted subset of JSON Schema.
     * Only top-level properties are allowed, without nesting.
     */
    requestedSchema: object2({
      type: literal("object"),
      properties: record(string2(), PrimitiveSchemaDefinitionSchema),
      required: array(string2()).optional()
    })
  });
  var ElicitRequestURLParamsSchema = TaskAugmentedRequestParamsSchema.extend({
    /**
     * The elicitation mode.
     */
    mode: literal("url"),
    /**
     * The message to present to the user explaining why the interaction is needed.
     */
    message: string2(),
    /**
     * The ID of the elicitation, which must be unique within the context of the server.
     * The client MUST treat this ID as an opaque value.
     */
    elicitationId: string2(),
    /**
     * The URL that the user should navigate to.
     */
    url: string2().url()
  });
  var ElicitRequestParamsSchema = union([ElicitRequestFormParamsSchema, ElicitRequestURLParamsSchema]);
  var ElicitRequestSchema = RequestSchema.extend({
    method: literal("elicitation/create"),
    params: ElicitRequestParamsSchema
  });
  var ElicitationCompleteNotificationParamsSchema = NotificationsParamsSchema.extend({
    /**
     * The ID of the elicitation that completed.
     */
    elicitationId: string2()
  });
  var ElicitationCompleteNotificationSchema = NotificationSchema.extend({
    method: literal("notifications/elicitation/complete"),
    params: ElicitationCompleteNotificationParamsSchema
  });
  var ElicitResultSchema = ResultSchema.extend({
    /**
     * The user action in response to the elicitation.
     * - "accept": User submitted the form/confirmed the action
     * - "decline": User explicitly decline the action
     * - "cancel": User dismissed without making an explicit choice
     */
    action: _enum(["accept", "decline", "cancel"]),
    /**
     * The submitted form data, only present when action is "accept".
     * Contains values matching the requested schema.
     * Per MCP spec, content is "typically omitted" for decline/cancel actions.
     * We normalize null to undefined for leniency while maintaining type compatibility.
     */
    content: preprocess((val) => val === null ? void 0 : val, record(string2(), union([string2(), number2(), boolean2(), array(string2())])).optional())
  });
  var ResourceTemplateReferenceSchema = object2({
    type: literal("ref/resource"),
    /**
     * The URI or URI template of the resource.
     */
    uri: string2()
  });
  var PromptReferenceSchema = object2({
    type: literal("ref/prompt"),
    /**
     * The name of the prompt or prompt template
     */
    name: string2()
  });
  var CompleteRequestParamsSchema = BaseRequestParamsSchema.extend({
    ref: union([PromptReferenceSchema, ResourceTemplateReferenceSchema]),
    /**
     * The argument's information
     */
    argument: object2({
      /**
       * The name of the argument
       */
      name: string2(),
      /**
       * The value of the argument to use for completion matching.
       */
      value: string2()
    }),
    context: object2({
      /**
       * Previously-resolved variables in a URI template or prompt.
       */
      arguments: record(string2(), string2()).optional()
    }).optional()
  });
  var CompleteRequestSchema = RequestSchema.extend({
    method: literal("completion/complete"),
    params: CompleteRequestParamsSchema
  });
  var CompleteResultSchema = ResultSchema.extend({
    completion: looseObject({
      /**
       * An array of completion values. Must not exceed 100 items.
       */
      values: array(string2()).max(100),
      /**
       * The total number of completion options available. This can exceed the number of values actually sent in the response.
       */
      total: optional(number2().int()),
      /**
       * Indicates whether there are additional completion options beyond those provided in the current response, even if the exact total is unknown.
       */
      hasMore: optional(boolean2())
    })
  });
  var RootSchema = object2({
    /**
     * The URI identifying the root. This *must* start with file:// for now.
     */
    uri: string2().startsWith("file://"),
    /**
     * An optional name for the root.
     */
    name: string2().optional(),
    /**
     * See [MCP specification](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/47339c03c143bb4ec01a26e721a1b8fe66634ebe/docs/specification/draft/basic/index.mdx#general-fields)
     * for notes on _meta usage.
     */
    _meta: record(string2(), unknown()).optional()
  });
  var ListRootsRequestSchema = RequestSchema.extend({
    method: literal("roots/list"),
    params: BaseRequestParamsSchema.optional()
  });
  var ListRootsResultSchema = ResultSchema.extend({
    roots: array(RootSchema)
  });
  var RootsListChangedNotificationSchema = NotificationSchema.extend({
    method: literal("notifications/roots/list_changed"),
    params: NotificationsParamsSchema.optional()
  });
  var ClientRequestSchema = union([
    PingRequestSchema,
    InitializeRequestSchema,
    CompleteRequestSchema,
    SetLevelRequestSchema,
    GetPromptRequestSchema,
    ListPromptsRequestSchema,
    ListResourcesRequestSchema,
    ListResourceTemplatesRequestSchema,
    ReadResourceRequestSchema,
    SubscribeRequestSchema,
    UnsubscribeRequestSchema,
    CallToolRequestSchema,
    ListToolsRequestSchema,
    GetTaskRequestSchema,
    GetTaskPayloadRequestSchema,
    ListTasksRequestSchema,
    CancelTaskRequestSchema
  ]);
  var ClientNotificationSchema = union([
    CancelledNotificationSchema,
    ProgressNotificationSchema,
    InitializedNotificationSchema,
    RootsListChangedNotificationSchema,
    TaskStatusNotificationSchema
  ]);
  var ClientResultSchema = union([
    EmptyResultSchema,
    CreateMessageResultSchema,
    CreateMessageResultWithToolsSchema,
    ElicitResultSchema,
    ListRootsResultSchema,
    GetTaskResultSchema,
    ListTasksResultSchema,
    CreateTaskResultSchema
  ]);
  var ServerRequestSchema = union([
    PingRequestSchema,
    CreateMessageRequestSchema,
    ElicitRequestSchema,
    ListRootsRequestSchema,
    GetTaskRequestSchema,
    GetTaskPayloadRequestSchema,
    ListTasksRequestSchema,
    CancelTaskRequestSchema
  ]);
  var ServerNotificationSchema = union([
    CancelledNotificationSchema,
    ProgressNotificationSchema,
    LoggingMessageNotificationSchema,
    ResourceUpdatedNotificationSchema,
    ResourceListChangedNotificationSchema,
    ToolListChangedNotificationSchema,
    PromptListChangedNotificationSchema,
    TaskStatusNotificationSchema,
    ElicitationCompleteNotificationSchema
  ]);
  var ServerResultSchema = union([
    EmptyResultSchema,
    InitializeResultSchema,
    CompleteResultSchema,
    GetPromptResultSchema,
    ListPromptsResultSchema,
    ListResourcesResultSchema,
    ListResourceTemplatesResultSchema,
    ReadResourceResultSchema,
    CallToolResultSchema,
    ListToolsResultSchema,
    GetTaskResultSchema,
    ListTasksResultSchema,
    CreateTaskResultSchema
  ]);
  var McpError = class _McpError extends Error {
    constructor(code, message, data) {
      super(`MCP error ${code}: ${message}`);
      this.code = code;
      this.data = data;
      this.name = "McpError";
    }
    /**
     * Factory method to create the appropriate error type based on the error code and data
     */
    static fromError(code, message, data) {
      if (code === ErrorCode.UrlElicitationRequired && data) {
        const errorData = data;
        if (errorData.elicitations) {
          return new UrlElicitationRequiredError(errorData.elicitations, message);
        }
      }
      return new _McpError(code, message, data);
    }
  };
  var UrlElicitationRequiredError = class extends McpError {
    constructor(elicitations, message = `URL elicitation${elicitations.length > 1 ? "s" : ""} required`) {
      super(ErrorCode.UrlElicitationRequired, message, {
        elicitations
      });
    }
    get elicitations() {
      return this.data?.elicitations ?? [];
    }
  };

  // node_modules/@modelcontextprotocol/sdk/dist/esm/experimental/tasks/interfaces.js
  function isTerminal(status) {
    return status === "completed" || status === "failed" || status === "cancelled";
  }

  // node_modules/zod-to-json-schema/dist/esm/Options.js
  var ignoreOverride = Symbol("Let zodToJsonSchema decide on which parser to use");

  // node_modules/zod-to-json-schema/dist/esm/parsers/string.js
  var ALPHA_NUMERIC = new Set("ABCDEFGHIJKLMNOPQRSTUVXYZabcdefghijklmnopqrstuvxyz0123456789");

  // node_modules/@modelcontextprotocol/sdk/dist/esm/server/zod-json-schema-compat.js
  function getMethodLiteral(schema) {
    const shape = getObjectShape(schema);
    const methodSchema = shape?.method;
    if (!methodSchema) {
      throw new Error("Schema is missing a method literal");
    }
    const value = getLiteralValue(methodSchema);
    if (typeof value !== "string") {
      throw new Error("Schema method literal must be a string");
    }
    return value;
  }
  function parseWithCompat(schema, data) {
    const result = safeParse2(schema, data);
    if (!result.success) {
      throw result.error;
    }
    return result.data;
  }

  // node_modules/@modelcontextprotocol/sdk/dist/esm/shared/protocol.js
  var DEFAULT_REQUEST_TIMEOUT_MSEC = 6e4;
  var Protocol = class {
    constructor(_options) {
      this._options = _options;
      this._requestMessageId = 0;
      this._requestHandlers = /* @__PURE__ */ new Map();
      this._requestHandlerAbortControllers = /* @__PURE__ */ new Map();
      this._notificationHandlers = /* @__PURE__ */ new Map();
      this._responseHandlers = /* @__PURE__ */ new Map();
      this._progressHandlers = /* @__PURE__ */ new Map();
      this._timeoutInfo = /* @__PURE__ */ new Map();
      this._pendingDebouncedNotifications = /* @__PURE__ */ new Set();
      this._taskProgressTokens = /* @__PURE__ */ new Map();
      this._requestResolvers = /* @__PURE__ */ new Map();
      this.setNotificationHandler(CancelledNotificationSchema, (notification) => {
        this._oncancel(notification);
      });
      this.setNotificationHandler(ProgressNotificationSchema, (notification) => {
        this._onprogress(notification);
      });
      this.setRequestHandler(
        PingRequestSchema,
        // Automatic pong by default.
        (_request) => ({})
      );
      this._taskStore = _options?.taskStore;
      this._taskMessageQueue = _options?.taskMessageQueue;
      if (this._taskStore) {
        this.setRequestHandler(GetTaskRequestSchema, async (request, extra) => {
          const task = await this._taskStore.getTask(request.params.taskId, extra.sessionId);
          if (!task) {
            throw new McpError(ErrorCode.InvalidParams, "Failed to retrieve task: Task not found");
          }
          return {
            ...task
          };
        });
        this.setRequestHandler(GetTaskPayloadRequestSchema, async (request, extra) => {
          const handleTaskResult = async () => {
            const taskId = request.params.taskId;
            if (this._taskMessageQueue) {
              let queuedMessage;
              while (queuedMessage = await this._taskMessageQueue.dequeue(taskId, extra.sessionId)) {
                if (queuedMessage.type === "response" || queuedMessage.type === "error") {
                  const message = queuedMessage.message;
                  const requestId = message.id;
                  const resolver = this._requestResolvers.get(requestId);
                  if (resolver) {
                    this._requestResolvers.delete(requestId);
                    if (queuedMessage.type === "response") {
                      resolver(message);
                    } else {
                      const errorMessage = message;
                      const error2 = new McpError(errorMessage.error.code, errorMessage.error.message, errorMessage.error.data);
                      resolver(error2);
                    }
                  } else {
                    const messageType = queuedMessage.type === "response" ? "Response" : "Error";
                    this._onerror(new Error(`${messageType} handler missing for request ${requestId}`));
                  }
                  continue;
                }
                await this._transport?.send(queuedMessage.message, { relatedRequestId: extra.requestId });
              }
            }
            const task = await this._taskStore.getTask(taskId, extra.sessionId);
            if (!task) {
              throw new McpError(ErrorCode.InvalidParams, `Task not found: ${taskId}`);
            }
            if (!isTerminal(task.status)) {
              await this._waitForTaskUpdate(taskId, extra.signal);
              return await handleTaskResult();
            }
            if (isTerminal(task.status)) {
              const result = await this._taskStore.getTaskResult(taskId, extra.sessionId);
              this._clearTaskQueue(taskId);
              return {
                ...result,
                _meta: {
                  ...result._meta,
                  [RELATED_TASK_META_KEY]: {
                    taskId
                  }
                }
              };
            }
            return await handleTaskResult();
          };
          return await handleTaskResult();
        });
        this.setRequestHandler(ListTasksRequestSchema, async (request, extra) => {
          try {
            const { tasks, nextCursor } = await this._taskStore.listTasks(request.params?.cursor, extra.sessionId);
            return {
              tasks,
              nextCursor,
              _meta: {}
            };
          } catch (error2) {
            throw new McpError(ErrorCode.InvalidParams, `Failed to list tasks: ${error2 instanceof Error ? error2.message : String(error2)}`);
          }
        });
        this.setRequestHandler(CancelTaskRequestSchema, async (request, extra) => {
          try {
            const task = await this._taskStore.getTask(request.params.taskId, extra.sessionId);
            if (!task) {
              throw new McpError(ErrorCode.InvalidParams, `Task not found: ${request.params.taskId}`);
            }
            if (isTerminal(task.status)) {
              throw new McpError(ErrorCode.InvalidParams, `Cannot cancel task in terminal status: ${task.status}`);
            }
            await this._taskStore.updateTaskStatus(request.params.taskId, "cancelled", "Client cancelled task execution.", extra.sessionId);
            this._clearTaskQueue(request.params.taskId);
            const cancelledTask = await this._taskStore.getTask(request.params.taskId, extra.sessionId);
            if (!cancelledTask) {
              throw new McpError(ErrorCode.InvalidParams, `Task not found after cancellation: ${request.params.taskId}`);
            }
            return {
              _meta: {},
              ...cancelledTask
            };
          } catch (error2) {
            if (error2 instanceof McpError) {
              throw error2;
            }
            throw new McpError(ErrorCode.InvalidRequest, `Failed to cancel task: ${error2 instanceof Error ? error2.message : String(error2)}`);
          }
        });
      }
    }
    async _oncancel(notification) {
      if (!notification.params.requestId) {
        return;
      }
      const controller = this._requestHandlerAbortControllers.get(notification.params.requestId);
      controller?.abort(notification.params.reason);
    }
    _setupTimeout(messageId, timeout, maxTotalTimeout, onTimeout, resetTimeoutOnProgress = false) {
      this._timeoutInfo.set(messageId, {
        timeoutId: setTimeout(onTimeout, timeout),
        startTime: Date.now(),
        timeout,
        maxTotalTimeout,
        resetTimeoutOnProgress,
        onTimeout
      });
    }
    _resetTimeout(messageId) {
      const info = this._timeoutInfo.get(messageId);
      if (!info)
        return false;
      const totalElapsed = Date.now() - info.startTime;
      if (info.maxTotalTimeout && totalElapsed >= info.maxTotalTimeout) {
        this._timeoutInfo.delete(messageId);
        throw McpError.fromError(ErrorCode.RequestTimeout, "Maximum total timeout exceeded", {
          maxTotalTimeout: info.maxTotalTimeout,
          totalElapsed
        });
      }
      clearTimeout(info.timeoutId);
      info.timeoutId = setTimeout(info.onTimeout, info.timeout);
      return true;
    }
    _cleanupTimeout(messageId) {
      const info = this._timeoutInfo.get(messageId);
      if (info) {
        clearTimeout(info.timeoutId);
        this._timeoutInfo.delete(messageId);
      }
    }
    /**
     * Attaches to the given transport, starts it, and starts listening for messages.
     *
     * The Protocol object assumes ownership of the Transport, replacing any callbacks that have already been set, and expects that it is the only user of the Transport instance going forward.
     */
    async connect(transport) {
      this._transport = transport;
      const _onclose = this.transport?.onclose;
      this._transport.onclose = () => {
        _onclose?.();
        this._onclose();
      };
      const _onerror = this.transport?.onerror;
      this._transport.onerror = (error2) => {
        _onerror?.(error2);
        this._onerror(error2);
      };
      const _onmessage = this._transport?.onmessage;
      this._transport.onmessage = (message, extra) => {
        _onmessage?.(message, extra);
        if (isJSONRPCResultResponse(message) || isJSONRPCErrorResponse(message)) {
          this._onresponse(message);
        } else if (isJSONRPCRequest(message)) {
          this._onrequest(message, extra);
        } else if (isJSONRPCNotification(message)) {
          this._onnotification(message);
        } else {
          this._onerror(new Error(`Unknown message type: ${JSON.stringify(message)}`));
        }
      };
      await this._transport.start();
    }
    _onclose() {
      const responseHandlers = this._responseHandlers;
      this._responseHandlers = /* @__PURE__ */ new Map();
      this._progressHandlers.clear();
      this._taskProgressTokens.clear();
      this._pendingDebouncedNotifications.clear();
      const error2 = McpError.fromError(ErrorCode.ConnectionClosed, "Connection closed");
      this._transport = void 0;
      this.onclose?.();
      for (const handler of responseHandlers.values()) {
        handler(error2);
      }
    }
    _onerror(error2) {
      this.onerror?.(error2);
    }
    _onnotification(notification) {
      const handler = this._notificationHandlers.get(notification.method) ?? this.fallbackNotificationHandler;
      if (handler === void 0) {
        return;
      }
      Promise.resolve().then(() => handler(notification)).catch((error2) => this._onerror(new Error(`Uncaught error in notification handler: ${error2}`)));
    }
    _onrequest(request, extra) {
      const handler = this._requestHandlers.get(request.method) ?? this.fallbackRequestHandler;
      const capturedTransport = this._transport;
      const relatedTaskId = request.params?._meta?.[RELATED_TASK_META_KEY]?.taskId;
      if (handler === void 0) {
        const errorResponse = {
          jsonrpc: "2.0",
          id: request.id,
          error: {
            code: ErrorCode.MethodNotFound,
            message: "Method not found"
          }
        };
        if (relatedTaskId && this._taskMessageQueue) {
          this._enqueueTaskMessage(relatedTaskId, {
            type: "error",
            message: errorResponse,
            timestamp: Date.now()
          }, capturedTransport?.sessionId).catch((error2) => this._onerror(new Error(`Failed to enqueue error response: ${error2}`)));
        } else {
          capturedTransport?.send(errorResponse).catch((error2) => this._onerror(new Error(`Failed to send an error response: ${error2}`)));
        }
        return;
      }
      const abortController = new AbortController();
      this._requestHandlerAbortControllers.set(request.id, abortController);
      const taskCreationParams = isTaskAugmentedRequestParams(request.params) ? request.params.task : void 0;
      const taskStore = this._taskStore ? this.requestTaskStore(request, capturedTransport?.sessionId) : void 0;
      const fullExtra = {
        signal: abortController.signal,
        sessionId: capturedTransport?.sessionId,
        _meta: request.params?._meta,
        sendNotification: async (notification) => {
          const notificationOptions = { relatedRequestId: request.id };
          if (relatedTaskId) {
            notificationOptions.relatedTask = { taskId: relatedTaskId };
          }
          await this.notification(notification, notificationOptions);
        },
        sendRequest: async (r, resultSchema, options) => {
          const requestOptions = { ...options, relatedRequestId: request.id };
          if (relatedTaskId && !requestOptions.relatedTask) {
            requestOptions.relatedTask = { taskId: relatedTaskId };
          }
          const effectiveTaskId = requestOptions.relatedTask?.taskId ?? relatedTaskId;
          if (effectiveTaskId && taskStore) {
            await taskStore.updateTaskStatus(effectiveTaskId, "input_required");
          }
          return await this.request(r, resultSchema, requestOptions);
        },
        authInfo: extra?.authInfo,
        requestId: request.id,
        requestInfo: extra?.requestInfo,
        taskId: relatedTaskId,
        taskStore,
        taskRequestedTtl: taskCreationParams?.ttl,
        closeSSEStream: extra?.closeSSEStream,
        closeStandaloneSSEStream: extra?.closeStandaloneSSEStream
      };
      Promise.resolve().then(() => {
        if (taskCreationParams) {
          this.assertTaskHandlerCapability(request.method);
        }
      }).then(() => handler(request, fullExtra)).then(async (result) => {
        if (abortController.signal.aborted) {
          return;
        }
        const response = {
          result,
          jsonrpc: "2.0",
          id: request.id
        };
        if (relatedTaskId && this._taskMessageQueue) {
          await this._enqueueTaskMessage(relatedTaskId, {
            type: "response",
            message: response,
            timestamp: Date.now()
          }, capturedTransport?.sessionId);
        } else {
          await capturedTransport?.send(response);
        }
      }, async (error2) => {
        if (abortController.signal.aborted) {
          return;
        }
        const errorResponse = {
          jsonrpc: "2.0",
          id: request.id,
          error: {
            code: Number.isSafeInteger(error2["code"]) ? error2["code"] : ErrorCode.InternalError,
            message: error2.message ?? "Internal error",
            ...error2["data"] !== void 0 && { data: error2["data"] }
          }
        };
        if (relatedTaskId && this._taskMessageQueue) {
          await this._enqueueTaskMessage(relatedTaskId, {
            type: "error",
            message: errorResponse,
            timestamp: Date.now()
          }, capturedTransport?.sessionId);
        } else {
          await capturedTransport?.send(errorResponse);
        }
      }).catch((error2) => this._onerror(new Error(`Failed to send response: ${error2}`))).finally(() => {
        this._requestHandlerAbortControllers.delete(request.id);
      });
    }
    _onprogress(notification) {
      const { progressToken, ...params } = notification.params;
      const messageId = Number(progressToken);
      const handler = this._progressHandlers.get(messageId);
      if (!handler) {
        this._onerror(new Error(`Received a progress notification for an unknown token: ${JSON.stringify(notification)}`));
        return;
      }
      const responseHandler = this._responseHandlers.get(messageId);
      const timeoutInfo = this._timeoutInfo.get(messageId);
      if (timeoutInfo && responseHandler && timeoutInfo.resetTimeoutOnProgress) {
        try {
          this._resetTimeout(messageId);
        } catch (error2) {
          this._responseHandlers.delete(messageId);
          this._progressHandlers.delete(messageId);
          this._cleanupTimeout(messageId);
          responseHandler(error2);
          return;
        }
      }
      handler(params);
    }
    _onresponse(response) {
      const messageId = Number(response.id);
      const resolver = this._requestResolvers.get(messageId);
      if (resolver) {
        this._requestResolvers.delete(messageId);
        if (isJSONRPCResultResponse(response)) {
          resolver(response);
        } else {
          const error2 = new McpError(response.error.code, response.error.message, response.error.data);
          resolver(error2);
        }
        return;
      }
      const handler = this._responseHandlers.get(messageId);
      if (handler === void 0) {
        this._onerror(new Error(`Received a response for an unknown message ID: ${JSON.stringify(response)}`));
        return;
      }
      this._responseHandlers.delete(messageId);
      this._cleanupTimeout(messageId);
      let isTaskResponse = false;
      if (isJSONRPCResultResponse(response) && response.result && typeof response.result === "object") {
        const result = response.result;
        if (result.task && typeof result.task === "object") {
          const task = result.task;
          if (typeof task.taskId === "string") {
            isTaskResponse = true;
            this._taskProgressTokens.set(task.taskId, messageId);
          }
        }
      }
      if (!isTaskResponse) {
        this._progressHandlers.delete(messageId);
      }
      if (isJSONRPCResultResponse(response)) {
        handler(response);
      } else {
        const error2 = McpError.fromError(response.error.code, response.error.message, response.error.data);
        handler(error2);
      }
    }
    get transport() {
      return this._transport;
    }
    /**
     * Closes the connection.
     */
    async close() {
      await this._transport?.close();
    }
    /**
     * Sends a request and returns an AsyncGenerator that yields response messages.
     * The generator is guaranteed to end with either a 'result' or 'error' message.
     *
     * @example
     * ```typescript
     * const stream = protocol.requestStream(request, resultSchema, options);
     * for await (const message of stream) {
     *   switch (message.type) {
     *     case 'taskCreated':
     *       console.log('Task created:', message.task.taskId);
     *       break;
     *     case 'taskStatus':
     *       console.log('Task status:', message.task.status);
     *       break;
     *     case 'result':
     *       console.log('Final result:', message.result);
     *       break;
     *     case 'error':
     *       console.error('Error:', message.error);
     *       break;
     *   }
     * }
     * ```
     *
     * @experimental Use `client.experimental.tasks.requestStream()` to access this method.
     */
    async *requestStream(request, resultSchema, options) {
      const { task } = options ?? {};
      if (!task) {
        try {
          const result = await this.request(request, resultSchema, options);
          yield { type: "result", result };
        } catch (error2) {
          yield {
            type: "error",
            error: error2 instanceof McpError ? error2 : new McpError(ErrorCode.InternalError, String(error2))
          };
        }
        return;
      }
      let taskId;
      try {
        const createResult = await this.request(request, CreateTaskResultSchema, options);
        if (createResult.task) {
          taskId = createResult.task.taskId;
          yield { type: "taskCreated", task: createResult.task };
        } else {
          throw new McpError(ErrorCode.InternalError, "Task creation did not return a task");
        }
        while (true) {
          const task2 = await this.getTask({ taskId }, options);
          yield { type: "taskStatus", task: task2 };
          if (isTerminal(task2.status)) {
            if (task2.status === "completed") {
              const result = await this.getTaskResult({ taskId }, resultSchema, options);
              yield { type: "result", result };
            } else if (task2.status === "failed") {
              yield {
                type: "error",
                error: new McpError(ErrorCode.InternalError, `Task ${taskId} failed`)
              };
            } else if (task2.status === "cancelled") {
              yield {
                type: "error",
                error: new McpError(ErrorCode.InternalError, `Task ${taskId} was cancelled`)
              };
            }
            return;
          }
          if (task2.status === "input_required") {
            const result = await this.getTaskResult({ taskId }, resultSchema, options);
            yield { type: "result", result };
            return;
          }
          const pollInterval = task2.pollInterval ?? this._options?.defaultTaskPollInterval ?? 1e3;
          await new Promise((resolve) => setTimeout(resolve, pollInterval));
          options?.signal?.throwIfAborted();
        }
      } catch (error2) {
        yield {
          type: "error",
          error: error2 instanceof McpError ? error2 : new McpError(ErrorCode.InternalError, String(error2))
        };
      }
    }
    /**
     * Sends a request and waits for a response.
     *
     * Do not use this method to emit notifications! Use notification() instead.
     */
    request(request, resultSchema, options) {
      const { relatedRequestId, resumptionToken, onresumptiontoken, task, relatedTask } = options ?? {};
      return new Promise((resolve, reject) => {
        const earlyReject = (error2) => {
          reject(error2);
        };
        if (!this._transport) {
          earlyReject(new Error("Not connected"));
          return;
        }
        if (this._options?.enforceStrictCapabilities === true) {
          try {
            this.assertCapabilityForMethod(request.method);
            if (task) {
              this.assertTaskCapability(request.method);
            }
          } catch (e) {
            earlyReject(e);
            return;
          }
        }
        options?.signal?.throwIfAborted();
        const messageId = this._requestMessageId++;
        const jsonrpcRequest = {
          ...request,
          jsonrpc: "2.0",
          id: messageId
        };
        if (options?.onprogress) {
          this._progressHandlers.set(messageId, options.onprogress);
          jsonrpcRequest.params = {
            ...request.params,
            _meta: {
              ...request.params?._meta || {},
              progressToken: messageId
            }
          };
        }
        if (task) {
          jsonrpcRequest.params = {
            ...jsonrpcRequest.params,
            task
          };
        }
        if (relatedTask) {
          jsonrpcRequest.params = {
            ...jsonrpcRequest.params,
            _meta: {
              ...jsonrpcRequest.params?._meta || {},
              [RELATED_TASK_META_KEY]: relatedTask
            }
          };
        }
        const cancel = (reason) => {
          this._responseHandlers.delete(messageId);
          this._progressHandlers.delete(messageId);
          this._cleanupTimeout(messageId);
          this._transport?.send({
            jsonrpc: "2.0",
            method: "notifications/cancelled",
            params: {
              requestId: messageId,
              reason: String(reason)
            }
          }, { relatedRequestId, resumptionToken, onresumptiontoken }).catch((error3) => this._onerror(new Error(`Failed to send cancellation: ${error3}`)));
          const error2 = reason instanceof McpError ? reason : new McpError(ErrorCode.RequestTimeout, String(reason));
          reject(error2);
        };
        this._responseHandlers.set(messageId, (response) => {
          if (options?.signal?.aborted) {
            return;
          }
          if (response instanceof Error) {
            return reject(response);
          }
          try {
            const parseResult = safeParse2(resultSchema, response.result);
            if (!parseResult.success) {
              reject(parseResult.error);
            } else {
              resolve(parseResult.data);
            }
          } catch (error2) {
            reject(error2);
          }
        });
        options?.signal?.addEventListener("abort", () => {
          cancel(options?.signal?.reason);
        });
        const timeout = options?.timeout ?? DEFAULT_REQUEST_TIMEOUT_MSEC;
        const timeoutHandler = () => cancel(McpError.fromError(ErrorCode.RequestTimeout, "Request timed out", { timeout }));
        this._setupTimeout(messageId, timeout, options?.maxTotalTimeout, timeoutHandler, options?.resetTimeoutOnProgress ?? false);
        const relatedTaskId = relatedTask?.taskId;
        if (relatedTaskId) {
          const responseResolver = (response) => {
            const handler = this._responseHandlers.get(messageId);
            if (handler) {
              handler(response);
            } else {
              this._onerror(new Error(`Response handler missing for side-channeled request ${messageId}`));
            }
          };
          this._requestResolvers.set(messageId, responseResolver);
          this._enqueueTaskMessage(relatedTaskId, {
            type: "request",
            message: jsonrpcRequest,
            timestamp: Date.now()
          }).catch((error2) => {
            this._cleanupTimeout(messageId);
            reject(error2);
          });
        } else {
          this._transport.send(jsonrpcRequest, { relatedRequestId, resumptionToken, onresumptiontoken }).catch((error2) => {
            this._cleanupTimeout(messageId);
            reject(error2);
          });
        }
      });
    }
    /**
     * Gets the current status of a task.
     *
     * @experimental Use `client.experimental.tasks.getTask()` to access this method.
     */
    async getTask(params, options) {
      return this.request({ method: "tasks/get", params }, GetTaskResultSchema, options);
    }
    /**
     * Retrieves the result of a completed task.
     *
     * @experimental Use `client.experimental.tasks.getTaskResult()` to access this method.
     */
    async getTaskResult(params, resultSchema, options) {
      return this.request({ method: "tasks/result", params }, resultSchema, options);
    }
    /**
     * Lists tasks, optionally starting from a pagination cursor.
     *
     * @experimental Use `client.experimental.tasks.listTasks()` to access this method.
     */
    async listTasks(params, options) {
      return this.request({ method: "tasks/list", params }, ListTasksResultSchema, options);
    }
    /**
     * Cancels a specific task.
     *
     * @experimental Use `client.experimental.tasks.cancelTask()` to access this method.
     */
    async cancelTask(params, options) {
      return this.request({ method: "tasks/cancel", params }, CancelTaskResultSchema, options);
    }
    /**
     * Emits a notification, which is a one-way message that does not expect a response.
     */
    async notification(notification, options) {
      if (!this._transport) {
        throw new Error("Not connected");
      }
      this.assertNotificationCapability(notification.method);
      const relatedTaskId = options?.relatedTask?.taskId;
      if (relatedTaskId) {
        const jsonrpcNotification2 = {
          ...notification,
          jsonrpc: "2.0",
          params: {
            ...notification.params,
            _meta: {
              ...notification.params?._meta || {},
              [RELATED_TASK_META_KEY]: options.relatedTask
            }
          }
        };
        await this._enqueueTaskMessage(relatedTaskId, {
          type: "notification",
          message: jsonrpcNotification2,
          timestamp: Date.now()
        });
        return;
      }
      const debouncedMethods = this._options?.debouncedNotificationMethods ?? [];
      const canDebounce = debouncedMethods.includes(notification.method) && !notification.params && !options?.relatedRequestId && !options?.relatedTask;
      if (canDebounce) {
        if (this._pendingDebouncedNotifications.has(notification.method)) {
          return;
        }
        this._pendingDebouncedNotifications.add(notification.method);
        Promise.resolve().then(() => {
          this._pendingDebouncedNotifications.delete(notification.method);
          if (!this._transport) {
            return;
          }
          let jsonrpcNotification2 = {
            ...notification,
            jsonrpc: "2.0"
          };
          if (options?.relatedTask) {
            jsonrpcNotification2 = {
              ...jsonrpcNotification2,
              params: {
                ...jsonrpcNotification2.params,
                _meta: {
                  ...jsonrpcNotification2.params?._meta || {},
                  [RELATED_TASK_META_KEY]: options.relatedTask
                }
              }
            };
          }
          this._transport?.send(jsonrpcNotification2, options).catch((error2) => this._onerror(error2));
        });
        return;
      }
      let jsonrpcNotification = {
        ...notification,
        jsonrpc: "2.0"
      };
      if (options?.relatedTask) {
        jsonrpcNotification = {
          ...jsonrpcNotification,
          params: {
            ...jsonrpcNotification.params,
            _meta: {
              ...jsonrpcNotification.params?._meta || {},
              [RELATED_TASK_META_KEY]: options.relatedTask
            }
          }
        };
      }
      await this._transport.send(jsonrpcNotification, options);
    }
    /**
     * Registers a handler to invoke when this protocol object receives a request with the given method.
     *
     * Note that this will replace any previous request handler for the same method.
     */
    setRequestHandler(requestSchema, handler) {
      const method = getMethodLiteral(requestSchema);
      this.assertRequestHandlerCapability(method);
      this._requestHandlers.set(method, (request, extra) => {
        const parsed = parseWithCompat(requestSchema, request);
        return Promise.resolve(handler(parsed, extra));
      });
    }
    /**
     * Removes the request handler for the given method.
     */
    removeRequestHandler(method) {
      this._requestHandlers.delete(method);
    }
    /**
     * Asserts that a request handler has not already been set for the given method, in preparation for a new one being automatically installed.
     */
    assertCanSetRequestHandler(method) {
      if (this._requestHandlers.has(method)) {
        throw new Error(`A request handler for ${method} already exists, which would be overridden`);
      }
    }
    /**
     * Registers a handler to invoke when this protocol object receives a notification with the given method.
     *
     * Note that this will replace any previous notification handler for the same method.
     */
    setNotificationHandler(notificationSchema, handler) {
      const method = getMethodLiteral(notificationSchema);
      this._notificationHandlers.set(method, (notification) => {
        const parsed = parseWithCompat(notificationSchema, notification);
        return Promise.resolve(handler(parsed));
      });
    }
    /**
     * Removes the notification handler for the given method.
     */
    removeNotificationHandler(method) {
      this._notificationHandlers.delete(method);
    }
    /**
     * Cleans up the progress handler associated with a task.
     * This should be called when a task reaches a terminal status.
     */
    _cleanupTaskProgressHandler(taskId) {
      const progressToken = this._taskProgressTokens.get(taskId);
      if (progressToken !== void 0) {
        this._progressHandlers.delete(progressToken);
        this._taskProgressTokens.delete(taskId);
      }
    }
    /**
     * Enqueues a task-related message for side-channel delivery via tasks/result.
     * @param taskId The task ID to associate the message with
     * @param message The message to enqueue
     * @param sessionId Optional session ID for binding the operation to a specific session
     * @throws Error if taskStore is not configured or if enqueue fails (e.g., queue overflow)
     *
     * Note: If enqueue fails, it's the TaskMessageQueue implementation's responsibility to handle
     * the error appropriately (e.g., by failing the task, logging, etc.). The Protocol layer
     * simply propagates the error.
     */
    async _enqueueTaskMessage(taskId, message, sessionId) {
      if (!this._taskStore || !this._taskMessageQueue) {
        throw new Error("Cannot enqueue task message: taskStore and taskMessageQueue are not configured");
      }
      const maxQueueSize = this._options?.maxTaskQueueSize;
      await this._taskMessageQueue.enqueue(taskId, message, sessionId, maxQueueSize);
    }
    /**
     * Clears the message queue for a task and rejects any pending request resolvers.
     * @param taskId The task ID whose queue should be cleared
     * @param sessionId Optional session ID for binding the operation to a specific session
     */
    async _clearTaskQueue(taskId, sessionId) {
      if (this._taskMessageQueue) {
        const messages = await this._taskMessageQueue.dequeueAll(taskId, sessionId);
        for (const message of messages) {
          if (message.type === "request" && isJSONRPCRequest(message.message)) {
            const requestId = message.message.id;
            const resolver = this._requestResolvers.get(requestId);
            if (resolver) {
              resolver(new McpError(ErrorCode.InternalError, "Task cancelled or completed"));
              this._requestResolvers.delete(requestId);
            } else {
              this._onerror(new Error(`Resolver missing for request ${requestId} during task ${taskId} cleanup`));
            }
          }
        }
      }
    }
    /**
     * Waits for a task update (new messages or status change) with abort signal support.
     * Uses polling to check for updates at the task's configured poll interval.
     * @param taskId The task ID to wait for
     * @param signal Abort signal to cancel the wait
     * @returns Promise that resolves when an update occurs or rejects if aborted
     */
    async _waitForTaskUpdate(taskId, signal) {
      let interval = this._options?.defaultTaskPollInterval ?? 1e3;
      try {
        const task = await this._taskStore?.getTask(taskId);
        if (task?.pollInterval) {
          interval = task.pollInterval;
        }
      } catch {
      }
      return new Promise((resolve, reject) => {
        if (signal.aborted) {
          reject(new McpError(ErrorCode.InvalidRequest, "Request cancelled"));
          return;
        }
        const timeoutId = setTimeout(resolve, interval);
        signal.addEventListener("abort", () => {
          clearTimeout(timeoutId);
          reject(new McpError(ErrorCode.InvalidRequest, "Request cancelled"));
        }, { once: true });
      });
    }
    requestTaskStore(request, sessionId) {
      const taskStore = this._taskStore;
      if (!taskStore) {
        throw new Error("No task store configured");
      }
      return {
        createTask: async (taskParams) => {
          if (!request) {
            throw new Error("No request provided");
          }
          return await taskStore.createTask(taskParams, request.id, {
            method: request.method,
            params: request.params
          }, sessionId);
        },
        getTask: async (taskId) => {
          const task = await taskStore.getTask(taskId, sessionId);
          if (!task) {
            throw new McpError(ErrorCode.InvalidParams, "Failed to retrieve task: Task not found");
          }
          return task;
        },
        storeTaskResult: async (taskId, status, result) => {
          await taskStore.storeTaskResult(taskId, status, result, sessionId);
          const task = await taskStore.getTask(taskId, sessionId);
          if (task) {
            const notification = TaskStatusNotificationSchema.parse({
              method: "notifications/tasks/status",
              params: task
            });
            await this.notification(notification);
            if (isTerminal(task.status)) {
              this._cleanupTaskProgressHandler(taskId);
            }
          }
        },
        getTaskResult: (taskId) => {
          return taskStore.getTaskResult(taskId, sessionId);
        },
        updateTaskStatus: async (taskId, status, statusMessage) => {
          const task = await taskStore.getTask(taskId, sessionId);
          if (!task) {
            throw new McpError(ErrorCode.InvalidParams, `Task "${taskId}" not found - it may have been cleaned up`);
          }
          if (isTerminal(task.status)) {
            throw new McpError(ErrorCode.InvalidParams, `Cannot update task "${taskId}" from terminal status "${task.status}" to "${status}". Terminal states (completed, failed, cancelled) cannot transition to other states.`);
          }
          await taskStore.updateTaskStatus(taskId, status, statusMessage, sessionId);
          const updatedTask = await taskStore.getTask(taskId, sessionId);
          if (updatedTask) {
            const notification = TaskStatusNotificationSchema.parse({
              method: "notifications/tasks/status",
              params: updatedTask
            });
            await this.notification(notification);
            if (isTerminal(updatedTask.status)) {
              this._cleanupTaskProgressHandler(taskId);
            }
          }
        },
        listTasks: (cursor) => {
          return taskStore.listTasks(cursor, sessionId);
        }
      };
    }
  };

  // node_modules/@modelcontextprotocol/ext-apps/dist/src/app.js
  var Kc = Object.defineProperty;
  var s = (r, i) => {
    for (var o in i)
      Kc(r, o, { get: i[o], enumerable: true, configurable: true, set: (t) => i[o] = () => t });
  };
  var Yn = class {
    eventTarget;
    eventSource;
    messageListener;
    constructor(r = window.parent, i) {
      this.eventTarget = r;
      this.eventSource = i;
      this.messageListener = (o) => {
        if (i && o.source !== this.eventSource) {
          console.debug("Ignoring message from unknown source", o);
          return;
        }
        let t = JSONRPCMessageSchema.safeParse(o.data);
        if (t.success)
          console.debug("Parsed message", t.data), this.onmessage?.(t.data);
        else
          console.error("Failed to parse message", t.error.message, o), this.onerror?.(Error("Invalid JSON-RPC message received: " + t.error.message));
      };
    }
    async start() {
      window.addEventListener("message", this.messageListener);
    }
    async send(r, i) {
      console.debug("Sending message", r), this.eventTarget.postMessage(r, "*");
    }
    async close() {
      window.removeEventListener("message", this.messageListener), this.onclose?.();
    }
    onclose;
    onerror;
    onmessage;
    sessionId;
    setProtocolVersion;
  };
  var wv = "2026-01-26";
  var g = {};
  s(g, { xor: () => yl, xid: () => Nl, void: () => xl, uuidv7: () => cl, uuidv6: () => ll, uuidv4: () => el, uuid: () => gl, util: () => D, url: () => Il, uppercase: () => Kr, unknown: () => Nr, union: () => ev, undefined: () => Ml, ulid: () => wl, uint64: () => Bl, uint32: () => ml, tuple: () => Qg, trim: () => Fr, treeifyError: () => Xv, transform: () => cv, toUpperCase: () => Hr, toLowerCase: () => Br, toJSONSchema: () => Qi, templateLiteral: () => ec, symbol: () => Hl, superRefine: () => ee, success: () => uc, stringbool: () => Dc, stringFormat: () => Xl, string: () => Mi, strictObject: () => fl, startsWith: () => Yr, slugify: () => Mr, size: () => kr, setErrorMap: () => J6, set: () => nc, safeParseAsync: () => lg, safeParse: () => eg, safeEncodeAsync: () => Dg, safeEncode: () => Ug, safeDecodeAsync: () => wg, safeDecode: () => kg, registry: () => $i, regexes: () => x, regex: () => Vr, refine: () => ge, record: () => mg, readonly: () => ie, property: () => Ai, promise: () => lc, prettifyError: () => Vv, preprocess: () => Nc, prefault: () => hg, positive: () => Gi, pipe: () => En, partialRecord: () => pl, parseAsync: () => gg, parse: () => $g, overwrite: () => d, optional: () => Jn, object: () => Cl, number: () => Og, nullish: () => tc, nullable: () => Ln, null: () => Jg, normalize: () => Tr, nonpositive: () => Xi, nonoptional: () => yg, nonnegative: () => Vi, never: () => gv, negative: () => Wi, nativeEnum: () => ic, nanoid: () => Ul, nan: () => $c, multipleOf: () => $r, minSize: () => a, minLength: () => nr, mime: () => mr, meta: () => Uc, maxSize: () => gr, maxLength: () => Dr, map: () => rc, mac: () => zl, lte: () => M, lt: () => h, lowercase: () => Ar, looseRecord: () => sl, looseObject: () => hl, locales: () => On, literal: () => vc, length: () => wr, lazy: () => te, ksuid: () => Ol, keyof: () => dl, jwt: () => Wl, json: () => wc, iso: () => Zr, ipv6: () => Pl, ipv4: () => Sl, intersection: () => qg, int64: () => Fl, int32: () => Ql, int: () => Ri, instanceof: () => kc, includes: () => qr, httpUrl: () => bl, hostname: () => Vl, hex: () => Al, hash: () => Kl, guid: () => $l, gte: () => Y, gt: () => y, globalRegistry: () => A, getErrorMap: () => L6, function: () => cc, fromJSONSchema: () => Sc, formatError: () => en, float64: () => Yl, float32: () => ql, flattenError: () => gn, file: () => oc, exactOptional: () => xg, enum: () => lv, endsWith: () => Qr, encodeAsync: () => bg, encode: () => cg, emoji: () => _l, email: () => ul, e164: () => Gl, discriminatedUnion: () => al, describe: () => _c, decodeAsync: () => _g, decode: () => Ig, date: () => Zl, custom: () => bc, cuid2: () => Dl, cuid: () => kl, core: () => ir, config: () => V, coerce: () => ce, codec: () => gc, clone: () => q, cidrv6: () => Jl, cidrv4: () => jl, check: () => Ic, catch: () => sg, boolean: () => Sg, bigint: () => Tl, base64url: () => El, base64: () => Ll, array: () => Xn, any: () => Rl, _function: () => cc, _default: () => Cg, _ZodString: () => xi, ZodXor: () => Vg, ZodXID: () => ai, ZodVoid: () => Wg, ZodUnknown: () => Eg, ZodUnion: () => An, ZodUndefined: () => Pg, ZodUUID: () => p, ZodURL: () => Gn, ZodULID: () => yi, ZodType: () => P, ZodTuple: () => Yg, ZodTransform: () => Mg, ZodTemplateLiteral: () => ve, ZodSymbol: () => zg, ZodSuccess: () => ag, ZodStringFormat: () => G, ZodString: () => Cr, ZodSet: () => Fg, ZodRecord: () => Kn, ZodRealError: () => B, ZodReadonly: () => ne, ZodPromise: () => ue, ZodPrefault: () => fg, ZodPipe: () => _v, ZodOptional: () => Iv, ZodObject: () => Vn, ZodNumberFormat: () => Or, ZodNumber: () => hr, ZodNullable: () => Zg, ZodNull: () => jg, ZodNonOptional: () => bv, ZodNever: () => Gg, ZodNanoID: () => Ci, ZodNaN: () => re, ZodMap: () => Tg, ZodMAC: () => Ng, ZodLiteral: () => Bg, ZodLazy: () => oe, ZodKSUID: () => pi, ZodJWT: () => uv, ZodIssueCode: () => j6, ZodIntersection: () => Kg, ZodISOTime: () => Bi, ZodISODuration: () => Hi, ZodISODateTime: () => Ti, ZodISODate: () => Fi, ZodIPv6: () => rv, ZodIPv4: () => si, ZodGUID: () => jn, ZodFunction: () => $e, ZodFirstPartyTypeKind: () => le, ZodFile: () => Hg, ZodExactOptional: () => Rg, ZodError: () => z6, ZodEnum: () => dr, ZodEmoji: () => di, ZodEmail: () => Zi, ZodE164: () => tv, ZodDiscriminatedUnion: () => Ag, ZodDefault: () => dg, ZodDate: () => Wn, ZodCustomStringFormat: () => fr, ZodCustom: () => qn, ZodCodec: () => Uv, ZodCatch: () => pg, ZodCUID2: () => hi, ZodCUID: () => fi, ZodCIDRv6: () => iv, ZodCIDRv4: () => nv, ZodBoolean: () => yr, ZodBigIntFormat: () => $v, ZodBigInt: () => ar, ZodBase64URL: () => ov, ZodBase64: () => vv, ZodArray: () => Xg, ZodAny: () => Lg, TimePrecision: () => Qu, NEVER: () => Nv, $output: () => Xu, $input: () => Vu, $brand: () => Ov });
  var ir = {};
  s(ir, { version: () => Lo, util: () => D, treeifyError: () => Xv, toJSONSchema: () => Qi, toDotPath: () => We, safeParseAsync: () => Kv, safeParse: () => Av, safeEncodeAsync: () => EI, safeEncode: () => JI, safeDecodeAsync: () => GI, safeDecode: () => LI, registry: () => $i, regexes: () => x, process: () => L, prettifyError: () => Vv, parseAsync: () => Fn, parse: () => Tn, meta: () => k$, locales: () => On, isValidJWT: () => fe, isValidBase64URL: () => Ce, isValidBase64: () => ho, initializeContext: () => er, globalRegistry: () => A, globalConfig: () => sr, formatError: () => en, flattenError: () => gn, finalize: () => cr, extractDefs: () => lr, encodeAsync: () => PI, encode: () => SI, describe: () => U$, decodeAsync: () => jI, decode: () => zI, createToJSONSchemaMethod: () => w$, createStandardJSONSchemaMethod: () => xr, config: () => V, clone: () => q, _xor: () => p4, _xid: () => wi, _void: () => u$, _uuidv7: () => Ii, _uuidv6: () => ci, _uuidv4: () => li, _uuid: () => ei, _url: () => zn, _uppercase: () => Kr, _unknown: () => o$, _union: () => a4, _undefined: () => n$, _ulid: () => Di, _uint64: () => su, _uint32: () => Cu, _tuple: () => n6, _trim: () => Fr, _transform: () => g6, _toUpperCase: () => Hr, _toLowerCase: () => Br, _templateLiteral: () => D6, _symbol: () => r$, _superRefine: () => _$, _success: () => b6, _stringbool: () => D$, _stringFormat: () => Rr, _string: () => Ku, _startsWith: () => Yr, _slugify: () => Mr, _size: () => kr, _set: () => o6, _safeParseAsync: () => Gr, _safeParse: () => Er, _safeEncodeAsync: () => dn, _safeEncode: () => xn, _safeDecodeAsync: () => Cn, _safeDecode: () => Zn, _regex: () => Vr, _refine: () => b$, _record: () => i6, _readonly: () => k6, _property: () => Ai, _promise: () => N6, _positive: () => Gi, _pipe: () => U6, _parseAsync: () => Lr, _parse: () => Jr, _overwrite: () => d, _optional: () => e6, _number: () => Hu, _nullable: () => l6, _null: () => i$, _normalize: () => Tr, _nonpositive: () => Xi, _nonoptional: () => I6, _nonnegative: () => Vi, _never: () => t$, _negative: () => Wi, _nativeEnum: () => u6, _nanoid: () => _i, _nan: () => e$, _multipleOf: () => $r, _minSize: () => a, _minLength: () => nr, _min: () => Y, _mime: () => mr, _maxSize: () => gr, _maxLength: () => Dr, _max: () => M, _map: () => v6, _mac: () => Yu, _lte: () => M, _lt: () => h, _lowercase: () => Ar, _literal: () => $6, _length: () => wr, _lazy: () => w6, _ksuid: () => Ni, _jwt: () => Ei, _isoTime: () => Fu, _isoDuration: () => Bu, _isoDateTime: () => mu, _isoDate: () => Tu, _ipv6: () => Si, _ipv4: () => Oi, _intersection: () => r6, _int64: () => pu, _int32: () => du, _int: () => Ru, _includes: () => qr, _guid: () => Sn, _gte: () => Y, _gt: () => y, _float64: () => Zu, _float32: () => xu, _file: () => c$, _enum: () => t6, _endsWith: () => Qr, _encodeAsync: () => Mn, _encode: () => Bn, _emoji: () => bi, _email: () => gi, _e164: () => Li, _discriminatedUnion: () => s4, _default: () => c6, _decodeAsync: () => Rn, _decode: () => Hn, _date: () => $$, _custom: () => I$, _cuid2: () => ki, _cuid: () => Ui, _coercedString: () => qu, _coercedNumber: () => Mu, _coercedDate: () => g$, _coercedBoolean: () => hu, _coercedBigint: () => au, _cidrv6: () => Pi, _cidrv4: () => zi, _check: () => vl, _catch: () => _6, _boolean: () => fu, _bigint: () => yu, _base64url: () => Ji, _base64: () => ji, _array: () => l$, _any: () => v$, TimePrecision: () => Qu, NEVER: () => Nv, JSONSchemaGenerator: () => ig, JSONSchema: () => ol, Doc: () => an, $output: () => Xu, $input: () => Vu, $constructor: () => c, $brand: () => Ov, $ZodXor: () => bt, $ZodXID: () => mo, $ZodVoid: () => et, $ZodUnknown: () => $t, $ZodUnion: () => _n, $ZodUndefined: () => ot, $ZodUUID: () => Wo, $ZodURL: () => Vo, $ZodULID: () => Qo, $ZodType: () => z, $ZodTuple: () => ti, $ZodTransform: () => zt, $ZodTemplateLiteral: () => Kt, $ZodSymbol: () => vt, $ZodSuccess: () => Gt, $ZodStringFormat: () => E, $ZodString: () => Ur, $ZodSet: () => wt, $ZodRegistry: () => Au, $ZodRecord: () => kt, $ZodRealError: () => F, $ZodReadonly: () => At, $ZodPromise: () => Yt, $ZodPrefault: () => Lt, $ZodPipe: () => Vt, $ZodOptional: () => ui, $ZodObjectJIT: () => It, $ZodObject: () => ae, $ZodNumberFormat: () => nt, $ZodNumber: () => vi, $ZodNullable: () => jt, $ZodNull: () => tt, $ZodNonOptional: () => Et, $ZodNever: () => gt, $ZodNanoID: () => Ko, $ZodNaN: () => Xt, $ZodMap: () => Dt, $ZodMAC: () => Zo, $ZodLiteral: () => Ot, $ZodLazy: () => Qt, $ZodKSUID: () => To, $ZodJWT: () => so, $ZodIntersection: () => Ut, $ZodISOTime: () => Ho, $ZodISODuration: () => Mo, $ZodISODateTime: () => Fo, $ZodISODate: () => Bo, $ZodIPv6: () => xo, $ZodIPv4: () => Ro, $ZodGUID: () => Go, $ZodFunction: () => qt, $ZodFile: () => St, $ZodExactOptional: () => Pt, $ZodError: () => $n, $ZodEnum: () => Nt, $ZodEncodeError: () => Ir, $ZodEmoji: () => Ao, $ZodEmail: () => Xo, $ZodE164: () => po, $ZodDiscriminatedUnion: () => _t, $ZodDefault: () => Jt, $ZodDate: () => lt, $ZodCustomStringFormat: () => rt, $ZodCustom: () => mt, $ZodCodec: () => Un, $ZodCheckUpperCase: () => No, $ZodCheckStringFormat: () => Wr, $ZodCheckStartsWith: () => So, $ZodCheckSizeEquals: () => bo, $ZodCheckRegex: () => Do, $ZodCheckProperty: () => Po, $ZodCheckOverwrite: () => Jo, $ZodCheckNumberFormat: () => eo, $ZodCheckMultipleOf: () => go, $ZodCheckMinSize: () => Io, $ZodCheckMinLength: () => Uo, $ZodCheckMimeType: () => jo, $ZodCheckMaxSize: () => co, $ZodCheckMaxLength: () => _o, $ZodCheckLowerCase: () => wo, $ZodCheckLessThan: () => hn, $ZodCheckLengthEquals: () => ko, $ZodCheckIncludes: () => Oo, $ZodCheckGreaterThan: () => yn, $ZodCheckEndsWith: () => zo, $ZodCheckBigIntFormat: () => lo, $ZodCheck: () => W, $ZodCatch: () => Wt, $ZodCUID2: () => Yo, $ZodCUID: () => qo, $ZodCIDRv6: () => fo, $ZodCIDRv4: () => Co, $ZodBoolean: () => bn, $ZodBigIntFormat: () => it, $ZodBigInt: () => oi, $ZodBase64URL: () => ao, $ZodBase64: () => yo, $ZodAsyncError: () => f, $ZodArray: () => ct, $ZodAny: () => ut });
  var Nv = Object.freeze({ status: "aborted" });
  function c(r, i, o) {
    function t($, l) {
      if (!$._zod)
        Object.defineProperty($, "_zod", { value: { def: l, constr: u, traits: /* @__PURE__ */ new Set() }, enumerable: false });
      if ($._zod.traits.has(r))
        return;
      $._zod.traits.add(r), i($, l);
      let e = u.prototype, I = Object.keys(e);
      for (let _ = 0; _ < I.length; _++) {
        let N = I[_];
        if (!(N in $))
          $[N] = e[N].bind($);
      }
    }
    let n = o?.Parent ?? Object;
    class v extends n {
    }
    Object.defineProperty(v, "name", { value: r });
    function u($) {
      var l;
      let e = o?.Parent ? new v() : this;
      t(e, $), (l = e._zod).deferred ?? (l.deferred = []);
      for (let I of e._zod.deferred)
        I();
      return e;
    }
    return Object.defineProperty(u, "init", { value: t }), Object.defineProperty(u, Symbol.hasInstance, { value: ($) => {
      if (o?.Parent && $ instanceof o.Parent)
        return true;
      return $?._zod?.traits?.has(r);
    } }), Object.defineProperty(u, "name", { value: r }), u;
  }
  var Ov = Symbol("zod_brand");
  var f = class extends Error {
    constructor() {
      super("Encountered Promise during synchronous parse. Use .parseAsync() instead.");
    }
  };
  var Ir = class extends Error {
    constructor(r) {
      super(`Encountered unidirectional transform during encode: ${r}`);
      this.name = "ZodEncodeError";
    }
  };
  var sr = {};
  function V(r) {
    if (r)
      Object.assign(sr, r);
    return sr;
  }
  var D = {};
  s(D, { unwrapMessage: () => rn, uint8ArrayToHex: () => NI, uint8ArrayToBase64url: () => DI, uint8ArrayToBase64: () => Le, stringifyPrimitive: () => U, slugify: () => Pv, shallowClone: () => Jv, safeExtend: () => cI, required: () => _I, randomString: () => oI, propertyKeyTypes: () => on, promiseAllObject: () => vI, primitiveTypes: () => Lv, prefixIssues: () => H, pick: () => gI, partial: () => bI, parsedType: () => k, optionalKeys: () => Ev, omit: () => eI, objectClone: () => rI, numKeys: () => tI, nullish: () => vr, normalizeParams: () => w, mergeDefs: () => rr, merge: () => II, jsonStringifyReplacer: () => zr, joinValues: () => b, issue: () => jr, isPlainObject: () => tr, isObject: () => br, hexToUint8Array: () => wI, getSizableOrigin: () => tn, getParsedType: () => uI, getLengthableOrigin: () => un, getEnumValues: () => nn, getElementAtPath: () => iI, floatSafeRemainder: () => zv, finalizeIssue: () => T, extend: () => lI, escapeRegex: () => R, esc: () => Qn, defineLazy: () => j, createTransparentProxy: () => $I, cloneDef: () => nI, clone: () => q, cleanRegex: () => vn, cleanEnum: () => UI, captureStackTrace: () => mn, cached: () => Pr, base64urlToUint8Array: () => kI, base64ToUint8Array: () => Je, assignProp: () => or, assertNotEqual: () => yc, assertNever: () => pc, assertIs: () => ac, assertEqual: () => hc, assert: () => sc, allowsEval: () => jv, aborted: () => ur, NUMBER_FORMAT_RANGES: () => Gv, Class: () => Ee, BIGINT_FORMAT_RANGES: () => Wv });
  function hc(r) {
    return r;
  }
  function yc(r) {
    return r;
  }
  function ac(r) {
  }
  function pc(r) {
    throw Error("Unexpected value in exhaustive check");
  }
  function sc(r) {
  }
  function nn(r) {
    let i = Object.values(r).filter((t) => typeof t === "number");
    return Object.entries(r).filter(([t, n]) => i.indexOf(+t) === -1).map(([t, n]) => n);
  }
  function b(r, i = "|") {
    return r.map((o) => U(o)).join(i);
  }
  function zr(r, i) {
    if (typeof i === "bigint")
      return i.toString();
    return i;
  }
  function Pr(r) {
    return { get value() {
      {
        let o = r();
        return Object.defineProperty(this, "value", { value: o }), o;
      }
      throw Error("cached value already set");
    } };
  }
  function vr(r) {
    return r === null || r === void 0;
  }
  function vn(r) {
    let i = r.startsWith("^") ? 1 : 0, o = r.endsWith("$") ? r.length - 1 : r.length;
    return r.slice(i, o);
  }
  function zv(r, i) {
    let o = (r.toString().split(".")[1] || "").length, t = i.toString(), n = (t.split(".")[1] || "").length;
    if (n === 0 && /\d?e-\d?/.test(t)) {
      let l = t.match(/\d?e-(\d?)/);
      if (l?.[1])
        n = Number.parseInt(l[1]);
    }
    let v = o > n ? o : n, u = Number.parseInt(r.toFixed(v).replace(".", "")), $ = Number.parseInt(i.toFixed(v).replace(".", ""));
    return u % $ / 10 ** v;
  }
  var je = Symbol("evaluating");
  function j(r, i, o) {
    let t = void 0;
    Object.defineProperty(r, i, { get() {
      if (t === je)
        return;
      if (t === void 0)
        t = je, t = o();
      return t;
    }, set(n) {
      Object.defineProperty(r, i, { value: n });
    }, configurable: true });
  }
  function rI(r) {
    return Object.create(Object.getPrototypeOf(r), Object.getOwnPropertyDescriptors(r));
  }
  function or(r, i, o) {
    Object.defineProperty(r, i, { value: o, writable: true, enumerable: true, configurable: true });
  }
  function rr(...r) {
    let i = {};
    for (let o of r) {
      let t = Object.getOwnPropertyDescriptors(o);
      Object.assign(i, t);
    }
    return Object.defineProperties({}, i);
  }
  function nI(r) {
    return rr(r._zod.def);
  }
  function iI(r, i) {
    if (!i)
      return r;
    return i.reduce((o, t) => o?.[t], r);
  }
  function vI(r) {
    let i = Object.keys(r), o = i.map((t) => r[t]);
    return Promise.all(o).then((t) => {
      let n = {};
      for (let v = 0; v < i.length; v++)
        n[i[v]] = t[v];
      return n;
    });
  }
  function oI(r = 10) {
    let o = "";
    for (let t = 0; t < r; t++)
      o += "abcdefghijklmnopqrstuvwxyz"[Math.floor(Math.random() * 26)];
    return o;
  }
  function Qn(r) {
    return JSON.stringify(r);
  }
  function Pv(r) {
    return r.toLowerCase().trim().replace(/[^\w\s-]/g, "").replace(/[\s_-]+/g, "-").replace(/^-+|-+$/g, "");
  }
  var mn = "captureStackTrace" in Error ? Error.captureStackTrace : (...r) => {
  };
  function br(r) {
    return typeof r === "object" && r !== null && !Array.isArray(r);
  }
  var jv = Pr(() => {
    if (typeof navigator < "u" && navigator?.userAgent?.includes("Cloudflare"))
      return false;
    try {
      return new Function(""), true;
    } catch (r) {
      return false;
    }
  });
  function tr(r) {
    if (br(r) === false)
      return false;
    let i = r.constructor;
    if (i === void 0)
      return true;
    if (typeof i !== "function")
      return true;
    let o = i.prototype;
    if (br(o) === false)
      return false;
    if (Object.prototype.hasOwnProperty.call(o, "isPrototypeOf") === false)
      return false;
    return true;
  }
  function Jv(r) {
    if (tr(r))
      return { ...r };
    if (Array.isArray(r))
      return [...r];
    return r;
  }
  function tI(r) {
    let i = 0;
    for (let o in r)
      if (Object.prototype.hasOwnProperty.call(r, o))
        i++;
    return i;
  }
  var uI = (r) => {
    let i = typeof r;
    switch (i) {
      case "undefined":
        return "undefined";
      case "string":
        return "string";
      case "number":
        return Number.isNaN(r) ? "nan" : "number";
      case "boolean":
        return "boolean";
      case "function":
        return "function";
      case "bigint":
        return "bigint";
      case "symbol":
        return "symbol";
      case "object":
        if (Array.isArray(r))
          return "array";
        if (r === null)
          return "null";
        if (r.then && typeof r.then === "function" && r.catch && typeof r.catch === "function")
          return "promise";
        if (typeof Map < "u" && r instanceof Map)
          return "map";
        if (typeof Set < "u" && r instanceof Set)
          return "set";
        if (typeof Date < "u" && r instanceof Date)
          return "date";
        if (typeof File < "u" && r instanceof File)
          return "file";
        return "object";
      default:
        throw Error(`Unknown data type: ${i}`);
    }
  };
  var on = /* @__PURE__ */ new Set(["string", "number", "symbol"]);
  var Lv = /* @__PURE__ */ new Set(["string", "number", "bigint", "boolean", "symbol", "undefined"]);
  function R(r) {
    return r.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  }
  function q(r, i, o) {
    let t = new r._zod.constr(i ?? r._zod.def);
    if (!i || o?.parent)
      t._zod.parent = r;
    return t;
  }
  function w(r) {
    let i = r;
    if (!i)
      return {};
    if (typeof i === "string")
      return { error: () => i };
    if (i?.message !== void 0) {
      if (i?.error !== void 0)
        throw Error("Cannot specify both `message` and `error` params");
      i.error = i.message;
    }
    if (delete i.message, typeof i.error === "string")
      return { ...i, error: () => i.error };
    return i;
  }
  function $I(r) {
    let i;
    return new Proxy({}, { get(o, t, n) {
      return i ?? (i = r()), Reflect.get(i, t, n);
    }, set(o, t, n, v) {
      return i ?? (i = r()), Reflect.set(i, t, n, v);
    }, has(o, t) {
      return i ?? (i = r()), Reflect.has(i, t);
    }, deleteProperty(o, t) {
      return i ?? (i = r()), Reflect.deleteProperty(i, t);
    }, ownKeys(o) {
      return i ?? (i = r()), Reflect.ownKeys(i);
    }, getOwnPropertyDescriptor(o, t) {
      return i ?? (i = r()), Reflect.getOwnPropertyDescriptor(i, t);
    }, defineProperty(o, t, n) {
      return i ?? (i = r()), Reflect.defineProperty(i, t, n);
    } });
  }
  function U(r) {
    if (typeof r === "bigint")
      return r.toString() + "n";
    if (typeof r === "string")
      return `"${r}"`;
    return `${r}`;
  }
  function Ev(r) {
    return Object.keys(r).filter((i) => {
      return r[i]._zod.optin === "optional" && r[i]._zod.optout === "optional";
    });
  }
  var Gv = { safeint: [Number.MIN_SAFE_INTEGER, Number.MAX_SAFE_INTEGER], int32: [-2147483648, 2147483647], uint32: [0, 4294967295], float32: [-34028234663852886e22, 34028234663852886e22], float64: [-Number.MAX_VALUE, Number.MAX_VALUE] };
  var Wv = { int64: [BigInt("-9223372036854775808"), BigInt("9223372036854775807")], uint64: [BigInt(0), BigInt("18446744073709551615")] };
  function gI(r, i) {
    let o = r._zod.def, t = o.checks;
    if (t && t.length > 0)
      throw Error(".pick() cannot be used on object schemas containing refinements");
    let v = rr(r._zod.def, { get shape() {
      let u = {};
      for (let $ in i) {
        if (!($ in o.shape))
          throw Error(`Unrecognized key: "${$}"`);
        if (!i[$])
          continue;
        u[$] = o.shape[$];
      }
      return or(this, "shape", u), u;
    }, checks: [] });
    return q(r, v);
  }
  function eI(r, i) {
    let o = r._zod.def, t = o.checks;
    if (t && t.length > 0)
      throw Error(".omit() cannot be used on object schemas containing refinements");
    let v = rr(r._zod.def, { get shape() {
      let u = { ...r._zod.def.shape };
      for (let $ in i) {
        if (!($ in o.shape))
          throw Error(`Unrecognized key: "${$}"`);
        if (!i[$])
          continue;
        delete u[$];
      }
      return or(this, "shape", u), u;
    }, checks: [] });
    return q(r, v);
  }
  function lI(r, i) {
    if (!tr(i))
      throw Error("Invalid input to extend: expected a plain object");
    let o = r._zod.def.checks;
    if (o && o.length > 0) {
      let v = r._zod.def.shape;
      for (let u in i)
        if (Object.getOwnPropertyDescriptor(v, u) !== void 0)
          throw Error("Cannot overwrite keys on object schemas containing refinements. Use `.safeExtend()` instead.");
    }
    let n = rr(r._zod.def, { get shape() {
      let v = { ...r._zod.def.shape, ...i };
      return or(this, "shape", v), v;
    } });
    return q(r, n);
  }
  function cI(r, i) {
    if (!tr(i))
      throw Error("Invalid input to safeExtend: expected a plain object");
    let o = rr(r._zod.def, { get shape() {
      let t = { ...r._zod.def.shape, ...i };
      return or(this, "shape", t), t;
    } });
    return q(r, o);
  }
  function II(r, i) {
    let o = rr(r._zod.def, { get shape() {
      let t = { ...r._zod.def.shape, ...i._zod.def.shape };
      return or(this, "shape", t), t;
    }, get catchall() {
      return i._zod.def.catchall;
    }, checks: [] });
    return q(r, o);
  }
  function bI(r, i, o) {
    let n = i._zod.def.checks;
    if (n && n.length > 0)
      throw Error(".partial() cannot be used on object schemas containing refinements");
    let u = rr(i._zod.def, { get shape() {
      let $ = i._zod.def.shape, l = { ...$ };
      if (o)
        for (let e in o) {
          if (!(e in $))
            throw Error(`Unrecognized key: "${e}"`);
          if (!o[e])
            continue;
          l[e] = r ? new r({ type: "optional", innerType: $[e] }) : $[e];
        }
      else
        for (let e in $)
          l[e] = r ? new r({ type: "optional", innerType: $[e] }) : $[e];
      return or(this, "shape", l), l;
    }, checks: [] });
    return q(i, u);
  }
  function _I(r, i, o) {
    let t = rr(i._zod.def, { get shape() {
      let n = i._zod.def.shape, v = { ...n };
      if (o)
        for (let u in o) {
          if (!(u in v))
            throw Error(`Unrecognized key: "${u}"`);
          if (!o[u])
            continue;
          v[u] = new r({ type: "nonoptional", innerType: n[u] });
        }
      else
        for (let u in n)
          v[u] = new r({ type: "nonoptional", innerType: n[u] });
      return or(this, "shape", v), v;
    } });
    return q(i, t);
  }
  function ur(r, i = 0) {
    if (r.aborted === true)
      return true;
    for (let o = i; o < r.issues.length; o++)
      if (r.issues[o]?.continue !== true)
        return true;
    return false;
  }
  function H(r, i) {
    return i.map((o) => {
      var t;
      return (t = o).path ?? (t.path = []), o.path.unshift(r), o;
    });
  }
  function rn(r) {
    return typeof r === "string" ? r : r?.message;
  }
  function T(r, i, o) {
    let t = { ...r, path: r.path ?? [] };
    if (!r.message) {
      let n = rn(r.inst?._zod.def?.error?.(r)) ?? rn(i?.error?.(r)) ?? rn(o.customError?.(r)) ?? rn(o.localeError?.(r)) ?? "Invalid input";
      t.message = n;
    }
    if (delete t.inst, delete t.continue, !i?.reportInput)
      delete t.input;
    return t;
  }
  function tn(r) {
    if (r instanceof Set)
      return "set";
    if (r instanceof Map)
      return "map";
    if (r instanceof File)
      return "file";
    return "unknown";
  }
  function un(r) {
    if (Array.isArray(r))
      return "array";
    if (typeof r === "string")
      return "string";
    return "unknown";
  }
  function k(r) {
    let i = typeof r;
    switch (i) {
      case "number":
        return Number.isNaN(r) ? "nan" : "number";
      case "object": {
        if (r === null)
          return "null";
        if (Array.isArray(r))
          return "array";
        let o = r;
        if (o && Object.getPrototypeOf(o) !== Object.prototype && "constructor" in o && o.constructor)
          return o.constructor.name;
      }
    }
    return i;
  }
  function jr(...r) {
    let [i, o, t] = r;
    if (typeof i === "string")
      return { message: i, code: "custom", input: o, inst: t };
    return { ...i };
  }
  function UI(r) {
    return Object.entries(r).filter(([i, o]) => {
      return Number.isNaN(Number.parseInt(i, 10));
    }).map((i) => i[1]);
  }
  function Je(r) {
    let i = atob(r), o = new Uint8Array(i.length);
    for (let t = 0; t < i.length; t++)
      o[t] = i.charCodeAt(t);
    return o;
  }
  function Le(r) {
    let i = "";
    for (let o = 0; o < r.length; o++)
      i += String.fromCharCode(r[o]);
    return btoa(i);
  }
  function kI(r) {
    let i = r.replace(/-/g, "+").replace(/_/g, "/"), o = "=".repeat((4 - i.length % 4) % 4);
    return Je(i + o);
  }
  function DI(r) {
    return Le(r).replace(/\+/g, "-").replace(/\//g, "_").replace(/=/g, "");
  }
  function wI(r) {
    let i = r.replace(/^0x/, "");
    if (i.length % 2 !== 0)
      throw Error("Invalid hex string length");
    let o = new Uint8Array(i.length / 2);
    for (let t = 0; t < i.length; t += 2)
      o[t / 2] = Number.parseInt(i.slice(t, t + 2), 16);
    return o;
  }
  function NI(r) {
    return Array.from(r).map((i) => i.toString(16).padStart(2, "0")).join("");
  }
  var Ee = class {
    constructor(...r) {
    }
  };
  var Ge = (r, i) => {
    r.name = "$ZodError", Object.defineProperty(r, "_zod", { value: r._zod, enumerable: false }), Object.defineProperty(r, "issues", { value: i, enumerable: false }), r.message = JSON.stringify(i, zr, 2), Object.defineProperty(r, "toString", { value: () => r.message, enumerable: false });
  };
  var $n = c("$ZodError", Ge);
  var F = c("$ZodError", Ge, { Parent: Error });
  function gn(r, i = (o) => o.message) {
    let o = {}, t = [];
    for (let n of r.issues)
      if (n.path.length > 0)
        o[n.path[0]] = o[n.path[0]] || [], o[n.path[0]].push(i(n));
      else
        t.push(i(n));
    return { formErrors: t, fieldErrors: o };
  }
  function en(r, i = (o) => o.message) {
    let o = { _errors: [] }, t = (n) => {
      for (let v of n.issues)
        if (v.code === "invalid_union" && v.errors.length)
          v.errors.map((u) => t({ issues: u }));
        else if (v.code === "invalid_key")
          t({ issues: v.issues });
        else if (v.code === "invalid_element")
          t({ issues: v.issues });
        else if (v.path.length === 0)
          o._errors.push(i(v));
        else {
          let u = o, $ = 0;
          while ($ < v.path.length) {
            let l = v.path[$];
            if ($ !== v.path.length - 1)
              u[l] = u[l] || { _errors: [] };
            else
              u[l] = u[l] || { _errors: [] }, u[l]._errors.push(i(v));
            u = u[l], $++;
          }
        }
    };
    return t(r), o;
  }
  function Xv(r, i = (o) => o.message) {
    let o = { errors: [] }, t = (n, v = []) => {
      var u, $;
      for (let l of n.issues)
        if (l.code === "invalid_union" && l.errors.length)
          l.errors.map((e) => t({ issues: e }, l.path));
        else if (l.code === "invalid_key")
          t({ issues: l.issues }, l.path);
        else if (l.code === "invalid_element")
          t({ issues: l.issues }, l.path);
        else {
          let e = [...v, ...l.path];
          if (e.length === 0) {
            o.errors.push(i(l));
            continue;
          }
          let I = o, _ = 0;
          while (_ < e.length) {
            let N = e[_], O = _ === e.length - 1;
            if (typeof N === "string")
              I.properties ?? (I.properties = {}), (u = I.properties)[N] ?? (u[N] = { errors: [] }), I = I.properties[N];
            else
              I.items ?? (I.items = []), ($ = I.items)[N] ?? ($[N] = { errors: [] }), I = I.items[N];
            if (O)
              I.errors.push(i(l));
            _++;
          }
        }
    };
    return t(r), o;
  }
  function We(r) {
    let i = [], o = r.map((t) => typeof t === "object" ? t.key : t);
    for (let t of o)
      if (typeof t === "number")
        i.push(`[${t}]`);
      else if (typeof t === "symbol")
        i.push(`[${JSON.stringify(String(t))}]`);
      else if (/[^\w$]/.test(t))
        i.push(`[${JSON.stringify(t)}]`);
      else {
        if (i.length)
          i.push(".");
        i.push(t);
      }
    return i.join("");
  }
  function Vv(r) {
    let i = [], o = [...r.issues].sort((t, n) => (t.path ?? []).length - (n.path ?? []).length);
    for (let t of o)
      if (i.push(`\u2716 ${t.message}`), t.path?.length)
        i.push(`  \u2192 at ${We(t.path)}`);
    return i.join(`
`);
  }
  var Jr = (r) => (i, o, t, n) => {
    let v = t ? Object.assign(t, { async: false }) : { async: false }, u = i._zod.run({ value: o, issues: [] }, v);
    if (u instanceof Promise)
      throw new f();
    if (u.issues.length) {
      let $ = new (n?.Err ?? r)(u.issues.map((l) => T(l, v, V())));
      throw mn($, n?.callee), $;
    }
    return u.value;
  };
  var Tn = Jr(F);
  var Lr = (r) => async (i, o, t, n) => {
    let v = t ? Object.assign(t, { async: true }) : { async: true }, u = i._zod.run({ value: o, issues: [] }, v);
    if (u instanceof Promise)
      u = await u;
    if (u.issues.length) {
      let $ = new (n?.Err ?? r)(u.issues.map((l) => T(l, v, V())));
      throw mn($, n?.callee), $;
    }
    return u.value;
  };
  var Fn = Lr(F);
  var Er = (r) => (i, o, t) => {
    let n = t ? { ...t, async: false } : { async: false }, v = i._zod.run({ value: o, issues: [] }, n);
    if (v instanceof Promise)
      throw new f();
    return v.issues.length ? { success: false, error: new (r ?? $n)(v.issues.map((u) => T(u, n, V()))) } : { success: true, data: v.value };
  };
  var Av = Er(F);
  var Gr = (r) => async (i, o, t) => {
    let n = t ? Object.assign(t, { async: true }) : { async: true }, v = i._zod.run({ value: o, issues: [] }, n);
    if (v instanceof Promise)
      v = await v;
    return v.issues.length ? { success: false, error: new r(v.issues.map((u) => T(u, n, V()))) } : { success: true, data: v.value };
  };
  var Kv = Gr(F);
  var Bn = (r) => (i, o, t) => {
    let n = t ? Object.assign(t, { direction: "backward" }) : { direction: "backward" };
    return Jr(r)(i, o, n);
  };
  var SI = Bn(F);
  var Hn = (r) => (i, o, t) => {
    return Jr(r)(i, o, t);
  };
  var zI = Hn(F);
  var Mn = (r) => async (i, o, t) => {
    let n = t ? Object.assign(t, { direction: "backward" }) : { direction: "backward" };
    return Lr(r)(i, o, n);
  };
  var PI = Mn(F);
  var Rn = (r) => async (i, o, t) => {
    return Lr(r)(i, o, t);
  };
  var jI = Rn(F);
  var xn = (r) => (i, o, t) => {
    let n = t ? Object.assign(t, { direction: "backward" }) : { direction: "backward" };
    return Er(r)(i, o, n);
  };
  var JI = xn(F);
  var Zn = (r) => (i, o, t) => {
    return Er(r)(i, o, t);
  };
  var LI = Zn(F);
  var dn = (r) => async (i, o, t) => {
    let n = t ? Object.assign(t, { direction: "backward" }) : { direction: "backward" };
    return Gr(r)(i, o, n);
  };
  var EI = dn(F);
  var Cn = (r) => async (i, o, t) => {
    return Gr(r)(i, o, t);
  };
  var GI = Cn(F);
  var x = {};
  s(x, { xid: () => mv, uuid7: () => AI, uuid6: () => VI, uuid4: () => XI, uuid: () => _r, uppercase: () => $o, unicodeEmail: () => Xe, undefined: () => to, ulid: () => Qv, time: () => pv, string: () => ro, sha512_hex: () => sI, sha512_base64url: () => n4, sha512_base64: () => r4, sha384_hex: () => yI, sha384_base64url: () => pI, sha384_base64: () => aI, sha256_hex: () => CI, sha256_base64url: () => hI, sha256_base64: () => fI, sha1_hex: () => xI, sha1_base64url: () => dI, sha1_base64: () => ZI, rfc5322Email: () => qI, number: () => ln, null: () => oo, nanoid: () => Fv, md5_hex: () => HI, md5_base64url: () => RI, md5_base64: () => MI, mac: () => dv, lowercase: () => uo, ksuid: () => Tv, ipv6: () => Zv, ipv4: () => xv, integer: () => io, idnEmail: () => YI, html5Email: () => KI, hostname: () => TI, hex: () => BI, guid: () => Hv, extendedDuration: () => WI, emoji: () => Rv, email: () => Mv, e164: () => yv, duration: () => Bv, domain: () => FI, datetime: () => sv, date: () => av, cuid2: () => Yv, cuid: () => qv, cidrv6: () => fv, cidrv4: () => Cv, browserEmail: () => QI, boolean: () => vo, bigint: () => no, base64url: () => fn, base64: () => hv });
  var qv = /^[cC][^\s-]{8,}$/;
  var Yv = /^[0-9a-z]+$/;
  var Qv = /^[0-9A-HJKMNP-TV-Za-hjkmnp-tv-z]{26}$/;
  var mv = /^[0-9a-vA-V]{20}$/;
  var Tv = /^[A-Za-z0-9]{27}$/;
  var Fv = /^[a-zA-Z0-9_-]{21}$/;
  var Bv = /^P(?:(\d+W)|(?!.*W)(?=\d|T\d)(\d+Y)?(\d+M)?(\d+D)?(T(?=\d)(\d+H)?(\d+M)?(\d+([.,]\d+)?S)?)?)$/;
  var WI = /^[-+]?P(?!$)(?:(?:[-+]?\d+Y)|(?:[-+]?\d+[.,]\d+Y$))?(?:(?:[-+]?\d+M)|(?:[-+]?\d+[.,]\d+M$))?(?:(?:[-+]?\d+W)|(?:[-+]?\d+[.,]\d+W$))?(?:(?:[-+]?\d+D)|(?:[-+]?\d+[.,]\d+D$))?(?:T(?=[\d+-])(?:(?:[-+]?\d+H)|(?:[-+]?\d+[.,]\d+H$))?(?:(?:[-+]?\d+M)|(?:[-+]?\d+[.,]\d+M$))?(?:[-+]?\d+(?:[.,]\d+)?S)?)??$/;
  var Hv = /^([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})$/;
  var _r = (r) => {
    if (!r)
      return /^([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-8][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}|00000000-0000-0000-0000-000000000000|ffffffff-ffff-ffff-ffff-ffffffffffff)$/;
    return new RegExp(`^([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-${r}[0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12})$`);
  };
  var XI = _r(4);
  var VI = _r(6);
  var AI = _r(7);
  var Mv = /^(?!\.)(?!.*\.\.)([A-Za-z0-9_'+\-\.]*)[A-Za-z0-9_+-]@([A-Za-z0-9][A-Za-z0-9\-]*\.)+[A-Za-z]{2,}$/;
  var KI = /^[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$/;
  var qI = /^(([^<>()\[\]\\.,;:\s@"]+(\.[^<>()\[\]\\.,;:\s@"]+)*)|(".+"))@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}])|(([a-zA-Z\-0-9]+\.)+[a-zA-Z]{2,}))$/;
  var Xe = /^[^\s@"]{1,64}@[^\s@]{1,255}$/u;
  var YI = Xe;
  var QI = /^[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$/;
  var mI = "^(\\p{Extended_Pictographic}|\\p{Emoji_Component})+$";
  function Rv() {
    return new RegExp(mI, "u");
  }
  var xv = /^(?:(?:25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9])\.){3}(?:25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9])$/;
  var Zv = /^(([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:))$/;
  var dv = (r) => {
    let i = R(r ?? ":");
    return new RegExp(`^(?:[0-9A-F]{2}${i}){5}[0-9A-F]{2}$|^(?:[0-9a-f]{2}${i}){5}[0-9a-f]{2}$`);
  };
  var Cv = /^((25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9])\.){3}(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9][0-9]|[0-9])\/([0-9]|[1-2][0-9]|3[0-2])$/;
  var fv = /^(([0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}|::|([0-9a-fA-F]{1,4})?::([0-9a-fA-F]{1,4}:?){0,6})\/(12[0-8]|1[01][0-9]|[1-9]?[0-9])$/;
  var hv = /^$|^(?:[0-9a-zA-Z+/]{4})*(?:(?:[0-9a-zA-Z+/]{2}==)|(?:[0-9a-zA-Z+/]{3}=))?$/;
  var fn = /^[A-Za-z0-9_-]*$/;
  var TI = /^(?=.{1,253}\.?$)[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[-0-9a-zA-Z]{0,61}[0-9a-zA-Z])?)*\.?$/;
  var FI = /^([a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,}$/;
  var yv = /^\+[1-9]\d{6,14}$/;
  var Ve = "(?:(?:\\d\\d[2468][048]|\\d\\d[13579][26]|\\d\\d0[48]|[02468][048]00|[13579][26]00)-02-29|\\d{4}-(?:(?:0[13578]|1[02])-(?:0[1-9]|[12]\\d|3[01])|(?:0[469]|11)-(?:0[1-9]|[12]\\d|30)|(?:02)-(?:0[1-9]|1\\d|2[0-8])))";
  var av = new RegExp(`^${Ve}$`);
  function Ae(r) {
    return typeof r.precision === "number" ? r.precision === -1 ? "(?:[01]\\d|2[0-3]):[0-5]\\d" : r.precision === 0 ? "(?:[01]\\d|2[0-3]):[0-5]\\d:[0-5]\\d" : `(?:[01]\\d|2[0-3]):[0-5]\\d:[0-5]\\d\\.\\d{${r.precision}}` : "(?:[01]\\d|2[0-3]):[0-5]\\d(?::[0-5]\\d(?:\\.\\d+)?)?";
  }
  function pv(r) {
    return new RegExp(`^${Ae(r)}$`);
  }
  function sv(r) {
    let i = Ae({ precision: r.precision }), o = ["Z"];
    if (r.local)
      o.push("");
    if (r.offset)
      o.push("([+-](?:[01]\\d|2[0-3]):[0-5]\\d)");
    let t = `${i}(?:${o.join("|")})`;
    return new RegExp(`^${Ve}T(?:${t})$`);
  }
  var ro = (r) => {
    let i = r ? `[\\s\\S]{${r?.minimum ?? 0},${r?.maximum ?? ""}}` : "[\\s\\S]*";
    return new RegExp(`^${i}$`);
  };
  var no = /^-?\d+n?$/;
  var io = /^-?\d+$/;
  var ln = /^-?\d+(?:\.\d+)?$/;
  var vo = /^(?:true|false)$/i;
  var oo = /^null$/i;
  var to = /^undefined$/i;
  var uo = /^[^A-Z]*$/;
  var $o = /^[^a-z]*$/;
  var BI = /^[0-9a-fA-F]*$/;
  function cn(r, i) {
    return new RegExp(`^[A-Za-z0-9+/]{${r}}${i}$`);
  }
  function In(r) {
    return new RegExp(`^[A-Za-z0-9_-]{${r}}$`);
  }
  var HI = /^[0-9a-fA-F]{32}$/;
  var MI = cn(22, "==");
  var RI = In(22);
  var xI = /^[0-9a-fA-F]{40}$/;
  var ZI = cn(27, "=");
  var dI = In(27);
  var CI = /^[0-9a-fA-F]{64}$/;
  var fI = cn(43, "=");
  var hI = In(43);
  var yI = /^[0-9a-fA-F]{96}$/;
  var aI = cn(64, "");
  var pI = In(64);
  var sI = /^[0-9a-fA-F]{128}$/;
  var r4 = cn(86, "==");
  var n4 = In(86);
  var W = c("$ZodCheck", (r, i) => {
    var o;
    r._zod ?? (r._zod = {}), r._zod.def = i, (o = r._zod).onattach ?? (o.onattach = []);
  });
  var qe = { number: "number", bigint: "bigint", object: "date" };
  var hn = c("$ZodCheckLessThan", (r, i) => {
    W.init(r, i);
    let o = qe[typeof i.value];
    r._zod.onattach.push((t) => {
      let n = t._zod.bag, v = (i.inclusive ? n.maximum : n.exclusiveMaximum) ?? Number.POSITIVE_INFINITY;
      if (i.value < v)
        if (i.inclusive)
          n.maximum = i.value;
        else
          n.exclusiveMaximum = i.value;
    }), r._zod.check = (t) => {
      if (i.inclusive ? t.value <= i.value : t.value < i.value)
        return;
      t.issues.push({ origin: o, code: "too_big", maximum: typeof i.value === "object" ? i.value.getTime() : i.value, input: t.value, inclusive: i.inclusive, inst: r, continue: !i.abort });
    };
  });
  var yn = c("$ZodCheckGreaterThan", (r, i) => {
    W.init(r, i);
    let o = qe[typeof i.value];
    r._zod.onattach.push((t) => {
      let n = t._zod.bag, v = (i.inclusive ? n.minimum : n.exclusiveMinimum) ?? Number.NEGATIVE_INFINITY;
      if (i.value > v)
        if (i.inclusive)
          n.minimum = i.value;
        else
          n.exclusiveMinimum = i.value;
    }), r._zod.check = (t) => {
      if (i.inclusive ? t.value >= i.value : t.value > i.value)
        return;
      t.issues.push({ origin: o, code: "too_small", minimum: typeof i.value === "object" ? i.value.getTime() : i.value, input: t.value, inclusive: i.inclusive, inst: r, continue: !i.abort });
    };
  });
  var go = c("$ZodCheckMultipleOf", (r, i) => {
    W.init(r, i), r._zod.onattach.push((o) => {
      var t;
      (t = o._zod.bag).multipleOf ?? (t.multipleOf = i.value);
    }), r._zod.check = (o) => {
      if (typeof o.value !== typeof i.value)
        throw Error("Cannot mix number and bigint in multiple_of check.");
      if (typeof o.value === "bigint" ? o.value % i.value === BigInt(0) : zv(o.value, i.value) === 0)
        return;
      o.issues.push({ origin: typeof o.value, code: "not_multiple_of", divisor: i.value, input: o.value, inst: r, continue: !i.abort });
    };
  });
  var eo = c("$ZodCheckNumberFormat", (r, i) => {
    W.init(r, i), i.format = i.format || "float64";
    let o = i.format?.includes("int"), t = o ? "int" : "number", [n, v] = Gv[i.format];
    r._zod.onattach.push((u) => {
      let $ = u._zod.bag;
      if ($.format = i.format, $.minimum = n, $.maximum = v, o)
        $.pattern = io;
    }), r._zod.check = (u) => {
      let $ = u.value;
      if (o) {
        if (!Number.isInteger($)) {
          u.issues.push({ expected: t, format: i.format, code: "invalid_type", continue: false, input: $, inst: r });
          return;
        }
        if (!Number.isSafeInteger($)) {
          if ($ > 0)
            u.issues.push({ input: $, code: "too_big", maximum: Number.MAX_SAFE_INTEGER, note: "Integers must be within the safe integer range.", inst: r, origin: t, inclusive: true, continue: !i.abort });
          else
            u.issues.push({ input: $, code: "too_small", minimum: Number.MIN_SAFE_INTEGER, note: "Integers must be within the safe integer range.", inst: r, origin: t, inclusive: true, continue: !i.abort });
          return;
        }
      }
      if ($ < n)
        u.issues.push({ origin: "number", input: $, code: "too_small", minimum: n, inclusive: true, inst: r, continue: !i.abort });
      if ($ > v)
        u.issues.push({ origin: "number", input: $, code: "too_big", maximum: v, inclusive: true, inst: r, continue: !i.abort });
    };
  });
  var lo = c("$ZodCheckBigIntFormat", (r, i) => {
    W.init(r, i);
    let [o, t] = Wv[i.format];
    r._zod.onattach.push((n) => {
      let v = n._zod.bag;
      v.format = i.format, v.minimum = o, v.maximum = t;
    }), r._zod.check = (n) => {
      let v = n.value;
      if (v < o)
        n.issues.push({ origin: "bigint", input: v, code: "too_small", minimum: o, inclusive: true, inst: r, continue: !i.abort });
      if (v > t)
        n.issues.push({ origin: "bigint", input: v, code: "too_big", maximum: t, inclusive: true, inst: r, continue: !i.abort });
    };
  });
  var co = c("$ZodCheckMaxSize", (r, i) => {
    var o;
    W.init(r, i), (o = r._zod.def).when ?? (o.when = (t) => {
      let n = t.value;
      return !vr(n) && n.size !== void 0;
    }), r._zod.onattach.push((t) => {
      let n = t._zod.bag.maximum ?? Number.POSITIVE_INFINITY;
      if (i.maximum < n)
        t._zod.bag.maximum = i.maximum;
    }), r._zod.check = (t) => {
      let n = t.value;
      if (n.size <= i.maximum)
        return;
      t.issues.push({ origin: tn(n), code: "too_big", maximum: i.maximum, inclusive: true, input: n, inst: r, continue: !i.abort });
    };
  });
  var Io = c("$ZodCheckMinSize", (r, i) => {
    var o;
    W.init(r, i), (o = r._zod.def).when ?? (o.when = (t) => {
      let n = t.value;
      return !vr(n) && n.size !== void 0;
    }), r._zod.onattach.push((t) => {
      let n = t._zod.bag.minimum ?? Number.NEGATIVE_INFINITY;
      if (i.minimum > n)
        t._zod.bag.minimum = i.minimum;
    }), r._zod.check = (t) => {
      let n = t.value;
      if (n.size >= i.minimum)
        return;
      t.issues.push({ origin: tn(n), code: "too_small", minimum: i.minimum, inclusive: true, input: n, inst: r, continue: !i.abort });
    };
  });
  var bo = c("$ZodCheckSizeEquals", (r, i) => {
    var o;
    W.init(r, i), (o = r._zod.def).when ?? (o.when = (t) => {
      let n = t.value;
      return !vr(n) && n.size !== void 0;
    }), r._zod.onattach.push((t) => {
      let n = t._zod.bag;
      n.minimum = i.size, n.maximum = i.size, n.size = i.size;
    }), r._zod.check = (t) => {
      let n = t.value, v = n.size;
      if (v === i.size)
        return;
      let u = v > i.size;
      t.issues.push({ origin: tn(n), ...u ? { code: "too_big", maximum: i.size } : { code: "too_small", minimum: i.size }, inclusive: true, exact: true, input: t.value, inst: r, continue: !i.abort });
    };
  });
  var _o = c("$ZodCheckMaxLength", (r, i) => {
    var o;
    W.init(r, i), (o = r._zod.def).when ?? (o.when = (t) => {
      let n = t.value;
      return !vr(n) && n.length !== void 0;
    }), r._zod.onattach.push((t) => {
      let n = t._zod.bag.maximum ?? Number.POSITIVE_INFINITY;
      if (i.maximum < n)
        t._zod.bag.maximum = i.maximum;
    }), r._zod.check = (t) => {
      let n = t.value;
      if (n.length <= i.maximum)
        return;
      let u = un(n);
      t.issues.push({ origin: u, code: "too_big", maximum: i.maximum, inclusive: true, input: n, inst: r, continue: !i.abort });
    };
  });
  var Uo = c("$ZodCheckMinLength", (r, i) => {
    var o;
    W.init(r, i), (o = r._zod.def).when ?? (o.when = (t) => {
      let n = t.value;
      return !vr(n) && n.length !== void 0;
    }), r._zod.onattach.push((t) => {
      let n = t._zod.bag.minimum ?? Number.NEGATIVE_INFINITY;
      if (i.minimum > n)
        t._zod.bag.minimum = i.minimum;
    }), r._zod.check = (t) => {
      let n = t.value;
      if (n.length >= i.minimum)
        return;
      let u = un(n);
      t.issues.push({ origin: u, code: "too_small", minimum: i.minimum, inclusive: true, input: n, inst: r, continue: !i.abort });
    };
  });
  var ko = c("$ZodCheckLengthEquals", (r, i) => {
    var o;
    W.init(r, i), (o = r._zod.def).when ?? (o.when = (t) => {
      let n = t.value;
      return !vr(n) && n.length !== void 0;
    }), r._zod.onattach.push((t) => {
      let n = t._zod.bag;
      n.minimum = i.length, n.maximum = i.length, n.length = i.length;
    }), r._zod.check = (t) => {
      let n = t.value, v = n.length;
      if (v === i.length)
        return;
      let u = un(n), $ = v > i.length;
      t.issues.push({ origin: u, ...$ ? { code: "too_big", maximum: i.length } : { code: "too_small", minimum: i.length }, inclusive: true, exact: true, input: t.value, inst: r, continue: !i.abort });
    };
  });
  var Wr = c("$ZodCheckStringFormat", (r, i) => {
    var o, t;
    if (W.init(r, i), r._zod.onattach.push((n) => {
      let v = n._zod.bag;
      if (v.format = i.format, i.pattern)
        v.patterns ?? (v.patterns = /* @__PURE__ */ new Set()), v.patterns.add(i.pattern);
    }), i.pattern)
      (o = r._zod).check ?? (o.check = (n) => {
        if (i.pattern.lastIndex = 0, i.pattern.test(n.value))
          return;
        n.issues.push({ origin: "string", code: "invalid_format", format: i.format, input: n.value, ...i.pattern ? { pattern: i.pattern.toString() } : {}, inst: r, continue: !i.abort });
      });
    else
      (t = r._zod).check ?? (t.check = () => {
      });
  });
  var Do = c("$ZodCheckRegex", (r, i) => {
    Wr.init(r, i), r._zod.check = (o) => {
      if (i.pattern.lastIndex = 0, i.pattern.test(o.value))
        return;
      o.issues.push({ origin: "string", code: "invalid_format", format: "regex", input: o.value, pattern: i.pattern.toString(), inst: r, continue: !i.abort });
    };
  });
  var wo = c("$ZodCheckLowerCase", (r, i) => {
    i.pattern ?? (i.pattern = uo), Wr.init(r, i);
  });
  var No = c("$ZodCheckUpperCase", (r, i) => {
    i.pattern ?? (i.pattern = $o), Wr.init(r, i);
  });
  var Oo = c("$ZodCheckIncludes", (r, i) => {
    W.init(r, i);
    let o = R(i.includes), t = new RegExp(typeof i.position === "number" ? `^.{${i.position}}${o}` : o);
    i.pattern = t, r._zod.onattach.push((n) => {
      let v = n._zod.bag;
      v.patterns ?? (v.patterns = /* @__PURE__ */ new Set()), v.patterns.add(t);
    }), r._zod.check = (n) => {
      if (n.value.includes(i.includes, i.position))
        return;
      n.issues.push({ origin: "string", code: "invalid_format", format: "includes", includes: i.includes, input: n.value, inst: r, continue: !i.abort });
    };
  });
  var So = c("$ZodCheckStartsWith", (r, i) => {
    W.init(r, i);
    let o = new RegExp(`^${R(i.prefix)}.*`);
    i.pattern ?? (i.pattern = o), r._zod.onattach.push((t) => {
      let n = t._zod.bag;
      n.patterns ?? (n.patterns = /* @__PURE__ */ new Set()), n.patterns.add(o);
    }), r._zod.check = (t) => {
      if (t.value.startsWith(i.prefix))
        return;
      t.issues.push({ origin: "string", code: "invalid_format", format: "starts_with", prefix: i.prefix, input: t.value, inst: r, continue: !i.abort });
    };
  });
  var zo = c("$ZodCheckEndsWith", (r, i) => {
    W.init(r, i);
    let o = new RegExp(`.*${R(i.suffix)}$`);
    i.pattern ?? (i.pattern = o), r._zod.onattach.push((t) => {
      let n = t._zod.bag;
      n.patterns ?? (n.patterns = /* @__PURE__ */ new Set()), n.patterns.add(o);
    }), r._zod.check = (t) => {
      if (t.value.endsWith(i.suffix))
        return;
      t.issues.push({ origin: "string", code: "invalid_format", format: "ends_with", suffix: i.suffix, input: t.value, inst: r, continue: !i.abort });
    };
  });
  function Ke(r, i, o) {
    if (r.issues.length)
      i.issues.push(...H(o, r.issues));
  }
  var Po = c("$ZodCheckProperty", (r, i) => {
    W.init(r, i), r._zod.check = (o) => {
      let t = i.schema._zod.run({ value: o.value[i.property], issues: [] }, {});
      if (t instanceof Promise)
        return t.then((n) => Ke(n, o, i.property));
      Ke(t, o, i.property);
      return;
    };
  });
  var jo = c("$ZodCheckMimeType", (r, i) => {
    W.init(r, i);
    let o = new Set(i.mime);
    r._zod.onattach.push((t) => {
      t._zod.bag.mime = i.mime;
    }), r._zod.check = (t) => {
      if (o.has(t.value.type))
        return;
      t.issues.push({ code: "invalid_value", values: i.mime, input: t.value.type, inst: r, continue: !i.abort });
    };
  });
  var Jo = c("$ZodCheckOverwrite", (r, i) => {
    W.init(r, i), r._zod.check = (o) => {
      o.value = i.tx(o.value);
    };
  });
  var an = class {
    constructor(r = []) {
      if (this.content = [], this.indent = 0, this)
        this.args = r;
    }
    indented(r) {
      this.indent += 1, r(this), this.indent -= 1;
    }
    write(r) {
      if (typeof r === "function") {
        r(this, { execution: "sync" }), r(this, { execution: "async" });
        return;
      }
      let o = r.split(`
`).filter((v) => v), t = Math.min(...o.map((v) => v.length - v.trimStart().length)), n = o.map((v) => v.slice(t)).map((v) => " ".repeat(this.indent * 2) + v);
      for (let v of n)
        this.content.push(v);
    }
    compile() {
      let r = Function, i = this?.args, t = [...(this?.content ?? [""]).map((n) => `  ${n}`)];
      return new r(...i, t.join(`
`));
    }
  };
  var Lo = { major: 4, minor: 3, patch: 5 };
  var z = c("$ZodType", (r, i) => {
    var o;
    r ?? (r = {}), r._zod.def = i, r._zod.bag = r._zod.bag || {}, r._zod.version = Lo;
    let t = [...r._zod.def.checks ?? []];
    if (r._zod.traits.has("$ZodCheck"))
      t.unshift(r);
    for (let n of t)
      for (let v of n._zod.onattach)
        v(r);
    if (t.length === 0)
      (o = r._zod).deferred ?? (o.deferred = []), r._zod.deferred?.push(() => {
        r._zod.run = r._zod.parse;
      });
    else {
      let n = (u, $, l) => {
        let e = ur(u), I;
        for (let _ of $) {
          if (_._zod.def.when) {
            if (!_._zod.def.when(u))
              continue;
          } else if (e)
            continue;
          let N = u.issues.length, O = _._zod.check(u);
          if (O instanceof Promise && l?.async === false)
            throw new f();
          if (I || O instanceof Promise)
            I = (I ?? Promise.resolve()).then(async () => {
              if (await O, u.issues.length === N)
                return;
              if (!e)
                e = ur(u, N);
            });
          else {
            if (u.issues.length === N)
              continue;
            if (!e)
              e = ur(u, N);
          }
        }
        if (I)
          return I.then(() => {
            return u;
          });
        return u;
      }, v = (u, $, l) => {
        if (ur(u))
          return u.aborted = true, u;
        let e = n($, t, l);
        if (e instanceof Promise) {
          if (l.async === false)
            throw new f();
          return e.then((I) => r._zod.parse(I, l));
        }
        return r._zod.parse(e, l);
      };
      r._zod.run = (u, $) => {
        if ($.skipChecks)
          return r._zod.parse(u, $);
        if ($.direction === "backward") {
          let e = r._zod.parse({ value: u.value, issues: [] }, { ...$, skipChecks: true });
          if (e instanceof Promise)
            return e.then((I) => {
              return v(I, u, $);
            });
          return v(e, u, $);
        }
        let l = r._zod.parse(u, $);
        if (l instanceof Promise) {
          if ($.async === false)
            throw new f();
          return l.then((e) => n(e, t, $));
        }
        return n(l, t, $);
      };
    }
    j(r, "~standard", () => ({ validate: (n) => {
      try {
        let v = Av(r, n);
        return v.success ? { value: v.data } : { issues: v.error?.issues };
      } catch (v) {
        return Kv(r, n).then((u) => u.success ? { value: u.data } : { issues: u.error?.issues });
      }
    }, vendor: "zod", version: 1 }));
  });
  var Ur = c("$ZodString", (r, i) => {
    z.init(r, i), r._zod.pattern = [...r?._zod.bag?.patterns ?? []].pop() ?? ro(r._zod.bag), r._zod.parse = (o, t) => {
      if (i.coerce)
        try {
          o.value = String(o.value);
        } catch (n) {
        }
      if (typeof o.value === "string")
        return o;
      return o.issues.push({ expected: "string", code: "invalid_type", input: o.value, inst: r }), o;
    };
  });
  var E = c("$ZodStringFormat", (r, i) => {
    Wr.init(r, i), Ur.init(r, i);
  });
  var Go = c("$ZodGUID", (r, i) => {
    i.pattern ?? (i.pattern = Hv), E.init(r, i);
  });
  var Wo = c("$ZodUUID", (r, i) => {
    if (i.version) {
      let t = { v1: 1, v2: 2, v3: 3, v4: 4, v5: 5, v6: 6, v7: 7, v8: 8 }[i.version];
      if (t === void 0)
        throw Error(`Invalid UUID version: "${i.version}"`);
      i.pattern ?? (i.pattern = _r(t));
    } else
      i.pattern ?? (i.pattern = _r());
    E.init(r, i);
  });
  var Xo = c("$ZodEmail", (r, i) => {
    i.pattern ?? (i.pattern = Mv), E.init(r, i);
  });
  var Vo = c("$ZodURL", (r, i) => {
    E.init(r, i), r._zod.check = (o) => {
      try {
        let t = o.value.trim(), n = new URL(t);
        if (i.hostname) {
          if (i.hostname.lastIndex = 0, !i.hostname.test(n.hostname))
            o.issues.push({ code: "invalid_format", format: "url", note: "Invalid hostname", pattern: i.hostname.source, input: o.value, inst: r, continue: !i.abort });
        }
        if (i.protocol) {
          if (i.protocol.lastIndex = 0, !i.protocol.test(n.protocol.endsWith(":") ? n.protocol.slice(0, -1) : n.protocol))
            o.issues.push({ code: "invalid_format", format: "url", note: "Invalid protocol", pattern: i.protocol.source, input: o.value, inst: r, continue: !i.abort });
        }
        if (i.normalize)
          o.value = n.href;
        else
          o.value = t;
        return;
      } catch (t) {
        o.issues.push({ code: "invalid_format", format: "url", input: o.value, inst: r, continue: !i.abort });
      }
    };
  });
  var Ao = c("$ZodEmoji", (r, i) => {
    i.pattern ?? (i.pattern = Rv()), E.init(r, i);
  });
  var Ko = c("$ZodNanoID", (r, i) => {
    i.pattern ?? (i.pattern = Fv), E.init(r, i);
  });
  var qo = c("$ZodCUID", (r, i) => {
    i.pattern ?? (i.pattern = qv), E.init(r, i);
  });
  var Yo = c("$ZodCUID2", (r, i) => {
    i.pattern ?? (i.pattern = Yv), E.init(r, i);
  });
  var Qo = c("$ZodULID", (r, i) => {
    i.pattern ?? (i.pattern = Qv), E.init(r, i);
  });
  var mo = c("$ZodXID", (r, i) => {
    i.pattern ?? (i.pattern = mv), E.init(r, i);
  });
  var To = c("$ZodKSUID", (r, i) => {
    i.pattern ?? (i.pattern = Tv), E.init(r, i);
  });
  var Fo = c("$ZodISODateTime", (r, i) => {
    i.pattern ?? (i.pattern = sv(i)), E.init(r, i);
  });
  var Bo = c("$ZodISODate", (r, i) => {
    i.pattern ?? (i.pattern = av), E.init(r, i);
  });
  var Ho = c("$ZodISOTime", (r, i) => {
    i.pattern ?? (i.pattern = pv(i)), E.init(r, i);
  });
  var Mo = c("$ZodISODuration", (r, i) => {
    i.pattern ?? (i.pattern = Bv), E.init(r, i);
  });
  var Ro = c("$ZodIPv4", (r, i) => {
    i.pattern ?? (i.pattern = xv), E.init(r, i), r._zod.bag.format = "ipv4";
  });
  var xo = c("$ZodIPv6", (r, i) => {
    i.pattern ?? (i.pattern = Zv), E.init(r, i), r._zod.bag.format = "ipv6", r._zod.check = (o) => {
      try {
        new URL(`http://[${o.value}]`);
      } catch {
        o.issues.push({ code: "invalid_format", format: "ipv6", input: o.value, inst: r, continue: !i.abort });
      }
    };
  });
  var Zo = c("$ZodMAC", (r, i) => {
    i.pattern ?? (i.pattern = dv(i.delimiter)), E.init(r, i), r._zod.bag.format = "mac";
  });
  var Co = c("$ZodCIDRv4", (r, i) => {
    i.pattern ?? (i.pattern = Cv), E.init(r, i);
  });
  var fo = c("$ZodCIDRv6", (r, i) => {
    i.pattern ?? (i.pattern = fv), E.init(r, i), r._zod.check = (o) => {
      let t = o.value.split("/");
      try {
        if (t.length !== 2)
          throw Error();
        let [n, v] = t;
        if (!v)
          throw Error();
        let u = Number(v);
        if (`${u}` !== v)
          throw Error();
        if (u < 0 || u > 128)
          throw Error();
        new URL(`http://[${n}]`);
      } catch {
        o.issues.push({ code: "invalid_format", format: "cidrv6", input: o.value, inst: r, continue: !i.abort });
      }
    };
  });
  function ho(r) {
    if (r === "")
      return true;
    if (r.length % 4 !== 0)
      return false;
    try {
      return atob(r), true;
    } catch {
      return false;
    }
  }
  var yo = c("$ZodBase64", (r, i) => {
    i.pattern ?? (i.pattern = hv), E.init(r, i), r._zod.bag.contentEncoding = "base64", r._zod.check = (o) => {
      if (ho(o.value))
        return;
      o.issues.push({ code: "invalid_format", format: "base64", input: o.value, inst: r, continue: !i.abort });
    };
  });
  function Ce(r) {
    if (!fn.test(r))
      return false;
    let i = r.replace(/[-_]/g, (t) => t === "-" ? "+" : "/"), o = i.padEnd(Math.ceil(i.length / 4) * 4, "=");
    return ho(o);
  }
  var ao = c("$ZodBase64URL", (r, i) => {
    i.pattern ?? (i.pattern = fn), E.init(r, i), r._zod.bag.contentEncoding = "base64url", r._zod.check = (o) => {
      if (Ce(o.value))
        return;
      o.issues.push({ code: "invalid_format", format: "base64url", input: o.value, inst: r, continue: !i.abort });
    };
  });
  var po = c("$ZodE164", (r, i) => {
    i.pattern ?? (i.pattern = yv), E.init(r, i);
  });
  function fe(r, i = null) {
    try {
      let o = r.split(".");
      if (o.length !== 3)
        return false;
      let [t] = o;
      if (!t)
        return false;
      let n = JSON.parse(atob(t));
      if ("typ" in n && n?.typ !== "JWT")
        return false;
      if (!n.alg)
        return false;
      if (i && (!("alg" in n) || n.alg !== i))
        return false;
      return true;
    } catch {
      return false;
    }
  }
  var so = c("$ZodJWT", (r, i) => {
    E.init(r, i), r._zod.check = (o) => {
      if (fe(o.value, i.alg))
        return;
      o.issues.push({ code: "invalid_format", format: "jwt", input: o.value, inst: r, continue: !i.abort });
    };
  });
  var rt = c("$ZodCustomStringFormat", (r, i) => {
    E.init(r, i), r._zod.check = (o) => {
      if (i.fn(o.value))
        return;
      o.issues.push({ code: "invalid_format", format: i.format, input: o.value, inst: r, continue: !i.abort });
    };
  });
  var vi = c("$ZodNumber", (r, i) => {
    z.init(r, i), r._zod.pattern = r._zod.bag.pattern ?? ln, r._zod.parse = (o, t) => {
      if (i.coerce)
        try {
          o.value = Number(o.value);
        } catch (u) {
        }
      let n = o.value;
      if (typeof n === "number" && !Number.isNaN(n) && Number.isFinite(n))
        return o;
      let v = typeof n === "number" ? Number.isNaN(n) ? "NaN" : !Number.isFinite(n) ? "Infinity" : void 0 : void 0;
      return o.issues.push({ expected: "number", code: "invalid_type", input: n, inst: r, ...v ? { received: v } : {} }), o;
    };
  });
  var nt = c("$ZodNumberFormat", (r, i) => {
    eo.init(r, i), vi.init(r, i);
  });
  var bn = c("$ZodBoolean", (r, i) => {
    z.init(r, i), r._zod.pattern = vo, r._zod.parse = (o, t) => {
      if (i.coerce)
        try {
          o.value = Boolean(o.value);
        } catch (v) {
        }
      let n = o.value;
      if (typeof n === "boolean")
        return o;
      return o.issues.push({ expected: "boolean", code: "invalid_type", input: n, inst: r }), o;
    };
  });
  var oi = c("$ZodBigInt", (r, i) => {
    z.init(r, i), r._zod.pattern = no, r._zod.parse = (o, t) => {
      if (i.coerce)
        try {
          o.value = BigInt(o.value);
        } catch (n) {
        }
      if (typeof o.value === "bigint")
        return o;
      return o.issues.push({ expected: "bigint", code: "invalid_type", input: o.value, inst: r }), o;
    };
  });
  var it = c("$ZodBigIntFormat", (r, i) => {
    lo.init(r, i), oi.init(r, i);
  });
  var vt = c("$ZodSymbol", (r, i) => {
    z.init(r, i), r._zod.parse = (o, t) => {
      let n = o.value;
      if (typeof n === "symbol")
        return o;
      return o.issues.push({ expected: "symbol", code: "invalid_type", input: n, inst: r }), o;
    };
  });
  var ot = c("$ZodUndefined", (r, i) => {
    z.init(r, i), r._zod.pattern = to, r._zod.values = /* @__PURE__ */ new Set([void 0]), r._zod.optin = "optional", r._zod.optout = "optional", r._zod.parse = (o, t) => {
      let n = o.value;
      if (typeof n > "u")
        return o;
      return o.issues.push({ expected: "undefined", code: "invalid_type", input: n, inst: r }), o;
    };
  });
  var tt = c("$ZodNull", (r, i) => {
    z.init(r, i), r._zod.pattern = oo, r._zod.values = /* @__PURE__ */ new Set([null]), r._zod.parse = (o, t) => {
      let n = o.value;
      if (n === null)
        return o;
      return o.issues.push({ expected: "null", code: "invalid_type", input: n, inst: r }), o;
    };
  });
  var ut = c("$ZodAny", (r, i) => {
    z.init(r, i), r._zod.parse = (o) => o;
  });
  var $t = c("$ZodUnknown", (r, i) => {
    z.init(r, i), r._zod.parse = (o) => o;
  });
  var gt = c("$ZodNever", (r, i) => {
    z.init(r, i), r._zod.parse = (o, t) => {
      return o.issues.push({ expected: "never", code: "invalid_type", input: o.value, inst: r }), o;
    };
  });
  var et = c("$ZodVoid", (r, i) => {
    z.init(r, i), r._zod.parse = (o, t) => {
      let n = o.value;
      if (typeof n > "u")
        return o;
      return o.issues.push({ expected: "void", code: "invalid_type", input: n, inst: r }), o;
    };
  });
  var lt = c("$ZodDate", (r, i) => {
    z.init(r, i), r._zod.parse = (o, t) => {
      if (i.coerce)
        try {
          o.value = new Date(o.value);
        } catch ($) {
        }
      let n = o.value, v = n instanceof Date;
      if (v && !Number.isNaN(n.getTime()))
        return o;
      return o.issues.push({ expected: "date", code: "invalid_type", input: n, ...v ? { received: "Invalid Date" } : {}, inst: r }), o;
    };
  });
  function Qe(r, i, o) {
    if (r.issues.length)
      i.issues.push(...H(o, r.issues));
    i.value[o] = r.value;
  }
  var ct = c("$ZodArray", (r, i) => {
    z.init(r, i), r._zod.parse = (o, t) => {
      let n = o.value;
      if (!Array.isArray(n))
        return o.issues.push({ expected: "array", code: "invalid_type", input: n, inst: r }), o;
      o.value = Array(n.length);
      let v = [];
      for (let u = 0; u < n.length; u++) {
        let $ = n[u], l = i.element._zod.run({ value: $, issues: [] }, t);
        if (l instanceof Promise)
          v.push(l.then((e) => Qe(e, o, u)));
        else
          Qe(l, o, u);
      }
      if (v.length)
        return Promise.all(v).then(() => o);
      return o;
    };
  });
  function ii(r, i, o, t, n) {
    if (r.issues.length) {
      if (n && !(o in t))
        return;
      i.issues.push(...H(o, r.issues));
    }
    if (r.value === void 0) {
      if (o in t)
        i.value[o] = void 0;
    } else
      i.value[o] = r.value;
  }
  function he(r) {
    let i = Object.keys(r.shape);
    for (let t of i)
      if (!r.shape?.[t]?._zod?.traits?.has("$ZodType"))
        throw Error(`Invalid element at key "${t}": expected a Zod schema`);
    let o = Ev(r.shape);
    return { ...r, keys: i, keySet: new Set(i), numKeys: i.length, optionalKeys: new Set(o) };
  }
  function ye(r, i, o, t, n, v) {
    let u = [], $ = n.keySet, l = n.catchall._zod, e = l.def.type, I = l.optout === "optional";
    for (let _ in i) {
      if ($.has(_))
        continue;
      if (e === "never") {
        u.push(_);
        continue;
      }
      let N = l.run({ value: i[_], issues: [] }, t);
      if (N instanceof Promise)
        r.push(N.then((O) => ii(O, o, _, i, I)));
      else
        ii(N, o, _, i, I);
    }
    if (u.length)
      o.issues.push({ code: "unrecognized_keys", keys: u, input: i, inst: v });
    if (!r.length)
      return o;
    return Promise.all(r).then(() => {
      return o;
    });
  }
  var ae = c("$ZodObject", (r, i) => {
    if (z.init(r, i), !Object.getOwnPropertyDescriptor(i, "shape")?.get) {
      let $ = i.shape;
      Object.defineProperty(i, "shape", { get: () => {
        let l = { ...$ };
        return Object.defineProperty(i, "shape", { value: l }), l;
      } });
    }
    let t = Pr(() => he(i));
    j(r._zod, "propValues", () => {
      let $ = i.shape, l = {};
      for (let e in $) {
        let I = $[e]._zod;
        if (I.values) {
          l[e] ?? (l[e] = /* @__PURE__ */ new Set());
          for (let _ of I.values)
            l[e].add(_);
        }
      }
      return l;
    });
    let n = br, v = i.catchall, u;
    r._zod.parse = ($, l) => {
      u ?? (u = t.value);
      let e = $.value;
      if (!n(e))
        return $.issues.push({ expected: "object", code: "invalid_type", input: e, inst: r }), $;
      $.value = {};
      let I = [], _ = u.shape;
      for (let N of u.keys) {
        let O = _[N], J = O._zod.optout === "optional", X = O._zod.run({ value: e[N], issues: [] }, l);
        if (X instanceof Promise)
          I.push(X.then((Sr) => ii(Sr, $, N, e, J)));
        else
          ii(X, $, N, e, J);
      }
      if (!v)
        return I.length ? Promise.all(I).then(() => $) : $;
      return ye(I, e, $, l, t.value, r);
    };
  });
  var It = c("$ZodObjectJIT", (r, i) => {
    ae.init(r, i);
    let o = r._zod.parse, t = Pr(() => he(i)), n = (N) => {
      let O = new an(["shape", "payload", "ctx"]), J = t.value, X = (C) => {
        let m = Qn(C);
        return `shape[${m}]._zod.run({ value: input[${m}], issues: [] }, ctx)`;
      };
      O.write("const input = payload.value;");
      let Sr = /* @__PURE__ */ Object.create(null), Xc = 0;
      for (let C of J.keys)
        Sr[C] = `key_${Xc++}`;
      O.write("const newResult = {};");
      for (let C of J.keys) {
        let m = Sr[C], Z = Qn(C), Ac = N[C]?._zod?.optout === "optional";
        if (O.write(`const ${m} = ${X(C)};`), Ac)
          O.write(`
        if (${m}.issues.length) {
          if (${Z} in input) {
            payload.issues = payload.issues.concat(${m}.issues.map(iss => ({
              ...iss,
              path: iss.path ? [${Z}, ...iss.path] : [${Z}]
            })));
          }
        }
        
        if (${m}.value === undefined) {
          if (${Z} in input) {
            newResult[${Z}] = undefined;
          }
        } else {
          newResult[${Z}] = ${m}.value;
        }
        
      `);
        else
          O.write(`
        if (${m}.issues.length) {
          payload.issues = payload.issues.concat(${m}.issues.map(iss => ({
            ...iss,
            path: iss.path ? [${Z}, ...iss.path] : [${Z}]
          })));
        }
        
        if (${m}.value === undefined) {
          if (${Z} in input) {
            newResult[${Z}] = undefined;
          }
        } else {
          newResult[${Z}] = ${m}.value;
        }
        
      `);
      }
      O.write("payload.value = newResult;"), O.write("return payload;");
      let Vc = O.compile();
      return (C, m) => Vc(N, C, m);
    }, v, u = br, $ = !sr.jitless, e = $ && jv.value, I = i.catchall, _;
    r._zod.parse = (N, O) => {
      _ ?? (_ = t.value);
      let J = N.value;
      if (!u(J))
        return N.issues.push({ expected: "object", code: "invalid_type", input: J, inst: r }), N;
      if ($ && e && O?.async === false && O.jitless !== true) {
        if (!v)
          v = n(i.shape);
        if (N = v(N, O), !I)
          return N;
        return ye([], J, N, O, _, r);
      }
      return o(N, O);
    };
  });
  function me(r, i, o, t) {
    for (let v of r)
      if (v.issues.length === 0)
        return i.value = v.value, i;
    let n = r.filter((v) => !ur(v));
    if (n.length === 1)
      return i.value = n[0].value, n[0];
    return i.issues.push({ code: "invalid_union", input: i.value, inst: o, errors: r.map((v) => v.issues.map((u) => T(u, t, V()))) }), i;
  }
  var _n = c("$ZodUnion", (r, i) => {
    z.init(r, i), j(r._zod, "optin", () => i.options.some((n) => n._zod.optin === "optional") ? "optional" : void 0), j(r._zod, "optout", () => i.options.some((n) => n._zod.optout === "optional") ? "optional" : void 0), j(r._zod, "values", () => {
      if (i.options.every((n) => n._zod.values))
        return new Set(i.options.flatMap((n) => Array.from(n._zod.values)));
      return;
    }), j(r._zod, "pattern", () => {
      if (i.options.every((n) => n._zod.pattern)) {
        let n = i.options.map((v) => v._zod.pattern);
        return new RegExp(`^(${n.map((v) => vn(v.source)).join("|")})$`);
      }
      return;
    });
    let o = i.options.length === 1, t = i.options[0]._zod.run;
    r._zod.parse = (n, v) => {
      if (o)
        return t(n, v);
      let u = false, $ = [];
      for (let l of i.options) {
        let e = l._zod.run({ value: n.value, issues: [] }, v);
        if (e instanceof Promise)
          $.push(e), u = true;
        else {
          if (e.issues.length === 0)
            return e;
          $.push(e);
        }
      }
      if (!u)
        return me($, n, r, v);
      return Promise.all($).then((l) => {
        return me(l, n, r, v);
      });
    };
  });
  function Te(r, i, o, t) {
    let n = r.filter((v) => v.issues.length === 0);
    if (n.length === 1)
      return i.value = n[0].value, i;
    if (n.length === 0)
      i.issues.push({ code: "invalid_union", input: i.value, inst: o, errors: r.map((v) => v.issues.map((u) => T(u, t, V()))) });
    else
      i.issues.push({ code: "invalid_union", input: i.value, inst: o, errors: [], inclusive: false });
    return i;
  }
  var bt = c("$ZodXor", (r, i) => {
    _n.init(r, i), i.inclusive = false;
    let o = i.options.length === 1, t = i.options[0]._zod.run;
    r._zod.parse = (n, v) => {
      if (o)
        return t(n, v);
      let u = false, $ = [];
      for (let l of i.options) {
        let e = l._zod.run({ value: n.value, issues: [] }, v);
        if (e instanceof Promise)
          $.push(e), u = true;
        else
          $.push(e);
      }
      if (!u)
        return Te($, n, r, v);
      return Promise.all($).then((l) => {
        return Te(l, n, r, v);
      });
    };
  });
  var _t = c("$ZodDiscriminatedUnion", (r, i) => {
    i.inclusive = false, _n.init(r, i);
    let o = r._zod.parse;
    j(r._zod, "propValues", () => {
      let n = {};
      for (let v of i.options) {
        let u = v._zod.propValues;
        if (!u || Object.keys(u).length === 0)
          throw Error(`Invalid discriminated union option at index "${i.options.indexOf(v)}"`);
        for (let [$, l] of Object.entries(u)) {
          if (!n[$])
            n[$] = /* @__PURE__ */ new Set();
          for (let e of l)
            n[$].add(e);
        }
      }
      return n;
    });
    let t = Pr(() => {
      let n = i.options, v = /* @__PURE__ */ new Map();
      for (let u of n) {
        let $ = u._zod.propValues?.[i.discriminator];
        if (!$ || $.size === 0)
          throw Error(`Invalid discriminated union option at index "${i.options.indexOf(u)}"`);
        for (let l of $) {
          if (v.has(l))
            throw Error(`Duplicate discriminator value "${String(l)}"`);
          v.set(l, u);
        }
      }
      return v;
    });
    r._zod.parse = (n, v) => {
      let u = n.value;
      if (!br(u))
        return n.issues.push({ code: "invalid_type", expected: "object", input: u, inst: r }), n;
      let $ = t.value.get(u?.[i.discriminator]);
      if ($)
        return $._zod.run(n, v);
      if (i.unionFallback)
        return o(n, v);
      return n.issues.push({ code: "invalid_union", errors: [], note: "No matching discriminator", discriminator: i.discriminator, input: u, path: [i.discriminator], inst: r }), n;
    };
  });
  var Ut = c("$ZodIntersection", (r, i) => {
    z.init(r, i), r._zod.parse = (o, t) => {
      let n = o.value, v = i.left._zod.run({ value: n, issues: [] }, t), u = i.right._zod.run({ value: n, issues: [] }, t);
      if (v instanceof Promise || u instanceof Promise)
        return Promise.all([v, u]).then(([l, e]) => {
          return Fe(o, l, e);
        });
      return Fe(o, v, u);
    };
  });
  function Eo(r, i) {
    if (r === i)
      return { valid: true, data: r };
    if (r instanceof Date && i instanceof Date && +r === +i)
      return { valid: true, data: r };
    if (tr(r) && tr(i)) {
      let o = Object.keys(i), t = Object.keys(r).filter((v) => o.indexOf(v) !== -1), n = { ...r, ...i };
      for (let v of t) {
        let u = Eo(r[v], i[v]);
        if (!u.valid)
          return { valid: false, mergeErrorPath: [v, ...u.mergeErrorPath] };
        n[v] = u.data;
      }
      return { valid: true, data: n };
    }
    if (Array.isArray(r) && Array.isArray(i)) {
      if (r.length !== i.length)
        return { valid: false, mergeErrorPath: [] };
      let o = [];
      for (let t = 0; t < r.length; t++) {
        let n = r[t], v = i[t], u = Eo(n, v);
        if (!u.valid)
          return { valid: false, mergeErrorPath: [t, ...u.mergeErrorPath] };
        o.push(u.data);
      }
      return { valid: true, data: o };
    }
    return { valid: false, mergeErrorPath: [] };
  }
  function Fe(r, i, o) {
    let t = /* @__PURE__ */ new Map(), n;
    for (let $ of i.issues)
      if ($.code === "unrecognized_keys") {
        n ?? (n = $);
        for (let l of $.keys) {
          if (!t.has(l))
            t.set(l, {});
          t.get(l).l = true;
        }
      } else
        r.issues.push($);
    for (let $ of o.issues)
      if ($.code === "unrecognized_keys")
        for (let l of $.keys) {
          if (!t.has(l))
            t.set(l, {});
          t.get(l).r = true;
        }
      else
        r.issues.push($);
    let v = [...t].filter(([, $]) => $.l && $.r).map(([$]) => $);
    if (v.length && n)
      r.issues.push({ ...n, keys: v });
    if (ur(r))
      return r;
    let u = Eo(i.value, o.value);
    if (!u.valid)
      throw Error(`Unmergable intersection. Error path: ${JSON.stringify(u.mergeErrorPath)}`);
    return r.value = u.data, r;
  }
  var ti = c("$ZodTuple", (r, i) => {
    z.init(r, i);
    let o = i.items;
    r._zod.parse = (t, n) => {
      let v = t.value;
      if (!Array.isArray(v))
        return t.issues.push({ input: v, inst: r, expected: "tuple", code: "invalid_type" }), t;
      t.value = [];
      let u = [], $ = [...o].reverse().findIndex((I) => I._zod.optin !== "optional"), l = $ === -1 ? 0 : o.length - $;
      if (!i.rest) {
        let I = v.length > o.length, _ = v.length < l - 1;
        if (I || _)
          return t.issues.push({ ...I ? { code: "too_big", maximum: o.length, inclusive: true } : { code: "too_small", minimum: o.length }, input: v, inst: r, origin: "array" }), t;
      }
      let e = -1;
      for (let I of o) {
        if (e++, e >= v.length) {
          if (e >= l)
            continue;
        }
        let _ = I._zod.run({ value: v[e], issues: [] }, n);
        if (_ instanceof Promise)
          u.push(_.then((N) => pn(N, t, e)));
        else
          pn(_, t, e);
      }
      if (i.rest) {
        let I = v.slice(o.length);
        for (let _ of I) {
          e++;
          let N = i.rest._zod.run({ value: _, issues: [] }, n);
          if (N instanceof Promise)
            u.push(N.then((O) => pn(O, t, e)));
          else
            pn(N, t, e);
        }
      }
      if (u.length)
        return Promise.all(u).then(() => t);
      return t;
    };
  });
  function pn(r, i, o) {
    if (r.issues.length)
      i.issues.push(...H(o, r.issues));
    i.value[o] = r.value;
  }
  var kt = c("$ZodRecord", (r, i) => {
    z.init(r, i), r._zod.parse = (o, t) => {
      let n = o.value;
      if (!tr(n))
        return o.issues.push({ expected: "record", code: "invalid_type", input: n, inst: r }), o;
      let v = [], u = i.keyType._zod.values;
      if (u) {
        o.value = {};
        let $ = /* @__PURE__ */ new Set();
        for (let e of u)
          if (typeof e === "string" || typeof e === "number" || typeof e === "symbol") {
            $.add(typeof e === "number" ? e.toString() : e);
            let I = i.valueType._zod.run({ value: n[e], issues: [] }, t);
            if (I instanceof Promise)
              v.push(I.then((_) => {
                if (_.issues.length)
                  o.issues.push(...H(e, _.issues));
                o.value[e] = _.value;
              }));
            else {
              if (I.issues.length)
                o.issues.push(...H(e, I.issues));
              o.value[e] = I.value;
            }
          }
        let l;
        for (let e in n)
          if (!$.has(e))
            l = l ?? [], l.push(e);
        if (l && l.length > 0)
          o.issues.push({ code: "unrecognized_keys", input: n, inst: r, keys: l });
      } else {
        o.value = {};
        for (let $ of Reflect.ownKeys(n)) {
          if ($ === "__proto__")
            continue;
          let l = i.keyType._zod.run({ value: $, issues: [] }, t);
          if (l instanceof Promise)
            throw Error("Async schemas not supported in object keys currently");
          if (typeof $ === "string" && ln.test($) && l.issues.length && l.issues.some((_) => _.code === "invalid_type" && _.expected === "number")) {
            let _ = i.keyType._zod.run({ value: Number($), issues: [] }, t);
            if (_ instanceof Promise)
              throw Error("Async schemas not supported in object keys currently");
            if (_.issues.length === 0)
              l = _;
          }
          if (l.issues.length) {
            if (i.mode === "loose")
              o.value[$] = n[$];
            else
              o.issues.push({ code: "invalid_key", origin: "record", issues: l.issues.map((_) => T(_, t, V())), input: $, path: [$], inst: r });
            continue;
          }
          let I = i.valueType._zod.run({ value: n[$], issues: [] }, t);
          if (I instanceof Promise)
            v.push(I.then((_) => {
              if (_.issues.length)
                o.issues.push(...H($, _.issues));
              o.value[l.value] = _.value;
            }));
          else {
            if (I.issues.length)
              o.issues.push(...H($, I.issues));
            o.value[l.value] = I.value;
          }
        }
      }
      if (v.length)
        return Promise.all(v).then(() => o);
      return o;
    };
  });
  var Dt = c("$ZodMap", (r, i) => {
    z.init(r, i), r._zod.parse = (o, t) => {
      let n = o.value;
      if (!(n instanceof Map))
        return o.issues.push({ expected: "map", code: "invalid_type", input: n, inst: r }), o;
      let v = [];
      o.value = /* @__PURE__ */ new Map();
      for (let [u, $] of n) {
        let l = i.keyType._zod.run({ value: u, issues: [] }, t), e = i.valueType._zod.run({ value: $, issues: [] }, t);
        if (l instanceof Promise || e instanceof Promise)
          v.push(Promise.all([l, e]).then(([I, _]) => {
            Be(I, _, o, u, n, r, t);
          }));
        else
          Be(l, e, o, u, n, r, t);
      }
      if (v.length)
        return Promise.all(v).then(() => o);
      return o;
    };
  });
  function Be(r, i, o, t, n, v, u) {
    if (r.issues.length)
      if (on.has(typeof t))
        o.issues.push(...H(t, r.issues));
      else
        o.issues.push({ code: "invalid_key", origin: "map", input: n, inst: v, issues: r.issues.map(($) => T($, u, V())) });
    if (i.issues.length)
      if (on.has(typeof t))
        o.issues.push(...H(t, i.issues));
      else
        o.issues.push({ origin: "map", code: "invalid_element", input: n, inst: v, key: t, issues: i.issues.map(($) => T($, u, V())) });
    o.value.set(r.value, i.value);
  }
  var wt = c("$ZodSet", (r, i) => {
    z.init(r, i), r._zod.parse = (o, t) => {
      let n = o.value;
      if (!(n instanceof Set))
        return o.issues.push({ input: n, inst: r, expected: "set", code: "invalid_type" }), o;
      let v = [];
      o.value = /* @__PURE__ */ new Set();
      for (let u of n) {
        let $ = i.valueType._zod.run({ value: u, issues: [] }, t);
        if ($ instanceof Promise)
          v.push($.then((l) => He(l, o)));
        else
          He($, o);
      }
      if (v.length)
        return Promise.all(v).then(() => o);
      return o;
    };
  });
  function He(r, i) {
    if (r.issues.length)
      i.issues.push(...r.issues);
    i.value.add(r.value);
  }
  var Nt = c("$ZodEnum", (r, i) => {
    z.init(r, i);
    let o = nn(i.entries), t = new Set(o);
    r._zod.values = t, r._zod.pattern = new RegExp(`^(${o.filter((n) => on.has(typeof n)).map((n) => typeof n === "string" ? R(n) : n.toString()).join("|")})$`), r._zod.parse = (n, v) => {
      let u = n.value;
      if (t.has(u))
        return n;
      return n.issues.push({ code: "invalid_value", values: o, input: u, inst: r }), n;
    };
  });
  var Ot = c("$ZodLiteral", (r, i) => {
    if (z.init(r, i), i.values.length === 0)
      throw Error("Cannot create literal schema with no valid values");
    let o = new Set(i.values);
    r._zod.values = o, r._zod.pattern = new RegExp(`^(${i.values.map((t) => typeof t === "string" ? R(t) : t ? R(t.toString()) : String(t)).join("|")})$`), r._zod.parse = (t, n) => {
      let v = t.value;
      if (o.has(v))
        return t;
      return t.issues.push({ code: "invalid_value", values: i.values, input: v, inst: r }), t;
    };
  });
  var St = c("$ZodFile", (r, i) => {
    z.init(r, i), r._zod.parse = (o, t) => {
      let n = o.value;
      if (n instanceof File)
        return o;
      return o.issues.push({ expected: "file", code: "invalid_type", input: n, inst: r }), o;
    };
  });
  var zt = c("$ZodTransform", (r, i) => {
    z.init(r, i), r._zod.parse = (o, t) => {
      if (t.direction === "backward")
        throw new Ir(r.constructor.name);
      let n = i.transform(o.value, o);
      if (t.async)
        return (n instanceof Promise ? n : Promise.resolve(n)).then((u) => {
          return o.value = u, o;
        });
      if (n instanceof Promise)
        throw new f();
      return o.value = n, o;
    };
  });
  function Me(r, i) {
    if (r.issues.length && i === void 0)
      return { issues: [], value: void 0 };
    return r;
  }
  var ui = c("$ZodOptional", (r, i) => {
    z.init(r, i), r._zod.optin = "optional", r._zod.optout = "optional", j(r._zod, "values", () => {
      return i.innerType._zod.values ? /* @__PURE__ */ new Set([...i.innerType._zod.values, void 0]) : void 0;
    }), j(r._zod, "pattern", () => {
      let o = i.innerType._zod.pattern;
      return o ? new RegExp(`^(${vn(o.source)})?$`) : void 0;
    }), r._zod.parse = (o, t) => {
      if (i.innerType._zod.optin === "optional") {
        let n = i.innerType._zod.run(o, t);
        if (n instanceof Promise)
          return n.then((v) => Me(v, o.value));
        return Me(n, o.value);
      }
      if (o.value === void 0)
        return o;
      return i.innerType._zod.run(o, t);
    };
  });
  var Pt = c("$ZodExactOptional", (r, i) => {
    ui.init(r, i), j(r._zod, "values", () => i.innerType._zod.values), j(r._zod, "pattern", () => i.innerType._zod.pattern), r._zod.parse = (o, t) => {
      return i.innerType._zod.run(o, t);
    };
  });
  var jt = c("$ZodNullable", (r, i) => {
    z.init(r, i), j(r._zod, "optin", () => i.innerType._zod.optin), j(r._zod, "optout", () => i.innerType._zod.optout), j(r._zod, "pattern", () => {
      let o = i.innerType._zod.pattern;
      return o ? new RegExp(`^(${vn(o.source)}|null)$`) : void 0;
    }), j(r._zod, "values", () => {
      return i.innerType._zod.values ? /* @__PURE__ */ new Set([...i.innerType._zod.values, null]) : void 0;
    }), r._zod.parse = (o, t) => {
      if (o.value === null)
        return o;
      return i.innerType._zod.run(o, t);
    };
  });
  var Jt = c("$ZodDefault", (r, i) => {
    z.init(r, i), r._zod.optin = "optional", j(r._zod, "values", () => i.innerType._zod.values), r._zod.parse = (o, t) => {
      if (t.direction === "backward")
        return i.innerType._zod.run(o, t);
      if (o.value === void 0)
        return o.value = i.defaultValue, o;
      let n = i.innerType._zod.run(o, t);
      if (n instanceof Promise)
        return n.then((v) => Re(v, i));
      return Re(n, i);
    };
  });
  function Re(r, i) {
    if (r.value === void 0)
      r.value = i.defaultValue;
    return r;
  }
  var Lt = c("$ZodPrefault", (r, i) => {
    z.init(r, i), r._zod.optin = "optional", j(r._zod, "values", () => i.innerType._zod.values), r._zod.parse = (o, t) => {
      if (t.direction === "backward")
        return i.innerType._zod.run(o, t);
      if (o.value === void 0)
        o.value = i.defaultValue;
      return i.innerType._zod.run(o, t);
    };
  });
  var Et = c("$ZodNonOptional", (r, i) => {
    z.init(r, i), j(r._zod, "values", () => {
      let o = i.innerType._zod.values;
      return o ? new Set([...o].filter((t) => t !== void 0)) : void 0;
    }), r._zod.parse = (o, t) => {
      let n = i.innerType._zod.run(o, t);
      if (n instanceof Promise)
        return n.then((v) => xe(v, r));
      return xe(n, r);
    };
  });
  function xe(r, i) {
    if (!r.issues.length && r.value === void 0)
      r.issues.push({ code: "invalid_type", expected: "nonoptional", input: r.value, inst: i });
    return r;
  }
  var Gt = c("$ZodSuccess", (r, i) => {
    z.init(r, i), r._zod.parse = (o, t) => {
      if (t.direction === "backward")
        throw new Ir("ZodSuccess");
      let n = i.innerType._zod.run(o, t);
      if (n instanceof Promise)
        return n.then((v) => {
          return o.value = v.issues.length === 0, o;
        });
      return o.value = n.issues.length === 0, o;
    };
  });
  var Wt = c("$ZodCatch", (r, i) => {
    z.init(r, i), j(r._zod, "optin", () => i.innerType._zod.optin), j(r._zod, "optout", () => i.innerType._zod.optout), j(r._zod, "values", () => i.innerType._zod.values), r._zod.parse = (o, t) => {
      if (t.direction === "backward")
        return i.innerType._zod.run(o, t);
      let n = i.innerType._zod.run(o, t);
      if (n instanceof Promise)
        return n.then((v) => {
          if (o.value = v.value, v.issues.length)
            o.value = i.catchValue({ ...o, error: { issues: v.issues.map((u) => T(u, t, V())) }, input: o.value }), o.issues = [];
          return o;
        });
      if (o.value = n.value, n.issues.length)
        o.value = i.catchValue({ ...o, error: { issues: n.issues.map((v) => T(v, t, V())) }, input: o.value }), o.issues = [];
      return o;
    };
  });
  var Xt = c("$ZodNaN", (r, i) => {
    z.init(r, i), r._zod.parse = (o, t) => {
      if (typeof o.value !== "number" || !Number.isNaN(o.value))
        return o.issues.push({ input: o.value, inst: r, expected: "nan", code: "invalid_type" }), o;
      return o;
    };
  });
  var Vt = c("$ZodPipe", (r, i) => {
    z.init(r, i), j(r._zod, "values", () => i.in._zod.values), j(r._zod, "optin", () => i.in._zod.optin), j(r._zod, "optout", () => i.out._zod.optout), j(r._zod, "propValues", () => i.in._zod.propValues), r._zod.parse = (o, t) => {
      if (t.direction === "backward") {
        let v = i.out._zod.run(o, t);
        if (v instanceof Promise)
          return v.then((u) => sn(u, i.in, t));
        return sn(v, i.in, t);
      }
      let n = i.in._zod.run(o, t);
      if (n instanceof Promise)
        return n.then((v) => sn(v, i.out, t));
      return sn(n, i.out, t);
    };
  });
  function sn(r, i, o) {
    if (r.issues.length)
      return r.aborted = true, r;
    return i._zod.run({ value: r.value, issues: r.issues }, o);
  }
  var Un = c("$ZodCodec", (r, i) => {
    z.init(r, i), j(r._zod, "values", () => i.in._zod.values), j(r._zod, "optin", () => i.in._zod.optin), j(r._zod, "optout", () => i.out._zod.optout), j(r._zod, "propValues", () => i.in._zod.propValues), r._zod.parse = (o, t) => {
      if ((t.direction || "forward") === "forward") {
        let v = i.in._zod.run(o, t);
        if (v instanceof Promise)
          return v.then((u) => ri(u, i, t));
        return ri(v, i, t);
      } else {
        let v = i.out._zod.run(o, t);
        if (v instanceof Promise)
          return v.then((u) => ri(u, i, t));
        return ri(v, i, t);
      }
    };
  });
  function ri(r, i, o) {
    if (r.issues.length)
      return r.aborted = true, r;
    if ((o.direction || "forward") === "forward") {
      let n = i.transform(r.value, r);
      if (n instanceof Promise)
        return n.then((v) => ni(r, v, i.out, o));
      return ni(r, n, i.out, o);
    } else {
      let n = i.reverseTransform(r.value, r);
      if (n instanceof Promise)
        return n.then((v) => ni(r, v, i.in, o));
      return ni(r, n, i.in, o);
    }
  }
  function ni(r, i, o, t) {
    if (r.issues.length)
      return r.aborted = true, r;
    return o._zod.run({ value: i, issues: r.issues }, t);
  }
  var At = c("$ZodReadonly", (r, i) => {
    z.init(r, i), j(r._zod, "propValues", () => i.innerType._zod.propValues), j(r._zod, "values", () => i.innerType._zod.values), j(r._zod, "optin", () => i.innerType?._zod?.optin), j(r._zod, "optout", () => i.innerType?._zod?.optout), r._zod.parse = (o, t) => {
      if (t.direction === "backward")
        return i.innerType._zod.run(o, t);
      let n = i.innerType._zod.run(o, t);
      if (n instanceof Promise)
        return n.then(Ze);
      return Ze(n);
    };
  });
  function Ze(r) {
    return r.value = Object.freeze(r.value), r;
  }
  var Kt = c("$ZodTemplateLiteral", (r, i) => {
    z.init(r, i);
    let o = [];
    for (let t of i.parts)
      if (typeof t === "object" && t !== null) {
        if (!t._zod.pattern)
          throw Error(`Invalid template literal part, no pattern found: ${[...t._zod.traits].shift()}`);
        let n = t._zod.pattern instanceof RegExp ? t._zod.pattern.source : t._zod.pattern;
        if (!n)
          throw Error(`Invalid template literal part: ${t._zod.traits}`);
        let v = n.startsWith("^") ? 1 : 0, u = n.endsWith("$") ? n.length - 1 : n.length;
        o.push(n.slice(v, u));
      } else if (t === null || Lv.has(typeof t))
        o.push(R(`${t}`));
      else
        throw Error(`Invalid template literal part: ${t}`);
    r._zod.pattern = new RegExp(`^${o.join("")}$`), r._zod.parse = (t, n) => {
      if (typeof t.value !== "string")
        return t.issues.push({ input: t.value, inst: r, expected: "string", code: "invalid_type" }), t;
      if (r._zod.pattern.lastIndex = 0, !r._zod.pattern.test(t.value))
        return t.issues.push({ input: t.value, inst: r, code: "invalid_format", format: i.format ?? "template_literal", pattern: r._zod.pattern.source }), t;
      return t;
    };
  });
  var qt = c("$ZodFunction", (r, i) => {
    return z.init(r, i), r._def = i, r._zod.def = i, r.implement = (o) => {
      if (typeof o !== "function")
        throw Error("implement() must be called with a function");
      return function(...t) {
        let n = r._def.input ? Tn(r._def.input, t) : t, v = Reflect.apply(o, this, n);
        if (r._def.output)
          return Tn(r._def.output, v);
        return v;
      };
    }, r.implementAsync = (o) => {
      if (typeof o !== "function")
        throw Error("implementAsync() must be called with a function");
      return async function(...t) {
        let n = r._def.input ? await Fn(r._def.input, t) : t, v = await Reflect.apply(o, this, n);
        if (r._def.output)
          return await Fn(r._def.output, v);
        return v;
      };
    }, r._zod.parse = (o, t) => {
      if (typeof o.value !== "function")
        return o.issues.push({ code: "invalid_type", expected: "function", input: o.value, inst: r }), o;
      if (r._def.output && r._def.output._zod.def.type === "promise")
        o.value = r.implementAsync(o.value);
      else
        o.value = r.implement(o.value);
      return o;
    }, r.input = (...o) => {
      let t = r.constructor;
      if (Array.isArray(o[0]))
        return new t({ type: "function", input: new ti({ type: "tuple", items: o[0], rest: o[1] }), output: r._def.output });
      return new t({ type: "function", input: o[0], output: r._def.output });
    }, r.output = (o) => {
      return new r.constructor({ type: "function", input: r._def.input, output: o });
    }, r;
  });
  var Yt = c("$ZodPromise", (r, i) => {
    z.init(r, i), r._zod.parse = (o, t) => {
      return Promise.resolve(o.value).then((n) => i.innerType._zod.run({ value: n, issues: [] }, t));
    };
  });
  var Qt = c("$ZodLazy", (r, i) => {
    z.init(r, i), j(r._zod, "innerType", () => i.getter()), j(r._zod, "pattern", () => r._zod.innerType?._zod?.pattern), j(r._zod, "propValues", () => r._zod.innerType?._zod?.propValues), j(r._zod, "optin", () => r._zod.innerType?._zod?.optin ?? void 0), j(r._zod, "optout", () => r._zod.innerType?._zod?.optout ?? void 0), r._zod.parse = (o, t) => {
      return r._zod.innerType._zod.run(o, t);
    };
  });
  var mt = c("$ZodCustom", (r, i) => {
    W.init(r, i), z.init(r, i), r._zod.parse = (o, t) => {
      return o;
    }, r._zod.check = (o) => {
      let t = o.value, n = i.fn(t);
      if (n instanceof Promise)
        return n.then((v) => de(v, o, t, r));
      de(n, o, t, r);
      return;
    };
  });
  function de(r, i, o, t) {
    if (!r) {
      let n = { code: "custom", input: o, inst: t, path: [...t._zod.def.path ?? []], continue: !t._zod.def.abort };
      if (t._zod.def.params)
        n.params = t._zod.def.params;
      i.issues.push(jr(n));
    }
  }
  var On = {};
  s(On, { zhTW: () => Gu, zhCN: () => Eu, yo: () => Wu, vi: () => Lu, uz: () => Ju, ur: () => ju, uk: () => Nn, ua: () => Pu, tr: () => zu, th: () => Su, ta: () => Ou, sv: () => Nu, sl: () => wu, ru: () => Du, pt: () => ku, ps: () => _u, pl: () => Uu, ota: () => bu, no: () => Iu, nl: () => cu, ms: () => lu, mk: () => eu, lt: () => gu, ko: () => $u, km: () => Dn, kh: () => uu, ka: () => tu, ja: () => ou, it: () => vu, is: () => iu, id: () => nu, hy: () => ru, hu: () => st, he: () => pt, frCA: () => at, fr: () => yt, fi: () => ht, fa: () => ft, es: () => Ct, eo: () => dt, en: () => kn, de: () => Zt, da: () => xt, cs: () => Rt, ca: () => Mt, bg: () => Ht, be: () => Bt, az: () => Ft, ar: () => Tt });
  var v4 = () => {
    let r = { string: { unit: "\u062D\u0631\u0641", verb: "\u0623\u0646 \u064A\u062D\u0648\u064A" }, file: { unit: "\u0628\u0627\u064A\u062A", verb: "\u0623\u0646 \u064A\u062D\u0648\u064A" }, array: { unit: "\u0639\u0646\u0635\u0631", verb: "\u0623\u0646 \u064A\u062D\u0648\u064A" }, set: { unit: "\u0639\u0646\u0635\u0631", verb: "\u0623\u0646 \u064A\u062D\u0648\u064A" } };
    function i(n) {
      return r[n] ?? null;
    }
    let o = { regex: "\u0645\u062F\u062E\u0644", email: "\u0628\u0631\u064A\u062F \u0625\u0644\u0643\u062A\u0631\u0648\u0646\u064A", url: "\u0631\u0627\u0628\u0637", emoji: "\u0625\u064A\u0645\u0648\u062C\u064A", uuid: "UUID", uuidv4: "UUIDv4", uuidv6: "UUIDv6", nanoid: "nanoid", guid: "GUID", cuid: "cuid", cuid2: "cuid2", ulid: "ULID", xid: "XID", ksuid: "KSUID", datetime: "\u062A\u0627\u0631\u064A\u062E \u0648\u0648\u0642\u062A \u0628\u0645\u0639\u064A\u0627\u0631 ISO", date: "\u062A\u0627\u0631\u064A\u062E \u0628\u0645\u0639\u064A\u0627\u0631 ISO", time: "\u0648\u0642\u062A \u0628\u0645\u0639\u064A\u0627\u0631 ISO", duration: "\u0645\u062F\u0629 \u0628\u0645\u0639\u064A\u0627\u0631 ISO", ipv4: "\u0639\u0646\u0648\u0627\u0646 IPv4", ipv6: "\u0639\u0646\u0648\u0627\u0646 IPv6", cidrv4: "\u0645\u062F\u0649 \u0639\u0646\u0627\u0648\u064A\u0646 \u0628\u0635\u064A\u063A\u0629 IPv4", cidrv6: "\u0645\u062F\u0649 \u0639\u0646\u0627\u0648\u064A\u0646 \u0628\u0635\u064A\u063A\u0629 IPv6", base64: "\u0646\u064E\u0635 \u0628\u062A\u0631\u0645\u064A\u0632 base64-encoded", base64url: "\u0646\u064E\u0635 \u0628\u062A\u0631\u0645\u064A\u0632 base64url-encoded", json_string: "\u0646\u064E\u0635 \u0639\u0644\u0649 \u0647\u064A\u0626\u0629 JSON", e164: "\u0631\u0642\u0645 \u0647\u0627\u062A\u0641 \u0628\u0645\u0639\u064A\u0627\u0631 E.164", jwt: "JWT", template_literal: "\u0645\u062F\u062E\u0644" }, t = { nan: "NaN" };
    return (n) => {
      switch (n.code) {
        case "invalid_type": {
          let v = t[n.expected] ?? n.expected, u = k(n.input), $ = t[u] ?? u;
          if (/^[A-Z]/.test(n.expected))
            return `\u0645\u062F\u062E\u0644\u0627\u062A \u063A\u064A\u0631 \u0645\u0642\u0628\u0648\u0644\u0629: \u064A\u0641\u062A\u0631\u0636 \u0625\u062F\u062E\u0627\u0644 instanceof ${n.expected}\u060C \u0648\u0644\u0643\u0646 \u062A\u0645 \u0625\u062F\u062E\u0627\u0644 ${$}`;
          return `\u0645\u062F\u062E\u0644\u0627\u062A \u063A\u064A\u0631 \u0645\u0642\u0628\u0648\u0644\u0629: \u064A\u0641\u062A\u0631\u0636 \u0625\u062F\u062E\u0627\u0644 ${v}\u060C \u0648\u0644\u0643\u0646 \u062A\u0645 \u0625\u062F\u062E\u0627\u0644 ${$}`;
        }
        case "invalid_value":
          if (n.values.length === 1)
            return `\u0645\u062F\u062E\u0644\u0627\u062A \u063A\u064A\u0631 \u0645\u0642\u0628\u0648\u0644\u0629: \u064A\u0641\u062A\u0631\u0636 \u0625\u062F\u062E\u0627\u0644 ${U(n.values[0])}`;
          return `\u0627\u062E\u062A\u064A\u0627\u0631 \u063A\u064A\u0631 \u0645\u0642\u0628\u0648\u0644: \u064A\u062A\u0648\u0642\u0639 \u0627\u0646\u062A\u0642\u0627\u0621 \u0623\u062D\u062F \u0647\u0630\u0647 \u0627\u0644\u062E\u064A\u0627\u0631\u0627\u062A: ${b(n.values, "|")}`;
        case "too_big": {
          let v = n.inclusive ? "<=" : "<", u = i(n.origin);
          if (u)
            return ` \u0623\u0643\u0628\u0631 \u0645\u0646 \u0627\u0644\u0644\u0627\u0632\u0645: \u064A\u0641\u062A\u0631\u0636 \u0623\u0646 \u062A\u0643\u0648\u0646 ${n.origin ?? "\u0627\u0644\u0642\u064A\u0645\u0629"} ${v} ${n.maximum.toString()} ${u.unit ?? "\u0639\u0646\u0635\u0631"}`;
          return `\u0623\u0643\u0628\u0631 \u0645\u0646 \u0627\u0644\u0644\u0627\u0632\u0645: \u064A\u0641\u062A\u0631\u0636 \u0623\u0646 \u062A\u0643\u0648\u0646 ${n.origin ?? "\u0627\u0644\u0642\u064A\u0645\u0629"} ${v} ${n.maximum.toString()}`;
        }
        case "too_small": {
          let v = n.inclusive ? ">=" : ">", u = i(n.origin);
          if (u)
            return `\u0623\u0635\u063A\u0631 \u0645\u0646 \u0627\u0644\u0644\u0627\u0632\u0645: \u064A\u0641\u062A\u0631\u0636 \u0644\u0640 ${n.origin} \u0623\u0646 \u064A\u0643\u0648\u0646 ${v} ${n.minimum.toString()} ${u.unit}`;
          return `\u0623\u0635\u063A\u0631 \u0645\u0646 \u0627\u0644\u0644\u0627\u0632\u0645: \u064A\u0641\u062A\u0631\u0636 \u0644\u0640 ${n.origin} \u0623\u0646 \u064A\u0643\u0648\u0646 ${v} ${n.minimum.toString()}`;
        }
        case "invalid_format": {
          let v = n;
          if (v.format === "starts_with")
            return `\u0646\u064E\u0635 \u063A\u064A\u0631 \u0645\u0642\u0628\u0648\u0644: \u064A\u062C\u0628 \u0623\u0646 \u064A\u0628\u062F\u0623 \u0628\u0640 "${n.prefix}"`;
          if (v.format === "ends_with")
            return `\u0646\u064E\u0635 \u063A\u064A\u0631 \u0645\u0642\u0628\u0648\u0644: \u064A\u062C\u0628 \u0623\u0646 \u064A\u0646\u062A\u0647\u064A \u0628\u0640 "${v.suffix}"`;
          if (v.format === "includes")
            return `\u0646\u064E\u0635 \u063A\u064A\u0631 \u0645\u0642\u0628\u0648\u0644: \u064A\u062C\u0628 \u0623\u0646 \u064A\u062A\u0636\u0645\u0651\u064E\u0646 "${v.includes}"`;
          if (v.format === "regex")
            return `\u0646\u064E\u0635 \u063A\u064A\u0631 \u0645\u0642\u0628\u0648\u0644: \u064A\u062C\u0628 \u0623\u0646 \u064A\u0637\u0627\u0628\u0642 \u0627\u0644\u0646\u0645\u0637 ${v.pattern}`;
          return `${o[v.format] ?? n.format} \u063A\u064A\u0631 \u0645\u0642\u0628\u0648\u0644`;
        }
        case "not_multiple_of":
          return `\u0631\u0642\u0645 \u063A\u064A\u0631 \u0645\u0642\u0628\u0648\u0644: \u064A\u062C\u0628 \u0623\u0646 \u064A\u0643\u0648\u0646 \u0645\u0646 \u0645\u0636\u0627\u0639\u0641\u0627\u062A ${n.divisor}`;
        case "unrecognized_keys":
          return `\u0645\u0639\u0631\u0641${n.keys.length > 1 ? "\u0627\u062A" : ""} \u063A\u0631\u064A\u0628${n.keys.length > 1 ? "\u0629" : ""}: ${b(n.keys, "\u060C ")}`;
        case "invalid_key":
          return `\u0645\u0639\u0631\u0641 \u063A\u064A\u0631 \u0645\u0642\u0628\u0648\u0644 \u0641\u064A ${n.origin}`;
        case "invalid_union":
          return "\u0645\u062F\u062E\u0644 \u063A\u064A\u0631 \u0645\u0642\u0628\u0648\u0644";
        case "invalid_element":
          return `\u0645\u062F\u062E\u0644 \u063A\u064A\u0631 \u0645\u0642\u0628\u0648\u0644 \u0641\u064A ${n.origin}`;
        default:
          return "\u0645\u062F\u062E\u0644 \u063A\u064A\u0631 \u0645\u0642\u0628\u0648\u0644";
      }
    };
  };
  function Tt() {
    return { localeError: v4() };
  }
  var o4 = () => {
    let r = { string: { unit: "simvol", verb: "olmal\u0131d\u0131r" }, file: { unit: "bayt", verb: "olmal\u0131d\u0131r" }, array: { unit: "element", verb: "olmal\u0131d\u0131r" }, set: { unit: "element", verb: "olmal\u0131d\u0131r" } };
    function i(n) {
      return r[n] ?? null;
    }
    let o = { regex: "input", email: "email address", url: "URL", emoji: "emoji", uuid: "UUID", uuidv4: "UUIDv4", uuidv6: "UUIDv6", nanoid: "nanoid", guid: "GUID", cuid: "cuid", cuid2: "cuid2", ulid: "ULID", xid: "XID", ksuid: "KSUID", datetime: "ISO datetime", date: "ISO date", time: "ISO time", duration: "ISO duration", ipv4: "IPv4 address", ipv6: "IPv6 address", cidrv4: "IPv4 range", cidrv6: "IPv6 range", base64: "base64-encoded string", base64url: "base64url-encoded string", json_string: "JSON string", e164: "E.164 number", jwt: "JWT", template_literal: "input" }, t = { nan: "NaN" };
    return (n) => {
      switch (n.code) {
        case "invalid_type": {
          let v = t[n.expected] ?? n.expected, u = k(n.input), $ = t[u] ?? u;
          if (/^[A-Z]/.test(n.expected))
            return `Yanl\u0131\u015F d\u0259y\u0259r: g\xF6zl\u0259nil\u0259n instanceof ${n.expected}, daxil olan ${$}`;
          return `Yanl\u0131\u015F d\u0259y\u0259r: g\xF6zl\u0259nil\u0259n ${v}, daxil olan ${$}`;
        }
        case "invalid_value":
          if (n.values.length === 1)
            return `Yanl\u0131\u015F d\u0259y\u0259r: g\xF6zl\u0259nil\u0259n ${U(n.values[0])}`;
          return `Yanl\u0131\u015F se\xE7im: a\u015Fa\u011F\u0131dak\u0131lardan biri olmal\u0131d\u0131r: ${b(n.values, "|")}`;
        case "too_big": {
          let v = n.inclusive ? "<=" : "<", u = i(n.origin);
          if (u)
            return `\xC7ox b\xF6y\xFCk: g\xF6zl\u0259nil\u0259n ${n.origin ?? "d\u0259y\u0259r"} ${v}${n.maximum.toString()} ${u.unit ?? "element"}`;
          return `\xC7ox b\xF6y\xFCk: g\xF6zl\u0259nil\u0259n ${n.origin ?? "d\u0259y\u0259r"} ${v}${n.maximum.toString()}`;
        }
        case "too_small": {
          let v = n.inclusive ? ">=" : ">", u = i(n.origin);
          if (u)
            return `\xC7ox ki\xE7ik: g\xF6zl\u0259nil\u0259n ${n.origin} ${v}${n.minimum.toString()} ${u.unit}`;
          return `\xC7ox ki\xE7ik: g\xF6zl\u0259nil\u0259n ${n.origin} ${v}${n.minimum.toString()}`;
        }
        case "invalid_format": {
          let v = n;
          if (v.format === "starts_with")
            return `Yanl\u0131\u015F m\u0259tn: "${v.prefix}" il\u0259 ba\u015Flamal\u0131d\u0131r`;
          if (v.format === "ends_with")
            return `Yanl\u0131\u015F m\u0259tn: "${v.suffix}" il\u0259 bitm\u0259lidir`;
          if (v.format === "includes")
            return `Yanl\u0131\u015F m\u0259tn: "${v.includes}" daxil olmal\u0131d\u0131r`;
          if (v.format === "regex")
            return `Yanl\u0131\u015F m\u0259tn: ${v.pattern} \u015Fablonuna uy\u011Fun olmal\u0131d\u0131r`;
          return `Yanl\u0131\u015F ${o[v.format] ?? n.format}`;
        }
        case "not_multiple_of":
          return `Yanl\u0131\u015F \u0259d\u0259d: ${n.divisor} il\u0259 b\xF6l\xFCn\u0259 bil\u0259n olmal\u0131d\u0131r`;
        case "unrecognized_keys":
          return `Tan\u0131nmayan a\xE7ar${n.keys.length > 1 ? "lar" : ""}: ${b(n.keys, ", ")}`;
        case "invalid_key":
          return `${n.origin} daxilind\u0259 yanl\u0131\u015F a\xE7ar`;
        case "invalid_union":
          return "Yanl\u0131\u015F d\u0259y\u0259r";
        case "invalid_element":
          return `${n.origin} daxilind\u0259 yanl\u0131\u015F d\u0259y\u0259r`;
        default:
          return "Yanl\u0131\u015F d\u0259y\u0259r";
      }
    };
  };
  function Ft() {
    return { localeError: o4() };
  }
  function pe(r, i, o, t) {
    let n = Math.abs(r), v = n % 10, u = n % 100;
    if (u >= 11 && u <= 19)
      return t;
    if (v === 1)
      return i;
    if (v >= 2 && v <= 4)
      return o;
    return t;
  }
  var t4 = () => {
    let r = { string: { unit: { one: "\u0441\u0456\u043C\u0432\u0430\u043B", few: "\u0441\u0456\u043C\u0432\u0430\u043B\u044B", many: "\u0441\u0456\u043C\u0432\u0430\u043B\u0430\u045E" }, verb: "\u043C\u0435\u0446\u044C" }, array: { unit: { one: "\u044D\u043B\u0435\u043C\u0435\u043D\u0442", few: "\u044D\u043B\u0435\u043C\u0435\u043D\u0442\u044B", many: "\u044D\u043B\u0435\u043C\u0435\u043D\u0442\u0430\u045E" }, verb: "\u043C\u0435\u0446\u044C" }, set: { unit: { one: "\u044D\u043B\u0435\u043C\u0435\u043D\u0442", few: "\u044D\u043B\u0435\u043C\u0435\u043D\u0442\u044B", many: "\u044D\u043B\u0435\u043C\u0435\u043D\u0442\u0430\u045E" }, verb: "\u043C\u0435\u0446\u044C" }, file: { unit: { one: "\u0431\u0430\u0439\u0442", few: "\u0431\u0430\u0439\u0442\u044B", many: "\u0431\u0430\u0439\u0442\u0430\u045E" }, verb: "\u043C\u0435\u0446\u044C" } };
    function i(n) {
      return r[n] ?? null;
    }
    let o = { regex: "\u0443\u0432\u043E\u0434", email: "email \u0430\u0434\u0440\u0430\u0441", url: "URL", emoji: "\u044D\u043C\u043E\u0434\u0437\u0456", uuid: "UUID", uuidv4: "UUIDv4", uuidv6: "UUIDv6", nanoid: "nanoid", guid: "GUID", cuid: "cuid", cuid2: "cuid2", ulid: "ULID", xid: "XID", ksuid: "KSUID", datetime: "ISO \u0434\u0430\u0442\u0430 \u0456 \u0447\u0430\u0441", date: "ISO \u0434\u0430\u0442\u0430", time: "ISO \u0447\u0430\u0441", duration: "ISO \u043F\u0440\u0430\u0446\u044F\u0433\u043B\u0430\u0441\u0446\u044C", ipv4: "IPv4 \u0430\u0434\u0440\u0430\u0441", ipv6: "IPv6 \u0430\u0434\u0440\u0430\u0441", cidrv4: "IPv4 \u0434\u044B\u044F\u043F\u0430\u0437\u043E\u043D", cidrv6: "IPv6 \u0434\u044B\u044F\u043F\u0430\u0437\u043E\u043D", base64: "\u0440\u0430\u0434\u043E\u043A \u0443 \u0444\u0430\u0440\u043C\u0430\u0446\u0435 base64", base64url: "\u0440\u0430\u0434\u043E\u043A \u0443 \u0444\u0430\u0440\u043C\u0430\u0446\u0435 base64url", json_string: "JSON \u0440\u0430\u0434\u043E\u043A", e164: "\u043D\u0443\u043C\u0430\u0440 E.164", jwt: "JWT", template_literal: "\u0443\u0432\u043E\u0434" }, t = { nan: "NaN", number: "\u043B\u0456\u043A", array: "\u043C\u0430\u0441\u0456\u045E" };
    return (n) => {
      switch (n.code) {
        case "invalid_type": {
          let v = t[n.expected] ?? n.expected, u = k(n.input), $ = t[u] ?? u;
          if (/^[A-Z]/.test(n.expected))
            return `\u041D\u044F\u043F\u0440\u0430\u0432\u0456\u043B\u044C\u043D\u044B \u045E\u0432\u043E\u0434: \u0447\u0430\u043A\u0430\u045E\u0441\u044F instanceof ${n.expected}, \u0430\u0442\u0440\u044B\u043C\u0430\u043D\u0430 ${$}`;
          return `\u041D\u044F\u043F\u0440\u0430\u0432\u0456\u043B\u044C\u043D\u044B \u045E\u0432\u043E\u0434: \u0447\u0430\u043A\u0430\u045E\u0441\u044F ${v}, \u0430\u0442\u0440\u044B\u043C\u0430\u043D\u0430 ${$}`;
        }
        case "invalid_value":
          if (n.values.length === 1)
            return `\u041D\u044F\u043F\u0440\u0430\u0432\u0456\u043B\u044C\u043D\u044B \u045E\u0432\u043E\u0434: \u0447\u0430\u043A\u0430\u043B\u0430\u0441\u044F ${U(n.values[0])}`;
          return `\u041D\u044F\u043F\u0440\u0430\u0432\u0456\u043B\u044C\u043D\u044B \u0432\u0430\u0440\u044B\u044F\u043D\u0442: \u0447\u0430\u043A\u0430\u045E\u0441\u044F \u0430\u0434\u0437\u0456\u043D \u0437 ${b(n.values, "|")}`;
        case "too_big": {
          let v = n.inclusive ? "<=" : "<", u = i(n.origin);
          if (u) {
            let $ = Number(n.maximum), l = pe($, u.unit.one, u.unit.few, u.unit.many);
            return `\u0417\u0430\u043D\u0430\u0434\u0442\u0430 \u0432\u044F\u043B\u0456\u043A\u0456: \u0447\u0430\u043A\u0430\u043B\u0430\u0441\u044F, \u0448\u0442\u043E ${n.origin ?? "\u0437\u043D\u0430\u0447\u044D\u043D\u043D\u0435"} \u043F\u0430\u0432\u0456\u043D\u043D\u0430 ${u.verb} ${v}${n.maximum.toString()} ${l}`;
          }
          return `\u0417\u0430\u043D\u0430\u0434\u0442\u0430 \u0432\u044F\u043B\u0456\u043A\u0456: \u0447\u0430\u043A\u0430\u043B\u0430\u0441\u044F, \u0448\u0442\u043E ${n.origin ?? "\u0437\u043D\u0430\u0447\u044D\u043D\u043D\u0435"} \u043F\u0430\u0432\u0456\u043D\u043D\u0430 \u0431\u044B\u0446\u044C ${v}${n.maximum.toString()}`;
        }
        case "too_small": {
          let v = n.inclusive ? ">=" : ">", u = i(n.origin);
          if (u) {
            let $ = Number(n.minimum), l = pe($, u.unit.one, u.unit.few, u.unit.many);
            return `\u0417\u0430\u043D\u0430\u0434\u0442\u0430 \u043C\u0430\u043B\u044B: \u0447\u0430\u043A\u0430\u043B\u0430\u0441\u044F, \u0448\u0442\u043E ${n.origin} \u043F\u0430\u0432\u0456\u043D\u043D\u0430 ${u.verb} ${v}${n.minimum.toString()} ${l}`;
          }
          return `\u0417\u0430\u043D\u0430\u0434\u0442\u0430 \u043C\u0430\u043B\u044B: \u0447\u0430\u043A\u0430\u043B\u0430\u0441\u044F, \u0448\u0442\u043E ${n.origin} \u043F\u0430\u0432\u0456\u043D\u043D\u0430 \u0431\u044B\u0446\u044C ${v}${n.minimum.toString()}`;
        }
        case "invalid_format": {
          let v = n;
          if (v.format === "starts_with")
            return `\u041D\u044F\u043F\u0440\u0430\u0432\u0456\u043B\u044C\u043D\u044B \u0440\u0430\u0434\u043E\u043A: \u043F\u0430\u0432\u0456\u043D\u0435\u043D \u043F\u0430\u0447\u044B\u043D\u0430\u0446\u0446\u0430 \u0437 "${v.prefix}"`;
          if (v.format === "ends_with")
            return `\u041D\u044F\u043F\u0440\u0430\u0432\u0456\u043B\u044C\u043D\u044B \u0440\u0430\u0434\u043E\u043A: \u043F\u0430\u0432\u0456\u043D\u0435\u043D \u0437\u0430\u043A\u0430\u043D\u0447\u0432\u0430\u0446\u0446\u0430 \u043D\u0430 "${v.suffix}"`;
          if (v.format === "includes")
            return `\u041D\u044F\u043F\u0440\u0430\u0432\u0456\u043B\u044C\u043D\u044B \u0440\u0430\u0434\u043E\u043A: \u043F\u0430\u0432\u0456\u043D\u0435\u043D \u0437\u043C\u044F\u0448\u0447\u0430\u0446\u044C "${v.includes}"`;
          if (v.format === "regex")
            return `\u041D\u044F\u043F\u0440\u0430\u0432\u0456\u043B\u044C\u043D\u044B \u0440\u0430\u0434\u043E\u043A: \u043F\u0430\u0432\u0456\u043D\u0435\u043D \u0430\u0434\u043F\u0430\u0432\u044F\u0434\u0430\u0446\u044C \u0448\u0430\u0431\u043B\u043E\u043D\u0443 ${v.pattern}`;
          return `\u041D\u044F\u043F\u0440\u0430\u0432\u0456\u043B\u044C\u043D\u044B ${o[v.format] ?? n.format}`;
        }
        case "not_multiple_of":
          return `\u041D\u044F\u043F\u0440\u0430\u0432\u0456\u043B\u044C\u043D\u044B \u043B\u0456\u043A: \u043F\u0430\u0432\u0456\u043D\u0435\u043D \u0431\u044B\u0446\u044C \u043A\u0440\u0430\u0442\u043D\u044B\u043C ${n.divisor}`;
        case "unrecognized_keys":
          return `\u041D\u0435\u0440\u0430\u0441\u043F\u0430\u0437\u043D\u0430\u043D\u044B ${n.keys.length > 1 ? "\u043A\u043B\u044E\u0447\u044B" : "\u043A\u043B\u044E\u0447"}: ${b(n.keys, ", ")}`;
        case "invalid_key":
          return `\u041D\u044F\u043F\u0440\u0430\u0432\u0456\u043B\u044C\u043D\u044B \u043A\u043B\u044E\u0447 \u0443 ${n.origin}`;
        case "invalid_union":
          return "\u041D\u044F\u043F\u0440\u0430\u0432\u0456\u043B\u044C\u043D\u044B \u045E\u0432\u043E\u0434";
        case "invalid_element":
          return `\u041D\u044F\u043F\u0440\u0430\u0432\u0456\u043B\u044C\u043D\u0430\u0435 \u0437\u043D\u0430\u0447\u044D\u043D\u043D\u0435 \u045E ${n.origin}`;
        default:
          return "\u041D\u044F\u043F\u0440\u0430\u0432\u0456\u043B\u044C\u043D\u044B \u045E\u0432\u043E\u0434";
      }
    };
  };
  function Bt() {
    return { localeError: t4() };
  }
  var u4 = () => {
    let r = { string: { unit: "\u0441\u0438\u043C\u0432\u043E\u043B\u0430", verb: "\u0434\u0430 \u0441\u044A\u0434\u044A\u0440\u0436\u0430" }, file: { unit: "\u0431\u0430\u0439\u0442\u0430", verb: "\u0434\u0430 \u0441\u044A\u0434\u044A\u0440\u0436\u0430" }, array: { unit: "\u0435\u043B\u0435\u043C\u0435\u043D\u0442\u0430", verb: "\u0434\u0430 \u0441\u044A\u0434\u044A\u0440\u0436\u0430" }, set: { unit: "\u0435\u043B\u0435\u043C\u0435\u043D\u0442\u0430", verb: "\u0434\u0430 \u0441\u044A\u0434\u044A\u0440\u0436\u0430" } };
    function i(n) {
      return r[n] ?? null;
    }
    let o = { regex: "\u0432\u0445\u043E\u0434", email: "\u0438\u043C\u0435\u0439\u043B \u0430\u0434\u0440\u0435\u0441", url: "URL", emoji: "\u0435\u043C\u043E\u0434\u0436\u0438", uuid: "UUID", uuidv4: "UUIDv4", uuidv6: "UUIDv6", nanoid: "nanoid", guid: "GUID", cuid: "cuid", cuid2: "cuid2", ulid: "ULID", xid: "XID", ksuid: "KSUID", datetime: "ISO \u0432\u0440\u0435\u043C\u0435", date: "ISO \u0434\u0430\u0442\u0430", time: "ISO \u0432\u0440\u0435\u043C\u0435", duration: "ISO \u043F\u0440\u043E\u0434\u044A\u043B\u0436\u0438\u0442\u0435\u043B\u043D\u043E\u0441\u0442", ipv4: "IPv4 \u0430\u0434\u0440\u0435\u0441", ipv6: "IPv6 \u0430\u0434\u0440\u0435\u0441", cidrv4: "IPv4 \u0434\u0438\u0430\u043F\u0430\u0437\u043E\u043D", cidrv6: "IPv6 \u0434\u0438\u0430\u043F\u0430\u0437\u043E\u043D", base64: "base64-\u043A\u043E\u0434\u0438\u0440\u0430\u043D \u043D\u0438\u0437", base64url: "base64url-\u043A\u043E\u0434\u0438\u0440\u0430\u043D \u043D\u0438\u0437", json_string: "JSON \u043D\u0438\u0437", e164: "E.164 \u043D\u043E\u043C\u0435\u0440", jwt: "JWT", template_literal: "\u0432\u0445\u043E\u0434" }, t = { nan: "NaN", number: "\u0447\u0438\u0441\u043B\u043E", array: "\u043C\u0430\u0441\u0438\u0432" };
    return (n) => {
      switch (n.code) {
        case "invalid_type": {
          let v = t[n.expected] ?? n.expected, u = k(n.input), $ = t[u] ?? u;
          if (/^[A-Z]/.test(n.expected))
            return `\u041D\u0435\u0432\u0430\u043B\u0438\u0434\u0435\u043D \u0432\u0445\u043E\u0434: \u043E\u0447\u0430\u043A\u0432\u0430\u043D instanceof ${n.expected}, \u043F\u043E\u043B\u0443\u0447\u0435\u043D ${$}`;
          return `\u041D\u0435\u0432\u0430\u043B\u0438\u0434\u0435\u043D \u0432\u0445\u043E\u0434: \u043E\u0447\u0430\u043A\u0432\u0430\u043D ${v}, \u043F\u043E\u043B\u0443\u0447\u0435\u043D ${$}`;
        }
        case "invalid_value":
          if (n.values.length === 1)
            return `\u041D\u0435\u0432\u0430\u043B\u0438\u0434\u0435\u043D \u0432\u0445\u043E\u0434: \u043E\u0447\u0430\u043A\u0432\u0430\u043D ${U(n.values[0])}`;
          return `\u041D\u0435\u0432\u0430\u043B\u0438\u0434\u043D\u0430 \u043E\u043F\u0446\u0438\u044F: \u043E\u0447\u0430\u043A\u0432\u0430\u043D\u043E \u0435\u0434\u043D\u043E \u043E\u0442 ${b(n.values, "|")}`;
        case "too_big": {
          let v = n.inclusive ? "<=" : "<", u = i(n.origin);
          if (u)
            return `\u0422\u0432\u044A\u0440\u0434\u0435 \u0433\u043E\u043B\u044F\u043C\u043E: \u043E\u0447\u0430\u043A\u0432\u0430 \u0441\u0435 ${n.origin ?? "\u0441\u0442\u043E\u0439\u043D\u043E\u0441\u0442"} \u0434\u0430 \u0441\u044A\u0434\u044A\u0440\u0436\u0430 ${v}${n.maximum.toString()} ${u.unit ?? "\u0435\u043B\u0435\u043C\u0435\u043D\u0442\u0430"}`;
          return `\u0422\u0432\u044A\u0440\u0434\u0435 \u0433\u043E\u043B\u044F\u043C\u043E: \u043E\u0447\u0430\u043A\u0432\u0430 \u0441\u0435 ${n.origin ?? "\u0441\u0442\u043E\u0439\u043D\u043E\u0441\u0442"} \u0434\u0430 \u0431\u044A\u0434\u0435 ${v}${n.maximum.toString()}`;
        }
        case "too_small": {
          let v = n.inclusive ? ">=" : ">", u = i(n.origin);
          if (u)
            return `\u0422\u0432\u044A\u0440\u0434\u0435 \u043C\u0430\u043B\u043A\u043E: \u043E\u0447\u0430\u043A\u0432\u0430 \u0441\u0435 ${n.origin} \u0434\u0430 \u0441\u044A\u0434\u044A\u0440\u0436\u0430 ${v}${n.minimum.toString()} ${u.unit}`;
          return `\u0422\u0432\u044A\u0440\u0434\u0435 \u043C\u0430\u043B\u043A\u043E: \u043E\u0447\u0430\u043A\u0432\u0430 \u0441\u0435 ${n.origin} \u0434\u0430 \u0431\u044A\u0434\u0435 ${v}${n.minimum.toString()}`;
        }
        case "invalid_format": {
          let v = n;
          if (v.format === "starts_with")
            return `\u041D\u0435\u0432\u0430\u043B\u0438\u0434\u0435\u043D \u043D\u0438\u0437: \u0442\u0440\u044F\u0431\u0432\u0430 \u0434\u0430 \u0437\u0430\u043F\u043E\u0447\u0432\u0430 \u0441 "${v.prefix}"`;
          if (v.format === "ends_with")
            return `\u041D\u0435\u0432\u0430\u043B\u0438\u0434\u0435\u043D \u043D\u0438\u0437: \u0442\u0440\u044F\u0431\u0432\u0430 \u0434\u0430 \u0437\u0430\u0432\u044A\u0440\u0448\u0432\u0430 \u0441 "${v.suffix}"`;
          if (v.format === "includes")
            return `\u041D\u0435\u0432\u0430\u043B\u0438\u0434\u0435\u043D \u043D\u0438\u0437: \u0442\u0440\u044F\u0431\u0432\u0430 \u0434\u0430 \u0432\u043A\u043B\u044E\u0447\u0432\u0430 "${v.includes}"`;
          if (v.format === "regex")
            return `\u041D\u0435\u0432\u0430\u043B\u0438\u0434\u0435\u043D \u043D\u0438\u0437: \u0442\u0440\u044F\u0431\u0432\u0430 \u0434\u0430 \u0441\u044A\u0432\u043F\u0430\u0434\u0430 \u0441 ${v.pattern}`;
          let u = "\u041D\u0435\u0432\u0430\u043B\u0438\u0434\u0435\u043D";
          if (v.format === "emoji")
            u = "\u041D\u0435\u0432\u0430\u043B\u0438\u0434\u043D\u043E";
          if (v.format === "datetime")
            u = "\u041D\u0435\u0432\u0430\u043B\u0438\u0434\u043D\u043E";
          if (v.format === "date")
            u = "\u041D\u0435\u0432\u0430\u043B\u0438\u0434\u043D\u0430";
          if (v.format === "time")
            u = "\u041D\u0435\u0432\u0430\u043B\u0438\u0434\u043D\u043E";
          if (v.format === "duration")
            u = "\u041D\u0435\u0432\u0430\u043B\u0438\u0434\u043D\u0430";
          return `${u} ${o[v.format] ?? n.format}`;
        }
        case "not_multiple_of":
          return `\u041D\u0435\u0432\u0430\u043B\u0438\u0434\u043D\u043E \u0447\u0438\u0441\u043B\u043E: \u0442\u0440\u044F\u0431\u0432\u0430 \u0434\u0430 \u0431\u044A\u0434\u0435 \u043A\u0440\u0430\u0442\u043D\u043E \u043D\u0430 ${n.divisor}`;
        case "unrecognized_keys":
          return `\u041D\u0435\u0440\u0430\u0437\u043F\u043E\u0437\u043D\u0430\u0442${n.keys.length > 1 ? "\u0438" : ""} \u043A\u043B\u044E\u0447${n.keys.length > 1 ? "\u043E\u0432\u0435" : ""}: ${b(n.keys, ", ")}`;
        case "invalid_key":
          return `\u041D\u0435\u0432\u0430\u043B\u0438\u0434\u0435\u043D \u043A\u043B\u044E\u0447 \u0432 ${n.origin}`;
        case "invalid_union":
          return "\u041D\u0435\u0432\u0430\u043B\u0438\u0434\u0435\u043D \u0432\u0445\u043E\u0434";
        case "invalid_element":
          return `\u041D\u0435\u0432\u0430\u043B\u0438\u0434\u043D\u0430 \u0441\u0442\u043E\u0439\u043D\u043E\u0441\u0442 \u0432 ${n.origin}`;
        default:
          return "\u041D\u0435\u0432\u0430\u043B\u0438\u0434\u0435\u043D \u0432\u0445\u043E\u0434";
      }
    };
  };
  function Ht() {
    return { localeError: u4() };
  }
  var $4 = () => {
    let r = { string: { unit: "car\xE0cters", verb: "contenir" }, file: { unit: "bytes", verb: "contenir" }, array: { unit: "elements", verb: "contenir" }, set: { unit: "elements", verb: "contenir" } };
    function i(n) {
      return r[n] ?? null;
    }
    let o = { regex: "entrada", email: "adre\xE7a electr\xF2nica", url: "URL", emoji: "emoji", uuid: "UUID", uuidv4: "UUIDv4", uuidv6: "UUIDv6", nanoid: "nanoid", guid: "GUID", cuid: "cuid", cuid2: "cuid2", ulid: "ULID", xid: "XID", ksuid: "KSUID", datetime: "data i hora ISO", date: "data ISO", time: "hora ISO", duration: "durada ISO", ipv4: "adre\xE7a IPv4", ipv6: "adre\xE7a IPv6", cidrv4: "rang IPv4", cidrv6: "rang IPv6", base64: "cadena codificada en base64", base64url: "cadena codificada en base64url", json_string: "cadena JSON", e164: "n\xFAmero E.164", jwt: "JWT", template_literal: "entrada" }, t = { nan: "NaN" };
    return (n) => {
      switch (n.code) {
        case "invalid_type": {
          let v = t[n.expected] ?? n.expected, u = k(n.input), $ = t[u] ?? u;
          if (/^[A-Z]/.test(n.expected))
            return `Tipus inv\xE0lid: s'esperava instanceof ${n.expected}, s'ha rebut ${$}`;
          return `Tipus inv\xE0lid: s'esperava ${v}, s'ha rebut ${$}`;
        }
        case "invalid_value":
          if (n.values.length === 1)
            return `Valor inv\xE0lid: s'esperava ${U(n.values[0])}`;
          return `Opci\xF3 inv\xE0lida: s'esperava una de ${b(n.values, " o ")}`;
        case "too_big": {
          let v = n.inclusive ? "com a m\xE0xim" : "menys de", u = i(n.origin);
          if (u)
            return `Massa gran: s'esperava que ${n.origin ?? "el valor"} contingu\xE9s ${v} ${n.maximum.toString()} ${u.unit ?? "elements"}`;
          return `Massa gran: s'esperava que ${n.origin ?? "el valor"} fos ${v} ${n.maximum.toString()}`;
        }
        case "too_small": {
          let v = n.inclusive ? "com a m\xEDnim" : "m\xE9s de", u = i(n.origin);
          if (u)
            return `Massa petit: s'esperava que ${n.origin} contingu\xE9s ${v} ${n.minimum.toString()} ${u.unit}`;
          return `Massa petit: s'esperava que ${n.origin} fos ${v} ${n.minimum.toString()}`;
        }
        case "invalid_format": {
          let v = n;
          if (v.format === "starts_with")
            return `Format inv\xE0lid: ha de comen\xE7ar amb "${v.prefix}"`;
          if (v.format === "ends_with")
            return `Format inv\xE0lid: ha d'acabar amb "${v.suffix}"`;
          if (v.format === "includes")
            return `Format inv\xE0lid: ha d'incloure "${v.includes}"`;
          if (v.format === "regex")
            return `Format inv\xE0lid: ha de coincidir amb el patr\xF3 ${v.pattern}`;
          return `Format inv\xE0lid per a ${o[v.format] ?? n.format}`;
        }
        case "not_multiple_of":
          return `N\xFAmero inv\xE0lid: ha de ser m\xFAltiple de ${n.divisor}`;
        case "unrecognized_keys":
          return `Clau${n.keys.length > 1 ? "s" : ""} no reconeguda${n.keys.length > 1 ? "s" : ""}: ${b(n.keys, ", ")}`;
        case "invalid_key":
          return `Clau inv\xE0lida a ${n.origin}`;
        case "invalid_union":
          return "Entrada inv\xE0lida";
        case "invalid_element":
          return `Element inv\xE0lid a ${n.origin}`;
        default:
          return "Entrada inv\xE0lida";
      }
    };
  };
  function Mt() {
    return { localeError: $4() };
  }
  var g4 = () => {
    let r = { string: { unit: "znak\u016F", verb: "m\xEDt" }, file: { unit: "bajt\u016F", verb: "m\xEDt" }, array: { unit: "prvk\u016F", verb: "m\xEDt" }, set: { unit: "prvk\u016F", verb: "m\xEDt" } };
    function i(n) {
      return r[n] ?? null;
    }
    let o = { regex: "regul\xE1rn\xED v\xFDraz", email: "e-mailov\xE1 adresa", url: "URL", emoji: "emoji", uuid: "UUID", uuidv4: "UUIDv4", uuidv6: "UUIDv6", nanoid: "nanoid", guid: "GUID", cuid: "cuid", cuid2: "cuid2", ulid: "ULID", xid: "XID", ksuid: "KSUID", datetime: "datum a \u010Das ve form\xE1tu ISO", date: "datum ve form\xE1tu ISO", time: "\u010Das ve form\xE1tu ISO", duration: "doba trv\xE1n\xED ISO", ipv4: "IPv4 adresa", ipv6: "IPv6 adresa", cidrv4: "rozsah IPv4", cidrv6: "rozsah IPv6", base64: "\u0159et\u011Bzec zak\xF3dovan\xFD ve form\xE1tu base64", base64url: "\u0159et\u011Bzec zak\xF3dovan\xFD ve form\xE1tu base64url", json_string: "\u0159et\u011Bzec ve form\xE1tu JSON", e164: "\u010D\xEDslo E.164", jwt: "JWT", template_literal: "vstup" }, t = { nan: "NaN", number: "\u010D\xEDslo", string: "\u0159et\u011Bzec", function: "funkce", array: "pole" };
    return (n) => {
      switch (n.code) {
        case "invalid_type": {
          let v = t[n.expected] ?? n.expected, u = k(n.input), $ = t[u] ?? u;
          if (/^[A-Z]/.test(n.expected))
            return `Neplatn\xFD vstup: o\u010Dek\xE1v\xE1no instanceof ${n.expected}, obdr\u017Eeno ${$}`;
          return `Neplatn\xFD vstup: o\u010Dek\xE1v\xE1no ${v}, obdr\u017Eeno ${$}`;
        }
        case "invalid_value":
          if (n.values.length === 1)
            return `Neplatn\xFD vstup: o\u010Dek\xE1v\xE1no ${U(n.values[0])}`;
          return `Neplatn\xE1 mo\u017Enost: o\u010Dek\xE1v\xE1na jedna z hodnot ${b(n.values, "|")}`;
        case "too_big": {
          let v = n.inclusive ? "<=" : "<", u = i(n.origin);
          if (u)
            return `Hodnota je p\u0159\xEDli\u0161 velk\xE1: ${n.origin ?? "hodnota"} mus\xED m\xEDt ${v}${n.maximum.toString()} ${u.unit ?? "prvk\u016F"}`;
          return `Hodnota je p\u0159\xEDli\u0161 velk\xE1: ${n.origin ?? "hodnota"} mus\xED b\xFDt ${v}${n.maximum.toString()}`;
        }
        case "too_small": {
          let v = n.inclusive ? ">=" : ">", u = i(n.origin);
          if (u)
            return `Hodnota je p\u0159\xEDli\u0161 mal\xE1: ${n.origin ?? "hodnota"} mus\xED m\xEDt ${v}${n.minimum.toString()} ${u.unit ?? "prvk\u016F"}`;
          return `Hodnota je p\u0159\xEDli\u0161 mal\xE1: ${n.origin ?? "hodnota"} mus\xED b\xFDt ${v}${n.minimum.toString()}`;
        }
        case "invalid_format": {
          let v = n;
          if (v.format === "starts_with")
            return `Neplatn\xFD \u0159et\u011Bzec: mus\xED za\u010D\xEDnat na "${v.prefix}"`;
          if (v.format === "ends_with")
            return `Neplatn\xFD \u0159et\u011Bzec: mus\xED kon\u010Dit na "${v.suffix}"`;
          if (v.format === "includes")
            return `Neplatn\xFD \u0159et\u011Bzec: mus\xED obsahovat "${v.includes}"`;
          if (v.format === "regex")
            return `Neplatn\xFD \u0159et\u011Bzec: mus\xED odpov\xEDdat vzoru ${v.pattern}`;
          return `Neplatn\xFD form\xE1t ${o[v.format] ?? n.format}`;
        }
        case "not_multiple_of":
          return `Neplatn\xE9 \u010D\xEDslo: mus\xED b\xFDt n\xE1sobkem ${n.divisor}`;
        case "unrecognized_keys":
          return `Nezn\xE1m\xE9 kl\xED\u010De: ${b(n.keys, ", ")}`;
        case "invalid_key":
          return `Neplatn\xFD kl\xED\u010D v ${n.origin}`;
        case "invalid_union":
          return "Neplatn\xFD vstup";
        case "invalid_element":
          return `Neplatn\xE1 hodnota v ${n.origin}`;
        default:
          return "Neplatn\xFD vstup";
      }
    };
  };
  function Rt() {
    return { localeError: g4() };
  }
  var e4 = () => {
    let r = { string: { unit: "tegn", verb: "havde" }, file: { unit: "bytes", verb: "havde" }, array: { unit: "elementer", verb: "indeholdt" }, set: { unit: "elementer", verb: "indeholdt" } };
    function i(n) {
      return r[n] ?? null;
    }
    let o = { regex: "input", email: "e-mailadresse", url: "URL", emoji: "emoji", uuid: "UUID", uuidv4: "UUIDv4", uuidv6: "UUIDv6", nanoid: "nanoid", guid: "GUID", cuid: "cuid", cuid2: "cuid2", ulid: "ULID", xid: "XID", ksuid: "KSUID", datetime: "ISO dato- og klokkesl\xE6t", date: "ISO-dato", time: "ISO-klokkesl\xE6t", duration: "ISO-varighed", ipv4: "IPv4-omr\xE5de", ipv6: "IPv6-omr\xE5de", cidrv4: "IPv4-spektrum", cidrv6: "IPv6-spektrum", base64: "base64-kodet streng", base64url: "base64url-kodet streng", json_string: "JSON-streng", e164: "E.164-nummer", jwt: "JWT", template_literal: "input" }, t = { nan: "NaN", string: "streng", number: "tal", boolean: "boolean", array: "liste", object: "objekt", set: "s\xE6t", file: "fil" };
    return (n) => {
      switch (n.code) {
        case "invalid_type": {
          let v = t[n.expected] ?? n.expected, u = k(n.input), $ = t[u] ?? u;
          if (/^[A-Z]/.test(n.expected))
            return `Ugyldigt input: forventede instanceof ${n.expected}, fik ${$}`;
          return `Ugyldigt input: forventede ${v}, fik ${$}`;
        }
        case "invalid_value":
          if (n.values.length === 1)
            return `Ugyldig v\xE6rdi: forventede ${U(n.values[0])}`;
          return `Ugyldigt valg: forventede en af f\xF8lgende ${b(n.values, "|")}`;
        case "too_big": {
          let v = n.inclusive ? "<=" : "<", u = i(n.origin), $ = t[n.origin] ?? n.origin;
          if (u)
            return `For stor: forventede ${$ ?? "value"} ${u.verb} ${v} ${n.maximum.toString()} ${u.unit ?? "elementer"}`;
          return `For stor: forventede ${$ ?? "value"} havde ${v} ${n.maximum.toString()}`;
        }
        case "too_small": {
          let v = n.inclusive ? ">=" : ">", u = i(n.origin), $ = t[n.origin] ?? n.origin;
          if (u)
            return `For lille: forventede ${$} ${u.verb} ${v} ${n.minimum.toString()} ${u.unit}`;
          return `For lille: forventede ${$} havde ${v} ${n.minimum.toString()}`;
        }
        case "invalid_format": {
          let v = n;
          if (v.format === "starts_with")
            return `Ugyldig streng: skal starte med "${v.prefix}"`;
          if (v.format === "ends_with")
            return `Ugyldig streng: skal ende med "${v.suffix}"`;
          if (v.format === "includes")
            return `Ugyldig streng: skal indeholde "${v.includes}"`;
          if (v.format === "regex")
            return `Ugyldig streng: skal matche m\xF8nsteret ${v.pattern}`;
          return `Ugyldig ${o[v.format] ?? n.format}`;
        }
        case "not_multiple_of":
          return `Ugyldigt tal: skal v\xE6re deleligt med ${n.divisor}`;
        case "unrecognized_keys":
          return `${n.keys.length > 1 ? "Ukendte n\xF8gler" : "Ukendt n\xF8gle"}: ${b(n.keys, ", ")}`;
        case "invalid_key":
          return `Ugyldig n\xF8gle i ${n.origin}`;
        case "invalid_union":
          return "Ugyldigt input: matcher ingen af de tilladte typer";
        case "invalid_element":
          return `Ugyldig v\xE6rdi i ${n.origin}`;
        default:
          return "Ugyldigt input";
      }
    };
  };
  function xt() {
    return { localeError: e4() };
  }
  var l4 = () => {
    let r = { string: { unit: "Zeichen", verb: "zu haben" }, file: { unit: "Bytes", verb: "zu haben" }, array: { unit: "Elemente", verb: "zu haben" }, set: { unit: "Elemente", verb: "zu haben" } };
    function i(n) {
      return r[n] ?? null;
    }
    let o = { regex: "Eingabe", email: "E-Mail-Adresse", url: "URL", emoji: "Emoji", uuid: "UUID", uuidv4: "UUIDv4", uuidv6: "UUIDv6", nanoid: "nanoid", guid: "GUID", cuid: "cuid", cuid2: "cuid2", ulid: "ULID", xid: "XID", ksuid: "KSUID", datetime: "ISO-Datum und -Uhrzeit", date: "ISO-Datum", time: "ISO-Uhrzeit", duration: "ISO-Dauer", ipv4: "IPv4-Adresse", ipv6: "IPv6-Adresse", cidrv4: "IPv4-Bereich", cidrv6: "IPv6-Bereich", base64: "Base64-codierter String", base64url: "Base64-URL-codierter String", json_string: "JSON-String", e164: "E.164-Nummer", jwt: "JWT", template_literal: "Eingabe" }, t = { nan: "NaN", number: "Zahl", array: "Array" };
    return (n) => {
      switch (n.code) {
        case "invalid_type": {
          let v = t[n.expected] ?? n.expected, u = k(n.input), $ = t[u] ?? u;
          if (/^[A-Z]/.test(n.expected))
            return `Ung\xFCltige Eingabe: erwartet instanceof ${n.expected}, erhalten ${$}`;
          return `Ung\xFCltige Eingabe: erwartet ${v}, erhalten ${$}`;
        }
        case "invalid_value":
          if (n.values.length === 1)
            return `Ung\xFCltige Eingabe: erwartet ${U(n.values[0])}`;
          return `Ung\xFCltige Option: erwartet eine von ${b(n.values, "|")}`;
        case "too_big": {
          let v = n.inclusive ? "<=" : "<", u = i(n.origin);
          if (u)
            return `Zu gro\xDF: erwartet, dass ${n.origin ?? "Wert"} ${v}${n.maximum.toString()} ${u.unit ?? "Elemente"} hat`;
          return `Zu gro\xDF: erwartet, dass ${n.origin ?? "Wert"} ${v}${n.maximum.toString()} ist`;
        }
        case "too_small": {
          let v = n.inclusive ? ">=" : ">", u = i(n.origin);
          if (u)
            return `Zu klein: erwartet, dass ${n.origin} ${v}${n.minimum.toString()} ${u.unit} hat`;
          return `Zu klein: erwartet, dass ${n.origin} ${v}${n.minimum.toString()} ist`;
        }
        case "invalid_format": {
          let v = n;
          if (v.format === "starts_with")
            return `Ung\xFCltiger String: muss mit "${v.prefix}" beginnen`;
          if (v.format === "ends_with")
            return `Ung\xFCltiger String: muss mit "${v.suffix}" enden`;
          if (v.format === "includes")
            return `Ung\xFCltiger String: muss "${v.includes}" enthalten`;
          if (v.format === "regex")
            return `Ung\xFCltiger String: muss dem Muster ${v.pattern} entsprechen`;
          return `Ung\xFCltig: ${o[v.format] ?? n.format}`;
        }
        case "not_multiple_of":
          return `Ung\xFCltige Zahl: muss ein Vielfaches von ${n.divisor} sein`;
        case "unrecognized_keys":
          return `${n.keys.length > 1 ? "Unbekannte Schl\xFCssel" : "Unbekannter Schl\xFCssel"}: ${b(n.keys, ", ")}`;
        case "invalid_key":
          return `Ung\xFCltiger Schl\xFCssel in ${n.origin}`;
        case "invalid_union":
          return "Ung\xFCltige Eingabe";
        case "invalid_element":
          return `Ung\xFCltiger Wert in ${n.origin}`;
        default:
          return "Ung\xFCltige Eingabe";
      }
    };
  };
  function Zt() {
    return { localeError: l4() };
  }
  var c4 = () => {
    let r = { string: { unit: "characters", verb: "to have" }, file: { unit: "bytes", verb: "to have" }, array: { unit: "items", verb: "to have" }, set: { unit: "items", verb: "to have" }, map: { unit: "entries", verb: "to have" } };
    function i(n) {
      return r[n] ?? null;
    }
    let o = { regex: "input", email: "email address", url: "URL", emoji: "emoji", uuid: "UUID", uuidv4: "UUIDv4", uuidv6: "UUIDv6", nanoid: "nanoid", guid: "GUID", cuid: "cuid", cuid2: "cuid2", ulid: "ULID", xid: "XID", ksuid: "KSUID", datetime: "ISO datetime", date: "ISO date", time: "ISO time", duration: "ISO duration", ipv4: "IPv4 address", ipv6: "IPv6 address", mac: "MAC address", cidrv4: "IPv4 range", cidrv6: "IPv6 range", base64: "base64-encoded string", base64url: "base64url-encoded string", json_string: "JSON string", e164: "E.164 number", jwt: "JWT", template_literal: "input" }, t = { nan: "NaN" };
    return (n) => {
      switch (n.code) {
        case "invalid_type": {
          let v = t[n.expected] ?? n.expected, u = k(n.input), $ = t[u] ?? u;
          return `Invalid input: expected ${v}, received ${$}`;
        }
        case "invalid_value":
          if (n.values.length === 1)
            return `Invalid input: expected ${U(n.values[0])}`;
          return `Invalid option: expected one of ${b(n.values, "|")}`;
        case "too_big": {
          let v = n.inclusive ? "<=" : "<", u = i(n.origin);
          if (u)
            return `Too big: expected ${n.origin ?? "value"} to have ${v}${n.maximum.toString()} ${u.unit ?? "elements"}`;
          return `Too big: expected ${n.origin ?? "value"} to be ${v}${n.maximum.toString()}`;
        }
        case "too_small": {
          let v = n.inclusive ? ">=" : ">", u = i(n.origin);
          if (u)
            return `Too small: expected ${n.origin} to have ${v}${n.minimum.toString()} ${u.unit}`;
          return `Too small: expected ${n.origin} to be ${v}${n.minimum.toString()}`;
        }
        case "invalid_format": {
          let v = n;
          if (v.format === "starts_with")
            return `Invalid string: must start with "${v.prefix}"`;
          if (v.format === "ends_with")
            return `Invalid string: must end with "${v.suffix}"`;
          if (v.format === "includes")
            return `Invalid string: must include "${v.includes}"`;
          if (v.format === "regex")
            return `Invalid string: must match pattern ${v.pattern}`;
          return `Invalid ${o[v.format] ?? n.format}`;
        }
        case "not_multiple_of":
          return `Invalid number: must be a multiple of ${n.divisor}`;
        case "unrecognized_keys":
          return `Unrecognized key${n.keys.length > 1 ? "s" : ""}: ${b(n.keys, ", ")}`;
        case "invalid_key":
          return `Invalid key in ${n.origin}`;
        case "invalid_union":
          return "Invalid input";
        case "invalid_element":
          return `Invalid value in ${n.origin}`;
        default:
          return "Invalid input";
      }
    };
  };
  function kn() {
    return { localeError: c4() };
  }
  var I4 = () => {
    let r = { string: { unit: "karaktrojn", verb: "havi" }, file: { unit: "bajtojn", verb: "havi" }, array: { unit: "elementojn", verb: "havi" }, set: { unit: "elementojn", verb: "havi" } };
    function i(n) {
      return r[n] ?? null;
    }
    let o = { regex: "enigo", email: "retadreso", url: "URL", emoji: "emo\u011Dio", uuid: "UUID", uuidv4: "UUIDv4", uuidv6: "UUIDv6", nanoid: "nanoid", guid: "GUID", cuid: "cuid", cuid2: "cuid2", ulid: "ULID", xid: "XID", ksuid: "KSUID", datetime: "ISO-datotempo", date: "ISO-dato", time: "ISO-tempo", duration: "ISO-da\u016Dro", ipv4: "IPv4-adreso", ipv6: "IPv6-adreso", cidrv4: "IPv4-rango", cidrv6: "IPv6-rango", base64: "64-ume kodita karaktraro", base64url: "URL-64-ume kodita karaktraro", json_string: "JSON-karaktraro", e164: "E.164-nombro", jwt: "JWT", template_literal: "enigo" }, t = { nan: "NaN", number: "nombro", array: "tabelo", null: "senvalora" };
    return (n) => {
      switch (n.code) {
        case "invalid_type": {
          let v = t[n.expected] ?? n.expected, u = k(n.input), $ = t[u] ?? u;
          if (/^[A-Z]/.test(n.expected))
            return `Nevalida enigo: atendi\u011Dis instanceof ${n.expected}, ricevi\u011Dis ${$}`;
          return `Nevalida enigo: atendi\u011Dis ${v}, ricevi\u011Dis ${$}`;
        }
        case "invalid_value":
          if (n.values.length === 1)
            return `Nevalida enigo: atendi\u011Dis ${U(n.values[0])}`;
          return `Nevalida opcio: atendi\u011Dis unu el ${b(n.values, "|")}`;
        case "too_big": {
          let v = n.inclusive ? "<=" : "<", u = i(n.origin);
          if (u)
            return `Tro granda: atendi\u011Dis ke ${n.origin ?? "valoro"} havu ${v}${n.maximum.toString()} ${u.unit ?? "elementojn"}`;
          return `Tro granda: atendi\u011Dis ke ${n.origin ?? "valoro"} havu ${v}${n.maximum.toString()}`;
        }
        case "too_small": {
          let v = n.inclusive ? ">=" : ">", u = i(n.origin);
          if (u)
            return `Tro malgranda: atendi\u011Dis ke ${n.origin} havu ${v}${n.minimum.toString()} ${u.unit}`;
          return `Tro malgranda: atendi\u011Dis ke ${n.origin} estu ${v}${n.minimum.toString()}`;
        }
        case "invalid_format": {
          let v = n;
          if (v.format === "starts_with")
            return `Nevalida karaktraro: devas komenci\u011Di per "${v.prefix}"`;
          if (v.format === "ends_with")
            return `Nevalida karaktraro: devas fini\u011Di per "${v.suffix}"`;
          if (v.format === "includes")
            return `Nevalida karaktraro: devas inkluzivi "${v.includes}"`;
          if (v.format === "regex")
            return `Nevalida karaktraro: devas kongrui kun la modelo ${v.pattern}`;
          return `Nevalida ${o[v.format] ?? n.format}`;
        }
        case "not_multiple_of":
          return `Nevalida nombro: devas esti oblo de ${n.divisor}`;
        case "unrecognized_keys":
          return `Nekonata${n.keys.length > 1 ? "j" : ""} \u015Dlosilo${n.keys.length > 1 ? "j" : ""}: ${b(n.keys, ", ")}`;
        case "invalid_key":
          return `Nevalida \u015Dlosilo en ${n.origin}`;
        case "invalid_union":
          return "Nevalida enigo";
        case "invalid_element":
          return `Nevalida valoro en ${n.origin}`;
        default:
          return "Nevalida enigo";
      }
    };
  };
  function dt() {
    return { localeError: I4() };
  }
  var b4 = () => {
    let r = { string: { unit: "caracteres", verb: "tener" }, file: { unit: "bytes", verb: "tener" }, array: { unit: "elementos", verb: "tener" }, set: { unit: "elementos", verb: "tener" } };
    function i(n) {
      return r[n] ?? null;
    }
    let o = { regex: "entrada", email: "direcci\xF3n de correo electr\xF3nico", url: "URL", emoji: "emoji", uuid: "UUID", uuidv4: "UUIDv4", uuidv6: "UUIDv6", nanoid: "nanoid", guid: "GUID", cuid: "cuid", cuid2: "cuid2", ulid: "ULID", xid: "XID", ksuid: "KSUID", datetime: "fecha y hora ISO", date: "fecha ISO", time: "hora ISO", duration: "duraci\xF3n ISO", ipv4: "direcci\xF3n IPv4", ipv6: "direcci\xF3n IPv6", cidrv4: "rango IPv4", cidrv6: "rango IPv6", base64: "cadena codificada en base64", base64url: "URL codificada en base64", json_string: "cadena JSON", e164: "n\xFAmero E.164", jwt: "JWT", template_literal: "entrada" }, t = { nan: "NaN", string: "texto", number: "n\xFAmero", boolean: "booleano", array: "arreglo", object: "objeto", set: "conjunto", file: "archivo", date: "fecha", bigint: "n\xFAmero grande", symbol: "s\xEDmbolo", undefined: "indefinido", null: "nulo", function: "funci\xF3n", map: "mapa", record: "registro", tuple: "tupla", enum: "enumeraci\xF3n", union: "uni\xF3n", literal: "literal", promise: "promesa", void: "vac\xEDo", never: "nunca", unknown: "desconocido", any: "cualquiera" };
    return (n) => {
      switch (n.code) {
        case "invalid_type": {
          let v = t[n.expected] ?? n.expected, u = k(n.input), $ = t[u] ?? u;
          if (/^[A-Z]/.test(n.expected))
            return `Entrada inv\xE1lida: se esperaba instanceof ${n.expected}, recibido ${$}`;
          return `Entrada inv\xE1lida: se esperaba ${v}, recibido ${$}`;
        }
        case "invalid_value":
          if (n.values.length === 1)
            return `Entrada inv\xE1lida: se esperaba ${U(n.values[0])}`;
          return `Opci\xF3n inv\xE1lida: se esperaba una de ${b(n.values, "|")}`;
        case "too_big": {
          let v = n.inclusive ? "<=" : "<", u = i(n.origin), $ = t[n.origin] ?? n.origin;
          if (u)
            return `Demasiado grande: se esperaba que ${$ ?? "valor"} tuviera ${v}${n.maximum.toString()} ${u.unit ?? "elementos"}`;
          return `Demasiado grande: se esperaba que ${$ ?? "valor"} fuera ${v}${n.maximum.toString()}`;
        }
        case "too_small": {
          let v = n.inclusive ? ">=" : ">", u = i(n.origin), $ = t[n.origin] ?? n.origin;
          if (u)
            return `Demasiado peque\xF1o: se esperaba que ${$} tuviera ${v}${n.minimum.toString()} ${u.unit}`;
          return `Demasiado peque\xF1o: se esperaba que ${$} fuera ${v}${n.minimum.toString()}`;
        }
        case "invalid_format": {
          let v = n;
          if (v.format === "starts_with")
            return `Cadena inv\xE1lida: debe comenzar con "${v.prefix}"`;
          if (v.format === "ends_with")
            return `Cadena inv\xE1lida: debe terminar en "${v.suffix}"`;
          if (v.format === "includes")
            return `Cadena inv\xE1lida: debe incluir "${v.includes}"`;
          if (v.format === "regex")
            return `Cadena inv\xE1lida: debe coincidir con el patr\xF3n ${v.pattern}`;
          return `Inv\xE1lido ${o[v.format] ?? n.format}`;
        }
        case "not_multiple_of":
          return `N\xFAmero inv\xE1lido: debe ser m\xFAltiplo de ${n.divisor}`;
        case "unrecognized_keys":
          return `Llave${n.keys.length > 1 ? "s" : ""} desconocida${n.keys.length > 1 ? "s" : ""}: ${b(n.keys, ", ")}`;
        case "invalid_key":
          return `Llave inv\xE1lida en ${t[n.origin] ?? n.origin}`;
        case "invalid_union":
          return "Entrada inv\xE1lida";
        case "invalid_element":
          return `Valor inv\xE1lido en ${t[n.origin] ?? n.origin}`;
        default:
          return "Entrada inv\xE1lida";
      }
    };
  };
  function Ct() {
    return { localeError: b4() };
  }
  var _4 = () => {
    let r = { string: { unit: "\u06A9\u0627\u0631\u0627\u06A9\u062A\u0631", verb: "\u062F\u0627\u0634\u062A\u0647 \u0628\u0627\u0634\u062F" }, file: { unit: "\u0628\u0627\u06CC\u062A", verb: "\u062F\u0627\u0634\u062A\u0647 \u0628\u0627\u0634\u062F" }, array: { unit: "\u0622\u06CC\u062A\u0645", verb: "\u062F\u0627\u0634\u062A\u0647 \u0628\u0627\u0634\u062F" }, set: { unit: "\u0622\u06CC\u062A\u0645", verb: "\u062F\u0627\u0634\u062A\u0647 \u0628\u0627\u0634\u062F" } };
    function i(n) {
      return r[n] ?? null;
    }
    let o = { regex: "\u0648\u0631\u0648\u062F\u06CC", email: "\u0622\u062F\u0631\u0633 \u0627\u06CC\u0645\u06CC\u0644", url: "URL", emoji: "\u0627\u06CC\u0645\u0648\u062C\u06CC", uuid: "UUID", uuidv4: "UUIDv4", uuidv6: "UUIDv6", nanoid: "nanoid", guid: "GUID", cuid: "cuid", cuid2: "cuid2", ulid: "ULID", xid: "XID", ksuid: "KSUID", datetime: "\u062A\u0627\u0631\u06CC\u062E \u0648 \u0632\u0645\u0627\u0646 \u0627\u06CC\u0632\u0648", date: "\u062A\u0627\u0631\u06CC\u062E \u0627\u06CC\u0632\u0648", time: "\u0632\u0645\u0627\u0646 \u0627\u06CC\u0632\u0648", duration: "\u0645\u062F\u062A \u0632\u0645\u0627\u0646 \u0627\u06CC\u0632\u0648", ipv4: "IPv4 \u0622\u062F\u0631\u0633", ipv6: "IPv6 \u0622\u062F\u0631\u0633", cidrv4: "IPv4 \u062F\u0627\u0645\u0646\u0647", cidrv6: "IPv6 \u062F\u0627\u0645\u0646\u0647", base64: "base64-encoded \u0631\u0634\u062A\u0647", base64url: "base64url-encoded \u0631\u0634\u062A\u0647", json_string: "JSON \u0631\u0634\u062A\u0647", e164: "E.164 \u0639\u062F\u062F", jwt: "JWT", template_literal: "\u0648\u0631\u0648\u062F\u06CC" }, t = { nan: "NaN", number: "\u0639\u062F\u062F", array: "\u0622\u0631\u0627\u06CC\u0647" };
    return (n) => {
      switch (n.code) {
        case "invalid_type": {
          let v = t[n.expected] ?? n.expected, u = k(n.input), $ = t[u] ?? u;
          if (/^[A-Z]/.test(n.expected))
            return `\u0648\u0631\u0648\u062F\u06CC \u0646\u0627\u0645\u0639\u062A\u0628\u0631: \u0645\u06CC\u200C\u0628\u0627\u06CC\u0633\u062A instanceof ${n.expected} \u0645\u06CC\u200C\u0628\u0648\u062F\u060C ${$} \u062F\u0631\u06CC\u0627\u0641\u062A \u0634\u062F`;
          return `\u0648\u0631\u0648\u062F\u06CC \u0646\u0627\u0645\u0639\u062A\u0628\u0631: \u0645\u06CC\u200C\u0628\u0627\u06CC\u0633\u062A ${v} \u0645\u06CC\u200C\u0628\u0648\u062F\u060C ${$} \u062F\u0631\u06CC\u0627\u0641\u062A \u0634\u062F`;
        }
        case "invalid_value":
          if (n.values.length === 1)
            return `\u0648\u0631\u0648\u062F\u06CC \u0646\u0627\u0645\u0639\u062A\u0628\u0631: \u0645\u06CC\u200C\u0628\u0627\u06CC\u0633\u062A ${U(n.values[0])} \u0645\u06CC\u200C\u0628\u0648\u062F`;
          return `\u06AF\u0632\u06CC\u0646\u0647 \u0646\u0627\u0645\u0639\u062A\u0628\u0631: \u0645\u06CC\u200C\u0628\u0627\u06CC\u0633\u062A \u06CC\u06A9\u06CC \u0627\u0632 ${b(n.values, "|")} \u0645\u06CC\u200C\u0628\u0648\u062F`;
        case "too_big": {
          let v = n.inclusive ? "<=" : "<", u = i(n.origin);
          if (u)
            return `\u062E\u06CC\u0644\u06CC \u0628\u0632\u0631\u06AF: ${n.origin ?? "\u0645\u0642\u062F\u0627\u0631"} \u0628\u0627\u06CC\u062F ${v}${n.maximum.toString()} ${u.unit ?? "\u0639\u0646\u0635\u0631"} \u0628\u0627\u0634\u062F`;
          return `\u062E\u06CC\u0644\u06CC \u0628\u0632\u0631\u06AF: ${n.origin ?? "\u0645\u0642\u062F\u0627\u0631"} \u0628\u0627\u06CC\u062F ${v}${n.maximum.toString()} \u0628\u0627\u0634\u062F`;
        }
        case "too_small": {
          let v = n.inclusive ? ">=" : ">", u = i(n.origin);
          if (u)
            return `\u062E\u06CC\u0644\u06CC \u06A9\u0648\u0686\u06A9: ${n.origin} \u0628\u0627\u06CC\u062F ${v}${n.minimum.toString()} ${u.unit} \u0628\u0627\u0634\u062F`;
          return `\u062E\u06CC\u0644\u06CC \u06A9\u0648\u0686\u06A9: ${n.origin} \u0628\u0627\u06CC\u062F ${v}${n.minimum.toString()} \u0628\u0627\u0634\u062F`;
        }
        case "invalid_format": {
          let v = n;
          if (v.format === "starts_with")
            return `\u0631\u0634\u062A\u0647 \u0646\u0627\u0645\u0639\u062A\u0628\u0631: \u0628\u0627\u06CC\u062F \u0628\u0627 "${v.prefix}" \u0634\u0631\u0648\u0639 \u0634\u0648\u062F`;
          if (v.format === "ends_with")
            return `\u0631\u0634\u062A\u0647 \u0646\u0627\u0645\u0639\u062A\u0628\u0631: \u0628\u0627\u06CC\u062F \u0628\u0627 "${v.suffix}" \u062A\u0645\u0627\u0645 \u0634\u0648\u062F`;
          if (v.format === "includes")
            return `\u0631\u0634\u062A\u0647 \u0646\u0627\u0645\u0639\u062A\u0628\u0631: \u0628\u0627\u06CC\u062F \u0634\u0627\u0645\u0644 "${v.includes}" \u0628\u0627\u0634\u062F`;
          if (v.format === "regex")
            return `\u0631\u0634\u062A\u0647 \u0646\u0627\u0645\u0639\u062A\u0628\u0631: \u0628\u0627\u06CC\u062F \u0628\u0627 \u0627\u0644\u06AF\u0648\u06CC ${v.pattern} \u0645\u0637\u0627\u0628\u0642\u062A \u062F\u0627\u0634\u062A\u0647 \u0628\u0627\u0634\u062F`;
          return `${o[v.format] ?? n.format} \u0646\u0627\u0645\u0639\u062A\u0628\u0631`;
        }
        case "not_multiple_of":
          return `\u0639\u062F\u062F \u0646\u0627\u0645\u0639\u062A\u0628\u0631: \u0628\u0627\u06CC\u062F \u0645\u0636\u0631\u0628 ${n.divisor} \u0628\u0627\u0634\u062F`;
        case "unrecognized_keys":
          return `\u06A9\u0644\u06CC\u062F${n.keys.length > 1 ? "\u0647\u0627\u06CC" : ""} \u0646\u0627\u0634\u0646\u0627\u0633: ${b(n.keys, ", ")}`;
        case "invalid_key":
          return `\u06A9\u0644\u06CC\u062F \u0646\u0627\u0634\u0646\u0627\u0633 \u062F\u0631 ${n.origin}`;
        case "invalid_union":
          return "\u0648\u0631\u0648\u062F\u06CC \u0646\u0627\u0645\u0639\u062A\u0628\u0631";
        case "invalid_element":
          return `\u0645\u0642\u062F\u0627\u0631 \u0646\u0627\u0645\u0639\u062A\u0628\u0631 \u062F\u0631 ${n.origin}`;
        default:
          return "\u0648\u0631\u0648\u062F\u06CC \u0646\u0627\u0645\u0639\u062A\u0628\u0631";
      }
    };
  };
  function ft() {
    return { localeError: _4() };
  }
  var U4 = () => {
    let r = { string: { unit: "merkki\xE4", subject: "merkkijonon" }, file: { unit: "tavua", subject: "tiedoston" }, array: { unit: "alkiota", subject: "listan" }, set: { unit: "alkiota", subject: "joukon" }, number: { unit: "", subject: "luvun" }, bigint: { unit: "", subject: "suuren kokonaisluvun" }, int: { unit: "", subject: "kokonaisluvun" }, date: { unit: "", subject: "p\xE4iv\xE4m\xE4\xE4r\xE4n" } };
    function i(n) {
      return r[n] ?? null;
    }
    let o = { regex: "s\xE4\xE4nn\xF6llinen lauseke", email: "s\xE4hk\xF6postiosoite", url: "URL-osoite", emoji: "emoji", uuid: "UUID", uuidv4: "UUIDv4", uuidv6: "UUIDv6", nanoid: "nanoid", guid: "GUID", cuid: "cuid", cuid2: "cuid2", ulid: "ULID", xid: "XID", ksuid: "KSUID", datetime: "ISO-aikaleima", date: "ISO-p\xE4iv\xE4m\xE4\xE4r\xE4", time: "ISO-aika", duration: "ISO-kesto", ipv4: "IPv4-osoite", ipv6: "IPv6-osoite", cidrv4: "IPv4-alue", cidrv6: "IPv6-alue", base64: "base64-koodattu merkkijono", base64url: "base64url-koodattu merkkijono", json_string: "JSON-merkkijono", e164: "E.164-luku", jwt: "JWT", template_literal: "templaattimerkkijono" }, t = { nan: "NaN" };
    return (n) => {
      switch (n.code) {
        case "invalid_type": {
          let v = t[n.expected] ?? n.expected, u = k(n.input), $ = t[u] ?? u;
          if (/^[A-Z]/.test(n.expected))
            return `Virheellinen tyyppi: odotettiin instanceof ${n.expected}, oli ${$}`;
          return `Virheellinen tyyppi: odotettiin ${v}, oli ${$}`;
        }
        case "invalid_value":
          if (n.values.length === 1)
            return `Virheellinen sy\xF6te: t\xE4ytyy olla ${U(n.values[0])}`;
          return `Virheellinen valinta: t\xE4ytyy olla yksi seuraavista: ${b(n.values, "|")}`;
        case "too_big": {
          let v = n.inclusive ? "<=" : "<", u = i(n.origin);
          if (u)
            return `Liian suuri: ${u.subject} t\xE4ytyy olla ${v}${n.maximum.toString()} ${u.unit}`.trim();
          return `Liian suuri: arvon t\xE4ytyy olla ${v}${n.maximum.toString()}`;
        }
        case "too_small": {
          let v = n.inclusive ? ">=" : ">", u = i(n.origin);
          if (u)
            return `Liian pieni: ${u.subject} t\xE4ytyy olla ${v}${n.minimum.toString()} ${u.unit}`.trim();
          return `Liian pieni: arvon t\xE4ytyy olla ${v}${n.minimum.toString()}`;
        }
        case "invalid_format": {
          let v = n;
          if (v.format === "starts_with")
            return `Virheellinen sy\xF6te: t\xE4ytyy alkaa "${v.prefix}"`;
          if (v.format === "ends_with")
            return `Virheellinen sy\xF6te: t\xE4ytyy loppua "${v.suffix}"`;
          if (v.format === "includes")
            return `Virheellinen sy\xF6te: t\xE4ytyy sis\xE4lt\xE4\xE4 "${v.includes}"`;
          if (v.format === "regex")
            return `Virheellinen sy\xF6te: t\xE4ytyy vastata s\xE4\xE4nn\xF6llist\xE4 lauseketta ${v.pattern}`;
          return `Virheellinen ${o[v.format] ?? n.format}`;
        }
        case "not_multiple_of":
          return `Virheellinen luku: t\xE4ytyy olla luvun ${n.divisor} monikerta`;
        case "unrecognized_keys":
          return `${n.keys.length > 1 ? "Tuntemattomat avaimet" : "Tuntematon avain"}: ${b(n.keys, ", ")}`;
        case "invalid_key":
          return "Virheellinen avain tietueessa";
        case "invalid_union":
          return "Virheellinen unioni";
        case "invalid_element":
          return "Virheellinen arvo joukossa";
        default:
          return "Virheellinen sy\xF6te";
      }
    };
  };
  function ht() {
    return { localeError: U4() };
  }
  var k4 = () => {
    let r = { string: { unit: "caract\xE8res", verb: "avoir" }, file: { unit: "octets", verb: "avoir" }, array: { unit: "\xE9l\xE9ments", verb: "avoir" }, set: { unit: "\xE9l\xE9ments", verb: "avoir" } };
    function i(n) {
      return r[n] ?? null;
    }
    let o = { regex: "entr\xE9e", email: "adresse e-mail", url: "URL", emoji: "emoji", uuid: "UUID", uuidv4: "UUIDv4", uuidv6: "UUIDv6", nanoid: "nanoid", guid: "GUID", cuid: "cuid", cuid2: "cuid2", ulid: "ULID", xid: "XID", ksuid: "KSUID", datetime: "date et heure ISO", date: "date ISO", time: "heure ISO", duration: "dur\xE9e ISO", ipv4: "adresse IPv4", ipv6: "adresse IPv6", cidrv4: "plage IPv4", cidrv6: "plage IPv6", base64: "cha\xEEne encod\xE9e en base64", base64url: "cha\xEEne encod\xE9e en base64url", json_string: "cha\xEEne JSON", e164: "num\xE9ro E.164", jwt: "JWT", template_literal: "entr\xE9e" }, t = { nan: "NaN", number: "nombre", array: "tableau" };
    return (n) => {
      switch (n.code) {
        case "invalid_type": {
          let v = t[n.expected] ?? n.expected, u = k(n.input), $ = t[u] ?? u;
          if (/^[A-Z]/.test(n.expected))
            return `Entr\xE9e invalide : instanceof ${n.expected} attendu, ${$} re\xE7u`;
          return `Entr\xE9e invalide : ${v} attendu, ${$} re\xE7u`;
        }
        case "invalid_value":
          if (n.values.length === 1)
            return `Entr\xE9e invalide : ${U(n.values[0])} attendu`;
          return `Option invalide : une valeur parmi ${b(n.values, "|")} attendue`;
        case "too_big": {
          let v = n.inclusive ? "<=" : "<", u = i(n.origin);
          if (u)
            return `Trop grand : ${n.origin ?? "valeur"} doit ${u.verb} ${v}${n.maximum.toString()} ${u.unit ?? "\xE9l\xE9ment(s)"}`;
          return `Trop grand : ${n.origin ?? "valeur"} doit \xEAtre ${v}${n.maximum.toString()}`;
        }
        case "too_small": {
          let v = n.inclusive ? ">=" : ">", u = i(n.origin);
          if (u)
            return `Trop petit : ${n.origin} doit ${u.verb} ${v}${n.minimum.toString()} ${u.unit}`;
          return `Trop petit : ${n.origin} doit \xEAtre ${v}${n.minimum.toString()}`;
        }
        case "invalid_format": {
          let v = n;
          if (v.format === "starts_with")
            return `Cha\xEEne invalide : doit commencer par "${v.prefix}"`;
          if (v.format === "ends_with")
            return `Cha\xEEne invalide : doit se terminer par "${v.suffix}"`;
          if (v.format === "includes")
            return `Cha\xEEne invalide : doit inclure "${v.includes}"`;
          if (v.format === "regex")
            return `Cha\xEEne invalide : doit correspondre au mod\xE8le ${v.pattern}`;
          return `${o[v.format] ?? n.format} invalide`;
        }
        case "not_multiple_of":
          return `Nombre invalide : doit \xEAtre un multiple de ${n.divisor}`;
        case "unrecognized_keys":
          return `Cl\xE9${n.keys.length > 1 ? "s" : ""} non reconnue${n.keys.length > 1 ? "s" : ""} : ${b(n.keys, ", ")}`;
        case "invalid_key":
          return `Cl\xE9 invalide dans ${n.origin}`;
        case "invalid_union":
          return "Entr\xE9e invalide";
        case "invalid_element":
          return `Valeur invalide dans ${n.origin}`;
        default:
          return "Entr\xE9e invalide";
      }
    };
  };
  function yt() {
    return { localeError: k4() };
  }
  var D4 = () => {
    let r = { string: { unit: "caract\xE8res", verb: "avoir" }, file: { unit: "octets", verb: "avoir" }, array: { unit: "\xE9l\xE9ments", verb: "avoir" }, set: { unit: "\xE9l\xE9ments", verb: "avoir" } };
    function i(n) {
      return r[n] ?? null;
    }
    let o = { regex: "entr\xE9e", email: "adresse courriel", url: "URL", emoji: "emoji", uuid: "UUID", uuidv4: "UUIDv4", uuidv6: "UUIDv6", nanoid: "nanoid", guid: "GUID", cuid: "cuid", cuid2: "cuid2", ulid: "ULID", xid: "XID", ksuid: "KSUID", datetime: "date-heure ISO", date: "date ISO", time: "heure ISO", duration: "dur\xE9e ISO", ipv4: "adresse IPv4", ipv6: "adresse IPv6", cidrv4: "plage IPv4", cidrv6: "plage IPv6", base64: "cha\xEEne encod\xE9e en base64", base64url: "cha\xEEne encod\xE9e en base64url", json_string: "cha\xEEne JSON", e164: "num\xE9ro E.164", jwt: "JWT", template_literal: "entr\xE9e" }, t = { nan: "NaN" };
    return (n) => {
      switch (n.code) {
        case "invalid_type": {
          let v = t[n.expected] ?? n.expected, u = k(n.input), $ = t[u] ?? u;
          if (/^[A-Z]/.test(n.expected))
            return `Entr\xE9e invalide : attendu instanceof ${n.expected}, re\xE7u ${$}`;
          return `Entr\xE9e invalide : attendu ${v}, re\xE7u ${$}`;
        }
        case "invalid_value":
          if (n.values.length === 1)
            return `Entr\xE9e invalide : attendu ${U(n.values[0])}`;
          return `Option invalide : attendu l'une des valeurs suivantes ${b(n.values, "|")}`;
        case "too_big": {
          let v = n.inclusive ? "\u2264" : "<", u = i(n.origin);
          if (u)
            return `Trop grand : attendu que ${n.origin ?? "la valeur"} ait ${v}${n.maximum.toString()} ${u.unit}`;
          return `Trop grand : attendu que ${n.origin ?? "la valeur"} soit ${v}${n.maximum.toString()}`;
        }
        case "too_small": {
          let v = n.inclusive ? "\u2265" : ">", u = i(n.origin);
          if (u)
            return `Trop petit : attendu que ${n.origin} ait ${v}${n.minimum.toString()} ${u.unit}`;
          return `Trop petit : attendu que ${n.origin} soit ${v}${n.minimum.toString()}`;
        }
        case "invalid_format": {
          let v = n;
          if (v.format === "starts_with")
            return `Cha\xEEne invalide : doit commencer par "${v.prefix}"`;
          if (v.format === "ends_with")
            return `Cha\xEEne invalide : doit se terminer par "${v.suffix}"`;
          if (v.format === "includes")
            return `Cha\xEEne invalide : doit inclure "${v.includes}"`;
          if (v.format === "regex")
            return `Cha\xEEne invalide : doit correspondre au motif ${v.pattern}`;
          return `${o[v.format] ?? n.format} invalide`;
        }
        case "not_multiple_of":
          return `Nombre invalide : doit \xEAtre un multiple de ${n.divisor}`;
        case "unrecognized_keys":
          return `Cl\xE9${n.keys.length > 1 ? "s" : ""} non reconnue${n.keys.length > 1 ? "s" : ""} : ${b(n.keys, ", ")}`;
        case "invalid_key":
          return `Cl\xE9 invalide dans ${n.origin}`;
        case "invalid_union":
          return "Entr\xE9e invalide";
        case "invalid_element":
          return `Valeur invalide dans ${n.origin}`;
        default:
          return "Entr\xE9e invalide";
      }
    };
  };
  function at() {
    return { localeError: D4() };
  }
  var w4 = () => {
    let r = { string: { label: "\u05DE\u05D7\u05E8\u05D5\u05D6\u05EA", gender: "f" }, number: { label: "\u05DE\u05E1\u05E4\u05E8", gender: "m" }, boolean: { label: "\u05E2\u05E8\u05DA \u05D1\u05D5\u05DC\u05D9\u05D0\u05E0\u05D9", gender: "m" }, bigint: { label: "BigInt", gender: "m" }, date: { label: "\u05EA\u05D0\u05E8\u05D9\u05DA", gender: "m" }, array: { label: "\u05DE\u05E2\u05E8\u05DA", gender: "m" }, object: { label: "\u05D0\u05D5\u05D1\u05D9\u05D9\u05E7\u05D8", gender: "m" }, null: { label: "\u05E2\u05E8\u05DA \u05E8\u05D9\u05E7 (null)", gender: "m" }, undefined: { label: "\u05E2\u05E8\u05DA \u05DC\u05D0 \u05DE\u05D5\u05D2\u05D3\u05E8 (undefined)", gender: "m" }, symbol: { label: "\u05E1\u05D9\u05DE\u05D1\u05D5\u05DC (Symbol)", gender: "m" }, function: { label: "\u05E4\u05D5\u05E0\u05E7\u05E6\u05D9\u05D4", gender: "f" }, map: { label: "\u05DE\u05E4\u05D4 (Map)", gender: "f" }, set: { label: "\u05E7\u05D1\u05D5\u05E6\u05D4 (Set)", gender: "f" }, file: { label: "\u05E7\u05D5\u05D1\u05E5", gender: "m" }, promise: { label: "Promise", gender: "m" }, NaN: { label: "NaN", gender: "m" }, unknown: { label: "\u05E2\u05E8\u05DA \u05DC\u05D0 \u05D9\u05D3\u05D5\u05E2", gender: "m" }, value: { label: "\u05E2\u05E8\u05DA", gender: "m" } }, i = { string: { unit: "\u05EA\u05D5\u05D5\u05D9\u05DD", shortLabel: "\u05E7\u05E6\u05E8", longLabel: "\u05D0\u05E8\u05D5\u05DA" }, file: { unit: "\u05D1\u05D9\u05D9\u05D8\u05D9\u05DD", shortLabel: "\u05E7\u05D8\u05DF", longLabel: "\u05D2\u05D3\u05D5\u05DC" }, array: { unit: "\u05E4\u05E8\u05D9\u05D8\u05D9\u05DD", shortLabel: "\u05E7\u05D8\u05DF", longLabel: "\u05D2\u05D3\u05D5\u05DC" }, set: { unit: "\u05E4\u05E8\u05D9\u05D8\u05D9\u05DD", shortLabel: "\u05E7\u05D8\u05DF", longLabel: "\u05D2\u05D3\u05D5\u05DC" }, number: { unit: "", shortLabel: "\u05E7\u05D8\u05DF", longLabel: "\u05D2\u05D3\u05D5\u05DC" } }, o = (e) => e ? r[e] : void 0, t = (e) => {
      let I = o(e);
      if (I)
        return I.label;
      return e ?? r.unknown.label;
    }, n = (e) => `\u05D4${t(e)}`, v = (e) => {
      return (o(e)?.gender ?? "m") === "f" ? "\u05E6\u05E8\u05D9\u05DB\u05D4 \u05DC\u05D4\u05D9\u05D5\u05EA" : "\u05E6\u05E8\u05D9\u05DA \u05DC\u05D4\u05D9\u05D5\u05EA";
    }, u = (e) => {
      if (!e)
        return null;
      return i[e] ?? null;
    }, $ = { regex: { label: "\u05E7\u05DC\u05D8", gender: "m" }, email: { label: "\u05DB\u05EA\u05D5\u05D1\u05EA \u05D0\u05D9\u05DE\u05D9\u05D9\u05DC", gender: "f" }, url: { label: "\u05DB\u05EA\u05D5\u05D1\u05EA \u05E8\u05E9\u05EA", gender: "f" }, emoji: { label: "\u05D0\u05D9\u05DE\u05D5\u05D2'\u05D9", gender: "m" }, uuid: { label: "UUID", gender: "m" }, nanoid: { label: "nanoid", gender: "m" }, guid: { label: "GUID", gender: "m" }, cuid: { label: "cuid", gender: "m" }, cuid2: { label: "cuid2", gender: "m" }, ulid: { label: "ULID", gender: "m" }, xid: { label: "XID", gender: "m" }, ksuid: { label: "KSUID", gender: "m" }, datetime: { label: "\u05EA\u05D0\u05E8\u05D9\u05DA \u05D5\u05D6\u05DE\u05DF ISO", gender: "m" }, date: { label: "\u05EA\u05D0\u05E8\u05D9\u05DA ISO", gender: "m" }, time: { label: "\u05D6\u05DE\u05DF ISO", gender: "m" }, duration: { label: "\u05DE\u05E9\u05DA \u05D6\u05DE\u05DF ISO", gender: "m" }, ipv4: { label: "\u05DB\u05EA\u05D5\u05D1\u05EA IPv4", gender: "f" }, ipv6: { label: "\u05DB\u05EA\u05D5\u05D1\u05EA IPv6", gender: "f" }, cidrv4: { label: "\u05D8\u05D5\u05D5\u05D7 IPv4", gender: "m" }, cidrv6: { label: "\u05D8\u05D5\u05D5\u05D7 IPv6", gender: "m" }, base64: { label: "\u05DE\u05D7\u05E8\u05D5\u05D6\u05EA \u05D1\u05D1\u05E1\u05D9\u05E1 64", gender: "f" }, base64url: { label: "\u05DE\u05D7\u05E8\u05D5\u05D6\u05EA \u05D1\u05D1\u05E1\u05D9\u05E1 64 \u05DC\u05DB\u05EA\u05D5\u05D1\u05D5\u05EA \u05E8\u05E9\u05EA", gender: "f" }, json_string: { label: "\u05DE\u05D7\u05E8\u05D5\u05D6\u05EA JSON", gender: "f" }, e164: { label: "\u05DE\u05E1\u05E4\u05E8 E.164", gender: "m" }, jwt: { label: "JWT", gender: "m" }, ends_with: { label: "\u05E7\u05DC\u05D8", gender: "m" }, includes: { label: "\u05E7\u05DC\u05D8", gender: "m" }, lowercase: { label: "\u05E7\u05DC\u05D8", gender: "m" }, starts_with: { label: "\u05E7\u05DC\u05D8", gender: "m" }, uppercase: { label: "\u05E7\u05DC\u05D8", gender: "m" } }, l = { nan: "NaN" };
    return (e) => {
      switch (e.code) {
        case "invalid_type": {
          let I = e.expected, _ = l[I ?? ""] ?? t(I), N = k(e.input), O = l[N] ?? r[N]?.label ?? N;
          if (/^[A-Z]/.test(e.expected))
            return `\u05E7\u05DC\u05D8 \u05DC\u05D0 \u05EA\u05E7\u05D9\u05DF: \u05E6\u05E8\u05D9\u05DA \u05DC\u05D4\u05D9\u05D5\u05EA instanceof ${e.expected}, \u05D4\u05EA\u05E7\u05D1\u05DC ${O}`;
          return `\u05E7\u05DC\u05D8 \u05DC\u05D0 \u05EA\u05E7\u05D9\u05DF: \u05E6\u05E8\u05D9\u05DA \u05DC\u05D4\u05D9\u05D5\u05EA ${_}, \u05D4\u05EA\u05E7\u05D1\u05DC ${O}`;
        }
        case "invalid_value": {
          if (e.values.length === 1)
            return `\u05E2\u05E8\u05DA \u05DC\u05D0 \u05EA\u05E7\u05D9\u05DF: \u05D4\u05E2\u05E8\u05DA \u05D7\u05D9\u05D9\u05D1 \u05DC\u05D4\u05D9\u05D5\u05EA ${U(e.values[0])}`;
          let I = e.values.map((O) => U(O));
          if (e.values.length === 2)
            return `\u05E2\u05E8\u05DA \u05DC\u05D0 \u05EA\u05E7\u05D9\u05DF: \u05D4\u05D0\u05E4\u05E9\u05E8\u05D5\u05D9\u05D5\u05EA \u05D4\u05DE\u05EA\u05D0\u05D9\u05DE\u05D5\u05EA \u05D4\u05DF ${I[0]} \u05D0\u05D5 ${I[1]}`;
          let _ = I[I.length - 1];
          return `\u05E2\u05E8\u05DA \u05DC\u05D0 \u05EA\u05E7\u05D9\u05DF: \u05D4\u05D0\u05E4\u05E9\u05E8\u05D5\u05D9\u05D5\u05EA \u05D4\u05DE\u05EA\u05D0\u05D9\u05DE\u05D5\u05EA \u05D4\u05DF ${I.slice(0, -1).join(", ")} \u05D0\u05D5 ${_}`;
        }
        case "too_big": {
          let I = u(e.origin), _ = n(e.origin ?? "value");
          if (e.origin === "string")
            return `${I?.longLabel ?? "\u05D0\u05E8\u05D5\u05DA"} \u05DE\u05D3\u05D9: ${_} \u05E6\u05E8\u05D9\u05DB\u05D4 \u05DC\u05D4\u05DB\u05D9\u05DC ${e.maximum.toString()} ${I?.unit ?? ""} ${e.inclusive ? "\u05D0\u05D5 \u05E4\u05D7\u05D5\u05EA" : "\u05DC\u05DB\u05DC \u05D4\u05D9\u05D5\u05EA\u05E8"}`.trim();
          if (e.origin === "number") {
            let J = e.inclusive ? `\u05E7\u05D8\u05DF \u05D0\u05D5 \u05E9\u05D5\u05D5\u05D4 \u05DC-${e.maximum}` : `\u05E7\u05D8\u05DF \u05DE-${e.maximum}`;
            return `\u05D2\u05D3\u05D5\u05DC \u05DE\u05D3\u05D9: ${_} \u05E6\u05E8\u05D9\u05DA \u05DC\u05D4\u05D9\u05D5\u05EA ${J}`;
          }
          if (e.origin === "array" || e.origin === "set") {
            let J = e.origin === "set" ? "\u05E6\u05E8\u05D9\u05DB\u05D4" : "\u05E6\u05E8\u05D9\u05DA", X = e.inclusive ? `${e.maximum} ${I?.unit ?? ""} \u05D0\u05D5 \u05E4\u05D7\u05D5\u05EA` : `\u05E4\u05D7\u05D5\u05EA \u05DE-${e.maximum} ${I?.unit ?? ""}`;
            return `\u05D2\u05D3\u05D5\u05DC \u05DE\u05D3\u05D9: ${_} ${J} \u05DC\u05D4\u05DB\u05D9\u05DC ${X}`.trim();
          }
          let N = e.inclusive ? "<=" : "<", O = v(e.origin ?? "value");
          if (I?.unit)
            return `${I.longLabel} \u05DE\u05D3\u05D9: ${_} ${O} ${N}${e.maximum.toString()} ${I.unit}`;
          return `${I?.longLabel ?? "\u05D2\u05D3\u05D5\u05DC"} \u05DE\u05D3\u05D9: ${_} ${O} ${N}${e.maximum.toString()}`;
        }
        case "too_small": {
          let I = u(e.origin), _ = n(e.origin ?? "value");
          if (e.origin === "string")
            return `${I?.shortLabel ?? "\u05E7\u05E6\u05E8"} \u05DE\u05D3\u05D9: ${_} \u05E6\u05E8\u05D9\u05DB\u05D4 \u05DC\u05D4\u05DB\u05D9\u05DC ${e.minimum.toString()} ${I?.unit ?? ""} ${e.inclusive ? "\u05D0\u05D5 \u05D9\u05D5\u05EA\u05E8" : "\u05DC\u05E4\u05D7\u05D5\u05EA"}`.trim();
          if (e.origin === "number") {
            let J = e.inclusive ? `\u05D2\u05D3\u05D5\u05DC \u05D0\u05D5 \u05E9\u05D5\u05D5\u05D4 \u05DC-${e.minimum}` : `\u05D2\u05D3\u05D5\u05DC \u05DE-${e.minimum}`;
            return `\u05E7\u05D8\u05DF \u05DE\u05D3\u05D9: ${_} \u05E6\u05E8\u05D9\u05DA \u05DC\u05D4\u05D9\u05D5\u05EA ${J}`;
          }
          if (e.origin === "array" || e.origin === "set") {
            let J = e.origin === "set" ? "\u05E6\u05E8\u05D9\u05DB\u05D4" : "\u05E6\u05E8\u05D9\u05DA";
            if (e.minimum === 1 && e.inclusive) {
              let Sr = e.origin === "set" ? "\u05DC\u05E4\u05D7\u05D5\u05EA \u05E4\u05E8\u05D9\u05D8 \u05D0\u05D7\u05D3" : "\u05DC\u05E4\u05D7\u05D5\u05EA \u05E4\u05E8\u05D9\u05D8 \u05D0\u05D7\u05D3";
              return `\u05E7\u05D8\u05DF \u05DE\u05D3\u05D9: ${_} ${J} \u05DC\u05D4\u05DB\u05D9\u05DC ${Sr}`;
            }
            let X = e.inclusive ? `${e.minimum} ${I?.unit ?? ""} \u05D0\u05D5 \u05D9\u05D5\u05EA\u05E8` : `\u05D9\u05D5\u05EA\u05E8 \u05DE-${e.minimum} ${I?.unit ?? ""}`;
            return `\u05E7\u05D8\u05DF \u05DE\u05D3\u05D9: ${_} ${J} \u05DC\u05D4\u05DB\u05D9\u05DC ${X}`.trim();
          }
          let N = e.inclusive ? ">=" : ">", O = v(e.origin ?? "value");
          if (I?.unit)
            return `${I.shortLabel} \u05DE\u05D3\u05D9: ${_} ${O} ${N}${e.minimum.toString()} ${I.unit}`;
          return `${I?.shortLabel ?? "\u05E7\u05D8\u05DF"} \u05DE\u05D3\u05D9: ${_} ${O} ${N}${e.minimum.toString()}`;
        }
        case "invalid_format": {
          let I = e;
          if (I.format === "starts_with")
            return `\u05D4\u05DE\u05D7\u05E8\u05D5\u05D6\u05EA \u05D7\u05D9\u05D9\u05D1\u05EA \u05DC\u05D4\u05EA\u05D7\u05D9\u05DC \u05D1 "${I.prefix}"`;
          if (I.format === "ends_with")
            return `\u05D4\u05DE\u05D7\u05E8\u05D5\u05D6\u05EA \u05D7\u05D9\u05D9\u05D1\u05EA \u05DC\u05D4\u05E1\u05EA\u05D9\u05D9\u05DD \u05D1 "${I.suffix}"`;
          if (I.format === "includes")
            return `\u05D4\u05DE\u05D7\u05E8\u05D5\u05D6\u05EA \u05D7\u05D9\u05D9\u05D1\u05EA \u05DC\u05DB\u05DC\u05D5\u05DC "${I.includes}"`;
          if (I.format === "regex")
            return `\u05D4\u05DE\u05D7\u05E8\u05D5\u05D6\u05EA \u05D7\u05D9\u05D9\u05D1\u05EA \u05DC\u05D4\u05EA\u05D0\u05D9\u05DD \u05DC\u05EA\u05D1\u05E0\u05D9\u05EA ${I.pattern}`;
          let _ = $[I.format], N = _?.label ?? I.format, J = (_?.gender ?? "m") === "f" ? "\u05EA\u05E7\u05D9\u05E0\u05D4" : "\u05EA\u05E7\u05D9\u05DF";
          return `${N} \u05DC\u05D0 ${J}`;
        }
        case "not_multiple_of":
          return `\u05DE\u05E1\u05E4\u05E8 \u05DC\u05D0 \u05EA\u05E7\u05D9\u05DF: \u05D7\u05D9\u05D9\u05D1 \u05DC\u05D4\u05D9\u05D5\u05EA \u05DE\u05DB\u05E4\u05DC\u05D4 \u05E9\u05DC ${e.divisor}`;
        case "unrecognized_keys":
          return `\u05DE\u05E4\u05EA\u05D7${e.keys.length > 1 ? "\u05D5\u05EA" : ""} \u05DC\u05D0 \u05DE\u05D6\u05D5\u05D4${e.keys.length > 1 ? "\u05D9\u05DD" : "\u05D4"}: ${b(e.keys, ", ")}`;
        case "invalid_key":
          return "\u05E9\u05D3\u05D4 \u05DC\u05D0 \u05EA\u05E7\u05D9\u05DF \u05D1\u05D0\u05D5\u05D1\u05D9\u05D9\u05E7\u05D8";
        case "invalid_union":
          return "\u05E7\u05DC\u05D8 \u05DC\u05D0 \u05EA\u05E7\u05D9\u05DF";
        case "invalid_element":
          return `\u05E2\u05E8\u05DA \u05DC\u05D0 \u05EA\u05E7\u05D9\u05DF \u05D1${n(e.origin ?? "array")}`;
        default:
          return "\u05E7\u05DC\u05D8 \u05DC\u05D0 \u05EA\u05E7\u05D9\u05DF";
      }
    };
  };
  function pt() {
    return { localeError: w4() };
  }
  var N4 = () => {
    let r = { string: { unit: "karakter", verb: "legyen" }, file: { unit: "byte", verb: "legyen" }, array: { unit: "elem", verb: "legyen" }, set: { unit: "elem", verb: "legyen" } };
    function i(n) {
      return r[n] ?? null;
    }
    let o = { regex: "bemenet", email: "email c\xEDm", url: "URL", emoji: "emoji", uuid: "UUID", uuidv4: "UUIDv4", uuidv6: "UUIDv6", nanoid: "nanoid", guid: "GUID", cuid: "cuid", cuid2: "cuid2", ulid: "ULID", xid: "XID", ksuid: "KSUID", datetime: "ISO id\u0151b\xE9lyeg", date: "ISO d\xE1tum", time: "ISO id\u0151", duration: "ISO id\u0151intervallum", ipv4: "IPv4 c\xEDm", ipv6: "IPv6 c\xEDm", cidrv4: "IPv4 tartom\xE1ny", cidrv6: "IPv6 tartom\xE1ny", base64: "base64-k\xF3dolt string", base64url: "base64url-k\xF3dolt string", json_string: "JSON string", e164: "E.164 sz\xE1m", jwt: "JWT", template_literal: "bemenet" }, t = { nan: "NaN", number: "sz\xE1m", array: "t\xF6mb" };
    return (n) => {
      switch (n.code) {
        case "invalid_type": {
          let v = t[n.expected] ?? n.expected, u = k(n.input), $ = t[u] ?? u;
          if (/^[A-Z]/.test(n.expected))
            return `\xC9rv\xE9nytelen bemenet: a v\xE1rt \xE9rt\xE9k instanceof ${n.expected}, a kapott \xE9rt\xE9k ${$}`;
          return `\xC9rv\xE9nytelen bemenet: a v\xE1rt \xE9rt\xE9k ${v}, a kapott \xE9rt\xE9k ${$}`;
        }
        case "invalid_value":
          if (n.values.length === 1)
            return `\xC9rv\xE9nytelen bemenet: a v\xE1rt \xE9rt\xE9k ${U(n.values[0])}`;
          return `\xC9rv\xE9nytelen opci\xF3: valamelyik \xE9rt\xE9k v\xE1rt ${b(n.values, "|")}`;
        case "too_big": {
          let v = n.inclusive ? "<=" : "<", u = i(n.origin);
          if (u)
            return `T\xFAl nagy: ${n.origin ?? "\xE9rt\xE9k"} m\xE9rete t\xFAl nagy ${v}${n.maximum.toString()} ${u.unit ?? "elem"}`;
          return `T\xFAl nagy: a bemeneti \xE9rt\xE9k ${n.origin ?? "\xE9rt\xE9k"} t\xFAl nagy: ${v}${n.maximum.toString()}`;
        }
        case "too_small": {
          let v = n.inclusive ? ">=" : ">", u = i(n.origin);
          if (u)
            return `T\xFAl kicsi: a bemeneti \xE9rt\xE9k ${n.origin} m\xE9rete t\xFAl kicsi ${v}${n.minimum.toString()} ${u.unit}`;
          return `T\xFAl kicsi: a bemeneti \xE9rt\xE9k ${n.origin} t\xFAl kicsi ${v}${n.minimum.toString()}`;
        }
        case "invalid_format": {
          let v = n;
          if (v.format === "starts_with")
            return `\xC9rv\xE9nytelen string: "${v.prefix}" \xE9rt\xE9kkel kell kezd\u0151dnie`;
          if (v.format === "ends_with")
            return `\xC9rv\xE9nytelen string: "${v.suffix}" \xE9rt\xE9kkel kell v\xE9gz\u0151dnie`;
          if (v.format === "includes")
            return `\xC9rv\xE9nytelen string: "${v.includes}" \xE9rt\xE9ket kell tartalmaznia`;
          if (v.format === "regex")
            return `\xC9rv\xE9nytelen string: ${v.pattern} mint\xE1nak kell megfelelnie`;
          return `\xC9rv\xE9nytelen ${o[v.format] ?? n.format}`;
        }
        case "not_multiple_of":
          return `\xC9rv\xE9nytelen sz\xE1m: ${n.divisor} t\xF6bbsz\xF6r\xF6s\xE9nek kell lennie`;
        case "unrecognized_keys":
          return `Ismeretlen kulcs${n.keys.length > 1 ? "s" : ""}: ${b(n.keys, ", ")}`;
        case "invalid_key":
          return `\xC9rv\xE9nytelen kulcs ${n.origin}`;
        case "invalid_union":
          return "\xC9rv\xE9nytelen bemenet";
        case "invalid_element":
          return `\xC9rv\xE9nytelen \xE9rt\xE9k: ${n.origin}`;
        default:
          return "\xC9rv\xE9nytelen bemenet";
      }
    };
  };
  function st() {
    return { localeError: N4() };
  }
  function se(r, i, o) {
    return Math.abs(r) === 1 ? i : o;
  }
  function Xr(r) {
    if (!r)
      return "";
    let i = ["\u0561", "\u0565", "\u0568", "\u056B", "\u0578", "\u0578\u0582", "\u0585"], o = r[r.length - 1];
    return r + (i.includes(o) ? "\u0576" : "\u0568");
  }
  var O4 = () => {
    let r = { string: { unit: { one: "\u0576\u0577\u0561\u0576", many: "\u0576\u0577\u0561\u0576\u0576\u0565\u0580" }, verb: "\u0578\u0582\u0576\u0565\u0576\u0561\u056C" }, file: { unit: { one: "\u0562\u0561\u0575\u0569", many: "\u0562\u0561\u0575\u0569\u0565\u0580" }, verb: "\u0578\u0582\u0576\u0565\u0576\u0561\u056C" }, array: { unit: { one: "\u057F\u0561\u0580\u0580", many: "\u057F\u0561\u0580\u0580\u0565\u0580" }, verb: "\u0578\u0582\u0576\u0565\u0576\u0561\u056C" }, set: { unit: { one: "\u057F\u0561\u0580\u0580", many: "\u057F\u0561\u0580\u0580\u0565\u0580" }, verb: "\u0578\u0582\u0576\u0565\u0576\u0561\u056C" } };
    function i(n) {
      return r[n] ?? null;
    }
    let o = { regex: "\u0574\u0578\u0582\u057F\u0584", email: "\u0567\u056C. \u0570\u0561\u057D\u0581\u0565", url: "URL", emoji: "\u0567\u0574\u0578\u057B\u056B", uuid: "UUID", uuidv4: "UUIDv4", uuidv6: "UUIDv6", nanoid: "nanoid", guid: "GUID", cuid: "cuid", cuid2: "cuid2", ulid: "ULID", xid: "XID", ksuid: "KSUID", datetime: "ISO \u0561\u0574\u057D\u0561\u0569\u056B\u057E \u0587 \u056A\u0561\u0574", date: "ISO \u0561\u0574\u057D\u0561\u0569\u056B\u057E", time: "ISO \u056A\u0561\u0574", duration: "ISO \u057F\u0587\u0578\u0572\u0578\u0582\u0569\u0575\u0578\u0582\u0576", ipv4: "IPv4 \u0570\u0561\u057D\u0581\u0565", ipv6: "IPv6 \u0570\u0561\u057D\u0581\u0565", cidrv4: "IPv4 \u0574\u056B\u057B\u0561\u056F\u0561\u0575\u0584", cidrv6: "IPv6 \u0574\u056B\u057B\u0561\u056F\u0561\u0575\u0584", base64: "base64 \u0571\u0587\u0561\u0579\u0561\u0583\u0578\u057E \u057F\u0578\u0572", base64url: "base64url \u0571\u0587\u0561\u0579\u0561\u0583\u0578\u057E \u057F\u0578\u0572", json_string: "JSON \u057F\u0578\u0572", e164: "E.164 \u0570\u0561\u0574\u0561\u0580", jwt: "JWT", template_literal: "\u0574\u0578\u0582\u057F\u0584" }, t = { nan: "NaN", number: "\u0569\u056B\u057E", array: "\u0566\u0561\u0576\u0563\u057E\u0561\u056E" };
    return (n) => {
      switch (n.code) {
        case "invalid_type": {
          let v = t[n.expected] ?? n.expected, u = k(n.input), $ = t[u] ?? u;
          if (/^[A-Z]/.test(n.expected))
            return `\u054D\u056D\u0561\u056C \u0574\u0578\u0582\u057F\u0584\u0561\u0563\u0580\u0578\u0582\u0574\u2024 \u057D\u057A\u0561\u057D\u057E\u0578\u0582\u0574 \u0567\u0580 instanceof ${n.expected}, \u057D\u057F\u0561\u0581\u057E\u0565\u056C \u0567 ${$}`;
          return `\u054D\u056D\u0561\u056C \u0574\u0578\u0582\u057F\u0584\u0561\u0563\u0580\u0578\u0582\u0574\u2024 \u057D\u057A\u0561\u057D\u057E\u0578\u0582\u0574 \u0567\u0580 ${v}, \u057D\u057F\u0561\u0581\u057E\u0565\u056C \u0567 ${$}`;
        }
        case "invalid_value":
          if (n.values.length === 1)
            return `\u054D\u056D\u0561\u056C \u0574\u0578\u0582\u057F\u0584\u0561\u0563\u0580\u0578\u0582\u0574\u2024 \u057D\u057A\u0561\u057D\u057E\u0578\u0582\u0574 \u0567\u0580 ${U(n.values[1])}`;
          return `\u054D\u056D\u0561\u056C \u057F\u0561\u0580\u0562\u0565\u0580\u0561\u056F\u2024 \u057D\u057A\u0561\u057D\u057E\u0578\u0582\u0574 \u0567\u0580 \u0570\u0565\u057F\u0587\u0575\u0561\u056C\u0576\u0565\u0580\u056B\u0581 \u0574\u0565\u056F\u0568\u055D ${b(n.values, "|")}`;
        case "too_big": {
          let v = n.inclusive ? "<=" : "<", u = i(n.origin);
          if (u) {
            let $ = Number(n.maximum), l = se($, u.unit.one, u.unit.many);
            return `\u0549\u0561\u0583\u0561\u0566\u0561\u0576\u0581 \u0574\u0565\u056E \u0561\u0580\u056A\u0565\u0584\u2024 \u057D\u057A\u0561\u057D\u057E\u0578\u0582\u0574 \u0567, \u0578\u0580 ${Xr(n.origin ?? "\u0561\u0580\u056A\u0565\u0584")} \u056F\u0578\u0582\u0576\u0565\u0576\u0561 ${v}${n.maximum.toString()} ${l}`;
          }
          return `\u0549\u0561\u0583\u0561\u0566\u0561\u0576\u0581 \u0574\u0565\u056E \u0561\u0580\u056A\u0565\u0584\u2024 \u057D\u057A\u0561\u057D\u057E\u0578\u0582\u0574 \u0567, \u0578\u0580 ${Xr(n.origin ?? "\u0561\u0580\u056A\u0565\u0584")} \u056C\u056B\u0576\u056B ${v}${n.maximum.toString()}`;
        }
        case "too_small": {
          let v = n.inclusive ? ">=" : ">", u = i(n.origin);
          if (u) {
            let $ = Number(n.minimum), l = se($, u.unit.one, u.unit.many);
            return `\u0549\u0561\u0583\u0561\u0566\u0561\u0576\u0581 \u0583\u0578\u0584\u0580 \u0561\u0580\u056A\u0565\u0584\u2024 \u057D\u057A\u0561\u057D\u057E\u0578\u0582\u0574 \u0567, \u0578\u0580 ${Xr(n.origin)} \u056F\u0578\u0582\u0576\u0565\u0576\u0561 ${v}${n.minimum.toString()} ${l}`;
          }
          return `\u0549\u0561\u0583\u0561\u0566\u0561\u0576\u0581 \u0583\u0578\u0584\u0580 \u0561\u0580\u056A\u0565\u0584\u2024 \u057D\u057A\u0561\u057D\u057E\u0578\u0582\u0574 \u0567, \u0578\u0580 ${Xr(n.origin)} \u056C\u056B\u0576\u056B ${v}${n.minimum.toString()}`;
        }
        case "invalid_format": {
          let v = n;
          if (v.format === "starts_with")
            return `\u054D\u056D\u0561\u056C \u057F\u0578\u0572\u2024 \u057A\u0565\u057F\u0584 \u0567 \u057D\u056F\u057D\u057E\u056B "${v.prefix}"-\u0578\u057E`;
          if (v.format === "ends_with")
            return `\u054D\u056D\u0561\u056C \u057F\u0578\u0572\u2024 \u057A\u0565\u057F\u0584 \u0567 \u0561\u057E\u0561\u0580\u057F\u057E\u056B "${v.suffix}"-\u0578\u057E`;
          if (v.format === "includes")
            return `\u054D\u056D\u0561\u056C \u057F\u0578\u0572\u2024 \u057A\u0565\u057F\u0584 \u0567 \u057A\u0561\u0580\u0578\u0582\u0576\u0561\u056F\u056B "${v.includes}"`;
          if (v.format === "regex")
            return `\u054D\u056D\u0561\u056C \u057F\u0578\u0572\u2024 \u057A\u0565\u057F\u0584 \u0567 \u0570\u0561\u0574\u0561\u057A\u0561\u057F\u0561\u057D\u056D\u0561\u0576\u056B ${v.pattern} \u0571\u0587\u0561\u0579\u0561\u0583\u056B\u0576`;
          return `\u054D\u056D\u0561\u056C ${o[v.format] ?? n.format}`;
        }
        case "not_multiple_of":
          return `\u054D\u056D\u0561\u056C \u0569\u056B\u057E\u2024 \u057A\u0565\u057F\u0584 \u0567 \u0562\u0561\u0566\u0574\u0561\u057A\u0561\u057F\u056B\u056F \u056C\u056B\u0576\u056B ${n.divisor}-\u056B`;
        case "unrecognized_keys":
          return `\u0549\u0573\u0561\u0576\u0561\u0579\u057E\u0561\u056E \u0562\u0561\u0576\u0561\u056C\u056B${n.keys.length > 1 ? "\u0576\u0565\u0580" : ""}. ${b(n.keys, ", ")}`;
        case "invalid_key":
          return `\u054D\u056D\u0561\u056C \u0562\u0561\u0576\u0561\u056C\u056B ${Xr(n.origin)}-\u0578\u0582\u0574`;
        case "invalid_union":
          return "\u054D\u056D\u0561\u056C \u0574\u0578\u0582\u057F\u0584\u0561\u0563\u0580\u0578\u0582\u0574";
        case "invalid_element":
          return `\u054D\u056D\u0561\u056C \u0561\u0580\u056A\u0565\u0584 ${Xr(n.origin)}-\u0578\u0582\u0574`;
        default:
          return "\u054D\u056D\u0561\u056C \u0574\u0578\u0582\u057F\u0584\u0561\u0563\u0580\u0578\u0582\u0574";
      }
    };
  };
  function ru() {
    return { localeError: O4() };
  }
  var S4 = () => {
    let r = { string: { unit: "karakter", verb: "memiliki" }, file: { unit: "byte", verb: "memiliki" }, array: { unit: "item", verb: "memiliki" }, set: { unit: "item", verb: "memiliki" } };
    function i(n) {
      return r[n] ?? null;
    }
    let o = { regex: "input", email: "alamat email", url: "URL", emoji: "emoji", uuid: "UUID", uuidv4: "UUIDv4", uuidv6: "UUIDv6", nanoid: "nanoid", guid: "GUID", cuid: "cuid", cuid2: "cuid2", ulid: "ULID", xid: "XID", ksuid: "KSUID", datetime: "tanggal dan waktu format ISO", date: "tanggal format ISO", time: "jam format ISO", duration: "durasi format ISO", ipv4: "alamat IPv4", ipv6: "alamat IPv6", cidrv4: "rentang alamat IPv4", cidrv6: "rentang alamat IPv6", base64: "string dengan enkode base64", base64url: "string dengan enkode base64url", json_string: "string JSON", e164: "angka E.164", jwt: "JWT", template_literal: "input" }, t = { nan: "NaN" };
    return (n) => {
      switch (n.code) {
        case "invalid_type": {
          let v = t[n.expected] ?? n.expected, u = k(n.input), $ = t[u] ?? u;
          if (/^[A-Z]/.test(n.expected))
            return `Input tidak valid: diharapkan instanceof ${n.expected}, diterima ${$}`;
          return `Input tidak valid: diharapkan ${v}, diterima ${$}`;
        }
        case "invalid_value":
          if (n.values.length === 1)
            return `Input tidak valid: diharapkan ${U(n.values[0])}`;
          return `Pilihan tidak valid: diharapkan salah satu dari ${b(n.values, "|")}`;
        case "too_big": {
          let v = n.inclusive ? "<=" : "<", u = i(n.origin);
          if (u)
            return `Terlalu besar: diharapkan ${n.origin ?? "value"} memiliki ${v}${n.maximum.toString()} ${u.unit ?? "elemen"}`;
          return `Terlalu besar: diharapkan ${n.origin ?? "value"} menjadi ${v}${n.maximum.toString()}`;
        }
        case "too_small": {
          let v = n.inclusive ? ">=" : ">", u = i(n.origin);
          if (u)
            return `Terlalu kecil: diharapkan ${n.origin} memiliki ${v}${n.minimum.toString()} ${u.unit}`;
          return `Terlalu kecil: diharapkan ${n.origin} menjadi ${v}${n.minimum.toString()}`;
        }
        case "invalid_format": {
          let v = n;
          if (v.format === "starts_with")
            return `String tidak valid: harus dimulai dengan "${v.prefix}"`;
          if (v.format === "ends_with")
            return `String tidak valid: harus berakhir dengan "${v.suffix}"`;
          if (v.format === "includes")
            return `String tidak valid: harus menyertakan "${v.includes}"`;
          if (v.format === "regex")
            return `String tidak valid: harus sesuai pola ${v.pattern}`;
          return `${o[v.format] ?? n.format} tidak valid`;
        }
        case "not_multiple_of":
          return `Angka tidak valid: harus kelipatan dari ${n.divisor}`;
        case "unrecognized_keys":
          return `Kunci tidak dikenali ${n.keys.length > 1 ? "s" : ""}: ${b(n.keys, ", ")}`;
        case "invalid_key":
          return `Kunci tidak valid di ${n.origin}`;
        case "invalid_union":
          return "Input tidak valid";
        case "invalid_element":
          return `Nilai tidak valid di ${n.origin}`;
        default:
          return "Input tidak valid";
      }
    };
  };
  function nu() {
    return { localeError: S4() };
  }
  var z4 = () => {
    let r = { string: { unit: "stafi", verb: "a\xF0 hafa" }, file: { unit: "b\xE6ti", verb: "a\xF0 hafa" }, array: { unit: "hluti", verb: "a\xF0 hafa" }, set: { unit: "hluti", verb: "a\xF0 hafa" } };
    function i(n) {
      return r[n] ?? null;
    }
    let o = { regex: "gildi", email: "netfang", url: "vefsl\xF3\xF0", emoji: "emoji", uuid: "UUID", uuidv4: "UUIDv4", uuidv6: "UUIDv6", nanoid: "nanoid", guid: "GUID", cuid: "cuid", cuid2: "cuid2", ulid: "ULID", xid: "XID", ksuid: "KSUID", datetime: "ISO dagsetning og t\xEDmi", date: "ISO dagsetning", time: "ISO t\xEDmi", duration: "ISO t\xEDmalengd", ipv4: "IPv4 address", ipv6: "IPv6 address", cidrv4: "IPv4 range", cidrv6: "IPv6 range", base64: "base64-encoded strengur", base64url: "base64url-encoded strengur", json_string: "JSON strengur", e164: "E.164 t\xF6lugildi", jwt: "JWT", template_literal: "gildi" }, t = { nan: "NaN", number: "n\xFAmer", array: "fylki" };
    return (n) => {
      switch (n.code) {
        case "invalid_type": {
          let v = t[n.expected] ?? n.expected, u = k(n.input), $ = t[u] ?? u;
          if (/^[A-Z]/.test(n.expected))
            return `Rangt gildi: \xDE\xFA sl\xF3st inn ${$} \xFEar sem \xE1 a\xF0 vera instanceof ${n.expected}`;
          return `Rangt gildi: \xDE\xFA sl\xF3st inn ${$} \xFEar sem \xE1 a\xF0 vera ${v}`;
        }
        case "invalid_value":
          if (n.values.length === 1)
            return `Rangt gildi: gert r\xE1\xF0 fyrir ${U(n.values[0])}`;
          return `\xD3gilt val: m\xE1 vera eitt af eftirfarandi ${b(n.values, "|")}`;
        case "too_big": {
          let v = n.inclusive ? "<=" : "<", u = i(n.origin);
          if (u)
            return `Of st\xF3rt: gert er r\xE1\xF0 fyrir a\xF0 ${n.origin ?? "gildi"} hafi ${v}${n.maximum.toString()} ${u.unit ?? "hluti"}`;
          return `Of st\xF3rt: gert er r\xE1\xF0 fyrir a\xF0 ${n.origin ?? "gildi"} s\xE9 ${v}${n.maximum.toString()}`;
        }
        case "too_small": {
          let v = n.inclusive ? ">=" : ">", u = i(n.origin);
          if (u)
            return `Of l\xEDti\xF0: gert er r\xE1\xF0 fyrir a\xF0 ${n.origin} hafi ${v}${n.minimum.toString()} ${u.unit}`;
          return `Of l\xEDti\xF0: gert er r\xE1\xF0 fyrir a\xF0 ${n.origin} s\xE9 ${v}${n.minimum.toString()}`;
        }
        case "invalid_format": {
          let v = n;
          if (v.format === "starts_with")
            return `\xD3gildur strengur: ver\xF0ur a\xF0 byrja \xE1 "${v.prefix}"`;
          if (v.format === "ends_with")
            return `\xD3gildur strengur: ver\xF0ur a\xF0 enda \xE1 "${v.suffix}"`;
          if (v.format === "includes")
            return `\xD3gildur strengur: ver\xF0ur a\xF0 innihalda "${v.includes}"`;
          if (v.format === "regex")
            return `\xD3gildur strengur: ver\xF0ur a\xF0 fylgja mynstri ${v.pattern}`;
          return `Rangt ${o[v.format] ?? n.format}`;
        }
        case "not_multiple_of":
          return `R\xF6ng tala: ver\xF0ur a\xF0 vera margfeldi af ${n.divisor}`;
        case "unrecognized_keys":
          return `\xD3\xFEekkt ${n.keys.length > 1 ? "ir lyklar" : "ur lykill"}: ${b(n.keys, ", ")}`;
        case "invalid_key":
          return `Rangur lykill \xED ${n.origin}`;
        case "invalid_union":
          return "Rangt gildi";
        case "invalid_element":
          return `Rangt gildi \xED ${n.origin}`;
        default:
          return "Rangt gildi";
      }
    };
  };
  function iu() {
    return { localeError: z4() };
  }
  var P4 = () => {
    let r = { string: { unit: "caratteri", verb: "avere" }, file: { unit: "byte", verb: "avere" }, array: { unit: "elementi", verb: "avere" }, set: { unit: "elementi", verb: "avere" } };
    function i(n) {
      return r[n] ?? null;
    }
    let o = { regex: "input", email: "indirizzo email", url: "URL", emoji: "emoji", uuid: "UUID", uuidv4: "UUIDv4", uuidv6: "UUIDv6", nanoid: "nanoid", guid: "GUID", cuid: "cuid", cuid2: "cuid2", ulid: "ULID", xid: "XID", ksuid: "KSUID", datetime: "data e ora ISO", date: "data ISO", time: "ora ISO", duration: "durata ISO", ipv4: "indirizzo IPv4", ipv6: "indirizzo IPv6", cidrv4: "intervallo IPv4", cidrv6: "intervallo IPv6", base64: "stringa codificata in base64", base64url: "URL codificata in base64", json_string: "stringa JSON", e164: "numero E.164", jwt: "JWT", template_literal: "input" }, t = { nan: "NaN", number: "numero", array: "vettore" };
    return (n) => {
      switch (n.code) {
        case "invalid_type": {
          let v = t[n.expected] ?? n.expected, u = k(n.input), $ = t[u] ?? u;
          if (/^[A-Z]/.test(n.expected))
            return `Input non valido: atteso instanceof ${n.expected}, ricevuto ${$}`;
          return `Input non valido: atteso ${v}, ricevuto ${$}`;
        }
        case "invalid_value":
          if (n.values.length === 1)
            return `Input non valido: atteso ${U(n.values[0])}`;
          return `Opzione non valida: atteso uno tra ${b(n.values, "|")}`;
        case "too_big": {
          let v = n.inclusive ? "<=" : "<", u = i(n.origin);
          if (u)
            return `Troppo grande: ${n.origin ?? "valore"} deve avere ${v}${n.maximum.toString()} ${u.unit ?? "elementi"}`;
          return `Troppo grande: ${n.origin ?? "valore"} deve essere ${v}${n.maximum.toString()}`;
        }
        case "too_small": {
          let v = n.inclusive ? ">=" : ">", u = i(n.origin);
          if (u)
            return `Troppo piccolo: ${n.origin} deve avere ${v}${n.minimum.toString()} ${u.unit}`;
          return `Troppo piccolo: ${n.origin} deve essere ${v}${n.minimum.toString()}`;
        }
        case "invalid_format": {
          let v = n;
          if (v.format === "starts_with")
            return `Stringa non valida: deve iniziare con "${v.prefix}"`;
          if (v.format === "ends_with")
            return `Stringa non valida: deve terminare con "${v.suffix}"`;
          if (v.format === "includes")
            return `Stringa non valida: deve includere "${v.includes}"`;
          if (v.format === "regex")
            return `Stringa non valida: deve corrispondere al pattern ${v.pattern}`;
          return `Invalid ${o[v.format] ?? n.format}`;
        }
        case "not_multiple_of":
          return `Numero non valido: deve essere un multiplo di ${n.divisor}`;
        case "unrecognized_keys":
          return `Chiav${n.keys.length > 1 ? "i" : "e"} non riconosciut${n.keys.length > 1 ? "e" : "a"}: ${b(n.keys, ", ")}`;
        case "invalid_key":
          return `Chiave non valida in ${n.origin}`;
        case "invalid_union":
          return "Input non valido";
        case "invalid_element":
          return `Valore non valido in ${n.origin}`;
        default:
          return "Input non valido";
      }
    };
  };
  function vu() {
    return { localeError: P4() };
  }
  var j4 = () => {
    let r = { string: { unit: "\u6587\u5B57", verb: "\u3067\u3042\u308B" }, file: { unit: "\u30D0\u30A4\u30C8", verb: "\u3067\u3042\u308B" }, array: { unit: "\u8981\u7D20", verb: "\u3067\u3042\u308B" }, set: { unit: "\u8981\u7D20", verb: "\u3067\u3042\u308B" } };
    function i(n) {
      return r[n] ?? null;
    }
    let o = { regex: "\u5165\u529B\u5024", email: "\u30E1\u30FC\u30EB\u30A2\u30C9\u30EC\u30B9", url: "URL", emoji: "\u7D75\u6587\u5B57", uuid: "UUID", uuidv4: "UUIDv4", uuidv6: "UUIDv6", nanoid: "nanoid", guid: "GUID", cuid: "cuid", cuid2: "cuid2", ulid: "ULID", xid: "XID", ksuid: "KSUID", datetime: "ISO\u65E5\u6642", date: "ISO\u65E5\u4ED8", time: "ISO\u6642\u523B", duration: "ISO\u671F\u9593", ipv4: "IPv4\u30A2\u30C9\u30EC\u30B9", ipv6: "IPv6\u30A2\u30C9\u30EC\u30B9", cidrv4: "IPv4\u7BC4\u56F2", cidrv6: "IPv6\u7BC4\u56F2", base64: "base64\u30A8\u30F3\u30B3\u30FC\u30C9\u6587\u5B57\u5217", base64url: "base64url\u30A8\u30F3\u30B3\u30FC\u30C9\u6587\u5B57\u5217", json_string: "JSON\u6587\u5B57\u5217", e164: "E.164\u756A\u53F7", jwt: "JWT", template_literal: "\u5165\u529B\u5024" }, t = { nan: "NaN", number: "\u6570\u5024", array: "\u914D\u5217" };
    return (n) => {
      switch (n.code) {
        case "invalid_type": {
          let v = t[n.expected] ?? n.expected, u = k(n.input), $ = t[u] ?? u;
          if (/^[A-Z]/.test(n.expected))
            return `\u7121\u52B9\u306A\u5165\u529B: instanceof ${n.expected}\u304C\u671F\u5F85\u3055\u308C\u307E\u3057\u305F\u304C\u3001${$}\u304C\u5165\u529B\u3055\u308C\u307E\u3057\u305F`;
          return `\u7121\u52B9\u306A\u5165\u529B: ${v}\u304C\u671F\u5F85\u3055\u308C\u307E\u3057\u305F\u304C\u3001${$}\u304C\u5165\u529B\u3055\u308C\u307E\u3057\u305F`;
        }
        case "invalid_value":
          if (n.values.length === 1)
            return `\u7121\u52B9\u306A\u5165\u529B: ${U(n.values[0])}\u304C\u671F\u5F85\u3055\u308C\u307E\u3057\u305F`;
          return `\u7121\u52B9\u306A\u9078\u629E: ${b(n.values, "\u3001")}\u306E\u3044\u305A\u308C\u304B\u3067\u3042\u308B\u5FC5\u8981\u304C\u3042\u308A\u307E\u3059`;
        case "too_big": {
          let v = n.inclusive ? "\u4EE5\u4E0B\u3067\u3042\u308B" : "\u3088\u308A\u5C0F\u3055\u3044", u = i(n.origin);
          if (u)
            return `\u5927\u304D\u3059\u304E\u308B\u5024: ${n.origin ?? "\u5024"}\u306F${n.maximum.toString()}${u.unit ?? "\u8981\u7D20"}${v}\u5FC5\u8981\u304C\u3042\u308A\u307E\u3059`;
          return `\u5927\u304D\u3059\u304E\u308B\u5024: ${n.origin ?? "\u5024"}\u306F${n.maximum.toString()}${v}\u5FC5\u8981\u304C\u3042\u308A\u307E\u3059`;
        }
        case "too_small": {
          let v = n.inclusive ? "\u4EE5\u4E0A\u3067\u3042\u308B" : "\u3088\u308A\u5927\u304D\u3044", u = i(n.origin);
          if (u)
            return `\u5C0F\u3055\u3059\u304E\u308B\u5024: ${n.origin}\u306F${n.minimum.toString()}${u.unit}${v}\u5FC5\u8981\u304C\u3042\u308A\u307E\u3059`;
          return `\u5C0F\u3055\u3059\u304E\u308B\u5024: ${n.origin}\u306F${n.minimum.toString()}${v}\u5FC5\u8981\u304C\u3042\u308A\u307E\u3059`;
        }
        case "invalid_format": {
          let v = n;
          if (v.format === "starts_with")
            return `\u7121\u52B9\u306A\u6587\u5B57\u5217: "${v.prefix}"\u3067\u59CB\u307E\u308B\u5FC5\u8981\u304C\u3042\u308A\u307E\u3059`;
          if (v.format === "ends_with")
            return `\u7121\u52B9\u306A\u6587\u5B57\u5217: "${v.suffix}"\u3067\u7D42\u308F\u308B\u5FC5\u8981\u304C\u3042\u308A\u307E\u3059`;
          if (v.format === "includes")
            return `\u7121\u52B9\u306A\u6587\u5B57\u5217: "${v.includes}"\u3092\u542B\u3080\u5FC5\u8981\u304C\u3042\u308A\u307E\u3059`;
          if (v.format === "regex")
            return `\u7121\u52B9\u306A\u6587\u5B57\u5217: \u30D1\u30BF\u30FC\u30F3${v.pattern}\u306B\u4E00\u81F4\u3059\u308B\u5FC5\u8981\u304C\u3042\u308A\u307E\u3059`;
          return `\u7121\u52B9\u306A${o[v.format] ?? n.format}`;
        }
        case "not_multiple_of":
          return `\u7121\u52B9\u306A\u6570\u5024: ${n.divisor}\u306E\u500D\u6570\u3067\u3042\u308B\u5FC5\u8981\u304C\u3042\u308A\u307E\u3059`;
        case "unrecognized_keys":
          return `\u8A8D\u8B58\u3055\u308C\u3066\u3044\u306A\u3044\u30AD\u30FC${n.keys.length > 1 ? "\u7FA4" : ""}: ${b(n.keys, "\u3001")}`;
        case "invalid_key":
          return `${n.origin}\u5185\u306E\u7121\u52B9\u306A\u30AD\u30FC`;
        case "invalid_union":
          return "\u7121\u52B9\u306A\u5165\u529B";
        case "invalid_element":
          return `${n.origin}\u5185\u306E\u7121\u52B9\u306A\u5024`;
        default:
          return "\u7121\u52B9\u306A\u5165\u529B";
      }
    };
  };
  function ou() {
    return { localeError: j4() };
  }
  var J4 = () => {
    let r = { string: { unit: "\u10E1\u10D8\u10DB\u10D1\u10DD\u10DA\u10DD", verb: "\u10E3\u10DC\u10D3\u10D0 \u10E8\u10D4\u10D8\u10EA\u10D0\u10D5\u10D3\u10D4\u10E1" }, file: { unit: "\u10D1\u10D0\u10D8\u10E2\u10D8", verb: "\u10E3\u10DC\u10D3\u10D0 \u10E8\u10D4\u10D8\u10EA\u10D0\u10D5\u10D3\u10D4\u10E1" }, array: { unit: "\u10D4\u10DA\u10D4\u10DB\u10D4\u10DC\u10E2\u10D8", verb: "\u10E3\u10DC\u10D3\u10D0 \u10E8\u10D4\u10D8\u10EA\u10D0\u10D5\u10D3\u10D4\u10E1" }, set: { unit: "\u10D4\u10DA\u10D4\u10DB\u10D4\u10DC\u10E2\u10D8", verb: "\u10E3\u10DC\u10D3\u10D0 \u10E8\u10D4\u10D8\u10EA\u10D0\u10D5\u10D3\u10D4\u10E1" } };
    function i(n) {
      return r[n] ?? null;
    }
    let o = { regex: "\u10E8\u10D4\u10E7\u10D5\u10D0\u10DC\u10D0", email: "\u10D4\u10DA-\u10E4\u10DD\u10E1\u10E2\u10D8\u10E1 \u10DB\u10D8\u10E1\u10D0\u10DB\u10D0\u10E0\u10D7\u10D8", url: "URL", emoji: "\u10D4\u10DB\u10DD\u10EF\u10D8", uuid: "UUID", uuidv4: "UUIDv4", uuidv6: "UUIDv6", nanoid: "nanoid", guid: "GUID", cuid: "cuid", cuid2: "cuid2", ulid: "ULID", xid: "XID", ksuid: "KSUID", datetime: "\u10D7\u10D0\u10E0\u10D8\u10E6\u10D8-\u10D3\u10E0\u10DD", date: "\u10D7\u10D0\u10E0\u10D8\u10E6\u10D8", time: "\u10D3\u10E0\u10DD", duration: "\u10EE\u10D0\u10DC\u10D2\u10E0\u10EB\u10DA\u10D8\u10D5\u10DD\u10D1\u10D0", ipv4: "IPv4 \u10DB\u10D8\u10E1\u10D0\u10DB\u10D0\u10E0\u10D7\u10D8", ipv6: "IPv6 \u10DB\u10D8\u10E1\u10D0\u10DB\u10D0\u10E0\u10D7\u10D8", cidrv4: "IPv4 \u10D3\u10D8\u10D0\u10DE\u10D0\u10D6\u10DD\u10DC\u10D8", cidrv6: "IPv6 \u10D3\u10D8\u10D0\u10DE\u10D0\u10D6\u10DD\u10DC\u10D8", base64: "base64-\u10D9\u10DD\u10D3\u10D8\u10E0\u10D4\u10D1\u10E3\u10DA\u10D8 \u10E1\u10E2\u10E0\u10D8\u10DC\u10D2\u10D8", base64url: "base64url-\u10D9\u10DD\u10D3\u10D8\u10E0\u10D4\u10D1\u10E3\u10DA\u10D8 \u10E1\u10E2\u10E0\u10D8\u10DC\u10D2\u10D8", json_string: "JSON \u10E1\u10E2\u10E0\u10D8\u10DC\u10D2\u10D8", e164: "E.164 \u10DC\u10DD\u10DB\u10D4\u10E0\u10D8", jwt: "JWT", template_literal: "\u10E8\u10D4\u10E7\u10D5\u10D0\u10DC\u10D0" }, t = { nan: "NaN", number: "\u10E0\u10D8\u10EA\u10EE\u10D5\u10D8", string: "\u10E1\u10E2\u10E0\u10D8\u10DC\u10D2\u10D8", boolean: "\u10D1\u10E3\u10DA\u10D4\u10D0\u10DC\u10D8", function: "\u10E4\u10E3\u10DC\u10E5\u10EA\u10D8\u10D0", array: "\u10DB\u10D0\u10E1\u10D8\u10D5\u10D8" };
    return (n) => {
      switch (n.code) {
        case "invalid_type": {
          let v = t[n.expected] ?? n.expected, u = k(n.input), $ = t[u] ?? u;
          if (/^[A-Z]/.test(n.expected))
            return `\u10D0\u10E0\u10D0\u10E1\u10EC\u10DD\u10E0\u10D8 \u10E8\u10D4\u10E7\u10D5\u10D0\u10DC\u10D0: \u10DB\u10DD\u10E1\u10D0\u10DA\u10DD\u10D3\u10DC\u10D4\u10DA\u10D8 instanceof ${n.expected}, \u10DB\u10D8\u10E6\u10D4\u10D1\u10E3\u10DA\u10D8 ${$}`;
          return `\u10D0\u10E0\u10D0\u10E1\u10EC\u10DD\u10E0\u10D8 \u10E8\u10D4\u10E7\u10D5\u10D0\u10DC\u10D0: \u10DB\u10DD\u10E1\u10D0\u10DA\u10DD\u10D3\u10DC\u10D4\u10DA\u10D8 ${v}, \u10DB\u10D8\u10E6\u10D4\u10D1\u10E3\u10DA\u10D8 ${$}`;
        }
        case "invalid_value":
          if (n.values.length === 1)
            return `\u10D0\u10E0\u10D0\u10E1\u10EC\u10DD\u10E0\u10D8 \u10E8\u10D4\u10E7\u10D5\u10D0\u10DC\u10D0: \u10DB\u10DD\u10E1\u10D0\u10DA\u10DD\u10D3\u10DC\u10D4\u10DA\u10D8 ${U(n.values[0])}`;
          return `\u10D0\u10E0\u10D0\u10E1\u10EC\u10DD\u10E0\u10D8 \u10D5\u10D0\u10E0\u10D8\u10D0\u10DC\u10E2\u10D8: \u10DB\u10DD\u10E1\u10D0\u10DA\u10DD\u10D3\u10DC\u10D4\u10DA\u10D8\u10D0 \u10D4\u10E0\u10D7-\u10D4\u10E0\u10D7\u10D8 ${b(n.values, "|")}-\u10D3\u10D0\u10DC`;
        case "too_big": {
          let v = n.inclusive ? "<=" : "<", u = i(n.origin);
          if (u)
            return `\u10D6\u10D4\u10D3\u10DB\u10D4\u10E2\u10D0\u10D3 \u10D3\u10D8\u10D3\u10D8: \u10DB\u10DD\u10E1\u10D0\u10DA\u10DD\u10D3\u10DC\u10D4\u10DA\u10D8 ${n.origin ?? "\u10DB\u10DC\u10D8\u10E8\u10D5\u10DC\u10D4\u10DA\u10DD\u10D1\u10D0"} ${u.verb} ${v}${n.maximum.toString()} ${u.unit}`;
          return `\u10D6\u10D4\u10D3\u10DB\u10D4\u10E2\u10D0\u10D3 \u10D3\u10D8\u10D3\u10D8: \u10DB\u10DD\u10E1\u10D0\u10DA\u10DD\u10D3\u10DC\u10D4\u10DA\u10D8 ${n.origin ?? "\u10DB\u10DC\u10D8\u10E8\u10D5\u10DC\u10D4\u10DA\u10DD\u10D1\u10D0"} \u10D8\u10E7\u10DD\u10E1 ${v}${n.maximum.toString()}`;
        }
        case "too_small": {
          let v = n.inclusive ? ">=" : ">", u = i(n.origin);
          if (u)
            return `\u10D6\u10D4\u10D3\u10DB\u10D4\u10E2\u10D0\u10D3 \u10DE\u10D0\u10E2\u10D0\u10E0\u10D0: \u10DB\u10DD\u10E1\u10D0\u10DA\u10DD\u10D3\u10DC\u10D4\u10DA\u10D8 ${n.origin} ${u.verb} ${v}${n.minimum.toString()} ${u.unit}`;
          return `\u10D6\u10D4\u10D3\u10DB\u10D4\u10E2\u10D0\u10D3 \u10DE\u10D0\u10E2\u10D0\u10E0\u10D0: \u10DB\u10DD\u10E1\u10D0\u10DA\u10DD\u10D3\u10DC\u10D4\u10DA\u10D8 ${n.origin} \u10D8\u10E7\u10DD\u10E1 ${v}${n.minimum.toString()}`;
        }
        case "invalid_format": {
          let v = n;
          if (v.format === "starts_with")
            return `\u10D0\u10E0\u10D0\u10E1\u10EC\u10DD\u10E0\u10D8 \u10E1\u10E2\u10E0\u10D8\u10DC\u10D2\u10D8: \u10E3\u10DC\u10D3\u10D0 \u10D8\u10EC\u10E7\u10D4\u10D1\u10DD\u10D3\u10D4\u10E1 "${v.prefix}"-\u10D8\u10D7`;
          if (v.format === "ends_with")
            return `\u10D0\u10E0\u10D0\u10E1\u10EC\u10DD\u10E0\u10D8 \u10E1\u10E2\u10E0\u10D8\u10DC\u10D2\u10D8: \u10E3\u10DC\u10D3\u10D0 \u10DB\u10D7\u10D0\u10D5\u10E0\u10D3\u10D4\u10D1\u10DD\u10D3\u10D4\u10E1 "${v.suffix}"-\u10D8\u10D7`;
          if (v.format === "includes")
            return `\u10D0\u10E0\u10D0\u10E1\u10EC\u10DD\u10E0\u10D8 \u10E1\u10E2\u10E0\u10D8\u10DC\u10D2\u10D8: \u10E3\u10DC\u10D3\u10D0 \u10E8\u10D4\u10D8\u10EA\u10D0\u10D5\u10D3\u10D4\u10E1 "${v.includes}"-\u10E1`;
          if (v.format === "regex")
            return `\u10D0\u10E0\u10D0\u10E1\u10EC\u10DD\u10E0\u10D8 \u10E1\u10E2\u10E0\u10D8\u10DC\u10D2\u10D8: \u10E3\u10DC\u10D3\u10D0 \u10E8\u10D4\u10D4\u10E1\u10D0\u10D1\u10D0\u10DB\u10D4\u10D1\u10DD\u10D3\u10D4\u10E1 \u10E8\u10D0\u10D1\u10DA\u10DD\u10DC\u10E1 ${v.pattern}`;
          return `\u10D0\u10E0\u10D0\u10E1\u10EC\u10DD\u10E0\u10D8 ${o[v.format] ?? n.format}`;
        }
        case "not_multiple_of":
          return `\u10D0\u10E0\u10D0\u10E1\u10EC\u10DD\u10E0\u10D8 \u10E0\u10D8\u10EA\u10EE\u10D5\u10D8: \u10E3\u10DC\u10D3\u10D0 \u10D8\u10E7\u10DD\u10E1 ${n.divisor}-\u10D8\u10E1 \u10EF\u10D4\u10E0\u10D0\u10D3\u10D8`;
        case "unrecognized_keys":
          return `\u10E3\u10EA\u10DC\u10DD\u10D1\u10D8 \u10D2\u10D0\u10E1\u10D0\u10E6\u10D4\u10D1${n.keys.length > 1 ? "\u10D4\u10D1\u10D8" : "\u10D8"}: ${b(n.keys, ", ")}`;
        case "invalid_key":
          return `\u10D0\u10E0\u10D0\u10E1\u10EC\u10DD\u10E0\u10D8 \u10D2\u10D0\u10E1\u10D0\u10E6\u10D4\u10D1\u10D8 ${n.origin}-\u10E8\u10D8`;
        case "invalid_union":
          return "\u10D0\u10E0\u10D0\u10E1\u10EC\u10DD\u10E0\u10D8 \u10E8\u10D4\u10E7\u10D5\u10D0\u10DC\u10D0";
        case "invalid_element":
          return `\u10D0\u10E0\u10D0\u10E1\u10EC\u10DD\u10E0\u10D8 \u10DB\u10DC\u10D8\u10E8\u10D5\u10DC\u10D4\u10DA\u10DD\u10D1\u10D0 ${n.origin}-\u10E8\u10D8`;
        default:
          return "\u10D0\u10E0\u10D0\u10E1\u10EC\u10DD\u10E0\u10D8 \u10E8\u10D4\u10E7\u10D5\u10D0\u10DC\u10D0";
      }
    };
  };
  function tu() {
    return { localeError: J4() };
  }
  var L4 = () => {
    let r = { string: { unit: "\u178F\u17BD\u17A2\u1780\u17D2\u179F\u179A", verb: "\u1782\u17BD\u179A\u1798\u17B6\u1793" }, file: { unit: "\u1794\u17C3", verb: "\u1782\u17BD\u179A\u1798\u17B6\u1793" }, array: { unit: "\u1792\u17B6\u178F\u17BB", verb: "\u1782\u17BD\u179A\u1798\u17B6\u1793" }, set: { unit: "\u1792\u17B6\u178F\u17BB", verb: "\u1782\u17BD\u179A\u1798\u17B6\u1793" } };
    function i(n) {
      return r[n] ?? null;
    }
    let o = { regex: "\u1791\u17B7\u1793\u17D2\u1793\u1793\u17D0\u1799\u1794\u1789\u17D2\u1785\u17BC\u179B", email: "\u17A2\u17B6\u179F\u1799\u178A\u17D2\u178B\u17B6\u1793\u17A2\u17CA\u17B8\u1798\u17C2\u179B", url: "URL", emoji: "\u179F\u1789\u17D2\u1789\u17B6\u17A2\u17B6\u179A\u1798\u17D2\u1798\u178E\u17CD", uuid: "UUID", uuidv4: "UUIDv4", uuidv6: "UUIDv6", nanoid: "nanoid", guid: "GUID", cuid: "cuid", cuid2: "cuid2", ulid: "ULID", xid: "XID", ksuid: "KSUID", datetime: "\u1780\u17B6\u179B\u1794\u179A\u17B7\u1785\u17D2\u1786\u17C1\u1791 \u1793\u17B7\u1784\u1798\u17C9\u17C4\u1784 ISO", date: "\u1780\u17B6\u179B\u1794\u179A\u17B7\u1785\u17D2\u1786\u17C1\u1791 ISO", time: "\u1798\u17C9\u17C4\u1784 ISO", duration: "\u179A\u1799\u17C8\u1796\u17C1\u179B ISO", ipv4: "\u17A2\u17B6\u179F\u1799\u178A\u17D2\u178B\u17B6\u1793 IPv4", ipv6: "\u17A2\u17B6\u179F\u1799\u178A\u17D2\u178B\u17B6\u1793 IPv6", cidrv4: "\u178A\u17C2\u1793\u17A2\u17B6\u179F\u1799\u178A\u17D2\u178B\u17B6\u1793 IPv4", cidrv6: "\u178A\u17C2\u1793\u17A2\u17B6\u179F\u1799\u178A\u17D2\u178B\u17B6\u1793 IPv6", base64: "\u1781\u17D2\u179F\u17C2\u17A2\u1780\u17D2\u179F\u179A\u17A2\u17CA\u17B7\u1780\u17BC\u178A base64", base64url: "\u1781\u17D2\u179F\u17C2\u17A2\u1780\u17D2\u179F\u179A\u17A2\u17CA\u17B7\u1780\u17BC\u178A base64url", json_string: "\u1781\u17D2\u179F\u17C2\u17A2\u1780\u17D2\u179F\u179A JSON", e164: "\u179B\u17C1\u1781 E.164", jwt: "JWT", template_literal: "\u1791\u17B7\u1793\u17D2\u1793\u1793\u17D0\u1799\u1794\u1789\u17D2\u1785\u17BC\u179B" }, t = { nan: "NaN", number: "\u179B\u17C1\u1781", array: "\u17A2\u17B6\u179A\u17C1 (Array)", null: "\u1782\u17D2\u1798\u17B6\u1793\u178F\u1798\u17D2\u179B\u17C3 (null)" };
    return (n) => {
      switch (n.code) {
        case "invalid_type": {
          let v = t[n.expected] ?? n.expected, u = k(n.input), $ = t[u] ?? u;
          if (/^[A-Z]/.test(n.expected))
            return `\u1791\u17B7\u1793\u17D2\u1793\u1793\u17D0\u1799\u1794\u1789\u17D2\u1785\u17BC\u179B\u1798\u17B7\u1793\u178F\u17D2\u179A\u17B9\u1798\u178F\u17D2\u179A\u17BC\u179C\u17D6 \u178F\u17D2\u179A\u17BC\u179C\u1780\u17B6\u179A instanceof ${n.expected} \u1794\u17C9\u17BB\u1793\u17D2\u178F\u17C2\u1791\u1791\u17BD\u179B\u1794\u17B6\u1793 ${$}`;
          return `\u1791\u17B7\u1793\u17D2\u1793\u1793\u17D0\u1799\u1794\u1789\u17D2\u1785\u17BC\u179B\u1798\u17B7\u1793\u178F\u17D2\u179A\u17B9\u1798\u178F\u17D2\u179A\u17BC\u179C\u17D6 \u178F\u17D2\u179A\u17BC\u179C\u1780\u17B6\u179A ${v} \u1794\u17C9\u17BB\u1793\u17D2\u178F\u17C2\u1791\u1791\u17BD\u179B\u1794\u17B6\u1793 ${$}`;
        }
        case "invalid_value":
          if (n.values.length === 1)
            return `\u1791\u17B7\u1793\u17D2\u1793\u1793\u17D0\u1799\u1794\u1789\u17D2\u1785\u17BC\u179B\u1798\u17B7\u1793\u178F\u17D2\u179A\u17B9\u1798\u178F\u17D2\u179A\u17BC\u179C\u17D6 \u178F\u17D2\u179A\u17BC\u179C\u1780\u17B6\u179A ${U(n.values[0])}`;
          return `\u1787\u1798\u17D2\u179A\u17BE\u179F\u1798\u17B7\u1793\u178F\u17D2\u179A\u17B9\u1798\u178F\u17D2\u179A\u17BC\u179C\u17D6 \u178F\u17D2\u179A\u17BC\u179C\u1787\u17B6\u1798\u17BD\u1799\u1780\u17D2\u1793\u17BB\u1784\u1785\u17C6\u178E\u17C4\u1798 ${b(n.values, "|")}`;
        case "too_big": {
          let v = n.inclusive ? "<=" : "<", u = i(n.origin);
          if (u)
            return `\u1792\u17C6\u1796\u17C1\u1780\u17D6 \u178F\u17D2\u179A\u17BC\u179C\u1780\u17B6\u179A ${n.origin ?? "\u178F\u1798\u17D2\u179B\u17C3"} ${v} ${n.maximum.toString()} ${u.unit ?? "\u1792\u17B6\u178F\u17BB"}`;
          return `\u1792\u17C6\u1796\u17C1\u1780\u17D6 \u178F\u17D2\u179A\u17BC\u179C\u1780\u17B6\u179A ${n.origin ?? "\u178F\u1798\u17D2\u179B\u17C3"} ${v} ${n.maximum.toString()}`;
        }
        case "too_small": {
          let v = n.inclusive ? ">=" : ">", u = i(n.origin);
          if (u)
            return `\u178F\u17BC\u1785\u1796\u17C1\u1780\u17D6 \u178F\u17D2\u179A\u17BC\u179C\u1780\u17B6\u179A ${n.origin} ${v} ${n.minimum.toString()} ${u.unit}`;
          return `\u178F\u17BC\u1785\u1796\u17C1\u1780\u17D6 \u178F\u17D2\u179A\u17BC\u179C\u1780\u17B6\u179A ${n.origin} ${v} ${n.minimum.toString()}`;
        }
        case "invalid_format": {
          let v = n;
          if (v.format === "starts_with")
            return `\u1781\u17D2\u179F\u17C2\u17A2\u1780\u17D2\u179F\u179A\u1798\u17B7\u1793\u178F\u17D2\u179A\u17B9\u1798\u178F\u17D2\u179A\u17BC\u179C\u17D6 \u178F\u17D2\u179A\u17BC\u179C\u1785\u17B6\u1794\u17CB\u1795\u17D2\u178F\u17BE\u1798\u178A\u17C4\u1799 "${v.prefix}"`;
          if (v.format === "ends_with")
            return `\u1781\u17D2\u179F\u17C2\u17A2\u1780\u17D2\u179F\u179A\u1798\u17B7\u1793\u178F\u17D2\u179A\u17B9\u1798\u178F\u17D2\u179A\u17BC\u179C\u17D6 \u178F\u17D2\u179A\u17BC\u179C\u1794\u1789\u17D2\u1785\u1794\u17CB\u178A\u17C4\u1799 "${v.suffix}"`;
          if (v.format === "includes")
            return `\u1781\u17D2\u179F\u17C2\u17A2\u1780\u17D2\u179F\u179A\u1798\u17B7\u1793\u178F\u17D2\u179A\u17B9\u1798\u178F\u17D2\u179A\u17BC\u179C\u17D6 \u178F\u17D2\u179A\u17BC\u179C\u1798\u17B6\u1793 "${v.includes}"`;
          if (v.format === "regex")
            return `\u1781\u17D2\u179F\u17C2\u17A2\u1780\u17D2\u179F\u179A\u1798\u17B7\u1793\u178F\u17D2\u179A\u17B9\u1798\u178F\u17D2\u179A\u17BC\u179C\u17D6 \u178F\u17D2\u179A\u17BC\u179C\u178F\u17C2\u1795\u17D2\u1782\u17BC\u1795\u17D2\u1782\u1784\u1793\u17B9\u1784\u1791\u1798\u17D2\u179A\u1784\u17CB\u178A\u17C2\u179B\u1794\u17B6\u1793\u1780\u17C6\u178E\u178F\u17CB ${v.pattern}`;
          return `\u1798\u17B7\u1793\u178F\u17D2\u179A\u17B9\u1798\u178F\u17D2\u179A\u17BC\u179C\u17D6 ${o[v.format] ?? n.format}`;
        }
        case "not_multiple_of":
          return `\u179B\u17C1\u1781\u1798\u17B7\u1793\u178F\u17D2\u179A\u17B9\u1798\u178F\u17D2\u179A\u17BC\u179C\u17D6 \u178F\u17D2\u179A\u17BC\u179C\u178F\u17C2\u1787\u17B6\u1796\u17A0\u17BB\u1782\u17BB\u178E\u1793\u17C3 ${n.divisor}`;
        case "unrecognized_keys":
          return `\u179A\u1780\u1783\u17BE\u1789\u179F\u17C4\u1798\u17B7\u1793\u179F\u17D2\u1782\u17B6\u179B\u17CB\u17D6 ${b(n.keys, ", ")}`;
        case "invalid_key":
          return `\u179F\u17C4\u1798\u17B7\u1793\u178F\u17D2\u179A\u17B9\u1798\u178F\u17D2\u179A\u17BC\u179C\u1793\u17C5\u1780\u17D2\u1793\u17BB\u1784 ${n.origin}`;
        case "invalid_union":
          return "\u1791\u17B7\u1793\u17D2\u1793\u1793\u17D0\u1799\u1798\u17B7\u1793\u178F\u17D2\u179A\u17B9\u1798\u178F\u17D2\u179A\u17BC\u179C";
        case "invalid_element":
          return `\u1791\u17B7\u1793\u17D2\u1793\u1793\u17D0\u1799\u1798\u17B7\u1793\u178F\u17D2\u179A\u17B9\u1798\u178F\u17D2\u179A\u17BC\u179C\u1793\u17C5\u1780\u17D2\u1793\u17BB\u1784 ${n.origin}`;
        default:
          return "\u1791\u17B7\u1793\u17D2\u1793\u1793\u17D0\u1799\u1798\u17B7\u1793\u178F\u17D2\u179A\u17B9\u1798\u178F\u17D2\u179A\u17BC\u179C";
      }
    };
  };
  function Dn() {
    return { localeError: L4() };
  }
  function uu() {
    return Dn();
  }
  var E4 = () => {
    let r = { string: { unit: "\uBB38\uC790", verb: "to have" }, file: { unit: "\uBC14\uC774\uD2B8", verb: "to have" }, array: { unit: "\uAC1C", verb: "to have" }, set: { unit: "\uAC1C", verb: "to have" } };
    function i(n) {
      return r[n] ?? null;
    }
    let o = { regex: "\uC785\uB825", email: "\uC774\uBA54\uC77C \uC8FC\uC18C", url: "URL", emoji: "\uC774\uBAA8\uC9C0", uuid: "UUID", uuidv4: "UUIDv4", uuidv6: "UUIDv6", nanoid: "nanoid", guid: "GUID", cuid: "cuid", cuid2: "cuid2", ulid: "ULID", xid: "XID", ksuid: "KSUID", datetime: "ISO \uB0A0\uC9DC\uC2DC\uAC04", date: "ISO \uB0A0\uC9DC", time: "ISO \uC2DC\uAC04", duration: "ISO \uAE30\uAC04", ipv4: "IPv4 \uC8FC\uC18C", ipv6: "IPv6 \uC8FC\uC18C", cidrv4: "IPv4 \uBC94\uC704", cidrv6: "IPv6 \uBC94\uC704", base64: "base64 \uC778\uCF54\uB529 \uBB38\uC790\uC5F4", base64url: "base64url \uC778\uCF54\uB529 \uBB38\uC790\uC5F4", json_string: "JSON \uBB38\uC790\uC5F4", e164: "E.164 \uBC88\uD638", jwt: "JWT", template_literal: "\uC785\uB825" }, t = { nan: "NaN" };
    return (n) => {
      switch (n.code) {
        case "invalid_type": {
          let v = t[n.expected] ?? n.expected, u = k(n.input), $ = t[u] ?? u;
          if (/^[A-Z]/.test(n.expected))
            return `\uC798\uBABB\uB41C \uC785\uB825: \uC608\uC0C1 \uD0C0\uC785\uC740 instanceof ${n.expected}, \uBC1B\uC740 \uD0C0\uC785\uC740 ${$}\uC785\uB2C8\uB2E4`;
          return `\uC798\uBABB\uB41C \uC785\uB825: \uC608\uC0C1 \uD0C0\uC785\uC740 ${v}, \uBC1B\uC740 \uD0C0\uC785\uC740 ${$}\uC785\uB2C8\uB2E4`;
        }
        case "invalid_value":
          if (n.values.length === 1)
            return `\uC798\uBABB\uB41C \uC785\uB825: \uAC12\uC740 ${U(n.values[0])} \uC774\uC5B4\uC57C \uD569\uB2C8\uB2E4`;
          return `\uC798\uBABB\uB41C \uC635\uC158: ${b(n.values, "\uB610\uB294 ")} \uC911 \uD558\uB098\uC5EC\uC57C \uD569\uB2C8\uB2E4`;
        case "too_big": {
          let v = n.inclusive ? "\uC774\uD558" : "\uBBF8\uB9CC", u = v === "\uBBF8\uB9CC" ? "\uC774\uC5B4\uC57C \uD569\uB2C8\uB2E4" : "\uC5EC\uC57C \uD569\uB2C8\uB2E4", $ = i(n.origin), l = $?.unit ?? "\uC694\uC18C";
          if ($)
            return `${n.origin ?? "\uAC12"}\uC774 \uB108\uBB34 \uD07D\uB2C8\uB2E4: ${n.maximum.toString()}${l} ${v}${u}`;
          return `${n.origin ?? "\uAC12"}\uC774 \uB108\uBB34 \uD07D\uB2C8\uB2E4: ${n.maximum.toString()} ${v}${u}`;
        }
        case "too_small": {
          let v = n.inclusive ? "\uC774\uC0C1" : "\uCD08\uACFC", u = v === "\uC774\uC0C1" ? "\uC774\uC5B4\uC57C \uD569\uB2C8\uB2E4" : "\uC5EC\uC57C \uD569\uB2C8\uB2E4", $ = i(n.origin), l = $?.unit ?? "\uC694\uC18C";
          if ($)
            return `${n.origin ?? "\uAC12"}\uC774 \uB108\uBB34 \uC791\uC2B5\uB2C8\uB2E4: ${n.minimum.toString()}${l} ${v}${u}`;
          return `${n.origin ?? "\uAC12"}\uC774 \uB108\uBB34 \uC791\uC2B5\uB2C8\uB2E4: ${n.minimum.toString()} ${v}${u}`;
        }
        case "invalid_format": {
          let v = n;
          if (v.format === "starts_with")
            return `\uC798\uBABB\uB41C \uBB38\uC790\uC5F4: "${v.prefix}"(\uC73C)\uB85C \uC2DC\uC791\uD574\uC57C \uD569\uB2C8\uB2E4`;
          if (v.format === "ends_with")
            return `\uC798\uBABB\uB41C \uBB38\uC790\uC5F4: "${v.suffix}"(\uC73C)\uB85C \uB05D\uB098\uC57C \uD569\uB2C8\uB2E4`;
          if (v.format === "includes")
            return `\uC798\uBABB\uB41C \uBB38\uC790\uC5F4: "${v.includes}"\uC744(\uB97C) \uD3EC\uD568\uD574\uC57C \uD569\uB2C8\uB2E4`;
          if (v.format === "regex")
            return `\uC798\uBABB\uB41C \uBB38\uC790\uC5F4: \uC815\uADDC\uC2DD ${v.pattern} \uD328\uD134\uACFC \uC77C\uCE58\uD574\uC57C \uD569\uB2C8\uB2E4`;
          return `\uC798\uBABB\uB41C ${o[v.format] ?? n.format}`;
        }
        case "not_multiple_of":
          return `\uC798\uBABB\uB41C \uC22B\uC790: ${n.divisor}\uC758 \uBC30\uC218\uC5EC\uC57C \uD569\uB2C8\uB2E4`;
        case "unrecognized_keys":
          return `\uC778\uC2DD\uD560 \uC218 \uC5C6\uB294 \uD0A4: ${b(n.keys, ", ")}`;
        case "invalid_key":
          return `\uC798\uBABB\uB41C \uD0A4: ${n.origin}`;
        case "invalid_union":
          return "\uC798\uBABB\uB41C \uC785\uB825";
        case "invalid_element":
          return `\uC798\uBABB\uB41C \uAC12: ${n.origin}`;
        default:
          return "\uC798\uBABB\uB41C \uC785\uB825";
      }
    };
  };
  function $u() {
    return { localeError: E4() };
  }
  var wn = (r) => {
    return r.charAt(0).toUpperCase() + r.slice(1);
  };
  function rl(r) {
    let i = Math.abs(r), o = i % 10, t = i % 100;
    if (t >= 11 && t <= 19 || o === 0)
      return "many";
    if (o === 1)
      return "one";
    return "few";
  }
  var G4 = () => {
    let r = { string: { unit: { one: "simbolis", few: "simboliai", many: "simboli\u0173" }, verb: { smaller: { inclusive: "turi b\u016Bti ne ilgesn\u0117 kaip", notInclusive: "turi b\u016Bti trumpesn\u0117 kaip" }, bigger: { inclusive: "turi b\u016Bti ne trumpesn\u0117 kaip", notInclusive: "turi b\u016Bti ilgesn\u0117 kaip" } } }, file: { unit: { one: "baitas", few: "baitai", many: "bait\u0173" }, verb: { smaller: { inclusive: "turi b\u016Bti ne didesnis kaip", notInclusive: "turi b\u016Bti ma\u017Eesnis kaip" }, bigger: { inclusive: "turi b\u016Bti ne ma\u017Eesnis kaip", notInclusive: "turi b\u016Bti didesnis kaip" } } }, array: { unit: { one: "element\u0105", few: "elementus", many: "element\u0173" }, verb: { smaller: { inclusive: "turi tur\u0117ti ne daugiau kaip", notInclusive: "turi tur\u0117ti ma\u017Eiau kaip" }, bigger: { inclusive: "turi tur\u0117ti ne ma\u017Eiau kaip", notInclusive: "turi tur\u0117ti daugiau kaip" } } }, set: { unit: { one: "element\u0105", few: "elementus", many: "element\u0173" }, verb: { smaller: { inclusive: "turi tur\u0117ti ne daugiau kaip", notInclusive: "turi tur\u0117ti ma\u017Eiau kaip" }, bigger: { inclusive: "turi tur\u0117ti ne ma\u017Eiau kaip", notInclusive: "turi tur\u0117ti daugiau kaip" } } } };
    function i(n, v, u, $) {
      let l = r[n] ?? null;
      if (l === null)
        return l;
      return { unit: l.unit[v], verb: l.verb[$][u ? "inclusive" : "notInclusive"] };
    }
    let o = { regex: "\u012Fvestis", email: "el. pa\u0161to adresas", url: "URL", emoji: "jaustukas", uuid: "UUID", uuidv4: "UUIDv4", uuidv6: "UUIDv6", nanoid: "nanoid", guid: "GUID", cuid: "cuid", cuid2: "cuid2", ulid: "ULID", xid: "XID", ksuid: "KSUID", datetime: "ISO data ir laikas", date: "ISO data", time: "ISO laikas", duration: "ISO trukm\u0117", ipv4: "IPv4 adresas", ipv6: "IPv6 adresas", cidrv4: "IPv4 tinklo prefiksas (CIDR)", cidrv6: "IPv6 tinklo prefiksas (CIDR)", base64: "base64 u\u017Ekoduota eilut\u0117", base64url: "base64url u\u017Ekoduota eilut\u0117", json_string: "JSON eilut\u0117", e164: "E.164 numeris", jwt: "JWT", template_literal: "\u012Fvestis" }, t = { nan: "NaN", number: "skai\u010Dius", bigint: "sveikasis skai\u010Dius", string: "eilut\u0117", boolean: "login\u0117 reik\u0161m\u0117", undefined: "neapibr\u0117\u017Eta reik\u0161m\u0117", function: "funkcija", symbol: "simbolis", array: "masyvas", object: "objektas", null: "nulin\u0117 reik\u0161m\u0117" };
    return (n) => {
      switch (n.code) {
        case "invalid_type": {
          let v = t[n.expected] ?? n.expected, u = k(n.input), $ = t[u] ?? u;
          if (/^[A-Z]/.test(n.expected))
            return `Gautas tipas ${$}, o tik\u0117tasi - instanceof ${n.expected}`;
          return `Gautas tipas ${$}, o tik\u0117tasi - ${v}`;
        }
        case "invalid_value":
          if (n.values.length === 1)
            return `Privalo b\u016Bti ${U(n.values[0])}`;
          return `Privalo b\u016Bti vienas i\u0161 ${b(n.values, "|")} pasirinkim\u0173`;
        case "too_big": {
          let v = t[n.origin] ?? n.origin, u = i(n.origin, rl(Number(n.maximum)), n.inclusive ?? false, "smaller");
          if (u?.verb)
            return `${wn(v ?? n.origin ?? "reik\u0161m\u0117")} ${u.verb} ${n.maximum.toString()} ${u.unit ?? "element\u0173"}`;
          let $ = n.inclusive ? "ne didesnis kaip" : "ma\u017Eesnis kaip";
          return `${wn(v ?? n.origin ?? "reik\u0161m\u0117")} turi b\u016Bti ${$} ${n.maximum.toString()} ${u?.unit}`;
        }
        case "too_small": {
          let v = t[n.origin] ?? n.origin, u = i(n.origin, rl(Number(n.minimum)), n.inclusive ?? false, "bigger");
          if (u?.verb)
            return `${wn(v ?? n.origin ?? "reik\u0161m\u0117")} ${u.verb} ${n.minimum.toString()} ${u.unit ?? "element\u0173"}`;
          let $ = n.inclusive ? "ne ma\u017Eesnis kaip" : "didesnis kaip";
          return `${wn(v ?? n.origin ?? "reik\u0161m\u0117")} turi b\u016Bti ${$} ${n.minimum.toString()} ${u?.unit}`;
        }
        case "invalid_format": {
          let v = n;
          if (v.format === "starts_with")
            return `Eilut\u0117 privalo prasid\u0117ti "${v.prefix}"`;
          if (v.format === "ends_with")
            return `Eilut\u0117 privalo pasibaigti "${v.suffix}"`;
          if (v.format === "includes")
            return `Eilut\u0117 privalo \u012Ftraukti "${v.includes}"`;
          if (v.format === "regex")
            return `Eilut\u0117 privalo atitikti ${v.pattern}`;
          return `Neteisingas ${o[v.format] ?? n.format}`;
        }
        case "not_multiple_of":
          return `Skai\u010Dius privalo b\u016Bti ${n.divisor} kartotinis.`;
        case "unrecognized_keys":
          return `Neatpa\u017Eint${n.keys.length > 1 ? "i" : "as"} rakt${n.keys.length > 1 ? "ai" : "as"}: ${b(n.keys, ", ")}`;
        case "invalid_key":
          return "Rastas klaidingas raktas";
        case "invalid_union":
          return "Klaidinga \u012Fvestis";
        case "invalid_element": {
          let v = t[n.origin] ?? n.origin;
          return `${wn(v ?? n.origin ?? "reik\u0161m\u0117")} turi klaiding\u0105 \u012Fvest\u012F`;
        }
        default:
          return "Klaidinga \u012Fvestis";
      }
    };
  };
  function gu() {
    return { localeError: G4() };
  }
  var W4 = () => {
    let r = { string: { unit: "\u0437\u043D\u0430\u0446\u0438", verb: "\u0434\u0430 \u0438\u043C\u0430\u0430\u0442" }, file: { unit: "\u0431\u0430\u0458\u0442\u0438", verb: "\u0434\u0430 \u0438\u043C\u0430\u0430\u0442" }, array: { unit: "\u0441\u0442\u0430\u0432\u043A\u0438", verb: "\u0434\u0430 \u0438\u043C\u0430\u0430\u0442" }, set: { unit: "\u0441\u0442\u0430\u0432\u043A\u0438", verb: "\u0434\u0430 \u0438\u043C\u0430\u0430\u0442" } };
    function i(n) {
      return r[n] ?? null;
    }
    let o = { regex: "\u0432\u043D\u0435\u0441", email: "\u0430\u0434\u0440\u0435\u0441\u0430 \u043D\u0430 \u0435-\u043F\u043E\u0448\u0442\u0430", url: "URL", emoji: "\u0435\u043C\u043E\u045F\u0438", uuid: "UUID", uuidv4: "UUIDv4", uuidv6: "UUIDv6", nanoid: "nanoid", guid: "GUID", cuid: "cuid", cuid2: "cuid2", ulid: "ULID", xid: "XID", ksuid: "KSUID", datetime: "ISO \u0434\u0430\u0442\u0443\u043C \u0438 \u0432\u0440\u0435\u043C\u0435", date: "ISO \u0434\u0430\u0442\u0443\u043C", time: "ISO \u0432\u0440\u0435\u043C\u0435", duration: "ISO \u0432\u0440\u0435\u043C\u0435\u0442\u0440\u0430\u0435\u045A\u0435", ipv4: "IPv4 \u0430\u0434\u0440\u0435\u0441\u0430", ipv6: "IPv6 \u0430\u0434\u0440\u0435\u0441\u0430", cidrv4: "IPv4 \u043E\u043F\u0441\u0435\u0433", cidrv6: "IPv6 \u043E\u043F\u0441\u0435\u0433", base64: "base64-\u0435\u043D\u043A\u043E\u0434\u0438\u0440\u0430\u043D\u0430 \u043D\u0438\u0437\u0430", base64url: "base64url-\u0435\u043D\u043A\u043E\u0434\u0438\u0440\u0430\u043D\u0430 \u043D\u0438\u0437\u0430", json_string: "JSON \u043D\u0438\u0437\u0430", e164: "E.164 \u0431\u0440\u043E\u0458", jwt: "JWT", template_literal: "\u0432\u043D\u0435\u0441" }, t = { nan: "NaN", number: "\u0431\u0440\u043E\u0458", array: "\u043D\u0438\u0437\u0430" };
    return (n) => {
      switch (n.code) {
        case "invalid_type": {
          let v = t[n.expected] ?? n.expected, u = k(n.input), $ = t[u] ?? u;
          if (/^[A-Z]/.test(n.expected))
            return `\u0413\u0440\u0435\u0448\u0435\u043D \u0432\u043D\u0435\u0441: \u0441\u0435 \u043E\u0447\u0435\u043A\u0443\u0432\u0430 instanceof ${n.expected}, \u043F\u0440\u0438\u043C\u0435\u043D\u043E ${$}`;
          return `\u0413\u0440\u0435\u0448\u0435\u043D \u0432\u043D\u0435\u0441: \u0441\u0435 \u043E\u0447\u0435\u043A\u0443\u0432\u0430 ${v}, \u043F\u0440\u0438\u043C\u0435\u043D\u043E ${$}`;
        }
        case "invalid_value":
          if (n.values.length === 1)
            return `Invalid input: expected ${U(n.values[0])}`;
          return `\u0413\u0440\u0435\u0448\u0430\u043D\u0430 \u043E\u043F\u0446\u0438\u0458\u0430: \u0441\u0435 \u043E\u0447\u0435\u043A\u0443\u0432\u0430 \u0435\u0434\u043D\u0430 ${b(n.values, "|")}`;
        case "too_big": {
          let v = n.inclusive ? "<=" : "<", u = i(n.origin);
          if (u)
            return `\u041F\u0440\u0435\u043C\u043D\u043E\u0433\u0443 \u0433\u043E\u043B\u0435\u043C: \u0441\u0435 \u043E\u0447\u0435\u043A\u0443\u0432\u0430 ${n.origin ?? "\u0432\u0440\u0435\u0434\u043D\u043E\u0441\u0442\u0430"} \u0434\u0430 \u0438\u043C\u0430 ${v}${n.maximum.toString()} ${u.unit ?? "\u0435\u043B\u0435\u043C\u0435\u043D\u0442\u0438"}`;
          return `\u041F\u0440\u0435\u043C\u043D\u043E\u0433\u0443 \u0433\u043E\u043B\u0435\u043C: \u0441\u0435 \u043E\u0447\u0435\u043A\u0443\u0432\u0430 ${n.origin ?? "\u0432\u0440\u0435\u0434\u043D\u043E\u0441\u0442\u0430"} \u0434\u0430 \u0431\u0438\u0434\u0435 ${v}${n.maximum.toString()}`;
        }
        case "too_small": {
          let v = n.inclusive ? ">=" : ">", u = i(n.origin);
          if (u)
            return `\u041F\u0440\u0435\u043C\u043D\u043E\u0433\u0443 \u043C\u0430\u043B: \u0441\u0435 \u043E\u0447\u0435\u043A\u0443\u0432\u0430 ${n.origin} \u0434\u0430 \u0438\u043C\u0430 ${v}${n.minimum.toString()} ${u.unit}`;
          return `\u041F\u0440\u0435\u043C\u043D\u043E\u0433\u0443 \u043C\u0430\u043B: \u0441\u0435 \u043E\u0447\u0435\u043A\u0443\u0432\u0430 ${n.origin} \u0434\u0430 \u0431\u0438\u0434\u0435 ${v}${n.minimum.toString()}`;
        }
        case "invalid_format": {
          let v = n;
          if (v.format === "starts_with")
            return `\u041D\u0435\u0432\u0430\u0436\u0435\u0447\u043A\u0430 \u043D\u0438\u0437\u0430: \u043C\u043E\u0440\u0430 \u0434\u0430 \u0437\u0430\u043F\u043E\u0447\u043D\u0443\u0432\u0430 \u0441\u043E "${v.prefix}"`;
          if (v.format === "ends_with")
            return `\u041D\u0435\u0432\u0430\u0436\u0435\u0447\u043A\u0430 \u043D\u0438\u0437\u0430: \u043C\u043E\u0440\u0430 \u0434\u0430 \u0437\u0430\u0432\u0440\u0448\u0443\u0432\u0430 \u0441\u043E "${v.suffix}"`;
          if (v.format === "includes")
            return `\u041D\u0435\u0432\u0430\u0436\u0435\u0447\u043A\u0430 \u043D\u0438\u0437\u0430: \u043C\u043E\u0440\u0430 \u0434\u0430 \u0432\u043A\u043B\u0443\u0447\u0443\u0432\u0430 "${v.includes}"`;
          if (v.format === "regex")
            return `\u041D\u0435\u0432\u0430\u0436\u0435\u0447\u043A\u0430 \u043D\u0438\u0437\u0430: \u043C\u043E\u0440\u0430 \u0434\u0430 \u043E\u0434\u0433\u043E\u0430\u0440\u0430 \u043D\u0430 \u043F\u0430\u0442\u0435\u0440\u043D\u043E\u0442 ${v.pattern}`;
          return `Invalid ${o[v.format] ?? n.format}`;
        }
        case "not_multiple_of":
          return `\u0413\u0440\u0435\u0448\u0435\u043D \u0431\u0440\u043E\u0458: \u043C\u043E\u0440\u0430 \u0434\u0430 \u0431\u0438\u0434\u0435 \u0434\u0435\u043B\u0438\u0432 \u0441\u043E ${n.divisor}`;
        case "unrecognized_keys":
          return `${n.keys.length > 1 ? "\u041D\u0435\u043F\u0440\u0435\u043F\u043E\u0437\u043D\u0430\u0435\u043D\u0438 \u043A\u043B\u0443\u0447\u0435\u0432\u0438" : "\u041D\u0435\u043F\u0440\u0435\u043F\u043E\u0437\u043D\u0430\u0435\u043D \u043A\u043B\u0443\u0447"}: ${b(n.keys, ", ")}`;
        case "invalid_key":
          return `\u0413\u0440\u0435\u0448\u0435\u043D \u043A\u043B\u0443\u0447 \u0432\u043E ${n.origin}`;
        case "invalid_union":
          return "\u0413\u0440\u0435\u0448\u0435\u043D \u0432\u043D\u0435\u0441";
        case "invalid_element":
          return `\u0413\u0440\u0435\u0448\u043D\u0430 \u0432\u0440\u0435\u0434\u043D\u043E\u0441\u0442 \u0432\u043E ${n.origin}`;
        default:
          return "\u0413\u0440\u0435\u0448\u0435\u043D \u0432\u043D\u0435\u0441";
      }
    };
  };
  function eu() {
    return { localeError: W4() };
  }
  var X4 = () => {
    let r = { string: { unit: "aksara", verb: "mempunyai" }, file: { unit: "bait", verb: "mempunyai" }, array: { unit: "elemen", verb: "mempunyai" }, set: { unit: "elemen", verb: "mempunyai" } };
    function i(n) {
      return r[n] ?? null;
    }
    let o = { regex: "input", email: "alamat e-mel", url: "URL", emoji: "emoji", uuid: "UUID", uuidv4: "UUIDv4", uuidv6: "UUIDv6", nanoid: "nanoid", guid: "GUID", cuid: "cuid", cuid2: "cuid2", ulid: "ULID", xid: "XID", ksuid: "KSUID", datetime: "tarikh masa ISO", date: "tarikh ISO", time: "masa ISO", duration: "tempoh ISO", ipv4: "alamat IPv4", ipv6: "alamat IPv6", cidrv4: "julat IPv4", cidrv6: "julat IPv6", base64: "string dikodkan base64", base64url: "string dikodkan base64url", json_string: "string JSON", e164: "nombor E.164", jwt: "JWT", template_literal: "input" }, t = { nan: "NaN", number: "nombor" };
    return (n) => {
      switch (n.code) {
        case "invalid_type": {
          let v = t[n.expected] ?? n.expected, u = k(n.input), $ = t[u] ?? u;
          if (/^[A-Z]/.test(n.expected))
            return `Input tidak sah: dijangka instanceof ${n.expected}, diterima ${$}`;
          return `Input tidak sah: dijangka ${v}, diterima ${$}`;
        }
        case "invalid_value":
          if (n.values.length === 1)
            return `Input tidak sah: dijangka ${U(n.values[0])}`;
          return `Pilihan tidak sah: dijangka salah satu daripada ${b(n.values, "|")}`;
        case "too_big": {
          let v = n.inclusive ? "<=" : "<", u = i(n.origin);
          if (u)
            return `Terlalu besar: dijangka ${n.origin ?? "nilai"} ${u.verb} ${v}${n.maximum.toString()} ${u.unit ?? "elemen"}`;
          return `Terlalu besar: dijangka ${n.origin ?? "nilai"} adalah ${v}${n.maximum.toString()}`;
        }
        case "too_small": {
          let v = n.inclusive ? ">=" : ">", u = i(n.origin);
          if (u)
            return `Terlalu kecil: dijangka ${n.origin} ${u.verb} ${v}${n.minimum.toString()} ${u.unit}`;
          return `Terlalu kecil: dijangka ${n.origin} adalah ${v}${n.minimum.toString()}`;
        }
        case "invalid_format": {
          let v = n;
          if (v.format === "starts_with")
            return `String tidak sah: mesti bermula dengan "${v.prefix}"`;
          if (v.format === "ends_with")
            return `String tidak sah: mesti berakhir dengan "${v.suffix}"`;
          if (v.format === "includes")
            return `String tidak sah: mesti mengandungi "${v.includes}"`;
          if (v.format === "regex")
            return `String tidak sah: mesti sepadan dengan corak ${v.pattern}`;
          return `${o[v.format] ?? n.format} tidak sah`;
        }
        case "not_multiple_of":
          return `Nombor tidak sah: perlu gandaan ${n.divisor}`;
        case "unrecognized_keys":
          return `Kunci tidak dikenali: ${b(n.keys, ", ")}`;
        case "invalid_key":
          return `Kunci tidak sah dalam ${n.origin}`;
        case "invalid_union":
          return "Input tidak sah";
        case "invalid_element":
          return `Nilai tidak sah dalam ${n.origin}`;
        default:
          return "Input tidak sah";
      }
    };
  };
  function lu() {
    return { localeError: X4() };
  }
  var V4 = () => {
    let r = { string: { unit: "tekens", verb: "heeft" }, file: { unit: "bytes", verb: "heeft" }, array: { unit: "elementen", verb: "heeft" }, set: { unit: "elementen", verb: "heeft" } };
    function i(n) {
      return r[n] ?? null;
    }
    let o = { regex: "invoer", email: "emailadres", url: "URL", emoji: "emoji", uuid: "UUID", uuidv4: "UUIDv4", uuidv6: "UUIDv6", nanoid: "nanoid", guid: "GUID", cuid: "cuid", cuid2: "cuid2", ulid: "ULID", xid: "XID", ksuid: "KSUID", datetime: "ISO datum en tijd", date: "ISO datum", time: "ISO tijd", duration: "ISO duur", ipv4: "IPv4-adres", ipv6: "IPv6-adres", cidrv4: "IPv4-bereik", cidrv6: "IPv6-bereik", base64: "base64-gecodeerde tekst", base64url: "base64 URL-gecodeerde tekst", json_string: "JSON string", e164: "E.164-nummer", jwt: "JWT", template_literal: "invoer" }, t = { nan: "NaN", number: "getal" };
    return (n) => {
      switch (n.code) {
        case "invalid_type": {
          let v = t[n.expected] ?? n.expected, u = k(n.input), $ = t[u] ?? u;
          if (/^[A-Z]/.test(n.expected))
            return `Ongeldige invoer: verwacht instanceof ${n.expected}, ontving ${$}`;
          return `Ongeldige invoer: verwacht ${v}, ontving ${$}`;
        }
        case "invalid_value":
          if (n.values.length === 1)
            return `Ongeldige invoer: verwacht ${U(n.values[0])}`;
          return `Ongeldige optie: verwacht \xE9\xE9n van ${b(n.values, "|")}`;
        case "too_big": {
          let v = n.inclusive ? "<=" : "<", u = i(n.origin), $ = n.origin === "date" ? "laat" : n.origin === "string" ? "lang" : "groot";
          if (u)
            return `Te ${$}: verwacht dat ${n.origin ?? "waarde"} ${v}${n.maximum.toString()} ${u.unit ?? "elementen"} ${u.verb}`;
          return `Te ${$}: verwacht dat ${n.origin ?? "waarde"} ${v}${n.maximum.toString()} is`;
        }
        case "too_small": {
          let v = n.inclusive ? ">=" : ">", u = i(n.origin), $ = n.origin === "date" ? "vroeg" : n.origin === "string" ? "kort" : "klein";
          if (u)
            return `Te ${$}: verwacht dat ${n.origin} ${v}${n.minimum.toString()} ${u.unit} ${u.verb}`;
          return `Te ${$}: verwacht dat ${n.origin} ${v}${n.minimum.toString()} is`;
        }
        case "invalid_format": {
          let v = n;
          if (v.format === "starts_with")
            return `Ongeldige tekst: moet met "${v.prefix}" beginnen`;
          if (v.format === "ends_with")
            return `Ongeldige tekst: moet op "${v.suffix}" eindigen`;
          if (v.format === "includes")
            return `Ongeldige tekst: moet "${v.includes}" bevatten`;
          if (v.format === "regex")
            return `Ongeldige tekst: moet overeenkomen met patroon ${v.pattern}`;
          return `Ongeldig: ${o[v.format] ?? n.format}`;
        }
        case "not_multiple_of":
          return `Ongeldig getal: moet een veelvoud van ${n.divisor} zijn`;
        case "unrecognized_keys":
          return `Onbekende key${n.keys.length > 1 ? "s" : ""}: ${b(n.keys, ", ")}`;
        case "invalid_key":
          return `Ongeldige key in ${n.origin}`;
        case "invalid_union":
          return "Ongeldige invoer";
        case "invalid_element":
          return `Ongeldige waarde in ${n.origin}`;
        default:
          return "Ongeldige invoer";
      }
    };
  };
  function cu() {
    return { localeError: V4() };
  }
  var A4 = () => {
    let r = { string: { unit: "tegn", verb: "\xE5 ha" }, file: { unit: "bytes", verb: "\xE5 ha" }, array: { unit: "elementer", verb: "\xE5 inneholde" }, set: { unit: "elementer", verb: "\xE5 inneholde" } };
    function i(n) {
      return r[n] ?? null;
    }
    let o = { regex: "input", email: "e-postadresse", url: "URL", emoji: "emoji", uuid: "UUID", uuidv4: "UUIDv4", uuidv6: "UUIDv6", nanoid: "nanoid", guid: "GUID", cuid: "cuid", cuid2: "cuid2", ulid: "ULID", xid: "XID", ksuid: "KSUID", datetime: "ISO dato- og klokkeslett", date: "ISO-dato", time: "ISO-klokkeslett", duration: "ISO-varighet", ipv4: "IPv4-omr\xE5de", ipv6: "IPv6-omr\xE5de", cidrv4: "IPv4-spekter", cidrv6: "IPv6-spekter", base64: "base64-enkodet streng", base64url: "base64url-enkodet streng", json_string: "JSON-streng", e164: "E.164-nummer", jwt: "JWT", template_literal: "input" }, t = { nan: "NaN", number: "tall", array: "liste" };
    return (n) => {
      switch (n.code) {
        case "invalid_type": {
          let v = t[n.expected] ?? n.expected, u = k(n.input), $ = t[u] ?? u;
          if (/^[A-Z]/.test(n.expected))
            return `Ugyldig input: forventet instanceof ${n.expected}, fikk ${$}`;
          return `Ugyldig input: forventet ${v}, fikk ${$}`;
        }
        case "invalid_value":
          if (n.values.length === 1)
            return `Ugyldig verdi: forventet ${U(n.values[0])}`;
          return `Ugyldig valg: forventet en av ${b(n.values, "|")}`;
        case "too_big": {
          let v = n.inclusive ? "<=" : "<", u = i(n.origin);
          if (u)
            return `For stor(t): forventet ${n.origin ?? "value"} til \xE5 ha ${v}${n.maximum.toString()} ${u.unit ?? "elementer"}`;
          return `For stor(t): forventet ${n.origin ?? "value"} til \xE5 ha ${v}${n.maximum.toString()}`;
        }
        case "too_small": {
          let v = n.inclusive ? ">=" : ">", u = i(n.origin);
          if (u)
            return `For lite(n): forventet ${n.origin} til \xE5 ha ${v}${n.minimum.toString()} ${u.unit}`;
          return `For lite(n): forventet ${n.origin} til \xE5 ha ${v}${n.minimum.toString()}`;
        }
        case "invalid_format": {
          let v = n;
          if (v.format === "starts_with")
            return `Ugyldig streng: m\xE5 starte med "${v.prefix}"`;
          if (v.format === "ends_with")
            return `Ugyldig streng: m\xE5 ende med "${v.suffix}"`;
          if (v.format === "includes")
            return `Ugyldig streng: m\xE5 inneholde "${v.includes}"`;
          if (v.format === "regex")
            return `Ugyldig streng: m\xE5 matche m\xF8nsteret ${v.pattern}`;
          return `Ugyldig ${o[v.format] ?? n.format}`;
        }
        case "not_multiple_of":
          return `Ugyldig tall: m\xE5 v\xE6re et multiplum av ${n.divisor}`;
        case "unrecognized_keys":
          return `${n.keys.length > 1 ? "Ukjente n\xF8kler" : "Ukjent n\xF8kkel"}: ${b(n.keys, ", ")}`;
        case "invalid_key":
          return `Ugyldig n\xF8kkel i ${n.origin}`;
        case "invalid_union":
          return "Ugyldig input";
        case "invalid_element":
          return `Ugyldig verdi i ${n.origin}`;
        default:
          return "Ugyldig input";
      }
    };
  };
  function Iu() {
    return { localeError: A4() };
  }
  var K4 = () => {
    let r = { string: { unit: "harf", verb: "olmal\u0131d\u0131r" }, file: { unit: "bayt", verb: "olmal\u0131d\u0131r" }, array: { unit: "unsur", verb: "olmal\u0131d\u0131r" }, set: { unit: "unsur", verb: "olmal\u0131d\u0131r" } };
    function i(n) {
      return r[n] ?? null;
    }
    let o = { regex: "giren", email: "epostag\xE2h", url: "URL", emoji: "emoji", uuid: "UUID", uuidv4: "UUIDv4", uuidv6: "UUIDv6", nanoid: "nanoid", guid: "GUID", cuid: "cuid", cuid2: "cuid2", ulid: "ULID", xid: "XID", ksuid: "KSUID", datetime: "ISO heng\xE2m\u0131", date: "ISO tarihi", time: "ISO zaman\u0131", duration: "ISO m\xFCddeti", ipv4: "IPv4 ni\u015F\xE2n\u0131", ipv6: "IPv6 ni\u015F\xE2n\u0131", cidrv4: "IPv4 menzili", cidrv6: "IPv6 menzili", base64: "base64-\u015Fifreli metin", base64url: "base64url-\u015Fifreli metin", json_string: "JSON metin", e164: "E.164 say\u0131s\u0131", jwt: "JWT", template_literal: "giren" }, t = { nan: "NaN", number: "numara", array: "saf", null: "gayb" };
    return (n) => {
      switch (n.code) {
        case "invalid_type": {
          let v = t[n.expected] ?? n.expected, u = k(n.input), $ = t[u] ?? u;
          if (/^[A-Z]/.test(n.expected))
            return `F\xE2sit giren: umulan instanceof ${n.expected}, al\u0131nan ${$}`;
          return `F\xE2sit giren: umulan ${v}, al\u0131nan ${$}`;
        }
        case "invalid_value":
          if (n.values.length === 1)
            return `F\xE2sit giren: umulan ${U(n.values[0])}`;
          return `F\xE2sit tercih: m\xFBteberler ${b(n.values, "|")}`;
        case "too_big": {
          let v = n.inclusive ? "<=" : "<", u = i(n.origin);
          if (u)
            return `Fazla b\xFCy\xFCk: ${n.origin ?? "value"}, ${v}${n.maximum.toString()} ${u.unit ?? "elements"} sahip olmal\u0131yd\u0131.`;
          return `Fazla b\xFCy\xFCk: ${n.origin ?? "value"}, ${v}${n.maximum.toString()} olmal\u0131yd\u0131.`;
        }
        case "too_small": {
          let v = n.inclusive ? ">=" : ">", u = i(n.origin);
          if (u)
            return `Fazla k\xFC\xE7\xFCk: ${n.origin}, ${v}${n.minimum.toString()} ${u.unit} sahip olmal\u0131yd\u0131.`;
          return `Fazla k\xFC\xE7\xFCk: ${n.origin}, ${v}${n.minimum.toString()} olmal\u0131yd\u0131.`;
        }
        case "invalid_format": {
          let v = n;
          if (v.format === "starts_with")
            return `F\xE2sit metin: "${v.prefix}" ile ba\u015Flamal\u0131.`;
          if (v.format === "ends_with")
            return `F\xE2sit metin: "${v.suffix}" ile bitmeli.`;
          if (v.format === "includes")
            return `F\xE2sit metin: "${v.includes}" ihtiv\xE2 etmeli.`;
          if (v.format === "regex")
            return `F\xE2sit metin: ${v.pattern} nak\u015F\u0131na uymal\u0131.`;
          return `F\xE2sit ${o[v.format] ?? n.format}`;
        }
        case "not_multiple_of":
          return `F\xE2sit say\u0131: ${n.divisor} kat\u0131 olmal\u0131yd\u0131.`;
        case "unrecognized_keys":
          return `Tan\u0131nmayan anahtar ${n.keys.length > 1 ? "s" : ""}: ${b(n.keys, ", ")}`;
        case "invalid_key":
          return `${n.origin} i\xE7in tan\u0131nmayan anahtar var.`;
        case "invalid_union":
          return "Giren tan\u0131namad\u0131.";
        case "invalid_element":
          return `${n.origin} i\xE7in tan\u0131nmayan k\u0131ymet var.`;
        default:
          return "K\u0131ymet tan\u0131namad\u0131.";
      }
    };
  };
  function bu() {
    return { localeError: K4() };
  }
  var q4 = () => {
    let r = { string: { unit: "\u062A\u0648\u06A9\u064A", verb: "\u0648\u0644\u0631\u064A" }, file: { unit: "\u0628\u0627\u06CC\u067C\u0633", verb: "\u0648\u0644\u0631\u064A" }, array: { unit: "\u062A\u0648\u06A9\u064A", verb: "\u0648\u0644\u0631\u064A" }, set: { unit: "\u062A\u0648\u06A9\u064A", verb: "\u0648\u0644\u0631\u064A" } };
    function i(n) {
      return r[n] ?? null;
    }
    let o = { regex: "\u0648\u0631\u0648\u062F\u064A", email: "\u0628\u0631\u06CC\u069A\u0646\u0627\u0644\u06CC\u06A9", url: "\u06CC\u0648 \u0622\u0631 \u0627\u0644", emoji: "\u0627\u06CC\u0645\u0648\u062C\u064A", uuid: "UUID", uuidv4: "UUIDv4", uuidv6: "UUIDv6", nanoid: "nanoid", guid: "GUID", cuid: "cuid", cuid2: "cuid2", ulid: "ULID", xid: "XID", ksuid: "KSUID", datetime: "\u0646\u06CC\u067C\u0647 \u0627\u0648 \u0648\u062E\u062A", date: "\u0646\u06D0\u067C\u0647", time: "\u0648\u062E\u062A", duration: "\u0645\u0648\u062F\u0647", ipv4: "\u062F IPv4 \u067E\u062A\u0647", ipv6: "\u062F IPv6 \u067E\u062A\u0647", cidrv4: "\u062F IPv4 \u0633\u0627\u062D\u0647", cidrv6: "\u062F IPv6 \u0633\u0627\u062D\u0647", base64: "base64-encoded \u0645\u062A\u0646", base64url: "base64url-encoded \u0645\u062A\u0646", json_string: "JSON \u0645\u062A\u0646", e164: "\u062F E.164 \u0634\u0645\u06D0\u0631\u0647", jwt: "JWT", template_literal: "\u0648\u0631\u0648\u062F\u064A" }, t = { nan: "NaN", number: "\u0639\u062F\u062F", array: "\u0627\u0631\u06D0" };
    return (n) => {
      switch (n.code) {
        case "invalid_type": {
          let v = t[n.expected] ?? n.expected, u = k(n.input), $ = t[u] ?? u;
          if (/^[A-Z]/.test(n.expected))
            return `\u0646\u0627\u0633\u0645 \u0648\u0631\u0648\u062F\u064A: \u0628\u0627\u06CC\u062F instanceof ${n.expected} \u0648\u0627\u06CC, \u0645\u06AB\u0631 ${$} \u062A\u0631\u0644\u0627\u0633\u0647 \u0634\u0648`;
          return `\u0646\u0627\u0633\u0645 \u0648\u0631\u0648\u062F\u064A: \u0628\u0627\u06CC\u062F ${v} \u0648\u0627\u06CC, \u0645\u06AB\u0631 ${$} \u062A\u0631\u0644\u0627\u0633\u0647 \u0634\u0648`;
        }
        case "invalid_value":
          if (n.values.length === 1)
            return `\u0646\u0627\u0633\u0645 \u0648\u0631\u0648\u062F\u064A: \u0628\u0627\u06CC\u062F ${U(n.values[0])} \u0648\u0627\u06CC`;
          return `\u0646\u0627\u0633\u0645 \u0627\u0646\u062A\u062E\u0627\u0628: \u0628\u0627\u06CC\u062F \u06CC\u0648 \u0644\u0647 ${b(n.values, "|")} \u0685\u062E\u0647 \u0648\u0627\u06CC`;
        case "too_big": {
          let v = n.inclusive ? "<=" : "<", u = i(n.origin);
          if (u)
            return `\u0689\u06CC\u0631 \u0644\u0648\u06CC: ${n.origin ?? "\u0627\u0631\u0632\u069A\u062A"} \u0628\u0627\u06CC\u062F ${v}${n.maximum.toString()} ${u.unit ?? "\u0639\u0646\u0635\u0631\u0648\u0646\u0647"} \u0648\u0644\u0631\u064A`;
          return `\u0689\u06CC\u0631 \u0644\u0648\u06CC: ${n.origin ?? "\u0627\u0631\u0632\u069A\u062A"} \u0628\u0627\u06CC\u062F ${v}${n.maximum.toString()} \u0648\u064A`;
        }
        case "too_small": {
          let v = n.inclusive ? ">=" : ">", u = i(n.origin);
          if (u)
            return `\u0689\u06CC\u0631 \u06A9\u0648\u0686\u0646\u06CC: ${n.origin} \u0628\u0627\u06CC\u062F ${v}${n.minimum.toString()} ${u.unit} \u0648\u0644\u0631\u064A`;
          return `\u0689\u06CC\u0631 \u06A9\u0648\u0686\u0646\u06CC: ${n.origin} \u0628\u0627\u06CC\u062F ${v}${n.minimum.toString()} \u0648\u064A`;
        }
        case "invalid_format": {
          let v = n;
          if (v.format === "starts_with")
            return `\u0646\u0627\u0633\u0645 \u0645\u062A\u0646: \u0628\u0627\u06CC\u062F \u062F "${v.prefix}" \u0633\u0631\u0647 \u067E\u06CC\u0644 \u0634\u064A`;
          if (v.format === "ends_with")
            return `\u0646\u0627\u0633\u0645 \u0645\u062A\u0646: \u0628\u0627\u06CC\u062F \u062F "${v.suffix}" \u0633\u0631\u0647 \u067E\u0627\u06CC \u062A\u0647 \u0648\u0631\u0633\u064A\u0696\u064A`;
          if (v.format === "includes")
            return `\u0646\u0627\u0633\u0645 \u0645\u062A\u0646: \u0628\u0627\u06CC\u062F "${v.includes}" \u0648\u0644\u0631\u064A`;
          if (v.format === "regex")
            return `\u0646\u0627\u0633\u0645 \u0645\u062A\u0646: \u0628\u0627\u06CC\u062F \u062F ${v.pattern} \u0633\u0631\u0647 \u0645\u0637\u0627\u0628\u0642\u062A \u0648\u0644\u0631\u064A`;
          return `${o[v.format] ?? n.format} \u0646\u0627\u0633\u0645 \u062F\u06CC`;
        }
        case "not_multiple_of":
          return `\u0646\u0627\u0633\u0645 \u0639\u062F\u062F: \u0628\u0627\u06CC\u062F \u062F ${n.divisor} \u0645\u0636\u0631\u0628 \u0648\u064A`;
        case "unrecognized_keys":
          return `\u0646\u0627\u0633\u0645 ${n.keys.length > 1 ? "\u06A9\u0644\u06CC\u0689\u0648\u0646\u0647" : "\u06A9\u0644\u06CC\u0689"}: ${b(n.keys, ", ")}`;
        case "invalid_key":
          return `\u0646\u0627\u0633\u0645 \u06A9\u0644\u06CC\u0689 \u067E\u0647 ${n.origin} \u06A9\u06D0`;
        case "invalid_union":
          return "\u0646\u0627\u0633\u0645\u0647 \u0648\u0631\u0648\u062F\u064A";
        case "invalid_element":
          return `\u0646\u0627\u0633\u0645 \u0639\u0646\u0635\u0631 \u067E\u0647 ${n.origin} \u06A9\u06D0`;
        default:
          return "\u0646\u0627\u0633\u0645\u0647 \u0648\u0631\u0648\u062F\u064A";
      }
    };
  };
  function _u() {
    return { localeError: q4() };
  }
  var Y4 = () => {
    let r = { string: { unit: "znak\xF3w", verb: "mie\u0107" }, file: { unit: "bajt\xF3w", verb: "mie\u0107" }, array: { unit: "element\xF3w", verb: "mie\u0107" }, set: { unit: "element\xF3w", verb: "mie\u0107" } };
    function i(n) {
      return r[n] ?? null;
    }
    let o = { regex: "wyra\u017Cenie", email: "adres email", url: "URL", emoji: "emoji", uuid: "UUID", uuidv4: "UUIDv4", uuidv6: "UUIDv6", nanoid: "nanoid", guid: "GUID", cuid: "cuid", cuid2: "cuid2", ulid: "ULID", xid: "XID", ksuid: "KSUID", datetime: "data i godzina w formacie ISO", date: "data w formacie ISO", time: "godzina w formacie ISO", duration: "czas trwania ISO", ipv4: "adres IPv4", ipv6: "adres IPv6", cidrv4: "zakres IPv4", cidrv6: "zakres IPv6", base64: "ci\u0105g znak\xF3w zakodowany w formacie base64", base64url: "ci\u0105g znak\xF3w zakodowany w formacie base64url", json_string: "ci\u0105g znak\xF3w w formacie JSON", e164: "liczba E.164", jwt: "JWT", template_literal: "wej\u015Bcie" }, t = { nan: "NaN", number: "liczba", array: "tablica" };
    return (n) => {
      switch (n.code) {
        case "invalid_type": {
          let v = t[n.expected] ?? n.expected, u = k(n.input), $ = t[u] ?? u;
          if (/^[A-Z]/.test(n.expected))
            return `Nieprawid\u0142owe dane wej\u015Bciowe: oczekiwano instanceof ${n.expected}, otrzymano ${$}`;
          return `Nieprawid\u0142owe dane wej\u015Bciowe: oczekiwano ${v}, otrzymano ${$}`;
        }
        case "invalid_value":
          if (n.values.length === 1)
            return `Nieprawid\u0142owe dane wej\u015Bciowe: oczekiwano ${U(n.values[0])}`;
          return `Nieprawid\u0142owa opcja: oczekiwano jednej z warto\u015Bci ${b(n.values, "|")}`;
        case "too_big": {
          let v = n.inclusive ? "<=" : "<", u = i(n.origin);
          if (u)
            return `Za du\u017Ca warto\u015B\u0107: oczekiwano, \u017Ce ${n.origin ?? "warto\u015B\u0107"} b\u0119dzie mie\u0107 ${v}${n.maximum.toString()} ${u.unit ?? "element\xF3w"}`;
          return `Zbyt du\u017C(y/a/e): oczekiwano, \u017Ce ${n.origin ?? "warto\u015B\u0107"} b\u0119dzie wynosi\u0107 ${v}${n.maximum.toString()}`;
        }
        case "too_small": {
          let v = n.inclusive ? ">=" : ">", u = i(n.origin);
          if (u)
            return `Za ma\u0142a warto\u015B\u0107: oczekiwano, \u017Ce ${n.origin ?? "warto\u015B\u0107"} b\u0119dzie mie\u0107 ${v}${n.minimum.toString()} ${u.unit ?? "element\xF3w"}`;
          return `Zbyt ma\u0142(y/a/e): oczekiwano, \u017Ce ${n.origin ?? "warto\u015B\u0107"} b\u0119dzie wynosi\u0107 ${v}${n.minimum.toString()}`;
        }
        case "invalid_format": {
          let v = n;
          if (v.format === "starts_with")
            return `Nieprawid\u0142owy ci\u0105g znak\xF3w: musi zaczyna\u0107 si\u0119 od "${v.prefix}"`;
          if (v.format === "ends_with")
            return `Nieprawid\u0142owy ci\u0105g znak\xF3w: musi ko\u0144czy\u0107 si\u0119 na "${v.suffix}"`;
          if (v.format === "includes")
            return `Nieprawid\u0142owy ci\u0105g znak\xF3w: musi zawiera\u0107 "${v.includes}"`;
          if (v.format === "regex")
            return `Nieprawid\u0142owy ci\u0105g znak\xF3w: musi odpowiada\u0107 wzorcowi ${v.pattern}`;
          return `Nieprawid\u0142ow(y/a/e) ${o[v.format] ?? n.format}`;
        }
        case "not_multiple_of":
          return `Nieprawid\u0142owa liczba: musi by\u0107 wielokrotno\u015Bci\u0105 ${n.divisor}`;
        case "unrecognized_keys":
          return `Nierozpoznane klucze${n.keys.length > 1 ? "s" : ""}: ${b(n.keys, ", ")}`;
        case "invalid_key":
          return `Nieprawid\u0142owy klucz w ${n.origin}`;
        case "invalid_union":
          return "Nieprawid\u0142owe dane wej\u015Bciowe";
        case "invalid_element":
          return `Nieprawid\u0142owa warto\u015B\u0107 w ${n.origin}`;
        default:
          return "Nieprawid\u0142owe dane wej\u015Bciowe";
      }
    };
  };
  function Uu() {
    return { localeError: Y4() };
  }
  var Q4 = () => {
    let r = { string: { unit: "caracteres", verb: "ter" }, file: { unit: "bytes", verb: "ter" }, array: { unit: "itens", verb: "ter" }, set: { unit: "itens", verb: "ter" } };
    function i(n) {
      return r[n] ?? null;
    }
    let o = { regex: "padr\xE3o", email: "endere\xE7o de e-mail", url: "URL", emoji: "emoji", uuid: "UUID", uuidv4: "UUIDv4", uuidv6: "UUIDv6", nanoid: "nanoid", guid: "GUID", cuid: "cuid", cuid2: "cuid2", ulid: "ULID", xid: "XID", ksuid: "KSUID", datetime: "data e hora ISO", date: "data ISO", time: "hora ISO", duration: "dura\xE7\xE3o ISO", ipv4: "endere\xE7o IPv4", ipv6: "endere\xE7o IPv6", cidrv4: "faixa de IPv4", cidrv6: "faixa de IPv6", base64: "texto codificado em base64", base64url: "URL codificada em base64", json_string: "texto JSON", e164: "n\xFAmero E.164", jwt: "JWT", template_literal: "entrada" }, t = { nan: "NaN", number: "n\xFAmero", null: "nulo" };
    return (n) => {
      switch (n.code) {
        case "invalid_type": {
          let v = t[n.expected] ?? n.expected, u = k(n.input), $ = t[u] ?? u;
          if (/^[A-Z]/.test(n.expected))
            return `Tipo inv\xE1lido: esperado instanceof ${n.expected}, recebido ${$}`;
          return `Tipo inv\xE1lido: esperado ${v}, recebido ${$}`;
        }
        case "invalid_value":
          if (n.values.length === 1)
            return `Entrada inv\xE1lida: esperado ${U(n.values[0])}`;
          return `Op\xE7\xE3o inv\xE1lida: esperada uma das ${b(n.values, "|")}`;
        case "too_big": {
          let v = n.inclusive ? "<=" : "<", u = i(n.origin);
          if (u)
            return `Muito grande: esperado que ${n.origin ?? "valor"} tivesse ${v}${n.maximum.toString()} ${u.unit ?? "elementos"}`;
          return `Muito grande: esperado que ${n.origin ?? "valor"} fosse ${v}${n.maximum.toString()}`;
        }
        case "too_small": {
          let v = n.inclusive ? ">=" : ">", u = i(n.origin);
          if (u)
            return `Muito pequeno: esperado que ${n.origin} tivesse ${v}${n.minimum.toString()} ${u.unit}`;
          return `Muito pequeno: esperado que ${n.origin} fosse ${v}${n.minimum.toString()}`;
        }
        case "invalid_format": {
          let v = n;
          if (v.format === "starts_with")
            return `Texto inv\xE1lido: deve come\xE7ar com "${v.prefix}"`;
          if (v.format === "ends_with")
            return `Texto inv\xE1lido: deve terminar com "${v.suffix}"`;
          if (v.format === "includes")
            return `Texto inv\xE1lido: deve incluir "${v.includes}"`;
          if (v.format === "regex")
            return `Texto inv\xE1lido: deve corresponder ao padr\xE3o ${v.pattern}`;
          return `${o[v.format] ?? n.format} inv\xE1lido`;
        }
        case "not_multiple_of":
          return `N\xFAmero inv\xE1lido: deve ser m\xFAltiplo de ${n.divisor}`;
        case "unrecognized_keys":
          return `Chave${n.keys.length > 1 ? "s" : ""} desconhecida${n.keys.length > 1 ? "s" : ""}: ${b(n.keys, ", ")}`;
        case "invalid_key":
          return `Chave inv\xE1lida em ${n.origin}`;
        case "invalid_union":
          return "Entrada inv\xE1lida";
        case "invalid_element":
          return `Valor inv\xE1lido em ${n.origin}`;
        default:
          return "Campo inv\xE1lido";
      }
    };
  };
  function ku() {
    return { localeError: Q4() };
  }
  function nl(r, i, o, t) {
    let n = Math.abs(r), v = n % 10, u = n % 100;
    if (u >= 11 && u <= 19)
      return t;
    if (v === 1)
      return i;
    if (v >= 2 && v <= 4)
      return o;
    return t;
  }
  var m4 = () => {
    let r = { string: { unit: { one: "\u0441\u0438\u043C\u0432\u043E\u043B", few: "\u0441\u0438\u043C\u0432\u043E\u043B\u0430", many: "\u0441\u0438\u043C\u0432\u043E\u043B\u043E\u0432" }, verb: "\u0438\u043C\u0435\u0442\u044C" }, file: { unit: { one: "\u0431\u0430\u0439\u0442", few: "\u0431\u0430\u0439\u0442\u0430", many: "\u0431\u0430\u0439\u0442" }, verb: "\u0438\u043C\u0435\u0442\u044C" }, array: { unit: { one: "\u044D\u043B\u0435\u043C\u0435\u043D\u0442", few: "\u044D\u043B\u0435\u043C\u0435\u043D\u0442\u0430", many: "\u044D\u043B\u0435\u043C\u0435\u043D\u0442\u043E\u0432" }, verb: "\u0438\u043C\u0435\u0442\u044C" }, set: { unit: { one: "\u044D\u043B\u0435\u043C\u0435\u043D\u0442", few: "\u044D\u043B\u0435\u043C\u0435\u043D\u0442\u0430", many: "\u044D\u043B\u0435\u043C\u0435\u043D\u0442\u043E\u0432" }, verb: "\u0438\u043C\u0435\u0442\u044C" } };
    function i(n) {
      return r[n] ?? null;
    }
    let o = { regex: "\u0432\u0432\u043E\u0434", email: "email \u0430\u0434\u0440\u0435\u0441", url: "URL", emoji: "\u044D\u043C\u043E\u0434\u0437\u0438", uuid: "UUID", uuidv4: "UUIDv4", uuidv6: "UUIDv6", nanoid: "nanoid", guid: "GUID", cuid: "cuid", cuid2: "cuid2", ulid: "ULID", xid: "XID", ksuid: "KSUID", datetime: "ISO \u0434\u0430\u0442\u0430 \u0438 \u0432\u0440\u0435\u043C\u044F", date: "ISO \u0434\u0430\u0442\u0430", time: "ISO \u0432\u0440\u0435\u043C\u044F", duration: "ISO \u0434\u043B\u0438\u0442\u0435\u043B\u044C\u043D\u043E\u0441\u0442\u044C", ipv4: "IPv4 \u0430\u0434\u0440\u0435\u0441", ipv6: "IPv6 \u0430\u0434\u0440\u0435\u0441", cidrv4: "IPv4 \u0434\u0438\u0430\u043F\u0430\u0437\u043E\u043D", cidrv6: "IPv6 \u0434\u0438\u0430\u043F\u0430\u0437\u043E\u043D", base64: "\u0441\u0442\u0440\u043E\u043A\u0430 \u0432 \u0444\u043E\u0440\u043C\u0430\u0442\u0435 base64", base64url: "\u0441\u0442\u0440\u043E\u043A\u0430 \u0432 \u0444\u043E\u0440\u043C\u0430\u0442\u0435 base64url", json_string: "JSON \u0441\u0442\u0440\u043E\u043A\u0430", e164: "\u043D\u043E\u043C\u0435\u0440 E.164", jwt: "JWT", template_literal: "\u0432\u0432\u043E\u0434" }, t = { nan: "NaN", number: "\u0447\u0438\u0441\u043B\u043E", array: "\u043C\u0430\u0441\u0441\u0438\u0432" };
    return (n) => {
      switch (n.code) {
        case "invalid_type": {
          let v = t[n.expected] ?? n.expected, u = k(n.input), $ = t[u] ?? u;
          if (/^[A-Z]/.test(n.expected))
            return `\u041D\u0435\u0432\u0435\u0440\u043D\u044B\u0439 \u0432\u0432\u043E\u0434: \u043E\u0436\u0438\u0434\u0430\u043B\u043E\u0441\u044C instanceof ${n.expected}, \u043F\u043E\u043B\u0443\u0447\u0435\u043D\u043E ${$}`;
          return `\u041D\u0435\u0432\u0435\u0440\u043D\u044B\u0439 \u0432\u0432\u043E\u0434: \u043E\u0436\u0438\u0434\u0430\u043B\u043E\u0441\u044C ${v}, \u043F\u043E\u043B\u0443\u0447\u0435\u043D\u043E ${$}`;
        }
        case "invalid_value":
          if (n.values.length === 1)
            return `\u041D\u0435\u0432\u0435\u0440\u043D\u044B\u0439 \u0432\u0432\u043E\u0434: \u043E\u0436\u0438\u0434\u0430\u043B\u043E\u0441\u044C ${U(n.values[0])}`;
          return `\u041D\u0435\u0432\u0435\u0440\u043D\u044B\u0439 \u0432\u0430\u0440\u0438\u0430\u043D\u0442: \u043E\u0436\u0438\u0434\u0430\u043B\u043E\u0441\u044C \u043E\u0434\u043D\u043E \u0438\u0437 ${b(n.values, "|")}`;
        case "too_big": {
          let v = n.inclusive ? "<=" : "<", u = i(n.origin);
          if (u) {
            let $ = Number(n.maximum), l = nl($, u.unit.one, u.unit.few, u.unit.many);
            return `\u0421\u043B\u0438\u0448\u043A\u043E\u043C \u0431\u043E\u043B\u044C\u0448\u043E\u0435 \u0437\u043D\u0430\u0447\u0435\u043D\u0438\u0435: \u043E\u0436\u0438\u0434\u0430\u043B\u043E\u0441\u044C, \u0447\u0442\u043E ${n.origin ?? "\u0437\u043D\u0430\u0447\u0435\u043D\u0438\u0435"} \u0431\u0443\u0434\u0435\u0442 \u0438\u043C\u0435\u0442\u044C ${v}${n.maximum.toString()} ${l}`;
          }
          return `\u0421\u043B\u0438\u0448\u043A\u043E\u043C \u0431\u043E\u043B\u044C\u0448\u043E\u0435 \u0437\u043D\u0430\u0447\u0435\u043D\u0438\u0435: \u043E\u0436\u0438\u0434\u0430\u043B\u043E\u0441\u044C, \u0447\u0442\u043E ${n.origin ?? "\u0437\u043D\u0430\u0447\u0435\u043D\u0438\u0435"} \u0431\u0443\u0434\u0435\u0442 ${v}${n.maximum.toString()}`;
        }
        case "too_small": {
          let v = n.inclusive ? ">=" : ">", u = i(n.origin);
          if (u) {
            let $ = Number(n.minimum), l = nl($, u.unit.one, u.unit.few, u.unit.many);
            return `\u0421\u043B\u0438\u0448\u043A\u043E\u043C \u043C\u0430\u043B\u0435\u043D\u044C\u043A\u043E\u0435 \u0437\u043D\u0430\u0447\u0435\u043D\u0438\u0435: \u043E\u0436\u0438\u0434\u0430\u043B\u043E\u0441\u044C, \u0447\u0442\u043E ${n.origin} \u0431\u0443\u0434\u0435\u0442 \u0438\u043C\u0435\u0442\u044C ${v}${n.minimum.toString()} ${l}`;
          }
          return `\u0421\u043B\u0438\u0448\u043A\u043E\u043C \u043C\u0430\u043B\u0435\u043D\u044C\u043A\u043E\u0435 \u0437\u043D\u0430\u0447\u0435\u043D\u0438\u0435: \u043E\u0436\u0438\u0434\u0430\u043B\u043E\u0441\u044C, \u0447\u0442\u043E ${n.origin} \u0431\u0443\u0434\u0435\u0442 ${v}${n.minimum.toString()}`;
        }
        case "invalid_format": {
          let v = n;
          if (v.format === "starts_with")
            return `\u041D\u0435\u0432\u0435\u0440\u043D\u0430\u044F \u0441\u0442\u0440\u043E\u043A\u0430: \u0434\u043E\u043B\u0436\u043D\u0430 \u043D\u0430\u0447\u0438\u043D\u0430\u0442\u044C\u0441\u044F \u0441 "${v.prefix}"`;
          if (v.format === "ends_with")
            return `\u041D\u0435\u0432\u0435\u0440\u043D\u0430\u044F \u0441\u0442\u0440\u043E\u043A\u0430: \u0434\u043E\u043B\u0436\u043D\u0430 \u0437\u0430\u043A\u0430\u043D\u0447\u0438\u0432\u0430\u0442\u044C\u0441\u044F \u043D\u0430 "${v.suffix}"`;
          if (v.format === "includes")
            return `\u041D\u0435\u0432\u0435\u0440\u043D\u0430\u044F \u0441\u0442\u0440\u043E\u043A\u0430: \u0434\u043E\u043B\u0436\u043D\u0430 \u0441\u043E\u0434\u0435\u0440\u0436\u0430\u0442\u044C "${v.includes}"`;
          if (v.format === "regex")
            return `\u041D\u0435\u0432\u0435\u0440\u043D\u0430\u044F \u0441\u0442\u0440\u043E\u043A\u0430: \u0434\u043E\u043B\u0436\u043D\u0430 \u0441\u043E\u043E\u0442\u0432\u0435\u0442\u0441\u0442\u0432\u043E\u0432\u0430\u0442\u044C \u0448\u0430\u0431\u043B\u043E\u043D\u0443 ${v.pattern}`;
          return `\u041D\u0435\u0432\u0435\u0440\u043D\u044B\u0439 ${o[v.format] ?? n.format}`;
        }
        case "not_multiple_of":
          return `\u041D\u0435\u0432\u0435\u0440\u043D\u043E\u0435 \u0447\u0438\u0441\u043B\u043E: \u0434\u043E\u043B\u0436\u043D\u043E \u0431\u044B\u0442\u044C \u043A\u0440\u0430\u0442\u043D\u044B\u043C ${n.divisor}`;
        case "unrecognized_keys":
          return `\u041D\u0435\u0440\u0430\u0441\u043F\u043E\u0437\u043D\u0430\u043D\u043D${n.keys.length > 1 ? "\u044B\u0435" : "\u044B\u0439"} \u043A\u043B\u044E\u0447${n.keys.length > 1 ? "\u0438" : ""}: ${b(n.keys, ", ")}`;
        case "invalid_key":
          return `\u041D\u0435\u0432\u0435\u0440\u043D\u044B\u0439 \u043A\u043B\u044E\u0447 \u0432 ${n.origin}`;
        case "invalid_union":
          return "\u041D\u0435\u0432\u0435\u0440\u043D\u044B\u0435 \u0432\u0445\u043E\u0434\u043D\u044B\u0435 \u0434\u0430\u043D\u043D\u044B\u0435";
        case "invalid_element":
          return `\u041D\u0435\u0432\u0435\u0440\u043D\u043E\u0435 \u0437\u043D\u0430\u0447\u0435\u043D\u0438\u0435 \u0432 ${n.origin}`;
        default:
          return "\u041D\u0435\u0432\u0435\u0440\u043D\u044B\u0435 \u0432\u0445\u043E\u0434\u043D\u044B\u0435 \u0434\u0430\u043D\u043D\u044B\u0435";
      }
    };
  };
  function Du() {
    return { localeError: m4() };
  }
  var T4 = () => {
    let r = { string: { unit: "znakov", verb: "imeti" }, file: { unit: "bajtov", verb: "imeti" }, array: { unit: "elementov", verb: "imeti" }, set: { unit: "elementov", verb: "imeti" } };
    function i(n) {
      return r[n] ?? null;
    }
    let o = { regex: "vnos", email: "e-po\u0161tni naslov", url: "URL", emoji: "emoji", uuid: "UUID", uuidv4: "UUIDv4", uuidv6: "UUIDv6", nanoid: "nanoid", guid: "GUID", cuid: "cuid", cuid2: "cuid2", ulid: "ULID", xid: "XID", ksuid: "KSUID", datetime: "ISO datum in \u010Das", date: "ISO datum", time: "ISO \u010Das", duration: "ISO trajanje", ipv4: "IPv4 naslov", ipv6: "IPv6 naslov", cidrv4: "obseg IPv4", cidrv6: "obseg IPv6", base64: "base64 kodiran niz", base64url: "base64url kodiran niz", json_string: "JSON niz", e164: "E.164 \u0161tevilka", jwt: "JWT", template_literal: "vnos" }, t = { nan: "NaN", number: "\u0161tevilo", array: "tabela" };
    return (n) => {
      switch (n.code) {
        case "invalid_type": {
          let v = t[n.expected] ?? n.expected, u = k(n.input), $ = t[u] ?? u;
          if (/^[A-Z]/.test(n.expected))
            return `Neveljaven vnos: pri\u010Dakovano instanceof ${n.expected}, prejeto ${$}`;
          return `Neveljaven vnos: pri\u010Dakovano ${v}, prejeto ${$}`;
        }
        case "invalid_value":
          if (n.values.length === 1)
            return `Neveljaven vnos: pri\u010Dakovano ${U(n.values[0])}`;
          return `Neveljavna mo\u017Enost: pri\u010Dakovano eno izmed ${b(n.values, "|")}`;
        case "too_big": {
          let v = n.inclusive ? "<=" : "<", u = i(n.origin);
          if (u)
            return `Preveliko: pri\u010Dakovano, da bo ${n.origin ?? "vrednost"} imelo ${v}${n.maximum.toString()} ${u.unit ?? "elementov"}`;
          return `Preveliko: pri\u010Dakovano, da bo ${n.origin ?? "vrednost"} ${v}${n.maximum.toString()}`;
        }
        case "too_small": {
          let v = n.inclusive ? ">=" : ">", u = i(n.origin);
          if (u)
            return `Premajhno: pri\u010Dakovano, da bo ${n.origin} imelo ${v}${n.minimum.toString()} ${u.unit}`;
          return `Premajhno: pri\u010Dakovano, da bo ${n.origin} ${v}${n.minimum.toString()}`;
        }
        case "invalid_format": {
          let v = n;
          if (v.format === "starts_with")
            return `Neveljaven niz: mora se za\u010Deti z "${v.prefix}"`;
          if (v.format === "ends_with")
            return `Neveljaven niz: mora se kon\u010Dati z "${v.suffix}"`;
          if (v.format === "includes")
            return `Neveljaven niz: mora vsebovati "${v.includes}"`;
          if (v.format === "regex")
            return `Neveljaven niz: mora ustrezati vzorcu ${v.pattern}`;
          return `Neveljaven ${o[v.format] ?? n.format}`;
        }
        case "not_multiple_of":
          return `Neveljavno \u0161tevilo: mora biti ve\u010Dkratnik ${n.divisor}`;
        case "unrecognized_keys":
          return `Neprepoznan${n.keys.length > 1 ? "i klju\u010Di" : " klju\u010D"}: ${b(n.keys, ", ")}`;
        case "invalid_key":
          return `Neveljaven klju\u010D v ${n.origin}`;
        case "invalid_union":
          return "Neveljaven vnos";
        case "invalid_element":
          return `Neveljavna vrednost v ${n.origin}`;
        default:
          return "Neveljaven vnos";
      }
    };
  };
  function wu() {
    return { localeError: T4() };
  }
  var F4 = () => {
    let r = { string: { unit: "tecken", verb: "att ha" }, file: { unit: "bytes", verb: "att ha" }, array: { unit: "objekt", verb: "att inneh\xE5lla" }, set: { unit: "objekt", verb: "att inneh\xE5lla" } };
    function i(n) {
      return r[n] ?? null;
    }
    let o = { regex: "regulj\xE4rt uttryck", email: "e-postadress", url: "URL", emoji: "emoji", uuid: "UUID", uuidv4: "UUIDv4", uuidv6: "UUIDv6", nanoid: "nanoid", guid: "GUID", cuid: "cuid", cuid2: "cuid2", ulid: "ULID", xid: "XID", ksuid: "KSUID", datetime: "ISO-datum och tid", date: "ISO-datum", time: "ISO-tid", duration: "ISO-varaktighet", ipv4: "IPv4-intervall", ipv6: "IPv6-intervall", cidrv4: "IPv4-spektrum", cidrv6: "IPv6-spektrum", base64: "base64-kodad str\xE4ng", base64url: "base64url-kodad str\xE4ng", json_string: "JSON-str\xE4ng", e164: "E.164-nummer", jwt: "JWT", template_literal: "mall-literal" }, t = { nan: "NaN", number: "antal", array: "lista" };
    return (n) => {
      switch (n.code) {
        case "invalid_type": {
          let v = t[n.expected] ?? n.expected, u = k(n.input), $ = t[u] ?? u;
          if (/^[A-Z]/.test(n.expected))
            return `Ogiltig inmatning: f\xF6rv\xE4ntat instanceof ${n.expected}, fick ${$}`;
          return `Ogiltig inmatning: f\xF6rv\xE4ntat ${v}, fick ${$}`;
        }
        case "invalid_value":
          if (n.values.length === 1)
            return `Ogiltig inmatning: f\xF6rv\xE4ntat ${U(n.values[0])}`;
          return `Ogiltigt val: f\xF6rv\xE4ntade en av ${b(n.values, "|")}`;
        case "too_big": {
          let v = n.inclusive ? "<=" : "<", u = i(n.origin);
          if (u)
            return `F\xF6r stor(t): f\xF6rv\xE4ntade ${n.origin ?? "v\xE4rdet"} att ha ${v}${n.maximum.toString()} ${u.unit ?? "element"}`;
          return `F\xF6r stor(t): f\xF6rv\xE4ntat ${n.origin ?? "v\xE4rdet"} att ha ${v}${n.maximum.toString()}`;
        }
        case "too_small": {
          let v = n.inclusive ? ">=" : ">", u = i(n.origin);
          if (u)
            return `F\xF6r lite(t): f\xF6rv\xE4ntade ${n.origin ?? "v\xE4rdet"} att ha ${v}${n.minimum.toString()} ${u.unit}`;
          return `F\xF6r lite(t): f\xF6rv\xE4ntade ${n.origin ?? "v\xE4rdet"} att ha ${v}${n.minimum.toString()}`;
        }
        case "invalid_format": {
          let v = n;
          if (v.format === "starts_with")
            return `Ogiltig str\xE4ng: m\xE5ste b\xF6rja med "${v.prefix}"`;
          if (v.format === "ends_with")
            return `Ogiltig str\xE4ng: m\xE5ste sluta med "${v.suffix}"`;
          if (v.format === "includes")
            return `Ogiltig str\xE4ng: m\xE5ste inneh\xE5lla "${v.includes}"`;
          if (v.format === "regex")
            return `Ogiltig str\xE4ng: m\xE5ste matcha m\xF6nstret "${v.pattern}"`;
          return `Ogiltig(t) ${o[v.format] ?? n.format}`;
        }
        case "not_multiple_of":
          return `Ogiltigt tal: m\xE5ste vara en multipel av ${n.divisor}`;
        case "unrecognized_keys":
          return `${n.keys.length > 1 ? "Ok\xE4nda nycklar" : "Ok\xE4nd nyckel"}: ${b(n.keys, ", ")}`;
        case "invalid_key":
          return `Ogiltig nyckel i ${n.origin ?? "v\xE4rdet"}`;
        case "invalid_union":
          return "Ogiltig input";
        case "invalid_element":
          return `Ogiltigt v\xE4rde i ${n.origin ?? "v\xE4rdet"}`;
        default:
          return "Ogiltig input";
      }
    };
  };
  function Nu() {
    return { localeError: F4() };
  }
  var B4 = () => {
    let r = { string: { unit: "\u0B8E\u0BB4\u0BC1\u0BA4\u0BCD\u0BA4\u0BC1\u0B95\u0BCD\u0B95\u0BB3\u0BCD", verb: "\u0B95\u0BCA\u0BA3\u0BCD\u0B9F\u0BBF\u0BB0\u0BC1\u0B95\u0BCD\u0B95 \u0BB5\u0BC7\u0BA3\u0BCD\u0B9F\u0BC1\u0BAE\u0BCD" }, file: { unit: "\u0BAA\u0BC8\u0B9F\u0BCD\u0B9F\u0BC1\u0B95\u0BB3\u0BCD", verb: "\u0B95\u0BCA\u0BA3\u0BCD\u0B9F\u0BBF\u0BB0\u0BC1\u0B95\u0BCD\u0B95 \u0BB5\u0BC7\u0BA3\u0BCD\u0B9F\u0BC1\u0BAE\u0BCD" }, array: { unit: "\u0B89\u0BB1\u0BC1\u0BAA\u0BCD\u0BAA\u0BC1\u0B95\u0BB3\u0BCD", verb: "\u0B95\u0BCA\u0BA3\u0BCD\u0B9F\u0BBF\u0BB0\u0BC1\u0B95\u0BCD\u0B95 \u0BB5\u0BC7\u0BA3\u0BCD\u0B9F\u0BC1\u0BAE\u0BCD" }, set: { unit: "\u0B89\u0BB1\u0BC1\u0BAA\u0BCD\u0BAA\u0BC1\u0B95\u0BB3\u0BCD", verb: "\u0B95\u0BCA\u0BA3\u0BCD\u0B9F\u0BBF\u0BB0\u0BC1\u0B95\u0BCD\u0B95 \u0BB5\u0BC7\u0BA3\u0BCD\u0B9F\u0BC1\u0BAE\u0BCD" } };
    function i(n) {
      return r[n] ?? null;
    }
    let o = { regex: "\u0B89\u0BB3\u0BCD\u0BB3\u0BC0\u0B9F\u0BC1", email: "\u0BAE\u0BBF\u0BA9\u0BCD\u0BA9\u0B9E\u0BCD\u0B9A\u0BB2\u0BCD \u0BAE\u0BC1\u0B95\u0BB5\u0BB0\u0BBF", url: "URL", emoji: "emoji", uuid: "UUID", uuidv4: "UUIDv4", uuidv6: "UUIDv6", nanoid: "nanoid", guid: "GUID", cuid: "cuid", cuid2: "cuid2", ulid: "ULID", xid: "XID", ksuid: "KSUID", datetime: "ISO \u0BA4\u0BC7\u0BA4\u0BBF \u0BA8\u0BC7\u0BB0\u0BAE\u0BCD", date: "ISO \u0BA4\u0BC7\u0BA4\u0BBF", time: "ISO \u0BA8\u0BC7\u0BB0\u0BAE\u0BCD", duration: "ISO \u0B95\u0BBE\u0BB2 \u0B85\u0BB3\u0BB5\u0BC1", ipv4: "IPv4 \u0BAE\u0BC1\u0B95\u0BB5\u0BB0\u0BBF", ipv6: "IPv6 \u0BAE\u0BC1\u0B95\u0BB5\u0BB0\u0BBF", cidrv4: "IPv4 \u0BB5\u0BB0\u0BAE\u0BCD\u0BAA\u0BC1", cidrv6: "IPv6 \u0BB5\u0BB0\u0BAE\u0BCD\u0BAA\u0BC1", base64: "base64-encoded \u0B9A\u0BB0\u0BAE\u0BCD", base64url: "base64url-encoded \u0B9A\u0BB0\u0BAE\u0BCD", json_string: "JSON \u0B9A\u0BB0\u0BAE\u0BCD", e164: "E.164 \u0B8E\u0BA3\u0BCD", jwt: "JWT", template_literal: "input" }, t = { nan: "NaN", number: "\u0B8E\u0BA3\u0BCD", array: "\u0B85\u0BA3\u0BBF", null: "\u0BB5\u0BC6\u0BB1\u0BC1\u0BAE\u0BC8" };
    return (n) => {
      switch (n.code) {
        case "invalid_type": {
          let v = t[n.expected] ?? n.expected, u = k(n.input), $ = t[u] ?? u;
          if (/^[A-Z]/.test(n.expected))
            return `\u0BA4\u0BB5\u0BB1\u0BBE\u0BA9 \u0B89\u0BB3\u0BCD\u0BB3\u0BC0\u0B9F\u0BC1: \u0B8E\u0BA4\u0BBF\u0BB0\u0BCD\u0BAA\u0BBE\u0BB0\u0BCD\u0B95\u0BCD\u0B95\u0BAA\u0BCD\u0BAA\u0B9F\u0BCD\u0B9F\u0BA4\u0BC1 instanceof ${n.expected}, \u0BAA\u0BC6\u0BB1\u0BAA\u0BCD\u0BAA\u0B9F\u0BCD\u0B9F\u0BA4\u0BC1 ${$}`;
          return `\u0BA4\u0BB5\u0BB1\u0BBE\u0BA9 \u0B89\u0BB3\u0BCD\u0BB3\u0BC0\u0B9F\u0BC1: \u0B8E\u0BA4\u0BBF\u0BB0\u0BCD\u0BAA\u0BBE\u0BB0\u0BCD\u0B95\u0BCD\u0B95\u0BAA\u0BCD\u0BAA\u0B9F\u0BCD\u0B9F\u0BA4\u0BC1 ${v}, \u0BAA\u0BC6\u0BB1\u0BAA\u0BCD\u0BAA\u0B9F\u0BCD\u0B9F\u0BA4\u0BC1 ${$}`;
        }
        case "invalid_value":
          if (n.values.length === 1)
            return `\u0BA4\u0BB5\u0BB1\u0BBE\u0BA9 \u0B89\u0BB3\u0BCD\u0BB3\u0BC0\u0B9F\u0BC1: \u0B8E\u0BA4\u0BBF\u0BB0\u0BCD\u0BAA\u0BBE\u0BB0\u0BCD\u0B95\u0BCD\u0B95\u0BAA\u0BCD\u0BAA\u0B9F\u0BCD\u0B9F\u0BA4\u0BC1 ${U(n.values[0])}`;
          return `\u0BA4\u0BB5\u0BB1\u0BBE\u0BA9 \u0BB5\u0BBF\u0BB0\u0BC1\u0BAA\u0BCD\u0BAA\u0BAE\u0BCD: \u0B8E\u0BA4\u0BBF\u0BB0\u0BCD\u0BAA\u0BBE\u0BB0\u0BCD\u0B95\u0BCD\u0B95\u0BAA\u0BCD\u0BAA\u0B9F\u0BCD\u0B9F\u0BA4\u0BC1 ${b(n.values, "|")} \u0B87\u0BB2\u0BCD \u0B92\u0BA9\u0BCD\u0BB1\u0BC1`;
        case "too_big": {
          let v = n.inclusive ? "<=" : "<", u = i(n.origin);
          if (u)
            return `\u0BAE\u0BBF\u0B95 \u0BAA\u0BC6\u0BB0\u0BBF\u0BAF\u0BA4\u0BC1: \u0B8E\u0BA4\u0BBF\u0BB0\u0BCD\u0BAA\u0BBE\u0BB0\u0BCD\u0B95\u0BCD\u0B95\u0BAA\u0BCD\u0BAA\u0B9F\u0BCD\u0B9F\u0BA4\u0BC1 ${n.origin ?? "\u0BAE\u0BA4\u0BBF\u0BAA\u0BCD\u0BAA\u0BC1"} ${v}${n.maximum.toString()} ${u.unit ?? "\u0B89\u0BB1\u0BC1\u0BAA\u0BCD\u0BAA\u0BC1\u0B95\u0BB3\u0BCD"} \u0B86\u0B95 \u0B87\u0BB0\u0BC1\u0B95\u0BCD\u0B95 \u0BB5\u0BC7\u0BA3\u0BCD\u0B9F\u0BC1\u0BAE\u0BCD`;
          return `\u0BAE\u0BBF\u0B95 \u0BAA\u0BC6\u0BB0\u0BBF\u0BAF\u0BA4\u0BC1: \u0B8E\u0BA4\u0BBF\u0BB0\u0BCD\u0BAA\u0BBE\u0BB0\u0BCD\u0B95\u0BCD\u0B95\u0BAA\u0BCD\u0BAA\u0B9F\u0BCD\u0B9F\u0BA4\u0BC1 ${n.origin ?? "\u0BAE\u0BA4\u0BBF\u0BAA\u0BCD\u0BAA\u0BC1"} ${v}${n.maximum.toString()} \u0B86\u0B95 \u0B87\u0BB0\u0BC1\u0B95\u0BCD\u0B95 \u0BB5\u0BC7\u0BA3\u0BCD\u0B9F\u0BC1\u0BAE\u0BCD`;
        }
        case "too_small": {
          let v = n.inclusive ? ">=" : ">", u = i(n.origin);
          if (u)
            return `\u0BAE\u0BBF\u0B95\u0B9A\u0BCD \u0B9A\u0BBF\u0BB1\u0BBF\u0BAF\u0BA4\u0BC1: \u0B8E\u0BA4\u0BBF\u0BB0\u0BCD\u0BAA\u0BBE\u0BB0\u0BCD\u0B95\u0BCD\u0B95\u0BAA\u0BCD\u0BAA\u0B9F\u0BCD\u0B9F\u0BA4\u0BC1 ${n.origin} ${v}${n.minimum.toString()} ${u.unit} \u0B86\u0B95 \u0B87\u0BB0\u0BC1\u0B95\u0BCD\u0B95 \u0BB5\u0BC7\u0BA3\u0BCD\u0B9F\u0BC1\u0BAE\u0BCD`;
          return `\u0BAE\u0BBF\u0B95\u0B9A\u0BCD \u0B9A\u0BBF\u0BB1\u0BBF\u0BAF\u0BA4\u0BC1: \u0B8E\u0BA4\u0BBF\u0BB0\u0BCD\u0BAA\u0BBE\u0BB0\u0BCD\u0B95\u0BCD\u0B95\u0BAA\u0BCD\u0BAA\u0B9F\u0BCD\u0B9F\u0BA4\u0BC1 ${n.origin} ${v}${n.minimum.toString()} \u0B86\u0B95 \u0B87\u0BB0\u0BC1\u0B95\u0BCD\u0B95 \u0BB5\u0BC7\u0BA3\u0BCD\u0B9F\u0BC1\u0BAE\u0BCD`;
        }
        case "invalid_format": {
          let v = n;
          if (v.format === "starts_with")
            return `\u0BA4\u0BB5\u0BB1\u0BBE\u0BA9 \u0B9A\u0BB0\u0BAE\u0BCD: "${v.prefix}" \u0B87\u0BB2\u0BCD \u0BA4\u0BCA\u0B9F\u0B99\u0BCD\u0B95 \u0BB5\u0BC7\u0BA3\u0BCD\u0B9F\u0BC1\u0BAE\u0BCD`;
          if (v.format === "ends_with")
            return `\u0BA4\u0BB5\u0BB1\u0BBE\u0BA9 \u0B9A\u0BB0\u0BAE\u0BCD: "${v.suffix}" \u0B87\u0BB2\u0BCD \u0BAE\u0BC1\u0B9F\u0BBF\u0BB5\u0B9F\u0BC8\u0BAF \u0BB5\u0BC7\u0BA3\u0BCD\u0B9F\u0BC1\u0BAE\u0BCD`;
          if (v.format === "includes")
            return `\u0BA4\u0BB5\u0BB1\u0BBE\u0BA9 \u0B9A\u0BB0\u0BAE\u0BCD: "${v.includes}" \u0B90 \u0B89\u0BB3\u0BCD\u0BB3\u0B9F\u0B95\u0BCD\u0B95 \u0BB5\u0BC7\u0BA3\u0BCD\u0B9F\u0BC1\u0BAE\u0BCD`;
          if (v.format === "regex")
            return `\u0BA4\u0BB5\u0BB1\u0BBE\u0BA9 \u0B9A\u0BB0\u0BAE\u0BCD: ${v.pattern} \u0BAE\u0BC1\u0BB1\u0BC8\u0BAA\u0BBE\u0B9F\u0BCD\u0B9F\u0BC1\u0B9F\u0BA9\u0BCD \u0BAA\u0BCA\u0BB0\u0BC1\u0BA8\u0BCD\u0BA4 \u0BB5\u0BC7\u0BA3\u0BCD\u0B9F\u0BC1\u0BAE\u0BCD`;
          return `\u0BA4\u0BB5\u0BB1\u0BBE\u0BA9 ${o[v.format] ?? n.format}`;
        }
        case "not_multiple_of":
          return `\u0BA4\u0BB5\u0BB1\u0BBE\u0BA9 \u0B8E\u0BA3\u0BCD: ${n.divisor} \u0B87\u0BA9\u0BCD \u0BAA\u0BB2\u0BAE\u0BBE\u0B95 \u0B87\u0BB0\u0BC1\u0B95\u0BCD\u0B95 \u0BB5\u0BC7\u0BA3\u0BCD\u0B9F\u0BC1\u0BAE\u0BCD`;
        case "unrecognized_keys":
          return `\u0B85\u0B9F\u0BC8\u0BAF\u0BBE\u0BB3\u0BAE\u0BCD \u0BA4\u0BC6\u0BB0\u0BBF\u0BAF\u0BBE\u0BA4 \u0BB5\u0BBF\u0B9A\u0BC8${n.keys.length > 1 ? "\u0B95\u0BB3\u0BCD" : ""}: ${b(n.keys, ", ")}`;
        case "invalid_key":
          return `${n.origin} \u0B87\u0BB2\u0BCD \u0BA4\u0BB5\u0BB1\u0BBE\u0BA9 \u0BB5\u0BBF\u0B9A\u0BC8`;
        case "invalid_union":
          return "\u0BA4\u0BB5\u0BB1\u0BBE\u0BA9 \u0B89\u0BB3\u0BCD\u0BB3\u0BC0\u0B9F\u0BC1";
        case "invalid_element":
          return `${n.origin} \u0B87\u0BB2\u0BCD \u0BA4\u0BB5\u0BB1\u0BBE\u0BA9 \u0BAE\u0BA4\u0BBF\u0BAA\u0BCD\u0BAA\u0BC1`;
        default:
          return "\u0BA4\u0BB5\u0BB1\u0BBE\u0BA9 \u0B89\u0BB3\u0BCD\u0BB3\u0BC0\u0B9F\u0BC1";
      }
    };
  };
  function Ou() {
    return { localeError: B4() };
  }
  var H4 = () => {
    let r = { string: { unit: "\u0E15\u0E31\u0E27\u0E2D\u0E31\u0E01\u0E29\u0E23", verb: "\u0E04\u0E27\u0E23\u0E21\u0E35" }, file: { unit: "\u0E44\u0E1A\u0E15\u0E4C", verb: "\u0E04\u0E27\u0E23\u0E21\u0E35" }, array: { unit: "\u0E23\u0E32\u0E22\u0E01\u0E32\u0E23", verb: "\u0E04\u0E27\u0E23\u0E21\u0E35" }, set: { unit: "\u0E23\u0E32\u0E22\u0E01\u0E32\u0E23", verb: "\u0E04\u0E27\u0E23\u0E21\u0E35" } };
    function i(n) {
      return r[n] ?? null;
    }
    let o = { regex: "\u0E02\u0E49\u0E2D\u0E21\u0E39\u0E25\u0E17\u0E35\u0E48\u0E1B\u0E49\u0E2D\u0E19", email: "\u0E17\u0E35\u0E48\u0E2D\u0E22\u0E39\u0E48\u0E2D\u0E35\u0E40\u0E21\u0E25", url: "URL", emoji: "\u0E2D\u0E34\u0E42\u0E21\u0E08\u0E34", uuid: "UUID", uuidv4: "UUIDv4", uuidv6: "UUIDv6", nanoid: "nanoid", guid: "GUID", cuid: "cuid", cuid2: "cuid2", ulid: "ULID", xid: "XID", ksuid: "KSUID", datetime: "\u0E27\u0E31\u0E19\u0E17\u0E35\u0E48\u0E40\u0E27\u0E25\u0E32\u0E41\u0E1A\u0E1A ISO", date: "\u0E27\u0E31\u0E19\u0E17\u0E35\u0E48\u0E41\u0E1A\u0E1A ISO", time: "\u0E40\u0E27\u0E25\u0E32\u0E41\u0E1A\u0E1A ISO", duration: "\u0E0A\u0E48\u0E27\u0E07\u0E40\u0E27\u0E25\u0E32\u0E41\u0E1A\u0E1A ISO", ipv4: "\u0E17\u0E35\u0E48\u0E2D\u0E22\u0E39\u0E48 IPv4", ipv6: "\u0E17\u0E35\u0E48\u0E2D\u0E22\u0E39\u0E48 IPv6", cidrv4: "\u0E0A\u0E48\u0E27\u0E07 IP \u0E41\u0E1A\u0E1A IPv4", cidrv6: "\u0E0A\u0E48\u0E27\u0E07 IP \u0E41\u0E1A\u0E1A IPv6", base64: "\u0E02\u0E49\u0E2D\u0E04\u0E27\u0E32\u0E21\u0E41\u0E1A\u0E1A Base64", base64url: "\u0E02\u0E49\u0E2D\u0E04\u0E27\u0E32\u0E21\u0E41\u0E1A\u0E1A Base64 \u0E2A\u0E33\u0E2B\u0E23\u0E31\u0E1A URL", json_string: "\u0E02\u0E49\u0E2D\u0E04\u0E27\u0E32\u0E21\u0E41\u0E1A\u0E1A JSON", e164: "\u0E40\u0E1A\u0E2D\u0E23\u0E4C\u0E42\u0E17\u0E23\u0E28\u0E31\u0E1E\u0E17\u0E4C\u0E23\u0E30\u0E2B\u0E27\u0E48\u0E32\u0E07\u0E1B\u0E23\u0E30\u0E40\u0E17\u0E28 (E.164)", jwt: "\u0E42\u0E17\u0E40\u0E04\u0E19 JWT", template_literal: "\u0E02\u0E49\u0E2D\u0E21\u0E39\u0E25\u0E17\u0E35\u0E48\u0E1B\u0E49\u0E2D\u0E19" }, t = { nan: "NaN", number: "\u0E15\u0E31\u0E27\u0E40\u0E25\u0E02", array: "\u0E2D\u0E32\u0E23\u0E4C\u0E40\u0E23\u0E22\u0E4C (Array)", null: "\u0E44\u0E21\u0E48\u0E21\u0E35\u0E04\u0E48\u0E32 (null)" };
    return (n) => {
      switch (n.code) {
        case "invalid_type": {
          let v = t[n.expected] ?? n.expected, u = k(n.input), $ = t[u] ?? u;
          if (/^[A-Z]/.test(n.expected))
            return `\u0E1B\u0E23\u0E30\u0E40\u0E20\u0E17\u0E02\u0E49\u0E2D\u0E21\u0E39\u0E25\u0E44\u0E21\u0E48\u0E16\u0E39\u0E01\u0E15\u0E49\u0E2D\u0E07: \u0E04\u0E27\u0E23\u0E40\u0E1B\u0E47\u0E19 instanceof ${n.expected} \u0E41\u0E15\u0E48\u0E44\u0E14\u0E49\u0E23\u0E31\u0E1A ${$}`;
          return `\u0E1B\u0E23\u0E30\u0E40\u0E20\u0E17\u0E02\u0E49\u0E2D\u0E21\u0E39\u0E25\u0E44\u0E21\u0E48\u0E16\u0E39\u0E01\u0E15\u0E49\u0E2D\u0E07: \u0E04\u0E27\u0E23\u0E40\u0E1B\u0E47\u0E19 ${v} \u0E41\u0E15\u0E48\u0E44\u0E14\u0E49\u0E23\u0E31\u0E1A ${$}`;
        }
        case "invalid_value":
          if (n.values.length === 1)
            return `\u0E04\u0E48\u0E32\u0E44\u0E21\u0E48\u0E16\u0E39\u0E01\u0E15\u0E49\u0E2D\u0E07: \u0E04\u0E27\u0E23\u0E40\u0E1B\u0E47\u0E19 ${U(n.values[0])}`;
          return `\u0E15\u0E31\u0E27\u0E40\u0E25\u0E37\u0E2D\u0E01\u0E44\u0E21\u0E48\u0E16\u0E39\u0E01\u0E15\u0E49\u0E2D\u0E07: \u0E04\u0E27\u0E23\u0E40\u0E1B\u0E47\u0E19\u0E2B\u0E19\u0E36\u0E48\u0E07\u0E43\u0E19 ${b(n.values, "|")}`;
        case "too_big": {
          let v = n.inclusive ? "\u0E44\u0E21\u0E48\u0E40\u0E01\u0E34\u0E19" : "\u0E19\u0E49\u0E2D\u0E22\u0E01\u0E27\u0E48\u0E32", u = i(n.origin);
          if (u)
            return `\u0E40\u0E01\u0E34\u0E19\u0E01\u0E33\u0E2B\u0E19\u0E14: ${n.origin ?? "\u0E04\u0E48\u0E32"} \u0E04\u0E27\u0E23\u0E21\u0E35${v} ${n.maximum.toString()} ${u.unit ?? "\u0E23\u0E32\u0E22\u0E01\u0E32\u0E23"}`;
          return `\u0E40\u0E01\u0E34\u0E19\u0E01\u0E33\u0E2B\u0E19\u0E14: ${n.origin ?? "\u0E04\u0E48\u0E32"} \u0E04\u0E27\u0E23\u0E21\u0E35${v} ${n.maximum.toString()}`;
        }
        case "too_small": {
          let v = n.inclusive ? "\u0E2D\u0E22\u0E48\u0E32\u0E07\u0E19\u0E49\u0E2D\u0E22" : "\u0E21\u0E32\u0E01\u0E01\u0E27\u0E48\u0E32", u = i(n.origin);
          if (u)
            return `\u0E19\u0E49\u0E2D\u0E22\u0E01\u0E27\u0E48\u0E32\u0E01\u0E33\u0E2B\u0E19\u0E14: ${n.origin} \u0E04\u0E27\u0E23\u0E21\u0E35${v} ${n.minimum.toString()} ${u.unit}`;
          return `\u0E19\u0E49\u0E2D\u0E22\u0E01\u0E27\u0E48\u0E32\u0E01\u0E33\u0E2B\u0E19\u0E14: ${n.origin} \u0E04\u0E27\u0E23\u0E21\u0E35${v} ${n.minimum.toString()}`;
        }
        case "invalid_format": {
          let v = n;
          if (v.format === "starts_with")
            return `\u0E23\u0E39\u0E1B\u0E41\u0E1A\u0E1A\u0E44\u0E21\u0E48\u0E16\u0E39\u0E01\u0E15\u0E49\u0E2D\u0E07: \u0E02\u0E49\u0E2D\u0E04\u0E27\u0E32\u0E21\u0E15\u0E49\u0E2D\u0E07\u0E02\u0E36\u0E49\u0E19\u0E15\u0E49\u0E19\u0E14\u0E49\u0E27\u0E22 "${v.prefix}"`;
          if (v.format === "ends_with")
            return `\u0E23\u0E39\u0E1B\u0E41\u0E1A\u0E1A\u0E44\u0E21\u0E48\u0E16\u0E39\u0E01\u0E15\u0E49\u0E2D\u0E07: \u0E02\u0E49\u0E2D\u0E04\u0E27\u0E32\u0E21\u0E15\u0E49\u0E2D\u0E07\u0E25\u0E07\u0E17\u0E49\u0E32\u0E22\u0E14\u0E49\u0E27\u0E22 "${v.suffix}"`;
          if (v.format === "includes")
            return `\u0E23\u0E39\u0E1B\u0E41\u0E1A\u0E1A\u0E44\u0E21\u0E48\u0E16\u0E39\u0E01\u0E15\u0E49\u0E2D\u0E07: \u0E02\u0E49\u0E2D\u0E04\u0E27\u0E32\u0E21\u0E15\u0E49\u0E2D\u0E07\u0E21\u0E35 "${v.includes}" \u0E2D\u0E22\u0E39\u0E48\u0E43\u0E19\u0E02\u0E49\u0E2D\u0E04\u0E27\u0E32\u0E21`;
          if (v.format === "regex")
            return `\u0E23\u0E39\u0E1B\u0E41\u0E1A\u0E1A\u0E44\u0E21\u0E48\u0E16\u0E39\u0E01\u0E15\u0E49\u0E2D\u0E07: \u0E15\u0E49\u0E2D\u0E07\u0E15\u0E23\u0E07\u0E01\u0E31\u0E1A\u0E23\u0E39\u0E1B\u0E41\u0E1A\u0E1A\u0E17\u0E35\u0E48\u0E01\u0E33\u0E2B\u0E19\u0E14 ${v.pattern}`;
          return `\u0E23\u0E39\u0E1B\u0E41\u0E1A\u0E1A\u0E44\u0E21\u0E48\u0E16\u0E39\u0E01\u0E15\u0E49\u0E2D\u0E07: ${o[v.format] ?? n.format}`;
        }
        case "not_multiple_of":
          return `\u0E15\u0E31\u0E27\u0E40\u0E25\u0E02\u0E44\u0E21\u0E48\u0E16\u0E39\u0E01\u0E15\u0E49\u0E2D\u0E07: \u0E15\u0E49\u0E2D\u0E07\u0E40\u0E1B\u0E47\u0E19\u0E08\u0E33\u0E19\u0E27\u0E19\u0E17\u0E35\u0E48\u0E2B\u0E32\u0E23\u0E14\u0E49\u0E27\u0E22 ${n.divisor} \u0E44\u0E14\u0E49\u0E25\u0E07\u0E15\u0E31\u0E27`;
        case "unrecognized_keys":
          return `\u0E1E\u0E1A\u0E04\u0E35\u0E22\u0E4C\u0E17\u0E35\u0E48\u0E44\u0E21\u0E48\u0E23\u0E39\u0E49\u0E08\u0E31\u0E01: ${b(n.keys, ", ")}`;
        case "invalid_key":
          return `\u0E04\u0E35\u0E22\u0E4C\u0E44\u0E21\u0E48\u0E16\u0E39\u0E01\u0E15\u0E49\u0E2D\u0E07\u0E43\u0E19 ${n.origin}`;
        case "invalid_union":
          return "\u0E02\u0E49\u0E2D\u0E21\u0E39\u0E25\u0E44\u0E21\u0E48\u0E16\u0E39\u0E01\u0E15\u0E49\u0E2D\u0E07: \u0E44\u0E21\u0E48\u0E15\u0E23\u0E07\u0E01\u0E31\u0E1A\u0E23\u0E39\u0E1B\u0E41\u0E1A\u0E1A\u0E22\u0E39\u0E40\u0E19\u0E35\u0E22\u0E19\u0E17\u0E35\u0E48\u0E01\u0E33\u0E2B\u0E19\u0E14\u0E44\u0E27\u0E49";
        case "invalid_element":
          return `\u0E02\u0E49\u0E2D\u0E21\u0E39\u0E25\u0E44\u0E21\u0E48\u0E16\u0E39\u0E01\u0E15\u0E49\u0E2D\u0E07\u0E43\u0E19 ${n.origin}`;
        default:
          return "\u0E02\u0E49\u0E2D\u0E21\u0E39\u0E25\u0E44\u0E21\u0E48\u0E16\u0E39\u0E01\u0E15\u0E49\u0E2D\u0E07";
      }
    };
  };
  function Su() {
    return { localeError: H4() };
  }
  var M4 = () => {
    let r = { string: { unit: "karakter", verb: "olmal\u0131" }, file: { unit: "bayt", verb: "olmal\u0131" }, array: { unit: "\xF6\u011Fe", verb: "olmal\u0131" }, set: { unit: "\xF6\u011Fe", verb: "olmal\u0131" } };
    function i(n) {
      return r[n] ?? null;
    }
    let o = { regex: "girdi", email: "e-posta adresi", url: "URL", emoji: "emoji", uuid: "UUID", uuidv4: "UUIDv4", uuidv6: "UUIDv6", nanoid: "nanoid", guid: "GUID", cuid: "cuid", cuid2: "cuid2", ulid: "ULID", xid: "XID", ksuid: "KSUID", datetime: "ISO tarih ve saat", date: "ISO tarih", time: "ISO saat", duration: "ISO s\xFCre", ipv4: "IPv4 adresi", ipv6: "IPv6 adresi", cidrv4: "IPv4 aral\u0131\u011F\u0131", cidrv6: "IPv6 aral\u0131\u011F\u0131", base64: "base64 ile \u015Fifrelenmi\u015F metin", base64url: "base64url ile \u015Fifrelenmi\u015F metin", json_string: "JSON dizesi", e164: "E.164 say\u0131s\u0131", jwt: "JWT", template_literal: "\u015Eablon dizesi" }, t = { nan: "NaN" };
    return (n) => {
      switch (n.code) {
        case "invalid_type": {
          let v = t[n.expected] ?? n.expected, u = k(n.input), $ = t[u] ?? u;
          if (/^[A-Z]/.test(n.expected))
            return `Ge\xE7ersiz de\u011Fer: beklenen instanceof ${n.expected}, al\u0131nan ${$}`;
          return `Ge\xE7ersiz de\u011Fer: beklenen ${v}, al\u0131nan ${$}`;
        }
        case "invalid_value":
          if (n.values.length === 1)
            return `Ge\xE7ersiz de\u011Fer: beklenen ${U(n.values[0])}`;
          return `Ge\xE7ersiz se\xE7enek: a\u015Fa\u011F\u0131dakilerden biri olmal\u0131: ${b(n.values, "|")}`;
        case "too_big": {
          let v = n.inclusive ? "<=" : "<", u = i(n.origin);
          if (u)
            return `\xC7ok b\xFCy\xFCk: beklenen ${n.origin ?? "de\u011Fer"} ${v}${n.maximum.toString()} ${u.unit ?? "\xF6\u011Fe"}`;
          return `\xC7ok b\xFCy\xFCk: beklenen ${n.origin ?? "de\u011Fer"} ${v}${n.maximum.toString()}`;
        }
        case "too_small": {
          let v = n.inclusive ? ">=" : ">", u = i(n.origin);
          if (u)
            return `\xC7ok k\xFC\xE7\xFCk: beklenen ${n.origin} ${v}${n.minimum.toString()} ${u.unit}`;
          return `\xC7ok k\xFC\xE7\xFCk: beklenen ${n.origin} ${v}${n.minimum.toString()}`;
        }
        case "invalid_format": {
          let v = n;
          if (v.format === "starts_with")
            return `Ge\xE7ersiz metin: "${v.prefix}" ile ba\u015Flamal\u0131`;
          if (v.format === "ends_with")
            return `Ge\xE7ersiz metin: "${v.suffix}" ile bitmeli`;
          if (v.format === "includes")
            return `Ge\xE7ersiz metin: "${v.includes}" i\xE7ermeli`;
          if (v.format === "regex")
            return `Ge\xE7ersiz metin: ${v.pattern} desenine uymal\u0131`;
          return `Ge\xE7ersiz ${o[v.format] ?? n.format}`;
        }
        case "not_multiple_of":
          return `Ge\xE7ersiz say\u0131: ${n.divisor} ile tam b\xF6l\xFCnebilmeli`;
        case "unrecognized_keys":
          return `Tan\u0131nmayan anahtar${n.keys.length > 1 ? "lar" : ""}: ${b(n.keys, ", ")}`;
        case "invalid_key":
          return `${n.origin} i\xE7inde ge\xE7ersiz anahtar`;
        case "invalid_union":
          return "Ge\xE7ersiz de\u011Fer";
        case "invalid_element":
          return `${n.origin} i\xE7inde ge\xE7ersiz de\u011Fer`;
        default:
          return "Ge\xE7ersiz de\u011Fer";
      }
    };
  };
  function zu() {
    return { localeError: M4() };
  }
  var R4 = () => {
    let r = { string: { unit: "\u0441\u0438\u043C\u0432\u043E\u043B\u0456\u0432", verb: "\u043C\u0430\u0442\u0438\u043C\u0435" }, file: { unit: "\u0431\u0430\u0439\u0442\u0456\u0432", verb: "\u043C\u0430\u0442\u0438\u043C\u0435" }, array: { unit: "\u0435\u043B\u0435\u043C\u0435\u043D\u0442\u0456\u0432", verb: "\u043C\u0430\u0442\u0438\u043C\u0435" }, set: { unit: "\u0435\u043B\u0435\u043C\u0435\u043D\u0442\u0456\u0432", verb: "\u043C\u0430\u0442\u0438\u043C\u0435" } };
    function i(n) {
      return r[n] ?? null;
    }
    let o = { regex: "\u0432\u0445\u0456\u0434\u043D\u0456 \u0434\u0430\u043D\u0456", email: "\u0430\u0434\u0440\u0435\u0441\u0430 \u0435\u043B\u0435\u043A\u0442\u0440\u043E\u043D\u043D\u043E\u0457 \u043F\u043E\u0448\u0442\u0438", url: "URL", emoji: "\u0435\u043C\u043E\u0434\u0437\u0456", uuid: "UUID", uuidv4: "UUIDv4", uuidv6: "UUIDv6", nanoid: "nanoid", guid: "GUID", cuid: "cuid", cuid2: "cuid2", ulid: "ULID", xid: "XID", ksuid: "KSUID", datetime: "\u0434\u0430\u0442\u0430 \u0442\u0430 \u0447\u0430\u0441 ISO", date: "\u0434\u0430\u0442\u0430 ISO", time: "\u0447\u0430\u0441 ISO", duration: "\u0442\u0440\u0438\u0432\u0430\u043B\u0456\u0441\u0442\u044C ISO", ipv4: "\u0430\u0434\u0440\u0435\u0441\u0430 IPv4", ipv6: "\u0430\u0434\u0440\u0435\u0441\u0430 IPv6", cidrv4: "\u0434\u0456\u0430\u043F\u0430\u0437\u043E\u043D IPv4", cidrv6: "\u0434\u0456\u0430\u043F\u0430\u0437\u043E\u043D IPv6", base64: "\u0440\u044F\u0434\u043E\u043A \u0443 \u043A\u043E\u0434\u0443\u0432\u0430\u043D\u043D\u0456 base64", base64url: "\u0440\u044F\u0434\u043E\u043A \u0443 \u043A\u043E\u0434\u0443\u0432\u0430\u043D\u043D\u0456 base64url", json_string: "\u0440\u044F\u0434\u043E\u043A JSON", e164: "\u043D\u043E\u043C\u0435\u0440 E.164", jwt: "JWT", template_literal: "\u0432\u0445\u0456\u0434\u043D\u0456 \u0434\u0430\u043D\u0456" }, t = { nan: "NaN", number: "\u0447\u0438\u0441\u043B\u043E", array: "\u043C\u0430\u0441\u0438\u0432" };
    return (n) => {
      switch (n.code) {
        case "invalid_type": {
          let v = t[n.expected] ?? n.expected, u = k(n.input), $ = t[u] ?? u;
          if (/^[A-Z]/.test(n.expected))
            return `\u041D\u0435\u043F\u0440\u0430\u0432\u0438\u043B\u044C\u043D\u0456 \u0432\u0445\u0456\u0434\u043D\u0456 \u0434\u0430\u043D\u0456: \u043E\u0447\u0456\u043A\u0443\u0454\u0442\u044C\u0441\u044F instanceof ${n.expected}, \u043E\u0442\u0440\u0438\u043C\u0430\u043D\u043E ${$}`;
          return `\u041D\u0435\u043F\u0440\u0430\u0432\u0438\u043B\u044C\u043D\u0456 \u0432\u0445\u0456\u0434\u043D\u0456 \u0434\u0430\u043D\u0456: \u043E\u0447\u0456\u043A\u0443\u0454\u0442\u044C\u0441\u044F ${v}, \u043E\u0442\u0440\u0438\u043C\u0430\u043D\u043E ${$}`;
        }
        case "invalid_value":
          if (n.values.length === 1)
            return `\u041D\u0435\u043F\u0440\u0430\u0432\u0438\u043B\u044C\u043D\u0456 \u0432\u0445\u0456\u0434\u043D\u0456 \u0434\u0430\u043D\u0456: \u043E\u0447\u0456\u043A\u0443\u0454\u0442\u044C\u0441\u044F ${U(n.values[0])}`;
          return `\u041D\u0435\u043F\u0440\u0430\u0432\u0438\u043B\u044C\u043D\u0430 \u043E\u043F\u0446\u0456\u044F: \u043E\u0447\u0456\u043A\u0443\u0454\u0442\u044C\u0441\u044F \u043E\u0434\u043D\u0435 \u0437 ${b(n.values, "|")}`;
        case "too_big": {
          let v = n.inclusive ? "<=" : "<", u = i(n.origin);
          if (u)
            return `\u0417\u0430\u043D\u0430\u0434\u0442\u043E \u0432\u0435\u043B\u0438\u043A\u0435: \u043E\u0447\u0456\u043A\u0443\u0454\u0442\u044C\u0441\u044F, \u0449\u043E ${n.origin ?? "\u0437\u043D\u0430\u0447\u0435\u043D\u043D\u044F"} ${u.verb} ${v}${n.maximum.toString()} ${u.unit ?? "\u0435\u043B\u0435\u043C\u0435\u043D\u0442\u0456\u0432"}`;
          return `\u0417\u0430\u043D\u0430\u0434\u0442\u043E \u0432\u0435\u043B\u0438\u043A\u0435: \u043E\u0447\u0456\u043A\u0443\u0454\u0442\u044C\u0441\u044F, \u0449\u043E ${n.origin ?? "\u0437\u043D\u0430\u0447\u0435\u043D\u043D\u044F"} \u0431\u0443\u0434\u0435 ${v}${n.maximum.toString()}`;
        }
        case "too_small": {
          let v = n.inclusive ? ">=" : ">", u = i(n.origin);
          if (u)
            return `\u0417\u0430\u043D\u0430\u0434\u0442\u043E \u043C\u0430\u043B\u0435: \u043E\u0447\u0456\u043A\u0443\u0454\u0442\u044C\u0441\u044F, \u0449\u043E ${n.origin} ${u.verb} ${v}${n.minimum.toString()} ${u.unit}`;
          return `\u0417\u0430\u043D\u0430\u0434\u0442\u043E \u043C\u0430\u043B\u0435: \u043E\u0447\u0456\u043A\u0443\u0454\u0442\u044C\u0441\u044F, \u0449\u043E ${n.origin} \u0431\u0443\u0434\u0435 ${v}${n.minimum.toString()}`;
        }
        case "invalid_format": {
          let v = n;
          if (v.format === "starts_with")
            return `\u041D\u0435\u043F\u0440\u0430\u0432\u0438\u043B\u044C\u043D\u0438\u0439 \u0440\u044F\u0434\u043E\u043A: \u043F\u043E\u0432\u0438\u043D\u0435\u043D \u043F\u043E\u0447\u0438\u043D\u0430\u0442\u0438\u0441\u044F \u0437 "${v.prefix}"`;
          if (v.format === "ends_with")
            return `\u041D\u0435\u043F\u0440\u0430\u0432\u0438\u043B\u044C\u043D\u0438\u0439 \u0440\u044F\u0434\u043E\u043A: \u043F\u043E\u0432\u0438\u043D\u0435\u043D \u0437\u0430\u043A\u0456\u043D\u0447\u0443\u0432\u0430\u0442\u0438\u0441\u044F \u043D\u0430 "${v.suffix}"`;
          if (v.format === "includes")
            return `\u041D\u0435\u043F\u0440\u0430\u0432\u0438\u043B\u044C\u043D\u0438\u0439 \u0440\u044F\u0434\u043E\u043A: \u043F\u043E\u0432\u0438\u043D\u0435\u043D \u043C\u0456\u0441\u0442\u0438\u0442\u0438 "${v.includes}"`;
          if (v.format === "regex")
            return `\u041D\u0435\u043F\u0440\u0430\u0432\u0438\u043B\u044C\u043D\u0438\u0439 \u0440\u044F\u0434\u043E\u043A: \u043F\u043E\u0432\u0438\u043D\u0435\u043D \u0432\u0456\u0434\u043F\u043E\u0432\u0456\u0434\u0430\u0442\u0438 \u0448\u0430\u0431\u043B\u043E\u043D\u0443 ${v.pattern}`;
          return `\u041D\u0435\u043F\u0440\u0430\u0432\u0438\u043B\u044C\u043D\u0438\u0439 ${o[v.format] ?? n.format}`;
        }
        case "not_multiple_of":
          return `\u041D\u0435\u043F\u0440\u0430\u0432\u0438\u043B\u044C\u043D\u0435 \u0447\u0438\u0441\u043B\u043E: \u043F\u043E\u0432\u0438\u043D\u043D\u043E \u0431\u0443\u0442\u0438 \u043A\u0440\u0430\u0442\u043D\u0438\u043C ${n.divisor}`;
        case "unrecognized_keys":
          return `\u041D\u0435\u0440\u043E\u0437\u043F\u0456\u0437\u043D\u0430\u043D\u0438\u0439 \u043A\u043B\u044E\u0447${n.keys.length > 1 ? "\u0456" : ""}: ${b(n.keys, ", ")}`;
        case "invalid_key":
          return `\u041D\u0435\u043F\u0440\u0430\u0432\u0438\u043B\u044C\u043D\u0438\u0439 \u043A\u043B\u044E\u0447 \u0443 ${n.origin}`;
        case "invalid_union":
          return "\u041D\u0435\u043F\u0440\u0430\u0432\u0438\u043B\u044C\u043D\u0456 \u0432\u0445\u0456\u0434\u043D\u0456 \u0434\u0430\u043D\u0456";
        case "invalid_element":
          return `\u041D\u0435\u043F\u0440\u0430\u0432\u0438\u043B\u044C\u043D\u0435 \u0437\u043D\u0430\u0447\u0435\u043D\u043D\u044F \u0443 ${n.origin}`;
        default:
          return "\u041D\u0435\u043F\u0440\u0430\u0432\u0438\u043B\u044C\u043D\u0456 \u0432\u0445\u0456\u0434\u043D\u0456 \u0434\u0430\u043D\u0456";
      }
    };
  };
  function Nn() {
    return { localeError: R4() };
  }
  function Pu() {
    return Nn();
  }
  var x4 = () => {
    let r = { string: { unit: "\u062D\u0631\u0648\u0641", verb: "\u06C1\u0648\u0646\u0627" }, file: { unit: "\u0628\u0627\u0626\u0679\u0633", verb: "\u06C1\u0648\u0646\u0627" }, array: { unit: "\u0622\u0626\u0679\u0645\u0632", verb: "\u06C1\u0648\u0646\u0627" }, set: { unit: "\u0622\u0626\u0679\u0645\u0632", verb: "\u06C1\u0648\u0646\u0627" } };
    function i(n) {
      return r[n] ?? null;
    }
    let o = { regex: "\u0627\u0646 \u067E\u0679", email: "\u0627\u06CC \u0645\u06CC\u0644 \u0627\u06CC\u0688\u0631\u06CC\u0633", url: "\u06CC\u0648 \u0622\u0631 \u0627\u06CC\u0644", emoji: "\u0627\u06CC\u0645\u0648\u062C\u06CC", uuid: "\u06CC\u0648 \u06CC\u0648 \u0622\u0626\u06CC \u0688\u06CC", uuidv4: "\u06CC\u0648 \u06CC\u0648 \u0622\u0626\u06CC \u0688\u06CC \u0648\u06CC 4", uuidv6: "\u06CC\u0648 \u06CC\u0648 \u0622\u0626\u06CC \u0688\u06CC \u0648\u06CC 6", nanoid: "\u0646\u06CC\u0646\u0648 \u0622\u0626\u06CC \u0688\u06CC", guid: "\u062C\u06CC \u06CC\u0648 \u0622\u0626\u06CC \u0688\u06CC", cuid: "\u0633\u06CC \u06CC\u0648 \u0622\u0626\u06CC \u0688\u06CC", cuid2: "\u0633\u06CC \u06CC\u0648 \u0622\u0626\u06CC \u0688\u06CC 2", ulid: "\u06CC\u0648 \u0627\u06CC\u0644 \u0622\u0626\u06CC \u0688\u06CC", xid: "\u0627\u06CC\u06A9\u0633 \u0622\u0626\u06CC \u0688\u06CC", ksuid: "\u06A9\u06D2 \u0627\u06CC\u0633 \u06CC\u0648 \u0622\u0626\u06CC \u0688\u06CC", datetime: "\u0622\u0626\u06CC \u0627\u06CC\u0633 \u0627\u0648 \u0688\u06CC\u0679 \u0679\u0627\u0626\u0645", date: "\u0622\u0626\u06CC \u0627\u06CC\u0633 \u0627\u0648 \u062A\u0627\u0631\u06CC\u062E", time: "\u0622\u0626\u06CC \u0627\u06CC\u0633 \u0627\u0648 \u0648\u0642\u062A", duration: "\u0622\u0626\u06CC \u0627\u06CC\u0633 \u0627\u0648 \u0645\u062F\u062A", ipv4: "\u0622\u0626\u06CC \u067E\u06CC \u0648\u06CC 4 \u0627\u06CC\u0688\u0631\u06CC\u0633", ipv6: "\u0622\u0626\u06CC \u067E\u06CC \u0648\u06CC 6 \u0627\u06CC\u0688\u0631\u06CC\u0633", cidrv4: "\u0622\u0626\u06CC \u067E\u06CC \u0648\u06CC 4 \u0631\u06CC\u0646\u062C", cidrv6: "\u0622\u0626\u06CC \u067E\u06CC \u0648\u06CC 6 \u0631\u06CC\u0646\u062C", base64: "\u0628\u06CC\u0633 64 \u0627\u0646 \u06A9\u0648\u0688\u0688 \u0633\u0679\u0631\u0646\u06AF", base64url: "\u0628\u06CC\u0633 64 \u06CC\u0648 \u0622\u0631 \u0627\u06CC\u0644 \u0627\u0646 \u06A9\u0648\u0688\u0688 \u0633\u0679\u0631\u0646\u06AF", json_string: "\u062C\u06D2 \u0627\u06CC\u0633 \u0627\u0648 \u0627\u06CC\u0646 \u0633\u0679\u0631\u0646\u06AF", e164: "\u0627\u06CC 164 \u0646\u0645\u0628\u0631", jwt: "\u062C\u06D2 \u0688\u0628\u0644\u06CC\u0648 \u0679\u06CC", template_literal: "\u0627\u0646 \u067E\u0679" }, t = { nan: "NaN", number: "\u0646\u0645\u0628\u0631", array: "\u0622\u0631\u06D2", null: "\u0646\u0644" };
    return (n) => {
      switch (n.code) {
        case "invalid_type": {
          let v = t[n.expected] ?? n.expected, u = k(n.input), $ = t[u] ?? u;
          if (/^[A-Z]/.test(n.expected))
            return `\u063A\u0644\u0637 \u0627\u0646 \u067E\u0679: instanceof ${n.expected} \u0645\u062A\u0648\u0642\u0639 \u062A\u06BE\u0627\u060C ${$} \u0645\u0648\u0635\u0648\u0644 \u06C1\u0648\u0627`;
          return `\u063A\u0644\u0637 \u0627\u0646 \u067E\u0679: ${v} \u0645\u062A\u0648\u0642\u0639 \u062A\u06BE\u0627\u060C ${$} \u0645\u0648\u0635\u0648\u0644 \u06C1\u0648\u0627`;
        }
        case "invalid_value":
          if (n.values.length === 1)
            return `\u063A\u0644\u0637 \u0627\u0646 \u067E\u0679: ${U(n.values[0])} \u0645\u062A\u0648\u0642\u0639 \u062A\u06BE\u0627`;
          return `\u063A\u0644\u0637 \u0622\u067E\u0634\u0646: ${b(n.values, "|")} \u0645\u06CC\u06BA \u0633\u06D2 \u0627\u06CC\u06A9 \u0645\u062A\u0648\u0642\u0639 \u062A\u06BE\u0627`;
        case "too_big": {
          let v = n.inclusive ? "<=" : "<", u = i(n.origin);
          if (u)
            return `\u0628\u06C1\u062A \u0628\u0691\u0627: ${n.origin ?? "\u0648\u06CC\u0644\u06CC\u0648"} \u06A9\u06D2 ${v}${n.maximum.toString()} ${u.unit ?? "\u0639\u0646\u0627\u0635\u0631"} \u06C1\u0648\u0646\u06D2 \u0645\u062A\u0648\u0642\u0639 \u062A\u06BE\u06D2`;
          return `\u0628\u06C1\u062A \u0628\u0691\u0627: ${n.origin ?? "\u0648\u06CC\u0644\u06CC\u0648"} \u06A9\u0627 ${v}${n.maximum.toString()} \u06C1\u0648\u0646\u0627 \u0645\u062A\u0648\u0642\u0639 \u062A\u06BE\u0627`;
        }
        case "too_small": {
          let v = n.inclusive ? ">=" : ">", u = i(n.origin);
          if (u)
            return `\u0628\u06C1\u062A \u0686\u06BE\u0648\u0679\u0627: ${n.origin} \u06A9\u06D2 ${v}${n.minimum.toString()} ${u.unit} \u06C1\u0648\u0646\u06D2 \u0645\u062A\u0648\u0642\u0639 \u062A\u06BE\u06D2`;
          return `\u0628\u06C1\u062A \u0686\u06BE\u0648\u0679\u0627: ${n.origin} \u06A9\u0627 ${v}${n.minimum.toString()} \u06C1\u0648\u0646\u0627 \u0645\u062A\u0648\u0642\u0639 \u062A\u06BE\u0627`;
        }
        case "invalid_format": {
          let v = n;
          if (v.format === "starts_with")
            return `\u063A\u0644\u0637 \u0633\u0679\u0631\u0646\u06AF: "${v.prefix}" \u0633\u06D2 \u0634\u0631\u0648\u0639 \u06C1\u0648\u0646\u0627 \u0686\u0627\u06C1\u06CC\u06D2`;
          if (v.format === "ends_with")
            return `\u063A\u0644\u0637 \u0633\u0679\u0631\u0646\u06AF: "${v.suffix}" \u067E\u0631 \u062E\u062A\u0645 \u06C1\u0648\u0646\u0627 \u0686\u0627\u06C1\u06CC\u06D2`;
          if (v.format === "includes")
            return `\u063A\u0644\u0637 \u0633\u0679\u0631\u0646\u06AF: "${v.includes}" \u0634\u0627\u0645\u0644 \u06C1\u0648\u0646\u0627 \u0686\u0627\u06C1\u06CC\u06D2`;
          if (v.format === "regex")
            return `\u063A\u0644\u0637 \u0633\u0679\u0631\u0646\u06AF: \u067E\u06CC\u0679\u0631\u0646 ${v.pattern} \u0633\u06D2 \u0645\u06CC\u0686 \u06C1\u0648\u0646\u0627 \u0686\u0627\u06C1\u06CC\u06D2`;
          return `\u063A\u0644\u0637 ${o[v.format] ?? n.format}`;
        }
        case "not_multiple_of":
          return `\u063A\u0644\u0637 \u0646\u0645\u0628\u0631: ${n.divisor} \u06A9\u0627 \u0645\u0636\u0627\u0639\u0641 \u06C1\u0648\u0646\u0627 \u0686\u0627\u06C1\u06CC\u06D2`;
        case "unrecognized_keys":
          return `\u063A\u06CC\u0631 \u062A\u0633\u0644\u06CC\u0645 \u0634\u062F\u06C1 \u06A9\u06CC${n.keys.length > 1 ? "\u0632" : ""}: ${b(n.keys, "\u060C ")}`;
        case "invalid_key":
          return `${n.origin} \u0645\u06CC\u06BA \u063A\u0644\u0637 \u06A9\u06CC`;
        case "invalid_union":
          return "\u063A\u0644\u0637 \u0627\u0646 \u067E\u0679";
        case "invalid_element":
          return `${n.origin} \u0645\u06CC\u06BA \u063A\u0644\u0637 \u0648\u06CC\u0644\u06CC\u0648`;
        default:
          return "\u063A\u0644\u0637 \u0627\u0646 \u067E\u0679";
      }
    };
  };
  function ju() {
    return { localeError: x4() };
  }
  var Z4 = () => {
    let r = { string: { unit: "belgi", verb: "bo\u2018lishi kerak" }, file: { unit: "bayt", verb: "bo\u2018lishi kerak" }, array: { unit: "element", verb: "bo\u2018lishi kerak" }, set: { unit: "element", verb: "bo\u2018lishi kerak" } };
    function i(n) {
      return r[n] ?? null;
    }
    let o = { regex: "kirish", email: "elektron pochta manzili", url: "URL", emoji: "emoji", uuid: "UUID", uuidv4: "UUIDv4", uuidv6: "UUIDv6", nanoid: "nanoid", guid: "GUID", cuid: "cuid", cuid2: "cuid2", ulid: "ULID", xid: "XID", ksuid: "KSUID", datetime: "ISO sana va vaqti", date: "ISO sana", time: "ISO vaqt", duration: "ISO davomiylik", ipv4: "IPv4 manzil", ipv6: "IPv6 manzil", mac: "MAC manzil", cidrv4: "IPv4 diapazon", cidrv6: "IPv6 diapazon", base64: "base64 kodlangan satr", base64url: "base64url kodlangan satr", json_string: "JSON satr", e164: "E.164 raqam", jwt: "JWT", template_literal: "kirish" }, t = { nan: "NaN", number: "raqam", array: "massiv" };
    return (n) => {
      switch (n.code) {
        case "invalid_type": {
          let v = t[n.expected] ?? n.expected, u = k(n.input), $ = t[u] ?? u;
          if (/^[A-Z]/.test(n.expected))
            return `Noto\u2018g\u2018ri kirish: kutilgan instanceof ${n.expected}, qabul qilingan ${$}`;
          return `Noto\u2018g\u2018ri kirish: kutilgan ${v}, qabul qilingan ${$}`;
        }
        case "invalid_value":
          if (n.values.length === 1)
            return `Noto\u2018g\u2018ri kirish: kutilgan ${U(n.values[0])}`;
          return `Noto\u2018g\u2018ri variant: quyidagilardan biri kutilgan ${b(n.values, "|")}`;
        case "too_big": {
          let v = n.inclusive ? "<=" : "<", u = i(n.origin);
          if (u)
            return `Juda katta: kutilgan ${n.origin ?? "qiymat"} ${v}${n.maximum.toString()} ${u.unit} ${u.verb}`;
          return `Juda katta: kutilgan ${n.origin ?? "qiymat"} ${v}${n.maximum.toString()}`;
        }
        case "too_small": {
          let v = n.inclusive ? ">=" : ">", u = i(n.origin);
          if (u)
            return `Juda kichik: kutilgan ${n.origin} ${v}${n.minimum.toString()} ${u.unit} ${u.verb}`;
          return `Juda kichik: kutilgan ${n.origin} ${v}${n.minimum.toString()}`;
        }
        case "invalid_format": {
          let v = n;
          if (v.format === "starts_with")
            return `Noto\u2018g\u2018ri satr: "${v.prefix}" bilan boshlanishi kerak`;
          if (v.format === "ends_with")
            return `Noto\u2018g\u2018ri satr: "${v.suffix}" bilan tugashi kerak`;
          if (v.format === "includes")
            return `Noto\u2018g\u2018ri satr: "${v.includes}" ni o\u2018z ichiga olishi kerak`;
          if (v.format === "regex")
            return `Noto\u2018g\u2018ri satr: ${v.pattern} shabloniga mos kelishi kerak`;
          return `Noto\u2018g\u2018ri ${o[v.format] ?? n.format}`;
        }
        case "not_multiple_of":
          return `Noto\u2018g\u2018ri raqam: ${n.divisor} ning karralisi bo\u2018lishi kerak`;
        case "unrecognized_keys":
          return `Noma\u2019lum kalit${n.keys.length > 1 ? "lar" : ""}: ${b(n.keys, ", ")}`;
        case "invalid_key":
          return `${n.origin} dagi kalit noto\u2018g\u2018ri`;
        case "invalid_union":
          return "Noto\u2018g\u2018ri kirish";
        case "invalid_element":
          return `${n.origin} da noto\u2018g\u2018ri qiymat`;
        default:
          return "Noto\u2018g\u2018ri kirish";
      }
    };
  };
  function Ju() {
    return { localeError: Z4() };
  }
  var d4 = () => {
    let r = { string: { unit: "k\xFD t\u1EF1", verb: "c\xF3" }, file: { unit: "byte", verb: "c\xF3" }, array: { unit: "ph\u1EA7n t\u1EED", verb: "c\xF3" }, set: { unit: "ph\u1EA7n t\u1EED", verb: "c\xF3" } };
    function i(n) {
      return r[n] ?? null;
    }
    let o = { regex: "\u0111\u1EA7u v\xE0o", email: "\u0111\u1ECBa ch\u1EC9 email", url: "URL", emoji: "emoji", uuid: "UUID", uuidv4: "UUIDv4", uuidv6: "UUIDv6", nanoid: "nanoid", guid: "GUID", cuid: "cuid", cuid2: "cuid2", ulid: "ULID", xid: "XID", ksuid: "KSUID", datetime: "ng\xE0y gi\u1EDD ISO", date: "ng\xE0y ISO", time: "gi\u1EDD ISO", duration: "kho\u1EA3ng th\u1EDDi gian ISO", ipv4: "\u0111\u1ECBa ch\u1EC9 IPv4", ipv6: "\u0111\u1ECBa ch\u1EC9 IPv6", cidrv4: "d\u1EA3i IPv4", cidrv6: "d\u1EA3i IPv6", base64: "chu\u1ED7i m\xE3 h\xF3a base64", base64url: "chu\u1ED7i m\xE3 h\xF3a base64url", json_string: "chu\u1ED7i JSON", e164: "s\u1ED1 E.164", jwt: "JWT", template_literal: "\u0111\u1EA7u v\xE0o" }, t = { nan: "NaN", number: "s\u1ED1", array: "m\u1EA3ng" };
    return (n) => {
      switch (n.code) {
        case "invalid_type": {
          let v = t[n.expected] ?? n.expected, u = k(n.input), $ = t[u] ?? u;
          if (/^[A-Z]/.test(n.expected))
            return `\u0110\u1EA7u v\xE0o kh\xF4ng h\u1EE3p l\u1EC7: mong \u0111\u1EE3i instanceof ${n.expected}, nh\u1EADn \u0111\u01B0\u1EE3c ${$}`;
          return `\u0110\u1EA7u v\xE0o kh\xF4ng h\u1EE3p l\u1EC7: mong \u0111\u1EE3i ${v}, nh\u1EADn \u0111\u01B0\u1EE3c ${$}`;
        }
        case "invalid_value":
          if (n.values.length === 1)
            return `\u0110\u1EA7u v\xE0o kh\xF4ng h\u1EE3p l\u1EC7: mong \u0111\u1EE3i ${U(n.values[0])}`;
          return `T\xF9y ch\u1ECDn kh\xF4ng h\u1EE3p l\u1EC7: mong \u0111\u1EE3i m\u1ED9t trong c\xE1c gi\xE1 tr\u1ECB ${b(n.values, "|")}`;
        case "too_big": {
          let v = n.inclusive ? "<=" : "<", u = i(n.origin);
          if (u)
            return `Qu\xE1 l\u1EDBn: mong \u0111\u1EE3i ${n.origin ?? "gi\xE1 tr\u1ECB"} ${u.verb} ${v}${n.maximum.toString()} ${u.unit ?? "ph\u1EA7n t\u1EED"}`;
          return `Qu\xE1 l\u1EDBn: mong \u0111\u1EE3i ${n.origin ?? "gi\xE1 tr\u1ECB"} ${v}${n.maximum.toString()}`;
        }
        case "too_small": {
          let v = n.inclusive ? ">=" : ">", u = i(n.origin);
          if (u)
            return `Qu\xE1 nh\u1ECF: mong \u0111\u1EE3i ${n.origin} ${u.verb} ${v}${n.minimum.toString()} ${u.unit}`;
          return `Qu\xE1 nh\u1ECF: mong \u0111\u1EE3i ${n.origin} ${v}${n.minimum.toString()}`;
        }
        case "invalid_format": {
          let v = n;
          if (v.format === "starts_with")
            return `Chu\u1ED7i kh\xF4ng h\u1EE3p l\u1EC7: ph\u1EA3i b\u1EAFt \u0111\u1EA7u b\u1EB1ng "${v.prefix}"`;
          if (v.format === "ends_with")
            return `Chu\u1ED7i kh\xF4ng h\u1EE3p l\u1EC7: ph\u1EA3i k\u1EBFt th\xFAc b\u1EB1ng "${v.suffix}"`;
          if (v.format === "includes")
            return `Chu\u1ED7i kh\xF4ng h\u1EE3p l\u1EC7: ph\u1EA3i bao g\u1ED3m "${v.includes}"`;
          if (v.format === "regex")
            return `Chu\u1ED7i kh\xF4ng h\u1EE3p l\u1EC7: ph\u1EA3i kh\u1EDBp v\u1EDBi m\u1EABu ${v.pattern}`;
          return `${o[v.format] ?? n.format} kh\xF4ng h\u1EE3p l\u1EC7`;
        }
        case "not_multiple_of":
          return `S\u1ED1 kh\xF4ng h\u1EE3p l\u1EC7: ph\u1EA3i l\xE0 b\u1ED9i s\u1ED1 c\u1EE7a ${n.divisor}`;
        case "unrecognized_keys":
          return `Kh\xF3a kh\xF4ng \u0111\u01B0\u1EE3c nh\u1EADn d\u1EA1ng: ${b(n.keys, ", ")}`;
        case "invalid_key":
          return `Kh\xF3a kh\xF4ng h\u1EE3p l\u1EC7 trong ${n.origin}`;
        case "invalid_union":
          return "\u0110\u1EA7u v\xE0o kh\xF4ng h\u1EE3p l\u1EC7";
        case "invalid_element":
          return `Gi\xE1 tr\u1ECB kh\xF4ng h\u1EE3p l\u1EC7 trong ${n.origin}`;
        default:
          return "\u0110\u1EA7u v\xE0o kh\xF4ng h\u1EE3p l\u1EC7";
      }
    };
  };
  function Lu() {
    return { localeError: d4() };
  }
  var C4 = () => {
    let r = { string: { unit: "\u5B57\u7B26", verb: "\u5305\u542B" }, file: { unit: "\u5B57\u8282", verb: "\u5305\u542B" }, array: { unit: "\u9879", verb: "\u5305\u542B" }, set: { unit: "\u9879", verb: "\u5305\u542B" } };
    function i(n) {
      return r[n] ?? null;
    }
    let o = { regex: "\u8F93\u5165", email: "\u7535\u5B50\u90AE\u4EF6", url: "URL", emoji: "\u8868\u60C5\u7B26\u53F7", uuid: "UUID", uuidv4: "UUIDv4", uuidv6: "UUIDv6", nanoid: "nanoid", guid: "GUID", cuid: "cuid", cuid2: "cuid2", ulid: "ULID", xid: "XID", ksuid: "KSUID", datetime: "ISO\u65E5\u671F\u65F6\u95F4", date: "ISO\u65E5\u671F", time: "ISO\u65F6\u95F4", duration: "ISO\u65F6\u957F", ipv4: "IPv4\u5730\u5740", ipv6: "IPv6\u5730\u5740", cidrv4: "IPv4\u7F51\u6BB5", cidrv6: "IPv6\u7F51\u6BB5", base64: "base64\u7F16\u7801\u5B57\u7B26\u4E32", base64url: "base64url\u7F16\u7801\u5B57\u7B26\u4E32", json_string: "JSON\u5B57\u7B26\u4E32", e164: "E.164\u53F7\u7801", jwt: "JWT", template_literal: "\u8F93\u5165" }, t = { nan: "NaN", number: "\u6570\u5B57", array: "\u6570\u7EC4", null: "\u7A7A\u503C(null)" };
    return (n) => {
      switch (n.code) {
        case "invalid_type": {
          let v = t[n.expected] ?? n.expected, u = k(n.input), $ = t[u] ?? u;
          if (/^[A-Z]/.test(n.expected))
            return `\u65E0\u6548\u8F93\u5165\uFF1A\u671F\u671B instanceof ${n.expected}\uFF0C\u5B9E\u9645\u63A5\u6536 ${$}`;
          return `\u65E0\u6548\u8F93\u5165\uFF1A\u671F\u671B ${v}\uFF0C\u5B9E\u9645\u63A5\u6536 ${$}`;
        }
        case "invalid_value":
          if (n.values.length === 1)
            return `\u65E0\u6548\u8F93\u5165\uFF1A\u671F\u671B ${U(n.values[0])}`;
          return `\u65E0\u6548\u9009\u9879\uFF1A\u671F\u671B\u4EE5\u4E0B\u4E4B\u4E00 ${b(n.values, "|")}`;
        case "too_big": {
          let v = n.inclusive ? "<=" : "<", u = i(n.origin);
          if (u)
            return `\u6570\u503C\u8FC7\u5927\uFF1A\u671F\u671B ${n.origin ?? "\u503C"} ${v}${n.maximum.toString()} ${u.unit ?? "\u4E2A\u5143\u7D20"}`;
          return `\u6570\u503C\u8FC7\u5927\uFF1A\u671F\u671B ${n.origin ?? "\u503C"} ${v}${n.maximum.toString()}`;
        }
        case "too_small": {
          let v = n.inclusive ? ">=" : ">", u = i(n.origin);
          if (u)
            return `\u6570\u503C\u8FC7\u5C0F\uFF1A\u671F\u671B ${n.origin} ${v}${n.minimum.toString()} ${u.unit}`;
          return `\u6570\u503C\u8FC7\u5C0F\uFF1A\u671F\u671B ${n.origin} ${v}${n.minimum.toString()}`;
        }
        case "invalid_format": {
          let v = n;
          if (v.format === "starts_with")
            return `\u65E0\u6548\u5B57\u7B26\u4E32\uFF1A\u5FC5\u987B\u4EE5 "${v.prefix}" \u5F00\u5934`;
          if (v.format === "ends_with")
            return `\u65E0\u6548\u5B57\u7B26\u4E32\uFF1A\u5FC5\u987B\u4EE5 "${v.suffix}" \u7ED3\u5C3E`;
          if (v.format === "includes")
            return `\u65E0\u6548\u5B57\u7B26\u4E32\uFF1A\u5FC5\u987B\u5305\u542B "${v.includes}"`;
          if (v.format === "regex")
            return `\u65E0\u6548\u5B57\u7B26\u4E32\uFF1A\u5FC5\u987B\u6EE1\u8DB3\u6B63\u5219\u8868\u8FBE\u5F0F ${v.pattern}`;
          return `\u65E0\u6548${o[v.format] ?? n.format}`;
        }
        case "not_multiple_of":
          return `\u65E0\u6548\u6570\u5B57\uFF1A\u5FC5\u987B\u662F ${n.divisor} \u7684\u500D\u6570`;
        case "unrecognized_keys":
          return `\u51FA\u73B0\u672A\u77E5\u7684\u952E(key): ${b(n.keys, ", ")}`;
        case "invalid_key":
          return `${n.origin} \u4E2D\u7684\u952E(key)\u65E0\u6548`;
        case "invalid_union":
          return "\u65E0\u6548\u8F93\u5165";
        case "invalid_element":
          return `${n.origin} \u4E2D\u5305\u542B\u65E0\u6548\u503C(value)`;
        default:
          return "\u65E0\u6548\u8F93\u5165";
      }
    };
  };
  function Eu() {
    return { localeError: C4() };
  }
  var f4 = () => {
    let r = { string: { unit: "\u5B57\u5143", verb: "\u64C1\u6709" }, file: { unit: "\u4F4D\u5143\u7D44", verb: "\u64C1\u6709" }, array: { unit: "\u9805\u76EE", verb: "\u64C1\u6709" }, set: { unit: "\u9805\u76EE", verb: "\u64C1\u6709" } };
    function i(n) {
      return r[n] ?? null;
    }
    let o = { regex: "\u8F38\u5165", email: "\u90F5\u4EF6\u5730\u5740", url: "URL", emoji: "emoji", uuid: "UUID", uuidv4: "UUIDv4", uuidv6: "UUIDv6", nanoid: "nanoid", guid: "GUID", cuid: "cuid", cuid2: "cuid2", ulid: "ULID", xid: "XID", ksuid: "KSUID", datetime: "ISO \u65E5\u671F\u6642\u9593", date: "ISO \u65E5\u671F", time: "ISO \u6642\u9593", duration: "ISO \u671F\u9593", ipv4: "IPv4 \u4F4D\u5740", ipv6: "IPv6 \u4F4D\u5740", cidrv4: "IPv4 \u7BC4\u570D", cidrv6: "IPv6 \u7BC4\u570D", base64: "base64 \u7DE8\u78BC\u5B57\u4E32", base64url: "base64url \u7DE8\u78BC\u5B57\u4E32", json_string: "JSON \u5B57\u4E32", e164: "E.164 \u6578\u503C", jwt: "JWT", template_literal: "\u8F38\u5165" }, t = { nan: "NaN" };
    return (n) => {
      switch (n.code) {
        case "invalid_type": {
          let v = t[n.expected] ?? n.expected, u = k(n.input), $ = t[u] ?? u;
          if (/^[A-Z]/.test(n.expected))
            return `\u7121\u6548\u7684\u8F38\u5165\u503C\uFF1A\u9810\u671F\u70BA instanceof ${n.expected}\uFF0C\u4F46\u6536\u5230 ${$}`;
          return `\u7121\u6548\u7684\u8F38\u5165\u503C\uFF1A\u9810\u671F\u70BA ${v}\uFF0C\u4F46\u6536\u5230 ${$}`;
        }
        case "invalid_value":
          if (n.values.length === 1)
            return `\u7121\u6548\u7684\u8F38\u5165\u503C\uFF1A\u9810\u671F\u70BA ${U(n.values[0])}`;
          return `\u7121\u6548\u7684\u9078\u9805\uFF1A\u9810\u671F\u70BA\u4EE5\u4E0B\u5176\u4E2D\u4E4B\u4E00 ${b(n.values, "|")}`;
        case "too_big": {
          let v = n.inclusive ? "<=" : "<", u = i(n.origin);
          if (u)
            return `\u6578\u503C\u904E\u5927\uFF1A\u9810\u671F ${n.origin ?? "\u503C"} \u61C9\u70BA ${v}${n.maximum.toString()} ${u.unit ?? "\u500B\u5143\u7D20"}`;
          return `\u6578\u503C\u904E\u5927\uFF1A\u9810\u671F ${n.origin ?? "\u503C"} \u61C9\u70BA ${v}${n.maximum.toString()}`;
        }
        case "too_small": {
          let v = n.inclusive ? ">=" : ">", u = i(n.origin);
          if (u)
            return `\u6578\u503C\u904E\u5C0F\uFF1A\u9810\u671F ${n.origin} \u61C9\u70BA ${v}${n.minimum.toString()} ${u.unit}`;
          return `\u6578\u503C\u904E\u5C0F\uFF1A\u9810\u671F ${n.origin} \u61C9\u70BA ${v}${n.minimum.toString()}`;
        }
        case "invalid_format": {
          let v = n;
          if (v.format === "starts_with")
            return `\u7121\u6548\u7684\u5B57\u4E32\uFF1A\u5FC5\u9808\u4EE5 "${v.prefix}" \u958B\u982D`;
          if (v.format === "ends_with")
            return `\u7121\u6548\u7684\u5B57\u4E32\uFF1A\u5FC5\u9808\u4EE5 "${v.suffix}" \u7D50\u5C3E`;
          if (v.format === "includes")
            return `\u7121\u6548\u7684\u5B57\u4E32\uFF1A\u5FC5\u9808\u5305\u542B "${v.includes}"`;
          if (v.format === "regex")
            return `\u7121\u6548\u7684\u5B57\u4E32\uFF1A\u5FC5\u9808\u7B26\u5408\u683C\u5F0F ${v.pattern}`;
          return `\u7121\u6548\u7684 ${o[v.format] ?? n.format}`;
        }
        case "not_multiple_of":
          return `\u7121\u6548\u7684\u6578\u5B57\uFF1A\u5FC5\u9808\u70BA ${n.divisor} \u7684\u500D\u6578`;
        case "unrecognized_keys":
          return `\u7121\u6CD5\u8B58\u5225\u7684\u9375\u503C${n.keys.length > 1 ? "\u5011" : ""}\uFF1A${b(n.keys, "\u3001")}`;
        case "invalid_key":
          return `${n.origin} \u4E2D\u6709\u7121\u6548\u7684\u9375\u503C`;
        case "invalid_union":
          return "\u7121\u6548\u7684\u8F38\u5165\u503C";
        case "invalid_element":
          return `${n.origin} \u4E2D\u6709\u7121\u6548\u7684\u503C`;
        default:
          return "\u7121\u6548\u7684\u8F38\u5165\u503C";
      }
    };
  };
  function Gu() {
    return { localeError: f4() };
  }
  var h4 = () => {
    let r = { string: { unit: "\xE0mi", verb: "n\xED" }, file: { unit: "bytes", verb: "n\xED" }, array: { unit: "nkan", verb: "n\xED" }, set: { unit: "nkan", verb: "n\xED" } };
    function i(n) {
      return r[n] ?? null;
    }
    let o = { regex: "\u1EB9\u0300r\u1ECD \xECb\xE1w\u1ECDl\xE9", email: "\xE0d\xEDr\u1EB9\u0301s\xEC \xECm\u1EB9\u0301l\xEC", url: "URL", emoji: "emoji", uuid: "UUID", uuidv4: "UUIDv4", uuidv6: "UUIDv6", nanoid: "nanoid", guid: "GUID", cuid: "cuid", cuid2: "cuid2", ulid: "ULID", xid: "XID", ksuid: "KSUID", datetime: "\xE0k\xF3k\xF2 ISO", date: "\u1ECDj\u1ECD\u0301 ISO", time: "\xE0k\xF3k\xF2 ISO", duration: "\xE0k\xF3k\xF2 t\xF3 p\xE9 ISO", ipv4: "\xE0d\xEDr\u1EB9\u0301s\xEC IPv4", ipv6: "\xE0d\xEDr\u1EB9\u0301s\xEC IPv6", cidrv4: "\xE0gb\xE8gb\xE8 IPv4", cidrv6: "\xE0gb\xE8gb\xE8 IPv6", base64: "\u1ECD\u0300r\u1ECD\u0300 t\xED a k\u1ECD\u0301 n\xED base64", base64url: "\u1ECD\u0300r\u1ECD\u0300 base64url", json_string: "\u1ECD\u0300r\u1ECD\u0300 JSON", e164: "n\u1ECD\u0301mb\xE0 E.164", jwt: "JWT", template_literal: "\u1EB9\u0300r\u1ECD \xECb\xE1w\u1ECDl\xE9" }, t = { nan: "NaN", number: "n\u1ECD\u0301mb\xE0", array: "akop\u1ECD" };
    return (n) => {
      switch (n.code) {
        case "invalid_type": {
          let v = t[n.expected] ?? n.expected, u = k(n.input), $ = t[u] ?? u;
          if (/^[A-Z]/.test(n.expected))
            return `\xCCb\xE1w\u1ECDl\xE9 a\u1E63\xEC\u1E63e: a n\xED l\xE1ti fi instanceof ${n.expected}, \xE0m\u1ECD\u0300 a r\xED ${$}`;
          return `\xCCb\xE1w\u1ECDl\xE9 a\u1E63\xEC\u1E63e: a n\xED l\xE1ti fi ${v}, \xE0m\u1ECD\u0300 a r\xED ${$}`;
        }
        case "invalid_value":
          if (n.values.length === 1)
            return `\xCCb\xE1w\u1ECDl\xE9 a\u1E63\xEC\u1E63e: a n\xED l\xE1ti fi ${U(n.values[0])}`;
          return `\xC0\u1E63\xE0y\xE0n a\u1E63\xEC\u1E63e: yan \u1ECD\u0300kan l\xE1ra ${b(n.values, "|")}`;
        case "too_big": {
          let v = n.inclusive ? "<=" : "<", u = i(n.origin);
          if (u)
            return `T\xF3 p\u1ECD\u0300 j\xF9: a n\xED l\xE1ti j\u1EB9\u0301 p\xE9 ${n.origin ?? "iye"} ${u.verb} ${v}${n.maximum} ${u.unit}`;
          return `T\xF3 p\u1ECD\u0300 j\xF9: a n\xED l\xE1ti j\u1EB9\u0301 ${v}${n.maximum}`;
        }
        case "too_small": {
          let v = n.inclusive ? ">=" : ">", u = i(n.origin);
          if (u)
            return `K\xE9r\xE9 ju: a n\xED l\xE1ti j\u1EB9\u0301 p\xE9 ${n.origin} ${u.verb} ${v}${n.minimum} ${u.unit}`;
          return `K\xE9r\xE9 ju: a n\xED l\xE1ti j\u1EB9\u0301 ${v}${n.minimum}`;
        }
        case "invalid_format": {
          let v = n;
          if (v.format === "starts_with")
            return `\u1ECC\u0300r\u1ECD\u0300 a\u1E63\xEC\u1E63e: gb\u1ECD\u0301d\u1ECD\u0300 b\u1EB9\u0300r\u1EB9\u0300 p\u1EB9\u0300l\xFA "${v.prefix}"`;
          if (v.format === "ends_with")
            return `\u1ECC\u0300r\u1ECD\u0300 a\u1E63\xEC\u1E63e: gb\u1ECD\u0301d\u1ECD\u0300 par\xED p\u1EB9\u0300l\xFA "${v.suffix}"`;
          if (v.format === "includes")
            return `\u1ECC\u0300r\u1ECD\u0300 a\u1E63\xEC\u1E63e: gb\u1ECD\u0301d\u1ECD\u0300 n\xED "${v.includes}"`;
          if (v.format === "regex")
            return `\u1ECC\u0300r\u1ECD\u0300 a\u1E63\xEC\u1E63e: gb\u1ECD\u0301d\u1ECD\u0300 b\xE1 \xE0p\u1EB9\u1EB9r\u1EB9 mu ${v.pattern}`;
          return `A\u1E63\xEC\u1E63e: ${o[v.format] ?? n.format}`;
        }
        case "not_multiple_of":
          return `N\u1ECD\u0301mb\xE0 a\u1E63\xEC\u1E63e: gb\u1ECD\u0301d\u1ECD\u0300 j\u1EB9\u0301 \xE8y\xE0 p\xEDp\xEDn ti ${n.divisor}`;
        case "unrecognized_keys":
          return `B\u1ECDt\xECn\xEC \xE0\xECm\u1ECD\u0300: ${b(n.keys, ", ")}`;
        case "invalid_key":
          return `B\u1ECDt\xECn\xEC a\u1E63\xEC\u1E63e n\xEDn\xFA ${n.origin}`;
        case "invalid_union":
          return "\xCCb\xE1w\u1ECDl\xE9 a\u1E63\xEC\u1E63e";
        case "invalid_element":
          return `Iye a\u1E63\xEC\u1E63e n\xEDn\xFA ${n.origin}`;
        default:
          return "\xCCb\xE1w\u1ECDl\xE9 a\u1E63\xEC\u1E63e";
      }
    };
  };
  function Wu() {
    return { localeError: h4() };
  }
  var il;
  var Xu = Symbol("ZodOutput");
  var Vu = Symbol("ZodInput");
  var Au = class {
    constructor() {
      this._map = /* @__PURE__ */ new WeakMap(), this._idmap = /* @__PURE__ */ new Map();
    }
    add(r, ...i) {
      let o = i[0];
      if (this._map.set(r, o), o && typeof o === "object" && "id" in o)
        this._idmap.set(o.id, r);
      return this;
    }
    clear() {
      return this._map = /* @__PURE__ */ new WeakMap(), this._idmap = /* @__PURE__ */ new Map(), this;
    }
    remove(r) {
      let i = this._map.get(r);
      if (i && typeof i === "object" && "id" in i)
        this._idmap.delete(i.id);
      return this._map.delete(r), this;
    }
    get(r) {
      let i = r._zod.parent;
      if (i) {
        let o = { ...this.get(i) ?? {} };
        delete o.id;
        let t = { ...o, ...this._map.get(r) };
        return Object.keys(t).length ? t : void 0;
      }
      return this._map.get(r);
    }
    has(r) {
      return this._map.has(r);
    }
  };
  function $i() {
    return new Au();
  }
  (il = globalThis).__zod_globalRegistry ?? (il.__zod_globalRegistry = $i());
  var A = globalThis.__zod_globalRegistry;
  function Ku(r, i) {
    return new r({ type: "string", ...w(i) });
  }
  function qu(r, i) {
    return new r({ type: "string", coerce: true, ...w(i) });
  }
  function gi(r, i) {
    return new r({ type: "string", format: "email", check: "string_format", abort: false, ...w(i) });
  }
  function Sn(r, i) {
    return new r({ type: "string", format: "guid", check: "string_format", abort: false, ...w(i) });
  }
  function ei(r, i) {
    return new r({ type: "string", format: "uuid", check: "string_format", abort: false, ...w(i) });
  }
  function li(r, i) {
    return new r({ type: "string", format: "uuid", check: "string_format", abort: false, version: "v4", ...w(i) });
  }
  function ci(r, i) {
    return new r({ type: "string", format: "uuid", check: "string_format", abort: false, version: "v6", ...w(i) });
  }
  function Ii(r, i) {
    return new r({ type: "string", format: "uuid", check: "string_format", abort: false, version: "v7", ...w(i) });
  }
  function zn(r, i) {
    return new r({ type: "string", format: "url", check: "string_format", abort: false, ...w(i) });
  }
  function bi(r, i) {
    return new r({ type: "string", format: "emoji", check: "string_format", abort: false, ...w(i) });
  }
  function _i(r, i) {
    return new r({ type: "string", format: "nanoid", check: "string_format", abort: false, ...w(i) });
  }
  function Ui(r, i) {
    return new r({ type: "string", format: "cuid", check: "string_format", abort: false, ...w(i) });
  }
  function ki(r, i) {
    return new r({ type: "string", format: "cuid2", check: "string_format", abort: false, ...w(i) });
  }
  function Di(r, i) {
    return new r({ type: "string", format: "ulid", check: "string_format", abort: false, ...w(i) });
  }
  function wi(r, i) {
    return new r({ type: "string", format: "xid", check: "string_format", abort: false, ...w(i) });
  }
  function Ni(r, i) {
    return new r({ type: "string", format: "ksuid", check: "string_format", abort: false, ...w(i) });
  }
  function Oi(r, i) {
    return new r({ type: "string", format: "ipv4", check: "string_format", abort: false, ...w(i) });
  }
  function Si(r, i) {
    return new r({ type: "string", format: "ipv6", check: "string_format", abort: false, ...w(i) });
  }
  function Yu(r, i) {
    return new r({ type: "string", format: "mac", check: "string_format", abort: false, ...w(i) });
  }
  function zi(r, i) {
    return new r({ type: "string", format: "cidrv4", check: "string_format", abort: false, ...w(i) });
  }
  function Pi(r, i) {
    return new r({ type: "string", format: "cidrv6", check: "string_format", abort: false, ...w(i) });
  }
  function ji(r, i) {
    return new r({ type: "string", format: "base64", check: "string_format", abort: false, ...w(i) });
  }
  function Ji(r, i) {
    return new r({ type: "string", format: "base64url", check: "string_format", abort: false, ...w(i) });
  }
  function Li(r, i) {
    return new r({ type: "string", format: "e164", check: "string_format", abort: false, ...w(i) });
  }
  function Ei(r, i) {
    return new r({ type: "string", format: "jwt", check: "string_format", abort: false, ...w(i) });
  }
  var Qu = { Any: null, Minute: -1, Second: 0, Millisecond: 3, Microsecond: 6 };
  function mu(r, i) {
    return new r({ type: "string", format: "datetime", check: "string_format", offset: false, local: false, precision: null, ...w(i) });
  }
  function Tu(r, i) {
    return new r({ type: "string", format: "date", check: "string_format", ...w(i) });
  }
  function Fu(r, i) {
    return new r({ type: "string", format: "time", check: "string_format", precision: null, ...w(i) });
  }
  function Bu(r, i) {
    return new r({ type: "string", format: "duration", check: "string_format", ...w(i) });
  }
  function Hu(r, i) {
    return new r({ type: "number", checks: [], ...w(i) });
  }
  function Mu(r, i) {
    return new r({ type: "number", coerce: true, checks: [], ...w(i) });
  }
  function Ru(r, i) {
    return new r({ type: "number", check: "number_format", abort: false, format: "safeint", ...w(i) });
  }
  function xu(r, i) {
    return new r({ type: "number", check: "number_format", abort: false, format: "float32", ...w(i) });
  }
  function Zu(r, i) {
    return new r({ type: "number", check: "number_format", abort: false, format: "float64", ...w(i) });
  }
  function du(r, i) {
    return new r({ type: "number", check: "number_format", abort: false, format: "int32", ...w(i) });
  }
  function Cu(r, i) {
    return new r({ type: "number", check: "number_format", abort: false, format: "uint32", ...w(i) });
  }
  function fu(r, i) {
    return new r({ type: "boolean", ...w(i) });
  }
  function hu(r, i) {
    return new r({ type: "boolean", coerce: true, ...w(i) });
  }
  function yu(r, i) {
    return new r({ type: "bigint", ...w(i) });
  }
  function au(r, i) {
    return new r({ type: "bigint", coerce: true, ...w(i) });
  }
  function pu(r, i) {
    return new r({ type: "bigint", check: "bigint_format", abort: false, format: "int64", ...w(i) });
  }
  function su(r, i) {
    return new r({ type: "bigint", check: "bigint_format", abort: false, format: "uint64", ...w(i) });
  }
  function r$(r, i) {
    return new r({ type: "symbol", ...w(i) });
  }
  function n$(r, i) {
    return new r({ type: "undefined", ...w(i) });
  }
  function i$(r, i) {
    return new r({ type: "null", ...w(i) });
  }
  function v$(r) {
    return new r({ type: "any" });
  }
  function o$(r) {
    return new r({ type: "unknown" });
  }
  function t$(r, i) {
    return new r({ type: "never", ...w(i) });
  }
  function u$(r, i) {
    return new r({ type: "void", ...w(i) });
  }
  function $$(r, i) {
    return new r({ type: "date", ...w(i) });
  }
  function g$(r, i) {
    return new r({ type: "date", coerce: true, ...w(i) });
  }
  function e$(r, i) {
    return new r({ type: "nan", ...w(i) });
  }
  function h(r, i) {
    return new hn({ check: "less_than", ...w(i), value: r, inclusive: false });
  }
  function M(r, i) {
    return new hn({ check: "less_than", ...w(i), value: r, inclusive: true });
  }
  function y(r, i) {
    return new yn({ check: "greater_than", ...w(i), value: r, inclusive: false });
  }
  function Y(r, i) {
    return new yn({ check: "greater_than", ...w(i), value: r, inclusive: true });
  }
  function Gi(r) {
    return y(0, r);
  }
  function Wi(r) {
    return h(0, r);
  }
  function Xi(r) {
    return M(0, r);
  }
  function Vi(r) {
    return Y(0, r);
  }
  function $r(r, i) {
    return new go({ check: "multiple_of", ...w(i), value: r });
  }
  function gr(r, i) {
    return new co({ check: "max_size", ...w(i), maximum: r });
  }
  function a(r, i) {
    return new Io({ check: "min_size", ...w(i), minimum: r });
  }
  function kr(r, i) {
    return new bo({ check: "size_equals", ...w(i), size: r });
  }
  function Dr(r, i) {
    return new _o({ check: "max_length", ...w(i), maximum: r });
  }
  function nr(r, i) {
    return new Uo({ check: "min_length", ...w(i), minimum: r });
  }
  function wr(r, i) {
    return new ko({ check: "length_equals", ...w(i), length: r });
  }
  function Vr(r, i) {
    return new Do({ check: "string_format", format: "regex", ...w(i), pattern: r });
  }
  function Ar(r) {
    return new wo({ check: "string_format", format: "lowercase", ...w(r) });
  }
  function Kr(r) {
    return new No({ check: "string_format", format: "uppercase", ...w(r) });
  }
  function qr(r, i) {
    return new Oo({ check: "string_format", format: "includes", ...w(i), includes: r });
  }
  function Yr(r, i) {
    return new So({ check: "string_format", format: "starts_with", ...w(i), prefix: r });
  }
  function Qr(r, i) {
    return new zo({ check: "string_format", format: "ends_with", ...w(i), suffix: r });
  }
  function Ai(r, i, o) {
    return new Po({ check: "property", property: r, schema: i, ...w(o) });
  }
  function mr(r, i) {
    return new jo({ check: "mime_type", mime: r, ...w(i) });
  }
  function d(r) {
    return new Jo({ check: "overwrite", tx: r });
  }
  function Tr(r) {
    return d((i) => i.normalize(r));
  }
  function Fr() {
    return d((r) => r.trim());
  }
  function Br() {
    return d((r) => r.toLowerCase());
  }
  function Hr() {
    return d((r) => r.toUpperCase());
  }
  function Mr() {
    return d((r) => Pv(r));
  }
  function l$(r, i, o) {
    return new r({ type: "array", element: i, ...w(o) });
  }
  function a4(r, i, o) {
    return new r({ type: "union", options: i, ...w(o) });
  }
  function p4(r, i, o) {
    return new r({ type: "union", options: i, inclusive: false, ...w(o) });
  }
  function s4(r, i, o, t) {
    return new r({ type: "union", options: o, discriminator: i, ...w(t) });
  }
  function r6(r, i, o) {
    return new r({ type: "intersection", left: i, right: o });
  }
  function n6(r, i, o, t) {
    let n = o instanceof z;
    return new r({ type: "tuple", items: i, rest: n ? o : null, ...w(n ? t : o) });
  }
  function i6(r, i, o, t) {
    return new r({ type: "record", keyType: i, valueType: o, ...w(t) });
  }
  function v6(r, i, o, t) {
    return new r({ type: "map", keyType: i, valueType: o, ...w(t) });
  }
  function o6(r, i, o) {
    return new r({ type: "set", valueType: i, ...w(o) });
  }
  function t6(r, i, o) {
    let t = Array.isArray(i) ? Object.fromEntries(i.map((n) => [n, n])) : i;
    return new r({ type: "enum", entries: t, ...w(o) });
  }
  function u6(r, i, o) {
    return new r({ type: "enum", entries: i, ...w(o) });
  }
  function $6(r, i, o) {
    return new r({ type: "literal", values: Array.isArray(i) ? i : [i], ...w(o) });
  }
  function c$(r, i) {
    return new r({ type: "file", ...w(i) });
  }
  function g6(r, i) {
    return new r({ type: "transform", transform: i });
  }
  function e6(r, i) {
    return new r({ type: "optional", innerType: i });
  }
  function l6(r, i) {
    return new r({ type: "nullable", innerType: i });
  }
  function c6(r, i, o) {
    return new r({ type: "default", innerType: i, get defaultValue() {
      return typeof o === "function" ? o() : Jv(o);
    } });
  }
  function I6(r, i, o) {
    return new r({ type: "nonoptional", innerType: i, ...w(o) });
  }
  function b6(r, i) {
    return new r({ type: "success", innerType: i });
  }
  function _6(r, i, o) {
    return new r({ type: "catch", innerType: i, catchValue: typeof o === "function" ? o : () => o });
  }
  function U6(r, i, o) {
    return new r({ type: "pipe", in: i, out: o });
  }
  function k6(r, i) {
    return new r({ type: "readonly", innerType: i });
  }
  function D6(r, i, o) {
    return new r({ type: "template_literal", parts: i, ...w(o) });
  }
  function w6(r, i) {
    return new r({ type: "lazy", getter: i });
  }
  function N6(r, i) {
    return new r({ type: "promise", innerType: i });
  }
  function I$(r, i, o) {
    let t = w(o);
    return t.abort ?? (t.abort = true), new r({ type: "custom", check: "custom", fn: i, ...t });
  }
  function b$(r, i, o) {
    return new r({ type: "custom", check: "custom", fn: i, ...w(o) });
  }
  function _$(r) {
    let i = vl((o) => {
      return o.addIssue = (t) => {
        if (typeof t === "string")
          o.issues.push(jr(t, o.value, i._zod.def));
        else {
          let n = t;
          if (n.fatal)
            n.continue = false;
          n.code ?? (n.code = "custom"), n.input ?? (n.input = o.value), n.inst ?? (n.inst = i), n.continue ?? (n.continue = !i._zod.def.abort), o.issues.push(jr(n));
        }
      }, r(o.value, o);
    });
    return i;
  }
  function vl(r, i) {
    let o = new W({ check: "custom", ...w(i) });
    return o._zod.check = r, o;
  }
  function U$(r) {
    let i = new W({ check: "describe" });
    return i._zod.onattach = [(o) => {
      let t = A.get(o) ?? {};
      A.add(o, { ...t, description: r });
    }], i._zod.check = () => {
    }, i;
  }
  function k$(r) {
    let i = new W({ check: "meta" });
    return i._zod.onattach = [(o) => {
      let t = A.get(o) ?? {};
      A.add(o, { ...t, ...r });
    }], i._zod.check = () => {
    }, i;
  }
  function D$(r, i) {
    let o = w(i), t = o.truthy ?? ["true", "1", "yes", "on", "y", "enabled"], n = o.falsy ?? ["false", "0", "no", "off", "n", "disabled"];
    if (o.case !== "sensitive")
      t = t.map((O) => typeof O === "string" ? O.toLowerCase() : O), n = n.map((O) => typeof O === "string" ? O.toLowerCase() : O);
    let v = new Set(t), u = new Set(n), $ = r.Codec ?? Un, l = r.Boolean ?? bn, I = new (r.String ?? Ur)({ type: "string", error: o.error }), _ = new l({ type: "boolean", error: o.error }), N = new $({ type: "pipe", in: I, out: _, transform: (O, J) => {
      let X = O;
      if (o.case !== "sensitive")
        X = X.toLowerCase();
      if (v.has(X))
        return true;
      else if (u.has(X))
        return false;
      else
        return J.issues.push({ code: "invalid_value", expected: "stringbool", values: [...v, ...u], input: J.value, inst: N, continue: false }), {};
    }, reverseTransform: (O, J) => {
      if (O === true)
        return t[0] || "true";
      else
        return n[0] || "false";
    }, error: o.error });
    return N;
  }
  function Rr(r, i, o, t = {}) {
    let n = w(t), v = { ...w(t), check: "string_format", type: "string", format: i, fn: typeof o === "function" ? o : ($) => o.test($), ...n };
    if (o instanceof RegExp)
      v.pattern = o;
    return new r(v);
  }
  function er(r) {
    let i = r?.target ?? "draft-2020-12";
    if (i === "draft-4")
      i = "draft-04";
    if (i === "draft-7")
      i = "draft-07";
    return { processors: r.processors ?? {}, metadataRegistry: r?.metadata ?? A, target: i, unrepresentable: r?.unrepresentable ?? "throw", override: r?.override ?? (() => {
    }), io: r?.io ?? "output", counter: 0, seen: /* @__PURE__ */ new Map(), cycles: r?.cycles ?? "ref", reused: r?.reused ?? "inline", external: r?.external ?? void 0 };
  }
  function L(r, i, o = { path: [], schemaPath: [] }) {
    var t;
    let n = r._zod.def, v = i.seen.get(r);
    if (v) {
      if (v.count++, o.schemaPath.includes(r))
        v.cycle = o.path;
      return v.schema;
    }
    let u = { schema: {}, count: 1, cycle: void 0, path: o.path };
    i.seen.set(r, u);
    let $ = r._zod.toJSONSchema?.();
    if ($)
      u.schema = $;
    else {
      let I = { ...o, schemaPath: [...o.schemaPath, r], path: o.path };
      if (r._zod.processJSONSchema)
        r._zod.processJSONSchema(i, u.schema, I);
      else {
        let N = u.schema, O = i.processors[n.type];
        if (!O)
          throw Error(`[toJSONSchema]: Non-representable type encountered: ${n.type}`);
        O(r, i, N, I);
      }
      let _ = r._zod.parent;
      if (_) {
        if (!u.ref)
          u.ref = _;
        L(_, i, I), i.seen.get(_).isParent = true;
      }
    }
    let l = i.metadataRegistry.get(r);
    if (l)
      Object.assign(u.schema, l);
    if (i.io === "input" && Q(r))
      delete u.schema.examples, delete u.schema.default;
    if (i.io === "input" && u.schema._prefault)
      (t = u.schema).default ?? (t.default = u.schema._prefault);
    return delete u.schema._prefault, i.seen.get(r).schema;
  }
  function lr(r, i) {
    let o = r.seen.get(i);
    if (!o)
      throw Error("Unprocessed schema. This is a bug in Zod.");
    let t = /* @__PURE__ */ new Map();
    for (let u of r.seen.entries()) {
      let $ = r.metadataRegistry.get(u[0])?.id;
      if ($) {
        let l = t.get($);
        if (l && l !== u[0])
          throw Error(`Duplicate schema id "${$}" detected during JSON Schema conversion. Two different schemas cannot share the same id when converted together.`);
        t.set($, u[0]);
      }
    }
    let n = (u) => {
      let $ = r.target === "draft-2020-12" ? "$defs" : "definitions";
      if (r.external) {
        let _ = r.external.registry.get(u[0])?.id, N = r.external.uri ?? ((J) => J);
        if (_)
          return { ref: N(_) };
        let O = u[1].defId ?? u[1].schema.id ?? `schema${r.counter++}`;
        return u[1].defId = O, { defId: O, ref: `${N("__shared")}#/${$}/${O}` };
      }
      if (u[1] === o)
        return { ref: "#" };
      let e = `${"#"}/${$}/`, I = u[1].schema.id ?? `__schema${r.counter++}`;
      return { defId: I, ref: e + I };
    }, v = (u) => {
      if (u[1].schema.$ref)
        return;
      let $ = u[1], { ref: l, defId: e } = n(u);
      if ($.def = { ...$.schema }, e)
        $.defId = e;
      let I = $.schema;
      for (let _ in I)
        delete I[_];
      I.$ref = l;
    };
    if (r.cycles === "throw")
      for (let u of r.seen.entries()) {
        let $ = u[1];
        if ($.cycle)
          throw Error(`Cycle detected: #/${$.cycle?.join("/")}/<root>

Set the \`cycles\` parameter to \`"ref"\` to resolve cyclical schemas with defs.`);
      }
    for (let u of r.seen.entries()) {
      let $ = u[1];
      if (i === u[0]) {
        v(u);
        continue;
      }
      if (r.external) {
        let e = r.external.registry.get(u[0])?.id;
        if (i !== u[0] && e) {
          v(u);
          continue;
        }
      }
      if (r.metadataRegistry.get(u[0])?.id) {
        v(u);
        continue;
      }
      if ($.cycle) {
        v(u);
        continue;
      }
      if ($.count > 1) {
        if (r.reused === "ref") {
          v(u);
          continue;
        }
      }
    }
  }
  function cr(r, i) {
    let o = r.seen.get(i);
    if (!o)
      throw Error("Unprocessed schema. This is a bug in Zod.");
    let t = (u) => {
      let $ = r.seen.get(u);
      if ($.ref === null)
        return;
      let l = $.def ?? $.schema, e = { ...l }, I = $.ref;
      if ($.ref = null, I) {
        t(I);
        let N = r.seen.get(I), O = N.schema;
        if (O.$ref && (r.target === "draft-07" || r.target === "draft-04" || r.target === "openapi-3.0"))
          l.allOf = l.allOf ?? [], l.allOf.push(O);
        else
          Object.assign(l, O);
        if (Object.assign(l, e), u._zod.parent === I)
          for (let X in l) {
            if (X === "$ref" || X === "allOf")
              continue;
            if (!(X in e))
              delete l[X];
          }
        if (O.$ref)
          for (let X in l) {
            if (X === "$ref" || X === "allOf")
              continue;
            if (X in N.def && JSON.stringify(l[X]) === JSON.stringify(N.def[X]))
              delete l[X];
          }
      }
      let _ = u._zod.parent;
      if (_ && _ !== I) {
        t(_);
        let N = r.seen.get(_);
        if (N?.schema.$ref) {
          if (l.$ref = N.schema.$ref, N.def)
            for (let O in l) {
              if (O === "$ref" || O === "allOf")
                continue;
              if (O in N.def && JSON.stringify(l[O]) === JSON.stringify(N.def[O]))
                delete l[O];
            }
        }
      }
      r.override({ zodSchema: u, jsonSchema: l, path: $.path ?? [] });
    };
    for (let u of [...r.seen.entries()].reverse())
      t(u[0]);
    let n = {};
    if (r.target === "draft-2020-12")
      n.$schema = "https://json-schema.org/draft/2020-12/schema";
    else if (r.target === "draft-07")
      n.$schema = "http://json-schema.org/draft-07/schema#";
    else if (r.target === "draft-04")
      n.$schema = "http://json-schema.org/draft-04/schema#";
    else if (r.target === "openapi-3.0")
      ;
    if (r.external?.uri) {
      let u = r.external.registry.get(i)?.id;
      if (!u)
        throw Error("Schema is missing an `id` property");
      n.$id = r.external.uri(u);
    }
    Object.assign(n, o.def ?? o.schema);
    let v = r.external?.defs ?? {};
    for (let u of r.seen.entries()) {
      let $ = u[1];
      if ($.def && $.defId)
        v[$.defId] = $.def;
    }
    if (r.external)
      ;
    else if (Object.keys(v).length > 0)
      if (r.target === "draft-2020-12")
        n.$defs = v;
      else
        n.definitions = v;
    try {
      let u = JSON.parse(JSON.stringify(n));
      return Object.defineProperty(u, "~standard", { value: { ...i["~standard"], jsonSchema: { input: xr(i, "input", r.processors), output: xr(i, "output", r.processors) } }, enumerable: false, writable: false }), u;
    } catch (u) {
      throw Error("Error converting schema to JSON.");
    }
  }
  function Q(r, i) {
    let o = i ?? { seen: /* @__PURE__ */ new Set() };
    if (o.seen.has(r))
      return false;
    o.seen.add(r);
    let t = r._zod.def;
    if (t.type === "transform")
      return true;
    if (t.type === "array")
      return Q(t.element, o);
    if (t.type === "set")
      return Q(t.valueType, o);
    if (t.type === "lazy")
      return Q(t.getter(), o);
    if (t.type === "promise" || t.type === "optional" || t.type === "nonoptional" || t.type === "nullable" || t.type === "readonly" || t.type === "default" || t.type === "prefault")
      return Q(t.innerType, o);
    if (t.type === "intersection")
      return Q(t.left, o) || Q(t.right, o);
    if (t.type === "record" || t.type === "map")
      return Q(t.keyType, o) || Q(t.valueType, o);
    if (t.type === "pipe")
      return Q(t.in, o) || Q(t.out, o);
    if (t.type === "object") {
      for (let n in t.shape)
        if (Q(t.shape[n], o))
          return true;
      return false;
    }
    if (t.type === "union") {
      for (let n of t.options)
        if (Q(n, o))
          return true;
      return false;
    }
    if (t.type === "tuple") {
      for (let n of t.items)
        if (Q(n, o))
          return true;
      if (t.rest && Q(t.rest, o))
        return true;
      return false;
    }
    return false;
  }
  var w$ = (r, i = {}) => (o) => {
    let t = er({ ...o, processors: i });
    return L(r, t), lr(t, r), cr(t, r);
  };
  var xr = (r, i, o = {}) => (t) => {
    let { libraryOptions: n, target: v } = t ?? {}, u = er({ ...n ?? {}, target: v, io: i, processors: o });
    return L(r, u), lr(u, r), cr(u, r);
  };
  var O6 = { guid: "uuid", url: "uri", datetime: "date-time", json_string: "json-string", regex: "" };
  var N$ = (r, i, o, t) => {
    let n = o;
    n.type = "string";
    let { minimum: v, maximum: u, format: $, patterns: l, contentEncoding: e } = r._zod.bag;
    if (typeof v === "number")
      n.minLength = v;
    if (typeof u === "number")
      n.maxLength = u;
    if ($) {
      if (n.format = O6[$] ?? $, n.format === "")
        delete n.format;
      if ($ === "time")
        delete n.format;
    }
    if (e)
      n.contentEncoding = e;
    if (l && l.size > 0) {
      let I = [...l];
      if (I.length === 1)
        n.pattern = I[0].source;
      else if (I.length > 1)
        n.allOf = [...I.map((_) => ({ ...i.target === "draft-07" || i.target === "draft-04" || i.target === "openapi-3.0" ? { type: "string" } : {}, pattern: _.source }))];
    }
  };
  var O$ = (r, i, o, t) => {
    let n = o, { minimum: v, maximum: u, format: $, multipleOf: l, exclusiveMaximum: e, exclusiveMinimum: I } = r._zod.bag;
    if (typeof $ === "string" && $.includes("int"))
      n.type = "integer";
    else
      n.type = "number";
    if (typeof I === "number")
      if (i.target === "draft-04" || i.target === "openapi-3.0")
        n.minimum = I, n.exclusiveMinimum = true;
      else
        n.exclusiveMinimum = I;
    if (typeof v === "number") {
      if (n.minimum = v, typeof I === "number" && i.target !== "draft-04")
        if (I >= v)
          delete n.minimum;
        else
          delete n.exclusiveMinimum;
    }
    if (typeof e === "number")
      if (i.target === "draft-04" || i.target === "openapi-3.0")
        n.maximum = e, n.exclusiveMaximum = true;
      else
        n.exclusiveMaximum = e;
    if (typeof u === "number") {
      if (n.maximum = u, typeof e === "number" && i.target !== "draft-04")
        if (e <= u)
          delete n.maximum;
        else
          delete n.exclusiveMaximum;
    }
    if (typeof l === "number")
      n.multipleOf = l;
  };
  var S$ = (r, i, o, t) => {
    o.type = "boolean";
  };
  var z$ = (r, i, o, t) => {
    if (i.unrepresentable === "throw")
      throw Error("BigInt cannot be represented in JSON Schema");
  };
  var P$ = (r, i, o, t) => {
    if (i.unrepresentable === "throw")
      throw Error("Symbols cannot be represented in JSON Schema");
  };
  var j$ = (r, i, o, t) => {
    if (i.target === "openapi-3.0")
      o.type = "string", o.nullable = true, o.enum = [null];
    else
      o.type = "null";
  };
  var J$ = (r, i, o, t) => {
    if (i.unrepresentable === "throw")
      throw Error("Undefined cannot be represented in JSON Schema");
  };
  var L$ = (r, i, o, t) => {
    if (i.unrepresentable === "throw")
      throw Error("Void cannot be represented in JSON Schema");
  };
  var E$ = (r, i, o, t) => {
    o.not = {};
  };
  var G$ = (r, i, o, t) => {
  };
  var W$ = (r, i, o, t) => {
  };
  var X$ = (r, i, o, t) => {
    if (i.unrepresentable === "throw")
      throw Error("Date cannot be represented in JSON Schema");
  };
  var V$ = (r, i, o, t) => {
    let n = r._zod.def, v = nn(n.entries);
    if (v.every((u) => typeof u === "number"))
      o.type = "number";
    if (v.every((u) => typeof u === "string"))
      o.type = "string";
    o.enum = v;
  };
  var A$ = (r, i, o, t) => {
    let n = r._zod.def, v = [];
    for (let u of n.values)
      if (u === void 0) {
        if (i.unrepresentable === "throw")
          throw Error("Literal `undefined` cannot be represented in JSON Schema");
      } else if (typeof u === "bigint")
        if (i.unrepresentable === "throw")
          throw Error("BigInt literals cannot be represented in JSON Schema");
        else
          v.push(Number(u));
      else
        v.push(u);
    if (v.length === 0)
      ;
    else if (v.length === 1) {
      let u = v[0];
      if (o.type = u === null ? "null" : typeof u, i.target === "draft-04" || i.target === "openapi-3.0")
        o.enum = [u];
      else
        o.const = u;
    } else {
      if (v.every((u) => typeof u === "number"))
        o.type = "number";
      if (v.every((u) => typeof u === "string"))
        o.type = "string";
      if (v.every((u) => typeof u === "boolean"))
        o.type = "boolean";
      if (v.every((u) => u === null))
        o.type = "null";
      o.enum = v;
    }
  };
  var K$ = (r, i, o, t) => {
    if (i.unrepresentable === "throw")
      throw Error("NaN cannot be represented in JSON Schema");
  };
  var q$ = (r, i, o, t) => {
    let n = o, v = r._zod.pattern;
    if (!v)
      throw Error("Pattern not found in template literal");
    n.type = "string", n.pattern = v.source;
  };
  var Y$ = (r, i, o, t) => {
    let n = o, v = { type: "string", format: "binary", contentEncoding: "binary" }, { minimum: u, maximum: $, mime: l } = r._zod.bag;
    if (u !== void 0)
      v.minLength = u;
    if ($ !== void 0)
      v.maxLength = $;
    if (l)
      if (l.length === 1)
        v.contentMediaType = l[0], Object.assign(n, v);
      else
        Object.assign(n, v), n.anyOf = l.map((e) => ({ contentMediaType: e }));
    else
      Object.assign(n, v);
  };
  var Q$ = (r, i, o, t) => {
    o.type = "boolean";
  };
  var m$ = (r, i, o, t) => {
    if (i.unrepresentable === "throw")
      throw Error("Custom types cannot be represented in JSON Schema");
  };
  var T$ = (r, i, o, t) => {
    if (i.unrepresentable === "throw")
      throw Error("Function types cannot be represented in JSON Schema");
  };
  var F$ = (r, i, o, t) => {
    if (i.unrepresentable === "throw")
      throw Error("Transforms cannot be represented in JSON Schema");
  };
  var B$ = (r, i, o, t) => {
    if (i.unrepresentable === "throw")
      throw Error("Map cannot be represented in JSON Schema");
  };
  var H$ = (r, i, o, t) => {
    if (i.unrepresentable === "throw")
      throw Error("Set cannot be represented in JSON Schema");
  };
  var M$ = (r, i, o, t) => {
    let n = o, v = r._zod.def, { minimum: u, maximum: $ } = r._zod.bag;
    if (typeof u === "number")
      n.minItems = u;
    if (typeof $ === "number")
      n.maxItems = $;
    n.type = "array", n.items = L(v.element, i, { ...t, path: [...t.path, "items"] });
  };
  var R$ = (r, i, o, t) => {
    let n = o, v = r._zod.def;
    n.type = "object", n.properties = {};
    let u = v.shape;
    for (let e in u)
      n.properties[e] = L(u[e], i, { ...t, path: [...t.path, "properties", e] });
    let $ = new Set(Object.keys(u)), l = new Set([...$].filter((e) => {
      let I = v.shape[e]._zod;
      if (i.io === "input")
        return I.optin === void 0;
      else
        return I.optout === void 0;
    }));
    if (l.size > 0)
      n.required = Array.from(l);
    if (v.catchall?._zod.def.type === "never")
      n.additionalProperties = false;
    else if (!v.catchall) {
      if (i.io === "output")
        n.additionalProperties = false;
    } else if (v.catchall)
      n.additionalProperties = L(v.catchall, i, { ...t, path: [...t.path, "additionalProperties"] });
  };
  var qi = (r, i, o, t) => {
    let n = r._zod.def, v = n.inclusive === false, u = n.options.map(($, l) => L($, i, { ...t, path: [...t.path, v ? "oneOf" : "anyOf", l] }));
    if (v)
      o.oneOf = u;
    else
      o.anyOf = u;
  };
  var x$ = (r, i, o, t) => {
    let n = r._zod.def, v = L(n.left, i, { ...t, path: [...t.path, "allOf", 0] }), u = L(n.right, i, { ...t, path: [...t.path, "allOf", 1] }), $ = (e) => "allOf" in e && Object.keys(e).length === 1, l = [...$(v) ? v.allOf : [v], ...$(u) ? u.allOf : [u]];
    o.allOf = l;
  };
  var Z$ = (r, i, o, t) => {
    let n = o, v = r._zod.def;
    n.type = "array";
    let u = i.target === "draft-2020-12" ? "prefixItems" : "items", $ = i.target === "draft-2020-12" ? "items" : i.target === "openapi-3.0" ? "items" : "additionalItems", l = v.items.map((N, O) => L(N, i, { ...t, path: [...t.path, u, O] })), e = v.rest ? L(v.rest, i, { ...t, path: [...t.path, $, ...i.target === "openapi-3.0" ? [v.items.length] : []] }) : null;
    if (i.target === "draft-2020-12") {
      if (n.prefixItems = l, e)
        n.items = e;
    } else if (i.target === "openapi-3.0") {
      if (n.items = { anyOf: l }, e)
        n.items.anyOf.push(e);
      if (n.minItems = l.length, !e)
        n.maxItems = l.length;
    } else if (n.items = l, e)
      n.additionalItems = e;
    let { minimum: I, maximum: _ } = r._zod.bag;
    if (typeof I === "number")
      n.minItems = I;
    if (typeof _ === "number")
      n.maxItems = _;
  };
  var d$ = (r, i, o, t) => {
    let n = o, v = r._zod.def;
    n.type = "object";
    let u = v.keyType, l = u._zod.bag?.patterns;
    if (v.mode === "loose" && l && l.size > 0) {
      let I = L(v.valueType, i, { ...t, path: [...t.path, "patternProperties", "*"] });
      n.patternProperties = {};
      for (let _ of l)
        n.patternProperties[_.source] = I;
    } else {
      if (i.target === "draft-07" || i.target === "draft-2020-12")
        n.propertyNames = L(v.keyType, i, { ...t, path: [...t.path, "propertyNames"] });
      n.additionalProperties = L(v.valueType, i, { ...t, path: [...t.path, "additionalProperties"] });
    }
    let e = u._zod.values;
    if (e) {
      let I = [...e].filter((_) => typeof _ === "string" || typeof _ === "number");
      if (I.length > 0)
        n.required = I;
    }
  };
  var C$ = (r, i, o, t) => {
    let n = r._zod.def, v = L(n.innerType, i, t), u = i.seen.get(r);
    if (i.target === "openapi-3.0")
      u.ref = n.innerType, o.nullable = true;
    else
      o.anyOf = [v, { type: "null" }];
  };
  var f$ = (r, i, o, t) => {
    let n = r._zod.def;
    L(n.innerType, i, t);
    let v = i.seen.get(r);
    v.ref = n.innerType;
  };
  var h$ = (r, i, o, t) => {
    let n = r._zod.def;
    L(n.innerType, i, t);
    let v = i.seen.get(r);
    v.ref = n.innerType, o.default = JSON.parse(JSON.stringify(n.defaultValue));
  };
  var y$ = (r, i, o, t) => {
    let n = r._zod.def;
    L(n.innerType, i, t);
    let v = i.seen.get(r);
    if (v.ref = n.innerType, i.io === "input")
      o._prefault = JSON.parse(JSON.stringify(n.defaultValue));
  };
  var a$ = (r, i, o, t) => {
    let n = r._zod.def;
    L(n.innerType, i, t);
    let v = i.seen.get(r);
    v.ref = n.innerType;
    let u;
    try {
      u = n.catchValue(void 0);
    } catch {
      throw Error("Dynamic catch values are not supported in JSON Schema");
    }
    o.default = u;
  };
  var p$ = (r, i, o, t) => {
    let n = r._zod.def, v = i.io === "input" ? n.in._zod.def.type === "transform" ? n.out : n.in : n.out;
    L(v, i, t);
    let u = i.seen.get(r);
    u.ref = v;
  };
  var s$ = (r, i, o, t) => {
    let n = r._zod.def;
    L(n.innerType, i, t);
    let v = i.seen.get(r);
    v.ref = n.innerType, o.readOnly = true;
  };
  var rg = (r, i, o, t) => {
    let n = r._zod.def;
    L(n.innerType, i, t);
    let v = i.seen.get(r);
    v.ref = n.innerType;
  };
  var Yi = (r, i, o, t) => {
    let n = r._zod.def;
    L(n.innerType, i, t);
    let v = i.seen.get(r);
    v.ref = n.innerType;
  };
  var ng = (r, i, o, t) => {
    let n = r._zod.innerType;
    L(n, i, t);
    let v = i.seen.get(r);
    v.ref = n;
  };
  var Ki = { string: N$, number: O$, boolean: S$, bigint: z$, symbol: P$, null: j$, undefined: J$, void: L$, never: E$, any: G$, unknown: W$, date: X$, enum: V$, literal: A$, nan: K$, template_literal: q$, file: Y$, success: Q$, custom: m$, function: T$, transform: F$, map: B$, set: H$, array: M$, object: R$, union: qi, intersection: x$, tuple: Z$, record: d$, nullable: C$, nonoptional: f$, default: h$, prefault: y$, catch: a$, pipe: p$, readonly: s$, promise: rg, optional: Yi, lazy: ng };
  function Qi(r, i) {
    if ("_idmap" in r) {
      let t = r, n = er({ ...i, processors: Ki }), v = {};
      for (let l of t._idmap.entries()) {
        let [e, I] = l;
        L(I, n);
      }
      let u = {}, $ = { registry: t, uri: i?.uri, defs: v };
      n.external = $;
      for (let l of t._idmap.entries()) {
        let [e, I] = l;
        lr(n, I), u[e] = cr(n, I);
      }
      if (Object.keys(v).length > 0) {
        let l = n.target === "draft-2020-12" ? "$defs" : "definitions";
        u.__shared = { [l]: v };
      }
      return { schemas: u };
    }
    let o = er({ ...i, processors: Ki });
    return L(r, o), lr(o, r), cr(o, r);
  }
  var ig = class {
    get metadataRegistry() {
      return this.ctx.metadataRegistry;
    }
    get target() {
      return this.ctx.target;
    }
    get unrepresentable() {
      return this.ctx.unrepresentable;
    }
    get override() {
      return this.ctx.override;
    }
    get io() {
      return this.ctx.io;
    }
    get counter() {
      return this.ctx.counter;
    }
    set counter(r) {
      this.ctx.counter = r;
    }
    get seen() {
      return this.ctx.seen;
    }
    constructor(r) {
      let i = r?.target ?? "draft-2020-12";
      if (i === "draft-4")
        i = "draft-04";
      if (i === "draft-7")
        i = "draft-07";
      this.ctx = er({ processors: Ki, target: i, ...r?.metadata && { metadata: r.metadata }, ...r?.unrepresentable && { unrepresentable: r.unrepresentable }, ...r?.override && { override: r.override }, ...r?.io && { io: r.io } });
    }
    process(r, i = { path: [], schemaPath: [] }) {
      return L(r, this.ctx, i);
    }
    emit(r, i) {
      if (i) {
        if (i.cycles)
          this.ctx.cycles = i.cycles;
        if (i.reused)
          this.ctx.reused = i.reused;
        if (i.external)
          this.ctx.external = i.external;
      }
      lr(this.ctx, r);
      let o = cr(this.ctx, r), { "~standard": t, ...n } = o;
      return n;
    }
  };
  var ol = {};
  var Pn = {};
  s(Pn, { xor: () => yl, xid: () => Nl, void: () => xl, uuidv7: () => cl, uuidv6: () => ll, uuidv4: () => el, uuid: () => gl, url: () => Il, unknown: () => Nr, union: () => ev, undefined: () => Ml, ulid: () => wl, uint64: () => Bl, uint32: () => ml, tuple: () => Qg, transform: () => cv, templateLiteral: () => ec, symbol: () => Hl, superRefine: () => ee, success: () => uc, stringbool: () => Dc, stringFormat: () => Xl, string: () => Mi, strictObject: () => fl, set: () => nc, refine: () => ge, record: () => mg, readonly: () => ie, promise: () => lc, preprocess: () => Nc, prefault: () => hg, pipe: () => En, partialRecord: () => pl, optional: () => Jn, object: () => Cl, number: () => Og, nullish: () => tc, nullable: () => Ln, null: () => Jg, nonoptional: () => yg, never: () => gv, nativeEnum: () => ic, nanoid: () => Ul, nan: () => $c, meta: () => Uc, map: () => rc, mac: () => zl, looseRecord: () => sl, looseObject: () => hl, literal: () => vc, lazy: () => te, ksuid: () => Ol, keyof: () => dl, jwt: () => Wl, json: () => wc, ipv6: () => Pl, ipv4: () => Sl, intersection: () => qg, int64: () => Fl, int32: () => Ql, int: () => Ri, instanceof: () => kc, httpUrl: () => bl, hostname: () => Vl, hex: () => Al, hash: () => Kl, guid: () => $l, function: () => cc, float64: () => Yl, float32: () => ql, file: () => oc, exactOptional: () => xg, enum: () => lv, emoji: () => _l, email: () => ul, e164: () => Gl, discriminatedUnion: () => al, describe: () => _c, date: () => Zl, custom: () => bc, cuid2: () => Dl, cuid: () => kl, codec: () => gc, cidrv6: () => Jl, cidrv4: () => jl, check: () => Ic, catch: () => sg, boolean: () => Sg, bigint: () => Tl, base64url: () => El, base64: () => Ll, array: () => Xn, any: () => Rl, _function: () => cc, _default: () => Cg, _ZodString: () => xi, ZodXor: () => Vg, ZodXID: () => ai, ZodVoid: () => Wg, ZodUnknown: () => Eg, ZodUnion: () => An, ZodUndefined: () => Pg, ZodUUID: () => p, ZodURL: () => Gn, ZodULID: () => yi, ZodType: () => P, ZodTuple: () => Yg, ZodTransform: () => Mg, ZodTemplateLiteral: () => ve, ZodSymbol: () => zg, ZodSuccess: () => ag, ZodStringFormat: () => G, ZodString: () => Cr, ZodSet: () => Fg, ZodRecord: () => Kn, ZodReadonly: () => ne, ZodPromise: () => ue, ZodPrefault: () => fg, ZodPipe: () => _v, ZodOptional: () => Iv, ZodObject: () => Vn, ZodNumberFormat: () => Or, ZodNumber: () => hr, ZodNullable: () => Zg, ZodNull: () => jg, ZodNonOptional: () => bv, ZodNever: () => Gg, ZodNanoID: () => Ci, ZodNaN: () => re, ZodMap: () => Tg, ZodMAC: () => Ng, ZodLiteral: () => Bg, ZodLazy: () => oe, ZodKSUID: () => pi, ZodJWT: () => uv, ZodIntersection: () => Kg, ZodIPv6: () => rv, ZodIPv4: () => si, ZodGUID: () => jn, ZodFunction: () => $e, ZodFile: () => Hg, ZodExactOptional: () => Rg, ZodEnum: () => dr, ZodEmoji: () => di, ZodEmail: () => Zi, ZodE164: () => tv, ZodDiscriminatedUnion: () => Ag, ZodDefault: () => dg, ZodDate: () => Wn, ZodCustomStringFormat: () => fr, ZodCustom: () => qn, ZodCodec: () => Uv, ZodCatch: () => pg, ZodCUID2: () => hi, ZodCUID: () => fi, ZodCIDRv6: () => iv, ZodCIDRv4: () => nv, ZodBoolean: () => yr, ZodBigIntFormat: () => $v, ZodBigInt: () => ar, ZodBase64URL: () => ov, ZodBase64: () => vv, ZodArray: () => Xg, ZodAny: () => Lg });
  var mi = {};
  s(mi, { uppercase: () => Kr, trim: () => Fr, toUpperCase: () => Hr, toLowerCase: () => Br, startsWith: () => Yr, slugify: () => Mr, size: () => kr, regex: () => Vr, property: () => Ai, positive: () => Gi, overwrite: () => d, normalize: () => Tr, nonpositive: () => Xi, nonnegative: () => Vi, negative: () => Wi, multipleOf: () => $r, minSize: () => a, minLength: () => nr, mime: () => mr, maxSize: () => gr, maxLength: () => Dr, lte: () => M, lt: () => h, lowercase: () => Ar, length: () => wr, includes: () => qr, gte: () => Y, gt: () => y, endsWith: () => Qr });
  var Zr = {};
  s(Zr, { time: () => tg, duration: () => ug, datetime: () => vg, date: () => og, ZodISOTime: () => Bi, ZodISODuration: () => Hi, ZodISODateTime: () => Ti, ZodISODate: () => Fi });
  var Ti = c("ZodISODateTime", (r, i) => {
    Fo.init(r, i), G.init(r, i);
  });
  function vg(r) {
    return mu(Ti, r);
  }
  var Fi = c("ZodISODate", (r, i) => {
    Bo.init(r, i), G.init(r, i);
  });
  function og(r) {
    return Tu(Fi, r);
  }
  var Bi = c("ZodISOTime", (r, i) => {
    Ho.init(r, i), G.init(r, i);
  });
  function tg(r) {
    return Fu(Bi, r);
  }
  var Hi = c("ZodISODuration", (r, i) => {
    Mo.init(r, i), G.init(r, i);
  });
  function ug(r) {
    return Bu(Hi, r);
  }
  var tl = (r, i) => {
    $n.init(r, i), r.name = "ZodError", Object.defineProperties(r, { format: { value: (o) => en(r, o) }, flatten: { value: (o) => gn(r, o) }, addIssue: { value: (o) => {
      r.issues.push(o), r.message = JSON.stringify(r.issues, zr, 2);
    } }, addIssues: { value: (o) => {
      r.issues.push(...o), r.message = JSON.stringify(r.issues, zr, 2);
    } }, isEmpty: { get() {
      return r.issues.length === 0;
    } } });
  };
  var z6 = c("ZodError", tl);
  var B = c("ZodError", tl, { Parent: Error });
  var $g = Jr(B);
  var gg = Lr(B);
  var eg = Er(B);
  var lg = Gr(B);
  var cg = Bn(B);
  var Ig = Hn(B);
  var bg = Mn(B);
  var _g = Rn(B);
  var Ug = xn(B);
  var kg = Zn(B);
  var Dg = dn(B);
  var wg = Cn(B);
  var P = c("ZodType", (r, i) => {
    return z.init(r, i), Object.assign(r["~standard"], { jsonSchema: { input: xr(r, "input"), output: xr(r, "output") } }), r.toJSONSchema = w$(r, {}), r.def = i, r.type = i.type, Object.defineProperty(r, "_def", { value: i }), r.check = (...o) => {
      return r.clone(D.mergeDefs(i, { checks: [...i.checks ?? [], ...o.map((t) => typeof t === "function" ? { _zod: { check: t, def: { check: "custom" }, onattach: [] } } : t)] }), { parent: true });
    }, r.with = r.check, r.clone = (o, t) => q(r, o, t), r.brand = () => r, r.register = (o, t) => {
      return o.add(r, t), r;
    }, r.parse = (o, t) => $g(r, o, t, { callee: r.parse }), r.safeParse = (o, t) => eg(r, o, t), r.parseAsync = async (o, t) => gg(r, o, t, { callee: r.parseAsync }), r.safeParseAsync = async (o, t) => lg(r, o, t), r.spa = r.safeParseAsync, r.encode = (o, t) => cg(r, o, t), r.decode = (o, t) => Ig(r, o, t), r.encodeAsync = async (o, t) => bg(r, o, t), r.decodeAsync = async (o, t) => _g(r, o, t), r.safeEncode = (o, t) => Ug(r, o, t), r.safeDecode = (o, t) => kg(r, o, t), r.safeEncodeAsync = async (o, t) => Dg(r, o, t), r.safeDecodeAsync = async (o, t) => wg(r, o, t), r.refine = (o, t) => r.check(ge(o, t)), r.superRefine = (o) => r.check(ee(o)), r.overwrite = (o) => r.check(d(o)), r.optional = () => Jn(r), r.exactOptional = () => xg(r), r.nullable = () => Ln(r), r.nullish = () => Jn(Ln(r)), r.nonoptional = (o) => yg(r, o), r.array = () => Xn(r), r.or = (o) => ev([r, o]), r.and = (o) => qg(r, o), r.transform = (o) => En(r, cv(o)), r.default = (o) => Cg(r, o), r.prefault = (o) => hg(r, o), r.catch = (o) => sg(r, o), r.pipe = (o) => En(r, o), r.readonly = () => ie(r), r.describe = (o) => {
      let t = r.clone();
      return A.add(t, { description: o }), t;
    }, Object.defineProperty(r, "description", { get() {
      return A.get(r)?.description;
    }, configurable: true }), r.meta = (...o) => {
      if (o.length === 0)
        return A.get(r);
      let t = r.clone();
      return A.add(t, o[0]), t;
    }, r.isOptional = () => r.safeParse(void 0).success, r.isNullable = () => r.safeParse(null).success, r.apply = (o) => o(r), r;
  });
  var xi = c("_ZodString", (r, i) => {
    Ur.init(r, i), P.init(r, i), r._zod.processJSONSchema = (t, n, v) => N$(r, t, n, v);
    let o = r._zod.bag;
    r.format = o.format ?? null, r.minLength = o.minimum ?? null, r.maxLength = o.maximum ?? null, r.regex = (...t) => r.check(Vr(...t)), r.includes = (...t) => r.check(qr(...t)), r.startsWith = (...t) => r.check(Yr(...t)), r.endsWith = (...t) => r.check(Qr(...t)), r.min = (...t) => r.check(nr(...t)), r.max = (...t) => r.check(Dr(...t)), r.length = (...t) => r.check(wr(...t)), r.nonempty = (...t) => r.check(nr(1, ...t)), r.lowercase = (t) => r.check(Ar(t)), r.uppercase = (t) => r.check(Kr(t)), r.trim = () => r.check(Fr()), r.normalize = (...t) => r.check(Tr(...t)), r.toLowerCase = () => r.check(Br()), r.toUpperCase = () => r.check(Hr()), r.slugify = () => r.check(Mr());
  });
  var Cr = c("ZodString", (r, i) => {
    Ur.init(r, i), xi.init(r, i), r.email = (o) => r.check(gi(Zi, o)), r.url = (o) => r.check(zn(Gn, o)), r.jwt = (o) => r.check(Ei(uv, o)), r.emoji = (o) => r.check(bi(di, o)), r.guid = (o) => r.check(Sn(jn, o)), r.uuid = (o) => r.check(ei(p, o)), r.uuidv4 = (o) => r.check(li(p, o)), r.uuidv6 = (o) => r.check(ci(p, o)), r.uuidv7 = (o) => r.check(Ii(p, o)), r.nanoid = (o) => r.check(_i(Ci, o)), r.guid = (o) => r.check(Sn(jn, o)), r.cuid = (o) => r.check(Ui(fi, o)), r.cuid2 = (o) => r.check(ki(hi, o)), r.ulid = (o) => r.check(Di(yi, o)), r.base64 = (o) => r.check(ji(vv, o)), r.base64url = (o) => r.check(Ji(ov, o)), r.xid = (o) => r.check(wi(ai, o)), r.ksuid = (o) => r.check(Ni(pi, o)), r.ipv4 = (o) => r.check(Oi(si, o)), r.ipv6 = (o) => r.check(Si(rv, o)), r.cidrv4 = (o) => r.check(zi(nv, o)), r.cidrv6 = (o) => r.check(Pi(iv, o)), r.e164 = (o) => r.check(Li(tv, o)), r.datetime = (o) => r.check(vg(o)), r.date = (o) => r.check(og(o)), r.time = (o) => r.check(tg(o)), r.duration = (o) => r.check(ug(o));
  });
  function Mi(r) {
    return Ku(Cr, r);
  }
  var G = c("ZodStringFormat", (r, i) => {
    E.init(r, i), xi.init(r, i);
  });
  var Zi = c("ZodEmail", (r, i) => {
    Xo.init(r, i), G.init(r, i);
  });
  function ul(r) {
    return gi(Zi, r);
  }
  var jn = c("ZodGUID", (r, i) => {
    Go.init(r, i), G.init(r, i);
  });
  function $l(r) {
    return Sn(jn, r);
  }
  var p = c("ZodUUID", (r, i) => {
    Wo.init(r, i), G.init(r, i);
  });
  function gl(r) {
    return ei(p, r);
  }
  function el(r) {
    return li(p, r);
  }
  function ll(r) {
    return ci(p, r);
  }
  function cl(r) {
    return Ii(p, r);
  }
  var Gn = c("ZodURL", (r, i) => {
    Vo.init(r, i), G.init(r, i);
  });
  function Il(r) {
    return zn(Gn, r);
  }
  function bl(r) {
    return zn(Gn, { protocol: /^https?$/, hostname: x.domain, ...D.normalizeParams(r) });
  }
  var di = c("ZodEmoji", (r, i) => {
    Ao.init(r, i), G.init(r, i);
  });
  function _l(r) {
    return bi(di, r);
  }
  var Ci = c("ZodNanoID", (r, i) => {
    Ko.init(r, i), G.init(r, i);
  });
  function Ul(r) {
    return _i(Ci, r);
  }
  var fi = c("ZodCUID", (r, i) => {
    qo.init(r, i), G.init(r, i);
  });
  function kl(r) {
    return Ui(fi, r);
  }
  var hi = c("ZodCUID2", (r, i) => {
    Yo.init(r, i), G.init(r, i);
  });
  function Dl(r) {
    return ki(hi, r);
  }
  var yi = c("ZodULID", (r, i) => {
    Qo.init(r, i), G.init(r, i);
  });
  function wl(r) {
    return Di(yi, r);
  }
  var ai = c("ZodXID", (r, i) => {
    mo.init(r, i), G.init(r, i);
  });
  function Nl(r) {
    return wi(ai, r);
  }
  var pi = c("ZodKSUID", (r, i) => {
    To.init(r, i), G.init(r, i);
  });
  function Ol(r) {
    return Ni(pi, r);
  }
  var si = c("ZodIPv4", (r, i) => {
    Ro.init(r, i), G.init(r, i);
  });
  function Sl(r) {
    return Oi(si, r);
  }
  var Ng = c("ZodMAC", (r, i) => {
    Zo.init(r, i), G.init(r, i);
  });
  function zl(r) {
    return Yu(Ng, r);
  }
  var rv = c("ZodIPv6", (r, i) => {
    xo.init(r, i), G.init(r, i);
  });
  function Pl(r) {
    return Si(rv, r);
  }
  var nv = c("ZodCIDRv4", (r, i) => {
    Co.init(r, i), G.init(r, i);
  });
  function jl(r) {
    return zi(nv, r);
  }
  var iv = c("ZodCIDRv6", (r, i) => {
    fo.init(r, i), G.init(r, i);
  });
  function Jl(r) {
    return Pi(iv, r);
  }
  var vv = c("ZodBase64", (r, i) => {
    yo.init(r, i), G.init(r, i);
  });
  function Ll(r) {
    return ji(vv, r);
  }
  var ov = c("ZodBase64URL", (r, i) => {
    ao.init(r, i), G.init(r, i);
  });
  function El(r) {
    return Ji(ov, r);
  }
  var tv = c("ZodE164", (r, i) => {
    po.init(r, i), G.init(r, i);
  });
  function Gl(r) {
    return Li(tv, r);
  }
  var uv = c("ZodJWT", (r, i) => {
    so.init(r, i), G.init(r, i);
  });
  function Wl(r) {
    return Ei(uv, r);
  }
  var fr = c("ZodCustomStringFormat", (r, i) => {
    rt.init(r, i), G.init(r, i);
  });
  function Xl(r, i, o = {}) {
    return Rr(fr, r, i, o);
  }
  function Vl(r) {
    return Rr(fr, "hostname", x.hostname, r);
  }
  function Al(r) {
    return Rr(fr, "hex", x.hex, r);
  }
  function Kl(r, i) {
    let o = i?.enc ?? "hex", t = `${r}_${o}`, n = x[t];
    if (!n)
      throw Error(`Unrecognized hash format: ${t}`);
    return Rr(fr, t, n, i);
  }
  var hr = c("ZodNumber", (r, i) => {
    vi.init(r, i), P.init(r, i), r._zod.processJSONSchema = (t, n, v) => O$(r, t, n, v), r.gt = (t, n) => r.check(y(t, n)), r.gte = (t, n) => r.check(Y(t, n)), r.min = (t, n) => r.check(Y(t, n)), r.lt = (t, n) => r.check(h(t, n)), r.lte = (t, n) => r.check(M(t, n)), r.max = (t, n) => r.check(M(t, n)), r.int = (t) => r.check(Ri(t)), r.safe = (t) => r.check(Ri(t)), r.positive = (t) => r.check(y(0, t)), r.nonnegative = (t) => r.check(Y(0, t)), r.negative = (t) => r.check(h(0, t)), r.nonpositive = (t) => r.check(M(0, t)), r.multipleOf = (t, n) => r.check($r(t, n)), r.step = (t, n) => r.check($r(t, n)), r.finite = () => r;
    let o = r._zod.bag;
    r.minValue = Math.max(o.minimum ?? Number.NEGATIVE_INFINITY, o.exclusiveMinimum ?? Number.NEGATIVE_INFINITY) ?? null, r.maxValue = Math.min(o.maximum ?? Number.POSITIVE_INFINITY, o.exclusiveMaximum ?? Number.POSITIVE_INFINITY) ?? null, r.isInt = (o.format ?? "").includes("int") || Number.isSafeInteger(o.multipleOf ?? 0.5), r.isFinite = true, r.format = o.format ?? null;
  });
  function Og(r) {
    return Hu(hr, r);
  }
  var Or = c("ZodNumberFormat", (r, i) => {
    nt.init(r, i), hr.init(r, i);
  });
  function Ri(r) {
    return Ru(Or, r);
  }
  function ql(r) {
    return xu(Or, r);
  }
  function Yl(r) {
    return Zu(Or, r);
  }
  function Ql(r) {
    return du(Or, r);
  }
  function ml(r) {
    return Cu(Or, r);
  }
  var yr = c("ZodBoolean", (r, i) => {
    bn.init(r, i), P.init(r, i), r._zod.processJSONSchema = (o, t, n) => S$(r, o, t, n);
  });
  function Sg(r) {
    return fu(yr, r);
  }
  var ar = c("ZodBigInt", (r, i) => {
    oi.init(r, i), P.init(r, i), r._zod.processJSONSchema = (t, n, v) => z$(r, t, n, v), r.gte = (t, n) => r.check(Y(t, n)), r.min = (t, n) => r.check(Y(t, n)), r.gt = (t, n) => r.check(y(t, n)), r.gte = (t, n) => r.check(Y(t, n)), r.min = (t, n) => r.check(Y(t, n)), r.lt = (t, n) => r.check(h(t, n)), r.lte = (t, n) => r.check(M(t, n)), r.max = (t, n) => r.check(M(t, n)), r.positive = (t) => r.check(y(BigInt(0), t)), r.negative = (t) => r.check(h(BigInt(0), t)), r.nonpositive = (t) => r.check(M(BigInt(0), t)), r.nonnegative = (t) => r.check(Y(BigInt(0), t)), r.multipleOf = (t, n) => r.check($r(t, n));
    let o = r._zod.bag;
    r.minValue = o.minimum ?? null, r.maxValue = o.maximum ?? null, r.format = o.format ?? null;
  });
  function Tl(r) {
    return yu(ar, r);
  }
  var $v = c("ZodBigIntFormat", (r, i) => {
    it.init(r, i), ar.init(r, i);
  });
  function Fl(r) {
    return pu($v, r);
  }
  function Bl(r) {
    return su($v, r);
  }
  var zg = c("ZodSymbol", (r, i) => {
    vt.init(r, i), P.init(r, i), r._zod.processJSONSchema = (o, t, n) => P$(r, o, t, n);
  });
  function Hl(r) {
    return r$(zg, r);
  }
  var Pg = c("ZodUndefined", (r, i) => {
    ot.init(r, i), P.init(r, i), r._zod.processJSONSchema = (o, t, n) => J$(r, o, t, n);
  });
  function Ml(r) {
    return n$(Pg, r);
  }
  var jg = c("ZodNull", (r, i) => {
    tt.init(r, i), P.init(r, i), r._zod.processJSONSchema = (o, t, n) => j$(r, o, t, n);
  });
  function Jg(r) {
    return i$(jg, r);
  }
  var Lg = c("ZodAny", (r, i) => {
    ut.init(r, i), P.init(r, i), r._zod.processJSONSchema = (o, t, n) => G$(r, o, t, n);
  });
  function Rl() {
    return v$(Lg);
  }
  var Eg = c("ZodUnknown", (r, i) => {
    $t.init(r, i), P.init(r, i), r._zod.processJSONSchema = (o, t, n) => W$(r, o, t, n);
  });
  function Nr() {
    return o$(Eg);
  }
  var Gg = c("ZodNever", (r, i) => {
    gt.init(r, i), P.init(r, i), r._zod.processJSONSchema = (o, t, n) => E$(r, o, t, n);
  });
  function gv(r) {
    return t$(Gg, r);
  }
  var Wg = c("ZodVoid", (r, i) => {
    et.init(r, i), P.init(r, i), r._zod.processJSONSchema = (o, t, n) => L$(r, o, t, n);
  });
  function xl(r) {
    return u$(Wg, r);
  }
  var Wn = c("ZodDate", (r, i) => {
    lt.init(r, i), P.init(r, i), r._zod.processJSONSchema = (t, n, v) => X$(r, t, n, v), r.min = (t, n) => r.check(Y(t, n)), r.max = (t, n) => r.check(M(t, n));
    let o = r._zod.bag;
    r.minDate = o.minimum ? new Date(o.minimum) : null, r.maxDate = o.maximum ? new Date(o.maximum) : null;
  });
  function Zl(r) {
    return $$(Wn, r);
  }
  var Xg = c("ZodArray", (r, i) => {
    ct.init(r, i), P.init(r, i), r._zod.processJSONSchema = (o, t, n) => M$(r, o, t, n), r.element = i.element, r.min = (o, t) => r.check(nr(o, t)), r.nonempty = (o) => r.check(nr(1, o)), r.max = (o, t) => r.check(Dr(o, t)), r.length = (o, t) => r.check(wr(o, t)), r.unwrap = () => r.element;
  });
  function Xn(r, i) {
    return l$(Xg, r, i);
  }
  function dl(r) {
    let i = r._zod.def.shape;
    return lv(Object.keys(i));
  }
  var Vn = c("ZodObject", (r, i) => {
    It.init(r, i), P.init(r, i), r._zod.processJSONSchema = (o, t, n) => R$(r, o, t, n), D.defineLazy(r, "shape", () => {
      return i.shape;
    }), r.keyof = () => lv(Object.keys(r._zod.def.shape)), r.catchall = (o) => r.clone({ ...r._zod.def, catchall: o }), r.passthrough = () => r.clone({ ...r._zod.def, catchall: Nr() }), r.loose = () => r.clone({ ...r._zod.def, catchall: Nr() }), r.strict = () => r.clone({ ...r._zod.def, catchall: gv() }), r.strip = () => r.clone({ ...r._zod.def, catchall: void 0 }), r.extend = (o) => {
      return D.extend(r, o);
    }, r.safeExtend = (o) => {
      return D.safeExtend(r, o);
    }, r.merge = (o) => D.merge(r, o), r.pick = (o) => D.pick(r, o), r.omit = (o) => D.omit(r, o), r.partial = (...o) => D.partial(Iv, r, o[0]), r.required = (...o) => D.required(bv, r, o[0]);
  });
  function Cl(r, i) {
    let o = { type: "object", shape: r ?? {}, ...D.normalizeParams(i) };
    return new Vn(o);
  }
  function fl(r, i) {
    return new Vn({ type: "object", shape: r, catchall: gv(), ...D.normalizeParams(i) });
  }
  function hl(r, i) {
    return new Vn({ type: "object", shape: r, catchall: Nr(), ...D.normalizeParams(i) });
  }
  var An = c("ZodUnion", (r, i) => {
    _n.init(r, i), P.init(r, i), r._zod.processJSONSchema = (o, t, n) => qi(r, o, t, n), r.options = i.options;
  });
  function ev(r, i) {
    return new An({ type: "union", options: r, ...D.normalizeParams(i) });
  }
  var Vg = c("ZodXor", (r, i) => {
    An.init(r, i), bt.init(r, i), r._zod.processJSONSchema = (o, t, n) => qi(r, o, t, n), r.options = i.options;
  });
  function yl(r, i) {
    return new Vg({ type: "union", options: r, inclusive: false, ...D.normalizeParams(i) });
  }
  var Ag = c("ZodDiscriminatedUnion", (r, i) => {
    An.init(r, i), _t.init(r, i);
  });
  function al(r, i, o) {
    return new Ag({ type: "union", options: i, discriminator: r, ...D.normalizeParams(o) });
  }
  var Kg = c("ZodIntersection", (r, i) => {
    Ut.init(r, i), P.init(r, i), r._zod.processJSONSchema = (o, t, n) => x$(r, o, t, n);
  });
  function qg(r, i) {
    return new Kg({ type: "intersection", left: r, right: i });
  }
  var Yg = c("ZodTuple", (r, i) => {
    ti.init(r, i), P.init(r, i), r._zod.processJSONSchema = (o, t, n) => Z$(r, o, t, n), r.rest = (o) => r.clone({ ...r._zod.def, rest: o });
  });
  function Qg(r, i, o) {
    let t = i instanceof z, n = t ? o : i;
    return new Yg({ type: "tuple", items: r, rest: t ? i : null, ...D.normalizeParams(n) });
  }
  var Kn = c("ZodRecord", (r, i) => {
    kt.init(r, i), P.init(r, i), r._zod.processJSONSchema = (o, t, n) => d$(r, o, t, n), r.keyType = i.keyType, r.valueType = i.valueType;
  });
  function mg(r, i, o) {
    return new Kn({ type: "record", keyType: r, valueType: i, ...D.normalizeParams(o) });
  }
  function pl(r, i, o) {
    let t = q(r);
    return t._zod.values = void 0, new Kn({ type: "record", keyType: t, valueType: i, ...D.normalizeParams(o) });
  }
  function sl(r, i, o) {
    return new Kn({ type: "record", keyType: r, valueType: i, mode: "loose", ...D.normalizeParams(o) });
  }
  var Tg = c("ZodMap", (r, i) => {
    Dt.init(r, i), P.init(r, i), r._zod.processJSONSchema = (o, t, n) => B$(r, o, t, n), r.keyType = i.keyType, r.valueType = i.valueType, r.min = (...o) => r.check(a(...o)), r.nonempty = (o) => r.check(a(1, o)), r.max = (...o) => r.check(gr(...o)), r.size = (...o) => r.check(kr(...o));
  });
  function rc(r, i, o) {
    return new Tg({ type: "map", keyType: r, valueType: i, ...D.normalizeParams(o) });
  }
  var Fg = c("ZodSet", (r, i) => {
    wt.init(r, i), P.init(r, i), r._zod.processJSONSchema = (o, t, n) => H$(r, o, t, n), r.min = (...o) => r.check(a(...o)), r.nonempty = (o) => r.check(a(1, o)), r.max = (...o) => r.check(gr(...o)), r.size = (...o) => r.check(kr(...o));
  });
  function nc(r, i) {
    return new Fg({ type: "set", valueType: r, ...D.normalizeParams(i) });
  }
  var dr = c("ZodEnum", (r, i) => {
    Nt.init(r, i), P.init(r, i), r._zod.processJSONSchema = (t, n, v) => V$(r, t, n, v), r.enum = i.entries, r.options = Object.values(i.entries);
    let o = new Set(Object.keys(i.entries));
    r.extract = (t, n) => {
      let v = {};
      for (let u of t)
        if (o.has(u))
          v[u] = i.entries[u];
        else
          throw Error(`Key ${u} not found in enum`);
      return new dr({ ...i, checks: [], ...D.normalizeParams(n), entries: v });
    }, r.exclude = (t, n) => {
      let v = { ...i.entries };
      for (let u of t)
        if (o.has(u))
          delete v[u];
        else
          throw Error(`Key ${u} not found in enum`);
      return new dr({ ...i, checks: [], ...D.normalizeParams(n), entries: v });
    };
  });
  function lv(r, i) {
    let o = Array.isArray(r) ? Object.fromEntries(r.map((t) => [t, t])) : r;
    return new dr({ type: "enum", entries: o, ...D.normalizeParams(i) });
  }
  function ic(r, i) {
    return new dr({ type: "enum", entries: r, ...D.normalizeParams(i) });
  }
  var Bg = c("ZodLiteral", (r, i) => {
    Ot.init(r, i), P.init(r, i), r._zod.processJSONSchema = (o, t, n) => A$(r, o, t, n), r.values = new Set(i.values), Object.defineProperty(r, "value", { get() {
      if (i.values.length > 1)
        throw Error("This schema contains multiple valid literal values. Use `.values` instead.");
      return i.values[0];
    } });
  });
  function vc(r, i) {
    return new Bg({ type: "literal", values: Array.isArray(r) ? r : [r], ...D.normalizeParams(i) });
  }
  var Hg = c("ZodFile", (r, i) => {
    St.init(r, i), P.init(r, i), r._zod.processJSONSchema = (o, t, n) => Y$(r, o, t, n), r.min = (o, t) => r.check(a(o, t)), r.max = (o, t) => r.check(gr(o, t)), r.mime = (o, t) => r.check(mr(Array.isArray(o) ? o : [o], t));
  });
  function oc(r) {
    return c$(Hg, r);
  }
  var Mg = c("ZodTransform", (r, i) => {
    zt.init(r, i), P.init(r, i), r._zod.processJSONSchema = (o, t, n) => F$(r, o, t, n), r._zod.parse = (o, t) => {
      if (t.direction === "backward")
        throw new Ir(r.constructor.name);
      o.addIssue = (v) => {
        if (typeof v === "string")
          o.issues.push(D.issue(v, o.value, i));
        else {
          let u = v;
          if (u.fatal)
            u.continue = false;
          u.code ?? (u.code = "custom"), u.input ?? (u.input = o.value), u.inst ?? (u.inst = r), o.issues.push(D.issue(u));
        }
      };
      let n = i.transform(o.value, o);
      if (n instanceof Promise)
        return n.then((v) => {
          return o.value = v, o;
        });
      return o.value = n, o;
    };
  });
  function cv(r) {
    return new Mg({ type: "transform", transform: r });
  }
  var Iv = c("ZodOptional", (r, i) => {
    ui.init(r, i), P.init(r, i), r._zod.processJSONSchema = (o, t, n) => Yi(r, o, t, n), r.unwrap = () => r._zod.def.innerType;
  });
  function Jn(r) {
    return new Iv({ type: "optional", innerType: r });
  }
  var Rg = c("ZodExactOptional", (r, i) => {
    Pt.init(r, i), P.init(r, i), r._zod.processJSONSchema = (o, t, n) => Yi(r, o, t, n), r.unwrap = () => r._zod.def.innerType;
  });
  function xg(r) {
    return new Rg({ type: "optional", innerType: r });
  }
  var Zg = c("ZodNullable", (r, i) => {
    jt.init(r, i), P.init(r, i), r._zod.processJSONSchema = (o, t, n) => C$(r, o, t, n), r.unwrap = () => r._zod.def.innerType;
  });
  function Ln(r) {
    return new Zg({ type: "nullable", innerType: r });
  }
  function tc(r) {
    return Jn(Ln(r));
  }
  var dg = c("ZodDefault", (r, i) => {
    Jt.init(r, i), P.init(r, i), r._zod.processJSONSchema = (o, t, n) => h$(r, o, t, n), r.unwrap = () => r._zod.def.innerType, r.removeDefault = r.unwrap;
  });
  function Cg(r, i) {
    return new dg({ type: "default", innerType: r, get defaultValue() {
      return typeof i === "function" ? i() : D.shallowClone(i);
    } });
  }
  var fg = c("ZodPrefault", (r, i) => {
    Lt.init(r, i), P.init(r, i), r._zod.processJSONSchema = (o, t, n) => y$(r, o, t, n), r.unwrap = () => r._zod.def.innerType;
  });
  function hg(r, i) {
    return new fg({ type: "prefault", innerType: r, get defaultValue() {
      return typeof i === "function" ? i() : D.shallowClone(i);
    } });
  }
  var bv = c("ZodNonOptional", (r, i) => {
    Et.init(r, i), P.init(r, i), r._zod.processJSONSchema = (o, t, n) => f$(r, o, t, n), r.unwrap = () => r._zod.def.innerType;
  });
  function yg(r, i) {
    return new bv({ type: "nonoptional", innerType: r, ...D.normalizeParams(i) });
  }
  var ag = c("ZodSuccess", (r, i) => {
    Gt.init(r, i), P.init(r, i), r._zod.processJSONSchema = (o, t, n) => Q$(r, o, t, n), r.unwrap = () => r._zod.def.innerType;
  });
  function uc(r) {
    return new ag({ type: "success", innerType: r });
  }
  var pg = c("ZodCatch", (r, i) => {
    Wt.init(r, i), P.init(r, i), r._zod.processJSONSchema = (o, t, n) => a$(r, o, t, n), r.unwrap = () => r._zod.def.innerType, r.removeCatch = r.unwrap;
  });
  function sg(r, i) {
    return new pg({ type: "catch", innerType: r, catchValue: typeof i === "function" ? i : () => i });
  }
  var re = c("ZodNaN", (r, i) => {
    Xt.init(r, i), P.init(r, i), r._zod.processJSONSchema = (o, t, n) => K$(r, o, t, n);
  });
  function $c(r) {
    return e$(re, r);
  }
  var _v = c("ZodPipe", (r, i) => {
    Vt.init(r, i), P.init(r, i), r._zod.processJSONSchema = (o, t, n) => p$(r, o, t, n), r.in = i.in, r.out = i.out;
  });
  function En(r, i) {
    return new _v({ type: "pipe", in: r, out: i });
  }
  var Uv = c("ZodCodec", (r, i) => {
    _v.init(r, i), Un.init(r, i);
  });
  function gc(r, i, o) {
    return new Uv({ type: "pipe", in: r, out: i, transform: o.decode, reverseTransform: o.encode });
  }
  var ne = c("ZodReadonly", (r, i) => {
    At.init(r, i), P.init(r, i), r._zod.processJSONSchema = (o, t, n) => s$(r, o, t, n), r.unwrap = () => r._zod.def.innerType;
  });
  function ie(r) {
    return new ne({ type: "readonly", innerType: r });
  }
  var ve = c("ZodTemplateLiteral", (r, i) => {
    Kt.init(r, i), P.init(r, i), r._zod.processJSONSchema = (o, t, n) => q$(r, o, t, n);
  });
  function ec(r, i) {
    return new ve({ type: "template_literal", parts: r, ...D.normalizeParams(i) });
  }
  var oe = c("ZodLazy", (r, i) => {
    Qt.init(r, i), P.init(r, i), r._zod.processJSONSchema = (o, t, n) => ng(r, o, t, n), r.unwrap = () => r._zod.def.getter();
  });
  function te(r) {
    return new oe({ type: "lazy", getter: r });
  }
  var ue = c("ZodPromise", (r, i) => {
    Yt.init(r, i), P.init(r, i), r._zod.processJSONSchema = (o, t, n) => rg(r, o, t, n), r.unwrap = () => r._zod.def.innerType;
  });
  function lc(r) {
    return new ue({ type: "promise", innerType: r });
  }
  var $e = c("ZodFunction", (r, i) => {
    qt.init(r, i), P.init(r, i), r._zod.processJSONSchema = (o, t, n) => T$(r, o, t, n);
  });
  function cc(r) {
    return new $e({ type: "function", input: Array.isArray(r?.input) ? Qg(r?.input) : r?.input ?? Xn(Nr()), output: r?.output ?? Nr() });
  }
  var qn = c("ZodCustom", (r, i) => {
    mt.init(r, i), P.init(r, i), r._zod.processJSONSchema = (o, t, n) => m$(r, o, t, n);
  });
  function Ic(r) {
    let i = new W({ check: "custom" });
    return i._zod.check = r, i;
  }
  function bc(r, i) {
    return I$(qn, r ?? (() => true), i);
  }
  function ge(r, i = {}) {
    return b$(qn, r, i);
  }
  function ee(r) {
    return _$(r);
  }
  var _c = U$;
  var Uc = k$;
  function kc(r, i = {}) {
    let o = new qn({ type: "custom", check: "custom", fn: (t) => t instanceof r, abort: true, ...D.normalizeParams(i) });
    return o._zod.bag.Class = r, o._zod.check = (t) => {
      if (!(t.value instanceof r))
        t.issues.push({ code: "invalid_type", expected: r.name, input: t.value, inst: o, path: [...o._zod.def.path ?? []] });
    }, o;
  }
  var Dc = (...r) => D$({ Codec: Uv, Boolean: yr, String: Cr }, ...r);
  function wc(r) {
    let i = te(() => {
      return ev([Mi(r), Og(), Sg(), Jg(), Xn(i), mg(Mi(), i)]);
    });
    return i;
  }
  function Nc(r, i) {
    return En(cv(r), i);
  }
  var j6 = { invalid_type: "invalid_type", too_big: "too_big", too_small: "too_small", invalid_format: "invalid_format", not_multiple_of: "not_multiple_of", unrecognized_keys: "unrecognized_keys", invalid_union: "invalid_union", invalid_key: "invalid_key", invalid_element: "invalid_element", invalid_value: "invalid_value", custom: "custom" };
  function J6(r) {
    V({ customError: r });
  }
  function L6() {
    return V().customError;
  }
  var le;
  /* @__PURE__ */ (function(r) {
  })(le || (le = {}));
  var S = { ...Pn, ...mi, iso: Zr };
  var E6 = /* @__PURE__ */ new Set(["$schema", "$ref", "$defs", "definitions", "$id", "id", "$comment", "$anchor", "$vocabulary", "$dynamicRef", "$dynamicAnchor", "type", "enum", "const", "anyOf", "oneOf", "allOf", "not", "properties", "required", "additionalProperties", "patternProperties", "propertyNames", "minProperties", "maxProperties", "items", "prefixItems", "additionalItems", "minItems", "maxItems", "uniqueItems", "contains", "minContains", "maxContains", "minLength", "maxLength", "pattern", "format", "minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum", "multipleOf", "description", "default", "contentEncoding", "contentMediaType", "contentSchema", "unevaluatedItems", "unevaluatedProperties", "if", "then", "else", "dependentSchemas", "dependentRequired", "nullable", "readOnly"]);
  function G6(r, i) {
    let o = r.$schema;
    if (o === "https://json-schema.org/draft/2020-12/schema")
      return "draft-2020-12";
    if (o === "http://json-schema.org/draft-07/schema#")
      return "draft-7";
    if (o === "http://json-schema.org/draft-04/schema#")
      return "draft-4";
    return i ?? "draft-2020-12";
  }
  function W6(r, i) {
    if (!r.startsWith("#"))
      throw Error("External $ref is not supported, only local refs (#/...) are allowed");
    let o = r.slice(1).split("/").filter(Boolean);
    if (o.length === 0)
      return i.rootSchema;
    let t = i.version === "draft-2020-12" ? "$defs" : "definitions";
    if (o[0] === t) {
      let n = o[1];
      if (!n || !i.defs[n])
        throw Error(`Reference not found: ${r}`);
      return i.defs[n];
    }
    throw Error(`Reference not found: ${r}`);
  }
  function Oc(r, i) {
    if (r.not !== void 0) {
      if (typeof r.not === "object" && Object.keys(r.not).length === 0)
        return S.never();
      throw Error("not is not supported in Zod (except { not: {} } for never)");
    }
    if (r.unevaluatedItems !== void 0)
      throw Error("unevaluatedItems is not supported");
    if (r.unevaluatedProperties !== void 0)
      throw Error("unevaluatedProperties is not supported");
    if (r.if !== void 0 || r.then !== void 0 || r.else !== void 0)
      throw Error("Conditional schemas (if/then/else) are not supported");
    if (r.dependentSchemas !== void 0 || r.dependentRequired !== void 0)
      throw Error("dependentSchemas and dependentRequired are not supported");
    if (r.$ref) {
      let n = r.$ref;
      if (i.refs.has(n))
        return i.refs.get(n);
      if (i.processing.has(n))
        return S.lazy(() => {
          if (!i.refs.has(n))
            throw Error(`Circular reference not resolved: ${n}`);
          return i.refs.get(n);
        });
      i.processing.add(n);
      let v = W6(n, i), u = K(v, i);
      return i.refs.set(n, u), i.processing.delete(n), u;
    }
    if (r.enum !== void 0) {
      let n = r.enum;
      if (i.version === "openapi-3.0" && r.nullable === true && n.length === 1 && n[0] === null)
        return S.null();
      if (n.length === 0)
        return S.never();
      if (n.length === 1)
        return S.literal(n[0]);
      if (n.every((u) => typeof u === "string"))
        return S.enum(n);
      let v = n.map((u) => S.literal(u));
      if (v.length < 2)
        return v[0];
      return S.union([v[0], v[1], ...v.slice(2)]);
    }
    if (r.const !== void 0)
      return S.literal(r.const);
    let o = r.type;
    if (Array.isArray(o)) {
      let n = o.map((v) => {
        let u = { ...r, type: v };
        return Oc(u, i);
      });
      if (n.length === 0)
        return S.never();
      if (n.length === 1)
        return n[0];
      return S.union(n);
    }
    if (!o)
      return S.any();
    let t;
    switch (o) {
      case "string": {
        let n = S.string();
        if (r.format) {
          let v = r.format;
          if (v === "email")
            n = n.check(S.email());
          else if (v === "uri" || v === "uri-reference")
            n = n.check(S.url());
          else if (v === "uuid" || v === "guid")
            n = n.check(S.uuid());
          else if (v === "date-time")
            n = n.check(S.iso.datetime());
          else if (v === "date")
            n = n.check(S.iso.date());
          else if (v === "time")
            n = n.check(S.iso.time());
          else if (v === "duration")
            n = n.check(S.iso.duration());
          else if (v === "ipv4")
            n = n.check(S.ipv4());
          else if (v === "ipv6")
            n = n.check(S.ipv6());
          else if (v === "mac")
            n = n.check(S.mac());
          else if (v === "cidr")
            n = n.check(S.cidrv4());
          else if (v === "cidr-v6")
            n = n.check(S.cidrv6());
          else if (v === "base64")
            n = n.check(S.base64());
          else if (v === "base64url")
            n = n.check(S.base64url());
          else if (v === "e164")
            n = n.check(S.e164());
          else if (v === "jwt")
            n = n.check(S.jwt());
          else if (v === "emoji")
            n = n.check(S.emoji());
          else if (v === "nanoid")
            n = n.check(S.nanoid());
          else if (v === "cuid")
            n = n.check(S.cuid());
          else if (v === "cuid2")
            n = n.check(S.cuid2());
          else if (v === "ulid")
            n = n.check(S.ulid());
          else if (v === "xid")
            n = n.check(S.xid());
          else if (v === "ksuid")
            n = n.check(S.ksuid());
        }
        if (typeof r.minLength === "number")
          n = n.min(r.minLength);
        if (typeof r.maxLength === "number")
          n = n.max(r.maxLength);
        if (r.pattern)
          n = n.regex(new RegExp(r.pattern));
        t = n;
        break;
      }
      case "number":
      case "integer": {
        let n = o === "integer" ? S.number().int() : S.number();
        if (typeof r.minimum === "number")
          n = n.min(r.minimum);
        if (typeof r.maximum === "number")
          n = n.max(r.maximum);
        if (typeof r.exclusiveMinimum === "number")
          n = n.gt(r.exclusiveMinimum);
        else if (r.exclusiveMinimum === true && typeof r.minimum === "number")
          n = n.gt(r.minimum);
        if (typeof r.exclusiveMaximum === "number")
          n = n.lt(r.exclusiveMaximum);
        else if (r.exclusiveMaximum === true && typeof r.maximum === "number")
          n = n.lt(r.maximum);
        if (typeof r.multipleOf === "number")
          n = n.multipleOf(r.multipleOf);
        t = n;
        break;
      }
      case "boolean": {
        t = S.boolean();
        break;
      }
      case "null": {
        t = S.null();
        break;
      }
      case "object": {
        let n = {}, v = r.properties || {}, u = new Set(r.required || []);
        for (let [l, e] of Object.entries(v)) {
          let I = K(e, i);
          n[l] = u.has(l) ? I : I.optional();
        }
        if (r.propertyNames) {
          let l = K(r.propertyNames, i), e = r.additionalProperties && typeof r.additionalProperties === "object" ? K(r.additionalProperties, i) : S.any();
          if (Object.keys(n).length === 0) {
            t = S.record(l, e);
            break;
          }
          let I = S.object(n).passthrough(), _ = S.looseRecord(l, e);
          t = S.intersection(I, _);
          break;
        }
        if (r.patternProperties) {
          let l = r.patternProperties, e = Object.keys(l), I = [];
          for (let N of e) {
            let O = K(l[N], i), J = S.string().regex(new RegExp(N));
            I.push(S.looseRecord(J, O));
          }
          let _ = [];
          if (Object.keys(n).length > 0)
            _.push(S.object(n).passthrough());
          if (_.push(...I), _.length === 0)
            t = S.object({}).passthrough();
          else if (_.length === 1)
            t = _[0];
          else {
            let N = S.intersection(_[0], _[1]);
            for (let O = 2; O < _.length; O++)
              N = S.intersection(N, _[O]);
            t = N;
          }
          break;
        }
        let $ = S.object(n);
        if (r.additionalProperties === false)
          t = $.strict();
        else if (typeof r.additionalProperties === "object")
          t = $.catchall(K(r.additionalProperties, i));
        else
          t = $.passthrough();
        break;
      }
      case "array": {
        let { prefixItems: n, items: v } = r;
        if (n && Array.isArray(n)) {
          let u = n.map((l) => K(l, i)), $ = v && typeof v === "object" && !Array.isArray(v) ? K(v, i) : void 0;
          if ($)
            t = S.tuple(u).rest($);
          else
            t = S.tuple(u);
          if (typeof r.minItems === "number")
            t = t.check(S.minLength(r.minItems));
          if (typeof r.maxItems === "number")
            t = t.check(S.maxLength(r.maxItems));
        } else if (Array.isArray(v)) {
          let u = v.map((l) => K(l, i)), $ = r.additionalItems && typeof r.additionalItems === "object" ? K(r.additionalItems, i) : void 0;
          if ($)
            t = S.tuple(u).rest($);
          else
            t = S.tuple(u);
          if (typeof r.minItems === "number")
            t = t.check(S.minLength(r.minItems));
          if (typeof r.maxItems === "number")
            t = t.check(S.maxLength(r.maxItems));
        } else if (v !== void 0) {
          let u = K(v, i), $ = S.array(u);
          if (typeof r.minItems === "number")
            $ = $.min(r.minItems);
          if (typeof r.maxItems === "number")
            $ = $.max(r.maxItems);
          t = $;
        } else
          t = S.array(S.any());
        break;
      }
      default:
        throw Error(`Unsupported type: ${o}`);
    }
    if (r.description)
      t = t.describe(r.description);
    if (r.default !== void 0)
      t = t.default(r.default);
    return t;
  }
  function K(r, i) {
    if (typeof r === "boolean")
      return r ? S.any() : S.never();
    let o = Oc(r, i), t = r.type || r.enum !== void 0 || r.const !== void 0;
    if (r.anyOf && Array.isArray(r.anyOf)) {
      let $ = r.anyOf.map((e) => K(e, i)), l = S.union($);
      o = t ? S.intersection(o, l) : l;
    }
    if (r.oneOf && Array.isArray(r.oneOf)) {
      let $ = r.oneOf.map((e) => K(e, i)), l = S.xor($);
      o = t ? S.intersection(o, l) : l;
    }
    if (r.allOf && Array.isArray(r.allOf))
      if (r.allOf.length === 0)
        o = t ? o : S.any();
      else {
        let $ = t ? o : K(r.allOf[0], i), l = t ? 0 : 1;
        for (let e = l; e < r.allOf.length; e++)
          $ = S.intersection($, K(r.allOf[e], i));
        o = $;
      }
    if (r.nullable === true && i.version === "openapi-3.0")
      o = S.nullable(o);
    if (r.readOnly === true)
      o = S.readonly(o);
    let n = {}, v = ["$id", "id", "$comment", "$anchor", "$vocabulary", "$dynamicRef", "$dynamicAnchor"];
    for (let $ of v)
      if ($ in r)
        n[$] = r[$];
    let u = ["contentEncoding", "contentMediaType", "contentSchema"];
    for (let $ of u)
      if ($ in r)
        n[$] = r[$];
    for (let $ of Object.keys(r))
      if (!E6.has($))
        n[$] = r[$];
    if (Object.keys(n).length > 0)
      i.registry.add(o, n);
    return o;
  }
  function Sc(r, i) {
    if (typeof r === "boolean")
      return r ? S.any() : S.never();
    let o = G6(r, i?.defaultTarget), t = r.$defs || r.definitions || {}, n = { version: o, defs: t, refs: /* @__PURE__ */ new Map(), processing: /* @__PURE__ */ new Set(), rootSchema: r, registry: i?.registry ?? A };
    return K(r, n);
  }
  var ce = {};
  s(ce, { string: () => X6, number: () => V6, date: () => q6, boolean: () => A6, bigint: () => K6 });
  function X6(r) {
    return qu(Cr, r);
  }
  function V6(r) {
    return Mu(hr, r);
  }
  function A6(r) {
    return hu(yr, r);
  }
  function K6(r) {
    return au(ar, r);
  }
  function q6(r) {
    return g$(Wn, r);
  }
  V(kn());
  var jc = g.union([g.literal("light"), g.literal("dark")]).describe("Color theme preference for the host environment.");
  var pr = g.union([g.literal("inline"), g.literal("fullscreen"), g.literal("pip")]).describe("Display mode for UI presentation.");
  var T6 = g.union([g.literal("--color-background-primary"), g.literal("--color-background-secondary"), g.literal("--color-background-tertiary"), g.literal("--color-background-inverse"), g.literal("--color-background-ghost"), g.literal("--color-background-info"), g.literal("--color-background-danger"), g.literal("--color-background-success"), g.literal("--color-background-warning"), g.literal("--color-background-disabled"), g.literal("--color-text-primary"), g.literal("--color-text-secondary"), g.literal("--color-text-tertiary"), g.literal("--color-text-inverse"), g.literal("--color-text-ghost"), g.literal("--color-text-info"), g.literal("--color-text-danger"), g.literal("--color-text-success"), g.literal("--color-text-warning"), g.literal("--color-text-disabled"), g.literal("--color-text-ghost"), g.literal("--color-border-primary"), g.literal("--color-border-secondary"), g.literal("--color-border-tertiary"), g.literal("--color-border-inverse"), g.literal("--color-border-ghost"), g.literal("--color-border-info"), g.literal("--color-border-danger"), g.literal("--color-border-success"), g.literal("--color-border-warning"), g.literal("--color-border-disabled"), g.literal("--color-ring-primary"), g.literal("--color-ring-secondary"), g.literal("--color-ring-inverse"), g.literal("--color-ring-info"), g.literal("--color-ring-danger"), g.literal("--color-ring-success"), g.literal("--color-ring-warning"), g.literal("--font-sans"), g.literal("--font-mono"), g.literal("--font-weight-normal"), g.literal("--font-weight-medium"), g.literal("--font-weight-semibold"), g.literal("--font-weight-bold"), g.literal("--font-text-xs-size"), g.literal("--font-text-sm-size"), g.literal("--font-text-md-size"), g.literal("--font-text-lg-size"), g.literal("--font-heading-xs-size"), g.literal("--font-heading-sm-size"), g.literal("--font-heading-md-size"), g.literal("--font-heading-lg-size"), g.literal("--font-heading-xl-size"), g.literal("--font-heading-2xl-size"), g.literal("--font-heading-3xl-size"), g.literal("--font-text-xs-line-height"), g.literal("--font-text-sm-line-height"), g.literal("--font-text-md-line-height"), g.literal("--font-text-lg-line-height"), g.literal("--font-heading-xs-line-height"), g.literal("--font-heading-sm-line-height"), g.literal("--font-heading-md-line-height"), g.literal("--font-heading-lg-line-height"), g.literal("--font-heading-xl-line-height"), g.literal("--font-heading-2xl-line-height"), g.literal("--font-heading-3xl-line-height"), g.literal("--border-radius-xs"), g.literal("--border-radius-sm"), g.literal("--border-radius-md"), g.literal("--border-radius-lg"), g.literal("--border-radius-xl"), g.literal("--border-radius-full"), g.literal("--border-width-regular"), g.literal("--shadow-hairline"), g.literal("--shadow-sm"), g.literal("--shadow-md"), g.literal("--shadow-lg")]).describe("CSS variable keys available to MCP apps for theming.");
  var F6 = g.record(T6.describe(`Style variables for theming MCP apps.

Individual style keys are optional - hosts may provide any subset of these values.
Values are strings containing CSS values (colors, sizes, font stacks, etc.).

Note: This type uses \`Record<K, string | undefined>\` rather than \`Partial<Record<K, string>>\`
for compatibility with Zod schema generation. Both are functionally equivalent for validation.`), g.union([g.string(), g.undefined()]).describe(`Style variables for theming MCP apps.

Individual style keys are optional - hosts may provide any subset of these values.
Values are strings containing CSS values (colors, sizes, font stacks, etc.).

Note: This type uses \`Record<K, string | undefined>\` rather than \`Partial<Record<K, string>>\`
for compatibility with Zod schema generation. Both are functionally equivalent for validation.`)).describe(`Style variables for theming MCP apps.

Individual style keys are optional - hosts may provide any subset of these values.
Values are strings containing CSS values (colors, sizes, font stacks, etc.).

Note: This type uses \`Record<K, string | undefined>\` rather than \`Partial<Record<K, string>>\`
for compatibility with Zod schema generation. Both are functionally equivalent for validation.`);
  var B6 = g.object({ method: g.literal("ui/open-link"), params: g.object({ url: g.string().describe("URL to open in the host's browser") }) });
  var be = g.object({ isError: g.boolean().optional().describe("True if the host failed to open the URL (e.g., due to security policy).") }).passthrough();
  var _e = g.object({ isError: g.boolean().optional().describe("True if the host rejected or failed to deliver the message.") }).passthrough();
  var H6 = g.object({ method: g.literal("ui/notifications/sandbox-proxy-ready"), params: g.object({}) });
  var kv = g.object({ connectDomains: g.array(g.string()).optional().describe("Origins for network requests (fetch/XHR/WebSocket)."), resourceDomains: g.array(g.string()).optional().describe("Origins for static resources (scripts, images, styles, fonts)."), frameDomains: g.array(g.string()).optional().describe("Origins for nested iframes (frame-src directive)."), baseUriDomains: g.array(g.string()).optional().describe("Allowed base URIs for the document (base-uri directive).") });
  var Dv = g.object({ camera: g.object({}).optional().describe("Request camera access (Permission Policy `camera` feature)."), microphone: g.object({}).optional().describe("Request microphone access (Permission Policy `microphone` feature)."), geolocation: g.object({}).optional().describe("Request geolocation access (Permission Policy `geolocation` feature)."), clipboardWrite: g.object({}).optional().describe("Request clipboard write access (Permission Policy `clipboard-write` feature).") });
  var M6 = g.object({ method: g.literal("ui/notifications/size-changed"), params: g.object({ width: g.number().optional().describe("New width in pixels."), height: g.number().optional().describe("New height in pixels.") }) });
  var Ue = g.object({ method: g.literal("ui/notifications/tool-input"), params: g.object({ arguments: g.record(g.string(), g.unknown().describe("Complete tool call arguments as key-value pairs.")).optional().describe("Complete tool call arguments as key-value pairs.") }) });
  var ke = g.object({ method: g.literal("ui/notifications/tool-input-partial"), params: g.object({ arguments: g.record(g.string(), g.unknown().describe("Partial tool call arguments (incomplete, may change).")).optional().describe("Partial tool call arguments (incomplete, may change).") }) });
  var De = g.object({ method: g.literal("ui/notifications/tool-cancelled"), params: g.object({ reason: g.string().optional().describe('Optional reason for the cancellation (e.g., "user action", "timeout").') }) });
  var Jc = g.object({ fonts: g.string().optional() });
  var Lc = g.object({ variables: F6.optional().describe("CSS variables for theming the app."), css: Jc.optional().describe("CSS blocks that apps can inject.") });
  var we = g.object({ method: g.literal("ui/resource-teardown"), params: g.object({}) });
  var R6 = g.record(g.string(), g.unknown());
  var Ie = g.object({ text: g.object({}).optional().describe("Host supports text content blocks."), image: g.object({}).optional().describe("Host supports image content blocks."), audio: g.object({}).optional().describe("Host supports audio content blocks."), resource: g.object({}).optional().describe("Host supports resource content blocks."), resourceLink: g.object({}).optional().describe("Host supports resource link content blocks."), structuredContent: g.object({}).optional().describe("Host supports structured content.") });
  var Ec = g.object({ experimental: g.object({}).optional().describe("Experimental features (structure TBD)."), openLinks: g.object({}).optional().describe("Host supports opening external URLs."), serverTools: g.object({ listChanged: g.boolean().optional().describe("Host supports tools/list_changed notifications.") }).optional().describe("Host can proxy tool calls to the MCP server."), serverResources: g.object({ listChanged: g.boolean().optional().describe("Host supports resources/list_changed notifications.") }).optional().describe("Host can proxy resource reads to the MCP server."), logging: g.object({}).optional().describe("Host accepts log messages."), sandbox: g.object({ permissions: Dv.optional().describe("Permissions granted by the host (camera, microphone, geolocation)."), csp: kv.optional().describe("CSP domains approved by the host.") }).optional().describe("Sandbox configuration applied by the host."), updateModelContext: Ie.optional().describe("Host accepts context updates (ui/update-model-context) to be included in the model's context for future turns."), message: Ie.optional().describe("Host supports receiving content messages (ui/message) from the view.") });
  var Gc = g.object({ experimental: g.object({}).optional().describe("Experimental features (structure TBD)."), tools: g.object({ listChanged: g.boolean().optional().describe("App supports tools/list_changed notifications.") }).optional().describe("App exposes MCP-style tools that the host can call."), availableDisplayModes: g.array(pr).optional().describe("Display modes the app supports.") });
  var x6 = g.object({ method: g.literal("ui/notifications/initialized"), params: g.object({}).optional() });
  var Z6 = g.object({ csp: kv.optional().describe("Content Security Policy configuration."), permissions: Dv.optional().describe("Sandbox permissions requested by the UI."), domain: g.string().optional().describe("Dedicated origin for view sandbox."), prefersBorder: g.boolean().optional().describe("Visual boundary preference - true if UI prefers a visible border.") });
  var d6 = g.object({ method: g.literal("ui/request-display-mode"), params: g.object({ mode: pr.describe("The display mode being requested.") }) });
  var Ne = g.object({ mode: pr.describe("The display mode that was actually set. May differ from requested if not supported.") }).passthrough();
  var Wc = g.union([g.literal("model"), g.literal("app")]).describe("Tool visibility scope - who can access the tool.");
  var C6 = g.object({ resourceUri: g.string().optional(), visibility: g.array(Wc).optional().describe(`Who can access this tool. Default: ["model", "app"]
- "model": Tool visible to and callable by the agent
- "app": Tool callable by the app from this server only`) });
  var uk = g.object({ mimeTypes: g.array(g.string()).optional().describe('Array of supported MIME types for UI resources.\nMust include `"text/html;profile=mcp-app"` for MCP Apps support.') });
  var f6 = g.object({ method: g.literal("ui/message"), params: g.object({ role: g.literal("user").describe('Message role, currently only "user" is supported.'), content: g.array(ContentBlockSchema).describe("Message content blocks (text, image, etc.).") }) });
  var h6 = g.object({ method: g.literal("ui/notifications/sandbox-resource-ready"), params: g.object({ html: g.string().describe("HTML content to load into the inner iframe."), sandbox: g.string().optional().describe("Optional override for the inner iframe's sandbox attribute."), csp: kv.optional().describe("CSP configuration from resource metadata."), permissions: Dv.optional().describe("Sandbox permissions from resource metadata.") }) });
  var Oe = g.object({ method: g.literal("ui/notifications/tool-result"), params: CallToolResultSchema.describe("Standard MCP tool execution result.") });
  var Se = g.object({ toolInfo: g.object({ id: RequestIdSchema.optional().describe("JSON-RPC id of the tools/call request."), tool: ToolSchema.describe("Tool definition including name, inputSchema, etc.") }).optional().describe("Metadata of the tool call that instantiated this App."), theme: jc.optional().describe("Current color theme preference."), styles: Lc.optional().describe("Style configuration for theming the app."), displayMode: pr.optional().describe("How the UI is currently displayed."), availableDisplayModes: g.array(pr).optional().describe("Display modes the host supports."), containerDimensions: g.union([g.object({ height: g.number().describe("Fixed container height in pixels.") }), g.object({ maxHeight: g.union([g.number(), g.undefined()]).optional().describe("Maximum container height in pixels.") })]).and(g.union([g.object({ width: g.number().describe("Fixed container width in pixels.") }), g.object({ maxWidth: g.union([g.number(), g.undefined()]).optional().describe("Maximum container width in pixels.") })])).optional().describe(`Container dimensions. Represents the dimensions of the iframe or other
container holding the app. Specify either width or maxWidth, and either height or maxHeight.`), locale: g.string().optional().describe("User's language and region preference in BCP 47 format."), timeZone: g.string().optional().describe("User's timezone in IANA format."), userAgent: g.string().optional().describe("Host application identifier."), platform: g.union([g.literal("web"), g.literal("desktop"), g.literal("mobile")]).optional().describe("Platform type for responsive design decisions."), deviceCapabilities: g.object({ touch: g.boolean().optional().describe("Whether the device supports touch input."), hover: g.boolean().optional().describe("Whether the device supports hover interactions.") }).optional().describe("Device input capabilities."), safeAreaInsets: g.object({ top: g.number().describe("Top safe area inset in pixels."), right: g.number().describe("Right safe area inset in pixels."), bottom: g.number().describe("Bottom safe area inset in pixels."), left: g.number().describe("Left safe area inset in pixels.") }).optional().describe("Mobile safe area boundaries in pixels.") }).passthrough();
  var ze = g.object({ method: g.literal("ui/notifications/host-context-changed"), params: Se.describe("Partial context update containing only changed fields.") });
  var y6 = g.object({ method: g.literal("ui/update-model-context"), params: g.object({ content: g.array(ContentBlockSchema).optional().describe("Context content blocks (text, image, etc.)."), structuredContent: g.record(g.string(), g.unknown().describe("Structured content for machine-readable context data.")).optional().describe("Structured content for machine-readable context data.") }) });
  var a6 = g.object({ method: g.literal("ui/initialize"), params: g.object({ appInfo: ImplementationSchema.describe("App identification (name and version)."), appCapabilities: Gc.describe("Features and capabilities this app provides."), protocolVersion: g.string().describe("Protocol version this app supports.") }) });
  var Pe = g.object({ protocolVersion: g.string().describe('Negotiated protocol version string (e.g., "2025-11-21").'), hostInfo: ImplementationSchema.describe("Host application identification and version."), hostCapabilities: Ec.describe("Features and capabilities provided by the host."), hostContext: Se.describe("Rich context about the host environment.") }).passthrough();
  function s6(r) {
    let i = document.documentElement;
    i.setAttribute("data-theme", r), i.style.colorScheme = r;
  }
  function rb(r, i = document.documentElement) {
    for (let [o, t] of Object.entries(r))
      if (t !== void 0)
        i.style.setProperty(o, t);
  }
  var gb = class extends Protocol {
    _appInfo;
    _capabilities;
    options;
    _hostCapabilities;
    _hostInfo;
    _hostContext;
    constructor(r, i = {}, o = { autoResize: true }) {
      super(o);
      this._appInfo = r;
      this._capabilities = i;
      this.options = o;
      this.setRequestHandler(PingRequestSchema, (t) => {
        return console.log("Received ping:", t.params), {};
      }), this.onhostcontextchanged = () => {
      };
    }
    getHostCapabilities() {
      return this._hostCapabilities;
    }
    getHostVersion() {
      return this._hostInfo;
    }
    getHostContext() {
      return this._hostContext;
    }
    set ontoolinput(r) {
      this.setNotificationHandler(Ue, (i) => r(i.params));
    }
    set ontoolinputpartial(r) {
      this.setNotificationHandler(ke, (i) => r(i.params));
    }
    set ontoolresult(r) {
      this.setNotificationHandler(Oe, (i) => r(i.params));
    }
    set ontoolcancelled(r) {
      this.setNotificationHandler(De, (i) => r(i.params));
    }
    set onhostcontextchanged(r) {
      this.setNotificationHandler(ze, (i) => {
        this._hostContext = { ...this._hostContext, ...i.params }, r(i.params);
      });
    }
    set onteardown(r) {
      this.setRequestHandler(we, (i, o) => r(i.params, o));
    }
    set oncalltool(r) {
      this.setRequestHandler(CallToolRequestSchema, (i, o) => r(i.params, o));
    }
    set onlisttools(r) {
      this.setRequestHandler(ListToolsRequestSchema, (i, o) => r(i.params, o));
    }
    assertCapabilityForMethod(r) {
    }
    assertRequestHandlerCapability(r) {
      switch (r) {
        case "tools/call":
        case "tools/list":
          if (!this._capabilities.tools)
            throw Error(`Client does not support tool capability (required for ${r})`);
          return;
        case "ping":
        case "ui/resource-teardown":
          return;
        default:
          throw Error(`No handler for method ${r} registered`);
      }
    }
    assertNotificationCapability(r) {
    }
    assertTaskCapability(r) {
      throw Error("Tasks are not supported in MCP Apps");
    }
    assertTaskHandlerCapability(r) {
      throw Error("Task handlers are not supported in MCP Apps");
    }
    async callServerTool(r, i) {
      return await this.request({ method: "tools/call", params: r }, CallToolResultSchema, i);
    }
    sendMessage(r, i) {
      return this.request({ method: "ui/message", params: r }, _e, i);
    }
    sendLog(r) {
      return this.notification({ method: "notifications/message", params: r });
    }
    updateModelContext(r, i) {
      return this.request({ method: "ui/update-model-context", params: r }, EmptyResultSchema, i);
    }
    openLink(r, i) {
      return this.request({ method: "ui/open-link", params: r }, be, i);
    }
    sendOpenLink = this.openLink;
    requestDisplayMode(r, i) {
      return this.request({ method: "ui/request-display-mode", params: r }, Ne, i);
    }
    sendSizeChanged(r) {
      return this.notification({ method: "ui/notifications/size-changed", params: r });
    }
    setupSizeChangedNotifications() {
      let r = false, i = 0, o = 0, t = () => {
        if (r)
          return;
        r = true, requestAnimationFrame(() => {
          r = false;
          let v = document.documentElement, u = v.style.width, $ = v.style.height;
          v.style.width = "fit-content", v.style.height = "fit-content";
          let l = v.getBoundingClientRect();
          v.style.width = u, v.style.height = $;
          let e = window.innerWidth - v.clientWidth, I = Math.ceil(l.width + e), _ = Math.ceil(l.height);
          if (I !== i || _ !== o)
            i = I, o = _, this.sendSizeChanged({ width: I, height: _ });
        });
      };
      t();
      let n = new ResizeObserver(t);
      return n.observe(document.documentElement), n.observe(document.body), () => n.disconnect();
    }
    async connect(r = new Yn(window.parent, window.parent), i) {
      await super.connect(r);
      try {
        let o = await this.request({ method: "ui/initialize", params: { appCapabilities: this._capabilities, appInfo: this._appInfo, protocolVersion: wv } }, Pe, i);
        if (o === void 0)
          throw Error(`Server sent invalid initialize result: ${o}`);
        if (this._hostCapabilities = o.hostCapabilities, this._hostInfo = o.hostInfo, this._hostContext = o.hostContext, await this.notification({ method: "ui/notifications/initialized" }), this.options?.autoResize)
          this.setupSizeChangedNotifications();
      } catch (o) {
        throw this.close(), o;
      }
    }
  };

  // src/app/app.ts
  var app = new gb(
    {
      name: "Iceberg Query Results",
      version: "1.0.0"
    },
    {}
  );
  app.ontoolresult = (params) => {
    try {
      const data = JSON.parse(params.content[0].text);
      renderResults(data);
    } catch (e) {
      showError(`Failed to parse results: ${e}`);
    }
  };
  app.onhostcontextchanged = (params) => {
    if (params.theme)
      s6(params.theme);
    if (params.styles?.variables)
      rb(params.styles.variables);
  };
  function renderResults(data) {
    const container = document.getElementById("results");
    const headerRow = data.schema.fields.map((f2) => `<th>${f2.name}<br><small>${f2.type}</small></th>`).join("");
    const dataRows = data.rows.map(
      (row) => `<tr>${row.map((cell) => `<td>${cell ?? "NULL"}</td>`).join("")}</tr>`
    ).join("");
    container.innerHTML = `
    <div class="metrics-bar">
      <span>Rows: ${data.row_count.toLocaleString()}</span>
      <span>Execution: ${data.metrics.total_ms}ms</span>
      <span>Memory: ${formatBytes(data.metrics.peak_memory_bytes)}</span>
    </div>
    <div class="metrics-breakdown">
      <div class="metric-item">
        <label>Parse</label>
        <span>${data.metrics.parse_ms}ms</span>
      </div>
      <div class="metric-item">
        <label>Plan</label>
        <span>${data.metrics.plan_ms}ms</span>
      </div>
      <div class="metric-item">
        <label>Optimize</label>
        <span>${data.metrics.optimize_ms}ms</span>
      </div>
      <div class="metric-item">
        <label>Execute</label>
        <span>${data.metrics.execute_ms}ms</span>
      </div>
    </div>
    <table class="results-table">
      <thead><tr>${headerRow}</tr></thead>
      <tbody>${dataRows}</tbody>
    </table>
  `;
  }
  function formatBytes(bytes) {
    if (bytes < 1024)
      return `${bytes} B`;
    if (bytes < 1024 * 1024)
      return `${(bytes / 1024).toFixed(1)} KB`;
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  }
  function showError(message) {
    document.getElementById("results").innerHTML = `
    <div class="error">${message}</div>
  `;
  }
  (async () => {
    await app.connect(new Yn(window.parent));
  })();
})();
