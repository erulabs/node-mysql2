'use strict';

const { createLRU } = require('lru.min');

const MAX_PARSER_CACHE_SIZE =
  parseInt(process.env.MYSQL2_MAX_PARSER_CACHE_SIZE, 10) || 15000;

const parserCache = createLRU({
  max: MAX_PARSER_CACHE_SIZE,
});

function keyFromFields(type, fields, options, config) {
  // Build cache key efficiently using array join (faster than string concatenation)
  // Pre-allocate array with exact size to avoid resizing
  const parts = new Array(10 + fields.length * 7);
  let idx = 0;

  // Add configuration options
  parts[idx++] = type;
  parts[idx++] = typeof options.nestTables;
  parts[idx++] = options.nestTables;
  parts[idx++] = Boolean(options.rowsAsArray);
  parts[idx++] = Boolean(options.supportBigNumbers || config.supportBigNumbers);
  parts[idx++] = Boolean(options.bigNumberStrings || config.bigNumberStrings);
  parts[idx++] =
    typeof options.typeCast === 'boolean'
      ? options.typeCast
      : typeof options.typeCast;
  parts[idx++] = options.timezone || config.timezone;
  parts[idx++] = Boolean(options.decimalNumbers);
  parts[idx++] = options.dateStrings;

  // Add field characteristics (accessing field properties triggers lazy getters)
  for (let i = 0; i < fields.length; ++i) {
    const field = fields[i];
    parts[idx++] = field.name;
    parts[idx++] = field.columnType;
    parts[idx++] = field.length;
    parts[idx++] = field.schema;
    parts[idx++] = field.table;
    parts[idx++] = field.flags;
    parts[idx++] = field.characterSet;
  }

  return parts.join('|');
}

function getParser(type, fields, options, config, compiler) {
  const key = keyFromFields(type, fields, options, config);
  let parser = parserCache.get(key);

  if (parser) {
    return parser;
  }

  parser = compiler(fields, options, config);
  parserCache.set(key, parser);
  return parser;
}

function setMaxCache(max) {
  parserCache.resize(max);
}

function clearCache() {
  parserCache.clear();
}

module.exports = {
  getParser: getParser,
  setMaxCache: setMaxCache,
  clearCache: clearCache,
  _keyFromFields: keyFromFields,
};
