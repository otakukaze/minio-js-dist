"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.calculateEvenSplits = calculateEvenSplits;
exports.extractMetadata = extractMetadata;
exports.getContentLength = getContentLength;
exports.getEncryptionHeaders = getEncryptionHeaders;
exports.getScope = getScope;
exports.getSourceVersionId = getSourceVersionId;
exports.getVersionId = getVersionId;
exports.hashBinary = hashBinary;
exports.insertContentType = insertContentType;
exports.isAmazonEndpoint = isAmazonEndpoint;
exports.isAmzHeader = isAmzHeader;
exports.isBoolean = isBoolean;
exports.isDefined = isDefined;
exports.isEmpty = isEmpty;
exports.isEmptyObject = isEmptyObject;
exports.isFunction = isFunction;
exports.isNumber = isNumber;
exports.isObject = isObject;
exports.isPlainObject = isPlainObject;
exports.isReadableStream = isReadableStream;
exports.isStorageClassHeader = isStorageClassHeader;
exports.isString = isString;
exports.isSupportedHeader = isSupportedHeader;
exports.isValidBucketName = isValidBucketName;
exports.isValidDate = isValidDate;
exports.isValidDomain = isValidDomain;
exports.isValidEndpoint = isValidEndpoint;
exports.isValidIP = isValidIP;
exports.isValidObjectName = isValidObjectName;
exports.isValidPort = isValidPort;
exports.isValidPrefix = isValidPrefix;
exports.isVirtualHostStyle = isVirtualHostStyle;
exports.makeDateLong = makeDateLong;
exports.makeDateShort = makeDateShort;
exports.parseXml = parseXml;
exports.partsRequired = partsRequired;
exports.pipesetup = pipesetup;
exports.prependXAMZMeta = prependXAMZMeta;
exports.probeContentType = probeContentType;
exports.readableStream = readableStream;
exports.sanitizeETag = sanitizeETag;
exports.sanitizeObjectKey = sanitizeObjectKey;
exports.sanitizeSize = sanitizeSize;
exports.toArray = toArray;
exports.toMd5 = toMd5;
exports.toSha256 = toSha256;
exports.uriEscape = uriEscape;
exports.uriResourceEscape = uriResourceEscape;
var crypto = _interopRequireWildcard(require("crypto"), true);
var stream = _interopRequireWildcard(require("stream"), true);
var _fastXmlParser = require("fast-xml-parser");
var _ipaddr = require("ipaddr.js");
var _lodash = require("lodash");
var mime = _interopRequireWildcard(require("mime-types"), true);
var _async = require("./async.js");
var _type = require("./type.js");
function _interopRequireWildcard(e, t) { if ("function" == typeof WeakMap) var r = new WeakMap(), n = new WeakMap(); return (_interopRequireWildcard = function (e, t) { if (!t && e && e.__esModule) return e; var o, i, f = { __proto__: null, default: e }; if (null === e || "object" != typeof e && "function" != typeof e) return f; if (o = t ? n : r) { if (o.has(e)) return o.get(e); o.set(e, f); } for (const t in e) "default" !== t && {}.hasOwnProperty.call(e, t) && ((i = (o = Object.defineProperty) && Object.getOwnPropertyDescriptor(e, t)) && (i.get || i.set) ? o(f, t, i) : f[t] = e[t]); return f; })(e, t); }
/*
 * MinIO Javascript Library for Amazon S3 Compatible Cloud Storage, (C) 2015 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

const MetaDataHeaderPrefix = 'x-amz-meta-';
function hashBinary(buf, enableSHA256) {
  let sha256sum = '';
  if (enableSHA256) {
    sha256sum = crypto.createHash('sha256').update(buf).digest('hex');
  }
  const md5sum = crypto.createHash('md5').update(buf).digest('base64');
  return {
    md5sum,
    sha256sum
  };
}

// S3 percent-encodes some extra non-standard characters in a URI . So comply with S3.
const encodeAsHex = c => `%${c.charCodeAt(0).toString(16).toUpperCase()}`;
function uriEscape(uriStr) {
  return encodeURIComponent(uriStr).replace(/[!'()*]/g, encodeAsHex);
}
function uriResourceEscape(string) {
  return uriEscape(string).replace(/%2F/g, '/');
}
function getScope(region, date, serviceName = 's3') {
  return `${makeDateShort(date)}/${region}/${serviceName}/aws4_request`;
}

/**
 * isAmazonEndpoint - true if endpoint is 's3.amazonaws.com' or 's3.cn-north-1.amazonaws.com.cn'
 */
function isAmazonEndpoint(endpoint) {
  return endpoint === 's3.amazonaws.com' || endpoint === 's3.cn-north-1.amazonaws.com.cn';
}

/**
 * isVirtualHostStyle - verify if bucket name is support with virtual
 * hosts. bucketNames with periods should be always treated as path
 * style if the protocol is 'https:', this is due to SSL wildcard
 * limitation. For all other buckets and Amazon S3 endpoint we will
 * default to virtual host style.
 */
function isVirtualHostStyle(endpoint, protocol, bucket, pathStyle) {
  if (protocol === 'https:' && bucket.includes('.')) {
    return false;
  }
  return isAmazonEndpoint(endpoint) || !pathStyle;
}
function isValidIP(ip) {
  return _ipaddr.isValid(ip);
}

/**
 * @returns if endpoint is valid domain.
 */
function isValidEndpoint(endpoint) {
  return isValidDomain(endpoint) || isValidIP(endpoint);
}

/**
 * @returns if input host is a valid domain.
 */
function isValidDomain(host) {
  if (!isString(host)) {
    return false;
  }
  // See RFC 1035, RFC 3696.
  if (host.length === 0 || host.length > 255) {
    return false;
  }
  // Host cannot start or end with a '-'
  if (host[0] === '-' || host.slice(-1) === '-') {
    return false;
  }
  // Host cannot start or end with a '_'
  if (host[0] === '_' || host.slice(-1) === '_') {
    return false;
  }
  // Host cannot start with a '.'
  if (host[0] === '.') {
    return false;
  }
  const nonAlphaNumerics = '`~!@#$%^&*()+={}[]|\\"\';:><?/';
  // All non alphanumeric characters are invalid.
  for (const char of nonAlphaNumerics) {
    if (host.includes(char)) {
      return false;
    }
  }
  // No need to regexp match, since the list is non-exhaustive.
  // We let it be valid and fail later.
  return true;
}

/**
 * Probes contentType using file extensions.
 *
 * @example
 * ```
 * // return 'image/png'
 * probeContentType('file.png')
 * ```
 */
function probeContentType(path) {
  let contentType = mime.lookup(path);
  if (!contentType) {
    contentType = 'application/octet-stream';
  }
  return contentType;
}

/**
 * is input port valid.
 */
function isValidPort(port) {
  // Convert string port to number if needed
  const portNum = typeof port === 'string' ? parseInt(port, 10) : port;

  // verify if port is a valid number
  if (!isNumber(portNum) || isNaN(portNum)) {
    return false;
  }

  // port `0` is valid and special case
  return 0 <= portNum && portNum <= 65535;
}
function isValidBucketName(bucket) {
  if (!isString(bucket)) {
    return false;
  }

  // bucket length should be less than and no more than 63
  // characters long.
  if (bucket.length < 3 || bucket.length > 63) {
    return false;
  }
  // bucket with successive periods is invalid.
  if (bucket.includes('..')) {
    return false;
  }
  // bucket cannot have ip address style.
  if (/[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+/.test(bucket)) {
    return false;
  }
  // bucket should begin with alphabet/number and end with alphabet/number,
  // with alphabet/number/.- in the middle.
  if (/^[a-z0-9][a-z0-9.-]+[a-z0-9]$/.test(bucket)) {
    return true;
  }
  return false;
}

/**
 * check if objectName is a valid object name
 */
function isValidObjectName(objectName) {
  if (!isValidPrefix(objectName)) {
    return false;
  }
  return objectName.length !== 0;
}

/**
 * check if prefix is valid
 */
function isValidPrefix(prefix) {
  if (!isString(prefix)) {
    return false;
  }
  if (prefix.length > 1024) {
    return false;
  }
  return true;
}

/**
 * check if typeof arg number
 */
function isNumber(arg) {
  return typeof arg === 'number';
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any

/**
 * check if typeof arg function
 */
function isFunction(arg) {
  return typeof arg === 'function';
}

/**
 * check if typeof arg string
 */
function isString(arg) {
  return typeof arg === 'string';
}

/**
 * check if typeof arg object
 */
function isObject(arg) {
  return typeof arg === 'object' && arg !== null;
}
/**
 * check if typeof arg is plain object
 */
function isPlainObject(arg) {
  return Object.prototype.toString.call(arg) === '[object Object]';
}
/**
 * check if object is readable stream
 */
function isReadableStream(arg) {
  // eslint-disable-next-line @typescript-eslint/unbound-method
  return isObject(arg) && isFunction(arg._read) && stream.isReadable(arg);
}

/**
 * check if arg is boolean
 */
function isBoolean(arg) {
  return typeof arg === 'boolean';
}
function isEmpty(o) {
  return _lodash.isEmpty(o);
}
function isEmptyObject(o) {
  return Object.values(o).filter(x => x !== undefined).length !== 0;
}
function isDefined(o) {
  return o !== null && o !== undefined;
}

/**
 * check if arg is a valid date
 */
function isValidDate(arg) {
  // @ts-expect-error checknew Date(Math.NaN)
  return arg instanceof Date && !isNaN(arg);
}

/**
 * Create a Date string with format: 'YYYYMMDDTHHmmss' + Z
 */
function makeDateLong(date) {
  date = date || new Date();

  // Gives format like: '2017-08-07T16:28:59.889Z'
  const s = date.toISOString();
  return s.slice(0, 4) + s.slice(5, 7) + s.slice(8, 13) + s.slice(14, 16) + s.slice(17, 19) + 'Z';
}

/**
 * Create a Date string with format: 'YYYYMMDD'
 */
function makeDateShort(date) {
  date = date || new Date();

  // Gives format like: '2017-08-07T16:28:59.889Z'
  const s = date.toISOString();
  return s.slice(0, 4) + s.slice(5, 7) + s.slice(8, 10);
}

/**
 * pipesetup sets up pipe() from left to right os streams array
 * pipesetup will also make sure that error emitted at any of the upstream Stream
 * will be emitted at the last stream. This makes error handling simple
 */
function pipesetup(...streams) {
  // @ts-expect-error ts can't narrow this
  return streams.reduce((src, dst) => {
    src.on('error', err => dst.emit('error', err));
    return src.pipe(dst);
  });
}

/**
 * return a Readable stream that emits data
 */
function readableStream(data) {
  const s = new stream.Readable();
  s._read = () => {};
  s.push(data);
  s.push(null);
  return s;
}

/**
 * Process metadata to insert appropriate value to `content-type` attribute
 */
function insertContentType(metaData, filePath) {
  // check if content-type attribute present in metaData
  for (const key in metaData) {
    if (key.toLowerCase() === 'content-type') {
      return metaData;
    }
  }

  // if `content-type` attribute is not present in metadata, then infer it from the extension in filePath
  return {
    ...metaData,
    'content-type': probeContentType(filePath)
  };
}

/**
 * Function prepends metadata with the appropriate prefix if it is not already on
 */
function prependXAMZMeta(metaData) {
  if (!metaData) {
    return {};
  }
  return _lodash.mapKeys(metaData, (value, key) => {
    if (isAmzHeader(key) || isSupportedHeader(key) || isStorageClassHeader(key)) {
      return key;
    }
    return MetaDataHeaderPrefix + key;
  });
}

/**
 * Checks if it is a valid header according to the AmazonS3 API
 */
function isAmzHeader(key) {
  const temp = key.toLowerCase();
  return temp.startsWith(MetaDataHeaderPrefix) || temp === 'x-amz-acl' || temp.startsWith('x-amz-server-side-encryption-') || temp === 'x-amz-server-side-encryption';
}

/**
 * Checks if it is a supported Header
 */
function isSupportedHeader(key) {
  const supported_headers = ['content-type', 'cache-control', 'content-encoding', 'content-disposition', 'content-language', 'x-amz-website-redirect-location', 'if-none-match', 'if-match'];
  return supported_headers.includes(key.toLowerCase());
}

/**
 * Checks if it is a storage header
 */
function isStorageClassHeader(key) {
  return key.toLowerCase() === 'x-amz-storage-class';
}
function extractMetadata(headers) {
  return _lodash.mapKeys(_lodash.pickBy(headers, (value, key) => isSupportedHeader(key) || isStorageClassHeader(key) || isAmzHeader(key)), (value, key) => {
    const lower = key.toLowerCase();
    if (lower.startsWith(MetaDataHeaderPrefix)) {
      return lower.slice(MetaDataHeaderPrefix.length);
    }
    return key;
  });
}
function getVersionId(headers = {}) {
  return headers['x-amz-version-id'] || null;
}
function getSourceVersionId(headers = {}) {
  return headers['x-amz-copy-source-version-id'] || null;
}
function sanitizeETag(etag = '') {
  const replaceChars = {
    '"': '',
    '&quot;': '',
    '&#34;': '',
    '&QUOT;': '',
    '&#x00022': ''
  };
  return etag.replace(/^("|&quot;|&#34;)|("|&quot;|&#34;)$/g, m => replaceChars[m]);
}
function toMd5(payload) {
  // use string from browser and buffer from nodejs
  // browser support is tested only against minio server
  return crypto.createHash('md5').update(Buffer.from(payload)).digest().toString('base64');
}
function toSha256(payload) {
  return crypto.createHash('sha256').update(payload).digest('hex');
}

/**
 * toArray returns a single element array with param being the element,
 * if param is just a string, and returns 'param' back if it is an array
 * So, it makes sure param is always an array
 */
function toArray(param) {
  if (!Array.isArray(param)) {
    return [param];
  }
  return param;
}
function sanitizeObjectKey(objectName) {
  // + symbol characters are not decoded as spaces in JS. so replace them first and decode to get the correct result.
  const asStrName = (objectName ? objectName.toString() : '').replace(/\+/g, ' ');
  return decodeURIComponent(asStrName);
}
function sanitizeSize(size) {
  return size ? Number.parseInt(size) : undefined;
}
const PART_CONSTRAINTS = exports.PART_CONSTRAINTS = {
  // absMinPartSize - absolute minimum part size (5 MiB)
  ABS_MIN_PART_SIZE: 1024 * 1024 * 5,
  // MIN_PART_SIZE - minimum part size 16MiB per object after which
  MIN_PART_SIZE: 1024 * 1024 * 16,
  // MAX_PARTS_COUNT - maximum number of parts for a single multipart session.
  MAX_PARTS_COUNT: 10000,
  // MAX_PART_SIZE - maximum part size 5GiB for a single multipart upload
  // operation.
  MAX_PART_SIZE: 1024 * 1024 * 1024 * 5,
  // MAX_SINGLE_PUT_OBJECT_SIZE - maximum size 5GiB of object per PUT
  // operation.
  MAX_SINGLE_PUT_OBJECT_SIZE: 1024 * 1024 * 1024 * 5,
  // MAX_MULTIPART_PUT_OBJECT_SIZE - maximum size 5TiB of object for
  // Multipart operation.
  MAX_MULTIPART_PUT_OBJECT_SIZE: 1024 * 1024 * 1024 * 1024 * 5
};
const GENERIC_SSE_HEADER = 'X-Amz-Server-Side-Encryption';
const ENCRYPTION_HEADERS = {
  // sseGenericHeader is the AWS SSE header used for SSE-S3 and SSE-KMS.
  sseGenericHeader: GENERIC_SSE_HEADER,
  // sseKmsKeyID is the AWS SSE-KMS key id.
  sseKmsKeyID: GENERIC_SSE_HEADER + '-Aws-Kms-Key-Id'
};

/**
 * Return Encryption headers
 * @param encConfig
 * @returns an object with key value pairs that can be used in headers.
 */
function getEncryptionHeaders(encConfig) {
  const encType = encConfig.type;
  if (!isEmpty(encType)) {
    if (encType === _type.ENCRYPTION_TYPES.SSEC) {
      return {
        [ENCRYPTION_HEADERS.sseGenericHeader]: 'AES256'
      };
    } else if (encType === _type.ENCRYPTION_TYPES.KMS) {
      return {
        [ENCRYPTION_HEADERS.sseGenericHeader]: encConfig.SSEAlgorithm,
        [ENCRYPTION_HEADERS.sseKmsKeyID]: encConfig.KMSMasterKeyID
      };
    }
  }
  return {};
}
function partsRequired(size) {
  const maxPartSize = PART_CONSTRAINTS.MAX_MULTIPART_PUT_OBJECT_SIZE / (PART_CONSTRAINTS.MAX_PARTS_COUNT - 1);
  let requiredPartSize = size / maxPartSize;
  if (size % maxPartSize > 0) {
    requiredPartSize++;
  }
  requiredPartSize = Math.trunc(requiredPartSize);
  return requiredPartSize;
}

/**
 * calculateEvenSplits - computes splits for a source and returns
 * start and end index slices. Splits happen evenly to be sure that no
 * part is less than 5MiB, as that could fail the multipart request if
 * it is not the last part.
 */
function calculateEvenSplits(size, objInfo) {
  if (size === 0) {
    return null;
  }
  const reqParts = partsRequired(size);
  const startIndexParts = [];
  const endIndexParts = [];
  let start = objInfo.Start;
  if (isEmpty(start) || start === -1) {
    start = 0;
  }
  const divisorValue = Math.trunc(size / reqParts);
  const reminderValue = size % reqParts;
  let nextStart = start;
  for (let i = 0; i < reqParts; i++) {
    let curPartSize = divisorValue;
    if (i < reminderValue) {
      curPartSize++;
    }
    const currentStart = nextStart;
    const currentEnd = currentStart + curPartSize - 1;
    nextStart = currentEnd + 1;
    startIndexParts.push(currentStart);
    endIndexParts.push(currentEnd);
  }
  return {
    startIndex: startIndexParts,
    endIndex: endIndexParts,
    objInfo: objInfo
  };
}
const fxp = new _fastXmlParser.XMLParser({
  numberParseOptions: {
    eNotation: false,
    hex: true,
    leadingZeros: true
  }
});

// eslint-disable-next-line @typescript-eslint/no-explicit-any
function parseXml(xml) {
  const result = fxp.parse(xml);
  if (result.Error) {
    throw result.Error;
  }
  return result;
}

/**
 * get content size of object content to upload
 */
async function getContentLength(s) {
  // use length property of string | Buffer
  if (typeof s === 'string' || Buffer.isBuffer(s)) {
    return s.length;
  }

  // property of `fs.ReadStream`
  const filePath = s.path;
  if (filePath && typeof filePath === 'string') {
    const stat = await _async.fsp.lstat(filePath);
    return stat.size;
  }

  // property of `fs.ReadStream`
  const fd = s.fd;
  if (fd && typeof fd === 'number') {
    const stat = await (0, _async.fstat)(fd);
    return stat.size;
  }
  return null;
}
//# sourceMappingURL=data:application/json;charset=utf-8;base64,eyJ2ZXJzaW9uIjozLCJuYW1lcyI6WyJjcnlwdG8iLCJfaW50ZXJvcFJlcXVpcmVXaWxkY2FyZCIsInJlcXVpcmUiLCJzdHJlYW0iLCJfZmFzdFhtbFBhcnNlciIsIl9pcGFkZHIiLCJfbG9kYXNoIiwibWltZSIsIl9hc3luYyIsIl90eXBlIiwiZSIsInQiLCJXZWFrTWFwIiwiciIsIm4iLCJfX2VzTW9kdWxlIiwibyIsImkiLCJmIiwiX19wcm90b19fIiwiZGVmYXVsdCIsImhhcyIsImdldCIsInNldCIsImhhc093blByb3BlcnR5IiwiY2FsbCIsIk9iamVjdCIsImRlZmluZVByb3BlcnR5IiwiZ2V0T3duUHJvcGVydHlEZXNjcmlwdG9yIiwiTWV0YURhdGFIZWFkZXJQcmVmaXgiLCJoYXNoQmluYXJ5IiwiYnVmIiwiZW5hYmxlU0hBMjU2Iiwic2hhMjU2c3VtIiwiY3JlYXRlSGFzaCIsInVwZGF0ZSIsImRpZ2VzdCIsIm1kNXN1bSIsImVuY29kZUFzSGV4IiwiYyIsImNoYXJDb2RlQXQiLCJ0b1N0cmluZyIsInRvVXBwZXJDYXNlIiwidXJpRXNjYXBlIiwidXJpU3RyIiwiZW5jb2RlVVJJQ29tcG9uZW50IiwicmVwbGFjZSIsInVyaVJlc291cmNlRXNjYXBlIiwic3RyaW5nIiwiZ2V0U2NvcGUiLCJyZWdpb24iLCJkYXRlIiwic2VydmljZU5hbWUiLCJtYWtlRGF0ZVNob3J0IiwiaXNBbWF6b25FbmRwb2ludCIsImVuZHBvaW50IiwiaXNWaXJ0dWFsSG9zdFN0eWxlIiwicHJvdG9jb2wiLCJidWNrZXQiLCJwYXRoU3R5bGUiLCJpbmNsdWRlcyIsImlzVmFsaWRJUCIsImlwIiwiaXBhZGRyIiwiaXNWYWxpZCIsImlzVmFsaWRFbmRwb2ludCIsImlzVmFsaWREb21haW4iLCJob3N0IiwiaXNTdHJpbmciLCJsZW5ndGgiLCJzbGljZSIsIm5vbkFscGhhTnVtZXJpY3MiLCJjaGFyIiwicHJvYmVDb250ZW50VHlwZSIsInBhdGgiLCJjb250ZW50VHlwZSIsImxvb2t1cCIsImlzVmFsaWRQb3J0IiwicG9ydCIsInBvcnROdW0iLCJwYXJzZUludCIsImlzTnVtYmVyIiwiaXNOYU4iLCJpc1ZhbGlkQnVja2V0TmFtZSIsInRlc3QiLCJpc1ZhbGlkT2JqZWN0TmFtZSIsIm9iamVjdE5hbWUiLCJpc1ZhbGlkUHJlZml4IiwicHJlZml4IiwiYXJnIiwiaXNGdW5jdGlvbiIsImlzT2JqZWN0IiwiaXNQbGFpbk9iamVjdCIsInByb3RvdHlwZSIsImlzUmVhZGFibGVTdHJlYW0iLCJfcmVhZCIsImlzUmVhZGFibGUiLCJpc0Jvb2xlYW4iLCJpc0VtcHR5IiwiXyIsImlzRW1wdHlPYmplY3QiLCJ2YWx1ZXMiLCJmaWx0ZXIiLCJ4IiwidW5kZWZpbmVkIiwiaXNEZWZpbmVkIiwiaXNWYWxpZERhdGUiLCJEYXRlIiwibWFrZURhdGVMb25nIiwicyIsInRvSVNPU3RyaW5nIiwicGlwZXNldHVwIiwic3RyZWFtcyIsInJlZHVjZSIsInNyYyIsImRzdCIsIm9uIiwiZXJyIiwiZW1pdCIsInBpcGUiLCJyZWFkYWJsZVN0cmVhbSIsImRhdGEiLCJSZWFkYWJsZSIsInB1c2giLCJpbnNlcnRDb250ZW50VHlwZSIsIm1ldGFEYXRhIiwiZmlsZVBhdGgiLCJrZXkiLCJ0b0xvd2VyQ2FzZSIsInByZXBlbmRYQU1aTWV0YSIsIm1hcEtleXMiLCJ2YWx1ZSIsImlzQW16SGVhZGVyIiwiaXNTdXBwb3J0ZWRIZWFkZXIiLCJpc1N0b3JhZ2VDbGFzc0hlYWRlciIsInRlbXAiLCJzdGFydHNXaXRoIiwic3VwcG9ydGVkX2hlYWRlcnMiLCJleHRyYWN0TWV0YWRhdGEiLCJoZWFkZXJzIiwicGlja0J5IiwibG93ZXIiLCJnZXRWZXJzaW9uSWQiLCJnZXRTb3VyY2VWZXJzaW9uSWQiLCJzYW5pdGl6ZUVUYWciLCJldGFnIiwicmVwbGFjZUNoYXJzIiwibSIsInRvTWQ1IiwicGF5bG9hZCIsIkJ1ZmZlciIsImZyb20iLCJ0b1NoYTI1NiIsInRvQXJyYXkiLCJwYXJhbSIsIkFycmF5IiwiaXNBcnJheSIsInNhbml0aXplT2JqZWN0S2V5IiwiYXNTdHJOYW1lIiwiZGVjb2RlVVJJQ29tcG9uZW50Iiwic2FuaXRpemVTaXplIiwic2l6ZSIsIk51bWJlciIsIlBBUlRfQ09OU1RSQUlOVFMiLCJleHBvcnRzIiwiQUJTX01JTl9QQVJUX1NJWkUiLCJNSU5fUEFSVF9TSVpFIiwiTUFYX1BBUlRTX0NPVU5UIiwiTUFYX1BBUlRfU0laRSIsIk1BWF9TSU5HTEVfUFVUX09CSkVDVF9TSVpFIiwiTUFYX01VTFRJUEFSVF9QVVRfT0JKRUNUX1NJWkUiLCJHRU5FUklDX1NTRV9IRUFERVIiLCJFTkNSWVBUSU9OX0hFQURFUlMiLCJzc2VHZW5lcmljSGVhZGVyIiwic3NlS21zS2V5SUQiLCJnZXRFbmNyeXB0aW9uSGVhZGVycyIsImVuY0NvbmZpZyIsImVuY1R5cGUiLCJ0eXBlIiwiRU5DUllQVElPTl9UWVBFUyIsIlNTRUMiLCJLTVMiLCJTU0VBbGdvcml0aG0iLCJLTVNNYXN0ZXJLZXlJRCIsInBhcnRzUmVxdWlyZWQiLCJtYXhQYXJ0U2l6ZSIsInJlcXVpcmVkUGFydFNpemUiLCJNYXRoIiwidHJ1bmMiLCJjYWxjdWxhdGVFdmVuU3BsaXRzIiwib2JqSW5mbyIsInJlcVBhcnRzIiwic3RhcnRJbmRleFBhcnRzIiwiZW5kSW5kZXhQYXJ0cyIsInN0YXJ0IiwiU3RhcnQiLCJkaXZpc29yVmFsdWUiLCJyZW1pbmRlclZhbHVlIiwibmV4dFN0YXJ0IiwiY3VyUGFydFNpemUiLCJjdXJyZW50U3RhcnQiLCJjdXJyZW50RW5kIiwic3RhcnRJbmRleCIsImVuZEluZGV4IiwiZnhwIiwiWE1MUGFyc2VyIiwibnVtYmVyUGFyc2VPcHRpb25zIiwiZU5vdGF0aW9uIiwiaGV4IiwibGVhZGluZ1plcm9zIiwicGFyc2VYbWwiLCJ4bWwiLCJyZXN1bHQiLCJwYXJzZSIsIkVycm9yIiwiZ2V0Q29udGVudExlbmd0aCIsImlzQnVmZmVyIiwic3RhdCIsImZzcCIsImxzdGF0IiwiZmQiLCJmc3RhdCJdLCJzb3VyY2VzIjpbImhlbHBlci50cyJdLCJzb3VyY2VzQ29udGVudCI6WyIvKlxuICogTWluSU8gSmF2YXNjcmlwdCBMaWJyYXJ5IGZvciBBbWF6b24gUzMgQ29tcGF0aWJsZSBDbG91ZCBTdG9yYWdlLCAoQykgMjAxNSBNaW5JTywgSW5jLlxuICpcbiAqIExpY2Vuc2VkIHVuZGVyIHRoZSBBcGFjaGUgTGljZW5zZSwgVmVyc2lvbiAyLjAgKHRoZSBcIkxpY2Vuc2VcIik7XG4gKiB5b3UgbWF5IG5vdCB1c2UgdGhpcyBmaWxlIGV4Y2VwdCBpbiBjb21wbGlhbmNlIHdpdGggdGhlIExpY2Vuc2UuXG4gKiBZb3UgbWF5IG9idGFpbiBhIGNvcHkgb2YgdGhlIExpY2Vuc2UgYXRcbiAqXG4gKiAgICAgaHR0cDovL3d3dy5hcGFjaGUub3JnL2xpY2Vuc2VzL0xJQ0VOU0UtMi4wXG4gKlxuICogVW5sZXNzIHJlcXVpcmVkIGJ5IGFwcGxpY2FibGUgbGF3IG9yIGFncmVlZCB0byBpbiB3cml0aW5nLCBzb2Z0d2FyZVxuICogZGlzdHJpYnV0ZWQgdW5kZXIgdGhlIExpY2Vuc2UgaXMgZGlzdHJpYnV0ZWQgb24gYW4gXCJBUyBJU1wiIEJBU0lTLFxuICogV0lUSE9VVCBXQVJSQU5USUVTIE9SIENPTkRJVElPTlMgT0YgQU5ZIEtJTkQsIGVpdGhlciBleHByZXNzIG9yIGltcGxpZWQuXG4gKiBTZWUgdGhlIExpY2Vuc2UgZm9yIHRoZSBzcGVjaWZpYyBsYW5ndWFnZSBnb3Zlcm5pbmcgcGVybWlzc2lvbnMgYW5kXG4gKiBsaW1pdGF0aW9ucyB1bmRlciB0aGUgTGljZW5zZS5cbiAqL1xuXG5pbXBvcnQgKiBhcyBjcnlwdG8gZnJvbSAnbm9kZTpjcnlwdG8nXG5pbXBvcnQgKiBhcyBzdHJlYW0gZnJvbSAnbm9kZTpzdHJlYW0nXG5cbmltcG9ydCB7IFhNTFBhcnNlciB9IGZyb20gJ2Zhc3QteG1sLXBhcnNlcidcbmltcG9ydCBpcGFkZHIgZnJvbSAnaXBhZGRyLmpzJ1xuaW1wb3J0IF8gZnJvbSAnbG9kYXNoJ1xuaW1wb3J0ICogYXMgbWltZSBmcm9tICdtaW1lLXR5cGVzJ1xuXG5pbXBvcnQgeyBmc3AsIGZzdGF0IH0gZnJvbSAnLi9hc3luYy50cydcbmltcG9ydCB0eXBlIHsgQmluYXJ5LCBFbmNyeXB0aW9uLCBPYmplY3RNZXRhRGF0YSwgUmVxdWVzdEhlYWRlcnMsIFJlc3BvbnNlSGVhZGVyIH0gZnJvbSAnLi90eXBlLnRzJ1xuaW1wb3J0IHsgRU5DUllQVElPTl9UWVBFUyB9IGZyb20gJy4vdHlwZS50cydcblxuY29uc3QgTWV0YURhdGFIZWFkZXJQcmVmaXggPSAneC1hbXotbWV0YS0nXG5cbmV4cG9ydCBmdW5jdGlvbiBoYXNoQmluYXJ5KGJ1ZjogQnVmZmVyLCBlbmFibGVTSEEyNTY6IGJvb2xlYW4pIHtcbiAgbGV0IHNoYTI1NnN1bSA9ICcnXG4gIGlmIChlbmFibGVTSEEyNTYpIHtcbiAgICBzaGEyNTZzdW0gPSBjcnlwdG8uY3JlYXRlSGFzaCgnc2hhMjU2JykudXBkYXRlKGJ1ZikuZGlnZXN0KCdoZXgnKVxuICB9XG4gIGNvbnN0IG1kNXN1bSA9IGNyeXB0by5jcmVhdGVIYXNoKCdtZDUnKS51cGRhdGUoYnVmKS5kaWdlc3QoJ2Jhc2U2NCcpXG5cbiAgcmV0dXJuIHsgbWQ1c3VtLCBzaGEyNTZzdW0gfVxufVxuXG4vLyBTMyBwZXJjZW50LWVuY29kZXMgc29tZSBleHRyYSBub24tc3RhbmRhcmQgY2hhcmFjdGVycyBpbiBhIFVSSSAuIFNvIGNvbXBseSB3aXRoIFMzLlxuY29uc3QgZW5jb2RlQXNIZXggPSAoYzogc3RyaW5nKSA9PiBgJSR7Yy5jaGFyQ29kZUF0KDApLnRvU3RyaW5nKDE2KS50b1VwcGVyQ2FzZSgpfWBcbmV4cG9ydCBmdW5jdGlvbiB1cmlFc2NhcGUodXJpU3RyOiBzdHJpbmcpOiBzdHJpbmcge1xuICByZXR1cm4gZW5jb2RlVVJJQ29tcG9uZW50KHVyaVN0cikucmVwbGFjZSgvWyEnKCkqXS9nLCBlbmNvZGVBc0hleClcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHVyaVJlc291cmNlRXNjYXBlKHN0cmluZzogc3RyaW5nKSB7XG4gIHJldHVybiB1cmlFc2NhcGUoc3RyaW5nKS5yZXBsYWNlKC8lMkYvZywgJy8nKVxufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0U2NvcGUocmVnaW9uOiBzdHJpbmcsIGRhdGU6IERhdGUsIHNlcnZpY2VOYW1lID0gJ3MzJykge1xuICByZXR1cm4gYCR7bWFrZURhdGVTaG9ydChkYXRlKX0vJHtyZWdpb259LyR7c2VydmljZU5hbWV9L2F3czRfcmVxdWVzdGBcbn1cblxuLyoqXG4gKiBpc0FtYXpvbkVuZHBvaW50IC0gdHJ1ZSBpZiBlbmRwb2ludCBpcyAnczMuYW1hem9uYXdzLmNvbScgb3IgJ3MzLmNuLW5vcnRoLTEuYW1hem9uYXdzLmNvbS5jbidcbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIGlzQW1hem9uRW5kcG9pbnQoZW5kcG9pbnQ6IHN0cmluZykge1xuICByZXR1cm4gZW5kcG9pbnQgPT09ICdzMy5hbWF6b25hd3MuY29tJyB8fCBlbmRwb2ludCA9PT0gJ3MzLmNuLW5vcnRoLTEuYW1hem9uYXdzLmNvbS5jbidcbn1cblxuLyoqXG4gKiBpc1ZpcnR1YWxIb3N0U3R5bGUgLSB2ZXJpZnkgaWYgYnVja2V0IG5hbWUgaXMgc3VwcG9ydCB3aXRoIHZpcnR1YWxcbiAqIGhvc3RzLiBidWNrZXROYW1lcyB3aXRoIHBlcmlvZHMgc2hvdWxkIGJlIGFsd2F5cyB0cmVhdGVkIGFzIHBhdGhcbiAqIHN0eWxlIGlmIHRoZSBwcm90b2NvbCBpcyAnaHR0cHM6JywgdGhpcyBpcyBkdWUgdG8gU1NMIHdpbGRjYXJkXG4gKiBsaW1pdGF0aW9uLiBGb3IgYWxsIG90aGVyIGJ1Y2tldHMgYW5kIEFtYXpvbiBTMyBlbmRwb2ludCB3ZSB3aWxsXG4gKiBkZWZhdWx0IHRvIHZpcnR1YWwgaG9zdCBzdHlsZS5cbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIGlzVmlydHVhbEhvc3RTdHlsZShlbmRwb2ludDogc3RyaW5nLCBwcm90b2NvbDogc3RyaW5nLCBidWNrZXQ6IHN0cmluZywgcGF0aFN0eWxlOiBib29sZWFuKSB7XG4gIGlmIChwcm90b2NvbCA9PT0gJ2h0dHBzOicgJiYgYnVja2V0LmluY2x1ZGVzKCcuJykpIHtcbiAgICByZXR1cm4gZmFsc2VcbiAgfVxuICByZXR1cm4gaXNBbWF6b25FbmRwb2ludChlbmRwb2ludCkgfHwgIXBhdGhTdHlsZVxufVxuXG5leHBvcnQgZnVuY3Rpb24gaXNWYWxpZElQKGlwOiBzdHJpbmcpIHtcbiAgcmV0dXJuIGlwYWRkci5pc1ZhbGlkKGlwKVxufVxuXG4vKipcbiAqIEByZXR1cm5zIGlmIGVuZHBvaW50IGlzIHZhbGlkIGRvbWFpbi5cbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIGlzVmFsaWRFbmRwb2ludChlbmRwb2ludDogc3RyaW5nKSB7XG4gIHJldHVybiBpc1ZhbGlkRG9tYWluKGVuZHBvaW50KSB8fCBpc1ZhbGlkSVAoZW5kcG9pbnQpXG59XG5cbi8qKlxuICogQHJldHVybnMgaWYgaW5wdXQgaG9zdCBpcyBhIHZhbGlkIGRvbWFpbi5cbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIGlzVmFsaWREb21haW4oaG9zdDogc3RyaW5nKSB7XG4gIGlmICghaXNTdHJpbmcoaG9zdCkpIHtcbiAgICByZXR1cm4gZmFsc2VcbiAgfVxuICAvLyBTZWUgUkZDIDEwMzUsIFJGQyAzNjk2LlxuICBpZiAoaG9zdC5sZW5ndGggPT09IDAgfHwgaG9zdC5sZW5ndGggPiAyNTUpIHtcbiAgICByZXR1cm4gZmFsc2VcbiAgfVxuICAvLyBIb3N0IGNhbm5vdCBzdGFydCBvciBlbmQgd2l0aCBhICctJ1xuICBpZiAoaG9zdFswXSA9PT0gJy0nIHx8IGhvc3Quc2xpY2UoLTEpID09PSAnLScpIHtcbiAgICByZXR1cm4gZmFsc2VcbiAgfVxuICAvLyBIb3N0IGNhbm5vdCBzdGFydCBvciBlbmQgd2l0aCBhICdfJ1xuICBpZiAoaG9zdFswXSA9PT0gJ18nIHx8IGhvc3Quc2xpY2UoLTEpID09PSAnXycpIHtcbiAgICByZXR1cm4gZmFsc2VcbiAgfVxuICAvLyBIb3N0IGNhbm5vdCBzdGFydCB3aXRoIGEgJy4nXG4gIGlmIChob3N0WzBdID09PSAnLicpIHtcbiAgICByZXR1cm4gZmFsc2VcbiAgfVxuXG4gIGNvbnN0IG5vbkFscGhhTnVtZXJpY3MgPSAnYH4hQCMkJV4mKigpKz17fVtdfFxcXFxcIlxcJzs6Pjw/LydcbiAgLy8gQWxsIG5vbiBhbHBoYW51bWVyaWMgY2hhcmFjdGVycyBhcmUgaW52YWxpZC5cbiAgZm9yIChjb25zdCBjaGFyIG9mIG5vbkFscGhhTnVtZXJpY3MpIHtcbiAgICBpZiAoaG9zdC5pbmNsdWRlcyhjaGFyKSkge1xuICAgICAgcmV0dXJuIGZhbHNlXG4gICAgfVxuICB9XG4gIC8vIE5vIG5lZWQgdG8gcmVnZXhwIG1hdGNoLCBzaW5jZSB0aGUgbGlzdCBpcyBub24tZXhoYXVzdGl2ZS5cbiAgLy8gV2UgbGV0IGl0IGJlIHZhbGlkIGFuZCBmYWlsIGxhdGVyLlxuICByZXR1cm4gdHJ1ZVxufVxuXG4vKipcbiAqIFByb2JlcyBjb250ZW50VHlwZSB1c2luZyBmaWxlIGV4dGVuc2lvbnMuXG4gKlxuICogQGV4YW1wbGVcbiAqIGBgYFxuICogLy8gcmV0dXJuICdpbWFnZS9wbmcnXG4gKiBwcm9iZUNvbnRlbnRUeXBlKCdmaWxlLnBuZycpXG4gKiBgYGBcbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIHByb2JlQ29udGVudFR5cGUocGF0aDogc3RyaW5nKSB7XG4gIGxldCBjb250ZW50VHlwZSA9IG1pbWUubG9va3VwKHBhdGgpXG4gIGlmICghY29udGVudFR5cGUpIHtcbiAgICBjb250ZW50VHlwZSA9ICdhcHBsaWNhdGlvbi9vY3RldC1zdHJlYW0nXG4gIH1cbiAgcmV0dXJuIGNvbnRlbnRUeXBlXG59XG5cbi8qKlxuICogaXMgaW5wdXQgcG9ydCB2YWxpZC5cbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIGlzVmFsaWRQb3J0KHBvcnQ6IHVua25vd24pOiBwb3J0IGlzIG51bWJlciB7XG4gIC8vIENvbnZlcnQgc3RyaW5nIHBvcnQgdG8gbnVtYmVyIGlmIG5lZWRlZFxuICBjb25zdCBwb3J0TnVtID0gdHlwZW9mIHBvcnQgPT09ICdzdHJpbmcnID8gcGFyc2VJbnQocG9ydCwgMTApIDogcG9ydFxuXG4gIC8vIHZlcmlmeSBpZiBwb3J0IGlzIGEgdmFsaWQgbnVtYmVyXG4gIGlmICghaXNOdW1iZXIocG9ydE51bSkgfHwgaXNOYU4ocG9ydE51bSkpIHtcbiAgICByZXR1cm4gZmFsc2VcbiAgfVxuXG4gIC8vIHBvcnQgYDBgIGlzIHZhbGlkIGFuZCBzcGVjaWFsIGNhc2VcbiAgcmV0dXJuIDAgPD0gcG9ydE51bSAmJiBwb3J0TnVtIDw9IDY1NTM1XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBpc1ZhbGlkQnVja2V0TmFtZShidWNrZXQ6IHVua25vd24pIHtcbiAgaWYgKCFpc1N0cmluZyhidWNrZXQpKSB7XG4gICAgcmV0dXJuIGZhbHNlXG4gIH1cblxuICAvLyBidWNrZXQgbGVuZ3RoIHNob3VsZCBiZSBsZXNzIHRoYW4gYW5kIG5vIG1vcmUgdGhhbiA2M1xuICAvLyBjaGFyYWN0ZXJzIGxvbmcuXG4gIGlmIChidWNrZXQubGVuZ3RoIDwgMyB8fCBidWNrZXQubGVuZ3RoID4gNjMpIHtcbiAgICByZXR1cm4gZmFsc2VcbiAgfVxuICAvLyBidWNrZXQgd2l0aCBzdWNjZXNzaXZlIHBlcmlvZHMgaXMgaW52YWxpZC5cbiAgaWYgKGJ1Y2tldC5pbmNsdWRlcygnLi4nKSkge1xuICAgIHJldHVybiBmYWxzZVxuICB9XG4gIC8vIGJ1Y2tldCBjYW5ub3QgaGF2ZSBpcCBhZGRyZXNzIHN0eWxlLlxuICBpZiAoL1swLTldK1xcLlswLTldK1xcLlswLTldK1xcLlswLTldKy8udGVzdChidWNrZXQpKSB7XG4gICAgcmV0dXJuIGZhbHNlXG4gIH1cbiAgLy8gYnVja2V0IHNob3VsZCBiZWdpbiB3aXRoIGFscGhhYmV0L251bWJlciBhbmQgZW5kIHdpdGggYWxwaGFiZXQvbnVtYmVyLFxuICAvLyB3aXRoIGFscGhhYmV0L251bWJlci8uLSBpbiB0aGUgbWlkZGxlLlxuICBpZiAoL15bYS16MC05XVthLXowLTkuLV0rW2EtejAtOV0kLy50ZXN0KGJ1Y2tldCkpIHtcbiAgICByZXR1cm4gdHJ1ZVxuICB9XG4gIHJldHVybiBmYWxzZVxufVxuXG4vKipcbiAqIGNoZWNrIGlmIG9iamVjdE5hbWUgaXMgYSB2YWxpZCBvYmplY3QgbmFtZVxuICovXG5leHBvcnQgZnVuY3Rpb24gaXNWYWxpZE9iamVjdE5hbWUob2JqZWN0TmFtZTogdW5rbm93bikge1xuICBpZiAoIWlzVmFsaWRQcmVmaXgob2JqZWN0TmFtZSkpIHtcbiAgICByZXR1cm4gZmFsc2VcbiAgfVxuXG4gIHJldHVybiBvYmplY3ROYW1lLmxlbmd0aCAhPT0gMFxufVxuXG4vKipcbiAqIGNoZWNrIGlmIHByZWZpeCBpcyB2YWxpZFxuICovXG5leHBvcnQgZnVuY3Rpb24gaXNWYWxpZFByZWZpeChwcmVmaXg6IHVua25vd24pOiBwcmVmaXggaXMgc3RyaW5nIHtcbiAgaWYgKCFpc1N0cmluZyhwcmVmaXgpKSB7XG4gICAgcmV0dXJuIGZhbHNlXG4gIH1cbiAgaWYgKHByZWZpeC5sZW5ndGggPiAxMDI0KSB7XG4gICAgcmV0dXJuIGZhbHNlXG4gIH1cbiAgcmV0dXJuIHRydWVcbn1cblxuLyoqXG4gKiBjaGVjayBpZiB0eXBlb2YgYXJnIG51bWJlclxuICovXG5leHBvcnQgZnVuY3Rpb24gaXNOdW1iZXIoYXJnOiB1bmtub3duKTogYXJnIGlzIG51bWJlciB7XG4gIHJldHVybiB0eXBlb2YgYXJnID09PSAnbnVtYmVyJ1xufVxuXG4vLyBlc2xpbnQtZGlzYWJsZS1uZXh0LWxpbmUgQHR5cGVzY3JpcHQtZXNsaW50L25vLWV4cGxpY2l0LWFueVxuZXhwb3J0IHR5cGUgQW55RnVuY3Rpb24gPSAoLi4uYXJnczogYW55W10pID0+IGFueVxuXG4vKipcbiAqIGNoZWNrIGlmIHR5cGVvZiBhcmcgZnVuY3Rpb25cbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIGlzRnVuY3Rpb24oYXJnOiB1bmtub3duKTogYXJnIGlzIEFueUZ1bmN0aW9uIHtcbiAgcmV0dXJuIHR5cGVvZiBhcmcgPT09ICdmdW5jdGlvbidcbn1cblxuLyoqXG4gKiBjaGVjayBpZiB0eXBlb2YgYXJnIHN0cmluZ1xuICovXG5leHBvcnQgZnVuY3Rpb24gaXNTdHJpbmcoYXJnOiB1bmtub3duKTogYXJnIGlzIHN0cmluZyB7XG4gIHJldHVybiB0eXBlb2YgYXJnID09PSAnc3RyaW5nJ1xufVxuXG4vKipcbiAqIGNoZWNrIGlmIHR5cGVvZiBhcmcgb2JqZWN0XG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBpc09iamVjdChhcmc6IHVua25vd24pOiBhcmcgaXMgb2JqZWN0IHtcbiAgcmV0dXJuIHR5cGVvZiBhcmcgPT09ICdvYmplY3QnICYmIGFyZyAhPT0gbnVsbFxufVxuLyoqXG4gKiBjaGVjayBpZiB0eXBlb2YgYXJnIGlzIHBsYWluIG9iamVjdFxuICovXG5leHBvcnQgZnVuY3Rpb24gaXNQbGFpbk9iamVjdChhcmc6IHVua25vd24pOiBhcmcgaXMgUmVjb3JkPHN0cmluZywgdW5rbm93bj4ge1xuICByZXR1cm4gT2JqZWN0LnByb3RvdHlwZS50b1N0cmluZy5jYWxsKGFyZykgPT09ICdbb2JqZWN0IE9iamVjdF0nXG59XG4vKipcbiAqIGNoZWNrIGlmIG9iamVjdCBpcyByZWFkYWJsZSBzdHJlYW1cbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIGlzUmVhZGFibGVTdHJlYW0oYXJnOiB1bmtub3duKTogYXJnIGlzIHN0cmVhbS5SZWFkYWJsZSB7XG4gIC8vIGVzbGludC1kaXNhYmxlLW5leHQtbGluZSBAdHlwZXNjcmlwdC1lc2xpbnQvdW5ib3VuZC1tZXRob2RcbiAgcmV0dXJuIGlzT2JqZWN0KGFyZykgJiYgaXNGdW5jdGlvbigoYXJnIGFzIHN0cmVhbS5SZWFkYWJsZSkuX3JlYWQpICYmIHN0cmVhbS5pc1JlYWRhYmxlKGFyZyBhcyBzdHJlYW0uUmVhZGFibGUpXG59XG5cbi8qKlxuICogY2hlY2sgaWYgYXJnIGlzIGJvb2xlYW5cbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIGlzQm9vbGVhbihhcmc6IHVua25vd24pOiBhcmcgaXMgYm9vbGVhbiB7XG4gIHJldHVybiB0eXBlb2YgYXJnID09PSAnYm9vbGVhbidcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGlzRW1wdHkobzogdW5rbm93bik6IG8gaXMgbnVsbCB8IHVuZGVmaW5lZCB7XG4gIHJldHVybiBfLmlzRW1wdHkobylcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGlzRW1wdHlPYmplY3QobzogUmVjb3JkPHN0cmluZywgdW5rbm93bj4pOiBib29sZWFuIHtcbiAgcmV0dXJuIE9iamVjdC52YWx1ZXMobykuZmlsdGVyKCh4KSA9PiB4ICE9PSB1bmRlZmluZWQpLmxlbmd0aCAhPT0gMFxufVxuXG5leHBvcnQgZnVuY3Rpb24gaXNEZWZpbmVkPFQ+KG86IFQpOiBvIGlzIEV4Y2x1ZGU8VCwgbnVsbCB8IHVuZGVmaW5lZD4ge1xuICByZXR1cm4gbyAhPT0gbnVsbCAmJiBvICE9PSB1bmRlZmluZWRcbn1cblxuLyoqXG4gKiBjaGVjayBpZiBhcmcgaXMgYSB2YWxpZCBkYXRlXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBpc1ZhbGlkRGF0ZShhcmc6IHVua25vd24pOiBhcmcgaXMgRGF0ZSB7XG4gIC8vIEB0cy1leHBlY3QtZXJyb3IgY2hlY2tuZXcgRGF0ZShNYXRoLk5hTilcbiAgcmV0dXJuIGFyZyBpbnN0YW5jZW9mIERhdGUgJiYgIWlzTmFOKGFyZylcbn1cblxuLyoqXG4gKiBDcmVhdGUgYSBEYXRlIHN0cmluZyB3aXRoIGZvcm1hdDogJ1lZWVlNTUREVEhIbW1zcycgKyBaXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBtYWtlRGF0ZUxvbmcoZGF0ZT86IERhdGUpOiBzdHJpbmcge1xuICBkYXRlID0gZGF0ZSB8fCBuZXcgRGF0ZSgpXG5cbiAgLy8gR2l2ZXMgZm9ybWF0IGxpa2U6ICcyMDE3LTA4LTA3VDE2OjI4OjU5Ljg4OVonXG4gIGNvbnN0IHMgPSBkYXRlLnRvSVNPU3RyaW5nKClcblxuICByZXR1cm4gcy5zbGljZSgwLCA0KSArIHMuc2xpY2UoNSwgNykgKyBzLnNsaWNlKDgsIDEzKSArIHMuc2xpY2UoMTQsIDE2KSArIHMuc2xpY2UoMTcsIDE5KSArICdaJ1xufVxuXG4vKipcbiAqIENyZWF0ZSBhIERhdGUgc3RyaW5nIHdpdGggZm9ybWF0OiAnWVlZWU1NREQnXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBtYWtlRGF0ZVNob3J0KGRhdGU/OiBEYXRlKSB7XG4gIGRhdGUgPSBkYXRlIHx8IG5ldyBEYXRlKClcblxuICAvLyBHaXZlcyBmb3JtYXQgbGlrZTogJzIwMTctMDgtMDdUMTY6Mjg6NTkuODg5WidcbiAgY29uc3QgcyA9IGRhdGUudG9JU09TdHJpbmcoKVxuXG4gIHJldHVybiBzLnNsaWNlKDAsIDQpICsgcy5zbGljZSg1LCA3KSArIHMuc2xpY2UoOCwgMTApXG59XG5cbi8qKlxuICogcGlwZXNldHVwIHNldHMgdXAgcGlwZSgpIGZyb20gbGVmdCB0byByaWdodCBvcyBzdHJlYW1zIGFycmF5XG4gKiBwaXBlc2V0dXAgd2lsbCBhbHNvIG1ha2Ugc3VyZSB0aGF0IGVycm9yIGVtaXR0ZWQgYXQgYW55IG9mIHRoZSB1cHN0cmVhbSBTdHJlYW1cbiAqIHdpbGwgYmUgZW1pdHRlZCBhdCB0aGUgbGFzdCBzdHJlYW0uIFRoaXMgbWFrZXMgZXJyb3IgaGFuZGxpbmcgc2ltcGxlXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBwaXBlc2V0dXAoLi4uc3RyZWFtczogW3N0cmVhbS5SZWFkYWJsZSwgLi4uc3RyZWFtLkR1cGxleFtdLCBzdHJlYW0uV3JpdGFibGVdKSB7XG4gIC8vIEB0cy1leHBlY3QtZXJyb3IgdHMgY2FuJ3QgbmFycm93IHRoaXNcbiAgcmV0dXJuIHN0cmVhbXMucmVkdWNlKChzcmM6IHN0cmVhbS5SZWFkYWJsZSwgZHN0OiBzdHJlYW0uV3JpdGFibGUpID0+IHtcbiAgICBzcmMub24oJ2Vycm9yJywgKGVycikgPT4gZHN0LmVtaXQoJ2Vycm9yJywgZXJyKSlcbiAgICByZXR1cm4gc3JjLnBpcGUoZHN0KVxuICB9KVxufVxuXG4vKipcbiAqIHJldHVybiBhIFJlYWRhYmxlIHN0cmVhbSB0aGF0IGVtaXRzIGRhdGFcbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIHJlYWRhYmxlU3RyZWFtKGRhdGE6IHVua25vd24pOiBzdHJlYW0uUmVhZGFibGUge1xuICBjb25zdCBzID0gbmV3IHN0cmVhbS5SZWFkYWJsZSgpXG4gIHMuX3JlYWQgPSAoKSA9PiB7fVxuICBzLnB1c2goZGF0YSlcbiAgcy5wdXNoKG51bGwpXG4gIHJldHVybiBzXG59XG5cbi8qKlxuICogUHJvY2VzcyBtZXRhZGF0YSB0byBpbnNlcnQgYXBwcm9wcmlhdGUgdmFsdWUgdG8gYGNvbnRlbnQtdHlwZWAgYXR0cmlidXRlXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBpbnNlcnRDb250ZW50VHlwZShtZXRhRGF0YTogT2JqZWN0TWV0YURhdGEsIGZpbGVQYXRoOiBzdHJpbmcpOiBPYmplY3RNZXRhRGF0YSB7XG4gIC8vIGNoZWNrIGlmIGNvbnRlbnQtdHlwZSBhdHRyaWJ1dGUgcHJlc2VudCBpbiBtZXRhRGF0YVxuICBmb3IgKGNvbnN0IGtleSBpbiBtZXRhRGF0YSkge1xuICAgIGlmIChrZXkudG9Mb3dlckNhc2UoKSA9PT0gJ2NvbnRlbnQtdHlwZScpIHtcbiAgICAgIHJldHVybiBtZXRhRGF0YVxuICAgIH1cbiAgfVxuXG4gIC8vIGlmIGBjb250ZW50LXR5cGVgIGF0dHJpYnV0ZSBpcyBub3QgcHJlc2VudCBpbiBtZXRhZGF0YSwgdGhlbiBpbmZlciBpdCBmcm9tIHRoZSBleHRlbnNpb24gaW4gZmlsZVBhdGhcbiAgcmV0dXJuIHtcbiAgICAuLi5tZXRhRGF0YSxcbiAgICAnY29udGVudC10eXBlJzogcHJvYmVDb250ZW50VHlwZShmaWxlUGF0aCksXG4gIH1cbn1cblxuLyoqXG4gKiBGdW5jdGlvbiBwcmVwZW5kcyBtZXRhZGF0YSB3aXRoIHRoZSBhcHByb3ByaWF0ZSBwcmVmaXggaWYgaXQgaXMgbm90IGFscmVhZHkgb25cbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIHByZXBlbmRYQU1aTWV0YShtZXRhRGF0YT86IE9iamVjdE1ldGFEYXRhKTogUmVxdWVzdEhlYWRlcnMge1xuICBpZiAoIW1ldGFEYXRhKSB7XG4gICAgcmV0dXJuIHt9XG4gIH1cblxuICByZXR1cm4gXy5tYXBLZXlzKG1ldGFEYXRhLCAodmFsdWUsIGtleSkgPT4ge1xuICAgIGlmIChpc0FtekhlYWRlcihrZXkpIHx8IGlzU3VwcG9ydGVkSGVhZGVyKGtleSkgfHwgaXNTdG9yYWdlQ2xhc3NIZWFkZXIoa2V5KSkge1xuICAgICAgcmV0dXJuIGtleVxuICAgIH1cblxuICAgIHJldHVybiBNZXRhRGF0YUhlYWRlclByZWZpeCArIGtleVxuICB9KVxufVxuXG4vKipcbiAqIENoZWNrcyBpZiBpdCBpcyBhIHZhbGlkIGhlYWRlciBhY2NvcmRpbmcgdG8gdGhlIEFtYXpvblMzIEFQSVxuICovXG5leHBvcnQgZnVuY3Rpb24gaXNBbXpIZWFkZXIoa2V5OiBzdHJpbmcpIHtcbiAgY29uc3QgdGVtcCA9IGtleS50b0xvd2VyQ2FzZSgpXG4gIHJldHVybiAoXG4gICAgdGVtcC5zdGFydHNXaXRoKE1ldGFEYXRhSGVhZGVyUHJlZml4KSB8fFxuICAgIHRlbXAgPT09ICd4LWFtei1hY2wnIHx8XG4gICAgdGVtcC5zdGFydHNXaXRoKCd4LWFtei1zZXJ2ZXItc2lkZS1lbmNyeXB0aW9uLScpIHx8XG4gICAgdGVtcCA9PT0gJ3gtYW16LXNlcnZlci1zaWRlLWVuY3J5cHRpb24nXG4gIClcbn1cblxuLyoqXG4gKiBDaGVja3MgaWYgaXQgaXMgYSBzdXBwb3J0ZWQgSGVhZGVyXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiBpc1N1cHBvcnRlZEhlYWRlcihrZXk6IHN0cmluZykge1xuICBjb25zdCBzdXBwb3J0ZWRfaGVhZGVycyA9IFtcbiAgICAnY29udGVudC10eXBlJyxcbiAgICAnY2FjaGUtY29udHJvbCcsXG4gICAgJ2NvbnRlbnQtZW5jb2RpbmcnLFxuICAgICdjb250ZW50LWRpc3Bvc2l0aW9uJyxcbiAgICAnY29udGVudC1sYW5ndWFnZScsXG4gICAgJ3gtYW16LXdlYnNpdGUtcmVkaXJlY3QtbG9jYXRpb24nLFxuICAgICdpZi1ub25lLW1hdGNoJyxcbiAgICAnaWYtbWF0Y2gnLFxuICBdXG4gIHJldHVybiBzdXBwb3J0ZWRfaGVhZGVycy5pbmNsdWRlcyhrZXkudG9Mb3dlckNhc2UoKSlcbn1cblxuLyoqXG4gKiBDaGVja3MgaWYgaXQgaXMgYSBzdG9yYWdlIGhlYWRlclxuICovXG5leHBvcnQgZnVuY3Rpb24gaXNTdG9yYWdlQ2xhc3NIZWFkZXIoa2V5OiBzdHJpbmcpIHtcbiAgcmV0dXJuIGtleS50b0xvd2VyQ2FzZSgpID09PSAneC1hbXotc3RvcmFnZS1jbGFzcydcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGV4dHJhY3RNZXRhZGF0YShoZWFkZXJzOiBSZXNwb25zZUhlYWRlcikge1xuICByZXR1cm4gXy5tYXBLZXlzKFxuICAgIF8ucGlja0J5KGhlYWRlcnMsICh2YWx1ZSwga2V5KSA9PiBpc1N1cHBvcnRlZEhlYWRlcihrZXkpIHx8IGlzU3RvcmFnZUNsYXNzSGVhZGVyKGtleSkgfHwgaXNBbXpIZWFkZXIoa2V5KSksXG4gICAgKHZhbHVlLCBrZXkpID0+IHtcbiAgICAgIGNvbnN0IGxvd2VyID0ga2V5LnRvTG93ZXJDYXNlKClcbiAgICAgIGlmIChsb3dlci5zdGFydHNXaXRoKE1ldGFEYXRhSGVhZGVyUHJlZml4KSkge1xuICAgICAgICByZXR1cm4gbG93ZXIuc2xpY2UoTWV0YURhdGFIZWFkZXJQcmVmaXgubGVuZ3RoKVxuICAgICAgfVxuXG4gICAgICByZXR1cm4ga2V5XG4gICAgfSxcbiAgKVxufVxuXG5leHBvcnQgZnVuY3Rpb24gZ2V0VmVyc2lvbklkKGhlYWRlcnM6IFJlc3BvbnNlSGVhZGVyID0ge30pIHtcbiAgcmV0dXJuIGhlYWRlcnNbJ3gtYW16LXZlcnNpb24taWQnXSB8fCBudWxsXG59XG5cbmV4cG9ydCBmdW5jdGlvbiBnZXRTb3VyY2VWZXJzaW9uSWQoaGVhZGVyczogUmVzcG9uc2VIZWFkZXIgPSB7fSkge1xuICByZXR1cm4gaGVhZGVyc1sneC1hbXotY29weS1zb3VyY2UtdmVyc2lvbi1pZCddIHx8IG51bGxcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHNhbml0aXplRVRhZyhldGFnID0gJycpOiBzdHJpbmcge1xuICBjb25zdCByZXBsYWNlQ2hhcnM6IFJlY29yZDxzdHJpbmcsIHN0cmluZz4gPSB7XG4gICAgJ1wiJzogJycsXG4gICAgJyZxdW90Oyc6ICcnLFxuICAgICcmIzM0Oyc6ICcnLFxuICAgICcmUVVPVDsnOiAnJyxcbiAgICAnJiN4MDAwMjInOiAnJyxcbiAgfVxuICByZXR1cm4gZXRhZy5yZXBsYWNlKC9eKFwifCZxdW90O3wmIzM0Oyl8KFwifCZxdW90O3wmIzM0OykkL2csIChtKSA9PiByZXBsYWNlQ2hhcnNbbV0gYXMgc3RyaW5nKVxufVxuXG5leHBvcnQgZnVuY3Rpb24gdG9NZDUocGF5bG9hZDogQmluYXJ5KTogc3RyaW5nIHtcbiAgLy8gdXNlIHN0cmluZyBmcm9tIGJyb3dzZXIgYW5kIGJ1ZmZlciBmcm9tIG5vZGVqc1xuICAvLyBicm93c2VyIHN1cHBvcnQgaXMgdGVzdGVkIG9ubHkgYWdhaW5zdCBtaW5pbyBzZXJ2ZXJcbiAgcmV0dXJuIGNyeXB0by5jcmVhdGVIYXNoKCdtZDUnKS51cGRhdGUoQnVmZmVyLmZyb20ocGF5bG9hZCkpLmRpZ2VzdCgpLnRvU3RyaW5nKCdiYXNlNjQnKVxufVxuXG5leHBvcnQgZnVuY3Rpb24gdG9TaGEyNTYocGF5bG9hZDogQmluYXJ5KTogc3RyaW5nIHtcbiAgcmV0dXJuIGNyeXB0by5jcmVhdGVIYXNoKCdzaGEyNTYnKS51cGRhdGUocGF5bG9hZCkuZGlnZXN0KCdoZXgnKVxufVxuXG4vKipcbiAqIHRvQXJyYXkgcmV0dXJucyBhIHNpbmdsZSBlbGVtZW50IGFycmF5IHdpdGggcGFyYW0gYmVpbmcgdGhlIGVsZW1lbnQsXG4gKiBpZiBwYXJhbSBpcyBqdXN0IGEgc3RyaW5nLCBhbmQgcmV0dXJucyAncGFyYW0nIGJhY2sgaWYgaXQgaXMgYW4gYXJyYXlcbiAqIFNvLCBpdCBtYWtlcyBzdXJlIHBhcmFtIGlzIGFsd2F5cyBhbiBhcnJheVxuICovXG5leHBvcnQgZnVuY3Rpb24gdG9BcnJheTxUID0gdW5rbm93bj4ocGFyYW06IFQgfCBUW10pOiBBcnJheTxUPiB7XG4gIGlmICghQXJyYXkuaXNBcnJheShwYXJhbSkpIHtcbiAgICByZXR1cm4gW3BhcmFtXSBhcyBUW11cbiAgfVxuICByZXR1cm4gcGFyYW1cbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHNhbml0aXplT2JqZWN0S2V5KG9iamVjdE5hbWU6IHN0cmluZyk6IHN0cmluZyB7XG4gIC8vICsgc3ltYm9sIGNoYXJhY3RlcnMgYXJlIG5vdCBkZWNvZGVkIGFzIHNwYWNlcyBpbiBKUy4gc28gcmVwbGFjZSB0aGVtIGZpcnN0IGFuZCBkZWNvZGUgdG8gZ2V0IHRoZSBjb3JyZWN0IHJlc3VsdC5cbiAgY29uc3QgYXNTdHJOYW1lID0gKG9iamVjdE5hbWUgPyBvYmplY3ROYW1lLnRvU3RyaW5nKCkgOiAnJykucmVwbGFjZSgvXFwrL2csICcgJylcbiAgcmV0dXJuIGRlY29kZVVSSUNvbXBvbmVudChhc1N0ck5hbWUpXG59XG5cbmV4cG9ydCBmdW5jdGlvbiBzYW5pdGl6ZVNpemUoc2l6ZT86IHN0cmluZyk6IG51bWJlciB8IHVuZGVmaW5lZCB7XG4gIHJldHVybiBzaXplID8gTnVtYmVyLnBhcnNlSW50KHNpemUpIDogdW5kZWZpbmVkXG59XG5cbmV4cG9ydCBjb25zdCBQQVJUX0NPTlNUUkFJTlRTID0ge1xuICAvLyBhYnNNaW5QYXJ0U2l6ZSAtIGFic29sdXRlIG1pbmltdW0gcGFydCBzaXplICg1IE1pQilcbiAgQUJTX01JTl9QQVJUX1NJWkU6IDEwMjQgKiAxMDI0ICogNSxcbiAgLy8gTUlOX1BBUlRfU0laRSAtIG1pbmltdW0gcGFydCBzaXplIDE2TWlCIHBlciBvYmplY3QgYWZ0ZXIgd2hpY2hcbiAgTUlOX1BBUlRfU0laRTogMTAyNCAqIDEwMjQgKiAxNixcbiAgLy8gTUFYX1BBUlRTX0NPVU5UIC0gbWF4aW11bSBudW1iZXIgb2YgcGFydHMgZm9yIGEgc2luZ2xlIG11bHRpcGFydCBzZXNzaW9uLlxuICBNQVhfUEFSVFNfQ09VTlQ6IDEwMDAwLFxuICAvLyBNQVhfUEFSVF9TSVpFIC0gbWF4aW11bSBwYXJ0IHNpemUgNUdpQiBmb3IgYSBzaW5nbGUgbXVsdGlwYXJ0IHVwbG9hZFxuICAvLyBvcGVyYXRpb24uXG4gIE1BWF9QQVJUX1NJWkU6IDEwMjQgKiAxMDI0ICogMTAyNCAqIDUsXG4gIC8vIE1BWF9TSU5HTEVfUFVUX09CSkVDVF9TSVpFIC0gbWF4aW11bSBzaXplIDVHaUIgb2Ygb2JqZWN0IHBlciBQVVRcbiAgLy8gb3BlcmF0aW9uLlxuICBNQVhfU0lOR0xFX1BVVF9PQkpFQ1RfU0laRTogMTAyNCAqIDEwMjQgKiAxMDI0ICogNSxcbiAgLy8gTUFYX01VTFRJUEFSVF9QVVRfT0JKRUNUX1NJWkUgLSBtYXhpbXVtIHNpemUgNVRpQiBvZiBvYmplY3QgZm9yXG4gIC8vIE11bHRpcGFydCBvcGVyYXRpb24uXG4gIE1BWF9NVUxUSVBBUlRfUFVUX09CSkVDVF9TSVpFOiAxMDI0ICogMTAyNCAqIDEwMjQgKiAxMDI0ICogNSxcbn1cblxuY29uc3QgR0VORVJJQ19TU0VfSEVBREVSID0gJ1gtQW16LVNlcnZlci1TaWRlLUVuY3J5cHRpb24nXG5cbmNvbnN0IEVOQ1JZUFRJT05fSEVBREVSUyA9IHtcbiAgLy8gc3NlR2VuZXJpY0hlYWRlciBpcyB0aGUgQVdTIFNTRSBoZWFkZXIgdXNlZCBmb3IgU1NFLVMzIGFuZCBTU0UtS01TLlxuICBzc2VHZW5lcmljSGVhZGVyOiBHRU5FUklDX1NTRV9IRUFERVIsXG4gIC8vIHNzZUttc0tleUlEIGlzIHRoZSBBV1MgU1NFLUtNUyBrZXkgaWQuXG4gIHNzZUttc0tleUlEOiBHRU5FUklDX1NTRV9IRUFERVIgKyAnLUF3cy1LbXMtS2V5LUlkJyxcbn0gYXMgY29uc3RcblxuLyoqXG4gKiBSZXR1cm4gRW5jcnlwdGlvbiBoZWFkZXJzXG4gKiBAcGFyYW0gZW5jQ29uZmlnXG4gKiBAcmV0dXJucyBhbiBvYmplY3Qgd2l0aCBrZXkgdmFsdWUgcGFpcnMgdGhhdCBjYW4gYmUgdXNlZCBpbiBoZWFkZXJzLlxuICovXG5leHBvcnQgZnVuY3Rpb24gZ2V0RW5jcnlwdGlvbkhlYWRlcnMoZW5jQ29uZmlnOiBFbmNyeXB0aW9uKTogUmVxdWVzdEhlYWRlcnMge1xuICBjb25zdCBlbmNUeXBlID0gZW5jQ29uZmlnLnR5cGVcblxuICBpZiAoIWlzRW1wdHkoZW5jVHlwZSkpIHtcbiAgICBpZiAoZW5jVHlwZSA9PT0gRU5DUllQVElPTl9UWVBFUy5TU0VDKSB7XG4gICAgICByZXR1cm4ge1xuICAgICAgICBbRU5DUllQVElPTl9IRUFERVJTLnNzZUdlbmVyaWNIZWFkZXJdOiAnQUVTMjU2JyxcbiAgICAgIH1cbiAgICB9IGVsc2UgaWYgKGVuY1R5cGUgPT09IEVOQ1JZUFRJT05fVFlQRVMuS01TKSB7XG4gICAgICByZXR1cm4ge1xuICAgICAgICBbRU5DUllQVElPTl9IRUFERVJTLnNzZUdlbmVyaWNIZWFkZXJdOiBlbmNDb25maWcuU1NFQWxnb3JpdGhtLFxuICAgICAgICBbRU5DUllQVElPTl9IRUFERVJTLnNzZUttc0tleUlEXTogZW5jQ29uZmlnLktNU01hc3RlcktleUlELFxuICAgICAgfVxuICAgIH1cbiAgfVxuXG4gIHJldHVybiB7fVxufVxuXG5leHBvcnQgZnVuY3Rpb24gcGFydHNSZXF1aXJlZChzaXplOiBudW1iZXIpOiBudW1iZXIge1xuICBjb25zdCBtYXhQYXJ0U2l6ZSA9IFBBUlRfQ09OU1RSQUlOVFMuTUFYX01VTFRJUEFSVF9QVVRfT0JKRUNUX1NJWkUgLyAoUEFSVF9DT05TVFJBSU5UUy5NQVhfUEFSVFNfQ09VTlQgLSAxKVxuICBsZXQgcmVxdWlyZWRQYXJ0U2l6ZSA9IHNpemUgLyBtYXhQYXJ0U2l6ZVxuICBpZiAoc2l6ZSAlIG1heFBhcnRTaXplID4gMCkge1xuICAgIHJlcXVpcmVkUGFydFNpemUrK1xuICB9XG4gIHJlcXVpcmVkUGFydFNpemUgPSBNYXRoLnRydW5jKHJlcXVpcmVkUGFydFNpemUpXG4gIHJldHVybiByZXF1aXJlZFBhcnRTaXplXG59XG5cbi8qKlxuICogY2FsY3VsYXRlRXZlblNwbGl0cyAtIGNvbXB1dGVzIHNwbGl0cyBmb3IgYSBzb3VyY2UgYW5kIHJldHVybnNcbiAqIHN0YXJ0IGFuZCBlbmQgaW5kZXggc2xpY2VzLiBTcGxpdHMgaGFwcGVuIGV2ZW5seSB0byBiZSBzdXJlIHRoYXQgbm9cbiAqIHBhcnQgaXMgbGVzcyB0aGFuIDVNaUIsIGFzIHRoYXQgY291bGQgZmFpbCB0aGUgbXVsdGlwYXJ0IHJlcXVlc3QgaWZcbiAqIGl0IGlzIG5vdCB0aGUgbGFzdCBwYXJ0LlxuICovXG5leHBvcnQgZnVuY3Rpb24gY2FsY3VsYXRlRXZlblNwbGl0czxUIGV4dGVuZHMgeyBTdGFydD86IG51bWJlciB9PihcbiAgc2l6ZTogbnVtYmVyLFxuICBvYmpJbmZvOiBULFxuKToge1xuICBzdGFydEluZGV4OiBudW1iZXJbXVxuICBvYmpJbmZvOiBUXG4gIGVuZEluZGV4OiBudW1iZXJbXVxufSB8IG51bGwge1xuICBpZiAoc2l6ZSA9PT0gMCkge1xuICAgIHJldHVybiBudWxsXG4gIH1cbiAgY29uc3QgcmVxUGFydHMgPSBwYXJ0c1JlcXVpcmVkKHNpemUpXG4gIGNvbnN0IHN0YXJ0SW5kZXhQYXJ0czogbnVtYmVyW10gPSBbXVxuICBjb25zdCBlbmRJbmRleFBhcnRzOiBudW1iZXJbXSA9IFtdXG5cbiAgbGV0IHN0YXJ0ID0gb2JqSW5mby5TdGFydFxuICBpZiAoaXNFbXB0eShzdGFydCkgfHwgc3RhcnQgPT09IC0xKSB7XG4gICAgc3RhcnQgPSAwXG4gIH1cbiAgY29uc3QgZGl2aXNvclZhbHVlID0gTWF0aC50cnVuYyhzaXplIC8gcmVxUGFydHMpXG5cbiAgY29uc3QgcmVtaW5kZXJWYWx1ZSA9IHNpemUgJSByZXFQYXJ0c1xuXG4gIGxldCBuZXh0U3RhcnQgPSBzdGFydFxuXG4gIGZvciAobGV0IGkgPSAwOyBpIDwgcmVxUGFydHM7IGkrKykge1xuICAgIGxldCBjdXJQYXJ0U2l6ZSA9IGRpdmlzb3JWYWx1ZVxuICAgIGlmIChpIDwgcmVtaW5kZXJWYWx1ZSkge1xuICAgICAgY3VyUGFydFNpemUrK1xuICAgIH1cblxuICAgIGNvbnN0IGN1cnJlbnRTdGFydCA9IG5leHRTdGFydFxuICAgIGNvbnN0IGN1cnJlbnRFbmQgPSBjdXJyZW50U3RhcnQgKyBjdXJQYXJ0U2l6ZSAtIDFcbiAgICBuZXh0U3RhcnQgPSBjdXJyZW50RW5kICsgMVxuXG4gICAgc3RhcnRJbmRleFBhcnRzLnB1c2goY3VycmVudFN0YXJ0KVxuICAgIGVuZEluZGV4UGFydHMucHVzaChjdXJyZW50RW5kKVxuICB9XG5cbiAgcmV0dXJuIHsgc3RhcnRJbmRleDogc3RhcnRJbmRleFBhcnRzLCBlbmRJbmRleDogZW5kSW5kZXhQYXJ0cywgb2JqSW5mbzogb2JqSW5mbyB9XG59XG5jb25zdCBmeHAgPSBuZXcgWE1MUGFyc2VyKHsgbnVtYmVyUGFyc2VPcHRpb25zOiB7IGVOb3RhdGlvbjogZmFsc2UsIGhleDogdHJ1ZSwgbGVhZGluZ1plcm9zOiB0cnVlIH0gfSlcblxuLy8gZXNsaW50LWRpc2FibGUtbmV4dC1saW5lIEB0eXBlc2NyaXB0LWVzbGludC9uby1leHBsaWNpdC1hbnlcbmV4cG9ydCBmdW5jdGlvbiBwYXJzZVhtbCh4bWw6IHN0cmluZyk6IGFueSB7XG4gIGNvbnN0IHJlc3VsdCA9IGZ4cC5wYXJzZSh4bWwpXG4gIGlmIChyZXN1bHQuRXJyb3IpIHtcbiAgICB0aHJvdyByZXN1bHQuRXJyb3JcbiAgfVxuXG4gIHJldHVybiByZXN1bHRcbn1cblxuLyoqXG4gKiBnZXQgY29udGVudCBzaXplIG9mIG9iamVjdCBjb250ZW50IHRvIHVwbG9hZFxuICovXG5leHBvcnQgYXN5bmMgZnVuY3Rpb24gZ2V0Q29udGVudExlbmd0aChzOiBzdHJlYW0uUmVhZGFibGUgfCBCdWZmZXIgfCBzdHJpbmcpOiBQcm9taXNlPG51bWJlciB8IG51bGw+IHtcbiAgLy8gdXNlIGxlbmd0aCBwcm9wZXJ0eSBvZiBzdHJpbmcgfCBCdWZmZXJcbiAgaWYgKHR5cGVvZiBzID09PSAnc3RyaW5nJyB8fCBCdWZmZXIuaXNCdWZmZXIocykpIHtcbiAgICByZXR1cm4gcy5sZW5ndGhcbiAgfVxuXG4gIC8vIHByb3BlcnR5IG9mIGBmcy5SZWFkU3RyZWFtYFxuICBjb25zdCBmaWxlUGF0aCA9IChzIGFzIHVua25vd24gYXMgUmVjb3JkPHN0cmluZywgdW5rbm93bj4pLnBhdGggYXMgc3RyaW5nIHwgdW5kZWZpbmVkXG4gIGlmIChmaWxlUGF0aCAmJiB0eXBlb2YgZmlsZVBhdGggPT09ICdzdHJpbmcnKSB7XG4gICAgY29uc3Qgc3RhdCA9IGF3YWl0IGZzcC5sc3RhdChmaWxlUGF0aClcbiAgICByZXR1cm4gc3RhdC5zaXplXG4gIH1cblxuICAvLyBwcm9wZXJ0eSBvZiBgZnMuUmVhZFN0cmVhbWBcbiAgY29uc3QgZmQgPSAocyBhcyB1bmtub3duIGFzIFJlY29yZDxzdHJpbmcsIHVua25vd24+KS5mZCBhcyBudW1iZXIgfCBudWxsIHwgdW5kZWZpbmVkXG4gIGlmIChmZCAmJiB0eXBlb2YgZmQgPT09ICdudW1iZXInKSB7XG4gICAgY29uc3Qgc3RhdCA9IGF3YWl0IGZzdGF0KGZkKVxuICAgIHJldHVybiBzdGF0LnNpemVcbiAgfVxuXG4gIHJldHVybiBudWxsXG59XG4iXSwibWFwcGluZ3MiOiI7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7O0FBZ0JBLElBQUFBLE1BQUEsR0FBQUMsdUJBQUEsQ0FBQUMsT0FBQTtBQUNBLElBQUFDLE1BQUEsR0FBQUYsdUJBQUEsQ0FBQUMsT0FBQTtBQUVBLElBQUFFLGNBQUEsR0FBQUYsT0FBQTtBQUNBLElBQUFHLE9BQUEsR0FBQUgsT0FBQTtBQUNBLElBQUFJLE9BQUEsR0FBQUosT0FBQTtBQUNBLElBQUFLLElBQUEsR0FBQU4sdUJBQUEsQ0FBQUMsT0FBQTtBQUVBLElBQUFNLE1BQUEsR0FBQU4sT0FBQTtBQUVBLElBQUFPLEtBQUEsR0FBQVAsT0FBQTtBQUE0QyxTQUFBRCx3QkFBQVMsQ0FBQSxF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