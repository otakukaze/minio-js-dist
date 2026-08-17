"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
exports.parseBucketEncryptionConfig = parseBucketEncryptionConfig;
exports.parseBucketNotification = parseBucketNotification;
exports.parseBucketRegion = parseBucketRegion;
exports.parseBucketVersioningConfig = parseBucketVersioningConfig;
exports.parseCompleteMultipart = parseCompleteMultipart;
exports.parseCopyObject = parseCopyObject;
exports.parseError = parseError;
exports.parseInitiateMultipart = parseInitiateMultipart;
exports.parseLifecycleConfig = parseLifecycleConfig;
exports.parseListBucket = parseListBucket;
exports.parseListMultipart = parseListMultipart;
exports.parseListObjects = parseListObjects;
exports.parseListObjectsV2 = parseListObjectsV2;
exports.parseListObjectsV2WithMetadata = parseListObjectsV2WithMetadata;
exports.parseListParts = parseListParts;
exports.parseObjectLegalHoldConfig = parseObjectLegalHoldConfig;
exports.parseObjectLockConfig = parseObjectLockConfig;
exports.parseObjectRetentionConfig = parseObjectRetentionConfig;
exports.parseReplicationConfig = parseReplicationConfig;
exports.parseResponseError = parseResponseError;
exports.parseSelectObjectContentResponse = parseSelectObjectContentResponse;
exports.parseTagging = parseTagging;
exports.removeObjectsParser = removeObjectsParser;
exports.uploadPartParser = uploadPartParser;
var _bufferCrc = _interopRequireDefault(require("buffer-crc32"));
var _fastXmlParser = require("fast-xml-parser");
var errors = _interopRequireWildcard(require("../errors.js"));
var _helpers = require("../helpers.js");
var _helper = require("./helper.js");
var _response = require("./response.js");
var _type = require("./type.js");
function _interopRequireWildcard(e, t) { if ("function" == typeof WeakMap) var r = new WeakMap(), n = new WeakMap(); return (_interopRequireWildcard = function (e, t) { if (!t && e && e.__esModule) return e; var o, i, f = { __proto__: null, default: e }; if (null === e || "object" != typeof e && "function" != typeof e) return f; if (o = t ? n : r) { if (o.has(e)) return o.get(e); o.set(e, f); } for (const t in e) "default" !== t && {}.hasOwnProperty.call(e, t) && ((i = (o = Object.defineProperty) && Object.getOwnPropertyDescriptor(e, t)) && (i.get || i.set) ? o(f, t, i) : f[t] = e[t]); return f; })(e, t); }
function _interopRequireDefault(e) { return e && e.__esModule ? e : { default: e }; }
// parse XML response for bucket region
function parseBucketRegion(xml) {
  // return region information
  return (0, _helper.parseXml)(xml).LocationConstraint;
}
const fxp = new _fastXmlParser.XMLParser();
const fxpWithoutNumParser = new _fastXmlParser.XMLParser({
  // @ts-ignore
  numberParseOptions: {
    skipLike: /./
  }
});

// Parse XML and return information as Javascript types
// parse error XML response
function parseError(xml, headerInfo) {
  let xmlErr = {};
  const xmlObj = fxp.parse(xml);
  if (xmlObj.Error) {
    xmlErr = xmlObj.Error;
  }
  const e = new errors.S3Error();
  Object.entries(xmlErr).forEach(([key, value]) => {
    e[key.toLowerCase()] = value;
  });
  Object.entries(headerInfo).forEach(([key, value]) => {
    e[key] = value;
  });
  return e;
}

// Generates an Error object depending on http statusCode and XML body
async function parseResponseError(response) {
  const statusCode = response.statusCode;
  let code = '',
    message = '';
  if (statusCode === 301) {
    code = 'MovedPermanently';
    message = 'Moved Permanently';
  } else if (statusCode === 307) {
    code = 'TemporaryRedirect';
    message = 'Are you using the correct endpoint URL?';
  } else if (statusCode === 403) {
    code = 'AccessDenied';
    message = 'Valid and authorized credentials required';
  } else if (statusCode === 404) {
    code = 'NotFound';
    message = 'Not Found';
  } else if (statusCode === 405) {
    code = 'MethodNotAllowed';
    message = 'Method Not Allowed';
  } else if (statusCode === 501) {
    code = 'MethodNotAllowed';
    message = 'Method Not Allowed';
  } else if (statusCode === 503) {
    code = 'SlowDown';
    message = 'Please reduce your request rate.';
  } else {
    const hErrCode = response.headers['x-minio-error-code'];
    const hErrDesc = response.headers['x-minio-error-desc'];
    if (hErrCode && hErrDesc) {
      code = hErrCode;
      message = hErrDesc;
    }
  }
  const headerInfo = {};
  // A value created by S3 compatible server that uniquely identifies the request.
  headerInfo.amzRequestid = response.headers['x-amz-request-id'];
  // A special token that helps troubleshoot API replies and issues.
  headerInfo.amzId2 = response.headers['x-amz-id-2'];

  // Region where the bucket is located. This header is returned only
  // in HEAD bucket and ListObjects response.
  headerInfo.amzBucketRegion = response.headers['x-amz-bucket-region'];
  const xmlString = await (0, _response.readAsString)(response);
  if (xmlString) {
    throw parseError(xmlString, headerInfo);
  }

  // Message should be instantiated for each S3Errors.
  const e = new errors.S3Error(message, {
    cause: headerInfo
  });
  // S3 Error code.
  e.code = code;
  Object.entries(headerInfo).forEach(([key, value]) => {
    // @ts-expect-error force set error properties
    e[key] = value;
  });
  throw e;
}

/**
 * parse XML response for list objects v2 with metadata in a bucket
 */
function parseListObjectsV2WithMetadata(xml) {
  const result = {
    objects: [],
    isTruncated: false,
    nextContinuationToken: ''
  };
  let xmlobj = (0, _helper.parseXml)(xml);
  if (!xmlobj.ListBucketResult) {
    throw new errors.InvalidXMLError('Missing tag: "ListBucketResult"');
  }
  xmlobj = xmlobj.ListBucketResult;
  if (xmlobj.IsTruncated) {
    result.isTruncated = xmlobj.IsTruncated;
  }
  if (xmlobj.NextContinuationToken) {
    result.nextContinuationToken = xmlobj.NextContinuationToken;
  }
  if (xmlobj.Contents) {
    (0, _helper.toArray)(xmlobj.Contents).forEach(content => {
      const name = (0, _helper.sanitizeObjectKey)(content.Key);
      const lastModified = new Date(content.LastModified);
      const etag = (0, _helper.sanitizeETag)(content.ETag);
      const size = content.Size;
      let tags = {};
      if (content.UserTags != null) {
        (0, _helper.toArray)(content.UserTags.split('&')).forEach(tag => {
          const [key, value] = tag.split('=');
          tags[key] = value;
        });
      } else {
        tags = {};
      }
      let metadata;
      if (content.UserMetadata != null) {
        metadata = (0, _helper.toArray)(content.UserMetadata)[0];
      } else {
        metadata = null;
      }
      result.objects.push({
        name,
        lastModified,
        etag,
        size,
        metadata,
        tags
      });
    });
  }
  if (xmlobj.CommonPrefixes) {
    (0, _helper.toArray)(xmlobj.CommonPrefixes).forEach(commonPrefix => {
      result.objects.push({
        prefix: (0, _helper.sanitizeObjectKey)((0, _helper.toArray)(commonPrefix.Prefix)[0]),
        size: 0
      });
    });
  }
  return result;
}
function parseListObjectsV2(xml) {
  const result = {
    objects: [],
    isTruncated: false,
    nextContinuationToken: ''
  };
  let xmlobj = (0, _helper.parseXml)(xml);
  if (!xmlobj.ListBucketResult) {
    throw new errors.InvalidXMLError('Missing tag: "ListBucketResult"');
  }
  xmlobj = xmlobj.ListBucketResult;
  if (xmlobj.IsTruncated) {
    result.isTruncated = xmlobj.IsTruncated;
  }
  if (xmlobj.NextContinuationToken) {
    result.nextContinuationToken = xmlobj.NextContinuationToken;
  }
  if (xmlobj.Contents) {
    (0, _helper.toArray)(xmlobj.Contents).forEach(content => {
      const name = (0, _helper.sanitizeObjectKey)((0, _helper.toArray)(content.Key)[0]);
      const lastModified = new Date(content.LastModified);
      const etag = (0, _helper.sanitizeETag)(content.ETag);
      const size = content.Size;
      result.objects.push({
        name,
        lastModified,
        etag,
        size
      });
    });
  }
  if (xmlobj.CommonPrefixes) {
    (0, _helper.toArray)(xmlobj.CommonPrefixes).forEach(commonPrefix => {
      result.objects.push({
        prefix: (0, _helper.sanitizeObjectKey)((0, _helper.toArray)(commonPrefix.Prefix)[0]),
        size: 0
      });
    });
  }
  return result;
}
function parseBucketNotification(xml) {
  const result = {
    TopicConfiguration: [],
    QueueConfiguration: [],
    CloudFunctionConfiguration: []
  };
  const genEvents = events => {
    if (!events) {
      return [];
    }
    return (0, _helper.toArray)(events);
  };
  const genFilterRules = filters => {
    var _filterArr$;
    const rules = [];
    if (!filters) {
      return rules;
    }
    const filterArr = (0, _helper.toArray)(filters);
    if ((_filterArr$ = filterArr[0]) !== null && _filterArr$ !== void 0 && _filterArr$.S3Key) {
      var _s3KeyArr$;
      const s3KeyArr = (0, _helper.toArray)(filterArr[0].S3Key);
      if ((_s3KeyArr$ = s3KeyArr[0]) !== null && _s3KeyArr$ !== void 0 && _s3KeyArr$.FilterRule) {
        (0, _helper.toArray)(s3KeyArr[0].FilterRule).forEach(rule => {
          const r = rule;
          const Name = (0, _helper.toArray)(r.Name)[0];
          const Value = (0, _helper.toArray)(r.Value)[0];
          rules.push({
            Name,
            Value
          });
        });
      }
    }
    return rules;
  };
  let xmlobj = (0, _helper.parseXml)(xml);
  xmlobj = xmlobj.NotificationConfiguration;
  if (xmlobj.TopicConfiguration) {
    (0, _helper.toArray)(xmlobj.TopicConfiguration).forEach(config => {
      const Id = (0, _helper.toArray)(config.Id)[0];
      const Topic = (0, _helper.toArray)(config.Topic)[0];
      const Event = genEvents(config.Event);
      const Filter = genFilterRules(config.Filter);
      result.TopicConfiguration.push({
        Id,
        Topic,
        Event,
        Filter
      });
    });
  }
  if (xmlobj.QueueConfiguration) {
    (0, _helper.toArray)(xmlobj.QueueConfiguration).forEach(config => {
      const Id = (0, _helper.toArray)(config.Id)[0];
      const Queue = (0, _helper.toArray)(config.Queue)[0];
      const Event = genEvents(config.Event);
      const Filter = genFilterRules(config.Filter);
      result.QueueConfiguration.push({
        Id,
        Queue,
        Event,
        Filter
      });
    });
  }
  if (xmlobj.CloudFunctionConfiguration) {
    (0, _helper.toArray)(xmlobj.CloudFunctionConfiguration).forEach(config => {
      const Id = (0, _helper.toArray)(config.Id)[0];
      const CloudFunction = (0, _helper.toArray)(config.CloudFunction)[0];
      const Event = genEvents(config.Event);
      const Filter = genFilterRules(config.Filter);
      result.CloudFunctionConfiguration.push({
        Id,
        CloudFunction,
        Event,
        Filter
      });
    });
  }
  return result;
}
// parse XML response for list parts of an in progress multipart upload
function parseListParts(xml) {
  let xmlobj = (0, _helper.parseXml)(xml);
  const result = {
    isTruncated: false,
    parts: [],
    marker: 0
  };
  if (!xmlobj.ListPartsResult) {
    throw new errors.InvalidXMLError('Missing tag: "ListPartsResult"');
  }
  xmlobj = xmlobj.ListPartsResult;
  if (xmlobj.IsTruncated) {
    result.isTruncated = xmlobj.IsTruncated;
  }
  if (xmlobj.NextPartNumberMarker) {
    result.marker = (0, _helper.toArray)(xmlobj.NextPartNumberMarker)[0] || '';
  }
  if (xmlobj.Part) {
    (0, _helper.toArray)(xmlobj.Part).forEach(p => {
      const part = parseInt((0, _helper.toArray)(p.PartNumber)[0], 10);
      const lastModified = new Date(p.LastModified);
      const etag = p.ETag.replace(/^"/g, '').replace(/"$/g, '').replace(/^&quot;/g, '').replace(/&quot;$/g, '').replace(/^&#34;/g, '').replace(/&#34;$/g, '');
      result.parts.push({
        part,
        lastModified,
        etag,
        size: parseInt(p.Size, 10)
      });
    });
  }
  return result;
}
function parseListBucket(xml) {
  let result = [];
  const listBucketResultParser = new _fastXmlParser.XMLParser({
    parseTagValue: true,
    // Enable parsing of values
    numberParseOptions: {
      leadingZeros: false,
      // Disable number parsing for values with leading zeros
      hex: false,
      // Disable hex number parsing - Invalid bucket name
      skipLike: /^[0-9]+$/ // Skip number parsing if the value consists entirely of digits
    },
    tagValueProcessor: (tagName, tagValue = '') => {
      // Ensure that the Name tag is always treated as a string
      if (tagName === 'Name') {
        return tagValue.toString();
      }
      return tagValue;
    },
    ignoreAttributes: false // Ensure that all attributes are parsed
  });
  const parsedXmlRes = listBucketResultParser.parse(xml);
  if (!parsedXmlRes.ListAllMyBucketsResult) {
    throw new errors.InvalidXMLError('Missing tag: "ListAllMyBucketsResult"');
  }
  const {
    ListAllMyBucketsResult: {
      Buckets = {}
    } = {}
  } = parsedXmlRes;
  if (Buckets.Bucket) {
    result = (0, _helper.toArray)(Buckets.Bucket).map((bucket = {}) => {
      const {
        Name: bucketName,
        CreationDate
      } = bucket;
      const creationDate = new Date(CreationDate);
      return {
        name: bucketName,
        creationDate
      };
    });
  }
  return result;
}
function parseInitiateMultipart(xml) {
  let xmlobj = (0, _helper.parseXml)(xml);
  if (!xmlobj.InitiateMultipartUploadResult) {
    throw new errors.InvalidXMLError('Missing tag: "InitiateMultipartUploadResult"');
  }
  xmlobj = xmlobj.InitiateMultipartUploadResult;
  if (xmlobj.UploadId) {
    return xmlobj.UploadId;
  }
  throw new errors.InvalidXMLError('Missing tag: "UploadId"');
}
function parseReplicationConfig(xml) {
  const xmlObj = (0, _helper.parseXml)(xml);
  const {
    Role,
    Rule
  } = xmlObj.ReplicationConfiguration;
  return {
    ReplicationConfiguration: {
      role: Role,
      rules: (0, _helper.toArray)(Rule)
    }
  };
}
function parseObjectLegalHoldConfig(xml) {
  const xmlObj = (0, _helper.parseXml)(xml);
  return xmlObj.LegalHold;
}
function parseTagging(xml) {
  const xmlObj = (0, _helper.parseXml)(xml);
  let result = [];
  if (xmlObj.Tagging && xmlObj.Tagging.TagSet && xmlObj.Tagging.TagSet.Tag) {
    const tagResult = xmlObj.Tagging.TagSet.Tag;
    // if it is a single tag convert into an array so that the return value is always an array.
    if (Array.isArray(tagResult)) {
      result = [...tagResult];
    } else {
      result.push(tagResult);
    }
  }
  return result;
}

// parse XML response when a multipart upload is completed
function parseCompleteMultipart(xml) {
  const xmlobj = (0, _helper.parseXml)(xml).CompleteMultipartUploadResult;
  if (xmlobj.Location) {
    const location = (0, _helper.toArray)(xmlobj.Location)[0];
    const bucket = (0, _helper.toArray)(xmlobj.Bucket)[0];
    const key = xmlobj.Key;
    const etag = xmlobj.ETag.replace(/^"/g, '').replace(/"$/g, '').replace(/^&quot;/g, '').replace(/&quot;$/g, '').replace(/^&#34;/g, '').replace(/&#34;$/g, '');
    return {
      location,
      bucket,
      key,
      etag
    };
  }
  // Complete Multipart can return XML Error after a 200 OK response
  if (xmlobj.Code && xmlobj.Message) {
    const errCode = (0, _helper.toArray)(xmlobj.Code)[0];
    const errMessage = (0, _helper.toArray)(xmlobj.Message)[0];
    return {
      errCode,
      errMessage
    };
  }
}
// parse XML response for listing in-progress multipart uploads
function parseListMultipart(xml) {
  const result = {
    prefixes: [],
    uploads: [],
    isTruncated: false,
    nextKeyMarker: '',
    nextUploadIdMarker: ''
  };
  let xmlobj = (0, _helper.parseXml)(xml);
  if (!xmlobj.ListMultipartUploadsResult) {
    throw new errors.InvalidXMLError('Missing tag: "ListMultipartUploadsResult"');
  }
  xmlobj = xmlobj.ListMultipartUploadsResult;
  if (xmlobj.IsTruncated) {
    result.isTruncated = xmlobj.IsTruncated;
  }
  if (xmlobj.NextKeyMarker) {
    result.nextKeyMarker = xmlobj.NextKeyMarker;
  }
  if (xmlobj.NextUploadIdMarker) {
    result.nextUploadIdMarker = xmlobj.nextUploadIdMarker || '';
  }
  if (xmlobj.CommonPrefixes) {
    (0, _helper.toArray)(xmlobj.CommonPrefixes).forEach(prefix => {
      // @ts-expect-error index check
      result.prefixes.push({
        prefix: (0, _helper.sanitizeObjectKey)((0, _helper.toArray)(prefix.Prefix)[0])
      });
    });
  }
  if (xmlobj.Upload) {
    (0, _helper.toArray)(xmlobj.Upload).forEach(upload => {
      const uploadItem = {
        key: upload.Key,
        uploadId: upload.UploadId,
        storageClass: upload.StorageClass,
        initiated: new Date(upload.Initiated)
      };
      if (upload.Initiator) {
        uploadItem.initiator = {
          id: upload.Initiator.ID,
          displayName: upload.Initiator.DisplayName
        };
      }
      if (upload.Owner) {
        uploadItem.owner = {
          id: upload.Owner.ID,
          displayName: upload.Owner.DisplayName
        };
      }
      result.uploads.push(uploadItem);
    });
  }
  return result;
}
function parseObjectLockConfig(xml) {
  const xmlObj = (0, _helper.parseXml)(xml);
  let lockConfigResult = {};
  if (xmlObj.ObjectLockConfiguration) {
    lockConfigResult = {
      objectLockEnabled: xmlObj.ObjectLockConfiguration.ObjectLockEnabled
    };
    let retentionResp;
    if (xmlObj.ObjectLockConfiguration && xmlObj.ObjectLockConfiguration.Rule && xmlObj.ObjectLockConfiguration.Rule.DefaultRetention) {
      retentionResp = xmlObj.ObjectLockConfiguration.Rule.DefaultRetention || {};
      lockConfigResult.mode = retentionResp.Mode;
    }
    if (retentionResp) {
      const isUnitYears = retentionResp.Years;
      if (isUnitYears) {
        lockConfigResult.validity = isUnitYears;
        lockConfigResult.unit = _type.RETENTION_VALIDITY_UNITS.YEARS;
      } else {
        lockConfigResult.validity = retentionResp.Days;
        lockConfigResult.unit = _type.RETENTION_VALIDITY_UNITS.DAYS;
      }
    }
  }
  return lockConfigResult;
}
function parseBucketVersioningConfig(xml) {
  const xmlObj = (0, _helper.parseXml)(xml);
  return xmlObj.VersioningConfiguration;
}

// Used only in selectObjectContent API.
// extractHeaderType extracts the first half of the header message, the header type.
function extractHeaderType(stream) {
  const headerNameLen = Buffer.from(stream.read(1)).readUInt8();
  const headerNameWithSeparator = Buffer.from(stream.read(headerNameLen)).toString();
  const splitBySeparator = (headerNameWithSeparator || '').split(':');
  return splitBySeparator.length >= 1 ? splitBySeparator[1] : '';
}
function extractHeaderValue(stream) {
  const bodyLen = Buffer.from(stream.read(2)).readUInt16BE();
  return Buffer.from(stream.read(bodyLen)).toString();
}
function parseSelectObjectContentResponse(res) {
  const selectResults = new _helpers.SelectResults({}); // will be returned

  const responseStream = (0, _helper.readableStream)(res); // convert byte array to a readable responseStream
  // @ts-ignore
  while (responseStream._readableState.length) {
    // Top level responseStream read tracker.
    let msgCrcAccumulator; // accumulate from start of the message till the message crc start.

    const totalByteLengthBuffer = Buffer.from(responseStream.read(4));
    msgCrcAccumulator = (0, _bufferCrc.default)(totalByteLengthBuffer);
    const headerBytesBuffer = Buffer.from(responseStream.read(4));
    msgCrcAccumulator = (0, _bufferCrc.default)(headerBytesBuffer, msgCrcAccumulator);
    const calculatedPreludeCrc = msgCrcAccumulator.readInt32BE(); // use it to check if any CRC mismatch in header itself.

    const preludeCrcBuffer = Buffer.from(responseStream.read(4)); // read 4 bytes    i.e 4+4 =8 + 4 = 12 ( prelude + prelude crc)
    msgCrcAccumulator = (0, _bufferCrc.default)(preludeCrcBuffer, msgCrcAccumulator);
    const totalMsgLength = totalByteLengthBuffer.readInt32BE();
    const headerLength = headerBytesBuffer.readInt32BE();
    const preludeCrcByteValue = preludeCrcBuffer.readInt32BE();
    if (preludeCrcByteValue !== calculatedPreludeCrc) {
      // Handle Header CRC mismatch Error
      throw new Error(`Header Checksum Mismatch, Prelude CRC of ${preludeCrcByteValue} does not equal expected CRC of ${calculatedPreludeCrc}`);
    }
    const headers = {};
    if (headerLength > 0) {
      const headerBytes = Buffer.from(responseStream.read(headerLength));
      msgCrcAccumulator = (0, _bufferCrc.default)(headerBytes, msgCrcAccumulator);
      const headerReaderStream = (0, _helper.readableStream)(headerBytes);
      // @ts-ignore
      while (headerReaderStream._readableState.length) {
        const headerTypeName = extractHeaderType(headerReaderStream);
        headerReaderStream.read(1); // just read and ignore it.
        if (headerTypeName) {
          headers[headerTypeName] = extractHeaderValue(headerReaderStream);
        }
      }
    }
    let payloadStream;
    const payLoadLength = totalMsgLength - headerLength - 16;
    if (payLoadLength > 0) {
      const payLoadBuffer = Buffer.from(responseStream.read(payLoadLength));
      msgCrcAccumulator = (0, _bufferCrc.default)(payLoadBuffer, msgCrcAccumulator);
      // read the checksum early and detect any mismatch so we can avoid unnecessary further processing.
      const messageCrcByteValue = Buffer.from(responseStream.read(4)).readInt32BE();
      const calculatedCrc = msgCrcAccumulator.readInt32BE();
      // Handle message CRC Error
      if (messageCrcByteValue !== calculatedCrc) {
        throw new Error(`Message Checksum Mismatch, Message CRC of ${messageCrcByteValue} does not equal expected CRC of ${calculatedCrc}`);
      }
      payloadStream = (0, _helper.readableStream)(payLoadBuffer);
    }
    const messageType = headers['message-type'];
    switch (messageType) {
      case 'error':
        {
          const errorMessage = headers['error-code'] + ':"' + headers['error-message'] + '"';
          throw new Error(errorMessage);
        }
      case 'event':
        {
          const contentType = headers['content-type'];
          const eventType = headers['event-type'];
          switch (eventType) {
            case 'End':
              {
                selectResults.setResponse(res);
                return selectResults;
              }
            case 'Records':
              {
                var _payloadStream;
                const readData = (_payloadStream = payloadStream) === null || _payloadStream === void 0 ? void 0 : _payloadStream.read(payLoadLength);
                selectResults.setRecords(readData);
                break;
              }
            case 'Progress':
              {
                switch (contentType) {
                  case 'text/xml':
                    {
                      var _payloadStream2;
                      const progressData = (_payloadStream2 = payloadStream) === null || _payloadStream2 === void 0 ? void 0 : _payloadStream2.read(payLoadLength);
                      selectResults.setProgress(progressData.toString());
                      break;
                    }
                  default:
                    {
                      const errorMessage = `Unexpected content-type ${contentType} sent for event-type Progress`;
                      throw new Error(errorMessage);
                    }
                }
              }
              break;
            case 'Stats':
              {
                switch (contentType) {
                  case 'text/xml':
                    {
                      var _payloadStream3;
                      const statsData = (_payloadStream3 = payloadStream) === null || _payloadStream3 === void 0 ? void 0 : _payloadStream3.read(payLoadLength);
                      selectResults.setStats(statsData.toString());
                      break;
                    }
                  default:
                    {
                      const errorMessage = `Unexpected content-type ${contentType} sent for event-type Stats`;
                      throw new Error(errorMessage);
                    }
                }
              }
              break;
            default:
              {
                // Continuation message: Not sure if it is supported. did not find a reference or any message in response.
                // It does not have a payload.
                const warningMessage = `Un implemented event detected  ${messageType}.`;
                // eslint-disable-next-line no-console
                console.warn(warningMessage);
              }
          }
        }
    }
  }
}
function parseLifecycleConfig(xml) {
  const xmlObj = (0, _helper.parseXml)(xml);
  const lifecycleConfig = xmlObj.LifecycleConfiguration;
  if (lifecycleConfig && lifecycleConfig.Rule) {
    // A single <Rule> is parsed as an object; normalize to an array so the
    // result matches the LifecycleConfig type and the multiple-rules case. See #1407.
    lifecycleConfig.Rule = (0, _helper.toArray)(lifecycleConfig.Rule);
  }
  return lifecycleConfig;
}
function parseBucketEncryptionConfig(xml) {
  return (0, _helper.parseXml)(xml);
}
function parseObjectRetentionConfig(xml) {
  const xmlObj = (0, _helper.parseXml)(xml);
  const retentionConfig = xmlObj.Retention;
  return {
    mode: retentionConfig.Mode,
    retainUntilDate: retentionConfig.RetainUntilDate
  };
}
function removeObjectsParser(xml) {
  const xmlObj = (0, _helper.parseXml)(xml);
  if (xmlObj.DeleteResult && xmlObj.DeleteResult.Error) {
    // return errors as array always. as the response is object in case of single object passed in removeObjects
    return (0, _helper.toArray)(xmlObj.DeleteResult.Error);
  }
  return [];
}

// parse XML response for copy object
function parseCopyObject(xml) {
  const result = {
    etag: '',
    lastModified: ''
  };
  let xmlobj = (0, _helper.parseXml)(xml);
  if (!xmlobj.CopyObjectResult) {
    throw new errors.InvalidXMLError('Missing tag: "CopyObjectResult"');
  }
  xmlobj = xmlobj.CopyObjectResult;
  if (xmlobj.ETag) {
    result.etag = xmlobj.ETag.replace(/^"/g, '').replace(/"$/g, '').replace(/^&quot;/g, '').replace(/&quot;$/g, '').replace(/^&#34;/g, '').replace(/&#34;$/g, '');
  }
  if (xmlobj.LastModified) {
    result.lastModified = new Date(xmlobj.LastModified);
  }
  return result;
}
const formatObjInfo = (content, opts = {}) => {
  const {
    Key,
    LastModified,
    ETag,
    Size,
    VersionId,
    IsLatest
  } = content;
  if (!(0, _helper.isObject)(opts)) {
    opts = {};
  }
  const name = (0, _helper.sanitizeObjectKey)((0, _helper.toArray)(Key)[0] || '');
  const lastModified = LastModified ? new Date((0, _helper.toArray)(LastModified)[0] || '') : undefined;
  const etag = (0, _helper.sanitizeETag)((0, _helper.toArray)(ETag)[0] || '');
  const size = (0, _helper.sanitizeSize)(Size || '');
  return {
    name,
    lastModified,
    etag,
    size,
    versionId: VersionId,
    isLatest: IsLatest,
    isDeleteMarker: opts.IsDeleteMarker ? opts.IsDeleteMarker : false
  };
};

// parse XML response for list objects in a bucket
function parseListObjects(xml) {
  const result = {
    objects: [],
    isTruncated: false,
    nextMarker: undefined,
    versionIdMarker: undefined,
    keyMarker: undefined
  };
  let isTruncated = false;
  let nextMarker;
  const xmlobj = fxpWithoutNumParser.parse(xml);
  const parseCommonPrefixesEntity = commonPrefixEntry => {
    if (commonPrefixEntry) {
      (0, _helper.toArray)(commonPrefixEntry).forEach(commonPrefix => {
        result.objects.push({
          prefix: (0, _helper.sanitizeObjectKey)((0, _helper.toArray)(commonPrefix.Prefix)[0] || ''),
          size: 0
        });
      });
    }
  };
  const listBucketResult = xmlobj.ListBucketResult;
  const listVersionsResult = xmlobj.ListVersionsResult;
  if (listBucketResult) {
    if (listBucketResult.IsTruncated) {
      isTruncated = listBucketResult.IsTruncated;
    }
    if (listBucketResult.Contents) {
      (0, _helper.toArray)(listBucketResult.Contents).forEach(content => {
        const name = (0, _helper.sanitizeObjectKey)((0, _helper.toArray)(content.Key)[0] || '');
        const lastModified = new Date((0, _helper.toArray)(content.LastModified)[0] || '');
        const etag = (0, _helper.sanitizeETag)((0, _helper.toArray)(content.ETag)[0] || '');
        const size = (0, _helper.sanitizeSize)(content.Size || '');
        result.objects.push({
          name,
          lastModified,
          etag,
          size
        });
      });
    }
    if (listBucketResult.Marker) {
      nextMarker = listBucketResult.Marker;
    }
    if (listBucketResult.NextMarker) {
      nextMarker = listBucketResult.NextMarker;
    } else if (isTruncated && result.objects.length > 0) {
      var _result$objects;
      nextMarker = (_result$objects = result.objects[result.objects.length - 1]) === null || _result$objects === void 0 ? void 0 : _result$objects.name;
    }
    if (listBucketResult.CommonPrefixes) {
      parseCommonPrefixesEntity(listBucketResult.CommonPrefixes);
    }
  }
  if (listVersionsResult) {
    if (listVersionsResult.IsTruncated) {
      isTruncated = listVersionsResult.IsTruncated;
    }
    if (listVersionsResult.Version) {
      (0, _helper.toArray)(listVersionsResult.Version).forEach(content => {
        result.objects.push(formatObjInfo(content));
      });
    }
    if (listVersionsResult.DeleteMarker) {
      (0, _helper.toArray)(listVersionsResult.DeleteMarker).forEach(content => {
        result.objects.push(formatObjInfo(content, {
          IsDeleteMarker: true
        }));
      });
    }
    if (listVersionsResult.NextKeyMarker) {
      result.keyMarker = listVersionsResult.NextKeyMarker;
    }
    if (listVersionsResult.NextVersionIdMarker) {
      result.versionIdMarker = listVersionsResult.NextVersionIdMarker;
    }
    if (listVersionsResult.CommonPrefixes) {
      parseCommonPrefixesEntity(listVersionsResult.CommonPrefixes);
    }
  }
  result.isTruncated = isTruncated;
  if (isTruncated) {
    result.nextMarker = nextMarker;
  }
  return result;
}
function uploadPartParser(xml) {
  const xmlObj = (0, _helper.parseXml)(xml);
  const respEl = xmlObj.CopyPartResult;
  return respEl;
}
//# sourceMappingURL=data:application/json;charset=utf-8;base64,eyJ2ZXJzaW9uIjozLCJuYW1lcyI6WyJfYnVmZmVyQ3JjIiwiX2ludGVyb3BSZXF1aXJlRGVmYXVsdCIsInJlcXVpcmUiLCJfZmFzdFhtbFBhcnNlciIsImVycm9ycyIsIl9pbnRlcm9wUmVxdWlyZVdpbGRjYXJkIiwiX2hlbHBlcnMiLCJfaGVscGVyIiwiX3Jlc3BvbnNlIiwiX3R5cGUiLCJlIiwidCIsIldlYWtNYXAiLCJyIiwibiIsIl9fZXNNb2R1bGUiLCJvIiwiaSIsImYiLCJfX3Byb3RvX18iLCJkZWZhdWx0IiwiaGFzIiwiZ2V0Iiwic2V0IiwiaGFzT3duUHJvcGVydHkiLCJjYWxsIiwiT2JqZWN0IiwiZGVmaW5lUHJvcGVydHkiLCJnZXRPd25Qcm9wZXJ0eURlc2NyaXB0b3IiLCJwYXJzZUJ1Y2tldFJlZ2lvbiIsInhtbCIsInBhcnNlWG1sIiwiTG9jYXRpb25Db25zdHJhaW50IiwiZnhwIiwiWE1MUGFyc2VyIiwiZnhwV2l0aG91dE51bVBhcnNlciIsIm51bWJlclBhcnNlT3B0aW9ucyIsInNraXBMaWtlIiwicGFyc2VFcnJvciIsImhlYWRlckluZm8iLCJ4bWxFcnIiLCJ4bWxPYmoiLCJwYXJzZSIsIkVycm9yIiwiUzNFcnJvciIsImVudHJpZXMiLCJmb3JFYWNoIiwia2V5IiwidmFsdWUiLCJ0b0xvd2VyQ2FzZSIsInBhcnNlUmVzcG9uc2VFcnJvciIsInJlc3BvbnNlIiwic3RhdHVzQ29kZSIsImNvZGUiLCJtZXNzYWdlIiwiaEVyckNvZGUiLCJoZWFkZXJzIiwiaEVyckRlc2MiLCJhbXpSZXF1ZXN0aWQiLCJhbXpJZDIiLCJhbXpCdWNrZXRSZWdpb24iLCJ4bWxTdHJpbmciLCJyZWFkQXNTdHJpbmciLCJjYXVzZSIsInBhcnNlTGlzdE9iamVjdHNWMldpdGhNZXRhZGF0YSIsInJlc3VsdCIsIm9iamVjdHMiLCJpc1RydW5jYXRlZCIsIm5leHRDb250aW51YXRpb25Ub2tlbiIsInhtbG9iaiIsIkxpc3RCdWNrZXRSZXN1bHQiLCJJbnZhbGlkWE1MRXJyb3IiLCJJc1RydW5jYXRlZCIsIk5leHRDb250aW51YXRpb25Ub2tlbiIsIkNvbnRlbnRzIiwidG9BcnJheSIsImNvbnRlbnQiLCJuYW1lIiwic2FuaXRpemVPYmplY3RLZXkiLCJLZXkiLCJsYXN0TW9kaWZpZWQiLCJEYXRlIiwiTGFzdE1vZGlmaWVkIiwiZXRhZyIsInNhbml0aXplRVRhZyIsIkVUYWciLCJzaXplIiwiU2l6ZSIsInRhZ3MiLCJVc2VyVGFncyIsInNwbGl0IiwidGFnIiwibWV0YWRhdGEiLCJVc2VyTWV0YWRhdGEiLCJwdXNoIiwiQ29tbW9uUHJlZml4ZXMiLCJjb21tb25QcmVmaXgiLCJwcmVmaXgiLCJQcmVmaXgiLCJwYXJzZUxpc3RPYmplY3RzVjIiLCJwYXJzZUJ1Y2tldE5vdGlmaWNhdGlvbiIsIlRvcGljQ29uZmlndXJhdGlvbiIsIlF1ZXVlQ29uZmlndXJhdGlvbiIsIkNsb3VkRnVuY3Rpb25Db25maWd1cmF0aW9uIiwiZ2VuRXZlbnRzIiwiZXZlbnRzIiwiZ2VuRmlsdGVyUnVsZXMiLCJmaWx0ZXJzIiwiX2ZpbHRlckFyciQiLCJydWxlcyIsImZpbHRlckFyciIsIlMzS2V5IiwiX3MzS2V5QXJyJCIsInMzS2V5QXJyIiwiRmlsdGVyUnVsZSIsInJ1bGUiLCJOYW1lIiwiVmFsdWUiLCJOb3RpZmljYXRpb25Db25maWd1cmF0aW9uIiwiY29uZmlnIiwiSWQiLCJUb3BpYyIsIkV2ZW50IiwiRmlsdGVyIiwiUXVldWUiLCJDbG91ZEZ1bmN0aW9uIiwicGFyc2VMaXN0UGFydHMiLCJwYXJ0cyIsIm1hcmtlciIsIkxpc3RQYXJ0c1Jlc3VsdCIsIk5leHRQYXJ0TnVtYmVyTWFya2VyIiwiUGFydCIsInAiLCJwYXJ0IiwicGFyc2VJbnQiLCJQYXJ0TnVtYmVyIiwicmVwbGFjZSIsInBhcnNlTGlzdEJ1Y2tldCIsImxpc3RCdWNrZXRSZXN1bHRQYXJzZXIiLCJwYXJzZVRhZ1ZhbHVlIiwibGVhZGluZ1plcm9zIiwiaGV4IiwidGFnVmFsdWVQcm9jZXNzb3IiLCJ0YWdOYW1lIiwidGFnVmFsdWUiLCJ0b1N0cmluZyIsImlnbm9yZUF0dHJpYnV0ZXMiLCJwYXJzZWRYbWxSZXMiLCJMaXN0QWxsTXlCdWNrZXRzUmVzdWx0IiwiQnVja2V0cyIsIkJ1Y2tldCIsIm1hcCIsImJ1Y2tldCIsImJ1Y2tldE5hbWUiLCJDcmVhdGlvbkRhdGUiLCJjcmVhdGlvbkRhdGUiLCJwYXJzZUluaXRpYXRlTXVsdGlwYXJ0IiwiSW5pdGlhdGVNdWx0aXBhcnRVcGxvYWRSZXN1bHQiLCJVcGxvYWRJZCIsInBhcnNlUmVwbGljYXRpb25Db25maWciLCJSb2xlIiwiUnVsZSIsIlJlcGxpY2F0aW9uQ29uZmlndXJhdGlvbiIsInJvbGUiLCJwYXJzZU9iamVjdExlZ2FsSG9sZENvbmZpZyIsIkxlZ2FsSG9sZCIsInBhcnNlVGFnZ2luZyIsIlRhZ2dpbmciLCJUYWdTZXQiLCJUYWciLCJ0YWdSZXN1bHQiLCJBcnJheSIsImlzQXJyYXkiLCJwYXJzZUNvbXBsZXRlTXVsdGlwYXJ0IiwiQ29tcGxldGVNdWx0aXBhcnRVcGxvYWRSZXN1bHQiLCJMb2NhdGlvbiIsImxvY2F0aW9uIiwiQ29kZSIsIk1lc3NhZ2UiLCJlcnJDb2RlIiwiZXJyTWVzc2FnZSIsInBhcnNlTGlzdE11bHRpcGFydCIsInByZWZpeGVzIiwidXBsb2FkcyIsIm5leHRLZXlNYXJrZXIiLCJuZXh0VXBsb2FkSWRNYXJrZXIiLCJMaXN0TXVsdGlwYXJ0VXBsb2Fkc1Jlc3VsdCIsIk5leHRLZXlNYXJrZXIiLCJOZXh0VXBsb2FkSWRNYXJrZXIiLCJVcGxvYWQiLCJ1cGxvYWQiLCJ1cGxvYWRJdGVtIiwidXBsb2FkSWQiLCJzdG9yYWdlQ2xhc3MiLCJTdG9yYWdlQ2xhc3MiLCJpbml0aWF0ZWQiLCJJbml0aWF0ZWQiLCJJbml0aWF0b3IiLCJpbml0aWF0b3IiLCJpZCIsIklEIiwiZGlzcGxheU5hbWUiLCJEaXNwbGF5TmFtZSIsIk93bmVyIiwib3duZXIiLCJwYXJzZU9iamVjdExvY2tDb25maWciLCJsb2NrQ29uZmlnUmVzdWx0IiwiT2JqZWN0TG9ja0NvbmZpZ3VyYXRpb24iLCJvYmplY3RMb2NrRW5hYmxlZCIsIk9iamVjdExvY2tFbmFibGVkIiwicmV0ZW50aW9uUmVzcCIsIkRlZmF1bHRSZXRlbnRpb24iLCJtb2RlIiwiTW9kZSIsImlzVW5pdFllYXJzIiwiWWVhcnMiLCJ2YWxpZGl0eSIsInVuaXQiLCJSRVRFTlRJT05fVkFMSURJVFlfVU5JVFMiLCJZRUFSUyIsIkRheXMiLCJEQVlTIiwicGFyc2VCdWNrZXRWZXJzaW9uaW5nQ29uZmlnIiwiVmVyc2lvbmluZ0NvbmZpZ3VyYXRpb24iLCJleHRyYWN0SGVhZGVyVHlwZSIsInN0cmVhbSIsImhlYWRlck5hbWVMZW4iLCJCdWZmZXIiLCJmcm9tIiwicmVhZCIsInJlYWRVSW50OCIsImhlYWRlck5hbWVXaXRoU2VwYXJhdG9yIiwic3BsaXRCeVNlcGFyYXRvciIsImxlbmd0aCIsImV4dHJhY3RIZWFkZXJWYWx1ZSIsImJvZHlMZW4iLCJyZWFkVUludDE2QkUiLCJwYXJzZVNlbGVjdE9iamVjdENvbnRlbnRSZXNwb25zZSIsInJlcyIsInNlbGVjdFJlc3VsdHMiLCJTZWxlY3RSZXN1bHRzIiwicmVzcG9uc2VTdHJlYW0iLCJyZWFkYWJsZVN0cmVhbSIsIl9yZWFkYWJsZVN0YXRlIiwibXNnQ3JjQWNjdW11bGF0b3IiLCJ0b3RhbEJ5dGVMZW5ndGhCdWZmZXIiLCJjcmMzMiIsImhlYWRlckJ5dGVzQnVmZmVyIiwiY2FsY3VsYXRlZFByZWx1ZGVDcmMiLCJyZWFkSW50MzJCRSIsInByZWx1ZGVDcmNCdWZmZXIiLCJ0b3RhbE1zZ0xlbmd0aCIsImhlYWRlckxlbmd0aCIsInByZWx1ZGVDcmNCeXRlVmFsdWUiLCJoZWFkZXJCeXRlcyIsImhlYWRlclJlYWRlclN0cmVhbSIsImhlYWRlclR5cGVOYW1lIiwicGF5bG9hZFN0cmVhbSIsInBheUxvYWRMZW5ndGgiLCJwYXlMb2FkQnVmZmVyIiwibWVzc2FnZUNyY0J5dGVWYWx1ZSIsImNhbGN1bGF0ZWRDcmMiLCJtZXNzYWdlVHlwZSIsImVycm9yTWVzc2FnZSIsImNvbnRlbnRUeXBlIiwiZXZlbnRUeXBlIiwic2V0UmVzcG9uc2UiLCJfcGF5bG9hZFN0cmVhbSIsInJlYWREYXRhIiwic2V0UmVjb3JkcyIsIl9wYXlsb2FkU3RyZWFtMiIsInByb2dyZXNzRGF0YSIsInNldFByb2dyZXNzIiwiX3BheWxvYWRTdHJlYW0zIiwic3RhdHNEYXRhIiwic2V0U3RhdHMiLCJ3YXJuaW5nTWVzc2FnZSIsImNvbnNvbGUiLCJ3YXJuIiwicGFyc2VMaWZlY3ljbGVDb25maWciLCJsaWZlY3ljbGVDb25maWciLCJMaWZlY3ljbGVDb25maWd1cmF0aW9uIiwicGFyc2VCdWNrZXRFbmNyeXB0aW9uQ29uZmlnIiwicGFyc2VPYmplY3RSZXRlbnRpb25Db25maWciLCJyZXRlbnRpb25Db25maWciLCJSZXRlbnRpb24iLCJyZXRhaW5VbnRpbERhdGUiLCJSZXRhaW5VbnRpbERhdGUiLCJyZW1vdmVPYmplY3RzUGFyc2VyIiwiRGVsZXRlUmVzdWx0IiwicGFyc2VDb3B5T2JqZWN0IiwiQ29weU9iamVjdFJlc3VsdCIsImZvcm1hdE9iakluZm8iLCJvcHRzIiwiVmVyc2lvbklkIiwiSXNMYXRlc3QiLCJpc09iamVjdCIsInVuZGVmaW5lZCIsInNhbml0aXplU2l6ZSIsInZlcnNpb25JZCIsImlzTGF0ZXN0IiwiaXNEZWxldGVNYXJrZXIiLCJJc0RlbGV0ZU1hcmtlciIsInBhcnNlTGlzdE9iamVjdHMiLCJuZXh0TWFya2VyIiwidmVyc2lvbklkTWFya2VyIiwia2V5TWFya2VyIiwicGFyc2VDb21tb25QcmVmaXhlc0VudGl0eSIsImNvbW1vblByZWZpeEVudHJ5IiwibGlzdEJ1Y2tldFJlc3VsdCIsImxpc3RWZXJzaW9uc1Jlc3VsdCIsIkxpc3RWZXJzaW9uc1Jlc3VsdCIsIk1hcmtlciIsIk5leHRNYXJrZXIiLCJfcmVzdWx0JG9iamVjdHMiLCJWZXJzaW9uIiwiRGVsZXRlTWFya2VyIiwiTmV4dFZlcnNpb25JZE1hcmtlciIsInVwbG9hZFBhcnRQYXJzZXIiLCJyZXNwRWwiLCJDb3B5UGFydFJlc3VsdCJdLCJzb3VyY2VzIjpbInhtbC1wYXJzZXIudHMiXSwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHR5cGUgKiBhcyBodHRwIGZyb20gJ25vZGU6aHR0cCdcbmltcG9ydCB0eXBlIHN0cmVhbSBmcm9tICdub2RlOnN0cmVhbSdcblxuaW1wb3J0IGNyYzMyIGZyb20gJ2J1ZmZlci1jcmMzMidcbmltcG9ydCB7IFhNTFBhcnNlciB9IGZyb20gJ2Zhc3QteG1sLXBhcnNlcidcblxuaW1wb3J0ICogYXMgZXJyb3JzIGZyb20gJy4uL2Vycm9ycy50cydcbmltcG9ydCB7IFNlbGVjdFJlc3VsdHMgfSBmcm9tICcuLi9oZWxwZXJzLnRzJ1xuaW1wb3J0IHsgaXNPYmplY3QsIHBhcnNlWG1sLCByZWFkYWJsZVN0cmVhbSwgc2FuaXRpemVFVGFnLCBzYW5pdGl6ZU9iamVjdEtleSwgc2FuaXRpemVTaXplLCB0b0FycmF5IH0gZnJvbSAnLi9oZWxwZXIudHMnXG5pbXBvcnQgeyByZWFkQXNTdHJpbmcgfSBmcm9tICcuL3Jlc3BvbnNlLnRzJ1xuaW1wb3J0IHR5cGUge1xuICBCdWNrZXRJdGVtRnJvbUxpc3QsXG4gIEJ1Y2tldEl0ZW1XaXRoTWV0YWRhdGEsXG4gIENsb3VkRnVuY3Rpb25Db25maWdFbnRyeSxcbiAgQ29tbW9uUHJlZml4LFxuICBDb3B5T2JqZWN0UmVzdWx0VjEsXG4gIExpc3RCdWNrZXRSZXN1bHRWMSxcbiAgTGlzdE9iamVjdFYyUmVzLFxuICBOb3RpZmljYXRpb25Db25maWdSZXN1bHQsXG4gIE9iamVjdEluZm8sXG4gIE9iamVjdExvY2tJbmZvLFxuICBPYmplY3RSb3dFbnRyeSxcbiAgUXVldWVDb25maWdFbnRyeSxcbiAgUmVwbGljYXRpb25Db25maWcsXG4gIFRhZyxcbiAgVGFncyxcbiAgVG9waWNDb25maWdFbnRyeSxcbn0gZnJvbSAnLi90eXBlLnRzJ1xuaW1wb3J0IHsgUkVURU5USU9OX1ZBTElESVRZX1VOSVRTIH0gZnJvbSAnLi90eXBlLnRzJ1xuXG4vLyBwYXJzZSBYTUwgcmVzcG9uc2UgZm9yIGJ1Y2tldCByZWdpb25cbmV4cG9ydCBmdW5jdGlvbiBwYXJzZUJ1Y2tldFJlZ2lvbih4bWw6IHN0cmluZyk6IHN0cmluZyB7XG4gIC8vIHJldHVybiByZWdpb24gaW5mb3JtYXRpb25cbiAgcmV0dXJuIHBhcnNlWG1sKHhtbCkuTG9jYXRpb25Db25zdHJhaW50XG59XG5cbmNvbnN0IGZ4cCA9IG5ldyBYTUxQYXJzZXIoKVxuXG5jb25zdCBmeHBXaXRob3V0TnVtUGFyc2VyID0gbmV3IFhNTFBhcnNlcih7XG4gIC8vIEB0cy1pZ25vcmVcbiAgbnVtYmVyUGFyc2VPcHRpb25zOiB7XG4gICAgc2tpcExpa2U6IC8uLyxcbiAgfSxcbn0pXG5cbi8vIFBhcnNlIFhNTCBhbmQgcmV0dXJuIGluZm9ybWF0aW9uIGFzIEphdmFzY3JpcHQgdHlwZXNcbi8vIHBhcnNlIGVycm9yIFhNTCByZXNwb25zZVxuZXhwb3J0IGZ1bmN0aW9uIHBhcnNlRXJyb3IoeG1sOiBzdHJpbmcsIGhlYWRlckluZm86IFJlY29yZDxzdHJpbmcsIHVua25vd24+KSB7XG4gIGxldCB4bWxFcnIgPSB7fVxuICBjb25zdCB4bWxPYmogPSBmeHAucGFyc2UoeG1sKVxuICBpZiAoeG1sT2JqLkVycm9yKSB7XG4gICAgeG1sRXJyID0geG1sT2JqLkVycm9yXG4gIH1cbiAgY29uc3QgZSA9IG5ldyBlcnJvcnMuUzNFcnJvcigpIGFzIHVua25vd24gYXMgUmVjb3JkPHN0cmluZywgdW5rbm93bj5cbiAgT2JqZWN0LmVudHJpZXMoeG1sRXJyKS5mb3JFYWNoKChba2V5LCB2YWx1ZV0pID0+IHtcbiAgICBlW2tleS50b0xvd2VyQ2FzZSgpXSA9IHZhbHVlXG4gIH0pXG4gIE9iamVjdC5lbnRyaWVzKGhlYWRlckluZm8pLmZvckVhY2goKFtrZXksIHZhbHVlXSkgPT4ge1xuICAgIGVba2V5XSA9IHZhbHVlXG4gIH0pXG4gIHJldHVybiBlXG59XG5cbi8vIEdlbmVyYXRlcyBhbiBFcnJvciBvYmplY3QgZGVwZW5kaW5nIG9uIGh0dHAgc3RhdHVzQ29kZSBhbmQgWE1MIGJvZHlcbmV4cG9ydCBhc3luYyBmdW5jdGlvbiBwYXJzZVJlc3BvbnNlRXJyb3IocmVzcG9uc2U6IGh0dHAuSW5jb21pbmdNZXNzYWdlKTogUHJvbWlzZTxSZWNvcmQ8c3RyaW5nLCBzdHJpbmc+PiB7XG4gIGNvbnN0IHN0YXR1c0NvZGUgPSByZXNwb25zZS5zdGF0dXNDb2RlXG4gIGxldCBjb2RlID0gJycsXG4gICAgbWVzc2FnZSA9ICcnXG4gIGlmIChzdGF0dXNDb2RlID09PSAzMDEpIHtcbiAgICBjb2RlID0gJ01vdmVkUGVybWFuZW50bHknXG4gICAgbWVzc2FnZSA9ICdNb3ZlZCBQZXJtYW5lbnRseSdcbiAgfSBlbHNlIGlmIChzdGF0dXNDb2RlID09PSAzMDcpIHtcbiAgICBjb2RlID0gJ1RlbXBvcmFyeVJlZGlyZWN0J1xuICAgIG1lc3NhZ2UgPSAnQXJlIHlvdSB1c2luZyB0aGUgY29ycmVjdCBlbmRwb2ludCBVUkw/J1xuICB9IGVsc2UgaWYgKHN0YXR1c0NvZGUgPT09IDQwMykge1xuICAgIGNvZGUgPSAnQWNjZXNzRGVuaWVkJ1xuICAgIG1lc3NhZ2UgPSAnVmFsaWQgYW5kIGF1dGhvcml6ZWQgY3JlZGVudGlhbHMgcmVxdWlyZWQnXG4gIH0gZWxzZSBpZiAoc3RhdHVzQ29kZSA9PT0gNDA0KSB7XG4gICAgY29kZSA9ICdOb3RGb3VuZCdcbiAgICBtZXNzYWdlID0gJ05vdCBGb3VuZCdcbiAgfSBlbHNlIGlmIChzdGF0dXNDb2RlID09PSA0MDUpIHtcbiAgICBjb2RlID0gJ01ldGhvZE5vdEFsbG93ZWQnXG4gICAgbWVzc2FnZSA9ICdNZXRob2QgTm90IEFsbG93ZWQnXG4gIH0gZWxzZSBpZiAoc3RhdHVzQ29kZSA9PT0gNTAxKSB7XG4gICAgY29kZSA9ICdNZXRob2ROb3RBbGxvd2VkJ1xuICAgIG1lc3NhZ2UgPSAnTWV0aG9kIE5vdCBBbGxvd2VkJ1xuICB9IGVsc2UgaWYgKHN0YXR1c0NvZGUgPT09IDUwMykge1xuICAgIGNvZGUgPSAnU2xvd0Rvd24nXG4gICAgbWVzc2FnZSA9ICdQbGVhc2UgcmVkdWNlIHlvdXIgcmVxdWVzdCByYXRlLidcbiAgfSBlbHNlIHtcbiAgICBjb25zdCBoRXJyQ29kZSA9IHJlc3BvbnNlLmhlYWRlcnNbJ3gtbWluaW8tZXJyb3ItY29kZSddIGFzIHN0cmluZ1xuICAgIGNvbnN0IGhFcnJEZXNjID0gcmVzcG9uc2UuaGVhZGVyc1sneC1taW5pby1lcnJvci1kZXNjJ10gYXMgc3RyaW5nXG5cbiAgICBpZiAoaEVyckNvZGUgJiYgaEVyckRlc2MpIHtcbiAgICAgIGNvZGUgPSBoRXJyQ29kZVxuICAgICAgbWVzc2FnZSA9IGhFcnJEZXNjXG4gICAgfVxuICB9XG4gIGNvbnN0IGhlYWRlckluZm86IFJlY29yZDxzdHJpbmcsIHN0cmluZyB8IHVuZGVmaW5lZCB8IG51bGw+ID0ge31cbiAgLy8gQSB2YWx1ZSBjcmVhdGVkIGJ5IFMzIGNvbXBhdGlibGUgc2VydmVyIHRoYXQgdW5pcXVlbHkgaWRlbnRpZmllcyB0aGUgcmVxdWVzdC5cbiAgaGVhZGVySW5mby5hbXpSZXF1ZXN0aWQgPSByZXNwb25zZS5oZWFkZXJzWyd4LWFtei1yZXF1ZXN0LWlkJ10gYXMgc3RyaW5nIHwgdW5kZWZpbmVkXG4gIC8vIEEgc3BlY2lhbCB0b2tlbiB0aGF0IGhlbHBzIHRyb3VibGVzaG9vdCBBUEkgcmVwbGllcyBhbmQgaXNzdWVzLlxuICBoZWFkZXJJbmZvLmFteklkMiA9IHJlc3BvbnNlLmhlYWRlcnNbJ3gtYW16LWlkLTInXSBhcyBzdHJpbmcgfCB1bmRlZmluZWRcblxuICAvLyBSZWdpb24gd2hlcmUgdGhlIGJ1Y2tldCBpcyBsb2NhdGVkLiBUaGlzIGhlYWRlciBpcyByZXR1cm5lZCBvbmx5XG4gIC8vIGluIEhFQUQgYnVja2V0IGFuZCBMaXN0T2JqZWN0cyByZXNwb25zZS5cbiAgaGVhZGVySW5mby5hbXpCdWNrZXRSZWdpb24gPSByZXNwb25zZS5oZWFkZXJzWyd4LWFtei1idWNrZXQtcmVnaW9uJ10gYXMgc3RyaW5nIHwgdW5kZWZpbmVkXG5cbiAgY29uc3QgeG1sU3RyaW5nID0gYXdhaXQgcmVhZEFzU3RyaW5nKHJlc3BvbnNlKVxuXG4gIGlmICh4bWxTdHJpbmcpIHtcbiAgICB0aHJvdyBwYXJzZUVycm9yKHhtbFN0cmluZywgaGVhZGVySW5mbylcbiAgfVxuXG4gIC8vIE1lc3NhZ2Ugc2hvdWxkIGJlIGluc3RhbnRpYXRlZCBmb3IgZWFjaCBTM0Vycm9ycy5cbiAgY29uc3QgZSA9IG5ldyBlcnJvcnMuUzNFcnJvcihtZXNzYWdlLCB7IGNhdXNlOiBoZWFkZXJJbmZvIH0pXG4gIC8vIFMzIEVycm9yIGNvZGUuXG4gIGUuY29kZSA9IGNvZGVcbiAgT2JqZWN0LmVudHJpZXMoaGVhZGVySW5mbykuZm9yRWFjaCgoW2tleSwgdmFsdWVdKSA9PiB7XG4gICAgLy8gQHRzLWV4cGVjdC1lcnJvciBmb3JjZSBzZXQgZXJyb3IgcHJvcGVydGllc1xuICAgIGVba2V5XSA9IHZhbHVlXG4gIH0pXG5cbiAgdGhyb3cgZVxufVxuXG4vKipcbiAqIHBhcnNlIFhNTCByZXNwb25zZSBmb3IgbGlzdCBvYmplY3RzIHYyIHdpdGggbWV0YWRhdGEgaW4gYSBidWNrZXRcbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIHBhcnNlTGlzdE9iamVjdHNWMldpdGhNZXRhZGF0YSh4bWw6IHN0cmluZykge1xuICBjb25zdCByZXN1bHQ6IHtcbiAgICBvYmplY3RzOiBBcnJheTxCdWNrZXRJdGVtV2l0aE1ldGFkYXRhPlxuICAgIGlzVHJ1bmNhdGVkOiBib29sZWFuXG4gICAgbmV4dENvbnRpbnVhdGlvblRva2VuOiBzdHJpbmdcbiAgfSA9IHtcbiAgICBvYmplY3RzOiBbXSxcbiAgICBpc1RydW5jYXRlZDogZmFsc2UsXG4gICAgbmV4dENvbnRpbnVhdGlvblRva2VuOiAnJyxcbiAgfVxuXG4gIGxldCB4bWxvYmogPSBwYXJzZVhtbCh4bWwpXG4gIGlmICgheG1sb2JqLkxpc3RCdWNrZXRSZXN1bHQpIHtcbiAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRYTUxFcnJvcignTWlzc2luZyB0YWc6IFwiTGlzdEJ1Y2tldFJlc3VsdFwiJylcbiAgfVxuICB4bWxvYmogPSB4bWxvYmouTGlzdEJ1Y2tldFJlc3VsdFxuICBpZiAoeG1sb2JqLklzVHJ1bmNhdGVkKSB7XG4gICAgcmVzdWx0LmlzVHJ1bmNhdGVkID0geG1sb2JqLklzVHJ1bmNhdGVkXG4gIH1cbiAgaWYgKHhtbG9iai5OZXh0Q29udGludWF0aW9uVG9rZW4pIHtcbiAgICByZXN1bHQubmV4dENvbnRpbnVhdGlvblRva2VuID0geG1sb2JqLk5leHRDb250aW51YXRpb25Ub2tlblxuICB9XG5cbiAgaWYgKHhtbG9iai5Db250ZW50cykge1xuICAgIHRvQXJyYXkoeG1sb2JqLkNvbnRlbnRzKS5mb3JFYWNoKChjb250ZW50KSA9PiB7XG4gICAgICBjb25zdCBuYW1lID0gc2FuaXRpemVPYmplY3RLZXkoY29udGVudC5LZXkpXG4gICAgICBjb25zdCBsYXN0TW9kaWZpZWQgPSBuZXcgRGF0ZShjb250ZW50Lkxhc3RNb2RpZmllZClcbiAgICAgIGNvbnN0IGV0YWcgPSBzYW5pdGl6ZUVUYWcoY29udGVudC5FVGFnKVxuICAgICAgY29uc3Qgc2l6ZSA9IGNvbnRlbnQuU2l6ZVxuXG4gICAgICBsZXQgdGFnczogVGFncyA9IHt9XG4gICAgICBpZiAoY29udGVudC5Vc2VyVGFncyAhPSBudWxsKSB7XG4gICAgICAgIHRvQXJyYXkoY29udGVudC5Vc2VyVGFncy5zcGxpdCgnJicpKS5mb3JFYWNoKCh0YWcpID0+IHtcbiAgICAgICAgICBjb25zdCBba2V5LCB2YWx1ZV0gPSB0YWcuc3BsaXQoJz0nKVxuICAgICAgICAgIHRhZ3Nba2V5XSA9IHZhbHVlXG4gICAgICAgIH0pXG4gICAgICB9IGVsc2Uge1xuICAgICAgICB0YWdzID0ge31cbiAgICAgIH1cblxuICAgICAgbGV0IG1ldGFkYXRhXG4gICAgICBpZiAoY29udGVudC5Vc2VyTWV0YWRhdGEgIT0gbnVsbCkge1xuICAgICAgICBtZXRhZGF0YSA9IHRvQXJyYXkoY29udGVudC5Vc2VyTWV0YWRhdGEpWzBdXG4gICAgICB9IGVsc2Uge1xuICAgICAgICBtZXRhZGF0YSA9IG51bGxcbiAgICAgIH1cbiAgICAgIHJlc3VsdC5vYmplY3RzLnB1c2goeyBuYW1lLCBsYXN0TW9kaWZpZWQsIGV0YWcsIHNpemUsIG1ldGFkYXRhLCB0YWdzIH0pXG4gICAgfSlcbiAgfVxuXG4gIGlmICh4bWxvYmouQ29tbW9uUHJlZml4ZXMpIHtcbiAgICB0b0FycmF5KHhtbG9iai5Db21tb25QcmVmaXhlcykuZm9yRWFjaCgoY29tbW9uUHJlZml4KSA9PiB7XG4gICAgICByZXN1bHQub2JqZWN0cy5wdXNoKHsgcHJlZml4OiBzYW5pdGl6ZU9iamVjdEtleSh0b0FycmF5KGNvbW1vblByZWZpeC5QcmVmaXgpWzBdKSwgc2l6ZTogMCB9KVxuICAgIH0pXG4gIH1cbiAgcmV0dXJuIHJlc3VsdFxufVxuXG5leHBvcnQgZnVuY3Rpb24gcGFyc2VMaXN0T2JqZWN0c1YyKHhtbDogc3RyaW5nKTogTGlzdE9iamVjdFYyUmVzIHtcbiAgY29uc3QgcmVzdWx0OiBMaXN0T2JqZWN0VjJSZXMgPSB7XG4gICAgb2JqZWN0czogW10sXG4gICAgaXNUcnVuY2F0ZWQ6IGZhbHNlLFxuICAgIG5leHRDb250aW51YXRpb25Ub2tlbjogJycsXG4gIH1cblxuICBsZXQgeG1sb2JqID0gcGFyc2VYbWwoeG1sKVxuICBpZiAoIXhtbG9iai5MaXN0QnVja2V0UmVzdWx0KSB7XG4gICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkWE1MRXJyb3IoJ01pc3NpbmcgdGFnOiBcIkxpc3RCdWNrZXRSZXN1bHRcIicpXG4gIH1cbiAgeG1sb2JqID0geG1sb2JqLkxpc3RCdWNrZXRSZXN1bHRcbiAgaWYgKHhtbG9iai5Jc1RydW5jYXRlZCkge1xuICAgIHJlc3VsdC5pc1RydW5jYXRlZCA9IHhtbG9iai5Jc1RydW5jYXRlZFxuICB9XG4gIGlmICh4bWxvYmouTmV4dENvbnRpbnVhdGlvblRva2VuKSB7XG4gICAgcmVzdWx0Lm5leHRDb250aW51YXRpb25Ub2tlbiA9IHhtbG9iai5OZXh0Q29udGludWF0aW9uVG9rZW5cbiAgfVxuICBpZiAoeG1sb2JqLkNvbnRlbnRzKSB7XG4gICAgdG9BcnJheSh4bWxvYmouQ29udGVudHMpLmZvckVhY2goKGNvbnRlbnQpID0+IHtcbiAgICAgIGNvbnN0IG5hbWUgPSBzYW5pdGl6ZU9iamVjdEtleSh0b0FycmF5KGNvbnRlbnQuS2V5KVswXSlcbiAgICAgIGNvbnN0IGxhc3RNb2RpZmllZCA9IG5ldyBEYXRlKGNvbnRlbnQuTGFzdE1vZGlmaWVkKVxuICAgICAgY29uc3QgZXRhZyA9IHNhbml0aXplRVRhZyhjb250ZW50LkVUYWcpXG4gICAgICBjb25zdCBzaXplID0gY29udGVudC5TaXplXG4gICAgICByZXN1bHQub2JqZWN0cy5wdXNoKHsgbmFtZSwgbGFzdE1vZGlmaWVkLCBldGFnLCBzaXplIH0pXG4gICAgfSlcbiAgfVxuICBpZiAoeG1sb2JqLkNvbW1vblByZWZpeGVzKSB7XG4gICAgdG9BcnJheSh4bWxvYmouQ29tbW9uUHJlZml4ZXMpLmZvckVhY2goKGNvbW1vblByZWZpeCkgPT4ge1xuICAgICAgcmVzdWx0Lm9iamVjdHMucHVzaCh7IHByZWZpeDogc2FuaXRpemVPYmplY3RLZXkodG9BcnJheShjb21tb25QcmVmaXguUHJlZml4KVswXSksIHNpemU6IDAgfSlcbiAgICB9KVxuICB9XG4gIHJldHVybiByZXN1bHRcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHBhcnNlQnVja2V0Tm90aWZpY2F0aW9uKHhtbDogc3RyaW5nKTogTm90aWZpY2F0aW9uQ29uZmlnUmVzdWx0IHtcbiAgY29uc3QgcmVzdWx0OiBOb3RpZmljYXRpb25Db25maWdSZXN1bHQgPSB7XG4gICAgVG9waWNDb25maWd1cmF0aW9uOiBbXSxcbiAgICBRdWV1ZUNvbmZpZ3VyYXRpb246IFtdLFxuICAgIENsb3VkRnVuY3Rpb25Db25maWd1cmF0aW9uOiBbXSxcbiAgfVxuXG4gIGNvbnN0IGdlbkV2ZW50cyA9IChldmVudHM6IHVua25vd24pOiBzdHJpbmdbXSA9PiB7XG4gICAgaWYgKCFldmVudHMpIHtcbiAgICAgIHJldHVybiBbXVxuICAgIH1cbiAgICByZXR1cm4gdG9BcnJheShldmVudHMpIGFzIHN0cmluZ1tdXG4gIH1cblxuICBjb25zdCBnZW5GaWx0ZXJSdWxlcyA9IChmaWx0ZXJzOiB1bmtub3duKTogeyBOYW1lOiBzdHJpbmc7IFZhbHVlOiBzdHJpbmcgfVtdID0+IHtcbiAgICBjb25zdCBydWxlczogeyBOYW1lOiBzdHJpbmc7IFZhbHVlOiBzdHJpbmcgfVtdID0gW11cbiAgICBpZiAoIWZpbHRlcnMpIHtcbiAgICAgIHJldHVybiBydWxlc1xuICAgIH1cbiAgICBjb25zdCBmaWx0ZXJBcnIgPSB0b0FycmF5KGZpbHRlcnMpIGFzIFJlY29yZDxzdHJpbmcsIHVua25vd24+W11cbiAgICBpZiAoZmlsdGVyQXJyWzBdPy5TM0tleSkge1xuICAgICAgY29uc3QgczNLZXlBcnIgPSB0b0FycmF5KChmaWx0ZXJBcnJbMF0gYXMgUmVjb3JkPHN0cmluZywgdW5rbm93bj4pLlMzS2V5KSBhcyBSZWNvcmQ8c3RyaW5nLCB1bmtub3duPltdXG4gICAgICBpZiAoczNLZXlBcnJbMF0/LkZpbHRlclJ1bGUpIHtcbiAgICAgICAgdG9BcnJheShzM0tleUFyclswXS5GaWx0ZXJSdWxlKS5mb3JFYWNoKChydWxlOiB1bmtub3duKSA9PiB7XG4gICAgICAgICAgY29uc3QgciA9IHJ1bGUgYXMgUmVjb3JkPHN0cmluZywgdW5rbm93bj5cbiAgICAgICAgICBjb25zdCBOYW1lID0gdG9BcnJheShyLk5hbWUpWzBdIGFzIHN0cmluZ1xuICAgICAgICAgIGNvbnN0IFZhbHVlID0gdG9BcnJheShyLlZhbHVlKVswXSBhcyBzdHJpbmdcbiAgICAgICAgICBydWxlcy5wdXNoKHsgTmFtZSwgVmFsdWUgfSlcbiAgICAgICAgfSlcbiAgICAgIH1cbiAgICB9XG4gICAgcmV0dXJuIHJ1bGVzXG4gIH1cblxuICBsZXQgeG1sb2JqID0gcGFyc2VYbWwoeG1sKVxuICB4bWxvYmogPSB4bWxvYmouTm90aWZpY2F0aW9uQ29uZmlndXJhdGlvblxuXG4gIGlmICh4bWxvYmouVG9waWNDb25maWd1cmF0aW9uKSB7XG4gICAgdG9BcnJheSh4bWxvYmouVG9waWNDb25maWd1cmF0aW9uKS5mb3JFYWNoKChjb25maWc6IFJlY29yZDxzdHJpbmcsIHVua25vd24+KSA9PiB7XG4gICAgICBjb25zdCBJZCA9IHRvQXJyYXkoY29uZmlnLklkKVswXSBhcyBzdHJpbmdcbiAgICAgIGNvbnN0IFRvcGljID0gdG9BcnJheShjb25maWcuVG9waWMpWzBdIGFzIHN0cmluZ1xuICAgICAgY29uc3QgRXZlbnQgPSBnZW5FdmVudHMoY29uZmlnLkV2ZW50KVxuICAgICAgY29uc3QgRmlsdGVyID0gZ2VuRmlsdGVyUnVsZXMoY29uZmlnLkZpbHRlcilcbiAgICAgIHJlc3VsdC5Ub3BpY0NvbmZpZ3VyYXRpb24ucHVzaCh7IElkLCBUb3BpYywgRXZlbnQsIEZpbHRlciB9IGFzIFRvcGljQ29uZmlnRW50cnkpXG4gICAgfSlcbiAgfVxuICBpZiAoeG1sb2JqLlF1ZXVlQ29uZmlndXJhdGlvbikge1xuICAgIHRvQXJyYXkoeG1sb2JqLlF1ZXVlQ29uZmlndXJhdGlvbikuZm9yRWFjaCgoY29uZmlnOiBSZWNvcmQ8c3RyaW5nLCB1bmtub3duPikgPT4ge1xuICAgICAgY29uc3QgSWQgPSB0b0FycmF5KGNvbmZpZy5JZClbMF0gYXMgc3RyaW5nXG4gICAgICBjb25zdCBRdWV1ZSA9IHRvQXJyYXkoY29uZmlnLlF1ZXVlKVswXSBhcyBzdHJpbmdcbiAgICAgIGNvbnN0IEV2ZW50ID0gZ2VuRXZlbnRzKGNvbmZpZy5FdmVudClcbiAgICAgIGNvbnN0IEZpbHRlciA9IGdlbkZpbHRlclJ1bGVzKGNvbmZpZy5GaWx0ZXIpXG4gICAgICByZXN1bHQuUXVldWVDb25maWd1cmF0aW9uLnB1c2goeyBJZCwgUXVldWUsIEV2ZW50LCBGaWx0ZXIgfSBhcyBRdWV1ZUNvbmZpZ0VudHJ5KVxuICAgIH0pXG4gIH1cbiAgaWYgKHhtbG9iai5DbG91ZEZ1bmN0aW9uQ29uZmlndXJhdGlvbikge1xuICAgIHRvQXJyYXkoeG1sb2JqLkNsb3VkRnVuY3Rpb25Db25maWd1cmF0aW9uKS5mb3JFYWNoKChjb25maWc6IFJlY29yZDxzdHJpbmcsIHVua25vd24+KSA9PiB7XG4gICAgICBjb25zdCBJZCA9IHRvQXJyYXkoY29uZmlnLklkKVswXSBhcyBzdHJpbmdcbiAgICAgIGNvbnN0IENsb3VkRnVuY3Rpb24gPSB0b0FycmF5KGNvbmZpZy5DbG91ZEZ1bmN0aW9uKVswXSBhcyBzdHJpbmdcbiAgICAgIGNvbnN0IEV2ZW50ID0gZ2VuRXZlbnRzKGNvbmZpZy5FdmVudClcbiAgICAgIGNvbnN0IEZpbHRlciA9IGdlbkZpbHRlclJ1bGVzKGNvbmZpZy5GaWx0ZXIpXG4gICAgICByZXN1bHQuQ2xvdWRGdW5jdGlvbkNvbmZpZ3VyYXRpb24ucHVzaCh7IElkLCBDbG91ZEZ1bmN0aW9uLCBFdmVudCwgRmlsdGVyIH0gYXMgQ2xvdWRGdW5jdGlvbkNvbmZpZ0VudHJ5KVxuICAgIH0pXG4gIH1cblxuICByZXR1cm4gcmVzdWx0XG59XG5cbmV4cG9ydCB0eXBlIFVwbG9hZGVkUGFydCA9IHtcbiAgcGFydDogbnVtYmVyXG4gIGxhc3RNb2RpZmllZD86IERhdGVcbiAgZXRhZzogc3RyaW5nXG4gIHNpemU6IG51bWJlclxufVxuXG4vLyBwYXJzZSBYTUwgcmVzcG9uc2UgZm9yIGxpc3QgcGFydHMgb2YgYW4gaW4gcHJvZ3Jlc3MgbXVsdGlwYXJ0IHVwbG9hZFxuZXhwb3J0IGZ1bmN0aW9uIHBhcnNlTGlzdFBhcnRzKHhtbDogc3RyaW5nKToge1xuICBpc1RydW5jYXRlZDogYm9vbGVhblxuICBtYXJrZXI6IG51bWJlclxuICBwYXJ0czogVXBsb2FkZWRQYXJ0W11cbn0ge1xuICBsZXQgeG1sb2JqID0gcGFyc2VYbWwoeG1sKVxuICBjb25zdCByZXN1bHQ6IHtcbiAgICBpc1RydW5jYXRlZDogYm9vbGVhblxuICAgIG1hcmtlcjogbnVtYmVyXG4gICAgcGFydHM6IFVwbG9hZGVkUGFydFtdXG4gIH0gPSB7XG4gICAgaXNUcnVuY2F0ZWQ6IGZhbHNlLFxuICAgIHBhcnRzOiBbXSxcbiAgICBtYXJrZXI6IDAsXG4gIH1cbiAgaWYgKCF4bWxvYmouTGlzdFBhcnRzUmVzdWx0KSB7XG4gICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkWE1MRXJyb3IoJ01pc3NpbmcgdGFnOiBcIkxpc3RQYXJ0c1Jlc3VsdFwiJylcbiAgfVxuICB4bWxvYmogPSB4bWxvYmouTGlzdFBhcnRzUmVzdWx0XG4gIGlmICh4bWxvYmouSXNUcnVuY2F0ZWQpIHtcbiAgICByZXN1bHQuaXNUcnVuY2F0ZWQgPSB4bWxvYmouSXNUcnVuY2F0ZWRcbiAgfVxuICBpZiAoeG1sb2JqLk5leHRQYXJ0TnVtYmVyTWFya2VyKSB7XG4gICAgcmVzdWx0Lm1hcmtlciA9IHRvQXJyYXkoeG1sb2JqLk5leHRQYXJ0TnVtYmVyTWFya2VyKVswXSB8fCAnJ1xuICB9XG4gIGlmICh4bWxvYmouUGFydCkge1xuICAgIHRvQXJyYXkoeG1sb2JqLlBhcnQpLmZvckVhY2goKHApID0+IHtcbiAgICAgIGNvbnN0IHBhcnQgPSBwYXJzZUludCh0b0FycmF5KHAuUGFydE51bWJlcilbMF0sIDEwKVxuICAgICAgY29uc3QgbGFzdE1vZGlmaWVkID0gbmV3IERhdGUocC5MYXN0TW9kaWZpZWQpXG4gICAgICBjb25zdCBldGFnID0gcC5FVGFnLnJlcGxhY2UoL15cIi9nLCAnJylcbiAgICAgICAgLnJlcGxhY2UoL1wiJC9nLCAnJylcbiAgICAgICAgLnJlcGxhY2UoL14mcXVvdDsvZywgJycpXG4gICAgICAgIC5yZXBsYWNlKC8mcXVvdDskL2csICcnKVxuICAgICAgICAucmVwbGFjZSgvXiYjMzQ7L2csICcnKVxuICAgICAgICAucmVwbGFjZSgvJiMzNDskL2csICcnKVxuICAgICAgcmVzdWx0LnBhcnRzLnB1c2goeyBwYXJ0LCBsYXN0TW9kaWZpZWQsIGV0YWcsIHNpemU6IHBhcnNlSW50KHAuU2l6ZSwgMTApIH0pXG4gICAgfSlcbiAgfVxuICByZXR1cm4gcmVzdWx0XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBwYXJzZUxpc3RCdWNrZXQoeG1sOiBzdHJpbmcpOiBCdWNrZXRJdGVtRnJvbUxpc3RbXSB7XG4gIGxldCByZXN1bHQ6IEJ1Y2tldEl0ZW1Gcm9tTGlzdFtdID0gW11cbiAgY29uc3QgbGlzdEJ1Y2tldFJlc3VsdFBhcnNlciA9IG5ldyBYTUxQYXJzZXIoe1xuICAgIHBhcnNlVGFnVmFsdWU6IHRydWUsIC8vIEVuYWJsZSBwYXJzaW5nIG9mIHZhbHVlc1xuICAgIG51bWJlclBhcnNlT3B0aW9uczoge1xuICAgICAgbGVhZGluZ1plcm9zOiBmYWxzZSwgLy8gRGlzYWJsZSBudW1iZXIgcGFyc2luZyBmb3IgdmFsdWVzIHdpdGggbGVhZGluZyB6ZXJvc1xuICAgICAgaGV4OiBmYWxzZSwgLy8gRGlzYWJsZSBoZXggbnVtYmVyIHBhcnNpbmcgLSBJbnZhbGlkIGJ1Y2tldCBuYW1lXG4gICAgICBza2lwTGlrZTogL15bMC05XSskLywgLy8gU2tpcCBudW1iZXIgcGFyc2luZyBpZiB0aGUgdmFsdWUgY29uc2lzdHMgZW50aXJlbHkgb2YgZGlnaXRzXG4gICAgfSxcbiAgICB0YWdWYWx1ZVByb2Nlc3NvcjogKHRhZ05hbWUsIHRhZ1ZhbHVlID0gJycpID0+IHtcbiAgICAgIC8vIEVuc3VyZSB0aGF0IHRoZSBOYW1lIHRhZyBpcyBhbHdheXMgdHJlYXRlZCBhcyBhIHN0cmluZ1xuICAgICAgaWYgKHRhZ05hbWUgPT09ICdOYW1lJykge1xuICAgICAgICByZXR1cm4gdGFnVmFsdWUudG9TdHJpbmcoKVxuICAgICAgfVxuICAgICAgcmV0dXJuIHRhZ1ZhbHVlXG4gICAgfSxcbiAgICBpZ25vcmVBdHRyaWJ1dGVzOiBmYWxzZSwgLy8gRW5zdXJlIHRoYXQgYWxsIGF0dHJpYnV0ZXMgYXJlIHBhcnNlZFxuICB9KVxuXG4gIGNvbnN0IHBhcnNlZFhtbFJlcyA9IGxpc3RCdWNrZXRSZXN1bHRQYXJzZXIucGFyc2UoeG1sKVxuXG4gIGlmICghcGFyc2VkWG1sUmVzLkxpc3RBbGxNeUJ1Y2tldHNSZXN1bHQpIHtcbiAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRYTUxFcnJvcignTWlzc2luZyB0YWc6IFwiTGlzdEFsbE15QnVja2V0c1Jlc3VsdFwiJylcbiAgfVxuXG4gIGNvbnN0IHsgTGlzdEFsbE15QnVja2V0c1Jlc3VsdDogeyBCdWNrZXRzID0ge30gfSA9IHt9IH0gPSBwYXJzZWRYbWxSZXNcblxuICBpZiAoQnVja2V0cy5CdWNrZXQpIHtcbiAgICByZXN1bHQgPSB0b0FycmF5KEJ1Y2tldHMuQnVja2V0KS5tYXAoKGJ1Y2tldCA9IHt9KSA9PiB7XG4gICAgICBjb25zdCB7IE5hbWU6IGJ1Y2tldE5hbWUsIENyZWF0aW9uRGF0ZSB9ID0gYnVja2V0XG4gICAgICBjb25zdCBjcmVhdGlvbkRhdGUgPSBuZXcgRGF0ZShDcmVhdGlvbkRhdGUpXG5cbiAgICAgIHJldHVybiB7IG5hbWU6IGJ1Y2tldE5hbWUsIGNyZWF0aW9uRGF0ZSB9XG4gICAgfSlcbiAgfVxuXG4gIHJldHVybiByZXN1bHRcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHBhcnNlSW5pdGlhdGVNdWx0aXBhcnQoeG1sOiBzdHJpbmcpOiBzdHJpbmcge1xuICBsZXQgeG1sb2JqID0gcGFyc2VYbWwoeG1sKVxuXG4gIGlmICgheG1sb2JqLkluaXRpYXRlTXVsdGlwYXJ0VXBsb2FkUmVzdWx0KSB7XG4gICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkWE1MRXJyb3IoJ01pc3NpbmcgdGFnOiBcIkluaXRpYXRlTXVsdGlwYXJ0VXBsb2FkUmVzdWx0XCInKVxuICB9XG4gIHhtbG9iaiA9IHhtbG9iai5Jbml0aWF0ZU11bHRpcGFydFVwbG9hZFJlc3VsdFxuXG4gIGlmICh4bWxvYmouVXBsb2FkSWQpIHtcbiAgICByZXR1cm4geG1sb2JqLlVwbG9hZElkXG4gIH1cbiAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkWE1MRXJyb3IoJ01pc3NpbmcgdGFnOiBcIlVwbG9hZElkXCInKVxufVxuXG5leHBvcnQgZnVuY3Rpb24gcGFyc2VSZXBsaWNhdGlvbkNvbmZpZyh4bWw6IHN0cmluZyk6IFJlcGxpY2F0aW9uQ29uZmlnIHtcbiAgY29uc3QgeG1sT2JqID0gcGFyc2VYbWwoeG1sKVxuICBjb25zdCB7IFJvbGUsIFJ1bGUgfSA9IHhtbE9iai5SZXBsaWNhdGlvbkNvbmZpZ3VyYXRpb25cbiAgcmV0dXJuIHtcbiAgICBSZXBsaWNhdGlvbkNvbmZpZ3VyYXRpb246IHtcbiAgICAgIHJvbGU6IFJvbGUsXG4gICAgICBydWxlczogdG9BcnJheShSdWxlKSxcbiAgICB9LFxuICB9XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBwYXJzZU9iamVjdExlZ2FsSG9sZENvbmZpZyh4bWw6IHN0cmluZykge1xuICBjb25zdCB4bWxPYmogPSBwYXJzZVhtbCh4bWwpXG4gIHJldHVybiB4bWxPYmouTGVnYWxIb2xkXG59XG5cbmV4cG9ydCBmdW5jdGlvbiBwYXJzZVRhZ2dpbmcoeG1sOiBzdHJpbmcpIHtcbiAgY29uc3QgeG1sT2JqID0gcGFyc2VYbWwoeG1sKVxuICBsZXQgcmVzdWx0OiBUYWdbXSA9IFtdXG4gIGlmICh4bWxPYmouVGFnZ2luZyAmJiB4bWxPYmouVGFnZ2luZy5UYWdTZXQgJiYgeG1sT2JqLlRhZ2dpbmcuVGFnU2V0LlRhZykge1xuICAgIGNvbnN0IHRhZ1Jlc3VsdDogVGFnID0geG1sT2JqLlRhZ2dpbmcuVGFnU2V0LlRhZ1xuICAgIC8vIGlmIGl0IGlzIGEgc2luZ2xlIHRhZyBjb252ZXJ0IGludG8gYW4gYXJyYXkgc28gdGhhdCB0aGUgcmV0dXJuIHZhbHVlIGlzIGFsd2F5cyBhbiBhcnJheS5cbiAgICBpZiAoQXJyYXkuaXNBcnJheSh0YWdSZXN1bHQpKSB7XG4gICAgICByZXN1bHQgPSBbLi4udGFnUmVzdWx0XVxuICAgIH0gZWxzZSB7XG4gICAgICByZXN1bHQucHVzaCh0YWdSZXN1bHQpXG4gICAgfVxuICB9XG4gIHJldHVybiByZXN1bHRcbn1cblxuLy8gcGFyc2UgWE1MIHJlc3BvbnNlIHdoZW4gYSBtdWx0aXBhcnQgdXBsb2FkIGlzIGNvbXBsZXRlZFxuZXhwb3J0IGZ1bmN0aW9uIHBhcnNlQ29tcGxldGVNdWx0aXBhcnQoeG1sOiBzdHJpbmcpIHtcbiAgY29uc3QgeG1sb2JqID0gcGFyc2VYbWwoeG1sKS5Db21wbGV0ZU11bHRpcGFydFVwbG9hZFJlc3VsdFxuICBpZiAoeG1sb2JqLkxvY2F0aW9uKSB7XG4gICAgY29uc3QgbG9jYXRpb24gPSB0b0FycmF5KHhtbG9iai5Mb2NhdGlvbilbMF1cbiAgICBjb25zdCBidWNrZXQgPSB0b0FycmF5KHhtbG9iai5CdWNrZXQpWzBdXG4gICAgY29uc3Qga2V5ID0geG1sb2JqLktleVxuICAgIGNvbnN0IGV0YWcgPSB4bWxvYmouRVRhZy5yZXBsYWNlKC9eXCIvZywgJycpXG4gICAgICAucmVwbGFjZSgvXCIkL2csICcnKVxuICAgICAgLnJlcGxhY2UoL14mcXVvdDsvZywgJycpXG4gICAgICAucmVwbGFjZSgvJnF1b3Q7JC9nLCAnJylcbiAgICAgIC5yZXBsYWNlKC9eJiMzNDsvZywgJycpXG4gICAgICAucmVwbGFjZSgvJiMzNDskL2csICcnKVxuXG4gICAgcmV0dXJuIHsgbG9jYXRpb24sIGJ1Y2tldCwga2V5LCBldGFnIH1cbiAgfVxuICAvLyBDb21wbGV0ZSBNdWx0aXBhcnQgY2FuIHJldHVybiBYTUwgRXJyb3IgYWZ0ZXIgYSAyMDAgT0sgcmVzcG9uc2VcbiAgaWYgKHhtbG9iai5Db2RlICYmIHhtbG9iai5NZXNzYWdlKSB7XG4gICAgY29uc3QgZXJyQ29kZSA9IHRvQXJyYXkoeG1sb2JqLkNvZGUpWzBdXG4gICAgY29uc3QgZXJyTWVzc2FnZSA9IHRvQXJyYXkoeG1sb2JqLk1lc3NhZ2UpWzBdXG4gICAgcmV0dXJuIHsgZXJyQ29kZSwgZXJyTWVzc2FnZSB9XG4gIH1cbn1cblxudHlwZSBVcGxvYWRJRCA9IHN0cmluZ1xuXG5leHBvcnQgdHlwZSBMaXN0TXVsdGlwYXJ0UmVzdWx0ID0ge1xuICB1cGxvYWRzOiB7XG4gICAga2V5OiBzdHJpbmdcbiAgICB1cGxvYWRJZDogVXBsb2FkSURcbiAgICBpbml0aWF0b3I/OiB7IGlkOiBzdHJpbmc7IGRpc3BsYXlOYW1lOiBzdHJpbmcgfVxuICAgIG93bmVyPzogeyBpZDogc3RyaW5nOyBkaXNwbGF5TmFtZTogc3RyaW5nIH1cbiAgICBzdG9yYWdlQ2xhc3M6IHVua25vd25cbiAgICBpbml0aWF0ZWQ6IERhdGVcbiAgfVtdXG4gIHByZWZpeGVzOiB7XG4gICAgcHJlZml4OiBzdHJpbmdcbiAgfVtdXG4gIGlzVHJ1bmNhdGVkOiBib29sZWFuXG4gIG5leHRLZXlNYXJrZXI6IHN0cmluZ1xuICBuZXh0VXBsb2FkSWRNYXJrZXI6IHN0cmluZ1xufVxuXG4vLyBwYXJzZSBYTUwgcmVzcG9uc2UgZm9yIGxpc3RpbmcgaW4tcHJvZ3Jlc3MgbXVsdGlwYXJ0IHVwbG9hZHNcbmV4cG9ydCBmdW5jdGlvbiBwYXJzZUxpc3RNdWx0aXBhcnQoeG1sOiBzdHJpbmcpOiBMaXN0TXVsdGlwYXJ0UmVzdWx0IHtcbiAgY29uc3QgcmVzdWx0OiBMaXN0TXVsdGlwYXJ0UmVzdWx0ID0ge1xuICAgIHByZWZpeGVzOiBbXSxcbiAgICB1cGxvYWRzOiBbXSxcbiAgICBpc1RydW5jYXRlZDogZmFsc2UsXG4gICAgbmV4dEtleU1hcmtlcjogJycsXG4gICAgbmV4dFVwbG9hZElkTWFya2VyOiAnJyxcbiAgfVxuXG4gIGxldCB4bWxvYmogPSBwYXJzZVhtbCh4bWwpXG5cbiAgaWYgKCF4bWxvYmouTGlzdE11bHRpcGFydFVwbG9hZHNSZXN1bHQpIHtcbiAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRYTUxFcnJvcignTWlzc2luZyB0YWc6IFwiTGlzdE11bHRpcGFydFVwbG9hZHNSZXN1bHRcIicpXG4gIH1cbiAgeG1sb2JqID0geG1sb2JqLkxpc3RNdWx0aXBhcnRVcGxvYWRzUmVzdWx0XG4gIGlmICh4bWxvYmouSXNUcnVuY2F0ZWQpIHtcbiAgICByZXN1bHQuaXNUcnVuY2F0ZWQgPSB4bWxvYmouSXNUcnVuY2F0ZWRcbiAgfVxuICBpZiAoeG1sb2JqLk5leHRLZXlNYXJrZXIpIHtcbiAgICByZXN1bHQubmV4dEtleU1hcmtlciA9IHhtbG9iai5OZXh0S2V5TWFya2VyXG4gIH1cbiAgaWYgKHhtbG9iai5OZXh0VXBsb2FkSWRNYXJrZXIpIHtcbiAgICByZXN1bHQubmV4dFVwbG9hZElkTWFya2VyID0geG1sb2JqLm5leHRVcGxvYWRJZE1hcmtlciB8fCAnJ1xuICB9XG5cbiAgaWYgKHhtbG9iai5Db21tb25QcmVmaXhlcykge1xuICAgIHRvQXJyYXkoeG1sb2JqLkNvbW1vblByZWZpeGVzKS5mb3JFYWNoKChwcmVmaXgpID0+IHtcbiAgICAgIC8vIEB0cy1leHBlY3QtZXJyb3IgaW5kZXggY2hlY2tcbiAgICAgIHJlc3VsdC5wcmVmaXhlcy5wdXNoKHsgcHJlZml4OiBzYW5pdGl6ZU9iamVjdEtleSh0b0FycmF5PHN0cmluZz4ocHJlZml4LlByZWZpeClbMF0pIH0pXG4gICAgfSlcbiAgfVxuXG4gIGlmICh4bWxvYmouVXBsb2FkKSB7XG4gICAgdG9BcnJheSh4bWxvYmouVXBsb2FkKS5mb3JFYWNoKCh1cGxvYWQpID0+IHtcbiAgICAgIGNvbnN0IHVwbG9hZEl0ZW06IExpc3RNdWx0aXBhcnRSZXN1bHRbJ3VwbG9hZHMnXVtudW1iZXJdID0ge1xuICAgICAgICBrZXk6IHVwbG9hZC5LZXksXG4gICAgICAgIHVwbG9hZElkOiB1cGxvYWQuVXBsb2FkSWQsXG4gICAgICAgIHN0b3JhZ2VDbGFzczogdXBsb2FkLlN0b3JhZ2VDbGFzcyxcbiAgICAgICAgaW5pdGlhdGVkOiBuZXcgRGF0ZSh1cGxvYWQuSW5pdGlhdGVkKSxcbiAgICAgIH1cbiAgICAgIGlmICh1cGxvYWQuSW5pdGlhdG9yKSB7XG4gICAgICAgIHVwbG9hZEl0ZW0uaW5pdGlhdG9yID0geyBpZDogdXBsb2FkLkluaXRpYXRvci5JRCwgZGlzcGxheU5hbWU6IHVwbG9hZC5Jbml0aWF0b3IuRGlzcGxheU5hbWUgfVxuICAgICAgfVxuICAgICAgaWYgKHVwbG9hZC5Pd25lcikge1xuICAgICAgICB1cGxvYWRJdGVtLm93bmVyID0geyBpZDogdXBsb2FkLk93bmVyLklELCBkaXNwbGF5TmFtZTogdXBsb2FkLk93bmVyLkRpc3BsYXlOYW1lIH1cbiAgICAgIH1cbiAgICAgIHJlc3VsdC51cGxvYWRzLnB1c2godXBsb2FkSXRlbSlcbiAgICB9KVxuICB9XG4gIHJldHVybiByZXN1bHRcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHBhcnNlT2JqZWN0TG9ja0NvbmZpZyh4bWw6IHN0cmluZyk6IE9iamVjdExvY2tJbmZvIHtcbiAgY29uc3QgeG1sT2JqID0gcGFyc2VYbWwoeG1sKVxuICBsZXQgbG9ja0NvbmZpZ1Jlc3VsdCA9IHt9IGFzIE9iamVjdExvY2tJbmZvXG4gIGlmICh4bWxPYmouT2JqZWN0TG9ja0NvbmZpZ3VyYXRpb24pIHtcbiAgICBsb2NrQ29uZmlnUmVzdWx0ID0ge1xuICAgICAgb2JqZWN0TG9ja0VuYWJsZWQ6IHhtbE9iai5PYmplY3RMb2NrQ29uZmlndXJhdGlvbi5PYmplY3RMb2NrRW5hYmxlZCxcbiAgICB9IGFzIE9iamVjdExvY2tJbmZvXG4gICAgbGV0IHJldGVudGlvblJlc3BcbiAgICBpZiAoXG4gICAgICB4bWxPYmouT2JqZWN0TG9ja0NvbmZpZ3VyYXRpb24gJiZcbiAgICAgIHhtbE9iai5PYmplY3RMb2NrQ29uZmlndXJhdGlvbi5SdWxlICYmXG4gICAgICB4bWxPYmouT2JqZWN0TG9ja0NvbmZpZ3VyYXRpb24uUnVsZS5EZWZhdWx0UmV0ZW50aW9uXG4gICAgKSB7XG4gICAgICByZXRlbnRpb25SZXNwID0geG1sT2JqLk9iamVjdExvY2tDb25maWd1cmF0aW9uLlJ1bGUuRGVmYXVsdFJldGVudGlvbiB8fCB7fVxuICAgICAgbG9ja0NvbmZpZ1Jlc3VsdC5tb2RlID0gcmV0ZW50aW9uUmVzcC5Nb2RlXG4gICAgfVxuICAgIGlmIChyZXRlbnRpb25SZXNwKSB7XG4gICAgICBjb25zdCBpc1VuaXRZZWFycyA9IHJldGVudGlvblJlc3AuWWVhcnNcbiAgICAgIGlmIChpc1VuaXRZZWFycykge1xuICAgICAgICBsb2NrQ29uZmlnUmVzdWx0LnZhbGlkaXR5ID0gaXNVbml0WWVhcnNcbiAgICAgICAgbG9ja0NvbmZpZ1Jlc3VsdC51bml0ID0gUkVURU5USU9OX1ZBTElESVRZX1VOSVRTLllFQVJTXG4gICAgICB9IGVsc2Uge1xuICAgICAgICBsb2NrQ29uZmlnUmVzdWx0LnZhbGlkaXR5ID0gcmV0ZW50aW9uUmVzcC5EYXlzXG4gICAgICAgIGxvY2tDb25maWdSZXN1bHQudW5pdCA9IFJFVEVOVElPTl9WQUxJRElUWV9VTklUUy5EQVlTXG4gICAgICB9XG4gICAgfVxuICB9XG5cbiAgcmV0dXJuIGxvY2tDb25maWdSZXN1bHRcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHBhcnNlQnVja2V0VmVyc2lvbmluZ0NvbmZpZyh4bWw6IHN0cmluZykge1xuICBjb25zdCB4bWxPYmogPSBwYXJzZVhtbCh4bWwpXG4gIHJldHVybiB4bWxPYmouVmVyc2lvbmluZ0NvbmZpZ3VyYXRpb25cbn1cblxuLy8gVXNlZCBvbmx5IGluIHNlbGVjdE9iamVjdENvbnRlbnQgQVBJLlxuLy8gZXh0cmFjdEhlYWRlclR5cGUgZXh0cmFjdHMgdGhlIGZpcnN0IGhhbGYgb2YgdGhlIGhlYWRlciBtZXNzYWdlLCB0aGUgaGVhZGVyIHR5cGUuXG5mdW5jdGlvbiBleHRyYWN0SGVhZGVyVHlwZShzdHJlYW06IHN0cmVhbS5SZWFkYWJsZSk6IHN0cmluZyB8IHVuZGVmaW5lZCB7XG4gIGNvbnN0IGhlYWRlck5hbWVMZW4gPSBCdWZmZXIuZnJvbShzdHJlYW0ucmVhZCgxKSkucmVhZFVJbnQ4KClcbiAgY29uc3QgaGVhZGVyTmFtZVdpdGhTZXBhcmF0b3IgPSBCdWZmZXIuZnJvbShzdHJlYW0ucmVhZChoZWFkZXJOYW1lTGVuKSkudG9TdHJpbmcoKVxuICBjb25zdCBzcGxpdEJ5U2VwYXJhdG9yID0gKGhlYWRlck5hbWVXaXRoU2VwYXJhdG9yIHx8ICcnKS5zcGxpdCgnOicpXG4gIHJldHVybiBzcGxpdEJ5U2VwYXJhdG9yLmxlbmd0aCA+PSAxID8gc3BsaXRCeVNlcGFyYXRvclsxXSA6ICcnXG59XG5cbmZ1bmN0aW9uIGV4dHJhY3RIZWFkZXJWYWx1ZShzdHJlYW06IHN0cmVhbS5SZWFkYWJsZSkge1xuICBjb25zdCBib2R5TGVuID0gQnVmZmVyLmZyb20oc3RyZWFtLnJlYWQoMikpLnJlYWRVSW50MTZCRSgpXG4gIHJldHVybiBCdWZmZXIuZnJvbShzdHJlYW0ucmVhZChib2R5TGVuKSkudG9TdHJpbmcoKVxufVxuXG5leHBvcnQgZnVuY3Rpb24gcGFyc2VTZWxlY3RPYmplY3RDb250ZW50UmVzcG9uc2UocmVzOiBCdWZmZXIpIHtcbiAgY29uc3Qgc2VsZWN0UmVzdWx0cyA9IG5ldyBTZWxlY3RSZXN1bHRzKHt9KSAvLyB3aWxsIGJlIHJldHVybmVkXG5cbiAgY29uc3QgcmVzcG9uc2VTdHJlYW0gPSByZWFkYWJsZVN0cmVhbShyZXMpIC8vIGNvbnZlcnQgYnl0ZSBhcnJheSB0byBhIHJlYWRhYmxlIHJlc3BvbnNlU3RyZWFtXG4gIC8vIEB0cy1pZ25vcmVcbiAgd2hpbGUgKHJlc3BvbnNlU3RyZWFtLl9yZWFkYWJsZVN0YXRlLmxlbmd0aCkge1xuICAgIC8vIFRvcCBsZXZlbCByZXNwb25zZVN0cmVhbSByZWFkIHRyYWNrZXIuXG4gICAgbGV0IG1zZ0NyY0FjY3VtdWxhdG9yIC8vIGFjY3VtdWxhdGUgZnJvbSBzdGFydCBvZiB0aGUgbWVzc2FnZSB0aWxsIHRoZSBtZXNzYWdlIGNyYyBzdGFydC5cblxuICAgIGNvbnN0IHRvdGFsQnl0ZUxlbmd0aEJ1ZmZlciA9IEJ1ZmZlci5mcm9tKHJlc3BvbnNlU3RyZWFtLnJlYWQoNCkpXG4gICAgbXNnQ3JjQWNjdW11bGF0b3IgPSBjcmMzMih0b3RhbEJ5dGVMZW5ndGhCdWZmZXIpXG5cbiAgICBjb25zdCBoZWFkZXJCeXRlc0J1ZmZlciA9IEJ1ZmZlci5mcm9tKHJlc3BvbnNlU3RyZWFtLnJlYWQoNCkpXG4gICAgbXNnQ3JjQWNjdW11bGF0b3IgPSBjcmMzMihoZWFkZXJCeXRlc0J1ZmZlciwgbXNnQ3JjQWNjdW11bGF0b3IpXG5cbiAgICBjb25zdCBjYWxjdWxhdGVkUHJlbHVkZUNyYyA9IG1zZ0NyY0FjY3VtdWxhdG9yLnJlYWRJbnQzMkJFKCkgLy8gdXNlIGl0IHRvIGNoZWNrIGlmIGFueSBDUkMgbWlzbWF0Y2ggaW4gaGVhZGVyIGl0c2VsZi5cblxuICAgIGNvbnN0IHByZWx1ZGVDcmNCdWZmZXIgPSBCdWZmZXIuZnJvbShyZXNwb25zZVN0cmVhbS5yZWFkKDQpKSAvLyByZWFkIDQgYnl0ZXMgICAgaS5lIDQrNCA9OCArIDQgPSAxMiAoIHByZWx1ZGUgKyBwcmVsdWRlIGNyYylcbiAgICBtc2dDcmNBY2N1bXVsYXRvciA9IGNyYzMyKHByZWx1ZGVDcmNCdWZmZXIsIG1zZ0NyY0FjY3VtdWxhdG9yKVxuXG4gICAgY29uc3QgdG90YWxNc2dMZW5ndGggPSB0b3RhbEJ5dGVMZW5ndGhCdWZmZXIucmVhZEludDMyQkUoKVxuICAgIGNvbnN0IGhlYWRlckxlbmd0aCA9IGhlYWRlckJ5dGVzQnVmZmVyLnJlYWRJbnQzMkJFKClcbiAgICBjb25zdCBwcmVsdWRlQ3JjQnl0ZVZhbHVlID0gcHJlbHVkZUNyY0J1ZmZlci5yZWFkSW50MzJCRSgpXG5cbiAgICBpZiAocHJlbHVkZUNyY0J5dGVWYWx1ZSAhPT0gY2FsY3VsYXRlZFByZWx1ZGVDcmMpIHtcbiAgICAgIC8vIEhhbmRsZSBIZWFkZXIgQ1JDIG1pc21hdGNoIEVycm9yXG4gICAgICB0aHJvdyBuZXcgRXJyb3IoXG4gICAgICAgIGBIZWFkZXIgQ2hlY2tzdW0gTWlzbWF0Y2gsIFByZWx1ZGUgQ1JDIG9mICR7cHJlbHVkZUNyY0J5dGVWYWx1ZX0gZG9lcyBub3QgZXF1YWwgZXhwZWN0ZWQgQ1JDIG9mICR7Y2FsY3VsYXRlZFByZWx1ZGVDcmN9YCxcbiAgICAgIClcbiAgICB9XG5cbiAgICBjb25zdCBoZWFkZXJzOiBSZWNvcmQ8c3RyaW5nLCB1bmtub3duPiA9IHt9XG4gICAgaWYgKGhlYWRlckxlbmd0aCA+IDApIHtcbiAgICAgIGNvbnN0IGhlYWRlckJ5dGVzID0gQnVmZmVyLmZyb20ocmVzcG9uc2VTdHJlYW0ucmVhZChoZWFkZXJMZW5ndGgpKVxuICAgICAgbXNnQ3JjQWNjdW11bGF0b3IgPSBjcmMzMihoZWFkZXJCeXRlcywgbXNnQ3JjQWNjdW11bGF0b3IpXG4gICAgICBjb25zdCBoZWFkZXJSZWFkZXJTdHJlYW0gPSByZWFkYWJsZVN0cmVhbShoZWFkZXJCeXRlcylcbiAgICAgIC8vIEB0cy1pZ25vcmVcbiAgICAgIHdoaWxlIChoZWFkZXJSZWFkZXJTdHJlYW0uX3JlYWRhYmxlU3RhdGUubGVuZ3RoKSB7XG4gICAgICAgIGNvbnN0IGhlYWRlclR5cGVOYW1lID0gZXh0cmFjdEhlYWRlclR5cGUoaGVhZGVyUmVhZGVyU3RyZWFtKVxuICAgICAgICBoZWFkZXJSZWFkZXJTdHJlYW0ucmVhZCgxKSAvLyBqdXN0IHJlYWQgYW5kIGlnbm9yZSBpdC5cbiAgICAgICAgaWYgKGhlYWRlclR5cGVOYW1lKSB7XG4gICAgICAgICAgaGVhZGVyc1toZWFkZXJUeXBlTmFtZV0gPSBleHRyYWN0SGVhZGVyVmFsdWUoaGVhZGVyUmVhZGVyU3RyZWFtKVxuICAgICAgICB9XG4gICAgICB9XG4gICAgfVxuXG4gICAgbGV0IHBheWxvYWRTdHJlYW1cbiAgICBjb25zdCBwYXlMb2FkTGVuZ3RoID0gdG90YWxNc2dMZW5ndGggLSBoZWFkZXJMZW5ndGggLSAxNlxuICAgIGlmIChwYXlMb2FkTGVuZ3RoID4gMCkge1xuICAgICAgY29uc3QgcGF5TG9hZEJ1ZmZlciA9IEJ1ZmZlci5mcm9tKHJlc3BvbnNlU3RyZWFtLnJlYWQocGF5TG9hZExlbmd0aCkpXG4gICAgICBtc2dDcmNBY2N1bXVsYXRvciA9IGNyYzMyKHBheUxvYWRCdWZmZXIsIG1zZ0NyY0FjY3VtdWxhdG9yKVxuICAgICAgLy8gcmVhZCB0aGUgY2hlY2tzdW0gZWFybHkgYW5kIGRldGVjdCBhbnkgbWlzbWF0Y2ggc28gd2UgY2FuIGF2b2lkIHVubmVjZXNzYXJ5IGZ1cnRoZXIgcHJvY2Vzc2luZy5cbiAgICAgIGNvbnN0IG1lc3NhZ2VDcmNCeXRlVmFsdWUgPSBCdWZmZXIuZnJvbShyZXNwb25zZVN0cmVhbS5yZWFkKDQpKS5yZWFkSW50MzJCRSgpXG4gICAgICBjb25zdCBjYWxjdWxhdGVkQ3JjID0gbXNnQ3JjQWNjdW11bGF0b3IucmVhZEludDMyQkUoKVxuICAgICAgLy8gSGFuZGxlIG1lc3NhZ2UgQ1JDIEVycm9yXG4gICAgICBpZiAobWVzc2FnZUNyY0J5dGVWYWx1ZSAhPT0gY2FsY3VsYXRlZENyYykge1xuICAgICAgICB0aHJvdyBuZXcgRXJyb3IoXG4gICAgICAgICAgYE1lc3NhZ2UgQ2hlY2tzdW0gTWlzbWF0Y2gsIE1lc3NhZ2UgQ1JDIG9mICR7bWVzc2FnZUNyY0J5dGVWYWx1ZX0gZG9lcyBub3QgZXF1YWwgZXhwZWN0ZWQgQ1JDIG9mICR7Y2FsY3VsYXRlZENyY31gLFxuICAgICAgICApXG4gICAgICB9XG4gICAgICBwYXlsb2FkU3RyZWFtID0gcmVhZGFibGVTdHJlYW0ocGF5TG9hZEJ1ZmZlcilcbiAgICB9XG4gICAgY29uc3QgbWVzc2FnZVR5cGUgPSBoZWFkZXJzWydtZXNzYWdlLXR5cGUnXVxuXG4gICAgc3dpdGNoIChtZXNzYWdlVHlwZSkge1xuICAgICAgY2FzZSAnZXJyb3InOiB7XG4gICAgICAgIGNvbnN0IGVycm9yTWVzc2FnZSA9IGhlYWRlcnNbJ2Vycm9yLWNvZGUnXSArICc6XCInICsgaGVhZGVyc1snZXJyb3ItbWVzc2FnZSddICsgJ1wiJ1xuICAgICAgICB0aHJvdyBuZXcgRXJyb3IoZXJyb3JNZXNzYWdlKVxuICAgICAgfVxuICAgICAgY2FzZSAnZXZlbnQnOiB7XG4gICAgICAgIGNvbnN0IGNvbnRlbnRUeXBlID0gaGVhZGVyc1snY29udGVudC10eXBlJ11cbiAgICAgICAgY29uc3QgZXZlbnRUeXBlID0gaGVhZGVyc1snZXZlbnQtdHlwZSddXG5cbiAgICAgICAgc3dpdGNoIChldmVudFR5cGUpIHtcbiAgICAgICAgICBjYXNlICdFbmQnOiB7XG4gICAgICAgICAgICBzZWxlY3RSZXN1bHRzLnNldFJlc3BvbnNlKHJlcylcbiAgICAgICAgICAgIHJldHVybiBzZWxlY3RSZXN1bHRzXG4gICAgICAgICAgfVxuXG4gICAgICAgICAgY2FzZSAnUmVjb3Jkcyc6IHtcbiAgICAgICAgICAgIGNvbnN0IHJlYWREYXRhID0gcGF5bG9hZFN0cmVhbT8ucmVhZChwYXlMb2FkTGVuZ3RoKVxuICAgICAgICAgICAgc2VsZWN0UmVzdWx0cy5zZXRSZWNvcmRzKHJlYWREYXRhKVxuICAgICAgICAgICAgYnJlYWtcbiAgICAgICAgICB9XG5cbiAgICAgICAgICBjYXNlICdQcm9ncmVzcyc6XG4gICAgICAgICAgICB7XG4gICAgICAgICAgICAgIHN3aXRjaCAoY29udGVudFR5cGUpIHtcbiAgICAgICAgICAgICAgICBjYXNlICd0ZXh0L3htbCc6IHtcbiAgICAgICAgICAgICAgICAgIGNvbnN0IHByb2dyZXNzRGF0YSA9IHBheWxvYWRTdHJlYW0/LnJlYWQocGF5TG9hZExlbmd0aClcbiAgICAgICAgICAgICAgICAgIHNlbGVjdFJlc3VsdHMuc2V0UHJvZ3Jlc3MocHJvZ3Jlc3NEYXRhLnRvU3RyaW5nKCkpXG4gICAgICAgICAgICAgICAgICBicmVha1xuICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgICBkZWZhdWx0OiB7XG4gICAgICAgICAgICAgICAgICBjb25zdCBlcnJvck1lc3NhZ2UgPSBgVW5leHBlY3RlZCBjb250ZW50LXR5cGUgJHtjb250ZW50VHlwZX0gc2VudCBmb3IgZXZlbnQtdHlwZSBQcm9ncmVzc2BcbiAgICAgICAgICAgICAgICAgIHRocm93IG5ldyBFcnJvcihlcnJvck1lc3NhZ2UpXG4gICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICB9XG4gICAgICAgICAgICB9XG4gICAgICAgICAgICBicmVha1xuICAgICAgICAgIGNhc2UgJ1N0YXRzJzpcbiAgICAgICAgICAgIHtcbiAgICAgICAgICAgICAgc3dpdGNoIChjb250ZW50VHlwZSkge1xuICAgICAgICAgICAgICAgIGNhc2UgJ3RleHQveG1sJzoge1xuICAgICAgICAgICAgICAgICAgY29uc3Qgc3RhdHNEYXRhID0gcGF5bG9hZFN0cmVhbT8ucmVhZChwYXlMb2FkTGVuZ3RoKVxuICAgICAgICAgICAgICAgICAgc2VsZWN0UmVzdWx0cy5zZXRTdGF0cyhzdGF0c0RhdGEudG9TdHJpbmcoKSlcbiAgICAgICAgICAgICAgICAgIGJyZWFrXG4gICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICAgIGRlZmF1bHQ6IHtcbiAgICAgICAgICAgICAgICAgIGNvbnN0IGVycm9yTWVzc2FnZSA9IGBVbmV4cGVjdGVkIGNvbnRlbnQtdHlwZSAke2NvbnRlbnRUeXBlfSBzZW50IGZvciBldmVudC10eXBlIFN0YXRzYFxuICAgICAgICAgICAgICAgICAgdGhyb3cgbmV3IEVycm9yKGVycm9yTWVzc2FnZSlcbiAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgIH1cbiAgICAgICAgICAgIH1cbiAgICAgICAgICAgIGJyZWFrXG4gICAgICAgICAgZGVmYXVsdDoge1xuICAgICAgICAgICAgLy8gQ29udGludWF0aW9uIG1lc3NhZ2U6IE5vdCBzdXJlIGlmIGl0IGlzIHN1cHBvcnRlZC4gZGlkIG5vdCBmaW5kIGEgcmVmZXJlbmNlIG9yIGFueSBtZXNzYWdlIGluIHJlc3BvbnNlLlxuICAgICAgICAgICAgLy8gSXQgZG9lcyBub3QgaGF2ZSBhIHBheWxvYWQuXG4gICAgICAgICAgICBjb25zdCB3YXJuaW5nTWVzc2FnZSA9IGBVbiBpbXBsZW1lbnRlZCBldmVudCBkZXRlY3RlZCAgJHttZXNzYWdlVHlwZX0uYFxuICAgICAgICAgICAgLy8gZXNsaW50LWRpc2FibGUtbmV4dC1saW5lIG5vLWNvbnNvbGVcbiAgICAgICAgICAgIGNvbnNvbGUud2Fybih3YXJuaW5nTWVzc2FnZSlcbiAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9XG4gIH1cbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHBhcnNlTGlmZWN5Y2xlQ29uZmlnKHhtbDogc3RyaW5nKSB7XG4gIGNvbnN0IHhtbE9iaiA9IHBhcnNlWG1sKHhtbClcbiAgY29uc3QgbGlmZWN5Y2xlQ29uZmlnID0geG1sT2JqLkxpZmVjeWNsZUNvbmZpZ3VyYXRpb25cbiAgaWYgKGxpZmVjeWNsZUNvbmZpZyAmJiBsaWZlY3ljbGVDb25maWcuUnVsZSkge1xuICAgIC8vIEEgc2luZ2xlIDxSdWxlPiBpcyBwYXJzZWQgYXMgYW4gb2JqZWN0OyBub3JtYWxpemUgdG8gYW4gYXJyYXkgc28gdGhlXG4gICAgLy8gcmVzdWx0IG1hdGNoZXMgdGhlIExpZmVjeWNsZUNvbmZpZyB0eXBlIGFuZCB0aGUgbXVsdGlwbGUtcnVsZXMgY2FzZS4gU2VlICMxNDA3LlxuICAgIGxpZmVjeWNsZUNvbmZpZy5SdWxlID0gdG9BcnJheShsaWZlY3ljbGVDb25maWcuUnVsZSlcbiAgfVxuICByZXR1cm4gbGlmZWN5Y2xlQ29uZmlnXG59XG5cbmV4cG9ydCBmdW5jdGlvbiBwYXJzZUJ1Y2tldEVuY3J5cHRpb25Db25maWcoeG1sOiBzdHJpbmcpIHtcbiAgcmV0dXJuIHBhcnNlWG1sKHhtbClcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHBhcnNlT2JqZWN0UmV0ZW50aW9uQ29uZmlnKHhtbDogc3RyaW5nKSB7XG4gIGNvbnN0IHhtbE9iaiA9IHBhcnNlWG1sKHhtbClcbiAgY29uc3QgcmV0ZW50aW9uQ29uZmlnID0geG1sT2JqLlJldGVudGlvblxuICByZXR1cm4ge1xuICAgIG1vZGU6IHJldGVudGlvbkNvbmZpZy5Nb2RlLFxuICAgIHJldGFpblVudGlsRGF0ZTogcmV0ZW50aW9uQ29uZmlnLlJldGFpblVudGlsRGF0ZSxcbiAgfVxufVxuXG5leHBvcnQgZnVuY3Rpb24gcmVtb3ZlT2JqZWN0c1BhcnNlcih4bWw6IHN0cmluZykge1xuICBjb25zdCB4bWxPYmogPSBwYXJzZVhtbCh4bWwpXG4gIGlmICh4bWxPYmouRGVsZXRlUmVzdWx0ICYmIHhtbE9iai5EZWxldGVSZXN1bHQuRXJyb3IpIHtcbiAgICAvLyByZXR1cm4gZXJyb3JzIGFzIGFycmF5IGFsd2F5cy4gYXMgdGhlIHJlc3BvbnNlIGlzIG9iamVjdCBpbiBjYXNlIG9mIHNpbmdsZSBvYmplY3QgcGFzc2VkIGluIHJlbW92ZU9iamVjdHNcbiAgICByZXR1cm4gdG9BcnJheSh4bWxPYmouRGVsZXRlUmVzdWx0LkVycm9yKVxuICB9XG4gIHJldHVybiBbXVxufVxuXG4vLyBwYXJzZSBYTUwgcmVzcG9uc2UgZm9yIGNvcHkgb2JqZWN0XG5leHBvcnQgZnVuY3Rpb24gcGFyc2VDb3B5T2JqZWN0KHhtbDogc3RyaW5nKTogQ29weU9iamVjdFJlc3VsdFYxIHtcbiAgY29uc3QgcmVzdWx0OiBDb3B5T2JqZWN0UmVzdWx0VjEgPSB7XG4gICAgZXRhZzogJycsXG4gICAgbGFzdE1vZGlmaWVkOiAnJyxcbiAgfVxuXG4gIGxldCB4bWxvYmogPSBwYXJzZVhtbCh4bWwpXG4gIGlmICgheG1sb2JqLkNvcHlPYmplY3RSZXN1bHQpIHtcbiAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRYTUxFcnJvcignTWlzc2luZyB0YWc6IFwiQ29weU9iamVjdFJlc3VsdFwiJylcbiAgfVxuICB4bWxvYmogPSB4bWxvYmouQ29weU9iamVjdFJlc3VsdFxuICBpZiAoeG1sb2JqLkVUYWcpIHtcbiAgICByZXN1bHQuZXRhZyA9IHhtbG9iai5FVGFnLnJlcGxhY2UoL15cIi9nLCAnJylcbiAgICAgIC5yZXBsYWNlKC9cIiQvZywgJycpXG4gICAgICAucmVwbGFjZSgvXiZxdW90Oy9nLCAnJylcbiAgICAgIC5yZXBsYWNlKC8mcXVvdDskL2csICcnKVxuICAgICAgLnJlcGxhY2UoL14mIzM0Oy9nLCAnJylcbiAgICAgIC5yZXBsYWNlKC8mIzM0OyQvZywgJycpXG4gIH1cbiAgaWYgKHhtbG9iai5MYXN0TW9kaWZpZWQpIHtcbiAgICByZXN1bHQubGFzdE1vZGlmaWVkID0gbmV3IERhdGUoeG1sb2JqLkxhc3RNb2RpZmllZClcbiAgfVxuXG4gIHJldHVybiByZXN1bHRcbn1cblxuY29uc3QgZm9ybWF0T2JqSW5mbyA9IChjb250ZW50OiBPYmplY3RSb3dFbnRyeSwgb3B0czogeyBJc0RlbGV0ZU1hcmtlcj86IGJvb2xlYW4gfSA9IHt9KSA9PiB7XG4gIGNvbnN0IHsgS2V5LCBMYXN0TW9kaWZpZWQsIEVUYWcsIFNpemUsIFZlcnNpb25JZCwgSXNMYXRlc3QgfSA9IGNvbnRlbnRcblxuICBpZiAoIWlzT2JqZWN0KG9wdHMpKSB7XG4gICAgb3B0cyA9IHt9XG4gIH1cblxuICBjb25zdCBuYW1lID0gc2FuaXRpemVPYmplY3RLZXkodG9BcnJheShLZXkpWzBdIHx8ICcnKVxuICBjb25zdCBsYXN0TW9kaWZpZWQgPSBMYXN0TW9kaWZpZWQgPyBuZXcgRGF0ZSh0b0FycmF5KExhc3RNb2RpZmllZClbMF0gfHwgJycpIDogdW5kZWZpbmVkXG4gIGNvbnN0IGV0YWcgPSBzYW5pdGl6ZUVUYWcodG9BcnJheShFVGFnKVswXSB8fCAnJylcbiAgY29uc3Qgc2l6ZSA9IHNhbml0aXplU2l6ZShTaXplIHx8ICcnKVxuXG4gIHJldHVybiB7XG4gICAgbmFtZSxcbiAgICBsYXN0TW9kaWZpZWQsXG4gICAgZXRhZyxcbiAgICBzaXplLFxuICAgIHZlcnNpb25JZDogVmVyc2lvbklkLFxuICAgIGlzTGF0ZXN0OiBJc0xhdGVzdCxcbiAgICBpc0RlbGV0ZU1hcmtlcjogb3B0cy5Jc0RlbGV0ZU1hcmtlciA/IG9wdHMuSXNEZWxldGVNYXJrZXIgOiBmYWxzZSxcbiAgfVxufVxuXG4vLyBwYXJzZSBYTUwgcmVzcG9uc2UgZm9yIGxpc3Qgb2JqZWN0cyBpbiBhIGJ1Y2tldFxuZXhwb3J0IGZ1bmN0aW9uIHBhcnNlTGlzdE9iamVjdHMoeG1sOiBzdHJpbmcpIHtcbiAgY29uc3QgcmVzdWx0OiB7XG4gICAgb2JqZWN0czogT2JqZWN0SW5mb1tdXG4gICAgaXNUcnVuY2F0ZWQ/OiBib29sZWFuXG4gICAgbmV4dE1hcmtlcj86IHN0cmluZ1xuICAgIHZlcnNpb25JZE1hcmtlcj86IHN0cmluZ1xuICAgIGtleU1hcmtlcj86IHN0cmluZ1xuICB9ID0ge1xuICAgIG9iamVjdHM6IFtdLFxuICAgIGlzVHJ1bmNhdGVkOiBmYWxzZSxcbiAgICBuZXh0TWFya2VyOiB1bmRlZmluZWQsXG4gICAgdmVyc2lvbklkTWFya2VyOiB1bmRlZmluZWQsXG4gICAga2V5TWFya2VyOiB1bmRlZmluZWQsXG4gIH1cbiAgbGV0IGlzVHJ1bmNhdGVkID0gZmFsc2VcbiAgbGV0IG5leHRNYXJrZXJcbiAgY29uc3QgeG1sb2JqID0gZnhwV2l0aG91dE51bVBhcnNlci5wYXJzZSh4bWwpXG5cbiAgY29uc3QgcGFyc2VDb21tb25QcmVmaXhlc0VudGl0eSA9IChjb21tb25QcmVmaXhFbnRyeTogQ29tbW9uUHJlZml4W10pID0+IHtcbiAgICBpZiAoY29tbW9uUHJlZml4RW50cnkpIHtcbiAgICAgIHRvQXJyYXkoY29tbW9uUHJlZml4RW50cnkpLmZvckVhY2goKGNvbW1vblByZWZpeCkgPT4ge1xuICAgICAgICByZXN1bHQub2JqZWN0cy5wdXNoKHsgcHJlZml4OiBzYW5pdGl6ZU9iamVjdEtleSh0b0FycmF5KGNvbW1vblByZWZpeC5QcmVmaXgpWzBdIHx8ICcnKSwgc2l6ZTogMCB9KVxuICAgICAgfSlcbiAgICB9XG4gIH1cblxuICBjb25zdCBsaXN0QnVja2V0UmVzdWx0OiBMaXN0QnVja2V0UmVzdWx0VjEgPSB4bWxvYmouTGlzdEJ1Y2tldFJlc3VsdFxuICBjb25zdCBsaXN0VmVyc2lvbnNSZXN1bHQ6IExpc3RCdWNrZXRSZXN1bHRWMSA9IHhtbG9iai5MaXN0VmVyc2lvbnNSZXN1bHRcblxuICBpZiAobGlzdEJ1Y2tldFJlc3VsdCkge1xuICAgIGlmIChsaXN0QnVja2V0UmVzdWx0LklzVHJ1bmNhdGVkKSB7XG4gICAgICBpc1RydW5jYXRlZCA9IGxpc3RCdWNrZXRSZXN1bHQuSXNUcnVuY2F0ZWRcbiAgICB9XG4gICAgaWYgKGxpc3RCdWNrZXRSZXN1bHQuQ29udGVudHMpIHtcbiAgICAgIHRvQXJyYXkobGlzdEJ1Y2tldFJlc3VsdC5Db250ZW50cykuZm9yRWFjaCgoY29udGVudCkgPT4ge1xuICAgICAgICBjb25zdCBuYW1lID0gc2FuaXRpemVPYmplY3RLZXkodG9BcnJheShjb250ZW50LktleSlbMF0gfHwgJycpXG4gICAgICAgIGNvbnN0IGxhc3RNb2RpZmllZCA9IG5ldyBEYXRlKHRvQXJyYXkoY29udGVudC5MYXN0TW9kaWZpZWQpWzBdIHx8ICcnKVxuICAgICAgICBjb25zdCBldGFnID0gc2FuaXRpemVFVGFnKHRvQXJyYXkoY29udGVudC5FVGFnKVswXSB8fCAnJylcbiAgICAgICAgY29uc3Qgc2l6ZSA9IHNhbml0aXplU2l6ZShjb250ZW50LlNpemUgfHwgJycpXG4gICAgICAgIHJlc3VsdC5vYmplY3RzLnB1c2goeyBuYW1lLCBsYXN0TW9kaWZpZWQsIGV0YWcsIHNpemUgfSlcbiAgICAgIH0pXG4gICAgfVxuXG4gICAgaWYgKGxpc3RCdWNrZXRSZXN1bHQuTWFya2VyKSB7XG4gICAgICBuZXh0TWFya2VyID0gbGlzdEJ1Y2tldFJlc3VsdC5NYXJrZXJcbiAgICB9XG4gICAgaWYgKGxpc3RCdWNrZXRSZXN1bHQuTmV4dE1hcmtlcikge1xuICAgICAgbmV4dE1hcmtlciA9IGxpc3RCdWNrZXRSZXN1bHQuTmV4dE1hcmtlclxuICAgIH0gZWxzZSBpZiAoaXNUcnVuY2F0ZWQgJiYgcmVzdWx0Lm9iamVjdHMubGVuZ3RoID4gMCkge1xuICAgICAgbmV4dE1hcmtlciA9IHJlc3VsdC5vYmplY3RzW3Jlc3VsdC5vYmplY3RzLmxlbmd0aCAtIDFdPy5uYW1lXG4gICAgfVxuICAgIGlmIChsaXN0QnVja2V0UmVzdWx0LkNvbW1vblByZWZpeGVzKSB7XG4gICAgICBwYXJzZUNvbW1vblByZWZpeGVzRW50aXR5KGxpc3RCdWNrZXRSZXN1bHQuQ29tbW9uUHJlZml4ZXMpXG4gICAgfVxuICB9XG5cbiAgaWYgKGxpc3RWZXJzaW9uc1Jlc3VsdCkge1xuICAgIGlmIChsaXN0VmVyc2lvbnNSZXN1bHQuSXNUcnVuY2F0ZWQpIHtcbiAgICAgIGlzVHJ1bmNhdGVkID0gbGlzdFZlcnNpb25zUmVzdWx0LklzVHJ1bmNhdGVkXG4gICAgfVxuXG4gICAgaWYgKGxpc3RWZXJzaW9uc1Jlc3VsdC5WZXJzaW9uKSB7XG4gICAgICB0b0FycmF5KGxpc3RWZXJzaW9uc1Jlc3VsdC5WZXJzaW9uKS5mb3JFYWNoKChjb250ZW50KSA9PiB7XG4gICAgICAgIHJlc3VsdC5vYmplY3RzLnB1c2goZm9ybWF0T2JqSW5mbyhjb250ZW50KSlcbiAgICAgIH0pXG4gICAgfVxuICAgIGlmIChsaXN0VmVyc2lvbnNSZXN1bHQuRGVsZXRlTWFya2VyKSB7XG4gICAgICB0b0FycmF5KGxpc3RWZXJzaW9uc1Jlc3VsdC5EZWxldGVNYXJrZXIpLmZvckVhY2goKGNvbnRlbnQpID0+IHtcbiAgICAgICAgcmVzdWx0Lm9iamVjdHMucHVzaChmb3JtYXRPYmpJbmZvKGNvbnRlbnQsIHsgSXNEZWxldGVNYXJrZXI6IHRydWUgfSkpXG4gICAgICB9KVxuICAgIH1cblxuICAgIGlmIChsaXN0VmVyc2lvbnNSZXN1bHQuTmV4dEtleU1hcmtlcikge1xuICAgICAgcmVzdWx0LmtleU1hcmtlciA9IGxpc3RWZXJzaW9uc1Jlc3VsdC5OZXh0S2V5TWFya2VyXG4gICAgfVxuICAgIGlmIChsaXN0VmVyc2lvbnNSZXN1bHQuTmV4dFZlcnNpb25JZE1hcmtlcikge1xuICAgICAgcmVzdWx0LnZlcnNpb25JZE1hcmtlciA9IGxpc3RWZXJzaW9uc1Jlc3VsdC5OZXh0VmVyc2lvbklkTWFya2VyXG4gICAgfVxuICAgIGlmIChsaXN0VmVyc2lvbnNSZXN1bHQuQ29tbW9uUHJlZml4ZXMpIHtcbiAgICAgIHBhcnNlQ29tbW9uUHJlZml4ZXNFbnRpdHkobGlzdFZlcnNpb25zUmVzdWx0LkNvbW1vblByZWZpeGVzKVxuICAgIH1cbiAgfVxuXG4gIHJlc3VsdC5pc1RydW5jYXRlZCA9IGlzVHJ1bmNhdGVkXG4gIGlmIChpc1RydW5jYXRlZCkge1xuICAgIHJlc3VsdC5uZXh0TWFya2VyID0gbmV4dE1hcmtlclxuICB9XG4gIHJldHVybiByZXN1bHRcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHVwbG9hZFBhcnRQYXJzZXIoeG1sOiBzdHJpbmcpIHtcbiAgY29uc3QgeG1sT2JqID0gcGFyc2VYbWwoeG1sKVxuICBjb25zdCByZXNwRWwgPSB4bWxPYmouQ29weVBhcnRSZXN1bHRcbiAgcmV0dXJuIHJlc3BFbFxufVxuIl0sIm1hcHBpbmdzIjoiOzs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7Ozs7OztBQUdBLElBQUFBLFVBQUEsR0FBQUMsc0JBQUEsQ0FBQUMsT0FBQTtBQUNBLElBQUFDLGNBQUEsR0FBQUQsT0FBQTtBQUVBLElBQUFFLE1BQUEsR0FBQUMsdUJBQUEsQ0FBQUgsT0FBQTtBQUNBLElBQUFJLFFBQUEsR0FBQUosT0FBQTtBQUNBLElBQUFLLE9BQUEsR0FBQUwsT0FBQTtBQUNBLElBQUFNLFNBQUEsR0FBQU4sT0FBQTtBQW1CQSxJQUFBTyxLQUFBLEdBQUFQLE9BQUE7QUFBb0QsU0FBQUcsd0JBQUFLLENBQUEsRUFBQUMsQ0FBQSw2QkFBQUMsT0FBQSxNQUFBQyxDQUFBLE9BQUFELE9BQUEsSUFBQUUsQ0FBQSxPQUFBRixPQUFBLFlBQUFQLHVCQUFBLFlBQUFBLENBQUFLLENBQUEsRUFBQUMsQ0FBQSxTQUFBQSxDQUFBLElBQUFELENBQUEsSUFBQUEsQ0FBQSxDQUFBSyxVQUFBLFNBQUFMLENBQUEsTUFBQU0sQ0FBQSxFQUFBQyxDQUFBLEVBQUFDLENBQUEsS0FBQUMsU0FBQSxRQUFBQyxPQUFBLEVBQUFWLENBQUEsaUJBQUFBLENBQUEsdUJBQUFBLENBQUEseUJBQUFBLENBQUEsU0FBQVEsQ0FBQSxNQUFBRixDQUFBLEdBQUFMLENBQUEsR0FBQUcsQ0FBQSxHQUFBRCxDQUFBLFFBQUFHLENBQUEsQ0FBQUssR0FBQSxDQUFBWCxDQUFBLFVBQUFNLENBQUEsQ0FBQU0sR0FBQSxDQUFBWixDQUFBLEdBQUFNLENBQUEsQ0FBQU8sR0FBQSxDQUFBYixDQUFBLEVBQUFRLENBQUEsZ0JBQUFQLENBQUEsSUFBQUQsQ0FBQSxnQkFBQUMsQ0FBQSxPQUFBYSxjQUFBLENBQUFDLElBQUEsQ0FBQWYsQ0FBQSxFQUFBQyxDQUFBLE9BQUFNLENBQUEsSUFBQUQsQ0FBQSxHQUFBVSxNQUFBLENBQUFDLGNBQUEsS0FBQUQsTUFBQSxDQUFBRSx3QkFBQSxDQUFBbEIsQ0FBQSxFQUFBQyxDQUFBLE9BQUFNLENBQUEsQ0FBQUssR0FBQSxJQUFBTCxDQUFBLENBQUFNLEdBQUEsSUFBQVAsQ0FBQSxDQUFBRSxDQUFBLEVBQUFQLENBQUEsRUFBQU0sQ0FBQSxJQUFBQyxDQUFBLENBQUFQLENBQUEsSUFBQUQsQ0FBQSxDQUFBQyxDQUFBLFdBQUFPLENBQUEsS0FBQVIsQ0FBQSxFQUFBQyxDQUFBO0FBQUEsU0FBQVYsdUJBQUFTLENBQUEsV0FBQUEsQ0FBQSxJQUFBQSxDQUFBLENBQUFLLFVBQUEsR0FBQUwsQ0FBQSxLQUFBVSxPQUFBLEVBQUFWLENBQUE7QUFFcEQ7QUFDTyxTQUFTbUIsaUJBQWlCQSxDQUFDQyxHQUFXLEVBQVU7RUFDckQ7RUFDQSxPQUFPLElBQUFDLGdCQUFRLEVBQUNELEdBQUcsQ0FBQyxDQUFDRSxrQkFBa0I7QUFDekM7QUFFQSxNQUFNQyxHQUFHLEdBQUcsSUFBSUMsd0JBQVMsQ0FBQyxDQUFDO0FBRTNCLE1BQU1DLG1CQUFtQixHQUFHLElBQUlELHdCQUFTLENBQUM7RUFDeEM7RUFDQUUsa0JBQWtCLEVBQUU7SUFDbEJDLFFBQVEsRUFBRTtFQUNaO0FBQ0YsQ0FBQyxDQUFDOztBQUVGO0FBQ0E7QUFDTyxTQUFTQyxVQUFVQSxDQUFDUixHQUFXLEVBQUVTLFVBQW1DLEVBQUU7RUFDM0UsSUFBSUMsTUFBTSxHQUFHLENBQUMsQ0FBQztFQUNmLE1BQU1DLE1BQU0sR0FBR1IsR0FBRyxDQUFDUyxLQUFLLENBQUNaLEdBQUcsQ0FBQztFQUM3QixJQUFJVyxNQUFNLENBQUNFLEtBQUssRUFBRTtJQUNoQkgsTUFBTSxHQUFHQyxNQUFNLENBQUNFLEtBQUs7RUFDdkI7RUFDQSxNQUFNakMsQ0FBQyxHQUFHLElBQUlOLE1BQU0sQ0FBQ3dDLE9BQU8sQ0FBQyxDQUF1QztFQUNwRWxCLE1BQU0sQ0FBQ21CLE9BQU8sQ0FBQ0wsTUFBTSxDQUFDLENBQUNNLE9BQU8sQ0FBQyxDQUFDLENBQUNDLEdBQUcsRUFBRUMsS0FBSyxDQUFDLEtBQUs7SUFDL0N0QyxDQUFDLENBQUNxQyxHQUFHLENBQUNFLFdBQVcsQ0FBQyxDQUFDLENBQUMsR0FBR0QsS0FBSztFQUM5QixDQUFDLENBQUM7RUFDRnRCLE1BQU0sQ0FBQ21CLE9BQU8sQ0FBQ04sVUFBVSxDQUFDLENBQUNPLE9BQU8sQ0FBQyxDQUFDLENBQUNDLEdBQUcsRUFBRUMsS0FBSyxDQUFDLEtBQUs7SUFDbkR0QyxDQUFDLENBQUNxQyxHQUFHLENBQUMsR0FBR0MsS0FBSztFQUNoQixDQUFDLENBQUM7RUFDRixPQUFPdEMsQ0FBQztBQUNWOztBQUVBO0FBQ08sZUFBZXdDLGtCQUFrQkEsQ0FBQ0MsUUFBOEIsRUFBbUM7RUFDeEcsTUFBTUMsVUFBVSxHQUFHRCxRQUFRLENBQUNDLFVBQVU7RUFDdEMsSUFBSUMsSUFBSSxHQUFHLEVBQUU7SUFDWEMsT0FBTyxHQUFHLEVBQUU7RUFDZCxJQUFJRixVQUFVLEtBQUssR0FBRyxFQUFFO0lBQ3RCQyxJQUFJLEdBQUcsa0JBQWtCO0lBQ3pCQyxPQUFPLEdBQUcsbUJBQW1CO0VBQy9CLENBQUMsTUFBTSxJQUFJRixVQUFVLEtBQUssR0FBRyxFQUFFO0lBQzdCQyxJQUFJLEdBQUcsbUJBQW1CO0lBQzFCQyxPQUFPLEdBQUcseUNBQXlDO0VBQ3JELENBQUMsTUFBTSxJQUFJRixVQUFVLEtBQUssR0FBRyxFQUFFO0lBQzdCQyxJQUFJLEdBQUcsY0FBYztJQUNyQkMsT0FBTyxHQUFHLDJDQUEyQztFQUN2RCxDQUFDLE1BQU0sSUFBSUYsVUFBVSxLQUFLLEdBQUcsRUFBRTtJQUM3QkMsSUFBSSxHQUFHLFVBQVU7SUFDakJDLE9BQU8sR0FBRyxXQUFXO0VBQ3ZCLENBQUMsTUFBTSxJQUFJRixVQUFVLEtBQUssR0FBRyxFQUFFO0lBQzdCQyxJQUFJLEdBQUcsa0JBQWtCO0lBQ3pCQyxPQUFPLEdBQUcsb0JBQW9CO0VBQ2hDLENBQUMsTUFBTSxJQUFJRixVQUFVLEtBQUssR0FBRyxFQUFFO0lBQzdCQyxJQUFJLEdBQUcsa0JBQWtCO0lBQ3pCQyxPQUFPLEdBQUcsb0JBQW9CO0VBQ2hDLENBQUMsTUFBTSxJQUFJRixVQUFVLEtBQUssR0FBRyxFQUFFO0lBQzdCQyxJQUFJLEdBQUcsVUFBVTtJQUNqQkMsT0FBTyxHQUFHLGtDQUFrQztFQUM5QyxDQUFDLE1BQU07SUFDTCxNQUFNQyxRQUFRLEdBQUdKLFFBQVEsQ0FBQ0ssT0FBTyxDQUFDLG9CQUFvQixDQUFXO0lBQ2pFLE1BQU1DLFFBQVEsR0FBR04sUUFBUSxDQUFDSyxPQUFPLENBQUMsb0JBQW9CLENBQVc7SUFFakUsSUFBSUQsUUFBUSxJQUFJRSxRQUFRLEVBQUU7TUFDeEJKLElBQUksR0FBR0UsUUFBUTtNQUNmRCxPQUFPLEdBQUdHLFFBQVE7SUFDcEI7RUFDRjtFQUNBLE1BQU1sQixVQUFxRCxHQUFHLENBQUMsQ0FBQztFQUNoRTtFQUNBQSxVQUFVLENBQUNtQixZQUFZLEdBQUdQLFFBQVEsQ0FBQ0ssT0FBTyxDQUFDLGtCQUFrQixDQUF1QjtFQUNwRjtFQUNBakIsVUFBVSxDQUFDb0IsTUFBTSxHQUFHUixRQUFRLENBQUNLLE9BQU8sQ0FBQyxZQUFZLENBQXVCOztFQUV4RTtFQUNBO0VBQ0FqQixVQUFVLENBQUNxQixlQUFlLEdBQUdULFFBQVEsQ0FBQ0ssT0FBTyxDQUFDLHFCQUFxQixDQUF1QjtFQUUxRixNQUFNSyxTQUFTLEdBQUcsTUFBTSxJQUFBQyxzQkFBWSxFQUFDWCxRQUFRLENBQUM7RUFFOUMsSUFBSVUsU0FBUyxFQUFFO0lBQ2IsTUFBTXZCLFVBQVUsQ0FBQ3VCLFNBQVMsRUFBRXRCLFVBQVUsQ0FBQztFQUN6Qzs7RUFFQTtFQUNBLE1BQU03QixDQUFDLEdBQUcsSUFBSU4sTUFBTSxDQUFDd0MsT0FBTyxDQUFDVSxPQUFPLEVBQUU7SUFBRVMsS0FBSyxFQUFFeEI7RUFBVyxDQUFDLENBQUM7RUFDNUQ7RUFDQTdCLENBQUMsQ0FBQzJDLElBQUksR0FBR0EsSUFBSTtFQUNiM0IsTUFBTSxDQUFDbUIsT0FBTyxDQUFDTixVQUFVLENBQUMsQ0FBQ08sT0FBTyxDQUFDLENBQUMsQ0FBQ0MsR0FBRyxFQUFFQyxLQUFLLENBQUMsS0FBSztJQUNuRDtJQUNBdEMsQ0FBQyxDQUFDcUMsR0FBRyxDQUFDLEdBQUdDLEtBQUs7RUFDaEIsQ0FBQyxDQUFDO0VBRUYsTUFBTXRDLENBQUM7QUFDVDs7QUFFQTtBQUNBO0FBQ0E7QUFDTyxTQUFTc0QsOEJBQThCQSxDQUFDbEMsR0FBVyxFQUFFO0VBQzFELE1BQU1tQyxNQUlMLEdBQUc7SUFDRkMsT0FBTyxFQUFFLEVBQUU7SUFDWEMsV0FBVyxFQUFFLEtBQUs7SUFDbEJDLHFCQUFxQixFQUFFO0VBQ3pCLENBQUM7RUFFRCxJQUFJQyxNQUFNLEdBQUcsSUFBQXRDLGdCQUFRLEVBQUNELEdBQUcsQ0FBQztFQUMxQixJQUFJLENBQUN1QyxNQUFNLENBQUNDLGdCQUFnQixFQUFFO0lBQzVCLE1BQU0sSUFBSWxFLE1BQU0sQ0FBQ21FLGVBQWUsQ0FBQyxpQ0FBaUMsQ0FBQztFQUNyRTtFQUNBRixNQUFNLEdBQUdBLE1BQU0sQ0FBQ0MsZ0JBQWdCO0VBQ2hDLElBQUlELE1BQU0sQ0FBQ0csV0FBVyxFQUFFO0lBQ3RCUCxNQUFNLENBQUNFLFdBQVcsR0FBR0UsTUFBTSxDQUFDRyxXQUFXO0VBQ3pDO0VBQ0EsSUFBSUgsTUFBTSxDQUFDSSxxQkFBcUIsRUFBRTtJQUNoQ1IsTUFBTSxDQUFDRyxxQkFBcUIsR0FBR0MsTUFBTSxDQUFDSSxxQkFBcUI7RUFDN0Q7RUFFQSxJQUFJSixNQUFNLENBQUNLLFFBQVEsRUFBRTtJQUNuQixJQUFBQyxlQUFPLEVBQUNOLE1BQU0sQ0FBQ0ssUUFBUSxDQUFDLENBQUM1QixPQUFPLENBQUU4QixPQUFPLElBQUs7TUFDNUMsTUFBTUMsSUFBSSxHQUFHLElBQUFDLHlCQUFpQixFQUFDRixPQUFPLENBQUNHLEdBQUcsQ0FBQztNQUMzQyxNQUFNQyxZQUFZLEdBQUcsSUFBSUMsSUFBSSxDQUFDTCxPQUFPLENBQUNNLFlBQVksQ0FBQztNQUNuRCxNQUFNQyxJQUFJLEdBQUcsSUFBQUMsb0JBQVksRUFBQ1IsT0FBTyxDQUFDUyxJQUFJLENBQUM7TUFDdkMsTUFBTUMsSUFBSSxHQUFHVixPQUFPLENBQUNXLElBQUk7TUFFekIsSUFBSUMsSUFBVSxHQUFHLENBQUMsQ0FBQztNQUNuQixJQUFJWixPQUFPLENBQUNhLFFBQVEsSUFBSSxJQUFJLEVBQUU7UUFDNUIsSUFBQWQsZUFBTyxFQUFDQyxPQUFPLENBQUNhLFFBQVEsQ0FBQ0MsS0FBSyxDQUFDLEdBQUcsQ0FBQyxDQUFDLENBQUM1QyxPQUFPLENBQUU2QyxHQUFHLElBQUs7VUFDcEQsTUFBTSxDQUFDNUMsR0FBRyxFQUFFQyxLQUFLLENBQUMsR0FBRzJDLEdBQUcsQ0FBQ0QsS0FBSyxDQUFDLEdBQUcsQ0FBQztVQUNuQ0YsSUFBSSxDQUFDekMsR0FBRyxDQUFDLEdBQUdDLEtBQUs7UUFDbkIsQ0FBQyxDQUFDO01BQ0osQ0FBQyxNQUFNO1FBQ0x3QyxJQUFJLEdBQUcsQ0FBQyxDQUFDO01BQ1g7TUFFQSxJQUFJSSxRQUFRO01BQ1osSUFBSWhCLE9BQU8sQ0FBQ2lCLFlBQVksSUFBSSxJQUFJLEVBQUU7UUFDaENELFFBQVEsR0FBRyxJQUFBakIsZUFBTyxFQUFDQyxPQUFPLENBQUNpQixZQUFZLENBQUMsQ0FBQyxDQUFDLENBQUM7TUFDN0MsQ0FBQyxNQUFNO1FBQ0xELFFBQVEsR0FBRyxJQUFJO01BQ2pCO01BQ0EzQixNQUFNLENBQUNDLE9BQU8sQ0FBQzRCLElBQUksQ0FBQztRQUFFakIsSUFBSTtRQUFFRyxZQUFZO1FBQUVHLElBQUk7UUFBRUcsSUFBSTtRQUFFTSxRQUFRO1FBQUVKO01BQUssQ0FBQyxDQUFDO0lBQ3pFLENBQUMsQ0FBQztFQUNKO0VBRUEsSUFBSW5CLE1BQU0sQ0FBQzBCLGNBQWMsRUFBRTtJQUN6QixJQUFBcEIsZUFBTyxFQUFDTixNQUFNLENBQUMwQixjQUFjLENBQUMsQ0FBQ2pELE9BQU8sQ0FBRWtELFlBQVksSUFBSztNQUN2RC9CLE1BQU0sQ0FBQ0MsT0FBTyxDQUFDNEIsSUFBSSxDQUFDO1FBQUVHLE1BQU0sRUFBRSxJQUFBbkIseUJBQWlCLEVBQUMsSUFBQUgsZUFBTyxFQUFDcUIsWUFBWSxDQUFDRSxNQUFNLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQztRQUFFWixJQUFJLEVBQUU7TUFBRSxDQUFDLENBQUM7SUFDOUYsQ0FBQyxDQUFDO0VBQ0o7RUFDQSxPQUFPckIsTUFBTTtBQUNmO0FBRU8sU0FBU2tDLGtCQUFrQkEsQ0FBQ3JFLEdBQVcsRUFBbUI7RUFDL0QsTUFBTW1DLE1BQXVCLEdBQUc7SUFDOUJDLE9BQU8sRUFBRSxFQUFFO0lBQ1hDLFdBQVcsRUFBRSxLQUFLO0lBQ2xCQyxxQkFBcUIsRUFBRTtFQUN6QixDQUFDO0VBRUQsSUFBSUMsTUFBTSxHQUFHLElBQUF0QyxnQkFBUSxFQUFDRCxHQUFHLENBQUM7RUFDMUIsSUFBSSxDQUFDdUMsTUFBTSxDQUFDQyxnQkFBZ0IsRUFBRTtJQUM1QixNQUFNLElBQUlsRSxNQUFNLENBQUNtRSxlQUFlLENBQUMsaUNBQWlDLENBQUM7RUFDckU7RUFDQUYsTUFBTSxHQUFHQSxNQUFNLENBQUNDLGdCQUFnQjtFQUNoQyxJQUFJRCxNQUFNLENBQUNHLFdBQVcsRUFBRTtJQUN0QlAsTUFBTSxDQUFDRSxXQUFXLEdBQUdFLE1BQU0sQ0FBQ0csV0FBVztFQUN6QztFQUNBLElBQUlILE1BQU0sQ0FBQ0kscUJBQXFCLEVBQUU7SUFDaENSLE1BQU0sQ0FBQ0cscUJBQXFCLEdBQUdDLE1BQU0sQ0FBQ0kscUJBQXFCO0VBQzdEO0VBQ0EsSUFBSUosTUFBTSxDQUFDSyxRQUFRLEVBQUU7SUFDbkIsSUFBQUMsZUFBTyxFQUFDTixNQUFNLENBQUNLLFFBQVEsQ0FBQyxDQUFDNUIsT0FBTyxDQUFFOEIsT0FBTyxJQUFLO01BQzVDLE1BQU1DLElBQUksR0FBRyxJQUFBQyx5QkFBaUIsRUFBQyxJQUFBSCxlQUFPLEVBQUNDLE9BQU8sQ0FBQ0csR0FBRyxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUM7TUFDdkQsTUFBTUMsWUFBWSxHQUFHLElBQUlDLElBQUksQ0FBQ0wsT0FBTyxDQUFDTSxZQUFZLENBQUM7TUFDbkQsTUFBTUMsSUFBSSxHQUFHLElBQUFDLG9CQUFZLEVBQUNSLE9BQU8sQ0FBQ1MsSUFBSSxDQUFDO01BQ3ZDLE1BQU1DLElBQUksR0FBR1YsT0FBTyxDQUFDVyxJQUFJO01BQ3pCdEIsTUFBTSxDQUFDQyxPQUFPLENBQUM0QixJQUFJLENBQUM7UUFBRWpCLElBQUk7UUFBRUcsWUFBWTtRQUFFRyxJQUFJO1FBQUVHO01BQUssQ0FBQyxDQUFDO0lBQ3pELENBQUMsQ0FBQztFQUNKO0VBQ0EsSUFBSWpCLE1BQU0sQ0FBQzBCLGNBQWMsRUFBRTtJQUN6QixJQUFBcEIsZUFBTyxFQUFDTixNQUFNLENBQUMwQixjQUFjLENBQUMsQ0FBQ2pELE9BQU8sQ0FBRWtELFlBQVksSUFBSztNQUN2RC9CLE1BQU0sQ0FBQ0MsT0FBTyxDQUFDNEIsSUFBSSxDQUFDO1FBQUVHLE1BQU0sRUFBRSxJQUFBbkIseUJBQWlCLEVBQUMsSUFBQUgsZUFBTyxFQUFDcUIsWUFBWSxDQUFDRSxNQUFNLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQztRQUFFWixJQUFJLEVBQUU7TUFBRSxDQUFDLENBQUM7SUFDOUYsQ0FBQyxDQUFDO0VBQ0o7RUFDQSxPQUFPckIsTUFBTTtBQUNmO0FBRU8sU0FBU21DLHVCQUF1QkEsQ0FBQ3RFLEdBQVcsRUFBNEI7RUFDN0UsTUFBTW1DLE1BQWdDLEdBQUc7SUFDdkNvQyxrQkFBa0IsRUFBRSxFQUFFO0lBQ3RCQyxrQkFBa0IsRUFBRSxFQUFFO0lBQ3RCQywwQkFBMEIsRUFBRTtFQUM5QixDQUFDO0VBRUQsTUFBTUMsU0FBUyxHQUFJQyxNQUFlLElBQWU7SUFDL0MsSUFBSSxDQUFDQSxNQUFNLEVBQUU7TUFDWCxPQUFPLEVBQUU7SUFDWDtJQUNBLE9BQU8sSUFBQTlCLGVBQU8sRUFBQzhCLE1BQU0sQ0FBQztFQUN4QixDQUFDO0VBRUQsTUFBTUMsY0FBYyxHQUFJQyxPQUFnQixJQUF3QztJQUFBLElBQUFDLFdBQUE7SUFDOUUsTUFBTUMsS0FBd0MsR0FBRyxFQUFFO0lBQ25ELElBQUksQ0FBQ0YsT0FBTyxFQUFFO01BQ1osT0FBT0UsS0FBSztJQUNkO0lBQ0EsTUFBTUMsU0FBUyxHQUFHLElBQUFuQyxlQUFPLEVBQUNnQyxPQUFPLENBQThCO0lBQy9ELEtBQUFDLFdBQUEsR0FBSUUsU0FBUyxDQUFDLENBQUMsQ0FBQyxjQUFBRixXQUFBLGVBQVpBLFdBQUEsQ0FBY0csS0FBSyxFQUFFO01BQUEsSUFBQUMsVUFBQTtNQUN2QixNQUFNQyxRQUFRLEdBQUcsSUFBQXRDLGVBQU8sRUFBRW1DLFNBQVMsQ0FBQyxDQUFDLENBQUMsQ0FBNkJDLEtBQUssQ0FBOEI7TUFDdEcsS0FBQUMsVUFBQSxHQUFJQyxRQUFRLENBQUMsQ0FBQyxDQUFDLGNBQUFELFVBQUEsZUFBWEEsVUFBQSxDQUFhRSxVQUFVLEVBQUU7UUFDM0IsSUFBQXZDLGVBQU8sRUFBQ3NDLFFBQVEsQ0FBQyxDQUFDLENBQUMsQ0FBQ0MsVUFBVSxDQUFDLENBQUNwRSxPQUFPLENBQUVxRSxJQUFhLElBQUs7VUFDekQsTUFBTXRHLENBQUMsR0FBR3NHLElBQStCO1VBQ3pDLE1BQU1DLElBQUksR0FBRyxJQUFBekMsZUFBTyxFQUFDOUQsQ0FBQyxDQUFDdUcsSUFBSSxDQUFDLENBQUMsQ0FBQyxDQUFXO1VBQ3pDLE1BQU1DLEtBQUssR0FBRyxJQUFBMUMsZUFBTyxFQUFDOUQsQ0FBQyxDQUFDd0csS0FBSyxDQUFDLENBQUMsQ0FBQyxDQUFXO1VBQzNDUixLQUFLLENBQUNmLElBQUksQ0FBQztZQUFFc0IsSUFBSTtZQUFFQztVQUFNLENBQUMsQ0FBQztRQUM3QixDQUFDLENBQUM7TUFDSjtJQUNGO0lBQ0EsT0FBT1IsS0FBSztFQUNkLENBQUM7RUFFRCxJQUFJeEMsTUFBTSxHQUFHLElBQUF0QyxnQkFBUSxFQUFDRCxHQUFHLENBQUM7RUFDMUJ1QyxNQUFNLEdBQUdBLE1BQU0sQ0FBQ2lELHlCQUF5QjtFQUV6QyxJQUFJakQsTUFBTSxDQUFDZ0Msa0JBQWtCLEVBQUU7SUFDN0IsSUFBQTFCLGVBQU8sRUFBQ04sTUFBTSxDQUFDZ0Msa0JBQWtCLENBQUMsQ0FBQ3ZELE9BQU8sQ0FBRXlFLE1BQStCLElBQUs7TUFDOUUsTUFBTUMsRUFBRSxHQUFHLElBQUE3QyxlQUFPLEVBQUM0QyxNQUFNLENBQUNDLEVBQUUsQ0FBQyxDQUFDLENBQUMsQ0FBVztNQUMxQyxNQUFNQyxLQUFLLEdBQUcsSUFBQTlDLGVBQU8sRUFBQzRDLE1BQU0sQ0FBQ0UsS0FBSyxDQUFDLENBQUMsQ0FBQyxDQUFXO01BQ2hELE1BQU1DLEtBQUssR0FBR2xCLFNBQVMsQ0FBQ2UsTUFBTSxDQUFDRyxLQUFLLENBQUM7TUFDckMsTUFBTUMsTUFBTSxHQUFHakIsY0FBYyxDQUFDYSxNQUFNLENBQUNJLE1BQU0sQ0FBQztNQUM1QzFELE1BQU0sQ0FBQ29DLGtCQUFrQixDQUFDUCxJQUFJLENBQUM7UUFBRTBCLEVBQUU7UUFBRUMsS0FBSztRQUFFQyxLQUFLO1FBQUVDO01BQU8sQ0FBcUIsQ0FBQztJQUNsRixDQUFDLENBQUM7RUFDSjtFQUNBLElBQUl0RCxNQUFNLENBQUNpQyxrQkFBa0IsRUFBRTtJQUM3QixJQUFBM0IsZUFBTyxFQUFDTixNQUFNLENBQUNpQyxrQkFBa0IsQ0FBQyxDQUFDeEQsT0FBTyxDQUFFeUUsTUFBK0IsSUFBSztNQUM5RSxNQUFNQyxFQUFFLEdBQUcsSUFBQTdDLGVBQU8sRUFBQzRDLE1BQU0sQ0FBQ0MsRUFBRSxDQUFDLENBQUMsQ0FBQyxDQUFXO01BQzFDLE1BQU1JLEtBQUssR0FBRyxJQUFBakQsZUFBTyxFQUFDNEMsTUFBTSxDQUFDSyxLQUFLLENBQUMsQ0FBQyxDQUFDLENBQVc7TUFDaEQsTUFBTUYsS0FBSyxHQUFHbEIsU0FBUyxDQUFDZSxNQUFNLENBQUNHLEtBQUssQ0FBQztNQUNyQyxNQUFNQyxNQUFNLEdBQUdqQixjQUFjLENBQUNhLE1BQU0sQ0FBQ0ksTUFBTSxDQUFDO01BQzVDMUQsTUFBTSxDQUFDcUMsa0JBQWtCLENBQUNSLElBQUksQ0FBQztRQUFFMEIsRUFBRTtRQUFFSSxLQUFLO1FBQUVGLEtBQUs7UUFBRUM7TUFBTyxDQUFxQixDQUFDO0lBQ2xGLENBQUMsQ0FBQztFQUNKO0VBQ0EsSUFBSXRELE1BQU0sQ0FBQ2tDLDBCQUEwQixFQUFFO0lBQ3JDLElBQUE1QixlQUFPLEVBQUNOLE1BQU0sQ0FBQ2tDLDBCQUEwQixDQUFDLENBQUN6RCxPQUFPLENBQUV5RSxNQUErQixJQUFLO01BQ3RGLE1BQU1DLEVBQUUsR0FBRyxJQUFBN0MsZUFBTyxFQUFDNEMsTUFBTSxDQUFDQyxFQUFFLENBQUMsQ0FBQyxDQUFDLENBQVc7TUFDMUMsTUFBTUssYUFBYSxHQUFHLElBQUFsRCxlQUFPLEVBQUM0QyxNQUFNLENBQUNNLGFBQWEsQ0FBQyxDQUFDLENBQUMsQ0FBVztNQUNoRSxNQUFNSCxLQUFLLEdBQUdsQixTQUFTLENBQUNlLE1BQU0sQ0FBQ0csS0FBSyxDQUFDO01BQ3JDLE1BQU1DLE1BQU0sR0FBR2pCLGNBQWMsQ0FBQ2EsTUFBTSxDQUFDSSxNQUFNLENBQUM7TUFDNUMxRCxNQUFNLENBQUNzQywwQkFBMEIsQ0FBQ1QsSUFBSSxDQUFDO1FBQUUwQixFQUFFO1FBQUVLLGFBQWE7UUFBRUgsS0FBSztRQUFFQztNQUFPLENBQTZCLENBQUM7SUFDMUcsQ0FBQyxDQUFDO0VBQ0o7RUFFQSxPQUFPMUQsTUFBTTtBQUNmO0FBU0E7QUFDTyxTQUFTNkQsY0FBY0EsQ0FBQ2hHLEdBQVcsRUFJeEM7RUFDQSxJQUFJdUMsTUFBTSxHQUFHLElBQUF0QyxnQkFBUSxFQUFDRCxHQUFHLENBQUM7RUFDMUIsTUFBTW1DLE1BSUwsR0FBRztJQUNGRSxXQUFXLEVBQUUsS0FBSztJQUNsQjRELEtBQUssRUFBRSxFQUFFO0lBQ1RDLE1BQU0sRUFBRTtFQUNWLENBQUM7RUFDRCxJQUFJLENBQUMzRCxNQUFNLENBQUM0RCxlQUFlLEVBQUU7SUFDM0IsTUFBTSxJQUFJN0gsTUFBTSxDQUFDbUUsZUFBZSxDQUFDLGdDQUFnQyxDQUFDO0VBQ3BFO0VBQ0FGLE1BQU0sR0FBR0EsTUFBTSxDQUFDNEQsZUFBZTtFQUMvQixJQUFJNUQsTUFBTSxDQUFDRyxXQUFXLEVBQUU7SUFDdEJQLE1BQU0sQ0FBQ0UsV0FBVyxHQUFHRSxNQUFNLENBQUNHLFdBQVc7RUFDekM7RUFDQSxJQUFJSCxNQUFNLENBQUM2RCxvQkFBb0IsRUFBRTtJQUMvQmpFLE1BQU0sQ0FBQytELE1BQU0sR0FBRyxJQUFBckQsZUFBTyxFQUFDTixNQUFNLENBQUM2RCxvQkFBb0IsQ0FBQyxDQUFDLENBQUMsQ0FBQyxJQUFJLEVBQUU7RUFDL0Q7RUFDQSxJQUFJN0QsTUFBTSxDQUFDOEQsSUFBSSxFQUFFO0lBQ2YsSUFBQXhELGVBQU8sRUFBQ04sTUFBTSxDQUFDOEQsSUFBSSxDQUFDLENBQUNyRixPQUFPLENBQUVzRixDQUFDLElBQUs7TUFDbEMsTUFBTUMsSUFBSSxHQUFHQyxRQUFRLENBQUMsSUFBQTNELGVBQU8sRUFBQ3lELENBQUMsQ0FBQ0csVUFBVSxDQUFDLENBQUMsQ0FBQyxDQUFDLEVBQUUsRUFBRSxDQUFDO01BQ25ELE1BQU12RCxZQUFZLEdBQUcsSUFBSUMsSUFBSSxDQUFDbUQsQ0FBQyxDQUFDbEQsWUFBWSxDQUFDO01BQzdDLE1BQU1DLElBQUksR0FBR2lELENBQUMsQ0FBQy9DLElBQUksQ0FBQ21ELE9BQU8sQ0FBQyxLQUFLLEVBQUUsRUFBRSxDQUFDLENBQ25DQSxPQUFPLENBQUMsS0FBSyxFQUFFLEVBQUUsQ0FBQyxDQUNsQkEsT0FBTyxDQUFDLFVBQVUsRUFBRSxFQUFFLENBQUMsQ0FDdkJBLE9BQU8sQ0FBQyxVQUFVLEVBQUUsRUFBRSxDQUFDLENBQ3ZCQSxPQUFPLENBQUMsU0FBUyxFQUFFLEVBQUUsQ0FBQyxDQUN0QkEsT0FBTyxDQUFDLFNBQVMsRUFBRSxFQUFFLENBQUM7TUFDekJ2RSxNQUFNLENBQUM4RCxLQUFLLENBQUNqQyxJQUFJLENBQUM7UUFBRXVDLElBQUk7UUFBRXJELFlBQVk7UUFBRUcsSUFBSTtRQUFFRyxJQUFJLEVBQUVnRCxRQUFRLENBQUNGLENBQUMsQ0FBQzdDLElBQUksRUFBRSxFQUFFO01BQUUsQ0FBQyxDQUFDO0lBQzdFLENBQUMsQ0FBQztFQUNKO0VBQ0EsT0FBT3RCLE1BQU07QUFDZjtBQUVPLFNBQVN3RSxlQUFlQSxDQUFDM0csR0FBVyxFQUF3QjtFQUNqRSxJQUFJbUMsTUFBNEIsR0FBRyxFQUFFO0VBQ3JDLE1BQU15RSxzQkFBc0IsR0FBRyxJQUFJeEcsd0JBQVMsQ0FBQztJQUMzQ3lHLGFBQWEsRUFBRSxJQUFJO0lBQUU7SUFDckJ2RyxrQkFBa0IsRUFBRTtNQUNsQndHLFlBQVksRUFBRSxLQUFLO01BQUU7TUFDckJDLEdBQUcsRUFBRSxLQUFLO01BQUU7TUFDWnhHLFFBQVEsRUFBRSxVQUFVLENBQUU7SUFDeEIsQ0FBQztJQUNEeUcsaUJBQWlCLEVBQUVBLENBQUNDLE9BQU8sRUFBRUMsUUFBUSxHQUFHLEVBQUUsS0FBSztNQUM3QztNQUNBLElBQUlELE9BQU8sS0FBSyxNQUFNLEVBQUU7UUFDdEIsT0FBT0MsUUFBUSxDQUFDQyxRQUFRLENBQUMsQ0FBQztNQUM1QjtNQUNBLE9BQU9ELFFBQVE7SUFDakIsQ0FBQztJQUNERSxnQkFBZ0IsRUFBRSxLQUFLLENBQUU7RUFDM0IsQ0FBQyxDQUFDO0VBRUYsTUFBTUMsWUFBWSxHQUFHVCxzQkFBc0IsQ0FBQ2hHLEtBQUssQ0FBQ1osR0FBRyxDQUFDO0VBRXRELElBQUksQ0FBQ3FILFlBQVksQ0FBQ0Msc0JBQXNCLEVBQUU7SUFDeEMsTUFBTSxJQUFJaEosTUFBTSxDQUFDbUUsZUFBZSxDQUFDLHVDQUF1QyxDQUFDO0VBQzNFO0VBRUEsTUFBTTtJQUFFNkUsc0JBQXNCLEVBQUU7TUFBRUMsT0FBTyxHQUFHLENBQUM7SUFBRSxDQUFDLEdBQUcsQ0FBQztFQUFFLENBQUMsR0FBR0YsWUFBWTtFQUV0RSxJQUFJRSxPQUFPLENBQUNDLE1BQU0sRUFBRTtJQUNsQnJGLE1BQU0sR0FBRyxJQUFBVSxlQUFPLEVBQUMwRSxPQUFPLENBQUNDLE1BQU0sQ0FBQyxDQUFDQyxHQUFHLENBQUMsQ0FBQ0MsTUFBTSxHQUFHLENBQUMsQ0FBQyxLQUFLO01BQ3BELE1BQU07UUFBRXBDLElBQUksRUFBRXFDLFVBQVU7UUFBRUM7TUFBYSxDQUFDLEdBQUdGLE1BQU07TUFDakQsTUFBTUcsWUFBWSxHQUFHLElBQUkxRSxJQUFJLENBQUN5RSxZQUFZLENBQUM7TUFFM0MsT0FBTztRQUFFN0UsSUFBSSxFQUFFNEUsVUFBVTtRQUFFRTtNQUFhLENBQUM7SUFDM0MsQ0FBQyxDQUFDO0VBQ0o7RUFFQSxPQUFPMUYsTUFBTTtBQUNmO0FBRU8sU0FBUzJGLHNCQUFzQkEsQ0FBQzlILEdBQVcsRUFBVTtFQUMxRCxJQUFJdUMsTUFBTSxHQUFHLElBQUF0QyxnQkFBUSxFQUFDRCxHQUFHLENBQUM7RUFFMUIsSUFBSSxDQUFDdUMsTUFBTSxDQUFDd0YsNkJBQTZCLEVBQUU7SUFDekMsTUFBTSxJQUFJekosTUFBTSxDQUFDbUUsZUFBZSxDQUFDLDhDQUE4QyxDQUFDO0VBQ2xGO0VBQ0FGLE1BQU0sR0FBR0EsTUFBTSxDQUFDd0YsNkJBQTZCO0VBRTdDLElBQUl4RixNQUFNLENBQUN5RixRQUFRLEVBQUU7SUFDbkIsT0FBT3pGLE1BQU0sQ0FBQ3lGLFFBQVE7RUFDeEI7RUFDQSxNQUFNLElBQUkxSixNQUFNLENBQUNtRSxlQUFlLENBQUMseUJBQXlCLENBQUM7QUFDN0Q7QUFFTyxTQUFTd0Ysc0JBQXNCQSxDQUFDakksR0FBVyxFQUFxQjtFQUNyRSxNQUFNVyxNQUFNLEdBQUcsSUFBQVYsZ0JBQVEsRUFBQ0QsR0FBRyxDQUFDO0VBQzVCLE1BQU07SUFBRWtJLElBQUk7SUFBRUM7RUFBSyxDQUFDLEdBQUd4SCxNQUFNLENBQUN5SCx3QkFBd0I7RUFDdEQsT0FBTztJQUNMQSx3QkFBd0IsRUFBRTtNQUN4QkMsSUFBSSxFQUFFSCxJQUFJO01BQ1ZuRCxLQUFLLEVBQUUsSUFBQWxDLGVBQU8sRUFBQ3NGLElBQUk7SUFDckI7RUFDRixDQUFDO0FBQ0g7QUFFTyxTQUFTRywwQkFBMEJBLENBQUN0SSxHQUFXLEVBQUU7RUFDdEQsTUFBTVcsTUFBTSxHQUFHLElBQUFWLGdCQUFRLEVBQUNELEdBQUcsQ0FBQztFQUM1QixPQUFPVyxNQUFNLENBQUM0SCxTQUFTO0FBQ3pCO0FBRU8sU0FBU0MsWUFBWUEsQ0FBQ3hJLEdBQVcsRUFBRTtFQUN4QyxNQUFNVyxNQUFNLEdBQUcsSUFBQVYsZ0JBQVEsRUFBQ0QsR0FBRyxDQUFDO0VBQzVCLElBQUltQyxNQUFhLEdBQUcsRUFBRTtFQUN0QixJQUFJeEIsTUFBTSxDQUFDOEgsT0FBTyxJQUFJOUgsTUFBTSxDQUFDOEgsT0FBTyxDQUFDQyxNQUFNLElBQUkvSCxNQUFNLENBQUM4SCxPQUFPLENBQUNDLE1BQU0sQ0FBQ0MsR0FBRyxFQUFFO0lBQ3hFLE1BQU1DLFNBQWMsR0FBR2pJLE1BQU0sQ0FBQzhILE9BQU8sQ0FBQ0MsTUFBTSxDQUFDQyxHQUFHO0lBQ2hEO0lBQ0EsSUFBSUUsS0FBSyxDQUFDQyxPQUFPLENBQUNGLFNBQVMsQ0FBQyxFQUFFO01BQzVCekcsTUFBTSxHQUFHLENBQUMsR0FBR3lHLFNBQVMsQ0FBQztJQUN6QixDQUFDLE1BQU07TUFDTHpHLE1BQU0sQ0FBQzZCLElBQUksQ0FBQzRFLFNBQVMsQ0FBQztJQUN4QjtFQUNGO0VBQ0EsT0FBT3pHLE1BQU07QUFDZjs7QUFFQTtBQUNPLFNBQVM0RyxzQkFBc0JBLENBQUMvSSxHQUFXLEVBQUU7RUFDbEQsTUFBTXVDLE1BQU0sR0FBRyxJQUFBdEMsZ0JBQVEsRUFBQ0QsR0FBRyxDQUFDLENBQUNnSiw2QkFBNkI7RUFDMUQsSUFBSXpHLE1BQU0sQ0FBQzBHLFFBQVEsRUFBRTtJQUNuQixNQUFNQyxRQUFRLEdBQUcsSUFBQXJHLGVBQU8sRUFBQ04sTUFBTSxDQUFDMEcsUUFBUSxDQUFDLENBQUMsQ0FBQyxDQUFDO0lBQzVDLE1BQU12QixNQUFNLEdBQUcsSUFBQTdFLGVBQU8sRUFBQ04sTUFBTSxDQUFDaUYsTUFBTSxDQUFDLENBQUMsQ0FBQyxDQUFDO0lBQ3hDLE1BQU12RyxHQUFHLEdBQUdzQixNQUFNLENBQUNVLEdBQUc7SUFDdEIsTUFBTUksSUFBSSxHQUFHZCxNQUFNLENBQUNnQixJQUFJLENBQUNtRCxPQUFPLENBQUMsS0FBSyxFQUFFLEVBQUUsQ0FBQyxDQUN4Q0EsT0FBTyxDQUFDLEtBQUssRUFBRSxFQUFFLENBQUMsQ0FDbEJBLE9BQU8sQ0FBQyxVQUFVLEVBQUUsRUFBRSxDQUFDLENBQ3ZCQSxPQUFPLENBQUMsVUFBVSxFQUFFLEVBQUUsQ0FBQyxDQUN2QkEsT0FBTyxDQUFDLFNBQVMsRUFBRSxFQUFFLENBQUMsQ0FDdEJBLE9BQU8sQ0FBQyxTQUFTLEVBQUUsRUFBRSxDQUFDO0lBRXpCLE9BQU87TUFBRXdDLFFBQVE7TUFBRXhCLE1BQU07TUFBRXpHLEdBQUc7TUFBRW9DO0lBQUssQ0FBQztFQUN4QztFQUNBO0VBQ0EsSUFBSWQsTUFBTSxDQUFDNEcsSUFBSSxJQUFJNUcsTUFBTSxDQUFDNkcsT0FBTyxFQUFFO0lBQ2pDLE1BQU1DLE9BQU8sR0FBRyxJQUFBeEcsZUFBTyxFQUFDTixNQUFNLENBQUM0RyxJQUFJLENBQUMsQ0FBQyxDQUFDLENBQUM7SUFDdkMsTUFBTUcsVUFBVSxHQUFHLElBQUF6RyxlQUFPLEVBQUNOLE1BQU0sQ0FBQzZHLE9BQU8sQ0FBQyxDQUFDLENBQUMsQ0FBQztJQUM3QyxPQUFPO01BQUVDLE9BQU87TUFBRUM7SUFBVyxDQUFDO0VBQ2hDO0FBQ0Y7QUFxQkE7QUFDTyxTQUFTQyxrQkFBa0JBLENBQUN2SixHQUFXLEVBQXVCO0VBQ25FLE1BQU1tQyxNQUEyQixHQUFHO0lBQ2xDcUgsUUFBUSxFQUFFLEVBQUU7SUFDWkMsT0FBTyxFQUFFLEVBQUU7SUFDWHBILFdBQVcsRUFBRSxLQUFLO0lBQ2xCcUgsYUFBYSxFQUFFLEVBQUU7SUFDakJDLGtCQUFrQixFQUFFO0VBQ3RCLENBQUM7RUFFRCxJQUFJcEgsTUFBTSxHQUFHLElBQUF0QyxnQkFBUSxFQUFDRCxHQUFHLENBQUM7RUFFMUIsSUFBSSxDQUFDdUMsTUFBTSxDQUFDcUgsMEJBQTBCLEVBQUU7SUFDdEMsTUFBTSxJQUFJdEwsTUFBTSxDQUFDbUUsZUFBZSxDQUFDLDJDQUEyQyxDQUFDO0VBQy9FO0VBQ0FGLE1BQU0sR0FBR0EsTUFBTSxDQUFDcUgsMEJBQTBCO0VBQzFDLElBQUlySCxNQUFNLENBQUNHLFdBQVcsRUFBRTtJQUN0QlAsTUFBTSxDQUFDRSxXQUFXLEdBQUdFLE1BQU0sQ0FBQ0csV0FBVztFQUN6QztFQUNBLElBQUlILE1BQU0sQ0FBQ3NILGFBQWEsRUFBRTtJQUN4QjFILE1BQU0sQ0FBQ3VILGFBQWEsR0FBR25ILE1BQU0sQ0FBQ3NILGFBQWE7RUFDN0M7RUFDQSxJQUFJdEgsTUFBTSxDQUFDdUgsa0JBQWtCLEVBQUU7SUFDN0IzSCxNQUFNLENBQUN3SCxrQkFBa0IsR0FBR3BILE1BQU0sQ0FBQ29ILGtCQUFrQixJQUFJLEVBQUU7RUFDN0Q7RUFFQSxJQUFJcEgsTUFBTSxDQUFDMEIsY0FBYyxFQUFFO0lBQ3pCLElBQUFwQixlQUFPLEVBQUNOLE1BQU0sQ0FBQzBCLGNBQWMsQ0FBQyxDQUFDakQsT0FBTyxDQUFFbUQsTUFBTSxJQUFLO01BQ2pEO01BQ0FoQyxNQUFNLENBQUNxSCxRQUFRLENBQUN4RixJQUFJLENBQUM7UUFBRUcsTUFBTSxFQUFFLElBQUFuQix5QkFBaUIsRUFBQyxJQUFBSCxlQUFPLEVBQVNzQixNQUFNLENBQUNDLE1BQU0sQ0FBQyxDQUFDLENBQUMsQ0FBQztNQUFFLENBQUMsQ0FBQztJQUN4RixDQUFDLENBQUM7RUFDSjtFQUVBLElBQUk3QixNQUFNLENBQUN3SCxNQUFNLEVBQUU7SUFDakIsSUFBQWxILGVBQU8sRUFBQ04sTUFBTSxDQUFDd0gsTUFBTSxDQUFDLENBQUMvSSxPQUFPLENBQUVnSixNQUFNLElBQUs7TUFDekMsTUFBTUMsVUFBa0QsR0FBRztRQUN6RGhKLEdBQUcsRUFBRStJLE1BQU0sQ0FBQy9HLEdBQUc7UUFDZmlILFFBQVEsRUFBRUYsTUFBTSxDQUFDaEMsUUFBUTtRQUN6Qm1DLFlBQVksRUFBRUgsTUFBTSxDQUFDSSxZQUFZO1FBQ2pDQyxTQUFTLEVBQUUsSUFBSWxILElBQUksQ0FBQzZHLE1BQU0sQ0FBQ00sU0FBUztNQUN0QyxDQUFDO01BQ0QsSUFBSU4sTUFBTSxDQUFDTyxTQUFTLEVBQUU7UUFDcEJOLFVBQVUsQ0FBQ08sU0FBUyxHQUFHO1VBQUVDLEVBQUUsRUFBRVQsTUFBTSxDQUFDTyxTQUFTLENBQUNHLEVBQUU7VUFBRUMsV0FBVyxFQUFFWCxNQUFNLENBQUNPLFNBQVMsQ0FBQ0s7UUFBWSxDQUFDO01BQy9GO01BQ0EsSUFBSVosTUFBTSxDQUFDYSxLQUFLLEVBQUU7UUFDaEJaLFVBQVUsQ0FBQ2EsS0FBSyxHQUFHO1VBQUVMLEVBQUUsRUFBRVQsTUFBTSxDQUFDYSxLQUFLLENBQUNILEVBQUU7VUFBRUMsV0FBVyxFQUFFWCxNQUFNLENBQUNhLEtBQUssQ0FBQ0Q7UUFBWSxDQUFDO01BQ25GO01BQ0F6SSxNQUFNLENBQUNzSCxPQUFPLENBQUN6RixJQUFJLENBQUNpRyxVQUFVLENBQUM7SUFDakMsQ0FBQyxDQUFDO0VBQ0o7RUFDQSxPQUFPOUgsTUFBTTtBQUNmO0FBRU8sU0FBUzRJLHFCQUFxQkEsQ0FBQy9LLEdBQVcsRUFBa0I7RUFDakUsTUFBTVcsTUFBTSxHQUFHLElBQUFWLGdCQUFRLEVBQUNELEdBQUcsQ0FBQztFQUM1QixJQUFJZ0wsZ0JBQWdCLEdBQUcsQ0FBQyxDQUFtQjtFQUMzQyxJQUFJckssTUFBTSxDQUFDc0ssdUJBQXVCLEVBQUU7SUFDbENELGdCQUFnQixHQUFHO01BQ2pCRSxpQkFBaUIsRUFBRXZLLE1BQU0sQ0FBQ3NLLHVCQUF1QixDQUFDRTtJQUNwRCxDQUFtQjtJQUNuQixJQUFJQyxhQUFhO0lBQ2pCLElBQ0V6SyxNQUFNLENBQUNzSyx1QkFBdUIsSUFDOUJ0SyxNQUFNLENBQUNzSyx1QkFBdUIsQ0FBQzlDLElBQUksSUFDbkN4SCxNQUFNLENBQUNzSyx1QkFBdUIsQ0FBQzlDLElBQUksQ0FBQ2tELGdCQUFnQixFQUNwRDtNQUNBRCxhQUFhLEdBQUd6SyxNQUFNLENBQUNzSyx1QkFBdUIsQ0FBQzlDLElBQUksQ0FBQ2tELGdCQUFnQixJQUFJLENBQUMsQ0FBQztNQUMxRUwsZ0JBQWdCLENBQUNNLElBQUksR0FBR0YsYUFBYSxDQUFDRyxJQUFJO0lBQzVDO0lBQ0EsSUFBSUgsYUFBYSxFQUFFO01BQ2pCLE1BQU1JLFdBQVcsR0FBR0osYUFBYSxDQUFDSyxLQUFLO01BQ3ZDLElBQUlELFdBQVcsRUFBRTtRQUNmUixnQkFBZ0IsQ0FBQ1UsUUFBUSxHQUFHRixXQUFXO1FBQ3ZDUixnQkFBZ0IsQ0FBQ1csSUFBSSxHQUFHQyw4QkFBd0IsQ0FBQ0MsS0FBSztNQUN4RCxDQUFDLE1BQU07UUFDTGIsZ0JBQWdCLENBQUNVLFFBQVEsR0FBR04sYUFBYSxDQUFDVSxJQUFJO1FBQzlDZCxnQkFBZ0IsQ0FBQ1csSUFBSSxHQUFHQyw4QkFBd0IsQ0FBQ0csSUFBSTtNQUN2RDtJQUNGO0VBQ0Y7RUFFQSxPQUFPZixnQkFBZ0I7QUFDekI7QUFFTyxTQUFTZ0IsMkJBQTJCQSxDQUFDaE0sR0FBVyxFQUFFO0VBQ3ZELE1BQU1XLE1BQU0sR0FBRyxJQUFBVixnQkFBUSxFQUFDRCxHQUFHLENBQUM7RUFDNUIsT0FBT1csTUFBTSxDQUFDc0wsdUJBQXVCO0FBQ3ZDOztBQUVBO0FBQ0E7QUFDQSxTQUFTQyxpQkFBaUJBLENBQUNDLE1BQXVCLEVBQXNCO0VBQ3RFLE1BQU1DLGFBQWEsR0FBR0MsTUFBTSxDQUFDQyxJQUFJLENBQUNILE1BQU0sQ0FBQ0ksSUFBSSxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUNDLFNBQVMsQ0FBQyxDQUFDO0VBQzdELE1BQU1DLHVCQUF1QixHQUFHSixNQUFNLENBQUNDLElBQUksQ0FBQ0gsTUFBTSxDQUFDSSxJQUFJLENBQUNILGFBQWEsQ0FBQyxDQUFDLENBQUNqRixRQUFRLENBQUMsQ0FBQztFQUNsRixNQUFNdUYsZ0JBQWdCLEdBQUcsQ0FBQ0QsdUJBQXVCLElBQUksRUFBRSxFQUFFN0ksS0FBSyxDQUFDLEdBQUcsQ0FBQztFQUNuRSxPQUFPOEksZ0JBQWdCLENBQUNDLE1BQU0sSUFBSSxDQUFDLEdBQUdELGdCQUFnQixDQUFDLENBQUMsQ0FBQyxHQUFHLEVBQUU7QUFDaEU7QUFFQSxTQUFTRSxrQkFBa0JBLENBQUNULE1BQXVCLEVBQUU7RUFDbkQsTUFBTVUsT0FBTyxHQUFHUixNQUFNLENBQUNDLElBQUksQ0FBQ0gsTUFBTSxDQUFDSSxJQUFJLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQ08sWUFBWSxDQUFDLENBQUM7RUFDMUQsT0FBT1QsTUFBTSxDQUFDQyxJQUFJLENBQUNILE1BQU0sQ0FBQ0ksSUFBSSxDQUFDTSxPQUFPLENBQUMsQ0FBQyxDQUFDMUYsUUFBUSxDQUFDLENBQUM7QUFDckQ7QUFFTyxTQUFTNEYsZ0NBQWdDQSxDQUFDQyxHQUFXLEVBQUU7RUFDNUQsTUFBTUMsYUFBYSxHQUFHLElBQUlDLHNCQUFhLENBQUMsQ0FBQyxDQUFDLENBQUMsRUFBQzs7RUFFNUMsTUFBTUMsY0FBYyxHQUFHLElBQUFDLHNCQUFjLEVBQUNKLEdBQUcsQ0FBQyxFQUFDO0VBQzNDO0VBQ0EsT0FBT0csY0FBYyxDQUFDRSxjQUFjLENBQUNWLE1BQU0sRUFBRTtJQUMzQztJQUNBLElBQUlXLGlCQUFpQixFQUFDOztJQUV0QixNQUFNQyxxQkFBcUIsR0FBR2xCLE1BQU0sQ0FBQ0MsSUFBSSxDQUFDYSxjQUFjLENBQUNaLElBQUksQ0FBQyxDQUFDLENBQUMsQ0FBQztJQUNqRWUsaUJBQWlCLEdBQUcsSUFBQUUsa0JBQUssRUFBQ0QscUJBQXFCLENBQUM7SUFFaEQsTUFBTUUsaUJBQWlCLEdBQUdwQixNQUFNLENBQUNDLElBQUksQ0FBQ2EsY0FBYyxDQUFDWixJQUFJLENBQUMsQ0FBQyxDQUFDLENBQUM7SUFDN0RlLGlCQUFpQixHQUFHLElBQUFFLGtCQUFLLEVBQUNDLGlCQUFpQixFQUFFSCxpQkFBaUIsQ0FBQztJQUUvRCxNQUFNSSxvQkFBb0IsR0FBR0osaUJBQWlCLENBQUNLLFdBQVcsQ0FBQyxDQUFDLEVBQUM7O0lBRTdELE1BQU1DLGdCQUFnQixHQUFHdkIsTUFBTSxDQUFDQyxJQUFJLENBQUNhLGNBQWMsQ0FBQ1osSUFBSSxDQUFDLENBQUMsQ0FBQyxDQUFDLEVBQUM7SUFDN0RlLGlCQUFpQixHQUFHLElBQUFFLGtCQUFLLEVBQUNJLGdCQUFnQixFQUFFTixpQkFBaUIsQ0FBQztJQUU5RCxNQUFNTyxjQUFjLEdBQUdOLHFCQUFxQixDQUFDSSxXQUFXLENBQUMsQ0FBQztJQUMxRCxNQUFNRyxZQUFZLEdBQUdMLGlCQUFpQixDQUFDRSxXQUFXLENBQUMsQ0FBQztJQUNwRCxNQUFNSSxtQkFBbUIsR0FBR0gsZ0JBQWdCLENBQUNELFdBQVcsQ0FBQyxDQUFDO0lBRTFELElBQUlJLG1CQUFtQixLQUFLTCxvQkFBb0IsRUFBRTtNQUNoRDtNQUNBLE1BQU0sSUFBSTdNLEtBQUssQ0FDYiw0Q0FBNENrTixtQkFBbUIsbUNBQW1DTCxvQkFBb0IsRUFDeEgsQ0FBQztJQUNIO0lBRUEsTUFBTWhNLE9BQWdDLEdBQUcsQ0FBQyxDQUFDO0lBQzNDLElBQUlvTSxZQUFZLEdBQUcsQ0FBQyxFQUFFO01BQ3BCLE1BQU1FLFdBQVcsR0FBRzNCLE1BQU0sQ0FBQ0MsSUFBSSxDQUFDYSxjQUFjLENBQUNaLElBQUksQ0FBQ3VCLFlBQVksQ0FBQyxDQUFDO01BQ2xFUixpQkFBaUIsR0FBRyxJQUFBRSxrQkFBSyxFQUFDUSxXQUFXLEVBQUVWLGlCQUFpQixDQUFDO01BQ3pELE1BQU1XLGtCQUFrQixHQUFHLElBQUFiLHNCQUFjLEVBQUNZLFdBQVcsQ0FBQztNQUN0RDtNQUNBLE9BQU9DLGtCQUFrQixDQUFDWixjQUFjLENBQUNWLE1BQU0sRUFBRTtRQUMvQyxNQUFNdUIsY0FBYyxHQUFHaEMsaUJBQWlCLENBQUMrQixrQkFBa0IsQ0FBQztRQUM1REEsa0JBQWtCLENBQUMxQixJQUFJLENBQUMsQ0FBQyxDQUFDLEVBQUM7UUFDM0IsSUFBSTJCLGNBQWMsRUFBRTtVQUNsQnhNLE9BQU8sQ0FBQ3dNLGNBQWMsQ0FBQyxHQUFHdEIsa0JBQWtCLENBQUNxQixrQkFBa0IsQ0FBQztRQUNsRTtNQUNGO0lBQ0Y7SUFFQSxJQUFJRSxhQUFhO0lBQ2pCLE1BQU1DLGFBQWEsR0FBR1AsY0FBYyxHQUFHQyxZQUFZLEdBQUcsRUFBRTtJQUN4RCxJQUFJTSxhQUFhLEdBQUcsQ0FBQyxFQUFFO01BQ3JCLE1BQU1DLGFBQWEsR0FBR2hDLE1BQU0sQ0FBQ0MsSUFBSSxDQUFDYSxjQUFjLENBQUNaLElBQUksQ0FBQzZCLGFBQWEsQ0FBQyxDQUFDO01BQ3JFZCxpQkFBaUIsR0FBRyxJQUFBRSxrQkFBSyxFQUFDYSxhQUFhLEVBQUVmLGlCQUFpQixDQUFDO01BQzNEO01BQ0EsTUFBTWdCLG1CQUFtQixHQUFHakMsTUFBTSxDQUFDQyxJQUFJLENBQUNhLGNBQWMsQ0FBQ1osSUFBSSxDQUFDLENBQUMsQ0FBQyxDQUFDLENBQUNvQixXQUFXLENBQUMsQ0FBQztNQUM3RSxNQUFNWSxhQUFhLEdBQUdqQixpQkFBaUIsQ0FBQ0ssV0FBVyxDQUFDLENBQUM7TUFDckQ7TUFDQSxJQUFJVyxtQkFBbUIsS0FBS0MsYUFBYSxFQUFFO1FBQ3pDLE1BQU0sSUFBSTFOLEtBQUssQ0FDYiw2Q0FBNkN5TixtQkFBbUIsbUNBQW1DQyxhQUFhLEVBQ2xILENBQUM7TUFDSDtNQUNBSixhQUFhLEdBQUcsSUFBQWYsc0JBQWMsRUFBQ2lCLGFBQWEsQ0FBQztJQUMvQztJQUNBLE1BQU1HLFdBQVcsR0FBRzlNLE9BQU8sQ0FBQyxjQUFjLENBQUM7SUFFM0MsUUFBUThNLFdBQVc7TUFDakIsS0FBSyxPQUFPO1FBQUU7VUFDWixNQUFNQyxZQUFZLEdBQUcvTSxPQUFPLENBQUMsWUFBWSxDQUFDLEdBQUcsSUFBSSxHQUFHQSxPQUFPLENBQUMsZUFBZSxDQUFDLEdBQUcsR0FBRztVQUNsRixNQUFNLElBQUliLEtBQUssQ0FBQzROLFlBQVksQ0FBQztRQUMvQjtNQUNBLEtBQUssT0FBTztRQUFFO1VBQ1osTUFBTUMsV0FBVyxHQUFHaE4sT0FBTyxDQUFDLGNBQWMsQ0FBQztVQUMzQyxNQUFNaU4sU0FBUyxHQUFHak4sT0FBTyxDQUFDLFlBQVksQ0FBQztVQUV2QyxRQUFRaU4sU0FBUztZQUNmLEtBQUssS0FBSztjQUFFO2dCQUNWMUIsYUFBYSxDQUFDMkIsV0FBVyxDQUFDNUIsR0FBRyxDQUFDO2dCQUM5QixPQUFPQyxhQUFhO2NBQ3RCO1lBRUEsS0FBSyxTQUFTO2NBQUU7Z0JBQUEsSUFBQTRCLGNBQUE7Z0JBQ2QsTUFBTUMsUUFBUSxJQUFBRCxjQUFBLEdBQUdWLGFBQWEsY0FBQVUsY0FBQSx1QkFBYkEsY0FBQSxDQUFldEMsSUFBSSxDQUFDNkIsYUFBYSxDQUFDO2dCQUNuRG5CLGFBQWEsQ0FBQzhCLFVBQVUsQ0FBQ0QsUUFBUSxDQUFDO2dCQUNsQztjQUNGO1lBRUEsS0FBSyxVQUFVO2NBQ2I7Z0JBQ0UsUUFBUUosV0FBVztrQkFDakIsS0FBSyxVQUFVO29CQUFFO3NCQUFBLElBQUFNLGVBQUE7c0JBQ2YsTUFBTUMsWUFBWSxJQUFBRCxlQUFBLEdBQUdiLGFBQWEsY0FBQWEsZUFBQSx1QkFBYkEsZUFBQSxDQUFlekMsSUFBSSxDQUFDNkIsYUFBYSxDQUFDO3NCQUN2RG5CLGFBQWEsQ0FBQ2lDLFdBQVcsQ0FBQ0QsWUFBWSxDQUFDOUgsUUFBUSxDQUFDLENBQUMsQ0FBQztzQkFDbEQ7b0JBQ0Y7a0JBQ0E7b0JBQVM7c0JBQ1AsTUFBTXNILFlBQVksR0FBRywyQkFBMkJDLFdBQVcsK0JBQStCO3NCQUMxRixNQUFNLElBQUk3TixLQUFLLENBQUM0TixZQUFZLENBQUM7b0JBQy9CO2dCQUNGO2NBQ0Y7Y0FDQTtZQUNGLEtBQUssT0FBTztjQUNWO2dCQUNFLFFBQVFDLFdBQVc7a0JBQ2pCLEtBQUssVUFBVTtvQkFBRTtzQkFBQSxJQUFBUyxlQUFBO3NCQUNmLE1BQU1DLFNBQVMsSUFBQUQsZUFBQSxHQUFHaEIsYUFBYSxjQUFBZ0IsZUFBQSx1QkFBYkEsZUFBQSxDQUFlNUMsSUFBSSxDQUFDNkIsYUFBYSxDQUFDO3NCQUNwRG5CLGFBQWEsQ0FBQ29DLFFBQVEsQ0FBQ0QsU0FBUyxDQUFDakksUUFBUSxDQUFDLENBQUMsQ0FBQztzQkFDNUM7b0JBQ0Y7a0JBQ0E7b0JBQVM7c0JBQ1AsTUFBTXNILFlBQVksR0FBRywyQkFBMkJDLFdBQVcsNEJBQTRCO3NCQUN2RixNQUFNLElBQUk3TixLQUFLLENBQUM0TixZQUFZLENBQUM7b0JBQy9CO2dCQUNGO2NBQ0Y7Y0FDQTtZQUNGO2NBQVM7Z0JBQ1A7Z0JBQ0E7Z0JBQ0EsTUFBTWEsY0FBYyxHQUFHLGtDQUFrQ2QsV0FBVyxHQUFHO2dCQUN2RTtnQkFDQWUsT0FBTyxDQUFDQyxJQUFJLENBQUNGLGNBQWMsQ0FBQztjQUM5QjtVQUNGO1FBQ0Y7SUFDRjtFQUNGO0FBQ0Y7QUFFTyxTQUFTRyxvQkFBb0JBLENBQUN6UCxHQUFXLEVBQUU7RUFDaEQsTUFBTVcsTUFBTSxHQUFHLElBQUFWLGdCQUFRLEVBQUNELEdBQUcsQ0FBQztFQUM1QixNQUFNMFAsZUFBZSxHQUFHL08sTUFBTSxDQUFDZ1Asc0JBQXNCO0VBQ3JELElBQUlELGVBQWUsSUFBSUEsZUFBZSxDQUFDdkgsSUFBSSxFQUFFO0lBQzNDO0lBQ0E7SUFDQXVILGVBQWUsQ0FBQ3ZILElBQUksR0FBRyxJQUFBdEYsZUFBTyxFQUFDNk0sZUFBZSxDQUFDdkgsSUFBSSxDQUFDO0VBQ3REO0VBQ0EsT0FBT3VILGVBQWU7QUFDeEI7QUFFTyxTQUFTRSwyQkFBMkJBLENBQUM1UCxHQUFXLEVBQUU7RUFDdkQsT0FBTyxJQUFBQyxnQkFBUSxFQUFDRCxHQUFHLENBQUM7QUFDdEI7QUFFTyxTQUFTNlAsMEJBQTBCQSxDQUFDN1AsR0FBVyxFQUFFO0VBQ3RELE1BQU1XLE1BQU0sR0FBRyxJQUFBVixnQkFBUSxFQUFDRCxHQUFHLENBQUM7RUFDNUIsTUFBTThQLGVBQWUsR0FBR25QLE1BQU0sQ0FBQ29QLFNBQVM7RUFDeEMsT0FBTztJQUNMekUsSUFBSSxFQUFFd0UsZUFBZSxDQUFDdkUsSUFBSTtJQUMxQnlFLGVBQWUsRUFBRUYsZUFBZSxDQUFDRztFQUNuQyxDQUFDO0FBQ0g7QUFFTyxTQUFTQyxtQkFBbUJBLENBQUNsUSxHQUFXLEVBQUU7RUFDL0MsTUFBTVcsTUFBTSxHQUFHLElBQUFWLGdCQUFRLEVBQUNELEdBQUcsQ0FBQztFQUM1QixJQUFJVyxNQUFNLENBQUN3UCxZQUFZLElBQUl4UCxNQUFNLENBQUN3UCxZQUFZLENBQUN0UCxLQUFLLEVBQUU7SUFDcEQ7SUFDQSxPQUFPLElBQUFnQyxlQUFPLEVBQUNsQyxNQUFNLENBQUN3UCxZQUFZLENBQUN0UCxLQUFLLENBQUM7RUFDM0M7RUFDQSxPQUFPLEVBQUU7QUFDWDs7QUFFQTtBQUNPLFNBQVN1UCxlQUFlQSxDQUFDcFEsR0FBVyxFQUFzQjtFQUMvRCxNQUFNbUMsTUFBMEIsR0FBRztJQUNqQ2tCLElBQUksRUFBRSxFQUFFO0lBQ1JILFlBQVksRUFBRTtFQUNoQixDQUFDO0VBRUQsSUFBSVgsTUFBTSxHQUFHLElBQUF0QyxnQkFBUSxFQUFDRCxHQUFHLENBQUM7RUFDMUIsSUFBSSxDQUFDdUMsTUFBTSxDQUFDOE4sZ0JBQWdCLEVBQUU7SUFDNUIsTUFBTSxJQUFJL1IsTUFBTSxDQUFDbUUsZUFBZSxDQUFDLGlDQUFpQyxDQUFDO0VBQ3JFO0VBQ0FGLE1BQU0sR0FBR0EsTUFBTSxDQUFDOE4sZ0JBQWdCO0VBQ2hDLElBQUk5TixNQUFNLENBQUNnQixJQUFJLEVBQUU7SUFDZnBCLE1BQU0sQ0FBQ2tCLElBQUksR0FBR2QsTUFBTSxDQUFDZ0IsSUFBSSxDQUFDbUQsT0FBTyxDQUFDLEtBQUssRUFBRSxFQUFFLENBQUMsQ0FDekNBLE9BQU8sQ0FBQyxLQUFLLEVBQUUsRUFBRSxDQUFDLENBQ2xCQSxPQUFPLENBQUMsVUFBVSxFQUFFLEVBQUUsQ0FBQyxDQUN2QkEsT0FBTyxDQUFDLFVBQVUsRUFBRSxFQUFFLENBQUMsQ0FDdkJBLE9BQU8sQ0FBQyxTQUFTLEVBQUUsRUFBRSxDQUFDLENBQ3RCQSxPQUFPLENBQUMsU0FBUyxFQUFFLEVBQUUsQ0FBQztFQUMzQjtFQUNBLElBQUluRSxNQUFNLENBQUNhLFlBQVksRUFBRTtJQUN2QmpCLE1BQU0sQ0FBQ2UsWUFBWSxHQUFHLElBQUlDLElBQUksQ0FBQ1osTUFBTSxDQUFDYSxZQUFZLENBQUM7RUFDckQ7RUFFQSxPQUFPakIsTUFBTTtBQUNmO0FBRUEsTUFBTW1PLGFBQWEsR0FBR0EsQ0FBQ3hOLE9BQXVCLEVBQUV5TixJQUFrQyxHQUFHLENBQUMsQ0FBQyxLQUFLO0VBQzFGLE1BQU07SUFBRXROLEdBQUc7SUFBRUcsWUFBWTtJQUFFRyxJQUFJO0lBQUVFLElBQUk7SUFBRStNLFNBQVM7SUFBRUM7RUFBUyxDQUFDLEdBQUczTixPQUFPO0VBRXRFLElBQUksQ0FBQyxJQUFBNE4sZ0JBQVEsRUFBQ0gsSUFBSSxDQUFDLEVBQUU7SUFDbkJBLElBQUksR0FBRyxDQUFDLENBQUM7RUFDWDtFQUVBLE1BQU14TixJQUFJLEdBQUcsSUFBQUMseUJBQWlCLEVBQUMsSUFBQUgsZUFBTyxFQUFDSSxHQUFHLENBQUMsQ0FBQyxDQUFDLENBQUMsSUFBSSxFQUFFLENBQUM7RUFDckQsTUFBTUMsWUFBWSxHQUFHRSxZQUFZLEdBQUcsSUFBSUQsSUFBSSxDQUFDLElBQUFOLGVBQU8sRUFBQ08sWUFBWSxDQUFDLENBQUMsQ0FBQyxDQUFDLElBQUksRUFBRSxDQUFDLEdBQUd1TixTQUFTO0VBQ3hGLE1BQU10TixJQUFJLEdBQUcsSUFBQUMsb0JBQVksRUFBQyxJQUFBVCxlQUFPLEVBQUNVLElBQUksQ0FBQyxDQUFDLENBQUMsQ0FBQyxJQUFJLEVBQUUsQ0FBQztFQUNqRCxNQUFNQyxJQUFJLEdBQUcsSUFBQW9OLG9CQUFZLEVBQUNuTixJQUFJLElBQUksRUFBRSxDQUFDO0VBRXJDLE9BQU87SUFDTFYsSUFBSTtJQUNKRyxZQUFZO0lBQ1pHLElBQUk7SUFDSkcsSUFBSTtJQUNKcU4sU0FBUyxFQUFFTCxTQUFTO0lBQ3BCTSxRQUFRLEVBQUVMLFFBQVE7SUFDbEJNLGNBQWMsRUFBRVIsSUFBSSxDQUFDUyxjQUFjLEdBQUdULElBQUksQ0FBQ1MsY0FBYyxHQUFHO0VBQzlELENBQUM7QUFDSCxDQUFDOztBQUVEO0FBQ08sU0FBU0MsZ0JBQWdCQSxDQUFDalIsR0FBVyxFQUFFO0VBQzVDLE1BQU1tQyxNQU1MLEdBQUc7SUFDRkMsT0FBTyxFQUFFLEVBQUU7SUFDWEMsV0FBVyxFQUFFLEtBQUs7SUFDbEI2TyxVQUFVLEVBQUVQLFNBQVM7SUFDckJRLGVBQWUsRUFBRVIsU0FBUztJQUMxQlMsU0FBUyxFQUFFVDtFQUNiLENBQUM7RUFDRCxJQUFJdE8sV0FBVyxHQUFHLEtBQUs7RUFDdkIsSUFBSTZPLFVBQVU7RUFDZCxNQUFNM08sTUFBTSxHQUFHbEMsbUJBQW1CLENBQUNPLEtBQUssQ0FBQ1osR0FBRyxDQUFDO0VBRTdDLE1BQU1xUix5QkFBeUIsR0FBSUMsaUJBQWlDLElBQUs7SUFDdkUsSUFBSUEsaUJBQWlCLEVBQUU7TUFDckIsSUFBQXpPLGVBQU8sRUFBQ3lPLGlCQUFpQixDQUFDLENBQUN0USxPQUFPLENBQUVrRCxZQUFZLElBQUs7UUFDbkQvQixNQUFNLENBQUNDLE9BQU8sQ0FBQzRCLElBQUksQ0FBQztVQUFFRyxNQUFNLEVBQUUsSUFBQW5CLHlCQUFpQixFQUFDLElBQUFILGVBQU8sRUFBQ3FCLFlBQVksQ0FBQ0UsTUFBTSxDQUFDLENBQUMsQ0FBQyxDQUFDLElBQUksRUFBRSxDQUFDO1VBQUVaLElBQUksRUFBRTtRQUFFLENBQUMsQ0FBQztNQUNwRyxDQUFDLENBQUM7SUFDSjtFQUNGLENBQUM7RUFFRCxNQUFNK04sZ0JBQW9DLEdBQUdoUCxNQUFNLENBQUNDLGdCQUFnQjtFQUNwRSxNQUFNZ1Asa0JBQXNDLEdBQUdqUCxNQUFNLENBQUNrUCxrQkFBa0I7RUFFeEUsSUFBSUYsZ0JBQWdCLEVBQUU7SUFDcEIsSUFBSUEsZ0JBQWdCLENBQUM3TyxXQUFXLEVBQUU7TUFDaENMLFdBQVcsR0FBR2tQLGdCQUFnQixDQUFDN08sV0FBVztJQUM1QztJQUNBLElBQUk2TyxnQkFBZ0IsQ0FBQzNPLFFBQVEsRUFBRTtNQUM3QixJQUFBQyxlQUFPLEVBQUMwTyxnQkFBZ0IsQ0FBQzNPLFFBQVEsQ0FBQyxDQUFDNUIsT0FBTyxDQUFFOEIsT0FBTyxJQUFLO1FBQ3RELE1BQU1DLElBQUksR0FBRyxJQUFBQyx5QkFBaUIsRUFBQyxJQUFBSCxlQUFPLEVBQUNDLE9BQU8sQ0FBQ0csR0FBRyxDQUFDLENBQUMsQ0FBQyxDQUFDLElBQUksRUFBRSxDQUFDO1FBQzdELE1BQU1DLFlBQVksR0FBRyxJQUFJQyxJQUFJLENBQUMsSUFBQU4sZUFBTyxFQUFDQyxPQUFPLENBQUNNLFlBQVksQ0FBQyxDQUFDLENBQUMsQ0FBQyxJQUFJLEVBQUUsQ0FBQztRQUNyRSxNQUFNQyxJQUFJLEdBQUcsSUFBQUMsb0JBQVksRUFBQyxJQUFBVCxlQUFPLEVBQUNDLE9BQU8sQ0FBQ1MsSUFBSSxDQUFDLENBQUMsQ0FBQyxDQUFDLElBQUksRUFBRSxDQUFDO1FBQ3pELE1BQU1DLElBQUksR0FBRyxJQUFBb04sb0JBQVksRUFBQzlOLE9BQU8sQ0FBQ1csSUFBSSxJQUFJLEVBQUUsQ0FBQztRQUM3Q3RCLE1BQU0sQ0FBQ0MsT0FBTyxDQUFDNEIsSUFBSSxDQUFDO1VBQUVqQixJQUFJO1VBQUVHLFlBQVk7VUFBRUcsSUFBSTtVQUFFRztRQUFLLENBQUMsQ0FBQztNQUN6RCxDQUFDLENBQUM7SUFDSjtJQUVBLElBQUkrTixnQkFBZ0IsQ0FBQ0csTUFBTSxFQUFFO01BQzNCUixVQUFVLEdBQUdLLGdCQUFnQixDQUFDRyxNQUFNO0lBQ3RDO0lBQ0EsSUFBSUgsZ0JBQWdCLENBQUNJLFVBQVUsRUFBRTtNQUMvQlQsVUFBVSxHQUFHSyxnQkFBZ0IsQ0FBQ0ksVUFBVTtJQUMxQyxDQUFDLE1BQU0sSUFBSXRQLFdBQVcsSUFBSUYsTUFBTSxDQUFDQyxPQUFPLENBQUN1SyxNQUFNLEdBQUcsQ0FBQyxFQUFFO01BQUEsSUFBQWlGLGVBQUE7TUFDbkRWLFVBQVUsSUFBQVUsZUFBQSxHQUFHelAsTUFBTSxDQUFDQyxPQUFPLENBQUNELE1BQU0sQ0FBQ0MsT0FBTyxDQUFDdUssTUFBTSxHQUFHLENBQUMsQ0FBQyxjQUFBaUYsZUFBQSx1QkFBekNBLGVBQUEsQ0FBMkM3TyxJQUFJO0lBQzlEO0lBQ0EsSUFBSXdPLGdCQUFnQixDQUFDdE4sY0FBYyxFQUFFO01BQ25Db04seUJBQXlCLENBQUNFLGdCQUFnQixDQUFDdE4sY0FBYyxDQUFDO0lBQzVEO0VBQ0Y7RUFFQSxJQUFJdU4sa0JBQWtCLEVBQUU7SUFDdEIsSUFBSUEsa0JBQWtCLENBQUM5TyxXQUFXLEVBQUU7TUFDbENMLFdBQVcsR0FBR21QLGtCQUFrQixDQUFDOU8sV0FBVztJQUM5QztJQUVBLElBQUk4TyxrQkFBa0IsQ0FBQ0ssT0FBTyxFQUFFO01BQzlCLElBQUFoUCxlQUFPLEVBQUMyTyxrQkFBa0IsQ0FBQ0ssT0FBTyxDQUFDLENBQUM3USxPQUFPLENBQUU4QixPQUFPLElBQUs7UUFDdkRYLE1BQU0sQ0FBQ0MsT0FBTyxDQUFDNEIsSUFBSSxDQUFDc00sYUFBYSxDQUFDeE4sT0FBTyxDQUFDLENBQUM7TUFDN0MsQ0FBQyxDQUFDO0lBQ0o7SUFDQSxJQUFJME8sa0JBQWtCLENBQUNNLFlBQVksRUFBRTtNQUNuQyxJQUFBalAsZUFBTyxFQUFDMk8sa0JBQWtCLENBQUNNLFlBQVksQ0FBQyxDQUFDOVEsT0FBTyxDQUFFOEIsT0FBTyxJQUFLO1FBQzVEWCxNQUFNLENBQUNDLE9BQU8sQ0FBQzRCLElBQUksQ0FBQ3NNLGFBQWEsQ0FBQ3hOLE9BQU8sRUFBRTtVQUFFa08sY0FBYyxFQUFFO1FBQUssQ0FBQyxDQUFDLENBQUM7TUFDdkUsQ0FBQyxDQUFDO0lBQ0o7SUFFQSxJQUFJUSxrQkFBa0IsQ0FBQzNILGFBQWEsRUFBRTtNQUNwQzFILE1BQU0sQ0FBQ2lQLFNBQVMsR0FBR0ksa0JBQWtCLENBQUMzSCxhQUFhO0lBQ3JEO0lBQ0EsSUFBSTJILGtCQUFrQixDQUFDTyxtQkFBbUIsRUFBRTtNQUMxQzVQLE1BQU0sQ0FBQ2dQLGVBQWUsR0FBR0ssa0JBQWtCLENBQUNPLG1CQUFtQjtJQUNqRTtJQUNBLElBQUlQLGtCQUFrQixDQUFDdk4sY0FBYyxFQUFFO01BQ3JDb04seUJBQXlCLENBQUNHLGtCQUFrQixDQUFDdk4sY0FBYyxDQUFDO0lBQzlEO0VBQ0Y7RUFFQTlCLE1BQU0sQ0FBQ0UsV0FBVyxHQUFHQSxXQUFXO0VBQ2hDLElBQUlBLFdBQVcsRUFBRTtJQUNmRixNQUFNLENBQUMrTyxVQUFVLEdBQUdBLFVBQVU7RUFDaEM7RUFDQSxPQUFPL08sTUFBTTtBQUNmO0FBRU8sU0FBUzZQLGdCQUFnQkEsQ0FBQ2hTLEdBQVcsRUFBRTtFQUM1QyxNQUFNVyxNQUFNLEdBQUcsSUFBQVYsZ0JBQVEsRUFBQ0QsR0FBRyxDQUFDO0VBQzVCLE1BQU1pUyxNQUFNLEdBQUd0UixNQUFNLENBQUN1UixjQUFjO0VBQ3BDLE9BQU9ELE1BQU07QUFDZiIsImlnbm9yZUxpc3QiOltdfQ==