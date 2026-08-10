import crc32 from 'buffer-crc32';
import { XMLParser } from 'fast-xml-parser';
import * as errors from "../errors.mjs";
import { SelectResults } from "../helpers.mjs";
import { isObject, parseXml, readableStream, sanitizeETag, sanitizeObjectKey, sanitizeSize, toArray } from "./helper.mjs";
import { readAsString } from "./response.mjs";
import { RETENTION_VALIDITY_UNITS } from "./type.mjs";

// parse XML response for bucket region
export function parseBucketRegion(xml) {
  // return region information
  return parseXml(xml).LocationConstraint;
}
const fxp = new XMLParser();
const fxpWithoutNumParser = new XMLParser({
  // @ts-ignore
  numberParseOptions: {
    skipLike: /./
  }
});

// Parse XML and return information as Javascript types
// parse error XML response
export function parseError(xml, headerInfo) {
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
export async function parseResponseError(response) {
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
  const xmlString = await readAsString(response);
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
export function parseListObjectsV2WithMetadata(xml) {
  const result = {
    objects: [],
    isTruncated: false,
    nextContinuationToken: ''
  };
  let xmlobj = parseXml(xml);
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
    toArray(xmlobj.Contents).forEach(content => {
      const name = sanitizeObjectKey(content.Key);
      const lastModified = new Date(content.LastModified);
      const etag = sanitizeETag(content.ETag);
      const size = content.Size;
      let tags = {};
      if (content.UserTags != null) {
        toArray(content.UserTags.split('&')).forEach(tag => {
          const [key, value] = tag.split('=');
          tags[key] = value;
        });
      } else {
        tags = {};
      }
      let metadata;
      if (content.UserMetadata != null) {
        metadata = toArray(content.UserMetadata)[0];
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
    toArray(xmlobj.CommonPrefixes).forEach(commonPrefix => {
      result.objects.push({
        prefix: sanitizeObjectKey(toArray(commonPrefix.Prefix)[0]),
        size: 0
      });
    });
  }
  return result;
}
export function parseListObjectsV2(xml) {
  const result = {
    objects: [],
    isTruncated: false,
    nextContinuationToken: ''
  };
  let xmlobj = parseXml(xml);
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
    toArray(xmlobj.Contents).forEach(content => {
      const name = sanitizeObjectKey(toArray(content.Key)[0]);
      const lastModified = new Date(content.LastModified);
      const etag = sanitizeETag(content.ETag);
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
    toArray(xmlobj.CommonPrefixes).forEach(commonPrefix => {
      result.objects.push({
        prefix: sanitizeObjectKey(toArray(commonPrefix.Prefix)[0]),
        size: 0
      });
    });
  }
  return result;
}
export function parseBucketNotification(xml) {
  const result = {
    TopicConfiguration: [],
    QueueConfiguration: [],
    CloudFunctionConfiguration: []
  };
  const genEvents = events => {
    if (!events) {
      return [];
    }
    return toArray(events);
  };
  const genFilterRules = filters => {
    var _filterArr$;
    const rules = [];
    if (!filters) {
      return rules;
    }
    const filterArr = toArray(filters);
    if ((_filterArr$ = filterArr[0]) !== null && _filterArr$ !== void 0 && _filterArr$.S3Key) {
      var _s3KeyArr$;
      const s3KeyArr = toArray(filterArr[0].S3Key);
      if ((_s3KeyArr$ = s3KeyArr[0]) !== null && _s3KeyArr$ !== void 0 && _s3KeyArr$.FilterRule) {
        toArray(s3KeyArr[0].FilterRule).forEach(rule => {
          const r = rule;
          const Name = toArray(r.Name)[0];
          const Value = toArray(r.Value)[0];
          rules.push({
            Name,
            Value
          });
        });
      }
    }
    return rules;
  };
  let xmlobj = parseXml(xml);
  xmlobj = xmlobj.NotificationConfiguration;
  if (xmlobj.TopicConfiguration) {
    toArray(xmlobj.TopicConfiguration).forEach(config => {
      const Id = toArray(config.Id)[0];
      const Topic = toArray(config.Topic)[0];
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
    toArray(xmlobj.QueueConfiguration).forEach(config => {
      const Id = toArray(config.Id)[0];
      const Queue = toArray(config.Queue)[0];
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
    toArray(xmlobj.CloudFunctionConfiguration).forEach(config => {
      const Id = toArray(config.Id)[0];
      const CloudFunction = toArray(config.CloudFunction)[0];
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
export function parseListParts(xml) {
  let xmlobj = parseXml(xml);
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
    result.marker = toArray(xmlobj.NextPartNumberMarker)[0] || '';
  }
  if (xmlobj.Part) {
    toArray(xmlobj.Part).forEach(p => {
      const part = parseInt(toArray(p.PartNumber)[0], 10);
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
export function parseListBucket(xml) {
  let result = [];
  const listBucketResultParser = new XMLParser({
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
    result = toArray(Buckets.Bucket).map((bucket = {}) => {
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
export function parseInitiateMultipart(xml) {
  let xmlobj = parseXml(xml);
  if (!xmlobj.InitiateMultipartUploadResult) {
    throw new errors.InvalidXMLError('Missing tag: "InitiateMultipartUploadResult"');
  }
  xmlobj = xmlobj.InitiateMultipartUploadResult;
  if (xmlobj.UploadId) {
    return xmlobj.UploadId;
  }
  throw new errors.InvalidXMLError('Missing tag: "UploadId"');
}
export function parseReplicationConfig(xml) {
  const xmlObj = parseXml(xml);
  const {
    Role,
    Rule
  } = xmlObj.ReplicationConfiguration;
  return {
    ReplicationConfiguration: {
      role: Role,
      rules: toArray(Rule)
    }
  };
}
export function parseObjectLegalHoldConfig(xml) {
  const xmlObj = parseXml(xml);
  return xmlObj.LegalHold;
}
export function parseTagging(xml) {
  const xmlObj = parseXml(xml);
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
export function parseCompleteMultipart(xml) {
  const xmlobj = parseXml(xml).CompleteMultipartUploadResult;
  if (xmlobj.Location) {
    const location = toArray(xmlobj.Location)[0];
    const bucket = toArray(xmlobj.Bucket)[0];
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
    const errCode = toArray(xmlobj.Code)[0];
    const errMessage = toArray(xmlobj.Message)[0];
    return {
      errCode,
      errMessage
    };
  }
}
// parse XML response for listing in-progress multipart uploads
export function parseListMultipart(xml) {
  const result = {
    prefixes: [],
    uploads: [],
    isTruncated: false,
    nextKeyMarker: '',
    nextUploadIdMarker: ''
  };
  let xmlobj = parseXml(xml);
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
    toArray(xmlobj.CommonPrefixes).forEach(prefix => {
      // @ts-expect-error index check
      result.prefixes.push({
        prefix: sanitizeObjectKey(toArray(prefix.Prefix)[0])
      });
    });
  }
  if (xmlobj.Upload) {
    toArray(xmlobj.Upload).forEach(upload => {
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
export function parseObjectLockConfig(xml) {
  const xmlObj = parseXml(xml);
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
        lockConfigResult.unit = RETENTION_VALIDITY_UNITS.YEARS;
      } else {
        lockConfigResult.validity = retentionResp.Days;
        lockConfigResult.unit = RETENTION_VALIDITY_UNITS.DAYS;
      }
    }
  }
  return lockConfigResult;
}
export function parseBucketVersioningConfig(xml) {
  const xmlObj = parseXml(xml);
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
export function parseSelectObjectContentResponse(res) {
  const selectResults = new SelectResults({}); // will be returned

  const responseStream = readableStream(res); // convert byte array to a readable responseStream
  // @ts-ignore
  while (responseStream._readableState.length) {
    // Top level responseStream read tracker.
    let msgCrcAccumulator; // accumulate from start of the message till the message crc start.

    const totalByteLengthBuffer = Buffer.from(responseStream.read(4));
    msgCrcAccumulator = crc32(totalByteLengthBuffer);
    const headerBytesBuffer = Buffer.from(responseStream.read(4));
    msgCrcAccumulator = crc32(headerBytesBuffer, msgCrcAccumulator);
    const calculatedPreludeCrc = msgCrcAccumulator.readInt32BE(); // use it to check if any CRC mismatch in header itself.

    const preludeCrcBuffer = Buffer.from(responseStream.read(4)); // read 4 bytes    i.e 4+4 =8 + 4 = 12 ( prelude + prelude crc)
    msgCrcAccumulator = crc32(preludeCrcBuffer, msgCrcAccumulator);
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
      msgCrcAccumulator = crc32(headerBytes, msgCrcAccumulator);
      const headerReaderStream = readableStream(headerBytes);
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
      msgCrcAccumulator = crc32(payLoadBuffer, msgCrcAccumulator);
      // read the checksum early and detect any mismatch so we can avoid unnecessary further processing.
      const messageCrcByteValue = Buffer.from(responseStream.read(4)).readInt32BE();
      const calculatedCrc = msgCrcAccumulator.readInt32BE();
      // Handle message CRC Error
      if (messageCrcByteValue !== calculatedCrc) {
        throw new Error(`Message Checksum Mismatch, Message CRC of ${messageCrcByteValue} does not equal expected CRC of ${calculatedCrc}`);
      }
      payloadStream = readableStream(payLoadBuffer);
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
export function parseLifecycleConfig(xml) {
  const xmlObj = parseXml(xml);
  const lifecycleConfig = xmlObj.LifecycleConfiguration;
  if (lifecycleConfig && lifecycleConfig.Rule) {
    // A single <Rule> is parsed as an object; normalize to an array so the
    // result matches the LifecycleConfig type and the multiple-rules case. See #1407.
    lifecycleConfig.Rule = toArray(lifecycleConfig.Rule);
  }
  return lifecycleConfig;
}
export function parseBucketEncryptionConfig(xml) {
  return parseXml(xml);
}
export function parseObjectRetentionConfig(xml) {
  const xmlObj = parseXml(xml);
  const retentionConfig = xmlObj.Retention;
  return {
    mode: retentionConfig.Mode,
    retainUntilDate: retentionConfig.RetainUntilDate
  };
}
export function removeObjectsParser(xml) {
  const xmlObj = parseXml(xml);
  if (xmlObj.DeleteResult && xmlObj.DeleteResult.Error) {
    // return errors as array always. as the response is object in case of single object passed in removeObjects
    return toArray(xmlObj.DeleteResult.Error);
  }
  return [];
}

// parse XML response for copy object
export function parseCopyObject(xml) {
  const result = {
    etag: '',
    lastModified: ''
  };
  let xmlobj = parseXml(xml);
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
  if (!isObject(opts)) {
    opts = {};
  }
  const name = sanitizeObjectKey(toArray(Key)[0] || '');
  const lastModified = LastModified ? new Date(toArray(LastModified)[0] || '') : undefined;
  const etag = sanitizeETag(toArray(ETag)[0] || '');
  const size = sanitizeSize(Size || '');
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
export function parseListObjects(xml) {
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
      toArray(commonPrefixEntry).forEach(commonPrefix => {
        result.objects.push({
          prefix: sanitizeObjectKey(toArray(commonPrefix.Prefix)[0] || ''),
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
      toArray(listBucketResult.Contents).forEach(content => {
        const name = sanitizeObjectKey(toArray(content.Key)[0] || '');
        const lastModified = new Date(toArray(content.LastModified)[0] || '');
        const etag = sanitizeETag(toArray(content.ETag)[0] || '');
        const size = sanitizeSize(content.Size || '');
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
      toArray(listVersionsResult.Version).forEach(content => {
        result.objects.push(formatObjInfo(content));
      });
    }
    if (listVersionsResult.DeleteMarker) {
      toArray(listVersionsResult.DeleteMarker).forEach(content => {
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
export function uploadPartParser(xml) {
  const xmlObj = parseXml(xml);
  const respEl = xmlObj.CopyPartResult;
  return respEl;
}
//# sourceMappingURL=data:application/json;charset=utf-8;base64,eyJ2ZXJzaW9uIjozLCJuYW1lcyI6WyJjcmMzMiIsIlhNTFBhcnNlciIsImVycm9ycyIsIlNlbGVjdFJlc3VsdHMiLCJpc09iamVjdCIsInBhcnNlWG1sIiwicmVhZGFibGVTdHJlYW0iLCJzYW5pdGl6ZUVUYWciLCJzYW5pdGl6ZU9iamVjdEtleSIsInNhbml0aXplU2l6ZSIsInRvQXJyYXkiLCJyZWFkQXNTdHJpbmciLCJSRVRFTlRJT05fVkFMSURJVFlfVU5JVFMiLCJwYXJzZUJ1Y2tldFJlZ2lvbiIsInhtbCIsIkxvY2F0aW9uQ29uc3RyYWludCIsImZ4cCIsImZ4cFdpdGhvdXROdW1QYXJzZXIiLCJudW1iZXJQYXJzZU9wdGlvbnMiLCJza2lwTGlrZSIsInBhcnNlRXJyb3IiLCJoZWFkZXJJbmZvIiwieG1sRXJyIiwieG1sT2JqIiwicGFyc2UiLCJFcnJvciIsImUiLCJTM0Vycm9yIiwiT2JqZWN0IiwiZW50cmllcyIsImZvckVhY2giLCJrZXkiLCJ2YWx1ZSIsInRvTG93ZXJDYXNlIiwicGFyc2VSZXNwb25zZUVycm9yIiwicmVzcG9uc2UiLCJzdGF0dXNDb2RlIiwiY29kZSIsIm1lc3NhZ2UiLCJoRXJyQ29kZSIsImhlYWRlcnMiLCJoRXJyRGVzYyIsImFtelJlcXVlc3RpZCIsImFteklkMiIsImFtekJ1Y2tldFJlZ2lvbiIsInhtbFN0cmluZyIsImNhdXNlIiwicGFyc2VMaXN0T2JqZWN0c1YyV2l0aE1ldGFkYXRhIiwicmVzdWx0Iiwib2JqZWN0cyIsImlzVHJ1bmNhdGVkIiwibmV4dENvbnRpbnVhdGlvblRva2VuIiwieG1sb2JqIiwiTGlzdEJ1Y2tldFJlc3VsdCIsIkludmFsaWRYTUxFcnJvciIsIklzVHJ1bmNhdGVkIiwiTmV4dENvbnRpbnVhdGlvblRva2VuIiwiQ29udGVudHMiLCJjb250ZW50IiwibmFtZSIsIktleSIsImxhc3RNb2RpZmllZCIsIkRhdGUiLCJMYXN0TW9kaWZpZWQiLCJldGFnIiwiRVRhZyIsInNpemUiLCJTaXplIiwidGFncyIsIlVzZXJUYWdzIiwic3BsaXQiLCJ0YWciLCJtZXRhZGF0YSIsIlVzZXJNZXRhZGF0YSIsInB1c2giLCJDb21tb25QcmVmaXhlcyIsImNvbW1vblByZWZpeCIsInByZWZpeCIsIlByZWZpeCIsInBhcnNlTGlzdE9iamVjdHNWMiIsInBhcnNlQnVja2V0Tm90aWZpY2F0aW9uIiwiVG9waWNDb25maWd1cmF0aW9uIiwiUXVldWVDb25maWd1cmF0aW9uIiwiQ2xvdWRGdW5jdGlvbkNvbmZpZ3VyYXRpb24iLCJnZW5FdmVudHMiLCJldmVudHMiLCJnZW5GaWx0ZXJSdWxlcyIsImZpbHRlcnMiLCJfZmlsdGVyQXJyJCIsInJ1bGVzIiwiZmlsdGVyQXJyIiwiUzNLZXkiLCJfczNLZXlBcnIkIiwiczNLZXlBcnIiLCJGaWx0ZXJSdWxlIiwicnVsZSIsInIiLCJOYW1lIiwiVmFsdWUiLCJOb3RpZmljYXRpb25Db25maWd1cmF0aW9uIiwiY29uZmlnIiwiSWQiLCJUb3BpYyIsIkV2ZW50IiwiRmlsdGVyIiwiUXVldWUiLCJDbG91ZEZ1bmN0aW9uIiwicGFyc2VMaXN0UGFydHMiLCJwYXJ0cyIsIm1hcmtlciIsIkxpc3RQYXJ0c1Jlc3VsdCIsIk5leHRQYXJ0TnVtYmVyTWFya2VyIiwiUGFydCIsInAiLCJwYXJ0IiwicGFyc2VJbnQiLCJQYXJ0TnVtYmVyIiwicmVwbGFjZSIsInBhcnNlTGlzdEJ1Y2tldCIsImxpc3RCdWNrZXRSZXN1bHRQYXJzZXIiLCJwYXJzZVRhZ1ZhbHVlIiwibGVhZGluZ1plcm9zIiwiaGV4IiwidGFnVmFsdWVQcm9jZXNzb3IiLCJ0YWdOYW1lIiwidGFnVmFsdWUiLCJ0b1N0cmluZyIsImlnbm9yZUF0dHJpYnV0ZXMiLCJwYXJzZWRYbWxSZXMiLCJMaXN0QWxsTXlCdWNrZXRzUmVzdWx0IiwiQnVja2V0cyIsIkJ1Y2tldCIsIm1hcCIsImJ1Y2tldCIsImJ1Y2tldE5hbWUiLCJDcmVhdGlvbkRhdGUiLCJjcmVhdGlvbkRhdGUiLCJwYXJzZUluaXRpYXRlTXVsdGlwYXJ0IiwiSW5pdGlhdGVNdWx0aXBhcnRVcGxvYWRSZXN1bHQiLCJVcGxvYWRJZCIsInBhcnNlUmVwbGljYXRpb25Db25maWciLCJSb2xlIiwiUnVsZSIsIlJlcGxpY2F0aW9uQ29uZmlndXJhdGlvbiIsInJvbGUiLCJwYXJzZU9iamVjdExlZ2FsSG9sZENvbmZpZyIsIkxlZ2FsSG9sZCIsInBhcnNlVGFnZ2luZyIsIlRhZ2dpbmciLCJUYWdTZXQiLCJUYWciLCJ0YWdSZXN1bHQiLCJBcnJheSIsImlzQXJyYXkiLCJwYXJzZUNvbXBsZXRlTXVsdGlwYXJ0IiwiQ29tcGxldGVNdWx0aXBhcnRVcGxvYWRSZXN1bHQiLCJMb2NhdGlvbiIsImxvY2F0aW9uIiwiQ29kZSIsIk1lc3NhZ2UiLCJlcnJDb2RlIiwiZXJyTWVzc2FnZSIsInBhcnNlTGlzdE11bHRpcGFydCIsInByZWZpeGVzIiwidXBsb2FkcyIsIm5leHRLZXlNYXJrZXIiLCJuZXh0VXBsb2FkSWRNYXJrZXIiLCJMaXN0TXVsdGlwYXJ0VXBsb2Fkc1Jlc3VsdCIsIk5leHRLZXlNYXJrZXIiLCJOZXh0VXBsb2FkSWRNYXJrZXIiLCJVcGxvYWQiLCJ1cGxvYWQiLCJ1cGxvYWRJdGVtIiwidXBsb2FkSWQiLCJzdG9yYWdlQ2xhc3MiLCJTdG9yYWdlQ2xhc3MiLCJpbml0aWF0ZWQiLCJJbml0aWF0ZWQiLCJJbml0aWF0b3IiLCJpbml0aWF0b3IiLCJpZCIsIklEIiwiZGlzcGxheU5hbWUiLCJEaXNwbGF5TmFtZSIsIk93bmVyIiwib3duZXIiLCJwYXJzZU9iamVjdExvY2tDb25maWciLCJsb2NrQ29uZmlnUmVzdWx0IiwiT2JqZWN0TG9ja0NvbmZpZ3VyYXRpb24iLCJvYmplY3RMb2NrRW5hYmxlZCIsIk9iamVjdExvY2tFbmFibGVkIiwicmV0ZW50aW9uUmVzcCIsIkRlZmF1bHRSZXRlbnRpb24iLCJtb2RlIiwiTW9kZSIsImlzVW5pdFllYXJzIiwiWWVhcnMiLCJ2YWxpZGl0eSIsInVuaXQiLCJZRUFSUyIsIkRheXMiLCJEQVlTIiwicGFyc2VCdWNrZXRWZXJzaW9uaW5nQ29uZmlnIiwiVmVyc2lvbmluZ0NvbmZpZ3VyYXRpb24iLCJleHRyYWN0SGVhZGVyVHlwZSIsInN0cmVhbSIsImhlYWRlck5hbWVMZW4iLCJCdWZmZXIiLCJmcm9tIiwicmVhZCIsInJlYWRVSW50OCIsImhlYWRlck5hbWVXaXRoU2VwYXJhdG9yIiwic3BsaXRCeVNlcGFyYXRvciIsImxlbmd0aCIsImV4dHJhY3RIZWFkZXJWYWx1ZSIsImJvZHlMZW4iLCJyZWFkVUludDE2QkUiLCJwYXJzZVNlbGVjdE9iamVjdENvbnRlbnRSZXNwb25zZSIsInJlcyIsInNlbGVjdFJlc3VsdHMiLCJyZXNwb25zZVN0cmVhbSIsIl9yZWFkYWJsZVN0YXRlIiwibXNnQ3JjQWNjdW11bGF0b3IiLCJ0b3RhbEJ5dGVMZW5ndGhCdWZmZXIiLCJoZWFkZXJCeXRlc0J1ZmZlciIsImNhbGN1bGF0ZWRQcmVsdWRlQ3JjIiwicmVhZEludDMyQkUiLCJwcmVsdWRlQ3JjQnVmZmVyIiwidG90YWxNc2dMZW5ndGgiLCJoZWFkZXJMZW5ndGgiLCJwcmVsdWRlQ3JjQnl0ZVZhbHVlIiwiaGVhZGVyQnl0ZXMiLCJoZWFkZXJSZWFkZXJTdHJlYW0iLCJoZWFkZXJUeXBlTmFtZSIsInBheWxvYWRTdHJlYW0iLCJwYXlMb2FkTGVuZ3RoIiwicGF5TG9hZEJ1ZmZlciIsIm1lc3NhZ2VDcmNCeXRlVmFsdWUiLCJjYWxjdWxhdGVkQ3JjIiwibWVzc2FnZVR5cGUiLCJlcnJvck1lc3NhZ2UiLCJjb250ZW50VHlwZSIsImV2ZW50VHlwZSIsInNldFJlc3BvbnNlIiwiX3BheWxvYWRTdHJlYW0iLCJyZWFkRGF0YSIsInNldFJlY29yZHMiLCJfcGF5bG9hZFN0cmVhbTIiLCJwcm9ncmVzc0RhdGEiLCJzZXRQcm9ncmVzcyIsIl9wYXlsb2FkU3RyZWFtMyIsInN0YXRzRGF0YSIsInNldFN0YXRzIiwid2FybmluZ01lc3NhZ2UiLCJjb25zb2xlIiwid2FybiIsInBhcnNlTGlmZWN5Y2xlQ29uZmlnIiwibGlmZWN5Y2xlQ29uZmlnIiwiTGlmZWN5Y2xlQ29uZmlndXJhdGlvbiIsInBhcnNlQnVja2V0RW5jcnlwdGlvbkNvbmZpZyIsInBhcnNlT2JqZWN0UmV0ZW50aW9uQ29uZmlnIiwicmV0ZW50aW9uQ29uZmlnIiwiUmV0ZW50aW9uIiwicmV0YWluVW50aWxEYXRlIiwiUmV0YWluVW50aWxEYXRlIiwicmVtb3ZlT2JqZWN0c1BhcnNlciIsIkRlbGV0ZVJlc3VsdCIsInBhcnNlQ29weU9iamVjdCIsIkNvcHlPYmplY3RSZXN1bHQiLCJmb3JtYXRPYmpJbmZvIiwib3B0cyIsIlZlcnNpb25JZCIsIklzTGF0ZXN0IiwidW5kZWZpbmVkIiwidmVyc2lvbklkIiwiaXNMYXRlc3QiLCJpc0RlbGV0ZU1hcmtlciIsIklzRGVsZXRlTWFya2VyIiwicGFyc2VMaXN0T2JqZWN0cyIsIm5leHRNYXJrZXIiLCJ2ZXJzaW9uSWRNYXJrZXIiLCJrZXlNYXJrZXIiLCJwYXJzZUNvbW1vblByZWZpeGVzRW50aXR5IiwiY29tbW9uUHJlZml4RW50cnkiLCJsaXN0QnVja2V0UmVzdWx0IiwibGlzdFZlcnNpb25zUmVzdWx0IiwiTGlzdFZlcnNpb25zUmVzdWx0IiwiTWFya2VyIiwiTmV4dE1hcmtlciIsIl9yZXN1bHQkb2JqZWN0cyIsIlZlcnNpb24iLCJEZWxldGVNYXJrZXIiLCJOZXh0VmVyc2lvbklkTWFya2VyIiwidXBsb2FkUGFydFBhcnNlciIsInJlc3BFbCIsIkNvcHlQYXJ0UmVzdWx0Il0sInNvdXJjZXMiOlsieG1sLXBhcnNlci50cyJdLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQgdHlwZSAqIGFzIGh0dHAgZnJvbSAnbm9kZTpodHRwJ1xuaW1wb3J0IHR5cGUgc3RyZWFtIGZyb20gJ25vZGU6c3RyZWFtJ1xuXG5pbXBvcnQgY3JjMzIgZnJvbSAnYnVmZmVyLWNyYzMyJ1xuaW1wb3J0IHsgWE1MUGFyc2VyIH0gZnJvbSAnZmFzdC14bWwtcGFyc2VyJ1xuXG5pbXBvcnQgKiBhcyBlcnJvcnMgZnJvbSAnLi4vZXJyb3JzLnRzJ1xuaW1wb3J0IHsgU2VsZWN0UmVzdWx0cyB9IGZyb20gJy4uL2hlbHBlcnMudHMnXG5pbXBvcnQgeyBpc09iamVjdCwgcGFyc2VYbWwsIHJlYWRhYmxlU3RyZWFtLCBzYW5pdGl6ZUVUYWcsIHNhbml0aXplT2JqZWN0S2V5LCBzYW5pdGl6ZVNpemUsIHRvQXJyYXkgfSBmcm9tICcuL2hlbHBlci50cydcbmltcG9ydCB7IHJlYWRBc1N0cmluZyB9IGZyb20gJy4vcmVzcG9uc2UudHMnXG5pbXBvcnQgdHlwZSB7XG4gIEJ1Y2tldEl0ZW1Gcm9tTGlzdCxcbiAgQnVja2V0SXRlbVdpdGhNZXRhZGF0YSxcbiAgQ2xvdWRGdW5jdGlvbkNvbmZpZ0VudHJ5LFxuICBDb21tb25QcmVmaXgsXG4gIENvcHlPYmplY3RSZXN1bHRWMSxcbiAgTGlzdEJ1Y2tldFJlc3VsdFYxLFxuICBMaXN0T2JqZWN0VjJSZXMsXG4gIE5vdGlmaWNhdGlvbkNvbmZpZ1Jlc3VsdCxcbiAgT2JqZWN0SW5mbyxcbiAgT2JqZWN0TG9ja0luZm8sXG4gIE9iamVjdFJvd0VudHJ5LFxuICBRdWV1ZUNvbmZpZ0VudHJ5LFxuICBSZXBsaWNhdGlvbkNvbmZpZyxcbiAgVGFnLFxuICBUYWdzLFxuICBUb3BpY0NvbmZpZ0VudHJ5LFxufSBmcm9tICcuL3R5cGUudHMnXG5pbXBvcnQgeyBSRVRFTlRJT05fVkFMSURJVFlfVU5JVFMgfSBmcm9tICcuL3R5cGUudHMnXG5cbi8vIHBhcnNlIFhNTCByZXNwb25zZSBmb3IgYnVja2V0IHJlZ2lvblxuZXhwb3J0IGZ1bmN0aW9uIHBhcnNlQnVja2V0UmVnaW9uKHhtbDogc3RyaW5nKTogc3RyaW5nIHtcbiAgLy8gcmV0dXJuIHJlZ2lvbiBpbmZvcm1hdGlvblxuICByZXR1cm4gcGFyc2VYbWwoeG1sKS5Mb2NhdGlvbkNvbnN0cmFpbnRcbn1cblxuY29uc3QgZnhwID0gbmV3IFhNTFBhcnNlcigpXG5cbmNvbnN0IGZ4cFdpdGhvdXROdW1QYXJzZXIgPSBuZXcgWE1MUGFyc2VyKHtcbiAgLy8gQHRzLWlnbm9yZVxuICBudW1iZXJQYXJzZU9wdGlvbnM6IHtcbiAgICBza2lwTGlrZTogLy4vLFxuICB9LFxufSlcblxuLy8gUGFyc2UgWE1MIGFuZCByZXR1cm4gaW5mb3JtYXRpb24gYXMgSmF2YXNjcmlwdCB0eXBlc1xuLy8gcGFyc2UgZXJyb3IgWE1MIHJlc3BvbnNlXG5leHBvcnQgZnVuY3Rpb24gcGFyc2VFcnJvcih4bWw6IHN0cmluZywgaGVhZGVySW5mbzogUmVjb3JkPHN0cmluZywgdW5rbm93bj4pIHtcbiAgbGV0IHhtbEVyciA9IHt9XG4gIGNvbnN0IHhtbE9iaiA9IGZ4cC5wYXJzZSh4bWwpXG4gIGlmICh4bWxPYmouRXJyb3IpIHtcbiAgICB4bWxFcnIgPSB4bWxPYmouRXJyb3JcbiAgfVxuICBjb25zdCBlID0gbmV3IGVycm9ycy5TM0Vycm9yKCkgYXMgdW5rbm93biBhcyBSZWNvcmQ8c3RyaW5nLCB1bmtub3duPlxuICBPYmplY3QuZW50cmllcyh4bWxFcnIpLmZvckVhY2goKFtrZXksIHZhbHVlXSkgPT4ge1xuICAgIGVba2V5LnRvTG93ZXJDYXNlKCldID0gdmFsdWVcbiAgfSlcbiAgT2JqZWN0LmVudHJpZXMoaGVhZGVySW5mbykuZm9yRWFjaCgoW2tleSwgdmFsdWVdKSA9PiB7XG4gICAgZVtrZXldID0gdmFsdWVcbiAgfSlcbiAgcmV0dXJuIGVcbn1cblxuLy8gR2VuZXJhdGVzIGFuIEVycm9yIG9iamVjdCBkZXBlbmRpbmcgb24gaHR0cCBzdGF0dXNDb2RlIGFuZCBYTUwgYm9keVxuZXhwb3J0IGFzeW5jIGZ1bmN0aW9uIHBhcnNlUmVzcG9uc2VFcnJvcihyZXNwb25zZTogaHR0cC5JbmNvbWluZ01lc3NhZ2UpOiBQcm9taXNlPFJlY29yZDxzdHJpbmcsIHN0cmluZz4+IHtcbiAgY29uc3Qgc3RhdHVzQ29kZSA9IHJlc3BvbnNlLnN0YXR1c0NvZGVcbiAgbGV0IGNvZGUgPSAnJyxcbiAgICBtZXNzYWdlID0gJydcbiAgaWYgKHN0YXR1c0NvZGUgPT09IDMwMSkge1xuICAgIGNvZGUgPSAnTW92ZWRQZXJtYW5lbnRseSdcbiAgICBtZXNzYWdlID0gJ01vdmVkIFBlcm1hbmVudGx5J1xuICB9IGVsc2UgaWYgKHN0YXR1c0NvZGUgPT09IDMwNykge1xuICAgIGNvZGUgPSAnVGVtcG9yYXJ5UmVkaXJlY3QnXG4gICAgbWVzc2FnZSA9ICdBcmUgeW91IHVzaW5nIHRoZSBjb3JyZWN0IGVuZHBvaW50IFVSTD8nXG4gIH0gZWxzZSBpZiAoc3RhdHVzQ29kZSA9PT0gNDAzKSB7XG4gICAgY29kZSA9ICdBY2Nlc3NEZW5pZWQnXG4gICAgbWVzc2FnZSA9ICdWYWxpZCBhbmQgYXV0aG9yaXplZCBjcmVkZW50aWFscyByZXF1aXJlZCdcbiAgfSBlbHNlIGlmIChzdGF0dXNDb2RlID09PSA0MDQpIHtcbiAgICBjb2RlID0gJ05vdEZvdW5kJ1xuICAgIG1lc3NhZ2UgPSAnTm90IEZvdW5kJ1xuICB9IGVsc2UgaWYgKHN0YXR1c0NvZGUgPT09IDQwNSkge1xuICAgIGNvZGUgPSAnTWV0aG9kTm90QWxsb3dlZCdcbiAgICBtZXNzYWdlID0gJ01ldGhvZCBOb3QgQWxsb3dlZCdcbiAgfSBlbHNlIGlmIChzdGF0dXNDb2RlID09PSA1MDEpIHtcbiAgICBjb2RlID0gJ01ldGhvZE5vdEFsbG93ZWQnXG4gICAgbWVzc2FnZSA9ICdNZXRob2QgTm90IEFsbG93ZWQnXG4gIH0gZWxzZSBpZiAoc3RhdHVzQ29kZSA9PT0gNTAzKSB7XG4gICAgY29kZSA9ICdTbG93RG93bidcbiAgICBtZXNzYWdlID0gJ1BsZWFzZSByZWR1Y2UgeW91ciByZXF1ZXN0IHJhdGUuJ1xuICB9IGVsc2Uge1xuICAgIGNvbnN0IGhFcnJDb2RlID0gcmVzcG9uc2UuaGVhZGVyc1sneC1taW5pby1lcnJvci1jb2RlJ10gYXMgc3RyaW5nXG4gICAgY29uc3QgaEVyckRlc2MgPSByZXNwb25zZS5oZWFkZXJzWyd4LW1pbmlvLWVycm9yLWRlc2MnXSBhcyBzdHJpbmdcblxuICAgIGlmIChoRXJyQ29kZSAmJiBoRXJyRGVzYykge1xuICAgICAgY29kZSA9IGhFcnJDb2RlXG4gICAgICBtZXNzYWdlID0gaEVyckRlc2NcbiAgICB9XG4gIH1cbiAgY29uc3QgaGVhZGVySW5mbzogUmVjb3JkPHN0cmluZywgc3RyaW5nIHwgdW5kZWZpbmVkIHwgbnVsbD4gPSB7fVxuICAvLyBBIHZhbHVlIGNyZWF0ZWQgYnkgUzMgY29tcGF0aWJsZSBzZXJ2ZXIgdGhhdCB1bmlxdWVseSBpZGVudGlmaWVzIHRoZSByZXF1ZXN0LlxuICBoZWFkZXJJbmZvLmFtelJlcXVlc3RpZCA9IHJlc3BvbnNlLmhlYWRlcnNbJ3gtYW16LXJlcXVlc3QtaWQnXSBhcyBzdHJpbmcgfCB1bmRlZmluZWRcbiAgLy8gQSBzcGVjaWFsIHRva2VuIHRoYXQgaGVscHMgdHJvdWJsZXNob290IEFQSSByZXBsaWVzIGFuZCBpc3N1ZXMuXG4gIGhlYWRlckluZm8uYW16SWQyID0gcmVzcG9uc2UuaGVhZGVyc1sneC1hbXotaWQtMiddIGFzIHN0cmluZyB8IHVuZGVmaW5lZFxuXG4gIC8vIFJlZ2lvbiB3aGVyZSB0aGUgYnVja2V0IGlzIGxvY2F0ZWQuIFRoaXMgaGVhZGVyIGlzIHJldHVybmVkIG9ubHlcbiAgLy8gaW4gSEVBRCBidWNrZXQgYW5kIExpc3RPYmplY3RzIHJlc3BvbnNlLlxuICBoZWFkZXJJbmZvLmFtekJ1Y2tldFJlZ2lvbiA9IHJlc3BvbnNlLmhlYWRlcnNbJ3gtYW16LWJ1Y2tldC1yZWdpb24nXSBhcyBzdHJpbmcgfCB1bmRlZmluZWRcblxuICBjb25zdCB4bWxTdHJpbmcgPSBhd2FpdCByZWFkQXNTdHJpbmcocmVzcG9uc2UpXG5cbiAgaWYgKHhtbFN0cmluZykge1xuICAgIHRocm93IHBhcnNlRXJyb3IoeG1sU3RyaW5nLCBoZWFkZXJJbmZvKVxuICB9XG5cbiAgLy8gTWVzc2FnZSBzaG91bGQgYmUgaW5zdGFudGlhdGVkIGZvciBlYWNoIFMzRXJyb3JzLlxuICBjb25zdCBlID0gbmV3IGVycm9ycy5TM0Vycm9yKG1lc3NhZ2UsIHsgY2F1c2U6IGhlYWRlckluZm8gfSlcbiAgLy8gUzMgRXJyb3IgY29kZS5cbiAgZS5jb2RlID0gY29kZVxuICBPYmplY3QuZW50cmllcyhoZWFkZXJJbmZvKS5mb3JFYWNoKChba2V5LCB2YWx1ZV0pID0+IHtcbiAgICAvLyBAdHMtZXhwZWN0LWVycm9yIGZvcmNlIHNldCBlcnJvciBwcm9wZXJ0aWVzXG4gICAgZVtrZXldID0gdmFsdWVcbiAgfSlcblxuICB0aHJvdyBlXG59XG5cbi8qKlxuICogcGFyc2UgWE1MIHJlc3BvbnNlIGZvciBsaXN0IG9iamVjdHMgdjIgd2l0aCBtZXRhZGF0YSBpbiBhIGJ1Y2tldFxuICovXG5leHBvcnQgZnVuY3Rpb24gcGFyc2VMaXN0T2JqZWN0c1YyV2l0aE1ldGFkYXRhKHhtbDogc3RyaW5nKSB7XG4gIGNvbnN0IHJlc3VsdDoge1xuICAgIG9iamVjdHM6IEFycmF5PEJ1Y2tldEl0ZW1XaXRoTWV0YWRhdGE+XG4gICAgaXNUcnVuY2F0ZWQ6IGJvb2xlYW5cbiAgICBuZXh0Q29udGludWF0aW9uVG9rZW46IHN0cmluZ1xuICB9ID0ge1xuICAgIG9iamVjdHM6IFtdLFxuICAgIGlzVHJ1bmNhdGVkOiBmYWxzZSxcbiAgICBuZXh0Q29udGludWF0aW9uVG9rZW46ICcnLFxuICB9XG5cbiAgbGV0IHhtbG9iaiA9IHBhcnNlWG1sKHhtbClcbiAgaWYgKCF4bWxvYmouTGlzdEJ1Y2tldFJlc3VsdCkge1xuICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZFhNTEVycm9yKCdNaXNzaW5nIHRhZzogXCJMaXN0QnVja2V0UmVzdWx0XCInKVxuICB9XG4gIHhtbG9iaiA9IHhtbG9iai5MaXN0QnVja2V0UmVzdWx0XG4gIGlmICh4bWxvYmouSXNUcnVuY2F0ZWQpIHtcbiAgICByZXN1bHQuaXNUcnVuY2F0ZWQgPSB4bWxvYmouSXNUcnVuY2F0ZWRcbiAgfVxuICBpZiAoeG1sb2JqLk5leHRDb250aW51YXRpb25Ub2tlbikge1xuICAgIHJlc3VsdC5uZXh0Q29udGludWF0aW9uVG9rZW4gPSB4bWxvYmouTmV4dENvbnRpbnVhdGlvblRva2VuXG4gIH1cblxuICBpZiAoeG1sb2JqLkNvbnRlbnRzKSB7XG4gICAgdG9BcnJheSh4bWxvYmouQ29udGVudHMpLmZvckVhY2goKGNvbnRlbnQpID0+IHtcbiAgICAgIGNvbnN0IG5hbWUgPSBzYW5pdGl6ZU9iamVjdEtleShjb250ZW50LktleSlcbiAgICAgIGNvbnN0IGxhc3RNb2RpZmllZCA9IG5ldyBEYXRlKGNvbnRlbnQuTGFzdE1vZGlmaWVkKVxuICAgICAgY29uc3QgZXRhZyA9IHNhbml0aXplRVRhZyhjb250ZW50LkVUYWcpXG4gICAgICBjb25zdCBzaXplID0gY29udGVudC5TaXplXG5cbiAgICAgIGxldCB0YWdzOiBUYWdzID0ge31cbiAgICAgIGlmIChjb250ZW50LlVzZXJUYWdzICE9IG51bGwpIHtcbiAgICAgICAgdG9BcnJheShjb250ZW50LlVzZXJUYWdzLnNwbGl0KCcmJykpLmZvckVhY2goKHRhZykgPT4ge1xuICAgICAgICAgIGNvbnN0IFtrZXksIHZhbHVlXSA9IHRhZy5zcGxpdCgnPScpXG4gICAgICAgICAgdGFnc1trZXldID0gdmFsdWVcbiAgICAgICAgfSlcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIHRhZ3MgPSB7fVxuICAgICAgfVxuXG4gICAgICBsZXQgbWV0YWRhdGFcbiAgICAgIGlmIChjb250ZW50LlVzZXJNZXRhZGF0YSAhPSBudWxsKSB7XG4gICAgICAgIG1ldGFkYXRhID0gdG9BcnJheShjb250ZW50LlVzZXJNZXRhZGF0YSlbMF1cbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIG1ldGFkYXRhID0gbnVsbFxuICAgICAgfVxuICAgICAgcmVzdWx0Lm9iamVjdHMucHVzaCh7IG5hbWUsIGxhc3RNb2RpZmllZCwgZXRhZywgc2l6ZSwgbWV0YWRhdGEsIHRhZ3MgfSlcbiAgICB9KVxuICB9XG5cbiAgaWYgKHhtbG9iai5Db21tb25QcmVmaXhlcykge1xuICAgIHRvQXJyYXkoeG1sb2JqLkNvbW1vblByZWZpeGVzKS5mb3JFYWNoKChjb21tb25QcmVmaXgpID0+IHtcbiAgICAgIHJlc3VsdC5vYmplY3RzLnB1c2goeyBwcmVmaXg6IHNhbml0aXplT2JqZWN0S2V5KHRvQXJyYXkoY29tbW9uUHJlZml4LlByZWZpeClbMF0pLCBzaXplOiAwIH0pXG4gICAgfSlcbiAgfVxuICByZXR1cm4gcmVzdWx0XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBwYXJzZUxpc3RPYmplY3RzVjIoeG1sOiBzdHJpbmcpOiBMaXN0T2JqZWN0VjJSZXMge1xuICBjb25zdCByZXN1bHQ6IExpc3RPYmplY3RWMlJlcyA9IHtcbiAgICBvYmplY3RzOiBbXSxcbiAgICBpc1RydW5jYXRlZDogZmFsc2UsXG4gICAgbmV4dENvbnRpbnVhdGlvblRva2VuOiAnJyxcbiAgfVxuXG4gIGxldCB4bWxvYmogPSBwYXJzZVhtbCh4bWwpXG4gIGlmICgheG1sb2JqLkxpc3RCdWNrZXRSZXN1bHQpIHtcbiAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRYTUxFcnJvcignTWlzc2luZyB0YWc6IFwiTGlzdEJ1Y2tldFJlc3VsdFwiJylcbiAgfVxuICB4bWxvYmogPSB4bWxvYmouTGlzdEJ1Y2tldFJlc3VsdFxuICBpZiAoeG1sb2JqLklzVHJ1bmNhdGVkKSB7XG4gICAgcmVzdWx0LmlzVHJ1bmNhdGVkID0geG1sb2JqLklzVHJ1bmNhdGVkXG4gIH1cbiAgaWYgKHhtbG9iai5OZXh0Q29udGludWF0aW9uVG9rZW4pIHtcbiAgICByZXN1bHQubmV4dENvbnRpbnVhdGlvblRva2VuID0geG1sb2JqLk5leHRDb250aW51YXRpb25Ub2tlblxuICB9XG4gIGlmICh4bWxvYmouQ29udGVudHMpIHtcbiAgICB0b0FycmF5KHhtbG9iai5Db250ZW50cykuZm9yRWFjaCgoY29udGVudCkgPT4ge1xuICAgICAgY29uc3QgbmFtZSA9IHNhbml0aXplT2JqZWN0S2V5KHRvQXJyYXkoY29udGVudC5LZXkpWzBdKVxuICAgICAgY29uc3QgbGFzdE1vZGlmaWVkID0gbmV3IERhdGUoY29udGVudC5MYXN0TW9kaWZpZWQpXG4gICAgICBjb25zdCBldGFnID0gc2FuaXRpemVFVGFnKGNvbnRlbnQuRVRhZylcbiAgICAgIGNvbnN0IHNpemUgPSBjb250ZW50LlNpemVcbiAgICAgIHJlc3VsdC5vYmplY3RzLnB1c2goeyBuYW1lLCBsYXN0TW9kaWZpZWQsIGV0YWcsIHNpemUgfSlcbiAgICB9KVxuICB9XG4gIGlmICh4bWxvYmouQ29tbW9uUHJlZml4ZXMpIHtcbiAgICB0b0FycmF5KHhtbG9iai5Db21tb25QcmVmaXhlcykuZm9yRWFjaCgoY29tbW9uUHJlZml4KSA9PiB7XG4gICAgICByZXN1bHQub2JqZWN0cy5wdXNoKHsgcHJlZml4OiBzYW5pdGl6ZU9iamVjdEtleSh0b0FycmF5KGNvbW1vblByZWZpeC5QcmVmaXgpWzBdKSwgc2l6ZTogMCB9KVxuICAgIH0pXG4gIH1cbiAgcmV0dXJuIHJlc3VsdFxufVxuXG5leHBvcnQgZnVuY3Rpb24gcGFyc2VCdWNrZXROb3RpZmljYXRpb24oeG1sOiBzdHJpbmcpOiBOb3RpZmljYXRpb25Db25maWdSZXN1bHQge1xuICBjb25zdCByZXN1bHQ6IE5vdGlmaWNhdGlvbkNvbmZpZ1Jlc3VsdCA9IHtcbiAgICBUb3BpY0NvbmZpZ3VyYXRpb246IFtdLFxuICAgIFF1ZXVlQ29uZmlndXJhdGlvbjogW10sXG4gICAgQ2xvdWRGdW5jdGlvbkNvbmZpZ3VyYXRpb246IFtdLFxuICB9XG5cbiAgY29uc3QgZ2VuRXZlbnRzID0gKGV2ZW50czogdW5rbm93bik6IHN0cmluZ1tdID0+IHtcbiAgICBpZiAoIWV2ZW50cykge1xuICAgICAgcmV0dXJuIFtdXG4gICAgfVxuICAgIHJldHVybiB0b0FycmF5KGV2ZW50cykgYXMgc3RyaW5nW11cbiAgfVxuXG4gIGNvbnN0IGdlbkZpbHRlclJ1bGVzID0gKGZpbHRlcnM6IHVua25vd24pOiB7IE5hbWU6IHN0cmluZzsgVmFsdWU6IHN0cmluZyB9W10gPT4ge1xuICAgIGNvbnN0IHJ1bGVzOiB7IE5hbWU6IHN0cmluZzsgVmFsdWU6IHN0cmluZyB9W10gPSBbXVxuICAgIGlmICghZmlsdGVycykge1xuICAgICAgcmV0dXJuIHJ1bGVzXG4gICAgfVxuICAgIGNvbnN0IGZpbHRlckFyciA9IHRvQXJyYXkoZmlsdGVycykgYXMgUmVjb3JkPHN0cmluZywgdW5rbm93bj5bXVxuICAgIGlmIChmaWx0ZXJBcnJbMF0/LlMzS2V5KSB7XG4gICAgICBjb25zdCBzM0tleUFyciA9IHRvQXJyYXkoKGZpbHRlckFyclswXSBhcyBSZWNvcmQ8c3RyaW5nLCB1bmtub3duPikuUzNLZXkpIGFzIFJlY29yZDxzdHJpbmcsIHVua25vd24+W11cbiAgICAgIGlmIChzM0tleUFyclswXT8uRmlsdGVyUnVsZSkge1xuICAgICAgICB0b0FycmF5KHMzS2V5QXJyWzBdLkZpbHRlclJ1bGUpLmZvckVhY2goKHJ1bGU6IHVua25vd24pID0+IHtcbiAgICAgICAgICBjb25zdCByID0gcnVsZSBhcyBSZWNvcmQ8c3RyaW5nLCB1bmtub3duPlxuICAgICAgICAgIGNvbnN0IE5hbWUgPSB0b0FycmF5KHIuTmFtZSlbMF0gYXMgc3RyaW5nXG4gICAgICAgICAgY29uc3QgVmFsdWUgPSB0b0FycmF5KHIuVmFsdWUpWzBdIGFzIHN0cmluZ1xuICAgICAgICAgIHJ1bGVzLnB1c2goeyBOYW1lLCBWYWx1ZSB9KVxuICAgICAgICB9KVxuICAgICAgfVxuICAgIH1cbiAgICByZXR1cm4gcnVsZXNcbiAgfVxuXG4gIGxldCB4bWxvYmogPSBwYXJzZVhtbCh4bWwpXG4gIHhtbG9iaiA9IHhtbG9iai5Ob3RpZmljYXRpb25Db25maWd1cmF0aW9uXG5cbiAgaWYgKHhtbG9iai5Ub3BpY0NvbmZpZ3VyYXRpb24pIHtcbiAgICB0b0FycmF5KHhtbG9iai5Ub3BpY0NvbmZpZ3VyYXRpb24pLmZvckVhY2goKGNvbmZpZzogUmVjb3JkPHN0cmluZywgdW5rbm93bj4pID0+IHtcbiAgICAgIGNvbnN0IElkID0gdG9BcnJheShjb25maWcuSWQpWzBdIGFzIHN0cmluZ1xuICAgICAgY29uc3QgVG9waWMgPSB0b0FycmF5KGNvbmZpZy5Ub3BpYylbMF0gYXMgc3RyaW5nXG4gICAgICBjb25zdCBFdmVudCA9IGdlbkV2ZW50cyhjb25maWcuRXZlbnQpXG4gICAgICBjb25zdCBGaWx0ZXIgPSBnZW5GaWx0ZXJSdWxlcyhjb25maWcuRmlsdGVyKVxuICAgICAgcmVzdWx0LlRvcGljQ29uZmlndXJhdGlvbi5wdXNoKHsgSWQsIFRvcGljLCBFdmVudCwgRmlsdGVyIH0gYXMgVG9waWNDb25maWdFbnRyeSlcbiAgICB9KVxuICB9XG4gIGlmICh4bWxvYmouUXVldWVDb25maWd1cmF0aW9uKSB7XG4gICAgdG9BcnJheSh4bWxvYmouUXVldWVDb25maWd1cmF0aW9uKS5mb3JFYWNoKChjb25maWc6IFJlY29yZDxzdHJpbmcsIHVua25vd24+KSA9PiB7XG4gICAgICBjb25zdCBJZCA9IHRvQXJyYXkoY29uZmlnLklkKVswXSBhcyBzdHJpbmdcbiAgICAgIGNvbnN0IFF1ZXVlID0gdG9BcnJheShjb25maWcuUXVldWUpWzBdIGFzIHN0cmluZ1xuICAgICAgY29uc3QgRXZlbnQgPSBnZW5FdmVudHMoY29uZmlnLkV2ZW50KVxuICAgICAgY29uc3QgRmlsdGVyID0gZ2VuRmlsdGVyUnVsZXMoY29uZmlnLkZpbHRlcilcbiAgICAgIHJlc3VsdC5RdWV1ZUNvbmZpZ3VyYXRpb24ucHVzaCh7IElkLCBRdWV1ZSwgRXZlbnQsIEZpbHRlciB9IGFzIFF1ZXVlQ29uZmlnRW50cnkpXG4gICAgfSlcbiAgfVxuICBpZiAoeG1sb2JqLkNsb3VkRnVuY3Rpb25Db25maWd1cmF0aW9uKSB7XG4gICAgdG9BcnJheSh4bWxvYmouQ2xvdWRGdW5jdGlvbkNvbmZpZ3VyYXRpb24pLmZvckVhY2goKGNvbmZpZzogUmVjb3JkPHN0cmluZywgdW5rbm93bj4pID0+IHtcbiAgICAgIGNvbnN0IElkID0gdG9BcnJheShjb25maWcuSWQpWzBdIGFzIHN0cmluZ1xuICAgICAgY29uc3QgQ2xvdWRGdW5jdGlvbiA9IHRvQXJyYXkoY29uZmlnLkNsb3VkRnVuY3Rpb24pWzBdIGFzIHN0cmluZ1xuICAgICAgY29uc3QgRXZlbnQgPSBnZW5FdmVudHMoY29uZmlnLkV2ZW50KVxuICAgICAgY29uc3QgRmlsdGVyID0gZ2VuRmlsdGVyUnVsZXMoY29uZmlnLkZpbHRlcilcbiAgICAgIHJlc3VsdC5DbG91ZEZ1bmN0aW9uQ29uZmlndXJhdGlvbi5wdXNoKHsgSWQsIENsb3VkRnVuY3Rpb24sIEV2ZW50LCBGaWx0ZXIgfSBhcyBDbG91ZEZ1bmN0aW9uQ29uZmlnRW50cnkpXG4gICAgfSlcbiAgfVxuXG4gIHJldHVybiByZXN1bHRcbn1cblxuZXhwb3J0IHR5cGUgVXBsb2FkZWRQYXJ0ID0ge1xuICBwYXJ0OiBudW1iZXJcbiAgbGFzdE1vZGlmaWVkPzogRGF0ZVxuICBldGFnOiBzdHJpbmdcbiAgc2l6ZTogbnVtYmVyXG59XG5cbi8vIHBhcnNlIFhNTCByZXNwb25zZSBmb3IgbGlzdCBwYXJ0cyBvZiBhbiBpbiBwcm9ncmVzcyBtdWx0aXBhcnQgdXBsb2FkXG5leHBvcnQgZnVuY3Rpb24gcGFyc2VMaXN0UGFydHMoeG1sOiBzdHJpbmcpOiB7XG4gIGlzVHJ1bmNhdGVkOiBib29sZWFuXG4gIG1hcmtlcjogbnVtYmVyXG4gIHBhcnRzOiBVcGxvYWRlZFBhcnRbXVxufSB7XG4gIGxldCB4bWxvYmogPSBwYXJzZVhtbCh4bWwpXG4gIGNvbnN0IHJlc3VsdDoge1xuICAgIGlzVHJ1bmNhdGVkOiBib29sZWFuXG4gICAgbWFya2VyOiBudW1iZXJcbiAgICBwYXJ0czogVXBsb2FkZWRQYXJ0W11cbiAgfSA9IHtcbiAgICBpc1RydW5jYXRlZDogZmFsc2UsXG4gICAgcGFydHM6IFtdLFxuICAgIG1hcmtlcjogMCxcbiAgfVxuICBpZiAoIXhtbG9iai5MaXN0UGFydHNSZXN1bHQpIHtcbiAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRYTUxFcnJvcignTWlzc2luZyB0YWc6IFwiTGlzdFBhcnRzUmVzdWx0XCInKVxuICB9XG4gIHhtbG9iaiA9IHhtbG9iai5MaXN0UGFydHNSZXN1bHRcbiAgaWYgKHhtbG9iai5Jc1RydW5jYXRlZCkge1xuICAgIHJlc3VsdC5pc1RydW5jYXRlZCA9IHhtbG9iai5Jc1RydW5jYXRlZFxuICB9XG4gIGlmICh4bWxvYmouTmV4dFBhcnROdW1iZXJNYXJrZXIpIHtcbiAgICByZXN1bHQubWFya2VyID0gdG9BcnJheSh4bWxvYmouTmV4dFBhcnROdW1iZXJNYXJrZXIpWzBdIHx8ICcnXG4gIH1cbiAgaWYgKHhtbG9iai5QYXJ0KSB7XG4gICAgdG9BcnJheSh4bWxvYmouUGFydCkuZm9yRWFjaCgocCkgPT4ge1xuICAgICAgY29uc3QgcGFydCA9IHBhcnNlSW50KHRvQXJyYXkocC5QYXJ0TnVtYmVyKVswXSwgMTApXG4gICAgICBjb25zdCBsYXN0TW9kaWZpZWQgPSBuZXcgRGF0ZShwLkxhc3RNb2RpZmllZClcbiAgICAgIGNvbnN0IGV0YWcgPSBwLkVUYWcucmVwbGFjZSgvXlwiL2csICcnKVxuICAgICAgICAucmVwbGFjZSgvXCIkL2csICcnKVxuICAgICAgICAucmVwbGFjZSgvXiZxdW90Oy9nLCAnJylcbiAgICAgICAgLnJlcGxhY2UoLyZxdW90OyQvZywgJycpXG4gICAgICAgIC5yZXBsYWNlKC9eJiMzNDsvZywgJycpXG4gICAgICAgIC5yZXBsYWNlKC8mIzM0OyQvZywgJycpXG4gICAgICByZXN1bHQucGFydHMucHVzaCh7IHBhcnQsIGxhc3RNb2RpZmllZCwgZXRhZywgc2l6ZTogcGFyc2VJbnQocC5TaXplLCAxMCkgfSlcbiAgICB9KVxuICB9XG4gIHJldHVybiByZXN1bHRcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHBhcnNlTGlzdEJ1Y2tldCh4bWw6IHN0cmluZyk6IEJ1Y2tldEl0ZW1Gcm9tTGlzdFtdIHtcbiAgbGV0IHJlc3VsdDogQnVja2V0SXRlbUZyb21MaXN0W10gPSBbXVxuICBjb25zdCBsaXN0QnVja2V0UmVzdWx0UGFyc2VyID0gbmV3IFhNTFBhcnNlcih7XG4gICAgcGFyc2VUYWdWYWx1ZTogdHJ1ZSwgLy8gRW5hYmxlIHBhcnNpbmcgb2YgdmFsdWVzXG4gICAgbnVtYmVyUGFyc2VPcHRpb25zOiB7XG4gICAgICBsZWFkaW5nWmVyb3M6IGZhbHNlLCAvLyBEaXNhYmxlIG51bWJlciBwYXJzaW5nIGZvciB2YWx1ZXMgd2l0aCBsZWFkaW5nIHplcm9zXG4gICAgICBoZXg6IGZhbHNlLCAvLyBEaXNhYmxlIGhleCBudW1iZXIgcGFyc2luZyAtIEludmFsaWQgYnVja2V0IG5hbWVcbiAgICAgIHNraXBMaWtlOiAvXlswLTldKyQvLCAvLyBTa2lwIG51bWJlciBwYXJzaW5nIGlmIHRoZSB2YWx1ZSBjb25zaXN0cyBlbnRpcmVseSBvZiBkaWdpdHNcbiAgICB9LFxuICAgIHRhZ1ZhbHVlUHJvY2Vzc29yOiAodGFnTmFtZSwgdGFnVmFsdWUgPSAnJykgPT4ge1xuICAgICAgLy8gRW5zdXJlIHRoYXQgdGhlIE5hbWUgdGFnIGlzIGFsd2F5cyB0cmVhdGVkIGFzIGEgc3RyaW5nXG4gICAgICBpZiAodGFnTmFtZSA9PT0gJ05hbWUnKSB7XG4gICAgICAgIHJldHVybiB0YWdWYWx1ZS50b1N0cmluZygpXG4gICAgICB9XG4gICAgICByZXR1cm4gdGFnVmFsdWVcbiAgICB9LFxuICAgIGlnbm9yZUF0dHJpYnV0ZXM6IGZhbHNlLCAvLyBFbnN1cmUgdGhhdCBhbGwgYXR0cmlidXRlcyBhcmUgcGFyc2VkXG4gIH0pXG5cbiAgY29uc3QgcGFyc2VkWG1sUmVzID0gbGlzdEJ1Y2tldFJlc3VsdFBhcnNlci5wYXJzZSh4bWwpXG5cbiAgaWYgKCFwYXJzZWRYbWxSZXMuTGlzdEFsbE15QnVja2V0c1Jlc3VsdCkge1xuICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZFhNTEVycm9yKCdNaXNzaW5nIHRhZzogXCJMaXN0QWxsTXlCdWNrZXRzUmVzdWx0XCInKVxuICB9XG5cbiAgY29uc3QgeyBMaXN0QWxsTXlCdWNrZXRzUmVzdWx0OiB7IEJ1Y2tldHMgPSB7fSB9ID0ge30gfSA9IHBhcnNlZFhtbFJlc1xuXG4gIGlmIChCdWNrZXRzLkJ1Y2tldCkge1xuICAgIHJlc3VsdCA9IHRvQXJyYXkoQnVja2V0cy5CdWNrZXQpLm1hcCgoYnVja2V0ID0ge30pID0+IHtcbiAgICAgIGNvbnN0IHsgTmFtZTogYnVja2V0TmFtZSwgQ3JlYXRpb25EYXRlIH0gPSBidWNrZXRcbiAgICAgIGNvbnN0IGNyZWF0aW9uRGF0ZSA9IG5ldyBEYXRlKENyZWF0aW9uRGF0ZSlcblxuICAgICAgcmV0dXJuIHsgbmFtZTogYnVja2V0TmFtZSwgY3JlYXRpb25EYXRlIH1cbiAgICB9KVxuICB9XG5cbiAgcmV0dXJuIHJlc3VsdFxufVxuXG5leHBvcnQgZnVuY3Rpb24gcGFyc2VJbml0aWF0ZU11bHRpcGFydCh4bWw6IHN0cmluZyk6IHN0cmluZyB7XG4gIGxldCB4bWxvYmogPSBwYXJzZVhtbCh4bWwpXG5cbiAgaWYgKCF4bWxvYmouSW5pdGlhdGVNdWx0aXBhcnRVcGxvYWRSZXN1bHQpIHtcbiAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRYTUxFcnJvcignTWlzc2luZyB0YWc6IFwiSW5pdGlhdGVNdWx0aXBhcnRVcGxvYWRSZXN1bHRcIicpXG4gIH1cbiAgeG1sb2JqID0geG1sb2JqLkluaXRpYXRlTXVsdGlwYXJ0VXBsb2FkUmVzdWx0XG5cbiAgaWYgKHhtbG9iai5VcGxvYWRJZCkge1xuICAgIHJldHVybiB4bWxvYmouVXBsb2FkSWRcbiAgfVxuICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRYTUxFcnJvcignTWlzc2luZyB0YWc6IFwiVXBsb2FkSWRcIicpXG59XG5cbmV4cG9ydCBmdW5jdGlvbiBwYXJzZVJlcGxpY2F0aW9uQ29uZmlnKHhtbDogc3RyaW5nKTogUmVwbGljYXRpb25Db25maWcge1xuICBjb25zdCB4bWxPYmogPSBwYXJzZVhtbCh4bWwpXG4gIGNvbnN0IHsgUm9sZSwgUnVsZSB9ID0geG1sT2JqLlJlcGxpY2F0aW9uQ29uZmlndXJhdGlvblxuICByZXR1cm4ge1xuICAgIFJlcGxpY2F0aW9uQ29uZmlndXJhdGlvbjoge1xuICAgICAgcm9sZTogUm9sZSxcbiAgICAgIHJ1bGVzOiB0b0FycmF5KFJ1bGUpLFxuICAgIH0sXG4gIH1cbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHBhcnNlT2JqZWN0TGVnYWxIb2xkQ29uZmlnKHhtbDogc3RyaW5nKSB7XG4gIGNvbnN0IHhtbE9iaiA9IHBhcnNlWG1sKHhtbClcbiAgcmV0dXJuIHhtbE9iai5MZWdhbEhvbGRcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHBhcnNlVGFnZ2luZyh4bWw6IHN0cmluZykge1xuICBjb25zdCB4bWxPYmogPSBwYXJzZVhtbCh4bWwpXG4gIGxldCByZXN1bHQ6IFRhZ1tdID0gW11cbiAgaWYgKHhtbE9iai5UYWdnaW5nICYmIHhtbE9iai5UYWdnaW5nLlRhZ1NldCAmJiB4bWxPYmouVGFnZ2luZy5UYWdTZXQuVGFnKSB7XG4gICAgY29uc3QgdGFnUmVzdWx0OiBUYWcgPSB4bWxPYmouVGFnZ2luZy5UYWdTZXQuVGFnXG4gICAgLy8gaWYgaXQgaXMgYSBzaW5nbGUgdGFnIGNvbnZlcnQgaW50byBhbiBhcnJheSBzbyB0aGF0IHRoZSByZXR1cm4gdmFsdWUgaXMgYWx3YXlzIGFuIGFycmF5LlxuICAgIGlmIChBcnJheS5pc0FycmF5KHRhZ1Jlc3VsdCkpIHtcbiAgICAgIHJlc3VsdCA9IFsuLi50YWdSZXN1bHRdXG4gICAgfSBlbHNlIHtcbiAgICAgIHJlc3VsdC5wdXNoKHRhZ1Jlc3VsdClcbiAgICB9XG4gIH1cbiAgcmV0dXJuIHJlc3VsdFxufVxuXG4vLyBwYXJzZSBYTUwgcmVzcG9uc2Ugd2hlbiBhIG11bHRpcGFydCB1cGxvYWQgaXMgY29tcGxldGVkXG5leHBvcnQgZnVuY3Rpb24gcGFyc2VDb21wbGV0ZU11bHRpcGFydCh4bWw6IHN0cmluZykge1xuICBjb25zdCB4bWxvYmogPSBwYXJzZVhtbCh4bWwpLkNvbXBsZXRlTXVsdGlwYXJ0VXBsb2FkUmVzdWx0XG4gIGlmICh4bWxvYmouTG9jYXRpb24pIHtcbiAgICBjb25zdCBsb2NhdGlvbiA9IHRvQXJyYXkoeG1sb2JqLkxvY2F0aW9uKVswXVxuICAgIGNvbnN0IGJ1Y2tldCA9IHRvQXJyYXkoeG1sb2JqLkJ1Y2tldClbMF1cbiAgICBjb25zdCBrZXkgPSB4bWxvYmouS2V5XG4gICAgY29uc3QgZXRhZyA9IHhtbG9iai5FVGFnLnJlcGxhY2UoL15cIi9nLCAnJylcbiAgICAgIC5yZXBsYWNlKC9cIiQvZywgJycpXG4gICAgICAucmVwbGFjZSgvXiZxdW90Oy9nLCAnJylcbiAgICAgIC5yZXBsYWNlKC8mcXVvdDskL2csICcnKVxuICAgICAgLnJlcGxhY2UoL14mIzM0Oy9nLCAnJylcbiAgICAgIC5yZXBsYWNlKC8mIzM0OyQvZywgJycpXG5cbiAgICByZXR1cm4geyBsb2NhdGlvbiwgYnVja2V0LCBrZXksIGV0YWcgfVxuICB9XG4gIC8vIENvbXBsZXRlIE11bHRpcGFydCBjYW4gcmV0dXJuIFhNTCBFcnJvciBhZnRlciBhIDIwMCBPSyByZXNwb25zZVxuICBpZiAoeG1sb2JqLkNvZGUgJiYgeG1sb2JqLk1lc3NhZ2UpIHtcbiAgICBjb25zdCBlcnJDb2RlID0gdG9BcnJheSh4bWxvYmouQ29kZSlbMF1cbiAgICBjb25zdCBlcnJNZXNzYWdlID0gdG9BcnJheSh4bWxvYmouTWVzc2FnZSlbMF1cbiAgICByZXR1cm4geyBlcnJDb2RlLCBlcnJNZXNzYWdlIH1cbiAgfVxufVxuXG50eXBlIFVwbG9hZElEID0gc3RyaW5nXG5cbmV4cG9ydCB0eXBlIExpc3RNdWx0aXBhcnRSZXN1bHQgPSB7XG4gIHVwbG9hZHM6IHtcbiAgICBrZXk6IHN0cmluZ1xuICAgIHVwbG9hZElkOiBVcGxvYWRJRFxuICAgIGluaXRpYXRvcj86IHsgaWQ6IHN0cmluZzsgZGlzcGxheU5hbWU6IHN0cmluZyB9XG4gICAgb3duZXI/OiB7IGlkOiBzdHJpbmc7IGRpc3BsYXlOYW1lOiBzdHJpbmcgfVxuICAgIHN0b3JhZ2VDbGFzczogdW5rbm93blxuICAgIGluaXRpYXRlZDogRGF0ZVxuICB9W11cbiAgcHJlZml4ZXM6IHtcbiAgICBwcmVmaXg6IHN0cmluZ1xuICB9W11cbiAgaXNUcnVuY2F0ZWQ6IGJvb2xlYW5cbiAgbmV4dEtleU1hcmtlcjogc3RyaW5nXG4gIG5leHRVcGxvYWRJZE1hcmtlcjogc3RyaW5nXG59XG5cbi8vIHBhcnNlIFhNTCByZXNwb25zZSBmb3IgbGlzdGluZyBpbi1wcm9ncmVzcyBtdWx0aXBhcnQgdXBsb2Fkc1xuZXhwb3J0IGZ1bmN0aW9uIHBhcnNlTGlzdE11bHRpcGFydCh4bWw6IHN0cmluZyk6IExpc3RNdWx0aXBhcnRSZXN1bHQge1xuICBjb25zdCByZXN1bHQ6IExpc3RNdWx0aXBhcnRSZXN1bHQgPSB7XG4gICAgcHJlZml4ZXM6IFtdLFxuICAgIHVwbG9hZHM6IFtdLFxuICAgIGlzVHJ1bmNhdGVkOiBmYWxzZSxcbiAgICBuZXh0S2V5TWFya2VyOiAnJyxcbiAgICBuZXh0VXBsb2FkSWRNYXJrZXI6ICcnLFxuICB9XG5cbiAgbGV0IHhtbG9iaiA9IHBhcnNlWG1sKHhtbClcblxuICBpZiAoIXhtbG9iai5MaXN0TXVsdGlwYXJ0VXBsb2Fkc1Jlc3VsdCkge1xuICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZFhNTEVycm9yKCdNaXNzaW5nIHRhZzogXCJMaXN0TXVsdGlwYXJ0VXBsb2Fkc1Jlc3VsdFwiJylcbiAgfVxuICB4bWxvYmogPSB4bWxvYmouTGlzdE11bHRpcGFydFVwbG9hZHNSZXN1bHRcbiAgaWYgKHhtbG9iai5Jc1RydW5jYXRlZCkge1xuICAgIHJlc3VsdC5pc1RydW5jYXRlZCA9IHhtbG9iai5Jc1RydW5jYXRlZFxuICB9XG4gIGlmICh4bWxvYmouTmV4dEtleU1hcmtlcikge1xuICAgIHJlc3VsdC5uZXh0S2V5TWFya2VyID0geG1sb2JqLk5leHRLZXlNYXJrZXJcbiAgfVxuICBpZiAoeG1sb2JqLk5leHRVcGxvYWRJZE1hcmtlcikge1xuICAgIHJlc3VsdC5uZXh0VXBsb2FkSWRNYXJrZXIgPSB4bWxvYmoubmV4dFVwbG9hZElkTWFya2VyIHx8ICcnXG4gIH1cblxuICBpZiAoeG1sb2JqLkNvbW1vblByZWZpeGVzKSB7XG4gICAgdG9BcnJheSh4bWxvYmouQ29tbW9uUHJlZml4ZXMpLmZvckVhY2goKHByZWZpeCkgPT4ge1xuICAgICAgLy8gQHRzLWV4cGVjdC1lcnJvciBpbmRleCBjaGVja1xuICAgICAgcmVzdWx0LnByZWZpeGVzLnB1c2goeyBwcmVmaXg6IHNhbml0aXplT2JqZWN0S2V5KHRvQXJyYXk8c3RyaW5nPihwcmVmaXguUHJlZml4KVswXSkgfSlcbiAgICB9KVxuICB9XG5cbiAgaWYgKHhtbG9iai5VcGxvYWQpIHtcbiAgICB0b0FycmF5KHhtbG9iai5VcGxvYWQpLmZvckVhY2goKHVwbG9hZCkgPT4ge1xuICAgICAgY29uc3QgdXBsb2FkSXRlbTogTGlzdE11bHRpcGFydFJlc3VsdFsndXBsb2FkcyddW251bWJlcl0gPSB7XG4gICAgICAgIGtleTogdXBsb2FkLktleSxcbiAgICAgICAgdXBsb2FkSWQ6IHVwbG9hZC5VcGxvYWRJZCxcbiAgICAgICAgc3RvcmFnZUNsYXNzOiB1cGxvYWQuU3RvcmFnZUNsYXNzLFxuICAgICAgICBpbml0aWF0ZWQ6IG5ldyBEYXRlKHVwbG9hZC5Jbml0aWF0ZWQpLFxuICAgICAgfVxuICAgICAgaWYgKHVwbG9hZC5Jbml0aWF0b3IpIHtcbiAgICAgICAgdXBsb2FkSXRlbS5pbml0aWF0b3IgPSB7IGlkOiB1cGxvYWQuSW5pdGlhdG9yLklELCBkaXNwbGF5TmFtZTogdXBsb2FkLkluaXRpYXRvci5EaXNwbGF5TmFtZSB9XG4gICAgICB9XG4gICAgICBpZiAodXBsb2FkLk93bmVyKSB7XG4gICAgICAgIHVwbG9hZEl0ZW0ub3duZXIgPSB7IGlkOiB1cGxvYWQuT3duZXIuSUQsIGRpc3BsYXlOYW1lOiB1cGxvYWQuT3duZXIuRGlzcGxheU5hbWUgfVxuICAgICAgfVxuICAgICAgcmVzdWx0LnVwbG9hZHMucHVzaCh1cGxvYWRJdGVtKVxuICAgIH0pXG4gIH1cbiAgcmV0dXJuIHJlc3VsdFxufVxuXG5leHBvcnQgZnVuY3Rpb24gcGFyc2VPYmplY3RMb2NrQ29uZmlnKHhtbDogc3RyaW5nKTogT2JqZWN0TG9ja0luZm8ge1xuICBjb25zdCB4bWxPYmogPSBwYXJzZVhtbCh4bWwpXG4gIGxldCBsb2NrQ29uZmlnUmVzdWx0ID0ge30gYXMgT2JqZWN0TG9ja0luZm9cbiAgaWYgKHhtbE9iai5PYmplY3RMb2NrQ29uZmlndXJhdGlvbikge1xuICAgIGxvY2tDb25maWdSZXN1bHQgPSB7XG4gICAgICBvYmplY3RMb2NrRW5hYmxlZDogeG1sT2JqLk9iamVjdExvY2tDb25maWd1cmF0aW9uLk9iamVjdExvY2tFbmFibGVkLFxuICAgIH0gYXMgT2JqZWN0TG9ja0luZm9cbiAgICBsZXQgcmV0ZW50aW9uUmVzcFxuICAgIGlmIChcbiAgICAgIHhtbE9iai5PYmplY3RMb2NrQ29uZmlndXJhdGlvbiAmJlxuICAgICAgeG1sT2JqLk9iamVjdExvY2tDb25maWd1cmF0aW9uLlJ1bGUgJiZcbiAgICAgIHhtbE9iai5PYmplY3RMb2NrQ29uZmlndXJhdGlvbi5SdWxlLkRlZmF1bHRSZXRlbnRpb25cbiAgICApIHtcbiAgICAgIHJldGVudGlvblJlc3AgPSB4bWxPYmouT2JqZWN0TG9ja0NvbmZpZ3VyYXRpb24uUnVsZS5EZWZhdWx0UmV0ZW50aW9uIHx8IHt9XG4gICAgICBsb2NrQ29uZmlnUmVzdWx0Lm1vZGUgPSByZXRlbnRpb25SZXNwLk1vZGVcbiAgICB9XG4gICAgaWYgKHJldGVudGlvblJlc3ApIHtcbiAgICAgIGNvbnN0IGlzVW5pdFllYXJzID0gcmV0ZW50aW9uUmVzcC5ZZWFyc1xuICAgICAgaWYgKGlzVW5pdFllYXJzKSB7XG4gICAgICAgIGxvY2tDb25maWdSZXN1bHQudmFsaWRpdHkgPSBpc1VuaXRZZWFyc1xuICAgICAgICBsb2NrQ29uZmlnUmVzdWx0LnVuaXQgPSBSRVRFTlRJT05fVkFMSURJVFlfVU5JVFMuWUVBUlNcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIGxvY2tDb25maWdSZXN1bHQudmFsaWRpdHkgPSByZXRlbnRpb25SZXNwLkRheXNcbiAgICAgICAgbG9ja0NvbmZpZ1Jlc3VsdC51bml0ID0gUkVURU5USU9OX1ZBTElESVRZX1VOSVRTLkRBWVNcbiAgICAgIH1cbiAgICB9XG4gIH1cblxuICByZXR1cm4gbG9ja0NvbmZpZ1Jlc3VsdFxufVxuXG5leHBvcnQgZnVuY3Rpb24gcGFyc2VCdWNrZXRWZXJzaW9uaW5nQ29uZmlnKHhtbDogc3RyaW5nKSB7XG4gIGNvbnN0IHhtbE9iaiA9IHBhcnNlWG1sKHhtbClcbiAgcmV0dXJuIHhtbE9iai5WZXJzaW9uaW5nQ29uZmlndXJhdGlvblxufVxuXG4vLyBVc2VkIG9ubHkgaW4gc2VsZWN0T2JqZWN0Q29udGVudCBBUEkuXG4vLyBleHRyYWN0SGVhZGVyVHlwZSBleHRyYWN0cyB0aGUgZmlyc3QgaGFsZiBvZiB0aGUgaGVhZGVyIG1lc3NhZ2UsIHRoZSBoZWFkZXIgdHlwZS5cbmZ1bmN0aW9uIGV4dHJhY3RIZWFkZXJUeXBlKHN0cmVhbTogc3RyZWFtLlJlYWRhYmxlKTogc3RyaW5nIHwgdW5kZWZpbmVkIHtcbiAgY29uc3QgaGVhZGVyTmFtZUxlbiA9IEJ1ZmZlci5mcm9tKHN0cmVhbS5yZWFkKDEpKS5yZWFkVUludDgoKVxuICBjb25zdCBoZWFkZXJOYW1lV2l0aFNlcGFyYXRvciA9IEJ1ZmZlci5mcm9tKHN0cmVhbS5yZWFkKGhlYWRlck5hbWVMZW4pKS50b1N0cmluZygpXG4gIGNvbnN0IHNwbGl0QnlTZXBhcmF0b3IgPSAoaGVhZGVyTmFtZVdpdGhTZXBhcmF0b3IgfHwgJycpLnNwbGl0KCc6JylcbiAgcmV0dXJuIHNwbGl0QnlTZXBhcmF0b3IubGVuZ3RoID49IDEgPyBzcGxpdEJ5U2VwYXJhdG9yWzFdIDogJydcbn1cblxuZnVuY3Rpb24gZXh0cmFjdEhlYWRlclZhbHVlKHN0cmVhbTogc3RyZWFtLlJlYWRhYmxlKSB7XG4gIGNvbnN0IGJvZHlMZW4gPSBCdWZmZXIuZnJvbShzdHJlYW0ucmVhZCgyKSkucmVhZFVJbnQxNkJFKClcbiAgcmV0dXJuIEJ1ZmZlci5mcm9tKHN0cmVhbS5yZWFkKGJvZHlMZW4pKS50b1N0cmluZygpXG59XG5cbmV4cG9ydCBmdW5jdGlvbiBwYXJzZVNlbGVjdE9iamVjdENvbnRlbnRSZXNwb25zZShyZXM6IEJ1ZmZlcikge1xuICBjb25zdCBzZWxlY3RSZXN1bHRzID0gbmV3IFNlbGVjdFJlc3VsdHMoe30pIC8vIHdpbGwgYmUgcmV0dXJuZWRcblxuICBjb25zdCByZXNwb25zZVN0cmVhbSA9IHJlYWRhYmxlU3RyZWFtKHJlcykgLy8gY29udmVydCBieXRlIGFycmF5IHRvIGEgcmVhZGFibGUgcmVzcG9uc2VTdHJlYW1cbiAgLy8gQHRzLWlnbm9yZVxuICB3aGlsZSAocmVzcG9uc2VTdHJlYW0uX3JlYWRhYmxlU3RhdGUubGVuZ3RoKSB7XG4gICAgLy8gVG9wIGxldmVsIHJlc3BvbnNlU3RyZWFtIHJlYWQgdHJhY2tlci5cbiAgICBsZXQgbXNnQ3JjQWNjdW11bGF0b3IgLy8gYWNjdW11bGF0ZSBmcm9tIHN0YXJ0IG9mIHRoZSBtZXNzYWdlIHRpbGwgdGhlIG1lc3NhZ2UgY3JjIHN0YXJ0LlxuXG4gICAgY29uc3QgdG90YWxCeXRlTGVuZ3RoQnVmZmVyID0gQnVmZmVyLmZyb20ocmVzcG9uc2VTdHJlYW0ucmVhZCg0KSlcbiAgICBtc2dDcmNBY2N1bXVsYXRvciA9IGNyYzMyKHRvdGFsQnl0ZUxlbmd0aEJ1ZmZlcilcblxuICAgIGNvbnN0IGhlYWRlckJ5dGVzQnVmZmVyID0gQnVmZmVyLmZyb20ocmVzcG9uc2VTdHJlYW0ucmVhZCg0KSlcbiAgICBtc2dDcmNBY2N1bXVsYXRvciA9IGNyYzMyKGhlYWRlckJ5dGVzQnVmZmVyLCBtc2dDcmNBY2N1bXVsYXRvcilcblxuICAgIGNvbnN0IGNhbGN1bGF0ZWRQcmVsdWRlQ3JjID0gbXNnQ3JjQWNjdW11bGF0b3IucmVhZEludDMyQkUoKSAvLyB1c2UgaXQgdG8gY2hlY2sgaWYgYW55IENSQyBtaXNtYXRjaCBpbiBoZWFkZXIgaXRzZWxmLlxuXG4gICAgY29uc3QgcHJlbHVkZUNyY0J1ZmZlciA9IEJ1ZmZlci5mcm9tKHJlc3BvbnNlU3RyZWFtLnJlYWQoNCkpIC8vIHJlYWQgNCBieXRlcyAgICBpLmUgNCs0ID04ICsgNCA9IDEyICggcHJlbHVkZSArIHByZWx1ZGUgY3JjKVxuICAgIG1zZ0NyY0FjY3VtdWxhdG9yID0gY3JjMzIocHJlbHVkZUNyY0J1ZmZlciwgbXNnQ3JjQWNjdW11bGF0b3IpXG5cbiAgICBjb25zdCB0b3RhbE1zZ0xlbmd0aCA9IHRvdGFsQnl0ZUxlbmd0aEJ1ZmZlci5yZWFkSW50MzJCRSgpXG4gICAgY29uc3QgaGVhZGVyTGVuZ3RoID0gaGVhZGVyQnl0ZXNCdWZmZXIucmVhZEludDMyQkUoKVxuICAgIGNvbnN0IHByZWx1ZGVDcmNCeXRlVmFsdWUgPSBwcmVsdWRlQ3JjQnVmZmVyLnJlYWRJbnQzMkJFKClcblxuICAgIGlmIChwcmVsdWRlQ3JjQnl0ZVZhbHVlICE9PSBjYWxjdWxhdGVkUHJlbHVkZUNyYykge1xuICAgICAgLy8gSGFuZGxlIEhlYWRlciBDUkMgbWlzbWF0Y2ggRXJyb3JcbiAgICAgIHRocm93IG5ldyBFcnJvcihcbiAgICAgICAgYEhlYWRlciBDaGVja3N1bSBNaXNtYXRjaCwgUHJlbHVkZSBDUkMgb2YgJHtwcmVsdWRlQ3JjQnl0ZVZhbHVlfSBkb2VzIG5vdCBlcXVhbCBleHBlY3RlZCBDUkMgb2YgJHtjYWxjdWxhdGVkUHJlbHVkZUNyY31gLFxuICAgICAgKVxuICAgIH1cblxuICAgIGNvbnN0IGhlYWRlcnM6IFJlY29yZDxzdHJpbmcsIHVua25vd24+ID0ge31cbiAgICBpZiAoaGVhZGVyTGVuZ3RoID4gMCkge1xuICAgICAgY29uc3QgaGVhZGVyQnl0ZXMgPSBCdWZmZXIuZnJvbShyZXNwb25zZVN0cmVhbS5yZWFkKGhlYWRlckxlbmd0aCkpXG4gICAgICBtc2dDcmNBY2N1bXVsYXRvciA9IGNyYzMyKGhlYWRlckJ5dGVzLCBtc2dDcmNBY2N1bXVsYXRvcilcbiAgICAgIGNvbnN0IGhlYWRlclJlYWRlclN0cmVhbSA9IHJlYWRhYmxlU3RyZWFtKGhlYWRlckJ5dGVzKVxuICAgICAgLy8gQHRzLWlnbm9yZVxuICAgICAgd2hpbGUgKGhlYWRlclJlYWRlclN0cmVhbS5fcmVhZGFibGVTdGF0ZS5sZW5ndGgpIHtcbiAgICAgICAgY29uc3QgaGVhZGVyVHlwZU5hbWUgPSBleHRyYWN0SGVhZGVyVHlwZShoZWFkZXJSZWFkZXJTdHJlYW0pXG4gICAgICAgIGhlYWRlclJlYWRlclN0cmVhbS5yZWFkKDEpIC8vIGp1c3QgcmVhZCBhbmQgaWdub3JlIGl0LlxuICAgICAgICBpZiAoaGVhZGVyVHlwZU5hbWUpIHtcbiAgICAgICAgICBoZWFkZXJzW2hlYWRlclR5cGVOYW1lXSA9IGV4dHJhY3RIZWFkZXJWYWx1ZShoZWFkZXJSZWFkZXJTdHJlYW0pXG4gICAgICAgIH1cbiAgICAgIH1cbiAgICB9XG5cbiAgICBsZXQgcGF5bG9hZFN0cmVhbVxuICAgIGNvbnN0IHBheUxvYWRMZW5ndGggPSB0b3RhbE1zZ0xlbmd0aCAtIGhlYWRlckxlbmd0aCAtIDE2XG4gICAgaWYgKHBheUxvYWRMZW5ndGggPiAwKSB7XG4gICAgICBjb25zdCBwYXlMb2FkQnVmZmVyID0gQnVmZmVyLmZyb20ocmVzcG9uc2VTdHJlYW0ucmVhZChwYXlMb2FkTGVuZ3RoKSlcbiAgICAgIG1zZ0NyY0FjY3VtdWxhdG9yID0gY3JjMzIocGF5TG9hZEJ1ZmZlciwgbXNnQ3JjQWNjdW11bGF0b3IpXG4gICAgICAvLyByZWFkIHRoZSBjaGVja3N1bSBlYXJseSBhbmQgZGV0ZWN0IGFueSBtaXNtYXRjaCBzbyB3ZSBjYW4gYXZvaWQgdW5uZWNlc3NhcnkgZnVydGhlciBwcm9jZXNzaW5nLlxuICAgICAgY29uc3QgbWVzc2FnZUNyY0J5dGVWYWx1ZSA9IEJ1ZmZlci5mcm9tKHJlc3BvbnNlU3RyZWFtLnJlYWQoNCkpLnJlYWRJbnQzMkJFKClcbiAgICAgIGNvbnN0IGNhbGN1bGF0ZWRDcmMgPSBtc2dDcmNBY2N1bXVsYXRvci5yZWFkSW50MzJCRSgpXG4gICAgICAvLyBIYW5kbGUgbWVzc2FnZSBDUkMgRXJyb3JcbiAgICAgIGlmIChtZXNzYWdlQ3JjQnl0ZVZhbHVlICE9PSBjYWxjdWxhdGVkQ3JjKSB7XG4gICAgICAgIHRocm93IG5ldyBFcnJvcihcbiAgICAgICAgICBgTWVzc2FnZSBDaGVja3N1bSBNaXNtYXRjaCwgTWVzc2FnZSBDUkMgb2YgJHttZXNzYWdlQ3JjQnl0ZVZhbHVlfSBkb2VzIG5vdCBlcXVhbCBleHBlY3RlZCBDUkMgb2YgJHtjYWxjdWxhdGVkQ3JjfWAsXG4gICAgICAgIClcbiAgICAgIH1cbiAgICAgIHBheWxvYWRTdHJlYW0gPSByZWFkYWJsZVN0cmVhbShwYXlMb2FkQnVmZmVyKVxuICAgIH1cbiAgICBjb25zdCBtZXNzYWdlVHlwZSA9IGhlYWRlcnNbJ21lc3NhZ2UtdHlwZSddXG5cbiAgICBzd2l0Y2ggKG1lc3NhZ2VUeXBlKSB7XG4gICAgICBjYXNlICdlcnJvcic6IHtcbiAgICAgICAgY29uc3QgZXJyb3JNZXNzYWdlID0gaGVhZGVyc1snZXJyb3ItY29kZSddICsgJzpcIicgKyBoZWFkZXJzWydlcnJvci1tZXNzYWdlJ10gKyAnXCInXG4gICAgICAgIHRocm93IG5ldyBFcnJvcihlcnJvck1lc3NhZ2UpXG4gICAgICB9XG4gICAgICBjYXNlICdldmVudCc6IHtcbiAgICAgICAgY29uc3QgY29udGVudFR5cGUgPSBoZWFkZXJzWydjb250ZW50LXR5cGUnXVxuICAgICAgICBjb25zdCBldmVudFR5cGUgPSBoZWFkZXJzWydldmVudC10eXBlJ11cblxuICAgICAgICBzd2l0Y2ggKGV2ZW50VHlwZSkge1xuICAgICAgICAgIGNhc2UgJ0VuZCc6IHtcbiAgICAgICAgICAgIHNlbGVjdFJlc3VsdHMuc2V0UmVzcG9uc2UocmVzKVxuICAgICAgICAgICAgcmV0dXJuIHNlbGVjdFJlc3VsdHNcbiAgICAgICAgICB9XG5cbiAgICAgICAgICBjYXNlICdSZWNvcmRzJzoge1xuICAgICAgICAgICAgY29uc3QgcmVhZERhdGEgPSBwYXlsb2FkU3RyZWFtPy5yZWFkKHBheUxvYWRMZW5ndGgpXG4gICAgICAgICAgICBzZWxlY3RSZXN1bHRzLnNldFJlY29yZHMocmVhZERhdGEpXG4gICAgICAgICAgICBicmVha1xuICAgICAgICAgIH1cblxuICAgICAgICAgIGNhc2UgJ1Byb2dyZXNzJzpcbiAgICAgICAgICAgIHtcbiAgICAgICAgICAgICAgc3dpdGNoIChjb250ZW50VHlwZSkge1xuICAgICAgICAgICAgICAgIGNhc2UgJ3RleHQveG1sJzoge1xuICAgICAgICAgICAgICAgICAgY29uc3QgcHJvZ3Jlc3NEYXRhID0gcGF5bG9hZFN0cmVhbT8ucmVhZChwYXlMb2FkTGVuZ3RoKVxuICAgICAgICAgICAgICAgICAgc2VsZWN0UmVzdWx0cy5zZXRQcm9ncmVzcyhwcm9ncmVzc0RhdGEudG9TdHJpbmcoKSlcbiAgICAgICAgICAgICAgICAgIGJyZWFrXG4gICAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICAgIGRlZmF1bHQ6IHtcbiAgICAgICAgICAgICAgICAgIGNvbnN0IGVycm9yTWVzc2FnZSA9IGBVbmV4cGVjdGVkIGNvbnRlbnQtdHlwZSAke2NvbnRlbnRUeXBlfSBzZW50IGZvciBldmVudC10eXBlIFByb2dyZXNzYFxuICAgICAgICAgICAgICAgICAgdGhyb3cgbmV3IEVycm9yKGVycm9yTWVzc2FnZSlcbiAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgIH1cbiAgICAgICAgICAgIH1cbiAgICAgICAgICAgIGJyZWFrXG4gICAgICAgICAgY2FzZSAnU3RhdHMnOlxuICAgICAgICAgICAge1xuICAgICAgICAgICAgICBzd2l0Y2ggKGNvbnRlbnRUeXBlKSB7XG4gICAgICAgICAgICAgICAgY2FzZSAndGV4dC94bWwnOiB7XG4gICAgICAgICAgICAgICAgICBjb25zdCBzdGF0c0RhdGEgPSBwYXlsb2FkU3RyZWFtPy5yZWFkKHBheUxvYWRMZW5ndGgpXG4gICAgICAgICAgICAgICAgICBzZWxlY3RSZXN1bHRzLnNldFN0YXRzKHN0YXRzRGF0YS50b1N0cmluZygpKVxuICAgICAgICAgICAgICAgICAgYnJlYWtcbiAgICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgICAgZGVmYXVsdDoge1xuICAgICAgICAgICAgICAgICAgY29uc3QgZXJyb3JNZXNzYWdlID0gYFVuZXhwZWN0ZWQgY29udGVudC10eXBlICR7Y29udGVudFR5cGV9IHNlbnQgZm9yIGV2ZW50LXR5cGUgU3RhdHNgXG4gICAgICAgICAgICAgICAgICB0aHJvdyBuZXcgRXJyb3IoZXJyb3JNZXNzYWdlKVxuICAgICAgICAgICAgICAgIH1cbiAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgfVxuICAgICAgICAgICAgYnJlYWtcbiAgICAgICAgICBkZWZhdWx0OiB7XG4gICAgICAgICAgICAvLyBDb250aW51YXRpb24gbWVzc2FnZTogTm90IHN1cmUgaWYgaXQgaXMgc3VwcG9ydGVkLiBkaWQgbm90IGZpbmQgYSByZWZlcmVuY2Ugb3IgYW55IG1lc3NhZ2UgaW4gcmVzcG9uc2UuXG4gICAgICAgICAgICAvLyBJdCBkb2VzIG5vdCBoYXZlIGEgcGF5bG9hZC5cbiAgICAgICAgICAgIGNvbnN0IHdhcm5pbmdNZXNzYWdlID0gYFVuIGltcGxlbWVudGVkIGV2ZW50IGRldGVjdGVkICAke21lc3NhZ2VUeXBlfS5gXG4gICAgICAgICAgICAvLyBlc2xpbnQtZGlzYWJsZS1uZXh0LWxpbmUgbm8tY29uc29sZVxuICAgICAgICAgICAgY29uc29sZS53YXJuKHdhcm5pbmdNZXNzYWdlKVxuICAgICAgICAgIH1cbiAgICAgICAgfVxuICAgICAgfVxuICAgIH1cbiAgfVxufVxuXG5leHBvcnQgZnVuY3Rpb24gcGFyc2VMaWZlY3ljbGVDb25maWcoeG1sOiBzdHJpbmcpIHtcbiAgY29uc3QgeG1sT2JqID0gcGFyc2VYbWwoeG1sKVxuICBjb25zdCBsaWZlY3ljbGVDb25maWcgPSB4bWxPYmouTGlmZWN5Y2xlQ29uZmlndXJhdGlvblxuICBpZiAobGlmZWN5Y2xlQ29uZmlnICYmIGxpZmVjeWNsZUNvbmZpZy5SdWxlKSB7XG4gICAgLy8gQSBzaW5nbGUgPFJ1bGU+IGlzIHBhcnNlZCBhcyBhbiBvYmplY3Q7IG5vcm1hbGl6ZSB0byBhbiBhcnJheSBzbyB0aGVcbiAgICAvLyByZXN1bHQgbWF0Y2hlcyB0aGUgTGlmZWN5Y2xlQ29uZmlnIHR5cGUgYW5kIHRoZSBtdWx0aXBsZS1ydWxlcyBjYXNlLiBTZWUgIzE0MDcuXG4gICAgbGlmZWN5Y2xlQ29uZmlnLlJ1bGUgPSB0b0FycmF5KGxpZmVjeWNsZUNvbmZpZy5SdWxlKVxuICB9XG4gIHJldHVybiBsaWZlY3ljbGVDb25maWdcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHBhcnNlQnVja2V0RW5jcnlwdGlvbkNvbmZpZyh4bWw6IHN0cmluZykge1xuICByZXR1cm4gcGFyc2VYbWwoeG1sKVxufVxuXG5leHBvcnQgZnVuY3Rpb24gcGFyc2VPYmplY3RSZXRlbnRpb25Db25maWcoeG1sOiBzdHJpbmcpIHtcbiAgY29uc3QgeG1sT2JqID0gcGFyc2VYbWwoeG1sKVxuICBjb25zdCByZXRlbnRpb25Db25maWcgPSB4bWxPYmouUmV0ZW50aW9uXG4gIHJldHVybiB7XG4gICAgbW9kZTogcmV0ZW50aW9uQ29uZmlnLk1vZGUsXG4gICAgcmV0YWluVW50aWxEYXRlOiByZXRlbnRpb25Db25maWcuUmV0YWluVW50aWxEYXRlLFxuICB9XG59XG5cbmV4cG9ydCBmdW5jdGlvbiByZW1vdmVPYmplY3RzUGFyc2VyKHhtbDogc3RyaW5nKSB7XG4gIGNvbnN0IHhtbE9iaiA9IHBhcnNlWG1sKHhtbClcbiAgaWYgKHhtbE9iai5EZWxldGVSZXN1bHQgJiYgeG1sT2JqLkRlbGV0ZVJlc3VsdC5FcnJvcikge1xuICAgIC8vIHJldHVybiBlcnJvcnMgYXMgYXJyYXkgYWx3YXlzLiBhcyB0aGUgcmVzcG9uc2UgaXMgb2JqZWN0IGluIGNhc2Ugb2Ygc2luZ2xlIG9iamVjdCBwYXNzZWQgaW4gcmVtb3ZlT2JqZWN0c1xuICAgIHJldHVybiB0b0FycmF5KHhtbE9iai5EZWxldGVSZXN1bHQuRXJyb3IpXG4gIH1cbiAgcmV0dXJuIFtdXG59XG5cbi8vIHBhcnNlIFhNTCByZXNwb25zZSBmb3IgY29weSBvYmplY3RcbmV4cG9ydCBmdW5jdGlvbiBwYXJzZUNvcHlPYmplY3QoeG1sOiBzdHJpbmcpOiBDb3B5T2JqZWN0UmVzdWx0VjEge1xuICBjb25zdCByZXN1bHQ6IENvcHlPYmplY3RSZXN1bHRWMSA9IHtcbiAgICBldGFnOiAnJyxcbiAgICBsYXN0TW9kaWZpZWQ6ICcnLFxuICB9XG5cbiAgbGV0IHhtbG9iaiA9IHBhcnNlWG1sKHhtbClcbiAgaWYgKCF4bWxvYmouQ29weU9iamVjdFJlc3VsdCkge1xuICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZFhNTEVycm9yKCdNaXNzaW5nIHRhZzogXCJDb3B5T2JqZWN0UmVzdWx0XCInKVxuICB9XG4gIHhtbG9iaiA9IHhtbG9iai5Db3B5T2JqZWN0UmVzdWx0XG4gIGlmICh4bWxvYmouRVRhZykge1xuICAgIHJlc3VsdC5ldGFnID0geG1sb2JqLkVUYWcucmVwbGFjZSgvXlwiL2csICcnKVxuICAgICAgLnJlcGxhY2UoL1wiJC9nLCAnJylcbiAgICAgIC5yZXBsYWNlKC9eJnF1b3Q7L2csICcnKVxuICAgICAgLnJlcGxhY2UoLyZxdW90OyQvZywgJycpXG4gICAgICAucmVwbGFjZSgvXiYjMzQ7L2csICcnKVxuICAgICAgLnJlcGxhY2UoLyYjMzQ7JC9nLCAnJylcbiAgfVxuICBpZiAoeG1sb2JqLkxhc3RNb2RpZmllZCkge1xuICAgIHJlc3VsdC5sYXN0TW9kaWZpZWQgPSBuZXcgRGF0ZSh4bWxvYmouTGFzdE1vZGlmaWVkKVxuICB9XG5cbiAgcmV0dXJuIHJlc3VsdFxufVxuXG5jb25zdCBmb3JtYXRPYmpJbmZvID0gKGNvbnRlbnQ6IE9iamVjdFJvd0VudHJ5LCBvcHRzOiB7IElzRGVsZXRlTWFya2VyPzogYm9vbGVhbiB9ID0ge30pID0+IHtcbiAgY29uc3QgeyBLZXksIExhc3RNb2RpZmllZCwgRVRhZywgU2l6ZSwgVmVyc2lvbklkLCBJc0xhdGVzdCB9ID0gY29udGVudFxuXG4gIGlmICghaXNPYmplY3Qob3B0cykpIHtcbiAgICBvcHRzID0ge31cbiAgfVxuXG4gIGNvbnN0IG5hbWUgPSBzYW5pdGl6ZU9iamVjdEtleSh0b0FycmF5KEtleSlbMF0gfHwgJycpXG4gIGNvbnN0IGxhc3RNb2RpZmllZCA9IExhc3RNb2RpZmllZCA/IG5ldyBEYXRlKHRvQXJyYXkoTGFzdE1vZGlmaWVkKVswXSB8fCAnJykgOiB1bmRlZmluZWRcbiAgY29uc3QgZXRhZyA9IHNhbml0aXplRVRhZyh0b0FycmF5KEVUYWcpWzBdIHx8ICcnKVxuICBjb25zdCBzaXplID0gc2FuaXRpemVTaXplKFNpemUgfHwgJycpXG5cbiAgcmV0dXJuIHtcbiAgICBuYW1lLFxuICAgIGxhc3RNb2RpZmllZCxcbiAgICBldGFnLFxuICAgIHNpemUsXG4gICAgdmVyc2lvbklkOiBWZXJzaW9uSWQsXG4gICAgaXNMYXRlc3Q6IElzTGF0ZXN0LFxuICAgIGlzRGVsZXRlTWFya2VyOiBvcHRzLklzRGVsZXRlTWFya2VyID8gb3B0cy5Jc0RlbGV0ZU1hcmtlciA6IGZhbHNlLFxuICB9XG59XG5cbi8vIHBhcnNlIFhNTCByZXNwb25zZSBmb3IgbGlzdCBvYmplY3RzIGluIGEgYnVja2V0XG5leHBvcnQgZnVuY3Rpb24gcGFyc2VMaXN0T2JqZWN0cyh4bWw6IHN0cmluZykge1xuICBjb25zdCByZXN1bHQ6IHtcbiAgICBvYmplY3RzOiBPYmplY3RJbmZvW11cbiAgICBpc1RydW5jYXRlZD86IGJvb2xlYW5cbiAgICBuZXh0TWFya2VyPzogc3RyaW5nXG4gICAgdmVyc2lvbklkTWFya2VyPzogc3RyaW5nXG4gICAga2V5TWFya2VyPzogc3RyaW5nXG4gIH0gPSB7XG4gICAgb2JqZWN0czogW10sXG4gICAgaXNUcnVuY2F0ZWQ6IGZhbHNlLFxuICAgIG5leHRNYXJrZXI6IHVuZGVmaW5lZCxcbiAgICB2ZXJzaW9uSWRNYXJrZXI6IHVuZGVmaW5lZCxcbiAgICBrZXlNYXJrZXI6IHVuZGVmaW5lZCxcbiAgfVxuICBsZXQgaXNUcnVuY2F0ZWQgPSBmYWxzZVxuICBsZXQgbmV4dE1hcmtlclxuICBjb25zdCB4bWxvYmogPSBmeHBXaXRob3V0TnVtUGFyc2VyLnBhcnNlKHhtbClcblxuICBjb25zdCBwYXJzZUNvbW1vblByZWZpeGVzRW50aXR5ID0gKGNvbW1vblByZWZpeEVudHJ5OiBDb21tb25QcmVmaXhbXSkgPT4ge1xuICAgIGlmIChjb21tb25QcmVmaXhFbnRyeSkge1xuICAgICAgdG9BcnJheShjb21tb25QcmVmaXhFbnRyeSkuZm9yRWFjaCgoY29tbW9uUHJlZml4KSA9PiB7XG4gICAgICAgIHJlc3VsdC5vYmplY3RzLnB1c2goeyBwcmVmaXg6IHNhbml0aXplT2JqZWN0S2V5KHRvQXJyYXkoY29tbW9uUHJlZml4LlByZWZpeClbMF0gfHwgJycpLCBzaXplOiAwIH0pXG4gICAgICB9KVxuICAgIH1cbiAgfVxuXG4gIGNvbnN0IGxpc3RCdWNrZXRSZXN1bHQ6IExpc3RCdWNrZXRSZXN1bHRWMSA9IHhtbG9iai5MaXN0QnVja2V0UmVzdWx0XG4gIGNvbnN0IGxpc3RWZXJzaW9uc1Jlc3VsdDogTGlzdEJ1Y2tldFJlc3VsdFYxID0geG1sb2JqLkxpc3RWZXJzaW9uc1Jlc3VsdFxuXG4gIGlmIChsaXN0QnVja2V0UmVzdWx0KSB7XG4gICAgaWYgKGxpc3RCdWNrZXRSZXN1bHQuSXNUcnVuY2F0ZWQpIHtcbiAgICAgIGlzVHJ1bmNhdGVkID0gbGlzdEJ1Y2tldFJlc3VsdC5Jc1RydW5jYXRlZFxuICAgIH1cbiAgICBpZiAobGlzdEJ1Y2tldFJlc3VsdC5Db250ZW50cykge1xuICAgICAgdG9BcnJheShsaXN0QnVja2V0UmVzdWx0LkNvbnRlbnRzKS5mb3JFYWNoKChjb250ZW50KSA9PiB7XG4gICAgICAgIGNvbnN0IG5hbWUgPSBzYW5pdGl6ZU9iamVjdEtleSh0b0FycmF5KGNvbnRlbnQuS2V5KVswXSB8fCAnJylcbiAgICAgICAgY29uc3QgbGFzdE1vZGlmaWVkID0gbmV3IERhdGUodG9BcnJheShjb250ZW50Lkxhc3RNb2RpZmllZClbMF0gfHwgJycpXG4gICAgICAgIGNvbnN0IGV0YWcgPSBzYW5pdGl6ZUVUYWcodG9BcnJheShjb250ZW50LkVUYWcpWzBdIHx8ICcnKVxuICAgICAgICBjb25zdCBzaXplID0gc2FuaXRpemVTaXplKGNvbnRlbnQuU2l6ZSB8fCAnJylcbiAgICAgICAgcmVzdWx0Lm9iamVjdHMucHVzaCh7IG5hbWUsIGxhc3RNb2RpZmllZCwgZXRhZywgc2l6ZSB9KVxuICAgICAgfSlcbiAgICB9XG5cbiAgICBpZiAobGlzdEJ1Y2tldFJlc3VsdC5NYXJrZXIpIHtcbiAgICAgIG5leHRNYXJrZXIgPSBsaXN0QnVja2V0UmVzdWx0Lk1hcmtlclxuICAgIH1cbiAgICBpZiAobGlzdEJ1Y2tldFJlc3VsdC5OZXh0TWFya2VyKSB7XG4gICAgICBuZXh0TWFya2VyID0gbGlzdEJ1Y2tldFJlc3VsdC5OZXh0TWFya2VyXG4gICAgfSBlbHNlIGlmIChpc1RydW5jYXRlZCAmJiByZXN1bHQub2JqZWN0cy5sZW5ndGggPiAwKSB7XG4gICAgICBuZXh0TWFya2VyID0gcmVzdWx0Lm9iamVjdHNbcmVzdWx0Lm9iamVjdHMubGVuZ3RoIC0gMV0/Lm5hbWVcbiAgICB9XG4gICAgaWYgKGxpc3RCdWNrZXRSZXN1bHQuQ29tbW9uUHJlZml4ZXMpIHtcbiAgICAgIHBhcnNlQ29tbW9uUHJlZml4ZXNFbnRpdHkobGlzdEJ1Y2tldFJlc3VsdC5Db21tb25QcmVmaXhlcylcbiAgICB9XG4gIH1cblxuICBpZiAobGlzdFZlcnNpb25zUmVzdWx0KSB7XG4gICAgaWYgKGxpc3RWZXJzaW9uc1Jlc3VsdC5Jc1RydW5jYXRlZCkge1xuICAgICAgaXNUcnVuY2F0ZWQgPSBsaXN0VmVyc2lvbnNSZXN1bHQuSXNUcnVuY2F0ZWRcbiAgICB9XG5cbiAgICBpZiAobGlzdFZlcnNpb25zUmVzdWx0LlZlcnNpb24pIHtcbiAgICAgIHRvQXJyYXkobGlzdFZlcnNpb25zUmVzdWx0LlZlcnNpb24pLmZvckVhY2goKGNvbnRlbnQpID0+IHtcbiAgICAgICAgcmVzdWx0Lm9iamVjdHMucHVzaChmb3JtYXRPYmpJbmZvKGNvbnRlbnQpKVxuICAgICAgfSlcbiAgICB9XG4gICAgaWYgKGxpc3RWZXJzaW9uc1Jlc3VsdC5EZWxldGVNYXJrZXIpIHtcbiAgICAgIHRvQXJyYXkobGlzdFZlcnNpb25zUmVzdWx0LkRlbGV0ZU1hcmtlcikuZm9yRWFjaCgoY29udGVudCkgPT4ge1xuICAgICAgICByZXN1bHQub2JqZWN0cy5wdXNoKGZvcm1hdE9iakluZm8oY29udGVudCwgeyBJc0RlbGV0ZU1hcmtlcjogdHJ1ZSB9KSlcbiAgICAgIH0pXG4gICAgfVxuXG4gICAgaWYgKGxpc3RWZXJzaW9uc1Jlc3VsdC5OZXh0S2V5TWFya2VyKSB7XG4gICAgICByZXN1bHQua2V5TWFya2VyID0gbGlzdFZlcnNpb25zUmVzdWx0Lk5leHRLZXlNYXJrZXJcbiAgICB9XG4gICAgaWYgKGxpc3RWZXJzaW9uc1Jlc3VsdC5OZXh0VmVyc2lvbklkTWFya2VyKSB7XG4gICAgICByZXN1bHQudmVyc2lvbklkTWFya2VyID0gbGlzdFZlcnNpb25zUmVzdWx0Lk5leHRWZXJzaW9uSWRNYXJrZXJcbiAgICB9XG4gICAgaWYgKGxpc3RWZXJzaW9uc1Jlc3VsdC5Db21tb25QcmVmaXhlcykge1xuICAgICAgcGFyc2VDb21tb25QcmVmaXhlc0VudGl0eShsaXN0VmVyc2lvbnNSZXN1bHQuQ29tbW9uUHJlZml4ZXMpXG4gICAgfVxuICB9XG5cbiAgcmVzdWx0LmlzVHJ1bmNhdGVkID0gaXNUcnVuY2F0ZWRcbiAgaWYgKGlzVHJ1bmNhdGVkKSB7XG4gICAgcmVzdWx0Lm5leHRNYXJrZXIgPSBuZXh0TWFya2VyXG4gIH1cbiAgcmV0dXJuIHJlc3VsdFxufVxuXG5leHBvcnQgZnVuY3Rpb24gdXBsb2FkUGFydFBhcnNlcih4bWw6IHN0cmluZykge1xuICBjb25zdCB4bWxPYmogPSBwYXJzZVhtbCh4bWwpXG4gIGNvbnN0IHJlc3BFbCA9IHhtbE9iai5Db3B5UGFydFJlc3VsdFxuICByZXR1cm4gcmVzcEVsXG59XG4iXSwibWFwcGluZ3MiOiJBQUdBLE9BQU9BLEtBQUssTUFBTSxjQUFj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