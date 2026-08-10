import * as crypto from "crypto";
import * as fs from "fs";
import * as http from "http";
import * as https from "https";
import * as path from "path";
import * as stream from "stream";
import * as async from 'async';
import BlockStream2 from 'block-stream2';
import { isBrowser } from 'browser-or-node';
import _ from 'lodash';
import qs from 'query-string';
import xml2js from 'xml2js';
import { CredentialProvider } from "../CredentialProvider.mjs";
import * as errors from "../errors.mjs";
import { CopyDestinationOptions, CopySourceOptions, DEFAULT_REGION, LEGAL_HOLD_STATUS, PRESIGN_EXPIRY_DAYS_MAX, RETENTION_MODES, RETENTION_VALIDITY_UNITS } from "../helpers.mjs";
import { NotificationConfig, NotificationPoller } from "../notification.mjs";
import { postPresignSignatureV4, presignSignatureV4, signV4 } from "../signing.mjs";
import { fsp, streamPromise } from "./async.mjs";
import { CopyConditions } from "./copy-conditions.mjs";
import { Extensions } from "./extensions.mjs";
import { calculateEvenSplits, extractMetadata, getContentLength, getScope, getSourceVersionId, getVersionId, hashBinary, insertContentType, isAmazonEndpoint, isBoolean, isDefined, isEmpty, isNumber, isObject, isPlainObject, isReadableStream, isString, isValidBucketName, isValidEndpoint, isValidObjectName, isValidPort, isValidPrefix, isVirtualHostStyle, makeDateLong, PART_CONSTRAINTS, partsRequired, prependXAMZMeta, readableStream, sanitizeETag, toMd5, toSha256, uriEscape, uriResourceEscape } from "./helper.mjs";
import { joinHostPort } from "./join-host-port.mjs";
import { PostPolicy } from "./post-policy.mjs";
import { requestWithRetry } from "./request.mjs";
import { drainResponse, readAsBuffer, readAsString } from "./response.mjs";
import { getS3Endpoint } from "./s3-endpoints.mjs";
import { parseBucketNotification, parseCompleteMultipart, parseInitiateMultipart, parseListObjects, parseListObjectsV2, parseObjectLegalHoldConfig, parseSelectObjectContentResponse, uploadPartParser } from "./xml-parser.mjs";
import * as xmlParsers from "./xml-parser.mjs";
const xml = new xml2js.Builder({
  renderOpts: {
    pretty: false
  },
  headless: true
});

// will be replaced by bundler.
const Package = {
  version: "8.0.8" || 'development'
};
const requestOptionProperties = ['agent', 'ca', 'cert', 'ciphers', 'clientCertEngine', 'crl', 'dhparam', 'ecdhCurve', 'family', 'honorCipherOrder', 'key', 'passphrase', 'pfx', 'rejectUnauthorized', 'secureOptions', 'secureProtocol', 'servername', 'sessionIdContext'];
export class TypedClient {
  partSize = 64 * 1024 * 1024;
  maximumPartSize = 5 * 1024 * 1024 * 1024;
  maxObjectSize = 5 * 1024 * 1024 * 1024 * 1024;
  constructor(params) {
    // @ts-expect-error deprecated property
    if (params.secure !== undefined) {
      throw new Error('"secure" option deprecated, "useSSL" should be used instead');
    }
    // Default values if not specified.
    if (params.useSSL === undefined) {
      params.useSSL = true;
    }
    if (!params.port) {
      params.port = 0;
    }
    // Validate input params.
    if (!isValidEndpoint(params.endPoint)) {
      throw new errors.InvalidEndpointError(`Invalid endPoint : ${params.endPoint}`);
    }
    if (!isValidPort(params.port)) {
      throw new errors.InvalidArgumentError(`Invalid port : ${params.port}`);
    }
    if (!isBoolean(params.useSSL)) {
      throw new errors.InvalidArgumentError(`Invalid useSSL flag type : ${params.useSSL}, expected to be of type "boolean"`);
    }

    // Validate region only if its set.
    if (params.region) {
      if (!isString(params.region)) {
        throw new errors.InvalidArgumentError(`Invalid region : ${params.region}`);
      }
    }
    const host = params.endPoint.toLowerCase();
    let port = params.port;
    let protocol;
    let transport;
    let transportAgent;
    // Validate if configuration is not using SSL
    // for constructing relevant endpoints.
    if (params.useSSL) {
      // Defaults to secure.
      transport = https;
      protocol = 'https:';
      port = port || 443;
      transportAgent = https.globalAgent;
    } else {
      transport = http;
      protocol = 'http:';
      port = port || 80;
      transportAgent = http.globalAgent;
    }

    // if custom transport is set, use it.
    if (params.transport) {
      if (!isObject(params.transport)) {
        throw new errors.InvalidArgumentError(`Invalid transport type : ${params.transport}, expected to be type "object"`);
      }
      transport = params.transport;
    }

    // if custom transport agent is set, use it.
    if (params.transportAgent) {
      if (!isObject(params.transportAgent)) {
        throw new errors.InvalidArgumentError(`Invalid transportAgent type: ${params.transportAgent}, expected to be type "object"`);
      }
      transportAgent = params.transportAgent;
    }

    // User Agent should always following the below style.
    // Please open an issue to discuss any new changes here.
    //
    //       MinIO (OS; ARCH) LIB/VER APP/VER
    //
    const libraryComments = `(${process.platform}; ${process.arch})`;
    const libraryAgent = `MinIO ${libraryComments} minio-js/${Package.version}`;
    // User agent block ends.

    this.transport = transport;
    this.transportAgent = transportAgent;
    this.host = host;
    this.port = port;
    this.protocol = protocol;
    this.userAgent = `${libraryAgent}`;

    // Default path style is true
    if (params.pathStyle === undefined) {
      this.pathStyle = true;
    } else {
      this.pathStyle = params.pathStyle;
    }
    this.accessKey = params.accessKey ?? '';
    this.secretKey = params.secretKey ?? '';
    this.sessionToken = params.sessionToken;
    this.anonymous = !this.accessKey || !this.secretKey;
    if (params.credentialsProvider) {
      this.anonymous = false;
      this.credentialsProvider = params.credentialsProvider;
    }
    this.regionMap = {};
    if (params.region) {
      this.region = params.region;
    }
    if (params.partSize) {
      this.partSize = params.partSize;
      this.overRidePartSize = true;
    }
    if (this.partSize < 5 * 1024 * 1024) {
      throw new errors.InvalidArgumentError(`Part size should be greater than 5MB`);
    }
    if (this.partSize > 5 * 1024 * 1024 * 1024) {
      throw new errors.InvalidArgumentError(`Part size should be less than 5GB`);
    }

    // SHA256 is enabled only for authenticated http requests. If the request is authenticated
    // and the connection is https we use x-amz-content-sha256=UNSIGNED-PAYLOAD
    // header for signature calculation.
    this.enableSHA256 = !this.anonymous && !params.useSSL;
    this.s3AccelerateEndpoint = params.s3AccelerateEndpoint || undefined;
    this.reqOptions = {};
    this.clientExtensions = new Extensions(this);
    if (params.retryOptions) {
      if (!isObject(params.retryOptions)) {
        throw new errors.InvalidArgumentError(`Invalid retryOptions type: ${params.retryOptions}, expected to be type "object"`);
      }
      this.retryOptions = params.retryOptions;
    } else {
      this.retryOptions = {
        disableRetry: false
      };
    }
  }
  /**
   * Minio extensions that aren't necessary present for Amazon S3 compatible storage servers
   */
  get extensions() {
    return this.clientExtensions;
  }

  /**
   * @param endPoint - valid S3 acceleration end point
   */
  setS3TransferAccelerate(endPoint) {
    this.s3AccelerateEndpoint = endPoint;
  }

  /**
   * Sets the supported request options.
   */
  setRequestOptions(options) {
    if (!isObject(options)) {
      throw new TypeError('request options should be of type "object"');
    }
    this.reqOptions = _.pick(options, requestOptionProperties);
  }

  /**
   *  This is s3 Specific and does not hold validity in any other Object storage.
   */
  getAccelerateEndPointIfSet(bucketName, objectName) {
    if (!isEmpty(this.s3AccelerateEndpoint) && !isEmpty(bucketName) && !isEmpty(objectName)) {
      // http://docs.aws.amazon.com/AmazonS3/latest/dev/transfer-acceleration.html
      // Disable transfer acceleration for non-compliant bucket names.
      if (bucketName.includes('.')) {
        throw new Error(`Transfer Acceleration is not supported for non compliant bucket:${bucketName}`);
      }
      // If transfer acceleration is requested set new host.
      // For more details about enabling transfer acceleration read here.
      // http://docs.aws.amazon.com/AmazonS3/latest/dev/transfer-acceleration.html
      return this.s3AccelerateEndpoint;
    }
    return false;
  }

  /**
   *   Set application specific information.
   *   Generates User-Agent in the following style.
   *   MinIO (OS; ARCH) LIB/VER APP/VER
   */
  setAppInfo(appName, appVersion) {
    if (!isString(appName)) {
      throw new TypeError(`Invalid appName: ${appName}`);
    }
    if (appName.trim() === '') {
      throw new errors.InvalidArgumentError('Input appName cannot be empty.');
    }
    if (!isString(appVersion)) {
      throw new TypeError(`Invalid appVersion: ${appVersion}`);
    }
    if (appVersion.trim() === '') {
      throw new errors.InvalidArgumentError('Input appVersion cannot be empty.');
    }
    this.userAgent = `${this.userAgent} ${appName}/${appVersion}`;
  }

  /**
   * returns options object that can be used with http.request()
   * Takes care of constructing virtual-host-style or path-style hostname
   */
  getRequestOptions(opts) {
    const method = opts.method;
    const region = opts.region;
    const bucketName = opts.bucketName;
    let objectName = opts.objectName;
    const headers = opts.headers;
    const query = opts.query;
    let reqOptions = {
      method,
      headers: {},
      protocol: this.protocol,
      // If custom transportAgent was supplied earlier, we'll inject it here
      agent: this.transportAgent
    };

    // Verify if virtual host supported.
    let virtualHostStyle;
    if (bucketName) {
      virtualHostStyle = isVirtualHostStyle(this.host, this.protocol, bucketName, this.pathStyle);
    }
    let path = '/';
    let host = this.host;
    let port;
    if (this.port) {
      port = this.port;
    }
    if (objectName) {
      objectName = uriResourceEscape(objectName);
    }

    // For Amazon S3 endpoint, get endpoint based on region.
    if (isAmazonEndpoint(host)) {
      const accelerateEndPoint = this.getAccelerateEndPointIfSet(bucketName, objectName);
      if (accelerateEndPoint) {
        host = `${accelerateEndPoint}`;
      } else {
        host = getS3Endpoint(region);
      }
    }
    if (virtualHostStyle && !opts.pathStyle) {
      // For all hosts which support virtual host style, `bucketName`
      // is part of the hostname in the following format:
      //
      //  var host = 'bucketName.example.com'
      //
      if (bucketName) {
        host = `${bucketName}.${host}`;
      }
      if (objectName) {
        path = `/${objectName}`;
      }
    } else {
      // For all S3 compatible storage services we will fallback to
      // path style requests, where `bucketName` is part of the URI
      // path.
      if (bucketName) {
        path = `/${bucketName}`;
      }
      if (objectName) {
        path = `/${bucketName}/${objectName}`;
      }
    }
    if (query) {
      path += `?${query}`;
    }
    reqOptions.headers.host = host;
    if (reqOptions.protocol === 'http:' && port !== 80 || reqOptions.protocol === 'https:' && port !== 443) {
      reqOptions.headers.host = joinHostPort(host, port);
    }
    reqOptions.headers['user-agent'] = this.userAgent;
    if (headers) {
      // have all header keys in lower case - to make signing easy
      for (const [k, v] of Object.entries(headers)) {
        reqOptions.headers[k.toLowerCase()] = v;
      }
    }

    // Use any request option specified in minioClient.setRequestOptions()
    reqOptions = Object.assign({}, this.reqOptions, reqOptions);
    return {
      ...reqOptions,
      headers: _.mapValues(_.pickBy(reqOptions.headers, isDefined), v => v.toString()),
      host,
      port,
      path
    };
  }
  async setCredentialsProvider(credentialsProvider) {
    if (!(credentialsProvider instanceof CredentialProvider)) {
      throw new Error('Unable to get credentials. Expected instance of CredentialProvider');
    }
    this.credentialsProvider = credentialsProvider;
    await this.checkAndRefreshCreds();
  }
  async checkAndRefreshCreds() {
    if (this.credentialsProvider) {
      try {
        const credentialsConf = await this.credentialsProvider.getCredentials();
        this.accessKey = credentialsConf.getAccessKey();
        this.secretKey = credentialsConf.getSecretKey();
        this.sessionToken = credentialsConf.getSessionToken();
      } catch (e) {
        throw new Error(`Unable to get credentials: ${e}`, {
          cause: e
        });
      }
    }
  }
  /**
   * log the request, response, error
   */
  logHTTP(reqOptions, response, err) {
    // if no logStream available return.
    if (!this.logStream) {
      return;
    }
    if (!isObject(reqOptions)) {
      throw new TypeError('reqOptions should be of type "object"');
    }
    if (response && !isReadableStream(response)) {
      throw new TypeError('response should be of type "Stream"');
    }
    if (err && !(err instanceof Error)) {
      throw new TypeError('err should be of type "Error"');
    }
    const logStream = this.logStream;
    const logHeaders = headers => {
      Object.entries(headers).forEach(([k, v]) => {
        if (k == 'authorization') {
          if (isString(v)) {
            const redactor = new RegExp('Signature=([0-9a-f]+)');
            v = v.replace(redactor, 'Signature=**REDACTED**');
          }
        }
        logStream.write(`${k}: ${v}\n`);
      });
      logStream.write('\n');
    };
    logStream.write(`REQUEST: ${reqOptions.method} ${reqOptions.path}\n`);
    logHeaders(reqOptions.headers);
    if (response) {
      this.logStream.write(`RESPONSE: ${response.statusCode}\n`);
      logHeaders(response.headers);
    }
    if (err) {
      logStream.write('ERROR BODY:\n');
      const errJSON = JSON.stringify(err, null, '\t');
      logStream.write(`${errJSON}\n`);
    }
  }

  /**
   * Enable tracing
   */
  traceOn(stream) {
    if (!stream) {
      stream = process.stdout;
    }
    this.logStream = stream;
  }

  /**
   * Disable tracing
   */
  traceOff() {
    this.logStream = undefined;
  }

  /**
   * makeRequest is the primitive used by the apis for making S3 requests.
   * payload can be empty string in case of no payload.
   * statusCode is the expected statusCode. If response.statusCode does not match
   * we parse the XML error and call the callback with the error message.
   *
   * A valid region is passed by the calls - listBuckets, makeBucket and getBucketRegion.
   *
   * @internal
   */
  async makeRequestAsync(options, payload = '', expectedCodes = [200], region = '') {
    if (!isObject(options)) {
      throw new TypeError('options should be of type "object"');
    }
    if (!isString(payload) && !isObject(payload)) {
      // Buffer is of type 'object'
      throw new TypeError('payload should be of type "string" or "Buffer"');
    }
    expectedCodes.forEach(statusCode => {
      if (!isNumber(statusCode)) {
        throw new TypeError('statusCode should be of type "number"');
      }
    });
    if (!isString(region)) {
      throw new TypeError('region should be of type "string"');
    }
    if (!options.headers) {
      options.headers = {};
    }
    if (options.method === 'POST' || options.method === 'PUT' || options.method === 'DELETE') {
      options.headers['content-length'] = payload.length.toString();
    }
    const sha256sum = this.enableSHA256 ? toSha256(payload) : '';
    return this.makeRequestStreamAsync(options, payload, sha256sum, expectedCodes, region);
  }

  /**
   * new request with promise
   *
   * No need to drain response, response body is not valid
   */
  async makeRequestAsyncOmit(options, payload = '', statusCodes = [200], region = '') {
    const res = await this.makeRequestAsync(options, payload, statusCodes, region);
    await drainResponse(res);
    return res;
  }

  /**
   * makeRequestStream will be used directly instead of makeRequest in case the payload
   * is available as a stream. for ex. putObject
   *
   * @internal
   */
  async makeRequestStreamAsync(options, body, sha256sum, statusCodes, region) {
    if (!isObject(options)) {
      throw new TypeError('options should be of type "object"');
    }
    if (!(Buffer.isBuffer(body) || typeof body === 'string' || isReadableStream(body))) {
      throw new errors.InvalidArgumentError(`stream should be a Buffer, string or readable Stream, got ${typeof body} instead`);
    }
    if (!isString(sha256sum)) {
      throw new TypeError('sha256sum should be of type "string"');
    }
    statusCodes.forEach(statusCode => {
      if (!isNumber(statusCode)) {
        throw new TypeError('statusCode should be of type "number"');
      }
    });
    if (!isString(region)) {
      throw new TypeError('region should be of type "string"');
    }
    // sha256sum will be empty for anonymous or https requests
    if (!this.enableSHA256 && sha256sum.length !== 0) {
      throw new errors.InvalidArgumentError(`sha256sum expected to be empty for anonymous or https requests`);
    }
    // sha256sum should be valid for non-anonymous http requests.
    if (this.enableSHA256 && sha256sum.length !== 64) {
      throw new errors.InvalidArgumentError(`Invalid sha256sum : ${sha256sum}`);
    }
    await this.checkAndRefreshCreds();

    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    region = region || (await this.getBucketRegionAsync(options.bucketName));
    const reqOptions = this.getRequestOptions({
      ...options,
      region
    });
    if (!this.anonymous) {
      // For non-anonymous https requests sha256sum is 'UNSIGNED-PAYLOAD' for signature calculation.
      if (!this.enableSHA256) {
        sha256sum = 'UNSIGNED-PAYLOAD';
      }
      const date = new Date();
      reqOptions.headers['x-amz-date'] = makeDateLong(date);
      reqOptions.headers['x-amz-content-sha256'] = sha256sum;
      if (this.sessionToken) {
        reqOptions.headers['x-amz-security-token'] = this.sessionToken;
      }
      reqOptions.headers.authorization = signV4(reqOptions, this.accessKey, this.secretKey, region, date, sha256sum);
    }
    const logStream = this.logStream;
    const log = logStream ? msg => logStream.write(`${msg}\n`) : () => {};
    const response = await requestWithRetry(this.transport, reqOptions, body, this.retryOptions.disableRetry === true ? 0 : this.retryOptions.maximumRetryCount, this.retryOptions.baseDelayMs, this.retryOptions.maximumDelayMs, log);
    if (!response.statusCode) {
      throw new Error("BUG: response doesn't have a statusCode");
    }
    if (!statusCodes.includes(response.statusCode)) {
      // For an incorrect region, S3 server always sends back 400.
      // But we will do cache invalidation for all errors so that,
      // in future, if AWS S3 decides to send a different status code or
      // XML error code we will still work fine.
      // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
      delete this.regionMap[options.bucketName];
      const err = await xmlParsers.parseResponseError(response);
      this.logHTTP(reqOptions, response, err);
      throw err;
    }
    this.logHTTP(reqOptions, response);
    return response;
  }

  /**
   * gets the region of the bucket
   *
   * @param bucketName
   *
   */
  async getBucketRegionAsync(bucketName) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError(`Invalid bucket name : ${bucketName}`);
    }

    // Region is set with constructor, return the region right here.
    if (this.region) {
      return this.region;
    }
    const cached = this.regionMap[bucketName];
    if (cached) {
      return cached;
    }
    const extractRegionAsync = async response => {
      const body = await readAsString(response);
      const region = xmlParsers.parseBucketRegion(body) || DEFAULT_REGION;
      this.regionMap[bucketName] = region;
      return region;
    };
    const method = 'GET';
    const query = 'location';
    // `getBucketLocation` behaves differently in following ways for
    // different environments.
    //
    // - For nodejs env we default to path style requests.
    // - For browser env path style requests on buckets yields CORS
    //   error. To circumvent this problem we make a virtual host
    //   style request signed with 'us-east-1'. This request fails
    //   with an error 'AuthorizationHeaderMalformed', additionally
    //   the error XML also provides Region of the bucket. To validate
    //   this region is proper we retry the same request with the newly
    //   obtained region.
    const pathStyle = this.pathStyle && !isBrowser;
    let region;
    try {
      const res = await this.makeRequestAsync({
        method,
        bucketName,
        query,
        pathStyle
      }, '', [200], DEFAULT_REGION);
      return extractRegionAsync(res);
    } catch (e) {
      // make alignment with mc cli
      if (e instanceof errors.S3Error) {
        const errCode = e.code;
        const errRegion = e.region;
        if (errCode === 'AccessDenied' && !errRegion) {
          return DEFAULT_REGION;
        }
      }
      // eslint-disable-next-line @typescript-eslint/ban-ts-comment
      // @ts-ignore
      if (!(e.name === 'AuthorizationHeaderMalformed')) {
        throw e;
      }
      // @ts-expect-error we set extra properties on error object
      region = e.Region;
      if (!region) {
        throw e;
      }
    }
    const res = await this.makeRequestAsync({
      method,
      bucketName,
      query,
      pathStyle
    }, '', [200], region);
    return await extractRegionAsync(res);
  }

  /**
   * makeRequest is the primitive used by the apis for making S3 requests.
   * payload can be empty string in case of no payload.
   * statusCode is the expected statusCode. If response.statusCode does not match
   * we parse the XML error and call the callback with the error message.
   * A valid region is passed by the calls - listBuckets, makeBucket and
   * getBucketRegion.
   *
   * @deprecated use `makeRequestAsync` instead
   */
  makeRequest(options, payload = '', expectedCodes = [200], region = '', returnResponse, cb) {
    let prom;
    if (returnResponse) {
      prom = this.makeRequestAsync(options, payload, expectedCodes, region);
    } else {
      // eslint-disable-next-line @typescript-eslint/ban-ts-comment
      // @ts-expect-error compatible for old behaviour
      prom = this.makeRequestAsyncOmit(options, payload, expectedCodes, region);
    }
    prom.then(result => cb(null, result), err => {
      // eslint-disable-next-line @typescript-eslint/ban-ts-comment
      // @ts-ignore
      cb(err);
    });
  }

  /**
   * makeRequestStream will be used directly instead of makeRequest in case the payload
   * is available as a stream. for ex. putObject
   *
   * @deprecated use `makeRequestStreamAsync` instead
   */
  makeRequestStream(options, stream, sha256sum, statusCodes, region, returnResponse, cb) {
    const executor = async () => {
      const res = await this.makeRequestStreamAsync(options, stream, sha256sum, statusCodes, region);
      if (!returnResponse) {
        await drainResponse(res);
      }
      return res;
    };
    executor().then(result => cb(null, result),
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore
    err => cb(err));
  }

  /**
   * @deprecated use `getBucketRegionAsync` instead
   */
  getBucketRegion(bucketName, cb) {
    return this.getBucketRegionAsync(bucketName).then(result => cb(null, result),
    // eslint-disable-next-line @typescript-eslint/ban-ts-comment
    // @ts-ignore
    err => cb(err));
  }

  // Bucket operations

  /**
   * Creates the bucket `bucketName`.
   *
   */
  async makeBucket(bucketName, region = '', makeOpts) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    // Backward Compatibility
    if (isObject(region)) {
      makeOpts = region;
      region = '';
    }
    if (!isString(region)) {
      throw new TypeError('region should be of type "string"');
    }
    if (makeOpts && !isObject(makeOpts)) {
      throw new TypeError('makeOpts should be of type "object"');
    }
    let payload = '';

    // Region already set in constructor, validate if
    // caller requested bucket location is same.
    if (region && this.region) {
      if (region !== this.region) {
        throw new errors.InvalidArgumentError(`Configured region ${this.region}, requested ${region}`);
      }
    }
    // sending makeBucket request with XML containing 'us-east-1' fails. For
    // default region server expects the request without body
    if (region && region !== DEFAULT_REGION) {
      payload = xml.buildObject({
        CreateBucketConfiguration: {
          $: {
            xmlns: 'http://s3.amazonaws.com/doc/2006-03-01/'
          },
          LocationConstraint: region
        }
      });
    }
    const method = 'PUT';
    const headers = {};
    if (makeOpts && makeOpts.ObjectLocking) {
      headers['x-amz-bucket-object-lock-enabled'] = true;
    }

    // For custom region clients  default to custom region specified in client constructor
    const finalRegion = this.region || region || DEFAULT_REGION;
    const requestOpt = {
      method,
      bucketName,
      headers
    };
    try {
      await this.makeRequestAsyncOmit(requestOpt, payload, [200], finalRegion);
    } catch (err) {
      if (region === '' || region === DEFAULT_REGION) {
        if (err instanceof errors.S3Error) {
          const errCode = err.code;
          const errRegion = err.region;
          if (errCode === 'AuthorizationHeaderMalformed' && errRegion !== '') {
            // Retry with region returned as part of error
            await this.makeRequestAsyncOmit(requestOpt, payload, [200], errCode);
          }
        }
      }
      throw err;
    }
  }

  /**
   * To check if a bucket already exists.
   */
  async bucketExists(bucketName) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    const method = 'HEAD';
    try {
      await this.makeRequestAsyncOmit({
        method,
        bucketName
      });
    } catch (err) {
      // @ts-ignore
      if (err.code === 'NoSuchBucket' || err.code === 'NotFound') {
        return false;
      }
      throw err;
    }
    return true;
  }

  /**
   * @deprecated use promise style API
   */

  async removeBucket(bucketName) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    const method = 'DELETE';
    await this.makeRequestAsyncOmit({
      method,
      bucketName
    }, '', [204]);
    delete this.regionMap[bucketName];
  }

  /**
   * Callback is called with readable stream of the object content.
   */
  async getObject(bucketName, objectName, getOpts) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!isValidObjectName(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`);
    }
    return this.getPartialObject(bucketName, objectName, 0, 0, getOpts);
  }

  /**
   * Callback is called with readable stream of the partial object content.
   * @param bucketName
   * @param objectName
   * @param offset
   * @param length - length of the object that will be read in the stream (optional, if not specified we read the rest of the file from the offset)
   * @param getOpts
   */
  async getPartialObject(bucketName, objectName, offset, length = 0, getOpts) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!isValidObjectName(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`);
    }
    if (!isNumber(offset)) {
      throw new TypeError('offset should be of type "number"');
    }
    if (!isNumber(length)) {
      throw new TypeError('length should be of type "number"');
    }
    let range = '';
    if (offset || length) {
      if (offset) {
        range = `bytes=${+offset}-`;
      } else {
        range = 'bytes=0-';
        offset = 0;
      }
      if (length) {
        range += `${+length + offset - 1}`;
      }
    }
    let query = '';
    let headers = {
      ...(range !== '' && {
        range
      })
    };
    if (getOpts) {
      const sseHeaders = {
        ...(getOpts.SSECustomerAlgorithm && {
          'X-Amz-Server-Side-Encryption-Customer-Algorithm': getOpts.SSECustomerAlgorithm
        }),
        ...(getOpts.SSECustomerKey && {
          'X-Amz-Server-Side-Encryption-Customer-Key': getOpts.SSECustomerKey
        }),
        ...(getOpts.SSECustomerKeyMD5 && {
          'X-Amz-Server-Side-Encryption-Customer-Key-MD5': getOpts.SSECustomerKeyMD5
        })
      };
      query = qs.stringify(getOpts);
      headers = {
        ...prependXAMZMeta(sseHeaders),
        ...headers
      };
    }
    const expectedStatusCodes = [200];
    if (range) {
      expectedStatusCodes.push(206);
    }
    const method = 'GET';
    return await this.makeRequestAsync({
      method,
      bucketName,
      objectName,
      headers,
      query
    }, '', expectedStatusCodes);
  }

  /**
   * download object content to a file.
   * This method will create a temp file named `${filename}.${base64(etag)}.part.minio` when downloading.
   *
   * @param bucketName - name of the bucket
   * @param objectName - name of the object
   * @param filePath - path to which the object data will be written to
   * @param getOpts - Optional object get option
   */
  async fGetObject(bucketName, objectName, filePath, getOpts) {
    // Input validation.
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!isValidObjectName(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`);
    }
    if (!isString(filePath)) {
      throw new TypeError('filePath should be of type "string"');
    }
    const downloadToTmpFile = async () => {
      let partFileStream;
      const objStat = await this.statObject(bucketName, objectName, getOpts);
      const encodedEtag = Buffer.from(objStat.etag).toString('base64');
      const partFile = `${filePath}.${encodedEtag}.part.minio`;
      await fsp.mkdir(path.dirname(filePath), {
        recursive: true
      });
      let offset = 0;
      try {
        const stats = await fsp.stat(partFile);
        if (objStat.size === stats.size) {
          return partFile;
        }
        offset = stats.size;
        partFileStream = fs.createWriteStream(partFile, {
          flags: 'a'
        });
      } catch (e) {
        if (e instanceof Error && e.code === 'ENOENT') {
          // file not exist
          partFileStream = fs.createWriteStream(partFile, {
            flags: 'w'
          });
        } else {
          // other error, maybe access deny
          throw e;
        }
      }
      const downloadStream = await this.getPartialObject(bucketName, objectName, offset, 0, getOpts);
      await streamPromise.pipeline(downloadStream, partFileStream);
      const stats = await fsp.stat(partFile);
      if (stats.size === objStat.size) {
        return partFile;
      }
      throw new Error('Size mismatch between downloaded file and the object');
    };
    const partFile = await downloadToTmpFile();
    await fsp.rename(partFile, filePath);
  }

  /**
   * Stat information of the object.
   */
  async statObject(bucketName, objectName, statOpts) {
    const statOptDef = statOpts || {};
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!isValidObjectName(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`);
    }
    if (!isObject(statOptDef)) {
      throw new errors.InvalidArgumentError('statOpts should be of type "object"');
    }
    const query = qs.stringify(statOptDef);
    const method = 'HEAD';
    const res = await this.makeRequestAsyncOmit({
      method,
      bucketName,
      objectName,
      query
    });
    return {
      size: parseInt(res.headers['content-length']),
      metaData: extractMetadata(res.headers),
      lastModified: new Date(res.headers['last-modified']),
      versionId: getVersionId(res.headers),
      etag: sanitizeETag(res.headers.etag)
    };
  }
  async removeObject(bucketName, objectName, removeOpts) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError(`Invalid bucket name: ${bucketName}`);
    }
    if (!isValidObjectName(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`);
    }
    if (removeOpts && !isObject(removeOpts)) {
      throw new errors.InvalidArgumentError('removeOpts should be of type "object"');
    }
    const method = 'DELETE';
    const headers = {};
    if (removeOpts !== null && removeOpts !== void 0 && removeOpts.governanceBypass) {
      headers['X-Amz-Bypass-Governance-Retention'] = true;
    }
    if (removeOpts !== null && removeOpts !== void 0 && removeOpts.forceDelete) {
      headers['x-minio-force-delete'] = true;
    }
    const queryParams = {};
    if (removeOpts !== null && removeOpts !== void 0 && removeOpts.versionId) {
      queryParams.versionId = `${removeOpts.versionId}`;
    }
    const query = qs.stringify(queryParams);
    await this.makeRequestAsyncOmit({
      method,
      bucketName,
      objectName,
      headers,
      query
    }, '', [200, 204]);
  }

  // Calls implemented below are related to multipart.

  listIncompleteUploads(bucket, prefix, recursive) {
    if (prefix === undefined) {
      prefix = '';
    }
    if (recursive === undefined) {
      recursive = false;
    }
    if (!isValidBucketName(bucket)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucket);
    }
    if (!isValidPrefix(prefix)) {
      throw new errors.InvalidPrefixError(`Invalid prefix : ${prefix}`);
    }
    if (!isBoolean(recursive)) {
      throw new TypeError('recursive should be of type "boolean"');
    }
    const delimiter = recursive ? '' : '/';
    let keyMarker = '';
    let uploadIdMarker = '';
    const uploads = [];
    let ended = false;

    // TODO: refactor this with async/await and `stream.Readable.from`
    const readStream = new stream.Readable({
      objectMode: true
    });
    readStream._read = () => {
      // push one upload info per _read()
      if (uploads.length) {
        return readStream.push(uploads.shift());
      }
      if (ended) {
        return readStream.push(null);
      }
      this.listIncompleteUploadsQuery(bucket, prefix, keyMarker, uploadIdMarker, delimiter).then(result => {
        // eslint-disable-next-line @typescript-eslint/ban-ts-comment
        // @ts-ignore
        result.prefixes.forEach(prefix => uploads.push(prefix));
        async.eachSeries(result.uploads, (upload, cb) => {
          // for each incomplete upload add the sizes of its uploaded parts
          // eslint-disable-next-line @typescript-eslint/ban-ts-comment
          // @ts-ignore
          this.listParts(bucket, upload.key, upload.uploadId).then(parts => {
            // eslint-disable-next-line @typescript-eslint/ban-ts-comment
            // @ts-ignore
            upload.size = parts.reduce((acc, item) => acc + item.size, 0);
            uploads.push(upload);
            cb();
          }, err => cb(err));
        }, err => {
          if (err) {
            readStream.emit('error', err);
            return;
          }
          if (result.isTruncated) {
            keyMarker = result.nextKeyMarker;
            uploadIdMarker = result.nextUploadIdMarker;
          } else {
            ended = true;
          }

          // eslint-disable-next-line @typescript-eslint/ban-ts-comment
          // @ts-ignore
          readStream._read();
        });
      }, e => {
        readStream.emit('error', e);
      });
    };
    return readStream;
  }

  /**
   * Called by listIncompleteUploads to fetch a batch of incomplete uploads.
   */
  async listIncompleteUploadsQuery(bucketName, prefix, keyMarker, uploadIdMarker, delimiter) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!isString(prefix)) {
      throw new TypeError('prefix should be of type "string"');
    }
    if (!isString(keyMarker)) {
      throw new TypeError('keyMarker should be of type "string"');
    }
    if (!isString(uploadIdMarker)) {
      throw new TypeError('uploadIdMarker should be of type "string"');
    }
    if (!isString(delimiter)) {
      throw new TypeError('delimiter should be of type "string"');
    }
    const queries = [];
    queries.push(`prefix=${uriEscape(prefix)}`);
    queries.push(`delimiter=${uriEscape(delimiter)}`);
    if (keyMarker) {
      queries.push(`key-marker=${uriEscape(keyMarker)}`);
    }
    if (uploadIdMarker) {
      queries.push(`upload-id-marker=${uploadIdMarker}`);
    }
    const maxUploads = 1000;
    queries.push(`max-uploads=${maxUploads}`);
    queries.sort();
    queries.unshift('uploads');
    let query = '';
    if (queries.length > 0) {
      query = `${queries.join('&')}`;
    }
    const method = 'GET';
    const res = await this.makeRequestAsync({
      method,
      bucketName,
      query
    });
    const body = await readAsString(res);
    return xmlParsers.parseListMultipart(body);
  }

  /**
   * Initiate a new multipart upload.
   * @internal
   */
  async initiateNewMultipartUpload(bucketName, objectName, headers) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!isValidObjectName(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`);
    }
    if (!isObject(headers)) {
      throw new errors.InvalidObjectNameError('contentType should be of type "object"');
    }
    const method = 'POST';
    const query = 'uploads';
    const res = await this.makeRequestAsync({
      method,
      bucketName,
      objectName,
      query,
      headers
    });
    const body = await readAsBuffer(res);
    return parseInitiateMultipart(body.toString());
  }

  /**
   * Internal Method to abort a multipart upload request in case of any errors.
   *
   * @param bucketName - Bucket Name
   * @param objectName - Object Name
   * @param uploadId - id of a multipart upload to cancel during compose object sequence.
   */
  async abortMultipartUpload(bucketName, objectName, uploadId) {
    const method = 'DELETE';
    const query = `uploadId=${uploadId}`;
    const requestOptions = {
      method,
      bucketName,
      objectName: objectName,
      query
    };
    await this.makeRequestAsyncOmit(requestOptions, '', [204]);
  }
  async findUploadId(bucketName, objectName) {
    var _latestUpload;
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!isValidObjectName(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`);
    }
    let latestUpload;
    let keyMarker = '';
    let uploadIdMarker = '';
    for (;;) {
      const result = await this.listIncompleteUploadsQuery(bucketName, objectName, keyMarker, uploadIdMarker, '');
      for (const upload of result.uploads) {
        if (upload.key === objectName) {
          if (!latestUpload || upload.initiated.getTime() > latestUpload.initiated.getTime()) {
            latestUpload = upload;
          }
        }
      }
      if (result.isTruncated) {
        keyMarker = result.nextKeyMarker;
        uploadIdMarker = result.nextUploadIdMarker;
        continue;
      }
      break;
    }
    return (_latestUpload = latestUpload) === null || _latestUpload === void 0 ? void 0 : _latestUpload.uploadId;
  }

  /**
   * this call will aggregate the parts on the server into a single object.
   */
  async completeMultipartUpload(bucketName, objectName, uploadId, etags) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!isValidObjectName(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`);
    }
    if (!isString(uploadId)) {
      throw new TypeError('uploadId should be of type "string"');
    }
    if (!isObject(etags)) {
      throw new TypeError('etags should be of type "Array"');
    }
    if (!uploadId) {
      throw new errors.InvalidArgumentError('uploadId cannot be empty');
    }
    const method = 'POST';
    const query = `uploadId=${uriEscape(uploadId)}`;
    const builder = new xml2js.Builder();
    const payload = builder.buildObject({
      CompleteMultipartUpload: {
        $: {
          xmlns: 'http://s3.amazonaws.com/doc/2006-03-01/'
        },
        Part: etags.map(etag => {
          return {
            PartNumber: etag.part,
            ETag: etag.etag
          };
        })
      }
    });
    const res = await this.makeRequestAsync({
      method,
      bucketName,
      objectName,
      query
    }, payload);
    const body = await readAsBuffer(res);
    const result = parseCompleteMultipart(body.toString());
    if (!result) {
      throw new Error('BUG: failed to parse server response');
    }
    if (result.errCode) {
      // Multipart Complete API returns an error XML after a 200 http status
      throw new errors.S3Error(result.errMessage);
    }
    return {
      // eslint-disable-next-line @typescript-eslint/ban-ts-comment
      // @ts-ignore
      etag: result.etag,
      versionId: getVersionId(res.headers)
    };
  }

  /**
   * Get part-info of all parts of an incomplete upload specified by uploadId.
   */
  async listParts(bucketName, objectName, uploadId) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!isValidObjectName(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`);
    }
    if (!isString(uploadId)) {
      throw new TypeError('uploadId should be of type "string"');
    }
    if (!uploadId) {
      throw new errors.InvalidArgumentError('uploadId cannot be empty');
    }
    const parts = [];
    let marker = 0;
    let result;
    do {
      result = await this.listPartsQuery(bucketName, objectName, uploadId, marker);
      marker = result.marker;
      parts.push(...result.parts);
    } while (result.isTruncated);
    return parts;
  }

  /**
   * Called by listParts to fetch a batch of part-info
   */
  async listPartsQuery(bucketName, objectName, uploadId, marker) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!isValidObjectName(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`);
    }
    if (!isString(uploadId)) {
      throw new TypeError('uploadId should be of type "string"');
    }
    if (!isNumber(marker)) {
      throw new TypeError('marker should be of type "number"');
    }
    if (!uploadId) {
      throw new errors.InvalidArgumentError('uploadId cannot be empty');
    }
    let query = `uploadId=${uriEscape(uploadId)}`;
    if (marker) {
      query += `&part-number-marker=${marker}`;
    }
    const method = 'GET';
    const res = await this.makeRequestAsync({
      method,
      bucketName,
      objectName,
      query
    });
    return xmlParsers.parseListParts(await readAsString(res));
  }
  async listBuckets() {
    const method = 'GET';
    const regionConf = this.region || DEFAULT_REGION;
    const httpRes = await this.makeRequestAsync({
      method
    }, '', [200], regionConf);
    const xmlResult = await readAsString(httpRes);
    return xmlParsers.parseListBucket(xmlResult);
  }

  /**
   * Calculate part size given the object size. Part size will be atleast this.partSize
   */
  calculatePartSize(size) {
    if (!isNumber(size)) {
      throw new TypeError('size should be of type "number"');
    }
    if (size > this.maxObjectSize) {
      throw new TypeError(`size should not be more than ${this.maxObjectSize}`);
    }
    if (this.overRidePartSize) {
      return this.partSize;
    }
    let partSize = this.partSize;
    for (;;) {
      // while(true) {...} throws linting error.
      // If partSize is big enough to accomodate the object size, then use it.
      if (partSize * 10000 > size) {
        return partSize;
      }
      // Try part sizes as 64MB, 80MB, 96MB etc.
      partSize += 16 * 1024 * 1024;
    }
  }

  /**
   * Uploads the object using contents from a file
   */
  async fPutObject(bucketName, objectName, filePath, metaData) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!isValidObjectName(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`);
    }
    if (!isString(filePath)) {
      throw new TypeError('filePath should be of type "string"');
    }
    if (metaData && !isObject(metaData)) {
      throw new TypeError('metaData should be of type "object"');
    }

    // Inserts correct `content-type` attribute based on metaData and filePath
    metaData = insertContentType(metaData || {}, filePath);
    const stat = await fsp.stat(filePath);
    return await this.putObject(bucketName, objectName, fs.createReadStream(filePath), stat.size, metaData);
  }

  /**
   *  Uploading a stream, "Buffer" or "string".
   *  It's recommended to pass `size` argument with stream.
   */
  async putObject(bucketName, objectName, stream, size, metaData) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError(`Invalid bucket name: ${bucketName}`);
    }
    if (!isValidObjectName(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`);
    }

    // We'll need to shift arguments to the left because of metaData
    // and size being optional.
    if (isObject(size)) {
      metaData = size;
    }
    // Ensures Metadata has appropriate prefix for A3 API
    const headers = prependXAMZMeta(metaData);
    if (typeof stream === 'string' || stream instanceof Buffer) {
      // Adapts the non-stream interface into a stream.
      size = stream.length;
      stream = readableStream(stream);
    } else if (!isReadableStream(stream)) {
      throw new TypeError('third argument should be of type "stream.Readable" or "Buffer" or "string"');
    }
    if (isNumber(size) && size < 0) {
      throw new errors.InvalidArgumentError(`size cannot be negative, given size: ${size}`);
    }

    // Get the part size and forward that to the BlockStream. Default to the
    // largest block size possible if necessary.
    if (!isNumber(size)) {
      size = this.maxObjectSize;
    }

    // Get the part size and forward that to the BlockStream. Default to the
    // largest block size possible if necessary.
    if (size === undefined) {
      const statSize = await getContentLength(stream);
      if (statSize !== null) {
        size = statSize;
      }
    }
    if (!isNumber(size)) {
      // Backward compatibility
      size = this.maxObjectSize;
    }
    if (size === 0) {
      return this.uploadBuffer(bucketName, objectName, headers, Buffer.from(''));
    }
    const partSize = this.calculatePartSize(size);
    if (typeof stream === 'string' || Buffer.isBuffer(stream) || size <= partSize) {
      const buf = isReadableStream(stream) ? await readAsBuffer(stream) : Buffer.from(stream);
      return this.uploadBuffer(bucketName, objectName, headers, buf);
    }
    return this.uploadStream(bucketName, objectName, headers, stream, partSize);
  }

  /**
   * method to upload buffer in one call
   * @private
   */
  async uploadBuffer(bucketName, objectName, headers, buf) {
    const {
      md5sum,
      sha256sum
    } = hashBinary(buf, this.enableSHA256);
    headers['Content-Length'] = buf.length;
    if (!this.enableSHA256) {
      headers['Content-MD5'] = md5sum;
    }
    const res = await this.makeRequestStreamAsync({
      method: 'PUT',
      bucketName,
      objectName,
      headers
    }, buf, sha256sum, [200], '');
    await drainResponse(res);
    return {
      etag: sanitizeETag(res.headers.etag),
      versionId: getVersionId(res.headers)
    };
  }

  /**
   * upload stream with MultipartUpload
   * @private
   */
  async uploadStream(bucketName, objectName, headers, body, partSize) {
    // A map of the previously uploaded chunks, for resuming a file upload. This
    // will be null if we aren't resuming an upload.
    const oldParts = {};

    // Keep track of the etags for aggregating the chunks together later. Each
    // etag represents a single chunk of the file.
    const eTags = [];
    const previousUploadId = await this.findUploadId(bucketName, objectName);
    let uploadId;
    if (!previousUploadId) {
      uploadId = await this.initiateNewMultipartUpload(bucketName, objectName, headers);
    } else {
      uploadId = previousUploadId;
      const oldTags = await this.listParts(bucketName, objectName, previousUploadId);
      oldTags.forEach(e => {
        oldParts[e.part] = e;
      });
    }
    if (!isReadableStream(body)) {
      throw new TypeError('third argument should be of type "stream.Readable" or "Buffer" or "string"');
    }
    const chunkier = new BlockStream2({
      size: partSize,
      zeroPadding: false
    });

    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const [_, o] = await Promise.all([new Promise((resolve, reject) => {
      body.pipe(chunkier).on('error', reject);
      chunkier.on('end', resolve).on('error', reject);
    }), (async () => {
      let partNumber = 0;
      for await (const chunk of chunkier) {
        const md5 = crypto.createHash('md5').update(chunk).digest();
        partNumber++;
        const oldPart = oldParts[partNumber];
        if (oldPart) {
          if (oldPart.etag === md5.toString('hex')) {
            eTags.push({
              part: partNumber,
              etag: oldPart.etag
            });
            continue;
          }
        }

        // now start to upload missing part
        const options = {
          method: 'PUT',
          query: qs.stringify({
            partNumber,
            uploadId
          }),
          headers: {
            'Content-Length': chunk.length,
            'Content-MD5': md5.toString('base64')
          },
          bucketName,
          objectName
        };
        const response = await this.makeRequestAsyncOmit(options, chunk);
        let etag = response.headers.etag;
        if (etag) {
          etag = etag.replace(/^"/, '').replace(/"$/, '');
        } else {
          etag = '';
        }
        eTags.push({
          part: partNumber,
          etag
        });
        partNumber++;
      }
      if (eTags.length === 0) {
        await this.abortMultipartUpload(bucketName, objectName, uploadId);
        return await this.uploadBuffer(bucketName, objectName, headers, Buffer.from(''));
      }
      return await this.completeMultipartUpload(bucketName, objectName, uploadId, eTags);
    })()]);
    return o;
  }
  async removeBucketReplication(bucketName) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    const method = 'DELETE';
    const query = 'replication';
    await this.makeRequestAsyncOmit({
      method,
      bucketName,
      query
    }, '', [200, 204], '');
  }
  async setBucketReplication(bucketName, replicationConfig) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!isObject(replicationConfig)) {
      throw new errors.InvalidArgumentError('replicationConfig should be of type "object"');
    } else {
      if (_.isEmpty(replicationConfig.role)) {
        throw new errors.InvalidArgumentError('Role cannot be empty');
      } else if (replicationConfig.role && !isString(replicationConfig.role)) {
        throw new errors.InvalidArgumentError('Invalid value for role', replicationConfig.role);
      }
      if (_.isEmpty(replicationConfig.rules)) {
        throw new errors.InvalidArgumentError('Minimum one replication rule must be specified');
      }
    }
    const method = 'PUT';
    const query = 'replication';
    const headers = {};
    const replicationParamsConfig = {
      ReplicationConfiguration: {
        Role: replicationConfig.role,
        Rule: replicationConfig.rules
      }
    };
    const builder = new xml2js.Builder({
      renderOpts: {
        pretty: false
      },
      headless: true
    });
    const payload = builder.buildObject(replicationParamsConfig);
    headers['Content-MD5'] = toMd5(payload);
    await this.makeRequestAsyncOmit({
      method,
      bucketName,
      query,
      headers
    }, payload);
  }
  async getBucketReplication(bucketName) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    const method = 'GET';
    const query = 'replication';
    const httpRes = await this.makeRequestAsync({
      method,
      bucketName,
      query
    }, '', [200, 204]);
    const xmlResult = await readAsString(httpRes);
    return xmlParsers.parseReplicationConfig(xmlResult);
  }
  async getObjectLegalHold(bucketName, objectName, getOpts) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!isValidObjectName(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`);
    }
    if (getOpts) {
      if (!isObject(getOpts)) {
        throw new TypeError('getOpts should be of type "Object"');
      } else if (Object.keys(getOpts).length > 0 && getOpts.versionId && !isString(getOpts.versionId)) {
        throw new TypeError('versionId should be of type string.:', getOpts.versionId);
      }
    }
    const method = 'GET';
    let query = 'legal-hold';
    if (getOpts !== null && getOpts !== void 0 && getOpts.versionId) {
      query += `&versionId=${getOpts.versionId}`;
    }
    const httpRes = await this.makeRequestAsync({
      method,
      bucketName,
      objectName,
      query
    }, '', [200]);
    const strRes = await readAsString(httpRes);
    return parseObjectLegalHoldConfig(strRes);
  }
  async setObjectLegalHold(bucketName, objectName, setOpts = {
    status: LEGAL_HOLD_STATUS.ENABLED
  }) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!isValidObjectName(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`);
    }
    if (!isObject(setOpts)) {
      throw new TypeError('setOpts should be of type "Object"');
    } else {
      if (![LEGAL_HOLD_STATUS.ENABLED, LEGAL_HOLD_STATUS.DISABLED].includes(setOpts === null || setOpts === void 0 ? void 0 : setOpts.status)) {
        throw new TypeError('Invalid status: ' + setOpts.status);
      }
      if (setOpts.versionId && !setOpts.versionId.length) {
        throw new TypeError('versionId should be of type string.:' + setOpts.versionId);
      }
    }
    const method = 'PUT';
    let query = 'legal-hold';
    if (setOpts.versionId) {
      query += `&versionId=${setOpts.versionId}`;
    }
    const config = {
      Status: setOpts.status
    };
    const builder = new xml2js.Builder({
      rootName: 'LegalHold',
      renderOpts: {
        pretty: false
      },
      headless: true
    });
    const payload = builder.buildObject(config);
    const headers = {};
    headers['Content-MD5'] = toMd5(payload);
    await this.makeRequestAsyncOmit({
      method,
      bucketName,
      objectName,
      query,
      headers
    }, payload);
  }

  /**
   * Get Tags associated with a Bucket
   */
  async getBucketTagging(bucketName) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError(`Invalid bucket name: ${bucketName}`);
    }
    const method = 'GET';
    const query = 'tagging';
    const requestOptions = {
      method,
      bucketName,
      query
    };
    const response = await this.makeRequestAsync(requestOptions);
    const body = await readAsString(response);
    return xmlParsers.parseTagging(body);
  }

  /**
   *  Get the tags associated with a bucket OR an object
   */
  async getObjectTagging(bucketName, objectName, getOpts) {
    const method = 'GET';
    let query = 'tagging';
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!isValidObjectName(objectName)) {
      throw new errors.InvalidBucketNameError('Invalid object name: ' + objectName);
    }
    if (getOpts && !isObject(getOpts)) {
      throw new errors.InvalidArgumentError('getOpts should be of type "object"');
    }
    if (getOpts && getOpts.versionId) {
      query = `${query}&versionId=${getOpts.versionId}`;
    }
    const requestOptions = {
      method,
      bucketName,
      query
    };
    if (objectName) {
      requestOptions['objectName'] = objectName;
    }
    const response = await this.makeRequestAsync(requestOptions);
    const body = await readAsString(response);
    return xmlParsers.parseTagging(body);
  }

  /**
   *  Set the policy on a bucket or an object prefix.
   */
  async setBucketPolicy(bucketName, policy) {
    // Validate arguments.
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError(`Invalid bucket name: ${bucketName}`);
    }
    if (!isString(policy)) {
      throw new errors.InvalidBucketPolicyError(`Invalid bucket policy: ${policy} - must be "string"`);
    }
    const query = 'policy';
    let method = 'DELETE';
    if (policy) {
      method = 'PUT';
    }
    await this.makeRequestAsyncOmit({
      method,
      bucketName,
      query
    }, policy, [204], '');
  }

  /**
   * Get the policy on a bucket or an object prefix.
   */
  async getBucketPolicy(bucketName) {
    // Validate arguments.
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError(`Invalid bucket name: ${bucketName}`);
    }
    const method = 'GET';
    const query = 'policy';
    const res = await this.makeRequestAsync({
      method,
      bucketName,
      query
    });
    return await readAsString(res);
  }
  async putObjectRetention(bucketName, objectName, retentionOpts = {}) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError(`Invalid bucket name: ${bucketName}`);
    }
    if (!isValidObjectName(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`);
    }
    if (!isObject(retentionOpts)) {
      throw new errors.InvalidArgumentError('retentionOpts should be of type "object"');
    } else {
      if (retentionOpts.governanceBypass && !isBoolean(retentionOpts.governanceBypass)) {
        throw new errors.InvalidArgumentError(`Invalid value for governanceBypass: ${retentionOpts.governanceBypass}`);
      }
      if (retentionOpts.mode && ![RETENTION_MODES.COMPLIANCE, RETENTION_MODES.GOVERNANCE].includes(retentionOpts.mode)) {
        throw new errors.InvalidArgumentError(`Invalid object retention mode: ${retentionOpts.mode}`);
      }
      if (retentionOpts.retainUntilDate && !isString(retentionOpts.retainUntilDate)) {
        throw new errors.InvalidArgumentError(`Invalid value for retainUntilDate: ${retentionOpts.retainUntilDate}`);
      }
      if (retentionOpts.versionId && !isString(retentionOpts.versionId)) {
        throw new errors.InvalidArgumentError(`Invalid value for versionId: ${retentionOpts.versionId}`);
      }
    }
    const method = 'PUT';
    let query = 'retention';
    const headers = {};
    if (retentionOpts.governanceBypass) {
      headers['X-Amz-Bypass-Governance-Retention'] = true;
    }
    const builder = new xml2js.Builder({
      rootName: 'Retention',
      renderOpts: {
        pretty: false
      },
      headless: true
    });
    const params = {};
    if (retentionOpts.mode) {
      params.Mode = retentionOpts.mode;
    }
    if (retentionOpts.retainUntilDate) {
      params.RetainUntilDate = retentionOpts.retainUntilDate;
    }
    if (retentionOpts.versionId) {
      query += `&versionId=${retentionOpts.versionId}`;
    }
    const payload = builder.buildObject(params);
    headers['Content-MD5'] = toMd5(payload);
    await this.makeRequestAsyncOmit({
      method,
      bucketName,
      objectName,
      query,
      headers
    }, payload, [200, 204]);
  }
  async getObjectLockConfig(bucketName) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    const method = 'GET';
    const query = 'object-lock';
    const httpRes = await this.makeRequestAsync({
      method,
      bucketName,
      query
    });
    const xmlResult = await readAsString(httpRes);
    return xmlParsers.parseObjectLockConfig(xmlResult);
  }
  async setObjectLockConfig(bucketName, lockConfigOpts) {
    const retentionModes = [RETENTION_MODES.COMPLIANCE, RETENTION_MODES.GOVERNANCE];
    const validUnits = [RETENTION_VALIDITY_UNITS.DAYS, RETENTION_VALIDITY_UNITS.YEARS];
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (lockConfigOpts.mode && !retentionModes.includes(lockConfigOpts.mode)) {
      throw new TypeError(`lockConfigOpts.mode should be one of ${retentionModes}`);
    }
    if (lockConfigOpts.unit && !validUnits.includes(lockConfigOpts.unit)) {
      throw new TypeError(`lockConfigOpts.unit should be one of ${validUnits}`);
    }
    if (lockConfigOpts.validity && !isNumber(lockConfigOpts.validity)) {
      throw new TypeError(`lockConfigOpts.validity should be a number`);
    }
    const method = 'PUT';
    const query = 'object-lock';
    const config = {
      ObjectLockEnabled: 'Enabled'
    };
    const configKeys = Object.keys(lockConfigOpts);
    const isAllKeysSet = ['unit', 'mode', 'validity'].every(lck => configKeys.includes(lck));
    // Check if keys are present and all keys are present.
    if (configKeys.length > 0) {
      if (!isAllKeysSet) {
        throw new TypeError(`lockConfigOpts.mode,lockConfigOpts.unit,lockConfigOpts.validity all the properties should be specified.`);
      } else {
        config.Rule = {
          DefaultRetention: {}
        };
        if (lockConfigOpts.mode) {
          config.Rule.DefaultRetention.Mode = lockConfigOpts.mode;
        }
        if (lockConfigOpts.unit === RETENTION_VALIDITY_UNITS.DAYS) {
          config.Rule.DefaultRetention.Days = lockConfigOpts.validity;
        } else if (lockConfigOpts.unit === RETENTION_VALIDITY_UNITS.YEARS) {
          config.Rule.DefaultRetention.Years = lockConfigOpts.validity;
        }
      }
    }
    const builder = new xml2js.Builder({
      rootName: 'ObjectLockConfiguration',
      renderOpts: {
        pretty: false
      },
      headless: true
    });
    const payload = builder.buildObject(config);
    const headers = {};
    headers['Content-MD5'] = toMd5(payload);
    await this.makeRequestAsyncOmit({
      method,
      bucketName,
      query,
      headers
    }, payload);
  }
  async getBucketVersioning(bucketName) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    const method = 'GET';
    const query = 'versioning';
    const httpRes = await this.makeRequestAsync({
      method,
      bucketName,
      query
    });
    const xmlResult = await readAsString(httpRes);
    return await xmlParsers.parseBucketVersioningConfig(xmlResult);
  }
  async setBucketVersioning(bucketName, versionConfig) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!Object.keys(versionConfig).length) {
      throw new errors.InvalidArgumentError('versionConfig should be of type "object"');
    }
    const method = 'PUT';
    const query = 'versioning';
    const builder = new xml2js.Builder({
      rootName: 'VersioningConfiguration',
      renderOpts: {
        pretty: false
      },
      headless: true
    });
    const payload = builder.buildObject(versionConfig);
    await this.makeRequestAsyncOmit({
      method,
      bucketName,
      query
    }, payload);
  }
  async setTagging(taggingParams) {
    const {
      bucketName,
      objectName,
      tags,
      putOpts
    } = taggingParams;
    const method = 'PUT';
    let query = 'tagging';
    if (putOpts && putOpts !== null && putOpts !== void 0 && putOpts.versionId) {
      query = `${query}&versionId=${putOpts.versionId}`;
    }
    const tagsList = [];
    for (const [key, value] of Object.entries(tags)) {
      tagsList.push({
        Key: key,
        Value: value
      });
    }
    const taggingConfig = {
      Tagging: {
        TagSet: {
          Tag: tagsList
        }
      }
    };
    const headers = {};
    const builder = new xml2js.Builder({
      headless: true,
      renderOpts: {
        pretty: false
      }
    });
    const payloadBuf = Buffer.from(builder.buildObject(taggingConfig));
    const requestOptions = {
      method,
      bucketName,
      query,
      headers,
      ...(objectName && {
        objectName: objectName
      })
    };
    headers['Content-MD5'] = toMd5(payloadBuf);
    await this.makeRequestAsyncOmit(requestOptions, payloadBuf);
  }
  async removeTagging({
    bucketName,
    objectName,
    removeOpts
  }) {
    const method = 'DELETE';
    let query = 'tagging';
    if (removeOpts && Object.keys(removeOpts).length && removeOpts.versionId) {
      query = `${query}&versionId=${removeOpts.versionId}`;
    }
    const requestOptions = {
      method,
      bucketName,
      objectName,
      query
    };
    if (objectName) {
      requestOptions['objectName'] = objectName;
    }
    await this.makeRequestAsync(requestOptions, '', [200, 204]);
  }
  async setBucketTagging(bucketName, tags) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!isPlainObject(tags)) {
      throw new errors.InvalidArgumentError('tags should be of type "object"');
    }
    if (Object.keys(tags).length > 10) {
      throw new errors.InvalidArgumentError('maximum tags allowed is 10"');
    }
    await this.setTagging({
      bucketName,
      tags
    });
  }
  async removeBucketTagging(bucketName) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    await this.removeTagging({
      bucketName
    });
  }
  async setObjectTagging(bucketName, objectName, tags, putOpts) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!isValidObjectName(objectName)) {
      throw new errors.InvalidBucketNameError('Invalid object name: ' + objectName);
    }
    if (!isPlainObject(tags)) {
      throw new errors.InvalidArgumentError('tags should be of type "object"');
    }
    if (Object.keys(tags).length > 10) {
      throw new errors.InvalidArgumentError('Maximum tags allowed is 10"');
    }
    await this.setTagging({
      bucketName,
      objectName,
      tags,
      putOpts
    });
  }
  async removeObjectTagging(bucketName, objectName, removeOpts) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!isValidObjectName(objectName)) {
      throw new errors.InvalidBucketNameError('Invalid object name: ' + objectName);
    }
    if (removeOpts && Object.keys(removeOpts).length && !isObject(removeOpts)) {
      throw new errors.InvalidArgumentError('removeOpts should be of type "object"');
    }
    await this.removeTagging({
      bucketName,
      objectName,
      removeOpts
    });
  }
  async selectObjectContent(bucketName, objectName, selectOpts) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError(`Invalid bucket name: ${bucketName}`);
    }
    if (!isValidObjectName(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`);
    }
    if (!_.isEmpty(selectOpts)) {
      if (!isString(selectOpts.expression)) {
        throw new TypeError('sqlExpression should be of type "string"');
      }
      if (!_.isEmpty(selectOpts.inputSerialization)) {
        if (!isObject(selectOpts.inputSerialization)) {
          throw new TypeError('inputSerialization should be of type "object"');
        }
      } else {
        throw new TypeError('inputSerialization is required');
      }
      if (!_.isEmpty(selectOpts.outputSerialization)) {
        if (!isObject(selectOpts.outputSerialization)) {
          throw new TypeError('outputSerialization should be of type "object"');
        }
      } else {
        throw new TypeError('outputSerialization is required');
      }
    } else {
      throw new TypeError('valid select configuration is required');
    }
    const method = 'POST';
    const query = `select&select-type=2`;
    const config = [{
      Expression: selectOpts.expression
    }, {
      ExpressionType: selectOpts.expressionType || 'SQL'
    }, {
      InputSerialization: [selectOpts.inputSerialization]
    }, {
      OutputSerialization: [selectOpts.outputSerialization]
    }];

    // Optional
    if (selectOpts.requestProgress) {
      config.push({
        RequestProgress: selectOpts === null || selectOpts === void 0 ? void 0 : selectOpts.requestProgress
      });
    }
    // Optional
    if (selectOpts.scanRange) {
      config.push({
        ScanRange: selectOpts.scanRange
      });
    }
    const builder = new xml2js.Builder({
      rootName: 'SelectObjectContentRequest',
      renderOpts: {
        pretty: false
      },
      headless: true
    });
    const payload = builder.buildObject(config);
    const res = await this.makeRequestAsync({
      method,
      bucketName,
      objectName,
      query
    }, payload);
    const body = await readAsBuffer(res);
    return parseSelectObjectContentResponse(body);
  }
  async applyBucketLifecycle(bucketName, policyConfig) {
    const method = 'PUT';
    const query = 'lifecycle';
    const headers = {};
    const builder = new xml2js.Builder({
      rootName: 'LifecycleConfiguration',
      headless: true,
      renderOpts: {
        pretty: false
      }
    });
    const payload = builder.buildObject(policyConfig);
    headers['Content-MD5'] = toMd5(payload);
    await this.makeRequestAsyncOmit({
      method,
      bucketName,
      query,
      headers
    }, payload);
  }
  async removeBucketLifecycle(bucketName) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    const method = 'DELETE';
    const query = 'lifecycle';
    await this.makeRequestAsyncOmit({
      method,
      bucketName,
      query
    }, '', [204]);
  }
  async setBucketLifecycle(bucketName, lifeCycleConfig) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (_.isEmpty(lifeCycleConfig)) {
      await this.removeBucketLifecycle(bucketName);
    } else {
      await this.applyBucketLifecycle(bucketName, lifeCycleConfig);
    }
  }
  async getBucketLifecycle(bucketName) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    const method = 'GET';
    const query = 'lifecycle';
    const res = await this.makeRequestAsync({
      method,
      bucketName,
      query
    });
    const body = await readAsString(res);
    return xmlParsers.parseLifecycleConfig(body);
  }
  async setBucketEncryption(bucketName, encryptionConfig) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!_.isEmpty(encryptionConfig) && encryptionConfig.Rule.length > 1) {
      throw new errors.InvalidArgumentError('Invalid Rule length. Only one rule is allowed.: ' + encryptionConfig.Rule);
    }
    let encryptionObj = encryptionConfig;
    if (_.isEmpty(encryptionConfig)) {
      encryptionObj = {
        // Default MinIO Server Supported Rule
        Rule: [{
          ApplyServerSideEncryptionByDefault: {
            SSEAlgorithm: 'AES256'
          }
        }]
      };
    }
    const method = 'PUT';
    const query = 'encryption';
    const builder = new xml2js.Builder({
      rootName: 'ServerSideEncryptionConfiguration',
      renderOpts: {
        pretty: false
      },
      headless: true
    });
    const payload = builder.buildObject(encryptionObj);
    const headers = {};
    headers['Content-MD5'] = toMd5(payload);
    await this.makeRequestAsyncOmit({
      method,
      bucketName,
      query,
      headers
    }, payload);
  }
  async getBucketEncryption(bucketName) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    const method = 'GET';
    const query = 'encryption';
    const res = await this.makeRequestAsync({
      method,
      bucketName,
      query
    });
    const body = await readAsString(res);
    return xmlParsers.parseBucketEncryptionConfig(body);
  }
  async removeBucketEncryption(bucketName) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    const method = 'DELETE';
    const query = 'encryption';
    await this.makeRequestAsyncOmit({
      method,
      bucketName,
      query
    }, '', [204]);
  }
  async getObjectRetention(bucketName, objectName, getOpts) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!isValidObjectName(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`);
    }
    if (getOpts && !isObject(getOpts)) {
      throw new errors.InvalidArgumentError('getOpts should be of type "object"');
    } else if (getOpts !== null && getOpts !== void 0 && getOpts.versionId && !isString(getOpts.versionId)) {
      throw new errors.InvalidArgumentError('versionId should be of type "string"');
    }
    const method = 'GET';
    let query = 'retention';
    if (getOpts !== null && getOpts !== void 0 && getOpts.versionId) {
      query += `&versionId=${getOpts.versionId}`;
    }
    const res = await this.makeRequestAsync({
      method,
      bucketName,
      objectName,
      query
    });
    const body = await readAsString(res);
    return xmlParsers.parseObjectRetentionConfig(body);
  }
  async removeObjects(bucketName, objectsList) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!Array.isArray(objectsList)) {
      throw new errors.InvalidArgumentError('objectsList should be a list');
    }
    const runDeleteObjects = async batch => {
      const delObjects = batch.map(value => {
        return isObject(value) ? {
          Key: value.name,
          VersionId: value.versionId
        } : {
          Key: value
        };
      });
      const remObjects = {
        Delete: {
          Quiet: true,
          Object: delObjects
        }
      };
      const payload = Buffer.from(new xml2js.Builder({
        headless: true
      }).buildObject(remObjects));
      const headers = {
        'Content-MD5': toMd5(payload)
      };
      const res = await this.makeRequestAsync({
        method: 'POST',
        bucketName,
        query: 'delete',
        headers
      }, payload);
      const body = await readAsString(res);
      return xmlParsers.removeObjectsParser(body);
    };
    const maxEntries = 1000; // max entries accepted in server for DeleteMultipleObjects API.
    // Client side batching
    const batches = [];
    for (let i = 0; i < objectsList.length; i += maxEntries) {
      batches.push(objectsList.slice(i, i + maxEntries));
    }
    const batchResults = await Promise.all(batches.map(runDeleteObjects));
    return batchResults.flat();
  }
  async removeIncompleteUpload(bucketName, objectName) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.IsValidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!isValidObjectName(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`);
    }
    const removeUploadId = await this.findUploadId(bucketName, objectName);
    const method = 'DELETE';
    const query = `uploadId=${removeUploadId}`;
    await this.makeRequestAsyncOmit({
      method,
      bucketName,
      objectName,
      query
    }, '', [204]);
  }
  async copyObjectV1(targetBucketName, targetObjectName, sourceBucketNameAndObjectName, conditions) {
    if (typeof conditions == 'function') {
      conditions = null;
    }
    if (!isValidBucketName(targetBucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + targetBucketName);
    }
    if (!isValidObjectName(targetObjectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${targetObjectName}`);
    }
    if (!isString(sourceBucketNameAndObjectName)) {
      throw new TypeError('sourceBucketNameAndObjectName should be of type "string"');
    }
    if (sourceBucketNameAndObjectName === '') {
      throw new errors.InvalidPrefixError(`Empty source prefix`);
    }
    if (conditions != null && !(conditions instanceof CopyConditions)) {
      throw new TypeError('conditions should be of type "CopyConditions"');
    }
    const headers = {};
    headers['x-amz-copy-source'] = uriResourceEscape(sourceBucketNameAndObjectName);
    if (conditions) {
      if (conditions.modified !== '') {
        headers['x-amz-copy-source-if-modified-since'] = conditions.modified;
      }
      if (conditions.unmodified !== '') {
        headers['x-amz-copy-source-if-unmodified-since'] = conditions.unmodified;
      }
      if (conditions.matchETag !== '') {
        headers['x-amz-copy-source-if-match'] = conditions.matchETag;
      }
      if (conditions.matchETagExcept !== '') {
        headers['x-amz-copy-source-if-none-match'] = conditions.matchETagExcept;
      }
    }
    const method = 'PUT';
    const res = await this.makeRequestAsync({
      method,
      bucketName: targetBucketName,
      objectName: targetObjectName,
      headers
    });
    const body = await readAsString(res);
    return xmlParsers.parseCopyObject(body);
  }
  async copyObjectV2(sourceConfig, destConfig) {
    if (!(sourceConfig instanceof CopySourceOptions)) {
      throw new errors.InvalidArgumentError('sourceConfig should of type CopySourceOptions ');
    }
    if (!(destConfig instanceof CopyDestinationOptions)) {
      throw new errors.InvalidArgumentError('destConfig should of type CopyDestinationOptions ');
    }
    if (!destConfig.validate()) {
      return Promise.reject();
    }
    if (!destConfig.validate()) {
      return Promise.reject();
    }
    const headers = Object.assign({}, sourceConfig.getHeaders(), destConfig.getHeaders());
    const bucketName = destConfig.Bucket;
    const objectName = destConfig.Object;
    const method = 'PUT';
    const res = await this.makeRequestAsync({
      method,
      bucketName,
      objectName,
      headers
    });
    const body = await readAsString(res);
    const copyRes = xmlParsers.parseCopyObject(body);
    const resHeaders = res.headers;
    const sizeHeaderValue = resHeaders && resHeaders['content-length'];
    const size = typeof sizeHeaderValue === 'number' ? sizeHeaderValue : undefined;
    return {
      Bucket: destConfig.Bucket,
      Key: destConfig.Object,
      LastModified: copyRes.lastModified,
      MetaData: extractMetadata(resHeaders),
      VersionId: getVersionId(resHeaders),
      SourceVersionId: getSourceVersionId(resHeaders),
      Etag: sanitizeETag(resHeaders.etag),
      Size: size
    };
  }
  async copyObject(...allArgs) {
    if (typeof allArgs[0] === 'string') {
      const [targetBucketName, targetObjectName, sourceBucketNameAndObjectName, conditions] = allArgs;
      return await this.copyObjectV1(targetBucketName, targetObjectName, sourceBucketNameAndObjectName, conditions);
    }
    const [source, dest] = allArgs;
    return await this.copyObjectV2(source, dest);
  }
  async uploadPart(partConfig, payload) {
    const {
      bucketName,
      objectName,
      uploadID,
      partNumber,
      headers
    } = partConfig;
    const method = 'PUT';
    const query = `uploadId=${uploadID}&partNumber=${partNumber}`;
    const requestOptions = {
      method,
      bucketName,
      objectName: objectName,
      query,
      headers
    };
    const res = await this.makeRequestAsync(requestOptions, payload);
    const body = await readAsString(res);
    const partRes = uploadPartParser(body);
    const partEtagVal = sanitizeETag(res.headers.etag) || sanitizeETag(partRes.ETag);
    return {
      etag: partEtagVal,
      key: objectName,
      part: partNumber
    };
  }
  async composeObject(destObjConfig, sourceObjList, {
    maxConcurrency = 10
  } = {}) {
    const sourceFilesLength = sourceObjList.length;
    if (!Array.isArray(sourceObjList)) {
      throw new errors.InvalidArgumentError('sourceConfig should an array of CopySourceOptions ');
    }
    if (!(destObjConfig instanceof CopyDestinationOptions)) {
      throw new errors.InvalidArgumentError('destConfig should of type CopyDestinationOptions ');
    }
    if (sourceFilesLength < 1 || sourceFilesLength > PART_CONSTRAINTS.MAX_PARTS_COUNT) {
      throw new errors.InvalidArgumentError(`"There must be as least one and up to ${PART_CONSTRAINTS.MAX_PARTS_COUNT} source objects.`);
    }
    for (let i = 0; i < sourceFilesLength; i++) {
      const sObj = sourceObjList[i];
      if (!sObj.validate()) {
        return false;
      }
    }
    if (!destObjConfig.validate()) {
      return false;
    }
    const getStatOptions = srcConfig => {
      let statOpts = {};
      if (!_.isEmpty(srcConfig.VersionID)) {
        statOpts = {
          versionId: srcConfig.VersionID
        };
      }
      return statOpts;
    };
    const srcObjectSizes = [];
    let totalSize = 0;
    let totalParts = 0;
    const sourceObjStats = sourceObjList.map(srcItem => this.statObject(srcItem.Bucket, srcItem.Object, getStatOptions(srcItem)));
    const srcObjectInfos = await Promise.all(sourceObjStats);
    const validatedStats = srcObjectInfos.map((resItemStat, index) => {
      const srcConfig = sourceObjList[index];
      let srcCopySize = resItemStat.size;
      // Check if a segment is specified, and if so, is the
      // segment within object bounds?
      if (srcConfig && srcConfig.MatchRange) {
        // Since range is specified,
        //    0 <= src.srcStart <= src.srcEnd
        // so only invalid case to check is:
        const srcStart = srcConfig.Start;
        const srcEnd = srcConfig.End;
        if (srcEnd >= srcCopySize || srcStart < 0) {
          throw new errors.InvalidArgumentError(`CopySrcOptions ${index} has invalid segment-to-copy [${srcStart}, ${srcEnd}] (size is ${srcCopySize})`);
        }
        srcCopySize = srcEnd - srcStart + 1;
      }

      // Only the last source may be less than `absMinPartSize`
      if (srcCopySize < PART_CONSTRAINTS.ABS_MIN_PART_SIZE && index < sourceFilesLength - 1) {
        throw new errors.InvalidArgumentError(`CopySrcOptions ${index} is too small (${srcCopySize}) and it is not the last part.`);
      }

      // Is data to copy too large?
      totalSize += srcCopySize;
      if (totalSize > PART_CONSTRAINTS.MAX_MULTIPART_PUT_OBJECT_SIZE) {
        throw new errors.InvalidArgumentError(`Cannot compose an object of size ${totalSize} (> 5TiB)`);
      }

      // record source size
      srcObjectSizes[index] = srcCopySize;

      // calculate parts needed for current source
      totalParts += partsRequired(srcCopySize);
      // Do we need more parts than we are allowed?
      if (totalParts > PART_CONSTRAINTS.MAX_PARTS_COUNT) {
        throw new errors.InvalidArgumentError(`Your proposed compose object requires more than ${PART_CONSTRAINTS.MAX_PARTS_COUNT} parts`);
      }
      return resItemStat;
    });
    if (totalParts === 1 && totalSize <= PART_CONSTRAINTS.MAX_PART_SIZE || totalSize === 0) {
      return await this.copyObject(sourceObjList[0], destObjConfig); // use copyObjectV2
    }

    // preserve etag to avoid modification of object while copying.
    for (let i = 0; i < sourceFilesLength; i++) {
      ;
      sourceObjList[i].MatchETag = validatedStats[i].etag;
    }
    const splitPartSizeList = validatedStats.map((resItemStat, idx) => {
      return calculateEvenSplits(srcObjectSizes[idx], sourceObjList[idx]);
    });
    const getUploadPartConfigList = uploadId => {
      const uploadPartConfigList = [];
      splitPartSizeList.forEach((splitSize, splitIndex) => {
        if (splitSize) {
          const {
            startIndex: startIdx,
            endIndex: endIdx
          } = splitSize;
          const partIndex = splitIndex + 1; // part index starts from 1.
          const totalUploads = Array.from(startIdx);
          const headers = sourceObjList[splitIndex].getHeaders();
          totalUploads.forEach((splitStart, upldCtrIdx) => {
            const splitEnd = endIdx[upldCtrIdx];

            // x-amz-copy-source is already set by CopySourceOptions.getHeaders() above,
            // URI-escaped and including the ?versionId selector when pinned. Don't
            // overwrite it here: the raw value broke non-ASCII names (ERR_INVALID_CHAR,
            // #1385) and silently dropped the source version.
            headers['x-amz-copy-source-range'] = `bytes=${splitStart}-${splitEnd}`;
            const uploadPartConfig = {
              bucketName: destObjConfig.Bucket,
              objectName: destObjConfig.Object,
              uploadID: uploadId,
              partNumber: partIndex,
              headers: headers
            };
            uploadPartConfigList.push(uploadPartConfig);
          });
        }
      });
      return uploadPartConfigList;
    };
    const uploadAllParts = async uploadList => {
      const partUploads = [];

      // Process upload parts in batches to avoid too many concurrent requests
      for (const batch of _.chunk(uploadList, maxConcurrency)) {
        const batchResults = await Promise.all(batch.map(item => this.uploadPart(item)));
        partUploads.push(...batchResults);
      }

      // Process results here if needed
      return partUploads;
    };
    const performUploadParts = async uploadId => {
      const uploadList = getUploadPartConfigList(uploadId);
      const partsRes = await uploadAllParts(uploadList);
      return partsRes.map(partCopy => ({
        etag: partCopy.etag,
        part: partCopy.part
      }));
    };
    const newUploadHeaders = destObjConfig.getHeaders();
    const uploadId = await this.initiateNewMultipartUpload(destObjConfig.Bucket, destObjConfig.Object, newUploadHeaders);
    try {
      const partsDone = await performUploadParts(uploadId);
      return await this.completeMultipartUpload(destObjConfig.Bucket, destObjConfig.Object, uploadId, partsDone);
    } catch (err) {
      return await this.abortMultipartUpload(destObjConfig.Bucket, destObjConfig.Object, uploadId);
    }
  }
  async presignedUrl(method, bucketName, objectName, expires, reqParams, requestDate) {
    var _requestDate;
    if (this.anonymous) {
      throw new errors.AnonymousRequestError(`Presigned ${method} url cannot be generated for anonymous requests`);
    }
    if (!expires) {
      expires = PRESIGN_EXPIRY_DAYS_MAX;
    }
    if (!reqParams) {
      reqParams = {};
    }
    if (!requestDate) {
      requestDate = new Date();
    }

    // Type assertions
    if (expires && typeof expires !== 'number') {
      throw new TypeError('expires should be of type "number"');
    }
    if (reqParams && typeof reqParams !== 'object') {
      throw new TypeError('reqParams should be of type "object"');
    }
    if (requestDate && !(requestDate instanceof Date) || requestDate && isNaN((_requestDate = requestDate) === null || _requestDate === void 0 ? void 0 : _requestDate.getTime())) {
      throw new TypeError('requestDate should be of type "Date" and valid');
    }
    const query = reqParams ? qs.stringify(reqParams) : undefined;
    try {
      const region = await this.getBucketRegionAsync(bucketName);
      await this.checkAndRefreshCreds();
      const reqOptions = this.getRequestOptions({
        method,
        region,
        bucketName,
        objectName,
        query
      });
      return presignSignatureV4(reqOptions, this.accessKey, this.secretKey, this.sessionToken, region, requestDate, expires);
    } catch (err) {
      if (err instanceof errors.InvalidBucketNameError) {
        throw new errors.InvalidArgumentError(`Unable to get bucket region for ${bucketName}.`);
      }
      throw err;
    }
  }
  async presignedGetObject(bucketName, objectName, expires, respHeaders, requestDate) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!isValidObjectName(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`);
    }
    const validRespHeaders = ['response-content-type', 'response-content-language', 'response-expires', 'response-cache-control', 'response-content-disposition', 'response-content-encoding'];
    validRespHeaders.forEach(header => {
      // @ts-ignore
      if (respHeaders !== undefined && respHeaders[header] !== undefined && !isString(respHeaders[header])) {
        throw new TypeError(`response header ${header} should be of type "string"`);
      }
    });
    return this.presignedUrl('GET', bucketName, objectName, expires, respHeaders, requestDate);
  }
  async presignedPutObject(bucketName, objectName, expires) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError(`Invalid bucket name: ${bucketName}`);
    }
    if (!isValidObjectName(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`);
    }
    return this.presignedUrl('PUT', bucketName, objectName, expires);
  }
  newPostPolicy() {
    return new PostPolicy();
  }
  async presignedPostPolicy(postPolicy) {
    if (this.anonymous) {
      throw new errors.AnonymousRequestError('Presigned POST policy cannot be generated for anonymous requests');
    }
    if (!isObject(postPolicy)) {
      throw new TypeError('postPolicy should be of type "object"');
    }
    const bucketName = postPolicy.formData.bucket;
    try {
      const region = await this.getBucketRegionAsync(bucketName);
      const date = new Date();
      const dateStr = makeDateLong(date);
      await this.checkAndRefreshCreds();
      if (!postPolicy.policy.expiration) {
        // 'expiration' is mandatory field for S3.
        // Set default expiration date of 7 days.
        const expires = new Date();
        expires.setSeconds(PRESIGN_EXPIRY_DAYS_MAX);
        postPolicy.setExpires(expires);
      }
      postPolicy.policy.conditions.push(['eq', '$x-amz-date', dateStr]);
      postPolicy.formData['x-amz-date'] = dateStr;
      postPolicy.policy.conditions.push(['eq', '$x-amz-algorithm', 'AWS4-HMAC-SHA256']);
      postPolicy.formData['x-amz-algorithm'] = 'AWS4-HMAC-SHA256';
      postPolicy.policy.conditions.push(['eq', '$x-amz-credential', this.accessKey + '/' + getScope(region, date)]);
      postPolicy.formData['x-amz-credential'] = this.accessKey + '/' + getScope(region, date);
      if (this.sessionToken) {
        postPolicy.policy.conditions.push(['eq', '$x-amz-security-token', this.sessionToken]);
        postPolicy.formData['x-amz-security-token'] = this.sessionToken;
      }
      const policyBase64 = Buffer.from(JSON.stringify(postPolicy.policy)).toString('base64');
      postPolicy.formData.policy = policyBase64;
      postPolicy.formData['x-amz-signature'] = postPresignSignatureV4(region, date, this.secretKey, policyBase64);
      const opts = {
        region: region,
        bucketName: bucketName,
        method: 'POST'
      };
      const reqOptions = this.getRequestOptions(opts);
      const portStr = this.port == 80 || this.port === 443 ? '' : `:${this.port.toString()}`;
      const urlStr = `${reqOptions.protocol}//${reqOptions.host}${portStr}${reqOptions.path}`;
      return {
        postURL: urlStr,
        formData: postPolicy.formData
      };
    } catch (err) {
      if (err instanceof errors.InvalidBucketNameError) {
        throw new errors.InvalidArgumentError(`Unable to get bucket region for ${bucketName}.`);
      }
      throw err;
    }
  }
  // list a batch of objects
  async listObjectsQuery(bucketName, prefix, marker, listQueryOpts) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!isString(prefix)) {
      throw new TypeError('prefix should be of type "string"');
    }
    if (marker && !isString(marker)) {
      throw new TypeError('marker should be of type "string"');
    }
    if (listQueryOpts && !isObject(listQueryOpts)) {
      throw new TypeError('listQueryOpts should be of type "object"');
    }
    let {
      Delimiter,
      MaxKeys,
      IncludeVersion,
      versionIdMarker,
      keyMarker
    } = listQueryOpts;
    if (!isString(Delimiter)) {
      throw new TypeError('Delimiter should be of type "string"');
    }
    if (!isNumber(MaxKeys)) {
      throw new TypeError('MaxKeys should be of type "number"');
    }
    const queries = [];
    // escape every value in query string, except maxKeys
    queries.push(`prefix=${uriEscape(prefix)}`);
    queries.push(`delimiter=${uriEscape(Delimiter)}`);
    queries.push(`encoding-type=url`);
    if (IncludeVersion) {
      queries.push(`versions`);
    }
    if (IncludeVersion) {
      // v1 version listing..
      if (keyMarker) {
        queries.push(`key-marker=${keyMarker}`);
      }
      if (versionIdMarker) {
        queries.push(`version-id-marker=${versionIdMarker}`);
      }
    } else if (marker) {
      marker = uriEscape(marker);
      queries.push(`marker=${marker}`);
    }

    // no need to escape maxKeys
    if (MaxKeys) {
      if (MaxKeys >= 1000) {
        MaxKeys = 1000;
      }
      queries.push(`max-keys=${MaxKeys}`);
    }
    queries.sort();
    let query = '';
    if (queries.length > 0) {
      query = `${queries.join('&')}`;
    }
    const method = 'GET';
    const res = await this.makeRequestAsync({
      method,
      bucketName,
      query
    });
    const body = await readAsString(res);
    const listQryList = parseListObjects(body);
    return listQryList;
  }
  listObjects(bucketName, prefix, recursive, listOpts) {
    if (prefix === undefined) {
      prefix = '';
    }
    if (recursive === undefined) {
      recursive = false;
    }
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!isValidPrefix(prefix)) {
      throw new errors.InvalidPrefixError(`Invalid prefix : ${prefix}`);
    }
    if (!isString(prefix)) {
      throw new TypeError('prefix should be of type "string"');
    }
    if (!isBoolean(recursive)) {
      throw new TypeError('recursive should be of type "boolean"');
    }
    if (listOpts && !isObject(listOpts)) {
      throw new TypeError('listOpts should be of type "object"');
    }
    let marker = '';
    let keyMarker = '';
    let versionIdMarker = '';
    let objects = [];
    let ended = false;
    const readStream = new stream.Readable({
      objectMode: true
    });
    readStream._read = async () => {
      // push one object per _read()
      if (objects.length) {
        readStream.push(objects.shift());
        return;
      }
      if (ended) {
        return readStream.push(null);
      }
      try {
        const listQueryOpts = {
          Delimiter: recursive ? '' : '/',
          // if recursive is false set delimiter to '/'
          MaxKeys: 1000,
          IncludeVersion: listOpts === null || listOpts === void 0 ? void 0 : listOpts.IncludeVersion,
          // version listing specific options
          keyMarker: keyMarker,
          versionIdMarker: versionIdMarker
        };
        const result = await this.listObjectsQuery(bucketName, prefix, marker, listQueryOpts);
        if (result.isTruncated) {
          marker = result.nextMarker || undefined;
          if (result.keyMarker) {
            keyMarker = result.keyMarker;
          }
          if (result.versionIdMarker) {
            versionIdMarker = result.versionIdMarker;
          }
        } else {
          ended = true;
        }
        if (result.objects) {
          objects = result.objects;
        }
        // @ts-ignore
        readStream._read();
      } catch (err) {
        readStream.emit('error', err);
      }
    };
    return readStream;
  }
  async listObjectsV2Query(bucketName, prefix, continuationToken, delimiter, maxKeys, startAfter) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!isString(prefix)) {
      throw new TypeError('prefix should be of type "string"');
    }
    if (!isString(continuationToken)) {
      throw new TypeError('continuationToken should be of type "string"');
    }
    if (!isString(delimiter)) {
      throw new TypeError('delimiter should be of type "string"');
    }
    if (!isNumber(maxKeys)) {
      throw new TypeError('maxKeys should be of type "number"');
    }
    if (!isString(startAfter)) {
      throw new TypeError('startAfter should be of type "string"');
    }
    const queries = [];
    queries.push(`list-type=2`);
    queries.push(`encoding-type=url`);
    queries.push(`prefix=${uriEscape(prefix)}`);
    queries.push(`delimiter=${uriEscape(delimiter)}`);
    if (continuationToken) {
      queries.push(`continuation-token=${uriEscape(continuationToken)}`);
    }
    if (startAfter) {
      queries.push(`start-after=${uriEscape(startAfter)}`);
    }
    if (maxKeys) {
      if (maxKeys >= 1000) {
        maxKeys = 1000;
      }
      queries.push(`max-keys=${maxKeys}`);
    }
    queries.sort();
    let query = '';
    if (queries.length > 0) {
      query = `${queries.join('&')}`;
    }
    const method = 'GET';
    const res = await this.makeRequestAsync({
      method,
      bucketName,
      query
    });
    const body = await readAsString(res);
    return parseListObjectsV2(body);
  }
  listObjectsV2(bucketName, prefix, recursive, startAfter) {
    if (prefix === undefined) {
      prefix = '';
    }
    if (recursive === undefined) {
      recursive = false;
    }
    if (startAfter === undefined) {
      startAfter = '';
    }
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!isValidPrefix(prefix)) {
      throw new errors.InvalidPrefixError(`Invalid prefix : ${prefix}`);
    }
    if (!isString(prefix)) {
      throw new TypeError('prefix should be of type "string"');
    }
    if (!isBoolean(recursive)) {
      throw new TypeError('recursive should be of type "boolean"');
    }
    if (!isString(startAfter)) {
      throw new TypeError('startAfter should be of type "string"');
    }
    const delimiter = recursive ? '' : '/';
    const prefixStr = prefix;
    const startAfterStr = startAfter;
    let continuationToken = '';
    let objects = [];
    let ended = false;
    const readStream = new stream.Readable({
      objectMode: true
    });
    readStream._read = async () => {
      if (objects.length) {
        readStream.push(objects.shift());
        return;
      }
      if (ended) {
        return readStream.push(null);
      }
      try {
        const result = await this.listObjectsV2Query(bucketName, prefixStr, continuationToken, delimiter, 1000, startAfterStr);
        if (result.isTruncated) {
          continuationToken = result.nextContinuationToken;
        } else {
          ended = true;
        }
        objects = result.objects;
        // @ts-ignore
        readStream._read();
      } catch (err) {
        readStream.emit('error', err);
      }
    };
    return readStream;
  }
  async setBucketNotification(bucketName, config) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!isObject(config)) {
      throw new TypeError('notification config should be of type "Object"');
    }
    const method = 'PUT';
    const query = 'notification';
    const builder = new xml2js.Builder({
      rootName: 'NotificationConfiguration',
      renderOpts: {
        pretty: false
      },
      headless: true
    });
    const payload = builder.buildObject(config);
    await this.makeRequestAsyncOmit({
      method,
      bucketName,
      query
    }, payload);
  }
  async removeAllBucketNotification(bucketName) {
    await this.setBucketNotification(bucketName, new NotificationConfig());
  }
  async getBucketNotification(bucketName) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    const method = 'GET';
    const query = 'notification';
    const res = await this.makeRequestAsync({
      method,
      bucketName,
      query
    });
    const body = await readAsString(res);
    return parseBucketNotification(body);
  }
  listenBucketNotification(bucketName, prefix, suffix, events) {
    if (!isValidBucketName(bucketName)) {
      throw new errors.InvalidBucketNameError(`Invalid bucket name: ${bucketName}`);
    }
    if (!isString(prefix)) {
      throw new TypeError('prefix must be of type string');
    }
    if (!isString(suffix)) {
      throw new TypeError('suffix must be of type string');
    }
    if (!Array.isArray(events)) {
      throw new TypeError('events must be of type Array');
    }
    const listener = new NotificationPoller(this, bucketName, prefix, suffix, events);
    listener.start();
    return listener;
  }
}
//# sourceMappingURL=data:application/json;charset=utf-8;base64,eyJ2ZXJzaW9uIjozLCJuYW1lcyI6WyJjcnlwdG8iLCJmcyIsImh0dHAiLCJodHRwcyIsInBhdGgiLCJzdHJlYW0iLCJhc3luYyIsIkJsb2NrU3RyZWFtMiIsImlzQnJvd3NlciIsIl8iLCJxcyIsInhtbDJqcyIsIkNyZWRlbnRpYWxQcm92aWRlciIsImVycm9ycyIsIkNvcHlEZXN0aW5hdGlvbk9wdGlvbnMiLCJDb3B5U291cmNlT3B0aW9ucyIsIkRFRkFVTFRfUkVHSU9OIiwiTEVHQUxfSE9MRF9TVEFUVVMiLCJQUkVTSUdOX0VYUElSWV9EQVlTX01BWCIsIlJFVEVOVElPTl9NT0RFUyIsIlJFVEVOVElPTl9WQUxJRElUWV9VTklUUyIsIk5vdGlmaWNhdGlvbkNvbmZpZyIsIk5vdGlmaWNhdGlvblBvbGxlciIsInBvc3RQcmVzaWduU2lnbmF0dXJlVjQiLCJwcmVzaWduU2lnbmF0dXJlVjQiLCJzaWduVjQiLCJmc3AiLCJzdHJlYW1Qcm9taXNlIiwiQ29weUNvbmRpdGlvbnMiLCJFeHRlbnNpb25zIiwiY2FsY3VsYXRlRXZlblNwbGl0cyIsImV4dHJhY3RNZXRhZGF0YSIsImdldENvbnRlbnRMZW5ndGgiLCJnZXRTY29wZSIsImdldFNvdXJjZVZlcnNpb25JZCIsImdldFZlcnNpb25JZCIsImhhc2hCaW5hcnkiLCJpbnNlcnRDb250ZW50VHlwZSIsImlzQW1hem9uRW5kcG9pbnQiLCJpc0Jvb2xlYW4iLCJpc0RlZmluZWQiLCJpc0VtcHR5IiwiaXNOdW1iZXIiLCJpc09iamVjdCIsImlzUGxhaW5PYmplY3QiLCJpc1JlYWRhYmxlU3RyZWFtIiwiaXNTdHJpbmciLCJpc1ZhbGlkQnVja2V0TmFtZSIsImlzVmFsaWRFbmRwb2ludCIsImlzVmFsaWRPYmplY3ROYW1lIiwiaXNWYWxpZFBvcnQiLCJpc1ZhbGlkUHJlZml4IiwiaXNWaXJ0dWFsSG9zdFN0eWxlIiwibWFrZURhdGVMb25nIiwiUEFSVF9DT05TVFJBSU5UUyIsInBhcnRzUmVxdWlyZWQiLCJwcmVwZW5kWEFNWk1ldGEiLCJyZWFkYWJsZVN0cmVhbSIsInNhbml0aXplRVRhZyIsInRvTWQ1IiwidG9TaGEyNTYiLCJ1cmlFc2NhcGUiLCJ1cmlSZXNvdXJjZUVzY2FwZSIsImpvaW5Ib3N0UG9ydCIsIlBvc3RQb2xpY3kiLCJyZXF1ZXN0V2l0aFJldHJ5IiwiZHJhaW5SZXNwb25zZSIsInJlYWRBc0J1ZmZlciIsInJlYWRBc1N0cmluZyIsImdldFMzRW5kcG9pbnQiLCJwYXJzZUJ1Y2tldE5vdGlmaWNhdGlvbiIsInBhcnNlQ29tcGxldGVNdWx0aXBhcnQiLCJwYXJzZUluaXRpYXRlTXVsdGlwYXJ0IiwicGFyc2VMaXN0T2JqZWN0cyIsInBhcnNlTGlzdE9iamVjdHNWMiIsInBhcnNlT2JqZWN0TGVnYWxIb2xkQ29uZmlnIiwicGFyc2VTZWxlY3RPYmplY3RDb250ZW50UmVzcG9uc2UiLCJ1cGxvYWRQYXJ0UGFyc2VyIiwieG1sUGFyc2VycyIsInhtbCIsIkJ1aWxkZXIiLCJyZW5kZXJPcHRzIiwicHJldHR5IiwiaGVhZGxlc3MiLCJQYWNrYWdlIiwidmVyc2lvbiIsInJlcXVlc3RPcHRpb25Qcm9wZXJ0aWVzIiwiVHlwZWRDbGllbnQiLCJwYXJ0U2l6ZSIsIm1heGltdW1QYXJ0U2l6ZSIsIm1heE9iamVjdFNpemUiLCJjb25zdHJ1Y3RvciIsInBhcmFtcyIsInNlY3VyZSIsInVuZGVmaW5lZCIsIkVycm9yIiwidXNlU1NMIiwicG9ydCIsImVuZFBvaW50IiwiSW52YWxpZEVuZHBvaW50RXJyb3IiLCJJbnZhbGlkQXJndW1lbnRFcnJvciIsInJlZ2lvbiIsImhvc3QiLCJ0b0xvd2VyQ2FzZSIsInByb3RvY29sIiwidHJhbnNwb3J0IiwidHJhbnNwb3J0QWdlbnQiLCJnbG9iYWxBZ2VudCIsImxpYnJhcnlDb21tZW50cyIsInByb2Nlc3MiLCJwbGF0Zm9ybSIsImFyY2giLCJsaWJyYXJ5QWdlbnQiLCJ1c2VyQWdlbnQiLCJwYXRoU3R5bGUiLCJhY2Nlc3NLZXkiLCJzZWNyZXRLZXkiLCJzZXNzaW9uVG9rZW4iLCJhbm9ueW1vdXMiLCJjcmVkZW50aWFsc1Byb3ZpZGVyIiwicmVnaW9uTWFwIiwib3ZlclJpZGVQYXJ0U2l6ZSIsImVuYWJsZVNIQTI1NiIsInMzQWNjZWxlcmF0ZUVuZHBvaW50IiwicmVxT3B0aW9ucyIsImNsaWVudEV4dGVuc2lvbnMiLCJyZXRyeU9wdGlvbnMiLCJkaXNhYmxlUmV0cnkiLCJleHRlbnNpb25zIiwic2V0UzNUcmFuc2ZlckFjY2VsZXJhdGUiLCJzZXRSZXF1ZXN0T3B0aW9ucyIsIm9wdGlvbnMiLCJUeXBlRXJyb3IiLCJwaWNrIiwiZ2V0QWNjZWxlcmF0ZUVuZFBvaW50SWZTZXQiLCJidWNrZXROYW1lIiwib2JqZWN0TmFtZSIsImluY2x1ZGVzIiwic2V0QXBwSW5mbyIsImFwcE5hbWUiLCJhcHBWZXJzaW9uIiwidHJpbSIsImdldFJlcXVlc3RPcHRpb25zIiwib3B0cyIsIm1ldGhvZCIsImhlYWRlcnMiLCJxdWVyeSIsImFnZW50IiwidmlydHVhbEhvc3RTdHlsZSIsImFjY2VsZXJhdGVFbmRQb2ludCIsImsiLCJ2IiwiT2JqZWN0IiwiZW50cmllcyIsImFzc2lnbiIsIm1hcFZhbHVlcyIsInBpY2tCeSIsInRvU3RyaW5nIiwic2V0Q3JlZGVudGlhbHNQcm92aWRlciIsImNoZWNrQW5kUmVmcmVzaENyZWRzIiwiY3JlZGVudGlhbHNDb25mIiwiZ2V0Q3JlZGVudGlhbHMiLCJnZXRBY2Nlc3NLZXkiLCJnZXRTZWNyZXRLZXkiLCJnZXRTZXNzaW9uVG9rZW4iLCJlIiwiY2F1c2UiLCJsb2dIVFRQIiwicmVzcG9uc2UiLCJlcnIiLCJsb2dTdHJlYW0iLCJsb2dIZWFkZXJzIiwiZm9yRWFjaCIsInJlZGFjdG9yIiwiUmVnRXhwIiwicmVwbGFjZSIsIndyaXRlIiwic3RhdHVzQ29kZSIsImVyckpTT04iLCJKU09OIiwic3RyaW5naWZ5IiwidHJhY2VPbiIsInN0ZG91dCIsInRyYWNlT2ZmIiwibWFrZVJlcXVlc3RBc3luYyIsInBheWxvYWQiLCJleHBlY3RlZENvZGVzIiwibGVuZ3RoIiwic2hhMjU2c3VtIiwibWFrZVJlcXVlc3RTdHJlYW1Bc3luYyIsIm1ha2VSZXF1ZXN0QXN5bmNPbWl0Iiwic3RhdHVzQ29kZXMiLCJyZXMiLCJib2R5IiwiQnVmZmVyIiwiaXNCdWZmZXIiLCJnZXRCdWNrZXRSZWdpb25Bc3luYyIsImRhdGUiLCJEYXRlIiwiYXV0aG9yaXphdGlvbiIsImxvZyIsIm1zZyIsIm1heGltdW1SZXRyeUNvdW50IiwiYmFzZURlbGF5TXMiLCJtYXhpbXVtRGVsYXlNcyIsInBhcnNlUmVzcG9uc2VFcnJvciIsIkludmFsaWRCdWNrZXROYW1lRXJyb3IiLCJjYWNoZWQiLCJleHRyYWN0UmVnaW9uQXN5bmMiLCJwYXJzZUJ1Y2tldFJlZ2lvbiIsIlMzRXJyb3IiLCJlcnJDb2RlIiwiY29kZSIsImVyclJlZ2lvbiIsIm5hbWUiLCJSZWdpb24iLCJtYWtlUmVxdWVzdCIsInJldHVyblJlc3BvbnNlIiwiY2IiLCJwcm9tIiwidGhlbiIsInJlc3VsdCIsIm1ha2VSZXF1ZXN0U3RyZWFtIiwiZXhlY3V0b3IiLCJnZXRCdWNrZXRSZWdpb24iLCJtYWtlQnVja2V0IiwibWFrZU9wdHMiLCJidWlsZE9iamVjdCIsIkNyZWF0ZUJ1Y2tldENvbmZpZ3VyYXRpb24iLCIkIiwieG1sbnMiLCJMb2NhdGlvbkNvbnN0cmFpbnQiLCJPYmplY3RMb2NraW5nIiwiZmluYWxSZWdpb24iLCJyZXF1ZXN0T3B0IiwiYnVja2V0RXhpc3RzIiwicmVtb3ZlQnVja2V0IiwiZ2V0T2JqZWN0IiwiZ2V0T3B0cyIsIkludmFsaWRPYmplY3ROYW1lRXJyb3IiLCJnZXRQYXJ0aWFsT2JqZWN0Iiwib2Zmc2V0IiwicmFuZ2UiLCJzc2VIZWFkZXJzIiwiU1NFQ3VzdG9tZXJBbGdvcml0aG0iLCJTU0VDdXN0b21lcktleSIsIlNTRUN1c3RvbWVyS2V5TUQ1IiwiZXhwZWN0ZWRTdGF0dXNDb2RlcyIsInB1c2giLCJmR2V0T2JqZWN0IiwiZmlsZVBhdGgiLCJkb3dubG9hZFRvVG1wRmlsZSIsInBhcnRGaWxlU3RyZWFtIiwib2JqU3RhdCIsInN0YXRPYmplY3QiLCJlbmNvZGVkRXRhZyIsImZyb20iLCJldGFnIiwicGFydEZpbGUiLCJta2RpciIsImRpcm5hbWUiLCJyZWN1cnNpdmUiLCJzdGF0cyIsInN0YXQiLCJzaXplIiwiY3JlYXRlV3JpdGVTdHJlYW0iLCJmbGFncyIsImRvd25sb2FkU3RyZWFtIiwicGlwZWxpbmUiLCJyZW5hbWUiLCJzdGF0T3B0cyIsInN0YXRPcHREZWYiLCJwYXJzZUludCIsIm1ldGFEYXRhIiwibGFzdE1vZGlmaWVkIiwidmVyc2lvbklkIiwicmVtb3ZlT2JqZWN0IiwicmVtb3ZlT3B0cyIsImdvdmVybmFuY2VCeXBhc3MiLCJmb3JjZURlbGV0ZSIsInF1ZXJ5UGFyYW1zIiwibGlzdEluY29tcGxldGVVcGxvYWRzIiwiYnVja2V0IiwicHJlZml4IiwiSW52YWxpZFByZWZpeEVycm9yIiwiZGVsaW1pdGVyIiwia2V5TWFya2VyIiwidXBsb2FkSWRNYXJrZXIiLCJ1cGxvYWRzIiwiZW5kZWQiLCJyZWFkU3RyZWFtIiwiUmVhZGFibGUiLCJvYmplY3RNb2RlIiwiX3JlYWQiLCJzaGlmdCIsImxpc3RJbmNvbXBsZXRlVXBsb2Fkc1F1ZXJ5IiwicHJlZml4ZXMiLCJlYWNoU2VyaWVzIiwidXBsb2FkIiwibGlzdFBhcnRzIiwia2V5IiwidXBsb2FkSWQiLCJwYXJ0cyIsInJlZHVjZSIsImFjYyIsIml0ZW0iLCJlbWl0IiwiaXNUcnVuY2F0ZWQiLCJuZXh0S2V5TWFya2VyIiwibmV4dFVwbG9hZElkTWFya2VyIiwicXVlcmllcyIsIm1heFVwbG9hZHMiLCJzb3J0IiwidW5zaGlmdCIsImpvaW4iLCJwYXJzZUxpc3RNdWx0aXBhcnQiLCJpbml0aWF0ZU5ld011bHRpcGFydFVwbG9hZCIsImFib3J0TXVsdGlwYXJ0VXBsb2FkIiwicmVxdWVzdE9wdGlvbnMiLCJmaW5kVXBsb2FkSWQiLCJfbGF0ZXN0VXBsb2FkIiwibGF0ZXN0VXBsb2FkIiwiaW5pdGlhdGVkIiwiZ2V0VGltZSIsImNvbXBsZXRlTXVsdGlwYXJ0VXBsb2FkIiwiZXRhZ3MiLCJidWlsZGVyIiwiQ29tcGxldGVNdWx0aXBhcnRVcGxvYWQiLCJQYXJ0IiwibWFwIiwiUGFydE51bWJlciIsInBhcnQiLCJFVGFnIiwiZXJyTWVzc2FnZSIsIm1hcmtlciIsImxpc3RQYXJ0c1F1ZXJ5IiwicGFyc2VMaXN0UGFydHMiLCJsaXN0QnVja2V0cyIsInJlZ2lvbkNvbmYiLCJodHRwUmVzIiwieG1sUmVzdWx0IiwicGFyc2VMaXN0QnVja2V0IiwiY2FsY3VsYXRlUGFydFNpemUiLCJmUHV0T2JqZWN0IiwicHV0T2JqZWN0IiwiY3JlYXRlUmVhZFN0cmVhbSIsInN0YXRTaXplIiwidXBsb2FkQnVmZmVyIiwiYnVmIiwidXBsb2FkU3RyZWFtIiwibWQ1c3VtIiwib2xkUGFydHMiLCJlVGFncyIsInByZXZpb3VzVXBsb2FkSWQiLCJvbGRUYWdzIiwiY2h1bmtpZXIiLCJ6ZXJvUGFkZGluZyIsIm8iLCJQcm9taXNlIiwiYWxsIiwicmVzb2x2ZSIsInJlamVjdCIsInBpcGUiLCJvbiIsInBhcnROdW1iZXIiLCJjaHVuayIsIm1kNSIsImNyZWF0ZUhhc2giLCJ1cGRhdGUiLCJkaWdlc3QiLCJvbGRQYXJ0IiwicmVtb3ZlQnVja2V0UmVwbGljYXRpb24iLCJzZXRCdWNrZXRSZXBsaWNhdGlvbiIsInJlcGxpY2F0aW9uQ29uZmlnIiwicm9sZSIsInJ1bGVzIiwicmVwbGljYXRpb25QYXJhbXNDb25maWciLCJSZXBsaWNhdGlvbkNvbmZpZ3VyYXRpb24iLCJSb2xlIiwiUnVsZSIsImdldEJ1Y2tldFJlcGxpY2F0aW9uIiwicGFyc2VSZXBsaWNhdGlvbkNvbmZpZyIsImdldE9iamVjdExlZ2FsSG9sZCIsImtleXMiLCJzdHJSZXMiLCJzZXRPYmplY3RMZWdhbEhvbGQiLCJzZXRPcHRzIiwic3RhdHVzIiwiRU5BQkxFRCIsIkRJU0FCTEVEIiwiY29uZmlnIiwiU3RhdHVzIiwicm9vdE5hbWUiLCJnZXRCdWNrZXRUYWdnaW5nIiwicGFyc2VUYWdnaW5nIiwiZ2V0T2JqZWN0VGFnZ2luZyIsInNldEJ1Y2tldFBvbGljeSIsInBvbGljeSIsIkludmFsaWRCdWNrZXRQb2xpY3lFcnJvciIsImdldEJ1Y2tldFBvbGljeSIsInB1dE9iamVjdFJldGVudGlvbiIsInJldGVudGlvbk9wdHMiLCJtb2RlIiwiQ09NUExJQU5DRSIsIkdPVkVSTkFOQ0UiLCJyZXRhaW5VbnRpbERhdGUiLCJNb2RlIiwiUmV0YWluVW50aWxEYXRlIiwiZ2V0T2JqZWN0TG9ja0NvbmZpZyIsInBhcnNlT2JqZWN0TG9ja0NvbmZpZyIsInNldE9iamVjdExvY2tDb25maWciLCJsb2NrQ29uZmlnT3B0cyIsInJldGVudGlvbk1vZGVzIiwidmFsaWRVbml0cyIsIkRBWVMiLCJZRUFSUyIsInVuaXQiLCJ2YWxpZGl0eSIsIk9iamVjdExvY2tFbmFibGVkIiwiY29uZmlnS2V5cyIsImlzQWxsS2V5c1NldCIsImV2ZXJ5IiwibGNrIiwiRGVmYXVsdFJldGVudGlvbiIsIkRheXMiLCJZZWFycyIsImdldEJ1Y2tldFZlcnNpb25pbmciLCJwYXJzZUJ1Y2tldFZlcnNpb25pbmdDb25maWciLCJzZXRCdWNrZXRWZXJzaW9uaW5nIiwidmVyc2lvbkNvbmZpZyIsInNldFRhZ2dpbmciLCJ0YWdnaW5nUGFyYW1zIiwidGFncyIsInB1dE9wdHMiLCJ0YWdzTGlzdCIsInZhbHVlIiwiS2V5IiwiVmFsdWUiLCJ0YWdnaW5nQ29uZmlnIiwiVGFnZ2luZyIsIlRhZ1NldCIsIlRhZyIsInBheWxvYWRCdWYiLCJyZW1vdmVUYWdnaW5nIiwic2V0QnVja2V0VGFnZ2luZyIsInJlbW92ZUJ1Y2tldFRhZ2dpbmciLCJzZXRPYmplY3RUYWdnaW5nIiwicmVtb3ZlT2JqZWN0VGFnZ2luZyIsInNlbGVjdE9iamVjdENvbnRlbnQiLCJzZWxlY3RPcHRzIiwiZXhwcmVzc2lvbiIsImlucHV0U2VyaWFsaXphdGlvbiIsIm91dHB1dFNlcmlhbGl6YXRpb24iLCJFeHByZXNzaW9uIiwiRXhwcmVzc2lvblR5cGUiLCJleHByZXNzaW9uVHlwZSIsIklucHV0U2VyaWFsaXphdGlvbiIsIk91dHB1dFNlcmlhbGl6YXRpb24iLCJyZXF1ZXN0UHJvZ3Jlc3MiLCJSZXF1ZXN0UHJvZ3Jlc3MiLCJzY2FuUmFuZ2UiLCJTY2FuUmFuZ2UiLCJhcHBseUJ1Y2tldExpZmVjeWNsZSIsInBvbGljeUNvbmZpZyIsInJlbW92ZUJ1Y2tldExpZmVjeWNsZSIsInNldEJ1Y2tldExpZmVjeWNsZSIsImxpZmVDeWNsZUNvbmZpZyIsImdldEJ1Y2tldExpZmVjeWNsZSIsInBhcnNlTGlmZWN5Y2xlQ29uZmlnIiwic2V0QnVja2V0RW5jcnlwdGlvbiIsImVuY3J5cHRpb25Db25maWciLCJlbmNyeXB0aW9uT2JqIiwiQXBwbHlTZXJ2ZXJTaWRlRW5jcnlwdGlvbkJ5RGVmYXVsdCIsIlNTRUFsZ29yaXRobSIsImdldEJ1Y2tldEVuY3J5cHRpb24iLCJwYXJzZUJ1Y2tldEVuY3J5cHRpb25Db25maWciLCJyZW1vdmVCdWNrZXRFbmNyeXB0aW9uIiwiZ2V0T2JqZWN0UmV0ZW50aW9uIiwicGFyc2VPYmplY3RSZXRlbnRpb25Db25maWciLCJyZW1vdmVPYmplY3RzIiwib2JqZWN0c0xpc3QiLCJBcnJheSIsImlzQXJyYXkiLCJydW5EZWxldGVPYmplY3RzIiwiYmF0Y2giLCJkZWxPYmplY3RzIiwiVmVyc2lvbklkIiwicmVtT2JqZWN0cyIsIkRlbGV0ZSIsIlF1aWV0IiwicmVtb3ZlT2JqZWN0c1BhcnNlciIsIm1heEVudHJpZXMiLCJiYXRjaGVzIiwiaSIsInNsaWNlIiwiYmF0Y2hSZXN1bHRzIiwiZmxhdCIsInJlbW92ZUluY29tcGxldGVVcGxvYWQiLCJJc1ZhbGlkQnVja2V0TmFtZUVycm9yIiwicmVtb3ZlVXBsb2FkSWQiLCJjb3B5T2JqZWN0VjEiLCJ0YXJnZXRCdWNrZXROYW1lIiwidGFyZ2V0T2JqZWN0TmFtZSIsInNvdXJjZUJ1Y2tldE5hbWVBbmRPYmplY3ROYW1lIiwiY29uZGl0aW9ucyIsIm1vZGlmaWVkIiwidW5tb2RpZmllZCIsIm1hdGNoRVRhZyIsIm1hdGNoRVRhZ0V4Y2VwdCIsInBhcnNlQ29weU9iamVjdCIsImNvcHlPYmplY3RWMiIsInNvdXJjZUNvbmZpZyIsImRlc3RDb25maWciLCJ2YWxpZGF0ZSIsImdldEhlYWRlcnMiLCJCdWNrZXQiLCJjb3B5UmVzIiwicmVzSGVhZGVycyIsInNpemVIZWFkZXJWYWx1ZSIsIkxhc3RNb2RpZmllZCIsIk1ldGFEYXRhIiwiU291cmNlVmVyc2lvbklkIiwiRXRhZyIsIlNpemUiLCJjb3B5T2JqZWN0IiwiYWxsQXJncyIsInNvdXJjZSIsImRlc3QiLCJ1cGxvYWRQYXJ0IiwicGFydENvbmZpZyIsInVwbG9hZElEIiwicGFydFJlcyIsInBhcnRFdGFnVmFsIiwiY29tcG9zZU9iamVjdCIsImRlc3RPYmpDb25maWciLCJzb3VyY2VPYmpMaXN0IiwibWF4Q29uY3VycmVuY3kiLCJzb3VyY2VGaWxlc0xlbmd0aCIsIk1BWF9QQVJUU19DT1VOVCIsInNPYmoiLCJnZXRTdGF0T3B0aW9ucyIsInNyY0NvbmZpZyIsIlZlcnNpb25JRCIsInNyY09iamVjdFNpemVzIiwidG90YWxTaXplIiwidG90YWxQYXJ0cyIsInNvdXJjZU9ialN0YXRzIiwic3JjSXRlbSIsInNyY09iamVjdEluZm9zIiwidmFsaWRhdGVkU3RhdHMiLCJyZXNJdGVtU3RhdCIsImluZGV4Iiwic3JjQ29weVNpemUiLCJNYXRjaFJhbmdlIiwic3JjU3RhcnQiLCJTdGFydCIsInNyY0VuZCIsIkVuZCIsIkFCU19NSU5fUEFSVF9TSVpFIiwiTUFYX01VTFRJUEFSVF9QVVRfT0JKRUNUX1NJWkUiLCJNQVhfUEFSVF9TSVpFIiwiTWF0Y2hFVGFnIiwic3BsaXRQYXJ0U2l6ZUxpc3QiLCJpZHgiLCJnZXRVcGxvYWRQYXJ0Q29uZmlnTGlzdCIsInVwbG9hZFBhcnRDb25maWdMaXN0Iiwic3BsaXRTaXplIiwic3BsaXRJbmRleCIsInN0YXJ0SW5kZXgiLCJzdGFydElkeCIsImVuZEluZGV4IiwiZW5kSWR4IiwicGFydEluZGV4IiwidG90YWxVcGxvYWRzIiwic3BsaXRTdGFydCIsInVwbGRDdHJJZHgiLCJzcGxpdEVuZCIsInVwbG9hZFBhcnRDb25maWciLCJ1cGxvYWRBbGxQYXJ0cyIsInVwbG9hZExpc3QiLCJwYXJ0VXBsb2FkcyIsInBlcmZvcm1VcGxvYWRQYXJ0cyIsInBhcnRzUmVzIiwicGFydENvcHkiLCJuZXdVcGxvYWRIZWFkZXJzIiwicGFydHNEb25lIiwicHJlc2lnbmVkVXJsIiwiZXhwaXJlcyIsInJlcVBhcmFtcyIsInJlcXVlc3REYXRlIiwiX3JlcXVlc3REYXRlIiwiQW5vbnltb3VzUmVxdWVzdEVycm9yIiwiaXNOYU4iLCJwcmVzaWduZWRHZXRPYmplY3QiLCJyZXNwSGVhZGVycyIsInZhbGlkUmVzcEhlYWRlcnMiLCJoZWFkZXIiLCJwcmVzaWduZWRQdXRPYmplY3QiLCJuZXdQb3N0UG9saWN5IiwicHJlc2lnbmVkUG9zdFBvbGljeSIsInBvc3RQb2xpY3kiLCJmb3JtRGF0YSIsImRhdGVTdHIiLCJleHBpcmF0aW9uIiwic2V0U2Vjb25kcyIsInNldEV4cGlyZXMiLCJwb2xpY3lCYXNlNjQiLCJwb3J0U3RyIiwidXJsU3RyIiwicG9zdFVSTCIsImxpc3RPYmplY3RzUXVlcnkiLCJsaXN0UXVlcnlPcHRzIiwiRGVsaW1pdGVyIiwiTWF4S2V5cyIsIkluY2x1ZGVWZXJzaW9uIiwidmVyc2lvbklkTWFya2VyIiwibGlzdFFyeUxpc3QiLCJsaXN0T2JqZWN0cyIsImxpc3RPcHRzIiwib2JqZWN0cyIsIm5leHRNYXJrZXIiLCJsaXN0T2JqZWN0c1YyUXVlcnkiLCJjb250aW51YXRpb25Ub2tlbiIsIm1heEtleXMiLCJzdGFydEFmdGVyIiwibGlzdE9iamVjdHNWMiIsInByZWZpeFN0ciIsInN0YXJ0QWZ0ZXJTdHIiLCJuZXh0Q29udGludWF0aW9uVG9rZW4iLCJzZXRCdWNrZXROb3RpZmljYXRpb24iLCJyZW1vdmVBbGxCdWNrZXROb3RpZmljYXRpb24iLCJnZXRCdWNrZXROb3RpZmljYXRpb24iLCJsaXN0ZW5CdWNrZXROb3RpZmljYXRpb24iLCJzdWZmaXgiLCJldmVudHMiLCJsaXN0ZW5lciIsInN0YXJ0Il0sInNvdXJjZXMiOlsiY2xpZW50LnRzIl0sInNvdXJjZXNDb250ZW50IjpbImltcG9ydCAqIGFzIGNyeXB0byBmcm9tICdub2RlOmNyeXB0bydcbmltcG9ydCAqIGFzIGZzIGZyb20gJ25vZGU6ZnMnXG5pbXBvcnQgdHlwZSB7IEluY29taW5nSHR0cEhlYWRlcnMgfSBmcm9tICdub2RlOmh0dHAnXG5pbXBvcnQgKiBhcyBodHRwIGZyb20gJ25vZGU6aHR0cCdcbmltcG9ydCAqIGFzIGh0dHBzIGZyb20gJ25vZGU6aHR0cHMnXG5pbXBvcnQgKiBhcyBwYXRoIGZyb20gJ25vZGU6cGF0aCdcbmltcG9ydCAqIGFzIHN0cmVhbSBmcm9tICdub2RlOnN0cmVhbSdcblxuaW1wb3J0ICogYXMgYXN5bmMgZnJvbSAnYXN5bmMnXG5pbXBvcnQgQmxvY2tTdHJlYW0yIGZyb20gJ2Jsb2NrLXN0cmVhbTInXG5pbXBvcnQgeyBpc0Jyb3dzZXIgfSBmcm9tICdicm93c2VyLW9yLW5vZGUnXG5pbXBvcnQgXyBmcm9tICdsb2Rhc2gnXG5pbXBvcnQgcXMgZnJvbSAncXVlcnktc3RyaW5nJ1xuaW1wb3J0IHhtbDJqcyBmcm9tICd4bWwyanMnXG5cbmltcG9ydCB7IENyZWRlbnRpYWxQcm92aWRlciB9IGZyb20gJy4uL0NyZWRlbnRpYWxQcm92aWRlci50cydcbmltcG9ydCAqIGFzIGVycm9ycyBmcm9tICcuLi9lcnJvcnMudHMnXG5pbXBvcnQgdHlwZSB7IFNlbGVjdFJlc3VsdHMgfSBmcm9tICcuLi9oZWxwZXJzLnRzJ1xuaW1wb3J0IHtcbiAgQ29weURlc3RpbmF0aW9uT3B0aW9ucyxcbiAgQ29weVNvdXJjZU9wdGlvbnMsXG4gIERFRkFVTFRfUkVHSU9OLFxuICBMRUdBTF9IT0xEX1NUQVRVUyxcbiAgUFJFU0lHTl9FWFBJUllfREFZU19NQVgsXG4gIFJFVEVOVElPTl9NT0RFUyxcbiAgUkVURU5USU9OX1ZBTElESVRZX1VOSVRTLFxufSBmcm9tICcuLi9oZWxwZXJzLnRzJ1xuaW1wb3J0IHR5cGUgeyBOb3RpZmljYXRpb25FdmVudCB9IGZyb20gJy4uL25vdGlmaWNhdGlvbi50cydcbmltcG9ydCB7IE5vdGlmaWNhdGlvbkNvbmZpZywgTm90aWZpY2F0aW9uUG9sbGVyIH0gZnJvbSAnLi4vbm90aWZpY2F0aW9uLnRzJ1xuaW1wb3J0IHsgcG9zdFByZXNpZ25TaWduYXR1cmVWNCwgcHJlc2lnblNpZ25hdHVyZVY0LCBzaWduVjQgfSBmcm9tICcuLi9zaWduaW5nLnRzJ1xuaW1wb3J0IHsgZnNwLCBzdHJlYW1Qcm9taXNlIH0gZnJvbSAnLi9hc3luYy50cydcbmltcG9ydCB7IENvcHlDb25kaXRpb25zIH0gZnJvbSAnLi9jb3B5LWNvbmRpdGlvbnMudHMnXG5pbXBvcnQgeyBFeHRlbnNpb25zIH0gZnJvbSAnLi9leHRlbnNpb25zLnRzJ1xuaW1wb3J0IHtcbiAgY2FsY3VsYXRlRXZlblNwbGl0cyxcbiAgZXh0cmFjdE1ldGFkYXRhLFxuICBnZXRDb250ZW50TGVuZ3RoLFxuICBnZXRTY29wZSxcbiAgZ2V0U291cmNlVmVyc2lvbklkLFxuICBnZXRWZXJzaW9uSWQsXG4gIGhhc2hCaW5hcnksXG4gIGluc2VydENvbnRlbnRUeXBlLFxuICBpc0FtYXpvbkVuZHBvaW50LFxuICBpc0Jvb2xlYW4sXG4gIGlzRGVmaW5lZCxcbiAgaXNFbXB0eSxcbiAgaXNOdW1iZXIsXG4gIGlzT2JqZWN0LFxuICBpc1BsYWluT2JqZWN0LFxuICBpc1JlYWRhYmxlU3RyZWFtLFxuICBpc1N0cmluZyxcbiAgaXNWYWxpZEJ1Y2tldE5hbWUsXG4gIGlzVmFsaWRFbmRwb2ludCxcbiAgaXNWYWxpZE9iamVjdE5hbWUsXG4gIGlzVmFsaWRQb3J0LFxuICBpc1ZhbGlkUHJlZml4LFxuICBpc1ZpcnR1YWxIb3N0U3R5bGUsXG4gIG1ha2VEYXRlTG9uZyxcbiAgUEFSVF9DT05TVFJBSU5UUyxcbiAgcGFydHNSZXF1aXJlZCxcbiAgcHJlcGVuZFhBTVpNZXRhLFxuICByZWFkYWJsZVN0cmVhbSxcbiAgc2FuaXRpemVFVGFnLFxuICB0b01kNSxcbiAgdG9TaGEyNTYsXG4gIHVyaUVzY2FwZSxcbiAgdXJpUmVzb3VyY2VFc2NhcGUsXG59IGZyb20gJy4vaGVscGVyLnRzJ1xuaW1wb3J0IHsgam9pbkhvc3RQb3J0IH0gZnJvbSAnLi9qb2luLWhvc3QtcG9ydC50cydcbmltcG9ydCB7IFBvc3RQb2xpY3kgfSBmcm9tICcuL3Bvc3QtcG9saWN5LnRzJ1xuaW1wb3J0IHsgcmVxdWVzdFdpdGhSZXRyeSB9IGZyb20gJy4vcmVxdWVzdC50cydcbmltcG9ydCB7IGRyYWluUmVzcG9uc2UsIHJlYWRBc0J1ZmZlciwgcmVhZEFzU3RyaW5nIH0gZnJvbSAnLi9yZXNwb25zZS50cydcbmltcG9ydCB0eXBlIHsgUmVnaW9uIH0gZnJvbSAnLi9zMy1lbmRwb2ludHMudHMnXG5pbXBvcnQgeyBnZXRTM0VuZHBvaW50IH0gZnJvbSAnLi9zMy1lbmRwb2ludHMudHMnXG5pbXBvcnQgdHlwZSB7XG4gIEJpbmFyeSxcbiAgQnVja2V0SXRlbSxcbiAgQnVja2V0SXRlbUZyb21MaXN0LFxuICBCdWNrZXRJdGVtU3RhdCxcbiAgQnVja2V0U3RyZWFtLFxuICBCdWNrZXRWZXJzaW9uaW5nQ29uZmlndXJhdGlvbixcbiAgQ29weU9iamVjdFBhcmFtcyxcbiAgQ29weU9iamVjdFJlc3VsdCxcbiAgQ29weU9iamVjdFJlc3VsdFYyLFxuICBFbmNyeXB0aW9uQ29uZmlnLFxuICBHZXRPYmplY3RMZWdhbEhvbGRPcHRpb25zLFxuICBHZXRPYmplY3RPcHRzLFxuICBHZXRPYmplY3RSZXRlbnRpb25PcHRzLFxuICBJbmNvbXBsZXRlVXBsb2FkZWRCdWNrZXRJdGVtLFxuICBJUmVxdWVzdCxcbiAgSXRlbUJ1Y2tldE1ldGFkYXRhLFxuICBMaWZlY3ljbGVDb25maWcsXG4gIExpZmVDeWNsZUNvbmZpZ1BhcmFtLFxuICBMaXN0T2JqZWN0UXVlcnlPcHRzLFxuICBMaXN0T2JqZWN0UXVlcnlSZXMsXG4gIExpc3RPYmplY3RWMlJlcyxcbiAgTm90aWZpY2F0aW9uQ29uZmlnUmVzdWx0LFxuICBPYmplY3RJbmZvLFxuICBPYmplY3RMb2NrQ29uZmlnUGFyYW0sXG4gIE9iamVjdExvY2tJbmZvLFxuICBPYmplY3RNZXRhRGF0YSxcbiAgT2JqZWN0UmV0ZW50aW9uSW5mbyxcbiAgUG9zdFBvbGljeVJlc3VsdCxcbiAgUHJlU2lnblJlcXVlc3RQYXJhbXMsXG4gIFB1dE9iamVjdExlZ2FsSG9sZE9wdGlvbnMsXG4gIFB1dFRhZ2dpbmdQYXJhbXMsXG4gIFJlbW92ZU9iamVjdHNQYXJhbSxcbiAgUmVtb3ZlT2JqZWN0c1JlcXVlc3RFbnRyeSxcbiAgUmVtb3ZlT2JqZWN0c1Jlc3BvbnNlLFxuICBSZW1vdmVUYWdnaW5nUGFyYW1zLFxuICBSZXBsaWNhdGlvbkNvbmZpZyxcbiAgUmVwbGljYXRpb25Db25maWdPcHRzLFxuICBSZXF1ZXN0SGVhZGVycyxcbiAgUmVzcG9uc2VIZWFkZXIsXG4gIFJlc3VsdENhbGxiYWNrLFxuICBSZXRlbnRpb24sXG4gIFNlbGVjdE9wdGlvbnMsXG4gIFN0YXRPYmplY3RPcHRzLFxuICBUYWcsXG4gIFRhZ2dpbmdPcHRzLFxuICBUYWdzLFxuICBUcmFuc3BvcnQsXG4gIFVwbG9hZGVkT2JqZWN0SW5mbyxcbiAgVXBsb2FkUGFydENvbmZpZyxcbn0gZnJvbSAnLi90eXBlLnRzJ1xuaW1wb3J0IHR5cGUgeyBMaXN0TXVsdGlwYXJ0UmVzdWx0LCBVcGxvYWRlZFBhcnQgfSBmcm9tICcuL3htbC1wYXJzZXIudHMnXG5pbXBvcnQge1xuICBwYXJzZUJ1Y2tldE5vdGlmaWNhdGlvbixcbiAgcGFyc2VDb21wbGV0ZU11bHRpcGFydCxcbiAgcGFyc2VJbml0aWF0ZU11bHRpcGFydCxcbiAgcGFyc2VMaXN0T2JqZWN0cyxcbiAgcGFyc2VMaXN0T2JqZWN0c1YyLFxuICBwYXJzZU9iamVjdExlZ2FsSG9sZENvbmZpZyxcbiAgcGFyc2VTZWxlY3RPYmplY3RDb250ZW50UmVzcG9uc2UsXG4gIHVwbG9hZFBhcnRQYXJzZXIsXG59IGZyb20gJy4veG1sLXBhcnNlci50cydcbmltcG9ydCAqIGFzIHhtbFBhcnNlcnMgZnJvbSAnLi94bWwtcGFyc2VyLnRzJ1xuXG5jb25zdCB4bWwgPSBuZXcgeG1sMmpzLkJ1aWxkZXIoeyByZW5kZXJPcHRzOiB7IHByZXR0eTogZmFsc2UgfSwgaGVhZGxlc3M6IHRydWUgfSlcblxuLy8gd2lsbCBiZSByZXBsYWNlZCBieSBidW5kbGVyLlxuY29uc3QgUGFja2FnZSA9IHsgdmVyc2lvbjogcHJvY2Vzcy5lbnYuTUlOSU9fSlNfUEFDS0FHRV9WRVJTSU9OIHx8ICdkZXZlbG9wbWVudCcgfVxuXG5jb25zdCByZXF1ZXN0T3B0aW9uUHJvcGVydGllcyA9IFtcbiAgJ2FnZW50JyxcbiAgJ2NhJyxcbiAgJ2NlcnQnLFxuICAnY2lwaGVycycsXG4gICdjbGllbnRDZXJ0RW5naW5lJyxcbiAgJ2NybCcsXG4gICdkaHBhcmFtJyxcbiAgJ2VjZGhDdXJ2ZScsXG4gICdmYW1pbHknLFxuICAnaG9ub3JDaXBoZXJPcmRlcicsXG4gICdrZXknLFxuICAncGFzc3BocmFzZScsXG4gICdwZngnLFxuICAncmVqZWN0VW5hdXRob3JpemVkJyxcbiAgJ3NlY3VyZU9wdGlvbnMnLFxuICAnc2VjdXJlUHJvdG9jb2wnLFxuICAnc2VydmVybmFtZScsXG4gICdzZXNzaW9uSWRDb250ZXh0Jyxcbl0gYXMgY29uc3RcblxuZXhwb3J0IGludGVyZmFjZSBSZXRyeU9wdGlvbnMge1xuICAvKipcbiAgICogSWYgdGhpcyBzZXQgdG8gdHJ1ZSwgaXQgd2lsbCB0YWtlIHByZWNlZGVuY2Ugb3ZlciBhbGwgb3RoZXIgcmV0cnkgb3B0aW9ucy5cbiAgICogQGRlZmF1bHQgZmFsc2VcbiAgICovXG4gIGRpc2FibGVSZXRyeT86IGJvb2xlYW5cbiAgLyoqXG4gICAqIFRoZSBtYXhpbXVtIGFtb3VudCBvZiByZXRyaWVzIGZvciBhIHJlcXVlc3QuXG4gICAqIEBkZWZhdWx0IDFcbiAgICovXG4gIG1heGltdW1SZXRyeUNvdW50PzogbnVtYmVyXG4gIC8qKlxuICAgKiBUaGUgbWluaW11bSBkdXJhdGlvbiAoaW4gbWlsbGlzZWNvbmRzKSBmb3IgdGhlIGV4cG9uZW50aWFsIGJhY2tvZmYgYWxnb3JpdGhtLlxuICAgKiBAZGVmYXVsdCAxMDBcbiAgICovXG4gIGJhc2VEZWxheU1zPzogbnVtYmVyXG4gIC8qKlxuICAgKiBUaGUgbWF4aW11bSBkdXJhdGlvbiAoaW4gbWlsbGlzZWNvbmRzKSBmb3IgdGhlIGV4cG9uZW50aWFsIGJhY2tvZmYgYWxnb3JpdGhtLlxuICAgKiBAZGVmYXVsdCA2MDAwMFxuICAgKi9cbiAgbWF4aW11bURlbGF5TXM/OiBudW1iZXJcbn1cblxuZXhwb3J0IGludGVyZmFjZSBDbGllbnRPcHRpb25zIHtcbiAgZW5kUG9pbnQ6IHN0cmluZ1xuICBhY2Nlc3NLZXk/OiBzdHJpbmdcbiAgc2VjcmV0S2V5Pzogc3RyaW5nXG4gIHVzZVNTTD86IGJvb2xlYW5cbiAgcG9ydD86IG51bWJlclxuICByZWdpb24/OiBSZWdpb25cbiAgdHJhbnNwb3J0PzogVHJhbnNwb3J0XG4gIHNlc3Npb25Ub2tlbj86IHN0cmluZ1xuICBwYXJ0U2l6ZT86IG51bWJlclxuICBwYXRoU3R5bGU/OiBib29sZWFuXG4gIGNyZWRlbnRpYWxzUHJvdmlkZXI/OiBDcmVkZW50aWFsUHJvdmlkZXJcbiAgczNBY2NlbGVyYXRlRW5kcG9pbnQ/OiBzdHJpbmdcbiAgdHJhbnNwb3J0QWdlbnQ/OiBodHRwLkFnZW50XG4gIHJldHJ5T3B0aW9ucz86IFJldHJ5T3B0aW9uc1xufVxuXG5leHBvcnQgdHlwZSBSZXF1ZXN0T3B0aW9uID0gUGFydGlhbDxJUmVxdWVzdD4gJiB7XG4gIG1ldGhvZDogc3RyaW5nXG4gIGJ1Y2tldE5hbWU/OiBzdHJpbmdcbiAgb2JqZWN0TmFtZT86IHN0cmluZ1xuICBxdWVyeT86IHN0cmluZ1xuICBwYXRoU3R5bGU/OiBib29sZWFuXG59XG5cbmV4cG9ydCB0eXBlIE5vUmVzdWx0Q2FsbGJhY2sgPSAoZXJyb3I6IHVua25vd24pID0+IHZvaWRcblxuZXhwb3J0IGludGVyZmFjZSBNYWtlQnVja2V0T3B0IHtcbiAgT2JqZWN0TG9ja2luZz86IGJvb2xlYW5cbn1cblxuZXhwb3J0IGludGVyZmFjZSBSZW1vdmVPcHRpb25zIHtcbiAgdmVyc2lvbklkPzogc3RyaW5nXG4gIGdvdmVybmFuY2VCeXBhc3M/OiBib29sZWFuXG4gIGZvcmNlRGVsZXRlPzogYm9vbGVhblxufVxuXG50eXBlIFBhcnQgPSB7XG4gIHBhcnQ6IG51bWJlclxuICBldGFnOiBzdHJpbmdcbn1cblxuZXhwb3J0IGNsYXNzIFR5cGVkQ2xpZW50IHtcbiAgcHJvdGVjdGVkIHRyYW5zcG9ydDogVHJhbnNwb3J0XG4gIHByb3RlY3RlZCBob3N0OiBzdHJpbmdcbiAgcHJvdGVjdGVkIHBvcnQ6IG51bWJlclxuICBwcm90ZWN0ZWQgcHJvdG9jb2w6IHN0cmluZ1xuICBwcm90ZWN0ZWQgYWNjZXNzS2V5OiBzdHJpbmdcbiAgcHJvdGVjdGVkIHNlY3JldEtleTogc3RyaW5nXG4gIHByb3RlY3RlZCBzZXNzaW9uVG9rZW4/OiBzdHJpbmdcbiAgcHJvdGVjdGVkIHVzZXJBZ2VudDogc3RyaW5nXG4gIHByb3RlY3RlZCBhbm9ueW1vdXM6IGJvb2xlYW5cbiAgcHJvdGVjdGVkIHBhdGhTdHlsZTogYm9vbGVhblxuICBwcm90ZWN0ZWQgcmVnaW9uTWFwOiBSZWNvcmQ8c3RyaW5nLCBzdHJpbmc+XG4gIHB1YmxpYyByZWdpb24/OiBzdHJpbmdcbiAgcHJvdGVjdGVkIGNyZWRlbnRpYWxzUHJvdmlkZXI/OiBDcmVkZW50aWFsUHJvdmlkZXJcbiAgcGFydFNpemU6IG51bWJlciA9IDY0ICogMTAyNCAqIDEwMjRcbiAgcHJvdGVjdGVkIG92ZXJSaWRlUGFydFNpemU/OiBib29sZWFuXG4gIHByb3RlY3RlZCByZXRyeU9wdGlvbnM6IFJldHJ5T3B0aW9uc1xuXG4gIHByb3RlY3RlZCBtYXhpbXVtUGFydFNpemUgPSA1ICogMTAyNCAqIDEwMjQgKiAxMDI0XG4gIHByb3RlY3RlZCBtYXhPYmplY3RTaXplID0gNSAqIDEwMjQgKiAxMDI0ICogMTAyNCAqIDEwMjRcbiAgcHVibGljIGVuYWJsZVNIQTI1NjogYm9vbGVhblxuICBwcm90ZWN0ZWQgczNBY2NlbGVyYXRlRW5kcG9pbnQ/OiBzdHJpbmdcbiAgcHJvdGVjdGVkIHJlcU9wdGlvbnM6IFJlY29yZDxzdHJpbmcsIHVua25vd24+XG5cbiAgcHJvdGVjdGVkIHRyYW5zcG9ydEFnZW50OiBodHRwLkFnZW50XG4gIHByaXZhdGUgcmVhZG9ubHkgY2xpZW50RXh0ZW5zaW9uczogRXh0ZW5zaW9uc1xuXG4gIGNvbnN0cnVjdG9yKHBhcmFtczogQ2xpZW50T3B0aW9ucykge1xuICAgIC8vIEB0cy1leHBlY3QtZXJyb3IgZGVwcmVjYXRlZCBwcm9wZXJ0eVxuICAgIGlmIChwYXJhbXMuc2VjdXJlICE9PSB1bmRlZmluZWQpIHtcbiAgICAgIHRocm93IG5ldyBFcnJvcignXCJzZWN1cmVcIiBvcHRpb24gZGVwcmVjYXRlZCwgXCJ1c2VTU0xcIiBzaG91bGQgYmUgdXNlZCBpbnN0ZWFkJylcbiAgICB9XG4gICAgLy8gRGVmYXVsdCB2YWx1ZXMgaWYgbm90IHNwZWNpZmllZC5cbiAgICBpZiAocGFyYW1zLnVzZVNTTCA9PT0gdW5kZWZpbmVkKSB7XG4gICAgICBwYXJhbXMudXNlU1NMID0gdHJ1ZVxuICAgIH1cbiAgICBpZiAoIXBhcmFtcy5wb3J0KSB7XG4gICAgICBwYXJhbXMucG9ydCA9IDBcbiAgICB9XG4gICAgLy8gVmFsaWRhdGUgaW5wdXQgcGFyYW1zLlxuICAgIGlmICghaXNWYWxpZEVuZHBvaW50KHBhcmFtcy5lbmRQb2ludCkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEVuZHBvaW50RXJyb3IoYEludmFsaWQgZW5kUG9pbnQgOiAke3BhcmFtcy5lbmRQb2ludH1gKVxuICAgIH1cbiAgICBpZiAoIWlzVmFsaWRQb3J0KHBhcmFtcy5wb3J0KSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQXJndW1lbnRFcnJvcihgSW52YWxpZCBwb3J0IDogJHtwYXJhbXMucG9ydH1gKVxuICAgIH1cbiAgICBpZiAoIWlzQm9vbGVhbihwYXJhbXMudXNlU1NMKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQXJndW1lbnRFcnJvcihcbiAgICAgICAgYEludmFsaWQgdXNlU1NMIGZsYWcgdHlwZSA6ICR7cGFyYW1zLnVzZVNTTH0sIGV4cGVjdGVkIHRvIGJlIG9mIHR5cGUgXCJib29sZWFuXCJgLFxuICAgICAgKVxuICAgIH1cblxuICAgIC8vIFZhbGlkYXRlIHJlZ2lvbiBvbmx5IGlmIGl0cyBzZXQuXG4gICAgaWYgKHBhcmFtcy5yZWdpb24pIHtcbiAgICAgIGlmICghaXNTdHJpbmcocGFyYW1zLnJlZ2lvbikpIHtcbiAgICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQXJndW1lbnRFcnJvcihgSW52YWxpZCByZWdpb24gOiAke3BhcmFtcy5yZWdpb259YClcbiAgICAgIH1cbiAgICB9XG5cbiAgICBjb25zdCBob3N0ID0gcGFyYW1zLmVuZFBvaW50LnRvTG93ZXJDYXNlKClcbiAgICBsZXQgcG9ydCA9IHBhcmFtcy5wb3J0XG4gICAgbGV0IHByb3RvY29sOiBzdHJpbmdcbiAgICBsZXQgdHJhbnNwb3J0XG4gICAgbGV0IHRyYW5zcG9ydEFnZW50OiBodHRwLkFnZW50XG4gICAgLy8gVmFsaWRhdGUgaWYgY29uZmlndXJhdGlvbiBpcyBub3QgdXNpbmcgU1NMXG4gICAgLy8gZm9yIGNvbnN0cnVjdGluZyByZWxldmFudCBlbmRwb2ludHMuXG4gICAgaWYgKHBhcmFtcy51c2VTU0wpIHtcbiAgICAgIC8vIERlZmF1bHRzIHRvIHNlY3VyZS5cbiAgICAgIHRyYW5zcG9ydCA9IGh0dHBzXG4gICAgICBwcm90b2NvbCA9ICdodHRwczonXG4gICAgICBwb3J0ID0gcG9ydCB8fCA0NDNcbiAgICAgIHRyYW5zcG9ydEFnZW50ID0gaHR0cHMuZ2xvYmFsQWdlbnRcbiAgICB9IGVsc2Uge1xuICAgICAgdHJhbnNwb3J0ID0gaHR0cFxuICAgICAgcHJvdG9jb2wgPSAnaHR0cDonXG4gICAgICBwb3J0ID0gcG9ydCB8fCA4MFxuICAgICAgdHJhbnNwb3J0QWdlbnQgPSBodHRwLmdsb2JhbEFnZW50XG4gICAgfVxuXG4gICAgLy8gaWYgY3VzdG9tIHRyYW5zcG9ydCBpcyBzZXQsIHVzZSBpdC5cbiAgICBpZiAocGFyYW1zLnRyYW5zcG9ydCkge1xuICAgICAgaWYgKCFpc09iamVjdChwYXJhbXMudHJhbnNwb3J0KSkge1xuICAgICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKFxuICAgICAgICAgIGBJbnZhbGlkIHRyYW5zcG9ydCB0eXBlIDogJHtwYXJhbXMudHJhbnNwb3J0fSwgZXhwZWN0ZWQgdG8gYmUgdHlwZSBcIm9iamVjdFwiYCxcbiAgICAgICAgKVxuICAgICAgfVxuICAgICAgdHJhbnNwb3J0ID0gcGFyYW1zLnRyYW5zcG9ydFxuICAgIH1cblxuICAgIC8vIGlmIGN1c3RvbSB0cmFuc3BvcnQgYWdlbnQgaXMgc2V0LCB1c2UgaXQuXG4gICAgaWYgKHBhcmFtcy50cmFuc3BvcnRBZ2VudCkge1xuICAgICAgaWYgKCFpc09iamVjdChwYXJhbXMudHJhbnNwb3J0QWdlbnQpKSB7XG4gICAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEFyZ3VtZW50RXJyb3IoXG4gICAgICAgICAgYEludmFsaWQgdHJhbnNwb3J0QWdlbnQgdHlwZTogJHtwYXJhbXMudHJhbnNwb3J0QWdlbnR9LCBleHBlY3RlZCB0byBiZSB0eXBlIFwib2JqZWN0XCJgLFxuICAgICAgICApXG4gICAgICB9XG5cbiAgICAgIHRyYW5zcG9ydEFnZW50ID0gcGFyYW1zLnRyYW5zcG9ydEFnZW50XG4gICAgfVxuXG4gICAgLy8gVXNlciBBZ2VudCBzaG91bGQgYWx3YXlzIGZvbGxvd2luZyB0aGUgYmVsb3cgc3R5bGUuXG4gICAgLy8gUGxlYXNlIG9wZW4gYW4gaXNzdWUgdG8gZGlzY3VzcyBhbnkgbmV3IGNoYW5nZXMgaGVyZS5cbiAgICAvL1xuICAgIC8vICAgICAgIE1pbklPIChPUzsgQVJDSCkgTElCL1ZFUiBBUFAvVkVSXG4gICAgLy9cbiAgICBjb25zdCBsaWJyYXJ5Q29tbWVudHMgPSBgKCR7cHJvY2Vzcy5wbGF0Zm9ybX07ICR7cHJvY2Vzcy5hcmNofSlgXG4gICAgY29uc3QgbGlicmFyeUFnZW50ID0gYE1pbklPICR7bGlicmFyeUNvbW1lbnRzfSBtaW5pby1qcy8ke1BhY2thZ2UudmVyc2lvbn1gXG4gICAgLy8gVXNlciBhZ2VudCBibG9jayBlbmRzLlxuXG4gICAgdGhpcy50cmFuc3BvcnQgPSB0cmFuc3BvcnRcbiAgICB0aGlzLnRyYW5zcG9ydEFnZW50ID0gdHJhbnNwb3J0QWdlbnRcbiAgICB0aGlzLmhvc3QgPSBob3N0XG4gICAgdGhpcy5wb3J0ID0gcG9ydFxuICAgIHRoaXMucHJvdG9jb2wgPSBwcm90b2NvbFxuICAgIHRoaXMudXNlckFnZW50ID0gYCR7bGlicmFyeUFnZW50fWBcblxuICAgIC8vIERlZmF1bHQgcGF0aCBzdHlsZSBpcyB0cnVlXG4gICAgaWYgKHBhcmFtcy5wYXRoU3R5bGUgPT09IHVuZGVmaW5lZCkge1xuICAgICAgdGhpcy5wYXRoU3R5bGUgPSB0cnVlXG4gICAgfSBlbHNlIHtcbiAgICAgIHRoaXMucGF0aFN0eWxlID0gcGFyYW1zLnBhdGhTdHlsZVxuICAgIH1cblxuICAgIHRoaXMuYWNjZXNzS2V5ID0gcGFyYW1zLmFjY2Vzc0tleSA/PyAnJ1xuICAgIHRoaXMuc2VjcmV0S2V5ID0gcGFyYW1zLnNlY3JldEtleSA/PyAnJ1xuICAgIHRoaXMuc2Vzc2lvblRva2VuID0gcGFyYW1zLnNlc3Npb25Ub2tlblxuICAgIHRoaXMuYW5vbnltb3VzID0gIXRoaXMuYWNjZXNzS2V5IHx8ICF0aGlzLnNlY3JldEtleVxuXG4gICAgaWYgKHBhcmFtcy5jcmVkZW50aWFsc1Byb3ZpZGVyKSB7XG4gICAgICB0aGlzLmFub255bW91cyA9IGZhbHNlXG4gICAgICB0aGlzLmNyZWRlbnRpYWxzUHJvdmlkZXIgPSBwYXJhbXMuY3JlZGVudGlhbHNQcm92aWRlclxuICAgIH1cblxuICAgIHRoaXMucmVnaW9uTWFwID0ge31cbiAgICBpZiAocGFyYW1zLnJlZ2lvbikge1xuICAgICAgdGhpcy5yZWdpb24gPSBwYXJhbXMucmVnaW9uXG4gICAgfVxuXG4gICAgaWYgKHBhcmFtcy5wYXJ0U2l6ZSkge1xuICAgICAgdGhpcy5wYXJ0U2l6ZSA9IHBhcmFtcy5wYXJ0U2l6ZVxuICAgICAgdGhpcy5vdmVyUmlkZVBhcnRTaXplID0gdHJ1ZVxuICAgIH1cbiAgICBpZiAodGhpcy5wYXJ0U2l6ZSA8IDUgKiAxMDI0ICogMTAyNCkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQXJndW1lbnRFcnJvcihgUGFydCBzaXplIHNob3VsZCBiZSBncmVhdGVyIHRoYW4gNU1CYClcbiAgICB9XG4gICAgaWYgKHRoaXMucGFydFNpemUgPiA1ICogMTAyNCAqIDEwMjQgKiAxMDI0KSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKGBQYXJ0IHNpemUgc2hvdWxkIGJlIGxlc3MgdGhhbiA1R0JgKVxuICAgIH1cblxuICAgIC8vIFNIQTI1NiBpcyBlbmFibGVkIG9ubHkgZm9yIGF1dGhlbnRpY2F0ZWQgaHR0cCByZXF1ZXN0cy4gSWYgdGhlIHJlcXVlc3QgaXMgYXV0aGVudGljYXRlZFxuICAgIC8vIGFuZCB0aGUgY29ubmVjdGlvbiBpcyBodHRwcyB3ZSB1c2UgeC1hbXotY29udGVudC1zaGEyNTY9VU5TSUdORUQtUEFZTE9BRFxuICAgIC8vIGhlYWRlciBmb3Igc2lnbmF0dXJlIGNhbGN1bGF0aW9uLlxuICAgIHRoaXMuZW5hYmxlU0hBMjU2ID0gIXRoaXMuYW5vbnltb3VzICYmICFwYXJhbXMudXNlU1NMXG5cbiAgICB0aGlzLnMzQWNjZWxlcmF0ZUVuZHBvaW50ID0gcGFyYW1zLnMzQWNjZWxlcmF0ZUVuZHBvaW50IHx8IHVuZGVmaW5lZFxuICAgIHRoaXMucmVxT3B0aW9ucyA9IHt9XG4gICAgdGhpcy5jbGllbnRFeHRlbnNpb25zID0gbmV3IEV4dGVuc2lvbnModGhpcylcblxuICAgIGlmIChwYXJhbXMucmV0cnlPcHRpb25zKSB7XG4gICAgICBpZiAoIWlzT2JqZWN0KHBhcmFtcy5yZXRyeU9wdGlvbnMpKSB7XG4gICAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEFyZ3VtZW50RXJyb3IoXG4gICAgICAgICAgYEludmFsaWQgcmV0cnlPcHRpb25zIHR5cGU6ICR7cGFyYW1zLnJldHJ5T3B0aW9uc30sIGV4cGVjdGVkIHRvIGJlIHR5cGUgXCJvYmplY3RcImAsXG4gICAgICAgIClcbiAgICAgIH1cblxuICAgICAgdGhpcy5yZXRyeU9wdGlvbnMgPSBwYXJhbXMucmV0cnlPcHRpb25zXG4gICAgfSBlbHNlIHtcbiAgICAgIHRoaXMucmV0cnlPcHRpb25zID0ge1xuICAgICAgICBkaXNhYmxlUmV0cnk6IGZhbHNlLFxuICAgICAgfVxuICAgIH1cbiAgfVxuICAvKipcbiAgICogTWluaW8gZXh0ZW5zaW9ucyB0aGF0IGFyZW4ndCBuZWNlc3NhcnkgcHJlc2VudCBmb3IgQW1hem9uIFMzIGNvbXBhdGlibGUgc3RvcmFnZSBzZXJ2ZXJzXG4gICAqL1xuICBnZXQgZXh0ZW5zaW9ucygpIHtcbiAgICByZXR1cm4gdGhpcy5jbGllbnRFeHRlbnNpb25zXG4gIH1cblxuICAvKipcbiAgICogQHBhcmFtIGVuZFBvaW50IC0gdmFsaWQgUzMgYWNjZWxlcmF0aW9uIGVuZCBwb2ludFxuICAgKi9cbiAgc2V0UzNUcmFuc2ZlckFjY2VsZXJhdGUoZW5kUG9pbnQ6IHN0cmluZykge1xuICAgIHRoaXMuczNBY2NlbGVyYXRlRW5kcG9pbnQgPSBlbmRQb2ludFxuICB9XG5cbiAgLyoqXG4gICAqIFNldHMgdGhlIHN1cHBvcnRlZCByZXF1ZXN0IG9wdGlvbnMuXG4gICAqL1xuICBwdWJsaWMgc2V0UmVxdWVzdE9wdGlvbnMob3B0aW9uczogUGljazxodHRwcy5SZXF1ZXN0T3B0aW9ucywgKHR5cGVvZiByZXF1ZXN0T3B0aW9uUHJvcGVydGllcylbbnVtYmVyXT4pIHtcbiAgICBpZiAoIWlzT2JqZWN0KG9wdGlvbnMpKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdyZXF1ZXN0IG9wdGlvbnMgc2hvdWxkIGJlIG9mIHR5cGUgXCJvYmplY3RcIicpXG4gICAgfVxuICAgIHRoaXMucmVxT3B0aW9ucyA9IF8ucGljayhvcHRpb25zLCByZXF1ZXN0T3B0aW9uUHJvcGVydGllcylcbiAgfVxuXG4gIC8qKlxuICAgKiAgVGhpcyBpcyBzMyBTcGVjaWZpYyBhbmQgZG9lcyBub3QgaG9sZCB2YWxpZGl0eSBpbiBhbnkgb3RoZXIgT2JqZWN0IHN0b3JhZ2UuXG4gICAqL1xuICBwcml2YXRlIGdldEFjY2VsZXJhdGVFbmRQb2ludElmU2V0KGJ1Y2tldE5hbWU/OiBzdHJpbmcsIG9iamVjdE5hbWU/OiBzdHJpbmcpIHtcbiAgICBpZiAoIWlzRW1wdHkodGhpcy5zM0FjY2VsZXJhdGVFbmRwb2ludCkgJiYgIWlzRW1wdHkoYnVja2V0TmFtZSkgJiYgIWlzRW1wdHkob2JqZWN0TmFtZSkpIHtcbiAgICAgIC8vIGh0dHA6Ly9kb2NzLmF3cy5hbWF6b24uY29tL0FtYXpvblMzL2xhdGVzdC9kZXYvdHJhbnNmZXItYWNjZWxlcmF0aW9uLmh0bWxcbiAgICAgIC8vIERpc2FibGUgdHJhbnNmZXIgYWNjZWxlcmF0aW9uIGZvciBub24tY29tcGxpYW50IGJ1Y2tldCBuYW1lcy5cbiAgICAgIGlmIChidWNrZXROYW1lLmluY2x1ZGVzKCcuJykpIHtcbiAgICAgICAgdGhyb3cgbmV3IEVycm9yKGBUcmFuc2ZlciBBY2NlbGVyYXRpb24gaXMgbm90IHN1cHBvcnRlZCBmb3Igbm9uIGNvbXBsaWFudCBidWNrZXQ6JHtidWNrZXROYW1lfWApXG4gICAgICB9XG4gICAgICAvLyBJZiB0cmFuc2ZlciBhY2NlbGVyYXRpb24gaXMgcmVxdWVzdGVkIHNldCBuZXcgaG9zdC5cbiAgICAgIC8vIEZvciBtb3JlIGRldGFpbHMgYWJvdXQgZW5hYmxpbmcgdHJhbnNmZXIgYWNjZWxlcmF0aW9uIHJlYWQgaGVyZS5cbiAgICAgIC8vIGh0dHA6Ly9kb2NzLmF3cy5hbWF6b24uY29tL0FtYXpvblMzL2xhdGVzdC9kZXYvdHJhbnNmZXItYWNjZWxlcmF0aW9uLmh0bWxcbiAgICAgIHJldHVybiB0aGlzLnMzQWNjZWxlcmF0ZUVuZHBvaW50XG4gICAgfVxuICAgIHJldHVybiBmYWxzZVxuICB9XG5cbiAgLyoqXG4gICAqICAgU2V0IGFwcGxpY2F0aW9uIHNwZWNpZmljIGluZm9ybWF0aW9uLlxuICAgKiAgIEdlbmVyYXRlcyBVc2VyLUFnZW50IGluIHRoZSBmb2xsb3dpbmcgc3R5bGUuXG4gICAqICAgTWluSU8gKE9TOyBBUkNIKSBMSUIvVkVSIEFQUC9WRVJcbiAgICovXG4gIHNldEFwcEluZm8oYXBwTmFtZTogc3RyaW5nLCBhcHBWZXJzaW9uOiBzdHJpbmcpIHtcbiAgICBpZiAoIWlzU3RyaW5nKGFwcE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKGBJbnZhbGlkIGFwcE5hbWU6ICR7YXBwTmFtZX1gKVxuICAgIH1cbiAgICBpZiAoYXBwTmFtZS50cmltKCkgPT09ICcnKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKCdJbnB1dCBhcHBOYW1lIGNhbm5vdCBiZSBlbXB0eS4nKVxuICAgIH1cbiAgICBpZiAoIWlzU3RyaW5nKGFwcFZlcnNpb24pKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKGBJbnZhbGlkIGFwcFZlcnNpb246ICR7YXBwVmVyc2lvbn1gKVxuICAgIH1cbiAgICBpZiAoYXBwVmVyc2lvbi50cmltKCkgPT09ICcnKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKCdJbnB1dCBhcHBWZXJzaW9uIGNhbm5vdCBiZSBlbXB0eS4nKVxuICAgIH1cbiAgICB0aGlzLnVzZXJBZ2VudCA9IGAke3RoaXMudXNlckFnZW50fSAke2FwcE5hbWV9LyR7YXBwVmVyc2lvbn1gXG4gIH1cblxuICAvKipcbiAgICogcmV0dXJucyBvcHRpb25zIG9iamVjdCB0aGF0IGNhbiBiZSB1c2VkIHdpdGggaHR0cC5yZXF1ZXN0KClcbiAgICogVGFrZXMgY2FyZSBvZiBjb25zdHJ1Y3RpbmcgdmlydHVhbC1ob3N0LXN0eWxlIG9yIHBhdGgtc3R5bGUgaG9zdG5hbWVcbiAgICovXG4gIHByb3RlY3RlZCBnZXRSZXF1ZXN0T3B0aW9ucyhcbiAgICBvcHRzOiBSZXF1ZXN0T3B0aW9uICYge1xuICAgICAgcmVnaW9uOiBzdHJpbmdcbiAgICB9LFxuICApOiBJUmVxdWVzdCAmIHtcbiAgICBob3N0OiBzdHJpbmdcbiAgICBoZWFkZXJzOiBSZWNvcmQ8c3RyaW5nLCBzdHJpbmc+XG4gIH0ge1xuICAgIGNvbnN0IG1ldGhvZCA9IG9wdHMubWV0aG9kXG4gICAgY29uc3QgcmVnaW9uID0gb3B0cy5yZWdpb25cbiAgICBjb25zdCBidWNrZXROYW1lID0gb3B0cy5idWNrZXROYW1lXG4gICAgbGV0IG9iamVjdE5hbWUgPSBvcHRzLm9iamVjdE5hbWVcbiAgICBjb25zdCBoZWFkZXJzID0gb3B0cy5oZWFkZXJzXG4gICAgY29uc3QgcXVlcnkgPSBvcHRzLnF1ZXJ5XG5cbiAgICBsZXQgcmVxT3B0aW9ucyA9IHtcbiAgICAgIG1ldGhvZCxcbiAgICAgIGhlYWRlcnM6IHt9IGFzIFJlcXVlc3RIZWFkZXJzLFxuICAgICAgcHJvdG9jb2w6IHRoaXMucHJvdG9jb2wsXG4gICAgICAvLyBJZiBjdXN0b20gdHJhbnNwb3J0QWdlbnQgd2FzIHN1cHBsaWVkIGVhcmxpZXIsIHdlJ2xsIGluamVjdCBpdCBoZXJlXG4gICAgICBhZ2VudDogdGhpcy50cmFuc3BvcnRBZ2VudCxcbiAgICB9XG5cbiAgICAvLyBWZXJpZnkgaWYgdmlydHVhbCBob3N0IHN1cHBvcnRlZC5cbiAgICBsZXQgdmlydHVhbEhvc3RTdHlsZVxuICAgIGlmIChidWNrZXROYW1lKSB7XG4gICAgICB2aXJ0dWFsSG9zdFN0eWxlID0gaXNWaXJ0dWFsSG9zdFN0eWxlKHRoaXMuaG9zdCwgdGhpcy5wcm90b2NvbCwgYnVja2V0TmFtZSwgdGhpcy5wYXRoU3R5bGUpXG4gICAgfVxuXG4gICAgbGV0IHBhdGggPSAnLydcbiAgICBsZXQgaG9zdCA9IHRoaXMuaG9zdFxuXG4gICAgbGV0IHBvcnQ6IHVuZGVmaW5lZCB8IG51bWJlclxuICAgIGlmICh0aGlzLnBvcnQpIHtcbiAgICAgIHBvcnQgPSB0aGlzLnBvcnRcbiAgICB9XG5cbiAgICBpZiAob2JqZWN0TmFtZSkge1xuICAgICAgb2JqZWN0TmFtZSA9IHVyaVJlc291cmNlRXNjYXBlKG9iamVjdE5hbWUpXG4gICAgfVxuXG4gICAgLy8gRm9yIEFtYXpvbiBTMyBlbmRwb2ludCwgZ2V0IGVuZHBvaW50IGJhc2VkIG9uIHJlZ2lvbi5cbiAgICBpZiAoaXNBbWF6b25FbmRwb2ludChob3N0KSkge1xuICAgICAgY29uc3QgYWNjZWxlcmF0ZUVuZFBvaW50ID0gdGhpcy5nZXRBY2NlbGVyYXRlRW5kUG9pbnRJZlNldChidWNrZXROYW1lLCBvYmplY3ROYW1lKVxuICAgICAgaWYgKGFjY2VsZXJhdGVFbmRQb2ludCkge1xuICAgICAgICBob3N0ID0gYCR7YWNjZWxlcmF0ZUVuZFBvaW50fWBcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIGhvc3QgPSBnZXRTM0VuZHBvaW50KHJlZ2lvbilcbiAgICAgIH1cbiAgICB9XG5cbiAgICBpZiAodmlydHVhbEhvc3RTdHlsZSAmJiAhb3B0cy5wYXRoU3R5bGUpIHtcbiAgICAgIC8vIEZvciBhbGwgaG9zdHMgd2hpY2ggc3VwcG9ydCB2aXJ0dWFsIGhvc3Qgc3R5bGUsIGBidWNrZXROYW1lYFxuICAgICAgLy8gaXMgcGFydCBvZiB0aGUgaG9zdG5hbWUgaW4gdGhlIGZvbGxvd2luZyBmb3JtYXQ6XG4gICAgICAvL1xuICAgICAgLy8gIHZhciBob3N0ID0gJ2J1Y2tldE5hbWUuZXhhbXBsZS5jb20nXG4gICAgICAvL1xuICAgICAgaWYgKGJ1Y2tldE5hbWUpIHtcbiAgICAgICAgaG9zdCA9IGAke2J1Y2tldE5hbWV9LiR7aG9zdH1gXG4gICAgICB9XG4gICAgICBpZiAob2JqZWN0TmFtZSkge1xuICAgICAgICBwYXRoID0gYC8ke29iamVjdE5hbWV9YFxuICAgICAgfVxuICAgIH0gZWxzZSB7XG4gICAgICAvLyBGb3IgYWxsIFMzIGNvbXBhdGlibGUgc3RvcmFnZSBzZXJ2aWNlcyB3ZSB3aWxsIGZhbGxiYWNrIHRvXG4gICAgICAvLyBwYXRoIHN0eWxlIHJlcXVlc3RzLCB3aGVyZSBgYnVja2V0TmFtZWAgaXMgcGFydCBvZiB0aGUgVVJJXG4gICAgICAvLyBwYXRoLlxuICAgICAgaWYgKGJ1Y2tldE5hbWUpIHtcbiAgICAgICAgcGF0aCA9IGAvJHtidWNrZXROYW1lfWBcbiAgICAgIH1cbiAgICAgIGlmIChvYmplY3ROYW1lKSB7XG4gICAgICAgIHBhdGggPSBgLyR7YnVja2V0TmFtZX0vJHtvYmplY3ROYW1lfWBcbiAgICAgIH1cbiAgICB9XG5cbiAgICBpZiAocXVlcnkpIHtcbiAgICAgIHBhdGggKz0gYD8ke3F1ZXJ5fWBcbiAgICB9XG4gICAgcmVxT3B0aW9ucy5oZWFkZXJzLmhvc3QgPSBob3N0XG4gICAgaWYgKChyZXFPcHRpb25zLnByb3RvY29sID09PSAnaHR0cDonICYmIHBvcnQgIT09IDgwKSB8fCAocmVxT3B0aW9ucy5wcm90b2NvbCA9PT0gJ2h0dHBzOicgJiYgcG9ydCAhPT0gNDQzKSkge1xuICAgICAgcmVxT3B0aW9ucy5oZWFkZXJzLmhvc3QgPSBqb2luSG9zdFBvcnQoaG9zdCwgcG9ydClcbiAgICB9XG5cbiAgICByZXFPcHRpb25zLmhlYWRlcnNbJ3VzZXItYWdlbnQnXSA9IHRoaXMudXNlckFnZW50XG4gICAgaWYgKGhlYWRlcnMpIHtcbiAgICAgIC8vIGhhdmUgYWxsIGhlYWRlciBrZXlzIGluIGxvd2VyIGNhc2UgLSB0byBtYWtlIHNpZ25pbmcgZWFzeVxuICAgICAgZm9yIChjb25zdCBbaywgdl0gb2YgT2JqZWN0LmVudHJpZXMoaGVhZGVycykpIHtcbiAgICAgICAgcmVxT3B0aW9ucy5oZWFkZXJzW2sudG9Mb3dlckNhc2UoKV0gPSB2XG4gICAgICB9XG4gICAgfVxuXG4gICAgLy8gVXNlIGFueSByZXF1ZXN0IG9wdGlvbiBzcGVjaWZpZWQgaW4gbWluaW9DbGllbnQuc2V0UmVxdWVzdE9wdGlvbnMoKVxuICAgIHJlcU9wdGlvbnMgPSBPYmplY3QuYXNzaWduKHt9LCB0aGlzLnJlcU9wdGlvbnMsIHJlcU9wdGlvbnMpXG5cbiAgICByZXR1cm4ge1xuICAgICAgLi4ucmVxT3B0aW9ucyxcbiAgICAgIGhlYWRlcnM6IF8ubWFwVmFsdWVzKF8ucGlja0J5KHJlcU9wdGlvbnMuaGVhZGVycywgaXNEZWZpbmVkKSwgKHYpID0+IHYudG9TdHJpbmcoKSksXG4gICAgICBob3N0LFxuICAgICAgcG9ydCxcbiAgICAgIHBhdGgsXG4gICAgfSBzYXRpc2ZpZXMgaHR0cHMuUmVxdWVzdE9wdGlvbnNcbiAgfVxuXG4gIHB1YmxpYyBhc3luYyBzZXRDcmVkZW50aWFsc1Byb3ZpZGVyKGNyZWRlbnRpYWxzUHJvdmlkZXI6IENyZWRlbnRpYWxQcm92aWRlcikge1xuICAgIGlmICghKGNyZWRlbnRpYWxzUHJvdmlkZXIgaW5zdGFuY2VvZiBDcmVkZW50aWFsUHJvdmlkZXIpKSB7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoJ1VuYWJsZSB0byBnZXQgY3JlZGVudGlhbHMuIEV4cGVjdGVkIGluc3RhbmNlIG9mIENyZWRlbnRpYWxQcm92aWRlcicpXG4gICAgfVxuICAgIHRoaXMuY3JlZGVudGlhbHNQcm92aWRlciA9IGNyZWRlbnRpYWxzUHJvdmlkZXJcbiAgICBhd2FpdCB0aGlzLmNoZWNrQW5kUmVmcmVzaENyZWRzKClcbiAgfVxuXG4gIHByaXZhdGUgYXN5bmMgY2hlY2tBbmRSZWZyZXNoQ3JlZHMoKSB7XG4gICAgaWYgKHRoaXMuY3JlZGVudGlhbHNQcm92aWRlcikge1xuICAgICAgdHJ5IHtcbiAgICAgICAgY29uc3QgY3JlZGVudGlhbHNDb25mID0gYXdhaXQgdGhpcy5jcmVkZW50aWFsc1Byb3ZpZGVyLmdldENyZWRlbnRpYWxzKClcbiAgICAgICAgdGhpcy5hY2Nlc3NLZXkgPSBjcmVkZW50aWFsc0NvbmYuZ2V0QWNjZXNzS2V5KClcbiAgICAgICAgdGhpcy5zZWNyZXRLZXkgPSBjcmVkZW50aWFsc0NvbmYuZ2V0U2VjcmV0S2V5KClcbiAgICAgICAgdGhpcy5zZXNzaW9uVG9rZW4gPSBjcmVkZW50aWFsc0NvbmYuZ2V0U2Vzc2lvblRva2VuKClcbiAgICAgIH0gY2F0Y2ggKGUpIHtcbiAgICAgICAgdGhyb3cgbmV3IEVycm9yKGBVbmFibGUgdG8gZ2V0IGNyZWRlbnRpYWxzOiAke2V9YCwgeyBjYXVzZTogZSB9KVxuICAgICAgfVxuICAgIH1cbiAgfVxuXG4gIHByaXZhdGUgbG9nU3RyZWFtPzogc3RyZWFtLldyaXRhYmxlXG5cbiAgLyoqXG4gICAqIGxvZyB0aGUgcmVxdWVzdCwgcmVzcG9uc2UsIGVycm9yXG4gICAqL1xuICBwcml2YXRlIGxvZ0hUVFAocmVxT3B0aW9uczogSVJlcXVlc3QsIHJlc3BvbnNlOiBodHRwLkluY29taW5nTWVzc2FnZSB8IG51bGwsIGVycj86IHVua25vd24pIHtcbiAgICAvLyBpZiBubyBsb2dTdHJlYW0gYXZhaWxhYmxlIHJldHVybi5cbiAgICBpZiAoIXRoaXMubG9nU3RyZWFtKSB7XG4gICAgICByZXR1cm5cbiAgICB9XG4gICAgaWYgKCFpc09iamVjdChyZXFPcHRpb25zKSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcigncmVxT3B0aW9ucyBzaG91bGQgYmUgb2YgdHlwZSBcIm9iamVjdFwiJylcbiAgICB9XG4gICAgaWYgKHJlc3BvbnNlICYmICFpc1JlYWRhYmxlU3RyZWFtKHJlc3BvbnNlKSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcigncmVzcG9uc2Ugc2hvdWxkIGJlIG9mIHR5cGUgXCJTdHJlYW1cIicpXG4gICAgfVxuICAgIGlmIChlcnIgJiYgIShlcnIgaW5zdGFuY2VvZiBFcnJvcikpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ2VyciBzaG91bGQgYmUgb2YgdHlwZSBcIkVycm9yXCInKVxuICAgIH1cbiAgICBjb25zdCBsb2dTdHJlYW0gPSB0aGlzLmxvZ1N0cmVhbVxuICAgIGNvbnN0IGxvZ0hlYWRlcnMgPSAoaGVhZGVyczogUmVxdWVzdEhlYWRlcnMpID0+IHtcbiAgICAgIE9iamVjdC5lbnRyaWVzKGhlYWRlcnMpLmZvckVhY2goKFtrLCB2XSkgPT4ge1xuICAgICAgICBpZiAoayA9PSAnYXV0aG9yaXphdGlvbicpIHtcbiAgICAgICAgICBpZiAoaXNTdHJpbmcodikpIHtcbiAgICAgICAgICAgIGNvbnN0IHJlZGFjdG9yID0gbmV3IFJlZ0V4cCgnU2lnbmF0dXJlPShbMC05YS1mXSspJylcbiAgICAgICAgICAgIHYgPSB2LnJlcGxhY2UocmVkYWN0b3IsICdTaWduYXR1cmU9KipSRURBQ1RFRCoqJylcbiAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICAgICAgbG9nU3RyZWFtLndyaXRlKGAke2t9OiAke3Z9XFxuYClcbiAgICAgIH0pXG4gICAgICBsb2dTdHJlYW0ud3JpdGUoJ1xcbicpXG4gICAgfVxuICAgIGxvZ1N0cmVhbS53cml0ZShgUkVRVUVTVDogJHtyZXFPcHRpb25zLm1ldGhvZH0gJHtyZXFPcHRpb25zLnBhdGh9XFxuYClcbiAgICBsb2dIZWFkZXJzKHJlcU9wdGlvbnMuaGVhZGVycylcbiAgICBpZiAocmVzcG9uc2UpIHtcbiAgICAgIHRoaXMubG9nU3RyZWFtLndyaXRlKGBSRVNQT05TRTogJHtyZXNwb25zZS5zdGF0dXNDb2RlfVxcbmApXG4gICAgICBsb2dIZWFkZXJzKHJlc3BvbnNlLmhlYWRlcnMgYXMgUmVxdWVzdEhlYWRlcnMpXG4gICAgfVxuICAgIGlmIChlcnIpIHtcbiAgICAgIGxvZ1N0cmVhbS53cml0ZSgnRVJST1IgQk9EWTpcXG4nKVxuICAgICAgY29uc3QgZXJySlNPTiA9IEpTT04uc3RyaW5naWZ5KGVyciwgbnVsbCwgJ1xcdCcpXG4gICAgICBsb2dTdHJlYW0ud3JpdGUoYCR7ZXJySlNPTn1cXG5gKVxuICAgIH1cbiAgfVxuXG4gIC8qKlxuICAgKiBFbmFibGUgdHJhY2luZ1xuICAgKi9cbiAgcHVibGljIHRyYWNlT24oc3RyZWFtPzogc3RyZWFtLldyaXRhYmxlKSB7XG4gICAgaWYgKCFzdHJlYW0pIHtcbiAgICAgIHN0cmVhbSA9IHByb2Nlc3Muc3Rkb3V0XG4gICAgfVxuICAgIHRoaXMubG9nU3RyZWFtID0gc3RyZWFtXG4gIH1cblxuICAvKipcbiAgICogRGlzYWJsZSB0cmFjaW5nXG4gICAqL1xuICBwdWJsaWMgdHJhY2VPZmYoKSB7XG4gICAgdGhpcy5sb2dTdHJlYW0gPSB1bmRlZmluZWRcbiAgfVxuXG4gIC8qKlxuICAgKiBtYWtlUmVxdWVzdCBpcyB0aGUgcHJpbWl0aXZlIHVzZWQgYnkgdGhlIGFwaXMgZm9yIG1ha2luZyBTMyByZXF1ZXN0cy5cbiAgICogcGF5bG9hZCBjYW4gYmUgZW1wdHkgc3RyaW5nIGluIGNhc2Ugb2Ygbm8gcGF5bG9hZC5cbiAgICogc3RhdHVzQ29kZSBpcyB0aGUgZXhwZWN0ZWQgc3RhdHVzQ29kZS4gSWYgcmVzcG9uc2Uuc3RhdHVzQ29kZSBkb2VzIG5vdCBtYXRjaFxuICAgKiB3ZSBwYXJzZSB0aGUgWE1MIGVycm9yIGFuZCBjYWxsIHRoZSBjYWxsYmFjayB3aXRoIHRoZSBlcnJvciBtZXNzYWdlLlxuICAgKlxuICAgKiBBIHZhbGlkIHJlZ2lvbiBpcyBwYXNzZWQgYnkgdGhlIGNhbGxzIC0gbGlzdEJ1Y2tldHMsIG1ha2VCdWNrZXQgYW5kIGdldEJ1Y2tldFJlZ2lvbi5cbiAgICpcbiAgICogQGludGVybmFsXG4gICAqL1xuICBhc3luYyBtYWtlUmVxdWVzdEFzeW5jKFxuICAgIG9wdGlvbnM6IFJlcXVlc3RPcHRpb24sXG4gICAgcGF5bG9hZDogQmluYXJ5ID0gJycsXG4gICAgZXhwZWN0ZWRDb2RlczogbnVtYmVyW10gPSBbMjAwXSxcbiAgICByZWdpb24gPSAnJyxcbiAgKTogUHJvbWlzZTxodHRwLkluY29taW5nTWVzc2FnZT4ge1xuICAgIGlmICghaXNPYmplY3Qob3B0aW9ucykpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ29wdGlvbnMgc2hvdWxkIGJlIG9mIHR5cGUgXCJvYmplY3RcIicpXG4gICAgfVxuICAgIGlmICghaXNTdHJpbmcocGF5bG9hZCkgJiYgIWlzT2JqZWN0KHBheWxvYWQpKSB7XG4gICAgICAvLyBCdWZmZXIgaXMgb2YgdHlwZSAnb2JqZWN0J1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcigncGF5bG9hZCBzaG91bGQgYmUgb2YgdHlwZSBcInN0cmluZ1wiIG9yIFwiQnVmZmVyXCInKVxuICAgIH1cbiAgICBleHBlY3RlZENvZGVzLmZvckVhY2goKHN0YXR1c0NvZGUpID0+IHtcbiAgICAgIGlmICghaXNOdW1iZXIoc3RhdHVzQ29kZSkpIHtcbiAgICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcignc3RhdHVzQ29kZSBzaG91bGQgYmUgb2YgdHlwZSBcIm51bWJlclwiJylcbiAgICAgIH1cbiAgICB9KVxuICAgIGlmICghaXNTdHJpbmcocmVnaW9uKSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcigncmVnaW9uIHNob3VsZCBiZSBvZiB0eXBlIFwic3RyaW5nXCInKVxuICAgIH1cbiAgICBpZiAoIW9wdGlvbnMuaGVhZGVycykge1xuICAgICAgb3B0aW9ucy5oZWFkZXJzID0ge31cbiAgICB9XG4gICAgaWYgKG9wdGlvbnMubWV0aG9kID09PSAnUE9TVCcgfHwgb3B0aW9ucy5tZXRob2QgPT09ICdQVVQnIHx8IG9wdGlvbnMubWV0aG9kID09PSAnREVMRVRFJykge1xuICAgICAgb3B0aW9ucy5oZWFkZXJzWydjb250ZW50LWxlbmd0aCddID0gcGF5bG9hZC5sZW5ndGgudG9TdHJpbmcoKVxuICAgIH1cbiAgICBjb25zdCBzaGEyNTZzdW0gPSB0aGlzLmVuYWJsZVNIQTI1NiA/IHRvU2hhMjU2KHBheWxvYWQpIDogJydcbiAgICByZXR1cm4gdGhpcy5tYWtlUmVxdWVzdFN0cmVhbUFzeW5jKG9wdGlvbnMsIHBheWxvYWQsIHNoYTI1NnN1bSwgZXhwZWN0ZWRDb2RlcywgcmVnaW9uKVxuICB9XG5cbiAgLyoqXG4gICAqIG5ldyByZXF1ZXN0IHdpdGggcHJvbWlzZVxuICAgKlxuICAgKiBObyBuZWVkIHRvIGRyYWluIHJlc3BvbnNlLCByZXNwb25zZSBib2R5IGlzIG5vdCB2YWxpZFxuICAgKi9cbiAgYXN5bmMgbWFrZVJlcXVlc3RBc3luY09taXQoXG4gICAgb3B0aW9uczogUmVxdWVzdE9wdGlvbixcbiAgICBwYXlsb2FkOiBCaW5hcnkgPSAnJyxcbiAgICBzdGF0dXNDb2RlczogbnVtYmVyW10gPSBbMjAwXSxcbiAgICByZWdpb24gPSAnJyxcbiAgKTogUHJvbWlzZTxPbWl0PGh0dHAuSW5jb21pbmdNZXNzYWdlLCAnb24nPj4ge1xuICAgIGNvbnN0IHJlcyA9IGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luYyhvcHRpb25zLCBwYXlsb2FkLCBzdGF0dXNDb2RlcywgcmVnaW9uKVxuICAgIGF3YWl0IGRyYWluUmVzcG9uc2UocmVzKVxuICAgIHJldHVybiByZXNcbiAgfVxuXG4gIC8qKlxuICAgKiBtYWtlUmVxdWVzdFN0cmVhbSB3aWxsIGJlIHVzZWQgZGlyZWN0bHkgaW5zdGVhZCBvZiBtYWtlUmVxdWVzdCBpbiBjYXNlIHRoZSBwYXlsb2FkXG4gICAqIGlzIGF2YWlsYWJsZSBhcyBhIHN0cmVhbS4gZm9yIGV4LiBwdXRPYmplY3RcbiAgICpcbiAgICogQGludGVybmFsXG4gICAqL1xuICBhc3luYyBtYWtlUmVxdWVzdFN0cmVhbUFzeW5jKFxuICAgIG9wdGlvbnM6IFJlcXVlc3RPcHRpb24sXG4gICAgYm9keTogc3RyZWFtLlJlYWRhYmxlIHwgQmluYXJ5LFxuICAgIHNoYTI1NnN1bTogc3RyaW5nLFxuICAgIHN0YXR1c0NvZGVzOiBudW1iZXJbXSxcbiAgICByZWdpb246IHN0cmluZyxcbiAgKTogUHJvbWlzZTxodHRwLkluY29taW5nTWVzc2FnZT4ge1xuICAgIGlmICghaXNPYmplY3Qob3B0aW9ucykpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ29wdGlvbnMgc2hvdWxkIGJlIG9mIHR5cGUgXCJvYmplY3RcIicpXG4gICAgfVxuICAgIGlmICghKEJ1ZmZlci5pc0J1ZmZlcihib2R5KSB8fCB0eXBlb2YgYm9keSA9PT0gJ3N0cmluZycgfHwgaXNSZWFkYWJsZVN0cmVhbShib2R5KSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEFyZ3VtZW50RXJyb3IoXG4gICAgICAgIGBzdHJlYW0gc2hvdWxkIGJlIGEgQnVmZmVyLCBzdHJpbmcgb3IgcmVhZGFibGUgU3RyZWFtLCBnb3QgJHt0eXBlb2YgYm9keX0gaW5zdGVhZGAsXG4gICAgICApXG4gICAgfVxuICAgIGlmICghaXNTdHJpbmcoc2hhMjU2c3VtKSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcignc2hhMjU2c3VtIHNob3VsZCBiZSBvZiB0eXBlIFwic3RyaW5nXCInKVxuICAgIH1cbiAgICBzdGF0dXNDb2Rlcy5mb3JFYWNoKChzdGF0dXNDb2RlKSA9PiB7XG4gICAgICBpZiAoIWlzTnVtYmVyKHN0YXR1c0NvZGUpKSB7XG4gICAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ3N0YXR1c0NvZGUgc2hvdWxkIGJlIG9mIHR5cGUgXCJudW1iZXJcIicpXG4gICAgICB9XG4gICAgfSlcbiAgICBpZiAoIWlzU3RyaW5nKHJlZ2lvbikpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ3JlZ2lvbiBzaG91bGQgYmUgb2YgdHlwZSBcInN0cmluZ1wiJylcbiAgICB9XG4gICAgLy8gc2hhMjU2c3VtIHdpbGwgYmUgZW1wdHkgZm9yIGFub255bW91cyBvciBodHRwcyByZXF1ZXN0c1xuICAgIGlmICghdGhpcy5lbmFibGVTSEEyNTYgJiYgc2hhMjU2c3VtLmxlbmd0aCAhPT0gMCkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQXJndW1lbnRFcnJvcihgc2hhMjU2c3VtIGV4cGVjdGVkIHRvIGJlIGVtcHR5IGZvciBhbm9ueW1vdXMgb3IgaHR0cHMgcmVxdWVzdHNgKVxuICAgIH1cbiAgICAvLyBzaGEyNTZzdW0gc2hvdWxkIGJlIHZhbGlkIGZvciBub24tYW5vbnltb3VzIGh0dHAgcmVxdWVzdHMuXG4gICAgaWYgKHRoaXMuZW5hYmxlU0hBMjU2ICYmIHNoYTI1NnN1bS5sZW5ndGggIT09IDY0KSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKGBJbnZhbGlkIHNoYTI1NnN1bSA6ICR7c2hhMjU2c3VtfWApXG4gICAgfVxuXG4gICAgYXdhaXQgdGhpcy5jaGVja0FuZFJlZnJlc2hDcmVkcygpXG5cbiAgICAvLyBlc2xpbnQtZGlzYWJsZS1uZXh0LWxpbmUgQHR5cGVzY3JpcHQtZXNsaW50L25vLW5vbi1udWxsLWFzc2VydGlvblxuICAgIHJlZ2lvbiA9IHJlZ2lvbiB8fCAoYXdhaXQgdGhpcy5nZXRCdWNrZXRSZWdpb25Bc3luYyhvcHRpb25zLmJ1Y2tldE5hbWUhKSlcblxuICAgIGNvbnN0IHJlcU9wdGlvbnMgPSB0aGlzLmdldFJlcXVlc3RPcHRpb25zKHsgLi4ub3B0aW9ucywgcmVnaW9uIH0pXG4gICAgaWYgKCF0aGlzLmFub255bW91cykge1xuICAgICAgLy8gRm9yIG5vbi1hbm9ueW1vdXMgaHR0cHMgcmVxdWVzdHMgc2hhMjU2c3VtIGlzICdVTlNJR05FRC1QQVlMT0FEJyBmb3Igc2lnbmF0dXJlIGNhbGN1bGF0aW9uLlxuICAgICAgaWYgKCF0aGlzLmVuYWJsZVNIQTI1Nikge1xuICAgICAgICBzaGEyNTZzdW0gPSAnVU5TSUdORUQtUEFZTE9BRCdcbiAgICAgIH1cbiAgICAgIGNvbnN0IGRhdGUgPSBuZXcgRGF0ZSgpXG4gICAgICByZXFPcHRpb25zLmhlYWRlcnNbJ3gtYW16LWRhdGUnXSA9IG1ha2VEYXRlTG9uZyhkYXRlKVxuICAgICAgcmVxT3B0aW9ucy5oZWFkZXJzWyd4LWFtei1jb250ZW50LXNoYTI1NiddID0gc2hhMjU2c3VtXG4gICAgICBpZiAodGhpcy5zZXNzaW9uVG9rZW4pIHtcbiAgICAgICAgcmVxT3B0aW9ucy5oZWFkZXJzWyd4LWFtei1zZWN1cml0eS10b2tlbiddID0gdGhpcy5zZXNzaW9uVG9rZW5cbiAgICAgIH1cbiAgICAgIHJlcU9wdGlvbnMuaGVhZGVycy5hdXRob3JpemF0aW9uID0gc2lnblY0KHJlcU9wdGlvbnMsIHRoaXMuYWNjZXNzS2V5LCB0aGlzLnNlY3JldEtleSwgcmVnaW9uLCBkYXRlLCBzaGEyNTZzdW0pXG4gICAgfVxuXG4gICAgY29uc3QgbG9nU3RyZWFtID0gdGhpcy5sb2dTdHJlYW1cbiAgICBjb25zdCBsb2cgPSBsb2dTdHJlYW0gPyAobXNnOiBzdHJpbmcpID0+IGxvZ1N0cmVhbS53cml0ZShgJHttc2d9XFxuYCkgOiAoKSA9PiB7fVxuXG4gICAgY29uc3QgcmVzcG9uc2UgPSBhd2FpdCByZXF1ZXN0V2l0aFJldHJ5KFxuICAgICAgdGhpcy50cmFuc3BvcnQsXG4gICAgICByZXFPcHRpb25zLFxuICAgICAgYm9keSxcbiAgICAgIHRoaXMucmV0cnlPcHRpb25zLmRpc2FibGVSZXRyeSA9PT0gdHJ1ZSA/IDAgOiB0aGlzLnJldHJ5T3B0aW9ucy5tYXhpbXVtUmV0cnlDb3VudCxcbiAgICAgIHRoaXMucmV0cnlPcHRpb25zLmJhc2VEZWxheU1zLFxuICAgICAgdGhpcy5yZXRyeU9wdGlvbnMubWF4aW11bURlbGF5TXMsXG4gICAgICBsb2csXG4gICAgKVxuICAgIGlmICghcmVzcG9uc2Uuc3RhdHVzQ29kZSkge1xuICAgICAgdGhyb3cgbmV3IEVycm9yKFwiQlVHOiByZXNwb25zZSBkb2Vzbid0IGhhdmUgYSBzdGF0dXNDb2RlXCIpXG4gICAgfVxuXG4gICAgaWYgKCFzdGF0dXNDb2Rlcy5pbmNsdWRlcyhyZXNwb25zZS5zdGF0dXNDb2RlKSkge1xuICAgICAgLy8gRm9yIGFuIGluY29ycmVjdCByZWdpb24sIFMzIHNlcnZlciBhbHdheXMgc2VuZHMgYmFjayA0MDAuXG4gICAgICAvLyBCdXQgd2Ugd2lsbCBkbyBjYWNoZSBpbnZhbGlkYXRpb24gZm9yIGFsbCBlcnJvcnMgc28gdGhhdCxcbiAgICAgIC8vIGluIGZ1dHVyZSwgaWYgQVdTIFMzIGRlY2lkZXMgdG8gc2VuZCBhIGRpZmZlcmVudCBzdGF0dXMgY29kZSBvclxuICAgICAgLy8gWE1MIGVycm9yIGNvZGUgd2Ugd2lsbCBzdGlsbCB3b3JrIGZpbmUuXG4gICAgICAvLyBlc2xpbnQtZGlzYWJsZS1uZXh0LWxpbmUgQHR5cGVzY3JpcHQtZXNsaW50L25vLW5vbi1udWxsLWFzc2VydGlvblxuICAgICAgZGVsZXRlIHRoaXMucmVnaW9uTWFwW29wdGlvbnMuYnVja2V0TmFtZSFdXG5cbiAgICAgIGNvbnN0IGVyciA9IGF3YWl0IHhtbFBhcnNlcnMucGFyc2VSZXNwb25zZUVycm9yKHJlc3BvbnNlKVxuICAgICAgdGhpcy5sb2dIVFRQKHJlcU9wdGlvbnMsIHJlc3BvbnNlLCBlcnIpXG4gICAgICB0aHJvdyBlcnJcbiAgICB9XG5cbiAgICB0aGlzLmxvZ0hUVFAocmVxT3B0aW9ucywgcmVzcG9uc2UpXG5cbiAgICByZXR1cm4gcmVzcG9uc2VcbiAgfVxuXG4gIC8qKlxuICAgKiBnZXRzIHRoZSByZWdpb24gb2YgdGhlIGJ1Y2tldFxuICAgKlxuICAgKiBAcGFyYW0gYnVja2V0TmFtZVxuICAgKlxuICAgKi9cbiAgYXN5bmMgZ2V0QnVja2V0UmVnaW9uQXN5bmMoYnVja2V0TmFtZTogc3RyaW5nKTogUHJvbWlzZTxzdHJpbmc+IHtcbiAgICBpZiAoIWlzVmFsaWRCdWNrZXROYW1lKGJ1Y2tldE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IoYEludmFsaWQgYnVja2V0IG5hbWUgOiAke2J1Y2tldE5hbWV9YClcbiAgICB9XG5cbiAgICAvLyBSZWdpb24gaXMgc2V0IHdpdGggY29uc3RydWN0b3IsIHJldHVybiB0aGUgcmVnaW9uIHJpZ2h0IGhlcmUuXG4gICAgaWYgKHRoaXMucmVnaW9uKSB7XG4gICAgICByZXR1cm4gdGhpcy5yZWdpb25cbiAgICB9XG5cbiAgICBjb25zdCBjYWNoZWQgPSB0aGlzLnJlZ2lvbk1hcFtidWNrZXROYW1lXVxuICAgIGlmIChjYWNoZWQpIHtcbiAgICAgIHJldHVybiBjYWNoZWRcbiAgICB9XG5cbiAgICBjb25zdCBleHRyYWN0UmVnaW9uQXN5bmMgPSBhc3luYyAocmVzcG9uc2U6IGh0dHAuSW5jb21pbmdNZXNzYWdlKSA9PiB7XG4gICAgICBjb25zdCBib2R5ID0gYXdhaXQgcmVhZEFzU3RyaW5nKHJlc3BvbnNlKVxuICAgICAgY29uc3QgcmVnaW9uID0geG1sUGFyc2Vycy5wYXJzZUJ1Y2tldFJlZ2lvbihib2R5KSB8fCBERUZBVUxUX1JFR0lPTlxuICAgICAgdGhpcy5yZWdpb25NYXBbYnVja2V0TmFtZV0gPSByZWdpb25cbiAgICAgIHJldHVybiByZWdpb25cbiAgICB9XG5cbiAgICBjb25zdCBtZXRob2QgPSAnR0VUJ1xuICAgIGNvbnN0IHF1ZXJ5ID0gJ2xvY2F0aW9uJ1xuICAgIC8vIGBnZXRCdWNrZXRMb2NhdGlvbmAgYmVoYXZlcyBkaWZmZXJlbnRseSBpbiBmb2xsb3dpbmcgd2F5cyBmb3JcbiAgICAvLyBkaWZmZXJlbnQgZW52aXJvbm1lbnRzLlxuICAgIC8vXG4gICAgLy8gLSBGb3Igbm9kZWpzIGVudiB3ZSBkZWZhdWx0IHRvIHBhdGggc3R5bGUgcmVxdWVzdHMuXG4gICAgLy8gLSBGb3IgYnJvd3NlciBlbnYgcGF0aCBzdHlsZSByZXF1ZXN0cyBvbiBidWNrZXRzIHlpZWxkcyBDT1JTXG4gICAgLy8gICBlcnJvci4gVG8gY2lyY3VtdmVudCB0aGlzIHByb2JsZW0gd2UgbWFrZSBhIHZpcnR1YWwgaG9zdFxuICAgIC8vICAgc3R5bGUgcmVxdWVzdCBzaWduZWQgd2l0aCAndXMtZWFzdC0xJy4gVGhpcyByZXF1ZXN0IGZhaWxzXG4gICAgLy8gICB3aXRoIGFuIGVycm9yICdBdXRob3JpemF0aW9uSGVhZGVyTWFsZm9ybWVkJywgYWRkaXRpb25hbGx5XG4gICAgLy8gICB0aGUgZXJyb3IgWE1MIGFsc28gcHJvdmlkZXMgUmVnaW9uIG9mIHRoZSBidWNrZXQuIFRvIHZhbGlkYXRlXG4gICAgLy8gICB0aGlzIHJlZ2lvbiBpcyBwcm9wZXIgd2UgcmV0cnkgdGhlIHNhbWUgcmVxdWVzdCB3aXRoIHRoZSBuZXdseVxuICAgIC8vICAgb2J0YWluZWQgcmVnaW9uLlxuICAgIGNvbnN0IHBhdGhTdHlsZSA9IHRoaXMucGF0aFN0eWxlICYmICFpc0Jyb3dzZXJcbiAgICBsZXQgcmVnaW9uOiBzdHJpbmdcbiAgICB0cnkge1xuICAgICAgY29uc3QgcmVzID0gYXdhaXQgdGhpcy5tYWtlUmVxdWVzdEFzeW5jKHsgbWV0aG9kLCBidWNrZXROYW1lLCBxdWVyeSwgcGF0aFN0eWxlIH0sICcnLCBbMjAwXSwgREVGQVVMVF9SRUdJT04pXG4gICAgICByZXR1cm4gZXh0cmFjdFJlZ2lvbkFzeW5jKHJlcylcbiAgICB9IGNhdGNoIChlKSB7XG4gICAgICAvLyBtYWtlIGFsaWdubWVudCB3aXRoIG1jIGNsaVxuICAgICAgaWYgKGUgaW5zdGFuY2VvZiBlcnJvcnMuUzNFcnJvcikge1xuICAgICAgICBjb25zdCBlcnJDb2RlID0gZS5jb2RlXG4gICAgICAgIGNvbnN0IGVyclJlZ2lvbiA9IGUucmVnaW9uXG4gICAgICAgIGlmIChlcnJDb2RlID09PSAnQWNjZXNzRGVuaWVkJyAmJiAhZXJyUmVnaW9uKSB7XG4gICAgICAgICAgcmV0dXJuIERFRkFVTFRfUkVHSU9OXG4gICAgICAgIH1cbiAgICAgIH1cbiAgICAgIC8vIGVzbGludC1kaXNhYmxlLW5leHQtbGluZSBAdHlwZXNjcmlwdC1lc2xpbnQvYmFuLXRzLWNvbW1lbnRcbiAgICAgIC8vIEB0cy1pZ25vcmVcbiAgICAgIGlmICghKGUubmFtZSA9PT0gJ0F1dGhvcml6YXRpb25IZWFkZXJNYWxmb3JtZWQnKSkge1xuICAgICAgICB0aHJvdyBlXG4gICAgICB9XG4gICAgICAvLyBAdHMtZXhwZWN0LWVycm9yIHdlIHNldCBleHRyYSBwcm9wZXJ0aWVzIG9uIGVycm9yIG9iamVjdFxuICAgICAgcmVnaW9uID0gZS5SZWdpb24gYXMgc3RyaW5nXG4gICAgICBpZiAoIXJlZ2lvbikge1xuICAgICAgICB0aHJvdyBlXG4gICAgICB9XG4gICAgfVxuXG4gICAgY29uc3QgcmVzID0gYXdhaXQgdGhpcy5tYWtlUmVxdWVzdEFzeW5jKHsgbWV0aG9kLCBidWNrZXROYW1lLCBxdWVyeSwgcGF0aFN0eWxlIH0sICcnLCBbMjAwXSwgcmVnaW9uKVxuICAgIHJldHVybiBhd2FpdCBleHRyYWN0UmVnaW9uQXN5bmMocmVzKVxuICB9XG5cbiAgLyoqXG4gICAqIG1ha2VSZXF1ZXN0IGlzIHRoZSBwcmltaXRpdmUgdXNlZCBieSB0aGUgYXBpcyBmb3IgbWFraW5nIFMzIHJlcXVlc3RzLlxuICAgKiBwYXlsb2FkIGNhbiBiZSBlbXB0eSBzdHJpbmcgaW4gY2FzZSBvZiBubyBwYXlsb2FkLlxuICAgKiBzdGF0dXNDb2RlIGlzIHRoZSBleHBlY3RlZCBzdGF0dXNDb2RlLiBJZiByZXNwb25zZS5zdGF0dXNDb2RlIGRvZXMgbm90IG1hdGNoXG4gICAqIHdlIHBhcnNlIHRoZSBYTUwgZXJyb3IgYW5kIGNhbGwgdGhlIGNhbGxiYWNrIHdpdGggdGhlIGVycm9yIG1lc3NhZ2UuXG4gICAqIEEgdmFsaWQgcmVnaW9uIGlzIHBhc3NlZCBieSB0aGUgY2FsbHMgLSBsaXN0QnVja2V0cywgbWFrZUJ1Y2tldCBhbmRcbiAgICogZ2V0QnVja2V0UmVnaW9uLlxuICAgKlxuICAgKiBAZGVwcmVjYXRlZCB1c2UgYG1ha2VSZXF1ZXN0QXN5bmNgIGluc3RlYWRcbiAgICovXG4gIG1ha2VSZXF1ZXN0KFxuICAgIG9wdGlvbnM6IFJlcXVlc3RPcHRpb24sXG4gICAgcGF5bG9hZDogQmluYXJ5ID0gJycsXG4gICAgZXhwZWN0ZWRDb2RlczogbnVtYmVyW10gPSBbMjAwXSxcbiAgICByZWdpb24gPSAnJyxcbiAgICByZXR1cm5SZXNwb25zZTogYm9vbGVhbixcbiAgICBjYjogKGNiOiB1bmtub3duLCByZXN1bHQ6IGh0dHAuSW5jb21pbmdNZXNzYWdlKSA9PiB2b2lkLFxuICApIHtcbiAgICBsZXQgcHJvbTogUHJvbWlzZTxodHRwLkluY29taW5nTWVzc2FnZT5cbiAgICBpZiAocmV0dXJuUmVzcG9uc2UpIHtcbiAgICAgIHByb20gPSB0aGlzLm1ha2VSZXF1ZXN0QXN5bmMob3B0aW9ucywgcGF5bG9hZCwgZXhwZWN0ZWRDb2RlcywgcmVnaW9uKVxuICAgIH0gZWxzZSB7XG4gICAgICAvLyBlc2xpbnQtZGlzYWJsZS1uZXh0LWxpbmUgQHR5cGVzY3JpcHQtZXNsaW50L2Jhbi10cy1jb21tZW50XG4gICAgICAvLyBAdHMtZXhwZWN0LWVycm9yIGNvbXBhdGlibGUgZm9yIG9sZCBiZWhhdmlvdXJcbiAgICAgIHByb20gPSB0aGlzLm1ha2VSZXF1ZXN0QXN5bmNPbWl0KG9wdGlvbnMsIHBheWxvYWQsIGV4cGVjdGVkQ29kZXMsIHJlZ2lvbilcbiAgICB9XG5cbiAgICBwcm9tLnRoZW4oXG4gICAgICAocmVzdWx0KSA9PiBjYihudWxsLCByZXN1bHQpLFxuICAgICAgKGVycikgPT4ge1xuICAgICAgICAvLyBlc2xpbnQtZGlzYWJsZS1uZXh0LWxpbmUgQHR5cGVzY3JpcHQtZXNsaW50L2Jhbi10cy1jb21tZW50XG4gICAgICAgIC8vIEB0cy1pZ25vcmVcbiAgICAgICAgY2IoZXJyKVxuICAgICAgfSxcbiAgICApXG4gIH1cblxuICAvKipcbiAgICogbWFrZVJlcXVlc3RTdHJlYW0gd2lsbCBiZSB1c2VkIGRpcmVjdGx5IGluc3RlYWQgb2YgbWFrZVJlcXVlc3QgaW4gY2FzZSB0aGUgcGF5bG9hZFxuICAgKiBpcyBhdmFpbGFibGUgYXMgYSBzdHJlYW0uIGZvciBleC4gcHV0T2JqZWN0XG4gICAqXG4gICAqIEBkZXByZWNhdGVkIHVzZSBgbWFrZVJlcXVlc3RTdHJlYW1Bc3luY2AgaW5zdGVhZFxuICAgKi9cbiAgbWFrZVJlcXVlc3RTdHJlYW0oXG4gICAgb3B0aW9uczogUmVxdWVzdE9wdGlvbixcbiAgICBzdHJlYW06IHN0cmVhbS5SZWFkYWJsZSB8IEJ1ZmZlcixcbiAgICBzaGEyNTZzdW06IHN0cmluZyxcbiAgICBzdGF0dXNDb2RlczogbnVtYmVyW10sXG4gICAgcmVnaW9uOiBzdHJpbmcsXG4gICAgcmV0dXJuUmVzcG9uc2U6IGJvb2xlYW4sXG4gICAgY2I6IChjYjogdW5rbm93biwgcmVzdWx0OiBodHRwLkluY29taW5nTWVzc2FnZSkgPT4gdm9pZCxcbiAgKSB7XG4gICAgY29uc3QgZXhlY3V0b3IgPSBhc3luYyAoKSA9PiB7XG4gICAgICBjb25zdCByZXMgPSBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0U3RyZWFtQXN5bmMob3B0aW9ucywgc3RyZWFtLCBzaGEyNTZzdW0sIHN0YXR1c0NvZGVzLCByZWdpb24pXG4gICAgICBpZiAoIXJldHVyblJlc3BvbnNlKSB7XG4gICAgICAgIGF3YWl0IGRyYWluUmVzcG9uc2UocmVzKVxuICAgICAgfVxuXG4gICAgICByZXR1cm4gcmVzXG4gICAgfVxuXG4gICAgZXhlY3V0b3IoKS50aGVuKFxuICAgICAgKHJlc3VsdCkgPT4gY2IobnVsbCwgcmVzdWx0KSxcbiAgICAgIC8vIGVzbGludC1kaXNhYmxlLW5leHQtbGluZSBAdHlwZXNjcmlwdC1lc2xpbnQvYmFuLXRzLWNvbW1lbnRcbiAgICAgIC8vIEB0cy1pZ25vcmVcbiAgICAgIChlcnIpID0+IGNiKGVyciksXG4gICAgKVxuICB9XG5cbiAgLyoqXG4gICAqIEBkZXByZWNhdGVkIHVzZSBgZ2V0QnVja2V0UmVnaW9uQXN5bmNgIGluc3RlYWRcbiAgICovXG4gIGdldEJ1Y2tldFJlZ2lvbihidWNrZXROYW1lOiBzdHJpbmcsIGNiOiAoZXJyOiB1bmtub3duLCByZWdpb246IHN0cmluZykgPT4gdm9pZCkge1xuICAgIHJldHVybiB0aGlzLmdldEJ1Y2tldFJlZ2lvbkFzeW5jKGJ1Y2tldE5hbWUpLnRoZW4oXG4gICAgICAocmVzdWx0KSA9PiBjYihudWxsLCByZXN1bHQpLFxuICAgICAgLy8gZXNsaW50LWRpc2FibGUtbmV4dC1saW5lIEB0eXBlc2NyaXB0LWVzbGludC9iYW4tdHMtY29tbWVudFxuICAgICAgLy8gQHRzLWlnbm9yZVxuICAgICAgKGVycikgPT4gY2IoZXJyKSxcbiAgICApXG4gIH1cblxuICAvLyBCdWNrZXQgb3BlcmF0aW9uc1xuXG4gIC8qKlxuICAgKiBDcmVhdGVzIHRoZSBidWNrZXQgYGJ1Y2tldE5hbWVgLlxuICAgKlxuICAgKi9cbiAgYXN5bmMgbWFrZUJ1Y2tldChidWNrZXROYW1lOiBzdHJpbmcsIHJlZ2lvbjogUmVnaW9uID0gJycsIG1ha2VPcHRzPzogTWFrZUJ1Y2tldE9wdCk6IFByb21pc2U8dm9pZD4ge1xuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcignSW52YWxpZCBidWNrZXQgbmFtZTogJyArIGJ1Y2tldE5hbWUpXG4gICAgfVxuICAgIC8vIEJhY2t3YXJkIENvbXBhdGliaWxpdHlcbiAgICBpZiAoaXNPYmplY3QocmVnaW9uKSkge1xuICAgICAgbWFrZU9wdHMgPSByZWdpb25cbiAgICAgIHJlZ2lvbiA9ICcnXG4gICAgfVxuXG4gICAgaWYgKCFpc1N0cmluZyhyZWdpb24pKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdyZWdpb24gc2hvdWxkIGJlIG9mIHR5cGUgXCJzdHJpbmdcIicpXG4gICAgfVxuICAgIGlmIChtYWtlT3B0cyAmJiAhaXNPYmplY3QobWFrZU9wdHMpKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdtYWtlT3B0cyBzaG91bGQgYmUgb2YgdHlwZSBcIm9iamVjdFwiJylcbiAgICB9XG5cbiAgICBsZXQgcGF5bG9hZCA9ICcnXG5cbiAgICAvLyBSZWdpb24gYWxyZWFkeSBzZXQgaW4gY29uc3RydWN0b3IsIHZhbGlkYXRlIGlmXG4gICAgLy8gY2FsbGVyIHJlcXVlc3RlZCBidWNrZXQgbG9jYXRpb24gaXMgc2FtZS5cbiAgICBpZiAocmVnaW9uICYmIHRoaXMucmVnaW9uKSB7XG4gICAgICBpZiAocmVnaW9uICE9PSB0aGlzLnJlZ2lvbikge1xuICAgICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKGBDb25maWd1cmVkIHJlZ2lvbiAke3RoaXMucmVnaW9ufSwgcmVxdWVzdGVkICR7cmVnaW9ufWApXG4gICAgICB9XG4gICAgfVxuICAgIC8vIHNlbmRpbmcgbWFrZUJ1Y2tldCByZXF1ZXN0IHdpdGggWE1MIGNvbnRhaW5pbmcgJ3VzLWVhc3QtMScgZmFpbHMuIEZvclxuICAgIC8vIGRlZmF1bHQgcmVnaW9uIHNlcnZlciBleHBlY3RzIHRoZSByZXF1ZXN0IHdpdGhvdXQgYm9keVxuICAgIGlmIChyZWdpb24gJiYgcmVnaW9uICE9PSBERUZBVUxUX1JFR0lPTikge1xuICAgICAgcGF5bG9hZCA9IHhtbC5idWlsZE9iamVjdCh7XG4gICAgICAgIENyZWF0ZUJ1Y2tldENvbmZpZ3VyYXRpb246IHtcbiAgICAgICAgICAkOiB7IHhtbG5zOiAnaHR0cDovL3MzLmFtYXpvbmF3cy5jb20vZG9jLzIwMDYtMDMtMDEvJyB9LFxuICAgICAgICAgIExvY2F0aW9uQ29uc3RyYWludDogcmVnaW9uLFxuICAgICAgICB9LFxuICAgICAgfSlcbiAgICB9XG4gICAgY29uc3QgbWV0aG9kID0gJ1BVVCdcbiAgICBjb25zdCBoZWFkZXJzOiBSZXF1ZXN0SGVhZGVycyA9IHt9XG5cbiAgICBpZiAobWFrZU9wdHMgJiYgbWFrZU9wdHMuT2JqZWN0TG9ja2luZykge1xuICAgICAgaGVhZGVyc1sneC1hbXotYnVja2V0LW9iamVjdC1sb2NrLWVuYWJsZWQnXSA9IHRydWVcbiAgICB9XG5cbiAgICAvLyBGb3IgY3VzdG9tIHJlZ2lvbiBjbGllbnRzICBkZWZhdWx0IHRvIGN1c3RvbSByZWdpb24gc3BlY2lmaWVkIGluIGNsaWVudCBjb25zdHJ1Y3RvclxuICAgIGNvbnN0IGZpbmFsUmVnaW9uID0gdGhpcy5yZWdpb24gfHwgcmVnaW9uIHx8IERFRkFVTFRfUkVHSU9OXG5cbiAgICBjb25zdCByZXF1ZXN0T3B0OiBSZXF1ZXN0T3B0aW9uID0geyBtZXRob2QsIGJ1Y2tldE5hbWUsIGhlYWRlcnMgfVxuXG4gICAgdHJ5IHtcbiAgICAgIGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luY09taXQocmVxdWVzdE9wdCwgcGF5bG9hZCwgWzIwMF0sIGZpbmFsUmVnaW9uKVxuICAgIH0gY2F0Y2ggKGVycjogdW5rbm93bikge1xuICAgICAgaWYgKHJlZ2lvbiA9PT0gJycgfHwgcmVnaW9uID09PSBERUZBVUxUX1JFR0lPTikge1xuICAgICAgICBpZiAoZXJyIGluc3RhbmNlb2YgZXJyb3JzLlMzRXJyb3IpIHtcbiAgICAgICAgICBjb25zdCBlcnJDb2RlID0gZXJyLmNvZGVcbiAgICAgICAgICBjb25zdCBlcnJSZWdpb24gPSBlcnIucmVnaW9uXG4gICAgICAgICAgaWYgKGVyckNvZGUgPT09ICdBdXRob3JpemF0aW9uSGVhZGVyTWFsZm9ybWVkJyAmJiBlcnJSZWdpb24gIT09ICcnKSB7XG4gICAgICAgICAgICAvLyBSZXRyeSB3aXRoIHJlZ2lvbiByZXR1cm5lZCBhcyBwYXJ0IG9mIGVycm9yXG4gICAgICAgICAgICBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0QXN5bmNPbWl0KHJlcXVlc3RPcHQsIHBheWxvYWQsIFsyMDBdLCBlcnJDb2RlKVxuICAgICAgICAgIH1cbiAgICAgICAgfVxuICAgICAgfVxuICAgICAgdGhyb3cgZXJyXG4gICAgfVxuICB9XG5cbiAgLyoqXG4gICAqIFRvIGNoZWNrIGlmIGEgYnVja2V0IGFscmVhZHkgZXhpc3RzLlxuICAgKi9cbiAgYXN5bmMgYnVja2V0RXhpc3RzKGJ1Y2tldE5hbWU6IHN0cmluZyk6IFByb21pc2U8Ym9vbGVhbj4ge1xuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcignSW52YWxpZCBidWNrZXQgbmFtZTogJyArIGJ1Y2tldE5hbWUpXG4gICAgfVxuICAgIGNvbnN0IG1ldGhvZCA9ICdIRUFEJ1xuICAgIHRyeSB7XG4gICAgICBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0QXN5bmNPbWl0KHsgbWV0aG9kLCBidWNrZXROYW1lIH0pXG4gICAgfSBjYXRjaCAoZXJyKSB7XG4gICAgICAvLyBAdHMtaWdub3JlXG4gICAgICBpZiAoZXJyLmNvZGUgPT09ICdOb1N1Y2hCdWNrZXQnIHx8IGVyci5jb2RlID09PSAnTm90Rm91bmQnKSB7XG4gICAgICAgIHJldHVybiBmYWxzZVxuICAgICAgfVxuICAgICAgdGhyb3cgZXJyXG4gICAgfVxuXG4gICAgcmV0dXJuIHRydWVcbiAgfVxuXG4gIGFzeW5jIHJlbW92ZUJ1Y2tldChidWNrZXROYW1lOiBzdHJpbmcpOiBQcm9taXNlPHZvaWQ+XG5cbiAgLyoqXG4gICAqIEBkZXByZWNhdGVkIHVzZSBwcm9taXNlIHN0eWxlIEFQSVxuICAgKi9cbiAgcmVtb3ZlQnVja2V0KGJ1Y2tldE5hbWU6IHN0cmluZywgY2FsbGJhY2s6IE5vUmVzdWx0Q2FsbGJhY2spOiB2b2lkXG5cbiAgYXN5bmMgcmVtb3ZlQnVja2V0KGJ1Y2tldE5hbWU6IHN0cmluZyk6IFByb21pc2U8dm9pZD4ge1xuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcignSW52YWxpZCBidWNrZXQgbmFtZTogJyArIGJ1Y2tldE5hbWUpXG4gICAgfVxuICAgIGNvbnN0IG1ldGhvZCA9ICdERUxFVEUnXG4gICAgYXdhaXQgdGhpcy5tYWtlUmVxdWVzdEFzeW5jT21pdCh7IG1ldGhvZCwgYnVja2V0TmFtZSB9LCAnJywgWzIwNF0pXG4gICAgZGVsZXRlIHRoaXMucmVnaW9uTWFwW2J1Y2tldE5hbWVdXG4gIH1cblxuICAvKipcbiAgICogQ2FsbGJhY2sgaXMgY2FsbGVkIHdpdGggcmVhZGFibGUgc3RyZWFtIG9mIHRoZSBvYmplY3QgY29udGVudC5cbiAgICovXG4gIGFzeW5jIGdldE9iamVjdChidWNrZXROYW1lOiBzdHJpbmcsIG9iamVjdE5hbWU6IHN0cmluZywgZ2V0T3B0cz86IEdldE9iamVjdE9wdHMpOiBQcm9taXNlPHN0cmVhbS5SZWFkYWJsZT4ge1xuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcignSW52YWxpZCBidWNrZXQgbmFtZTogJyArIGJ1Y2tldE5hbWUpXG4gICAgfVxuICAgIGlmICghaXNWYWxpZE9iamVjdE5hbWUob2JqZWN0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZE9iamVjdE5hbWVFcnJvcihgSW52YWxpZCBvYmplY3QgbmFtZTogJHtvYmplY3ROYW1lfWApXG4gICAgfVxuICAgIHJldHVybiB0aGlzLmdldFBhcnRpYWxPYmplY3QoYnVja2V0TmFtZSwgb2JqZWN0TmFtZSwgMCwgMCwgZ2V0T3B0cylcbiAgfVxuXG4gIC8qKlxuICAgKiBDYWxsYmFjayBpcyBjYWxsZWQgd2l0aCByZWFkYWJsZSBzdHJlYW0gb2YgdGhlIHBhcnRpYWwgb2JqZWN0IGNvbnRlbnQuXG4gICAqIEBwYXJhbSBidWNrZXROYW1lXG4gICAqIEBwYXJhbSBvYmplY3ROYW1lXG4gICAqIEBwYXJhbSBvZmZzZXRcbiAgICogQHBhcmFtIGxlbmd0aCAtIGxlbmd0aCBvZiB0aGUgb2JqZWN0IHRoYXQgd2lsbCBiZSByZWFkIGluIHRoZSBzdHJlYW0gKG9wdGlvbmFsLCBpZiBub3Qgc3BlY2lmaWVkIHdlIHJlYWQgdGhlIHJlc3Qgb2YgdGhlIGZpbGUgZnJvbSB0aGUgb2Zmc2V0KVxuICAgKiBAcGFyYW0gZ2V0T3B0c1xuICAgKi9cbiAgYXN5bmMgZ2V0UGFydGlhbE9iamVjdChcbiAgICBidWNrZXROYW1lOiBzdHJpbmcsXG4gICAgb2JqZWN0TmFtZTogc3RyaW5nLFxuICAgIG9mZnNldDogbnVtYmVyLFxuICAgIGxlbmd0aCA9IDAsXG4gICAgZ2V0T3B0cz86IEdldE9iamVjdE9wdHMsXG4gICk6IFByb21pc2U8c3RyZWFtLlJlYWRhYmxlPiB7XG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZShidWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKCdJbnZhbGlkIGJ1Y2tldCBuYW1lOiAnICsgYnVja2V0TmFtZSlcbiAgICB9XG4gICAgaWYgKCFpc1ZhbGlkT2JqZWN0TmFtZShvYmplY3ROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkT2JqZWN0TmFtZUVycm9yKGBJbnZhbGlkIG9iamVjdCBuYW1lOiAke29iamVjdE5hbWV9YClcbiAgICB9XG4gICAgaWYgKCFpc051bWJlcihvZmZzZXQpKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdvZmZzZXQgc2hvdWxkIGJlIG9mIHR5cGUgXCJudW1iZXJcIicpXG4gICAgfVxuICAgIGlmICghaXNOdW1iZXIobGVuZ3RoKSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcignbGVuZ3RoIHNob3VsZCBiZSBvZiB0eXBlIFwibnVtYmVyXCInKVxuICAgIH1cblxuICAgIGxldCByYW5nZSA9ICcnXG4gICAgaWYgKG9mZnNldCB8fCBsZW5ndGgpIHtcbiAgICAgIGlmIChvZmZzZXQpIHtcbiAgICAgICAgcmFuZ2UgPSBgYnl0ZXM9JHsrb2Zmc2V0fS1gXG4gICAgICB9IGVsc2Uge1xuICAgICAgICByYW5nZSA9ICdieXRlcz0wLSdcbiAgICAgICAgb2Zmc2V0ID0gMFxuICAgICAgfVxuICAgICAgaWYgKGxlbmd0aCkge1xuICAgICAgICByYW5nZSArPSBgJHsrbGVuZ3RoICsgb2Zmc2V0IC0gMX1gXG4gICAgICB9XG4gICAgfVxuXG4gICAgbGV0IHF1ZXJ5ID0gJydcbiAgICBsZXQgaGVhZGVyczogUmVxdWVzdEhlYWRlcnMgPSB7XG4gICAgICAuLi4ocmFuZ2UgIT09ICcnICYmIHsgcmFuZ2UgfSksXG4gICAgfVxuXG4gICAgaWYgKGdldE9wdHMpIHtcbiAgICAgIGNvbnN0IHNzZUhlYWRlcnM6IFJlY29yZDxzdHJpbmcsIHN0cmluZz4gPSB7XG4gICAgICAgIC4uLihnZXRPcHRzLlNTRUN1c3RvbWVyQWxnb3JpdGhtICYmIHtcbiAgICAgICAgICAnWC1BbXotU2VydmVyLVNpZGUtRW5jcnlwdGlvbi1DdXN0b21lci1BbGdvcml0aG0nOiBnZXRPcHRzLlNTRUN1c3RvbWVyQWxnb3JpdGhtLFxuICAgICAgICB9KSxcbiAgICAgICAgLi4uKGdldE9wdHMuU1NFQ3VzdG9tZXJLZXkgJiYgeyAnWC1BbXotU2VydmVyLVNpZGUtRW5jcnlwdGlvbi1DdXN0b21lci1LZXknOiBnZXRPcHRzLlNTRUN1c3RvbWVyS2V5IH0pLFxuICAgICAgICAuLi4oZ2V0T3B0cy5TU0VDdXN0b21lcktleU1ENSAmJiB7XG4gICAgICAgICAgJ1gtQW16LVNlcnZlci1TaWRlLUVuY3J5cHRpb24tQ3VzdG9tZXItS2V5LU1ENSc6IGdldE9wdHMuU1NFQ3VzdG9tZXJLZXlNRDUsXG4gICAgICAgIH0pLFxuICAgICAgfVxuICAgICAgcXVlcnkgPSBxcy5zdHJpbmdpZnkoZ2V0T3B0cylcbiAgICAgIGhlYWRlcnMgPSB7XG4gICAgICAgIC4uLnByZXBlbmRYQU1aTWV0YShzc2VIZWFkZXJzKSxcbiAgICAgICAgLi4uaGVhZGVycyxcbiAgICAgIH1cbiAgICB9XG5cbiAgICBjb25zdCBleHBlY3RlZFN0YXR1c0NvZGVzID0gWzIwMF1cbiAgICBpZiAocmFuZ2UpIHtcbiAgICAgIGV4cGVjdGVkU3RhdHVzQ29kZXMucHVzaCgyMDYpXG4gICAgfVxuICAgIGNvbnN0IG1ldGhvZCA9ICdHRVQnXG5cbiAgICByZXR1cm4gYXdhaXQgdGhpcy5tYWtlUmVxdWVzdEFzeW5jKHsgbWV0aG9kLCBidWNrZXROYW1lLCBvYmplY3ROYW1lLCBoZWFkZXJzLCBxdWVyeSB9LCAnJywgZXhwZWN0ZWRTdGF0dXNDb2RlcylcbiAgfVxuXG4gIC8qKlxuICAgKiBkb3dubG9hZCBvYmplY3QgY29udGVudCB0byBhIGZpbGUuXG4gICAqIFRoaXMgbWV0aG9kIHdpbGwgY3JlYXRlIGEgdGVtcCBmaWxlIG5hbWVkIGAke2ZpbGVuYW1lfS4ke2Jhc2U2NChldGFnKX0ucGFydC5taW5pb2Agd2hlbiBkb3dubG9hZGluZy5cbiAgICpcbiAgICogQHBhcmFtIGJ1Y2tldE5hbWUgLSBuYW1lIG9mIHRoZSBidWNrZXRcbiAgICogQHBhcmFtIG9iamVjdE5hbWUgLSBuYW1lIG9mIHRoZSBvYmplY3RcbiAgICogQHBhcmFtIGZpbGVQYXRoIC0gcGF0aCB0byB3aGljaCB0aGUgb2JqZWN0IGRhdGEgd2lsbCBiZSB3cml0dGVuIHRvXG4gICAqIEBwYXJhbSBnZXRPcHRzIC0gT3B0aW9uYWwgb2JqZWN0IGdldCBvcHRpb25cbiAgICovXG4gIGFzeW5jIGZHZXRPYmplY3QoYnVja2V0TmFtZTogc3RyaW5nLCBvYmplY3ROYW1lOiBzdHJpbmcsIGZpbGVQYXRoOiBzdHJpbmcsIGdldE9wdHM/OiBHZXRPYmplY3RPcHRzKTogUHJvbWlzZTx2b2lkPiB7XG4gICAgLy8gSW5wdXQgdmFsaWRhdGlvbi5cbiAgICBpZiAoIWlzVmFsaWRCdWNrZXROYW1lKGJ1Y2tldE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IoJ0ludmFsaWQgYnVja2V0IG5hbWU6ICcgKyBidWNrZXROYW1lKVxuICAgIH1cbiAgICBpZiAoIWlzVmFsaWRPYmplY3ROYW1lKG9iamVjdE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRPYmplY3ROYW1lRXJyb3IoYEludmFsaWQgb2JqZWN0IG5hbWU6ICR7b2JqZWN0TmFtZX1gKVxuICAgIH1cbiAgICBpZiAoIWlzU3RyaW5nKGZpbGVQYXRoKSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcignZmlsZVBhdGggc2hvdWxkIGJlIG9mIHR5cGUgXCJzdHJpbmdcIicpXG4gICAgfVxuXG4gICAgY29uc3QgZG93bmxvYWRUb1RtcEZpbGUgPSBhc3luYyAoKTogUHJvbWlzZTxzdHJpbmc+ID0+IHtcbiAgICAgIGxldCBwYXJ0RmlsZVN0cmVhbTogc3RyZWFtLldyaXRhYmxlXG4gICAgICBjb25zdCBvYmpTdGF0ID0gYXdhaXQgdGhpcy5zdGF0T2JqZWN0KGJ1Y2tldE5hbWUsIG9iamVjdE5hbWUsIGdldE9wdHMpXG4gICAgICBjb25zdCBlbmNvZGVkRXRhZyA9IEJ1ZmZlci5mcm9tKG9ialN0YXQuZXRhZykudG9TdHJpbmcoJ2Jhc2U2NCcpXG4gICAgICBjb25zdCBwYXJ0RmlsZSA9IGAke2ZpbGVQYXRofS4ke2VuY29kZWRFdGFnfS5wYXJ0Lm1pbmlvYFxuXG4gICAgICBhd2FpdCBmc3AubWtkaXIocGF0aC5kaXJuYW1lKGZpbGVQYXRoKSwgeyByZWN1cnNpdmU6IHRydWUgfSlcblxuICAgICAgbGV0IG9mZnNldCA9IDBcbiAgICAgIHRyeSB7XG4gICAgICAgIGNvbnN0IHN0YXRzID0gYXdhaXQgZnNwLnN0YXQocGFydEZpbGUpXG4gICAgICAgIGlmIChvYmpTdGF0LnNpemUgPT09IHN0YXRzLnNpemUpIHtcbiAgICAgICAgICByZXR1cm4gcGFydEZpbGVcbiAgICAgICAgfVxuICAgICAgICBvZmZzZXQgPSBzdGF0cy5zaXplXG4gICAgICAgIHBhcnRGaWxlU3RyZWFtID0gZnMuY3JlYXRlV3JpdGVTdHJlYW0ocGFydEZpbGUsIHsgZmxhZ3M6ICdhJyB9KVxuICAgICAgfSBjYXRjaCAoZSkge1xuICAgICAgICBpZiAoZSBpbnN0YW5jZW9mIEVycm9yICYmIChlIGFzIHVua25vd24gYXMgeyBjb2RlOiBzdHJpbmcgfSkuY29kZSA9PT0gJ0VOT0VOVCcpIHtcbiAgICAgICAgICAvLyBmaWxlIG5vdCBleGlzdFxuICAgICAgICAgIHBhcnRGaWxlU3RyZWFtID0gZnMuY3JlYXRlV3JpdGVTdHJlYW0ocGFydEZpbGUsIHsgZmxhZ3M6ICd3JyB9KVxuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIC8vIG90aGVyIGVycm9yLCBtYXliZSBhY2Nlc3MgZGVueVxuICAgICAgICAgIHRocm93IGVcbiAgICAgICAgfVxuICAgICAgfVxuXG4gICAgICBjb25zdCBkb3dubG9hZFN0cmVhbSA9IGF3YWl0IHRoaXMuZ2V0UGFydGlhbE9iamVjdChidWNrZXROYW1lLCBvYmplY3ROYW1lLCBvZmZzZXQsIDAsIGdldE9wdHMpXG5cbiAgICAgIGF3YWl0IHN0cmVhbVByb21pc2UucGlwZWxpbmUoZG93bmxvYWRTdHJlYW0sIHBhcnRGaWxlU3RyZWFtKVxuICAgICAgY29uc3Qgc3RhdHMgPSBhd2FpdCBmc3Auc3RhdChwYXJ0RmlsZSlcbiAgICAgIGlmIChzdGF0cy5zaXplID09PSBvYmpTdGF0LnNpemUpIHtcbiAgICAgICAgcmV0dXJuIHBhcnRGaWxlXG4gICAgICB9XG5cbiAgICAgIHRocm93IG5ldyBFcnJvcignU2l6ZSBtaXNtYXRjaCBiZXR3ZWVuIGRvd25sb2FkZWQgZmlsZSBhbmQgdGhlIG9iamVjdCcpXG4gICAgfVxuXG4gICAgY29uc3QgcGFydEZpbGUgPSBhd2FpdCBkb3dubG9hZFRvVG1wRmlsZSgpXG4gICAgYXdhaXQgZnNwLnJlbmFtZShwYXJ0RmlsZSwgZmlsZVBhdGgpXG4gIH1cblxuICAvKipcbiAgICogU3RhdCBpbmZvcm1hdGlvbiBvZiB0aGUgb2JqZWN0LlxuICAgKi9cbiAgYXN5bmMgc3RhdE9iamVjdChidWNrZXROYW1lOiBzdHJpbmcsIG9iamVjdE5hbWU6IHN0cmluZywgc3RhdE9wdHM/OiBTdGF0T2JqZWN0T3B0cyk6IFByb21pc2U8QnVja2V0SXRlbVN0YXQ+IHtcbiAgICBjb25zdCBzdGF0T3B0RGVmID0gc3RhdE9wdHMgfHwge31cbiAgICBpZiAoIWlzVmFsaWRCdWNrZXROYW1lKGJ1Y2tldE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IoJ0ludmFsaWQgYnVja2V0IG5hbWU6ICcgKyBidWNrZXROYW1lKVxuICAgIH1cbiAgICBpZiAoIWlzVmFsaWRPYmplY3ROYW1lKG9iamVjdE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRPYmplY3ROYW1lRXJyb3IoYEludmFsaWQgb2JqZWN0IG5hbWU6ICR7b2JqZWN0TmFtZX1gKVxuICAgIH1cblxuICAgIGlmICghaXNPYmplY3Qoc3RhdE9wdERlZikpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEFyZ3VtZW50RXJyb3IoJ3N0YXRPcHRzIHNob3VsZCBiZSBvZiB0eXBlIFwib2JqZWN0XCInKVxuICAgIH1cblxuICAgIGNvbnN0IHF1ZXJ5ID0gcXMuc3RyaW5naWZ5KHN0YXRPcHREZWYpXG4gICAgY29uc3QgbWV0aG9kID0gJ0hFQUQnXG4gICAgY29uc3QgcmVzID0gYXdhaXQgdGhpcy5tYWtlUmVxdWVzdEFzeW5jT21pdCh7IG1ldGhvZCwgYnVja2V0TmFtZSwgb2JqZWN0TmFtZSwgcXVlcnkgfSlcblxuICAgIHJldHVybiB7XG4gICAgICBzaXplOiBwYXJzZUludChyZXMuaGVhZGVyc1snY29udGVudC1sZW5ndGgnXSBhcyBzdHJpbmcpLFxuICAgICAgbWV0YURhdGE6IGV4dHJhY3RNZXRhZGF0YShyZXMuaGVhZGVycyBhcyBSZXNwb25zZUhlYWRlciksXG4gICAgICBsYXN0TW9kaWZpZWQ6IG5ldyBEYXRlKHJlcy5oZWFkZXJzWydsYXN0LW1vZGlmaWVkJ10gYXMgc3RyaW5nKSxcbiAgICAgIHZlcnNpb25JZDogZ2V0VmVyc2lvbklkKHJlcy5oZWFkZXJzIGFzIFJlc3BvbnNlSGVhZGVyKSxcbiAgICAgIGV0YWc6IHNhbml0aXplRVRhZyhyZXMuaGVhZGVycy5ldGFnKSxcbiAgICB9XG4gIH1cblxuICBhc3luYyByZW1vdmVPYmplY3QoYnVja2V0TmFtZTogc3RyaW5nLCBvYmplY3ROYW1lOiBzdHJpbmcsIHJlbW92ZU9wdHM/OiBSZW1vdmVPcHRpb25zKTogUHJvbWlzZTx2b2lkPiB7XG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZShidWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKGBJbnZhbGlkIGJ1Y2tldCBuYW1lOiAke2J1Y2tldE5hbWV9YClcbiAgICB9XG4gICAgaWYgKCFpc1ZhbGlkT2JqZWN0TmFtZShvYmplY3ROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkT2JqZWN0TmFtZUVycm9yKGBJbnZhbGlkIG9iamVjdCBuYW1lOiAke29iamVjdE5hbWV9YClcbiAgICB9XG5cbiAgICBpZiAocmVtb3ZlT3B0cyAmJiAhaXNPYmplY3QocmVtb3ZlT3B0cykpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEFyZ3VtZW50RXJyb3IoJ3JlbW92ZU9wdHMgc2hvdWxkIGJlIG9mIHR5cGUgXCJvYmplY3RcIicpXG4gICAgfVxuXG4gICAgY29uc3QgbWV0aG9kID0gJ0RFTEVURSdcblxuICAgIGNvbnN0IGhlYWRlcnM6IFJlcXVlc3RIZWFkZXJzID0ge31cbiAgICBpZiAocmVtb3ZlT3B0cz8uZ292ZXJuYW5jZUJ5cGFzcykge1xuICAgICAgaGVhZGVyc1snWC1BbXotQnlwYXNzLUdvdmVybmFuY2UtUmV0ZW50aW9uJ10gPSB0cnVlXG4gICAgfVxuICAgIGlmIChyZW1vdmVPcHRzPy5mb3JjZURlbGV0ZSkge1xuICAgICAgaGVhZGVyc1sneC1taW5pby1mb3JjZS1kZWxldGUnXSA9IHRydWVcbiAgICB9XG5cbiAgICBjb25zdCBxdWVyeVBhcmFtczogUmVjb3JkPHN0cmluZywgc3RyaW5nPiA9IHt9XG4gICAgaWYgKHJlbW92ZU9wdHM/LnZlcnNpb25JZCkge1xuICAgICAgcXVlcnlQYXJhbXMudmVyc2lvbklkID0gYCR7cmVtb3ZlT3B0cy52ZXJzaW9uSWR9YFxuICAgIH1cbiAgICBjb25zdCBxdWVyeSA9IHFzLnN0cmluZ2lmeShxdWVyeVBhcmFtcylcblxuICAgIGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luY09taXQoeyBtZXRob2QsIGJ1Y2tldE5hbWUsIG9iamVjdE5hbWUsIGhlYWRlcnMsIHF1ZXJ5IH0sICcnLCBbMjAwLCAyMDRdKVxuICB9XG5cbiAgLy8gQ2FsbHMgaW1wbGVtZW50ZWQgYmVsb3cgYXJlIHJlbGF0ZWQgdG8gbXVsdGlwYXJ0LlxuXG4gIGxpc3RJbmNvbXBsZXRlVXBsb2FkcyhcbiAgICBidWNrZXQ6IHN0cmluZyxcbiAgICBwcmVmaXg6IHN0cmluZyxcbiAgICByZWN1cnNpdmU6IGJvb2xlYW4sXG4gICk6IEJ1Y2tldFN0cmVhbTxJbmNvbXBsZXRlVXBsb2FkZWRCdWNrZXRJdGVtPiB7XG4gICAgaWYgKHByZWZpeCA9PT0gdW5kZWZpbmVkKSB7XG4gICAgICBwcmVmaXggPSAnJ1xuICAgIH1cbiAgICBpZiAocmVjdXJzaXZlID09PSB1bmRlZmluZWQpIHtcbiAgICAgIHJlY3Vyc2l2ZSA9IGZhbHNlXG4gICAgfVxuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0KSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKCdJbnZhbGlkIGJ1Y2tldCBuYW1lOiAnICsgYnVja2V0KVxuICAgIH1cbiAgICBpZiAoIWlzVmFsaWRQcmVmaXgocHJlZml4KSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkUHJlZml4RXJyb3IoYEludmFsaWQgcHJlZml4IDogJHtwcmVmaXh9YClcbiAgICB9XG4gICAgaWYgKCFpc0Jvb2xlYW4ocmVjdXJzaXZlKSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcigncmVjdXJzaXZlIHNob3VsZCBiZSBvZiB0eXBlIFwiYm9vbGVhblwiJylcbiAgICB9XG4gICAgY29uc3QgZGVsaW1pdGVyID0gcmVjdXJzaXZlID8gJycgOiAnLydcbiAgICBsZXQga2V5TWFya2VyID0gJydcbiAgICBsZXQgdXBsb2FkSWRNYXJrZXIgPSAnJ1xuICAgIGNvbnN0IHVwbG9hZHM6IHVua25vd25bXSA9IFtdXG4gICAgbGV0IGVuZGVkID0gZmFsc2VcblxuICAgIC8vIFRPRE86IHJlZmFjdG9yIHRoaXMgd2l0aCBhc3luYy9hd2FpdCBhbmQgYHN0cmVhbS5SZWFkYWJsZS5mcm9tYFxuICAgIGNvbnN0IHJlYWRTdHJlYW0gPSBuZXcgc3RyZWFtLlJlYWRhYmxlKHsgb2JqZWN0TW9kZTogdHJ1ZSB9KVxuICAgIHJlYWRTdHJlYW0uX3JlYWQgPSAoKSA9PiB7XG4gICAgICAvLyBwdXNoIG9uZSB1cGxvYWQgaW5mbyBwZXIgX3JlYWQoKVxuICAgICAgaWYgKHVwbG9hZHMubGVuZ3RoKSB7XG4gICAgICAgIHJldHVybiByZWFkU3RyZWFtLnB1c2godXBsb2Fkcy5zaGlmdCgpKVxuICAgICAgfVxuICAgICAgaWYgKGVuZGVkKSB7XG4gICAgICAgIHJldHVybiByZWFkU3RyZWFtLnB1c2gobnVsbClcbiAgICAgIH1cbiAgICAgIHRoaXMubGlzdEluY29tcGxldGVVcGxvYWRzUXVlcnkoYnVja2V0LCBwcmVmaXgsIGtleU1hcmtlciwgdXBsb2FkSWRNYXJrZXIsIGRlbGltaXRlcikudGhlbihcbiAgICAgICAgKHJlc3VsdCkgPT4ge1xuICAgICAgICAgIC8vIGVzbGludC1kaXNhYmxlLW5leHQtbGluZSBAdHlwZXNjcmlwdC1lc2xpbnQvYmFuLXRzLWNvbW1lbnRcbiAgICAgICAgICAvLyBAdHMtaWdub3JlXG4gICAgICAgICAgcmVzdWx0LnByZWZpeGVzLmZvckVhY2goKHByZWZpeCkgPT4gdXBsb2Fkcy5wdXNoKHByZWZpeCkpXG4gICAgICAgICAgYXN5bmMuZWFjaFNlcmllcyhcbiAgICAgICAgICAgIHJlc3VsdC51cGxvYWRzLFxuICAgICAgICAgICAgKHVwbG9hZCwgY2IpID0+IHtcbiAgICAgICAgICAgICAgLy8gZm9yIGVhY2ggaW5jb21wbGV0ZSB1cGxvYWQgYWRkIHRoZSBzaXplcyBvZiBpdHMgdXBsb2FkZWQgcGFydHNcbiAgICAgICAgICAgICAgLy8gZXNsaW50LWRpc2FibGUtbmV4dC1saW5lIEB0eXBlc2NyaXB0LWVzbGludC9iYW4tdHMtY29tbWVudFxuICAgICAgICAgICAgICAvLyBAdHMtaWdub3JlXG4gICAgICAgICAgICAgIHRoaXMubGlzdFBhcnRzKGJ1Y2tldCwgdXBsb2FkLmtleSwgdXBsb2FkLnVwbG9hZElkKS50aGVuKFxuICAgICAgICAgICAgICAgIChwYXJ0czogUGFydFtdKSA9PiB7XG4gICAgICAgICAgICAgICAgICAvLyBlc2xpbnQtZGlzYWJsZS1uZXh0LWxpbmUgQHR5cGVzY3JpcHQtZXNsaW50L2Jhbi10cy1jb21tZW50XG4gICAgICAgICAgICAgICAgICAvLyBAdHMtaWdub3JlXG4gICAgICAgICAgICAgICAgICB1cGxvYWQuc2l6ZSA9IHBhcnRzLnJlZHVjZSgoYWNjLCBpdGVtKSA9PiBhY2MgKyBpdGVtLnNpemUsIDApXG4gICAgICAgICAgICAgICAgICB1cGxvYWRzLnB1c2godXBsb2FkKVxuICAgICAgICAgICAgICAgICAgY2IoKVxuICAgICAgICAgICAgICAgIH0sXG4gICAgICAgICAgICAgICAgKGVycjogRXJyb3IpID0+IGNiKGVyciksXG4gICAgICAgICAgICAgIClcbiAgICAgICAgICAgIH0sXG4gICAgICAgICAgICAoZXJyKSA9PiB7XG4gICAgICAgICAgICAgIGlmIChlcnIpIHtcbiAgICAgICAgICAgICAgICByZWFkU3RyZWFtLmVtaXQoJ2Vycm9yJywgZXJyKVxuICAgICAgICAgICAgICAgIHJldHVyblxuICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgIGlmIChyZXN1bHQuaXNUcnVuY2F0ZWQpIHtcbiAgICAgICAgICAgICAgICBrZXlNYXJrZXIgPSByZXN1bHQubmV4dEtleU1hcmtlclxuICAgICAgICAgICAgICAgIHVwbG9hZElkTWFya2VyID0gcmVzdWx0Lm5leHRVcGxvYWRJZE1hcmtlclxuICAgICAgICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgICAgICAgIGVuZGVkID0gdHJ1ZVxuICAgICAgICAgICAgICB9XG5cbiAgICAgICAgICAgICAgLy8gZXNsaW50LWRpc2FibGUtbmV4dC1saW5lIEB0eXBlc2NyaXB0LWVzbGludC9iYW4tdHMtY29tbWVudFxuICAgICAgICAgICAgICAvLyBAdHMtaWdub3JlXG4gICAgICAgICAgICAgIHJlYWRTdHJlYW0uX3JlYWQoKVxuICAgICAgICAgICAgfSxcbiAgICAgICAgICApXG4gICAgICAgIH0sXG4gICAgICAgIChlKSA9PiB7XG4gICAgICAgICAgcmVhZFN0cmVhbS5lbWl0KCdlcnJvcicsIGUpXG4gICAgICAgIH0sXG4gICAgICApXG4gICAgfVxuICAgIHJldHVybiByZWFkU3RyZWFtXG4gIH1cblxuICAvKipcbiAgICogQ2FsbGVkIGJ5IGxpc3RJbmNvbXBsZXRlVXBsb2FkcyB0byBmZXRjaCBhIGJhdGNoIG9mIGluY29tcGxldGUgdXBsb2Fkcy5cbiAgICovXG4gIGFzeW5jIGxpc3RJbmNvbXBsZXRlVXBsb2Fkc1F1ZXJ5KFxuICAgIGJ1Y2tldE5hbWU6IHN0cmluZyxcbiAgICBwcmVmaXg6IHN0cmluZyxcbiAgICBrZXlNYXJrZXI6IHN0cmluZyxcbiAgICB1cGxvYWRJZE1hcmtlcjogc3RyaW5nLFxuICAgIGRlbGltaXRlcjogc3RyaW5nLFxuICApOiBQcm9taXNlPExpc3RNdWx0aXBhcnRSZXN1bHQ+IHtcbiAgICBpZiAoIWlzVmFsaWRCdWNrZXROYW1lKGJ1Y2tldE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IoJ0ludmFsaWQgYnVja2V0IG5hbWU6ICcgKyBidWNrZXROYW1lKVxuICAgIH1cbiAgICBpZiAoIWlzU3RyaW5nKHByZWZpeCkpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ3ByZWZpeCBzaG91bGQgYmUgb2YgdHlwZSBcInN0cmluZ1wiJylcbiAgICB9XG4gICAgaWYgKCFpc1N0cmluZyhrZXlNYXJrZXIpKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdrZXlNYXJrZXIgc2hvdWxkIGJlIG9mIHR5cGUgXCJzdHJpbmdcIicpXG4gICAgfVxuICAgIGlmICghaXNTdHJpbmcodXBsb2FkSWRNYXJrZXIpKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCd1cGxvYWRJZE1hcmtlciBzaG91bGQgYmUgb2YgdHlwZSBcInN0cmluZ1wiJylcbiAgICB9XG4gICAgaWYgKCFpc1N0cmluZyhkZWxpbWl0ZXIpKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdkZWxpbWl0ZXIgc2hvdWxkIGJlIG9mIHR5cGUgXCJzdHJpbmdcIicpXG4gICAgfVxuICAgIGNvbnN0IHF1ZXJpZXMgPSBbXVxuICAgIHF1ZXJpZXMucHVzaChgcHJlZml4PSR7dXJpRXNjYXBlKHByZWZpeCl9YClcbiAgICBxdWVyaWVzLnB1c2goYGRlbGltaXRlcj0ke3VyaUVzY2FwZShkZWxpbWl0ZXIpfWApXG5cbiAgICBpZiAoa2V5TWFya2VyKSB7XG4gICAgICBxdWVyaWVzLnB1c2goYGtleS1tYXJrZXI9JHt1cmlFc2NhcGUoa2V5TWFya2VyKX1gKVxuICAgIH1cbiAgICBpZiAodXBsb2FkSWRNYXJrZXIpIHtcbiAgICAgIHF1ZXJpZXMucHVzaChgdXBsb2FkLWlkLW1hcmtlcj0ke3VwbG9hZElkTWFya2VyfWApXG4gICAgfVxuXG4gICAgY29uc3QgbWF4VXBsb2FkcyA9IDEwMDBcbiAgICBxdWVyaWVzLnB1c2goYG1heC11cGxvYWRzPSR7bWF4VXBsb2Fkc31gKVxuICAgIHF1ZXJpZXMuc29ydCgpXG4gICAgcXVlcmllcy51bnNoaWZ0KCd1cGxvYWRzJylcbiAgICBsZXQgcXVlcnkgPSAnJ1xuICAgIGlmIChxdWVyaWVzLmxlbmd0aCA+IDApIHtcbiAgICAgIHF1ZXJ5ID0gYCR7cXVlcmllcy5qb2luKCcmJyl9YFxuICAgIH1cbiAgICBjb25zdCBtZXRob2QgPSAnR0VUJ1xuICAgIGNvbnN0IHJlcyA9IGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luYyh7IG1ldGhvZCwgYnVja2V0TmFtZSwgcXVlcnkgfSlcbiAgICBjb25zdCBib2R5ID0gYXdhaXQgcmVhZEFzU3RyaW5nKHJlcylcbiAgICByZXR1cm4geG1sUGFyc2Vycy5wYXJzZUxpc3RNdWx0aXBhcnQoYm9keSlcbiAgfVxuXG4gIC8qKlxuICAgKiBJbml0aWF0ZSBhIG5ldyBtdWx0aXBhcnQgdXBsb2FkLlxuICAgKiBAaW50ZXJuYWxcbiAgICovXG4gIGFzeW5jIGluaXRpYXRlTmV3TXVsdGlwYXJ0VXBsb2FkKGJ1Y2tldE5hbWU6IHN0cmluZywgb2JqZWN0TmFtZTogc3RyaW5nLCBoZWFkZXJzOiBSZXF1ZXN0SGVhZGVycyk6IFByb21pc2U8c3RyaW5nPiB7XG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZShidWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKCdJbnZhbGlkIGJ1Y2tldCBuYW1lOiAnICsgYnVja2V0TmFtZSlcbiAgICB9XG4gICAgaWYgKCFpc1ZhbGlkT2JqZWN0TmFtZShvYmplY3ROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkT2JqZWN0TmFtZUVycm9yKGBJbnZhbGlkIG9iamVjdCBuYW1lOiAke29iamVjdE5hbWV9YClcbiAgICB9XG4gICAgaWYgKCFpc09iamVjdChoZWFkZXJzKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkT2JqZWN0TmFtZUVycm9yKCdjb250ZW50VHlwZSBzaG91bGQgYmUgb2YgdHlwZSBcIm9iamVjdFwiJylcbiAgICB9XG4gICAgY29uc3QgbWV0aG9kID0gJ1BPU1QnXG4gICAgY29uc3QgcXVlcnkgPSAndXBsb2FkcydcbiAgICBjb25zdCByZXMgPSBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0QXN5bmMoeyBtZXRob2QsIGJ1Y2tldE5hbWUsIG9iamVjdE5hbWUsIHF1ZXJ5LCBoZWFkZXJzIH0pXG4gICAgY29uc3QgYm9keSA9IGF3YWl0IHJlYWRBc0J1ZmZlcihyZXMpXG4gICAgcmV0dXJuIHBhcnNlSW5pdGlhdGVNdWx0aXBhcnQoYm9keS50b1N0cmluZygpKVxuICB9XG5cbiAgLyoqXG4gICAqIEludGVybmFsIE1ldGhvZCB0byBhYm9ydCBhIG11bHRpcGFydCB1cGxvYWQgcmVxdWVzdCBpbiBjYXNlIG9mIGFueSBlcnJvcnMuXG4gICAqXG4gICAqIEBwYXJhbSBidWNrZXROYW1lIC0gQnVja2V0IE5hbWVcbiAgICogQHBhcmFtIG9iamVjdE5hbWUgLSBPYmplY3QgTmFtZVxuICAgKiBAcGFyYW0gdXBsb2FkSWQgLSBpZCBvZiBhIG11bHRpcGFydCB1cGxvYWQgdG8gY2FuY2VsIGR1cmluZyBjb21wb3NlIG9iamVjdCBzZXF1ZW5jZS5cbiAgICovXG4gIGFzeW5jIGFib3J0TXVsdGlwYXJ0VXBsb2FkKGJ1Y2tldE5hbWU6IHN0cmluZywgb2JqZWN0TmFtZTogc3RyaW5nLCB1cGxvYWRJZDogc3RyaW5nKTogUHJvbWlzZTx2b2lkPiB7XG4gICAgY29uc3QgbWV0aG9kID0gJ0RFTEVURSdcbiAgICBjb25zdCBxdWVyeSA9IGB1cGxvYWRJZD0ke3VwbG9hZElkfWBcblxuICAgIGNvbnN0IHJlcXVlc3RPcHRpb25zID0geyBtZXRob2QsIGJ1Y2tldE5hbWUsIG9iamVjdE5hbWU6IG9iamVjdE5hbWUsIHF1ZXJ5IH1cbiAgICBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0QXN5bmNPbWl0KHJlcXVlc3RPcHRpb25zLCAnJywgWzIwNF0pXG4gIH1cblxuICBhc3luYyBmaW5kVXBsb2FkSWQoYnVja2V0TmFtZTogc3RyaW5nLCBvYmplY3ROYW1lOiBzdHJpbmcpOiBQcm9taXNlPHN0cmluZyB8IHVuZGVmaW5lZD4ge1xuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcignSW52YWxpZCBidWNrZXQgbmFtZTogJyArIGJ1Y2tldE5hbWUpXG4gICAgfVxuICAgIGlmICghaXNWYWxpZE9iamVjdE5hbWUob2JqZWN0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZE9iamVjdE5hbWVFcnJvcihgSW52YWxpZCBvYmplY3QgbmFtZTogJHtvYmplY3ROYW1lfWApXG4gICAgfVxuXG4gICAgbGV0IGxhdGVzdFVwbG9hZDogTGlzdE11bHRpcGFydFJlc3VsdFsndXBsb2FkcyddW251bWJlcl0gfCB1bmRlZmluZWRcbiAgICBsZXQga2V5TWFya2VyID0gJydcbiAgICBsZXQgdXBsb2FkSWRNYXJrZXIgPSAnJ1xuICAgIGZvciAoOzspIHtcbiAgICAgIGNvbnN0IHJlc3VsdCA9IGF3YWl0IHRoaXMubGlzdEluY29tcGxldGVVcGxvYWRzUXVlcnkoYnVja2V0TmFtZSwgb2JqZWN0TmFtZSwga2V5TWFya2VyLCB1cGxvYWRJZE1hcmtlciwgJycpXG4gICAgICBmb3IgKGNvbnN0IHVwbG9hZCBvZiByZXN1bHQudXBsb2Fkcykge1xuICAgICAgICBpZiAodXBsb2FkLmtleSA9PT0gb2JqZWN0TmFtZSkge1xuICAgICAgICAgIGlmICghbGF0ZXN0VXBsb2FkIHx8IHVwbG9hZC5pbml0aWF0ZWQuZ2V0VGltZSgpID4gbGF0ZXN0VXBsb2FkLmluaXRpYXRlZC5nZXRUaW1lKCkpIHtcbiAgICAgICAgICAgIGxhdGVzdFVwbG9hZCA9IHVwbG9hZFxuICAgICAgICAgIH1cbiAgICAgICAgfVxuICAgICAgfVxuICAgICAgaWYgKHJlc3VsdC5pc1RydW5jYXRlZCkge1xuICAgICAgICBrZXlNYXJrZXIgPSByZXN1bHQubmV4dEtleU1hcmtlclxuICAgICAgICB1cGxvYWRJZE1hcmtlciA9IHJlc3VsdC5uZXh0VXBsb2FkSWRNYXJrZXJcbiAgICAgICAgY29udGludWVcbiAgICAgIH1cblxuICAgICAgYnJlYWtcbiAgICB9XG4gICAgcmV0dXJuIGxhdGVzdFVwbG9hZD8udXBsb2FkSWRcbiAgfVxuXG4gIC8qKlxuICAgKiB0aGlzIGNhbGwgd2lsbCBhZ2dyZWdhdGUgdGhlIHBhcnRzIG9uIHRoZSBzZXJ2ZXIgaW50byBhIHNpbmdsZSBvYmplY3QuXG4gICAqL1xuICBhc3luYyBjb21wbGV0ZU11bHRpcGFydFVwbG9hZChcbiAgICBidWNrZXROYW1lOiBzdHJpbmcsXG4gICAgb2JqZWN0TmFtZTogc3RyaW5nLFxuICAgIHVwbG9hZElkOiBzdHJpbmcsXG4gICAgZXRhZ3M6IHtcbiAgICAgIHBhcnQ6IG51bWJlclxuICAgICAgZXRhZz86IHN0cmluZ1xuICAgIH1bXSxcbiAgKTogUHJvbWlzZTx7IGV0YWc6IHN0cmluZzsgdmVyc2lvbklkOiBzdHJpbmcgfCBudWxsIH0+IHtcbiAgICBpZiAoIWlzVmFsaWRCdWNrZXROYW1lKGJ1Y2tldE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IoJ0ludmFsaWQgYnVja2V0IG5hbWU6ICcgKyBidWNrZXROYW1lKVxuICAgIH1cbiAgICBpZiAoIWlzVmFsaWRPYmplY3ROYW1lKG9iamVjdE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRPYmplY3ROYW1lRXJyb3IoYEludmFsaWQgb2JqZWN0IG5hbWU6ICR7b2JqZWN0TmFtZX1gKVxuICAgIH1cbiAgICBpZiAoIWlzU3RyaW5nKHVwbG9hZElkKSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcigndXBsb2FkSWQgc2hvdWxkIGJlIG9mIHR5cGUgXCJzdHJpbmdcIicpXG4gICAgfVxuICAgIGlmICghaXNPYmplY3QoZXRhZ3MpKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdldGFncyBzaG91bGQgYmUgb2YgdHlwZSBcIkFycmF5XCInKVxuICAgIH1cblxuICAgIGlmICghdXBsb2FkSWQpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEFyZ3VtZW50RXJyb3IoJ3VwbG9hZElkIGNhbm5vdCBiZSBlbXB0eScpXG4gICAgfVxuXG4gICAgY29uc3QgbWV0aG9kID0gJ1BPU1QnXG4gICAgY29uc3QgcXVlcnkgPSBgdXBsb2FkSWQ9JHt1cmlFc2NhcGUodXBsb2FkSWQpfWBcblxuICAgIGNvbnN0IGJ1aWxkZXIgPSBuZXcgeG1sMmpzLkJ1aWxkZXIoKVxuICAgIGNvbnN0IHBheWxvYWQgPSBidWlsZGVyLmJ1aWxkT2JqZWN0KHtcbiAgICAgIENvbXBsZXRlTXVsdGlwYXJ0VXBsb2FkOiB7XG4gICAgICAgICQ6IHtcbiAgICAgICAgICB4bWxuczogJ2h0dHA6Ly9zMy5hbWF6b25hd3MuY29tL2RvYy8yMDA2LTAzLTAxLycsXG4gICAgICAgIH0sXG4gICAgICAgIFBhcnQ6IGV0YWdzLm1hcCgoZXRhZykgPT4ge1xuICAgICAgICAgIHJldHVybiB7XG4gICAgICAgICAgICBQYXJ0TnVtYmVyOiBldGFnLnBhcnQsXG4gICAgICAgICAgICBFVGFnOiBldGFnLmV0YWcsXG4gICAgICAgICAgfVxuICAgICAgICB9KSxcbiAgICAgIH0sXG4gICAgfSlcblxuICAgIGNvbnN0IHJlcyA9IGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luYyh7IG1ldGhvZCwgYnVja2V0TmFtZSwgb2JqZWN0TmFtZSwgcXVlcnkgfSwgcGF5bG9hZClcbiAgICBjb25zdCBib2R5ID0gYXdhaXQgcmVhZEFzQnVmZmVyKHJlcylcbiAgICBjb25zdCByZXN1bHQgPSBwYXJzZUNvbXBsZXRlTXVsdGlwYXJ0KGJvZHkudG9TdHJpbmcoKSlcbiAgICBpZiAoIXJlc3VsdCkge1xuICAgICAgdGhyb3cgbmV3IEVycm9yKCdCVUc6IGZhaWxlZCB0byBwYXJzZSBzZXJ2ZXIgcmVzcG9uc2UnKVxuICAgIH1cblxuICAgIGlmIChyZXN1bHQuZXJyQ29kZSkge1xuICAgICAgLy8gTXVsdGlwYXJ0IENvbXBsZXRlIEFQSSByZXR1cm5zIGFuIGVycm9yIFhNTCBhZnRlciBhIDIwMCBodHRwIHN0YXR1c1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5TM0Vycm9yKHJlc3VsdC5lcnJNZXNzYWdlKVxuICAgIH1cblxuICAgIHJldHVybiB7XG4gICAgICAvLyBlc2xpbnQtZGlzYWJsZS1uZXh0LWxpbmUgQHR5cGVzY3JpcHQtZXNsaW50L2Jhbi10cy1jb21tZW50XG4gICAgICAvLyBAdHMtaWdub3JlXG4gICAgICBldGFnOiByZXN1bHQuZXRhZyBhcyBzdHJpbmcsXG4gICAgICB2ZXJzaW9uSWQ6IGdldFZlcnNpb25JZChyZXMuaGVhZGVycyBhcyBSZXNwb25zZUhlYWRlciksXG4gICAgfVxuICB9XG5cbiAgLyoqXG4gICAqIEdldCBwYXJ0LWluZm8gb2YgYWxsIHBhcnRzIG9mIGFuIGluY29tcGxldGUgdXBsb2FkIHNwZWNpZmllZCBieSB1cGxvYWRJZC5cbiAgICovXG4gIHByb3RlY3RlZCBhc3luYyBsaXN0UGFydHMoYnVja2V0TmFtZTogc3RyaW5nLCBvYmplY3ROYW1lOiBzdHJpbmcsIHVwbG9hZElkOiBzdHJpbmcpOiBQcm9taXNlPFVwbG9hZGVkUGFydFtdPiB7XG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZShidWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKCdJbnZhbGlkIGJ1Y2tldCBuYW1lOiAnICsgYnVja2V0TmFtZSlcbiAgICB9XG4gICAgaWYgKCFpc1ZhbGlkT2JqZWN0TmFtZShvYmplY3ROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkT2JqZWN0TmFtZUVycm9yKGBJbnZhbGlkIG9iamVjdCBuYW1lOiAke29iamVjdE5hbWV9YClcbiAgICB9XG4gICAgaWYgKCFpc1N0cmluZyh1cGxvYWRJZCkpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ3VwbG9hZElkIHNob3VsZCBiZSBvZiB0eXBlIFwic3RyaW5nXCInKVxuICAgIH1cbiAgICBpZiAoIXVwbG9hZElkKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKCd1cGxvYWRJZCBjYW5ub3QgYmUgZW1wdHknKVxuICAgIH1cblxuICAgIGNvbnN0IHBhcnRzOiBVcGxvYWRlZFBhcnRbXSA9IFtdXG4gICAgbGV0IG1hcmtlciA9IDBcbiAgICBsZXQgcmVzdWx0XG4gICAgZG8ge1xuICAgICAgcmVzdWx0ID0gYXdhaXQgdGhpcy5saXN0UGFydHNRdWVyeShidWNrZXROYW1lLCBvYmplY3ROYW1lLCB1cGxvYWRJZCwgbWFya2VyKVxuICAgICAgbWFya2VyID0gcmVzdWx0Lm1hcmtlclxuICAgICAgcGFydHMucHVzaCguLi5yZXN1bHQucGFydHMpXG4gICAgfSB3aGlsZSAocmVzdWx0LmlzVHJ1bmNhdGVkKVxuXG4gICAgcmV0dXJuIHBhcnRzXG4gIH1cblxuICAvKipcbiAgICogQ2FsbGVkIGJ5IGxpc3RQYXJ0cyB0byBmZXRjaCBhIGJhdGNoIG9mIHBhcnQtaW5mb1xuICAgKi9cbiAgcHJpdmF0ZSBhc3luYyBsaXN0UGFydHNRdWVyeShidWNrZXROYW1lOiBzdHJpbmcsIG9iamVjdE5hbWU6IHN0cmluZywgdXBsb2FkSWQ6IHN0cmluZywgbWFya2VyOiBudW1iZXIpIHtcbiAgICBpZiAoIWlzVmFsaWRCdWNrZXROYW1lKGJ1Y2tldE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IoJ0ludmFsaWQgYnVja2V0IG5hbWU6ICcgKyBidWNrZXROYW1lKVxuICAgIH1cbiAgICBpZiAoIWlzVmFsaWRPYmplY3ROYW1lKG9iamVjdE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRPYmplY3ROYW1lRXJyb3IoYEludmFsaWQgb2JqZWN0IG5hbWU6ICR7b2JqZWN0TmFtZX1gKVxuICAgIH1cbiAgICBpZiAoIWlzU3RyaW5nKHVwbG9hZElkKSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcigndXBsb2FkSWQgc2hvdWxkIGJlIG9mIHR5cGUgXCJzdHJpbmdcIicpXG4gICAgfVxuICAgIGlmICghaXNOdW1iZXIobWFya2VyKSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcignbWFya2VyIHNob3VsZCBiZSBvZiB0eXBlIFwibnVtYmVyXCInKVxuICAgIH1cbiAgICBpZiAoIXVwbG9hZElkKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKCd1cGxvYWRJZCBjYW5ub3QgYmUgZW1wdHknKVxuICAgIH1cblxuICAgIGxldCBxdWVyeSA9IGB1cGxvYWRJZD0ke3VyaUVzY2FwZSh1cGxvYWRJZCl9YFxuICAgIGlmIChtYXJrZXIpIHtcbiAgICAgIHF1ZXJ5ICs9IGAmcGFydC1udW1iZXItbWFya2VyPSR7bWFya2VyfWBcbiAgICB9XG5cbiAgICBjb25zdCBtZXRob2QgPSAnR0VUJ1xuICAgIGNvbnN0IHJlcyA9IGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luYyh7IG1ldGhvZCwgYnVja2V0TmFtZSwgb2JqZWN0TmFtZSwgcXVlcnkgfSlcbiAgICByZXR1cm4geG1sUGFyc2Vycy5wYXJzZUxpc3RQYXJ0cyhhd2FpdCByZWFkQXNTdHJpbmcocmVzKSlcbiAgfVxuXG4gIGFzeW5jIGxpc3RCdWNrZXRzKCk6IFByb21pc2U8QnVja2V0SXRlbUZyb21MaXN0W10+IHtcbiAgICBjb25zdCBtZXRob2QgPSAnR0VUJ1xuICAgIGNvbnN0IHJlZ2lvbkNvbmYgPSB0aGlzLnJlZ2lvbiB8fCBERUZBVUxUX1JFR0lPTlxuICAgIGNvbnN0IGh0dHBSZXMgPSBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0QXN5bmMoeyBtZXRob2QgfSwgJycsIFsyMDBdLCByZWdpb25Db25mKVxuICAgIGNvbnN0IHhtbFJlc3VsdCA9IGF3YWl0IHJlYWRBc1N0cmluZyhodHRwUmVzKVxuICAgIHJldHVybiB4bWxQYXJzZXJzLnBhcnNlTGlzdEJ1Y2tldCh4bWxSZXN1bHQpXG4gIH1cblxuICAvKipcbiAgICogQ2FsY3VsYXRlIHBhcnQgc2l6ZSBnaXZlbiB0aGUgb2JqZWN0IHNpemUuIFBhcnQgc2l6ZSB3aWxsIGJlIGF0bGVhc3QgdGhpcy5wYXJ0U2l6ZVxuICAgKi9cbiAgY2FsY3VsYXRlUGFydFNpemUoc2l6ZTogbnVtYmVyKSB7XG4gICAgaWYgKCFpc051bWJlcihzaXplKSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcignc2l6ZSBzaG91bGQgYmUgb2YgdHlwZSBcIm51bWJlclwiJylcbiAgICB9XG4gICAgaWYgKHNpemUgPiB0aGlzLm1heE9iamVjdFNpemUpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoYHNpemUgc2hvdWxkIG5vdCBiZSBtb3JlIHRoYW4gJHt0aGlzLm1heE9iamVjdFNpemV9YClcbiAgICB9XG4gICAgaWYgKHRoaXMub3ZlclJpZGVQYXJ0U2l6ZSkge1xuICAgICAgcmV0dXJuIHRoaXMucGFydFNpemVcbiAgICB9XG4gICAgbGV0IHBhcnRTaXplID0gdGhpcy5wYXJ0U2l6ZVxuICAgIGZvciAoOzspIHtcbiAgICAgIC8vIHdoaWxlKHRydWUpIHsuLi59IHRocm93cyBsaW50aW5nIGVycm9yLlxuICAgICAgLy8gSWYgcGFydFNpemUgaXMgYmlnIGVub3VnaCB0byBhY2NvbW9kYXRlIHRoZSBvYmplY3Qgc2l6ZSwgdGhlbiB1c2UgaXQuXG4gICAgICBpZiAocGFydFNpemUgKiAxMDAwMCA+IHNpemUpIHtcbiAgICAgICAgcmV0dXJuIHBhcnRTaXplXG4gICAgICB9XG4gICAgICAvLyBUcnkgcGFydCBzaXplcyBhcyA2NE1CLCA4ME1CLCA5Nk1CIGV0Yy5cbiAgICAgIHBhcnRTaXplICs9IDE2ICogMTAyNCAqIDEwMjRcbiAgICB9XG4gIH1cblxuICAvKipcbiAgICogVXBsb2FkcyB0aGUgb2JqZWN0IHVzaW5nIGNvbnRlbnRzIGZyb20gYSBmaWxlXG4gICAqL1xuICBhc3luYyBmUHV0T2JqZWN0KGJ1Y2tldE5hbWU6IHN0cmluZywgb2JqZWN0TmFtZTogc3RyaW5nLCBmaWxlUGF0aDogc3RyaW5nLCBtZXRhRGF0YT86IE9iamVjdE1ldGFEYXRhKSB7XG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZShidWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKCdJbnZhbGlkIGJ1Y2tldCBuYW1lOiAnICsgYnVja2V0TmFtZSlcbiAgICB9XG4gICAgaWYgKCFpc1ZhbGlkT2JqZWN0TmFtZShvYmplY3ROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkT2JqZWN0TmFtZUVycm9yKGBJbnZhbGlkIG9iamVjdCBuYW1lOiAke29iamVjdE5hbWV9YClcbiAgICB9XG5cbiAgICBpZiAoIWlzU3RyaW5nKGZpbGVQYXRoKSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcignZmlsZVBhdGggc2hvdWxkIGJlIG9mIHR5cGUgXCJzdHJpbmdcIicpXG4gICAgfVxuICAgIGlmIChtZXRhRGF0YSAmJiAhaXNPYmplY3QobWV0YURhdGEpKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdtZXRhRGF0YSBzaG91bGQgYmUgb2YgdHlwZSBcIm9iamVjdFwiJylcbiAgICB9XG5cbiAgICAvLyBJbnNlcnRzIGNvcnJlY3QgYGNvbnRlbnQtdHlwZWAgYXR0cmlidXRlIGJhc2VkIG9uIG1ldGFEYXRhIGFuZCBmaWxlUGF0aFxuICAgIG1ldGFEYXRhID0gaW5zZXJ0Q29udGVudFR5cGUobWV0YURhdGEgfHwge30sIGZpbGVQYXRoKVxuICAgIGNvbnN0IHN0YXQgPSBhd2FpdCBmc3Auc3RhdChmaWxlUGF0aClcbiAgICByZXR1cm4gYXdhaXQgdGhpcy5wdXRPYmplY3QoYnVja2V0TmFtZSwgb2JqZWN0TmFtZSwgZnMuY3JlYXRlUmVhZFN0cmVhbShmaWxlUGF0aCksIHN0YXQuc2l6ZSwgbWV0YURhdGEpXG4gIH1cblxuICAvKipcbiAgICogIFVwbG9hZGluZyBhIHN0cmVhbSwgXCJCdWZmZXJcIiBvciBcInN0cmluZ1wiLlxuICAgKiAgSXQncyByZWNvbW1lbmRlZCB0byBwYXNzIGBzaXplYCBhcmd1bWVudCB3aXRoIHN0cmVhbS5cbiAgICovXG4gIGFzeW5jIHB1dE9iamVjdChcbiAgICBidWNrZXROYW1lOiBzdHJpbmcsXG4gICAgb2JqZWN0TmFtZTogc3RyaW5nLFxuICAgIHN0cmVhbTogc3RyZWFtLlJlYWRhYmxlIHwgQnVmZmVyIHwgc3RyaW5nLFxuICAgIHNpemU/OiBudW1iZXIsXG4gICAgbWV0YURhdGE/OiBJdGVtQnVja2V0TWV0YWRhdGEsXG4gICk6IFByb21pc2U8VXBsb2FkZWRPYmplY3RJbmZvPiB7XG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZShidWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKGBJbnZhbGlkIGJ1Y2tldCBuYW1lOiAke2J1Y2tldE5hbWV9YClcbiAgICB9XG4gICAgaWYgKCFpc1ZhbGlkT2JqZWN0TmFtZShvYmplY3ROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkT2JqZWN0TmFtZUVycm9yKGBJbnZhbGlkIG9iamVjdCBuYW1lOiAke29iamVjdE5hbWV9YClcbiAgICB9XG5cbiAgICAvLyBXZSdsbCBuZWVkIHRvIHNoaWZ0IGFyZ3VtZW50cyB0byB0aGUgbGVmdCBiZWNhdXNlIG9mIG1ldGFEYXRhXG4gICAgLy8gYW5kIHNpemUgYmVpbmcgb3B0aW9uYWwuXG4gICAgaWYgKGlzT2JqZWN0KHNpemUpKSB7XG4gICAgICBtZXRhRGF0YSA9IHNpemVcbiAgICB9XG4gICAgLy8gRW5zdXJlcyBNZXRhZGF0YSBoYXMgYXBwcm9wcmlhdGUgcHJlZml4IGZvciBBMyBBUElcbiAgICBjb25zdCBoZWFkZXJzID0gcHJlcGVuZFhBTVpNZXRhKG1ldGFEYXRhKVxuICAgIGlmICh0eXBlb2Ygc3RyZWFtID09PSAnc3RyaW5nJyB8fCBzdHJlYW0gaW5zdGFuY2VvZiBCdWZmZXIpIHtcbiAgICAgIC8vIEFkYXB0cyB0aGUgbm9uLXN0cmVhbSBpbnRlcmZhY2UgaW50byBhIHN0cmVhbS5cbiAgICAgIHNpemUgPSBzdHJlYW0ubGVuZ3RoXG4gICAgICBzdHJlYW0gPSByZWFkYWJsZVN0cmVhbShzdHJlYW0pXG4gICAgfSBlbHNlIGlmICghaXNSZWFkYWJsZVN0cmVhbShzdHJlYW0pKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCd0aGlyZCBhcmd1bWVudCBzaG91bGQgYmUgb2YgdHlwZSBcInN0cmVhbS5SZWFkYWJsZVwiIG9yIFwiQnVmZmVyXCIgb3IgXCJzdHJpbmdcIicpXG4gICAgfVxuXG4gICAgaWYgKGlzTnVtYmVyKHNpemUpICYmIHNpemUgPCAwKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKGBzaXplIGNhbm5vdCBiZSBuZWdhdGl2ZSwgZ2l2ZW4gc2l6ZTogJHtzaXplfWApXG4gICAgfVxuXG4gICAgLy8gR2V0IHRoZSBwYXJ0IHNpemUgYW5kIGZvcndhcmQgdGhhdCB0byB0aGUgQmxvY2tTdHJlYW0uIERlZmF1bHQgdG8gdGhlXG4gICAgLy8gbGFyZ2VzdCBibG9jayBzaXplIHBvc3NpYmxlIGlmIG5lY2Vzc2FyeS5cbiAgICBpZiAoIWlzTnVtYmVyKHNpemUpKSB7XG4gICAgICBzaXplID0gdGhpcy5tYXhPYmplY3RTaXplXG4gICAgfVxuXG4gICAgLy8gR2V0IHRoZSBwYXJ0IHNpemUgYW5kIGZvcndhcmQgdGhhdCB0byB0aGUgQmxvY2tTdHJlYW0uIERlZmF1bHQgdG8gdGhlXG4gICAgLy8gbGFyZ2VzdCBibG9jayBzaXplIHBvc3NpYmxlIGlmIG5lY2Vzc2FyeS5cbiAgICBpZiAoc2l6ZSA9PT0gdW5kZWZpbmVkKSB7XG4gICAgICBjb25zdCBzdGF0U2l6ZSA9IGF3YWl0IGdldENvbnRlbnRMZW5ndGgoc3RyZWFtKVxuICAgICAgaWYgKHN0YXRTaXplICE9PSBudWxsKSB7XG4gICAgICAgIHNpemUgPSBzdGF0U2l6ZVxuICAgICAgfVxuICAgIH1cblxuICAgIGlmICghaXNOdW1iZXIoc2l6ZSkpIHtcbiAgICAgIC8vIEJhY2t3YXJkIGNvbXBhdGliaWxpdHlcbiAgICAgIHNpemUgPSB0aGlzLm1heE9iamVjdFNpemVcbiAgICB9XG4gICAgaWYgKHNpemUgPT09IDApIHtcbiAgICAgIHJldHVybiB0aGlzLnVwbG9hZEJ1ZmZlcihidWNrZXROYW1lLCBvYmplY3ROYW1lLCBoZWFkZXJzLCBCdWZmZXIuZnJvbSgnJykpXG4gICAgfVxuXG4gICAgY29uc3QgcGFydFNpemUgPSB0aGlzLmNhbGN1bGF0ZVBhcnRTaXplKHNpemUpXG4gICAgaWYgKHR5cGVvZiBzdHJlYW0gPT09ICdzdHJpbmcnIHx8IEJ1ZmZlci5pc0J1ZmZlcihzdHJlYW0pIHx8IHNpemUgPD0gcGFydFNpemUpIHtcbiAgICAgIGNvbnN0IGJ1ZiA9IGlzUmVhZGFibGVTdHJlYW0oc3RyZWFtKSA/IGF3YWl0IHJlYWRBc0J1ZmZlcihzdHJlYW0pIDogQnVmZmVyLmZyb20oc3RyZWFtKVxuICAgICAgcmV0dXJuIHRoaXMudXBsb2FkQnVmZmVyKGJ1Y2tldE5hbWUsIG9iamVjdE5hbWUsIGhlYWRlcnMsIGJ1ZilcbiAgICB9XG5cbiAgICByZXR1cm4gdGhpcy51cGxvYWRTdHJlYW0oYnVja2V0TmFtZSwgb2JqZWN0TmFtZSwgaGVhZGVycywgc3RyZWFtLCBwYXJ0U2l6ZSlcbiAgfVxuXG4gIC8qKlxuICAgKiBtZXRob2QgdG8gdXBsb2FkIGJ1ZmZlciBpbiBvbmUgY2FsbFxuICAgKiBAcHJpdmF0ZVxuICAgKi9cbiAgcHJpdmF0ZSBhc3luYyB1cGxvYWRCdWZmZXIoXG4gICAgYnVja2V0TmFtZTogc3RyaW5nLFxuICAgIG9iamVjdE5hbWU6IHN0cmluZyxcbiAgICBoZWFkZXJzOiBSZXF1ZXN0SGVhZGVycyxcbiAgICBidWY6IEJ1ZmZlcixcbiAgKTogUHJvbWlzZTxVcGxvYWRlZE9iamVjdEluZm8+IHtcbiAgICBjb25zdCB7IG1kNXN1bSwgc2hhMjU2c3VtIH0gPSBoYXNoQmluYXJ5KGJ1ZiwgdGhpcy5lbmFibGVTSEEyNTYpXG4gICAgaGVhZGVyc1snQ29udGVudC1MZW5ndGgnXSA9IGJ1Zi5sZW5ndGhcbiAgICBpZiAoIXRoaXMuZW5hYmxlU0hBMjU2KSB7XG4gICAgICBoZWFkZXJzWydDb250ZW50LU1ENSddID0gbWQ1c3VtXG4gICAgfVxuICAgIGNvbnN0IHJlcyA9IGF3YWl0IHRoaXMubWFrZVJlcXVlc3RTdHJlYW1Bc3luYyhcbiAgICAgIHtcbiAgICAgICAgbWV0aG9kOiAnUFVUJyxcbiAgICAgICAgYnVja2V0TmFtZSxcbiAgICAgICAgb2JqZWN0TmFtZSxcbiAgICAgICAgaGVhZGVycyxcbiAgICAgIH0sXG4gICAgICBidWYsXG4gICAgICBzaGEyNTZzdW0sXG4gICAgICBbMjAwXSxcbiAgICAgICcnLFxuICAgIClcbiAgICBhd2FpdCBkcmFpblJlc3BvbnNlKHJlcylcbiAgICByZXR1cm4ge1xuICAgICAgZXRhZzogc2FuaXRpemVFVGFnKHJlcy5oZWFkZXJzLmV0YWcpLFxuICAgICAgdmVyc2lvbklkOiBnZXRWZXJzaW9uSWQocmVzLmhlYWRlcnMgYXMgUmVzcG9uc2VIZWFkZXIpLFxuICAgIH1cbiAgfVxuXG4gIC8qKlxuICAgKiB1cGxvYWQgc3RyZWFtIHdpdGggTXVsdGlwYXJ0VXBsb2FkXG4gICAqIEBwcml2YXRlXG4gICAqL1xuICBwcml2YXRlIGFzeW5jIHVwbG9hZFN0cmVhbShcbiAgICBidWNrZXROYW1lOiBzdHJpbmcsXG4gICAgb2JqZWN0TmFtZTogc3RyaW5nLFxuICAgIGhlYWRlcnM6IFJlcXVlc3RIZWFkZXJzLFxuICAgIGJvZHk6IHN0cmVhbS5SZWFkYWJsZSxcbiAgICBwYXJ0U2l6ZTogbnVtYmVyLFxuICApOiBQcm9taXNlPFVwbG9hZGVkT2JqZWN0SW5mbz4ge1xuICAgIC8vIEEgbWFwIG9mIHRoZSBwcmV2aW91c2x5IHVwbG9hZGVkIGNodW5rcywgZm9yIHJlc3VtaW5nIGEgZmlsZSB1cGxvYWQuIFRoaXNcbiAgICAvLyB3aWxsIGJlIG51bGwgaWYgd2UgYXJlbid0IHJlc3VtaW5nIGFuIHVwbG9hZC5cbiAgICBjb25zdCBvbGRQYXJ0czogUmVjb3JkPG51bWJlciwgUGFydD4gPSB7fVxuXG4gICAgLy8gS2VlcCB0cmFjayBvZiB0aGUgZXRhZ3MgZm9yIGFnZ3JlZ2F0aW5nIHRoZSBjaHVua3MgdG9nZXRoZXIgbGF0ZXIuIEVhY2hcbiAgICAvLyBldGFnIHJlcHJlc2VudHMgYSBzaW5nbGUgY2h1bmsgb2YgdGhlIGZpbGUuXG4gICAgY29uc3QgZVRhZ3M6IFBhcnRbXSA9IFtdXG5cbiAgICBjb25zdCBwcmV2aW91c1VwbG9hZElkID0gYXdhaXQgdGhpcy5maW5kVXBsb2FkSWQoYnVja2V0TmFtZSwgb2JqZWN0TmFtZSlcbiAgICBsZXQgdXBsb2FkSWQ6IHN0cmluZ1xuICAgIGlmICghcHJldmlvdXNVcGxvYWRJZCkge1xuICAgICAgdXBsb2FkSWQgPSBhd2FpdCB0aGlzLmluaXRpYXRlTmV3TXVsdGlwYXJ0VXBsb2FkKGJ1Y2tldE5hbWUsIG9iamVjdE5hbWUsIGhlYWRlcnMpXG4gICAgfSBlbHNlIHtcbiAgICAgIHVwbG9hZElkID0gcHJldmlvdXNVcGxvYWRJZFxuICAgICAgY29uc3Qgb2xkVGFncyA9IGF3YWl0IHRoaXMubGlzdFBhcnRzKGJ1Y2tldE5hbWUsIG9iamVjdE5hbWUsIHByZXZpb3VzVXBsb2FkSWQpXG4gICAgICBvbGRUYWdzLmZvckVhY2goKGUpID0+IHtcbiAgICAgICAgb2xkUGFydHNbZS5wYXJ0XSA9IGVcbiAgICAgIH0pXG4gICAgfVxuXG4gICAgaWYgKCFpc1JlYWRhYmxlU3RyZWFtKGJvZHkpKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCd0aGlyZCBhcmd1bWVudCBzaG91bGQgYmUgb2YgdHlwZSBcInN0cmVhbS5SZWFkYWJsZVwiIG9yIFwiQnVmZmVyXCIgb3IgXCJzdHJpbmdcIicpXG4gICAgfVxuXG4gICAgY29uc3QgY2h1bmtpZXIgPSBuZXcgQmxvY2tTdHJlYW0yKHsgc2l6ZTogcGFydFNpemUsIHplcm9QYWRkaW5nOiBmYWxzZSB9KVxuXG4gICAgLy8gZXNsaW50LWRpc2FibGUtbmV4dC1saW5lIEB0eXBlc2NyaXB0LWVzbGludC9uby11bnVzZWQtdmFyc1xuICAgIGNvbnN0IFtfLCBvXSA9IGF3YWl0IFByb21pc2UuYWxsKFtcbiAgICAgIG5ldyBQcm9taXNlKChyZXNvbHZlLCByZWplY3QpID0+IHtcbiAgICAgICAgYm9keS5waXBlKGNodW5raWVyKS5vbignZXJyb3InLCByZWplY3QpXG4gICAgICAgIGNodW5raWVyLm9uKCdlbmQnLCByZXNvbHZlKS5vbignZXJyb3InLCByZWplY3QpXG4gICAgICB9KSxcbiAgICAgIChhc3luYyAoKSA9PiB7XG4gICAgICAgIGxldCBwYXJ0TnVtYmVyID0gMFxuXG4gICAgICAgIGZvciBhd2FpdCAoY29uc3QgY2h1bmsgb2YgY2h1bmtpZXIpIHtcbiAgICAgICAgICBjb25zdCBtZDUgPSBjcnlwdG8uY3JlYXRlSGFzaCgnbWQ1JykudXBkYXRlKGNodW5rKS5kaWdlc3QoKVxuXG4gICAgICAgICAgcGFydE51bWJlcisrXG4gICAgICAgICAgY29uc3Qgb2xkUGFydCA9IG9sZFBhcnRzW3BhcnROdW1iZXJdXG4gICAgICAgICAgaWYgKG9sZFBhcnQpIHtcbiAgICAgICAgICAgIGlmIChvbGRQYXJ0LmV0YWcgPT09IG1kNS50b1N0cmluZygnaGV4JykpIHtcbiAgICAgICAgICAgICAgZVRhZ3MucHVzaCh7IHBhcnQ6IHBhcnROdW1iZXIsIGV0YWc6IG9sZFBhcnQuZXRhZyB9KVxuICAgICAgICAgICAgICBjb250aW51ZVxuICAgICAgICAgICAgfVxuICAgICAgICAgIH1cblxuICAgICAgICAgIC8vIG5vdyBzdGFydCB0byB1cGxvYWQgbWlzc2luZyBwYXJ0XG4gICAgICAgICAgY29uc3Qgb3B0aW9uczogUmVxdWVzdE9wdGlvbiA9IHtcbiAgICAgICAgICAgIG1ldGhvZDogJ1BVVCcsXG4gICAgICAgICAgICBxdWVyeTogcXMuc3RyaW5naWZ5KHsgcGFydE51bWJlciwgdXBsb2FkSWQgfSksXG4gICAgICAgICAgICBoZWFkZXJzOiB7XG4gICAgICAgICAgICAgICdDb250ZW50LUxlbmd0aCc6IGNodW5rLmxlbmd0aCxcbiAgICAgICAgICAgICAgJ0NvbnRlbnQtTUQ1JzogbWQ1LnRvU3RyaW5nKCdiYXNlNjQnKSxcbiAgICAgICAgICAgIH0sXG4gICAgICAgICAgICBidWNrZXROYW1lLFxuICAgICAgICAgICAgb2JqZWN0TmFtZSxcbiAgICAgICAgICB9XG5cbiAgICAgICAgICBjb25zdCByZXNwb25zZSA9IGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luY09taXQob3B0aW9ucywgY2h1bmspXG5cbiAgICAgICAgICBsZXQgZXRhZyA9IHJlc3BvbnNlLmhlYWRlcnMuZXRhZ1xuICAgICAgICAgIGlmIChldGFnKSB7XG4gICAgICAgICAgICBldGFnID0gZXRhZy5yZXBsYWNlKC9eXCIvLCAnJykucmVwbGFjZSgvXCIkLywgJycpXG4gICAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICAgIGV0YWcgPSAnJ1xuICAgICAgICAgIH1cblxuICAgICAgICAgIGVUYWdzLnB1c2goeyBwYXJ0OiBwYXJ0TnVtYmVyLCBldGFnIH0pXG5cbiAgICAgICAgICBwYXJ0TnVtYmVyKytcbiAgICAgICAgfVxuXG4gICAgICAgIGlmIChlVGFncy5sZW5ndGggPT09IDApIHtcbiAgICAgICAgICBhd2FpdCB0aGlzLmFib3J0TXVsdGlwYXJ0VXBsb2FkKGJ1Y2tldE5hbWUsIG9iamVjdE5hbWUsIHVwbG9hZElkKVxuICAgICAgICAgIHJldHVybiBhd2FpdCB0aGlzLnVwbG9hZEJ1ZmZlcihidWNrZXROYW1lLCBvYmplY3ROYW1lLCBoZWFkZXJzLCBCdWZmZXIuZnJvbSgnJykpXG4gICAgICAgIH1cblxuICAgICAgICByZXR1cm4gYXdhaXQgdGhpcy5jb21wbGV0ZU11bHRpcGFydFVwbG9hZChidWNrZXROYW1lLCBvYmplY3ROYW1lLCB1cGxvYWRJZCwgZVRhZ3MpXG4gICAgICB9KSgpLFxuICAgIF0pXG5cbiAgICByZXR1cm4gb1xuICB9XG5cbiAgYXN5bmMgcmVtb3ZlQnVja2V0UmVwbGljYXRpb24oYnVja2V0TmFtZTogc3RyaW5nKTogUHJvbWlzZTx2b2lkPlxuICByZW1vdmVCdWNrZXRSZXBsaWNhdGlvbihidWNrZXROYW1lOiBzdHJpbmcsIGNhbGxiYWNrOiBOb1Jlc3VsdENhbGxiYWNrKTogdm9pZFxuICBhc3luYyByZW1vdmVCdWNrZXRSZXBsaWNhdGlvbihidWNrZXROYW1lOiBzdHJpbmcpOiBQcm9taXNlPHZvaWQ+IHtcbiAgICBpZiAoIWlzVmFsaWRCdWNrZXROYW1lKGJ1Y2tldE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IoJ0ludmFsaWQgYnVja2V0IG5hbWU6ICcgKyBidWNrZXROYW1lKVxuICAgIH1cbiAgICBjb25zdCBtZXRob2QgPSAnREVMRVRFJ1xuICAgIGNvbnN0IHF1ZXJ5ID0gJ3JlcGxpY2F0aW9uJ1xuICAgIGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luY09taXQoeyBtZXRob2QsIGJ1Y2tldE5hbWUsIHF1ZXJ5IH0sICcnLCBbMjAwLCAyMDRdLCAnJylcbiAgfVxuXG4gIHNldEJ1Y2tldFJlcGxpY2F0aW9uKGJ1Y2tldE5hbWU6IHN0cmluZywgcmVwbGljYXRpb25Db25maWc6IFJlcGxpY2F0aW9uQ29uZmlnT3B0cyk6IHZvaWRcbiAgYXN5bmMgc2V0QnVja2V0UmVwbGljYXRpb24oYnVja2V0TmFtZTogc3RyaW5nLCByZXBsaWNhdGlvbkNvbmZpZzogUmVwbGljYXRpb25Db25maWdPcHRzKTogUHJvbWlzZTx2b2lkPlxuICBhc3luYyBzZXRCdWNrZXRSZXBsaWNhdGlvbihidWNrZXROYW1lOiBzdHJpbmcsIHJlcGxpY2F0aW9uQ29uZmlnOiBSZXBsaWNhdGlvbkNvbmZpZ09wdHMpIHtcbiAgICBpZiAoIWlzVmFsaWRCdWNrZXROYW1lKGJ1Y2tldE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IoJ0ludmFsaWQgYnVja2V0IG5hbWU6ICcgKyBidWNrZXROYW1lKVxuICAgIH1cbiAgICBpZiAoIWlzT2JqZWN0KHJlcGxpY2F0aW9uQ29uZmlnKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQXJndW1lbnRFcnJvcigncmVwbGljYXRpb25Db25maWcgc2hvdWxkIGJlIG9mIHR5cGUgXCJvYmplY3RcIicpXG4gICAgfSBlbHNlIHtcbiAgICAgIGlmIChfLmlzRW1wdHkocmVwbGljYXRpb25Db25maWcucm9sZSkpIHtcbiAgICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQXJndW1lbnRFcnJvcignUm9sZSBjYW5ub3QgYmUgZW1wdHknKVxuICAgICAgfSBlbHNlIGlmIChyZXBsaWNhdGlvbkNvbmZpZy5yb2xlICYmICFpc1N0cmluZyhyZXBsaWNhdGlvbkNvbmZpZy5yb2xlKSkge1xuICAgICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKCdJbnZhbGlkIHZhbHVlIGZvciByb2xlJywgcmVwbGljYXRpb25Db25maWcucm9sZSlcbiAgICAgIH1cbiAgICAgIGlmIChfLmlzRW1wdHkocmVwbGljYXRpb25Db25maWcucnVsZXMpKSB7XG4gICAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEFyZ3VtZW50RXJyb3IoJ01pbmltdW0gb25lIHJlcGxpY2F0aW9uIHJ1bGUgbXVzdCBiZSBzcGVjaWZpZWQnKVxuICAgICAgfVxuICAgIH1cbiAgICBjb25zdCBtZXRob2QgPSAnUFVUJ1xuICAgIGNvbnN0IHF1ZXJ5ID0gJ3JlcGxpY2F0aW9uJ1xuICAgIGNvbnN0IGhlYWRlcnM6IFJlY29yZDxzdHJpbmcsIHN0cmluZz4gPSB7fVxuXG4gICAgY29uc3QgcmVwbGljYXRpb25QYXJhbXNDb25maWcgPSB7XG4gICAgICBSZXBsaWNhdGlvbkNvbmZpZ3VyYXRpb246IHtcbiAgICAgICAgUm9sZTogcmVwbGljYXRpb25Db25maWcucm9sZSxcbiAgICAgICAgUnVsZTogcmVwbGljYXRpb25Db25maWcucnVsZXMsXG4gICAgICB9LFxuICAgIH1cblxuICAgIGNvbnN0IGJ1aWxkZXIgPSBuZXcgeG1sMmpzLkJ1aWxkZXIoeyByZW5kZXJPcHRzOiB7IHByZXR0eTogZmFsc2UgfSwgaGVhZGxlc3M6IHRydWUgfSlcbiAgICBjb25zdCBwYXlsb2FkID0gYnVpbGRlci5idWlsZE9iamVjdChyZXBsaWNhdGlvblBhcmFtc0NvbmZpZylcbiAgICBoZWFkZXJzWydDb250ZW50LU1ENSddID0gdG9NZDUocGF5bG9hZClcbiAgICBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0QXN5bmNPbWl0KHsgbWV0aG9kLCBidWNrZXROYW1lLCBxdWVyeSwgaGVhZGVycyB9LCBwYXlsb2FkKVxuICB9XG5cbiAgZ2V0QnVja2V0UmVwbGljYXRpb24oYnVja2V0TmFtZTogc3RyaW5nKTogdm9pZFxuICBhc3luYyBnZXRCdWNrZXRSZXBsaWNhdGlvbihidWNrZXROYW1lOiBzdHJpbmcpOiBQcm9taXNlPFJlcGxpY2F0aW9uQ29uZmlnPlxuICBhc3luYyBnZXRCdWNrZXRSZXBsaWNhdGlvbihidWNrZXROYW1lOiBzdHJpbmcpIHtcbiAgICBpZiAoIWlzVmFsaWRCdWNrZXROYW1lKGJ1Y2tldE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IoJ0ludmFsaWQgYnVja2V0IG5hbWU6ICcgKyBidWNrZXROYW1lKVxuICAgIH1cbiAgICBjb25zdCBtZXRob2QgPSAnR0VUJ1xuICAgIGNvbnN0IHF1ZXJ5ID0gJ3JlcGxpY2F0aW9uJ1xuXG4gICAgY29uc3QgaHR0cFJlcyA9IGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luYyh7IG1ldGhvZCwgYnVja2V0TmFtZSwgcXVlcnkgfSwgJycsIFsyMDAsIDIwNF0pXG4gICAgY29uc3QgeG1sUmVzdWx0ID0gYXdhaXQgcmVhZEFzU3RyaW5nKGh0dHBSZXMpXG4gICAgcmV0dXJuIHhtbFBhcnNlcnMucGFyc2VSZXBsaWNhdGlvbkNvbmZpZyh4bWxSZXN1bHQpXG4gIH1cblxuICBnZXRPYmplY3RMZWdhbEhvbGQoXG4gICAgYnVja2V0TmFtZTogc3RyaW5nLFxuICAgIG9iamVjdE5hbWU6IHN0cmluZyxcbiAgICBnZXRPcHRzPzogR2V0T2JqZWN0TGVnYWxIb2xkT3B0aW9ucyxcbiAgICBjYWxsYmFjaz86IFJlc3VsdENhbGxiYWNrPExFR0FMX0hPTERfU1RBVFVTPixcbiAgKTogUHJvbWlzZTxMRUdBTF9IT0xEX1NUQVRVUz5cbiAgYXN5bmMgZ2V0T2JqZWN0TGVnYWxIb2xkKFxuICAgIGJ1Y2tldE5hbWU6IHN0cmluZyxcbiAgICBvYmplY3ROYW1lOiBzdHJpbmcsXG4gICAgZ2V0T3B0cz86IEdldE9iamVjdExlZ2FsSG9sZE9wdGlvbnMsXG4gICk6IFByb21pc2U8TEVHQUxfSE9MRF9TVEFUVVM+IHtcbiAgICBpZiAoIWlzVmFsaWRCdWNrZXROYW1lKGJ1Y2tldE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IoJ0ludmFsaWQgYnVja2V0IG5hbWU6ICcgKyBidWNrZXROYW1lKVxuICAgIH1cbiAgICBpZiAoIWlzVmFsaWRPYmplY3ROYW1lKG9iamVjdE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRPYmplY3ROYW1lRXJyb3IoYEludmFsaWQgb2JqZWN0IG5hbWU6ICR7b2JqZWN0TmFtZX1gKVxuICAgIH1cblxuICAgIGlmIChnZXRPcHRzKSB7XG4gICAgICBpZiAoIWlzT2JqZWN0KGdldE9wdHMpKSB7XG4gICAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ2dldE9wdHMgc2hvdWxkIGJlIG9mIHR5cGUgXCJPYmplY3RcIicpXG4gICAgICB9IGVsc2UgaWYgKE9iamVjdC5rZXlzKGdldE9wdHMpLmxlbmd0aCA+IDAgJiYgZ2V0T3B0cy52ZXJzaW9uSWQgJiYgIWlzU3RyaW5nKGdldE9wdHMudmVyc2lvbklkKSkge1xuICAgICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCd2ZXJzaW9uSWQgc2hvdWxkIGJlIG9mIHR5cGUgc3RyaW5nLjonLCBnZXRPcHRzLnZlcnNpb25JZClcbiAgICAgIH1cbiAgICB9XG5cbiAgICBjb25zdCBtZXRob2QgPSAnR0VUJ1xuICAgIGxldCBxdWVyeSA9ICdsZWdhbC1ob2xkJ1xuXG4gICAgaWYgKGdldE9wdHM/LnZlcnNpb25JZCkge1xuICAgICAgcXVlcnkgKz0gYCZ2ZXJzaW9uSWQ9JHtnZXRPcHRzLnZlcnNpb25JZH1gXG4gICAgfVxuXG4gICAgY29uc3QgaHR0cFJlcyA9IGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luYyh7IG1ldGhvZCwgYnVja2V0TmFtZSwgb2JqZWN0TmFtZSwgcXVlcnkgfSwgJycsIFsyMDBdKVxuICAgIGNvbnN0IHN0clJlcyA9IGF3YWl0IHJlYWRBc1N0cmluZyhodHRwUmVzKVxuICAgIHJldHVybiBwYXJzZU9iamVjdExlZ2FsSG9sZENvbmZpZyhzdHJSZXMpXG4gIH1cblxuICBzZXRPYmplY3RMZWdhbEhvbGQoYnVja2V0TmFtZTogc3RyaW5nLCBvYmplY3ROYW1lOiBzdHJpbmcsIHNldE9wdHM/OiBQdXRPYmplY3RMZWdhbEhvbGRPcHRpb25zKTogdm9pZFxuICBhc3luYyBzZXRPYmplY3RMZWdhbEhvbGQoXG4gICAgYnVja2V0TmFtZTogc3RyaW5nLFxuICAgIG9iamVjdE5hbWU6IHN0cmluZyxcbiAgICBzZXRPcHRzID0ge1xuICAgICAgc3RhdHVzOiBMRUdBTF9IT0xEX1NUQVRVUy5FTkFCTEVELFxuICAgIH0gYXMgUHV0T2JqZWN0TGVnYWxIb2xkT3B0aW9ucyxcbiAgKTogUHJvbWlzZTx2b2lkPiB7XG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZShidWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKCdJbnZhbGlkIGJ1Y2tldCBuYW1lOiAnICsgYnVja2V0TmFtZSlcbiAgICB9XG4gICAgaWYgKCFpc1ZhbGlkT2JqZWN0TmFtZShvYmplY3ROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkT2JqZWN0TmFtZUVycm9yKGBJbnZhbGlkIG9iamVjdCBuYW1lOiAke29iamVjdE5hbWV9YClcbiAgICB9XG5cbiAgICBpZiAoIWlzT2JqZWN0KHNldE9wdHMpKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdzZXRPcHRzIHNob3VsZCBiZSBvZiB0eXBlIFwiT2JqZWN0XCInKVxuICAgIH0gZWxzZSB7XG4gICAgICBpZiAoIVtMRUdBTF9IT0xEX1NUQVRVUy5FTkFCTEVELCBMRUdBTF9IT0xEX1NUQVRVUy5ESVNBQkxFRF0uaW5jbHVkZXMoc2V0T3B0cz8uc3RhdHVzKSkge1xuICAgICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdJbnZhbGlkIHN0YXR1czogJyArIHNldE9wdHMuc3RhdHVzKVxuICAgICAgfVxuICAgICAgaWYgKHNldE9wdHMudmVyc2lvbklkICYmICFzZXRPcHRzLnZlcnNpb25JZC5sZW5ndGgpIHtcbiAgICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcigndmVyc2lvbklkIHNob3VsZCBiZSBvZiB0eXBlIHN0cmluZy46JyArIHNldE9wdHMudmVyc2lvbklkKVxuICAgICAgfVxuICAgIH1cblxuICAgIGNvbnN0IG1ldGhvZCA9ICdQVVQnXG4gICAgbGV0IHF1ZXJ5ID0gJ2xlZ2FsLWhvbGQnXG5cbiAgICBpZiAoc2V0T3B0cy52ZXJzaW9uSWQpIHtcbiAgICAgIHF1ZXJ5ICs9IGAmdmVyc2lvbklkPSR7c2V0T3B0cy52ZXJzaW9uSWR9YFxuICAgIH1cblxuICAgIGNvbnN0IGNvbmZpZyA9IHtcbiAgICAgIFN0YXR1czogc2V0T3B0cy5zdGF0dXMsXG4gICAgfVxuXG4gICAgY29uc3QgYnVpbGRlciA9IG5ldyB4bWwyanMuQnVpbGRlcih7IHJvb3ROYW1lOiAnTGVnYWxIb2xkJywgcmVuZGVyT3B0czogeyBwcmV0dHk6IGZhbHNlIH0sIGhlYWRsZXNzOiB0cnVlIH0pXG4gICAgY29uc3QgcGF5bG9hZCA9IGJ1aWxkZXIuYnVpbGRPYmplY3QoY29uZmlnKVxuICAgIGNvbnN0IGhlYWRlcnM6IFJlY29yZDxzdHJpbmcsIHN0cmluZz4gPSB7fVxuICAgIGhlYWRlcnNbJ0NvbnRlbnQtTUQ1J10gPSB0b01kNShwYXlsb2FkKVxuXG4gICAgYXdhaXQgdGhpcy5tYWtlUmVxdWVzdEFzeW5jT21pdCh7IG1ldGhvZCwgYnVja2V0TmFtZSwgb2JqZWN0TmFtZSwgcXVlcnksIGhlYWRlcnMgfSwgcGF5bG9hZClcbiAgfVxuXG4gIC8qKlxuICAgKiBHZXQgVGFncyBhc3NvY2lhdGVkIHdpdGggYSBCdWNrZXRcbiAgICovXG4gIGFzeW5jIGdldEJ1Y2tldFRhZ2dpbmcoYnVja2V0TmFtZTogc3RyaW5nKTogUHJvbWlzZTxUYWdbXT4ge1xuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcihgSW52YWxpZCBidWNrZXQgbmFtZTogJHtidWNrZXROYW1lfWApXG4gICAgfVxuXG4gICAgY29uc3QgbWV0aG9kID0gJ0dFVCdcbiAgICBjb25zdCBxdWVyeSA9ICd0YWdnaW5nJ1xuICAgIGNvbnN0IHJlcXVlc3RPcHRpb25zID0geyBtZXRob2QsIGJ1Y2tldE5hbWUsIHF1ZXJ5IH1cblxuICAgIGNvbnN0IHJlc3BvbnNlID0gYXdhaXQgdGhpcy5tYWtlUmVxdWVzdEFzeW5jKHJlcXVlc3RPcHRpb25zKVxuICAgIGNvbnN0IGJvZHkgPSBhd2FpdCByZWFkQXNTdHJpbmcocmVzcG9uc2UpXG4gICAgcmV0dXJuIHhtbFBhcnNlcnMucGFyc2VUYWdnaW5nKGJvZHkpXG4gIH1cblxuICAvKipcbiAgICogIEdldCB0aGUgdGFncyBhc3NvY2lhdGVkIHdpdGggYSBidWNrZXQgT1IgYW4gb2JqZWN0XG4gICAqL1xuICBhc3luYyBnZXRPYmplY3RUYWdnaW5nKGJ1Y2tldE5hbWU6IHN0cmluZywgb2JqZWN0TmFtZTogc3RyaW5nLCBnZXRPcHRzPzogR2V0T2JqZWN0T3B0cyk6IFByb21pc2U8VGFnW10+IHtcbiAgICBjb25zdCBtZXRob2QgPSAnR0VUJ1xuICAgIGxldCBxdWVyeSA9ICd0YWdnaW5nJ1xuXG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZShidWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKCdJbnZhbGlkIGJ1Y2tldCBuYW1lOiAnICsgYnVja2V0TmFtZSlcbiAgICB9XG4gICAgaWYgKCFpc1ZhbGlkT2JqZWN0TmFtZShvYmplY3ROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKCdJbnZhbGlkIG9iamVjdCBuYW1lOiAnICsgb2JqZWN0TmFtZSlcbiAgICB9XG4gICAgaWYgKGdldE9wdHMgJiYgIWlzT2JqZWN0KGdldE9wdHMpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKCdnZXRPcHRzIHNob3VsZCBiZSBvZiB0eXBlIFwib2JqZWN0XCInKVxuICAgIH1cblxuICAgIGlmIChnZXRPcHRzICYmIGdldE9wdHMudmVyc2lvbklkKSB7XG4gICAgICBxdWVyeSA9IGAke3F1ZXJ5fSZ2ZXJzaW9uSWQ9JHtnZXRPcHRzLnZlcnNpb25JZH1gXG4gICAgfVxuICAgIGNvbnN0IHJlcXVlc3RPcHRpb25zOiBSZXF1ZXN0T3B0aW9uID0geyBtZXRob2QsIGJ1Y2tldE5hbWUsIHF1ZXJ5IH1cbiAgICBpZiAob2JqZWN0TmFtZSkge1xuICAgICAgcmVxdWVzdE9wdGlvbnNbJ29iamVjdE5hbWUnXSA9IG9iamVjdE5hbWVcbiAgICB9XG5cbiAgICBjb25zdCByZXNwb25zZSA9IGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luYyhyZXF1ZXN0T3B0aW9ucylcbiAgICBjb25zdCBib2R5ID0gYXdhaXQgcmVhZEFzU3RyaW5nKHJlc3BvbnNlKVxuICAgIHJldHVybiB4bWxQYXJzZXJzLnBhcnNlVGFnZ2luZyhib2R5KVxuICB9XG5cbiAgLyoqXG4gICAqICBTZXQgdGhlIHBvbGljeSBvbiBhIGJ1Y2tldCBvciBhbiBvYmplY3QgcHJlZml4LlxuICAgKi9cbiAgYXN5bmMgc2V0QnVja2V0UG9saWN5KGJ1Y2tldE5hbWU6IHN0cmluZywgcG9saWN5OiBzdHJpbmcpOiBQcm9taXNlPHZvaWQ+IHtcbiAgICAvLyBWYWxpZGF0ZSBhcmd1bWVudHMuXG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZShidWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKGBJbnZhbGlkIGJ1Y2tldCBuYW1lOiAke2J1Y2tldE5hbWV9YClcbiAgICB9XG4gICAgaWYgKCFpc1N0cmluZyhwb2xpY3kpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRCdWNrZXRQb2xpY3lFcnJvcihgSW52YWxpZCBidWNrZXQgcG9saWN5OiAke3BvbGljeX0gLSBtdXN0IGJlIFwic3RyaW5nXCJgKVxuICAgIH1cblxuICAgIGNvbnN0IHF1ZXJ5ID0gJ3BvbGljeSdcblxuICAgIGxldCBtZXRob2QgPSAnREVMRVRFJ1xuICAgIGlmIChwb2xpY3kpIHtcbiAgICAgIG1ldGhvZCA9ICdQVVQnXG4gICAgfVxuXG4gICAgYXdhaXQgdGhpcy5tYWtlUmVxdWVzdEFzeW5jT21pdCh7IG1ldGhvZCwgYnVja2V0TmFtZSwgcXVlcnkgfSwgcG9saWN5LCBbMjA0XSwgJycpXG4gIH1cblxuICAvKipcbiAgICogR2V0IHRoZSBwb2xpY3kgb24gYSBidWNrZXQgb3IgYW4gb2JqZWN0IHByZWZpeC5cbiAgICovXG4gIGFzeW5jIGdldEJ1Y2tldFBvbGljeShidWNrZXROYW1lOiBzdHJpbmcpOiBQcm9taXNlPHN0cmluZz4ge1xuICAgIC8vIFZhbGlkYXRlIGFyZ3VtZW50cy5cbiAgICBpZiAoIWlzVmFsaWRCdWNrZXROYW1lKGJ1Y2tldE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IoYEludmFsaWQgYnVja2V0IG5hbWU6ICR7YnVja2V0TmFtZX1gKVxuICAgIH1cblxuICAgIGNvbnN0IG1ldGhvZCA9ICdHRVQnXG4gICAgY29uc3QgcXVlcnkgPSAncG9saWN5J1xuICAgIGNvbnN0IHJlcyA9IGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luYyh7IG1ldGhvZCwgYnVja2V0TmFtZSwgcXVlcnkgfSlcbiAgICByZXR1cm4gYXdhaXQgcmVhZEFzU3RyaW5nKHJlcylcbiAgfVxuXG4gIGFzeW5jIHB1dE9iamVjdFJldGVudGlvbihidWNrZXROYW1lOiBzdHJpbmcsIG9iamVjdE5hbWU6IHN0cmluZywgcmV0ZW50aW9uT3B0czogUmV0ZW50aW9uID0ge30pOiBQcm9taXNlPHZvaWQ+IHtcbiAgICBpZiAoIWlzVmFsaWRCdWNrZXROYW1lKGJ1Y2tldE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IoYEludmFsaWQgYnVja2V0IG5hbWU6ICR7YnVja2V0TmFtZX1gKVxuICAgIH1cbiAgICBpZiAoIWlzVmFsaWRPYmplY3ROYW1lKG9iamVjdE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRPYmplY3ROYW1lRXJyb3IoYEludmFsaWQgb2JqZWN0IG5hbWU6ICR7b2JqZWN0TmFtZX1gKVxuICAgIH1cbiAgICBpZiAoIWlzT2JqZWN0KHJldGVudGlvbk9wdHMpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKCdyZXRlbnRpb25PcHRzIHNob3VsZCBiZSBvZiB0eXBlIFwib2JqZWN0XCInKVxuICAgIH0gZWxzZSB7XG4gICAgICBpZiAocmV0ZW50aW9uT3B0cy5nb3Zlcm5hbmNlQnlwYXNzICYmICFpc0Jvb2xlYW4ocmV0ZW50aW9uT3B0cy5nb3Zlcm5hbmNlQnlwYXNzKSkge1xuICAgICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKGBJbnZhbGlkIHZhbHVlIGZvciBnb3Zlcm5hbmNlQnlwYXNzOiAke3JldGVudGlvbk9wdHMuZ292ZXJuYW5jZUJ5cGFzc31gKVxuICAgICAgfVxuICAgICAgaWYgKFxuICAgICAgICByZXRlbnRpb25PcHRzLm1vZGUgJiZcbiAgICAgICAgIVtSRVRFTlRJT05fTU9ERVMuQ09NUExJQU5DRSwgUkVURU5USU9OX01PREVTLkdPVkVSTkFOQ0VdLmluY2x1ZGVzKHJldGVudGlvbk9wdHMubW9kZSlcbiAgICAgICkge1xuICAgICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKGBJbnZhbGlkIG9iamVjdCByZXRlbnRpb24gbW9kZTogJHtyZXRlbnRpb25PcHRzLm1vZGV9YClcbiAgICAgIH1cbiAgICAgIGlmIChyZXRlbnRpb25PcHRzLnJldGFpblVudGlsRGF0ZSAmJiAhaXNTdHJpbmcocmV0ZW50aW9uT3B0cy5yZXRhaW5VbnRpbERhdGUpKSB7XG4gICAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEFyZ3VtZW50RXJyb3IoYEludmFsaWQgdmFsdWUgZm9yIHJldGFpblVudGlsRGF0ZTogJHtyZXRlbnRpb25PcHRzLnJldGFpblVudGlsRGF0ZX1gKVxuICAgICAgfVxuICAgICAgaWYgKHJldGVudGlvbk9wdHMudmVyc2lvbklkICYmICFpc1N0cmluZyhyZXRlbnRpb25PcHRzLnZlcnNpb25JZCkpIHtcbiAgICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQXJndW1lbnRFcnJvcihgSW52YWxpZCB2YWx1ZSBmb3IgdmVyc2lvbklkOiAke3JldGVudGlvbk9wdHMudmVyc2lvbklkfWApXG4gICAgICB9XG4gICAgfVxuXG4gICAgY29uc3QgbWV0aG9kID0gJ1BVVCdcbiAgICBsZXQgcXVlcnkgPSAncmV0ZW50aW9uJ1xuXG4gICAgY29uc3QgaGVhZGVyczogUmVxdWVzdEhlYWRlcnMgPSB7fVxuICAgIGlmIChyZXRlbnRpb25PcHRzLmdvdmVybmFuY2VCeXBhc3MpIHtcbiAgICAgIGhlYWRlcnNbJ1gtQW16LUJ5cGFzcy1Hb3Zlcm5hbmNlLVJldGVudGlvbiddID0gdHJ1ZVxuICAgIH1cblxuICAgIGNvbnN0IGJ1aWxkZXIgPSBuZXcgeG1sMmpzLkJ1aWxkZXIoeyByb290TmFtZTogJ1JldGVudGlvbicsIHJlbmRlck9wdHM6IHsgcHJldHR5OiBmYWxzZSB9LCBoZWFkbGVzczogdHJ1ZSB9KVxuICAgIGNvbnN0IHBhcmFtczogUmVjb3JkPHN0cmluZywgc3RyaW5nPiA9IHt9XG5cbiAgICBpZiAocmV0ZW50aW9uT3B0cy5tb2RlKSB7XG4gICAgICBwYXJhbXMuTW9kZSA9IHJldGVudGlvbk9wdHMubW9kZVxuICAgIH1cbiAgICBpZiAocmV0ZW50aW9uT3B0cy5yZXRhaW5VbnRpbERhdGUpIHtcbiAgICAgIHBhcmFtcy5SZXRhaW5VbnRpbERhdGUgPSByZXRlbnRpb25PcHRzLnJldGFpblVudGlsRGF0ZVxuICAgIH1cbiAgICBpZiAocmV0ZW50aW9uT3B0cy52ZXJzaW9uSWQpIHtcbiAgICAgIHF1ZXJ5ICs9IGAmdmVyc2lvbklkPSR7cmV0ZW50aW9uT3B0cy52ZXJzaW9uSWR9YFxuICAgIH1cblxuICAgIGNvbnN0IHBheWxvYWQgPSBidWlsZGVyLmJ1aWxkT2JqZWN0KHBhcmFtcylcblxuICAgIGhlYWRlcnNbJ0NvbnRlbnQtTUQ1J10gPSB0b01kNShwYXlsb2FkKVxuICAgIGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luY09taXQoeyBtZXRob2QsIGJ1Y2tldE5hbWUsIG9iamVjdE5hbWUsIHF1ZXJ5LCBoZWFkZXJzIH0sIHBheWxvYWQsIFsyMDAsIDIwNF0pXG4gIH1cblxuICBnZXRPYmplY3RMb2NrQ29uZmlnKGJ1Y2tldE5hbWU6IHN0cmluZywgY2FsbGJhY2s6IFJlc3VsdENhbGxiYWNrPE9iamVjdExvY2tJbmZvPik6IHZvaWRcbiAgZ2V0T2JqZWN0TG9ja0NvbmZpZyhidWNrZXROYW1lOiBzdHJpbmcpOiB2b2lkXG4gIGFzeW5jIGdldE9iamVjdExvY2tDb25maWcoYnVja2V0TmFtZTogc3RyaW5nKTogUHJvbWlzZTxPYmplY3RMb2NrSW5mbz5cbiAgYXN5bmMgZ2V0T2JqZWN0TG9ja0NvbmZpZyhidWNrZXROYW1lOiBzdHJpbmcpIHtcbiAgICBpZiAoIWlzVmFsaWRCdWNrZXROYW1lKGJ1Y2tldE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IoJ0ludmFsaWQgYnVja2V0IG5hbWU6ICcgKyBidWNrZXROYW1lKVxuICAgIH1cbiAgICBjb25zdCBtZXRob2QgPSAnR0VUJ1xuICAgIGNvbnN0IHF1ZXJ5ID0gJ29iamVjdC1sb2NrJ1xuXG4gICAgY29uc3QgaHR0cFJlcyA9IGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luYyh7IG1ldGhvZCwgYnVja2V0TmFtZSwgcXVlcnkgfSlcbiAgICBjb25zdCB4bWxSZXN1bHQgPSBhd2FpdCByZWFkQXNTdHJpbmcoaHR0cFJlcylcbiAgICByZXR1cm4geG1sUGFyc2Vycy5wYXJzZU9iamVjdExvY2tDb25maWcoeG1sUmVzdWx0KVxuICB9XG5cbiAgc2V0T2JqZWN0TG9ja0NvbmZpZyhidWNrZXROYW1lOiBzdHJpbmcsIGxvY2tDb25maWdPcHRzOiBPbWl0PE9iamVjdExvY2tJbmZvLCAnb2JqZWN0TG9ja0VuYWJsZWQnPik6IHZvaWRcbiAgYXN5bmMgc2V0T2JqZWN0TG9ja0NvbmZpZyhcbiAgICBidWNrZXROYW1lOiBzdHJpbmcsXG4gICAgbG9ja0NvbmZpZ09wdHM6IE9taXQ8T2JqZWN0TG9ja0luZm8sICdvYmplY3RMb2NrRW5hYmxlZCc+LFxuICApOiBQcm9taXNlPHZvaWQ+XG4gIGFzeW5jIHNldE9iamVjdExvY2tDb25maWcoYnVja2V0TmFtZTogc3RyaW5nLCBsb2NrQ29uZmlnT3B0czogT21pdDxPYmplY3RMb2NrSW5mbywgJ29iamVjdExvY2tFbmFibGVkJz4pIHtcbiAgICBjb25zdCByZXRlbnRpb25Nb2RlcyA9IFtSRVRFTlRJT05fTU9ERVMuQ09NUExJQU5DRSwgUkVURU5USU9OX01PREVTLkdPVkVSTkFOQ0VdXG4gICAgY29uc3QgdmFsaWRVbml0cyA9IFtSRVRFTlRJT05fVkFMSURJVFlfVU5JVFMuREFZUywgUkVURU5USU9OX1ZBTElESVRZX1VOSVRTLllFQVJTXVxuXG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZShidWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKCdJbnZhbGlkIGJ1Y2tldCBuYW1lOiAnICsgYnVja2V0TmFtZSlcbiAgICB9XG5cbiAgICBpZiAobG9ja0NvbmZpZ09wdHMubW9kZSAmJiAhcmV0ZW50aW9uTW9kZXMuaW5jbHVkZXMobG9ja0NvbmZpZ09wdHMubW9kZSkpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoYGxvY2tDb25maWdPcHRzLm1vZGUgc2hvdWxkIGJlIG9uZSBvZiAke3JldGVudGlvbk1vZGVzfWApXG4gICAgfVxuICAgIGlmIChsb2NrQ29uZmlnT3B0cy51bml0ICYmICF2YWxpZFVuaXRzLmluY2x1ZGVzKGxvY2tDb25maWdPcHRzLnVuaXQpKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKGBsb2NrQ29uZmlnT3B0cy51bml0IHNob3VsZCBiZSBvbmUgb2YgJHt2YWxpZFVuaXRzfWApXG4gICAgfVxuICAgIGlmIChsb2NrQ29uZmlnT3B0cy52YWxpZGl0eSAmJiAhaXNOdW1iZXIobG9ja0NvbmZpZ09wdHMudmFsaWRpdHkpKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKGBsb2NrQ29uZmlnT3B0cy52YWxpZGl0eSBzaG91bGQgYmUgYSBudW1iZXJgKVxuICAgIH1cblxuICAgIGNvbnN0IG1ldGhvZCA9ICdQVVQnXG4gICAgY29uc3QgcXVlcnkgPSAnb2JqZWN0LWxvY2snXG5cbiAgICBjb25zdCBjb25maWc6IE9iamVjdExvY2tDb25maWdQYXJhbSA9IHtcbiAgICAgIE9iamVjdExvY2tFbmFibGVkOiAnRW5hYmxlZCcsXG4gICAgfVxuICAgIGNvbnN0IGNvbmZpZ0tleXMgPSBPYmplY3Qua2V5cyhsb2NrQ29uZmlnT3B0cylcblxuICAgIGNvbnN0IGlzQWxsS2V5c1NldCA9IFsndW5pdCcsICdtb2RlJywgJ3ZhbGlkaXR5J10uZXZlcnkoKGxjaykgPT4gY29uZmlnS2V5cy5pbmNsdWRlcyhsY2spKVxuICAgIC8vIENoZWNrIGlmIGtleXMgYXJlIHByZXNlbnQgYW5kIGFsbCBrZXlzIGFyZSBwcmVzZW50LlxuICAgIGlmIChjb25maWdLZXlzLmxlbmd0aCA+IDApIHtcbiAgICAgIGlmICghaXNBbGxLZXlzU2V0KSB7XG4gICAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoXG4gICAgICAgICAgYGxvY2tDb25maWdPcHRzLm1vZGUsbG9ja0NvbmZpZ09wdHMudW5pdCxsb2NrQ29uZmlnT3B0cy52YWxpZGl0eSBhbGwgdGhlIHByb3BlcnRpZXMgc2hvdWxkIGJlIHNwZWNpZmllZC5gLFxuICAgICAgICApXG4gICAgICB9IGVsc2Uge1xuICAgICAgICBjb25maWcuUnVsZSA9IHtcbiAgICAgICAgICBEZWZhdWx0UmV0ZW50aW9uOiB7fSxcbiAgICAgICAgfVxuICAgICAgICBpZiAobG9ja0NvbmZpZ09wdHMubW9kZSkge1xuICAgICAgICAgIGNvbmZpZy5SdWxlLkRlZmF1bHRSZXRlbnRpb24uTW9kZSA9IGxvY2tDb25maWdPcHRzLm1vZGVcbiAgICAgICAgfVxuICAgICAgICBpZiAobG9ja0NvbmZpZ09wdHMudW5pdCA9PT0gUkVURU5USU9OX1ZBTElESVRZX1VOSVRTLkRBWVMpIHtcbiAgICAgICAgICBjb25maWcuUnVsZS5EZWZhdWx0UmV0ZW50aW9uLkRheXMgPSBsb2NrQ29uZmlnT3B0cy52YWxpZGl0eVxuICAgICAgICB9IGVsc2UgaWYgKGxvY2tDb25maWdPcHRzLnVuaXQgPT09IFJFVEVOVElPTl9WQUxJRElUWV9VTklUUy5ZRUFSUykge1xuICAgICAgICAgIGNvbmZpZy5SdWxlLkRlZmF1bHRSZXRlbnRpb24uWWVhcnMgPSBsb2NrQ29uZmlnT3B0cy52YWxpZGl0eVxuICAgICAgICB9XG4gICAgICB9XG4gICAgfVxuXG4gICAgY29uc3QgYnVpbGRlciA9IG5ldyB4bWwyanMuQnVpbGRlcih7XG4gICAgICByb290TmFtZTogJ09iamVjdExvY2tDb25maWd1cmF0aW9uJyxcbiAgICAgIHJlbmRlck9wdHM6IHsgcHJldHR5OiBmYWxzZSB9LFxuICAgICAgaGVhZGxlc3M6IHRydWUsXG4gICAgfSlcbiAgICBjb25zdCBwYXlsb2FkID0gYnVpbGRlci5idWlsZE9iamVjdChjb25maWcpXG5cbiAgICBjb25zdCBoZWFkZXJzOiBSZXF1ZXN0SGVhZGVycyA9IHt9XG4gICAgaGVhZGVyc1snQ29udGVudC1NRDUnXSA9IHRvTWQ1KHBheWxvYWQpXG5cbiAgICBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0QXN5bmNPbWl0KHsgbWV0aG9kLCBidWNrZXROYW1lLCBxdWVyeSwgaGVhZGVycyB9LCBwYXlsb2FkKVxuICB9XG5cbiAgYXN5bmMgZ2V0QnVja2V0VmVyc2lvbmluZyhidWNrZXROYW1lOiBzdHJpbmcpOiBQcm9taXNlPEJ1Y2tldFZlcnNpb25pbmdDb25maWd1cmF0aW9uPiB7XG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZShidWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKCdJbnZhbGlkIGJ1Y2tldCBuYW1lOiAnICsgYnVja2V0TmFtZSlcbiAgICB9XG4gICAgY29uc3QgbWV0aG9kID0gJ0dFVCdcbiAgICBjb25zdCBxdWVyeSA9ICd2ZXJzaW9uaW5nJ1xuXG4gICAgY29uc3QgaHR0cFJlcyA9IGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luYyh7IG1ldGhvZCwgYnVja2V0TmFtZSwgcXVlcnkgfSlcbiAgICBjb25zdCB4bWxSZXN1bHQgPSBhd2FpdCByZWFkQXNTdHJpbmcoaHR0cFJlcylcbiAgICByZXR1cm4gYXdhaXQgeG1sUGFyc2Vycy5wYXJzZUJ1Y2tldFZlcnNpb25pbmdDb25maWcoeG1sUmVzdWx0KVxuICB9XG5cbiAgYXN5bmMgc2V0QnVja2V0VmVyc2lvbmluZyhidWNrZXROYW1lOiBzdHJpbmcsIHZlcnNpb25Db25maWc6IEJ1Y2tldFZlcnNpb25pbmdDb25maWd1cmF0aW9uKTogUHJvbWlzZTx2b2lkPiB7XG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZShidWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKCdJbnZhbGlkIGJ1Y2tldCBuYW1lOiAnICsgYnVja2V0TmFtZSlcbiAgICB9XG4gICAgaWYgKCFPYmplY3Qua2V5cyh2ZXJzaW9uQ29uZmlnKS5sZW5ndGgpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEFyZ3VtZW50RXJyb3IoJ3ZlcnNpb25Db25maWcgc2hvdWxkIGJlIG9mIHR5cGUgXCJvYmplY3RcIicpXG4gICAgfVxuXG4gICAgY29uc3QgbWV0aG9kID0gJ1BVVCdcbiAgICBjb25zdCBxdWVyeSA9ICd2ZXJzaW9uaW5nJ1xuICAgIGNvbnN0IGJ1aWxkZXIgPSBuZXcgeG1sMmpzLkJ1aWxkZXIoe1xuICAgICAgcm9vdE5hbWU6ICdWZXJzaW9uaW5nQ29uZmlndXJhdGlvbicsXG4gICAgICByZW5kZXJPcHRzOiB7IHByZXR0eTogZmFsc2UgfSxcbiAgICAgIGhlYWRsZXNzOiB0cnVlLFxuICAgIH0pXG4gICAgY29uc3QgcGF5bG9hZCA9IGJ1aWxkZXIuYnVpbGRPYmplY3QodmVyc2lvbkNvbmZpZylcblxuICAgIGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luY09taXQoeyBtZXRob2QsIGJ1Y2tldE5hbWUsIHF1ZXJ5IH0sIHBheWxvYWQpXG4gIH1cblxuICBwcml2YXRlIGFzeW5jIHNldFRhZ2dpbmcodGFnZ2luZ1BhcmFtczogUHV0VGFnZ2luZ1BhcmFtcyk6IFByb21pc2U8dm9pZD4ge1xuICAgIGNvbnN0IHsgYnVja2V0TmFtZSwgb2JqZWN0TmFtZSwgdGFncywgcHV0T3B0cyB9ID0gdGFnZ2luZ1BhcmFtc1xuICAgIGNvbnN0IG1ldGhvZCA9ICdQVVQnXG4gICAgbGV0IHF1ZXJ5ID0gJ3RhZ2dpbmcnXG5cbiAgICBpZiAocHV0T3B0cyAmJiBwdXRPcHRzPy52ZXJzaW9uSWQpIHtcbiAgICAgIHF1ZXJ5ID0gYCR7cXVlcnl9JnZlcnNpb25JZD0ke3B1dE9wdHMudmVyc2lvbklkfWBcbiAgICB9XG4gICAgY29uc3QgdGFnc0xpc3QgPSBbXVxuICAgIGZvciAoY29uc3QgW2tleSwgdmFsdWVdIG9mIE9iamVjdC5lbnRyaWVzKHRhZ3MpKSB7XG4gICAgICB0YWdzTGlzdC5wdXNoKHsgS2V5OiBrZXksIFZhbHVlOiB2YWx1ZSB9KVxuICAgIH1cbiAgICBjb25zdCB0YWdnaW5nQ29uZmlnID0ge1xuICAgICAgVGFnZ2luZzoge1xuICAgICAgICBUYWdTZXQ6IHtcbiAgICAgICAgICBUYWc6IHRhZ3NMaXN0LFxuICAgICAgICB9LFxuICAgICAgfSxcbiAgICB9XG4gICAgY29uc3QgaGVhZGVycyA9IHt9IGFzIFJlcXVlc3RIZWFkZXJzXG4gICAgY29uc3QgYnVpbGRlciA9IG5ldyB4bWwyanMuQnVpbGRlcih7IGhlYWRsZXNzOiB0cnVlLCByZW5kZXJPcHRzOiB7IHByZXR0eTogZmFsc2UgfSB9KVxuICAgIGNvbnN0IHBheWxvYWRCdWYgPSBCdWZmZXIuZnJvbShidWlsZGVyLmJ1aWxkT2JqZWN0KHRhZ2dpbmdDb25maWcpKVxuICAgIGNvbnN0IHJlcXVlc3RPcHRpb25zID0ge1xuICAgICAgbWV0aG9kLFxuICAgICAgYnVja2V0TmFtZSxcbiAgICAgIHF1ZXJ5LFxuICAgICAgaGVhZGVycyxcblxuICAgICAgLi4uKG9iamVjdE5hbWUgJiYgeyBvYmplY3ROYW1lOiBvYmplY3ROYW1lIH0pLFxuICAgIH1cblxuICAgIGhlYWRlcnNbJ0NvbnRlbnQtTUQ1J10gPSB0b01kNShwYXlsb2FkQnVmKVxuXG4gICAgYXdhaXQgdGhpcy5tYWtlUmVxdWVzdEFzeW5jT21pdChyZXF1ZXN0T3B0aW9ucywgcGF5bG9hZEJ1ZilcbiAgfVxuXG4gIHByaXZhdGUgYXN5bmMgcmVtb3ZlVGFnZ2luZyh7IGJ1Y2tldE5hbWUsIG9iamVjdE5hbWUsIHJlbW92ZU9wdHMgfTogUmVtb3ZlVGFnZ2luZ1BhcmFtcyk6IFByb21pc2U8dm9pZD4ge1xuICAgIGNvbnN0IG1ldGhvZCA9ICdERUxFVEUnXG4gICAgbGV0IHF1ZXJ5ID0gJ3RhZ2dpbmcnXG5cbiAgICBpZiAocmVtb3ZlT3B0cyAmJiBPYmplY3Qua2V5cyhyZW1vdmVPcHRzKS5sZW5ndGggJiYgcmVtb3ZlT3B0cy52ZXJzaW9uSWQpIHtcbiAgICAgIHF1ZXJ5ID0gYCR7cXVlcnl9JnZlcnNpb25JZD0ke3JlbW92ZU9wdHMudmVyc2lvbklkfWBcbiAgICB9XG4gICAgY29uc3QgcmVxdWVzdE9wdGlvbnMgPSB7IG1ldGhvZCwgYnVja2V0TmFtZSwgb2JqZWN0TmFtZSwgcXVlcnkgfVxuXG4gICAgaWYgKG9iamVjdE5hbWUpIHtcbiAgICAgIHJlcXVlc3RPcHRpb25zWydvYmplY3ROYW1lJ10gPSBvYmplY3ROYW1lXG4gICAgfVxuICAgIGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luYyhyZXF1ZXN0T3B0aW9ucywgJycsIFsyMDAsIDIwNF0pXG4gIH1cblxuICBhc3luYyBzZXRCdWNrZXRUYWdnaW5nKGJ1Y2tldE5hbWU6IHN0cmluZywgdGFnczogVGFncyk6IFByb21pc2U8dm9pZD4ge1xuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcignSW52YWxpZCBidWNrZXQgbmFtZTogJyArIGJ1Y2tldE5hbWUpXG4gICAgfVxuICAgIGlmICghaXNQbGFpbk9iamVjdCh0YWdzKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQXJndW1lbnRFcnJvcigndGFncyBzaG91bGQgYmUgb2YgdHlwZSBcIm9iamVjdFwiJylcbiAgICB9XG4gICAgaWYgKE9iamVjdC5rZXlzKHRhZ3MpLmxlbmd0aCA+IDEwKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKCdtYXhpbXVtIHRhZ3MgYWxsb3dlZCBpcyAxMFwiJylcbiAgICB9XG5cbiAgICBhd2FpdCB0aGlzLnNldFRhZ2dpbmcoeyBidWNrZXROYW1lLCB0YWdzIH0pXG4gIH1cblxuICBhc3luYyByZW1vdmVCdWNrZXRUYWdnaW5nKGJ1Y2tldE5hbWU6IHN0cmluZykge1xuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcignSW52YWxpZCBidWNrZXQgbmFtZTogJyArIGJ1Y2tldE5hbWUpXG4gICAgfVxuICAgIGF3YWl0IHRoaXMucmVtb3ZlVGFnZ2luZyh7IGJ1Y2tldE5hbWUgfSlcbiAgfVxuXG4gIGFzeW5jIHNldE9iamVjdFRhZ2dpbmcoYnVja2V0TmFtZTogc3RyaW5nLCBvYmplY3ROYW1lOiBzdHJpbmcsIHRhZ3M6IFRhZ3MsIHB1dE9wdHM/OiBUYWdnaW5nT3B0cykge1xuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcignSW52YWxpZCBidWNrZXQgbmFtZTogJyArIGJ1Y2tldE5hbWUpXG4gICAgfVxuICAgIGlmICghaXNWYWxpZE9iamVjdE5hbWUob2JqZWN0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcignSW52YWxpZCBvYmplY3QgbmFtZTogJyArIG9iamVjdE5hbWUpXG4gICAgfVxuXG4gICAgaWYgKCFpc1BsYWluT2JqZWN0KHRhZ3MpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKCd0YWdzIHNob3VsZCBiZSBvZiB0eXBlIFwib2JqZWN0XCInKVxuICAgIH1cbiAgICBpZiAoT2JqZWN0LmtleXModGFncykubGVuZ3RoID4gMTApIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEFyZ3VtZW50RXJyb3IoJ01heGltdW0gdGFncyBhbGxvd2VkIGlzIDEwXCInKVxuICAgIH1cblxuICAgIGF3YWl0IHRoaXMuc2V0VGFnZ2luZyh7IGJ1Y2tldE5hbWUsIG9iamVjdE5hbWUsIHRhZ3MsIHB1dE9wdHMgfSlcbiAgfVxuXG4gIGFzeW5jIHJlbW92ZU9iamVjdFRhZ2dpbmcoYnVja2V0TmFtZTogc3RyaW5nLCBvYmplY3ROYW1lOiBzdHJpbmcsIHJlbW92ZU9wdHM6IFRhZ2dpbmdPcHRzKSB7XG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZShidWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKCdJbnZhbGlkIGJ1Y2tldCBuYW1lOiAnICsgYnVja2V0TmFtZSlcbiAgICB9XG4gICAgaWYgKCFpc1ZhbGlkT2JqZWN0TmFtZShvYmplY3ROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKCdJbnZhbGlkIG9iamVjdCBuYW1lOiAnICsgb2JqZWN0TmFtZSlcbiAgICB9XG4gICAgaWYgKHJlbW92ZU9wdHMgJiYgT2JqZWN0LmtleXMocmVtb3ZlT3B0cykubGVuZ3RoICYmICFpc09iamVjdChyZW1vdmVPcHRzKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQXJndW1lbnRFcnJvcigncmVtb3ZlT3B0cyBzaG91bGQgYmUgb2YgdHlwZSBcIm9iamVjdFwiJylcbiAgICB9XG5cbiAgICBhd2FpdCB0aGlzLnJlbW92ZVRhZ2dpbmcoeyBidWNrZXROYW1lLCBvYmplY3ROYW1lLCByZW1vdmVPcHRzIH0pXG4gIH1cblxuICBhc3luYyBzZWxlY3RPYmplY3RDb250ZW50KFxuICAgIGJ1Y2tldE5hbWU6IHN0cmluZyxcbiAgICBvYmplY3ROYW1lOiBzdHJpbmcsXG4gICAgc2VsZWN0T3B0czogU2VsZWN0T3B0aW9ucyxcbiAgKTogUHJvbWlzZTxTZWxlY3RSZXN1bHRzIHwgdW5kZWZpbmVkPiB7XG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZShidWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKGBJbnZhbGlkIGJ1Y2tldCBuYW1lOiAke2J1Y2tldE5hbWV9YClcbiAgICB9XG4gICAgaWYgKCFpc1ZhbGlkT2JqZWN0TmFtZShvYmplY3ROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkT2JqZWN0TmFtZUVycm9yKGBJbnZhbGlkIG9iamVjdCBuYW1lOiAke29iamVjdE5hbWV9YClcbiAgICB9XG4gICAgaWYgKCFfLmlzRW1wdHkoc2VsZWN0T3B0cykpIHtcbiAgICAgIGlmICghaXNTdHJpbmcoc2VsZWN0T3B0cy5leHByZXNzaW9uKSkge1xuICAgICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdzcWxFeHByZXNzaW9uIHNob3VsZCBiZSBvZiB0eXBlIFwic3RyaW5nXCInKVxuICAgICAgfVxuICAgICAgaWYgKCFfLmlzRW1wdHkoc2VsZWN0T3B0cy5pbnB1dFNlcmlhbGl6YXRpb24pKSB7XG4gICAgICAgIGlmICghaXNPYmplY3Qoc2VsZWN0T3B0cy5pbnB1dFNlcmlhbGl6YXRpb24pKSB7XG4gICAgICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcignaW5wdXRTZXJpYWxpemF0aW9uIHNob3VsZCBiZSBvZiB0eXBlIFwib2JqZWN0XCInKVxuICAgICAgICB9XG4gICAgICB9IGVsc2Uge1xuICAgICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdpbnB1dFNlcmlhbGl6YXRpb24gaXMgcmVxdWlyZWQnKVxuICAgICAgfVxuICAgICAgaWYgKCFfLmlzRW1wdHkoc2VsZWN0T3B0cy5vdXRwdXRTZXJpYWxpemF0aW9uKSkge1xuICAgICAgICBpZiAoIWlzT2JqZWN0KHNlbGVjdE9wdHMub3V0cHV0U2VyaWFsaXphdGlvbikpIHtcbiAgICAgICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdvdXRwdXRTZXJpYWxpemF0aW9uIHNob3VsZCBiZSBvZiB0eXBlIFwib2JqZWN0XCInKVxuICAgICAgICB9XG4gICAgICB9IGVsc2Uge1xuICAgICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdvdXRwdXRTZXJpYWxpemF0aW9uIGlzIHJlcXVpcmVkJylcbiAgICAgIH1cbiAgICB9IGVsc2Uge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcigndmFsaWQgc2VsZWN0IGNvbmZpZ3VyYXRpb24gaXMgcmVxdWlyZWQnKVxuICAgIH1cblxuICAgIGNvbnN0IG1ldGhvZCA9ICdQT1NUJ1xuICAgIGNvbnN0IHF1ZXJ5ID0gYHNlbGVjdCZzZWxlY3QtdHlwZT0yYFxuXG4gICAgY29uc3QgY29uZmlnOiBSZWNvcmQ8c3RyaW5nLCB1bmtub3duPltdID0gW1xuICAgICAge1xuICAgICAgICBFeHByZXNzaW9uOiBzZWxlY3RPcHRzLmV4cHJlc3Npb24sXG4gICAgICB9LFxuICAgICAge1xuICAgICAgICBFeHByZXNzaW9uVHlwZTogc2VsZWN0T3B0cy5leHByZXNzaW9uVHlwZSB8fCAnU1FMJyxcbiAgICAgIH0sXG4gICAgICB7XG4gICAgICAgIElucHV0U2VyaWFsaXphdGlvbjogW3NlbGVjdE9wdHMuaW5wdXRTZXJpYWxpemF0aW9uXSxcbiAgICAgIH0sXG4gICAgICB7XG4gICAgICAgIE91dHB1dFNlcmlhbGl6YXRpb246IFtzZWxlY3RPcHRzLm91dHB1dFNlcmlhbGl6YXRpb25dLFxuICAgICAgfSxcbiAgICBdXG5cbiAgICAvLyBPcHRpb25hbFxuICAgIGlmIChzZWxlY3RPcHRzLnJlcXVlc3RQcm9ncmVzcykge1xuICAgICAgY29uZmlnLnB1c2goeyBSZXF1ZXN0UHJvZ3Jlc3M6IHNlbGVjdE9wdHM/LnJlcXVlc3RQcm9ncmVzcyB9KVxuICAgIH1cbiAgICAvLyBPcHRpb25hbFxuICAgIGlmIChzZWxlY3RPcHRzLnNjYW5SYW5nZSkge1xuICAgICAgY29uZmlnLnB1c2goeyBTY2FuUmFuZ2U6IHNlbGVjdE9wdHMuc2NhblJhbmdlIH0pXG4gICAgfVxuXG4gICAgY29uc3QgYnVpbGRlciA9IG5ldyB4bWwyanMuQnVpbGRlcih7XG4gICAgICByb290TmFtZTogJ1NlbGVjdE9iamVjdENvbnRlbnRSZXF1ZXN0JyxcbiAgICAgIHJlbmRlck9wdHM6IHsgcHJldHR5OiBmYWxzZSB9LFxuICAgICAgaGVhZGxlc3M6IHRydWUsXG4gICAgfSlcbiAgICBjb25zdCBwYXlsb2FkID0gYnVpbGRlci5idWlsZE9iamVjdChjb25maWcpXG5cbiAgICBjb25zdCByZXMgPSBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0QXN5bmMoeyBtZXRob2QsIGJ1Y2tldE5hbWUsIG9iamVjdE5hbWUsIHF1ZXJ5IH0sIHBheWxvYWQpXG4gICAgY29uc3QgYm9keSA9IGF3YWl0IHJlYWRBc0J1ZmZlcihyZXMpXG4gICAgcmV0dXJuIHBhcnNlU2VsZWN0T2JqZWN0Q29udGVudFJlc3BvbnNlKGJvZHkpXG4gIH1cblxuICBwcml2YXRlIGFzeW5jIGFwcGx5QnVja2V0TGlmZWN5Y2xlKGJ1Y2tldE5hbWU6IHN0cmluZywgcG9saWN5Q29uZmlnOiBMaWZlQ3ljbGVDb25maWdQYXJhbSk6IFByb21pc2U8dm9pZD4ge1xuICAgIGNvbnN0IG1ldGhvZCA9ICdQVVQnXG4gICAgY29uc3QgcXVlcnkgPSAnbGlmZWN5Y2xlJ1xuXG4gICAgY29uc3QgaGVhZGVyczogUmVxdWVzdEhlYWRlcnMgPSB7fVxuICAgIGNvbnN0IGJ1aWxkZXIgPSBuZXcgeG1sMmpzLkJ1aWxkZXIoe1xuICAgICAgcm9vdE5hbWU6ICdMaWZlY3ljbGVDb25maWd1cmF0aW9uJyxcbiAgICAgIGhlYWRsZXNzOiB0cnVlLFxuICAgICAgcmVuZGVyT3B0czogeyBwcmV0dHk6IGZhbHNlIH0sXG4gICAgfSlcbiAgICBjb25zdCBwYXlsb2FkID0gYnVpbGRlci5idWlsZE9iamVjdChwb2xpY3lDb25maWcpXG4gICAgaGVhZGVyc1snQ29udGVudC1NRDUnXSA9IHRvTWQ1KHBheWxvYWQpXG5cbiAgICBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0QXN5bmNPbWl0KHsgbWV0aG9kLCBidWNrZXROYW1lLCBxdWVyeSwgaGVhZGVycyB9LCBwYXlsb2FkKVxuICB9XG5cbiAgYXN5bmMgcmVtb3ZlQnVja2V0TGlmZWN5Y2xlKGJ1Y2tldE5hbWU6IHN0cmluZyk6IFByb21pc2U8dm9pZD4ge1xuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcignSW52YWxpZCBidWNrZXQgbmFtZTogJyArIGJ1Y2tldE5hbWUpXG4gICAgfVxuICAgIGNvbnN0IG1ldGhvZCA9ICdERUxFVEUnXG4gICAgY29uc3QgcXVlcnkgPSAnbGlmZWN5Y2xlJ1xuICAgIGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luY09taXQoeyBtZXRob2QsIGJ1Y2tldE5hbWUsIHF1ZXJ5IH0sICcnLCBbMjA0XSlcbiAgfVxuXG4gIGFzeW5jIHNldEJ1Y2tldExpZmVjeWNsZShidWNrZXROYW1lOiBzdHJpbmcsIGxpZmVDeWNsZUNvbmZpZzogTGlmZUN5Y2xlQ29uZmlnUGFyYW0pOiBQcm9taXNlPHZvaWQ+IHtcbiAgICBpZiAoIWlzVmFsaWRCdWNrZXROYW1lKGJ1Y2tldE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IoJ0ludmFsaWQgYnVja2V0IG5hbWU6ICcgKyBidWNrZXROYW1lKVxuICAgIH1cbiAgICBpZiAoXy5pc0VtcHR5KGxpZmVDeWNsZUNvbmZpZykpIHtcbiAgICAgIGF3YWl0IHRoaXMucmVtb3ZlQnVja2V0TGlmZWN5Y2xlKGJ1Y2tldE5hbWUpXG4gICAgfSBlbHNlIHtcbiAgICAgIGF3YWl0IHRoaXMuYXBwbHlCdWNrZXRMaWZlY3ljbGUoYnVja2V0TmFtZSwgbGlmZUN5Y2xlQ29uZmlnKVxuICAgIH1cbiAgfVxuXG4gIGFzeW5jIGdldEJ1Y2tldExpZmVjeWNsZShidWNrZXROYW1lOiBzdHJpbmcpOiBQcm9taXNlPExpZmVjeWNsZUNvbmZpZyB8IG51bGw+IHtcbiAgICBpZiAoIWlzVmFsaWRCdWNrZXROYW1lKGJ1Y2tldE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IoJ0ludmFsaWQgYnVja2V0IG5hbWU6ICcgKyBidWNrZXROYW1lKVxuICAgIH1cbiAgICBjb25zdCBtZXRob2QgPSAnR0VUJ1xuICAgIGNvbnN0IHF1ZXJ5ID0gJ2xpZmVjeWNsZSdcblxuICAgIGNvbnN0IHJlcyA9IGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luYyh7IG1ldGhvZCwgYnVja2V0TmFtZSwgcXVlcnkgfSlcbiAgICBjb25zdCBib2R5ID0gYXdhaXQgcmVhZEFzU3RyaW5nKHJlcylcbiAgICByZXR1cm4geG1sUGFyc2Vycy5wYXJzZUxpZmVjeWNsZUNvbmZpZyhib2R5KVxuICB9XG5cbiAgYXN5bmMgc2V0QnVja2V0RW5jcnlwdGlvbihidWNrZXROYW1lOiBzdHJpbmcsIGVuY3J5cHRpb25Db25maWc/OiBFbmNyeXB0aW9uQ29uZmlnKTogUHJvbWlzZTx2b2lkPiB7XG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZShidWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKCdJbnZhbGlkIGJ1Y2tldCBuYW1lOiAnICsgYnVja2V0TmFtZSlcbiAgICB9XG4gICAgaWYgKCFfLmlzRW1wdHkoZW5jcnlwdGlvbkNvbmZpZykgJiYgZW5jcnlwdGlvbkNvbmZpZy5SdWxlLmxlbmd0aCA+IDEpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEFyZ3VtZW50RXJyb3IoJ0ludmFsaWQgUnVsZSBsZW5ndGguIE9ubHkgb25lIHJ1bGUgaXMgYWxsb3dlZC46ICcgKyBlbmNyeXB0aW9uQ29uZmlnLlJ1bGUpXG4gICAgfVxuXG4gICAgbGV0IGVuY3J5cHRpb25PYmogPSBlbmNyeXB0aW9uQ29uZmlnXG4gICAgaWYgKF8uaXNFbXB0eShlbmNyeXB0aW9uQ29uZmlnKSkge1xuICAgICAgZW5jcnlwdGlvbk9iaiA9IHtcbiAgICAgICAgLy8gRGVmYXVsdCBNaW5JTyBTZXJ2ZXIgU3VwcG9ydGVkIFJ1bGVcbiAgICAgICAgUnVsZTogW1xuICAgICAgICAgIHtcbiAgICAgICAgICAgIEFwcGx5U2VydmVyU2lkZUVuY3J5cHRpb25CeURlZmF1bHQ6IHtcbiAgICAgICAgICAgICAgU1NFQWxnb3JpdGhtOiAnQUVTMjU2JyxcbiAgICAgICAgICAgIH0sXG4gICAgICAgICAgfSxcbiAgICAgICAgXSxcbiAgICAgIH1cbiAgICB9XG5cbiAgICBjb25zdCBtZXRob2QgPSAnUFVUJ1xuICAgIGNvbnN0IHF1ZXJ5ID0gJ2VuY3J5cHRpb24nXG4gICAgY29uc3QgYnVpbGRlciA9IG5ldyB4bWwyanMuQnVpbGRlcih7XG4gICAgICByb290TmFtZTogJ1NlcnZlclNpZGVFbmNyeXB0aW9uQ29uZmlndXJhdGlvbicsXG4gICAgICByZW5kZXJPcHRzOiB7IHByZXR0eTogZmFsc2UgfSxcbiAgICAgIGhlYWRsZXNzOiB0cnVlLFxuICAgIH0pXG4gICAgY29uc3QgcGF5bG9hZCA9IGJ1aWxkZXIuYnVpbGRPYmplY3QoZW5jcnlwdGlvbk9iailcblxuICAgIGNvbnN0IGhlYWRlcnM6IFJlcXVlc3RIZWFkZXJzID0ge31cbiAgICBoZWFkZXJzWydDb250ZW50LU1ENSddID0gdG9NZDUocGF5bG9hZClcblxuICAgIGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luY09taXQoeyBtZXRob2QsIGJ1Y2tldE5hbWUsIHF1ZXJ5LCBoZWFkZXJzIH0sIHBheWxvYWQpXG4gIH1cblxuICBhc3luYyBnZXRCdWNrZXRFbmNyeXB0aW9uKGJ1Y2tldE5hbWU6IHN0cmluZykge1xuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcignSW52YWxpZCBidWNrZXQgbmFtZTogJyArIGJ1Y2tldE5hbWUpXG4gICAgfVxuICAgIGNvbnN0IG1ldGhvZCA9ICdHRVQnXG4gICAgY29uc3QgcXVlcnkgPSAnZW5jcnlwdGlvbidcblxuICAgIGNvbnN0IHJlcyA9IGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luYyh7IG1ldGhvZCwgYnVja2V0TmFtZSwgcXVlcnkgfSlcbiAgICBjb25zdCBib2R5ID0gYXdhaXQgcmVhZEFzU3RyaW5nKHJlcylcbiAgICByZXR1cm4geG1sUGFyc2Vycy5wYXJzZUJ1Y2tldEVuY3J5cHRpb25Db25maWcoYm9keSlcbiAgfVxuXG4gIGFzeW5jIHJlbW92ZUJ1Y2tldEVuY3J5cHRpb24oYnVja2V0TmFtZTogc3RyaW5nKSB7XG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZShidWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKCdJbnZhbGlkIGJ1Y2tldCBuYW1lOiAnICsgYnVja2V0TmFtZSlcbiAgICB9XG4gICAgY29uc3QgbWV0aG9kID0gJ0RFTEVURSdcbiAgICBjb25zdCBxdWVyeSA9ICdlbmNyeXB0aW9uJ1xuXG4gICAgYXdhaXQgdGhpcy5tYWtlUmVxdWVzdEFzeW5jT21pdCh7IG1ldGhvZCwgYnVja2V0TmFtZSwgcXVlcnkgfSwgJycsIFsyMDRdKVxuICB9XG5cbiAgYXN5bmMgZ2V0T2JqZWN0UmV0ZW50aW9uKFxuICAgIGJ1Y2tldE5hbWU6IHN0cmluZyxcbiAgICBvYmplY3ROYW1lOiBzdHJpbmcsXG4gICAgZ2V0T3B0cz86IEdldE9iamVjdFJldGVudGlvbk9wdHMsXG4gICk6IFByb21pc2U8T2JqZWN0UmV0ZW50aW9uSW5mbyB8IG51bGwgfCB1bmRlZmluZWQ+IHtcbiAgICBpZiAoIWlzVmFsaWRCdWNrZXROYW1lKGJ1Y2tldE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IoJ0ludmFsaWQgYnVja2V0IG5hbWU6ICcgKyBidWNrZXROYW1lKVxuICAgIH1cbiAgICBpZiAoIWlzVmFsaWRPYmplY3ROYW1lKG9iamVjdE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRPYmplY3ROYW1lRXJyb3IoYEludmFsaWQgb2JqZWN0IG5hbWU6ICR7b2JqZWN0TmFtZX1gKVxuICAgIH1cbiAgICBpZiAoZ2V0T3B0cyAmJiAhaXNPYmplY3QoZ2V0T3B0cykpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEFyZ3VtZW50RXJyb3IoJ2dldE9wdHMgc2hvdWxkIGJlIG9mIHR5cGUgXCJvYmplY3RcIicpXG4gICAgfSBlbHNlIGlmIChnZXRPcHRzPy52ZXJzaW9uSWQgJiYgIWlzU3RyaW5nKGdldE9wdHMudmVyc2lvbklkKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQXJndW1lbnRFcnJvcigndmVyc2lvbklkIHNob3VsZCBiZSBvZiB0eXBlIFwic3RyaW5nXCInKVxuICAgIH1cblxuICAgIGNvbnN0IG1ldGhvZCA9ICdHRVQnXG4gICAgbGV0IHF1ZXJ5ID0gJ3JldGVudGlvbidcbiAgICBpZiAoZ2V0T3B0cz8udmVyc2lvbklkKSB7XG4gICAgICBxdWVyeSArPSBgJnZlcnNpb25JZD0ke2dldE9wdHMudmVyc2lvbklkfWBcbiAgICB9XG4gICAgY29uc3QgcmVzID0gYXdhaXQgdGhpcy5tYWtlUmVxdWVzdEFzeW5jKHsgbWV0aG9kLCBidWNrZXROYW1lLCBvYmplY3ROYW1lLCBxdWVyeSB9KVxuICAgIGNvbnN0IGJvZHkgPSBhd2FpdCByZWFkQXNTdHJpbmcocmVzKVxuICAgIHJldHVybiB4bWxQYXJzZXJzLnBhcnNlT2JqZWN0UmV0ZW50aW9uQ29uZmlnKGJvZHkpXG4gIH1cblxuICBhc3luYyByZW1vdmVPYmplY3RzKGJ1Y2tldE5hbWU6IHN0cmluZywgb2JqZWN0c0xpc3Q6IFJlbW92ZU9iamVjdHNQYXJhbSk6IFByb21pc2U8UmVtb3ZlT2JqZWN0c1Jlc3BvbnNlW10+IHtcbiAgICBpZiAoIWlzVmFsaWRCdWNrZXROYW1lKGJ1Y2tldE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IoJ0ludmFsaWQgYnVja2V0IG5hbWU6ICcgKyBidWNrZXROYW1lKVxuICAgIH1cbiAgICBpZiAoIUFycmF5LmlzQXJyYXkob2JqZWN0c0xpc3QpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKCdvYmplY3RzTGlzdCBzaG91bGQgYmUgYSBsaXN0JylcbiAgICB9XG5cbiAgICBjb25zdCBydW5EZWxldGVPYmplY3RzID0gYXN5bmMgKGJhdGNoOiBSZW1vdmVPYmplY3RzUGFyYW0pOiBQcm9taXNlPFJlbW92ZU9iamVjdHNSZXNwb25zZVtdPiA9PiB7XG4gICAgICBjb25zdCBkZWxPYmplY3RzOiBSZW1vdmVPYmplY3RzUmVxdWVzdEVudHJ5W10gPSBiYXRjaC5tYXAoKHZhbHVlKSA9PiB7XG4gICAgICAgIHJldHVybiBpc09iamVjdCh2YWx1ZSkgPyB7IEtleTogdmFsdWUubmFtZSwgVmVyc2lvbklkOiB2YWx1ZS52ZXJzaW9uSWQgfSA6IHsgS2V5OiB2YWx1ZSB9XG4gICAgICB9KVxuXG4gICAgICBjb25zdCByZW1PYmplY3RzID0geyBEZWxldGU6IHsgUXVpZXQ6IHRydWUsIE9iamVjdDogZGVsT2JqZWN0cyB9IH1cbiAgICAgIGNvbnN0IHBheWxvYWQgPSBCdWZmZXIuZnJvbShuZXcgeG1sMmpzLkJ1aWxkZXIoeyBoZWFkbGVzczogdHJ1ZSB9KS5idWlsZE9iamVjdChyZW1PYmplY3RzKSlcbiAgICAgIGNvbnN0IGhlYWRlcnM6IFJlcXVlc3RIZWFkZXJzID0geyAnQ29udGVudC1NRDUnOiB0b01kNShwYXlsb2FkKSB9XG5cbiAgICAgIGNvbnN0IHJlcyA9IGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luYyh7IG1ldGhvZDogJ1BPU1QnLCBidWNrZXROYW1lLCBxdWVyeTogJ2RlbGV0ZScsIGhlYWRlcnMgfSwgcGF5bG9hZClcbiAgICAgIGNvbnN0IGJvZHkgPSBhd2FpdCByZWFkQXNTdHJpbmcocmVzKVxuICAgICAgcmV0dXJuIHhtbFBhcnNlcnMucmVtb3ZlT2JqZWN0c1BhcnNlcihib2R5KVxuICAgIH1cblxuICAgIGNvbnN0IG1heEVudHJpZXMgPSAxMDAwIC8vIG1heCBlbnRyaWVzIGFjY2VwdGVkIGluIHNlcnZlciBmb3IgRGVsZXRlTXVsdGlwbGVPYmplY3RzIEFQSS5cbiAgICAvLyBDbGllbnQgc2lkZSBiYXRjaGluZ1xuICAgIGNvbnN0IGJhdGNoZXMgPSBbXVxuICAgIGZvciAobGV0IGkgPSAwOyBpIDwgb2JqZWN0c0xpc3QubGVuZ3RoOyBpICs9IG1heEVudHJpZXMpIHtcbiAgICAgIGJhdGNoZXMucHVzaChvYmplY3RzTGlzdC5zbGljZShpLCBpICsgbWF4RW50cmllcykpXG4gICAgfVxuXG4gICAgY29uc3QgYmF0Y2hSZXN1bHRzID0gYXdhaXQgUHJvbWlzZS5hbGwoYmF0Y2hlcy5tYXAocnVuRGVsZXRlT2JqZWN0cykpXG4gICAgcmV0dXJuIGJhdGNoUmVzdWx0cy5mbGF0KClcbiAgfVxuXG4gIGFzeW5jIHJlbW92ZUluY29tcGxldGVVcGxvYWQoYnVja2V0TmFtZTogc3RyaW5nLCBvYmplY3ROYW1lOiBzdHJpbmcpOiBQcm9taXNlPHZvaWQ+IHtcbiAgICBpZiAoIWlzVmFsaWRCdWNrZXROYW1lKGJ1Y2tldE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLklzVmFsaWRCdWNrZXROYW1lRXJyb3IoJ0ludmFsaWQgYnVja2V0IG5hbWU6ICcgKyBidWNrZXROYW1lKVxuICAgIH1cbiAgICBpZiAoIWlzVmFsaWRPYmplY3ROYW1lKG9iamVjdE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRPYmplY3ROYW1lRXJyb3IoYEludmFsaWQgb2JqZWN0IG5hbWU6ICR7b2JqZWN0TmFtZX1gKVxuICAgIH1cbiAgICBjb25zdCByZW1vdmVVcGxvYWRJZCA9IGF3YWl0IHRoaXMuZmluZFVwbG9hZElkKGJ1Y2tldE5hbWUsIG9iamVjdE5hbWUpXG4gICAgY29uc3QgbWV0aG9kID0gJ0RFTEVURSdcbiAgICBjb25zdCBxdWVyeSA9IGB1cGxvYWRJZD0ke3JlbW92ZVVwbG9hZElkfWBcbiAgICBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0QXN5bmNPbWl0KHsgbWV0aG9kLCBidWNrZXROYW1lLCBvYmplY3ROYW1lLCBxdWVyeSB9LCAnJywgWzIwNF0pXG4gIH1cblxuICBwcml2YXRlIGFzeW5jIGNvcHlPYmplY3RWMShcbiAgICB0YXJnZXRCdWNrZXROYW1lOiBzdHJpbmcsXG4gICAgdGFyZ2V0T2JqZWN0TmFtZTogc3RyaW5nLFxuICAgIHNvdXJjZUJ1Y2tldE5hbWVBbmRPYmplY3ROYW1lOiBzdHJpbmcsXG4gICAgY29uZGl0aW9ucz86IG51bGwgfCBDb3B5Q29uZGl0aW9ucyxcbiAgKSB7XG4gICAgaWYgKHR5cGVvZiBjb25kaXRpb25zID09ICdmdW5jdGlvbicpIHtcbiAgICAgIGNvbmRpdGlvbnMgPSBudWxsXG4gICAgfVxuXG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZSh0YXJnZXRCdWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKCdJbnZhbGlkIGJ1Y2tldCBuYW1lOiAnICsgdGFyZ2V0QnVja2V0TmFtZSlcbiAgICB9XG4gICAgaWYgKCFpc1ZhbGlkT2JqZWN0TmFtZSh0YXJnZXRPYmplY3ROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkT2JqZWN0TmFtZUVycm9yKGBJbnZhbGlkIG9iamVjdCBuYW1lOiAke3RhcmdldE9iamVjdE5hbWV9YClcbiAgICB9XG4gICAgaWYgKCFpc1N0cmluZyhzb3VyY2VCdWNrZXROYW1lQW5kT2JqZWN0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ3NvdXJjZUJ1Y2tldE5hbWVBbmRPYmplY3ROYW1lIHNob3VsZCBiZSBvZiB0eXBlIFwic3RyaW5nXCInKVxuICAgIH1cbiAgICBpZiAoc291cmNlQnVja2V0TmFtZUFuZE9iamVjdE5hbWUgPT09ICcnKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRQcmVmaXhFcnJvcihgRW1wdHkgc291cmNlIHByZWZpeGApXG4gICAgfVxuXG4gICAgaWYgKGNvbmRpdGlvbnMgIT0gbnVsbCAmJiAhKGNvbmRpdGlvbnMgaW5zdGFuY2VvZiBDb3B5Q29uZGl0aW9ucykpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ2NvbmRpdGlvbnMgc2hvdWxkIGJlIG9mIHR5cGUgXCJDb3B5Q29uZGl0aW9uc1wiJylcbiAgICB9XG5cbiAgICBjb25zdCBoZWFkZXJzOiBSZXF1ZXN0SGVhZGVycyA9IHt9XG4gICAgaGVhZGVyc1sneC1hbXotY29weS1zb3VyY2UnXSA9IHVyaVJlc291cmNlRXNjYXBlKHNvdXJjZUJ1Y2tldE5hbWVBbmRPYmplY3ROYW1lKVxuXG4gICAgaWYgKGNvbmRpdGlvbnMpIHtcbiAgICAgIGlmIChjb25kaXRpb25zLm1vZGlmaWVkICE9PSAnJykge1xuICAgICAgICBoZWFkZXJzWyd4LWFtei1jb3B5LXNvdXJjZS1pZi1tb2RpZmllZC1zaW5jZSddID0gY29uZGl0aW9ucy5tb2RpZmllZFxuICAgICAgfVxuICAgICAgaWYgKGNvbmRpdGlvbnMudW5tb2RpZmllZCAhPT0gJycpIHtcbiAgICAgICAgaGVhZGVyc1sneC1hbXotY29weS1zb3VyY2UtaWYtdW5tb2RpZmllZC1zaW5jZSddID0gY29uZGl0aW9ucy51bm1vZGlmaWVkXG4gICAgICB9XG4gICAgICBpZiAoY29uZGl0aW9ucy5tYXRjaEVUYWcgIT09ICcnKSB7XG4gICAgICAgIGhlYWRlcnNbJ3gtYW16LWNvcHktc291cmNlLWlmLW1hdGNoJ10gPSBjb25kaXRpb25zLm1hdGNoRVRhZ1xuICAgICAgfVxuICAgICAgaWYgKGNvbmRpdGlvbnMubWF0Y2hFVGFnRXhjZXB0ICE9PSAnJykge1xuICAgICAgICBoZWFkZXJzWyd4LWFtei1jb3B5LXNvdXJjZS1pZi1ub25lLW1hdGNoJ10gPSBjb25kaXRpb25zLm1hdGNoRVRhZ0V4Y2VwdFxuICAgICAgfVxuICAgIH1cblxuICAgIGNvbnN0IG1ldGhvZCA9ICdQVVQnXG5cbiAgICBjb25zdCByZXMgPSBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0QXN5bmMoe1xuICAgICAgbWV0aG9kLFxuICAgICAgYnVja2V0TmFtZTogdGFyZ2V0QnVja2V0TmFtZSxcbiAgICAgIG9iamVjdE5hbWU6IHRhcmdldE9iamVjdE5hbWUsXG4gICAgICBoZWFkZXJzLFxuICAgIH0pXG4gICAgY29uc3QgYm9keSA9IGF3YWl0IHJlYWRBc1N0cmluZyhyZXMpXG4gICAgcmV0dXJuIHhtbFBhcnNlcnMucGFyc2VDb3B5T2JqZWN0KGJvZHkpXG4gIH1cblxuICBwcml2YXRlIGFzeW5jIGNvcHlPYmplY3RWMihcbiAgICBzb3VyY2VDb25maWc6IENvcHlTb3VyY2VPcHRpb25zLFxuICAgIGRlc3RDb25maWc6IENvcHlEZXN0aW5hdGlvbk9wdGlvbnMsXG4gICk6IFByb21pc2U8Q29weU9iamVjdFJlc3VsdFYyPiB7XG4gICAgaWYgKCEoc291cmNlQ29uZmlnIGluc3RhbmNlb2YgQ29weVNvdXJjZU9wdGlvbnMpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKCdzb3VyY2VDb25maWcgc2hvdWxkIG9mIHR5cGUgQ29weVNvdXJjZU9wdGlvbnMgJylcbiAgICB9XG4gICAgaWYgKCEoZGVzdENvbmZpZyBpbnN0YW5jZW9mIENvcHlEZXN0aW5hdGlvbk9wdGlvbnMpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKCdkZXN0Q29uZmlnIHNob3VsZCBvZiB0eXBlIENvcHlEZXN0aW5hdGlvbk9wdGlvbnMgJylcbiAgICB9XG4gICAgaWYgKCFkZXN0Q29uZmlnLnZhbGlkYXRlKCkpIHtcbiAgICAgIHJldHVybiBQcm9taXNlLnJlamVjdCgpXG4gICAgfVxuICAgIGlmICghZGVzdENvbmZpZy52YWxpZGF0ZSgpKSB7XG4gICAgICByZXR1cm4gUHJvbWlzZS5yZWplY3QoKVxuICAgIH1cblxuICAgIGNvbnN0IGhlYWRlcnMgPSBPYmplY3QuYXNzaWduKHt9LCBzb3VyY2VDb25maWcuZ2V0SGVhZGVycygpLCBkZXN0Q29uZmlnLmdldEhlYWRlcnMoKSlcblxuICAgIGNvbnN0IGJ1Y2tldE5hbWUgPSBkZXN0Q29uZmlnLkJ1Y2tldFxuICAgIGNvbnN0IG9iamVjdE5hbWUgPSBkZXN0Q29uZmlnLk9iamVjdFxuXG4gICAgY29uc3QgbWV0aG9kID0gJ1BVVCdcblxuICAgIGNvbnN0IHJlcyA9IGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luYyh7IG1ldGhvZCwgYnVja2V0TmFtZSwgb2JqZWN0TmFtZSwgaGVhZGVycyB9KVxuICAgIGNvbnN0IGJvZHkgPSBhd2FpdCByZWFkQXNTdHJpbmcocmVzKVxuICAgIGNvbnN0IGNvcHlSZXMgPSB4bWxQYXJzZXJzLnBhcnNlQ29weU9iamVjdChib2R5KVxuICAgIGNvbnN0IHJlc0hlYWRlcnM6IEluY29taW5nSHR0cEhlYWRlcnMgPSByZXMuaGVhZGVyc1xuXG4gICAgY29uc3Qgc2l6ZUhlYWRlclZhbHVlID0gcmVzSGVhZGVycyAmJiByZXNIZWFkZXJzWydjb250ZW50LWxlbmd0aCddXG4gICAgY29uc3Qgc2l6ZSA9IHR5cGVvZiBzaXplSGVhZGVyVmFsdWUgPT09ICdudW1iZXInID8gc2l6ZUhlYWRlclZhbHVlIDogdW5kZWZpbmVkXG5cbiAgICByZXR1cm4ge1xuICAgICAgQnVja2V0OiBkZXN0Q29uZmlnLkJ1Y2tldCxcbiAgICAgIEtleTogZGVzdENvbmZpZy5PYmplY3QsXG4gICAgICBMYXN0TW9kaWZpZWQ6IGNvcHlSZXMubGFzdE1vZGlmaWVkLFxuICAgICAgTWV0YURhdGE6IGV4dHJhY3RNZXRhZGF0YShyZXNIZWFkZXJzIGFzIFJlc3BvbnNlSGVhZGVyKSxcbiAgICAgIFZlcnNpb25JZDogZ2V0VmVyc2lvbklkKHJlc0hlYWRlcnMgYXMgUmVzcG9uc2VIZWFkZXIpLFxuICAgICAgU291cmNlVmVyc2lvbklkOiBnZXRTb3VyY2VWZXJzaW9uSWQocmVzSGVhZGVycyBhcyBSZXNwb25zZUhlYWRlciksXG4gICAgICBFdGFnOiBzYW5pdGl6ZUVUYWcocmVzSGVhZGVycy5ldGFnKSxcbiAgICAgIFNpemU6IHNpemUsXG4gICAgfVxuICB9XG5cbiAgYXN5bmMgY29weU9iamVjdChzb3VyY2U6IENvcHlTb3VyY2VPcHRpb25zLCBkZXN0OiBDb3B5RGVzdGluYXRpb25PcHRpb25zKTogUHJvbWlzZTxDb3B5T2JqZWN0UmVzdWx0PlxuICBhc3luYyBjb3B5T2JqZWN0KFxuICAgIHRhcmdldEJ1Y2tldE5hbWU6IHN0cmluZyxcbiAgICB0YXJnZXRPYmplY3ROYW1lOiBzdHJpbmcsXG4gICAgc291cmNlQnVja2V0TmFtZUFuZE9iamVjdE5hbWU6IHN0cmluZyxcbiAgICBjb25kaXRpb25zPzogQ29weUNvbmRpdGlvbnMsXG4gICk6IFByb21pc2U8Q29weU9iamVjdFJlc3VsdD5cbiAgYXN5bmMgY29weU9iamVjdCguLi5hbGxBcmdzOiBDb3B5T2JqZWN0UGFyYW1zKTogUHJvbWlzZTxDb3B5T2JqZWN0UmVzdWx0PiB7XG4gICAgaWYgKHR5cGVvZiBhbGxBcmdzWzBdID09PSAnc3RyaW5nJykge1xuICAgICAgY29uc3QgW3RhcmdldEJ1Y2tldE5hbWUsIHRhcmdldE9iamVjdE5hbWUsIHNvdXJjZUJ1Y2tldE5hbWVBbmRPYmplY3ROYW1lLCBjb25kaXRpb25zXSA9IGFsbEFyZ3MgYXMgW1xuICAgICAgICBzdHJpbmcsXG4gICAgICAgIHN0cmluZyxcbiAgICAgICAgc3RyaW5nLFxuICAgICAgICBDb3B5Q29uZGl0aW9ucz8sXG4gICAgICBdXG4gICAgICByZXR1cm4gYXdhaXQgdGhpcy5jb3B5T2JqZWN0VjEodGFyZ2V0QnVja2V0TmFtZSwgdGFyZ2V0T2JqZWN0TmFtZSwgc291cmNlQnVja2V0TmFtZUFuZE9iamVjdE5hbWUsIGNvbmRpdGlvbnMpXG4gICAgfVxuICAgIGNvbnN0IFtzb3VyY2UsIGRlc3RdID0gYWxsQXJncyBhcyBbQ29weVNvdXJjZU9wdGlvbnMsIENvcHlEZXN0aW5hdGlvbk9wdGlvbnNdXG4gICAgcmV0dXJuIGF3YWl0IHRoaXMuY29weU9iamVjdFYyKHNvdXJjZSwgZGVzdClcbiAgfVxuXG4gIGFzeW5jIHVwbG9hZFBhcnQoXG4gICAgcGFydENvbmZpZzoge1xuICAgICAgYnVja2V0TmFtZTogc3RyaW5nXG4gICAgICBvYmplY3ROYW1lOiBzdHJpbmdcbiAgICAgIHVwbG9hZElEOiBzdHJpbmdcbiAgICAgIHBhcnROdW1iZXI6IG51bWJlclxuICAgICAgaGVhZGVyczogUmVxdWVzdEhlYWRlcnNcbiAgICB9LFxuICAgIHBheWxvYWQ/OiBCaW5hcnksXG4gICkge1xuICAgIGNvbnN0IHsgYnVja2V0TmFtZSwgb2JqZWN0TmFtZSwgdXBsb2FkSUQsIHBhcnROdW1iZXIsIGhlYWRlcnMgfSA9IHBhcnRDb25maWdcblxuICAgIGNvbnN0IG1ldGhvZCA9ICdQVVQnXG4gICAgY29uc3QgcXVlcnkgPSBgdXBsb2FkSWQ9JHt1cGxvYWRJRH0mcGFydE51bWJlcj0ke3BhcnROdW1iZXJ9YFxuICAgIGNvbnN0IHJlcXVlc3RPcHRpb25zID0geyBtZXRob2QsIGJ1Y2tldE5hbWUsIG9iamVjdE5hbWU6IG9iamVjdE5hbWUsIHF1ZXJ5LCBoZWFkZXJzIH1cbiAgICBjb25zdCByZXMgPSBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0QXN5bmMocmVxdWVzdE9wdGlvbnMsIHBheWxvYWQpXG4gICAgY29uc3QgYm9keSA9IGF3YWl0IHJlYWRBc1N0cmluZyhyZXMpXG4gICAgY29uc3QgcGFydFJlcyA9IHVwbG9hZFBhcnRQYXJzZXIoYm9keSlcbiAgICBjb25zdCBwYXJ0RXRhZ1ZhbCA9IHNhbml0aXplRVRhZyhyZXMuaGVhZGVycy5ldGFnKSB8fCBzYW5pdGl6ZUVUYWcocGFydFJlcy5FVGFnKVxuICAgIHJldHVybiB7XG4gICAgICBldGFnOiBwYXJ0RXRhZ1ZhbCxcbiAgICAgIGtleTogb2JqZWN0TmFtZSxcbiAgICAgIHBhcnQ6IHBhcnROdW1iZXIsXG4gICAgfVxuICB9XG5cbiAgYXN5bmMgY29tcG9zZU9iamVjdChcbiAgICBkZXN0T2JqQ29uZmlnOiBDb3B5RGVzdGluYXRpb25PcHRpb25zLFxuICAgIHNvdXJjZU9iakxpc3Q6IENvcHlTb3VyY2VPcHRpb25zW10sXG4gICAgeyBtYXhDb25jdXJyZW5jeSA9IDEwIH0gPSB7fSxcbiAgKTogUHJvbWlzZTxib29sZWFuIHwgeyBldGFnOiBzdHJpbmc7IHZlcnNpb25JZDogc3RyaW5nIHwgbnVsbCB9IHwgUHJvbWlzZTx2b2lkPiB8IENvcHlPYmplY3RSZXN1bHQ+IHtcbiAgICBjb25zdCBzb3VyY2VGaWxlc0xlbmd0aCA9IHNvdXJjZU9iakxpc3QubGVuZ3RoXG5cbiAgICBpZiAoIUFycmF5LmlzQXJyYXkoc291cmNlT2JqTGlzdCkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEFyZ3VtZW50RXJyb3IoJ3NvdXJjZUNvbmZpZyBzaG91bGQgYW4gYXJyYXkgb2YgQ29weVNvdXJjZU9wdGlvbnMgJylcbiAgICB9XG4gICAgaWYgKCEoZGVzdE9iakNvbmZpZyBpbnN0YW5jZW9mIENvcHlEZXN0aW5hdGlvbk9wdGlvbnMpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKCdkZXN0Q29uZmlnIHNob3VsZCBvZiB0eXBlIENvcHlEZXN0aW5hdGlvbk9wdGlvbnMgJylcbiAgICB9XG5cbiAgICBpZiAoc291cmNlRmlsZXNMZW5ndGggPCAxIHx8IHNvdXJjZUZpbGVzTGVuZ3RoID4gUEFSVF9DT05TVFJBSU5UUy5NQVhfUEFSVFNfQ09VTlQpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEFyZ3VtZW50RXJyb3IoXG4gICAgICAgIGBcIlRoZXJlIG11c3QgYmUgYXMgbGVhc3Qgb25lIGFuZCB1cCB0byAke1BBUlRfQ09OU1RSQUlOVFMuTUFYX1BBUlRTX0NPVU5UfSBzb3VyY2Ugb2JqZWN0cy5gLFxuICAgICAgKVxuICAgIH1cblxuICAgIGZvciAobGV0IGkgPSAwOyBpIDwgc291cmNlRmlsZXNMZW5ndGg7IGkrKykge1xuICAgICAgY29uc3Qgc09iaiA9IHNvdXJjZU9iakxpc3RbaV0gYXMgQ29weVNvdXJjZU9wdGlvbnNcbiAgICAgIGlmICghc09iai52YWxpZGF0ZSgpKSB7XG4gICAgICAgIHJldHVybiBmYWxzZVxuICAgICAgfVxuICAgIH1cblxuICAgIGlmICghKGRlc3RPYmpDb25maWcgYXMgQ29weURlc3RpbmF0aW9uT3B0aW9ucykudmFsaWRhdGUoKSkge1xuICAgICAgcmV0dXJuIGZhbHNlXG4gICAgfVxuXG4gICAgY29uc3QgZ2V0U3RhdE9wdGlvbnMgPSAoc3JjQ29uZmlnOiBDb3B5U291cmNlT3B0aW9ucykgPT4ge1xuICAgICAgbGV0IHN0YXRPcHRzID0ge31cbiAgICAgIGlmICghXy5pc0VtcHR5KHNyY0NvbmZpZy5WZXJzaW9uSUQpKSB7XG4gICAgICAgIHN0YXRPcHRzID0ge1xuICAgICAgICAgIHZlcnNpb25JZDogc3JjQ29uZmlnLlZlcnNpb25JRCxcbiAgICAgICAgfVxuICAgICAgfVxuICAgICAgcmV0dXJuIHN0YXRPcHRzXG4gICAgfVxuICAgIGNvbnN0IHNyY09iamVjdFNpemVzOiBudW1iZXJbXSA9IFtdXG4gICAgbGV0IHRvdGFsU2l6ZSA9IDBcbiAgICBsZXQgdG90YWxQYXJ0cyA9IDBcblxuICAgIGNvbnN0IHNvdXJjZU9ialN0YXRzID0gc291cmNlT2JqTGlzdC5tYXAoKHNyY0l0ZW0pID0+XG4gICAgICB0aGlzLnN0YXRPYmplY3Qoc3JjSXRlbS5CdWNrZXQsIHNyY0l0ZW0uT2JqZWN0LCBnZXRTdGF0T3B0aW9ucyhzcmNJdGVtKSksXG4gICAgKVxuXG4gICAgY29uc3Qgc3JjT2JqZWN0SW5mb3MgPSBhd2FpdCBQcm9taXNlLmFsbChzb3VyY2VPYmpTdGF0cylcblxuICAgIGNvbnN0IHZhbGlkYXRlZFN0YXRzID0gc3JjT2JqZWN0SW5mb3MubWFwKChyZXNJdGVtU3RhdCwgaW5kZXgpID0+IHtcbiAgICAgIGNvbnN0IHNyY0NvbmZpZzogQ29weVNvdXJjZU9wdGlvbnMgfCB1bmRlZmluZWQgPSBzb3VyY2VPYmpMaXN0W2luZGV4XVxuXG4gICAgICBsZXQgc3JjQ29weVNpemUgPSByZXNJdGVtU3RhdC5zaXplXG4gICAgICAvLyBDaGVjayBpZiBhIHNlZ21lbnQgaXMgc3BlY2lmaWVkLCBhbmQgaWYgc28sIGlzIHRoZVxuICAgICAgLy8gc2VnbWVudCB3aXRoaW4gb2JqZWN0IGJvdW5kcz9cbiAgICAgIGlmIChzcmNDb25maWcgJiYgc3JjQ29uZmlnLk1hdGNoUmFuZ2UpIHtcbiAgICAgICAgLy8gU2luY2UgcmFuZ2UgaXMgc3BlY2lmaWVkLFxuICAgICAgICAvLyAgICAwIDw9IHNyYy5zcmNTdGFydCA8PSBzcmMuc3JjRW5kXG4gICAgICAgIC8vIHNvIG9ubHkgaW52YWxpZCBjYXNlIHRvIGNoZWNrIGlzOlxuICAgICAgICBjb25zdCBzcmNTdGFydCA9IHNyY0NvbmZpZy5TdGFydFxuICAgICAgICBjb25zdCBzcmNFbmQgPSBzcmNDb25maWcuRW5kXG4gICAgICAgIGlmIChzcmNFbmQgPj0gc3JjQ29weVNpemUgfHwgc3JjU3RhcnQgPCAwKSB7XG4gICAgICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQXJndW1lbnRFcnJvcihcbiAgICAgICAgICAgIGBDb3B5U3JjT3B0aW9ucyAke2luZGV4fSBoYXMgaW52YWxpZCBzZWdtZW50LXRvLWNvcHkgWyR7c3JjU3RhcnR9LCAke3NyY0VuZH1dIChzaXplIGlzICR7c3JjQ29weVNpemV9KWAsXG4gICAgICAgICAgKVxuICAgICAgICB9XG4gICAgICAgIHNyY0NvcHlTaXplID0gc3JjRW5kIC0gc3JjU3RhcnQgKyAxXG4gICAgICB9XG5cbiAgICAgIC8vIE9ubHkgdGhlIGxhc3Qgc291cmNlIG1heSBiZSBsZXNzIHRoYW4gYGFic01pblBhcnRTaXplYFxuICAgICAgaWYgKHNyY0NvcHlTaXplIDwgUEFSVF9DT05TVFJBSU5UUy5BQlNfTUlOX1BBUlRfU0laRSAmJiBpbmRleCA8IHNvdXJjZUZpbGVzTGVuZ3RoIC0gMSkge1xuICAgICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKFxuICAgICAgICAgIGBDb3B5U3JjT3B0aW9ucyAke2luZGV4fSBpcyB0b28gc21hbGwgKCR7c3JjQ29weVNpemV9KSBhbmQgaXQgaXMgbm90IHRoZSBsYXN0IHBhcnQuYCxcbiAgICAgICAgKVxuICAgICAgfVxuXG4gICAgICAvLyBJcyBkYXRhIHRvIGNvcHkgdG9vIGxhcmdlP1xuICAgICAgdG90YWxTaXplICs9IHNyY0NvcHlTaXplXG4gICAgICBpZiAodG90YWxTaXplID4gUEFSVF9DT05TVFJBSU5UUy5NQVhfTVVMVElQQVJUX1BVVF9PQkpFQ1RfU0laRSkge1xuICAgICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKGBDYW5ub3QgY29tcG9zZSBhbiBvYmplY3Qgb2Ygc2l6ZSAke3RvdGFsU2l6ZX0gKD4gNVRpQilgKVxuICAgICAgfVxuXG4gICAgICAvLyByZWNvcmQgc291cmNlIHNpemVcbiAgICAgIHNyY09iamVjdFNpemVzW2luZGV4XSA9IHNyY0NvcHlTaXplXG5cbiAgICAgIC8vIGNhbGN1bGF0ZSBwYXJ0cyBuZWVkZWQgZm9yIGN1cnJlbnQgc291cmNlXG4gICAgICB0b3RhbFBhcnRzICs9IHBhcnRzUmVxdWlyZWQoc3JjQ29weVNpemUpXG4gICAgICAvLyBEbyB3ZSBuZWVkIG1vcmUgcGFydHMgdGhhbiB3ZSBhcmUgYWxsb3dlZD9cbiAgICAgIGlmICh0b3RhbFBhcnRzID4gUEFSVF9DT05TVFJBSU5UUy5NQVhfUEFSVFNfQ09VTlQpIHtcbiAgICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQXJndW1lbnRFcnJvcihcbiAgICAgICAgICBgWW91ciBwcm9wb3NlZCBjb21wb3NlIG9iamVjdCByZXF1aXJlcyBtb3JlIHRoYW4gJHtQQVJUX0NPTlNUUkFJTlRTLk1BWF9QQVJUU19DT1VOVH0gcGFydHNgLFxuICAgICAgICApXG4gICAgICB9XG5cbiAgICAgIHJldHVybiByZXNJdGVtU3RhdFxuICAgIH0pXG5cbiAgICBpZiAoKHRvdGFsUGFydHMgPT09IDEgJiYgdG90YWxTaXplIDw9IFBBUlRfQ09OU1RSQUlOVFMuTUFYX1BBUlRfU0laRSkgfHwgdG90YWxTaXplID09PSAwKSB7XG4gICAgICByZXR1cm4gYXdhaXQgdGhpcy5jb3B5T2JqZWN0KHNvdXJjZU9iakxpc3RbMF0gYXMgQ29weVNvdXJjZU9wdGlvbnMsIGRlc3RPYmpDb25maWcpIC8vIHVzZSBjb3B5T2JqZWN0VjJcbiAgICB9XG5cbiAgICAvLyBwcmVzZXJ2ZSBldGFnIHRvIGF2b2lkIG1vZGlmaWNhdGlvbiBvZiBvYmplY3Qgd2hpbGUgY29weWluZy5cbiAgICBmb3IgKGxldCBpID0gMDsgaSA8IHNvdXJjZUZpbGVzTGVuZ3RoOyBpKyspIHtcbiAgICAgIDsoc291cmNlT2JqTGlzdFtpXSBhcyBDb3B5U291cmNlT3B0aW9ucykuTWF0Y2hFVGFnID0gKHZhbGlkYXRlZFN0YXRzW2ldIGFzIEJ1Y2tldEl0ZW1TdGF0KS5ldGFnXG4gICAgfVxuXG4gICAgY29uc3Qgc3BsaXRQYXJ0U2l6ZUxpc3QgPSB2YWxpZGF0ZWRTdGF0cy5tYXAoKHJlc0l0ZW1TdGF0LCBpZHgpID0+IHtcbiAgICAgIHJldHVybiBjYWxjdWxhdGVFdmVuU3BsaXRzKHNyY09iamVjdFNpemVzW2lkeF0gYXMgbnVtYmVyLCBzb3VyY2VPYmpMaXN0W2lkeF0gYXMgQ29weVNvdXJjZU9wdGlvbnMpXG4gICAgfSlcblxuICAgIGNvbnN0IGdldFVwbG9hZFBhcnRDb25maWdMaXN0ID0gKHVwbG9hZElkOiBzdHJpbmcpID0+IHtcbiAgICAgIGNvbnN0IHVwbG9hZFBhcnRDb25maWdMaXN0OiBVcGxvYWRQYXJ0Q29uZmlnW10gPSBbXVxuXG4gICAgICBzcGxpdFBhcnRTaXplTGlzdC5mb3JFYWNoKChzcGxpdFNpemUsIHNwbGl0SW5kZXg6IG51bWJlcikgPT4ge1xuICAgICAgICBpZiAoc3BsaXRTaXplKSB7XG4gICAgICAgICAgY29uc3QgeyBzdGFydEluZGV4OiBzdGFydElkeCwgZW5kSW5kZXg6IGVuZElkeCB9ID0gc3BsaXRTaXplXG5cbiAgICAgICAgICBjb25zdCBwYXJ0SW5kZXggPSBzcGxpdEluZGV4ICsgMSAvLyBwYXJ0IGluZGV4IHN0YXJ0cyBmcm9tIDEuXG4gICAgICAgICAgY29uc3QgdG90YWxVcGxvYWRzID0gQXJyYXkuZnJvbShzdGFydElkeClcblxuICAgICAgICAgIGNvbnN0IGhlYWRlcnMgPSAoc291cmNlT2JqTGlzdFtzcGxpdEluZGV4XSBhcyBDb3B5U291cmNlT3B0aW9ucykuZ2V0SGVhZGVycygpXG5cbiAgICAgICAgICB0b3RhbFVwbG9hZHMuZm9yRWFjaCgoc3BsaXRTdGFydCwgdXBsZEN0cklkeCkgPT4ge1xuICAgICAgICAgICAgY29uc3Qgc3BsaXRFbmQgPSBlbmRJZHhbdXBsZEN0cklkeF1cblxuICAgICAgICAgICAgLy8geC1hbXotY29weS1zb3VyY2UgaXMgYWxyZWFkeSBzZXQgYnkgQ29weVNvdXJjZU9wdGlvbnMuZ2V0SGVhZGVycygpIGFib3ZlLFxuICAgICAgICAgICAgLy8gVVJJLWVzY2FwZWQgYW5kIGluY2x1ZGluZyB0aGUgP3ZlcnNpb25JZCBzZWxlY3RvciB3aGVuIHBpbm5lZC4gRG9uJ3RcbiAgICAgICAgICAgIC8vIG92ZXJ3cml0ZSBpdCBoZXJlOiB0aGUgcmF3IHZhbHVlIGJyb2tlIG5vbi1BU0NJSSBuYW1lcyAoRVJSX0lOVkFMSURfQ0hBUixcbiAgICAgICAgICAgIC8vICMxMzg1KSBhbmQgc2lsZW50bHkgZHJvcHBlZCB0aGUgc291cmNlIHZlcnNpb24uXG4gICAgICAgICAgICBoZWFkZXJzWyd4LWFtei1jb3B5LXNvdXJjZS1yYW5nZSddID0gYGJ5dGVzPSR7c3BsaXRTdGFydH0tJHtzcGxpdEVuZH1gXG5cbiAgICAgICAgICAgIGNvbnN0IHVwbG9hZFBhcnRDb25maWcgPSB7XG4gICAgICAgICAgICAgIGJ1Y2tldE5hbWU6IGRlc3RPYmpDb25maWcuQnVja2V0LFxuICAgICAgICAgICAgICBvYmplY3ROYW1lOiBkZXN0T2JqQ29uZmlnLk9iamVjdCxcbiAgICAgICAgICAgICAgdXBsb2FkSUQ6IHVwbG9hZElkLFxuICAgICAgICAgICAgICBwYXJ0TnVtYmVyOiBwYXJ0SW5kZXgsXG4gICAgICAgICAgICAgIGhlYWRlcnM6IGhlYWRlcnMsXG4gICAgICAgICAgICB9XG5cbiAgICAgICAgICAgIHVwbG9hZFBhcnRDb25maWdMaXN0LnB1c2godXBsb2FkUGFydENvbmZpZylcbiAgICAgICAgICB9KVxuICAgICAgICB9XG4gICAgICB9KVxuXG4gICAgICByZXR1cm4gdXBsb2FkUGFydENvbmZpZ0xpc3RcbiAgICB9XG5cbiAgICBjb25zdCB1cGxvYWRBbGxQYXJ0cyA9IGFzeW5jICh1cGxvYWRMaXN0OiBVcGxvYWRQYXJ0Q29uZmlnW10pID0+IHtcbiAgICAgIGNvbnN0IHBhcnRVcGxvYWRzOiBBd2FpdGVkPFJldHVyblR5cGU8dHlwZW9mIHRoaXMudXBsb2FkUGFydD4+W10gPSBbXVxuXG4gICAgICAvLyBQcm9jZXNzIHVwbG9hZCBwYXJ0cyBpbiBiYXRjaGVzIHRvIGF2b2lkIHRvbyBtYW55IGNvbmN1cnJlbnQgcmVxdWVzdHNcbiAgICAgIGZvciAoY29uc3QgYmF0Y2ggb2YgXy5jaHVuayh1cGxvYWRMaXN0LCBtYXhDb25jdXJyZW5jeSkpIHtcbiAgICAgICAgY29uc3QgYmF0Y2hSZXN1bHRzID0gYXdhaXQgUHJvbWlzZS5hbGwoYmF0Y2gubWFwKChpdGVtKSA9PiB0aGlzLnVwbG9hZFBhcnQoaXRlbSkpKVxuXG4gICAgICAgIHBhcnRVcGxvYWRzLnB1c2goLi4uYmF0Y2hSZXN1bHRzKVxuICAgICAgfVxuXG4gICAgICAvLyBQcm9jZXNzIHJlc3VsdHMgaGVyZSBpZiBuZWVkZWRcbiAgICAgIHJldHVybiBwYXJ0VXBsb2Fkc1xuICAgIH1cblxuICAgIGNvbnN0IHBlcmZvcm1VcGxvYWRQYXJ0cyA9IGFzeW5jICh1cGxvYWRJZDogc3RyaW5nKSA9PiB7XG4gICAgICBjb25zdCB1cGxvYWRMaXN0ID0gZ2V0VXBsb2FkUGFydENvbmZpZ0xpc3QodXBsb2FkSWQpXG4gICAgICBjb25zdCBwYXJ0c1JlcyA9IGF3YWl0IHVwbG9hZEFsbFBhcnRzKHVwbG9hZExpc3QpXG4gICAgICByZXR1cm4gcGFydHNSZXMubWFwKChwYXJ0Q29weSkgPT4gKHsgZXRhZzogcGFydENvcHkuZXRhZywgcGFydDogcGFydENvcHkucGFydCB9KSlcbiAgICB9XG5cbiAgICBjb25zdCBuZXdVcGxvYWRIZWFkZXJzID0gZGVzdE9iakNvbmZpZy5nZXRIZWFkZXJzKClcblxuICAgIGNvbnN0IHVwbG9hZElkID0gYXdhaXQgdGhpcy5pbml0aWF0ZU5ld011bHRpcGFydFVwbG9hZChkZXN0T2JqQ29uZmlnLkJ1Y2tldCwgZGVzdE9iakNvbmZpZy5PYmplY3QsIG5ld1VwbG9hZEhlYWRlcnMpXG4gICAgdHJ5IHtcbiAgICAgIGNvbnN0IHBhcnRzRG9uZSA9IGF3YWl0IHBlcmZvcm1VcGxvYWRQYXJ0cyh1cGxvYWRJZClcbiAgICAgIHJldHVybiBhd2FpdCB0aGlzLmNvbXBsZXRlTXVsdGlwYXJ0VXBsb2FkKGRlc3RPYmpDb25maWcuQnVja2V0LCBkZXN0T2JqQ29uZmlnLk9iamVjdCwgdXBsb2FkSWQsIHBhcnRzRG9uZSlcbiAgICB9IGNhdGNoIChlcnIpIHtcbiAgICAgIHJldHVybiBhd2FpdCB0aGlzLmFib3J0TXVsdGlwYXJ0VXBsb2FkKGRlc3RPYmpDb25maWcuQnVja2V0LCBkZXN0T2JqQ29uZmlnLk9iamVjdCwgdXBsb2FkSWQpXG4gICAgfVxuICB9XG5cbiAgYXN5bmMgcHJlc2lnbmVkVXJsKFxuICAgIG1ldGhvZDogc3RyaW5nLFxuICAgIGJ1Y2tldE5hbWU6IHN0cmluZyxcbiAgICBvYmplY3ROYW1lOiBzdHJpbmcsXG4gICAgZXhwaXJlcz86IG51bWJlciB8IFByZVNpZ25SZXF1ZXN0UGFyYW1zIHwgdW5kZWZpbmVkLFxuICAgIHJlcVBhcmFtcz86IFByZVNpZ25SZXF1ZXN0UGFyYW1zIHwgRGF0ZSxcbiAgICByZXF1ZXN0RGF0ZT86IERhdGUsXG4gICk6IFByb21pc2U8c3RyaW5nPiB7XG4gICAgaWYgKHRoaXMuYW5vbnltb3VzKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkFub255bW91c1JlcXVlc3RFcnJvcihgUHJlc2lnbmVkICR7bWV0aG9kfSB1cmwgY2Fubm90IGJlIGdlbmVyYXRlZCBmb3IgYW5vbnltb3VzIHJlcXVlc3RzYClcbiAgICB9XG5cbiAgICBpZiAoIWV4cGlyZXMpIHtcbiAgICAgIGV4cGlyZXMgPSBQUkVTSUdOX0VYUElSWV9EQVlTX01BWFxuICAgIH1cbiAgICBpZiAoIXJlcVBhcmFtcykge1xuICAgICAgcmVxUGFyYW1zID0ge31cbiAgICB9XG4gICAgaWYgKCFyZXF1ZXN0RGF0ZSkge1xuICAgICAgcmVxdWVzdERhdGUgPSBuZXcgRGF0ZSgpXG4gICAgfVxuXG4gICAgLy8gVHlwZSBhc3NlcnRpb25zXG4gICAgaWYgKGV4cGlyZXMgJiYgdHlwZW9mIGV4cGlyZXMgIT09ICdudW1iZXInKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdleHBpcmVzIHNob3VsZCBiZSBvZiB0eXBlIFwibnVtYmVyXCInKVxuICAgIH1cbiAgICBpZiAocmVxUGFyYW1zICYmIHR5cGVvZiByZXFQYXJhbXMgIT09ICdvYmplY3QnKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdyZXFQYXJhbXMgc2hvdWxkIGJlIG9mIHR5cGUgXCJvYmplY3RcIicpXG4gICAgfVxuICAgIGlmICgocmVxdWVzdERhdGUgJiYgIShyZXF1ZXN0RGF0ZSBpbnN0YW5jZW9mIERhdGUpKSB8fCAocmVxdWVzdERhdGUgJiYgaXNOYU4ocmVxdWVzdERhdGU/LmdldFRpbWUoKSkpKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdyZXF1ZXN0RGF0ZSBzaG91bGQgYmUgb2YgdHlwZSBcIkRhdGVcIiBhbmQgdmFsaWQnKVxuICAgIH1cblxuICAgIGNvbnN0IHF1ZXJ5ID0gcmVxUGFyYW1zID8gcXMuc3RyaW5naWZ5KHJlcVBhcmFtcykgOiB1bmRlZmluZWRcblxuICAgIHRyeSB7XG4gICAgICBjb25zdCByZWdpb24gPSBhd2FpdCB0aGlzLmdldEJ1Y2tldFJlZ2lvbkFzeW5jKGJ1Y2tldE5hbWUpXG4gICAgICBhd2FpdCB0aGlzLmNoZWNrQW5kUmVmcmVzaENyZWRzKClcbiAgICAgIGNvbnN0IHJlcU9wdGlvbnMgPSB0aGlzLmdldFJlcXVlc3RPcHRpb25zKHsgbWV0aG9kLCByZWdpb24sIGJ1Y2tldE5hbWUsIG9iamVjdE5hbWUsIHF1ZXJ5IH0pXG5cbiAgICAgIHJldHVybiBwcmVzaWduU2lnbmF0dXJlVjQoXG4gICAgICAgIHJlcU9wdGlvbnMsXG4gICAgICAgIHRoaXMuYWNjZXNzS2V5LFxuICAgICAgICB0aGlzLnNlY3JldEtleSxcbiAgICAgICAgdGhpcy5zZXNzaW9uVG9rZW4sXG4gICAgICAgIHJlZ2lvbixcbiAgICAgICAgcmVxdWVzdERhdGUsXG4gICAgICAgIGV4cGlyZXMsXG4gICAgICApXG4gICAgfSBjYXRjaCAoZXJyKSB7XG4gICAgICBpZiAoZXJyIGluc3RhbmNlb2YgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IpIHtcbiAgICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQXJndW1lbnRFcnJvcihgVW5hYmxlIHRvIGdldCBidWNrZXQgcmVnaW9uIGZvciAke2J1Y2tldE5hbWV9LmApXG4gICAgICB9XG5cbiAgICAgIHRocm93IGVyclxuICAgIH1cbiAgfVxuXG4gIGFzeW5jIHByZXNpZ25lZEdldE9iamVjdChcbiAgICBidWNrZXROYW1lOiBzdHJpbmcsXG4gICAgb2JqZWN0TmFtZTogc3RyaW5nLFxuICAgIGV4cGlyZXM/OiBudW1iZXIsXG4gICAgcmVzcEhlYWRlcnM/OiBQcmVTaWduUmVxdWVzdFBhcmFtcyB8IERhdGUsXG4gICAgcmVxdWVzdERhdGU/OiBEYXRlLFxuICApOiBQcm9taXNlPHN0cmluZz4ge1xuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcignSW52YWxpZCBidWNrZXQgbmFtZTogJyArIGJ1Y2tldE5hbWUpXG4gICAgfVxuICAgIGlmICghaXNWYWxpZE9iamVjdE5hbWUob2JqZWN0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZE9iamVjdE5hbWVFcnJvcihgSW52YWxpZCBvYmplY3QgbmFtZTogJHtvYmplY3ROYW1lfWApXG4gICAgfVxuXG4gICAgY29uc3QgdmFsaWRSZXNwSGVhZGVycyA9IFtcbiAgICAgICdyZXNwb25zZS1jb250ZW50LXR5cGUnLFxuICAgICAgJ3Jlc3BvbnNlLWNvbnRlbnQtbGFuZ3VhZ2UnLFxuICAgICAgJ3Jlc3BvbnNlLWV4cGlyZXMnLFxuICAgICAgJ3Jlc3BvbnNlLWNhY2hlLWNvbnRyb2wnLFxuICAgICAgJ3Jlc3BvbnNlLWNvbnRlbnQtZGlzcG9zaXRpb24nLFxuICAgICAgJ3Jlc3BvbnNlLWNvbnRlbnQtZW5jb2RpbmcnLFxuICAgIF1cbiAgICB2YWxpZFJlc3BIZWFkZXJzLmZvckVhY2goKGhlYWRlcikgPT4ge1xuICAgICAgLy8gQHRzLWlnbm9yZVxuICAgICAgaWYgKHJlc3BIZWFkZXJzICE9PSB1bmRlZmluZWQgJiYgcmVzcEhlYWRlcnNbaGVhZGVyXSAhPT0gdW5kZWZpbmVkICYmICFpc1N0cmluZyhyZXNwSGVhZGVyc1toZWFkZXJdKSkge1xuICAgICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKGByZXNwb25zZSBoZWFkZXIgJHtoZWFkZXJ9IHNob3VsZCBiZSBvZiB0eXBlIFwic3RyaW5nXCJgKVxuICAgICAgfVxuICAgIH0pXG4gICAgcmV0dXJuIHRoaXMucHJlc2lnbmVkVXJsKCdHRVQnLCBidWNrZXROYW1lLCBvYmplY3ROYW1lLCBleHBpcmVzLCByZXNwSGVhZGVycywgcmVxdWVzdERhdGUpXG4gIH1cblxuICBhc3luYyBwcmVzaWduZWRQdXRPYmplY3QoYnVja2V0TmFtZTogc3RyaW5nLCBvYmplY3ROYW1lOiBzdHJpbmcsIGV4cGlyZXM/OiBudW1iZXIpOiBQcm9taXNlPHN0cmluZz4ge1xuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcihgSW52YWxpZCBidWNrZXQgbmFtZTogJHtidWNrZXROYW1lfWApXG4gICAgfVxuICAgIGlmICghaXNWYWxpZE9iamVjdE5hbWUob2JqZWN0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZE9iamVjdE5hbWVFcnJvcihgSW52YWxpZCBvYmplY3QgbmFtZTogJHtvYmplY3ROYW1lfWApXG4gICAgfVxuXG4gICAgcmV0dXJuIHRoaXMucHJlc2lnbmVkVXJsKCdQVVQnLCBidWNrZXROYW1lLCBvYmplY3ROYW1lLCBleHBpcmVzKVxuICB9XG5cbiAgbmV3UG9zdFBvbGljeSgpOiBQb3N0UG9saWN5IHtcbiAgICByZXR1cm4gbmV3IFBvc3RQb2xpY3koKVxuICB9XG5cbiAgYXN5bmMgcHJlc2lnbmVkUG9zdFBvbGljeShwb3N0UG9saWN5OiBQb3N0UG9saWN5KTogUHJvbWlzZTxQb3N0UG9saWN5UmVzdWx0PiB7XG4gICAgaWYgKHRoaXMuYW5vbnltb3VzKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkFub255bW91c1JlcXVlc3RFcnJvcignUHJlc2lnbmVkIFBPU1QgcG9saWN5IGNhbm5vdCBiZSBnZW5lcmF0ZWQgZm9yIGFub255bW91cyByZXF1ZXN0cycpXG4gICAgfVxuICAgIGlmICghaXNPYmplY3QocG9zdFBvbGljeSkpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ3Bvc3RQb2xpY3kgc2hvdWxkIGJlIG9mIHR5cGUgXCJvYmplY3RcIicpXG4gICAgfVxuICAgIGNvbnN0IGJ1Y2tldE5hbWUgPSBwb3N0UG9saWN5LmZvcm1EYXRhLmJ1Y2tldCBhcyBzdHJpbmdcbiAgICB0cnkge1xuICAgICAgY29uc3QgcmVnaW9uID0gYXdhaXQgdGhpcy5nZXRCdWNrZXRSZWdpb25Bc3luYyhidWNrZXROYW1lKVxuXG4gICAgICBjb25zdCBkYXRlID0gbmV3IERhdGUoKVxuICAgICAgY29uc3QgZGF0ZVN0ciA9IG1ha2VEYXRlTG9uZyhkYXRlKVxuICAgICAgYXdhaXQgdGhpcy5jaGVja0FuZFJlZnJlc2hDcmVkcygpXG5cbiAgICAgIGlmICghcG9zdFBvbGljeS5wb2xpY3kuZXhwaXJhdGlvbikge1xuICAgICAgICAvLyAnZXhwaXJhdGlvbicgaXMgbWFuZGF0b3J5IGZpZWxkIGZvciBTMy5cbiAgICAgICAgLy8gU2V0IGRlZmF1bHQgZXhwaXJhdGlvbiBkYXRlIG9mIDcgZGF5cy5cbiAgICAgICAgY29uc3QgZXhwaXJlcyA9IG5ldyBEYXRlKClcbiAgICAgICAgZXhwaXJlcy5zZXRTZWNvbmRzKFBSRVNJR05fRVhQSVJZX0RBWVNfTUFYKVxuICAgICAgICBwb3N0UG9saWN5LnNldEV4cGlyZXMoZXhwaXJlcylcbiAgICAgIH1cblxuICAgICAgcG9zdFBvbGljeS5wb2xpY3kuY29uZGl0aW9ucy5wdXNoKFsnZXEnLCAnJHgtYW16LWRhdGUnLCBkYXRlU3RyXSlcbiAgICAgIHBvc3RQb2xpY3kuZm9ybURhdGFbJ3gtYW16LWRhdGUnXSA9IGRhdGVTdHJcblxuICAgICAgcG9zdFBvbGljeS5wb2xpY3kuY29uZGl0aW9ucy5wdXNoKFsnZXEnLCAnJHgtYW16LWFsZ29yaXRobScsICdBV1M0LUhNQUMtU0hBMjU2J10pXG4gICAgICBwb3N0UG9saWN5LmZvcm1EYXRhWyd4LWFtei1hbGdvcml0aG0nXSA9ICdBV1M0LUhNQUMtU0hBMjU2J1xuXG4gICAgICBwb3N0UG9saWN5LnBvbGljeS5jb25kaXRpb25zLnB1c2goWydlcScsICckeC1hbXotY3JlZGVudGlhbCcsIHRoaXMuYWNjZXNzS2V5ICsgJy8nICsgZ2V0U2NvcGUocmVnaW9uLCBkYXRlKV0pXG4gICAgICBwb3N0UG9saWN5LmZvcm1EYXRhWyd4LWFtei1jcmVkZW50aWFsJ10gPSB0aGlzLmFjY2Vzc0tleSArICcvJyArIGdldFNjb3BlKHJlZ2lvbiwgZGF0ZSlcblxuICAgICAgaWYgKHRoaXMuc2Vzc2lvblRva2VuKSB7XG4gICAgICAgIHBvc3RQb2xpY3kucG9saWN5LmNvbmRpdGlvbnMucHVzaChbJ2VxJywgJyR4LWFtei1zZWN1cml0eS10b2tlbicsIHRoaXMuc2Vzc2lvblRva2VuXSlcbiAgICAgICAgcG9zdFBvbGljeS5mb3JtRGF0YVsneC1hbXotc2VjdXJpdHktdG9rZW4nXSA9IHRoaXMuc2Vzc2lvblRva2VuXG4gICAgICB9XG5cbiAgICAgIGNvbnN0IHBvbGljeUJhc2U2NCA9IEJ1ZmZlci5mcm9tKEpTT04uc3RyaW5naWZ5KHBvc3RQb2xpY3kucG9saWN5KSkudG9TdHJpbmcoJ2Jhc2U2NCcpXG5cbiAgICAgIHBvc3RQb2xpY3kuZm9ybURhdGEucG9saWN5ID0gcG9saWN5QmFzZTY0XG5cbiAgICAgIHBvc3RQb2xpY3kuZm9ybURhdGFbJ3gtYW16LXNpZ25hdHVyZSddID0gcG9zdFByZXNpZ25TaWduYXR1cmVWNChyZWdpb24sIGRhdGUsIHRoaXMuc2VjcmV0S2V5LCBwb2xpY3lCYXNlNjQpXG4gICAgICBjb25zdCBvcHRzID0ge1xuICAgICAgICByZWdpb246IHJlZ2lvbixcbiAgICAgICAgYnVja2V0TmFtZTogYnVja2V0TmFtZSxcbiAgICAgICAgbWV0aG9kOiAnUE9TVCcsXG4gICAgICB9XG4gICAgICBjb25zdCByZXFPcHRpb25zID0gdGhpcy5nZXRSZXF1ZXN0T3B0aW9ucyhvcHRzKVxuICAgICAgY29uc3QgcG9ydFN0ciA9IHRoaXMucG9ydCA9PSA4MCB8fCB0aGlzLnBvcnQgPT09IDQ0MyA/ICcnIDogYDoke3RoaXMucG9ydC50b1N0cmluZygpfWBcbiAgICAgIGNvbnN0IHVybFN0ciA9IGAke3JlcU9wdGlvbnMucHJvdG9jb2x9Ly8ke3JlcU9wdGlvbnMuaG9zdH0ke3BvcnRTdHJ9JHtyZXFPcHRpb25zLnBhdGh9YFxuICAgICAgcmV0dXJuIHsgcG9zdFVSTDogdXJsU3RyLCBmb3JtRGF0YTogcG9zdFBvbGljeS5mb3JtRGF0YSB9XG4gICAgfSBjYXRjaCAoZXJyKSB7XG4gICAgICBpZiAoZXJyIGluc3RhbmNlb2YgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IpIHtcbiAgICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQXJndW1lbnRFcnJvcihgVW5hYmxlIHRvIGdldCBidWNrZXQgcmVnaW9uIGZvciAke2J1Y2tldE5hbWV9LmApXG4gICAgICB9XG5cbiAgICAgIHRocm93IGVyclxuICAgIH1cbiAgfVxuICAvLyBsaXN0IGEgYmF0Y2ggb2Ygb2JqZWN0c1xuICBhc3luYyBsaXN0T2JqZWN0c1F1ZXJ5KGJ1Y2tldE5hbWU6IHN0cmluZywgcHJlZml4Pzogc3RyaW5nLCBtYXJrZXI/OiBzdHJpbmcsIGxpc3RRdWVyeU9wdHM/OiBMaXN0T2JqZWN0UXVlcnlPcHRzKSB7XG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZShidWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKCdJbnZhbGlkIGJ1Y2tldCBuYW1lOiAnICsgYnVja2V0TmFtZSlcbiAgICB9XG4gICAgaWYgKCFpc1N0cmluZyhwcmVmaXgpKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdwcmVmaXggc2hvdWxkIGJlIG9mIHR5cGUgXCJzdHJpbmdcIicpXG4gICAgfVxuICAgIGlmIChtYXJrZXIgJiYgIWlzU3RyaW5nKG1hcmtlcikpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ21hcmtlciBzaG91bGQgYmUgb2YgdHlwZSBcInN0cmluZ1wiJylcbiAgICB9XG5cbiAgICBpZiAobGlzdFF1ZXJ5T3B0cyAmJiAhaXNPYmplY3QobGlzdFF1ZXJ5T3B0cykpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ2xpc3RRdWVyeU9wdHMgc2hvdWxkIGJlIG9mIHR5cGUgXCJvYmplY3RcIicpXG4gICAgfVxuICAgIGxldCB7IERlbGltaXRlciwgTWF4S2V5cywgSW5jbHVkZVZlcnNpb24sIHZlcnNpb25JZE1hcmtlciwga2V5TWFya2VyIH0gPSBsaXN0UXVlcnlPcHRzIGFzIExpc3RPYmplY3RRdWVyeU9wdHNcblxuICAgIGlmICghaXNTdHJpbmcoRGVsaW1pdGVyKSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcignRGVsaW1pdGVyIHNob3VsZCBiZSBvZiB0eXBlIFwic3RyaW5nXCInKVxuICAgIH1cbiAgICBpZiAoIWlzTnVtYmVyKE1heEtleXMpKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdNYXhLZXlzIHNob3VsZCBiZSBvZiB0eXBlIFwibnVtYmVyXCInKVxuICAgIH1cblxuICAgIGNvbnN0IHF1ZXJpZXMgPSBbXVxuICAgIC8vIGVzY2FwZSBldmVyeSB2YWx1ZSBpbiBxdWVyeSBzdHJpbmcsIGV4Y2VwdCBtYXhLZXlzXG4gICAgcXVlcmllcy5wdXNoKGBwcmVmaXg9JHt1cmlFc2NhcGUocHJlZml4KX1gKVxuICAgIHF1ZXJpZXMucHVzaChgZGVsaW1pdGVyPSR7dXJpRXNjYXBlKERlbGltaXRlcil9YClcbiAgICBxdWVyaWVzLnB1c2goYGVuY29kaW5nLXR5cGU9dXJsYClcblxuICAgIGlmIChJbmNsdWRlVmVyc2lvbikge1xuICAgICAgcXVlcmllcy5wdXNoKGB2ZXJzaW9uc2ApXG4gICAgfVxuXG4gICAgaWYgKEluY2x1ZGVWZXJzaW9uKSB7XG4gICAgICAvLyB2MSB2ZXJzaW9uIGxpc3RpbmcuLlxuICAgICAgaWYgKGtleU1hcmtlcikge1xuICAgICAgICBxdWVyaWVzLnB1c2goYGtleS1tYXJrZXI9JHtrZXlNYXJrZXJ9YClcbiAgICAgIH1cbiAgICAgIGlmICh2ZXJzaW9uSWRNYXJrZXIpIHtcbiAgICAgICAgcXVlcmllcy5wdXNoKGB2ZXJzaW9uLWlkLW1hcmtlcj0ke3ZlcnNpb25JZE1hcmtlcn1gKVxuICAgICAgfVxuICAgIH0gZWxzZSBpZiAobWFya2VyKSB7XG4gICAgICBtYXJrZXIgPSB1cmlFc2NhcGUobWFya2VyKVxuICAgICAgcXVlcmllcy5wdXNoKGBtYXJrZXI9JHttYXJrZXJ9YClcbiAgICB9XG5cbiAgICAvLyBubyBuZWVkIHRvIGVzY2FwZSBtYXhLZXlzXG4gICAgaWYgKE1heEtleXMpIHtcbiAgICAgIGlmIChNYXhLZXlzID49IDEwMDApIHtcbiAgICAgICAgTWF4S2V5cyA9IDEwMDBcbiAgICAgIH1cbiAgICAgIHF1ZXJpZXMucHVzaChgbWF4LWtleXM9JHtNYXhLZXlzfWApXG4gICAgfVxuICAgIHF1ZXJpZXMuc29ydCgpXG4gICAgbGV0IHF1ZXJ5ID0gJydcbiAgICBpZiAocXVlcmllcy5sZW5ndGggPiAwKSB7XG4gICAgICBxdWVyeSA9IGAke3F1ZXJpZXMuam9pbignJicpfWBcbiAgICB9XG5cbiAgICBjb25zdCBtZXRob2QgPSAnR0VUJ1xuICAgIGNvbnN0IHJlcyA9IGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luYyh7IG1ldGhvZCwgYnVja2V0TmFtZSwgcXVlcnkgfSlcbiAgICBjb25zdCBib2R5ID0gYXdhaXQgcmVhZEFzU3RyaW5nKHJlcylcbiAgICBjb25zdCBsaXN0UXJ5TGlzdCA9IHBhcnNlTGlzdE9iamVjdHMoYm9keSlcbiAgICByZXR1cm4gbGlzdFFyeUxpc3RcbiAgfVxuXG4gIGxpc3RPYmplY3RzKFxuICAgIGJ1Y2tldE5hbWU6IHN0cmluZyxcbiAgICBwcmVmaXg/OiBzdHJpbmcsXG4gICAgcmVjdXJzaXZlPzogYm9vbGVhbixcbiAgICBsaXN0T3B0cz86IExpc3RPYmplY3RRdWVyeU9wdHMgfCB1bmRlZmluZWQsXG4gICk6IEJ1Y2tldFN0cmVhbTxPYmplY3RJbmZvPiB7XG4gICAgaWYgKHByZWZpeCA9PT0gdW5kZWZpbmVkKSB7XG4gICAgICBwcmVmaXggPSAnJ1xuICAgIH1cbiAgICBpZiAocmVjdXJzaXZlID09PSB1bmRlZmluZWQpIHtcbiAgICAgIHJlY3Vyc2l2ZSA9IGZhbHNlXG4gICAgfVxuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcignSW52YWxpZCBidWNrZXQgbmFtZTogJyArIGJ1Y2tldE5hbWUpXG4gICAgfVxuICAgIGlmICghaXNWYWxpZFByZWZpeChwcmVmaXgpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRQcmVmaXhFcnJvcihgSW52YWxpZCBwcmVmaXggOiAke3ByZWZpeH1gKVxuICAgIH1cbiAgICBpZiAoIWlzU3RyaW5nKHByZWZpeCkpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ3ByZWZpeCBzaG91bGQgYmUgb2YgdHlwZSBcInN0cmluZ1wiJylcbiAgICB9XG4gICAgaWYgKCFpc0Jvb2xlYW4ocmVjdXJzaXZlKSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcigncmVjdXJzaXZlIHNob3VsZCBiZSBvZiB0eXBlIFwiYm9vbGVhblwiJylcbiAgICB9XG4gICAgaWYgKGxpc3RPcHRzICYmICFpc09iamVjdChsaXN0T3B0cykpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ2xpc3RPcHRzIHNob3VsZCBiZSBvZiB0eXBlIFwib2JqZWN0XCInKVxuICAgIH1cbiAgICBsZXQgbWFya2VyOiBzdHJpbmcgfCB1bmRlZmluZWQgPSAnJ1xuICAgIGxldCBrZXlNYXJrZXI6IHN0cmluZyB8IHVuZGVmaW5lZCA9ICcnXG4gICAgbGV0IHZlcnNpb25JZE1hcmtlcjogc3RyaW5nIHwgdW5kZWZpbmVkID0gJydcbiAgICBsZXQgb2JqZWN0czogT2JqZWN0SW5mb1tdID0gW11cbiAgICBsZXQgZW5kZWQgPSBmYWxzZVxuICAgIGNvbnN0IHJlYWRTdHJlYW06IHN0cmVhbS5SZWFkYWJsZSA9IG5ldyBzdHJlYW0uUmVhZGFibGUoeyBvYmplY3RNb2RlOiB0cnVlIH0pXG4gICAgcmVhZFN0cmVhbS5fcmVhZCA9IGFzeW5jICgpID0+IHtcbiAgICAgIC8vIHB1c2ggb25lIG9iamVjdCBwZXIgX3JlYWQoKVxuICAgICAgaWYgKG9iamVjdHMubGVuZ3RoKSB7XG4gICAgICAgIHJlYWRTdHJlYW0ucHVzaChvYmplY3RzLnNoaWZ0KCkpXG4gICAgICAgIHJldHVyblxuICAgICAgfVxuICAgICAgaWYgKGVuZGVkKSB7XG4gICAgICAgIHJldHVybiByZWFkU3RyZWFtLnB1c2gobnVsbClcbiAgICAgIH1cblxuICAgICAgdHJ5IHtcbiAgICAgICAgY29uc3QgbGlzdFF1ZXJ5T3B0cyA9IHtcbiAgICAgICAgICBEZWxpbWl0ZXI6IHJlY3Vyc2l2ZSA/ICcnIDogJy8nLCAvLyBpZiByZWN1cnNpdmUgaXMgZmFsc2Ugc2V0IGRlbGltaXRlciB0byAnLydcbiAgICAgICAgICBNYXhLZXlzOiAxMDAwLFxuICAgICAgICAgIEluY2x1ZGVWZXJzaW9uOiBsaXN0T3B0cz8uSW5jbHVkZVZlcnNpb24sXG4gICAgICAgICAgLy8gdmVyc2lvbiBsaXN0aW5nIHNwZWNpZmljIG9wdGlvbnNcbiAgICAgICAgICBrZXlNYXJrZXI6IGtleU1hcmtlcixcbiAgICAgICAgICB2ZXJzaW9uSWRNYXJrZXI6IHZlcnNpb25JZE1hcmtlcixcbiAgICAgICAgfVxuXG4gICAgICAgIGNvbnN0IHJlc3VsdDogTGlzdE9iamVjdFF1ZXJ5UmVzID0gYXdhaXQgdGhpcy5saXN0T2JqZWN0c1F1ZXJ5KGJ1Y2tldE5hbWUsIHByZWZpeCwgbWFya2VyLCBsaXN0UXVlcnlPcHRzKVxuICAgICAgICBpZiAocmVzdWx0LmlzVHJ1bmNhdGVkKSB7XG4gICAgICAgICAgbWFya2VyID0gcmVzdWx0Lm5leHRNYXJrZXIgfHwgdW5kZWZpbmVkXG4gICAgICAgICAgaWYgKHJlc3VsdC5rZXlNYXJrZXIpIHtcbiAgICAgICAgICAgIGtleU1hcmtlciA9IHJlc3VsdC5rZXlNYXJrZXJcbiAgICAgICAgICB9XG4gICAgICAgICAgaWYgKHJlc3VsdC52ZXJzaW9uSWRNYXJrZXIpIHtcbiAgICAgICAgICAgIHZlcnNpb25JZE1hcmtlciA9IHJlc3VsdC52ZXJzaW9uSWRNYXJrZXJcbiAgICAgICAgICB9XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgZW5kZWQgPSB0cnVlXG4gICAgICAgIH1cbiAgICAgICAgaWYgKHJlc3VsdC5vYmplY3RzKSB7XG4gICAgICAgICAgb2JqZWN0cyA9IHJlc3VsdC5vYmplY3RzXG4gICAgICAgIH1cbiAgICAgICAgLy8gQHRzLWlnbm9yZVxuICAgICAgICByZWFkU3RyZWFtLl9yZWFkKClcbiAgICAgIH0gY2F0Y2ggKGVycikge1xuICAgICAgICByZWFkU3RyZWFtLmVtaXQoJ2Vycm9yJywgZXJyKVxuICAgICAgfVxuICAgIH1cbiAgICByZXR1cm4gcmVhZFN0cmVhbVxuICB9XG5cbiAgYXN5bmMgbGlzdE9iamVjdHNWMlF1ZXJ5KFxuICAgIGJ1Y2tldE5hbWU6IHN0cmluZyxcbiAgICBwcmVmaXg6IHN0cmluZyxcbiAgICBjb250aW51YXRpb25Ub2tlbjogc3RyaW5nLFxuICAgIGRlbGltaXRlcjogc3RyaW5nLFxuICAgIG1heEtleXM6IG51bWJlcixcbiAgICBzdGFydEFmdGVyOiBzdHJpbmcsXG4gICk6IFByb21pc2U8TGlzdE9iamVjdFYyUmVzPiB7XG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZShidWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKCdJbnZhbGlkIGJ1Y2tldCBuYW1lOiAnICsgYnVja2V0TmFtZSlcbiAgICB9XG4gICAgaWYgKCFpc1N0cmluZyhwcmVmaXgpKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdwcmVmaXggc2hvdWxkIGJlIG9mIHR5cGUgXCJzdHJpbmdcIicpXG4gICAgfVxuICAgIGlmICghaXNTdHJpbmcoY29udGludWF0aW9uVG9rZW4pKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdjb250aW51YXRpb25Ub2tlbiBzaG91bGQgYmUgb2YgdHlwZSBcInN0cmluZ1wiJylcbiAgICB9XG4gICAgaWYgKCFpc1N0cmluZyhkZWxpbWl0ZXIpKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdkZWxpbWl0ZXIgc2hvdWxkIGJlIG9mIHR5cGUgXCJzdHJpbmdcIicpXG4gICAgfVxuICAgIGlmICghaXNOdW1iZXIobWF4S2V5cykpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ21heEtleXMgc2hvdWxkIGJlIG9mIHR5cGUgXCJudW1iZXJcIicpXG4gICAgfVxuICAgIGlmICghaXNTdHJpbmcoc3RhcnRBZnRlcikpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ3N0YXJ0QWZ0ZXIgc2hvdWxkIGJlIG9mIHR5cGUgXCJzdHJpbmdcIicpXG4gICAgfVxuXG4gICAgY29uc3QgcXVlcmllcyA9IFtdXG4gICAgcXVlcmllcy5wdXNoKGBsaXN0LXR5cGU9MmApXG4gICAgcXVlcmllcy5wdXNoKGBlbmNvZGluZy10eXBlPXVybGApXG4gICAgcXVlcmllcy5wdXNoKGBwcmVmaXg9JHt1cmlFc2NhcGUocHJlZml4KX1gKVxuICAgIHF1ZXJpZXMucHVzaChgZGVsaW1pdGVyPSR7dXJpRXNjYXBlKGRlbGltaXRlcil9YClcblxuICAgIGlmIChjb250aW51YXRpb25Ub2tlbikge1xuICAgICAgcXVlcmllcy5wdXNoKGBjb250aW51YXRpb24tdG9rZW49JHt1cmlFc2NhcGUoY29udGludWF0aW9uVG9rZW4pfWApXG4gICAgfVxuICAgIGlmIChzdGFydEFmdGVyKSB7XG4gICAgICBxdWVyaWVzLnB1c2goYHN0YXJ0LWFmdGVyPSR7dXJpRXNjYXBlKHN0YXJ0QWZ0ZXIpfWApXG4gICAgfVxuICAgIGlmIChtYXhLZXlzKSB7XG4gICAgICBpZiAobWF4S2V5cyA+PSAxMDAwKSB7XG4gICAgICAgIG1heEtleXMgPSAxMDAwXG4gICAgICB9XG4gICAgICBxdWVyaWVzLnB1c2goYG1heC1rZXlzPSR7bWF4S2V5c31gKVxuICAgIH1cbiAgICBxdWVyaWVzLnNvcnQoKVxuICAgIGxldCBxdWVyeSA9ICcnXG4gICAgaWYgKHF1ZXJpZXMubGVuZ3RoID4gMCkge1xuICAgICAgcXVlcnkgPSBgJHtxdWVyaWVzLmpvaW4oJyYnKX1gXG4gICAgfVxuXG4gICAgY29uc3QgbWV0aG9kID0gJ0dFVCdcbiAgICBjb25zdCByZXMgPSBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0QXN5bmMoeyBtZXRob2QsIGJ1Y2tldE5hbWUsIHF1ZXJ5IH0pXG4gICAgY29uc3QgYm9keSA9IGF3YWl0IHJlYWRBc1N0cmluZyhyZXMpXG4gICAgcmV0dXJuIHBhcnNlTGlzdE9iamVjdHNWMihib2R5KVxuICB9XG5cbiAgbGlzdE9iamVjdHNWMihcbiAgICBidWNrZXROYW1lOiBzdHJpbmcsXG4gICAgcHJlZml4Pzogc3RyaW5nLFxuICAgIHJlY3Vyc2l2ZT86IGJvb2xlYW4sXG4gICAgc3RhcnRBZnRlcj86IHN0cmluZyxcbiAgKTogQnVja2V0U3RyZWFtPEJ1Y2tldEl0ZW0+IHtcbiAgICBpZiAocHJlZml4ID09PSB1bmRlZmluZWQpIHtcbiAgICAgIHByZWZpeCA9ICcnXG4gICAgfVxuICAgIGlmIChyZWN1cnNpdmUgPT09IHVuZGVmaW5lZCkge1xuICAgICAgcmVjdXJzaXZlID0gZmFsc2VcbiAgICB9XG4gICAgaWYgKHN0YXJ0QWZ0ZXIgPT09IHVuZGVmaW5lZCkge1xuICAgICAgc3RhcnRBZnRlciA9ICcnXG4gICAgfVxuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcignSW52YWxpZCBidWNrZXQgbmFtZTogJyArIGJ1Y2tldE5hbWUpXG4gICAgfVxuICAgIGlmICghaXNWYWxpZFByZWZpeChwcmVmaXgpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRQcmVmaXhFcnJvcihgSW52YWxpZCBwcmVmaXggOiAke3ByZWZpeH1gKVxuICAgIH1cbiAgICBpZiAoIWlzU3RyaW5nKHByZWZpeCkpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ3ByZWZpeCBzaG91bGQgYmUgb2YgdHlwZSBcInN0cmluZ1wiJylcbiAgICB9XG4gICAgaWYgKCFpc0Jvb2xlYW4ocmVjdXJzaXZlKSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcigncmVjdXJzaXZlIHNob3VsZCBiZSBvZiB0eXBlIFwiYm9vbGVhblwiJylcbiAgICB9XG4gICAgaWYgKCFpc1N0cmluZyhzdGFydEFmdGVyKSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcignc3RhcnRBZnRlciBzaG91bGQgYmUgb2YgdHlwZSBcInN0cmluZ1wiJylcbiAgICB9XG5cbiAgICBjb25zdCBkZWxpbWl0ZXIgPSByZWN1cnNpdmUgPyAnJyA6ICcvJ1xuICAgIGNvbnN0IHByZWZpeFN0ciA9IHByZWZpeFxuICAgIGNvbnN0IHN0YXJ0QWZ0ZXJTdHIgPSBzdGFydEFmdGVyXG4gICAgbGV0IGNvbnRpbnVhdGlvblRva2VuID0gJydcbiAgICBsZXQgb2JqZWN0czogQnVja2V0SXRlbVtdID0gW11cbiAgICBsZXQgZW5kZWQgPSBmYWxzZVxuICAgIGNvbnN0IHJlYWRTdHJlYW06IHN0cmVhbS5SZWFkYWJsZSA9IG5ldyBzdHJlYW0uUmVhZGFibGUoeyBvYmplY3RNb2RlOiB0cnVlIH0pXG4gICAgcmVhZFN0cmVhbS5fcmVhZCA9IGFzeW5jICgpID0+IHtcbiAgICAgIGlmIChvYmplY3RzLmxlbmd0aCkge1xuICAgICAgICByZWFkU3RyZWFtLnB1c2gob2JqZWN0cy5zaGlmdCgpKVxuICAgICAgICByZXR1cm5cbiAgICAgIH1cbiAgICAgIGlmIChlbmRlZCkge1xuICAgICAgICByZXR1cm4gcmVhZFN0cmVhbS5wdXNoKG51bGwpXG4gICAgICB9XG5cbiAgICAgIHRyeSB7XG4gICAgICAgIGNvbnN0IHJlc3VsdCA9IGF3YWl0IHRoaXMubGlzdE9iamVjdHNWMlF1ZXJ5KFxuICAgICAgICAgIGJ1Y2tldE5hbWUsXG4gICAgICAgICAgcHJlZml4U3RyLFxuICAgICAgICAgIGNvbnRpbnVhdGlvblRva2VuLFxuICAgICAgICAgIGRlbGltaXRlcixcbiAgICAgICAgICAxMDAwLFxuICAgICAgICAgIHN0YXJ0QWZ0ZXJTdHIsXG4gICAgICAgIClcbiAgICAgICAgaWYgKHJlc3VsdC5pc1RydW5jYXRlZCkge1xuICAgICAgICAgIGNvbnRpbnVhdGlvblRva2VuID0gcmVzdWx0Lm5leHRDb250aW51YXRpb25Ub2tlblxuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIGVuZGVkID0gdHJ1ZVxuICAgICAgICB9XG4gICAgICAgIG9iamVjdHMgPSByZXN1bHQub2JqZWN0c1xuICAgICAgICAvLyBAdHMtaWdub3JlXG4gICAgICAgIHJlYWRTdHJlYW0uX3JlYWQoKVxuICAgICAgfSBjYXRjaCAoZXJyKSB7XG4gICAgICAgIHJlYWRTdHJlYW0uZW1pdCgnZXJyb3InLCBlcnIpXG4gICAgICB9XG4gICAgfVxuICAgIHJldHVybiByZWFkU3RyZWFtXG4gIH1cblxuICBhc3luYyBzZXRCdWNrZXROb3RpZmljYXRpb24oYnVja2V0TmFtZTogc3RyaW5nLCBjb25maWc6IE5vdGlmaWNhdGlvbkNvbmZpZyk6IFByb21pc2U8dm9pZD4ge1xuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcignSW52YWxpZCBidWNrZXQgbmFtZTogJyArIGJ1Y2tldE5hbWUpXG4gICAgfVxuICAgIGlmICghaXNPYmplY3QoY29uZmlnKSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcignbm90aWZpY2F0aW9uIGNvbmZpZyBzaG91bGQgYmUgb2YgdHlwZSBcIk9iamVjdFwiJylcbiAgICB9XG4gICAgY29uc3QgbWV0aG9kID0gJ1BVVCdcbiAgICBjb25zdCBxdWVyeSA9ICdub3RpZmljYXRpb24nXG4gICAgY29uc3QgYnVpbGRlciA9IG5ldyB4bWwyanMuQnVpbGRlcih7XG4gICAgICByb290TmFtZTogJ05vdGlmaWNhdGlvbkNvbmZpZ3VyYXRpb24nLFxuICAgICAgcmVuZGVyT3B0czogeyBwcmV0dHk6IGZhbHNlIH0sXG4gICAgICBoZWFkbGVzczogdHJ1ZSxcbiAgICB9KVxuICAgIGNvbnN0IHBheWxvYWQgPSBidWlsZGVyLmJ1aWxkT2JqZWN0KGNvbmZpZylcbiAgICBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0QXN5bmNPbWl0KHsgbWV0aG9kLCBidWNrZXROYW1lLCBxdWVyeSB9LCBwYXlsb2FkKVxuICB9XG5cbiAgYXN5bmMgcmVtb3ZlQWxsQnVja2V0Tm90aWZpY2F0aW9uKGJ1Y2tldE5hbWU6IHN0cmluZyk6IFByb21pc2U8dm9pZD4ge1xuICAgIGF3YWl0IHRoaXMuc2V0QnVja2V0Tm90aWZpY2F0aW9uKGJ1Y2tldE5hbWUsIG5ldyBOb3RpZmljYXRpb25Db25maWcoKSlcbiAgfVxuXG4gIGFzeW5jIGdldEJ1Y2tldE5vdGlmaWNhdGlvbihidWNrZXROYW1lOiBzdHJpbmcpOiBQcm9taXNlPE5vdGlmaWNhdGlvbkNvbmZpZ1Jlc3VsdD4ge1xuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcignSW52YWxpZCBidWNrZXQgbmFtZTogJyArIGJ1Y2tldE5hbWUpXG4gICAgfVxuICAgIGNvbnN0IG1ldGhvZCA9ICdHRVQnXG4gICAgY29uc3QgcXVlcnkgPSAnbm90aWZpY2F0aW9uJ1xuICAgIGNvbnN0IHJlcyA9IGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luYyh7IG1ldGhvZCwgYnVja2V0TmFtZSwgcXVlcnkgfSlcbiAgICBjb25zdCBib2R5ID0gYXdhaXQgcmVhZEFzU3RyaW5nKHJlcylcbiAgICByZXR1cm4gcGFyc2VCdWNrZXROb3RpZmljYXRpb24oYm9keSlcbiAgfVxuXG4gIGxpc3RlbkJ1Y2tldE5vdGlmaWNhdGlvbihcbiAgICBidWNrZXROYW1lOiBzdHJpbmcsXG4gICAgcHJlZml4OiBzdHJpbmcsXG4gICAgc3VmZml4OiBzdHJpbmcsXG4gICAgZXZlbnRzOiBOb3RpZmljYXRpb25FdmVudFtdLFxuICApOiBOb3RpZmljYXRpb25Qb2xsZXIge1xuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcihgSW52YWxpZCBidWNrZXQgbmFtZTogJHtidWNrZXROYW1lfWApXG4gICAgfVxuICAgIGlmICghaXNTdHJpbmcocHJlZml4KSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcigncHJlZml4IG11c3QgYmUgb2YgdHlwZSBzdHJpbmcnKVxuICAgIH1cbiAgICBpZiAoIWlzU3RyaW5nKHN1ZmZpeCkpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ3N1ZmZpeCBtdXN0IGJlIG9mIHR5cGUgc3RyaW5nJylcbiAgICB9XG4gICAgaWYgKCFBcnJheS5pc0FycmF5KGV2ZW50cykpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ2V2ZW50cyBtdXN0IGJlIG9mIHR5cGUgQXJyYXknKVxuICAgIH1cbiAgICBjb25zdCBsaXN0ZW5lciA9IG5ldyBOb3RpZmljYXRpb25Qb2xsZXIodGhpcywgYnVja2V0TmFtZSwgcHJlZml4LCBzdWZmaXgsIGV2ZW50cylcbiAgICBsaXN0ZW5lci5zdGFydCgpXG4gICAgcmV0dXJuIGxpc3RlbmVyXG4gIH1cbn1cbiJdLCJtYXBwaW5ncyI6IkFBQUEsT0FB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