"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
var crypto = _interopRequireWildcard(require("crypto"), true);
var fs = _interopRequireWildcard(require("fs"), true);
var http = _interopRequireWildcard(require("http"), true);
var https = _interopRequireWildcard(require("https"), true);
var path = _interopRequireWildcard(require("path"), true);
var stream = _interopRequireWildcard(require("stream"), true);
var async = _interopRequireWildcard(require("async"), true);
var _blockStream = require("block-stream2");
var _browserOrNode = require("browser-or-node");
var _lodash = require("lodash");
var _queryString = require("query-string");
var _xml2js = require("xml2js");
var _CredentialProvider = require("../CredentialProvider.js");
var errors = _interopRequireWildcard(require("../errors.js"), true);
var _helpers = require("../helpers.js");
var _notification = require("../notification.js");
var _signing = require("../signing.js");
var _async2 = require("./async.js");
var _copyConditions = require("./copy-conditions.js");
var _extensions = require("./extensions.js");
var _helper = require("./helper.js");
var _joinHostPort = require("./join-host-port.js");
var _postPolicy = require("./post-policy.js");
var _request = require("./request.js");
var _response = require("./response.js");
var _s3Endpoints = require("./s3-endpoints.js");
var _xmlParser = _interopRequireWildcard(require("./xml-parser.js"), true);
var xmlParsers = _xmlParser;
function _interopRequireWildcard(e, t) { if ("function" == typeof WeakMap) var r = new WeakMap(), n = new WeakMap(); return (_interopRequireWildcard = function (e, t) { if (!t && e && e.__esModule) return e; var o, i, f = { __proto__: null, default: e }; if (null === e || "object" != typeof e && "function" != typeof e) return f; if (o = t ? n : r) { if (o.has(e)) return o.get(e); o.set(e, f); } for (const t in e) "default" !== t && {}.hasOwnProperty.call(e, t) && ((i = (o = Object.defineProperty) && Object.getOwnPropertyDescriptor(e, t)) && (i.get || i.set) ? o(f, t, i) : f[t] = e[t]); return f; })(e, t); }
const xml = new _xml2js.Builder({
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
class TypedClient {
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
    if (!(0, _helper.isValidEndpoint)(params.endPoint)) {
      throw new errors.InvalidEndpointError(`Invalid endPoint : ${params.endPoint}`);
    }
    if (!(0, _helper.isValidPort)(params.port)) {
      throw new errors.InvalidArgumentError(`Invalid port : ${params.port}`);
    }
    if (!(0, _helper.isBoolean)(params.useSSL)) {
      throw new errors.InvalidArgumentError(`Invalid useSSL flag type : ${params.useSSL}, expected to be of type "boolean"`);
    }

    // Validate region only if its set.
    if (params.region) {
      if (!(0, _helper.isString)(params.region)) {
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
      if (!(0, _helper.isObject)(params.transport)) {
        throw new errors.InvalidArgumentError(`Invalid transport type : ${params.transport}, expected to be type "object"`);
      }
      transport = params.transport;
    }

    // if custom transport agent is set, use it.
    if (params.transportAgent) {
      if (!(0, _helper.isObject)(params.transportAgent)) {
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
    this.clientExtensions = new _extensions.Extensions(this);
    if (params.retryOptions) {
      if (!(0, _helper.isObject)(params.retryOptions)) {
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
    if (!(0, _helper.isObject)(options)) {
      throw new TypeError('request options should be of type "object"');
    }
    this.reqOptions = _lodash.pick(options, requestOptionProperties);
  }

  /**
   *  This is s3 Specific and does not hold validity in any other Object storage.
   */
  getAccelerateEndPointIfSet(bucketName, objectName) {
    if (!(0, _helper.isEmpty)(this.s3AccelerateEndpoint) && !(0, _helper.isEmpty)(bucketName) && !(0, _helper.isEmpty)(objectName)) {
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
    if (!(0, _helper.isString)(appName)) {
      throw new TypeError(`Invalid appName: ${appName}`);
    }
    if (appName.trim() === '') {
      throw new errors.InvalidArgumentError('Input appName cannot be empty.');
    }
    if (!(0, _helper.isString)(appVersion)) {
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
      virtualHostStyle = (0, _helper.isVirtualHostStyle)(this.host, this.protocol, bucketName, this.pathStyle);
    }
    let path = '/';
    let host = this.host;
    let port;
    if (this.port) {
      port = this.port;
    }
    if (objectName) {
      objectName = (0, _helper.uriResourceEscape)(objectName);
    }

    // For Amazon S3 endpoint, get endpoint based on region.
    if ((0, _helper.isAmazonEndpoint)(host)) {
      const accelerateEndPoint = this.getAccelerateEndPointIfSet(bucketName, objectName);
      if (accelerateEndPoint) {
        host = `${accelerateEndPoint}`;
      } else {
        host = (0, _s3Endpoints.getS3Endpoint)(region);
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
      reqOptions.headers.host = (0, _joinHostPort.joinHostPort)(host, port);
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
      headers: _lodash.mapValues(_lodash.pickBy(reqOptions.headers, _helper.isDefined), v => v.toString()),
      host,
      port,
      path
    };
  }
  async setCredentialsProvider(credentialsProvider) {
    if (!(credentialsProvider instanceof _CredentialProvider.CredentialProvider)) {
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
    if (!(0, _helper.isObject)(reqOptions)) {
      throw new TypeError('reqOptions should be of type "object"');
    }
    if (response && !(0, _helper.isReadableStream)(response)) {
      throw new TypeError('response should be of type "Stream"');
    }
    if (err && !(err instanceof Error)) {
      throw new TypeError('err should be of type "Error"');
    }
    const logStream = this.logStream;
    const logHeaders = headers => {
      Object.entries(headers).forEach(([k, v]) => {
        if (k == 'authorization') {
          if ((0, _helper.isString)(v)) {
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
    if (!(0, _helper.isObject)(options)) {
      throw new TypeError('options should be of type "object"');
    }
    if (!(0, _helper.isString)(payload) && !(0, _helper.isObject)(payload)) {
      // Buffer is of type 'object'
      throw new TypeError('payload should be of type "string" or "Buffer"');
    }
    expectedCodes.forEach(statusCode => {
      if (!(0, _helper.isNumber)(statusCode)) {
        throw new TypeError('statusCode should be of type "number"');
      }
    });
    if (!(0, _helper.isString)(region)) {
      throw new TypeError('region should be of type "string"');
    }
    if (!options.headers) {
      options.headers = {};
    }
    if (options.method === 'POST' || options.method === 'PUT' || options.method === 'DELETE') {
      options.headers['content-length'] = payload.length.toString();
    }
    const sha256sum = this.enableSHA256 ? (0, _helper.toSha256)(payload) : '';
    return this.makeRequestStreamAsync(options, payload, sha256sum, expectedCodes, region);
  }

  /**
   * new request with promise
   *
   * No need to drain response, response body is not valid
   */
  async makeRequestAsyncOmit(options, payload = '', statusCodes = [200], region = '') {
    const res = await this.makeRequestAsync(options, payload, statusCodes, region);
    await (0, _response.drainResponse)(res);
    return res;
  }

  /**
   * makeRequestStream will be used directly instead of makeRequest in case the payload
   * is available as a stream. for ex. putObject
   *
   * @internal
   */
  async makeRequestStreamAsync(options, body, sha256sum, statusCodes, region) {
    if (!(0, _helper.isObject)(options)) {
      throw new TypeError('options should be of type "object"');
    }
    if (!(Buffer.isBuffer(body) || typeof body === 'string' || (0, _helper.isReadableStream)(body))) {
      throw new errors.InvalidArgumentError(`stream should be a Buffer, string or readable Stream, got ${typeof body} instead`);
    }
    if (!(0, _helper.isString)(sha256sum)) {
      throw new TypeError('sha256sum should be of type "string"');
    }
    statusCodes.forEach(statusCode => {
      if (!(0, _helper.isNumber)(statusCode)) {
        throw new TypeError('statusCode should be of type "number"');
      }
    });
    if (!(0, _helper.isString)(region)) {
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
      reqOptions.headers['x-amz-date'] = (0, _helper.makeDateLong)(date);
      reqOptions.headers['x-amz-content-sha256'] = sha256sum;
      if (this.sessionToken) {
        reqOptions.headers['x-amz-security-token'] = this.sessionToken;
      }
      reqOptions.headers.authorization = (0, _signing.signV4)(reqOptions, this.accessKey, this.secretKey, region, date, sha256sum);
    }
    const logStream = this.logStream;
    const log = logStream ? msg => logStream.write(`${msg}\n`) : () => {};
    const response = await (0, _request.requestWithRetry)(this.transport, reqOptions, body, this.retryOptions.disableRetry === true ? 0 : this.retryOptions.maximumRetryCount, this.retryOptions.baseDelayMs, this.retryOptions.maximumDelayMs, log);
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
    if (!(0, _helper.isValidBucketName)(bucketName)) {
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
      const body = await (0, _response.readAsString)(response);
      const region = xmlParsers.parseBucketRegion(body) || _helpers.DEFAULT_REGION;
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
    const pathStyle = this.pathStyle && !_browserOrNode.isBrowser;
    let region;
    try {
      const res = await this.makeRequestAsync({
        method,
        bucketName,
        query,
        pathStyle
      }, '', [200], _helpers.DEFAULT_REGION);
      return extractRegionAsync(res);
    } catch (e) {
      // make alignment with mc cli
      if (e instanceof errors.S3Error) {
        const errCode = e.code;
        const errRegion = e.region;
        if (errCode === 'AccessDenied' && !errRegion) {
          return _helpers.DEFAULT_REGION;
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
        await (0, _response.drainResponse)(res);
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
    if (!(0, _helper.isValidBucketName)(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    // Backward Compatibility
    if ((0, _helper.isObject)(region)) {
      makeOpts = region;
      region = '';
    }
    if (!(0, _helper.isString)(region)) {
      throw new TypeError('region should be of type "string"');
    }
    if (makeOpts && !(0, _helper.isObject)(makeOpts)) {
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
    if (region && region !== _helpers.DEFAULT_REGION) {
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
    const finalRegion = this.region || region || _helpers.DEFAULT_REGION;
    const requestOpt = {
      method,
      bucketName,
      headers
    };
    try {
      await this.makeRequestAsyncOmit(requestOpt, payload, [200], finalRegion);
    } catch (err) {
      if (region === '' || region === _helpers.DEFAULT_REGION) {
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
    if (!(0, _helper.isValidBucketName)(bucketName)) {
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
    if (!(0, _helper.isValidBucketName)(bucketName)) {
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
    if (!(0, _helper.isValidBucketName)(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!(0, _helper.isValidObjectName)(objectName)) {
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
    if (!(0, _helper.isValidBucketName)(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!(0, _helper.isValidObjectName)(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`);
    }
    if (!(0, _helper.isNumber)(offset)) {
      throw new TypeError('offset should be of type "number"');
    }
    if (!(0, _helper.isNumber)(length)) {
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
      query = _queryString.stringify(getOpts);
      headers = {
        ...(0, _helper.prependXAMZMeta)(sseHeaders),
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
    if (!(0, _helper.isValidBucketName)(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!(0, _helper.isValidObjectName)(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`);
    }
    if (!(0, _helper.isString)(filePath)) {
      throw new TypeError('filePath should be of type "string"');
    }
    const downloadToTmpFile = async () => {
      let partFileStream;
      const objStat = await this.statObject(bucketName, objectName, getOpts);
      const encodedEtag = Buffer.from(objStat.etag).toString('base64');
      const partFile = `${filePath}.${encodedEtag}.part.minio`;
      await _async2.fsp.mkdir(path.dirname(filePath), {
        recursive: true
      });
      let offset = 0;
      try {
        const stats = await _async2.fsp.stat(partFile);
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
      await _async2.streamPromise.pipeline(downloadStream, partFileStream);
      const stats = await _async2.fsp.stat(partFile);
      if (stats.size === objStat.size) {
        return partFile;
      }
      throw new Error('Size mismatch between downloaded file and the object');
    };
    const partFile = await downloadToTmpFile();
    await _async2.fsp.rename(partFile, filePath);
  }

  /**
   * Stat information of the object.
   */
  async statObject(bucketName, objectName, statOpts) {
    const statOptDef = statOpts || {};
    if (!(0, _helper.isValidBucketName)(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!(0, _helper.isValidObjectName)(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`);
    }
    if (!(0, _helper.isObject)(statOptDef)) {
      throw new errors.InvalidArgumentError('statOpts should be of type "object"');
    }
    const query = _queryString.stringify(statOptDef);
    const method = 'HEAD';
    const res = await this.makeRequestAsyncOmit({
      method,
      bucketName,
      objectName,
      query
    });
    return {
      size: parseInt(res.headers['content-length']),
      metaData: (0, _helper.extractMetadata)(res.headers),
      lastModified: new Date(res.headers['last-modified']),
      versionId: (0, _helper.getVersionId)(res.headers),
      etag: (0, _helper.sanitizeETag)(res.headers.etag)
    };
  }
  async removeObject(bucketName, objectName, removeOpts) {
    if (!(0, _helper.isValidBucketName)(bucketName)) {
      throw new errors.InvalidBucketNameError(`Invalid bucket name: ${bucketName}`);
    }
    if (!(0, _helper.isValidObjectName)(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`);
    }
    if (removeOpts && !(0, _helper.isObject)(removeOpts)) {
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
    const query = _queryString.stringify(queryParams);
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
    if (!(0, _helper.isValidBucketName)(bucket)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucket);
    }
    if (!(0, _helper.isValidPrefix)(prefix)) {
      throw new errors.InvalidPrefixError(`Invalid prefix : ${prefix}`);
    }
    if (!(0, _helper.isBoolean)(recursive)) {
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
    if (!(0, _helper.isValidBucketName)(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!(0, _helper.isString)(prefix)) {
      throw new TypeError('prefix should be of type "string"');
    }
    if (!(0, _helper.isString)(keyMarker)) {
      throw new TypeError('keyMarker should be of type "string"');
    }
    if (!(0, _helper.isString)(uploadIdMarker)) {
      throw new TypeError('uploadIdMarker should be of type "string"');
    }
    if (!(0, _helper.isString)(delimiter)) {
      throw new TypeError('delimiter should be of type "string"');
    }
    const queries = [];
    queries.push(`prefix=${(0, _helper.uriEscape)(prefix)}`);
    queries.push(`delimiter=${(0, _helper.uriEscape)(delimiter)}`);
    if (keyMarker) {
      queries.push(`key-marker=${(0, _helper.uriEscape)(keyMarker)}`);
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
    const body = await (0, _response.readAsString)(res);
    return xmlParsers.parseListMultipart(body);
  }

  /**
   * Initiate a new multipart upload.
   * @internal
   */
  async initiateNewMultipartUpload(bucketName, objectName, headers) {
    if (!(0, _helper.isValidBucketName)(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!(0, _helper.isValidObjectName)(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`);
    }
    if (!(0, _helper.isObject)(headers)) {
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
    const body = await (0, _response.readAsBuffer)(res);
    return (0, _xmlParser.parseInitiateMultipart)(body.toString());
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
    if (!(0, _helper.isValidBucketName)(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!(0, _helper.isValidObjectName)(objectName)) {
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
    if (!(0, _helper.isValidBucketName)(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!(0, _helper.isValidObjectName)(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`);
    }
    if (!(0, _helper.isString)(uploadId)) {
      throw new TypeError('uploadId should be of type "string"');
    }
    if (!(0, _helper.isObject)(etags)) {
      throw new TypeError('etags should be of type "Array"');
    }
    if (!uploadId) {
      throw new errors.InvalidArgumentError('uploadId cannot be empty');
    }
    const method = 'POST';
    const query = `uploadId=${(0, _helper.uriEscape)(uploadId)}`;
    const builder = new _xml2js.Builder();
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
    const body = await (0, _response.readAsBuffer)(res);
    const result = (0, _xmlParser.parseCompleteMultipart)(body.toString());
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
      versionId: (0, _helper.getVersionId)(res.headers)
    };
  }

  /**
   * Get part-info of all parts of an incomplete upload specified by uploadId.
   */
  async listParts(bucketName, objectName, uploadId) {
    if (!(0, _helper.isValidBucketName)(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!(0, _helper.isValidObjectName)(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`);
    }
    if (!(0, _helper.isString)(uploadId)) {
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
    if (!(0, _helper.isValidBucketName)(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!(0, _helper.isValidObjectName)(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`);
    }
    if (!(0, _helper.isString)(uploadId)) {
      throw new TypeError('uploadId should be of type "string"');
    }
    if (!(0, _helper.isNumber)(marker)) {
      throw new TypeError('marker should be of type "number"');
    }
    if (!uploadId) {
      throw new errors.InvalidArgumentError('uploadId cannot be empty');
    }
    let query = `uploadId=${(0, _helper.uriEscape)(uploadId)}`;
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
    return xmlParsers.parseListParts(await (0, _response.readAsString)(res));
  }
  async listBuckets() {
    const method = 'GET';
    const regionConf = this.region || _helpers.DEFAULT_REGION;
    const httpRes = await this.makeRequestAsync({
      method
    }, '', [200], regionConf);
    const xmlResult = await (0, _response.readAsString)(httpRes);
    return xmlParsers.parseListBucket(xmlResult);
  }

  /**
   * Calculate part size given the object size. Part size will be atleast this.partSize
   */
  calculatePartSize(size) {
    if (!(0, _helper.isNumber)(size)) {
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
    if (!(0, _helper.isValidBucketName)(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!(0, _helper.isValidObjectName)(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`);
    }
    if (!(0, _helper.isString)(filePath)) {
      throw new TypeError('filePath should be of type "string"');
    }
    if (metaData && !(0, _helper.isObject)(metaData)) {
      throw new TypeError('metaData should be of type "object"');
    }

    // Inserts correct `content-type` attribute based on metaData and filePath
    metaData = (0, _helper.insertContentType)(metaData || {}, filePath);
    const stat = await _async2.fsp.stat(filePath);
    return await this.putObject(bucketName, objectName, fs.createReadStream(filePath), stat.size, metaData);
  }

  /**
   *  Uploading a stream, "Buffer" or "string".
   *  It's recommended to pass `size` argument with stream.
   */
  async putObject(bucketName, objectName, stream, size, metaData) {
    if (!(0, _helper.isValidBucketName)(bucketName)) {
      throw new errors.InvalidBucketNameError(`Invalid bucket name: ${bucketName}`);
    }
    if (!(0, _helper.isValidObjectName)(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`);
    }

    // We'll need to shift arguments to the left because of metaData
    // and size being optional.
    if ((0, _helper.isObject)(size)) {
      metaData = size;
    }
    // Ensures Metadata has appropriate prefix for A3 API
    const headers = (0, _helper.prependXAMZMeta)(metaData);
    if (typeof stream === 'string' || stream instanceof Buffer) {
      // Adapts the non-stream interface into a stream.
      size = stream.length;
      stream = (0, _helper.readableStream)(stream);
    } else if (!(0, _helper.isReadableStream)(stream)) {
      throw new TypeError('third argument should be of type "stream.Readable" or "Buffer" or "string"');
    }
    if ((0, _helper.isNumber)(size) && size < 0) {
      throw new errors.InvalidArgumentError(`size cannot be negative, given size: ${size}`);
    }

    // Get the part size and forward that to the BlockStream. Default to the
    // largest block size possible if necessary.
    if (!(0, _helper.isNumber)(size)) {
      size = this.maxObjectSize;
    }

    // Get the part size and forward that to the BlockStream. Default to the
    // largest block size possible if necessary.
    if (size === undefined) {
      const statSize = await (0, _helper.getContentLength)(stream);
      if (statSize !== null) {
        size = statSize;
      }
    }
    if (!(0, _helper.isNumber)(size)) {
      // Backward compatibility
      size = this.maxObjectSize;
    }
    if (size === 0) {
      return this.uploadBuffer(bucketName, objectName, headers, Buffer.from(''));
    }
    const partSize = this.calculatePartSize(size);
    if (typeof stream === 'string' || Buffer.isBuffer(stream) || size <= partSize) {
      const buf = (0, _helper.isReadableStream)(stream) ? await (0, _response.readAsBuffer)(stream) : Buffer.from(stream);
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
    } = (0, _helper.hashBinary)(buf, this.enableSHA256);
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
    await (0, _response.drainResponse)(res);
    return {
      etag: (0, _helper.sanitizeETag)(res.headers.etag),
      versionId: (0, _helper.getVersionId)(res.headers)
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
    if (!(0, _helper.isReadableStream)(body)) {
      throw new TypeError('third argument should be of type "stream.Readable" or "Buffer" or "string"');
    }
    const chunkier = new _blockStream({
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
          query: _queryString.stringify({
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
    if (!(0, _helper.isValidBucketName)(bucketName)) {
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
    if (!(0, _helper.isValidBucketName)(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!(0, _helper.isObject)(replicationConfig)) {
      throw new errors.InvalidArgumentError('replicationConfig should be of type "object"');
    } else {
      if (_lodash.isEmpty(replicationConfig.role)) {
        throw new errors.InvalidArgumentError('Role cannot be empty');
      } else if (replicationConfig.role && !(0, _helper.isString)(replicationConfig.role)) {
        throw new errors.InvalidArgumentError('Invalid value for role', replicationConfig.role);
      }
      if (_lodash.isEmpty(replicationConfig.rules)) {
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
    const builder = new _xml2js.Builder({
      renderOpts: {
        pretty: false
      },
      headless: true
    });
    const payload = builder.buildObject(replicationParamsConfig);
    headers['Content-MD5'] = (0, _helper.toMd5)(payload);
    await this.makeRequestAsyncOmit({
      method,
      bucketName,
      query,
      headers
    }, payload);
  }
  async getBucketReplication(bucketName) {
    if (!(0, _helper.isValidBucketName)(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    const method = 'GET';
    const query = 'replication';
    const httpRes = await this.makeRequestAsync({
      method,
      bucketName,
      query
    }, '', [200, 204]);
    const xmlResult = await (0, _response.readAsString)(httpRes);
    return xmlParsers.parseReplicationConfig(xmlResult);
  }
  async getObjectLegalHold(bucketName, objectName, getOpts) {
    if (!(0, _helper.isValidBucketName)(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!(0, _helper.isValidObjectName)(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`);
    }
    if (getOpts) {
      if (!(0, _helper.isObject)(getOpts)) {
        throw new TypeError('getOpts should be of type "Object"');
      } else if (Object.keys(getOpts).length > 0 && getOpts.versionId && !(0, _helper.isString)(getOpts.versionId)) {
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
    const strRes = await (0, _response.readAsString)(httpRes);
    return (0, _xmlParser.parseObjectLegalHoldConfig)(strRes);
  }
  async setObjectLegalHold(bucketName, objectName, setOpts = {
    status: _helpers.LEGAL_HOLD_STATUS.ENABLED
  }) {
    if (!(0, _helper.isValidBucketName)(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!(0, _helper.isValidObjectName)(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`);
    }
    if (!(0, _helper.isObject)(setOpts)) {
      throw new TypeError('setOpts should be of type "Object"');
    } else {
      if (![_helpers.LEGAL_HOLD_STATUS.ENABLED, _helpers.LEGAL_HOLD_STATUS.DISABLED].includes(setOpts === null || setOpts === void 0 ? void 0 : setOpts.status)) {
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
    const builder = new _xml2js.Builder({
      rootName: 'LegalHold',
      renderOpts: {
        pretty: false
      },
      headless: true
    });
    const payload = builder.buildObject(config);
    const headers = {};
    headers['Content-MD5'] = (0, _helper.toMd5)(payload);
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
    if (!(0, _helper.isValidBucketName)(bucketName)) {
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
    const body = await (0, _response.readAsString)(response);
    return xmlParsers.parseTagging(body);
  }

  /**
   *  Get the tags associated with a bucket OR an object
   */
  async getObjectTagging(bucketName, objectName, getOpts) {
    const method = 'GET';
    let query = 'tagging';
    if (!(0, _helper.isValidBucketName)(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!(0, _helper.isValidObjectName)(objectName)) {
      throw new errors.InvalidBucketNameError('Invalid object name: ' + objectName);
    }
    if (getOpts && !(0, _helper.isObject)(getOpts)) {
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
    const body = await (0, _response.readAsString)(response);
    return xmlParsers.parseTagging(body);
  }

  /**
   *  Set the policy on a bucket or an object prefix.
   */
  async setBucketPolicy(bucketName, policy) {
    // Validate arguments.
    if (!(0, _helper.isValidBucketName)(bucketName)) {
      throw new errors.InvalidBucketNameError(`Invalid bucket name: ${bucketName}`);
    }
    if (!(0, _helper.isString)(policy)) {
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
    if (!(0, _helper.isValidBucketName)(bucketName)) {
      throw new errors.InvalidBucketNameError(`Invalid bucket name: ${bucketName}`);
    }
    const method = 'GET';
    const query = 'policy';
    const res = await this.makeRequestAsync({
      method,
      bucketName,
      query
    });
    return await (0, _response.readAsString)(res);
  }
  async putObjectRetention(bucketName, objectName, retentionOpts = {}) {
    if (!(0, _helper.isValidBucketName)(bucketName)) {
      throw new errors.InvalidBucketNameError(`Invalid bucket name: ${bucketName}`);
    }
    if (!(0, _helper.isValidObjectName)(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`);
    }
    if (!(0, _helper.isObject)(retentionOpts)) {
      throw new errors.InvalidArgumentError('retentionOpts should be of type "object"');
    } else {
      if (retentionOpts.governanceBypass && !(0, _helper.isBoolean)(retentionOpts.governanceBypass)) {
        throw new errors.InvalidArgumentError(`Invalid value for governanceBypass: ${retentionOpts.governanceBypass}`);
      }
      if (retentionOpts.mode && ![_helpers.RETENTION_MODES.COMPLIANCE, _helpers.RETENTION_MODES.GOVERNANCE].includes(retentionOpts.mode)) {
        throw new errors.InvalidArgumentError(`Invalid object retention mode: ${retentionOpts.mode}`);
      }
      if (retentionOpts.retainUntilDate && !(0, _helper.isString)(retentionOpts.retainUntilDate)) {
        throw new errors.InvalidArgumentError(`Invalid value for retainUntilDate: ${retentionOpts.retainUntilDate}`);
      }
      if (retentionOpts.versionId && !(0, _helper.isString)(retentionOpts.versionId)) {
        throw new errors.InvalidArgumentError(`Invalid value for versionId: ${retentionOpts.versionId}`);
      }
    }
    const method = 'PUT';
    let query = 'retention';
    const headers = {};
    if (retentionOpts.governanceBypass) {
      headers['X-Amz-Bypass-Governance-Retention'] = true;
    }
    const builder = new _xml2js.Builder({
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
    headers['Content-MD5'] = (0, _helper.toMd5)(payload);
    await this.makeRequestAsyncOmit({
      method,
      bucketName,
      objectName,
      query,
      headers
    }, payload, [200, 204]);
  }
  async getObjectLockConfig(bucketName) {
    if (!(0, _helper.isValidBucketName)(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    const method = 'GET';
    const query = 'object-lock';
    const httpRes = await this.makeRequestAsync({
      method,
      bucketName,
      query
    });
    const xmlResult = await (0, _response.readAsString)(httpRes);
    return xmlParsers.parseObjectLockConfig(xmlResult);
  }
  async setObjectLockConfig(bucketName, lockConfigOpts) {
    const retentionModes = [_helpers.RETENTION_MODES.COMPLIANCE, _helpers.RETENTION_MODES.GOVERNANCE];
    const validUnits = [_helpers.RETENTION_VALIDITY_UNITS.DAYS, _helpers.RETENTION_VALIDITY_UNITS.YEARS];
    if (!(0, _helper.isValidBucketName)(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (lockConfigOpts.mode && !retentionModes.includes(lockConfigOpts.mode)) {
      throw new TypeError(`lockConfigOpts.mode should be one of ${retentionModes}`);
    }
    if (lockConfigOpts.unit && !validUnits.includes(lockConfigOpts.unit)) {
      throw new TypeError(`lockConfigOpts.unit should be one of ${validUnits}`);
    }
    if (lockConfigOpts.validity && !(0, _helper.isNumber)(lockConfigOpts.validity)) {
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
        if (lockConfigOpts.unit === _helpers.RETENTION_VALIDITY_UNITS.DAYS) {
          config.Rule.DefaultRetention.Days = lockConfigOpts.validity;
        } else if (lockConfigOpts.unit === _helpers.RETENTION_VALIDITY_UNITS.YEARS) {
          config.Rule.DefaultRetention.Years = lockConfigOpts.validity;
        }
      }
    }
    const builder = new _xml2js.Builder({
      rootName: 'ObjectLockConfiguration',
      renderOpts: {
        pretty: false
      },
      headless: true
    });
    const payload = builder.buildObject(config);
    const headers = {};
    headers['Content-MD5'] = (0, _helper.toMd5)(payload);
    await this.makeRequestAsyncOmit({
      method,
      bucketName,
      query,
      headers
    }, payload);
  }
  async getBucketVersioning(bucketName) {
    if (!(0, _helper.isValidBucketName)(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    const method = 'GET';
    const query = 'versioning';
    const httpRes = await this.makeRequestAsync({
      method,
      bucketName,
      query
    });
    const xmlResult = await (0, _response.readAsString)(httpRes);
    return await xmlParsers.parseBucketVersioningConfig(xmlResult);
  }
  async setBucketVersioning(bucketName, versionConfig) {
    if (!(0, _helper.isValidBucketName)(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!Object.keys(versionConfig).length) {
      throw new errors.InvalidArgumentError('versionConfig should be of type "object"');
    }
    const method = 'PUT';
    const query = 'versioning';
    const builder = new _xml2js.Builder({
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
    const builder = new _xml2js.Builder({
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
    headers['Content-MD5'] = (0, _helper.toMd5)(payloadBuf);
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
    if (!(0, _helper.isValidBucketName)(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!(0, _helper.isPlainObject)(tags)) {
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
    if (!(0, _helper.isValidBucketName)(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    await this.removeTagging({
      bucketName
    });
  }
  async setObjectTagging(bucketName, objectName, tags, putOpts) {
    if (!(0, _helper.isValidBucketName)(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!(0, _helper.isValidObjectName)(objectName)) {
      throw new errors.InvalidBucketNameError('Invalid object name: ' + objectName);
    }
    if (!(0, _helper.isPlainObject)(tags)) {
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
    if (!(0, _helper.isValidBucketName)(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!(0, _helper.isValidObjectName)(objectName)) {
      throw new errors.InvalidBucketNameError('Invalid object name: ' + objectName);
    }
    if (removeOpts && Object.keys(removeOpts).length && !(0, _helper.isObject)(removeOpts)) {
      throw new errors.InvalidArgumentError('removeOpts should be of type "object"');
    }
    await this.removeTagging({
      bucketName,
      objectName,
      removeOpts
    });
  }
  async selectObjectContent(bucketName, objectName, selectOpts) {
    if (!(0, _helper.isValidBucketName)(bucketName)) {
      throw new errors.InvalidBucketNameError(`Invalid bucket name: ${bucketName}`);
    }
    if (!(0, _helper.isValidObjectName)(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`);
    }
    if (!_lodash.isEmpty(selectOpts)) {
      if (!(0, _helper.isString)(selectOpts.expression)) {
        throw new TypeError('sqlExpression should be of type "string"');
      }
      if (!_lodash.isEmpty(selectOpts.inputSerialization)) {
        if (!(0, _helper.isObject)(selectOpts.inputSerialization)) {
          throw new TypeError('inputSerialization should be of type "object"');
        }
      } else {
        throw new TypeError('inputSerialization is required');
      }
      if (!_lodash.isEmpty(selectOpts.outputSerialization)) {
        if (!(0, _helper.isObject)(selectOpts.outputSerialization)) {
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
    const builder = new _xml2js.Builder({
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
    const body = await (0, _response.readAsBuffer)(res);
    return (0, _xmlParser.parseSelectObjectContentResponse)(body);
  }
  async applyBucketLifecycle(bucketName, policyConfig) {
    const method = 'PUT';
    const query = 'lifecycle';
    const headers = {};
    const builder = new _xml2js.Builder({
      rootName: 'LifecycleConfiguration',
      headless: true,
      renderOpts: {
        pretty: false
      }
    });
    const payload = builder.buildObject(policyConfig);
    headers['Content-MD5'] = (0, _helper.toMd5)(payload);
    await this.makeRequestAsyncOmit({
      method,
      bucketName,
      query,
      headers
    }, payload);
  }
  async removeBucketLifecycle(bucketName) {
    if (!(0, _helper.isValidBucketName)(bucketName)) {
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
    if (!(0, _helper.isValidBucketName)(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (_lodash.isEmpty(lifeCycleConfig)) {
      await this.removeBucketLifecycle(bucketName);
    } else {
      await this.applyBucketLifecycle(bucketName, lifeCycleConfig);
    }
  }
  async getBucketLifecycle(bucketName) {
    if (!(0, _helper.isValidBucketName)(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    const method = 'GET';
    const query = 'lifecycle';
    const res = await this.makeRequestAsync({
      method,
      bucketName,
      query
    });
    const body = await (0, _response.readAsString)(res);
    return xmlParsers.parseLifecycleConfig(body);
  }
  async setBucketEncryption(bucketName, encryptionConfig) {
    if (!(0, _helper.isValidBucketName)(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!_lodash.isEmpty(encryptionConfig) && encryptionConfig.Rule.length > 1) {
      throw new errors.InvalidArgumentError('Invalid Rule length. Only one rule is allowed.: ' + encryptionConfig.Rule);
    }
    let encryptionObj = encryptionConfig;
    if (_lodash.isEmpty(encryptionConfig)) {
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
    const builder = new _xml2js.Builder({
      rootName: 'ServerSideEncryptionConfiguration',
      renderOpts: {
        pretty: false
      },
      headless: true
    });
    const payload = builder.buildObject(encryptionObj);
    const headers = {};
    headers['Content-MD5'] = (0, _helper.toMd5)(payload);
    await this.makeRequestAsyncOmit({
      method,
      bucketName,
      query,
      headers
    }, payload);
  }
  async getBucketEncryption(bucketName) {
    if (!(0, _helper.isValidBucketName)(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    const method = 'GET';
    const query = 'encryption';
    const res = await this.makeRequestAsync({
      method,
      bucketName,
      query
    });
    const body = await (0, _response.readAsString)(res);
    return xmlParsers.parseBucketEncryptionConfig(body);
  }
  async removeBucketEncryption(bucketName) {
    if (!(0, _helper.isValidBucketName)(bucketName)) {
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
    if (!(0, _helper.isValidBucketName)(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!(0, _helper.isValidObjectName)(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`);
    }
    if (getOpts && !(0, _helper.isObject)(getOpts)) {
      throw new errors.InvalidArgumentError('getOpts should be of type "object"');
    } else if (getOpts !== null && getOpts !== void 0 && getOpts.versionId && !(0, _helper.isString)(getOpts.versionId)) {
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
    const body = await (0, _response.readAsString)(res);
    return xmlParsers.parseObjectRetentionConfig(body);
  }
  async removeObjects(bucketName, objectsList) {
    if (!(0, _helper.isValidBucketName)(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!Array.isArray(objectsList)) {
      throw new errors.InvalidArgumentError('objectsList should be a list');
    }
    const runDeleteObjects = async batch => {
      const delObjects = batch.map(value => {
        return (0, _helper.isObject)(value) ? {
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
      const payload = Buffer.from(new _xml2js.Builder({
        headless: true
      }).buildObject(remObjects));
      const headers = {
        'Content-MD5': (0, _helper.toMd5)(payload)
      };
      const res = await this.makeRequestAsync({
        method: 'POST',
        bucketName,
        query: 'delete',
        headers
      }, payload);
      const body = await (0, _response.readAsString)(res);
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
    if (!(0, _helper.isValidBucketName)(bucketName)) {
      throw new errors.IsValidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!(0, _helper.isValidObjectName)(objectName)) {
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
    if (!(0, _helper.isValidBucketName)(targetBucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + targetBucketName);
    }
    if (!(0, _helper.isValidObjectName)(targetObjectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${targetObjectName}`);
    }
    if (!(0, _helper.isString)(sourceBucketNameAndObjectName)) {
      throw new TypeError('sourceBucketNameAndObjectName should be of type "string"');
    }
    if (sourceBucketNameAndObjectName === '') {
      throw new errors.InvalidPrefixError(`Empty source prefix`);
    }
    if (conditions != null && !(conditions instanceof _copyConditions.CopyConditions)) {
      throw new TypeError('conditions should be of type "CopyConditions"');
    }
    const headers = {};
    headers['x-amz-copy-source'] = (0, _helper.uriResourceEscape)(sourceBucketNameAndObjectName);
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
    const body = await (0, _response.readAsString)(res);
    return xmlParsers.parseCopyObject(body);
  }
  async copyObjectV2(sourceConfig, destConfig) {
    if (!(sourceConfig instanceof _helpers.CopySourceOptions)) {
      throw new errors.InvalidArgumentError('sourceConfig should of type CopySourceOptions ');
    }
    if (!(destConfig instanceof _helpers.CopyDestinationOptions)) {
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
    const body = await (0, _response.readAsString)(res);
    const copyRes = xmlParsers.parseCopyObject(body);
    const resHeaders = res.headers;
    const sizeHeaderValue = resHeaders && resHeaders['content-length'];
    const size = typeof sizeHeaderValue === 'number' ? sizeHeaderValue : undefined;
    return {
      Bucket: destConfig.Bucket,
      Key: destConfig.Object,
      LastModified: copyRes.lastModified,
      MetaData: (0, _helper.extractMetadata)(resHeaders),
      VersionId: (0, _helper.getVersionId)(resHeaders),
      SourceVersionId: (0, _helper.getSourceVersionId)(resHeaders),
      Etag: (0, _helper.sanitizeETag)(resHeaders.etag),
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
    const body = await (0, _response.readAsString)(res);
    const partRes = (0, _xmlParser.uploadPartParser)(body);
    const partEtagVal = (0, _helper.sanitizeETag)(res.headers.etag) || (0, _helper.sanitizeETag)(partRes.ETag);
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
    if (!(destObjConfig instanceof _helpers.CopyDestinationOptions)) {
      throw new errors.InvalidArgumentError('destConfig should of type CopyDestinationOptions ');
    }
    if (sourceFilesLength < 1 || sourceFilesLength > _helper.PART_CONSTRAINTS.MAX_PARTS_COUNT) {
      throw new errors.InvalidArgumentError(`"There must be as least one and up to ${_helper.PART_CONSTRAINTS.MAX_PARTS_COUNT} source objects.`);
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
      if (!_lodash.isEmpty(srcConfig.VersionID)) {
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
      if (srcCopySize < _helper.PART_CONSTRAINTS.ABS_MIN_PART_SIZE && index < sourceFilesLength - 1) {
        throw new errors.InvalidArgumentError(`CopySrcOptions ${index} is too small (${srcCopySize}) and it is not the last part.`);
      }

      // Is data to copy too large?
      totalSize += srcCopySize;
      if (totalSize > _helper.PART_CONSTRAINTS.MAX_MULTIPART_PUT_OBJECT_SIZE) {
        throw new errors.InvalidArgumentError(`Cannot compose an object of size ${totalSize} (> 5TiB)`);
      }

      // record source size
      srcObjectSizes[index] = srcCopySize;

      // calculate parts needed for current source
      totalParts += (0, _helper.partsRequired)(srcCopySize);
      // Do we need more parts than we are allowed?
      if (totalParts > _helper.PART_CONSTRAINTS.MAX_PARTS_COUNT) {
        throw new errors.InvalidArgumentError(`Your proposed compose object requires more than ${_helper.PART_CONSTRAINTS.MAX_PARTS_COUNT} parts`);
      }
      return resItemStat;
    });
    if (totalParts === 1 && totalSize <= _helper.PART_CONSTRAINTS.MAX_PART_SIZE || totalSize === 0) {
      return await this.copyObject(sourceObjList[0], destObjConfig); // use copyObjectV2
    }

    // preserve etag to avoid modification of object while copying.
    for (let i = 0; i < sourceFilesLength; i++) {
      ;
      sourceObjList[i].MatchETag = validatedStats[i].etag;
    }
    const splitPartSizeList = validatedStats.map((resItemStat, idx) => {
      return (0, _helper.calculateEvenSplits)(srcObjectSizes[idx], sourceObjList[idx]);
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
      for (const batch of _lodash.chunk(uploadList, maxConcurrency)) {
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
      expires = _helpers.PRESIGN_EXPIRY_DAYS_MAX;
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
    const query = reqParams ? _queryString.stringify(reqParams) : undefined;
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
      return (0, _signing.presignSignatureV4)(reqOptions, this.accessKey, this.secretKey, this.sessionToken, region, requestDate, expires);
    } catch (err) {
      if (err instanceof errors.InvalidBucketNameError) {
        throw new errors.InvalidArgumentError(`Unable to get bucket region for ${bucketName}.`);
      }
      throw err;
    }
  }
  async presignedGetObject(bucketName, objectName, expires, respHeaders, requestDate) {
    if (!(0, _helper.isValidBucketName)(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!(0, _helper.isValidObjectName)(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`);
    }
    const validRespHeaders = ['response-content-type', 'response-content-language', 'response-expires', 'response-cache-control', 'response-content-disposition', 'response-content-encoding'];
    validRespHeaders.forEach(header => {
      // @ts-ignore
      if (respHeaders !== undefined && respHeaders[header] !== undefined && !(0, _helper.isString)(respHeaders[header])) {
        throw new TypeError(`response header ${header} should be of type "string"`);
      }
    });
    return this.presignedUrl('GET', bucketName, objectName, expires, respHeaders, requestDate);
  }
  async presignedPutObject(bucketName, objectName, expires) {
    if (!(0, _helper.isValidBucketName)(bucketName)) {
      throw new errors.InvalidBucketNameError(`Invalid bucket name: ${bucketName}`);
    }
    if (!(0, _helper.isValidObjectName)(objectName)) {
      throw new errors.InvalidObjectNameError(`Invalid object name: ${objectName}`);
    }
    return this.presignedUrl('PUT', bucketName, objectName, expires);
  }
  newPostPolicy() {
    return new _postPolicy.PostPolicy();
  }
  async presignedPostPolicy(postPolicy) {
    if (this.anonymous) {
      throw new errors.AnonymousRequestError('Presigned POST policy cannot be generated for anonymous requests');
    }
    if (!(0, _helper.isObject)(postPolicy)) {
      throw new TypeError('postPolicy should be of type "object"');
    }
    const bucketName = postPolicy.formData.bucket;
    try {
      const region = await this.getBucketRegionAsync(bucketName);
      const date = new Date();
      const dateStr = (0, _helper.makeDateLong)(date);
      await this.checkAndRefreshCreds();
      if (!postPolicy.policy.expiration) {
        // 'expiration' is mandatory field for S3.
        // Set default expiration date of 7 days.
        const expires = new Date();
        expires.setSeconds(_helpers.PRESIGN_EXPIRY_DAYS_MAX);
        postPolicy.setExpires(expires);
      }
      postPolicy.policy.conditions.push(['eq', '$x-amz-date', dateStr]);
      postPolicy.formData['x-amz-date'] = dateStr;
      postPolicy.policy.conditions.push(['eq', '$x-amz-algorithm', 'AWS4-HMAC-SHA256']);
      postPolicy.formData['x-amz-algorithm'] = 'AWS4-HMAC-SHA256';
      postPolicy.policy.conditions.push(['eq', '$x-amz-credential', this.accessKey + '/' + (0, _helper.getScope)(region, date)]);
      postPolicy.formData['x-amz-credential'] = this.accessKey + '/' + (0, _helper.getScope)(region, date);
      if (this.sessionToken) {
        postPolicy.policy.conditions.push(['eq', '$x-amz-security-token', this.sessionToken]);
        postPolicy.formData['x-amz-security-token'] = this.sessionToken;
      }
      const policyBase64 = Buffer.from(JSON.stringify(postPolicy.policy)).toString('base64');
      postPolicy.formData.policy = policyBase64;
      postPolicy.formData['x-amz-signature'] = (0, _signing.postPresignSignatureV4)(region, date, this.secretKey, policyBase64);
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
    if (!(0, _helper.isValidBucketName)(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!(0, _helper.isString)(prefix)) {
      throw new TypeError('prefix should be of type "string"');
    }
    if (marker && !(0, _helper.isString)(marker)) {
      throw new TypeError('marker should be of type "string"');
    }
    if (listQueryOpts && !(0, _helper.isObject)(listQueryOpts)) {
      throw new TypeError('listQueryOpts should be of type "object"');
    }
    let {
      Delimiter,
      MaxKeys,
      IncludeVersion,
      versionIdMarker,
      keyMarker
    } = listQueryOpts;
    if (!(0, _helper.isString)(Delimiter)) {
      throw new TypeError('Delimiter should be of type "string"');
    }
    if (!(0, _helper.isNumber)(MaxKeys)) {
      throw new TypeError('MaxKeys should be of type "number"');
    }
    const queries = [];
    // escape every value in query string, except maxKeys
    queries.push(`prefix=${(0, _helper.uriEscape)(prefix)}`);
    queries.push(`delimiter=${(0, _helper.uriEscape)(Delimiter)}`);
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
      marker = (0, _helper.uriEscape)(marker);
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
    const body = await (0, _response.readAsString)(res);
    const listQryList = (0, _xmlParser.parseListObjects)(body);
    return listQryList;
  }
  listObjects(bucketName, prefix, recursive, listOpts) {
    if (prefix === undefined) {
      prefix = '';
    }
    if (recursive === undefined) {
      recursive = false;
    }
    if (!(0, _helper.isValidBucketName)(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!(0, _helper.isValidPrefix)(prefix)) {
      throw new errors.InvalidPrefixError(`Invalid prefix : ${prefix}`);
    }
    if (!(0, _helper.isString)(prefix)) {
      throw new TypeError('prefix should be of type "string"');
    }
    if (!(0, _helper.isBoolean)(recursive)) {
      throw new TypeError('recursive should be of type "boolean"');
    }
    if (listOpts && !(0, _helper.isObject)(listOpts)) {
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
    if (!(0, _helper.isValidBucketName)(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!(0, _helper.isString)(prefix)) {
      throw new TypeError('prefix should be of type "string"');
    }
    if (!(0, _helper.isString)(continuationToken)) {
      throw new TypeError('continuationToken should be of type "string"');
    }
    if (!(0, _helper.isString)(delimiter)) {
      throw new TypeError('delimiter should be of type "string"');
    }
    if (!(0, _helper.isNumber)(maxKeys)) {
      throw new TypeError('maxKeys should be of type "number"');
    }
    if (!(0, _helper.isString)(startAfter)) {
      throw new TypeError('startAfter should be of type "string"');
    }
    const queries = [];
    queries.push(`list-type=2`);
    queries.push(`encoding-type=url`);
    queries.push(`prefix=${(0, _helper.uriEscape)(prefix)}`);
    queries.push(`delimiter=${(0, _helper.uriEscape)(delimiter)}`);
    if (continuationToken) {
      queries.push(`continuation-token=${(0, _helper.uriEscape)(continuationToken)}`);
    }
    if (startAfter) {
      queries.push(`start-after=${(0, _helper.uriEscape)(startAfter)}`);
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
    const body = await (0, _response.readAsString)(res);
    return (0, _xmlParser.parseListObjectsV2)(body);
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
    if (!(0, _helper.isValidBucketName)(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!(0, _helper.isValidPrefix)(prefix)) {
      throw new errors.InvalidPrefixError(`Invalid prefix : ${prefix}`);
    }
    if (!(0, _helper.isString)(prefix)) {
      throw new TypeError('prefix should be of type "string"');
    }
    if (!(0, _helper.isBoolean)(recursive)) {
      throw new TypeError('recursive should be of type "boolean"');
    }
    if (!(0, _helper.isString)(startAfter)) {
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
    if (!(0, _helper.isValidBucketName)(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    if (!(0, _helper.isObject)(config)) {
      throw new TypeError('notification config should be of type "Object"');
    }
    const method = 'PUT';
    const query = 'notification';
    const builder = new _xml2js.Builder({
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
    await this.setBucketNotification(bucketName, new _notification.NotificationConfig());
  }
  async getBucketNotification(bucketName) {
    if (!(0, _helper.isValidBucketName)(bucketName)) {
      throw new errors.InvalidBucketNameError('Invalid bucket name: ' + bucketName);
    }
    const method = 'GET';
    const query = 'notification';
    const res = await this.makeRequestAsync({
      method,
      bucketName,
      query
    });
    const body = await (0, _response.readAsString)(res);
    return (0, _xmlParser.parseBucketNotification)(body);
  }
  listenBucketNotification(bucketName, prefix, suffix, events) {
    if (!(0, _helper.isValidBucketName)(bucketName)) {
      throw new errors.InvalidBucketNameError(`Invalid bucket name: ${bucketName}`);
    }
    if (!(0, _helper.isString)(prefix)) {
      throw new TypeError('prefix must be of type string');
    }
    if (!(0, _helper.isString)(suffix)) {
      throw new TypeError('suffix must be of type string');
    }
    if (!Array.isArray(events)) {
      throw new TypeError('events must be of type Array');
    }
    const listener = new _notification.NotificationPoller(this, bucketName, prefix, suffix, events);
    listener.start();
    return listener;
  }
}
exports.TypedClient = TypedClient;
//# sourceMappingURL=data:application/json;charset=utf-8;base64,eyJ2ZXJzaW9uIjozLCJuYW1lcyI6WyJjcnlwdG8iLCJfaW50ZXJvcFJlcXVpcmVXaWxkY2FyZCIsInJlcXVpcmUiLCJmcyIsImh0dHAiLCJodHRwcyIsInBhdGgiLCJzdHJlYW0iLCJhc3luYyIsIl9ibG9ja1N0cmVhbSIsIl9icm93c2VyT3JOb2RlIiwiX2xvZGFzaCIsIl9xdWVyeVN0cmluZyIsIl94bWwyanMiLCJfQ3JlZGVudGlhbFByb3ZpZGVyIiwiZXJyb3JzIiwiX2hlbHBlcnMiLCJfbm90aWZpY2F0aW9uIiwiX3NpZ25pbmciLCJfYXN5bmMyIiwiX2NvcHlDb25kaXRpb25zIiwiX2V4dGVuc2lvbnMiLCJfaGVscGVyIiwiX2pvaW5Ib3N0UG9ydCIsIl9wb3N0UG9saWN5IiwiX3JlcXVlc3QiLCJfcmVzcG9uc2UiLCJfczNFbmRwb2ludHMiLCJfeG1sUGFyc2VyIiwieG1sUGFyc2VycyIsImUiLCJ0IiwiV2Vha01hcCIsInIiLCJuIiwiX19lc01vZHVsZSIsIm8iLCJpIiwiZiIsIl9fcHJvdG9fXyIsImRlZmF1bHQiLCJoYXMiLCJnZXQiLCJzZXQiLCJoYXNPd25Qcm9wZXJ0eSIsImNhbGwiLCJPYmplY3QiLCJkZWZpbmVQcm9wZXJ0eSIsImdldE93blByb3BlcnR5RGVzY3JpcHRvciIsInhtbCIsInhtbDJqcyIsIkJ1aWxkZXIiLCJyZW5kZXJPcHRzIiwicHJldHR5IiwiaGVhZGxlc3MiLCJQYWNrYWdlIiwidmVyc2lvbiIsInJlcXVlc3RPcHRpb25Qcm9wZXJ0aWVzIiwiVHlwZWRDbGllbnQiLCJwYXJ0U2l6ZSIsIm1heGltdW1QYXJ0U2l6ZSIsIm1heE9iamVjdFNpemUiLCJjb25zdHJ1Y3RvciIsInBhcmFtcyIsInNlY3VyZSIsInVuZGVmaW5lZCIsIkVycm9yIiwidXNlU1NMIiwicG9ydCIsImlzVmFsaWRFbmRwb2ludCIsImVuZFBvaW50IiwiSW52YWxpZEVuZHBvaW50RXJyb3IiLCJpc1ZhbGlkUG9ydCIsIkludmFsaWRBcmd1bWVudEVycm9yIiwiaXNCb29sZWFuIiwicmVnaW9uIiwiaXNTdHJpbmciLCJob3N0IiwidG9Mb3dlckNhc2UiLCJwcm90b2NvbCIsInRyYW5zcG9ydCIsInRyYW5zcG9ydEFnZW50IiwiZ2xvYmFsQWdlbnQiLCJpc09iamVjdCIsImxpYnJhcnlDb21tZW50cyIsInByb2Nlc3MiLCJwbGF0Zm9ybSIsImFyY2giLCJsaWJyYXJ5QWdlbnQiLCJ1c2VyQWdlbnQiLCJwYXRoU3R5bGUiLCJhY2Nlc3NLZXkiLCJzZWNyZXRLZXkiLCJzZXNzaW9uVG9rZW4iLCJhbm9ueW1vdXMiLCJjcmVkZW50aWFsc1Byb3ZpZGVyIiwicmVnaW9uTWFwIiwib3ZlclJpZGVQYXJ0U2l6ZSIsImVuYWJsZVNIQTI1NiIsInMzQWNjZWxlcmF0ZUVuZHBvaW50IiwicmVxT3B0aW9ucyIsImNsaWVudEV4dGVuc2lvbnMiLCJFeHRlbnNpb25zIiwicmV0cnlPcHRpb25zIiwiZGlzYWJsZVJldHJ5IiwiZXh0ZW5zaW9ucyIsInNldFMzVHJhbnNmZXJBY2NlbGVyYXRlIiwic2V0UmVxdWVzdE9wdGlvbnMiLCJvcHRpb25zIiwiVHlwZUVycm9yIiwiXyIsInBpY2siLCJnZXRBY2NlbGVyYXRlRW5kUG9pbnRJZlNldCIsImJ1Y2tldE5hbWUiLCJvYmplY3ROYW1lIiwiaXNFbXB0eSIsImluY2x1ZGVzIiwic2V0QXBwSW5mbyIsImFwcE5hbWUiLCJhcHBWZXJzaW9uIiwidHJpbSIsImdldFJlcXVlc3RPcHRpb25zIiwib3B0cyIsIm1ldGhvZCIsImhlYWRlcnMiLCJxdWVyeSIsImFnZW50IiwidmlydHVhbEhvc3RTdHlsZSIsImlzVmlydHVhbEhvc3RTdHlsZSIsInVyaVJlc291cmNlRXNjYXBlIiwiaXNBbWF6b25FbmRwb2ludCIsImFjY2VsZXJhdGVFbmRQb2ludCIsImdldFMzRW5kcG9pbnQiLCJqb2luSG9zdFBvcnQiLCJrIiwidiIsImVudHJpZXMiLCJhc3NpZ24iLCJtYXBWYWx1ZXMiLCJwaWNrQnkiLCJpc0RlZmluZWQiLCJ0b1N0cmluZyIsInNldENyZWRlbnRpYWxzUHJvdmlkZXIiLCJDcmVkZW50aWFsUHJvdmlkZXIiLCJjaGVja0FuZFJlZnJlc2hDcmVkcyIsImNyZWRlbnRpYWxzQ29uZiIsImdldENyZWRlbnRpYWxzIiwiZ2V0QWNjZXNzS2V5IiwiZ2V0U2VjcmV0S2V5IiwiZ2V0U2Vzc2lvblRva2VuIiwiY2F1c2UiLCJsb2dIVFRQIiwicmVzcG9uc2UiLCJlcnIiLCJsb2dTdHJlYW0iLCJpc1JlYWRhYmxlU3RyZWFtIiwibG9nSGVhZGVycyIsImZvckVhY2giLCJyZWRhY3RvciIsIlJlZ0V4cCIsInJlcGxhY2UiLCJ3cml0ZSIsInN0YXR1c0NvZGUiLCJlcnJKU09OIiwiSlNPTiIsInN0cmluZ2lmeSIsInRyYWNlT24iLCJzdGRvdXQiLCJ0cmFjZU9mZiIsIm1ha2VSZXF1ZXN0QXN5bmMiLCJwYXlsb2FkIiwiZXhwZWN0ZWRDb2RlcyIsImlzTnVtYmVyIiwibGVuZ3RoIiwic2hhMjU2c3VtIiwidG9TaGEyNTYiLCJtYWtlUmVxdWVzdFN0cmVhbUFzeW5jIiwibWFrZVJlcXVlc3RBc3luY09taXQiLCJzdGF0dXNDb2RlcyIsInJlcyIsImRyYWluUmVzcG9uc2UiLCJib2R5IiwiQnVmZmVyIiwiaXNCdWZmZXIiLCJnZXRCdWNrZXRSZWdpb25Bc3luYyIsImRhdGUiLCJEYXRlIiwibWFrZURhdGVMb25nIiwiYXV0aG9yaXphdGlvbiIsInNpZ25WNCIsImxvZyIsIm1zZyIsInJlcXVlc3RXaXRoUmV0cnkiLCJtYXhpbXVtUmV0cnlDb3VudCIsImJhc2VEZWxheU1zIiwibWF4aW11bURlbGF5TXMiLCJwYXJzZVJlc3BvbnNlRXJyb3IiLCJpc1ZhbGlkQnVja2V0TmFtZSIsIkludmFsaWRCdWNrZXROYW1lRXJyb3IiLCJjYWNoZWQiLCJleHRyYWN0UmVnaW9uQXN5bmMiLCJyZWFkQXNTdHJpbmciLCJwYXJzZUJ1Y2tldFJlZ2lvbiIsIkRFRkFVTFRfUkVHSU9OIiwiaXNCcm93c2VyIiwiUzNFcnJvciIsImVyckNvZGUiLCJjb2RlIiwiZXJyUmVnaW9uIiwibmFtZSIsIlJlZ2lvbiIsIm1ha2VSZXF1ZXN0IiwicmV0dXJuUmVzcG9uc2UiLCJjYiIsInByb20iLCJ0aGVuIiwicmVzdWx0IiwibWFrZVJlcXVlc3RTdHJlYW0iLCJleGVjdXRvciIsImdldEJ1Y2tldFJlZ2lvbiIsIm1ha2VCdWNrZXQiLCJtYWtlT3B0cyIsImJ1aWxkT2JqZWN0IiwiQ3JlYXRlQnVja2V0Q29uZmlndXJhdGlvbiIsIiQiLCJ4bWxucyIsIkxvY2F0aW9uQ29uc3RyYWludCIsIk9iamVjdExvY2tpbmciLCJmaW5hbFJlZ2lvbiIsInJlcXVlc3RPcHQiLCJidWNrZXRFeGlzdHMiLCJyZW1vdmVCdWNrZXQiLCJnZXRPYmplY3QiLCJnZXRPcHRzIiwiaXNWYWxpZE9iamVjdE5hbWUiLCJJbnZhbGlkT2JqZWN0TmFtZUVycm9yIiwiZ2V0UGFydGlhbE9iamVjdCIsIm9mZnNldCIsInJhbmdlIiwic3NlSGVhZGVycyIsIlNTRUN1c3RvbWVyQWxnb3JpdGhtIiwiU1NFQ3VzdG9tZXJLZXkiLCJTU0VDdXN0b21lcktleU1ENSIsInFzIiwicHJlcGVuZFhBTVpNZXRhIiwiZXhwZWN0ZWRTdGF0dXNDb2RlcyIsInB1c2giLCJmR2V0T2JqZWN0IiwiZmlsZVBhdGgiLCJkb3dubG9hZFRvVG1wRmlsZSIsInBhcnRGaWxlU3RyZWFtIiwib2JqU3RhdCIsInN0YXRPYmplY3QiLCJlbmNvZGVkRXRhZyIsImZyb20iLCJldGFnIiwicGFydEZpbGUiLCJmc3AiLCJta2RpciIsImRpcm5hbWUiLCJyZWN1cnNpdmUiLCJzdGF0cyIsInN0YXQiLCJzaXplIiwiY3JlYXRlV3JpdGVTdHJlYW0iLCJmbGFncyIsImRvd25sb2FkU3RyZWFtIiwic3RyZWFtUHJvbWlzZSIsInBpcGVsaW5lIiwicmVuYW1lIiwic3RhdE9wdHMiLCJzdGF0T3B0RGVmIiwicGFyc2VJbnQiLCJtZXRhRGF0YSIsImV4dHJhY3RNZXRhZGF0YSIsImxhc3RNb2RpZmllZCIsInZlcnNpb25JZCIsImdldFZlcnNpb25JZCIsInNhbml0aXplRVRhZyIsInJlbW92ZU9iamVjdCIsInJlbW92ZU9wdHMiLCJnb3Zlcm5hbmNlQnlwYXNzIiwiZm9yY2VEZWxldGUiLCJxdWVyeVBhcmFtcyIsImxpc3RJbmNvbXBsZXRlVXBsb2FkcyIsImJ1Y2tldCIsInByZWZpeCIsImlzVmFsaWRQcmVmaXgiLCJJbnZhbGlkUHJlZml4RXJyb3IiLCJkZWxpbWl0ZXIiLCJrZXlNYXJrZXIiLCJ1cGxvYWRJZE1hcmtlciIsInVwbG9hZHMiLCJlbmRlZCIsInJlYWRTdHJlYW0iLCJSZWFkYWJsZSIsIm9iamVjdE1vZGUiLCJfcmVhZCIsInNoaWZ0IiwibGlzdEluY29tcGxldGVVcGxvYWRzUXVlcnkiLCJwcmVmaXhlcyIsImVhY2hTZXJpZXMiLCJ1cGxvYWQiLCJsaXN0UGFydHMiLCJrZXkiLCJ1cGxvYWRJZCIsInBhcnRzIiwicmVkdWNlIiwiYWNjIiwiaXRlbSIsImVtaXQiLCJpc1RydW5jYXRlZCIsIm5leHRLZXlNYXJrZXIiLCJuZXh0VXBsb2FkSWRNYXJrZXIiLCJxdWVyaWVzIiwidXJpRXNjYXBlIiwibWF4VXBsb2FkcyIsInNvcnQiLCJ1bnNoaWZ0Iiwiam9pbiIsInBhcnNlTGlzdE11bHRpcGFydCIsImluaXRpYXRlTmV3TXVsdGlwYXJ0VXBsb2FkIiwicmVhZEFzQnVmZmVyIiwicGFyc2VJbml0aWF0ZU11bHRpcGFydCIsImFib3J0TXVsdGlwYXJ0VXBsb2FkIiwicmVxdWVzdE9wdGlvbnMiLCJmaW5kVXBsb2FkSWQiLCJfbGF0ZXN0VXBsb2FkIiwibGF0ZXN0VXBsb2FkIiwiaW5pdGlhdGVkIiwiZ2V0VGltZSIsImNvbXBsZXRlTXVsdGlwYXJ0VXBsb2FkIiwiZXRhZ3MiLCJidWlsZGVyIiwiQ29tcGxldGVNdWx0aXBhcnRVcGxvYWQiLCJQYXJ0IiwibWFwIiwiUGFydE51bWJlciIsInBhcnQiLCJFVGFnIiwicGFyc2VDb21wbGV0ZU11bHRpcGFydCIsImVyck1lc3NhZ2UiLCJtYXJrZXIiLCJsaXN0UGFydHNRdWVyeSIsInBhcnNlTGlzdFBhcnRzIiwibGlzdEJ1Y2tldHMiLCJyZWdpb25Db25mIiwiaHR0cFJlcyIsInhtbFJlc3VsdCIsInBhcnNlTGlzdEJ1Y2tldCIsImNhbGN1bGF0ZVBhcnRTaXplIiwiZlB1dE9iamVjdCIsImluc2VydENvbnRlbnRUeXBlIiwicHV0T2JqZWN0IiwiY3JlYXRlUmVhZFN0cmVhbSIsInJlYWRhYmxlU3RyZWFtIiwic3RhdFNpemUiLCJnZXRDb250ZW50TGVuZ3RoIiwidXBsb2FkQnVmZmVyIiwiYnVmIiwidXBsb2FkU3RyZWFtIiwibWQ1c3VtIiwiaGFzaEJpbmFyeSIsIm9sZFBhcnRzIiwiZVRhZ3MiLCJwcmV2aW91c1VwbG9hZElkIiwib2xkVGFncyIsImNodW5raWVyIiwiQmxvY2tTdHJlYW0yIiwiemVyb1BhZGRpbmciLCJQcm9taXNlIiwiYWxsIiwicmVzb2x2ZSIsInJlamVjdCIsInBpcGUiLCJvbiIsInBhcnROdW1iZXIiLCJjaHVuayIsIm1kNSIsImNyZWF0ZUhhc2giLCJ1cGRhdGUiLCJkaWdlc3QiLCJvbGRQYXJ0IiwicmVtb3ZlQnVja2V0UmVwbGljYXRpb24iLCJzZXRCdWNrZXRSZXBsaWNhdGlvbiIsInJlcGxpY2F0aW9uQ29uZmlnIiwicm9sZSIsInJ1bGVzIiwicmVwbGljYXRpb25QYXJhbXNDb25maWciLCJSZXBsaWNhdGlvbkNvbmZpZ3VyYXRpb24iLCJSb2xlIiwiUnVsZSIsInRvTWQ1IiwiZ2V0QnVja2V0UmVwbGljYXRpb24iLCJwYXJzZVJlcGxpY2F0aW9uQ29uZmlnIiwiZ2V0T2JqZWN0TGVnYWxIb2xkIiwia2V5cyIsInN0clJlcyIsInBhcnNlT2JqZWN0TGVnYWxIb2xkQ29uZmlnIiwic2V0T2JqZWN0TGVnYWxIb2xkIiwic2V0T3B0cyIsInN0YXR1cyIsIkxFR0FMX0hPTERfU1RBVFVTIiwiRU5BQkxFRCIsIkRJU0FCTEVEIiwiY29uZmlnIiwiU3RhdHVzIiwicm9vdE5hbWUiLCJnZXRCdWNrZXRUYWdnaW5nIiwicGFyc2VUYWdnaW5nIiwiZ2V0T2JqZWN0VGFnZ2luZyIsInNldEJ1Y2tldFBvbGljeSIsInBvbGljeSIsIkludmFsaWRCdWNrZXRQb2xpY3lFcnJvciIsImdldEJ1Y2tldFBvbGljeSIsInB1dE9iamVjdFJldGVudGlvbiIsInJldGVudGlvbk9wdHMiLCJtb2RlIiwiUkVURU5USU9OX01PREVTIiwiQ09NUExJQU5DRSIsIkdPVkVSTkFOQ0UiLCJyZXRhaW5VbnRpbERhdGUiLCJNb2RlIiwiUmV0YWluVW50aWxEYXRlIiwiZ2V0T2JqZWN0TG9ja0NvbmZpZyIsInBhcnNlT2JqZWN0TG9ja0NvbmZpZyIsInNldE9iamVjdExvY2tDb25maWciLCJsb2NrQ29uZmlnT3B0cyIsInJldGVudGlvbk1vZGVzIiwidmFsaWRVbml0cyIsIlJFVEVOVElPTl9WQUxJRElUWV9VTklUUyIsIkRBWVMiLCJZRUFSUyIsInVuaXQiLCJ2YWxpZGl0eSIsIk9iamVjdExvY2tFbmFibGVkIiwiY29uZmlnS2V5cyIsImlzQWxsS2V5c1NldCIsImV2ZXJ5IiwibGNrIiwiRGVmYXVsdFJldGVudGlvbiIsIkRheXMiLCJZZWFycyIsImdldEJ1Y2tldFZlcnNpb25pbmciLCJwYXJzZUJ1Y2tldFZlcnNpb25pbmdDb25maWciLCJzZXRCdWNrZXRWZXJzaW9uaW5nIiwidmVyc2lvbkNvbmZpZyIsInNldFRhZ2dpbmciLCJ0YWdnaW5nUGFyYW1zIiwidGFncyIsInB1dE9wdHMiLCJ0YWdzTGlzdCIsInZhbHVlIiwiS2V5IiwiVmFsdWUiLCJ0YWdnaW5nQ29uZmlnIiwiVGFnZ2luZyIsIlRhZ1NldCIsIlRhZyIsInBheWxvYWRCdWYiLCJyZW1vdmVUYWdnaW5nIiwic2V0QnVja2V0VGFnZ2luZyIsImlzUGxhaW5PYmplY3QiLCJyZW1vdmVCdWNrZXRUYWdnaW5nIiwic2V0T2JqZWN0VGFnZ2luZyIsInJlbW92ZU9iamVjdFRhZ2dpbmciLCJzZWxlY3RPYmplY3RDb250ZW50Iiwic2VsZWN0T3B0cyIsImV4cHJlc3Npb24iLCJpbnB1dFNlcmlhbGl6YXRpb24iLCJvdXRwdXRTZXJpYWxpemF0aW9uIiwiRXhwcmVzc2lvbiIsIkV4cHJlc3Npb25UeXBlIiwiZXhwcmVzc2lvblR5cGUiLCJJbnB1dFNlcmlhbGl6YXRpb24iLCJPdXRwdXRTZXJpYWxpemF0aW9uIiwicmVxdWVzdFByb2dyZXNzIiwiUmVxdWVzdFByb2dyZXNzIiwic2NhblJhbmdlIiwiU2NhblJhbmdlIiwicGFyc2VTZWxlY3RPYmplY3RDb250ZW50UmVzcG9uc2UiLCJhcHBseUJ1Y2tldExpZmVjeWNsZSIsInBvbGljeUNvbmZpZyIsInJlbW92ZUJ1Y2tldExpZmVjeWNsZSIsInNldEJ1Y2tldExpZmVjeWNsZSIsImxpZmVDeWNsZUNvbmZpZyIsImdldEJ1Y2tldExpZmVjeWNsZSIsInBhcnNlTGlmZWN5Y2xlQ29uZmlnIiwic2V0QnVja2V0RW5jcnlwdGlvbiIsImVuY3J5cHRpb25Db25maWciLCJlbmNyeXB0aW9uT2JqIiwiQXBwbHlTZXJ2ZXJTaWRlRW5jcnlwdGlvbkJ5RGVmYXVsdCIsIlNTRUFsZ29yaXRobSIsImdldEJ1Y2tldEVuY3J5cHRpb24iLCJwYXJzZUJ1Y2tldEVuY3J5cHRpb25Db25maWciLCJyZW1vdmVCdWNrZXRFbmNyeXB0aW9uIiwiZ2V0T2JqZWN0UmV0ZW50aW9uIiwicGFyc2VPYmplY3RSZXRlbnRpb25Db25maWciLCJyZW1vdmVPYmplY3RzIiwib2JqZWN0c0xpc3QiLCJBcnJheSIsImlzQXJyYXkiLCJydW5EZWxldGVPYmplY3RzIiwiYmF0Y2giLCJkZWxPYmplY3RzIiwiVmVyc2lvbklkIiwicmVtT2JqZWN0cyIsIkRlbGV0ZSIsIlF1aWV0IiwicmVtb3ZlT2JqZWN0c1BhcnNlciIsIm1heEVudHJpZXMiLCJiYXRjaGVzIiwic2xpY2UiLCJiYXRjaFJlc3VsdHMiLCJmbGF0IiwicmVtb3ZlSW5jb21wbGV0ZVVwbG9hZCIsIklzVmFsaWRCdWNrZXROYW1lRXJyb3IiLCJyZW1vdmVVcGxvYWRJZCIsImNvcHlPYmplY3RWMSIsInRhcmdldEJ1Y2tldE5hbWUiLCJ0YXJnZXRPYmplY3ROYW1lIiwic291cmNlQnVja2V0TmFtZUFuZE9iamVjdE5hbWUiLCJjb25kaXRpb25zIiwiQ29weUNvbmRpdGlvbnMiLCJtb2RpZmllZCIsInVubW9kaWZpZWQiLCJtYXRjaEVUYWciLCJtYXRjaEVUYWdFeGNlcHQiLCJwYXJzZUNvcHlPYmplY3QiLCJjb3B5T2JqZWN0VjIiLCJzb3VyY2VDb25maWciLCJkZXN0Q29uZmlnIiwiQ29weVNvdXJjZU9wdGlvbnMiLCJDb3B5RGVzdGluYXRpb25PcHRpb25zIiwidmFsaWRhdGUiLCJnZXRIZWFkZXJzIiwiQnVja2V0IiwiY29weVJlcyIsInJlc0hlYWRlcnMiLCJzaXplSGVhZGVyVmFsdWUiLCJMYXN0TW9kaWZpZWQiLCJNZXRhRGF0YSIsIlNvdXJjZVZlcnNpb25JZCIsImdldFNvdXJjZVZlcnNpb25JZCIsIkV0YWciLCJTaXplIiwiY29weU9iamVjdCIsImFsbEFyZ3MiLCJzb3VyY2UiLCJkZXN0IiwidXBsb2FkUGFydCIsInBhcnRDb25maWciLCJ1cGxvYWRJRCIsInBhcnRSZXMiLCJ1cGxvYWRQYXJ0UGFyc2VyIiwicGFydEV0YWdWYWwiLCJjb21wb3NlT2JqZWN0IiwiZGVzdE9iakNvbmZpZyIsInNvdXJjZU9iakxpc3QiLCJtYXhDb25jdXJyZW5jeSIsInNvdXJjZUZpbGVzTGVuZ3RoIiwiUEFSVF9DT05TVFJBSU5UUyIsIk1BWF9QQVJUU19DT1VOVCIsInNPYmoiLCJnZXRTdGF0T3B0aW9ucyIsInNyY0NvbmZpZyIsIlZlcnNpb25JRCIsInNyY09iamVjdFNpemVzIiwidG90YWxTaXplIiwidG90YWxQYXJ0cyIsInNvdXJjZU9ialN0YXRzIiwic3JjSXRlbSIsInNyY09iamVjdEluZm9zIiwidmFsaWRhdGVkU3RhdHMiLCJyZXNJdGVtU3RhdCIsImluZGV4Iiwic3JjQ29weVNpemUiLCJNYXRjaFJhbmdlIiwic3JjU3RhcnQiLCJTdGFydCIsInNyY0VuZCIsIkVuZCIsIkFCU19NSU5fUEFSVF9TSVpFIiwiTUFYX01VTFRJUEFSVF9QVVRfT0JKRUNUX1NJWkUiLCJwYXJ0c1JlcXVpcmVkIiwiTUFYX1BBUlRfU0laRSIsIk1hdGNoRVRhZyIsInNwbGl0UGFydFNpemVMaXN0IiwiaWR4IiwiY2FsY3VsYXRlRXZlblNwbGl0cyIsImdldFVwbG9hZFBhcnRDb25maWdMaXN0IiwidXBsb2FkUGFydENvbmZpZ0xpc3QiLCJzcGxpdFNpemUiLCJzcGxpdEluZGV4Iiwic3RhcnRJbmRleCIsInN0YXJ0SWR4IiwiZW5kSW5kZXgiLCJlbmRJZHgiLCJwYXJ0SW5kZXgiLCJ0b3RhbFVwbG9hZHMiLCJzcGxpdFN0YXJ0IiwidXBsZEN0cklkeCIsInNwbGl0RW5kIiwidXBsb2FkUGFydENvbmZpZyIsInVwbG9hZEFsbFBhcnRzIiwidXBsb2FkTGlzdCIsInBhcnRVcGxvYWRzIiwicGVyZm9ybVVwbG9hZFBhcnRzIiwicGFydHNSZXMiLCJwYXJ0Q29weSIsIm5ld1VwbG9hZEhlYWRlcnMiLCJwYXJ0c0RvbmUiLCJwcmVzaWduZWRVcmwiLCJleHBpcmVzIiwicmVxUGFyYW1zIiwicmVxdWVzdERhdGUiLCJfcmVxdWVzdERhdGUiLCJBbm9ueW1vdXNSZXF1ZXN0RXJyb3IiLCJQUkVTSUdOX0VYUElSWV9EQVlTX01BWCIsImlzTmFOIiwicHJlc2lnblNpZ25hdHVyZVY0IiwicHJlc2lnbmVkR2V0T2JqZWN0IiwicmVzcEhlYWRlcnMiLCJ2YWxpZFJlc3BIZWFkZXJzIiwiaGVhZGVyIiwicHJlc2lnbmVkUHV0T2JqZWN0IiwibmV3UG9zdFBvbGljeSIsIlBvc3RQb2xpY3kiLCJwcmVzaWduZWRQb3N0UG9saWN5IiwicG9zdFBvbGljeSIsImZvcm1EYXRhIiwiZGF0ZVN0ciIsImV4cGlyYXRpb24iLCJzZXRTZWNvbmRzIiwic2V0RXhwaXJlcyIsImdldFNjb3BlIiwicG9saWN5QmFzZTY0IiwicG9zdFByZXNpZ25TaWduYXR1cmVWNCIsInBvcnRTdHIiLCJ1cmxTdHIiLCJwb3N0VVJMIiwibGlzdE9iamVjdHNRdWVyeSIsImxpc3RRdWVyeU9wdHMiLCJEZWxpbWl0ZXIiLCJNYXhLZXlzIiwiSW5jbHVkZVZlcnNpb24iLCJ2ZXJzaW9uSWRNYXJrZXIiLCJsaXN0UXJ5TGlzdCIsInBhcnNlTGlzdE9iamVjdHMiLCJsaXN0T2JqZWN0cyIsImxpc3RPcHRzIiwib2JqZWN0cyIsIm5leHRNYXJrZXIiLCJsaXN0T2JqZWN0c1YyUXVlcnkiLCJjb250aW51YXRpb25Ub2tlbiIsIm1heEtleXMiLCJzdGFydEFmdGVyIiwicGFyc2VMaXN0T2JqZWN0c1YyIiwibGlzdE9iamVjdHNWMiIsInByZWZpeFN0ciIsInN0YXJ0QWZ0ZXJTdHIiLCJuZXh0Q29udGludWF0aW9uVG9rZW4iLCJzZXRCdWNrZXROb3RpZmljYXRpb24iLCJyZW1vdmVBbGxCdWNrZXROb3RpZmljYXRpb24iLCJOb3RpZmljYXRpb25Db25maWciLCJnZXRCdWNrZXROb3RpZmljYXRpb24iLCJwYXJzZUJ1Y2tldE5vdGlmaWNhdGlvbiIsImxpc3RlbkJ1Y2tldE5vdGlmaWNhdGlvbiIsInN1ZmZpeCIsImV2ZW50cyIsImxpc3RlbmVyIiwiTm90aWZpY2F0aW9uUG9sbGVyIiwic3RhcnQiLCJleHBvcnRzIl0sInNvdXJjZXMiOlsiY2xpZW50LnRzIl0sInNvdXJjZXNDb250ZW50IjpbImltcG9ydCAqIGFzIGNyeXB0byBmcm9tICdub2RlOmNyeXB0bydcbmltcG9ydCAqIGFzIGZzIGZyb20gJ25vZGU6ZnMnXG5pbXBvcnQgdHlwZSB7IEluY29taW5nSHR0cEhlYWRlcnMgfSBmcm9tICdub2RlOmh0dHAnXG5pbXBvcnQgKiBhcyBodHRwIGZyb20gJ25vZGU6aHR0cCdcbmltcG9ydCAqIGFzIGh0dHBzIGZyb20gJ25vZGU6aHR0cHMnXG5pbXBvcnQgKiBhcyBwYXRoIGZyb20gJ25vZGU6cGF0aCdcbmltcG9ydCAqIGFzIHN0cmVhbSBmcm9tICdub2RlOnN0cmVhbSdcblxuaW1wb3J0ICogYXMgYXN5bmMgZnJvbSAnYXN5bmMnXG5pbXBvcnQgQmxvY2tTdHJlYW0yIGZyb20gJ2Jsb2NrLXN0cmVhbTInXG5pbXBvcnQgeyBpc0Jyb3dzZXIgfSBmcm9tICdicm93c2VyLW9yLW5vZGUnXG5pbXBvcnQgXyBmcm9tICdsb2Rhc2gnXG5pbXBvcnQgcXMgZnJvbSAncXVlcnktc3RyaW5nJ1xuaW1wb3J0IHhtbDJqcyBmcm9tICd4bWwyanMnXG5cbmltcG9ydCB7IENyZWRlbnRpYWxQcm92aWRlciB9IGZyb20gJy4uL0NyZWRlbnRpYWxQcm92aWRlci50cydcbmltcG9ydCAqIGFzIGVycm9ycyBmcm9tICcuLi9lcnJvcnMudHMnXG5pbXBvcnQgdHlwZSB7IFNlbGVjdFJlc3VsdHMgfSBmcm9tICcuLi9oZWxwZXJzLnRzJ1xuaW1wb3J0IHtcbiAgQ29weURlc3RpbmF0aW9uT3B0aW9ucyxcbiAgQ29weVNvdXJjZU9wdGlvbnMsXG4gIERFRkFVTFRfUkVHSU9OLFxuICBMRUdBTF9IT0xEX1NUQVRVUyxcbiAgUFJFU0lHTl9FWFBJUllfREFZU19NQVgsXG4gIFJFVEVOVElPTl9NT0RFUyxcbiAgUkVURU5USU9OX1ZBTElESVRZX1VOSVRTLFxufSBmcm9tICcuLi9oZWxwZXJzLnRzJ1xuaW1wb3J0IHR5cGUgeyBOb3RpZmljYXRpb25FdmVudCB9IGZyb20gJy4uL25vdGlmaWNhdGlvbi50cydcbmltcG9ydCB7IE5vdGlmaWNhdGlvbkNvbmZpZywgTm90aWZpY2F0aW9uUG9sbGVyIH0gZnJvbSAnLi4vbm90aWZpY2F0aW9uLnRzJ1xuaW1wb3J0IHsgcG9zdFByZXNpZ25TaWduYXR1cmVWNCwgcHJlc2lnblNpZ25hdHVyZVY0LCBzaWduVjQgfSBmcm9tICcuLi9zaWduaW5nLnRzJ1xuaW1wb3J0IHsgZnNwLCBzdHJlYW1Qcm9taXNlIH0gZnJvbSAnLi9hc3luYy50cydcbmltcG9ydCB7IENvcHlDb25kaXRpb25zIH0gZnJvbSAnLi9jb3B5LWNvbmRpdGlvbnMudHMnXG5pbXBvcnQgeyBFeHRlbnNpb25zIH0gZnJvbSAnLi9leHRlbnNpb25zLnRzJ1xuaW1wb3J0IHtcbiAgY2FsY3VsYXRlRXZlblNwbGl0cyxcbiAgZXh0cmFjdE1ldGFkYXRhLFxuICBnZXRDb250ZW50TGVuZ3RoLFxuICBnZXRTY29wZSxcbiAgZ2V0U291cmNlVmVyc2lvbklkLFxuICBnZXRWZXJzaW9uSWQsXG4gIGhhc2hCaW5hcnksXG4gIGluc2VydENvbnRlbnRUeXBlLFxuICBpc0FtYXpvbkVuZHBvaW50LFxuICBpc0Jvb2xlYW4sXG4gIGlzRGVmaW5lZCxcbiAgaXNFbXB0eSxcbiAgaXNOdW1iZXIsXG4gIGlzT2JqZWN0LFxuICBpc1BsYWluT2JqZWN0LFxuICBpc1JlYWRhYmxlU3RyZWFtLFxuICBpc1N0cmluZyxcbiAgaXNWYWxpZEJ1Y2tldE5hbWUsXG4gIGlzVmFsaWRFbmRwb2ludCxcbiAgaXNWYWxpZE9iamVjdE5hbWUsXG4gIGlzVmFsaWRQb3J0LFxuICBpc1ZhbGlkUHJlZml4LFxuICBpc1ZpcnR1YWxIb3N0U3R5bGUsXG4gIG1ha2VEYXRlTG9uZyxcbiAgUEFSVF9DT05TVFJBSU5UUyxcbiAgcGFydHNSZXF1aXJlZCxcbiAgcHJlcGVuZFhBTVpNZXRhLFxuICByZWFkYWJsZVN0cmVhbSxcbiAgc2FuaXRpemVFVGFnLFxuICB0b01kNSxcbiAgdG9TaGEyNTYsXG4gIHVyaUVzY2FwZSxcbiAgdXJpUmVzb3VyY2VFc2NhcGUsXG59IGZyb20gJy4vaGVscGVyLnRzJ1xuaW1wb3J0IHsgam9pbkhvc3RQb3J0IH0gZnJvbSAnLi9qb2luLWhvc3QtcG9ydC50cydcbmltcG9ydCB7IFBvc3RQb2xpY3kgfSBmcm9tICcuL3Bvc3QtcG9saWN5LnRzJ1xuaW1wb3J0IHsgcmVxdWVzdFdpdGhSZXRyeSB9IGZyb20gJy4vcmVxdWVzdC50cydcbmltcG9ydCB7IGRyYWluUmVzcG9uc2UsIHJlYWRBc0J1ZmZlciwgcmVhZEFzU3RyaW5nIH0gZnJvbSAnLi9yZXNwb25zZS50cydcbmltcG9ydCB0eXBlIHsgUmVnaW9uIH0gZnJvbSAnLi9zMy1lbmRwb2ludHMudHMnXG5pbXBvcnQgeyBnZXRTM0VuZHBvaW50IH0gZnJvbSAnLi9zMy1lbmRwb2ludHMudHMnXG5pbXBvcnQgdHlwZSB7XG4gIEJpbmFyeSxcbiAgQnVja2V0SXRlbSxcbiAgQnVja2V0SXRlbUZyb21MaXN0LFxuICBCdWNrZXRJdGVtU3RhdCxcbiAgQnVja2V0U3RyZWFtLFxuICBCdWNrZXRWZXJzaW9uaW5nQ29uZmlndXJhdGlvbixcbiAgQ29weU9iamVjdFBhcmFtcyxcbiAgQ29weU9iamVjdFJlc3VsdCxcbiAgQ29weU9iamVjdFJlc3VsdFYyLFxuICBFbmNyeXB0aW9uQ29uZmlnLFxuICBHZXRPYmplY3RMZWdhbEhvbGRPcHRpb25zLFxuICBHZXRPYmplY3RPcHRzLFxuICBHZXRPYmplY3RSZXRlbnRpb25PcHRzLFxuICBJbmNvbXBsZXRlVXBsb2FkZWRCdWNrZXRJdGVtLFxuICBJUmVxdWVzdCxcbiAgSXRlbUJ1Y2tldE1ldGFkYXRhLFxuICBMaWZlY3ljbGVDb25maWcsXG4gIExpZmVDeWNsZUNvbmZpZ1BhcmFtLFxuICBMaXN0T2JqZWN0UXVlcnlPcHRzLFxuICBMaXN0T2JqZWN0UXVlcnlSZXMsXG4gIExpc3RPYmplY3RWMlJlcyxcbiAgTm90aWZpY2F0aW9uQ29uZmlnUmVzdWx0LFxuICBPYmplY3RJbmZvLFxuICBPYmplY3RMb2NrQ29uZmlnUGFyYW0sXG4gIE9iamVjdExvY2tJbmZvLFxuICBPYmplY3RNZXRhRGF0YSxcbiAgT2JqZWN0UmV0ZW50aW9uSW5mbyxcbiAgUG9zdFBvbGljeVJlc3VsdCxcbiAgUHJlU2lnblJlcXVlc3RQYXJhbXMsXG4gIFB1dE9iamVjdExlZ2FsSG9sZE9wdGlvbnMsXG4gIFB1dFRhZ2dpbmdQYXJhbXMsXG4gIFJlbW92ZU9iamVjdHNQYXJhbSxcbiAgUmVtb3ZlT2JqZWN0c1JlcXVlc3RFbnRyeSxcbiAgUmVtb3ZlT2JqZWN0c1Jlc3BvbnNlLFxuICBSZW1vdmVUYWdnaW5nUGFyYW1zLFxuICBSZXBsaWNhdGlvbkNvbmZpZyxcbiAgUmVwbGljYXRpb25Db25maWdPcHRzLFxuICBSZXF1ZXN0SGVhZGVycyxcbiAgUmVzcG9uc2VIZWFkZXIsXG4gIFJlc3VsdENhbGxiYWNrLFxuICBSZXRlbnRpb24sXG4gIFNlbGVjdE9wdGlvbnMsXG4gIFN0YXRPYmplY3RPcHRzLFxuICBUYWcsXG4gIFRhZ2dpbmdPcHRzLFxuICBUYWdzLFxuICBUcmFuc3BvcnQsXG4gIFVwbG9hZGVkT2JqZWN0SW5mbyxcbiAgVXBsb2FkUGFydENvbmZpZyxcbn0gZnJvbSAnLi90eXBlLnRzJ1xuaW1wb3J0IHR5cGUgeyBMaXN0TXVsdGlwYXJ0UmVzdWx0LCBVcGxvYWRlZFBhcnQgfSBmcm9tICcuL3htbC1wYXJzZXIudHMnXG5pbXBvcnQge1xuICBwYXJzZUJ1Y2tldE5vdGlmaWNhdGlvbixcbiAgcGFyc2VDb21wbGV0ZU11bHRpcGFydCxcbiAgcGFyc2VJbml0aWF0ZU11bHRpcGFydCxcbiAgcGFyc2VMaXN0T2JqZWN0cyxcbiAgcGFyc2VMaXN0T2JqZWN0c1YyLFxuICBwYXJzZU9iamVjdExlZ2FsSG9sZENvbmZpZyxcbiAgcGFyc2VTZWxlY3RPYmplY3RDb250ZW50UmVzcG9uc2UsXG4gIHVwbG9hZFBhcnRQYXJzZXIsXG59IGZyb20gJy4veG1sLXBhcnNlci50cydcbmltcG9ydCAqIGFzIHhtbFBhcnNlcnMgZnJvbSAnLi94bWwtcGFyc2VyLnRzJ1xuXG5jb25zdCB4bWwgPSBuZXcgeG1sMmpzLkJ1aWxkZXIoeyByZW5kZXJPcHRzOiB7IHByZXR0eTogZmFsc2UgfSwgaGVhZGxlc3M6IHRydWUgfSlcblxuLy8gd2lsbCBiZSByZXBsYWNlZCBieSBidW5kbGVyLlxuY29uc3QgUGFja2FnZSA9IHsgdmVyc2lvbjogcHJvY2Vzcy5lbnYuTUlOSU9fSlNfUEFDS0FHRV9WRVJTSU9OIHx8ICdkZXZlbG9wbWVudCcgfVxuXG5jb25zdCByZXF1ZXN0T3B0aW9uUHJvcGVydGllcyA9IFtcbiAgJ2FnZW50JyxcbiAgJ2NhJyxcbiAgJ2NlcnQnLFxuICAnY2lwaGVycycsXG4gICdjbGllbnRDZXJ0RW5naW5lJyxcbiAgJ2NybCcsXG4gICdkaHBhcmFtJyxcbiAgJ2VjZGhDdXJ2ZScsXG4gICdmYW1pbHknLFxuICAnaG9ub3JDaXBoZXJPcmRlcicsXG4gICdrZXknLFxuICAncGFzc3BocmFzZScsXG4gICdwZngnLFxuICAncmVqZWN0VW5hdXRob3JpemVkJyxcbiAgJ3NlY3VyZU9wdGlvbnMnLFxuICAnc2VjdXJlUHJvdG9jb2wnLFxuICAnc2VydmVybmFtZScsXG4gICdzZXNzaW9uSWRDb250ZXh0Jyxcbl0gYXMgY29uc3RcblxuZXhwb3J0IGludGVyZmFjZSBSZXRyeU9wdGlvbnMge1xuICAvKipcbiAgICogSWYgdGhpcyBzZXQgdG8gdHJ1ZSwgaXQgd2lsbCB0YWtlIHByZWNlZGVuY2Ugb3ZlciBhbGwgb3RoZXIgcmV0cnkgb3B0aW9ucy5cbiAgICogQGRlZmF1bHQgZmFsc2VcbiAgICovXG4gIGRpc2FibGVSZXRyeT86IGJvb2xlYW5cbiAgLyoqXG4gICAqIFRoZSBtYXhpbXVtIGFtb3VudCBvZiByZXRyaWVzIGZvciBhIHJlcXVlc3QuXG4gICAqIEBkZWZhdWx0IDFcbiAgICovXG4gIG1heGltdW1SZXRyeUNvdW50PzogbnVtYmVyXG4gIC8qKlxuICAgKiBUaGUgbWluaW11bSBkdXJhdGlvbiAoaW4gbWlsbGlzZWNvbmRzKSBmb3IgdGhlIGV4cG9uZW50aWFsIGJhY2tvZmYgYWxnb3JpdGhtLlxuICAgKiBAZGVmYXVsdCAxMDBcbiAgICovXG4gIGJhc2VEZWxheU1zPzogbnVtYmVyXG4gIC8qKlxuICAgKiBUaGUgbWF4aW11bSBkdXJhdGlvbiAoaW4gbWlsbGlzZWNvbmRzKSBmb3IgdGhlIGV4cG9uZW50aWFsIGJhY2tvZmYgYWxnb3JpdGhtLlxuICAgKiBAZGVmYXVsdCA2MDAwMFxuICAgKi9cbiAgbWF4aW11bURlbGF5TXM/OiBudW1iZXJcbn1cblxuZXhwb3J0IGludGVyZmFjZSBDbGllbnRPcHRpb25zIHtcbiAgZW5kUG9pbnQ6IHN0cmluZ1xuICBhY2Nlc3NLZXk/OiBzdHJpbmdcbiAgc2VjcmV0S2V5Pzogc3RyaW5nXG4gIHVzZVNTTD86IGJvb2xlYW5cbiAgcG9ydD86IG51bWJlclxuICByZWdpb24/OiBSZWdpb25cbiAgdHJhbnNwb3J0PzogVHJhbnNwb3J0XG4gIHNlc3Npb25Ub2tlbj86IHN0cmluZ1xuICBwYXJ0U2l6ZT86IG51bWJlclxuICBwYXRoU3R5bGU/OiBib29sZWFuXG4gIGNyZWRlbnRpYWxzUHJvdmlkZXI/OiBDcmVkZW50aWFsUHJvdmlkZXJcbiAgczNBY2NlbGVyYXRlRW5kcG9pbnQ/OiBzdHJpbmdcbiAgdHJhbnNwb3J0QWdlbnQ/OiBodHRwLkFnZW50XG4gIHJldHJ5T3B0aW9ucz86IFJldHJ5T3B0aW9uc1xufVxuXG5leHBvcnQgdHlwZSBSZXF1ZXN0T3B0aW9uID0gUGFydGlhbDxJUmVxdWVzdD4gJiB7XG4gIG1ldGhvZDogc3RyaW5nXG4gIGJ1Y2tldE5hbWU/OiBzdHJpbmdcbiAgb2JqZWN0TmFtZT86IHN0cmluZ1xuICBxdWVyeT86IHN0cmluZ1xuICBwYXRoU3R5bGU/OiBib29sZWFuXG59XG5cbmV4cG9ydCB0eXBlIE5vUmVzdWx0Q2FsbGJhY2sgPSAoZXJyb3I6IHVua25vd24pID0+IHZvaWRcblxuZXhwb3J0IGludGVyZmFjZSBNYWtlQnVja2V0T3B0IHtcbiAgT2JqZWN0TG9ja2luZz86IGJvb2xlYW5cbn1cblxuZXhwb3J0IGludGVyZmFjZSBSZW1vdmVPcHRpb25zIHtcbiAgdmVyc2lvbklkPzogc3RyaW5nXG4gIGdvdmVybmFuY2VCeXBhc3M/OiBib29sZWFuXG4gIGZvcmNlRGVsZXRlPzogYm9vbGVhblxufVxuXG50eXBlIFBhcnQgPSB7XG4gIHBhcnQ6IG51bWJlclxuICBldGFnOiBzdHJpbmdcbn1cblxuZXhwb3J0IGNsYXNzIFR5cGVkQ2xpZW50IHtcbiAgcHJvdGVjdGVkIHRyYW5zcG9ydDogVHJhbnNwb3J0XG4gIHByb3RlY3RlZCBob3N0OiBzdHJpbmdcbiAgcHJvdGVjdGVkIHBvcnQ6IG51bWJlclxuICBwcm90ZWN0ZWQgcHJvdG9jb2w6IHN0cmluZ1xuICBwcm90ZWN0ZWQgYWNjZXNzS2V5OiBzdHJpbmdcbiAgcHJvdGVjdGVkIHNlY3JldEtleTogc3RyaW5nXG4gIHByb3RlY3RlZCBzZXNzaW9uVG9rZW4/OiBzdHJpbmdcbiAgcHJvdGVjdGVkIHVzZXJBZ2VudDogc3RyaW5nXG4gIHByb3RlY3RlZCBhbm9ueW1vdXM6IGJvb2xlYW5cbiAgcHJvdGVjdGVkIHBhdGhTdHlsZTogYm9vbGVhblxuICBwcm90ZWN0ZWQgcmVnaW9uTWFwOiBSZWNvcmQ8c3RyaW5nLCBzdHJpbmc+XG4gIHB1YmxpYyByZWdpb24/OiBzdHJpbmdcbiAgcHJvdGVjdGVkIGNyZWRlbnRpYWxzUHJvdmlkZXI/OiBDcmVkZW50aWFsUHJvdmlkZXJcbiAgcGFydFNpemU6IG51bWJlciA9IDY0ICogMTAyNCAqIDEwMjRcbiAgcHJvdGVjdGVkIG92ZXJSaWRlUGFydFNpemU/OiBib29sZWFuXG4gIHByb3RlY3RlZCByZXRyeU9wdGlvbnM6IFJldHJ5T3B0aW9uc1xuXG4gIHByb3RlY3RlZCBtYXhpbXVtUGFydFNpemUgPSA1ICogMTAyNCAqIDEwMjQgKiAxMDI0XG4gIHByb3RlY3RlZCBtYXhPYmplY3RTaXplID0gNSAqIDEwMjQgKiAxMDI0ICogMTAyNCAqIDEwMjRcbiAgcHVibGljIGVuYWJsZVNIQTI1NjogYm9vbGVhblxuICBwcm90ZWN0ZWQgczNBY2NlbGVyYXRlRW5kcG9pbnQ/OiBzdHJpbmdcbiAgcHJvdGVjdGVkIHJlcU9wdGlvbnM6IFJlY29yZDxzdHJpbmcsIHVua25vd24+XG5cbiAgcHJvdGVjdGVkIHRyYW5zcG9ydEFnZW50OiBodHRwLkFnZW50XG4gIHByaXZhdGUgcmVhZG9ubHkgY2xpZW50RXh0ZW5zaW9uczogRXh0ZW5zaW9uc1xuXG4gIGNvbnN0cnVjdG9yKHBhcmFtczogQ2xpZW50T3B0aW9ucykge1xuICAgIC8vIEB0cy1leHBlY3QtZXJyb3IgZGVwcmVjYXRlZCBwcm9wZXJ0eVxuICAgIGlmIChwYXJhbXMuc2VjdXJlICE9PSB1bmRlZmluZWQpIHtcbiAgICAgIHRocm93IG5ldyBFcnJvcignXCJzZWN1cmVcIiBvcHRpb24gZGVwcmVjYXRlZCwgXCJ1c2VTU0xcIiBzaG91bGQgYmUgdXNlZCBpbnN0ZWFkJylcbiAgICB9XG4gICAgLy8gRGVmYXVsdCB2YWx1ZXMgaWYgbm90IHNwZWNpZmllZC5cbiAgICBpZiAocGFyYW1zLnVzZVNTTCA9PT0gdW5kZWZpbmVkKSB7XG4gICAgICBwYXJhbXMudXNlU1NMID0gdHJ1ZVxuICAgIH1cbiAgICBpZiAoIXBhcmFtcy5wb3J0KSB7XG4gICAgICBwYXJhbXMucG9ydCA9IDBcbiAgICB9XG4gICAgLy8gVmFsaWRhdGUgaW5wdXQgcGFyYW1zLlxuICAgIGlmICghaXNWYWxpZEVuZHBvaW50KHBhcmFtcy5lbmRQb2ludCkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEVuZHBvaW50RXJyb3IoYEludmFsaWQgZW5kUG9pbnQgOiAke3BhcmFtcy5lbmRQb2ludH1gKVxuICAgIH1cbiAgICBpZiAoIWlzVmFsaWRQb3J0KHBhcmFtcy5wb3J0KSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQXJndW1lbnRFcnJvcihgSW52YWxpZCBwb3J0IDogJHtwYXJhbXMucG9ydH1gKVxuICAgIH1cbiAgICBpZiAoIWlzQm9vbGVhbihwYXJhbXMudXNlU1NMKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQXJndW1lbnRFcnJvcihcbiAgICAgICAgYEludmFsaWQgdXNlU1NMIGZsYWcgdHlwZSA6ICR7cGFyYW1zLnVzZVNTTH0sIGV4cGVjdGVkIHRvIGJlIG9mIHR5cGUgXCJib29sZWFuXCJgLFxuICAgICAgKVxuICAgIH1cblxuICAgIC8vIFZhbGlkYXRlIHJlZ2lvbiBvbmx5IGlmIGl0cyBzZXQuXG4gICAgaWYgKHBhcmFtcy5yZWdpb24pIHtcbiAgICAgIGlmICghaXNTdHJpbmcocGFyYW1zLnJlZ2lvbikpIHtcbiAgICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQXJndW1lbnRFcnJvcihgSW52YWxpZCByZWdpb24gOiAke3BhcmFtcy5yZWdpb259YClcbiAgICAgIH1cbiAgICB9XG5cbiAgICBjb25zdCBob3N0ID0gcGFyYW1zLmVuZFBvaW50LnRvTG93ZXJDYXNlKClcbiAgICBsZXQgcG9ydCA9IHBhcmFtcy5wb3J0XG4gICAgbGV0IHByb3RvY29sOiBzdHJpbmdcbiAgICBsZXQgdHJhbnNwb3J0XG4gICAgbGV0IHRyYW5zcG9ydEFnZW50OiBodHRwLkFnZW50XG4gICAgLy8gVmFsaWRhdGUgaWYgY29uZmlndXJhdGlvbiBpcyBub3QgdXNpbmcgU1NMXG4gICAgLy8gZm9yIGNvbnN0cnVjdGluZyByZWxldmFudCBlbmRwb2ludHMuXG4gICAgaWYgKHBhcmFtcy51c2VTU0wpIHtcbiAgICAgIC8vIERlZmF1bHRzIHRvIHNlY3VyZS5cbiAgICAgIHRyYW5zcG9ydCA9IGh0dHBzXG4gICAgICBwcm90b2NvbCA9ICdodHRwczonXG4gICAgICBwb3J0ID0gcG9ydCB8fCA0NDNcbiAgICAgIHRyYW5zcG9ydEFnZW50ID0gaHR0cHMuZ2xvYmFsQWdlbnRcbiAgICB9IGVsc2Uge1xuICAgICAgdHJhbnNwb3J0ID0gaHR0cFxuICAgICAgcHJvdG9jb2wgPSAnaHR0cDonXG4gICAgICBwb3J0ID0gcG9ydCB8fCA4MFxuICAgICAgdHJhbnNwb3J0QWdlbnQgPSBodHRwLmdsb2JhbEFnZW50XG4gICAgfVxuXG4gICAgLy8gaWYgY3VzdG9tIHRyYW5zcG9ydCBpcyBzZXQsIHVzZSBpdC5cbiAgICBpZiAocGFyYW1zLnRyYW5zcG9ydCkge1xuICAgICAgaWYgKCFpc09iamVjdChwYXJhbXMudHJhbnNwb3J0KSkge1xuICAgICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKFxuICAgICAgICAgIGBJbnZhbGlkIHRyYW5zcG9ydCB0eXBlIDogJHtwYXJhbXMudHJhbnNwb3J0fSwgZXhwZWN0ZWQgdG8gYmUgdHlwZSBcIm9iamVjdFwiYCxcbiAgICAgICAgKVxuICAgICAgfVxuICAgICAgdHJhbnNwb3J0ID0gcGFyYW1zLnRyYW5zcG9ydFxuICAgIH1cblxuICAgIC8vIGlmIGN1c3RvbSB0cmFuc3BvcnQgYWdlbnQgaXMgc2V0LCB1c2UgaXQuXG4gICAgaWYgKHBhcmFtcy50cmFuc3BvcnRBZ2VudCkge1xuICAgICAgaWYgKCFpc09iamVjdChwYXJhbXMudHJhbnNwb3J0QWdlbnQpKSB7XG4gICAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEFyZ3VtZW50RXJyb3IoXG4gICAgICAgICAgYEludmFsaWQgdHJhbnNwb3J0QWdlbnQgdHlwZTogJHtwYXJhbXMudHJhbnNwb3J0QWdlbnR9LCBleHBlY3RlZCB0byBiZSB0eXBlIFwib2JqZWN0XCJgLFxuICAgICAgICApXG4gICAgICB9XG5cbiAgICAgIHRyYW5zcG9ydEFnZW50ID0gcGFyYW1zLnRyYW5zcG9ydEFnZW50XG4gICAgfVxuXG4gICAgLy8gVXNlciBBZ2VudCBzaG91bGQgYWx3YXlzIGZvbGxvd2luZyB0aGUgYmVsb3cgc3R5bGUuXG4gICAgLy8gUGxlYXNlIG9wZW4gYW4gaXNzdWUgdG8gZGlzY3VzcyBhbnkgbmV3IGNoYW5nZXMgaGVyZS5cbiAgICAvL1xuICAgIC8vICAgICAgIE1pbklPIChPUzsgQVJDSCkgTElCL1ZFUiBBUFAvVkVSXG4gICAgLy9cbiAgICBjb25zdCBsaWJyYXJ5Q29tbWVudHMgPSBgKCR7cHJvY2Vzcy5wbGF0Zm9ybX07ICR7cHJvY2Vzcy5hcmNofSlgXG4gICAgY29uc3QgbGlicmFyeUFnZW50ID0gYE1pbklPICR7bGlicmFyeUNvbW1lbnRzfSBtaW5pby1qcy8ke1BhY2thZ2UudmVyc2lvbn1gXG4gICAgLy8gVXNlciBhZ2VudCBibG9jayBlbmRzLlxuXG4gICAgdGhpcy50cmFuc3BvcnQgPSB0cmFuc3BvcnRcbiAgICB0aGlzLnRyYW5zcG9ydEFnZW50ID0gdHJhbnNwb3J0QWdlbnRcbiAgICB0aGlzLmhvc3QgPSBob3N0XG4gICAgdGhpcy5wb3J0ID0gcG9ydFxuICAgIHRoaXMucHJvdG9jb2wgPSBwcm90b2NvbFxuICAgIHRoaXMudXNlckFnZW50ID0gYCR7bGlicmFyeUFnZW50fWBcblxuICAgIC8vIERlZmF1bHQgcGF0aCBzdHlsZSBpcyB0cnVlXG4gICAgaWYgKHBhcmFtcy5wYXRoU3R5bGUgPT09IHVuZGVmaW5lZCkge1xuICAgICAgdGhpcy5wYXRoU3R5bGUgPSB0cnVlXG4gICAgfSBlbHNlIHtcbiAgICAgIHRoaXMucGF0aFN0eWxlID0gcGFyYW1zLnBhdGhTdHlsZVxuICAgIH1cblxuICAgIHRoaXMuYWNjZXNzS2V5ID0gcGFyYW1zLmFjY2Vzc0tleSA/PyAnJ1xuICAgIHRoaXMuc2VjcmV0S2V5ID0gcGFyYW1zLnNlY3JldEtleSA/PyAnJ1xuICAgIHRoaXMuc2Vzc2lvblRva2VuID0gcGFyYW1zLnNlc3Npb25Ub2tlblxuICAgIHRoaXMuYW5vbnltb3VzID0gIXRoaXMuYWNjZXNzS2V5IHx8ICF0aGlzLnNlY3JldEtleVxuXG4gICAgaWYgKHBhcmFtcy5jcmVkZW50aWFsc1Byb3ZpZGVyKSB7XG4gICAgICB0aGlzLmFub255bW91cyA9IGZhbHNlXG4gICAgICB0aGlzLmNyZWRlbnRpYWxzUHJvdmlkZXIgPSBwYXJhbXMuY3JlZGVudGlhbHNQcm92aWRlclxuICAgIH1cblxuICAgIHRoaXMucmVnaW9uTWFwID0ge31cbiAgICBpZiAocGFyYW1zLnJlZ2lvbikge1xuICAgICAgdGhpcy5yZWdpb24gPSBwYXJhbXMucmVnaW9uXG4gICAgfVxuXG4gICAgaWYgKHBhcmFtcy5wYXJ0U2l6ZSkge1xuICAgICAgdGhpcy5wYXJ0U2l6ZSA9IHBhcmFtcy5wYXJ0U2l6ZVxuICAgICAgdGhpcy5vdmVyUmlkZVBhcnRTaXplID0gdHJ1ZVxuICAgIH1cbiAgICBpZiAodGhpcy5wYXJ0U2l6ZSA8IDUgKiAxMDI0ICogMTAyNCkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQXJndW1lbnRFcnJvcihgUGFydCBzaXplIHNob3VsZCBiZSBncmVhdGVyIHRoYW4gNU1CYClcbiAgICB9XG4gICAgaWYgKHRoaXMucGFydFNpemUgPiA1ICogMTAyNCAqIDEwMjQgKiAxMDI0KSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKGBQYXJ0IHNpemUgc2hvdWxkIGJlIGxlc3MgdGhhbiA1R0JgKVxuICAgIH1cblxuICAgIC8vIFNIQTI1NiBpcyBlbmFibGVkIG9ubHkgZm9yIGF1dGhlbnRpY2F0ZWQgaHR0cCByZXF1ZXN0cy4gSWYgdGhlIHJlcXVlc3QgaXMgYXV0aGVudGljYXRlZFxuICAgIC8vIGFuZCB0aGUgY29ubmVjdGlvbiBpcyBodHRwcyB3ZSB1c2UgeC1hbXotY29udGVudC1zaGEyNTY9VU5TSUdORUQtUEFZTE9BRFxuICAgIC8vIGhlYWRlciBmb3Igc2lnbmF0dXJlIGNhbGN1bGF0aW9uLlxuICAgIHRoaXMuZW5hYmxlU0hBMjU2ID0gIXRoaXMuYW5vbnltb3VzICYmICFwYXJhbXMudXNlU1NMXG5cbiAgICB0aGlzLnMzQWNjZWxlcmF0ZUVuZHBvaW50ID0gcGFyYW1zLnMzQWNjZWxlcmF0ZUVuZHBvaW50IHx8IHVuZGVmaW5lZFxuICAgIHRoaXMucmVxT3B0aW9ucyA9IHt9XG4gICAgdGhpcy5jbGllbnRFeHRlbnNpb25zID0gbmV3IEV4dGVuc2lvbnModGhpcylcblxuICAgIGlmIChwYXJhbXMucmV0cnlPcHRpb25zKSB7XG4gICAgICBpZiAoIWlzT2JqZWN0KHBhcmFtcy5yZXRyeU9wdGlvbnMpKSB7XG4gICAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEFyZ3VtZW50RXJyb3IoXG4gICAgICAgICAgYEludmFsaWQgcmV0cnlPcHRpb25zIHR5cGU6ICR7cGFyYW1zLnJldHJ5T3B0aW9uc30sIGV4cGVjdGVkIHRvIGJlIHR5cGUgXCJvYmplY3RcImAsXG4gICAgICAgIClcbiAgICAgIH1cblxuICAgICAgdGhpcy5yZXRyeU9wdGlvbnMgPSBwYXJhbXMucmV0cnlPcHRpb25zXG4gICAgfSBlbHNlIHtcbiAgICAgIHRoaXMucmV0cnlPcHRpb25zID0ge1xuICAgICAgICBkaXNhYmxlUmV0cnk6IGZhbHNlLFxuICAgICAgfVxuICAgIH1cbiAgfVxuICAvKipcbiAgICogTWluaW8gZXh0ZW5zaW9ucyB0aGF0IGFyZW4ndCBuZWNlc3NhcnkgcHJlc2VudCBmb3IgQW1hem9uIFMzIGNvbXBhdGlibGUgc3RvcmFnZSBzZXJ2ZXJzXG4gICAqL1xuICBnZXQgZXh0ZW5zaW9ucygpIHtcbiAgICByZXR1cm4gdGhpcy5jbGllbnRFeHRlbnNpb25zXG4gIH1cblxuICAvKipcbiAgICogQHBhcmFtIGVuZFBvaW50IC0gdmFsaWQgUzMgYWNjZWxlcmF0aW9uIGVuZCBwb2ludFxuICAgKi9cbiAgc2V0UzNUcmFuc2ZlckFjY2VsZXJhdGUoZW5kUG9pbnQ6IHN0cmluZykge1xuICAgIHRoaXMuczNBY2NlbGVyYXRlRW5kcG9pbnQgPSBlbmRQb2ludFxuICB9XG5cbiAgLyoqXG4gICAqIFNldHMgdGhlIHN1cHBvcnRlZCByZXF1ZXN0IG9wdGlvbnMuXG4gICAqL1xuICBwdWJsaWMgc2V0UmVxdWVzdE9wdGlvbnMob3B0aW9uczogUGljazxodHRwcy5SZXF1ZXN0T3B0aW9ucywgKHR5cGVvZiByZXF1ZXN0T3B0aW9uUHJvcGVydGllcylbbnVtYmVyXT4pIHtcbiAgICBpZiAoIWlzT2JqZWN0KG9wdGlvbnMpKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdyZXF1ZXN0IG9wdGlvbnMgc2hvdWxkIGJlIG9mIHR5cGUgXCJvYmplY3RcIicpXG4gICAgfVxuICAgIHRoaXMucmVxT3B0aW9ucyA9IF8ucGljayhvcHRpb25zLCByZXF1ZXN0T3B0aW9uUHJvcGVydGllcylcbiAgfVxuXG4gIC8qKlxuICAgKiAgVGhpcyBpcyBzMyBTcGVjaWZpYyBhbmQgZG9lcyBub3QgaG9sZCB2YWxpZGl0eSBpbiBhbnkgb3RoZXIgT2JqZWN0IHN0b3JhZ2UuXG4gICAqL1xuICBwcml2YXRlIGdldEFjY2VsZXJhdGVFbmRQb2ludElmU2V0KGJ1Y2tldE5hbWU/OiBzdHJpbmcsIG9iamVjdE5hbWU/OiBzdHJpbmcpIHtcbiAgICBpZiAoIWlzRW1wdHkodGhpcy5zM0FjY2VsZXJhdGVFbmRwb2ludCkgJiYgIWlzRW1wdHkoYnVja2V0TmFtZSkgJiYgIWlzRW1wdHkob2JqZWN0TmFtZSkpIHtcbiAgICAgIC8vIGh0dHA6Ly9kb2NzLmF3cy5hbWF6b24uY29tL0FtYXpvblMzL2xhdGVzdC9kZXYvdHJhbnNmZXItYWNjZWxlcmF0aW9uLmh0bWxcbiAgICAgIC8vIERpc2FibGUgdHJhbnNmZXIgYWNjZWxlcmF0aW9uIGZvciBub24tY29tcGxpYW50IGJ1Y2tldCBuYW1lcy5cbiAgICAgIGlmIChidWNrZXROYW1lLmluY2x1ZGVzKCcuJykpIHtcbiAgICAgICAgdGhyb3cgbmV3IEVycm9yKGBUcmFuc2ZlciBBY2NlbGVyYXRpb24gaXMgbm90IHN1cHBvcnRlZCBmb3Igbm9uIGNvbXBsaWFudCBidWNrZXQ6JHtidWNrZXROYW1lfWApXG4gICAgICB9XG4gICAgICAvLyBJZiB0cmFuc2ZlciBhY2NlbGVyYXRpb24gaXMgcmVxdWVzdGVkIHNldCBuZXcgaG9zdC5cbiAgICAgIC8vIEZvciBtb3JlIGRldGFpbHMgYWJvdXQgZW5hYmxpbmcgdHJhbnNmZXIgYWNjZWxlcmF0aW9uIHJlYWQgaGVyZS5cbiAgICAgIC8vIGh0dHA6Ly9kb2NzLmF3cy5hbWF6b24uY29tL0FtYXpvblMzL2xhdGVzdC9kZXYvdHJhbnNmZXItYWNjZWxlcmF0aW9uLmh0bWxcbiAgICAgIHJldHVybiB0aGlzLnMzQWNjZWxlcmF0ZUVuZHBvaW50XG4gICAgfVxuICAgIHJldHVybiBmYWxzZVxuICB9XG5cbiAgLyoqXG4gICAqICAgU2V0IGFwcGxpY2F0aW9uIHNwZWNpZmljIGluZm9ybWF0aW9uLlxuICAgKiAgIEdlbmVyYXRlcyBVc2VyLUFnZW50IGluIHRoZSBmb2xsb3dpbmcgc3R5bGUuXG4gICAqICAgTWluSU8gKE9TOyBBUkNIKSBMSUIvVkVSIEFQUC9WRVJcbiAgICovXG4gIHNldEFwcEluZm8oYXBwTmFtZTogc3RyaW5nLCBhcHBWZXJzaW9uOiBzdHJpbmcpIHtcbiAgICBpZiAoIWlzU3RyaW5nKGFwcE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKGBJbnZhbGlkIGFwcE5hbWU6ICR7YXBwTmFtZX1gKVxuICAgIH1cbiAgICBpZiAoYXBwTmFtZS50cmltKCkgPT09ICcnKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKCdJbnB1dCBhcHBOYW1lIGNhbm5vdCBiZSBlbXB0eS4nKVxuICAgIH1cbiAgICBpZiAoIWlzU3RyaW5nKGFwcFZlcnNpb24pKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKGBJbnZhbGlkIGFwcFZlcnNpb246ICR7YXBwVmVyc2lvbn1gKVxuICAgIH1cbiAgICBpZiAoYXBwVmVyc2lvbi50cmltKCkgPT09ICcnKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKCdJbnB1dCBhcHBWZXJzaW9uIGNhbm5vdCBiZSBlbXB0eS4nKVxuICAgIH1cbiAgICB0aGlzLnVzZXJBZ2VudCA9IGAke3RoaXMudXNlckFnZW50fSAke2FwcE5hbWV9LyR7YXBwVmVyc2lvbn1gXG4gIH1cblxuICAvKipcbiAgICogcmV0dXJucyBvcHRpb25zIG9iamVjdCB0aGF0IGNhbiBiZSB1c2VkIHdpdGggaHR0cC5yZXF1ZXN0KClcbiAgICogVGFrZXMgY2FyZSBvZiBjb25zdHJ1Y3RpbmcgdmlydHVhbC1ob3N0LXN0eWxlIG9yIHBhdGgtc3R5bGUgaG9zdG5hbWVcbiAgICovXG4gIHByb3RlY3RlZCBnZXRSZXF1ZXN0T3B0aW9ucyhcbiAgICBvcHRzOiBSZXF1ZXN0T3B0aW9uICYge1xuICAgICAgcmVnaW9uOiBzdHJpbmdcbiAgICB9LFxuICApOiBJUmVxdWVzdCAmIHtcbiAgICBob3N0OiBzdHJpbmdcbiAgICBoZWFkZXJzOiBSZWNvcmQ8c3RyaW5nLCBzdHJpbmc+XG4gIH0ge1xuICAgIGNvbnN0IG1ldGhvZCA9IG9wdHMubWV0aG9kXG4gICAgY29uc3QgcmVnaW9uID0gb3B0cy5yZWdpb25cbiAgICBjb25zdCBidWNrZXROYW1lID0gb3B0cy5idWNrZXROYW1lXG4gICAgbGV0IG9iamVjdE5hbWUgPSBvcHRzLm9iamVjdE5hbWVcbiAgICBjb25zdCBoZWFkZXJzID0gb3B0cy5oZWFkZXJzXG4gICAgY29uc3QgcXVlcnkgPSBvcHRzLnF1ZXJ5XG5cbiAgICBsZXQgcmVxT3B0aW9ucyA9IHtcbiAgICAgIG1ldGhvZCxcbiAgICAgIGhlYWRlcnM6IHt9IGFzIFJlcXVlc3RIZWFkZXJzLFxuICAgICAgcHJvdG9jb2w6IHRoaXMucHJvdG9jb2wsXG4gICAgICAvLyBJZiBjdXN0b20gdHJhbnNwb3J0QWdlbnQgd2FzIHN1cHBsaWVkIGVhcmxpZXIsIHdlJ2xsIGluamVjdCBpdCBoZXJlXG4gICAgICBhZ2VudDogdGhpcy50cmFuc3BvcnRBZ2VudCxcbiAgICB9XG5cbiAgICAvLyBWZXJpZnkgaWYgdmlydHVhbCBob3N0IHN1cHBvcnRlZC5cbiAgICBsZXQgdmlydHVhbEhvc3RTdHlsZVxuICAgIGlmIChidWNrZXROYW1lKSB7XG4gICAgICB2aXJ0dWFsSG9zdFN0eWxlID0gaXNWaXJ0dWFsSG9zdFN0eWxlKHRoaXMuaG9zdCwgdGhpcy5wcm90b2NvbCwgYnVja2V0TmFtZSwgdGhpcy5wYXRoU3R5bGUpXG4gICAgfVxuXG4gICAgbGV0IHBhdGggPSAnLydcbiAgICBsZXQgaG9zdCA9IHRoaXMuaG9zdFxuXG4gICAgbGV0IHBvcnQ6IHVuZGVmaW5lZCB8IG51bWJlclxuICAgIGlmICh0aGlzLnBvcnQpIHtcbiAgICAgIHBvcnQgPSB0aGlzLnBvcnRcbiAgICB9XG5cbiAgICBpZiAob2JqZWN0TmFtZSkge1xuICAgICAgb2JqZWN0TmFtZSA9IHVyaVJlc291cmNlRXNjYXBlKG9iamVjdE5hbWUpXG4gICAgfVxuXG4gICAgLy8gRm9yIEFtYXpvbiBTMyBlbmRwb2ludCwgZ2V0IGVuZHBvaW50IGJhc2VkIG9uIHJlZ2lvbi5cbiAgICBpZiAoaXNBbWF6b25FbmRwb2ludChob3N0KSkge1xuICAgICAgY29uc3QgYWNjZWxlcmF0ZUVuZFBvaW50ID0gdGhpcy5nZXRBY2NlbGVyYXRlRW5kUG9pbnRJZlNldChidWNrZXROYW1lLCBvYmplY3ROYW1lKVxuICAgICAgaWYgKGFjY2VsZXJhdGVFbmRQb2ludCkge1xuICAgICAgICBob3N0ID0gYCR7YWNjZWxlcmF0ZUVuZFBvaW50fWBcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIGhvc3QgPSBnZXRTM0VuZHBvaW50KHJlZ2lvbilcbiAgICAgIH1cbiAgICB9XG5cbiAgICBpZiAodmlydHVhbEhvc3RTdHlsZSAmJiAhb3B0cy5wYXRoU3R5bGUpIHtcbiAgICAgIC8vIEZvciBhbGwgaG9zdHMgd2hpY2ggc3VwcG9ydCB2aXJ0dWFsIGhvc3Qgc3R5bGUsIGBidWNrZXROYW1lYFxuICAgICAgLy8gaXMgcGFydCBvZiB0aGUgaG9zdG5hbWUgaW4gdGhlIGZvbGxvd2luZyBmb3JtYXQ6XG4gICAgICAvL1xuICAgICAgLy8gIHZhciBob3N0ID0gJ2J1Y2tldE5hbWUuZXhhbXBsZS5jb20nXG4gICAgICAvL1xuICAgICAgaWYgKGJ1Y2tldE5hbWUpIHtcbiAgICAgICAgaG9zdCA9IGAke2J1Y2tldE5hbWV9LiR7aG9zdH1gXG4gICAgICB9XG4gICAgICBpZiAob2JqZWN0TmFtZSkge1xuICAgICAgICBwYXRoID0gYC8ke29iamVjdE5hbWV9YFxuICAgICAgfVxuICAgIH0gZWxzZSB7XG4gICAgICAvLyBGb3IgYWxsIFMzIGNvbXBhdGlibGUgc3RvcmFnZSBzZXJ2aWNlcyB3ZSB3aWxsIGZhbGxiYWNrIHRvXG4gICAgICAvLyBwYXRoIHN0eWxlIHJlcXVlc3RzLCB3aGVyZSBgYnVja2V0TmFtZWAgaXMgcGFydCBvZiB0aGUgVVJJXG4gICAgICAvLyBwYXRoLlxuICAgICAgaWYgKGJ1Y2tldE5hbWUpIHtcbiAgICAgICAgcGF0aCA9IGAvJHtidWNrZXROYW1lfWBcbiAgICAgIH1cbiAgICAgIGlmIChvYmplY3ROYW1lKSB7XG4gICAgICAgIHBhdGggPSBgLyR7YnVja2V0TmFtZX0vJHtvYmplY3ROYW1lfWBcbiAgICAgIH1cbiAgICB9XG5cbiAgICBpZiAocXVlcnkpIHtcbiAgICAgIHBhdGggKz0gYD8ke3F1ZXJ5fWBcbiAgICB9XG4gICAgcmVxT3B0aW9ucy5oZWFkZXJzLmhvc3QgPSBob3N0XG4gICAgaWYgKChyZXFPcHRpb25zLnByb3RvY29sID09PSAnaHR0cDonICYmIHBvcnQgIT09IDgwKSB8fCAocmVxT3B0aW9ucy5wcm90b2NvbCA9PT0gJ2h0dHBzOicgJiYgcG9ydCAhPT0gNDQzKSkge1xuICAgICAgcmVxT3B0aW9ucy5oZWFkZXJzLmhvc3QgPSBqb2luSG9zdFBvcnQoaG9zdCwgcG9ydClcbiAgICB9XG5cbiAgICByZXFPcHRpb25zLmhlYWRlcnNbJ3VzZXItYWdlbnQnXSA9IHRoaXMudXNlckFnZW50XG4gICAgaWYgKGhlYWRlcnMpIHtcbiAgICAgIC8vIGhhdmUgYWxsIGhlYWRlciBrZXlzIGluIGxvd2VyIGNhc2UgLSB0byBtYWtlIHNpZ25pbmcgZWFzeVxuICAgICAgZm9yIChjb25zdCBbaywgdl0gb2YgT2JqZWN0LmVudHJpZXMoaGVhZGVycykpIHtcbiAgICAgICAgcmVxT3B0aW9ucy5oZWFkZXJzW2sudG9Mb3dlckNhc2UoKV0gPSB2XG4gICAgICB9XG4gICAgfVxuXG4gICAgLy8gVXNlIGFueSByZXF1ZXN0IG9wdGlvbiBzcGVjaWZpZWQgaW4gbWluaW9DbGllbnQuc2V0UmVxdWVzdE9wdGlvbnMoKVxuICAgIHJlcU9wdGlvbnMgPSBPYmplY3QuYXNzaWduKHt9LCB0aGlzLnJlcU9wdGlvbnMsIHJlcU9wdGlvbnMpXG5cbiAgICByZXR1cm4ge1xuICAgICAgLi4ucmVxT3B0aW9ucyxcbiAgICAgIGhlYWRlcnM6IF8ubWFwVmFsdWVzKF8ucGlja0J5KHJlcU9wdGlvbnMuaGVhZGVycywgaXNEZWZpbmVkKSwgKHYpID0+IHYudG9TdHJpbmcoKSksXG4gICAgICBob3N0LFxuICAgICAgcG9ydCxcbiAgICAgIHBhdGgsXG4gICAgfSBzYXRpc2ZpZXMgaHR0cHMuUmVxdWVzdE9wdGlvbnNcbiAgfVxuXG4gIHB1YmxpYyBhc3luYyBzZXRDcmVkZW50aWFsc1Byb3ZpZGVyKGNyZWRlbnRpYWxzUHJvdmlkZXI6IENyZWRlbnRpYWxQcm92aWRlcikge1xuICAgIGlmICghKGNyZWRlbnRpYWxzUHJvdmlkZXIgaW5zdGFuY2VvZiBDcmVkZW50aWFsUHJvdmlkZXIpKSB7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoJ1VuYWJsZSB0byBnZXQgY3JlZGVudGlhbHMuIEV4cGVjdGVkIGluc3RhbmNlIG9mIENyZWRlbnRpYWxQcm92aWRlcicpXG4gICAgfVxuICAgIHRoaXMuY3JlZGVudGlhbHNQcm92aWRlciA9IGNyZWRlbnRpYWxzUHJvdmlkZXJcbiAgICBhd2FpdCB0aGlzLmNoZWNrQW5kUmVmcmVzaENyZWRzKClcbiAgfVxuXG4gIHByaXZhdGUgYXN5bmMgY2hlY2tBbmRSZWZyZXNoQ3JlZHMoKSB7XG4gICAgaWYgKHRoaXMuY3JlZGVudGlhbHNQcm92aWRlcikge1xuICAgICAgdHJ5IHtcbiAgICAgICAgY29uc3QgY3JlZGVudGlhbHNDb25mID0gYXdhaXQgdGhpcy5jcmVkZW50aWFsc1Byb3ZpZGVyLmdldENyZWRlbnRpYWxzKClcbiAgICAgICAgdGhpcy5hY2Nlc3NLZXkgPSBjcmVkZW50aWFsc0NvbmYuZ2V0QWNjZXNzS2V5KClcbiAgICAgICAgdGhpcy5zZWNyZXRLZXkgPSBjcmVkZW50aWFsc0NvbmYuZ2V0U2VjcmV0S2V5KClcbiAgICAgICAgdGhpcy5zZXNzaW9uVG9rZW4gPSBjcmVkZW50aWFsc0NvbmYuZ2V0U2Vzc2lvblRva2VuKClcbiAgICAgIH0gY2F0Y2ggKGUpIHtcbiAgICAgICAgdGhyb3cgbmV3IEVycm9yKGBVbmFibGUgdG8gZ2V0IGNyZWRlbnRpYWxzOiAke2V9YCwgeyBjYXVzZTogZSB9KVxuICAgICAgfVxuICAgIH1cbiAgfVxuXG4gIHByaXZhdGUgbG9nU3RyZWFtPzogc3RyZWFtLldyaXRhYmxlXG5cbiAgLyoqXG4gICAqIGxvZyB0aGUgcmVxdWVzdCwgcmVzcG9uc2UsIGVycm9yXG4gICAqL1xuICBwcml2YXRlIGxvZ0hUVFAocmVxT3B0aW9uczogSVJlcXVlc3QsIHJlc3BvbnNlOiBodHRwLkluY29taW5nTWVzc2FnZSB8IG51bGwsIGVycj86IHVua25vd24pIHtcbiAgICAvLyBpZiBubyBsb2dTdHJlYW0gYXZhaWxhYmxlIHJldHVybi5cbiAgICBpZiAoIXRoaXMubG9nU3RyZWFtKSB7XG4gICAgICByZXR1cm5cbiAgICB9XG4gICAgaWYgKCFpc09iamVjdChyZXFPcHRpb25zKSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcigncmVxT3B0aW9ucyBzaG91bGQgYmUgb2YgdHlwZSBcIm9iamVjdFwiJylcbiAgICB9XG4gICAgaWYgKHJlc3BvbnNlICYmICFpc1JlYWRhYmxlU3RyZWFtKHJlc3BvbnNlKSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcigncmVzcG9uc2Ugc2hvdWxkIGJlIG9mIHR5cGUgXCJTdHJlYW1cIicpXG4gICAgfVxuICAgIGlmIChlcnIgJiYgIShlcnIgaW5zdGFuY2VvZiBFcnJvcikpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ2VyciBzaG91bGQgYmUgb2YgdHlwZSBcIkVycm9yXCInKVxuICAgIH1cbiAgICBjb25zdCBsb2dTdHJlYW0gPSB0aGlzLmxvZ1N0cmVhbVxuICAgIGNvbnN0IGxvZ0hlYWRlcnMgPSAoaGVhZGVyczogUmVxdWVzdEhlYWRlcnMpID0+IHtcbiAgICAgIE9iamVjdC5lbnRyaWVzKGhlYWRlcnMpLmZvckVhY2goKFtrLCB2XSkgPT4ge1xuICAgICAgICBpZiAoayA9PSAnYXV0aG9yaXphdGlvbicpIHtcbiAgICAgICAgICBpZiAoaXNTdHJpbmcodikpIHtcbiAgICAgICAgICAgIGNvbnN0IHJlZGFjdG9yID0gbmV3IFJlZ0V4cCgnU2lnbmF0dXJlPShbMC05YS1mXSspJylcbiAgICAgICAgICAgIHYgPSB2LnJlcGxhY2UocmVkYWN0b3IsICdTaWduYXR1cmU9KipSRURBQ1RFRCoqJylcbiAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICAgICAgbG9nU3RyZWFtLndyaXRlKGAke2t9OiAke3Z9XFxuYClcbiAgICAgIH0pXG4gICAgICBsb2dTdHJlYW0ud3JpdGUoJ1xcbicpXG4gICAgfVxuICAgIGxvZ1N0cmVhbS53cml0ZShgUkVRVUVTVDogJHtyZXFPcHRpb25zLm1ldGhvZH0gJHtyZXFPcHRpb25zLnBhdGh9XFxuYClcbiAgICBsb2dIZWFkZXJzKHJlcU9wdGlvbnMuaGVhZGVycylcbiAgICBpZiAocmVzcG9uc2UpIHtcbiAgICAgIHRoaXMubG9nU3RyZWFtLndyaXRlKGBSRVNQT05TRTogJHtyZXNwb25zZS5zdGF0dXNDb2RlfVxcbmApXG4gICAgICBsb2dIZWFkZXJzKHJlc3BvbnNlLmhlYWRlcnMgYXMgUmVxdWVzdEhlYWRlcnMpXG4gICAgfVxuICAgIGlmIChlcnIpIHtcbiAgICAgIGxvZ1N0cmVhbS53cml0ZSgnRVJST1IgQk9EWTpcXG4nKVxuICAgICAgY29uc3QgZXJySlNPTiA9IEpTT04uc3RyaW5naWZ5KGVyciwgbnVsbCwgJ1xcdCcpXG4gICAgICBsb2dTdHJlYW0ud3JpdGUoYCR7ZXJySlNPTn1cXG5gKVxuICAgIH1cbiAgfVxuXG4gIC8qKlxuICAgKiBFbmFibGUgdHJhY2luZ1xuICAgKi9cbiAgcHVibGljIHRyYWNlT24oc3RyZWFtPzogc3RyZWFtLldyaXRhYmxlKSB7XG4gICAgaWYgKCFzdHJlYW0pIHtcbiAgICAgIHN0cmVhbSA9IHByb2Nlc3Muc3Rkb3V0XG4gICAgfVxuICAgIHRoaXMubG9nU3RyZWFtID0gc3RyZWFtXG4gIH1cblxuICAvKipcbiAgICogRGlzYWJsZSB0cmFjaW5nXG4gICAqL1xuICBwdWJsaWMgdHJhY2VPZmYoKSB7XG4gICAgdGhpcy5sb2dTdHJlYW0gPSB1bmRlZmluZWRcbiAgfVxuXG4gIC8qKlxuICAgKiBtYWtlUmVxdWVzdCBpcyB0aGUgcHJpbWl0aXZlIHVzZWQgYnkgdGhlIGFwaXMgZm9yIG1ha2luZyBTMyByZXF1ZXN0cy5cbiAgICogcGF5bG9hZCBjYW4gYmUgZW1wdHkgc3RyaW5nIGluIGNhc2Ugb2Ygbm8gcGF5bG9hZC5cbiAgICogc3RhdHVzQ29kZSBpcyB0aGUgZXhwZWN0ZWQgc3RhdHVzQ29kZS4gSWYgcmVzcG9uc2Uuc3RhdHVzQ29kZSBkb2VzIG5vdCBtYXRjaFxuICAgKiB3ZSBwYXJzZSB0aGUgWE1MIGVycm9yIGFuZCBjYWxsIHRoZSBjYWxsYmFjayB3aXRoIHRoZSBlcnJvciBtZXNzYWdlLlxuICAgKlxuICAgKiBBIHZhbGlkIHJlZ2lvbiBpcyBwYXNzZWQgYnkgdGhlIGNhbGxzIC0gbGlzdEJ1Y2tldHMsIG1ha2VCdWNrZXQgYW5kIGdldEJ1Y2tldFJlZ2lvbi5cbiAgICpcbiAgICogQGludGVybmFsXG4gICAqL1xuICBhc3luYyBtYWtlUmVxdWVzdEFzeW5jKFxuICAgIG9wdGlvbnM6IFJlcXVlc3RPcHRpb24sXG4gICAgcGF5bG9hZDogQmluYXJ5ID0gJycsXG4gICAgZXhwZWN0ZWRDb2RlczogbnVtYmVyW10gPSBbMjAwXSxcbiAgICByZWdpb24gPSAnJyxcbiAgKTogUHJvbWlzZTxodHRwLkluY29taW5nTWVzc2FnZT4ge1xuICAgIGlmICghaXNPYmplY3Qob3B0aW9ucykpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ29wdGlvbnMgc2hvdWxkIGJlIG9mIHR5cGUgXCJvYmplY3RcIicpXG4gICAgfVxuICAgIGlmICghaXNTdHJpbmcocGF5bG9hZCkgJiYgIWlzT2JqZWN0KHBheWxvYWQpKSB7XG4gICAgICAvLyBCdWZmZXIgaXMgb2YgdHlwZSAnb2JqZWN0J1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcigncGF5bG9hZCBzaG91bGQgYmUgb2YgdHlwZSBcInN0cmluZ1wiIG9yIFwiQnVmZmVyXCInKVxuICAgIH1cbiAgICBleHBlY3RlZENvZGVzLmZvckVhY2goKHN0YXR1c0NvZGUpID0+IHtcbiAgICAgIGlmICghaXNOdW1iZXIoc3RhdHVzQ29kZSkpIHtcbiAgICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcignc3RhdHVzQ29kZSBzaG91bGQgYmUgb2YgdHlwZSBcIm51bWJlclwiJylcbiAgICAgIH1cbiAgICB9KVxuICAgIGlmICghaXNTdHJpbmcocmVnaW9uKSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcigncmVnaW9uIHNob3VsZCBiZSBvZiB0eXBlIFwic3RyaW5nXCInKVxuICAgIH1cbiAgICBpZiAoIW9wdGlvbnMuaGVhZGVycykge1xuICAgICAgb3B0aW9ucy5oZWFkZXJzID0ge31cbiAgICB9XG4gICAgaWYgKG9wdGlvbnMubWV0aG9kID09PSAnUE9TVCcgfHwgb3B0aW9ucy5tZXRob2QgPT09ICdQVVQnIHx8IG9wdGlvbnMubWV0aG9kID09PSAnREVMRVRFJykge1xuICAgICAgb3B0aW9ucy5oZWFkZXJzWydjb250ZW50LWxlbmd0aCddID0gcGF5bG9hZC5sZW5ndGgudG9TdHJpbmcoKVxuICAgIH1cbiAgICBjb25zdCBzaGEyNTZzdW0gPSB0aGlzLmVuYWJsZVNIQTI1NiA/IHRvU2hhMjU2KHBheWxvYWQpIDogJydcbiAgICByZXR1cm4gdGhpcy5tYWtlUmVxdWVzdFN0cmVhbUFzeW5jKG9wdGlvbnMsIHBheWxvYWQsIHNoYTI1NnN1bSwgZXhwZWN0ZWRDb2RlcywgcmVnaW9uKVxuICB9XG5cbiAgLyoqXG4gICAqIG5ldyByZXF1ZXN0IHdpdGggcHJvbWlzZVxuICAgKlxuICAgKiBObyBuZWVkIHRvIGRyYWluIHJlc3BvbnNlLCByZXNwb25zZSBib2R5IGlzIG5vdCB2YWxpZFxuICAgKi9cbiAgYXN5bmMgbWFrZVJlcXVlc3RBc3luY09taXQoXG4gICAgb3B0aW9uczogUmVxdWVzdE9wdGlvbixcbiAgICBwYXlsb2FkOiBCaW5hcnkgPSAnJyxcbiAgICBzdGF0dXNDb2RlczogbnVtYmVyW10gPSBbMjAwXSxcbiAgICByZWdpb24gPSAnJyxcbiAgKTogUHJvbWlzZTxPbWl0PGh0dHAuSW5jb21pbmdNZXNzYWdlLCAnb24nPj4ge1xuICAgIGNvbnN0IHJlcyA9IGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luYyhvcHRpb25zLCBwYXlsb2FkLCBzdGF0dXNDb2RlcywgcmVnaW9uKVxuICAgIGF3YWl0IGRyYWluUmVzcG9uc2UocmVzKVxuICAgIHJldHVybiByZXNcbiAgfVxuXG4gIC8qKlxuICAgKiBtYWtlUmVxdWVzdFN0cmVhbSB3aWxsIGJlIHVzZWQgZGlyZWN0bHkgaW5zdGVhZCBvZiBtYWtlUmVxdWVzdCBpbiBjYXNlIHRoZSBwYXlsb2FkXG4gICAqIGlzIGF2YWlsYWJsZSBhcyBhIHN0cmVhbS4gZm9yIGV4LiBwdXRPYmplY3RcbiAgICpcbiAgICogQGludGVybmFsXG4gICAqL1xuICBhc3luYyBtYWtlUmVxdWVzdFN0cmVhbUFzeW5jKFxuICAgIG9wdGlvbnM6IFJlcXVlc3RPcHRpb24sXG4gICAgYm9keTogc3RyZWFtLlJlYWRhYmxlIHwgQmluYXJ5LFxuICAgIHNoYTI1NnN1bTogc3RyaW5nLFxuICAgIHN0YXR1c0NvZGVzOiBudW1iZXJbXSxcbiAgICByZWdpb246IHN0cmluZyxcbiAgKTogUHJvbWlzZTxodHRwLkluY29taW5nTWVzc2FnZT4ge1xuICAgIGlmICghaXNPYmplY3Qob3B0aW9ucykpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ29wdGlvbnMgc2hvdWxkIGJlIG9mIHR5cGUgXCJvYmplY3RcIicpXG4gICAgfVxuICAgIGlmICghKEJ1ZmZlci5pc0J1ZmZlcihib2R5KSB8fCB0eXBlb2YgYm9keSA9PT0gJ3N0cmluZycgfHwgaXNSZWFkYWJsZVN0cmVhbShib2R5KSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEFyZ3VtZW50RXJyb3IoXG4gICAgICAgIGBzdHJlYW0gc2hvdWxkIGJlIGEgQnVmZmVyLCBzdHJpbmcgb3IgcmVhZGFibGUgU3RyZWFtLCBnb3QgJHt0eXBlb2YgYm9keX0gaW5zdGVhZGAsXG4gICAgICApXG4gICAgfVxuICAgIGlmICghaXNTdHJpbmcoc2hhMjU2c3VtKSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcignc2hhMjU2c3VtIHNob3VsZCBiZSBvZiB0eXBlIFwic3RyaW5nXCInKVxuICAgIH1cbiAgICBzdGF0dXNDb2Rlcy5mb3JFYWNoKChzdGF0dXNDb2RlKSA9PiB7XG4gICAgICBpZiAoIWlzTnVtYmVyKHN0YXR1c0NvZGUpKSB7XG4gICAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ3N0YXR1c0NvZGUgc2hvdWxkIGJlIG9mIHR5cGUgXCJudW1iZXJcIicpXG4gICAgICB9XG4gICAgfSlcbiAgICBpZiAoIWlzU3RyaW5nKHJlZ2lvbikpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ3JlZ2lvbiBzaG91bGQgYmUgb2YgdHlwZSBcInN0cmluZ1wiJylcbiAgICB9XG4gICAgLy8gc2hhMjU2c3VtIHdpbGwgYmUgZW1wdHkgZm9yIGFub255bW91cyBvciBodHRwcyByZXF1ZXN0c1xuICAgIGlmICghdGhpcy5lbmFibGVTSEEyNTYgJiYgc2hhMjU2c3VtLmxlbmd0aCAhPT0gMCkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQXJndW1lbnRFcnJvcihgc2hhMjU2c3VtIGV4cGVjdGVkIHRvIGJlIGVtcHR5IGZvciBhbm9ueW1vdXMgb3IgaHR0cHMgcmVxdWVzdHNgKVxuICAgIH1cbiAgICAvLyBzaGEyNTZzdW0gc2hvdWxkIGJlIHZhbGlkIGZvciBub24tYW5vbnltb3VzIGh0dHAgcmVxdWVzdHMuXG4gICAgaWYgKHRoaXMuZW5hYmxlU0hBMjU2ICYmIHNoYTI1NnN1bS5sZW5ndGggIT09IDY0KSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKGBJbnZhbGlkIHNoYTI1NnN1bSA6ICR7c2hhMjU2c3VtfWApXG4gICAgfVxuXG4gICAgYXdhaXQgdGhpcy5jaGVja0FuZFJlZnJlc2hDcmVkcygpXG5cbiAgICAvLyBlc2xpbnQtZGlzYWJsZS1uZXh0LWxpbmUgQHR5cGVzY3JpcHQtZXNsaW50L25vLW5vbi1udWxsLWFzc2VydGlvblxuICAgIHJlZ2lvbiA9IHJlZ2lvbiB8fCAoYXdhaXQgdGhpcy5nZXRCdWNrZXRSZWdpb25Bc3luYyhvcHRpb25zLmJ1Y2tldE5hbWUhKSlcblxuICAgIGNvbnN0IHJlcU9wdGlvbnMgPSB0aGlzLmdldFJlcXVlc3RPcHRpb25zKHsgLi4ub3B0aW9ucywgcmVnaW9uIH0pXG4gICAgaWYgKCF0aGlzLmFub255bW91cykge1xuICAgICAgLy8gRm9yIG5vbi1hbm9ueW1vdXMgaHR0cHMgcmVxdWVzdHMgc2hhMjU2c3VtIGlzICdVTlNJR05FRC1QQVlMT0FEJyBmb3Igc2lnbmF0dXJlIGNhbGN1bGF0aW9uLlxuICAgICAgaWYgKCF0aGlzLmVuYWJsZVNIQTI1Nikge1xuICAgICAgICBzaGEyNTZzdW0gPSAnVU5TSUdORUQtUEFZTE9BRCdcbiAgICAgIH1cbiAgICAgIGNvbnN0IGRhdGUgPSBuZXcgRGF0ZSgpXG4gICAgICByZXFPcHRpb25zLmhlYWRlcnNbJ3gtYW16LWRhdGUnXSA9IG1ha2VEYXRlTG9uZyhkYXRlKVxuICAgICAgcmVxT3B0aW9ucy5oZWFkZXJzWyd4LWFtei1jb250ZW50LXNoYTI1NiddID0gc2hhMjU2c3VtXG4gICAgICBpZiAodGhpcy5zZXNzaW9uVG9rZW4pIHtcbiAgICAgICAgcmVxT3B0aW9ucy5oZWFkZXJzWyd4LWFtei1zZWN1cml0eS10b2tlbiddID0gdGhpcy5zZXNzaW9uVG9rZW5cbiAgICAgIH1cbiAgICAgIHJlcU9wdGlvbnMuaGVhZGVycy5hdXRob3JpemF0aW9uID0gc2lnblY0KHJlcU9wdGlvbnMsIHRoaXMuYWNjZXNzS2V5LCB0aGlzLnNlY3JldEtleSwgcmVnaW9uLCBkYXRlLCBzaGEyNTZzdW0pXG4gICAgfVxuXG4gICAgY29uc3QgbG9nU3RyZWFtID0gdGhpcy5sb2dTdHJlYW1cbiAgICBjb25zdCBsb2cgPSBsb2dTdHJlYW0gPyAobXNnOiBzdHJpbmcpID0+IGxvZ1N0cmVhbS53cml0ZShgJHttc2d9XFxuYCkgOiAoKSA9PiB7fVxuXG4gICAgY29uc3QgcmVzcG9uc2UgPSBhd2FpdCByZXF1ZXN0V2l0aFJldHJ5KFxuICAgICAgdGhpcy50cmFuc3BvcnQsXG4gICAgICByZXFPcHRpb25zLFxuICAgICAgYm9keSxcbiAgICAgIHRoaXMucmV0cnlPcHRpb25zLmRpc2FibGVSZXRyeSA9PT0gdHJ1ZSA/IDAgOiB0aGlzLnJldHJ5T3B0aW9ucy5tYXhpbXVtUmV0cnlDb3VudCxcbiAgICAgIHRoaXMucmV0cnlPcHRpb25zLmJhc2VEZWxheU1zLFxuICAgICAgdGhpcy5yZXRyeU9wdGlvbnMubWF4aW11bURlbGF5TXMsXG4gICAgICBsb2csXG4gICAgKVxuICAgIGlmICghcmVzcG9uc2Uuc3RhdHVzQ29kZSkge1xuICAgICAgdGhyb3cgbmV3IEVycm9yKFwiQlVHOiByZXNwb25zZSBkb2Vzbid0IGhhdmUgYSBzdGF0dXNDb2RlXCIpXG4gICAgfVxuXG4gICAgaWYgKCFzdGF0dXNDb2Rlcy5pbmNsdWRlcyhyZXNwb25zZS5zdGF0dXNDb2RlKSkge1xuICAgICAgLy8gRm9yIGFuIGluY29ycmVjdCByZWdpb24sIFMzIHNlcnZlciBhbHdheXMgc2VuZHMgYmFjayA0MDAuXG4gICAgICAvLyBCdXQgd2Ugd2lsbCBkbyBjYWNoZSBpbnZhbGlkYXRpb24gZm9yIGFsbCBlcnJvcnMgc28gdGhhdCxcbiAgICAgIC8vIGluIGZ1dHVyZSwgaWYgQVdTIFMzIGRlY2lkZXMgdG8gc2VuZCBhIGRpZmZlcmVudCBzdGF0dXMgY29kZSBvclxuICAgICAgLy8gWE1MIGVycm9yIGNvZGUgd2Ugd2lsbCBzdGlsbCB3b3JrIGZpbmUuXG4gICAgICAvLyBlc2xpbnQtZGlzYWJsZS1uZXh0LWxpbmUgQHR5cGVzY3JpcHQtZXNsaW50L25vLW5vbi1udWxsLWFzc2VydGlvblxuICAgICAgZGVsZXRlIHRoaXMucmVnaW9uTWFwW29wdGlvbnMuYnVja2V0TmFtZSFdXG5cbiAgICAgIGNvbnN0IGVyciA9IGF3YWl0IHhtbFBhcnNlcnMucGFyc2VSZXNwb25zZUVycm9yKHJlc3BvbnNlKVxuICAgICAgdGhpcy5sb2dIVFRQKHJlcU9wdGlvbnMsIHJlc3BvbnNlLCBlcnIpXG4gICAgICB0aHJvdyBlcnJcbiAgICB9XG5cbiAgICB0aGlzLmxvZ0hUVFAocmVxT3B0aW9ucywgcmVzcG9uc2UpXG5cbiAgICByZXR1cm4gcmVzcG9uc2VcbiAgfVxuXG4gIC8qKlxuICAgKiBnZXRzIHRoZSByZWdpb24gb2YgdGhlIGJ1Y2tldFxuICAgKlxuICAgKiBAcGFyYW0gYnVja2V0TmFtZVxuICAgKlxuICAgKi9cbiAgYXN5bmMgZ2V0QnVja2V0UmVnaW9uQXN5bmMoYnVja2V0TmFtZTogc3RyaW5nKTogUHJvbWlzZTxzdHJpbmc+IHtcbiAgICBpZiAoIWlzVmFsaWRCdWNrZXROYW1lKGJ1Y2tldE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IoYEludmFsaWQgYnVja2V0IG5hbWUgOiAke2J1Y2tldE5hbWV9YClcbiAgICB9XG5cbiAgICAvLyBSZWdpb24gaXMgc2V0IHdpdGggY29uc3RydWN0b3IsIHJldHVybiB0aGUgcmVnaW9uIHJpZ2h0IGhlcmUuXG4gICAgaWYgKHRoaXMucmVnaW9uKSB7XG4gICAgICByZXR1cm4gdGhpcy5yZWdpb25cbiAgICB9XG5cbiAgICBjb25zdCBjYWNoZWQgPSB0aGlzLnJlZ2lvbk1hcFtidWNrZXROYW1lXVxuICAgIGlmIChjYWNoZWQpIHtcbiAgICAgIHJldHVybiBjYWNoZWRcbiAgICB9XG5cbiAgICBjb25zdCBleHRyYWN0UmVnaW9uQXN5bmMgPSBhc3luYyAocmVzcG9uc2U6IGh0dHAuSW5jb21pbmdNZXNzYWdlKSA9PiB7XG4gICAgICBjb25zdCBib2R5ID0gYXdhaXQgcmVhZEFzU3RyaW5nKHJlc3BvbnNlKVxuICAgICAgY29uc3QgcmVnaW9uID0geG1sUGFyc2Vycy5wYXJzZUJ1Y2tldFJlZ2lvbihib2R5KSB8fCBERUZBVUxUX1JFR0lPTlxuICAgICAgdGhpcy5yZWdpb25NYXBbYnVja2V0TmFtZV0gPSByZWdpb25cbiAgICAgIHJldHVybiByZWdpb25cbiAgICB9XG5cbiAgICBjb25zdCBtZXRob2QgPSAnR0VUJ1xuICAgIGNvbnN0IHF1ZXJ5ID0gJ2xvY2F0aW9uJ1xuICAgIC8vIGBnZXRCdWNrZXRMb2NhdGlvbmAgYmVoYXZlcyBkaWZmZXJlbnRseSBpbiBmb2xsb3dpbmcgd2F5cyBmb3JcbiAgICAvLyBkaWZmZXJlbnQgZW52aXJvbm1lbnRzLlxuICAgIC8vXG4gICAgLy8gLSBGb3Igbm9kZWpzIGVudiB3ZSBkZWZhdWx0IHRvIHBhdGggc3R5bGUgcmVxdWVzdHMuXG4gICAgLy8gLSBGb3IgYnJvd3NlciBlbnYgcGF0aCBzdHlsZSByZXF1ZXN0cyBvbiBidWNrZXRzIHlpZWxkcyBDT1JTXG4gICAgLy8gICBlcnJvci4gVG8gY2lyY3VtdmVudCB0aGlzIHByb2JsZW0gd2UgbWFrZSBhIHZpcnR1YWwgaG9zdFxuICAgIC8vICAgc3R5bGUgcmVxdWVzdCBzaWduZWQgd2l0aCAndXMtZWFzdC0xJy4gVGhpcyByZXF1ZXN0IGZhaWxzXG4gICAgLy8gICB3aXRoIGFuIGVycm9yICdBdXRob3JpemF0aW9uSGVhZGVyTWFsZm9ybWVkJywgYWRkaXRpb25hbGx5XG4gICAgLy8gICB0aGUgZXJyb3IgWE1MIGFsc28gcHJvdmlkZXMgUmVnaW9uIG9mIHRoZSBidWNrZXQuIFRvIHZhbGlkYXRlXG4gICAgLy8gICB0aGlzIHJlZ2lvbiBpcyBwcm9wZXIgd2UgcmV0cnkgdGhlIHNhbWUgcmVxdWVzdCB3aXRoIHRoZSBuZXdseVxuICAgIC8vICAgb2J0YWluZWQgcmVnaW9uLlxuICAgIGNvbnN0IHBhdGhTdHlsZSA9IHRoaXMucGF0aFN0eWxlICYmICFpc0Jyb3dzZXJcbiAgICBsZXQgcmVnaW9uOiBzdHJpbmdcbiAgICB0cnkge1xuICAgICAgY29uc3QgcmVzID0gYXdhaXQgdGhpcy5tYWtlUmVxdWVzdEFzeW5jKHsgbWV0aG9kLCBidWNrZXROYW1lLCBxdWVyeSwgcGF0aFN0eWxlIH0sICcnLCBbMjAwXSwgREVGQVVMVF9SRUdJT04pXG4gICAgICByZXR1cm4gZXh0cmFjdFJlZ2lvbkFzeW5jKHJlcylcbiAgICB9IGNhdGNoIChlKSB7XG4gICAgICAvLyBtYWtlIGFsaWdubWVudCB3aXRoIG1jIGNsaVxuICAgICAgaWYgKGUgaW5zdGFuY2VvZiBlcnJvcnMuUzNFcnJvcikge1xuICAgICAgICBjb25zdCBlcnJDb2RlID0gZS5jb2RlXG4gICAgICAgIGNvbnN0IGVyclJlZ2lvbiA9IGUucmVnaW9uXG4gICAgICAgIGlmIChlcnJDb2RlID09PSAnQWNjZXNzRGVuaWVkJyAmJiAhZXJyUmVnaW9uKSB7XG4gICAgICAgICAgcmV0dXJuIERFRkFVTFRfUkVHSU9OXG4gICAgICAgIH1cbiAgICAgIH1cbiAgICAgIC8vIGVzbGludC1kaXNhYmxlLW5leHQtbGluZSBAdHlwZXNjcmlwdC1lc2xpbnQvYmFuLXRzLWNvbW1lbnRcbiAgICAgIC8vIEB0cy1pZ25vcmVcbiAgICAgIGlmICghKGUubmFtZSA9PT0gJ0F1dGhvcml6YXRpb25IZWFkZXJNYWxmb3JtZWQnKSkge1xuICAgICAgICB0aHJvdyBlXG4gICAgICB9XG4gICAgICAvLyBAdHMtZXhwZWN0LWVycm9yIHdlIHNldCBleHRyYSBwcm9wZXJ0aWVzIG9uIGVycm9yIG9iamVjdFxuICAgICAgcmVnaW9uID0gZS5SZWdpb24gYXMgc3RyaW5nXG4gICAgICBpZiAoIXJlZ2lvbikge1xuICAgICAgICB0aHJvdyBlXG4gICAgICB9XG4gICAgfVxuXG4gICAgY29uc3QgcmVzID0gYXdhaXQgdGhpcy5tYWtlUmVxdWVzdEFzeW5jKHsgbWV0aG9kLCBidWNrZXROYW1lLCBxdWVyeSwgcGF0aFN0eWxlIH0sICcnLCBbMjAwXSwgcmVnaW9uKVxuICAgIHJldHVybiBhd2FpdCBleHRyYWN0UmVnaW9uQXN5bmMocmVzKVxuICB9XG5cbiAgLyoqXG4gICAqIG1ha2VSZXF1ZXN0IGlzIHRoZSBwcmltaXRpdmUgdXNlZCBieSB0aGUgYXBpcyBmb3IgbWFraW5nIFMzIHJlcXVlc3RzLlxuICAgKiBwYXlsb2FkIGNhbiBiZSBlbXB0eSBzdHJpbmcgaW4gY2FzZSBvZiBubyBwYXlsb2FkLlxuICAgKiBzdGF0dXNDb2RlIGlzIHRoZSBleHBlY3RlZCBzdGF0dXNDb2RlLiBJZiByZXNwb25zZS5zdGF0dXNDb2RlIGRvZXMgbm90IG1hdGNoXG4gICAqIHdlIHBhcnNlIHRoZSBYTUwgZXJyb3IgYW5kIGNhbGwgdGhlIGNhbGxiYWNrIHdpdGggdGhlIGVycm9yIG1lc3NhZ2UuXG4gICAqIEEgdmFsaWQgcmVnaW9uIGlzIHBhc3NlZCBieSB0aGUgY2FsbHMgLSBsaXN0QnVja2V0cywgbWFrZUJ1Y2tldCBhbmRcbiAgICogZ2V0QnVja2V0UmVnaW9uLlxuICAgKlxuICAgKiBAZGVwcmVjYXRlZCB1c2UgYG1ha2VSZXF1ZXN0QXN5bmNgIGluc3RlYWRcbiAgICovXG4gIG1ha2VSZXF1ZXN0KFxuICAgIG9wdGlvbnM6IFJlcXVlc3RPcHRpb24sXG4gICAgcGF5bG9hZDogQmluYXJ5ID0gJycsXG4gICAgZXhwZWN0ZWRDb2RlczogbnVtYmVyW10gPSBbMjAwXSxcbiAgICByZWdpb24gPSAnJyxcbiAgICByZXR1cm5SZXNwb25zZTogYm9vbGVhbixcbiAgICBjYjogKGNiOiB1bmtub3duLCByZXN1bHQ6IGh0dHAuSW5jb21pbmdNZXNzYWdlKSA9PiB2b2lkLFxuICApIHtcbiAgICBsZXQgcHJvbTogUHJvbWlzZTxodHRwLkluY29taW5nTWVzc2FnZT5cbiAgICBpZiAocmV0dXJuUmVzcG9uc2UpIHtcbiAgICAgIHByb20gPSB0aGlzLm1ha2VSZXF1ZXN0QXN5bmMob3B0aW9ucywgcGF5bG9hZCwgZXhwZWN0ZWRDb2RlcywgcmVnaW9uKVxuICAgIH0gZWxzZSB7XG4gICAgICAvLyBlc2xpbnQtZGlzYWJsZS1uZXh0LWxpbmUgQHR5cGVzY3JpcHQtZXNsaW50L2Jhbi10cy1jb21tZW50XG4gICAgICAvLyBAdHMtZXhwZWN0LWVycm9yIGNvbXBhdGlibGUgZm9yIG9sZCBiZWhhdmlvdXJcbiAgICAgIHByb20gPSB0aGlzLm1ha2VSZXF1ZXN0QXN5bmNPbWl0KG9wdGlvbnMsIHBheWxvYWQsIGV4cGVjdGVkQ29kZXMsIHJlZ2lvbilcbiAgICB9XG5cbiAgICBwcm9tLnRoZW4oXG4gICAgICAocmVzdWx0KSA9PiBjYihudWxsLCByZXN1bHQpLFxuICAgICAgKGVycikgPT4ge1xuICAgICAgICAvLyBlc2xpbnQtZGlzYWJsZS1uZXh0LWxpbmUgQHR5cGVzY3JpcHQtZXNsaW50L2Jhbi10cy1jb21tZW50XG4gICAgICAgIC8vIEB0cy1pZ25vcmVcbiAgICAgICAgY2IoZXJyKVxuICAgICAgfSxcbiAgICApXG4gIH1cblxuICAvKipcbiAgICogbWFrZVJlcXVlc3RTdHJlYW0gd2lsbCBiZSB1c2VkIGRpcmVjdGx5IGluc3RlYWQgb2YgbWFrZVJlcXVlc3QgaW4gY2FzZSB0aGUgcGF5bG9hZFxuICAgKiBpcyBhdmFpbGFibGUgYXMgYSBzdHJlYW0uIGZvciBleC4gcHV0T2JqZWN0XG4gICAqXG4gICAqIEBkZXByZWNhdGVkIHVzZSBgbWFrZVJlcXVlc3RTdHJlYW1Bc3luY2AgaW5zdGVhZFxuICAgKi9cbiAgbWFrZVJlcXVlc3RTdHJlYW0oXG4gICAgb3B0aW9uczogUmVxdWVzdE9wdGlvbixcbiAgICBzdHJlYW06IHN0cmVhbS5SZWFkYWJsZSB8IEJ1ZmZlcixcbiAgICBzaGEyNTZzdW06IHN0cmluZyxcbiAgICBzdGF0dXNDb2RlczogbnVtYmVyW10sXG4gICAgcmVnaW9uOiBzdHJpbmcsXG4gICAgcmV0dXJuUmVzcG9uc2U6IGJvb2xlYW4sXG4gICAgY2I6IChjYjogdW5rbm93biwgcmVzdWx0OiBodHRwLkluY29taW5nTWVzc2FnZSkgPT4gdm9pZCxcbiAgKSB7XG4gICAgY29uc3QgZXhlY3V0b3IgPSBhc3luYyAoKSA9PiB7XG4gICAgICBjb25zdCByZXMgPSBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0U3RyZWFtQXN5bmMob3B0aW9ucywgc3RyZWFtLCBzaGEyNTZzdW0sIHN0YXR1c0NvZGVzLCByZWdpb24pXG4gICAgICBpZiAoIXJldHVyblJlc3BvbnNlKSB7XG4gICAgICAgIGF3YWl0IGRyYWluUmVzcG9uc2UocmVzKVxuICAgICAgfVxuXG4gICAgICByZXR1cm4gcmVzXG4gICAgfVxuXG4gICAgZXhlY3V0b3IoKS50aGVuKFxuICAgICAgKHJlc3VsdCkgPT4gY2IobnVsbCwgcmVzdWx0KSxcbiAgICAgIC8vIGVzbGludC1kaXNhYmxlLW5leHQtbGluZSBAdHlwZXNjcmlwdC1lc2xpbnQvYmFuLXRzLWNvbW1lbnRcbiAgICAgIC8vIEB0cy1pZ25vcmVcbiAgICAgIChlcnIpID0+IGNiKGVyciksXG4gICAgKVxuICB9XG5cbiAgLyoqXG4gICAqIEBkZXByZWNhdGVkIHVzZSBgZ2V0QnVja2V0UmVnaW9uQXN5bmNgIGluc3RlYWRcbiAgICovXG4gIGdldEJ1Y2tldFJlZ2lvbihidWNrZXROYW1lOiBzdHJpbmcsIGNiOiAoZXJyOiB1bmtub3duLCByZWdpb246IHN0cmluZykgPT4gdm9pZCkge1xuICAgIHJldHVybiB0aGlzLmdldEJ1Y2tldFJlZ2lvbkFzeW5jKGJ1Y2tldE5hbWUpLnRoZW4oXG4gICAgICAocmVzdWx0KSA9PiBjYihudWxsLCByZXN1bHQpLFxuICAgICAgLy8gZXNsaW50LWRpc2FibGUtbmV4dC1saW5lIEB0eXBlc2NyaXB0LWVzbGludC9iYW4tdHMtY29tbWVudFxuICAgICAgLy8gQHRzLWlnbm9yZVxuICAgICAgKGVycikgPT4gY2IoZXJyKSxcbiAgICApXG4gIH1cblxuICAvLyBCdWNrZXQgb3BlcmF0aW9uc1xuXG4gIC8qKlxuICAgKiBDcmVhdGVzIHRoZSBidWNrZXQgYGJ1Y2tldE5hbWVgLlxuICAgKlxuICAgKi9cbiAgYXN5bmMgbWFrZUJ1Y2tldChidWNrZXROYW1lOiBzdHJpbmcsIHJlZ2lvbjogUmVnaW9uID0gJycsIG1ha2VPcHRzPzogTWFrZUJ1Y2tldE9wdCk6IFByb21pc2U8dm9pZD4ge1xuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcignSW52YWxpZCBidWNrZXQgbmFtZTogJyArIGJ1Y2tldE5hbWUpXG4gICAgfVxuICAgIC8vIEJhY2t3YXJkIENvbXBhdGliaWxpdHlcbiAgICBpZiAoaXNPYmplY3QocmVnaW9uKSkge1xuICAgICAgbWFrZU9wdHMgPSByZWdpb25cbiAgICAgIHJlZ2lvbiA9ICcnXG4gICAgfVxuXG4gICAgaWYgKCFpc1N0cmluZyhyZWdpb24pKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdyZWdpb24gc2hvdWxkIGJlIG9mIHR5cGUgXCJzdHJpbmdcIicpXG4gICAgfVxuICAgIGlmIChtYWtlT3B0cyAmJiAhaXNPYmplY3QobWFrZU9wdHMpKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdtYWtlT3B0cyBzaG91bGQgYmUgb2YgdHlwZSBcIm9iamVjdFwiJylcbiAgICB9XG5cbiAgICBsZXQgcGF5bG9hZCA9ICcnXG5cbiAgICAvLyBSZWdpb24gYWxyZWFkeSBzZXQgaW4gY29uc3RydWN0b3IsIHZhbGlkYXRlIGlmXG4gICAgLy8gY2FsbGVyIHJlcXVlc3RlZCBidWNrZXQgbG9jYXRpb24gaXMgc2FtZS5cbiAgICBpZiAocmVnaW9uICYmIHRoaXMucmVnaW9uKSB7XG4gICAgICBpZiAocmVnaW9uICE9PSB0aGlzLnJlZ2lvbikge1xuICAgICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKGBDb25maWd1cmVkIHJlZ2lvbiAke3RoaXMucmVnaW9ufSwgcmVxdWVzdGVkICR7cmVnaW9ufWApXG4gICAgICB9XG4gICAgfVxuICAgIC8vIHNlbmRpbmcgbWFrZUJ1Y2tldCByZXF1ZXN0IHdpdGggWE1MIGNvbnRhaW5pbmcgJ3VzLWVhc3QtMScgZmFpbHMuIEZvclxuICAgIC8vIGRlZmF1bHQgcmVnaW9uIHNlcnZlciBleHBlY3RzIHRoZSByZXF1ZXN0IHdpdGhvdXQgYm9keVxuICAgIGlmIChyZWdpb24gJiYgcmVnaW9uICE9PSBERUZBVUxUX1JFR0lPTikge1xuICAgICAgcGF5bG9hZCA9IHhtbC5idWlsZE9iamVjdCh7XG4gICAgICAgIENyZWF0ZUJ1Y2tldENvbmZpZ3VyYXRpb246IHtcbiAgICAgICAgICAkOiB7IHhtbG5zOiAnaHR0cDovL3MzLmFtYXpvbmF3cy5jb20vZG9jLzIwMDYtMDMtMDEvJyB9LFxuICAgICAgICAgIExvY2F0aW9uQ29uc3RyYWludDogcmVnaW9uLFxuICAgICAgICB9LFxuICAgICAgfSlcbiAgICB9XG4gICAgY29uc3QgbWV0aG9kID0gJ1BVVCdcbiAgICBjb25zdCBoZWFkZXJzOiBSZXF1ZXN0SGVhZGVycyA9IHt9XG5cbiAgICBpZiAobWFrZU9wdHMgJiYgbWFrZU9wdHMuT2JqZWN0TG9ja2luZykge1xuICAgICAgaGVhZGVyc1sneC1hbXotYnVja2V0LW9iamVjdC1sb2NrLWVuYWJsZWQnXSA9IHRydWVcbiAgICB9XG5cbiAgICAvLyBGb3IgY3VzdG9tIHJlZ2lvbiBjbGllbnRzICBkZWZhdWx0IHRvIGN1c3RvbSByZWdpb24gc3BlY2lmaWVkIGluIGNsaWVudCBjb25zdHJ1Y3RvclxuICAgIGNvbnN0IGZpbmFsUmVnaW9uID0gdGhpcy5yZWdpb24gfHwgcmVnaW9uIHx8IERFRkFVTFRfUkVHSU9OXG5cbiAgICBjb25zdCByZXF1ZXN0T3B0OiBSZXF1ZXN0T3B0aW9uID0geyBtZXRob2QsIGJ1Y2tldE5hbWUsIGhlYWRlcnMgfVxuXG4gICAgdHJ5IHtcbiAgICAgIGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luY09taXQocmVxdWVzdE9wdCwgcGF5bG9hZCwgWzIwMF0sIGZpbmFsUmVnaW9uKVxuICAgIH0gY2F0Y2ggKGVycjogdW5rbm93bikge1xuICAgICAgaWYgKHJlZ2lvbiA9PT0gJycgfHwgcmVnaW9uID09PSBERUZBVUxUX1JFR0lPTikge1xuICAgICAgICBpZiAoZXJyIGluc3RhbmNlb2YgZXJyb3JzLlMzRXJyb3IpIHtcbiAgICAgICAgICBjb25zdCBlcnJDb2RlID0gZXJyLmNvZGVcbiAgICAgICAgICBjb25zdCBlcnJSZWdpb24gPSBlcnIucmVnaW9uXG4gICAgICAgICAgaWYgKGVyckNvZGUgPT09ICdBdXRob3JpemF0aW9uSGVhZGVyTWFsZm9ybWVkJyAmJiBlcnJSZWdpb24gIT09ICcnKSB7XG4gICAgICAgICAgICAvLyBSZXRyeSB3aXRoIHJlZ2lvbiByZXR1cm5lZCBhcyBwYXJ0IG9mIGVycm9yXG4gICAgICAgICAgICBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0QXN5bmNPbWl0KHJlcXVlc3RPcHQsIHBheWxvYWQsIFsyMDBdLCBlcnJDb2RlKVxuICAgICAgICAgIH1cbiAgICAgICAgfVxuICAgICAgfVxuICAgICAgdGhyb3cgZXJyXG4gICAgfVxuICB9XG5cbiAgLyoqXG4gICAqIFRvIGNoZWNrIGlmIGEgYnVja2V0IGFscmVhZHkgZXhpc3RzLlxuICAgKi9cbiAgYXN5bmMgYnVja2V0RXhpc3RzKGJ1Y2tldE5hbWU6IHN0cmluZyk6IFByb21pc2U8Ym9vbGVhbj4ge1xuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcignSW52YWxpZCBidWNrZXQgbmFtZTogJyArIGJ1Y2tldE5hbWUpXG4gICAgfVxuICAgIGNvbnN0IG1ldGhvZCA9ICdIRUFEJ1xuICAgIHRyeSB7XG4gICAgICBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0QXN5bmNPbWl0KHsgbWV0aG9kLCBidWNrZXROYW1lIH0pXG4gICAgfSBjYXRjaCAoZXJyKSB7XG4gICAgICAvLyBAdHMtaWdub3JlXG4gICAgICBpZiAoZXJyLmNvZGUgPT09ICdOb1N1Y2hCdWNrZXQnIHx8IGVyci5jb2RlID09PSAnTm90Rm91bmQnKSB7XG4gICAgICAgIHJldHVybiBmYWxzZVxuICAgICAgfVxuICAgICAgdGhyb3cgZXJyXG4gICAgfVxuXG4gICAgcmV0dXJuIHRydWVcbiAgfVxuXG4gIGFzeW5jIHJlbW92ZUJ1Y2tldChidWNrZXROYW1lOiBzdHJpbmcpOiBQcm9taXNlPHZvaWQ+XG5cbiAgLyoqXG4gICAqIEBkZXByZWNhdGVkIHVzZSBwcm9taXNlIHN0eWxlIEFQSVxuICAgKi9cbiAgcmVtb3ZlQnVja2V0KGJ1Y2tldE5hbWU6IHN0cmluZywgY2FsbGJhY2s6IE5vUmVzdWx0Q2FsbGJhY2spOiB2b2lkXG5cbiAgYXN5bmMgcmVtb3ZlQnVja2V0KGJ1Y2tldE5hbWU6IHN0cmluZyk6IFByb21pc2U8dm9pZD4ge1xuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcignSW52YWxpZCBidWNrZXQgbmFtZTogJyArIGJ1Y2tldE5hbWUpXG4gICAgfVxuICAgIGNvbnN0IG1ldGhvZCA9ICdERUxFVEUnXG4gICAgYXdhaXQgdGhpcy5tYWtlUmVxdWVzdEFzeW5jT21pdCh7IG1ldGhvZCwgYnVja2V0TmFtZSB9LCAnJywgWzIwNF0pXG4gICAgZGVsZXRlIHRoaXMucmVnaW9uTWFwW2J1Y2tldE5hbWVdXG4gIH1cblxuICAvKipcbiAgICogQ2FsbGJhY2sgaXMgY2FsbGVkIHdpdGggcmVhZGFibGUgc3RyZWFtIG9mIHRoZSBvYmplY3QgY29udGVudC5cbiAgICovXG4gIGFzeW5jIGdldE9iamVjdChidWNrZXROYW1lOiBzdHJpbmcsIG9iamVjdE5hbWU6IHN0cmluZywgZ2V0T3B0cz86IEdldE9iamVjdE9wdHMpOiBQcm9taXNlPHN0cmVhbS5SZWFkYWJsZT4ge1xuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcignSW52YWxpZCBidWNrZXQgbmFtZTogJyArIGJ1Y2tldE5hbWUpXG4gICAgfVxuICAgIGlmICghaXNWYWxpZE9iamVjdE5hbWUob2JqZWN0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZE9iamVjdE5hbWVFcnJvcihgSW52YWxpZCBvYmplY3QgbmFtZTogJHtvYmplY3ROYW1lfWApXG4gICAgfVxuICAgIHJldHVybiB0aGlzLmdldFBhcnRpYWxPYmplY3QoYnVja2V0TmFtZSwgb2JqZWN0TmFtZSwgMCwgMCwgZ2V0T3B0cylcbiAgfVxuXG4gIC8qKlxuICAgKiBDYWxsYmFjayBpcyBjYWxsZWQgd2l0aCByZWFkYWJsZSBzdHJlYW0gb2YgdGhlIHBhcnRpYWwgb2JqZWN0IGNvbnRlbnQuXG4gICAqIEBwYXJhbSBidWNrZXROYW1lXG4gICAqIEBwYXJhbSBvYmplY3ROYW1lXG4gICAqIEBwYXJhbSBvZmZzZXRcbiAgICogQHBhcmFtIGxlbmd0aCAtIGxlbmd0aCBvZiB0aGUgb2JqZWN0IHRoYXQgd2lsbCBiZSByZWFkIGluIHRoZSBzdHJlYW0gKG9wdGlvbmFsLCBpZiBub3Qgc3BlY2lmaWVkIHdlIHJlYWQgdGhlIHJlc3Qgb2YgdGhlIGZpbGUgZnJvbSB0aGUgb2Zmc2V0KVxuICAgKiBAcGFyYW0gZ2V0T3B0c1xuICAgKi9cbiAgYXN5bmMgZ2V0UGFydGlhbE9iamVjdChcbiAgICBidWNrZXROYW1lOiBzdHJpbmcsXG4gICAgb2JqZWN0TmFtZTogc3RyaW5nLFxuICAgIG9mZnNldDogbnVtYmVyLFxuICAgIGxlbmd0aCA9IDAsXG4gICAgZ2V0T3B0cz86IEdldE9iamVjdE9wdHMsXG4gICk6IFByb21pc2U8c3RyZWFtLlJlYWRhYmxlPiB7XG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZShidWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKCdJbnZhbGlkIGJ1Y2tldCBuYW1lOiAnICsgYnVja2V0TmFtZSlcbiAgICB9XG4gICAgaWYgKCFpc1ZhbGlkT2JqZWN0TmFtZShvYmplY3ROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkT2JqZWN0TmFtZUVycm9yKGBJbnZhbGlkIG9iamVjdCBuYW1lOiAke29iamVjdE5hbWV9YClcbiAgICB9XG4gICAgaWYgKCFpc051bWJlcihvZmZzZXQpKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdvZmZzZXQgc2hvdWxkIGJlIG9mIHR5cGUgXCJudW1iZXJcIicpXG4gICAgfVxuICAgIGlmICghaXNOdW1iZXIobGVuZ3RoKSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcignbGVuZ3RoIHNob3VsZCBiZSBvZiB0eXBlIFwibnVtYmVyXCInKVxuICAgIH1cblxuICAgIGxldCByYW5nZSA9ICcnXG4gICAgaWYgKG9mZnNldCB8fCBsZW5ndGgpIHtcbiAgICAgIGlmIChvZmZzZXQpIHtcbiAgICAgICAgcmFuZ2UgPSBgYnl0ZXM9JHsrb2Zmc2V0fS1gXG4gICAgICB9IGVsc2Uge1xuICAgICAgICByYW5nZSA9ICdieXRlcz0wLSdcbiAgICAgICAgb2Zmc2V0ID0gMFxuICAgICAgfVxuICAgICAgaWYgKGxlbmd0aCkge1xuICAgICAgICByYW5nZSArPSBgJHsrbGVuZ3RoICsgb2Zmc2V0IC0gMX1gXG4gICAgICB9XG4gICAgfVxuXG4gICAgbGV0IHF1ZXJ5ID0gJydcbiAgICBsZXQgaGVhZGVyczogUmVxdWVzdEhlYWRlcnMgPSB7XG4gICAgICAuLi4ocmFuZ2UgIT09ICcnICYmIHsgcmFuZ2UgfSksXG4gICAgfVxuXG4gICAgaWYgKGdldE9wdHMpIHtcbiAgICAgIGNvbnN0IHNzZUhlYWRlcnM6IFJlY29yZDxzdHJpbmcsIHN0cmluZz4gPSB7XG4gICAgICAgIC4uLihnZXRPcHRzLlNTRUN1c3RvbWVyQWxnb3JpdGhtICYmIHtcbiAgICAgICAgICAnWC1BbXotU2VydmVyLVNpZGUtRW5jcnlwdGlvbi1DdXN0b21lci1BbGdvcml0aG0nOiBnZXRPcHRzLlNTRUN1c3RvbWVyQWxnb3JpdGhtLFxuICAgICAgICB9KSxcbiAgICAgICAgLi4uKGdldE9wdHMuU1NFQ3VzdG9tZXJLZXkgJiYgeyAnWC1BbXotU2VydmVyLVNpZGUtRW5jcnlwdGlvbi1DdXN0b21lci1LZXknOiBnZXRPcHRzLlNTRUN1c3RvbWVyS2V5IH0pLFxuICAgICAgICAuLi4oZ2V0T3B0cy5TU0VDdXN0b21lcktleU1ENSAmJiB7XG4gICAgICAgICAgJ1gtQW16LVNlcnZlci1TaWRlLUVuY3J5cHRpb24tQ3VzdG9tZXItS2V5LU1ENSc6IGdldE9wdHMuU1NFQ3VzdG9tZXJLZXlNRDUsXG4gICAgICAgIH0pLFxuICAgICAgfVxuICAgICAgcXVlcnkgPSBxcy5zdHJpbmdpZnkoZ2V0T3B0cylcbiAgICAgIGhlYWRlcnMgPSB7XG4gICAgICAgIC4uLnByZXBlbmRYQU1aTWV0YShzc2VIZWFkZXJzKSxcbiAgICAgICAgLi4uaGVhZGVycyxcbiAgICAgIH1cbiAgICB9XG5cbiAgICBjb25zdCBleHBlY3RlZFN0YXR1c0NvZGVzID0gWzIwMF1cbiAgICBpZiAocmFuZ2UpIHtcbiAgICAgIGV4cGVjdGVkU3RhdHVzQ29kZXMucHVzaCgyMDYpXG4gICAgfVxuICAgIGNvbnN0IG1ldGhvZCA9ICdHRVQnXG5cbiAgICByZXR1cm4gYXdhaXQgdGhpcy5tYWtlUmVxdWVzdEFzeW5jKHsgbWV0aG9kLCBidWNrZXROYW1lLCBvYmplY3ROYW1lLCBoZWFkZXJzLCBxdWVyeSB9LCAnJywgZXhwZWN0ZWRTdGF0dXNDb2RlcylcbiAgfVxuXG4gIC8qKlxuICAgKiBkb3dubG9hZCBvYmplY3QgY29udGVudCB0byBhIGZpbGUuXG4gICAqIFRoaXMgbWV0aG9kIHdpbGwgY3JlYXRlIGEgdGVtcCBmaWxlIG5hbWVkIGAke2ZpbGVuYW1lfS4ke2Jhc2U2NChldGFnKX0ucGFydC5taW5pb2Agd2hlbiBkb3dubG9hZGluZy5cbiAgICpcbiAgICogQHBhcmFtIGJ1Y2tldE5hbWUgLSBuYW1lIG9mIHRoZSBidWNrZXRcbiAgICogQHBhcmFtIG9iamVjdE5hbWUgLSBuYW1lIG9mIHRoZSBvYmplY3RcbiAgICogQHBhcmFtIGZpbGVQYXRoIC0gcGF0aCB0byB3aGljaCB0aGUgb2JqZWN0IGRhdGEgd2lsbCBiZSB3cml0dGVuIHRvXG4gICAqIEBwYXJhbSBnZXRPcHRzIC0gT3B0aW9uYWwgb2JqZWN0IGdldCBvcHRpb25cbiAgICovXG4gIGFzeW5jIGZHZXRPYmplY3QoYnVja2V0TmFtZTogc3RyaW5nLCBvYmplY3ROYW1lOiBzdHJpbmcsIGZpbGVQYXRoOiBzdHJpbmcsIGdldE9wdHM/OiBHZXRPYmplY3RPcHRzKTogUHJvbWlzZTx2b2lkPiB7XG4gICAgLy8gSW5wdXQgdmFsaWRhdGlvbi5cbiAgICBpZiAoIWlzVmFsaWRCdWNrZXROYW1lKGJ1Y2tldE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IoJ0ludmFsaWQgYnVja2V0IG5hbWU6ICcgKyBidWNrZXROYW1lKVxuICAgIH1cbiAgICBpZiAoIWlzVmFsaWRPYmplY3ROYW1lKG9iamVjdE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRPYmplY3ROYW1lRXJyb3IoYEludmFsaWQgb2JqZWN0IG5hbWU6ICR7b2JqZWN0TmFtZX1gKVxuICAgIH1cbiAgICBpZiAoIWlzU3RyaW5nKGZpbGVQYXRoKSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcignZmlsZVBhdGggc2hvdWxkIGJlIG9mIHR5cGUgXCJzdHJpbmdcIicpXG4gICAgfVxuXG4gICAgY29uc3QgZG93bmxvYWRUb1RtcEZpbGUgPSBhc3luYyAoKTogUHJvbWlzZTxzdHJpbmc+ID0+IHtcbiAgICAgIGxldCBwYXJ0RmlsZVN0cmVhbTogc3RyZWFtLldyaXRhYmxlXG4gICAgICBjb25zdCBvYmpTdGF0ID0gYXdhaXQgdGhpcy5zdGF0T2JqZWN0KGJ1Y2tldE5hbWUsIG9iamVjdE5hbWUsIGdldE9wdHMpXG4gICAgICBjb25zdCBlbmNvZGVkRXRhZyA9IEJ1ZmZlci5mcm9tKG9ialN0YXQuZXRhZykudG9TdHJpbmcoJ2Jhc2U2NCcpXG4gICAgICBjb25zdCBwYXJ0RmlsZSA9IGAke2ZpbGVQYXRofS4ke2VuY29kZWRFdGFnfS5wYXJ0Lm1pbmlvYFxuXG4gICAgICBhd2FpdCBmc3AubWtkaXIocGF0aC5kaXJuYW1lKGZpbGVQYXRoKSwgeyByZWN1cnNpdmU6IHRydWUgfSlcblxuICAgICAgbGV0IG9mZnNldCA9IDBcbiAgICAgIHRyeSB7XG4gICAgICAgIGNvbnN0IHN0YXRzID0gYXdhaXQgZnNwLnN0YXQocGFydEZpbGUpXG4gICAgICAgIGlmIChvYmpTdGF0LnNpemUgPT09IHN0YXRzLnNpemUpIHtcbiAgICAgICAgICByZXR1cm4gcGFydEZpbGVcbiAgICAgICAgfVxuICAgICAgICBvZmZzZXQgPSBzdGF0cy5zaXplXG4gICAgICAgIHBhcnRGaWxlU3RyZWFtID0gZnMuY3JlYXRlV3JpdGVTdHJlYW0ocGFydEZpbGUsIHsgZmxhZ3M6ICdhJyB9KVxuICAgICAgfSBjYXRjaCAoZSkge1xuICAgICAgICBpZiAoZSBpbnN0YW5jZW9mIEVycm9yICYmIChlIGFzIHVua25vd24gYXMgeyBjb2RlOiBzdHJpbmcgfSkuY29kZSA9PT0gJ0VOT0VOVCcpIHtcbiAgICAgICAgICAvLyBmaWxlIG5vdCBleGlzdFxuICAgICAgICAgIHBhcnRGaWxlU3RyZWFtID0gZnMuY3JlYXRlV3JpdGVTdHJlYW0ocGFydEZpbGUsIHsgZmxhZ3M6ICd3JyB9KVxuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIC8vIG90aGVyIGVycm9yLCBtYXliZSBhY2Nlc3MgZGVueVxuICAgICAgICAgIHRocm93IGVcbiAgICAgICAgfVxuICAgICAgfVxuXG4gICAgICBjb25zdCBkb3dubG9hZFN0cmVhbSA9IGF3YWl0IHRoaXMuZ2V0UGFydGlhbE9iamVjdChidWNrZXROYW1lLCBvYmplY3ROYW1lLCBvZmZzZXQsIDAsIGdldE9wdHMpXG5cbiAgICAgIGF3YWl0IHN0cmVhbVByb21pc2UucGlwZWxpbmUoZG93bmxvYWRTdHJlYW0sIHBhcnRGaWxlU3RyZWFtKVxuICAgICAgY29uc3Qgc3RhdHMgPSBhd2FpdCBmc3Auc3RhdChwYXJ0RmlsZSlcbiAgICAgIGlmIChzdGF0cy5zaXplID09PSBvYmpTdGF0LnNpemUpIHtcbiAgICAgICAgcmV0dXJuIHBhcnRGaWxlXG4gICAgICB9XG5cbiAgICAgIHRocm93IG5ldyBFcnJvcignU2l6ZSBtaXNtYXRjaCBiZXR3ZWVuIGRvd25sb2FkZWQgZmlsZSBhbmQgdGhlIG9iamVjdCcpXG4gICAgfVxuXG4gICAgY29uc3QgcGFydEZpbGUgPSBhd2FpdCBkb3dubG9hZFRvVG1wRmlsZSgpXG4gICAgYXdhaXQgZnNwLnJlbmFtZShwYXJ0RmlsZSwgZmlsZVBhdGgpXG4gIH1cblxuICAvKipcbiAgICogU3RhdCBpbmZvcm1hdGlvbiBvZiB0aGUgb2JqZWN0LlxuICAgKi9cbiAgYXN5bmMgc3RhdE9iamVjdChidWNrZXROYW1lOiBzdHJpbmcsIG9iamVjdE5hbWU6IHN0cmluZywgc3RhdE9wdHM/OiBTdGF0T2JqZWN0T3B0cyk6IFByb21pc2U8QnVja2V0SXRlbVN0YXQ+IHtcbiAgICBjb25zdCBzdGF0T3B0RGVmID0gc3RhdE9wdHMgfHwge31cbiAgICBpZiAoIWlzVmFsaWRCdWNrZXROYW1lKGJ1Y2tldE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IoJ0ludmFsaWQgYnVja2V0IG5hbWU6ICcgKyBidWNrZXROYW1lKVxuICAgIH1cbiAgICBpZiAoIWlzVmFsaWRPYmplY3ROYW1lKG9iamVjdE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRPYmplY3ROYW1lRXJyb3IoYEludmFsaWQgb2JqZWN0IG5hbWU6ICR7b2JqZWN0TmFtZX1gKVxuICAgIH1cblxuICAgIGlmICghaXNPYmplY3Qoc3RhdE9wdERlZikpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEFyZ3VtZW50RXJyb3IoJ3N0YXRPcHRzIHNob3VsZCBiZSBvZiB0eXBlIFwib2JqZWN0XCInKVxuICAgIH1cblxuICAgIGNvbnN0IHF1ZXJ5ID0gcXMuc3RyaW5naWZ5KHN0YXRPcHREZWYpXG4gICAgY29uc3QgbWV0aG9kID0gJ0hFQUQnXG4gICAgY29uc3QgcmVzID0gYXdhaXQgdGhpcy5tYWtlUmVxdWVzdEFzeW5jT21pdCh7IG1ldGhvZCwgYnVja2V0TmFtZSwgb2JqZWN0TmFtZSwgcXVlcnkgfSlcblxuICAgIHJldHVybiB7XG4gICAgICBzaXplOiBwYXJzZUludChyZXMuaGVhZGVyc1snY29udGVudC1sZW5ndGgnXSBhcyBzdHJpbmcpLFxuICAgICAgbWV0YURhdGE6IGV4dHJhY3RNZXRhZGF0YShyZXMuaGVhZGVycyBhcyBSZXNwb25zZUhlYWRlciksXG4gICAgICBsYXN0TW9kaWZpZWQ6IG5ldyBEYXRlKHJlcy5oZWFkZXJzWydsYXN0LW1vZGlmaWVkJ10gYXMgc3RyaW5nKSxcbiAgICAgIHZlcnNpb25JZDogZ2V0VmVyc2lvbklkKHJlcy5oZWFkZXJzIGFzIFJlc3BvbnNlSGVhZGVyKSxcbiAgICAgIGV0YWc6IHNhbml0aXplRVRhZyhyZXMuaGVhZGVycy5ldGFnKSxcbiAgICB9XG4gIH1cblxuICBhc3luYyByZW1vdmVPYmplY3QoYnVja2V0TmFtZTogc3RyaW5nLCBvYmplY3ROYW1lOiBzdHJpbmcsIHJlbW92ZU9wdHM/OiBSZW1vdmVPcHRpb25zKTogUHJvbWlzZTx2b2lkPiB7XG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZShidWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKGBJbnZhbGlkIGJ1Y2tldCBuYW1lOiAke2J1Y2tldE5hbWV9YClcbiAgICB9XG4gICAgaWYgKCFpc1ZhbGlkT2JqZWN0TmFtZShvYmplY3ROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkT2JqZWN0TmFtZUVycm9yKGBJbnZhbGlkIG9iamVjdCBuYW1lOiAke29iamVjdE5hbWV9YClcbiAgICB9XG5cbiAgICBpZiAocmVtb3ZlT3B0cyAmJiAhaXNPYmplY3QocmVtb3ZlT3B0cykpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEFyZ3VtZW50RXJyb3IoJ3JlbW92ZU9wdHMgc2hvdWxkIGJlIG9mIHR5cGUgXCJvYmplY3RcIicpXG4gICAgfVxuXG4gICAgY29uc3QgbWV0aG9kID0gJ0RFTEVURSdcblxuICAgIGNvbnN0IGhlYWRlcnM6IFJlcXVlc3RIZWFkZXJzID0ge31cbiAgICBpZiAocmVtb3ZlT3B0cz8uZ292ZXJuYW5jZUJ5cGFzcykge1xuICAgICAgaGVhZGVyc1snWC1BbXotQnlwYXNzLUdvdmVybmFuY2UtUmV0ZW50aW9uJ10gPSB0cnVlXG4gICAgfVxuICAgIGlmIChyZW1vdmVPcHRzPy5mb3JjZURlbGV0ZSkge1xuICAgICAgaGVhZGVyc1sneC1taW5pby1mb3JjZS1kZWxldGUnXSA9IHRydWVcbiAgICB9XG5cbiAgICBjb25zdCBxdWVyeVBhcmFtczogUmVjb3JkPHN0cmluZywgc3RyaW5nPiA9IHt9XG4gICAgaWYgKHJlbW92ZU9wdHM/LnZlcnNpb25JZCkge1xuICAgICAgcXVlcnlQYXJhbXMudmVyc2lvbklkID0gYCR7cmVtb3ZlT3B0cy52ZXJzaW9uSWR9YFxuICAgIH1cbiAgICBjb25zdCBxdWVyeSA9IHFzLnN0cmluZ2lmeShxdWVyeVBhcmFtcylcblxuICAgIGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luY09taXQoeyBtZXRob2QsIGJ1Y2tldE5hbWUsIG9iamVjdE5hbWUsIGhlYWRlcnMsIHF1ZXJ5IH0sICcnLCBbMjAwLCAyMDRdKVxuICB9XG5cbiAgLy8gQ2FsbHMgaW1wbGVtZW50ZWQgYmVsb3cgYXJlIHJlbGF0ZWQgdG8gbXVsdGlwYXJ0LlxuXG4gIGxpc3RJbmNvbXBsZXRlVXBsb2FkcyhcbiAgICBidWNrZXQ6IHN0cmluZyxcbiAgICBwcmVmaXg6IHN0cmluZyxcbiAgICByZWN1cnNpdmU6IGJvb2xlYW4sXG4gICk6IEJ1Y2tldFN0cmVhbTxJbmNvbXBsZXRlVXBsb2FkZWRCdWNrZXRJdGVtPiB7XG4gICAgaWYgKHByZWZpeCA9PT0gdW5kZWZpbmVkKSB7XG4gICAgICBwcmVmaXggPSAnJ1xuICAgIH1cbiAgICBpZiAocmVjdXJzaXZlID09PSB1bmRlZmluZWQpIHtcbiAgICAgIHJlY3Vyc2l2ZSA9IGZhbHNlXG4gICAgfVxuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0KSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKCdJbnZhbGlkIGJ1Y2tldCBuYW1lOiAnICsgYnVja2V0KVxuICAgIH1cbiAgICBpZiAoIWlzVmFsaWRQcmVmaXgocHJlZml4KSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkUHJlZml4RXJyb3IoYEludmFsaWQgcHJlZml4IDogJHtwcmVmaXh9YClcbiAgICB9XG4gICAgaWYgKCFpc0Jvb2xlYW4ocmVjdXJzaXZlKSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcigncmVjdXJzaXZlIHNob3VsZCBiZSBvZiB0eXBlIFwiYm9vbGVhblwiJylcbiAgICB9XG4gICAgY29uc3QgZGVsaW1pdGVyID0gcmVjdXJzaXZlID8gJycgOiAnLydcbiAgICBsZXQga2V5TWFya2VyID0gJydcbiAgICBsZXQgdXBsb2FkSWRNYXJrZXIgPSAnJ1xuICAgIGNvbnN0IHVwbG9hZHM6IHVua25vd25bXSA9IFtdXG4gICAgbGV0IGVuZGVkID0gZmFsc2VcblxuICAgIC8vIFRPRE86IHJlZmFjdG9yIHRoaXMgd2l0aCBhc3luYy9hd2FpdCBhbmQgYHN0cmVhbS5SZWFkYWJsZS5mcm9tYFxuICAgIGNvbnN0IHJlYWRTdHJlYW0gPSBuZXcgc3RyZWFtLlJlYWRhYmxlKHsgb2JqZWN0TW9kZTogdHJ1ZSB9KVxuICAgIHJlYWRTdHJlYW0uX3JlYWQgPSAoKSA9PiB7XG4gICAgICAvLyBwdXNoIG9uZSB1cGxvYWQgaW5mbyBwZXIgX3JlYWQoKVxuICAgICAgaWYgKHVwbG9hZHMubGVuZ3RoKSB7XG4gICAgICAgIHJldHVybiByZWFkU3RyZWFtLnB1c2godXBsb2Fkcy5zaGlmdCgpKVxuICAgICAgfVxuICAgICAgaWYgKGVuZGVkKSB7XG4gICAgICAgIHJldHVybiByZWFkU3RyZWFtLnB1c2gobnVsbClcbiAgICAgIH1cbiAgICAgIHRoaXMubGlzdEluY29tcGxldGVVcGxvYWRzUXVlcnkoYnVja2V0LCBwcmVmaXgsIGtleU1hcmtlciwgdXBsb2FkSWRNYXJrZXIsIGRlbGltaXRlcikudGhlbihcbiAgICAgICAgKHJlc3VsdCkgPT4ge1xuICAgICAgICAgIC8vIGVzbGludC1kaXNhYmxlLW5leHQtbGluZSBAdHlwZXNjcmlwdC1lc2xpbnQvYmFuLXRzLWNvbW1lbnRcbiAgICAgICAgICAvLyBAdHMtaWdub3JlXG4gICAgICAgICAgcmVzdWx0LnByZWZpeGVzLmZvckVhY2goKHByZWZpeCkgPT4gdXBsb2Fkcy5wdXNoKHByZWZpeCkpXG4gICAgICAgICAgYXN5bmMuZWFjaFNlcmllcyhcbiAgICAgICAgICAgIHJlc3VsdC51cGxvYWRzLFxuICAgICAgICAgICAgKHVwbG9hZCwgY2IpID0+IHtcbiAgICAgICAgICAgICAgLy8gZm9yIGVhY2ggaW5jb21wbGV0ZSB1cGxvYWQgYWRkIHRoZSBzaXplcyBvZiBpdHMgdXBsb2FkZWQgcGFydHNcbiAgICAgICAgICAgICAgLy8gZXNsaW50LWRpc2FibGUtbmV4dC1saW5lIEB0eXBlc2NyaXB0LWVzbGludC9iYW4tdHMtY29tbWVudFxuICAgICAgICAgICAgICAvLyBAdHMtaWdub3JlXG4gICAgICAgICAgICAgIHRoaXMubGlzdFBhcnRzKGJ1Y2tldCwgdXBsb2FkLmtleSwgdXBsb2FkLnVwbG9hZElkKS50aGVuKFxuICAgICAgICAgICAgICAgIChwYXJ0czogUGFydFtdKSA9PiB7XG4gICAgICAgICAgICAgICAgICAvLyBlc2xpbnQtZGlzYWJsZS1uZXh0LWxpbmUgQHR5cGVzY3JpcHQtZXNsaW50L2Jhbi10cy1jb21tZW50XG4gICAgICAgICAgICAgICAgICAvLyBAdHMtaWdub3JlXG4gICAgICAgICAgICAgICAgICB1cGxvYWQuc2l6ZSA9IHBhcnRzLnJlZHVjZSgoYWNjLCBpdGVtKSA9PiBhY2MgKyBpdGVtLnNpemUsIDApXG4gICAgICAgICAgICAgICAgICB1cGxvYWRzLnB1c2godXBsb2FkKVxuICAgICAgICAgICAgICAgICAgY2IoKVxuICAgICAgICAgICAgICAgIH0sXG4gICAgICAgICAgICAgICAgKGVycjogRXJyb3IpID0+IGNiKGVyciksXG4gICAgICAgICAgICAgIClcbiAgICAgICAgICAgIH0sXG4gICAgICAgICAgICAoZXJyKSA9PiB7XG4gICAgICAgICAgICAgIGlmIChlcnIpIHtcbiAgICAgICAgICAgICAgICByZWFkU3RyZWFtLmVtaXQoJ2Vycm9yJywgZXJyKVxuICAgICAgICAgICAgICAgIHJldHVyblxuICAgICAgICAgICAgICB9XG4gICAgICAgICAgICAgIGlmIChyZXN1bHQuaXNUcnVuY2F0ZWQpIHtcbiAgICAgICAgICAgICAgICBrZXlNYXJrZXIgPSByZXN1bHQubmV4dEtleU1hcmtlclxuICAgICAgICAgICAgICAgIHVwbG9hZElkTWFya2VyID0gcmVzdWx0Lm5leHRVcGxvYWRJZE1hcmtlclxuICAgICAgICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgICAgICAgIGVuZGVkID0gdHJ1ZVxuICAgICAgICAgICAgICB9XG5cbiAgICAgICAgICAgICAgLy8gZXNsaW50LWRpc2FibGUtbmV4dC1saW5lIEB0eXBlc2NyaXB0LWVzbGludC9iYW4tdHMtY29tbWVudFxuICAgICAgICAgICAgICAvLyBAdHMtaWdub3JlXG4gICAgICAgICAgICAgIHJlYWRTdHJlYW0uX3JlYWQoKVxuICAgICAgICAgICAgfSxcbiAgICAgICAgICApXG4gICAgICAgIH0sXG4gICAgICAgIChlKSA9PiB7XG4gICAgICAgICAgcmVhZFN0cmVhbS5lbWl0KCdlcnJvcicsIGUpXG4gICAgICAgIH0sXG4gICAgICApXG4gICAgfVxuICAgIHJldHVybiByZWFkU3RyZWFtXG4gIH1cblxuICAvKipcbiAgICogQ2FsbGVkIGJ5IGxpc3RJbmNvbXBsZXRlVXBsb2FkcyB0byBmZXRjaCBhIGJhdGNoIG9mIGluY29tcGxldGUgdXBsb2Fkcy5cbiAgICovXG4gIGFzeW5jIGxpc3RJbmNvbXBsZXRlVXBsb2Fkc1F1ZXJ5KFxuICAgIGJ1Y2tldE5hbWU6IHN0cmluZyxcbiAgICBwcmVmaXg6IHN0cmluZyxcbiAgICBrZXlNYXJrZXI6IHN0cmluZyxcbiAgICB1cGxvYWRJZE1hcmtlcjogc3RyaW5nLFxuICAgIGRlbGltaXRlcjogc3RyaW5nLFxuICApOiBQcm9taXNlPExpc3RNdWx0aXBhcnRSZXN1bHQ+IHtcbiAgICBpZiAoIWlzVmFsaWRCdWNrZXROYW1lKGJ1Y2tldE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IoJ0ludmFsaWQgYnVja2V0IG5hbWU6ICcgKyBidWNrZXROYW1lKVxuICAgIH1cbiAgICBpZiAoIWlzU3RyaW5nKHByZWZpeCkpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ3ByZWZpeCBzaG91bGQgYmUgb2YgdHlwZSBcInN0cmluZ1wiJylcbiAgICB9XG4gICAgaWYgKCFpc1N0cmluZyhrZXlNYXJrZXIpKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdrZXlNYXJrZXIgc2hvdWxkIGJlIG9mIHR5cGUgXCJzdHJpbmdcIicpXG4gICAgfVxuICAgIGlmICghaXNTdHJpbmcodXBsb2FkSWRNYXJrZXIpKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCd1cGxvYWRJZE1hcmtlciBzaG91bGQgYmUgb2YgdHlwZSBcInN0cmluZ1wiJylcbiAgICB9XG4gICAgaWYgKCFpc1N0cmluZyhkZWxpbWl0ZXIpKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdkZWxpbWl0ZXIgc2hvdWxkIGJlIG9mIHR5cGUgXCJzdHJpbmdcIicpXG4gICAgfVxuICAgIGNvbnN0IHF1ZXJpZXMgPSBbXVxuICAgIHF1ZXJpZXMucHVzaChgcHJlZml4PSR7dXJpRXNjYXBlKHByZWZpeCl9YClcbiAgICBxdWVyaWVzLnB1c2goYGRlbGltaXRlcj0ke3VyaUVzY2FwZShkZWxpbWl0ZXIpfWApXG5cbiAgICBpZiAoa2V5TWFya2VyKSB7XG4gICAgICBxdWVyaWVzLnB1c2goYGtleS1tYXJrZXI9JHt1cmlFc2NhcGUoa2V5TWFya2VyKX1gKVxuICAgIH1cbiAgICBpZiAodXBsb2FkSWRNYXJrZXIpIHtcbiAgICAgIHF1ZXJpZXMucHVzaChgdXBsb2FkLWlkLW1hcmtlcj0ke3VwbG9hZElkTWFya2VyfWApXG4gICAgfVxuXG4gICAgY29uc3QgbWF4VXBsb2FkcyA9IDEwMDBcbiAgICBxdWVyaWVzLnB1c2goYG1heC11cGxvYWRzPSR7bWF4VXBsb2Fkc31gKVxuICAgIHF1ZXJpZXMuc29ydCgpXG4gICAgcXVlcmllcy51bnNoaWZ0KCd1cGxvYWRzJylcbiAgICBsZXQgcXVlcnkgPSAnJ1xuICAgIGlmIChxdWVyaWVzLmxlbmd0aCA+IDApIHtcbiAgICAgIHF1ZXJ5ID0gYCR7cXVlcmllcy5qb2luKCcmJyl9YFxuICAgIH1cbiAgICBjb25zdCBtZXRob2QgPSAnR0VUJ1xuICAgIGNvbnN0IHJlcyA9IGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luYyh7IG1ldGhvZCwgYnVja2V0TmFtZSwgcXVlcnkgfSlcbiAgICBjb25zdCBib2R5ID0gYXdhaXQgcmVhZEFzU3RyaW5nKHJlcylcbiAgICByZXR1cm4geG1sUGFyc2Vycy5wYXJzZUxpc3RNdWx0aXBhcnQoYm9keSlcbiAgfVxuXG4gIC8qKlxuICAgKiBJbml0aWF0ZSBhIG5ldyBtdWx0aXBhcnQgdXBsb2FkLlxuICAgKiBAaW50ZXJuYWxcbiAgICovXG4gIGFzeW5jIGluaXRpYXRlTmV3TXVsdGlwYXJ0VXBsb2FkKGJ1Y2tldE5hbWU6IHN0cmluZywgb2JqZWN0TmFtZTogc3RyaW5nLCBoZWFkZXJzOiBSZXF1ZXN0SGVhZGVycyk6IFByb21pc2U8c3RyaW5nPiB7XG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZShidWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKCdJbnZhbGlkIGJ1Y2tldCBuYW1lOiAnICsgYnVja2V0TmFtZSlcbiAgICB9XG4gICAgaWYgKCFpc1ZhbGlkT2JqZWN0TmFtZShvYmplY3ROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkT2JqZWN0TmFtZUVycm9yKGBJbnZhbGlkIG9iamVjdCBuYW1lOiAke29iamVjdE5hbWV9YClcbiAgICB9XG4gICAgaWYgKCFpc09iamVjdChoZWFkZXJzKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkT2JqZWN0TmFtZUVycm9yKCdjb250ZW50VHlwZSBzaG91bGQgYmUgb2YgdHlwZSBcIm9iamVjdFwiJylcbiAgICB9XG4gICAgY29uc3QgbWV0aG9kID0gJ1BPU1QnXG4gICAgY29uc3QgcXVlcnkgPSAndXBsb2FkcydcbiAgICBjb25zdCByZXMgPSBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0QXN5bmMoeyBtZXRob2QsIGJ1Y2tldE5hbWUsIG9iamVjdE5hbWUsIHF1ZXJ5LCBoZWFkZXJzIH0pXG4gICAgY29uc3QgYm9keSA9IGF3YWl0IHJlYWRBc0J1ZmZlcihyZXMpXG4gICAgcmV0dXJuIHBhcnNlSW5pdGlhdGVNdWx0aXBhcnQoYm9keS50b1N0cmluZygpKVxuICB9XG5cbiAgLyoqXG4gICAqIEludGVybmFsIE1ldGhvZCB0byBhYm9ydCBhIG11bHRpcGFydCB1cGxvYWQgcmVxdWVzdCBpbiBjYXNlIG9mIGFueSBlcnJvcnMuXG4gICAqXG4gICAqIEBwYXJhbSBidWNrZXROYW1lIC0gQnVja2V0IE5hbWVcbiAgICogQHBhcmFtIG9iamVjdE5hbWUgLSBPYmplY3QgTmFtZVxuICAgKiBAcGFyYW0gdXBsb2FkSWQgLSBpZCBvZiBhIG11bHRpcGFydCB1cGxvYWQgdG8gY2FuY2VsIGR1cmluZyBjb21wb3NlIG9iamVjdCBzZXF1ZW5jZS5cbiAgICovXG4gIGFzeW5jIGFib3J0TXVsdGlwYXJ0VXBsb2FkKGJ1Y2tldE5hbWU6IHN0cmluZywgb2JqZWN0TmFtZTogc3RyaW5nLCB1cGxvYWRJZDogc3RyaW5nKTogUHJvbWlzZTx2b2lkPiB7XG4gICAgY29uc3QgbWV0aG9kID0gJ0RFTEVURSdcbiAgICBjb25zdCBxdWVyeSA9IGB1cGxvYWRJZD0ke3VwbG9hZElkfWBcblxuICAgIGNvbnN0IHJlcXVlc3RPcHRpb25zID0geyBtZXRob2QsIGJ1Y2tldE5hbWUsIG9iamVjdE5hbWU6IG9iamVjdE5hbWUsIHF1ZXJ5IH1cbiAgICBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0QXN5bmNPbWl0KHJlcXVlc3RPcHRpb25zLCAnJywgWzIwNF0pXG4gIH1cblxuICBhc3luYyBmaW5kVXBsb2FkSWQoYnVja2V0TmFtZTogc3RyaW5nLCBvYmplY3ROYW1lOiBzdHJpbmcpOiBQcm9taXNlPHN0cmluZyB8IHVuZGVmaW5lZD4ge1xuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcignSW52YWxpZCBidWNrZXQgbmFtZTogJyArIGJ1Y2tldE5hbWUpXG4gICAgfVxuICAgIGlmICghaXNWYWxpZE9iamVjdE5hbWUob2JqZWN0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZE9iamVjdE5hbWVFcnJvcihgSW52YWxpZCBvYmplY3QgbmFtZTogJHtvYmplY3ROYW1lfWApXG4gICAgfVxuXG4gICAgbGV0IGxhdGVzdFVwbG9hZDogTGlzdE11bHRpcGFydFJlc3VsdFsndXBsb2FkcyddW251bWJlcl0gfCB1bmRlZmluZWRcbiAgICBsZXQga2V5TWFya2VyID0gJydcbiAgICBsZXQgdXBsb2FkSWRNYXJrZXIgPSAnJ1xuICAgIGZvciAoOzspIHtcbiAgICAgIGNvbnN0IHJlc3VsdCA9IGF3YWl0IHRoaXMubGlzdEluY29tcGxldGVVcGxvYWRzUXVlcnkoYnVja2V0TmFtZSwgb2JqZWN0TmFtZSwga2V5TWFya2VyLCB1cGxvYWRJZE1hcmtlciwgJycpXG4gICAgICBmb3IgKGNvbnN0IHVwbG9hZCBvZiByZXN1bHQudXBsb2Fkcykge1xuICAgICAgICBpZiAodXBsb2FkLmtleSA9PT0gb2JqZWN0TmFtZSkge1xuICAgICAgICAgIGlmICghbGF0ZXN0VXBsb2FkIHx8IHVwbG9hZC5pbml0aWF0ZWQuZ2V0VGltZSgpID4gbGF0ZXN0VXBsb2FkLmluaXRpYXRlZC5nZXRUaW1lKCkpIHtcbiAgICAgICAgICAgIGxhdGVzdFVwbG9hZCA9IHVwbG9hZFxuICAgICAgICAgIH1cbiAgICAgICAgfVxuICAgICAgfVxuICAgICAgaWYgKHJlc3VsdC5pc1RydW5jYXRlZCkge1xuICAgICAgICBrZXlNYXJrZXIgPSByZXN1bHQubmV4dEtleU1hcmtlclxuICAgICAgICB1cGxvYWRJZE1hcmtlciA9IHJlc3VsdC5uZXh0VXBsb2FkSWRNYXJrZXJcbiAgICAgICAgY29udGludWVcbiAgICAgIH1cblxuICAgICAgYnJlYWtcbiAgICB9XG4gICAgcmV0dXJuIGxhdGVzdFVwbG9hZD8udXBsb2FkSWRcbiAgfVxuXG4gIC8qKlxuICAgKiB0aGlzIGNhbGwgd2lsbCBhZ2dyZWdhdGUgdGhlIHBhcnRzIG9uIHRoZSBzZXJ2ZXIgaW50byBhIHNpbmdsZSBvYmplY3QuXG4gICAqL1xuICBhc3luYyBjb21wbGV0ZU11bHRpcGFydFVwbG9hZChcbiAgICBidWNrZXROYW1lOiBzdHJpbmcsXG4gICAgb2JqZWN0TmFtZTogc3RyaW5nLFxuICAgIHVwbG9hZElkOiBzdHJpbmcsXG4gICAgZXRhZ3M6IHtcbiAgICAgIHBhcnQ6IG51bWJlclxuICAgICAgZXRhZz86IHN0cmluZ1xuICAgIH1bXSxcbiAgKTogUHJvbWlzZTx7IGV0YWc6IHN0cmluZzsgdmVyc2lvbklkOiBzdHJpbmcgfCBudWxsIH0+IHtcbiAgICBpZiAoIWlzVmFsaWRCdWNrZXROYW1lKGJ1Y2tldE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IoJ0ludmFsaWQgYnVja2V0IG5hbWU6ICcgKyBidWNrZXROYW1lKVxuICAgIH1cbiAgICBpZiAoIWlzVmFsaWRPYmplY3ROYW1lKG9iamVjdE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRPYmplY3ROYW1lRXJyb3IoYEludmFsaWQgb2JqZWN0IG5hbWU6ICR7b2JqZWN0TmFtZX1gKVxuICAgIH1cbiAgICBpZiAoIWlzU3RyaW5nKHVwbG9hZElkKSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcigndXBsb2FkSWQgc2hvdWxkIGJlIG9mIHR5cGUgXCJzdHJpbmdcIicpXG4gICAgfVxuICAgIGlmICghaXNPYmplY3QoZXRhZ3MpKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdldGFncyBzaG91bGQgYmUgb2YgdHlwZSBcIkFycmF5XCInKVxuICAgIH1cblxuICAgIGlmICghdXBsb2FkSWQpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEFyZ3VtZW50RXJyb3IoJ3VwbG9hZElkIGNhbm5vdCBiZSBlbXB0eScpXG4gICAgfVxuXG4gICAgY29uc3QgbWV0aG9kID0gJ1BPU1QnXG4gICAgY29uc3QgcXVlcnkgPSBgdXBsb2FkSWQ9JHt1cmlFc2NhcGUodXBsb2FkSWQpfWBcblxuICAgIGNvbnN0IGJ1aWxkZXIgPSBuZXcgeG1sMmpzLkJ1aWxkZXIoKVxuICAgIGNvbnN0IHBheWxvYWQgPSBidWlsZGVyLmJ1aWxkT2JqZWN0KHtcbiAgICAgIENvbXBsZXRlTXVsdGlwYXJ0VXBsb2FkOiB7XG4gICAgICAgICQ6IHtcbiAgICAgICAgICB4bWxuczogJ2h0dHA6Ly9zMy5hbWF6b25hd3MuY29tL2RvYy8yMDA2LTAzLTAxLycsXG4gICAgICAgIH0sXG4gICAgICAgIFBhcnQ6IGV0YWdzLm1hcCgoZXRhZykgPT4ge1xuICAgICAgICAgIHJldHVybiB7XG4gICAgICAgICAgICBQYXJ0TnVtYmVyOiBldGFnLnBhcnQsXG4gICAgICAgICAgICBFVGFnOiBldGFnLmV0YWcsXG4gICAgICAgICAgfVxuICAgICAgICB9KSxcbiAgICAgIH0sXG4gICAgfSlcblxuICAgIGNvbnN0IHJlcyA9IGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luYyh7IG1ldGhvZCwgYnVja2V0TmFtZSwgb2JqZWN0TmFtZSwgcXVlcnkgfSwgcGF5bG9hZClcbiAgICBjb25zdCBib2R5ID0gYXdhaXQgcmVhZEFzQnVmZmVyKHJlcylcbiAgICBjb25zdCByZXN1bHQgPSBwYXJzZUNvbXBsZXRlTXVsdGlwYXJ0KGJvZHkudG9TdHJpbmcoKSlcbiAgICBpZiAoIXJlc3VsdCkge1xuICAgICAgdGhyb3cgbmV3IEVycm9yKCdCVUc6IGZhaWxlZCB0byBwYXJzZSBzZXJ2ZXIgcmVzcG9uc2UnKVxuICAgIH1cblxuICAgIGlmIChyZXN1bHQuZXJyQ29kZSkge1xuICAgICAgLy8gTXVsdGlwYXJ0IENvbXBsZXRlIEFQSSByZXR1cm5zIGFuIGVycm9yIFhNTCBhZnRlciBhIDIwMCBodHRwIHN0YXR1c1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5TM0Vycm9yKHJlc3VsdC5lcnJNZXNzYWdlKVxuICAgIH1cblxuICAgIHJldHVybiB7XG4gICAgICAvLyBlc2xpbnQtZGlzYWJsZS1uZXh0LWxpbmUgQHR5cGVzY3JpcHQtZXNsaW50L2Jhbi10cy1jb21tZW50XG4gICAgICAvLyBAdHMtaWdub3JlXG4gICAgICBldGFnOiByZXN1bHQuZXRhZyBhcyBzdHJpbmcsXG4gICAgICB2ZXJzaW9uSWQ6IGdldFZlcnNpb25JZChyZXMuaGVhZGVycyBhcyBSZXNwb25zZUhlYWRlciksXG4gICAgfVxuICB9XG5cbiAgLyoqXG4gICAqIEdldCBwYXJ0LWluZm8gb2YgYWxsIHBhcnRzIG9mIGFuIGluY29tcGxldGUgdXBsb2FkIHNwZWNpZmllZCBieSB1cGxvYWRJZC5cbiAgICovXG4gIHByb3RlY3RlZCBhc3luYyBsaXN0UGFydHMoYnVja2V0TmFtZTogc3RyaW5nLCBvYmplY3ROYW1lOiBzdHJpbmcsIHVwbG9hZElkOiBzdHJpbmcpOiBQcm9taXNlPFVwbG9hZGVkUGFydFtdPiB7XG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZShidWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKCdJbnZhbGlkIGJ1Y2tldCBuYW1lOiAnICsgYnVja2V0TmFtZSlcbiAgICB9XG4gICAgaWYgKCFpc1ZhbGlkT2JqZWN0TmFtZShvYmplY3ROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkT2JqZWN0TmFtZUVycm9yKGBJbnZhbGlkIG9iamVjdCBuYW1lOiAke29iamVjdE5hbWV9YClcbiAgICB9XG4gICAgaWYgKCFpc1N0cmluZyh1cGxvYWRJZCkpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ3VwbG9hZElkIHNob3VsZCBiZSBvZiB0eXBlIFwic3RyaW5nXCInKVxuICAgIH1cbiAgICBpZiAoIXVwbG9hZElkKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKCd1cGxvYWRJZCBjYW5ub3QgYmUgZW1wdHknKVxuICAgIH1cblxuICAgIGNvbnN0IHBhcnRzOiBVcGxvYWRlZFBhcnRbXSA9IFtdXG4gICAgbGV0IG1hcmtlciA9IDBcbiAgICBsZXQgcmVzdWx0XG4gICAgZG8ge1xuICAgICAgcmVzdWx0ID0gYXdhaXQgdGhpcy5saXN0UGFydHNRdWVyeShidWNrZXROYW1lLCBvYmplY3ROYW1lLCB1cGxvYWRJZCwgbWFya2VyKVxuICAgICAgbWFya2VyID0gcmVzdWx0Lm1hcmtlclxuICAgICAgcGFydHMucHVzaCguLi5yZXN1bHQucGFydHMpXG4gICAgfSB3aGlsZSAocmVzdWx0LmlzVHJ1bmNhdGVkKVxuXG4gICAgcmV0dXJuIHBhcnRzXG4gIH1cblxuICAvKipcbiAgICogQ2FsbGVkIGJ5IGxpc3RQYXJ0cyB0byBmZXRjaCBhIGJhdGNoIG9mIHBhcnQtaW5mb1xuICAgKi9cbiAgcHJpdmF0ZSBhc3luYyBsaXN0UGFydHNRdWVyeShidWNrZXROYW1lOiBzdHJpbmcsIG9iamVjdE5hbWU6IHN0cmluZywgdXBsb2FkSWQ6IHN0cmluZywgbWFya2VyOiBudW1iZXIpIHtcbiAgICBpZiAoIWlzVmFsaWRCdWNrZXROYW1lKGJ1Y2tldE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IoJ0ludmFsaWQgYnVja2V0IG5hbWU6ICcgKyBidWNrZXROYW1lKVxuICAgIH1cbiAgICBpZiAoIWlzVmFsaWRPYmplY3ROYW1lKG9iamVjdE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRPYmplY3ROYW1lRXJyb3IoYEludmFsaWQgb2JqZWN0IG5hbWU6ICR7b2JqZWN0TmFtZX1gKVxuICAgIH1cbiAgICBpZiAoIWlzU3RyaW5nKHVwbG9hZElkKSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcigndXBsb2FkSWQgc2hvdWxkIGJlIG9mIHR5cGUgXCJzdHJpbmdcIicpXG4gICAgfVxuICAgIGlmICghaXNOdW1iZXIobWFya2VyKSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcignbWFya2VyIHNob3VsZCBiZSBvZiB0eXBlIFwibnVtYmVyXCInKVxuICAgIH1cbiAgICBpZiAoIXVwbG9hZElkKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKCd1cGxvYWRJZCBjYW5ub3QgYmUgZW1wdHknKVxuICAgIH1cblxuICAgIGxldCBxdWVyeSA9IGB1cGxvYWRJZD0ke3VyaUVzY2FwZSh1cGxvYWRJZCl9YFxuICAgIGlmIChtYXJrZXIpIHtcbiAgICAgIHF1ZXJ5ICs9IGAmcGFydC1udW1iZXItbWFya2VyPSR7bWFya2VyfWBcbiAgICB9XG5cbiAgICBjb25zdCBtZXRob2QgPSAnR0VUJ1xuICAgIGNvbnN0IHJlcyA9IGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luYyh7IG1ldGhvZCwgYnVja2V0TmFtZSwgb2JqZWN0TmFtZSwgcXVlcnkgfSlcbiAgICByZXR1cm4geG1sUGFyc2Vycy5wYXJzZUxpc3RQYXJ0cyhhd2FpdCByZWFkQXNTdHJpbmcocmVzKSlcbiAgfVxuXG4gIGFzeW5jIGxpc3RCdWNrZXRzKCk6IFByb21pc2U8QnVja2V0SXRlbUZyb21MaXN0W10+IHtcbiAgICBjb25zdCBtZXRob2QgPSAnR0VUJ1xuICAgIGNvbnN0IHJlZ2lvbkNvbmYgPSB0aGlzLnJlZ2lvbiB8fCBERUZBVUxUX1JFR0lPTlxuICAgIGNvbnN0IGh0dHBSZXMgPSBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0QXN5bmMoeyBtZXRob2QgfSwgJycsIFsyMDBdLCByZWdpb25Db25mKVxuICAgIGNvbnN0IHhtbFJlc3VsdCA9IGF3YWl0IHJlYWRBc1N0cmluZyhodHRwUmVzKVxuICAgIHJldHVybiB4bWxQYXJzZXJzLnBhcnNlTGlzdEJ1Y2tldCh4bWxSZXN1bHQpXG4gIH1cblxuICAvKipcbiAgICogQ2FsY3VsYXRlIHBhcnQgc2l6ZSBnaXZlbiB0aGUgb2JqZWN0IHNpemUuIFBhcnQgc2l6ZSB3aWxsIGJlIGF0bGVhc3QgdGhpcy5wYXJ0U2l6ZVxuICAgKi9cbiAgY2FsY3VsYXRlUGFydFNpemUoc2l6ZTogbnVtYmVyKSB7XG4gICAgaWYgKCFpc051bWJlcihzaXplKSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcignc2l6ZSBzaG91bGQgYmUgb2YgdHlwZSBcIm51bWJlclwiJylcbiAgICB9XG4gICAgaWYgKHNpemUgPiB0aGlzLm1heE9iamVjdFNpemUpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoYHNpemUgc2hvdWxkIG5vdCBiZSBtb3JlIHRoYW4gJHt0aGlzLm1heE9iamVjdFNpemV9YClcbiAgICB9XG4gICAgaWYgKHRoaXMub3ZlclJpZGVQYXJ0U2l6ZSkge1xuICAgICAgcmV0dXJuIHRoaXMucGFydFNpemVcbiAgICB9XG4gICAgbGV0IHBhcnRTaXplID0gdGhpcy5wYXJ0U2l6ZVxuICAgIGZvciAoOzspIHtcbiAgICAgIC8vIHdoaWxlKHRydWUpIHsuLi59IHRocm93cyBsaW50aW5nIGVycm9yLlxuICAgICAgLy8gSWYgcGFydFNpemUgaXMgYmlnIGVub3VnaCB0byBhY2NvbW9kYXRlIHRoZSBvYmplY3Qgc2l6ZSwgdGhlbiB1c2UgaXQuXG4gICAgICBpZiAocGFydFNpemUgKiAxMDAwMCA+IHNpemUpIHtcbiAgICAgICAgcmV0dXJuIHBhcnRTaXplXG4gICAgICB9XG4gICAgICAvLyBUcnkgcGFydCBzaXplcyBhcyA2NE1CLCA4ME1CLCA5Nk1CIGV0Yy5cbiAgICAgIHBhcnRTaXplICs9IDE2ICogMTAyNCAqIDEwMjRcbiAgICB9XG4gIH1cblxuICAvKipcbiAgICogVXBsb2FkcyB0aGUgb2JqZWN0IHVzaW5nIGNvbnRlbnRzIGZyb20gYSBmaWxlXG4gICAqL1xuICBhc3luYyBmUHV0T2JqZWN0KGJ1Y2tldE5hbWU6IHN0cmluZywgb2JqZWN0TmFtZTogc3RyaW5nLCBmaWxlUGF0aDogc3RyaW5nLCBtZXRhRGF0YT86IE9iamVjdE1ldGFEYXRhKSB7XG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZShidWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKCdJbnZhbGlkIGJ1Y2tldCBuYW1lOiAnICsgYnVja2V0TmFtZSlcbiAgICB9XG4gICAgaWYgKCFpc1ZhbGlkT2JqZWN0TmFtZShvYmplY3ROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkT2JqZWN0TmFtZUVycm9yKGBJbnZhbGlkIG9iamVjdCBuYW1lOiAke29iamVjdE5hbWV9YClcbiAgICB9XG5cbiAgICBpZiAoIWlzU3RyaW5nKGZpbGVQYXRoKSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcignZmlsZVBhdGggc2hvdWxkIGJlIG9mIHR5cGUgXCJzdHJpbmdcIicpXG4gICAgfVxuICAgIGlmIChtZXRhRGF0YSAmJiAhaXNPYmplY3QobWV0YURhdGEpKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdtZXRhRGF0YSBzaG91bGQgYmUgb2YgdHlwZSBcIm9iamVjdFwiJylcbiAgICB9XG5cbiAgICAvLyBJbnNlcnRzIGNvcnJlY3QgYGNvbnRlbnQtdHlwZWAgYXR0cmlidXRlIGJhc2VkIG9uIG1ldGFEYXRhIGFuZCBmaWxlUGF0aFxuICAgIG1ldGFEYXRhID0gaW5zZXJ0Q29udGVudFR5cGUobWV0YURhdGEgfHwge30sIGZpbGVQYXRoKVxuICAgIGNvbnN0IHN0YXQgPSBhd2FpdCBmc3Auc3RhdChmaWxlUGF0aClcbiAgICByZXR1cm4gYXdhaXQgdGhpcy5wdXRPYmplY3QoYnVja2V0TmFtZSwgb2JqZWN0TmFtZSwgZnMuY3JlYXRlUmVhZFN0cmVhbShmaWxlUGF0aCksIHN0YXQuc2l6ZSwgbWV0YURhdGEpXG4gIH1cblxuICAvKipcbiAgICogIFVwbG9hZGluZyBhIHN0cmVhbSwgXCJCdWZmZXJcIiBvciBcInN0cmluZ1wiLlxuICAgKiAgSXQncyByZWNvbW1lbmRlZCB0byBwYXNzIGBzaXplYCBhcmd1bWVudCB3aXRoIHN0cmVhbS5cbiAgICovXG4gIGFzeW5jIHB1dE9iamVjdChcbiAgICBidWNrZXROYW1lOiBzdHJpbmcsXG4gICAgb2JqZWN0TmFtZTogc3RyaW5nLFxuICAgIHN0cmVhbTogc3RyZWFtLlJlYWRhYmxlIHwgQnVmZmVyIHwgc3RyaW5nLFxuICAgIHNpemU/OiBudW1iZXIsXG4gICAgbWV0YURhdGE/OiBJdGVtQnVja2V0TWV0YWRhdGEsXG4gICk6IFByb21pc2U8VXBsb2FkZWRPYmplY3RJbmZvPiB7XG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZShidWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKGBJbnZhbGlkIGJ1Y2tldCBuYW1lOiAke2J1Y2tldE5hbWV9YClcbiAgICB9XG4gICAgaWYgKCFpc1ZhbGlkT2JqZWN0TmFtZShvYmplY3ROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkT2JqZWN0TmFtZUVycm9yKGBJbnZhbGlkIG9iamVjdCBuYW1lOiAke29iamVjdE5hbWV9YClcbiAgICB9XG5cbiAgICAvLyBXZSdsbCBuZWVkIHRvIHNoaWZ0IGFyZ3VtZW50cyB0byB0aGUgbGVmdCBiZWNhdXNlIG9mIG1ldGFEYXRhXG4gICAgLy8gYW5kIHNpemUgYmVpbmcgb3B0aW9uYWwuXG4gICAgaWYgKGlzT2JqZWN0KHNpemUpKSB7XG4gICAgICBtZXRhRGF0YSA9IHNpemVcbiAgICB9XG4gICAgLy8gRW5zdXJlcyBNZXRhZGF0YSBoYXMgYXBwcm9wcmlhdGUgcHJlZml4IGZvciBBMyBBUElcbiAgICBjb25zdCBoZWFkZXJzID0gcHJlcGVuZFhBTVpNZXRhKG1ldGFEYXRhKVxuICAgIGlmICh0eXBlb2Ygc3RyZWFtID09PSAnc3RyaW5nJyB8fCBzdHJlYW0gaW5zdGFuY2VvZiBCdWZmZXIpIHtcbiAgICAgIC8vIEFkYXB0cyB0aGUgbm9uLXN0cmVhbSBpbnRlcmZhY2UgaW50byBhIHN0cmVhbS5cbiAgICAgIHNpemUgPSBzdHJlYW0ubGVuZ3RoXG4gICAgICBzdHJlYW0gPSByZWFkYWJsZVN0cmVhbShzdHJlYW0pXG4gICAgfSBlbHNlIGlmICghaXNSZWFkYWJsZVN0cmVhbShzdHJlYW0pKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCd0aGlyZCBhcmd1bWVudCBzaG91bGQgYmUgb2YgdHlwZSBcInN0cmVhbS5SZWFkYWJsZVwiIG9yIFwiQnVmZmVyXCIgb3IgXCJzdHJpbmdcIicpXG4gICAgfVxuXG4gICAgaWYgKGlzTnVtYmVyKHNpemUpICYmIHNpemUgPCAwKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKGBzaXplIGNhbm5vdCBiZSBuZWdhdGl2ZSwgZ2l2ZW4gc2l6ZTogJHtzaXplfWApXG4gICAgfVxuXG4gICAgLy8gR2V0IHRoZSBwYXJ0IHNpemUgYW5kIGZvcndhcmQgdGhhdCB0byB0aGUgQmxvY2tTdHJlYW0uIERlZmF1bHQgdG8gdGhlXG4gICAgLy8gbGFyZ2VzdCBibG9jayBzaXplIHBvc3NpYmxlIGlmIG5lY2Vzc2FyeS5cbiAgICBpZiAoIWlzTnVtYmVyKHNpemUpKSB7XG4gICAgICBzaXplID0gdGhpcy5tYXhPYmplY3RTaXplXG4gICAgfVxuXG4gICAgLy8gR2V0IHRoZSBwYXJ0IHNpemUgYW5kIGZvcndhcmQgdGhhdCB0byB0aGUgQmxvY2tTdHJlYW0uIERlZmF1bHQgdG8gdGhlXG4gICAgLy8gbGFyZ2VzdCBibG9jayBzaXplIHBvc3NpYmxlIGlmIG5lY2Vzc2FyeS5cbiAgICBpZiAoc2l6ZSA9PT0gdW5kZWZpbmVkKSB7XG4gICAgICBjb25zdCBzdGF0U2l6ZSA9IGF3YWl0IGdldENvbnRlbnRMZW5ndGgoc3RyZWFtKVxuICAgICAgaWYgKHN0YXRTaXplICE9PSBudWxsKSB7XG4gICAgICAgIHNpemUgPSBzdGF0U2l6ZVxuICAgICAgfVxuICAgIH1cblxuICAgIGlmICghaXNOdW1iZXIoc2l6ZSkpIHtcbiAgICAgIC8vIEJhY2t3YXJkIGNvbXBhdGliaWxpdHlcbiAgICAgIHNpemUgPSB0aGlzLm1heE9iamVjdFNpemVcbiAgICB9XG4gICAgaWYgKHNpemUgPT09IDApIHtcbiAgICAgIHJldHVybiB0aGlzLnVwbG9hZEJ1ZmZlcihidWNrZXROYW1lLCBvYmplY3ROYW1lLCBoZWFkZXJzLCBCdWZmZXIuZnJvbSgnJykpXG4gICAgfVxuXG4gICAgY29uc3QgcGFydFNpemUgPSB0aGlzLmNhbGN1bGF0ZVBhcnRTaXplKHNpemUpXG4gICAgaWYgKHR5cGVvZiBzdHJlYW0gPT09ICdzdHJpbmcnIHx8IEJ1ZmZlci5pc0J1ZmZlcihzdHJlYW0pIHx8IHNpemUgPD0gcGFydFNpemUpIHtcbiAgICAgIGNvbnN0IGJ1ZiA9IGlzUmVhZGFibGVTdHJlYW0oc3RyZWFtKSA/IGF3YWl0IHJlYWRBc0J1ZmZlcihzdHJlYW0pIDogQnVmZmVyLmZyb20oc3RyZWFtKVxuICAgICAgcmV0dXJuIHRoaXMudXBsb2FkQnVmZmVyKGJ1Y2tldE5hbWUsIG9iamVjdE5hbWUsIGhlYWRlcnMsIGJ1ZilcbiAgICB9XG5cbiAgICByZXR1cm4gdGhpcy51cGxvYWRTdHJlYW0oYnVja2V0TmFtZSwgb2JqZWN0TmFtZSwgaGVhZGVycywgc3RyZWFtLCBwYXJ0U2l6ZSlcbiAgfVxuXG4gIC8qKlxuICAgKiBtZXRob2QgdG8gdXBsb2FkIGJ1ZmZlciBpbiBvbmUgY2FsbFxuICAgKiBAcHJpdmF0ZVxuICAgKi9cbiAgcHJpdmF0ZSBhc3luYyB1cGxvYWRCdWZmZXIoXG4gICAgYnVja2V0TmFtZTogc3RyaW5nLFxuICAgIG9iamVjdE5hbWU6IHN0cmluZyxcbiAgICBoZWFkZXJzOiBSZXF1ZXN0SGVhZGVycyxcbiAgICBidWY6IEJ1ZmZlcixcbiAgKTogUHJvbWlzZTxVcGxvYWRlZE9iamVjdEluZm8+IHtcbiAgICBjb25zdCB7IG1kNXN1bSwgc2hhMjU2c3VtIH0gPSBoYXNoQmluYXJ5KGJ1ZiwgdGhpcy5lbmFibGVTSEEyNTYpXG4gICAgaGVhZGVyc1snQ29udGVudC1MZW5ndGgnXSA9IGJ1Zi5sZW5ndGhcbiAgICBpZiAoIXRoaXMuZW5hYmxlU0hBMjU2KSB7XG4gICAgICBoZWFkZXJzWydDb250ZW50LU1ENSddID0gbWQ1c3VtXG4gICAgfVxuICAgIGNvbnN0IHJlcyA9IGF3YWl0IHRoaXMubWFrZVJlcXVlc3RTdHJlYW1Bc3luYyhcbiAgICAgIHtcbiAgICAgICAgbWV0aG9kOiAnUFVUJyxcbiAgICAgICAgYnVja2V0TmFtZSxcbiAgICAgICAgb2JqZWN0TmFtZSxcbiAgICAgICAgaGVhZGVycyxcbiAgICAgIH0sXG4gICAgICBidWYsXG4gICAgICBzaGEyNTZzdW0sXG4gICAgICBbMjAwXSxcbiAgICAgICcnLFxuICAgIClcbiAgICBhd2FpdCBkcmFpblJlc3BvbnNlKHJlcylcbiAgICByZXR1cm4ge1xuICAgICAgZXRhZzogc2FuaXRpemVFVGFnKHJlcy5oZWFkZXJzLmV0YWcpLFxuICAgICAgdmVyc2lvbklkOiBnZXRWZXJzaW9uSWQocmVzLmhlYWRlcnMgYXMgUmVzcG9uc2VIZWFkZXIpLFxuICAgIH1cbiAgfVxuXG4gIC8qKlxuICAgKiB1cGxvYWQgc3RyZWFtIHdpdGggTXVsdGlwYXJ0VXBsb2FkXG4gICAqIEBwcml2YXRlXG4gICAqL1xuICBwcml2YXRlIGFzeW5jIHVwbG9hZFN0cmVhbShcbiAgICBidWNrZXROYW1lOiBzdHJpbmcsXG4gICAgb2JqZWN0TmFtZTogc3RyaW5nLFxuICAgIGhlYWRlcnM6IFJlcXVlc3RIZWFkZXJzLFxuICAgIGJvZHk6IHN0cmVhbS5SZWFkYWJsZSxcbiAgICBwYXJ0U2l6ZTogbnVtYmVyLFxuICApOiBQcm9taXNlPFVwbG9hZGVkT2JqZWN0SW5mbz4ge1xuICAgIC8vIEEgbWFwIG9mIHRoZSBwcmV2aW91c2x5IHVwbG9hZGVkIGNodW5rcywgZm9yIHJlc3VtaW5nIGEgZmlsZSB1cGxvYWQuIFRoaXNcbiAgICAvLyB3aWxsIGJlIG51bGwgaWYgd2UgYXJlbid0IHJlc3VtaW5nIGFuIHVwbG9hZC5cbiAgICBjb25zdCBvbGRQYXJ0czogUmVjb3JkPG51bWJlciwgUGFydD4gPSB7fVxuXG4gICAgLy8gS2VlcCB0cmFjayBvZiB0aGUgZXRhZ3MgZm9yIGFnZ3JlZ2F0aW5nIHRoZSBjaHVua3MgdG9nZXRoZXIgbGF0ZXIuIEVhY2hcbiAgICAvLyBldGFnIHJlcHJlc2VudHMgYSBzaW5nbGUgY2h1bmsgb2YgdGhlIGZpbGUuXG4gICAgY29uc3QgZVRhZ3M6IFBhcnRbXSA9IFtdXG5cbiAgICBjb25zdCBwcmV2aW91c1VwbG9hZElkID0gYXdhaXQgdGhpcy5maW5kVXBsb2FkSWQoYnVja2V0TmFtZSwgb2JqZWN0TmFtZSlcbiAgICBsZXQgdXBsb2FkSWQ6IHN0cmluZ1xuICAgIGlmICghcHJldmlvdXNVcGxvYWRJZCkge1xuICAgICAgdXBsb2FkSWQgPSBhd2FpdCB0aGlzLmluaXRpYXRlTmV3TXVsdGlwYXJ0VXBsb2FkKGJ1Y2tldE5hbWUsIG9iamVjdE5hbWUsIGhlYWRlcnMpXG4gICAgfSBlbHNlIHtcbiAgICAgIHVwbG9hZElkID0gcHJldmlvdXNVcGxvYWRJZFxuICAgICAgY29uc3Qgb2xkVGFncyA9IGF3YWl0IHRoaXMubGlzdFBhcnRzKGJ1Y2tldE5hbWUsIG9iamVjdE5hbWUsIHByZXZpb3VzVXBsb2FkSWQpXG4gICAgICBvbGRUYWdzLmZvckVhY2goKGUpID0+IHtcbiAgICAgICAgb2xkUGFydHNbZS5wYXJ0XSA9IGVcbiAgICAgIH0pXG4gICAgfVxuXG4gICAgaWYgKCFpc1JlYWRhYmxlU3RyZWFtKGJvZHkpKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCd0aGlyZCBhcmd1bWVudCBzaG91bGQgYmUgb2YgdHlwZSBcInN0cmVhbS5SZWFkYWJsZVwiIG9yIFwiQnVmZmVyXCIgb3IgXCJzdHJpbmdcIicpXG4gICAgfVxuXG4gICAgY29uc3QgY2h1bmtpZXIgPSBuZXcgQmxvY2tTdHJlYW0yKHsgc2l6ZTogcGFydFNpemUsIHplcm9QYWRkaW5nOiBmYWxzZSB9KVxuXG4gICAgLy8gZXNsaW50LWRpc2FibGUtbmV4dC1saW5lIEB0eXBlc2NyaXB0LWVzbGludC9uby11bnVzZWQtdmFyc1xuICAgIGNvbnN0IFtfLCBvXSA9IGF3YWl0IFByb21pc2UuYWxsKFtcbiAgICAgIG5ldyBQcm9taXNlKChyZXNvbHZlLCByZWplY3QpID0+IHtcbiAgICAgICAgYm9keS5waXBlKGNodW5raWVyKS5vbignZXJyb3InLCByZWplY3QpXG4gICAgICAgIGNodW5raWVyLm9uKCdlbmQnLCByZXNvbHZlKS5vbignZXJyb3InLCByZWplY3QpXG4gICAgICB9KSxcbiAgICAgIChhc3luYyAoKSA9PiB7XG4gICAgICAgIGxldCBwYXJ0TnVtYmVyID0gMFxuXG4gICAgICAgIGZvciBhd2FpdCAoY29uc3QgY2h1bmsgb2YgY2h1bmtpZXIpIHtcbiAgICAgICAgICBjb25zdCBtZDUgPSBjcnlwdG8uY3JlYXRlSGFzaCgnbWQ1JykudXBkYXRlKGNodW5rKS5kaWdlc3QoKVxuXG4gICAgICAgICAgcGFydE51bWJlcisrXG4gICAgICAgICAgY29uc3Qgb2xkUGFydCA9IG9sZFBhcnRzW3BhcnROdW1iZXJdXG4gICAgICAgICAgaWYgKG9sZFBhcnQpIHtcbiAgICAgICAgICAgIGlmIChvbGRQYXJ0LmV0YWcgPT09IG1kNS50b1N0cmluZygnaGV4JykpIHtcbiAgICAgICAgICAgICAgZVRhZ3MucHVzaCh7IHBhcnQ6IHBhcnROdW1iZXIsIGV0YWc6IG9sZFBhcnQuZXRhZyB9KVxuICAgICAgICAgICAgICBjb250aW51ZVxuICAgICAgICAgICAgfVxuICAgICAgICAgIH1cblxuICAgICAgICAgIC8vIG5vdyBzdGFydCB0byB1cGxvYWQgbWlzc2luZyBwYXJ0XG4gICAgICAgICAgY29uc3Qgb3B0aW9uczogUmVxdWVzdE9wdGlvbiA9IHtcbiAgICAgICAgICAgIG1ldGhvZDogJ1BVVCcsXG4gICAgICAgICAgICBxdWVyeTogcXMuc3RyaW5naWZ5KHsgcGFydE51bWJlciwgdXBsb2FkSWQgfSksXG4gICAgICAgICAgICBoZWFkZXJzOiB7XG4gICAgICAgICAgICAgICdDb250ZW50LUxlbmd0aCc6IGNodW5rLmxlbmd0aCxcbiAgICAgICAgICAgICAgJ0NvbnRlbnQtTUQ1JzogbWQ1LnRvU3RyaW5nKCdiYXNlNjQnKSxcbiAgICAgICAgICAgIH0sXG4gICAgICAgICAgICBidWNrZXROYW1lLFxuICAgICAgICAgICAgb2JqZWN0TmFtZSxcbiAgICAgICAgICB9XG5cbiAgICAgICAgICBjb25zdCByZXNwb25zZSA9IGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luY09taXQob3B0aW9ucywgY2h1bmspXG5cbiAgICAgICAgICBsZXQgZXRhZyA9IHJlc3BvbnNlLmhlYWRlcnMuZXRhZ1xuICAgICAgICAgIGlmIChldGFnKSB7XG4gICAgICAgICAgICBldGFnID0gZXRhZy5yZXBsYWNlKC9eXCIvLCAnJykucmVwbGFjZSgvXCIkLywgJycpXG4gICAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICAgIGV0YWcgPSAnJ1xuICAgICAgICAgIH1cblxuICAgICAgICAgIGVUYWdzLnB1c2goeyBwYXJ0OiBwYXJ0TnVtYmVyLCBldGFnIH0pXG5cbiAgICAgICAgICBwYXJ0TnVtYmVyKytcbiAgICAgICAgfVxuXG4gICAgICAgIGlmIChlVGFncy5sZW5ndGggPT09IDApIHtcbiAgICAgICAgICBhd2FpdCB0aGlzLmFib3J0TXVsdGlwYXJ0VXBsb2FkKGJ1Y2tldE5hbWUsIG9iamVjdE5hbWUsIHVwbG9hZElkKVxuICAgICAgICAgIHJldHVybiBhd2FpdCB0aGlzLnVwbG9hZEJ1ZmZlcihidWNrZXROYW1lLCBvYmplY3ROYW1lLCBoZWFkZXJzLCBCdWZmZXIuZnJvbSgnJykpXG4gICAgICAgIH1cblxuICAgICAgICByZXR1cm4gYXdhaXQgdGhpcy5jb21wbGV0ZU11bHRpcGFydFVwbG9hZChidWNrZXROYW1lLCBvYmplY3ROYW1lLCB1cGxvYWRJZCwgZVRhZ3MpXG4gICAgICB9KSgpLFxuICAgIF0pXG5cbiAgICByZXR1cm4gb1xuICB9XG5cbiAgYXN5bmMgcmVtb3ZlQnVja2V0UmVwbGljYXRpb24oYnVja2V0TmFtZTogc3RyaW5nKTogUHJvbWlzZTx2b2lkPlxuICByZW1vdmVCdWNrZXRSZXBsaWNhdGlvbihidWNrZXROYW1lOiBzdHJpbmcsIGNhbGxiYWNrOiBOb1Jlc3VsdENhbGxiYWNrKTogdm9pZFxuICBhc3luYyByZW1vdmVCdWNrZXRSZXBsaWNhdGlvbihidWNrZXROYW1lOiBzdHJpbmcpOiBQcm9taXNlPHZvaWQ+IHtcbiAgICBpZiAoIWlzVmFsaWRCdWNrZXROYW1lKGJ1Y2tldE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IoJ0ludmFsaWQgYnVja2V0IG5hbWU6ICcgKyBidWNrZXROYW1lKVxuICAgIH1cbiAgICBjb25zdCBtZXRob2QgPSAnREVMRVRFJ1xuICAgIGNvbnN0IHF1ZXJ5ID0gJ3JlcGxpY2F0aW9uJ1xuICAgIGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luY09taXQoeyBtZXRob2QsIGJ1Y2tldE5hbWUsIHF1ZXJ5IH0sICcnLCBbMjAwLCAyMDRdLCAnJylcbiAgfVxuXG4gIHNldEJ1Y2tldFJlcGxpY2F0aW9uKGJ1Y2tldE5hbWU6IHN0cmluZywgcmVwbGljYXRpb25Db25maWc6IFJlcGxpY2F0aW9uQ29uZmlnT3B0cyk6IHZvaWRcbiAgYXN5bmMgc2V0QnVja2V0UmVwbGljYXRpb24oYnVja2V0TmFtZTogc3RyaW5nLCByZXBsaWNhdGlvbkNvbmZpZzogUmVwbGljYXRpb25Db25maWdPcHRzKTogUHJvbWlzZTx2b2lkPlxuICBhc3luYyBzZXRCdWNrZXRSZXBsaWNhdGlvbihidWNrZXROYW1lOiBzdHJpbmcsIHJlcGxpY2F0aW9uQ29uZmlnOiBSZXBsaWNhdGlvbkNvbmZpZ09wdHMpIHtcbiAgICBpZiAoIWlzVmFsaWRCdWNrZXROYW1lKGJ1Y2tldE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IoJ0ludmFsaWQgYnVja2V0IG5hbWU6ICcgKyBidWNrZXROYW1lKVxuICAgIH1cbiAgICBpZiAoIWlzT2JqZWN0KHJlcGxpY2F0aW9uQ29uZmlnKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQXJndW1lbnRFcnJvcigncmVwbGljYXRpb25Db25maWcgc2hvdWxkIGJlIG9mIHR5cGUgXCJvYmplY3RcIicpXG4gICAgfSBlbHNlIHtcbiAgICAgIGlmIChfLmlzRW1wdHkocmVwbGljYXRpb25Db25maWcucm9sZSkpIHtcbiAgICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQXJndW1lbnRFcnJvcignUm9sZSBjYW5ub3QgYmUgZW1wdHknKVxuICAgICAgfSBlbHNlIGlmIChyZXBsaWNhdGlvbkNvbmZpZy5yb2xlICYmICFpc1N0cmluZyhyZXBsaWNhdGlvbkNvbmZpZy5yb2xlKSkge1xuICAgICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKCdJbnZhbGlkIHZhbHVlIGZvciByb2xlJywgcmVwbGljYXRpb25Db25maWcucm9sZSlcbiAgICAgIH1cbiAgICAgIGlmIChfLmlzRW1wdHkocmVwbGljYXRpb25Db25maWcucnVsZXMpKSB7XG4gICAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEFyZ3VtZW50RXJyb3IoJ01pbmltdW0gb25lIHJlcGxpY2F0aW9uIHJ1bGUgbXVzdCBiZSBzcGVjaWZpZWQnKVxuICAgICAgfVxuICAgIH1cbiAgICBjb25zdCBtZXRob2QgPSAnUFVUJ1xuICAgIGNvbnN0IHF1ZXJ5ID0gJ3JlcGxpY2F0aW9uJ1xuICAgIGNvbnN0IGhlYWRlcnM6IFJlY29yZDxzdHJpbmcsIHN0cmluZz4gPSB7fVxuXG4gICAgY29uc3QgcmVwbGljYXRpb25QYXJhbXNDb25maWcgPSB7XG4gICAgICBSZXBsaWNhdGlvbkNvbmZpZ3VyYXRpb246IHtcbiAgICAgICAgUm9sZTogcmVwbGljYXRpb25Db25maWcucm9sZSxcbiAgICAgICAgUnVsZTogcmVwbGljYXRpb25Db25maWcucnVsZXMsXG4gICAgICB9LFxuICAgIH1cblxuICAgIGNvbnN0IGJ1aWxkZXIgPSBuZXcgeG1sMmpzLkJ1aWxkZXIoeyByZW5kZXJPcHRzOiB7IHByZXR0eTogZmFsc2UgfSwgaGVhZGxlc3M6IHRydWUgfSlcbiAgICBjb25zdCBwYXlsb2FkID0gYnVpbGRlci5idWlsZE9iamVjdChyZXBsaWNhdGlvblBhcmFtc0NvbmZpZylcbiAgICBoZWFkZXJzWydDb250ZW50LU1ENSddID0gdG9NZDUocGF5bG9hZClcbiAgICBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0QXN5bmNPbWl0KHsgbWV0aG9kLCBidWNrZXROYW1lLCBxdWVyeSwgaGVhZGVycyB9LCBwYXlsb2FkKVxuICB9XG5cbiAgZ2V0QnVja2V0UmVwbGljYXRpb24oYnVja2V0TmFtZTogc3RyaW5nKTogdm9pZFxuICBhc3luYyBnZXRCdWNrZXRSZXBsaWNhdGlvbihidWNrZXROYW1lOiBzdHJpbmcpOiBQcm9taXNlPFJlcGxpY2F0aW9uQ29uZmlnPlxuICBhc3luYyBnZXRCdWNrZXRSZXBsaWNhdGlvbihidWNrZXROYW1lOiBzdHJpbmcpIHtcbiAgICBpZiAoIWlzVmFsaWRCdWNrZXROYW1lKGJ1Y2tldE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IoJ0ludmFsaWQgYnVja2V0IG5hbWU6ICcgKyBidWNrZXROYW1lKVxuICAgIH1cbiAgICBjb25zdCBtZXRob2QgPSAnR0VUJ1xuICAgIGNvbnN0IHF1ZXJ5ID0gJ3JlcGxpY2F0aW9uJ1xuXG4gICAgY29uc3QgaHR0cFJlcyA9IGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luYyh7IG1ldGhvZCwgYnVja2V0TmFtZSwgcXVlcnkgfSwgJycsIFsyMDAsIDIwNF0pXG4gICAgY29uc3QgeG1sUmVzdWx0ID0gYXdhaXQgcmVhZEFzU3RyaW5nKGh0dHBSZXMpXG4gICAgcmV0dXJuIHhtbFBhcnNlcnMucGFyc2VSZXBsaWNhdGlvbkNvbmZpZyh4bWxSZXN1bHQpXG4gIH1cblxuICBnZXRPYmplY3RMZWdhbEhvbGQoXG4gICAgYnVja2V0TmFtZTogc3RyaW5nLFxuICAgIG9iamVjdE5hbWU6IHN0cmluZyxcbiAgICBnZXRPcHRzPzogR2V0T2JqZWN0TGVnYWxIb2xkT3B0aW9ucyxcbiAgICBjYWxsYmFjaz86IFJlc3VsdENhbGxiYWNrPExFR0FMX0hPTERfU1RBVFVTPixcbiAgKTogUHJvbWlzZTxMRUdBTF9IT0xEX1NUQVRVUz5cbiAgYXN5bmMgZ2V0T2JqZWN0TGVnYWxIb2xkKFxuICAgIGJ1Y2tldE5hbWU6IHN0cmluZyxcbiAgICBvYmplY3ROYW1lOiBzdHJpbmcsXG4gICAgZ2V0T3B0cz86IEdldE9iamVjdExlZ2FsSG9sZE9wdGlvbnMsXG4gICk6IFByb21pc2U8TEVHQUxfSE9MRF9TVEFUVVM+IHtcbiAgICBpZiAoIWlzVmFsaWRCdWNrZXROYW1lKGJ1Y2tldE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IoJ0ludmFsaWQgYnVja2V0IG5hbWU6ICcgKyBidWNrZXROYW1lKVxuICAgIH1cbiAgICBpZiAoIWlzVmFsaWRPYmplY3ROYW1lKG9iamVjdE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRPYmplY3ROYW1lRXJyb3IoYEludmFsaWQgb2JqZWN0IG5hbWU6ICR7b2JqZWN0TmFtZX1gKVxuICAgIH1cblxuICAgIGlmIChnZXRPcHRzKSB7XG4gICAgICBpZiAoIWlzT2JqZWN0KGdldE9wdHMpKSB7XG4gICAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ2dldE9wdHMgc2hvdWxkIGJlIG9mIHR5cGUgXCJPYmplY3RcIicpXG4gICAgICB9IGVsc2UgaWYgKE9iamVjdC5rZXlzKGdldE9wdHMpLmxlbmd0aCA+IDAgJiYgZ2V0T3B0cy52ZXJzaW9uSWQgJiYgIWlzU3RyaW5nKGdldE9wdHMudmVyc2lvbklkKSkge1xuICAgICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCd2ZXJzaW9uSWQgc2hvdWxkIGJlIG9mIHR5cGUgc3RyaW5nLjonLCBnZXRPcHRzLnZlcnNpb25JZClcbiAgICAgIH1cbiAgICB9XG5cbiAgICBjb25zdCBtZXRob2QgPSAnR0VUJ1xuICAgIGxldCBxdWVyeSA9ICdsZWdhbC1ob2xkJ1xuXG4gICAgaWYgKGdldE9wdHM/LnZlcnNpb25JZCkge1xuICAgICAgcXVlcnkgKz0gYCZ2ZXJzaW9uSWQ9JHtnZXRPcHRzLnZlcnNpb25JZH1gXG4gICAgfVxuXG4gICAgY29uc3QgaHR0cFJlcyA9IGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luYyh7IG1ldGhvZCwgYnVja2V0TmFtZSwgb2JqZWN0TmFtZSwgcXVlcnkgfSwgJycsIFsyMDBdKVxuICAgIGNvbnN0IHN0clJlcyA9IGF3YWl0IHJlYWRBc1N0cmluZyhodHRwUmVzKVxuICAgIHJldHVybiBwYXJzZU9iamVjdExlZ2FsSG9sZENvbmZpZyhzdHJSZXMpXG4gIH1cblxuICBzZXRPYmplY3RMZWdhbEhvbGQoYnVja2V0TmFtZTogc3RyaW5nLCBvYmplY3ROYW1lOiBzdHJpbmcsIHNldE9wdHM/OiBQdXRPYmplY3RMZWdhbEhvbGRPcHRpb25zKTogdm9pZFxuICBhc3luYyBzZXRPYmplY3RMZWdhbEhvbGQoXG4gICAgYnVja2V0TmFtZTogc3RyaW5nLFxuICAgIG9iamVjdE5hbWU6IHN0cmluZyxcbiAgICBzZXRPcHRzID0ge1xuICAgICAgc3RhdHVzOiBMRUdBTF9IT0xEX1NUQVRVUy5FTkFCTEVELFxuICAgIH0gYXMgUHV0T2JqZWN0TGVnYWxIb2xkT3B0aW9ucyxcbiAgKTogUHJvbWlzZTx2b2lkPiB7XG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZShidWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKCdJbnZhbGlkIGJ1Y2tldCBuYW1lOiAnICsgYnVja2V0TmFtZSlcbiAgICB9XG4gICAgaWYgKCFpc1ZhbGlkT2JqZWN0TmFtZShvYmplY3ROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkT2JqZWN0TmFtZUVycm9yKGBJbnZhbGlkIG9iamVjdCBuYW1lOiAke29iamVjdE5hbWV9YClcbiAgICB9XG5cbiAgICBpZiAoIWlzT2JqZWN0KHNldE9wdHMpKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdzZXRPcHRzIHNob3VsZCBiZSBvZiB0eXBlIFwiT2JqZWN0XCInKVxuICAgIH0gZWxzZSB7XG4gICAgICBpZiAoIVtMRUdBTF9IT0xEX1NUQVRVUy5FTkFCTEVELCBMRUdBTF9IT0xEX1NUQVRVUy5ESVNBQkxFRF0uaW5jbHVkZXMoc2V0T3B0cz8uc3RhdHVzKSkge1xuICAgICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdJbnZhbGlkIHN0YXR1czogJyArIHNldE9wdHMuc3RhdHVzKVxuICAgICAgfVxuICAgICAgaWYgKHNldE9wdHMudmVyc2lvbklkICYmICFzZXRPcHRzLnZlcnNpb25JZC5sZW5ndGgpIHtcbiAgICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcigndmVyc2lvbklkIHNob3VsZCBiZSBvZiB0eXBlIHN0cmluZy46JyArIHNldE9wdHMudmVyc2lvbklkKVxuICAgICAgfVxuICAgIH1cblxuICAgIGNvbnN0IG1ldGhvZCA9ICdQVVQnXG4gICAgbGV0IHF1ZXJ5ID0gJ2xlZ2FsLWhvbGQnXG5cbiAgICBpZiAoc2V0T3B0cy52ZXJzaW9uSWQpIHtcbiAgICAgIHF1ZXJ5ICs9IGAmdmVyc2lvbklkPSR7c2V0T3B0cy52ZXJzaW9uSWR9YFxuICAgIH1cblxuICAgIGNvbnN0IGNvbmZpZyA9IHtcbiAgICAgIFN0YXR1czogc2V0T3B0cy5zdGF0dXMsXG4gICAgfVxuXG4gICAgY29uc3QgYnVpbGRlciA9IG5ldyB4bWwyanMuQnVpbGRlcih7IHJvb3ROYW1lOiAnTGVnYWxIb2xkJywgcmVuZGVyT3B0czogeyBwcmV0dHk6IGZhbHNlIH0sIGhlYWRsZXNzOiB0cnVlIH0pXG4gICAgY29uc3QgcGF5bG9hZCA9IGJ1aWxkZXIuYnVpbGRPYmplY3QoY29uZmlnKVxuICAgIGNvbnN0IGhlYWRlcnM6IFJlY29yZDxzdHJpbmcsIHN0cmluZz4gPSB7fVxuICAgIGhlYWRlcnNbJ0NvbnRlbnQtTUQ1J10gPSB0b01kNShwYXlsb2FkKVxuXG4gICAgYXdhaXQgdGhpcy5tYWtlUmVxdWVzdEFzeW5jT21pdCh7IG1ldGhvZCwgYnVja2V0TmFtZSwgb2JqZWN0TmFtZSwgcXVlcnksIGhlYWRlcnMgfSwgcGF5bG9hZClcbiAgfVxuXG4gIC8qKlxuICAgKiBHZXQgVGFncyBhc3NvY2lhdGVkIHdpdGggYSBCdWNrZXRcbiAgICovXG4gIGFzeW5jIGdldEJ1Y2tldFRhZ2dpbmcoYnVja2V0TmFtZTogc3RyaW5nKTogUHJvbWlzZTxUYWdbXT4ge1xuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcihgSW52YWxpZCBidWNrZXQgbmFtZTogJHtidWNrZXROYW1lfWApXG4gICAgfVxuXG4gICAgY29uc3QgbWV0aG9kID0gJ0dFVCdcbiAgICBjb25zdCBxdWVyeSA9ICd0YWdnaW5nJ1xuICAgIGNvbnN0IHJlcXVlc3RPcHRpb25zID0geyBtZXRob2QsIGJ1Y2tldE5hbWUsIHF1ZXJ5IH1cblxuICAgIGNvbnN0IHJlc3BvbnNlID0gYXdhaXQgdGhpcy5tYWtlUmVxdWVzdEFzeW5jKHJlcXVlc3RPcHRpb25zKVxuICAgIGNvbnN0IGJvZHkgPSBhd2FpdCByZWFkQXNTdHJpbmcocmVzcG9uc2UpXG4gICAgcmV0dXJuIHhtbFBhcnNlcnMucGFyc2VUYWdnaW5nKGJvZHkpXG4gIH1cblxuICAvKipcbiAgICogIEdldCB0aGUgdGFncyBhc3NvY2lhdGVkIHdpdGggYSBidWNrZXQgT1IgYW4gb2JqZWN0XG4gICAqL1xuICBhc3luYyBnZXRPYmplY3RUYWdnaW5nKGJ1Y2tldE5hbWU6IHN0cmluZywgb2JqZWN0TmFtZTogc3RyaW5nLCBnZXRPcHRzPzogR2V0T2JqZWN0T3B0cyk6IFByb21pc2U8VGFnW10+IHtcbiAgICBjb25zdCBtZXRob2QgPSAnR0VUJ1xuICAgIGxldCBxdWVyeSA9ICd0YWdnaW5nJ1xuXG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZShidWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKCdJbnZhbGlkIGJ1Y2tldCBuYW1lOiAnICsgYnVja2V0TmFtZSlcbiAgICB9XG4gICAgaWYgKCFpc1ZhbGlkT2JqZWN0TmFtZShvYmplY3ROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKCdJbnZhbGlkIG9iamVjdCBuYW1lOiAnICsgb2JqZWN0TmFtZSlcbiAgICB9XG4gICAgaWYgKGdldE9wdHMgJiYgIWlzT2JqZWN0KGdldE9wdHMpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKCdnZXRPcHRzIHNob3VsZCBiZSBvZiB0eXBlIFwib2JqZWN0XCInKVxuICAgIH1cblxuICAgIGlmIChnZXRPcHRzICYmIGdldE9wdHMudmVyc2lvbklkKSB7XG4gICAgICBxdWVyeSA9IGAke3F1ZXJ5fSZ2ZXJzaW9uSWQ9JHtnZXRPcHRzLnZlcnNpb25JZH1gXG4gICAgfVxuICAgIGNvbnN0IHJlcXVlc3RPcHRpb25zOiBSZXF1ZXN0T3B0aW9uID0geyBtZXRob2QsIGJ1Y2tldE5hbWUsIHF1ZXJ5IH1cbiAgICBpZiAob2JqZWN0TmFtZSkge1xuICAgICAgcmVxdWVzdE9wdGlvbnNbJ29iamVjdE5hbWUnXSA9IG9iamVjdE5hbWVcbiAgICB9XG5cbiAgICBjb25zdCByZXNwb25zZSA9IGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luYyhyZXF1ZXN0T3B0aW9ucylcbiAgICBjb25zdCBib2R5ID0gYXdhaXQgcmVhZEFzU3RyaW5nKHJlc3BvbnNlKVxuICAgIHJldHVybiB4bWxQYXJzZXJzLnBhcnNlVGFnZ2luZyhib2R5KVxuICB9XG5cbiAgLyoqXG4gICAqICBTZXQgdGhlIHBvbGljeSBvbiBhIGJ1Y2tldCBvciBhbiBvYmplY3QgcHJlZml4LlxuICAgKi9cbiAgYXN5bmMgc2V0QnVja2V0UG9saWN5KGJ1Y2tldE5hbWU6IHN0cmluZywgcG9saWN5OiBzdHJpbmcpOiBQcm9taXNlPHZvaWQ+IHtcbiAgICAvLyBWYWxpZGF0ZSBhcmd1bWVudHMuXG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZShidWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKGBJbnZhbGlkIGJ1Y2tldCBuYW1lOiAke2J1Y2tldE5hbWV9YClcbiAgICB9XG4gICAgaWYgKCFpc1N0cmluZyhwb2xpY3kpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRCdWNrZXRQb2xpY3lFcnJvcihgSW52YWxpZCBidWNrZXQgcG9saWN5OiAke3BvbGljeX0gLSBtdXN0IGJlIFwic3RyaW5nXCJgKVxuICAgIH1cblxuICAgIGNvbnN0IHF1ZXJ5ID0gJ3BvbGljeSdcblxuICAgIGxldCBtZXRob2QgPSAnREVMRVRFJ1xuICAgIGlmIChwb2xpY3kpIHtcbiAgICAgIG1ldGhvZCA9ICdQVVQnXG4gICAgfVxuXG4gICAgYXdhaXQgdGhpcy5tYWtlUmVxdWVzdEFzeW5jT21pdCh7IG1ldGhvZCwgYnVja2V0TmFtZSwgcXVlcnkgfSwgcG9saWN5LCBbMjA0XSwgJycpXG4gIH1cblxuICAvKipcbiAgICogR2V0IHRoZSBwb2xpY3kgb24gYSBidWNrZXQgb3IgYW4gb2JqZWN0IHByZWZpeC5cbiAgICovXG4gIGFzeW5jIGdldEJ1Y2tldFBvbGljeShidWNrZXROYW1lOiBzdHJpbmcpOiBQcm9taXNlPHN0cmluZz4ge1xuICAgIC8vIFZhbGlkYXRlIGFyZ3VtZW50cy5cbiAgICBpZiAoIWlzVmFsaWRCdWNrZXROYW1lKGJ1Y2tldE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IoYEludmFsaWQgYnVja2V0IG5hbWU6ICR7YnVja2V0TmFtZX1gKVxuICAgIH1cblxuICAgIGNvbnN0IG1ldGhvZCA9ICdHRVQnXG4gICAgY29uc3QgcXVlcnkgPSAncG9saWN5J1xuICAgIGNvbnN0IHJlcyA9IGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luYyh7IG1ldGhvZCwgYnVja2V0TmFtZSwgcXVlcnkgfSlcbiAgICByZXR1cm4gYXdhaXQgcmVhZEFzU3RyaW5nKHJlcylcbiAgfVxuXG4gIGFzeW5jIHB1dE9iamVjdFJldGVudGlvbihidWNrZXROYW1lOiBzdHJpbmcsIG9iamVjdE5hbWU6IHN0cmluZywgcmV0ZW50aW9uT3B0czogUmV0ZW50aW9uID0ge30pOiBQcm9taXNlPHZvaWQ+IHtcbiAgICBpZiAoIWlzVmFsaWRCdWNrZXROYW1lKGJ1Y2tldE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IoYEludmFsaWQgYnVja2V0IG5hbWU6ICR7YnVja2V0TmFtZX1gKVxuICAgIH1cbiAgICBpZiAoIWlzVmFsaWRPYmplY3ROYW1lKG9iamVjdE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRPYmplY3ROYW1lRXJyb3IoYEludmFsaWQgb2JqZWN0IG5hbWU6ICR7b2JqZWN0TmFtZX1gKVxuICAgIH1cbiAgICBpZiAoIWlzT2JqZWN0KHJldGVudGlvbk9wdHMpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKCdyZXRlbnRpb25PcHRzIHNob3VsZCBiZSBvZiB0eXBlIFwib2JqZWN0XCInKVxuICAgIH0gZWxzZSB7XG4gICAgICBpZiAocmV0ZW50aW9uT3B0cy5nb3Zlcm5hbmNlQnlwYXNzICYmICFpc0Jvb2xlYW4ocmV0ZW50aW9uT3B0cy5nb3Zlcm5hbmNlQnlwYXNzKSkge1xuICAgICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKGBJbnZhbGlkIHZhbHVlIGZvciBnb3Zlcm5hbmNlQnlwYXNzOiAke3JldGVudGlvbk9wdHMuZ292ZXJuYW5jZUJ5cGFzc31gKVxuICAgICAgfVxuICAgICAgaWYgKFxuICAgICAgICByZXRlbnRpb25PcHRzLm1vZGUgJiZcbiAgICAgICAgIVtSRVRFTlRJT05fTU9ERVMuQ09NUExJQU5DRSwgUkVURU5USU9OX01PREVTLkdPVkVSTkFOQ0VdLmluY2x1ZGVzKHJldGVudGlvbk9wdHMubW9kZSlcbiAgICAgICkge1xuICAgICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKGBJbnZhbGlkIG9iamVjdCByZXRlbnRpb24gbW9kZTogJHtyZXRlbnRpb25PcHRzLm1vZGV9YClcbiAgICAgIH1cbiAgICAgIGlmIChyZXRlbnRpb25PcHRzLnJldGFpblVudGlsRGF0ZSAmJiAhaXNTdHJpbmcocmV0ZW50aW9uT3B0cy5yZXRhaW5VbnRpbERhdGUpKSB7XG4gICAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEFyZ3VtZW50RXJyb3IoYEludmFsaWQgdmFsdWUgZm9yIHJldGFpblVudGlsRGF0ZTogJHtyZXRlbnRpb25PcHRzLnJldGFpblVudGlsRGF0ZX1gKVxuICAgICAgfVxuICAgICAgaWYgKHJldGVudGlvbk9wdHMudmVyc2lvbklkICYmICFpc1N0cmluZyhyZXRlbnRpb25PcHRzLnZlcnNpb25JZCkpIHtcbiAgICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQXJndW1lbnRFcnJvcihgSW52YWxpZCB2YWx1ZSBmb3IgdmVyc2lvbklkOiAke3JldGVudGlvbk9wdHMudmVyc2lvbklkfWApXG4gICAgICB9XG4gICAgfVxuXG4gICAgY29uc3QgbWV0aG9kID0gJ1BVVCdcbiAgICBsZXQgcXVlcnkgPSAncmV0ZW50aW9uJ1xuXG4gICAgY29uc3QgaGVhZGVyczogUmVxdWVzdEhlYWRlcnMgPSB7fVxuICAgIGlmIChyZXRlbnRpb25PcHRzLmdvdmVybmFuY2VCeXBhc3MpIHtcbiAgICAgIGhlYWRlcnNbJ1gtQW16LUJ5cGFzcy1Hb3Zlcm5hbmNlLVJldGVudGlvbiddID0gdHJ1ZVxuICAgIH1cblxuICAgIGNvbnN0IGJ1aWxkZXIgPSBuZXcgeG1sMmpzLkJ1aWxkZXIoeyByb290TmFtZTogJ1JldGVudGlvbicsIHJlbmRlck9wdHM6IHsgcHJldHR5OiBmYWxzZSB9LCBoZWFkbGVzczogdHJ1ZSB9KVxuICAgIGNvbnN0IHBhcmFtczogUmVjb3JkPHN0cmluZywgc3RyaW5nPiA9IHt9XG5cbiAgICBpZiAocmV0ZW50aW9uT3B0cy5tb2RlKSB7XG4gICAgICBwYXJhbXMuTW9kZSA9IHJldGVudGlvbk9wdHMubW9kZVxuICAgIH1cbiAgICBpZiAocmV0ZW50aW9uT3B0cy5yZXRhaW5VbnRpbERhdGUpIHtcbiAgICAgIHBhcmFtcy5SZXRhaW5VbnRpbERhdGUgPSByZXRlbnRpb25PcHRzLnJldGFpblVudGlsRGF0ZVxuICAgIH1cbiAgICBpZiAocmV0ZW50aW9uT3B0cy52ZXJzaW9uSWQpIHtcbiAgICAgIHF1ZXJ5ICs9IGAmdmVyc2lvbklkPSR7cmV0ZW50aW9uT3B0cy52ZXJzaW9uSWR9YFxuICAgIH1cblxuICAgIGNvbnN0IHBheWxvYWQgPSBidWlsZGVyLmJ1aWxkT2JqZWN0KHBhcmFtcylcblxuICAgIGhlYWRlcnNbJ0NvbnRlbnQtTUQ1J10gPSB0b01kNShwYXlsb2FkKVxuICAgIGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luY09taXQoeyBtZXRob2QsIGJ1Y2tldE5hbWUsIG9iamVjdE5hbWUsIHF1ZXJ5LCBoZWFkZXJzIH0sIHBheWxvYWQsIFsyMDAsIDIwNF0pXG4gIH1cblxuICBnZXRPYmplY3RMb2NrQ29uZmlnKGJ1Y2tldE5hbWU6IHN0cmluZywgY2FsbGJhY2s6IFJlc3VsdENhbGxiYWNrPE9iamVjdExvY2tJbmZvPik6IHZvaWRcbiAgZ2V0T2JqZWN0TG9ja0NvbmZpZyhidWNrZXROYW1lOiBzdHJpbmcpOiB2b2lkXG4gIGFzeW5jIGdldE9iamVjdExvY2tDb25maWcoYnVja2V0TmFtZTogc3RyaW5nKTogUHJvbWlzZTxPYmplY3RMb2NrSW5mbz5cbiAgYXN5bmMgZ2V0T2JqZWN0TG9ja0NvbmZpZyhidWNrZXROYW1lOiBzdHJpbmcpIHtcbiAgICBpZiAoIWlzVmFsaWRCdWNrZXROYW1lKGJ1Y2tldE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IoJ0ludmFsaWQgYnVja2V0IG5hbWU6ICcgKyBidWNrZXROYW1lKVxuICAgIH1cbiAgICBjb25zdCBtZXRob2QgPSAnR0VUJ1xuICAgIGNvbnN0IHF1ZXJ5ID0gJ29iamVjdC1sb2NrJ1xuXG4gICAgY29uc3QgaHR0cFJlcyA9IGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luYyh7IG1ldGhvZCwgYnVja2V0TmFtZSwgcXVlcnkgfSlcbiAgICBjb25zdCB4bWxSZXN1bHQgPSBhd2FpdCByZWFkQXNTdHJpbmcoaHR0cFJlcylcbiAgICByZXR1cm4geG1sUGFyc2Vycy5wYXJzZU9iamVjdExvY2tDb25maWcoeG1sUmVzdWx0KVxuICB9XG5cbiAgc2V0T2JqZWN0TG9ja0NvbmZpZyhidWNrZXROYW1lOiBzdHJpbmcsIGxvY2tDb25maWdPcHRzOiBPbWl0PE9iamVjdExvY2tJbmZvLCAnb2JqZWN0TG9ja0VuYWJsZWQnPik6IHZvaWRcbiAgYXN5bmMgc2V0T2JqZWN0TG9ja0NvbmZpZyhcbiAgICBidWNrZXROYW1lOiBzdHJpbmcsXG4gICAgbG9ja0NvbmZpZ09wdHM6IE9taXQ8T2JqZWN0TG9ja0luZm8sICdvYmplY3RMb2NrRW5hYmxlZCc+LFxuICApOiBQcm9taXNlPHZvaWQ+XG4gIGFzeW5jIHNldE9iamVjdExvY2tDb25maWcoYnVja2V0TmFtZTogc3RyaW5nLCBsb2NrQ29uZmlnT3B0czogT21pdDxPYmplY3RMb2NrSW5mbywgJ29iamVjdExvY2tFbmFibGVkJz4pIHtcbiAgICBjb25zdCByZXRlbnRpb25Nb2RlcyA9IFtSRVRFTlRJT05fTU9ERVMuQ09NUExJQU5DRSwgUkVURU5USU9OX01PREVTLkdPVkVSTkFOQ0VdXG4gICAgY29uc3QgdmFsaWRVbml0cyA9IFtSRVRFTlRJT05fVkFMSURJVFlfVU5JVFMuREFZUywgUkVURU5USU9OX1ZBTElESVRZX1VOSVRTLllFQVJTXVxuXG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZShidWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKCdJbnZhbGlkIGJ1Y2tldCBuYW1lOiAnICsgYnVja2V0TmFtZSlcbiAgICB9XG5cbiAgICBpZiAobG9ja0NvbmZpZ09wdHMubW9kZSAmJiAhcmV0ZW50aW9uTW9kZXMuaW5jbHVkZXMobG9ja0NvbmZpZ09wdHMubW9kZSkpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoYGxvY2tDb25maWdPcHRzLm1vZGUgc2hvdWxkIGJlIG9uZSBvZiAke3JldGVudGlvbk1vZGVzfWApXG4gICAgfVxuICAgIGlmIChsb2NrQ29uZmlnT3B0cy51bml0ICYmICF2YWxpZFVuaXRzLmluY2x1ZGVzKGxvY2tDb25maWdPcHRzLnVuaXQpKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKGBsb2NrQ29uZmlnT3B0cy51bml0IHNob3VsZCBiZSBvbmUgb2YgJHt2YWxpZFVuaXRzfWApXG4gICAgfVxuICAgIGlmIChsb2NrQ29uZmlnT3B0cy52YWxpZGl0eSAmJiAhaXNOdW1iZXIobG9ja0NvbmZpZ09wdHMudmFsaWRpdHkpKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKGBsb2NrQ29uZmlnT3B0cy52YWxpZGl0eSBzaG91bGQgYmUgYSBudW1iZXJgKVxuICAgIH1cblxuICAgIGNvbnN0IG1ldGhvZCA9ICdQVVQnXG4gICAgY29uc3QgcXVlcnkgPSAnb2JqZWN0LWxvY2snXG5cbiAgICBjb25zdCBjb25maWc6IE9iamVjdExvY2tDb25maWdQYXJhbSA9IHtcbiAgICAgIE9iamVjdExvY2tFbmFibGVkOiAnRW5hYmxlZCcsXG4gICAgfVxuICAgIGNvbnN0IGNvbmZpZ0tleXMgPSBPYmplY3Qua2V5cyhsb2NrQ29uZmlnT3B0cylcblxuICAgIGNvbnN0IGlzQWxsS2V5c1NldCA9IFsndW5pdCcsICdtb2RlJywgJ3ZhbGlkaXR5J10uZXZlcnkoKGxjaykgPT4gY29uZmlnS2V5cy5pbmNsdWRlcyhsY2spKVxuICAgIC8vIENoZWNrIGlmIGtleXMgYXJlIHByZXNlbnQgYW5kIGFsbCBrZXlzIGFyZSBwcmVzZW50LlxuICAgIGlmIChjb25maWdLZXlzLmxlbmd0aCA+IDApIHtcbiAgICAgIGlmICghaXNBbGxLZXlzU2V0KSB7XG4gICAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoXG4gICAgICAgICAgYGxvY2tDb25maWdPcHRzLm1vZGUsbG9ja0NvbmZpZ09wdHMudW5pdCxsb2NrQ29uZmlnT3B0cy52YWxpZGl0eSBhbGwgdGhlIHByb3BlcnRpZXMgc2hvdWxkIGJlIHNwZWNpZmllZC5gLFxuICAgICAgICApXG4gICAgICB9IGVsc2Uge1xuICAgICAgICBjb25maWcuUnVsZSA9IHtcbiAgICAgICAgICBEZWZhdWx0UmV0ZW50aW9uOiB7fSxcbiAgICAgICAgfVxuICAgICAgICBpZiAobG9ja0NvbmZpZ09wdHMubW9kZSkge1xuICAgICAgICAgIGNvbmZpZy5SdWxlLkRlZmF1bHRSZXRlbnRpb24uTW9kZSA9IGxvY2tDb25maWdPcHRzLm1vZGVcbiAgICAgICAgfVxuICAgICAgICBpZiAobG9ja0NvbmZpZ09wdHMudW5pdCA9PT0gUkVURU5USU9OX1ZBTElESVRZX1VOSVRTLkRBWVMpIHtcbiAgICAgICAgICBjb25maWcuUnVsZS5EZWZhdWx0UmV0ZW50aW9uLkRheXMgPSBsb2NrQ29uZmlnT3B0cy52YWxpZGl0eVxuICAgICAgICB9IGVsc2UgaWYgKGxvY2tDb25maWdPcHRzLnVuaXQgPT09IFJFVEVOVElPTl9WQUxJRElUWV9VTklUUy5ZRUFSUykge1xuICAgICAgICAgIGNvbmZpZy5SdWxlLkRlZmF1bHRSZXRlbnRpb24uWWVhcnMgPSBsb2NrQ29uZmlnT3B0cy52YWxpZGl0eVxuICAgICAgICB9XG4gICAgICB9XG4gICAgfVxuXG4gICAgY29uc3QgYnVpbGRlciA9IG5ldyB4bWwyanMuQnVpbGRlcih7XG4gICAgICByb290TmFtZTogJ09iamVjdExvY2tDb25maWd1cmF0aW9uJyxcbiAgICAgIHJlbmRlck9wdHM6IHsgcHJldHR5OiBmYWxzZSB9LFxuICAgICAgaGVhZGxlc3M6IHRydWUsXG4gICAgfSlcbiAgICBjb25zdCBwYXlsb2FkID0gYnVpbGRlci5idWlsZE9iamVjdChjb25maWcpXG5cbiAgICBjb25zdCBoZWFkZXJzOiBSZXF1ZXN0SGVhZGVycyA9IHt9XG4gICAgaGVhZGVyc1snQ29udGVudC1NRDUnXSA9IHRvTWQ1KHBheWxvYWQpXG5cbiAgICBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0QXN5bmNPbWl0KHsgbWV0aG9kLCBidWNrZXROYW1lLCBxdWVyeSwgaGVhZGVycyB9LCBwYXlsb2FkKVxuICB9XG5cbiAgYXN5bmMgZ2V0QnVja2V0VmVyc2lvbmluZyhidWNrZXROYW1lOiBzdHJpbmcpOiBQcm9taXNlPEJ1Y2tldFZlcnNpb25pbmdDb25maWd1cmF0aW9uPiB7XG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZShidWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKCdJbnZhbGlkIGJ1Y2tldCBuYW1lOiAnICsgYnVja2V0TmFtZSlcbiAgICB9XG4gICAgY29uc3QgbWV0aG9kID0gJ0dFVCdcbiAgICBjb25zdCBxdWVyeSA9ICd2ZXJzaW9uaW5nJ1xuXG4gICAgY29uc3QgaHR0cFJlcyA9IGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luYyh7IG1ldGhvZCwgYnVja2V0TmFtZSwgcXVlcnkgfSlcbiAgICBjb25zdCB4bWxSZXN1bHQgPSBhd2FpdCByZWFkQXNTdHJpbmcoaHR0cFJlcylcbiAgICByZXR1cm4gYXdhaXQgeG1sUGFyc2Vycy5wYXJzZUJ1Y2tldFZlcnNpb25pbmdDb25maWcoeG1sUmVzdWx0KVxuICB9XG5cbiAgYXN5bmMgc2V0QnVja2V0VmVyc2lvbmluZyhidWNrZXROYW1lOiBzdHJpbmcsIHZlcnNpb25Db25maWc6IEJ1Y2tldFZlcnNpb25pbmdDb25maWd1cmF0aW9uKTogUHJvbWlzZTx2b2lkPiB7XG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZShidWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKCdJbnZhbGlkIGJ1Y2tldCBuYW1lOiAnICsgYnVja2V0TmFtZSlcbiAgICB9XG4gICAgaWYgKCFPYmplY3Qua2V5cyh2ZXJzaW9uQ29uZmlnKS5sZW5ndGgpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEFyZ3VtZW50RXJyb3IoJ3ZlcnNpb25Db25maWcgc2hvdWxkIGJlIG9mIHR5cGUgXCJvYmplY3RcIicpXG4gICAgfVxuXG4gICAgY29uc3QgbWV0aG9kID0gJ1BVVCdcbiAgICBjb25zdCBxdWVyeSA9ICd2ZXJzaW9uaW5nJ1xuICAgIGNvbnN0IGJ1aWxkZXIgPSBuZXcgeG1sMmpzLkJ1aWxkZXIoe1xuICAgICAgcm9vdE5hbWU6ICdWZXJzaW9uaW5nQ29uZmlndXJhdGlvbicsXG4gICAgICByZW5kZXJPcHRzOiB7IHByZXR0eTogZmFsc2UgfSxcbiAgICAgIGhlYWRsZXNzOiB0cnVlLFxuICAgIH0pXG4gICAgY29uc3QgcGF5bG9hZCA9IGJ1aWxkZXIuYnVpbGRPYmplY3QodmVyc2lvbkNvbmZpZylcblxuICAgIGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luY09taXQoeyBtZXRob2QsIGJ1Y2tldE5hbWUsIHF1ZXJ5IH0sIHBheWxvYWQpXG4gIH1cblxuICBwcml2YXRlIGFzeW5jIHNldFRhZ2dpbmcodGFnZ2luZ1BhcmFtczogUHV0VGFnZ2luZ1BhcmFtcyk6IFByb21pc2U8dm9pZD4ge1xuICAgIGNvbnN0IHsgYnVja2V0TmFtZSwgb2JqZWN0TmFtZSwgdGFncywgcHV0T3B0cyB9ID0gdGFnZ2luZ1BhcmFtc1xuICAgIGNvbnN0IG1ldGhvZCA9ICdQVVQnXG4gICAgbGV0IHF1ZXJ5ID0gJ3RhZ2dpbmcnXG5cbiAgICBpZiAocHV0T3B0cyAmJiBwdXRPcHRzPy52ZXJzaW9uSWQpIHtcbiAgICAgIHF1ZXJ5ID0gYCR7cXVlcnl9JnZlcnNpb25JZD0ke3B1dE9wdHMudmVyc2lvbklkfWBcbiAgICB9XG4gICAgY29uc3QgdGFnc0xpc3QgPSBbXVxuICAgIGZvciAoY29uc3QgW2tleSwgdmFsdWVdIG9mIE9iamVjdC5lbnRyaWVzKHRhZ3MpKSB7XG4gICAgICB0YWdzTGlzdC5wdXNoKHsgS2V5OiBrZXksIFZhbHVlOiB2YWx1ZSB9KVxuICAgIH1cbiAgICBjb25zdCB0YWdnaW5nQ29uZmlnID0ge1xuICAgICAgVGFnZ2luZzoge1xuICAgICAgICBUYWdTZXQ6IHtcbiAgICAgICAgICBUYWc6IHRhZ3NMaXN0LFxuICAgICAgICB9LFxuICAgICAgfSxcbiAgICB9XG4gICAgY29uc3QgaGVhZGVycyA9IHt9IGFzIFJlcXVlc3RIZWFkZXJzXG4gICAgY29uc3QgYnVpbGRlciA9IG5ldyB4bWwyanMuQnVpbGRlcih7IGhlYWRsZXNzOiB0cnVlLCByZW5kZXJPcHRzOiB7IHByZXR0eTogZmFsc2UgfSB9KVxuICAgIGNvbnN0IHBheWxvYWRCdWYgPSBCdWZmZXIuZnJvbShidWlsZGVyLmJ1aWxkT2JqZWN0KHRhZ2dpbmdDb25maWcpKVxuICAgIGNvbnN0IHJlcXVlc3RPcHRpb25zID0ge1xuICAgICAgbWV0aG9kLFxuICAgICAgYnVja2V0TmFtZSxcbiAgICAgIHF1ZXJ5LFxuICAgICAgaGVhZGVycyxcblxuICAgICAgLi4uKG9iamVjdE5hbWUgJiYgeyBvYmplY3ROYW1lOiBvYmplY3ROYW1lIH0pLFxuICAgIH1cblxuICAgIGhlYWRlcnNbJ0NvbnRlbnQtTUQ1J10gPSB0b01kNShwYXlsb2FkQnVmKVxuXG4gICAgYXdhaXQgdGhpcy5tYWtlUmVxdWVzdEFzeW5jT21pdChyZXF1ZXN0T3B0aW9ucywgcGF5bG9hZEJ1ZilcbiAgfVxuXG4gIHByaXZhdGUgYXN5bmMgcmVtb3ZlVGFnZ2luZyh7IGJ1Y2tldE5hbWUsIG9iamVjdE5hbWUsIHJlbW92ZU9wdHMgfTogUmVtb3ZlVGFnZ2luZ1BhcmFtcyk6IFByb21pc2U8dm9pZD4ge1xuICAgIGNvbnN0IG1ldGhvZCA9ICdERUxFVEUnXG4gICAgbGV0IHF1ZXJ5ID0gJ3RhZ2dpbmcnXG5cbiAgICBpZiAocmVtb3ZlT3B0cyAmJiBPYmplY3Qua2V5cyhyZW1vdmVPcHRzKS5sZW5ndGggJiYgcmVtb3ZlT3B0cy52ZXJzaW9uSWQpIHtcbiAgICAgIHF1ZXJ5ID0gYCR7cXVlcnl9JnZlcnNpb25JZD0ke3JlbW92ZU9wdHMudmVyc2lvbklkfWBcbiAgICB9XG4gICAgY29uc3QgcmVxdWVzdE9wdGlvbnMgPSB7IG1ldGhvZCwgYnVja2V0TmFtZSwgb2JqZWN0TmFtZSwgcXVlcnkgfVxuXG4gICAgaWYgKG9iamVjdE5hbWUpIHtcbiAgICAgIHJlcXVlc3RPcHRpb25zWydvYmplY3ROYW1lJ10gPSBvYmplY3ROYW1lXG4gICAgfVxuICAgIGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luYyhyZXF1ZXN0T3B0aW9ucywgJycsIFsyMDAsIDIwNF0pXG4gIH1cblxuICBhc3luYyBzZXRCdWNrZXRUYWdnaW5nKGJ1Y2tldE5hbWU6IHN0cmluZywgdGFnczogVGFncyk6IFByb21pc2U8dm9pZD4ge1xuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcignSW52YWxpZCBidWNrZXQgbmFtZTogJyArIGJ1Y2tldE5hbWUpXG4gICAgfVxuICAgIGlmICghaXNQbGFpbk9iamVjdCh0YWdzKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQXJndW1lbnRFcnJvcigndGFncyBzaG91bGQgYmUgb2YgdHlwZSBcIm9iamVjdFwiJylcbiAgICB9XG4gICAgaWYgKE9iamVjdC5rZXlzKHRhZ3MpLmxlbmd0aCA+IDEwKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKCdtYXhpbXVtIHRhZ3MgYWxsb3dlZCBpcyAxMFwiJylcbiAgICB9XG5cbiAgICBhd2FpdCB0aGlzLnNldFRhZ2dpbmcoeyBidWNrZXROYW1lLCB0YWdzIH0pXG4gIH1cblxuICBhc3luYyByZW1vdmVCdWNrZXRUYWdnaW5nKGJ1Y2tldE5hbWU6IHN0cmluZykge1xuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcignSW52YWxpZCBidWNrZXQgbmFtZTogJyArIGJ1Y2tldE5hbWUpXG4gICAgfVxuICAgIGF3YWl0IHRoaXMucmVtb3ZlVGFnZ2luZyh7IGJ1Y2tldE5hbWUgfSlcbiAgfVxuXG4gIGFzeW5jIHNldE9iamVjdFRhZ2dpbmcoYnVja2V0TmFtZTogc3RyaW5nLCBvYmplY3ROYW1lOiBzdHJpbmcsIHRhZ3M6IFRhZ3MsIHB1dE9wdHM/OiBUYWdnaW5nT3B0cykge1xuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcignSW52YWxpZCBidWNrZXQgbmFtZTogJyArIGJ1Y2tldE5hbWUpXG4gICAgfVxuICAgIGlmICghaXNWYWxpZE9iamVjdE5hbWUob2JqZWN0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcignSW52YWxpZCBvYmplY3QgbmFtZTogJyArIG9iamVjdE5hbWUpXG4gICAgfVxuXG4gICAgaWYgKCFpc1BsYWluT2JqZWN0KHRhZ3MpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKCd0YWdzIHNob3VsZCBiZSBvZiB0eXBlIFwib2JqZWN0XCInKVxuICAgIH1cbiAgICBpZiAoT2JqZWN0LmtleXModGFncykubGVuZ3RoID4gMTApIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEFyZ3VtZW50RXJyb3IoJ01heGltdW0gdGFncyBhbGxvd2VkIGlzIDEwXCInKVxuICAgIH1cblxuICAgIGF3YWl0IHRoaXMuc2V0VGFnZ2luZyh7IGJ1Y2tldE5hbWUsIG9iamVjdE5hbWUsIHRhZ3MsIHB1dE9wdHMgfSlcbiAgfVxuXG4gIGFzeW5jIHJlbW92ZU9iamVjdFRhZ2dpbmcoYnVja2V0TmFtZTogc3RyaW5nLCBvYmplY3ROYW1lOiBzdHJpbmcsIHJlbW92ZU9wdHM6IFRhZ2dpbmdPcHRzKSB7XG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZShidWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKCdJbnZhbGlkIGJ1Y2tldCBuYW1lOiAnICsgYnVja2V0TmFtZSlcbiAgICB9XG4gICAgaWYgKCFpc1ZhbGlkT2JqZWN0TmFtZShvYmplY3ROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKCdJbnZhbGlkIG9iamVjdCBuYW1lOiAnICsgb2JqZWN0TmFtZSlcbiAgICB9XG4gICAgaWYgKHJlbW92ZU9wdHMgJiYgT2JqZWN0LmtleXMocmVtb3ZlT3B0cykubGVuZ3RoICYmICFpc09iamVjdChyZW1vdmVPcHRzKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQXJndW1lbnRFcnJvcigncmVtb3ZlT3B0cyBzaG91bGQgYmUgb2YgdHlwZSBcIm9iamVjdFwiJylcbiAgICB9XG5cbiAgICBhd2FpdCB0aGlzLnJlbW92ZVRhZ2dpbmcoeyBidWNrZXROYW1lLCBvYmplY3ROYW1lLCByZW1vdmVPcHRzIH0pXG4gIH1cblxuICBhc3luYyBzZWxlY3RPYmplY3RDb250ZW50KFxuICAgIGJ1Y2tldE5hbWU6IHN0cmluZyxcbiAgICBvYmplY3ROYW1lOiBzdHJpbmcsXG4gICAgc2VsZWN0T3B0czogU2VsZWN0T3B0aW9ucyxcbiAgKTogUHJvbWlzZTxTZWxlY3RSZXN1bHRzIHwgdW5kZWZpbmVkPiB7XG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZShidWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKGBJbnZhbGlkIGJ1Y2tldCBuYW1lOiAke2J1Y2tldE5hbWV9YClcbiAgICB9XG4gICAgaWYgKCFpc1ZhbGlkT2JqZWN0TmFtZShvYmplY3ROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkT2JqZWN0TmFtZUVycm9yKGBJbnZhbGlkIG9iamVjdCBuYW1lOiAke29iamVjdE5hbWV9YClcbiAgICB9XG4gICAgaWYgKCFfLmlzRW1wdHkoc2VsZWN0T3B0cykpIHtcbiAgICAgIGlmICghaXNTdHJpbmcoc2VsZWN0T3B0cy5leHByZXNzaW9uKSkge1xuICAgICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdzcWxFeHByZXNzaW9uIHNob3VsZCBiZSBvZiB0eXBlIFwic3RyaW5nXCInKVxuICAgICAgfVxuICAgICAgaWYgKCFfLmlzRW1wdHkoc2VsZWN0T3B0cy5pbnB1dFNlcmlhbGl6YXRpb24pKSB7XG4gICAgICAgIGlmICghaXNPYmplY3Qoc2VsZWN0T3B0cy5pbnB1dFNlcmlhbGl6YXRpb24pKSB7XG4gICAgICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcignaW5wdXRTZXJpYWxpemF0aW9uIHNob3VsZCBiZSBvZiB0eXBlIFwib2JqZWN0XCInKVxuICAgICAgICB9XG4gICAgICB9IGVsc2Uge1xuICAgICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdpbnB1dFNlcmlhbGl6YXRpb24gaXMgcmVxdWlyZWQnKVxuICAgICAgfVxuICAgICAgaWYgKCFfLmlzRW1wdHkoc2VsZWN0T3B0cy5vdXRwdXRTZXJpYWxpemF0aW9uKSkge1xuICAgICAgICBpZiAoIWlzT2JqZWN0KHNlbGVjdE9wdHMub3V0cHV0U2VyaWFsaXphdGlvbikpIHtcbiAgICAgICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdvdXRwdXRTZXJpYWxpemF0aW9uIHNob3VsZCBiZSBvZiB0eXBlIFwib2JqZWN0XCInKVxuICAgICAgICB9XG4gICAgICB9IGVsc2Uge1xuICAgICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdvdXRwdXRTZXJpYWxpemF0aW9uIGlzIHJlcXVpcmVkJylcbiAgICAgIH1cbiAgICB9IGVsc2Uge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcigndmFsaWQgc2VsZWN0IGNvbmZpZ3VyYXRpb24gaXMgcmVxdWlyZWQnKVxuICAgIH1cblxuICAgIGNvbnN0IG1ldGhvZCA9ICdQT1NUJ1xuICAgIGNvbnN0IHF1ZXJ5ID0gYHNlbGVjdCZzZWxlY3QtdHlwZT0yYFxuXG4gICAgY29uc3QgY29uZmlnOiBSZWNvcmQ8c3RyaW5nLCB1bmtub3duPltdID0gW1xuICAgICAge1xuICAgICAgICBFeHByZXNzaW9uOiBzZWxlY3RPcHRzLmV4cHJlc3Npb24sXG4gICAgICB9LFxuICAgICAge1xuICAgICAgICBFeHByZXNzaW9uVHlwZTogc2VsZWN0T3B0cy5leHByZXNzaW9uVHlwZSB8fCAnU1FMJyxcbiAgICAgIH0sXG4gICAgICB7XG4gICAgICAgIElucHV0U2VyaWFsaXphdGlvbjogW3NlbGVjdE9wdHMuaW5wdXRTZXJpYWxpemF0aW9uXSxcbiAgICAgIH0sXG4gICAgICB7XG4gICAgICAgIE91dHB1dFNlcmlhbGl6YXRpb246IFtzZWxlY3RPcHRzLm91dHB1dFNlcmlhbGl6YXRpb25dLFxuICAgICAgfSxcbiAgICBdXG5cbiAgICAvLyBPcHRpb25hbFxuICAgIGlmIChzZWxlY3RPcHRzLnJlcXVlc3RQcm9ncmVzcykge1xuICAgICAgY29uZmlnLnB1c2goeyBSZXF1ZXN0UHJvZ3Jlc3M6IHNlbGVjdE9wdHM/LnJlcXVlc3RQcm9ncmVzcyB9KVxuICAgIH1cbiAgICAvLyBPcHRpb25hbFxuICAgIGlmIChzZWxlY3RPcHRzLnNjYW5SYW5nZSkge1xuICAgICAgY29uZmlnLnB1c2goeyBTY2FuUmFuZ2U6IHNlbGVjdE9wdHMuc2NhblJhbmdlIH0pXG4gICAgfVxuXG4gICAgY29uc3QgYnVpbGRlciA9IG5ldyB4bWwyanMuQnVpbGRlcih7XG4gICAgICByb290TmFtZTogJ1NlbGVjdE9iamVjdENvbnRlbnRSZXF1ZXN0JyxcbiAgICAgIHJlbmRlck9wdHM6IHsgcHJldHR5OiBmYWxzZSB9LFxuICAgICAgaGVhZGxlc3M6IHRydWUsXG4gICAgfSlcbiAgICBjb25zdCBwYXlsb2FkID0gYnVpbGRlci5idWlsZE9iamVjdChjb25maWcpXG5cbiAgICBjb25zdCByZXMgPSBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0QXN5bmMoeyBtZXRob2QsIGJ1Y2tldE5hbWUsIG9iamVjdE5hbWUsIHF1ZXJ5IH0sIHBheWxvYWQpXG4gICAgY29uc3QgYm9keSA9IGF3YWl0IHJlYWRBc0J1ZmZlcihyZXMpXG4gICAgcmV0dXJuIHBhcnNlU2VsZWN0T2JqZWN0Q29udGVudFJlc3BvbnNlKGJvZHkpXG4gIH1cblxuICBwcml2YXRlIGFzeW5jIGFwcGx5QnVja2V0TGlmZWN5Y2xlKGJ1Y2tldE5hbWU6IHN0cmluZywgcG9saWN5Q29uZmlnOiBMaWZlQ3ljbGVDb25maWdQYXJhbSk6IFByb21pc2U8dm9pZD4ge1xuICAgIGNvbnN0IG1ldGhvZCA9ICdQVVQnXG4gICAgY29uc3QgcXVlcnkgPSAnbGlmZWN5Y2xlJ1xuXG4gICAgY29uc3QgaGVhZGVyczogUmVxdWVzdEhlYWRlcnMgPSB7fVxuICAgIGNvbnN0IGJ1aWxkZXIgPSBuZXcgeG1sMmpzLkJ1aWxkZXIoe1xuICAgICAgcm9vdE5hbWU6ICdMaWZlY3ljbGVDb25maWd1cmF0aW9uJyxcbiAgICAgIGhlYWRsZXNzOiB0cnVlLFxuICAgICAgcmVuZGVyT3B0czogeyBwcmV0dHk6IGZhbHNlIH0sXG4gICAgfSlcbiAgICBjb25zdCBwYXlsb2FkID0gYnVpbGRlci5idWlsZE9iamVjdChwb2xpY3lDb25maWcpXG4gICAgaGVhZGVyc1snQ29udGVudC1NRDUnXSA9IHRvTWQ1KHBheWxvYWQpXG5cbiAgICBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0QXN5bmNPbWl0KHsgbWV0aG9kLCBidWNrZXROYW1lLCBxdWVyeSwgaGVhZGVycyB9LCBwYXlsb2FkKVxuICB9XG5cbiAgYXN5bmMgcmVtb3ZlQnVja2V0TGlmZWN5Y2xlKGJ1Y2tldE5hbWU6IHN0cmluZyk6IFByb21pc2U8dm9pZD4ge1xuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcignSW52YWxpZCBidWNrZXQgbmFtZTogJyArIGJ1Y2tldE5hbWUpXG4gICAgfVxuICAgIGNvbnN0IG1ldGhvZCA9ICdERUxFVEUnXG4gICAgY29uc3QgcXVlcnkgPSAnbGlmZWN5Y2xlJ1xuICAgIGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luY09taXQoeyBtZXRob2QsIGJ1Y2tldE5hbWUsIHF1ZXJ5IH0sICcnLCBbMjA0XSlcbiAgfVxuXG4gIGFzeW5jIHNldEJ1Y2tldExpZmVjeWNsZShidWNrZXROYW1lOiBzdHJpbmcsIGxpZmVDeWNsZUNvbmZpZzogTGlmZUN5Y2xlQ29uZmlnUGFyYW0pOiBQcm9taXNlPHZvaWQ+IHtcbiAgICBpZiAoIWlzVmFsaWRCdWNrZXROYW1lKGJ1Y2tldE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IoJ0ludmFsaWQgYnVja2V0IG5hbWU6ICcgKyBidWNrZXROYW1lKVxuICAgIH1cbiAgICBpZiAoXy5pc0VtcHR5KGxpZmVDeWNsZUNvbmZpZykpIHtcbiAgICAgIGF3YWl0IHRoaXMucmVtb3ZlQnVja2V0TGlmZWN5Y2xlKGJ1Y2tldE5hbWUpXG4gICAgfSBlbHNlIHtcbiAgICAgIGF3YWl0IHRoaXMuYXBwbHlCdWNrZXRMaWZlY3ljbGUoYnVja2V0TmFtZSwgbGlmZUN5Y2xlQ29uZmlnKVxuICAgIH1cbiAgfVxuXG4gIGFzeW5jIGdldEJ1Y2tldExpZmVjeWNsZShidWNrZXROYW1lOiBzdHJpbmcpOiBQcm9taXNlPExpZmVjeWNsZUNvbmZpZyB8IG51bGw+IHtcbiAgICBpZiAoIWlzVmFsaWRCdWNrZXROYW1lKGJ1Y2tldE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IoJ0ludmFsaWQgYnVja2V0IG5hbWU6ICcgKyBidWNrZXROYW1lKVxuICAgIH1cbiAgICBjb25zdCBtZXRob2QgPSAnR0VUJ1xuICAgIGNvbnN0IHF1ZXJ5ID0gJ2xpZmVjeWNsZSdcblxuICAgIGNvbnN0IHJlcyA9IGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luYyh7IG1ldGhvZCwgYnVja2V0TmFtZSwgcXVlcnkgfSlcbiAgICBjb25zdCBib2R5ID0gYXdhaXQgcmVhZEFzU3RyaW5nKHJlcylcbiAgICByZXR1cm4geG1sUGFyc2Vycy5wYXJzZUxpZmVjeWNsZUNvbmZpZyhib2R5KVxuICB9XG5cbiAgYXN5bmMgc2V0QnVja2V0RW5jcnlwdGlvbihidWNrZXROYW1lOiBzdHJpbmcsIGVuY3J5cHRpb25Db25maWc/OiBFbmNyeXB0aW9uQ29uZmlnKTogUHJvbWlzZTx2b2lkPiB7XG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZShidWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKCdJbnZhbGlkIGJ1Y2tldCBuYW1lOiAnICsgYnVja2V0TmFtZSlcbiAgICB9XG4gICAgaWYgKCFfLmlzRW1wdHkoZW5jcnlwdGlvbkNvbmZpZykgJiYgZW5jcnlwdGlvbkNvbmZpZy5SdWxlLmxlbmd0aCA+IDEpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEFyZ3VtZW50RXJyb3IoJ0ludmFsaWQgUnVsZSBsZW5ndGguIE9ubHkgb25lIHJ1bGUgaXMgYWxsb3dlZC46ICcgKyBlbmNyeXB0aW9uQ29uZmlnLlJ1bGUpXG4gICAgfVxuXG4gICAgbGV0IGVuY3J5cHRpb25PYmogPSBlbmNyeXB0aW9uQ29uZmlnXG4gICAgaWYgKF8uaXNFbXB0eShlbmNyeXB0aW9uQ29uZmlnKSkge1xuICAgICAgZW5jcnlwdGlvbk9iaiA9IHtcbiAgICAgICAgLy8gRGVmYXVsdCBNaW5JTyBTZXJ2ZXIgU3VwcG9ydGVkIFJ1bGVcbiAgICAgICAgUnVsZTogW1xuICAgICAgICAgIHtcbiAgICAgICAgICAgIEFwcGx5U2VydmVyU2lkZUVuY3J5cHRpb25CeURlZmF1bHQ6IHtcbiAgICAgICAgICAgICAgU1NFQWxnb3JpdGhtOiAnQUVTMjU2JyxcbiAgICAgICAgICAgIH0sXG4gICAgICAgICAgfSxcbiAgICAgICAgXSxcbiAgICAgIH1cbiAgICB9XG5cbiAgICBjb25zdCBtZXRob2QgPSAnUFVUJ1xuICAgIGNvbnN0IHF1ZXJ5ID0gJ2VuY3J5cHRpb24nXG4gICAgY29uc3QgYnVpbGRlciA9IG5ldyB4bWwyanMuQnVpbGRlcih7XG4gICAgICByb290TmFtZTogJ1NlcnZlclNpZGVFbmNyeXB0aW9uQ29uZmlndXJhdGlvbicsXG4gICAgICByZW5kZXJPcHRzOiB7IHByZXR0eTogZmFsc2UgfSxcbiAgICAgIGhlYWRsZXNzOiB0cnVlLFxuICAgIH0pXG4gICAgY29uc3QgcGF5bG9hZCA9IGJ1aWxkZXIuYnVpbGRPYmplY3QoZW5jcnlwdGlvbk9iailcblxuICAgIGNvbnN0IGhlYWRlcnM6IFJlcXVlc3RIZWFkZXJzID0ge31cbiAgICBoZWFkZXJzWydDb250ZW50LU1ENSddID0gdG9NZDUocGF5bG9hZClcblxuICAgIGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luY09taXQoeyBtZXRob2QsIGJ1Y2tldE5hbWUsIHF1ZXJ5LCBoZWFkZXJzIH0sIHBheWxvYWQpXG4gIH1cblxuICBhc3luYyBnZXRCdWNrZXRFbmNyeXB0aW9uKGJ1Y2tldE5hbWU6IHN0cmluZykge1xuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcignSW52YWxpZCBidWNrZXQgbmFtZTogJyArIGJ1Y2tldE5hbWUpXG4gICAgfVxuICAgIGNvbnN0IG1ldGhvZCA9ICdHRVQnXG4gICAgY29uc3QgcXVlcnkgPSAnZW5jcnlwdGlvbidcblxuICAgIGNvbnN0IHJlcyA9IGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luYyh7IG1ldGhvZCwgYnVja2V0TmFtZSwgcXVlcnkgfSlcbiAgICBjb25zdCBib2R5ID0gYXdhaXQgcmVhZEFzU3RyaW5nKHJlcylcbiAgICByZXR1cm4geG1sUGFyc2Vycy5wYXJzZUJ1Y2tldEVuY3J5cHRpb25Db25maWcoYm9keSlcbiAgfVxuXG4gIGFzeW5jIHJlbW92ZUJ1Y2tldEVuY3J5cHRpb24oYnVja2V0TmFtZTogc3RyaW5nKSB7XG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZShidWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKCdJbnZhbGlkIGJ1Y2tldCBuYW1lOiAnICsgYnVja2V0TmFtZSlcbiAgICB9XG4gICAgY29uc3QgbWV0aG9kID0gJ0RFTEVURSdcbiAgICBjb25zdCBxdWVyeSA9ICdlbmNyeXB0aW9uJ1xuXG4gICAgYXdhaXQgdGhpcy5tYWtlUmVxdWVzdEFzeW5jT21pdCh7IG1ldGhvZCwgYnVja2V0TmFtZSwgcXVlcnkgfSwgJycsIFsyMDRdKVxuICB9XG5cbiAgYXN5bmMgZ2V0T2JqZWN0UmV0ZW50aW9uKFxuICAgIGJ1Y2tldE5hbWU6IHN0cmluZyxcbiAgICBvYmplY3ROYW1lOiBzdHJpbmcsXG4gICAgZ2V0T3B0cz86IEdldE9iamVjdFJldGVudGlvbk9wdHMsXG4gICk6IFByb21pc2U8T2JqZWN0UmV0ZW50aW9uSW5mbyB8IG51bGwgfCB1bmRlZmluZWQ+IHtcbiAgICBpZiAoIWlzVmFsaWRCdWNrZXROYW1lKGJ1Y2tldE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IoJ0ludmFsaWQgYnVja2V0IG5hbWU6ICcgKyBidWNrZXROYW1lKVxuICAgIH1cbiAgICBpZiAoIWlzVmFsaWRPYmplY3ROYW1lKG9iamVjdE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRPYmplY3ROYW1lRXJyb3IoYEludmFsaWQgb2JqZWN0IG5hbWU6ICR7b2JqZWN0TmFtZX1gKVxuICAgIH1cbiAgICBpZiAoZ2V0T3B0cyAmJiAhaXNPYmplY3QoZ2V0T3B0cykpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEFyZ3VtZW50RXJyb3IoJ2dldE9wdHMgc2hvdWxkIGJlIG9mIHR5cGUgXCJvYmplY3RcIicpXG4gICAgfSBlbHNlIGlmIChnZXRPcHRzPy52ZXJzaW9uSWQgJiYgIWlzU3RyaW5nKGdldE9wdHMudmVyc2lvbklkKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQXJndW1lbnRFcnJvcigndmVyc2lvbklkIHNob3VsZCBiZSBvZiB0eXBlIFwic3RyaW5nXCInKVxuICAgIH1cblxuICAgIGNvbnN0IG1ldGhvZCA9ICdHRVQnXG4gICAgbGV0IHF1ZXJ5ID0gJ3JldGVudGlvbidcbiAgICBpZiAoZ2V0T3B0cz8udmVyc2lvbklkKSB7XG4gICAgICBxdWVyeSArPSBgJnZlcnNpb25JZD0ke2dldE9wdHMudmVyc2lvbklkfWBcbiAgICB9XG4gICAgY29uc3QgcmVzID0gYXdhaXQgdGhpcy5tYWtlUmVxdWVzdEFzeW5jKHsgbWV0aG9kLCBidWNrZXROYW1lLCBvYmplY3ROYW1lLCBxdWVyeSB9KVxuICAgIGNvbnN0IGJvZHkgPSBhd2FpdCByZWFkQXNTdHJpbmcocmVzKVxuICAgIHJldHVybiB4bWxQYXJzZXJzLnBhcnNlT2JqZWN0UmV0ZW50aW9uQ29uZmlnKGJvZHkpXG4gIH1cblxuICBhc3luYyByZW1vdmVPYmplY3RzKGJ1Y2tldE5hbWU6IHN0cmluZywgb2JqZWN0c0xpc3Q6IFJlbW92ZU9iamVjdHNQYXJhbSk6IFByb21pc2U8UmVtb3ZlT2JqZWN0c1Jlc3BvbnNlW10+IHtcbiAgICBpZiAoIWlzVmFsaWRCdWNrZXROYW1lKGJ1Y2tldE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IoJ0ludmFsaWQgYnVja2V0IG5hbWU6ICcgKyBidWNrZXROYW1lKVxuICAgIH1cbiAgICBpZiAoIUFycmF5LmlzQXJyYXkob2JqZWN0c0xpc3QpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKCdvYmplY3RzTGlzdCBzaG91bGQgYmUgYSBsaXN0JylcbiAgICB9XG5cbiAgICBjb25zdCBydW5EZWxldGVPYmplY3RzID0gYXN5bmMgKGJhdGNoOiBSZW1vdmVPYmplY3RzUGFyYW0pOiBQcm9taXNlPFJlbW92ZU9iamVjdHNSZXNwb25zZVtdPiA9PiB7XG4gICAgICBjb25zdCBkZWxPYmplY3RzOiBSZW1vdmVPYmplY3RzUmVxdWVzdEVudHJ5W10gPSBiYXRjaC5tYXAoKHZhbHVlKSA9PiB7XG4gICAgICAgIHJldHVybiBpc09iamVjdCh2YWx1ZSkgPyB7IEtleTogdmFsdWUubmFtZSwgVmVyc2lvbklkOiB2YWx1ZS52ZXJzaW9uSWQgfSA6IHsgS2V5OiB2YWx1ZSB9XG4gICAgICB9KVxuXG4gICAgICBjb25zdCByZW1PYmplY3RzID0geyBEZWxldGU6IHsgUXVpZXQ6IHRydWUsIE9iamVjdDogZGVsT2JqZWN0cyB9IH1cbiAgICAgIGNvbnN0IHBheWxvYWQgPSBCdWZmZXIuZnJvbShuZXcgeG1sMmpzLkJ1aWxkZXIoeyBoZWFkbGVzczogdHJ1ZSB9KS5idWlsZE9iamVjdChyZW1PYmplY3RzKSlcbiAgICAgIGNvbnN0IGhlYWRlcnM6IFJlcXVlc3RIZWFkZXJzID0geyAnQ29udGVudC1NRDUnOiB0b01kNShwYXlsb2FkKSB9XG5cbiAgICAgIGNvbnN0IHJlcyA9IGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luYyh7IG1ldGhvZDogJ1BPU1QnLCBidWNrZXROYW1lLCBxdWVyeTogJ2RlbGV0ZScsIGhlYWRlcnMgfSwgcGF5bG9hZClcbiAgICAgIGNvbnN0IGJvZHkgPSBhd2FpdCByZWFkQXNTdHJpbmcocmVzKVxuICAgICAgcmV0dXJuIHhtbFBhcnNlcnMucmVtb3ZlT2JqZWN0c1BhcnNlcihib2R5KVxuICAgIH1cblxuICAgIGNvbnN0IG1heEVudHJpZXMgPSAxMDAwIC8vIG1heCBlbnRyaWVzIGFjY2VwdGVkIGluIHNlcnZlciBmb3IgRGVsZXRlTXVsdGlwbGVPYmplY3RzIEFQSS5cbiAgICAvLyBDbGllbnQgc2lkZSBiYXRjaGluZ1xuICAgIGNvbnN0IGJhdGNoZXMgPSBbXVxuICAgIGZvciAobGV0IGkgPSAwOyBpIDwgb2JqZWN0c0xpc3QubGVuZ3RoOyBpICs9IG1heEVudHJpZXMpIHtcbiAgICAgIGJhdGNoZXMucHVzaChvYmplY3RzTGlzdC5zbGljZShpLCBpICsgbWF4RW50cmllcykpXG4gICAgfVxuXG4gICAgY29uc3QgYmF0Y2hSZXN1bHRzID0gYXdhaXQgUHJvbWlzZS5hbGwoYmF0Y2hlcy5tYXAocnVuRGVsZXRlT2JqZWN0cykpXG4gICAgcmV0dXJuIGJhdGNoUmVzdWx0cy5mbGF0KClcbiAgfVxuXG4gIGFzeW5jIHJlbW92ZUluY29tcGxldGVVcGxvYWQoYnVja2V0TmFtZTogc3RyaW5nLCBvYmplY3ROYW1lOiBzdHJpbmcpOiBQcm9taXNlPHZvaWQ+IHtcbiAgICBpZiAoIWlzVmFsaWRCdWNrZXROYW1lKGJ1Y2tldE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLklzVmFsaWRCdWNrZXROYW1lRXJyb3IoJ0ludmFsaWQgYnVja2V0IG5hbWU6ICcgKyBidWNrZXROYW1lKVxuICAgIH1cbiAgICBpZiAoIWlzVmFsaWRPYmplY3ROYW1lKG9iamVjdE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRPYmplY3ROYW1lRXJyb3IoYEludmFsaWQgb2JqZWN0IG5hbWU6ICR7b2JqZWN0TmFtZX1gKVxuICAgIH1cbiAgICBjb25zdCByZW1vdmVVcGxvYWRJZCA9IGF3YWl0IHRoaXMuZmluZFVwbG9hZElkKGJ1Y2tldE5hbWUsIG9iamVjdE5hbWUpXG4gICAgY29uc3QgbWV0aG9kID0gJ0RFTEVURSdcbiAgICBjb25zdCBxdWVyeSA9IGB1cGxvYWRJZD0ke3JlbW92ZVVwbG9hZElkfWBcbiAgICBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0QXN5bmNPbWl0KHsgbWV0aG9kLCBidWNrZXROYW1lLCBvYmplY3ROYW1lLCBxdWVyeSB9LCAnJywgWzIwNF0pXG4gIH1cblxuICBwcml2YXRlIGFzeW5jIGNvcHlPYmplY3RWMShcbiAgICB0YXJnZXRCdWNrZXROYW1lOiBzdHJpbmcsXG4gICAgdGFyZ2V0T2JqZWN0TmFtZTogc3RyaW5nLFxuICAgIHNvdXJjZUJ1Y2tldE5hbWVBbmRPYmplY3ROYW1lOiBzdHJpbmcsXG4gICAgY29uZGl0aW9ucz86IG51bGwgfCBDb3B5Q29uZGl0aW9ucyxcbiAgKSB7XG4gICAgaWYgKHR5cGVvZiBjb25kaXRpb25zID09ICdmdW5jdGlvbicpIHtcbiAgICAgIGNvbmRpdGlvbnMgPSBudWxsXG4gICAgfVxuXG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZSh0YXJnZXRCdWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKCdJbnZhbGlkIGJ1Y2tldCBuYW1lOiAnICsgdGFyZ2V0QnVja2V0TmFtZSlcbiAgICB9XG4gICAgaWYgKCFpc1ZhbGlkT2JqZWN0TmFtZSh0YXJnZXRPYmplY3ROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkT2JqZWN0TmFtZUVycm9yKGBJbnZhbGlkIG9iamVjdCBuYW1lOiAke3RhcmdldE9iamVjdE5hbWV9YClcbiAgICB9XG4gICAgaWYgKCFpc1N0cmluZyhzb3VyY2VCdWNrZXROYW1lQW5kT2JqZWN0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ3NvdXJjZUJ1Y2tldE5hbWVBbmRPYmplY3ROYW1lIHNob3VsZCBiZSBvZiB0eXBlIFwic3RyaW5nXCInKVxuICAgIH1cbiAgICBpZiAoc291cmNlQnVja2V0TmFtZUFuZE9iamVjdE5hbWUgPT09ICcnKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRQcmVmaXhFcnJvcihgRW1wdHkgc291cmNlIHByZWZpeGApXG4gICAgfVxuXG4gICAgaWYgKGNvbmRpdGlvbnMgIT0gbnVsbCAmJiAhKGNvbmRpdGlvbnMgaW5zdGFuY2VvZiBDb3B5Q29uZGl0aW9ucykpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ2NvbmRpdGlvbnMgc2hvdWxkIGJlIG9mIHR5cGUgXCJDb3B5Q29uZGl0aW9uc1wiJylcbiAgICB9XG5cbiAgICBjb25zdCBoZWFkZXJzOiBSZXF1ZXN0SGVhZGVycyA9IHt9XG4gICAgaGVhZGVyc1sneC1hbXotY29weS1zb3VyY2UnXSA9IHVyaVJlc291cmNlRXNjYXBlKHNvdXJjZUJ1Y2tldE5hbWVBbmRPYmplY3ROYW1lKVxuXG4gICAgaWYgKGNvbmRpdGlvbnMpIHtcbiAgICAgIGlmIChjb25kaXRpb25zLm1vZGlmaWVkICE9PSAnJykge1xuICAgICAgICBoZWFkZXJzWyd4LWFtei1jb3B5LXNvdXJjZS1pZi1tb2RpZmllZC1zaW5jZSddID0gY29uZGl0aW9ucy5tb2RpZmllZFxuICAgICAgfVxuICAgICAgaWYgKGNvbmRpdGlvbnMudW5tb2RpZmllZCAhPT0gJycpIHtcbiAgICAgICAgaGVhZGVyc1sneC1hbXotY29weS1zb3VyY2UtaWYtdW5tb2RpZmllZC1zaW5jZSddID0gY29uZGl0aW9ucy51bm1vZGlmaWVkXG4gICAgICB9XG4gICAgICBpZiAoY29uZGl0aW9ucy5tYXRjaEVUYWcgIT09ICcnKSB7XG4gICAgICAgIGhlYWRlcnNbJ3gtYW16LWNvcHktc291cmNlLWlmLW1hdGNoJ10gPSBjb25kaXRpb25zLm1hdGNoRVRhZ1xuICAgICAgfVxuICAgICAgaWYgKGNvbmRpdGlvbnMubWF0Y2hFVGFnRXhjZXB0ICE9PSAnJykge1xuICAgICAgICBoZWFkZXJzWyd4LWFtei1jb3B5LXNvdXJjZS1pZi1ub25lLW1hdGNoJ10gPSBjb25kaXRpb25zLm1hdGNoRVRhZ0V4Y2VwdFxuICAgICAgfVxuICAgIH1cblxuICAgIGNvbnN0IG1ldGhvZCA9ICdQVVQnXG5cbiAgICBjb25zdCByZXMgPSBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0QXN5bmMoe1xuICAgICAgbWV0aG9kLFxuICAgICAgYnVja2V0TmFtZTogdGFyZ2V0QnVja2V0TmFtZSxcbiAgICAgIG9iamVjdE5hbWU6IHRhcmdldE9iamVjdE5hbWUsXG4gICAgICBoZWFkZXJzLFxuICAgIH0pXG4gICAgY29uc3QgYm9keSA9IGF3YWl0IHJlYWRBc1N0cmluZyhyZXMpXG4gICAgcmV0dXJuIHhtbFBhcnNlcnMucGFyc2VDb3B5T2JqZWN0KGJvZHkpXG4gIH1cblxuICBwcml2YXRlIGFzeW5jIGNvcHlPYmplY3RWMihcbiAgICBzb3VyY2VDb25maWc6IENvcHlTb3VyY2VPcHRpb25zLFxuICAgIGRlc3RDb25maWc6IENvcHlEZXN0aW5hdGlvbk9wdGlvbnMsXG4gICk6IFByb21pc2U8Q29weU9iamVjdFJlc3VsdFYyPiB7XG4gICAgaWYgKCEoc291cmNlQ29uZmlnIGluc3RhbmNlb2YgQ29weVNvdXJjZU9wdGlvbnMpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKCdzb3VyY2VDb25maWcgc2hvdWxkIG9mIHR5cGUgQ29weVNvdXJjZU9wdGlvbnMgJylcbiAgICB9XG4gICAgaWYgKCEoZGVzdENvbmZpZyBpbnN0YW5jZW9mIENvcHlEZXN0aW5hdGlvbk9wdGlvbnMpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKCdkZXN0Q29uZmlnIHNob3VsZCBvZiB0eXBlIENvcHlEZXN0aW5hdGlvbk9wdGlvbnMgJylcbiAgICB9XG4gICAgaWYgKCFkZXN0Q29uZmlnLnZhbGlkYXRlKCkpIHtcbiAgICAgIHJldHVybiBQcm9taXNlLnJlamVjdCgpXG4gICAgfVxuICAgIGlmICghZGVzdENvbmZpZy52YWxpZGF0ZSgpKSB7XG4gICAgICByZXR1cm4gUHJvbWlzZS5yZWplY3QoKVxuICAgIH1cblxuICAgIGNvbnN0IGhlYWRlcnMgPSBPYmplY3QuYXNzaWduKHt9LCBzb3VyY2VDb25maWcuZ2V0SGVhZGVycygpLCBkZXN0Q29uZmlnLmdldEhlYWRlcnMoKSlcblxuICAgIGNvbnN0IGJ1Y2tldE5hbWUgPSBkZXN0Q29uZmlnLkJ1Y2tldFxuICAgIGNvbnN0IG9iamVjdE5hbWUgPSBkZXN0Q29uZmlnLk9iamVjdFxuXG4gICAgY29uc3QgbWV0aG9kID0gJ1BVVCdcblxuICAgIGNvbnN0IHJlcyA9IGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luYyh7IG1ldGhvZCwgYnVja2V0TmFtZSwgb2JqZWN0TmFtZSwgaGVhZGVycyB9KVxuICAgIGNvbnN0IGJvZHkgPSBhd2FpdCByZWFkQXNTdHJpbmcocmVzKVxuICAgIGNvbnN0IGNvcHlSZXMgPSB4bWxQYXJzZXJzLnBhcnNlQ29weU9iamVjdChib2R5KVxuICAgIGNvbnN0IHJlc0hlYWRlcnM6IEluY29taW5nSHR0cEhlYWRlcnMgPSByZXMuaGVhZGVyc1xuXG4gICAgY29uc3Qgc2l6ZUhlYWRlclZhbHVlID0gcmVzSGVhZGVycyAmJiByZXNIZWFkZXJzWydjb250ZW50LWxlbmd0aCddXG4gICAgY29uc3Qgc2l6ZSA9IHR5cGVvZiBzaXplSGVhZGVyVmFsdWUgPT09ICdudW1iZXInID8gc2l6ZUhlYWRlclZhbHVlIDogdW5kZWZpbmVkXG5cbiAgICByZXR1cm4ge1xuICAgICAgQnVja2V0OiBkZXN0Q29uZmlnLkJ1Y2tldCxcbiAgICAgIEtleTogZGVzdENvbmZpZy5PYmplY3QsXG4gICAgICBMYXN0TW9kaWZpZWQ6IGNvcHlSZXMubGFzdE1vZGlmaWVkLFxuICAgICAgTWV0YURhdGE6IGV4dHJhY3RNZXRhZGF0YShyZXNIZWFkZXJzIGFzIFJlc3BvbnNlSGVhZGVyKSxcbiAgICAgIFZlcnNpb25JZDogZ2V0VmVyc2lvbklkKHJlc0hlYWRlcnMgYXMgUmVzcG9uc2VIZWFkZXIpLFxuICAgICAgU291cmNlVmVyc2lvbklkOiBnZXRTb3VyY2VWZXJzaW9uSWQocmVzSGVhZGVycyBhcyBSZXNwb25zZUhlYWRlciksXG4gICAgICBFdGFnOiBzYW5pdGl6ZUVUYWcocmVzSGVhZGVycy5ldGFnKSxcbiAgICAgIFNpemU6IHNpemUsXG4gICAgfVxuICB9XG5cbiAgYXN5bmMgY29weU9iamVjdChzb3VyY2U6IENvcHlTb3VyY2VPcHRpb25zLCBkZXN0OiBDb3B5RGVzdGluYXRpb25PcHRpb25zKTogUHJvbWlzZTxDb3B5T2JqZWN0UmVzdWx0PlxuICBhc3luYyBjb3B5T2JqZWN0KFxuICAgIHRhcmdldEJ1Y2tldE5hbWU6IHN0cmluZyxcbiAgICB0YXJnZXRPYmplY3ROYW1lOiBzdHJpbmcsXG4gICAgc291cmNlQnVja2V0TmFtZUFuZE9iamVjdE5hbWU6IHN0cmluZyxcbiAgICBjb25kaXRpb25zPzogQ29weUNvbmRpdGlvbnMsXG4gICk6IFByb21pc2U8Q29weU9iamVjdFJlc3VsdD5cbiAgYXN5bmMgY29weU9iamVjdCguLi5hbGxBcmdzOiBDb3B5T2JqZWN0UGFyYW1zKTogUHJvbWlzZTxDb3B5T2JqZWN0UmVzdWx0PiB7XG4gICAgaWYgKHR5cGVvZiBhbGxBcmdzWzBdID09PSAnc3RyaW5nJykge1xuICAgICAgY29uc3QgW3RhcmdldEJ1Y2tldE5hbWUsIHRhcmdldE9iamVjdE5hbWUsIHNvdXJjZUJ1Y2tldE5hbWVBbmRPYmplY3ROYW1lLCBjb25kaXRpb25zXSA9IGFsbEFyZ3MgYXMgW1xuICAgICAgICBzdHJpbmcsXG4gICAgICAgIHN0cmluZyxcbiAgICAgICAgc3RyaW5nLFxuICAgICAgICBDb3B5Q29uZGl0aW9ucz8sXG4gICAgICBdXG4gICAgICByZXR1cm4gYXdhaXQgdGhpcy5jb3B5T2JqZWN0VjEodGFyZ2V0QnVja2V0TmFtZSwgdGFyZ2V0T2JqZWN0TmFtZSwgc291cmNlQnVja2V0TmFtZUFuZE9iamVjdE5hbWUsIGNvbmRpdGlvbnMpXG4gICAgfVxuICAgIGNvbnN0IFtzb3VyY2UsIGRlc3RdID0gYWxsQXJncyBhcyBbQ29weVNvdXJjZU9wdGlvbnMsIENvcHlEZXN0aW5hdGlvbk9wdGlvbnNdXG4gICAgcmV0dXJuIGF3YWl0IHRoaXMuY29weU9iamVjdFYyKHNvdXJjZSwgZGVzdClcbiAgfVxuXG4gIGFzeW5jIHVwbG9hZFBhcnQoXG4gICAgcGFydENvbmZpZzoge1xuICAgICAgYnVja2V0TmFtZTogc3RyaW5nXG4gICAgICBvYmplY3ROYW1lOiBzdHJpbmdcbiAgICAgIHVwbG9hZElEOiBzdHJpbmdcbiAgICAgIHBhcnROdW1iZXI6IG51bWJlclxuICAgICAgaGVhZGVyczogUmVxdWVzdEhlYWRlcnNcbiAgICB9LFxuICAgIHBheWxvYWQ/OiBCaW5hcnksXG4gICkge1xuICAgIGNvbnN0IHsgYnVja2V0TmFtZSwgb2JqZWN0TmFtZSwgdXBsb2FkSUQsIHBhcnROdW1iZXIsIGhlYWRlcnMgfSA9IHBhcnRDb25maWdcblxuICAgIGNvbnN0IG1ldGhvZCA9ICdQVVQnXG4gICAgY29uc3QgcXVlcnkgPSBgdXBsb2FkSWQ9JHt1cGxvYWRJRH0mcGFydE51bWJlcj0ke3BhcnROdW1iZXJ9YFxuICAgIGNvbnN0IHJlcXVlc3RPcHRpb25zID0geyBtZXRob2QsIGJ1Y2tldE5hbWUsIG9iamVjdE5hbWU6IG9iamVjdE5hbWUsIHF1ZXJ5LCBoZWFkZXJzIH1cbiAgICBjb25zdCByZXMgPSBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0QXN5bmMocmVxdWVzdE9wdGlvbnMsIHBheWxvYWQpXG4gICAgY29uc3QgYm9keSA9IGF3YWl0IHJlYWRBc1N0cmluZyhyZXMpXG4gICAgY29uc3QgcGFydFJlcyA9IHVwbG9hZFBhcnRQYXJzZXIoYm9keSlcbiAgICBjb25zdCBwYXJ0RXRhZ1ZhbCA9IHNhbml0aXplRVRhZyhyZXMuaGVhZGVycy5ldGFnKSB8fCBzYW5pdGl6ZUVUYWcocGFydFJlcy5FVGFnKVxuICAgIHJldHVybiB7XG4gICAgICBldGFnOiBwYXJ0RXRhZ1ZhbCxcbiAgICAgIGtleTogb2JqZWN0TmFtZSxcbiAgICAgIHBhcnQ6IHBhcnROdW1iZXIsXG4gICAgfVxuICB9XG5cbiAgYXN5bmMgY29tcG9zZU9iamVjdChcbiAgICBkZXN0T2JqQ29uZmlnOiBDb3B5RGVzdGluYXRpb25PcHRpb25zLFxuICAgIHNvdXJjZU9iakxpc3Q6IENvcHlTb3VyY2VPcHRpb25zW10sXG4gICAgeyBtYXhDb25jdXJyZW5jeSA9IDEwIH0gPSB7fSxcbiAgKTogUHJvbWlzZTxib29sZWFuIHwgeyBldGFnOiBzdHJpbmc7IHZlcnNpb25JZDogc3RyaW5nIHwgbnVsbCB9IHwgUHJvbWlzZTx2b2lkPiB8IENvcHlPYmplY3RSZXN1bHQ+IHtcbiAgICBjb25zdCBzb3VyY2VGaWxlc0xlbmd0aCA9IHNvdXJjZU9iakxpc3QubGVuZ3RoXG5cbiAgICBpZiAoIUFycmF5LmlzQXJyYXkoc291cmNlT2JqTGlzdCkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEFyZ3VtZW50RXJyb3IoJ3NvdXJjZUNvbmZpZyBzaG91bGQgYW4gYXJyYXkgb2YgQ29weVNvdXJjZU9wdGlvbnMgJylcbiAgICB9XG4gICAgaWYgKCEoZGVzdE9iakNvbmZpZyBpbnN0YW5jZW9mIENvcHlEZXN0aW5hdGlvbk9wdGlvbnMpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKCdkZXN0Q29uZmlnIHNob3VsZCBvZiB0eXBlIENvcHlEZXN0aW5hdGlvbk9wdGlvbnMgJylcbiAgICB9XG5cbiAgICBpZiAoc291cmNlRmlsZXNMZW5ndGggPCAxIHx8IHNvdXJjZUZpbGVzTGVuZ3RoID4gUEFSVF9DT05TVFJBSU5UUy5NQVhfUEFSVFNfQ09VTlQpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEFyZ3VtZW50RXJyb3IoXG4gICAgICAgIGBcIlRoZXJlIG11c3QgYmUgYXMgbGVhc3Qgb25lIGFuZCB1cCB0byAke1BBUlRfQ09OU1RSQUlOVFMuTUFYX1BBUlRTX0NPVU5UfSBzb3VyY2Ugb2JqZWN0cy5gLFxuICAgICAgKVxuICAgIH1cblxuICAgIGZvciAobGV0IGkgPSAwOyBpIDwgc291cmNlRmlsZXNMZW5ndGg7IGkrKykge1xuICAgICAgY29uc3Qgc09iaiA9IHNvdXJjZU9iakxpc3RbaV0gYXMgQ29weVNvdXJjZU9wdGlvbnNcbiAgICAgIGlmICghc09iai52YWxpZGF0ZSgpKSB7XG4gICAgICAgIHJldHVybiBmYWxzZVxuICAgICAgfVxuICAgIH1cblxuICAgIGlmICghKGRlc3RPYmpDb25maWcgYXMgQ29weURlc3RpbmF0aW9uT3B0aW9ucykudmFsaWRhdGUoKSkge1xuICAgICAgcmV0dXJuIGZhbHNlXG4gICAgfVxuXG4gICAgY29uc3QgZ2V0U3RhdE9wdGlvbnMgPSAoc3JjQ29uZmlnOiBDb3B5U291cmNlT3B0aW9ucykgPT4ge1xuICAgICAgbGV0IHN0YXRPcHRzID0ge31cbiAgICAgIGlmICghXy5pc0VtcHR5KHNyY0NvbmZpZy5WZXJzaW9uSUQpKSB7XG4gICAgICAgIHN0YXRPcHRzID0ge1xuICAgICAgICAgIHZlcnNpb25JZDogc3JjQ29uZmlnLlZlcnNpb25JRCxcbiAgICAgICAgfVxuICAgICAgfVxuICAgICAgcmV0dXJuIHN0YXRPcHRzXG4gICAgfVxuICAgIGNvbnN0IHNyY09iamVjdFNpemVzOiBudW1iZXJbXSA9IFtdXG4gICAgbGV0IHRvdGFsU2l6ZSA9IDBcbiAgICBsZXQgdG90YWxQYXJ0cyA9IDBcblxuICAgIGNvbnN0IHNvdXJjZU9ialN0YXRzID0gc291cmNlT2JqTGlzdC5tYXAoKHNyY0l0ZW0pID0+XG4gICAgICB0aGlzLnN0YXRPYmplY3Qoc3JjSXRlbS5CdWNrZXQsIHNyY0l0ZW0uT2JqZWN0LCBnZXRTdGF0T3B0aW9ucyhzcmNJdGVtKSksXG4gICAgKVxuXG4gICAgY29uc3Qgc3JjT2JqZWN0SW5mb3MgPSBhd2FpdCBQcm9taXNlLmFsbChzb3VyY2VPYmpTdGF0cylcblxuICAgIGNvbnN0IHZhbGlkYXRlZFN0YXRzID0gc3JjT2JqZWN0SW5mb3MubWFwKChyZXNJdGVtU3RhdCwgaW5kZXgpID0+IHtcbiAgICAgIGNvbnN0IHNyY0NvbmZpZzogQ29weVNvdXJjZU9wdGlvbnMgfCB1bmRlZmluZWQgPSBzb3VyY2VPYmpMaXN0W2luZGV4XVxuXG4gICAgICBsZXQgc3JjQ29weVNpemUgPSByZXNJdGVtU3RhdC5zaXplXG4gICAgICAvLyBDaGVjayBpZiBhIHNlZ21lbnQgaXMgc3BlY2lmaWVkLCBhbmQgaWYgc28sIGlzIHRoZVxuICAgICAgLy8gc2VnbWVudCB3aXRoaW4gb2JqZWN0IGJvdW5kcz9cbiAgICAgIGlmIChzcmNDb25maWcgJiYgc3JjQ29uZmlnLk1hdGNoUmFuZ2UpIHtcbiAgICAgICAgLy8gU2luY2UgcmFuZ2UgaXMgc3BlY2lmaWVkLFxuICAgICAgICAvLyAgICAwIDw9IHNyYy5zcmNTdGFydCA8PSBzcmMuc3JjRW5kXG4gICAgICAgIC8vIHNvIG9ubHkgaW52YWxpZCBjYXNlIHRvIGNoZWNrIGlzOlxuICAgICAgICBjb25zdCBzcmNTdGFydCA9IHNyY0NvbmZpZy5TdGFydFxuICAgICAgICBjb25zdCBzcmNFbmQgPSBzcmNDb25maWcuRW5kXG4gICAgICAgIGlmIChzcmNFbmQgPj0gc3JjQ29weVNpemUgfHwgc3JjU3RhcnQgPCAwKSB7XG4gICAgICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQXJndW1lbnRFcnJvcihcbiAgICAgICAgICAgIGBDb3B5U3JjT3B0aW9ucyAke2luZGV4fSBoYXMgaW52YWxpZCBzZWdtZW50LXRvLWNvcHkgWyR7c3JjU3RhcnR9LCAke3NyY0VuZH1dIChzaXplIGlzICR7c3JjQ29weVNpemV9KWAsXG4gICAgICAgICAgKVxuICAgICAgICB9XG4gICAgICAgIHNyY0NvcHlTaXplID0gc3JjRW5kIC0gc3JjU3RhcnQgKyAxXG4gICAgICB9XG5cbiAgICAgIC8vIE9ubHkgdGhlIGxhc3Qgc291cmNlIG1heSBiZSBsZXNzIHRoYW4gYGFic01pblBhcnRTaXplYFxuICAgICAgaWYgKHNyY0NvcHlTaXplIDwgUEFSVF9DT05TVFJBSU5UUy5BQlNfTUlOX1BBUlRfU0laRSAmJiBpbmRleCA8IHNvdXJjZUZpbGVzTGVuZ3RoIC0gMSkge1xuICAgICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKFxuICAgICAgICAgIGBDb3B5U3JjT3B0aW9ucyAke2luZGV4fSBpcyB0b28gc21hbGwgKCR7c3JjQ29weVNpemV9KSBhbmQgaXQgaXMgbm90IHRoZSBsYXN0IHBhcnQuYCxcbiAgICAgICAgKVxuICAgICAgfVxuXG4gICAgICAvLyBJcyBkYXRhIHRvIGNvcHkgdG9vIGxhcmdlP1xuICAgICAgdG90YWxTaXplICs9IHNyY0NvcHlTaXplXG4gICAgICBpZiAodG90YWxTaXplID4gUEFSVF9DT05TVFJBSU5UUy5NQVhfTVVMVElQQVJUX1BVVF9PQkpFQ1RfU0laRSkge1xuICAgICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKGBDYW5ub3QgY29tcG9zZSBhbiBvYmplY3Qgb2Ygc2l6ZSAke3RvdGFsU2l6ZX0gKD4gNVRpQilgKVxuICAgICAgfVxuXG4gICAgICAvLyByZWNvcmQgc291cmNlIHNpemVcbiAgICAgIHNyY09iamVjdFNpemVzW2luZGV4XSA9IHNyY0NvcHlTaXplXG5cbiAgICAgIC8vIGNhbGN1bGF0ZSBwYXJ0cyBuZWVkZWQgZm9yIGN1cnJlbnQgc291cmNlXG4gICAgICB0b3RhbFBhcnRzICs9IHBhcnRzUmVxdWlyZWQoc3JjQ29weVNpemUpXG4gICAgICAvLyBEbyB3ZSBuZWVkIG1vcmUgcGFydHMgdGhhbiB3ZSBhcmUgYWxsb3dlZD9cbiAgICAgIGlmICh0b3RhbFBhcnRzID4gUEFSVF9DT05TVFJBSU5UUy5NQVhfUEFSVFNfQ09VTlQpIHtcbiAgICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQXJndW1lbnRFcnJvcihcbiAgICAgICAgICBgWW91ciBwcm9wb3NlZCBjb21wb3NlIG9iamVjdCByZXF1aXJlcyBtb3JlIHRoYW4gJHtQQVJUX0NPTlNUUkFJTlRTLk1BWF9QQVJUU19DT1VOVH0gcGFydHNgLFxuICAgICAgICApXG4gICAgICB9XG5cbiAgICAgIHJldHVybiByZXNJdGVtU3RhdFxuICAgIH0pXG5cbiAgICBpZiAoKHRvdGFsUGFydHMgPT09IDEgJiYgdG90YWxTaXplIDw9IFBBUlRfQ09OU1RSQUlOVFMuTUFYX1BBUlRfU0laRSkgfHwgdG90YWxTaXplID09PSAwKSB7XG4gICAgICByZXR1cm4gYXdhaXQgdGhpcy5jb3B5T2JqZWN0KHNvdXJjZU9iakxpc3RbMF0gYXMgQ29weVNvdXJjZU9wdGlvbnMsIGRlc3RPYmpDb25maWcpIC8vIHVzZSBjb3B5T2JqZWN0VjJcbiAgICB9XG5cbiAgICAvLyBwcmVzZXJ2ZSBldGFnIHRvIGF2b2lkIG1vZGlmaWNhdGlvbiBvZiBvYmplY3Qgd2hpbGUgY29weWluZy5cbiAgICBmb3IgKGxldCBpID0gMDsgaSA8IHNvdXJjZUZpbGVzTGVuZ3RoOyBpKyspIHtcbiAgICAgIDsoc291cmNlT2JqTGlzdFtpXSBhcyBDb3B5U291cmNlT3B0aW9ucykuTWF0Y2hFVGFnID0gKHZhbGlkYXRlZFN0YXRzW2ldIGFzIEJ1Y2tldEl0ZW1TdGF0KS5ldGFnXG4gICAgfVxuXG4gICAgY29uc3Qgc3BsaXRQYXJ0U2l6ZUxpc3QgPSB2YWxpZGF0ZWRTdGF0cy5tYXAoKHJlc0l0ZW1TdGF0LCBpZHgpID0+IHtcbiAgICAgIHJldHVybiBjYWxjdWxhdGVFdmVuU3BsaXRzKHNyY09iamVjdFNpemVzW2lkeF0gYXMgbnVtYmVyLCBzb3VyY2VPYmpMaXN0W2lkeF0gYXMgQ29weVNvdXJjZU9wdGlvbnMpXG4gICAgfSlcblxuICAgIGNvbnN0IGdldFVwbG9hZFBhcnRDb25maWdMaXN0ID0gKHVwbG9hZElkOiBzdHJpbmcpID0+IHtcbiAgICAgIGNvbnN0IHVwbG9hZFBhcnRDb25maWdMaXN0OiBVcGxvYWRQYXJ0Q29uZmlnW10gPSBbXVxuXG4gICAgICBzcGxpdFBhcnRTaXplTGlzdC5mb3JFYWNoKChzcGxpdFNpemUsIHNwbGl0SW5kZXg6IG51bWJlcikgPT4ge1xuICAgICAgICBpZiAoc3BsaXRTaXplKSB7XG4gICAgICAgICAgY29uc3QgeyBzdGFydEluZGV4OiBzdGFydElkeCwgZW5kSW5kZXg6IGVuZElkeCB9ID0gc3BsaXRTaXplXG5cbiAgICAgICAgICBjb25zdCBwYXJ0SW5kZXggPSBzcGxpdEluZGV4ICsgMSAvLyBwYXJ0IGluZGV4IHN0YXJ0cyBmcm9tIDEuXG4gICAgICAgICAgY29uc3QgdG90YWxVcGxvYWRzID0gQXJyYXkuZnJvbShzdGFydElkeClcblxuICAgICAgICAgIGNvbnN0IGhlYWRlcnMgPSAoc291cmNlT2JqTGlzdFtzcGxpdEluZGV4XSBhcyBDb3B5U291cmNlT3B0aW9ucykuZ2V0SGVhZGVycygpXG5cbiAgICAgICAgICB0b3RhbFVwbG9hZHMuZm9yRWFjaCgoc3BsaXRTdGFydCwgdXBsZEN0cklkeCkgPT4ge1xuICAgICAgICAgICAgY29uc3Qgc3BsaXRFbmQgPSBlbmRJZHhbdXBsZEN0cklkeF1cblxuICAgICAgICAgICAgLy8geC1hbXotY29weS1zb3VyY2UgaXMgYWxyZWFkeSBzZXQgYnkgQ29weVNvdXJjZU9wdGlvbnMuZ2V0SGVhZGVycygpIGFib3ZlLFxuICAgICAgICAgICAgLy8gVVJJLWVzY2FwZWQgYW5kIGluY2x1ZGluZyB0aGUgP3ZlcnNpb25JZCBzZWxlY3RvciB3aGVuIHBpbm5lZC4gRG9uJ3RcbiAgICAgICAgICAgIC8vIG92ZXJ3cml0ZSBpdCBoZXJlOiB0aGUgcmF3IHZhbHVlIGJyb2tlIG5vbi1BU0NJSSBuYW1lcyAoRVJSX0lOVkFMSURfQ0hBUixcbiAgICAgICAgICAgIC8vICMxMzg1KSBhbmQgc2lsZW50bHkgZHJvcHBlZCB0aGUgc291cmNlIHZlcnNpb24uXG4gICAgICAgICAgICBoZWFkZXJzWyd4LWFtei1jb3B5LXNvdXJjZS1yYW5nZSddID0gYGJ5dGVzPSR7c3BsaXRTdGFydH0tJHtzcGxpdEVuZH1gXG5cbiAgICAgICAgICAgIGNvbnN0IHVwbG9hZFBhcnRDb25maWcgPSB7XG4gICAgICAgICAgICAgIGJ1Y2tldE5hbWU6IGRlc3RPYmpDb25maWcuQnVja2V0LFxuICAgICAgICAgICAgICBvYmplY3ROYW1lOiBkZXN0T2JqQ29uZmlnLk9iamVjdCxcbiAgICAgICAgICAgICAgdXBsb2FkSUQ6IHVwbG9hZElkLFxuICAgICAgICAgICAgICBwYXJ0TnVtYmVyOiBwYXJ0SW5kZXgsXG4gICAgICAgICAgICAgIGhlYWRlcnM6IGhlYWRlcnMsXG4gICAgICAgICAgICB9XG5cbiAgICAgICAgICAgIHVwbG9hZFBhcnRDb25maWdMaXN0LnB1c2godXBsb2FkUGFydENvbmZpZylcbiAgICAgICAgICB9KVxuICAgICAgICB9XG4gICAgICB9KVxuXG4gICAgICByZXR1cm4gdXBsb2FkUGFydENvbmZpZ0xpc3RcbiAgICB9XG5cbiAgICBjb25zdCB1cGxvYWRBbGxQYXJ0cyA9IGFzeW5jICh1cGxvYWRMaXN0OiBVcGxvYWRQYXJ0Q29uZmlnW10pID0+IHtcbiAgICAgIGNvbnN0IHBhcnRVcGxvYWRzOiBBd2FpdGVkPFJldHVyblR5cGU8dHlwZW9mIHRoaXMudXBsb2FkUGFydD4+W10gPSBbXVxuXG4gICAgICAvLyBQcm9jZXNzIHVwbG9hZCBwYXJ0cyBpbiBiYXRjaGVzIHRvIGF2b2lkIHRvbyBtYW55IGNvbmN1cnJlbnQgcmVxdWVzdHNcbiAgICAgIGZvciAoY29uc3QgYmF0Y2ggb2YgXy5jaHVuayh1cGxvYWRMaXN0LCBtYXhDb25jdXJyZW5jeSkpIHtcbiAgICAgICAgY29uc3QgYmF0Y2hSZXN1bHRzID0gYXdhaXQgUHJvbWlzZS5hbGwoYmF0Y2gubWFwKChpdGVtKSA9PiB0aGlzLnVwbG9hZFBhcnQoaXRlbSkpKVxuXG4gICAgICAgIHBhcnRVcGxvYWRzLnB1c2goLi4uYmF0Y2hSZXN1bHRzKVxuICAgICAgfVxuXG4gICAgICAvLyBQcm9jZXNzIHJlc3VsdHMgaGVyZSBpZiBuZWVkZWRcbiAgICAgIHJldHVybiBwYXJ0VXBsb2Fkc1xuICAgIH1cblxuICAgIGNvbnN0IHBlcmZvcm1VcGxvYWRQYXJ0cyA9IGFzeW5jICh1cGxvYWRJZDogc3RyaW5nKSA9PiB7XG4gICAgICBjb25zdCB1cGxvYWRMaXN0ID0gZ2V0VXBsb2FkUGFydENvbmZpZ0xpc3QodXBsb2FkSWQpXG4gICAgICBjb25zdCBwYXJ0c1JlcyA9IGF3YWl0IHVwbG9hZEFsbFBhcnRzKHVwbG9hZExpc3QpXG4gICAgICByZXR1cm4gcGFydHNSZXMubWFwKChwYXJ0Q29weSkgPT4gKHsgZXRhZzogcGFydENvcHkuZXRhZywgcGFydDogcGFydENvcHkucGFydCB9KSlcbiAgICB9XG5cbiAgICBjb25zdCBuZXdVcGxvYWRIZWFkZXJzID0gZGVzdE9iakNvbmZpZy5nZXRIZWFkZXJzKClcblxuICAgIGNvbnN0IHVwbG9hZElkID0gYXdhaXQgdGhpcy5pbml0aWF0ZU5ld011bHRpcGFydFVwbG9hZChkZXN0T2JqQ29uZmlnLkJ1Y2tldCwgZGVzdE9iakNvbmZpZy5PYmplY3QsIG5ld1VwbG9hZEhlYWRlcnMpXG4gICAgdHJ5IHtcbiAgICAgIGNvbnN0IHBhcnRzRG9uZSA9IGF3YWl0IHBlcmZvcm1VcGxvYWRQYXJ0cyh1cGxvYWRJZClcbiAgICAgIHJldHVybiBhd2FpdCB0aGlzLmNvbXBsZXRlTXVsdGlwYXJ0VXBsb2FkKGRlc3RPYmpDb25maWcuQnVja2V0LCBkZXN0T2JqQ29uZmlnLk9iamVjdCwgdXBsb2FkSWQsIHBhcnRzRG9uZSlcbiAgICB9IGNhdGNoIChlcnIpIHtcbiAgICAgIHJldHVybiBhd2FpdCB0aGlzLmFib3J0TXVsdGlwYXJ0VXBsb2FkKGRlc3RPYmpDb25maWcuQnVja2V0LCBkZXN0T2JqQ29uZmlnLk9iamVjdCwgdXBsb2FkSWQpXG4gICAgfVxuICB9XG5cbiAgYXN5bmMgcHJlc2lnbmVkVXJsKFxuICAgIG1ldGhvZDogc3RyaW5nLFxuICAgIGJ1Y2tldE5hbWU6IHN0cmluZyxcbiAgICBvYmplY3ROYW1lOiBzdHJpbmcsXG4gICAgZXhwaXJlcz86IG51bWJlciB8IFByZVNpZ25SZXF1ZXN0UGFyYW1zIHwgdW5kZWZpbmVkLFxuICAgIHJlcVBhcmFtcz86IFByZVNpZ25SZXF1ZXN0UGFyYW1zIHwgRGF0ZSxcbiAgICByZXF1ZXN0RGF0ZT86IERhdGUsXG4gICk6IFByb21pc2U8c3RyaW5nPiB7XG4gICAgaWYgKHRoaXMuYW5vbnltb3VzKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkFub255bW91c1JlcXVlc3RFcnJvcihgUHJlc2lnbmVkICR7bWV0aG9kfSB1cmwgY2Fubm90IGJlIGdlbmVyYXRlZCBmb3IgYW5vbnltb3VzIHJlcXVlc3RzYClcbiAgICB9XG5cbiAgICBpZiAoIWV4cGlyZXMpIHtcbiAgICAgIGV4cGlyZXMgPSBQUkVTSUdOX0VYUElSWV9EQVlTX01BWFxuICAgIH1cbiAgICBpZiAoIXJlcVBhcmFtcykge1xuICAgICAgcmVxUGFyYW1zID0ge31cbiAgICB9XG4gICAgaWYgKCFyZXF1ZXN0RGF0ZSkge1xuICAgICAgcmVxdWVzdERhdGUgPSBuZXcgRGF0ZSgpXG4gICAgfVxuXG4gICAgLy8gVHlwZSBhc3NlcnRpb25zXG4gICAgaWYgKGV4cGlyZXMgJiYgdHlwZW9mIGV4cGlyZXMgIT09ICdudW1iZXInKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdleHBpcmVzIHNob3VsZCBiZSBvZiB0eXBlIFwibnVtYmVyXCInKVxuICAgIH1cbiAgICBpZiAocmVxUGFyYW1zICYmIHR5cGVvZiByZXFQYXJhbXMgIT09ICdvYmplY3QnKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdyZXFQYXJhbXMgc2hvdWxkIGJlIG9mIHR5cGUgXCJvYmplY3RcIicpXG4gICAgfVxuICAgIGlmICgocmVxdWVzdERhdGUgJiYgIShyZXF1ZXN0RGF0ZSBpbnN0YW5jZW9mIERhdGUpKSB8fCAocmVxdWVzdERhdGUgJiYgaXNOYU4ocmVxdWVzdERhdGU/LmdldFRpbWUoKSkpKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdyZXF1ZXN0RGF0ZSBzaG91bGQgYmUgb2YgdHlwZSBcIkRhdGVcIiBhbmQgdmFsaWQnKVxuICAgIH1cblxuICAgIGNvbnN0IHF1ZXJ5ID0gcmVxUGFyYW1zID8gcXMuc3RyaW5naWZ5KHJlcVBhcmFtcykgOiB1bmRlZmluZWRcblxuICAgIHRyeSB7XG4gICAgICBjb25zdCByZWdpb24gPSBhd2FpdCB0aGlzLmdldEJ1Y2tldFJlZ2lvbkFzeW5jKGJ1Y2tldE5hbWUpXG4gICAgICBhd2FpdCB0aGlzLmNoZWNrQW5kUmVmcmVzaENyZWRzKClcbiAgICAgIGNvbnN0IHJlcU9wdGlvbnMgPSB0aGlzLmdldFJlcXVlc3RPcHRpb25zKHsgbWV0aG9kLCByZWdpb24sIGJ1Y2tldE5hbWUsIG9iamVjdE5hbWUsIHF1ZXJ5IH0pXG5cbiAgICAgIHJldHVybiBwcmVzaWduU2lnbmF0dXJlVjQoXG4gICAgICAgIHJlcU9wdGlvbnMsXG4gICAgICAgIHRoaXMuYWNjZXNzS2V5LFxuICAgICAgICB0aGlzLnNlY3JldEtleSxcbiAgICAgICAgdGhpcy5zZXNzaW9uVG9rZW4sXG4gICAgICAgIHJlZ2lvbixcbiAgICAgICAgcmVxdWVzdERhdGUsXG4gICAgICAgIGV4cGlyZXMsXG4gICAgICApXG4gICAgfSBjYXRjaCAoZXJyKSB7XG4gICAgICBpZiAoZXJyIGluc3RhbmNlb2YgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IpIHtcbiAgICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQXJndW1lbnRFcnJvcihgVW5hYmxlIHRvIGdldCBidWNrZXQgcmVnaW9uIGZvciAke2J1Y2tldE5hbWV9LmApXG4gICAgICB9XG5cbiAgICAgIHRocm93IGVyclxuICAgIH1cbiAgfVxuXG4gIGFzeW5jIHByZXNpZ25lZEdldE9iamVjdChcbiAgICBidWNrZXROYW1lOiBzdHJpbmcsXG4gICAgb2JqZWN0TmFtZTogc3RyaW5nLFxuICAgIGV4cGlyZXM/OiBudW1iZXIsXG4gICAgcmVzcEhlYWRlcnM/OiBQcmVTaWduUmVxdWVzdFBhcmFtcyB8IERhdGUsXG4gICAgcmVxdWVzdERhdGU/OiBEYXRlLFxuICApOiBQcm9taXNlPHN0cmluZz4ge1xuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcignSW52YWxpZCBidWNrZXQgbmFtZTogJyArIGJ1Y2tldE5hbWUpXG4gICAgfVxuICAgIGlmICghaXNWYWxpZE9iamVjdE5hbWUob2JqZWN0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZE9iamVjdE5hbWVFcnJvcihgSW52YWxpZCBvYmplY3QgbmFtZTogJHtvYmplY3ROYW1lfWApXG4gICAgfVxuXG4gICAgY29uc3QgdmFsaWRSZXNwSGVhZGVycyA9IFtcbiAgICAgICdyZXNwb25zZS1jb250ZW50LXR5cGUnLFxuICAgICAgJ3Jlc3BvbnNlLWNvbnRlbnQtbGFuZ3VhZ2UnLFxuICAgICAgJ3Jlc3BvbnNlLWV4cGlyZXMnLFxuICAgICAgJ3Jlc3BvbnNlLWNhY2hlLWNvbnRyb2wnLFxuICAgICAgJ3Jlc3BvbnNlLWNvbnRlbnQtZGlzcG9zaXRpb24nLFxuICAgICAgJ3Jlc3BvbnNlLWNvbnRlbnQtZW5jb2RpbmcnLFxuICAgIF1cbiAgICB2YWxpZFJlc3BIZWFkZXJzLmZvckVhY2goKGhlYWRlcikgPT4ge1xuICAgICAgLy8gQHRzLWlnbm9yZVxuICAgICAgaWYgKHJlc3BIZWFkZXJzICE9PSB1bmRlZmluZWQgJiYgcmVzcEhlYWRlcnNbaGVhZGVyXSAhPT0gdW5kZWZpbmVkICYmICFpc1N0cmluZyhyZXNwSGVhZGVyc1toZWFkZXJdKSkge1xuICAgICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKGByZXNwb25zZSBoZWFkZXIgJHtoZWFkZXJ9IHNob3VsZCBiZSBvZiB0eXBlIFwic3RyaW5nXCJgKVxuICAgICAgfVxuICAgIH0pXG4gICAgcmV0dXJuIHRoaXMucHJlc2lnbmVkVXJsKCdHRVQnLCBidWNrZXROYW1lLCBvYmplY3ROYW1lLCBleHBpcmVzLCByZXNwSGVhZGVycywgcmVxdWVzdERhdGUpXG4gIH1cblxuICBhc3luYyBwcmVzaWduZWRQdXRPYmplY3QoYnVja2V0TmFtZTogc3RyaW5nLCBvYmplY3ROYW1lOiBzdHJpbmcsIGV4cGlyZXM/OiBudW1iZXIpOiBQcm9taXNlPHN0cmluZz4ge1xuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcihgSW52YWxpZCBidWNrZXQgbmFtZTogJHtidWNrZXROYW1lfWApXG4gICAgfVxuICAgIGlmICghaXNWYWxpZE9iamVjdE5hbWUob2JqZWN0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZE9iamVjdE5hbWVFcnJvcihgSW52YWxpZCBvYmplY3QgbmFtZTogJHtvYmplY3ROYW1lfWApXG4gICAgfVxuXG4gICAgcmV0dXJuIHRoaXMucHJlc2lnbmVkVXJsKCdQVVQnLCBidWNrZXROYW1lLCBvYmplY3ROYW1lLCBleHBpcmVzKVxuICB9XG5cbiAgbmV3UG9zdFBvbGljeSgpOiBQb3N0UG9saWN5IHtcbiAgICByZXR1cm4gbmV3IFBvc3RQb2xpY3koKVxuICB9XG5cbiAgYXN5bmMgcHJlc2lnbmVkUG9zdFBvbGljeShwb3N0UG9saWN5OiBQb3N0UG9saWN5KTogUHJvbWlzZTxQb3N0UG9saWN5UmVzdWx0PiB7XG4gICAgaWYgKHRoaXMuYW5vbnltb3VzKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkFub255bW91c1JlcXVlc3RFcnJvcignUHJlc2lnbmVkIFBPU1QgcG9saWN5IGNhbm5vdCBiZSBnZW5lcmF0ZWQgZm9yIGFub255bW91cyByZXF1ZXN0cycpXG4gICAgfVxuICAgIGlmICghaXNPYmplY3QocG9zdFBvbGljeSkpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ3Bvc3RQb2xpY3kgc2hvdWxkIGJlIG9mIHR5cGUgXCJvYmplY3RcIicpXG4gICAgfVxuICAgIGNvbnN0IGJ1Y2tldE5hbWUgPSBwb3N0UG9saWN5LmZvcm1EYXRhLmJ1Y2tldCBhcyBzdHJpbmdcbiAgICB0cnkge1xuICAgICAgY29uc3QgcmVnaW9uID0gYXdhaXQgdGhpcy5nZXRCdWNrZXRSZWdpb25Bc3luYyhidWNrZXROYW1lKVxuXG4gICAgICBjb25zdCBkYXRlID0gbmV3IERhdGUoKVxuICAgICAgY29uc3QgZGF0ZVN0ciA9IG1ha2VEYXRlTG9uZyhkYXRlKVxuICAgICAgYXdhaXQgdGhpcy5jaGVja0FuZFJlZnJlc2hDcmVkcygpXG5cbiAgICAgIGlmICghcG9zdFBvbGljeS5wb2xpY3kuZXhwaXJhdGlvbikge1xuICAgICAgICAvLyAnZXhwaXJhdGlvbicgaXMgbWFuZGF0b3J5IGZpZWxkIGZvciBTMy5cbiAgICAgICAgLy8gU2V0IGRlZmF1bHQgZXhwaXJhdGlvbiBkYXRlIG9mIDcgZGF5cy5cbiAgICAgICAgY29uc3QgZXhwaXJlcyA9IG5ldyBEYXRlKClcbiAgICAgICAgZXhwaXJlcy5zZXRTZWNvbmRzKFBSRVNJR05fRVhQSVJZX0RBWVNfTUFYKVxuICAgICAgICBwb3N0UG9saWN5LnNldEV4cGlyZXMoZXhwaXJlcylcbiAgICAgIH1cblxuICAgICAgcG9zdFBvbGljeS5wb2xpY3kuY29uZGl0aW9ucy5wdXNoKFsnZXEnLCAnJHgtYW16LWRhdGUnLCBkYXRlU3RyXSlcbiAgICAgIHBvc3RQb2xpY3kuZm9ybURhdGFbJ3gtYW16LWRhdGUnXSA9IGRhdGVTdHJcblxuICAgICAgcG9zdFBvbGljeS5wb2xpY3kuY29uZGl0aW9ucy5wdXNoKFsnZXEnLCAnJHgtYW16LWFsZ29yaXRobScsICdBV1M0LUhNQUMtU0hBMjU2J10pXG4gICAgICBwb3N0UG9saWN5LmZvcm1EYXRhWyd4LWFtei1hbGdvcml0aG0nXSA9ICdBV1M0LUhNQUMtU0hBMjU2J1xuXG4gICAgICBwb3N0UG9saWN5LnBvbGljeS5jb25kaXRpb25zLnB1c2goWydlcScsICckeC1hbXotY3JlZGVudGlhbCcsIHRoaXMuYWNjZXNzS2V5ICsgJy8nICsgZ2V0U2NvcGUocmVnaW9uLCBkYXRlKV0pXG4gICAgICBwb3N0UG9saWN5LmZvcm1EYXRhWyd4LWFtei1jcmVkZW50aWFsJ10gPSB0aGlzLmFjY2Vzc0tleSArICcvJyArIGdldFNjb3BlKHJlZ2lvbiwgZGF0ZSlcblxuICAgICAgaWYgKHRoaXMuc2Vzc2lvblRva2VuKSB7XG4gICAgICAgIHBvc3RQb2xpY3kucG9saWN5LmNvbmRpdGlvbnMucHVzaChbJ2VxJywgJyR4LWFtei1zZWN1cml0eS10b2tlbicsIHRoaXMuc2Vzc2lvblRva2VuXSlcbiAgICAgICAgcG9zdFBvbGljeS5mb3JtRGF0YVsneC1hbXotc2VjdXJpdHktdG9rZW4nXSA9IHRoaXMuc2Vzc2lvblRva2VuXG4gICAgICB9XG5cbiAgICAgIGNvbnN0IHBvbGljeUJhc2U2NCA9IEJ1ZmZlci5mcm9tKEpTT04uc3RyaW5naWZ5KHBvc3RQb2xpY3kucG9saWN5KSkudG9TdHJpbmcoJ2Jhc2U2NCcpXG5cbiAgICAgIHBvc3RQb2xpY3kuZm9ybURhdGEucG9saWN5ID0gcG9saWN5QmFzZTY0XG5cbiAgICAgIHBvc3RQb2xpY3kuZm9ybURhdGFbJ3gtYW16LXNpZ25hdHVyZSddID0gcG9zdFByZXNpZ25TaWduYXR1cmVWNChyZWdpb24sIGRhdGUsIHRoaXMuc2VjcmV0S2V5LCBwb2xpY3lCYXNlNjQpXG4gICAgICBjb25zdCBvcHRzID0ge1xuICAgICAgICByZWdpb246IHJlZ2lvbixcbiAgICAgICAgYnVja2V0TmFtZTogYnVja2V0TmFtZSxcbiAgICAgICAgbWV0aG9kOiAnUE9TVCcsXG4gICAgICB9XG4gICAgICBjb25zdCByZXFPcHRpb25zID0gdGhpcy5nZXRSZXF1ZXN0T3B0aW9ucyhvcHRzKVxuICAgICAgY29uc3QgcG9ydFN0ciA9IHRoaXMucG9ydCA9PSA4MCB8fCB0aGlzLnBvcnQgPT09IDQ0MyA/ICcnIDogYDoke3RoaXMucG9ydC50b1N0cmluZygpfWBcbiAgICAgIGNvbnN0IHVybFN0ciA9IGAke3JlcU9wdGlvbnMucHJvdG9jb2x9Ly8ke3JlcU9wdGlvbnMuaG9zdH0ke3BvcnRTdHJ9JHtyZXFPcHRpb25zLnBhdGh9YFxuICAgICAgcmV0dXJuIHsgcG9zdFVSTDogdXJsU3RyLCBmb3JtRGF0YTogcG9zdFBvbGljeS5mb3JtRGF0YSB9XG4gICAgfSBjYXRjaCAoZXJyKSB7XG4gICAgICBpZiAoZXJyIGluc3RhbmNlb2YgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IpIHtcbiAgICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQXJndW1lbnRFcnJvcihgVW5hYmxlIHRvIGdldCBidWNrZXQgcmVnaW9uIGZvciAke2J1Y2tldE5hbWV9LmApXG4gICAgICB9XG5cbiAgICAgIHRocm93IGVyclxuICAgIH1cbiAgfVxuICAvLyBsaXN0IGEgYmF0Y2ggb2Ygb2JqZWN0c1xuICBhc3luYyBsaXN0T2JqZWN0c1F1ZXJ5KGJ1Y2tldE5hbWU6IHN0cmluZywgcHJlZml4Pzogc3RyaW5nLCBtYXJrZXI/OiBzdHJpbmcsIGxpc3RRdWVyeU9wdHM/OiBMaXN0T2JqZWN0UXVlcnlPcHRzKSB7XG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZShidWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKCdJbnZhbGlkIGJ1Y2tldCBuYW1lOiAnICsgYnVja2V0TmFtZSlcbiAgICB9XG4gICAgaWYgKCFpc1N0cmluZyhwcmVmaXgpKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdwcmVmaXggc2hvdWxkIGJlIG9mIHR5cGUgXCJzdHJpbmdcIicpXG4gICAgfVxuICAgIGlmIChtYXJrZXIgJiYgIWlzU3RyaW5nKG1hcmtlcikpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ21hcmtlciBzaG91bGQgYmUgb2YgdHlwZSBcInN0cmluZ1wiJylcbiAgICB9XG5cbiAgICBpZiAobGlzdFF1ZXJ5T3B0cyAmJiAhaXNPYmplY3QobGlzdFF1ZXJ5T3B0cykpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ2xpc3RRdWVyeU9wdHMgc2hvdWxkIGJlIG9mIHR5cGUgXCJvYmplY3RcIicpXG4gICAgfVxuICAgIGxldCB7IERlbGltaXRlciwgTWF4S2V5cywgSW5jbHVkZVZlcnNpb24sIHZlcnNpb25JZE1hcmtlciwga2V5TWFya2VyIH0gPSBsaXN0UXVlcnlPcHRzIGFzIExpc3RPYmplY3RRdWVyeU9wdHNcblxuICAgIGlmICghaXNTdHJpbmcoRGVsaW1pdGVyKSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcignRGVsaW1pdGVyIHNob3VsZCBiZSBvZiB0eXBlIFwic3RyaW5nXCInKVxuICAgIH1cbiAgICBpZiAoIWlzTnVtYmVyKE1heEtleXMpKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdNYXhLZXlzIHNob3VsZCBiZSBvZiB0eXBlIFwibnVtYmVyXCInKVxuICAgIH1cblxuICAgIGNvbnN0IHF1ZXJpZXMgPSBbXVxuICAgIC8vIGVzY2FwZSBldmVyeSB2YWx1ZSBpbiBxdWVyeSBzdHJpbmcsIGV4Y2VwdCBtYXhLZXlzXG4gICAgcXVlcmllcy5wdXNoKGBwcmVmaXg9JHt1cmlFc2NhcGUocHJlZml4KX1gKVxuICAgIHF1ZXJpZXMucHVzaChgZGVsaW1pdGVyPSR7dXJpRXNjYXBlKERlbGltaXRlcil9YClcbiAgICBxdWVyaWVzLnB1c2goYGVuY29kaW5nLXR5cGU9dXJsYClcblxuICAgIGlmIChJbmNsdWRlVmVyc2lvbikge1xuICAgICAgcXVlcmllcy5wdXNoKGB2ZXJzaW9uc2ApXG4gICAgfVxuXG4gICAgaWYgKEluY2x1ZGVWZXJzaW9uKSB7XG4gICAgICAvLyB2MSB2ZXJzaW9uIGxpc3RpbmcuLlxuICAgICAgaWYgKGtleU1hcmtlcikge1xuICAgICAgICBxdWVyaWVzLnB1c2goYGtleS1tYXJrZXI9JHtrZXlNYXJrZXJ9YClcbiAgICAgIH1cbiAgICAgIGlmICh2ZXJzaW9uSWRNYXJrZXIpIHtcbiAgICAgICAgcXVlcmllcy5wdXNoKGB2ZXJzaW9uLWlkLW1hcmtlcj0ke3ZlcnNpb25JZE1hcmtlcn1gKVxuICAgICAgfVxuICAgIH0gZWxzZSBpZiAobWFya2VyKSB7XG4gICAgICBtYXJrZXIgPSB1cmlFc2NhcGUobWFya2VyKVxuICAgICAgcXVlcmllcy5wdXNoKGBtYXJrZXI9JHttYXJrZXJ9YClcbiAgICB9XG5cbiAgICAvLyBubyBuZWVkIHRvIGVzY2FwZSBtYXhLZXlzXG4gICAgaWYgKE1heEtleXMpIHtcbiAgICAgIGlmIChNYXhLZXlzID49IDEwMDApIHtcbiAgICAgICAgTWF4S2V5cyA9IDEwMDBcbiAgICAgIH1cbiAgICAgIHF1ZXJpZXMucHVzaChgbWF4LWtleXM9JHtNYXhLZXlzfWApXG4gICAgfVxuICAgIHF1ZXJpZXMuc29ydCgpXG4gICAgbGV0IHF1ZXJ5ID0gJydcbiAgICBpZiAocXVlcmllcy5sZW5ndGggPiAwKSB7XG4gICAgICBxdWVyeSA9IGAke3F1ZXJpZXMuam9pbignJicpfWBcbiAgICB9XG5cbiAgICBjb25zdCBtZXRob2QgPSAnR0VUJ1xuICAgIGNvbnN0IHJlcyA9IGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luYyh7IG1ldGhvZCwgYnVja2V0TmFtZSwgcXVlcnkgfSlcbiAgICBjb25zdCBib2R5ID0gYXdhaXQgcmVhZEFzU3RyaW5nKHJlcylcbiAgICBjb25zdCBsaXN0UXJ5TGlzdCA9IHBhcnNlTGlzdE9iamVjdHMoYm9keSlcbiAgICByZXR1cm4gbGlzdFFyeUxpc3RcbiAgfVxuXG4gIGxpc3RPYmplY3RzKFxuICAgIGJ1Y2tldE5hbWU6IHN0cmluZyxcbiAgICBwcmVmaXg/OiBzdHJpbmcsXG4gICAgcmVjdXJzaXZlPzogYm9vbGVhbixcbiAgICBsaXN0T3B0cz86IExpc3RPYmplY3RRdWVyeU9wdHMgfCB1bmRlZmluZWQsXG4gICk6IEJ1Y2tldFN0cmVhbTxPYmplY3RJbmZvPiB7XG4gICAgaWYgKHByZWZpeCA9PT0gdW5kZWZpbmVkKSB7XG4gICAgICBwcmVmaXggPSAnJ1xuICAgIH1cbiAgICBpZiAocmVjdXJzaXZlID09PSB1bmRlZmluZWQpIHtcbiAgICAgIHJlY3Vyc2l2ZSA9IGZhbHNlXG4gICAgfVxuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcignSW52YWxpZCBidWNrZXQgbmFtZTogJyArIGJ1Y2tldE5hbWUpXG4gICAgfVxuICAgIGlmICghaXNWYWxpZFByZWZpeChwcmVmaXgpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRQcmVmaXhFcnJvcihgSW52YWxpZCBwcmVmaXggOiAke3ByZWZpeH1gKVxuICAgIH1cbiAgICBpZiAoIWlzU3RyaW5nKHByZWZpeCkpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ3ByZWZpeCBzaG91bGQgYmUgb2YgdHlwZSBcInN0cmluZ1wiJylcbiAgICB9XG4gICAgaWYgKCFpc0Jvb2xlYW4ocmVjdXJzaXZlKSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcigncmVjdXJzaXZlIHNob3VsZCBiZSBvZiB0eXBlIFwiYm9vbGVhblwiJylcbiAgICB9XG4gICAgaWYgKGxpc3RPcHRzICYmICFpc09iamVjdChsaXN0T3B0cykpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ2xpc3RPcHRzIHNob3VsZCBiZSBvZiB0eXBlIFwib2JqZWN0XCInKVxuICAgIH1cbiAgICBsZXQgbWFya2VyOiBzdHJpbmcgfCB1bmRlZmluZWQgPSAnJ1xuICAgIGxldCBrZXlNYXJrZXI6IHN0cmluZyB8IHVuZGVmaW5lZCA9ICcnXG4gICAgbGV0IHZlcnNpb25JZE1hcmtlcjogc3RyaW5nIHwgdW5kZWZpbmVkID0gJydcbiAgICBsZXQgb2JqZWN0czogT2JqZWN0SW5mb1tdID0gW11cbiAgICBsZXQgZW5kZWQgPSBmYWxzZVxuICAgIGNvbnN0IHJlYWRTdHJlYW06IHN0cmVhbS5SZWFkYWJsZSA9IG5ldyBzdHJlYW0uUmVhZGFibGUoeyBvYmplY3RNb2RlOiB0cnVlIH0pXG4gICAgcmVhZFN0cmVhbS5fcmVhZCA9IGFzeW5jICgpID0+IHtcbiAgICAgIC8vIHB1c2ggb25lIG9iamVjdCBwZXIgX3JlYWQoKVxuICAgICAgaWYgKG9iamVjdHMubGVuZ3RoKSB7XG4gICAgICAgIHJlYWRTdHJlYW0ucHVzaChvYmplY3RzLnNoaWZ0KCkpXG4gICAgICAgIHJldHVyblxuICAgICAgfVxuICAgICAgaWYgKGVuZGVkKSB7XG4gICAgICAgIHJldHVybiByZWFkU3RyZWFtLnB1c2gobnVsbClcbiAgICAgIH1cblxuICAgICAgdHJ5IHtcbiAgICAgICAgY29uc3QgbGlzdFF1ZXJ5T3B0cyA9IHtcbiAgICAgICAgICBEZWxpbWl0ZXI6IHJlY3Vyc2l2ZSA/ICcnIDogJy8nLCAvLyBpZiByZWN1cnNpdmUgaXMgZmFsc2Ugc2V0IGRlbGltaXRlciB0byAnLydcbiAgICAgICAgICBNYXhLZXlzOiAxMDAwLFxuICAgICAgICAgIEluY2x1ZGVWZXJzaW9uOiBsaXN0T3B0cz8uSW5jbHVkZVZlcnNpb24sXG4gICAgICAgICAgLy8gdmVyc2lvbiBsaXN0aW5nIHNwZWNpZmljIG9wdGlvbnNcbiAgICAgICAgICBrZXlNYXJrZXI6IGtleU1hcmtlcixcbiAgICAgICAgICB2ZXJzaW9uSWRNYXJrZXI6IHZlcnNpb25JZE1hcmtlcixcbiAgICAgICAgfVxuXG4gICAgICAgIGNvbnN0IHJlc3VsdDogTGlzdE9iamVjdFF1ZXJ5UmVzID0gYXdhaXQgdGhpcy5saXN0T2JqZWN0c1F1ZXJ5KGJ1Y2tldE5hbWUsIHByZWZpeCwgbWFya2VyLCBsaXN0UXVlcnlPcHRzKVxuICAgICAgICBpZiAocmVzdWx0LmlzVHJ1bmNhdGVkKSB7XG4gICAgICAgICAgbWFya2VyID0gcmVzdWx0Lm5leHRNYXJrZXIgfHwgdW5kZWZpbmVkXG4gICAgICAgICAgaWYgKHJlc3VsdC5rZXlNYXJrZXIpIHtcbiAgICAgICAgICAgIGtleU1hcmtlciA9IHJlc3VsdC5rZXlNYXJrZXJcbiAgICAgICAgICB9XG4gICAgICAgICAgaWYgKHJlc3VsdC52ZXJzaW9uSWRNYXJrZXIpIHtcbiAgICAgICAgICAgIHZlcnNpb25JZE1hcmtlciA9IHJlc3VsdC52ZXJzaW9uSWRNYXJrZXJcbiAgICAgICAgICB9XG4gICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgZW5kZWQgPSB0cnVlXG4gICAgICAgIH1cbiAgICAgICAgaWYgKHJlc3VsdC5vYmplY3RzKSB7XG4gICAgICAgICAgb2JqZWN0cyA9IHJlc3VsdC5vYmplY3RzXG4gICAgICAgIH1cbiAgICAgICAgLy8gQHRzLWlnbm9yZVxuICAgICAgICByZWFkU3RyZWFtLl9yZWFkKClcbiAgICAgIH0gY2F0Y2ggKGVycikge1xuICAgICAgICByZWFkU3RyZWFtLmVtaXQoJ2Vycm9yJywgZXJyKVxuICAgICAgfVxuICAgIH1cbiAgICByZXR1cm4gcmVhZFN0cmVhbVxuICB9XG5cbiAgYXN5bmMgbGlzdE9iamVjdHNWMlF1ZXJ5KFxuICAgIGJ1Y2tldE5hbWU6IHN0cmluZyxcbiAgICBwcmVmaXg6IHN0cmluZyxcbiAgICBjb250aW51YXRpb25Ub2tlbjogc3RyaW5nLFxuICAgIGRlbGltaXRlcjogc3RyaW5nLFxuICAgIG1heEtleXM6IG51bWJlcixcbiAgICBzdGFydEFmdGVyOiBzdHJpbmcsXG4gICk6IFByb21pc2U8TGlzdE9iamVjdFYyUmVzPiB7XG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZShidWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKCdJbnZhbGlkIGJ1Y2tldCBuYW1lOiAnICsgYnVja2V0TmFtZSlcbiAgICB9XG4gICAgaWYgKCFpc1N0cmluZyhwcmVmaXgpKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdwcmVmaXggc2hvdWxkIGJlIG9mIHR5cGUgXCJzdHJpbmdcIicpXG4gICAgfVxuICAgIGlmICghaXNTdHJpbmcoY29udGludWF0aW9uVG9rZW4pKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdjb250aW51YXRpb25Ub2tlbiBzaG91bGQgYmUgb2YgdHlwZSBcInN0cmluZ1wiJylcbiAgICB9XG4gICAgaWYgKCFpc1N0cmluZyhkZWxpbWl0ZXIpKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdkZWxpbWl0ZXIgc2hvdWxkIGJlIG9mIHR5cGUgXCJzdHJpbmdcIicpXG4gICAgfVxuICAgIGlmICghaXNOdW1iZXIobWF4S2V5cykpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ21heEtleXMgc2hvdWxkIGJlIG9mIHR5cGUgXCJudW1iZXJcIicpXG4gICAgfVxuICAgIGlmICghaXNTdHJpbmcoc3RhcnRBZnRlcikpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ3N0YXJ0QWZ0ZXIgc2hvdWxkIGJlIG9mIHR5cGUgXCJzdHJpbmdcIicpXG4gICAgfVxuXG4gICAgY29uc3QgcXVlcmllcyA9IFtdXG4gICAgcXVlcmllcy5wdXNoKGBsaXN0LXR5cGU9MmApXG4gICAgcXVlcmllcy5wdXNoKGBlbmNvZGluZy10eXBlPXVybGApXG4gICAgcXVlcmllcy5wdXNoKGBwcmVmaXg9JHt1cmlFc2NhcGUocHJlZml4KX1gKVxuICAgIHF1ZXJpZXMucHVzaChgZGVsaW1pdGVyPSR7dXJpRXNjYXBlKGRlbGltaXRlcil9YClcblxuICAgIGlmIChjb250aW51YXRpb25Ub2tlbikge1xuICAgICAgcXVlcmllcy5wdXNoKGBjb250aW51YXRpb24tdG9rZW49JHt1cmlFc2NhcGUoY29udGludWF0aW9uVG9rZW4pfWApXG4gICAgfVxuICAgIGlmIChzdGFydEFmdGVyKSB7XG4gICAgICBxdWVyaWVzLnB1c2goYHN0YXJ0LWFmdGVyPSR7dXJpRXNjYXBlKHN0YXJ0QWZ0ZXIpfWApXG4gICAgfVxuICAgIGlmIChtYXhLZXlzKSB7XG4gICAgICBpZiAobWF4S2V5cyA+PSAxMDAwKSB7XG4gICAgICAgIG1heEtleXMgPSAxMDAwXG4gICAgICB9XG4gICAgICBxdWVyaWVzLnB1c2goYG1heC1rZXlzPSR7bWF4S2V5c31gKVxuICAgIH1cbiAgICBxdWVyaWVzLnNvcnQoKVxuICAgIGxldCBxdWVyeSA9ICcnXG4gICAgaWYgKHF1ZXJpZXMubGVuZ3RoID4gMCkge1xuICAgICAgcXVlcnkgPSBgJHtxdWVyaWVzLmpvaW4oJyYnKX1gXG4gICAgfVxuXG4gICAgY29uc3QgbWV0aG9kID0gJ0dFVCdcbiAgICBjb25zdCByZXMgPSBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0QXN5bmMoeyBtZXRob2QsIGJ1Y2tldE5hbWUsIHF1ZXJ5IH0pXG4gICAgY29uc3QgYm9keSA9IGF3YWl0IHJlYWRBc1N0cmluZyhyZXMpXG4gICAgcmV0dXJuIHBhcnNlTGlzdE9iamVjdHNWMihib2R5KVxuICB9XG5cbiAgbGlzdE9iamVjdHNWMihcbiAgICBidWNrZXROYW1lOiBzdHJpbmcsXG4gICAgcHJlZml4Pzogc3RyaW5nLFxuICAgIHJlY3Vyc2l2ZT86IGJvb2xlYW4sXG4gICAgc3RhcnRBZnRlcj86IHN0cmluZyxcbiAgKTogQnVja2V0U3RyZWFtPEJ1Y2tldEl0ZW0+IHtcbiAgICBpZiAocHJlZml4ID09PSB1bmRlZmluZWQpIHtcbiAgICAgIHByZWZpeCA9ICcnXG4gICAgfVxuICAgIGlmIChyZWN1cnNpdmUgPT09IHVuZGVmaW5lZCkge1xuICAgICAgcmVjdXJzaXZlID0gZmFsc2VcbiAgICB9XG4gICAgaWYgKHN0YXJ0QWZ0ZXIgPT09IHVuZGVmaW5lZCkge1xuICAgICAgc3RhcnRBZnRlciA9ICcnXG4gICAgfVxuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcignSW52YWxpZCBidWNrZXQgbmFtZTogJyArIGJ1Y2tldE5hbWUpXG4gICAgfVxuICAgIGlmICghaXNWYWxpZFByZWZpeChwcmVmaXgpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRQcmVmaXhFcnJvcihgSW52YWxpZCBwcmVmaXggOiAke3ByZWZpeH1gKVxuICAgIH1cbiAgICBpZiAoIWlzU3RyaW5nKHByZWZpeCkpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ3ByZWZpeCBzaG91bGQgYmUgb2YgdHlwZSBcInN0cmluZ1wiJylcbiAgICB9XG4gICAgaWYgKCFpc0Jvb2xlYW4ocmVjdXJzaXZlKSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcigncmVjdXJzaXZlIHNob3VsZCBiZSBvZiB0eXBlIFwiYm9vbGVhblwiJylcbiAgICB9XG4gICAgaWYgKCFpc1N0cmluZyhzdGFydEFmdGVyKSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcignc3RhcnRBZnRlciBzaG91bGQgYmUgb2YgdHlwZSBcInN0cmluZ1wiJylcbiAgICB9XG5cbiAgICBjb25zdCBkZWxpbWl0ZXIgPSByZWN1cnNpdmUgPyAnJyA6ICcvJ1xuICAgIGNvbnN0IHByZWZpeFN0ciA9IHByZWZpeFxuICAgIGNvbnN0IHN0YXJ0QWZ0ZXJTdHIgPSBzdGFydEFmdGVyXG4gICAgbGV0IGNvbnRpbnVhdGlvblRva2VuID0gJydcbiAgICBsZXQgb2JqZWN0czogQnVja2V0SXRlbVtdID0gW11cbiAgICBsZXQgZW5kZWQgPSBmYWxzZVxuICAgIGNvbnN0IHJlYWRTdHJlYW06IHN0cmVhbS5SZWFkYWJsZSA9IG5ldyBzdHJlYW0uUmVhZGFibGUoeyBvYmplY3RNb2RlOiB0cnVlIH0pXG4gICAgcmVhZFN0cmVhbS5fcmVhZCA9IGFzeW5jICgpID0+IHtcbiAgICAgIGlmIChvYmplY3RzLmxlbmd0aCkge1xuICAgICAgICByZWFkU3RyZWFtLnB1c2gob2JqZWN0cy5zaGlmdCgpKVxuICAgICAgICByZXR1cm5cbiAgICAgIH1cbiAgICAgIGlmIChlbmRlZCkge1xuICAgICAgICByZXR1cm4gcmVhZFN0cmVhbS5wdXNoKG51bGwpXG4gICAgICB9XG5cbiAgICAgIHRyeSB7XG4gICAgICAgIGNvbnN0IHJlc3VsdCA9IGF3YWl0IHRoaXMubGlzdE9iamVjdHNWMlF1ZXJ5KFxuICAgICAgICAgIGJ1Y2tldE5hbWUsXG4gICAgICAgICAgcHJlZml4U3RyLFxuICAgICAgICAgIGNvbnRpbnVhdGlvblRva2VuLFxuICAgICAgICAgIGRlbGltaXRlcixcbiAgICAgICAgICAxMDAwLFxuICAgICAgICAgIHN0YXJ0QWZ0ZXJTdHIsXG4gICAgICAgIClcbiAgICAgICAgaWYgKHJlc3VsdC5pc1RydW5jYXRlZCkge1xuICAgICAgICAgIGNvbnRpbnVhdGlvblRva2VuID0gcmVzdWx0Lm5leHRDb250aW51YXRpb25Ub2tlblxuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIGVuZGVkID0gdHJ1ZVxuICAgICAgICB9XG4gICAgICAgIG9iamVjdHMgPSByZXN1bHQub2JqZWN0c1xuICAgICAgICAvLyBAdHMtaWdub3JlXG4gICAgICAgIHJlYWRTdHJlYW0uX3JlYWQoKVxuICAgICAgfSBjYXRjaCAoZXJyKSB7XG4gICAgICAgIHJlYWRTdHJlYW0uZW1pdCgnZXJyb3InLCBlcnIpXG4gICAgICB9XG4gICAgfVxuICAgIHJldHVybiByZWFkU3RyZWFtXG4gIH1cblxuICBhc3luYyBzZXRCdWNrZXROb3RpZmljYXRpb24oYnVja2V0TmFtZTogc3RyaW5nLCBjb25maWc6IE5vdGlmaWNhdGlvbkNvbmZpZyk6IFByb21pc2U8dm9pZD4ge1xuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcignSW52YWxpZCBidWNrZXQgbmFtZTogJyArIGJ1Y2tldE5hbWUpXG4gICAgfVxuICAgIGlmICghaXNPYmplY3QoY29uZmlnKSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcignbm90aWZpY2F0aW9uIGNvbmZpZyBzaG91bGQgYmUgb2YgdHlwZSBcIk9iamVjdFwiJylcbiAgICB9XG4gICAgY29uc3QgbWV0aG9kID0gJ1BVVCdcbiAgICBjb25zdCBxdWVyeSA9ICdub3RpZmljYXRpb24nXG4gICAgY29uc3QgYnVpbGRlciA9IG5ldyB4bWwyanMuQnVpbGRlcih7XG4gICAgICByb290TmFtZTogJ05vdGlmaWNhdGlvbkNvbmZpZ3VyYXRpb24nLFxuICAgICAgcmVuZGVyT3B0czogeyBwcmV0dHk6IGZhbHNlIH0sXG4gICAgICBoZWFkbGVzczogdHJ1ZSxcbiAgICB9KVxuICAgIGNvbnN0IHBheWxvYWQgPSBidWlsZGVyLmJ1aWxkT2JqZWN0KGNvbmZpZylcbiAgICBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0QXN5bmNPbWl0KHsgbWV0aG9kLCBidWNrZXROYW1lLCBxdWVyeSB9LCBwYXlsb2FkKVxuICB9XG5cbiAgYXN5bmMgcmVtb3ZlQWxsQnVja2V0Tm90aWZpY2F0aW9uKGJ1Y2tldE5hbWU6IHN0cmluZyk6IFByb21pc2U8dm9pZD4ge1xuICAgIGF3YWl0IHRoaXMuc2V0QnVja2V0Tm90aWZpY2F0aW9uKGJ1Y2tldE5hbWUsIG5ldyBOb3RpZmljYXRpb25Db25maWcoKSlcbiAgfVxuXG4gIGFzeW5jIGdldEJ1Y2tldE5vdGlmaWNhdGlvbihidWNrZXROYW1lOiBzdHJpbmcpOiBQcm9taXNlPE5vdGlmaWNhdGlvbkNvbmZpZ1Jlc3VsdD4ge1xuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcignSW52YWxpZCBidWNrZXQgbmFtZTogJyArIGJ1Y2tldE5hbWUpXG4gICAgfVxuICAgIGNvbnN0IG1ldGhvZCA9ICdHRVQnXG4gICAgY29uc3QgcXVlcnkgPSAnbm90aWZpY2F0aW9uJ1xuICAgIGNvbnN0IHJlcyA9IGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luYyh7IG1ldGhvZCwgYnVja2V0TmFtZSwgcXVlcnkgfSlcbiAgICBjb25zdCBib2R5ID0gYXdhaXQgcmVhZEFzU3RyaW5nKHJlcylcbiAgICByZXR1cm4gcGFyc2VCdWNrZXROb3RpZmljYXRpb24oYm9keSlcbiAgfVxuXG4gIGxpc3RlbkJ1Y2tldE5vdGlmaWNhdGlvbihcbiAgICBidWNrZXROYW1lOiBzdHJpbmcsXG4gICAgcHJlZml4OiBzdHJpbmcsXG4gICAgc3VmZml4OiBzdHJpbmcsXG4gICAgZXZlbnRzOiBOb3RpZmljYXRpb25FdmVudFtdLFxuICApOiBOb3RpZmljYXRpb25Qb2xsZXIge1xuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcihgSW52YWxpZCBidWNrZXQgbmFtZTogJHtidWNrZXROYW1lfWApXG4gICAgfVxuICAgIGlmICghaXNTdHJpbmcocHJlZml4KSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcigncHJlZml4IG11c3QgYmUgb2YgdHlwZSBzdHJpbmcnKVxuICAgIH1cbiAgICBpZiAoIWlzU3RyaW5nKHN1ZmZpeCkpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ3N1ZmZpeCBtdXN0IGJlIG9mIHR5cGUgc3RyaW5nJylcbiAgICB9XG4gICAgaWYgKCFBcnJheS5pc0FycmF5KGV2ZW50cykpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ2V2ZW50cyBtdXN0IGJlIG9mIHR5cGUgQXJyYXknKVxuICAgIH1cbiAgICBjb25zdCBsaXN0ZW5lciA9IG5ldyBOb3RpZmljYXRpb25Qb2xsZXIodGhpcywgYnVja2V0TmFtZSwgcHJlZml4LCBzdWZmaXgsIGV2ZW50cylcbiAgICBsaXN0ZW5lci5zdGFydCgpXG4gICAgcmV0dXJuIGxpc3RlbmVyXG4gIH1cbn1cbiJdLCJtYXBwaW5ncyI6Ijs7Ozs7QUFBQSxJQUFBQSxNQUFBLEdBQUFDLHVCQUFBLENBQUFDLE9BQUE7QUFDQSxJQUFBQyxFQUFBLEdBQUFGLHVCQUFBLENBQUFDLE9BQUE7QUFFQSxJQUFBRSxJQUFBLEdBQUFILHVCQUFBLENBQUFDLE9BQUE7QUFDQSxJQUFBRyxLQUFBLEdBQUFKLHVCQUFBLENBQUFDLE9BQUE7QUFDQSxJQUFBSSxJQUFBLEdBQUFMLHVCQUFBLENBQUFDLE9BQUE7QUFDQSxJQUFBSyxNQUFBLEdBQUFOLHVCQUFBLENBQUFDLE9BQUE7QUFFQSxJQUFBTSxLQUFBLEdBQUFQLHVCQUFBLENBQUFDLE9BQUE7QUFDQSxJQUFBTyxZQUFBLEdBQUFQLE9BQUE7QUFDQSxJQUFBUSxjQUFBLEdBQUFSLE9BQUE7QUFDQSxJQUFBUyxPQUFBLEdBQUFULE9BQUE7QUFDQSxJQUFBVSxZQUFBLEdBQUFWLE9BQUE7QUFDQSxJQUFBVyxPQUFBLEdBQUFYLE9BQUE7QUFFQSxJQUFBWSxtQkFBQSxHQUFBWixPQUFBO0FBQ0EsSUFBQWEsTUFB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TUMscUJBQXFCQSxDQUFDeGlCLFVBQWtCLEVBQXFDO0lBQ2pGLElBQUksQ0FBQyxJQUFBb0YseUJBQWlCLEVBQUNwRixVQUFVLENBQUMsRUFBRTtNQUNsQyxNQUFNLElBQUlsRyxNQUFNLENBQUN1TCxzQkFBc0IsQ0FBQyx1QkFBdUIsR0FBR3JGLFVBQVUsQ0FBQztJQUMvRTtJQUNBLE1BQU1VLE1BQU0sR0FBRyxLQUFLO0lBQ3BCLE1BQU1FLEtBQUssR0FBRyxjQUFjO0lBQzVCLE1BQU1zRCxHQUFHLEdBQUcsTUFBTSxJQUFJLENBQUNWLGdCQUFnQixDQUFDO01BQUU5QyxNQUFNO01BQUVWLFVBQVU7TUFBRVk7SUFBTSxDQUFDLENBQUM7SUFDdEUsTUFBTXdELElBQUksR0FBRyxNQUFNLElBQUFvQixzQkFBWSxFQUFDdEIsR0FBRyxDQUFDO0lBQ3BDLE9BQU8sSUFBQXVlLGtDQUF1QixFQUFDcmUsSUFBSSxDQUFDO0VBQ3RDO0VBRUFzZSx3QkFBd0JBLENBQ3RCMWlCLFVBQWtCLEVBQ2xCNkssTUFBYyxFQUNkOFgsTUFBYyxFQUNkQyxNQUEyQixFQUNQO0lBQ3BCLElBQUksQ0FBQyxJQUFBeGQseUJBQWlCLEVBQUNwRixVQUFVLENBQUMsRUFBRTtNQUNsQyxNQUFNLElBQUlsRyxNQUFNLENBQUN1TCxzQkFBc0IsQ0FBQyx3QkFBd0JyRixVQUFVLEVBQUUsQ0FBQztJQUMvRTtJQUNBLElBQUksQ0FBQyxJQUFBckMsZ0JBQVEsRUFBQ2tOLE1BQU0sQ0FBQyxFQUFFO01BQ3JCLE1BQU0sSUFBSWpMLFNBQVMsQ0FBQywrQkFBK0IsQ0FBQztJQUN0RDtJQUNBLElBQUksQ0FBQyxJQUFBakMsZ0JBQVEsRUFBQ2dsQixNQUFNLENBQUMsRUFBRTtNQUNyQixNQUFNLElBQUkvaUIsU0FBUyxDQUFDLCtCQUErQixDQUFDO0lBQ3REO0lBQ0EsSUFBSSxDQUFDdVksS0FBSyxDQUFDQyxPQUFPLENBQUN3SyxNQUFNLENBQUMsRUFBRTtNQUMxQixNQUFNLElBQUloakIsU0FBUyxDQUFDLDhCQUE4QixDQUFDO0lBQ3JEO0lBQ0EsTUFBTWlqQixRQUFRLEdBQUcsSUFBSUMsZ0NBQWtCLENBQUMsSUFBSSxFQUFFOWlCLFVBQVUsRUFBRTZLLE1BQU0sRUFBRThYLE1BQU0sRUFBRUMsTUFBTSxDQUFDO0lBQ2pGQyxRQUFRLENBQUNFLEtBQUssQ0FBQyxDQUFDO0lBQ2hCLE9BQU9GLFFBQVE7RUFDakI7QUFDRjtBQUFDRyxPQUFBLENBQUF2bUIsV0FBQSxHQUFBQSxXQUFBIiwiaWdub3JlTGlzdCI6W119