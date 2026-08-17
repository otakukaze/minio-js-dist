"use strict";

Object.defineProperty(exports, "__esModule", {
  value: true
});
var crypto = _interopRequireWildcard(require("crypto"));
var fs = _interopRequireWildcard(require("fs"));
var http = _interopRequireWildcard(require("http"));
var https = _interopRequireWildcard(require("https"));
var path = _interopRequireWildcard(require("path"));
var stream = _interopRequireWildcard(require("stream"));
var async = _interopRequireWildcard(require("async"));
var _blockStream = _interopRequireDefault(require("block-stream2"));
var _browserOrNode = require("browser-or-node");
var _lodash = _interopRequireDefault(require("lodash"));
var _queryString = _interopRequireDefault(require("query-string"));
var _xml2js = _interopRequireDefault(require("xml2js"));
var _CredentialProvider = require("../CredentialProvider.js");
var errors = _interopRequireWildcard(require("../errors.js"));
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
var _xmlParser = _interopRequireWildcard(require("./xml-parser.js"));
var xmlParsers = _xmlParser;
function _interopRequireDefault(e) { return e && e.__esModule ? e : { default: e }; }
function _interopRequireWildcard(e, t) { if ("function" == typeof WeakMap) var r = new WeakMap(), n = new WeakMap(); return (_interopRequireWildcard = function (e, t) { if (!t && e && e.__esModule) return e; var o, i, f = { __proto__: null, default: e }; if (null === e || "object" != typeof e && "function" != typeof e) return f; if (o = t ? n : r) { if (o.has(e)) return o.get(e); o.set(e, f); } for (const t in e) "default" !== t && {}.hasOwnProperty.call(e, t) && ((i = (o = Object.defineProperty) && Object.getOwnPropertyDescriptor(e, t)) && (i.get || i.set) ? o(f, t, i) : f[t] = e[t]); return f; })(e, t); }
const xml = new _xml2js.default.Builder({
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
    this.reqOptions = _lodash.default.pick(options, requestOptionProperties);
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
      headers: _lodash.default.mapValues(_lodash.default.pickBy(reqOptions.headers, _helper.isDefined), v => v.toString()),
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
      query = _queryString.default.stringify(getOpts);
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
    const query = _queryString.default.stringify(statOptDef);
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
    const query = _queryString.default.stringify(queryParams);
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
    const builder = new _xml2js.default.Builder();
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
    const chunkier = new _blockStream.default({
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
          query: _queryString.default.stringify({
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
      if (_lodash.default.isEmpty(replicationConfig.role)) {
        throw new errors.InvalidArgumentError('Role cannot be empty');
      } else if (replicationConfig.role && !(0, _helper.isString)(replicationConfig.role)) {
        throw new errors.InvalidArgumentError('Invalid value for role', replicationConfig.role);
      }
      if (_lodash.default.isEmpty(replicationConfig.rules)) {
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
    const builder = new _xml2js.default.Builder({
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
    const builder = new _xml2js.default.Builder({
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
    const builder = new _xml2js.default.Builder({
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
    const builder = new _xml2js.default.Builder({
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
    const builder = new _xml2js.default.Builder({
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
    const builder = new _xml2js.default.Builder({
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
    if (!_lodash.default.isEmpty(selectOpts)) {
      if (!(0, _helper.isString)(selectOpts.expression)) {
        throw new TypeError('sqlExpression should be of type "string"');
      }
      if (!_lodash.default.isEmpty(selectOpts.inputSerialization)) {
        if (!(0, _helper.isObject)(selectOpts.inputSerialization)) {
          throw new TypeError('inputSerialization should be of type "object"');
        }
      } else {
        throw new TypeError('inputSerialization is required');
      }
      if (!_lodash.default.isEmpty(selectOpts.outputSerialization)) {
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
    const builder = new _xml2js.default.Builder({
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
    const builder = new _xml2js.default.Builder({
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
    if (_lodash.default.isEmpty(lifeCycleConfig)) {
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
    if (!_lodash.default.isEmpty(encryptionConfig) && encryptionConfig.Rule.length > 1) {
      throw new errors.InvalidArgumentError('Invalid Rule length. Only one rule is allowed.: ' + encryptionConfig.Rule);
    }
    let encryptionObj = encryptionConfig;
    if (_lodash.default.isEmpty(encryptionConfig)) {
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
    const builder = new _xml2js.default.Builder({
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
      const payload = Buffer.from(new _xml2js.default.Builder({
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
      if (!_lodash.default.isEmpty(srcConfig.VersionID)) {
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
      for (const batch of _lodash.default.chunk(uploadList, maxConcurrency)) {
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
    const query = reqParams ? _queryString.default.stringify(reqParams) : undefined;
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
    const builder = new _xml2js.default.Builder({
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
//# sourceMappingURL=data:application/json;charset=utf-8;base64,eyJ2ZXJzaW9uIjozLCJuYW1lcyI6WyJjcnlwdG8iLCJfaW50ZXJvcFJlcXVpcmVXaWxkY2FyZCIsInJlcXVpcmUiLCJmcyIsImh0dHAiLCJodHRwcyIsInBhdGgiLCJzdHJlYW0iLCJhc3luYyIsIl9ibG9ja1N0cmVhbSIsIl9pbnRlcm9wUmVxdWlyZURlZmF1bHQiLCJfYnJvd3Nlck9yTm9kZSIsIl9sb2Rhc2giLCJfcXVlcnlTdHJpbmciLCJfeG1sMmpzIiwiX0NyZWRlbnRpYWxQcm92aWRlciIsImVycm9ycyIsIl9oZWxwZXJzIiwiX25vdGlmaWNhdGlvbiIsIl9zaWduaW5nIiwiX2FzeW5jMiIsIl9jb3B5Q29uZGl0aW9ucyIsIl9leHRlbnNpb25zIiwiX2hlbHBlciIsIl9qb2luSG9zdFBvcnQiLCJfcG9zdFBvbGljeSIsIl9yZXF1ZXN0IiwiX3Jlc3BvbnNlIiwiX3MzRW5kcG9pbnRzIiwiX3htbFBhcnNlciIsInhtbFBhcnNlcnMiLCJlIiwiX19lc01vZHVsZSIsImRlZmF1bHQiLCJ0IiwiV2Vha01hcCIsInIiLCJuIiwibyIsImkiLCJmIiwiX19wcm90b19fIiwiaGFzIiwiZ2V0Iiwic2V0IiwiaGFzT3duUHJvcGVydHkiLCJjYWxsIiwiT2JqZWN0IiwiZGVmaW5lUHJvcGVydHkiLCJnZXRPd25Qcm9wZXJ0eURlc2NyaXB0b3IiLCJ4bWwiLCJ4bWwyanMiLCJCdWlsZGVyIiwicmVuZGVyT3B0cyIsInByZXR0eSIsImhlYWRsZXNzIiwiUGFja2FnZSIsInZlcnNpb24iLCJyZXF1ZXN0T3B0aW9uUHJvcGVydGllcyIsIlR5cGVkQ2xpZW50IiwicGFydFNpemUiLCJtYXhpbXVtUGFydFNpemUiLCJtYXhPYmplY3RTaXplIiwiY29uc3RydWN0b3IiLCJwYXJhbXMiLCJzZWN1cmUiLCJ1bmRlZmluZWQiLCJFcnJvciIsInVzZVNTTCIsInBvcnQiLCJpc1ZhbGlkRW5kcG9pbnQiLCJlbmRQb2ludCIsIkludmFsaWRFbmRwb2ludEVycm9yIiwiaXNWYWxpZFBvcnQiLCJJbnZhbGlkQXJndW1lbnRFcnJvciIsImlzQm9vbGVhbiIsInJlZ2lvbiIsImlzU3RyaW5nIiwiaG9zdCIsInRvTG93ZXJDYXNlIiwicHJvdG9jb2wiLCJ0cmFuc3BvcnQiLCJ0cmFuc3BvcnRBZ2VudCIsImdsb2JhbEFnZW50IiwiaXNPYmplY3QiLCJsaWJyYXJ5Q29tbWVudHMiLCJwcm9jZXNzIiwicGxhdGZvcm0iLCJhcmNoIiwibGlicmFyeUFnZW50IiwidXNlckFnZW50IiwicGF0aFN0eWxlIiwiYWNjZXNzS2V5Iiwic2VjcmV0S2V5Iiwic2Vzc2lvblRva2VuIiwiYW5vbnltb3VzIiwiY3JlZGVudGlhbHNQcm92aWRlciIsInJlZ2lvbk1hcCIsIm92ZXJSaWRlUGFydFNpemUiLCJlbmFibGVTSEEyNTYiLCJzM0FjY2VsZXJhdGVFbmRwb2ludCIsInJlcU9wdGlvbnMiLCJjbGllbnRFeHRlbnNpb25zIiwiRXh0ZW5zaW9ucyIsInJldHJ5T3B0aW9ucyIsImRpc2FibGVSZXRyeSIsImV4dGVuc2lvbnMiLCJzZXRTM1RyYW5zZmVyQWNjZWxlcmF0ZSIsInNldFJlcXVlc3RPcHRpb25zIiwib3B0aW9ucyIsIlR5cGVFcnJvciIsIl8iLCJwaWNrIiwiZ2V0QWNjZWxlcmF0ZUVuZFBvaW50SWZTZXQiLCJidWNrZXROYW1lIiwib2JqZWN0TmFtZSIsImlzRW1wdHkiLCJpbmNsdWRlcyIsInNldEFwcEluZm8iLCJhcHBOYW1lIiwiYXBwVmVyc2lvbiIsInRyaW0iLCJnZXRSZXF1ZXN0T3B0aW9ucyIsIm9wdHMiLCJtZXRob2QiLCJoZWFkZXJzIiwicXVlcnkiLCJhZ2VudCIsInZpcnR1YWxIb3N0U3R5bGUiLCJpc1ZpcnR1YWxIb3N0U3R5bGUiLCJ1cmlSZXNvdXJjZUVzY2FwZSIsImlzQW1hem9uRW5kcG9pbnQiLCJhY2NlbGVyYXRlRW5kUG9pbnQiLCJnZXRTM0VuZHBvaW50Iiwiam9pbkhvc3RQb3J0IiwiayIsInYiLCJlbnRyaWVzIiwiYXNzaWduIiwibWFwVmFsdWVzIiwicGlja0J5IiwiaXNEZWZpbmVkIiwidG9TdHJpbmciLCJzZXRDcmVkZW50aWFsc1Byb3ZpZGVyIiwiQ3JlZGVudGlhbFByb3ZpZGVyIiwiY2hlY2tBbmRSZWZyZXNoQ3JlZHMiLCJjcmVkZW50aWFsc0NvbmYiLCJnZXRDcmVkZW50aWFscyIsImdldEFjY2Vzc0tleSIsImdldFNlY3JldEtleSIsImdldFNlc3Npb25Ub2tlbiIsImNhdXNlIiwibG9nSFRUUCIsInJlc3BvbnNlIiwiZXJyIiwibG9nU3RyZWFtIiwiaXNSZWFkYWJsZVN0cmVhbSIsImxvZ0hlYWRlcnMiLCJmb3JFYWNoIiwicmVkYWN0b3IiLCJSZWdFeHAiLCJyZXBsYWNlIiwid3JpdGUiLCJzdGF0dXNDb2RlIiwiZXJySlNPTiIsIkpTT04iLCJzdHJpbmdpZnkiLCJ0cmFjZU9uIiwic3Rkb3V0IiwidHJhY2VPZmYiLCJtYWtlUmVxdWVzdEFzeW5jIiwicGF5bG9hZCIsImV4cGVjdGVkQ29kZXMiLCJpc051bWJlciIsImxlbmd0aCIsInNoYTI1NnN1bSIsInRvU2hhMjU2IiwibWFrZVJlcXVlc3RTdHJlYW1Bc3luYyIsIm1ha2VSZXF1ZXN0QXN5bmNPbWl0Iiwic3RhdHVzQ29kZXMiLCJyZXMiLCJkcmFpblJlc3BvbnNlIiwiYm9keSIsIkJ1ZmZlciIsImlzQnVmZmVyIiwiZ2V0QnVja2V0UmVnaW9uQXN5bmMiLCJkYXRlIiwiRGF0ZSIsIm1ha2VEYXRlTG9uZyIsImF1dGhvcml6YXRpb24iLCJzaWduVjQiLCJsb2ciLCJtc2ciLCJyZXF1ZXN0V2l0aFJldHJ5IiwibWF4aW11bVJldHJ5Q291bnQiLCJiYXNlRGVsYXlNcyIsIm1heGltdW1EZWxheU1zIiwicGFyc2VSZXNwb25zZUVycm9yIiwiaXNWYWxpZEJ1Y2tldE5hbWUiLCJJbnZhbGlkQnVja2V0TmFtZUVycm9yIiwiY2FjaGVkIiwiZXh0cmFjdFJlZ2lvbkFzeW5jIiwicmVhZEFzU3RyaW5nIiwicGFyc2VCdWNrZXRSZWdpb24iLCJERUZBVUxUX1JFR0lPTiIsImlzQnJvd3NlciIsIlMzRXJyb3IiLCJlcnJDb2RlIiwiY29kZSIsImVyclJlZ2lvbiIsIm5hbWUiLCJSZWdpb24iLCJtYWtlUmVxdWVzdCIsInJldHVyblJlc3BvbnNlIiwiY2IiLCJwcm9tIiwidGhlbiIsInJlc3VsdCIsIm1ha2VSZXF1ZXN0U3RyZWFtIiwiZXhlY3V0b3IiLCJnZXRCdWNrZXRSZWdpb24iLCJtYWtlQnVja2V0IiwibWFrZU9wdHMiLCJidWlsZE9iamVjdCIsIkNyZWF0ZUJ1Y2tldENvbmZpZ3VyYXRpb24iLCIkIiwieG1sbnMiLCJMb2NhdGlvbkNvbnN0cmFpbnQiLCJPYmplY3RMb2NraW5nIiwiZmluYWxSZWdpb24iLCJyZXF1ZXN0T3B0IiwiYnVja2V0RXhpc3RzIiwicmVtb3ZlQnVja2V0IiwiZ2V0T2JqZWN0IiwiZ2V0T3B0cyIsImlzVmFsaWRPYmplY3ROYW1lIiwiSW52YWxpZE9iamVjdE5hbWVFcnJvciIsImdldFBhcnRpYWxPYmplY3QiLCJvZmZzZXQiLCJyYW5nZSIsInNzZUhlYWRlcnMiLCJTU0VDdXN0b21lckFsZ29yaXRobSIsIlNTRUN1c3RvbWVyS2V5IiwiU1NFQ3VzdG9tZXJLZXlNRDUiLCJxcyIsInByZXBlbmRYQU1aTWV0YSIsImV4cGVjdGVkU3RhdHVzQ29kZXMiLCJwdXNoIiwiZkdldE9iamVjdCIsImZpbGVQYXRoIiwiZG93bmxvYWRUb1RtcEZpbGUiLCJwYXJ0RmlsZVN0cmVhbSIsIm9ialN0YXQiLCJzdGF0T2JqZWN0IiwiZW5jb2RlZEV0YWciLCJmcm9tIiwiZXRhZyIsInBhcnRGaWxlIiwiZnNwIiwibWtkaXIiLCJkaXJuYW1lIiwicmVjdXJzaXZlIiwic3RhdHMiLCJzdGF0Iiwic2l6ZSIsImNyZWF0ZVdyaXRlU3RyZWFtIiwiZmxhZ3MiLCJkb3dubG9hZFN0cmVhbSIsInN0cmVhbVByb21pc2UiLCJwaXBlbGluZSIsInJlbmFtZSIsInN0YXRPcHRzIiwic3RhdE9wdERlZiIsInBhcnNlSW50IiwibWV0YURhdGEiLCJleHRyYWN0TWV0YWRhdGEiLCJsYXN0TW9kaWZpZWQiLCJ2ZXJzaW9uSWQiLCJnZXRWZXJzaW9uSWQiLCJzYW5pdGl6ZUVUYWciLCJyZW1vdmVPYmplY3QiLCJyZW1vdmVPcHRzIiwiZ292ZXJuYW5jZUJ5cGFzcyIsImZvcmNlRGVsZXRlIiwicXVlcnlQYXJhbXMiLCJsaXN0SW5jb21wbGV0ZVVwbG9hZHMiLCJidWNrZXQiLCJwcmVmaXgiLCJpc1ZhbGlkUHJlZml4IiwiSW52YWxpZFByZWZpeEVycm9yIiwiZGVsaW1pdGVyIiwia2V5TWFya2VyIiwidXBsb2FkSWRNYXJrZXIiLCJ1cGxvYWRzIiwiZW5kZWQiLCJyZWFkU3RyZWFtIiwiUmVhZGFibGUiLCJvYmplY3RNb2RlIiwiX3JlYWQiLCJzaGlmdCIsImxpc3RJbmNvbXBsZXRlVXBsb2Fkc1F1ZXJ5IiwicHJlZml4ZXMiLCJlYWNoU2VyaWVzIiwidXBsb2FkIiwibGlzdFBhcnRzIiwia2V5IiwidXBsb2FkSWQiLCJwYXJ0cyIsInJlZHVjZSIsImFjYyIsIml0ZW0iLCJlbWl0IiwiaXNUcnVuY2F0ZWQiLCJuZXh0S2V5TWFya2VyIiwibmV4dFVwbG9hZElkTWFya2VyIiwicXVlcmllcyIsInVyaUVzY2FwZSIsIm1heFVwbG9hZHMiLCJzb3J0IiwidW5zaGlmdCIsImpvaW4iLCJwYXJzZUxpc3RNdWx0aXBhcnQiLCJpbml0aWF0ZU5ld011bHRpcGFydFVwbG9hZCIsInJlYWRBc0J1ZmZlciIsInBhcnNlSW5pdGlhdGVNdWx0aXBhcnQiLCJhYm9ydE11bHRpcGFydFVwbG9hZCIsInJlcXVlc3RPcHRpb25zIiwiZmluZFVwbG9hZElkIiwiX2xhdGVzdFVwbG9hZCIsImxhdGVzdFVwbG9hZCIsImluaXRpYXRlZCIsImdldFRpbWUiLCJjb21wbGV0ZU11bHRpcGFydFVwbG9hZCIsImV0YWdzIiwiYnVpbGRlciIsIkNvbXBsZXRlTXVsdGlwYXJ0VXBsb2FkIiwiUGFydCIsIm1hcCIsIlBhcnROdW1iZXIiLCJwYXJ0IiwiRVRhZyIsInBhcnNlQ29tcGxldGVNdWx0aXBhcnQiLCJlcnJNZXNzYWdlIiwibWFya2VyIiwibGlzdFBhcnRzUXVlcnkiLCJwYXJzZUxpc3RQYXJ0cyIsImxpc3RCdWNrZXRzIiwicmVnaW9uQ29uZiIsImh0dHBSZXMiLCJ4bWxSZXN1bHQiLCJwYXJzZUxpc3RCdWNrZXQiLCJjYWxjdWxhdGVQYXJ0U2l6ZSIsImZQdXRPYmplY3QiLCJpbnNlcnRDb250ZW50VHlwZSIsInB1dE9iamVjdCIsImNyZWF0ZVJlYWRTdHJlYW0iLCJyZWFkYWJsZVN0cmVhbSIsInN0YXRTaXplIiwiZ2V0Q29udGVudExlbmd0aCIsInVwbG9hZEJ1ZmZlciIsImJ1ZiIsInVwbG9hZFN0cmVhbSIsIm1kNXN1bSIsImhhc2hCaW5hcnkiLCJvbGRQYXJ0cyIsImVUYWdzIiwicHJldmlvdXNVcGxvYWRJZCIsIm9sZFRhZ3MiLCJjaHVua2llciIsIkJsb2NrU3RyZWFtMiIsInplcm9QYWRkaW5nIiwiUHJvbWlzZSIsImFsbCIsInJlc29sdmUiLCJyZWplY3QiLCJwaXBlIiwib24iLCJwYXJ0TnVtYmVyIiwiY2h1bmsiLCJtZDUiLCJjcmVhdGVIYXNoIiwidXBkYXRlIiwiZGlnZXN0Iiwib2xkUGFydCIsInJlbW92ZUJ1Y2tldFJlcGxpY2F0aW9uIiwic2V0QnVja2V0UmVwbGljYXRpb24iLCJyZXBsaWNhdGlvbkNvbmZpZyIsInJvbGUiLCJydWxlcyIsInJlcGxpY2F0aW9uUGFyYW1zQ29uZmlnIiwiUmVwbGljYXRpb25Db25maWd1cmF0aW9uIiwiUm9sZSIsIlJ1bGUiLCJ0b01kNSIsImdldEJ1Y2tldFJlcGxpY2F0aW9uIiwicGFyc2VSZXBsaWNhdGlvbkNvbmZpZyIsImdldE9iamVjdExlZ2FsSG9sZCIsImtleXMiLCJzdHJSZXMiLCJwYXJzZU9iamVjdExlZ2FsSG9sZENvbmZpZyIsInNldE9iamVjdExlZ2FsSG9sZCIsInNldE9wdHMiLCJzdGF0dXMiLCJMRUdBTF9IT0xEX1NUQVRVUyIsIkVOQUJMRUQiLCJESVNBQkxFRCIsImNvbmZpZyIsIlN0YXR1cyIsInJvb3ROYW1lIiwiZ2V0QnVja2V0VGFnZ2luZyIsInBhcnNlVGFnZ2luZyIsImdldE9iamVjdFRhZ2dpbmciLCJzZXRCdWNrZXRQb2xpY3kiLCJwb2xpY3kiLCJJbnZhbGlkQnVja2V0UG9saWN5RXJyb3IiLCJnZXRCdWNrZXRQb2xpY3kiLCJwdXRPYmplY3RSZXRlbnRpb24iLCJyZXRlbnRpb25PcHRzIiwibW9kZSIsIlJFVEVOVElPTl9NT0RFUyIsIkNPTVBMSUFOQ0UiLCJHT1ZFUk5BTkNFIiwicmV0YWluVW50aWxEYXRlIiwiTW9kZSIsIlJldGFpblVudGlsRGF0ZSIsImdldE9iamVjdExvY2tDb25maWciLCJwYXJzZU9iamVjdExvY2tDb25maWciLCJzZXRPYmplY3RMb2NrQ29uZmlnIiwibG9ja0NvbmZpZ09wdHMiLCJyZXRlbnRpb25Nb2RlcyIsInZhbGlkVW5pdHMiLCJSRVRFTlRJT05fVkFMSURJVFlfVU5JVFMiLCJEQVlTIiwiWUVBUlMiLCJ1bml0IiwidmFsaWRpdHkiLCJPYmplY3RMb2NrRW5hYmxlZCIsImNvbmZpZ0tleXMiLCJpc0FsbEtleXNTZXQiLCJldmVyeSIsImxjayIsIkRlZmF1bHRSZXRlbnRpb24iLCJEYXlzIiwiWWVhcnMiLCJnZXRCdWNrZXRWZXJzaW9uaW5nIiwicGFyc2VCdWNrZXRWZXJzaW9uaW5nQ29uZmlnIiwic2V0QnVja2V0VmVyc2lvbmluZyIsInZlcnNpb25Db25maWciLCJzZXRUYWdnaW5nIiwidGFnZ2luZ1BhcmFtcyIsInRhZ3MiLCJwdXRPcHRzIiwidGFnc0xpc3QiLCJ2YWx1ZSIsIktleSIsIlZhbHVlIiwidGFnZ2luZ0NvbmZpZyIsIlRhZ2dpbmciLCJUYWdTZXQiLCJUYWciLCJwYXlsb2FkQnVmIiwicmVtb3ZlVGFnZ2luZyIsInNldEJ1Y2tldFRhZ2dpbmciLCJpc1BsYWluT2JqZWN0IiwicmVtb3ZlQnVja2V0VGFnZ2luZyIsInNldE9iamVjdFRhZ2dpbmciLCJyZW1vdmVPYmplY3RUYWdnaW5nIiwic2VsZWN0T2JqZWN0Q29udGVudCIsInNlbGVjdE9wdHMiLCJleHByZXNzaW9uIiwiaW5wdXRTZXJpYWxpemF0aW9uIiwib3V0cHV0U2VyaWFsaXphdGlvbiIsIkV4cHJlc3Npb24iLCJFeHByZXNzaW9uVHlwZSIsImV4cHJlc3Npb25UeXBlIiwiSW5wdXRTZXJpYWxpemF0aW9uIiwiT3V0cHV0U2VyaWFsaXphdGlvbiIsInJlcXVlc3RQcm9ncmVzcyIsIlJlcXVlc3RQcm9ncmVzcyIsInNjYW5SYW5nZSIsIlNjYW5SYW5nZSIsInBhcnNlU2VsZWN0T2JqZWN0Q29udGVudFJlc3BvbnNlIiwiYXBwbHlCdWNrZXRMaWZlY3ljbGUiLCJwb2xpY3lDb25maWciLCJyZW1vdmVCdWNrZXRMaWZlY3ljbGUiLCJzZXRCdWNrZXRMaWZlY3ljbGUiLCJsaWZlQ3ljbGVDb25maWciLCJnZXRCdWNrZXRMaWZlY3ljbGUiLCJwYXJzZUxpZmVjeWNsZUNvbmZpZyIsInNldEJ1Y2tldEVuY3J5cHRpb24iLCJlbmNyeXB0aW9uQ29uZmlnIiwiZW5jcnlwdGlvbk9iaiIsIkFwcGx5U2VydmVyU2lkZUVuY3J5cHRpb25CeURlZmF1bHQiLCJTU0VBbGdvcml0aG0iLCJnZXRCdWNrZXRFbmNyeXB0aW9uIiwicGFyc2VCdWNrZXRFbmNyeXB0aW9uQ29uZmlnIiwicmVtb3ZlQnVja2V0RW5jcnlwdGlvbiIsImdldE9iamVjdFJldGVudGlvbiIsInBhcnNlT2JqZWN0UmV0ZW50aW9uQ29uZmlnIiwicmVtb3ZlT2JqZWN0cyIsIm9iamVjdHNMaXN0IiwiQXJyYXkiLCJpc0FycmF5IiwicnVuRGVsZXRlT2JqZWN0cyIsImJhdGNoIiwiZGVsT2JqZWN0cyIsIlZlcnNpb25JZCIsInJlbU9iamVjdHMiLCJEZWxldGUiLCJRdWlldCIsInJlbW92ZU9iamVjdHNQYXJzZXIiLCJtYXhFbnRyaWVzIiwiYmF0Y2hlcyIsInNsaWNlIiwiYmF0Y2hSZXN1bHRzIiwiZmxhdCIsInJlbW92ZUluY29tcGxldGVVcGxvYWQiLCJJc1ZhbGlkQnVja2V0TmFtZUVycm9yIiwicmVtb3ZlVXBsb2FkSWQiLCJjb3B5T2JqZWN0VjEiLCJ0YXJnZXRCdWNrZXROYW1lIiwidGFyZ2V0T2JqZWN0TmFtZSIsInNvdXJjZUJ1Y2tldE5hbWVBbmRPYmplY3ROYW1lIiwiY29uZGl0aW9ucyIsIkNvcHlDb25kaXRpb25zIiwibW9kaWZpZWQiLCJ1bm1vZGlmaWVkIiwibWF0Y2hFVGFnIiwibWF0Y2hFVGFnRXhjZXB0IiwicGFyc2VDb3B5T2JqZWN0IiwiY29weU9iamVjdFYyIiwic291cmNlQ29uZmlnIiwiZGVzdENvbmZpZyIsIkNvcHlTb3VyY2VPcHRpb25zIiwiQ29weURlc3RpbmF0aW9uT3B0aW9ucyIsInZhbGlkYXRlIiwiZ2V0SGVhZGVycyIsIkJ1Y2tldCIsImNvcHlSZXMiLCJyZXNIZWFkZXJzIiwic2l6ZUhlYWRlclZhbHVlIiwiTGFzdE1vZGlmaWVkIiwiTWV0YURhdGEiLCJTb3VyY2VWZXJzaW9uSWQiLCJnZXRTb3VyY2VWZXJzaW9uSWQiLCJFdGFnIiwiU2l6ZSIsImNvcHlPYmplY3QiLCJhbGxBcmdzIiwic291cmNlIiwiZGVzdCIsInVwbG9hZFBhcnQiLCJwYXJ0Q29uZmlnIiwidXBsb2FkSUQiLCJwYXJ0UmVzIiwidXBsb2FkUGFydFBhcnNlciIsInBhcnRFdGFnVmFsIiwiY29tcG9zZU9iamVjdCIsImRlc3RPYmpDb25maWciLCJzb3VyY2VPYmpMaXN0IiwibWF4Q29uY3VycmVuY3kiLCJzb3VyY2VGaWxlc0xlbmd0aCIsIlBBUlRfQ09OU1RSQUlOVFMiLCJNQVhfUEFSVFNfQ09VTlQiLCJzT2JqIiwiZ2V0U3RhdE9wdGlvbnMiLCJzcmNDb25maWciLCJWZXJzaW9uSUQiLCJzcmNPYmplY3RTaXplcyIsInRvdGFsU2l6ZSIsInRvdGFsUGFydHMiLCJzb3VyY2VPYmpTdGF0cyIsInNyY0l0ZW0iLCJzcmNPYmplY3RJbmZvcyIsInZhbGlkYXRlZFN0YXRzIiwicmVzSXRlbVN0YXQiLCJpbmRleCIsInNyY0NvcHlTaXplIiwiTWF0Y2hSYW5nZSIsInNyY1N0YXJ0IiwiU3RhcnQiLCJzcmNFbmQiLCJFbmQiLCJBQlNfTUlOX1BBUlRfU0laRSIsIk1BWF9NVUxUSVBBUlRfUFVUX09CSkVDVF9TSVpFIiwicGFydHNSZXF1aXJlZCIsIk1BWF9QQVJUX1NJWkUiLCJNYXRjaEVUYWciLCJzcGxpdFBhcnRTaXplTGlzdCIsImlkeCIsImNhbGN1bGF0ZUV2ZW5TcGxpdHMiLCJnZXRVcGxvYWRQYXJ0Q29uZmlnTGlzdCIsInVwbG9hZFBhcnRDb25maWdMaXN0Iiwic3BsaXRTaXplIiwic3BsaXRJbmRleCIsInN0YXJ0SW5kZXgiLCJzdGFydElkeCIsImVuZEluZGV4IiwiZW5kSWR4IiwicGFydEluZGV4IiwidG90YWxVcGxvYWRzIiwic3BsaXRTdGFydCIsInVwbGRDdHJJZHgiLCJzcGxpdEVuZCIsInVwbG9hZFBhcnRDb25maWciLCJ1cGxvYWRBbGxQYXJ0cyIsInVwbG9hZExpc3QiLCJwYXJ0VXBsb2FkcyIsInBlcmZvcm1VcGxvYWRQYXJ0cyIsInBhcnRzUmVzIiwicGFydENvcHkiLCJuZXdVcGxvYWRIZWFkZXJzIiwicGFydHNEb25lIiwicHJlc2lnbmVkVXJsIiwiZXhwaXJlcyIsInJlcVBhcmFtcyIsInJlcXVlc3REYXRlIiwiX3JlcXVlc3REYXRlIiwiQW5vbnltb3VzUmVxdWVzdEVycm9yIiwiUFJFU0lHTl9FWFBJUllfREFZU19NQVgiLCJpc05hTiIsInByZXNpZ25TaWduYXR1cmVWNCIsInByZXNpZ25lZEdldE9iamVjdCIsInJlc3BIZWFkZXJzIiwidmFsaWRSZXNwSGVhZGVycyIsImhlYWRlciIsInByZXNpZ25lZFB1dE9iamVjdCIsIm5ld1Bvc3RQb2xpY3kiLCJQb3N0UG9saWN5IiwicHJlc2lnbmVkUG9zdFBvbGljeSIsInBvc3RQb2xpY3kiLCJmb3JtRGF0YSIsImRhdGVTdHIiLCJleHBpcmF0aW9uIiwic2V0U2Vjb25kcyIsInNldEV4cGlyZXMiLCJnZXRTY29wZSIsInBvbGljeUJhc2U2NCIsInBvc3RQcmVzaWduU2lnbmF0dXJlVjQiLCJwb3J0U3RyIiwidXJsU3RyIiwicG9zdFVSTCIsImxpc3RPYmplY3RzUXVlcnkiLCJsaXN0UXVlcnlPcHRzIiwiRGVsaW1pdGVyIiwiTWF4S2V5cyIsIkluY2x1ZGVWZXJzaW9uIiwidmVyc2lvbklkTWFya2VyIiwibGlzdFFyeUxpc3QiLCJwYXJzZUxpc3RPYmplY3RzIiwibGlzdE9iamVjdHMiLCJsaXN0T3B0cyIsIm9iamVjdHMiLCJuZXh0TWFya2VyIiwibGlzdE9iamVjdHNWMlF1ZXJ5IiwiY29udGludWF0aW9uVG9rZW4iLCJtYXhLZXlzIiwic3RhcnRBZnRlciIsInBhcnNlTGlzdE9iamVjdHNWMiIsImxpc3RPYmplY3RzVjIiLCJwcmVmaXhTdHIiLCJzdGFydEFmdGVyU3RyIiwibmV4dENvbnRpbnVhdGlvblRva2VuIiwic2V0QnVja2V0Tm90aWZpY2F0aW9uIiwicmVtb3ZlQWxsQnVja2V0Tm90aWZpY2F0aW9uIiwiTm90aWZpY2F0aW9uQ29uZmlnIiwiZ2V0QnVja2V0Tm90aWZpY2F0aW9uIiwicGFyc2VCdWNrZXROb3RpZmljYXRpb24iLCJsaXN0ZW5CdWNrZXROb3RpZmljYXRpb24iLCJzdWZmaXgiLCJldmVudHMiLCJsaXN0ZW5lciIsIk5vdGlmaWNhdGlvblBvbGxlciIsInN0YXJ0IiwiZXhwb3J0cyJdLCJzb3VyY2VzIjpbImNsaWVudC50cyJdLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQgKiBhcyBjcnlwdG8gZnJvbSAnbm9kZTpjcnlwdG8nXG5pbXBvcnQgKiBhcyBmcyBmcm9tICdub2RlOmZzJ1xuaW1wb3J0IHR5cGUgeyBJbmNvbWluZ0h0dHBIZWFkZXJzIH0gZnJvbSAnbm9kZTpodHRwJ1xuaW1wb3J0ICogYXMgaHR0cCBmcm9tICdub2RlOmh0dHAnXG5pbXBvcnQgKiBhcyBodHRwcyBmcm9tICdub2RlOmh0dHBzJ1xuaW1wb3J0ICogYXMgcGF0aCBmcm9tICdub2RlOnBhdGgnXG5pbXBvcnQgKiBhcyBzdHJlYW0gZnJvbSAnbm9kZTpzdHJlYW0nXG5cbmltcG9ydCAqIGFzIGFzeW5jIGZyb20gJ2FzeW5jJ1xuaW1wb3J0IEJsb2NrU3RyZWFtMiBmcm9tICdibG9jay1zdHJlYW0yJ1xuaW1wb3J0IHsgaXNCcm93c2VyIH0gZnJvbSAnYnJvd3Nlci1vci1ub2RlJ1xuaW1wb3J0IF8gZnJvbSAnbG9kYXNoJ1xuaW1wb3J0IHFzIGZyb20gJ3F1ZXJ5LXN0cmluZydcbmltcG9ydCB4bWwyanMgZnJvbSAneG1sMmpzJ1xuXG5pbXBvcnQgeyBDcmVkZW50aWFsUHJvdmlkZXIgfSBmcm9tICcuLi9DcmVkZW50aWFsUHJvdmlkZXIudHMnXG5pbXBvcnQgKiBhcyBlcnJvcnMgZnJvbSAnLi4vZXJyb3JzLnRzJ1xuaW1wb3J0IHR5cGUgeyBTZWxlY3RSZXN1bHRzIH0gZnJvbSAnLi4vaGVscGVycy50cydcbmltcG9ydCB7XG4gIENvcHlEZXN0aW5hdGlvbk9wdGlvbnMsXG4gIENvcHlTb3VyY2VPcHRpb25zLFxuICBERUZBVUxUX1JFR0lPTixcbiAgTEVHQUxfSE9MRF9TVEFUVVMsXG4gIFBSRVNJR05fRVhQSVJZX0RBWVNfTUFYLFxuICBSRVRFTlRJT05fTU9ERVMsXG4gIFJFVEVOVElPTl9WQUxJRElUWV9VTklUUyxcbn0gZnJvbSAnLi4vaGVscGVycy50cydcbmltcG9ydCB0eXBlIHsgTm90aWZpY2F0aW9uRXZlbnQgfSBmcm9tICcuLi9ub3RpZmljYXRpb24udHMnXG5pbXBvcnQgeyBOb3RpZmljYXRpb25Db25maWcsIE5vdGlmaWNhdGlvblBvbGxlciB9IGZyb20gJy4uL25vdGlmaWNhdGlvbi50cydcbmltcG9ydCB7IHBvc3RQcmVzaWduU2lnbmF0dXJlVjQsIHByZXNpZ25TaWduYXR1cmVWNCwgc2lnblY0IH0gZnJvbSAnLi4vc2lnbmluZy50cydcbmltcG9ydCB7IGZzcCwgc3RyZWFtUHJvbWlzZSB9IGZyb20gJy4vYXN5bmMudHMnXG5pbXBvcnQgeyBDb3B5Q29uZGl0aW9ucyB9IGZyb20gJy4vY29weS1jb25kaXRpb25zLnRzJ1xuaW1wb3J0IHsgRXh0ZW5zaW9ucyB9IGZyb20gJy4vZXh0ZW5zaW9ucy50cydcbmltcG9ydCB7XG4gIGNhbGN1bGF0ZUV2ZW5TcGxpdHMsXG4gIGV4dHJhY3RNZXRhZGF0YSxcbiAgZ2V0Q29udGVudExlbmd0aCxcbiAgZ2V0U2NvcGUsXG4gIGdldFNvdXJjZVZlcnNpb25JZCxcbiAgZ2V0VmVyc2lvbklkLFxuICBoYXNoQmluYXJ5LFxuICBpbnNlcnRDb250ZW50VHlwZSxcbiAgaXNBbWF6b25FbmRwb2ludCxcbiAgaXNCb29sZWFuLFxuICBpc0RlZmluZWQsXG4gIGlzRW1wdHksXG4gIGlzTnVtYmVyLFxuICBpc09iamVjdCxcbiAgaXNQbGFpbk9iamVjdCxcbiAgaXNSZWFkYWJsZVN0cmVhbSxcbiAgaXNTdHJpbmcsXG4gIGlzVmFsaWRCdWNrZXROYW1lLFxuICBpc1ZhbGlkRW5kcG9pbnQsXG4gIGlzVmFsaWRPYmplY3ROYW1lLFxuICBpc1ZhbGlkUG9ydCxcbiAgaXNWYWxpZFByZWZpeCxcbiAgaXNWaXJ0dWFsSG9zdFN0eWxlLFxuICBtYWtlRGF0ZUxvbmcsXG4gIFBBUlRfQ09OU1RSQUlOVFMsXG4gIHBhcnRzUmVxdWlyZWQsXG4gIHByZXBlbmRYQU1aTWV0YSxcbiAgcmVhZGFibGVTdHJlYW0sXG4gIHNhbml0aXplRVRhZyxcbiAgdG9NZDUsXG4gIHRvU2hhMjU2LFxuICB1cmlFc2NhcGUsXG4gIHVyaVJlc291cmNlRXNjYXBlLFxufSBmcm9tICcuL2hlbHBlci50cydcbmltcG9ydCB7IGpvaW5Ib3N0UG9ydCB9IGZyb20gJy4vam9pbi1ob3N0LXBvcnQudHMnXG5pbXBvcnQgeyBQb3N0UG9saWN5IH0gZnJvbSAnLi9wb3N0LXBvbGljeS50cydcbmltcG9ydCB7IHJlcXVlc3RXaXRoUmV0cnkgfSBmcm9tICcuL3JlcXVlc3QudHMnXG5pbXBvcnQgeyBkcmFpblJlc3BvbnNlLCByZWFkQXNCdWZmZXIsIHJlYWRBc1N0cmluZyB9IGZyb20gJy4vcmVzcG9uc2UudHMnXG5pbXBvcnQgdHlwZSB7IFJlZ2lvbiB9IGZyb20gJy4vczMtZW5kcG9pbnRzLnRzJ1xuaW1wb3J0IHsgZ2V0UzNFbmRwb2ludCB9IGZyb20gJy4vczMtZW5kcG9pbnRzLnRzJ1xuaW1wb3J0IHR5cGUge1xuICBCaW5hcnksXG4gIEJ1Y2tldEl0ZW0sXG4gIEJ1Y2tldEl0ZW1Gcm9tTGlzdCxcbiAgQnVja2V0SXRlbVN0YXQsXG4gIEJ1Y2tldFN0cmVhbSxcbiAgQnVja2V0VmVyc2lvbmluZ0NvbmZpZ3VyYXRpb24sXG4gIENvcHlPYmplY3RQYXJhbXMsXG4gIENvcHlPYmplY3RSZXN1bHQsXG4gIENvcHlPYmplY3RSZXN1bHRWMixcbiAgRW5jcnlwdGlvbkNvbmZpZyxcbiAgR2V0T2JqZWN0TGVnYWxIb2xkT3B0aW9ucyxcbiAgR2V0T2JqZWN0T3B0cyxcbiAgR2V0T2JqZWN0UmV0ZW50aW9uT3B0cyxcbiAgSW5jb21wbGV0ZVVwbG9hZGVkQnVja2V0SXRlbSxcbiAgSVJlcXVlc3QsXG4gIEl0ZW1CdWNrZXRNZXRhZGF0YSxcbiAgTGlmZWN5Y2xlQ29uZmlnLFxuICBMaWZlQ3ljbGVDb25maWdQYXJhbSxcbiAgTGlzdE9iamVjdFF1ZXJ5T3B0cyxcbiAgTGlzdE9iamVjdFF1ZXJ5UmVzLFxuICBMaXN0T2JqZWN0VjJSZXMsXG4gIE5vdGlmaWNhdGlvbkNvbmZpZ1Jlc3VsdCxcbiAgT2JqZWN0SW5mbyxcbiAgT2JqZWN0TG9ja0NvbmZpZ1BhcmFtLFxuICBPYmplY3RMb2NrSW5mbyxcbiAgT2JqZWN0TWV0YURhdGEsXG4gIE9iamVjdFJldGVudGlvbkluZm8sXG4gIFBvc3RQb2xpY3lSZXN1bHQsXG4gIFByZVNpZ25SZXF1ZXN0UGFyYW1zLFxuICBQdXRPYmplY3RMZWdhbEhvbGRPcHRpb25zLFxuICBQdXRUYWdnaW5nUGFyYW1zLFxuICBSZW1vdmVPYmplY3RzUGFyYW0sXG4gIFJlbW92ZU9iamVjdHNSZXF1ZXN0RW50cnksXG4gIFJlbW92ZU9iamVjdHNSZXNwb25zZSxcbiAgUmVtb3ZlVGFnZ2luZ1BhcmFtcyxcbiAgUmVwbGljYXRpb25Db25maWcsXG4gIFJlcGxpY2F0aW9uQ29uZmlnT3B0cyxcbiAgUmVxdWVzdEhlYWRlcnMsXG4gIFJlc3BvbnNlSGVhZGVyLFxuICBSZXN1bHRDYWxsYmFjayxcbiAgUmV0ZW50aW9uLFxuICBTZWxlY3RPcHRpb25zLFxuICBTdGF0T2JqZWN0T3B0cyxcbiAgVGFnLFxuICBUYWdnaW5nT3B0cyxcbiAgVGFncyxcbiAgVHJhbnNwb3J0LFxuICBVcGxvYWRlZE9iamVjdEluZm8sXG4gIFVwbG9hZFBhcnRDb25maWcsXG59IGZyb20gJy4vdHlwZS50cydcbmltcG9ydCB0eXBlIHsgTGlzdE11bHRpcGFydFJlc3VsdCwgVXBsb2FkZWRQYXJ0IH0gZnJvbSAnLi94bWwtcGFyc2VyLnRzJ1xuaW1wb3J0IHtcbiAgcGFyc2VCdWNrZXROb3RpZmljYXRpb24sXG4gIHBhcnNlQ29tcGxldGVNdWx0aXBhcnQsXG4gIHBhcnNlSW5pdGlhdGVNdWx0aXBhcnQsXG4gIHBhcnNlTGlzdE9iamVjdHMsXG4gIHBhcnNlTGlzdE9iamVjdHNWMixcbiAgcGFyc2VPYmplY3RMZWdhbEhvbGRDb25maWcsXG4gIHBhcnNlU2VsZWN0T2JqZWN0Q29udGVudFJlc3BvbnNlLFxuICB1cGxvYWRQYXJ0UGFyc2VyLFxufSBmcm9tICcuL3htbC1wYXJzZXIudHMnXG5pbXBvcnQgKiBhcyB4bWxQYXJzZXJzIGZyb20gJy4veG1sLXBhcnNlci50cydcblxuY29uc3QgeG1sID0gbmV3IHhtbDJqcy5CdWlsZGVyKHsgcmVuZGVyT3B0czogeyBwcmV0dHk6IGZhbHNlIH0sIGhlYWRsZXNzOiB0cnVlIH0pXG5cbi8vIHdpbGwgYmUgcmVwbGFjZWQgYnkgYnVuZGxlci5cbmNvbnN0IFBhY2thZ2UgPSB7IHZlcnNpb246IHByb2Nlc3MuZW52Lk1JTklPX0pTX1BBQ0tBR0VfVkVSU0lPTiB8fCAnZGV2ZWxvcG1lbnQnIH1cblxuY29uc3QgcmVxdWVzdE9wdGlvblByb3BlcnRpZXMgPSBbXG4gICdhZ2VudCcsXG4gICdjYScsXG4gICdjZXJ0JyxcbiAgJ2NpcGhlcnMnLFxuICAnY2xpZW50Q2VydEVuZ2luZScsXG4gICdjcmwnLFxuICAnZGhwYXJhbScsXG4gICdlY2RoQ3VydmUnLFxuICAnZmFtaWx5JyxcbiAgJ2hvbm9yQ2lwaGVyT3JkZXInLFxuICAna2V5JyxcbiAgJ3Bhc3NwaHJhc2UnLFxuICAncGZ4JyxcbiAgJ3JlamVjdFVuYXV0aG9yaXplZCcsXG4gICdzZWN1cmVPcHRpb25zJyxcbiAgJ3NlY3VyZVByb3RvY29sJyxcbiAgJ3NlcnZlcm5hbWUnLFxuICAnc2Vzc2lvbklkQ29udGV4dCcsXG5dIGFzIGNvbnN0XG5cbmV4cG9ydCBpbnRlcmZhY2UgUmV0cnlPcHRpb25zIHtcbiAgLyoqXG4gICAqIElmIHRoaXMgc2V0IHRvIHRydWUsIGl0IHdpbGwgdGFrZSBwcmVjZWRlbmNlIG92ZXIgYWxsIG90aGVyIHJldHJ5IG9wdGlvbnMuXG4gICAqIEBkZWZhdWx0IGZhbHNlXG4gICAqL1xuICBkaXNhYmxlUmV0cnk/OiBib29sZWFuXG4gIC8qKlxuICAgKiBUaGUgbWF4aW11bSBhbW91bnQgb2YgcmV0cmllcyBmb3IgYSByZXF1ZXN0LlxuICAgKiBAZGVmYXVsdCAxXG4gICAqL1xuICBtYXhpbXVtUmV0cnlDb3VudD86IG51bWJlclxuICAvKipcbiAgICogVGhlIG1pbmltdW0gZHVyYXRpb24gKGluIG1pbGxpc2Vjb25kcykgZm9yIHRoZSBleHBvbmVudGlhbCBiYWNrb2ZmIGFsZ29yaXRobS5cbiAgICogQGRlZmF1bHQgMTAwXG4gICAqL1xuICBiYXNlRGVsYXlNcz86IG51bWJlclxuICAvKipcbiAgICogVGhlIG1heGltdW0gZHVyYXRpb24gKGluIG1pbGxpc2Vjb25kcykgZm9yIHRoZSBleHBvbmVudGlhbCBiYWNrb2ZmIGFsZ29yaXRobS5cbiAgICogQGRlZmF1bHQgNjAwMDBcbiAgICovXG4gIG1heGltdW1EZWxheU1zPzogbnVtYmVyXG59XG5cbmV4cG9ydCBpbnRlcmZhY2UgQ2xpZW50T3B0aW9ucyB7XG4gIGVuZFBvaW50OiBzdHJpbmdcbiAgYWNjZXNzS2V5Pzogc3RyaW5nXG4gIHNlY3JldEtleT86IHN0cmluZ1xuICB1c2VTU0w/OiBib29sZWFuXG4gIHBvcnQ/OiBudW1iZXJcbiAgcmVnaW9uPzogUmVnaW9uXG4gIHRyYW5zcG9ydD86IFRyYW5zcG9ydFxuICBzZXNzaW9uVG9rZW4/OiBzdHJpbmdcbiAgcGFydFNpemU/OiBudW1iZXJcbiAgcGF0aFN0eWxlPzogYm9vbGVhblxuICBjcmVkZW50aWFsc1Byb3ZpZGVyPzogQ3JlZGVudGlhbFByb3ZpZGVyXG4gIHMzQWNjZWxlcmF0ZUVuZHBvaW50Pzogc3RyaW5nXG4gIHRyYW5zcG9ydEFnZW50PzogaHR0cC5BZ2VudFxuICByZXRyeU9wdGlvbnM/OiBSZXRyeU9wdGlvbnNcbn1cblxuZXhwb3J0IHR5cGUgUmVxdWVzdE9wdGlvbiA9IFBhcnRpYWw8SVJlcXVlc3Q+ICYge1xuICBtZXRob2Q6IHN0cmluZ1xuICBidWNrZXROYW1lPzogc3RyaW5nXG4gIG9iamVjdE5hbWU/OiBzdHJpbmdcbiAgcXVlcnk/OiBzdHJpbmdcbiAgcGF0aFN0eWxlPzogYm9vbGVhblxufVxuXG5leHBvcnQgdHlwZSBOb1Jlc3VsdENhbGxiYWNrID0gKGVycm9yOiB1bmtub3duKSA9PiB2b2lkXG5cbmV4cG9ydCBpbnRlcmZhY2UgTWFrZUJ1Y2tldE9wdCB7XG4gIE9iamVjdExvY2tpbmc/OiBib29sZWFuXG59XG5cbmV4cG9ydCBpbnRlcmZhY2UgUmVtb3ZlT3B0aW9ucyB7XG4gIHZlcnNpb25JZD86IHN0cmluZ1xuICBnb3Zlcm5hbmNlQnlwYXNzPzogYm9vbGVhblxuICBmb3JjZURlbGV0ZT86IGJvb2xlYW5cbn1cblxudHlwZSBQYXJ0ID0ge1xuICBwYXJ0OiBudW1iZXJcbiAgZXRhZzogc3RyaW5nXG59XG5cbmV4cG9ydCBjbGFzcyBUeXBlZENsaWVudCB7XG4gIHByb3RlY3RlZCB0cmFuc3BvcnQ6IFRyYW5zcG9ydFxuICBwcm90ZWN0ZWQgaG9zdDogc3RyaW5nXG4gIHByb3RlY3RlZCBwb3J0OiBudW1iZXJcbiAgcHJvdGVjdGVkIHByb3RvY29sOiBzdHJpbmdcbiAgcHJvdGVjdGVkIGFjY2Vzc0tleTogc3RyaW5nXG4gIHByb3RlY3RlZCBzZWNyZXRLZXk6IHN0cmluZ1xuICBwcm90ZWN0ZWQgc2Vzc2lvblRva2VuPzogc3RyaW5nXG4gIHByb3RlY3RlZCB1c2VyQWdlbnQ6IHN0cmluZ1xuICBwcm90ZWN0ZWQgYW5vbnltb3VzOiBib29sZWFuXG4gIHByb3RlY3RlZCBwYXRoU3R5bGU6IGJvb2xlYW5cbiAgcHJvdGVjdGVkIHJlZ2lvbk1hcDogUmVjb3JkPHN0cmluZywgc3RyaW5nPlxuICBwdWJsaWMgcmVnaW9uPzogc3RyaW5nXG4gIHByb3RlY3RlZCBjcmVkZW50aWFsc1Byb3ZpZGVyPzogQ3JlZGVudGlhbFByb3ZpZGVyXG4gIHBhcnRTaXplOiBudW1iZXIgPSA2NCAqIDEwMjQgKiAxMDI0XG4gIHByb3RlY3RlZCBvdmVyUmlkZVBhcnRTaXplPzogYm9vbGVhblxuICBwcm90ZWN0ZWQgcmV0cnlPcHRpb25zOiBSZXRyeU9wdGlvbnNcblxuICBwcm90ZWN0ZWQgbWF4aW11bVBhcnRTaXplID0gNSAqIDEwMjQgKiAxMDI0ICogMTAyNFxuICBwcm90ZWN0ZWQgbWF4T2JqZWN0U2l6ZSA9IDUgKiAxMDI0ICogMTAyNCAqIDEwMjQgKiAxMDI0XG4gIHB1YmxpYyBlbmFibGVTSEEyNTY6IGJvb2xlYW5cbiAgcHJvdGVjdGVkIHMzQWNjZWxlcmF0ZUVuZHBvaW50Pzogc3RyaW5nXG4gIHByb3RlY3RlZCByZXFPcHRpb25zOiBSZWNvcmQ8c3RyaW5nLCB1bmtub3duPlxuXG4gIHByb3RlY3RlZCB0cmFuc3BvcnRBZ2VudDogaHR0cC5BZ2VudFxuICBwcml2YXRlIHJlYWRvbmx5IGNsaWVudEV4dGVuc2lvbnM6IEV4dGVuc2lvbnNcblxuICBjb25zdHJ1Y3RvcihwYXJhbXM6IENsaWVudE9wdGlvbnMpIHtcbiAgICAvLyBAdHMtZXhwZWN0LWVycm9yIGRlcHJlY2F0ZWQgcHJvcGVydHlcbiAgICBpZiAocGFyYW1zLnNlY3VyZSAhPT0gdW5kZWZpbmVkKSB7XG4gICAgICB0aHJvdyBuZXcgRXJyb3IoJ1wic2VjdXJlXCIgb3B0aW9uIGRlcHJlY2F0ZWQsIFwidXNlU1NMXCIgc2hvdWxkIGJlIHVzZWQgaW5zdGVhZCcpXG4gICAgfVxuICAgIC8vIERlZmF1bHQgdmFsdWVzIGlmIG5vdCBzcGVjaWZpZWQuXG4gICAgaWYgKHBhcmFtcy51c2VTU0wgPT09IHVuZGVmaW5lZCkge1xuICAgICAgcGFyYW1zLnVzZVNTTCA9IHRydWVcbiAgICB9XG4gICAgaWYgKCFwYXJhbXMucG9ydCkge1xuICAgICAgcGFyYW1zLnBvcnQgPSAwXG4gICAgfVxuICAgIC8vIFZhbGlkYXRlIGlucHV0IHBhcmFtcy5cbiAgICBpZiAoIWlzVmFsaWRFbmRwb2ludChwYXJhbXMuZW5kUG9pbnQpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRFbmRwb2ludEVycm9yKGBJbnZhbGlkIGVuZFBvaW50IDogJHtwYXJhbXMuZW5kUG9pbnR9YClcbiAgICB9XG4gICAgaWYgKCFpc1ZhbGlkUG9ydChwYXJhbXMucG9ydCkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEFyZ3VtZW50RXJyb3IoYEludmFsaWQgcG9ydCA6ICR7cGFyYW1zLnBvcnR9YClcbiAgICB9XG4gICAgaWYgKCFpc0Jvb2xlYW4ocGFyYW1zLnVzZVNTTCkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEFyZ3VtZW50RXJyb3IoXG4gICAgICAgIGBJbnZhbGlkIHVzZVNTTCBmbGFnIHR5cGUgOiAke3BhcmFtcy51c2VTU0x9LCBleHBlY3RlZCB0byBiZSBvZiB0eXBlIFwiYm9vbGVhblwiYCxcbiAgICAgIClcbiAgICB9XG5cbiAgICAvLyBWYWxpZGF0ZSByZWdpb24gb25seSBpZiBpdHMgc2V0LlxuICAgIGlmIChwYXJhbXMucmVnaW9uKSB7XG4gICAgICBpZiAoIWlzU3RyaW5nKHBhcmFtcy5yZWdpb24pKSB7XG4gICAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEFyZ3VtZW50RXJyb3IoYEludmFsaWQgcmVnaW9uIDogJHtwYXJhbXMucmVnaW9ufWApXG4gICAgICB9XG4gICAgfVxuXG4gICAgY29uc3QgaG9zdCA9IHBhcmFtcy5lbmRQb2ludC50b0xvd2VyQ2FzZSgpXG4gICAgbGV0IHBvcnQgPSBwYXJhbXMucG9ydFxuICAgIGxldCBwcm90b2NvbDogc3RyaW5nXG4gICAgbGV0IHRyYW5zcG9ydFxuICAgIGxldCB0cmFuc3BvcnRBZ2VudDogaHR0cC5BZ2VudFxuICAgIC8vIFZhbGlkYXRlIGlmIGNvbmZpZ3VyYXRpb24gaXMgbm90IHVzaW5nIFNTTFxuICAgIC8vIGZvciBjb25zdHJ1Y3RpbmcgcmVsZXZhbnQgZW5kcG9pbnRzLlxuICAgIGlmIChwYXJhbXMudXNlU1NMKSB7XG4gICAgICAvLyBEZWZhdWx0cyB0byBzZWN1cmUuXG4gICAgICB0cmFuc3BvcnQgPSBodHRwc1xuICAgICAgcHJvdG9jb2wgPSAnaHR0cHM6J1xuICAgICAgcG9ydCA9IHBvcnQgfHwgNDQzXG4gICAgICB0cmFuc3BvcnRBZ2VudCA9IGh0dHBzLmdsb2JhbEFnZW50XG4gICAgfSBlbHNlIHtcbiAgICAgIHRyYW5zcG9ydCA9IGh0dHBcbiAgICAgIHByb3RvY29sID0gJ2h0dHA6J1xuICAgICAgcG9ydCA9IHBvcnQgfHwgODBcbiAgICAgIHRyYW5zcG9ydEFnZW50ID0gaHR0cC5nbG9iYWxBZ2VudFxuICAgIH1cblxuICAgIC8vIGlmIGN1c3RvbSB0cmFuc3BvcnQgaXMgc2V0LCB1c2UgaXQuXG4gICAgaWYgKHBhcmFtcy50cmFuc3BvcnQpIHtcbiAgICAgIGlmICghaXNPYmplY3QocGFyYW1zLnRyYW5zcG9ydCkpIHtcbiAgICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQXJndW1lbnRFcnJvcihcbiAgICAgICAgICBgSW52YWxpZCB0cmFuc3BvcnQgdHlwZSA6ICR7cGFyYW1zLnRyYW5zcG9ydH0sIGV4cGVjdGVkIHRvIGJlIHR5cGUgXCJvYmplY3RcImAsXG4gICAgICAgIClcbiAgICAgIH1cbiAgICAgIHRyYW5zcG9ydCA9IHBhcmFtcy50cmFuc3BvcnRcbiAgICB9XG5cbiAgICAvLyBpZiBjdXN0b20gdHJhbnNwb3J0IGFnZW50IGlzIHNldCwgdXNlIGl0LlxuICAgIGlmIChwYXJhbXMudHJhbnNwb3J0QWdlbnQpIHtcbiAgICAgIGlmICghaXNPYmplY3QocGFyYW1zLnRyYW5zcG9ydEFnZW50KSkge1xuICAgICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKFxuICAgICAgICAgIGBJbnZhbGlkIHRyYW5zcG9ydEFnZW50IHR5cGU6ICR7cGFyYW1zLnRyYW5zcG9ydEFnZW50fSwgZXhwZWN0ZWQgdG8gYmUgdHlwZSBcIm9iamVjdFwiYCxcbiAgICAgICAgKVxuICAgICAgfVxuXG4gICAgICB0cmFuc3BvcnRBZ2VudCA9IHBhcmFtcy50cmFuc3BvcnRBZ2VudFxuICAgIH1cblxuICAgIC8vIFVzZXIgQWdlbnQgc2hvdWxkIGFsd2F5cyBmb2xsb3dpbmcgdGhlIGJlbG93IHN0eWxlLlxuICAgIC8vIFBsZWFzZSBvcGVuIGFuIGlzc3VlIHRvIGRpc2N1c3MgYW55IG5ldyBjaGFuZ2VzIGhlcmUuXG4gICAgLy9cbiAgICAvLyAgICAgICBNaW5JTyAoT1M7IEFSQ0gpIExJQi9WRVIgQVBQL1ZFUlxuICAgIC8vXG4gICAgY29uc3QgbGlicmFyeUNvbW1lbnRzID0gYCgke3Byb2Nlc3MucGxhdGZvcm19OyAke3Byb2Nlc3MuYXJjaH0pYFxuICAgIGNvbnN0IGxpYnJhcnlBZ2VudCA9IGBNaW5JTyAke2xpYnJhcnlDb21tZW50c30gbWluaW8tanMvJHtQYWNrYWdlLnZlcnNpb259YFxuICAgIC8vIFVzZXIgYWdlbnQgYmxvY2sgZW5kcy5cblxuICAgIHRoaXMudHJhbnNwb3J0ID0gdHJhbnNwb3J0XG4gICAgdGhpcy50cmFuc3BvcnRBZ2VudCA9IHRyYW5zcG9ydEFnZW50XG4gICAgdGhpcy5ob3N0ID0gaG9zdFxuICAgIHRoaXMucG9ydCA9IHBvcnRcbiAgICB0aGlzLnByb3RvY29sID0gcHJvdG9jb2xcbiAgICB0aGlzLnVzZXJBZ2VudCA9IGAke2xpYnJhcnlBZ2VudH1gXG5cbiAgICAvLyBEZWZhdWx0IHBhdGggc3R5bGUgaXMgdHJ1ZVxuICAgIGlmIChwYXJhbXMucGF0aFN0eWxlID09PSB1bmRlZmluZWQpIHtcbiAgICAgIHRoaXMucGF0aFN0eWxlID0gdHJ1ZVxuICAgIH0gZWxzZSB7XG4gICAgICB0aGlzLnBhdGhTdHlsZSA9IHBhcmFtcy5wYXRoU3R5bGVcbiAgICB9XG5cbiAgICB0aGlzLmFjY2Vzc0tleSA9IHBhcmFtcy5hY2Nlc3NLZXkgPz8gJydcbiAgICB0aGlzLnNlY3JldEtleSA9IHBhcmFtcy5zZWNyZXRLZXkgPz8gJydcbiAgICB0aGlzLnNlc3Npb25Ub2tlbiA9IHBhcmFtcy5zZXNzaW9uVG9rZW5cbiAgICB0aGlzLmFub255bW91cyA9ICF0aGlzLmFjY2Vzc0tleSB8fCAhdGhpcy5zZWNyZXRLZXlcblxuICAgIGlmIChwYXJhbXMuY3JlZGVudGlhbHNQcm92aWRlcikge1xuICAgICAgdGhpcy5hbm9ueW1vdXMgPSBmYWxzZVxuICAgICAgdGhpcy5jcmVkZW50aWFsc1Byb3ZpZGVyID0gcGFyYW1zLmNyZWRlbnRpYWxzUHJvdmlkZXJcbiAgICB9XG5cbiAgICB0aGlzLnJlZ2lvbk1hcCA9IHt9XG4gICAgaWYgKHBhcmFtcy5yZWdpb24pIHtcbiAgICAgIHRoaXMucmVnaW9uID0gcGFyYW1zLnJlZ2lvblxuICAgIH1cblxuICAgIGlmIChwYXJhbXMucGFydFNpemUpIHtcbiAgICAgIHRoaXMucGFydFNpemUgPSBwYXJhbXMucGFydFNpemVcbiAgICAgIHRoaXMub3ZlclJpZGVQYXJ0U2l6ZSA9IHRydWVcbiAgICB9XG4gICAgaWYgKHRoaXMucGFydFNpemUgPCA1ICogMTAyNCAqIDEwMjQpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEFyZ3VtZW50RXJyb3IoYFBhcnQgc2l6ZSBzaG91bGQgYmUgZ3JlYXRlciB0aGFuIDVNQmApXG4gICAgfVxuICAgIGlmICh0aGlzLnBhcnRTaXplID4gNSAqIDEwMjQgKiAxMDI0ICogMTAyNCkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQXJndW1lbnRFcnJvcihgUGFydCBzaXplIHNob3VsZCBiZSBsZXNzIHRoYW4gNUdCYClcbiAgICB9XG5cbiAgICAvLyBTSEEyNTYgaXMgZW5hYmxlZCBvbmx5IGZvciBhdXRoZW50aWNhdGVkIGh0dHAgcmVxdWVzdHMuIElmIHRoZSByZXF1ZXN0IGlzIGF1dGhlbnRpY2F0ZWRcbiAgICAvLyBhbmQgdGhlIGNvbm5lY3Rpb24gaXMgaHR0cHMgd2UgdXNlIHgtYW16LWNvbnRlbnQtc2hhMjU2PVVOU0lHTkVELVBBWUxPQURcbiAgICAvLyBoZWFkZXIgZm9yIHNpZ25hdHVyZSBjYWxjdWxhdGlvbi5cbiAgICB0aGlzLmVuYWJsZVNIQTI1NiA9ICF0aGlzLmFub255bW91cyAmJiAhcGFyYW1zLnVzZVNTTFxuXG4gICAgdGhpcy5zM0FjY2VsZXJhdGVFbmRwb2ludCA9IHBhcmFtcy5zM0FjY2VsZXJhdGVFbmRwb2ludCB8fCB1bmRlZmluZWRcbiAgICB0aGlzLnJlcU9wdGlvbnMgPSB7fVxuICAgIHRoaXMuY2xpZW50RXh0ZW5zaW9ucyA9IG5ldyBFeHRlbnNpb25zKHRoaXMpXG5cbiAgICBpZiAocGFyYW1zLnJldHJ5T3B0aW9ucykge1xuICAgICAgaWYgKCFpc09iamVjdChwYXJhbXMucmV0cnlPcHRpb25zKSkge1xuICAgICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKFxuICAgICAgICAgIGBJbnZhbGlkIHJldHJ5T3B0aW9ucyB0eXBlOiAke3BhcmFtcy5yZXRyeU9wdGlvbnN9LCBleHBlY3RlZCB0byBiZSB0eXBlIFwib2JqZWN0XCJgLFxuICAgICAgICApXG4gICAgICB9XG5cbiAgICAgIHRoaXMucmV0cnlPcHRpb25zID0gcGFyYW1zLnJldHJ5T3B0aW9uc1xuICAgIH0gZWxzZSB7XG4gICAgICB0aGlzLnJldHJ5T3B0aW9ucyA9IHtcbiAgICAgICAgZGlzYWJsZVJldHJ5OiBmYWxzZSxcbiAgICAgIH1cbiAgICB9XG4gIH1cbiAgLyoqXG4gICAqIE1pbmlvIGV4dGVuc2lvbnMgdGhhdCBhcmVuJ3QgbmVjZXNzYXJ5IHByZXNlbnQgZm9yIEFtYXpvbiBTMyBjb21wYXRpYmxlIHN0b3JhZ2Ugc2VydmVyc1xuICAgKi9cbiAgZ2V0IGV4dGVuc2lvbnMoKSB7XG4gICAgcmV0dXJuIHRoaXMuY2xpZW50RXh0ZW5zaW9uc1xuICB9XG5cbiAgLyoqXG4gICAqIEBwYXJhbSBlbmRQb2ludCAtIHZhbGlkIFMzIGFjY2VsZXJhdGlvbiBlbmQgcG9pbnRcbiAgICovXG4gIHNldFMzVHJhbnNmZXJBY2NlbGVyYXRlKGVuZFBvaW50OiBzdHJpbmcpIHtcbiAgICB0aGlzLnMzQWNjZWxlcmF0ZUVuZHBvaW50ID0gZW5kUG9pbnRcbiAgfVxuXG4gIC8qKlxuICAgKiBTZXRzIHRoZSBzdXBwb3J0ZWQgcmVxdWVzdCBvcHRpb25zLlxuICAgKi9cbiAgcHVibGljIHNldFJlcXVlc3RPcHRpb25zKG9wdGlvbnM6IFBpY2s8aHR0cHMuUmVxdWVzdE9wdGlvbnMsICh0eXBlb2YgcmVxdWVzdE9wdGlvblByb3BlcnRpZXMpW251bWJlcl0+KSB7XG4gICAgaWYgKCFpc09iamVjdChvcHRpb25zKSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcigncmVxdWVzdCBvcHRpb25zIHNob3VsZCBiZSBvZiB0eXBlIFwib2JqZWN0XCInKVxuICAgIH1cbiAgICB0aGlzLnJlcU9wdGlvbnMgPSBfLnBpY2sob3B0aW9ucywgcmVxdWVzdE9wdGlvblByb3BlcnRpZXMpXG4gIH1cblxuICAvKipcbiAgICogIFRoaXMgaXMgczMgU3BlY2lmaWMgYW5kIGRvZXMgbm90IGhvbGQgdmFsaWRpdHkgaW4gYW55IG90aGVyIE9iamVjdCBzdG9yYWdlLlxuICAgKi9cbiAgcHJpdmF0ZSBnZXRBY2NlbGVyYXRlRW5kUG9pbnRJZlNldChidWNrZXROYW1lPzogc3RyaW5nLCBvYmplY3ROYW1lPzogc3RyaW5nKSB7XG4gICAgaWYgKCFpc0VtcHR5KHRoaXMuczNBY2NlbGVyYXRlRW5kcG9pbnQpICYmICFpc0VtcHR5KGJ1Y2tldE5hbWUpICYmICFpc0VtcHR5KG9iamVjdE5hbWUpKSB7XG4gICAgICAvLyBodHRwOi8vZG9jcy5hd3MuYW1hem9uLmNvbS9BbWF6b25TMy9sYXRlc3QvZGV2L3RyYW5zZmVyLWFjY2VsZXJhdGlvbi5odG1sXG4gICAgICAvLyBEaXNhYmxlIHRyYW5zZmVyIGFjY2VsZXJhdGlvbiBmb3Igbm9uLWNvbXBsaWFudCBidWNrZXQgbmFtZXMuXG4gICAgICBpZiAoYnVja2V0TmFtZS5pbmNsdWRlcygnLicpKSB7XG4gICAgICAgIHRocm93IG5ldyBFcnJvcihgVHJhbnNmZXIgQWNjZWxlcmF0aW9uIGlzIG5vdCBzdXBwb3J0ZWQgZm9yIG5vbiBjb21wbGlhbnQgYnVja2V0OiR7YnVja2V0TmFtZX1gKVxuICAgICAgfVxuICAgICAgLy8gSWYgdHJhbnNmZXIgYWNjZWxlcmF0aW9uIGlzIHJlcXVlc3RlZCBzZXQgbmV3IGhvc3QuXG4gICAgICAvLyBGb3IgbW9yZSBkZXRhaWxzIGFib3V0IGVuYWJsaW5nIHRyYW5zZmVyIGFjY2VsZXJhdGlvbiByZWFkIGhlcmUuXG4gICAgICAvLyBodHRwOi8vZG9jcy5hd3MuYW1hem9uLmNvbS9BbWF6b25TMy9sYXRlc3QvZGV2L3RyYW5zZmVyLWFjY2VsZXJhdGlvbi5odG1sXG4gICAgICByZXR1cm4gdGhpcy5zM0FjY2VsZXJhdGVFbmRwb2ludFxuICAgIH1cbiAgICByZXR1cm4gZmFsc2VcbiAgfVxuXG4gIC8qKlxuICAgKiAgIFNldCBhcHBsaWNhdGlvbiBzcGVjaWZpYyBpbmZvcm1hdGlvbi5cbiAgICogICBHZW5lcmF0ZXMgVXNlci1BZ2VudCBpbiB0aGUgZm9sbG93aW5nIHN0eWxlLlxuICAgKiAgIE1pbklPIChPUzsgQVJDSCkgTElCL1ZFUiBBUFAvVkVSXG4gICAqL1xuICBzZXRBcHBJbmZvKGFwcE5hbWU6IHN0cmluZywgYXBwVmVyc2lvbjogc3RyaW5nKSB7XG4gICAgaWYgKCFpc1N0cmluZyhhcHBOYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcihgSW52YWxpZCBhcHBOYW1lOiAke2FwcE5hbWV9YClcbiAgICB9XG4gICAgaWYgKGFwcE5hbWUudHJpbSgpID09PSAnJykge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQXJndW1lbnRFcnJvcignSW5wdXQgYXBwTmFtZSBjYW5ub3QgYmUgZW1wdHkuJylcbiAgICB9XG4gICAgaWYgKCFpc1N0cmluZyhhcHBWZXJzaW9uKSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcihgSW52YWxpZCBhcHBWZXJzaW9uOiAke2FwcFZlcnNpb259YClcbiAgICB9XG4gICAgaWYgKGFwcFZlcnNpb24udHJpbSgpID09PSAnJykge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQXJndW1lbnRFcnJvcignSW5wdXQgYXBwVmVyc2lvbiBjYW5ub3QgYmUgZW1wdHkuJylcbiAgICB9XG4gICAgdGhpcy51c2VyQWdlbnQgPSBgJHt0aGlzLnVzZXJBZ2VudH0gJHthcHBOYW1lfS8ke2FwcFZlcnNpb259YFxuICB9XG5cbiAgLyoqXG4gICAqIHJldHVybnMgb3B0aW9ucyBvYmplY3QgdGhhdCBjYW4gYmUgdXNlZCB3aXRoIGh0dHAucmVxdWVzdCgpXG4gICAqIFRha2VzIGNhcmUgb2YgY29uc3RydWN0aW5nIHZpcnR1YWwtaG9zdC1zdHlsZSBvciBwYXRoLXN0eWxlIGhvc3RuYW1lXG4gICAqL1xuICBwcm90ZWN0ZWQgZ2V0UmVxdWVzdE9wdGlvbnMoXG4gICAgb3B0czogUmVxdWVzdE9wdGlvbiAmIHtcbiAgICAgIHJlZ2lvbjogc3RyaW5nXG4gICAgfSxcbiAgKTogSVJlcXVlc3QgJiB7XG4gICAgaG9zdDogc3RyaW5nXG4gICAgaGVhZGVyczogUmVjb3JkPHN0cmluZywgc3RyaW5nPlxuICB9IHtcbiAgICBjb25zdCBtZXRob2QgPSBvcHRzLm1ldGhvZFxuICAgIGNvbnN0IHJlZ2lvbiA9IG9wdHMucmVnaW9uXG4gICAgY29uc3QgYnVja2V0TmFtZSA9IG9wdHMuYnVja2V0TmFtZVxuICAgIGxldCBvYmplY3ROYW1lID0gb3B0cy5vYmplY3ROYW1lXG4gICAgY29uc3QgaGVhZGVycyA9IG9wdHMuaGVhZGVyc1xuICAgIGNvbnN0IHF1ZXJ5ID0gb3B0cy5xdWVyeVxuXG4gICAgbGV0IHJlcU9wdGlvbnMgPSB7XG4gICAgICBtZXRob2QsXG4gICAgICBoZWFkZXJzOiB7fSBhcyBSZXF1ZXN0SGVhZGVycyxcbiAgICAgIHByb3RvY29sOiB0aGlzLnByb3RvY29sLFxuICAgICAgLy8gSWYgY3VzdG9tIHRyYW5zcG9ydEFnZW50IHdhcyBzdXBwbGllZCBlYXJsaWVyLCB3ZSdsbCBpbmplY3QgaXQgaGVyZVxuICAgICAgYWdlbnQ6IHRoaXMudHJhbnNwb3J0QWdlbnQsXG4gICAgfVxuXG4gICAgLy8gVmVyaWZ5IGlmIHZpcnR1YWwgaG9zdCBzdXBwb3J0ZWQuXG4gICAgbGV0IHZpcnR1YWxIb3N0U3R5bGVcbiAgICBpZiAoYnVja2V0TmFtZSkge1xuICAgICAgdmlydHVhbEhvc3RTdHlsZSA9IGlzVmlydHVhbEhvc3RTdHlsZSh0aGlzLmhvc3QsIHRoaXMucHJvdG9jb2wsIGJ1Y2tldE5hbWUsIHRoaXMucGF0aFN0eWxlKVxuICAgIH1cblxuICAgIGxldCBwYXRoID0gJy8nXG4gICAgbGV0IGhvc3QgPSB0aGlzLmhvc3RcblxuICAgIGxldCBwb3J0OiB1bmRlZmluZWQgfCBudW1iZXJcbiAgICBpZiAodGhpcy5wb3J0KSB7XG4gICAgICBwb3J0ID0gdGhpcy5wb3J0XG4gICAgfVxuXG4gICAgaWYgKG9iamVjdE5hbWUpIHtcbiAgICAgIG9iamVjdE5hbWUgPSB1cmlSZXNvdXJjZUVzY2FwZShvYmplY3ROYW1lKVxuICAgIH1cblxuICAgIC8vIEZvciBBbWF6b24gUzMgZW5kcG9pbnQsIGdldCBlbmRwb2ludCBiYXNlZCBvbiByZWdpb24uXG4gICAgaWYgKGlzQW1hem9uRW5kcG9pbnQoaG9zdCkpIHtcbiAgICAgIGNvbnN0IGFjY2VsZXJhdGVFbmRQb2ludCA9IHRoaXMuZ2V0QWNjZWxlcmF0ZUVuZFBvaW50SWZTZXQoYnVja2V0TmFtZSwgb2JqZWN0TmFtZSlcbiAgICAgIGlmIChhY2NlbGVyYXRlRW5kUG9pbnQpIHtcbiAgICAgICAgaG9zdCA9IGAke2FjY2VsZXJhdGVFbmRQb2ludH1gXG4gICAgICB9IGVsc2Uge1xuICAgICAgICBob3N0ID0gZ2V0UzNFbmRwb2ludChyZWdpb24pXG4gICAgICB9XG4gICAgfVxuXG4gICAgaWYgKHZpcnR1YWxIb3N0U3R5bGUgJiYgIW9wdHMucGF0aFN0eWxlKSB7XG4gICAgICAvLyBGb3IgYWxsIGhvc3RzIHdoaWNoIHN1cHBvcnQgdmlydHVhbCBob3N0IHN0eWxlLCBgYnVja2V0TmFtZWBcbiAgICAgIC8vIGlzIHBhcnQgb2YgdGhlIGhvc3RuYW1lIGluIHRoZSBmb2xsb3dpbmcgZm9ybWF0OlxuICAgICAgLy9cbiAgICAgIC8vICB2YXIgaG9zdCA9ICdidWNrZXROYW1lLmV4YW1wbGUuY29tJ1xuICAgICAgLy9cbiAgICAgIGlmIChidWNrZXROYW1lKSB7XG4gICAgICAgIGhvc3QgPSBgJHtidWNrZXROYW1lfS4ke2hvc3R9YFxuICAgICAgfVxuICAgICAgaWYgKG9iamVjdE5hbWUpIHtcbiAgICAgICAgcGF0aCA9IGAvJHtvYmplY3ROYW1lfWBcbiAgICAgIH1cbiAgICB9IGVsc2Uge1xuICAgICAgLy8gRm9yIGFsbCBTMyBjb21wYXRpYmxlIHN0b3JhZ2Ugc2VydmljZXMgd2Ugd2lsbCBmYWxsYmFjayB0b1xuICAgICAgLy8gcGF0aCBzdHlsZSByZXF1ZXN0cywgd2hlcmUgYGJ1Y2tldE5hbWVgIGlzIHBhcnQgb2YgdGhlIFVSSVxuICAgICAgLy8gcGF0aC5cbiAgICAgIGlmIChidWNrZXROYW1lKSB7XG4gICAgICAgIHBhdGggPSBgLyR7YnVja2V0TmFtZX1gXG4gICAgICB9XG4gICAgICBpZiAob2JqZWN0TmFtZSkge1xuICAgICAgICBwYXRoID0gYC8ke2J1Y2tldE5hbWV9LyR7b2JqZWN0TmFtZX1gXG4gICAgICB9XG4gICAgfVxuXG4gICAgaWYgKHF1ZXJ5KSB7XG4gICAgICBwYXRoICs9IGA/JHtxdWVyeX1gXG4gICAgfVxuICAgIHJlcU9wdGlvbnMuaGVhZGVycy5ob3N0ID0gaG9zdFxuICAgIGlmICgocmVxT3B0aW9ucy5wcm90b2NvbCA9PT0gJ2h0dHA6JyAmJiBwb3J0ICE9PSA4MCkgfHwgKHJlcU9wdGlvbnMucHJvdG9jb2wgPT09ICdodHRwczonICYmIHBvcnQgIT09IDQ0MykpIHtcbiAgICAgIHJlcU9wdGlvbnMuaGVhZGVycy5ob3N0ID0gam9pbkhvc3RQb3J0KGhvc3QsIHBvcnQpXG4gICAgfVxuXG4gICAgcmVxT3B0aW9ucy5oZWFkZXJzWyd1c2VyLWFnZW50J10gPSB0aGlzLnVzZXJBZ2VudFxuICAgIGlmIChoZWFkZXJzKSB7XG4gICAgICAvLyBoYXZlIGFsbCBoZWFkZXIga2V5cyBpbiBsb3dlciBjYXNlIC0gdG8gbWFrZSBzaWduaW5nIGVhc3lcbiAgICAgIGZvciAoY29uc3QgW2ssIHZdIG9mIE9iamVjdC5lbnRyaWVzKGhlYWRlcnMpKSB7XG4gICAgICAgIHJlcU9wdGlvbnMuaGVhZGVyc1trLnRvTG93ZXJDYXNlKCldID0gdlxuICAgICAgfVxuICAgIH1cblxuICAgIC8vIFVzZSBhbnkgcmVxdWVzdCBvcHRpb24gc3BlY2lmaWVkIGluIG1pbmlvQ2xpZW50LnNldFJlcXVlc3RPcHRpb25zKClcbiAgICByZXFPcHRpb25zID0gT2JqZWN0LmFzc2lnbih7fSwgdGhpcy5yZXFPcHRpb25zLCByZXFPcHRpb25zKVxuXG4gICAgcmV0dXJuIHtcbiAgICAgIC4uLnJlcU9wdGlvbnMsXG4gICAgICBoZWFkZXJzOiBfLm1hcFZhbHVlcyhfLnBpY2tCeShyZXFPcHRpb25zLmhlYWRlcnMsIGlzRGVmaW5lZCksICh2KSA9PiB2LnRvU3RyaW5nKCkpLFxuICAgICAgaG9zdCxcbiAgICAgIHBvcnQsXG4gICAgICBwYXRoLFxuICAgIH0gc2F0aXNmaWVzIGh0dHBzLlJlcXVlc3RPcHRpb25zXG4gIH1cblxuICBwdWJsaWMgYXN5bmMgc2V0Q3JlZGVudGlhbHNQcm92aWRlcihjcmVkZW50aWFsc1Byb3ZpZGVyOiBDcmVkZW50aWFsUHJvdmlkZXIpIHtcbiAgICBpZiAoIShjcmVkZW50aWFsc1Byb3ZpZGVyIGluc3RhbmNlb2YgQ3JlZGVudGlhbFByb3ZpZGVyKSkge1xuICAgICAgdGhyb3cgbmV3IEVycm9yKCdVbmFibGUgdG8gZ2V0IGNyZWRlbnRpYWxzLiBFeHBlY3RlZCBpbnN0YW5jZSBvZiBDcmVkZW50aWFsUHJvdmlkZXInKVxuICAgIH1cbiAgICB0aGlzLmNyZWRlbnRpYWxzUHJvdmlkZXIgPSBjcmVkZW50aWFsc1Byb3ZpZGVyXG4gICAgYXdhaXQgdGhpcy5jaGVja0FuZFJlZnJlc2hDcmVkcygpXG4gIH1cblxuICBwcml2YXRlIGFzeW5jIGNoZWNrQW5kUmVmcmVzaENyZWRzKCkge1xuICAgIGlmICh0aGlzLmNyZWRlbnRpYWxzUHJvdmlkZXIpIHtcbiAgICAgIHRyeSB7XG4gICAgICAgIGNvbnN0IGNyZWRlbnRpYWxzQ29uZiA9IGF3YWl0IHRoaXMuY3JlZGVudGlhbHNQcm92aWRlci5nZXRDcmVkZW50aWFscygpXG4gICAgICAgIHRoaXMuYWNjZXNzS2V5ID0gY3JlZGVudGlhbHNDb25mLmdldEFjY2Vzc0tleSgpXG4gICAgICAgIHRoaXMuc2VjcmV0S2V5ID0gY3JlZGVudGlhbHNDb25mLmdldFNlY3JldEtleSgpXG4gICAgICAgIHRoaXMuc2Vzc2lvblRva2VuID0gY3JlZGVudGlhbHNDb25mLmdldFNlc3Npb25Ub2tlbigpXG4gICAgICB9IGNhdGNoIChlKSB7XG4gICAgICAgIHRocm93IG5ldyBFcnJvcihgVW5hYmxlIHRvIGdldCBjcmVkZW50aWFsczogJHtlfWAsIHsgY2F1c2U6IGUgfSlcbiAgICAgIH1cbiAgICB9XG4gIH1cblxuICBwcml2YXRlIGxvZ1N0cmVhbT86IHN0cmVhbS5Xcml0YWJsZVxuXG4gIC8qKlxuICAgKiBsb2cgdGhlIHJlcXVlc3QsIHJlc3BvbnNlLCBlcnJvclxuICAgKi9cbiAgcHJpdmF0ZSBsb2dIVFRQKHJlcU9wdGlvbnM6IElSZXF1ZXN0LCByZXNwb25zZTogaHR0cC5JbmNvbWluZ01lc3NhZ2UgfCBudWxsLCBlcnI/OiB1bmtub3duKSB7XG4gICAgLy8gaWYgbm8gbG9nU3RyZWFtIGF2YWlsYWJsZSByZXR1cm4uXG4gICAgaWYgKCF0aGlzLmxvZ1N0cmVhbSkge1xuICAgICAgcmV0dXJuXG4gICAgfVxuICAgIGlmICghaXNPYmplY3QocmVxT3B0aW9ucykpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ3JlcU9wdGlvbnMgc2hvdWxkIGJlIG9mIHR5cGUgXCJvYmplY3RcIicpXG4gICAgfVxuICAgIGlmIChyZXNwb25zZSAmJiAhaXNSZWFkYWJsZVN0cmVhbShyZXNwb25zZSkpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ3Jlc3BvbnNlIHNob3VsZCBiZSBvZiB0eXBlIFwiU3RyZWFtXCInKVxuICAgIH1cbiAgICBpZiAoZXJyICYmICEoZXJyIGluc3RhbmNlb2YgRXJyb3IpKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdlcnIgc2hvdWxkIGJlIG9mIHR5cGUgXCJFcnJvclwiJylcbiAgICB9XG4gICAgY29uc3QgbG9nU3RyZWFtID0gdGhpcy5sb2dTdHJlYW1cbiAgICBjb25zdCBsb2dIZWFkZXJzID0gKGhlYWRlcnM6IFJlcXVlc3RIZWFkZXJzKSA9PiB7XG4gICAgICBPYmplY3QuZW50cmllcyhoZWFkZXJzKS5mb3JFYWNoKChbaywgdl0pID0+IHtcbiAgICAgICAgaWYgKGsgPT0gJ2F1dGhvcml6YXRpb24nKSB7XG4gICAgICAgICAgaWYgKGlzU3RyaW5nKHYpKSB7XG4gICAgICAgICAgICBjb25zdCByZWRhY3RvciA9IG5ldyBSZWdFeHAoJ1NpZ25hdHVyZT0oWzAtOWEtZl0rKScpXG4gICAgICAgICAgICB2ID0gdi5yZXBsYWNlKHJlZGFjdG9yLCAnU2lnbmF0dXJlPSoqUkVEQUNURUQqKicpXG4gICAgICAgICAgfVxuICAgICAgICB9XG4gICAgICAgIGxvZ1N0cmVhbS53cml0ZShgJHtrfTogJHt2fVxcbmApXG4gICAgICB9KVxuICAgICAgbG9nU3RyZWFtLndyaXRlKCdcXG4nKVxuICAgIH1cbiAgICBsb2dTdHJlYW0ud3JpdGUoYFJFUVVFU1Q6ICR7cmVxT3B0aW9ucy5tZXRob2R9ICR7cmVxT3B0aW9ucy5wYXRofVxcbmApXG4gICAgbG9nSGVhZGVycyhyZXFPcHRpb25zLmhlYWRlcnMpXG4gICAgaWYgKHJlc3BvbnNlKSB7XG4gICAgICB0aGlzLmxvZ1N0cmVhbS53cml0ZShgUkVTUE9OU0U6ICR7cmVzcG9uc2Uuc3RhdHVzQ29kZX1cXG5gKVxuICAgICAgbG9nSGVhZGVycyhyZXNwb25zZS5oZWFkZXJzIGFzIFJlcXVlc3RIZWFkZXJzKVxuICAgIH1cbiAgICBpZiAoZXJyKSB7XG4gICAgICBsb2dTdHJlYW0ud3JpdGUoJ0VSUk9SIEJPRFk6XFxuJylcbiAgICAgIGNvbnN0IGVyckpTT04gPSBKU09OLnN0cmluZ2lmeShlcnIsIG51bGwsICdcXHQnKVxuICAgICAgbG9nU3RyZWFtLndyaXRlKGAke2VyckpTT059XFxuYClcbiAgICB9XG4gIH1cblxuICAvKipcbiAgICogRW5hYmxlIHRyYWNpbmdcbiAgICovXG4gIHB1YmxpYyB0cmFjZU9uKHN0cmVhbT86IHN0cmVhbS5Xcml0YWJsZSkge1xuICAgIGlmICghc3RyZWFtKSB7XG4gICAgICBzdHJlYW0gPSBwcm9jZXNzLnN0ZG91dFxuICAgIH1cbiAgICB0aGlzLmxvZ1N0cmVhbSA9IHN0cmVhbVxuICB9XG5cbiAgLyoqXG4gICAqIERpc2FibGUgdHJhY2luZ1xuICAgKi9cbiAgcHVibGljIHRyYWNlT2ZmKCkge1xuICAgIHRoaXMubG9nU3RyZWFtID0gdW5kZWZpbmVkXG4gIH1cblxuICAvKipcbiAgICogbWFrZVJlcXVlc3QgaXMgdGhlIHByaW1pdGl2ZSB1c2VkIGJ5IHRoZSBhcGlzIGZvciBtYWtpbmcgUzMgcmVxdWVzdHMuXG4gICAqIHBheWxvYWQgY2FuIGJlIGVtcHR5IHN0cmluZyBpbiBjYXNlIG9mIG5vIHBheWxvYWQuXG4gICAqIHN0YXR1c0NvZGUgaXMgdGhlIGV4cGVjdGVkIHN0YXR1c0NvZGUuIElmIHJlc3BvbnNlLnN0YXR1c0NvZGUgZG9lcyBub3QgbWF0Y2hcbiAgICogd2UgcGFyc2UgdGhlIFhNTCBlcnJvciBhbmQgY2FsbCB0aGUgY2FsbGJhY2sgd2l0aCB0aGUgZXJyb3IgbWVzc2FnZS5cbiAgICpcbiAgICogQSB2YWxpZCByZWdpb24gaXMgcGFzc2VkIGJ5IHRoZSBjYWxscyAtIGxpc3RCdWNrZXRzLCBtYWtlQnVja2V0IGFuZCBnZXRCdWNrZXRSZWdpb24uXG4gICAqXG4gICAqIEBpbnRlcm5hbFxuICAgKi9cbiAgYXN5bmMgbWFrZVJlcXVlc3RBc3luYyhcbiAgICBvcHRpb25zOiBSZXF1ZXN0T3B0aW9uLFxuICAgIHBheWxvYWQ6IEJpbmFyeSA9ICcnLFxuICAgIGV4cGVjdGVkQ29kZXM6IG51bWJlcltdID0gWzIwMF0sXG4gICAgcmVnaW9uID0gJycsXG4gICk6IFByb21pc2U8aHR0cC5JbmNvbWluZ01lc3NhZ2U+IHtcbiAgICBpZiAoIWlzT2JqZWN0KG9wdGlvbnMpKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdvcHRpb25zIHNob3VsZCBiZSBvZiB0eXBlIFwib2JqZWN0XCInKVxuICAgIH1cbiAgICBpZiAoIWlzU3RyaW5nKHBheWxvYWQpICYmICFpc09iamVjdChwYXlsb2FkKSkge1xuICAgICAgLy8gQnVmZmVyIGlzIG9mIHR5cGUgJ29iamVjdCdcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ3BheWxvYWQgc2hvdWxkIGJlIG9mIHR5cGUgXCJzdHJpbmdcIiBvciBcIkJ1ZmZlclwiJylcbiAgICB9XG4gICAgZXhwZWN0ZWRDb2Rlcy5mb3JFYWNoKChzdGF0dXNDb2RlKSA9PiB7XG4gICAgICBpZiAoIWlzTnVtYmVyKHN0YXR1c0NvZGUpKSB7XG4gICAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ3N0YXR1c0NvZGUgc2hvdWxkIGJlIG9mIHR5cGUgXCJudW1iZXJcIicpXG4gICAgICB9XG4gICAgfSlcbiAgICBpZiAoIWlzU3RyaW5nKHJlZ2lvbikpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ3JlZ2lvbiBzaG91bGQgYmUgb2YgdHlwZSBcInN0cmluZ1wiJylcbiAgICB9XG4gICAgaWYgKCFvcHRpb25zLmhlYWRlcnMpIHtcbiAgICAgIG9wdGlvbnMuaGVhZGVycyA9IHt9XG4gICAgfVxuICAgIGlmIChvcHRpb25zLm1ldGhvZCA9PT0gJ1BPU1QnIHx8IG9wdGlvbnMubWV0aG9kID09PSAnUFVUJyB8fCBvcHRpb25zLm1ldGhvZCA9PT0gJ0RFTEVURScpIHtcbiAgICAgIG9wdGlvbnMuaGVhZGVyc1snY29udGVudC1sZW5ndGgnXSA9IHBheWxvYWQubGVuZ3RoLnRvU3RyaW5nKClcbiAgICB9XG4gICAgY29uc3Qgc2hhMjU2c3VtID0gdGhpcy5lbmFibGVTSEEyNTYgPyB0b1NoYTI1NihwYXlsb2FkKSA6ICcnXG4gICAgcmV0dXJuIHRoaXMubWFrZVJlcXVlc3RTdHJlYW1Bc3luYyhvcHRpb25zLCBwYXlsb2FkLCBzaGEyNTZzdW0sIGV4cGVjdGVkQ29kZXMsIHJlZ2lvbilcbiAgfVxuXG4gIC8qKlxuICAgKiBuZXcgcmVxdWVzdCB3aXRoIHByb21pc2VcbiAgICpcbiAgICogTm8gbmVlZCB0byBkcmFpbiByZXNwb25zZSwgcmVzcG9uc2UgYm9keSBpcyBub3QgdmFsaWRcbiAgICovXG4gIGFzeW5jIG1ha2VSZXF1ZXN0QXN5bmNPbWl0KFxuICAgIG9wdGlvbnM6IFJlcXVlc3RPcHRpb24sXG4gICAgcGF5bG9hZDogQmluYXJ5ID0gJycsXG4gICAgc3RhdHVzQ29kZXM6IG51bWJlcltdID0gWzIwMF0sXG4gICAgcmVnaW9uID0gJycsXG4gICk6IFByb21pc2U8T21pdDxodHRwLkluY29taW5nTWVzc2FnZSwgJ29uJz4+IHtcbiAgICBjb25zdCByZXMgPSBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0QXN5bmMob3B0aW9ucywgcGF5bG9hZCwgc3RhdHVzQ29kZXMsIHJlZ2lvbilcbiAgICBhd2FpdCBkcmFpblJlc3BvbnNlKHJlcylcbiAgICByZXR1cm4gcmVzXG4gIH1cblxuICAvKipcbiAgICogbWFrZVJlcXVlc3RTdHJlYW0gd2lsbCBiZSB1c2VkIGRpcmVjdGx5IGluc3RlYWQgb2YgbWFrZVJlcXVlc3QgaW4gY2FzZSB0aGUgcGF5bG9hZFxuICAgKiBpcyBhdmFpbGFibGUgYXMgYSBzdHJlYW0uIGZvciBleC4gcHV0T2JqZWN0XG4gICAqXG4gICAqIEBpbnRlcm5hbFxuICAgKi9cbiAgYXN5bmMgbWFrZVJlcXVlc3RTdHJlYW1Bc3luYyhcbiAgICBvcHRpb25zOiBSZXF1ZXN0T3B0aW9uLFxuICAgIGJvZHk6IHN0cmVhbS5SZWFkYWJsZSB8IEJpbmFyeSxcbiAgICBzaGEyNTZzdW06IHN0cmluZyxcbiAgICBzdGF0dXNDb2RlczogbnVtYmVyW10sXG4gICAgcmVnaW9uOiBzdHJpbmcsXG4gICk6IFByb21pc2U8aHR0cC5JbmNvbWluZ01lc3NhZ2U+IHtcbiAgICBpZiAoIWlzT2JqZWN0KG9wdGlvbnMpKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdvcHRpb25zIHNob3VsZCBiZSBvZiB0eXBlIFwib2JqZWN0XCInKVxuICAgIH1cbiAgICBpZiAoIShCdWZmZXIuaXNCdWZmZXIoYm9keSkgfHwgdHlwZW9mIGJvZHkgPT09ICdzdHJpbmcnIHx8IGlzUmVhZGFibGVTdHJlYW0oYm9keSkpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKFxuICAgICAgICBgc3RyZWFtIHNob3VsZCBiZSBhIEJ1ZmZlciwgc3RyaW5nIG9yIHJlYWRhYmxlIFN0cmVhbSwgZ290ICR7dHlwZW9mIGJvZHl9IGluc3RlYWRgLFxuICAgICAgKVxuICAgIH1cbiAgICBpZiAoIWlzU3RyaW5nKHNoYTI1NnN1bSkpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ3NoYTI1NnN1bSBzaG91bGQgYmUgb2YgdHlwZSBcInN0cmluZ1wiJylcbiAgICB9XG4gICAgc3RhdHVzQ29kZXMuZm9yRWFjaCgoc3RhdHVzQ29kZSkgPT4ge1xuICAgICAgaWYgKCFpc051bWJlcihzdGF0dXNDb2RlKSkge1xuICAgICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdzdGF0dXNDb2RlIHNob3VsZCBiZSBvZiB0eXBlIFwibnVtYmVyXCInKVxuICAgICAgfVxuICAgIH0pXG4gICAgaWYgKCFpc1N0cmluZyhyZWdpb24pKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdyZWdpb24gc2hvdWxkIGJlIG9mIHR5cGUgXCJzdHJpbmdcIicpXG4gICAgfVxuICAgIC8vIHNoYTI1NnN1bSB3aWxsIGJlIGVtcHR5IGZvciBhbm9ueW1vdXMgb3IgaHR0cHMgcmVxdWVzdHNcbiAgICBpZiAoIXRoaXMuZW5hYmxlU0hBMjU2ICYmIHNoYTI1NnN1bS5sZW5ndGggIT09IDApIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEFyZ3VtZW50RXJyb3IoYHNoYTI1NnN1bSBleHBlY3RlZCB0byBiZSBlbXB0eSBmb3IgYW5vbnltb3VzIG9yIGh0dHBzIHJlcXVlc3RzYClcbiAgICB9XG4gICAgLy8gc2hhMjU2c3VtIHNob3VsZCBiZSB2YWxpZCBmb3Igbm9uLWFub255bW91cyBodHRwIHJlcXVlc3RzLlxuICAgIGlmICh0aGlzLmVuYWJsZVNIQTI1NiAmJiBzaGEyNTZzdW0ubGVuZ3RoICE9PSA2NCkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQXJndW1lbnRFcnJvcihgSW52YWxpZCBzaGEyNTZzdW0gOiAke3NoYTI1NnN1bX1gKVxuICAgIH1cblxuICAgIGF3YWl0IHRoaXMuY2hlY2tBbmRSZWZyZXNoQ3JlZHMoKVxuXG4gICAgLy8gZXNsaW50LWRpc2FibGUtbmV4dC1saW5lIEB0eXBlc2NyaXB0LWVzbGludC9uby1ub24tbnVsbC1hc3NlcnRpb25cbiAgICByZWdpb24gPSByZWdpb24gfHwgKGF3YWl0IHRoaXMuZ2V0QnVja2V0UmVnaW9uQXN5bmMob3B0aW9ucy5idWNrZXROYW1lISkpXG5cbiAgICBjb25zdCByZXFPcHRpb25zID0gdGhpcy5nZXRSZXF1ZXN0T3B0aW9ucyh7IC4uLm9wdGlvbnMsIHJlZ2lvbiB9KVxuICAgIGlmICghdGhpcy5hbm9ueW1vdXMpIHtcbiAgICAgIC8vIEZvciBub24tYW5vbnltb3VzIGh0dHBzIHJlcXVlc3RzIHNoYTI1NnN1bSBpcyAnVU5TSUdORUQtUEFZTE9BRCcgZm9yIHNpZ25hdHVyZSBjYWxjdWxhdGlvbi5cbiAgICAgIGlmICghdGhpcy5lbmFibGVTSEEyNTYpIHtcbiAgICAgICAgc2hhMjU2c3VtID0gJ1VOU0lHTkVELVBBWUxPQUQnXG4gICAgICB9XG4gICAgICBjb25zdCBkYXRlID0gbmV3IERhdGUoKVxuICAgICAgcmVxT3B0aW9ucy5oZWFkZXJzWyd4LWFtei1kYXRlJ10gPSBtYWtlRGF0ZUxvbmcoZGF0ZSlcbiAgICAgIHJlcU9wdGlvbnMuaGVhZGVyc1sneC1hbXotY29udGVudC1zaGEyNTYnXSA9IHNoYTI1NnN1bVxuICAgICAgaWYgKHRoaXMuc2Vzc2lvblRva2VuKSB7XG4gICAgICAgIHJlcU9wdGlvbnMuaGVhZGVyc1sneC1hbXotc2VjdXJpdHktdG9rZW4nXSA9IHRoaXMuc2Vzc2lvblRva2VuXG4gICAgICB9XG4gICAgICByZXFPcHRpb25zLmhlYWRlcnMuYXV0aG9yaXphdGlvbiA9IHNpZ25WNChyZXFPcHRpb25zLCB0aGlzLmFjY2Vzc0tleSwgdGhpcy5zZWNyZXRLZXksIHJlZ2lvbiwgZGF0ZSwgc2hhMjU2c3VtKVxuICAgIH1cblxuICAgIGNvbnN0IGxvZ1N0cmVhbSA9IHRoaXMubG9nU3RyZWFtXG4gICAgY29uc3QgbG9nID0gbG9nU3RyZWFtID8gKG1zZzogc3RyaW5nKSA9PiBsb2dTdHJlYW0ud3JpdGUoYCR7bXNnfVxcbmApIDogKCkgPT4ge31cblxuICAgIGNvbnN0IHJlc3BvbnNlID0gYXdhaXQgcmVxdWVzdFdpdGhSZXRyeShcbiAgICAgIHRoaXMudHJhbnNwb3J0LFxuICAgICAgcmVxT3B0aW9ucyxcbiAgICAgIGJvZHksXG4gICAgICB0aGlzLnJldHJ5T3B0aW9ucy5kaXNhYmxlUmV0cnkgPT09IHRydWUgPyAwIDogdGhpcy5yZXRyeU9wdGlvbnMubWF4aW11bVJldHJ5Q291bnQsXG4gICAgICB0aGlzLnJldHJ5T3B0aW9ucy5iYXNlRGVsYXlNcyxcbiAgICAgIHRoaXMucmV0cnlPcHRpb25zLm1heGltdW1EZWxheU1zLFxuICAgICAgbG9nLFxuICAgIClcbiAgICBpZiAoIXJlc3BvbnNlLnN0YXR1c0NvZGUpIHtcbiAgICAgIHRocm93IG5ldyBFcnJvcihcIkJVRzogcmVzcG9uc2UgZG9lc24ndCBoYXZlIGEgc3RhdHVzQ29kZVwiKVxuICAgIH1cblxuICAgIGlmICghc3RhdHVzQ29kZXMuaW5jbHVkZXMocmVzcG9uc2Uuc3RhdHVzQ29kZSkpIHtcbiAgICAgIC8vIEZvciBhbiBpbmNvcnJlY3QgcmVnaW9uLCBTMyBzZXJ2ZXIgYWx3YXlzIHNlbmRzIGJhY2sgNDAwLlxuICAgICAgLy8gQnV0IHdlIHdpbGwgZG8gY2FjaGUgaW52YWxpZGF0aW9uIGZvciBhbGwgZXJyb3JzIHNvIHRoYXQsXG4gICAgICAvLyBpbiBmdXR1cmUsIGlmIEFXUyBTMyBkZWNpZGVzIHRvIHNlbmQgYSBkaWZmZXJlbnQgc3RhdHVzIGNvZGUgb3JcbiAgICAgIC8vIFhNTCBlcnJvciBjb2RlIHdlIHdpbGwgc3RpbGwgd29yayBmaW5lLlxuICAgICAgLy8gZXNsaW50LWRpc2FibGUtbmV4dC1saW5lIEB0eXBlc2NyaXB0LWVzbGludC9uby1ub24tbnVsbC1hc3NlcnRpb25cbiAgICAgIGRlbGV0ZSB0aGlzLnJlZ2lvbk1hcFtvcHRpb25zLmJ1Y2tldE5hbWUhXVxuXG4gICAgICBjb25zdCBlcnIgPSBhd2FpdCB4bWxQYXJzZXJzLnBhcnNlUmVzcG9uc2VFcnJvcihyZXNwb25zZSlcbiAgICAgIHRoaXMubG9nSFRUUChyZXFPcHRpb25zLCByZXNwb25zZSwgZXJyKVxuICAgICAgdGhyb3cgZXJyXG4gICAgfVxuXG4gICAgdGhpcy5sb2dIVFRQKHJlcU9wdGlvbnMsIHJlc3BvbnNlKVxuXG4gICAgcmV0dXJuIHJlc3BvbnNlXG4gIH1cblxuICAvKipcbiAgICogZ2V0cyB0aGUgcmVnaW9uIG9mIHRoZSBidWNrZXRcbiAgICpcbiAgICogQHBhcmFtIGJ1Y2tldE5hbWVcbiAgICpcbiAgICovXG4gIGFzeW5jIGdldEJ1Y2tldFJlZ2lvbkFzeW5jKGJ1Y2tldE5hbWU6IHN0cmluZyk6IFByb21pc2U8c3RyaW5nPiB7XG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZShidWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKGBJbnZhbGlkIGJ1Y2tldCBuYW1lIDogJHtidWNrZXROYW1lfWApXG4gICAgfVxuXG4gICAgLy8gUmVnaW9uIGlzIHNldCB3aXRoIGNvbnN0cnVjdG9yLCByZXR1cm4gdGhlIHJlZ2lvbiByaWdodCBoZXJlLlxuICAgIGlmICh0aGlzLnJlZ2lvbikge1xuICAgICAgcmV0dXJuIHRoaXMucmVnaW9uXG4gICAgfVxuXG4gICAgY29uc3QgY2FjaGVkID0gdGhpcy5yZWdpb25NYXBbYnVja2V0TmFtZV1cbiAgICBpZiAoY2FjaGVkKSB7XG4gICAgICByZXR1cm4gY2FjaGVkXG4gICAgfVxuXG4gICAgY29uc3QgZXh0cmFjdFJlZ2lvbkFzeW5jID0gYXN5bmMgKHJlc3BvbnNlOiBodHRwLkluY29taW5nTWVzc2FnZSkgPT4ge1xuICAgICAgY29uc3QgYm9keSA9IGF3YWl0IHJlYWRBc1N0cmluZyhyZXNwb25zZSlcbiAgICAgIGNvbnN0IHJlZ2lvbiA9IHhtbFBhcnNlcnMucGFyc2VCdWNrZXRSZWdpb24oYm9keSkgfHwgREVGQVVMVF9SRUdJT05cbiAgICAgIHRoaXMucmVnaW9uTWFwW2J1Y2tldE5hbWVdID0gcmVnaW9uXG4gICAgICByZXR1cm4gcmVnaW9uXG4gICAgfVxuXG4gICAgY29uc3QgbWV0aG9kID0gJ0dFVCdcbiAgICBjb25zdCBxdWVyeSA9ICdsb2NhdGlvbidcbiAgICAvLyBgZ2V0QnVja2V0TG9jYXRpb25gIGJlaGF2ZXMgZGlmZmVyZW50bHkgaW4gZm9sbG93aW5nIHdheXMgZm9yXG4gICAgLy8gZGlmZmVyZW50IGVudmlyb25tZW50cy5cbiAgICAvL1xuICAgIC8vIC0gRm9yIG5vZGVqcyBlbnYgd2UgZGVmYXVsdCB0byBwYXRoIHN0eWxlIHJlcXVlc3RzLlxuICAgIC8vIC0gRm9yIGJyb3dzZXIgZW52IHBhdGggc3R5bGUgcmVxdWVzdHMgb24gYnVja2V0cyB5aWVsZHMgQ09SU1xuICAgIC8vICAgZXJyb3IuIFRvIGNpcmN1bXZlbnQgdGhpcyBwcm9ibGVtIHdlIG1ha2UgYSB2aXJ0dWFsIGhvc3RcbiAgICAvLyAgIHN0eWxlIHJlcXVlc3Qgc2lnbmVkIHdpdGggJ3VzLWVhc3QtMScuIFRoaXMgcmVxdWVzdCBmYWlsc1xuICAgIC8vICAgd2l0aCBhbiBlcnJvciAnQXV0aG9yaXphdGlvbkhlYWRlck1hbGZvcm1lZCcsIGFkZGl0aW9uYWxseVxuICAgIC8vICAgdGhlIGVycm9yIFhNTCBhbHNvIHByb3ZpZGVzIFJlZ2lvbiBvZiB0aGUgYnVja2V0LiBUbyB2YWxpZGF0ZVxuICAgIC8vICAgdGhpcyByZWdpb24gaXMgcHJvcGVyIHdlIHJldHJ5IHRoZSBzYW1lIHJlcXVlc3Qgd2l0aCB0aGUgbmV3bHlcbiAgICAvLyAgIG9idGFpbmVkIHJlZ2lvbi5cbiAgICBjb25zdCBwYXRoU3R5bGUgPSB0aGlzLnBhdGhTdHlsZSAmJiAhaXNCcm93c2VyXG4gICAgbGV0IHJlZ2lvbjogc3RyaW5nXG4gICAgdHJ5IHtcbiAgICAgIGNvbnN0IHJlcyA9IGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luYyh7IG1ldGhvZCwgYnVja2V0TmFtZSwgcXVlcnksIHBhdGhTdHlsZSB9LCAnJywgWzIwMF0sIERFRkFVTFRfUkVHSU9OKVxuICAgICAgcmV0dXJuIGV4dHJhY3RSZWdpb25Bc3luYyhyZXMpXG4gICAgfSBjYXRjaCAoZSkge1xuICAgICAgLy8gbWFrZSBhbGlnbm1lbnQgd2l0aCBtYyBjbGlcbiAgICAgIGlmIChlIGluc3RhbmNlb2YgZXJyb3JzLlMzRXJyb3IpIHtcbiAgICAgICAgY29uc3QgZXJyQ29kZSA9IGUuY29kZVxuICAgICAgICBjb25zdCBlcnJSZWdpb24gPSBlLnJlZ2lvblxuICAgICAgICBpZiAoZXJyQ29kZSA9PT0gJ0FjY2Vzc0RlbmllZCcgJiYgIWVyclJlZ2lvbikge1xuICAgICAgICAgIHJldHVybiBERUZBVUxUX1JFR0lPTlxuICAgICAgICB9XG4gICAgICB9XG4gICAgICAvLyBlc2xpbnQtZGlzYWJsZS1uZXh0LWxpbmUgQHR5cGVzY3JpcHQtZXNsaW50L2Jhbi10cy1jb21tZW50XG4gICAgICAvLyBAdHMtaWdub3JlXG4gICAgICBpZiAoIShlLm5hbWUgPT09ICdBdXRob3JpemF0aW9uSGVhZGVyTWFsZm9ybWVkJykpIHtcbiAgICAgICAgdGhyb3cgZVxuICAgICAgfVxuICAgICAgLy8gQHRzLWV4cGVjdC1lcnJvciB3ZSBzZXQgZXh0cmEgcHJvcGVydGllcyBvbiBlcnJvciBvYmplY3RcbiAgICAgIHJlZ2lvbiA9IGUuUmVnaW9uIGFzIHN0cmluZ1xuICAgICAgaWYgKCFyZWdpb24pIHtcbiAgICAgICAgdGhyb3cgZVxuICAgICAgfVxuICAgIH1cblxuICAgIGNvbnN0IHJlcyA9IGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luYyh7IG1ldGhvZCwgYnVja2V0TmFtZSwgcXVlcnksIHBhdGhTdHlsZSB9LCAnJywgWzIwMF0sIHJlZ2lvbilcbiAgICByZXR1cm4gYXdhaXQgZXh0cmFjdFJlZ2lvbkFzeW5jKHJlcylcbiAgfVxuXG4gIC8qKlxuICAgKiBtYWtlUmVxdWVzdCBpcyB0aGUgcHJpbWl0aXZlIHVzZWQgYnkgdGhlIGFwaXMgZm9yIG1ha2luZyBTMyByZXF1ZXN0cy5cbiAgICogcGF5bG9hZCBjYW4gYmUgZW1wdHkgc3RyaW5nIGluIGNhc2Ugb2Ygbm8gcGF5bG9hZC5cbiAgICogc3RhdHVzQ29kZSBpcyB0aGUgZXhwZWN0ZWQgc3RhdHVzQ29kZS4gSWYgcmVzcG9uc2Uuc3RhdHVzQ29kZSBkb2VzIG5vdCBtYXRjaFxuICAgKiB3ZSBwYXJzZSB0aGUgWE1MIGVycm9yIGFuZCBjYWxsIHRoZSBjYWxsYmFjayB3aXRoIHRoZSBlcnJvciBtZXNzYWdlLlxuICAgKiBBIHZhbGlkIHJlZ2lvbiBpcyBwYXNzZWQgYnkgdGhlIGNhbGxzIC0gbGlzdEJ1Y2tldHMsIG1ha2VCdWNrZXQgYW5kXG4gICAqIGdldEJ1Y2tldFJlZ2lvbi5cbiAgICpcbiAgICogQGRlcHJlY2F0ZWQgdXNlIGBtYWtlUmVxdWVzdEFzeW5jYCBpbnN0ZWFkXG4gICAqL1xuICBtYWtlUmVxdWVzdChcbiAgICBvcHRpb25zOiBSZXF1ZXN0T3B0aW9uLFxuICAgIHBheWxvYWQ6IEJpbmFyeSA9ICcnLFxuICAgIGV4cGVjdGVkQ29kZXM6IG51bWJlcltdID0gWzIwMF0sXG4gICAgcmVnaW9uID0gJycsXG4gICAgcmV0dXJuUmVzcG9uc2U6IGJvb2xlYW4sXG4gICAgY2I6IChjYjogdW5rbm93biwgcmVzdWx0OiBodHRwLkluY29taW5nTWVzc2FnZSkgPT4gdm9pZCxcbiAgKSB7XG4gICAgbGV0IHByb206IFByb21pc2U8aHR0cC5JbmNvbWluZ01lc3NhZ2U+XG4gICAgaWYgKHJldHVyblJlc3BvbnNlKSB7XG4gICAgICBwcm9tID0gdGhpcy5tYWtlUmVxdWVzdEFzeW5jKG9wdGlvbnMsIHBheWxvYWQsIGV4cGVjdGVkQ29kZXMsIHJlZ2lvbilcbiAgICB9IGVsc2Uge1xuICAgICAgLy8gZXNsaW50LWRpc2FibGUtbmV4dC1saW5lIEB0eXBlc2NyaXB0LWVzbGludC9iYW4tdHMtY29tbWVudFxuICAgICAgLy8gQHRzLWV4cGVjdC1lcnJvciBjb21wYXRpYmxlIGZvciBvbGQgYmVoYXZpb3VyXG4gICAgICBwcm9tID0gdGhpcy5tYWtlUmVxdWVzdEFzeW5jT21pdChvcHRpb25zLCBwYXlsb2FkLCBleHBlY3RlZENvZGVzLCByZWdpb24pXG4gICAgfVxuXG4gICAgcHJvbS50aGVuKFxuICAgICAgKHJlc3VsdCkgPT4gY2IobnVsbCwgcmVzdWx0KSxcbiAgICAgIChlcnIpID0+IHtcbiAgICAgICAgLy8gZXNsaW50LWRpc2FibGUtbmV4dC1saW5lIEB0eXBlc2NyaXB0LWVzbGludC9iYW4tdHMtY29tbWVudFxuICAgICAgICAvLyBAdHMtaWdub3JlXG4gICAgICAgIGNiKGVycilcbiAgICAgIH0sXG4gICAgKVxuICB9XG5cbiAgLyoqXG4gICAqIG1ha2VSZXF1ZXN0U3RyZWFtIHdpbGwgYmUgdXNlZCBkaXJlY3RseSBpbnN0ZWFkIG9mIG1ha2VSZXF1ZXN0IGluIGNhc2UgdGhlIHBheWxvYWRcbiAgICogaXMgYXZhaWxhYmxlIGFzIGEgc3RyZWFtLiBmb3IgZXguIHB1dE9iamVjdFxuICAgKlxuICAgKiBAZGVwcmVjYXRlZCB1c2UgYG1ha2VSZXF1ZXN0U3RyZWFtQXN5bmNgIGluc3RlYWRcbiAgICovXG4gIG1ha2VSZXF1ZXN0U3RyZWFtKFxuICAgIG9wdGlvbnM6IFJlcXVlc3RPcHRpb24sXG4gICAgc3RyZWFtOiBzdHJlYW0uUmVhZGFibGUgfCBCdWZmZXIsXG4gICAgc2hhMjU2c3VtOiBzdHJpbmcsXG4gICAgc3RhdHVzQ29kZXM6IG51bWJlcltdLFxuICAgIHJlZ2lvbjogc3RyaW5nLFxuICAgIHJldHVyblJlc3BvbnNlOiBib29sZWFuLFxuICAgIGNiOiAoY2I6IHVua25vd24sIHJlc3VsdDogaHR0cC5JbmNvbWluZ01lc3NhZ2UpID0+IHZvaWQsXG4gICkge1xuICAgIGNvbnN0IGV4ZWN1dG9yID0gYXN5bmMgKCkgPT4ge1xuICAgICAgY29uc3QgcmVzID0gYXdhaXQgdGhpcy5tYWtlUmVxdWVzdFN0cmVhbUFzeW5jKG9wdGlvbnMsIHN0cmVhbSwgc2hhMjU2c3VtLCBzdGF0dXNDb2RlcywgcmVnaW9uKVxuICAgICAgaWYgKCFyZXR1cm5SZXNwb25zZSkge1xuICAgICAgICBhd2FpdCBkcmFpblJlc3BvbnNlKHJlcylcbiAgICAgIH1cblxuICAgICAgcmV0dXJuIHJlc1xuICAgIH1cblxuICAgIGV4ZWN1dG9yKCkudGhlbihcbiAgICAgIChyZXN1bHQpID0+IGNiKG51bGwsIHJlc3VsdCksXG4gICAgICAvLyBlc2xpbnQtZGlzYWJsZS1uZXh0LWxpbmUgQHR5cGVzY3JpcHQtZXNsaW50L2Jhbi10cy1jb21tZW50XG4gICAgICAvLyBAdHMtaWdub3JlXG4gICAgICAoZXJyKSA9PiBjYihlcnIpLFxuICAgIClcbiAgfVxuXG4gIC8qKlxuICAgKiBAZGVwcmVjYXRlZCB1c2UgYGdldEJ1Y2tldFJlZ2lvbkFzeW5jYCBpbnN0ZWFkXG4gICAqL1xuICBnZXRCdWNrZXRSZWdpb24oYnVja2V0TmFtZTogc3RyaW5nLCBjYjogKGVycjogdW5rbm93biwgcmVnaW9uOiBzdHJpbmcpID0+IHZvaWQpIHtcbiAgICByZXR1cm4gdGhpcy5nZXRCdWNrZXRSZWdpb25Bc3luYyhidWNrZXROYW1lKS50aGVuKFxuICAgICAgKHJlc3VsdCkgPT4gY2IobnVsbCwgcmVzdWx0KSxcbiAgICAgIC8vIGVzbGludC1kaXNhYmxlLW5leHQtbGluZSBAdHlwZXNjcmlwdC1lc2xpbnQvYmFuLXRzLWNvbW1lbnRcbiAgICAgIC8vIEB0cy1pZ25vcmVcbiAgICAgIChlcnIpID0+IGNiKGVyciksXG4gICAgKVxuICB9XG5cbiAgLy8gQnVja2V0IG9wZXJhdGlvbnNcblxuICAvKipcbiAgICogQ3JlYXRlcyB0aGUgYnVja2V0IGBidWNrZXROYW1lYC5cbiAgICpcbiAgICovXG4gIGFzeW5jIG1ha2VCdWNrZXQoYnVja2V0TmFtZTogc3RyaW5nLCByZWdpb246IFJlZ2lvbiA9ICcnLCBtYWtlT3B0cz86IE1ha2VCdWNrZXRPcHQpOiBQcm9taXNlPHZvaWQ+IHtcbiAgICBpZiAoIWlzVmFsaWRCdWNrZXROYW1lKGJ1Y2tldE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IoJ0ludmFsaWQgYnVja2V0IG5hbWU6ICcgKyBidWNrZXROYW1lKVxuICAgIH1cbiAgICAvLyBCYWNrd2FyZCBDb21wYXRpYmlsaXR5XG4gICAgaWYgKGlzT2JqZWN0KHJlZ2lvbikpIHtcbiAgICAgIG1ha2VPcHRzID0gcmVnaW9uXG4gICAgICByZWdpb24gPSAnJ1xuICAgIH1cblxuICAgIGlmICghaXNTdHJpbmcocmVnaW9uKSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcigncmVnaW9uIHNob3VsZCBiZSBvZiB0eXBlIFwic3RyaW5nXCInKVxuICAgIH1cbiAgICBpZiAobWFrZU9wdHMgJiYgIWlzT2JqZWN0KG1ha2VPcHRzKSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcignbWFrZU9wdHMgc2hvdWxkIGJlIG9mIHR5cGUgXCJvYmplY3RcIicpXG4gICAgfVxuXG4gICAgbGV0IHBheWxvYWQgPSAnJ1xuXG4gICAgLy8gUmVnaW9uIGFscmVhZHkgc2V0IGluIGNvbnN0cnVjdG9yLCB2YWxpZGF0ZSBpZlxuICAgIC8vIGNhbGxlciByZXF1ZXN0ZWQgYnVja2V0IGxvY2F0aW9uIGlzIHNhbWUuXG4gICAgaWYgKHJlZ2lvbiAmJiB0aGlzLnJlZ2lvbikge1xuICAgICAgaWYgKHJlZ2lvbiAhPT0gdGhpcy5yZWdpb24pIHtcbiAgICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQXJndW1lbnRFcnJvcihgQ29uZmlndXJlZCByZWdpb24gJHt0aGlzLnJlZ2lvbn0sIHJlcXVlc3RlZCAke3JlZ2lvbn1gKVxuICAgICAgfVxuICAgIH1cbiAgICAvLyBzZW5kaW5nIG1ha2VCdWNrZXQgcmVxdWVzdCB3aXRoIFhNTCBjb250YWluaW5nICd1cy1lYXN0LTEnIGZhaWxzLiBGb3JcbiAgICAvLyBkZWZhdWx0IHJlZ2lvbiBzZXJ2ZXIgZXhwZWN0cyB0aGUgcmVxdWVzdCB3aXRob3V0IGJvZHlcbiAgICBpZiAocmVnaW9uICYmIHJlZ2lvbiAhPT0gREVGQVVMVF9SRUdJT04pIHtcbiAgICAgIHBheWxvYWQgPSB4bWwuYnVpbGRPYmplY3Qoe1xuICAgICAgICBDcmVhdGVCdWNrZXRDb25maWd1cmF0aW9uOiB7XG4gICAgICAgICAgJDogeyB4bWxuczogJ2h0dHA6Ly9zMy5hbWF6b25hd3MuY29tL2RvYy8yMDA2LTAzLTAxLycgfSxcbiAgICAgICAgICBMb2NhdGlvbkNvbnN0cmFpbnQ6IHJlZ2lvbixcbiAgICAgICAgfSxcbiAgICAgIH0pXG4gICAgfVxuICAgIGNvbnN0IG1ldGhvZCA9ICdQVVQnXG4gICAgY29uc3QgaGVhZGVyczogUmVxdWVzdEhlYWRlcnMgPSB7fVxuXG4gICAgaWYgKG1ha2VPcHRzICYmIG1ha2VPcHRzLk9iamVjdExvY2tpbmcpIHtcbiAgICAgIGhlYWRlcnNbJ3gtYW16LWJ1Y2tldC1vYmplY3QtbG9jay1lbmFibGVkJ10gPSB0cnVlXG4gICAgfVxuXG4gICAgLy8gRm9yIGN1c3RvbSByZWdpb24gY2xpZW50cyAgZGVmYXVsdCB0byBjdXN0b20gcmVnaW9uIHNwZWNpZmllZCBpbiBjbGllbnQgY29uc3RydWN0b3JcbiAgICBjb25zdCBmaW5hbFJlZ2lvbiA9IHRoaXMucmVnaW9uIHx8IHJlZ2lvbiB8fCBERUZBVUxUX1JFR0lPTlxuXG4gICAgY29uc3QgcmVxdWVzdE9wdDogUmVxdWVzdE9wdGlvbiA9IHsgbWV0aG9kLCBidWNrZXROYW1lLCBoZWFkZXJzIH1cblxuICAgIHRyeSB7XG4gICAgICBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0QXN5bmNPbWl0KHJlcXVlc3RPcHQsIHBheWxvYWQsIFsyMDBdLCBmaW5hbFJlZ2lvbilcbiAgICB9IGNhdGNoIChlcnI6IHVua25vd24pIHtcbiAgICAgIGlmIChyZWdpb24gPT09ICcnIHx8IHJlZ2lvbiA9PT0gREVGQVVMVF9SRUdJT04pIHtcbiAgICAgICAgaWYgKGVyciBpbnN0YW5jZW9mIGVycm9ycy5TM0Vycm9yKSB7XG4gICAgICAgICAgY29uc3QgZXJyQ29kZSA9IGVyci5jb2RlXG4gICAgICAgICAgY29uc3QgZXJyUmVnaW9uID0gZXJyLnJlZ2lvblxuICAgICAgICAgIGlmIChlcnJDb2RlID09PSAnQXV0aG9yaXphdGlvbkhlYWRlck1hbGZvcm1lZCcgJiYgZXJyUmVnaW9uICE9PSAnJykge1xuICAgICAgICAgICAgLy8gUmV0cnkgd2l0aCByZWdpb24gcmV0dXJuZWQgYXMgcGFydCBvZiBlcnJvclxuICAgICAgICAgICAgYXdhaXQgdGhpcy5tYWtlUmVxdWVzdEFzeW5jT21pdChyZXF1ZXN0T3B0LCBwYXlsb2FkLCBbMjAwXSwgZXJyQ29kZSlcbiAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICAgIHRocm93IGVyclxuICAgIH1cbiAgfVxuXG4gIC8qKlxuICAgKiBUbyBjaGVjayBpZiBhIGJ1Y2tldCBhbHJlYWR5IGV4aXN0cy5cbiAgICovXG4gIGFzeW5jIGJ1Y2tldEV4aXN0cyhidWNrZXROYW1lOiBzdHJpbmcpOiBQcm9taXNlPGJvb2xlYW4+IHtcbiAgICBpZiAoIWlzVmFsaWRCdWNrZXROYW1lKGJ1Y2tldE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IoJ0ludmFsaWQgYnVja2V0IG5hbWU6ICcgKyBidWNrZXROYW1lKVxuICAgIH1cbiAgICBjb25zdCBtZXRob2QgPSAnSEVBRCdcbiAgICB0cnkge1xuICAgICAgYXdhaXQgdGhpcy5tYWtlUmVxdWVzdEFzeW5jT21pdCh7IG1ldGhvZCwgYnVja2V0TmFtZSB9KVxuICAgIH0gY2F0Y2ggKGVycikge1xuICAgICAgLy8gQHRzLWlnbm9yZVxuICAgICAgaWYgKGVyci5jb2RlID09PSAnTm9TdWNoQnVja2V0JyB8fCBlcnIuY29kZSA9PT0gJ05vdEZvdW5kJykge1xuICAgICAgICByZXR1cm4gZmFsc2VcbiAgICAgIH1cbiAgICAgIHRocm93IGVyclxuICAgIH1cblxuICAgIHJldHVybiB0cnVlXG4gIH1cblxuICBhc3luYyByZW1vdmVCdWNrZXQoYnVja2V0TmFtZTogc3RyaW5nKTogUHJvbWlzZTx2b2lkPlxuXG4gIC8qKlxuICAgKiBAZGVwcmVjYXRlZCB1c2UgcHJvbWlzZSBzdHlsZSBBUElcbiAgICovXG4gIHJlbW92ZUJ1Y2tldChidWNrZXROYW1lOiBzdHJpbmcsIGNhbGxiYWNrOiBOb1Jlc3VsdENhbGxiYWNrKTogdm9pZFxuXG4gIGFzeW5jIHJlbW92ZUJ1Y2tldChidWNrZXROYW1lOiBzdHJpbmcpOiBQcm9taXNlPHZvaWQ+IHtcbiAgICBpZiAoIWlzVmFsaWRCdWNrZXROYW1lKGJ1Y2tldE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IoJ0ludmFsaWQgYnVja2V0IG5hbWU6ICcgKyBidWNrZXROYW1lKVxuICAgIH1cbiAgICBjb25zdCBtZXRob2QgPSAnREVMRVRFJ1xuICAgIGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luY09taXQoeyBtZXRob2QsIGJ1Y2tldE5hbWUgfSwgJycsIFsyMDRdKVxuICAgIGRlbGV0ZSB0aGlzLnJlZ2lvbk1hcFtidWNrZXROYW1lXVxuICB9XG5cbiAgLyoqXG4gICAqIENhbGxiYWNrIGlzIGNhbGxlZCB3aXRoIHJlYWRhYmxlIHN0cmVhbSBvZiB0aGUgb2JqZWN0IGNvbnRlbnQuXG4gICAqL1xuICBhc3luYyBnZXRPYmplY3QoYnVja2V0TmFtZTogc3RyaW5nLCBvYmplY3ROYW1lOiBzdHJpbmcsIGdldE9wdHM/OiBHZXRPYmplY3RPcHRzKTogUHJvbWlzZTxzdHJlYW0uUmVhZGFibGU+IHtcbiAgICBpZiAoIWlzVmFsaWRCdWNrZXROYW1lKGJ1Y2tldE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IoJ0ludmFsaWQgYnVja2V0IG5hbWU6ICcgKyBidWNrZXROYW1lKVxuICAgIH1cbiAgICBpZiAoIWlzVmFsaWRPYmplY3ROYW1lKG9iamVjdE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRPYmplY3ROYW1lRXJyb3IoYEludmFsaWQgb2JqZWN0IG5hbWU6ICR7b2JqZWN0TmFtZX1gKVxuICAgIH1cbiAgICByZXR1cm4gdGhpcy5nZXRQYXJ0aWFsT2JqZWN0KGJ1Y2tldE5hbWUsIG9iamVjdE5hbWUsIDAsIDAsIGdldE9wdHMpXG4gIH1cblxuICAvKipcbiAgICogQ2FsbGJhY2sgaXMgY2FsbGVkIHdpdGggcmVhZGFibGUgc3RyZWFtIG9mIHRoZSBwYXJ0aWFsIG9iamVjdCBjb250ZW50LlxuICAgKiBAcGFyYW0gYnVja2V0TmFtZVxuICAgKiBAcGFyYW0gb2JqZWN0TmFtZVxuICAgKiBAcGFyYW0gb2Zmc2V0XG4gICAqIEBwYXJhbSBsZW5ndGggLSBsZW5ndGggb2YgdGhlIG9iamVjdCB0aGF0IHdpbGwgYmUgcmVhZCBpbiB0aGUgc3RyZWFtIChvcHRpb25hbCwgaWYgbm90IHNwZWNpZmllZCB3ZSByZWFkIHRoZSByZXN0IG9mIHRoZSBmaWxlIGZyb20gdGhlIG9mZnNldClcbiAgICogQHBhcmFtIGdldE9wdHNcbiAgICovXG4gIGFzeW5jIGdldFBhcnRpYWxPYmplY3QoXG4gICAgYnVja2V0TmFtZTogc3RyaW5nLFxuICAgIG9iamVjdE5hbWU6IHN0cmluZyxcbiAgICBvZmZzZXQ6IG51bWJlcixcbiAgICBsZW5ndGggPSAwLFxuICAgIGdldE9wdHM/OiBHZXRPYmplY3RPcHRzLFxuICApOiBQcm9taXNlPHN0cmVhbS5SZWFkYWJsZT4ge1xuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcignSW52YWxpZCBidWNrZXQgbmFtZTogJyArIGJ1Y2tldE5hbWUpXG4gICAgfVxuICAgIGlmICghaXNWYWxpZE9iamVjdE5hbWUob2JqZWN0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZE9iamVjdE5hbWVFcnJvcihgSW52YWxpZCBvYmplY3QgbmFtZTogJHtvYmplY3ROYW1lfWApXG4gICAgfVxuICAgIGlmICghaXNOdW1iZXIob2Zmc2V0KSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcignb2Zmc2V0IHNob3VsZCBiZSBvZiB0eXBlIFwibnVtYmVyXCInKVxuICAgIH1cbiAgICBpZiAoIWlzTnVtYmVyKGxlbmd0aCkpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ2xlbmd0aCBzaG91bGQgYmUgb2YgdHlwZSBcIm51bWJlclwiJylcbiAgICB9XG5cbiAgICBsZXQgcmFuZ2UgPSAnJ1xuICAgIGlmIChvZmZzZXQgfHwgbGVuZ3RoKSB7XG4gICAgICBpZiAob2Zmc2V0KSB7XG4gICAgICAgIHJhbmdlID0gYGJ5dGVzPSR7K29mZnNldH0tYFxuICAgICAgfSBlbHNlIHtcbiAgICAgICAgcmFuZ2UgPSAnYnl0ZXM9MC0nXG4gICAgICAgIG9mZnNldCA9IDBcbiAgICAgIH1cbiAgICAgIGlmIChsZW5ndGgpIHtcbiAgICAgICAgcmFuZ2UgKz0gYCR7K2xlbmd0aCArIG9mZnNldCAtIDF9YFxuICAgICAgfVxuICAgIH1cblxuICAgIGxldCBxdWVyeSA9ICcnXG4gICAgbGV0IGhlYWRlcnM6IFJlcXVlc3RIZWFkZXJzID0ge1xuICAgICAgLi4uKHJhbmdlICE9PSAnJyAmJiB7IHJhbmdlIH0pLFxuICAgIH1cblxuICAgIGlmIChnZXRPcHRzKSB7XG4gICAgICBjb25zdCBzc2VIZWFkZXJzOiBSZWNvcmQ8c3RyaW5nLCBzdHJpbmc+ID0ge1xuICAgICAgICAuLi4oZ2V0T3B0cy5TU0VDdXN0b21lckFsZ29yaXRobSAmJiB7XG4gICAgICAgICAgJ1gtQW16LVNlcnZlci1TaWRlLUVuY3J5cHRpb24tQ3VzdG9tZXItQWxnb3JpdGhtJzogZ2V0T3B0cy5TU0VDdXN0b21lckFsZ29yaXRobSxcbiAgICAgICAgfSksXG4gICAgICAgIC4uLihnZXRPcHRzLlNTRUN1c3RvbWVyS2V5ICYmIHsgJ1gtQW16LVNlcnZlci1TaWRlLUVuY3J5cHRpb24tQ3VzdG9tZXItS2V5JzogZ2V0T3B0cy5TU0VDdXN0b21lcktleSB9KSxcbiAgICAgICAgLi4uKGdldE9wdHMuU1NFQ3VzdG9tZXJLZXlNRDUgJiYge1xuICAgICAgICAgICdYLUFtei1TZXJ2ZXItU2lkZS1FbmNyeXB0aW9uLUN1c3RvbWVyLUtleS1NRDUnOiBnZXRPcHRzLlNTRUN1c3RvbWVyS2V5TUQ1LFxuICAgICAgICB9KSxcbiAgICAgIH1cbiAgICAgIHF1ZXJ5ID0gcXMuc3RyaW5naWZ5KGdldE9wdHMpXG4gICAgICBoZWFkZXJzID0ge1xuICAgICAgICAuLi5wcmVwZW5kWEFNWk1ldGEoc3NlSGVhZGVycyksXG4gICAgICAgIC4uLmhlYWRlcnMsXG4gICAgICB9XG4gICAgfVxuXG4gICAgY29uc3QgZXhwZWN0ZWRTdGF0dXNDb2RlcyA9IFsyMDBdXG4gICAgaWYgKHJhbmdlKSB7XG4gICAgICBleHBlY3RlZFN0YXR1c0NvZGVzLnB1c2goMjA2KVxuICAgIH1cbiAgICBjb25zdCBtZXRob2QgPSAnR0VUJ1xuXG4gICAgcmV0dXJuIGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luYyh7IG1ldGhvZCwgYnVja2V0TmFtZSwgb2JqZWN0TmFtZSwgaGVhZGVycywgcXVlcnkgfSwgJycsIGV4cGVjdGVkU3RhdHVzQ29kZXMpXG4gIH1cblxuICAvKipcbiAgICogZG93bmxvYWQgb2JqZWN0IGNvbnRlbnQgdG8gYSBmaWxlLlxuICAgKiBUaGlzIG1ldGhvZCB3aWxsIGNyZWF0ZSBhIHRlbXAgZmlsZSBuYW1lZCBgJHtmaWxlbmFtZX0uJHtiYXNlNjQoZXRhZyl9LnBhcnQubWluaW9gIHdoZW4gZG93bmxvYWRpbmcuXG4gICAqXG4gICAqIEBwYXJhbSBidWNrZXROYW1lIC0gbmFtZSBvZiB0aGUgYnVja2V0XG4gICAqIEBwYXJhbSBvYmplY3ROYW1lIC0gbmFtZSBvZiB0aGUgb2JqZWN0XG4gICAqIEBwYXJhbSBmaWxlUGF0aCAtIHBhdGggdG8gd2hpY2ggdGhlIG9iamVjdCBkYXRhIHdpbGwgYmUgd3JpdHRlbiB0b1xuICAgKiBAcGFyYW0gZ2V0T3B0cyAtIE9wdGlvbmFsIG9iamVjdCBnZXQgb3B0aW9uXG4gICAqL1xuICBhc3luYyBmR2V0T2JqZWN0KGJ1Y2tldE5hbWU6IHN0cmluZywgb2JqZWN0TmFtZTogc3RyaW5nLCBmaWxlUGF0aDogc3RyaW5nLCBnZXRPcHRzPzogR2V0T2JqZWN0T3B0cyk6IFByb21pc2U8dm9pZD4ge1xuICAgIC8vIElucHV0IHZhbGlkYXRpb24uXG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZShidWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKCdJbnZhbGlkIGJ1Y2tldCBuYW1lOiAnICsgYnVja2V0TmFtZSlcbiAgICB9XG4gICAgaWYgKCFpc1ZhbGlkT2JqZWN0TmFtZShvYmplY3ROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkT2JqZWN0TmFtZUVycm9yKGBJbnZhbGlkIG9iamVjdCBuYW1lOiAke29iamVjdE5hbWV9YClcbiAgICB9XG4gICAgaWYgKCFpc1N0cmluZyhmaWxlUGF0aCkpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ2ZpbGVQYXRoIHNob3VsZCBiZSBvZiB0eXBlIFwic3RyaW5nXCInKVxuICAgIH1cblxuICAgIGNvbnN0IGRvd25sb2FkVG9UbXBGaWxlID0gYXN5bmMgKCk6IFByb21pc2U8c3RyaW5nPiA9PiB7XG4gICAgICBsZXQgcGFydEZpbGVTdHJlYW06IHN0cmVhbS5Xcml0YWJsZVxuICAgICAgY29uc3Qgb2JqU3RhdCA9IGF3YWl0IHRoaXMuc3RhdE9iamVjdChidWNrZXROYW1lLCBvYmplY3ROYW1lLCBnZXRPcHRzKVxuICAgICAgY29uc3QgZW5jb2RlZEV0YWcgPSBCdWZmZXIuZnJvbShvYmpTdGF0LmV0YWcpLnRvU3RyaW5nKCdiYXNlNjQnKVxuICAgICAgY29uc3QgcGFydEZpbGUgPSBgJHtmaWxlUGF0aH0uJHtlbmNvZGVkRXRhZ30ucGFydC5taW5pb2BcblxuICAgICAgYXdhaXQgZnNwLm1rZGlyKHBhdGguZGlybmFtZShmaWxlUGF0aCksIHsgcmVjdXJzaXZlOiB0cnVlIH0pXG5cbiAgICAgIGxldCBvZmZzZXQgPSAwXG4gICAgICB0cnkge1xuICAgICAgICBjb25zdCBzdGF0cyA9IGF3YWl0IGZzcC5zdGF0KHBhcnRGaWxlKVxuICAgICAgICBpZiAob2JqU3RhdC5zaXplID09PSBzdGF0cy5zaXplKSB7XG4gICAgICAgICAgcmV0dXJuIHBhcnRGaWxlXG4gICAgICAgIH1cbiAgICAgICAgb2Zmc2V0ID0gc3RhdHMuc2l6ZVxuICAgICAgICBwYXJ0RmlsZVN0cmVhbSA9IGZzLmNyZWF0ZVdyaXRlU3RyZWFtKHBhcnRGaWxlLCB7IGZsYWdzOiAnYScgfSlcbiAgICAgIH0gY2F0Y2ggKGUpIHtcbiAgICAgICAgaWYgKGUgaW5zdGFuY2VvZiBFcnJvciAmJiAoZSBhcyB1bmtub3duIGFzIHsgY29kZTogc3RyaW5nIH0pLmNvZGUgPT09ICdFTk9FTlQnKSB7XG4gICAgICAgICAgLy8gZmlsZSBub3QgZXhpc3RcbiAgICAgICAgICBwYXJ0RmlsZVN0cmVhbSA9IGZzLmNyZWF0ZVdyaXRlU3RyZWFtKHBhcnRGaWxlLCB7IGZsYWdzOiAndycgfSlcbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICAvLyBvdGhlciBlcnJvciwgbWF5YmUgYWNjZXNzIGRlbnlcbiAgICAgICAgICB0aHJvdyBlXG4gICAgICAgIH1cbiAgICAgIH1cblxuICAgICAgY29uc3QgZG93bmxvYWRTdHJlYW0gPSBhd2FpdCB0aGlzLmdldFBhcnRpYWxPYmplY3QoYnVja2V0TmFtZSwgb2JqZWN0TmFtZSwgb2Zmc2V0LCAwLCBnZXRPcHRzKVxuXG4gICAgICBhd2FpdCBzdHJlYW1Qcm9taXNlLnBpcGVsaW5lKGRvd25sb2FkU3RyZWFtLCBwYXJ0RmlsZVN0cmVhbSlcbiAgICAgIGNvbnN0IHN0YXRzID0gYXdhaXQgZnNwLnN0YXQocGFydEZpbGUpXG4gICAgICBpZiAoc3RhdHMuc2l6ZSA9PT0gb2JqU3RhdC5zaXplKSB7XG4gICAgICAgIHJldHVybiBwYXJ0RmlsZVxuICAgICAgfVxuXG4gICAgICB0aHJvdyBuZXcgRXJyb3IoJ1NpemUgbWlzbWF0Y2ggYmV0d2VlbiBkb3dubG9hZGVkIGZpbGUgYW5kIHRoZSBvYmplY3QnKVxuICAgIH1cblxuICAgIGNvbnN0IHBhcnRGaWxlID0gYXdhaXQgZG93bmxvYWRUb1RtcEZpbGUoKVxuICAgIGF3YWl0IGZzcC5yZW5hbWUocGFydEZpbGUsIGZpbGVQYXRoKVxuICB9XG5cbiAgLyoqXG4gICAqIFN0YXQgaW5mb3JtYXRpb24gb2YgdGhlIG9iamVjdC5cbiAgICovXG4gIGFzeW5jIHN0YXRPYmplY3QoYnVja2V0TmFtZTogc3RyaW5nLCBvYmplY3ROYW1lOiBzdHJpbmcsIHN0YXRPcHRzPzogU3RhdE9iamVjdE9wdHMpOiBQcm9taXNlPEJ1Y2tldEl0ZW1TdGF0PiB7XG4gICAgY29uc3Qgc3RhdE9wdERlZiA9IHN0YXRPcHRzIHx8IHt9XG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZShidWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKCdJbnZhbGlkIGJ1Y2tldCBuYW1lOiAnICsgYnVja2V0TmFtZSlcbiAgICB9XG4gICAgaWYgKCFpc1ZhbGlkT2JqZWN0TmFtZShvYmplY3ROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkT2JqZWN0TmFtZUVycm9yKGBJbnZhbGlkIG9iamVjdCBuYW1lOiAke29iamVjdE5hbWV9YClcbiAgICB9XG5cbiAgICBpZiAoIWlzT2JqZWN0KHN0YXRPcHREZWYpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKCdzdGF0T3B0cyBzaG91bGQgYmUgb2YgdHlwZSBcIm9iamVjdFwiJylcbiAgICB9XG5cbiAgICBjb25zdCBxdWVyeSA9IHFzLnN0cmluZ2lmeShzdGF0T3B0RGVmKVxuICAgIGNvbnN0IG1ldGhvZCA9ICdIRUFEJ1xuICAgIGNvbnN0IHJlcyA9IGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luY09taXQoeyBtZXRob2QsIGJ1Y2tldE5hbWUsIG9iamVjdE5hbWUsIHF1ZXJ5IH0pXG5cbiAgICByZXR1cm4ge1xuICAgICAgc2l6ZTogcGFyc2VJbnQocmVzLmhlYWRlcnNbJ2NvbnRlbnQtbGVuZ3RoJ10gYXMgc3RyaW5nKSxcbiAgICAgIG1ldGFEYXRhOiBleHRyYWN0TWV0YWRhdGEocmVzLmhlYWRlcnMgYXMgUmVzcG9uc2VIZWFkZXIpLFxuICAgICAgbGFzdE1vZGlmaWVkOiBuZXcgRGF0ZShyZXMuaGVhZGVyc1snbGFzdC1tb2RpZmllZCddIGFzIHN0cmluZyksXG4gICAgICB2ZXJzaW9uSWQ6IGdldFZlcnNpb25JZChyZXMuaGVhZGVycyBhcyBSZXNwb25zZUhlYWRlciksXG4gICAgICBldGFnOiBzYW5pdGl6ZUVUYWcocmVzLmhlYWRlcnMuZXRhZyksXG4gICAgfVxuICB9XG5cbiAgYXN5bmMgcmVtb3ZlT2JqZWN0KGJ1Y2tldE5hbWU6IHN0cmluZywgb2JqZWN0TmFtZTogc3RyaW5nLCByZW1vdmVPcHRzPzogUmVtb3ZlT3B0aW9ucyk6IFByb21pc2U8dm9pZD4ge1xuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcihgSW52YWxpZCBidWNrZXQgbmFtZTogJHtidWNrZXROYW1lfWApXG4gICAgfVxuICAgIGlmICghaXNWYWxpZE9iamVjdE5hbWUob2JqZWN0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZE9iamVjdE5hbWVFcnJvcihgSW52YWxpZCBvYmplY3QgbmFtZTogJHtvYmplY3ROYW1lfWApXG4gICAgfVxuXG4gICAgaWYgKHJlbW92ZU9wdHMgJiYgIWlzT2JqZWN0KHJlbW92ZU9wdHMpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKCdyZW1vdmVPcHRzIHNob3VsZCBiZSBvZiB0eXBlIFwib2JqZWN0XCInKVxuICAgIH1cblxuICAgIGNvbnN0IG1ldGhvZCA9ICdERUxFVEUnXG5cbiAgICBjb25zdCBoZWFkZXJzOiBSZXF1ZXN0SGVhZGVycyA9IHt9XG4gICAgaWYgKHJlbW92ZU9wdHM/LmdvdmVybmFuY2VCeXBhc3MpIHtcbiAgICAgIGhlYWRlcnNbJ1gtQW16LUJ5cGFzcy1Hb3Zlcm5hbmNlLVJldGVudGlvbiddID0gdHJ1ZVxuICAgIH1cbiAgICBpZiAocmVtb3ZlT3B0cz8uZm9yY2VEZWxldGUpIHtcbiAgICAgIGhlYWRlcnNbJ3gtbWluaW8tZm9yY2UtZGVsZXRlJ10gPSB0cnVlXG4gICAgfVxuXG4gICAgY29uc3QgcXVlcnlQYXJhbXM6IFJlY29yZDxzdHJpbmcsIHN0cmluZz4gPSB7fVxuICAgIGlmIChyZW1vdmVPcHRzPy52ZXJzaW9uSWQpIHtcbiAgICAgIHF1ZXJ5UGFyYW1zLnZlcnNpb25JZCA9IGAke3JlbW92ZU9wdHMudmVyc2lvbklkfWBcbiAgICB9XG4gICAgY29uc3QgcXVlcnkgPSBxcy5zdHJpbmdpZnkocXVlcnlQYXJhbXMpXG5cbiAgICBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0QXN5bmNPbWl0KHsgbWV0aG9kLCBidWNrZXROYW1lLCBvYmplY3ROYW1lLCBoZWFkZXJzLCBxdWVyeSB9LCAnJywgWzIwMCwgMjA0XSlcbiAgfVxuXG4gIC8vIENhbGxzIGltcGxlbWVudGVkIGJlbG93IGFyZSByZWxhdGVkIHRvIG11bHRpcGFydC5cblxuICBsaXN0SW5jb21wbGV0ZVVwbG9hZHMoXG4gICAgYnVja2V0OiBzdHJpbmcsXG4gICAgcHJlZml4OiBzdHJpbmcsXG4gICAgcmVjdXJzaXZlOiBib29sZWFuLFxuICApOiBCdWNrZXRTdHJlYW08SW5jb21wbGV0ZVVwbG9hZGVkQnVja2V0SXRlbT4ge1xuICAgIGlmIChwcmVmaXggPT09IHVuZGVmaW5lZCkge1xuICAgICAgcHJlZml4ID0gJydcbiAgICB9XG4gICAgaWYgKHJlY3Vyc2l2ZSA9PT0gdW5kZWZpbmVkKSB7XG4gICAgICByZWN1cnNpdmUgPSBmYWxzZVxuICAgIH1cbiAgICBpZiAoIWlzVmFsaWRCdWNrZXROYW1lKGJ1Y2tldCkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcignSW52YWxpZCBidWNrZXQgbmFtZTogJyArIGJ1Y2tldClcbiAgICB9XG4gICAgaWYgKCFpc1ZhbGlkUHJlZml4KHByZWZpeCkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZFByZWZpeEVycm9yKGBJbnZhbGlkIHByZWZpeCA6ICR7cHJlZml4fWApXG4gICAgfVxuICAgIGlmICghaXNCb29sZWFuKHJlY3Vyc2l2ZSkpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ3JlY3Vyc2l2ZSBzaG91bGQgYmUgb2YgdHlwZSBcImJvb2xlYW5cIicpXG4gICAgfVxuICAgIGNvbnN0IGRlbGltaXRlciA9IHJlY3Vyc2l2ZSA/ICcnIDogJy8nXG4gICAgbGV0IGtleU1hcmtlciA9ICcnXG4gICAgbGV0IHVwbG9hZElkTWFya2VyID0gJydcbiAgICBjb25zdCB1cGxvYWRzOiB1bmtub3duW10gPSBbXVxuICAgIGxldCBlbmRlZCA9IGZhbHNlXG5cbiAgICAvLyBUT0RPOiByZWZhY3RvciB0aGlzIHdpdGggYXN5bmMvYXdhaXQgYW5kIGBzdHJlYW0uUmVhZGFibGUuZnJvbWBcbiAgICBjb25zdCByZWFkU3RyZWFtID0gbmV3IHN0cmVhbS5SZWFkYWJsZSh7IG9iamVjdE1vZGU6IHRydWUgfSlcbiAgICByZWFkU3RyZWFtLl9yZWFkID0gKCkgPT4ge1xuICAgICAgLy8gcHVzaCBvbmUgdXBsb2FkIGluZm8gcGVyIF9yZWFkKClcbiAgICAgIGlmICh1cGxvYWRzLmxlbmd0aCkge1xuICAgICAgICByZXR1cm4gcmVhZFN0cmVhbS5wdXNoKHVwbG9hZHMuc2hpZnQoKSlcbiAgICAgIH1cbiAgICAgIGlmIChlbmRlZCkge1xuICAgICAgICByZXR1cm4gcmVhZFN0cmVhbS5wdXNoKG51bGwpXG4gICAgICB9XG4gICAgICB0aGlzLmxpc3RJbmNvbXBsZXRlVXBsb2Fkc1F1ZXJ5KGJ1Y2tldCwgcHJlZml4LCBrZXlNYXJrZXIsIHVwbG9hZElkTWFya2VyLCBkZWxpbWl0ZXIpLnRoZW4oXG4gICAgICAgIChyZXN1bHQpID0+IHtcbiAgICAgICAgICAvLyBlc2xpbnQtZGlzYWJsZS1uZXh0LWxpbmUgQHR5cGVzY3JpcHQtZXNsaW50L2Jhbi10cy1jb21tZW50XG4gICAgICAgICAgLy8gQHRzLWlnbm9yZVxuICAgICAgICAgIHJlc3VsdC5wcmVmaXhlcy5mb3JFYWNoKChwcmVmaXgpID0+IHVwbG9hZHMucHVzaChwcmVmaXgpKVxuICAgICAgICAgIGFzeW5jLmVhY2hTZXJpZXMoXG4gICAgICAgICAgICByZXN1bHQudXBsb2FkcyxcbiAgICAgICAgICAgICh1cGxvYWQsIGNiKSA9PiB7XG4gICAgICAgICAgICAgIC8vIGZvciBlYWNoIGluY29tcGxldGUgdXBsb2FkIGFkZCB0aGUgc2l6ZXMgb2YgaXRzIHVwbG9hZGVkIHBhcnRzXG4gICAgICAgICAgICAgIC8vIGVzbGludC1kaXNhYmxlLW5leHQtbGluZSBAdHlwZXNjcmlwdC1lc2xpbnQvYmFuLXRzLWNvbW1lbnRcbiAgICAgICAgICAgICAgLy8gQHRzLWlnbm9yZVxuICAgICAgICAgICAgICB0aGlzLmxpc3RQYXJ0cyhidWNrZXQsIHVwbG9hZC5rZXksIHVwbG9hZC51cGxvYWRJZCkudGhlbihcbiAgICAgICAgICAgICAgICAocGFydHM6IFBhcnRbXSkgPT4ge1xuICAgICAgICAgICAgICAgICAgLy8gZXNsaW50LWRpc2FibGUtbmV4dC1saW5lIEB0eXBlc2NyaXB0LWVzbGludC9iYW4tdHMtY29tbWVudFxuICAgICAgICAgICAgICAgICAgLy8gQHRzLWlnbm9yZVxuICAgICAgICAgICAgICAgICAgdXBsb2FkLnNpemUgPSBwYXJ0cy5yZWR1Y2UoKGFjYywgaXRlbSkgPT4gYWNjICsgaXRlbS5zaXplLCAwKVxuICAgICAgICAgICAgICAgICAgdXBsb2Fkcy5wdXNoKHVwbG9hZClcbiAgICAgICAgICAgICAgICAgIGNiKClcbiAgICAgICAgICAgICAgICB9LFxuICAgICAgICAgICAgICAgIChlcnI6IEVycm9yKSA9PiBjYihlcnIpLFxuICAgICAgICAgICAgICApXG4gICAgICAgICAgICB9LFxuICAgICAgICAgICAgKGVycikgPT4ge1xuICAgICAgICAgICAgICBpZiAoZXJyKSB7XG4gICAgICAgICAgICAgICAgcmVhZFN0cmVhbS5lbWl0KCdlcnJvcicsIGVycilcbiAgICAgICAgICAgICAgICByZXR1cm5cbiAgICAgICAgICAgICAgfVxuICAgICAgICAgICAgICBpZiAocmVzdWx0LmlzVHJ1bmNhdGVkKSB7XG4gICAgICAgICAgICAgICAga2V5TWFya2VyID0gcmVzdWx0Lm5leHRLZXlNYXJrZXJcbiAgICAgICAgICAgICAgICB1cGxvYWRJZE1hcmtlciA9IHJlc3VsdC5uZXh0VXBsb2FkSWRNYXJrZXJcbiAgICAgICAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICAgICAgICBlbmRlZCA9IHRydWVcbiAgICAgICAgICAgICAgfVxuXG4gICAgICAgICAgICAgIC8vIGVzbGludC1kaXNhYmxlLW5leHQtbGluZSBAdHlwZXNjcmlwdC1lc2xpbnQvYmFuLXRzLWNvbW1lbnRcbiAgICAgICAgICAgICAgLy8gQHRzLWlnbm9yZVxuICAgICAgICAgICAgICByZWFkU3RyZWFtLl9yZWFkKClcbiAgICAgICAgICAgIH0sXG4gICAgICAgICAgKVxuICAgICAgICB9LFxuICAgICAgICAoZSkgPT4ge1xuICAgICAgICAgIHJlYWRTdHJlYW0uZW1pdCgnZXJyb3InLCBlKVxuICAgICAgICB9LFxuICAgICAgKVxuICAgIH1cbiAgICByZXR1cm4gcmVhZFN0cmVhbVxuICB9XG5cbiAgLyoqXG4gICAqIENhbGxlZCBieSBsaXN0SW5jb21wbGV0ZVVwbG9hZHMgdG8gZmV0Y2ggYSBiYXRjaCBvZiBpbmNvbXBsZXRlIHVwbG9hZHMuXG4gICAqL1xuICBhc3luYyBsaXN0SW5jb21wbGV0ZVVwbG9hZHNRdWVyeShcbiAgICBidWNrZXROYW1lOiBzdHJpbmcsXG4gICAgcHJlZml4OiBzdHJpbmcsXG4gICAga2V5TWFya2VyOiBzdHJpbmcsXG4gICAgdXBsb2FkSWRNYXJrZXI6IHN0cmluZyxcbiAgICBkZWxpbWl0ZXI6IHN0cmluZyxcbiAgKTogUHJvbWlzZTxMaXN0TXVsdGlwYXJ0UmVzdWx0PiB7XG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZShidWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKCdJbnZhbGlkIGJ1Y2tldCBuYW1lOiAnICsgYnVja2V0TmFtZSlcbiAgICB9XG4gICAgaWYgKCFpc1N0cmluZyhwcmVmaXgpKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdwcmVmaXggc2hvdWxkIGJlIG9mIHR5cGUgXCJzdHJpbmdcIicpXG4gICAgfVxuICAgIGlmICghaXNTdHJpbmcoa2V5TWFya2VyKSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcigna2V5TWFya2VyIHNob3VsZCBiZSBvZiB0eXBlIFwic3RyaW5nXCInKVxuICAgIH1cbiAgICBpZiAoIWlzU3RyaW5nKHVwbG9hZElkTWFya2VyKSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcigndXBsb2FkSWRNYXJrZXIgc2hvdWxkIGJlIG9mIHR5cGUgXCJzdHJpbmdcIicpXG4gICAgfVxuICAgIGlmICghaXNTdHJpbmcoZGVsaW1pdGVyKSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcignZGVsaW1pdGVyIHNob3VsZCBiZSBvZiB0eXBlIFwic3RyaW5nXCInKVxuICAgIH1cbiAgICBjb25zdCBxdWVyaWVzID0gW11cbiAgICBxdWVyaWVzLnB1c2goYHByZWZpeD0ke3VyaUVzY2FwZShwcmVmaXgpfWApXG4gICAgcXVlcmllcy5wdXNoKGBkZWxpbWl0ZXI9JHt1cmlFc2NhcGUoZGVsaW1pdGVyKX1gKVxuXG4gICAgaWYgKGtleU1hcmtlcikge1xuICAgICAgcXVlcmllcy5wdXNoKGBrZXktbWFya2VyPSR7dXJpRXNjYXBlKGtleU1hcmtlcil9YClcbiAgICB9XG4gICAgaWYgKHVwbG9hZElkTWFya2VyKSB7XG4gICAgICBxdWVyaWVzLnB1c2goYHVwbG9hZC1pZC1tYXJrZXI9JHt1cGxvYWRJZE1hcmtlcn1gKVxuICAgIH1cblxuICAgIGNvbnN0IG1heFVwbG9hZHMgPSAxMDAwXG4gICAgcXVlcmllcy5wdXNoKGBtYXgtdXBsb2Fkcz0ke21heFVwbG9hZHN9YClcbiAgICBxdWVyaWVzLnNvcnQoKVxuICAgIHF1ZXJpZXMudW5zaGlmdCgndXBsb2FkcycpXG4gICAgbGV0IHF1ZXJ5ID0gJydcbiAgICBpZiAocXVlcmllcy5sZW5ndGggPiAwKSB7XG4gICAgICBxdWVyeSA9IGAke3F1ZXJpZXMuam9pbignJicpfWBcbiAgICB9XG4gICAgY29uc3QgbWV0aG9kID0gJ0dFVCdcbiAgICBjb25zdCByZXMgPSBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0QXN5bmMoeyBtZXRob2QsIGJ1Y2tldE5hbWUsIHF1ZXJ5IH0pXG4gICAgY29uc3QgYm9keSA9IGF3YWl0IHJlYWRBc1N0cmluZyhyZXMpXG4gICAgcmV0dXJuIHhtbFBhcnNlcnMucGFyc2VMaXN0TXVsdGlwYXJ0KGJvZHkpXG4gIH1cblxuICAvKipcbiAgICogSW5pdGlhdGUgYSBuZXcgbXVsdGlwYXJ0IHVwbG9hZC5cbiAgICogQGludGVybmFsXG4gICAqL1xuICBhc3luYyBpbml0aWF0ZU5ld011bHRpcGFydFVwbG9hZChidWNrZXROYW1lOiBzdHJpbmcsIG9iamVjdE5hbWU6IHN0cmluZywgaGVhZGVyczogUmVxdWVzdEhlYWRlcnMpOiBQcm9taXNlPHN0cmluZz4ge1xuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcignSW52YWxpZCBidWNrZXQgbmFtZTogJyArIGJ1Y2tldE5hbWUpXG4gICAgfVxuICAgIGlmICghaXNWYWxpZE9iamVjdE5hbWUob2JqZWN0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZE9iamVjdE5hbWVFcnJvcihgSW52YWxpZCBvYmplY3QgbmFtZTogJHtvYmplY3ROYW1lfWApXG4gICAgfVxuICAgIGlmICghaXNPYmplY3QoaGVhZGVycykpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZE9iamVjdE5hbWVFcnJvcignY29udGVudFR5cGUgc2hvdWxkIGJlIG9mIHR5cGUgXCJvYmplY3RcIicpXG4gICAgfVxuICAgIGNvbnN0IG1ldGhvZCA9ICdQT1NUJ1xuICAgIGNvbnN0IHF1ZXJ5ID0gJ3VwbG9hZHMnXG4gICAgY29uc3QgcmVzID0gYXdhaXQgdGhpcy5tYWtlUmVxdWVzdEFzeW5jKHsgbWV0aG9kLCBidWNrZXROYW1lLCBvYmplY3ROYW1lLCBxdWVyeSwgaGVhZGVycyB9KVxuICAgIGNvbnN0IGJvZHkgPSBhd2FpdCByZWFkQXNCdWZmZXIocmVzKVxuICAgIHJldHVybiBwYXJzZUluaXRpYXRlTXVsdGlwYXJ0KGJvZHkudG9TdHJpbmcoKSlcbiAgfVxuXG4gIC8qKlxuICAgKiBJbnRlcm5hbCBNZXRob2QgdG8gYWJvcnQgYSBtdWx0aXBhcnQgdXBsb2FkIHJlcXVlc3QgaW4gY2FzZSBvZiBhbnkgZXJyb3JzLlxuICAgKlxuICAgKiBAcGFyYW0gYnVja2V0TmFtZSAtIEJ1Y2tldCBOYW1lXG4gICAqIEBwYXJhbSBvYmplY3ROYW1lIC0gT2JqZWN0IE5hbWVcbiAgICogQHBhcmFtIHVwbG9hZElkIC0gaWQgb2YgYSBtdWx0aXBhcnQgdXBsb2FkIHRvIGNhbmNlbCBkdXJpbmcgY29tcG9zZSBvYmplY3Qgc2VxdWVuY2UuXG4gICAqL1xuICBhc3luYyBhYm9ydE11bHRpcGFydFVwbG9hZChidWNrZXROYW1lOiBzdHJpbmcsIG9iamVjdE5hbWU6IHN0cmluZywgdXBsb2FkSWQ6IHN0cmluZyk6IFByb21pc2U8dm9pZD4ge1xuICAgIGNvbnN0IG1ldGhvZCA9ICdERUxFVEUnXG4gICAgY29uc3QgcXVlcnkgPSBgdXBsb2FkSWQ9JHt1cGxvYWRJZH1gXG5cbiAgICBjb25zdCByZXF1ZXN0T3B0aW9ucyA9IHsgbWV0aG9kLCBidWNrZXROYW1lLCBvYmplY3ROYW1lOiBvYmplY3ROYW1lLCBxdWVyeSB9XG4gICAgYXdhaXQgdGhpcy5tYWtlUmVxdWVzdEFzeW5jT21pdChyZXF1ZXN0T3B0aW9ucywgJycsIFsyMDRdKVxuICB9XG5cbiAgYXN5bmMgZmluZFVwbG9hZElkKGJ1Y2tldE5hbWU6IHN0cmluZywgb2JqZWN0TmFtZTogc3RyaW5nKTogUHJvbWlzZTxzdHJpbmcgfCB1bmRlZmluZWQ+IHtcbiAgICBpZiAoIWlzVmFsaWRCdWNrZXROYW1lKGJ1Y2tldE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IoJ0ludmFsaWQgYnVja2V0IG5hbWU6ICcgKyBidWNrZXROYW1lKVxuICAgIH1cbiAgICBpZiAoIWlzVmFsaWRPYmplY3ROYW1lKG9iamVjdE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRPYmplY3ROYW1lRXJyb3IoYEludmFsaWQgb2JqZWN0IG5hbWU6ICR7b2JqZWN0TmFtZX1gKVxuICAgIH1cblxuICAgIGxldCBsYXRlc3RVcGxvYWQ6IExpc3RNdWx0aXBhcnRSZXN1bHRbJ3VwbG9hZHMnXVtudW1iZXJdIHwgdW5kZWZpbmVkXG4gICAgbGV0IGtleU1hcmtlciA9ICcnXG4gICAgbGV0IHVwbG9hZElkTWFya2VyID0gJydcbiAgICBmb3IgKDs7KSB7XG4gICAgICBjb25zdCByZXN1bHQgPSBhd2FpdCB0aGlzLmxpc3RJbmNvbXBsZXRlVXBsb2Fkc1F1ZXJ5KGJ1Y2tldE5hbWUsIG9iamVjdE5hbWUsIGtleU1hcmtlciwgdXBsb2FkSWRNYXJrZXIsICcnKVxuICAgICAgZm9yIChjb25zdCB1cGxvYWQgb2YgcmVzdWx0LnVwbG9hZHMpIHtcbiAgICAgICAgaWYgKHVwbG9hZC5rZXkgPT09IG9iamVjdE5hbWUpIHtcbiAgICAgICAgICBpZiAoIWxhdGVzdFVwbG9hZCB8fCB1cGxvYWQuaW5pdGlhdGVkLmdldFRpbWUoKSA+IGxhdGVzdFVwbG9hZC5pbml0aWF0ZWQuZ2V0VGltZSgpKSB7XG4gICAgICAgICAgICBsYXRlc3RVcGxvYWQgPSB1cGxvYWRcbiAgICAgICAgICB9XG4gICAgICAgIH1cbiAgICAgIH1cbiAgICAgIGlmIChyZXN1bHQuaXNUcnVuY2F0ZWQpIHtcbiAgICAgICAga2V5TWFya2VyID0gcmVzdWx0Lm5leHRLZXlNYXJrZXJcbiAgICAgICAgdXBsb2FkSWRNYXJrZXIgPSByZXN1bHQubmV4dFVwbG9hZElkTWFya2VyXG4gICAgICAgIGNvbnRpbnVlXG4gICAgICB9XG5cbiAgICAgIGJyZWFrXG4gICAgfVxuICAgIHJldHVybiBsYXRlc3RVcGxvYWQ/LnVwbG9hZElkXG4gIH1cblxuICAvKipcbiAgICogdGhpcyBjYWxsIHdpbGwgYWdncmVnYXRlIHRoZSBwYXJ0cyBvbiB0aGUgc2VydmVyIGludG8gYSBzaW5nbGUgb2JqZWN0LlxuICAgKi9cbiAgYXN5bmMgY29tcGxldGVNdWx0aXBhcnRVcGxvYWQoXG4gICAgYnVja2V0TmFtZTogc3RyaW5nLFxuICAgIG9iamVjdE5hbWU6IHN0cmluZyxcbiAgICB1cGxvYWRJZDogc3RyaW5nLFxuICAgIGV0YWdzOiB7XG4gICAgICBwYXJ0OiBudW1iZXJcbiAgICAgIGV0YWc/OiBzdHJpbmdcbiAgICB9W10sXG4gICk6IFByb21pc2U8eyBldGFnOiBzdHJpbmc7IHZlcnNpb25JZDogc3RyaW5nIHwgbnVsbCB9PiB7XG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZShidWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKCdJbnZhbGlkIGJ1Y2tldCBuYW1lOiAnICsgYnVja2V0TmFtZSlcbiAgICB9XG4gICAgaWYgKCFpc1ZhbGlkT2JqZWN0TmFtZShvYmplY3ROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkT2JqZWN0TmFtZUVycm9yKGBJbnZhbGlkIG9iamVjdCBuYW1lOiAke29iamVjdE5hbWV9YClcbiAgICB9XG4gICAgaWYgKCFpc1N0cmluZyh1cGxvYWRJZCkpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ3VwbG9hZElkIHNob3VsZCBiZSBvZiB0eXBlIFwic3RyaW5nXCInKVxuICAgIH1cbiAgICBpZiAoIWlzT2JqZWN0KGV0YWdzKSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcignZXRhZ3Mgc2hvdWxkIGJlIG9mIHR5cGUgXCJBcnJheVwiJylcbiAgICB9XG5cbiAgICBpZiAoIXVwbG9hZElkKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKCd1cGxvYWRJZCBjYW5ub3QgYmUgZW1wdHknKVxuICAgIH1cblxuICAgIGNvbnN0IG1ldGhvZCA9ICdQT1NUJ1xuICAgIGNvbnN0IHF1ZXJ5ID0gYHVwbG9hZElkPSR7dXJpRXNjYXBlKHVwbG9hZElkKX1gXG5cbiAgICBjb25zdCBidWlsZGVyID0gbmV3IHhtbDJqcy5CdWlsZGVyKClcbiAgICBjb25zdCBwYXlsb2FkID0gYnVpbGRlci5idWlsZE9iamVjdCh7XG4gICAgICBDb21wbGV0ZU11bHRpcGFydFVwbG9hZDoge1xuICAgICAgICAkOiB7XG4gICAgICAgICAgeG1sbnM6ICdodHRwOi8vczMuYW1hem9uYXdzLmNvbS9kb2MvMjAwNi0wMy0wMS8nLFxuICAgICAgICB9LFxuICAgICAgICBQYXJ0OiBldGFncy5tYXAoKGV0YWcpID0+IHtcbiAgICAgICAgICByZXR1cm4ge1xuICAgICAgICAgICAgUGFydE51bWJlcjogZXRhZy5wYXJ0LFxuICAgICAgICAgICAgRVRhZzogZXRhZy5ldGFnLFxuICAgICAgICAgIH1cbiAgICAgICAgfSksXG4gICAgICB9LFxuICAgIH0pXG5cbiAgICBjb25zdCByZXMgPSBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0QXN5bmMoeyBtZXRob2QsIGJ1Y2tldE5hbWUsIG9iamVjdE5hbWUsIHF1ZXJ5IH0sIHBheWxvYWQpXG4gICAgY29uc3QgYm9keSA9IGF3YWl0IHJlYWRBc0J1ZmZlcihyZXMpXG4gICAgY29uc3QgcmVzdWx0ID0gcGFyc2VDb21wbGV0ZU11bHRpcGFydChib2R5LnRvU3RyaW5nKCkpXG4gICAgaWYgKCFyZXN1bHQpIHtcbiAgICAgIHRocm93IG5ldyBFcnJvcignQlVHOiBmYWlsZWQgdG8gcGFyc2Ugc2VydmVyIHJlc3BvbnNlJylcbiAgICB9XG5cbiAgICBpZiAocmVzdWx0LmVyckNvZGUpIHtcbiAgICAgIC8vIE11bHRpcGFydCBDb21wbGV0ZSBBUEkgcmV0dXJucyBhbiBlcnJvciBYTUwgYWZ0ZXIgYSAyMDAgaHR0cCBzdGF0dXNcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuUzNFcnJvcihyZXN1bHQuZXJyTWVzc2FnZSlcbiAgICB9XG5cbiAgICByZXR1cm4ge1xuICAgICAgLy8gZXNsaW50LWRpc2FibGUtbmV4dC1saW5lIEB0eXBlc2NyaXB0LWVzbGludC9iYW4tdHMtY29tbWVudFxuICAgICAgLy8gQHRzLWlnbm9yZVxuICAgICAgZXRhZzogcmVzdWx0LmV0YWcgYXMgc3RyaW5nLFxuICAgICAgdmVyc2lvbklkOiBnZXRWZXJzaW9uSWQocmVzLmhlYWRlcnMgYXMgUmVzcG9uc2VIZWFkZXIpLFxuICAgIH1cbiAgfVxuXG4gIC8qKlxuICAgKiBHZXQgcGFydC1pbmZvIG9mIGFsbCBwYXJ0cyBvZiBhbiBpbmNvbXBsZXRlIHVwbG9hZCBzcGVjaWZpZWQgYnkgdXBsb2FkSWQuXG4gICAqL1xuICBwcm90ZWN0ZWQgYXN5bmMgbGlzdFBhcnRzKGJ1Y2tldE5hbWU6IHN0cmluZywgb2JqZWN0TmFtZTogc3RyaW5nLCB1cGxvYWRJZDogc3RyaW5nKTogUHJvbWlzZTxVcGxvYWRlZFBhcnRbXT4ge1xuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcignSW52YWxpZCBidWNrZXQgbmFtZTogJyArIGJ1Y2tldE5hbWUpXG4gICAgfVxuICAgIGlmICghaXNWYWxpZE9iamVjdE5hbWUob2JqZWN0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZE9iamVjdE5hbWVFcnJvcihgSW52YWxpZCBvYmplY3QgbmFtZTogJHtvYmplY3ROYW1lfWApXG4gICAgfVxuICAgIGlmICghaXNTdHJpbmcodXBsb2FkSWQpKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCd1cGxvYWRJZCBzaG91bGQgYmUgb2YgdHlwZSBcInN0cmluZ1wiJylcbiAgICB9XG4gICAgaWYgKCF1cGxvYWRJZCkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQXJndW1lbnRFcnJvcigndXBsb2FkSWQgY2Fubm90IGJlIGVtcHR5JylcbiAgICB9XG5cbiAgICBjb25zdCBwYXJ0czogVXBsb2FkZWRQYXJ0W10gPSBbXVxuICAgIGxldCBtYXJrZXIgPSAwXG4gICAgbGV0IHJlc3VsdFxuICAgIGRvIHtcbiAgICAgIHJlc3VsdCA9IGF3YWl0IHRoaXMubGlzdFBhcnRzUXVlcnkoYnVja2V0TmFtZSwgb2JqZWN0TmFtZSwgdXBsb2FkSWQsIG1hcmtlcilcbiAgICAgIG1hcmtlciA9IHJlc3VsdC5tYXJrZXJcbiAgICAgIHBhcnRzLnB1c2goLi4ucmVzdWx0LnBhcnRzKVxuICAgIH0gd2hpbGUgKHJlc3VsdC5pc1RydW5jYXRlZClcblxuICAgIHJldHVybiBwYXJ0c1xuICB9XG5cbiAgLyoqXG4gICAqIENhbGxlZCBieSBsaXN0UGFydHMgdG8gZmV0Y2ggYSBiYXRjaCBvZiBwYXJ0LWluZm9cbiAgICovXG4gIHByaXZhdGUgYXN5bmMgbGlzdFBhcnRzUXVlcnkoYnVja2V0TmFtZTogc3RyaW5nLCBvYmplY3ROYW1lOiBzdHJpbmcsIHVwbG9hZElkOiBzdHJpbmcsIG1hcmtlcjogbnVtYmVyKSB7XG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZShidWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKCdJbnZhbGlkIGJ1Y2tldCBuYW1lOiAnICsgYnVja2V0TmFtZSlcbiAgICB9XG4gICAgaWYgKCFpc1ZhbGlkT2JqZWN0TmFtZShvYmplY3ROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkT2JqZWN0TmFtZUVycm9yKGBJbnZhbGlkIG9iamVjdCBuYW1lOiAke29iamVjdE5hbWV9YClcbiAgICB9XG4gICAgaWYgKCFpc1N0cmluZyh1cGxvYWRJZCkpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ3VwbG9hZElkIHNob3VsZCBiZSBvZiB0eXBlIFwic3RyaW5nXCInKVxuICAgIH1cbiAgICBpZiAoIWlzTnVtYmVyKG1hcmtlcikpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ21hcmtlciBzaG91bGQgYmUgb2YgdHlwZSBcIm51bWJlclwiJylcbiAgICB9XG4gICAgaWYgKCF1cGxvYWRJZCkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQXJndW1lbnRFcnJvcigndXBsb2FkSWQgY2Fubm90IGJlIGVtcHR5JylcbiAgICB9XG5cbiAgICBsZXQgcXVlcnkgPSBgdXBsb2FkSWQ9JHt1cmlFc2NhcGUodXBsb2FkSWQpfWBcbiAgICBpZiAobWFya2VyKSB7XG4gICAgICBxdWVyeSArPSBgJnBhcnQtbnVtYmVyLW1hcmtlcj0ke21hcmtlcn1gXG4gICAgfVxuXG4gICAgY29uc3QgbWV0aG9kID0gJ0dFVCdcbiAgICBjb25zdCByZXMgPSBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0QXN5bmMoeyBtZXRob2QsIGJ1Y2tldE5hbWUsIG9iamVjdE5hbWUsIHF1ZXJ5IH0pXG4gICAgcmV0dXJuIHhtbFBhcnNlcnMucGFyc2VMaXN0UGFydHMoYXdhaXQgcmVhZEFzU3RyaW5nKHJlcykpXG4gIH1cblxuICBhc3luYyBsaXN0QnVja2V0cygpOiBQcm9taXNlPEJ1Y2tldEl0ZW1Gcm9tTGlzdFtdPiB7XG4gICAgY29uc3QgbWV0aG9kID0gJ0dFVCdcbiAgICBjb25zdCByZWdpb25Db25mID0gdGhpcy5yZWdpb24gfHwgREVGQVVMVF9SRUdJT05cbiAgICBjb25zdCBodHRwUmVzID0gYXdhaXQgdGhpcy5tYWtlUmVxdWVzdEFzeW5jKHsgbWV0aG9kIH0sICcnLCBbMjAwXSwgcmVnaW9uQ29uZilcbiAgICBjb25zdCB4bWxSZXN1bHQgPSBhd2FpdCByZWFkQXNTdHJpbmcoaHR0cFJlcylcbiAgICByZXR1cm4geG1sUGFyc2Vycy5wYXJzZUxpc3RCdWNrZXQoeG1sUmVzdWx0KVxuICB9XG5cbiAgLyoqXG4gICAqIENhbGN1bGF0ZSBwYXJ0IHNpemUgZ2l2ZW4gdGhlIG9iamVjdCBzaXplLiBQYXJ0IHNpemUgd2lsbCBiZSBhdGxlYXN0IHRoaXMucGFydFNpemVcbiAgICovXG4gIGNhbGN1bGF0ZVBhcnRTaXplKHNpemU6IG51bWJlcikge1xuICAgIGlmICghaXNOdW1iZXIoc2l6ZSkpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ3NpemUgc2hvdWxkIGJlIG9mIHR5cGUgXCJudW1iZXJcIicpXG4gICAgfVxuICAgIGlmIChzaXplID4gdGhpcy5tYXhPYmplY3RTaXplKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKGBzaXplIHNob3VsZCBub3QgYmUgbW9yZSB0aGFuICR7dGhpcy5tYXhPYmplY3RTaXplfWApXG4gICAgfVxuICAgIGlmICh0aGlzLm92ZXJSaWRlUGFydFNpemUpIHtcbiAgICAgIHJldHVybiB0aGlzLnBhcnRTaXplXG4gICAgfVxuICAgIGxldCBwYXJ0U2l6ZSA9IHRoaXMucGFydFNpemVcbiAgICBmb3IgKDs7KSB7XG4gICAgICAvLyB3aGlsZSh0cnVlKSB7Li4ufSB0aHJvd3MgbGludGluZyBlcnJvci5cbiAgICAgIC8vIElmIHBhcnRTaXplIGlzIGJpZyBlbm91Z2ggdG8gYWNjb21vZGF0ZSB0aGUgb2JqZWN0IHNpemUsIHRoZW4gdXNlIGl0LlxuICAgICAgaWYgKHBhcnRTaXplICogMTAwMDAgPiBzaXplKSB7XG4gICAgICAgIHJldHVybiBwYXJ0U2l6ZVxuICAgICAgfVxuICAgICAgLy8gVHJ5IHBhcnQgc2l6ZXMgYXMgNjRNQiwgODBNQiwgOTZNQiBldGMuXG4gICAgICBwYXJ0U2l6ZSArPSAxNiAqIDEwMjQgKiAxMDI0XG4gICAgfVxuICB9XG5cbiAgLyoqXG4gICAqIFVwbG9hZHMgdGhlIG9iamVjdCB1c2luZyBjb250ZW50cyBmcm9tIGEgZmlsZVxuICAgKi9cbiAgYXN5bmMgZlB1dE9iamVjdChidWNrZXROYW1lOiBzdHJpbmcsIG9iamVjdE5hbWU6IHN0cmluZywgZmlsZVBhdGg6IHN0cmluZywgbWV0YURhdGE/OiBPYmplY3RNZXRhRGF0YSkge1xuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcignSW52YWxpZCBidWNrZXQgbmFtZTogJyArIGJ1Y2tldE5hbWUpXG4gICAgfVxuICAgIGlmICghaXNWYWxpZE9iamVjdE5hbWUob2JqZWN0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZE9iamVjdE5hbWVFcnJvcihgSW52YWxpZCBvYmplY3QgbmFtZTogJHtvYmplY3ROYW1lfWApXG4gICAgfVxuXG4gICAgaWYgKCFpc1N0cmluZyhmaWxlUGF0aCkpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ2ZpbGVQYXRoIHNob3VsZCBiZSBvZiB0eXBlIFwic3RyaW5nXCInKVxuICAgIH1cbiAgICBpZiAobWV0YURhdGEgJiYgIWlzT2JqZWN0KG1ldGFEYXRhKSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcignbWV0YURhdGEgc2hvdWxkIGJlIG9mIHR5cGUgXCJvYmplY3RcIicpXG4gICAgfVxuXG4gICAgLy8gSW5zZXJ0cyBjb3JyZWN0IGBjb250ZW50LXR5cGVgIGF0dHJpYnV0ZSBiYXNlZCBvbiBtZXRhRGF0YSBhbmQgZmlsZVBhdGhcbiAgICBtZXRhRGF0YSA9IGluc2VydENvbnRlbnRUeXBlKG1ldGFEYXRhIHx8IHt9LCBmaWxlUGF0aClcbiAgICBjb25zdCBzdGF0ID0gYXdhaXQgZnNwLnN0YXQoZmlsZVBhdGgpXG4gICAgcmV0dXJuIGF3YWl0IHRoaXMucHV0T2JqZWN0KGJ1Y2tldE5hbWUsIG9iamVjdE5hbWUsIGZzLmNyZWF0ZVJlYWRTdHJlYW0oZmlsZVBhdGgpLCBzdGF0LnNpemUsIG1ldGFEYXRhKVxuICB9XG5cbiAgLyoqXG4gICAqICBVcGxvYWRpbmcgYSBzdHJlYW0sIFwiQnVmZmVyXCIgb3IgXCJzdHJpbmdcIi5cbiAgICogIEl0J3MgcmVjb21tZW5kZWQgdG8gcGFzcyBgc2l6ZWAgYXJndW1lbnQgd2l0aCBzdHJlYW0uXG4gICAqL1xuICBhc3luYyBwdXRPYmplY3QoXG4gICAgYnVja2V0TmFtZTogc3RyaW5nLFxuICAgIG9iamVjdE5hbWU6IHN0cmluZyxcbiAgICBzdHJlYW06IHN0cmVhbS5SZWFkYWJsZSB8IEJ1ZmZlciB8IHN0cmluZyxcbiAgICBzaXplPzogbnVtYmVyLFxuICAgIG1ldGFEYXRhPzogSXRlbUJ1Y2tldE1ldGFkYXRhLFxuICApOiBQcm9taXNlPFVwbG9hZGVkT2JqZWN0SW5mbz4ge1xuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcihgSW52YWxpZCBidWNrZXQgbmFtZTogJHtidWNrZXROYW1lfWApXG4gICAgfVxuICAgIGlmICghaXNWYWxpZE9iamVjdE5hbWUob2JqZWN0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZE9iamVjdE5hbWVFcnJvcihgSW52YWxpZCBvYmplY3QgbmFtZTogJHtvYmplY3ROYW1lfWApXG4gICAgfVxuXG4gICAgLy8gV2UnbGwgbmVlZCB0byBzaGlmdCBhcmd1bWVudHMgdG8gdGhlIGxlZnQgYmVjYXVzZSBvZiBtZXRhRGF0YVxuICAgIC8vIGFuZCBzaXplIGJlaW5nIG9wdGlvbmFsLlxuICAgIGlmIChpc09iamVjdChzaXplKSkge1xuICAgICAgbWV0YURhdGEgPSBzaXplXG4gICAgfVxuICAgIC8vIEVuc3VyZXMgTWV0YWRhdGEgaGFzIGFwcHJvcHJpYXRlIHByZWZpeCBmb3IgQTMgQVBJXG4gICAgY29uc3QgaGVhZGVycyA9IHByZXBlbmRYQU1aTWV0YShtZXRhRGF0YSlcbiAgICBpZiAodHlwZW9mIHN0cmVhbSA9PT0gJ3N0cmluZycgfHwgc3RyZWFtIGluc3RhbmNlb2YgQnVmZmVyKSB7XG4gICAgICAvLyBBZGFwdHMgdGhlIG5vbi1zdHJlYW0gaW50ZXJmYWNlIGludG8gYSBzdHJlYW0uXG4gICAgICBzaXplID0gc3RyZWFtLmxlbmd0aFxuICAgICAgc3RyZWFtID0gcmVhZGFibGVTdHJlYW0oc3RyZWFtKVxuICAgIH0gZWxzZSBpZiAoIWlzUmVhZGFibGVTdHJlYW0oc3RyZWFtKSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcigndGhpcmQgYXJndW1lbnQgc2hvdWxkIGJlIG9mIHR5cGUgXCJzdHJlYW0uUmVhZGFibGVcIiBvciBcIkJ1ZmZlclwiIG9yIFwic3RyaW5nXCInKVxuICAgIH1cblxuICAgIGlmIChpc051bWJlcihzaXplKSAmJiBzaXplIDwgMCkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQXJndW1lbnRFcnJvcihgc2l6ZSBjYW5ub3QgYmUgbmVnYXRpdmUsIGdpdmVuIHNpemU6ICR7c2l6ZX1gKVxuICAgIH1cblxuICAgIC8vIEdldCB0aGUgcGFydCBzaXplIGFuZCBmb3J3YXJkIHRoYXQgdG8gdGhlIEJsb2NrU3RyZWFtLiBEZWZhdWx0IHRvIHRoZVxuICAgIC8vIGxhcmdlc3QgYmxvY2sgc2l6ZSBwb3NzaWJsZSBpZiBuZWNlc3NhcnkuXG4gICAgaWYgKCFpc051bWJlcihzaXplKSkge1xuICAgICAgc2l6ZSA9IHRoaXMubWF4T2JqZWN0U2l6ZVxuICAgIH1cblxuICAgIC8vIEdldCB0aGUgcGFydCBzaXplIGFuZCBmb3J3YXJkIHRoYXQgdG8gdGhlIEJsb2NrU3RyZWFtLiBEZWZhdWx0IHRvIHRoZVxuICAgIC8vIGxhcmdlc3QgYmxvY2sgc2l6ZSBwb3NzaWJsZSBpZiBuZWNlc3NhcnkuXG4gICAgaWYgKHNpemUgPT09IHVuZGVmaW5lZCkge1xuICAgICAgY29uc3Qgc3RhdFNpemUgPSBhd2FpdCBnZXRDb250ZW50TGVuZ3RoKHN0cmVhbSlcbiAgICAgIGlmIChzdGF0U2l6ZSAhPT0gbnVsbCkge1xuICAgICAgICBzaXplID0gc3RhdFNpemVcbiAgICAgIH1cbiAgICB9XG5cbiAgICBpZiAoIWlzTnVtYmVyKHNpemUpKSB7XG4gICAgICAvLyBCYWNrd2FyZCBjb21wYXRpYmlsaXR5XG4gICAgICBzaXplID0gdGhpcy5tYXhPYmplY3RTaXplXG4gICAgfVxuICAgIGlmIChzaXplID09PSAwKSB7XG4gICAgICByZXR1cm4gdGhpcy51cGxvYWRCdWZmZXIoYnVja2V0TmFtZSwgb2JqZWN0TmFtZSwgaGVhZGVycywgQnVmZmVyLmZyb20oJycpKVxuICAgIH1cblxuICAgIGNvbnN0IHBhcnRTaXplID0gdGhpcy5jYWxjdWxhdGVQYXJ0U2l6ZShzaXplKVxuICAgIGlmICh0eXBlb2Ygc3RyZWFtID09PSAnc3RyaW5nJyB8fCBCdWZmZXIuaXNCdWZmZXIoc3RyZWFtKSB8fCBzaXplIDw9IHBhcnRTaXplKSB7XG4gICAgICBjb25zdCBidWYgPSBpc1JlYWRhYmxlU3RyZWFtKHN0cmVhbSkgPyBhd2FpdCByZWFkQXNCdWZmZXIoc3RyZWFtKSA6IEJ1ZmZlci5mcm9tKHN0cmVhbSlcbiAgICAgIHJldHVybiB0aGlzLnVwbG9hZEJ1ZmZlcihidWNrZXROYW1lLCBvYmplY3ROYW1lLCBoZWFkZXJzLCBidWYpXG4gICAgfVxuXG4gICAgcmV0dXJuIHRoaXMudXBsb2FkU3RyZWFtKGJ1Y2tldE5hbWUsIG9iamVjdE5hbWUsIGhlYWRlcnMsIHN0cmVhbSwgcGFydFNpemUpXG4gIH1cblxuICAvKipcbiAgICogbWV0aG9kIHRvIHVwbG9hZCBidWZmZXIgaW4gb25lIGNhbGxcbiAgICogQHByaXZhdGVcbiAgICovXG4gIHByaXZhdGUgYXN5bmMgdXBsb2FkQnVmZmVyKFxuICAgIGJ1Y2tldE5hbWU6IHN0cmluZyxcbiAgICBvYmplY3ROYW1lOiBzdHJpbmcsXG4gICAgaGVhZGVyczogUmVxdWVzdEhlYWRlcnMsXG4gICAgYnVmOiBCdWZmZXIsXG4gICk6IFByb21pc2U8VXBsb2FkZWRPYmplY3RJbmZvPiB7XG4gICAgY29uc3QgeyBtZDVzdW0sIHNoYTI1NnN1bSB9ID0gaGFzaEJpbmFyeShidWYsIHRoaXMuZW5hYmxlU0hBMjU2KVxuICAgIGhlYWRlcnNbJ0NvbnRlbnQtTGVuZ3RoJ10gPSBidWYubGVuZ3RoXG4gICAgaWYgKCF0aGlzLmVuYWJsZVNIQTI1Nikge1xuICAgICAgaGVhZGVyc1snQ29udGVudC1NRDUnXSA9IG1kNXN1bVxuICAgIH1cbiAgICBjb25zdCByZXMgPSBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0U3RyZWFtQXN5bmMoXG4gICAgICB7XG4gICAgICAgIG1ldGhvZDogJ1BVVCcsXG4gICAgICAgIGJ1Y2tldE5hbWUsXG4gICAgICAgIG9iamVjdE5hbWUsXG4gICAgICAgIGhlYWRlcnMsXG4gICAgICB9LFxuICAgICAgYnVmLFxuICAgICAgc2hhMjU2c3VtLFxuICAgICAgWzIwMF0sXG4gICAgICAnJyxcbiAgICApXG4gICAgYXdhaXQgZHJhaW5SZXNwb25zZShyZXMpXG4gICAgcmV0dXJuIHtcbiAgICAgIGV0YWc6IHNhbml0aXplRVRhZyhyZXMuaGVhZGVycy5ldGFnKSxcbiAgICAgIHZlcnNpb25JZDogZ2V0VmVyc2lvbklkKHJlcy5oZWFkZXJzIGFzIFJlc3BvbnNlSGVhZGVyKSxcbiAgICB9XG4gIH1cblxuICAvKipcbiAgICogdXBsb2FkIHN0cmVhbSB3aXRoIE11bHRpcGFydFVwbG9hZFxuICAgKiBAcHJpdmF0ZVxuICAgKi9cbiAgcHJpdmF0ZSBhc3luYyB1cGxvYWRTdHJlYW0oXG4gICAgYnVja2V0TmFtZTogc3RyaW5nLFxuICAgIG9iamVjdE5hbWU6IHN0cmluZyxcbiAgICBoZWFkZXJzOiBSZXF1ZXN0SGVhZGVycyxcbiAgICBib2R5OiBzdHJlYW0uUmVhZGFibGUsXG4gICAgcGFydFNpemU6IG51bWJlcixcbiAgKTogUHJvbWlzZTxVcGxvYWRlZE9iamVjdEluZm8+IHtcbiAgICAvLyBBIG1hcCBvZiB0aGUgcHJldmlvdXNseSB1cGxvYWRlZCBjaHVua3MsIGZvciByZXN1bWluZyBhIGZpbGUgdXBsb2FkLiBUaGlzXG4gICAgLy8gd2lsbCBiZSBudWxsIGlmIHdlIGFyZW4ndCByZXN1bWluZyBhbiB1cGxvYWQuXG4gICAgY29uc3Qgb2xkUGFydHM6IFJlY29yZDxudW1iZXIsIFBhcnQ+ID0ge31cblxuICAgIC8vIEtlZXAgdHJhY2sgb2YgdGhlIGV0YWdzIGZvciBhZ2dyZWdhdGluZyB0aGUgY2h1bmtzIHRvZ2V0aGVyIGxhdGVyLiBFYWNoXG4gICAgLy8gZXRhZyByZXByZXNlbnRzIGEgc2luZ2xlIGNodW5rIG9mIHRoZSBmaWxlLlxuICAgIGNvbnN0IGVUYWdzOiBQYXJ0W10gPSBbXVxuXG4gICAgY29uc3QgcHJldmlvdXNVcGxvYWRJZCA9IGF3YWl0IHRoaXMuZmluZFVwbG9hZElkKGJ1Y2tldE5hbWUsIG9iamVjdE5hbWUpXG4gICAgbGV0IHVwbG9hZElkOiBzdHJpbmdcbiAgICBpZiAoIXByZXZpb3VzVXBsb2FkSWQpIHtcbiAgICAgIHVwbG9hZElkID0gYXdhaXQgdGhpcy5pbml0aWF0ZU5ld011bHRpcGFydFVwbG9hZChidWNrZXROYW1lLCBvYmplY3ROYW1lLCBoZWFkZXJzKVxuICAgIH0gZWxzZSB7XG4gICAgICB1cGxvYWRJZCA9IHByZXZpb3VzVXBsb2FkSWRcbiAgICAgIGNvbnN0IG9sZFRhZ3MgPSBhd2FpdCB0aGlzLmxpc3RQYXJ0cyhidWNrZXROYW1lLCBvYmplY3ROYW1lLCBwcmV2aW91c1VwbG9hZElkKVxuICAgICAgb2xkVGFncy5mb3JFYWNoKChlKSA9PiB7XG4gICAgICAgIG9sZFBhcnRzW2UucGFydF0gPSBlXG4gICAgICB9KVxuICAgIH1cblxuICAgIGlmICghaXNSZWFkYWJsZVN0cmVhbShib2R5KSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcigndGhpcmQgYXJndW1lbnQgc2hvdWxkIGJlIG9mIHR5cGUgXCJzdHJlYW0uUmVhZGFibGVcIiBvciBcIkJ1ZmZlclwiIG9yIFwic3RyaW5nXCInKVxuICAgIH1cblxuICAgIGNvbnN0IGNodW5raWVyID0gbmV3IEJsb2NrU3RyZWFtMih7IHNpemU6IHBhcnRTaXplLCB6ZXJvUGFkZGluZzogZmFsc2UgfSlcblxuICAgIC8vIGVzbGludC1kaXNhYmxlLW5leHQtbGluZSBAdHlwZXNjcmlwdC1lc2xpbnQvbm8tdW51c2VkLXZhcnNcbiAgICBjb25zdCBbXywgb10gPSBhd2FpdCBQcm9taXNlLmFsbChbXG4gICAgICBuZXcgUHJvbWlzZSgocmVzb2x2ZSwgcmVqZWN0KSA9PiB7XG4gICAgICAgIGJvZHkucGlwZShjaHVua2llcikub24oJ2Vycm9yJywgcmVqZWN0KVxuICAgICAgICBjaHVua2llci5vbignZW5kJywgcmVzb2x2ZSkub24oJ2Vycm9yJywgcmVqZWN0KVxuICAgICAgfSksXG4gICAgICAoYXN5bmMgKCkgPT4ge1xuICAgICAgICBsZXQgcGFydE51bWJlciA9IDBcblxuICAgICAgICBmb3IgYXdhaXQgKGNvbnN0IGNodW5rIG9mIGNodW5raWVyKSB7XG4gICAgICAgICAgY29uc3QgbWQ1ID0gY3J5cHRvLmNyZWF0ZUhhc2goJ21kNScpLnVwZGF0ZShjaHVuaykuZGlnZXN0KClcblxuICAgICAgICAgIHBhcnROdW1iZXIrK1xuICAgICAgICAgIGNvbnN0IG9sZFBhcnQgPSBvbGRQYXJ0c1twYXJ0TnVtYmVyXVxuICAgICAgICAgIGlmIChvbGRQYXJ0KSB7XG4gICAgICAgICAgICBpZiAob2xkUGFydC5ldGFnID09PSBtZDUudG9TdHJpbmcoJ2hleCcpKSB7XG4gICAgICAgICAgICAgIGVUYWdzLnB1c2goeyBwYXJ0OiBwYXJ0TnVtYmVyLCBldGFnOiBvbGRQYXJ0LmV0YWcgfSlcbiAgICAgICAgICAgICAgY29udGludWVcbiAgICAgICAgICAgIH1cbiAgICAgICAgICB9XG5cbiAgICAgICAgICAvLyBub3cgc3RhcnQgdG8gdXBsb2FkIG1pc3NpbmcgcGFydFxuICAgICAgICAgIGNvbnN0IG9wdGlvbnM6IFJlcXVlc3RPcHRpb24gPSB7XG4gICAgICAgICAgICBtZXRob2Q6ICdQVVQnLFxuICAgICAgICAgICAgcXVlcnk6IHFzLnN0cmluZ2lmeSh7IHBhcnROdW1iZXIsIHVwbG9hZElkIH0pLFxuICAgICAgICAgICAgaGVhZGVyczoge1xuICAgICAgICAgICAgICAnQ29udGVudC1MZW5ndGgnOiBjaHVuay5sZW5ndGgsXG4gICAgICAgICAgICAgICdDb250ZW50LU1ENSc6IG1kNS50b1N0cmluZygnYmFzZTY0JyksXG4gICAgICAgICAgICB9LFxuICAgICAgICAgICAgYnVja2V0TmFtZSxcbiAgICAgICAgICAgIG9iamVjdE5hbWUsXG4gICAgICAgICAgfVxuXG4gICAgICAgICAgY29uc3QgcmVzcG9uc2UgPSBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0QXN5bmNPbWl0KG9wdGlvbnMsIGNodW5rKVxuXG4gICAgICAgICAgbGV0IGV0YWcgPSByZXNwb25zZS5oZWFkZXJzLmV0YWdcbiAgICAgICAgICBpZiAoZXRhZykge1xuICAgICAgICAgICAgZXRhZyA9IGV0YWcucmVwbGFjZSgvXlwiLywgJycpLnJlcGxhY2UoL1wiJC8sICcnKVxuICAgICAgICAgIH0gZWxzZSB7XG4gICAgICAgICAgICBldGFnID0gJydcbiAgICAgICAgICB9XG5cbiAgICAgICAgICBlVGFncy5wdXNoKHsgcGFydDogcGFydE51bWJlciwgZXRhZyB9KVxuXG4gICAgICAgICAgcGFydE51bWJlcisrXG4gICAgICAgIH1cblxuICAgICAgICBpZiAoZVRhZ3MubGVuZ3RoID09PSAwKSB7XG4gICAgICAgICAgYXdhaXQgdGhpcy5hYm9ydE11bHRpcGFydFVwbG9hZChidWNrZXROYW1lLCBvYmplY3ROYW1lLCB1cGxvYWRJZClcbiAgICAgICAgICByZXR1cm4gYXdhaXQgdGhpcy51cGxvYWRCdWZmZXIoYnVja2V0TmFtZSwgb2JqZWN0TmFtZSwgaGVhZGVycywgQnVmZmVyLmZyb20oJycpKVxuICAgICAgICB9XG5cbiAgICAgICAgcmV0dXJuIGF3YWl0IHRoaXMuY29tcGxldGVNdWx0aXBhcnRVcGxvYWQoYnVja2V0TmFtZSwgb2JqZWN0TmFtZSwgdXBsb2FkSWQsIGVUYWdzKVxuICAgICAgfSkoKSxcbiAgICBdKVxuXG4gICAgcmV0dXJuIG9cbiAgfVxuXG4gIGFzeW5jIHJlbW92ZUJ1Y2tldFJlcGxpY2F0aW9uKGJ1Y2tldE5hbWU6IHN0cmluZyk6IFByb21pc2U8dm9pZD5cbiAgcmVtb3ZlQnVja2V0UmVwbGljYXRpb24oYnVja2V0TmFtZTogc3RyaW5nLCBjYWxsYmFjazogTm9SZXN1bHRDYWxsYmFjayk6IHZvaWRcbiAgYXN5bmMgcmVtb3ZlQnVja2V0UmVwbGljYXRpb24oYnVja2V0TmFtZTogc3RyaW5nKTogUHJvbWlzZTx2b2lkPiB7XG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZShidWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKCdJbnZhbGlkIGJ1Y2tldCBuYW1lOiAnICsgYnVja2V0TmFtZSlcbiAgICB9XG4gICAgY29uc3QgbWV0aG9kID0gJ0RFTEVURSdcbiAgICBjb25zdCBxdWVyeSA9ICdyZXBsaWNhdGlvbidcbiAgICBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0QXN5bmNPbWl0KHsgbWV0aG9kLCBidWNrZXROYW1lLCBxdWVyeSB9LCAnJywgWzIwMCwgMjA0XSwgJycpXG4gIH1cblxuICBzZXRCdWNrZXRSZXBsaWNhdGlvbihidWNrZXROYW1lOiBzdHJpbmcsIHJlcGxpY2F0aW9uQ29uZmlnOiBSZXBsaWNhdGlvbkNvbmZpZ09wdHMpOiB2b2lkXG4gIGFzeW5jIHNldEJ1Y2tldFJlcGxpY2F0aW9uKGJ1Y2tldE5hbWU6IHN0cmluZywgcmVwbGljYXRpb25Db25maWc6IFJlcGxpY2F0aW9uQ29uZmlnT3B0cyk6IFByb21pc2U8dm9pZD5cbiAgYXN5bmMgc2V0QnVja2V0UmVwbGljYXRpb24oYnVja2V0TmFtZTogc3RyaW5nLCByZXBsaWNhdGlvbkNvbmZpZzogUmVwbGljYXRpb25Db25maWdPcHRzKSB7XG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZShidWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKCdJbnZhbGlkIGJ1Y2tldCBuYW1lOiAnICsgYnVja2V0TmFtZSlcbiAgICB9XG4gICAgaWYgKCFpc09iamVjdChyZXBsaWNhdGlvbkNvbmZpZykpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEFyZ3VtZW50RXJyb3IoJ3JlcGxpY2F0aW9uQ29uZmlnIHNob3VsZCBiZSBvZiB0eXBlIFwib2JqZWN0XCInKVxuICAgIH0gZWxzZSB7XG4gICAgICBpZiAoXy5pc0VtcHR5KHJlcGxpY2F0aW9uQ29uZmlnLnJvbGUpKSB7XG4gICAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEFyZ3VtZW50RXJyb3IoJ1JvbGUgY2Fubm90IGJlIGVtcHR5JylcbiAgICAgIH0gZWxzZSBpZiAocmVwbGljYXRpb25Db25maWcucm9sZSAmJiAhaXNTdHJpbmcocmVwbGljYXRpb25Db25maWcucm9sZSkpIHtcbiAgICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQXJndW1lbnRFcnJvcignSW52YWxpZCB2YWx1ZSBmb3Igcm9sZScsIHJlcGxpY2F0aW9uQ29uZmlnLnJvbGUpXG4gICAgICB9XG4gICAgICBpZiAoXy5pc0VtcHR5KHJlcGxpY2F0aW9uQ29uZmlnLnJ1bGVzKSkge1xuICAgICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKCdNaW5pbXVtIG9uZSByZXBsaWNhdGlvbiBydWxlIG11c3QgYmUgc3BlY2lmaWVkJylcbiAgICAgIH1cbiAgICB9XG4gICAgY29uc3QgbWV0aG9kID0gJ1BVVCdcbiAgICBjb25zdCBxdWVyeSA9ICdyZXBsaWNhdGlvbidcbiAgICBjb25zdCBoZWFkZXJzOiBSZWNvcmQ8c3RyaW5nLCBzdHJpbmc+ID0ge31cblxuICAgIGNvbnN0IHJlcGxpY2F0aW9uUGFyYW1zQ29uZmlnID0ge1xuICAgICAgUmVwbGljYXRpb25Db25maWd1cmF0aW9uOiB7XG4gICAgICAgIFJvbGU6IHJlcGxpY2F0aW9uQ29uZmlnLnJvbGUsXG4gICAgICAgIFJ1bGU6IHJlcGxpY2F0aW9uQ29uZmlnLnJ1bGVzLFxuICAgICAgfSxcbiAgICB9XG5cbiAgICBjb25zdCBidWlsZGVyID0gbmV3IHhtbDJqcy5CdWlsZGVyKHsgcmVuZGVyT3B0czogeyBwcmV0dHk6IGZhbHNlIH0sIGhlYWRsZXNzOiB0cnVlIH0pXG4gICAgY29uc3QgcGF5bG9hZCA9IGJ1aWxkZXIuYnVpbGRPYmplY3QocmVwbGljYXRpb25QYXJhbXNDb25maWcpXG4gICAgaGVhZGVyc1snQ29udGVudC1NRDUnXSA9IHRvTWQ1KHBheWxvYWQpXG4gICAgYXdhaXQgdGhpcy5tYWtlUmVxdWVzdEFzeW5jT21pdCh7IG1ldGhvZCwgYnVja2V0TmFtZSwgcXVlcnksIGhlYWRlcnMgfSwgcGF5bG9hZClcbiAgfVxuXG4gIGdldEJ1Y2tldFJlcGxpY2F0aW9uKGJ1Y2tldE5hbWU6IHN0cmluZyk6IHZvaWRcbiAgYXN5bmMgZ2V0QnVja2V0UmVwbGljYXRpb24oYnVja2V0TmFtZTogc3RyaW5nKTogUHJvbWlzZTxSZXBsaWNhdGlvbkNvbmZpZz5cbiAgYXN5bmMgZ2V0QnVja2V0UmVwbGljYXRpb24oYnVja2V0TmFtZTogc3RyaW5nKSB7XG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZShidWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKCdJbnZhbGlkIGJ1Y2tldCBuYW1lOiAnICsgYnVja2V0TmFtZSlcbiAgICB9XG4gICAgY29uc3QgbWV0aG9kID0gJ0dFVCdcbiAgICBjb25zdCBxdWVyeSA9ICdyZXBsaWNhdGlvbidcblxuICAgIGNvbnN0IGh0dHBSZXMgPSBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0QXN5bmMoeyBtZXRob2QsIGJ1Y2tldE5hbWUsIHF1ZXJ5IH0sICcnLCBbMjAwLCAyMDRdKVxuICAgIGNvbnN0IHhtbFJlc3VsdCA9IGF3YWl0IHJlYWRBc1N0cmluZyhodHRwUmVzKVxuICAgIHJldHVybiB4bWxQYXJzZXJzLnBhcnNlUmVwbGljYXRpb25Db25maWcoeG1sUmVzdWx0KVxuICB9XG5cbiAgZ2V0T2JqZWN0TGVnYWxIb2xkKFxuICAgIGJ1Y2tldE5hbWU6IHN0cmluZyxcbiAgICBvYmplY3ROYW1lOiBzdHJpbmcsXG4gICAgZ2V0T3B0cz86IEdldE9iamVjdExlZ2FsSG9sZE9wdGlvbnMsXG4gICAgY2FsbGJhY2s/OiBSZXN1bHRDYWxsYmFjazxMRUdBTF9IT0xEX1NUQVRVUz4sXG4gICk6IFByb21pc2U8TEVHQUxfSE9MRF9TVEFUVVM+XG4gIGFzeW5jIGdldE9iamVjdExlZ2FsSG9sZChcbiAgICBidWNrZXROYW1lOiBzdHJpbmcsXG4gICAgb2JqZWN0TmFtZTogc3RyaW5nLFxuICAgIGdldE9wdHM/OiBHZXRPYmplY3RMZWdhbEhvbGRPcHRpb25zLFxuICApOiBQcm9taXNlPExFR0FMX0hPTERfU1RBVFVTPiB7XG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZShidWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKCdJbnZhbGlkIGJ1Y2tldCBuYW1lOiAnICsgYnVja2V0TmFtZSlcbiAgICB9XG4gICAgaWYgKCFpc1ZhbGlkT2JqZWN0TmFtZShvYmplY3ROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkT2JqZWN0TmFtZUVycm9yKGBJbnZhbGlkIG9iamVjdCBuYW1lOiAke29iamVjdE5hbWV9YClcbiAgICB9XG5cbiAgICBpZiAoZ2V0T3B0cykge1xuICAgICAgaWYgKCFpc09iamVjdChnZXRPcHRzKSkge1xuICAgICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdnZXRPcHRzIHNob3VsZCBiZSBvZiB0eXBlIFwiT2JqZWN0XCInKVxuICAgICAgfSBlbHNlIGlmIChPYmplY3Qua2V5cyhnZXRPcHRzKS5sZW5ndGggPiAwICYmIGdldE9wdHMudmVyc2lvbklkICYmICFpc1N0cmluZyhnZXRPcHRzLnZlcnNpb25JZCkpIHtcbiAgICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcigndmVyc2lvbklkIHNob3VsZCBiZSBvZiB0eXBlIHN0cmluZy46JywgZ2V0T3B0cy52ZXJzaW9uSWQpXG4gICAgICB9XG4gICAgfVxuXG4gICAgY29uc3QgbWV0aG9kID0gJ0dFVCdcbiAgICBsZXQgcXVlcnkgPSAnbGVnYWwtaG9sZCdcblxuICAgIGlmIChnZXRPcHRzPy52ZXJzaW9uSWQpIHtcbiAgICAgIHF1ZXJ5ICs9IGAmdmVyc2lvbklkPSR7Z2V0T3B0cy52ZXJzaW9uSWR9YFxuICAgIH1cblxuICAgIGNvbnN0IGh0dHBSZXMgPSBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0QXN5bmMoeyBtZXRob2QsIGJ1Y2tldE5hbWUsIG9iamVjdE5hbWUsIHF1ZXJ5IH0sICcnLCBbMjAwXSlcbiAgICBjb25zdCBzdHJSZXMgPSBhd2FpdCByZWFkQXNTdHJpbmcoaHR0cFJlcylcbiAgICByZXR1cm4gcGFyc2VPYmplY3RMZWdhbEhvbGRDb25maWcoc3RyUmVzKVxuICB9XG5cbiAgc2V0T2JqZWN0TGVnYWxIb2xkKGJ1Y2tldE5hbWU6IHN0cmluZywgb2JqZWN0TmFtZTogc3RyaW5nLCBzZXRPcHRzPzogUHV0T2JqZWN0TGVnYWxIb2xkT3B0aW9ucyk6IHZvaWRcbiAgYXN5bmMgc2V0T2JqZWN0TGVnYWxIb2xkKFxuICAgIGJ1Y2tldE5hbWU6IHN0cmluZyxcbiAgICBvYmplY3ROYW1lOiBzdHJpbmcsXG4gICAgc2V0T3B0cyA9IHtcbiAgICAgIHN0YXR1czogTEVHQUxfSE9MRF9TVEFUVVMuRU5BQkxFRCxcbiAgICB9IGFzIFB1dE9iamVjdExlZ2FsSG9sZE9wdGlvbnMsXG4gICk6IFByb21pc2U8dm9pZD4ge1xuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcignSW52YWxpZCBidWNrZXQgbmFtZTogJyArIGJ1Y2tldE5hbWUpXG4gICAgfVxuICAgIGlmICghaXNWYWxpZE9iamVjdE5hbWUob2JqZWN0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZE9iamVjdE5hbWVFcnJvcihgSW52YWxpZCBvYmplY3QgbmFtZTogJHtvYmplY3ROYW1lfWApXG4gICAgfVxuXG4gICAgaWYgKCFpc09iamVjdChzZXRPcHRzKSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcignc2V0T3B0cyBzaG91bGQgYmUgb2YgdHlwZSBcIk9iamVjdFwiJylcbiAgICB9IGVsc2Uge1xuICAgICAgaWYgKCFbTEVHQUxfSE9MRF9TVEFUVVMuRU5BQkxFRCwgTEVHQUxfSE9MRF9TVEFUVVMuRElTQUJMRURdLmluY2x1ZGVzKHNldE9wdHM/LnN0YXR1cykpIHtcbiAgICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcignSW52YWxpZCBzdGF0dXM6ICcgKyBzZXRPcHRzLnN0YXR1cylcbiAgICAgIH1cbiAgICAgIGlmIChzZXRPcHRzLnZlcnNpb25JZCAmJiAhc2V0T3B0cy52ZXJzaW9uSWQubGVuZ3RoKSB7XG4gICAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ3ZlcnNpb25JZCBzaG91bGQgYmUgb2YgdHlwZSBzdHJpbmcuOicgKyBzZXRPcHRzLnZlcnNpb25JZClcbiAgICAgIH1cbiAgICB9XG5cbiAgICBjb25zdCBtZXRob2QgPSAnUFVUJ1xuICAgIGxldCBxdWVyeSA9ICdsZWdhbC1ob2xkJ1xuXG4gICAgaWYgKHNldE9wdHMudmVyc2lvbklkKSB7XG4gICAgICBxdWVyeSArPSBgJnZlcnNpb25JZD0ke3NldE9wdHMudmVyc2lvbklkfWBcbiAgICB9XG5cbiAgICBjb25zdCBjb25maWcgPSB7XG4gICAgICBTdGF0dXM6IHNldE9wdHMuc3RhdHVzLFxuICAgIH1cblxuICAgIGNvbnN0IGJ1aWxkZXIgPSBuZXcgeG1sMmpzLkJ1aWxkZXIoeyByb290TmFtZTogJ0xlZ2FsSG9sZCcsIHJlbmRlck9wdHM6IHsgcHJldHR5OiBmYWxzZSB9LCBoZWFkbGVzczogdHJ1ZSB9KVxuICAgIGNvbnN0IHBheWxvYWQgPSBidWlsZGVyLmJ1aWxkT2JqZWN0KGNvbmZpZylcbiAgICBjb25zdCBoZWFkZXJzOiBSZWNvcmQ8c3RyaW5nLCBzdHJpbmc+ID0ge31cbiAgICBoZWFkZXJzWydDb250ZW50LU1ENSddID0gdG9NZDUocGF5bG9hZClcblxuICAgIGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luY09taXQoeyBtZXRob2QsIGJ1Y2tldE5hbWUsIG9iamVjdE5hbWUsIHF1ZXJ5LCBoZWFkZXJzIH0sIHBheWxvYWQpXG4gIH1cblxuICAvKipcbiAgICogR2V0IFRhZ3MgYXNzb2NpYXRlZCB3aXRoIGEgQnVja2V0XG4gICAqL1xuICBhc3luYyBnZXRCdWNrZXRUYWdnaW5nKGJ1Y2tldE5hbWU6IHN0cmluZyk6IFByb21pc2U8VGFnW10+IHtcbiAgICBpZiAoIWlzVmFsaWRCdWNrZXROYW1lKGJ1Y2tldE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IoYEludmFsaWQgYnVja2V0IG5hbWU6ICR7YnVja2V0TmFtZX1gKVxuICAgIH1cblxuICAgIGNvbnN0IG1ldGhvZCA9ICdHRVQnXG4gICAgY29uc3QgcXVlcnkgPSAndGFnZ2luZydcbiAgICBjb25zdCByZXF1ZXN0T3B0aW9ucyA9IHsgbWV0aG9kLCBidWNrZXROYW1lLCBxdWVyeSB9XG5cbiAgICBjb25zdCByZXNwb25zZSA9IGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luYyhyZXF1ZXN0T3B0aW9ucylcbiAgICBjb25zdCBib2R5ID0gYXdhaXQgcmVhZEFzU3RyaW5nKHJlc3BvbnNlKVxuICAgIHJldHVybiB4bWxQYXJzZXJzLnBhcnNlVGFnZ2luZyhib2R5KVxuICB9XG5cbiAgLyoqXG4gICAqICBHZXQgdGhlIHRhZ3MgYXNzb2NpYXRlZCB3aXRoIGEgYnVja2V0IE9SIGFuIG9iamVjdFxuICAgKi9cbiAgYXN5bmMgZ2V0T2JqZWN0VGFnZ2luZyhidWNrZXROYW1lOiBzdHJpbmcsIG9iamVjdE5hbWU6IHN0cmluZywgZ2V0T3B0cz86IEdldE9iamVjdE9wdHMpOiBQcm9taXNlPFRhZ1tdPiB7XG4gICAgY29uc3QgbWV0aG9kID0gJ0dFVCdcbiAgICBsZXQgcXVlcnkgPSAndGFnZ2luZydcblxuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcignSW52YWxpZCBidWNrZXQgbmFtZTogJyArIGJ1Y2tldE5hbWUpXG4gICAgfVxuICAgIGlmICghaXNWYWxpZE9iamVjdE5hbWUob2JqZWN0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcignSW52YWxpZCBvYmplY3QgbmFtZTogJyArIG9iamVjdE5hbWUpXG4gICAgfVxuICAgIGlmIChnZXRPcHRzICYmICFpc09iamVjdChnZXRPcHRzKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQXJndW1lbnRFcnJvcignZ2V0T3B0cyBzaG91bGQgYmUgb2YgdHlwZSBcIm9iamVjdFwiJylcbiAgICB9XG5cbiAgICBpZiAoZ2V0T3B0cyAmJiBnZXRPcHRzLnZlcnNpb25JZCkge1xuICAgICAgcXVlcnkgPSBgJHtxdWVyeX0mdmVyc2lvbklkPSR7Z2V0T3B0cy52ZXJzaW9uSWR9YFxuICAgIH1cbiAgICBjb25zdCByZXF1ZXN0T3B0aW9uczogUmVxdWVzdE9wdGlvbiA9IHsgbWV0aG9kLCBidWNrZXROYW1lLCBxdWVyeSB9XG4gICAgaWYgKG9iamVjdE5hbWUpIHtcbiAgICAgIHJlcXVlc3RPcHRpb25zWydvYmplY3ROYW1lJ10gPSBvYmplY3ROYW1lXG4gICAgfVxuXG4gICAgY29uc3QgcmVzcG9uc2UgPSBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0QXN5bmMocmVxdWVzdE9wdGlvbnMpXG4gICAgY29uc3QgYm9keSA9IGF3YWl0IHJlYWRBc1N0cmluZyhyZXNwb25zZSlcbiAgICByZXR1cm4geG1sUGFyc2Vycy5wYXJzZVRhZ2dpbmcoYm9keSlcbiAgfVxuXG4gIC8qKlxuICAgKiAgU2V0IHRoZSBwb2xpY3kgb24gYSBidWNrZXQgb3IgYW4gb2JqZWN0IHByZWZpeC5cbiAgICovXG4gIGFzeW5jIHNldEJ1Y2tldFBvbGljeShidWNrZXROYW1lOiBzdHJpbmcsIHBvbGljeTogc3RyaW5nKTogUHJvbWlzZTx2b2lkPiB7XG4gICAgLy8gVmFsaWRhdGUgYXJndW1lbnRzLlxuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcihgSW52YWxpZCBidWNrZXQgbmFtZTogJHtidWNrZXROYW1lfWApXG4gICAgfVxuICAgIGlmICghaXNTdHJpbmcocG9saWN5KSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0UG9saWN5RXJyb3IoYEludmFsaWQgYnVja2V0IHBvbGljeTogJHtwb2xpY3l9IC0gbXVzdCBiZSBcInN0cmluZ1wiYClcbiAgICB9XG5cbiAgICBjb25zdCBxdWVyeSA9ICdwb2xpY3knXG5cbiAgICBsZXQgbWV0aG9kID0gJ0RFTEVURSdcbiAgICBpZiAocG9saWN5KSB7XG4gICAgICBtZXRob2QgPSAnUFVUJ1xuICAgIH1cblxuICAgIGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luY09taXQoeyBtZXRob2QsIGJ1Y2tldE5hbWUsIHF1ZXJ5IH0sIHBvbGljeSwgWzIwNF0sICcnKVxuICB9XG5cbiAgLyoqXG4gICAqIEdldCB0aGUgcG9saWN5IG9uIGEgYnVja2V0IG9yIGFuIG9iamVjdCBwcmVmaXguXG4gICAqL1xuICBhc3luYyBnZXRCdWNrZXRQb2xpY3koYnVja2V0TmFtZTogc3RyaW5nKTogUHJvbWlzZTxzdHJpbmc+IHtcbiAgICAvLyBWYWxpZGF0ZSBhcmd1bWVudHMuXG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZShidWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKGBJbnZhbGlkIGJ1Y2tldCBuYW1lOiAke2J1Y2tldE5hbWV9YClcbiAgICB9XG5cbiAgICBjb25zdCBtZXRob2QgPSAnR0VUJ1xuICAgIGNvbnN0IHF1ZXJ5ID0gJ3BvbGljeSdcbiAgICBjb25zdCByZXMgPSBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0QXN5bmMoeyBtZXRob2QsIGJ1Y2tldE5hbWUsIHF1ZXJ5IH0pXG4gICAgcmV0dXJuIGF3YWl0IHJlYWRBc1N0cmluZyhyZXMpXG4gIH1cblxuICBhc3luYyBwdXRPYmplY3RSZXRlbnRpb24oYnVja2V0TmFtZTogc3RyaW5nLCBvYmplY3ROYW1lOiBzdHJpbmcsIHJldGVudGlvbk9wdHM6IFJldGVudGlvbiA9IHt9KTogUHJvbWlzZTx2b2lkPiB7XG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZShidWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKGBJbnZhbGlkIGJ1Y2tldCBuYW1lOiAke2J1Y2tldE5hbWV9YClcbiAgICB9XG4gICAgaWYgKCFpc1ZhbGlkT2JqZWN0TmFtZShvYmplY3ROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkT2JqZWN0TmFtZUVycm9yKGBJbnZhbGlkIG9iamVjdCBuYW1lOiAke29iamVjdE5hbWV9YClcbiAgICB9XG4gICAgaWYgKCFpc09iamVjdChyZXRlbnRpb25PcHRzKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQXJndW1lbnRFcnJvcigncmV0ZW50aW9uT3B0cyBzaG91bGQgYmUgb2YgdHlwZSBcIm9iamVjdFwiJylcbiAgICB9IGVsc2Uge1xuICAgICAgaWYgKHJldGVudGlvbk9wdHMuZ292ZXJuYW5jZUJ5cGFzcyAmJiAhaXNCb29sZWFuKHJldGVudGlvbk9wdHMuZ292ZXJuYW5jZUJ5cGFzcykpIHtcbiAgICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQXJndW1lbnRFcnJvcihgSW52YWxpZCB2YWx1ZSBmb3IgZ292ZXJuYW5jZUJ5cGFzczogJHtyZXRlbnRpb25PcHRzLmdvdmVybmFuY2VCeXBhc3N9YClcbiAgICAgIH1cbiAgICAgIGlmIChcbiAgICAgICAgcmV0ZW50aW9uT3B0cy5tb2RlICYmXG4gICAgICAgICFbUkVURU5USU9OX01PREVTLkNPTVBMSUFOQ0UsIFJFVEVOVElPTl9NT0RFUy5HT1ZFUk5BTkNFXS5pbmNsdWRlcyhyZXRlbnRpb25PcHRzLm1vZGUpXG4gICAgICApIHtcbiAgICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQXJndW1lbnRFcnJvcihgSW52YWxpZCBvYmplY3QgcmV0ZW50aW9uIG1vZGU6ICR7cmV0ZW50aW9uT3B0cy5tb2RlfWApXG4gICAgICB9XG4gICAgICBpZiAocmV0ZW50aW9uT3B0cy5yZXRhaW5VbnRpbERhdGUgJiYgIWlzU3RyaW5nKHJldGVudGlvbk9wdHMucmV0YWluVW50aWxEYXRlKSkge1xuICAgICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKGBJbnZhbGlkIHZhbHVlIGZvciByZXRhaW5VbnRpbERhdGU6ICR7cmV0ZW50aW9uT3B0cy5yZXRhaW5VbnRpbERhdGV9YClcbiAgICAgIH1cbiAgICAgIGlmIChyZXRlbnRpb25PcHRzLnZlcnNpb25JZCAmJiAhaXNTdHJpbmcocmV0ZW50aW9uT3B0cy52ZXJzaW9uSWQpKSB7XG4gICAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEFyZ3VtZW50RXJyb3IoYEludmFsaWQgdmFsdWUgZm9yIHZlcnNpb25JZDogJHtyZXRlbnRpb25PcHRzLnZlcnNpb25JZH1gKVxuICAgICAgfVxuICAgIH1cblxuICAgIGNvbnN0IG1ldGhvZCA9ICdQVVQnXG4gICAgbGV0IHF1ZXJ5ID0gJ3JldGVudGlvbidcblxuICAgIGNvbnN0IGhlYWRlcnM6IFJlcXVlc3RIZWFkZXJzID0ge31cbiAgICBpZiAocmV0ZW50aW9uT3B0cy5nb3Zlcm5hbmNlQnlwYXNzKSB7XG4gICAgICBoZWFkZXJzWydYLUFtei1CeXBhc3MtR292ZXJuYW5jZS1SZXRlbnRpb24nXSA9IHRydWVcbiAgICB9XG5cbiAgICBjb25zdCBidWlsZGVyID0gbmV3IHhtbDJqcy5CdWlsZGVyKHsgcm9vdE5hbWU6ICdSZXRlbnRpb24nLCByZW5kZXJPcHRzOiB7IHByZXR0eTogZmFsc2UgfSwgaGVhZGxlc3M6IHRydWUgfSlcbiAgICBjb25zdCBwYXJhbXM6IFJlY29yZDxzdHJpbmcsIHN0cmluZz4gPSB7fVxuXG4gICAgaWYgKHJldGVudGlvbk9wdHMubW9kZSkge1xuICAgICAgcGFyYW1zLk1vZGUgPSByZXRlbnRpb25PcHRzLm1vZGVcbiAgICB9XG4gICAgaWYgKHJldGVudGlvbk9wdHMucmV0YWluVW50aWxEYXRlKSB7XG4gICAgICBwYXJhbXMuUmV0YWluVW50aWxEYXRlID0gcmV0ZW50aW9uT3B0cy5yZXRhaW5VbnRpbERhdGVcbiAgICB9XG4gICAgaWYgKHJldGVudGlvbk9wdHMudmVyc2lvbklkKSB7XG4gICAgICBxdWVyeSArPSBgJnZlcnNpb25JZD0ke3JldGVudGlvbk9wdHMudmVyc2lvbklkfWBcbiAgICB9XG5cbiAgICBjb25zdCBwYXlsb2FkID0gYnVpbGRlci5idWlsZE9iamVjdChwYXJhbXMpXG5cbiAgICBoZWFkZXJzWydDb250ZW50LU1ENSddID0gdG9NZDUocGF5bG9hZClcbiAgICBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0QXN5bmNPbWl0KHsgbWV0aG9kLCBidWNrZXROYW1lLCBvYmplY3ROYW1lLCBxdWVyeSwgaGVhZGVycyB9LCBwYXlsb2FkLCBbMjAwLCAyMDRdKVxuICB9XG5cbiAgZ2V0T2JqZWN0TG9ja0NvbmZpZyhidWNrZXROYW1lOiBzdHJpbmcsIGNhbGxiYWNrOiBSZXN1bHRDYWxsYmFjazxPYmplY3RMb2NrSW5mbz4pOiB2b2lkXG4gIGdldE9iamVjdExvY2tDb25maWcoYnVja2V0TmFtZTogc3RyaW5nKTogdm9pZFxuICBhc3luYyBnZXRPYmplY3RMb2NrQ29uZmlnKGJ1Y2tldE5hbWU6IHN0cmluZyk6IFByb21pc2U8T2JqZWN0TG9ja0luZm8+XG4gIGFzeW5jIGdldE9iamVjdExvY2tDb25maWcoYnVja2V0TmFtZTogc3RyaW5nKSB7XG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZShidWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKCdJbnZhbGlkIGJ1Y2tldCBuYW1lOiAnICsgYnVja2V0TmFtZSlcbiAgICB9XG4gICAgY29uc3QgbWV0aG9kID0gJ0dFVCdcbiAgICBjb25zdCBxdWVyeSA9ICdvYmplY3QtbG9jaydcblxuICAgIGNvbnN0IGh0dHBSZXMgPSBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0QXN5bmMoeyBtZXRob2QsIGJ1Y2tldE5hbWUsIHF1ZXJ5IH0pXG4gICAgY29uc3QgeG1sUmVzdWx0ID0gYXdhaXQgcmVhZEFzU3RyaW5nKGh0dHBSZXMpXG4gICAgcmV0dXJuIHhtbFBhcnNlcnMucGFyc2VPYmplY3RMb2NrQ29uZmlnKHhtbFJlc3VsdClcbiAgfVxuXG4gIHNldE9iamVjdExvY2tDb25maWcoYnVja2V0TmFtZTogc3RyaW5nLCBsb2NrQ29uZmlnT3B0czogT21pdDxPYmplY3RMb2NrSW5mbywgJ29iamVjdExvY2tFbmFibGVkJz4pOiB2b2lkXG4gIGFzeW5jIHNldE9iamVjdExvY2tDb25maWcoXG4gICAgYnVja2V0TmFtZTogc3RyaW5nLFxuICAgIGxvY2tDb25maWdPcHRzOiBPbWl0PE9iamVjdExvY2tJbmZvLCAnb2JqZWN0TG9ja0VuYWJsZWQnPixcbiAgKTogUHJvbWlzZTx2b2lkPlxuICBhc3luYyBzZXRPYmplY3RMb2NrQ29uZmlnKGJ1Y2tldE5hbWU6IHN0cmluZywgbG9ja0NvbmZpZ09wdHM6IE9taXQ8T2JqZWN0TG9ja0luZm8sICdvYmplY3RMb2NrRW5hYmxlZCc+KSB7XG4gICAgY29uc3QgcmV0ZW50aW9uTW9kZXMgPSBbUkVURU5USU9OX01PREVTLkNPTVBMSUFOQ0UsIFJFVEVOVElPTl9NT0RFUy5HT1ZFUk5BTkNFXVxuICAgIGNvbnN0IHZhbGlkVW5pdHMgPSBbUkVURU5USU9OX1ZBTElESVRZX1VOSVRTLkRBWVMsIFJFVEVOVElPTl9WQUxJRElUWV9VTklUUy5ZRUFSU11cblxuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcignSW52YWxpZCBidWNrZXQgbmFtZTogJyArIGJ1Y2tldE5hbWUpXG4gICAgfVxuXG4gICAgaWYgKGxvY2tDb25maWdPcHRzLm1vZGUgJiYgIXJldGVudGlvbk1vZGVzLmluY2x1ZGVzKGxvY2tDb25maWdPcHRzLm1vZGUpKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKGBsb2NrQ29uZmlnT3B0cy5tb2RlIHNob3VsZCBiZSBvbmUgb2YgJHtyZXRlbnRpb25Nb2Rlc31gKVxuICAgIH1cbiAgICBpZiAobG9ja0NvbmZpZ09wdHMudW5pdCAmJiAhdmFsaWRVbml0cy5pbmNsdWRlcyhsb2NrQ29uZmlnT3B0cy51bml0KSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcihgbG9ja0NvbmZpZ09wdHMudW5pdCBzaG91bGQgYmUgb25lIG9mICR7dmFsaWRVbml0c31gKVxuICAgIH1cbiAgICBpZiAobG9ja0NvbmZpZ09wdHMudmFsaWRpdHkgJiYgIWlzTnVtYmVyKGxvY2tDb25maWdPcHRzLnZhbGlkaXR5KSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcihgbG9ja0NvbmZpZ09wdHMudmFsaWRpdHkgc2hvdWxkIGJlIGEgbnVtYmVyYClcbiAgICB9XG5cbiAgICBjb25zdCBtZXRob2QgPSAnUFVUJ1xuICAgIGNvbnN0IHF1ZXJ5ID0gJ29iamVjdC1sb2NrJ1xuXG4gICAgY29uc3QgY29uZmlnOiBPYmplY3RMb2NrQ29uZmlnUGFyYW0gPSB7XG4gICAgICBPYmplY3RMb2NrRW5hYmxlZDogJ0VuYWJsZWQnLFxuICAgIH1cbiAgICBjb25zdCBjb25maWdLZXlzID0gT2JqZWN0LmtleXMobG9ja0NvbmZpZ09wdHMpXG5cbiAgICBjb25zdCBpc0FsbEtleXNTZXQgPSBbJ3VuaXQnLCAnbW9kZScsICd2YWxpZGl0eSddLmV2ZXJ5KChsY2spID0+IGNvbmZpZ0tleXMuaW5jbHVkZXMobGNrKSlcbiAgICAvLyBDaGVjayBpZiBrZXlzIGFyZSBwcmVzZW50IGFuZCBhbGwga2V5cyBhcmUgcHJlc2VudC5cbiAgICBpZiAoY29uZmlnS2V5cy5sZW5ndGggPiAwKSB7XG4gICAgICBpZiAoIWlzQWxsS2V5c1NldCkge1xuICAgICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKFxuICAgICAgICAgIGBsb2NrQ29uZmlnT3B0cy5tb2RlLGxvY2tDb25maWdPcHRzLnVuaXQsbG9ja0NvbmZpZ09wdHMudmFsaWRpdHkgYWxsIHRoZSBwcm9wZXJ0aWVzIHNob3VsZCBiZSBzcGVjaWZpZWQuYCxcbiAgICAgICAgKVxuICAgICAgfSBlbHNlIHtcbiAgICAgICAgY29uZmlnLlJ1bGUgPSB7XG4gICAgICAgICAgRGVmYXVsdFJldGVudGlvbjoge30sXG4gICAgICAgIH1cbiAgICAgICAgaWYgKGxvY2tDb25maWdPcHRzLm1vZGUpIHtcbiAgICAgICAgICBjb25maWcuUnVsZS5EZWZhdWx0UmV0ZW50aW9uLk1vZGUgPSBsb2NrQ29uZmlnT3B0cy5tb2RlXG4gICAgICAgIH1cbiAgICAgICAgaWYgKGxvY2tDb25maWdPcHRzLnVuaXQgPT09IFJFVEVOVElPTl9WQUxJRElUWV9VTklUUy5EQVlTKSB7XG4gICAgICAgICAgY29uZmlnLlJ1bGUuRGVmYXVsdFJldGVudGlvbi5EYXlzID0gbG9ja0NvbmZpZ09wdHMudmFsaWRpdHlcbiAgICAgICAgfSBlbHNlIGlmIChsb2NrQ29uZmlnT3B0cy51bml0ID09PSBSRVRFTlRJT05fVkFMSURJVFlfVU5JVFMuWUVBUlMpIHtcbiAgICAgICAgICBjb25maWcuUnVsZS5EZWZhdWx0UmV0ZW50aW9uLlllYXJzID0gbG9ja0NvbmZpZ09wdHMudmFsaWRpdHlcbiAgICAgICAgfVxuICAgICAgfVxuICAgIH1cblxuICAgIGNvbnN0IGJ1aWxkZXIgPSBuZXcgeG1sMmpzLkJ1aWxkZXIoe1xuICAgICAgcm9vdE5hbWU6ICdPYmplY3RMb2NrQ29uZmlndXJhdGlvbicsXG4gICAgICByZW5kZXJPcHRzOiB7IHByZXR0eTogZmFsc2UgfSxcbiAgICAgIGhlYWRsZXNzOiB0cnVlLFxuICAgIH0pXG4gICAgY29uc3QgcGF5bG9hZCA9IGJ1aWxkZXIuYnVpbGRPYmplY3QoY29uZmlnKVxuXG4gICAgY29uc3QgaGVhZGVyczogUmVxdWVzdEhlYWRlcnMgPSB7fVxuICAgIGhlYWRlcnNbJ0NvbnRlbnQtTUQ1J10gPSB0b01kNShwYXlsb2FkKVxuXG4gICAgYXdhaXQgdGhpcy5tYWtlUmVxdWVzdEFzeW5jT21pdCh7IG1ldGhvZCwgYnVja2V0TmFtZSwgcXVlcnksIGhlYWRlcnMgfSwgcGF5bG9hZClcbiAgfVxuXG4gIGFzeW5jIGdldEJ1Y2tldFZlcnNpb25pbmcoYnVja2V0TmFtZTogc3RyaW5nKTogUHJvbWlzZTxCdWNrZXRWZXJzaW9uaW5nQ29uZmlndXJhdGlvbj4ge1xuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcignSW52YWxpZCBidWNrZXQgbmFtZTogJyArIGJ1Y2tldE5hbWUpXG4gICAgfVxuICAgIGNvbnN0IG1ldGhvZCA9ICdHRVQnXG4gICAgY29uc3QgcXVlcnkgPSAndmVyc2lvbmluZydcblxuICAgIGNvbnN0IGh0dHBSZXMgPSBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0QXN5bmMoeyBtZXRob2QsIGJ1Y2tldE5hbWUsIHF1ZXJ5IH0pXG4gICAgY29uc3QgeG1sUmVzdWx0ID0gYXdhaXQgcmVhZEFzU3RyaW5nKGh0dHBSZXMpXG4gICAgcmV0dXJuIGF3YWl0IHhtbFBhcnNlcnMucGFyc2VCdWNrZXRWZXJzaW9uaW5nQ29uZmlnKHhtbFJlc3VsdClcbiAgfVxuXG4gIGFzeW5jIHNldEJ1Y2tldFZlcnNpb25pbmcoYnVja2V0TmFtZTogc3RyaW5nLCB2ZXJzaW9uQ29uZmlnOiBCdWNrZXRWZXJzaW9uaW5nQ29uZmlndXJhdGlvbik6IFByb21pc2U8dm9pZD4ge1xuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcignSW52YWxpZCBidWNrZXQgbmFtZTogJyArIGJ1Y2tldE5hbWUpXG4gICAgfVxuICAgIGlmICghT2JqZWN0LmtleXModmVyc2lvbkNvbmZpZykubGVuZ3RoKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKCd2ZXJzaW9uQ29uZmlnIHNob3VsZCBiZSBvZiB0eXBlIFwib2JqZWN0XCInKVxuICAgIH1cblxuICAgIGNvbnN0IG1ldGhvZCA9ICdQVVQnXG4gICAgY29uc3QgcXVlcnkgPSAndmVyc2lvbmluZydcbiAgICBjb25zdCBidWlsZGVyID0gbmV3IHhtbDJqcy5CdWlsZGVyKHtcbiAgICAgIHJvb3ROYW1lOiAnVmVyc2lvbmluZ0NvbmZpZ3VyYXRpb24nLFxuICAgICAgcmVuZGVyT3B0czogeyBwcmV0dHk6IGZhbHNlIH0sXG4gICAgICBoZWFkbGVzczogdHJ1ZSxcbiAgICB9KVxuICAgIGNvbnN0IHBheWxvYWQgPSBidWlsZGVyLmJ1aWxkT2JqZWN0KHZlcnNpb25Db25maWcpXG5cbiAgICBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0QXN5bmNPbWl0KHsgbWV0aG9kLCBidWNrZXROYW1lLCBxdWVyeSB9LCBwYXlsb2FkKVxuICB9XG5cbiAgcHJpdmF0ZSBhc3luYyBzZXRUYWdnaW5nKHRhZ2dpbmdQYXJhbXM6IFB1dFRhZ2dpbmdQYXJhbXMpOiBQcm9taXNlPHZvaWQ+IHtcbiAgICBjb25zdCB7IGJ1Y2tldE5hbWUsIG9iamVjdE5hbWUsIHRhZ3MsIHB1dE9wdHMgfSA9IHRhZ2dpbmdQYXJhbXNcbiAgICBjb25zdCBtZXRob2QgPSAnUFVUJ1xuICAgIGxldCBxdWVyeSA9ICd0YWdnaW5nJ1xuXG4gICAgaWYgKHB1dE9wdHMgJiYgcHV0T3B0cz8udmVyc2lvbklkKSB7XG4gICAgICBxdWVyeSA9IGAke3F1ZXJ5fSZ2ZXJzaW9uSWQ9JHtwdXRPcHRzLnZlcnNpb25JZH1gXG4gICAgfVxuICAgIGNvbnN0IHRhZ3NMaXN0ID0gW11cbiAgICBmb3IgKGNvbnN0IFtrZXksIHZhbHVlXSBvZiBPYmplY3QuZW50cmllcyh0YWdzKSkge1xuICAgICAgdGFnc0xpc3QucHVzaCh7IEtleToga2V5LCBWYWx1ZTogdmFsdWUgfSlcbiAgICB9XG4gICAgY29uc3QgdGFnZ2luZ0NvbmZpZyA9IHtcbiAgICAgIFRhZ2dpbmc6IHtcbiAgICAgICAgVGFnU2V0OiB7XG4gICAgICAgICAgVGFnOiB0YWdzTGlzdCxcbiAgICAgICAgfSxcbiAgICAgIH0sXG4gICAgfVxuICAgIGNvbnN0IGhlYWRlcnMgPSB7fSBhcyBSZXF1ZXN0SGVhZGVyc1xuICAgIGNvbnN0IGJ1aWxkZXIgPSBuZXcgeG1sMmpzLkJ1aWxkZXIoeyBoZWFkbGVzczogdHJ1ZSwgcmVuZGVyT3B0czogeyBwcmV0dHk6IGZhbHNlIH0gfSlcbiAgICBjb25zdCBwYXlsb2FkQnVmID0gQnVmZmVyLmZyb20oYnVpbGRlci5idWlsZE9iamVjdCh0YWdnaW5nQ29uZmlnKSlcbiAgICBjb25zdCByZXF1ZXN0T3B0aW9ucyA9IHtcbiAgICAgIG1ldGhvZCxcbiAgICAgIGJ1Y2tldE5hbWUsXG4gICAgICBxdWVyeSxcbiAgICAgIGhlYWRlcnMsXG5cbiAgICAgIC4uLihvYmplY3ROYW1lICYmIHsgb2JqZWN0TmFtZTogb2JqZWN0TmFtZSB9KSxcbiAgICB9XG5cbiAgICBoZWFkZXJzWydDb250ZW50LU1ENSddID0gdG9NZDUocGF5bG9hZEJ1ZilcblxuICAgIGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luY09taXQocmVxdWVzdE9wdGlvbnMsIHBheWxvYWRCdWYpXG4gIH1cblxuICBwcml2YXRlIGFzeW5jIHJlbW92ZVRhZ2dpbmcoeyBidWNrZXROYW1lLCBvYmplY3ROYW1lLCByZW1vdmVPcHRzIH06IFJlbW92ZVRhZ2dpbmdQYXJhbXMpOiBQcm9taXNlPHZvaWQ+IHtcbiAgICBjb25zdCBtZXRob2QgPSAnREVMRVRFJ1xuICAgIGxldCBxdWVyeSA9ICd0YWdnaW5nJ1xuXG4gICAgaWYgKHJlbW92ZU9wdHMgJiYgT2JqZWN0LmtleXMocmVtb3ZlT3B0cykubGVuZ3RoICYmIHJlbW92ZU9wdHMudmVyc2lvbklkKSB7XG4gICAgICBxdWVyeSA9IGAke3F1ZXJ5fSZ2ZXJzaW9uSWQ9JHtyZW1vdmVPcHRzLnZlcnNpb25JZH1gXG4gICAgfVxuICAgIGNvbnN0IHJlcXVlc3RPcHRpb25zID0geyBtZXRob2QsIGJ1Y2tldE5hbWUsIG9iamVjdE5hbWUsIHF1ZXJ5IH1cblxuICAgIGlmIChvYmplY3ROYW1lKSB7XG4gICAgICByZXF1ZXN0T3B0aW9uc1snb2JqZWN0TmFtZSddID0gb2JqZWN0TmFtZVxuICAgIH1cbiAgICBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0QXN5bmMocmVxdWVzdE9wdGlvbnMsICcnLCBbMjAwLCAyMDRdKVxuICB9XG5cbiAgYXN5bmMgc2V0QnVja2V0VGFnZ2luZyhidWNrZXROYW1lOiBzdHJpbmcsIHRhZ3M6IFRhZ3MpOiBQcm9taXNlPHZvaWQ+IHtcbiAgICBpZiAoIWlzVmFsaWRCdWNrZXROYW1lKGJ1Y2tldE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IoJ0ludmFsaWQgYnVja2V0IG5hbWU6ICcgKyBidWNrZXROYW1lKVxuICAgIH1cbiAgICBpZiAoIWlzUGxhaW5PYmplY3QodGFncykpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEFyZ3VtZW50RXJyb3IoJ3RhZ3Mgc2hvdWxkIGJlIG9mIHR5cGUgXCJvYmplY3RcIicpXG4gICAgfVxuICAgIGlmIChPYmplY3Qua2V5cyh0YWdzKS5sZW5ndGggPiAxMCkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQXJndW1lbnRFcnJvcignbWF4aW11bSB0YWdzIGFsbG93ZWQgaXMgMTBcIicpXG4gICAgfVxuXG4gICAgYXdhaXQgdGhpcy5zZXRUYWdnaW5nKHsgYnVja2V0TmFtZSwgdGFncyB9KVxuICB9XG5cbiAgYXN5bmMgcmVtb3ZlQnVja2V0VGFnZ2luZyhidWNrZXROYW1lOiBzdHJpbmcpIHtcbiAgICBpZiAoIWlzVmFsaWRCdWNrZXROYW1lKGJ1Y2tldE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IoJ0ludmFsaWQgYnVja2V0IG5hbWU6ICcgKyBidWNrZXROYW1lKVxuICAgIH1cbiAgICBhd2FpdCB0aGlzLnJlbW92ZVRhZ2dpbmcoeyBidWNrZXROYW1lIH0pXG4gIH1cblxuICBhc3luYyBzZXRPYmplY3RUYWdnaW5nKGJ1Y2tldE5hbWU6IHN0cmluZywgb2JqZWN0TmFtZTogc3RyaW5nLCB0YWdzOiBUYWdzLCBwdXRPcHRzPzogVGFnZ2luZ09wdHMpIHtcbiAgICBpZiAoIWlzVmFsaWRCdWNrZXROYW1lKGJ1Y2tldE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IoJ0ludmFsaWQgYnVja2V0IG5hbWU6ICcgKyBidWNrZXROYW1lKVxuICAgIH1cbiAgICBpZiAoIWlzVmFsaWRPYmplY3ROYW1lKG9iamVjdE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IoJ0ludmFsaWQgb2JqZWN0IG5hbWU6ICcgKyBvYmplY3ROYW1lKVxuICAgIH1cblxuICAgIGlmICghaXNQbGFpbk9iamVjdCh0YWdzKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQXJndW1lbnRFcnJvcigndGFncyBzaG91bGQgYmUgb2YgdHlwZSBcIm9iamVjdFwiJylcbiAgICB9XG4gICAgaWYgKE9iamVjdC5rZXlzKHRhZ3MpLmxlbmd0aCA+IDEwKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKCdNYXhpbXVtIHRhZ3MgYWxsb3dlZCBpcyAxMFwiJylcbiAgICB9XG5cbiAgICBhd2FpdCB0aGlzLnNldFRhZ2dpbmcoeyBidWNrZXROYW1lLCBvYmplY3ROYW1lLCB0YWdzLCBwdXRPcHRzIH0pXG4gIH1cblxuICBhc3luYyByZW1vdmVPYmplY3RUYWdnaW5nKGJ1Y2tldE5hbWU6IHN0cmluZywgb2JqZWN0TmFtZTogc3RyaW5nLCByZW1vdmVPcHRzOiBUYWdnaW5nT3B0cykge1xuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcignSW52YWxpZCBidWNrZXQgbmFtZTogJyArIGJ1Y2tldE5hbWUpXG4gICAgfVxuICAgIGlmICghaXNWYWxpZE9iamVjdE5hbWUob2JqZWN0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcignSW52YWxpZCBvYmplY3QgbmFtZTogJyArIG9iamVjdE5hbWUpXG4gICAgfVxuICAgIGlmIChyZW1vdmVPcHRzICYmIE9iamVjdC5rZXlzKHJlbW92ZU9wdHMpLmxlbmd0aCAmJiAhaXNPYmplY3QocmVtb3ZlT3B0cykpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEFyZ3VtZW50RXJyb3IoJ3JlbW92ZU9wdHMgc2hvdWxkIGJlIG9mIHR5cGUgXCJvYmplY3RcIicpXG4gICAgfVxuXG4gICAgYXdhaXQgdGhpcy5yZW1vdmVUYWdnaW5nKHsgYnVja2V0TmFtZSwgb2JqZWN0TmFtZSwgcmVtb3ZlT3B0cyB9KVxuICB9XG5cbiAgYXN5bmMgc2VsZWN0T2JqZWN0Q29udGVudChcbiAgICBidWNrZXROYW1lOiBzdHJpbmcsXG4gICAgb2JqZWN0TmFtZTogc3RyaW5nLFxuICAgIHNlbGVjdE9wdHM6IFNlbGVjdE9wdGlvbnMsXG4gICk6IFByb21pc2U8U2VsZWN0UmVzdWx0cyB8IHVuZGVmaW5lZD4ge1xuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcihgSW52YWxpZCBidWNrZXQgbmFtZTogJHtidWNrZXROYW1lfWApXG4gICAgfVxuICAgIGlmICghaXNWYWxpZE9iamVjdE5hbWUob2JqZWN0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZE9iamVjdE5hbWVFcnJvcihgSW52YWxpZCBvYmplY3QgbmFtZTogJHtvYmplY3ROYW1lfWApXG4gICAgfVxuICAgIGlmICghXy5pc0VtcHR5KHNlbGVjdE9wdHMpKSB7XG4gICAgICBpZiAoIWlzU3RyaW5nKHNlbGVjdE9wdHMuZXhwcmVzc2lvbikpIHtcbiAgICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcignc3FsRXhwcmVzc2lvbiBzaG91bGQgYmUgb2YgdHlwZSBcInN0cmluZ1wiJylcbiAgICAgIH1cbiAgICAgIGlmICghXy5pc0VtcHR5KHNlbGVjdE9wdHMuaW5wdXRTZXJpYWxpemF0aW9uKSkge1xuICAgICAgICBpZiAoIWlzT2JqZWN0KHNlbGVjdE9wdHMuaW5wdXRTZXJpYWxpemF0aW9uKSkge1xuICAgICAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ2lucHV0U2VyaWFsaXphdGlvbiBzaG91bGQgYmUgb2YgdHlwZSBcIm9iamVjdFwiJylcbiAgICAgICAgfVxuICAgICAgfSBlbHNlIHtcbiAgICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcignaW5wdXRTZXJpYWxpemF0aW9uIGlzIHJlcXVpcmVkJylcbiAgICAgIH1cbiAgICAgIGlmICghXy5pc0VtcHR5KHNlbGVjdE9wdHMub3V0cHV0U2VyaWFsaXphdGlvbikpIHtcbiAgICAgICAgaWYgKCFpc09iamVjdChzZWxlY3RPcHRzLm91dHB1dFNlcmlhbGl6YXRpb24pKSB7XG4gICAgICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcignb3V0cHV0U2VyaWFsaXphdGlvbiBzaG91bGQgYmUgb2YgdHlwZSBcIm9iamVjdFwiJylcbiAgICAgICAgfVxuICAgICAgfSBlbHNlIHtcbiAgICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcignb3V0cHV0U2VyaWFsaXphdGlvbiBpcyByZXF1aXJlZCcpXG4gICAgICB9XG4gICAgfSBlbHNlIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ3ZhbGlkIHNlbGVjdCBjb25maWd1cmF0aW9uIGlzIHJlcXVpcmVkJylcbiAgICB9XG5cbiAgICBjb25zdCBtZXRob2QgPSAnUE9TVCdcbiAgICBjb25zdCBxdWVyeSA9IGBzZWxlY3Qmc2VsZWN0LXR5cGU9MmBcblxuICAgIGNvbnN0IGNvbmZpZzogUmVjb3JkPHN0cmluZywgdW5rbm93bj5bXSA9IFtcbiAgICAgIHtcbiAgICAgICAgRXhwcmVzc2lvbjogc2VsZWN0T3B0cy5leHByZXNzaW9uLFxuICAgICAgfSxcbiAgICAgIHtcbiAgICAgICAgRXhwcmVzc2lvblR5cGU6IHNlbGVjdE9wdHMuZXhwcmVzc2lvblR5cGUgfHwgJ1NRTCcsXG4gICAgICB9LFxuICAgICAge1xuICAgICAgICBJbnB1dFNlcmlhbGl6YXRpb246IFtzZWxlY3RPcHRzLmlucHV0U2VyaWFsaXphdGlvbl0sXG4gICAgICB9LFxuICAgICAge1xuICAgICAgICBPdXRwdXRTZXJpYWxpemF0aW9uOiBbc2VsZWN0T3B0cy5vdXRwdXRTZXJpYWxpemF0aW9uXSxcbiAgICAgIH0sXG4gICAgXVxuXG4gICAgLy8gT3B0aW9uYWxcbiAgICBpZiAoc2VsZWN0T3B0cy5yZXF1ZXN0UHJvZ3Jlc3MpIHtcbiAgICAgIGNvbmZpZy5wdXNoKHsgUmVxdWVzdFByb2dyZXNzOiBzZWxlY3RPcHRzPy5yZXF1ZXN0UHJvZ3Jlc3MgfSlcbiAgICB9XG4gICAgLy8gT3B0aW9uYWxcbiAgICBpZiAoc2VsZWN0T3B0cy5zY2FuUmFuZ2UpIHtcbiAgICAgIGNvbmZpZy5wdXNoKHsgU2NhblJhbmdlOiBzZWxlY3RPcHRzLnNjYW5SYW5nZSB9KVxuICAgIH1cblxuICAgIGNvbnN0IGJ1aWxkZXIgPSBuZXcgeG1sMmpzLkJ1aWxkZXIoe1xuICAgICAgcm9vdE5hbWU6ICdTZWxlY3RPYmplY3RDb250ZW50UmVxdWVzdCcsXG4gICAgICByZW5kZXJPcHRzOiB7IHByZXR0eTogZmFsc2UgfSxcbiAgICAgIGhlYWRsZXNzOiB0cnVlLFxuICAgIH0pXG4gICAgY29uc3QgcGF5bG9hZCA9IGJ1aWxkZXIuYnVpbGRPYmplY3QoY29uZmlnKVxuXG4gICAgY29uc3QgcmVzID0gYXdhaXQgdGhpcy5tYWtlUmVxdWVzdEFzeW5jKHsgbWV0aG9kLCBidWNrZXROYW1lLCBvYmplY3ROYW1lLCBxdWVyeSB9LCBwYXlsb2FkKVxuICAgIGNvbnN0IGJvZHkgPSBhd2FpdCByZWFkQXNCdWZmZXIocmVzKVxuICAgIHJldHVybiBwYXJzZVNlbGVjdE9iamVjdENvbnRlbnRSZXNwb25zZShib2R5KVxuICB9XG5cbiAgcHJpdmF0ZSBhc3luYyBhcHBseUJ1Y2tldExpZmVjeWNsZShidWNrZXROYW1lOiBzdHJpbmcsIHBvbGljeUNvbmZpZzogTGlmZUN5Y2xlQ29uZmlnUGFyYW0pOiBQcm9taXNlPHZvaWQ+IHtcbiAgICBjb25zdCBtZXRob2QgPSAnUFVUJ1xuICAgIGNvbnN0IHF1ZXJ5ID0gJ2xpZmVjeWNsZSdcblxuICAgIGNvbnN0IGhlYWRlcnM6IFJlcXVlc3RIZWFkZXJzID0ge31cbiAgICBjb25zdCBidWlsZGVyID0gbmV3IHhtbDJqcy5CdWlsZGVyKHtcbiAgICAgIHJvb3ROYW1lOiAnTGlmZWN5Y2xlQ29uZmlndXJhdGlvbicsXG4gICAgICBoZWFkbGVzczogdHJ1ZSxcbiAgICAgIHJlbmRlck9wdHM6IHsgcHJldHR5OiBmYWxzZSB9LFxuICAgIH0pXG4gICAgY29uc3QgcGF5bG9hZCA9IGJ1aWxkZXIuYnVpbGRPYmplY3QocG9saWN5Q29uZmlnKVxuICAgIGhlYWRlcnNbJ0NvbnRlbnQtTUQ1J10gPSB0b01kNShwYXlsb2FkKVxuXG4gICAgYXdhaXQgdGhpcy5tYWtlUmVxdWVzdEFzeW5jT21pdCh7IG1ldGhvZCwgYnVja2V0TmFtZSwgcXVlcnksIGhlYWRlcnMgfSwgcGF5bG9hZClcbiAgfVxuXG4gIGFzeW5jIHJlbW92ZUJ1Y2tldExpZmVjeWNsZShidWNrZXROYW1lOiBzdHJpbmcpOiBQcm9taXNlPHZvaWQ+IHtcbiAgICBpZiAoIWlzVmFsaWRCdWNrZXROYW1lKGJ1Y2tldE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IoJ0ludmFsaWQgYnVja2V0IG5hbWU6ICcgKyBidWNrZXROYW1lKVxuICAgIH1cbiAgICBjb25zdCBtZXRob2QgPSAnREVMRVRFJ1xuICAgIGNvbnN0IHF1ZXJ5ID0gJ2xpZmVjeWNsZSdcbiAgICBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0QXN5bmNPbWl0KHsgbWV0aG9kLCBidWNrZXROYW1lLCBxdWVyeSB9LCAnJywgWzIwNF0pXG4gIH1cblxuICBhc3luYyBzZXRCdWNrZXRMaWZlY3ljbGUoYnVja2V0TmFtZTogc3RyaW5nLCBsaWZlQ3ljbGVDb25maWc6IExpZmVDeWNsZUNvbmZpZ1BhcmFtKTogUHJvbWlzZTx2b2lkPiB7XG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZShidWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKCdJbnZhbGlkIGJ1Y2tldCBuYW1lOiAnICsgYnVja2V0TmFtZSlcbiAgICB9XG4gICAgaWYgKF8uaXNFbXB0eShsaWZlQ3ljbGVDb25maWcpKSB7XG4gICAgICBhd2FpdCB0aGlzLnJlbW92ZUJ1Y2tldExpZmVjeWNsZShidWNrZXROYW1lKVxuICAgIH0gZWxzZSB7XG4gICAgICBhd2FpdCB0aGlzLmFwcGx5QnVja2V0TGlmZWN5Y2xlKGJ1Y2tldE5hbWUsIGxpZmVDeWNsZUNvbmZpZylcbiAgICB9XG4gIH1cblxuICBhc3luYyBnZXRCdWNrZXRMaWZlY3ljbGUoYnVja2V0TmFtZTogc3RyaW5nKTogUHJvbWlzZTxMaWZlY3ljbGVDb25maWcgfCBudWxsPiB7XG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZShidWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKCdJbnZhbGlkIGJ1Y2tldCBuYW1lOiAnICsgYnVja2V0TmFtZSlcbiAgICB9XG4gICAgY29uc3QgbWV0aG9kID0gJ0dFVCdcbiAgICBjb25zdCBxdWVyeSA9ICdsaWZlY3ljbGUnXG5cbiAgICBjb25zdCByZXMgPSBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0QXN5bmMoeyBtZXRob2QsIGJ1Y2tldE5hbWUsIHF1ZXJ5IH0pXG4gICAgY29uc3QgYm9keSA9IGF3YWl0IHJlYWRBc1N0cmluZyhyZXMpXG4gICAgcmV0dXJuIHhtbFBhcnNlcnMucGFyc2VMaWZlY3ljbGVDb25maWcoYm9keSlcbiAgfVxuXG4gIGFzeW5jIHNldEJ1Y2tldEVuY3J5cHRpb24oYnVja2V0TmFtZTogc3RyaW5nLCBlbmNyeXB0aW9uQ29uZmlnPzogRW5jcnlwdGlvbkNvbmZpZyk6IFByb21pc2U8dm9pZD4ge1xuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcignSW52YWxpZCBidWNrZXQgbmFtZTogJyArIGJ1Y2tldE5hbWUpXG4gICAgfVxuICAgIGlmICghXy5pc0VtcHR5KGVuY3J5cHRpb25Db25maWcpICYmIGVuY3J5cHRpb25Db25maWcuUnVsZS5sZW5ndGggPiAxKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKCdJbnZhbGlkIFJ1bGUgbGVuZ3RoLiBPbmx5IG9uZSBydWxlIGlzIGFsbG93ZWQuOiAnICsgZW5jcnlwdGlvbkNvbmZpZy5SdWxlKVxuICAgIH1cblxuICAgIGxldCBlbmNyeXB0aW9uT2JqID0gZW5jcnlwdGlvbkNvbmZpZ1xuICAgIGlmIChfLmlzRW1wdHkoZW5jcnlwdGlvbkNvbmZpZykpIHtcbiAgICAgIGVuY3J5cHRpb25PYmogPSB7XG4gICAgICAgIC8vIERlZmF1bHQgTWluSU8gU2VydmVyIFN1cHBvcnRlZCBSdWxlXG4gICAgICAgIFJ1bGU6IFtcbiAgICAgICAgICB7XG4gICAgICAgICAgICBBcHBseVNlcnZlclNpZGVFbmNyeXB0aW9uQnlEZWZhdWx0OiB7XG4gICAgICAgICAgICAgIFNTRUFsZ29yaXRobTogJ0FFUzI1NicsXG4gICAgICAgICAgICB9LFxuICAgICAgICAgIH0sXG4gICAgICAgIF0sXG4gICAgICB9XG4gICAgfVxuXG4gICAgY29uc3QgbWV0aG9kID0gJ1BVVCdcbiAgICBjb25zdCBxdWVyeSA9ICdlbmNyeXB0aW9uJ1xuICAgIGNvbnN0IGJ1aWxkZXIgPSBuZXcgeG1sMmpzLkJ1aWxkZXIoe1xuICAgICAgcm9vdE5hbWU6ICdTZXJ2ZXJTaWRlRW5jcnlwdGlvbkNvbmZpZ3VyYXRpb24nLFxuICAgICAgcmVuZGVyT3B0czogeyBwcmV0dHk6IGZhbHNlIH0sXG4gICAgICBoZWFkbGVzczogdHJ1ZSxcbiAgICB9KVxuICAgIGNvbnN0IHBheWxvYWQgPSBidWlsZGVyLmJ1aWxkT2JqZWN0KGVuY3J5cHRpb25PYmopXG5cbiAgICBjb25zdCBoZWFkZXJzOiBSZXF1ZXN0SGVhZGVycyA9IHt9XG4gICAgaGVhZGVyc1snQ29udGVudC1NRDUnXSA9IHRvTWQ1KHBheWxvYWQpXG5cbiAgICBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0QXN5bmNPbWl0KHsgbWV0aG9kLCBidWNrZXROYW1lLCBxdWVyeSwgaGVhZGVycyB9LCBwYXlsb2FkKVxuICB9XG5cbiAgYXN5bmMgZ2V0QnVja2V0RW5jcnlwdGlvbihidWNrZXROYW1lOiBzdHJpbmcpIHtcbiAgICBpZiAoIWlzVmFsaWRCdWNrZXROYW1lKGJ1Y2tldE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IoJ0ludmFsaWQgYnVja2V0IG5hbWU6ICcgKyBidWNrZXROYW1lKVxuICAgIH1cbiAgICBjb25zdCBtZXRob2QgPSAnR0VUJ1xuICAgIGNvbnN0IHF1ZXJ5ID0gJ2VuY3J5cHRpb24nXG5cbiAgICBjb25zdCByZXMgPSBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0QXN5bmMoeyBtZXRob2QsIGJ1Y2tldE5hbWUsIHF1ZXJ5IH0pXG4gICAgY29uc3QgYm9keSA9IGF3YWl0IHJlYWRBc1N0cmluZyhyZXMpXG4gICAgcmV0dXJuIHhtbFBhcnNlcnMucGFyc2VCdWNrZXRFbmNyeXB0aW9uQ29uZmlnKGJvZHkpXG4gIH1cblxuICBhc3luYyByZW1vdmVCdWNrZXRFbmNyeXB0aW9uKGJ1Y2tldE5hbWU6IHN0cmluZykge1xuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcignSW52YWxpZCBidWNrZXQgbmFtZTogJyArIGJ1Y2tldE5hbWUpXG4gICAgfVxuICAgIGNvbnN0IG1ldGhvZCA9ICdERUxFVEUnXG4gICAgY29uc3QgcXVlcnkgPSAnZW5jcnlwdGlvbidcblxuICAgIGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luY09taXQoeyBtZXRob2QsIGJ1Y2tldE5hbWUsIHF1ZXJ5IH0sICcnLCBbMjA0XSlcbiAgfVxuXG4gIGFzeW5jIGdldE9iamVjdFJldGVudGlvbihcbiAgICBidWNrZXROYW1lOiBzdHJpbmcsXG4gICAgb2JqZWN0TmFtZTogc3RyaW5nLFxuICAgIGdldE9wdHM/OiBHZXRPYmplY3RSZXRlbnRpb25PcHRzLFxuICApOiBQcm9taXNlPE9iamVjdFJldGVudGlvbkluZm8gfCBudWxsIHwgdW5kZWZpbmVkPiB7XG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZShidWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKCdJbnZhbGlkIGJ1Y2tldCBuYW1lOiAnICsgYnVja2V0TmFtZSlcbiAgICB9XG4gICAgaWYgKCFpc1ZhbGlkT2JqZWN0TmFtZShvYmplY3ROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkT2JqZWN0TmFtZUVycm9yKGBJbnZhbGlkIG9iamVjdCBuYW1lOiAke29iamVjdE5hbWV9YClcbiAgICB9XG4gICAgaWYgKGdldE9wdHMgJiYgIWlzT2JqZWN0KGdldE9wdHMpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKCdnZXRPcHRzIHNob3VsZCBiZSBvZiB0eXBlIFwib2JqZWN0XCInKVxuICAgIH0gZWxzZSBpZiAoZ2V0T3B0cz8udmVyc2lvbklkICYmICFpc1N0cmluZyhnZXRPcHRzLnZlcnNpb25JZCkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEFyZ3VtZW50RXJyb3IoJ3ZlcnNpb25JZCBzaG91bGQgYmUgb2YgdHlwZSBcInN0cmluZ1wiJylcbiAgICB9XG5cbiAgICBjb25zdCBtZXRob2QgPSAnR0VUJ1xuICAgIGxldCBxdWVyeSA9ICdyZXRlbnRpb24nXG4gICAgaWYgKGdldE9wdHM/LnZlcnNpb25JZCkge1xuICAgICAgcXVlcnkgKz0gYCZ2ZXJzaW9uSWQ9JHtnZXRPcHRzLnZlcnNpb25JZH1gXG4gICAgfVxuICAgIGNvbnN0IHJlcyA9IGF3YWl0IHRoaXMubWFrZVJlcXVlc3RBc3luYyh7IG1ldGhvZCwgYnVja2V0TmFtZSwgb2JqZWN0TmFtZSwgcXVlcnkgfSlcbiAgICBjb25zdCBib2R5ID0gYXdhaXQgcmVhZEFzU3RyaW5nKHJlcylcbiAgICByZXR1cm4geG1sUGFyc2Vycy5wYXJzZU9iamVjdFJldGVudGlvbkNvbmZpZyhib2R5KVxuICB9XG5cbiAgYXN5bmMgcmVtb3ZlT2JqZWN0cyhidWNrZXROYW1lOiBzdHJpbmcsIG9iamVjdHNMaXN0OiBSZW1vdmVPYmplY3RzUGFyYW0pOiBQcm9taXNlPFJlbW92ZU9iamVjdHNSZXNwb25zZVtdPiB7XG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZShidWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKCdJbnZhbGlkIGJ1Y2tldCBuYW1lOiAnICsgYnVja2V0TmFtZSlcbiAgICB9XG4gICAgaWYgKCFBcnJheS5pc0FycmF5KG9iamVjdHNMaXN0KSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQXJndW1lbnRFcnJvcignb2JqZWN0c0xpc3Qgc2hvdWxkIGJlIGEgbGlzdCcpXG4gICAgfVxuXG4gICAgY29uc3QgcnVuRGVsZXRlT2JqZWN0cyA9IGFzeW5jIChiYXRjaDogUmVtb3ZlT2JqZWN0c1BhcmFtKTogUHJvbWlzZTxSZW1vdmVPYmplY3RzUmVzcG9uc2VbXT4gPT4ge1xuICAgICAgY29uc3QgZGVsT2JqZWN0czogUmVtb3ZlT2JqZWN0c1JlcXVlc3RFbnRyeVtdID0gYmF0Y2gubWFwKCh2YWx1ZSkgPT4ge1xuICAgICAgICByZXR1cm4gaXNPYmplY3QodmFsdWUpID8geyBLZXk6IHZhbHVlLm5hbWUsIFZlcnNpb25JZDogdmFsdWUudmVyc2lvbklkIH0gOiB7IEtleTogdmFsdWUgfVxuICAgICAgfSlcblxuICAgICAgY29uc3QgcmVtT2JqZWN0cyA9IHsgRGVsZXRlOiB7IFF1aWV0OiB0cnVlLCBPYmplY3Q6IGRlbE9iamVjdHMgfSB9XG4gICAgICBjb25zdCBwYXlsb2FkID0gQnVmZmVyLmZyb20obmV3IHhtbDJqcy5CdWlsZGVyKHsgaGVhZGxlc3M6IHRydWUgfSkuYnVpbGRPYmplY3QocmVtT2JqZWN0cykpXG4gICAgICBjb25zdCBoZWFkZXJzOiBSZXF1ZXN0SGVhZGVycyA9IHsgJ0NvbnRlbnQtTUQ1JzogdG9NZDUocGF5bG9hZCkgfVxuXG4gICAgICBjb25zdCByZXMgPSBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0QXN5bmMoeyBtZXRob2Q6ICdQT1NUJywgYnVja2V0TmFtZSwgcXVlcnk6ICdkZWxldGUnLCBoZWFkZXJzIH0sIHBheWxvYWQpXG4gICAgICBjb25zdCBib2R5ID0gYXdhaXQgcmVhZEFzU3RyaW5nKHJlcylcbiAgICAgIHJldHVybiB4bWxQYXJzZXJzLnJlbW92ZU9iamVjdHNQYXJzZXIoYm9keSlcbiAgICB9XG5cbiAgICBjb25zdCBtYXhFbnRyaWVzID0gMTAwMCAvLyBtYXggZW50cmllcyBhY2NlcHRlZCBpbiBzZXJ2ZXIgZm9yIERlbGV0ZU11bHRpcGxlT2JqZWN0cyBBUEkuXG4gICAgLy8gQ2xpZW50IHNpZGUgYmF0Y2hpbmdcbiAgICBjb25zdCBiYXRjaGVzID0gW11cbiAgICBmb3IgKGxldCBpID0gMDsgaSA8IG9iamVjdHNMaXN0Lmxlbmd0aDsgaSArPSBtYXhFbnRyaWVzKSB7XG4gICAgICBiYXRjaGVzLnB1c2gob2JqZWN0c0xpc3Quc2xpY2UoaSwgaSArIG1heEVudHJpZXMpKVxuICAgIH1cblxuICAgIGNvbnN0IGJhdGNoUmVzdWx0cyA9IGF3YWl0IFByb21pc2UuYWxsKGJhdGNoZXMubWFwKHJ1bkRlbGV0ZU9iamVjdHMpKVxuICAgIHJldHVybiBiYXRjaFJlc3VsdHMuZmxhdCgpXG4gIH1cblxuICBhc3luYyByZW1vdmVJbmNvbXBsZXRlVXBsb2FkKGJ1Y2tldE5hbWU6IHN0cmluZywgb2JqZWN0TmFtZTogc3RyaW5nKTogUHJvbWlzZTx2b2lkPiB7XG4gICAgaWYgKCFpc1ZhbGlkQnVja2V0TmFtZShidWNrZXROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5Jc1ZhbGlkQnVja2V0TmFtZUVycm9yKCdJbnZhbGlkIGJ1Y2tldCBuYW1lOiAnICsgYnVja2V0TmFtZSlcbiAgICB9XG4gICAgaWYgKCFpc1ZhbGlkT2JqZWN0TmFtZShvYmplY3ROYW1lKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkT2JqZWN0TmFtZUVycm9yKGBJbnZhbGlkIG9iamVjdCBuYW1lOiAke29iamVjdE5hbWV9YClcbiAgICB9XG4gICAgY29uc3QgcmVtb3ZlVXBsb2FkSWQgPSBhd2FpdCB0aGlzLmZpbmRVcGxvYWRJZChidWNrZXROYW1lLCBvYmplY3ROYW1lKVxuICAgIGNvbnN0IG1ldGhvZCA9ICdERUxFVEUnXG4gICAgY29uc3QgcXVlcnkgPSBgdXBsb2FkSWQ9JHtyZW1vdmVVcGxvYWRJZH1gXG4gICAgYXdhaXQgdGhpcy5tYWtlUmVxdWVzdEFzeW5jT21pdCh7IG1ldGhvZCwgYnVja2V0TmFtZSwgb2JqZWN0TmFtZSwgcXVlcnkgfSwgJycsIFsyMDRdKVxuICB9XG5cbiAgcHJpdmF0ZSBhc3luYyBjb3B5T2JqZWN0VjEoXG4gICAgdGFyZ2V0QnVja2V0TmFtZTogc3RyaW5nLFxuICAgIHRhcmdldE9iamVjdE5hbWU6IHN0cmluZyxcbiAgICBzb3VyY2VCdWNrZXROYW1lQW5kT2JqZWN0TmFtZTogc3RyaW5nLFxuICAgIGNvbmRpdGlvbnM/OiBudWxsIHwgQ29weUNvbmRpdGlvbnMsXG4gICkge1xuICAgIGlmICh0eXBlb2YgY29uZGl0aW9ucyA9PSAnZnVuY3Rpb24nKSB7XG4gICAgICBjb25kaXRpb25zID0gbnVsbFxuICAgIH1cblxuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUodGFyZ2V0QnVja2V0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcignSW52YWxpZCBidWNrZXQgbmFtZTogJyArIHRhcmdldEJ1Y2tldE5hbWUpXG4gICAgfVxuICAgIGlmICghaXNWYWxpZE9iamVjdE5hbWUodGFyZ2V0T2JqZWN0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZE9iamVjdE5hbWVFcnJvcihgSW52YWxpZCBvYmplY3QgbmFtZTogJHt0YXJnZXRPYmplY3ROYW1lfWApXG4gICAgfVxuICAgIGlmICghaXNTdHJpbmcoc291cmNlQnVja2V0TmFtZUFuZE9iamVjdE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdzb3VyY2VCdWNrZXROYW1lQW5kT2JqZWN0TmFtZSBzaG91bGQgYmUgb2YgdHlwZSBcInN0cmluZ1wiJylcbiAgICB9XG4gICAgaWYgKHNvdXJjZUJ1Y2tldE5hbWVBbmRPYmplY3ROYW1lID09PSAnJykge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkUHJlZml4RXJyb3IoYEVtcHR5IHNvdXJjZSBwcmVmaXhgKVxuICAgIH1cblxuICAgIGlmIChjb25kaXRpb25zICE9IG51bGwgJiYgIShjb25kaXRpb25zIGluc3RhbmNlb2YgQ29weUNvbmRpdGlvbnMpKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdjb25kaXRpb25zIHNob3VsZCBiZSBvZiB0eXBlIFwiQ29weUNvbmRpdGlvbnNcIicpXG4gICAgfVxuXG4gICAgY29uc3QgaGVhZGVyczogUmVxdWVzdEhlYWRlcnMgPSB7fVxuICAgIGhlYWRlcnNbJ3gtYW16LWNvcHktc291cmNlJ10gPSB1cmlSZXNvdXJjZUVzY2FwZShzb3VyY2VCdWNrZXROYW1lQW5kT2JqZWN0TmFtZSlcblxuICAgIGlmIChjb25kaXRpb25zKSB7XG4gICAgICBpZiAoY29uZGl0aW9ucy5tb2RpZmllZCAhPT0gJycpIHtcbiAgICAgICAgaGVhZGVyc1sneC1hbXotY29weS1zb3VyY2UtaWYtbW9kaWZpZWQtc2luY2UnXSA9IGNvbmRpdGlvbnMubW9kaWZpZWRcbiAgICAgIH1cbiAgICAgIGlmIChjb25kaXRpb25zLnVubW9kaWZpZWQgIT09ICcnKSB7XG4gICAgICAgIGhlYWRlcnNbJ3gtYW16LWNvcHktc291cmNlLWlmLXVubW9kaWZpZWQtc2luY2UnXSA9IGNvbmRpdGlvbnMudW5tb2RpZmllZFxuICAgICAgfVxuICAgICAgaWYgKGNvbmRpdGlvbnMubWF0Y2hFVGFnICE9PSAnJykge1xuICAgICAgICBoZWFkZXJzWyd4LWFtei1jb3B5LXNvdXJjZS1pZi1tYXRjaCddID0gY29uZGl0aW9ucy5tYXRjaEVUYWdcbiAgICAgIH1cbiAgICAgIGlmIChjb25kaXRpb25zLm1hdGNoRVRhZ0V4Y2VwdCAhPT0gJycpIHtcbiAgICAgICAgaGVhZGVyc1sneC1hbXotY29weS1zb3VyY2UtaWYtbm9uZS1tYXRjaCddID0gY29uZGl0aW9ucy5tYXRjaEVUYWdFeGNlcHRcbiAgICAgIH1cbiAgICB9XG5cbiAgICBjb25zdCBtZXRob2QgPSAnUFVUJ1xuXG4gICAgY29uc3QgcmVzID0gYXdhaXQgdGhpcy5tYWtlUmVxdWVzdEFzeW5jKHtcbiAgICAgIG1ldGhvZCxcbiAgICAgIGJ1Y2tldE5hbWU6IHRhcmdldEJ1Y2tldE5hbWUsXG4gICAgICBvYmplY3ROYW1lOiB0YXJnZXRPYmplY3ROYW1lLFxuICAgICAgaGVhZGVycyxcbiAgICB9KVxuICAgIGNvbnN0IGJvZHkgPSBhd2FpdCByZWFkQXNTdHJpbmcocmVzKVxuICAgIHJldHVybiB4bWxQYXJzZXJzLnBhcnNlQ29weU9iamVjdChib2R5KVxuICB9XG5cbiAgcHJpdmF0ZSBhc3luYyBjb3B5T2JqZWN0VjIoXG4gICAgc291cmNlQ29uZmlnOiBDb3B5U291cmNlT3B0aW9ucyxcbiAgICBkZXN0Q29uZmlnOiBDb3B5RGVzdGluYXRpb25PcHRpb25zLFxuICApOiBQcm9taXNlPENvcHlPYmplY3RSZXN1bHRWMj4ge1xuICAgIGlmICghKHNvdXJjZUNvbmZpZyBpbnN0YW5jZW9mIENvcHlTb3VyY2VPcHRpb25zKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQXJndW1lbnRFcnJvcignc291cmNlQ29uZmlnIHNob3VsZCBvZiB0eXBlIENvcHlTb3VyY2VPcHRpb25zICcpXG4gICAgfVxuICAgIGlmICghKGRlc3RDb25maWcgaW5zdGFuY2VvZiBDb3B5RGVzdGluYXRpb25PcHRpb25zKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQXJndW1lbnRFcnJvcignZGVzdENvbmZpZyBzaG91bGQgb2YgdHlwZSBDb3B5RGVzdGluYXRpb25PcHRpb25zICcpXG4gICAgfVxuICAgIGlmICghZGVzdENvbmZpZy52YWxpZGF0ZSgpKSB7XG4gICAgICByZXR1cm4gUHJvbWlzZS5yZWplY3QoKVxuICAgIH1cbiAgICBpZiAoIWRlc3RDb25maWcudmFsaWRhdGUoKSkge1xuICAgICAgcmV0dXJuIFByb21pc2UucmVqZWN0KClcbiAgICB9XG5cbiAgICBjb25zdCBoZWFkZXJzID0gT2JqZWN0LmFzc2lnbih7fSwgc291cmNlQ29uZmlnLmdldEhlYWRlcnMoKSwgZGVzdENvbmZpZy5nZXRIZWFkZXJzKCkpXG5cbiAgICBjb25zdCBidWNrZXROYW1lID0gZGVzdENvbmZpZy5CdWNrZXRcbiAgICBjb25zdCBvYmplY3ROYW1lID0gZGVzdENvbmZpZy5PYmplY3RcblxuICAgIGNvbnN0IG1ldGhvZCA9ICdQVVQnXG5cbiAgICBjb25zdCByZXMgPSBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0QXN5bmMoeyBtZXRob2QsIGJ1Y2tldE5hbWUsIG9iamVjdE5hbWUsIGhlYWRlcnMgfSlcbiAgICBjb25zdCBib2R5ID0gYXdhaXQgcmVhZEFzU3RyaW5nKHJlcylcbiAgICBjb25zdCBjb3B5UmVzID0geG1sUGFyc2Vycy5wYXJzZUNvcHlPYmplY3QoYm9keSlcbiAgICBjb25zdCByZXNIZWFkZXJzOiBJbmNvbWluZ0h0dHBIZWFkZXJzID0gcmVzLmhlYWRlcnNcblxuICAgIGNvbnN0IHNpemVIZWFkZXJWYWx1ZSA9IHJlc0hlYWRlcnMgJiYgcmVzSGVhZGVyc1snY29udGVudC1sZW5ndGgnXVxuICAgIGNvbnN0IHNpemUgPSB0eXBlb2Ygc2l6ZUhlYWRlclZhbHVlID09PSAnbnVtYmVyJyA/IHNpemVIZWFkZXJWYWx1ZSA6IHVuZGVmaW5lZFxuXG4gICAgcmV0dXJuIHtcbiAgICAgIEJ1Y2tldDogZGVzdENvbmZpZy5CdWNrZXQsXG4gICAgICBLZXk6IGRlc3RDb25maWcuT2JqZWN0LFxuICAgICAgTGFzdE1vZGlmaWVkOiBjb3B5UmVzLmxhc3RNb2RpZmllZCxcbiAgICAgIE1ldGFEYXRhOiBleHRyYWN0TWV0YWRhdGEocmVzSGVhZGVycyBhcyBSZXNwb25zZUhlYWRlciksXG4gICAgICBWZXJzaW9uSWQ6IGdldFZlcnNpb25JZChyZXNIZWFkZXJzIGFzIFJlc3BvbnNlSGVhZGVyKSxcbiAgICAgIFNvdXJjZVZlcnNpb25JZDogZ2V0U291cmNlVmVyc2lvbklkKHJlc0hlYWRlcnMgYXMgUmVzcG9uc2VIZWFkZXIpLFxuICAgICAgRXRhZzogc2FuaXRpemVFVGFnKHJlc0hlYWRlcnMuZXRhZyksXG4gICAgICBTaXplOiBzaXplLFxuICAgIH1cbiAgfVxuXG4gIGFzeW5jIGNvcHlPYmplY3Qoc291cmNlOiBDb3B5U291cmNlT3B0aW9ucywgZGVzdDogQ29weURlc3RpbmF0aW9uT3B0aW9ucyk6IFByb21pc2U8Q29weU9iamVjdFJlc3VsdD5cbiAgYXN5bmMgY29weU9iamVjdChcbiAgICB0YXJnZXRCdWNrZXROYW1lOiBzdHJpbmcsXG4gICAgdGFyZ2V0T2JqZWN0TmFtZTogc3RyaW5nLFxuICAgIHNvdXJjZUJ1Y2tldE5hbWVBbmRPYmplY3ROYW1lOiBzdHJpbmcsXG4gICAgY29uZGl0aW9ucz86IENvcHlDb25kaXRpb25zLFxuICApOiBQcm9taXNlPENvcHlPYmplY3RSZXN1bHQ+XG4gIGFzeW5jIGNvcHlPYmplY3QoLi4uYWxsQXJnczogQ29weU9iamVjdFBhcmFtcyk6IFByb21pc2U8Q29weU9iamVjdFJlc3VsdD4ge1xuICAgIGlmICh0eXBlb2YgYWxsQXJnc1swXSA9PT0gJ3N0cmluZycpIHtcbiAgICAgIGNvbnN0IFt0YXJnZXRCdWNrZXROYW1lLCB0YXJnZXRPYmplY3ROYW1lLCBzb3VyY2VCdWNrZXROYW1lQW5kT2JqZWN0TmFtZSwgY29uZGl0aW9uc10gPSBhbGxBcmdzIGFzIFtcbiAgICAgICAgc3RyaW5nLFxuICAgICAgICBzdHJpbmcsXG4gICAgICAgIHN0cmluZyxcbiAgICAgICAgQ29weUNvbmRpdGlvbnM/LFxuICAgICAgXVxuICAgICAgcmV0dXJuIGF3YWl0IHRoaXMuY29weU9iamVjdFYxKHRhcmdldEJ1Y2tldE5hbWUsIHRhcmdldE9iamVjdE5hbWUsIHNvdXJjZUJ1Y2tldE5hbWVBbmRPYmplY3ROYW1lLCBjb25kaXRpb25zKVxuICAgIH1cbiAgICBjb25zdCBbc291cmNlLCBkZXN0XSA9IGFsbEFyZ3MgYXMgW0NvcHlTb3VyY2VPcHRpb25zLCBDb3B5RGVzdGluYXRpb25PcHRpb25zXVxuICAgIHJldHVybiBhd2FpdCB0aGlzLmNvcHlPYmplY3RWMihzb3VyY2UsIGRlc3QpXG4gIH1cblxuICBhc3luYyB1cGxvYWRQYXJ0KFxuICAgIHBhcnRDb25maWc6IHtcbiAgICAgIGJ1Y2tldE5hbWU6IHN0cmluZ1xuICAgICAgb2JqZWN0TmFtZTogc3RyaW5nXG4gICAgICB1cGxvYWRJRDogc3RyaW5nXG4gICAgICBwYXJ0TnVtYmVyOiBudW1iZXJcbiAgICAgIGhlYWRlcnM6IFJlcXVlc3RIZWFkZXJzXG4gICAgfSxcbiAgICBwYXlsb2FkPzogQmluYXJ5LFxuICApIHtcbiAgICBjb25zdCB7IGJ1Y2tldE5hbWUsIG9iamVjdE5hbWUsIHVwbG9hZElELCBwYXJ0TnVtYmVyLCBoZWFkZXJzIH0gPSBwYXJ0Q29uZmlnXG5cbiAgICBjb25zdCBtZXRob2QgPSAnUFVUJ1xuICAgIGNvbnN0IHF1ZXJ5ID0gYHVwbG9hZElkPSR7dXBsb2FkSUR9JnBhcnROdW1iZXI9JHtwYXJ0TnVtYmVyfWBcbiAgICBjb25zdCByZXF1ZXN0T3B0aW9ucyA9IHsgbWV0aG9kLCBidWNrZXROYW1lLCBvYmplY3ROYW1lOiBvYmplY3ROYW1lLCBxdWVyeSwgaGVhZGVycyB9XG4gICAgY29uc3QgcmVzID0gYXdhaXQgdGhpcy5tYWtlUmVxdWVzdEFzeW5jKHJlcXVlc3RPcHRpb25zLCBwYXlsb2FkKVxuICAgIGNvbnN0IGJvZHkgPSBhd2FpdCByZWFkQXNTdHJpbmcocmVzKVxuICAgIGNvbnN0IHBhcnRSZXMgPSB1cGxvYWRQYXJ0UGFyc2VyKGJvZHkpXG4gICAgY29uc3QgcGFydEV0YWdWYWwgPSBzYW5pdGl6ZUVUYWcocmVzLmhlYWRlcnMuZXRhZykgfHwgc2FuaXRpemVFVGFnKHBhcnRSZXMuRVRhZylcbiAgICByZXR1cm4ge1xuICAgICAgZXRhZzogcGFydEV0YWdWYWwsXG4gICAgICBrZXk6IG9iamVjdE5hbWUsXG4gICAgICBwYXJ0OiBwYXJ0TnVtYmVyLFxuICAgIH1cbiAgfVxuXG4gIGFzeW5jIGNvbXBvc2VPYmplY3QoXG4gICAgZGVzdE9iakNvbmZpZzogQ29weURlc3RpbmF0aW9uT3B0aW9ucyxcbiAgICBzb3VyY2VPYmpMaXN0OiBDb3B5U291cmNlT3B0aW9uc1tdLFxuICAgIHsgbWF4Q29uY3VycmVuY3kgPSAxMCB9ID0ge30sXG4gICk6IFByb21pc2U8Ym9vbGVhbiB8IHsgZXRhZzogc3RyaW5nOyB2ZXJzaW9uSWQ6IHN0cmluZyB8IG51bGwgfSB8IFByb21pc2U8dm9pZD4gfCBDb3B5T2JqZWN0UmVzdWx0PiB7XG4gICAgY29uc3Qgc291cmNlRmlsZXNMZW5ndGggPSBzb3VyY2VPYmpMaXN0Lmxlbmd0aFxuXG4gICAgaWYgKCFBcnJheS5pc0FycmF5KHNvdXJjZU9iakxpc3QpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKCdzb3VyY2VDb25maWcgc2hvdWxkIGFuIGFycmF5IG9mIENvcHlTb3VyY2VPcHRpb25zICcpXG4gICAgfVxuICAgIGlmICghKGRlc3RPYmpDb25maWcgaW5zdGFuY2VvZiBDb3B5RGVzdGluYXRpb25PcHRpb25zKSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQXJndW1lbnRFcnJvcignZGVzdENvbmZpZyBzaG91bGQgb2YgdHlwZSBDb3B5RGVzdGluYXRpb25PcHRpb25zICcpXG4gICAgfVxuXG4gICAgaWYgKHNvdXJjZUZpbGVzTGVuZ3RoIDwgMSB8fCBzb3VyY2VGaWxlc0xlbmd0aCA+IFBBUlRfQ09OU1RSQUlOVFMuTUFYX1BBUlRTX0NPVU5UKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRBcmd1bWVudEVycm9yKFxuICAgICAgICBgXCJUaGVyZSBtdXN0IGJlIGFzIGxlYXN0IG9uZSBhbmQgdXAgdG8gJHtQQVJUX0NPTlNUUkFJTlRTLk1BWF9QQVJUU19DT1VOVH0gc291cmNlIG9iamVjdHMuYCxcbiAgICAgIClcbiAgICB9XG5cbiAgICBmb3IgKGxldCBpID0gMDsgaSA8IHNvdXJjZUZpbGVzTGVuZ3RoOyBpKyspIHtcbiAgICAgIGNvbnN0IHNPYmogPSBzb3VyY2VPYmpMaXN0W2ldIGFzIENvcHlTb3VyY2VPcHRpb25zXG4gICAgICBpZiAoIXNPYmoudmFsaWRhdGUoKSkge1xuICAgICAgICByZXR1cm4gZmFsc2VcbiAgICAgIH1cbiAgICB9XG5cbiAgICBpZiAoIShkZXN0T2JqQ29uZmlnIGFzIENvcHlEZXN0aW5hdGlvbk9wdGlvbnMpLnZhbGlkYXRlKCkpIHtcbiAgICAgIHJldHVybiBmYWxzZVxuICAgIH1cblxuICAgIGNvbnN0IGdldFN0YXRPcHRpb25zID0gKHNyY0NvbmZpZzogQ29weVNvdXJjZU9wdGlvbnMpID0+IHtcbiAgICAgIGxldCBzdGF0T3B0cyA9IHt9XG4gICAgICBpZiAoIV8uaXNFbXB0eShzcmNDb25maWcuVmVyc2lvbklEKSkge1xuICAgICAgICBzdGF0T3B0cyA9IHtcbiAgICAgICAgICB2ZXJzaW9uSWQ6IHNyY0NvbmZpZy5WZXJzaW9uSUQsXG4gICAgICAgIH1cbiAgICAgIH1cbiAgICAgIHJldHVybiBzdGF0T3B0c1xuICAgIH1cbiAgICBjb25zdCBzcmNPYmplY3RTaXplczogbnVtYmVyW10gPSBbXVxuICAgIGxldCB0b3RhbFNpemUgPSAwXG4gICAgbGV0IHRvdGFsUGFydHMgPSAwXG5cbiAgICBjb25zdCBzb3VyY2VPYmpTdGF0cyA9IHNvdXJjZU9iakxpc3QubWFwKChzcmNJdGVtKSA9PlxuICAgICAgdGhpcy5zdGF0T2JqZWN0KHNyY0l0ZW0uQnVja2V0LCBzcmNJdGVtLk9iamVjdCwgZ2V0U3RhdE9wdGlvbnMoc3JjSXRlbSkpLFxuICAgIClcblxuICAgIGNvbnN0IHNyY09iamVjdEluZm9zID0gYXdhaXQgUHJvbWlzZS5hbGwoc291cmNlT2JqU3RhdHMpXG5cbiAgICBjb25zdCB2YWxpZGF0ZWRTdGF0cyA9IHNyY09iamVjdEluZm9zLm1hcCgocmVzSXRlbVN0YXQsIGluZGV4KSA9PiB7XG4gICAgICBjb25zdCBzcmNDb25maWc6IENvcHlTb3VyY2VPcHRpb25zIHwgdW5kZWZpbmVkID0gc291cmNlT2JqTGlzdFtpbmRleF1cblxuICAgICAgbGV0IHNyY0NvcHlTaXplID0gcmVzSXRlbVN0YXQuc2l6ZVxuICAgICAgLy8gQ2hlY2sgaWYgYSBzZWdtZW50IGlzIHNwZWNpZmllZCwgYW5kIGlmIHNvLCBpcyB0aGVcbiAgICAgIC8vIHNlZ21lbnQgd2l0aGluIG9iamVjdCBib3VuZHM/XG4gICAgICBpZiAoc3JjQ29uZmlnICYmIHNyY0NvbmZpZy5NYXRjaFJhbmdlKSB7XG4gICAgICAgIC8vIFNpbmNlIHJhbmdlIGlzIHNwZWNpZmllZCxcbiAgICAgICAgLy8gICAgMCA8PSBzcmMuc3JjU3RhcnQgPD0gc3JjLnNyY0VuZFxuICAgICAgICAvLyBzbyBvbmx5IGludmFsaWQgY2FzZSB0byBjaGVjayBpczpcbiAgICAgICAgY29uc3Qgc3JjU3RhcnQgPSBzcmNDb25maWcuU3RhcnRcbiAgICAgICAgY29uc3Qgc3JjRW5kID0gc3JjQ29uZmlnLkVuZFxuICAgICAgICBpZiAoc3JjRW5kID49IHNyY0NvcHlTaXplIHx8IHNyY1N0YXJ0IDwgMCkge1xuICAgICAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEFyZ3VtZW50RXJyb3IoXG4gICAgICAgICAgICBgQ29weVNyY09wdGlvbnMgJHtpbmRleH0gaGFzIGludmFsaWQgc2VnbWVudC10by1jb3B5IFske3NyY1N0YXJ0fSwgJHtzcmNFbmR9XSAoc2l6ZSBpcyAke3NyY0NvcHlTaXplfSlgLFxuICAgICAgICAgIClcbiAgICAgICAgfVxuICAgICAgICBzcmNDb3B5U2l6ZSA9IHNyY0VuZCAtIHNyY1N0YXJ0ICsgMVxuICAgICAgfVxuXG4gICAgICAvLyBPbmx5IHRoZSBsYXN0IHNvdXJjZSBtYXkgYmUgbGVzcyB0aGFuIGBhYnNNaW5QYXJ0U2l6ZWBcbiAgICAgIGlmIChzcmNDb3B5U2l6ZSA8IFBBUlRfQ09OU1RSQUlOVFMuQUJTX01JTl9QQVJUX1NJWkUgJiYgaW5kZXggPCBzb3VyY2VGaWxlc0xlbmd0aCAtIDEpIHtcbiAgICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQXJndW1lbnRFcnJvcihcbiAgICAgICAgICBgQ29weVNyY09wdGlvbnMgJHtpbmRleH0gaXMgdG9vIHNtYWxsICgke3NyY0NvcHlTaXplfSkgYW5kIGl0IGlzIG5vdCB0aGUgbGFzdCBwYXJ0LmAsXG4gICAgICAgIClcbiAgICAgIH1cblxuICAgICAgLy8gSXMgZGF0YSB0byBjb3B5IHRvbyBsYXJnZT9cbiAgICAgIHRvdGFsU2l6ZSArPSBzcmNDb3B5U2l6ZVxuICAgICAgaWYgKHRvdGFsU2l6ZSA+IFBBUlRfQ09OU1RSQUlOVFMuTUFYX01VTFRJUEFSVF9QVVRfT0JKRUNUX1NJWkUpIHtcbiAgICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkQXJndW1lbnRFcnJvcihgQ2Fubm90IGNvbXBvc2UgYW4gb2JqZWN0IG9mIHNpemUgJHt0b3RhbFNpemV9ICg+IDVUaUIpYClcbiAgICAgIH1cblxuICAgICAgLy8gcmVjb3JkIHNvdXJjZSBzaXplXG4gICAgICBzcmNPYmplY3RTaXplc1tpbmRleF0gPSBzcmNDb3B5U2l6ZVxuXG4gICAgICAvLyBjYWxjdWxhdGUgcGFydHMgbmVlZGVkIGZvciBjdXJyZW50IHNvdXJjZVxuICAgICAgdG90YWxQYXJ0cyArPSBwYXJ0c1JlcXVpcmVkKHNyY0NvcHlTaXplKVxuICAgICAgLy8gRG8gd2UgbmVlZCBtb3JlIHBhcnRzIHRoYW4gd2UgYXJlIGFsbG93ZWQ/XG4gICAgICBpZiAodG90YWxQYXJ0cyA+IFBBUlRfQ09OU1RSQUlOVFMuTUFYX1BBUlRTX0NPVU5UKSB7XG4gICAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEFyZ3VtZW50RXJyb3IoXG4gICAgICAgICAgYFlvdXIgcHJvcG9zZWQgY29tcG9zZSBvYmplY3QgcmVxdWlyZXMgbW9yZSB0aGFuICR7UEFSVF9DT05TVFJBSU5UUy5NQVhfUEFSVFNfQ09VTlR9IHBhcnRzYCxcbiAgICAgICAgKVxuICAgICAgfVxuXG4gICAgICByZXR1cm4gcmVzSXRlbVN0YXRcbiAgICB9KVxuXG4gICAgaWYgKCh0b3RhbFBhcnRzID09PSAxICYmIHRvdGFsU2l6ZSA8PSBQQVJUX0NPTlNUUkFJTlRTLk1BWF9QQVJUX1NJWkUpIHx8IHRvdGFsU2l6ZSA9PT0gMCkge1xuICAgICAgcmV0dXJuIGF3YWl0IHRoaXMuY29weU9iamVjdChzb3VyY2VPYmpMaXN0WzBdIGFzIENvcHlTb3VyY2VPcHRpb25zLCBkZXN0T2JqQ29uZmlnKSAvLyB1c2UgY29weU9iamVjdFYyXG4gICAgfVxuXG4gICAgLy8gcHJlc2VydmUgZXRhZyB0byBhdm9pZCBtb2RpZmljYXRpb24gb2Ygb2JqZWN0IHdoaWxlIGNvcHlpbmcuXG4gICAgZm9yIChsZXQgaSA9IDA7IGkgPCBzb3VyY2VGaWxlc0xlbmd0aDsgaSsrKSB7XG4gICAgICA7KHNvdXJjZU9iakxpc3RbaV0gYXMgQ29weVNvdXJjZU9wdGlvbnMpLk1hdGNoRVRhZyA9ICh2YWxpZGF0ZWRTdGF0c1tpXSBhcyBCdWNrZXRJdGVtU3RhdCkuZXRhZ1xuICAgIH1cblxuICAgIGNvbnN0IHNwbGl0UGFydFNpemVMaXN0ID0gdmFsaWRhdGVkU3RhdHMubWFwKChyZXNJdGVtU3RhdCwgaWR4KSA9PiB7XG4gICAgICByZXR1cm4gY2FsY3VsYXRlRXZlblNwbGl0cyhzcmNPYmplY3RTaXplc1tpZHhdIGFzIG51bWJlciwgc291cmNlT2JqTGlzdFtpZHhdIGFzIENvcHlTb3VyY2VPcHRpb25zKVxuICAgIH0pXG5cbiAgICBjb25zdCBnZXRVcGxvYWRQYXJ0Q29uZmlnTGlzdCA9ICh1cGxvYWRJZDogc3RyaW5nKSA9PiB7XG4gICAgICBjb25zdCB1cGxvYWRQYXJ0Q29uZmlnTGlzdDogVXBsb2FkUGFydENvbmZpZ1tdID0gW11cblxuICAgICAgc3BsaXRQYXJ0U2l6ZUxpc3QuZm9yRWFjaCgoc3BsaXRTaXplLCBzcGxpdEluZGV4OiBudW1iZXIpID0+IHtcbiAgICAgICAgaWYgKHNwbGl0U2l6ZSkge1xuICAgICAgICAgIGNvbnN0IHsgc3RhcnRJbmRleDogc3RhcnRJZHgsIGVuZEluZGV4OiBlbmRJZHggfSA9IHNwbGl0U2l6ZVxuXG4gICAgICAgICAgY29uc3QgcGFydEluZGV4ID0gc3BsaXRJbmRleCArIDEgLy8gcGFydCBpbmRleCBzdGFydHMgZnJvbSAxLlxuICAgICAgICAgIGNvbnN0IHRvdGFsVXBsb2FkcyA9IEFycmF5LmZyb20oc3RhcnRJZHgpXG5cbiAgICAgICAgICBjb25zdCBoZWFkZXJzID0gKHNvdXJjZU9iakxpc3Rbc3BsaXRJbmRleF0gYXMgQ29weVNvdXJjZU9wdGlvbnMpLmdldEhlYWRlcnMoKVxuXG4gICAgICAgICAgdG90YWxVcGxvYWRzLmZvckVhY2goKHNwbGl0U3RhcnQsIHVwbGRDdHJJZHgpID0+IHtcbiAgICAgICAgICAgIGNvbnN0IHNwbGl0RW5kID0gZW5kSWR4W3VwbGRDdHJJZHhdXG5cbiAgICAgICAgICAgIC8vIHgtYW16LWNvcHktc291cmNlIGlzIGFscmVhZHkgc2V0IGJ5IENvcHlTb3VyY2VPcHRpb25zLmdldEhlYWRlcnMoKSBhYm92ZSxcbiAgICAgICAgICAgIC8vIFVSSS1lc2NhcGVkIGFuZCBpbmNsdWRpbmcgdGhlID92ZXJzaW9uSWQgc2VsZWN0b3Igd2hlbiBwaW5uZWQuIERvbid0XG4gICAgICAgICAgICAvLyBvdmVyd3JpdGUgaXQgaGVyZTogdGhlIHJhdyB2YWx1ZSBicm9rZSBub24tQVNDSUkgbmFtZXMgKEVSUl9JTlZBTElEX0NIQVIsXG4gICAgICAgICAgICAvLyAjMTM4NSkgYW5kIHNpbGVudGx5IGRyb3BwZWQgdGhlIHNvdXJjZSB2ZXJzaW9uLlxuICAgICAgICAgICAgaGVhZGVyc1sneC1hbXotY29weS1zb3VyY2UtcmFuZ2UnXSA9IGBieXRlcz0ke3NwbGl0U3RhcnR9LSR7c3BsaXRFbmR9YFxuXG4gICAgICAgICAgICBjb25zdCB1cGxvYWRQYXJ0Q29uZmlnID0ge1xuICAgICAgICAgICAgICBidWNrZXROYW1lOiBkZXN0T2JqQ29uZmlnLkJ1Y2tldCxcbiAgICAgICAgICAgICAgb2JqZWN0TmFtZTogZGVzdE9iakNvbmZpZy5PYmplY3QsXG4gICAgICAgICAgICAgIHVwbG9hZElEOiB1cGxvYWRJZCxcbiAgICAgICAgICAgICAgcGFydE51bWJlcjogcGFydEluZGV4LFxuICAgICAgICAgICAgICBoZWFkZXJzOiBoZWFkZXJzLFxuICAgICAgICAgICAgfVxuXG4gICAgICAgICAgICB1cGxvYWRQYXJ0Q29uZmlnTGlzdC5wdXNoKHVwbG9hZFBhcnRDb25maWcpXG4gICAgICAgICAgfSlcbiAgICAgICAgfVxuICAgICAgfSlcblxuICAgICAgcmV0dXJuIHVwbG9hZFBhcnRDb25maWdMaXN0XG4gICAgfVxuXG4gICAgY29uc3QgdXBsb2FkQWxsUGFydHMgPSBhc3luYyAodXBsb2FkTGlzdDogVXBsb2FkUGFydENvbmZpZ1tdKSA9PiB7XG4gICAgICBjb25zdCBwYXJ0VXBsb2FkczogQXdhaXRlZDxSZXR1cm5UeXBlPHR5cGVvZiB0aGlzLnVwbG9hZFBhcnQ+PltdID0gW11cblxuICAgICAgLy8gUHJvY2VzcyB1cGxvYWQgcGFydHMgaW4gYmF0Y2hlcyB0byBhdm9pZCB0b28gbWFueSBjb25jdXJyZW50IHJlcXVlc3RzXG4gICAgICBmb3IgKGNvbnN0IGJhdGNoIG9mIF8uY2h1bmsodXBsb2FkTGlzdCwgbWF4Q29uY3VycmVuY3kpKSB7XG4gICAgICAgIGNvbnN0IGJhdGNoUmVzdWx0cyA9IGF3YWl0IFByb21pc2UuYWxsKGJhdGNoLm1hcCgoaXRlbSkgPT4gdGhpcy51cGxvYWRQYXJ0KGl0ZW0pKSlcblxuICAgICAgICBwYXJ0VXBsb2Fkcy5wdXNoKC4uLmJhdGNoUmVzdWx0cylcbiAgICAgIH1cblxuICAgICAgLy8gUHJvY2VzcyByZXN1bHRzIGhlcmUgaWYgbmVlZGVkXG4gICAgICByZXR1cm4gcGFydFVwbG9hZHNcbiAgICB9XG5cbiAgICBjb25zdCBwZXJmb3JtVXBsb2FkUGFydHMgPSBhc3luYyAodXBsb2FkSWQ6IHN0cmluZykgPT4ge1xuICAgICAgY29uc3QgdXBsb2FkTGlzdCA9IGdldFVwbG9hZFBhcnRDb25maWdMaXN0KHVwbG9hZElkKVxuICAgICAgY29uc3QgcGFydHNSZXMgPSBhd2FpdCB1cGxvYWRBbGxQYXJ0cyh1cGxvYWRMaXN0KVxuICAgICAgcmV0dXJuIHBhcnRzUmVzLm1hcCgocGFydENvcHkpID0+ICh7IGV0YWc6IHBhcnRDb3B5LmV0YWcsIHBhcnQ6IHBhcnRDb3B5LnBhcnQgfSkpXG4gICAgfVxuXG4gICAgY29uc3QgbmV3VXBsb2FkSGVhZGVycyA9IGRlc3RPYmpDb25maWcuZ2V0SGVhZGVycygpXG5cbiAgICBjb25zdCB1cGxvYWRJZCA9IGF3YWl0IHRoaXMuaW5pdGlhdGVOZXdNdWx0aXBhcnRVcGxvYWQoZGVzdE9iakNvbmZpZy5CdWNrZXQsIGRlc3RPYmpDb25maWcuT2JqZWN0LCBuZXdVcGxvYWRIZWFkZXJzKVxuICAgIHRyeSB7XG4gICAgICBjb25zdCBwYXJ0c0RvbmUgPSBhd2FpdCBwZXJmb3JtVXBsb2FkUGFydHModXBsb2FkSWQpXG4gICAgICByZXR1cm4gYXdhaXQgdGhpcy5jb21wbGV0ZU11bHRpcGFydFVwbG9hZChkZXN0T2JqQ29uZmlnLkJ1Y2tldCwgZGVzdE9iakNvbmZpZy5PYmplY3QsIHVwbG9hZElkLCBwYXJ0c0RvbmUpXG4gICAgfSBjYXRjaCAoZXJyKSB7XG4gICAgICByZXR1cm4gYXdhaXQgdGhpcy5hYm9ydE11bHRpcGFydFVwbG9hZChkZXN0T2JqQ29uZmlnLkJ1Y2tldCwgZGVzdE9iakNvbmZpZy5PYmplY3QsIHVwbG9hZElkKVxuICAgIH1cbiAgfVxuXG4gIGFzeW5jIHByZXNpZ25lZFVybChcbiAgICBtZXRob2Q6IHN0cmluZyxcbiAgICBidWNrZXROYW1lOiBzdHJpbmcsXG4gICAgb2JqZWN0TmFtZTogc3RyaW5nLFxuICAgIGV4cGlyZXM/OiBudW1iZXIgfCBQcmVTaWduUmVxdWVzdFBhcmFtcyB8IHVuZGVmaW5lZCxcbiAgICByZXFQYXJhbXM/OiBQcmVTaWduUmVxdWVzdFBhcmFtcyB8IERhdGUsXG4gICAgcmVxdWVzdERhdGU/OiBEYXRlLFxuICApOiBQcm9taXNlPHN0cmluZz4ge1xuICAgIGlmICh0aGlzLmFub255bW91cykge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5Bbm9ueW1vdXNSZXF1ZXN0RXJyb3IoYFByZXNpZ25lZCAke21ldGhvZH0gdXJsIGNhbm5vdCBiZSBnZW5lcmF0ZWQgZm9yIGFub255bW91cyByZXF1ZXN0c2ApXG4gICAgfVxuXG4gICAgaWYgKCFleHBpcmVzKSB7XG4gICAgICBleHBpcmVzID0gUFJFU0lHTl9FWFBJUllfREFZU19NQVhcbiAgICB9XG4gICAgaWYgKCFyZXFQYXJhbXMpIHtcbiAgICAgIHJlcVBhcmFtcyA9IHt9XG4gICAgfVxuICAgIGlmICghcmVxdWVzdERhdGUpIHtcbiAgICAgIHJlcXVlc3REYXRlID0gbmV3IERhdGUoKVxuICAgIH1cblxuICAgIC8vIFR5cGUgYXNzZXJ0aW9uc1xuICAgIGlmIChleHBpcmVzICYmIHR5cGVvZiBleHBpcmVzICE9PSAnbnVtYmVyJykge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcignZXhwaXJlcyBzaG91bGQgYmUgb2YgdHlwZSBcIm51bWJlclwiJylcbiAgICB9XG4gICAgaWYgKHJlcVBhcmFtcyAmJiB0eXBlb2YgcmVxUGFyYW1zICE9PSAnb2JqZWN0Jykge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcigncmVxUGFyYW1zIHNob3VsZCBiZSBvZiB0eXBlIFwib2JqZWN0XCInKVxuICAgIH1cbiAgICBpZiAoKHJlcXVlc3REYXRlICYmICEocmVxdWVzdERhdGUgaW5zdGFuY2VvZiBEYXRlKSkgfHwgKHJlcXVlc3REYXRlICYmIGlzTmFOKHJlcXVlc3REYXRlPy5nZXRUaW1lKCkpKSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcigncmVxdWVzdERhdGUgc2hvdWxkIGJlIG9mIHR5cGUgXCJEYXRlXCIgYW5kIHZhbGlkJylcbiAgICB9XG5cbiAgICBjb25zdCBxdWVyeSA9IHJlcVBhcmFtcyA/IHFzLnN0cmluZ2lmeShyZXFQYXJhbXMpIDogdW5kZWZpbmVkXG5cbiAgICB0cnkge1xuICAgICAgY29uc3QgcmVnaW9uID0gYXdhaXQgdGhpcy5nZXRCdWNrZXRSZWdpb25Bc3luYyhidWNrZXROYW1lKVxuICAgICAgYXdhaXQgdGhpcy5jaGVja0FuZFJlZnJlc2hDcmVkcygpXG4gICAgICBjb25zdCByZXFPcHRpb25zID0gdGhpcy5nZXRSZXF1ZXN0T3B0aW9ucyh7IG1ldGhvZCwgcmVnaW9uLCBidWNrZXROYW1lLCBvYmplY3ROYW1lLCBxdWVyeSB9KVxuXG4gICAgICByZXR1cm4gcHJlc2lnblNpZ25hdHVyZVY0KFxuICAgICAgICByZXFPcHRpb25zLFxuICAgICAgICB0aGlzLmFjY2Vzc0tleSxcbiAgICAgICAgdGhpcy5zZWNyZXRLZXksXG4gICAgICAgIHRoaXMuc2Vzc2lvblRva2VuLFxuICAgICAgICByZWdpb24sXG4gICAgICAgIHJlcXVlc3REYXRlLFxuICAgICAgICBleHBpcmVzLFxuICAgICAgKVxuICAgIH0gY2F0Y2ggKGVycikge1xuICAgICAgaWYgKGVyciBpbnN0YW5jZW9mIGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKSB7XG4gICAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEFyZ3VtZW50RXJyb3IoYFVuYWJsZSB0byBnZXQgYnVja2V0IHJlZ2lvbiBmb3IgJHtidWNrZXROYW1lfS5gKVxuICAgICAgfVxuXG4gICAgICB0aHJvdyBlcnJcbiAgICB9XG4gIH1cblxuICBhc3luYyBwcmVzaWduZWRHZXRPYmplY3QoXG4gICAgYnVja2V0TmFtZTogc3RyaW5nLFxuICAgIG9iamVjdE5hbWU6IHN0cmluZyxcbiAgICBleHBpcmVzPzogbnVtYmVyLFxuICAgIHJlc3BIZWFkZXJzPzogUHJlU2lnblJlcXVlc3RQYXJhbXMgfCBEYXRlLFxuICAgIHJlcXVlc3REYXRlPzogRGF0ZSxcbiAgKTogUHJvbWlzZTxzdHJpbmc+IHtcbiAgICBpZiAoIWlzVmFsaWRCdWNrZXROYW1lKGJ1Y2tldE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IoJ0ludmFsaWQgYnVja2V0IG5hbWU6ICcgKyBidWNrZXROYW1lKVxuICAgIH1cbiAgICBpZiAoIWlzVmFsaWRPYmplY3ROYW1lKG9iamVjdE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRPYmplY3ROYW1lRXJyb3IoYEludmFsaWQgb2JqZWN0IG5hbWU6ICR7b2JqZWN0TmFtZX1gKVxuICAgIH1cblxuICAgIGNvbnN0IHZhbGlkUmVzcEhlYWRlcnMgPSBbXG4gICAgICAncmVzcG9uc2UtY29udGVudC10eXBlJyxcbiAgICAgICdyZXNwb25zZS1jb250ZW50LWxhbmd1YWdlJyxcbiAgICAgICdyZXNwb25zZS1leHBpcmVzJyxcbiAgICAgICdyZXNwb25zZS1jYWNoZS1jb250cm9sJyxcbiAgICAgICdyZXNwb25zZS1jb250ZW50LWRpc3Bvc2l0aW9uJyxcbiAgICAgICdyZXNwb25zZS1jb250ZW50LWVuY29kaW5nJyxcbiAgICBdXG4gICAgdmFsaWRSZXNwSGVhZGVycy5mb3JFYWNoKChoZWFkZXIpID0+IHtcbiAgICAgIC8vIEB0cy1pZ25vcmVcbiAgICAgIGlmIChyZXNwSGVhZGVycyAhPT0gdW5kZWZpbmVkICYmIHJlc3BIZWFkZXJzW2hlYWRlcl0gIT09IHVuZGVmaW5lZCAmJiAhaXNTdHJpbmcocmVzcEhlYWRlcnNbaGVhZGVyXSkpIHtcbiAgICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcihgcmVzcG9uc2UgaGVhZGVyICR7aGVhZGVyfSBzaG91bGQgYmUgb2YgdHlwZSBcInN0cmluZ1wiYClcbiAgICAgIH1cbiAgICB9KVxuICAgIHJldHVybiB0aGlzLnByZXNpZ25lZFVybCgnR0VUJywgYnVja2V0TmFtZSwgb2JqZWN0TmFtZSwgZXhwaXJlcywgcmVzcEhlYWRlcnMsIHJlcXVlc3REYXRlKVxuICB9XG5cbiAgYXN5bmMgcHJlc2lnbmVkUHV0T2JqZWN0KGJ1Y2tldE5hbWU6IHN0cmluZywgb2JqZWN0TmFtZTogc3RyaW5nLCBleHBpcmVzPzogbnVtYmVyKTogUHJvbWlzZTxzdHJpbmc+IHtcbiAgICBpZiAoIWlzVmFsaWRCdWNrZXROYW1lKGJ1Y2tldE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IoYEludmFsaWQgYnVja2V0IG5hbWU6ICR7YnVja2V0TmFtZX1gKVxuICAgIH1cbiAgICBpZiAoIWlzVmFsaWRPYmplY3ROYW1lKG9iamVjdE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRPYmplY3ROYW1lRXJyb3IoYEludmFsaWQgb2JqZWN0IG5hbWU6ICR7b2JqZWN0TmFtZX1gKVxuICAgIH1cblxuICAgIHJldHVybiB0aGlzLnByZXNpZ25lZFVybCgnUFVUJywgYnVja2V0TmFtZSwgb2JqZWN0TmFtZSwgZXhwaXJlcylcbiAgfVxuXG4gIG5ld1Bvc3RQb2xpY3koKTogUG9zdFBvbGljeSB7XG4gICAgcmV0dXJuIG5ldyBQb3N0UG9saWN5KClcbiAgfVxuXG4gIGFzeW5jIHByZXNpZ25lZFBvc3RQb2xpY3kocG9zdFBvbGljeTogUG9zdFBvbGljeSk6IFByb21pc2U8UG9zdFBvbGljeVJlc3VsdD4ge1xuICAgIGlmICh0aGlzLmFub255bW91cykge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5Bbm9ueW1vdXNSZXF1ZXN0RXJyb3IoJ1ByZXNpZ25lZCBQT1NUIHBvbGljeSBjYW5ub3QgYmUgZ2VuZXJhdGVkIGZvciBhbm9ueW1vdXMgcmVxdWVzdHMnKVxuICAgIH1cbiAgICBpZiAoIWlzT2JqZWN0KHBvc3RQb2xpY3kpKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdwb3N0UG9saWN5IHNob3VsZCBiZSBvZiB0eXBlIFwib2JqZWN0XCInKVxuICAgIH1cbiAgICBjb25zdCBidWNrZXROYW1lID0gcG9zdFBvbGljeS5mb3JtRGF0YS5idWNrZXQgYXMgc3RyaW5nXG4gICAgdHJ5IHtcbiAgICAgIGNvbnN0IHJlZ2lvbiA9IGF3YWl0IHRoaXMuZ2V0QnVja2V0UmVnaW9uQXN5bmMoYnVja2V0TmFtZSlcblxuICAgICAgY29uc3QgZGF0ZSA9IG5ldyBEYXRlKClcbiAgICAgIGNvbnN0IGRhdGVTdHIgPSBtYWtlRGF0ZUxvbmcoZGF0ZSlcbiAgICAgIGF3YWl0IHRoaXMuY2hlY2tBbmRSZWZyZXNoQ3JlZHMoKVxuXG4gICAgICBpZiAoIXBvc3RQb2xpY3kucG9saWN5LmV4cGlyYXRpb24pIHtcbiAgICAgICAgLy8gJ2V4cGlyYXRpb24nIGlzIG1hbmRhdG9yeSBmaWVsZCBmb3IgUzMuXG4gICAgICAgIC8vIFNldCBkZWZhdWx0IGV4cGlyYXRpb24gZGF0ZSBvZiA3IGRheXMuXG4gICAgICAgIGNvbnN0IGV4cGlyZXMgPSBuZXcgRGF0ZSgpXG4gICAgICAgIGV4cGlyZXMuc2V0U2Vjb25kcyhQUkVTSUdOX0VYUElSWV9EQVlTX01BWClcbiAgICAgICAgcG9zdFBvbGljeS5zZXRFeHBpcmVzKGV4cGlyZXMpXG4gICAgICB9XG5cbiAgICAgIHBvc3RQb2xpY3kucG9saWN5LmNvbmRpdGlvbnMucHVzaChbJ2VxJywgJyR4LWFtei1kYXRlJywgZGF0ZVN0cl0pXG4gICAgICBwb3N0UG9saWN5LmZvcm1EYXRhWyd4LWFtei1kYXRlJ10gPSBkYXRlU3RyXG5cbiAgICAgIHBvc3RQb2xpY3kucG9saWN5LmNvbmRpdGlvbnMucHVzaChbJ2VxJywgJyR4LWFtei1hbGdvcml0aG0nLCAnQVdTNC1ITUFDLVNIQTI1NiddKVxuICAgICAgcG9zdFBvbGljeS5mb3JtRGF0YVsneC1hbXotYWxnb3JpdGhtJ10gPSAnQVdTNC1ITUFDLVNIQTI1NidcblxuICAgICAgcG9zdFBvbGljeS5wb2xpY3kuY29uZGl0aW9ucy5wdXNoKFsnZXEnLCAnJHgtYW16LWNyZWRlbnRpYWwnLCB0aGlzLmFjY2Vzc0tleSArICcvJyArIGdldFNjb3BlKHJlZ2lvbiwgZGF0ZSldKVxuICAgICAgcG9zdFBvbGljeS5mb3JtRGF0YVsneC1hbXotY3JlZGVudGlhbCddID0gdGhpcy5hY2Nlc3NLZXkgKyAnLycgKyBnZXRTY29wZShyZWdpb24sIGRhdGUpXG5cbiAgICAgIGlmICh0aGlzLnNlc3Npb25Ub2tlbikge1xuICAgICAgICBwb3N0UG9saWN5LnBvbGljeS5jb25kaXRpb25zLnB1c2goWydlcScsICckeC1hbXotc2VjdXJpdHktdG9rZW4nLCB0aGlzLnNlc3Npb25Ub2tlbl0pXG4gICAgICAgIHBvc3RQb2xpY3kuZm9ybURhdGFbJ3gtYW16LXNlY3VyaXR5LXRva2VuJ10gPSB0aGlzLnNlc3Npb25Ub2tlblxuICAgICAgfVxuXG4gICAgICBjb25zdCBwb2xpY3lCYXNlNjQgPSBCdWZmZXIuZnJvbShKU09OLnN0cmluZ2lmeShwb3N0UG9saWN5LnBvbGljeSkpLnRvU3RyaW5nKCdiYXNlNjQnKVxuXG4gICAgICBwb3N0UG9saWN5LmZvcm1EYXRhLnBvbGljeSA9IHBvbGljeUJhc2U2NFxuXG4gICAgICBwb3N0UG9saWN5LmZvcm1EYXRhWyd4LWFtei1zaWduYXR1cmUnXSA9IHBvc3RQcmVzaWduU2lnbmF0dXJlVjQocmVnaW9uLCBkYXRlLCB0aGlzLnNlY3JldEtleSwgcG9saWN5QmFzZTY0KVxuICAgICAgY29uc3Qgb3B0cyA9IHtcbiAgICAgICAgcmVnaW9uOiByZWdpb24sXG4gICAgICAgIGJ1Y2tldE5hbWU6IGJ1Y2tldE5hbWUsXG4gICAgICAgIG1ldGhvZDogJ1BPU1QnLFxuICAgICAgfVxuICAgICAgY29uc3QgcmVxT3B0aW9ucyA9IHRoaXMuZ2V0UmVxdWVzdE9wdGlvbnMob3B0cylcbiAgICAgIGNvbnN0IHBvcnRTdHIgPSB0aGlzLnBvcnQgPT0gODAgfHwgdGhpcy5wb3J0ID09PSA0NDMgPyAnJyA6IGA6JHt0aGlzLnBvcnQudG9TdHJpbmcoKX1gXG4gICAgICBjb25zdCB1cmxTdHIgPSBgJHtyZXFPcHRpb25zLnByb3RvY29sfS8vJHtyZXFPcHRpb25zLmhvc3R9JHtwb3J0U3RyfSR7cmVxT3B0aW9ucy5wYXRofWBcbiAgICAgIHJldHVybiB7IHBvc3RVUkw6IHVybFN0ciwgZm9ybURhdGE6IHBvc3RQb2xpY3kuZm9ybURhdGEgfVxuICAgIH0gY2F0Y2ggKGVycikge1xuICAgICAgaWYgKGVyciBpbnN0YW5jZW9mIGVycm9ycy5JbnZhbGlkQnVja2V0TmFtZUVycm9yKSB7XG4gICAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEFyZ3VtZW50RXJyb3IoYFVuYWJsZSB0byBnZXQgYnVja2V0IHJlZ2lvbiBmb3IgJHtidWNrZXROYW1lfS5gKVxuICAgICAgfVxuXG4gICAgICB0aHJvdyBlcnJcbiAgICB9XG4gIH1cbiAgLy8gbGlzdCBhIGJhdGNoIG9mIG9iamVjdHNcbiAgYXN5bmMgbGlzdE9iamVjdHNRdWVyeShidWNrZXROYW1lOiBzdHJpbmcsIHByZWZpeD86IHN0cmluZywgbWFya2VyPzogc3RyaW5nLCBsaXN0UXVlcnlPcHRzPzogTGlzdE9iamVjdFF1ZXJ5T3B0cykge1xuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcignSW52YWxpZCBidWNrZXQgbmFtZTogJyArIGJ1Y2tldE5hbWUpXG4gICAgfVxuICAgIGlmICghaXNTdHJpbmcocHJlZml4KSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcigncHJlZml4IHNob3VsZCBiZSBvZiB0eXBlIFwic3RyaW5nXCInKVxuICAgIH1cbiAgICBpZiAobWFya2VyICYmICFpc1N0cmluZyhtYXJrZXIpKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdtYXJrZXIgc2hvdWxkIGJlIG9mIHR5cGUgXCJzdHJpbmdcIicpXG4gICAgfVxuXG4gICAgaWYgKGxpc3RRdWVyeU9wdHMgJiYgIWlzT2JqZWN0KGxpc3RRdWVyeU9wdHMpKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdsaXN0UXVlcnlPcHRzIHNob3VsZCBiZSBvZiB0eXBlIFwib2JqZWN0XCInKVxuICAgIH1cbiAgICBsZXQgeyBEZWxpbWl0ZXIsIE1heEtleXMsIEluY2x1ZGVWZXJzaW9uLCB2ZXJzaW9uSWRNYXJrZXIsIGtleU1hcmtlciB9ID0gbGlzdFF1ZXJ5T3B0cyBhcyBMaXN0T2JqZWN0UXVlcnlPcHRzXG5cbiAgICBpZiAoIWlzU3RyaW5nKERlbGltaXRlcikpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ0RlbGltaXRlciBzaG91bGQgYmUgb2YgdHlwZSBcInN0cmluZ1wiJylcbiAgICB9XG4gICAgaWYgKCFpc051bWJlcihNYXhLZXlzKSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcignTWF4S2V5cyBzaG91bGQgYmUgb2YgdHlwZSBcIm51bWJlclwiJylcbiAgICB9XG5cbiAgICBjb25zdCBxdWVyaWVzID0gW11cbiAgICAvLyBlc2NhcGUgZXZlcnkgdmFsdWUgaW4gcXVlcnkgc3RyaW5nLCBleGNlcHQgbWF4S2V5c1xuICAgIHF1ZXJpZXMucHVzaChgcHJlZml4PSR7dXJpRXNjYXBlKHByZWZpeCl9YClcbiAgICBxdWVyaWVzLnB1c2goYGRlbGltaXRlcj0ke3VyaUVzY2FwZShEZWxpbWl0ZXIpfWApXG4gICAgcXVlcmllcy5wdXNoKGBlbmNvZGluZy10eXBlPXVybGApXG5cbiAgICBpZiAoSW5jbHVkZVZlcnNpb24pIHtcbiAgICAgIHF1ZXJpZXMucHVzaChgdmVyc2lvbnNgKVxuICAgIH1cblxuICAgIGlmIChJbmNsdWRlVmVyc2lvbikge1xuICAgICAgLy8gdjEgdmVyc2lvbiBsaXN0aW5nLi5cbiAgICAgIGlmIChrZXlNYXJrZXIpIHtcbiAgICAgICAgcXVlcmllcy5wdXNoKGBrZXktbWFya2VyPSR7a2V5TWFya2VyfWApXG4gICAgICB9XG4gICAgICBpZiAodmVyc2lvbklkTWFya2VyKSB7XG4gICAgICAgIHF1ZXJpZXMucHVzaChgdmVyc2lvbi1pZC1tYXJrZXI9JHt2ZXJzaW9uSWRNYXJrZXJ9YClcbiAgICAgIH1cbiAgICB9IGVsc2UgaWYgKG1hcmtlcikge1xuICAgICAgbWFya2VyID0gdXJpRXNjYXBlKG1hcmtlcilcbiAgICAgIHF1ZXJpZXMucHVzaChgbWFya2VyPSR7bWFya2VyfWApXG4gICAgfVxuXG4gICAgLy8gbm8gbmVlZCB0byBlc2NhcGUgbWF4S2V5c1xuICAgIGlmIChNYXhLZXlzKSB7XG4gICAgICBpZiAoTWF4S2V5cyA+PSAxMDAwKSB7XG4gICAgICAgIE1heEtleXMgPSAxMDAwXG4gICAgICB9XG4gICAgICBxdWVyaWVzLnB1c2goYG1heC1rZXlzPSR7TWF4S2V5c31gKVxuICAgIH1cbiAgICBxdWVyaWVzLnNvcnQoKVxuICAgIGxldCBxdWVyeSA9ICcnXG4gICAgaWYgKHF1ZXJpZXMubGVuZ3RoID4gMCkge1xuICAgICAgcXVlcnkgPSBgJHtxdWVyaWVzLmpvaW4oJyYnKX1gXG4gICAgfVxuXG4gICAgY29uc3QgbWV0aG9kID0gJ0dFVCdcbiAgICBjb25zdCByZXMgPSBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0QXN5bmMoeyBtZXRob2QsIGJ1Y2tldE5hbWUsIHF1ZXJ5IH0pXG4gICAgY29uc3QgYm9keSA9IGF3YWl0IHJlYWRBc1N0cmluZyhyZXMpXG4gICAgY29uc3QgbGlzdFFyeUxpc3QgPSBwYXJzZUxpc3RPYmplY3RzKGJvZHkpXG4gICAgcmV0dXJuIGxpc3RRcnlMaXN0XG4gIH1cblxuICBsaXN0T2JqZWN0cyhcbiAgICBidWNrZXROYW1lOiBzdHJpbmcsXG4gICAgcHJlZml4Pzogc3RyaW5nLFxuICAgIHJlY3Vyc2l2ZT86IGJvb2xlYW4sXG4gICAgbGlzdE9wdHM/OiBMaXN0T2JqZWN0UXVlcnlPcHRzIHwgdW5kZWZpbmVkLFxuICApOiBCdWNrZXRTdHJlYW08T2JqZWN0SW5mbz4ge1xuICAgIGlmIChwcmVmaXggPT09IHVuZGVmaW5lZCkge1xuICAgICAgcHJlZml4ID0gJydcbiAgICB9XG4gICAgaWYgKHJlY3Vyc2l2ZSA9PT0gdW5kZWZpbmVkKSB7XG4gICAgICByZWN1cnNpdmUgPSBmYWxzZVxuICAgIH1cbiAgICBpZiAoIWlzVmFsaWRCdWNrZXROYW1lKGJ1Y2tldE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IoJ0ludmFsaWQgYnVja2V0IG5hbWU6ICcgKyBidWNrZXROYW1lKVxuICAgIH1cbiAgICBpZiAoIWlzVmFsaWRQcmVmaXgocHJlZml4KSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkUHJlZml4RXJyb3IoYEludmFsaWQgcHJlZml4IDogJHtwcmVmaXh9YClcbiAgICB9XG4gICAgaWYgKCFpc1N0cmluZyhwcmVmaXgpKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdwcmVmaXggc2hvdWxkIGJlIG9mIHR5cGUgXCJzdHJpbmdcIicpXG4gICAgfVxuICAgIGlmICghaXNCb29sZWFuKHJlY3Vyc2l2ZSkpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ3JlY3Vyc2l2ZSBzaG91bGQgYmUgb2YgdHlwZSBcImJvb2xlYW5cIicpXG4gICAgfVxuICAgIGlmIChsaXN0T3B0cyAmJiAhaXNPYmplY3QobGlzdE9wdHMpKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdsaXN0T3B0cyBzaG91bGQgYmUgb2YgdHlwZSBcIm9iamVjdFwiJylcbiAgICB9XG4gICAgbGV0IG1hcmtlcjogc3RyaW5nIHwgdW5kZWZpbmVkID0gJydcbiAgICBsZXQga2V5TWFya2VyOiBzdHJpbmcgfCB1bmRlZmluZWQgPSAnJ1xuICAgIGxldCB2ZXJzaW9uSWRNYXJrZXI6IHN0cmluZyB8IHVuZGVmaW5lZCA9ICcnXG4gICAgbGV0IG9iamVjdHM6IE9iamVjdEluZm9bXSA9IFtdXG4gICAgbGV0IGVuZGVkID0gZmFsc2VcbiAgICBjb25zdCByZWFkU3RyZWFtOiBzdHJlYW0uUmVhZGFibGUgPSBuZXcgc3RyZWFtLlJlYWRhYmxlKHsgb2JqZWN0TW9kZTogdHJ1ZSB9KVxuICAgIHJlYWRTdHJlYW0uX3JlYWQgPSBhc3luYyAoKSA9PiB7XG4gICAgICAvLyBwdXNoIG9uZSBvYmplY3QgcGVyIF9yZWFkKClcbiAgICAgIGlmIChvYmplY3RzLmxlbmd0aCkge1xuICAgICAgICByZWFkU3RyZWFtLnB1c2gob2JqZWN0cy5zaGlmdCgpKVxuICAgICAgICByZXR1cm5cbiAgICAgIH1cbiAgICAgIGlmIChlbmRlZCkge1xuICAgICAgICByZXR1cm4gcmVhZFN0cmVhbS5wdXNoKG51bGwpXG4gICAgICB9XG5cbiAgICAgIHRyeSB7XG4gICAgICAgIGNvbnN0IGxpc3RRdWVyeU9wdHMgPSB7XG4gICAgICAgICAgRGVsaW1pdGVyOiByZWN1cnNpdmUgPyAnJyA6ICcvJywgLy8gaWYgcmVjdXJzaXZlIGlzIGZhbHNlIHNldCBkZWxpbWl0ZXIgdG8gJy8nXG4gICAgICAgICAgTWF4S2V5czogMTAwMCxcbiAgICAgICAgICBJbmNsdWRlVmVyc2lvbjogbGlzdE9wdHM/LkluY2x1ZGVWZXJzaW9uLFxuICAgICAgICAgIC8vIHZlcnNpb24gbGlzdGluZyBzcGVjaWZpYyBvcHRpb25zXG4gICAgICAgICAga2V5TWFya2VyOiBrZXlNYXJrZXIsXG4gICAgICAgICAgdmVyc2lvbklkTWFya2VyOiB2ZXJzaW9uSWRNYXJrZXIsXG4gICAgICAgIH1cblxuICAgICAgICBjb25zdCByZXN1bHQ6IExpc3RPYmplY3RRdWVyeVJlcyA9IGF3YWl0IHRoaXMubGlzdE9iamVjdHNRdWVyeShidWNrZXROYW1lLCBwcmVmaXgsIG1hcmtlciwgbGlzdFF1ZXJ5T3B0cylcbiAgICAgICAgaWYgKHJlc3VsdC5pc1RydW5jYXRlZCkge1xuICAgICAgICAgIG1hcmtlciA9IHJlc3VsdC5uZXh0TWFya2VyIHx8IHVuZGVmaW5lZFxuICAgICAgICAgIGlmIChyZXN1bHQua2V5TWFya2VyKSB7XG4gICAgICAgICAgICBrZXlNYXJrZXIgPSByZXN1bHQua2V5TWFya2VyXG4gICAgICAgICAgfVxuICAgICAgICAgIGlmIChyZXN1bHQudmVyc2lvbklkTWFya2VyKSB7XG4gICAgICAgICAgICB2ZXJzaW9uSWRNYXJrZXIgPSByZXN1bHQudmVyc2lvbklkTWFya2VyXG4gICAgICAgICAgfVxuICAgICAgICB9IGVsc2Uge1xuICAgICAgICAgIGVuZGVkID0gdHJ1ZVxuICAgICAgICB9XG4gICAgICAgIGlmIChyZXN1bHQub2JqZWN0cykge1xuICAgICAgICAgIG9iamVjdHMgPSByZXN1bHQub2JqZWN0c1xuICAgICAgICB9XG4gICAgICAgIC8vIEB0cy1pZ25vcmVcbiAgICAgICAgcmVhZFN0cmVhbS5fcmVhZCgpXG4gICAgICB9IGNhdGNoIChlcnIpIHtcbiAgICAgICAgcmVhZFN0cmVhbS5lbWl0KCdlcnJvcicsIGVycilcbiAgICAgIH1cbiAgICB9XG4gICAgcmV0dXJuIHJlYWRTdHJlYW1cbiAgfVxuXG4gIGFzeW5jIGxpc3RPYmplY3RzVjJRdWVyeShcbiAgICBidWNrZXROYW1lOiBzdHJpbmcsXG4gICAgcHJlZml4OiBzdHJpbmcsXG4gICAgY29udGludWF0aW9uVG9rZW46IHN0cmluZyxcbiAgICBkZWxpbWl0ZXI6IHN0cmluZyxcbiAgICBtYXhLZXlzOiBudW1iZXIsXG4gICAgc3RhcnRBZnRlcjogc3RyaW5nLFxuICApOiBQcm9taXNlPExpc3RPYmplY3RWMlJlcz4ge1xuICAgIGlmICghaXNWYWxpZEJ1Y2tldE5hbWUoYnVja2V0TmFtZSkpIHtcbiAgICAgIHRocm93IG5ldyBlcnJvcnMuSW52YWxpZEJ1Y2tldE5hbWVFcnJvcignSW52YWxpZCBidWNrZXQgbmFtZTogJyArIGJ1Y2tldE5hbWUpXG4gICAgfVxuICAgIGlmICghaXNTdHJpbmcocHJlZml4KSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcigncHJlZml4IHNob3VsZCBiZSBvZiB0eXBlIFwic3RyaW5nXCInKVxuICAgIH1cbiAgICBpZiAoIWlzU3RyaW5nKGNvbnRpbnVhdGlvblRva2VuKSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcignY29udGludWF0aW9uVG9rZW4gc2hvdWxkIGJlIG9mIHR5cGUgXCJzdHJpbmdcIicpXG4gICAgfVxuICAgIGlmICghaXNTdHJpbmcoZGVsaW1pdGVyKSkge1xuICAgICAgdGhyb3cgbmV3IFR5cGVFcnJvcignZGVsaW1pdGVyIHNob3VsZCBiZSBvZiB0eXBlIFwic3RyaW5nXCInKVxuICAgIH1cbiAgICBpZiAoIWlzTnVtYmVyKG1heEtleXMpKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdtYXhLZXlzIHNob3VsZCBiZSBvZiB0eXBlIFwibnVtYmVyXCInKVxuICAgIH1cbiAgICBpZiAoIWlzU3RyaW5nKHN0YXJ0QWZ0ZXIpKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdzdGFydEFmdGVyIHNob3VsZCBiZSBvZiB0eXBlIFwic3RyaW5nXCInKVxuICAgIH1cblxuICAgIGNvbnN0IHF1ZXJpZXMgPSBbXVxuICAgIHF1ZXJpZXMucHVzaChgbGlzdC10eXBlPTJgKVxuICAgIHF1ZXJpZXMucHVzaChgZW5jb2RpbmctdHlwZT11cmxgKVxuICAgIHF1ZXJpZXMucHVzaChgcHJlZml4PSR7dXJpRXNjYXBlKHByZWZpeCl9YClcbiAgICBxdWVyaWVzLnB1c2goYGRlbGltaXRlcj0ke3VyaUVzY2FwZShkZWxpbWl0ZXIpfWApXG5cbiAgICBpZiAoY29udGludWF0aW9uVG9rZW4pIHtcbiAgICAgIHF1ZXJpZXMucHVzaChgY29udGludWF0aW9uLXRva2VuPSR7dXJpRXNjYXBlKGNvbnRpbnVhdGlvblRva2VuKX1gKVxuICAgIH1cbiAgICBpZiAoc3RhcnRBZnRlcikge1xuICAgICAgcXVlcmllcy5wdXNoKGBzdGFydC1hZnRlcj0ke3VyaUVzY2FwZShzdGFydEFmdGVyKX1gKVxuICAgIH1cbiAgICBpZiAobWF4S2V5cykge1xuICAgICAgaWYgKG1heEtleXMgPj0gMTAwMCkge1xuICAgICAgICBtYXhLZXlzID0gMTAwMFxuICAgICAgfVxuICAgICAgcXVlcmllcy5wdXNoKGBtYXgta2V5cz0ke21heEtleXN9YClcbiAgICB9XG4gICAgcXVlcmllcy5zb3J0KClcbiAgICBsZXQgcXVlcnkgPSAnJ1xuICAgIGlmIChxdWVyaWVzLmxlbmd0aCA+IDApIHtcbiAgICAgIHF1ZXJ5ID0gYCR7cXVlcmllcy5qb2luKCcmJyl9YFxuICAgIH1cblxuICAgIGNvbnN0IG1ldGhvZCA9ICdHRVQnXG4gICAgY29uc3QgcmVzID0gYXdhaXQgdGhpcy5tYWtlUmVxdWVzdEFzeW5jKHsgbWV0aG9kLCBidWNrZXROYW1lLCBxdWVyeSB9KVxuICAgIGNvbnN0IGJvZHkgPSBhd2FpdCByZWFkQXNTdHJpbmcocmVzKVxuICAgIHJldHVybiBwYXJzZUxpc3RPYmplY3RzVjIoYm9keSlcbiAgfVxuXG4gIGxpc3RPYmplY3RzVjIoXG4gICAgYnVja2V0TmFtZTogc3RyaW5nLFxuICAgIHByZWZpeD86IHN0cmluZyxcbiAgICByZWN1cnNpdmU/OiBib29sZWFuLFxuICAgIHN0YXJ0QWZ0ZXI/OiBzdHJpbmcsXG4gICk6IEJ1Y2tldFN0cmVhbTxCdWNrZXRJdGVtPiB7XG4gICAgaWYgKHByZWZpeCA9PT0gdW5kZWZpbmVkKSB7XG4gICAgICBwcmVmaXggPSAnJ1xuICAgIH1cbiAgICBpZiAocmVjdXJzaXZlID09PSB1bmRlZmluZWQpIHtcbiAgICAgIHJlY3Vyc2l2ZSA9IGZhbHNlXG4gICAgfVxuICAgIGlmIChzdGFydEFmdGVyID09PSB1bmRlZmluZWQpIHtcbiAgICAgIHN0YXJ0QWZ0ZXIgPSAnJ1xuICAgIH1cbiAgICBpZiAoIWlzVmFsaWRCdWNrZXROYW1lKGJ1Y2tldE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IoJ0ludmFsaWQgYnVja2V0IG5hbWU6ICcgKyBidWNrZXROYW1lKVxuICAgIH1cbiAgICBpZiAoIWlzVmFsaWRQcmVmaXgocHJlZml4KSkge1xuICAgICAgdGhyb3cgbmV3IGVycm9ycy5JbnZhbGlkUHJlZml4RXJyb3IoYEludmFsaWQgcHJlZml4IDogJHtwcmVmaXh9YClcbiAgICB9XG4gICAgaWYgKCFpc1N0cmluZyhwcmVmaXgpKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdwcmVmaXggc2hvdWxkIGJlIG9mIHR5cGUgXCJzdHJpbmdcIicpXG4gICAgfVxuICAgIGlmICghaXNCb29sZWFuKHJlY3Vyc2l2ZSkpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ3JlY3Vyc2l2ZSBzaG91bGQgYmUgb2YgdHlwZSBcImJvb2xlYW5cIicpXG4gICAgfVxuICAgIGlmICghaXNTdHJpbmcoc3RhcnRBZnRlcikpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ3N0YXJ0QWZ0ZXIgc2hvdWxkIGJlIG9mIHR5cGUgXCJzdHJpbmdcIicpXG4gICAgfVxuXG4gICAgY29uc3QgZGVsaW1pdGVyID0gcmVjdXJzaXZlID8gJycgOiAnLydcbiAgICBjb25zdCBwcmVmaXhTdHIgPSBwcmVmaXhcbiAgICBjb25zdCBzdGFydEFmdGVyU3RyID0gc3RhcnRBZnRlclxuICAgIGxldCBjb250aW51YXRpb25Ub2tlbiA9ICcnXG4gICAgbGV0IG9iamVjdHM6IEJ1Y2tldEl0ZW1bXSA9IFtdXG4gICAgbGV0IGVuZGVkID0gZmFsc2VcbiAgICBjb25zdCByZWFkU3RyZWFtOiBzdHJlYW0uUmVhZGFibGUgPSBuZXcgc3RyZWFtLlJlYWRhYmxlKHsgb2JqZWN0TW9kZTogdHJ1ZSB9KVxuICAgIHJlYWRTdHJlYW0uX3JlYWQgPSBhc3luYyAoKSA9PiB7XG4gICAgICBpZiAob2JqZWN0cy5sZW5ndGgpIHtcbiAgICAgICAgcmVhZFN0cmVhbS5wdXNoKG9iamVjdHMuc2hpZnQoKSlcbiAgICAgICAgcmV0dXJuXG4gICAgICB9XG4gICAgICBpZiAoZW5kZWQpIHtcbiAgICAgICAgcmV0dXJuIHJlYWRTdHJlYW0ucHVzaChudWxsKVxuICAgICAgfVxuXG4gICAgICB0cnkge1xuICAgICAgICBjb25zdCByZXN1bHQgPSBhd2FpdCB0aGlzLmxpc3RPYmplY3RzVjJRdWVyeShcbiAgICAgICAgICBidWNrZXROYW1lLFxuICAgICAgICAgIHByZWZpeFN0cixcbiAgICAgICAgICBjb250aW51YXRpb25Ub2tlbixcbiAgICAgICAgICBkZWxpbWl0ZXIsXG4gICAgICAgICAgMTAwMCxcbiAgICAgICAgICBzdGFydEFmdGVyU3RyLFxuICAgICAgICApXG4gICAgICAgIGlmIChyZXN1bHQuaXNUcnVuY2F0ZWQpIHtcbiAgICAgICAgICBjb250aW51YXRpb25Ub2tlbiA9IHJlc3VsdC5uZXh0Q29udGludWF0aW9uVG9rZW5cbiAgICAgICAgfSBlbHNlIHtcbiAgICAgICAgICBlbmRlZCA9IHRydWVcbiAgICAgICAgfVxuICAgICAgICBvYmplY3RzID0gcmVzdWx0Lm9iamVjdHNcbiAgICAgICAgLy8gQHRzLWlnbm9yZVxuICAgICAgICByZWFkU3RyZWFtLl9yZWFkKClcbiAgICAgIH0gY2F0Y2ggKGVycikge1xuICAgICAgICByZWFkU3RyZWFtLmVtaXQoJ2Vycm9yJywgZXJyKVxuICAgICAgfVxuICAgIH1cbiAgICByZXR1cm4gcmVhZFN0cmVhbVxuICB9XG5cbiAgYXN5bmMgc2V0QnVja2V0Tm90aWZpY2F0aW9uKGJ1Y2tldE5hbWU6IHN0cmluZywgY29uZmlnOiBOb3RpZmljYXRpb25Db25maWcpOiBQcm9taXNlPHZvaWQ+IHtcbiAgICBpZiAoIWlzVmFsaWRCdWNrZXROYW1lKGJ1Y2tldE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IoJ0ludmFsaWQgYnVja2V0IG5hbWU6ICcgKyBidWNrZXROYW1lKVxuICAgIH1cbiAgICBpZiAoIWlzT2JqZWN0KGNvbmZpZykpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ25vdGlmaWNhdGlvbiBjb25maWcgc2hvdWxkIGJlIG9mIHR5cGUgXCJPYmplY3RcIicpXG4gICAgfVxuICAgIGNvbnN0IG1ldGhvZCA9ICdQVVQnXG4gICAgY29uc3QgcXVlcnkgPSAnbm90aWZpY2F0aW9uJ1xuICAgIGNvbnN0IGJ1aWxkZXIgPSBuZXcgeG1sMmpzLkJ1aWxkZXIoe1xuICAgICAgcm9vdE5hbWU6ICdOb3RpZmljYXRpb25Db25maWd1cmF0aW9uJyxcbiAgICAgIHJlbmRlck9wdHM6IHsgcHJldHR5OiBmYWxzZSB9LFxuICAgICAgaGVhZGxlc3M6IHRydWUsXG4gICAgfSlcbiAgICBjb25zdCBwYXlsb2FkID0gYnVpbGRlci5idWlsZE9iamVjdChjb25maWcpXG4gICAgYXdhaXQgdGhpcy5tYWtlUmVxdWVzdEFzeW5jT21pdCh7IG1ldGhvZCwgYnVja2V0TmFtZSwgcXVlcnkgfSwgcGF5bG9hZClcbiAgfVxuXG4gIGFzeW5jIHJlbW92ZUFsbEJ1Y2tldE5vdGlmaWNhdGlvbihidWNrZXROYW1lOiBzdHJpbmcpOiBQcm9taXNlPHZvaWQ+IHtcbiAgICBhd2FpdCB0aGlzLnNldEJ1Y2tldE5vdGlmaWNhdGlvbihidWNrZXROYW1lLCBuZXcgTm90aWZpY2F0aW9uQ29uZmlnKCkpXG4gIH1cblxuICBhc3luYyBnZXRCdWNrZXROb3RpZmljYXRpb24oYnVja2V0TmFtZTogc3RyaW5nKTogUHJvbWlzZTxOb3RpZmljYXRpb25Db25maWdSZXN1bHQ+IHtcbiAgICBpZiAoIWlzVmFsaWRCdWNrZXROYW1lKGJ1Y2tldE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IoJ0ludmFsaWQgYnVja2V0IG5hbWU6ICcgKyBidWNrZXROYW1lKVxuICAgIH1cbiAgICBjb25zdCBtZXRob2QgPSAnR0VUJ1xuICAgIGNvbnN0IHF1ZXJ5ID0gJ25vdGlmaWNhdGlvbidcbiAgICBjb25zdCByZXMgPSBhd2FpdCB0aGlzLm1ha2VSZXF1ZXN0QXN5bmMoeyBtZXRob2QsIGJ1Y2tldE5hbWUsIHF1ZXJ5IH0pXG4gICAgY29uc3QgYm9keSA9IGF3YWl0IHJlYWRBc1N0cmluZyhyZXMpXG4gICAgcmV0dXJuIHBhcnNlQnVja2V0Tm90aWZpY2F0aW9uKGJvZHkpXG4gIH1cblxuICBsaXN0ZW5CdWNrZXROb3RpZmljYXRpb24oXG4gICAgYnVja2V0TmFtZTogc3RyaW5nLFxuICAgIHByZWZpeDogc3RyaW5nLFxuICAgIHN1ZmZpeDogc3RyaW5nLFxuICAgIGV2ZW50czogTm90aWZpY2F0aW9uRXZlbnRbXSxcbiAgKTogTm90aWZpY2F0aW9uUG9sbGVyIHtcbiAgICBpZiAoIWlzVmFsaWRCdWNrZXROYW1lKGJ1Y2tldE5hbWUpKSB7XG4gICAgICB0aHJvdyBuZXcgZXJyb3JzLkludmFsaWRCdWNrZXROYW1lRXJyb3IoYEludmFsaWQgYnVja2V0IG5hbWU6ICR7YnVja2V0TmFtZX1gKVxuICAgIH1cbiAgICBpZiAoIWlzU3RyaW5nKHByZWZpeCkpIHtcbiAgICAgIHRocm93IG5ldyBUeXBlRXJyb3IoJ3ByZWZpeCBtdXN0IGJlIG9mIHR5cGUgc3RyaW5nJylcbiAgICB9XG4gICAgaWYgKCFpc1N0cmluZyhzdWZmaXgpKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdzdWZmaXggbXVzdCBiZSBvZiB0eXBlIHN0cmluZycpXG4gICAgfVxuICAgIGlmICghQXJyYXkuaXNBcnJheShldmVudHMpKSB7XG4gICAgICB0aHJvdyBuZXcgVHlwZUVycm9yKCdldmVudHMgbXVzdCBiZSBvZiB0eXBlIEFycmF5JylcbiAgICB9XG4gICAgY29uc3QgbGlzdGVuZXIgPSBuZXcgTm90aWZpY2F0aW9uUG9sbGVyKHRoaXMsIGJ1Y2tldE5hbWUsIHByZWZpeCwgc3VmZml4LCBldmVudHMpXG4gICAgbGlzdGVuZXIuc3RhcnQoKVxuICAgIHJldHVybiBsaXN0ZW5lclxuICB9XG59XG4iXSwibWFwcGluZ3MiOiI7Ozs7O0FBQUEsSUFBQUEsTUFBQSxHQUFBQyx1QkFBQSxDQUFBQyxPQUFBO0FBQ0EsSUFBQUMsRUFBQSxHQUFBRix1QkFBQSxDQUFBQyxPQUFBO0FBRUEsSUFBQUUsSUFBQSxHQUFBSCx1QkFBQSxDQUFBQyxPQUFBO0FBQ0EsSUFBQUcsS0FBQSxHQUFBSix1QkFBQSxDQUFBQyxPQUFBO0FBQ0EsSUFBQUksSUFBQSxHQUFBTCx1QkFBQSxDQUFBQyxPQUFBO0FBQ0EsSUFBQUssTUFBQSxHQUFBTix1QkFBQSxDQUFBQyxPQUFBO0FBRUEsSUFBQU0sS0FBQSxHQUFBUCx1QkFBQSxDQUFBQyxPQUFBO0FBQ0EsSUFBQU8sWUFBQSxHQUFBQyxzQkFBQSxDQUFBUixPQUFBO0FBQ0EsSUFBQVMsY0FBQSxHQUFBVCxPQUFBO0FBQ0EsSUFBQVUsT0FBQSxHQUFBRixzQkFBQSxDQUFBUixPQUFBO0FBQ0EsSUFBQVcsWUFBQSxHQUFBSCxzQkFBQSxDQUFBUixPQUFBO0FBQ0EsSUFBQVksT0FBQSxHQUFBSixzQkFBQSxDQUFBUixPQUFBO0FBRUEsSUFBQWEsbUJBQUEsR0FBQWIsT0FBQTtBQUNBLElBQUFjLE1BQUEsR0FBQWYsdUJBQUEsQ0FBQUMsT0FBQTtBQUVBLElBQUFlLFFBQUEsR0FBQWYsT0FBQTtBQVVBLElBQUFnQixhQUFBLEdBQUFoQixPQUFBO0FBQ0EsSUFBQWlCLFFBQUEsR0FBQWpCLE9BQUE7QUFDQSxJQUFBa0IsT0FBQSxHQUFBbEIsT0FBQTtBQUNBLElBQUFtQixlQUFBLEdBQUFuQixPQUFBO0FBQ0EsSUFBQW9CLFdBQUEsR0FBQXBCLE9BQUE7QUFDQSxJQUFBcUIsT0FBQSxHQUFBckIsT0FBQTtBQW1DQSxJQUFBc0IsYUFBQSxHQUFBdEIsT0FBQTtBQUNBLElBQUF1QixXQUFBLEdBQUF2QixPQUFBO0FBQ0EsSUFBQXdCLFFBQUEsR0FBQXhCLE9BQUE7QUFDQSxJQUFBeUIsU0FBQSxHQUFBekIsT0FBQTtBQUVBLElBQUEwQixZQUFBLEdBQUExQixPQUFBO0FBcURBLElBQUEyQixVQUFBLEdBQUE1Qix1QkFBQSxDQUFBQyxPQUFBO0FBU3dCLElBQUE0QixVQUFBLEdBQUFELFVBQUE7QUFBQSxTQUFBbkIsdUJBQUFxQixDQUFBLFdBQUFBLENBQUEsSUFBQUEsQ0FBQSxDQUFBQyxVQUFBLEdBQUFELENBQUEsS0FBQUUsT0FBQSxFQUFBRixDQUFBO0FBQUEsU0FBQTlCLHdCQUFBOEIsQ0FBQSxFQUFBRyxDQUFBLDZCQUFBQyxPQUFBLE1BQUFDLENBQUEsT0FBQUQsT0FBQSxJQUFBRSxDQUFBLE9BQUFGLE9BQUEsWUFBQWxDLHVCQUFBLFlBQUFBLENBQUE4QixDQUFBLEVBQUFHLENBQUEsU0FBQUEsQ0FBQSxJQUFBSCxDQUFBLElBQUFBLENBQUEsQ0FBQUMsVUFBQSxTQUFBRCxDQUFBLE1BQUFPLENBQUEsRUFBQUMsQ0FBQSxFQUFBQyxDQUFBLEtBQUFDLFNBQUEsUUFBQVIsT0FBQSxFQUFBRixDQUFBLGlCQUFBQSxDQUFBLHVCQUFBQSxDQUFBLHlCQUFBQSxDQUFBLFNBQUFTLENBQUEsTUFBQUYsQ0FBQSxHQUFBSixDQUFBLEdBQUFHLENBQUEsR0FBQUQsQ0FBQSxRQUFBRSxDQUFBLENBQUFJLEdBQUEsQ0FBQVgsQ0FBQSxVQUFBTyxDQUFBLENBQUFLLEdBQUEsQ0FBQVosQ0FBQSxHQUFBTyxDQUFBLENBQUFNLEdBQUEsQ0FBQWIsQ0FBQSxFQUFBUyxDQUFBLGdCQUFBTixDQUFBLElBQUFILENBQUEsZ0JBQUFHLENBQUEsT0FBQVcsY0FBQSxDQUFBQyxJQUFBLENBQUFmLENBQUEsRUFBQUcsQ0FBQSxPQUFBSyxDQUFBLElBQUFELENBQUEsR0FBQVMsTUFBQSxDQUFBQyxjQUFBLEtBQUFELE1BQUEsQ0FBQUUsd0JBQUEsQ0FBQWxCLENBQUEsRUFBQUcsQ0FBQSxPQUFBSyxDQUFBLENBQUFJLEdBQUEsSUFBQUosQ0FBQSxDQUFBSyxHQUFBLElBQUFOLENBQUEsQ0FBQUUsQ0FBQSxFQUFBTixDQUFBLEVBQUFLLENBQUEsSUFBQUMsQ0FBQSxDQUFBTixDQUFBLElBQUFILENBQUEsQ0FBQUcsQ0FBQSxXQUFBTSxDQUFBLEtBQUFULENBQUEsRUFBQUcsQ0FBQTtBQUd4QixNQUFNZ0IsR0FBRyxHQUFHLElBQUlDLGVBQU0sQ0FBQ0MsT0FBTyxDQUFDO0VBQUVDLFVBQVUsRUFBRTtJQUFFQyxNQUFNLEVBQUU7RUFBTSxDQUFDO0VBQUVDLFFBQVEsRUFBRTtBQUFLLENBQUMsQ0FBQzs7QUFFakY7QUFDQSxNQUFNQyxPQUFPLEdBQUc7RUFBRUMsT0FBTyxFQTdJekIsT0FBTyxJQTZJNEQ7QUFBYyxDQUFDO0FBRWxGLE1BQU1DLHVCQUF1QixHQUFHLENBQzlCLE9BQU8sRUFDUCxJQUFJLEVBQ0osTUFBTSxFQUNOLFNBQVMsRUFDVCxrQkFBa0IsRUFDbEIsS0FBSyxFQUNMLFNBQVMsRUFDVCxXQUFXLEVBQ1gsUUFBUSxFQUNSLGtCQUFrQixFQUNsQixLQUFLLEVBQ0wsWUFBWSxFQUNaLEtBQUssRUFDTCxvQkFBb0IsRUFDcEIsZUFBZSxFQUNmLGdCQUFnQixFQUNoQixZQUFZLEVBQ1osa0JBQWtCLENBQ1Y7QUFtRUgsTUFBTUMsV0FBVyxDQUFDO0VBY3ZCQyxRQUFRLEdBQVcsRUFBRSxHQUFHLElBQUksR0FBRyxJQUFJO0VBSXpCQyxlQUFlLEdBQUcsQ0FBQyxHQUFHLElBQUksR0FBRyxJQUFJLEdBQUcsSUFBSTtFQUN4Q0MsYUFBYSxHQUFHLENBQUMsR0FBRyxJQUFJLEdBQUcsSUFBSSxHQUFHLElBQUksR0FBRyxJQUFJO0VBUXZEQyxXQUFXQSxDQUFDQyxNQUFxQixFQUFFO0lBQ2pDO0lBQ0EsSUFBSUEsTUFBTSxDQUFDQyxNQUFNLEtBQUtDLFNBQVMsRUFBRTtNQUMvQixNQUFNLElBQUlDLEtBQUssQ0FBQyw2REFBNkQsQ0FBQztJQUNoRjtJQUNBO0lBQ0EsSUFBSUgsTUFBTSxDQUFDSSxNQUFNLEtBQUtGLFNBQVMsRUFBRTtNQUMvQkYsTUFBTSxDQUFDSSxNQUFNLEdBQUcsSUFBSTtJQUN0QjtJQUNBLElBQUksQ0FBQ0osTUFBTSxDQUFDSyxJQUFJLEVBQUU7TUFDaEJMLE1BQU0sQ0FBQ0ssSUFBSSxHQUFHLENBQUM7SUFDakI7SUFDQTtJQUNBLElBQUksQ0FBQyxJQUFBQyx1QkFBZSxFQUFDTixNQUFNLENBQUNPLFFBQVEsQ0FBQyxFQUFFO01BQ3JDLE1BQU0sSUFBSXZELE1BQU0sQ0FBQ3dELG9CQUFvQixDQUFDLHNCQUFzQlIsTUFBTSxDQUFDTyxRQUFRLEVBQUUsQ0FBQztJQUNoRjtJQUNBLElBQUksQ0FBQyxJQUFBRSxtQkFBVyxFQUFDVCxNQUFNLENBQUNLLElBQUksQ0FBQyxFQUFFO01BQzdCLE1BQU0sSUFBSXJELE1BQU0sQ0FBQzBELG9CQUFvQixDQUFDLGtCQUFrQlYsTUFBTSxDQUFDSyxJQUFJLEVBQUUsQ0FBQztJQUN4RTtJQUNBLElBQUksQ0FBQyxJQUFBTSxpQkFBUyxFQUFDWCxNQUFNLENBQUNJLE1BQU0sQ0FBQyxFQUFFO01BQzdCLE1BQU0sSUFBSXBELE1BQU0sQ0FBQzBELG9CQUFvQixDQUNuQyw4QkFBOEJWLE1BQU0sQ0FBQ0ksTUFBTSxvQ0FDN0MsQ0FBQztJQUNIOztJQUVBO0lBQ0EsSUFBSUosTUFBTSxDQUFDWSxNQUFNLEVBQUU7TUFDakIsSUFBSSxDQUFDLElBQUFDLGdCQUFRLEVBQUNiLE1BQU0sQ0FBQ1ksTUFBTSxDQUFDLEVBQUU7UUFDNUIsTUFBTSxJQUFJNUQsTUFBTSxDQUFDMEQsb0JBQW9CLENBQUMsb0JBQW9CVixNQUFNLENBQUNZLE1BQU0sRUFBRSxDQUFDO01BQzVFO0lBQ0Y7SUFFQSxNQUFNRSxJQUFJLEdBQUdkLE1BQU0sQ0FBQ08sUUFBUSxDQUFDUSxXQUFXLENBQUMsQ0FBQztJQUMxQyxJQUFJVixJQUFJLEdBQUdMLE1BQU0sQ0FBQ0ssSUFBSTtJQUN0QixJQUFJVyxRQUFnQjtJQUNwQixJQUFJQyxTQUFTO0lBQ2IsSUFBSUMsY0FBMEI7SUFDOUI7SUFDQTtJQUNBLElBQUlsQixNQUFNLENBQUNJLE1BQU0sRUFBRTtNQUNqQjtNQUNBYSxTQUFTLEdBQUc1RSxLQUFLO01BQ2pCMkUsUUFBUSxHQUFHLFFBQVE7TUFDbkJYLElBQUksR0FBR0EsSUFBSSxJQUFJLEdBQUc7TUFDbEJhLGNBQWMsR0FBRzdFLEtBQUssQ0FBQzhFLFdBQVc7SUFDcEMsQ0FBQyxNQUFNO01BQ0xGLFNBQVMsR0FBRzdFLElBQUk7TUFDaEI0RSxRQUFRLEdBQUcsT0FBTztNQUNsQlgsSUFBSSxHQUFHQSxJQUFJLElBQUksRUFBRTtNQUNqQmEsY0FBYyxHQUFHOUUsSUFBSSxDQUFDK0UsV0FBVztJQUNuQzs7SUFFQTtJQUNBLElBQUluQixNQUFNLENBQUNpQixTQUFTLEVBQUU7TUFDcEIsSUFBSSxDQUFDLElBQUFHLGdCQUFRLEVBQUNwQixNQUFNLENBQUNpQixTQUFTLENBQUMsRUFBRTtRQUMvQixNQUFNLElBQUlqRSxNQUFNLENBQUMwRCxvQkFBb0IsQ0FDbkMsNEJBQTRCVixNQUFNLENBQUNpQixTQUFTLGdDQUM5QyxDQUFDO01BQ0g7TUFDQUEsU0FBUyxHQUFHakIsTUFBTSxDQUFDaUIsU0FBUztJQUM5Qjs7SUFFQTtJQUNBLElBQUlqQixNQUFNLENBQUNrQixjQUFjLEVBQUU7TUFDekIsSUFBSSxDQUFDLElBQUFFLGdCQUFRLEVBQUNwQixNQUFNLENBQUNrQixjQUFjLENBQUMsRUFBRTtRQUNwQyxNQUFNLElBQUlsRSxNQUFNLENBQUMwRCxvQkFBb0IsQ0FDbkMsZ0NBQWdDVixNQUFNLENBQUNrQixjQUFjLGdDQUN2RCxDQUFDO01BQ0g7TUFFQUEsY0FBYyxHQUFHbEIsTUFBTSxDQUFDa0IsY0FBYztJQUN4Qzs7SUFFQTtJQUNBO0lBQ0E7SUFDQTtJQUNBO0lBQ0EsTUFBTUcsZUFBZSxHQUFHLElBQUlDLE9BQU8sQ0FBQ0MsUUFBUSxLQUFLRCxPQUFPLENBQUNFLElBQUksR0FBRztJQUNoRSxNQUFNQyxZQUFZLEdBQUcsU0FBU0osZUFBZSxhQUFhN0IsT0FBTyxDQUFDQyxPQUFPLEVBQUU7SUFDM0U7O0lBRUEsSUFBSSxDQUFDd0IsU0FBUyxHQUFHQSxTQUFTO0lBQzFCLElBQUksQ0FBQ0MsY0FBYyxHQUFHQSxjQUFjO0lBQ3BDLElBQUksQ0FBQ0osSUFBSSxHQUFHQSxJQUFJO0lBQ2hCLElBQUksQ0FBQ1QsSUFBSSxHQUFHQSxJQUFJO0lBQ2hCLElBQUksQ0FBQ1csUUFBUSxHQUFHQSxRQUFRO0lBQ3hCLElBQUksQ0FBQ1UsU0FBUyxHQUFHLEdBQUdELFlBQVksRUFBRTs7SUFFbEM7SUFDQSxJQUFJekIsTUFBTSxDQUFDMkIsU0FBUyxLQUFLekIsU0FBUyxFQUFFO01BQ2xDLElBQUksQ0FBQ3lCLFNBQVMsR0FBRyxJQUFJO0lBQ3ZCLENBQUMsTUFBTTtNQUNMLElBQUksQ0FBQ0EsU0FBUyxHQUFHM0IsTUFBTSxDQUFDMkIsU0FBUztJQUNuQztJQUVBLElBQUksQ0FBQ0MsU0FBUyxHQUFHNUIsTUFBTSxDQUFDNEIsU0FBUyxJQUFJLEVBQUU7SUFDdkMsSUFBSSxDQUFDQyxTQUFTLEdBQUc3QixNQUFNLENBQUM2QixTQUFTLElBQUksRUFBRTtJQUN2QyxJQUFJLENBQUNDLFlBQVksR0FBRzlCLE1BQU0sQ0FBQzhCLFlBQVk7SUFDdkMsSUFBSSxDQUFDQyxTQUFTLEdBQUcsQ0FBQyxJQUFJLENBQUNILFNBQVMsSUFBSSxDQUFDLElBQUksQ0FBQ0MsU0FBUztJQUVuRCxJQUFJN0IsTUFBTSxDQUFDZ0MsbUJBQW1CLEVBQUU7TUFDOUIsSUFBSSxDQUFDRCxTQUFTLEdBQUcsS0FBSztNQUN0QixJQUFJLENBQUNDLG1CQUFtQixHQUFHaEMsTUFBTSxDQUFDZ0MsbUJBQW1CO0lBQ3ZEO0lBRUEsSUFBSSxDQUFDQyxTQUFTLEdBQUcsQ0FBQyxDQUFDO0lBQ25CLElBQUlqQyxNQUFNLENBQUNZLE1BQU0sRUFBRTtNQUNqQixJQUFJLENBQUNBLE1BQU0sR0FBR1osTUFBTSxDQUFDWSxNQUFNO0lBQzdCO0lBRUEsSUFBSVosTUFBTSxDQUFDSixRQUFRLEVBQUU7TUFDbkIsSUFBSSxDQUFDQSxRQUFRLEdBQUdJLE1BQU0sQ0FBQ0osUUFBUTtNQUMvQixJQUFJLENBQUNzQyxnQkFBZ0IsR0FBRyxJQUFJO0lBQzlCO0lBQ0EsSUFBSSxJQUFJLENBQUN0QyxRQUFRLEdBQUcsQ0FBQyxHQUFHLElBQUksR0FBRyxJQUFJLEVBQUU7TUFDbkMsTUFBTSxJQUFJNUMsTUFBTSxDQUFDMEQsb0JBQW9CLENBQUMsc0NBQXNDLENBQUM7SUFDL0U7SUFDQSxJQUFJLElBQUksQ0FBQ2QsUUFBUSxHQUFHLENBQUMsR0FBRyxJQUFJLEdBQUcsSUFBSSxHQUFHLElBQUksRUFBRTtNQUMxQyxNQUFNLElBQUk1QyxNQUFNLENBQUMwRCxvQkFBb0IsQ0FBQyxtQ0FBbUMsQ0FBQztJQUM1RTs7SUFFQTtJQUNBO0lBQ0E7SUFDQSxJQUFJLENBQUN5QixZQUFZLEdBQUcsQ0FBQyxJQUFJLENBQUNKLFNBQVMsSUFBSSxDQUFDL0IsTUFBTSxDQUFDSSxNQUFNO0lBRXJELElBQUksQ0FBQ2dDLG9CQUFvQixHQUFHcEMsTUFBTSxDQUFDb0Msb0JBQW9CLElBQUlsQyxTQUFTO0lBQ3BFLElBQUksQ0FBQ21DLFVBQVUsR0FBRyxDQUFDLENBQUM7SUFDcEIsSUFBSSxDQUFDQyxnQkFBZ0IsR0FBRyxJQUFJQyxzQkFBVSxDQUFDLElBQUksQ0FBQztJQUU1QyxJQUFJdkMsTUFBTSxDQUFDd0MsWUFBWSxFQUFFO01BQ3ZCLElBQUksQ0FBQyxJQUFBcEIsZ0JBQVEsRUFBQ3BCLE1BQU0sQ0FBQ3dDLFlBQVksQ0FBQyxFQUFFO1FBQ2xDLE1BQU0sSUFBSXhGLE1BQU0sQ0FBQzBELG9CQUFvQixDQUNuQyw4QkFBOEJWLE1BQU0sQ0FBQ3dDLFlBQVksZ0NBQ25ELENBQUM7TUFDSDtNQUVBLElBQUksQ0FBQ0EsWUFBWSxHQUFHeEMsTUFBTSxDQUFDd0MsWUFBWTtJQUN6QyxDQUFDLE1BQU07TUFDTCxJQUFJLENBQUNBLFlBQVksR0FBRztRQUNsQkMsWUFBWSxFQUFFO01BQ2hCLENBQUM7SUFDSDtFQUNGO0VBQ0E7QUFDRjtBQUNBO0VBQ0UsSUFBSUMsVUFBVUEsQ0FBQSxFQUFHO0lBQ2YsT0FBTyxJQUFJLENBQUNKLGdCQUFnQjtFQUM5Qjs7RUFFQTtBQUNGO0FBQ0E7RUFDRUssdUJBQXVCQSxDQUFDcEMsUUFBZ0IsRUFBRTtJQUN4QyxJQUFJLENBQUM2QixvQkFBb0IsR0FBRzdCLFFBQVE7RUFDdEM7O0VBRUE7QUFDRjtBQUNBO0VBQ1NxQyxpQkFBaUJBLENBQUNDLE9BQTZFLEVBQUU7SUFDdEcsSUFBSSxDQUFDLElBQUF6QixnQkFBUSxFQUFDeUIsT0FBTyxDQUFDLEVBQUU7TUFDdEIsTUFBTSxJQUFJQyxTQUFTLENBQUMsNENBQTRDLENBQUM7SUFDbkU7SUFDQSxJQUFJLENBQUNULFVBQVUsR0FBR1UsZUFBQyxDQUFDQyxJQUFJLENBQUNILE9BQU8sRUFBRW5ELHVCQUF1QixDQUFDO0VBQzVEOztFQUVBO0FBQ0Y7QUFDQTtFQUNVdUQsMEJBQTBCQSxDQUFDQyxVQUFtQixFQUFFQyxVQUFtQixFQUFFO0lBQzNFLElBQUksQ0FBQyxJQUFBQyxlQUFPLEVBQUMsSUFBSSxDQUFDaEIsb0JBQW9CLENBQUMsSUFBSSxDQUFDLElBQUFnQixlQUFPLEVBQUNGLFVBQVUsQ0FBQyxJQUFJLENBQUMsSUFBQUUsZUFBTyxFQUFDRCxVQUFVLENBQUMsRUFBRTtNQUN2RjtNQUNBO01BQ0EsSUFBSUQsVUFBVSxDQUFDRyxRQUFRLENBQUMsR0FBRyxDQUFDLEVBQUU7UUFDNUIsTUFBTSxJQUFJbEQsS0FBSyxDQUFDLG1FQUFtRStDLFVBQVUsRUFBRSxDQUFDO01BQ2xHO01BQ0E7TUFDQTtNQUNBO01BQ0EsT0FBTyxJQUFJLENBQUNkLG9CQUFvQjtJQUNsQztJQUNBLE9BQU8sS0FBSztFQUNkOztFQUVBO0FBQ0Y7QUFDQTtBQUNBO0FBQ0E7RUFDRWtCLFVBQVVBLENBQUNDLE9BQWUsRUFBRUMsVUFBa0IsRUFBRTtJQUM5QyxJQUFJLENBQUMsSUFBQTNDLGdCQUFRLEVBQUMwQyxPQUFPLENBQUMsRUFBRTtNQUN0QixNQUFNLElBQUlULFNBQVMsQ0FBQyxvQkFBb0JTLE9BQU8sRUFBRSxDQUFDO0lBQ3BEO0lBQ0EsSUFBSUEsT0FBTyxDQUFDRSxJQUFJLENBQUMsQ0FBQyxLQUFLLEVBQUUsRUFBRTtNQUN6QixNQUFNLElBQUl6RyxNQUFNLENBQUMwRCxvQkFBb0IsQ0FBQyxnQ0FBZ0MsQ0FBQztJQUN6RTtJQUNBLElBQUksQ0FBQyxJQUFBRyxnQkFBUSxFQUFDMkMsVUFBVSxDQUFDLEVBQUU7TUFDekIsTUFBTSxJQUFJVixTQUFTLENBQUMsdUJBQXVCVSxVQUFVLEVBQUUsQ0FBQztJQUMxRDtJQUNBLElBQUlBLFVBQVUsQ0FBQ0MsSUFBSSxDQUFDLENBQUMsS0FBSyxFQUFFLEVBQUU7TUFDNUIsTUFBTSxJQUFJekcsTUFBTSxDQUFDMEQsb0JBQW9CLENBQUMsbUNBQW1DLENBQUM7SUFDNUU7SUFDQSxJQUFJLENBQUNnQixTQUFTLEdBQUcsR0FBRyxJQUFJLENBQUNBLFNBQVMsSUFBSTZCLE9BQU8sSUFBSUMsVUFBVSxFQUFFO0VBQy9EOztFQUVBO0FBQ0Y7QUFDQTtBQUNBO0VBQ1lFLGlCQUFpQkEsQ0FDekJDLElBRUMsRUFJRDtJQUNBLE1BQU1DLE1BQU0sR0FBR0QsSUFBSSxDQUFDQyxNQUFNO0lBQzFCLE1BQU1oRCxNQUFNLEdBQUcrQyxJQUFJLENBQUMvQyxNQUFNO0lBQzFCLE1BQU1zQyxVQUFVLEdBQUdTLElBQUksQ0FBQ1QsVUFBVTtJQUNsQyxJQUFJQyxVQUFVLEdBQUdRLElBQUksQ0FBQ1IsVUFBVTtJQUNoQyxNQUFNVSxPQUFPLEdBQUdGLElBQUksQ0FBQ0UsT0FBTztJQUM1QixNQUFNQyxLQUFLLEdBQUdILElBQUksQ0FBQ0csS0FBSztJQUV4QixJQUFJekIsVUFBVSxHQUFHO01BQ2Z1QixNQUFNO01BQ05DLE9BQU8sRUFBRSxDQUFDLENBQW1CO01BQzdCN0MsUUFBUSxFQUFFLElBQUksQ0FBQ0EsUUFBUTtNQUN2QjtNQUNBK0MsS0FBSyxFQUFFLElBQUksQ0FBQzdDO0lBQ2QsQ0FBQzs7SUFFRDtJQUNBLElBQUk4QyxnQkFBZ0I7SUFDcEIsSUFBSWQsVUFBVSxFQUFFO01BQ2RjLGdCQUFnQixHQUFHLElBQUFDLDBCQUFrQixFQUFDLElBQUksQ0FBQ25ELElBQUksRUFBRSxJQUFJLENBQUNFLFFBQVEsRUFBRWtDLFVBQVUsRUFBRSxJQUFJLENBQUN2QixTQUFTLENBQUM7SUFDN0Y7SUFFQSxJQUFJckYsSUFBSSxHQUFHLEdBQUc7SUFDZCxJQUFJd0UsSUFBSSxHQUFHLElBQUksQ0FBQ0EsSUFBSTtJQUVwQixJQUFJVCxJQUF3QjtJQUM1QixJQUFJLElBQUksQ0FBQ0EsSUFBSSxFQUFFO01BQ2JBLElBQUksR0FBRyxJQUFJLENBQUNBLElBQUk7SUFDbEI7SUFFQSxJQUFJOEMsVUFBVSxFQUFFO01BQ2RBLFVBQVUsR0FBRyxJQUFBZSx5QkFBaUIsRUFBQ2YsVUFBVSxDQUFDO0lBQzVDOztJQUVBO0lBQ0EsSUFBSSxJQUFBZ0Isd0JBQWdCLEVBQUNyRCxJQUFJLENBQUMsRUFBRTtNQUMxQixNQUFNc0Qsa0JBQWtCLEdBQUcsSUFBSSxDQUFDbkIsMEJBQTBCLENBQUNDLFVBQVUsRUFBRUMsVUFBVSxDQUFDO01BQ2xGLElBQUlpQixrQkFBa0IsRUFBRTtRQUN0QnRELElBQUksR0FBRyxHQUFHc0Qsa0JBQWtCLEVBQUU7TUFDaEMsQ0FBQyxNQUFNO1FBQ0x0RCxJQUFJLEdBQUcsSUFBQXVELDBCQUFhLEVBQUN6RCxNQUFNLENBQUM7TUFDOUI7SUFDRjtJQUVBLElBQUlvRCxnQkFBZ0IsSUFBSSxDQUFDTCxJQUFJLENBQUNoQyxTQUFTLEVBQUU7TUFDdkM7TUFDQTtNQUNBO01BQ0E7TUFDQTtNQUNBLElBQUl1QixVQUFVLEVBQUU7UUFDZHBDLElBQUksR0FBRyxHQUFHb0MsVUFBVSxJQUFJcEMsSUFBSSxFQUFFO01BQ2hDO01BQ0EsSUFBSXFDLFVBQVUsRUFBRTtRQUNkN0csSUFBSSxHQUFHLElBQUk2RyxVQUFVLEVBQUU7TUFDekI7SUFDRixDQUFDLE1BQU07TUFDTDtNQUNBO01BQ0E7TUFDQSxJQUFJRCxVQUFVLEVBQUU7UUFDZDVHLElBQUksR0FBRyxJQUFJNEcsVUFBVSxFQUFFO01BQ3pCO01BQ0EsSUFBSUMsVUFBVSxFQUFFO1FBQ2Q3RyxJQUFJLEdBQUcsSUFBSTRHLFVBQVUsSUFBSUMsVUFBVSxFQUFFO01BQ3ZDO0lBQ0Y7SUFFQSxJQUFJVyxLQUFLLEVBQUU7TUFDVHhILElBQUksSUFBSSxJQUFJd0gsS0FBSyxFQUFFO0lBQ3JCO0lBQ0F6QixVQUFVLENBQUN3QixPQUFPLENBQUMvQyxJQUFJLEdBQUdBLElBQUk7SUFDOUIsSUFBS3VCLFVBQVUsQ0FBQ3JCLFFBQVEsS0FBSyxPQUFPLElBQUlYLElBQUksS0FBSyxFQUFFLElBQU1nQyxVQUFVLENBQUNyQixRQUFRLEtBQUssUUFBUSxJQUFJWCxJQUFJLEtBQUssR0FBSSxFQUFFO01BQzFHZ0MsVUFBVSxDQUFDd0IsT0FBTyxDQUFDL0MsSUFBSSxHQUFHLElBQUF3RCwwQkFBWSxFQUFDeEQsSUFBSSxFQUFFVCxJQUFJLENBQUM7SUFDcEQ7SUFFQWdDLFVBQVUsQ0FBQ3dCLE9BQU8sQ0FBQyxZQUFZLENBQUMsR0FBRyxJQUFJLENBQUNuQyxTQUFTO0lBQ2pELElBQUltQyxPQUFPLEVBQUU7TUFDWDtNQUNBLEtBQUssTUFBTSxDQUFDVSxDQUFDLEVBQUVDLENBQUMsQ0FBQyxJQUFJekYsTUFBTSxDQUFDMEYsT0FBTyxDQUFDWixPQUFPLENBQUMsRUFBRTtRQUM1Q3hCLFVBQVUsQ0FBQ3dCLE9BQU8sQ0FBQ1UsQ0FBQyxDQUFDeEQsV0FBVyxDQUFDLENBQUMsQ0FBQyxHQUFHeUQsQ0FBQztNQUN6QztJQUNGOztJQUVBO0lBQ0FuQyxVQUFVLEdBQUd0RCxNQUFNLENBQUMyRixNQUFNLENBQUMsQ0FBQyxDQUFDLEVBQUUsSUFBSSxDQUFDckMsVUFBVSxFQUFFQSxVQUFVLENBQUM7SUFFM0QsT0FBTztNQUNMLEdBQUdBLFVBQVU7TUFDYndCLE9BQU8sRUFBRWQsZUFBQyxDQUFDNEIsU0FBUyxDQUFDNUIsZUFBQyxDQUFDNkIsTUFBTSxDQUFDdkMsVUFBVSxDQUFDd0IsT0FBTyxFQUFFZ0IsaUJBQVMsQ0FBQyxFQUFHTCxDQUFDLElBQUtBLENBQUMsQ0FBQ00sUUFBUSxDQUFDLENBQUMsQ0FBQztNQUNsRmhFLElBQUk7TUFDSlQsSUFBSTtNQUNKL0Q7SUFDRixDQUFDO0VBQ0g7RUFFQSxNQUFheUksc0JBQXNCQSxDQUFDL0MsbUJBQXVDLEVBQUU7SUFDM0UsSUFBSSxFQUFFQSxtQkFBbUIsWUFBWWdELHNDQUFrQixDQUFDLEVBQUU7TUFDeEQsTUFBTSxJQUFJN0UsS0FBSyxDQUFDLG9FQUFvRSxDQUFDO0lBQ3ZGO0lBQ0EsSUFBSSxDQUFDNkIsbUJBQW1CLEdBQUdBLG1CQUFtQjtJQUM5QyxNQUFNLElBQUksQ0FBQ2lELG9CQUFvQixDQUFDLENBQUM7RUFDbkM7RUFFQSxNQUFjQSxvQkFBb0JBLENBQUEsRUFBRztJQUNuQyxJQUFJLElBQUksQ0FBQ2pELG1CQUFtQixFQUFFO01BQzVCLElBQUk7UUFDRixNQUFNa0QsZUFBZSxHQUFHLE1BQU0sSUFBSSxDQUFDbEQsbUJBQW1CLENBQUNtRCxjQUFjLENBQUMsQ0FBQztRQUN2RSxJQUFJLENBQUN2RCxTQUFTLEdBQUdzRCxlQUFlLENBQUNFLFlBQVksQ0FBQyxDQUFDO1FBQy9DLElBQUksQ0FBQ3ZELFNBQVMsR0FBR3FELGVBQWUsQ0FBQ0csWUFBWSxDQUFDLENBQUM7UUFDL0MsSUFBSSxDQUFDdkQsWUFBWSxHQUFHb0QsZUFBZSxDQUFDSSxlQUFlLENBQUMsQ0FBQztNQUN2RCxDQUFDLENBQUMsT0FBT3ZILENBQUMsRUFBRTtRQUNWLE1BQU0sSUFBSW9DLEtBQUssQ0FBQyw4QkFBOEJwQyxDQUFDLEVBQUUsRUFBRTtVQUFFd0gsS0FBSyxFQUFFeEg7UUFBRSxDQUFDLENBQUM7TUFDbEU7SUFDRjtFQUNGO0VBSUE7QUFDRjtBQUNBO0VBQ1V5SCxPQUFPQSxDQUFDbkQsVUFBb0IsRUFBRW9ELFFBQXFDLEVBQUVDLEdBQWEsRUFBRTtJQUMxRjtJQUNBLElBQUksQ0FBQyxJQUFJLENBQUNDLFNBQVMsRUFBRTtNQUNuQjtJQUNGO0lBQ0EsSUFBSSxDQUFDLElBQUF2RSxnQkFBUSxFQUFDaUIsVUFBVSxDQUFDLEVBQUU7TUFDekIsTUFBTSxJQUFJUyxTQUFTLENBQUMsdUNBQXVDLENBQUM7SUFDOUQ7SUFDQSxJQUFJMkMsUUFBUSxJQUFJLENBQUMsSUFBQUcsd0JBQWdCLEVBQUNILFFBQVEsQ0FBQyxFQUFFO01BQzNDLE1BQU0sSUFBSTNDLFNBQVMsQ0FBQyxxQ0FBcUMsQ0FBQztJQUM1RDtJQUNBLElBQUk0QyxHQUFHLElBQUksRUFBRUEsR0FBRyxZQUFZdkYsS0FBSyxDQUFDLEVBQUU7TUFDbEMsTUFBTSxJQUFJMkMsU0FBUyxDQUFDLCtCQUErQixDQUFDO0lBQ3REO0lBQ0EsTUFBTTZDLFNBQVMsR0FBRyxJQUFJLENBQUNBLFNBQVM7SUFDaEMsTUFBTUUsVUFBVSxHQUFJaEMsT0FBdUIsSUFBSztNQUM5QzlFLE1BQU0sQ0FBQzBGLE9BQU8sQ0FBQ1osT0FBTyxDQUFDLENBQUNpQyxPQUFPLENBQUMsQ0FBQyxDQUFDdkIsQ0FBQyxFQUFFQyxDQUFDLENBQUMsS0FBSztRQUMxQyxJQUFJRCxDQUFDLElBQUksZUFBZSxFQUFFO1VBQ3hCLElBQUksSUFBQTFELGdCQUFRLEVBQUMyRCxDQUFDLENBQUMsRUFBRTtZQUNmLE1BQU11QixRQUFRLEdBQUcsSUFBSUMsTUFBTSxDQUFDLHVCQUF1QixDQUFDO1lBQ3BEeEIsQ0FBQyxHQUFHQSxDQUFDLENBQUN5QixPQUFPLENBQUNGLFFBQVEsRUFBRSx3QkFBd0IsQ0FBQztVQUNuRDtRQUNGO1FBQ0FKLFNBQVMsQ0FBQ08sS0FBSyxDQUFDLEdBQUczQixDQUFDLEtBQUtDLENBQUMsSUFBSSxDQUFDO01BQ2pDLENBQUMsQ0FBQztNQUNGbUIsU0FBUyxDQUFDTyxLQUFLLENBQUMsSUFBSSxDQUFDO0lBQ3ZCLENBQUM7SUFDRFAsU0FBUyxDQUFDTyxLQUFLLENBQUMsWUFBWTdELFVBQVUsQ0FBQ3VCLE1BQU0sSUFBSXZCLFVBQVUsQ0FBQy9GLElBQUksSUFBSSxDQUFDO0lBQ3JFdUosVUFBVSxDQUFDeEQsVUFBVSxDQUFDd0IsT0FBTyxDQUFDO0lBQzlCLElBQUk0QixRQUFRLEVBQUU7TUFDWixJQUFJLENBQUNFLFNBQVMsQ0FBQ08sS0FBSyxDQUFDLGFBQWFULFFBQVEsQ0FBQ1UsVUFBVSxJQUFJLENBQUM7TUFDMUROLFVBQVUsQ0FBQ0osUUFBUSxDQUFDNUIsT0FBeUIsQ0FBQztJQUNoRDtJQUNBLElBQUk2QixHQUFHLEVBQUU7TUFDUEMsU0FBUyxDQUFDTyxLQUFLLENBQUMsZUFBZSxDQUFDO01BQ2hDLE1BQU1FLE9BQU8sR0FBR0MsSUFBSSxDQUFDQyxTQUFTLENBQUNaLEdBQUcsRUFBRSxJQUFJLEVBQUUsSUFBSSxDQUFDO01BQy9DQyxTQUFTLENBQUNPLEtBQUssQ0FBQyxHQUFHRSxPQUFPLElBQUksQ0FBQztJQUNqQztFQUNGOztFQUVBO0FBQ0Y7QUFDQTtFQUNTRyxPQUFPQSxDQUFDaEssTUFBd0IsRUFBRTtJQUN2QyxJQUFJLENBQUNBLE1BQU0sRUFBRTtNQUNYQSxNQUFNLEdBQUcrRSxPQUFPLENBQUNrRixNQUFNO0lBQ3pCO0lBQ0EsSUFBSSxDQUFDYixTQUFTLEdBQUdwSixNQUFNO0VBQ3pCOztFQUVBO0FBQ0Y7QUFDQTtFQUNTa0ssUUFBUUEsQ0FBQSxFQUFHO0lBQ2hCLElBQUksQ0FBQ2QsU0FBUyxHQUFHekYsU0FBUztFQUM1Qjs7RUFFQTtBQUNGO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtFQUNFLE1BQU13RyxnQkFBZ0JBLENBQ3BCN0QsT0FBc0IsRUFDdEI4RCxPQUFlLEdBQUcsRUFBRSxFQUNwQkMsYUFBdUIsR0FBRyxDQUFDLEdBQUcsQ0FBQyxFQUMvQmhHLE1BQU0sR0FBRyxFQUFFLEVBQ29CO0lBQy9CLElBQUksQ0FBQyxJQUFBUSxnQkFBUSxFQUFDeUIsT0FBTyxDQUFDLEVBQUU7TUFDdEIsTUFBTSxJQUFJQyxTQUFTLENBQUMsb0NBQW9DLENBQUM7SUFDM0Q7SUFDQSxJQUFJLENBQUMsSUFBQWpDLGdCQUFRLEVBQUM4RixPQUFPLENBQUMsSUFBSSxDQUFDLElBQUF2RixnQkFBUSxFQUFDdUYsT0FBTyxDQUFDLEVBQUU7TUFDNUM7TUFDQSxNQUFNLElBQUk3RCxTQUFTLENBQUMsZ0RBQWdELENBQUM7SUFDdkU7SUFDQThELGFBQWEsQ0FBQ2QsT0FBTyxDQUFFSyxVQUFVLElBQUs7TUFDcEMsSUFBSSxDQUFDLElBQUFVLGdCQUFRLEVBQUNWLFVBQVUsQ0FBQyxFQUFFO1FBQ3pCLE1BQU0sSUFBSXJELFNBQVMsQ0FBQyx1Q0FBdUMsQ0FBQztNQUM5RDtJQUNGLENBQUMsQ0FBQztJQUNGLElBQUksQ0FBQyxJQUFBakMsZ0JBQVEsRUFBQ0QsTUFBTSxDQUFDLEVBQUU7TUFDckIsTUFBTSxJQUFJa0MsU0FBUyxDQUFDLG1DQUFtQyxDQUFDO0lBQzFEO0lBQ0EsSUFBSSxDQUFDRCxPQUFPLENBQUNnQixPQUFPLEVBQUU7TUFDcEJoQixPQUFPLENBQUNnQixPQUFPLEdBQUcsQ0FBQyxDQUFDO0lBQ3RCO0lBQ0EsSUFBSWhCLE9BQU8sQ0FBQ2UsTUFBTSxLQUFLLE1BQU0sSUFBSWYsT0FBTyxDQUFDZSxNQUFNLEtBQUssS0FBSyxJQUFJZixPQUFPLENBQUNlLE1BQU0sS0FBSyxRQUFRLEVBQUU7TUFDeEZmLE9BQU8sQ0FBQ2dCLE9BQU8sQ0FBQyxnQkFBZ0IsQ0FBQyxHQUFHOEMsT0FBTyxDQUFDRyxNQUFNLENBQUNoQyxRQUFRLENBQUMsQ0FBQztJQUMvRDtJQUNBLE1BQU1pQyxTQUFTLEdBQUcsSUFBSSxDQUFDNUUsWUFBWSxHQUFHLElBQUE2RSxnQkFBUSxFQUFDTCxPQUFPLENBQUMsR0FBRyxFQUFFO0lBQzVELE9BQU8sSUFBSSxDQUFDTSxzQkFBc0IsQ0FBQ3BFLE9BQU8sRUFBRThELE9BQU8sRUFBRUksU0FBUyxFQUFFSCxhQUFhLEVBQUVoRyxNQUFNLENBQUM7RUFDeEY7O0VBRUE7QUFDRjtBQUNBO0FBQ0E7QUFDQTtFQUNFLE1BQU1zRyxvQkFBb0JBLENBQ3hCckUsT0FBc0IsRUFDdEI4RCxPQUFlLEdBQUcsRUFBRSxFQUNwQlEsV0FBcUIsR0FBRyxDQUFDLEdBQUcsQ0FBQyxFQUM3QnZHLE1BQU0sR0FBRyxFQUFFLEVBQ2dDO0lBQzNDLE1BQU13RyxHQUFHLEdBQUcsTUFBTSxJQUFJLENBQUNWLGdCQUFnQixDQUFDN0QsT0FBTyxFQUFFOEQsT0FBTyxFQUFFUSxXQUFXLEVBQUV2RyxNQUFNLENBQUM7SUFDOUUsTUFBTSxJQUFBeUcsdUJBQWEsRUFBQ0QsR0FBRyxDQUFDO0lBQ3hCLE9BQU9BLEdBQUc7RUFDWjs7RUFFQTtBQUNGO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7RUFDRSxNQUFNSCxzQkFBc0JBLENBQzFCcEUsT0FBc0IsRUFDdEJ5RSxJQUE4QixFQUM5QlAsU0FBaUIsRUFDakJJLFdBQXFCLEVBQ3JCdkcsTUFBYyxFQUNpQjtJQUMvQixJQUFJLENBQUMsSUFBQVEsZ0JBQVEsRUFBQ3lCLE9BQU8sQ0FBQyxFQUFFO01BQ3RCLE1BQU0sSUFBSUMsU0FBUyxDQUFDLG9DQUFvQyxDQUFDO0lBQzNEO0lBQ0EsSUFBSSxFQUFFeUUsTUFBTSxDQUFDQyxRQUFRLENBQUNGLElBQUksQ0FBQyxJQUFJLE9BQU9BLElBQUksS0FBSyxRQUFRLElBQUksSUFBQTFCLHdCQUFnQixFQUFDMEIsSUFBSSxDQUFDLENBQUMsRUFBRTtNQUNsRixNQUFNLElBQUl0SyxNQUFNLENBQUMwRCxvQkFBb0IsQ0FDbkMsNkRBQTZELE9BQU80RyxJQUFJLFVBQzFFLENBQUM7SUFDSDtJQUNBLElBQUksQ0FBQyxJQUFBekcsZ0JBQVEsRUFBQ2tHLFNBQVMsQ0FBQyxFQUFFO01BQ3hCLE1BQU0sSUFBSWpFLFNBQVMsQ0FBQyxzQ0FBc0MsQ0FBQztJQUM3RDtJQUNBcUUsV0FBVyxDQUFDckIsT0FBTyxDQUFFSyxVQUFVLElBQUs7TUFDbEMsSUFBSSxDQUFDLElBQUFVLGdCQUFRLEVBQUNWLFVBQVUsQ0FBQyxFQUFFO1FBQ3pCLE1BQU0sSUFBSXJELFNBQVMsQ0FBQyx1Q0FBdUMsQ0FBQztNQUM5RDtJQUNGLENBQUMsQ0FBQztJQUNGLElBQUksQ0FBQyxJQUFBakMsZ0JBQVEsRUFBQ0QsTUFBTSxDQUFDLEVBQUU7TUFDckIsTUFBTSxJQUFJa0MsU0FBUyxDQUFDLG1DQUFtQyxDQUFDO0lBQzFEO0lBQ0E7SUFDQSxJQUFJLENBQUMsSUFBSSxDQUFDWCxZQUFZLElBQUk0RSxTQUFTLENBQUNELE1BQU0sS0FBSyxDQUFDLEVBQUU7TUFDaEQsTUFBTSxJQUFJOUosTUFBTSxDQUFDMEQsb0JBQW9CLENBQUMsZ0VBQWdFLENBQUM7SUFDekc7SUFDQTtJQUNBLElBQUksSUFBSSxDQUFDeUIsWUFBWSxJQUFJNEUsU0FBUyxDQUFDRCxNQUFNLEtBQUssRUFBRSxFQUFFO01BQ2hELE1BQU0sSUFBSTlKLE1BQU0sQ0FBQzBELG9CQUFvQixDQUFDLHVCQUF1QnFHLFNBQVMsRUFBRSxDQUFDO0lBQzNFO0lBRUEsTUFBTSxJQUFJLENBQUM5QixvQkFBb0IsQ0FBQyxDQUFDOztJQUVqQztJQUNBckUsTUFBTSxHQUFHQSxNQUFNLEtBQUssTUFBTSxJQUFJLENBQUM2RyxvQkFBb0IsQ0FBQzVFLE9BQU8sQ0FBQ0ssVUFBVyxDQUFDLENBQUM7SUFFekUsTUFBTWIsVUFBVSxHQUFHLElBQUksQ0FBQ3FCLGlCQUFpQixDQUFDO01BQUUsR0FBR2IsT0FBTztNQUFFakM7SUFBTyxDQUFDLENBQUM7SUFDakUsSUFBSSxDQUFDLElBQUksQ0FBQ21CLFNBQVMsRUFBRTtNQUNuQjtNQUNBLElBQUksQ0FBQyxJQUFJLENBQUNJLFlBQVksRUFBRTtRQUN0QjRFLFNBQVMsR0FBRyxrQkFBa0I7TUFDaEM7TUFDQSxNQUFNVyxJQUFJLEdBQUcsSUFBSUMsSUFBSSxDQUFDLENBQUM7TUFDdkJ0RixVQUFVLENBQUN3QixPQUFPLENBQUMsWUFBWSxDQUFDLEdBQUcsSUFBQStELG9CQUFZLEVBQUNGLElBQUksQ0FBQztNQUNyRHJGLFVBQVUsQ0FBQ3dCLE9BQU8sQ0FBQyxzQkFBc0IsQ0FBQyxHQUFHa0QsU0FBUztNQUN0RCxJQUFJLElBQUksQ0FBQ2pGLFlBQVksRUFBRTtRQUNyQk8sVUFBVSxDQUFDd0IsT0FBTyxDQUFDLHNCQUFzQixDQUFDLEdBQUcsSUFBSSxDQUFDL0IsWUFBWTtNQUNoRTtNQUNBTyxVQUFVLENBQUN3QixPQUFPLENBQUNnRSxhQUFhLEdBQUcsSUFBQUMsZUFBTSxFQUFDekYsVUFBVSxFQUFFLElBQUksQ0FBQ1QsU0FBUyxFQUFFLElBQUksQ0FBQ0MsU0FBUyxFQUFFakIsTUFBTSxFQUFFOEcsSUFBSSxFQUFFWCxTQUFTLENBQUM7SUFDaEg7SUFFQSxNQUFNcEIsU0FBUyxHQUFHLElBQUksQ0FBQ0EsU0FBUztJQUNoQyxNQUFNb0MsR0FBRyxHQUFHcEMsU0FBUyxHQUFJcUMsR0FBVyxJQUFLckMsU0FBUyxDQUFDTyxLQUFLLENBQUMsR0FBRzhCLEdBQUcsSUFBSSxDQUFDLEdBQUcsTUFBTSxDQUFDLENBQUM7SUFFL0UsTUFBTXZDLFFBQVEsR0FBRyxNQUFNLElBQUF3Qyx5QkFBZ0IsRUFDckMsSUFBSSxDQUFDaEgsU0FBUyxFQUNkb0IsVUFBVSxFQUNWaUYsSUFBSSxFQUNKLElBQUksQ0FBQzlFLFlBQVksQ0FBQ0MsWUFBWSxLQUFLLElBQUksR0FBRyxDQUFDLEdBQUcsSUFBSSxDQUFDRCxZQUFZLENBQUMwRixpQkFBaUIsRUFDakYsSUFBSSxDQUFDMUYsWUFBWSxDQUFDMkYsV0FBVyxFQUM3QixJQUFJLENBQUMzRixZQUFZLENBQUM0RixjQUFjLEVBQ2hDTCxHQUNGLENBQUM7SUFDRCxJQUFJLENBQUN0QyxRQUFRLENBQUNVLFVBQVUsRUFBRTtNQUN4QixNQUFNLElBQUloRyxLQUFLLENBQUMseUNBQXlDLENBQUM7SUFDNUQ7SUFFQSxJQUFJLENBQUNnSCxXQUFXLENBQUM5RCxRQUFRLENBQUNvQyxRQUFRLENBQUNVLFVBQVUsQ0FBQyxFQUFFO01BQzlDO01BQ0E7TUFDQTtNQUNBO01BQ0E7TUFDQSxPQUFPLElBQUksQ0FBQ2xFLFNBQVMsQ0FBQ1ksT0FBTyxDQUFDSyxVQUFVLENBQUU7TUFFMUMsTUFBTXdDLEdBQUcsR0FBRyxNQUFNNUgsVUFBVSxDQUFDdUssa0JBQWtCLENBQUM1QyxRQUFRLENBQUM7TUFDekQsSUFBSSxDQUFDRCxPQUFPLENBQUNuRCxVQUFVLEVBQUVvRCxRQUFRLEVBQUVDLEdBQUcsQ0FBQztNQUN2QyxNQUFNQSxHQUFHO0lBQ1g7SUFFQSxJQUFJLENBQUNGLE9BQU8sQ0FBQ25ELFVBQVUsRUFBRW9ELFFBQVEsQ0FBQztJQUVsQyxPQUFPQSxRQUFRO0VBQ2pCOztFQUVBO0FBQ0Y7QUFDQTtBQUNBO0FBQ0E7QUFDQTtFQUNFLE1BQU1nQyxvQkFBb0JBLENBQUN2RSxVQUFrQixFQUFtQjtJQUM5RCxJQUFJLENBQUMsSUFBQW9GLHlCQUFpQixFQUFDcEYsVUFBVSxDQUFDLEVBQUU7TUFDbEMsTUFBTSxJQUFJbEcsTUFBTSxDQUFDdUwsc0JBQXNCLENBQUMseUJBQXlCckYsVUFBVSxFQUFFLENBQUM7SUFDaEY7O0lBRUE7SUFDQSxJQUFJLElBQUksQ0FBQ3RDLE1BQU0sRUFBRTtNQUNmLE9BQU8sSUFBSSxDQUFDQSxNQUFNO0lBQ3BCO0lBRUEsTUFBTTRILE1BQU0sR0FBRyxJQUFJLENBQUN2RyxTQUFTLENBQUNpQixVQUFVLENBQUM7SUFDekMsSUFBSXNGLE1BQU0sRUFBRTtNQUNWLE9BQU9BLE1BQU07SUFDZjtJQUVBLE1BQU1DLGtCQUFrQixHQUFHLE1BQU9oRCxRQUE4QixJQUFLO01BQ25FLE1BQU02QixJQUFJLEdBQUcsTUFBTSxJQUFBb0Isc0JBQVksRUFBQ2pELFFBQVEsQ0FBQztNQUN6QyxNQUFNN0UsTUFBTSxHQUFHOUMsVUFBVSxDQUFDNkssaUJBQWlCLENBQUNyQixJQUFJLENBQUMsSUFBSXNCLHVCQUFjO01BQ25FLElBQUksQ0FBQzNHLFNBQVMsQ0FBQ2lCLFVBQVUsQ0FBQyxHQUFHdEMsTUFBTTtNQUNuQyxPQUFPQSxNQUFNO0lBQ2YsQ0FBQztJQUVELE1BQU1nRCxNQUFNLEdBQUcsS0FBSztJQUNwQixNQUFNRSxLQUFLLEdBQUcsVUFBVTtJQUN4QjtJQUNBO0lBQ0E7SUFDQTtJQUNBO0lBQ0E7SUFDQTtJQUNBO0lBQ0E7SUFDQTtJQUNBO0lBQ0EsTUFBTW5DLFNBQVMsR0FBRyxJQUFJLENBQUNBLFNBQVMsSUFBSSxDQUFDa0gsd0JBQVM7SUFDOUMsSUFBSWpJLE1BQWM7SUFDbEIsSUFBSTtNQUNGLE1BQU13RyxHQUFHLEdBQUcsTUFBTSxJQUFJLENBQUNWLGdCQUFnQixDQUFDO1FBQUU5QyxNQUFNO1FBQUVWLFVBQVU7UUFBRVksS0FBSztRQUFFbkM7TUFBVSxDQUFDLEVBQUUsRUFBRSxFQUFFLENBQUMsR0FBRyxDQUFDLEVBQUVpSCx1QkFBYyxDQUFDO01BQzVHLE9BQU9ILGtCQUFrQixDQUFDckIsR0FBRyxDQUFDO0lBQ2hDLENBQUMsQ0FBQyxPQUFPckosQ0FBQyxFQUFFO01BQ1Y7TUFDQSxJQUFJQSxDQUFDLFlBQVlmLE1BQU0sQ0FBQzhMLE9BQU8sRUFBRTtRQUMvQixNQUFNQyxPQUFPLEdBQUdoTCxDQUFDLENBQUNpTCxJQUFJO1FBQ3RCLE1BQU1DLFNBQVMsR0FBR2xMLENBQUMsQ0FBQzZDLE1BQU07UUFDMUIsSUFBSW1JLE9BQU8sS0FBSyxjQUFjLElBQUksQ0FBQ0UsU0FBUyxFQUFFO1VBQzVDLE9BQU9MLHVCQUFjO1FBQ3ZCO01BQ0Y7TUFDQTtNQUNBO01BQ0EsSUFBSSxFQUFFN0ssQ0FBQyxDQUFDbUwsSUFBSSxLQUFLLDhCQUE4QixDQUFDLEVBQUU7UUFDaEQsTUFBTW5MLENBQUM7TUFDVDtNQUNBO01BQ0E2QyxNQUFNLEdBQUc3QyxDQUFDLENBQUNvTCxNQUFnQjtNQUMzQixJQUFJLENBQUN2SSxNQUFNLEVBQUU7UUFDWCxNQUFNN0MsQ0FBQztNQUNUO0lBQ0Y7SUFFQSxNQUFNcUosR0FBRyxHQUFHLE1BQU0sSUFBSSxDQUFDVixnQkFBZ0IsQ0FBQztNQUFFOUMsTUFBTTtNQUFFVixVQUFVO01BQUVZLEtBQUs7TUFBRW5DO0lBQVUsQ0FBQyxFQUFFLEVBQUUsRUFBRSxDQUFDLEdBQUcsQ0FBQyxFQUFFZixNQUFNLENBQUM7SUFDcEcsT0FBTyxNQUFNNkgsa0JBQWtCLENBQUNyQixHQUFHLENBQUM7RUFDdEM7O0VBRUE7QUFDRjtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7RUFDRWdDLFdBQVdBLENBQ1R2RyxPQUFzQixFQUN0QjhELE9BQWUsR0FBRyxFQUFFLEVBQ3BCQyxhQUF1QixHQUFHLENBQUMsR0FBRyxDQUFDLEVBQy9CaEcsTUFBTSxHQUFHLEVBQUUsRUFDWHlJLGNBQXVCLEVBQ3ZCQyxFQUF1RCxFQUN2RDtJQUNBLElBQUlDLElBQW1DO0lBQ3ZDLElBQUlGLGNBQWMsRUFBRTtNQUNsQkUsSUFBSSxHQUFHLElBQUksQ0FBQzdDLGdCQUFnQixDQUFDN0QsT0FBTyxFQUFFOEQsT0FBTyxFQUFFQyxhQUFhLEVBQUVoRyxNQUFNLENBQUM7SUFDdkUsQ0FBQyxNQUFNO01BQ0w7TUFDQTtNQUNBMkksSUFBSSxHQUFHLElBQUksQ0FBQ3JDLG9CQUFvQixDQUFDckUsT0FBTyxFQUFFOEQsT0FBTyxFQUFFQyxhQUFhLEVBQUVoRyxNQUFNLENBQUM7SUFDM0U7SUFFQTJJLElBQUksQ0FBQ0MsSUFBSSxDQUNOQyxNQUFNLElBQUtILEVBQUUsQ0FBQyxJQUFJLEVBQUVHLE1BQU0sQ0FBQyxFQUMzQi9ELEdBQUcsSUFBSztNQUNQO01BQ0E7TUFDQTRELEVBQUUsQ0FBQzVELEdBQUcsQ0FBQztJQUNULENBQ0YsQ0FBQztFQUNIOztFQUVBO0FBQ0Y7QUFDQTtBQUNBO0FBQ0E7QUFDQTtFQUNFZ0UsaUJBQWlCQSxDQUNmN0csT0FBc0IsRUFDdEJ0RyxNQUFnQyxFQUNoQ3dLLFNBQWlCLEVBQ2pCSSxXQUFxQixFQUNyQnZHLE1BQWMsRUFDZHlJLGNBQXVCLEVBQ3ZCQyxFQUF1RCxFQUN2RDtJQUNBLE1BQU1LLFFBQVEsR0FBRyxNQUFBQSxDQUFBLEtBQVk7TUFDM0IsTUFBTXZDLEdBQUcsR0FBRyxNQUFNLElBQUksQ0FBQ0gsc0JBQXNCLENBQUNwRSxPQUFPLEVBQUV0RyxNQUFNLEVBQUV3SyxTQUFTLEVBQUVJLFdBQVcsRUFBRXZHLE1BQU0sQ0FBQztNQUM5RixJQUFJLENBQUN5SSxjQUFjLEVBQUU7UUFDbkIsTUFBTSxJQUFBaEMsdUJBQWEsRUFBQ0QsR0FBRyxDQUFDO01BQzFCO01BRUEsT0FBT0EsR0FBRztJQUNaLENBQUM7SUFFRHVDLFFBQVEsQ0FBQyxDQUFDLENBQUNILElBQUksQ0FDWkMsTUFBTSxJQUFLSCxFQUFFLENBQUMsSUFBSSxFQUFFRyxNQUFNLENBQUM7SUFDNUI7SUFDQTtJQUNDL0QsR0FBRyxJQUFLNEQsRUFBRSxDQUFDNUQsR0FBRyxDQUNqQixDQUFDO0VBQ0g7O0VBRUE7QUFDRjtBQUNBO0VBQ0VrRSxlQUFlQSxDQUFDMUcsVUFBa0IsRUFBRW9HLEVBQTBDLEVBQUU7SUFDOUUsT0FBTyxJQUFJLENBQUM3QixvQkFBb0IsQ0FBQ3ZFLFVBQVUsQ0FBQyxDQUFDc0csSUFBSSxDQUM5Q0MsTUFBTSxJQUFLSCxFQUFFLENBQUMsSUFBSSxFQUFFRyxNQUFNLENBQUM7SUFDNUI7SUFDQTtJQUNDL0QsR0FBRyxJQUFLNEQsRUFBRSxDQUFDNUQsR0FBRyxDQUNqQixDQUFDO0VBQ0g7O0VBRUE7O0VBRUE7QUFDRjtBQUNBO0FBQ0E7RUFDRSxNQUFNbUUsVUFBVUEsQ0FBQzNHLFVBQWtCLEVBQUV0QyxNQUFjLEdBQUcsRUFBRSxFQUFFa0osUUFBd0IsRUFBaUI7SUFDakcsSUFBSSxDQUFDLElBQUF4Qix5QkFBaUIsRUFBQ3BGLFVBQVUsQ0FBQyxFQUFFO01BQ2xDLE1BQU0sSUFBSWxHLE1BQU0sQ0FBQ3VMLHNCQUFzQixDQUFDLHVCQUF1QixHQUFHckYsVUFBVSxDQUFDO0lBQy9FO0lBQ0E7SUFDQSxJQUFJLElBQUE5QixnQkFBUSxFQUFDUixNQUFNLENBQUMsRUFBRTtNQUNwQmtKLFFBQVEsR0FBR2xKLE1BQU07TUFDakJBLE1BQU0sR0FBRyxFQUFFO0lBQ2I7SUFFQSxJQUFJLENBQUMsSUFBQUMsZ0JBQVEsRUFBQ0QsTUFBTSxDQUFDLEVBQUU7TUFDckIsTUFBTSxJQUFJa0MsU0FBUyxDQUFDLG1DQUFtQyxDQUFDO0lBQzFEO0lBQ0EsSUFBSWdILFFBQVEsSUFBSSxDQUFDLElBQUExSSxnQkFBUSxFQUFDMEksUUFBUSxDQUFDLEVBQUU7TUFDbkMsTUFBTSxJQUFJaEgsU0FBUyxDQUFDLHFDQUFxQyxDQUFDO0lBQzVEO0lBRUEsSUFBSTZELE9BQU8sR0FBRyxFQUFFOztJQUVoQjtJQUNBO0lBQ0EsSUFBSS9GLE1BQU0sSUFBSSxJQUFJLENBQUNBLE1BQU0sRUFBRTtNQUN6QixJQUFJQSxNQUFNLEtBQUssSUFBSSxDQUFDQSxNQUFNLEVBQUU7UUFDMUIsTUFBTSxJQUFJNUQsTUFBTSxDQUFDMEQsb0JBQW9CLENBQUMscUJBQXFCLElBQUksQ0FBQ0UsTUFBTSxlQUFlQSxNQUFNLEVBQUUsQ0FBQztNQUNoRztJQUNGO0lBQ0E7SUFDQTtJQUNBLElBQUlBLE1BQU0sSUFBSUEsTUFBTSxLQUFLZ0ksdUJBQWMsRUFBRTtNQUN2Q2pDLE9BQU8sR0FBR3pILEdBQUcsQ0FBQzZLLFdBQVcsQ0FBQztRQUN4QkMseUJBQXlCLEVBQUU7VUFDekJDLENBQUMsRUFBRTtZQUFFQyxLQUFLLEVBQUU7VUFBMEMsQ0FBQztVQUN2REMsa0JBQWtCLEVBQUV2SjtRQUN0QjtNQUNGLENBQUMsQ0FBQztJQUNKO0lBQ0EsTUFBTWdELE1BQU0sR0FBRyxLQUFLO0lBQ3BCLE1BQU1DLE9BQXVCLEdBQUcsQ0FBQyxDQUFDO0lBRWxDLElBQUlpRyxRQUFRLElBQUlBLFFBQVEsQ0FBQ00sYUFBYSxFQUFFO01BQ3RDdkcsT0FBTyxDQUFDLGtDQUFrQyxDQUFDLEdBQUcsSUFBSTtJQUNwRDs7SUFFQTtJQUNBLE1BQU13RyxXQUFXLEdBQUcsSUFBSSxDQUFDekosTUFBTSxJQUFJQSxNQUFNLElBQUlnSSx1QkFBYztJQUUzRCxNQUFNMEIsVUFBeUIsR0FBRztNQUFFMUcsTUFBTTtNQUFFVixVQUFVO01BQUVXO0lBQVEsQ0FBQztJQUVqRSxJQUFJO01BQ0YsTUFBTSxJQUFJLENBQUNxRCxvQkFBb0IsQ0FBQ29ELFVBQVUsRUFBRTNELE9BQU8sRUFBRSxDQUFDLEdBQUcsQ0FBQyxFQUFFMEQsV0FBVyxDQUFDO0lBQzFFLENBQUMsQ0FBQyxPQUFPM0UsR0FBWSxFQUFFO01BQ3JCLElBQUk5RSxNQUFNLEtBQUssRUFBRSxJQUFJQSxNQUFNLEtBQUtnSSx1QkFBYyxFQUFFO1FBQzlDLElBQUlsRCxHQUFHLFlBQVkxSSxNQUFNLENBQUM4TCxPQUFPLEVBQUU7VUFDakMsTUFBTUMsT0FBTyxHQUFHckQsR0FBRyxDQUFDc0QsSUFBSTtVQUN4QixNQUFNQyxTQUFTLEdBQUd2RCxHQUFHLENBQUM5RSxNQUFNO1VBQzVCLElBQUltSSxPQUFPLEtBQUssOEJBQThCLElBQUlFLFNBQVMsS0FBSyxFQUFFLEVBQUU7WUFDbEU7WUFDQSxNQUFNLElBQUksQ0FBQy9CLG9CQUFvQixDQUFDb0QsVUFBVSxFQUFFM0QsT0FBTyxFQUFFLENBQUMsR0FBRyxDQUFDLEVBQUVvQyxPQUFPLENBQUM7VUFDdEU7UUFDRjtNQUNGO01BQ0EsTUFBTXJELEdBQUc7SUFDWDtFQUNGOztFQUVBO0FBQ0Y7QUFDQTtFQUNFLE1BQU02RSxZQUFZQSxDQUFDckgsVUFBa0IsRUFBb0I7SUFDdkQsSUFBSSxDQUFDLElBQUFvRix5QkFBaUIsRUFBQ3BGLFVBQVUsQ0FBQyxFQUFFO01BQ2xDLE1BQU0sSUFBSWxHLE1BQU0sQ0FBQ3VMLHNCQUFzQixDQUFDLHVCQUF1QixHQUFHckYsVUFBVSxDQUFDO0lBQy9FO0lBQ0EsTUFBTVUsTUFBTSxHQUFHLE1BQU07SUFDckIsSUFBSTtNQUNGLE1BQU0sSUFBSSxDQUFDc0Qsb0JBQW9CLENBQUM7UUFBRXRELE1BQU07UUFBRVY7TUFBVyxDQUFDLENBQUM7SUFDekQsQ0FBQyxDQUFDLE9BQU93QyxHQUFHLEVBQUU7TUFDWjtNQUNBLElBQUlBLEdBQUcsQ0FBQ3NELElBQUksS0FBSyxjQUFjLElBQUl0RCxHQUFHLENBQUNzRCxJQUFJLEtBQUssVUFBVSxFQUFFO1FBQzFELE9BQU8sS0FBSztNQUNkO01BQ0EsTUFBTXRELEdBQUc7SUFDWDtJQUVBLE9BQU8sSUFBSTtFQUNiOztFQUlBO0FBQ0Y7QUFDQTs7RUFHRSxNQUFNOEUsWUFBWUEsQ0FBQ3RILFVBQWtCLEVBQWlCO0lBQ3BELElBQUksQ0FBQyxJQUFBb0YseUJBQWlCLEVBQUNwRixVQUFVLENBQUMsRUFBRTtNQUNsQyxNQUFNLElBQUlsRyxNQUFNLENBQUN1TCxzQkFBc0IsQ0FBQyx1QkFBdUIsR0FBR3JGLFVBQVUsQ0FBQztJQUMvRTtJQUNBLE1BQU1VLE1BQU0sR0FBRyxRQUFRO0lBQ3ZCLE1BQU0sSUFBSSxDQUFDc0Qsb0JBQW9CLENBQUM7TUFBRXRELE1BQU07TUFBRVY7SUFBVyxDQUFDLEVBQUUsRUFBRSxFQUFFLENBQUMsR0FBRyxDQUFDLENBQUM7SUFDbEUsT0FBTyxJQUFJLENBQUNqQixTQUFTLENBQUNpQixVQUFVLENBQUM7RUFDbkM7O0VBRUE7QUFDRjtBQUNBO0VBQ0UsTUFBTXVILFNBQVNBLENBQUN2SCxVQUFrQixFQUFFQyxVQUFrQixFQUFFdUgsT0FBdUIsRUFBNEI7SUFDekcsSUFBSSxDQUFDLElBQUFwQyx5QkFBaUIsRUFBQ3BGLFVBQVUsQ0FBQyxFQUFFO01BQ2xDLE1BQU0sSUFBSWxHLE1BQU0sQ0FBQ3VMLHNCQUFzQixDQUFDLHVCQUF1QixHQUFHckYsVUFBVSxDQUFDO0lBQy9FO0lBQ0EsSUFBSSxDQUFDLElBQUF5SCx5QkFBaUIsRUFBQ3hILFVBQVUsQ0FBQyxFQUFFO01BQ2xDLE1BQU0sSUFBSW5HLE1BQU0sQ0FBQzROLHNCQUFzQixDQUFDLHdCQUF3QnpILFVBQVUsRUFBRSxDQUFDO0lBQy9FO0lBQ0EsT0FBTyxJQUFJLENBQUMwSCxnQkFBZ0IsQ0FBQzNILFVBQVUsRUFBRUMsVUFBVSxFQUFFLENBQUMsRUFBRSxDQUFDLEVBQUV1SCxPQUFPLENBQUM7RUFDckU7O0VBRUE7QUFDRjtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtFQUNFLE1BQU1HLGdCQUFnQkEsQ0FDcEIzSCxVQUFrQixFQUNsQkMsVUFBa0IsRUFDbEIySCxNQUFjLEVBQ2RoRSxNQUFNLEdBQUcsQ0FBQyxFQUNWNEQsT0FBdUIsRUFDRztJQUMxQixJQUFJLENBQUMsSUFBQXBDLHlCQUFpQixFQUFDcEYsVUFBVSxDQUFDLEVBQUU7TUFDbEMsTUFBTSxJQUFJbEcsTUFBTSxDQUFDdUwsc0JBQXNCLENBQUMsdUJBQXVCLEdBQUdyRixVQUFVLENBQUM7SUFDL0U7SUFDQSxJQUFJLENBQUMsSUFBQXlILHlCQUFpQixFQUFDeEgsVUFBVSxDQUFDLEVBQUU7TUFDbEMsTUFBTSxJQUFJbkcsTUFBTSxDQUFDNE4sc0JBQXNCLENBQUMsd0JBQXdCekgsVUFBVSxFQUFFLENBQUM7SUFDL0U7SUFDQSxJQUFJLENBQUMsSUFBQTBELGdCQUFRLEVBQUNpRSxNQUFNLENBQUMsRUFBRTtNQUNyQixNQUFNLElBQUloSSxTQUFTLENBQUMsbUNBQW1DLENBQUM7SUFDMUQ7SUFDQSxJQUFJLENBQUMsSUFBQStELGdCQUFRLEVBQUNDLE1BQU0sQ0FBQyxFQUFFO01BQ3JCLE1BQU0sSUFBSWhFLFNBQVMsQ0FBQyxtQ0FBbUMsQ0FBQztJQUMxRDtJQUVBLElBQUlpSSxLQUFLLEdBQUcsRUFBRTtJQUNkLElBQUlELE1BQU0sSUFBSWhFLE1BQU0sRUFBRTtNQUNwQixJQUFJZ0UsTUFBTSxFQUFFO1FBQ1ZDLEtBQUssR0FBRyxTQUFTLENBQUNELE1BQU0sR0FBRztNQUM3QixDQUFDLE1BQU07UUFDTEMsS0FBSyxHQUFHLFVBQVU7UUFDbEJELE1BQU0sR0FBRyxDQUFDO01BQ1o7TUFDQSxJQUFJaEUsTUFBTSxFQUFFO1FBQ1ZpRSxLQUFLLElBQUksR0FBRyxDQUFDakUsTUFBTSxHQUFHZ0UsTUFBTSxHQUFHLENBQUMsRUFBRTtNQUNwQztJQUNGO0lBRUEsSUFBSWhILEtBQUssR0FBRyxFQUFFO0lBQ2QsSUFBSUQsT0FBdUIsR0FBRztNQUM1QixJQUFJa0gsS0FBSyxLQUFLLEVBQUUsSUFBSTtRQUFFQTtNQUFNLENBQUM7SUFDL0IsQ0FBQztJQUVELElBQUlMLE9BQU8sRUFBRTtNQUNYLE1BQU1NLFVBQWtDLEdBQUc7UUFDekMsSUFBSU4sT0FBTyxDQUFDTyxvQkFBb0IsSUFBSTtVQUNsQyxpREFBaUQsRUFBRVAsT0FBTyxDQUFDTztRQUM3RCxDQUFDLENBQUM7UUFDRixJQUFJUCxPQUFPLENBQUNRLGNBQWMsSUFBSTtVQUFFLDJDQUEyQyxFQUFFUixPQUFPLENBQUNRO1FBQWUsQ0FBQyxDQUFDO1FBQ3RHLElBQUlSLE9BQU8sQ0FBQ1MsaUJBQWlCLElBQUk7VUFDL0IsK0NBQStDLEVBQUVULE9BQU8sQ0FBQ1M7UUFDM0QsQ0FBQztNQUNILENBQUM7TUFDRHJILEtBQUssR0FBR3NILG9CQUFFLENBQUM5RSxTQUFTLENBQUNvRSxPQUFPLENBQUM7TUFDN0I3RyxPQUFPLEdBQUc7UUFDUixHQUFHLElBQUF3SCx1QkFBZSxFQUFDTCxVQUFVLENBQUM7UUFDOUIsR0FBR25IO01BQ0wsQ0FBQztJQUNIO0lBRUEsTUFBTXlILG1CQUFtQixHQUFHLENBQUMsR0FBRyxDQUFDO0lBQ2pDLElBQUlQLEtBQUssRUFBRTtNQUNUTyxtQkFBbUIsQ0FBQ0MsSUFBSSxDQUFDLEdBQUcsQ0FBQztJQUMvQjtJQUNBLE1BQU0zSCxNQUFNLEdBQUcsS0FBSztJQUVwQixPQUFPLE1BQU0sSUFBSSxDQUFDOEMsZ0JBQWdCLENBQUM7TUFBRTlDLE1BQU07TUFBRVYsVUFBVTtNQUFFQyxVQUFVO01BQUVVLE9BQU87TUFBRUM7SUFBTSxDQUFDLEVBQUUsRUFBRSxFQUFFd0gsbUJBQW1CLENBQUM7RUFDakg7O0VBRUE7QUFDRjtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0VBQ0UsTUFBTUUsVUFBVUEsQ0FBQ3RJLFVBQWtCLEVBQUVDLFVBQWtCLEVBQUVzSSxRQUFnQixFQUFFZixPQUF1QixFQUFpQjtJQUNqSDtJQUNBLElBQUksQ0FBQyxJQUFBcEMseUJBQWlCLEVBQUNwRixVQUFVLENBQUMsRUFBRTtNQUNsQyxNQUFNLElBQUlsRyxNQUFNLENBQUN1TCxzQkFBc0IsQ0FBQyx1QkFBdUIsR0FBR3JGLFVBQVUsQ0FBQztJQUMvRTtJQUNBLElBQUksQ0FBQyxJQUFBeUgseUJBQWlCLEVBQUN4SCxVQUFVLENBQUMsRUFBRTtNQUNsQyxNQUFNLElBQUluRyxNQUFNLENBQUM0TixzQkFBc0IsQ0FBQyx3QkFBd0J6SCxVQUFVLEVBQUUsQ0FBQztJQUMvRTtJQUNBLElBQUksQ0FBQyxJQUFBdEMsZ0JBQVEsRUFBQzRLLFFBQVEsQ0FBQyxFQUFFO01BQ3ZCLE1BQU0sSUFBSTNJLFNBQVMsQ0FBQyxxQ0FBcUMsQ0FBQztJQUM1RDtJQUVBLE1BQU00SSxpQkFBaUIsR0FBRyxNQUFBQSxDQUFBLEtBQTZCO01BQ3JELElBQUlDLGNBQStCO01BQ25DLE1BQU1DLE9BQU8sR0FBRyxNQUFNLElBQUksQ0FBQ0MsVUFBVSxDQUFDM0ksVUFBVSxFQUFFQyxVQUFVLEVBQUV1SCxPQUFPLENBQUM7TUFDdEUsTUFBTW9CLFdBQVcsR0FBR3ZFLE1BQU0sQ0FBQ3dFLElBQUksQ0FBQ0gsT0FBTyxDQUFDSSxJQUFJLENBQUMsQ0FBQ2xILFFBQVEsQ0FBQyxRQUFRLENBQUM7TUFDaEUsTUFBTW1ILFFBQVEsR0FBRyxHQUFHUixRQUFRLElBQUlLLFdBQVcsYUFBYTtNQUV4RCxNQUFNSSxXQUFHLENBQUNDLEtBQUssQ0FBQzdQLElBQUksQ0FBQzhQLE9BQU8sQ0FBQ1gsUUFBUSxDQUFDLEVBQUU7UUFBRVksU0FBUyxFQUFFO01BQUssQ0FBQyxDQUFDO01BRTVELElBQUl2QixNQUFNLEdBQUcsQ0FBQztNQUNkLElBQUk7UUFDRixNQUFNd0IsS0FBSyxHQUFHLE1BQU1KLFdBQUcsQ0FBQ0ssSUFBSSxDQUFDTixRQUFRLENBQUM7UUFDdEMsSUFBSUwsT0FBTyxDQUFDWSxJQUFJLEtBQUtGLEtBQUssQ0FBQ0UsSUFBSSxFQUFFO1VBQy9CLE9BQU9QLFFBQVE7UUFDakI7UUFDQW5CLE1BQU0sR0FBR3dCLEtBQUssQ0FBQ0UsSUFBSTtRQUNuQmIsY0FBYyxHQUFHeFAsRUFBRSxDQUFDc1EsaUJBQWlCLENBQUNSLFFBQVEsRUFBRTtVQUFFUyxLQUFLLEVBQUU7UUFBSSxDQUFDLENBQUM7TUFDakUsQ0FBQyxDQUFDLE9BQU8zTyxDQUFDLEVBQUU7UUFDVixJQUFJQSxDQUFDLFlBQVlvQyxLQUFLLElBQUtwQyxDQUFDLENBQWlDaUwsSUFBSSxLQUFLLFFBQVEsRUFBRTtVQUM5RTtVQUNBMkMsY0FBYyxHQUFHeFAsRUFBRSxDQUFDc1EsaUJBQWlCLENBQUNSLFFBQVEsRUFBRTtZQUFFUyxLQUFLLEVBQUU7VUFBSSxDQUFDLENBQUM7UUFDakUsQ0FBQyxNQUFNO1VBQ0w7VUFDQSxNQUFNM08sQ0FBQztRQUNUO01BQ0Y7TUFFQSxNQUFNNE8sY0FBYyxHQUFHLE1BQU0sSUFBSSxDQUFDOUIsZ0JBQWdCLENBQUMzSCxVQUFVLEVBQUVDLFVBQVUsRUFBRTJILE1BQU0sRUFBRSxDQUFDLEVBQUVKLE9BQU8sQ0FBQztNQUU5RixNQUFNa0MscUJBQWEsQ0FBQ0MsUUFBUSxDQUFDRixjQUFjLEVBQUVoQixjQUFjLENBQUM7TUFDNUQsTUFBTVcsS0FBSyxHQUFHLE1BQU1KLFdBQUcsQ0FBQ0ssSUFBSSxDQUFDTixRQUFRLENBQUM7TUFDdEMsSUFBSUssS0FBSyxDQUFDRSxJQUFJLEtBQUtaLE9BQU8sQ0FBQ1ksSUFBSSxFQUFFO1FBQy9CLE9BQU9QLFFBQVE7TUFDakI7TUFFQSxNQUFNLElBQUk5TCxLQUFLLENBQUMsc0RBQXNELENBQUM7SUFDekUsQ0FBQztJQUVELE1BQU04TCxRQUFRLEdBQUcsTUFBTVAsaUJBQWlCLENBQUMsQ0FBQztJQUMxQyxNQUFNUSxXQUFHLENBQUNZLE1BQU0sQ0FBQ2IsUUFBUSxFQUFFUixRQUFRLENBQUM7RUFDdEM7O0VBRUE7QUFDRjtBQUNBO0VBQ0UsTUFBTUksVUFBVUEsQ0FBQzNJLFVBQWtCLEVBQUVDLFVBQWtCLEVBQUU0SixRQUF5QixFQUEyQjtJQUMzRyxNQUFNQyxVQUFVLEdBQUdELFFBQVEsSUFBSSxDQUFDLENBQUM7SUFDakMsSUFBSSxDQUFDLElBQUF6RSx5QkFBaUIsRUFBQ3BGLFVBQVUsQ0FBQyxFQUFFO01BQ2xDLE1BQU0sSUFBSWxHLE1BQU0sQ0FBQ3VMLHNCQUFzQixDQUFDLHVCQUF1QixHQUFHckYsVUFBVSxDQUFDO0lBQy9FO0lBQ0EsSUFBSSxDQUFDLElBQUF5SCx5QkFBaUIsRUFBQ3hILFVBQVUsQ0FBQyxFQUFFO01BQ2xDLE1BQU0sSUFBSW5HLE1BQU0sQ0FBQzROLHNCQUFzQixDQUFDLHdCQUF3QnpILFVBQVUsRUFBRSxDQUFDO0lBQy9FO0lBRUEsSUFBSSxDQUFDLElBQUEvQixnQkFBUSxFQUFDNEwsVUFBVSxDQUFDLEVBQUU7TUFDekIsTUFBTSxJQUFJaFEsTUFBTSxDQUFDMEQsb0JBQW9CLENBQUMscUNBQXFDLENBQUM7SUFDOUU7SUFFQSxNQUFNb0QsS0FBSyxHQUFHc0gsb0JBQUUsQ0FBQzlFLFNBQVMsQ0FBQzBHLFVBQVUsQ0FBQztJQUN0QyxNQUFNcEosTUFBTSxHQUFHLE1BQU07SUFDckIsTUFBTXdELEdBQUcsR0FBRyxNQUFNLElBQUksQ0FBQ0Ysb0JBQW9CLENBQUM7TUFBRXRELE1BQU07TUFBRVYsVUFBVTtNQUFFQyxVQUFVO01BQUVXO0lBQU0sQ0FBQyxDQUFDO0lBRXRGLE9BQU87TUFDTDBJLElBQUksRUFBRVMsUUFBUSxDQUFDN0YsR0FBRyxDQUFDdkQsT0FBTyxDQUFDLGdCQUFnQixDQUFXLENBQUM7TUFDdkRxSixRQUFRLEVBQUUsSUFBQUMsdUJBQWUsRUFBQy9GLEdBQUcsQ0FBQ3ZELE9BQXlCLENBQUM7TUFDeER1SixZQUFZLEVBQUUsSUFBSXpGLElBQUksQ0FBQ1AsR0FBRyxDQUFDdkQsT0FBTyxDQUFDLGVBQWUsQ0FBVyxDQUFDO01BQzlEd0osU0FBUyxFQUFFLElBQUFDLG9CQUFZLEVBQUNsRyxHQUFHLENBQUN2RCxPQUF5QixDQUFDO01BQ3REbUksSUFBSSxFQUFFLElBQUF1QixvQkFBWSxFQUFDbkcsR0FBRyxDQUFDdkQsT0FBTyxDQUFDbUksSUFBSTtJQUNyQyxDQUFDO0VBQ0g7RUFFQSxNQUFNd0IsWUFBWUEsQ0FBQ3RLLFVBQWtCLEVBQUVDLFVBQWtCLEVBQUVzSyxVQUEwQixFQUFpQjtJQUNwRyxJQUFJLENBQUMsSUFBQW5GLHlCQUFpQixFQUFDcEYsVUFBVSxDQUFDLEVBQUU7TUFDbEMsTUFBTSxJQUFJbEcsTUFBTSxDQUFDdUwsc0JBQXNCLENBQUMsd0JBQXdCckYsVUFBVSxFQUFFLENBQUM7SUFDL0U7SUFDQSxJQUFJLENBQUMsSUFBQXlILHlCQUFpQixFQUFDeEgsVUFBVSxDQUFDLEVBQUU7TUFDbEMsTUFBTSxJQUFJbkcsTUFBTSxDQUFDNE4sc0JBQXNCLENBQUMsd0JBQXdCekgsVUFBVSxFQUFFLENBQUM7SUFDL0U7SUFFQSxJQUFJc0ssVUFBVSxJQUFJLENBQUMsSUFBQXJNLGdCQUFRLEVBQUNxTSxVQUFVLENBQUMsRUFBRTtNQUN2QyxNQUFNLElBQUl6USxNQUFNLENBQUMwRCxvQkFBb0IsQ0FBQyx1Q0FBdUMsQ0FBQztJQUNoRjtJQUVBLE1BQU1rRCxNQUFNLEdBQUcsUUFBUTtJQUV2QixNQUFNQyxPQUF1QixHQUFHLENBQUMsQ0FBQztJQUNsQyxJQUFJNEosVUFBVSxhQUFWQSxVQUFVLGVBQVZBLFVBQVUsQ0FBRUMsZ0JBQWdCLEVBQUU7TUFDaEM3SixPQUFPLENBQUMsbUNBQW1DLENBQUMsR0FBRyxJQUFJO0lBQ3JEO0lBQ0EsSUFBSTRKLFVBQVUsYUFBVkEsVUFBVSxlQUFWQSxVQUFVLENBQUVFLFdBQVcsRUFBRTtNQUMzQjlKLE9BQU8sQ0FBQyxzQkFBc0IsQ0FBQyxHQUFHLElBQUk7SUFDeEM7SUFFQSxNQUFNK0osV0FBbUMsR0FBRyxDQUFDLENBQUM7SUFDOUMsSUFBSUgsVUFBVSxhQUFWQSxVQUFVLGVBQVZBLFVBQVUsQ0FBRUosU0FBUyxFQUFFO01BQ3pCTyxXQUFXLENBQUNQLFNBQVMsR0FBRyxHQUFHSSxVQUFVLENBQUNKLFNBQVMsRUFBRTtJQUNuRDtJQUNBLE1BQU12SixLQUFLLEdBQUdzSCxvQkFBRSxDQUFDOUUsU0FBUyxDQUFDc0gsV0FBVyxDQUFDO0lBRXZDLE1BQU0sSUFBSSxDQUFDMUcsb0JBQW9CLENBQUM7TUFBRXRELE1BQU07TUFBRVYsVUFBVTtNQUFFQyxVQUFVO01BQUVVLE9BQU87TUFBRUM7SUFBTSxDQUFDLEVBQUUsRUFBRSxFQUFFLENBQUMsR0FBRyxFQUFFLEdBQUcsQ0FBQyxDQUFDO0VBQ3JHOztFQUVBOztFQUVBK0oscUJBQXFCQSxDQUNuQkMsTUFBYyxFQUNkQyxNQUFjLEVBQ2QxQixTQUFrQixFQUMwQjtJQUM1QyxJQUFJMEIsTUFBTSxLQUFLN04sU0FBUyxFQUFFO01BQ3hCNk4sTUFBTSxHQUFHLEVBQUU7SUFDYjtJQUNBLElBQUkxQixTQUFTLEtBQUtuTSxTQUFTLEVBQUU7TUFDM0JtTSxTQUFTLEdBQUcsS0FBSztJQUNuQjtJQUNBLElBQUksQ0FBQyxJQUFBL0QseUJBQWlCLEVBQUN3RixNQUFNLENBQUMsRUFBRTtNQUM5QixNQUFNLElBQUk5USxNQUFNLENBQUN1TCxzQkFBc0IsQ0FBQyx1QkFBdUIsR0FBR3VGLE1BQU0sQ0FBQztJQUMzRTtJQUNBLElBQUksQ0FBQyxJQUFBRSxxQkFBYSxFQUFDRCxNQUFNLENBQUMsRUFBRTtNQUMxQixNQUFNLElBQUkvUSxNQUFNLENBQUNpUixrQkFBa0IsQ0FBQyxvQkFBb0JGLE1BQU0sRUFBRSxDQUFDO0lBQ25FO0lBQ0EsSUFBSSxDQUFDLElBQUFwTixpQkFBUyxFQUFDMEwsU0FBUyxDQUFDLEVBQUU7TUFDekIsTUFBTSxJQUFJdkosU0FBUyxDQUFDLHVDQUF1QyxDQUFDO0lBQzlEO0lBQ0EsTUFBTW9MLFNBQVMsR0FBRzdCLFNBQVMsR0FBRyxFQUFFLEdBQUcsR0FBRztJQUN0QyxJQUFJOEIsU0FBUyxHQUFHLEVBQUU7SUFDbEIsSUFBSUMsY0FBYyxHQUFHLEVBQUU7SUFDdkIsTUFBTUMsT0FBa0IsR0FBRyxFQUFFO0lBQzdCLElBQUlDLEtBQUssR0FBRyxLQUFLOztJQUVqQjtJQUNBLE1BQU1DLFVBQVUsR0FBRyxJQUFJaFMsTUFBTSxDQUFDaVMsUUFBUSxDQUFDO01BQUVDLFVBQVUsRUFBRTtJQUFLLENBQUMsQ0FBQztJQUM1REYsVUFBVSxDQUFDRyxLQUFLLEdBQUcsTUFBTTtNQUN2QjtNQUNBLElBQUlMLE9BQU8sQ0FBQ3ZILE1BQU0sRUFBRTtRQUNsQixPQUFPeUgsVUFBVSxDQUFDaEQsSUFBSSxDQUFDOEMsT0FBTyxDQUFDTSxLQUFLLENBQUMsQ0FBQyxDQUFDO01BQ3pDO01BQ0EsSUFBSUwsS0FBSyxFQUFFO1FBQ1QsT0FBT0MsVUFBVSxDQUFDaEQsSUFBSSxDQUFDLElBQUksQ0FBQztNQUM5QjtNQUNBLElBQUksQ0FBQ3FELDBCQUEwQixDQUFDZCxNQUFNLEVBQUVDLE1BQU0sRUFBRUksU0FBUyxFQUFFQyxjQUFjLEVBQUVGLFNBQVMsQ0FBQyxDQUFDMUUsSUFBSSxDQUN2RkMsTUFBTSxJQUFLO1FBQ1Y7UUFDQTtRQUNBQSxNQUFNLENBQUNvRixRQUFRLENBQUMvSSxPQUFPLENBQUVpSSxNQUFNLElBQUtNLE9BQU8sQ0FBQzlDLElBQUksQ0FBQ3dDLE1BQU0sQ0FBQyxDQUFDO1FBQ3pEdlIsS0FBSyxDQUFDc1MsVUFBVSxDQUNkckYsTUFBTSxDQUFDNEUsT0FBTyxFQUNkLENBQUNVLE1BQU0sRUFBRXpGLEVBQUUsS0FBSztVQUNkO1VBQ0E7VUFDQTtVQUNBLElBQUksQ0FBQzBGLFNBQVMsQ0FBQ2xCLE1BQU0sRUFBRWlCLE1BQU0sQ0FBQ0UsR0FBRyxFQUFFRixNQUFNLENBQUNHLFFBQVEsQ0FBQyxDQUFDMUYsSUFBSSxDQUNyRDJGLEtBQWEsSUFBSztZQUNqQjtZQUNBO1lBQ0FKLE1BQU0sQ0FBQ3ZDLElBQUksR0FBRzJDLEtBQUssQ0FBQ0MsTUFBTSxDQUFDLENBQUNDLEdBQUcsRUFBRUMsSUFBSSxLQUFLRCxHQUFHLEdBQUdDLElBQUksQ0FBQzlDLElBQUksRUFBRSxDQUFDLENBQUM7WUFDN0Q2QixPQUFPLENBQUM5QyxJQUFJLENBQUN3RCxNQUFNLENBQUM7WUFDcEJ6RixFQUFFLENBQUMsQ0FBQztVQUNOLENBQUMsRUFDQTVELEdBQVUsSUFBSzRELEVBQUUsQ0FBQzVELEdBQUcsQ0FDeEIsQ0FBQztRQUNILENBQUMsRUFDQUEsR0FBRyxJQUFLO1VBQ1AsSUFBSUEsR0FBRyxFQUFFO1lBQ1A2SSxVQUFVLENBQUNnQixJQUFJLENBQUMsT0FBTyxFQUFFN0osR0FBRyxDQUFDO1lBQzdCO1VBQ0Y7VUFDQSxJQUFJK0QsTUFBTSxDQUFDK0YsV0FBVyxFQUFFO1lBQ3RCckIsU0FBUyxHQUFHMUUsTUFBTSxDQUFDZ0csYUFBYTtZQUNoQ3JCLGNBQWMsR0FBRzNFLE1BQU0sQ0FBQ2lHLGtCQUFrQjtVQUM1QyxDQUFDLE1BQU07WUFDTHBCLEtBQUssR0FBRyxJQUFJO1VBQ2Q7O1VBRUE7VUFDQTtVQUNBQyxVQUFVLENBQUNHLEtBQUssQ0FBQyxDQUFDO1FBQ3BCLENBQ0YsQ0FBQztNQUNILENBQUMsRUFDQTNRLENBQUMsSUFBSztRQUNMd1EsVUFBVSxDQUFDZ0IsSUFBSSxDQUFDLE9BQU8sRUFBRXhSLENBQUMsQ0FBQztNQUM3QixDQUNGLENBQUM7SUFDSCxDQUFDO0lBQ0QsT0FBT3dRLFVBQVU7RUFDbkI7O0VBRUE7QUFDRjtBQUNBO0VBQ0UsTUFBTUssMEJBQTBCQSxDQUM5QjFMLFVBQWtCLEVBQ2xCNkssTUFBYyxFQUNkSSxTQUFpQixFQUNqQkMsY0FBc0IsRUFDdEJGLFNBQWlCLEVBQ2E7SUFDOUIsSUFBSSxDQUFDLElBQUE1Rix5QkFBaUIsRUFBQ3BGLFVBQVUsQ0FBQyxFQUFFO01BQ2xDLE1BQU0sSUFBSWxHLE1BQU0sQ0FBQ3VMLHNCQUFzQixDQUFDLHVCQUF1QixHQUFHckYsVUFBVSxDQUFDO0lBQy9FO0lBQ0EsSUFBSSxDQUFDLElBQUFyQyxnQkFBUSxFQUFDa04sTUFBTSxDQUFDLEVBQUU7TUFDckIsTUFBTSxJQUFJakwsU0FBUyxDQUFDLG1DQUFtQyxDQUFDO0lBQzFEO0lBQ0EsSUFBSSxDQUFDLElBQUFqQyxnQkFBUSxFQUFDc04sU0FBUyxDQUFDLEVBQUU7TUFDeEIsTUFBTSxJQUFJckwsU0FBUyxDQUFDLHNDQUFzQyxDQUFDO0lBQzdEO0lBQ0EsSUFBSSxDQUFDLElBQUFqQyxnQkFBUSxFQUFDdU4sY0FBYyxDQUFDLEVBQUU7TUFDN0IsTUFBTSxJQUFJdEwsU0FBUyxDQUFDLDJDQUEyQyxDQUFDO0lBQ2xFO0lBQ0EsSUFBSSxDQUFDLElBQUFqQyxnQkFBUSxFQUFDcU4sU0FBUyxDQUFDLEVBQUU7TUFDeEIsTUFBTSxJQUFJcEwsU0FBUyxDQUFDLHNDQUFzQyxDQUFDO0lBQzdEO0lBQ0EsTUFBTTZNLE9BQU8sR0FBRyxFQUFFO0lBQ2xCQSxPQUFPLENBQUNwRSxJQUFJLENBQUMsVUFBVSxJQUFBcUUsaUJBQVMsRUFBQzdCLE1BQU0sQ0FBQyxFQUFFLENBQUM7SUFDM0M0QixPQUFPLENBQUNwRSxJQUFJLENBQUMsYUFBYSxJQUFBcUUsaUJBQVMsRUFBQzFCLFNBQVMsQ0FBQyxFQUFFLENBQUM7SUFFakQsSUFBSUMsU0FBUyxFQUFFO01BQ2J3QixPQUFPLENBQUNwRSxJQUFJLENBQUMsY0FBYyxJQUFBcUUsaUJBQVMsRUFBQ3pCLFNBQVMsQ0FBQyxFQUFFLENBQUM7SUFDcEQ7SUFDQSxJQUFJQyxjQUFjLEVBQUU7TUFDbEJ1QixPQUFPLENBQUNwRSxJQUFJLENBQUMsb0JBQW9CNkMsY0FBYyxFQUFFLENBQUM7SUFDcEQ7SUFFQSxNQUFNeUIsVUFBVSxHQUFHLElBQUk7SUFDdkJGLE9BQU8sQ0FBQ3BFLElBQUksQ0FBQyxlQUFlc0UsVUFBVSxFQUFFLENBQUM7SUFDekNGLE9BQU8sQ0FBQ0csSUFBSSxDQUFDLENBQUM7SUFDZEgsT0FBTyxDQUFDSSxPQUFPLENBQUMsU0FBUyxDQUFDO0lBQzFCLElBQUlqTSxLQUFLLEdBQUcsRUFBRTtJQUNkLElBQUk2TCxPQUFPLENBQUM3SSxNQUFNLEdBQUcsQ0FBQyxFQUFFO01BQ3RCaEQsS0FBSyxHQUFHLEdBQUc2TCxPQUFPLENBQUNLLElBQUksQ0FBQyxHQUFHLENBQUMsRUFBRTtJQUNoQztJQUNBLE1BQU1wTSxNQUFNLEdBQUcsS0FBSztJQUNwQixNQUFNd0QsR0FBRyxHQUFHLE1BQU0sSUFBSSxDQUFDVixnQkFBZ0IsQ0FBQztNQUFFOUMsTUFBTTtNQUFFVixVQUFVO01BQUVZO0lBQU0sQ0FBQyxDQUFDO0lBQ3RFLE1BQU13RCxJQUFJLEdBQUcsTUFBTSxJQUFBb0Isc0JBQVksRUFBQ3RCLEdBQUcsQ0FBQztJQUNwQyxPQUFPdEosVUFBVSxDQUFDbVMsa0JBQWtCLENBQUMzSSxJQUFJLENBQUM7RUFDNUM7O0VBRUE7QUFDRjtBQUNBO0FBQ0E7RUFDRSxNQUFNNEksMEJBQTBCQSxDQUFDaE4sVUFBa0IsRUFBRUMsVUFBa0IsRUFBRVUsT0FBdUIsRUFBbUI7SUFDakgsSUFBSSxDQUFDLElBQUF5RSx5QkFBaUIsRUFBQ3BGLFVBQVUsQ0FBQyxFQUFFO01BQ2xDLE1BQU0sSUFBSWxHLE1BQU0sQ0FBQ3VMLHNCQUFzQixDQUFDLHVCQUF1QixHQUFHckYsVUFBVSxDQUFDO0lBQy9FO0lBQ0EsSUFBSSxDQUFDLElBQUF5SCx5QkFBaUIsRUFBQ3hILFVBQVUsQ0FBQyxFQUFFO01BQ2xDLE1BQU0sSUFBSW5HLE1BQU0sQ0FBQzROLHNCQUFzQixDQUFDLHdCQUF3QnpILFVBQVUsRUFBRSxDQUFDO0lBQy9FO0lBQ0EsSUFBSSxDQUFDLElBQUEvQixnQkFBUSxFQUFDeUMsT0FBTyxDQUFDLEVBQUU7TUFDdEIsTUFBTSxJQUFJN0csTUFBTSxDQUFDNE4sc0JBQXNCLENBQUMsd0NBQXdDLENBQUM7SUFDbkY7SUFDQSxNQUFNaEgsTUFBTSxHQUFHLE1BQU07SUFDckIsTUFBTUUsS0FBSyxHQUFHLFNBQVM7SUFDdkIsTUFBTXNELEdBQUcsR0FBRyxNQUFNLElBQUksQ0FBQ1YsZ0JBQWdCLENBQUM7TUFBRTlDLE1BQU07TUFBRVYsVUFBVTtNQUFFQyxVQUFVO01BQUVXLEtBQUs7TUFBRUQ7SUFBUSxDQUFDLENBQUM7SUFDM0YsTUFBTXlELElBQUksR0FBRyxNQUFNLElBQUE2SSxzQkFBWSxFQUFDL0ksR0FBRyxDQUFDO0lBQ3BDLE9BQU8sSUFBQWdKLGlDQUFzQixFQUFDOUksSUFBSSxDQUFDeEMsUUFBUSxDQUFDLENBQUMsQ0FBQztFQUNoRDs7RUFFQTtBQUNGO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtFQUNFLE1BQU11TCxvQkFBb0JBLENBQUNuTixVQUFrQixFQUFFQyxVQUFrQixFQUFFK0wsUUFBZ0IsRUFBaUI7SUFDbEcsTUFBTXRMLE1BQU0sR0FBRyxRQUFRO0lBQ3ZCLE1BQU1FLEtBQUssR0FBRyxZQUFZb0wsUUFBUSxFQUFFO0lBRXBDLE1BQU1vQixjQUFjLEdBQUc7TUFBRTFNLE1BQU07TUFBRVYsVUFBVTtNQUFFQyxVQUFVLEVBQUVBLFVBQVU7TUFBRVc7SUFBTSxDQUFDO0lBQzVFLE1BQU0sSUFBSSxDQUFDb0Qsb0JBQW9CLENBQUNvSixjQUFjLEVBQUUsRUFBRSxFQUFFLENBQUMsR0FBRyxDQUFDLENBQUM7RUFDNUQ7RUFFQSxNQUFNQyxZQUFZQSxDQUFDck4sVUFBa0IsRUFBRUMsVUFBa0IsRUFBK0I7SUFBQSxJQUFBcU4sYUFBQTtJQUN0RixJQUFJLENBQUMsSUFBQWxJLHlCQUFpQixFQUFDcEYsVUFBVSxDQUFDLEVBQUU7TUFDbEMsTUFBTSxJQUFJbEcsTUFBTSxDQUFDdUwsc0JBQXNCLENBQUMsdUJBQXVCLEdBQUdyRixVQUFVLENBQUM7SUFDL0U7SUFDQSxJQUFJLENBQUMsSUFBQXlILHlCQUFpQixFQUFDeEgsVUFBVSxDQUFDLEVBQUU7TUFDbEMsTUFBTSxJQUFJbkcsTUFBTSxDQUFDNE4sc0JBQXNCLENBQUMsd0JBQXdCekgsVUFBVSxFQUFFLENBQUM7SUFDL0U7SUFFQSxJQUFJc04sWUFBZ0U7SUFDcEUsSUFBSXRDLFNBQVMsR0FBRyxFQUFFO0lBQ2xCLElBQUlDLGNBQWMsR0FBRyxFQUFFO0lBQ3ZCLFNBQVM7TUFDUCxNQUFNM0UsTUFBTSxHQUFHLE1BQU0sSUFBSSxDQUFDbUYsMEJBQTBCLENBQUMxTCxVQUFVLEVBQUVDLFVBQVUsRUFBRWdMLFNBQVMsRUFBRUMsY0FBYyxFQUFFLEVBQUUsQ0FBQztNQUMzRyxLQUFLLE1BQU1XLE1BQU0sSUFBSXRGLE1BQU0sQ0FBQzRFLE9BQU8sRUFBRTtRQUNuQyxJQUFJVSxNQUFNLENBQUNFLEdBQUcsS0FBSzlMLFVBQVUsRUFBRTtVQUM3QixJQUFJLENBQUNzTixZQUFZLElBQUkxQixNQUFNLENBQUMyQixTQUFTLENBQUNDLE9BQU8sQ0FBQyxDQUFDLEdBQUdGLFlBQVksQ0FBQ0MsU0FBUyxDQUFDQyxPQUFPLENBQUMsQ0FBQyxFQUFFO1lBQ2xGRixZQUFZLEdBQUcxQixNQUFNO1VBQ3ZCO1FBQ0Y7TUFDRjtNQUNBLElBQUl0RixNQUFNLENBQUMrRixXQUFXLEVBQUU7UUFDdEJyQixTQUFTLEdBQUcxRSxNQUFNLENBQUNnRyxhQUFhO1FBQ2hDckIsY0FBYyxHQUFHM0UsTUFBTSxDQUFDaUcsa0JBQWtCO1FBQzFDO01BQ0Y7TUFFQTtJQUNGO0lBQ0EsUUFBQWMsYUFBQSxHQUFPQyxZQUFZLGNBQUFELGFBQUEsdUJBQVpBLGFBQUEsQ0FBY3RCLFFBQVE7RUFDL0I7O0VBRUE7QUFDRjtBQUNBO0VBQ0UsTUFBTTBCLHVCQUF1QkEsQ0FDM0IxTixVQUFrQixFQUNsQkMsVUFBa0IsRUFDbEIrTCxRQUFnQixFQUNoQjJCLEtBR0csRUFDa0Q7SUFDckQsSUFBSSxDQUFDLElBQUF2SSx5QkFBaUIsRUFBQ3BGLFVBQVUsQ0FBQyxFQUFFO01BQ2xDLE1BQU0sSUFBSWxHLE1BQU0sQ0FBQ3VMLHNCQUFzQixDQUFDLHVCQUF1QixHQUFHckYsVUFBVSxDQUFDO0lBQy9FO0lBQ0EsSUFBSSxDQUFDLElBQUF5SCx5QkFBaUIsRUFBQ3hILFVBQVUsQ0FBQyxFQUFFO01BQ2xDLE1BQU0sSUFBSW5HLE1BQU0sQ0FBQzROLHNCQUFzQixDQUFDLHdCQUF3QnpILFVBQVUsRUFBRSxDQUFDO0lBQy9FO0lBQ0EsSUFBSSxDQUFDLElBQUF0QyxnQkFBUSxFQUFDcU8sUUFBUSxDQUFDLEVBQUU7TUFDdkIsTUFBTSxJQUFJcE0sU0FBUyxDQUFDLHFDQUFxQyxDQUFDO0lBQzVEO0lBQ0EsSUFBSSxDQUFDLElBQUExQixnQkFBUSxFQUFDeVAsS0FBSyxDQUFDLEVBQUU7TUFDcEIsTUFBTSxJQUFJL04sU0FBUyxDQUFDLGlDQUFpQyxDQUFDO0lBQ3hEO0lBRUEsSUFBSSxDQUFDb00sUUFBUSxFQUFFO01BQ2IsTUFBTSxJQUFJbFMsTUFBTSxDQUFDMEQsb0JBQW9CLENBQUMsMEJBQTBCLENBQUM7SUFDbkU7SUFFQSxNQUFNa0QsTUFBTSxHQUFHLE1BQU07SUFDckIsTUFBTUUsS0FBSyxHQUFHLFlBQVksSUFBQThMLGlCQUFTLEVBQUNWLFFBQVEsQ0FBQyxFQUFFO0lBRS9DLE1BQU00QixPQUFPLEdBQUcsSUFBSTNSLGVBQU0sQ0FBQ0MsT0FBTyxDQUFDLENBQUM7SUFDcEMsTUFBTXVILE9BQU8sR0FBR21LLE9BQU8sQ0FBQy9HLFdBQVcsQ0FBQztNQUNsQ2dILHVCQUF1QixFQUFFO1FBQ3ZCOUcsQ0FBQyxFQUFFO1VBQ0RDLEtBQUssRUFBRTtRQUNULENBQUM7UUFDRDhHLElBQUksRUFBRUgsS0FBSyxDQUFDSSxHQUFHLENBQUVqRixJQUFJLElBQUs7VUFDeEIsT0FBTztZQUNMa0YsVUFBVSxFQUFFbEYsSUFBSSxDQUFDbUYsSUFBSTtZQUNyQkMsSUFBSSxFQUFFcEYsSUFBSSxDQUFDQTtVQUNiLENBQUM7UUFDSCxDQUFDO01BQ0g7SUFDRixDQUFDLENBQUM7SUFFRixNQUFNNUUsR0FBRyxHQUFHLE1BQU0sSUFBSSxDQUFDVixnQkFBZ0IsQ0FBQztNQUFFOUMsTUFBTTtNQUFFVixVQUFVO01BQUVDLFVBQVU7TUFBRVc7SUFBTSxDQUFDLEVBQUU2QyxPQUFPLENBQUM7SUFDM0YsTUFBTVcsSUFBSSxHQUFHLE1BQU0sSUFBQTZJLHNCQUFZLEVBQUMvSSxHQUFHLENBQUM7SUFDcEMsTUFBTXFDLE1BQU0sR0FBRyxJQUFBNEgsaUNBQXNCLEVBQUMvSixJQUFJLENBQUN4QyxRQUFRLENBQUMsQ0FBQyxDQUFDO0lBQ3RELElBQUksQ0FBQzJFLE1BQU0sRUFBRTtNQUNYLE1BQU0sSUFBSXRKLEtBQUssQ0FBQyxzQ0FBc0MsQ0FBQztJQUN6RDtJQUVBLElBQUlzSixNQUFNLENBQUNWLE9BQU8sRUFBRTtNQUNsQjtNQUNBLE1BQU0sSUFBSS9MLE1BQU0sQ0FBQzhMLE9BQU8sQ0FBQ1csTUFBTSxDQUFDNkgsVUFBVSxDQUFDO0lBQzdDO0lBRUEsT0FBTztNQUNMO01BQ0E7TUFDQXRGLElBQUksRUFBRXZDLE1BQU0sQ0FBQ3VDLElBQWM7TUFDM0JxQixTQUFTLEVBQUUsSUFBQUMsb0JBQVksRUFBQ2xHLEdBQUcsQ0FBQ3ZELE9BQXlCO0lBQ3ZELENBQUM7RUFDSDs7RUFFQTtBQUNGO0FBQ0E7RUFDRSxNQUFnQm1MLFNBQVNBLENBQUM5TCxVQUFrQixFQUFFQyxVQUFrQixFQUFFK0wsUUFBZ0IsRUFBMkI7SUFDM0csSUFBSSxDQUFDLElBQUE1Ryx5QkFBaUIsRUFBQ3BGLFVBQVUsQ0FBQyxFQUFFO01BQ2xDLE1BQU0sSUFBSWxHLE1BQU0sQ0FBQ3VMLHNCQUFzQixDQUFDLHVCQUF1QixHQUFHckYsVUFBVSxDQUFDO0lBQy9FO0lBQ0EsSUFBSSxDQUFDLElBQUF5SCx5QkFBaUIsRUFBQ3hILFVBQVUsQ0FBQyxFQUFFO01BQ2xDLE1BQU0sSUFBSW5HLE1BQU0sQ0FBQzROLHNCQUFzQixDQUFDLHdCQUF3QnpILFVBQVUsRUFBRSxDQUFDO0lBQy9FO0lBQ0EsSUFBSSxDQUFDLElBQUF0QyxnQkFBUSxFQUFDcU8sUUFBUSxDQUFDLEVBQUU7TUFDdkIsTUFBTSxJQUFJcE0sU0FBUyxDQUFDLHFDQUFxQyxDQUFDO0lBQzVEO0lBQ0EsSUFBSSxDQUFDb00sUUFBUSxFQUFFO01BQ2IsTUFBTSxJQUFJbFMsTUFBTSxDQUFDMEQsb0JBQW9CLENBQUMsMEJBQTBCLENBQUM7SUFDbkU7SUFFQSxNQUFNeU8sS0FBcUIsR0FBRyxFQUFFO0lBQ2hDLElBQUlvQyxNQUFNLEdBQUcsQ0FBQztJQUNkLElBQUk5SCxNQUFNO0lBQ1YsR0FBRztNQUNEQSxNQUFNLEdBQUcsTUFBTSxJQUFJLENBQUMrSCxjQUFjLENBQUN0TyxVQUFVLEVBQUVDLFVBQVUsRUFBRStMLFFBQVEsRUFBRXFDLE1BQU0sQ0FBQztNQUM1RUEsTUFBTSxHQUFHOUgsTUFBTSxDQUFDOEgsTUFBTTtNQUN0QnBDLEtBQUssQ0FBQzVELElBQUksQ0FBQyxHQUFHOUIsTUFBTSxDQUFDMEYsS0FBSyxDQUFDO0lBQzdCLENBQUMsUUFBUTFGLE1BQU0sQ0FBQytGLFdBQVc7SUFFM0IsT0FBT0wsS0FBSztFQUNkOztFQUVBO0FBQ0Y7QUFDQTtFQUNFLE1BQWNxQyxjQUFjQSxDQUFDdE8sVUFBa0IsRUFBRUMsVUFBa0IsRUFBRStMLFFBQWdCLEVBQUVxQyxNQUFjLEVBQUU7SUFDckcsSUFBSSxDQUFDLElBQUFqSix5QkFBaUIsRUFBQ3BGLFVBQVUsQ0FBQyxFQUFFO01BQ2xDLE1BQU0sSUFBSWxHLE1BQU0sQ0FBQ3VMLHNCQUFzQixDQUFDLHVCQUF1QixHQUFHckYsVUFBVSxDQUFDO0lBQy9FO0lBQ0EsSUFBSSxDQUFDLElBQUF5SCx5QkFBaUIsRUFBQ3hILFVBQVUsQ0FBQyxFQUFFO01BQ2xDLE1BQU0sSUFBSW5HLE1BQU0sQ0FBQzROLHNCQUFzQixDQUFDLHdCQUF3QnpILFVBQVUsRUFBRSxDQUFDO0lBQy9FO0lBQ0EsSUFBSSxDQUFDLElBQUF0QyxnQkFBUSxFQUFDcU8sUUFBUSxDQUFDLEVBQUU7TUFDdkIsTUFBTSxJQUFJcE0sU0FBUyxDQUFDLHFDQUFxQyxDQUFDO0lBQzVEO0lBQ0EsSUFBSSxDQUFDLElBQUErRCxnQkFBUSxFQUFDMEssTUFBTSxDQUFDLEVBQUU7TUFDckIsTUFBTSxJQUFJek8sU0FBUyxDQUFDLG1DQUFtQyxDQUFDO0lBQzFEO0lBQ0EsSUFBSSxDQUFDb00sUUFBUSxFQUFFO01BQ2IsTUFBTSxJQUFJbFMsTUFBTSxDQUFDMEQsb0JBQW9CLENBQUMsMEJBQTBCLENBQUM7SUFDbkU7SUFFQSxJQUFJb0QsS0FBSyxHQUFHLFlBQVksSUFBQThMLGlCQUFTLEVBQUNWLFFBQVEsQ0FBQyxFQUFFO0lBQzdDLElBQUlxQyxNQUFNLEVBQUU7TUFDVnpOLEtBQUssSUFBSSx1QkFBdUJ5TixNQUFNLEVBQUU7SUFDMUM7SUFFQSxNQUFNM04sTUFBTSxHQUFHLEtBQUs7SUFDcEIsTUFBTXdELEdBQUcsR0FBRyxNQUFNLElBQUksQ0FBQ1YsZ0JBQWdCLENBQUM7TUFBRTlDLE1BQU07TUFBRVYsVUFBVTtNQUFFQyxVQUFVO01BQUVXO0lBQU0sQ0FBQyxDQUFDO0lBQ2xGLE9BQU9oRyxVQUFVLENBQUMyVCxjQUFjLENBQUMsTUFBTSxJQUFBL0ksc0JBQVksRUFBQ3RCLEdBQUcsQ0FBQyxDQUFDO0VBQzNEO0VBRUEsTUFBTXNLLFdBQVdBLENBQUEsRUFBa0M7SUFDakQsTUFBTTlOLE1BQU0sR0FBRyxLQUFLO0lBQ3BCLE1BQU0rTixVQUFVLEdBQUcsSUFBSSxDQUFDL1EsTUFBTSxJQUFJZ0ksdUJBQWM7SUFDaEQsTUFBTWdKLE9BQU8sR0FBRyxNQUFNLElBQUksQ0FBQ2xMLGdCQUFnQixDQUFDO01BQUU5QztJQUFPLENBQUMsRUFBRSxFQUFFLEVBQUUsQ0FBQyxHQUFHLENBQUMsRUFBRStOLFVBQVUsQ0FBQztJQUM5RSxNQUFNRSxTQUFTLEdBQUcsTUFBTSxJQUFBbkosc0JBQVksRUFBQ2tKLE9BQU8sQ0FBQztJQUM3QyxPQUFPOVQsVUFBVSxDQUFDZ1UsZUFBZSxDQUFDRCxTQUFTLENBQUM7RUFDOUM7O0VBRUE7QUFDRjtBQUNBO0VBQ0VFLGlCQUFpQkEsQ0FBQ3ZGLElBQVksRUFBRTtJQUM5QixJQUFJLENBQUMsSUFBQTNGLGdCQUFRLEVBQUMyRixJQUFJLENBQUMsRUFBRTtNQUNuQixNQUFNLElBQUkxSixTQUFTLENBQUMsaUNBQWlDLENBQUM7SUFDeEQ7SUFDQSxJQUFJMEosSUFBSSxHQUFHLElBQUksQ0FBQzFNLGFBQWEsRUFBRTtNQUM3QixNQUFNLElBQUlnRCxTQUFTLENBQUMsZ0NBQWdDLElBQUksQ0FBQ2hELGFBQWEsRUFBRSxDQUFDO0lBQzNFO0lBQ0EsSUFBSSxJQUFJLENBQUNvQyxnQkFBZ0IsRUFBRTtNQUN6QixPQUFPLElBQUksQ0FBQ3RDLFFBQVE7SUFDdEI7SUFDQSxJQUFJQSxRQUFRLEdBQUcsSUFBSSxDQUFDQSxRQUFRO0lBQzVCLFNBQVM7TUFDUDtNQUNBO01BQ0EsSUFBSUEsUUFBUSxHQUFHLEtBQUssR0FBRzRNLElBQUksRUFBRTtRQUMzQixPQUFPNU0sUUFBUTtNQUNqQjtNQUNBO01BQ0FBLFFBQVEsSUFBSSxFQUFFLEdBQUcsSUFBSSxHQUFHLElBQUk7SUFDOUI7RUFDRjs7RUFFQTtBQUNGO0FBQ0E7RUFDRSxNQUFNb1MsVUFBVUEsQ0FBQzlPLFVBQWtCLEVBQUVDLFVBQWtCLEVBQUVzSSxRQUFnQixFQUFFeUIsUUFBeUIsRUFBRTtJQUNwRyxJQUFJLENBQUMsSUFBQTVFLHlCQUFpQixFQUFDcEYsVUFBVSxDQUFDLEVBQUU7TUFDbEMsTUFBTSxJQUFJbEcsTUFBTSxDQUFDdUwsc0JBQXNCLENBQUMsdUJBQXVCLEdBQUdyRixVQUFVLENBQUM7SUFDL0U7SUFDQSxJQUFJLENBQUMsSUFBQXlILHlCQUFpQixFQUFDeEgsVUFBVSxDQUFDLEVBQUU7TUFDbEMsTUFBTSxJQUFJbkcsTUFBTSxDQUFDNE4sc0JBQXNCLENBQUMsd0JBQXdCekgsVUFBVSxFQUFFLENBQUM7SUFDL0U7SUFFQSxJQUFJLENBQUMsSUFBQXRDLGdCQUFRLEVBQUM0SyxRQUFRLENBQUMsRUFBRTtNQUN2QixNQUFNLElBQUkzSSxTQUFTLENBQUMscUNBQXFDLENBQUM7SUFDNUQ7SUFDQSxJQUFJb0ssUUFBUSxJQUFJLENBQUMsSUFBQTlMLGdCQUFRLEVBQUM4TCxRQUFRLENBQUMsRUFBRTtNQUNuQyxNQUFNLElBQUlwSyxTQUFTLENBQUMscUNBQXFDLENBQUM7SUFDNUQ7O0lBRUE7SUFDQW9LLFFBQVEsR0FBRyxJQUFBK0UseUJBQWlCLEVBQUMvRSxRQUFRLElBQUksQ0FBQyxDQUFDLEVBQUV6QixRQUFRLENBQUM7SUFDdEQsTUFBTWMsSUFBSSxHQUFHLE1BQU1MLFdBQUcsQ0FBQ0ssSUFBSSxDQUFDZCxRQUFRLENBQUM7SUFDckMsT0FBTyxNQUFNLElBQUksQ0FBQ3lHLFNBQVMsQ0FBQ2hQLFVBQVUsRUFBRUMsVUFBVSxFQUFFaEgsRUFBRSxDQUFDZ1csZ0JBQWdCLENBQUMxRyxRQUFRLENBQUMsRUFBRWMsSUFBSSxDQUFDQyxJQUFJLEVBQUVVLFFBQVEsQ0FBQztFQUN6Rzs7RUFFQTtBQUNGO0FBQ0E7QUFDQTtFQUNFLE1BQU1nRixTQUFTQSxDQUNiaFAsVUFBa0IsRUFDbEJDLFVBQWtCLEVBQ2xCNUcsTUFBeUMsRUFDekNpUSxJQUFhLEVBQ2JVLFFBQTZCLEVBQ0E7SUFDN0IsSUFBSSxDQUFDLElBQUE1RSx5QkFBaUIsRUFBQ3BGLFVBQVUsQ0FBQyxFQUFFO01BQ2xDLE1BQU0sSUFBSWxHLE1BQU0sQ0FBQ3VMLHNCQUFzQixDQUFDLHdCQUF3QnJGLFVBQVUsRUFBRSxDQUFDO0lBQy9FO0lBQ0EsSUFBSSxDQUFDLElBQUF5SCx5QkFBaUIsRUFBQ3hILFVBQVUsQ0FBQyxFQUFFO01BQ2xDLE1BQU0sSUFBSW5HLE1BQU0sQ0FBQzROLHNCQUFzQixDQUFDLHdCQUF3QnpILFVBQVUsRUFBRSxDQUFDO0lBQy9FOztJQUVBO0lBQ0E7SUFDQSxJQUFJLElBQUEvQixnQkFBUSxFQUFDb0wsSUFBSSxDQUFDLEVBQUU7TUFDbEJVLFFBQVEsR0FBR1YsSUFBSTtJQUNqQjtJQUNBO0lBQ0EsTUFBTTNJLE9BQU8sR0FBRyxJQUFBd0gsdUJBQWUsRUFBQzZCLFFBQVEsQ0FBQztJQUN6QyxJQUFJLE9BQU8zUSxNQUFNLEtBQUssUUFBUSxJQUFJQSxNQUFNLFlBQVlnTCxNQUFNLEVBQUU7TUFDMUQ7TUFDQWlGLElBQUksR0FBR2pRLE1BQU0sQ0FBQ3VLLE1BQU07TUFDcEJ2SyxNQUFNLEdBQUcsSUFBQTZWLHNCQUFjLEVBQUM3VixNQUFNLENBQUM7SUFDakMsQ0FBQyxNQUFNLElBQUksQ0FBQyxJQUFBcUosd0JBQWdCLEVBQUNySixNQUFNLENBQUMsRUFBRTtNQUNwQyxNQUFNLElBQUl1RyxTQUFTLENBQUMsNEVBQTRFLENBQUM7SUFDbkc7SUFFQSxJQUFJLElBQUErRCxnQkFBUSxFQUFDMkYsSUFBSSxDQUFDLElBQUlBLElBQUksR0FBRyxDQUFDLEVBQUU7TUFDOUIsTUFBTSxJQUFJeFAsTUFBTSxDQUFDMEQsb0JBQW9CLENBQUMsd0NBQXdDOEwsSUFBSSxFQUFFLENBQUM7SUFDdkY7O0lBRUE7SUFDQTtJQUNBLElBQUksQ0FBQyxJQUFBM0YsZ0JBQVEsRUFBQzJGLElBQUksQ0FBQyxFQUFFO01BQ25CQSxJQUFJLEdBQUcsSUFBSSxDQUFDMU0sYUFBYTtJQUMzQjs7SUFFQTtJQUNBO0lBQ0EsSUFBSTBNLElBQUksS0FBS3RNLFNBQVMsRUFBRTtNQUN0QixNQUFNbVMsUUFBUSxHQUFHLE1BQU0sSUFBQUMsd0JBQWdCLEVBQUMvVixNQUFNLENBQUM7TUFDL0MsSUFBSThWLFFBQVEsS0FBSyxJQUFJLEVBQUU7UUFDckI3RixJQUFJLEdBQUc2RixRQUFRO01BQ2pCO0lBQ0Y7SUFFQSxJQUFJLENBQUMsSUFBQXhMLGdCQUFRLEVBQUMyRixJQUFJLENBQUMsRUFBRTtNQUNuQjtNQUNBQSxJQUFJLEdBQUcsSUFBSSxDQUFDMU0sYUFBYTtJQUMzQjtJQUNBLElBQUkwTSxJQUFJLEtBQUssQ0FBQyxFQUFFO01BQ2QsT0FBTyxJQUFJLENBQUMrRixZQUFZLENBQUNyUCxVQUFVLEVBQUVDLFVBQVUsRUFBRVUsT0FBTyxFQUFFMEQsTUFBTSxDQUFDd0UsSUFBSSxDQUFDLEVBQUUsQ0FBQyxDQUFDO0lBQzVFO0lBRUEsTUFBTW5NLFFBQVEsR0FBRyxJQUFJLENBQUNtUyxpQkFBaUIsQ0FBQ3ZGLElBQUksQ0FBQztJQUM3QyxJQUFJLE9BQU9qUSxNQUFNLEtBQUssUUFBUSxJQUFJZ0wsTUFBTSxDQUFDQyxRQUFRLENBQUNqTCxNQUFNLENBQUMsSUFBSWlRLElBQUksSUFBSTVNLFFBQVEsRUFBRTtNQUM3RSxNQUFNNFMsR0FBRyxHQUFHLElBQUE1TSx3QkFBZ0IsRUFBQ3JKLE1BQU0sQ0FBQyxHQUFHLE1BQU0sSUFBQTRULHNCQUFZLEVBQUM1VCxNQUFNLENBQUMsR0FBR2dMLE1BQU0sQ0FBQ3dFLElBQUksQ0FBQ3hQLE1BQU0sQ0FBQztNQUN2RixPQUFPLElBQUksQ0FBQ2dXLFlBQVksQ0FBQ3JQLFVBQVUsRUFBRUMsVUFBVSxFQUFFVSxPQUFPLEVBQUUyTyxHQUFHLENBQUM7SUFDaEU7SUFFQSxPQUFPLElBQUksQ0FBQ0MsWUFBWSxDQUFDdlAsVUFBVSxFQUFFQyxVQUFVLEVBQUVVLE9BQU8sRUFBRXRILE1BQU0sRUFBRXFELFFBQVEsQ0FBQztFQUM3RTs7RUFFQTtBQUNGO0FBQ0E7QUFDQTtFQUNFLE1BQWMyUyxZQUFZQSxDQUN4QnJQLFVBQWtCLEVBQ2xCQyxVQUFrQixFQUNsQlUsT0FBdUIsRUFDdkIyTyxHQUFXLEVBQ2tCO0lBQzdCLE1BQU07TUFBRUUsTUFBTTtNQUFFM0w7SUFBVSxDQUFDLEdBQUcsSUFBQTRMLGtCQUFVLEVBQUNILEdBQUcsRUFBRSxJQUFJLENBQUNyUSxZQUFZLENBQUM7SUFDaEUwQixPQUFPLENBQUMsZ0JBQWdCLENBQUMsR0FBRzJPLEdBQUcsQ0FBQzFMLE1BQU07SUFDdEMsSUFBSSxDQUFDLElBQUksQ0FBQzNFLFlBQVksRUFBRTtNQUN0QjBCLE9BQU8sQ0FBQyxhQUFhLENBQUMsR0FBRzZPLE1BQU07SUFDakM7SUFDQSxNQUFNdEwsR0FBRyxHQUFHLE1BQU0sSUFBSSxDQUFDSCxzQkFBc0IsQ0FDM0M7TUFDRXJELE1BQU0sRUFBRSxLQUFLO01BQ2JWLFVBQVU7TUFDVkMsVUFBVTtNQUNWVTtJQUNGLENBQUMsRUFDRDJPLEdBQUcsRUFDSHpMLFNBQVMsRUFDVCxDQUFDLEdBQUcsQ0FBQyxFQUNMLEVBQ0YsQ0FBQztJQUNELE1BQU0sSUFBQU0sdUJBQWEsRUFBQ0QsR0FBRyxDQUFDO0lBQ3hCLE9BQU87TUFDTDRFLElBQUksRUFBRSxJQUFBdUIsb0JBQVksRUFBQ25HLEdBQUcsQ0FBQ3ZELE9BQU8sQ0FBQ21JLElBQUksQ0FBQztNQUNwQ3FCLFNBQVMsRUFBRSxJQUFBQyxvQkFBWSxFQUFDbEcsR0FBRyxDQUFDdkQsT0FBeUI7SUFDdkQsQ0FBQztFQUNIOztFQUVBO0FBQ0Y7QUFDQTtBQUNBO0VBQ0UsTUFBYzRPLFlBQVlBLENBQ3hCdlAsVUFBa0IsRUFDbEJDLFVBQWtCLEVBQ2xCVSxPQUF1QixFQUN2QnlELElBQXFCLEVBQ3JCMUgsUUFBZ0IsRUFDYTtJQUM3QjtJQUNBO0lBQ0EsTUFBTWdULFFBQThCLEdBQUcsQ0FBQyxDQUFDOztJQUV6QztJQUNBO0lBQ0EsTUFBTUMsS0FBYSxHQUFHLEVBQUU7SUFFeEIsTUFBTUMsZ0JBQWdCLEdBQUcsTUFBTSxJQUFJLENBQUN2QyxZQUFZLENBQUNyTixVQUFVLEVBQUVDLFVBQVUsQ0FBQztJQUN4RSxJQUFJK0wsUUFBZ0I7SUFDcEIsSUFBSSxDQUFDNEQsZ0JBQWdCLEVBQUU7TUFDckI1RCxRQUFRLEdBQUcsTUFBTSxJQUFJLENBQUNnQiwwQkFBMEIsQ0FBQ2hOLFVBQVUsRUFBRUMsVUFBVSxFQUFFVSxPQUFPLENBQUM7SUFDbkYsQ0FBQyxNQUFNO01BQ0xxTCxRQUFRLEdBQUc0RCxnQkFBZ0I7TUFDM0IsTUFBTUMsT0FBTyxHQUFHLE1BQU0sSUFBSSxDQUFDL0QsU0FBUyxDQUFDOUwsVUFBVSxFQUFFQyxVQUFVLEVBQUUyUCxnQkFBZ0IsQ0FBQztNQUM5RUMsT0FBTyxDQUFDak4sT0FBTyxDQUFFL0gsQ0FBQyxJQUFLO1FBQ3JCNlUsUUFBUSxDQUFDN1UsQ0FBQyxDQUFDb1QsSUFBSSxDQUFDLEdBQUdwVCxDQUFDO01BQ3RCLENBQUMsQ0FBQztJQUNKO0lBRUEsSUFBSSxDQUFDLElBQUE2SCx3QkFBZ0IsRUFBQzBCLElBQUksQ0FBQyxFQUFFO01BQzNCLE1BQU0sSUFBSXhFLFNBQVMsQ0FBQyw0RUFBNEUsQ0FBQztJQUNuRztJQUVBLE1BQU1rUSxRQUFRLEdBQUcsSUFBSUMsb0JBQVksQ0FBQztNQUFFekcsSUFBSSxFQUFFNU0sUUFBUTtNQUFFc1QsV0FBVyxFQUFFO0lBQU0sQ0FBQyxDQUFDOztJQUV6RTtJQUNBLE1BQU0sQ0FBQ25RLENBQUMsRUFBRXpFLENBQUMsQ0FBQyxHQUFHLE1BQU02VSxPQUFPLENBQUNDLEdBQUcsQ0FBQyxDQUMvQixJQUFJRCxPQUFPLENBQUMsQ0FBQ0UsT0FBTyxFQUFFQyxNQUFNLEtBQUs7TUFDL0JoTSxJQUFJLENBQUNpTSxJQUFJLENBQUNQLFFBQVEsQ0FBQyxDQUFDUSxFQUFFLENBQUMsT0FBTyxFQUFFRixNQUFNLENBQUM7TUFDdkNOLFFBQVEsQ0FBQ1EsRUFBRSxDQUFDLEtBQUssRUFBRUgsT0FBTyxDQUFDLENBQUNHLEVBQUUsQ0FBQyxPQUFPLEVBQUVGLE1BQU0sQ0FBQztJQUNqRCxDQUFDLENBQUMsRUFDRixDQUFDLFlBQVk7TUFDWCxJQUFJRyxVQUFVLEdBQUcsQ0FBQztNQUVsQixXQUFXLE1BQU1DLEtBQUssSUFBSVYsUUFBUSxFQUFFO1FBQ2xDLE1BQU1XLEdBQUcsR0FBRzNYLE1BQU0sQ0FBQzRYLFVBQVUsQ0FBQyxLQUFLLENBQUMsQ0FBQ0MsTUFBTSxDQUFDSCxLQUFLLENBQUMsQ0FBQ0ksTUFBTSxDQUFDLENBQUM7UUFFM0RMLFVBQVUsRUFBRTtRQUNaLE1BQU1NLE9BQU8sR0FBR25CLFFBQVEsQ0FBQ2EsVUFBVSxDQUFDO1FBQ3BDLElBQUlNLE9BQU8sRUFBRTtVQUNYLElBQUlBLE9BQU8sQ0FBQy9ILElBQUksS0FBSzJILEdBQUcsQ0FBQzdPLFFBQVEsQ0FBQyxLQUFLLENBQUMsRUFBRTtZQUN4QytOLEtBQUssQ0FBQ3RILElBQUksQ0FBQztjQUFFNEYsSUFBSSxFQUFFc0MsVUFBVTtjQUFFekgsSUFBSSxFQUFFK0gsT0FBTyxDQUFDL0g7WUFBSyxDQUFDLENBQUM7WUFDcEQ7VUFDRjtRQUNGOztRQUVBO1FBQ0EsTUFBTW5KLE9BQXNCLEdBQUc7VUFDN0JlLE1BQU0sRUFBRSxLQUFLO1VBQ2JFLEtBQUssRUFBRXNILG9CQUFFLENBQUM5RSxTQUFTLENBQUM7WUFBRW1OLFVBQVU7WUFBRXZFO1VBQVMsQ0FBQyxDQUFDO1VBQzdDckwsT0FBTyxFQUFFO1lBQ1AsZ0JBQWdCLEVBQUU2UCxLQUFLLENBQUM1TSxNQUFNO1lBQzlCLGFBQWEsRUFBRTZNLEdBQUcsQ0FBQzdPLFFBQVEsQ0FBQyxRQUFRO1VBQ3RDLENBQUM7VUFDRDVCLFVBQVU7VUFDVkM7UUFDRixDQUFDO1FBRUQsTUFBTXNDLFFBQVEsR0FBRyxNQUFNLElBQUksQ0FBQ3lCLG9CQUFvQixDQUFDckUsT0FBTyxFQUFFNlEsS0FBSyxDQUFDO1FBRWhFLElBQUkxSCxJQUFJLEdBQUd2RyxRQUFRLENBQUM1QixPQUFPLENBQUNtSSxJQUFJO1FBQ2hDLElBQUlBLElBQUksRUFBRTtVQUNSQSxJQUFJLEdBQUdBLElBQUksQ0FBQy9GLE9BQU8sQ0FBQyxJQUFJLEVBQUUsRUFBRSxDQUFDLENBQUNBLE9BQU8sQ0FBQyxJQUFJLEVBQUUsRUFBRSxDQUFDO1FBQ2pELENBQUMsTUFBTTtVQUNMK0YsSUFBSSxHQUFHLEVBQUU7UUFDWDtRQUVBNkcsS0FBSyxDQUFDdEgsSUFBSSxDQUFDO1VBQUU0RixJQUFJLEVBQUVzQyxVQUFVO1VBQUV6SDtRQUFLLENBQUMsQ0FBQztRQUV0Q3lILFVBQVUsRUFBRTtNQUNkO01BRUEsSUFBSVosS0FBSyxDQUFDL0wsTUFBTSxLQUFLLENBQUMsRUFBRTtRQUN0QixNQUFNLElBQUksQ0FBQ3VKLG9CQUFvQixDQUFDbk4sVUFBVSxFQUFFQyxVQUFVLEVBQUUrTCxRQUFRLENBQUM7UUFDakUsT0FBTyxNQUFNLElBQUksQ0FBQ3FELFlBQVksQ0FBQ3JQLFVBQVUsRUFBRUMsVUFBVSxFQUFFVSxPQUFPLEVBQUUwRCxNQUFNLENBQUN3RSxJQUFJLENBQUMsRUFBRSxDQUFDLENBQUM7TUFDbEY7TUFFQSxPQUFPLE1BQU0sSUFBSSxDQUFDNkUsdUJBQXVCLENBQUMxTixVQUFVLEVBQUVDLFVBQVUsRUFBRStMLFFBQVEsRUFBRTJELEtBQUssQ0FBQztJQUNwRixDQUFDLEVBQUUsQ0FBQyxDQUNMLENBQUM7SUFFRixPQUFPdlUsQ0FBQztFQUNWO0VBSUEsTUFBTTBWLHVCQUF1QkEsQ0FBQzlRLFVBQWtCLEVBQWlCO0lBQy9ELElBQUksQ0FBQyxJQUFBb0YseUJBQWlCLEVBQUNwRixVQUFVLENBQUMsRUFBRTtNQUNsQyxNQUFNLElBQUlsRyxNQUFNLENBQUN1TCxzQkFBc0IsQ0FBQyx1QkFBdUIsR0FBR3JGLFVBQVUsQ0FBQztJQUMvRTtJQUNBLE1BQU1VLE1BQU0sR0FBRyxRQUFRO0lBQ3ZCLE1BQU1FLEtBQUssR0FBRyxhQUFhO0lBQzNCLE1BQU0sSUFBSSxDQUFDb0Qsb0JBQW9CLENBQUM7TUFBRXRELE1BQU07TUFBRVYsVUFBVTtNQUFFWTtJQUFNLENBQUMsRUFBRSxFQUFFLEVBQUUsQ0FBQyxHQUFHLEVBQUUsR0FBRyxDQUFDLEVBQUUsRUFBRSxDQUFDO0VBQ3BGO0VBSUEsTUFBTW1RLG9CQUFvQkEsQ0FBQy9RLFVBQWtCLEVBQUVnUixpQkFBd0MsRUFBRTtJQUN2RixJQUFJLENBQUMsSUFBQTVMLHlCQUFpQixFQUFDcEYsVUFBVSxDQUFDLEVBQUU7TUFDbEMsTUFBTSxJQUFJbEcsTUFBTSxDQUFDdUwsc0JBQXNCLENBQUMsdUJBQXVCLEdBQUdyRixVQUFVLENBQUM7SUFDL0U7SUFDQSxJQUFJLENBQUMsSUFBQTlCLGdCQUFRLEVBQUM4UyxpQkFBaUIsQ0FBQyxFQUFFO01BQ2hDLE1BQU0sSUFBSWxYLE1BQU0sQ0FBQzBELG9CQUFvQixDQUFDLDhDQUE4QyxDQUFDO0lBQ3ZGLENBQUMsTUFBTTtNQUNMLElBQUlxQyxlQUFDLENBQUNLLE9BQU8sQ0FBQzhRLGlCQUFpQixDQUFDQyxJQUFJLENBQUMsRUFBRTtRQUNyQyxNQUFNLElBQUluWCxNQUFNLENBQUMwRCxvQkFBb0IsQ0FBQyxzQkFBc0IsQ0FBQztNQUMvRCxDQUFDLE1BQU0sSUFBSXdULGlCQUFpQixDQUFDQyxJQUFJLElBQUksQ0FBQyxJQUFBdFQsZ0JBQVEsRUFBQ3FULGlCQUFpQixDQUFDQyxJQUFJLENBQUMsRUFBRTtRQUN0RSxNQUFNLElBQUluWCxNQUFNLENBQUMwRCxvQkFBb0IsQ0FBQyx3QkFBd0IsRUFBRXdULGlCQUFpQixDQUFDQyxJQUFJLENBQUM7TUFDekY7TUFDQSxJQUFJcFIsZUFBQyxDQUFDSyxPQUFPLENBQUM4USxpQkFBaUIsQ0FBQ0UsS0FBSyxDQUFDLEVBQUU7UUFDdEMsTUFBTSxJQUFJcFgsTUFBTSxDQUFDMEQsb0JBQW9CLENBQUMsZ0RBQWdELENBQUM7TUFDekY7SUFDRjtJQUNBLE1BQU1rRCxNQUFNLEdBQUcsS0FBSztJQUNwQixNQUFNRSxLQUFLLEdBQUcsYUFBYTtJQUMzQixNQUFNRCxPQUErQixHQUFHLENBQUMsQ0FBQztJQUUxQyxNQUFNd1EsdUJBQXVCLEdBQUc7TUFDOUJDLHdCQUF3QixFQUFFO1FBQ3hCQyxJQUFJLEVBQUVMLGlCQUFpQixDQUFDQyxJQUFJO1FBQzVCSyxJQUFJLEVBQUVOLGlCQUFpQixDQUFDRTtNQUMxQjtJQUNGLENBQUM7SUFFRCxNQUFNdEQsT0FBTyxHQUFHLElBQUkzUixlQUFNLENBQUNDLE9BQU8sQ0FBQztNQUFFQyxVQUFVLEVBQUU7UUFBRUMsTUFBTSxFQUFFO01BQU0sQ0FBQztNQUFFQyxRQUFRLEVBQUU7SUFBSyxDQUFDLENBQUM7SUFDckYsTUFBTW9ILE9BQU8sR0FBR21LLE9BQU8sQ0FBQy9HLFdBQVcsQ0FBQ3NLLHVCQUF1QixDQUFDO0lBQzVEeFEsT0FBTyxDQUFDLGFBQWEsQ0FBQyxHQUFHLElBQUE0USxhQUFLLEVBQUM5TixPQUFPLENBQUM7SUFDdkMsTUFBTSxJQUFJLENBQUNPLG9CQUFvQixDQUFDO01BQUV0RCxNQUFNO01BQUVWLFVBQVU7TUFBRVksS0FBSztNQUFFRDtJQUFRLENBQUMsRUFBRThDLE9BQU8sQ0FBQztFQUNsRjtFQUlBLE1BQU0rTixvQkFBb0JBLENBQUN4UixVQUFrQixFQUFFO0lBQzdDLElBQUksQ0FBQyxJQUFBb0YseUJBQWlCLEVBQUNwRixVQUFVLENBQUMsRUFBRTtNQUNsQyxNQUFNLElBQUlsRyxNQUFNLENBQUN1TCxzQkFBc0IsQ0FBQyx1QkFBdUIsR0FBR3JGLFVBQVUsQ0FBQztJQUMvRTtJQUNBLE1BQU1VLE1BQU0sR0FBRyxLQUFLO0lBQ3BCLE1BQU1FLEtBQUssR0FBRyxhQUFhO0lBRTNCLE1BQU04TixPQUFPLEdBQUcsTUFBTSxJQUFJLENBQUNsTCxnQkFBZ0IsQ0FBQztNQUFFOUMsTUFBTTtNQUFFVixVQUFVO01BQUVZO0lBQU0sQ0FBQyxFQUFFLEVBQUUsRUFBRSxDQUFDLEdBQUcsRUFBRSxHQUFHLENBQUMsQ0FBQztJQUMxRixNQUFNK04sU0FBUyxHQUFHLE1BQU0sSUFBQW5KLHNCQUFZLEVBQUNrSixPQUFPLENBQUM7SUFDN0MsT0FBTzlULFVBQVUsQ0FBQzZXLHNCQUFzQixDQUFDOUMsU0FBUyxDQUFDO0VBQ3JEO0VBUUEsTUFBTStDLGtCQUFrQkEsQ0FDdEIxUixVQUFrQixFQUNsQkMsVUFBa0IsRUFDbEJ1SCxPQUFtQyxFQUNQO0lBQzVCLElBQUksQ0FBQyxJQUFBcEMseUJBQWlCLEVBQUNwRixVQUFVLENBQUMsRUFBRTtNQUNsQyxNQUFNLElBQUlsRyxNQUFNLENBQUN1TCxzQkFBc0IsQ0FBQyx1QkFBdUIsR0FBR3JGLFVBQVUsQ0FBQztJQUMvRTtJQUNBLElBQUksQ0FBQyxJQUFBeUgseUJBQWlCLEVBQUN4SCxVQUFVLENBQUMsRUFBRTtNQUNsQyxNQUFNLElBQUluRyxNQUFNLENBQUM0TixzQkFBc0IsQ0FBQyx3QkFBd0J6SCxVQUFVLEVBQUUsQ0FBQztJQUMvRTtJQUVBLElBQUl1SCxPQUFPLEVBQUU7TUFDWCxJQUFJLENBQUMsSUFBQXRKLGdCQUFRLEVBQUNzSixPQUFPLENBQUMsRUFBRTtRQUN0QixNQUFNLElBQUk1SCxTQUFTLENBQUMsb0NBQW9DLENBQUM7TUFDM0QsQ0FBQyxNQUFNLElBQUkvRCxNQUFNLENBQUM4VixJQUFJLENBQUNuSyxPQUFPLENBQUMsQ0FBQzVELE1BQU0sR0FBRyxDQUFDLElBQUk0RCxPQUFPLENBQUMyQyxTQUFTLElBQUksQ0FBQyxJQUFBeE0sZ0JBQVEsRUFBQzZKLE9BQU8sQ0FBQzJDLFNBQVMsQ0FBQyxFQUFFO1FBQy9GLE1BQU0sSUFBSXZLLFNBQVMsQ0FBQyxzQ0FBc0MsRUFBRTRILE9BQU8sQ0FBQzJDLFNBQVMsQ0FBQztNQUNoRjtJQUNGO0lBRUEsTUFBTXpKLE1BQU0sR0FBRyxLQUFLO0lBQ3BCLElBQUlFLEtBQUssR0FBRyxZQUFZO0lBRXhCLElBQUk0RyxPQUFPLGFBQVBBLE9BQU8sZUFBUEEsT0FBTyxDQUFFMkMsU0FBUyxFQUFFO01BQ3RCdkosS0FBSyxJQUFJLGNBQWM0RyxPQUFPLENBQUMyQyxTQUFTLEVBQUU7SUFDNUM7SUFFQSxNQUFNdUUsT0FBTyxHQUFHLE1BQU0sSUFBSSxDQUFDbEwsZ0JBQWdCLENBQUM7TUFBRTlDLE1BQU07TUFBRVYsVUFBVTtNQUFFQyxVQUFVO01BQUVXO0lBQU0sQ0FBQyxFQUFFLEVBQUUsRUFBRSxDQUFDLEdBQUcsQ0FBQyxDQUFDO0lBQ2pHLE1BQU1nUixNQUFNLEdBQUcsTUFBTSxJQUFBcE0sc0JBQVksRUFBQ2tKLE9BQU8sQ0FBQztJQUMxQyxPQUFPLElBQUFtRCxxQ0FBMEIsRUFBQ0QsTUFBTSxDQUFDO0VBQzNDO0VBR0EsTUFBTUUsa0JBQWtCQSxDQUN0QjlSLFVBQWtCLEVBQ2xCQyxVQUFrQixFQUNsQjhSLE9BQU8sR0FBRztJQUNSQyxNQUFNLEVBQUVDLDBCQUFpQixDQUFDQztFQUM1QixDQUE4QixFQUNmO0lBQ2YsSUFBSSxDQUFDLElBQUE5TSx5QkFBaUIsRUFBQ3BGLFVBQVUsQ0FBQyxFQUFFO01BQ2xDLE1BQU0sSUFBSWxHLE1BQU0sQ0FBQ3VMLHNCQUFzQixDQUFDLHVCQUF1QixHQUFHckYsVUFBVSxDQUFDO0lBQy9FO0lBQ0EsSUFBSSxDQUFDLElBQUF5SCx5QkFBaUIsRUFBQ3hILFVBQVUsQ0FBQyxFQUFFO01BQ2xDLE1BQU0sSUFBSW5HLE1BQU0sQ0FBQzROLHNCQUFzQixDQUFDLHdCQUF3QnpILFVBQVUsRUFBRSxDQUFDO0lBQy9FO0lBRUEsSUFBSSxDQUFDLElBQUEvQixnQkFBUSxFQUFDNlQsT0FBTyxDQUFDLEVBQUU7TUFDdEIsTUFBTSxJQUFJblMsU0FBUyxDQUFDLG9DQUFvQyxDQUFDO0lBQzNELENBQUMsTUFBTTtNQUNMLElBQUksQ0FBQyxDQUFDcVMsMEJBQWlCLENBQUNDLE9BQU8sRUFBRUQsMEJBQWlCLENBQUNFLFFBQVEsQ0FBQyxDQUFDaFMsUUFBUSxDQUFDNFIsT0FBTyxhQUFQQSxPQUFPLHVCQUFQQSxPQUFPLENBQUVDLE1BQU0sQ0FBQyxFQUFFO1FBQ3RGLE1BQU0sSUFBSXBTLFNBQVMsQ0FBQyxrQkFBa0IsR0FBR21TLE9BQU8sQ0FBQ0MsTUFBTSxDQUFDO01BQzFEO01BQ0EsSUFBSUQsT0FBTyxDQUFDNUgsU0FBUyxJQUFJLENBQUM0SCxPQUFPLENBQUM1SCxTQUFTLENBQUN2RyxNQUFNLEVBQUU7UUFDbEQsTUFBTSxJQUFJaEUsU0FBUyxDQUFDLHNDQUFzQyxHQUFHbVMsT0FBTyxDQUFDNUgsU0FBUyxDQUFDO01BQ2pGO0lBQ0Y7SUFFQSxNQUFNekosTUFBTSxHQUFHLEtBQUs7SUFDcEIsSUFBSUUsS0FBSyxHQUFHLFlBQVk7SUFFeEIsSUFBSW1SLE9BQU8sQ0FBQzVILFNBQVMsRUFBRTtNQUNyQnZKLEtBQUssSUFBSSxjQUFjbVIsT0FBTyxDQUFDNUgsU0FBUyxFQUFFO0lBQzVDO0lBRUEsTUFBTWlJLE1BQU0sR0FBRztNQUNiQyxNQUFNLEVBQUVOLE9BQU8sQ0FBQ0M7SUFDbEIsQ0FBQztJQUVELE1BQU1wRSxPQUFPLEdBQUcsSUFBSTNSLGVBQU0sQ0FBQ0MsT0FBTyxDQUFDO01BQUVvVyxRQUFRLEVBQUUsV0FBVztNQUFFblcsVUFBVSxFQUFFO1FBQUVDLE1BQU0sRUFBRTtNQUFNLENBQUM7TUFBRUMsUUFBUSxFQUFFO0lBQUssQ0FBQyxDQUFDO0lBQzVHLE1BQU1vSCxPQUFPLEdBQUdtSyxPQUFPLENBQUMvRyxXQUFXLENBQUN1TCxNQUFNLENBQUM7SUFDM0MsTUFBTXpSLE9BQStCLEdBQUcsQ0FBQyxDQUFDO0lBQzFDQSxPQUFPLENBQUMsYUFBYSxDQUFDLEdBQUcsSUFBQTRRLGFBQUssRUFBQzlOLE9BQU8sQ0FBQztJQUV2QyxNQUFNLElBQUksQ0FBQ08sb0JBQW9CLENBQUM7TUFBRXRELE1BQU07TUFBRVYsVUFBVTtNQUFFQyxVQUFVO01BQUVXLEtBQUs7TUFBRUQ7SUFBUSxDQUFDLEVBQUU4QyxPQUFPLENBQUM7RUFDOUY7O0VBRUE7QUFDRjtBQUNBO0VBQ0UsTUFBTThPLGdCQUFnQkEsQ0FBQ3ZTLFVBQWtCLEVBQWtCO0lBQ3pELElBQUksQ0FBQyxJQUFBb0YseUJBQWlCLEVBQUNwRixVQUFVLENBQUMsRUFBRTtNQUNsQyxNQUFNLElBQUlsRyxNQUFNLENBQUN1TCxzQkFBc0IsQ0FBQyx3QkFBd0JyRixVQUFVLEVBQUUsQ0FBQztJQUMvRTtJQUVBLE1BQU1VLE1BQU0sR0FBRyxLQUFLO0lBQ3BCLE1BQU1FLEtBQUssR0FBRyxTQUFTO0lBQ3ZCLE1BQU13TSxjQUFjLEdBQUc7TUFBRTFNLE1BQU07TUFBRVYsVUFBVTtNQUFFWTtJQUFNLENBQUM7SUFFcEQsTUFBTTJCLFFBQVEsR0FBRyxNQUFNLElBQUksQ0FBQ2lCLGdCQUFnQixDQUFDNEosY0FBYyxDQUFDO0lBQzVELE1BQU1oSixJQUFJLEdBQUcsTUFBTSxJQUFBb0Isc0JBQVksRUFBQ2pELFFBQVEsQ0FBQztJQUN6QyxPQUFPM0gsVUFBVSxDQUFDNFgsWUFBWSxDQUFDcE8sSUFBSSxDQUFDO0VBQ3RDOztFQUVBO0FBQ0Y7QUFDQTtFQUNFLE1BQU1xTyxnQkFBZ0JBLENBQUN6UyxVQUFrQixFQUFFQyxVQUFrQixFQUFFdUgsT0FBdUIsRUFBa0I7SUFDdEcsTUFBTTlHLE1BQU0sR0FBRyxLQUFLO0lBQ3BCLElBQUlFLEtBQUssR0FBRyxTQUFTO0lBRXJCLElBQUksQ0FBQyxJQUFBd0UseUJBQWlCLEVBQUNwRixVQUFVLENBQUMsRUFBRTtNQUNsQyxNQUFNLElBQUlsRyxNQUFNLENBQUN1TCxzQkFBc0IsQ0FBQyx1QkFBdUIsR0FBR3JGLFVBQVUsQ0FBQztJQUMvRTtJQUNBLElBQUksQ0FBQyxJQUFBeUgseUJBQWlCLEVBQUN4SCxVQUFVLENBQUMsRUFBRTtNQUNsQyxNQUFNLElBQUluRyxNQUFNLENBQUN1TCxzQkFBc0IsQ0FBQyx1QkFBdUIsR0FBR3BGLFVBQVUsQ0FBQztJQUMvRTtJQUNBLElBQUl1SCxPQUFPLElBQUksQ0FBQyxJQUFBdEosZ0JBQVEsRUFBQ3NKLE9BQU8sQ0FBQyxFQUFFO01BQ2pDLE1BQU0sSUFBSTFOLE1BQU0sQ0FBQzBELG9CQUFvQixDQUFDLG9DQUFvQyxDQUFDO0lBQzdFO0lBRUEsSUFBSWdLLE9BQU8sSUFBSUEsT0FBTyxDQUFDMkMsU0FBUyxFQUFFO01BQ2hDdkosS0FBSyxHQUFHLEdBQUdBLEtBQUssY0FBYzRHLE9BQU8sQ0FBQzJDLFNBQVMsRUFBRTtJQUNuRDtJQUNBLE1BQU1pRCxjQUE2QixHQUFHO01BQUUxTSxNQUFNO01BQUVWLFVBQVU7TUFBRVk7SUFBTSxDQUFDO0lBQ25FLElBQUlYLFVBQVUsRUFBRTtNQUNkbU4sY0FBYyxDQUFDLFlBQVksQ0FBQyxHQUFHbk4sVUFBVTtJQUMzQztJQUVBLE1BQU1zQyxRQUFRLEdBQUcsTUFBTSxJQUFJLENBQUNpQixnQkFBZ0IsQ0FBQzRKLGNBQWMsQ0FBQztJQUM1RCxNQUFNaEosSUFBSSxHQUFHLE1BQU0sSUFBQW9CLHNCQUFZLEVBQUNqRCxRQUFRLENBQUM7SUFDekMsT0FBTzNILFVBQVUsQ0FBQzRYLFlBQVksQ0FBQ3BPLElBQUksQ0FBQztFQUN0Qzs7RUFFQTtBQUNGO0FBQ0E7RUFDRSxNQUFNc08sZUFBZUEsQ0FBQzFTLFVBQWtCLEVBQUUyUyxNQUFjLEVBQWlCO0lBQ3ZFO0lBQ0EsSUFBSSxDQUFDLElBQUF2Tix5QkFBaUIsRUFBQ3BGLFVBQVUsQ0FBQyxFQUFFO01BQ2xDLE1BQU0sSUFBSWxHLE1BQU0sQ0FBQ3VMLHNCQUFzQixDQUFDLHdCQUF3QnJGLFVBQVUsRUFBRSxDQUFDO0lBQy9FO0lBQ0EsSUFBSSxDQUFDLElBQUFyQyxnQkFBUSxFQUFDZ1YsTUFBTSxDQUFDLEVBQUU7TUFDckIsTUFBTSxJQUFJN1ksTUFBTSxDQUFDOFksd0JBQXdCLENBQUMsMEJBQTBCRCxNQUFNLHFCQUFxQixDQUFDO0lBQ2xHO0lBRUEsTUFBTS9SLEtBQUssR0FBRyxRQUFRO0lBRXRCLElBQUlGLE1BQU0sR0FBRyxRQUFRO0lBQ3JCLElBQUlpUyxNQUFNLEVBQUU7TUFDVmpTLE1BQU0sR0FBRyxLQUFLO0lBQ2hCO0lBRUEsTUFBTSxJQUFJLENBQUNzRCxvQkFBb0IsQ0FBQztNQUFFdEQsTUFBTTtNQUFFVixVQUFVO01BQUVZO0lBQU0sQ0FBQyxFQUFFK1IsTUFBTSxFQUFFLENBQUMsR0FBRyxDQUFDLEVBQUUsRUFBRSxDQUFDO0VBQ25GOztFQUVBO0FBQ0Y7QUFDQTtFQUNFLE1BQU1FLGVBQWVBLENBQUM3UyxVQUFrQixFQUFtQjtJQUN6RDtJQUNBLElBQUksQ0FBQyxJQUFBb0YseUJBQWlCLEVBQUNwRixVQUFVLENBQUMsRUFBRTtNQUNsQyxNQUFNLElBQUlsRyxNQUFNLENBQUN1TCxzQkFBc0IsQ0FBQyx3QkFBd0JyRixVQUFVLEVBQUUsQ0FBQztJQUMvRTtJQUVBLE1BQU1VLE1BQU0sR0FBRyxLQUFLO0lBQ3BCLE1BQU1FLEtBQUssR0FBRyxRQUFRO0lBQ3RCLE1BQU1zRCxHQUFHLEdBQUcsTUFBTSxJQUFJLENBQUNWLGdCQUFnQixDQUFDO01BQUU5QyxNQUFNO01BQUVWLFVBQVU7TUFBRVk7SUFBTSxDQUFDLENBQUM7SUFDdEUsT0FBTyxNQUFNLElBQUE0RSxzQkFBWSxFQUFDdEIsR0FBRyxDQUFDO0VBQ2hDO0VBRUEsTUFBTTRPLGtCQUFrQkEsQ0FBQzlTLFVBQWtCLEVBQUVDLFVBQWtCLEVBQUU4UyxhQUF3QixHQUFHLENBQUMsQ0FBQyxFQUFpQjtJQUM3RyxJQUFJLENBQUMsSUFBQTNOLHlCQUFpQixFQUFDcEYsVUFBVSxDQUFDLEVBQUU7TUFDbEMsTUFBTSxJQUFJbEcsTUFBTSxDQUFDdUwsc0JBQXNCLENBQUMsd0JBQXdCckYsVUFBVSxFQUFFLENBQUM7SUFDL0U7SUFDQSxJQUFJLENBQUMsSUFBQXlILHlCQUFpQixFQUFDeEgsVUFBVSxDQUFDLEVBQUU7TUFDbEMsTUFBTSxJQUFJbkcsTUFBTSxDQUFDNE4sc0JBQXNCLENBQUMsd0JBQXdCekgsVUFBVSxFQUFFLENBQUM7SUFDL0U7SUFDQSxJQUFJLENBQUMsSUFBQS9CLGdCQUFRLEVBQUM2VSxhQUFhLENBQUMsRUFBRTtNQUM1QixNQUFNLElBQUlqWixNQUFNLENBQUMwRCxvQkFBb0IsQ0FBQywwQ0FBMEMsQ0FBQztJQUNuRixDQUFDLE1BQU07TUFDTCxJQUFJdVYsYUFBYSxDQUFDdkksZ0JBQWdCLElBQUksQ0FBQyxJQUFBL00saUJBQVMsRUFBQ3NWLGFBQWEsQ0FBQ3ZJLGdCQUFnQixDQUFDLEVBQUU7UUFDaEYsTUFBTSxJQUFJMVEsTUFBTSxDQUFDMEQsb0JBQW9CLENBQUMsdUNBQXVDdVYsYUFBYSxDQUFDdkksZ0JBQWdCLEVBQUUsQ0FBQztNQUNoSDtNQUNBLElBQ0V1SSxhQUFhLENBQUNDLElBQUksSUFDbEIsQ0FBQyxDQUFDQyx3QkFBZSxDQUFDQyxVQUFVLEVBQUVELHdCQUFlLENBQUNFLFVBQVUsQ0FBQyxDQUFDaFQsUUFBUSxDQUFDNFMsYUFBYSxDQUFDQyxJQUFJLENBQUMsRUFDdEY7UUFDQSxNQUFNLElBQUlsWixNQUFNLENBQUMwRCxvQkFBb0IsQ0FBQyxrQ0FBa0N1VixhQUFhLENBQUNDLElBQUksRUFBRSxDQUFDO01BQy9GO01BQ0EsSUFBSUQsYUFBYSxDQUFDSyxlQUFlLElBQUksQ0FBQyxJQUFBelYsZ0JBQVEsRUFBQ29WLGFBQWEsQ0FBQ0ssZUFBZSxDQUFDLEVBQUU7UUFDN0UsTUFBTSxJQUFJdFosTUFBTSxDQUFDMEQsb0JBQW9CLENBQUMsc0NBQXNDdVYsYUFBYSxDQUFDSyxlQUFlLEVBQUUsQ0FBQztNQUM5RztNQUNBLElBQUlMLGFBQWEsQ0FBQzVJLFNBQVMsSUFBSSxDQUFDLElBQUF4TSxnQkFBUSxFQUFDb1YsYUFBYSxDQUFDNUksU0FBUyxDQUFDLEVBQUU7UUFDakUsTUFBTSxJQUFJclEsTUFBTSxDQUFDMEQsb0JBQW9CLENBQUMsZ0NBQWdDdVYsYUFBYSxDQUFDNUksU0FBUyxFQUFFLENBQUM7TUFDbEc7SUFDRjtJQUVBLE1BQU16SixNQUFNLEdBQUcsS0FBSztJQUNwQixJQUFJRSxLQUFLLEdBQUcsV0FBVztJQUV2QixNQUFNRCxPQUF1QixHQUFHLENBQUMsQ0FBQztJQUNsQyxJQUFJb1MsYUFBYSxDQUFDdkksZ0JBQWdCLEVBQUU7TUFDbEM3SixPQUFPLENBQUMsbUNBQW1DLENBQUMsR0FBRyxJQUFJO0lBQ3JEO0lBRUEsTUFBTWlOLE9BQU8sR0FBRyxJQUFJM1IsZUFBTSxDQUFDQyxPQUFPLENBQUM7TUFBRW9XLFFBQVEsRUFBRSxXQUFXO01BQUVuVyxVQUFVLEVBQUU7UUFBRUMsTUFBTSxFQUFFO01BQU0sQ0FBQztNQUFFQyxRQUFRLEVBQUU7SUFBSyxDQUFDLENBQUM7SUFDNUcsTUFBTVMsTUFBOEIsR0FBRyxDQUFDLENBQUM7SUFFekMsSUFBSWlXLGFBQWEsQ0FBQ0MsSUFBSSxFQUFFO01BQ3RCbFcsTUFBTSxDQUFDdVcsSUFBSSxHQUFHTixhQUFhLENBQUNDLElBQUk7SUFDbEM7SUFDQSxJQUFJRCxhQUFhLENBQUNLLGVBQWUsRUFBRTtNQUNqQ3RXLE1BQU0sQ0FBQ3dXLGVBQWUsR0FBR1AsYUFBYSxDQUFDSyxlQUFlO0lBQ3hEO0lBQ0EsSUFBSUwsYUFBYSxDQUFDNUksU0FBUyxFQUFFO01BQzNCdkosS0FBSyxJQUFJLGNBQWNtUyxhQUFhLENBQUM1SSxTQUFTLEVBQUU7SUFDbEQ7SUFFQSxNQUFNMUcsT0FBTyxHQUFHbUssT0FBTyxDQUFDL0csV0FBVyxDQUFDL0osTUFBTSxDQUFDO0lBRTNDNkQsT0FBTyxDQUFDLGFBQWEsQ0FBQyxHQUFHLElBQUE0USxhQUFLLEVBQUM5TixPQUFPLENBQUM7SUFDdkMsTUFBTSxJQUFJLENBQUNPLG9CQUFvQixDQUFDO01BQUV0RCxNQUFNO01BQUVWLFVBQVU7TUFBRUMsVUFBVTtNQUFFVyxLQUFLO01BQUVEO0lBQVEsQ0FBQyxFQUFFOEMsT0FBTyxFQUFFLENBQUMsR0FBRyxFQUFFLEdBQUcsQ0FBQyxDQUFDO0VBQzFHO0VBS0EsTUFBTThQLG1CQUFtQkEsQ0FBQ3ZULFVBQWtCLEVBQUU7SUFDNUMsSUFBSSxDQUFDLElBQUFvRix5QkFBaUIsRUFBQ3BGLFVBQVUsQ0FBQyxFQUFFO01BQ2xDLE1BQU0sSUFBSWxHLE1BQU0sQ0FBQ3VMLHNCQUFzQixDQUFDLHVCQUF1QixHQUFHckYsVUFBVSxDQUFDO0lBQy9FO0lBQ0EsTUFBTVUsTUFBTSxHQUFHLEtBQUs7SUFDcEIsTUFBTUUsS0FBSyxHQUFHLGFBQWE7SUFFM0IsTUFBTThOLE9BQU8sR0FBRyxNQUFNLElBQUksQ0FBQ2xMLGdCQUFnQixDQUFDO01BQUU5QyxNQUFNO01BQUVWLFVBQVU7TUFBRVk7SUFBTSxDQUFDLENBQUM7SUFDMUUsTUFBTStOLFNBQVMsR0FBRyxNQUFNLElBQUFuSixzQkFBWSxFQUFDa0osT0FBTyxDQUFDO0lBQzdDLE9BQU85VCxVQUFVLENBQUM0WSxxQkFBcUIsQ0FBQzdFLFNBQVMsQ0FBQztFQUNwRDtFQU9BLE1BQU04RSxtQkFBbUJBLENBQUN6VCxVQUFrQixFQUFFMFQsY0FBeUQsRUFBRTtJQUN2RyxNQUFNQyxjQUFjLEdBQUcsQ0FBQ1Ysd0JBQWUsQ0FBQ0MsVUFBVSxFQUFFRCx3QkFBZSxDQUFDRSxVQUFVLENBQUM7SUFDL0UsTUFBTVMsVUFBVSxHQUFHLENBQUNDLGlDQUF3QixDQUFDQyxJQUFJLEVBQUVELGlDQUF3QixDQUFDRSxLQUFLLENBQUM7SUFFbEYsSUFBSSxDQUFDLElBQUEzTyx5QkFBaUIsRUFBQ3BGLFVBQVUsQ0FBQyxFQUFFO01BQ2xDLE1BQU0sSUFBSWxHLE1BQU0sQ0FBQ3VMLHNCQUFzQixDQUFDLHVCQUF1QixHQUFHckYsVUFBVSxDQUFDO0lBQy9FO0lBRUEsSUFBSTBULGNBQWMsQ0FBQ1YsSUFBSSxJQUFJLENBQUNXLGNBQWMsQ0FBQ3hULFFBQVEsQ0FBQ3VULGNBQWMsQ0FBQ1YsSUFBSSxDQUFDLEVBQUU7TUFDeEUsTUFBTSxJQUFJcFQsU0FBUyxDQUFDLHdDQUF3QytULGNBQWMsRUFBRSxDQUFDO0lBQy9FO0lBQ0EsSUFBSUQsY0FBYyxDQUFDTSxJQUFJLElBQUksQ0FBQ0osVUFBVSxDQUFDelQsUUFBUSxDQUFDdVQsY0FBYyxDQUFDTSxJQUFJLENBQUMsRUFBRTtNQUNwRSxNQUFNLElBQUlwVSxTQUFTLENBQUMsd0NBQXdDZ1UsVUFBVSxFQUFFLENBQUM7SUFDM0U7SUFDQSxJQUFJRixjQUFjLENBQUNPLFFBQVEsSUFBSSxDQUFDLElBQUF0USxnQkFBUSxFQUFDK1AsY0FBYyxDQUFDTyxRQUFRLENBQUMsRUFBRTtNQUNqRSxNQUFNLElBQUlyVSxTQUFTLENBQUMsNENBQTRDLENBQUM7SUFDbkU7SUFFQSxNQUFNYyxNQUFNLEdBQUcsS0FBSztJQUNwQixNQUFNRSxLQUFLLEdBQUcsYUFBYTtJQUUzQixNQUFNd1IsTUFBNkIsR0FBRztNQUNwQzhCLGlCQUFpQixFQUFFO0lBQ3JCLENBQUM7SUFDRCxNQUFNQyxVQUFVLEdBQUd0WSxNQUFNLENBQUM4VixJQUFJLENBQUMrQixjQUFjLENBQUM7SUFFOUMsTUFBTVUsWUFBWSxHQUFHLENBQUMsTUFBTSxFQUFFLE1BQU0sRUFBRSxVQUFVLENBQUMsQ0FBQ0MsS0FBSyxDQUFFQyxHQUFHLElBQUtILFVBQVUsQ0FBQ2hVLFFBQVEsQ0FBQ21VLEdBQUcsQ0FBQyxDQUFDO0lBQzFGO0lBQ0EsSUFBSUgsVUFBVSxDQUFDdlEsTUFBTSxHQUFHLENBQUMsRUFBRTtNQUN6QixJQUFJLENBQUN3USxZQUFZLEVBQUU7UUFDakIsTUFBTSxJQUFJeFUsU0FBUyxDQUNqQix5R0FDRixDQUFDO01BQ0gsQ0FBQyxNQUFNO1FBQ0x3UyxNQUFNLENBQUNkLElBQUksR0FBRztVQUNaaUQsZ0JBQWdCLEVBQUUsQ0FBQztRQUNyQixDQUFDO1FBQ0QsSUFBSWIsY0FBYyxDQUFDVixJQUFJLEVBQUU7VUFDdkJaLE1BQU0sQ0FBQ2QsSUFBSSxDQUFDaUQsZ0JBQWdCLENBQUNsQixJQUFJLEdBQUdLLGNBQWMsQ0FBQ1YsSUFBSTtRQUN6RDtRQUNBLElBQUlVLGNBQWMsQ0FBQ00sSUFBSSxLQUFLSCxpQ0FBd0IsQ0FBQ0MsSUFBSSxFQUFFO1VBQ3pEMUIsTUFBTSxDQUFDZCxJQUFJLENBQUNpRCxnQkFBZ0IsQ0FBQ0MsSUFBSSxHQUFHZCxjQUFjLENBQUNPLFFBQVE7UUFDN0QsQ0FBQyxNQUFNLElBQUlQLGNBQWMsQ0FBQ00sSUFBSSxLQUFLSCxpQ0FBd0IsQ0FBQ0UsS0FBSyxFQUFFO1VBQ2pFM0IsTUFBTSxDQUFDZCxJQUFJLENBQUNpRCxnQkFBZ0IsQ0FBQ0UsS0FBSyxHQUFHZixjQUFjLENBQUNPLFFBQVE7UUFDOUQ7TUFDRjtJQUNGO0lBRUEsTUFBTXJHLE9BQU8sR0FBRyxJQUFJM1IsZUFBTSxDQUFDQyxPQUFPLENBQUM7TUFDakNvVyxRQUFRLEVBQUUseUJBQXlCO01BQ25DblcsVUFBVSxFQUFFO1FBQUVDLE1BQU0sRUFBRTtNQUFNLENBQUM7TUFDN0JDLFFBQVEsRUFBRTtJQUNaLENBQUMsQ0FBQztJQUNGLE1BQU1vSCxPQUFPLEdBQUdtSyxPQUFPLENBQUMvRyxXQUFXLENBQUN1TCxNQUFNLENBQUM7SUFFM0MsTUFBTXpSLE9BQXVCLEdBQUcsQ0FBQyxDQUFDO0lBQ2xDQSxPQUFPLENBQUMsYUFBYSxDQUFDLEdBQUcsSUFBQTRRLGFBQUssRUFBQzlOLE9BQU8sQ0FBQztJQUV2QyxNQUFNLElBQUksQ0FBQ08sb0JBQW9CLENBQUM7TUFBRXRELE1BQU07TUFBRVYsVUFBVTtNQUFFWSxLQUFLO01BQUVEO0lBQVEsQ0FBQyxFQUFFOEMsT0FBTyxDQUFDO0VBQ2xGO0VBRUEsTUFBTWlSLG1CQUFtQkEsQ0FBQzFVLFVBQWtCLEVBQTBDO0lBQ3BGLElBQUksQ0FBQyxJQUFBb0YseUJBQWlCLEVBQUNwRixVQUFVLENBQUMsRUFBRTtNQUNsQyxNQUFNLElBQUlsRyxNQUFNLENBQUN1TCxzQkFBc0IsQ0FBQyx1QkFBdUIsR0FBR3JGLFVBQVUsQ0FBQztJQUMvRTtJQUNBLE1BQU1VLE1BQU0sR0FBRyxLQUFLO0lBQ3BCLE1BQU1FLEtBQUssR0FBRyxZQUFZO0lBRTFCLE1BQU04TixPQUFPLEdBQUcsTUFBTSxJQUFJLENBQUNsTCxnQkFBZ0IsQ0FBQztNQUFFOUMsTUFBTTtNQUFFVixVQUFVO01BQUVZO0lBQU0sQ0FBQyxDQUFDO0lBQzFFLE1BQU0rTixTQUFTLEdBQUcsTUFBTSxJQUFBbkosc0JBQVksRUFBQ2tKLE9BQU8sQ0FBQztJQUM3QyxPQUFPLE1BQU05VCxVQUFVLENBQUMrWiwyQkFBMkIsQ0FBQ2hHLFNBQVMsQ0FBQztFQUNoRTtFQUVBLE1BQU1pRyxtQkFBbUJBLENBQUM1VSxVQUFrQixFQUFFNlUsYUFBNEMsRUFBaUI7SUFDekcsSUFBSSxDQUFDLElBQUF6UCx5QkFBaUIsRUFBQ3BGLFVBQVUsQ0FBQyxFQUFFO01BQ2xDLE1BQU0sSUFBSWxHLE1BQU0sQ0FBQ3VMLHNCQUFzQixDQUFDLHVCQUF1QixHQUFHckYsVUFBVSxDQUFDO0lBQy9FO0lBQ0EsSUFBSSxDQUFDbkUsTUFBTSxDQUFDOFYsSUFBSSxDQUFDa0QsYUFBYSxDQUFDLENBQUNqUixNQUFNLEVBQUU7TUFDdEMsTUFBTSxJQUFJOUosTUFBTSxDQUFDMEQsb0JBQW9CLENBQUMsMENBQTBDLENBQUM7SUFDbkY7SUFFQSxNQUFNa0QsTUFBTSxHQUFHLEtBQUs7SUFDcEIsTUFBTUUsS0FBSyxHQUFHLFlBQVk7SUFDMUIsTUFBTWdOLE9BQU8sR0FBRyxJQUFJM1IsZUFBTSxDQUFDQyxPQUFPLENBQUM7TUFDakNvVyxRQUFRLEVBQUUseUJBQXlCO01BQ25DblcsVUFBVSxFQUFFO1FBQUVDLE1BQU0sRUFBRTtNQUFNLENBQUM7TUFDN0JDLFFBQVEsRUFBRTtJQUNaLENBQUMsQ0FBQztJQUNGLE1BQU1vSCxPQUFPLEdBQUdtSyxPQUFPLENBQUMvRyxXQUFXLENBQUNnTyxhQUFhLENBQUM7SUFFbEQsTUFBTSxJQUFJLENBQUM3USxvQkFBb0IsQ0FBQztNQUFFdEQsTUFBTTtNQUFFVixVQUFVO01BQUVZO0lBQU0sQ0FBQyxFQUFFNkMsT0FBTyxDQUFDO0VBQ3pFO0VBRUEsTUFBY3FSLFVBQVVBLENBQUNDLGFBQStCLEVBQWlCO0lBQ3ZFLE1BQU07TUFBRS9VLFVBQVU7TUFBRUMsVUFBVTtNQUFFK1UsSUFBSTtNQUFFQztJQUFRLENBQUMsR0FBR0YsYUFBYTtJQUMvRCxNQUFNclUsTUFBTSxHQUFHLEtBQUs7SUFDcEIsSUFBSUUsS0FBSyxHQUFHLFNBQVM7SUFFckIsSUFBSXFVLE9BQU8sSUFBSUEsT0FBTyxhQUFQQSxPQUFPLGVBQVBBLE9BQU8sQ0FBRTlLLFNBQVMsRUFBRTtNQUNqQ3ZKLEtBQUssR0FBRyxHQUFHQSxLQUFLLGNBQWNxVSxPQUFPLENBQUM5SyxTQUFTLEVBQUU7SUFDbkQ7SUFDQSxNQUFNK0ssUUFBUSxHQUFHLEVBQUU7SUFDbkIsS0FBSyxNQUFNLENBQUNuSixHQUFHLEVBQUVvSixLQUFLLENBQUMsSUFBSXRaLE1BQU0sQ0FBQzBGLE9BQU8sQ0FBQ3lULElBQUksQ0FBQyxFQUFFO01BQy9DRSxRQUFRLENBQUM3TSxJQUFJLENBQUM7UUFBRStNLEdBQUcsRUFBRXJKLEdBQUc7UUFBRXNKLEtBQUssRUFBRUY7TUFBTSxDQUFDLENBQUM7SUFDM0M7SUFDQSxNQUFNRyxhQUFhLEdBQUc7TUFDcEJDLE9BQU8sRUFBRTtRQUNQQyxNQUFNLEVBQUU7VUFDTkMsR0FBRyxFQUFFUDtRQUNQO01BQ0Y7SUFDRixDQUFDO0lBQ0QsTUFBTXZVLE9BQU8sR0FBRyxDQUFDLENBQW1CO0lBQ3BDLE1BQU1pTixPQUFPLEdBQUcsSUFBSTNSLGVBQU0sQ0FBQ0MsT0FBTyxDQUFDO01BQUVHLFFBQVEsRUFBRSxJQUFJO01BQUVGLFVBQVUsRUFBRTtRQUFFQyxNQUFNLEVBQUU7TUFBTTtJQUFFLENBQUMsQ0FBQztJQUNyRixNQUFNc1osVUFBVSxHQUFHclIsTUFBTSxDQUFDd0UsSUFBSSxDQUFDK0UsT0FBTyxDQUFDL0csV0FBVyxDQUFDeU8sYUFBYSxDQUFDLENBQUM7SUFDbEUsTUFBTWxJLGNBQWMsR0FBRztNQUNyQjFNLE1BQU07TUFDTlYsVUFBVTtNQUNWWSxLQUFLO01BQ0xELE9BQU87TUFFUCxJQUFJVixVQUFVLElBQUk7UUFBRUEsVUFBVSxFQUFFQTtNQUFXLENBQUM7SUFDOUMsQ0FBQztJQUVEVSxPQUFPLENBQUMsYUFBYSxDQUFDLEdBQUcsSUFBQTRRLGFBQUssRUFBQ21FLFVBQVUsQ0FBQztJQUUxQyxNQUFNLElBQUksQ0FBQzFSLG9CQUFvQixDQUFDb0osY0FBYyxFQUFFc0ksVUFBVSxDQUFDO0VBQzdEO0VBRUEsTUFBY0MsYUFBYUEsQ0FBQztJQUFFM1YsVUFBVTtJQUFFQyxVQUFVO0lBQUVzSztFQUFnQyxDQUFDLEVBQWlCO0lBQ3RHLE1BQU03SixNQUFNLEdBQUcsUUFBUTtJQUN2QixJQUFJRSxLQUFLLEdBQUcsU0FBUztJQUVyQixJQUFJMkosVUFBVSxJQUFJMU8sTUFBTSxDQUFDOFYsSUFBSSxDQUFDcEgsVUFBVSxDQUFDLENBQUMzRyxNQUFNLElBQUkyRyxVQUFVLENBQUNKLFNBQVMsRUFBRTtNQUN4RXZKLEtBQUssR0FBRyxHQUFHQSxLQUFLLGNBQWMySixVQUFVLENBQUNKLFNBQVMsRUFBRTtJQUN0RDtJQUNBLE1BQU1pRCxjQUFjLEdBQUc7TUFBRTFNLE1BQU07TUFBRVYsVUFBVTtNQUFFQyxVQUFVO01BQUVXO0lBQU0sQ0FBQztJQUVoRSxJQUFJWCxVQUFVLEVBQUU7TUFDZG1OLGNBQWMsQ0FBQyxZQUFZLENBQUMsR0FBR25OLFVBQVU7SUFDM0M7SUFDQSxNQUFNLElBQUksQ0FBQ3VELGdCQUFnQixDQUFDNEosY0FBYyxFQUFFLEVBQUUsRUFBRSxDQUFDLEdBQUcsRUFBRSxHQUFHLENBQUMsQ0FBQztFQUM3RDtFQUVBLE1BQU13SSxnQkFBZ0JBLENBQUM1VixVQUFrQixFQUFFZ1YsSUFBVSxFQUFpQjtJQUNwRSxJQUFJLENBQUMsSUFBQTVQLHlCQUFpQixFQUFDcEYsVUFBVSxDQUFDLEVBQUU7TUFDbEMsTUFBTSxJQUFJbEcsTUFBTSxDQUFDdUwsc0JBQXNCLENBQUMsdUJBQXVCLEdBQUdyRixVQUFVLENBQUM7SUFDL0U7SUFDQSxJQUFJLENBQUMsSUFBQTZWLHFCQUFhLEVBQUNiLElBQUksQ0FBQyxFQUFFO01BQ3hCLE1BQU0sSUFBSWxiLE1BQU0sQ0FBQzBELG9CQUFvQixDQUFDLGlDQUFpQyxDQUFDO0lBQzFFO0lBQ0EsSUFBSTNCLE1BQU0sQ0FBQzhWLElBQUksQ0FBQ3FELElBQUksQ0FBQyxDQUFDcFIsTUFBTSxHQUFHLEVBQUUsRUFBRTtNQUNqQyxNQUFNLElBQUk5SixNQUFNLENBQUMwRCxvQkFBb0IsQ0FBQyw2QkFBNkIsQ0FBQztJQUN0RTtJQUVBLE1BQU0sSUFBSSxDQUFDc1gsVUFBVSxDQUFDO01BQUU5VSxVQUFVO01BQUVnVjtJQUFLLENBQUMsQ0FBQztFQUM3QztFQUVBLE1BQU1jLG1CQUFtQkEsQ0FBQzlWLFVBQWtCLEVBQUU7SUFDNUMsSUFBSSxDQUFDLElBQUFvRix5QkFBaUIsRUFBQ3BGLFVBQVUsQ0FBQyxFQUFFO01BQ2xDLE1BQU0sSUFBSWxHLE1BQU0sQ0FBQ3VMLHNCQUFzQixDQUFDLHVCQUF1QixHQUFHckYsVUFBVSxDQUFDO0lBQy9FO0lBQ0EsTUFBTSxJQUFJLENBQUMyVixhQUFhLENBQUM7TUFBRTNWO0lBQVcsQ0FBQyxDQUFDO0VBQzFDO0VBRUEsTUFBTStWLGdCQUFnQkEsQ0FBQy9WLFVBQWtCLEVBQUVDLFVBQWtCLEVBQUUrVSxJQUFVLEVBQUVDLE9BQXFCLEVBQUU7SUFDaEcsSUFBSSxDQUFDLElBQUE3UCx5QkFBaUIsRUFBQ3BGLFVBQVUsQ0FBQyxFQUFFO01BQ2xDLE1BQU0sSUFBSWxHLE1BQU0sQ0FBQ3VMLHNCQUFzQixDQUFDLHVCQUF1QixHQUFHckYsVUFBVSxDQUFDO0lBQy9FO0lBQ0EsSUFBSSxDQUFDLElBQUF5SCx5QkFBaUIsRUFBQ3hILFVBQVUsQ0FBQyxFQUFFO01BQ2xDLE1BQU0sSUFBSW5HLE1BQU0sQ0FBQ3VMLHNCQUFzQixDQUFDLHVCQUF1QixHQUFHcEYsVUFBVSxDQUFDO0lBQy9FO0lBRUEsSUFBSSxDQUFDLElBQUE0VixxQkFBYSxFQUFDYixJQUFJLENBQUMsRUFBRTtNQUN4QixNQUFNLElBQUlsYixNQUFNLENBQUMwRCxvQkFBb0IsQ0FBQyxpQ0FBaUMsQ0FBQztJQUMxRTtJQUNBLElBQUkzQixNQUFNLENBQUM4VixJQUFJLENBQUNxRCxJQUFJLENBQUMsQ0FBQ3BSLE1BQU0sR0FBRyxFQUFFLEVBQUU7TUFDakMsTUFBTSxJQUFJOUosTUFBTSxDQUFDMEQsb0JBQW9CLENBQUMsNkJBQTZCLENBQUM7SUFDdEU7SUFFQSxNQUFNLElBQUksQ0FBQ3NYLFVBQVUsQ0FBQztNQUFFOVUsVUFBVTtNQUFFQyxVQUFVO01BQUUrVSxJQUFJO01BQUVDO0lBQVEsQ0FBQyxDQUFDO0VBQ2xFO0VBRUEsTUFBTWUsbUJBQW1CQSxDQUFDaFcsVUFBa0IsRUFBRUMsVUFBa0IsRUFBRXNLLFVBQXVCLEVBQUU7SUFDekYsSUFBSSxDQUFDLElBQUFuRix5QkFBaUIsRUFBQ3BGLFVBQVUsQ0FBQyxFQUFFO01BQ2xDLE1BQU0sSUFBSWxHLE1BQU0sQ0FBQ3VMLHNCQUFzQixDQUFDLHVCQUF1QixHQUFHckYsVUFBVSxDQUFDO0lBQy9FO0lBQ0EsSUFBSSxDQUFDLElBQUF5SCx5QkFBaUIsRUFBQ3hILFVBQVUsQ0FBQyxFQUFFO01BQ2xDLE1BQU0sSUFBSW5HLE1BQU0sQ0FBQ3VMLHNCQUFzQixDQUFDLHVCQUF1QixHQUFHcEYsVUFBVSxDQUFDO0lBQy9FO0lBQ0EsSUFBSXNLLFVBQVUsSUFBSTFPLE1BQU0sQ0FBQzhWLElBQUksQ0FBQ3BILFVBQVUsQ0FBQyxDQUFDM0csTUFBTSxJQUFJLENBQUMsSUFBQTFGLGdCQUFRLEVBQUNxTSxVQUFVLENBQUMsRUFBRTtNQUN6RSxNQUFNLElBQUl6USxNQUFNLENBQUMwRCxvQkFBb0IsQ0FBQyx1Q0FBdUMsQ0FBQztJQUNoRjtJQUVBLE1BQU0sSUFBSSxDQUFDbVksYUFBYSxDQUFDO01BQUUzVixVQUFVO01BQUVDLFVBQVU7TUFBRXNLO0lBQVcsQ0FBQyxDQUFDO0VBQ2xFO0VBRUEsTUFBTTBMLG1CQUFtQkEsQ0FDdkJqVyxVQUFrQixFQUNsQkMsVUFBa0IsRUFDbEJpVyxVQUF5QixFQUNXO0lBQ3BDLElBQUksQ0FBQyxJQUFBOVEseUJBQWlCLEVBQUNwRixVQUFVLENBQUMsRUFBRTtNQUNsQyxNQUFNLElBQUlsRyxNQUFNLENBQUN1TCxzQkFBc0IsQ0FBQyx3QkFBd0JyRixVQUFVLEVBQUUsQ0FBQztJQUMvRTtJQUNBLElBQUksQ0FBQyxJQUFBeUgseUJBQWlCLEVBQUN4SCxVQUFVLENBQUMsRUFBRTtNQUNsQyxNQUFNLElBQUluRyxNQUFNLENBQUM0TixzQkFBc0IsQ0FBQyx3QkFBd0J6SCxVQUFVLEVBQUUsQ0FBQztJQUMvRTtJQUNBLElBQUksQ0FBQ0osZUFBQyxDQUFDSyxPQUFPLENBQUNnVyxVQUFVLENBQUMsRUFBRTtNQUMxQixJQUFJLENBQUMsSUFBQXZZLGdCQUFRLEVBQUN1WSxVQUFVLENBQUNDLFVBQVUsQ0FBQyxFQUFFO1FBQ3BDLE1BQU0sSUFBSXZXLFNBQVMsQ0FBQywwQ0FBMEMsQ0FBQztNQUNqRTtNQUNBLElBQUksQ0FBQ0MsZUFBQyxDQUFDSyxPQUFPLENBQUNnVyxVQUFVLENBQUNFLGtCQUFrQixDQUFDLEVBQUU7UUFDN0MsSUFBSSxDQUFDLElBQUFsWSxnQkFBUSxFQUFDZ1ksVUFBVSxDQUFDRSxrQkFBa0IsQ0FBQyxFQUFFO1VBQzVDLE1BQU0sSUFBSXhXLFNBQVMsQ0FBQywrQ0FBK0MsQ0FBQztRQUN0RTtNQUNGLENBQUMsTUFBTTtRQUNMLE1BQU0sSUFBSUEsU0FBUyxDQUFDLGdDQUFnQyxDQUFDO01BQ3ZEO01BQ0EsSUFBSSxDQUFDQyxlQUFDLENBQUNLLE9BQU8sQ0FBQ2dXLFVBQVUsQ0FBQ0csbUJBQW1CLENBQUMsRUFBRTtRQUM5QyxJQUFJLENBQUMsSUFBQW5ZLGdCQUFRLEVBQUNnWSxVQUFVLENBQUNHLG1CQUFtQixDQUFDLEVBQUU7VUFDN0MsTUFBTSxJQUFJelcsU0FBUyxDQUFDLGdEQUFnRCxDQUFDO1FBQ3ZFO01BQ0YsQ0FBQyxNQUFNO1FBQ0wsTUFBTSxJQUFJQSxTQUFTLENBQUMsaUNBQWlDLENBQUM7TUFDeEQ7SUFDRixDQUFDLE1BQU07TUFDTCxNQUFNLElBQUlBLFNBQVMsQ0FBQyx3Q0FBd0MsQ0FBQztJQUMvRDtJQUVBLE1BQU1jLE1BQU0sR0FBRyxNQUFNO0lBQ3JCLE1BQU1FLEtBQUssR0FBRyxzQkFBc0I7SUFFcEMsTUFBTXdSLE1BQWlDLEdBQUcsQ0FDeEM7TUFDRWtFLFVBQVUsRUFBRUosVUFBVSxDQUFDQztJQUN6QixDQUFDLEVBQ0Q7TUFDRUksY0FBYyxFQUFFTCxVQUFVLENBQUNNLGNBQWMsSUFBSTtJQUMvQyxDQUFDLEVBQ0Q7TUFDRUMsa0JBQWtCLEVBQUUsQ0FBQ1AsVUFBVSxDQUFDRSxrQkFBa0I7SUFDcEQsQ0FBQyxFQUNEO01BQ0VNLG1CQUFtQixFQUFFLENBQUNSLFVBQVUsQ0FBQ0csbUJBQW1CO0lBQ3RELENBQUMsQ0FDRjs7SUFFRDtJQUNBLElBQUlILFVBQVUsQ0FBQ1MsZUFBZSxFQUFFO01BQzlCdkUsTUFBTSxDQUFDL0osSUFBSSxDQUFDO1FBQUV1TyxlQUFlLEVBQUVWLFVBQVUsYUFBVkEsVUFBVSx1QkFBVkEsVUFBVSxDQUFFUztNQUFnQixDQUFDLENBQUM7SUFDL0Q7SUFDQTtJQUNBLElBQUlULFVBQVUsQ0FBQ1csU0FBUyxFQUFFO01BQ3hCekUsTUFBTSxDQUFDL0osSUFBSSxDQUFDO1FBQUV5TyxTQUFTLEVBQUVaLFVBQVUsQ0FBQ1c7TUFBVSxDQUFDLENBQUM7SUFDbEQ7SUFFQSxNQUFNakosT0FBTyxHQUFHLElBQUkzUixlQUFNLENBQUNDLE9BQU8sQ0FBQztNQUNqQ29XLFFBQVEsRUFBRSw0QkFBNEI7TUFDdENuVyxVQUFVLEVBQUU7UUFBRUMsTUFBTSxFQUFFO01BQU0sQ0FBQztNQUM3QkMsUUFBUSxFQUFFO0lBQ1osQ0FBQyxDQUFDO0lBQ0YsTUFBTW9ILE9BQU8sR0FBR21LLE9BQU8sQ0FBQy9HLFdBQVcsQ0FBQ3VMLE1BQU0sQ0FBQztJQUUzQyxNQUFNbE8sR0FBRyxHQUFHLE1BQU0sSUFBSSxDQUFDVixnQkFBZ0IsQ0FBQztNQUFFOUMsTUFBTTtNQUFFVixVQUFVO01BQUVDLFVBQVU7TUFBRVc7SUFBTSxDQUFDLEVBQUU2QyxPQUFPLENBQUM7SUFDM0YsTUFBTVcsSUFBSSxHQUFHLE1BQU0sSUFBQTZJLHNCQUFZLEVBQUMvSSxHQUFHLENBQUM7SUFDcEMsT0FBTyxJQUFBNlMsMkNBQWdDLEVBQUMzUyxJQUFJLENBQUM7RUFDL0M7RUFFQSxNQUFjNFMsb0JBQW9CQSxDQUFDaFgsVUFBa0IsRUFBRWlYLFlBQWtDLEVBQWlCO0lBQ3hHLE1BQU12VyxNQUFNLEdBQUcsS0FBSztJQUNwQixNQUFNRSxLQUFLLEdBQUcsV0FBVztJQUV6QixNQUFNRCxPQUF1QixHQUFHLENBQUMsQ0FBQztJQUNsQyxNQUFNaU4sT0FBTyxHQUFHLElBQUkzUixlQUFNLENBQUNDLE9BQU8sQ0FBQztNQUNqQ29XLFFBQVEsRUFBRSx3QkFBd0I7TUFDbENqVyxRQUFRLEVBQUUsSUFBSTtNQUNkRixVQUFVLEVBQUU7UUFBRUMsTUFBTSxFQUFFO01BQU07SUFDOUIsQ0FBQyxDQUFDO0lBQ0YsTUFBTXFILE9BQU8sR0FBR21LLE9BQU8sQ0FBQy9HLFdBQVcsQ0FBQ29RLFlBQVksQ0FBQztJQUNqRHRXLE9BQU8sQ0FBQyxhQUFhLENBQUMsR0FBRyxJQUFBNFEsYUFBSyxFQUFDOU4sT0FBTyxDQUFDO0lBRXZDLE1BQU0sSUFBSSxDQUFDTyxvQkFBb0IsQ0FBQztNQUFFdEQsTUFBTTtNQUFFVixVQUFVO01BQUVZLEtBQUs7TUFBRUQ7SUFBUSxDQUFDLEVBQUU4QyxPQUFPLENBQUM7RUFDbEY7RUFFQSxNQUFNeVQscUJBQXFCQSxDQUFDbFgsVUFBa0IsRUFBaUI7SUFDN0QsSUFBSSxDQUFDLElBQUFvRix5QkFBaUIsRUFBQ3BGLFVBQVUsQ0FBQyxFQUFFO01BQ2xDLE1BQU0sSUFBSWxHLE1BQU0sQ0FBQ3VMLHNCQUFzQixDQUFDLHVCQUF1QixHQUFHckYsVUFBVSxDQUFDO0lBQy9FO0lBQ0EsTUFBTVUsTUFBTSxHQUFHLFFBQVE7SUFDdkIsTUFBTUUsS0FBSyxHQUFHLFdBQVc7SUFDekIsTUFBTSxJQUFJLENBQUNvRCxvQkFBb0IsQ0FBQztNQUFFdEQsTUFBTTtNQUFFVixVQUFVO01BQUVZO0lBQU0sQ0FBQyxFQUFFLEVBQUUsRUFBRSxDQUFDLEdBQUcsQ0FBQyxDQUFDO0VBQzNFO0VBRUEsTUFBTXVXLGtCQUFrQkEsQ0FBQ25YLFVBQWtCLEVBQUVvWCxlQUFxQyxFQUFpQjtJQUNqRyxJQUFJLENBQUMsSUFBQWhTLHlCQUFpQixFQUFDcEYsVUFBVSxDQUFDLEVBQUU7TUFDbEMsTUFBTSxJQUFJbEcsTUFBTSxDQUFDdUwsc0JBQXNCLENBQUMsdUJBQXVCLEdBQUdyRixVQUFVLENBQUM7SUFDL0U7SUFDQSxJQUFJSCxlQUFDLENBQUNLLE9BQU8sQ0FBQ2tYLGVBQWUsQ0FBQyxFQUFFO01BQzlCLE1BQU0sSUFBSSxDQUFDRixxQkFBcUIsQ0FBQ2xYLFVBQVUsQ0FBQztJQUM5QyxDQUFDLE1BQU07TUFDTCxNQUFNLElBQUksQ0FBQ2dYLG9CQUFvQixDQUFDaFgsVUFBVSxFQUFFb1gsZUFBZSxDQUFDO0lBQzlEO0VBQ0Y7RUFFQSxNQUFNQyxrQkFBa0JBLENBQUNyWCxVQUFrQixFQUFtQztJQUM1RSxJQUFJLENBQUMsSUFBQW9GLHlCQUFpQixFQUFDcEYsVUFBVSxDQUFDLEVBQUU7TUFDbEMsTUFBTSxJQUFJbEcsTUFBTSxDQUFDdUwsc0JBQXNCLENBQUMsdUJBQXVCLEdBQUdyRixVQUFVLENBQUM7SUFDL0U7SUFDQSxNQUFNVSxNQUFNLEdBQUcsS0FBSztJQUNwQixNQUFNRSxLQUFLLEdBQUcsV0FBVztJQUV6QixNQUFNc0QsR0FBRyxHQUFHLE1BQU0sSUFBSSxDQUFDVixnQkFBZ0IsQ0FBQztNQUFFOUMsTUFBTTtNQUFFVixVQUFVO01BQUVZO0lBQU0sQ0FBQyxDQUFDO0lBQ3RFLE1BQU13RCxJQUFJLEdBQUcsTUFBTSxJQUFBb0Isc0JBQVksRUFBQ3RCLEdBQUcsQ0FBQztJQUNwQyxPQUFPdEosVUFBVSxDQUFDMGMsb0JBQW9CLENBQUNsVCxJQUFJLENBQUM7RUFDOUM7RUFFQSxNQUFNbVQsbUJBQW1CQSxDQUFDdlgsVUFBa0IsRUFBRXdYLGdCQUFtQyxFQUFpQjtJQUNoRyxJQUFJLENBQUMsSUFBQXBTLHlCQUFpQixFQUFDcEYsVUFBVSxDQUFDLEVBQUU7TUFDbEMsTUFBTSxJQUFJbEcsTUFBTSxDQUFDdUwsc0JBQXNCLENBQUMsdUJBQXVCLEdBQUdyRixVQUFVLENBQUM7SUFDL0U7SUFDQSxJQUFJLENBQUNILGVBQUMsQ0FBQ0ssT0FBTyxDQUFDc1gsZ0JBQWdCLENBQUMsSUFBSUEsZ0JBQWdCLENBQUNsRyxJQUFJLENBQUMxTixNQUFNLEdBQUcsQ0FBQyxFQUFFO01BQ3BFLE1BQU0sSUFBSTlKLE1BQU0sQ0FBQzBELG9CQUFvQixDQUFDLGtEQUFrRCxHQUFHZ2EsZ0JBQWdCLENBQUNsRyxJQUFJLENBQUM7SUFDbkg7SUFFQSxJQUFJbUcsYUFBYSxHQUFHRCxnQkFBZ0I7SUFDcEMsSUFBSTNYLGVBQUMsQ0FBQ0ssT0FBTyxDQUFDc1gsZ0JBQWdCLENBQUMsRUFBRTtNQUMvQkMsYUFBYSxHQUFHO1FBQ2Q7UUFDQW5HLElBQUksRUFBRSxDQUNKO1VBQ0VvRyxrQ0FBa0MsRUFBRTtZQUNsQ0MsWUFBWSxFQUFFO1VBQ2hCO1FBQ0YsQ0FBQztNQUVMLENBQUM7SUFDSDtJQUVBLE1BQU1qWCxNQUFNLEdBQUcsS0FBSztJQUNwQixNQUFNRSxLQUFLLEdBQUcsWUFBWTtJQUMxQixNQUFNZ04sT0FBTyxHQUFHLElBQUkzUixlQUFNLENBQUNDLE9BQU8sQ0FBQztNQUNqQ29XLFFBQVEsRUFBRSxtQ0FBbUM7TUFDN0NuVyxVQUFVLEVBQUU7UUFBRUMsTUFBTSxFQUFFO01BQU0sQ0FBQztNQUM3QkMsUUFBUSxFQUFFO0lBQ1osQ0FBQyxDQUFDO0lBQ0YsTUFBTW9ILE9BQU8sR0FBR21LLE9BQU8sQ0FBQy9HLFdBQVcsQ0FBQzRRLGFBQWEsQ0FBQztJQUVsRCxNQUFNOVcsT0FBdUIsR0FBRyxDQUFDLENBQUM7SUFDbENBLE9BQU8sQ0FBQyxhQUFhLENBQUMsR0FBRyxJQUFBNFEsYUFBSyxFQUFDOU4sT0FBTyxDQUFDO0lBRXZDLE1BQU0sSUFBSSxDQUFDTyxvQkFBb0IsQ0FBQztNQUFFdEQsTUFBTTtNQUFFVixVQUFVO01BQUVZLEtBQUs7TUFBRUQ7SUFBUSxDQUFDLEVBQUU4QyxPQUFPLENBQUM7RUFDbEY7RUFFQSxNQUFNbVUsbUJBQW1CQSxDQUFDNVgsVUFBa0IsRUFBRTtJQUM1QyxJQUFJLENBQUMsSUFBQW9GLHlCQUFpQixFQUFDcEYsVUFBVSxDQUFDLEVBQUU7TUFDbEMsTUFBTSxJQUFJbEcsTUFBTSxDQUFDdUwsc0JBQXNCLENBQUMsdUJBQXVCLEdBQUdyRixVQUFVLENBQUM7SUFDL0U7SUFDQSxNQUFNVSxNQUFNLEdBQUcsS0FBSztJQUNwQixNQUFNRSxLQUFLLEdBQUcsWUFBWTtJQUUxQixNQUFNc0QsR0FBRyxHQUFHLE1BQU0sSUFBSSxDQUFDVixnQkFBZ0IsQ0FBQztNQUFFOUMsTUFBTTtNQUFFVixVQUFVO01BQUVZO0lBQU0sQ0FBQyxDQUFDO0lBQ3RFLE1BQU13RCxJQUFJLEdBQUcsTUFBTSxJQUFBb0Isc0JBQVksRUFBQ3RCLEdBQUcsQ0FBQztJQUNwQyxPQUFPdEosVUFBVSxDQUFDaWQsMkJBQTJCLENBQUN6VCxJQUFJLENBQUM7RUFDckQ7RUFFQSxNQUFNMFQsc0JBQXNCQSxDQUFDOVgsVUFBa0IsRUFBRTtJQUMvQyxJQUFJLENBQUMsSUFBQW9GLHlCQUFpQixFQUFDcEYsVUFBVSxDQUFDLEVBQUU7TUFDbEMsTUFBTSxJQUFJbEcsTUFBTSxDQUFDdUwsc0JBQXNCLENBQUMsdUJBQXVCLEdBQUdyRixVQUFVLENBQUM7SUFDL0U7SUFDQSxNQUFNVSxNQUFNLEdBQUcsUUFBUTtJQUN2QixNQUFNRSxLQUFLLEdBQUcsWUFBWTtJQUUxQixNQUFNLElBQUksQ0FBQ29ELG9CQUFvQixDQUFDO01BQUV0RCxNQUFNO01BQUVWLFVBQVU7TUFBRVk7SUFBTSxDQUFDLEVBQUUsRUFBRSxFQUFFLENBQUMsR0FBRyxDQUFDLENBQUM7RUFDM0U7RUFFQSxNQUFNbVgsa0JBQWtCQSxDQUN0Qi9YLFVBQWtCLEVBQ2xCQyxVQUFrQixFQUNsQnVILE9BQWdDLEVBQ2lCO0lBQ2pELElBQUksQ0FBQyxJQUFBcEMseUJBQWlCLEVBQUNwRixVQUFVLENBQUMsRUFBRTtNQUNsQyxNQUFNLElBQUlsRyxNQUFNLENBQUN1TCxzQkFBc0IsQ0FBQyx1QkFBdUIsR0FBR3JGLFVBQVUsQ0FBQztJQUMvRTtJQUNBLElBQUksQ0FBQyxJQUFBeUgseUJBQWlCLEVBQUN4SCxVQUFVLENBQUMsRUFBRTtNQUNsQyxNQUFNLElBQUluRyxNQUFNLENBQUM0TixzQkFBc0IsQ0FBQyx3QkFBd0J6SCxVQUFVLEVBQUUsQ0FBQztJQUMvRTtJQUNBLElBQUl1SCxPQUFPLElBQUksQ0FBQyxJQUFBdEosZ0JBQVEsRUFBQ3NKLE9BQU8sQ0FBQyxFQUFFO01BQ2pDLE1BQU0sSUFBSTFOLE1BQU0sQ0FBQzBELG9CQUFvQixDQUFDLG9DQUFvQyxDQUFDO0lBQzdFLENBQUMsTUFBTSxJQUFJZ0ssT0FBTyxhQUFQQSxPQUFPLGVBQVBBLE9BQU8sQ0FBRTJDLFNBQVMsSUFBSSxDQUFDLElBQUF4TSxnQkFBUSxFQUFDNkosT0FBTyxDQUFDMkMsU0FBUyxDQUFDLEVBQUU7TUFDN0QsTUFBTSxJQUFJclEsTUFBTSxDQUFDMEQsb0JBQW9CLENBQUMsc0NBQXNDLENBQUM7SUFDL0U7SUFFQSxNQUFNa0QsTUFBTSxHQUFHLEtBQUs7SUFDcEIsSUFBSUUsS0FBSyxHQUFHLFdBQVc7SUFDdkIsSUFBSTRHLE9BQU8sYUFBUEEsT0FBTyxlQUFQQSxPQUFPLENBQUUyQyxTQUFTLEVBQUU7TUFDdEJ2SixLQUFLLElBQUksY0FBYzRHLE9BQU8sQ0FBQzJDLFNBQVMsRUFBRTtJQUM1QztJQUNBLE1BQU1qRyxHQUFHLEdBQUcsTUFBTSxJQUFJLENBQUNWLGdCQUFnQixDQUFDO01BQUU5QyxNQUFNO01BQUVWLFVBQVU7TUFBRUMsVUFBVTtNQUFFVztJQUFNLENBQUMsQ0FBQztJQUNsRixNQUFNd0QsSUFBSSxHQUFHLE1BQU0sSUFBQW9CLHNCQUFZLEVBQUN0QixHQUFHLENBQUM7SUFDcEMsT0FBT3RKLFVBQVUsQ0FBQ29kLDBCQUEwQixDQUFDNVQsSUFBSSxDQUFDO0VBQ3BEO0VBRUEsTUFBTTZULGFBQWFBLENBQUNqWSxVQUFrQixFQUFFa1ksV0FBK0IsRUFBb0M7SUFDekcsSUFBSSxDQUFDLElBQUE5Uyx5QkFBaUIsRUFBQ3BGLFVBQVUsQ0FBQyxFQUFFO01BQ2xDLE1BQU0sSUFBSWxHLE1BQU0sQ0FBQ3VMLHNCQUFzQixDQUFDLHVCQUF1QixHQUFHckYsVUFBVSxDQUFDO0lBQy9FO0lBQ0EsSUFBSSxDQUFDbVksS0FBSyxDQUFDQyxPQUFPLENBQUNGLFdBQVcsQ0FBQyxFQUFFO01BQy9CLE1BQU0sSUFBSXBlLE1BQU0sQ0FBQzBELG9CQUFvQixDQUFDLDhCQUE4QixDQUFDO0lBQ3ZFO0lBRUEsTUFBTTZhLGdCQUFnQixHQUFHLE1BQU9DLEtBQXlCLElBQXVDO01BQzlGLE1BQU1DLFVBQXVDLEdBQUdELEtBQUssQ0FBQ3ZLLEdBQUcsQ0FBRW9ILEtBQUssSUFBSztRQUNuRSxPQUFPLElBQUFqWCxnQkFBUSxFQUFDaVgsS0FBSyxDQUFDLEdBQUc7VUFBRUMsR0FBRyxFQUFFRCxLQUFLLENBQUNuUCxJQUFJO1VBQUV3UyxTQUFTLEVBQUVyRCxLQUFLLENBQUNoTDtRQUFVLENBQUMsR0FBRztVQUFFaUwsR0FBRyxFQUFFRDtRQUFNLENBQUM7TUFDM0YsQ0FBQyxDQUFDO01BRUYsTUFBTXNELFVBQVUsR0FBRztRQUFFQyxNQUFNLEVBQUU7VUFBRUMsS0FBSyxFQUFFLElBQUk7VUFBRTljLE1BQU0sRUFBRTBjO1FBQVc7TUFBRSxDQUFDO01BQ2xFLE1BQU05VSxPQUFPLEdBQUdZLE1BQU0sQ0FBQ3dFLElBQUksQ0FBQyxJQUFJNU0sZUFBTSxDQUFDQyxPQUFPLENBQUM7UUFBRUcsUUFBUSxFQUFFO01BQUssQ0FBQyxDQUFDLENBQUN3SyxXQUFXLENBQUM0UixVQUFVLENBQUMsQ0FBQztNQUMzRixNQUFNOVgsT0FBdUIsR0FBRztRQUFFLGFBQWEsRUFBRSxJQUFBNFEsYUFBSyxFQUFDOU4sT0FBTztNQUFFLENBQUM7TUFFakUsTUFBTVMsR0FBRyxHQUFHLE1BQU0sSUFBSSxDQUFDVixnQkFBZ0IsQ0FBQztRQUFFOUMsTUFBTSxFQUFFLE1BQU07UUFBRVYsVUFBVTtRQUFFWSxLQUFLLEVBQUUsUUFBUTtRQUFFRDtNQUFRLENBQUMsRUFBRThDLE9BQU8sQ0FBQztNQUMxRyxNQUFNVyxJQUFJLEdBQUcsTUFBTSxJQUFBb0Isc0JBQVksRUFBQ3RCLEdBQUcsQ0FBQztNQUNwQyxPQUFPdEosVUFBVSxDQUFDZ2UsbUJBQW1CLENBQUN4VSxJQUFJLENBQUM7SUFDN0MsQ0FBQztJQUVELE1BQU15VSxVQUFVLEdBQUcsSUFBSSxFQUFDO0lBQ3hCO0lBQ0EsTUFBTUMsT0FBTyxHQUFHLEVBQUU7SUFDbEIsS0FBSyxJQUFJemQsQ0FBQyxHQUFHLENBQUMsRUFBRUEsQ0FBQyxHQUFHNmMsV0FBVyxDQUFDdFUsTUFBTSxFQUFFdkksQ0FBQyxJQUFJd2QsVUFBVSxFQUFFO01BQ3ZEQyxPQUFPLENBQUN6USxJQUFJLENBQUM2UCxXQUFXLENBQUNhLEtBQUssQ0FBQzFkLENBQUMsRUFBRUEsQ0FBQyxHQUFHd2QsVUFBVSxDQUFDLENBQUM7SUFDcEQ7SUFFQSxNQUFNRyxZQUFZLEdBQUcsTUFBTS9JLE9BQU8sQ0FBQ0MsR0FBRyxDQUFDNEksT0FBTyxDQUFDL0ssR0FBRyxDQUFDc0ssZ0JBQWdCLENBQUMsQ0FBQztJQUNyRSxPQUFPVyxZQUFZLENBQUNDLElBQUksQ0FBQyxDQUFDO0VBQzVCO0VBRUEsTUFBTUMsc0JBQXNCQSxDQUFDbFosVUFBa0IsRUFBRUMsVUFBa0IsRUFBaUI7SUFDbEYsSUFBSSxDQUFDLElBQUFtRix5QkFBaUIsRUFBQ3BGLFVBQVUsQ0FBQyxFQUFFO01BQ2xDLE1BQU0sSUFBSWxHLE1BQU0sQ0FBQ3FmLHNCQUFzQixDQUFDLHVCQUF1QixHQUFHblosVUFBVSxDQUFDO0lBQy9FO0lBQ0EsSUFBSSxDQUFDLElBQUF5SCx5QkFBaUIsRUFBQ3hILFVBQVUsQ0FBQyxFQUFFO01BQ2xDLE1BQU0sSUFBSW5HLE1BQU0sQ0FBQzROLHNCQUFzQixDQUFDLHdCQUF3QnpILFVBQVUsRUFBRSxDQUFDO0lBQy9FO0lBQ0EsTUFBTW1aLGNBQWMsR0FBRyxNQUFNLElBQUksQ0FBQy9MLFlBQVksQ0FBQ3JOLFVBQVUsRUFBRUMsVUFBVSxDQUFDO0lBQ3RFLE1BQU1TLE1BQU0sR0FBRyxRQUFRO0lBQ3ZCLE1BQU1FLEtBQUssR0FBRyxZQUFZd1ksY0FBYyxFQUFFO0lBQzFDLE1BQU0sSUFBSSxDQUFDcFYsb0JBQW9CLENBQUM7TUFBRXRELE1BQU07TUFBRVYsVUFBVTtNQUFFQyxVQUFVO01BQUVXO0lBQU0sQ0FBQyxFQUFFLEVBQUUsRUFBRSxDQUFDLEdBQUcsQ0FBQyxDQUFDO0VBQ3ZGO0VBRUEsTUFBY3lZLFlBQVlBLENBQ3hCQyxnQkFBd0IsRUFDeEJDLGdCQUF3QixFQUN4QkMsNkJBQXFDLEVBQ3JDQyxVQUFrQyxFQUNsQztJQUNBLElBQUksT0FBT0EsVUFBVSxJQUFJLFVBQVUsRUFBRTtNQUNuQ0EsVUFBVSxHQUFHLElBQUk7SUFDbkI7SUFFQSxJQUFJLENBQUMsSUFBQXJVLHlCQUFpQixFQUFDa1UsZ0JBQWdCLENBQUMsRUFBRTtNQUN4QyxNQUFNLElBQUl4ZixNQUFNLENBQUN1TCxzQkFBc0IsQ0FBQyx1QkFBdUIsR0FBR2lVLGdCQUFnQixDQUFDO0lBQ3JGO0lBQ0EsSUFBSSxDQUFDLElBQUE3Uix5QkFBaUIsRUFBQzhSLGdCQUFnQixDQUFDLEVBQUU7TUFDeEMsTUFBTSxJQUFJemYsTUFBTSxDQUFDNE4sc0JBQXNCLENBQUMsd0JBQXdCNlIsZ0JBQWdCLEVBQUUsQ0FBQztJQUNyRjtJQUNBLElBQUksQ0FBQyxJQUFBNWIsZ0JBQVEsRUFBQzZiLDZCQUE2QixDQUFDLEVBQUU7TUFDNUMsTUFBTSxJQUFJNVosU0FBUyxDQUFDLDBEQUEwRCxDQUFDO0lBQ2pGO0lBQ0EsSUFBSTRaLDZCQUE2QixLQUFLLEVBQUUsRUFBRTtNQUN4QyxNQUFNLElBQUkxZixNQUFNLENBQUNpUixrQkFBa0IsQ0FBQyxxQkFBcUIsQ0FBQztJQUM1RDtJQUVBLElBQUkwTyxVQUFVLElBQUksSUFBSSxJQUFJLEVBQUVBLFVBQVUsWUFBWUMsOEJBQWMsQ0FBQyxFQUFFO01BQ2pFLE1BQU0sSUFBSTlaLFNBQVMsQ0FBQywrQ0FBK0MsQ0FBQztJQUN0RTtJQUVBLE1BQU1lLE9BQXVCLEdBQUcsQ0FBQyxDQUFDO0lBQ2xDQSxPQUFPLENBQUMsbUJBQW1CLENBQUMsR0FBRyxJQUFBSyx5QkFBaUIsRUFBQ3dZLDZCQUE2QixDQUFDO0lBRS9FLElBQUlDLFVBQVUsRUFBRTtNQUNkLElBQUlBLFVBQVUsQ0FBQ0UsUUFBUSxLQUFLLEVBQUUsRUFBRTtRQUM5QmhaLE9BQU8sQ0FBQyxxQ0FBcUMsQ0FBQyxHQUFHOFksVUFBVSxDQUFDRSxRQUFRO01BQ3RFO01BQ0EsSUFBSUYsVUFBVSxDQUFDRyxVQUFVLEtBQUssRUFBRSxFQUFFO1FBQ2hDalosT0FBTyxDQUFDLHVDQUF1QyxDQUFDLEdBQUc4WSxVQUFVLENBQUNHLFVBQVU7TUFDMUU7TUFDQSxJQUFJSCxVQUFVLENBQUNJLFNBQVMsS0FBSyxFQUFFLEVBQUU7UUFDL0JsWixPQUFPLENBQUMsNEJBQTRCLENBQUMsR0FBRzhZLFVBQVUsQ0FBQ0ksU0FBUztNQUM5RDtNQUNBLElBQUlKLFVBQVUsQ0FBQ0ssZUFBZSxLQUFLLEVBQUUsRUFBRTtRQUNyQ25aLE9BQU8sQ0FBQyxpQ0FBaUMsQ0FBQyxHQUFHOFksVUFBVSxDQUFDSyxlQUFlO01BQ3pFO0lBQ0Y7SUFFQSxNQUFNcFosTUFBTSxHQUFHLEtBQUs7SUFFcEIsTUFBTXdELEdBQUcsR0FBRyxNQUFNLElBQUksQ0FBQ1YsZ0JBQWdCLENBQUM7TUFDdEM5QyxNQUFNO01BQ05WLFVBQVUsRUFBRXNaLGdCQUFnQjtNQUM1QnJaLFVBQVUsRUFBRXNaLGdCQUFnQjtNQUM1QjVZO0lBQ0YsQ0FBQyxDQUFDO0lBQ0YsTUFBTXlELElBQUksR0FBRyxNQUFNLElBQUFvQixzQkFBWSxFQUFDdEIsR0FBRyxDQUFDO0lBQ3BDLE9BQU90SixVQUFVLENBQUNtZixlQUFlLENBQUMzVixJQUFJLENBQUM7RUFDekM7RUFFQSxNQUFjNFYsWUFBWUEsQ0FDeEJDLFlBQStCLEVBQy9CQyxVQUFrQyxFQUNMO0lBQzdCLElBQUksRUFBRUQsWUFBWSxZQUFZRSwwQkFBaUIsQ0FBQyxFQUFFO01BQ2hELE1BQU0sSUFBSXJnQixNQUFNLENBQUMwRCxvQkFBb0IsQ0FBQyxnREFBZ0QsQ0FBQztJQUN6RjtJQUNBLElBQUksRUFBRTBjLFVBQVUsWUFBWUUsK0JBQXNCLENBQUMsRUFBRTtNQUNuRCxNQUFNLElBQUl0Z0IsTUFBTSxDQUFDMEQsb0JBQW9CLENBQUMsbURBQW1ELENBQUM7SUFDNUY7SUFDQSxJQUFJLENBQUMwYyxVQUFVLENBQUNHLFFBQVEsQ0FBQyxDQUFDLEVBQUU7TUFDMUIsT0FBT3BLLE9BQU8sQ0FBQ0csTUFBTSxDQUFDLENBQUM7SUFDekI7SUFDQSxJQUFJLENBQUM4SixVQUFVLENBQUNHLFFBQVEsQ0FBQyxDQUFDLEVBQUU7TUFDMUIsT0FBT3BLLE9BQU8sQ0FBQ0csTUFBTSxDQUFDLENBQUM7SUFDekI7SUFFQSxNQUFNelAsT0FBTyxHQUFHOUUsTUFBTSxDQUFDMkYsTUFBTSxDQUFDLENBQUMsQ0FBQyxFQUFFeVksWUFBWSxDQUFDSyxVQUFVLENBQUMsQ0FBQyxFQUFFSixVQUFVLENBQUNJLFVBQVUsQ0FBQyxDQUFDLENBQUM7SUFFckYsTUFBTXRhLFVBQVUsR0FBR2thLFVBQVUsQ0FBQ0ssTUFBTTtJQUNwQyxNQUFNdGEsVUFBVSxHQUFHaWEsVUFBVSxDQUFDcmUsTUFBTTtJQUVwQyxNQUFNNkUsTUFBTSxHQUFHLEtBQUs7SUFFcEIsTUFBTXdELEdBQUcsR0FBRyxNQUFNLElBQUksQ0FBQ1YsZ0JBQWdCLENBQUM7TUFBRTlDLE1BQU07TUFBRVYsVUFBVTtNQUFFQyxVQUFVO01BQUVVO0lBQVEsQ0FBQyxDQUFDO0lBQ3BGLE1BQU15RCxJQUFJLEdBQUcsTUFBTSxJQUFBb0Isc0JBQVksRUFBQ3RCLEdBQUcsQ0FBQztJQUNwQyxNQUFNc1csT0FBTyxHQUFHNWYsVUFBVSxDQUFDbWYsZUFBZSxDQUFDM1YsSUFBSSxDQUFDO0lBQ2hELE1BQU1xVyxVQUErQixHQUFHdlcsR0FBRyxDQUFDdkQsT0FBTztJQUVuRCxNQUFNK1osZUFBZSxHQUFHRCxVQUFVLElBQUlBLFVBQVUsQ0FBQyxnQkFBZ0IsQ0FBQztJQUNsRSxNQUFNblIsSUFBSSxHQUFHLE9BQU9vUixlQUFlLEtBQUssUUFBUSxHQUFHQSxlQUFlLEdBQUcxZCxTQUFTO0lBRTlFLE9BQU87TUFDTHVkLE1BQU0sRUFBRUwsVUFBVSxDQUFDSyxNQUFNO01BQ3pCbkYsR0FBRyxFQUFFOEUsVUFBVSxDQUFDcmUsTUFBTTtNQUN0QjhlLFlBQVksRUFBRUgsT0FBTyxDQUFDdFEsWUFBWTtNQUNsQzBRLFFBQVEsRUFBRSxJQUFBM1EsdUJBQWUsRUFBQ3dRLFVBQTRCLENBQUM7TUFDdkRqQyxTQUFTLEVBQUUsSUFBQXBPLG9CQUFZLEVBQUNxUSxVQUE0QixDQUFDO01BQ3JESSxlQUFlLEVBQUUsSUFBQUMsMEJBQWtCLEVBQUNMLFVBQTRCLENBQUM7TUFDakVNLElBQUksRUFBRSxJQUFBMVEsb0JBQVksRUFBQ29RLFVBQVUsQ0FBQzNSLElBQUksQ0FBQztNQUNuQ2tTLElBQUksRUFBRTFSO0lBQ1IsQ0FBQztFQUNIO0VBU0EsTUFBTTJSLFVBQVVBLENBQUMsR0FBR0MsT0FBeUIsRUFBNkI7SUFDeEUsSUFBSSxPQUFPQSxPQUFPLENBQUMsQ0FBQyxDQUFDLEtBQUssUUFBUSxFQUFFO01BQ2xDLE1BQU0sQ0FBQzVCLGdCQUFnQixFQUFFQyxnQkFBZ0IsRUFBRUMsNkJBQTZCLEVBQUVDLFVBQVUsQ0FBQyxHQUFHeUIsT0FLdkY7TUFDRCxPQUFPLE1BQU0sSUFBSSxDQUFDN0IsWUFBWSxDQUFDQyxnQkFBZ0IsRUFBRUMsZ0JBQWdCLEVBQUVDLDZCQUE2QixFQUFFQyxVQUFVLENBQUM7SUFDL0c7SUFDQSxNQUFNLENBQUMwQixNQUFNLEVBQUVDLElBQUksQ0FBQyxHQUFHRixPQUFzRDtJQUM3RSxPQUFPLE1BQU0sSUFBSSxDQUFDbEIsWUFBWSxDQUFDbUIsTUFBTSxFQUFFQyxJQUFJLENBQUM7RUFDOUM7RUFFQSxNQUFNQyxVQUFVQSxDQUNkQyxVQU1DLEVBQ0Q3WCxPQUFnQixFQUNoQjtJQUNBLE1BQU07TUFBRXpELFVBQVU7TUFBRUMsVUFBVTtNQUFFc2IsUUFBUTtNQUFFaEwsVUFBVTtNQUFFNVA7SUFBUSxDQUFDLEdBQUcyYSxVQUFVO0lBRTVFLE1BQU01YSxNQUFNLEdBQUcsS0FBSztJQUNwQixNQUFNRSxLQUFLLEdBQUcsWUFBWTJhLFFBQVEsZUFBZWhMLFVBQVUsRUFBRTtJQUM3RCxNQUFNbkQsY0FBYyxHQUFHO01BQUUxTSxNQUFNO01BQUVWLFVBQVU7TUFBRUMsVUFBVSxFQUFFQSxVQUFVO01BQUVXLEtBQUs7TUFBRUQ7SUFBUSxDQUFDO0lBQ3JGLE1BQU11RCxHQUFHLEdBQUcsTUFBTSxJQUFJLENBQUNWLGdCQUFnQixDQUFDNEosY0FBYyxFQUFFM0osT0FBTyxDQUFDO0lBQ2hFLE1BQU1XLElBQUksR0FBRyxNQUFNLElBQUFvQixzQkFBWSxFQUFDdEIsR0FBRyxDQUFDO0lBQ3BDLE1BQU1zWCxPQUFPLEdBQUcsSUFBQUMsMkJBQWdCLEVBQUNyWCxJQUFJLENBQUM7SUFDdEMsTUFBTXNYLFdBQVcsR0FBRyxJQUFBclIsb0JBQVksRUFBQ25HLEdBQUcsQ0FBQ3ZELE9BQU8sQ0FBQ21JLElBQUksQ0FBQyxJQUFJLElBQUF1QixvQkFBWSxFQUFDbVIsT0FBTyxDQUFDdE4sSUFBSSxDQUFDO0lBQ2hGLE9BQU87TUFDTHBGLElBQUksRUFBRTRTLFdBQVc7TUFDakIzUCxHQUFHLEVBQUU5TCxVQUFVO01BQ2ZnTyxJQUFJLEVBQUVzQztJQUNSLENBQUM7RUFDSDtFQUVBLE1BQU1vTCxhQUFhQSxDQUNqQkMsYUFBcUMsRUFDckNDLGFBQWtDLEVBQ2xDO0lBQUVDLGNBQWMsR0FBRztFQUFHLENBQUMsR0FBRyxDQUFDLENBQUMsRUFDc0U7SUFDbEcsTUFBTUMsaUJBQWlCLEdBQUdGLGFBQWEsQ0FBQ2pZLE1BQU07SUFFOUMsSUFBSSxDQUFDdVUsS0FBSyxDQUFDQyxPQUFPLENBQUN5RCxhQUFhLENBQUMsRUFBRTtNQUNqQyxNQUFNLElBQUkvaEIsTUFBTSxDQUFDMEQsb0JBQW9CLENBQUMsb0RBQW9ELENBQUM7SUFDN0Y7SUFDQSxJQUFJLEVBQUVvZSxhQUFhLFlBQVl4QiwrQkFBc0IsQ0FBQyxFQUFFO01BQ3RELE1BQU0sSUFBSXRnQixNQUFNLENBQUMwRCxvQkFBb0IsQ0FBQyxtREFBbUQsQ0FBQztJQUM1RjtJQUVBLElBQUl1ZSxpQkFBaUIsR0FBRyxDQUFDLElBQUlBLGlCQUFpQixHQUFHQyx3QkFBZ0IsQ0FBQ0MsZUFBZSxFQUFFO01BQ2pGLE1BQU0sSUFBSW5pQixNQUFNLENBQUMwRCxvQkFBb0IsQ0FDbkMseUNBQXlDd2Usd0JBQWdCLENBQUNDLGVBQWUsa0JBQzNFLENBQUM7SUFDSDtJQUVBLEtBQUssSUFBSTVnQixDQUFDLEdBQUcsQ0FBQyxFQUFFQSxDQUFDLEdBQUcwZ0IsaUJBQWlCLEVBQUUxZ0IsQ0FBQyxFQUFFLEVBQUU7TUFDMUMsTUFBTTZnQixJQUFJLEdBQUdMLGFBQWEsQ0FBQ3hnQixDQUFDLENBQXNCO01BQ2xELElBQUksQ0FBQzZnQixJQUFJLENBQUM3QixRQUFRLENBQUMsQ0FBQyxFQUFFO1FBQ3BCLE9BQU8sS0FBSztNQUNkO0lBQ0Y7SUFFQSxJQUFJLENBQUV1QixhQUFhLENBQTRCdkIsUUFBUSxDQUFDLENBQUMsRUFBRTtNQUN6RCxPQUFPLEtBQUs7SUFDZDtJQUVBLE1BQU04QixjQUFjLEdBQUlDLFNBQTRCLElBQUs7TUFDdkQsSUFBSXZTLFFBQVEsR0FBRyxDQUFDLENBQUM7TUFDakIsSUFBSSxDQUFDaEssZUFBQyxDQUFDSyxPQUFPLENBQUNrYyxTQUFTLENBQUNDLFNBQVMsQ0FBQyxFQUFFO1FBQ25DeFMsUUFBUSxHQUFHO1VBQ1RNLFNBQVMsRUFBRWlTLFNBQVMsQ0FBQ0M7UUFDdkIsQ0FBQztNQUNIO01BQ0EsT0FBT3hTLFFBQVE7SUFDakIsQ0FBQztJQUNELE1BQU15UyxjQUF3QixHQUFHLEVBQUU7SUFDbkMsSUFBSUMsU0FBUyxHQUFHLENBQUM7SUFDakIsSUFBSUMsVUFBVSxHQUFHLENBQUM7SUFFbEIsTUFBTUMsY0FBYyxHQUFHWixhQUFhLENBQUM5TixHQUFHLENBQUUyTyxPQUFPLElBQy9DLElBQUksQ0FBQy9ULFVBQVUsQ0FBQytULE9BQU8sQ0FBQ25DLE1BQU0sRUFBRW1DLE9BQU8sQ0FBQzdnQixNQUFNLEVBQUVzZ0IsY0FBYyxDQUFDTyxPQUFPLENBQUMsQ0FDekUsQ0FBQztJQUVELE1BQU1DLGNBQWMsR0FBRyxNQUFNMU0sT0FBTyxDQUFDQyxHQUFHLENBQUN1TSxjQUFjLENBQUM7SUFFeEQsTUFBTUcsY0FBYyxHQUFHRCxjQUFjLENBQUM1TyxHQUFHLENBQUMsQ0FBQzhPLFdBQVcsRUFBRUMsS0FBSyxLQUFLO01BQ2hFLE1BQU1WLFNBQXdDLEdBQUdQLGFBQWEsQ0FBQ2lCLEtBQUssQ0FBQztNQUVyRSxJQUFJQyxXQUFXLEdBQUdGLFdBQVcsQ0FBQ3ZULElBQUk7TUFDbEM7TUFDQTtNQUNBLElBQUk4UyxTQUFTLElBQUlBLFNBQVMsQ0FBQ1ksVUFBVSxFQUFFO1FBQ3JDO1FBQ0E7UUFDQTtRQUNBLE1BQU1DLFFBQVEsR0FBR2IsU0FBUyxDQUFDYyxLQUFLO1FBQ2hDLE1BQU1DLE1BQU0sR0FBR2YsU0FBUyxDQUFDZ0IsR0FBRztRQUM1QixJQUFJRCxNQUFNLElBQUlKLFdBQVcsSUFBSUUsUUFBUSxHQUFHLENBQUMsRUFBRTtVQUN6QyxNQUFNLElBQUluakIsTUFBTSxDQUFDMEQsb0JBQW9CLENBQ25DLGtCQUFrQnNmLEtBQUssaUNBQWlDRyxRQUFRLEtBQUtFLE1BQU0sY0FBY0osV0FBVyxHQUN0RyxDQUFDO1FBQ0g7UUFDQUEsV0FBVyxHQUFHSSxNQUFNLEdBQUdGLFFBQVEsR0FBRyxDQUFDO01BQ3JDOztNQUVBO01BQ0EsSUFBSUYsV0FBVyxHQUFHZix3QkFBZ0IsQ0FBQ3FCLGlCQUFpQixJQUFJUCxLQUFLLEdBQUdmLGlCQUFpQixHQUFHLENBQUMsRUFBRTtRQUNyRixNQUFNLElBQUlqaUIsTUFBTSxDQUFDMEQsb0JBQW9CLENBQ25DLGtCQUFrQnNmLEtBQUssa0JBQWtCQyxXQUFXLGdDQUN0RCxDQUFDO01BQ0g7O01BRUE7TUFDQVIsU0FBUyxJQUFJUSxXQUFXO01BQ3hCLElBQUlSLFNBQVMsR0FBR1Asd0JBQWdCLENBQUNzQiw2QkFBNkIsRUFBRTtRQUM5RCxNQUFNLElBQUl4akIsTUFBTSxDQUFDMEQsb0JBQW9CLENBQUMsb0NBQW9DK2UsU0FBUyxXQUFXLENBQUM7TUFDakc7O01BRUE7TUFDQUQsY0FBYyxDQUFDUSxLQUFLLENBQUMsR0FBR0MsV0FBVzs7TUFFbkM7TUFDQVAsVUFBVSxJQUFJLElBQUFlLHFCQUFhLEVBQUNSLFdBQVcsQ0FBQztNQUN4QztNQUNBLElBQUlQLFVBQVUsR0FBR1Isd0JBQWdCLENBQUNDLGVBQWUsRUFBRTtRQUNqRCxNQUFNLElBQUluaUIsTUFBTSxDQUFDMEQsb0JBQW9CLENBQ25DLG1EQUFtRHdlLHdCQUFnQixDQUFDQyxlQUFlLFFBQ3JGLENBQUM7TUFDSDtNQUVBLE9BQU9ZLFdBQVc7SUFDcEIsQ0FBQyxDQUFDO0lBRUYsSUFBS0wsVUFBVSxLQUFLLENBQUMsSUFBSUQsU0FBUyxJQUFJUCx3QkFBZ0IsQ0FBQ3dCLGFBQWEsSUFBS2pCLFNBQVMsS0FBSyxDQUFDLEVBQUU7TUFDeEYsT0FBTyxNQUFNLElBQUksQ0FBQ3RCLFVBQVUsQ0FBQ1ksYUFBYSxDQUFDLENBQUMsQ0FBQyxFQUF1QkQsYUFBYSxDQUFDLEVBQUM7SUFDckY7O0lBRUE7SUFDQSxLQUFLLElBQUl2Z0IsQ0FBQyxHQUFHLENBQUMsRUFBRUEsQ0FBQyxHQUFHMGdCLGlCQUFpQixFQUFFMWdCLENBQUMsRUFBRSxFQUFFO01BQzFDO01BQUV3Z0IsYUFBYSxDQUFDeGdCLENBQUMsQ0FBQyxDQUF1Qm9pQixTQUFTLEdBQUliLGNBQWMsQ0FBQ3ZoQixDQUFDLENBQUMsQ0FBb0J5TixJQUFJO0lBQ2pHO0lBRUEsTUFBTTRVLGlCQUFpQixHQUFHZCxjQUFjLENBQUM3TyxHQUFHLENBQUMsQ0FBQzhPLFdBQVcsRUFBRWMsR0FBRyxLQUFLO01BQ2pFLE9BQU8sSUFBQUMsMkJBQW1CLEVBQUN0QixjQUFjLENBQUNxQixHQUFHLENBQUMsRUFBWTlCLGFBQWEsQ0FBQzhCLEdBQUcsQ0FBc0IsQ0FBQztJQUNwRyxDQUFDLENBQUM7SUFFRixNQUFNRSx1QkFBdUIsR0FBSTdSLFFBQWdCLElBQUs7TUFDcEQsTUFBTThSLG9CQUF3QyxHQUFHLEVBQUU7TUFFbkRKLGlCQUFpQixDQUFDOWEsT0FBTyxDQUFDLENBQUNtYixTQUFTLEVBQUVDLFVBQWtCLEtBQUs7UUFDM0QsSUFBSUQsU0FBUyxFQUFFO1VBQ2IsTUFBTTtZQUFFRSxVQUFVLEVBQUVDLFFBQVE7WUFBRUMsUUFBUSxFQUFFQztVQUFPLENBQUMsR0FBR0wsU0FBUztVQUU1RCxNQUFNTSxTQUFTLEdBQUdMLFVBQVUsR0FBRyxDQUFDLEVBQUM7VUFDakMsTUFBTU0sWUFBWSxHQUFHbkcsS0FBSyxDQUFDdFAsSUFBSSxDQUFDcVYsUUFBUSxDQUFDO1VBRXpDLE1BQU12ZCxPQUFPLEdBQUlrYixhQUFhLENBQUNtQyxVQUFVLENBQUMsQ0FBdUIxRCxVQUFVLENBQUMsQ0FBQztVQUU3RWdFLFlBQVksQ0FBQzFiLE9BQU8sQ0FBQyxDQUFDMmIsVUFBVSxFQUFFQyxVQUFVLEtBQUs7WUFDL0MsTUFBTUMsUUFBUSxHQUFHTCxNQUFNLENBQUNJLFVBQVUsQ0FBQzs7WUFFbkM7WUFDQTtZQUNBO1lBQ0E7WUFDQTdkLE9BQU8sQ0FBQyx5QkFBeUIsQ0FBQyxHQUFHLFNBQVM0ZCxVQUFVLElBQUlFLFFBQVEsRUFBRTtZQUV0RSxNQUFNQyxnQkFBZ0IsR0FBRztjQUN2QjFlLFVBQVUsRUFBRTRiLGFBQWEsQ0FBQ3JCLE1BQU07Y0FDaEN0YSxVQUFVLEVBQUUyYixhQUFhLENBQUMvZixNQUFNO2NBQ2hDMGYsUUFBUSxFQUFFdlAsUUFBUTtjQUNsQnVFLFVBQVUsRUFBRThOLFNBQVM7Y0FDckIxZCxPQUFPLEVBQUVBO1lBQ1gsQ0FBQztZQUVEbWQsb0JBQW9CLENBQUN6VixJQUFJLENBQUNxVyxnQkFBZ0IsQ0FBQztVQUM3QyxDQUFDLENBQUM7UUFDSjtNQUNGLENBQUMsQ0FBQztNQUVGLE9BQU9aLG9CQUFvQjtJQUM3QixDQUFDO0lBRUQsTUFBTWEsY0FBYyxHQUFHLE1BQU9DLFVBQThCLElBQUs7TUFDL0QsTUFBTUMsV0FBMEQsR0FBRyxFQUFFOztNQUVyRTtNQUNBLEtBQUssTUFBTXZHLEtBQUssSUFBSXpZLGVBQUMsQ0FBQzJRLEtBQUssQ0FBQ29PLFVBQVUsRUFBRTlDLGNBQWMsQ0FBQyxFQUFFO1FBQ3ZELE1BQU05QyxZQUFZLEdBQUcsTUFBTS9JLE9BQU8sQ0FBQ0MsR0FBRyxDQUFDb0ksS0FBSyxDQUFDdkssR0FBRyxDQUFFM0IsSUFBSSxJQUFLLElBQUksQ0FBQ2lQLFVBQVUsQ0FBQ2pQLElBQUksQ0FBQyxDQUFDLENBQUM7UUFFbEZ5UyxXQUFXLENBQUN4VyxJQUFJLENBQUMsR0FBRzJRLFlBQVksQ0FBQztNQUNuQzs7TUFFQTtNQUNBLE9BQU82RixXQUFXO0lBQ3BCLENBQUM7SUFFRCxNQUFNQyxrQkFBa0IsR0FBRyxNQUFPOVMsUUFBZ0IsSUFBSztNQUNyRCxNQUFNNFMsVUFBVSxHQUFHZix1QkFBdUIsQ0FBQzdSLFFBQVEsQ0FBQztNQUNwRCxNQUFNK1MsUUFBUSxHQUFHLE1BQU1KLGNBQWMsQ0FBQ0MsVUFBVSxDQUFDO01BQ2pELE9BQU9HLFFBQVEsQ0FBQ2hSLEdBQUcsQ0FBRWlSLFFBQVEsS0FBTTtRQUFFbFcsSUFBSSxFQUFFa1csUUFBUSxDQUFDbFcsSUFBSTtRQUFFbUYsSUFBSSxFQUFFK1EsUUFBUSxDQUFDL1E7TUFBSyxDQUFDLENBQUMsQ0FBQztJQUNuRixDQUFDO0lBRUQsTUFBTWdSLGdCQUFnQixHQUFHckQsYUFBYSxDQUFDdEIsVUFBVSxDQUFDLENBQUM7SUFFbkQsTUFBTXRPLFFBQVEsR0FBRyxNQUFNLElBQUksQ0FBQ2dCLDBCQUEwQixDQUFDNE8sYUFBYSxDQUFDckIsTUFBTSxFQUFFcUIsYUFBYSxDQUFDL2YsTUFBTSxFQUFFb2pCLGdCQUFnQixDQUFDO0lBQ3BILElBQUk7TUFDRixNQUFNQyxTQUFTLEdBQUcsTUFBTUosa0JBQWtCLENBQUM5UyxRQUFRLENBQUM7TUFDcEQsT0FBTyxNQUFNLElBQUksQ0FBQzBCLHVCQUF1QixDQUFDa08sYUFBYSxDQUFDckIsTUFBTSxFQUFFcUIsYUFBYSxDQUFDL2YsTUFBTSxFQUFFbVEsUUFBUSxFQUFFa1QsU0FBUyxDQUFDO0lBQzVHLENBQUMsQ0FBQyxPQUFPMWMsR0FBRyxFQUFFO01BQ1osT0FBTyxNQUFNLElBQUksQ0FBQzJLLG9CQUFvQixDQUFDeU8sYUFBYSxDQUFDckIsTUFBTSxFQUFFcUIsYUFBYSxDQUFDL2YsTUFBTSxFQUFFbVEsUUFBUSxDQUFDO0lBQzlGO0VBQ0Y7RUFFQSxNQUFNbVQsWUFBWUEsQ0FDaEJ6ZSxNQUFjLEVBQ2RWLFVBQWtCLEVBQ2xCQyxVQUFrQixFQUNsQm1mLE9BQW1ELEVBQ25EQyxTQUF1QyxFQUN2Q0MsV0FBa0IsRUFDRDtJQUFBLElBQUFDLFlBQUE7SUFDakIsSUFBSSxJQUFJLENBQUMxZ0IsU0FBUyxFQUFFO01BQ2xCLE1BQU0sSUFBSS9FLE1BQU0sQ0FBQzBsQixxQkFBcUIsQ0FBQyxhQUFhOWUsTUFBTSxpREFBaUQsQ0FBQztJQUM5RztJQUVBLElBQUksQ0FBQzBlLE9BQU8sRUFBRTtNQUNaQSxPQUFPLEdBQUdLLGdDQUF1QjtJQUNuQztJQUNBLElBQUksQ0FBQ0osU0FBUyxFQUFFO01BQ2RBLFNBQVMsR0FBRyxDQUFDLENBQUM7SUFDaEI7SUFDQSxJQUFJLENBQUNDLFdBQVcsRUFBRTtNQUNoQkEsV0FBVyxHQUFHLElBQUk3YSxJQUFJLENBQUMsQ0FBQztJQUMxQjs7SUFFQTtJQUNBLElBQUkyYSxPQUFPLElBQUksT0FBT0EsT0FBTyxLQUFLLFFBQVEsRUFBRTtNQUMxQyxNQUFNLElBQUl4ZixTQUFTLENBQUMsb0NBQW9DLENBQUM7SUFDM0Q7SUFDQSxJQUFJeWYsU0FBUyxJQUFJLE9BQU9BLFNBQVMsS0FBSyxRQUFRLEVBQUU7TUFDOUMsTUFBTSxJQUFJemYsU0FBUyxDQUFDLHNDQUFzQyxDQUFDO0lBQzdEO0lBQ0EsSUFBSzBmLFdBQVcsSUFBSSxFQUFFQSxXQUFXLFlBQVk3YSxJQUFJLENBQUMsSUFBTTZhLFdBQVcsSUFBSUksS0FBSyxFQUFBSCxZQUFBLEdBQUNELFdBQVcsY0FBQUMsWUFBQSx1QkFBWEEsWUFBQSxDQUFhOVIsT0FBTyxDQUFDLENBQUMsQ0FBRSxFQUFFO01BQ3JHLE1BQU0sSUFBSTdOLFNBQVMsQ0FBQyxnREFBZ0QsQ0FBQztJQUN2RTtJQUVBLE1BQU1nQixLQUFLLEdBQUd5ZSxTQUFTLEdBQUduWCxvQkFBRSxDQUFDOUUsU0FBUyxDQUFDaWMsU0FBUyxDQUFDLEdBQUdyaUIsU0FBUztJQUU3RCxJQUFJO01BQ0YsTUFBTVUsTUFBTSxHQUFHLE1BQU0sSUFBSSxDQUFDNkcsb0JBQW9CLENBQUN2RSxVQUFVLENBQUM7TUFDMUQsTUFBTSxJQUFJLENBQUMrQixvQkFBb0IsQ0FBQyxDQUFDO01BQ2pDLE1BQU01QyxVQUFVLEdBQUcsSUFBSSxDQUFDcUIsaUJBQWlCLENBQUM7UUFBRUUsTUFBTTtRQUFFaEQsTUFBTTtRQUFFc0MsVUFBVTtRQUFFQyxVQUFVO1FBQUVXO01BQU0sQ0FBQyxDQUFDO01BRTVGLE9BQU8sSUFBQStlLDJCQUFrQixFQUN2QnhnQixVQUFVLEVBQ1YsSUFBSSxDQUFDVCxTQUFTLEVBQ2QsSUFBSSxDQUFDQyxTQUFTLEVBQ2QsSUFBSSxDQUFDQyxZQUFZLEVBQ2pCbEIsTUFBTSxFQUNONGhCLFdBQVcsRUFDWEYsT0FDRixDQUFDO0lBQ0gsQ0FBQyxDQUFDLE9BQU81YyxHQUFHLEVBQUU7TUFDWixJQUFJQSxHQUFHLFlBQVkxSSxNQUFNLENBQUN1TCxzQkFBc0IsRUFBRTtRQUNoRCxNQUFNLElBQUl2TCxNQUFNLENBQUMwRCxvQkFBb0IsQ0FBQyxtQ0FBbUN3QyxVQUFVLEdBQUcsQ0FBQztNQUN6RjtNQUVBLE1BQU13QyxHQUFHO0lBQ1g7RUFDRjtFQUVBLE1BQU1vZCxrQkFBa0JBLENBQ3RCNWYsVUFBa0IsRUFDbEJDLFVBQWtCLEVBQ2xCbWYsT0FBZ0IsRUFDaEJTLFdBQXlDLEVBQ3pDUCxXQUFrQixFQUNEO0lBQ2pCLElBQUksQ0FBQyxJQUFBbGEseUJBQWlCLEVBQUNwRixVQUFVLENBQUMsRUFBRTtNQUNsQyxNQUFNLElBQUlsRyxNQUFNLENBQUN1TCxzQkFBc0IsQ0FBQyx1QkFBdUIsR0FBR3JGLFVBQVUsQ0FBQztJQUMvRTtJQUNBLElBQUksQ0FBQyxJQUFBeUgseUJBQWlCLEVBQUN4SCxVQUFVLENBQUMsRUFBRTtNQUNsQyxNQUFNLElBQUluRyxNQUFNLENBQUM0TixzQkFBc0IsQ0FBQyx3QkFBd0J6SCxVQUFVLEVBQUUsQ0FBQztJQUMvRTtJQUVBLE1BQU02ZixnQkFBZ0IsR0FBRyxDQUN2Qix1QkFBdUIsRUFDdkIsMkJBQTJCLEVBQzNCLGtCQUFrQixFQUNsQix3QkFBd0IsRUFDeEIsOEJBQThCLEVBQzlCLDJCQUEyQixDQUM1QjtJQUNEQSxnQkFBZ0IsQ0FBQ2xkLE9BQU8sQ0FBRW1kLE1BQU0sSUFBSztNQUNuQztNQUNBLElBQUlGLFdBQVcsS0FBSzdpQixTQUFTLElBQUk2aUIsV0FBVyxDQUFDRSxNQUFNLENBQUMsS0FBSy9pQixTQUFTLElBQUksQ0FBQyxJQUFBVyxnQkFBUSxFQUFDa2lCLFdBQVcsQ0FBQ0UsTUFBTSxDQUFDLENBQUMsRUFBRTtRQUNwRyxNQUFNLElBQUluZ0IsU0FBUyxDQUFDLG1CQUFtQm1nQixNQUFNLDZCQUE2QixDQUFDO01BQzdFO0lBQ0YsQ0FBQyxDQUFDO0lBQ0YsT0FBTyxJQUFJLENBQUNaLFlBQVksQ0FBQyxLQUFLLEVBQUVuZixVQUFVLEVBQUVDLFVBQVUsRUFBRW1mLE9BQU8sRUFBRVMsV0FBVyxFQUFFUCxXQUFXLENBQUM7RUFDNUY7RUFFQSxNQUFNVSxrQkFBa0JBLENBQUNoZ0IsVUFBa0IsRUFBRUMsVUFBa0IsRUFBRW1mLE9BQWdCLEVBQW1CO0lBQ2xHLElBQUksQ0FBQyxJQUFBaGEseUJBQWlCLEVBQUNwRixVQUFVLENBQUMsRUFBRTtNQUNsQyxNQUFNLElBQUlsRyxNQUFNLENBQUN1TCxzQkFBc0IsQ0FBQyx3QkFBd0JyRixVQUFVLEVBQUUsQ0FBQztJQUMvRTtJQUNBLElBQUksQ0FBQyxJQUFBeUgseUJBQWlCLEVBQUN4SCxVQUFVLENBQUMsRUFBRTtNQUNsQyxNQUFNLElBQUluRyxNQUFNLENBQUM0TixzQkFBc0IsQ0FBQyx3QkFBd0J6SCxVQUFVLEVBQUUsQ0FBQztJQUMvRTtJQUVBLE9BQU8sSUFBSSxDQUFDa2YsWUFBWSxDQUFDLEtBQUssRUFBRW5mLFVBQVUsRUFBRUMsVUFBVSxFQUFFbWYsT0FBTyxDQUFDO0VBQ2xFO0VBRUFhLGFBQWFBLENBQUEsRUFBZTtJQUMxQixPQUFPLElBQUlDLHNCQUFVLENBQUMsQ0FBQztFQUN6QjtFQUVBLE1BQU1DLG1CQUFtQkEsQ0FBQ0MsVUFBc0IsRUFBNkI7SUFDM0UsSUFBSSxJQUFJLENBQUN2aEIsU0FBUyxFQUFFO01BQ2xCLE1BQU0sSUFBSS9FLE1BQU0sQ0FBQzBsQixxQkFBcUIsQ0FBQyxrRUFBa0UsQ0FBQztJQUM1RztJQUNBLElBQUksQ0FBQyxJQUFBdGhCLGdCQUFRLEVBQUNraUIsVUFBVSxDQUFDLEVBQUU7TUFDekIsTUFBTSxJQUFJeGdCLFNBQVMsQ0FBQyx1Q0FBdUMsQ0FBQztJQUM5RDtJQUNBLE1BQU1JLFVBQVUsR0FBR29nQixVQUFVLENBQUNDLFFBQVEsQ0FBQ3pWLE1BQWdCO0lBQ3ZELElBQUk7TUFDRixNQUFNbE4sTUFBTSxHQUFHLE1BQU0sSUFBSSxDQUFDNkcsb0JBQW9CLENBQUN2RSxVQUFVLENBQUM7TUFFMUQsTUFBTXdFLElBQUksR0FBRyxJQUFJQyxJQUFJLENBQUMsQ0FBQztNQUN2QixNQUFNNmIsT0FBTyxHQUFHLElBQUE1YixvQkFBWSxFQUFDRixJQUFJLENBQUM7TUFDbEMsTUFBTSxJQUFJLENBQUN6QyxvQkFBb0IsQ0FBQyxDQUFDO01BRWpDLElBQUksQ0FBQ3FlLFVBQVUsQ0FBQ3pOLE1BQU0sQ0FBQzROLFVBQVUsRUFBRTtRQUNqQztRQUNBO1FBQ0EsTUFBTW5CLE9BQU8sR0FBRyxJQUFJM2EsSUFBSSxDQUFDLENBQUM7UUFDMUIyYSxPQUFPLENBQUNvQixVQUFVLENBQUNmLGdDQUF1QixDQUFDO1FBQzNDVyxVQUFVLENBQUNLLFVBQVUsQ0FBQ3JCLE9BQU8sQ0FBQztNQUNoQztNQUVBZ0IsVUFBVSxDQUFDek4sTUFBTSxDQUFDOEcsVUFBVSxDQUFDcFIsSUFBSSxDQUFDLENBQUMsSUFBSSxFQUFFLGFBQWEsRUFBRWlZLE9BQU8sQ0FBQyxDQUFDO01BQ2pFRixVQUFVLENBQUNDLFFBQVEsQ0FBQyxZQUFZLENBQUMsR0FBR0MsT0FBTztNQUUzQ0YsVUFBVSxDQUFDek4sTUFBTSxDQUFDOEcsVUFBVSxDQUFDcFIsSUFBSSxDQUFDLENBQUMsSUFBSSxFQUFFLGtCQUFrQixFQUFFLGtCQUFrQixDQUFDLENBQUM7TUFDakYrWCxVQUFVLENBQUNDLFFBQVEsQ0FBQyxpQkFBaUIsQ0FBQyxHQUFHLGtCQUFrQjtNQUUzREQsVUFBVSxDQUFDek4sTUFBTSxDQUFDOEcsVUFBVSxDQUFDcFIsSUFBSSxDQUFDLENBQUMsSUFBSSxFQUFFLG1CQUFtQixFQUFFLElBQUksQ0FBQzNKLFNBQVMsR0FBRyxHQUFHLEdBQUcsSUFBQWdpQixnQkFBUSxFQUFDaGpCLE1BQU0sRUFBRThHLElBQUksQ0FBQyxDQUFDLENBQUM7TUFDN0c0YixVQUFVLENBQUNDLFFBQVEsQ0FBQyxrQkFBa0IsQ0FBQyxHQUFHLElBQUksQ0FBQzNoQixTQUFTLEdBQUcsR0FBRyxHQUFHLElBQUFnaUIsZ0JBQVEsRUFBQ2hqQixNQUFNLEVBQUU4RyxJQUFJLENBQUM7TUFFdkYsSUFBSSxJQUFJLENBQUM1RixZQUFZLEVBQUU7UUFDckJ3aEIsVUFBVSxDQUFDek4sTUFBTSxDQUFDOEcsVUFBVSxDQUFDcFIsSUFBSSxDQUFDLENBQUMsSUFBSSxFQUFFLHVCQUF1QixFQUFFLElBQUksQ0FBQ3pKLFlBQVksQ0FBQyxDQUFDO1FBQ3JGd2hCLFVBQVUsQ0FBQ0MsUUFBUSxDQUFDLHNCQUFzQixDQUFDLEdBQUcsSUFBSSxDQUFDemhCLFlBQVk7TUFDakU7TUFFQSxNQUFNK2hCLFlBQVksR0FBR3RjLE1BQU0sQ0FBQ3dFLElBQUksQ0FBQzFGLElBQUksQ0FBQ0MsU0FBUyxDQUFDZ2QsVUFBVSxDQUFDek4sTUFBTSxDQUFDLENBQUMsQ0FBQy9RLFFBQVEsQ0FBQyxRQUFRLENBQUM7TUFFdEZ3ZSxVQUFVLENBQUNDLFFBQVEsQ0FBQzFOLE1BQU0sR0FBR2dPLFlBQVk7TUFFekNQLFVBQVUsQ0FBQ0MsUUFBUSxDQUFDLGlCQUFpQixDQUFDLEdBQUcsSUFBQU8sK0JBQXNCLEVBQUNsakIsTUFBTSxFQUFFOEcsSUFBSSxFQUFFLElBQUksQ0FBQzdGLFNBQVMsRUFBRWdpQixZQUFZLENBQUM7TUFDM0csTUFBTWxnQixJQUFJLEdBQUc7UUFDWC9DLE1BQU0sRUFBRUEsTUFBTTtRQUNkc0MsVUFBVSxFQUFFQSxVQUFVO1FBQ3RCVSxNQUFNLEVBQUU7TUFDVixDQUFDO01BQ0QsTUFBTXZCLFVBQVUsR0FBRyxJQUFJLENBQUNxQixpQkFBaUIsQ0FBQ0MsSUFBSSxDQUFDO01BQy9DLE1BQU1vZ0IsT0FBTyxHQUFHLElBQUksQ0FBQzFqQixJQUFJLElBQUksRUFBRSxJQUFJLElBQUksQ0FBQ0EsSUFBSSxLQUFLLEdBQUcsR0FBRyxFQUFFLEdBQUcsSUFBSSxJQUFJLENBQUNBLElBQUksQ0FBQ3lFLFFBQVEsQ0FBQyxDQUFDLEVBQUU7TUFDdEYsTUFBTWtmLE1BQU0sR0FBRyxHQUFHM2hCLFVBQVUsQ0FBQ3JCLFFBQVEsS0FBS3FCLFVBQVUsQ0FBQ3ZCLElBQUksR0FBR2lqQixPQUFPLEdBQUcxaEIsVUFBVSxDQUFDL0YsSUFBSSxFQUFFO01BQ3ZGLE9BQU87UUFBRTJuQixPQUFPLEVBQUVELE1BQU07UUFBRVQsUUFBUSxFQUFFRCxVQUFVLENBQUNDO01BQVMsQ0FBQztJQUMzRCxDQUFDLENBQUMsT0FBTzdkLEdBQUcsRUFBRTtNQUNaLElBQUlBLEdBQUcsWUFBWTFJLE1BQU0sQ0FBQ3VMLHNCQUFzQixFQUFFO1FBQ2hELE1BQU0sSUFBSXZMLE1BQU0sQ0FBQzBELG9CQUFvQixDQUFDLG1DQUFtQ3dDLFVBQVUsR0FBRyxDQUFDO01BQ3pGO01BRUEsTUFBTXdDLEdBQUc7SUFDWDtFQUNGO0VBQ0E7RUFDQSxNQUFNd2UsZ0JBQWdCQSxDQUFDaGhCLFVBQWtCLEVBQUU2SyxNQUFlLEVBQUV3RCxNQUFlLEVBQUU0UyxhQUFtQyxFQUFFO0lBQ2hILElBQUksQ0FBQyxJQUFBN2IseUJBQWlCLEVBQUNwRixVQUFVLENBQUMsRUFBRTtNQUNsQyxNQUFNLElBQUlsRyxNQUFNLENBQUN1TCxzQkFBc0IsQ0FBQyx1QkFBdUIsR0FBR3JGLFVBQVUsQ0FBQztJQUMvRTtJQUNBLElBQUksQ0FBQyxJQUFBckMsZ0JBQVEsRUFBQ2tOLE1BQU0sQ0FBQyxFQUFFO01BQ3JCLE1BQU0sSUFBSWpMLFNBQVMsQ0FBQyxtQ0FBbUMsQ0FBQztJQUMxRDtJQUNBLElBQUl5TyxNQUFNLElBQUksQ0FBQyxJQUFBMVEsZ0JBQVEsRUFBQzBRLE1BQU0sQ0FBQyxFQUFFO01BQy9CLE1BQU0sSUFBSXpPLFNBQVMsQ0FBQyxtQ0FBbUMsQ0FBQztJQUMxRDtJQUVBLElBQUlxaEIsYUFBYSxJQUFJLENBQUMsSUFBQS9pQixnQkFBUSxFQUFDK2lCLGFBQWEsQ0FBQyxFQUFFO01BQzdDLE1BQU0sSUFBSXJoQixTQUFTLENBQUMsMENBQTBDLENBQUM7SUFDakU7SUFDQSxJQUFJO01BQUVzaEIsU0FBUztNQUFFQyxPQUFPO01BQUVDLGNBQWM7TUFBRUMsZUFBZTtNQUFFcFc7SUFBVSxDQUFDLEdBQUdnVyxhQUFvQztJQUU3RyxJQUFJLENBQUMsSUFBQXRqQixnQkFBUSxFQUFDdWpCLFNBQVMsQ0FBQyxFQUFFO01BQ3hCLE1BQU0sSUFBSXRoQixTQUFTLENBQUMsc0NBQXNDLENBQUM7SUFDN0Q7SUFDQSxJQUFJLENBQUMsSUFBQStELGdCQUFRLEVBQUN3ZCxPQUFPLENBQUMsRUFBRTtNQUN0QixNQUFNLElBQUl2aEIsU0FBUyxDQUFDLG9DQUFvQyxDQUFDO0lBQzNEO0lBRUEsTUFBTTZNLE9BQU8sR0FBRyxFQUFFO0lBQ2xCO0lBQ0FBLE9BQU8sQ0FBQ3BFLElBQUksQ0FBQyxVQUFVLElBQUFxRSxpQkFBUyxFQUFDN0IsTUFBTSxDQUFDLEVBQUUsQ0FBQztJQUMzQzRCLE9BQU8sQ0FBQ3BFLElBQUksQ0FBQyxhQUFhLElBQUFxRSxpQkFBUyxFQUFDd1UsU0FBUyxDQUFDLEVBQUUsQ0FBQztJQUNqRHpVLE9BQU8sQ0FBQ3BFLElBQUksQ0FBQyxtQkFBbUIsQ0FBQztJQUVqQyxJQUFJK1ksY0FBYyxFQUFFO01BQ2xCM1UsT0FBTyxDQUFDcEUsSUFBSSxDQUFDLFVBQVUsQ0FBQztJQUMxQjtJQUVBLElBQUkrWSxjQUFjLEVBQUU7TUFDbEI7TUFDQSxJQUFJblcsU0FBUyxFQUFFO1FBQ2J3QixPQUFPLENBQUNwRSxJQUFJLENBQUMsY0FBYzRDLFNBQVMsRUFBRSxDQUFDO01BQ3pDO01BQ0EsSUFBSW9XLGVBQWUsRUFBRTtRQUNuQjVVLE9BQU8sQ0FBQ3BFLElBQUksQ0FBQyxxQkFBcUJnWixlQUFlLEVBQUUsQ0FBQztNQUN0RDtJQUNGLENBQUMsTUFBTSxJQUFJaFQsTUFBTSxFQUFFO01BQ2pCQSxNQUFNLEdBQUcsSUFBQTNCLGlCQUFTLEVBQUMyQixNQUFNLENBQUM7TUFDMUI1QixPQUFPLENBQUNwRSxJQUFJLENBQUMsVUFBVWdHLE1BQU0sRUFBRSxDQUFDO0lBQ2xDOztJQUVBO0lBQ0EsSUFBSThTLE9BQU8sRUFBRTtNQUNYLElBQUlBLE9BQU8sSUFBSSxJQUFJLEVBQUU7UUFDbkJBLE9BQU8sR0FBRyxJQUFJO01BQ2hCO01BQ0ExVSxPQUFPLENBQUNwRSxJQUFJLENBQUMsWUFBWThZLE9BQU8sRUFBRSxDQUFDO0lBQ3JDO0lBQ0ExVSxPQUFPLENBQUNHLElBQUksQ0FBQyxDQUFDO0lBQ2QsSUFBSWhNLEtBQUssR0FBRyxFQUFFO0lBQ2QsSUFBSTZMLE9BQU8sQ0FBQzdJLE1BQU0sR0FBRyxDQUFDLEVBQUU7TUFDdEJoRCxLQUFLLEdBQUcsR0FBRzZMLE9BQU8sQ0FBQ0ssSUFBSSxDQUFDLEdBQUcsQ0FBQyxFQUFFO0lBQ2hDO0lBRUEsTUFBTXBNLE1BQU0sR0FBRyxLQUFLO0lBQ3BCLE1BQU13RCxHQUFHLEdBQUcsTUFBTSxJQUFJLENBQUNWLGdCQUFnQixDQUFDO01BQUU5QyxNQUFNO01BQUVWLFVBQVU7TUFBRVk7SUFBTSxDQUFDLENBQUM7SUFDdEUsTUFBTXdELElBQUksR0FBRyxNQUFNLElBQUFvQixzQkFBWSxFQUFDdEIsR0FBRyxDQUFDO0lBQ3BDLE1BQU1vZCxXQUFXLEdBQUcsSUFBQUMsMkJBQWdCLEVBQUNuZCxJQUFJLENBQUM7SUFDMUMsT0FBT2tkLFdBQVc7RUFDcEI7RUFFQUUsV0FBV0EsQ0FDVHhoQixVQUFrQixFQUNsQjZLLE1BQWUsRUFDZjFCLFNBQW1CLEVBQ25Cc1ksUUFBMEMsRUFDaEI7SUFDMUIsSUFBSTVXLE1BQU0sS0FBSzdOLFNBQVMsRUFBRTtNQUN4QjZOLE1BQU0sR0FBRyxFQUFFO0lBQ2I7SUFDQSxJQUFJMUIsU0FBUyxLQUFLbk0sU0FBUyxFQUFFO01BQzNCbU0sU0FBUyxHQUFHLEtBQUs7SUFDbkI7SUFDQSxJQUFJLENBQUMsSUFBQS9ELHlCQUFpQixFQUFDcEYsVUFBVSxDQUFDLEVBQUU7TUFDbEMsTUFBTSxJQUFJbEcsTUFBTSxDQUFDdUwsc0JBQXNCLENBQUMsdUJBQXVCLEdBQUdyRixVQUFVLENBQUM7SUFDL0U7SUFDQSxJQUFJLENBQUMsSUFBQThLLHFCQUFhLEVBQUNELE1BQU0sQ0FBQyxFQUFFO01BQzFCLE1BQU0sSUFBSS9RLE1BQU0sQ0FBQ2lSLGtCQUFrQixDQUFDLG9CQUFvQkYsTUFBTSxFQUFFLENBQUM7SUFDbkU7SUFDQSxJQUFJLENBQUMsSUFBQWxOLGdCQUFRLEVBQUNrTixNQUFNLENBQUMsRUFBRTtNQUNyQixNQUFNLElBQUlqTCxTQUFTLENBQUMsbUNBQW1DLENBQUM7SUFDMUQ7SUFDQSxJQUFJLENBQUMsSUFBQW5DLGlCQUFTLEVBQUMwTCxTQUFTLENBQUMsRUFBRTtNQUN6QixNQUFNLElBQUl2SixTQUFTLENBQUMsdUNBQXVDLENBQUM7SUFDOUQ7SUFDQSxJQUFJNmhCLFFBQVEsSUFBSSxDQUFDLElBQUF2akIsZ0JBQVEsRUFBQ3VqQixRQUFRLENBQUMsRUFBRTtNQUNuQyxNQUFNLElBQUk3aEIsU0FBUyxDQUFDLHFDQUFxQyxDQUFDO0lBQzVEO0lBQ0EsSUFBSXlPLE1BQTBCLEdBQUcsRUFBRTtJQUNuQyxJQUFJcEQsU0FBNkIsR0FBRyxFQUFFO0lBQ3RDLElBQUlvVyxlQUFtQyxHQUFHLEVBQUU7SUFDNUMsSUFBSUssT0FBcUIsR0FBRyxFQUFFO0lBQzlCLElBQUl0VyxLQUFLLEdBQUcsS0FBSztJQUNqQixNQUFNQyxVQUEyQixHQUFHLElBQUloUyxNQUFNLENBQUNpUyxRQUFRLENBQUM7TUFBRUMsVUFBVSxFQUFFO0lBQUssQ0FBQyxDQUFDO0lBQzdFRixVQUFVLENBQUNHLEtBQUssR0FBRyxZQUFZO01BQzdCO01BQ0EsSUFBSWtXLE9BQU8sQ0FBQzlkLE1BQU0sRUFBRTtRQUNsQnlILFVBQVUsQ0FBQ2hELElBQUksQ0FBQ3FaLE9BQU8sQ0FBQ2pXLEtBQUssQ0FBQyxDQUFDLENBQUM7UUFDaEM7TUFDRjtNQUNBLElBQUlMLEtBQUssRUFBRTtRQUNULE9BQU9DLFVBQVUsQ0FBQ2hELElBQUksQ0FBQyxJQUFJLENBQUM7TUFDOUI7TUFFQSxJQUFJO1FBQ0YsTUFBTTRZLGFBQWEsR0FBRztVQUNwQkMsU0FBUyxFQUFFL1gsU0FBUyxHQUFHLEVBQUUsR0FBRyxHQUFHO1VBQUU7VUFDakNnWSxPQUFPLEVBQUUsSUFBSTtVQUNiQyxjQUFjLEVBQUVLLFFBQVEsYUFBUkEsUUFBUSx1QkFBUkEsUUFBUSxDQUFFTCxjQUFjO1VBQ3hDO1VBQ0FuVyxTQUFTLEVBQUVBLFNBQVM7VUFDcEJvVyxlQUFlLEVBQUVBO1FBQ25CLENBQUM7UUFFRCxNQUFNOWEsTUFBMEIsR0FBRyxNQUFNLElBQUksQ0FBQ3lhLGdCQUFnQixDQUFDaGhCLFVBQVUsRUFBRTZLLE1BQU0sRUFBRXdELE1BQU0sRUFBRTRTLGFBQWEsQ0FBQztRQUN6RyxJQUFJMWEsTUFBTSxDQUFDK0YsV0FBVyxFQUFFO1VBQ3RCK0IsTUFBTSxHQUFHOUgsTUFBTSxDQUFDb2IsVUFBVSxJQUFJM2tCLFNBQVM7VUFDdkMsSUFBSXVKLE1BQU0sQ0FBQzBFLFNBQVMsRUFBRTtZQUNwQkEsU0FBUyxHQUFHMUUsTUFBTSxDQUFDMEUsU0FBUztVQUM5QjtVQUNBLElBQUkxRSxNQUFNLENBQUM4YSxlQUFlLEVBQUU7WUFDMUJBLGVBQWUsR0FBRzlhLE1BQU0sQ0FBQzhhLGVBQWU7VUFDMUM7UUFDRixDQUFDLE1BQU07VUFDTGpXLEtBQUssR0FBRyxJQUFJO1FBQ2Q7UUFDQSxJQUFJN0UsTUFBTSxDQUFDbWIsT0FBTyxFQUFFO1VBQ2xCQSxPQUFPLEdBQUduYixNQUFNLENBQUNtYixPQUFPO1FBQzFCO1FBQ0E7UUFDQXJXLFVBQVUsQ0FBQ0csS0FBSyxDQUFDLENBQUM7TUFDcEIsQ0FBQyxDQUFDLE9BQU9oSixHQUFHLEVBQUU7UUFDWjZJLFVBQVUsQ0FBQ2dCLElBQUksQ0FBQyxPQUFPLEVBQUU3SixHQUFHLENBQUM7TUFDL0I7SUFDRixDQUFDO0lBQ0QsT0FBTzZJLFVBQVU7RUFDbkI7RUFFQSxNQUFNdVcsa0JBQWtCQSxDQUN0QjVoQixVQUFrQixFQUNsQjZLLE1BQWMsRUFDZGdYLGlCQUF5QixFQUN6QjdXLFNBQWlCLEVBQ2pCOFcsT0FBZSxFQUNmQyxVQUFrQixFQUNRO0lBQzFCLElBQUksQ0FBQyxJQUFBM2MseUJBQWlCLEVBQUNwRixVQUFVLENBQUMsRUFBRTtNQUNsQyxNQUFNLElBQUlsRyxNQUFNLENBQUN1TCxzQkFBc0IsQ0FBQyx1QkFBdUIsR0FBR3JGLFVBQVUsQ0FBQztJQUMvRTtJQUNBLElBQUksQ0FBQyxJQUFBckMsZ0JBQVEsRUFBQ2tOLE1BQU0sQ0FBQyxFQUFFO01BQ3JCLE1BQU0sSUFBSWpMLFNBQVMsQ0FBQyxtQ0FBbUMsQ0FBQztJQUMxRDtJQUNBLElBQUksQ0FBQyxJQUFBakMsZ0JBQVEsRUFBQ2trQixpQkFBaUIsQ0FBQyxFQUFFO01BQ2hDLE1BQU0sSUFBSWppQixTQUFTLENBQUMsOENBQThDLENBQUM7SUFDckU7SUFDQSxJQUFJLENBQUMsSUFBQWpDLGdCQUFRLEVBQUNxTixTQUFTLENBQUMsRUFBRTtNQUN4QixNQUFNLElBQUlwTCxTQUFTLENBQUMsc0NBQXNDLENBQUM7SUFDN0Q7SUFDQSxJQUFJLENBQUMsSUFBQStELGdCQUFRLEVBQUNtZSxPQUFPLENBQUMsRUFBRTtNQUN0QixNQUFNLElBQUlsaUIsU0FBUyxDQUFDLG9DQUFvQyxDQUFDO0lBQzNEO0lBQ0EsSUFBSSxDQUFDLElBQUFqQyxnQkFBUSxFQUFDb2tCLFVBQVUsQ0FBQyxFQUFFO01BQ3pCLE1BQU0sSUFBSW5pQixTQUFTLENBQUMsdUNBQXVDLENBQUM7SUFDOUQ7SUFFQSxNQUFNNk0sT0FBTyxHQUFHLEVBQUU7SUFDbEJBLE9BQU8sQ0FBQ3BFLElBQUksQ0FBQyxhQUFhLENBQUM7SUFDM0JvRSxPQUFPLENBQUNwRSxJQUFJLENBQUMsbUJBQW1CLENBQUM7SUFDakNvRSxPQUFPLENBQUNwRSxJQUFJLENBQUMsVUFBVSxJQUFBcUUsaUJBQVMsRUFBQzdCLE1BQU0sQ0FBQyxFQUFFLENBQUM7SUFDM0M0QixPQUFPLENBQUNwRSxJQUFJLENBQUMsYUFBYSxJQUFBcUUsaUJBQVMsRUFBQzFCLFNBQVMsQ0FBQyxFQUFFLENBQUM7SUFFakQsSUFBSTZXLGlCQUFpQixFQUFFO01BQ3JCcFYsT0FBTyxDQUFDcEUsSUFBSSxDQUFDLHNCQUFzQixJQUFBcUUsaUJBQVMsRUFBQ21WLGlCQUFpQixDQUFDLEVBQUUsQ0FBQztJQUNwRTtJQUNBLElBQUlFLFVBQVUsRUFBRTtNQUNkdFYsT0FBTyxDQUFDcEUsSUFBSSxDQUFDLGVBQWUsSUFBQXFFLGlCQUFTLEVBQUNxVixVQUFVLENBQUMsRUFBRSxDQUFDO0lBQ3REO0lBQ0EsSUFBSUQsT0FBTyxFQUFFO01BQ1gsSUFBSUEsT0FBTyxJQUFJLElBQUksRUFBRTtRQUNuQkEsT0FBTyxHQUFHLElBQUk7TUFDaEI7TUFDQXJWLE9BQU8sQ0FBQ3BFLElBQUksQ0FBQyxZQUFZeVosT0FBTyxFQUFFLENBQUM7SUFDckM7SUFDQXJWLE9BQU8sQ0FBQ0csSUFBSSxDQUFDLENBQUM7SUFDZCxJQUFJaE0sS0FBSyxHQUFHLEVBQUU7SUFDZCxJQUFJNkwsT0FBTyxDQUFDN0ksTUFBTSxHQUFHLENBQUMsRUFBRTtNQUN0QmhELEtBQUssR0FBRyxHQUFHNkwsT0FBTyxDQUFDSyxJQUFJLENBQUMsR0FBRyxDQUFDLEVBQUU7SUFDaEM7SUFFQSxNQUFNcE0sTUFBTSxHQUFHLEtBQUs7SUFDcEIsTUFBTXdELEdBQUcsR0FBRyxNQUFNLElBQUksQ0FBQ1YsZ0JBQWdCLENBQUM7TUFBRTlDLE1BQU07TUFBRVYsVUFBVTtNQUFFWTtJQUFNLENBQUMsQ0FBQztJQUN0RSxNQUFNd0QsSUFBSSxHQUFHLE1BQU0sSUFBQW9CLHNCQUFZLEVBQUN0QixHQUFHLENBQUM7SUFDcEMsT0FBTyxJQUFBOGQsNkJBQWtCLEVBQUM1ZCxJQUFJLENBQUM7RUFDakM7RUFFQTZkLGFBQWFBLENBQ1hqaUIsVUFBa0IsRUFDbEI2SyxNQUFlLEVBQ2YxQixTQUFtQixFQUNuQjRZLFVBQW1CLEVBQ087SUFDMUIsSUFBSWxYLE1BQU0sS0FBSzdOLFNBQVMsRUFBRTtNQUN4QjZOLE1BQU0sR0FBRyxFQUFFO0lBQ2I7SUFDQSxJQUFJMUIsU0FBUyxLQUFLbk0sU0FBUyxFQUFFO01BQzNCbU0sU0FBUyxHQUFHLEtBQUs7SUFDbkI7SUFDQSxJQUFJNFksVUFBVSxLQUFLL2tCLFNBQVMsRUFBRTtNQUM1QitrQixVQUFVLEdBQUcsRUFBRTtJQUNqQjtJQUNBLElBQUksQ0FBQyxJQUFBM2MseUJBQWlCLEVBQUNwRixVQUFVLENBQUMsRUFBRTtNQUNsQyxNQUFNLElBQUlsRyxNQUFNLENBQUN1TCxzQkFBc0IsQ0FBQyx1QkFBdUIsR0FBR3JGLFVBQVUsQ0FBQztJQUMvRTtJQUNBLElBQUksQ0FBQyxJQUFBOEsscUJBQWEsRUFBQ0QsTUFBTSxDQUFDLEVBQUU7TUFDMUIsTUFBTSxJQUFJL1EsTUFBTSxDQUFDaVIsa0JBQWtCLENBQUMsb0JBQW9CRixNQUFNLEVBQUUsQ0FBQztJQUNuRTtJQUNBLElBQUksQ0FBQyxJQUFBbE4sZ0JBQVEsRUFBQ2tOLE1BQU0sQ0FBQyxFQUFFO01BQ3JCLE1BQU0sSUFBSWpMLFNBQVMsQ0FBQyxtQ0FBbUMsQ0FBQztJQUMxRDtJQUNBLElBQUksQ0FBQyxJQUFBbkMsaUJBQVMsRUFBQzBMLFNBQVMsQ0FBQyxFQUFFO01BQ3pCLE1BQU0sSUFBSXZKLFNBQVMsQ0FBQyx1Q0FBdUMsQ0FBQztJQUM5RDtJQUNBLElBQUksQ0FBQyxJQUFBakMsZ0JBQVEsRUFBQ29rQixVQUFVLENBQUMsRUFBRTtNQUN6QixNQUFNLElBQUluaUIsU0FBUyxDQUFDLHVDQUF1QyxDQUFDO0lBQzlEO0lBRUEsTUFBTW9MLFNBQVMsR0FBRzdCLFNBQVMsR0FBRyxFQUFFLEdBQUcsR0FBRztJQUN0QyxNQUFNK1ksU0FBUyxHQUFHclgsTUFBTTtJQUN4QixNQUFNc1gsYUFBYSxHQUFHSixVQUFVO0lBQ2hDLElBQUlGLGlCQUFpQixHQUFHLEVBQUU7SUFDMUIsSUFBSUgsT0FBcUIsR0FBRyxFQUFFO0lBQzlCLElBQUl0VyxLQUFLLEdBQUcsS0FBSztJQUNqQixNQUFNQyxVQUEyQixHQUFHLElBQUloUyxNQUFNLENBQUNpUyxRQUFRLENBQUM7TUFBRUMsVUFBVSxFQUFFO0lBQUssQ0FBQyxDQUFDO0lBQzdFRixVQUFVLENBQUNHLEtBQUssR0FBRyxZQUFZO01BQzdCLElBQUlrVyxPQUFPLENBQUM5ZCxNQUFNLEVBQUU7UUFDbEJ5SCxVQUFVLENBQUNoRCxJQUFJLENBQUNxWixPQUFPLENBQUNqVyxLQUFLLENBQUMsQ0FBQyxDQUFDO1FBQ2hDO01BQ0Y7TUFDQSxJQUFJTCxLQUFLLEVBQUU7UUFDVCxPQUFPQyxVQUFVLENBQUNoRCxJQUFJLENBQUMsSUFBSSxDQUFDO01BQzlCO01BRUEsSUFBSTtRQUNGLE1BQU05QixNQUFNLEdBQUcsTUFBTSxJQUFJLENBQUNxYixrQkFBa0IsQ0FDMUM1aEIsVUFBVSxFQUNWa2lCLFNBQVMsRUFDVEwsaUJBQWlCLEVBQ2pCN1csU0FBUyxFQUNULElBQUksRUFDSm1YLGFBQ0YsQ0FBQztRQUNELElBQUk1YixNQUFNLENBQUMrRixXQUFXLEVBQUU7VUFDdEJ1VixpQkFBaUIsR0FBR3RiLE1BQU0sQ0FBQzZiLHFCQUFxQjtRQUNsRCxDQUFDLE1BQU07VUFDTGhYLEtBQUssR0FBRyxJQUFJO1FBQ2Q7UUFDQXNXLE9BQU8sR0FBR25iLE1BQU0sQ0FBQ21iLE9BQU87UUFDeEI7UUFDQXJXLFVBQVUsQ0FBQ0csS0FBSyxDQUFDLENBQUM7TUFDcEIsQ0FBQyxDQUFDLE9BQU9oSixHQUFHLEVBQUU7UUFDWjZJLFVBQVUsQ0FBQ2dCLElBQUksQ0FBQyxPQUFPLEVBQUU3SixHQUFHLENBQUM7TUFDL0I7SUFDRixDQUFDO0lBQ0QsT0FBTzZJLFVBQVU7RUFDbkI7RUFFQSxNQUFNZ1gscUJBQXFCQSxDQUFDcmlCLFVBQWtCLEVBQUVvUyxNQUEwQixFQUFpQjtJQUN6RixJQUFJLENBQUMsSUFBQWhOLHlCQUFpQixFQUFDcEYsVUFBVSxDQUFDLEVBQUU7TUFDbEMsTUFBTSxJQUFJbEcsTUFBTSxDQUFDdUwsc0JBQXNCLENBQUMsdUJBQXVCLEdBQUdyRixVQUFVLENBQUM7SUFDL0U7SUFDQSxJQUFJLENBQUMsSUFBQTlCLGdCQUFRLEVBQUNrVSxNQUFNLENBQUMsRUFBRTtNQUNyQixNQUFNLElBQUl4UyxTQUFTLENBQUMsZ0RBQWdELENBQUM7SUFDdkU7SUFDQSxNQUFNYyxNQUFNLEdBQUcsS0FBSztJQUNwQixNQUFNRSxLQUFLLEdBQUcsY0FBYztJQUM1QixNQUFNZ04sT0FBTyxHQUFHLElBQUkzUixlQUFNLENBQUNDLE9BQU8sQ0FBQztNQUNqQ29XLFFBQVEsRUFBRSwyQkFBMkI7TUFDckNuVyxVQUFVLEVBQUU7UUFBRUMsTUFBTSxFQUFFO01BQU0sQ0FBQztNQUM3QkMsUUFBUSxFQUFFO0lBQ1osQ0FBQyxDQUFDO0lBQ0YsTUFBTW9ILE9BQU8sR0FBR21LLE9BQU8sQ0FBQy9HLFdBQVcsQ0FBQ3VMLE1BQU0sQ0FBQztJQUMzQyxNQUFNLElBQUksQ0FBQ3BPLG9CQUFvQixDQUFDO01BQUV0RCxNQUFNO01BQUVWLFVBQVU7TUFBRVk7SUFBTSxDQUFDLEVBQUU2QyxPQUFPLENBQUM7RUFDekU7RUFFQSxNQUFNNmUsMkJBQTJCQSxDQUFDdGlCLFVBQWtCLEVBQWlCO0lBQ25FLE1BQU0sSUFBSSxDQUFDcWlCLHFCQUFxQixDQUFDcmlCLFVBQVUsRUFBRSxJQUFJdWlCLGdDQUFrQixDQUFDLENBQUMsQ0FBQztFQUN4RTtFQUVBLE1BQU1DLHFCQUFxQkEsQ0FBQ3hpQixVQUFrQixFQUFxQztJQUNqRixJQUFJLENBQUMsSUFBQW9GLHlCQUFpQixFQUFDcEYsVUFBVSxDQUFDLEVBQUU7TUFDbEMsTUFBTSxJQUFJbEcsTUFBTSxDQUFDdUwsc0JBQXNCLENBQUMsdUJBQXVCLEdBQUdyRixVQUFVLENBQUM7SUFDL0U7SUFDQSxNQUFNVSxNQUFNLEdBQUcsS0FBSztJQUNwQixNQUFNRSxLQUFLLEdBQUcsY0FBYztJQUM1QixNQUFNc0QsR0FBRyxHQUFHLE1BQU0sSUFBSSxDQUFDVixnQkFBZ0IsQ0FBQztNQUFFOUMsTUFBTTtNQUFFVixVQUFVO01BQUVZO0lBQU0sQ0FBQyxDQUFDO0lBQ3RFLE1BQU13RCxJQUFJLEdBQUcsTUFBTSxJQUFBb0Isc0JBQVksRUFBQ3RCLEdBQUcsQ0FBQztJQUNwQyxPQUFPLElBQUF1ZSxrQ0FBdUIsRUFBQ3JlLElBQUksQ0FBQztFQUN0QztFQUVBc2Usd0JBQXdCQSxDQUN0QjFpQixVQUFrQixFQUNsQjZLLE1BQWMsRUFDZDhYLE1BQWMsRUFDZEMsTUFBMkIsRUFDUDtJQUNwQixJQUFJLENBQUMsSUFBQXhkLHlCQUFpQixFQUFDcEYsVUFBVSxDQUFDLEVBQUU7TUFDbEMsTUFBTSxJQUFJbEcsTUFBTSxDQUFDdUwsc0JBQXNCLENBQUMsd0JBQXdCckYsVUFBVSxFQUFFLENBQUM7SUFDL0U7SUFDQSxJQUFJLENBQUMsSUFBQXJDLGdCQUFRLEVBQUNrTixNQUFNLENBQUMsRUFBRTtNQUNyQixNQUFNLElBQUlqTCxTQUFTLENBQUMsK0JBQStCLENBQUM7SUFDdEQ7SUFDQSxJQUFJLENBQUMsSUFBQWpDLGdCQUFRLEVBQUNnbEIsTUFBTSxDQUFDLEVBQUU7TUFDckIsTUFBTSxJQUFJL2lCLFNBQVMsQ0FBQywrQkFBK0IsQ0FBQztJQUN0RDtJQUNBLElBQUksQ0FBQ3VZLEtBQUssQ0FBQ0MsT0FBTyxDQUFDd0ssTUFBTSxDQUFDLEVBQUU7TUFDMUIsTUFBTSxJQUFJaGpCLFNBQVMsQ0FBQyw4QkFBOEIsQ0FBQztJQUNyRDtJQUNBLE1BQU1pakIsUUFBUSxHQUFHLElBQUlDLGdDQUFrQixDQUFDLElBQUksRUFBRTlpQixVQUFVLEVBQUU2SyxNQUFNLEVBQUU4WCxNQUFNLEVBQUVDLE1BQU0sQ0FBQztJQUNqRkMsUUFBUSxDQUFDRSxLQUFLLENBQUMsQ0FBQztJQUNoQixPQUFPRixRQUFRO0VBQ2pCO0FBQ0Y7QUFBQ0csT0FBQSxDQUFBdm1CLFdBQUEsR0FBQUEsV0FBQSIsImlnbm9yZUxpc3QiOltdfQ==