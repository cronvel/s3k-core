/*
	S3K core

	Copyright (c) 2018 - 2021 Cédric Ronvel

	The MIT License (MIT)

	Permission is hereby granted, free of charge, to any person obtaining a copy
	of this software and associated documentation files (the "Software"), to deal
	in the Software without restriction, including without limitation the rights
	to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
	copies of the Software, and to permit persons to whom the Software is
	furnished to do so, subject to the following conditions:

	The above copyright notice and this permission notice shall be included in all
	copies or substantial portions of the Software.

	THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
	IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
	FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
	AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
	LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
	OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
	SOFTWARE.
*/

"use strict" ;



const Promise = require( 'seventh' ) ;

// The SDK
const AWS = require( 'aws-sdk' ) ;

// The various Amazon header-signing version (seriously, it takes them 4 versions to get a not-too-much-sucking-authorization).
// No support for v1 (too insecure).
const aws2 = require( 'aws2' ) ;
const aws3 = require( 'aws3' ) ;
const aws4 = require( 'aws4' ) ;

const url = require( 'url' ) ;
const querystring = require( 'querystring' ) ;

const Logfella = require( 'logfella' ) ;
const log = Logfella.global.use( 's3k' ) ;



// /!\ Should be implemented soon!
const defaultRetryOptions = {
	retries: 10 ,
	coolDown: 10 ,
	raiseFactor: 1.5 ,
	maxCoolDown: 30000
} ;



/*
	Full doc:
	https://docs.aws.amazon.com/AWSJavaScriptSDK/latest/AWS/S3.html
*/



function S3k( options ) {
	if ( ! options || typeof options !== 'object' || ! options.endpoint || ! options.accessKeyId || ! options.secretAccessKey ) {
		throw new Error( 'S3k should be created with at least those mandatory options: endpoint, accessKeyId and secretAccessKey' ) ;
	}

	this.endpoint = new AWS.Endpoint( options.endpoint ) ;
	this.accessKeyId = options.accessKeyId ;
	this.secretAccessKey = options.secretAccessKey ;
	this.bucket = options.bucket || null ;
	this.prefix = options.prefix || null ;
	this.delimiter = options.delimiter || null ;

	this._s3 = new AWS.S3( {
		endpoint: new AWS.Endpoint( this.endpoint ) ,
		accessKeyId: this.accessKeyId ,
		secretAccessKey: this.secretAccessKey
	} ) ;

	Promise.promisifyAnyNodeApi( this._s3 ) ;
}

module.exports = S3k ;



/*
	Bucket
*/
S3k.prototype.getBucketAcl = function( params ) {
	params = params ? Object.assign( {} , params ) : {} ;
	if ( this.bucket && ! params.Bucket ) { params.Bucket = this.bucket ; }

	return this._s3.getBucketAclAsync( params ) ;
} ;



/*
	Bucket
	ACL: private | public-read | public-read-write | authenticated-read
	AccessControlPolicy: `object`, where:
		Grants: `array` of `object` :
			Grantee: `object` where:
				Type: CanonicalUser | AmazonCustomerByEmail | Group
				DisplayName: `string`
				EmailAddress: `string`
				ID: `string`
				URI: `string`
			Permission: FULL_CONTROL | WRITE | WRITE_ACP | READ | READ_ACP
		Owner: `object` where:
			DisplayName: `string`
			ID: `string`
*/
S3k.prototype.setBucketAcl =
S3k.prototype.putBucketAcl = function( params ) {
	params = params ? Object.assign( {} , params ) : {} ;
	if ( this.bucket && ! params.Bucket ) { params.Bucket = this.bucket ; }

	return this._s3.putBucketAclAsync( params ) ;
} ;



/*
	Bucket, Prefix, Delimiter
*/
S3k.prototype.listObjects = function( params ) {
	params = params ? Object.assign( {} , params ) : {} ;

	if ( this.bucket && ! params.Bucket ) { params.Bucket = this.bucket ; }
	if ( this.prefix ) { params.Prefix = this.prefix + ( params.Prefix || '' ) ; }
	if ( this.delimiter && ! params.Delimiter ) { params.Delimiter = this.delimiter ; }

	return this._s3.listObjectsAsync( params ) ;
} ;



/*
	Bucket, Key
*/
S3k.prototype.getObject = function( params = {} ) {
	params = params ? Object.assign( {} , params ) : {} ;

	if ( this.bucket && ! params.Bucket ) { params.Bucket = this.bucket ; }
	if ( this.prefix ) { params.Key = this.prefix + params.Key ; }

	//console.log( "Params:" , params ) ;

	return this._s3.getObjectAsync( params ) ;
} ;



/*
	Bucket, Key
*/
S3k.prototype.getObjectStream = function( params = {} ) {
	params = params ? Object.assign( {} , params ) : {} ;

	if ( this.bucket && ! params.Bucket ) { params.Bucket = this.bucket ; }
	if ( this.prefix ) { params.Key = this.prefix + params.Key ; }

	//console.log( "Params:" , params ) ;

	return this._s3.getObject( params ).createReadStream() ;
} ;



/*
	Bucket, Key, Body
	DOES NOT WORK WITH STREAM if the size is unknown: use upload instead!
*/
S3k.prototype.putObject = function( params = {} ) {
	params = params ? Object.assign( {} , params ) : {} ;

	if ( this.bucket && ! params.Bucket ) { params.Bucket = this.bucket ; }
	if ( this.prefix ) { params.Key = this.prefix + params.Key ; }

	return this._s3.putObjectAsync( params ) ;
} ;



/*
	Bucket, Key, Body
	Work with stream of unknown size
*/
S3k.prototype.upload = function( params = {} ) {
	params = params ? Object.assign( {} , params ) : {} ;

	if ( this.bucket && ! params.Bucket ) { params.Bucket = this.bucket ; }
	if ( this.prefix ) { params.Key = this.prefix + params.Key ; }

	return this._s3.uploadAsync( params ) ;
} ;



/*
	Bucket, Key
*/
S3k.prototype.deleteObject = function( params = {} ) {
	params = params ? Object.assign( {} , params ) : {} ;

	if ( this.bucket && ! params.Bucket ) { params.Bucket = this.bucket ; }
	if ( this.prefix ) { params.Key = this.prefix + params.Key ; }

	return this._s3.deleteObjectAsync( params ) ;
} ;



/*
	Bucket, Key
*/
S3k.prototype.deleteObjects = function( params = {} ) {
	params = params ? Object.assign( {} , params ) : {} ;

	var keys ;

	if ( this.bucket && ! params.Bucket ) { params.Bucket = this.bucket ; }

	if ( Array.isArray( keys = params.Keys || params.Key ) ) {
		if ( this.prefix ) {
			params.Delete = {
				Objects: keys.map( key => ( { Key: this.prefix + key } ) ) ,
				Quiet: false
			} ;
		}
		else {
			params.Delete = {
				Objects: keys.map( key => ( { Key: key } ) ) ,
				Quiet: false
			} ;
		}
	}
	else if ( this.prefix ) {
		params.Key = this.prefix + params.Key ;
	}

	return this._s3.deleteObjectAsync( params ) ;
} ;



// This parses the authorization HTTP header
S3k.parseAuthorizationHeader = function( authorization ) {
	var match , type , version , credential , signedHeaders , signature ;

	if ( ! authorization ) { throw new Error( "No authorization header" ) ; }

	match = authorization.match( /^([^ -]+)(?:-[^ ]+)?/ ) ;
	if ( ! match ) { throw new Error( "Unknown authorization header" ) ; }

	type = match[ 0 ] ;
	version = match[ 1 ] ;
	//log.hdebug( "type: %s -- version: %s" , type , version ) ;

	switch ( version ) {
		/*
		case 'AWS' :
			throw new Error( "AWS (v1) authorization header not supported" ) ;
		*/
		case 'AWS' :
		case 'AWS2' :
			throw new Error( "AWS v2 authorization header not supported" ) ;

		case 'AWS3' :
			throw new Error( "AWS v3 authorization header not supported" ) ;

		case 'AWS4' :
			credential = authorization.match( /Credential=([^,]+)/ ) ;
			signedHeaders = authorization.match( /SignedHeaders=([^,]+)/ ) ;
			signature = authorization.match( /Signature=([^,]+)/ ) ;

			if ( ! credential || ! signedHeaders || ! signature ) { throw new Error( "Bad AWS v4 authorization header" ) ; }

			return {
				version: version ,
				type: type ,
				credential: credential[ 1 ] ,
				accessKeyId: credential[ 1 ].split( '/' )[ 0 ] ,
				signedHeaders: signedHeaders[ 1 ].split( ';' ) ,
				signature: signature[ 1 ]
			} ;

		default :
			throw new Error( "Unknown authorization header type: " + type ) ;
	}
} ;



// Same than .parseAuthorizationHeader()
S3k.parseAuthorizationQueryString = function( query ) {
	var type , version , credential , accessKeyId , signedHeaders , signature , date , expires ;

	if ( ! query || typeof query !== 'object' ) {
		query = querystring.parse( query ) ;
	}

	type = query['X-Amz-Algorithm'] ;

	if ( ! type ) { throw new Error( "Unknown authorization query string" ) ; }

	version = type.split( '-' )[ 0 ] ;

	//log.hdebug( "type: %s -- version: %s" , type , version ) ;

	switch ( version ) {
		/*
		case 'AWS' :
			throw new Error( "AWS (v1) authorization header not supported" ) ;
		*/
		case 'AWS' :
		case 'AWS2' :
			throw new Error( "AWS v2 authorization query string not supported" ) ;

		case 'AWS3' :
			throw new Error( "AWS v3 authorization query string not supported" ) ;

		case 'AWS4' :
			credential = query['X-Amz-Credential'] ;
			signedHeaders = query['X-Amz-SignedHeaders'] ;
			signature = query['X-Amz-Signature'] || query['Signature'] ;

			if ( ! credential || ! signedHeaders || ! signature ) { throw new Error( "Bad AWS v4 authorization query string" ) ; }

			accessKeyId = query['AWSAccessKeyId'] || credential.split( '/' )[ 0 ] ;

			// Unused ATM, but will probably be in the future:
			date = new Date( query['X-Amz-Date'] ) ;
			expires = query['X-Amz-Expires'] ;

			return {
				version: version ,
				type: type ,
				credential: credential ,
				accessKeyId: accessKeyId ,
				signedHeaders: signedHeaders.split( ';' ) ,
				signature: signature
			} ;

		default :
			throw new Error( "Unknown authorization query string type: " + type ) ;
	}
} ;



S3k.signHeadersFromRequest = function( request , signedHeaders , accessKeyId , secretAccessKey ) {
	var opts = {
		host: request.headers.host ,
		method: request.method ,
		path: request.url ,
		headers: {}
	} ;

	signedHeaders.forEach( headerName => {
		opts.headers[ headerName ] = request.headers[ headerName ] ;
	} ) ;

	return S3k.signHeaders( opts , accessKeyId , secretAccessKey ) ;
} ;



S3k.signHeaders = function( opts , accessKeyId , secretAccessKey ) {
	if ( ! opts.service ) { opts.service = 's3' ; }

	log.debug( "Signing headers with:\n%J" , { request: opts , accessKeyId , secretAccessKey } ) ;
	aws4.sign( opts , { accessKeyId: accessKeyId , secretAccessKey: secretAccessKey } ) ;

	//log.hdebug( "Final opts: %Y" , opts ) ;

	return opts.headers ;
} ;



S3k.signQueryStringFromRequest = function( request , signedHeaders , accessKeyId , secretAccessKey ) {
	//log.hdebug( "request.url: %s" , request.url ) ;

	// Parse the URL and query string, remove any pre-existing signature, then re-stringify it
	var parsed = url.parse( request.url , true ) ;
	//log.hdebug( "parsed query: %Y" , parsed.query ) ;
	delete parsed.query['X-Amz-Signature'] ;
	delete parsed.query['Signature'] ;
	parsed.search = '?' + querystring.stringify( parsed.query ) ;
	parsed.path = parsed.pathname + parsed.search ;
	//log.hdebug( "stringified query: %Y" , parsed.query ) ;

	var opts = {
		host: request.headers.host ,
		method: request.method ,
		path: parsed.path ,
		headers: {}
	} ;

	signedHeaders.forEach( headerName => {
		opts.headers[ headerName ] = request.headers[ headerName ] ;
	} ) ;

	return S3k.signQueryString( opts , accessKeyId , secretAccessKey ) ;
} ;



S3k.signQueryString = function( opts , accessKeyId , secretAccessKey ) {
	return url.parse( S3k.signPath( opts , accessKeyId , secretAccessKey ) ).query ;
} ;



S3k.signPath = function( opts , accessKeyId , secretAccessKey ) {
	if ( ! opts.service ) { opts.service = 's3' ; }
	opts.signQuery = true ;

	log.debug( "Signing query with:\n%J" , { request: opts , accessKeyId , secretAccessKey } ) ;
	aws4.sign( opts , { accessKeyId: accessKeyId , secretAccessKey: secretAccessKey } ) ;

	//log.hdebug( "Final opts: %Y" , opts ) ;
	//log.hdebug( "opts.path  : %s" , opts.path ) ;

	return opts.path ;
} ;

