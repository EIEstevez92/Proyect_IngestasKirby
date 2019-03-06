'use strict';
const Hapi 			= require('hapi');
const fs 				= require('fs');
const joi 				= require('joi');
const server 		= new Hapi.Server();

let defs = {
	port:'8080',
	localhost: '0.0.0.0'
};

const headerEpsilonValidate = joi.object().keys({
    'api-key': joi.required()
}).options({ allowUnknown: true })

server.connection({
	host: defs.localhost,
	port: defs.port
});
let pay  = [];
let routes = [
	{
		method: 'POST', // Must handle both GET and POST
		path: '/',      // The callback endpoint registered with the provider
		config: {
			handler: function (request, reply) {
			    pay.push(request.payload);
			    //pay.push(JSON.parse(request.payload))
				reply(request.payload);
			}
		}
	},{
        method: 'GET', // Must handle both GET and POST
        path: '/',      // The callback endpoint registered with the provider
        config: {
            handler: function (request, reply) {
                reply(pay)
            }
        }
    },{
         method: 'DELETE', // Must handle both GET and POST
         path: '/',      // The callback endpoint registered with the provider
         config: {
             handler: function (request, reply) {
                 pay = [];
                 reply(pay);
             }
         }
     },
     {
         method: 'GET',
         path: '/ns/namespaceId/buckets/bucketAvroLocal/files/{tags?}',
         config: {
             validate:{
                 headers: headerEpsilonValidate
             },
             handler: function (request, reply) {
                 return reply(jsonBucketAvroLocal);
             }
         }
     },{
           method: 'GET',
           path: '/ns/namespaceId/buckets/bucketCSVCompressLocal/files/{tags?}',
           config: {
               validate:{
                   headers: headerEpsilonValidate
               },
               handler: function (request, reply) {
                   return reply(jsonBucketCSVCompressLocal);
               }
           }
       },
       {
           method: 'GET',
           path: '/ns/namespaceId/buckets/bucketCSVLocal/files/{tags?}',
           config: {
               validate:{
                   headers: headerEpsilonValidate
               },
               handler: function (request, reply) {
                   return reply(jsonBucketCSVLocal);
               }
           }
       }
];

server.route(routes);

server.start(() => {
	console.log('Server running at:', server.info.uri);
});

module.exports = server;


let jsonBucketCSVLocal =
{
    "files": [
    {
        "_ac": false,
        "_id": "filewith10lines.txt",
        "length": 10,
        "created": "2017-07-03T08:49:56.347Z",
        "_locator": "locator",
        "_owner": "n.a.",
        "_type": "epsilon.file",
        "origin": "hdfs://hadoop:9000/tests/flow/epsilon/filewith10lines.txt",
        "name": "file1.txt",
          "tags": [
            "tag1"
          ]
    },
    {
        "_ac": false,
        "_id": "filewith15lines.txt",
        "length": 10,
        "created": "2017-07-03T08:49:56.347Z",
        "_locator": "locator",
        "_owner": "n.a.",
        "_type": "epsilon.file",
        "origin": "hdfs://hadoop:9000/tests/flow/epsilon/filewith15lines.txt",
        "name": "file2.txt",
          "tags": [
            "tag1"
          ]
    }
  ]
}

let jsonBucketCSVCompressLocal =
{
    "files": [
    {
        "_ac": false,
        "_id": "filewith10linescompress.txt",
        "length": 10,
        "created": "2017-07-03T08:49:56.347Z",
        "_locator": "locator",
        "_owner": "n.a.",
        "_type": "epsilon.file",
        "origin": "hdfs://hadoop:9000/tests/flow/epsilon/filewith10linescompress.txt/",
        "name": "file1.txt",
          "tags": [
            "tag1"
          ]
    },
    {
        "_ac": false,
        "_id": "filewith15linescompress.txt",
        "length": 10,
        "created": "2017-07-03T08:49:56.347Z",
        "_locator": "locator",
        "_owner": "n.a.",
        "_type": "epsilon.file",
        "origin": "hdfs://hadoop:9000/tests/flow/epsilon/filewith15linescompress.txt/",
        "name": "file2.txt",
          "tags": [
            "tag1"
          ]
    }
  ]
}

let jsonBucketAvroLocal =
{
    "files": [
    {
        "_ac": false,
        "_id": "10lines.avro",
        "length": 10,
        "created": "2017-07-03T08:49:56.347Z",
        "_locator": "locator",
        "_owner": "n.a.",
        "_type": "epsilon.file",
        "origin": "hdfs://hadoop:9000/tests/flow/epsilon/10lines.avro",
        "name": "file1Avro.txt",
          "tags": [
            "tag1"
          ]
    },
    {
        "_ac": false,
        "_id": "15lines.avro",
        "length": 10,
        "created": "2017-07-03T08:49:56.347Z",
        "_locator": "locator",
        "_owner": "n.a.",
        "_type": "epsilon.file",
        "origin": "hdfs://hadoop:9000/tests/flow/epsilon/15lines.avro",
        "name": "file2Avro.txt",
          "tags": [
            "tag1"
          ]
    }
  ]
}
