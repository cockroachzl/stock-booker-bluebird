/**
 * Created by zhangliang on 8/7/2015.
 */

'use strict';

var StockFetcher = require('../index.js');
var Promise = require("bluebird");
var MongoDB = Promise.promisifyAll(require("mongodb"));
//Promise.resolve(200).then(console.log.bind(console));
//console.log(global.Promise == Promise);
var mongoConnection = MongoDB.MongoClient.connectAsync('mongodb://@localhost:27017/stock');
var findOnePromise = mongoConnection
    .then(function (db) {
        return db.collectionAsync('stocks');
    })
    .then(function (collection){
        return collection.findOneAsync();
    })
    .tap(console.log)
    .catch(function (error) {
        console.error(error);
    })
//.finally(function () {
//    return mongoConnection.then(function (dbConnPromise) {
//        dbConnPromise.close();
//    })
//});
var findPromise = mongoConnection
    .then(function(db) {
        return db.collection('stocks').findAsync();
    })
    .then(function(cursor){
        return cursor.toArrayAsync();
    })
    .tap(console.log)
    .catch(console.error);
Promise.all([mongoConnection, findOnePromise, findPromise]).spread(function(db){ //second argument is omitted.
    return db.close();
})
//var MongoDB = require("mongodb");
//MongoDB.MongoClient.connect('mongodb://@localhost:27017/stock')
//    .then(function(dbConnPromise){
//        console.log(dbConnPromise);
//        return dbConnPromise.close();
//    })
//MongoDB.MongoClient.connect('mongodb://@localhost:27017/stock', function(err, dbConnPromise){
//    console.log(dbConnPromise);
//    dbConnPromise.close();
//})

