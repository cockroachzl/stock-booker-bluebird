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

var stockFetcher = new StockFetcher(mongoConnection);
setInterval(stockFetcher.updateUsingPromise.bind(stockFetcher, 'stocks', 'intraday_quotes', 'interday_quotes', 'population'), 5000);
