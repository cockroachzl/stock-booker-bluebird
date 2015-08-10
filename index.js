/**
 * Created by zhangliang on 6/29/2015.
 * Populate stock infos from Yahoo finance and store in the data store.
 */

'use strict';

var http = require('http');
var moment = require('moment');
var Promise = require("bluebird");
var MongoDB = Promise.promisifyAll(require("mongodb"));


//stocks: stock static info: symbols
//intraday_quotes : quotes within the current day
//interday_quotes : historicall quotes
//population: date, populated
function StockFetcher(dbConnPromise) {
    this.dbConnPromise = !!dbConnPromise ? dbConnPromise : MongoDB.MongoClient.connectAsync('mongodb://@localhost:27017/stock');
    this.symbols = [];
}

StockFetcher.BASE = 'http://query.yahooapis.com/v1/public/yql';

/**
* Main processing function for communicating with Yahoo Finance API
* @param symbols an array of stock symbols
* return a promise of a list of quotes.
*/
StockFetcher.fetch = function (symbols) {
    console.log("going to fetch quotes");
    var query = encodeURIComponent('select * from yahoo.finance.quotes ' +
    'where symbol in (\'' + symbols.join(',') + '\')');
    var urlWithParams = StockFetcher.BASE + '?' + 'q=' + query + '&format=json&diagnostics=true' + '&env=' +
        encodeURIComponent('store://datatables.org/alltableswithkeys');
    var completeUrl = urlWithParams + '&callback=';
    var promise = new Promise(function (resolve, reject) {
        http.get(completeUrl, function (res) {
            console.log(completeUrl);
            res.setEncoding('utf8');
            var data = '';

            res.on('data', function (chunk) {
                data += chunk;
            });

            res.on('end', function () {
                try {
                    var obj = JSON.parse(data);
                    console.log('Got ' + obj.query.count + ' stocks');
                    resolve(obj.query.results.quote);
                } catch (e) {
                    reject(e);
                }
            })

        }).on('error', function (e) {
            reject(e);
        });
    });
    return promise;
};

StockFetcher.isMarketOpen = function (now) {
    var now = now ? now : moment().utcOffset(-4);
    var current_hour = now.hours();
    var current_minutes = current_hour * 60 + now.minutes();
    var marketOpen = 9 * 60 + 30;
    var marketClose = 16 * 60;
    if (current_minutes >= marketOpen && current_minutes <= marketClose) {
        return true;
    }
    else {
        return false;
    }
}

StockFetcher.prototype.updateUsingPromise = function (stocksCollectionName, intradayQuotesCollectionname,
                                                      interdayQuotesCollectionName, populationCollectionName) {
    var that = this;
    //var stocksPromise = this.findOnePromise(stocksCollectionName, {}).then(function (value) {
    //    return value.symbols;
    //});
    var stocksPromise = this.dbConnPromise.then(function (db) {
        return db.collectionAsync(stocksCollectionName);
    }).then(function (collection) {
        return collection.findOneAsync();
    }).then(function(record){
        return record.symbols;
    });
    if (StockFetcher.isMarketOpen()) {
        return stocksPromise
            .then(StockFetcher.fetch) //fetch stock quotes
            .then(function (quotes) { //insert into intraday collection
                return this.dbConnPromise.then(function (db) {
                    return db.collectionAsync(intradayQuotesCollectionname)
                }).then(function (collection) {
                    return collection.insertManyAsync(quotes);
                })
            });
    }
    var now = moment().utcOffset(-4);
    //var date = [now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()].join('-');
    if (now.hours() < 10) { //next early morning
        now.subtract(1, 'days');
    }
    var date = now.format('YYYY-MM-DD');
    //var populationPromise = this.findOnePromise(populationCollectionName, );
    var populationPromise = this.dbConnPromise.then(function (db) {
        return db.collectionAsync(populationCollectionName)
    }).then(function (collection) {
        return collection.findOneAsync({'date': date});
    })
    //Resume
    return populationPromise.then(function (value) {
        if (!value) {
            console.log('fetching and populating interday quotes');
            return stocksPromise
                .then(StockFetcher.fetch)
                .then(function(quotes){
                    return that.dbConnPromise
                        .then(function (db) {
                            return db.collectionAsync(interdayQuotesCollectionName);
                        })
                        .then(function (collection) {
                            return collection.insertMany(quotes);
                        });
                })
                .then(function () {
                    var population = {'date': date};
                    return that.dbConnPromise
                        .then(function (db) {
                            return db.collectionAsync(populationCollectionName);
                        })
                        .then(function (collection) {
                            return collection.insertOne(population);
                        });
                })
                .catch(console.log);
        }
        else {
            console.log('already populated ' + value.date);
        }
    });
}

/**
 *
 * @param user one user has multiple watch lists, each of them has multiple stock symbols
 */
StockFetcher.prototype.createUser = function(userId, firstName, lastName) {
    var user = {
        'userId' : userId,
        'firstName' : firstName,
        'lastName' : lastName,
        'watchlists' : {}
    };
    return this.dbConnPromise
        .then(function (dbConn) {
            return dbConn.collectionAsync('User');
        })
        .then(function (collection) {
            return collection.insertOne(user);
        });
}

StockFetcher.prototype.createWatchList = function(userId, watchListName) {
    return this.dbConnPromise
        .then(function (dbConn) {
            return dbConn.collectionAsync('User');
        })
        .then(function (collection) {
            return collection.findOne({'userId' : userId});
        })
        .then(function (user){
            user.watchlists.watchListName...
        })
}


module.exports = StockFetcher;
