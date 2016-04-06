/*global require __dirname */

var query_vds = require('./lib/query_vds_info')
var tweak = require('./lib/tweak_sql_output')
var write = require('./lib/couchdb_writer')

var config_okay = require('config_okay')
var queue = require('d3-queue').queue

var path    = require('path')
var rootdir = path.normalize(__dirname)

var getter = require('couchdb_get_views')
var putdoc = require('couchdb_put_doc')


var pg = require('pg')
var _ = require('lodash')
var fs = require('fs')

var config_file = rootdir+'/config.json'
var config = {}


config_okay(config_file,function(err,c){
    if(err){
        console.log('Problem trying to parse options in ',config_file)
        throw new Error(err)
    }
    if(c.couchdb === undefined || c.couchdb.db === undefined){
        throw new Error('test.config.json needs couchdb.db defined to a test database name.  See the README for details')
    }
    if(c.postgresql === undefined
       || c.postgresql.db === undefined ){
           throw new Error('need valid db defined in test.config.json under postgresql.db.  See the README for details')
       }

    config = Object.assign(config,c)

    var q = queue(1)
    // only process newer than 2014
    config.years=2014
    q.defer(function(cb){
        var passwd = config.postgresql.auth.password || ''
        var connection_string = "pg://"
                + config.postgresql.auth.username+":"
                + passwd+"@"
                + config.postgresql.host+":"
                + config.postgresql.port+"/"
                + config.postgresql.db
        pg.connect(connection_string
                   ,query_vds(config,cb)
                  )
        return null
    })
    q.defer(tweak,config)
    q.defer(write,config)

    q.await(function(e,r1,r2,r3){
        if(e) console.log(e)
        return null
    })
})
