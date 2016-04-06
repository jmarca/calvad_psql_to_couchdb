/*global require __dirname */
var should = require('should')
var query_vds = require('../lib/query_vds_info')
var tweak = require('../lib/tweak_sql_output')

var config_okay = require('config_okay')
var queue = require('d3-queue').queue


var path    = require('path')
var rootdir = path.normalize(__dirname)

var pg = require('pg')
var _ = require('lodash')
var fs = require('fs')
var config_file = rootdir+'/../test.config.json'
var config = {}
var utils = require('./utils.js')


before(function(done){
    config_okay(config_file,function(err,c){
        if(err){
            console.log('Problem trying to parse options in ',config_file)
            throw new Error(err)
        }
        if(c.couchdb === undefined || c.couchdb.db === undefined){
            return done('test.config.json needs couchdb.db defined to a test database name.  See the README for details')
        }
        if(c.postgresql === undefined
           || c.postgresql.db === undefined ){
               return done('need valid db defined in test.config.json under postgresql.db.  See the README for details')
           }

        config = Object.assign(config,c)
        utils.create_tempdb(config,done)
        return null
    })
})
after(function(done){
    // uncomment to bail in development
    // return done()
    console.log('done')
    utils.delete_tempdb(config,done)
    return null
})
describe('get vds info from psql database',function(){
    it('should get data for vds detector',function(done){
        var q = queue(1)
        config.id = '1009910'
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

        q.await(function(e,r1,r2){
            console.log('testing things')
            should.not.exist(e)
            should.exist(r1)
            should.exist(r2)
            r1.should.eql(config)
            r2.should.eql(config)

            r2.should.have.property('docs')

            var len = r2.docs.length
            len.should.eql(1) // just one detector
            var compound_length=0
            console.log('len is ',len)

            r2.docs.forEach(function(v,k){
                console.log(k)
                _.forEach(v,function(metadata,year){
                    console.log(year)
                    if(year==='_id') return null
                    if(/\d\d\d\d/.test(year)){
                        metadata.should.have.property('properties')
                        compound_length += metadata.properties.length
                    }
                    return null
                })
                return null
            })
            console.log(compound_length)
            compound_length.should.eql(11)
            return done()
        })
        return null
    })
})
