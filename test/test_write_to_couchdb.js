/*global require __dirname */
var should = require('should')
var query_vds = require('../lib/query_vds_info')
var tweak = require('../lib/tweak_sql_output')
var write = require('../lib/couchdb_writer')
var config_okay = require('config_okay')
var async = require('async')

var path    = require('path')
var rootdir = path.normalize(__dirname)
var getter = require('couchdb_get_views')
var pg = require('pg')
var _ = require('lodash')
var fs = require('fs')
describe('get vds info from psql database',function(){
    it('should get data for vds detector',function(done){

        async.waterfall([function(cb){
                             var config_file = rootdir+'/../test.config.json'

                             config_okay(config_file,function(err,c){
                                 if(!c.postgresql || ! c.postgresql.db){ throw new Error('need valid db defined in test.config.json under postgresql.db.  See the README for details')}
                                 c.detector_id='1000210'
                                 c.direction='S'
                                 return cb(null,c)
                             })
                             return null
                         }
                        ,function(task,cb){
                             var connection_string = "pg://"
                                                   + task.postgresql.auth.username+":"
                                                   + task.postgresql.auth.password+"@"
                                                   + task.postgresql.host+":"
                                                   + task.postgresql.port+"/"
                                                   + task.postgresql.db
                             pg.connect(connection_string
                                       ,query_vds(task,cb)
                                       )
                             return null
                         }
                        ,tweak
                        ,write]
                       ,function(e,task){
                            should.not.exist(e)
                            should.exist(task)
                            var db = task.couchdb.db
                            var cdb = task.couchdb.url || '127.0.0.1'
                            var cport = task.couchdb.port || 5984
                            cdb = cdb+':'+cport
                            if(! /http/.test(cdb)){
                                cdb = 'http://'+cdb
                            }
                            cdb += '/'+db
                            var rq = getter({db:db
                                            ,couchdb:task.couchdb.url
                                            ,port:task.couchdb.port
                                            ,include_docs:true
                                            ,startkey:0
                                            ,endkey:'a'
                                            }
                                           ,function(e,result){
                                                // now test that I saved what I expect
                                                var len = result.rows.length
                                                //len.should.eql(17110) // as of april, 2014

                                                _.each(result.rows,function(row){
                                                    var doc = row.doc
                                                    var source =  task.vds_version_data[doc._id]
                                                    if(source === undefined){
                                                        console.log('problem with '+doc._id)
                                                        return null
                                                    }

                                                    var expected_keys =_.keys(source)
                                                    _.each(expected_keys,function(k){
                                                        doc.should.have.property(k)
                                                        doc[k].should.have.property('properties')
                                                        doc[k].properties.should.eql(source[k])
                                                        return null
                                                    });

                                                    return null
                                                });
                                                return done()
                                            });
                            return null
                        })
        return null
    })
})
