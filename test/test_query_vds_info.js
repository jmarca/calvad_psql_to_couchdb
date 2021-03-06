/*global require __dirname */
var should = require('should')
var query_vds = require('../lib/query_vds_info')
var config_okay = require('config_okay')
var async = require('async')

var path    = require('path')
var rootdir = path.normalize(__dirname)

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
                         }]
                       ,function(e,r){
                            should.not.exist(e)
                            should.exist(r)
                            r.should.have.property('vds_version_data')
                            var detectors = Object.keys(r.vds_version_data)
                           fs.writeFileSync('/tmp/test.json',JSON.stringify(r.vds_version_data[detectors[0]]),'utf8')
                            var len = detectors.length
                            //len.should.eql(14606) // as of march, 2014
                            len.should.eql(17110) // as of april, 2014
                            var compound_length = 0
                            _.forEach(r.vds_version_data,function(v,k){
                                _.forEach(v,function(metadata,year){
                                    compound_length += metadata.length
                                })
                            })
                            compound_length.should.eql(104444)
                            return done()
                        })
        return null
    })
})
