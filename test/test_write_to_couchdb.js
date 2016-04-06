/*global require __dirname */
var should = require('should')
var query_vds = require('../lib/query_vds_info')
var tweak = require('../lib/tweak_sql_output')
var write = require('../lib/couchdb_writer')

var config_okay = require('config_okay')
var queue = require('d3-queue').queue


var path    = require('path')
var rootdir = path.normalize(__dirname)

var getter = require('couchdb_get_views')
var putdoc = require('couchdb_put_doc')
var putter
var initdoc = require('./files/1009910.json')

var pg = require('pg')
var _ = require('lodash')
var fs = require('fs')

var config_file = rootdir+'/../test.config.json'
var config = {}
var utils = require('./utils.js')


var expected_1009910_2012 = {
    "2012": {
        "properties": [
            {
                "name": "Roberts Island Rd",
                "cal_pm": "14.045",
                "abs_pm": 62.267,
                "latitude_4269": "37.927495",
                "longitude_4269": "-121.332465",
                "lanes": 1,
                "segment_length": "3.698",
                "freeway": 4,
                "direction": "W",
                "vdstype": "ML",
                "district": 10,
                "versions": [
                    "2012-09-25",
                    "2012-10-04",
                    "2012-11-15",
                    "2012-12-14"
                ],
                "geojson": {
                    "type": "Point",
                    "crs": {
                        "type": "name",
                        "properties": {
                            "name": "EPSG:4326"
                        }
                    },
                    "coordinates": [
                            -121.33,
                        37.927
                    ]
                }
            }
        ],
        "rawdata": "mean volumes too low in lane: nr1 in raw vds file",
        "vdsraw_chain_lengths": "raw data failed basic sanity checks"
    }
}


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
        var q_b = queue(1)
        q_b.defer(utils.create_tempdb,config)
        q_b.defer(function(cb){
            var putter_opts = {cdb:config.couchdb.db,
                             cuser:config.couchdb.auth.username,
                             cpass:config.couchdb.auth.password,
                             chost:config.couchdb.host,
                             cport:config.couchdb.port
                              }
            putter = putdoc(putter_opts)

            putter(initdoc,function(e,r){
                should.not.exist(e)
                should.exist(r)
                r.should.have.property('ok')
                r.should.have.property('rev')
                r.should.have.property('id')
                r.id.should.eql('1009910')
                return cb()
            })

            return null
        })
        q_b.await(done)
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
        config.years=2012
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

            should.not.exist(e)
            should.exist(r1)
            should.exist(r2)
            should.exist(r3)
            config.should.eql(r1)
            config.should.eql(r2)
            config.should.eql(r3)


            var rq = getter(Object.assign({}
                                          ,config.couchdb
                                          ,{include_docs:true
                                            ,startkey:0
                                            ,endkey:'a'
                                           })
                            ,function(e,result){
                                // now test that I saved what I expect
                                var len = result.rows.length
                                len.should.eql(1) // just one detector

                                result.rows.forEach(function(row){
                                    var doc = row.doc
                                    var source =  config.vds_version_data[doc._id]
                                    if(source === undefined){
                                        console.log('problem with '+doc._id)
                                        return null
                                    }

                                    var expected_keys =Object.keys(source)
                                    expected_keys.forEach(function(k){
                                        doc.should.have.property(k)
                                        doc[k].should.have.property('properties')
                                        doc[k].properties.should.eql(source[k])
                                        return null
                                    })

                                    doc[2012].should.have.property('rawdata')
                                    doc[2012].rawdata.should.eql(
                                        expected_1009910_2012[2012].rawdata)
                                    doc[2006].should.eql(initdoc[2006])

                                    return null
                                })
                                return done()
                            })

            return null
        })
        return null
    })
})
