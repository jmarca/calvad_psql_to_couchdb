/*global require */
var make_bulkdoc_appender = require('couchdb_bulkdoc_appender')
var async = require('async')
var _ = require('lodash')
function update_doc(task,cb){
    // given a doc, update it with the new (or old or updated) properties.

    // can I use couchdb utilities I wrote long ago?
    var appender = make_bulkdoc_appender(task.couchdb)

    var cargo = async.cargo(appender,500)

    cargo.drain=function(){
        console.log('done processing')
        return cb()
    }
    // convert each "doc" into a proper doc
    var docs = task.docs
    cargo.push(docs)

    return null

}
