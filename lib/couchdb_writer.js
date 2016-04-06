/*global require */
var make_bulkdoc_appender = require('couchdb_bulkdoc_appender')
var queue = require('d3-queue').queue

var _ = require('lodash')
module.exports = function update_doc(task,cb){
    // given a doc, update it with the new (or old or updated) properties.

    // can I use couchdb utilities I wrote long ago?
    var appender = make_bulkdoc_appender(task.couchdb)

    var q = queue(1)
    // convert each "doc" into a proper doc
    var docs = task.docs
    var groups = _.groupBy(docs, function(num,idx) {

                     return Math.floor(idx/500)

             });
    var n = _.size(groups)
    _.each(groups,function(group,i){
        var idx = i + 1
        q.defer(appender,{'docs':group})
        return null
    })
    q.await(function(e,r){
        console.log('done processing')
        return cb(null,task)
    })

    return null

}
