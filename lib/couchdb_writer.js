var make_bulkdoc_appender = require('couchdb_bulkdoc_appender')


function update_doc(task,cb){
    // given a doc, update it with the new (or old or updated) properties.

    // can I use couchdb utilities I wrote long ago?
    var appender = make_bulkdoc_appender(task.couchdb)

    var id=task.id
    var year

}