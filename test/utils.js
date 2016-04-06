var request = require('request')

function create_tempdb(config,done){
    // create a test db
    var date = new Date()
    var test_db,cdb,options
    test_db = [config.couchdb.db,
                          date.getHours(),
                          date.getMinutes(),
                          date.getSeconds(),
                          date.getMilliseconds()].join('-')
    cdb =
        [config.couchdb.host+':'+config.couchdb.port
         ,test_db].join('/')
    if(! /http/.test(cdb)){
        cdb = 'http://'+cdb
    }
    options = {
        'url':cdb,
        'method':'PUT',
        'json':true,
        'auth':{'username':config.couchdb.auth.username
                ,'password':config.couchdb.auth.password
               }
    }
    // console.log(options)
    request(options,function(e,r,b){
        if (e){
            throw new Error(e)
        }
        config.couchdb.db = test_db
        if(r.statusCode == 200) {
            // do something here?
        }
        return done()
    })
    return null
}

function delete_tempdb (config,done){
    var cdb,options
    cdb =
        [config.couchdb.host+':'+config.couchdb.port
         ,config.couchdb.db].join('/')
    if(! /http/.test(cdb)){
        cdb = 'http://'+cdb
    }
    options = {
        'url':cdb,
        'method':'DELETE',
        'json':true,
        'auth':config.couchdb.auth
    }
    request(options,function(e,r,b){
        if(e) return done(e)
        return done()
    })
    return null

}

exports.create_tempdb=create_tempdb
exports.delete_tempdb=delete_tempdb
