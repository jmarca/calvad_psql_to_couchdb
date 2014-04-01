var _ = require('lodash')

var year_regex = /\d{4}/;

module.exports=function tweak_sql(task,cb){
    task.docs =
    _.map(task.vds_version_data,function(vs,id){
        var doc =  {'_id':id }
        _.each(vs,function(props,yr){
            doc[yr]={'properties':props}
            return null
        });
        return doc
    })
    return cb(null,task)
}
