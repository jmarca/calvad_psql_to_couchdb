/**
 * query_vds_info
 *
 * get vds info from postgresql
 *
 * call with a task with details, returns a handler for pg.connect call
 *
 */
var pad2 = require('./pad.js')(2)
var pg = require('pg')
var versionmatcher = /(\d{4}-\d{2}-\d{2})/;
var _ = require('lodash')

module.exports = function vds_query(task,next){

    var select_properties=
        {  'id'                        : 'id'
        , 'name'                      : 'name'
        , 'cal_pm'                    : 'cal_pm'
        , 'abs_pm'                    : 'abs_pm'
        , 'latitude'                  : 'latitude_4269'
        , 'longitude'                 : 'longitude_4269'
        , 'lanes'                     : 'lanes'
        , 'segment_length'            : 'segment_length'
        , 'freeway_id'                : 'freeway'
        , 'freeway_dir'               : 'direction'
        , 'vdstype'                   : 'vdstype'
        , 'district'                  : 'district'
        , 'versions'                  : 'versions' // will need to resort these
        , 'vyear'                     : 'year'
        , 'st_asgeojson(gg.geom,12,2)': 'GeoJSON'
        }

    // make the select statement
    var cols = []
    for (var key in select_properties){
        cols.push(key +' as '+ select_properties[key])
    }
    var propcols = cols.join(',')

    var query=
        ['SELECT '+propcols
        ,'from('
        ,' SELECT v.id, v.name, v.cal_pm, v.abs_pm, v.latitude, v.longitude, vv.lanes, vv.segment_length,'
        ,'         array_agg(vv.version) AS versions,  extract( year from vv.version) as vyear, '
        ,'         vf.freeway_id, vf.freeway_dir, vt.type_id AS vdstype, vd.district_id AS district,'
        ,'         g.gid'
        ,'   FROM vds_id_all v'
        ,'   LEFT OUTER JOIN vds_versioned vv USING (id)'
        ,'   LEFT OUTER JOIN vds_points_4326 ON v.id = vds_points_4326.vds_id'
        ,'   LEFT OUTER JOIN vds_vdstype vt USING (vds_id)'
        ,'   LEFT OUTER JOIN vds_district vd USING (vds_id)'
        ,'   LEFT OUTER JOIN vds_freeway vf USING (vds_id)'
        ,'   LEFT OUTER JOIN geom_points_4326 g USING (gid)'
        ]
    if(task.id){
        query.push('where id='+task.id )
    }
    if(task.years){
        if(Array.isArray(task.years)){
            query.push('and extract( year from vv.version) in ('+task.years.join(',')+')')
        }else{
            query.push('and extract( year from vv.version) >= '+task.years)
        }
    }
    var group = ['   group by v.id, v.name, v.cal_pm, v.abs_pm, v.latitude, v.longitude, vv.lanes, vv.segment_length,'
                ,'         vf.freeway_id, vf.freeway_dir, vt.type_id, vd.district_id, vyear, g.gid'
                ,' ) s LEFT OUTER JOIN geom_points_4326 gg USING (gid)'
                ,'ORDER BY id, vyear']
    query = query.join(' ') + ' '+ group.join(' ')

    var vds_version_data = {}

    return function(err,client,done){
        if(err) {
            console.log(err)
            next(err)
            return done()
        }
        var result = client.query(query)

        result.on('row',function(row){
            var vdsid = row.id
            var year = row.year
            if(vds_version_data[vdsid]===undefined){
                vds_version_data[vdsid]={}
            }
            if(vds_version_data[vdsid][year]===undefined){
                vds_version_data[vdsid][year]=[]
            }
            var targetval = {}
            //console.log(row)
            _.each(row
                   ,function(val,key){
                       var savearray
                      if(key === 'versions'){
                          //console.log(val)
                          savearray = []
                          val.forEach(function(d){
                              // current node-pg converts timestamp
                              // to date objects?
                              savearray.push(d.getFullYear() + '-'
                                             + pad2(d.getMonth() + 1) + '-'
                                             + pad2(d.getDate()))
                              return null
                          })
                          //console.log(verarray)
                          targetval[key]=savearray.sort()
                      }
                       else if(key === 'geojson'){
                           targetval[key]=JSON.parse(val)
                       }
                       else if(key !== 'id' && key !== 'year'){
                           targetval[key]=val
                       }
                   })
            vds_version_data[vdsid][year].push(targetval)
            return null
        })

        var error_condition = false
        result.on('error',function(err){
            error_condition = true
            console.log('query error: ')
            console.log(err)
            return next(err)
        })
        result.on('end',function(){
            if(!error_condition){
                if(vds_version_data !== undefined && _.size(vds_version_data)>0){
                    task.vds_version_data =
                        vds_version_data
                }
                next(null,task)
            }
            done()
            return null
        })
        return null
    }

}
