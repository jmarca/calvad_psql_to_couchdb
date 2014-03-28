/**
 * vds_info_to_couch.js
 */
var sys = require('sys'),
    http = require('http'),
    pg = require('pg');
var  parseUrl = require('url').parse;
var _ = require('underscore')._;
var request = require('request');

var async = require('async');

var tcuser = process.env.COUCHDB_USER ;
var tcpass = process.env.COUCHDB_PASS ;
var tchost = process.env.COUCHDB_HOST ;
var tcport = process.env.COUCHDB_PORT || 5984;

exports.vds_version_select = function vds_version_select(options){

    var dbname = options.db || 'spatialvds';
    var host = options.host || '127.0.0.1';
    var user = options.username|| 'myname';
    var pass = options.password || 'secret';
    var port = options.port || 5432;
    var connectionString = 'pg://'+user+':'+pass+'@'+host+':'+port+'/'+dbname;
    var table = options.table || 'vds_id_all';

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
        };

    // make the select statement
    var cols = [];
    for (var key in select_properties){
        cols.push(key +' as '+ select_properties[key]);
    }
    var propcols = cols.join(',');

    var vds_version_data = {};

    function vds_version_select(next){
        // clear the target object
        vds_version_data = {};
        pg.connect(connectionString
                  , function(err, client) {
                        if(err){
                            console.log('connection error '+JSON.stringify(err));
                            next(err);
                            return;
                        }

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

                            //  // devel limit query
                            // ,' where id = 311831'
                            //  // devel limit query

                            ,'   group by v.id, v.name, v.cal_pm, v.abs_pm, v.latitude, v.longitude, vv.lanes, vv.segment_length,'
                            ,'         vf.freeway_id, vf.freeway_dir, vt.type_id, vd.district_id, vyear, g.gid'
                            ,' ) s LEFT OUTER JOIN geom_points_4326 gg USING (gid)'
                            ,'ORDER BY id, vyear'
                            ].join(' ');
                        console.log(query);
                        var result = client.query(query);
                        // add an error callback to the PostgreSQL call
                        result.on('error', function(err){
                                      console.log("request error" + JSON.stringify(err));
                                      throw new Error (err);
                                  });
                        var versionmatcher = /(\d{4}-\d{2}-\d{2})/;
                        // create a row by row handler
                        result.on('row'
                                 ,function(row) {
                                      var vdsid = row.id;
                                      var year = row.year;
                                      if(vds_version_data[vdsid]===undefined){
                                          vds_version_data[vdsid]={};
                                      }
                                      if(vds_version_data[vdsid][year]===undefined){
                                          vds_version_data[vdsid][year]=[];
                                      }
                                      var targetval = {}
                                      // must copy to be safe
                                      _.each(row
                                            ,function(val,key){
                                                 if(key === 'versions'){
                                                     var verarray = val.split(',');
                                                     verarray = _(verarray)
                                                                    .chain()
                                                                    .map(function(v){
                                                                        var match = versionmatcher.exec(v);
                                                                        return match[1];
                                                                    })
                                                                    .value();
                                                     targetval[key]=verarray.sort();
                                                 }
                                                 else if(key === 'geojson'){
                                                     targetval[key]=JSON.parse(val);
                                                 }
                                                 else if(key !== 'id' && key !== 'year'){
                                                     targetval[key]=val;
                                                 }
                                             });
                                      vds_version_data[vdsid][year].push(targetval);
                                  });

                        result.on('end'
                                 , function(){
                                       console.log('done with postgres query')
                                       next();
                                   }
                                 );

                    });

        return 1;
    };

    vds_version_select.get_vds_version_data = function(){
        return vds_version_data;
    }

    var uri = 'http://'+tchost+':'+tcport+'/vdsdata%2ftracking';

    function modify_doc(vdsid,next){
        var doc;
        var url = [uri,vdsid].join('/');
        console.log('couchdb url is '+url);


        function do_put(err){
            // all done
            request({ 'method' : 'PUT'
                    ,'uri' : url
                    ,'headers' : { 'authorization' : "Basic " + new Buffer(tcuser + ":" + tcpass).toString('base64')
                                 ,'content-type': 'application/json'
                                 ,'accept':'application/json'
                                 }
                    ,'body':JSON.stringify(doc)
                    }
                   );
            next(); // let caller know we're done here
        }

        function get_handler(e,r,b){
            if(e) handle_err(e,r,b);
            doc = JSON.parse(b);
            var verinfo = vds_version_data[vdsid];
            async.forEach(Object.keys(verinfo)
                         ,function(year,_cb){
                              if(doc[year]===undefined){
                                  doc[year]={};
                              }
                              // stomp on what is already there
                              doc[year].properties=verinfo[year];

                              _cb();
                          }
                         ,do_put);
            }

        var x = request({'method' : 'GET'
                        ,'uri' : url
                        ,'headers' : { 'authorization' : "Basic " + new Buffer(tcuser + ":" + tcpass).toString('base64')
                                     ,'content-type': 'application/json'
                                     ,'accept':'application/json'
                                     }
                        }
                       ,get_handler);

    }


    vds_version_select.pass_along = function(next){

        // write vds_version_data to the vdsdata/tracking couchdb docs
        // sift thought the ids, get the docs, write the data, return
        // from past experience, should only do 30 or so at a time so
        // as not to overwhelm the max open dbs on couchdb, but I
        // think this approach will flood the request queue and it
        // will just work out okay

        // get a doc, pipes to write a doc, with a handler in between
        // that adds the yearly version data

        async.forEach(Object.keys(vds_version_data)
                     ,modify_doc
                     ,function(err){
                          console.log(err + ' all done');
                          next();
                      })
    }

    return vds_version_select;
};
