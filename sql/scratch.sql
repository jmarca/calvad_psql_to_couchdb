-- need a view or query to get "version"

-- currently, for vds_active, I have:
CREATE OR REPLACE VIEW vds_active AS
with
in_out as (
   select id,min(version) as deployed,max(version) as latest,district_id as district from vds_versioned v join vds_district d on v.id = d.vds_id group by id,district
),
in_out_details as (
   select i.id,deployed,latest,district,   -- and now double join for the segement lengths, etc per version
    vvd.lanes as deployed_lanes,
    vvd.segment_length as deployed_length,
    vvr.segment_length as latest_length,
    vvd.lanes as latest_lanes
   FROM in_out i
   JOIN vds_versioned vvd on (i.id=vvd.id and i.deployed=vvd.version)
   JOIN vds_versioned vvr on (i.id=vvr.id and i.latest=vvr.version)
),
district_updates as (
   SELECT distinct vds_district.district_id as district, max(vds_versioned.version) AS version  FROM vds_versioned  JOIN vds_district ON vds_versioned.id = vds_district.vds_id group by district
),
service_history as (
   select l.id,deployed,version as latest_update,deployed_lanes,deployed_length,latest_lanes,latest_length,l.district
   from in_out_details l
   join district_updates d on (l.district = d.district)
   where l.latest = d.version
)
SELECT v.id,
    v.name,
    v.cal_pm,
    v.abs_pm,
    v.latitude,
    v.longitude,
    vf.freeway_id,
    vf.freeway_dir,
    vt.type_id AS vdstype,
    deployed,latest_update,deployed_lanes,deployed_length,latest_lanes,latest_length,district,
    g.gid,
    g.geom,
    regexp_replace(v.cal_pm::text, '[^[:digit:]^\.]'::text, ''::text, 'g'::text)::numeric AS cal_pm_numeric
   FROM vds_id_all v
   left outer JOIN vds_points_4326 ON v.id = vds_points_4326.vds_id
   left outer JOIN vds_vdstype vt USING (vds_id)
   left outer JOIN vds_freeway vf USING (vds_id)
   left outer JOIN geom_points_4326 g USING (gid)
   -- now sort out the earliest and latest
   JOIN service_history s using (id)
   order by v.id;

-- now, modify so that all versions are merged together
CREATE OR REPLACE VIEW vds_active AS
with
in_out as (
   select id,min(version) as deployed,max(version) as latest,district_id as district from vds_versioned v join vds_district d on v.id = d.vds_id group by id,district
),
in_out_details as (
   select i.id,deployed,latest,district,   -- and now double join for the segement lengths, etc per version
    vvd.lanes as deployed_lanes,
    vvd.segment_length as deployed_length,
    vvr.segment_length as latest_length,
    vvd.lanes as latest_lanes
   FROM in_out i
   JOIN vds_versioned vvd on (i.id=vvd.id and i.deployed=vvd.version)
   JOIN vds_versioned vvr on (i.id=vvr.id and i.latest=vvr.version)
),
district_updates as (
   SELECT distinct vds_district.district_id as district, max(vds_versioned.version) AS version  FROM vds_versioned  JOIN vds_district ON vds_versioned.id = vds_district.vds_id group by district
),
service_history as (
   select l.id,deployed,version as latest_update,deployed_lanes,deployed_length,latest_lanes,latest_length,l.district
   from in_out_details l
   join district_updates d on (l.district = d.district)
   where l.latest = d.version
)
SELECT v.id,
    v.name,
    v.cal_pm,
    v.abs_pm,
    v.latitude,
    v.longitude,
    vf.freeway_id,
    vf.freeway_dir,
    vt.type_id AS vdstype,
    deployed,latest_update,deployed_lanes,deployed_length,latest_lanes,latest_length,district,
    g.gid,
    g.geom,
    regexp_replace(v.cal_pm::text, '[^[:digit:]^\.]'::text, ''::text, 'g'::text)::numeric AS cal_pm_numeric
   FROM vds_id_all v
   left outer JOIN vds_points_4326 ON v.id = vds_points_4326.vds_id
   left outer JOIN vds_vdstype vt USING (vds_id)
   left outer JOIN vds_freeway vf USING (vds_id)
   left outer JOIN geom_points_4326 g USING (gid)
   -- now sort out the earliest and latest
   JOIN service_history s using (id)
   order by v.id;

-- old query was

                            ['SELECT '+propcols
                            ,'from('
                            ,' SELECT v.id, v.name, v.cal_pm, v.abs_pm, v.latitude, v.longitude, vv.lanes, vv.segment_length,'
                            ,'         array_agg(vv.version) AS versions,  extract( year from vv.version) as vyear, '
                            ,'         vf.freeway_id, vf.freeway_dir, vt.type_id AS vdstype, vd.district_id AS district,'
                            ,'         g.gid'
                            ,'   FROM vds_id_all v'
                            ,'   JOIN vds_versioned vv USING (id)'
                            ,'   JOIN vds_points_4326 ON v.id = vds_points_4326.vds_id'
                            ,'   JOIN vds_vdstype vt USING (vds_id)'
                            ,'   JOIN vds_district vd USING (vds_id)'
                            ,'   JOIN vds_freeway vf USING (vds_id)'
                            ,'   JOIN geom_points_4326 g USING (gid)'

                            //  // devel limit query
                            // ,' where id = 311831'
                            //  // devel limit query

                            ,'   group by v.id, v.name, v.cal_pm, v.abs_pm, v.latitude, v.longitude, vv.lanes, vv.segment_length,'
                            ,'         vf.freeway_id, vf.freeway_dir, vt.type_id, vd.district_id, vyear, g.gid'
                            ,' ) s join geom_points_4326 gg USING (gid)'
                            ,'ORDER BY id, vyear'


-- actually, that works fine for me, but for the addition of left
-- outer in front of the various joins to include cases where some
-- extra data is missing.
