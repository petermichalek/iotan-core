/** 
 * Populate tables for test purposes

 To run:
 cqlsh localhost  -k {keyspace} < db/populate.cql
 or
 cqlsh IP_ADDRESS -k {keyspace} < db/populate.cql

*/

// ---------------------------------
// --- project table
// ---------------------------------


// --- test-project
// ---------------------------------

INSERT INTO project (id, created, mod, author, deleted, pid, name, nominalInterval, tz,
    geoAddr, geoCity, geoZip,
    description, tags, groups, json)
      VALUES (
          856716f7-2e54-4715-9f00-91dcbea6c101,
          '2016-11-17',
          '2016-11-17',
          'sysdeamon',
          false,
          'test-project',
          'Test project 01',
          900,
          'America/Los_Angeles',
          '1161 W Georgia St, Vancouver, BC V6E 0C6, Canada',
          'Vancouver',
          'V6E 0C6',
          'This is a test project #1',
          {'test', 'regionNW'},
          {'iotus-admin-group': 'rwx', 'guest-group': 'r'},
          '{"dis": "test 01", "custId": "targetCorp"}')
        ;


INSERT INTO project (id, created, mod, author, deleted, pid, name, nominalInterval, tz,
    geoAddr, geoCity, geoZip,
    description, tags, groups, json)
      VALUES (
          856716f7-2e54-4715-9f00-91dcbea6c102,
          '2016-11-17',
          '2016-11-17',
          'sysdeamon',
          false,
          'demo',
          'Demo',
          // 5 minutes interval
          300,
          'America/New_York',
          '725 5th Ave, New York, NY 10022',
          'New York',
          '10022',
          'This is the demo project #1',
          {'demo', 'regionNE'},
          {'iotus-admin-group': 'rwx', 'guest-group': 'r'},
          '{"dis": "demo 01", "custId": "acnecorp"}')
        ;

INSERT INTO project (id, created, mod, author, deleted, pid, name, nominalInterval, tz,
    geoAddr, geoCity, geoZip,
    description, tags, groups, json)
      VALUES (
          856716f7-2e54-4715-9f00-91dcbea6c103,
          '2016-11-17',
          '2016-11-17',
          'sysdeamon',
          false,
          'demo-02',
          'Demo #2',
          // 5 minutes interval
          300,
          'America/New_York',
          '401 N Wabash Ave, Chicago, IL 60611',
          'Chicago',
          '60611',
          'This is the demo project #2',
          {'demo', 'regionMW'},
          {'iotus-admin-group': 'rwx', 'guest-group': 'r'},
          '{"dis": "demo 02", "custId": "acnecorp"}')
        ;

// ---------------------------------
// --- users
// ---------------------------------
// password may be of two kinds:
// SSHA or pbkdf2

//slappasswd -s frank123
//{SSHA}V9ezVqAXEy1QVjGsh7cSfRNO8ITYk5uz
INSERT INTO user (id, created, mod, author, deleted, email, firstname, lastname, password)
      VALUES (
          856716f7-2e54-4715-9f00-91dcbea66661,
          '2017-01-23',
          '2017-01-23',
          'sysdeamon',
          false,
          'frank@michalek.org',
          'Frank',
          'Sinatra',
          '{SSHA}V9ezVqAXEy1QVjGsh7cSfRNO8ITYk5uz')
        ;


//  slappasswd -s suzie123
//   {SSHA}8eP5MgVaUX49xKa3G6T16yeSq+CdjW0j
//slappasswd -s suzie123
//{SSHA}XsvoXcthXhcmQjubJG74vjo1OYoGlR/b

INSERT INTO user (id, created, mod, author, deleted, email, firstname, lastname, password)
      VALUES (
          856716f7-2e54-4715-9f00-91dcbea66662,
          '2017-01-23',
          '2017-01-23',
          'sysdeamon',
          false,
          'suzie@michalek.org',
          'Suzie',
          'Eizus',
          '{SSHA}8eP5MgVaUX49xKa3G6T16yeSq+CdjW0j'
          )
        ;

INSERT INTO user (id, created, mod, author, deleted, email, firstname, lastname, password)
      VALUES (
          856716f7-2e54-4715-9f00-91dcbea66663,
          '2017-01-23',
          '2017-01-23',
          'sysdeamon',
          false,
          'fred@michalek.org',
          'Fred',
          'Astair',
          'pbkdf2(1000,20,sha512)$bd8848decc977593$9fb52e83f7271355fb06d4fb86382b35f96b0a5e'
          )
        ;

INSERT INTO user (id, created, mod, author, deleted, email, firstname, lastname, password)
      VALUES (
          856716f7-2e54-4715-9f00-91dcbea66664,
          '2017-01-23',
          '2017-01-23',
          'sysdeamon',
          false,
          'rosa@michalek.org',
          'Rosa',
          'Asor',
          'pbkdf2(1000,20,sha512)$ad75f71b70cc23f7$e34b95270e6886c49ebfd3c1d5d2711a69706dab'
          )
        ;


// ---------------------------------
// --------- nodes -----------------
// ---------------------------------

// ---------------------------------
// --- nodes for test-project
// ---------------------------------

INSERT INTO node (id, created, mod, author, deleted, pid, tags, props, propsn)
      VALUES (
          756716f7-2e54-4715-9f00-91dcbea6c100,
          '2016-11-11',
          '2016-11-11',
          'sysdeamon',
          false,
          'test-project',
          {'point', 'his'},
          {'dis': 'test 01', 'equipRef': '756716f7-2e54-4715-9f01-91dcbea6c300', 'kind': 'Number', 'tz': 'Los_Angeles'},
          {}
        );

INSERT INTO node (id, created, mod, author, deleted, pid, tags, props, propsn)
      VALUES (
          756716f7-2e54-4715-9f00-91dcbea6c101,
          '2016-11-11T01:00:00-0800',
          '2016-11-11T01:00:00-0800',
          'sysdeamon',
          false,
          'test-project',
          {'point', 'his'},
          {'dis': 'test pt 02', 'equipRef': '756716f7-2e54-4715-9f01-91dcbea6c300',
            'kind': 'Number', 'tz': 'Los_Angeles'},
          {'hisSize': 10}
        );

INSERT INTO node (id, created, mod, author, deleted, pid, tags, props, propsn)
      VALUES (
          756716f7-2e54-4715-9f00-91dcbea6c102,
          '2016-11-11T01:00:00-0800',
          '2016-11-11T01:00:00-0800',
          'sysdeamon',
          false,
          'test-project',
          {'point', 'his'},
          {'dis': 'test pt 03', 'equipRef': '756716f7-2e54-4715-9f01-91dcbea6c300',
                       'kind': 'Number', 'tz': 'Los_Angeles'},
          {'hisSize': 10}
        );


INSERT INTO node (id, created, mod, author, deleted, pid, tags, props, propsn)
      VALUES (
          756716f7-2e54-4715-9f00-91dcbea6c103,
          '2017-01-11T01:00:00-0800',
          '2017-01-11T01:00:00-0800',
          'sysdeamon',
          false,
          'test-project',
          {'point', 'his'},
          {'dis': 'test pt 04 (c103)', 'equipRef': '756716f7-2e54-4715-9f01-91dcbea6c300',
                       'kind': 'Number', 'tz': 'Los_Angeles'},
          {'hisSize': 10, 'hisVal': 73.2}
          // bool: {'mybool': true}
        );

INSERT INTO node (id, created, mod, author, deleted, pid, tags, props, propsn, propsb)
      VALUES (
          756716f7-2e54-4715-9f00-91dcbea6c104,
          '2017-01-11T01:00:00-0800',
          '2017-01-11T01:00:00-0800',
          'sysdeamon',
          false,
          'test-project',
          {'point', 'his'},
          {'dis': 'test pt 05 (c104)', 'equipRef': '756716f7-2e54-4715-9f01-91dcbea6c300',
                       'kind': 'Number', 'tz': 'Los_Angeles'},
          {'hisSize': 12, 'hisVal': 68.5},
          {'mybool': true}
        );

INSERT INTO node (id, created, mod, author, deleted, pid, tags, props, propsn)
      VALUES (
          756716f7-2e54-4715-9f00-91dcbea6c300,
          '2016-11-11T02:00:00-0800',
          '2016-11-11T02:00:00-0800',
          'sysdeamon',
          false,
          'test-project',
          {'equip'},
          {'dis': 'equip 01', 'siteRef': '756716f7-2e54-4715-9f01-91dcbea6c400', 'tz': 'Los_Angeles'},
          {}
        );

INSERT INTO node (id, created, mod, author, deleted, pid, tags, props, propsn)
      VALUES (
          756716f7-2e54-4715-9f00-91dcbea6c400,
          '2016-11-11T02:00:00-0800',
          '2016-11-11T02:00:00-0800',
          'sysdeamon',
          false,
          'test-project',
          {'site'},
          {'dis': 'site 01', 'tz': 'Los_Angeles'},
          {}
        );


INSERT INTO node (id, created, mod, author, deleted, pid, tags, props, propsn)
      VALUES (
          756716f7-2e54-4715-9f00-91dcbea6c401,
          '2016-11-11T02:00:00-0800',
          '2016-11-11T02:00:00-0800',
          'sysdeamon',
          false,
          'test-project',
          {'site'},
          {'dis': 'site 02', 'tz': 'Los_Angeles'},
          {}
        );



// ---------------------------------
// --- nodes for demo project
// ---------------------------------

INSERT INTO node (id, created, mod, author, deleted, pid, tags, props, propsn)
      VALUES (
          756716f7-2e54-4715-9f01-91dcbea6c111,
          '2016-12-06T05:49:19-0800',
          '2016-12-06T05:49:19-0800',
          'sysdeamon',
          false,
          'demo',
          {'point', 'his'},
          {'dis': 'demo pt 01', 'equipRef': '756716f7-2e54-4715-9f01-91dcbea6c555', 'kind': 'Number', 'tz': 'New_York'},
          {}
        );

INSERT INTO node (id, created, mod, author, deleted, pid, tags, props, propsn)
      VALUES (
          756716f7-2e54-4715-9f01-91dcbea6c222,
          '2016-12-06T05:51:01.001-0800',
          '2016-12-06T05:51:01.001-0800',
          'sysdeamon',
          false,
          'demo',
          {'point', 'his'},
          {'dis': 'demo pt 02', 'equipRef': '756716f7-2e54-4715-9f01-91dcbea6c555', 'kind': 'Number', 'tz': 'New_York'},
          {}
        );

INSERT INTO node (id, created, mod, author, deleted, pid, tags, props, propsn)
      VALUES (
          756716f7-2e54-4715-9f01-91dcbea6c223,
          '2016-12-06T05:51:01.002-0800',
          '2016-12-06T05:51:01.002-0800',
          'sysdeamon',
          false,
          'demo',
          {'point', 'his'},
          {'dis': 'demo pt 03', 'equipRef': '756716f7-2e54-4715-9f01-91dcbea6c555', 'kind': 'Number', 'tz': 'New_York'},
          {}
        );

INSERT INTO node (id, created, mod, author, deleted, pid, tags, props, propsn)
      VALUES (
          756716f7-2e54-4715-9f01-91dcbea6c105,
          '2016-12-06T05:51:01.003-0800',
          '2016-12-06T05:51:01.003-0800',
          'sysdeamon',
          false,
          'demo',
          {'equip'},
          {'dis': 'demo equip 01', 'siteRef': '756716f7-2e54-4715-9f01-91dcbea6c333', 'tz': 'New_York'},
          {}
        );

INSERT INTO node (id, created, mod, author, deleted, pid, tags, props, propsn)
      VALUES (
          756716f7-2e54-4715-9f01-91dcbea6c203,
          '2016-12-06T05:51:01.003-0800',
          '2016-12-06T05:51:01.003-0800',
          'sysdeamon',
          false,
          'demo',
          {'site'},
          {'dis': 'demo site 01',  'tz': 'New_York'},
          {}
        );

INSERT INTO node (id, created, mod, author, deleted, pid, tags, props, propsn)
      VALUES (
          756716f7-2e54-4715-9f01-91dcbea6c204,
          '2016-12-06T05:51:01.004-0800',
          '2016-12-06T05:51:01.004-0800',
          'sysdeamon',
          false,
          'demo',
          {'site'},
          {'dis': 'demo site 02',  'tz': 'New_York'},
          {}
        );


// ---------------------------------
// --- reading/measurement table for test-project
// ---------------------------------
/*
select id, project from node;

 id                                   | project
--------------------------------------+--------------
 756716f7-2e54-4715-9f00-91dcbea6c300 | test-project
 756716f7-2e54-4715-9f00-91dcbea6c401 | test-project
 756716f7-2e54-4715-9f00-91dcbea6c101 | test-project
 756716f7-2e54-4715-9f00-91dcbea6c100 | test-project
 756716f7-2e54-4715-9f00-91dcbea6c400 | test-project

*/

/*
TTL 5 years, including possibly two extra days:
86400 * ((365 * 5) + 2)
157852800

*/

// 2014

INSERT INTO measurement (
  id, pid, ts, val)
  VALUES (
    '756716f7-2e54-4715-9f00-91dcbea6c100',
    'test-project',
    '2014-10-23T19:00:13.260-07:00',
    '70.8') USING TTL 157852800;

INSERT INTO measurement (
  id, pid, ts, val)
  VALUES (
    '756716f7-2e54-4715-9f00-91dcbea6c100',
    'test-project',
    '2014-10-23T19:15:00.000-07:00',
    '71.2') USING TTL 157852800;


INSERT INTO measurement (
  id, pid, ts, val)
  VALUES (
    '756716f7-2e54-4715-9f00-91dcbea6c100',
    'test-project',
    '2014-10-23T19:30:00.000-07:00',
    '71.4') USING TTL 157852800;

INSERT INTO measurement (
  id, pid, ts, val)
  VALUES (
    '756716f7-2e54-4715-9f00-91dcbea6c100',
    'test-project',
    '2014-11-17T00:00:00.000-08:00',
    '69.4') USING TTL 157852800;

INSERT INTO measurement (
  id, pid, ts, val)
  VALUES (
    '756716f7-2e54-4715-9f00-91dcbea6c100',
    'test-project',
    '2014-11-17T00:15:00.000-08:00',
    '69.1') USING TTL 157852800;

INSERT INTO measurement (
  id, pid, ts, val)
  VALUES (
    '756716f7-2e54-4715-9f00-91dcbea6c100',
    'test-project',
    '2014-11-17T04:30:00.000-08:00',
    '66.5') USING TTL 157852800;

// 2015

INSERT INTO measurement (
  id, pid, ts, val)
  VALUES (
    '756716f7-2e54-4715-9f00-91dcbea6c100',
    'test-project',
    '2015-10-23T19:00:13.260-07:00',
    '70.8') USING TTL 157852800;

INSERT INTO measurement (
  id, pid, ts, val)
  VALUES (
    '756716f7-2e54-4715-9f00-91dcbea6c100',
    'test-project',
    '2015-10-23T19:15:00.000-07:00',
    '71.2') USING TTL 157852800;


INSERT INTO measurement (
  id, pid, ts, val)
  VALUES (
    '756716f7-2e54-4715-9f00-91dcbea6c100',
    'test-project',
    '2015-10-23T19:30:00.000-07:00',
    '71.4') USING TTL 157852800;

INSERT INTO measurement (
  id, pid, ts, val)
  VALUES (
    '756716f7-2e54-4715-9f00-91dcbea6c100',
    'test-project',
    '2015-11-17T00:00:00.000-08:00',
    '69.4') USING TTL 157852800;

INSERT INTO measurement (
  id, pid, ts, val)
  VALUES (
    '756716f7-2e54-4715-9f00-91dcbea6c100',
    'test-project',
    '2015-11-17T00:15:00.000-08:00',
    '69.1') USING TTL 157852800;

INSERT INTO measurement (
  id, pid, ts, val)
  VALUES (
    '756716f7-2e54-4715-9f00-91dcbea6c100',
    'test-project',
    '2015-11-17T04:30:00.000-08:00',
    '66.5') USING TTL 157852800;



// 2016

INSERT INTO measurement (
  id, pid, ts, val)
  VALUES (
    '756716f7-2e54-4715-9f00-91dcbea6c100',
    'test-project',
    '2016-10-23T19:00:13.260-07:00',
    '70.8') USING TTL 157852800;

INSERT INTO measurement (
  id, pid, ts, val)
  VALUES (
    '756716f7-2e54-4715-9f00-91dcbea6c100',
    'test-project',
    '2016-10-23T19:15:00.000-07:00',
    '71.2') USING TTL 157852800;


INSERT INTO measurement (
  id, pid, ts, val)
  VALUES (
    '756716f7-2e54-4715-9f00-91dcbea6c100',
    'test-project',
    '2016-10-23T19:30:00.000-07:00',
    '71.4') USING TTL 157852800;

INSERT INTO measurement (
  id, pid, ts, val)
  VALUES (
    '756716f7-2e54-4715-9f00-91dcbea6c100',
    'test-project',
    '2016-11-17T00:00:00.000-08:00',
    '69.4') USING TTL 157852800;

INSERT INTO measurement (
  id, pid, ts, val)
  VALUES (
    '756716f7-2e54-4715-9f00-91dcbea6c100',
    'test-project',
    '2016-11-17T00:15:00.000-08:00',
    '69.1') USING TTL 157852800;

INSERT INTO measurement (
  id, pid, ts, val)
  VALUES (
    '756716f7-2e54-4715-9f00-91dcbea6c100',
    'test-project',
    '2016-11-17T04:30:00.000-08:00',
    '66.5') USING TTL 157852800;

// PT3



// 2016

INSERT INTO measurement (
  id, pid, ts, val)
  VALUES (
    '756716f7-2e54-4715-9f00-91dcbea6c102',
    'test-project',
    '2016-10-23T19:00:13.260-07:00',
    '70.9') USING TTL 157852800;

INSERT INTO measurement (
  id, pid, ts, val)
  VALUES (
    '756716f7-2e54-4715-9f00-91dcbea6c102',
    'test-project',
    '2016-10-23T19:15:00.000-07:00',
    '71.3') USING TTL 157852800;


INSERT INTO measurement (
  id, pid, ts, val)
  VALUES (
    '756716f7-2e54-4715-9f00-91dcbea6c102',
    'test-project',
    '2016-10-23T19:30:00.000-07:00',
    '71.5') USING TTL 157852800;

INSERT INTO measurement (
  id, pid, ts, val)
  VALUES (
    '756716f7-2e54-4715-9f00-91dcbea6c102',
    'test-project',
    '2016-11-17T00:00:00.000-08:00',
    '69.6') USING TTL 157852800;

INSERT INTO measurement (
  id, pid, ts, val)
  VALUES (
    '756716f7-2e54-4715-9f00-91dcbea6c102',
    'test-project',
    '2016-11-17T00:15:00.000-08:00',
    '69.3') USING TTL 157852800;

INSERT INTO measurement (
  id, pid, ts, val)
  VALUES (
    '756716f7-2e54-4715-9f00-91dcbea6c102',
    'test-project',
    '2016-11-17T00:15:00.000-08:00',
    '66.7') USING TTL 157852800;

