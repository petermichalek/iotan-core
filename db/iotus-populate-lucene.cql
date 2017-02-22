/** 
 * Populate tables for test purposes

 Experiments with lucene based schema

*/


// Note: this is now obsolete and was incorporated into the main file iotus-populate.cql

USE iotus;

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

