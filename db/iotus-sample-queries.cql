/** 
 * Sample queries to run after populate.

*/

USE iotus;

(2 rows)
cqlsh:iotus> select id, json from node where tags CONTAINS 'site' and project='test-project' ALLOW FILTERING;

 id                                   | json
--------------------------------------+--------------------
 756716f7-2e54-4715-9f00-91dcbea6c401 | {"dis": "site 02"}
 756716f7-2e54-4715-9f00-91dcbea6c400 | {"dis": "site 01"}

(2 rows)
cqlsh:iotus> select id, json from node where tags CONTAINS 'point' and project='test-project' ALLOW FILTERING;

 id                                   | json
--------------------------------------+-------------------------------------------------------------------------------------------------------
 756716f7-2e54-4715-9f00-91dcbea6c101 | {"dis": "test pt 02", "equipRef": "756716f7-2e54-4715-9f01-91dcbea6c300",\n            "hisSize": 10}
 756716f7-2e54-4715-9f00-91dcbea6c100 |                                {"dis": "test 01", "equipRef": "756716f7-2e54-4715-9f01-91dcbea6c300"}

(2 rows)
cqlsh:iotus> select id, json from node where tags CONTAINS 'equip' and project='test-project' ALLOW FILTERING;

 id                                   | json
--------------------------------------+------------------------------------------------------------------------
 756716f7-2e54-4715-9f00-91dcbea6c300 | {"dis": "equip 01", "siteRef": "756716f7-2e54-4715-9f01-91dcbea6c400"}

(1 rows)



/** Sample queries for project

*/

cqlsh:iotus> select id, pid, tz, nominalInterval from project;

 id                                   | pid          | tz                  | nominalinterval
--------------------------------------+--------------+---------------------+-----------------
 856716f7-2e54-4715-9f00-91dcbea6c103 |      demo-02 |    America/New_York |             300
 856716f7-2e54-4715-9f00-91dcbea6c101 | test-project | America/Los_Angeles |             900
 856716f7-2e54-4715-9f00-91dcbea6c102 |         demo |    America/New_York |             300

(3 rows)
cqlsh:iotus> CREATE INDEX project_tags_key ON project (tags);

cqlsh:iotus> select id, pid, tz, nominalInterval from project where tags contains 'demo';

 id                                   | pid     | tz               | nominalinterval
--------------------------------------+---------+------------------+-----------------
 856716f7-2e54-4715-9f00-91dcbea6c103 | demo-02 | America/New_York |             300
 856716f7-2e54-4715-9f00-91dcbea6c102 |    demo | America/New_York |             300

(2 rows)
cqlsh:iotus> select id, pid, tz, nominalInterval from project where tags contains 'test';

 id                                   | pid          | tz                  | nominalinterval
--------------------------------------+--------------+---------------------+-----------------
 856716f7-2e54-4715-9f00-91dcbea6c101 | test-project | America/Los_Angeles |             900

(1 rows)


/** Sample queries for measurement/reading, including bad queries

*/


cqlsh:iotus> select * from measurement where id='756716f7-2e54-4715-9f00-91dcbea6c300' and ts >= '2015-11-17T04:30:00.000-08:00' and pid='test-project';

 id                                   | ts                       | pid          | val
--------------------------------------+--------------------------+--------------+------
 756716f7-2e54-4715-9f00-91dcbea6c300 | 2015-11-17 04:30:00-0800 | test-project | 66.5
 756716f7-2e54-4715-9f00-91dcbea6c300 | 2016-10-23 19:00:13-0700 | test-project | 70.8
 756716f7-2e54-4715-9f00-91dcbea6c300 | 2016-10-23 19:15:00-0700 | test-project | 71.2
 756716f7-2e54-4715-9f00-91dcbea6c300 | 2016-10-23 19:30:00-0700 | test-project | 71.4
 756716f7-2e54-4715-9f00-91dcbea6c300 | 2016-11-17 00:00:00-0800 | test-project | 69.4
 756716f7-2e54-4715-9f00-91dcbea6c300 | 2016-11-17 00:15:00-0800 | test-project | 69.1
 756716f7-2e54-4715-9f00-91dcbea6c300 | 2016-11-17 04:30:00-0800 | test-project | 66.5


 /** Sample queries for measurement/reading below experimental when trying to make 'date' column part of clustering
*/

select * from measurement;

 id                                   | date       | ts                       | pid          | val
--------------------------------------+------------+--------------------------+--------------+------
 756716f7-2e54-4715-9f00-91dcbea6c300 | 2014-10-23 | 2014-10-23 17:00:13-0700 | test-project | 70.8
 756716f7-2e54-4715-9f00-91dcbea6c300 | 2014-10-23 | 2014-10-23 17:15:00-0700 | test-project | 71.2
 756716f7-2e54-4715-9f00-91dcbea6c300 | 2016-11-17 | 2016-11-16 23:00:00-0800 | test-project | 69.4
 756716f7-2e54-4715-9f00-91dcbea6c300 | 2016-11-17 | 2016-11-16 23:15:00-0800 | test-project | 69.1
 756716f7-2e54-4715-9f00-91dcbea6c300 | 2016-11-17 | 2016-11-17 03:30:00-0800 | test-project | 66.5

(5 rows)
cqlsh:iotus> select * from measurement where id = 756716f7-2e54-4715-9f00-91dcbea6c300;
code=2200 [Invalid query] message="Partition key part date must be restricted since preceding part is"
cqlsh:iotus> select * from measurement where id = 756716f7-2e54-4715-9f00-91dcbea6c300 and date = '2016-11-17';

 id                                   | date       | ts                       | pid          | val
--------------------------------------+------------+--------------------------+--------------+------
 756716f7-2e54-4715-9f00-91dcbea6c300 | 2016-11-17 | 2016-11-16 23:00:00-0800 | test-project | 69.4
 756716f7-2e54-4715-9f00-91dcbea6c300 | 2016-11-17 | 2016-11-16 23:15:00-0800 | test-project | 69.1
 756716f7-2e54-4715-9f00-91dcbea6c300 | 2016-11-17 | 2016-11-17 03:30:00-0800 | test-project | 66.5

(3 rows)
cqlsh:iotus> select * from measurement where id = 756716f7-2e54-4715-9f00-91dcbea6c300 and ts > '2016-11-17';
code=2200 [Invalid query] message="Partition key part date must be restricted since preceding part is"
cqlsh:iotus> select * from measurement where id = 756716f7-2e54-4715-9f00-91dcbea6c300 and ts > '2016-11-17' and date > '2016-11-17';
code=2200 [Invalid query] message="Only EQ and IN relation are supported on the partition key (unless you use the token() function)"
cqlsh:iotus> select * from measurement where id = 756716f7-2e54-4715-9f00-91dcbea6c300 and ts > '2016-11-17' and date = '2016-11-17';

 id                                   | date       | ts                       | pid          | val
--------------------------------------+------------+--------------------------+--------------+------
 756716f7-2e54-4715-9f00-91dcbea6c300 | 2016-11-17 | 2016-11-17 03:30:00-0800 | test-project | 66.5

(1 rows)
cqlsh:iotus> select * from measurement where id = 756716f7-2e54-4715-9f00-91dcbea6c300 and ts > '2016-11-17' and date in ('2016-11-17', '2016-11-18');

 id                                   | date       | ts                       | pid          | val
--------------------------------------+------------+--------------------------+--------------+------
 756716f7-2e54-4715-9f00-91dcbea6c300 | 2016-11-17 | 2016-11-17 03:30:00-0800 | test-project | 66.5

cqlsh:iotus> select * from measurement where id = 756716f7-2e54-4715-9f00-91dcbea6c300 and ts > '2016-11-16' and date in ('2016-11-16', '2016-11-17', '2016-11-18');
 id                                   | date       | ts                       | pid          | val
--------------------------------------+------------+--------------------------+--------------+------
 756716f7-2e54-4715-9f00-91dcbea6c300 | 2016-11-17 | 2016-11-16 23:00:00-0800 | test-project | 69.4
 756716f7-2e54-4715-9f00-91dcbea6c300 | 2016-11-17 | 2016-11-16 23:15:00-0800 | test-project | 69.1
 756716f7-2e54-4715-9f00-91dcbea6c300 | 2016-11-17 | 2016-11-17 03:30:00-0800 | test-project | 66.5

(3 rows)

// this doesn't work as expected
select * from measurement where token(id, date) >= token(756716f7-2e54-4715-9f00-91dcbea6c300, '2016-11-16') and
   token(id, date) < token(756716f7-2e54-4715-9f00-91dcbea6c300, '2016-11-18') ;
 id | date | ts | pid | val
----+------+----+-----+-----

(0 rows)

// this doesn't work as expected

select * from measurement where token(id, date) > token(756716f7-2e54-4715-9f00-91dcbea6c300, '2016-11-15') and
   token(id, date) < token(756716f7-2e54-4715-9f00-91dcbea6c300, '2016-11-18') ;
 id | date | ts | pid | val
----+------+----+-----+-----

(0 rows)


select * from measurement where token(id, date) < token(756716f7-2e54-4715-9f00-91dcbea6c300, '2016-11-15');
---
select * from measurement where token(id, date) > token(756716f7-2e54-4715-9f00-91dcbea6c300, '2016-11-15') and
   token(id, date) < token(756716f7-2e54-4715-9f00-91dcbea6c300, '2016-11-18') ;


---
== experiment 11/18/16 with text-based id
// baseline/all record:
select token(id,date), id, date, ts, pid from measurement ;
cqlsh:iotus> select token(id,date), id, date, ts, pid from measurement ;

 token(id, date)      | id                                   | date       | ts                       | pid
----------------------+--------------------------------------+------------+--------------------------+--------------
 -7529827560707582294 | 756716f7-2e54-4715-9f00-91dcbea6c300 | 2015-11-17 | 2015-11-17 00:00:00-0800 |         null
 -7529827560707582294 | 756716f7-2e54-4715-9f00-91dcbea6c300 | 2015-11-17 | 2015-11-17 00:15:00-0800 | test-project
 -7529827560707582294 | 756716f7-2e54-4715-9f00-91dcbea6c300 | 2015-11-17 | 2015-11-17 04:30:00-0800 | test-project
 -7505241882025539133 | 756716f7-2e54-4715-9f00-91dcbea6c300 | 2016-10-23 | 2016-10-23 19:00:13-0700 | test-project
 -7505241882025539133 | 756716f7-2e54-4715-9f00-91dcbea6c300 | 2016-10-23 | 2016-10-23 19:15:00-0700 | test-project
 -7505241882025539133 | 756716f7-2e54-4715-9f00-91dcbea6c300 | 2016-10-23 | 2016-10-23 19:30:00-0700 | test-project
 -3887792216267834280 | 756716f7-2e54-4715-9f00-91dcbea6c300 | 2015-10-23 | 2015-10-23 19:00:13-0700 | test-project
 -3887792216267834280 | 756716f7-2e54-4715-9f00-91dcbea6c300 | 2015-10-23 | 2015-10-23 19:15:00-0700 | test-project
 -3887792216267834280 | 756716f7-2e54-4715-9f00-91dcbea6c300 | 2015-10-23 | 2015-10-23 19:30:00-0700 | test-project
  3954980817302725337 | 756716f7-2e54-4715-9f00-91dcbea6c300 | 2016-11-17 | 2016-11-17 00:00:00-0800 | test-project
  3954980817302725337 | 756716f7-2e54-4715-9f00-91dcbea6c300 | 2016-11-17 | 2016-11-17 00:15:00-0800 | test-project
  3954980817302725337 | 756716f7-2e54-4715-9f00-91dcbea6c300 | 2016-11-17 | 2016-11-17 04:30:00-0800 | test-project
  8966480984896970565 | 756716f7-2e54-4715-9f00-91dcbea6c300 | 2014-11-17 | 2014-11-17 00:00:00-0800 | test-project
  8966480984896970565 | 756716f7-2e54-4715-9f00-91dcbea6c300 | 2014-11-17 | 2014-11-17 00:15:00-0800 | test-project
  8966480984896970565 | 756716f7-2e54-4715-9f00-91dcbea6c300 | 2014-11-17 | 2014-11-17 04:30:00-0800 | test-project
  9059924875865477567 | 756716f7-2e54-4715-9f00-91dcbea6c300 | 2014-10-23 | 2014-10-23 19:00:13-0700 | test-project
  9059924875865477567 | 756716f7-2e54-4715-9f00-91dcbea6c300 | 2014-10-23 | 2014-10-23 19:15:00-0700 | test-project
  9059924875865477567 | 756716f7-2e54-4715-9f00-91dcbea6c300 | 2014-10-23 | 2014-10-23 19:30:00-0700 | test-project

// reverse logic works for records within a few days:

select token(id,date), id, date, ts, pid from measurement where token(id, date) <= token('756716f7-2e54-4715-9f00-91dcbea6c300', '2016-11-15')
and token(id, date) > token('756716f7-2e54-4715-9f00-91dcbea6c300', '2016-11-18');

token(id, date)     | id                                   | date       | ts                       | pid
---------------------+--------------------------------------+------------+--------------------------+--------------
 3954980817302725337 | 756716f7-2e54-4715-9f00-91dcbea6c300 | 2016-11-17 | 2016-11-17 00:00:00-0800 | test-project
 3954980817302725337 | 756716f7-2e54-4715-9f00-91dcbea6c300 | 2016-11-17 | 2016-11-17 00:15:00-0800 | test-project
 3954980817302725337 | 756716f7-2e54-4715-9f00-91dcbea6c300 | 2016-11-17 | 2016-11-17 04:30:00-0800 | test-project

// reverse logic doesn't work for records within a several years:

select token(id,date), id, date, ts, pid from measurement where token(id, date) <= token('756716f7-2e54-4715-9f00-91dcbea6c300', '2016-11-15')
and token(id, date) > token('756716f7-2e54-4715-9f00-91dcbea6c300', '2013-11-18');

token(id, date)      | id                                   | date       | ts                       | pid
----------------------+--------------------------------------+------------+--------------------------+--------------
 -7529827560707582294 | 756716f7-2e54-4715-9f00-91dcbea6c300 | 2015-11-17 | 2015-11-17 00:00:00-0800 |         null
 -7529827560707582294 | 756716f7-2e54-4715-9f00-91dcbea6c300 | 2015-11-17 | 2015-11-17 00:15:00-0800 | test-project
 -7529827560707582294 | 756716f7-2e54-4715-9f00-91dcbea6c300 | 2015-11-17 | 2015-11-17 04:30:00-0800 | test-project
 -7505241882025539133 | 756716f7-2e54-4715-9f00-91dcbea6c300 | 2016-10-23 | 2016-10-23 19:00:13-0700 | test-project
 -7505241882025539133 | 756716f7-2e54-4715-9f00-91dcbea6c300 | 2016-10-23 | 2016-10-23 19:15:00-0700 | test-project
 -7505241882025539133 | 756716f7-2e54-4715-9f00-91dcbea6c300 | 2016-10-23 | 2016-10-23 19:30:00-0700 | test-project
 -3887792216267834280 | 756716f7-2e54-4715-9f00-91dcbea6c300 | 2015-10-23 | 2015-10-23 19:00:13-0700 | test-project
 -3887792216267834280 | 756716f7-2e54-4715-9f00-91dcbea6c300 | 2015-10-23 | 2015-10-23 19:15:00-0700 | test-project
 -3887792216267834280 | 756716f7-2e54-4715-9f00-91dcbea6c300 | 2015-10-23 | 2015-10-23 19:30:00-0700 | test-project
  3954980817302725337 | 756716f7-2e54-4715-9f00-91dcbea6c300 | 2016-11-17 | 2016-11-17 00:00:00-0800 | test-project
  3954980817302725337 | 756716f7-2e54-4715-9f00-91dcbea6c300 | 2016-11-17 | 2016-11-17 00:15:00-0800 | test-project
  3954980817302725337 | 756716f7-2e54-4715-9f00-91dcbea6c300 | 2016-11-17 | 2016-11-17 04:30:00-0800 | test-project


---
select * from measurement where id='756716f7-2e54-4715-9f00-91dcbea6c300' and date='2015-11-17' and ts > '2015-11-17';

// regular logic 0 results:
select token(id,date), id, date, ts, pid from measurement where token(id, date) >= token('756716f7-2e54-4715-9f00-91dcbea6c300', '2016-11-15')
and token(id, date) < token('756716f7-2e54-4715-9f00-91dcbea6c300', '2016-11-18');
0


---
select * from measurement where token(id, ts) > token(756716f7-2e54-4715-9f00-91dcbea6c300, '2016-11-15') and
   token(id, ts) < token(756716f7-2e54-4715-9f00-91dcbea6c300, '2016-11-18') ;
code=2200 [Invalid query] message="The token() function is only supported on the partition key, found on ts"



select token(id,date), id, date, ts, pid from measurement where token(id, date) < token('756716f7-2e54-4715-9f00-91dcbea6c300', '2016-11-15');


token(id, date)     | id                                   | date       | ts                       | pid
---------------------+--------------------------------------+------------+--------------------------+--------------
 3954980817302725337 | 756716f7-2e54-4715-9f00-91dcbea6c300 | 2016-11-17 | 2016-11-17 00:00:00-0800 | test-project
 3954980817302725337 | 756716f7-2e54-4715-9f00-91dcbea6c300 | 2016-11-17 | 2016-11-17 00:15:00-0800 | test-project
 3954980817302725337 | 756716f7-2e54-4715-9f00-91dcbea6c300 | 2016-11-17 | 2016-11-17 04:30:00-0800 | test-project


select token(id,date), id, date, ts, pid from measurement where token(id, date) < token('756716f7-2e54-4715-9f00-91dcbea6c300', '2016-11-15')
and ts > '2016-11-17 00:15:00-0800' allow filtering;
token(id, date)     | id                                   | date       | ts                       | pid
---------------------+--------------------------------------+------------+--------------------------+--------------
 3954980817302725337 | 756716f7-2e54-4715-9f00-91dcbea6c300 | 2016-11-17 | 2016-11-17 04:30:00-0800 | test-project


select token(id,date), id, date, ts, pid from measurement where token(id, date) <= token('756716f7-2e54-4715-9f00-91dcbea6c300', '2016-11-15')
and ts >= '2016-11-17 00:15:00-0800' allow filtering;
token(id, date)     | id                                   | date       | ts                       | pid
---------------------+--------------------------------------+------------+--------------------------+--------------
 3954980817302725337 | 756716f7-2e54-4715-9f00-91dcbea6c300 | 2016-11-17 | 2016-11-17 00:15:00-0800 | test-project
 3954980817302725337 | 756716f7-2e54-4715-9f00-91dcbea6c300 | 2016-11-17 | 2016-11-17 04:30:00-0800 | test-project


select token(id,date), id, date, ts, pid from measurement where token(id, date) <= token('756716f7-2e54-4715-9f00-91dcbea6c300', '2016-11-15')
and ts >= '2016-11-17 00:15:00-0800' and id = '756716f7-2e54-4715-9f00-91dcbea6c300' allow filtering;
code=2200 [Invalid query] message="id cannot be restricted by more than one relation if it includes an Equal"


order by ts



cqlsh:iotus> select * from measurement where id = '756716f7-2e54-4715-9f00-91dcbea6c300' and date = '2016-11-17' order by ts asc;

 id                                   | date       | ts                       | pid          | val
--------------------------------------+------------+--------------------------+--------------+------
 756716f7-2e54-4715-9f00-91dcbea6c300 | 2016-11-17 | 2016-11-17 00:00:00-0800 | test-project | 69.4
 756716f7-2e54-4715-9f00-91dcbea6c300 | 2016-11-17 | 2016-11-17 00:15:00-0800 | test-project | 69.1
 756716f7-2e54-4715-9f00-91dcbea6c300 | 2016-11-17 | 2016-11-17 04:30:00-0800 | test-project | 66.5


----

select * from measurement where id = 756716f7-2e54-4715-9f00-91dcbea6c300 and token(date, ts) >= token('2016-11-16', '2016-11-16') and
   token(date, ts) < token('2016-11-18', '2016-11-18') ;
code=2200 [Invalid query] message="Type error: '2016-11-16' cannot be passed as argument 0 of function token of type uuid"


------- queries below are obsolete


cqlsh:iotus> select * from node where props  contains 'test 01';

 id                                   | mod                      | author    | created                  | deleted | project      | props          | tags
--------------------------------------+--------------------------+-----------+--------------------------+---------+--------------+----------------+------------------
 756716f7-2e54-4715-9f00-91dcbea6cf11 | 2016-11-11 00:00:00-0800 | sysdeamon | 2016-11-11 00:00:00-0800 |   False | test-project | {dis: test 01} | {'his', 'point'}

(1 rows)
cqlsh:iotus> select * from node where props  contains 'test 02';

 id                                   | mod                      | author    | created                  | deleted | project      | props          | tags
--------------------------------------+--------------------------+-----------+--------------------------+---------+--------------+----------------+------------------
 756716f7-2e54-4715-9f00-91dcbea6ce22 | 2016-11-11 01:00:00-0800 | sysdeamon | 2016-11-11 01:00:00-0800 |   False | test-project | {dis: test 02} | {'his', 'point'}

(1 rows)
cqlsh:iotus> select * from node where tags  contains 'his';

 id                                   | mod                      | author    | created                  | deleted | project      | props          | tags
--------------------------------------+--------------------------+-----------+--------------------------+---------+--------------+----------------+------------------
 756716f7-2e54-4715-9f00-91dcbea6cf11 | 2016-11-11 00:00:00-0800 | sysdeamon | 2016-11-11 00:00:00-0800 |   False | test-project | {dis: test 01} | {'his', 'point'}
 756716f7-2e54-4715-9f00-91dcbea6ce22 | 2016-11-11 01:00:00-0800 | sysdeamon | 2016-11-11 01:00:00-0800 |   False | test-project | {dis: test 02} | {'his', 'point'}

(2 rows)
cqlsh:iotus> select * from node where tags  contains 'his1';

 id | mod | author | created | deleted | project | props | tags

cqlsh:iotus> select * from node where tags  contains 'his' and project='test-project' ALLOW FILTERING;

 id                                   | mod                      | author    | created                  | deleted | project      | props          | tags
--------------------------------------+--------------------------+-----------+--------------------------+---------+--------------+----------------+------------------
 756716f7-2e54-4715-9f00-91dcbea6cf11 | 2016-11-11 00:00:00-0800 | sysdeamon | 2016-11-11 00:00:00-0800 |   False | test-project | {dis: test 01} | {'his', 'point'}
 756716f7-2e54-4715-9f00-91dcbea6ce22 | 2016-11-11 01:00:00-0800 | sysdeamon | 2016-11-11 01:00:00-0800 |   False | test-project | {dis: test 02} | {'his', 'point'}

(2 rows)
cqlsh:iotus> select * from node where tags  contains 'his' and project='demo' ALLOW FILTERING;

 id                                   | mod                      | author    | created                  | deleted | project | props             | tags
--------------------------------------+--------------------------+-----------+--------------------------+---------+---------+-------------------+------------------
 756716f7-2e54-4715-9f01-91dcbea6c111 | 2016-11-11 01:00:00-0800 | sysdeamon | 2016-11-11 01:00:00-0800 |   False |    demo | {dis: demo pt 01} | {'his', 'point'}
 756716f7-2e54-4715-9f01-91dcbea6c222 | 2016-11-11 01:00:00-0800 | sysdeamon | 2016-11-11 01:00:00-0800 |   False |    demo | {dis: demo pt 02} | {'his', 'point'}

(2 rows)

