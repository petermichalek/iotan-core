/** 
 * Populate user table with two users: admin@admin.org and guest@example.org
 * This will overwrite user records if they already exist.

 To run:
 cqlsh localhost  -k {keyspace} < db/iotus-populate-user.cql
 or
 cqlsh IP_ADDRESS -k {keyspace} < db/iotus-populate-user.cql

*/

-- ---------------------------------
-- --- users
-- ---------------------------------
-- password may be of two kinds:
-- SSHA or pbkdf2

--slappasswd -s guest
--{SSHA}4tlaTRkpdRqq68/nP/SCoUF6fa3Zb1nj
-- guest@example.org / guest
INSERT INTO user (id, created, mod, author, deleted, email, firstname, lastname, password)
      VALUES (
          856716f7-2e54-4715-9f00-91dcbea66671,
          '2017-02-14',
          '2017-02-14',
          'sysdeamon',
          false,
          'guest@example.org',
          'Guest',
          'User',
          '{SSHA}4tlaTRkpdRqq68/nP/SCoUF6fa3Zb1nj')
        ;


--  slappasswd -s 4dmin123
--   {SSHA}d7Coy/9Osx0rWqdLCNl8lrho8qTPM04B
-- admin@admin.org / 4dmin123
INSERT INTO user (id, created, mod, author, deleted, email, firstname, lastname, password)
      VALUES (
          856716f7-2e54-4715-9f00-91dcbea66672,
          '2017-02-14',
          '2017-02-14',
          'sysdeamon',
          false,
          'admin@admin.org',
          'Admin',
          'Superuser',
          '{SSHA}d7Coy/9Osx0rWqdLCNl8lrho8qTPM04B'
          )
        ;

