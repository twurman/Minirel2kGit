-------test 1---------
CREATE TABLE gdis(gid integer, name char(34), occupation char(34));
CREATE TABLE doggs(did integer, real_name char(34), occupation char(34));

-- insert into gdis
INSERT INTO gdis(gid, name, occupation)
	VALUES (1, 'ryan wolande', 'pledge master');
INSERT INTO gdis(gid, name, occupation)
	VALUES(2, 'anders ders', 'fisherman');
INSERT INTO gdis(gid, name, occupation)
	VALUES(3, 'dersly', 'blue heron');
INSERT INTO gdis(gid, name, occupation)
	VALUES(4, 'chis', 'cherbs');

-- insert into doggs
INSERT INTO doggs(did, real_name, occupation)
	VALUES (3,'timmy wurm', 'cherbs');
INSERT INTO doggs(did, real_name, occupation)
	VALUES(4, 'the realest SC', 'fisherman');
INSERT INTO doggs(did, real_name, occupation)
	VALUES(6, 'gert dogg', 'fisherman');
INSERT INTO doggs(did, real_name, occupation)
	VALUES(7, 'nate dogg', 'CEO');


-- test select * with no where clause
SELECT * FROM doggs;

-- test select with projection and no where clause
SELECT gdis.name, gdis.occupation FROM gdis;

-- test select * with a where clause
SELECT * FROM gdis WHERE gdis.gid > 2;

-- test select with projection and where clause
SELECT gdis.name, gdis.occupation FROM gdis WHERE gdis.occupation = 'blue heron';

-- smj joins
SELECT * FROM doggs, gdis WHERE doggs.occupation = gdis.occupation;

SELECT gdis.name, doggs.real_name, gdis.occupation, doggs.occupation FROM gdis, doggs WHERE gdis.gid = doggs.did;

-- inl joins
CREATE INDEX gdis(gid);
CREATE INDEX doggs(did);

SELECT * FROM doggs, gdis WHERE doggs.occupation = gdis.occupation;

SELECT gdis.name, doggs.real_name, gdis.occupation, doggs.occupation FROM gdis, doggs WHERE gdis.gid = doggs.did;

-- index select
SELECT gdis.name, gdis.occupation FROM gdis WHERE gdis.name = 'chis';
SELECT * FROM doggs WHERE doggs.did = 3;
SELECT * FROM gdis WHERE gdis.gid = 5;

DROP INDEX gdis(gid);
DROP INDEX doggs(did);

-- snl joins
SELECT gdis.name, doggs.real_name, gdis.occupation, doggs.occupation FROM gdis, doggs WHERE gdis.gid > doggs.did;

-- should return 3 rows, 3&3, 3&4, and 4&4
SELECT * FROM gdis, doggs WHERE gdis.gid <= doggs.did;

-- should return 1 row, 3&4
SELECT * FROM gdis, doggs WHERE gdis.gid < doggs.did;

-- should return 1 row, 4&3
SELECT * FROM gdis, doggs WHERE doggs.did > gdis.gid;

-- should return 3 rows, 3&3, 4&3, 4&4
SELECT * FROM gdis, doggs WHERE doggs.did >= gdis.gid;

-- should be cross product minus when did == gid
SELECT * FROM gdis, doggs WHERE doggs.did <> gdis.gid;


DROP TABLE gdis;
DROP TABLE doggs;

-------test 2---------
CREATE TABLE redTeam(id integer, red double);
CREATE TABLE blueTeam(id integer, blue double);

INSERT INTO redTeam(id, red)
	VALUES(2,4.0);
INSERT INTO redTeam(id, red)
	VALUES(2,5.2);
INSERT INTO redTeam(id, red)
	VALUES(3,5.8);
INSERT INTO blueTeam(id, blue)
	VALUES(1,3.2);
INSERT INTO blueTeam(id, blue)
	VALUES(1,4.4);
INSERT INTO blueTeam(id, blue)
	VALUES(3,3.9);

SELECT blueTeam.blue, redTeam.red from blueTeam, redTeam where blueTeam.id = redTeam.id;


DROP TABLE redTeam;
DROP TABLE blueTeam;






