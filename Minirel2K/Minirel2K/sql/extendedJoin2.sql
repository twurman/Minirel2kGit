-- create relations
CREATE TABLE cherbs(cherbid integer, name char(32), rating double);
CREATE TABLE nerbs(nerbid integer, name char(32), rating double);


-- test case to check SMJ and INL joins with different sized relations
INSERT INTO cherbs(cherbid, name, rating)
	VALUES (1, 'the real sean carney', 10.0);
	
INSERT INTO cherbs(cherbid, name, rating)
	VALUES (2, 'the fake sean carney', 0.0);
	
INSERT INTO cherbs(cherbid, name, rating)
	VALUES (2, 'carneys', 5.4);
	
INSERT INTO cherbs(cherbid, name, rating)
	VALUES (4, 'scarney', 3.2);
	
INSERT INTO nerbs(nerbid, name, rating)
	VALUES (1, 'the real tim wurman', 10.0);
	
INSERT INTO nerbs(nerbid, name, rating)
	VALUES (2, 'the fake tim wurman', 2.4);
	
INSERT INTO nerbs(nerbid, name, rating)
	VALUES (2, 'twurman', 8.9);
	
SELECT * FROM cherbs, nerbs WHERE cherbs.cherbid = nerbs.nerbid; -- use SMJ & test right relation smaller than left relation

CREATE INDEX cherbs (cherbid);

SELECT * FROM cherbs, nerbs WHERE cherbs.cherbid = nerbs.nerbid; -- use INL & test right relation smaller than left relation

DROP INDEX cherbs (cherbid);

INSERT INTO nerbs(nerbid, name, rating)
	VALUES (4, 'wolandis', 7.1);
	
INSERT INTO nerbs(nerbid, name, rating)
	VALUES (4, 'daters', 7.2);
	
SELECT * FROM cherbs, nerbs WHERE cherbs.cherbid = nerbs.nerbid; -- use SMJ & test left relation smaller than right relation

CREATE INDEX nerbs (nerbid);

SELECT * FROM cherbs, nerbs WHERE cherbs.cherbid = nerbs.nerbid; -- use INL & test left relation smaller than right relation

DROP INDEX nerbs (nerbid);

DROP TABLE cherbs;
DROP TABLE nerbs;