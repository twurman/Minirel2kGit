-------test 1---------
CREATE TABLE gdis(gid integer, name char(34), occupation char(34));
CREATE TABLE doggs(did integer, real_name char(34), occupation char(34));

insert into gdis(gid, name, occupation)
values (1, 'ryan wolande', 'pledge master');
insert into gdis(gid, name, occupation)
values(2, 'anders ders', 'fisherman');
insert into gdis(gid, name, occupation)
values(3, 'dersly', 'blue heron');
insert into gdis(gid, name, occupation)
values(4, 'chis', 'cherbs');

insert into doggs(did, real_name, occupation)
values (1,'timmy wurm', 'cherbs');
insert into doggs(did, real_name, occupation)
values(2, 'the realest SC', 'fisherman');
insert into doggs(did, real_name, occupation)
values(6, 'gert dogg', 'fisherman');
insert into doggs(did, real_name, occupation)
values(7, 'nate dogg', 'CEO');

select * from doggs;
select gdis.name, gdis.occupation from gdis;
select gid from gdis where occupation = 'blue heron';
select * from doggs, gdis where doggs.occupation = gdis.occupation

-------test 2---------
create table redTeam(id integer, red integer);
create table blueTeam(id integer, blue integer);

insert into redTeam(id, red)
values(2,4);
insert into redTeam(id, red)
values(2,5);
insert into redTeam(id, red)
values(3,5);
insert into blueTeam(id, blue)
values(1,3);
insert into blueTeam(id, blue)
values(1,4);
insert into blueTeam(id, blue)
values(3,3);

select blue.blueTeam, red.blue team from blueTeam, redTeam where blueTeam.id = redTeam.id;