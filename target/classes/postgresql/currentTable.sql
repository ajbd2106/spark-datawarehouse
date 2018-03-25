drop table currentTable;

CREATE TABLE IF NOT EXISTS currentTable (
    id bigint PRIMARY KEY,
    functionalId varchar (50) NOT NULL,
    name varchar (25) NOT NULL,
    type varchar (25) NOT NULL,
    startDate date NOT NULL,
    endDate date NOT NULL,
    updatedDate date NOT NULL,
    current char(1) NOT NULL,
    scd1Checksum varchar (40) NOT NULL,
    scd2Checksum varchar (40) NOT NULL
);

insert into currentTable (id, functionalId, name, type,  startDate, endDate, updatedDate, current, scd1Checksum, scd2Checksum )
values (1, '3542', 'name1', 'type0', to_date('2016-01-01', 'YYYY-MM-DD'), to_date('2016-12-31', 'YYYY-MM-DD'), to_date('2016-01-01', 'YYYY-MM-DD'), 'N', '836db9ecc83d8397a5d0205eb8344d4c', '828a71027955270c8a79ef5b8f570cde');

insert into currentTable (id, functionalId, name, type, startDate, endDate, updatedDate, current, scd1Checksum, scd2Checksum )
values (2, '3542', 'name1', 'type1', to_date('2017-01-01', 'YYYY-MM-DD'), to_date('2100-01-01', 'YYYY-MM-DD'), to_date('2017-01-01', 'YYYY-MM-DD'), 'Y', '836db9ecc83d8397a5d0205eb8344d4c', '3156e42ab24604b8de92a93ed761532d');

insert into currentTable (id, functionalId, name, type,  startDate, endDate, updatedDate, current, scd1Checksum, scd2Checksum )
values (3, '3543', 'name2', 'type2', to_date('2017-01-01', 'YYYY-MM-DD'), to_date('2100-01-01', 'YYYY-MM-DD'), to_date('2017-01-01', 'YYYY-MM-DD'), 'Y', '5a7fcd4f1c785c8ef4931a5a9c698ac0', '8fe8b170aa076a4233d8eda7d28804d4');

insert into currentTable (id, functionalId, name, type,  startDate, endDate, updatedDate, current, scd1Checksum, scd2Checksum )
values (4, '3545', 'name5', 'type2', to_date('2017-01-01', 'YYYY-MM-DD'), to_date('2100-01-01', 'YYYY-MM-DD'), to_date('2017-01-01', 'YYYY-MM-DD'), 'Y', '0de5fc94d0ba53fc7a44f0f136e82fbb', '8fe8b170aa076a4233d8eda7d28804d4');

commit;

drop table stagingTable;

CREATE TABLE IF NOT EXISTS stagingTable (
    functionalId varchar (50) NOT NULL,
    name varchar (25) NOT NULL,
    type varchar (25) NOT NULL
);

insert into stagingTable (functionalId, name, type) values ('3542', 'name1', 'type2');
insert into stagingTable (functionalId, name, type) values ('3543', 'name3', 'type2');
insert into stagingTable (functionalId, name, type) values ('3544', 'name4', 'type1');

commit;


drop table newTable;

CREATE TABLE IF NOT EXISTS newTable (
    id bigint PRIMARY KEY,
    functionalId varchar (50) NOT NULL,
    name varchar (25) NOT NULL,
    type varchar (25) NOT NULL,
    startDate date NOT NULL,
    endDate date NOT NULL,
    updatedDate date NOT NULL,
    current char(1) NOT NULL,
    scd1Checksum varchar (40) NOT NULL,
    scd2Checksum varchar (40) NOT NULL
);


drop table lookupTable;

CREATE TABLE IF NOT EXISTS lookupTable (
    id bigint PRIMARY KEY,
    functionalId varchar (50) NOT NULL
);



drop table factTable;

CREATE TABLE IF NOT EXISTS factTable (
    inventoryDate int,
    partyRole1Id bigint,
    partyRole2Id bigint,
    junkDimension1Id bigint,
    junkDimension2Id bigint,
    amount1 NUMERIC(20,6) NULL,
    amount2 NUMERIC(20,6) NULL
);


drop table stagingFactTable;
CREATE TABLE IF NOT EXISTS stagingFactTable (
    partyRole1FuncId VARCHAR(50),
    partyRole2FuncId VARCHAR(50),
    amount1 NUMERIC(20,6) NULL,
    amount2 NUMERIC(20,6) NULL
);

insert into stagingFactTable (partyRole1FuncId, partyRole2FuncId, amount1, amount2) values ('3543', '3542',  '4586123.4512', '4512345.845');
insert into stagingFactTable (partyRole1FuncId, partyRole2FuncId, amount1, amount2) values ('3542', '3544',  '7845123.4512', '6547864.845');
insert into stagingFactTable (partyRole1FuncId, partyRole2FuncId, amount1, amount2) values ('3541', '3545',  '9845641.4512', '7845312.845');


drop table junkTable;
CREATE TABLE IF NOT EXISTS junkTable (
    id bigint PRIMARY KEY,
    field1 VARCHAR(50),
    updatedDate date NOT NULL,
    checksum varchar(40) NOT NULL
);

insert into junkTable (id, field1, updatedDate, checksum) values (1, 'field1Value',  to_date('2017-12-03', 'YYYY-MM-DD'), 'bc23176e41899331b9f9a1c73527e5b8');

drop table junkLookupTable;

CREATE TABLE IF NOT EXISTS junkLookupTable (
    id bigint PRIMARY KEY,
    functionalId varchar (50) NOT NULL
);

insert into junkLookupTable (id, functionalId) values (1, 'bc23176e41899331b9f9a1c73527e5b8');
