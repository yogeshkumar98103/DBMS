# Database Managent System

This is an implementation of Relational Database from scratch in C++.

### Features
This supports following commands as of now:-
1. Insert
2. Index
3. Delete
4. Select
5. Update
6. Delete

Following Features will be added in future
1. Join
2. Cross Product
3. Complex conditions in `where` clause.

### Syntax

~~~~sql
create table <table-name>{<col-1>: DATATYPE , <col-2> : DATATYPE, ...}
index on {<col-1>, <col-2>} in table
insert into <table-name>{<col-1-data> , <col-1-data> , ...}
update <table-name>{<col-1> = <data-1>, <col-1> = <data-1>, ...}
update <table-name>{<col-1> = <data-1>, <col-1> = <data-1>, ...} where CONDITION
delete from <table-name> where <CONDITION>
delete table <table-name>
drop table <table-name>
select (<col-1>, <col-2>, ...) from <table-name> where CONDITION
select * from <table-name> where CONDITION
~~~~
 
 
 #### Data Types
 1. string(length)
 2. int
 3. float
 4. long
 5. bool
 6. char
 
 #### Condition
 *  `col == data`
 *  `col != data`
 *  `col > data`
 *  `col < data`
 *  `col <= data`
 *  `col >= data`
 *  `condition1 && condition2`
 
 ### Examples
~~~~sql
create table mytable {rollno: int, name: string(50), grade: char)}
index on {rollno} in mytable

insert into mytable { "1", "Yogesh Kumar", "A" }
insert into mytable { "2", "Pranav Guptar", "A" }
insert into mytable { "3", "Yogesh Kumar", "B" }

select * from mytable

delete from mytable where rollno == "3"
~~~~