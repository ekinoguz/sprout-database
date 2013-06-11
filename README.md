cs222-database
==============

Database Project in CS222, UCI

Command Line Interface
----------------------
We support the following commands:

	create table <tableName> (col1 = <type>, col2 = <type>, ...)
		<type> = int | real | <varchar>
		<varchar> = "varchar(" <length> ")"
	example: create table Employee EmpName = varchar(30), Age = int, Height = real, Salary = int
	
	create index <columnName> on <tableName>
	
	insert into <tableName> tuple(col1 = <value1>, col2 = <value2>, ...)
	example: insert into tbl_employee2 tuple(EmpName = sky, Age = 22, Height = 6.1, Salary = 13291)
	
	SELECT <query>: where <query> is defined in Query Language section
	drop table <tableName>
	drop index <indexName> on <tableName>
	drop attribute <attributeName> from <tableName>
	
	print <tableName>
	print attributes <tableName>
	print index <attributeName> on <tableName>
	
	load <tableName> <fileName>: loads the file which is in data/ folder.
	help <commandName>
	help
	quit or exit
	
All commands are case-insensitive. However, all user defined strings such as table names, attribute names etc. are case sensitive.

Query Language
--------------
We support the following operations:
* Projection
* Filtering
* Nested Loop Join
* Index Nested Loop Join
* Aggregates
* Index Scan

All queries start with "SELECT"

	<query> = 
		PROJECT <query> GET "[" <attrs> "]"
		FILTER <query> WHERE <attr> <op> <value>
		INLJOIN <query>, <query> WHERE <attr> <op> <attr> PAGES(<pageNum>)
		NLJOIN <query>, <query> WHERE <attr> <op> <attr> PAGES(<pageNum>)
		AGG <query> [ GROUPBY(<attr>) ] GET <agg-op>(<attr>)
		IS <query> <attr> <op> <value>
		<tableName>
	
	<agg-op> = MIN | MAX | SUM | AVG | COUNT
	<op> = < | > | = | != | >= | <= | NOOP
	<attrs> = <attr> { "," <attr> }
	<pageNum> = is a number bigger than 0, usually 10
