cs222-database
==============

Database Project in CS222, UCI


Query Language
--------------
All queries start with SELECT

<query> = SELECT

PROJECT <query> GET [ <attrs> ]
FILTER <query> WHERE <attr> <op> <value>
INLJOIN/NLJOIN <query>, <query> WHERE <attr> <op> <attr> PAGES(<pageNum>)
AGG <query> GET <agg-op>(<attr>)
AGG <query> GROUPBY(<attr>) GET <agg-op>(<attr>)

<agg-op> = MIN | MAX | SUM | AVG | COUNT
<op> = < | > | = | != | >= | <=