
drop database tpch100_minio;
create database tpch100_minio;
use tpch100_minio;



insert into tpch100.customer select * from customer ;

insert into tpch100.nation select * from nation ;
insert into tpch100.orders
select
    o_orderkey,
    o_orderdate,
    o_custkey,
    o_orderstatus,
    o_totalprice,
    o_orderpriority,
    o_clerk,
    o_shippriority,
    o_comment
from orders ;

insert into tpch100.partsupp select * from partsupp ;
insert into tpch100.region select * from region ;
insert into tpch100.supplier select * from supplier ;
