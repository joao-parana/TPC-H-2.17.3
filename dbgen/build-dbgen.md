# build-dbgen

```bash
docker build -t parana/gcc .
# Para rodar com X11 use:
# docker run -it --rm -e DISPLAY -v /tmp/.X11-unix:/tmp/.X11-unix -v $PWD:/usr/app/build-dbgen parana/gcc bash
docker run -it --rm -v $PWD:/usr/app/build-dbgen parana/gcc bash
```

Dentro do contêiner executar:

```bash
cd /usr/app/build-dbgen
#  cmake -DCMAKE_CXX_COMPILER=g++ -DCMAKE_BUILD_TYPE=Release
make
./dbgen -h
```


Dentro do contêiner executar:

```bash
# To generate the SF=1 (1GB), validation database population, with customers, 
# orders and lineitems, use:
# ./dbgen -vf -s 1 -T c -T o 
# To generate the lineitem table only, for a scale factor 10 database,
# and over-write any existing flat files:
# ./dbgen -s 1 -f -T L
# To generate the SF=1 (1GB), validation database population, with customers, 
# orders and lineitems, use:
./dbgen -s 1 -T c
./dbgen -s 1 -T o 
wc customer.tbl 
  150000  1653059 24346144 customer.tbl
wc orders.tbl      
  1500000  11467545 171952161 orders.tbl
mv *.tbl DATA/
```

Veja esse repository : [https://github.com/tvondra/pg_tpch](https://github.com/tvondra/pg_tpch)

And to convert them to a CSV format compatible with PostgreSQL, do this

```bash
cd DATA
for i in `ls *.tbl`; do sed 's/|$//' $i > ${i/tbl/csv}; echo $i; done;
rm *.tbl
mv orders.csv order.csv
```

```bash
echo "c_custkey|c_name|c_address|c_nationkey|c_phone|c_acctbal|c_mktsegment|c_comment" > customers.csv
cat customer.csv >> customers.csv 
head customers.csv
tail customers.csv
```




```bash
echo "l_orderkey|l_partkey|l_suppkey|l_linenumber|l_quantity|l_extendedprice|l_discount|l_tax|l_returnflag|l_linestatus|l_shipdate|l_commitdate|l_receiptdate|l_shipinstruct|l_shipmode|l_comment" > lineitems.csv
cat lineitem.csv >> lineitems.csv 
head lineitems.csv
tail lineitems.csv
```



```bash
echo "o_orderkey|o_custkey|o_orderstatus|o_totalprice|o_orderdate|o_orderpriority|o_clerk|o_shippriority|o_comment" > orders.csv
cat order.csv >> orders.csv 
head orders.csv
tail orders.csv
```

```bash
# Removendo arquivo anterior
rm customer.csv 
rm order.csv
rm lineitem.csv
```

```bash
```

```bash
```



Finally, move these data to the 'dss/data' directory or somewhere else, and 
create a symlink to /tmp/dss-data (that's where tpch-load.sql is looking for 
the data from).


