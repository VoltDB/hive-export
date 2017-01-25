# VoltDB Hive Export Conduit

An experimental VoltDB to Hive export conduit that takes advantage of Hive's new [streaming
hcatalog API](https://cwiki.apache.org/confluence/display/Hive/Streaming+Data+Ingest). It
allows export table writers to push data directly into correspoding Hive tables.

## How to build artifacts and setup Eclipse

* Install Gradle

On a Mac if you have [Homebrew](http://brew.sh/) setup then simply install the gradle bottle

```bash
brew install gradle
```

On Linux setup [GVM](http://gvmtool.net/), and install gradle as follows

```bash
gvm install gradle
```

* Create `gradle.properties` file and set the `voltdbhome` property
   to the base directory where your VoltDB is installed

```bash
echo voltdbhome=/voltdb/home/dirname > gradle.properties
```

* Invoke gradle to compile artifacts

```bash
gradle shadowJar
```

* To setup an eclipse project run gradle as follows

```bash
gradle cleanEclipse eclipse
```
then import it into your eclipse workspace by using File->Import projects menu option

## Configuration

* Copy the built jar from `build/libs` to `lib/extension` under your VoltDB installation directory

* Edit your deployment file and use the following export XML stanza as a template

```xml
<?xml version="1.0"?>
<deployment>
    <cluster hostcount="1" sitesperhost="4" kfactor="0" />
    <httpd enabled="true">
        <jsonapi enabled="true" />
    </httpd>
    <export>
        <configuration stream="hive" enabled="true" type="custom"
            exportconnectorclass="org.voltdb.exportclient.hive.HiveExportClient">
            <property name="hive.uri">thrift://hive-host:9083</property>
            <property name="hive.db">meco</property>
            <property name="hive.table">alerts</property>
            <property name="hive.partition.columns">ALERTS:CONTINENT|COUNTRY</property>
        </configuration>
    </export>
</deployment>
```

This tells VoltDB to write to the alerts table on Hive, via the
homonymous export table in VoltDB, using columns CONTINENT and COUNTRY
as value providers for Hive partitions discerners. For example the
alerts table is defined in Hive as:

```sql
create table alerts ( id int , msg string )
     partitioned by (continent string, country string)
     clustered by (id) into 5 buckets
     stored as orc; // currently ORC is required for streaming
```

while the VoltDB export table is defined as:

```sql
FILE -inlinebatch END_OF_EXPORT

create stream alerts partitioned on column id export to target hive (
  id integer not null,
  msg varchar(128),
  continent varchar(64),
  country varchar(64)
)
;
END_OF_EXPORT
```

When a row is inserted into the export table

```sql
INSERT INTO ALERTS (ID,MSG,CONTINENT,COUNTRY) VALUES (1,'fab-02 inoperable','EU','IT');
```

The continent ('EU') and country ('IT') column values are used to
specify the Hive table partition.

## Configuration Properties

- `hive.uri` (mandatory) thrift URI to the Hive host
- `hive.db`  (mandatory) Hive database
- `hive.table` (mandatory) Hive table
- `hive.partition.columns` (mandatory if the hive table is partitioned) format: _table-1:column-1|column-2|...|column-n,table-2:column-1|column-2|...|column-n,...,table-n:column-1|column-2|...|column-n_
- `timezone` (optional, _default:_ local timezone) timezone used to format timestamp values

Partition columns must be of type VARCHAR. Any empty or null partition column values are converted to `__VoltDB_unspecified__`
