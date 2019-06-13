# 概述
Mee名字的含义是 MySQL to ElasticSearch（M2E -> MEE）。

Mee是基于Python 2.7开发的，计划今年下半年升级到 Python 3.x。

目前Mee只在CentOS下测试并运行，其它操作系统未做进一步的测试。

## Mee能做什么
通过Mee，我们可以实时（秒级）将MySQL中的数据同步到ElasticSearch中。
亮点：
1. 支持跨库跨表的数据同步
2. 支持ElasticSearch中的nested类型
3. 代码与具体业务无关，通过配置文件可灵活实现不同业务的数据同步需求

## 配置文件
Mee有两种类型的配置文件：
1. app.ini：在目录conf/ 中，命名暂固定为app.ini，是Mee项目的配置文件。其中包含了Mee项目本身相关的配置信息，例如数据库连接配置、日志配置等等。我们称之为**应用配置文件**。
2. \*.yml：在目录 conf/handlers/ 中，基于yaml格式，是数据同步的配置文件。其中指定了MySQL字段和ES字段的对应关系 —— 除了支持最简单的单一字段映射外（即MySQL中的某列，对应到ES中的某个字段），还支持复杂的字段映射关系（例如多条MySQL行记录映射到ES中的一个字段中）。通过yml配置文件，可以在不修改代码的前提下，灵活实现不同业务的数据同步需求。我们称之为**同步配置文件**。


## 如何使用
Mee需要启动三个服务：
1. ListenService：
ListenService的职责是监听指定MySQL的binlog日志，然后写入到Kafka队列中。

    启动命令：python listen.py -d <database>

    例子：python listen.py -d admin_service

    >ListenService是常驻进程，建议使用类似supervisord的工具进行管理。


2.	UpdateService：
UpdateService的职责是根据同步配置文件从MySQL中获取数据，并全量更新到ElasticSearch中。

    启动命令：python update.py -c <handler config> -n <task_name>
    
    例子：python update.py -c admin_service.yml -n task_admin_service

3.	SyncService：
SyncService的职责是从Kafka队列中获取MySQL的binlog日志，然后根据同步配置文件增量更新ElasticSearch。

    启动命令：python sync.py -n <task_name>

    例子：python sync.py -n task_admin_service

    >需要注意的是，SyncService的启动参数 -n 需要和对应的UpdateService的启动参数 -n 保持一致。换而言之，增量更新服务的task name需要与全量更新服务的task name保持一致，Mee才能将增量更新和全量更新的服务联系起来。

    >SyncService是常驻进程，建议使用类似supervisord的工具进行管理。

# Mee的设计框架
Mee的组件框架如下（箭头表示数据流方向）：

![Mee的组件框架图](https://github.com/ioduduk/mee/blob/master/docs/images/Mee.png)


1. write metadata：每次全量更新时，都会创建新的kafka consumer group，以及新的ElasticSearch索引，这些元数据需要写入Redis中，以便后续SyncService服务使用
2. set offset：创建新的kafka consumer group，需要将它的消费offset设置为当前kafka最新消息的offset，随后的SyncService将会从该offset开始消费新的binlog消息
3. fetch data：UpdateService根据同步配置文件从MySQL中获取数据
4. write documents：UpdateService根据同步配置文件，构造ES的文档，并写入到ES中
5. fetch binlog：ListenService从MySQL获取binlog日志
6. write binlog：ListenService将binlog日志Json序列化后，写入Kafka
7. read metadata：SyncService从Redis中获取最新的元数据（主要包括kafka consumer group，和ES的索引名称
8. fetch binlog：SyncService从Kafka中拉取binlog消息
9. update documents：SyncService根据同步配置文件和binlog消息，更新ES中的文档，从而达成实时的增量更新


# 配置文件

## 应用配置文件

路径：<mee>/conf/app.ini

应用配置文件较为简单，可参考 [app.ini](https://github.com/ioduduk/mee/blob/master/conf/app.ini)

唯一值得一提的是，实现了按日期滚动的logger。

另外，需要注意的是，mysql相关配置节的名称，[mysql:]后面应该跟着数据库的名称。

## 同步配置文件

同步配置文件使用yaml格式。

每个同步配置文件对应着一个ElasticSearch的索引。（当然，也可以使用include语法，在单独一个配置文件中包含多个ElasticSearch索引，但这样就无法单独针对某个索引进行全量更新，数据量大的时候，会影响全量更新的速度。）

同步配置文件的格式如下：
```
<index_alias>:
   <type_name>:
     -
       <config item 1>
     -
       <config item 2>
```

index_alias：指的是ElasticSearch的索引别名——每次全量更新时都会生成一个全新的ES索引，这样可以保证在全量更新时依然可以向外提供服务，全量更新完毕后，通过修改索引的别名，使应用的查询落在最新的ES索引上。

type_name：指的是ElasticSearch中的type名称。

config item：配置节，每个配置节对应着MySQL中的某个表（可跨库）。MySQL表的列和ElasticSearch索引的字段，它们的对应关系在config item中定义。

config item的格式如下：
```
key: "<unique key>"
database: "<database name>"
table: "<table name>"
statement: "<sql statement>"
document_id: "<document id>"
routing: "<routing value>"
query: 
  <es field>: <value>
parent_query: 
  <es field>: <value>
filter:
  <mysql column>: <value>
mapping:
  - <field_name>
  - 
    db_field: <mysql column name>
    es_field: <es field name>
```

其中：
key：配置节的唯一标识。在其它配置节中，可以通过该唯一标识引用到配置节对应的mysql table的行数据。

database：数据库名称

table：数据库表名称

statement：从mysql中查询数据的SQL语句

document_id：写入ES时，指定的document id

routing：写入ES时，指定的routing参数

query：从ES查询时，指定的查询参数

parent_query：parent_query只存在与nested类型的配置节中。和query的含义类似

filter：用来过滤不相干的mysql binlog。通常与statement中的where子句一致

mapping：定义了mysql字段和es字段的映射关系


更详尽的说明和示例，可参考 conf/ 目录下的配置文件示例：

[index_carteam_user.yml](https://github.com/ioduduk/mee/blob/master/conf/handlers/index_carteam_user.yml)

[index_vehicle.yml](https://github.com/ioduduk/mee/blob/master/conf/handlers/index_vehicle.yml)

[config.yml](https://github.com/ioduduk/mee/blob/master/conf/handlers/config.yml)

# 入门示例

TODO

# 使用限制 && 注意事项
1. 当数据量大的时候，不要在ES索引中存储大量重复的而且可能发生变化的冗余数据，否则增量更新时，性能会非常慢——因为要更新大量ES文档中的对应字段。

# 待优化事项
1. 全量更新时，应该使用批量插入ES文档，提升更新性能
2. query、parent_query以及filter都可以从statement配置项中解析出来
3. 全量更新完成后，应该等待增量更新服务赶上进度后，再修改ES索引别名对外提供服务
4. 需要增加ES和MySQL的同步指标：例如当前ES的数据比MySQL的落后多少秒


