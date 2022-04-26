# kudu-spring-boot-starter
1. 通过ApachePool实现KuduSession连接池
2. 通过封装KuduTemplate类，实现insert、upsert、update、delete及query等数据库操作
3. 数据刷入模式为手动刷入，默认数量为1000（可配置），每缓存一千条后再刷入到数据库，提高效率
