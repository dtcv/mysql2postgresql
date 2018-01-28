# mysql2postgresql test
Convert mysql dump file to postgresql script.   
### How to build test2
`go get github.com/dtcv/mysql2postgresql`
### How to use
1. use mysqldump to dump database from mysql:
`mysqldump --compatible=postgresql --default-character-set=utf8 --hex-blob -r databasename.mysql -u root databasename`
2. convert the dump file to postgresql script:   
`mysql2postgresql -i databasename.mysql -o databasename.sql`
3. use the script with psql:   
`\i databasename.sql`

### Reference
[mysql-postgresql-converter](https://github.com/lanyrd/mysql-postgresql-converter)

