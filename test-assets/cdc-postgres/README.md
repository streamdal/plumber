# Testing CDC Postgres

1. `docker compose up -d`
2. Exec into postgres shell
   ```bash
   docker exec -it testpostgres psql -U postgres postgres
   ```
3. Create test table `create table employees (id int primary key, name varchar, age int);`
4. Create publication `CREATE PUBLICATION streamdal_plumber FOR ALL TABLES;`
5. Create replication slot `SELECT * FROM pg_create_logical_replication_slot('plumber_slot', 'pgoutput');` 
6. Start plumber in read mode
   ```bash
   plumber read postgres --replication-slot-name plumber_slot --publisher-name streamdal_plumber --database postgres --address localhost:5432 --username postgres --password postgres
   ```
7. Write something to the table `INSERT INTO employees VALUES (1, 'John Doe', 30);`