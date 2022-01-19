docker exec -i $(docker-compose ps -q postgres) psql -U nuts_user nuts_db < sql/nuts_ddl.sql
