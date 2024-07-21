####################################################################################################################
# Setup containers to run Airflow

docker-spin-up:
	docker compose up airflow-init && docker compose up --build -d

perms:
	sudo mkdir -p logs plugins config dags data && sudo chmod -R u=rwx,g=rwx,o=rwx logs plugins config dags data

setup-conn:
	docker exec scheduler python /opt/airflow/setup_connections.py

do-sleep:
	sleep 30

up: perms docker-spin-up do-sleep setup-conn


down:
	docker compose down --volumes --rmi all

restart: down up

sh:
	docker exec -ti webserver bash


