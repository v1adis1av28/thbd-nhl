.PHONY: init up trigger counts compare down

init:
	docker compose up airflow-init

up:
	docker compose up -d --build

trigger:
	./scripts/trigger_all.sh

counts:
	./scripts/show_counts.sh

compare:
	docker compose exec -T airflow-scheduler python /opt/airflow/analysis/compare_storages.py

down:
	docker compose down
