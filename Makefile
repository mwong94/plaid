deploy-server:
	docker compose up -d prefect-server prefect-worker-1

redeploy-server:
	docker compose stop prefect-server prefect-worker-1
	docker compose up --build -d prefect-server prefect-worker-1

deploy-flows:
	/usr/bin/bash -c "source venv/bin/activate; python3 src/create_deployment.py"