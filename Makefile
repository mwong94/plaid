deploy-server:
	docker compose up -d prefect-server

redeploy-server:
	docker compose stop prefect-server
	docker compose up --build -d prefect-server

deploy-flows:
	/usr/bin/bash -c "source venv/bin/activate; python3 src/create_deployment.py"