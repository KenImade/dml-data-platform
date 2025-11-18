dev:
	docker compose -f docker-compose.dev.yaml up --build -d

down:
	docker compose -f docker-compose.dev.yaml down

down-v:
	docker compose -f docker-compose.dev.yaml down -v
