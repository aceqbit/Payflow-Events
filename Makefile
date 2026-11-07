SHELL := /bin/bash
COMPOSE := docker compose -f deploy/docker-compose.yml -f deploy/docker-compose.observability.yml -f deploy/docker-compose.connect.yml -f deploy/docker-compose.apps.yml
MATRIX ?= full

.PHONY: up down test it images source dualwrite chaos chaos-v1 skew pills rebalance bench report clean

up:
	$(COMPOSE) up -d --build --wait

down:
	$(COMPOSE) down -v

test:
	mvn -B -ntp verify

it:
	mvn -B -ntp verify -Pit

images:
	mvn -B -ntp -DskipTests package jib:dockerBuild -pl source-app,dual-write-app,relay-poller,consumer,verifier,generator,rollup-streams -am

# RATE=5000 OUTBOX_MODE=retain|delete RUN_ID=... make source
source:
	$(COMPOSE) --profile source up -d source-app

# RATE=5000 DUALWRITE_MODE=db-first|kafka-first RUN_ID=... make dualwrite
dualwrite:
	$(COMPOSE) --profile dualwrite up -d dual-write-app

chaos:
	MATRIX=$(MATRIX) bash chaos/run-matrix.sh

chaos-v1:
	bash chaos/run-v1.sh

skew:
	MATRIX=$(MATRIX) bash perf/skew-matrix.sh

pills:
	bash perf/pills.sh

rebalance:
	bash perf/rebalance.sh

bench:
	bash perf/bench.sh

report:
	python perf/report.py

clean:
	mvn -B -ntp clean
	rm -rf perf/state chaos/state
