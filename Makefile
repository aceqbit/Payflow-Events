SHELL := /bin/bash
COMPOSE := docker compose -f deploy/docker-compose.yml -f deploy/docker-compose.observability.yml -f deploy/docker-compose.connect.yml
MATRIX ?= full

.PHONY: up down test it chaos chaos-v1 skew pills rebalance bench report clean

up:
	$(COMPOSE) up -d --build --wait

down:
	$(COMPOSE) down -v

test:
	mvn -B -ntp verify

it:
	mvn -B -ntp verify -Pit

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
