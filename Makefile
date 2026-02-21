# Variables
KUBECTL = kubectl
KUSTOMIZE = $(KUBECTL) apply -k
HELM = helm
NS_AIRFLOW = airflow
NS_DATA = data

.PHONY: help setup deploy-data deploy-airflow status destroy

help: ## Show this help message
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2}'

setup: ## Create namespaces if they don't exist
	$(KUBECTL) create namespace $(NS_AIRFLOW) --dry-run=client -o yaml | $(KUBECTL) apply -f -
	$(KUBECTL) create namespace $(NS_DATA) --dry-run=client -o yaml | $(KUBECTL) apply -f -

deploy-data: setup ## Deploy Postgres (Landing) and ClickHouse
	$(KUSTOMIZE) data/postgres-landing/overlays/minikube
	$(KUSTOMIZE) data/clickhouse/overlays/minikube

deploy-airflow: setup ## Deploy Airflow Metadata DB and Airflow via Helm
	$(KUSTOMIZE) airflow-metadata-postgres/overlays/minikube
	@echo "Waiting for Metadata DB to be ready..."
	$(KUBECTL) wait --for=condition=ready pod -l app=airflow-metadata -n $(NS_AIRFLOW) --timeout=60s
	$(HELM) upgrade --install airflow apache-airflow/airflow \
		--namespace $(NS_AIRFLOW) \
		-f k8s/airflow/values.yaml

deploy-all: deploy-data deploy-airflow ## Deploy the entire stack

status: ## Check the status of all resources
	@echo "--- Data Namespace ---"
	$(KUBECTL) get all -n $(NS_DATA)
	@echo "\n--- Airflow Namespace ---"
	$(KUBECTL) get all -n $(NS_AIRFLOW)

destroy: ## Tear down the entire local environment
	$(KUBECTL) delete ns $(NS_AIRFLOW) $(NS_DATA)