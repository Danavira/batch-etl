# batch-etl

```
my-data-repo/
├── dags/                       # Airflow DAGs
│   └── etl.py
├── dbt_project/                # dbt folder
│   ├── dbt_project.yml         # The "heart" of the project
│   ├── profiles.yml            # Connection settings (host, port, etc.)
│   ├── models/                 # Where the SQL lives
│   │   ├── staging/            # Initial cleaning of raw data
│   │   │   ├── stg_orders.sql
│   │   │   └── stg_users.sql
│   │   └── marts/              # Final tables for BI/Reporting
│   │       └── daily_revenue.sql
│   └── tests/                  # Custom data quality tests
└── Dockerfile                  # Builds an image containing dbt + your SQL
```


minikube (kubernetes is really important for japan)
logging
pipeline monitoring
metrics dashboard
documentation clarity


# Big picture flow
Airflow launches a kubernetes pod operator. The pod operator runs a python script that extracts-loads data from an API to the clickhouse warehouse. Airflow then triggers a dbt job in another kubernetes pod operator that transforms the data in clickhouse for reporting.

Minimum set of images

Airflow image: mostly stock (plus Kubernetes connection bits)

Python job image: contains your extractor code + deps

dbt image: contains dbt + dbt-clickhouse + your dbt project (or pulls it at runtime)

Installing system dependencies for ubuntu 24.04.


1. install docker
Official docker documentation
```
sudo apt install -y ca-certificates curl gnupg
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg

echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo usermod -aG docker $USER
newgrp docker
```
2. install minikube
```
curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
sudo install minikube-linux-amd64 /usr/local/bin/minikube
```

3. install kubectl
```
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
sudo install kubectl /usr/local/bin/kubectl
```


## Prerequisites

Before you can run this project locally, install these system-level tools:

- **Docker** (required for the Docker driver)  
  → [Official install guide](https://docs.docker.com/engine/install/)

- **Minikube** (local Kubernetes)  
  → [Official quickstart](https://minikube.sigs.k8s.io/docs/start/)

- **kubectl** (Kubernetes CLI) — usually comes with Minikube, but you can install separately  
  → [Install kubectl](https://kubernetes.io/docs/tasks/tools/)

### Platform-specific notes

**Ubuntu / WSL2** (recommended for this project)

```bash
# 1. Docker Engine (inside WSL)
sudo apt update
sudo apt install docker.io -y
sudo usermod -aG docker $USER
newgrp docker

# 2. Minikube
curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
sudo install minikube-linux-amd64 /usr/local/bin/minikube

# 3. kubectl (if needed)
curl -LO "https://dl.k8s.io/release/$(curl -L -s https://dl.k8s.io/release/stable.txt)/bin/linux/amd64/kubectl"
sudo install kubectl /usr/local/bin/kubectl
```


Use a placeholder secret.yaml but never commit actual passwords. Mention in your README that you use SealedSecrets or ExternalSecrets in "real" prod.


operator and executor tradeoffs