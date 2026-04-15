# Provider Reference

Orbi manages infrastructure through **providers** -- pluggable backends that
know how to create, update, and destroy resources on a specific platform.
Each provider is registered once per file (or session) with `ADD PROVIDER` and
then referenced by name in resource statements.

---

## Azure

Cloud resources on Microsoft Azure. Uses the `az` CLI.

### Registration

```sql
ADD PROVIDER azure
  name      = 'my-azure'
  auth      = '<subscription-id>'
  labels    = '{"location":"eastus"}';
```

Auth accepts a subscription ID directly, or reads credentials from the
environment when `az login` has already been run.

### Resource types

| Type                 | Description                                      |
|----------------------|--------------------------------------------------|
| `vm`                 | Virtual machine                                  |
| `postgres`           | Azure Database for PostgreSQL flexible server     |
| `pg_database`        | Individual database inside a PostgreSQL server    |
| `redis`              | Azure Cache for Redis instance                   |
| `aks`                | Azure Kubernetes Service cluster                 |
| `vnet`               | Virtual network                                  |
| `subnet`             | Subnet within a virtual network                  |
| `nsg`                | Network security group                           |
| `nsg_rule`           | Inbound/outbound rule on an NSG                  |
| `storage_account`    | Azure Storage account                            |
| `container_registry` | Azure Container Registry                         |
| `container_app`      | Azure Container Apps application                 |
| `container_job`      | Azure Container Apps job                         |
| `dns_zone`           | Azure DNS zone                                   |
| `load_balancer`      | Azure Load Balancer                              |
| `keyvault`           | Azure Key Vault                                  |

---

## AWS

Cloud resources on Amazon Web Services. Uses the `aws` CLI.

### Registration

```sql
ADD PROVIDER aws
  name      = 'my-aws'
  auth      = 'env:AWS_PROFILE=myprofile'
  labels    = '{"region":"us-east-1"}';
```

Auth can be a bare profile name (`auth = 'myprofile'`) or an environment
variable binding (`auth = 'env:AWS_PROFILE=myprofile'`). The `region` label is
required.

### Resource types

| Type              | Description                                  |
|-------------------|----------------------------------------------|
| `ec2`             | EC2 instance (virtual machine)               |
| `rds_postgres`    | RDS PostgreSQL database instance             |
| `vpc`             | Virtual Private Cloud                        |
| `aws_subnet`      | Subnet within a VPC                          |
| `security_group`  | VPC security group                           |
| `sg_rule`         | Inbound/outbound rule on a security group    |
| `s3_bucket`       | S3 storage bucket                            |
| `lambda`          | Lambda function                              |
| `elb`             | Elastic Load Balancer                        |

---

## Cloudflare

DNS and edge services on Cloudflare. Uses the REST API (no CLI required).

### Registration

```sql
ADD PROVIDER cloudflare
  name      = 'my-cf'
  auth      = 'op:Infrastructure/CF-Token';
```

Auth accepts a 1Password reference (`op:...`) or an environment variable
(`auth = 'env:CLOUDFLARE_API_TOKEN'`). The token needs Zone and DNS edit
permissions.

### Resource types

| Type                | Description                                    |
|---------------------|------------------------------------------------|
| `cf_zone`           | Cloudflare DNS zone                            |
| `cf_dns_record`     | DNS record (A, AAAA, CNAME, MX, TXT, etc.)    |
| `cf_firewall_rule`  | Firewall rule (IP access rules, WAF rules)     |
| `cf_page_rule`      | Page rule (redirects, caching overrides)       |

---

## GitHub

Repository configuration and CI/CD on GitHub. Uses the `gh` CLI.

### Registration

```sql
ADD PROVIDER github
  name      = 'my-gh'
  auth      = 'env:GITHUB_TOKEN';
```

Auth reads the token from an environment variable. The token needs `repo`,
`admin:org`, and `workflow` scopes depending on the resource types used.

### Resource types

| Type                    | Description                                     |
|-------------------------|-------------------------------------------------|
| `gh_repo`               | GitHub repository                               |
| `gh_ruleset`            | Repository or organization ruleset              |
| `gh_secret`             | Actions secret (repo or environment scope)      |
| `gh_variable`           | Actions variable (repo or environment scope)    |
| `gh_workflow_file`      | Workflow file committed to `.github/workflows/` |
| `gh_branch_protection`  | Branch protection rule                          |

---

## Kubernetes

Workloads and configuration on any Kubernetes cluster. Uses the `kubectl` CLI.

### Registration

```sql
ADD PROVIDER kubernetes
  name      = 'my-k8s'
  auth      = 'my-kubeconfig-context';
```

Auth is a kubeconfig context name. The context must already exist in
`~/.kube/config` or in the file pointed to by `KUBECONFIG`.

### Resource types

| Type                        | Description                                  |
|-----------------------------|----------------------------------------------|
| `k8s_namespace`             | Namespace                                    |
| `k8s_deployment`            | Deployment                                   |
| `k8s_service`               | Service (ClusterIP, NodePort, LoadBalancer)   |
| `k8s_configmap`             | ConfigMap                                    |
| `k8s_secret`                | Secret                                       |
| `k8s_ingress`               | Ingress rule                                 |
| `k8s_hpa`                   | Horizontal Pod Autoscaler                    |
| `k8s_pvc`                   | PersistentVolumeClaim                        |
| `k8s_job`                   | Job (one-off batch workload)                 |
| `k8s_cronjob`               | CronJob (scheduled batch workload)           |
| `k8s_networkpolicy`         | NetworkPolicy                                |
| `k8s_serviceaccount`        | ServiceAccount                               |
| `k8s_clusterrole`           | ClusterRole (cluster-wide RBAC role)         |
| `k8s_clusterrolebinding`    | ClusterRoleBinding                           |
| `k8s_role`                  | Role (namespace-scoped RBAC role)            |
| `k8s_rolebinding`           | RoleBinding                                  |

### Live query nouns

These nouns query the live cluster state rather than the registry.

| Noun                | Description                                   |
|---------------------|-----------------------------------------------|
| `k8s_pods`          | Running pods with phase and container status  |
| `k8s_deployments`   | Deployments with replica counts               |
| `k8s_services`      | Services with type and cluster IP             |
| `k8s_ingresses`     | Ingress rules with hosts and paths            |
| `k8s_configmaps`    | ConfigMaps with key listing                   |
| `k8s_secrets`       | Secrets with key listing (values redacted)    |
| `k8s_namespaces`    | Namespaces with status                        |
| `k8s_nodes`         | Nodes with capacity and conditions            |

```sql
SELECT * FROM k8s_pods WHERE status = 'CrashLoopBackOff';
SELECT name, ready_replicas FROM k8s_deployments WHERE namespace = 'prod';
```

---

## SSH

Remote hosts managed over OpenSSH. Uses the `ssh` CLI.

### Registration

```sql
ADD PROVIDER ssh
  name      = 'web-server'
  auth      = 'op:Infrastructure/ssh-key'
  host      = '10.0.1.5'
  labels    = '{"ssh_user":"azureuser"}';
```

Auth accepts a 1Password reference (`op:...`) or a local file path
(`auth = 'file:~/.ssh/id_ed25519'`). The `host` parameter is required. The
`ssh_user` label defaults to `root` if omitted.

### Resource types

| Type                | Description                                        |
|---------------------|----------------------------------------------------|
| `file`              | File with owner, mode, and content                 |
| `directory`         | Directory with owner and mode                      |
| `symlink`           | Symbolic link                                      |
| `systemd_service`   | systemd service unit                               |
| `systemd_timer`     | systemd timer unit                                 |
| `nginx_vhost`       | Nginx virtual host configuration                   |
| `nginx_proxy`       | Nginx reverse proxy site                           |
| `docker_container`  | Docker container                                   |
| `docker_volume`     | Docker volume                                      |
| `docker_network`    | Docker network                                     |
| `docker_compose`    | Docker Compose stack (multi-container application) |
| `letsencrypt_cert`  | Let's Encrypt TLS certificate via certbot          |

### Query functions

These functions execute commands on the remote host and return tabular results.

| Function               | Description                                          |
|------------------------|------------------------------------------------------|
| `file_stat()`          | Return file metadata (size, owner, mode, mtime)      |
| `systemd_services()`   | List systemd services with enabled/active state      |
| `nginx_vhosts()`       | List configured Nginx virtual hosts                  |
| `nginx_config_test()`  | Run `nginx -t` and return pass/fail with diagnostics |
| `docker_containers()`  | List Docker containers with image, state, and ports  |

```sql
SELECT * FROM systemd_services('web-server') WHERE active = 'running';
SELECT * FROM docker_containers('web-server');
```

---

## KVM / Firecracker

Local KVM virtualisation via the Firecracker VMM. Used for lightweight
microvms and their backing volumes. No cloud credentials required -- all
operations happen on the local host through the Firecracker Unix socket API.
