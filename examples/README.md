# Orbi Examples

| File | Description | Requirements |
|------|-------------|-------------|
| demo.kvmql | General demo (works with --simulate) | None |
| azure-stack.kvmql | Full Azure environment | `az login` |
| aws-stack.kvmql | AWS VPC + RDS + EC2 | `aws configure` |
| cloudflare-demo.kvmql | DNS, firewall, page rules | `CLOUDFLARE_API_TOKEN` |
| github-project-setup.kvmql | Repo + ruleset + secrets + workflow | `gh auth login` |
| kubernetes-deploy.kvmql | K8s namespace, deployment, service, ingress | `kubectl` + cluster |
| multi-env.kvmql | Multi-environment setup | None (--simulate) |
| dr-failover.kvmql | Disaster recovery workflow | Two providers |

## Running Examples

```bash
# No credentials needed
orbi --simulate exec examples/demo.kvmql

# With real Azure
orbi exec examples/azure-stack.kvmql

# With real AWS
orbi exec examples/aws-stack.kvmql

# Plan first, then apply
orbi plan examples/azure-stack.kvmql --name "staging deploy"
orbi plans --status pending
orbi approve <plan-id>
orbi apply <plan-id>
```
