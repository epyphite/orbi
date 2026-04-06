# KVMQL Examples

| File | Description | Requirements |
|------|-------------|-------------|
| demo.kvmql | General demo (works with --simulate) | None |
| azure-stack.kvmql | Full Azure environment | `az login` |
| aws-stack.kvmql | AWS VPC + RDS + EC2 | `aws configure` |
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
