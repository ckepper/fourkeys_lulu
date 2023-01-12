terraform_deploy:
	@echo "Deploying Terraform"
	@cd terraform/example && terraform init && terraform apply -auto-approve
