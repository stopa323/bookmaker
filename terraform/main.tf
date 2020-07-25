terraform {
  backend "s3" {
    bucket = "bookmaker-terraform-state-eu-west-1"
    key    = "staging/terraform.tfstate"
    region = "eu-west-1"
  }
}

provider "aws" {
  region  = "eu-west-1"
  version = ">= 2.38.0"
}