#!/usr/bin/env bash

set -eux

kubectl apply -f ./deploy.yml
kubectl rollout status deployment/glaredb-proxy
kubectl get services glaredb-proxy-service -o wide
