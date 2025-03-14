#!/usr/bin/env bash


rm -f *.pem *.csr

echo "Generating CA certificate..."
cfssl selfsign -config cfssl.json --profile rootca "Babuza Dev Testing CA" csr.json | cfssljson -bare root

echo "Generating server/client certificate..."

cfssl genkey csr.json | cfssljson -bare server
cfssl genkey csr.json | cfssljson -bare client

echo "Sign server/client certificate..."
cfssl sign -ca root.pem -ca-key root-key.pem -config cfssl.json -profile server server.csr | cfssljson -bare server
cfssl sign -ca root.pem -ca-key root-key.pem -config cfssl.json -profile client client.csr | cfssljson -bare client

echo "Certificate generation completed successfully"

# remove the CSR files
rm -f *.csr