#!/usr/bin/env bash


cfssl gencert -initca  ca-csr.json | cfssljson -bare ca
cfssl gencert -ca=ca.pem -ca-key=ca-key.pem -config=ca-config.json -profile=babuza babuza-csr.json | cfssljson -bare babuza