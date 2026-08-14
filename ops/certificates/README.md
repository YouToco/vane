# Certificate primitives

`renew-cert.sh` retains the existing ACME issuance throttle, key/certificate
match, Aliyun upload, and exact edge fingerprint verification. Repository-side
`vane cert check` is read-only; issuance credentials and scheduling belong to
the external production broker.
