# Forced-command production broker

The installed root-owned copy is the only Server production lock and CAS authority.
SSH accepts only exact `vane-broker <verb>` forced commands and JSON on stdin;
there is no shell passthrough. The repository version validates signatures,
manifest chains, request basenames, the global lock, and current-release CAS.
It accepts only Server artifacts; Web assets and Aliyun credentials never enter
the broker or VPS. Production mutation remains disabled until a fixed broker
revision is installed and exercised on the VPS.
