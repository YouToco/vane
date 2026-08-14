# Forced-command production broker

The installed root-owned copy is the only production lock and CAS authority.
SSH accepts only exact `vane-broker <verb>` forced commands and JSON on stdin;
there is no shell passthrough. The repository version validates signatures,
manifest chains, request basenames, the global lock, and current-release CAS.
Production mutation handlers remain deliberately disabled until a fixed broker
revision is installed and exercised on the VPS.
