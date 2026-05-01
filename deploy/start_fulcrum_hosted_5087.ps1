$ErrorActionPreference = "Stop"

throw "This old Fulcrum starter is disabled because it binds the stale 5092 worker. Use .\deploy\ensure_fulcrum_hosted_stack.ps1, which starts the canonical worker on 127.0.0.1:5093."
