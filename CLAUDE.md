## Ways of Working
Use `uv run` to run Python scripts and tests — it automatically manages the virtual environment.
- `uv sync` to install/sync dependencies
- `CLASSPATH="test/jars/*" uv run python -m unittest test.test_integration.HsqldbTest` to run integration tests
- `CLASSPATH="test/jars/*:test/mock-jars/*" uv run python -m unittest test.test_mock` to run mock tests
- `uv run bash test/build.sh` to build JARs


## Speical Requirements in YOLO Mode

When in YOLO mode, i.e., when all user approvals are skipped, you should not execute any commands outside of the current working directory. Also please follow the agile development practice:

1. When tests are available, always run tests before calling a task done
2. After all development for a task is finished, use `gh` (GitHub CLI) to create a pull request for this feature branch with a concise summary of what you've done
