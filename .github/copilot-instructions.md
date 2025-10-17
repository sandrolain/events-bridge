## Code and language

The code and the texts in the code and comments must be written in English.

## Commit messages

For commits use conventional commits format.

## After every change or integration

- run the specific tests to ensure the code works as expected and a minium coverage of 80% is reached.
- assure that inline docs are updated and accurate.
- run `golangci-lint run` and `go vet ./...` and `gosec -exclude-generated ./...` to ensure no issues are present.

## tests

- benchmark tests must be created in a separated file respecting the unit tests file, and with the suffix `_bench_test.go`.

## changes context

- track changes in markdown files into the file `context/changes.md` with a short description of the change and the date of the change.
- use a file to preserve context and description of the project into `context/project.md`.