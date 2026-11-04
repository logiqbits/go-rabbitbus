.PHONY: test broker-test release

TAG := $(shell git describe --tags --exact-match HEAD 2>/dev/null)

test:            ## Build + run unit tests (no broker needed)
	go build ./...
	go test ./gbus/...

broker-test:     ## Run the broker-gated suite (needs RabbitMQ, RABBITBUS_AMQP or localhost default)
	cd tests && go test ./...

release: test    ## One command: test, then push HEAD + its exact version tag (go modules publish via git tags)
	@test -n "$(TAG)" || (echo "error: HEAD is not tagged; create the version tag first (git tag vX.Y.Z)" && exit 1)
	git push origin main
	git push origin $(TAG)
	@echo "released $(TAG) — the Go module proxy will serve it once it fetches from GitHub"

release-force: test   ## Same as release, but pushes even with a dirty working tree (use rarely)
	@test -n "$(TAG)" || (echo "error: HEAD is not tagged; create the version tag first (git tag vX.Y.Z)" && exit 1)
	git push origin main
	git push origin $(TAG)
