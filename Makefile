.PHONY: hooks
hooks:
	chmod +x $(realpath ./hooks/pre-commit.sh)
	ln -sf $(realpath ./hooks/pre-commit.sh) $(CURDIR)/.git/hooks/pre-commit

.PHONY: dev
dev:
	watchexec -e zig -- "clear && zig build test"

.PHONY: install
install:
	zig build -p ~/.local -Doptimize=ReleaseSmall
