SRC_FILES := $(wildcard src/data_loader/*.py)
TEST_FILES := $(wildcard tests/*.py)

format:
	black $(SRC_FILES)
	black $(TEST_FILES)

black: format

check_black_version:
	black --version

install_black:
	pip install black