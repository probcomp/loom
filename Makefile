cpu_count=$(shell python -c 'import multiprocessing as m; print m.cpu_count()')

nose_env=
ifndef NOSE_PROCESSES
	nose_env+=NOSE_PROCESSES=$(cpu_count) NOSE_PROCESS_TIMEOUT=1800
endif

cmake_args=
cmake_env=
ifdef VIRTUAL_ENV
	cmake_args+=-DCMAKE_INSTALL_PREFIX=$(VIRTUAL_ENV)
	cmake_env+=CMAKE_PREFIX_PATH=$(VIRTUAL_ENV)
else
	ifdef CONDA_PREFIX
		cmake_args+=-DCMAKE_INSTALL_PREFIX=$(CONDA_PREFIX)
		cmake_args+=-DEXTRA_INCLUDE_PATH=$(CONDA_PREFIX)/include
		cmake_args+=-DEXTRA_LIBRARY_PATH=$(CONDA_PREFIX)/lib
		cmake_env+=CMAKE_PREFIX_PATH=$(CONDA_PREFIX)
	endif
endif
cmake = $(cmake_env) cmake $(cmake_args)

all: debug release

check_gcc_version: FORCE
	@($(CXX) -dumpversion | grep -v '^4\.9') || (		\
	  echo 'ERROR loom is known to break with gcc 4.9';	\
	  echo '  try gcc-4.8 instead';				\
	  exit 1						\
	)

debug: check_gcc_version FORCE
	mkdir -p build/debug
	cd build/debug \
	  && $(cmake) -DCMAKE_BUILD_TYPE=Debug ../.. \
	  && $(MAKE)

release: check_gcc_version FORCE
	mkdir -p build/release
	cd build/release \
	  && $(cmake) -DCMAKE_BUILD_TYPE=Release ../.. \
	  && $(MAKE)

install_cc: debug release FORCE
	cd build/release && $(MAKE) install
	cd build/debug && $(MAKE) install

dev: install_cc FORCE
	pip install --editable .

install: install_cc FORCE
	pip install --upgrade .

package: release FORCE
	cd build/release && $(MAKE) package
	mv build/release/loom.tar.gz build/

clean: FORCE
	git clean -xdf -e loom.egg-info -e data

data: dev FORCE
	python -m loom.datasets init

test: dev
	@pyflakes loom/schema_pb2.py \
	  || (echo '...patching schema_pb2.py' \
	    ; sed -i '/descriptor_pb2/d' loom/schema_pb2.py)  # HACK
	pyflakes setup.py loom examples
	pep8 --repeat --ignore=E402,E731 --exclude=*_pb2.py setup.py loom examples
	python -m loom.datasets test
	$(nose_env) nosetests -v loom examples
	$(MAKE) -C doc
	@echo '----------------'
	@echo 'PASSED ALL TESTS'

small-test:
	LOOM_TEST_COST=100 $(MAKE) test

big-test:
	LOOM_TEST_COST=1000 $(MAKE) test

bigger-test:
	LOOM_TEST_COST=10000 $(MAKE) test

license: FORCE
	python update_license.py update

FORCE:
