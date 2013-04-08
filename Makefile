all:
	echo "Building distribution"
	python setup.py build

clean:
	@rm -f another/*.pyc
	@rm -f *.pyc
