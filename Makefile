all:
	echo "Building distribution"
	python setup.py build
develop:
	python setup.py develop

clean:
	@rm -f another/*.pyc
	@rm -f *.pyc
