lint:
	isort src/
	black src/
	flake8 src/
	pydocstyle src/

clean:
	rm -f *~
	cd src/ && rm -f *~ && rm -rf plotly_charts && rm -f table_of_contents.html
