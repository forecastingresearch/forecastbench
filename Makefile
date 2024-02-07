lint:
	isort .
	black .
	flake8 .
	pydocstyle .

clean:
	find . -type f -name "*~" -exec rm -f {} +
	cd src/gpt && rm -rf plotly_charts && rm -f table_of_contents.html
