from setuptools import setup

with open('README.md', 'r') as oF:
	long_description=oF.read()

setup(
	name='record-mysql',
	version='1.0.1',
	description='Provides abstract classes meant to represent record data as Define Node types',
	long_description=long_description,
	long_description_content_type='text/markdown',
	url='https://ouroboroscoding.com/record-mysql',
	project_urls={
		'Documentation': 'https://ouroboroscoding.com/record-mysql',
		'Source': 'https://github.com/ouroboroscoding/record-mysql-python',
		'Tracker': 'https://github.com/ouroboroscoding/record-mysql-python/issues'
	},
	keywords=['data','define','database','db','sql','nosql'],
	author='Chris Nasr - Ouroboros Coding Inc.',
	author_email='chris@ouroboroscoding.com',
	license='MIT',
	packages=['record_mysql'],
	python_requires='>=3.10',
	install_requires=[
		'arrow>=1.2.2,<1.3',
		'define-oc>=1.0.0,<1.1',
		'jobject>=1.0.2,<1.1',
		'jsonb>=1.0.0,<1.1',
		'PyMySQL>=1.0.2,<1.1',
		'record-oc>=1.0.0,<1.1',
		'undefined-oc>=1.0.0,<1.1'
	],
	test_suite='tests',
	zip_safe=True
)