from setuptools import setup

setup(
    name='flask_redisdict',
    version='1.0.0',
    url='https://github.com/lovette/flask_redisdict',
	download_url = 'https://github.com/lovette/flask_redisdict/archive/master.tar.gz',
    license='BSD',
    author='Lance Lovette',
    author_email='lance.lovette@gmail.com',
    description='Flask extension that allows access to Redis hash as a dictionary.',
    long_description=open('README.md').read(),
    py_modules=['flask_redisdict',],
    install_requires=['Flask', 'redis',],
    tests_require=['nose',],
    zip_safe=False,
    platforms='any',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Topic :: Internet :: WWW/HTTP :: Dynamic Content',
        'Topic :: Software Development :: Libraries :: Python Modules'
    ]
)
