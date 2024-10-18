# Flask-RedisDict

Flask extension that allows access to Redis hash as a dictionary.

Serializes values using `flask.sessions.TaggedJSONSerializer`.

Default keys generated with `uuid4`.

## Install for development

	git clone https://github.com/lovette/flask_redisdict.git
	cd flask_redisdict/
	make virtualenv
	source $HOME/.virtualenvs/flask_redisdict/bin/activate
	make requirements
	make install-dev
