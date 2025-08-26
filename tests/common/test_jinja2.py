
from jinja2 import Environment
from pipe_gaps.common.jinja2 import EnvironmentLoader


def test_environment_loader():
    env = EnvironmentLoader().from_package(
        package="pipe_gaps",
        path="assets/queries",
    )

    assert isinstance(env, Environment)
