import gza_server


def test_package_imports():
    assert gza_server.__version__


def test_gza_api_importable():
    # The server consumes gza through its Python APIs; the editable
    # path dependency must resolve inside this environment.
    import gza  # noqa: F401
