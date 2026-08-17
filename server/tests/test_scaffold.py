import gza_server


def test_package_imports():
    assert gza_server.__version__


def test_gza_api_importable():
    import gza  # noqa: F401
