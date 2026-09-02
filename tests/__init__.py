"""Test package.

Makes ``tests`` importable so modules can share ``tests.conftest.load_fixture``. Without
it, the import only resolves when something incidentally puts the repository root on
``sys.path`` -- true for some editable installs, not for a plain install or CI.
"""
