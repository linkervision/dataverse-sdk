## How to develop
Develop utilities were managed by `Makefile`.

```bash
# Install git-hooks and commit template
make install-dev
```


## How to build

`python/setup.py` holds the only copy of the version -- bump `PACKAGE_VERSION` first.

```bash
cd python
pip install build          # once per environment
python -m build            # writes python/dist/
```

Produces `dataverse_sdk-<version>-py3-none-any.whl` and `dataverse_sdk-<version>.tar.gz`.

## How to test a build locally

Use **Python 3.10 or 3.11**: `numpy>=1.21,<2` has no wheels for 3.12+, so a newer
interpreter fails partway through building numpy rather than refusing the install.

```bash
python3.11 -m venv test-venv
source test-venv/bin/activate                    # Windows: test-venv\Scripts\activate
pip install dataverse_sdk-<version>-py3-none-any.whl

python -c "import importlib.metadata as m; print(m.version('dataverse-sdk'))"
python -c "import dataverse_sdk; print(dataverse_sdk.__file__)"   # must sit under test-venv/
```

`tools/` is not a package, so only the sdist carries the scripts. Unpack it and run them
the usual way:

```bash
tar -xzf dataverse_sdk-<version>.tar.gz          # Windows 10+ ships tar as well
cd dataverse_sdk-<version>
python tools/export_dataslice.py --help
```

A script's own directory goes on `sys.path`, not the working directory, so this imports
`dataverse_sdk` from the venv even though the unpacked sdist keeps a copy of the source
right next to `tools/`.

Reinstalling the same version number needs `--force-reinstall`, or pip skips the file and
the old code stays. `pip uninstall -y dataverse-sdk` to clean up.
