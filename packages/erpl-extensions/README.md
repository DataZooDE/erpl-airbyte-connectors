# erpl-extensions

The [ERPL](https://erpl.io) DuckDB extensions — `erpl_rfc`, `erpl_bics`,
`erpl_odp`, `erpl_web` — together with the SAP NetWeaver RFC SDK and ICU shared
objects they link against, packaged so they can be installed rather than
downloaded at runtime.

It exists because Airbyte's connector images are built from a template that
copies only `/usr/local` and `/airbyte/integration_code` out of the builder
stage. A pip-installed package lands in site-packages, inside `/usr/local`, and
therefore survives; anything fetched into a directory of its own does not.

```python
from erpl_extensions import extension_dir

duckdb.connect(config={"extension_directory": str(extension_dir()), "allow_unsigned_extensions": "true"})
```

Linux x86-64 only: ERPL publishes no arm64 or macOS build of these extensions,
and the wheel is platform-tagged so installing it anywhere else fails loudly
instead of yielding an image that starts and can load nothing.

## Publishing

```bash
./bin/build-wheel.sh                       # fetch, verify checksums, build, tag
twine upload dist/*.whl                    # DataZoo PyPI credentials
```

The wheel is ~72 MB (194 MB of binaries, compressed), one file, under PyPI's
100 MB per-file limit. Version it with the ERPL release it carries.
