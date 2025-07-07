## Install Development Requirements
VegaFusion's entire development environment setup is managed by [Pixi](https://prefix.dev/docs/pixi/overview).  Pixi is a cross-platform environment manager that installs packages from [conda-forge](https://conda-forge.org/) into an isolated environment directory for the project. Conda-forge provides all the development and build technologies that are required by VegaFusion, and so Pixi is the only system installation required to build and develop VegaFusion.

### Installing Pixi
On Linux and MacOS install pixi with:

```
curl -fsSL https://pixi.sh/install.sh | bash
```

On Windows, install Pixi with:
```
iwr -useb https://pixi.sh/install.ps1 | iex
```

Then restart your shell. 

For more information on installing Pixi, see https://prefix.dev/docs/pixi/overview.

## Build and test Rust

Start the test minio server in a dedicated terminal

```
pixi run start-minio
```

If command fails unable to import numpy, check [this workaround](https://github.com/conda-forge/numpy-feedstock/issues/347#issuecomment-2779297248).

Build and test the VegaFusion Rust crates with:

```
pixi run test-rs
```

Individual rust crates can be tested using:
```
pixi run test-rs-core
pixi run test-rs-runtime
pixi run test-rs-server
pixi run test-rs-sql
```

## Build and test Python

Build and test the `vegafusion-python` package with:

```
pixi run test-py
```

### Try Python package
To use the development versions of the Python package, first install the packages in development mode with:

```
pixi run dev-py
```

Then just run your Python script with Pixi, it will use development build of VegaFusion:

```
pixi run python examples/python-examples/column_usage.py
```


## Build Python packages for distribution
To build Python wheels for the current platform, the `build-py` task may be used

```
pixi run build-py
```
This will build a wheel and sdist in the `target/wheels` directory.



## Making a change to development conda environment requirements
To add a conda-forge package to the Pixi development environment, use the `pixi add` command with the `--build` flag

```
pixi add my-new-package --build
```

This will install the package and update the pixi.lock file with the new environment solution.
