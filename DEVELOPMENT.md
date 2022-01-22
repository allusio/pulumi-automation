
# Packaging

See https://packaging.python.org/en/latest/guides/distributing-packages-using-setuptools/#packaging-your-project

## Prerequisities

```shell
python3 -m pip install build twine
```

## Build the package

```shell
python -m build --wheel
```

## Check and upload

```shell
twine check dist/* && twine upload dist/*
```
