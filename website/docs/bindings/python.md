:::info
Python SDK is currently in alpha, requiring wvlet and Java installation on your local machine.
It will be available in PyPI in the future.
:::


# Wvlet Python binding

## How to install

```sh
pip install -e "git+https://github.com/wvlet/wvlet/#egg=wvlet&subdirectory=sdks/python"
```

## Usage Examples

```python
from wvlet.runner import WvletCompiler
c = WvletCompiler()
sql = c.compile("show tables")
```
