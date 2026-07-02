# Calculator

A minimal Python calculator module with basic arithmetic operations.

## Files

- `calculator.py` — the module (`add`, `subtract`, `multiply`, `divide`)
- `test_calculator.py` — pytest test suite
- `README.md` — this file

## API

### `add(a, b)`
Returns the sum of `a` and `b`.

### `subtract(a, b)`
Returns `a - b`.

### `multiply(a, b)`
Returns the product of `a` and `b`.

### `divide(a, b)`
Returns `a / b`. Raises `ZeroDivisionError` if `b == 0`.

## Usage

```python
from calculator import add, subtract, multiply, divide

add(2, 3)        # 5
subtract(5, 3)    # 2
multiply(4, 3)    # 12
divide(10, 2)     # 5.0
divide(1, 0)      # raises ZeroDivisionError
```

## Running tests

```bash
pip install pytest
pytest
```
