# GDG JSON Parser

A simplified JSON parser implementation.

## Installation

```bash
pip install gdg_json
```

## Usage

```python
from gdg_json import parse_json

# Parse a JSON string
result = parse_json('{"key": "value"}')
print(result)  # {'key': 'value'}
```

## Development

1. Clone the repository
2. Create a virtual environment: `python -m venv .venv`
3. Activate the virtual environment:
   - Windows: `.venv\Scripts\activate`
   - Unix/MacOS: `source .venv/bin/activate`
4. Install development dependencies: `pip install -e .`
