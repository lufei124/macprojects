# AGENTS.md

## Project Overview

Flask-based real-time event tracking viewer. Single-file app (`backend_app.py`) with embedded HTML/JS/CSS frontend. Polls MySQL database to display device events in browser.

## Build / Run Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run the application
python backend_app.py
# Serves at http://127.0.0.1:8888

# Configuration (required before first run)
cp config.example.py config.py
# Then edit config.py with actual MySQL credentials
```

## Testing

No test suite exists. When adding tests:
- Use `pytest` for test framework
- Create `tests/` directory with `test_*.py` files
- Run all tests: `pytest`
- Run single test: `pytest tests/test_file.py::test_function_name`

## Linting / Type Checking

No linting configured. Recommended setup:

```bash
# Install tools
pip install ruff mypy

# Lint
ruff check .

# Type check
mypy backend_app.py
```

## Code Style Guidelines

### Python

- **Imports**: Standard library first, then third-party, then local
- **Formatting**: Follow PEP 8, 4-space indentation
- **Types**: Use type hints for function signatures
- **Naming**: 
  - `snake_case` for functions, variables, modules
  - `UPPER_CASE` for constants (e.g., `DB_CONFIG`, `INDEX_HTML`)
- **Error handling**: Use try/except with specific exceptions, log errors to `errorText` in frontend

### JavaScript (embedded in INDEX_HTML)

- **Naming**: `camelCase` for variables and functions
- **DOM references**: Prefix with element purpose (e.g., `deviceIdInput`, `toggleBtn`)
- **Async**: Use `async/await` for fetch calls
- **Error handling**: Wrap in try/catch, display to user via `errorText`

### HTML/CSS (embedded in INDEX_HTML)

- **Classes**: Use BEM-like naming or descriptive names (`.status-pill`, `.event-card`)
- **Responsive**: Mobile-first, use `@media (max-width: 720px)` breakpoints
- **Colors**: CSS custom properties for theme consistency

## Architecture

```
backend_app.py
├── get_connection()     # MySQL connection factory
├── /health              # Health check endpoint
├── /events              # Event query endpoint (GET)
│   └── Params: device_id, event_name, since_id, before_id, limit
├── INDEX_HTML           # Embedded frontend (HTML + CSS + JS)
└── /                    # Serves INDEX_HTML
```

## Key Conventions

1. **Single-file architecture**: All code in `backend_app.py`, no separate modules unless complexity demands it
2. **Embedded frontend**: HTML/CSS/JS stored in `INDEX_HTML` constant
3. **Config externalization**: Database credentials in `config.py` (gitignored)
4. **Defensive parsing**: Handle bytes, datetime, large integers in API responses
5. **Polling pattern**: Frontend polls `/events` with `since_id` for incremental updates

## Security Notes

- `config.py` contains credentials - never commit
- Use parameterized queries (pymysql placeholders) to prevent SQL injection
- Input validation on `limit` (1-500) and `device_id` (required)
