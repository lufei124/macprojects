# AGENTS.md

## Project Overview

Flask-based real-time event tracking viewer (埋点实时查看工具). Single-file app (`backend_app.py`) with embedded HTML/JS/CSS frontend. Polls MySQL database to display device events in browser. Used for QA verification of analytics events during mobile app testing.

## Build / Run Commands

```bash
# Install dependencies (use virtual environment)
pip install -r requirements.txt

# Configure database (required before first run)
cp config.example.py config.py
# Edit config.py with actual MySQL credentials, or set .env variables

# Run the application
python backend_app.py
# Serves at http://127.0.0.1:8888
```

## Testing

No test suite exists. When adding tests:
- Use `pytest` for test framework
- Create `tests/` directory with `test_*.py` files
- Run all tests: `pytest`
- Run single test: `pytest tests/test_file.py::test_function_name`
- Run single test by keyword: `pytest -k "test_health"`

## Linting / Type Checking

No linting configured. Recommended setup:

```bash
pip install ruff mypy

# Lint
ruff check .

# Auto-fix lint issues
ruff check --fix .

# Format
ruff format .

# Type check
mypy backend_app.py
```

## Code Style Guidelines

### Python

- **Imports**: Standard library first, then third-party, then local; separated by blank lines
- **Formatting**: PEP 8, 4-space indentation, ~120 char line width
- **Types**: Use type hints for all function signatures and key variables
  - `Optional[T]` for nullable types
  - `List[Dict[str, Any]]` for collection types
  - Return type `-> Any` for Flask route handlers
- **Naming**:
  - `snake_case` for functions, variables, modules
  - `UPPER_CASE` for constants (e.g., `DB_CONFIGS`, `DEFAULT_ENV`, `INDEX_HTML`)
  - Prefix private helpers with `_` (e.g., `_load_env_config`)
- **Error handling**:
  - Use `try/except` with specific exception types
  - Fallback to defaults on parse errors (e.g., `int()` ValueError → default 100)
  - Frontend errors display via `errorText` element, not `alert()`
- **Config**: Use `config.example.py` as template; actual `config.py` is gitignored
  - Config loaded via `python-dotenv` from `.env` file
  - Support multiple environments via `DB_CONFIGS` dict keyed by env name

### JavaScript (embedded in INDEX_HTML)

- **Naming**: `camelCase` for variables and functions
- **DOM references**: Prefix with element purpose (e.g., `deviceIdInput`, `toggleBtn`, `envSelect`)
- **Async**: Use `async/await` for fetch calls; avoid `.then()` chains
- **Error handling**: Wrap fetch calls in try/catch, display to user via `errorText.textContent`
- **Variables**: Use `var` (ES5 style to match embedded context); avoid `let`/`const`
- **DOM manipulation**: Direct `innerHTML`/`createElement` — no framework

### HTML/CSS (embedded in INDEX_HTML)

- **Classes**: Descriptive BEM-like names (`.status-pill`, `.event-item`, `.category-header`)
- **Responsive**: Mobile-first, use `@media (max-width: 768px)` breakpoints
- **Colors**: CSS custom properties for theme (`--accent`, `--green`, `--red`)
- **Layout**: Flexbox-based, no CSS grid for main layout

## Architecture

```
backend_app.py (single file, ~750 lines)
├── get_connection(env)        # MySQL connection factory using pymysql
├── /health                    # GET → {"status": "ok"}
├── /envs                      # GET → available environments from DB_CONFIGS
├── /events                    # GET → filtered event rows (main API)
│   └── Params: device_id*, env, event_name, since_id, before_id, limit
├── INDEX_HTML                 # Embedded frontend (~600 lines HTML/CSS/JS)
└── /                          # Serves INDEX_HTML

config.example.py              # Template config with env var loading
config.py                      # Actual config (gitignored)
requirements.txt               # Flask, pymysql, python-dotenv
```

## Key Conventions

1. **Single-file architecture**: All code in `backend_app.py`; avoid splitting unless complexity exceeds ~1000 lines
2. **Embedded frontend**: HTML/CSS/JS stored in `INDEX_HTML` string constant; no separate frontend build
3. **Config externalization**: Database credentials in `config.py` (gitignored) or `.env`
4. **Defensive parsing**: Handle bytes (decode), datetime (isoformat), large integers (>9007199254740991 → string) in API responses
5. **Polling pattern**: Frontend polls `/events` with `since_id` for incremental updates via `setInterval`
6. **Parameterized queries**: Always use `%s` placeholders with `pymysql`; never f-string user input into SQL except for validated table names from config
7. **Input validation**: `limit` clamped to 1-500, `device_id` required, `env` validated against `DB_CONFIGS` keys

## Security Notes

- `config.py` and `.env` contain credentials — never commit
- Use parameterized queries (`%s` placeholders) to prevent SQL injection
- Table name comes from config (not user input), so `f-string` in SQL is acceptable
- No authentication on endpoints — intended for local use only
