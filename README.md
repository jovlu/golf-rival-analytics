# Golf Rival Analytics

Small FastAPI app for cleaning Golf Rival event data, loading it into PostgreSQL, and serving analytics endpoints.

## Requirements

- Python 3.11+
- PostgreSQL

## 1. Create a virtual environment

Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

macOS/Linux:

```bash
python -m venv .venv
source .venv/bin/activate
```

## 2. Install dependencies

```bash
pip install -r requirements.txt
```

## 3. Configure the database

Create a PostgreSQL database, then create a `.env` file in the project root:

```env
DATABASE_URL=postgresql+psycopg://USER:PASSWORD@localhost:5432/DATABASE_NAME
```

Example:

```env
DATABASE_URL=postgresql+psycopg://postgres:postgres@localhost:5432/golf_rival_analytics
```

Check that the app can connect:

```bash
python main.py check-db
```

## 4. Clean and load the data

Run the full pipeline:

```bash
python main.py clean-jsonl
python main.py load-db
```

This creates/updates:

- `events.deduped.jsonl`
- `events.cleaned.jsonl`


## 5. Run the API

```bash
uvicorn app.main:app --reload
```

Open:

- API root: http://127.0.0.1:8000/
- Health check: http://127.0.0.1:8000/health
- Swagger docs: http://127.0.0.1:8000/docs
- Chart http://127.0.0.1:8000/map-stats-chart