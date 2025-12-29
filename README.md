# Carbon Platform (CFO/CFP/Carbon Credit) — Full Stack

Stack:
- Backend: FastAPI + SQLAlchemy + PostgreSQL
- Frontend: React (Vite)
- Features: EF library (rich metadata), dynamic activity forms, formula engine, CFO/CFP runs, carbon credit project developer, RBAC, audit engine, CSV/Excel import, PDF/Excel report export.

## Quick start (Docker)
```bash
docker compose up -d --build
```

Backend: http://localhost:8000  
Frontend: http://localhost:5173

Default admin (dev):
- username: admin
- password: admin1234

## Dev (no docker)
### Backend
```bash
cd backend
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000
```

### Frontend
```bash
cd frontend
npm install
npm run dev
```

## Notes
- In enterprise mode you should use Alembic migrations and a proper secrets manager (see docs folder).
