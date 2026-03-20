
## Quick Start

### Prerequisites

- **Python 3.11+** - Required for async features and type hints
- **PostgreSQL 15+** - Event store backend
- **uv** - Fast Python package installer (recommended)
- **Git** - Version control

### Installation

#### 1. Clone the repository

```bash
git clone https://github.com/your-org/the-ledger.git
cd the-ledger

# Install uv (if not already installed)

# On macOS/Linux
curl -LsSf https://astral.sh/uv/install.sh | sh

# On Windows (PowerShell)
powershell -c "irm https://astral.sh/uv/install.ps1 | iex"

# Or via pip
pip install uv


# 3. Create virtual environment and install dependencies
# Create virtual environment
uv venv

# Activate it
# On macOS/Linux:
source .venv/bin/activate
# On Windows:
.venv\Scripts\activate

# Install dependencies
uv pip install -e .

# Install development dependencies (for testing)
uv pip install -e ".[dev]"


# 4. Set up PostgreSQL database
# Create database (if not exists)
createdb ledger

# Or via psql
psql -c "CREATE DATABASE ledger;"


# 5. Configure environment

#Create a .env file in the project root:

# Database connection
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/ledger
# or with Unix socket
# DATABASE_URL=postgresql:///ledger?host=/var/run/postgresql

# Logging
LOG_LEVEL=INFO
LOG_FORMAT=json

# Projection daemon
PROJECTION_POLL_INTERVAL_MS=100
PROJECTION_BATCH_SIZE=500

# Server settings
MCP_SERVER_HOST=0.0.0.0
MCP_SERVER_PORT=8000

# Security
JWT_SECRET_KEY=your-secret-key-here
RATE_LIMIT_PER_MINUTE=100


# 6. Run database migrations

# Initialize the database schema
python -m ledger.cli migrate

# Or using the installed script
ledger-migrate

# Verify migration
psql -d ledger -c "\dt"
# Should show: events, event_streams, projection_checkpoints, outbox
