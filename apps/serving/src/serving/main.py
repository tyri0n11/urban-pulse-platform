"""Entry point for the serving API (uvicorn launcher)."""

import uvicorn

from serving.app import create_app

app = create_app()

if __name__ == "__main__":
    uvicorn.run("serving.main:app", host="0.0.0.0", port=8000, reload=False)
