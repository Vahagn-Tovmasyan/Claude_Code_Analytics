"""
FastAPI application for programmatic access to analytics data.
"""

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.routes import router

app = FastAPI(
    title="Claude Code Analytics API",
    description="Programmatic access to Claude Code telemetry analytics",
    version="1.0.0",
)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router, prefix="/api")


@app.get("/")
def root():
    return {
        "service": "Claude Code Analytics API",
        "version": "1.0.0",
        "docs": "/docs",
    }
