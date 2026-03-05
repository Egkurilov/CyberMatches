from cybermatches.api.app import app


if __name__ == "__main__":
    import os
    import uvicorn

    uvicorn.run(
        "api:app",
        host="0.0.0.0",
        port=int(os.getenv("API_PORT", 8050)),
        reload=False,
    )
