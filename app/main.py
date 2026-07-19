from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.routers import lag, velocity, topology, thresholds,cascade

app = FastAPI(title="EventMesh API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # dev-friendly; tighten before any real deployment
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health_check():
    return {"status": "ok", "service": "EventMesh API"}


app.include_router(lag.router)
app.include_router(velocity.router)
app.include_router(topology.router)
app.include_router(thresholds.router)
app.include_router(cascade.router)