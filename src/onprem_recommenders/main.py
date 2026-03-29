from fastapi import FastAPI

from onprem_recommenders.recommendations import router as recommendations_router


app = FastAPI(
    title="On-Premise Recommenders",
    description="Proof-of-concept search and recommendation system",
    version="0.1.0",
)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "stage": "stage2"}


# Include recommendation endpoints
app.include_router(recommendations_router)
