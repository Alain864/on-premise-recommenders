from fastapi import FastAPI


app = FastAPI(title="On-Premise Recommenders")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "stage": "data-foundation"}
