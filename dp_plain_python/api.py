from fastapi import FastAPI
from dp_plain_python.run_all import main
from dp_plain_python.environment import config

app = FastAPI()


@app.post("/run")
async def run():
    main()

    return {"message": "Data pipeline completed successfully!"}


@app.get("/test")
async def test():
    return {"message": "Alive"}
