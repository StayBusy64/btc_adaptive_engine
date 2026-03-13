from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates

app = FastAPI(title="BTC Adaptive Engine Dashboard")
templates = Jinja2Templates(directory="dashboard/templates")


@app.get("/")
def dashboard_home(request: Request):
    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "title": "BTC Adaptive Engine Dashboard"
        }
    )
