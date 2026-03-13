from backend.event_writer import init_db

if __name__ == "__main__":
    init_db()
    print("BTC Adaptive Engine scaffold initialized.")
    print("Next step: run the API with uvicorn backend.api_server:app --reload")
