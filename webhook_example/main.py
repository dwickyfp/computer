from fastapi import FastAPI, Request
import json
import logging
import uvicorn
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("webhook-listener")

app = FastAPI(title="Webhook Receiver Example")

@app.get("/")
async def root():
    return {"message": "Webhook listener is running. Send POST requests to /webhook"}

@app.post("/webhook")
async def receive_webhook(request: Request):
    """
    Endpoint to receive webhook notifications.
    """
    try:
        # Get raw body
        body = await request.json()
        
        # Log the received data
        logger.info("----------------------------------------")
        logger.info(f"Received webhook at {datetime.now().isoformat()}")
        logger.info(f"Payload: {json.dumps(body, indent=2)}")
        logger.info("----------------------------------------")
        print("test")
        
        return {"status": "success", "message": "Webhook received"}
        
    except Exception as e:
        logger.error(f"Error processing webhook: {str(e)}")
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    print("Starting webhook listener on http://localhost:8025")
    uvicorn.run(app, host="0.0.0.0", port=8025)
