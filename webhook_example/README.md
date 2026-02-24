# Webhook Example

This is a simple FastAPI application that acts as a webhook receiver. It logs all incoming POST requests to the console.

## Usage

1.  Install dependencies:

    ```bash
    uv sync
    ```

2.  Run the server:

    ```bash
    uv run main.py
    ```

    The server will start at `http://localhost:8001`.

3.  Send a test webhook:

    You can use `curl` or Postman to send a POST request to `http://localhost:8001/webhook`.

    ```bash
    curl -X POST http://localhost:8001/webhook \
      -H "Content-Type: application/json" \
      -d '{"key": "value", "message": "hello world"}'
    ```

4.  Check the logs:

    The application will print the received JSON payload to the console.
