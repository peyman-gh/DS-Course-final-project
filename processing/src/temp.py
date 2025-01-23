from fastapi import FastAPI, HTTPException
import os

app = FastAPI()

@app.get("/temp")
def read_temp():
    file_path = "temp.txt"
    
    # Check if the file exists
    if not os.path.exists(file_path):
        return {"message": "empty"}
    
    try:
        # Read the content of the file
        with open(file_path, 'r') as file:
            content = file.read().strip()
        return {"content": content}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"An error occurred: {e}")

# To run the FastAPI app, use the following command:
# uvicorn your_script_name:app --reload --host 0.0.0.0 --port 8585

